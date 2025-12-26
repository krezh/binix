use std::io;

use std::io::Cursor;
use std::marker::Unpin;
use std::sync::Arc;

use anyhow::anyhow;
use async_compression::tokio::bufread::{BrotliEncoder, XzEncoder, ZstdEncoder};
use async_compression::Level as CompressionLevel;
use axum::{
    body::Body,
    extract::{Extension, Json},
    http::HeaderMap,
};
use bytes::{Bytes, BytesMut};
use chrono::Utc;
use futures::future::join_all;
use futures::StreamExt;
use sea_orm::entity::prelude::*;
use sea_orm::sea_query::Expr;
use sea_orm::ActiveValue::Set;
use sea_orm::{QuerySelect, TransactionTrait};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt};
use tokio::sync::Semaphore;
use tokio::task::spawn;
use tokio_util::io::StreamReader;
use tracing::instrument;
use uuid::Uuid;

use crate::compression::{CompressionStream, CompressorFn};
use crate::config::CompressionType;
use crate::error::{ErrorKind, ServerError, ServerResult};
use crate::narinfo::Compression;
use crate::{RequestState, State};
use binix::api::v1::upload_path::{
    UploadPathNarInfo, UploadPathResult, UploadPathResultKind, BINIX_NAR_INFO,
    BINIX_NAR_INFO_PREAMBLE_SIZE,
};
use binix::chunking::chunk_stream;
use binix::hash::Hash;
use binix::io::{read_chunk_async, HashReader};
use binix::util::Finally;

use crate::database::entity::cache;
use crate::database::entity::chunk::{self, ChunkState, Entity as Chunk};
use crate::database::entity::chunkref::{self, Entity as ChunkRef};
use crate::database::entity::nar::{self, Entity as Nar, NarState};
use crate::database::entity::object::{self, Entity as Object, InsertExt};
use crate::database::entity::Json as DbJson;
use crate::database::{BinixDatabase, ChunkGuard, NarGuard};

/// Number of chunks to upload to the storage backend at once.
///
/// TODO: Make this configurable
const CONCURRENT_CHUNK_UPLOADS: usize = 10;

/// Data of a chunk.
enum ChunkData {
    /// Some bytes in memory.
    Bytes(Bytes),

    /// A stream with a user-claimed hash and size that are potentially incorrect.
    Stream(Box<dyn AsyncBufRead + Send + Unpin + 'static>, Hash, usize),
}

/// Result of a chunk upload.
struct UploadChunkResult {
    guard: ChunkGuard,
    deduplicated: bool,
}

trait UploadPathNarInfoExt {
    fn to_active_model(&self) -> object::ActiveModel;
}

/// Uploads a new object to the cache.
///
/// When clients request to upload an object, we first try to increment
/// the `holders_count` of one `nar` row with same NAR hash. If rows were
/// updated, it means the NAR exists in the global cache and we can deduplicate
/// after confirming the NAR hash ("Deduplicate" case). Otherwise, we perform
/// a new upload to the storage backend ("New NAR" case).
#[instrument(skip_all)]
#[axum_macros::debug_handler]
pub(crate) async fn upload_path(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    headers: HeaderMap,
    body: Body,
) -> ServerResult<Json<UploadPathResult>> {
    use binix::api::v1::upload_path::BINIX_CHUNKED_UPLOAD;

    let stream = body.into_data_stream();
    let mut stream = StreamReader::new(
        stream.map(|r| r.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))),
    );

    let upload_info: UploadPathNarInfo = {
        if let Some(preamble_size_bytes) = headers.get(BINIX_NAR_INFO_PREAMBLE_SIZE) {
            // Read from the beginning of the PUT body
            let preamble_size: usize = preamble_size_bytes
                .to_str()
                .map_err(|_| {
                    ErrorKind::RequestError(anyhow!(
                        "{} has invalid encoding",
                        BINIX_NAR_INFO_PREAMBLE_SIZE
                    ))
                })?
                .parse()
                .map_err(|_| {
                    ErrorKind::RequestError(anyhow!(
                        "{} must be a valid unsigned integer",
                        BINIX_NAR_INFO_PREAMBLE_SIZE
                    ))
                })?;

            if preamble_size > state.config.max_nar_info_size {
                return Err(ErrorKind::RequestError(anyhow!("Upload info is too large")).into());
            }

            let buf = BytesMut::with_capacity(preamble_size);
            let preamble = read_chunk_async(&mut stream, buf)
                .await
                .map_err(|e| ErrorKind::RequestError(e.into()))?;

            if preamble.len() != preamble_size {
                return Err(ErrorKind::RequestError(anyhow!(
                    "Upload info doesn't match specified size"
                ))
                .into());
            }

            serde_json::from_slice(&preamble).map_err(ServerError::request_error)?
        } else if let Some(nar_info_bytes) = headers.get(BINIX_NAR_INFO) {
            // Read from X-Binix-Nar-Info header
            serde_json::from_slice(nar_info_bytes.as_bytes()).map_err(ServerError::request_error)?
        } else {
            return Err(ErrorKind::RequestError(anyhow!("{} must be set", BINIX_NAR_INFO)).into());
        }
    };

    if headers.contains_key(BINIX_CHUNKED_UPLOAD) {
        return handle_chunked_upload(state, req_state, headers, upload_info, stream).await;
    }

    let cache_name = &upload_info.cache;

    let database = state.database().await?;
    let cache = req_state
        .auth
        .auth_cache(database, cache_name, |cache, permission| {
            permission.require_push()?;
            Ok(cache)
        })
        .await?;

    let username = req_state.auth.username().map(str::to_string);

    // Try to acquire a lock on an existing NAR
    if let Some(existing_nar) = database.find_and_lock_nar(&upload_info.nar_hash).await? {
        // Deduplicate?
        let missing_chunk = ChunkRef::find()
            .filter(chunkref::Column::NarId.eq(existing_nar.id))
            .filter(chunkref::Column::ChunkId.is_null())
            .limit(1)
            .one(database)
            .await
            .map_err(ServerError::database_error)?;

        if missing_chunk.is_none() {
            // Can actually be deduplicated
            return upload_path_dedup(
                username,
                cache,
                upload_info,
                stream,
                database,
                &state,
                existing_nar,
            )
            .await;
        }
    }

    // New NAR or need to repair
    upload_path_new(username, cache, upload_info, stream, database, &state).await
}

/// Handles a chunked upload request.
async fn handle_chunked_upload(
    state: State,
    req_state: RequestState,
    headers: HeaderMap,
    upload_info: UploadPathNarInfo,
    mut stream: StreamReader<
        impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Unpin,
        bytes::Bytes,
    >,
) -> ServerResult<Json<UploadPathResult>> {
    use binix::api::v1::upload_path::{
        BINIX_CHUNK_HASH, BINIX_CHUNK_INDEX, BINIX_CHUNK_SESSION_ID, BINIX_CHUNK_TOTAL,
    };

    let database = state.database().await?;
    let cache_name = &upload_info.cache;

    let cache = req_state
        .auth
        .auth_cache(database, cache_name, |cache, permission| {
            permission.require_push()?;
            Ok(cache)
        })
        .await?;

    let session_id = headers
        .get(BINIX_CHUNK_SESSION_ID)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let chunk_index: u32 = headers
        .get(BINIX_CHUNK_INDEX)
        .ok_or_else(|| ErrorKind::RequestError(anyhow!("Missing chunk index header")))?
        .to_str()
        .map_err(|_| ErrorKind::RequestError(anyhow!("Invalid chunk index header")))?
        .parse()
        .map_err(|_| ErrorKind::RequestError(anyhow!("Invalid chunk index value")))?;

    let total_chunks: u32 = headers
        .get(BINIX_CHUNK_TOTAL)
        .ok_or_else(|| ErrorKind::RequestError(anyhow!("Missing total chunks header")))?
        .to_str()
        .map_err(|_| ErrorKind::RequestError(anyhow!("Invalid total chunks header")))?
        .parse()
        .map_err(|_| ErrorKind::RequestError(anyhow!("Invalid total chunks value")))?;

    let chunk_hash_header = headers
        .get(BINIX_CHUNK_HASH)
        .ok_or_else(|| ErrorKind::RequestError(anyhow!("Missing chunk hash header")))?
        .to_str()
        .map_err(|_| ErrorKind::RequestError(anyhow!("Invalid chunk hash header")))?
        .to_string();

    let session_id = if let Some(sid) = session_id {
        // Check if session exists
        if state
            .chunked_session_manager
            .session_exists(&sid, database)
            .await
            .map_err(|e| ErrorKind::RequestError(e.into()))?
        {
            // Validate that parameters match
            if state
                .chunked_session_manager
                .validate_session(&sid, total_chunks, upload_info.nar_size, database)
                .await
                .is_err()
            {
                return Err(ErrorKind::RequestError(anyhow!(
                    "Session parameters mismatch for session {}. Expected total_chunks: {}, nar_size: {}",
                    sid,
                    total_chunks,
                    upload_info.nar_size
                ))
                .into());
            }
        } else {
            state
                .chunked_session_manager
                .create_session(sid.clone(), cache.id, upload_info.clone(), total_chunks, database)
                .await
                .map_err(|e| ErrorKind::RequestError(e.into()))?;
        }
        sid
    } else {
        let new_id = Uuid::new_v4().to_string();
        state
            .chunked_session_manager
            .create_session(new_id.clone(), cache.id, upload_info.clone(), total_chunks, database)
            .await
            .map_err(|e| ErrorKind::RequestError(e.into()))?
    };

    // Read chunk data from stream
    let mut chunk_data = Vec::new();
    stream
        .read_to_end(&mut chunk_data)
        .await
        .map_err(ServerError::request_error)?;

    let chunk_bytes = Bytes::from(chunk_data);

    // Validate chunk hash
    let chunk_hash = {
        let mut hasher = Sha256::new();
        hasher.update(&chunk_bytes);
        let result = hasher.finalize();
        format!("sha256:{}", hex::encode(result))
    };

    if chunk_hash != chunk_hash_header {
        return Err(ErrorKind::RequestError(anyhow!(
            "Chunk hash mismatch: expected {}, got {}",
            chunk_hash_header,
            chunk_hash
        ))
        .into());
    }

    // Prepare chunk stream (don't update session state yet)
    let (reader, already_received) = state
        .chunked_session_manager
        .prepare_chunk_stream(&session_id, chunk_index, &chunk_bytes, database)
        .await
        .map_err(|e| ErrorKind::RequestError(e.into()))?;

    // If already received, skip processing and check if complete
    if already_received {
        let is_complete = state
            .chunked_session_manager
            .is_complete(&session_id, database)
            .await
            .map_err(|e| ErrorKind::RequestError(e.into()))?;

        if is_complete {
            return finalize_chunked_upload(session_id, database, &state, req_state.auth.username().map(str::to_string)).await;
        } else {
            return Ok(Json(UploadPathResult {
                kind: UploadPathResultKind::ChunkReceived,
                file_size: None,
                frac_deduplicated: None,
            }));
        }
    }

    // Process through CDC chunking
    let compression_config = &state.config.compression;
    let compression_type = compression_config.r#type;
    let compression_level = compression_config.level();

    let cdc_chunks = process_cdc_chunks(
        reader,
        chunk_index,
        compression_type,
        compression_level,
        database.clone(),
        state.clone(),
    )
    .await?;

    // Mark chunk as successfully received and store CDC chunks
    state
        .chunked_session_manager
        .complete_chunk(&session_id, chunk_index, &chunk_bytes, cdc_chunks, database)
        .await
        .map_err(|e| ErrorKind::RequestError(e.into()))?;

    tracing::debug!(
        "Chunk {} completed for session {}, checking if upload is complete",
        chunk_index,
        session_id
    );

    // Check if upload is complete
    let is_complete = state
        .chunked_session_manager
        .is_complete(&session_id, database)
        .await
        .map_err(|e| ErrorKind::RequestError(e.into()))?;

    if is_complete {
        tracing::info!("Upload complete for session {}, finalizing", session_id);
        finalize_chunked_upload(session_id, database, &state, req_state.auth.username().map(str::to_string)).await
    } else {
        Ok(Json(UploadPathResult {
            kind: UploadPathResultKind::ChunkReceived,
            file_size: None,
            frac_deduplicated: None,
        }))
    }
}

/// Processes a client chunk through CDC chunking and uploads CDC chunks to S3.
async fn process_cdc_chunks(
    stream: Box<dyn AsyncBufRead + Send + Unpin>,
    source_chunk_index: u32,
    compression_type: CompressionType,
    compression_level: CompressionLevel,
    database: DatabaseConnection,
    state: State,
) -> ServerResult<Vec<crate::chunked_session::StoredChunkInfo>> {
    // Stream through CDC chunker
    let chunking_config = &state.config.chunking;
    let chunk_stream = chunk_stream(
        stream,
        chunking_config.min_size,
        chunking_config.avg_size,
        chunking_config.max_size,
    );

    // Process CDC chunks in parallel with bounded concurrency
    // Use buffered() to maintain order (required for correct position tracking)
    let concurrency = state.config.chunked_upload.cdc_upload_concurrency;
    let cdc_chunks: Vec<_> = chunk_stream
        .enumerate()
        .map(|(position, cdc_chunk_result)| {
            let compression_type = compression_type;
            let compression_level = compression_level;
            let database = database.clone();
            let state = state.clone();

            async move {
                let cdc_chunk_bytes = cdc_chunk_result.map_err(ServerError::request_error)?;

                // Upload CDC chunk to S3
                let result = upload_chunk(
                    ChunkData::Bytes(cdc_chunk_bytes),
                    compression_type,
                    compression_level,
                    database,
                    state,
                    false, // Don't require proof of possession - we created this data
                )
                .await?;

                Ok::<_, ServerError>(crate::chunked_session::StoredChunkInfo {
                    source_chunk_index,
                    position_in_chunk: position as u32,
                    chunk_id: result.guard.id,
                    chunk_hash: result.guard.chunk_hash.clone(),
                    compression: result.guard.compression.clone(),
                })
            }
        })
        .buffered(concurrency)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<ServerResult<Vec<_>>>()?;

    Ok(cdc_chunks)
}

/// Finalizes a chunked upload after all chunks have been received.
async fn finalize_chunked_upload(
    session_id: String,
    database: &DatabaseConnection,
    state: &State,
    username: Option<String>,
) -> ServerResult<Json<UploadPathResult>> {
    // Finalize session - gets CDC chunks
    let (cache_id, nar_info, mut cdc_chunks) = state
        .chunked_session_manager
        .finalize_session(&session_id, database)
        .await
        .map_err(|e| ErrorKind::RequestError(e.into()))?;

    // Sort CDC chunks by source chunk index and position to ensure correct order
    cdc_chunks.sort_by(|a, b| {
        a.source_chunk_index
            .cmp(&b.source_chunk_index)
            .then(a.position_in_chunk.cmp(&b.position_in_chunk))
    });

    let compression_config = &state.config.compression;
    let compression_type = compression_config.r#type;
    let compression: Compression = compression_type.into();

    // Check if NAR already exists with all chunks present
    if let Some(existing_nar) = database.find_and_lock_nar(&nar_info.nar_hash).await? {
        let missing_chunk = ChunkRef::find()
            .filter(chunkref::Column::NarId.eq(existing_nar.id))
            .filter(chunkref::Column::ChunkId.is_null())
            .limit(1)
            .one(database)
            .await
            .map_err(ServerError::database_error)?;

        if missing_chunk.is_none() {
            // Download CDC chunks and validate hash (unless skipped)
            if !state.config.chunked_upload.skip_nar_hash_validation {
                validate_nar_hash_from_cdc_chunks(&cdc_chunks, &nar_info, database, state).await?;
            }

            // NAR is complete, create object mapping
            let txn = database
                .begin()
                .await
                .map_err(ServerError::database_error)?;

            Object::insert({
                let mut new_object = nar_info.to_active_model();
                new_object.cache_id = Set(cache_id);
                new_object.nar_id = Set(existing_nar.id);
                new_object.created_at = Set(Utc::now());
                new_object.created_by = Set(username.clone());
                new_object
            })
            .on_conflict_do_update()
            .exec(&txn)
            .await
            .map_err(ServerError::database_error)?;

            txn.commit().await.map_err(ServerError::database_error)?;

            return Ok(Json(UploadPathResult {
                kind: UploadPathResultKind::Deduplicated,
                file_size: Some(nar_info.nar_size),
                frac_deduplicated: Some(1.0),
            }));
        }
    }

    // Create new NAR
    let nar_size_db = i64::try_from(nar_info.nar_size).map_err(ServerError::request_error)?;

    let nar_id = {
        let model = nar::ActiveModel {
            state: Set(NarState::PendingUpload),
            compression: Set(compression.to_string()),
            nar_hash: Set(nar_info.nar_hash.to_typed_base16()),
            nar_size: Set(nar_size_db),
            num_chunks: Set(0),
            created_at: Set(Utc::now()),
            ..Default::default()
        };

        let insertion = Nar::insert(model)
            .exec(database)
            .await
            .map_err(ServerError::database_error)?;

        insertion.last_insert_id
    };

    let cleanup = Finally::new({
        let database = database.clone();
        let nar_model = nar::ActiveModel {
            id: Set(nar_id),
            ..Default::default()
        };

        async move {
            tracing::warn!("Error occurred - Cleaning up NAR entry");

            if let Err(e) = Nar::delete(nar_model).exec(&database).await {
                tracing::warn!("Failed to unregister failed NAR: {}", e);
            }
        }
    });

    // Download CDC chunks and validate hash (unless skipped)
    if !state.config.chunked_upload.skip_nar_hash_validation {
        validate_nar_hash_from_cdc_chunks(&cdc_chunks, &nar_info, database, state).await?;
    }

    let txn = database
        .begin()
        .await
        .map_err(ServerError::database_error)?;

    // Create chunk references in batches to avoid PostgreSQL parameter limit
    const BATCH_SIZE: usize = 1000;
    for (batch_idx, batch) in cdc_chunks.chunks(BATCH_SIZE).enumerate() {
        let start_seq = batch_idx * BATCH_SIZE;

        let chunk_refs: Vec<chunkref::ActiveModel> = batch
            .iter()
            .enumerate()
            .map(|(i, chunk_info)| chunkref::ActiveModel {
                nar_id: Set(nar_id),
                seq: Set((start_seq + i) as i32),
                chunk_id: Set(Some(chunk_info.chunk_id)),
                chunk_hash: Set(chunk_info.chunk_hash.clone()),
                compression: Set(chunk_info.compression.clone()),
                ..Default::default()
            })
            .collect();

        ChunkRef::insert_many(chunk_refs)
            .exec(&txn)
            .await
            .map_err(ServerError::database_error)?;
    }

    // Mark NAR as valid
    Nar::update(nar::ActiveModel {
        id: Set(nar_id),
        state: Set(NarState::Valid),
        num_chunks: Set(cdc_chunks.len() as i32),
        ..Default::default()
    })
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?;

    // Create object mapping
    Object::insert({
        let mut new_object = nar_info.to_active_model();
        new_object.cache_id = Set(cache_id);
        new_object.nar_id = Set(nar_id);
        new_object.created_at = Set(Utc::now());
        new_object.created_by = Set(username);
        new_object
    })
    .on_conflict_do_update()
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?;

    txn.commit().await.map_err(ServerError::database_error)?;

    cleanup.cancel();

    Ok(Json(UploadPathResult {
        kind: UploadPathResultKind::Uploaded,
        file_size: Some(nar_info.nar_size),
        frac_deduplicated: None,
    }))
}

/// Downloads CDC chunks and validates NAR hash.
///
/// Uses bounded concurrency to limit memory usage - only a few chunks
/// are in memory at any time.
async fn validate_nar_hash_from_cdc_chunks(
    cdc_chunks: &[crate::chunked_session::StoredChunkInfo],
    nar_info: &UploadPathNarInfo,
    database: &DatabaseConnection,
    state: &State,
) -> ServerResult<()> {
    use async_compression::tokio::bufread::{BrotliDecoder, XzDecoder, ZstdDecoder};
    use futures::stream::StreamExt;

    let mut hasher = Sha256::new();
    let mut total_size = 0usize;

    // Process chunks in order with bounded concurrency (5 chunks buffered ahead)
    let mut chunk_stream = futures::stream::iter(cdc_chunks.iter().cloned())
        .map(|chunk_info| {
            let database = database.clone();
            let state = state.clone();

            async move {
                let chunk = Chunk::find_by_id(chunk_info.chunk_id)
                    .one(&database)
                    .await
                    .map_err(ServerError::database_error)?
                    .ok_or_else(|| {
                        ErrorKind::RequestError(anyhow!(
                            "CDC chunk {} not found",
                            chunk_info.chunk_id
                        ))
                    })?;

                let storage = state.storage().await?;
                let download = storage
                    .download_file_db(&chunk.remote_file.0, true)
                    .await
                    .map_err(|e| {
                        tracing::error!(
                            "Failed to download CDC chunk {}: {}",
                            chunk_info.chunk_id,
                            e
                        );
                        e
                    })?;

                let reader: Box<dyn AsyncBufRead + Send + Unpin> = match download {
                    crate::storage::Download::AsyncRead(r) => {
                        Box::new(tokio::io::BufReader::new(r))
                    }
                    crate::storage::Download::Url(_) => {
                        return Err(ErrorKind::RequestError(anyhow!(
                            "Expected stream, got URL from storage backend"
                        ))
                        .into());
                    }
                };

                // Decompress based on the chunk's stored compression type
                let decompressed: Box<dyn AsyncRead + Send + Unpin> =
                    match chunk.compression.as_str() {
                        "none" => Box::new(reader),
                        "br" => Box::new(BrotliDecoder::new(reader)),
                        "zstd" => Box::new(ZstdDecoder::new(reader)),
                        "xz" => Box::new(XzDecoder::new(reader)),
                        other => {
                            return Err(ErrorKind::RequestError(anyhow!(
                                "Unknown compression type: {}",
                                other
                            ))
                            .into());
                        }
                    };

                let mut chunk_data = Vec::new();
                let mut buf_reader = tokio::io::BufReader::new(decompressed);
                buf_reader
                    .read_to_end(&mut chunk_data)
                    .await
                    .map_err(ServerError::request_error)?;

                Ok::<_, ServerError>(chunk_data)
            }
        })
        .buffered(5);

    // Process each chunk immediately as it completes (in order)
    while let Some(result) = chunk_stream.next().await {
        let chunk_data = result?;
        total_size += chunk_data.len();
        hasher.update(&chunk_data);
    }

    // Compute final hash
    let hash_result = hasher.finalize();
    let computed_hash = Hash::Sha256(hash_result.as_slice().try_into().unwrap());

    // Validate
    if computed_hash != nar_info.nar_hash || total_size != nar_info.nar_size {
        return Err(ErrorKind::RequestError(anyhow!(
            "NAR hash validation failed: expected {}, got {}, size: expected {}, got {}",
            nar_info.nar_hash.to_typed_base16(),
            computed_hash.to_typed_base16(),
            nar_info.nar_size,
            total_size
        ))
        .into());
    }

    Ok(())
}

/// Uploads a path when there is already a matching NAR in the global cache.
async fn upload_path_dedup(
    username: Option<String>,
    cache: cache::Model,
    upload_info: UploadPathNarInfo,
    stream: impl AsyncBufRead + Unpin,
    database: &DatabaseConnection,
    state: &State,
    existing_nar: NarGuard,
) -> ServerResult<Json<UploadPathResult>> {
    if state.config.require_proof_of_possession {
        let (mut stream, nar_compute) = HashReader::new(stream, Sha256::new());
        tokio::io::copy(&mut stream, &mut tokio::io::sink())
            .await
            .map_err(ServerError::request_error)?;

        // FIXME: errors
        let (nar_hash, nar_size) = nar_compute.get().unwrap();
        let nar_hash = Hash::Sha256(nar_hash.as_slice().try_into().unwrap());

        // Confirm that the NAR Hash and Size are correct
        if nar_hash.to_typed_base16() != existing_nar.nar_hash
            || *nar_size != upload_info.nar_size
            || *nar_size != existing_nar.nar_size as usize
        {
            return Err(ErrorKind::RequestError(anyhow!("Bad NAR Hash or Size")).into());
        }
    }

    // Finally...
    let txn = database
        .begin()
        .await
        .map_err(ServerError::database_error)?;

    // Create a mapping granting the local cache access to the NAR
    Object::insert({
        let mut new_object = upload_info.to_active_model();
        new_object.cache_id = Set(cache.id);
        new_object.nar_id = Set(existing_nar.id);
        new_object.created_at = Set(Utc::now());
        new_object.created_by = Set(username);
        new_object
    })
    .on_conflict_do_update()
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?;

    // Also mark the NAR as complete again
    //
    // This is racy (a chunkref might have been broken in the
    // meantime), but it's okay since it's just a hint to
    // `get-missing-paths` so clients don't attempt to upload
    // again. Also see the comments in `server/src/database/entity/nar.rs`.
    Nar::update(nar::ActiveModel {
        id: Set(existing_nar.id),
        completeness_hint: Set(true),
        ..Default::default()
    })
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?;

    txn.commit().await.map_err(ServerError::database_error)?;

    // Ensure it's not unlocked earlier
    drop(existing_nar);

    Ok(Json(UploadPathResult {
        kind: UploadPathResultKind::Deduplicated,
        file_size: None, // TODO: Sum the chunks
        frac_deduplicated: None,
    }))
}

/// Uploads a path when there is no matching NAR in the global cache.
///
/// It's okay if some other client races to upload the same NAR before
/// us. The `nar` table can hold duplicate NARs which can be deduplicated
/// in a background process.
async fn upload_path_new(
    username: Option<String>,
    cache: cache::Model,
    upload_info: UploadPathNarInfo,
    stream: impl AsyncBufRead + Send + Unpin + 'static,
    database: &DatabaseConnection,
    state: &State,
) -> ServerResult<Json<UploadPathResult>> {
    let nar_size_threshold = state.config.chunking.nar_size_threshold;

    if nar_size_threshold == 0 || upload_info.nar_size < nar_size_threshold {
        upload_path_new_unchunked(username, cache, upload_info, stream, database, state).await
    } else {
        upload_path_new_chunked(username, cache, upload_info, stream, database, state).await
    }
}

/// Uploads a path when there is no matching NAR in the global cache (chunked).
async fn upload_path_new_chunked(
    username: Option<String>,
    cache: cache::Model,
    upload_info: UploadPathNarInfo,
    stream: impl AsyncBufRead + Send + Unpin + 'static,
    database: &DatabaseConnection,
    state: &State,
) -> ServerResult<Json<UploadPathResult>> {
    let chunking_config = &state.config.chunking;
    let compression_config = &state.config.compression;
    let compression_type = compression_config.r#type;
    let compression_level = compression_config.level();
    let compression: Compression = compression_type.into();

    let nar_size_db = i64::try_from(upload_info.nar_size).map_err(ServerError::request_error)?;

    // Create a pending NAR entry
    let nar_id = {
        let model = nar::ActiveModel {
            state: Set(NarState::PendingUpload),
            compression: Set(compression.to_string()),

            nar_hash: Set(upload_info.nar_hash.to_typed_base16()),
            nar_size: Set(nar_size_db),

            num_chunks: Set(0),

            created_at: Set(Utc::now()),
            ..Default::default()
        };

        let insertion = Nar::insert(model)
            .exec(database)
            .await
            .map_err(ServerError::database_error)?;

        insertion.last_insert_id
    };

    let cleanup = Finally::new({
        let database = database.clone();
        let nar_model = nar::ActiveModel {
            id: Set(nar_id),
            ..Default::default()
        };

        async move {
            tracing::warn!("Error occurred - Cleaning up NAR entry");

            if let Err(e) = Nar::delete(nar_model).exec(&database).await {
                tracing::warn!("Failed to unregister failed NAR: {}", e);
            }
        }
    });

    let stream = stream.take(upload_info.nar_size as u64);
    let (stream, nar_compute) = HashReader::new(stream, Sha256::new());
    let mut chunks = chunk_stream(
        stream,
        chunking_config.min_size,
        chunking_config.avg_size,
        chunking_config.max_size,
    );

    let upload_chunk_limit = Arc::new(Semaphore::new(CONCURRENT_CHUNK_UPLOADS));
    let mut futures = Vec::new();

    let mut chunk_idx = 0;
    while let Some(bytes) = chunks.next().await {
        let bytes = bytes.map_err(ServerError::request_error)?;
        let data = ChunkData::Bytes(bytes);

        // Wait for a permit before spawning
        //
        // We want to block the receive process as well, otherwise it stays ahead and
        // consumes too much memory
        let permit = upload_chunk_limit.clone().acquire_owned().await.unwrap();
        futures.push({
            let database = database.clone();
            let state = state.clone();
            let require_proof_of_possession = state.config.require_proof_of_possession;

            spawn(async move {
                let chunk = upload_chunk(
                    data,
                    compression_type,
                    compression_level,
                    database.clone(),
                    state,
                    require_proof_of_possession,
                )
                .await?;

                // Create mapping from the NAR to the chunk
                ChunkRef::insert(chunkref::ActiveModel {
                    nar_id: Set(nar_id),
                    seq: Set(chunk_idx),
                    chunk_id: Set(Some(chunk.guard.id)),
                    chunk_hash: Set(chunk.guard.chunk_hash.clone()),
                    compression: Set(chunk.guard.compression.clone()),
                    ..Default::default()
                })
                .exec(&database)
                .await
                .map_err(ServerError::database_error)?;

                drop(permit);
                Ok(chunk)
            })
        });

        chunk_idx += 1;
    }

    // Confirm that the NAR Hash and Size are correct
    // FIXME: errors
    let (nar_hash, nar_size) = nar_compute.get().unwrap();
    let nar_hash = Hash::Sha256(nar_hash.as_slice().try_into().unwrap());

    if nar_hash != upload_info.nar_hash || *nar_size != upload_info.nar_size {
        return Err(ErrorKind::RequestError(anyhow!("Bad NAR Hash or Size")).into());
    }

    // Wait for all uploads to complete
    let chunks: Vec<UploadChunkResult> = join_all(futures)
        .await
        .into_iter()
        .map(|join_result| join_result.unwrap())
        .collect::<ServerResult<Vec<_>>>()?;

    let (file_size, deduplicated_size) =
        chunks
            .iter()
            .fold((0, 0), |(file_size, deduplicated_size), c| {
                (
                    file_size + c.guard.file_size.unwrap() as usize,
                    if c.deduplicated {
                        deduplicated_size + c.guard.chunk_size as usize
                    } else {
                        deduplicated_size
                    },
                )
            });

    // Finally...
    let txn = database
        .begin()
        .await
        .map_err(ServerError::database_error)?;

    // Set num_chunks and mark the NAR as Valid
    Nar::update(nar::ActiveModel {
        id: Set(nar_id),
        state: Set(NarState::Valid),
        num_chunks: Set(chunks.len() as i32),
        ..Default::default()
    })
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?;

    // Create a mapping granting the local cache access to the NAR
    Object::insert({
        let mut new_object = upload_info.to_active_model();
        new_object.cache_id = Set(cache.id);
        new_object.nar_id = Set(nar_id);
        new_object.created_at = Set(Utc::now());
        new_object.created_by = Set(username);
        new_object
    })
    .on_conflict_do_update()
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?;

    txn.commit().await.map_err(ServerError::database_error)?;

    cleanup.cancel();

    Ok(Json(UploadPathResult {
        kind: UploadPathResultKind::Uploaded,
        file_size: Some(file_size),

        // Currently, frac_deduplicated is computed from size before compression
        frac_deduplicated: Some(deduplicated_size as f64 / *nar_size as f64),
    }))
}

/// Uploads a path when there is no matching NAR in the global cache (unchunked).
///
/// We upload the entire NAR as a single chunk.
async fn upload_path_new_unchunked(
    username: Option<String>,
    cache: cache::Model,
    upload_info: UploadPathNarInfo,
    stream: impl AsyncBufRead + Send + Unpin + 'static,
    database: &DatabaseConnection,
    state: &State,
) -> ServerResult<Json<UploadPathResult>> {
    let compression_config = &state.config.compression;
    let compression_type = compression_config.r#type;
    let compression: Compression = compression_type.into();

    // Upload the entire NAR as a single chunk
    let stream = stream.take(upload_info.nar_size as u64);
    let data = ChunkData::Stream(
        Box::new(stream),
        upload_info.nar_hash.clone(),
        upload_info.nar_size,
    );
    let chunk = upload_chunk(
        data,
        compression_type,
        compression_config.level(),
        database.clone(),
        state.clone(),
        state.config.require_proof_of_possession,
    )
    .await?;
    let file_size = chunk.guard.file_size.unwrap() as usize;

    // Finally...
    let txn = database
        .begin()
        .await
        .map_err(ServerError::database_error)?;

    // Create a NAR entry
    let nar_id = {
        let model = nar::ActiveModel {
            state: Set(NarState::Valid),
            compression: Set(compression.to_string()),

            nar_hash: Set(upload_info.nar_hash.to_typed_base16()),
            nar_size: Set(chunk.guard.chunk_size),

            num_chunks: Set(1),

            created_at: Set(Utc::now()),
            ..Default::default()
        };

        let insertion = Nar::insert(model)
            .exec(&txn)
            .await
            .map_err(ServerError::database_error)?;

        insertion.last_insert_id
    };

    // Create a mapping from the NAR to the chunk
    ChunkRef::insert(chunkref::ActiveModel {
        nar_id: Set(nar_id),
        seq: Set(0),
        chunk_id: Set(Some(chunk.guard.id)),
        chunk_hash: Set(upload_info.nar_hash.to_typed_base16()),
        compression: Set(compression.to_string()),
        ..Default::default()
    })
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?;

    // Create a mapping granting the local cache access to the NAR
    Object::insert({
        let mut new_object = upload_info.to_active_model();
        new_object.cache_id = Set(cache.id);
        new_object.nar_id = Set(nar_id);
        new_object.created_at = Set(Utc::now());
        new_object.created_by = Set(username);
        new_object
    })
    .on_conflict_do_update()
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?;

    txn.commit().await.map_err(ServerError::database_error)?;

    Ok(Json(UploadPathResult {
        kind: UploadPathResultKind::Uploaded,
        file_size: Some(file_size),
        frac_deduplicated: None,
    }))
}

/// Uploads a chunk with the desired compression.
///
/// This will automatically perform deduplication if the chunk exists.
async fn upload_chunk(
    data: ChunkData,
    compression_type: CompressionType,
    compression_level: CompressionLevel,
    database: DatabaseConnection,
    state: State,
    require_proof_of_possession: bool,
) -> ServerResult<UploadChunkResult> {
    let compression: Compression = compression_type.into();

    let given_chunk_hash = data.hash();
    let given_chunk_size = data.size();

    if let Some(existing_chunk) = database
        .find_and_lock_chunk(&given_chunk_hash, compression)
        .await?
    {
        // There's an existing chunk matching the hash
        if require_proof_of_possession && !data.is_hash_trusted() {
            let stream = data.into_async_buf_read();

            let (mut stream, nar_compute) = HashReader::new(stream, Sha256::new());
            tokio::io::copy(&mut stream, &mut tokio::io::sink())
                .await
                .map_err(ServerError::request_error)?;

            // FIXME: errors
            let (nar_hash, nar_size) = nar_compute.get().unwrap();
            let nar_hash = Hash::Sha256(nar_hash.as_slice().try_into().unwrap());

            // Confirm that the NAR Hash and Size are correct
            if nar_hash.to_typed_base16() != existing_chunk.chunk_hash
                || *nar_size != given_chunk_size
                || *nar_size != existing_chunk.chunk_size as usize
            {
                return Err(ErrorKind::RequestError(anyhow!("Bad chunk hash or size")).into());
            }
        }

        return Ok(UploadChunkResult {
            guard: existing_chunk,
            deduplicated: true,
        });
    }

    let key = format!("{}.chunk", Uuid::new_v4());

    let backend = state.storage().await?;
    let remote_file = backend.make_db_reference(key.clone()).await?;
    let remote_file_id = remote_file.remote_file_id();

    let chunk_size_db = i64::try_from(given_chunk_size).map_err(ServerError::request_error)?;

    let chunk_id = {
        let model = chunk::ActiveModel {
            state: Set(ChunkState::PendingUpload),
            compression: Set(compression.to_string()),

            // Untrusted data - To be confirmed later
            chunk_hash: Set(given_chunk_hash.to_typed_base16()),
            chunk_size: Set(chunk_size_db),

            remote_file: Set(DbJson(remote_file)),
            remote_file_id: Set(remote_file_id),

            created_at: Set(Utc::now()),
            ..Default::default()
        };

        let insertion = Chunk::insert(model)
            .exec(&database)
            .await
            .map_err(ServerError::database_error)?;

        insertion.last_insert_id
    };

    let cleanup = Finally::new({
        let database = database.clone();
        let chunk_model = chunk::ActiveModel {
            id: Set(chunk_id),
            ..Default::default()
        };
        let backend = backend.clone();
        let key = key.clone();

        async move {
            tracing::warn!("Error occurred - Cleaning up uploaded file and chunk entry");

            if let Err(e) = backend.delete_file(key).await {
                tracing::warn!("Failed to clean up failed upload: {}", e);
            }

            if let Err(e) = Chunk::delete(chunk_model).exec(&database).await {
                tracing::warn!("Failed to unregister failed chunk: {}", e);
            }
        }
    });

    // Compress and stream to the storage backend
    let compressor = get_compressor_fn(compression_type, compression_level);
    let mut stream = CompressionStream::new(data.into_async_buf_read(), compressor);

    backend
        .upload_file(key, stream.stream())
        .await
        .map_err(ServerError::storage_error)?;

    // Confirm that the chunk hash is correct
    let (chunk_hash, chunk_size) = stream.nar_hash_and_size().unwrap();
    let (file_hash, file_size) = stream.file_hash_and_size().unwrap();

    let chunk_hash = Hash::Sha256(chunk_hash.as_slice().try_into().unwrap());
    let file_hash = Hash::Sha256(file_hash.as_slice().try_into().unwrap());

    if chunk_hash != given_chunk_hash || *chunk_size != given_chunk_size {
        return Err(ErrorKind::RequestError(anyhow!("Bad chunk hash or size")).into());
    }

    // Finally...
    let txn = database
        .begin()
        .await
        .map_err(ServerError::database_error)?;

    // Update the file hash and size, and set the chunk to valid
    let file_size_db = i64::try_from(*file_size).map_err(ServerError::request_error)?;
    let chunk = Chunk::update(chunk::ActiveModel {
        id: Set(chunk_id),
        state: Set(ChunkState::Valid),
        file_hash: Set(Some(file_hash.to_typed_base16())),
        file_size: Set(Some(file_size_db)),
        holders_count: Set(1),
        ..Default::default()
    })
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?;

    // Also repair broken chunk references pointing at the same chunk
    let repaired = ChunkRef::update_many()
        .col_expr(chunkref::Column::ChunkId, Expr::value(chunk_id))
        .filter(chunkref::Column::ChunkId.is_null())
        .filter(chunkref::Column::ChunkHash.eq(chunk_hash.to_typed_base16()))
        .filter(chunkref::Column::Compression.eq(compression.to_string()))
        .exec(&txn)
        .await
        .map_err(ServerError::database_error)?;

    txn.commit().await.map_err(ServerError::database_error)?;

    cleanup.cancel();

    tracing::debug!("Repaired {} chunkrefs", repaired.rows_affected);

    let guard = ChunkGuard::from_locked(database.clone(), chunk);

    Ok(UploadChunkResult {
        guard,
        deduplicated: false,
    })
}

/// Returns a compressor function that takes some stream as input.
fn get_compressor_fn<C: AsyncBufRead + Unpin + Send + 'static>(
    ctype: CompressionType,
    level: CompressionLevel,
) -> CompressorFn<C> {
    match ctype {
        CompressionType::None => Box::new(|c| Box::new(c)),
        CompressionType::Brotli => {
            Box::new(move |s| Box::new(BrotliEncoder::with_quality(s, level)))
        }
        CompressionType::Zstd => Box::new(move |s| Box::new(ZstdEncoder::with_quality(s, level))),
        CompressionType::Xz => Box::new(move |s| Box::new(XzEncoder::with_quality(s, level))),
    }
}

impl ChunkData {
    /// Returns the potentially-incorrect hash of the chunk.
    fn hash(&self) -> Hash {
        match self {
            Self::Bytes(bytes) => {
                let mut hasher = Sha256::new();
                hasher.update(bytes);
                let hash = hasher.finalize();
                Hash::Sha256(hash.as_slice().try_into().unwrap())
            }
            Self::Stream(_, hash, _) => hash.clone(),
        }
    }

    /// Returns the potentially-incorrect size of the chunk.
    fn size(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
            Self::Stream(_, _, size) => *size,
        }
    }

    /// Returns whether the hash is trusted.
    fn is_hash_trusted(&self) -> bool {
        matches!(self, ChunkData::Bytes(_))
    }

    /// Turns the data into an AsyncBufRead.
    fn into_async_buf_read(self) -> Box<dyn AsyncBufRead + Unpin + Send> {
        match self {
            Self::Bytes(bytes) => Box::new(Cursor::new(bytes)),
            Self::Stream(stream, _, _) => stream,
        }
    }
}

impl UploadPathNarInfoExt for UploadPathNarInfo {
    fn to_active_model(&self) -> object::ActiveModel {
        object::ActiveModel {
            store_path_hash: Set(self.store_path_hash.to_string()),
            store_path: Set(self.store_path.clone()),
            references: Set(DbJson(self.references.clone())),
            deriver: Set(self.deriver.clone()),
            sigs: Set(DbJson(self.sigs.clone())),
            ca: Set(self.ca.clone()),
            ..Default::default()
        }
    }
}
