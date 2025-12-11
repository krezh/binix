use std::io;

use anyhow::anyhow;
use axum::{
    body::Body,
    extract::{Extension, Json},
    http::HeaderMap,
};
use chrono::Utc;
use futures::StreamExt;
use sea_orm::entity::prelude::*;
use sea_orm::ActiveValue::Set;
use sea_orm::{sea_query::Expr, TransactionTrait};
use sha2::{Digest, Sha256};
use tokio_util::io::StreamReader;
use tracing::instrument;
use uuid::Uuid;

use crate::compression::{get_compressor_fn, CompressionStream};
use crate::error::{ErrorKind, ServerError, ServerResult};
use crate::narinfo::Compression;
use crate::{RequestState, State};
use binix::api::v1::upload_chunk::{
    FinalizeNarRequest, FinalizeNarResponse, UploadChunkResponse, BINIX_CHUNK_HASH,
    BINIX_CHUNK_SIZE,
};
use binix::hash::Hash;
use binix::io::HashReader;
use binix::util::Finally;

use crate::database::entity::cache;
use crate::database::entity::chunk::{self, ChunkState, Entity as Chunk};
use crate::database::entity::chunkref::{self, Entity as ChunkRef};
use crate::database::entity::nar::{self, Entity as Nar, NarState};
use crate::database::entity::object::{self, Entity as Object, InsertExt};
use crate::database::entity::Json as DbJson;
use crate::database::BinixDatabase;

/// Uploads a single chunk.
///
/// The chunk hash and size are provided via headers. The server will
/// decompress and verify the hash before storing.
#[instrument(skip_all)]
#[axum_macros::debug_handler]
pub(crate) async fn upload_chunk(
    Extension(state): Extension<State>,
    Extension(_req_state): Extension<RequestState>,
    headers: HeaderMap,
    body: Body,
) -> ServerResult<Json<UploadChunkResponse>> {
    let compression_config = &state.config.compression;
    let compression_type = compression_config.r#type;
    let compression_level = compression_config.level();
    let compression: Compression = compression_type.into();

    // Extract chunk hash from header
    let chunk_hash_str = headers
        .get(BINIX_CHUNK_HASH)
        .ok_or_else(|| ErrorKind::RequestError(anyhow!("Missing chunk hash header")))?
        .to_str()
        .map_err(|_| ErrorKind::RequestError(anyhow!("Invalid chunk hash header")))?;

    let given_chunk_hash = Hash::from_typed(chunk_hash_str)
        .map_err(|e| ErrorKind::RequestError(anyhow!("Invalid chunk hash format: {}", e)))?;

    // Extract chunk size from header
    let chunk_size_str = headers
        .get(BINIX_CHUNK_SIZE)
        .ok_or_else(|| ErrorKind::RequestError(anyhow!("Missing chunk size header")))?
        .to_str()
        .map_err(|_| ErrorKind::RequestError(anyhow!("Invalid chunk size header")))?;

    let given_chunk_size: usize = chunk_size_str
        .parse()
        .map_err(|_| ErrorKind::RequestError(anyhow!("Invalid chunk size format")))?;

    let database = state.database().await?;

    // Check if chunk already exists
    if let Some(existing_chunk) = database
        .find_and_lock_chunk(&given_chunk_hash, compression)
        .await?
    {
        // Chunk already exists - verify the stream matches (proof of possession)
        let stream = body.into_data_stream();
        let stream = StreamReader::new(
            stream.map(|r| r.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))),
        );

        let (mut stream, compute) = HashReader::new(stream, Sha256::new());
        tokio::io::copy(&mut stream, &mut tokio::io::sink())
            .await
            .map_err(ServerError::request_error)?;

        let (hash_bytes, size) = compute.get().unwrap();
        let hash = Hash::Sha256(hash_bytes.as_slice().try_into().unwrap());

        if hash.to_typed_base16() != existing_chunk.chunk_hash
            || *size != given_chunk_size
            || *size != existing_chunk.chunk_size as usize
        {
            return Err(ErrorKind::RequestError(anyhow!("Bad chunk hash or size")).into());
        }

        return Ok(Json(UploadChunkResponse {
            deduplicated: true,
            chunk_id: existing_chunk.id,
            file_size: existing_chunk.file_size.map(|s| s as usize),
        }));
    }

    // Create new chunk
    let key = format!("{}.chunk", Uuid::new_v4());
    let backend = state.storage().await?;
    let remote_file = backend.make_db_reference(key.clone()).await?;
    let remote_file_id = remote_file.remote_file_id();

    let chunk_size_db = i64::try_from(given_chunk_size).map_err(ServerError::request_error)?;

    let chunk_id = {
        let model = chunk::ActiveModel {
            state: Set(ChunkState::PendingUpload),
            compression: Set(compression.to_string()),
            chunk_hash: Set(given_chunk_hash.to_typed_base16()),
            chunk_size: Set(chunk_size_db),
            remote_file: Set(DbJson(remote_file)),
            remote_file_id: Set(remote_file_id),
            created_at: Set(Utc::now()),
            ..Default::default()
        };

        let insertion = Chunk::insert(model)
            .exec(database)
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

    // Stream the body and compress it
    let stream = body.into_data_stream();
    let stream = StreamReader::new(
        stream.map(|r| r.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))),
    );

    let compressor = get_compressor_fn(compression_type, compression_level);
    let mut compression_stream = CompressionStream::new(stream, compressor);

    backend
        .upload_file(key, compression_stream.stream())
        .await
        .map_err(ServerError::storage_error)?;

    // Verify the chunk hash
    let (chunk_hash, chunk_size) = compression_stream.nar_hash_and_size().unwrap();
    let (file_hash, file_size) = compression_stream.file_hash_and_size().unwrap();

    let chunk_hash = Hash::Sha256(chunk_hash.as_slice().try_into().unwrap());
    let file_hash = Hash::Sha256(file_hash.as_slice().try_into().unwrap());

    if chunk_hash != given_chunk_hash || *chunk_size != given_chunk_size {
        return Err(ErrorKind::RequestError(anyhow!("Bad chunk hash or size")).into());
    }

    // Update chunk to valid state
    let txn = database
        .begin()
        .await
        .map_err(ServerError::database_error)?;

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

    // Repair broken chunk references
    ChunkRef::update_many()
        .col_expr(chunkref::Column::ChunkId, Expr::value(chunk_id))
        .filter(chunkref::Column::ChunkId.is_null())
        .filter(chunkref::Column::ChunkHash.eq(chunk_hash.to_typed_base16()))
        .filter(chunkref::Column::Compression.eq(compression.to_string()))
        .exec(&txn)
        .await
        .map_err(ServerError::database_error)?;

    txn.commit().await.map_err(ServerError::database_error)?;

    cleanup.cancel();

    Ok(Json(UploadChunkResponse {
        deduplicated: false,
        chunk_id: chunk.id,
        file_size: Some(*file_size),
    }))
}

/// Finalizes a chunked NAR upload by linking chunks to a NAR entry.
#[instrument(skip_all)]
#[axum_macros::debug_handler]
pub(crate) async fn finalize_nar(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Json(request): Json<FinalizeNarRequest>,
) -> ServerResult<Json<FinalizeNarResponse>> {
    let username = req_state.auth.username().map(|s| s.to_string());
    let database = state.database().await?;

    // Look up the cache
    let cache = cache::Entity::find()
        .filter(cache::Column::Name.eq(request.cache.as_str()))
        .one(database)
        .await
        .map_err(ServerError::database_error)?
        .ok_or(ErrorKind::NoSuchCache)?;

    let compression_config = &state.config.compression;
    let compression_type = compression_config.r#type;
    let compression: Compression = compression_type.into();

    let nar_size_db = i64::try_from(request.nar_size).map_err(ServerError::request_error)?;

    // Create NAR entry
    let nar_id = {
        let model = nar::ActiveModel {
            state: Set(NarState::PendingUpload),
            compression: Set(compression.to_string()),
            nar_hash: Set(request.nar_hash.to_typed_base16()),
            nar_size: Set(nar_size_db),
            num_chunks: Set(request.chunk_hashes.len() as i32),
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

    // Link chunks to NAR
    let txn = database
        .begin()
        .await
        .map_err(ServerError::database_error)?;

    let mut total_file_size = 0usize;
    let mut num_deduplicated = 0;

    for (seq, chunk_hash) in request.chunk_hashes.iter().enumerate() {
        // Find the chunk
        let chunk = Chunk::find()
            .filter(chunk::Column::ChunkHash.eq(chunk_hash.to_typed_base16()))
            .filter(chunk::Column::Compression.eq(compression.to_string()))
            .filter(chunk::Column::State.eq(ChunkState::Valid))
            .one(&txn)
            .await
            .map_err(ServerError::database_error)?
            .ok_or_else(|| {
                ErrorKind::RequestError(anyhow!(
                    "Chunk not found: {}",
                    chunk_hash.to_typed_base16()
                ))
            })?;

        // Check if this chunk was deduplicated (holders_count > 1 means it was reused)
        if chunk.holders_count > 1 {
            num_deduplicated += 1;
        }

        if let Some(file_size) = chunk.file_size {
            total_file_size += file_size as usize;
        }

        // Create chunk reference
        let chunkref_model = chunkref::ActiveModel {
            nar_id: Set(nar_id),
            seq: Set(seq as i32),
            chunk_id: Set(Some(chunk.id)),
            chunk_hash: Set(chunk_hash.to_typed_base16()),
            compression: Set(compression.to_string()),
            ..Default::default()
        };

        ChunkRef::insert(chunkref_model)
            .exec(&txn)
            .await
            .map_err(ServerError::database_error)?;
    }

    // Mark NAR as valid
    Nar::update(nar::ActiveModel {
        id: Set(nar_id),
        state: Set(NarState::Valid),
        completeness_hint: Set(true),
        ..Default::default()
    })
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?;

    // Create object mapping
    Object::insert(object::ActiveModel {
        cache_id: Set(cache.id),
        nar_id: Set(nar_id),
        store_path_hash: Set(request.store_path_hash),
        store_path: Set(request.store_path),
        references: Set(DbJson(request.references)),
        system: Set(request.system),
        deriver: Set(request.deriver),
        sigs: Set(DbJson(request.sigs)),
        ca: Set(request.ca),
        created_at: Set(Utc::now()),
        created_by: Set(username),
        ..Default::default()
    })
    .on_conflict_do_update()
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?;

    txn.commit().await.map_err(ServerError::database_error)?;

    cleanup.cancel();

    let frac_deduplicated = if !request.chunk_hashes.is_empty() {
        Some(num_deduplicated as f64 / request.chunk_hashes.len() as f64)
    } else {
        None
    };

    Ok(Json(FinalizeNarResponse {
        file_size: total_file_size,
        frac_deduplicated,
    }))
}
