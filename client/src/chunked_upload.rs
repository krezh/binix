use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures::stream::TryStreamExt;
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use tokio::io::AsyncReadExt;

use crate::api::ApiClient;
use crate::push::PushConfig;
use binix::api::v1::upload_path::{UploadPathNarInfo, UploadPathResult, UploadPathResultKind};
use binix::cache::CacheName;
use binix::nix_store::{NixStore, ValidPathInfo};

/// Default chunk size: 64 MB.
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024 * 1024;

/// Chunking threshold: 64 MB.
pub const CHUNKING_THRESHOLD: usize = 64 * 1024 * 1024;

/// Uploads a path using chunked upload.
pub async fn upload_path_chunked(
    path_info: ValidPathInfo,
    store: Arc<NixStore>,
    api: ApiClient,
    cache: &CacheName,
    mp: MultiProgress,
    config: PushConfig,
) -> Result<()> {
    let nar_size = path_info.nar_size;
    let chunk_size = DEFAULT_CHUNK_SIZE as u64;
    let total_chunks = ((nar_size + chunk_size - 1) / chunk_size) as u32;

    let full_path = store
        .get_full_path(&path_info.path)
        .to_str()
        .ok_or_else(|| anyhow!("Path contains non-UTF-8"))?
        .to_string();

    let references = path_info
        .references
        .iter()
        .map(|pb| {
            pb.to_str()
                .ok_or_else(|| anyhow!("Reference contains non-UTF-8"))
                .map(|s| s.to_owned())
        })
        .collect::<Result<Vec<String>, anyhow::Error>>()?;

    let upload_info = UploadPathNarInfo {
        cache: cache.clone(),
        store_path_hash: path_info.path.to_hash(),
        store_path: full_path,
        references,
        system: None,
        deriver: None,
        sigs: path_info.sigs.clone(),
        ca: path_info.ca.clone(),
        nar_hash: path_info.nar_hash.clone(),
        nar_size: nar_size as usize,
    };

    let session_id = {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        format!("chunk-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
    };

    let pb = mp.add(ProgressBar::new(nar_size));
    let template = format!(
        "{{spinner}} {: <20.20} {{bar:40.green/blue}} {{human_bytes:10}} ({{average_speed}})",
        path_info.path.name(),
    );
    let style = ProgressStyle::with_template(&template)
        .unwrap()
        .tick_chars("🕛🕐🕑🕒🕓🕔🕕🕖🕗🕘🕙🕚✅")
        .progress_chars("██ ")
        .with_key("human_bytes", |state: &ProgressState, w: &mut dyn Write| {
            write!(w, "{}", HumanBytes(state.pos())).unwrap();
        })
        .with_key(
            "average_speed",
            |state: &ProgressState, w: &mut dyn Write| match (state.pos(), state.elapsed()) {
                (pos, elapsed) if elapsed > Duration::ZERO => {
                    let bytes_per_sec = pos as f64 / elapsed.as_secs_f64();
                    write!(w, "{}/s", HumanBytes(bytes_per_sec as u64)).unwrap();
                }
                _ => write!(w, "-").unwrap(),
            },
        );
    pb.set_style(style);

    let nar = store.nar_from_path(path_info.path.clone());
    let mut nar_reader = tokio_util::io::StreamReader::new(
        nar.map_ok(Bytes::from)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
    );

    for chunk_index in 0..total_chunks {
        let chunk_capacity = chunk_size.min(nar_size) as usize;
        let mut chunk_data = vec![0u8; chunk_capacity];
        let bytes_read = nar_reader.read(&mut chunk_data).await?;

        if bytes_read == 0 {
            break;
        }

        chunk_data.truncate(bytes_read);

        let chunk_bytes = Bytes::from(chunk_data);
        let chunk_hash = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(&chunk_bytes);
            let result = hasher.finalize();
            format!("sha256:{:x}", result)
        };

        let result = upload_chunk_with_retry(
            &api,
            &upload_info,
            &session_id,
            chunk_index,
            total_chunks,
            &chunk_hash,
            chunk_bytes.clone(),
            config.force_preamble,
            3,
        )
        .await?;

        pb.inc(chunk_bytes.len() as u64);

        match result.kind {
            UploadPathResultKind::ChunkReceived => {
                // Continue uploading
            }
            UploadPathResultKind::Uploaded | UploadPathResultKind::Deduplicated => {
                pb.finish();
                return Ok(());
            }
            _ => {
                pb.finish();
                return Ok(());
            }
        }
    }

    pb.finish();
    Ok(())
}

/// Uploads a single chunk with retry logic.
async fn upload_chunk_with_retry(
    api: &ApiClient,
    nar_info: &UploadPathNarInfo,
    session_id: &str,
    chunk_index: u32,
    total_chunks: u32,
    chunk_hash: &str,
    chunk_data: Bytes,
    force_preamble: bool,
    max_retries: usize,
) -> Result<UploadPathResult> {
    let mut attempt = 0;

    loop {
        match api
            .upload_path_chunked(
                nar_info,
                session_id,
                chunk_index,
                total_chunks,
                chunk_hash,
                chunk_data.clone(),
                force_preamble,
            )
            .await
        {
            Ok(Some(result)) => return Ok(result),
            Ok(None) => {
                return Err(anyhow!("Server returned no result for chunk upload"));
            }
            Err(_e) if attempt < max_retries => {
                attempt += 1;
                tokio::time::sleep(Duration::from_secs(2u64.pow(attempt as u32))).await;
            }
            Err(e) => return Err(e),
        }
    }
}
