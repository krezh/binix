use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures::stream::{Stream, TryStreamExt};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressStyle};
use tokio::io::AsyncReadExt;

use crate::api::ApiClient;
use crate::push::{PushConfig, UploadOutcome};
use binix::api::v1::upload_path::{UploadPathNarInfo, UploadPathResult, UploadPathResultKind};
use binix::cache::CacheName;
use binix::nix_store::{NixStore, ValidPathInfo};

/// Default chunk size: 64 MB.
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024 * 1024;

/// Chunking threshold: 64 MB.
pub const CHUNKING_THRESHOLD: usize = 64 * 1024 * 1024;

/// Reports progress as bytes are consumed from the stream.
struct ProgressStream {
    data: Bytes,
    pb: ProgressBar,
    chunk_size: usize,
}

impl ProgressStream {
    fn new(data: Bytes, pb: ProgressBar) -> Self {
        // Update every 512KB
        let chunk_size = 512 * 1024;
        Self { data, pb, chunk_size }
    }
}

impl Stream for ProgressStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.data.is_empty() {
            return Poll::Ready(None);
        }

        let chunk_size = self.chunk_size.min(self.data.len());
        let chunk = self.data.split_to(chunk_size);
        self.pb.inc(chunk.len() as u64);

        Poll::Ready(Some(Ok(chunk)))
    }
}

/// Uploads a path using chunked upload.
pub async fn upload_path_chunked(
    path_info: ValidPathInfo,
    store: Arc<NixStore>,
    api: ApiClient,
    cache: &CacheName,
    mp: MultiProgress,
    config: PushConfig,
) -> Result<UploadOutcome> {
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

    // Use NAR hash as session ID to ensure uniqueness per package
    let session_id = format!("chunk-{}", path_info.nar_hash.to_typed_base16());

    // Track completed chunks for progress display
    let chunks_completed = Arc::new(AtomicU32::new(0));

    // Create progress bar (hidden in quiet mode)
    let pb = if config.quiet {
        ProgressBar::hidden()
    } else {
        let pb = mp.add(ProgressBar::new(nar_size));
        let template = format!(
            "{{spinner}} {: <20.20} {{bar:40.green/blue}} {{msg}}",
            path_info.path.name(),
        );
        let style = ProgressStyle::with_template(&template)
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏✓")
            .progress_chars("━━─");
        pb.set_style(style);
        pb.set_message(format!("chunk 0/{} | {}", total_chunks, HumanBytes(0)));
        pb
    };

    let nar = store.nar_from_path(path_info.path.clone());
    let mut nar_reader = tokio_util::io::StreamReader::new(
        nar.map_ok(Bytes::from)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
    );

    use std::sync::Arc as StdArc;
    use tokio::sync::Semaphore;

    // Use semaphore to bound memory: only read next chunk when we have upload capacity.
    // This limits memory to chunk_upload_concurrency * chunk_size instead of all chunks.
    let semaphore = StdArc::new(Semaphore::new(config.chunk_upload_concurrency));
    let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();

    for chunk_index in 0..total_chunks {
        // Acquire permit BEFORE reading to bound memory usage.
        // This blocks if we have too many chunks in flight.
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        let bytes_read = (chunk_index as u64) * chunk_size;
        let remaining = nar_size - bytes_read;
        let chunk_capacity = chunk_size.min(remaining) as usize;
        let mut chunk_data = vec![0u8; chunk_capacity];

        nar_reader.read_exact(&mut chunk_data).await?;

        let chunk_bytes = Bytes::from(chunk_data);
        let chunk_hash = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(&chunk_bytes);
            let result = hasher.finalize();
            format!("sha256:{:x}", result)
        };

        let api = api.clone();
        let upload_info = upload_info.clone();
        let session_id = session_id.clone();
        let pb = pb.clone();
        let force_preamble = config.force_preamble;
        let result_tx = result_tx.clone();
        let chunks_completed = chunks_completed.clone();
        let chunk_len = chunk_bytes.len() as u64;

        tokio::spawn(async move {
            let result = upload_chunk_with_retry(
                &api,
                &upload_info,
                &session_id,
                chunk_index,
                total_chunks,
                &chunk_hash,
                chunk_bytes,
                force_preamble,
                3,
            )
            .await;

            // Update progress on completion
            if result.is_ok() {
                let completed = chunks_completed.fetch_add(1, Ordering::Relaxed) + 1;
                pb.inc(chunk_len);
                pb.set_message(format!(
                    "chunk {}/{} | {}",
                    completed,
                    total_chunks,
                    HumanBytes(pb.position())
                ));
            }

            // Drop permit after upload completes to allow next chunk to be read
            drop(permit);

            // Send result, ignore error if receiver dropped (early success)
            let _ = result_tx.send(result);
        });
    }

    // Drop our sender so the receiver knows when all sends are done
    drop(result_tx);

    // Collect results
    let mut final_result = None;
    while let Some(result) = result_rx.recv().await {
        let upload_result = result?;
        match upload_result.kind {
            UploadPathResultKind::Uploaded | UploadPathResultKind::Deduplicated => {
                final_result = Some(upload_result);
                break;
            }
            _ => {}
        }
    }

    pb.finish_and_clear();

    match final_result {
        Some(r) => match r.kind {
            UploadPathResultKind::Deduplicated => Ok(UploadOutcome::Deduplicated),
            _ => Ok(UploadOutcome::Uploaded),
        },
        None => Err(anyhow!("Upload completed but no final result received")),
    }
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
    // Hidden progress bar for stream - actual progress tracked in caller
    let pb = ProgressBar::hidden();

    loop {
        let stream = ProgressStream::new(chunk_data.clone(), pb.clone());

        match api
            .upload_path_chunked(
                nar_info,
                session_id,
                chunk_index,
                total_chunks,
                chunk_hash,
                stream,
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
