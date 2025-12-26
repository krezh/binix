//! Store path uploader.
//!
//! There are two APIs: `Pusher` and `PushSession`.
//!
//! A `Pusher` simply dispatches `ValidPathInfo`s for workers to push. Use this
//! when you know all store paths to push beforehand. The push plan (closure, missing
//! paths, all path metadata) should be computed prior to pushing.
//!
//! A `PushSession`, on the other hand, accepts a stream of `StorePath`s and
//! takes care of retrieving the closure and path metadata. It automatically
//! batches expensive operations (closure computation, querying missing paths).
//! Use this when the list of store paths is streamed from some external
//! source (e.g., FS watcher, Unix Domain Socket) and a push plan cannot be
//! created statically.

use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use async_channel as channel;
use bytes::Bytes;
use futures::future::join_all;
use futures::stream::{Stream, TryStreamExt};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use tokio::sync::{mpsc, Mutex};
use tokio::task::{spawn, JoinHandle};
use tokio::time;

use crate::api::ApiClient;
use crate::chunked_upload;
use binix::api::v1::cache_config::CacheConfig;
use binix::api::v1::upload_path::{UploadPathNarInfo, UploadPathResult, UploadPathResultKind};
use binix::cache::CacheName;
use binix::error::BinixResult;
use binix::nix_store::{NixStore, StorePath, StorePathHash, ValidPathInfo};

type JobSender = channel::Sender<ValidPathInfo>;
type JobReceiver = channel::Receiver<ValidPathInfo>;

/// Configuration for pushing store paths.
#[derive(Clone, Copy, Debug)]
pub struct PushConfig {
    /// The number of workers to spawn.
    pub num_workers: usize,

    /// Whether to always include the upload info in the PUT payload.
    pub force_preamble: bool,

    /// Number of concurrent chunk uploads for large files.
    pub chunk_upload_concurrency: usize,

    /// Suppress progress bars, show only errors and final summary.
    pub quiet: bool,
}

/// Tracks statistics during a push operation.
#[derive(Debug)]
pub struct PushStats {
    pub paths_total: AtomicUsize,
    pub paths_uploaded: AtomicUsize,
    pub paths_deduplicated: AtomicUsize,
    pub paths_failed: AtomicUsize,
    pub bytes_total: AtomicU64,
    pub bytes_uploaded: AtomicU64,
    pub bytes_deduplicated: AtomicU64,
    pub start_time: Instant,
    pub errors: Mutex<Vec<(String, String)>>,
}

impl PushStats {
    pub fn new() -> Self {
        Self {
            paths_total: AtomicUsize::new(0),
            paths_uploaded: AtomicUsize::new(0),
            paths_deduplicated: AtomicUsize::new(0),
            paths_failed: AtomicUsize::new(0),
            bytes_total: AtomicU64::new(0),
            bytes_uploaded: AtomicU64::new(0),
            bytes_deduplicated: AtomicU64::new(0),
            start_time: Instant::now(),
            errors: Mutex::new(Vec::new()),
        }
    }
}

impl Default for PushStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for a push session.
#[derive(Clone, Copy, Debug)]
pub struct PushSessionConfig {
    /// Push the specified paths only and do not compute closures.
    pub no_closure: bool,

    /// Ignore the upstream cache filter.
    pub ignore_upstream_cache_filter: bool,
}

/// A handle to push store paths to a cache.
///
/// The caller is responsible for computing closures and
/// checking for paths that already exist on the remote
/// cache.
pub struct Pusher {
    api: ApiClient,
    store: Arc<NixStore>,
    cache: CacheName,
    cache_config: CacheConfig,
    workers: Vec<JoinHandle<()>>,
    sender: JobSender,
    stats: Arc<PushStats>,
    overall_bar: Arc<Mutex<Option<ProgressBar>>>,
    mp: MultiProgress,
    config: PushConfig,
}

/// A wrapper over a `Pusher` that accepts a stream of `StorePath`s.
///
/// Unlike a `Pusher`, a `PushSession` takes a stream of `StorePath`s
/// instead of `ValidPathInfo`s, taking care of retrieving the closure
/// and path metadata.
///
/// This is useful when the list of store paths is streamed from some
/// external source (e.g., FS watcher, Unix Domain Socket) and a push
/// plan cannot be computed statically.
///
/// ## Batching
///
/// Many store paths can be built in a short period of time, with each
/// having a big closure. It can be very inefficient if we were to compute
/// closure and query for missing paths for each individual path. This is
/// especially true if we have a lot of remote builders (e.g., `binix watch-store`
/// running alongside a beefy Hydra instance).
///
/// `PushSession` batches operations in order to minimize the number of
/// closure computations and API calls. It also remembers which paths already
/// exist on the remote cache. By default, it submits a batch if it's been 2
/// seconds since the last path is queued or it's been 10 seconds in total.
pub struct PushSession {
    /// Sender to the batching future.
    sender: channel::Sender<SessionQueueCommand>,

    /// Receiver of results.
    result_receiver: mpsc::Receiver<Result<HashMap<StorePath, Result<()>>>>,
}

enum SessionQueueCommand {
    Paths(Vec<StorePath>),
    Flush,
    Terminate,
}

enum SessionQueuePoll {
    Paths(Vec<StorePath>),
    Flush,
    Terminate,
    Closed,
    TimedOut,
}

#[derive(Debug)]
pub struct PushPlan {
    /// Store paths to push.
    pub store_path_map: HashMap<StorePathHash, ValidPathInfo>,

    /// The number of paths in the original full closure.
    pub num_all_paths: usize,

    /// Number of paths that have been filtered out because they are already cached.
    pub num_already_cached: usize,

    /// Number of paths that have been filtered out because they are signed by an upstream cache.
    pub num_upstream: usize,
}

/// Wrapper to update a progress bar as a NAR is streamed.
struct NarStreamProgress<S> {
    stream: S,
    bar: ProgressBar,
}

impl Pusher {
    pub fn new(
        store: Arc<NixStore>,
        api: ApiClient,
        cache: CacheName,
        cache_config: CacheConfig,
        mp: MultiProgress,
        config: PushConfig,
    ) -> Self {
        let (sender, receiver) = channel::unbounded();
        let stats = Arc::new(PushStats::new());
        let overall_bar = Arc::new(Mutex::new(None));
        let mut workers = Vec::new();

        for _ in 0..config.num_workers {
            workers.push(spawn(Self::worker(
                receiver.clone(),
                store.clone(),
                api.clone(),
                cache.clone(),
                mp.clone(),
                config,
                stats.clone(),
                overall_bar.clone(),
            )));
        }

        Self {
            api,
            store,
            cache,
            cache_config,
            workers,
            sender,
            stats,
            overall_bar,
            mp,
            config,
        }
    }

    /// Initializes the overall progress bar with total paths and bytes.
    pub async fn init_progress(&self, total_paths: usize, total_bytes: u64) {
        self.stats.paths_total.store(total_paths, Ordering::Relaxed);
        self.stats.bytes_total.store(total_bytes, Ordering::Relaxed);

        if self.config.quiet {
            return;
        }

        let template =
            "{spinner:.cyan} [{bar:30.green/dim}] {pos}/{len} paths | {bytes}/{total_bytes} | ETA {eta}";
        let style = ProgressStyle::with_template(template)
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
            .progress_chars("━━─");

        let bar = self.mp.insert(0, ProgressBar::new(total_bytes));
        bar.set_style(style);
        bar.set_length(total_bytes);
        *self.overall_bar.lock().await = Some(bar);
    }

    /// Queues a store path to be pushed.
    pub async fn queue(&self, path_info: ValidPathInfo) -> Result<()> {
        self.sender.send(path_info).await.map_err(|e| anyhow!(e))
    }

    /// Waits for all workers to terminate and prints summary.
    pub async fn wait(mut self) -> Result<()> {
        // Take sender to close channel
        let sender = std::mem::replace(&mut self.sender, channel::unbounded().0);
        drop(sender);

        // Wait for all workers
        let workers = std::mem::take(&mut self.workers);
        for worker in workers {
            let _ = worker.await;
        }

        // Finish overall progress bar
        if let Some(bar) = self.overall_bar.lock().await.as_ref() {
            bar.finish_and_clear();
        }

        // Print summary
        self.print_summary();

        // Return error if any paths failed
        let errors = self.stats.errors.lock().await;
        if !errors.is_empty() {
            return Err(anyhow!("{} path(s) failed to upload", errors.len()));
        }

        Ok(())
    }

    /// Prints a summary of the push operation.
    fn print_summary(&self) {
        let stats = &self.stats;
        let elapsed = stats.start_time.elapsed();
        let uploaded = stats.paths_uploaded.load(Ordering::Relaxed);
        let deduplicated = stats.paths_deduplicated.load(Ordering::Relaxed);
        let failed = stats.paths_failed.load(Ordering::Relaxed);
        let bytes_uploaded = stats.bytes_uploaded.load(Ordering::Relaxed);
        let bytes_dedup = stats.bytes_deduplicated.load(Ordering::Relaxed);

        // Only print summary if we actually did something
        if uploaded == 0 && deduplicated == 0 && failed == 0 {
            return;
        }

        let speed = if elapsed.as_secs_f64() > 0.0 {
            (bytes_uploaded as f64 / elapsed.as_secs_f64()) as u64
        } else {
            0
        };

        eprintln!();
        eprintln!("────────────────────────────────────────────────────────────");

        if failed == 0 {
            eprintln!(
                "✅ Push complete in {:.1}s",
                elapsed.as_secs_f64()
            );
        } else {
            eprintln!(
                "⚠️  Push complete in {:.1}s (with errors)",
                elapsed.as_secs_f64()
            );
        }
        eprintln!();

        if uploaded > 0 {
            eprintln!(
                "   Uploaded:     {:>3} paths ({}) at {}/s",
                uploaded,
                HumanBytes(bytes_uploaded),
                HumanBytes(speed)
            );
        }
        if deduplicated > 0 {
            eprintln!(
                "   Deduplicated: {:>3} paths ({} saved)",
                deduplicated,
                HumanBytes(bytes_dedup)
            );
        }
        if failed > 0 {
            eprintln!("   Failed:       {:>3} paths", failed);
        }

        // Print errors if any
        if failed > 0 {
            eprintln!();
            eprintln!("Failures:");
            // We can't await here, so we use blocking lock
            if let Ok(errors) = stats.errors.try_lock() {
                for (path, error) in errors.iter() {
                    eprintln!("   ❌ {}: {}", path, error);
                }
            }
        }

        eprintln!("────────────────────────────────────────────────────────────");
    }

    /// Creates a push plan.
    pub async fn plan(
        &self,
        roots: Vec<StorePath>,
        no_closure: bool,
        ignore_upstream_filter: bool,
    ) -> Result<PushPlan> {
        PushPlan::plan(
            self.store.clone(),
            &self.api,
            &self.cache,
            &self.cache_config,
            roots,
            no_closure,
            ignore_upstream_filter,
        )
        .await
    }

    /// Converts the pusher into a `PushSession`.
    ///
    /// This is useful when the list of store paths is streamed from some
    /// external source (e.g., FS watcher, Unix Domain Socket) and a push
    /// plan cannot be computed statically.
    pub fn into_push_session(self, config: PushSessionConfig) -> PushSession {
        PushSession::with_pusher(self, config)
    }

    async fn worker(
        receiver: JobReceiver,
        store: Arc<NixStore>,
        api: ApiClient,
        cache: CacheName,
        mp: MultiProgress,
        config: PushConfig,
        stats: Arc<PushStats>,
        overall_bar: Arc<Mutex<Option<ProgressBar>>>,
    ) {
        loop {
            let path_info = match receiver.recv().await {
                Ok(path_info) => path_info,
                Err(_) => {
                    // channel is closed - we are done
                    break;
                }
            };

            let path_name = path_info.path.as_os_str().to_string_lossy().to_string();
            let nar_size = path_info.nar_size;

            let result = upload_path(
                path_info,
                store.clone(),
                api.clone(),
                &cache,
                mp.clone(),
                config,
                stats.clone(),
            )
            .await;

            // Update stats and log result
            match result {
                Ok(UploadOutcome::Uploaded) => {
                    stats.paths_uploaded.fetch_add(1, Ordering::Relaxed);
                    stats.bytes_uploaded.fetch_add(nar_size, Ordering::Relaxed);
                    if !config.quiet {
                        mp.suspend(|| {
                            eprintln!("✅ {} ({})", path_name, HumanBytes(nar_size));
                        });
                    }
                }
                Ok(UploadOutcome::Deduplicated) => {
                    stats.paths_deduplicated.fetch_add(1, Ordering::Relaxed);
                    stats.bytes_deduplicated.fetch_add(nar_size, Ordering::Relaxed);
                    if !config.quiet {
                        mp.suspend(|| {
                            eprintln!("✅ {} ({}, deduplicated)", path_name, HumanBytes(nar_size));
                        });
                    }
                }
                Err(e) => {
                    stats.paths_failed.fetch_add(1, Ordering::Relaxed);
                    let error_msg = e.to_string();
                    mp.suspend(|| {
                        eprintln!("❌ {}: {}", path_name, error_msg);
                    });
                    let mut errors = stats.errors.lock().await;
                    errors.push((path_name, error_msg));
                }
            }

            // Update overall progress bar
            if let Some(bar) = overall_bar.lock().await.as_ref() {
                bar.inc(nar_size);
            }
        }
    }
}

/// Outcome of uploading a path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadOutcome {
    Uploaded,
    Deduplicated,
}

impl PushSession {
    pub fn with_pusher(pusher: Pusher, config: PushSessionConfig) -> Self {
        let (sender, receiver) = channel::unbounded();
        let (result_sender, result_receiver) = mpsc::channel(1);

        let known_paths_mutex = Arc::new(Mutex::new(HashSet::new()));

        spawn(async move {
            if let Err(e) = Self::worker(
                pusher,
                config,
                known_paths_mutex.clone(),
                receiver.clone(),
                result_sender.clone(),
            )
            .await
            {
                let _ = result_sender.send(Err(e)).await;
            }
        });

        Self {
            sender,
            result_receiver,
        }
    }

    async fn worker(
        pusher: Pusher,
        config: PushSessionConfig,
        known_paths_mutex: Arc<Mutex<HashSet<StorePathHash>>>,
        receiver: channel::Receiver<SessionQueueCommand>,
        result_sender: mpsc::Sender<Result<HashMap<StorePath, Result<()>>>>,
    ) -> Result<()> {
        let mut roots = HashSet::new();

        loop {
            // Get outstanding paths in queue
            let done = tokio::select! {
                // 2 seconds since last queued path
                done = async {
                    loop {
                        let poll = tokio::select! {
                            r = receiver.recv() => match r {
                                Ok(SessionQueueCommand::Paths(paths)) => SessionQueuePoll::Paths(paths),
                                Ok(SessionQueueCommand::Flush) => SessionQueuePoll::Flush,
                                Ok(SessionQueueCommand::Terminate) => SessionQueuePoll::Terminate,
                                _ => SessionQueuePoll::Closed,
                            },
                            _ = time::sleep(Duration::from_secs(2)) => SessionQueuePoll::TimedOut,
                        };

                        match poll {
                            SessionQueuePoll::Paths(store_paths) => {
                                roots.extend(store_paths.into_iter());
                            }
                            SessionQueuePoll::Closed | SessionQueuePoll::Terminate => {
                                break true;
                            }
                            SessionQueuePoll::Flush | SessionQueuePoll::TimedOut => {
                                break false;
                            }
                        }
                    }
                } => done,

                // 10 seconds
                _ = time::sleep(Duration::from_secs(10)) => {
                    false
                },
            };

            // Compute push plan
            let roots_vec: Vec<StorePath> = {
                let known_paths = known_paths_mutex.lock().await;
                roots
                    .drain()
                    .filter(|root| !known_paths.contains(&root.to_hash()))
                    .collect()
            };

            let mut plan = pusher
                .plan(
                    roots_vec,
                    config.no_closure,
                    config.ignore_upstream_cache_filter,
                )
                .await?;

            let mut known_paths = known_paths_mutex.lock().await;
            plan.store_path_map
                .retain(|sph, _| !known_paths.contains(sph));

            // Push everything
            for (store_path_hash, path_info) in plan.store_path_map.into_iter() {
                pusher.queue(path_info).await?;
                known_paths.insert(store_path_hash);
            }

            drop(known_paths);

            if done {
                // Wait for pusher to complete and handle any errors
                let wait_result = pusher.wait().await;
                // Convert to HashMap format for compatibility
                let result_map = HashMap::new();
                if let Err(e) = wait_result {
                    result_sender.send(Err(e)).await?;
                } else {
                    result_sender.send(Ok(result_map)).await?;
                }
                return Ok(());
            }
        }
    }

    /// Waits for all workers to terminate, returning all results.
    pub async fn wait(mut self) -> Result<HashMap<StorePath, Result<()>>> {
        self.flush()?;

        // The worker might have died
        let _ = self.sender.send(SessionQueueCommand::Terminate).await;

        self.result_receiver
            .recv()
            .await
            .expect("Nothing in result channel")
    }

    /// Queues multiple store paths to be pushed.
    pub fn queue_many(&self, store_paths: Vec<StorePath>) -> Result<()> {
        self.sender
            .send_blocking(SessionQueueCommand::Paths(store_paths))
            .map_err(|e| anyhow!(e))
    }

    /// Flushes the worker queue.
    pub fn flush(&self) -> Result<()> {
        self.sender
            .send_blocking(SessionQueueCommand::Flush)
            .map_err(|e| anyhow!(e))
    }
}

impl PushPlan {
    /// Creates a plan.
    async fn plan(
        store: Arc<NixStore>,
        api: &ApiClient,
        cache: &CacheName,
        cache_config: &CacheConfig,
        roots: Vec<StorePath>,
        no_closure: bool,
        ignore_upstream_filter: bool,
    ) -> Result<Self> {
        // Compute closure
        let closure = if no_closure {
            roots
        } else {
            store
                .compute_fs_closure_multi(roots, false, false, false)
                .await?
        };

        let mut store_path_map: HashMap<StorePathHash, ValidPathInfo> = {
            let futures = closure
                .iter()
                .map(|path| {
                    let store = store.clone();
                    let path = path.clone();
                    let path_hash = path.to_hash();

                    async move {
                        let path_info = store.query_path_info(path).await?;
                        Ok((path_hash, path_info))
                    }
                })
                .collect::<Vec<_>>();

            join_all(futures).await.into_iter().collect::<Result<_>>()?
        };

        let num_all_paths = store_path_map.len();
        if store_path_map.is_empty() {
            return Ok(Self {
                store_path_map,
                num_all_paths,
                num_already_cached: 0,
                num_upstream: 0,
            });
        }

        if !ignore_upstream_filter {
            // Filter out paths signed by upstream caches
            let upstream_cache_key_names = cache_config
                .upstream_cache_key_names
                .as_ref()
                .map_or([].as_slice(), |v| v.as_slice());
            store_path_map.retain(|_, pi| {
                for sig in &pi.sigs {
                    if let Some((name, _)) = sig.split_once(':') {
                        if upstream_cache_key_names.iter().any(|u| name == u) {
                            return false;
                        }
                    }
                }

                true
            });
        }

        let num_filtered_paths = store_path_map.len();
        if store_path_map.is_empty() {
            return Ok(Self {
                store_path_map,
                num_all_paths,
                num_already_cached: 0,
                num_upstream: num_all_paths - num_filtered_paths,
            });
        }

        // Query missing paths
        let missing_path_hashes: HashSet<StorePathHash> = {
            let store_path_hashes = store_path_map.keys().map(|sph| sph.to_owned()).collect();
            let res = api.get_missing_paths(cache, store_path_hashes).await?;
            res.missing_paths.into_iter().collect()
        };
        store_path_map.retain(|sph, _| missing_path_hashes.contains(sph));
        let num_missing_paths = store_path_map.len();

        Ok(Self {
            store_path_map,
            num_all_paths,
            num_already_cached: num_filtered_paths - num_missing_paths,
            num_upstream: num_all_paths - num_filtered_paths,
        })
    }
}

/// Uploads a single path to a cache.
pub async fn upload_path(
    path_info: ValidPathInfo,
    store: Arc<NixStore>,
    api: ApiClient,
    cache: &CacheName,
    mp: MultiProgress,
    config: PushConfig,
    _stats: Arc<PushStats>,
) -> Result<UploadOutcome> {
    if path_info.nar_size >= chunked_upload::CHUNKING_THRESHOLD as u64 {
        return chunked_upload::upload_path_chunked(
            path_info, store, api, cache, mp, config,
        )
        .await;
    }

    let path = &path_info.path;
    let upload_info = {
        let full_path = store
            .get_full_path(path)
            .to_str()
            .ok_or_else(|| anyhow!("Path contains non-UTF-8"))?
            .to_string();

        let references = path_info
            .references
            .into_iter()
            .map(|pb| {
                pb.to_str()
                    .ok_or_else(|| anyhow!("Reference contains non-UTF-8"))
                    .map(|s| s.to_owned())
            })
            .collect::<Result<Vec<String>, anyhow::Error>>()?;

        UploadPathNarInfo {
            cache: cache.to_owned(),
            store_path_hash: path.to_hash(),
            store_path: full_path,
            references,
            system: None,  // TODO
            deriver: None, // TODO
            sigs: path_info.sigs,
            ca: path_info.ca,
            nar_hash: path_info.nar_hash.to_owned(),
            nar_size: path_info.nar_size as usize,
        }
    };

    // Create progress bar (hidden in quiet mode)
    let bar = if config.quiet {
        ProgressBar::hidden()
    } else {
        let template = format!(
            "{{spinner}} {: <20.20} {{bar:40.green/blue}} {{human_bytes:10}} ({{average_speed}})",
            path.name(),
        );
        let style = ProgressStyle::with_template(&template)
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏✓")
            .progress_chars("━━─")
            .with_key("human_bytes", |state: &ProgressState, w: &mut dyn Write| {
                write!(w, "{}", HumanBytes(state.pos())).unwrap();
            })
            .with_key(
                "average_speed",
                |state: &ProgressState, w: &mut dyn Write| match (state.pos(), state.elapsed()) {
                    (pos, elapsed) if elapsed > Duration::ZERO => {
                        write!(w, "{}", average_speed(pos, elapsed)).unwrap();
                    }
                    _ => write!(w, "-").unwrap(),
                },
            );
        let bar = mp.add(ProgressBar::new(path_info.nar_size));
        bar.set_style(style);
        bar
    };

    let nar_stream = NarStreamProgress::new(store.nar_from_path(path.to_owned()), bar.clone())
        .map_ok(Bytes::from);

    match api
        .upload_path(upload_info, nar_stream, config.force_preamble)
        .await
    {
        Ok(r) => {
            let r = r.unwrap_or(UploadPathResult {
                kind: UploadPathResultKind::Uploaded,
                file_size: None,
                frac_deduplicated: None,
            });

            bar.finish_and_clear();

            match r.kind {
                UploadPathResultKind::Deduplicated => Ok(UploadOutcome::Deduplicated),
                _ => Ok(UploadOutcome::Uploaded),
            }
        }
        Err(e) => {
            bar.finish_and_clear();
            Err(e)
        }
    }
}

impl<S: Stream<Item = BinixResult<Vec<u8>>>> NarStreamProgress<S> {
    fn new(stream: S, bar: ProgressBar) -> Self {
        Self { stream, bar }
    }
}

impl<S: Stream<Item = BinixResult<Vec<u8>>> + Unpin> Stream for NarStreamProgress<S> {
    type Item = BinixResult<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).as_mut().poll_next(cx) {
            Poll::Ready(Some(data)) => {
                if let Ok(data) = &data {
                    self.bar.inc(data.len() as u64);
                }

                Poll::Ready(Some(data))
            }
            other => other,
        }
    }
}

// Just the average, no fancy sliding windows that cause wild fluctuations
// <https://github.com/console-rs/indicatif/issues/394>
fn average_speed(bytes: u64, duration: Duration) -> String {
    let speed = bytes as f64 * 1000_f64 / duration.as_millis() as f64;
    format!("{}/s", HumanBytes(speed as u64))
}
