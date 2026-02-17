//! Daemon for processing batched requests with per-model concurrency control.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use metrics::{counter, gauge, histogram};
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinSet;

use opentelemetry::trace::TraceContextExt;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::FusilladeError;
use crate::batch::BatchId;
use crate::error::Result;
use crate::http::{HttpClient, HttpResponse};
use crate::manager::{DaemonStorage, Storage};
use crate::request::{DaemonId, RequestCompletionResult};

pub mod transitions;
pub mod types;

pub use types::{
    AnyDaemonRecord, DaemonData, DaemonRecord, DaemonState, DaemonStats, DaemonStatus, Dead,
    Initializing, Running,
};

/// Predicate function to determine if a response should be retried.
///
/// Takes an HTTP response and returns true if the request should be retried.
pub type ShouldRetryFn = Arc<dyn Fn(&HttpResponse) -> bool + Send + Sync>;

/// Semaphore entry tracking both the semaphore and its configured limit.
type SemaphoreEntry = (Arc<Semaphore>, usize);

/// Default retry predicate: retry on server errors (5xx), rate limits (429), timeouts (408), and not found (404).
pub fn default_should_retry(response: &HttpResponse) -> bool {
    response.status >= 500
        || response.status == 429
        || response.status == 408
        || response.status == 404
}

/// Default function for creating the should_retry Arc
fn default_should_retry_fn() -> ShouldRetryFn {
    Arc::new(default_should_retry)
}

/// Default model escalations (empty map)
fn default_model_escalations() -> Arc<dashmap::DashMap<String, ModelEscalationConfig>> {
    Arc::new(dashmap::DashMap::new())
}

/// Default escalation threshold (15 minutes)
/// This should be greater than the processing timeout (10 minutes) to allow
/// a processing request to fall back to pending before escalation kicks in.
fn default_escalation_threshold_seconds() -> i64 {
    900
}

/// Model-based escalation configuration for routing requests to a different model
/// at claim time when approaching SLA deadline.
///
/// When a request is claimed with less than `escalation_threshold_seconds` remaining
/// before batch expiry, it will be routed to the `escalation_model` instead of the
/// original model. The batch API key automatically has access to escalation models
/// in the onwards routing cache (no separate API key needed).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModelEscalationConfig {
    /// The model to escalate to (e.g., "o1-preview" for requests using "gpt-4")
    pub escalation_model: String,

    /// Time threshold in seconds - escalate when time remaining before batch expiry
    /// is less than this value. Default: 900 (15 minutes)
    #[serde(default = "default_escalation_threshold_seconds")]
    pub escalation_threshold_seconds: i64,
}

/// Configuration for the daemon.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DaemonConfig {
    /// Maximum number of requests to claim in each iteration
    pub claim_batch_size: usize,

    /// Default concurrency limit per model
    pub default_model_concurrency: usize,

    /// Per-model concurrency overrides (shared, can be updated dynamically)
    pub model_concurrency_limits: Arc<dashmap::DashMap<String, usize>>,

    /// Per-model escalation configurations for SLA-based model switching
    /// Maps model name -> escalation config (e.g., "gpt-4" -> "o1-preview")
    /// When a request is escalated, it's routed to the escalation_model by the control layer
    #[serde(skip, default = "default_model_escalations")]
    pub model_escalations: Arc<dashmap::DashMap<String, ModelEscalationConfig>>,

    /// How long to sleep between claim iterations
    pub claim_interval_ms: u64,

    /// Maximum number of retry attempts before giving up.
    pub max_retries: Option<u32>,

    /// Stop retrying (including escalations) this many milliseconds before the batch expires.
    ///
    /// - **Negative** (e.g., -300000 = -5 min): Retry for a buffer window AFTER SLA deadline (recommended)
    /// - **Zero**: Stop exactly at SLA deadline
    /// - **Positive**: Stop BEFORE SLA deadline (terminates retryable errors within the SLA deadline, avoid!)
    /// - **None**: No deadline awareness
    ///
    /// Default: 0 (stop retrying/escalating on SLA deadline)
    pub stop_before_deadline_ms: Option<i64>,

    /// Base backoff duration in milliseconds (will be exponentially increased)
    pub backoff_ms: u64,

    /// Factor by which the backoff_ms is increased with each retry
    pub backoff_factor: u64,

    /// Maximum backoff time in milliseconds
    pub max_backoff_ms: u64,

    /// Timeout for each individual request attempt in milliseconds
    pub timeout_ms: u64,

    /// Interval for logging daemon status (requests in flight) in milliseconds
    /// Set to None to disable periodic status logging
    pub status_log_interval_ms: Option<u64>,

    /// Interval for sending heartbeats to update daemon status in database (milliseconds)
    pub heartbeat_interval_ms: u64,

    /// Predicate function to determine if a response should be retried.
    /// Defaults to retrying 5xx, 429, 408, and 404 status codes.
    #[serde(skip, default = "default_should_retry_fn")]
    pub should_retry: ShouldRetryFn,

    /// Maximum time a request can stay in "claimed" state before being unclaimed
    /// and returned to pending (milliseconds). This handles daemon crashes.
    pub claim_timeout_ms: u64,

    /// Maximum time a request can stay in "processing" state before being unclaimed
    /// and returned to pending (milliseconds). This handles daemon crashes during execution.
    pub processing_timeout_ms: u64,

    /// Time after a daemon's last heartbeat before its requests are considered
    /// orphaned and returned to pending (milliseconds). Should be significantly
    /// larger than `heartbeat_interval_ms` to avoid reclaiming from live daemons
    /// that are merely slow. Also reclaims from daemons explicitly marked dead.
    /// Default: 30,000 (30 seconds, 6× the default heartbeat interval).
    pub stale_daemon_threshold_ms: u64,

    /// Maximum number of stale requests to unclaim in a single poll cycle.
    /// Limits database load when many requests become stale simultaneously (e.g., daemon crash).
    pub unclaim_batch_size: usize,

    /// Interval for polling batches to perform finalization and check for cancellations (milliseconds).
    ///
    /// This polling loop serves two purposes:
    /// 1. **Batch Finalization**: Fetches active batches and triggers lazy finalization
    ///    (computing completion timestamps when all requests reach terminal states)
    /// 2. **Cancellation Detection**: Checks if any active batches have been cancelled
    ///    and aborts their in-flight requests
    ///
    /// Default: 5000ms (5 seconds)
    #[serde(default = "default_cancellation_poll_interval_ms")]
    pub cancellation_poll_interval_ms: u64,

    /// Batch table column names to include as request headers.
    /// These values are sent as `x-fusillade-batch-{column}` headers with each request.
    /// Example: ["id", "created_by", "endpoint"] produces headers like:
    ///   - x-fusillade-batch-id
    ///   - x-fusillade-batch-created-by
    ///   - x-fusillade-batch-endpoint
    #[serde(default = "default_batch_metadata_fields")]
    pub batch_metadata_fields: Vec<String>,

    /// Interval for running the orphaned row purge task (milliseconds).
    /// Deletes orphaned request_templates and requests whose parent file/batch
    /// has been soft-deleted, for right-to-erasure compliance.
    /// Set to 0 to disable purging. Default: 3,600,000 (1 hour).
    pub purge_interval_ms: u64,

    /// Maximum number of orphaned rows to delete per purge iteration.
    /// Each iteration deletes up to this many requests and this many request_templates.
    /// Default: 1000.
    pub purge_batch_size: i64,

    /// Throttle delay between consecutive purge batches within a single drain
    /// cycle (milliseconds). Prevents sustained high DB load when many orphans
    /// exist. Default: 100.
    pub purge_throttle_ms: u64,
}

fn default_batch_metadata_fields() -> Vec<String> {
    vec![
        "id".to_string(),
        "endpoint".to_string(),
        "created_at".to_string(),
        "completion_window".to_string(),
    ]
}

fn default_cancellation_poll_interval_ms() -> u64 {
    5000
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            claim_batch_size: 100,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
            model_escalations: default_model_escalations(),
            claim_interval_ms: 1000,
            max_retries: Some(1000),
            stop_before_deadline_ms: Some(0),
            backoff_ms: 1000,
            backoff_factor: 2,
            max_backoff_ms: 10000,
            timeout_ms: 600000,
            status_log_interval_ms: Some(2000), // Log every 2 seconds by default
            heartbeat_interval_ms: 5000,        // Heartbeat every 5 seconds by default
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,             // 1 minute
            processing_timeout_ms: 600000,       // 10 minutes
            stale_daemon_threshold_ms: 30_000,   // 30 seconds (6× heartbeat interval)
            unclaim_batch_size: 100,             // Unclaim up to 100 stale requests per poll
            cancellation_poll_interval_ms: 5000, // Poll every 5 seconds by default
            batch_metadata_fields: default_batch_metadata_fields(),
            purge_interval_ms: 600_000, // 10 minutes
            purge_batch_size: 1000,
            purge_throttle_ms: 100,
        }
    }
}

/// Daemon that processes batched requests.
///
/// The daemon continuously claims pending requests from storage, enforces
/// per-model concurrency limits, and dispatches requests for execution.
pub struct Daemon<S, H>
where
    S: Storage + DaemonStorage,
    H: HttpClient,
{
    daemon_id: DaemonId,
    storage: Arc<S>,
    http_client: Arc<H>,
    config: DaemonConfig,
    semaphores: Arc<RwLock<HashMap<String, SemaphoreEntry>>>,
    requests_in_flight: Arc<AtomicUsize>,
    requests_processed: Arc<AtomicU64>,
    requests_failed: Arc<AtomicU64>,
    shutdown_token: tokio_util::sync::CancellationToken,
    /// Map of batch_id -> cancellation token for batch-level cancellation
    /// All requests in a batch share the same cancellation token
    cancellation_tokens: Arc<dashmap::DashMap<BatchId, tokio_util::sync::CancellationToken>>,
}

impl<S, H> Daemon<S, H>
where
    S: Storage + DaemonStorage + 'static,
    H: HttpClient + 'static,
{
    /// Create a new daemon.
    pub fn new(
        storage: Arc<S>,
        http_client: Arc<H>,
        config: DaemonConfig,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            daemon_id: DaemonId::from(uuid::Uuid::new_v4()),
            storage,
            http_client,
            config,
            semaphores: Arc::new(RwLock::new(HashMap::new())),
            requests_in_flight: Arc::new(AtomicUsize::new(0)),
            requests_processed: Arc::new(AtomicU64::new(0)),
            requests_failed: Arc::new(AtomicU64::new(0)),
            shutdown_token,
            cancellation_tokens: Arc::new(dashmap::DashMap::new()),
        }
    }

    /// Get or create a semaphore for a model.
    ///
    /// Automatically adjusts the semaphore's permit count if the configured limit has changed.
    /// For limit increases, adds permits. For decreases, forgets permits (as many as possible).
    /// Note: When decreasing, we can only forget permits that aren't currently held, so the
    /// effective limit may temporarily remain higher until requests complete.
    async fn get_semaphore(&self, model: &str) -> Arc<Semaphore> {
        let current_limit = self
            .config
            .model_concurrency_limits
            .get(model)
            .map(|entry| *entry.value())
            .unwrap_or(self.config.default_model_concurrency);

        let mut semaphores = self.semaphores.write().await;

        let entry = semaphores
            .entry(model.to_string())
            .or_insert_with(|| (Arc::new(Semaphore::new(current_limit)), current_limit));

        let (semaphore, stored_limit) = entry;

        // Check if the limit has changed
        if *stored_limit != current_limit {
            if current_limit > *stored_limit {
                // Limit increased - add permits
                let delta = current_limit - *stored_limit;
                semaphore.add_permits(delta);
                tracing::info!(
                    model = %model,
                    old_limit = *stored_limit,
                    new_limit = current_limit,
                    added_permits = delta,
                    "Increased model concurrency limit"
                );
                *stored_limit = current_limit;
            } else {
                // Limit decreased - forget permits (as many as we can)
                let desired_delta = *stored_limit - current_limit;
                let actual_forgotten = semaphore.forget_permits(desired_delta);

                if actual_forgotten < desired_delta {
                    tracing::warn!(
                        model = %model,
                        old_limit = *stored_limit,
                        target_limit = current_limit,
                        desired_to_forget = desired_delta,
                        actually_forgot = actual_forgotten,
                        held_permits = desired_delta - actual_forgotten,
                        "Decreased model concurrency limit (some permits still held by in-flight requests)"
                    );
                } else {
                    tracing::info!(
                        model = %model,
                        old_limit = *stored_limit,
                        new_limit = current_limit,
                        forgot_permits = actual_forgotten,
                        "Decreased model concurrency limit"
                    );
                }

                // Update to the new effective limit (accounting for unforgettable permits)
                *stored_limit = current_limit + (desired_delta - actual_forgotten);
            }
        }

        semaphore.clone()
    }

    /// Try to acquire a permit for a model (non-blocking).
    async fn try_acquire_permit(&self, model: &str) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let semaphore = self.get_semaphore(model).await;
        semaphore.clone().try_acquire_owned().ok()
    }

    /// Run the daemon loop.
    ///
    /// This continuously claims and processes requests until an error occurs
    /// or the task is cancelled.
    ///
    /// The daemon periodically polls for cancelled batches and aborts in-flight requests.
    #[tracing::instrument(skip(self), fields(daemon_id = %self.daemon_id))]
    pub async fn run(self: Arc<Self>) -> Result<()> {
        tracing::info!("Daemon starting main processing loop");

        // Register daemon in database
        let daemon_record = DaemonRecord {
            data: DaemonData {
                id: self.daemon_id,
                hostname: types::get_hostname(),
                pid: types::get_pid(),
                version: types::get_version(),
                config_snapshot: serde_json::to_value(&self.config)
                    .expect("Failed to serialize daemon config"),
            },
            state: Initializing {
                started_at: chrono::Utc::now(),
            },
        };

        let running_record = daemon_record.start(self.storage.as_ref()).await?;
        tracing::info!("Daemon registered in database");

        // Spawn periodic heartbeat task
        let storage = self.storage.clone();
        let requests_in_flight = self.requests_in_flight.clone();
        let requests_processed = self.requests_processed.clone();
        let requests_failed = self.requests_failed.clone();
        let daemon_id = self.daemon_id;
        let heartbeat_interval_ms = self.config.heartbeat_interval_ms;
        let shutdown_signal = self.shutdown_token.clone();

        let heartbeat_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(heartbeat_interval_ms));
            let mut daemon_record = running_record;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let stats = DaemonStats {
                            requests_processed: requests_processed.load(Ordering::Relaxed),
                            requests_failed: requests_failed.load(Ordering::Relaxed),
                            requests_in_flight: requests_in_flight.load(Ordering::Relaxed),
                        };

                        // Clone the record so we preserve it if heartbeat fails
                        let current = daemon_record.clone();
                        let heartbeat_start = std::time::Instant::now();
                        match current.heartbeat(stats, storage.as_ref()).await {
                            Ok(updated) => {
                                histogram!("fusillade_heartbeat_duration_seconds")
                                    .record(heartbeat_start.elapsed().as_secs_f64());
                                daemon_record = updated;
                                tracing::trace!(
                                    daemon_id = %daemon_id,
                                    "Heartbeat sent"
                                );
                            }
                            Err(e) => {
                                histogram!("fusillade_heartbeat_duration_seconds")
                                    .record(heartbeat_start.elapsed().as_secs_f64());
                                counter!("fusillade_heartbeat_failures_total").increment(1);
                                tracing::error!(
                                    daemon_id = %daemon_id,
                                    error = %e,
                                    "Failed to send heartbeat"
                                );
                                // daemon_record stays unchanged on error
                            }
                        }
                    }
                    _ = shutdown_signal.cancelled() => {
                        // Mark daemon as dead on shutdown
                        tracing::info!("Shutting down heartbeat task");
                        if let Err(e) = daemon_record.shutdown(storage.as_ref()).await {
                            tracing::error!(
                                daemon_id = %daemon_id,
                                error = %e,
                                "Failed to mark daemon as dead during shutdown"
                            );
                        }
                        break;
                    }
                }
            }
        });

        // Spawn periodic status logging task if configured
        if let Some(interval_ms) = self.config.status_log_interval_ms {
            let requests_in_flight = self.requests_in_flight.clone();
            let daemon_id = self.daemon_id;
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
                loop {
                    interval.tick().await;
                    let count = requests_in_flight.load(Ordering::Relaxed);
                    tracing::debug!(
                        daemon_id = %daemon_id,
                        requests_in_flight = count,
                        "Daemon status"
                    );
                }
            });
        }

        // Spawn periodic batch polling task for finalization and cancellation detection
        // This serves two purposes in one efficient loop:
        // 1. Triggers lazy finalization by fetching batches (computes completion timestamps)
        // 2. Detects cancelled batches and aborts their in-flight requests
        let cancellation_tokens = self.cancellation_tokens.clone();
        let storage = self.storage.clone();
        let shutdown_token = self.shutdown_token.clone();
        let cancellation_poll_interval_ms = self.config.cancellation_poll_interval_ms;

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(cancellation_poll_interval_ms));
            tracing::info!(
                interval_ms = cancellation_poll_interval_ms,
                "Batch polling started (finalization + cancellation detection)"
            );

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Get all active batch IDs we're currently processing
                        let active_batch_ids: Vec<BatchId> = cancellation_tokens
                            .iter()
                            .map(|entry| *entry.key())
                            .collect();

                        if active_batch_ids.is_empty() {
                            continue;
                        }

                        let poll_start = std::time::Instant::now();
                        gauge!("fusillade_cancellation_poll_batches_checked")
                            .set(active_batch_ids.len() as f64);

                        // Fetch batches once for both finalization and cancellation check
                        for batch_id in active_batch_ids {
                            match storage.get_batch(batch_id).await {
                                Ok(batch) => {
                                    // Lazy finalization happens as side effect of get_batch()

                                    // Check if batch is being cancelled
                                    if batch.cancelling_at.is_some()
                                        && let Some(entry) = cancellation_tokens.get(&batch_id) {
                                            entry.value().cancel();
                                            counter!("fusillade_batches_cancelled_total").increment(1);
                                            tracing::info!(batch_id = %batch_id, "Cancelled all requests in batch");
                                            drop(entry);
                                            cancellation_tokens.remove(&batch_id);
                                    }
                                }
                                Err(e) => {
                                    counter!("fusillade_cancellation_poll_errors_total").increment(1);
                                    tracing::warn!(
                                        batch_id = %batch_id,
                                        error = %e,
                                        "Failed to fetch batch during cancellation poll"
                                    );
                                }
                            }
                        }

                        histogram!("fusillade_cancellation_poll_duration_seconds")
                            .record(poll_start.elapsed().as_secs_f64());
                    }
                    _ = shutdown_token.cancelled() => {
                        tracing::info!("Shutting down batch polling");
                        break;
                    }
                }
            }
        });

        // Spawn periodic purge task for orphaned rows (right-to-erasure compliance)
        if self.config.purge_interval_ms > 0 {
            let storage = self.storage.clone();
            let shutdown_token = self.shutdown_token.clone();
            let purge_interval_ms = self.config.purge_interval_ms;
            let purge_batch_size = self.config.purge_batch_size;
            let purge_throttle_ms = self.config.purge_throttle_ms;

            tokio::spawn(async move {
                tracing::info!(
                    interval_ms = purge_interval_ms,
                    batch_size = purge_batch_size,
                    throttle_ms = purge_throttle_ms,
                    "Orphaned row purge task started"
                );

                loop {
                    // Sleep for the configured interval (interruptible by shutdown)
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(purge_interval_ms)) => {},
                        _ = shutdown_token.cancelled() => {
                            tracing::info!("Shutting down purge task");
                            break;
                        }
                    }

                    // Drain orphaned rows in batches
                    loop {
                        match storage.purge_orphaned_rows(purge_batch_size).await {
                            Ok(0) => break,
                            Ok(deleted) => {
                                counter!("fusillade_rows_purged_total").increment(deleted);
                                tracing::debug!(deleted, "Purged orphaned rows");
                                // Throttle between batches to avoid sustained DB load
                                tokio::select! {
                                    _ = tokio::time::sleep(Duration::from_millis(purge_throttle_ms)) => {},
                                    _ = shutdown_token.cancelled() => {
                                        tracing::info!("Shutting down purge task during drain");
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                counter!("fusillade_purge_errors_total").increment(1);
                                tracing::error!(error = %e, "Failed to purge orphaned rows");
                                break;
                            }
                        }
                    }
                }
            });
        }

        let mut join_set: JoinSet<Result<()>> = JoinSet::new();

        let run_result = loop {
            // Check for shutdown signal
            if self.shutdown_token.is_cancelled() {
                tracing::info!("Shutdown signal received, stopping daemon");
                break Ok(());
            }

            // Poll for completed tasks (non-blocking)
            while let Some(result) = join_set.try_join_next() {
                match result {
                    Ok(Ok(())) => {
                        tracing::trace!("Task completed successfully");
                    }
                    Ok(Err(e)) => {
                        tracing::error!(error = %e, "Task failed");
                    }
                    Err(join_error) => {
                        counter!("fusillade_task_panics_total").increment(1);
                        tracing::error!(error = %join_error, "Task panicked");
                    }
                }
            }

            tracing::trace!("Sleeping before claiming");
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(self.config.claim_interval_ms)) => {},
                _ = self.shutdown_token.cancelled() => {
                    tracing::info!("Shutdown signal received, stopping daemon");
                    break Ok(());
                }
            }
            // Snapshot available semaphore permits per model so claim_requests
            // only claims what this daemon can actually process.
            let available_capacity: std::collections::HashMap<String, usize> = {
                let semaphores = self.semaphores.read().await;
                semaphores
                    .iter()
                    .map(|(model, (sem, _))| (model.clone(), sem.available_permits()))
                    .collect()
            };

            // Record available capacity before claiming
            let total_capacity: usize = available_capacity.values().sum();
            gauge!("fusillade_claim_capacity").set(total_capacity as f64);

            // Claim a batch of pending requests
            let claim_start = std::time::Instant::now();
            let mut claimed = self
                .storage
                .claim_requests(
                    self.config.claim_batch_size,
                    self.daemon_id,
                    &available_capacity,
                )
                .await?;
            histogram!("fusillade_claim_duration_seconds")
                .record(claim_start.elapsed().as_secs_f64());

            // Record claim metrics
            histogram!("fusillade_claim_size").record(claimed.len() as f64);

            tracing::debug!(
                claimed_count = claimed.len(),
                "Claimed requests from storage"
            );

            // Route requests to escalated models if time is running low
            // This replaces the old SLA racing system with a simpler approach:
            // at claim time, we check if there's enough time remaining and route
            // to the escalated model if below threshold
            for request in &mut claimed {
                if let Some(config) = self.config.model_escalations.get(&request.data.model) {
                    let time_remaining = request.state.batch_expires_at - chrono::Utc::now();
                    if time_remaining.num_seconds() < config.escalation_threshold_seconds {
                        let original_model = request.data.model.clone();
                        request.data.model = config.escalation_model.clone();

                        // Update the model field in the request body JSON
                        if let Ok(mut json) =
                            serde_json::from_str::<serde_json::Value>(&request.data.body)
                            && let Some(obj) = json.as_object_mut()
                        {
                            obj.insert(
                                "model".to_string(),
                                serde_json::Value::String(config.escalation_model.clone()),
                            );
                            if let Ok(new_body) = serde_json::to_string(&json) {
                                request.data.body = new_body;
                            }
                        }

                        // No API key swap needed - batch API keys automatically have access
                        // to escalation models in the onwards routing cache
                        counter!("fusillade_requests_routed_to_escalation_total", "original_model" => original_model.clone(), "escalation_model" => config.escalation_model.clone()).increment(1);
                        tracing::info!(
                            request_id = %request.data.id,
                            original_model = %original_model,
                            escalation_model = %config.escalation_model,
                            time_remaining_seconds = time_remaining.num_seconds(),
                            threshold_seconds = config.escalation_threshold_seconds,
                            "Routing request to escalation model due to time pressure"
                        );
                    }
                }
            }

            // Group requests by model for better concurrency control visibility
            let mut by_model: HashMap<String, Vec<_>> = HashMap::new();
            for request in claimed {
                let model = request.data.model.clone();
                by_model.entry(model).or_default().push(request);
            }

            tracing::debug!(
                models = by_model.len(),
                total_requests = by_model.values().map(|v| v.len()).sum::<usize>(),
                "Grouped requests by model"
            );

            // Dispatch requests
            for (model, requests) in by_model {
                tracing::debug!(model = %model, count = requests.len(), "Processing requests for model");

                for request in requests {
                    let request_id = request.data.id;
                    let batch_id = request.data.batch_id;

                    // Try to acquire a semaphore permit for this model
                    match self.try_acquire_permit(&model).await {
                        Some(permit) => {
                            tracing::debug!(
                                request_id = %request_id,
                                batch_id = %batch_id,
                                model = %model,
                                "Acquired permit, spawning processing task"
                            );

                            // We have capacity - spawn a task
                            let model_clone = model.clone(); // Clone model for the spawned task
                            let storage = self.storage.clone();
                            let http_client = (*self.http_client).clone();
                            let timeout_ms = self.config.timeout_ms;
                            let retry_config = (&self.config).into();
                            let requests_in_flight = self.requests_in_flight.clone();
                            let requests_processed = self.requests_processed.clone();
                            let requests_failed = self.requests_failed.clone();
                            let should_retry = self.config.should_retry.clone();
                            let shutdown_token = self.shutdown_token.clone();
                            let cancellation_tokens = self.cancellation_tokens.clone();

                            // Get or create a cancellation token for this batch
                            // All requests in a batch share the same token
                            let batch_cancellation_token =
                                cancellation_tokens.entry(batch_id).or_default().clone();

                            // Increment in-flight counter and gauge
                            requests_in_flight.fetch_add(1, Ordering::Relaxed);
                            gauge!("fusillade_requests_in_flight", "model" => model_clone.clone())
                                .increment(1.0);

                            let process_span = tracing::info_span!(
                                "process_request",
                                trace_id = tracing::field::Empty,
                                otel.name = "process_request",
                                request_id = %request_id,
                                batch_id = %batch_id,
                                model = %model,
                            );
                            // Start a new trace root so process_request isn't
                            // parented under the claim_requests span.
                            let _ = process_span.set_parent(opentelemetry::Context::new());

                            join_set.spawn(async move {
                                // Record trace_id from OTel context
                                let span = tracing::Span::current();
                                let sc = span.context().span().span_context().clone();
                                if sc.is_valid() {
                                    span.record("trace_id", tracing::field::display(sc.trace_id()));
                                }

                                // Permit is held for the duration of this task
                                let _permit = permit;

                                // Track processing start time for duration metrics
                                let processing_start = std::time::Instant::now();

                                // Ensure we decrement the counter when this task completes
                                let model_for_guard = model_clone.clone();
                                let _guard = scopeguard::guard((), move |_| {
                                    requests_in_flight.fetch_sub(1, Ordering::Relaxed);
                                    gauge!("fusillade_requests_in_flight", "model" => model_for_guard).decrement(1.0);
                                });

                                tracing::info!(request_id = %request_id, "Processing request");

                                // Launch request processing (this goes on a background thread)
                                let processing = request.process(
                                    http_client,
                                    timeout_ms,
                                    storage.as_ref()
                                ).await?;

                                // Capture retry attempt count before completion (not preserved in Completed state)
                                let retry_attempt_at_completion = processing.state.retry_attempt;
                                // Capture batch expiry time for SLA missed completion tracking
                                let batch_expires_at = processing.state.batch_expires_at;

                                let cancellation = async {
                                    tokio::select! {
                                        _ = batch_cancellation_token.cancelled() => {
                                            crate::request::transitions::CancellationReason::User
                                        }
                                        _ = shutdown_token.cancelled() => {
                                            crate::request::transitions::CancellationReason::Shutdown
                                        }
                                    }
                                };

                                // Wait for completion
                                match processing.complete(storage.as_ref(), |response| {
                                    (should_retry)(response)
                                }, cancellation).await {
                                    Ok(RequestCompletionResult::Completed(completed)) => {
                                        requests_processed.fetch_add(1, Ordering::Relaxed);
                                        counter!("fusillade_requests_completed_total", "model" => model_clone.clone(), "status" => "success").increment(1);
                                        histogram!("fusillade_request_duration_seconds", "model" => model_clone.clone(), "status" => "success")
                                            .record(processing_start.elapsed().as_secs_f64());
                                        // Record how many retries it took to succeed (0 = first attempt succeeded)
                                        histogram!("fusillade_retry_attempts_on_success", "model" => model_clone.clone())
                                            .record(retry_attempt_at_completion as f64);

                                        // Track requests completing after SLA
                                        if completed.state.completed_at > batch_expires_at {
                                            counter!("fusillade_requests_completed_after_sla_total", "model" => model_clone.clone(), "status" => "success").increment(1);
                                            tracing::warn!(
                                                request_id = %request_id,
                                                batch_id = %batch_id,
                                                "Request completed successfully after SLA"
                                            );
                                        }

                                        tracing::info!(request_id = %request_id, retry_attempts = retry_attempt_at_completion, "Request completed successfully");
                                    }
                                    Ok(RequestCompletionResult::Failed(failed)) => {
                                        let retry_attempt = failed.state.retry_attempt;

                                        // Check if this is a retriable error using the FailureReason
                                        if failed.state.reason.is_retriable() {
                                            tracing::warn!(
                                                request_id = %request_id,
                                                retry_attempt,
                                                error = %failed.state.reason.to_error_message(),
                                                "Request failed with retriable error, attempting retry"
                                            );

                                            // Attempt to retry
                                            match failed.can_retry(retry_attempt, retry_config) {
                                                Ok(pending) => {
                                                    // Can retry - persist as Pending
                                                    storage.persist(&pending).await?;
                                                    counter!(
                                                        "fusillade_requests_retried_total",
                                                        "model" => model_clone.clone(),
                                                        "attempt" => (retry_attempt + 1).to_string()
                                                    ).increment(1);
                                                    tracing::info!(
                                                        request_id = %request_id,
                                                        retry_attempt = retry_attempt + 1,
                                                        "Request queued for retry"
                                                    );
                                                }
                                                Err(failed) => {
                                                    // No retries left - persist as Failed (terminal)
                                                    storage.persist(&*failed).await?;
                                                    requests_failed.fetch_add(1, Ordering::Relaxed);
                                                    counter!("fusillade_requests_completed_total", "model" => model_clone.clone(), "status" => "failed", "reason" => failed.state.reason.metric_label(), "status_code" => failed.state.reason.status_code_label()).increment(1);
                                                    histogram!("fusillade_request_duration_seconds", "model" => model_clone.clone(), "status" => "failed")
                                                        .record(processing_start.elapsed().as_secs_f64());

                                                    // Track requests completing after SLA
                                                    if failed.state.failed_at > batch_expires_at {
                                                        counter!("fusillade_requests_completed_after_sla_total", "model" => model_clone.clone(), "status" => "failed").increment(1);
                                                        tracing::warn!(
                                                            request_id = %request_id,
                                                            batch_id = %batch_id,
                                                            "Request failed permanently after SLA"
                                                        );
                                                    }

                                                    tracing::warn!(
                                                        request_id = %request_id,
                                                        retry_attempt,
                                                        "Request failed permanently (no retries remaining)"
                                                    );
                                                }
                                            }
                                        } else {
                                            requests_failed.fetch_add(1, Ordering::Relaxed);
                                            counter!("fusillade_requests_completed_total", "model" => model_clone.clone(), "status" => "failed", "reason" => failed.state.reason.metric_label(), "status_code" => failed.state.reason.status_code_label()).increment(1);
                                            histogram!("fusillade_request_duration_seconds", "model" => model_clone.clone(), "status" => "failed")
                                                .record(processing_start.elapsed().as_secs_f64());

                                            // Track requests completing after SLA
                                            if failed.state.failed_at > batch_expires_at {
                                                counter!("fusillade_requests_completed_after_sla_total", "model" => model_clone.clone(), "status" => "failed").increment(1);
                                                tracing::warn!(
                                                    request_id = %request_id,
                                                    batch_id = %batch_id,
                                                    "Request failed with non-retriable error after SLA"
                                                );
                                            }

                                            tracing::warn!(
                                                request_id = %request_id,
                                                error = %failed.state.reason.to_error_message(),
                                                "Request failed with non-retriable error, not retrying"
                                            );
                                        }
                                    }
                                    Ok(RequestCompletionResult::Canceled(_canceled)) => {
                                        counter!("fusillade_requests_completed_total", "model" => model_clone.clone(), "status" => "cancelled").increment(1);
                                        tracing::debug!(request_id = %request_id, "Request canceled by user");
                                    }
                                    Err(FusilladeError::Shutdown) => {
                                        tracing::info!(request_id = %request_id, "Request aborted due to shutdown");
                                        // Don't count as failed - request will be reclaimed
                                    }
                                    Err(e) => {
                                        // Unexpected error
                                        tracing::error!(request_id = %request_id, error = %e, "Unexpected error processing request");
                                        return Err(e);
                                    }
                                }

                                // Note: We don't remove the batch cancellation token here since
                                // multiple requests in the same batch share it. Tokens are cleaned
                                // up when the daemon shuts down or batch completes.

                                Ok(())
                            }.instrument(process_span));
                        }
                        None => {
                            counter!("fusillade_requests_unclaimed_no_capacity_total", "model" => model.clone()).increment(1);
                            tracing::debug!(
                                request_id = %request_id,
                                model = %model,
                                "No capacity available, unclaiming request"
                            );

                            // No capacity for this model - unclaim the request
                            let storage = self.storage.clone();
                            if let Err(e) = request.unclaim(storage.as_ref()).await {
                                counter!("fusillade_unclaim_errors_total").increment(1);
                                tracing::error!(
                                    request_id = %request_id,
                                    error = %e,
                                    "Failed to unclaim request"
                                );
                            };
                        }
                    }
                }
            }
        };

        // Wait for heartbeat task to complete (it will mark daemon as dead)
        tracing::info!("Waiting for heartbeat task to complete");
        if let Err(e) = heartbeat_handle.await {
            tracing::error!(error = %e, "Heartbeat task panicked");
        }

        run_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestDbPools;
    use crate::http::{HttpResponse, MockHttpClient};
    use crate::manager::{DaemonExecutor, postgres::PostgresRequestManager};
    use std::time::Duration;

    #[sqlx::test]
    #[test_log::test]
    async fn test_daemon_claims_and_completes_request(pool: sqlx::PgPool) {
        // Setup: Create HTTP client with mock response
        let http_client = Arc::new(MockHttpClient::new());
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"success"}"#.to_string(),
            }),
        );

        // Setup: Create manager with fast claim interval (no sleeping)
        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10, // Very fast for testing
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

            model_escalations: Arc::new(dashmap::DashMap::new()),
            max_retries: Some(3),
            stop_before_deadline_ms: None,
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None, // Disable status logging in tests
            heartbeat_interval_ms: 5000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
            processing_timeout_ms: 600000,
            stale_daemon_threshold_ms: 30_000,
            unclaim_batch_size: 100,
            batch_metadata_fields: vec![],
            cancellation_poll_interval_ms: 100, // Fast polling for tests
            purge_interval_ms: 0,               // Disabled in tests
            purge_batch_size: 1000,
            purge_throttle_ms: 100,
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client.clone(),
            )
            .with_config(config),
        );

        // Setup: Create a file and batch to associate with our request
        let file_id = manager
            .create_file(
                "test-file".to_string(),
                Some("Test file".to_string()),
                vec![crate::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test"}"#.to_string(),
                    model: "test-model".to_string(),
                    api_key: "test-key".to_string(),
                }],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        // Get the created request from the batch
        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get batch requests");
        assert_eq!(requests.len(), 1);
        let request_id = requests[0].id();

        // Start the daemon
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Poll for completion (with timeout)
        let start = tokio::time::Instant::now();
        let timeout = Duration::from_secs(5);
        let mut completed = false;

        while start.elapsed() < timeout {
            let results = manager
                .get_requests(vec![request_id])
                .await
                .expect("Failed to get request");

            if let Some(Ok(any_request)) = results.first()
                && any_request.is_terminal()
            {
                if let crate::AnyRequest::Completed(req) = any_request {
                    // Verify the request was completed successfully
                    assert_eq!(req.state.response_status, 200);
                    assert_eq!(req.state.response_body, r#"{"result":"success"}"#);
                    completed = true;
                    break;
                } else {
                    panic!(
                        "Request reached terminal state but was not completed: {:?}",
                        any_request
                    );
                }
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Stop the daemon
        shutdown_token.cancel();

        // Assert that the request completed
        assert!(
            completed,
            "Request did not complete within timeout. Check daemon processing."
        );

        // Verify HTTP client was called exactly once
        assert_eq!(http_client.call_count(), 1);
        let calls = http_client.get_calls();
        assert_eq!(calls[0].method, "POST");
        assert_eq!(calls[0].path, "/v1/test");
        assert_eq!(calls[0].api_key, "test-key");
    }

    #[sqlx::test]
    async fn test_daemon_respects_per_model_concurrency_limits(pool: sqlx::PgPool) {
        // Setup: Create HTTP client with triggered responses
        let http_client = Arc::new(MockHttpClient::new());

        // Add 5 triggered responses for our 5 requests
        let trigger1 = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"1"}"#.to_string(),
            }),
        );
        let trigger2 = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"2"}"#.to_string(),
            }),
        );
        let trigger3 = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"3"}"#.to_string(),
            }),
        );
        let trigger4 = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"4"}"#.to_string(),
            }),
        );
        let trigger5 = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"5"}"#.to_string(),
            }),
        );

        // Setup: Create manager with concurrency limit of 2 for "gpt-4"
        let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
        model_concurrency_limits.insert("gpt-4".to_string(), 2);

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits,

            model_escalations: Arc::new(dashmap::DashMap::new()),
            max_retries: Some(3),
            stop_before_deadline_ms: None,
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 5000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
            processing_timeout_ms: 600000,
            stale_daemon_threshold_ms: 30_000,
            unclaim_batch_size: 100,
            batch_metadata_fields: vec![],
            cancellation_poll_interval_ms: 100, // Fast polling for tests
            purge_interval_ms: 0,               // Disabled in tests
            purge_batch_size: 1000,
            purge_throttle_ms: 100,
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client.clone(),
            )
            .with_config(config),
        );

        // Setup: Create a file with 5 templates, all using "gpt-4"
        let file_id = manager
            .create_file(
                "test-file".to_string(),
                Some("Test concurrency limits".to_string()),
                vec![
                    crate::RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test1"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "test-key".to_string(),
                    },
                    crate::RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test2"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "test-key".to_string(),
                    },
                    crate::RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test3"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "test-key".to_string(),
                    },
                    crate::RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test4"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "test-key".to_string(),
                    },
                    crate::RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test5"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "test-key".to_string(),
                    },
                ],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        // Start the daemon
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for exactly 2 requests to be in-flight (respecting concurrency limit)
        let start = tokio::time::Instant::now();
        let timeout = Duration::from_secs(2);
        let mut reached_limit = false;

        while start.elapsed() < timeout {
            let in_flight = http_client.in_flight_count();
            if in_flight == 2 {
                reached_limit = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(
            reached_limit,
            "Expected exactly 2 requests in-flight, got {}",
            http_client.in_flight_count()
        );

        // Verify exactly 2 are in-flight (not more)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            http_client.in_flight_count(),
            2,
            "Concurrency limit violated: more than 2 requests in-flight"
        );

        // Trigger completion of first request
        trigger1.send(()).unwrap();

        // Wait for the third request to start
        let start = tokio::time::Instant::now();
        let timeout = Duration::from_secs(2);
        let mut third_started = false;

        while start.elapsed() < timeout {
            if http_client.call_count() >= 3 {
                third_started = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(
            third_started,
            "Third request should have started after first completed"
        );

        // Verify still only 2 in-flight
        assert_eq!(
            http_client.in_flight_count(),
            2,
            "Should maintain concurrency limit of 2"
        );

        // Complete remaining requests to clean up
        trigger2.send(()).unwrap();
        trigger3.send(()).unwrap();
        trigger4.send(()).unwrap();
        trigger5.send(()).unwrap();

        // Wait for all requests to complete
        let start = tokio::time::Instant::now();
        let timeout = Duration::from_secs(5);
        let mut all_completed = false;

        while start.elapsed() < timeout {
            let status = manager
                .get_batch_status(batch.id)
                .await
                .expect("Failed to get batch status");

            if status.completed_requests == 5 {
                all_completed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Stop the daemon
        shutdown_token.cancel();

        assert!(all_completed, "All 5 requests should have completed");

        // Verify all 5 HTTP calls were made
        assert_eq!(http_client.call_count(), 5);
    }

    #[sqlx::test]
    async fn test_daemon_retries_failed_requests(pool: sqlx::PgPool) {
        // Setup: Create HTTP client with failing responses, then success
        let http_client = Arc::new(MockHttpClient::new());

        // First attempt: fails with 500
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 500,
                body: r#"{"error":"internal error"}"#.to_string(),
            }),
        );

        // Second attempt: fails with 503
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 503,
                body: r#"{"error":"service unavailable"}"#.to_string(),
            }),
        );

        // Third attempt: succeeds
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"success after retries"}"#.to_string(),
            }),
        );

        // Setup: Create manager with fast backoff for testing
        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

            model_escalations: Arc::new(dashmap::DashMap::new()),
            max_retries: Some(5),
            stop_before_deadline_ms: None,
            backoff_ms: 10, // Very fast backoff for testing
            backoff_factor: 2,
            max_backoff_ms: 100,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 5000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
            processing_timeout_ms: 600000,
            stale_daemon_threshold_ms: 30_000,
            unclaim_batch_size: 100,
            batch_metadata_fields: vec![],
            cancellation_poll_interval_ms: 100,
            purge_interval_ms: 0, // Disabled in tests
            purge_batch_size: 1000,
            purge_throttle_ms: 100,
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client.clone(),
            )
            .with_config(config),
        );

        // Setup: Create a file and batch
        let file_id = manager
            .create_file(
                "test-file".to_string(),
                Some("Test retry logic".to_string()),
                vec![crate::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test"}"#.to_string(),
                    model: "test-model".to_string(),
                    api_key: "test-key".to_string(),
                }],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get batch requests");
        assert_eq!(requests.len(), 1);
        let request_id = requests[0].id();

        // Start the daemon
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Poll for completion (with timeout)
        let start = tokio::time::Instant::now();
        let timeout = Duration::from_secs(5);
        let mut completed = false;

        while start.elapsed() < timeout {
            let results = manager
                .get_requests(vec![request_id])
                .await
                .expect("Failed to get request");

            if let Some(Ok(any_request)) = results.first()
                && let crate::AnyRequest::Completed(req) = any_request
            {
                // Verify the request eventually completed successfully
                assert_eq!(req.state.response_status, 200);
                assert_eq!(
                    req.state.response_body,
                    r#"{"result":"success after retries"}"#
                );
                completed = true;
                break;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Stop the daemon
        shutdown_token.cancel();

        assert!(completed, "Request should have completed after retries");

        // Verify the request was attempted 3 times (2 failures + 1 success)
        assert_eq!(
            http_client.call_count(),
            3,
            "Expected 3 HTTP calls (2 failed attempts + 1 success)"
        );
    }

    #[sqlx::test]
    async fn test_daemon_dynamically_updates_concurrency_limits(pool: sqlx::PgPool) {
        // Setup: Create HTTP client with triggered responses
        let http_client = Arc::new(MockHttpClient::new());

        // Add 10 triggered responses
        let mut triggers = vec![];
        for i in 1..=10 {
            let trigger = http_client.add_response_with_trigger(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 200,
                    body: format!(r#"{{"result":"{}"}}"#, i),
                }),
            );
            triggers.push(trigger);
        }

        // Setup: Start with concurrency limit of 2 for "gpt-4"
        let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
        model_concurrency_limits.insert("gpt-4".to_string(), 2);

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: model_concurrency_limits.clone(),

            model_escalations: Arc::new(dashmap::DashMap::new()),
            max_retries: Some(3),
            stop_before_deadline_ms: None,
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 5000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
            processing_timeout_ms: 600000,
            stale_daemon_threshold_ms: 30_000,
            unclaim_batch_size: 100,
            batch_metadata_fields: vec![],
            cancellation_poll_interval_ms: 100,
            purge_interval_ms: 0, // Disabled in tests
            purge_batch_size: 1000,
            purge_throttle_ms: 100,
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client.clone(),
            )
            .with_config(config),
        );

        // Setup: Create a file with 10 requests, all using "gpt-4"
        let templates: Vec<_> = (1..=10)
            .map(|i| crate::RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: format!(r#"{{"prompt":"test{}"}}"#, i),
                model: "gpt-4".to_string(),
                api_key: "test-key".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file(
                "test-file".to_string(),
                Some("Test dynamic limits".to_string()),
                templates,
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        // Start the daemon
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for exactly 2 requests to be in-flight (initial limit)
        let start = tokio::time::Instant::now();
        let timeout = Duration::from_secs(2);
        let mut reached_initial_limit = false;

        while start.elapsed() < timeout {
            let in_flight = http_client.in_flight_count();
            if in_flight == 2 {
                reached_initial_limit = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(
            reached_initial_limit,
            "Expected exactly 2 requests in-flight with initial limit"
        );

        // Increase the limit to 5
        model_concurrency_limits.insert("gpt-4".to_string(), 5);

        // Wait a bit for the daemon to pick up the new limit
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Complete one request to free up a permit and trigger daemon to check limits
        triggers.remove(0).send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Now we should see up to 5 requests in flight
        let start = tokio::time::Instant::now();
        let timeout = Duration::from_secs(2);
        let mut reached_new_limit = false;

        while start.elapsed() < timeout {
            let in_flight = http_client.in_flight_count();
            if in_flight >= 4 {
                // Should see at least 4-5 in flight with new limit
                reached_new_limit = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(
            reached_new_limit,
            "Expected more requests in-flight after limit increase, got {}",
            http_client.in_flight_count()
        );

        // Now decrease the limit to 3
        model_concurrency_limits.insert("gpt-4".to_string(), 3);

        // Complete remaining requests
        for trigger in triggers {
            trigger.send(()).unwrap();
        }

        // Wait for all requests to complete
        let start = tokio::time::Instant::now();
        let timeout = Duration::from_secs(5);
        let mut all_completed = false;

        while start.elapsed() < timeout {
            let status = manager
                .get_batch_status(batch.id)
                .await
                .expect("Failed to get batch status");

            if status.completed_requests == 10 {
                all_completed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Stop the daemon
        shutdown_token.cancel();

        assert!(all_completed, "All 10 requests should have completed");
        assert_eq!(http_client.call_count(), 10);
    }

    #[sqlx::test]
    async fn test_deadline_aware_retry_stops_before_deadline(pool: sqlx::PgPool) {
        // Test that retries stop when approaching the deadline
        let http_client = Arc::new(MockHttpClient::new());

        // All requests will fail
        for _ in 0..20 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 500,
                    body: r#"{"error":"server error"}"#.to_string(),
                }),
            );
        }

        // Use deadline-aware retry with a short completion window and short buffer
        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

            model_escalations: Arc::new(dashmap::DashMap::new()),
            max_retries: Some(10_000),
            stop_before_deadline_ms: Some(500), // 500ms buffer before deadline
            backoff_ms: 50,
            backoff_factor: 2,
            max_backoff_ms: 200,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 5000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
            processing_timeout_ms: 600000,
            stale_daemon_threshold_ms: 30_000,
            unclaim_batch_size: 100,
            batch_metadata_fields: vec![],
            cancellation_poll_interval_ms: 100,
            purge_interval_ms: 0, // Disabled in tests
            purge_batch_size: 1000,
            purge_throttle_ms: 100,
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client.clone(),
            )
            .with_config(config),
        );

        // Create a batch with a very short completion window (2 seconds)
        let file_id = manager
            .create_file(
                "test-file".to_string(),
                Some("Test deadline cutoff".to_string()),
                vec![crate::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test"}"#.to_string(),
                    model: "test-model".to_string(),
                    api_key: "test-key".to_string(),
                }],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "2s".to_string(), // Very short window
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get batch requests");
        let request_id = requests[0].id();

        // Start the daemon
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for the deadline to pass
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Check the request state
        let results = manager
            .get_requests(vec![request_id])
            .await
            .expect("Failed to get request");

        shutdown_token.cancel();

        if let Some(Ok(crate::AnyRequest::Failed(failed))) = results.first() {
            // Calculate expected retry attempts:
            // - Completion window: 2000ms
            // - Buffer: 500ms
            // - Effective deadline: 1500ms
            // - Backoff sequence: 50ms, 100ms, 200ms, 200ms, 200ms, 200ms, 200ms
            // - Timeline:
            //   - Initial attempt: t=0ms (attempt 0)
            //   - Retry 1: t=50ms (attempt 1)
            //   - Retry 2: t=150ms (attempt 2)
            //   - Retry 3: t=350ms (attempt 3)
            //   - Retry 4: t=550ms (attempt 4)
            //   - Retry 5: t=750ms (attempt 5)
            //   - Retry 6: t=950ms (attempt 6)
            //   - Retry 7: t=1150ms (attempt 7)
            //   - Retry 8: t=1350ms (attempt 8)
            //   - Next would be t=1550ms - EXCEEDS 1500ms deadline
            // Expected: 8 retry attempts (9 total including initial)

            let retry_count = failed.state.retry_attempt;
            let call_count = http_client.call_count();

            // 1. Verify we stopped before too many retries (deadline constraint)
            // Allow 4-9 attempts to account for timing variations in test execution,
            // parallel test execution overhead, and query overhead from batch metadata fields
            assert!(
                (4..=9).contains(&retry_count),
                "Expected 4-9 retry attempts based on deadline and backoff calculation, got {}",
                retry_count
            );

            // 2. Verify HTTP call count matches retry attempts (1 initial + N retries)
            assert_eq!(
                call_count,
                (retry_count + 1) as usize,
                "Expected call count to match retry attempts + 1 initial attempt, got {} calls for {} retry attempts",
                call_count,
                retry_count
            );

            // 3. Verify the request actually has error details from the last attempt
            assert!(
                !failed.state.reason.to_error_message().is_empty(),
                "Expected failed request to have failure reason"
            );
        } else {
            panic!(
                "Expected request to be in Failed state, got {:?}",
                results.first()
            );
        }
    }

    #[sqlx::test]
    async fn test_retry_stops_at_deadline_when_no_limits_set(pool: sqlx::PgPool) {
        // Test that when neither max_retries nor stop_before_deadline_ms is set,
        // retries stop exactly at the deadline (no buffer)
        let http_client = Arc::new(MockHttpClient::new());

        // All requests will fail
        for _ in 0..20 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 500,
                    body: r#"{"error":"server error"}"#.to_string(),
                }),
            );
        }

        // No max_retries, no stop_before_deadline_ms
        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

            model_escalations: Arc::new(dashmap::DashMap::new()),
            max_retries: None,             // No retry limit
            stop_before_deadline_ms: None, // No buffer - should retry until deadline
            backoff_ms: 50,
            backoff_factor: 2,
            max_backoff_ms: 200,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 5000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
            processing_timeout_ms: 600000,
            stale_daemon_threshold_ms: 30_000,
            unclaim_batch_size: 100,
            batch_metadata_fields: vec![],
            cancellation_poll_interval_ms: 100,
            purge_interval_ms: 0, // Disabled in tests
            purge_batch_size: 1000,
            purge_throttle_ms: 100,
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client.clone(),
            )
            .with_config(config),
        );

        // Create a batch with a 2 second completion window
        let file_id = manager
            .create_file(
                "test-file".to_string(),
                Some("Test no limits retry".to_string()),
                vec![crate::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test"}"#.to_string(),
                    model: "test-model".to_string(),
                    api_key: "test-key".to_string(),
                }],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "2s".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get batch requests");
        let request_id = requests[0].id();

        // Start the daemon
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for the deadline to pass
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Check the request state
        let results = manager
            .get_requests(vec![request_id])
            .await
            .expect("Failed to get request");

        shutdown_token.cancel();

        if let Some(Ok(crate::AnyRequest::Failed(failed))) = results.first() {
            // Calculate expected retry attempts with NO buffer:
            // - Completion window: 2000ms
            // - Buffer: 0ms (none set)
            // - Effective deadline: 2000ms
            // - Backoff sequence: 50ms, 100ms, 200ms, 200ms, 200ms...
            // - Timeline:
            //   - Initial attempt: t=0ms (attempt 0)
            //   - Retry 1: t=50ms (attempt 1)
            //   - Retry 2: t=150ms (attempt 2)
            //   - Retry 3: t=350ms (attempt 3)
            //   - Retry 4: t=550ms (attempt 4)
            //   - Retry 5: t=750ms (attempt 5)
            //   - Retry 6: t=950ms (attempt 6)
            //   - Retry 7: t=1150ms (attempt 7)
            //   - Retry 8: t=1350ms (attempt 8)
            //   - Retry 9: t=1550ms (attempt 9)
            //   - Retry 10: t=1750ms (attempt 10)
            //   - Retry 11: t=1950ms (attempt 11)
            //   - Next would be t=2150ms - EXCEEDS 2000ms deadline
            // Expected: ~11 retry attempts (12 total including initial)
            // In reality, we will see <11 due to DB calls and CPU overhead in making requests

            let retry_count = failed.state.retry_attempt;
            let call_count = http_client.call_count();

            // 1. Verify we retried more than the buffered case (which stopped at ~8)
            //    but still stopped before too many attempts
            // Allow 6-12 attempts to account for timing variations with CI slower CI CPUs,
            // parallel test execution overhead, and query overhead from batch metadata fields
            assert!(
                (6..12).contains(&retry_count),
                "Expected 6-12 retry attempts (should retry until deadline with no buffer), got {}",
                retry_count
            );

            // 2. Verify HTTP call count matches retry attempts (1 initial + N retries)
            assert_eq!(
                call_count,
                (retry_count + 1) as usize,
                "Expected call count to match retry attempts + 1 initial attempt, got {} calls for {} retry attempts",
                call_count,
                retry_count
            );

            // 3. Verify the request has error details from the last attempt
            assert!(
                !failed.state.reason.to_error_message().is_empty(),
                "Expected failed request to have failure reason"
            );
        } else {
            panic!(
                "Expected request to be in Failed state, got {:?}",
                results.first()
            );
        }
    }

    #[sqlx::test]
    #[test_log::test]
    async fn test_batch_metadata_headers_passed_through(pool: sqlx::PgPool) {
        let http_client = crate::http::MockHttpClient::new();
        http_client.add_response(
            "POST /v1/chat/completions",
            Ok(crate::http::HttpResponse {
                status: 200,
                body: r#"{"id":"chatcmpl-123","choices":[{"message":{"content":"test"}}]}"#
                    .to_string(),
            }),
        );

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

            model_escalations: Arc::new(dashmap::DashMap::new()),
            max_retries: Some(3),
            stop_before_deadline_ms: None,
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 5000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
            processing_timeout_ms: 600000,
            stale_daemon_threshold_ms: 30_000,
            unclaim_batch_size: 100,
            batch_metadata_fields: vec![
                "id".to_string(),
                "endpoint".to_string(),
                "created_at".to_string(),
                "completion_window".to_string(),
            ],
            cancellation_poll_interval_ms: 100,
            purge_interval_ms: 0, // Disabled in tests
            purge_batch_size: 1000,
            purge_throttle_ms: 100,
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                Arc::new(http_client.clone()),
            )
            .with_config(config),
        );

        // Create a batch
        let file_id = manager
            .create_file(
                "test-file".to_string(),
                Some("Test batch metadata".to_string()),
                vec![crate::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/chat/completions".to_string(),
                    body: r#"{"prompt":"test"}"#.to_string(),
                    model: "test-model".to_string(),
                    api_key: "test-key".to_string(),
                }],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: Some("test-user".to_string()),
            })
            .await
            .expect("Failed to create batch");

        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get batch requests");
        let request_id = requests[0].id();

        // Start the daemon
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for request to be processed
        tokio::time::sleep(Duration::from_millis(500)).await;

        shutdown_token.cancel();

        // Wait a bit for shutdown
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify the request was completed
        let results = manager
            .get_requests(vec![request_id])
            .await
            .expect("Failed to get request");

        assert_eq!(results.len(), 1);
        assert!(
            matches!(results[0], Ok(crate::AnyRequest::Completed(_))),
            "Expected request to be completed"
        );

        // Verify batch metadata was passed to HTTP client
        let calls = http_client.get_calls();
        assert_eq!(calls.len(), 1, "Expected exactly one HTTP call");

        let call = &calls[0];
        assert_eq!(
            call.batch_metadata.len(),
            4,
            "Expected 4 batch metadata fields"
        );

        // Verify each configured field was passed through
        assert!(
            call.batch_metadata.contains_key("id"),
            "Expected batch id in metadata"
        );
        assert!(
            call.batch_metadata.contains_key("endpoint"),
            "Expected batch endpoint in metadata"
        );
        assert!(
            call.batch_metadata.contains_key("created_at"),
            "Expected batch created_at in metadata"
        );
        assert!(
            call.batch_metadata.contains_key("completion_window"),
            "Expected batch completion_window in metadata"
        );

        // Verify values are correct
        assert_eq!(
            call.batch_metadata.get("endpoint"),
            Some(&"/v1/chat/completions".to_string()),
            "Batch endpoint should match"
        );
        assert_eq!(
            call.batch_metadata.get("completion_window"),
            Some(&"24h".to_string()),
            "Completion window should match"
        );
    }

    #[sqlx::test]
    #[test_log::test]
    async fn test_batch_metadata_extracts_fields_from_json_metadata(pool: sqlx::PgPool) {
        let http_client = crate::http::MockHttpClient::new();
        http_client.add_response(
            "POST /v1/chat/completions",
            Ok(crate::http::HttpResponse {
                status: 200,
                body: r#"{"id":"chatcmpl-123","choices":[{"message":{"content":"test"}}]}"#
                    .to_string(),
            }),
        );

        // Configure batch_metadata_fields to include "request_source" which is stored
        // inside the metadata JSON, not as a direct column
        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

            model_escalations: Arc::new(dashmap::DashMap::new()),
            max_retries: Some(3),
            stop_before_deadline_ms: None,
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 5000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
            processing_timeout_ms: 600000,
            stale_daemon_threshold_ms: 30_000,
            unclaim_batch_size: 100,
            batch_metadata_fields: vec![
                "id".to_string(),
                "endpoint".to_string(),
                "completion_window".to_string(),
                "request_source".to_string(), // This comes from metadata JSON
            ],
            cancellation_poll_interval_ms: 100,
            purge_interval_ms: 0, // Disabled in tests
            purge_batch_size: 1000,
            purge_throttle_ms: 100,
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                Arc::new(http_client.clone()),
            )
            .with_config(config),
        );

        // Create a batch with metadata containing request_source
        let file_id = manager
            .create_file(
                "test-file".to_string(),
                Some("Test metadata JSON extraction".to_string()),
                vec![crate::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/chat/completions".to_string(),
                    body: r#"{"prompt":"test"}"#.to_string(),
                    model: "test-model".to_string(),
                    api_key: "test-key".to_string(),
                }],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: Some(serde_json::json!({
                    "request_source": "api",
                    "created_by": "user-123"
                })),
                created_by: Some("test-user".to_string()),
            })
            .await
            .expect("Failed to create batch");

        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get batch requests");
        let request_id = requests[0].id();

        // Start the daemon
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for request to be processed
        tokio::time::sleep(Duration::from_millis(500)).await;

        shutdown_token.cancel();

        // Wait a bit for shutdown
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify the request was completed
        let results = manager
            .get_requests(vec![request_id])
            .await
            .expect("Failed to get request");

        assert_eq!(results.len(), 1);
        assert!(
            matches!(results[0], Ok(crate::AnyRequest::Completed(_))),
            "Expected request to be completed"
        );

        // Verify batch metadata was passed to HTTP client
        let calls = http_client.get_calls();
        assert_eq!(calls.len(), 1, "Expected exactly one HTTP call");

        let call = &calls[0];
        assert_eq!(
            call.batch_metadata.len(),
            4,
            "Expected 4 batch metadata fields (id, endpoint, completion_window, request_source)"
        );

        // Verify direct column fields
        assert!(
            call.batch_metadata.contains_key("id"),
            "Expected batch id in metadata"
        );
        assert_eq!(
            call.batch_metadata.get("endpoint"),
            Some(&"/v1/chat/completions".to_string()),
            "Batch endpoint should match"
        );

        // Verify request_source was extracted from metadata JSON
        assert_eq!(
            call.batch_metadata.get("request_source"),
            Some(&"api".to_string()),
            "request_source should be extracted from metadata JSON"
        );
    }
}
