//! Daemon for processing batched requests with per-model concurrency control.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinSet;

use crate::FusilladeError;
use crate::batch::BatchId;
use crate::error::Result;
use crate::http::{HttpClient, HttpResponse};
use crate::manager::{DaemonStorage, Storage};
use crate::request::{DaemonId, RequestCompletionResult, RequestId};

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

/// Default retry predicate: retry on server errors (5xx), rate limits (429), and timeouts (408).
pub fn default_should_retry(response: &HttpResponse) -> bool {
    response.status >= 500 || response.status == 429 || response.status == 408
}

/// Default function for creating the should_retry Arc
fn default_should_retry_fn() -> ShouldRetryFn {
    Arc::new(default_should_retry)
}

/// Default SLA check interval (1 minute)
fn default_sla_check_interval_seconds() -> u64 {
    60
}

/// Default priority endpoints (empty map)
fn default_priority_endpoints() -> Arc<dashmap::DashMap<String, PriorityEndpointConfig>> {
    Arc::new(dashmap::DashMap::new())
}

/// Log level for SLA threshold violations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlaLogLevel {
    /// Log as warning
    Warn,
    /// Log as error
    Error,
}

/// Configuration for a priority endpoint used in SLA escalation.
///
/// When a request is escalated, it's cloned and sent to the priority endpoint
/// specified in this configuration, while the original continues processing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PriorityEndpointConfig {
    /// Priority endpoint URL (e.g., "https://priority-api.openai.com")
    pub endpoint: String,

    /// Optional override for API key (if None, uses original request's API key)
    pub api_key: Option<String>,

    /// Optional override for path (if None, uses original request's path)
    pub path_override: Option<String>,

    /// Optional override for model name (if None, uses original request's model)
    /// This allows routing to different model tiers for priority endpoints
    /// (e.g., "gpt-4" -> "gpt-4-priority")
    pub model_override: Option<String>,
}

/// Action to take when a batch crosses an SLA threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlaAction {
    /// Log the SLA violation at the specified level
    Log {
        /// Log level (warn or error)
        level: SlaLogLevel,
    },
    /// Escalate at-risk requests to priority endpoints
    ///
    /// Creates a cloned request sent to the priority endpoint configured for the model,
    /// while the original request continues processing. First to complete wins.
    Escalate,
    // Future actions:
    // Notify,    // Send notification/webhook
    // Abort,     // Cancel the batch
}

/// SLA threshold configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SlaThreshold {
    /// Human-readable name for this threshold (e.g., "warning", "critical")
    pub name: String,

    /// Trigger when time remaining is less than this many seconds
    pub threshold_seconds: i64,

    /// Action to take when threshold is crossed
    pub action: SlaAction,

    /// Request states to act on for this threshold.
    /// Allows configuring different state filters for different thresholds
    /// (e.g., escalate only pending at 1 hour, but escalate pending+claimed at 5 minutes).
    /// Defaults to `[Pending]` if not specified.
    #[serde(default = "default_sla_allowed_states")]
    pub allowed_states: Vec<crate::request::RequestStateFilter>,
}

fn default_sla_allowed_states() -> Vec<crate::request::RequestStateFilter> {
    vec![crate::request::RequestStateFilter::Pending]
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

    /// Per-model priority endpoint configurations for SLA escalation
    /// Maps model name -> priority endpoint config
    /// When a request is escalated, it's cloned and sent to the priority endpoint for that model
    #[serde(skip, default = "default_priority_endpoints")]
    pub priority_endpoints: Arc<dashmap::DashMap<String, PriorityEndpointConfig>>,

    /// How long to sleep between claim iterations
    pub claim_interval_ms: u64,

    /// Maximum number of retry attempts before giving up.
    pub max_retries: Option<u32>,

    /// Stop retrying this many milliseconds before the batch expires.
    /// Positive values stop before the deadline (safety buffer).
    /// Negative values allow retrying after the deadline.
    /// If None, retries are not deadline-aware.
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
    /// Defaults to retrying 5xx, 429, and 408 status codes.
    #[serde(skip, default = "default_should_retry_fn")]
    pub should_retry: ShouldRetryFn,

    /// Maximum time a request can stay in "claimed" state before being unclaimed
    /// and returned to pending (milliseconds). This handles daemon crashes.
    pub claim_timeout_ms: u64,

    /// Maximum time a request can stay in "processing" state before being unclaimed
    /// and returned to pending (milliseconds). This handles daemon crashes during execution.
    pub processing_timeout_ms: u64,

    /// Interval for polling database to check for cancelled batches (milliseconds)
    /// Determines how quickly in-flight requests are aborted when their batch is cancelled
    pub cancellation_poll_interval_ms: u64,

    /// How often to check for batches approaching SLA deadlines (seconds)
    /// Default: 60 (1 minute)
    /// Only used if sla_thresholds is non-empty
    #[serde(default = "default_sla_check_interval_seconds")]
    pub sla_check_interval_seconds: u64,

    /// SLA threshold configurations.
    /// Each threshold defines a time limit and action to take when batches approach expiration.
    /// The daemon will query the database once per threshold to find at-risk batches.
    ///
    /// Example: Two thresholds (warning at 1 hour, critical at 15 minutes)
    /// ```
    /// use fusillade::daemon::{SlaThreshold, SlaAction, SlaLogLevel};
    /// use fusillade::request::RequestStateFilter;
    ///
    /// vec![
    ///     SlaThreshold {
    ///         name: "warning".to_string(),
    ///         threshold_seconds: 3600,
    ///         action: SlaAction::Log { level: SlaLogLevel::Warn },
    ///         allowed_states: vec![RequestStateFilter::Pending],
    ///     },
    ///     SlaThreshold {
    ///         name: "critical".to_string(),
    ///         threshold_seconds: 900,
    ///         action: SlaAction::Log { level: SlaLogLevel::Error },
    ///         // Act on both pending and claimed requests for critical threshold
    ///         allowed_states: vec![RequestStateFilter::Pending, RequestStateFilter::Claimed],
    ///     },
    /// ]
    /// # ;
    /// ```
    #[serde(default)]
    pub sla_thresholds: Vec<SlaThreshold>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            claim_batch_size: 100,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
            priority_endpoints: default_priority_endpoints(),
            claim_interval_ms: 1000,
            max_retries: Some(1000),
            stop_before_deadline_ms: Some(900_000),
            backoff_ms: 1000,
            backoff_factor: 2,
            max_backoff_ms: 10000,
            timeout_ms: 600000,
            status_log_interval_ms: Some(2000), // Log every 2 seconds by default
            heartbeat_interval_ms: 10000,       // Heartbeat every 10 seconds by default
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,             // 1 minute
            processing_timeout_ms: 600000,       // 10 minutes
            cancellation_poll_interval_ms: 5000, // Poll every 5 seconds by default
            sla_check_interval_seconds: default_sla_check_interval_seconds(),
            sla_thresholds: vec![],
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
    /// Map of request_id -> cancellation token for request-level cancellation
    /// Used to abort in-flight HTTP requests when their racing pair completes first (supersession)
    request_cancellation_tokens:
        Arc<dashmap::DashMap<RequestId, tokio_util::sync::CancellationToken>>,
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
            request_cancellation_tokens: Arc::new(dashmap::DashMap::new()),
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
                        match current.heartbeat(stats, storage.as_ref()).await {
                            Ok(updated) => {
                                daemon_record = updated;
                                tracing::trace!(
                                    daemon_id = %daemon_id,
                                    "Heartbeat sent"
                                );
                            }
                            Err(e) => {
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

        // Spawn periodic task to poll for cancelled batches and abort in-flight requests
        let cancellation_tokens = self.cancellation_tokens.clone();
        let storage = self.storage.clone();
        let shutdown_token = self.shutdown_token.clone();
        let cancellation_poll_interval_ms = self.config.cancellation_poll_interval_ms;
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(cancellation_poll_interval_ms));
            tracing::info!(
                interval_ms = cancellation_poll_interval_ms,
                "Batch cancellation polling started"
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

                        // Query database to check which of these batches have been cancelled
                        // Note: DaemonStorage doesn't have a method for this, so we'll check via the batch
                        // For now, we'll check each batch individually
                        for batch_id in active_batch_ids {
                            // Try to get the batch - if it has cancelling_at set, cancel the token
                            if let Ok(batch) = storage.get_batch(batch_id).await
                                && batch.cancelling_at.is_some()
                                    && let Some(entry) = cancellation_tokens.get(&batch_id) {
                                        entry.value().cancel();
                                        tracing::info!(batch_id = %batch_id, "Cancelled all requests in batch");
                                        // Remove from map so we don't keep checking it
                                        drop(entry);
                                        cancellation_tokens.remove(&batch_id);
                                    }
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        tracing::info!("Shutting down cancellation polling");
                        break;
                    }
                }
            }
        });

        // SLA Monitoring Task
        // Periodically checks for batches approaching their SLA deadline and logs warnings/errors
        if !self.config.sla_thresholds.is_empty() {
            let storage = self.storage.clone();
            let shutdown_token = self.shutdown_token.clone();
            let sla_thresholds = self.config.sla_thresholds.clone();
            let sla_check_interval_seconds = self.config.sla_check_interval_seconds;
            let priority_endpoints = self.config.priority_endpoints.clone();

            // Warn about configuration mismatches
            let has_escalate_threshold = sla_thresholds
                .iter()
                .any(|t| matches!(t.action, SlaAction::Escalate));
            if !priority_endpoints.is_empty() && !has_escalate_threshold {
                tracing::warn!(
                    "Priority endpoints are configured but no SLA thresholds with Escalate action are set. Priority endpoints will not be used."
                );
            }

            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_secs(sla_check_interval_seconds));

                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            // Query once per configured threshold
                            for threshold in &sla_thresholds {
                                // Match on action type to determine what DB operation to perform
                                match threshold.action {
                                    SlaAction::Log { level } => {
                                        // Get batch counts for logging
                                        match storage.get_at_risk_batches(threshold.threshold_seconds, &threshold.allowed_states).await {
                                            Ok(batch_counts) => {
                                                if batch_counts.is_empty() {
                                                    continue;
                                                }

                                                for (batch_id, at_risk_count) in batch_counts {
                                                    match level {
                                                        SlaLogLevel::Error => {
                                                            tracing::error!(
                                                                batch_id = %batch_id,
                                                                at_risk_count = at_risk_count,
                                                                threshold_seconds = threshold.threshold_seconds,
                                                                sla_name = %threshold.name,
                                                                "Requests at risk of missing SLA"
                                                            );
                                                        }
                                                        SlaLogLevel::Warn => {
                                                            tracing::warn!(
                                                                batch_id = %batch_id,
                                                                at_risk_count = at_risk_count,
                                                                threshold_seconds = threshold.threshold_seconds,
                                                                sla_name = %threshold.name,
                                                                "Requests at risk of missing SLA"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                tracing::error!(
                                                    error = %e,
                                                    "Failed to get at-risk batches"
                                                );
                                            }
                                        }
                                    }
                                    SlaAction::Escalate => {
                                        if priority_endpoints.is_empty() {
                                            tracing::warn!(
                                                sla_name = %threshold.name,
                                                "SLA threshold configured with Escalate action but no priority endpoints are set. No escalations will be created."
                                            );
                                        } else {
                                            // Escalate for each configured model
                                            for entry in priority_endpoints.iter() {
                                                let model = entry.key();
                                                let priority_config = entry.value();

                                                // Create escalated requests
                                                match storage
                                                    .create_escalated_requests(
                                                        model,
                                                        threshold.threshold_seconds,
                                                        &threshold.allowed_states,
                                                        priority_config.model_override.as_deref(),
                                                    )
                                                    .await
                                                {
                                                    Ok(escalated_count) => {
                                                        if escalated_count > 0 {
                                                            tracing::info!(
                                                                model = %model,
                                                                escalated_count = escalated_count,
                                                                priority_endpoint = %priority_config.endpoint,
                                                                threshold_seconds = threshold.threshold_seconds,
                                                                sla_name = %threshold.name,
                                                                "Successfully created escalated requests"
                                                            );
                                                        }
                                                    }
                                                    Err(e) => {
                                                        tracing::error!(
                                                            model = %model,
                                                            error = %e,
                                                            "Failed to escalate requests"
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        _ = shutdown_token.cancelled() => {
                            tracing::debug!("SLA checker shutting down");
                            break;
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
            // Claim a batch of pending requests
            let claimed = self
                .storage
                .claim_requests(self.config.claim_batch_size, self.daemon_id)
                .await?;

            tracing::debug!(
                claimed_count = claimed.len(),
                "Claimed requests from storage"
            );

            // Group requests by model for better concurrency control visibility
            let mut by_model: HashMap<String, Vec<_>> = HashMap::new();
            for request in claimed {
                by_model
                    .entry(request.data.model.clone())
                    .or_default()
                    .push(request);
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
                            let request_cancellation_tokens =
                                self.request_cancellation_tokens.clone();
                            let priority_endpoints = self.config.priority_endpoints.clone();

                            // Get or create a cancellation token for this batch
                            // All requests in a batch share the same token
                            let batch_cancellation_token =
                                cancellation_tokens.entry(batch_id).or_default().clone();

                            // Create a request-specific cancellation token for supersession
                            let request_cancellation_token =
                                tokio_util::sync::CancellationToken::new();
                            request_cancellation_tokens
                                .insert(request_id, request_cancellation_token.clone());

                            // Increment in-flight counter
                            requests_in_flight.fetch_add(1, Ordering::Relaxed);

                            join_set.spawn(async move {
                                // Permit is held for the duration of this task
                                let _permit = permit;

                                // Ensure we decrement the counter when this task completes
                                let _guard = scopeguard::guard((), |_| {
                                    requests_in_flight.fetch_sub(1, Ordering::Relaxed);
                                });

                                tracing::info!(request_id = %request_id, "Processing request");

                                // If this is an escalated request, modify endpoint/path/api_key based on priority config
                                let request_to_process = if request.data.is_escalated {
                                    if let Some(priority_config) = priority_endpoints.get(&model_clone) {
                                        let mut modified_data = request.data.clone();
                                        modified_data.endpoint = priority_config.endpoint.clone();
                                        if let Some(path_override) = &priority_config.path_override {
                                            modified_data.path = path_override.clone();
                                        }
                                        if let Some(api_key_override) = &priority_config.api_key {
                                            modified_data.api_key = api_key_override.clone();
                                        }

                                        // Apply model override to request body if specified
                                        if let Some(model_override) = &priority_config.model_override {
                                            match serde_json::from_str::<serde_json::Value>(&modified_data.body) {
                                                Ok(mut body_json) => {
                                                    if let Some(obj) = body_json.as_object_mut() {
                                                        obj.insert("model".to_string(), serde_json::Value::String(model_override.clone()));
                                                        modified_data.body = serde_json::to_string(&body_json)
                                                            .unwrap_or_else(|_| modified_data.body.clone());
                                                        tracing::debug!(
                                                            request_id = %request_id,
                                                            original_model = %model_clone,
                                                            override_model = %model_override,
                                                            "Applied model override to request body"
                                                        );
                                                    } else {
                                                        tracing::warn!(
                                                            request_id = %request_id,
                                                            "Request body is not a JSON object, cannot apply model override"
                                                        );
                                                    }
                                                }
                                                Err(e) => {
                                                    tracing::warn!(
                                                        request_id = %request_id,
                                                        error = %e,
                                                        "Failed to parse request body as JSON, cannot apply model override"
                                                    );
                                                }
                                            }
                                        }

                                        tracing::info!(
                                            request_id = %request_id,
                                            original_endpoint = %request.data.endpoint,
                                            priority_endpoint = %modified_data.endpoint,
                                            "Routing escalated request to priority endpoint"
                                        );

                                        crate::request::Request {
                                            data: modified_data,
                                            state: request.state.clone(),
                                        }
                                    } else {
                                        tracing::warn!(
                                            request_id = %request_id,
                                            model = %model_clone,
                                            "Escalated request but no priority endpoint configured for model"
                                        );
                                        request
                                    }
                                } else {
                                    request
                                };

                                // Launch request processing (this goes on a background thread)
                                let processing = request_to_process.process(
                                    http_client,
                                    timeout_ms,
                                    storage.as_ref()
                                ).await?;

                                let cancellation = async {
                                    tokio::select! {
                                        _ = batch_cancellation_token.cancelled() => {
                                            crate::request::transitions::CancellationReason::User
                                        }
                                        _ = request_cancellation_token.cancelled() => {
                                            // Request was superseded by its racing pair - don't persist as canceled
                                            crate::request::transitions::CancellationReason::Superseded
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
                                        tracing::info!(request_id = %request_id, "Request completed successfully");

                                        // If this request is part of a race, cancel the superseded racing pair's token
                                        let superseded_id_opt = if completed.data.is_escalated {
                                            // This is an escalated request - the original was superseded
                                            completed.data.escalated_from_request_id
                                        } else {
                                            // This is an original - check if there's an escalated request to supersede
                                            // Query DB to find any escalated request with escalated_from_request_id = this ID
                                            match storage.get_batch_requests(batch_id).await {
                                                Ok(requests) => {
                                                    requests.iter().find_map(|req| {
                                                        match req {
                                                            crate::AnyRequest::Processing(r) if r.data.is_escalated && r.data.escalated_from_request_id == Some(request_id) => {
                                                                Some(r.data.id)
                                                            }
                                                            crate::AnyRequest::Pending(r) if r.data.is_escalated && r.data.escalated_from_request_id == Some(request_id) => {
                                                                Some(r.data.id)
                                                            }
                                                            crate::AnyRequest::Claimed(r) if r.data.is_escalated && r.data.escalated_from_request_id == Some(request_id) => {
                                                                Some(r.data.id)
                                                            }
                                                            _ => None
                                                        }
                                                    })
                                                }
                                                Err(_) => None
                                            }
                                        };

                                        if let Some(sid) = superseded_id_opt
                                            && let Some((_, token)) = request_cancellation_tokens.remove(&sid) {
                                                token.cancel();
                                                tracing::info!(
                                                    winner_id = %request_id,
                                                    superseded_id = %sid,
                                                    "Cancelled superseded request's in-flight HTTP task"
                                                );
                                            }
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
                                            match failed.retry(retry_attempt, retry_config, storage.as_ref()).await? {
                                                Some(_pending) => {
                                                    tracing::info!(
                                                        request_id = %request_id,
                                                        retry_attempt = retry_attempt + 1,
                                                        "Request queued for retry"
                                                    );
                                                }
                                                None => {
                                                    requests_failed.fetch_add(1, Ordering::Relaxed);
                                                    tracing::warn!(
                                                        request_id = %request_id,
                                                        retry_attempt,
                                                        "Request failed permanently (no retries remaining)"
                                                    );
                                                }
                                            }
                                        } else {
                                            requests_failed.fetch_add(1, Ordering::Relaxed);
                                            tracing::warn!(
                                                request_id = %request_id,
                                                error = %failed.state.reason.to_error_message(),
                                                "Request failed with non-retriable error, not retrying"
                                            );
                                        }
                                    }
                                    Ok(RequestCompletionResult::Canceled(_canceled)) => {
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

                                // Clean up request-specific cancellation token
                                request_cancellation_tokens.remove(&request_id);

                                // Note: We don't remove the batch cancellation token here since
                                // multiple requests in the same batch share it. Tokens are cleaned
                                // up when the daemon shuts down or batch completes.

                                Ok(())
                            });
                        }
                        None => {
                            tracing::debug!(
                                request_id = %request_id,
                                model = %model,
                                "No capacity available, unclaiming request"
                            );

                            // No capacity for this model - unclaim the request
                            let storage = self.storage.clone();
                            if let Err(e) = request.unclaim(storage.as_ref()).await {
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
