//! State transitions for batch requests using the typestate pattern.
//!
//! This module implements state transitions for HTTP batch requests using Rust's
//! type system to enforce valid state transitions at compile time. Each request
//! state is represented as a distinct type parameter on `Request<State>`.
//!
//! # Typestate Pattern
//!
//! The typestate pattern leverages Rust's type system to make invalid states
//! unrepresentable. A `Request<Pending>` can only call methods available for
//! pending requests, and transitions return different types:
//!
//! ```text
//! Request<Pending> ──claim()──> Request<Claimed> ──process()──> Request<Processing>
//!       │                             │                               │
//!       │                             │                               └──complete()──> Request<Completed>
//!       │                             │                               └──complete()──> Request<Failed>
//!       └──cancel()──> Request<Canceled>                              └──cancel()────> Request<Canceled>
//!                              │
//!                              └──unclaim()─> Request<Pending>
//!
//! Request<Failed> ──retry()──> Request<Pending>  (if retries remain)
//!                 ──retry()──> None              (if max retries reached)
//! ```
//!
//! # State Lifecycle
//!
//! ## 1. Pending → Claimed
//!
//! A daemon claims a pending request for processing:
//! - Records which daemon claimed it
//! - Sets claimed_at timestamp
//! - Preserves retry attempt count
//!
//! ## 2. Claimed → Processing
//!
//! The daemon starts executing the HTTP request:
//! - Spawns an async task to make the HTTP call
//! - Creates a channel to receive the result
//! - Provides an abort handle for cancellation
//!
//! ## 3. Processing → Completed or Failed
//!
//! The HTTP request completes:
//! - **Success**: Transitions to `Completed` with response body
//! - **Failure**: Transitions to `Failed` with error message
//! - **Retriable**: HTTP succeeded but status code indicates retry (e.g., 429, 500)
//!
//! ## 4. Failed → Pending (Retry)
//!
//! Failed requests can be retried with exponential backoff:
//! - Increments retry_attempt counter
//! - Calculates backoff delay: `backoff_ms * (factor ^ attempt)`
//! - Sets not_before timestamp to delay retry
//! - Returns `None` if max retries exceeded
//!
//! ## 5. Any State → Canceled
//!
//! Requests can be canceled from most states:
//! - `Pending`: Simply marks as canceled
//! - `Claimed`: Releases claim and cancels
//! - `Processing`: Aborts the in-flight HTTP request
//!
//! # Retry Configuration
//!
//! Exponential backoff and retry limits are configured via [`RetryConfig`]:
//!
//! ```rust
//! # use fusillade::domain::request::transitions::RetryConfig;
//! let config = RetryConfig {
//!     max_retries: Some(1000),
//!     stop_before_deadline_ms: Some(900_000),
//!     backoff_ms: 1000,         // Start with 1 second
//!     backoff_factor: 2,        // Double each time (1s, 2s, 4s)
//!     max_backoff_ms: 60000,    // Cap at 60 seconds
//! };
//! ```

use std::sync::Arc;

use metrics::counter;
use tokio::sync::Mutex;

use crate::{
    FusilladeError,
    error::Result,
    http::{HttpClient, HttpResponse},
    manager::Storage,
};

use super::state::{
    Canceled, Claimed, Completed, DaemonId, Failed, FailureReason, Pending, Processing, Request,
    RequestCompletionResult,
};

/// Reason for cancelling a request.
#[derive(Debug, Clone, Copy)]
pub enum CancellationReason {
    /// User-initiated cancellation (should persist Canceled state).
    User,
    /// Daemon shutdown (abort HTTP but don't persist state change).
    Shutdown,
    /// Request superseded by racing pair (abort HTTP but don't persist - supersede_racing_pair already updated DB).
    Superseded,
}

impl Request<Pending> {
    pub async fn claim<S: Storage + ?Sized>(
        self,
        daemon_id: DaemonId,
        storage: &S,
    ) -> Result<Request<Claimed>> {
        let request = Request {
            data: self.data,
            state: Claimed {
                daemon_id,
                claimed_at: chrono::Utc::now(),
                retry_attempt: self.state.retry_attempt, // Carry over retry attempt
                batch_expires_at: self.state.batch_expires_at, // Carry over batch deadline
            },
        };
        storage.persist(&request).await?;
        Ok(request)
    }

    pub async fn cancel<S: Storage + ?Sized>(self, storage: &S) -> Result<Request<Canceled>> {
        let request = Request {
            data: self.data,
            state: Canceled {
                canceled_at: chrono::Utc::now(),
            },
        };
        storage.persist(&request).await?;
        Ok(request)
    }
}

impl Request<Claimed> {
    pub async fn unclaim<S: Storage + ?Sized>(self, storage: &S) -> Result<Request<Pending>> {
        let request = Request {
            data: self.data,
            state: Pending {
                retry_attempt: self.state.retry_attempt, // Preserve retry attempt
                not_before: None,                        // Can be claimed immediately
                batch_expires_at: self.state.batch_expires_at, // Carry over batch deadline
            },
        };
        storage.persist(&request).await?;
        Ok(request)
    }

    pub async fn cancel<S: Storage + ?Sized>(self, storage: &S) -> Result<Request<Canceled>> {
        let request = Request {
            data: self.data,
            state: Canceled {
                canceled_at: chrono::Utc::now(),
            },
        };
        storage.persist(&request).await?;
        Ok(request)
    }

    pub async fn process<H: HttpClient + 'static, S: Storage>(
        self,
        http_client: H,
        timeout_ms: u64,
        storage: &S,
    ) -> Result<Request<Processing>> {
        let request_data = self.data.clone();

        // Create a channel for the HTTP result
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        // Spawn the HTTP request as an async task
        let task_handle = tokio::spawn(async move {
            let result = http_client
                .execute(&request_data, &request_data.api_key, timeout_ms)
                .await;
            let _ = tx.send(result).await; // Ignore send errors (receiver dropped)
        });

        let processing_state = Processing {
            daemon_id: self.state.daemon_id,
            claimed_at: self.state.claimed_at,
            started_at: chrono::Utc::now(),
            retry_attempt: self.state.retry_attempt, // Carry over retry attempt
            batch_expires_at: self.state.batch_expires_at, // Carry over batch deadline
            result_rx: Arc::new(Mutex::new(rx)),
            abort_handle: task_handle.abort_handle(),
        };

        let request = Request {
            data: self.data,
            state: processing_state,
        };

        // Persist the Processing state so we can cancel it if needed
        // If persist fails, abort the spawned HTTP task
        if let Err(e) = storage.persist(&request).await {
            request.state.abort_handle.abort();
            return Err(e);
        }

        Ok(request)
    }
}

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: Option<u32>,
    pub stop_before_deadline_ms: Option<i64>,
    pub backoff_ms: u64,
    pub backoff_factor: u64,
    pub max_backoff_ms: u64,
}

impl From<&crate::daemon::DaemonConfig> for RetryConfig {
    fn from(config: &crate::daemon::DaemonConfig) -> Self {
        RetryConfig {
            max_retries: config.max_retries,
            stop_before_deadline_ms: config.stop_before_deadline_ms,
            backoff_ms: config.backoff_ms,
            backoff_factor: config.backoff_factor,
            max_backoff_ms: config.max_backoff_ms,
        }
    }
}

impl Request<Failed> {
    /// Attempt to retry this failed request.
    ///
    /// If retries are available, transitions the request back to Pending with:
    /// - Incremented retry_attempt
    /// - Calculated not_before timestamp for exponential backoff
    ///
    /// If no retries remain, returns None and the request stays Failed.
    ///
    /// The retry logic considers:
    /// - max_retries: Hard cap on total retry attempts
    /// - stop_before_deadline_ms: Deadline-aware retry (stops before batch expiration)
    pub fn can_retry(
        self,
        retry_attempt: u32,
        config: RetryConfig,
    ) -> std::result::Result<Request<Pending>, Box<Self>> {
        // Calculate exponential backoff: backoff_ms * (backoff_factor ^ retry_attempt)
        let backoff_duration = {
            let exponential = config
                .backoff_ms
                .saturating_mul(config.backoff_factor.saturating_pow(retry_attempt));
            exponential.min(config.max_backoff_ms)
        };

        let now = chrono::Utc::now();
        let not_before = now + chrono::Duration::milliseconds(backoff_duration as i64);

        if let Some(max_retries) = config.max_retries
            && retry_attempt >= max_retries
        {
            counter!(
                "fusillade_retry_denied_total",
                "model" => self.data.model.clone(),
                "reason" => "max_retries"
            )
            .increment(1);
            tracing::debug!(
                request_id = %self.data.id,
                retry_attempt,
                max_retries,
                "No retries remaining (reached max_retries), request remains failed"
            );
            return Err(Box::new(self));
        }

        // Determine the effective deadline (with or without buffer)
        let effective_deadline = if let Some(stop_before_deadline_ms) =
            config.stop_before_deadline_ms
        {
            self.state.batch_expires_at - chrono::Duration::milliseconds(stop_before_deadline_ms)
        } else {
            // No buffer configured - use the actual deadline
            self.state.batch_expires_at
        };

        // Check if the next retry would start before the effective deadline
        if not_before >= effective_deadline {
            counter!(
                "fusillade_retry_denied_total",
                "model" => self.data.model.clone(),
                "reason" => "deadline"
            )
            .increment(1);
            let time_until_deadline = self.state.batch_expires_at - now;
            tracing::warn!(
                request_id = %self.data.id,
                retry_attempt,
                time_until_deadline_seconds = time_until_deadline.num_seconds(),
                batch_expires_at = %self.state.batch_expires_at,
                stop_before_deadline_ms = config.stop_before_deadline_ms,
                "No retries remaining (would exceed batch deadline), request remains failed"
            );
            return Err(Box::new(self));
        }

        let time_until_deadline = effective_deadline - now;
        tracing::debug!(
            request_id = %self.data.id,
            retry_attempt,
            time_until_deadline_seconds = time_until_deadline.num_seconds(),
            batch_expires_at = %self.state.batch_expires_at,
            "Retrying (deadline-aware: time remaining)"
        );

        tracing::info!(
            request_id = %self.data.id,
            retry_attempt = retry_attempt + 1,
            backoff_ms = backoff_duration,
            not_before = %not_before,
            batch_expires_at = %self.state.batch_expires_at,
            "Retrying failed request with exponential backoff"
        );

        let request = Request {
            data: self.data,
            state: Pending {
                retry_attempt: retry_attempt + 1,
                not_before: Some(not_before),
                batch_expires_at: self.state.batch_expires_at,
            },
        };

        Ok(request)
    }
}

impl Request<Processing> {
    /// Wait for the HTTP request to complete.
    ///
    /// This method awaits the result from the spawned HTTP task and transitions
    /// the request to one of three terminal states: `Completed`, `Failed`, or `Canceled`.
    ///
    /// The `should_retry` predicate determines whether a response should be considered
    /// a failure (and thus eligible for retry) or a success.
    ///
    /// The `cancellation` future allows external cancellation of the request. It should
    /// resolve to a `CancellationReason`:
    /// - `CancellationReason::User`: User-initiated cancellation (persists Canceled state)
    /// - `CancellationReason::Shutdown`: Daemon shutdown (aborts HTTP but doesn't persist)
    /// - `CancellationReason::Superseded`: Request superseded by racing pair (aborts HTTP but doesn't persist)
    ///
    /// Returns:
    /// - `RequestCompletionResult::Completed` if the HTTP request succeeded
    /// - `RequestCompletionResult::Failed` if the HTTP request failed or should be retried
    /// - `RequestCompletionResult::Canceled` if the request was canceled by user
    /// - `Err(FusilladeError::Shutdown)` if the daemon is shutting down or request was superseded
    pub async fn complete<S, F, Fut>(
        self,
        storage: &S,
        should_retry: F,
        cancellation: Fut,
    ) -> Result<RequestCompletionResult>
    where
        S: Storage + ?Sized,
        F: Fn(&HttpResponse) -> bool,
        Fut: std::future::Future<Output = CancellationReason>,
    {
        // Await the result from the channel (lock the mutex to access the receiver)
        // We use an enum to track whether we got a result or cancellation so we can
        // drop the mutex guard before calling self.cancel()
        enum Outcome {
            Result(Option<std::result::Result<HttpResponse, FusilladeError>>),
            Canceled(CancellationReason),
        }

        let outcome = {
            let mut rx = self.state.result_rx.lock().await;

            tokio::select! {
                // Wait for the HTTP request to finish processing
                result = rx.recv() => Outcome::Result(result),
                // Handle cancellation
                reason = cancellation => Outcome::Canceled(reason),
            }
        };

        // Handle cancellation outside the mutex guard
        let result = match outcome {
            Outcome::Canceled(CancellationReason::User) => {
                // User cancellation: persist Canceled state
                // (self.cancel() will abort the HTTP task)
                let canceled = self.cancel(storage).await?;
                return Ok(RequestCompletionResult::Canceled(canceled));
            }
            Outcome::Canceled(CancellationReason::Shutdown) => {
                // Shutdown: abort HTTP task but don't persist state change
                // Request stays in Processing state and will be reclaimed later
                self.state.abort_handle.abort();
                return Err(FusilladeError::Shutdown);
            }
            Outcome::Canceled(CancellationReason::Superseded) => {
                // Superseded: abort HTTP task but don't persist state change
                // supersede_racing_pair already updated the DB to state='superseded'
                self.state.abort_handle.abort();
                return Err(FusilladeError::Shutdown);
            }
            Outcome::Result(result) => result,
        };

        match result {
            Some(Ok(http_response)) => {
                // Check if this is an error response (4xx or 5xx)
                let is_error = http_response.status >= 400;

                // Check if this response should be retried
                if should_retry(&http_response) {
                    // Record retriable HTTP status for observability
                    counter!(
                        "fusillade_http_status_retriable_total",
                        "model" => self.data.model.clone(),
                        "status" => http_response.status.to_string()
                    )
                    .increment(1);

                    // Treat as failure for retry purposes
                    let failed_state = Failed {
                        reason: FailureReason::RetriableHttpStatus {
                            status: http_response.status,
                            body: http_response.body.clone(),
                        },
                        failed_at: chrono::Utc::now(),
                        retry_attempt: self.state.retry_attempt,
                        batch_expires_at: self.state.batch_expires_at,
                    };
                    let request = Request {
                        data: self.data,
                        state: failed_state,
                    };
                    Ok(RequestCompletionResult::Failed(request))
                } else if is_error {
                    // Non-retriable error (e.g., 4xx client errors)
                    // Mark as failed but don't retry
                    let failed_state = Failed {
                        reason: FailureReason::NonRetriableHttpStatus {
                            status: http_response.status,
                            body: http_response.body.clone(),
                        },
                        failed_at: chrono::Utc::now(),
                        retry_attempt: self.state.retry_attempt,
                        batch_expires_at: self.state.batch_expires_at,
                    };
                    let request = Request {
                        data: self.data,
                        state: failed_state,
                    };
                    storage.persist(&request).await?;
                    Ok(RequestCompletionResult::Failed(request))
                } else {
                    // HTTP request completed successfully
                    let completed_state = Completed {
                        response_status: http_response.status,
                        response_body: http_response.body,
                        claimed_at: self.state.claimed_at,
                        started_at: self.state.started_at,
                        completed_at: chrono::Utc::now(),
                    };
                    let request = Request {
                        data: self.data,
                        state: completed_state,
                    };
                    storage.persist(&request).await?;
                    Ok(RequestCompletionResult::Completed(request))
                }
            }
            Some(Err(e)) => {
                let reason = match &e {
                    FusilladeError::HttpClient(reqwest_err) if reqwest_err.is_builder() => {
                        FailureReason::RequestBuilderError {
                            error: reqwest_err.to_string(),
                        }
                    }
                    _ => FailureReason::NetworkError {
                        error: crate::error::error_serialization::serialize_error(&e.into()),
                    },
                };

                let failed_state = Failed {
                    reason,
                    failed_at: chrono::Utc::now(),
                    retry_attempt: self.state.retry_attempt,
                    batch_expires_at: self.state.batch_expires_at,
                };
                let request = Request {
                    data: self.data,
                    state: failed_state,
                };
                Ok(RequestCompletionResult::Failed(request))
            }
            None => {
                // Channel closed - task died without sending a result
                let failed_state = Failed {
                    reason: FailureReason::TaskTerminated,
                    failed_at: chrono::Utc::now(),
                    retry_attempt: self.state.retry_attempt,
                    batch_expires_at: self.state.batch_expires_at,
                };
                let request = Request {
                    data: self.data,
                    state: failed_state,
                };
                storage.persist(&request).await?;
                Ok(RequestCompletionResult::Failed(request))
            }
        }
    }

    pub async fn cancel<S: Storage + ?Sized>(self, storage: &S) -> Result<Request<Canceled>> {
        // Abort the in-flight HTTP request
        self.state.abort_handle.abort();

        let request = Request {
            data: self.data,
            state: Canceled {
                canceled_at: chrono::Utc::now(),
            },
        };
        storage.persist(&request).await?;
        Ok(request)
    }
}
