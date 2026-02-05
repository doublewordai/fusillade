//! PostgreSQL implementation of Storage and DaemonExecutor.
//!
//! This implementation combines PostgreSQL storage with the daemon to provide
//! a production-ready batching system with persistent storage and real-time updates.

use crate::request::AnyRequest;
use futures::StreamExt;
pub use sqlx_pool_router::{PoolProvider, TestDbPools};
use std::pin::Pin;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::Stream;
use sqlx::QueryBuilder;
use sqlx::Row;
use sqlx::postgres::{PgListener, PgPool};
use std::collections::HashMap;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use super::{DaemonStorage, Storage};
use crate::batch::{
    Batch, BatchErrorDetails, BatchErrorItem, BatchId, BatchInput, BatchOutputItem,
    BatchResponseDetails, BatchStatus, File, FileContentItem, FileId, FileMetadata, FileStreamItem,
    OutputFileType, RequestTemplateInput, TemplateId,
};
use crate::daemon::{
    AnyDaemonRecord, Daemon, DaemonConfig, DaemonData, DaemonRecord, DaemonState, DaemonStatus,
    Dead, Initializing, Running,
};
use crate::error::{FusilladeError, Result};
use crate::http::HttpClient;
use crate::request::{
    Canceled, Claimed, Completed, DaemonId, Failed, FailureReason, Pending, Processing, Request,
    RequestData, RequestId, RequestState,
};

use super::DaemonExecutor;
use super::utils::{
    calculate_error_message_size, calculate_response_body_size, estimate_error_file_size,
    estimate_output_file_size,
};

/// PostgreSQL implementation of the Storage and DaemonExecutor traits.
///
/// This manager uses PostgreSQL for persistent storage and runs a daemon for processing requests.
/// It leverages Postgres LISTEN/NOTIFY for real-time status updates.
///
/// # Example
/// ```ignore
/// use fusillade::PostgresRequestManager;
/// use sqlx::PgPool;
///
/// let pool = PgPool::connect("postgresql://localhost/fusillade").await?;
/// let manager = Arc::new(PostgresRequestManager::new(TestDbPools::new(pool).await.unwrap()));
///
/// // Start processing
/// let handle = manager.clone().run()?;
///
/// // Create files and batches
/// let file_id = manager.create_file(name, description, templates).await?;
/// let batch_id = manager.create_batch(file_id).await?;
/// ```
/// Batch insert strategy for template insertion
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchInsertStrategy {
    /// Insert templates in batches using UNNEST (optimized for all file sizes)
    Batched { batch_size: usize },
}

impl Default for BatchInsertStrategy {
    fn default() -> Self {
        // Default to batched inserts with 5000 templates per batch
        BatchInsertStrategy::Batched { batch_size: 5000 }
    }
}

pub struct PostgresRequestManager<P: PoolProvider, H: HttpClient> {
    pools: P,
    http_client: Arc<H>,
    config: DaemonConfig,
    download_buffer_size: usize,
    batch_insert_strategy: BatchInsertStrategy,
}

/// Macro for extracting a [`Batch`] from a dynamic query row (PgRow).
///
/// This works with `sqlx::query()` (not `sqlx::query!()`) results where fields must
/// be accessed via `.get()` instead of direct access.
macro_rules! batch_from_dynamic_row {
    ($row:expr) => {
        Batch {
            id: BatchId($row.get("id")),
            file_id: $row.get::<Option<Uuid>, _>("file_id").map(FileId),
            endpoint: $row.get("endpoint"),
            completion_window: $row.get("completion_window"),
            metadata: $row.get("metadata"),
            output_file_id: $row.get::<Option<Uuid>, _>("output_file_id").map(FileId),
            error_file_id: $row.get::<Option<Uuid>, _>("error_file_id").map(FileId),
            created_by: $row.get("created_by"),
            created_at: $row.get("created_at"),
            expires_at: $row.get("expires_at"),
            cancelling_at: $row.get("cancelling_at"),
            errors: $row.get("errors"),
            total_requests: $row.get("total_requests"),
            requests_started_at: $row.get("requests_started_at"),
            finalizing_at: $row.get("finalizing_at"),
            completed_at: $row.get("completed_at"),
            failed_at: $row.get("failed_at"),
            cancelled_at: $row.get("cancelled_at"),
            deleted_at: $row.get("deleted_at"),
            pending_requests: $row.get("pending_requests"),
            in_progress_requests: $row.get("in_progress_requests"),
            completed_requests: $row.get("completed_requests"),
            failed_requests: $row.get("failed_requests"),
            canceled_requests: $row.get("canceled_requests"),
            notification_sent_at: $row.get("notification_sent_at"),
        }
    };
}

/// Macro for extracting a [`BatchStatus`] from a dynamic query row (PgRow).
macro_rules! batch_status_from_dynamic_row {
    ($row:expr) => {
        BatchStatus {
            batch_id: BatchId($row.get("batch_id")),
            file_id: $row.get::<Option<Uuid>, _>("file_id").map(FileId),
            file_name: $row.get("file_name"),
            total_requests: $row.get("total_requests"),
            pending_requests: $row.get("pending_requests"),
            in_progress_requests: $row.get("in_progress_requests"),
            completed_requests: $row.get("completed_requests"),
            failed_requests: $row.get("failed_requests"),
            canceled_requests: $row.get("canceled_requests"),
            started_at: $row.get("started_at"),
            created_at: $row.get("created_at"),
        }
    };
}

impl<P: PoolProvider> PostgresRequestManager<P, crate::http::ReqwestHttpClient> {
    /// Create a new PostgreSQL request manager with default settings.
    ///
    /// Uses the default Reqwest HTTP client and default daemon configuration.
    /// Customize with `.with_config()` if needed.
    ///
    /// # Example
    /// ```ignore
    /// use fusillade::{PostgresRequestManager, SinglePool};
    ///
    /// let pools = TestDbPools::new(pool).await.unwrap();
    /// let manager = PostgresRequestManager::new(pools)
    ///     .with_config(my_config);
    /// ```
    pub fn new(pools: P) -> Self {
        Self {
            pools,
            http_client: Arc::new(crate::http::ReqwestHttpClient::default()),
            config: DaemonConfig::default(),
            download_buffer_size: 100,
            batch_insert_strategy: BatchInsertStrategy::default(),
        }
    }
}

impl<P: PoolProvider, H: HttpClient + 'static> PostgresRequestManager<P, H> {
    /// Create a PostgreSQL request manager with a custom HTTP client.
    ///
    /// Uses the default daemon configuration. Customize with `.with_config()` if needed.
    ///
    /// # Example
    /// ```ignore
    /// use fusillade::SinglePool;
    ///
    /// let pools = TestDbPools::new(pool).await.unwrap();
    /// let manager = PostgresRequestManager::with_client(pools, Arc::new(my_client))
    ///     .with_config(my_config);
    /// ```
    pub fn with_client(pools: P, http_client: Arc<H>) -> Self {
        Self {
            pools,
            http_client,
            config: DaemonConfig::default(),
            download_buffer_size: 100,
            batch_insert_strategy: BatchInsertStrategy::default(),
        }
    }

    /// Set a custom daemon configuration.
    ///
    /// This is a builder method that can be chained after `new()` or `with_client()`.
    pub fn with_config(mut self, config: DaemonConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the download buffer size for file content streams.
    ///
    /// This is a builder method that can be chained after `new()` or `with_client()`.
    /// Default is 100.
    pub fn with_download_buffer_size(mut self, buffer_size: usize) -> Self {
        self.download_buffer_size = buffer_size;
        self
    }

    /// Set the batch insert strategy for template insertion.
    ///
    /// This is a builder method that can be chained after `new()` or `with_client()`.
    ///
    /// # Examples
    /// ```ignore
    /// // Use batched inserts with custom batch size
    /// let manager = PostgresRequestManager::new(pool)
    ///     .with_batch_insert_strategy(BatchInsertStrategy::Batched { batch_size: 10000 });
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `batch_size` is 0. The batch size must be at least 1 to avoid
    /// attempting to flush the buffer after every single template insertion,
    /// which would severely degrade performance.
    pub fn with_batch_insert_strategy(mut self, strategy: BatchInsertStrategy) -> Self {
        // Validate batch size
        match strategy {
            BatchInsertStrategy::Batched { batch_size } => {
                assert!(
                    batch_size > 0,
                    "batch_size must be greater than 0, got {}",
                    batch_size
                );
            }
        }
        self.batch_insert_strategy = strategy;
        self
    }

    /// Get SQL fragments for filtering failed requests based on SLA status.
    ///
    /// When `hide_retriable_before_sla` is true, generates SQL that filters per-batch
    /// based on each batch's expires_at timestamp, hiding retriable errors before SLA expiry.
    ///
    /// # Returns
    /// Tuple of (where_clause, failed_count):
    /// - where_clause: Additional WHERE condition for stream filtering (empty when not hiding)
    /// - failed_count: COUNT expression with CASE for per-batch SLA filtering
    fn error_filter_sql_fragments(hide_retriable_before_sla: bool) -> (&'static str, &'static str) {
        if hide_retriable_before_sla {
            (
                // For stream filtering: check batch SLA and apply filter
                "AND (b.expires_at <= NOW() OR is_retriable_error = false OR is_retriable_error IS NULL)",
                // For failed count: use CASE to check per-batch SLA status
                r#"CASE
                    WHEN b.expires_at > NOW() THEN COUNT(*) FILTER (WHERE state = 'failed' AND (is_retriable_error = false OR is_retriable_error IS NULL))
                    ELSE COUNT(*) FILTER (WHERE state = 'failed')
                END"#,
            )
        } else {
            // No filtering - show all errors
            ("", "COUNT(*) FILTER (WHERE state = 'failed')")
        }
    }

    /// Get the connection pool.
    /// Get the primary connection pool for write operations.
    ///
    /// For backward compatibility, this returns the write pool (primary).
    /// Use the pool provider's `.read()` and `.write()` methods directly
    /// for explicit read/write routing.
    pub fn pool(&self) -> &PgPool {
        self.pools.write()
    }

    /// Create a listener for real-time request updates.
    ///
    /// This returns a PgListener that can be used to receive notifications
    /// when requests are updated. Uses the write pool (primary) for consistency.
    pub async fn create_listener(&self) -> Result<PgListener> {
        PgListener::connect_with(self.pools.write())
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to create listener: {}", e)))
    }
}

// Additional methods for PostgresRequestManager (not part of Storage trait)
impl<P: PoolProvider, H: HttpClient + 'static> PostgresRequestManager<P, H> {
    /// Unclaim stale requests that have been stuck in "claimed" or "processing" states
    /// for longer than the configured timeouts. This handles daemon crashes.
    ///
    /// Returns the number of requests that were unclaimed. Limited by `unclaim_batch_size`
    /// to prevent unbounded database load when many requests become stale simultaneously.
    #[tracing::instrument(skip(self))]
    async fn unclaim_stale_requests(&self) -> Result<usize> {
        let claim_timeout_ms = self.config.claim_timeout_ms as i64;
        let processing_timeout_ms = self.config.processing_timeout_ms as i64;
        let limit = self.config.unclaim_batch_size as i64;

        // Unclaim requests that are stuck in claimed or processing states.
        // Uses a subquery with LIMIT to bound the number of rows updated per poll cycle.
        let result = sqlx::query!(
            r#"
            UPDATE requests
            SET
                state = 'pending',
                daemon_id = NULL,
                claimed_at = NULL,
                started_at = NULL
            WHERE id IN (
                SELECT id FROM requests
                WHERE
                    (state = 'claimed' AND claimed_at < NOW() - ($1 || ' milliseconds')::INTERVAL)
                    OR
                    (state = 'processing' AND started_at < NOW() - ($2 || ' milliseconds')::INTERVAL)
                LIMIT $3
            )
            "#,
            claim_timeout_ms.to_string(),
            processing_timeout_ms.to_string(),
            limit,
        )
        .execute(self.pools.write())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to unclaim stale requests: {}", e)))?;

        let count = result.rows_affected() as usize;

        if count > 0 {
            tracing::warn!(
                count = count,
                claim_timeout_ms,
                processing_timeout_ms,
                "Unclaimed stale requests (likely due to daemon crash)"
            );
        }

        Ok(count)
    }

    /// Check if a file should be expired and mark it as such.
    /// Returns true if the file was marked as expired.
    async fn check_and_mark_expired(&self, file: &mut File) -> Result<bool> {
        // Only check files that are currently in 'processed' status
        if file.status != crate::batch::FileStatus::Processed {
            return Ok(false);
        }

        // Check if file has an expiration date and it has passed
        if let Some(expires_at) = file.expires_at
            && Utc::now() > expires_at
        {
            // Mark as expired in the database
            sqlx::query!(
                r#"
                UPDATE files
                SET status = 'expired'
                WHERE id = $1 AND status = 'processed'
                "#,
                *file.id as Uuid,
            )
            .execute(self.pools.write())
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to mark file as expired: {}", e)))?;

            // Update the in-memory file object
            file.status = crate::batch::FileStatus::Expired;
            return Ok(true);
        }

        Ok(false)
    }

    /// Generate a consistent advisory lock key for a file
    fn file_lock_key(file_id: FileId) -> i64 {
        file_id.0.as_u128() as u64 as i64
    }

    /// Calculate the estimated file size for a virtual batch file.
    /// Returns None if this isn't a virtual file or calculation isn't possible.
    fn calculate_virtual_file_size_from_batch(
        &self,
        batch: &Batch,
        file_type: OutputFileType,
        raw_size_sum: i64,
    ) -> Option<i64> {
        let request_count = match file_type {
            OutputFileType::Output => batch.completed_requests,
            OutputFileType::Error => batch.failed_requests,
        };

        if request_count == 0 {
            return Some(0);
        }

        // Add JSONL overhead to get estimated file size - return directly
        match file_type {
            OutputFileType::Output => estimate_output_file_size(raw_size_sum, request_count, None),
            OutputFileType::Error => estimate_error_file_size(raw_size_sum, request_count, None),
        }
    }

    /// Calculate estimated file size from a list_files query row.
    /// Returns None if this isn't a virtual file or if it's already finalized.
    fn calculate_virtual_file_size_from_row(
        &self,
        row: &sqlx::postgres::PgRow,
        purpose: &Option<crate::batch::Purpose>,
        size_finalized: bool,
    ) -> Result<Option<i64>> {
        // Skip if already finalized or not a virtual file
        if size_finalized
            || (purpose != &Some(crate::batch::Purpose::BatchOutput)
                && purpose != &Some(crate::batch::Purpose::BatchError))
        {
            return Ok(None);
        }

        // Get raw size sum from LATERAL join
        let raw_size_sum: Option<i64> = row.try_get("calculated_size").ok().flatten();
        let raw_sum = raw_size_sum.unwrap_or(0);

        // Get request count for this file type
        let completed: Option<i64> = row.try_get("completed_requests").ok().flatten();
        let failed: Option<i64> = row.try_get("failed_requests").ok().flatten();

        let request_count = if purpose == &Some(crate::batch::Purpose::BatchOutput) {
            completed.unwrap_or(0)
        } else {
            failed.unwrap_or(0)
        };

        if request_count == 0 {
            return Ok(Some(0));
        }

        // Add JSONL overhead
        let estimated_size = if purpose == &Some(crate::batch::Purpose::BatchOutput) {
            estimate_output_file_size(raw_sum, request_count, None)
        } else {
            estimate_error_file_size(raw_sum, request_count, None)
        };

        // If estimation failed (overflow), log a warning and return None
        if estimated_size.is_none() {
            tracing::warn!(
                "File size estimation overflow for {:?} file with {} requests",
                purpose,
                request_count
            );
        }

        Ok(estimated_size)
    }

    /// Check if a batch is complete based on request counts.
    fn is_batch_complete(batch: &Batch) -> bool {
        let terminal_count =
            batch.completed_requests + batch.failed_requests + batch.canceled_requests;
        terminal_count == batch.total_requests && batch.total_requests > 0
    }

    /// Finalize a virtual file's size in the database.
    /// Uses an advisory lock to prevent concurrent finalization.
    /// Returns whether the finalization was performed.
    async fn finalize_file_size(
        pool: &PgPool,
        file_id: FileId,
        estimated_size: i64,
    ) -> Result<bool> {
        let lock_key = Self::file_lock_key(file_id);

        // Try to acquire advisory lock (non-blocking)
        let lock_acquired = match sqlx::query_scalar!("SELECT pg_try_advisory_lock($1)", lock_key)
            .fetch_one(pool)
            .await
        {
            Ok(Some(acquired)) => acquired,
            Ok(None) => {
                // Unexpected - pg_try_advisory_lock shouldn't return NULL
                tracing::warn!(
                    file_id = %file_id,
                    "Advisory lock query returned NULL unexpectedly"
                );
                false
            }
            Err(e) => {
                // Database error - this IS a problem
                tracing::error!(
                    file_id = %file_id,
                    error = %e,
                    "Database error while trying to acquire advisory lock"
                );
                return Err(FusilladeError::Other(anyhow!(
                    "Failed to acquire lock: {}",
                    e
                )));
            }
        };

        if !lock_acquired {
            // Another process is finalizing
            return Ok(false);
        }

        // We have the lock - finalize the file
        let result = sqlx::query!(
            r#"
            UPDATE files
            SET size_bytes = $2, size_finalized = TRUE
            WHERE id = $1 AND size_finalized = FALSE
            "#,
            *file_id as Uuid,
            estimated_size,
        )
        .execute(pool)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to update file size: {}", e)));

        // Release the lock - only log if release fails (which is unusual)
        if let Err(e) = sqlx::query_scalar!("SELECT pg_advisory_unlock($1)", lock_key)
            .fetch_one(pool)
            .await
        {
            tracing::warn!(
                file_id = %file_id,
                error = %e,
                "Failed to release advisory lock (will be released on connection return to pool)"
            );
        }

        result?;
        Ok(true)
    }

    /// Spawn a background task to finalize a completed batch's virtual file.
    /// Returns immediately - finalization happens asynchronously.
    fn spawn_finalize_if_complete(
        &self,
        row: &sqlx::postgres::PgRow,
        file_id: FileId,
        estimated_size: i64,
    ) {
        // Check if batch is complete
        let total: Option<i64> = row.try_get("total_requests").ok().flatten();
        let completed: Option<i64> = row.try_get("completed_requests").ok().flatten();
        let failed: Option<i64> = row.try_get("failed_requests").ok().flatten();
        let canceled: Option<i64> = row.try_get("canceled_requests").ok().flatten();
        let in_progress: Option<i64> = row.try_get("in_progress_requests").ok().flatten();

        if let (Some(total_count), Some(comp), Some(fail), Some(canc), Some(_prog)) =
            (total, completed, failed, canceled, in_progress)
        {
            let terminal_count = comp + fail + canc;
            let is_complete = terminal_count == total_count && total_count > 0;

            if is_complete {
                // Spawn background finalization - don't block listing
                let pool = self.pools.write().clone();

                tokio::spawn(async move {
                    if let Err(e) = Self::finalize_file_size(&pool, file_id, estimated_size).await {
                        tracing::warn!("Failed to finalize file size for {}: {}", file_id, e);
                    }
                });
            }
        }
    }

    async fn maybe_finalize_file_size(&self, file: &mut File) -> Result<()> {
        // Only process virtual output/error files that are not yet finalized
        if file.size_finalized {
            return Ok(());
        }

        let file_type = match file.purpose {
            Some(crate::batch::Purpose::BatchOutput) => OutputFileType::Output,
            Some(crate::batch::Purpose::BatchError) => OutputFileType::Error,
            _ => return Ok(()),
        };

        // Find the batch that owns this file
        let batch = match self.get_batch_by_output_file_id(file.id, file_type).await? {
            Some(b) => b,
            None => return Ok(()),
        };

        let state_filter = match file_type {
            OutputFileType::Output => "completed",
            OutputFileType::Error => "failed",
        };

        let raw_size_sum = sqlx::query_scalar!(
            r#"
            SELECT COALESCE(SUM(response_size), 0)::BIGINT as "sum!"
            FROM requests
            WHERE batch_id = $1
              AND state = $2
            "#,
            *batch.id as Uuid,
            state_filter,
        )
        .fetch_one(self.pools.write())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to calculate file size: {}", e)))?;

        // Calculate estimated size with JSONL overhead using shared helper
        let estimated_size = self
            .calculate_virtual_file_size_from_batch(&batch, file_type, raw_size_sum)
            .unwrap_or(0);

        // Update with JSONL-formatted estimate
        file.size_bytes = estimated_size;

        // Early exit if batch is not complete
        if !Self::is_batch_complete(&batch) {
            return Ok(());
        }

        // Batch is complete - try to finalize this file
        let finalized =
            Self::finalize_file_size(self.pools.write(), file.id, estimated_size).await?;

        if finalized {
            file.size_finalized = true;
        }

        Ok(())
    }

    /// Internal helper to fetch a file from a specific pool.
    ///
    /// Accepts a pool parameter to control read-after-write consistency.
    /// Typically used with the primary pool for immediate reads after writes,
    /// or replica pools for normal reads.
    async fn get_file_from_pool(&self, file_id: FileId, pool: &PgPool) -> Result<File> {
        let row = sqlx::query!(
            r#"
            SELECT id, name, description, size_bytes, size_finalized, status, error_message, purpose, expires_at, deleted_at, uploaded_by, created_at, updated_at
            FROM files
            WHERE id = $1 AND deleted_at IS NULL
            "#,
            *file_id as Uuid,
        )
        .fetch_optional(pool)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch file: {}", e)))?
        .ok_or_else(|| FusilladeError::Other(anyhow!("File not found")))?;

        let status = row
            .status
            .parse::<crate::batch::FileStatus>()
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Invalid file status '{}': {}", row.status, e))
            })?;

        let purpose = row
            .purpose
            .map(|s| s.parse::<crate::batch::Purpose>())
            .transpose()
            .map_err(|e| FusilladeError::Other(anyhow!("Invalid purpose: {}", e)))?;

        Ok(File {
            id: FileId(row.id),
            name: row.name,
            description: row.description,
            size_bytes: row.size_bytes,
            size_finalized: row.size_finalized,
            status,
            error_message: row.error_message,
            purpose,
            expires_at: row.expires_at,
            deleted_at: row.deleted_at,
            uploaded_by: row.uploaded_by,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

// Implement Storage trait directly (no delegation)
#[async_trait]
/// Returns counts of **claimable** pending requests grouped by model and expiry window.
///
/// This intentionally excludes:
/// - Requests without a template (`template_id IS NULL`), which are not claimable.
/// - Requests from batches that are being cancelled (`b.cancelling_at IS NOT NULL`).
///
/// If you need counts of all pending requests regardless of claimability, adjust the query
/// to remove these filters.
impl<P: PoolProvider, H: HttpClient + 'static> Storage for PostgresRequestManager<P, H> {
    async fn get_pending_request_counts_by_model_and_completion_window(
        &self,
    ) -> Result<HashMap<String, HashMap<String, i64>>> {
        let rows = sqlx::query!(
            r#"
            WITH windows(label, window_interval) AS (
                VALUES
                    ('1h', INTERVAL '1 hour'),
                    ('24h', INTERVAL '24 hours')
            )
            SELECT
                r.model as "model!",
                w.label as "completion_window!",
                COUNT(*) FILTER (
                    WHERE b.expires_at <= NOW() + w.window_interval
                )::BIGINT as "count!"
            FROM requests r
            JOIN batches b ON r.batch_id = b.id
            CROSS JOIN windows w
            WHERE r.state = 'pending'
              AND r.template_id IS NOT NULL
              AND b.cancelling_at IS NULL
            GROUP BY r.model, w.label
            "#
        )
        .fetch_all(self.pools.read())
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Failed to get pending request counts by model and completion window: {}",
                e
            ))
        })?;

        let mut result: HashMap<String, HashMap<String, i64>> = HashMap::new();
        for row in rows {
            result
                .entry(row.model)
                .or_default()
                .insert(row.completion_window, row.count);
        }

        Ok(result)
    }

    #[tracing::instrument(skip(self), fields(limit))]
    async fn claim_requests(
        &self,
        limit: usize,
        daemon_id: DaemonId,
    ) -> Result<Vec<Request<Claimed>>> {
        // First, unclaim any stale requests (self-healing for daemon crashes)
        let unclaimed_count = self.unclaim_stale_requests().await?;
        if unclaimed_count > 0 {
            tracing::info!(
                unclaimed_count,
                "Unclaimed stale requests before claiming new ones"
            );
        }

        let now = Utc::now();

        // Get all models with pending requests
        // Exclude requests from cancelled or soft-deleted batches
        let mut models = sqlx::query_scalar!(
            r#"
            SELECT DISTINCT r.model
            FROM requests r
            JOIN batches b ON r.batch_id = b.id
            WHERE r.state = 'pending'
                AND (r.not_before IS NULL OR r.not_before <= $1)
                AND b.cancelling_at IS NULL
                AND b.deleted_at IS NULL
            "#,
            now
        )
        .fetch_all(self.pools.write())
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to get models with pending requests: {}", e))
        })?;

        // Randomize model order to prevent starvation when hitting global limit
        {
            use rand::seq::SliceRandom;
            let mut rng = rand::rng();
            models.shuffle(&mut rng);
        } // Drop rng before async operations

        tracing::debug!(
            model_count = models.len(),
            "Found models with pending requests"
        );

        // Claim from models sequentially until we hit the global limit
        let mut all_claimed = Vec::new();
        let mut remaining_limit = limit;

        for model in models {
            if remaining_limit == 0 {
                break;
            }

            let model_limit = self
                .config
                .model_concurrency_limits
                .get(&model)
                .map(|entry| *entry.value())
                .unwrap_or(self.config.default_model_concurrency);

            // Atomically count in-progress requests and claim available slots in a single query
            // This ensures the count and claim are consistent (no race condition)
            let rows = sqlx::query!(
                r#"
                WITH in_progress_count AS (
                    SELECT COUNT(*)::BIGINT as count
                    FROM active_requests r
                    WHERE r.model = $4
                        AND r.state IN ('claimed', 'processing')
                ),
                available_slots AS (
                    SELECT GREATEST(0, $2::BIGINT - (SELECT count FROM in_progress_count)) as slots
                ),
                to_claim AS (
                    SELECT r.id, r.template_id, r.batch_id,
                           b.id::TEXT as batch_id_str,
                           b.file_id::TEXT as batch_file_id,
                           b.endpoint as batch_endpoint,
                           b.completion_window as batch_completion_window,
                           b.metadata::TEXT as batch_metadata,
                           b.output_file_id::TEXT as batch_output_file_id,
                           b.error_file_id::TEXT as batch_error_file_id,
                           COALESCE(b.created_by, '') as batch_created_by,
                           to_char(b.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as batch_created_at,
                           to_char(b.expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as batch_expires_at,
                           to_char(b.cancelling_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as batch_cancelling_at,
                           b.errors::TEXT as batch_errors,
                           b.total_requests::TEXT as batch_total_requests
                    FROM requests r
                    JOIN batches b ON r.batch_id = b.id
                    CROSS JOIN available_slots
                    WHERE r.state = 'pending'
                        AND r.model = $4
                        AND r.template_id IS NOT NULL
                        AND (r.not_before IS NULL OR r.not_before <= $3)
                        AND b.cancelling_at IS NULL
                        AND b.deleted_at IS NULL
                        AND available_slots.slots > 0
                    ORDER BY b.expires_at ASC
                    LIMIT LEAST($5, (SELECT slots FROM available_slots))
                    FOR UPDATE OF r SKIP LOCKED
                )
                UPDATE requests r
                SET
                    state = 'claimed',
                    daemon_id = $1,
                    claimed_at = $3
                FROM to_claim tc
                JOIN active_request_templates t ON tc.template_id = t.id
                JOIN batches b ON tc.batch_id = b.id
                WHERE r.id = tc.id
                RETURNING r.id, r.batch_id as "batch_id!", r.template_id as "template_id!", r.retry_attempt,
                          t.custom_id, t.endpoint as "endpoint!", t.method as "method!", t.path as "path!",
                          t.body as "body!", t.model as "model!", t.api_key as "api_key!",
                          b.expires_at as batch_expires_at,
                          tc.batch_id_str as "batch_id_str!",
                          tc.batch_file_id as "batch_file_id!",
                          tc.batch_endpoint as "batch_endpoint!",
                          tc.batch_completion_window as "batch_completion_window!",
                          tc.batch_metadata as "batch_metadata",
                          tc.batch_output_file_id as "batch_output_file_id",
                          tc.batch_error_file_id as "batch_error_file_id",
                          tc.batch_created_by as "batch_created_by!",
                          tc.batch_created_at as "batch_created_at!",
                          tc.batch_expires_at as "batch_expires_at_str",
                          tc.batch_cancelling_at as "batch_cancelling_at",
                          tc.batch_errors as "batch_errors",
                          tc.batch_total_requests as "batch_total_requests!"
                "#,
                *daemon_id as Uuid,
                model_limit as i64,
                now,
                &model,
                remaining_limit as i64,
            )
            .fetch_all(self.pools.write())
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!(
                    "Failed to claim requests for model {}: {}",
                    model,
                    e
                ))
            })?;

            let claimed_count = rows.len();
            if claimed_count > 0 {
                tracing::debug!(
                    model = %model,
                    claimed = claimed_count,
                    remaining_limit = remaining_limit - claimed_count,
                    "Claimed requests for model"
                );

                remaining_limit -= claimed_count;

                // Cache parsed metadata JSON per batch to avoid re-parsing for each request
                let mut parsed_metadata_cache: std::collections::HashMap<
                    Uuid,
                    Option<serde_json::Value>,
                > = std::collections::HashMap::new();

                all_claimed.extend(rows.into_iter().map(|row| {
                    // Build batch metadata HashMap from configured fields
                    let mut batch_metadata = std::collections::HashMap::new();

                    // Get or parse the metadata JSON for this batch
                    let parsed_metadata =
                        parsed_metadata_cache
                            .entry(row.batch_id)
                            .or_insert_with(|| {
                                row.batch_metadata
                                    .as_deref()
                                    .and_then(|s| serde_json::from_str(s).ok())
                            });

                    for field_name in &self.config.batch_metadata_fields {
                        // First check if it's a known column field
                        let value: Option<&str> = match field_name.as_str() {
                            "id" => Some(&row.batch_id_str),
                            "file_id" => Some(&row.batch_file_id),
                            "endpoint" => Some(&row.batch_endpoint),
                            "completion_window" => Some(&row.batch_completion_window),
                            "metadata" => row.batch_metadata.as_deref(),
                            "output_file_id" => row.batch_output_file_id.as_deref(),
                            "error_file_id" => row.batch_error_file_id.as_deref(),
                            "created_by" => Some(&row.batch_created_by),
                            "created_at" => Some(&row.batch_created_at),
                            "expires_at" => row.batch_expires_at_str.as_deref(),
                            "cancelling_at" => row.batch_cancelling_at.as_deref(),
                            "errors" => row.batch_errors.as_deref(),
                            "total_requests" => Some(&row.batch_total_requests),
                            _ => None,
                        };

                        if let Some(v) = value {
                            batch_metadata.insert(field_name.clone(), v.to_string());
                        } else if let Some(metadata_json) = parsed_metadata.as_ref() {
                            // Fall back to extracting from metadata JSON for unknown field names
                            if let Some(v) = metadata_json.get(field_name).and_then(|v| v.as_str())
                            {
                                batch_metadata.insert(field_name.clone(), v.to_string());
                            }
                        }
                    }

                    Request {
                        state: Claimed {
                            daemon_id,
                            claimed_at: now,
                            retry_attempt: row.retry_attempt as u32,
                            batch_expires_at: row.batch_expires_at,
                        },
                        data: RequestData {
                            id: RequestId(row.id),
                            batch_id: BatchId(row.batch_id),
                            template_id: TemplateId(row.template_id),
                            custom_id: row.custom_id,
                            endpoint: row.endpoint,
                            method: row.method,
                            path: row.path,
                            body: row.body,
                            model: row.model,
                            api_key: row.api_key,
                            batch_metadata,
                        },
                    }
                }));
            }
        }

        tracing::debug!(
            total_claimed = all_claimed.len(),
            "Finished claiming requests across all models"
        );

        Ok(all_claimed)
    }

    #[tracing::instrument(skip(self, request), fields(request_id = %request.data.id))]
    async fn persist<T: RequestState + Clone>(
        &self,
        request: &Request<T>,
    ) -> Result<Option<RequestId>>
    where
        AnyRequest: From<Request<T>>,
    {
        let any_request = AnyRequest::from(request.clone());

        match any_request {
            AnyRequest::Pending(req) => {
                let rows_affected = sqlx::query!(
                    r#"
                    UPDATE requests SET
                        state = 'pending',
                        retry_attempt = $2,
                        not_before = $3,
                        daemon_id = NULL,
                        claimed_at = NULL,
                        started_at = NULL
                    WHERE id = $1
                    "#,
                    *req.data.id as Uuid,
                    req.state.retry_attempt as i32,
                    req.state.not_before,
                )
                .execute(self.pools.write())
                .await
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to update request: {}", e)))?
                .rows_affected();

                if rows_affected == 0 {
                    return Err(FusilladeError::RequestNotFound(req.data.id));
                }
            }
            AnyRequest::Claimed(req) => {
                let rows_affected = sqlx::query!(
                    r#"
                    UPDATE requests SET
                        state = 'claimed',
                        retry_attempt = $2,
                        daemon_id = $3,
                        claimed_at = $4,
                        started_at = NULL,
                        not_before = NULL
                    WHERE id = $1
                    "#,
                    *req.data.id as Uuid,
                    req.state.retry_attempt as i32,
                    *req.state.daemon_id as Uuid,
                    req.state.claimed_at,
                )
                .execute(self.pools.write())
                .await
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to update request: {}", e)))?
                .rows_affected();

                if rows_affected == 0 {
                    return Err(FusilladeError::RequestNotFound(req.data.id));
                }
            }
            AnyRequest::Processing(req) => {
                let rows_affected = sqlx::query!(
                    r#"
                    UPDATE requests SET
                        state = 'processing',
                        retry_attempt = $2,
                        daemon_id = $3,
                        claimed_at = $4,
                        started_at = $5
                    WHERE id = $1
                    "#,
                    *req.data.id as Uuid,
                    req.state.retry_attempt as i32,
                    *req.state.daemon_id as Uuid,
                    req.state.claimed_at,
                    req.state.started_at,
                )
                .execute(self.pools.write())
                .await
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to update request: {}", e)))?
                .rows_affected();

                if rows_affected == 0 {
                    return Err(FusilladeError::RequestNotFound(req.data.id));
                }
            }
            AnyRequest::Completed(req) => {
                // Store the raw response body size
                let response_size = calculate_response_body_size(&req.state.response_body)
                    .ok_or_else(|| FusilladeError::Other(anyhow!("Response body too large")))?;

                let rows_affected = sqlx::query!(
                    r#"
                    UPDATE requests SET
                        state = 'completed',
                        response_status = $2,
                        response_body = $3,
                        claimed_at = $4,
                        started_at = $5,
                        completed_at = $6,
                        response_size = $7,
                        routed_model = $8
                    WHERE id = $1
                    "#,
                    *req.data.id as Uuid,
                    req.state.response_status as i16,
                    req.state.response_body,
                    req.state.claimed_at,
                    req.state.started_at,
                    req.state.completed_at,
                    response_size,
                    req.state.routed_model,
                )
                .execute(self.pools.write())
                .await
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to update request: {}", e)))?
                .rows_affected();

                if rows_affected == 0 {
                    return Err(FusilladeError::RequestNotFound(req.data.id));
                }
            }
            AnyRequest::Failed(req) => {
                // Serialize FailureReason as JSON
                let error_json = serde_json::to_string(&req.state.reason).map_err(|e| {
                    FusilladeError::Other(anyhow!("Failed to serialize failure reason: {}", e))
                })?;

                // Store raw error message size
                let response_size = calculate_error_message_size(&error_json)
                    .ok_or_else(|| FusilladeError::Other(anyhow!("Error message too large")))?;

                // Determine if this is a retriable error
                let is_retriable_error = req.state.reason.is_retriable();

                let rows_affected = sqlx::query!(
                    r#"
                    UPDATE requests SET
                        state = 'failed',
                        retry_attempt = $2,
                        error = $3,
                        failed_at = $4,
                        response_size = $5,
                        routed_model = $6,
                        is_retriable_error = $7
                    WHERE id = $1
                    "#,
                    *req.data.id as Uuid,
                    req.state.retry_attempt as i32,
                    error_json,
                    req.state.failed_at,
                    response_size,
                    req.state.routed_model,
                    is_retriable_error,
                )
                .execute(self.pools.write())
                .await
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to update request: {}", e)))?
                .rows_affected();

                if rows_affected == 0 {
                    return Err(FusilladeError::RequestNotFound(req.data.id));
                }
            }
            AnyRequest::Canceled(req) => {
                let rows_affected = sqlx::query!(
                    r#"
                    UPDATE requests SET
                        state = 'canceled',
                        canceled_at = $2
                    WHERE id = $1
                    "#,
                    *req.data.id as Uuid,
                    req.state.canceled_at,
                )
                .execute(self.pools.write())
                .await
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to update request: {}", e)))?
                .rows_affected();

                if rows_affected == 0 {
                    return Err(FusilladeError::RequestNotFound(req.data.id));
                }
            }
        }

        Ok(None)
    }

    #[tracing::instrument(skip(self, ids), fields(count = ids.len()))]
    async fn get_requests(&self, ids: Vec<RequestId>) -> Result<Vec<Result<AnyRequest>>> {
        let uuid_ids: Vec<Uuid> = ids.iter().map(|id| **id).collect();

        let rows = sqlx::query!(
            r#"
            SELECT
                r.id, r.batch_id as "batch_id!", r.template_id as "template_id?", r.state,
                t.custom_id as "custom_id?", t.endpoint as "endpoint?", t.method as "method?",
                t.path as "path?", t.body as "body?", t.model as "model?", t.api_key as "api_key?",
                r.retry_attempt, r.not_before, r.daemon_id, r.claimed_at, r.started_at,
                r.response_status, r.response_body, r.completed_at, r.error, r.failed_at, r.canceled_at,
                b.expires_at as batch_expires_at, r.routed_model
            FROM requests r
            LEFT JOIN active_request_templates t ON r.template_id = t.id
            JOIN batches b ON r.batch_id = b.id
            WHERE r.id = ANY($1)
            "#,
            &uuid_ids,
        )
        .fetch_all(self.pools.read())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch requests: {}", e)))?;

        // Build a map of id -> request for efficient lookup
        let mut request_map: std::collections::HashMap<RequestId, Result<AnyRequest>> =
            std::collections::HashMap::new();

        for row in rows {
            let request_id = RequestId(row.id);

            // Check if template data exists (template may have been deleted)
            let data = match (
                row.template_id,
                row.endpoint,
                row.method,
                row.path,
                row.body,
                row.model,
                row.api_key,
            ) {
                (
                    Some(template_id),
                    Some(endpoint),
                    Some(method),
                    Some(path),
                    Some(body),
                    Some(model),
                    Some(api_key),
                ) => RequestData {
                    id: request_id,
                    batch_id: BatchId(row.batch_id),
                    template_id: TemplateId(template_id),
                    custom_id: row.custom_id,
                    endpoint,
                    method,
                    path,
                    body,
                    model,
                    api_key,
                    batch_metadata: std::collections::HashMap::new(),
                },
                _ => {
                    // Template was deleted - cannot reconstruct request
                    request_map.insert(
                        request_id,
                        Err(FusilladeError::Other(anyhow!(
                            "Request template has been deleted"
                        ))),
                    );
                    continue;
                }
            };

            let state = &row.state;

            let any_request = match state.as_str() {
                "pending" => Ok(AnyRequest::Pending(Request {
                    state: Pending {
                        retry_attempt: row.retry_attempt as u32,
                        not_before: row.not_before,
                        batch_expires_at: row.batch_expires_at,
                    },
                    data,
                })),
                "claimed" => Ok(AnyRequest::Claimed(Request {
                    state: Claimed {
                        daemon_id: DaemonId(row.daemon_id.ok_or_else(|| {
                            FusilladeError::Other(anyhow!("Missing daemon_id for claimed request"))
                        })?),
                        claimed_at: row.claimed_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!("Missing claimed_at for claimed request"))
                        })?,
                        retry_attempt: row.retry_attempt as u32,
                        batch_expires_at: row.batch_expires_at,
                    },
                    data,
                })),
                "processing" => {
                    // TODO: fix this - creating dummy channels is ugly but works for now
                    // Create a "read-only" Processing state for status display.
                    // The channel fields are marked #[serde(skip)] and won't be serialized anyway.
                    let (_tx, rx) = tokio::sync::mpsc::channel(1);
                    // Create a dummy abort handle (from a noop task)
                    let abort_handle = tokio::spawn(async {}).abort_handle();
                    Ok(AnyRequest::Processing(Request {
                        state: Processing {
                            daemon_id: DaemonId(row.daemon_id.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing daemon_id for processing request"
                                ))
                            })?),
                            claimed_at: row.claimed_at.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing claimed_at for processing request"
                                ))
                            })?,
                            started_at: row.started_at.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing started_at for processing request"
                                ))
                            })?,
                            retry_attempt: row.retry_attempt as u32,
                            batch_expires_at: row.batch_expires_at,
                            result_rx: Arc::new(Mutex::new(rx)),
                            abort_handle,
                        },
                        data,
                    }))
                }
                "completed" => Ok(AnyRequest::Completed(Request {
                    state: Completed {
                        response_status: row.response_status.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing response_status for completed request"
                            ))
                        })? as u16,
                        response_body: row.response_body.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing response_body for completed request"
                            ))
                        })?,
                        claimed_at: row.claimed_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing claimed_at for completed request"
                            ))
                        })?,
                        started_at: row.started_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing started_at for completed request"
                            ))
                        })?,
                        completed_at: row.completed_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing completed_at for completed request"
                            ))
                        })?,
                        // Fall back to template model for old data without routed_model
                        routed_model: row.routed_model.unwrap_or_else(|| data.model.clone()),
                    },
                    data,
                })),
                "failed" => {
                    let error_json = row.error.ok_or_else(|| {
                        FusilladeError::Other(anyhow!("Missing error for failed request"))
                    })?;

                    // Try to deserialize as FailureReason, fall back to NetworkError for old data
                    let reason: FailureReason =
                        serde_json::from_str(&error_json).unwrap_or_else(|_| {
                            // If deserialization fails, treat it as a legacy error string
                            // and wrap it as a NetworkError for backwards compatibility
                            FailureReason::NetworkError {
                                error: error_json.clone(),
                            }
                        });

                    Ok(AnyRequest::Failed(Request {
                        state: Failed {
                            reason,
                            failed_at: row.failed_at.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing failed_at for failed request"
                                ))
                            })?,
                            retry_attempt: row.retry_attempt as u32,
                            batch_expires_at: row.batch_expires_at,
                            // Fall back to template model for old data without routed_model
                            routed_model: row.routed_model.unwrap_or_else(|| data.model.clone()),
                        },
                        data,
                    }))
                }
                "canceled" => Ok(AnyRequest::Canceled(Request {
                    state: Canceled {
                        canceled_at: row.canceled_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing canceled_at for canceled request"
                            ))
                        })?,
                    },
                    data,
                })),
                _ => Err(FusilladeError::Other(anyhow!("Unknown state: {}", state))),
            };

            request_map.insert(request_id, any_request);
        }

        // Return results in the same order as the input ids
        Ok(ids
            .into_iter()
            .map(|id| {
                request_map
                    .remove(&id)
                    .unwrap_or_else(|| Err(FusilladeError::RequestNotFound(id)))
            })
            .collect())
    }

    // ===================================================================
    // File and Batch Management
    // ===================================================================

    #[tracing::instrument(skip(self, templates), fields(name = %name, template_count = templates.len()))]
    async fn create_file(
        &self,
        name: String,
        description: Option<String>,
        templates: Vec<RequestTemplateInput>,
    ) -> Result<FileId> {
        use futures::stream;

        // Convert the Vec into a stream of FileStreamItems
        let mut items = vec![FileStreamItem::Metadata(FileMetadata {
            filename: Some(name),
            description,
            ..Default::default()
        })];

        for template in templates {
            items.push(FileStreamItem::Template(template));
        }

        let stream = stream::iter(items);
        self.create_file_stream(stream).await
    }

    #[tracing::instrument(skip(self, stream))]
    async fn create_file_stream<S: Stream<Item = FileStreamItem> + Send + Unpin>(
        &self,
        mut stream: S,
    ) -> Result<FileId> {
        use futures::StreamExt;

        // Start a transaction for atomic file + templates creation
        let mut tx =
            self.pools.write().begin().await.map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to begin transaction: {}", e))
            })?;

        // Accumulate metadata as we encounter it
        let mut metadata = FileMetadata::default();
        let mut file_id: Option<Uuid> = None;
        let mut template_count = 0;

        // Extract batch size from strategy
        let batch_size = match self.batch_insert_strategy {
            BatchInsertStrategy::Batched { batch_size } => batch_size,
        };

        // Accumulate templates and insert in batches
        let mut template_buffer = Vec::with_capacity(batch_size);

        while let Some(item) = stream.next().await {
            match item {
                FileStreamItem::Metadata(meta) => {
                    if meta.filename.is_some() {
                        metadata.filename = meta.filename;
                    }
                    if meta.description.is_some() {
                        metadata.description = meta.description;
                    }
                    if meta.purpose.is_some() {
                        metadata.purpose = meta.purpose;
                    }
                    if meta.expires_after_anchor.is_some() {
                        metadata.expires_after_anchor = meta.expires_after_anchor;
                    }
                    if meta.expires_after_seconds.is_some() {
                        metadata.expires_after_seconds = meta.expires_after_seconds;
                    }
                    if meta.size_bytes.is_some() {
                        metadata.size_bytes = meta.size_bytes;
                    }
                    if meta.uploaded_by.is_some() {
                        metadata.uploaded_by = meta.uploaded_by;
                    }
                }
                FileStreamItem::Template(template) => {
                    // Ensure we have a file ID (create stub if needed)
                    if file_id.is_none() {
                        let new_id = Uuid::new_v4();
                        let stub_name = metadata
                            .filename
                            .clone()
                            .unwrap_or_else(|| format!("upload-{}", new_id));
                        let status = crate::batch::FileStatus::Processed.to_string();

                        sqlx::query!(
                            r#"
                            INSERT INTO files (id, name, status, created_at, updated_at)
                            VALUES ($1, $2, $3, NOW(), NOW())
                            "#,
                            new_id,
                            stub_name,
                            status,
                        )
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| {
                            FusilladeError::Other(anyhow!("Failed to create file stub: {}", e))
                        })?;

                        file_id = Some(new_id);
                    }

                    // Add to buffer
                    template_buffer.push((template, template_count));
                    template_count += 1;

                    // Flush buffer if it reaches batch size
                    if template_buffer.len() >= batch_size {
                        Self::insert_template_batch(&mut tx, file_id.unwrap(), &template_buffer)
                            .await?;
                        template_buffer.clear();
                    }
                }
                FileStreamItem::Error(err) => {
                    return Err(FusilladeError::ValidationError(err));
                }
            }
        }

        // Flush any remaining templates in buffer
        if !template_buffer.is_empty() {
            // file_id is guaranteed to be Some if buffer has items
            Self::insert_template_batch(&mut tx, file_id.unwrap(), &template_buffer).await?;
        }

        // If no templates were received, still create an empty file with whatever metadata we have
        let fid = if let Some(id) = file_id {
            id
        } else {
            let new_id = Uuid::new_v4();
            let stub_name = metadata
                .filename
                .clone()
                .unwrap_or_else(|| format!("upload-{}", new_id));
            let status = crate::batch::FileStatus::Processed.to_string();

            sqlx::query!(
                r#"
                INSERT INTO files (id, name, status, created_at, updated_at)
                VALUES ($1, $2, $3, NOW(), NOW())
                "#,
                new_id,
                stub_name,
                status,
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to create file: {}", e)))?;

            new_id
        };

        // Now update the file with all the final metadata
        let size_bytes = metadata.size_bytes.unwrap_or(0);
        let status = crate::batch::FileStatus::Processed.to_string();
        let purpose = metadata.purpose.clone();

        // Use provided anchor time if available, otherwise use now
        let expires_at = if let Some(seconds) = metadata.expires_after_seconds {
            // Calculate expires_at from expires_after if provided
            let anchor = if let Some(anchor_str) = metadata.expires_after_anchor.as_ref() {
                match anchor_str.as_str() {
                    "created_at" => Utc::now(), // Use file creation time
                    _ => {
                        tracing::warn!(
                            anchor = anchor_str,
                            "Unknown expires_after_anchor value, defaulting to 'created_at'"
                        );
                        Utc::now()
                    }
                }
            } else {
                Utc::now()
            };

            anchor.checked_add_signed(chrono::Duration::seconds(seconds))
        } else {
            // Default expiration: 30 days from now when no explicit expires_after_seconds is provided
            Utc::now().checked_add_signed(chrono::Duration::days(30))
        };

        let description = metadata.description.clone();
        let uploaded_by = metadata.uploaded_by.clone();
        let name = metadata.filename.clone();

        // Final update with file metadata
        // Updates the file stub with complete metadata from the stream
        // Input files always have finalized sizes (calculated at upload time)
        sqlx::query!(
            r#"
            UPDATE files
            SET name = COALESCE($2, name),
                description = $3,
                size_bytes = $4,
                status = $5,
                purpose = $6,
                expires_at = $7,
                uploaded_by = $8,
                size_finalized = TRUE,
                updated_at = NOW()
            WHERE id = $1
            "#,
            fid,
            name,
            description,
            size_bytes,
            status,
            purpose,
            expires_at,
            uploaded_by,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to update file metadata: {}", e)))?;

        // Commit the transaction
        tx.commit()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to commit transaction: {}", e)))?;

        tracing::info!(
            file_id = %fid,
            template_count = template_count,
            strategy = ?self.batch_insert_strategy,
            "Successfully created file with templates"
        );

        Ok(FileId(fid))
    }

    #[tracing::instrument(skip(self), fields(file_id = %file_id))]
    async fn get_file(&self, file_id: FileId) -> Result<File> {
        let mut file = self.get_file_from_pool(file_id, self.pools.read()).await?;

        // Check and mark as expired if needed (passive expiration)
        self.check_and_mark_expired(&mut file).await?;

        // Try to finalize size for virtual output/error files. Uses cached value once finalized
        self.maybe_finalize_file_size(&mut file).await?;

        Ok(file)
    }

    #[tracing::instrument(skip(self), fields(file_id = %file_id))]
    async fn get_file_from_primary_pool(&self, file_id: FileId) -> Result<File> {
        let mut file = self.get_file_from_pool(file_id, self.pools.write()).await?;

        // Check and mark as expired if needed (passive expiration)
        self.check_and_mark_expired(&mut file).await?;

        // Try to finalize size for virtual output/error files. Uses cached value once finalized
        self.maybe_finalize_file_size(&mut file).await?;

        Ok(file)
    }

    async fn get_file_content(&self, file_id: FileId) -> Result<Vec<FileContentItem>> {
        let mut stream = self.get_file_content_stream(file_id, 0, None, false);
        let mut items = Vec::new();

        while let Some(result) = stream.next().await {
            items.push(result?);
        }

        Ok(items)
    }

    #[tracing::instrument(skip(self), fields(file_id = %file_id))]
    async fn get_file_template_stats(
        &self,
        file_id: FileId,
    ) -> Result<Vec<crate::batch::ModelTemplateStats>> {
        // Single optimized query that aggregates by model using pre-computed body_byte_size
        // This avoids the expensive LENGTH(body) calculation on large text fields
        let stats = sqlx::query!(
            r#"
            SELECT
                model,
                COUNT(*)::BIGINT as "request_count!",
                SUM(body_byte_size)::BIGINT as "total_body_bytes!"
            FROM request_templates
            WHERE file_id = $1
            GROUP BY model
            ORDER BY model
            "#,
            *file_id as Uuid,
        )
        .fetch_all(self.pools.read())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch template stats: {}", e)))?;

        Ok(stats
            .into_iter()
            .map(|row| crate::batch::ModelTemplateStats {
                model: row.model,
                request_count: row.request_count,
                total_body_bytes: row.total_body_bytes,
            })
            .collect())
    }

    #[tracing::instrument(skip(self), fields(file_id = %file_id, search = ?search))]
    fn get_file_content_stream(
        &self,
        file_id: FileId,
        offset: usize,
        search: Option<String>,
        hide_retriable_before_sla: bool,
    ) -> Pin<Box<dyn Stream<Item = Result<FileContentItem>> + Send>> {
        let pool = self.pools.read().clone();
        let (tx, rx) = mpsc::channel(self.download_buffer_size);
        let offset = offset as i64;

        tokio::spawn(async move {
            // First, get the file to determine its purpose
            let file_result = sqlx::query!(
                r#"
                SELECT purpose
                FROM files
                WHERE id = $1 AND deleted_at IS NULL
                "#,
                *file_id as Uuid,
            )
            .fetch_one(&pool)
            .await;

            let purpose = match file_result {
                Ok(row) => row.purpose,
                Err(e) => {
                    let _ = tx
                        .send(Err(FusilladeError::Other(anyhow!(
                            "Failed to fetch file: {}",
                            e
                        ))))
                        .await;
                    return;
                }
            };

            // Route to appropriate streaming logic based on purpose
            match purpose.as_deref() {
                Some("batch_output") => {
                    Self::stream_batch_output(pool, file_id, offset, search, tx).await;
                }
                Some("batch_error") => {
                    Self::stream_batch_error(
                        pool,
                        file_id,
                        offset,
                        search,
                        hide_retriable_before_sla,
                        tx,
                    )
                    .await;
                }
                _ => {
                    // Regular file or purpose='batch': stream request templates
                    Self::stream_request_templates(pool, file_id, offset, search, tx).await;
                }
            }
        });

        Box::pin(ReceiverStream::new(rx))
    }

    #[tracing::instrument(skip(self, filter), fields(uploaded_by = ?filter.uploaded_by, status = ?filter.status, purpose = ?filter.purpose, after = ?filter.after, limit = ?filter.limit))]
    async fn list_files(&self, filter: crate::batch::FileFilter) -> Result<Vec<File>> {
        use sqlx::QueryBuilder;

        // Get cursor timestamp if needed
        let after_created_at = if let Some(after_id) = &filter.after {
            sqlx::query!(
                r#"SELECT created_at FROM files WHERE id = $1"#,
                **after_id as Uuid
            )
            .fetch_optional(self.pools.read())
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch after cursor: {}", e)))?
            .map(|row| row.created_at)
        } else {
            None
        };

        let mut query_builder = QueryBuilder::new(
            r#"
            SELECT
                f.id, f.name, f.description, f.size_bytes, f.size_finalized,
                f.status, f.error_message, f.purpose, f.expires_at, f.deleted_at,
                f.uploaded_by, f.created_at, f.updated_at,
                b.id as batch_id,
                b.total_requests,
                COALESCE(counts.completed, 0)::BIGINT as completed_requests,
                COALESCE(counts.failed, 0)::BIGINT as failed_requests,
                COALESCE(counts.canceled, 0)::BIGINT as canceled_requests,
                COALESCE(counts.in_progress, 0)::BIGINT as in_progress_requests,
                size_calc.calculated_size
            FROM files f
            LEFT JOIN batches b ON (
                (f.purpose = 'batch_output' AND b.output_file_id = f.id) OR
                (f.purpose = 'batch_error' AND b.error_file_id = f.id)
            )
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE r.state = 'completed') as completed,
                    COUNT(*) FILTER (WHERE r.state = 'failed') as failed,
                    COUNT(*) FILTER (WHERE r.state = 'canceled' OR (r.state IN ('pending', 'claimed', 'processing') AND b.cancelling_at IS NOT NULL)) as canceled,
                    COUNT(*) FILTER (WHERE r.state IN ('claimed', 'processing') AND b.cancelling_at IS NULL) as in_progress
                FROM requests r
                WHERE r.batch_id = b.id
            ) counts ON (f.purpose IN ('batch_output', 'batch_error'))
            LEFT JOIN LATERAL (
                SELECT SUM(r.response_size)::BIGINT as calculated_size
                FROM requests r
                WHERE r.batch_id = b.id
                AND ((f.purpose = 'batch_output' AND r.state = 'completed') OR
                    (f.purpose = 'batch_error' AND r.state = 'failed'))
            ) size_calc ON (f.purpose IN ('batch_output', 'batch_error') AND f.size_finalized = FALSE AND b.id IS NOT NULL)
            "#,
        );

        // Build WHERE clause - always filter out soft-deleted files
        query_builder.push(" WHERE f.deleted_at IS NULL");

        if let Some(uploaded_by) = &filter.uploaded_by {
            query_builder.push(" AND f.uploaded_by = ");
            query_builder.push_bind(uploaded_by);
        }

        if let Some(status) = &filter.status {
            query_builder.push(" AND f.status = ");
            query_builder.push_bind(status);
        }

        if let Some(purpose) = &filter.purpose {
            query_builder.push(" AND f.purpose = ");
            query_builder.push_bind(purpose);
        }

        if let Some(search) = &filter.search {
            let search_pattern = format!("%{}%", search.to_lowercase());
            query_builder.push(" AND LOWER(f.name) LIKE ");
            query_builder.push_bind(search_pattern);
        }

        // Add cursor-based pagination
        if let (Some(after_id), Some(after_ts)) = (&filter.after, after_created_at) {
            let comparison = if filter.ascending { ">" } else { "<" };

            query_builder.push(" AND (f.created_at ");
            query_builder.push(comparison);
            query_builder.push(" ");
            query_builder.push_bind(after_ts);
            query_builder.push(" OR (f.created_at = ");
            query_builder.push_bind(after_ts);
            query_builder.push(" AND f.id ");
            query_builder.push(comparison);
            query_builder.push(" ");
            query_builder.push_bind(**after_id as Uuid);
            query_builder.push("))");
        }

        // Add ORDER BY
        let order_direction = if filter.ascending { "ASC" } else { "DESC" };
        query_builder.push(" ORDER BY f.created_at ");
        query_builder.push(order_direction);
        query_builder.push(", f.id ");
        query_builder.push(order_direction);

        // Add LIMIT
        if let Some(limit) = filter.limit {
            query_builder.push(" LIMIT ");
            query_builder.push_bind(limit as i64);
        }

        let rows = query_builder
            .build()
            .fetch_all(self.pools.read())
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to list files: {}", e)))?;

        let mut files = Vec::new();

        for row in rows {
            let id: Uuid = row
                .try_get("id")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read id: {}", e)))?;
            let name: String = row
                .try_get("name")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read name: {}", e)))?;
            let description: Option<String> = row
                .try_get("description")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read description: {}", e)))?;
            let mut size_bytes: i64 = row
                .try_get("size_bytes")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read size_bytes: {}", e)))?;
            let size_finalized: bool = row.try_get("size_finalized").map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to read size_finalized: {}", e))
            })?;
            let status_str: String = row
                .try_get("status")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read status: {}", e)))?;
            let status = status_str
                .parse::<crate::batch::FileStatus>()
                .map_err(|e| {
                    FusilladeError::Other(anyhow!("Invalid file status '{}': {}", status_str, e))
                })?;
            let error_message: Option<String> = row.try_get("error_message").map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to read error_message: {}", e))
            })?;
            let purpose_str: Option<String> = row
                .try_get("purpose")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read purpose: {}", e)))?;
            let purpose = purpose_str
                .map(|s| s.parse::<crate::batch::Purpose>())
                .transpose()
                .map_err(|e| FusilladeError::Other(anyhow!("Invalid purpose: {}", e)))?;
            let expires_at: Option<chrono::DateTime<Utc>> = row
                .try_get("expires_at")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read expires_at: {}", e)))?;
            let deleted_at: Option<chrono::DateTime<Utc>> = row
                .try_get("deleted_at")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read deleted_at: {}", e)))?;
            let uploaded_by: Option<String> = row
                .try_get("uploaded_by")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read uploaded_by: {}", e)))?;
            let created_at: chrono::DateTime<Utc> = row
                .try_get("created_at")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read created_at: {}", e)))?;
            let updated_at: chrono::DateTime<Utc> = row
                .try_get("updated_at")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read updated_at: {}", e)))?;

            // Calculate size for virtual files if not yet finalized
            if let Some(estimated_size) =
                self.calculate_virtual_file_size_from_row(&row, &purpose, size_finalized)?
            {
                size_bytes = estimated_size;

                // Spawn background finalization if batch is complete
                let file_id = FileId(id);
                self.spawn_finalize_if_complete(&row, file_id, estimated_size);
            }

            let mut file = File {
                id: FileId(id),
                name,
                description,
                size_bytes,
                size_finalized,
                status,
                error_message,
                purpose,
                expires_at,
                deleted_at,
                uploaded_by,
                created_at,
                updated_at,
            };

            // Check and mark as expired if needed (passive expiration)
            self.check_and_mark_expired(&mut file).await?;

            files.push(file);
        }

        Ok(files)
    }

    #[tracing::instrument(skip(self), fields(file_id = %file_id))]
    async fn delete_file(&self, file_id: FileId) -> Result<()> {
        // Use a transaction to ensure atomicity
        let mut tx =
            self.pools.write().begin().await.map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to begin transaction: {}", e))
            })?;

        // Step 1: Cancel non-terminal batches associated with this file
        // This will:
        // - Prevent pending requests from being claimed (claim_requests filters by cancelling_at)
        // - Count pending requests as canceled in batch status
        // - Signal daemons to abort in-flight (claimed/processing) requests
        // Only cancel batches that haven't already reached a terminal state
        // All batches get file_id = NULL so they remain visible but unlinked
        sqlx::query!(
            r#"
            UPDATE batches
            SET cancelling_at = CASE
                    WHEN completed_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL
                        THEN COALESCE(cancelling_at, NOW())
                    ELSE cancelling_at
                END,
                cancelled_at = CASE
                    WHEN completed_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL
                        THEN COALESCE(cancelled_at, NOW())
                    ELSE cancelled_at
                END,
                file_id = NULL
            WHERE file_id = $1
            "#,
            *file_id as Uuid,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to update batches: {}", e)))?;

        // Step 2: Clear output_file_id and error_file_id references
        // This mirrors the ON DELETE SET NULL FK behavior for soft deletes
        // Without this, batches would have dangling references to soft-deleted files
        sqlx::query!(
            r#"
            UPDATE batches
            SET output_file_id = NULL
            WHERE output_file_id = $1
            "#,
            *file_id as Uuid,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to clear output_file_id reference: {}", e))
        })?;

        sqlx::query!(
            r#"
            UPDATE batches
            SET error_file_id = NULL
            WHERE error_file_id = $1
            "#,
            *file_id as Uuid,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to clear error_file_id reference: {}", e))
        })?;

        // Step 3: Soft-delete the file
        // Set deleted_at and status to 'deleted'
        // The file and its templates remain in the database for audit purposes
        // Templates are excluded from active_request_templates view via the JOIN on files.deleted_at
        let rows_affected = sqlx::query!(
            r#"
            UPDATE files
            SET deleted_at = NOW(),
                status = 'deleted'
            WHERE id = $1
              AND deleted_at IS NULL
            "#,
            *file_id as Uuid,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to soft-delete file: {}", e)))?
        .rows_affected();

        if rows_affected == 0 {
            tx.rollback()
                .await
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to rollback: {}", e)))?;
            return Err(FusilladeError::Other(anyhow!("File not found")));
        }

        tx.commit()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to commit transaction: {}", e)))?;

        Ok(())
    }

    #[tracing::instrument(skip(self, input), fields(file_id = ?input.file_id, endpoint = %input.endpoint))]
    async fn create_batch(&self, input: BatchInput) -> Result<Batch> {
        let mut tx =
            self.pools.write().begin().await.map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to begin transaction: {}", e))
            })?;

        // Calculate expires_at from completion_window
        // IMPORTANT: expires_at is required for queue prioritization and SLA monitoring
        // Batches without it will never be processed, so we fail-fast on invalid completion_window
        let now = Utc::now();
        let std_duration = humantime::parse_duration(&input.completion_window).map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Invalid completion_window '{}': {}. Expected format like '24h', '7d', etc.",
                input.completion_window,
                e
            ))
        })?;
        let chrono_duration = chrono::Duration::from_std(std_duration).map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Failed to convert completion_window duration: {}",
                e
            ))
        })?;
        let expires_at = now.checked_add_signed(chrono_duration).ok_or_else(|| {
            FusilladeError::Other(anyhow!(
                "Expiration time overflow when calculating expires_at from completion_window '{}'",
                input.completion_window
            ))
        })?;

        // Create batch with new fields
        let row = sqlx::query!(
            r#"
            INSERT INTO batches (file_id, endpoint, completion_window, metadata, created_by, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, file_id, endpoint, completion_window, metadata, output_file_id, error_file_id, created_by, created_at, expires_at, cancelling_at, errors
            "#,
            *input.file_id as Uuid,
            input.endpoint,
            input.completion_window,
            input.metadata,
            input.created_by,
            expires_at,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to create batch: {}", e)))?;

        let batch_id = row.id;

        // Create virtual output and error files
        let output_file_id = self
            .create_virtual_output_file(&mut tx, batch_id, &input.created_by)
            .await?;
        let error_file_id = self
            .create_virtual_error_file(&mut tx, batch_id, &input.created_by)
            .await?;

        // Update batch with file IDs
        sqlx::query!(
            r#"
            UPDATE batches
            SET output_file_id = $2, error_file_id = $3
            WHERE id = $1
            "#,
            batch_id,
            output_file_id,
            error_file_id,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to update batch with file IDs: {}", e))
        })?;

        // bulk insert requests from templates
        let rows_affected = sqlx::query!(
            r#"
            INSERT INTO requests (batch_id, template_id, state, custom_id, retry_attempt, model)
            SELECT $1, id, 'pending', custom_id, 0, model
            FROM request_templates
            WHERE file_id = $2
            "#,
            batch_id,
            *input.file_id as Uuid,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to create requests: {}", e)))?
        .rows_affected();

        if rows_affected == 0 {
            tx.rollback().await.map_err(|e| {
                FusilladeError::Other(anyhow!(
                    "Failed to rollback transaction after zero templates: {}",
                    e
                ))
            })?;
            return Err(FusilladeError::Other(anyhow!(
                "Cannot create batch from file with no templates"
            )));
        }

        // Update batch metadata
        // Note: Request state counts are computed on-demand, not stored
        sqlx::query!(
            r#"
            UPDATE batches
            SET total_requests = $2,
                requests_started_at = NOW()
            WHERE id = $1
            "#,
            batch_id,
            rows_affected as i64
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to update batch metadata: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to commit transaction: {}", e)))?;

        // IMPORTANT: Must query from write pool to guarantee read-after-write consistency.
        // Different connections from the pool can have different transaction snapshots due to
        // isolation levels (READ COMMITTED or REPEATABLE READ). Querying from the read pool
        // immediately after committing on the write pool can result in "Batch not found" if
        // the read connection's snapshot predates the write commit.
        self.get_batch_from_pool(BatchId(batch_id), false, self.pools.write())
            .await
    }

    #[tracing::instrument(skip(self), fields(batch_id = %batch_id))]
    async fn get_batch(&self, batch_id: BatchId, hide_retriable_before_sla: bool) -> Result<Batch> {
        self.get_batch_from_pool(batch_id, hide_retriable_before_sla, self.pools.read())
            .await
    }

    #[tracing::instrument(skip(self), fields(batch_id = %batch_id))]
    async fn get_batch_status(
        &self,
        batch_id: BatchId,
        hide_retriable_before_sla: bool,
    ) -> Result<BatchStatus> {
        // Get SQL fragments for error filtering
        let (_, failed_count) = Self::error_filter_sql_fragments(hide_retriable_before_sla);

        let mut query_builder = QueryBuilder::new(
            r#"
            SELECT
                b.id as batch_id,
                b.file_id,
                f.name as file_name,
                b.total_requests,
                b.requests_started_at as started_at,
                b.created_at,
                COALESCE(counts.pending, 0)::BIGINT as pending_requests,
                COALESCE(counts.in_progress, 0)::BIGINT as in_progress_requests,
                COALESCE(counts.completed, 0)::BIGINT as completed_requests,
                COALESCE(counts.failed, 0)::BIGINT as failed_requests,
                COALESCE(counts.canceled, 0)::BIGINT as canceled_requests
            FROM batches b
            LEFT JOIN files f ON f.id = b.file_id
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE state = 'pending' AND b.cancelling_at IS NULL) as pending,
                    COUNT(*) FILTER (WHERE state IN ('claimed', 'processing') AND b.cancelling_at IS NULL) as in_progress,
                    COUNT(*) FILTER (WHERE state = 'completed') as completed,
                    "#,
        );
        query_builder.push(failed_count);
        query_builder.push(
            r#" as failed,
                    COUNT(*) FILTER (WHERE state = 'canceled' OR (state IN ('pending', 'claimed', 'processing') AND b.cancelling_at IS NOT NULL)) as canceled
                FROM requests
                WHERE batch_id = b.id
            ) counts ON TRUE
            WHERE b.id = "#
        );
        query_builder.push_bind(*batch_id as Uuid);
        query_builder.push(" AND b.deleted_at IS NULL");

        let row = query_builder
            .build()
            .fetch_optional(self.pools.read())
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch batch status: {}", e)))?
            .ok_or_else(|| FusilladeError::Other(anyhow!("Batch not found")))?;

        Ok(batch_status_from_dynamic_row!(row))
    }

    async fn get_batch_by_output_file_id(
        &self,
        file_id: FileId,
        file_type: OutputFileType,
    ) -> Result<Option<Batch>> {
        // Don't hide any errors since this method doesn't take a filter parameter
        let (_, failed_count) = Self::error_filter_sql_fragments(false);

        let mut query_builder = QueryBuilder::new(
            r#"
                    SELECT
                        b.id, b.file_id, b.endpoint, b.completion_window, b.metadata,
                        b.output_file_id, b.error_file_id, b.created_by, b.created_at,
                        b.expires_at, b.cancelling_at, b.errors,
                        b.total_requests,
                        b.requests_started_at,
                        b.finalizing_at,
                        b.completed_at,
                        b.failed_at,
                        b.cancelled_at,
                        b.deleted_at,
                        b.notification_sent_at,
                        COALESCE(counts.pending, 0)::BIGINT as pending_requests,
                        COALESCE(counts.in_progress, 0)::BIGINT as in_progress_requests,
                        COALESCE(counts.completed, 0)::BIGINT as completed_requests,
                        COALESCE(counts.failed, 0)::BIGINT as failed_requests,
                        COALESCE(counts.canceled, 0)::BIGINT as canceled_requests
                    FROM batches b
                    LEFT JOIN LATERAL (
                        SELECT
                            COUNT(*) FILTER (WHERE state = 'pending' AND b.cancelling_at IS NULL) as pending,
                            COUNT(*) FILTER (WHERE state IN ('claimed', 'processing') AND b.cancelling_at IS NULL) as in_progress,
                            COUNT(*) FILTER (WHERE state = 'completed') as completed,
                            "#,
        );
        query_builder.push(failed_count);
        query_builder.push(" as failed, ");
        query_builder.push(
            r#"
                            COUNT(*) FILTER (WHERE state = 'canceled' OR (state IN ('pending', 'claimed', 'processing') AND b.cancelling_at IS NOT NULL)) as canceled
                        FROM requests
                        WHERE batch_id = b.id
                    ) counts ON TRUE
                    WHERE "#
        );

        match file_type {
            OutputFileType::Output => {
                query_builder.push("b.output_file_id = ");
                query_builder.push_bind(*file_id as Uuid);
                query_builder.push(" AND b.deleted_at IS NULL");

                let row = query_builder
                    .build()
                    .fetch_optional(self.pools.read())
                    .await
                    .map_err(|e| {
                        FusilladeError::Other(anyhow!("Failed to get batch by output file: {}", e))
                    })?;

                Ok(row.map(|row| batch_from_dynamic_row!(row)))
            }
            OutputFileType::Error => {
                query_builder.push("b.error_file_id = ");
                query_builder.push_bind(*file_id as Uuid);
                query_builder.push(" AND b.deleted_at IS NULL");

                let row = query_builder
                    .build()
                    .fetch_optional(self.pools.read())
                    .await
                    .map_err(|e| {
                        FusilladeError::Other(anyhow!("Failed to get batch by error file: {}", e))
                    })?;

                Ok(row.map(|row| batch_from_dynamic_row!(row)))
            }
        }
    }

    #[tracing::instrument(skip(self), fields(created_by = ?created_by, limit))]
    async fn list_batches(
        &self,
        created_by: Option<String>,
        search: Option<String>,
        after: Option<BatchId>,
        limit: i64,
        hide_retriable_before_sla: bool,
    ) -> Result<Vec<Batch>> {
        // If after is provided, get the created_at timestamp of that batch for cursor-based pagination
        let (after_created_at, after_id) = if let Some(after_id) = after {
            let row = sqlx::query!(
                r#"
                SELECT created_at
                FROM batches
                WHERE id = $1
                "#,
                *after_id as Uuid,
            )
            .fetch_optional(self.pools.read())
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch after batch: {}", e)))?;

            (row.map(|r| r.created_at), Some(*after_id as Uuid))
        } else {
            (None, None)
        };

        // Get SQL fragments for error filtering
        let (_, failed_count) = Self::error_filter_sql_fragments(hide_retriable_before_sla);

        // Use a single query with optional cursor filtering and on-demand counting
        // Join with files table to enable searching by input filename
        let search_pattern = search.as_ref().map(|s| format!("%{}%", s.to_lowercase()));

        let mut query_builder = QueryBuilder::new(
            r#"
            SELECT
                b.id, b.file_id, b.endpoint, b.completion_window, b.metadata,
                b.output_file_id, b.error_file_id, b.created_by, b.created_at,
                b.expires_at, b.cancelling_at, b.errors,
                b.total_requests,
                b.requests_started_at,
                b.finalizing_at,
                b.completed_at,
                b.failed_at,
                b.cancelled_at,
                b.deleted_at,
                b.notification_sent_at,
                COALESCE(counts.pending, 0)::BIGINT as pending_requests,
                COALESCE(counts.in_progress, 0)::BIGINT as in_progress_requests,
                COALESCE(counts.completed, 0)::BIGINT as completed_requests,
                COALESCE(counts.failed, 0)::BIGINT as failed_requests,
                COALESCE(counts.canceled, 0)::BIGINT as canceled_requests
            FROM batches b
            LEFT JOIN files f ON b.file_id = f.id
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE state = 'pending' AND b.cancelling_at IS NULL) as pending,
                    COUNT(*) FILTER (WHERE state IN ('claimed', 'processing') AND b.cancelling_at IS NULL) as in_progress,
                    COUNT(*) FILTER (WHERE state = 'completed') as completed,
                    "#,
        );
        query_builder.push(failed_count);
        query_builder.push(" as failed, ");
        query_builder.push(
            r#"
                    COUNT(*) FILTER (WHERE state = 'canceled' OR (state IN ('pending', 'claimed', 'processing') AND b.cancelling_at IS NOT NULL)) as canceled
                FROM requests
                WHERE batch_id = b.id
            ) counts ON TRUE
            WHERE b.deleted_at IS NULL
              AND ("#
        );
        query_builder.push_bind(&created_by);
        query_builder.push("::TEXT IS NULL OR b.created_by = ");
        query_builder.push_bind(&created_by);
        query_builder.push(") AND (");
        query_builder.push_bind(after_created_at);
        query_builder.push("::TIMESTAMPTZ IS NULL OR b.created_at < ");
        query_builder.push_bind(after_created_at);
        query_builder.push(" OR (b.created_at = ");
        query_builder.push_bind(after_created_at);
        query_builder.push(" AND b.id < ");
        query_builder.push_bind(after_id);
        query_builder.push(")) AND (");
        query_builder.push_bind(&search_pattern);
        query_builder.push("::TEXT IS NULL OR LOWER(b.metadata::text) LIKE ");
        query_builder.push_bind(&search_pattern);
        query_builder.push(" OR LOWER(f.name) LIKE ");
        query_builder.push_bind(&search_pattern);
        query_builder.push(") ORDER BY b.created_at DESC, b.id DESC LIMIT ");
        query_builder.push_bind(limit);

        let rows = query_builder
            .build()
            .fetch_all(self.pools.read())
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to list batches: {}", e)))?;

        Ok(rows
            .into_iter()
            .map(|row| batch_from_dynamic_row!(row))
            .collect())
    }

    async fn list_file_batches(
        &self,
        file_id: FileId,
        hide_retriable_before_sla: bool,
    ) -> Result<Vec<BatchStatus>> {
        // Get SQL fragments for error filtering
        let (_, failed_count) = Self::error_filter_sql_fragments(hide_retriable_before_sla);

        let mut query_builder = QueryBuilder::new(
            r#"
            SELECT
                b.id as batch_id,
                b.file_id,
                f.name as file_name,
                b.total_requests,
                b.requests_started_at as started_at,
                b.created_at,
                COALESCE(counts.pending, 0)::BIGINT as pending_requests,
                COALESCE(counts.in_progress, 0)::BIGINT as in_progress_requests,
                COALESCE(counts.completed, 0)::BIGINT as completed_requests,
                COALESCE(counts.failed, 0)::BIGINT as failed_requests,
                COALESCE(counts.canceled, 0)::BIGINT as canceled_requests
            FROM batches b
            LEFT JOIN files f ON f.id = b.file_id
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE state = 'pending' AND b.cancelling_at IS NULL) as pending,
                    COUNT(*) FILTER (WHERE state IN ('claimed', 'processing') AND b.cancelling_at IS NULL) as in_progress,
                    COUNT(*) FILTER (WHERE state = 'completed') as completed,
                    "#,
        );
        query_builder.push(failed_count);
        query_builder.push(" as failed, ");
        query_builder.push(
            r#"
                    COUNT(*) FILTER (WHERE state = 'canceled' OR (state IN ('pending', 'claimed', 'processing') AND b.cancelling_at IS NOT NULL)) as canceled
                FROM requests
                WHERE batch_id = b.id
            ) counts ON TRUE
            WHERE b.file_id = "#
        );
        query_builder.push_bind(*file_id as Uuid);
        query_builder.push(" AND b.deleted_at IS NULL ORDER BY b.created_at DESC");

        let rows = query_builder
            .build()
            .fetch_all(self.pools.read())
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to list batches: {}", e)))?;

        Ok(rows
            .into_iter()
            .map(|row| batch_status_from_dynamic_row!(row))
            .collect())
    }

    #[tracing::instrument(skip(self), fields(batch_id = %batch_id))]
    async fn cancel_batch(&self, batch_id: BatchId) -> Result<()> {
        let now = Utc::now();

        // Set both cancelling_at and cancelled_at
        // cancelling_at = source of truth for "batch is being cancelled"
        // cancelled_at = timestamp of user's cancellation action
        // Pending requests won't be claimed (claim_requests checks cancelling_at)
        // In-flight requests will be aborted via polling
        // Counts will be computed based on cancelling_at + state
        sqlx::query!(
            r#"
            UPDATE batches
            SET cancelling_at = $2,
                cancelled_at = $2
            WHERE id = $1 AND cancelling_at IS NULL
            "#,
            *batch_id as Uuid,
            now,
        )
        .execute(self.pools.write())
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to set cancellation timestamps: {}", e))
        })?;

        Ok(())
    }

    async fn delete_batch(&self, batch_id: BatchId) -> Result<()> {
        // Soft-delete the batch by setting deleted_at
        // Also cancel it if not already in a terminal state
        // Requests are excluded from active_requests view via the JOIN on batches.deleted_at
        let rows_affected = sqlx::query!(
            r#"
            UPDATE batches
            SET deleted_at = NOW(),
                cancelling_at = CASE
                    WHEN completed_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL
                        THEN COALESCE(cancelling_at, NOW())
                    ELSE cancelling_at
                END,
                cancelled_at = CASE
                    WHEN completed_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL
                        THEN COALESCE(cancelled_at, NOW())
                    ELSE cancelled_at
                END
            WHERE id = $1
              AND deleted_at IS NULL
            "#,
            *batch_id as Uuid,
        )
        .execute(self.pools.write())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to soft-delete batch: {}", e)))?
        .rows_affected();

        if rows_affected == 0 {
            return Err(FusilladeError::Other(anyhow!("Batch not found")));
        }

        Ok(())
    }

    async fn retry_failed_requests(&self, ids: Vec<RequestId>) -> Result<Vec<Result<()>>> {
        tracing::info!(count = ids.len(), "Retrying failed requests");

        // Get all requests in a single bulk query to avoid N+1 problem
        let get_results = self.get_requests(ids.clone()).await?;
        let found_count = get_results.len();

        // Check if any requests were not found (e.g., template was deleted)
        if found_count != ids.len() {
            // Some requests were not returned - likely because their template was deleted
            // Find which ones are missing
            let returned_ids: std::collections::HashSet<_> = get_results
                .iter()
                .filter_map(|r| r.as_ref().ok().map(|req| req.id()))
                .collect();

            let missing_ids: Vec<_> = ids.iter().filter(|id| !returned_ids.contains(id)).collect();

            tracing::warn!(
                missing_count = missing_ids.len(),
                "Some requests not found, likely due to deleted templates"
            );
        }

        let mut results = Vec::new();

        for (id, request_result) in ids.iter().zip(get_results.into_iter()) {
            let result = match request_result {
                Ok(AnyRequest::Failed(req)) => {
                    // Reset to pending state with retry_attempt = 0
                    let pending_request = Request {
                        state: Pending {
                            retry_attempt: 0,
                            not_before: None,
                            batch_expires_at: req.state.batch_expires_at,
                        },
                        data: req.data,
                    };

                    self.persist(&pending_request).await?;
                    Ok(())
                }
                Ok(_) => Err(crate::error::FusilladeError::InvalidState(
                    *id,
                    "non-failed state".to_string(),
                    "failed state".to_string(),
                )),
                Err(e) => Err(e),
            };

            results.push(result);
        }

        // For any missing requests, add an error result
        for _ in 0..(ids.len() - found_count) {
            results.push(Err(FusilladeError::Other(anyhow!(
                "Request not found - template may have been deleted"
            ))));
        }

        Ok(results)
    }

    async fn retry_failed_requests_for_batch(&self, batch_id: BatchId) -> Result<u64> {
        tracing::info!(%batch_id, "Retrying all failed requests for batch");

        let result = sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'pending',
                retry_attempt = 0,
                not_before = NULL,
                error = NULL,
                failed_at = NULL,
                daemon_id = NULL,
                claimed_at = NULL,
                started_at = NULL
            WHERE batch_id = $1 AND state = 'failed'
            "#,
            *batch_id as Uuid,
        )
        .execute(self.pools.write())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to retry failed requests: {}", e)))?;

        let count = result.rows_affected();
        tracing::info!(%batch_id, count, "Retried failed requests for batch");

        Ok(count)
    }

    #[tracing::instrument(skip(self), fields(batch_id = %batch_id))]
    async fn get_batch_requests(&self, batch_id: BatchId) -> Result<Vec<AnyRequest>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                r.id, r.batch_id as "batch_id!", r.template_id as "template_id?", r.state,
                t.custom_id as "custom_id?", t.endpoint as "endpoint?", t.method as "method?",
                t.path as "path?", t.body as "body?", t.model as "model?", t.api_key as "api_key?",
                r.retry_attempt, r.not_before, r.daemon_id, r.claimed_at, r.started_at,
                r.response_status, r.response_body, r.completed_at, r.error, r.failed_at, r.canceled_at,
                b.expires_at as batch_expires_at, r.routed_model
            FROM requests r
            LEFT JOIN active_request_templates t ON r.template_id = t.id
            JOIN batches b ON r.batch_id = b.id
            WHERE r.batch_id = $1 AND b.deleted_at IS NULL
            ORDER BY r.created_at ASC
            "#,
            *batch_id as Uuid,
        )
        .fetch_all(self.pools.read())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch batch executions: {}", e)))?;

        let mut results = Vec::new();

        for row in rows {
            let request_id = RequestId(row.id);

            // Check if template data exists (template may have been deleted)
            let data = match (
                row.template_id,
                row.endpoint,
                row.method,
                row.path,
                row.body,
                row.model,
                row.api_key,
            ) {
                (
                    Some(template_id),
                    Some(endpoint),
                    Some(method),
                    Some(path),
                    Some(body),
                    Some(model),
                    Some(api_key),
                ) => RequestData {
                    id: request_id,
                    batch_id: BatchId(row.batch_id),
                    template_id: TemplateId(template_id),
                    custom_id: row.custom_id,
                    endpoint,
                    method,
                    path,
                    body,
                    model,
                    api_key,
                    batch_metadata: std::collections::HashMap::new(),
                },
                _ => {
                    // Template was deleted - skip this request
                    tracing::debug!(request_id = %request_id, "Skipping batch request with deleted template");
                    continue;
                }
            };

            let state = &row.state;

            let any_request = match state.as_str() {
                "pending" => AnyRequest::Pending(Request {
                    state: Pending {
                        retry_attempt: row.retry_attempt as u32,
                        not_before: row.not_before,
                        batch_expires_at: row.batch_expires_at,
                    },
                    data,
                }),
                "claimed" => AnyRequest::Claimed(Request {
                    state: Claimed {
                        daemon_id: DaemonId(row.daemon_id.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing daemon_id for claimed execution"
                            ))
                        })?),
                        claimed_at: row.claimed_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing claimed_at for claimed execution"
                            ))
                        })?,
                        retry_attempt: row.retry_attempt as u32,
                        batch_expires_at: row.batch_expires_at,
                    },
                    data,
                }),
                "processing" => {
                    let (_tx, rx) = tokio::sync::mpsc::channel(1);
                    let abort_handle = tokio::spawn(async {}).abort_handle();
                    AnyRequest::Processing(Request {
                        state: Processing {
                            daemon_id: DaemonId(row.daemon_id.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing daemon_id for processing execution"
                                ))
                            })?),
                            claimed_at: row.claimed_at.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing claimed_at for processing execution"
                                ))
                            })?,
                            started_at: row.started_at.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing started_at for processing execution"
                                ))
                            })?,
                            retry_attempt: row.retry_attempt as u32,
                            batch_expires_at: row.batch_expires_at,
                            result_rx: Arc::new(Mutex::new(rx)),
                            abort_handle,
                        },
                        data,
                    })
                }
                "completed" => AnyRequest::Completed(Request {
                    state: Completed {
                        response_status: row.response_status.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing response_status for completed execution"
                            ))
                        })? as u16,
                        response_body: row.response_body.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing response_body for completed execution"
                            ))
                        })?,
                        claimed_at: row.claimed_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing claimed_at for completed execution"
                            ))
                        })?,
                        started_at: row.started_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing started_at for completed execution"
                            ))
                        })?,
                        completed_at: row.completed_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing completed_at for completed execution"
                            ))
                        })?,
                        // Fall back to template model for old data without routed_model
                        routed_model: row.routed_model.unwrap_or_else(|| data.model.clone()),
                    },
                    data,
                }),
                "failed" => {
                    let error_json = row.error.ok_or_else(|| {
                        FusilladeError::Other(anyhow!("Missing error for failed execution"))
                    })?;

                    // Try to deserialize as FailureReason, fall back to NetworkError for old data
                    let reason: FailureReason =
                        serde_json::from_str(&error_json).unwrap_or_else(|_| {
                            // If deserialization fails, treat it as a legacy error string
                            // and wrap it as a NetworkError for backwards compatibility
                            FailureReason::NetworkError {
                                error: error_json.clone(),
                            }
                        });

                    AnyRequest::Failed(Request {
                        state: Failed {
                            reason,
                            failed_at: row.failed_at.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing failed_at for failed execution"
                                ))
                            })?,
                            retry_attempt: row.retry_attempt as u32,
                            batch_expires_at: row.batch_expires_at,
                            // Fall back to template model for old data without routed_model
                            routed_model: row.routed_model.unwrap_or_else(|| data.model.clone()),
                        },
                        data,
                    })
                }
                "canceled" => AnyRequest::Canceled(Request {
                    state: Canceled {
                        canceled_at: row.canceled_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing canceled_at for canceled execution"
                            ))
                        })?,
                    },
                    data,
                }),
                _ => {
                    return Err(FusilladeError::Other(anyhow!("Unknown state: {}", state)));
                }
            };

            results.push(any_request);
        }

        Ok(results)
    }

    #[tracing::instrument(skip(self), fields(batch_id = %batch_id, search = ?search, status = ?status))]
    fn get_batch_results_stream(
        &self,
        batch_id: BatchId,
        offset: usize,
        search: Option<String>,
        status: Option<String>,
        hide_retriable_before_sla: bool,
    ) -> Pin<Box<dyn Stream<Item = Result<crate::batch::BatchResultItem>> + Send>> {
        let pool = self.pools.read().clone();
        let (tx, rx) = mpsc::channel(self.download_buffer_size);
        let offset = offset as i64;

        tokio::spawn(async move {
            Self::stream_batch_results(
                pool,
                batch_id,
                offset,
                search,
                status,
                hide_retriable_before_sla,
                tx,
            )
            .await;
        });

        Box::pin(ReceiverStream::new(rx))
    }
}

// Helper methods for file streaming and virtual file creation
impl<P: PoolProvider, H: HttpClient + 'static> PostgresRequestManager<P, H> {
    /// Internal helper to fetch a batch from a specific pool.
    ///
    /// This is used when we require read-after-write consistency and must query
    /// from the same pool where a write was committed. This avoids transaction
    /// isolation issues where a different connection's snapshot might not yet
    /// see the committed data.
    ///
    /// # Arguments
    /// * `batch_id` - The ID of the batch to fetch
    /// * `pool` - The specific pool to query from (typically write pool after commit)
    async fn get_batch_from_pool(
        &self,
        batch_id: BatchId,
        hide_retriable_before_sla: bool,
        pool: &PgPool,
    ) -> Result<Batch> {
        // Get SQL fragments for error filtering
        let (_, failed_count) = Self::error_filter_sql_fragments(hide_retriable_before_sla);

        let mut query_builder = QueryBuilder::new(
            r#"
            SELECT
                b.id, b.file_id, b.endpoint, b.completion_window, b.metadata,
                b.output_file_id, b.error_file_id, b.created_by, b.created_at,
                b.expires_at, b.cancelling_at, b.errors,
                b.total_requests,
                b.requests_started_at,
                b.finalizing_at,
                b.completed_at,
                b.failed_at,
                b.cancelled_at,
                b.deleted_at,
                b.notification_sent_at,
                COALESCE(counts.pending, 0)::BIGINT as pending_requests,
                COALESCE(counts.in_progress, 0)::BIGINT as in_progress_requests,
                COALESCE(counts.completed, 0)::BIGINT as completed_requests,
                COALESCE(counts.failed, 0)::BIGINT as failed_requests,
                COALESCE(counts.canceled, 0)::BIGINT as canceled_requests
            FROM batches b
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE state = 'pending' AND b.cancelling_at IS NULL) as pending,
                    COUNT(*) FILTER (WHERE state IN ('claimed', 'processing') AND b.cancelling_at IS NULL) as in_progress,
                    COUNT(*) FILTER (WHERE state = 'completed') as completed,
                    "#,
        );
        query_builder.push(failed_count);
        query_builder.push(
            r#" as failed,
                    COUNT(*) FILTER (WHERE state = 'canceled' OR (state IN ('pending', 'claimed', 'processing') AND b.cancelling_at IS NOT NULL)) as canceled
                FROM requests
                WHERE batch_id = b.id
            ) counts ON TRUE
            WHERE b.id = "#
        );
        query_builder.push_bind(*batch_id as Uuid);
        query_builder.push(" AND b.deleted_at IS NULL");

        let row = query_builder
            .build()
            .fetch_optional(pool)
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch batch: {}", e)))?
            .ok_or_else(|| FusilladeError::Other(anyhow!("Batch not found")))?;

        // Extract counts for terminal state checking
        let pending_requests: i64 = row.get("pending_requests");
        let in_progress_requests: i64 = row.get("in_progress_requests");
        let completed_requests: i64 = row.get("completed_requests");
        let failed_requests: i64 = row.get("failed_requests");
        let canceled_requests: i64 = row.get("canceled_requests");
        let total_requests: i64 = row.get("total_requests");
        let completed_at: Option<DateTime<Utc>> = row.get("completed_at");
        let failed_at: Option<DateTime<Utc>> = row.get("failed_at");
        let cancelled_at: Option<DateTime<Utc>> = row.get("cancelled_at");
        let finalizing_at_db: Option<DateTime<Utc>> = row.get("finalizing_at");

        // Lazy computation of terminal timestamps
        // Check if batch is in terminal state and update timestamps if needed
        let terminal_count = completed_requests + failed_requests + canceled_requests;
        let is_terminal = terminal_count == total_requests && total_requests > 0;

        let (finalizing_at, completed_at, failed_at) = if is_terminal
            && completed_at.is_none()
            && failed_at.is_none()
            && cancelled_at.is_none()
        {
            let now = Utc::now();

            // Determine which terminal state based on counts
            let (finalizing, completed, failed) = if completed_requests > 0 {
                // At least one completion = completed batch
                (Some(now), Some(now), None)
            } else {
                // No completions = failed batch
                (Some(now), None, Some(now))
            };

            // Update the database with the terminal timestamps
            sqlx::query!(
                r#"
                UPDATE batches
                SET finalizing_at = COALESCE(finalizing_at, $2),
                    completed_at = COALESCE(completed_at, $3),
                    failed_at = COALESCE(failed_at, $4)
                WHERE id = $1
                "#,
                *batch_id as Uuid,
                finalizing,
                completed,
                failed,
            )
            .execute(self.pools.write()) // Use the provided pool parameter here too
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to update terminal timestamps: {}", e))
            })?;

            (finalizing, completed, failed)
        } else {
            (finalizing_at_db, completed_at, failed_at)
        };

        Ok(Batch {
            id: BatchId(row.get("id")),
            file_id: row.get::<Option<Uuid>, _>("file_id").map(FileId),
            created_at: row.get("created_at"),
            metadata: row.get("metadata"),
            completion_window: row.get("completion_window"),
            endpoint: row.get("endpoint"),
            output_file_id: row.get::<Option<Uuid>, _>("output_file_id").map(FileId),
            error_file_id: row.get::<Option<Uuid>, _>("error_file_id").map(FileId),
            created_by: row.get("created_by"),
            expires_at: row.get("expires_at"),
            cancelling_at: row.get("cancelling_at"),
            errors: row.get("errors"),
            total_requests,
            pending_requests,
            in_progress_requests,
            completed_requests,
            failed_requests,
            canceled_requests,
            requests_started_at: row.get("requests_started_at"),
            finalizing_at,
            completed_at,
            failed_at,
            cancelled_at,
            deleted_at: row.get("deleted_at"),
            notification_sent_at: row.get("notification_sent_at"),
        })
    }

    /// Find terminal batches that need notification, finalize if needed, and claim atomically.
    ///
    /// Handles both batches already finalized by `get_batch()` API calls and batches that
    /// are terminal by count but not yet finalized. Sets `notification_sent_at` atomically
    /// to prevent duplicate notifications across replicas polling concurrently.
    ///
    /// Request counts (completed, failed, etc.) are computed here via LATERAL JOIN for the
    /// notification email body but are not persisted — they'll be recomputed on subsequent
    /// reads. This is acceptable because the counts are cheap to compute (indexed on
    /// `batch_id` + `state`), the values are immutable once finalized, and storing them
    /// would add schema complexity and staleness risk for marginal gain.
    /// Finds terminal batches that haven't had notifications sent yet, atomically
    /// marks them as notified, and returns them for processing.
    ///
    /// Also writes terminal timestamps (completed_at/failed_at/cancelled_at) via
    /// COALESCE. Currently these are almost always already set by get_batch()'s
    /// lazy finalization (triggered as a side-effect of the daemon's cancellation
    /// poller), but the finalization logic is kept here so this poller remains
    /// self-contained and correct regardless of how callers of get_batch() change.
    pub async fn poll_completed_batches(
        &self,
        hide_retriable_before_sla: bool,
    ) -> Result<Vec<Batch>> {
        let (_, failed_count) = Self::error_filter_sql_fragments(hide_retriable_before_sla);

        let mut query_builder = QueryBuilder::new(
            r#"
            WITH candidates AS (
                SELECT b.id,
                       COALESCE(counts.completed, 0)::BIGINT as completed_requests,
                       COALESCE(counts.failed, 0)::BIGINT as failed_requests,
                       COALESCE(counts.canceled, 0)::BIGINT as canceled_requests,
                       COALESCE(counts.pending, 0)::BIGINT as pending_requests,
                       COALESCE(counts.in_progress, 0)::BIGINT as in_progress_requests
                FROM batches b
                LEFT JOIN LATERAL (
                    SELECT
                        COUNT(*) FILTER (WHERE state = 'completed') as completed,
                        "#,
        );
        query_builder.push(failed_count);
        query_builder.push(
            r#" as failed,
                        COUNT(*) FILTER (WHERE state = 'canceled' OR (state IN ('pending', 'claimed', 'processing') AND b.cancelling_at IS NOT NULL)) as canceled,
                        COUNT(*) FILTER (WHERE state = 'pending' AND b.cancelling_at IS NULL) as pending,
                        COUNT(*) FILTER (WHERE state IN ('claimed', 'processing') AND b.cancelling_at IS NULL) as in_progress
                    FROM requests WHERE batch_id = b.id
                ) counts ON TRUE
                WHERE b.notification_sent_at IS NULL
                  AND b.deleted_at IS NULL
                  AND b.total_requests > 0
                  AND (
                      -- Already finalized by get_batch()
                      (b.completed_at IS NOT NULL OR b.failed_at IS NOT NULL OR b.cancelled_at IS NOT NULL)
                      OR
                      -- Terminal by count but not yet finalized
                      (COALESCE(counts.completed, 0) + COALESCE(counts.failed, 0) + COALESCE(counts.canceled, 0) = b.total_requests)
                  )
            ),
            updated AS (
                UPDATE batches b
                SET notification_sent_at = NOW(),
                    finalizing_at = COALESCE(b.finalizing_at, NOW()),
                    completed_at = COALESCE(b.completed_at,
                        CASE WHEN c.completed_requests > 0 THEN NOW() END),
                    failed_at = COALESCE(b.failed_at,
                        CASE WHEN c.completed_requests = 0 AND c.canceled_requests < b.total_requests THEN NOW() END),
                    cancelled_at = COALESCE(b.cancelled_at,
                        CASE WHEN c.canceled_requests = b.total_requests THEN NOW() END)
                FROM candidates c
                WHERE b.id = c.id
                  AND b.notification_sent_at IS NULL
                RETURNING b.id, b.file_id, b.endpoint, b.completion_window, b.metadata,
                          b.output_file_id, b.error_file_id, b.created_by, b.created_at,
                          b.expires_at, b.cancelling_at, b.errors, b.total_requests,
                          b.requests_started_at, b.finalizing_at, b.completed_at,
                          b.failed_at, b.cancelled_at, b.deleted_at, b.notification_sent_at,
                          c.completed_requests, c.failed_requests, c.canceled_requests,
                          c.pending_requests, c.in_progress_requests
            )
            SELECT * FROM updated
            "#,
        );

        let rows = query_builder
            .build()
            .fetch_all(self.pools.write())
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to poll completed batches: {}", e))
            })?;

        Ok(rows
            .into_iter()
            .map(|row| batch_from_dynamic_row!(row))
            .collect())
    }

    /// Insert a batch of templates using PostgreSQL UNNEST for bulk insertion.
    /// This is significantly faster than individual INSERTs for large template counts.
    async fn insert_template_batch(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        file_id: Uuid,
        templates: &[(RequestTemplateInput, i32)],
    ) -> Result<()> {
        if templates.is_empty() {
            return Ok(());
        }

        // Prepare parallel arrays for UNNEST
        let custom_ids: Vec<Option<&str>> = templates
            .iter()
            .map(|(t, _)| t.custom_id.as_deref())
            .collect();
        let endpoints: Vec<&str> = templates.iter().map(|(t, _)| t.endpoint.as_str()).collect();
        let methods: Vec<&str> = templates.iter().map(|(t, _)| t.method.as_str()).collect();
        let paths: Vec<&str> = templates.iter().map(|(t, _)| t.path.as_str()).collect();
        let bodies: Vec<&str> = templates.iter().map(|(t, _)| t.body.as_str()).collect();
        let models: Vec<&str> = templates.iter().map(|(t, _)| t.model.as_str()).collect();
        let api_keys: Vec<&str> = templates.iter().map(|(t, _)| t.api_key.as_str()).collect();
        let line_numbers: Vec<i32> = templates.iter().map(|(_, line)| *line).collect();
        let body_byte_sizes: Vec<i64> =
            templates.iter().map(|(t, _)| t.body.len() as i64).collect();

        sqlx::query!(
            r#"
            INSERT INTO request_templates (file_id, custom_id, endpoint, method, path, body, model, api_key, line_number, body_byte_size)
            SELECT $1, custom_id, endpoint, method, path, body, model, api_key, line_number, body_byte_size
            FROM UNNEST(
                $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
                $7::text[], $8::text[], $9::int[], $10::bigint[]
            ) AS t(custom_id, endpoint, method, path, body, model, api_key, line_number, body_byte_size)
            "#,
            file_id,
            &custom_ids as &[Option<&str>],
            &endpoints as &[&str],
            &methods as &[&str],
            &paths as &[&str],
            &bodies as &[&str],
            &models as &[&str],
            &api_keys as &[&str],
            &line_numbers as &[i32],
            &body_byte_sizes as &[i64],
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to batch insert templates: {}", e)))?;

        Ok(())
    }

    /// Stream request templates from a regular file
    async fn stream_request_templates(
        pool: sqlx::PgPool,
        file_id: FileId,
        offset: i64,
        search: Option<String>,
        tx: mpsc::Sender<Result<FileContentItem>>,
    ) {
        const BATCH_SIZE: i64 = 1000;
        let mut last_line_number: i32 = -1;
        let mut is_first_batch = true;
        let search_pattern = search.map(|s| format!("%{}%", s.to_lowercase()));

        loop {
            // Use OFFSET only on first batch, then use cursor pagination
            let (line_filter, offset_val) = if is_first_batch {
                (-1i32, offset)
            } else {
                (last_line_number, 0i64)
            };
            is_first_batch = false;

            let template_batch = sqlx::query!(
                r#"
                SELECT custom_id, endpoint, method, path, body, model, api_key, line_number
                FROM request_templates
                WHERE file_id = $1 AND ($2 = -1 OR line_number > $2)
                  AND ($5::text IS NULL OR LOWER(custom_id) LIKE $5)
                ORDER BY line_number ASC
                OFFSET $3
                LIMIT $4
                "#,
                *file_id as Uuid,
                line_filter,
                offset_val,
                BATCH_SIZE,
                search_pattern.as_deref(),
            )
            .fetch_all(&pool)
            .await;

            match template_batch {
                Ok(templates) => {
                    if templates.is_empty() {
                        break;
                    }

                    tracing::debug!(
                        "Fetched batch of {} templates, line_numbers {}-{}",
                        templates.len(),
                        templates.first().map(|r| r.line_number).unwrap_or(0),
                        templates.last().map(|r| r.line_number).unwrap_or(0)
                    );

                    for row in templates {
                        last_line_number = row.line_number;

                        let template = RequestTemplateInput {
                            custom_id: row.custom_id,
                            endpoint: row.endpoint,
                            method: row.method,
                            path: row.path,
                            body: row.body,
                            model: row.model,
                            api_key: row.api_key,
                        };
                        if tx
                            .send(Ok(FileContentItem::Template(template)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(FusilladeError::Other(anyhow!(
                            "Failed to fetch template batch: {}",
                            e
                        ))))
                        .await;
                    return;
                }
            }
        }
    }

    /// Stream batch output (completed requests) for a virtual output file
    async fn stream_batch_output(
        pool: sqlx::PgPool,
        file_id: FileId,
        offset: i64,
        search: Option<String>,
        tx: mpsc::Sender<Result<FileContentItem>>,
    ) {
        // First, find the batch that owns this output file
        // Note: We allow streaming even for soft-deleted batches since the output file
        // represents completed work that users should be able to download
        let batch_result = sqlx::query!(
            r#"
            SELECT id
            FROM batches
            WHERE output_file_id = $1
            "#,
            *file_id as Uuid,
        )
        .fetch_one(&pool)
        .await;

        let batch_id = match batch_result {
            Ok(row) => row.id,
            Err(e) => {
                let _ = tx
                    .send(Err(FusilladeError::Other(anyhow!(
                        "Failed to find batch for output file: {}",
                        e
                    ))))
                    .await;
                return;
            }
        };

        // Stream completed requests, ordered by completion time
        // This ensures new completions always append (no out-of-order issues)
        const BATCH_SIZE: i64 = 1000;
        let mut last_completed_at: Option<chrono::DateTime<chrono::Utc>> = None;
        let mut last_id: Uuid = Uuid::nil();
        let mut is_first_batch = true;
        let search_pattern = search.map(|s| format!("%{}%", s.to_lowercase()));

        loop {
            // Use OFFSET only on first batch, then use cursor pagination
            let (cursor_time, cursor_id, offset_val) = if is_first_batch {
                (None, Uuid::nil(), offset)
            } else {
                (last_completed_at, last_id, 0i64)
            };
            is_first_batch = false;

            let request_batch = sqlx::query!(
                r#"
                SELECT id, custom_id, response_status, response_body, completed_at
                FROM requests
                WHERE batch_id = $1
                  AND state = 'completed'
                  AND ($2::TIMESTAMPTZ IS NULL OR completed_at > $2 OR (completed_at = $2 AND id > $3))
                  AND ($6::text IS NULL OR LOWER(custom_id) LIKE $6)
                ORDER BY completed_at ASC, id ASC
                OFFSET $4
                LIMIT $5
                "#,
                batch_id,
                cursor_time,
                cursor_id,
                offset_val,
                BATCH_SIZE,
                search_pattern.as_deref(),
            )
            .fetch_all(&pool)
            .await;

            match request_batch {
                Ok(requests) => {
                    if requests.is_empty() {
                        break;
                    }

                    tracing::debug!("Fetched batch of {} completed requests", requests.len());

                    for row in requests {
                        last_completed_at = row.completed_at;
                        last_id = row.id;

                        let response_body: serde_json::Value = match &row.response_body {
                            Some(body) => match serde_json::from_str(body) {
                                Ok(json) => json,
                                Err(e) => {
                                    tracing::warn!("Failed to parse response body as JSON: {}", e);
                                    serde_json::Value::String(body.to_string())
                                }
                            },
                            None => serde_json::Value::Null,
                        };

                        let output_item = BatchOutputItem {
                            id: format!("batch_req_{}", row.id),
                            custom_id: row.custom_id,
                            response: BatchResponseDetails {
                                status_code: row.response_status.unwrap_or(200),
                                request_id: None, // Could be extracted from response if available
                                body: response_body,
                            },
                            error: None,
                        };

                        if tx
                            .send(Ok(FileContentItem::Output(output_item)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(FusilladeError::Other(anyhow!(
                            "Failed to fetch completed requests: {}",
                            e
                        ))))
                        .await;
                    return;
                }
            }
        }
    }

    /// Stream batch errors (failed requests) for a virtual error file
    async fn stream_batch_error(
        pool: sqlx::PgPool,
        file_id: FileId,
        offset: i64,
        search: Option<String>,
        hide_retriable_before_sla: bool,
        tx: mpsc::Sender<Result<FileContentItem>>,
    ) {
        // First, find the batch that owns this error file
        // Note: We allow streaming even for soft-deleted batches since the error file
        // represents completed work that users should be able to download
        let batch_result = sqlx::query!(
            r#"
            SELECT id, expires_at
            FROM batches
            WHERE error_file_id = $1
            "#,
            *file_id as Uuid,
        )
        .fetch_one(&pool)
        .await;

        let (batch_id, expires_at) = match batch_result {
            Ok(row) => (row.id, row.expires_at),
            Err(e) => {
                let _ = tx
                    .send(Err(FusilladeError::Other(anyhow!(
                        "Failed to find batch for error file: {}",
                        e
                    ))))
                    .await;
                return;
            }
        };

        // Determine whether to filter retriable errors based on SLA status
        let should_filter_retriable = hide_retriable_before_sla && chrono::Utc::now() < expires_at;

        // Stream failed requests, ordered by failure time
        // This ensures new failures always append (no out-of-order issues)
        const BATCH_SIZE: i64 = 1000;
        let mut last_failed_at: Option<chrono::DateTime<chrono::Utc>> = None;
        let mut last_id: Uuid = Uuid::nil();
        let mut is_first_batch = true;
        let search_pattern = search.map(|s| format!("%{}%", s.to_lowercase()));

        loop {
            // Use OFFSET only on first batch, then use cursor pagination
            let (cursor_time, cursor_id, offset_val) = if is_first_batch {
                (None, Uuid::nil(), offset)
            } else {
                (last_failed_at, last_id, 0i64)
            };
            is_first_batch = false;

            // Build dynamic query with error filter
            let mut query_builder = QueryBuilder::new(
                r#"
                SELECT id, custom_id, error, failed_at
                FROM requests
                WHERE batch_id = "#,
            );
            query_builder.push_bind(batch_id);
            query_builder.push(" AND state = 'failed' AND (");
            query_builder.push_bind(cursor_time);
            query_builder.push("::TIMESTAMPTZ IS NULL OR failed_at > ");
            query_builder.push_bind(cursor_time);
            query_builder.push(" OR (failed_at = ");
            query_builder.push_bind(cursor_time);
            query_builder.push(" AND id > ");
            query_builder.push_bind(cursor_id);
            query_builder.push(")) AND (");
            query_builder.push_bind(search_pattern.as_deref());
            query_builder.push("::text IS NULL OR LOWER(custom_id) LIKE ");
            query_builder.push_bind(search_pattern.as_deref());
            query_builder.push(")");

            // Filter retriable errors if before SLA expiry
            if should_filter_retriable {
                query_builder
                    .push(" AND (is_retriable_error = false OR is_retriable_error IS NULL)");
            }

            query_builder.push(" ORDER BY failed_at ASC, id ASC OFFSET ");
            query_builder.push_bind(offset_val);
            query_builder.push(" LIMIT ");
            query_builder.push_bind(BATCH_SIZE);

            let request_batch = query_builder.build().fetch_all(&pool).await;

            match request_batch {
                Ok(requests) => {
                    if requests.is_empty() {
                        break;
                    }

                    tracing::debug!("Fetched batch of {} failed requests", requests.len());

                    for row in requests {
                        let id: Uuid = row.get("id");
                        let custom_id: Option<String> = row.get("custom_id");
                        let error: Option<String> = row.get("error");
                        let failed_at: Option<DateTime<Utc>> = row.get("failed_at");

                        last_failed_at = failed_at;
                        last_id = id;

                        let error_item = BatchErrorItem {
                            id: format!("batch_req_{}", id),
                            custom_id,
                            response: None,
                            error: BatchErrorDetails {
                                code: None, // Could parse from error field if structured
                                message: error.unwrap_or_else(|| "Unknown error".to_string()),
                            },
                        };

                        if tx
                            .send(Ok(FileContentItem::Error(error_item)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(FusilladeError::Other(anyhow!(
                            "Failed to fetch failed requests: {}",
                            e
                        ))))
                        .await;
                    return;
                }
            }
        }
    }

    /// Stream batch results with merged input/output data for the Results view.
    /// This joins requests with their templates to provide input body alongside response/error.
    async fn stream_batch_results(
        pool: sqlx::PgPool,
        batch_id: BatchId,
        offset: i64,
        search: Option<String>,
        status: Option<String>,
        hide_retriable_before_sla: bool,
        tx: mpsc::Sender<Result<crate::batch::BatchResultItem>>,
    ) {
        use crate::batch::{BatchResultItem, BatchResultStatus};

        // First, get the file_id and expires_at from the batch
        // This allows us to query by file_id to avoid duplicates from SLA escalation
        // and to check if we should filter retriable errors
        let (file_id, expires_at) = match sqlx::query!(
            r#"SELECT file_id, expires_at FROM batches WHERE id = $1 AND deleted_at IS NULL"#,
            *batch_id as Uuid,
        )
        .fetch_optional(&pool)
        .await
        {
            Ok(Some(row)) => {
                if let Some(fid) = row.file_id {
                    (fid, row.expires_at)
                } else {
                    let _ = tx
                        .send(Err(FusilladeError::Other(anyhow!(
                            "Batch has no associated file_id"
                        ))))
                        .await;
                    return;
                }
            }
            Ok(None) => {
                let _ = tx
                    .send(Err(FusilladeError::Other(anyhow!("Batch not found"))))
                    .await;
                return;
            }
            Err(e) => {
                let _ = tx
                    .send(Err(FusilladeError::Other(anyhow!(
                        "Failed to fetch batch: {}",
                        e
                    ))))
                    .await;
                return;
            }
        };

        // Determine if we should filter retriable errors based on SLA
        // Before SLA expiry: hide retriable errors
        // After SLA expiry: show all errors
        let should_filter_retriable = hide_retriable_before_sla && chrono::Utc::now() < expires_at;

        const BATCH_SIZE: i64 = 1000;
        let mut last_line_number: i32 = -1;
        let mut is_first_batch = true;
        let search_pattern = search.map(|s| format!("%{}%", s.to_lowercase()));

        // Convert status filter to database state values
        // in_progress maps to both 'claimed' and 'processing' states
        let state_filter: Option<Vec<String>> = status.map(|s| match s.as_str() {
            "in_progress" => vec!["claimed".to_string(), "processing".to_string()],
            other => vec![other.to_string()],
        });

        loop {
            // Use OFFSET only on first batch, then use cursor pagination by line_number
            let (line_filter, offset_val) = if is_first_batch {
                (-1i32, offset)
            } else {
                (last_line_number, 0i64)
            };
            is_first_batch = false;

            // Build dynamic query with error filter
            // The error filter only applies to failed requests
            let mut query_builder = QueryBuilder::new(
                r#"
                SELECT
                    r.id,
                    r.custom_id,
                    r.model,
                    r.state,
                    t.body as input_body,
                    r.response_body,
                    r.error,
                    t.line_number
                FROM request_templates t
                JOIN requests r ON r.template_id = t.id AND r.batch_id = "#,
            );
            query_builder.push_bind(*batch_id as Uuid);
            query_builder.push(" WHERE t.file_id = ");
            query_builder.push_bind(file_id);
            query_builder.push(" AND (");
            query_builder.push_bind(line_filter);
            query_builder.push(" = -1 OR t.line_number > ");
            query_builder.push_bind(line_filter);
            query_builder.push(") AND (");
            query_builder.push_bind(search_pattern.as_deref());
            query_builder.push("::text IS NULL OR LOWER(r.custom_id) LIKE ");
            query_builder.push_bind(search_pattern.as_deref());
            query_builder.push(") AND (");
            query_builder.push_bind(state_filter.as_deref());
            query_builder.push("::text[] IS NULL OR r.state = ANY(");
            query_builder.push_bind(state_filter.as_deref());
            query_builder.push("))");

            // Add error filter condition if needed (only applies to failed requests)
            // Before SLA expiry: hide retriable errors
            if should_filter_retriable {
                query_builder.push(" AND (r.state != 'failed' OR (r.state = 'failed' AND (r.is_retriable_error = false OR r.is_retriable_error IS NULL)))");
            }

            query_builder.push(" ORDER BY t.line_number ASC OFFSET ");
            query_builder.push_bind(offset_val);
            query_builder.push(" LIMIT ");
            query_builder.push_bind(BATCH_SIZE);

            // Query from request_templates joined to requests.
            // For each template, we find the matching request for this batch.
            let request_batch = query_builder.build().fetch_all(&pool).await;

            match request_batch {
                Ok(requests) => {
                    if requests.is_empty() {
                        break;
                    }

                    tracing::debug!("Fetched batch of {} results", requests.len());

                    for row in requests {
                        let line_number: i32 = row.get("line_number");
                        last_line_number = line_number;

                        let input_body_str: String = row.get("input_body");
                        let response_body_opt: Option<String> = row.get("response_body");
                        let state: String = row.get("state");
                        let id: Uuid = row.get("id");
                        let custom_id: Option<String> = row.get("custom_id");
                        let model: String = row.get("model");
                        let error: Option<String> = row.get("error");

                        // Parse input body as JSON
                        let input_body: serde_json::Value = serde_json::from_str(&input_body_str)
                            .unwrap_or_else(|_| serde_json::Value::String(input_body_str.clone()));

                        // Parse response body as JSON if present
                        let response_body: Option<serde_json::Value> =
                            response_body_opt.as_ref().map(|body| {
                                serde_json::from_str(body)
                                    .unwrap_or_else(|_| serde_json::Value::String(body.to_string()))
                            });

                        // Map state to BatchResultStatus
                        let status = match state.as_str() {
                            "completed" => BatchResultStatus::Completed,
                            "failed" => BatchResultStatus::Failed,
                            "pending" => BatchResultStatus::Pending,
                            "claimed" | "processing" => BatchResultStatus::InProgress,
                            _ => BatchResultStatus::Pending, // Default for unknown states
                        };

                        let result_item = BatchResultItem {
                            id: id.to_string(),
                            custom_id,
                            model,
                            input_body,
                            response_body,
                            error,
                            status,
                        };

                        if tx.send(Ok(result_item)).await.is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(FusilladeError::Other(anyhow!(
                            "Failed to fetch batch results: {}",
                            e
                        ))))
                        .await;
                    return;
                }
            }
        }
    }

    /// Create a virtual output file for a batch (stores no data, streams from requests table)
    async fn create_virtual_output_file(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        batch_id: Uuid,
        created_by: &Option<String>,
    ) -> Result<Uuid> {
        let name = format!("batch-{}-output.jsonl", batch_id);
        let description = format!("Output file for batch {}", batch_id);

        let file_id = sqlx::query_scalar!(
            r#"
            INSERT INTO files (name, description, size_bytes, size_finalized, status, purpose, uploaded_by)
            VALUES ($1, $2, 0, FALSE, 'processed', 'batch_output', $3)
            RETURNING id
            "#,
            name,
            description,
            created_by.as_deref(),
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to create output file: {}", e)))?;

        Ok(file_id)
    }

    /// Create a virtual error file for a batch (stores no data, streams from requests table)
    async fn create_virtual_error_file(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        batch_id: Uuid,
        created_by: &Option<String>,
    ) -> Result<Uuid> {
        let name = format!("batch-{}-error.jsonl", batch_id);
        let description = format!("Error file for batch {}", batch_id);

        let file_id = sqlx::query_scalar!(
            r#"
            INSERT INTO files (name, description, size_bytes, size_finalized, status, purpose, uploaded_by)
            VALUES ($1, $2, 0, FALSE, 'processed', 'batch_error', $3)
            RETURNING id
            "#,
            name,
            description,
            created_by.as_deref(),
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to create error file: {}", e)))?;

        Ok(file_id)
    }
}

// Implement DaemonStorage trait
#[async_trait]
impl<P: PoolProvider, H: HttpClient> DaemonStorage for PostgresRequestManager<P, H> {
    async fn persist_daemon<T: DaemonState + Clone>(&self, record: &DaemonRecord<T>) -> Result<()>
    where
        AnyDaemonRecord: From<DaemonRecord<T>>,
    {
        let any_daemon = AnyDaemonRecord::from(record.clone());

        match any_daemon {
            AnyDaemonRecord::Initializing(daemon) => {
                sqlx::query!(
                    r#"
                    INSERT INTO daemons (
                        id, status, hostname, pid, version, config_snapshot,
                        started_at, last_heartbeat, stopped_at,
                        requests_processed, requests_failed, requests_in_flight
                    ) VALUES ($1, 'initializing', $2, $3, $4, $5, $6, NULL, NULL, 0, 0, 0)
                    ON CONFLICT (id) DO UPDATE SET
                        status = 'initializing',
                        started_at = $6,
                        updated_at = NOW()
                    "#,
                    *daemon.data.id as Uuid,
                    daemon.data.hostname,
                    daemon.data.pid,
                    daemon.data.version,
                    daemon.data.config_snapshot,
                    daemon.state.started_at,
                )
                .execute(self.pools.write())
                .await
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to persist daemon: {}", e)))?;
            }
            AnyDaemonRecord::Running(daemon) => {
                sqlx::query!(
                    r#"
                    INSERT INTO daemons (
                        id, status, hostname, pid, version, config_snapshot,
                        started_at, last_heartbeat, stopped_at,
                        requests_processed, requests_failed, requests_in_flight
                    ) VALUES ($1, 'running', $2, $3, $4, $5, $6, $7, NULL, $8, $9, $10)
                    ON CONFLICT (id) DO UPDATE SET
                        status = 'running',
                        last_heartbeat = $7,
                        requests_processed = $8,
                        requests_failed = $9,
                        requests_in_flight = $10,
                        updated_at = NOW()
                    "#,
                    *daemon.data.id as Uuid,
                    daemon.data.hostname,
                    daemon.data.pid,
                    daemon.data.version,
                    daemon.data.config_snapshot,
                    daemon.state.started_at,
                    daemon.state.last_heartbeat,
                    daemon.state.stats.requests_processed as i64,
                    daemon.state.stats.requests_failed as i64,
                    daemon.state.stats.requests_in_flight as i32,
                )
                .execute(self.pools.write())
                .await
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to persist daemon: {}", e)))?;
            }
            AnyDaemonRecord::Dead(daemon) => {
                sqlx::query!(
                    r#"
                    INSERT INTO daemons (
                        id, status, hostname, pid, version, config_snapshot,
                        started_at, last_heartbeat, stopped_at,
                        requests_processed, requests_failed, requests_in_flight
                    ) VALUES ($1, 'dead', $2, $3, $4, $5, $6, NULL, $7, $8, $9, $10)
                    ON CONFLICT (id) DO UPDATE SET
                        status = 'dead',
                        stopped_at = $7,
                        requests_processed = $8,
                        requests_failed = $9,
                        requests_in_flight = $10,
                        updated_at = NOW()
                    "#,
                    *daemon.data.id as Uuid,
                    daemon.data.hostname,
                    daemon.data.pid,
                    daemon.data.version,
                    daemon.data.config_snapshot,
                    daemon.state.started_at,
                    daemon.state.stopped_at,
                    daemon.state.final_stats.requests_processed as i64,
                    daemon.state.final_stats.requests_failed as i64,
                    daemon.state.final_stats.requests_in_flight as i32,
                )
                .execute(self.pools.write())
                .await
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to persist daemon: {}", e)))?;
            }
        }

        Ok(())
    }

    async fn get_daemon(&self, daemon_id: DaemonId) -> Result<AnyDaemonRecord> {
        let row = sqlx::query!(
            r#"
            SELECT
                id, status, hostname, pid, version, config_snapshot,
                started_at, last_heartbeat, stopped_at,
                requests_processed, requests_failed, requests_in_flight
            FROM daemons
            WHERE id = $1
            "#,
            *daemon_id as Uuid,
        )
        .fetch_one(self.pools.read())
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => FusilladeError::Other(anyhow!("Daemon not found")),
            _ => FusilladeError::Other(anyhow!("Failed to fetch daemon: {}", e)),
        })?;

        let data = DaemonData {
            id: DaemonId(row.id),
            hostname: row.hostname,
            pid: row.pid,
            version: row.version,
            config_snapshot: row.config_snapshot,
        };

        let any_daemon = match row.status.as_str() {
            "initializing" => AnyDaemonRecord::Initializing(DaemonRecord {
                data,
                state: Initializing {
                    started_at: row.started_at,
                },
            }),
            "running" => AnyDaemonRecord::Running(DaemonRecord {
                data,
                state: Running {
                    started_at: row.started_at,
                    last_heartbeat: row.last_heartbeat.ok_or_else(|| {
                        FusilladeError::Other(anyhow!("Running daemon missing last_heartbeat"))
                    })?,
                    stats: crate::daemon::DaemonStats {
                        requests_processed: row.requests_processed as u64,
                        requests_failed: row.requests_failed as u64,
                        requests_in_flight: row.requests_in_flight as usize,
                    },
                },
            }),
            "dead" => AnyDaemonRecord::Dead(DaemonRecord {
                data,
                state: Dead {
                    started_at: row.started_at,
                    stopped_at: row.stopped_at.ok_or_else(|| {
                        FusilladeError::Other(anyhow!("Dead daemon missing stopped_at"))
                    })?,
                    final_stats: crate::daemon::DaemonStats {
                        requests_processed: row.requests_processed as u64,
                        requests_failed: row.requests_failed as u64,
                        requests_in_flight: row.requests_in_flight as usize,
                    },
                },
            }),
            _ => {
                return Err(FusilladeError::Other(anyhow!(
                    "Unknown daemon status: {}",
                    row.status
                )));
            }
        };

        Ok(any_daemon)
    }

    async fn list_daemons(
        &self,
        status_filter: Option<DaemonStatus>,
    ) -> Result<Vec<AnyDaemonRecord>> {
        let status_str = status_filter.as_ref().map(|s| s.as_str());

        let rows = sqlx::query!(
            r#"
            SELECT
                id, status, hostname, pid, version, config_snapshot,
                started_at, last_heartbeat, stopped_at,
                requests_processed, requests_failed, requests_in_flight
            FROM daemons
            WHERE ($1::text IS NULL OR status = $1)
            ORDER BY created_at DESC
            "#,
            status_str,
        )
        .fetch_all(self.pools.read())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to list daemons: {}", e)))?;

        let mut daemons = Vec::new();

        for row in rows {
            let data = DaemonData {
                id: DaemonId(row.id),
                hostname: row.hostname,
                pid: row.pid,
                version: row.version,
                config_snapshot: row.config_snapshot,
            };

            let any_daemon = match row.status.as_str() {
                "initializing" => AnyDaemonRecord::Initializing(DaemonRecord {
                    data,
                    state: Initializing {
                        started_at: row.started_at,
                    },
                }),
                "running" => {
                    if let Some(last_heartbeat) = row.last_heartbeat {
                        AnyDaemonRecord::Running(DaemonRecord {
                            data,
                            state: Running {
                                started_at: row.started_at,
                                last_heartbeat,
                                stats: crate::daemon::DaemonStats {
                                    requests_processed: row.requests_processed as u64,
                                    requests_failed: row.requests_failed as u64,
                                    requests_in_flight: row.requests_in_flight as usize,
                                },
                            },
                        })
                    } else {
                        // Skip invalid running daemons
                        continue;
                    }
                }
                "dead" => {
                    if let Some(stopped_at) = row.stopped_at {
                        AnyDaemonRecord::Dead(DaemonRecord {
                            data,
                            state: Dead {
                                started_at: row.started_at,
                                stopped_at,
                                final_stats: crate::daemon::DaemonStats {
                                    requests_processed: row.requests_processed as u64,
                                    requests_failed: row.requests_failed as u64,
                                    requests_in_flight: row.requests_in_flight as usize,
                                },
                            },
                        })
                    } else {
                        // Skip invalid dead daemons
                        continue;
                    }
                }
                _ => {
                    // Skip unknown statuses
                    continue;
                }
            };

            daemons.push(any_daemon);
        }

        Ok(daemons)
    }
}

// Implement DaemonExecutor trait
#[async_trait]
impl<P: PoolProvider, H: HttpClient + 'static> DaemonExecutor<H> for PostgresRequestManager<P, H> {
    fn http_client(&self) -> &Arc<H> {
        &self.http_client
    }

    fn config(&self) -> &DaemonConfig {
        &self.config
    }

    fn run(
        self: Arc<Self>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        tracing::info!("Starting PostgreSQL request manager daemon");

        let daemon = Arc::new(Daemon::new(
            self.clone(),
            self.http_client.clone(),
            self.config.clone(),
            shutdown_token,
        ));

        let handle = tokio::spawn(async move {
            // Daemon will poll for cancelled batches periodically
            daemon.run().await
        });

        tracing::info!("Daemon spawned successfully");

        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestDbPools;
    use crate::daemon::{
        AnyDaemonRecord, DaemonData, DaemonRecord, DaemonStats, DaemonStatus, Dead, Initializing,
        Running,
    };
    use crate::http::MockHttpClient;
    use chrono::Timelike;

    // =========================================================================
    // FILE OPERATIONS
    // =========================================================================
    // Tests for create_file, get_file, list_files, delete_file
    // Basic CRUD operations for file management

    #[sqlx::test]
    async fn test_create_and_get_file(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a file with templates (uses batched strategy by default)
        let file_id = manager
            .create_file(
                "test-file".to_string(),
                Some("A test file".to_string()),
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/completions".to_string(),
                        body: r#"{"model":"gpt-4"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "key1".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/completions".to_string(),
                        body: r#"{"model":"gpt-3.5"}"#.to_string(),
                        model: "gpt-3.5".to_string(),
                        api_key: "key2".to_string(),
                    },
                ],
            )
            .await
            .expect("Failed to create file");

        // Get the file back
        let file = manager.get_file(file_id).await.expect("Failed to get file");

        assert_eq!(file.id, file_id);
        assert_eq!(file.name, "test-file");
        assert_eq!(file.description, Some("A test file".to_string()));

        // Get content for the file
        let content = manager
            .get_file_content(file_id)
            .await
            .expect("Failed to get content");

        assert_eq!(content.len(), 2);
        match &content[0] {
            FileContentItem::Template(t) => assert_eq!(t.model, "gpt-4"),
            _ => panic!("Expected template"),
        }
        match &content[1] {
            FileContentItem::Template(t) => assert_eq!(t.model, "gpt-3.5"),
            _ => panic!("Expected template"),
        }
    }

    // =========================================================================
    // BATCH INSERT STRATEGY TESTS
    // =========================================================================
    // Tests for the batched UNNEST upload mechanism

    #[sqlx::test]
    async fn test_batched_insert_small_file(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a small file with 10 templates using batched strategy (default)
        let templates: Vec<RequestTemplateInput> = (0..10)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("batch-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: format!(r#"{{"prompt":"test {}"}}"#, i),
                model: "gpt-4".to_string(),
                api_key: "key".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file(
                "batched-small".to_string(),
                Some("Small file for batched insert test".to_string()),
                templates,
            )
            .await
            .expect("Failed to create file");

        // Verify all templates were inserted
        let content = manager
            .get_file_content(file_id)
            .await
            .expect("Failed to get content");

        assert_eq!(content.len(), 10, "Should have 10 templates");

        // Verify ordering and custom IDs
        for (i, item) in content.iter().enumerate() {
            match item {
                FileContentItem::Template(t) => {
                    assert_eq!(t.custom_id, Some(format!("batch-{}", i)));
                    assert_eq!(t.body, format!(r#"{{"prompt":"test {}"}}"#, i));
                }
                _ => panic!("Expected template"),
            }
        }
    }

    #[sqlx::test]
    async fn test_batched_insert_large_file(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a larger file with 15k templates (exceeds batch size of 5000)
        let template_count = 15_000;
        let templates: Vec<RequestTemplateInput> = (0..template_count)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("large-batch-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: format!(r#"{{"prompt":"test {}","data":{}}}"#, i, "x".repeat(100)),
                model: "gpt-4".to_string(),
                api_key: "key".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file(
                "batched-large".to_string(),
                Some("Large file for batched insert test".to_string()),
                templates,
            )
            .await
            .expect("Failed to create file");

        // Verify all templates were inserted
        let content = manager
            .get_file_content(file_id)
            .await
            .expect("Failed to get content");

        assert_eq!(
            content.len(),
            template_count,
            "Should have {} templates",
            template_count
        );

        // Spot check first, middle, and last templates
        match &content[0] {
            FileContentItem::Template(t) => {
                assert_eq!(t.custom_id, Some("large-batch-0".to_string()));
            }
            _ => panic!("Expected template"),
        }

        match &content[7500] {
            FileContentItem::Template(t) => {
                assert_eq!(t.custom_id, Some("large-batch-7500".to_string()));
            }
            _ => panic!("Expected template"),
        }

        match &content[14999] {
            FileContentItem::Template(t) => {
                assert_eq!(t.custom_id, Some("large-batch-14999".to_string()));
            }
            _ => panic!("Expected template"),
        }
    }

    #[sqlx::test]
    async fn test_batched_insert_preserves_line_numbers(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        )
        .with_batch_insert_strategy(BatchInsertStrategy::Batched { batch_size: 50 });

        // Create templates across 3 batches (50 per batch)
        let template_count = 150;
        let templates: Vec<RequestTemplateInput> = (0..template_count)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("line-{}", i)),
                endpoint: "https://api.openai.com/v1".to_string(),
                method: "POST".to_string(),
                path: "/chat/completions".to_string(),
                body: format!(
                    r#"{{"model":"gpt-4","messages":[{{"role":"user","content":"line {}"}}]}}"#,
                    i
                ),
                model: "gpt-4".to_string(),
                api_key: "test-key".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file("test-batched-lines".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        // Query line numbers directly from database
        let rows = sqlx::query!(
            r#"
            SELECT custom_id, line_number
            FROM request_templates
            WHERE file_id = $1
            ORDER BY line_number
            "#,
            *file_id as Uuid,
        )
        .fetch_all(&pool)
        .await
        .expect("Failed to query templates");

        // Verify sequential line numbering across batch boundaries (0-indexed)
        assert_eq!(rows.len(), template_count);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(
                row.line_number, i as i32,
                "Line number {} should be sequential",
                i
            );
            assert_eq!(row.custom_id.as_ref().unwrap(), &format!("line-{}", i));
        }
    }

    #[sqlx::test]
    async fn test_batched_insert_with_stream(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a stream with 8000 templates (crosses batch boundary)
        let mut items = vec![FileStreamItem::Metadata(FileMetadata {
            filename: Some("streamed-batched".to_string()),
            description: Some("Batched insert via stream".to_string()),
            purpose: None,
            expires_after_anchor: None,
            expires_after_seconds: None,
            size_bytes: None,
            uploaded_by: Some("test-user".to_string()),
        })];

        for i in 0..8000 {
            items.push(FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some(format!("stream-batch-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: format!(r#"{{"n":{}}}"#, i),
                model: "gpt-4".to_string(),
                api_key: "key".to_string(),
            }));
        }

        let stream = stream::iter(items);
        let file_id = manager
            .create_file_stream(stream)
            .await
            .expect("Failed to create file from stream");

        // Verify file metadata
        let file = manager.get_file(file_id).await.expect("Failed to get file");
        assert_eq!(file.name, "streamed-batched");

        // Verify all templates were inserted
        let content = manager
            .get_file_content(file_id)
            .await
            .expect("Failed to get content");

        assert_eq!(content.len(), 8000);

        // Spot check templates across batch boundaries
        // First batch (0-4999), second batch (5000-7999)
        match &content[4999] {
            FileContentItem::Template(t) => {
                assert_eq!(t.custom_id, Some("stream-batch-4999".to_string()));
            }
            _ => panic!("Expected template"),
        }

        match &content[5000] {
            FileContentItem::Template(t) => {
                assert_eq!(t.custom_id, Some("stream-batch-5000".to_string()));
            }
            _ => panic!("Expected template"),
        }
    }

    #[sqlx::test]
    async fn test_batched_insert_empty_batches(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a stream with only metadata (no templates)
        let items = vec![FileStreamItem::Metadata(FileMetadata {
            filename: Some("empty-file".to_string()),
            description: Some("File with no templates".to_string()),
            purpose: None,
            expires_after_anchor: None,
            expires_after_seconds: None,
            size_bytes: None,
            uploaded_by: None,
        })];

        let stream = stream::iter(items);
        let file_id = manager
            .create_file_stream(stream)
            .await
            .expect("Failed to create empty file");

        // Verify file was created
        let file = manager.get_file(file_id).await.expect("Failed to get file");
        assert_eq!(file.name, "empty-file");

        // Verify no templates
        let content = manager
            .get_file_content(file_id)
            .await
            .expect("Failed to get content");

        assert_eq!(content.len(), 0, "Should have no templates");
    }

    #[sqlx::test]
    async fn test_batched_insert_transactional_rollback(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a stream with templates followed by an error
        let mut items = vec![FileStreamItem::Metadata(FileMetadata {
            filename: Some("rollback-test".to_string()),
            description: Some("Should rollback on error".to_string()),
            purpose: None,
            expires_after_anchor: None,
            expires_after_seconds: None,
            size_bytes: None,
            uploaded_by: None,
        })];

        // Add 3000 templates
        for i in 0..3000 {
            items.push(FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some(format!("rollback-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: format!(r#"{{"n":{}}}"#, i),
                model: "gpt-4".to_string(),
                api_key: "key".to_string(),
            }));
        }

        // Add error in the middle
        items.push(FileStreamItem::Error("Simulated parse error".to_string()));

        // Add more templates after error (should not be inserted)
        for i in 3000..3100 {
            items.push(FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some(format!("rollback-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: format!(r#"{{"n":{}}}"#, i),
                model: "gpt-4".to_string(),
                api_key: "key".to_string(),
            }));
        }

        let stream = stream::iter(items);
        let result = manager.create_file_stream(stream).await;

        // Should fail
        assert!(result.is_err());

        // Verify no file or templates were persisted
        let files =
            sqlx::query!(r#"SELECT COUNT(*) as count FROM files WHERE name = 'rollback-test'"#)
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(files.count, Some(0), "File should not exist after rollback");
    }

    #[sqlx::test]
    async fn test_batched_insert_body_byte_size_calculation(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create templates with varying body sizes
        let templates = vec![
            RequestTemplateInput {
                custom_id: Some("small".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: r#"{"a":1}"#.to_string(), // 7 bytes
                model: "gpt-4".to_string(),
                api_key: "key".to_string(),
            },
            RequestTemplateInput {
                custom_id: Some("large".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: format!(r#"{{"data":"{}"}}"#, "x".repeat(5000)), // ~5010 bytes
                model: "gpt-4".to_string(),
                api_key: "key".to_string(),
            },
        ];

        let file_id = manager
            .create_file("byte-size-test".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        // Query database to verify body_byte_size was calculated correctly
        let rows = sqlx::query!(
            r#"
            SELECT custom_id, body_byte_size, LENGTH(body) as actual_length
            FROM request_templates
            WHERE file_id = $1
            ORDER BY line_number ASC
            "#,
            *file_id as Uuid,
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(rows.len(), 2);

        // Verify small body
        assert_eq!(rows[0].custom_id, Some("small".to_string()));
        assert_eq!(rows[0].body_byte_size, 7);
        assert_eq!(rows[0].actual_length, Some(7));

        // Verify large body
        assert_eq!(rows[1].custom_id, Some("large".to_string()));
        assert_eq!(
            rows[1].body_byte_size,
            rows[1].actual_length.unwrap() as i64
        );
        assert!(rows[1].body_byte_size > 5000);
    }

    #[sqlx::test]
    async fn test_batched_insert_performance_comparison(pool: sqlx::PgPool) {
        use std::time::Instant;

        let http_client = Arc::new(MockHttpClient::new());

        // Test with batched strategy (default)
        let manager_batched = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        );

        let template_count = 1000;
        let templates: Vec<RequestTemplateInput> = (0..template_count)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("perf-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: format!(r#"{{"prompt":"test {}","data":{}}}"#, i, "x".repeat(50)),
                model: "gpt-4".to_string(),
                api_key: "key".to_string(),
            })
            .collect();

        let start = Instant::now();
        let file_id = manager_batched
            .create_file("perf-test-batched".to_string(), None, templates)
            .await
            .expect("Failed to create file");
        let batched_duration = start.elapsed();

        // Verify all templates were inserted
        let content = manager_batched
            .get_file_content(file_id)
            .await
            .expect("Failed to get content");

        assert_eq!(content.len(), template_count);

        println!(
            "Batched insert of {} templates took: {:?}",
            template_count, batched_duration
        );

        // The batched approach should be reasonably fast (under 1 second for 1000 templates)
        // This is a sanity check, not a strict performance requirement
        assert!(
            batched_duration.as_secs() < 2,
            "Batched insert should be fast"
        );
    }

    #[sqlx::test]
    #[should_panic(expected = "batch_size must be greater than 0")]
    async fn test_batched_insert_rejects_zero_batch_size(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let _manager =
            PostgresRequestManager::with_client(TestDbPools::new(pool).await.unwrap(), http_client)
                .with_batch_insert_strategy(BatchInsertStrategy::Batched { batch_size: 0 });
    }

    #[sqlx::test]
    async fn test_batched_insert_valid_batch_sizes(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        // Test batch_size = 1
        let manager1 = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_batch_insert_strategy(BatchInsertStrategy::Batched { batch_size: 1 });

        let templates: Vec<RequestTemplateInput> = (0..5)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("req-{}", i)),
                endpoint: "/v1/chat/completions".to_string(),
                method: "POST".to_string(),
                path: "/v1/chat/completions".to_string(),
                body: format!(
                    r#"{{"model":"gpt-4","messages":[{{"role":"user","content":"test {}"}}]}}"#,
                    i
                ),
                model: "gpt-4".to_string(),
                api_key: "test-key".to_string(),
            })
            .collect();

        let file_id1 = manager1
            .create_file("batch-size-1".to_string(), None, templates.clone())
            .await
            .expect("Failed to create file with batch_size=1");

        let content1 = manager1
            .get_file_content(file_id1)
            .await
            .expect("Failed to get content");
        assert_eq!(
            content1.len(),
            5,
            "Should have 5 templates with batch_size=1"
        );

        // Test batch_size = 100
        let manager100 = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_batch_insert_strategy(BatchInsertStrategy::Batched { batch_size: 100 });

        let file_id100 = manager100
            .create_file("batch-size-100".to_string(), None, templates.clone())
            .await
            .expect("Failed to create file with batch_size=100");

        let content100 = manager100
            .get_file_content(file_id100)
            .await
            .expect("Failed to get content");
        assert_eq!(
            content100.len(),
            5,
            "Should have 5 templates with batch_size=100"
        );

        // Test batch_size = 5000 (default)
        let manager5000 =
            PostgresRequestManager::with_client(TestDbPools::new(pool).await.unwrap(), http_client)
                .with_batch_insert_strategy(BatchInsertStrategy::Batched { batch_size: 5000 });

        let file_id5000 = manager5000
            .create_file("batch-size-5000".to_string(), None, templates)
            .await
            .expect("Failed to create file with batch_size=5000");

        let content5000 = manager5000
            .get_file_content(file_id5000)
            .await
            .expect("Failed to get content");
        assert_eq!(
            content5000.len(),
            5,
            "Should have 5 templates with batch_size=5000"
        );
    }

    // =========================================================================
    // BATCH OPERATIONS
    // =========================================================================
    // Tests for create_batch, get_batch, list_batches, cancel_batch
    // Batch lifecycle management and status tracking

    #[sqlx::test]
    async fn test_create_batch_and_get_status(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a file with 3 templates
        let file_id = manager
            .create_file(
                "batch-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"1"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"2"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"3"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "key".to_string(),
                    },
                ],
            )
            .await
            .expect("Failed to create file");

        // Create a batch
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

        // Get batch status
        let status = manager
            .get_batch_status(batch.id, false)
            .await
            .expect("Failed to get batch status");

        assert_eq!(status.batch_id, batch.id);
        assert_eq!(status.file_id, Some(file_id));
        assert_eq!(status.file_name, Some("batch-test".to_string()));
        assert_eq!(status.total_requests, 3);
        assert_eq!(status.pending_requests, 3);
        assert_eq!(status.completed_requests, 0);
        assert_eq!(status.failed_requests, 0);

        // Get batch requests
        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get batch requests");

        assert_eq!(requests.len(), 3);
        for request in requests {
            assert!(request.is_pending());
        }
    }

    // =========================================================================
    // REQUEST OPERATIONS
    // =========================================================================
    // Tests for claim_requests, cancel_requests, get_requests
    // Request claiming, cancellation, and retrieval

    #[sqlx::test]
    async fn test_claim_requests(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a file with 5 templates
        let file_id = manager
            .create_file(
                "claim-test".to_string(),
                None,
                (0..5)
                    .map(|i| RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());

        // Claim 3 requests
        let claimed = manager
            .claim_requests(3, daemon_id)
            .await
            .expect("Failed to claim requests");

        assert_eq!(claimed.len(), 3);
        for request in &claimed {
            assert_eq!(request.state.daemon_id, daemon_id);
            assert_eq!(request.state.retry_attempt, 0);
        }

        // Try to claim again - should get the remaining 2
        let claimed2 = manager
            .claim_requests(10, daemon_id)
            .await
            .expect("Failed to claim requests");

        assert_eq!(claimed2.len(), 2);

        // Verify batch status shows claimed requests
        let status = manager.get_batch_status(batch.id, false).await.unwrap();
        assert_eq!(status.total_requests, 5);
        assert_eq!(status.pending_requests, 0);
        assert_eq!(status.in_progress_requests, 5); // All claimed
    }

    #[sqlx::test]
    async fn test_cancel_batch(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a file with 3 templates
        let file_id = manager
            .create_file(
                "cancel-test".to_string(),
                None,
                (0..3)
                    .map(|i| RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Verify all are pending
        let status_before = manager.get_batch_status(batch.id, false).await.unwrap();
        assert_eq!(status_before.pending_requests, 3);
        assert_eq!(status_before.canceled_requests, 0);

        // Cancel the batch
        manager.cancel_batch(batch.id).await.unwrap();

        // Verify all are canceled
        let status_after = manager.get_batch_status(batch.id, false).await.unwrap();
        assert_eq!(status_after.pending_requests, 0);
        assert_eq!(status_after.canceled_requests, 3);

        // Get the actual requests - they remain in Pending state as an optimization
        // but are logically canceled (the batch has cancelling_at set)
        let requests = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests.len(), 3);
        for request in requests {
            assert!(matches!(request, AnyRequest::Pending(_)));
        }
    }

    #[sqlx::test]
    async fn test_delete_batch(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a file with templates
        let file_id = manager
            .create_file(
                "delete-batch-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":1}"#.to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":2}"#.to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        // Create a batch
        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Verify batch exists
        let batch_before = manager.get_batch(batch.id, false).await;
        assert!(batch_before.is_ok());

        // Verify requests exist
        let requests_before = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests_before.len(), 2);

        // Delete the batch
        manager.delete_batch(batch.id).await.unwrap();

        // Verify batch is gone
        let batch_after = manager.get_batch(batch.id, false).await;
        assert!(batch_after.is_err());

        // Verify requests are not returned (orphaned with batch_id = NULL, filtered by view)
        let requests_after = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests_after.len(), 0);

        // Verify file still exists (should NOT be cascade deleted)
        let file_after = manager.get_file(file_id).await;
        assert!(file_after.is_ok());

        // Verify deleting non-existent batch returns error
        let delete_result = manager.delete_batch(batch.id).await;
        assert!(delete_result.is_err());
    }

    #[sqlx::test]
    async fn test_cancel_individual_requests(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a file with 5 templates
        let file_id = manager
            .create_file(
                "individual-cancel-test".to_string(),
                None,
                (0..5)
                    .map(|i| RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Get all request IDs
        let requests = manager.get_batch_requests(batch.id).await.unwrap();
        let request_ids: Vec<_> = requests.iter().map(|r| r.id()).collect();

        // Cancel the first 3 requests
        let results = manager
            .cancel_requests(request_ids[0..3].to_vec())
            .await
            .unwrap();

        // All 3 cancellations should succeed
        for result in results {
            assert!(result.is_ok());
        }

        // Verify batch status
        let status = manager.get_batch_status(batch.id, false).await.unwrap();
        assert_eq!(status.pending_requests, 2);
        assert_eq!(status.canceled_requests, 3);

        // Verify the requests
        let all_requests = manager.get_batch_requests(batch.id).await.unwrap();
        let canceled_count = all_requests
            .iter()
            .filter(|r| matches!(r, AnyRequest::Canceled(_)))
            .count();
        assert_eq!(canceled_count, 3);
    }

    #[sqlx::test]
    async fn test_list_files(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create 3 files
        let file1_id = manager
            .create_file("file1".to_string(), Some("First".to_string()), vec![])
            .await
            .unwrap();

        let file2_id = manager
            .create_file("file2".to_string(), Some("Second".to_string()), vec![])
            .await
            .unwrap();

        let file3_id = manager
            .create_file("file3".to_string(), None, vec![])
            .await
            .unwrap();

        // List all files
        let files = manager
            .list_files(crate::batch::FileFilter::default())
            .await
            .unwrap();

        // Should have at least our 3 files (may have more from other tests)
        assert!(files.len() >= 3);

        // Verify our files are present
        let file_ids: Vec<_> = files.iter().map(|f| f.id).collect();
        assert!(file_ids.contains(&file1_id));
        assert!(file_ids.contains(&file2_id));
        assert!(file_ids.contains(&file3_id));

        // Verify names and descriptions
        let file1 = files.iter().find(|f| f.id == file1_id).unwrap();
        assert_eq!(file1.name, "file1");
        assert_eq!(file1.description, Some("First".to_string()));

        let file3 = files.iter().find(|f| f.id == file3_id).unwrap();
        assert_eq!(file3.name, "file3");
        assert_eq!(file3.description, None);
    }

    #[sqlx::test]
    async fn test_list_file_batches(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a file with templates
        let file_id = manager
            .create_file(
                "batch-list-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                    api_key: "key".to_string(),
                }],
            )
            .await
            .unwrap();

        // Create 3 batches
        let batch1 = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();
        let batch2 = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();
        let batch3 = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // List batches for this file
        let batches = manager.list_file_batches(file_id, false).await.unwrap();

        assert_eq!(batches.len(), 3);

        // Verify all batch IDs are present
        let batch_ids: Vec<_> = batches.iter().map(|b| b.batch_id).collect();
        assert!(batch_ids.contains(&batch1.id));
        assert!(batch_ids.contains(&batch2.id));
        assert!(batch_ids.contains(&batch3.id));

        // Verify each batch has 1 pending request
        for batch in batches {
            assert_eq!(batch.total_requests, 1);
            assert_eq!(batch.pending_requests, 1);
        }
    }

    #[sqlx::test]
    async fn test_delete_file_cascade(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a file with templates
        let file_id = manager
            .create_file(
                "delete-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":1}"#.to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":2}"#.to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        // Create a batch
        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Verify the batch exists with file_id set
        let batch_before = manager.get_batch(batch.id, false).await.unwrap();
        assert_eq!(batch_before.file_id, Some(file_id));
        assert!(batch_before.cancelling_at.is_none());
        assert!(batch_before.cancelled_at.is_none());
        assert_eq!(batch_before.pending_requests, 2);

        // Verify we have pending requests with template_id
        let requests_before = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests_before.len(), 2);

        // Delete the file
        manager.delete_file(file_id).await.unwrap();

        // Verify file is gone
        let file_result = manager.get_file(file_id).await;
        assert!(file_result.is_err());

        // Verify batch still exists but file_id is NULL and batch is cancelled
        let batch_after = manager.get_batch(batch.id, false).await.unwrap();
        assert_eq!(batch_after.file_id, None);
        assert!(batch_after.cancelling_at.is_some());
        assert!(batch_after.cancelled_at.is_some());
        assert_eq!(batch_after.canceled_requests, 2); // Both requests should be counted as canceled

        // Verify requests still exist but are skipped when template is deleted
        let requests_after = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests_after.len(), 0); // Requests with deleted templates are skipped
    }

    // =========================================================================
    // REQUEST LIFECYCLE & UNCLAIMING
    // =========================================================================
    // Tests for automatic request lifecycle management and unclaiming logic:
    // - Unclaiming stale claimed requests (claimed but not processing)
    // - Unclaiming stale processing requests (stuck in processing)
    // - Not unclaiming recently claimed requests
    // - Preserving retry_attempt across unclaim operations

    #[sqlx::test]
    async fn test_unclaim_stale_claimed_requests(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        // Create manager with 1-second claim timeout for testing
        let config = crate::daemon::DaemonConfig {
            claim_timeout_ms: 1000,       // 1 second
            processing_timeout_ms: 60000, // 1 minute
            ..Default::default()
        };
        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client,
            )
            .with_config(config),
        );

        // Create a file and batch
        let file_id = manager
            .create_file(
                "stale-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                    api_key: "key".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Claim the request with daemon1
        let daemon1_id = DaemonId::from(Uuid::new_v4());
        let claimed = manager.claim_requests(1, daemon1_id).await.unwrap();
        assert_eq!(claimed.len(), 1);
        let request_id = claimed[0].data.id;

        // Manually set claimed_at to 3 seconds ago (past the 1s timeout)
        sqlx::query!(
            "UPDATE requests SET claimed_at = NOW() - INTERVAL '3 seconds' WHERE id = $1",
            *request_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        // Now daemon2 tries to claim - should unclaim the stale request and re-claim it
        let daemon2_id = DaemonId::from(Uuid::new_v4());
        let reclaimed = manager.claim_requests(1, daemon2_id).await.unwrap();

        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].data.id, request_id);
        assert_eq!(reclaimed[0].state.daemon_id, daemon2_id);

        // Verify the request is now claimed by daemon2
        let status = manager.get_batch_status(batch.id, false).await.unwrap();
        assert_eq!(status.in_progress_requests, 1);
    }

    #[sqlx::test]
    async fn test_unclaim_stale_processing_requests(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        // Create manager with 1-second processing timeout for testing
        let config = crate::daemon::DaemonConfig {
            claim_timeout_ms: 60000,     // 1 minute
            processing_timeout_ms: 1000, // 1 second
            ..Default::default()
        };
        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client,
            )
            .with_config(config),
        );

        // Create a file and batch
        let file_id = manager
            .create_file(
                "stale-processing-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                    api_key: "key".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Claim and manually set to processing state
        let daemon1_id = DaemonId::from(Uuid::new_v4());
        let claimed = manager.claim_requests(1, daemon1_id).await.unwrap();
        assert_eq!(claimed.len(), 1);
        let request_id = claimed[0].data.id;

        // Manually set to processing state with started_at 3 seconds ago
        sqlx::query!(
            r#"
            UPDATE requests
            SET
                state = 'processing',
                started_at = NOW() - INTERVAL '3 seconds'
            WHERE id = $1
            "#,
            *request_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        // Verify it's in processing state
        let status_before = manager.get_batch_status(batch.id, false).await.unwrap();
        assert_eq!(status_before.in_progress_requests, 1);

        // Now daemon2 tries to claim - should unclaim the stale processing request
        let daemon2_id = DaemonId::from(Uuid::new_v4());
        let reclaimed = manager.claim_requests(1, daemon2_id).await.unwrap();

        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].data.id, request_id);
        assert_eq!(reclaimed[0].state.daemon_id, daemon2_id);
    }

    #[sqlx::test]
    async fn test_dont_unclaim_recent_requests(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        // Create manager with long timeouts
        let config = crate::daemon::DaemonConfig {
            claim_timeout_ms: 60000,       // 1 minute
            processing_timeout_ms: 600000, // 10 minutes
            ..Default::default()
        };
        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client,
            )
            .with_config(config),
        );

        // Create a file with 2 templates
        let file_id = manager
            .create_file(
                "recent-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":1}"#.to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":2}"#.to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Daemon1 claims first request
        let daemon1_id = DaemonId::from(Uuid::new_v4());
        let claimed1 = manager.claim_requests(1, daemon1_id).await.unwrap();
        assert_eq!(claimed1.len(), 1);

        // Daemon2 immediately tries to claim - should get the second request, not steal the first
        let daemon2_id = DaemonId::from(Uuid::new_v4());
        let claimed2 = manager.claim_requests(1, daemon2_id).await.unwrap();
        assert_eq!(claimed2.len(), 1);

        // Verify they got different requests
        assert_ne!(claimed1[0].data.id, claimed2[0].data.id);

        // Verify first request still belongs to daemon1
        let results = manager
            .get_requests(vec![claimed1[0].data.id])
            .await
            .unwrap();
        if let Ok(crate::AnyRequest::Claimed(req)) = &results[0] {
            assert_eq!(req.state.daemon_id, daemon1_id);
        } else {
            panic!("Request should still be claimed by daemon1");
        }
    }

    #[sqlx::test]
    async fn test_preserve_retry_attempt_on_unclaim(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        // Create manager with 1-second claim timeout
        let config = crate::daemon::DaemonConfig {
            claim_timeout_ms: 1000,
            processing_timeout_ms: 60000,
            ..Default::default()
        };
        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client,
            )
            .with_config(config),
        );

        // Create a file and batch
        let file_id = manager
            .create_file(
                "retry-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                    api_key: "key".to_string(),
                }],
            )
            .await
            .unwrap();

        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Manually set a request to claimed with retry_attempt=2
        sqlx::query!(
            r#"
            UPDATE requests
            SET
                retry_attempt = 2,
                state = 'claimed',
                daemon_id = $1,
                claimed_at = NOW() - INTERVAL '3 seconds'
            WHERE id IN (SELECT id FROM requests WHERE state = 'pending' LIMIT 1)
            RETURNING id
            "#,
            Uuid::new_v4()
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        // Claim should unclaim the stale request and reclaim it
        let daemon_id = DaemonId::from(Uuid::new_v4());
        let claimed = manager.claim_requests(1, daemon_id).await.unwrap();

        assert_eq!(claimed.len(), 1);
        // Verify retry_attempt is preserved
        assert_eq!(claimed[0].state.retry_attempt, 2);
    }

    // =========================================================================
    // BATCH OUTPUT & ERROR STREAMING
    // =========================================================================
    // Tests for streaming batch results:
    // - get_file_content_stream for output and error files
    // - Virtual file IDs for batch results
    // - Streaming completed and failed requests

    #[sqlx::test]
    async fn test_batch_output_and_error_streaming(pool: sqlx::PgPool) {
        use futures::StreamExt;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a file with 3 templates
        let file_id = manager
            .create_file(
                "streaming-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("req-1".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/chat/completions".to_string(),
                        body: r#"{"prompt":"first"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req-2".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/chat/completions".to_string(),
                        body: r#"{"prompt":"second"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req-3".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/chat/completions".to_string(),
                        body: r#"{"prompt":"third"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "key".to_string(),
                    },
                ],
            )
            .await
            .expect("Failed to create file");

        // Create a batch
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

        // Verify virtual output and error files were created
        assert!(batch.output_file_id.is_some());
        assert!(batch.error_file_id.is_some());
        let output_file_id = batch.output_file_id.unwrap();
        let error_file_id = batch.error_file_id.unwrap();

        // Get the virtual files and verify they exist
        let output_file = manager
            .get_file(output_file_id)
            .await
            .expect("Failed to get output file");
        let error_file = manager
            .get_file(error_file_id)
            .await
            .expect("Failed to get error file");

        assert_eq!(
            output_file.name,
            format!("batch-{}-output.jsonl", batch.id.0)
        );
        assert_eq!(error_file.name, format!("batch-{}-error.jsonl", batch.id.0));

        // Manually mark 2 requests as completed and 1 as failed
        // This simulates what the daemon would do after processing
        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get requests");
        assert_eq!(requests.len(), 3);

        // Mark first request as completed
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = $2,
                completed_at = NOW()
            WHERE id = $1
            "#,
            *requests[0].id() as Uuid,
            r#"{"id":"chatcmpl-123","choices":[{"message":{"content":"Response 1"}}]}"#,
        )
        .execute(&pool)
        .await
        .expect("Failed to mark request as completed");

        // Mark second request as completed
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = $2,
                completed_at = NOW()
            WHERE id = $1
            "#,
            *requests[1].id() as Uuid,
            r#"{"id":"chatcmpl-456","choices":[{"message":{"content":"Response 2"}}]}"#,
        )
        .execute(&pool)
        .await
        .expect("Failed to mark request as completed");

        // Mark third request as failed
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'failed',
                error = $2,
                failed_at = NOW()
            WHERE id = $1
            "#,
            *requests[2].id() as Uuid,
            "Rate limit exceeded",
        )
        .execute(&pool)
        .await
        .expect("Failed to mark request as failed");

        // Stream the output file - should contain 2 completed requests
        let output_stream = manager.get_file_content_stream(output_file_id, 0, None, false);
        let output_items: Vec<_> = output_stream.collect().await;

        assert_eq!(output_items.len(), 2, "Should have 2 output items");

        // Collect and verify custom_ids (order doesn't matter)
        let mut found_custom_ids = Vec::new();
        for item_result in output_items.iter() {
            let item = item_result.as_ref().expect("Output item should be Ok");

            match item {
                FileContentItem::Output(output) => {
                    found_custom_ids.push(output.custom_id.clone());

                    // Verify response structure
                    assert_eq!(output.response.status_code, 200);
                    assert!(output.response.body.is_object());
                    assert!(output.error.is_none());

                    // Verify ID format
                    assert!(output.id.starts_with("batch_req_"));
                }
                _ => panic!("Expected FileContentItem::Output, got different type"),
            }
        }

        // Verify we got both custom IDs (order doesn't matter)
        found_custom_ids.sort();
        assert_eq!(
            found_custom_ids,
            vec![Some("req-1".to_string()), Some("req-2".to_string())]
        );

        // Stream the error file - should contain 1 failed request
        let error_stream = manager.get_file_content_stream(error_file_id, 0, None, false);
        let error_items: Vec<_> = error_stream.collect().await;

        assert_eq!(error_items.len(), 1, "Should have 1 error item");

        // Verify the error item
        let error_result = &error_items[0];
        let error_item = error_result.as_ref().expect("Error item should be Ok");

        match error_item {
            FileContentItem::Error(error) => {
                assert_eq!(error.custom_id, Some("req-3".to_string()));
                assert_eq!(error.error.message, "Rate limit exceeded");
                assert!(error.response.is_none());
                assert!(error.id.starts_with("batch_req_"));
            }
            _ => panic!("Expected FileContentItem::Error, got different type"),
        }

        // Verify that streaming a regular input file still works
        let input_stream = manager.get_file_content_stream(file_id, 0, None, false);
        let input_items: Vec<_> = input_stream.collect().await;

        assert_eq!(input_items.len(), 3, "Input file should have 3 templates");

        for item_result in input_items {
            let item = item_result.expect("Input item should be Ok");
            match item {
                FileContentItem::Template(_) => {
                    // Expected - input files contain templates
                }
                _ => panic!("Expected FileContentItem::Template for input file"),
            }
        }
    }

    // =========================================================================
    // DAEMON STORAGE
    // =========================================================================
    // Tests for daemon state persistence:
    // - persist_daemon and get_daemon
    // - Heartbeat updates
    // - list_daemons with filtering

    #[sqlx::test]
    async fn test_daemon_persist_and_get(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let daemon_id = DaemonId(Uuid::new_v4());
        let daemon_data = DaemonData {
            id: daemon_id,
            hostname: "test-host".to_string(),
            pid: 12345,
            version: "1.0.0".to_string(),
            config_snapshot: serde_json::json!({"test": "config"}),
        };

        // Test Initializing state
        let initializing = DaemonRecord {
            data: daemon_data.clone(),
            state: Initializing {
                started_at: Utc::now(),
            },
        };

        manager.persist_daemon(&initializing).await.unwrap();

        let retrieved = manager.get_daemon(daemon_id).await.unwrap();
        match retrieved {
            AnyDaemonRecord::Initializing(d) => {
                assert_eq!(d.data.id, daemon_id);
                assert_eq!(d.data.hostname, "test-host");
            }
            _ => panic!("Expected Initializing state"),
        }

        // Test Running state
        let running = DaemonRecord {
            data: daemon_data.clone(),
            state: Running {
                started_at: Utc::now(),
                last_heartbeat: Utc::now(),
                stats: DaemonStats {
                    requests_processed: 10,
                    requests_failed: 2,
                    requests_in_flight: 3,
                },
            },
        };

        manager.persist_daemon(&running).await.unwrap();

        let retrieved = manager.get_daemon(daemon_id).await.unwrap();
        match retrieved {
            AnyDaemonRecord::Running(d) => {
                assert_eq!(d.data.id, daemon_id);
                assert_eq!(d.state.stats.requests_processed, 10);
                assert_eq!(d.state.stats.requests_failed, 2);
                assert_eq!(d.state.stats.requests_in_flight, 3);
            }
            _ => panic!("Expected Running state"),
        }

        // Test Dead state
        let dead = DaemonRecord {
            data: daemon_data,
            state: Dead {
                started_at: Utc::now() - chrono::Duration::hours(1),
                stopped_at: Utc::now(),
                final_stats: DaemonStats {
                    requests_processed: 100,
                    requests_failed: 5,
                    requests_in_flight: 0,
                },
            },
        };

        manager.persist_daemon(&dead).await.unwrap();

        let retrieved = manager.get_daemon(daemon_id).await.unwrap();
        match retrieved {
            AnyDaemonRecord::Dead(d) => {
                assert_eq!(d.data.id, daemon_id);
                assert_eq!(d.state.final_stats.requests_processed, 100);
                assert_eq!(d.state.final_stats.requests_failed, 5);
            }
            _ => panic!("Expected Dead state"),
        }
    }

    #[sqlx::test]
    async fn test_daemon_list_all(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create multiple daemons in different states
        let daemon1 = DaemonRecord {
            data: DaemonData {
                id: DaemonId(Uuid::new_v4()),
                hostname: "host1".to_string(),
                pid: 1001,
                version: "1.0.0".to_string(),
                config_snapshot: serde_json::json!({}),
            },
            state: Running {
                started_at: Utc::now(),
                last_heartbeat: Utc::now(),
                stats: DaemonStats::default(),
            },
        };

        let daemon2 = DaemonRecord {
            data: DaemonData {
                id: DaemonId(Uuid::new_v4()),
                hostname: "host2".to_string(),
                pid: 1002,
                version: "1.0.0".to_string(),
                config_snapshot: serde_json::json!({}),
            },
            state: Running {
                started_at: Utc::now(),
                last_heartbeat: Utc::now(),
                stats: DaemonStats::default(),
            },
        };

        let daemon3 = DaemonRecord {
            data: DaemonData {
                id: DaemonId(Uuid::new_v4()),
                hostname: "host3".to_string(),
                pid: 1003,
                version: "1.0.0".to_string(),
                config_snapshot: serde_json::json!({}),
            },
            state: Dead {
                started_at: Utc::now() - chrono::Duration::hours(1),
                stopped_at: Utc::now(),
                final_stats: DaemonStats::default(),
            },
        };

        manager.persist_daemon(&daemon1).await.unwrap();
        manager.persist_daemon(&daemon2).await.unwrap();
        manager.persist_daemon(&daemon3).await.unwrap();

        // List all daemons
        let all = manager.list_daemons(None).await.unwrap();
        assert_eq!(all.len(), 3);

        // List only running daemons
        let running = manager
            .list_daemons(Some(DaemonStatus::Running))
            .await
            .unwrap();
        assert_eq!(running.len(), 2);

        // List only dead daemons
        let dead = manager
            .list_daemons(Some(DaemonStatus::Dead))
            .await
            .unwrap();
        assert_eq!(dead.len(), 1);
    }

    #[sqlx::test]
    async fn test_daemon_heartbeat_updates(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let daemon_id = DaemonId(Uuid::new_v4());
        let daemon_data = DaemonData {
            id: daemon_id,
            hostname: "test-host".to_string(),
            pid: 12345,
            version: "1.0.0".to_string(),
            config_snapshot: serde_json::json!({}),
        };

        // Start with Running state
        let running = DaemonRecord {
            data: daemon_data,
            state: Running {
                started_at: Utc::now(),
                last_heartbeat: Utc::now(),
                stats: DaemonStats {
                    requests_processed: 0,
                    requests_failed: 0,
                    requests_in_flight: 0,
                },
            },
        };

        manager.persist_daemon(&running).await.unwrap();

        // Simulate heartbeat updates
        for i in 1..=3 {
            let updated = DaemonRecord {
                data: running.data.clone(),
                state: Running {
                    started_at: running.state.started_at,
                    last_heartbeat: Utc::now(),
                    stats: DaemonStats {
                        requests_processed: i * 10,
                        requests_failed: i,
                        requests_in_flight: i as usize,
                    },
                },
            };
            manager.persist_daemon(&updated).await.unwrap();
        }

        // Verify final state
        let retrieved = manager.get_daemon(daemon_id).await.unwrap();
        match retrieved {
            AnyDaemonRecord::Running(d) => {
                assert_eq!(d.state.stats.requests_processed, 30);
                assert_eq!(d.state.stats.requests_failed, 3);
                assert_eq!(d.state.stats.requests_in_flight, 3);
            }
            _ => panic!("Expected Running state"),
        }
    }

    // =========================================================================
    // FILE STREAMING (create_file_stream)
    // =========================================================================
    // Tests for streaming file uploads:
    // - Creating files from streams (metadata, templates, outputs)
    // - Metadata and template ordering edge cases
    // - Error handling in streams
    // - Filename generation and conflict detection

    #[sqlx::test]
    async fn test_create_file_stream_with_metadata_and_templates(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a stream with metadata first, then templates
        let items = vec![
            FileStreamItem::Metadata(FileMetadata {
                filename: Some("streamed-file".to_string()),
                description: Some("A file created via streaming".to_string()),
                purpose: None,
                expires_after_anchor: None,
                expires_after_seconds: None,
                size_bytes: None,
                uploaded_by: Some("test-user".to_string()),
            }),
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some("stream-1".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: r#"{"prompt":"first"}"#.to_string(),
                model: "gpt-4".to_string(),
                api_key: "key1".to_string(),
            }),
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some("stream-2".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: r#"{"prompt":"second"}"#.to_string(),
                model: "gpt-3.5".to_string(),
                api_key: "key2".to_string(),
            }),
        ];

        let stream = stream::iter(items);

        // Create file from stream
        let file_id = manager
            .create_file_stream(stream)
            .await
            .expect("Failed to create file from stream");

        // Verify the file was created with correct metadata
        let file = manager.get_file(file_id).await.expect("Failed to get file");
        assert_eq!(file.name, "streamed-file");
        assert_eq!(
            file.description,
            Some("A file created via streaming".to_string())
        );

        // Verify templates were created
        let content = manager
            .get_file_content(file_id)
            .await
            .expect("Failed to get content");
        assert_eq!(content.len(), 2);

        match &content[0] {
            FileContentItem::Template(t) => {
                assert_eq!(t.custom_id, Some("stream-1".to_string()));
                assert_eq!(t.model, "gpt-4");
            }
            _ => panic!("Expected template"),
        }
        match &content[1] {
            FileContentItem::Template(t) => {
                assert_eq!(t.custom_id, Some("stream-2".to_string()));
                assert_eq!(t.model, "gpt-3.5");
            }
            _ => panic!("Expected template"),
        }
    }

    #[sqlx::test]
    async fn test_create_file_stream_templates_before_metadata(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a stream with templates first, then metadata
        // This tests that metadata (including filename) can come after templates
        // and will properly update the file
        let items = vec![
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: r#"{"n":1}"#.to_string(),
                model: "test".to_string(),
                api_key: "key".to_string(),
            }),
            FileStreamItem::Metadata(FileMetadata {
                filename: Some("late-metadata".to_string()),
                description: Some("Metadata came late".to_string()),
                purpose: None,
                expires_after_anchor: None,
                expires_after_seconds: None,
                size_bytes: None,
                uploaded_by: Some("test-user".to_string()),
            }),
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: r#"{"n":2}"#.to_string(),
                model: "test".to_string(),
                api_key: "key".to_string(),
            }),
        ];

        let stream = stream::iter(items);
        let file_id = manager
            .create_file_stream(stream)
            .await
            .expect("Failed to create file from stream");

        // File should have the metadata even though it came after first template
        let file = manager.get_file(file_id).await.unwrap();
        assert_eq!(file.name, "late-metadata");
        assert_eq!(file.description, Some("Metadata came late".to_string()));

        // Should have 2 templates
        let content = manager.get_file_content(file_id).await.unwrap();
        assert_eq!(content.len(), 2);
    }

    #[sqlx::test]
    async fn test_create_file_stream_error_handling(pool: sqlx::PgPool) {
        use crate::batch::FileStreamItem;
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a stream with an error in the middle
        let items = vec![
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: r#"{"n":1}"#.to_string(),
                model: "test".to_string(),
                api_key: "key".to_string(),
            }),
            FileStreamItem::Error("Invalid JSON on line 2".to_string()),
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: r#"{"n":2}"#.to_string(),
                model: "test".to_string(),
                api_key: "key".to_string(),
            }),
        ];

        let stream = stream::iter(items);
        let result = manager.create_file_stream(stream).await;

        // Should fail with validation error
        assert!(result.is_err());
        match result {
            Err(FusilladeError::ValidationError(msg)) => {
                assert_eq!(msg, "Invalid JSON on line 2");
            }
            _ => panic!("Expected ValidationError"),
        }
    }

    // =========================================================================
    // GET OPERATIONS (get_batch, get_requests)
    // =========================================================================
    // Tests for retrieving batch and request data:
    // - get_batch with various states
    // - get_batch error handling (not found)
    // - get_requests with different states
    // - Custom ID preservation and handling

    #[sqlx::test]
    async fn test_get_batch(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a file and batch
        let file_id = manager
            .create_file(
                "batch-retrieval-test".to_string(),
                Some("Test file for batch retrieval".to_string()),
                vec![RequestTemplateInput {
                    custom_id: Some("req-1".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/completions".to_string(),
                    body: r#"{"prompt":"test"}"#.to_string(),
                    model: "gpt-4".to_string(),
                    api_key: "key".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch_input = crate::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: Some(serde_json::json!({"project": "test"})),
            created_by: Some("test-user".to_string()),
        };

        let created_batch = manager.create_batch(batch_input).await.unwrap();

        // Retrieve the batch
        let retrieved_batch = manager
            .get_batch(created_batch.id, false)
            .await
            .expect("Failed to get batch");

        // Verify all fields match
        assert_eq!(retrieved_batch.id, created_batch.id);
        assert_eq!(retrieved_batch.file_id, Some(file_id));
        assert_eq!(retrieved_batch.endpoint, "/v1/chat/completions");
        assert_eq!(retrieved_batch.completion_window, "24h");
        assert_eq!(
            retrieved_batch.metadata,
            Some(serde_json::json!({"project": "test"}))
        );
        assert_eq!(retrieved_batch.created_by, Some("test-user".to_string()));
        assert!(retrieved_batch.output_file_id.is_some());
        assert!(retrieved_batch.error_file_id.is_some());
        assert_eq!(retrieved_batch.total_requests, 1);
        assert_eq!(retrieved_batch.pending_requests, 1);
        assert_eq!(retrieved_batch.completed_requests, 0);
    }

    #[sqlx::test]
    async fn test_get_batch_not_found(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Try to get a batch that doesn't exist
        let fake_batch_id = BatchId(Uuid::new_v4());
        let result = manager.get_batch(fake_batch_id, false).await;

        // Should return an error
        assert!(result.is_err());
        match result {
            Err(FusilladeError::Other(e)) => {
                assert!(e.to_string().contains("Batch not found"));
            }
            _ => panic!("Expected Other error with 'Batch not found' message"),
        }
    }

    #[sqlx::test]
    async fn test_get_batch_with_progress(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a batch with multiple requests
        let file_id = manager
            .create_file(
                "progress-test".to_string(),
                None,
                (0..5)
                    .map(|i| RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Claim and complete some requests
        let daemon_id = DaemonId::from(Uuid::new_v4());
        let claimed = manager.claim_requests(2, daemon_id).await.unwrap();

        // Mark one as completed
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = '{"result":"ok"}',
                completed_at = NOW()
            WHERE id = $1
            "#,
            *claimed[0].data.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        // Get the batch and verify progress
        let retrieved = manager.get_batch(batch.id, false).await.unwrap();
        assert_eq!(retrieved.total_requests, 5);
        assert_eq!(retrieved.pending_requests, 3);
        assert_eq!(retrieved.in_progress_requests, 1); // Still claimed
        assert_eq!(retrieved.completed_requests, 1);
        assert_eq!(retrieved.failed_requests, 0);
        assert_eq!(retrieved.canceled_requests, 0);
    }

    #[sqlx::test]
    async fn test_get_batch_lazy_finalization(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a batch with multiple requests
        let file_id = manager
            .create_file(
                "lazy-finalization-test".to_string(),
                None,
                (0..3)
                    .map(|i| RequestTemplateInput {
                        custom_id: Some(format!("req-{}", i)),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Manually mark all requests as completed (simulating terminal state without daemon)
        // This bypasses normal timestamp setting in persist()
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = '{"result":"ok"}',
                completed_at = NOW()
            WHERE batch_id = $1
            "#,
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        // Call get_batch() - this should trigger lazy finalization UPDATE
        // If the UPDATE incorrectly used .read() pool, this would fail with TestDbPools
        let retrieved = manager.get_batch(batch.id, false).await.unwrap();

        // Verify the batch is marked as completed
        assert_eq!(retrieved.total_requests, 3);
        assert_eq!(retrieved.completed_requests, 3);
        assert_eq!(retrieved.pending_requests, 0);
        assert_eq!(retrieved.failed_requests, 0);

        // Verify lazy finalization set the timestamps
        assert!(
            retrieved.finalizing_at.is_some(),
            "finalizing_at should be set by lazy finalization"
        );
        assert!(
            retrieved.completed_at.is_some(),
            "completed_at should be set by lazy finalization"
        );
        assert!(
            retrieved.failed_at.is_none(),
            "failed_at should be None for completed batch"
        );

        // Call get_batch again - should not trigger UPDATE again (idempotent)
        let retrieved_again = manager.get_batch(batch.id, false).await.unwrap();

        // Compare timestamps with microsecond precision (PostgreSQL limitation)
        // Truncate nanoseconds to avoid precision mismatch
        let truncate_nanos = |ts: Option<chrono::DateTime<chrono::Utc>>| {
            ts.map(|t| t.with_nanosecond(t.nanosecond() / 1000 * 1000).unwrap())
        };

        assert_eq!(
            truncate_nanos(retrieved.finalizing_at),
            truncate_nanos(retrieved_again.finalizing_at)
        );
        assert_eq!(
            truncate_nanos(retrieved.completed_at),
            truncate_nanos(retrieved_again.completed_at)
        );
    }

    #[sqlx::test]
    async fn test_get_requests_various_states(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a batch with 5 requests
        let file_id = manager
            .create_file(
                "get-requests-test".to_string(),
                None,
                (0..5)
                    .map(|i| RequestTemplateInput {
                        custom_id: Some(format!("req-{}", i)),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        let all_requests = manager.get_batch_requests(batch.id).await.unwrap();
        let request_ids: Vec<_> = all_requests.iter().map(|r| r.id()).collect();

        // Put requests in different states
        let daemon_id = DaemonId::from(Uuid::new_v4());
        let claimed = manager.claim_requests(2, daemon_id).await.unwrap();
        let claimed_ids: Vec<_> = claimed.iter().map(|r| r.data.id).collect();

        // Mark first claimed as completed (needs started_at for Processing->Completed transition)
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                started_at = NOW() - INTERVAL '1 minute',
                response_status = 200,
                response_body = '{"done":true}',
                completed_at = NOW()
            WHERE id = $1
            "#,
            *claimed_ids[0] as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        // Mark one pending request as failed
        let pending_id = request_ids
            .iter()
            .find(|id| !claimed_ids.contains(id))
            .unwrap();
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'failed',
                error = 'Rate limit exceeded',
                failed_at = NOW()
            WHERE id = $1
            "#,
            **pending_id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        // Retrieve all requests
        let results = manager.get_requests(request_ids.clone()).await.unwrap();
        assert_eq!(results.len(), 5);

        // Verify states
        let states: Vec<_> = results
            .iter()
            .map(|r| match r {
                Ok(AnyRequest::Pending(_)) => "pending",
                Ok(AnyRequest::Claimed(_)) => "claimed",
                Ok(AnyRequest::Processing(_)) => "processing",
                Ok(AnyRequest::Completed(_)) => "completed",
                Ok(AnyRequest::Failed(_)) => "failed",
                Ok(AnyRequest::Canceled(_)) => "canceled",
                Err(_) => "error",
            })
            .collect();

        // Should have: 1 completed, 1 failed, 1 claimed, 2 pending
        assert_eq!(states.iter().filter(|&&s| s == "completed").count(), 1);
        assert_eq!(states.iter().filter(|&&s| s == "failed").count(), 1);
        assert_eq!(states.iter().filter(|&&s| s == "claimed").count(), 1);
        assert_eq!(states.iter().filter(|&&s| s == "pending").count(), 2);
    }

    #[sqlx::test]
    async fn test_get_requests_preserves_custom_ids(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create requests with custom IDs
        let file_id = manager
            .create_file(
                "custom-id-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("my-custom-id-1".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"test":1}"#.to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("my-custom-id-2".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"test":2}"#.to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        let all_requests = manager.get_batch_requests(batch.id).await.unwrap();
        let request_ids: Vec<_> = all_requests.iter().map(|r| r.id()).collect();

        // Get requests
        let results = manager.get_requests(request_ids).await.unwrap();
        assert_eq!(results.len(), 2);

        // Verify custom IDs are preserved
        for result in results {
            let request = result.expect("Request should be Ok");
            let custom_id = match &request {
                AnyRequest::Pending(r) => &r.data.custom_id,
                _ => panic!("Expected Pending"),
            };

            assert!(
                custom_id == &Some("my-custom-id-1".to_string())
                    || custom_id == &Some("my-custom-id-2".to_string())
            );
        }
    }

    #[sqlx::test]
    async fn test_get_requests_with_nonexistent_ids(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create one real request
        let file_id = manager
            .create_file(
                "mixed-ids-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{}"#.to_string(),
                    model: "test".to_string(),
                    api_key: "key".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        let all_requests = manager.get_batch_requests(batch.id).await.unwrap();
        let real_id = all_requests[0].id();

        // Mix real and fake IDs
        let mixed_ids = vec![
            real_id,
            RequestId(Uuid::new_v4()),
            RequestId(Uuid::new_v4()),
        ];

        let results = manager.get_requests(mixed_ids).await.unwrap();

        // Should get results for all IDs requested
        // Real ID should be Ok, fake IDs should be Err
        assert_eq!(results.len(), 3);
        assert!(results[0].is_ok()); // Real request
        assert!(results[1].is_err()); // Fake ID
        assert!(results[2].is_err()); // Fake ID
    }

    // =========================================================================
    // CONCURRENCY & GLOBAL LIMITS
    // =========================================================================
    // Tests for per-model concurrency limits:
    // - Global per-model limits enforced across daemons
    // - Request claiming respects concurrent processing limits

    #[sqlx::test]
    async fn test_global_per_model_limit_enforced_across_daemons(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        // Set up manager with per-model limit of 3
        let config = crate::daemon::DaemonConfig::default();
        config
            .model_concurrency_limits
            .insert("model-a".to_string(), 3);
        config
            .model_concurrency_limits
            .insert("model-b".to_string(), 3);

        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client,
            )
            .with_config(config),
        );

        // Create file with 20 requests (10 per model)
        let mut templates = Vec::new();
        for model in &["model-a", "model-b"] {
            for n in 1..=10 {
                templates.push(RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: format!(r#"{{"model":"{}","n":{}}}"#, model, n),
                    model: model.to_string(),
                    api_key: "key".to_string(),
                });
            }
        }

        let file_id = manager
            .create_file("multi-daemon-test".to_string(), None, templates)
            .await
            .unwrap();

        let _batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Simulate 3 daemons claiming simultaneously
        let daemon1_id = DaemonId::from(Uuid::new_v4());
        let daemon2_id = DaemonId::from(Uuid::new_v4());
        let daemon3_id = DaemonId::from(Uuid::new_v4());

        // Each daemon tries to claim 10 requests (more than available per model)
        let claimed1 = manager.claim_requests(10, daemon1_id).await.unwrap();
        let claimed2 = manager.claim_requests(10, daemon2_id).await.unwrap();
        let claimed3 = manager.claim_requests(10, daemon3_id).await.unwrap();

        // Count requests per model across all daemons
        let mut model_counts = std::collections::HashMap::new();
        for claimed in [&claimed1, &claimed2, &claimed3] {
            for request in claimed {
                *model_counts.entry(request.data.model.clone()).or_insert(0) += 1;
            }
        }

        // Verify global per-model limits are enforced
        assert!(
            *model_counts.get("model-a").unwrap_or(&0) <= 3,
            "model-a should not exceed limit of 3 across all daemons, got {}",
            model_counts.get("model-a").unwrap_or(&0)
        );
        assert!(
            *model_counts.get("model-b").unwrap_or(&0) <= 3,
            "model-b should not exceed limit of 3 across all daemons, got {}",
            model_counts.get("model-b").unwrap_or(&0)
        );

        // Total claimed should be at most 6 (3 per model × 2 models)
        let total: i32 = model_counts.values().sum();
        assert!(
            total <= 6,
            "Total claimed should not exceed 6 (respecting per-model limits), got {}",
            total
        );
    }

    // =========================================================================
    // FILENAME HANDLING
    // =========================================================================
    // Tests for filename validation and metadata handling:
    // - Filename timing edge cases (before template, after stub)
    // - Auto-generated vs real filenames
    // - Multiple metadata updates
    // - Empty file handling

    #[sqlx::test]
    async fn test_auto_generated_filename_then_real_filename_differs(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a file where:
        // 1. Template arrives first (creates stub with auto-generated UUID name)
        // 2. Metadata arrives with a different, valid filename
        // 3. Should succeed and update the stub name
        let items = vec![
            FileStreamItem::Metadata(FileMetadata {
                uploaded_by: Some("user1".to_string()),
                // No filename yet
                ..Default::default()
            }),
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some("test-1".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: "{}".to_string(),
                model: "test".to_string(),
                api_key: "key".to_string(),
            }),
            // Real filename arrives
            FileStreamItem::Metadata(FileMetadata {
                filename: Some("updated-filename.jsonl".to_string()),
                uploaded_by: Some("user1".to_string()),
                ..Default::default()
            }),
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some("test-2".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: "{}".to_string(),
                model: "test".to_string(),
                api_key: "key".to_string(),
            }),
        ];

        let file_id = manager
            .create_file_stream(stream::iter(items))
            .await
            .expect("Should create file successfully");

        // Verify the final filename is the one from metadata, not auto-generated
        let file = manager.get_file(file_id).await.unwrap();
        assert_eq!(file.name, "updated-filename.jsonl");
        assert_eq!(file.uploaded_by, Some("user1".to_string()));

        // Verify both templates were created
        let content = manager.get_file_content(file_id).await.unwrap();
        assert_eq!(content.len(), 2);
    }

    #[sqlx::test]
    async fn test_multiple_metadata_updates_last_wins(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Send multiple metadata items - last one should win
        let items = vec![
            FileStreamItem::Metadata(FileMetadata {
                filename: Some("first-name.jsonl".to_string()),
                description: Some("First description".to_string()),
                uploaded_by: Some("user1".to_string()),
                ..Default::default()
            }),
            FileStreamItem::Metadata(FileMetadata {
                filename: Some("second-name.jsonl".to_string()),
                description: Some("Second description".to_string()),
                uploaded_by: Some("user1".to_string()),
                ..Default::default()
            }),
            FileStreamItem::Metadata(FileMetadata {
                filename: Some("final-name.jsonl".to_string()),
                description: Some("Final description".to_string()),
                uploaded_by: Some("user1".to_string()),
                ..Default::default()
            }),
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: "{}".to_string(),
                model: "test".to_string(),
                api_key: "key".to_string(),
            }),
        ];

        let file_id = manager
            .create_file_stream(stream::iter(items))
            .await
            .expect("Should create file");

        let file = manager.get_file(file_id).await.unwrap();
        assert_eq!(file.name, "final-name.jsonl");
        assert_eq!(file.description, Some("Final description".to_string()));
    }

    #[sqlx::test]
    async fn test_empty_file_no_templates_but_with_filename(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a file with metadata but no templates
        let items = vec![FileStreamItem::Metadata(FileMetadata {
            filename: Some("empty-file.jsonl".to_string()),
            description: Some("A file with no templates".to_string()),
            uploaded_by: Some("user1".to_string()),
            ..Default::default()
        })];

        let file_id = manager
            .create_file_stream(stream::iter(items))
            .await
            .expect("Should create empty file");

        let file = manager.get_file(file_id).await.unwrap();
        assert_eq!(file.name, "empty-file.jsonl");
        assert_eq!(
            file.description,
            Some("A file with no templates".to_string())
        );

        // Verify no templates
        let content = manager.get_file_content(file_id).await.unwrap();
        assert_eq!(content.len(), 0);
    }

    /// Helper to poll for a condition with timeout
    async fn wait_for<F, Fut>(mut check: F, timeout: std::time::Duration) -> bool
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if check().await {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        false
    }

    #[sqlx::test]
    async fn test_batch_cancellation_with_stream(pool: sqlx::PgPool) {
        use crate::http::HttpResponse;
        use std::time::Duration;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = Arc::new(PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        ));

        // Create a file with templates
        let file_id = manager
            .create_file(
                "test_cancellation".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("req1".to_string()),
                        endpoint: "http://example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"test": 1}"#.to_string(),
                        model: "model-a".to_string(),
                        api_key: "test-key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req2".to_string()),
                        endpoint: "http://example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"test": 2}"#.to_string(),
                        model: "model-a".to_string(),
                        api_key: "test-key".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        // Create a batch
        let batch = manager
            .create_batch(BatchInput {
                file_id,
                created_by: Some("test-user".to_string()),
                completion_window: "24h".to_string(),
                endpoint: "/v1/chat/completions".to_string(),
                metadata: None,
            })
            .await
            .unwrap();

        // Set up triggered responses that won't complete until we tell them to
        http_client.clear_calls();
        let trigger1 = http_client.add_response_with_trigger(
            "POST /test",
            Ok(HttpResponse {
                status: 200,
                body: "ok".to_string(),
            }),
        );
        let _trigger2 = http_client.add_response_with_trigger(
            "POST /test",
            Ok(HttpResponse {
                status: 200,
                body: "ok".to_string(),
            }),
        );

        // Start a daemon with the mock cancellation stream
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        let config = crate::daemon::DaemonConfig {
            claim_batch_size: 10,
            default_model_concurrency: 5,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
            claim_interval_ms: 10,
            max_retries: Some(10_000),
            stop_before_deadline_ms: Some(900_000),
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 30000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 1000,
            should_retry: Arc::new(|_| false),
            claim_timeout_ms: 5000,
            processing_timeout_ms: 10000,
            cancellation_poll_interval_ms: 100, // Fast polling for tests
            ..Default::default()
        };

        let daemon = Arc::new(crate::daemon::Daemon::new(
            manager.clone(),
            http_client.clone(),
            config,
            shutdown_token.clone(),
        ));

        // Run daemon (it will poll for cancelled batches)
        let daemon_handle = tokio::spawn({
            let daemon = daemon.clone();
            async move { daemon.run().await }
        });

        // Verify we have 2 requests in the batch
        let requests = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests.len(), 2);

        // Wait for both requests to be processing (blocked on triggers)
        let manager_clone = manager.clone();
        let batch_id = batch.id;
        let reached_processing = wait_for(
            || async {
                if let Ok(reqs) = manager_clone.get_batch_requests(batch_id).await {
                    reqs.iter().all(|r| matches!(r, AnyRequest::Processing(_)))
                } else {
                    false
                }
            },
            Duration::from_secs(3),
        )
        .await;
        assert!(
            reached_processing,
            "Both requests should reach processing state"
        );

        // Cancel the batch - daemon will detect via polling (every 100ms in test)
        manager.cancel_batch(batch.id).await.unwrap();

        // Wait for batch to show canceled status via the count queries.
        // Note: Requests stay in their current state (Processing) but are counted
        // as canceled when cancelling_at is set on the batch.
        let manager_clone = manager.clone();
        let batch_shows_canceled = wait_for(
            || async {
                if let Ok(status) = manager_clone.get_batch_status(batch_id, false).await {
                    // Both requests should be counted as canceled (not in_progress)
                    // when batch has cancelling_at set
                    return status.canceled_requests == 2 && status.in_progress_requests == 0;
                }
                false
            },
            Duration::from_secs(2), // Fast polling (100ms) should detect quickly
        )
        .await;
        assert!(
            batch_shows_canceled,
            "Batch should show 2 canceled requests and 0 in_progress"
        );

        // Shutdown daemon
        shutdown_token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), daemon_handle).await;

        // Drop trigger1 to unblock if still waiting
        drop(trigger1);
    }

    // =========================================================================
    // VIRTUAL FILES & SIZE CALCULATION
    // =========================================================================
    // Tests for lazy size calculation and finalization of virtual files:
    // - Lazy finalization via get_file
    // - Lazy finalization via list_files
    // - Pagination behavior during finalization
    // - Size estimation for incomplete batches
    // - Cached value usage for finalized files
    // - Empty virtual file handling

    #[sqlx::test]
    async fn test_virtual_files_lazy_finalized_via_get_file(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create batch with 3 requests (not 2) so we can have an incomplete batch
        let file_id = manager
            .create_file(
                "lazy-finalize-get-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("req-1".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req-2".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req-3".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/test".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        let output_file_id = batch.output_file_id.unwrap();
        let error_file_id = batch.error_file_id.unwrap();

        // Complete one request, fail one request (leaving one pending = incomplete batch)
        let requests = manager.get_batch_requests(batch.id).await.unwrap();
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = $2,
                response_size = $3,
                completed_at = NOW()
            WHERE id = $1
            "#,
            *requests[0].id() as Uuid,
            r#"{"result":"success"}"#,
            19i64, // Raw body size
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'failed',
                error = $2,
                response_size = $3,
                failed_at = NOW()
            WHERE id = $1
            "#,
            *requests[1].id() as Uuid,
            r#"{"code":"rate_limit","message":"Too many requests"}"#,
            52i64, // Raw error size
        )
        .execute(&pool)
        .await
        .unwrap();

        // Get output file - should calculate size (not finalized yet, batch incomplete)
        let output_file = manager.get_file(output_file_id).await.unwrap();
        assert!(
            !output_file.size_finalized,
            "Output file should not be finalized (batch incomplete)"
        );
        assert!(
            output_file.size_bytes > 0,
            "Output file should have estimated size > 0"
        );

        // Get error file - should calculate size (not finalized yet, batch incomplete)
        let error_file = manager.get_file(error_file_id).await.unwrap();
        assert!(
            !error_file.size_finalized,
            "Error file should not be finalized (batch incomplete)"
        );
        assert!(
            error_file.size_bytes > 0,
            "Error file should have estimated size > 0"
        );

        // Now complete the batch by marking remaining request as completed
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = '{"done":true}',
                response_size = 14,
                completed_at = NOW()
            WHERE id = $1
            "#,
            *requests[2].id() as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        // Get files again - should now finalize
        let output_file_after = manager.get_file(output_file_id).await.unwrap();
        assert!(
            output_file_after.size_finalized,
            "Output file should be finalized after batch complete"
        );

        let error_file_after = manager.get_file(error_file_id).await.unwrap();
        assert!(
            error_file_after.size_finalized,
            "Error file should be finalized after batch complete"
        );
    }

    #[sqlx::test]
    async fn test_virtual_files_lazy_finalized_via_list_files(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create batch
        let file_id = manager
            .create_file(
                "lazy-finalize-list-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                    api_key: "key".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/test".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: Some("user1".to_string()),
            })
            .await
            .unwrap();

        let output_file_id = batch.output_file_id.unwrap();

        // Complete the request
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = '{"ok":true}',
                response_size = 12,
                completed_at = NOW()
            WHERE batch_id = $1
            "#,
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        // List files - should calculate and finalize (batch complete)
        let files = manager
            .list_files(crate::batch::FileFilter {
                purpose: Some(crate::batch::Purpose::BatchOutput.to_string()),
                uploaded_by: Some("user1".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let output_file = files.iter().find(|f| f.id == output_file_id).unwrap();
        assert!(output_file.size_bytes > 0, "Should have calculated size");

        // Give background finalization a moment to complete
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Verify finalization was persisted in DB
        let db_file = sqlx::query!(
            "SELECT size_finalized FROM files WHERE id = $1",
            *output_file_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(db_file.size_finalized);
    }

    #[sqlx::test]
    async fn test_list_files_respects_pagination_only_updates_current_page(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create 3 batches
        let mut output_file_ids = Vec::new();

        for i in 0..3 {
            let file_id = manager
                .create_file(
                    format!("batch-{}", i),
                    None,
                    vec![RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    }],
                )
                .await
                .unwrap();

            let batch = manager
                .create_batch(BatchInput {
                    file_id,
                    endpoint: "/v1/test".to_string(),
                    completion_window: "24h".to_string(),
                    metadata: None,
                    created_by: Some("user1".to_string()),
                })
                .await
                .unwrap();

            output_file_ids.push(batch.output_file_id.unwrap());

            // Complete all batches
            sqlx::query!(
                r#"
                UPDATE requests
                SET state = 'completed',
                    response_status = 200,
                    response_body = '{"done":true}',
                    response_size = 14,
                    completed_at = NOW()
                WHERE batch_id = $1
                "#,
                *batch.id as Uuid,
            )
            .execute(&pool)
            .await
            .unwrap();
        }

        // List first page (limit=2) - should only finalize those 2
        let page1 = manager
            .list_files(crate::batch::FileFilter {
                purpose: Some(crate::batch::Purpose::BatchOutput.to_string()),
                uploaded_by: Some("user1".to_string()),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(page1.len(), 2, "First page should have 2 files");

        // Give background finalization a moment to complete
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Check which files were finalized in DB
        let finalized_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM files
            WHERE id = ANY($1) AND size_finalized = TRUE
            "#,
            &output_file_ids
                .iter()
                .map(|id| **id as Uuid)
                .collect::<Vec<_>>(),
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        // Should have finalized exactly 2 (the ones on page 1)
        assert_eq!(finalized_count, 2, "Only page 1 files should be finalized");

        // List second page - should finalize the remaining one
        let page2 = manager
            .list_files(crate::batch::FileFilter {
                purpose: Some(crate::batch::Purpose::BatchOutput.to_string()),
                uploaded_by: Some("user1".to_string()),
                after: Some(page1.last().unwrap().id),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(page2.len(), 1, "Second page should have 1 file");

        // Give background finalization a moment
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Now all 3 should be finalized
        let all_finalized = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM files
            WHERE id = ANY($1) AND size_finalized = TRUE
            "#,
            &output_file_ids
                .iter()
                .map(|id| **id as Uuid)
                .collect::<Vec<_>>(),
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(all_finalized, 3, "All files should now be finalized");
    }

    #[sqlx::test]
    async fn test_incomplete_batch_gives_estimate_not_finalized(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create batch with 3 requests
        let file_id = manager
            .create_file(
                "incomplete-batch-test".to_string(),
                None,
                (0..3)
                    .map(|_| RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/test".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        let output_file_id = batch.output_file_id.unwrap();
        let error_file_id = batch.error_file_id.unwrap();

        // Complete only 1 out of 3 requests, fail another
        let requests = manager.get_batch_requests(batch.id).await.unwrap();
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = '{"partial":true}',
                response_size = 17,
                completed_at = NOW()
            WHERE id = $1
            "#,
            *requests[0].id() as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'failed',
                error = '{"error":"test"}',
                response_size = 16,
                failed_at = NOW()
            WHERE id = $1
            "#,
            *requests[1].id() as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        // Get files multiple times - should give estimates but not finalize
        for _ in 0..3 {
            let output_file = manager.get_file(output_file_id).await.unwrap();
            assert!(
                !output_file.size_finalized,
                "Output file should NOT be finalized (batch incomplete)"
            );
            assert!(output_file.size_bytes > 0, "Should have non-zero estimate");

            let error_file = manager.get_file(error_file_id).await.unwrap();
            assert!(
                !error_file.size_finalized,
                "Error file should NOT be finalized (batch incomplete)"
            );
            assert!(error_file.size_bytes > 0, "Should have non-zero estimate");
        }

        // Verify nothing was finalized in DB
        let output_db = sqlx::query!(
            "SELECT size_finalized FROM files WHERE id = $1",
            *output_file_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(!output_db.size_finalized);

        let error_db = sqlx::query!(
            "SELECT size_finalized FROM files WHERE id = $1",
            *error_file_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(!error_db.size_finalized);
    }

    #[sqlx::test]
    async fn test_finalized_file_uses_cached_value_no_recomputation(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create and complete a batch
        let file_id = manager
            .create_file(
                "cached-value-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                    api_key: "key".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/test".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        let output_file_id = batch.output_file_id.unwrap();

        // Complete the batch
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = '{"cached":true}',
                response_size = 16,
                completed_at = NOW()
            WHERE batch_id = $1
            "#,
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        // Get file - should finalize
        let file1 = manager.get_file(output_file_id).await.unwrap();
        assert!(file1.size_finalized);
        let finalized_size = file1.size_bytes;

        // Tamper with the response_size in DB to simulate data change
        sqlx::query!(
            r#"
            UPDATE requests
            SET response_size = 999999
            WHERE batch_id = $1
            "#,
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        // Get file again - should use cached value, NOT recalculate from tampered data
        let file2 = manager.get_file(output_file_id).await.unwrap();
        assert!(file2.size_finalized);
        assert_eq!(
            file2.size_bytes, finalized_size,
            "Should use cached finalized value, not recalculate"
        );

        // List files - should also use cached value
        let files = manager
            .list_files(crate::batch::FileFilter {
                purpose: Some(crate::batch::Purpose::BatchOutput.to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let listed_file = files.iter().find(|f| f.id == output_file_id).unwrap();
        assert_eq!(
            listed_file.size_bytes, finalized_size,
            "List should use cached value too"
        );
    }

    #[sqlx::test]
    async fn test_normal_files_finalized_immediately_no_calculation(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create a regular input file (purpose='batch' or NULL)
        let file_id = manager
            .create_file(
                "normal-file-test".to_string(),
                Some("A normal input file".to_string()),
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        // Get the file
        let file = manager.get_file(file_id).await.unwrap();

        // Normal files should be finalized immediately (size_finalized=TRUE on creation)
        assert!(
            file.size_finalized,
            "Normal input files should be finalized immediately"
        );
        assert_eq!(file.size_bytes, 0, "Input files have size 0 by default");

        // Verify in DB
        let db_file = sqlx::query!(
            "SELECT size_finalized, purpose FROM files WHERE id = $1",
            *file_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(db_file.size_finalized, "Should be finalized in DB");
        assert!(
            db_file.purpose.is_none() || db_file.purpose.as_deref() == Some("batch"),
            "Should not be a virtual output/error file"
        );
    }

    // =========================================================================
    // QUEUE PRIORITIZATION TESTS (claim_requests)
    // =========================================================================
    // Tests for SLA-based queue prioritization behavior:
    // - Batches with sooner expires_at prioritized higher
    // - Cancelling batches excluded from queue
    // - Past expires_at batches still claimable (SLA target, not hard deadline)
    // - Uses index: idx_batches_active_by_expiration

    #[sqlx::test]
    async fn test_sla_based_claim_priority(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create three batches with different expiration times
        // IMPORTANT: Create in REVERSE order of SLA priority to test that SLA ordering works
        // (not just relying on FIFO/created_at ordering)

        // Batch 3: No expiration (LOWEST SLA PRIORITY, created FIRST)
        let file3 = manager
            .create_file(
                "no-sla-batch".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("no-sla-1".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{"no_sla":true}"#.to_string(),
                    model: "test".to_string(),
                    api_key: "key".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch3 = manager
            .create_batch(crate::batch::BatchInput {
                file_id: file3,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // batch3.expires_at remains NULL (no SLA)

        // Small delay to ensure different created_at timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Batch 2: Expires in 2 hours (MEDIUM SLA PRIORITY, created SECOND)
        let file2 = manager
            .create_file(
                "medium-batch".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("medium-1".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{"medium":true}"#.to_string(),
                    model: "test".to_string(),
                    api_key: "key".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch2 = manager
            .create_batch(crate::batch::BatchInput {
                file_id: file2,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Set batch2 to expire in 2 hours
        sqlx::query!(
            "UPDATE batches SET expires_at = NOW() + INTERVAL '2 hours' WHERE id = $1",
            *batch2.id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        // Small delay to ensure different created_at timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Batch 1: Expires in 30 minutes (HIGHEST SLA PRIORITY, created LAST)
        let file1 = manager
            .create_file(
                "urgent-batch".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("urgent-1".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{"urgent":true}"#.to_string(),
                    model: "test".to_string(),
                    api_key: "key".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch1 = manager
            .create_batch(crate::batch::BatchInput {
                file_id: file1,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Set batch1 to expire in 30 minutes
        sqlx::query!(
            "UPDATE batches SET expires_at = NOW() + INTERVAL '30 minutes' WHERE id = $1",
            *batch1.id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());

        // Claim 1 request - should get the most urgent one (batch1, 30 min)
        let claimed = manager
            .claim_requests(1, daemon_id)
            .await
            .expect("Failed to claim requests");

        assert_eq!(claimed.len(), 1);
        assert_eq!(
            claimed[0].data.batch_id, batch1.id,
            "First claim should be from most urgent batch (30 min expiration)"
        );
        assert_eq!(claimed[0].data.custom_id, Some("urgent-1".to_string()));

        // Claim another - should get medium priority (batch2, 2 hours)
        let claimed2 = manager
            .claim_requests(1, daemon_id)
            .await
            .expect("Failed to claim requests");

        assert_eq!(claimed2.len(), 1);
        assert_eq!(
            claimed2[0].data.batch_id, batch2.id,
            "Second claim should be from medium priority batch (2 hour expiration)"
        );
        assert_eq!(claimed2[0].data.custom_id, Some("medium-1".to_string()));

        // Claim last one - should get no-SLA batch (batch3)
        let claimed3 = manager
            .claim_requests(1, daemon_id)
            .await
            .expect("Failed to claim requests");

        assert_eq!(claimed3.len(), 1);
        assert_eq!(
            claimed3[0].data.batch_id, batch3.id,
            "Third claim should be from no-SLA batch (NULL expiration)"
        );
        assert_eq!(claimed3[0].data.custom_id, Some("no-sla-1".to_string()));
    }

    #[sqlx::test]
    async fn test_empty_virtual_files_finalized_at_zero(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        // Create batch where all requests fail (output file will be empty)
        let file_id = manager
            .create_file(
                "all-fail-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/test".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        let output_file_id = batch.output_file_id.unwrap();
        let error_file_id = batch.error_file_id.unwrap();

        // Fail all requests (no completions)
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'failed',
                error = '{"error":"all failed"}',
                response_size = 22,
                failed_at = NOW()
            WHERE batch_id = $1
            "#,
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        // Get output file - should finalize at size 0 (no completed requests)
        let output_file = manager.get_file(output_file_id).await.unwrap();
        assert!(
            output_file.size_finalized,
            "Empty output file should be finalized"
        );
        assert_eq!(
            output_file.size_bytes, 0,
            "Output file with no completions should have size 0"
        );

        // Get error file - should finalize with actual error content
        let error_file = manager.get_file(error_file_id).await.unwrap();
        assert!(error_file.size_finalized, "Error file should be finalized");
        assert!(
            error_file.size_bytes > 0,
            "Error file should have size > 0 (2 failed requests)"
        );
    }

    #[sqlx::test]
    async fn test_retry_failed_requests_for_batch(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = Arc::new(PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        ));

        // Create a file with 3 templates
        let file_id = manager
            .create_file(
                "retry-batch-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("req-1".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req-2".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req-3".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                        api_key: "key".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Manually set 2 requests to failed state with daemon metadata, leave 1 as pending
        // This simulates a request that was claimed, started processing, and then failed
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'failed',
                error = 'test error',
                failed_at = NOW(),
                retry_attempt = 3,
                daemon_id = '00000000-0000-0000-0000-000000000001',
                claimed_at = NOW() - INTERVAL '1 hour',
                started_at = NOW() - INTERVAL '30 minutes'
            WHERE batch_id = $1
            AND id IN (
                SELECT id FROM requests WHERE batch_id = $1 ORDER BY created_at LIMIT 2
            )
            "#,
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        // Verify initial state: 2 failed, 1 pending
        let requests_before = manager.get_batch_requests(batch.id).await.unwrap();
        let failed_count = requests_before
            .iter()
            .filter(|r| matches!(r, AnyRequest::Failed(_)))
            .count();
        let pending_count = requests_before
            .iter()
            .filter(|r| matches!(r, AnyRequest::Pending(_)))
            .count();
        assert_eq!(failed_count, 2);
        assert_eq!(pending_count, 1);

        // Call retry_failed_requests_for_batch
        let retried = manager
            .retry_failed_requests_for_batch(batch.id)
            .await
            .unwrap();
        assert_eq!(retried, 2, "Should have retried 2 failed requests");

        // Verify: all 3 requests are now pending
        let requests_after = manager.get_batch_requests(batch.id).await.unwrap();
        let pending_after = requests_after
            .iter()
            .filter(|r| matches!(r, AnyRequest::Pending(_)))
            .count();
        assert_eq!(
            pending_after, 3,
            "All requests should be pending after retry"
        );

        // Verify: retry_attempt is reset to 0
        for req in &requests_after {
            if let AnyRequest::Pending(r) = req {
                assert_eq!(
                    r.state.retry_attempt, 0,
                    "retry_attempt should be reset to 0"
                );
            }
        }

        // Verify: daemon_id, claimed_at, started_at are cleared (matching persist(Pending) behavior)
        let cleared_check = sqlx::query!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM requests
            WHERE batch_id = $1
                AND (daemon_id IS NOT NULL OR claimed_at IS NOT NULL OR started_at IS NOT NULL)
            "#,
            *batch.id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            cleared_check.count, 0,
            "daemon_id, claimed_at, and started_at should all be NULL after retry"
        );

        // Calling again should return 0 (no failed requests)
        let retried_again = manager
            .retry_failed_requests_for_batch(batch.id)
            .await
            .unwrap();
        assert_eq!(retried_again, 0, "No failed requests to retry");
    }
}
