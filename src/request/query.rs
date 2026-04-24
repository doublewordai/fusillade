//! Cross-batch request query types and filters.
//!
//! These types support listing and retrieving individual requests across batches,
//! with server-side filtering, pagination, and sorting.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Default number of rows to return when limit is not specified.
const DEFAULT_LIMIT: i64 = 50;

/// Derive the service tier from the batch completion window.
/// "1h" → "flex" (async), everything else → NULL (batch).
pub(crate) fn service_tier_from_completion_window(completion_window: &str) -> Option<&'static str> {
    match completion_window {
        "1h" => Some("flex"),
        _ => None,
    }
}

/// Filter parameters for listing requests across batches.
#[derive(Debug, Clone)]
pub struct ListRequestsFilter {
    /// Filter by batch creator (user ID or org ID)
    pub created_by: Option<String>,
    /// Filter by batch completion window (e.g., "1h", "24h")
    pub completion_window: Option<String>,
    /// Filter by request state (pending, claimed, processing, completed, failed, canceled)
    pub status: Option<String>,
    /// Filter by model(s) — when multiple, matches any.
    /// `None` disables model filtering. `Some(vec![])` matches no rows.
    pub models: Option<Vec<String>>,
    /// Only return requests created after this timestamp
    pub created_after: Option<DateTime<Utc>>,
    /// Only return requests created before this timestamp
    pub created_before: Option<DateTime<Utc>>,
    /// Filter by service tier ("auto", "default", "flex", "priority").
    /// Partial indexes accelerate the `"flex"` case for both
    /// `active_first` orderings; other tiers fall back to the full index.
    /// Batch-tier requests have NULL service_tier and are not filterable
    /// by this field (use `completion_window` instead).
    pub service_tier: Option<String>,
    /// Sort active requests (pending/claimed/processing) first
    pub active_first: bool,
    /// Number of rows to skip (offset pagination)
    pub skip: i64,
    /// Maximum number of rows to return (defaults to 50)
    pub limit: i64,
}

impl Default for ListRequestsFilter {
    fn default() -> Self {
        Self {
            created_by: None,
            completion_window: None,
            status: None,
            models: None,
            created_after: None,
            created_before: None,
            service_tier: None,
            active_first: false,
            skip: 0,
            limit: DEFAULT_LIMIT,
        }
    }
}

/// Summary of an individual request, suitable for list views.
///
/// Note: This type does not include user email or token/cost metrics.
/// Callers should enrich with data from their own tables (users, analytics).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
pub struct RequestSummary {
    pub id: Uuid,
    pub batch_id: Option<Uuid>,
    pub model: String,
    #[cfg_attr(feature = "postgres", sqlx(rename = "state"))]
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<f64>,
    pub response_status: Option<i16>,
    pub service_tier: Option<String>,
    /// Batch or daemon creator ID — for ownership checks and email lookup.
    /// NULL for daemon-managed requests that don't have a batch.
    pub batch_created_by: Option<String>,
}

/// Internal row shape used previously when `list_requests` computed the total
/// count via a `COUNT(*) OVER()` window function. Kept for backward
/// compatibility with the public re-export introduced in v16.1.0; no longer
/// constructed by this crate.
#[allow(deprecated)]
mod deprecated_types {
    use super::{DateTime, Deserialize, RequestSummary, Serialize, Utc, Uuid};

    #[deprecated(
        since = "16.1.1",
        note = "no longer used internally; use RequestSummary and RequestListResult.total_count"
    )]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
    pub struct RequestSummaryWithCount {
        pub id: Uuid,
        pub batch_id: Uuid,
        pub model: String,
        #[cfg_attr(feature = "postgres", sqlx(rename = "state"))]
        pub status: String,
        pub created_at: DateTime<Utc>,
        pub completed_at: Option<DateTime<Utc>>,
        pub failed_at: Option<DateTime<Utc>>,
        pub duration_ms: Option<f64>,
        pub response_status: Option<i16>,
        pub service_tier: Option<String>,
        pub batch_created_by: String,
        pub total_count: i64,
    }

    impl From<RequestSummaryWithCount> for RequestSummary {
        fn from(r: RequestSummaryWithCount) -> Self {
            Self {
                id: r.id,
                batch_id: Some(r.batch_id),
                model: r.model,
                status: r.status,
                created_at: r.created_at,
                completed_at: r.completed_at,
                failed_at: r.failed_at,
                duration_ms: r.duration_ms,
                response_status: r.response_status,
                service_tier: r.service_tier,
                batch_created_by: Some(r.batch_created_by),
            }
        }
    }
}

#[allow(deprecated)]
pub use deprecated_types::RequestSummaryWithCount;

/// Full detail of an individual request, including body and response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
pub struct RequestDetail {
    pub id: Uuid,
    /// `None` for daemon-managed requests (created via `create_daemon_request`).
    pub batch_id: Option<Uuid>,
    pub model: String,
    #[cfg_attr(feature = "postgres", sqlx(rename = "state"))]
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<f64>,
    pub response_status: Option<i16>,
    /// `None` when the template has been purged (via file deletion + orphan purge).
    pub body: Option<String>,
    pub response_body: Option<String>,
    pub error: Option<String>,
    /// `None` for daemon-managed requests (no batch).
    pub completion_window: Option<String>,
    pub service_tier: Option<String>,
    /// `None` for daemon-managed requests (no batch).
    pub batch_created_by: Option<String>,
}

/// Input for creating a daemon-managed request (no batch).
///
/// Used when a daemon (e.g., an AI proxy) is already processing a request and
/// wants to track it in fusillade without creating a batch. The request is
/// created directly in "processing" state with the specified daemon ID.
#[derive(Debug, Clone)]
pub struct CreateDaemonRequestInput {
    /// Pre-generated request ID. If `None`, a new UUID is generated.
    pub id: Option<Uuid>,
    /// The request body as a JSON string.
    pub body: String,
    /// Model identifier.
    pub model: String,
    /// Base URL of the target endpoint (e.g., "http://localhost:3001/ai").
    pub endpoint: String,
    /// HTTP method (e.g., "POST").
    pub method: String,
    /// API path (e.g., "/v1/responses").
    pub path: String,
    /// API key for the request (empty string if none).
    pub api_key: String,
    /// The daemon that is processing this request.
    pub daemon_id: super::DaemonId,
}

/// Result of a paginated request list query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestListResult {
    pub data: Vec<RequestSummary>,
    /// Best-effort total row count for the full query result.
    ///
    /// Returns an exact count when the count query completes within a short
    /// internal timeout; otherwise falls back to a query-planner row estimate.
    /// Planner estimates are typically within a few percent when table
    /// statistics are current, but may diverge more if stats are stale.
    pub total_count: i64,
}
