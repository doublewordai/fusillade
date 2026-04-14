//! Cross-batch request query types and filters.
//!
//! These types support listing and retrieving individual requests across batches,
//! with server-side filtering, pagination, and sorting.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Filter parameters for listing requests across batches.
#[derive(Debug, Clone, Default)]
pub struct ListRequestsFilter {
    /// Filter by batch creator (user ID or org ID)
    pub created_by: Option<String>,
    /// Filter by batch completion window (e.g., "1h", "24h")
    pub completion_window: Option<String>,
    /// Filter by request state (pending, processing, completed, failed, canceled)
    pub status: Option<String>,
    /// Filter by model(s) — when multiple, matches any
    pub models: Option<Vec<String>>,
    /// Only return requests created after this timestamp
    pub created_after: Option<DateTime<Utc>>,
    /// Only return requests created before this timestamp
    pub created_before: Option<DateTime<Utc>>,
    /// Sort active requests (pending/claimed/processing) first
    pub active_first: bool,
    /// Number of rows to skip (offset pagination)
    pub skip: i64,
    /// Maximum number of rows to return
    pub limit: i64,
}

/// Summary of an individual request, suitable for list views.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
pub struct RequestSummary {
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
    pub created_by_email: Option<String>,
}

/// Full detail of an individual request, including body and response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
pub struct RequestDetail {
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
    pub body: String,
    pub response_body: Option<String>,
    pub error: Option<String>,
    pub completion_window: String,
    pub batch_created_by: String,
}

/// Result of a paginated request list query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestListResult {
    pub data: Vec<RequestSummary>,
    pub total_count: i64,
}
