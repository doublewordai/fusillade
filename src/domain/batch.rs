//! Batch types for grouping and executing requests.
//!
//! A batch represents one execution of all templates in a file.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::file::FileId;

/// Unique identifier for a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct BatchId(pub Uuid);

impl From<Uuid> for BatchId {
    fn from(uuid: Uuid) -> Self {
        BatchId(uuid)
    }
}

impl std::ops::Deref for BatchId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for BatchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0.to_string()[..8])
    }
}

/// Input parameters for creating a new batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchInput {
    /// The file containing request templates
    pub file_id: FileId,
    /// The API endpoint to use for all requests (e.g., "/v1/chat/completions")
    pub endpoint: String,
    /// Completion window (e.g., "24h")
    pub completion_window: String,
    /// Optional metadata key-value pairs (OpenAI allows up to 16 pairs)
    pub metadata: Option<serde_json::Value>,
    /// User who created this batch (for ownership tracking)
    pub created_by: Option<String>,
}

/// A batch represents one execution of all of a file's templates.
#[derive(Debug, Clone, Serialize)]
pub struct Batch {
    pub id: BatchId,
    pub file_id: Option<FileId>,
    pub created_at: DateTime<Utc>,
    /// Metadata key-value pairs (OpenAI allows up to 16 pairs)
    pub metadata: Option<serde_json::Value>,
    /// Completion window (e.g., "24h")
    pub completion_window: String,
    /// The API endpoint to use for all requests (e.g., "/v1/chat/completions")
    pub endpoint: String,
    /// File ID containing the successful results
    pub output_file_id: Option<FileId>,
    /// File ID containing the error results
    pub error_file_id: Option<FileId>,
    /// User who created this batch
    pub created_by: Option<String>,
    /// When the batch will expire (created_at + completion_window)
    /// This is required for queue prioritization and SLA monitoring
    pub expires_at: DateTime<Utc>,
    /// When batch cancellation was initiated
    pub cancelling_at: Option<DateTime<Utc>>,
    /// Batch-level errors (validation errors, system errors, etc.)
    pub errors: Option<serde_json::Value>,

    /// Status fields
    pub total_requests: i64,
    pub pending_requests: i64,
    pub in_progress_requests: i64,
    pub completed_requests: i64,
    pub failed_requests: i64,
    pub canceled_requests: i64,
    pub requests_started_at: Option<DateTime<Utc>>,

    /// Terminal state timestamps (set once when batch enters that state)
    pub finalizing_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
}

/// Status information for a batch, computed from its executions.
#[derive(Debug, Clone, Serialize)]
pub struct BatchStatus {
    pub batch_id: BatchId,
    pub file_id: Option<FileId>,
    pub file_name: Option<String>,
    pub total_requests: i64,
    pub pending_requests: i64,
    pub in_progress_requests: i64,
    pub completed_requests: i64,
    pub failed_requests: i64,
    pub canceled_requests: i64,
    pub started_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl BatchStatus {
    /// Check if the batch has finished (all requests in terminal state).
    pub fn is_finished(&self) -> bool {
        self.completed_requests + self.failed_requests + self.canceled_requests
            == self.total_requests
    }

    /// Check if the batch is still running.
    pub fn is_running(&self) -> bool {
        !self.is_finished()
    }

    /// Get OpenAI-compatible status string.
    /// Maps internal state to OpenAI's status values:
    /// - "validating" - batch just created, no requests started yet
    /// - "in_progress" - batch is being processed
    /// - "finalizing" - nearly all requests done (95%+ complete)
    /// - "completed" - all requests in terminal state and at least one succeeded
    /// - "failed" - all requests in terminal state and all failed
    /// - "cancelled" - all requests cancelled
    pub fn openai_status(&self) -> &'static str {
        if self.total_requests == 0 {
            return "validating";
        }

        let terminal_count =
            self.completed_requests + self.failed_requests + self.canceled_requests;

        if terminal_count == 0 {
            // Nothing has started yet
            "validating"
        } else if terminal_count == self.total_requests {
            // All done - determine outcome
            if self.canceled_requests == self.total_requests {
                "cancelled"
            } else if self.completed_requests == 0 {
                "failed"
            } else {
                "completed"
            }
        } else if terminal_count as f64 / self.total_requests as f64 >= 0.95 {
            // Nearly done (95%+)
            "finalizing"
        } else {
            // In progress
            "in_progress"
        }
    }
}

/// Status of a batch result item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchResultStatus {
    /// Request is pending execution
    Pending,
    /// Request is currently being processed (claimed or processing)
    InProgress,
    /// Request completed successfully
    Completed,
    /// Request failed with an error
    Failed,
}

/// Merged batch result item combining input, output, and status.
/// Used for the Results view to show input/output pairs in a single row.
#[derive(Debug, Clone, Serialize)]
pub struct BatchResultItem {
    /// Fusillade request ID (unique identifier)
    pub id: String,
    /// User-provided identifier (NOT unique - may be duplicated)
    pub custom_id: Option<String>,
    /// Model used for this request
    pub model: String,
    /// Original request body from the input template
    pub input_body: serde_json::Value,
    /// Full response object (choices, usage, etc.) for completed requests
    pub response_body: Option<serde_json::Value>,
    /// Error message for failed requests
    pub error: Option<String>,
    /// Current status of the request
    pub status: BatchResultStatus,
}

/// Batch output item - represents a completed request in OpenAI format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOutputItem {
    /// Request ID
    pub id: String,
    /// Custom ID from the original request
    pub custom_id: Option<String>,
    /// Response details
    pub response: BatchResponseDetails,
    /// Error (should be null for successful responses)
    pub error: Option<serde_json::Value>,
}

/// Batch error item - represents a failed request in OpenAI format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchErrorItem {
    /// Request ID
    pub id: String,
    /// Custom ID from the original request
    pub custom_id: Option<String>,
    /// Response (should be null for errors)
    pub response: Option<serde_json::Value>,
    /// Error details
    pub error: BatchErrorDetails,
}

/// Response details for a batch output item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResponseDetails {
    /// HTTP status code
    pub status_code: i16,
    /// Request ID from the upstream API
    pub request_id: Option<String>,
    /// Response body (e.g., ChatCompletion, Embedding, etc.)
    pub body: serde_json::Value,
}

/// Error details for a batch error item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchErrorDetails {
    /// Error code
    pub code: Option<String>,
    /// Error message
    pub message: String,
}

/// Enum for different types of file content that can be streamed.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum FileContentItem {
    /// Request template (for input files with purpose='batch')
    Template(super::file::RequestTemplateInput),
    /// Batch output (for output files with purpose='batch_output')
    Output(BatchOutputItem),
    /// Batch error (for error files with purpose='batch_error')
    Error(BatchErrorItem),
}
