//! File and request template types.
//!
//! Files are collections of request templates that can be executed as batches.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

/// Unique identifier for a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FileId(pub Uuid);

impl From<Uuid> for FileId {
    fn from(uuid: Uuid) -> Self {
        FileId(uuid)
    }
}

impl std::ops::Deref for FileId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for FileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0.to_string()[..8])
    }
}

/// Unique identifier for a request template.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct TemplateId(pub Uuid);

impl From<Uuid> for TemplateId {
    fn from(uuid: Uuid) -> Self {
        TemplateId(uuid)
    }
}

impl std::ops::Deref for TemplateId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for TemplateId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0.to_string()[..8])
    }
}

/// Purpose for which a file was created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    /// File contains batch API request templates
    Batch,
    /// Virtual file that streams batch output (completed requests)
    BatchOutput,
    /// Virtual file that streams batch errors (failed requests)
    BatchError,
}

/// Type of batch output file for lookup purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFileType {
    /// Output file containing completed requests
    Output,
    /// Error file containing failed requests
    Error,
}

impl fmt::Display for Purpose {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Purpose::Batch => write!(f, "batch"),
            Purpose::BatchOutput => write!(f, "batch_output"),
            Purpose::BatchError => write!(f, "batch_error"),
        }
    }
}

impl FromStr for Purpose {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "batch" => Ok(Purpose::Batch),
            "batch_output" => Ok(Purpose::BatchOutput),
            "batch_error" => Ok(Purpose::BatchError),
            _ => Err(format!("Invalid purpose: {}", s)),
        }
    }
}

/// File status tracking lifecycle and health.
///
/// Status tracks the file's lifecycle:
/// - `Processed`: Successfully uploaded and parsed into templates (users can access)
/// - `Error`: Failed to process during upload (only visible to admins with SystemAccess)
/// - `Deleted`: Soft-deleted by user (metadata retained for audit, only visible to admins)
/// - `Expired`: Past its expiration date (metadata retained for audit, only visible to admins)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileStatus {
    /// File was successfully processed and templates created
    Processed,
    /// File processing failed (see error_message for details)
    Error,
    /// File was soft-deleted by user (metadata retained for audit)
    Deleted,
    /// File has passed its expiration date
    Expired,
}

impl fmt::Display for FileStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileStatus::Processed => write!(f, "processed"),
            FileStatus::Error => write!(f, "error"),
            FileStatus::Deleted => write!(f, "deleted"),
            FileStatus::Expired => write!(f, "expired"),
        }
    }
}

impl FromStr for FileStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "processed" => Ok(FileStatus::Processed),
            "error" => Ok(FileStatus::Error),
            "deleted" => Ok(FileStatus::Deleted),
            "expired" => Ok(FileStatus::Expired),
            _ => Err(format!("Invalid file status: {}", s)),
        }
    }
}

/// A file containing a collection of request templates.
#[derive(Debug, Clone, Serialize)]
pub struct File {
    pub id: FileId,
    pub name: String,
    pub description: Option<String>,
    pub size_bytes: i64,
    pub status: FileStatus,
    pub error_message: Option<String>,
    pub purpose: Option<Purpose>,
    pub expires_at: Option<DateTime<Utc>>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub uploaded_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub size_finalized: bool,
}

/// A request template defining how to make a request.
///
/// Templates are mutable, but requests snapshot the template state
/// at execution time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RequestTemplate {
    pub id: TemplateId,
    pub file_id: FileId,
    pub custom_id: Option<String>, // OpenAI Batch API custom identifier
    pub endpoint: String,
    pub method: String,
    pub path: String,
    pub body: String,
    pub model: String,
    pub api_key: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Input for creating a new request template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestTemplateInput {
    pub custom_id: Option<String>, // OpenAI Batch API custom identifier
    pub endpoint: String,
    pub method: String,
    pub path: String,
    pub body: String,
    pub model: String,
    pub api_key: String,
}

/// Metadata for creating a file from a stream
#[derive(Debug, Clone, Default, Serialize)]
pub struct FileMetadata {
    pub filename: Option<String>,
    pub description: Option<String>,
    pub purpose: Option<String>,
    pub expires_after_anchor: Option<String>,
    pub expires_after_seconds: Option<i64>,
    pub size_bytes: Option<i64>,
    pub uploaded_by: Option<String>,
}

/// Filter parameters for listing files
#[derive(Debug, Clone, Default)]
pub struct FileFilter {
    /// Filter by user who uploaded the file
    /// TODO: We use a string here, because this crate is decoupled from the dwctl one which uses a
    /// UUID. Is this fine? This just needs to be a unique identifier per user.
    pub uploaded_by: Option<String>,
    /// Filter by file status (processed, error, deleted, expired)
    pub status: Option<String>,
    /// Filter by purpose
    pub purpose: Option<String>,
    /// Search query for filename (case-insensitive substring match)
    pub search: Option<String>,
    /// Cursor for pagination (file ID to start after)
    pub after: Option<FileId>,
    /// Maximum number of results to return
    pub limit: Option<usize>,
    /// Sort order (true = ascending, false = descending)
    pub ascending: bool,
}

/// Items that can be yielded from a file upload stream
#[derive(Debug, Clone, Serialize)]
pub enum FileStreamItem {
    /// File metadata (should be first item in stream)
    Metadata(FileMetadata),
    /// A request template parsed from JSONL
    Template(RequestTemplateInput),
    /// An error occurred during parsing
    Error(String),
}

/// Aggregated statistics for a model's templates in a file.
/// Used for efficient cost estimation without streaming all template data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelTemplateStats {
    /// The model name
    pub model: String,
    /// Number of request templates using this model
    pub request_count: i64,
    /// Total size of all request bodies in bytes
    pub total_body_bytes: i64,
}
