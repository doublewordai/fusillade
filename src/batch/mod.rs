//! File and batch types for grouping requests.
//!
//! This module re-exports types from `domain::file` and `domain::batch`.
//! See those modules for the actual implementations.

// Re-export all file types
pub use crate::domain::file::{
    File, FileFilter, FileId, FileMetadata, FileStatus, FileStreamItem, ModelTemplateStats,
    OutputFileType, Purpose, RequestTemplate, RequestTemplateInput, TemplateId,
};

// Re-export all batch types
pub use crate::domain::batch::{
    Batch, BatchErrorDetails, BatchErrorItem, BatchId, BatchInput, BatchOutputItem,
    BatchResponseDetails, BatchResultItem, BatchResultStatus, BatchStatus, FileContentItem,
};
