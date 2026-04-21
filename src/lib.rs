//! Batching system for HTTP requests with retry logic and concurrency control.
//!
//! This crate provides 'managers' that accept submitted HTTP requests, and provides an API for
//! checking their status over time. Behind the scenes, a daemon processes these requests in
//! batches, retrying failed requests with exponential backoff and enforcing concurrency limits
//!
//! Batching system with PostgreSQL storage and background daemon for processing requests.

pub mod batch;
pub mod daemon;
pub mod error;
pub mod http;
pub mod manager;
pub mod request;

// Re-export commonly used types
pub use batch::*;
pub use daemon::{Daemon, DaemonConfig, ModelEscalationConfig};
pub use error::{FusilladeError, Result};
pub use http::{HttpClient, HttpResponse, MockHttpClient, ReqwestHttpClient, StreamReassembler};
pub use manager::postgres::{PoolProvider, PostgresRequestManager, TestDbPools};
pub use manager::{DaemonExecutor, RetentionSweepSummary, Storage};
pub use request::*;

/// Internal batch metadata key storing artifact retention TTL in whole seconds.
pub const RETENTION_TTL_METADATA_KEY: &str = "dw_retention_ttl_seconds";

/// Get the fusillade database migrator
///
/// Returns a migrator that can be run against a connection pool.
#[cfg(feature = "postgres")]
pub fn migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate!("./migrations")
}
