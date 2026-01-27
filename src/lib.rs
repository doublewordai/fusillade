//! Batching system for HTTP requests with retry logic and concurrency control.
//!
//! This crate provides 'managers' that accept submitted HTTP requests, and provides an API for
//! checking their status over time. Behind the scenes, a daemon processes these requests in
//! batches, retrying failed requests with exponential backoff and enforcing concurrency limits
//!
//! Batching system with PostgreSQL storage and background daemon for processing requests.

pub mod batch;
pub mod daemon;
pub mod domain;
pub mod error;
pub mod http;
pub mod manager;
pub mod request;
pub mod storage;

// Re-export commonly used types
pub use batch::*;
pub use daemon::{Daemon, DaemonConfig, ModelEscalationConfig, SlaThreshold};
pub use error::{FusilladeError, Result};
pub use http::{HttpClient, HttpResponse, MockHttpClient, ReqwestHttpClient};
pub use request::*;
pub use storage::postgres::{PoolProvider, PostgresRequestManager, TestDbPools};
pub use storage::{DaemonExecutor, DaemonStorage, Storage};

/// Get the fusillade database migrator
///
/// Returns a migrator that can be run against a connection pool.
#[cfg(feature = "postgres")]
pub fn migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate!("./migrations")
}
