//! PostgreSQL storage for the Fusillade scheduling daemon.

use std::future::Future;
use std::time::Duration;

use fusillade_core::FusilladeError;

mod db;
pub mod postgres;
#[path = "response_step.rs"]
pub mod postgres_response_step;
mod utils;

pub use fusillade_core::manager::{DaemonStorage, ModelFilter, ModelFilterState, Storage};
pub use fusillade_core::request::AnyRequest;
pub use fusillade_core::response_step;
pub use postgres::{BatchInsertStrategy, PoolProvider, PostgresRequestManager, TestDbPools};
pub use postgres_response_step::PostgresResponseStepManager;

pub mod batch {
    pub use fusillade_core::batch::*;
}

pub mod daemon {
    pub use fusillade_core::daemon::*;
}

pub mod error {
    pub use fusillade_core::error::*;
}

pub mod http {
    pub use fusillade_core::http::*;
}

pub mod manager {
    pub use fusillade_core::manager::*;
}

pub mod processor {
    pub use fusillade_core::processor::*;
}

pub mod request {
    pub use fusillade_core::request::*;
}

pub mod transform {
    pub use fusillade_core::transform::*;
}

/// Retry cadence for transient database failures.
///
/// Each entry is the delay before the next retry. An empty cadence disables
/// retries and preserves the first error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbRetryConfig {
    retry_delays: Vec<Duration>,
}

impl DbRetryConfig {
    pub fn new(retry_delays: Vec<Duration>) -> Self {
        Self { retry_delays }
    }

    pub fn fixed(retries: usize, delay: Duration) -> Self {
        Self {
            retry_delays: vec![delay; retries],
        }
    }

    pub fn disabled() -> Self {
        Self::new(Vec::new())
    }

    pub fn retry_delays(&self) -> &[Duration] {
        &self.retry_delays
    }
}

impl Default for DbRetryConfig {
    fn default() -> Self {
        Self::fixed(3, Duration::from_millis(50))
    }
}

pub async fn retry_transient_db_errors<T, Op, Fut>(
    config: &DbRetryConfig,
    mut operation: Op,
) -> fusillade_core::Result<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = fusillade_core::Result<T>>,
{
    for delay in config.retry_delays() {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if is_retryable_db_error(&error) => {
                if !delay.is_zero() {
                    tokio::time::sleep(*delay).await;
                }
            }
            Err(error) => return Err(error),
        }
    }

    operation().await
}

pub fn is_retryable_db_error(error: &FusilladeError) -> bool {
    is_retryable_db_error_message(&error.to_string())
}

pub fn is_retryable_db_error_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("pool timed out while waiting for an open connection")
        || message.contains("pooltimedout")
        || message.contains("connection pool timed out")
}

/// Fusillade Arsenal database migrator.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Get the Fusillade Arsenal database migrator.
///
/// Returns a migrator that can be run against a PostgreSQL pool.
pub fn migrator() -> &'static sqlx::migrate::Migrator {
    &MIGRATOR
}
