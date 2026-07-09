//! Daemon runtime traits.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::error::Result;
use crate::http::HttpClient;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
pub use postgres::PostgresRequestManager;

pub use fusillade_core::manager::{DaemonStorage, ModelFilter, ModelFilterState, Storage};

/// Daemon executor trait for runtime orchestration.
///
/// This trait handles only the daemon lifecycle: spawning the background
/// worker that processes requests. Durable data operations live on
/// [`Storage`] in `fusillade-core`.
#[async_trait]
pub trait DaemonExecutor<H: HttpClient>: Send + Sync {
    /// Get a reference to the HTTP client.
    fn http_client(&self) -> &Arc<H>;

    /// Get a reference to the daemon configuration.
    fn config(&self) -> &crate::daemon::DaemonConfig;

    /// Run the daemon thread.
    fn run(
        self: Arc<Self>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        let mode = self.config().mode;
        self.run_with_mode(shutdown_token, mode)
    }

    /// Run the daemon thread with an explicit mode.
    fn run_with_mode(
        self: Arc<Self>,
        shutdown_token: tokio_util::sync::CancellationToken,
        mode: crate::daemon::DaemonMode,
    ) -> Result<JoinHandle<Result<()>>>;
}

#[cfg(feature = "postgres")]
#[async_trait]
impl<P, H> DaemonExecutor<H> for PostgresRequestManager<P, H>
where
    P: fusillade_arsenal::PoolProvider,
    H: HttpClient + 'static,
{
    fn http_client(&self) -> &Arc<H> {
        self.http_client()
    }

    fn config(&self) -> &crate::daemon::DaemonConfig {
        self.config()
    }

    fn run_with_mode(
        self: Arc<Self>,
        shutdown_token: tokio_util::sync::CancellationToken,
        mode: crate::daemon::DaemonMode,
    ) -> Result<JoinHandle<Result<()>>> {
        tracing::info!(?mode, "Starting PostgreSQL request manager daemon");

        let mut daemon = crate::daemon::Daemon::new(
            self.storage().clone(),
            self.http_client().clone(),
            self.config().clone(),
            shutdown_token,
        );
        if let Some(processor) = self.processor().cloned() {
            daemon = daemon.with_processor(processor);
        }
        let daemon = Arc::new(daemon);

        let handle = tokio::spawn(async move { daemon.run_with_mode(mode).await });

        tracing::info!("Daemon spawned successfully");

        Ok(handle)
    }
}
