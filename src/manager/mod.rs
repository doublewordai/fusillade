//! Daemon runtime traits.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::error::Result;
use crate::http::HttpClient;

pub use fusillade_core::manager::{DaemonStorage, ModelFilter, ModelFilterState, Storage};

/// Daemon executor trait for runtime orchestration.
///
/// This trait handles only the daemon lifecycle: spawning the background
/// worker that processes requests. Durable data operations live on
/// [`Storage`] in `fusillade-core`.
#[async_trait]
pub trait DaemonExecutor<H: HttpClient>: Storage + Send + Sync {
    /// Get a reference to the HTTP client.
    fn http_client(&self) -> &Arc<H>;

    /// Get a reference to the daemon configuration.
    fn config(&self) -> &crate::daemon::DaemonConfig;

    /// Run the daemon thread.
    fn run(
        self: Arc<Self>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Result<JoinHandle<Result<()>>>;
}

#[cfg(feature = "postgres")]
#[async_trait]
impl<P, H> DaemonExecutor<H> for fusillade_arsenal::PostgresRequestManager<P, H>
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

    fn run(
        self: Arc<Self>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        tracing::info!("Starting PostgreSQL request manager daemon");

        let mut daemon = crate::daemon::Daemon::new(
            self.clone(),
            self.http_client().clone(),
            self.config().clone(),
            shutdown_token,
        );
        if let Some(processor) = self.processor().cloned() {
            daemon = daemon.with_processor(processor);
        }
        let daemon = Arc::new(daemon);

        let handle = tokio::spawn(async move { daemon.run().await });

        tracing::info!("Daemon spawned successfully");

        Ok(handle)
    }
}
