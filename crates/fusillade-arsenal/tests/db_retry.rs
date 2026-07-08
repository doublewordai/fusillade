use std::time::Duration;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::anyhow;
use fusillade_arsenal::{
    DbRetryConfig, PoolProvider, PostgresRequestManager, retry_transient_db_errors,
};
use fusillade_core::FusilladeError;
use sqlx::postgres::{PgPool, PgPoolOptions};

#[derive(Clone)]
struct LazyPools(PgPool);

impl PoolProvider for LazyPools {
    fn read(&self) -> &PgPool {
        &self.0
    }

    fn write(&self) -> &PgPool {
        &self.0
    }
}

#[test]
fn fixed_retry_config_expands_to_cadence() {
    let config = DbRetryConfig::fixed(3, Duration::from_millis(25));

    assert_eq!(
        config.retry_delays(),
        &[
            Duration::from_millis(25),
            Duration::from_millis(25),
            Duration::from_millis(25),
        ]
    );
}

#[test]
fn custom_retry_config_preserves_cadence() {
    let cadence = vec![
        Duration::from_millis(5),
        Duration::from_millis(25),
        Duration::from_millis(100),
    ];

    let config = DbRetryConfig::new(cadence.clone());

    assert_eq!(config.retry_delays(), cadence.as_slice());
}

#[tokio::test]
async fn retries_pool_timeout_errors_until_success() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let config = DbRetryConfig::fixed(2, Duration::ZERO);

    let result = retry_transient_db_errors(&config, {
        let attempts = Arc::clone(&attempts);
        move || {
            let attempts = Arc::clone(&attempts);
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    Err(FusilladeError::Other(anyhow!(
                        "pool timed out while waiting for an open connection"
                    )))
                } else {
                    Ok("connected")
                }
            }
        }
    })
    .await;

    assert_eq!(result.unwrap(), "connected");
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn does_not_retry_non_transient_errors() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let config = DbRetryConfig::fixed(3, Duration::ZERO);

    let result: Result<(), FusilladeError> = retry_transient_db_errors(&config, {
        let attempts = Arc::clone(&attempts);
        move || {
            let attempts = Arc::clone(&attempts);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(FusilladeError::ValidationError("bad input".to_string()))
            }
        }
    })
    .await;

    assert!(matches!(result, Err(FusilladeError::ValidationError(_))));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn postgres_manager_stores_configured_db_retry_cadence() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://postgres:password@localhost/fusillade")
        .unwrap();
    let config = DbRetryConfig::new(vec![Duration::from_millis(1), Duration::from_millis(2)]);

    let manager = PostgresRequestManager::new(LazyPools(pool), Default::default())
        .with_db_retry_config(config.clone());

    assert_eq!(manager.db_retry_config(), &config);
}
