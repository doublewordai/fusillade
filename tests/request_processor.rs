//! Integration tests for the [`RequestProcessor`] daemon hook.
//!
//! Covers:
//! - The default processor preserves today's batch path (a request claimed by
//!   the daemon completes via the same mock HTTP fixture that drove the
//!   pre-hook integration tests).
//! - A custom processor is invoked exactly once per claimed request, with
//!   the daemon's storage/http/cancellation parameters threaded through.
//!
//! These tests guard against accidental regressions in the spawn-task wiring.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use fusillade::TestDbPools;
use fusillade::batch::{BatchInput, RequestTemplateInput};
use fusillade::daemon::{DaemonConfig, default_should_retry};
use fusillade::http::{HttpClient, HttpResponse, MockHttpClient};
use fusillade::manager::postgres::PostgresRequestManager;
use fusillade::manager::{DaemonExecutor, Storage};
use fusillade::processor::{
    CancellationFuture, DefaultRequestProcessor, RequestProcessor, ShouldRetry,
};
use fusillade::request::{Claimed, Completed, Failed, Request, RequestCompletionResult};
use tokio_util::sync::CancellationToken;

fn fast_test_config() -> DaemonConfig {
    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("test-model".to_string(), 10);
    DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        model_concurrency_limits,
        max_retries: Some(3),
        backoff_ms: 100,
        backoff_factor: 2,
        max_backoff_ms: 1000,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100,
        ..Default::default()
    }
}

async fn submit_one_request(
    manager: &Arc<PostgresRequestManager<TestDbPools, MockHttpClient>>,
) -> fusillade::request::RequestId {
    let template = RequestTemplateInput {
        custom_id: None,
        endpoint: "http://upstream".into(),
        method: "POST".into(),
        path: "/v1/test".into(),
        body: r#"{"hello":"world"}"#.into(),
        model: "test-model".into(),
        api_key: "test-key".into(),
    };
    let file_id = manager
        .create_file("test_file".into(), None, vec![template])
        .await
        .unwrap();
    let batch = manager
        .create_batch(BatchInput {
            file_id,
            endpoint: "/v1/test".into(),
            completion_window: "24h".into(),
            metadata: None,
            created_by: Some("tester".into()),
            api_key_id: None,
            api_key: None,
            total_requests: None,
        })
        .await
        .unwrap();
    let requests = manager.get_batch_requests(batch.id).await.unwrap();
    requests[0].id()
}

async fn wait_until_completed(
    manager: &Arc<PostgresRequestManager<TestDbPools, MockHttpClient>>,
    request_id: fusillade::request::RequestId,
) {
    let start = std::time::Instant::now();
    loop {
        let detail = manager.get_request_detail(request_id).await.unwrap();
        if detail.status == "completed" || detail.status == "failed" {
            return;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!(
                "Timeout waiting for request to reach terminal state, last={:?}",
                detail.status
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[sqlx::test]
#[test_log::test]
async fn default_processor_preserves_batch_path(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());
    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"ok":true}"#.into(),
        }),
    );

    let shutdown_token = CancellationToken::new();
    let manager = Arc::new(
        PostgresRequestManager::with_client(
            TestDbPools::new(pool).await.unwrap(),
            http_client.clone(),
        )
        .with_config(fast_test_config()),
    );
    let _handle = manager.clone().run(shutdown_token.clone()).unwrap();

    let request_id = submit_one_request(&manager).await;
    wait_until_completed(&manager, request_id).await;

    let detail = manager.get_request_detail(request_id).await.unwrap();
    assert_eq!(detail.status, "completed");
    let body = detail.response_body.unwrap_or_default();
    assert!(body.contains("\"ok\":true"));

    shutdown_token.cancel();
}

/// Counting processor: wraps DefaultRequestProcessor and records how many
/// times it was invoked. Validates that the daemon's spawn task actually
/// dispatches through the trait object.
struct CountingProcessor {
    invocations: Arc<AtomicUsize>,
    inner: DefaultRequestProcessor,
}

#[async_trait]
impl<S, H> RequestProcessor<S, H> for CountingProcessor
where
    S: Storage + Sync,
    H: HttpClient + 'static,
{
    async fn process(
        &self,
        request: Request<Claimed>,
        http: H,
        storage: &S,
        should_retry: ShouldRetry,
        cancellation: CancellationFuture,
    ) -> fusillade::Result<RequestCompletionResult> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        self.inner
            .process(request, http, storage, should_retry, cancellation)
            .await
    }
}

#[sqlx::test]
#[test_log::test]
async fn custom_processor_is_invoked(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());
    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: "{}".into(),
        }),
    );

    let invocations = Arc::new(AtomicUsize::new(0));
    let processor = Arc::new(CountingProcessor {
        invocations: invocations.clone(),
        inner: DefaultRequestProcessor,
    });

    let shutdown_token = CancellationToken::new();
    let manager = Arc::new(
        PostgresRequestManager::with_client(
            TestDbPools::new(pool).await.unwrap(),
            http_client.clone(),
        )
        .with_config(fast_test_config())
        .with_processor(processor),
    );
    let _handle = manager.clone().run(shutdown_token.clone()).unwrap();

    let request_id = submit_one_request(&manager).await;
    wait_until_completed(&manager, request_id).await;

    assert_eq!(
        invocations.load(Ordering::SeqCst),
        1,
        "processor must be invoked exactly once for a single claimed request"
    );
    let detail = manager.get_request_detail(request_id).await.unwrap();
    assert_eq!(detail.status, "completed");

    shutdown_token.cancel();
}

/// Synthesizes a hard failure: validates that the processor's terminal-state
/// outcome is honored by the daemon's downstream metric/retry logic.
struct AlwaysFailProcessor;

#[async_trait]
impl<S, H> RequestProcessor<S, H> for AlwaysFailProcessor
where
    S: Storage + Sync,
    H: HttpClient + 'static,
{
    async fn process(
        &self,
        request: Request<Claimed>,
        _http: H,
        storage: &S,
        _should_retry: ShouldRetry,
        _cancellation: CancellationFuture,
    ) -> fusillade::Result<RequestCompletionResult> {
        // Synthesize a non-retriable failure so the daemon persists the
        // terminal state and we can observe it via the manager.
        let model = request.data.model.clone();
        let retry_attempt = request.state.retry_attempt;
        let batch_expires_at = request.state.batch_expires_at;
        let failed = Request {
            data: request.data,
            state: Failed {
                reason: fusillade::request::FailureReason::NonRetriableHttpStatus {
                    status: 400,
                    body: "synthetic test failure".into(),
                },
                failed_at: chrono::Utc::now(),
                retry_attempt,
                batch_expires_at,
                routed_model: model,
            },
        };
        storage.persist(&failed).await?;
        Ok(RequestCompletionResult::Failed(failed))
    }
}

#[sqlx::test]
#[test_log::test]
async fn custom_processor_can_synthesize_terminal_failure(pool: sqlx::PgPool) {
    // No HTTP fixture needed — the processor never calls upstream.
    let http_client = Arc::new(MockHttpClient::new());

    let shutdown_token = CancellationToken::new();
    let manager = Arc::new(
        PostgresRequestManager::with_client(
            TestDbPools::new(pool).await.unwrap(),
            http_client.clone(),
        )
        .with_config(fast_test_config())
        .with_processor(Arc::new(AlwaysFailProcessor)),
    );
    let _handle = manager.clone().run(shutdown_token.clone()).unwrap();

    let request_id = submit_one_request(&manager).await;
    wait_until_completed(&manager, request_id).await;

    let detail = manager.get_request_detail(request_id).await.unwrap();
    assert_eq!(detail.status, "failed");
    let err = detail.error.unwrap_or_default();
    assert!(
        err.contains("synthetic test failure"),
        "expected synthesized failure body in error: {err}"
    );

    shutdown_token.cancel();
}

// Compile-time check: the typestate Completed variant of RequestCompletionResult
// is reachable from our trait, ensuring downstream consumers can construct any
// terminal state if they need to.
#[allow(dead_code)]
fn _smoke_check_completed_variant(c: Request<Completed>) -> RequestCompletionResult {
    RequestCompletionResult::Completed(c)
}
