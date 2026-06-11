//! Integration tests for prompt-prefix cache tracking (tracking-only).
//!
//! Covers all three ingestion paths (realtime inline, flex + batch via the
//! daemon), per-user/per-model scoping (hits are matched across routes), the
//! per-batch read surface, and route-TTL eviction. No `sleep`-based timing — TTL
//! behaviour is exercised with a zero-lifetime route so expiry is deterministic.

use std::sync::Arc;
use std::time::Duration;

use fusillade::batch::{BatchInput, RequestTemplateInput};
use fusillade::daemon::DaemonConfig;
use fusillade::http::{HttpResponse, MockHttpClient};
use fusillade::manager::postgres::PostgresRequestManager;
use fusillade::manager::{DaemonExecutor, DaemonStorage, Storage};
use fusillade::{CreateFlexInput, CreateRealtimeInput, PrefixCacheConfig, TestDbPools};

/// A prefix-cache config tuned for deterministic, easy-to-assert hits:
/// one char per token, one token per block, no minimum — so `cached_tokens`
/// for two identical N-char bodies is exactly the shared leading char count.
fn hit_friendly_cache() -> PrefixCacheConfig {
    PrefixCacheConfig {
        chars_per_token: 1,
        block_tokens: 1,
        min_cached_tokens: 0,
        ..Default::default()
    }
}

async fn manager_with_cache(
    pool: sqlx::PgPool,
    http_client: Arc<MockHttpClient>,
    prefix_cache: PrefixCacheConfig,
) -> Arc<PostgresRequestManager<TestDbPools, MockHttpClient>> {
    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    // Limit 1 makes daemon processing serial, so prefix hits are deterministic.
    model_concurrency_limits.insert("test-model".to_string(), 1);

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        model_concurrency_limits,
        status_log_interval_ms: None,
        purge_interval_ms: 0,
        prefix_cache,
        ..Default::default()
    };

    Arc::new(
        PostgresRequestManager::with_client(TestDbPools::new(pool).await.unwrap(), http_client)
            .with_config(config),
    )
}

/// Create a realtime request and return the `cached_tokens` it recorded.
async fn realtime_cached(
    m: &PostgresRequestManager<TestDbPools, MockHttpClient>,
    input: CreateRealtimeInput,
) -> i64 {
    let id = m.create_realtime(input).await.unwrap();
    m.get_request_detail(fusillade::RequestId(id.0))
        .await
        .unwrap()
        .cached_tokens
}

fn realtime_input(user: &str, model: &str, path: &str, body: &str) -> CreateRealtimeInput {
    CreateRealtimeInput {
        request_id: uuid::Uuid::new_v4(),
        body: body.to_string(),
        model: model.to_string(),
        endpoint: "http://localhost".to_string(),
        method: "POST".to_string(),
        path: path.to_string(),
        api_key: String::new(),
        created_by: user.to_string(),
    }
}

const SHARED_BODY: &str = r#"{"system":"You are a helpful assistant. Follow the shared preamble exactly.","prompt":"first"}"#;
const SHARED_BODY_2: &str = r#"{"system":"You are a helpful assistant. Follow the shared preamble exactly.","prompt":"second"}"#;

/// Realtime requests compute `cached_tokens` inline at creation. A repeated
/// prefix (same user/model) hits — including across routes; a different user or
/// model misses.
#[sqlx::test]
#[test_log::test]
async fn realtime_prefix_hits_are_scoped_by_user_and_model(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());
    let manager = manager_with_cache(pool, http_client, hit_friendly_cache()).await;

    // First request seeds the cache: no prior prefix, so zero.
    let first = realtime_cached(
        &manager,
        realtime_input("u1", "test-model", "/v1/responses", SHARED_BODY),
    )
    .await;
    assert_eq!(first, 0, "first request must have no cache hit");

    // Second request, same scope + shared prefix: hits.
    let second = realtime_cached(
        &manager,
        realtime_input("u1", "test-model", "/v1/responses", SHARED_BODY_2),
    )
    .await;
    assert!(
        second > 0,
        "repeated prefix in same scope should hit, got {second}"
    );

    // Different user / model / path each miss (isolated scopes).
    let other_user = realtime_cached(
        &manager,
        realtime_input("u2", "test-model", "/v1/responses", SHARED_BODY_2),
    )
    .await;
    assert_eq!(other_user, 0, "different user must not share cache");
    let other_model = realtime_cached(
        &manager,
        realtime_input("u1", "other-model", "/v1/responses", SHARED_BODY_2),
    )
    .await;
    assert_eq!(other_model, 0, "different model must not share cache");
    // Same user + model on a *different* route still hits: matching ignores path.
    let other_path = realtime_cached(
        &manager,
        realtime_input("u1", "test-model", "/v1/other", SHARED_BODY_2),
    )
    .await;
    assert!(
        other_path > 0,
        "same user+model should share cache across routes, got {other_path}"
    );
}

/// Flex requests are processed by the daemon; `cached_tokens` is filled in at
/// dispatch time. Two identical flex requests yield one miss and one hit.
#[sqlx::test]
#[test_log::test]
async fn flex_prefix_hit_recorded_by_daemon(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());
    // One queued response is consumed per call; register enough for both requests.
    for _ in 0..4 {
        http_client.add_response(
            "POST /v1/responses",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"ok":true}"#.to_string(),
            }),
        );
    }
    let manager = manager_with_cache(pool, http_client, hit_friendly_cache()).await;

    let mk = |body: &str| CreateFlexInput {
        request_id: uuid::Uuid::new_v4(),
        body: body.to_string(),
        model: "test-model".to_string(),
        endpoint: "http://localhost".to_string(),
        method: "POST".to_string(),
        path: "/v1/responses".to_string(),
        api_key: String::new(),
        created_by: "flex-user".to_string(),
    };
    let id1 = manager.create_flex(mk(SHARED_BODY)).await.unwrap();
    let id2 = manager.create_flex(mk(SHARED_BODY_2)).await.unwrap();

    let shutdown = tokio_util::sync::CancellationToken::new();
    manager.clone().run(shutdown.clone()).unwrap();

    // Poll (via the batchless request-detail surface) until both complete,
    // then read their recorded cached_tokens.
    let start = tokio::time::Instant::now();
    let mut cached: Vec<i64> = Vec::new();
    while start.elapsed() < Duration::from_secs(10) {
        let d1 = manager
            .get_request_detail(fusillade::RequestId(id1.0))
            .await
            .unwrap();
        let d2 = manager
            .get_request_detail(fusillade::RequestId(id2.0))
            .await
            .unwrap();
        if d1.status == "completed" && d2.status == "completed" {
            cached = vec![d1.cached_tokens, d2.cached_tokens];
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    shutdown.cancel();

    cached.sort_unstable();
    assert_eq!(cached.len(), 2, "both flex requests should complete");
    assert_eq!(cached[0], 0, "one request seeds the cache (miss)");
    assert!(
        cached[1] > 0,
        "the other reuses the prefix (hit), got {:?}",
        cached
    );
}

/// Batch requests record cached_tokens at dispatch time; the per-batch read
/// surface (`get_batch_status`) exposes the aggregates.
#[sqlx::test]
#[test_log::test]
async fn batch_prefix_hits_surface_in_batch_status(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());
    // One queued response is consumed per call; register enough for both requests.
    for _ in 0..4 {
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"ok":true}"#.to_string(),
            }),
        );
    }
    let manager = manager_with_cache(pool, http_client, hit_friendly_cache()).await;

    let template = |body: &str| RequestTemplateInput {
        custom_id: None,
        endpoint: "https://api.example.com".to_string(),
        method: "POST".to_string(),
        path: "/v1/test".to_string(),
        body: body.to_string(),
        model: "test-model".to_string(),
        api_key: "k".to_string(),
    };
    let file_id = manager
        .create_file(
            "f".to_string(),
            None,
            vec![template(SHARED_BODY), template(SHARED_BODY_2)],
        )
        .await
        .unwrap();

    let batch = manager
        .create_batch(BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            // created_by is the cache scope for batch requests (COALESCE in claim).
            created_by: Some("batch-user".to_string()),
            api_key_id: None,
            api_key: None,
            total_requests: None,
        })
        .await
        .unwrap();

    let shutdown = tokio_util::sync::CancellationToken::new();
    manager.clone().run(shutdown.clone()).unwrap();

    let start = tokio::time::Instant::now();
    let mut status = None;
    while start.elapsed() < Duration::from_secs(10) {
        let s = manager.get_batch_status(batch.id).await.unwrap();
        if s.completed_requests == 2 {
            status = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    shutdown.cancel();

    let status = status.expect("batch should complete");
    assert!(
        status.cached_requests >= 1,
        "at least one batch request should record a cache hit, got {}",
        status.cached_requests
    );
    assert!(
        status.cached_tokens > 0,
        "batch should report cached tokens, got {}",
        status.cached_tokens
    );
}

/// A zero-lifetime route expires blocks immediately (lazy filter), so repeats
/// never hit; a normal route still hits. `purge_prefix_cache` then deletes the
/// expired rows. Exercises TTL without time-based sleeps.
#[sqlx::test]
#[test_log::test]
async fn route_ttl_controls_hits_and_purge(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());
    let mut cache = hit_friendly_cache();
    cache.default_ttl = Duration::from_secs(3600);
    cache
        .route_ttls
        .insert("/v1/zero".to_string(), Duration::ZERO);
    let manager = manager_with_cache(pool, http_client, cache).await;

    // Zero-TTL route: blocks are "expired" the instant they're written, so even
    // an identical repeat misses.
    realtime_cached(
        &manager,
        realtime_input("ttl-user", "test-model", "/v1/zero", SHARED_BODY),
    )
    .await;
    let zero_repeat = realtime_cached(
        &manager,
        realtime_input("ttl-user", "test-model", "/v1/zero", SHARED_BODY_2),
    )
    .await;
    assert_eq!(zero_repeat, 0, "zero-TTL route must never hit");

    // Default-TTL route: identical repeat hits.
    realtime_cached(
        &manager,
        realtime_input("ttl-user", "test-model", "/v1/live", SHARED_BODY),
    )
    .await;
    let live_repeat = realtime_cached(
        &manager,
        realtime_input("ttl-user", "test-model", "/v1/live", SHARED_BODY_2),
    )
    .await;
    assert!(
        live_repeat > 0,
        "default-TTL route should hit, got {live_repeat}"
    );

    // Purge removes the expired zero-TTL rows (the live route's rows survive).
    let deleted = manager.purge_prefix_cache().await.unwrap();
    assert!(
        deleted > 0,
        "purge should delete expired zero-TTL blocks, got {deleted}"
    );
}
