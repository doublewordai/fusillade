use fusillade::TestDbPools;
use fusillade::batch::{BatchInput, RequestTemplateInput};
use fusillade::daemon::{DaemonConfig, default_should_retry};
use fusillade::http::{HttpResponse, MockHttpClient};
use fusillade::manager::postgres::PostgresRequestManager;
use fusillade::manager::{DaemonExecutor, Storage};
use std::sync::Arc;
use std::time::Duration;

#[sqlx::test]
#[test_log::test]
async fn test_daemon_claims_and_completes_request(pool: sqlx::PgPool) {
    // Setup: Create HTTP client with mock response
    let http_client = Arc::new(MockHttpClient::new());
    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"success"}"#.to_string(),
        }),
    );

    // Setup: Create manager with fast claim interval (no sleeping)
    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10, // Very fast for testing
        default_model_concurrency: 10,
        model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
        max_retries: Some(3),
        stop_before_deadline_ms: None,
        backoff_ms: 100,
        backoff_factor: 2,
        max_backoff_ms: 1000,
        timeout_ms: 5000,
        status_log_interval_ms: None, // Disable status logging in tests
        heartbeat_interval_ms: 10000, // 10 seconds
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100, // Fast polling for tests
        ..Default::default()
    };

    let manager = Arc::new(
        PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_config(config),
    );

    // Setup: Create a file and batch to associate with our request
    let file_id = manager
        .create_file(
            "test-file".to_string(),
            Some("Test file".to_string()),
            vec![fusillade::RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "test-model".to_string(),
                api_key: "test-key".to_string(),
            }],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: None,
        })
        .await
        .expect("Failed to create batch");

    // Get the created request from the batch
    let requests = manager
        .get_batch_requests(batch.id)
        .await
        .expect("Failed to get batch requests");
    assert_eq!(requests.len(), 1);
    let request_id = requests[0].id();

    // Start the daemon
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    manager
        .clone()
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    // Poll for completion (with timeout)
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut completed = false;

    while start.elapsed() < timeout {
        let results = manager
            .get_requests(vec![request_id])
            .await
            .expect("Failed to get request");

        if let Some(Ok(any_request)) = results.first()
            && any_request.is_terminal()
        {
            if let fusillade::AnyRequest::Completed(req) = any_request {
                // Verify the request was completed successfully
                assert_eq!(req.state.response_status, 200);
                assert_eq!(req.state.response_body, r#"{"result":"success"}"#);
                completed = true;
                break;
            } else {
                panic!(
                    "Request reached terminal state but was not completed: {:?}",
                    any_request
                );
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Stop the daemon
    shutdown_token.cancel();

    // Assert that the request completed
    assert!(
        completed,
        "Request did not complete within timeout. Check daemon processing."
    );

    // Verify HTTP client was called exactly once
    assert_eq!(http_client.call_count(), 1);
    let calls = http_client.get_calls();
    assert_eq!(calls[0].method, "POST");
    assert_eq!(calls[0].path, "/v1/test");
    assert_eq!(calls[0].api_key, "test-key");
}

#[sqlx::test]
async fn test_daemon_respects_per_model_concurrency_limits(pool: sqlx::PgPool) {
    // Setup: Create HTTP client with triggered responses
    let http_client = Arc::new(MockHttpClient::new());

    // Add 5 triggered responses for our 5 requests
    let trigger1 = http_client.add_response_with_trigger(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"1"}"#.to_string(),
        }),
    );
    let trigger2 = http_client.add_response_with_trigger(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"2"}"#.to_string(),
        }),
    );
    let trigger3 = http_client.add_response_with_trigger(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"3"}"#.to_string(),
        }),
    );
    let trigger4 = http_client.add_response_with_trigger(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"4"}"#.to_string(),
        }),
    );
    let trigger5 = http_client.add_response_with_trigger(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"5"}"#.to_string(),
        }),
    );

    // Setup: Create manager with concurrency limit of 2 for "gpt-4"
    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("gpt-4".to_string(), 2);

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        default_model_concurrency: 10,
        model_concurrency_limits,
        max_retries: Some(3),
        stop_before_deadline_ms: None,
        backoff_ms: 100,
        backoff_factor: 2,
        max_backoff_ms: 1000,
        timeout_ms: 5000,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100, // Fast polling for tests
        ..Default::default()
    };

    let manager = Arc::new(
        PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_config(config),
    );

    // Setup: Create a file with 5 templates, all using "gpt-4"
    let file_id = manager
        .create_file(
            "test-file".to_string(),
            Some("Test concurrency limits".to_string()),
            vec![
                fusillade::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test1"}"#.to_string(),
                    model: "gpt-4".to_string(),
                    api_key: "test-key".to_string(),
                },
                fusillade::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test2"}"#.to_string(),
                    model: "gpt-4".to_string(),
                    api_key: "test-key".to_string(),
                },
                fusillade::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test3"}"#.to_string(),
                    model: "gpt-4".to_string(),
                    api_key: "test-key".to_string(),
                },
                fusillade::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test4"}"#.to_string(),
                    model: "gpt-4".to_string(),
                    api_key: "test-key".to_string(),
                },
                fusillade::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test5"}"#.to_string(),
                    model: "gpt-4".to_string(),
                    api_key: "test-key".to_string(),
                },
            ],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: None,
        })
        .await
        .expect("Failed to create batch");

    // Start the daemon
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    manager
        .clone()
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    // Wait for exactly 2 requests to be in-flight (respecting concurrency limit)
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(2);
    let mut reached_limit = false;

    while start.elapsed() < timeout {
        let in_flight = http_client.in_flight_count();
        if in_flight == 2 {
            reached_limit = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        reached_limit,
        "Expected exactly 2 requests in-flight, got {}",
        http_client.in_flight_count()
    );

    // Verify exactly 2 are in-flight (not more) by polling for a bit
    let start = tokio::time::Instant::now();
    let stable_duration = Duration::from_millis(100);
    while start.elapsed() < stable_duration {
        let in_flight = http_client.in_flight_count();
        assert!(
            in_flight <= 2,
            "Concurrency limit violated: {} requests in-flight (expected max 2)",
            in_flight
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        http_client.in_flight_count(),
        2,
        "Expected exactly 2 requests in-flight after stability check"
    );

    // Trigger completion of first request
    trigger1.send(()).unwrap();

    // Wait for the third request to start
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(2);
    let mut third_started = false;

    while start.elapsed() < timeout {
        if http_client.call_count() >= 3 {
            third_started = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        third_started,
        "Third request should have started after first completed"
    );

    // Verify still only 2 in-flight
    assert_eq!(
        http_client.in_flight_count(),
        2,
        "Should maintain concurrency limit of 2"
    );

    // Complete remaining requests to clean up
    trigger2.send(()).unwrap();
    trigger3.send(()).unwrap();
    trigger4.send(()).unwrap();
    trigger5.send(()).unwrap();

    // Wait for all requests to complete
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut all_completed = false;

    while start.elapsed() < timeout {
        let status = manager
            .get_batch_status(batch.id)
            .await
            .expect("Failed to get batch status");

        if status.completed_requests == 5 {
            all_completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Stop the daemon
    shutdown_token.cancel();

    assert!(all_completed, "All 5 requests should have completed");

    // Verify all 5 HTTP calls were made
    assert_eq!(http_client.call_count(), 5);
}

#[sqlx::test]
async fn test_daemon_retries_failed_requests(pool: sqlx::PgPool) {
    // Setup: Create HTTP client with failing responses, then success
    let http_client = Arc::new(MockHttpClient::new());

    // First attempt: fails with 500
    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 500,
            body: r#"{"error":"internal error"}"#.to_string(),
        }),
    );

    // Second attempt: fails with 503
    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 503,
            body: r#"{"error":"service unavailable"}"#.to_string(),
        }),
    );

    // Third attempt: succeeds
    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"success after retries"}"#.to_string(),
        }),
    );

    // Setup: Create manager with fast backoff for testing
    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        default_model_concurrency: 10,
        model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
        max_retries: Some(5),
        stop_before_deadline_ms: None,
        backoff_ms: 10, // Very fast backoff for testing
        backoff_factor: 2,
        max_backoff_ms: 100,
        timeout_ms: 5000,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100, // Fast polling for tests
        ..Default::default()
    };

    let manager = Arc::new(
        PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_config(config),
    );

    // Setup: Create a file and batch
    let file_id = manager
        .create_file(
            "test-file".to_string(),
            Some("Test retry logic".to_string()),
            vec![fusillade::RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "test-model".to_string(),
                api_key: "test-key".to_string(),
            }],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: None,
        })
        .await
        .expect("Failed to create batch");

    let requests = manager
        .get_batch_requests(batch.id)
        .await
        .expect("Failed to get batch requests");
    assert_eq!(requests.len(), 1);
    let request_id = requests[0].id();

    // Start the daemon
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    manager
        .clone()
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    // Poll for completion (with timeout)
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut completed = false;

    while start.elapsed() < timeout {
        let results = manager
            .get_requests(vec![request_id])
            .await
            .expect("Failed to get request");

        if let Some(Ok(any_request)) = results.first()
            && let fusillade::AnyRequest::Completed(req) = any_request
        {
            // Verify the request eventually completed successfully
            assert_eq!(req.state.response_status, 200);
            assert_eq!(
                req.state.response_body,
                r#"{"result":"success after retries"}"#
            );
            completed = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Stop the daemon
    shutdown_token.cancel();

    assert!(completed, "Request should have completed after retries");

    // Verify the request was attempted 3 times (2 failures + 1 success)
    assert_eq!(
        http_client.call_count(),
        3,
        "Expected 3 HTTP calls (2 failed attempts + 1 success)"
    );
}

#[sqlx::test]
async fn test_daemon_dynamically_updates_concurrency_limits(pool: sqlx::PgPool) {
    // Setup: Create HTTP client with triggered responses
    let http_client = Arc::new(MockHttpClient::new());

    // Add 10 triggered responses
    let mut triggers = vec![];
    for i in 1..=10 {
        let trigger = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: format!(r#"{{"result":"{}"}}"#, i),
            }),
        );
        triggers.push(trigger);
    }

    // Setup: Start with concurrency limit of 2 for "gpt-4"
    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("gpt-4".to_string(), 2);

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        default_model_concurrency: 10,
        model_concurrency_limits: model_concurrency_limits.clone(),
        max_retries: Some(3),
        stop_before_deadline_ms: None,
        backoff_ms: 100,
        backoff_factor: 2,
        max_backoff_ms: 1000,
        timeout_ms: 5000,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100, // Fast polling for tests
        ..Default::default()
    };

    let manager = Arc::new(
        PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_config(config),
    );

    // Setup: Create a file with 10 requests, all using "gpt-4"
    let templates: Vec<_> = (1..=10)
        .map(|i| fusillade::RequestTemplateInput {
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/v1/test".to_string(),
            body: format!(r#"{{"prompt":"test{}"}}"#, i),
            model: "gpt-4".to_string(),
            api_key: "test-key".to_string(),
        })
        .collect();

    let file_id = manager
        .create_file(
            "test-file".to_string(),
            Some("Test dynamic limits".to_string()),
            templates,
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: None,
        })
        .await
        .expect("Failed to create batch");

    // Start the daemon
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    manager
        .clone()
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    // Wait for exactly 2 requests to be in-flight (initial limit)
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(2);
    let mut reached_initial_limit = false;

    while start.elapsed() < timeout {
        let in_flight = http_client.in_flight_count();
        if in_flight == 2 {
            reached_initial_limit = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        reached_initial_limit,
        "Expected exactly 2 requests in-flight with initial limit"
    );

    // Increase the limit to 5
    model_concurrency_limits.insert("gpt-4".to_string(), 5);

    // Complete one request to free up a permit and trigger daemon to check limits
    triggers.remove(0).send(()).unwrap();

    // Now we should see up to 5 requests in flight (daemon picks up new limit)
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(2);
    let mut reached_new_limit = false;

    while start.elapsed() < timeout {
        let in_flight = http_client.in_flight_count();
        if in_flight >= 4 {
            // Should see at least 4-5 in flight with new limit
            reached_new_limit = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        reached_new_limit,
        "Expected more requests in-flight after limit increase, got {}",
        http_client.in_flight_count()
    );

    // Now decrease the limit to 3
    model_concurrency_limits.insert("gpt-4".to_string(), 3);

    // Complete remaining requests
    for trigger in triggers {
        trigger.send(()).unwrap();
    }

    // Wait for all requests to complete
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut all_completed = false;

    while start.elapsed() < timeout {
        let status = manager
            .get_batch_status(batch.id)
            .await
            .expect("Failed to get batch status");

        if status.completed_requests == 10 {
            all_completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Stop the daemon
    shutdown_token.cancel();

    assert!(all_completed, "All 10 requests should have completed");
    assert_eq!(http_client.call_count(), 10);
}

#[sqlx::test]
async fn test_deadline_aware_retry_stops_before_deadline(pool: sqlx::PgPool) {
    // Test that retries stop when approaching the deadline
    let http_client = Arc::new(MockHttpClient::new());

    // All requests will fail
    for _ in 0..20 {
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 500,
                body: r#"{"error":"server error"}"#.to_string(),
            }),
        );
    }

    // Use deadline-aware retry with a short completion window and short buffer
    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        default_model_concurrency: 10,
        model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
        max_retries: Some(10_000),
        stop_before_deadline_ms: Some(500), // 500ms buffer before deadline
        backoff_ms: 50,
        backoff_factor: 2,
        max_backoff_ms: 200,
        timeout_ms: 5000,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100,
        ..Default::default()
    };

    let manager = Arc::new(
        PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_config(config),
    );

    // Create a batch with a very short completion window (2 seconds)
    let file_id = manager
        .create_file(
            "test-file".to_string(),
            Some("Test deadline cutoff".to_string()),
            vec![fusillade::RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "test-model".to_string(),
                api_key: "test-key".to_string(),
            }],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "2s".to_string(), // Very short window
            metadata: None,
            created_by: None,
        })
        .await
        .expect("Failed to create batch");

    let requests = manager
        .get_batch_requests(batch.id)
        .await
        .expect("Failed to get batch requests");
    let request_id = requests[0].id();

    // Start the daemon
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    manager
        .clone()
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    // Poll until request reaches Failed state (due to deadline)
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut results = None;

    while start.elapsed() < timeout {
        let res = manager
            .get_requests(vec![request_id])
            .await
            .expect("Failed to get request");

        if let Some(Ok(req)) = res.first()
            && req.is_terminal()
        {
            results = Some(res);
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_token.cancel();

    let results = results.expect("Request should have reached terminal state within timeout");

    if let Some(Ok(fusillade::AnyRequest::Failed(failed))) = results.first() {
        // Calculate expected retry attempts:
        // - Completion window: 2000ms
        // - Buffer: 500ms
        // - Effective deadline: 1500ms
        // - Backoff sequence: 50ms, 100ms, 200ms, 200ms, 200ms, 200ms, 200ms
        // - Timeline:
        //   - Initial attempt: t=0ms (attempt 0)
        //   - Retry 1: t=50ms (attempt 1)
        //   - Retry 2: t=150ms (attempt 2)
        //   - Retry 3: t=350ms (attempt 3)
        //   - Retry 4: t=550ms (attempt 4)
        //   - Retry 5: t=750ms (attempt 5)
        //   - Retry 6: t=950ms (attempt 6)
        //   - Retry 7: t=1150ms (attempt 7)
        //   - Retry 8: t=1350ms (attempt 8)
        //   - Next would be t=1550ms - EXCEEDS 1500ms deadline
        // Expected: 8 retry attempts (9 total including initial)

        let retry_count = failed.state.retry_attempt;
        let call_count = http_client.call_count();

        // 1. Verify we stopped before too many retries (deadline constraint)
        // Allow 7-9 attempts to account for timing variations in test execution
        assert!(
            (7..=9).contains(&retry_count),
            "Expected 7-9 retry attempts based on deadline and backoff calculation, got {}",
            retry_count
        );

        // 2. Verify HTTP call count matches retry attempts (1 initial + N retries)
        assert_eq!(
            call_count,
            (retry_count + 1) as usize,
            "Expected call count to match retry attempts + 1 initial attempt, got {} calls for {} retry attempts",
            call_count,
            retry_count
        );

        // 3. Verify the request actually has error details from the last attempt
        assert!(
            !failed.state.reason.to_error_message().is_empty(),
            "Expected failed request to have failure reason"
        );
    } else {
        panic!(
            "Expected request to be in Failed state, got {:?}",
            results.first()
        );
    }
}

#[sqlx::test]
async fn test_retry_stops_at_deadline_when_no_limits_set(pool: sqlx::PgPool) {
    // Test that when neither max_retries nor stop_before_deadline_ms is set,
    // retries stop exactly at the deadline (no buffer)
    let http_client = Arc::new(MockHttpClient::new());

    // All requests will fail
    for _ in 0..20 {
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 500,
                body: r#"{"error":"server error"}"#.to_string(),
            }),
        );
    }

    // No max_retries, no stop_before_deadline_ms
    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        default_model_concurrency: 10,
        model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
        max_retries: None,             // No retry limit
        stop_before_deadline_ms: None, // No buffer - should retry until deadline
        backoff_ms: 50,
        backoff_factor: 2,
        max_backoff_ms: 200,
        timeout_ms: 5000,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100,
        ..Default::default()
    };

    let manager = Arc::new(
        PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_config(config),
    );

    // Create a batch with a 2 second completion window
    let file_id = manager
        .create_file(
            "test-file".to_string(),
            Some("Test no limits retry".to_string()),
            vec![fusillade::RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "test-model".to_string(),
                api_key: "test-key".to_string(),
            }],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "2s".to_string(),
            metadata: None,
            created_by: None,
        })
        .await
        .expect("Failed to create batch");

    let requests = manager
        .get_batch_requests(batch.id)
        .await
        .expect("Failed to get batch requests");
    let request_id = requests[0].id();

    // Start the daemon
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    manager
        .clone()
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    // Poll until request reaches Failed state (due to deadline)
    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut results = None;

    while start.elapsed() < timeout {
        let res = manager
            .get_requests(vec![request_id])
            .await
            .expect("Failed to get request");

        if let Some(Ok(req)) = res.first()
            && req.is_terminal()
        {
            results = Some(res);
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_token.cancel();

    let results = results.expect("Request should have reached terminal state within timeout");

    if let Some(Ok(fusillade::AnyRequest::Failed(failed))) = results.first() {
        // Calculate expected retry attempts with NO buffer:
        // - Completion window: 2000ms
        // - Buffer: 0ms (none set)
        // - Effective deadline: 2000ms
        // - Backoff sequence: 50ms, 100ms, 200ms, 200ms, 200ms...
        // - Timeline:
        //   - Initial attempt: t=0ms (attempt 0)
        //   - Retry 1: t=50ms (attempt 1)
        //   - Retry 2: t=150ms (attempt 2)
        //   - Retry 3: t=350ms (attempt 3)
        //   - Retry 4: t=550ms (attempt 4)
        //   - Retry 5: t=750ms (attempt 5)
        //   - Retry 6: t=950ms (attempt 6)
        //   - Retry 7: t=1150ms (attempt 7)
        //   - Retry 8: t=1350ms (attempt 8)
        //   - Retry 9: t=1550ms (attempt 9)
        //   - Retry 10: t=1750ms (attempt 10)
        //   - Retry 11: t=1950ms (attempt 11)
        //   - Next would be t=2150ms - EXCEEDS 2000ms deadline
        // Expected: ~11 retry attempts (12 total including initial)
        // In reality, we will see <11 due to DB calls and CPU overhead in making requests

        let retry_count = failed.state.retry_attempt;
        let call_count = http_client.call_count();

        // 1. Verify we retried more than the buffered case (which stopped at ~8)
        //    but still stopped before too many attempts
        // Allow 9-12 attempts to account for timing variations with CI slower CI CPUs
        assert!(
            (9..12).contains(&retry_count),
            "Expected 9-12 retry attempts (should retry until deadline with no buffer), got {}",
            retry_count
        );

        // 2. Verify HTTP call count matches retry attempts (1 initial + N retries)
        assert_eq!(
            call_count,
            (retry_count + 1) as usize,
            "Expected call count to match retry attempts + 1 initial attempt, got {} calls for {} retry attempts",
            call_count,
            retry_count
        );

        // 3. Verify the request has error details from the last attempt
        assert!(
            !failed.state.reason.to_error_message().is_empty(),
            "Expected failed request to have failure reason"
        );
    } else {
        panic!(
            "Expected request to be in Failed state, got {:?}",
            results.first()
        );
    }
}
/// Tests for `get_batch_results_stream` and `stream_batch_results`.
///
/// These tests verify that batch results streaming works correctly,
/// including pagination, filtering, and error handling.
mod batch_results_stream {
    use super::*;
    use futures::StreamExt;

    /// Helper to collect all results from a batch results stream
    async fn collect_batch_results(
        manager: &PostgresRequestManager<TestDbPools, MockHttpClient>,
        batch_id: fusillade::batch::BatchId,
    ) -> Vec<fusillade::batch::BatchResultItem> {
        let stream = manager.get_batch_results_stream(batch_id, 0, None, None);
        stream
            .filter_map(|r| async { r.ok() })
            .collect::<Vec<_>>()
            .await
    }

    #[sqlx::test]
    async fn test_batch_results_basic(pool: sqlx::PgPool) {
        // Test: Basic batch returns all results correctly
        let http_client = Arc::new(MockHttpClient::new());
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"success1"}"#.to_string(),
            }),
        );
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"success2"}"#.to_string(),
            }),
        );

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            ..Default::default()
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client.clone(),
            )
            .with_config(config),
        );

        // Create file with 2 templates
        let file_id = manager
            .create_file(
                "test-results".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("req-1".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test1"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "test-key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req-2".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test2"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "test-key".to_string(),
                    },
                ],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        // Run daemon to complete requests
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for requests to complete
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");
            if requests.iter().all(|r| r.is_terminal()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_token.cancel();

        // Get batch results stream
        let results = collect_batch_results(&manager, batch.id).await;

        // Should have exactly 2 results (one per template)
        assert_eq!(results.len(), 2, "Should have 2 results");

        // Verify custom IDs
        let custom_ids: Vec<_> = results
            .iter()
            .filter_map(|r| r.custom_id.as_ref())
            .collect();
        assert!(custom_ids.contains(&&"req-1".to_string()));
        assert!(custom_ids.contains(&&"req-2".to_string()));

        // All should be completed
        for result in &results {
            assert_eq!(
                result.status,
                fusillade::batch::BatchResultStatus::Completed
            );
            assert!(result.response_body.is_some());
        }
    }

    #[sqlx::test]
    async fn test_batch_results_deleted_file_returns_error(pool: sqlx::PgPool) {
        // Test: When batch's file is deleted, stream returns error
        let http_client = Arc::new(MockHttpClient::new());

        let manager = Arc::new(PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        ));

        let file_id = manager
            .create_file(
                "to-delete".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("test".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test"}"#.to_string(),
                    model: "gpt-4".to_string(),
                    api_key: "test-key".to_string(),
                }],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        // Delete the file (set file_id to NULL on batch)
        sqlx::query!(
            "UPDATE batches SET file_id = NULL WHERE id = $1",
            *batch.id as uuid::Uuid
        )
        .execute(&pool)
        .await
        .expect("Failed to clear file_id");

        // Try to get results - should get an error
        let stream = manager.get_batch_results_stream(batch.id, 0, None, None);
        let results: Vec<_> = stream.collect().await;

        // Should have one error result
        assert_eq!(results.len(), 1, "Should have one result (the error)");
        assert!(
            results[0].is_err(),
            "Result should be an error when file_id is NULL"
        );

        let err = results[0].as_ref().unwrap_err();
        assert!(
            err.to_string().contains("file_id"),
            "Error should mention file_id: {}",
            err
        );
    }

    #[sqlx::test]
    async fn test_batch_results_pagination(pool: sqlx::PgPool) {
        // Test: Pagination works correctly with offset
        let http_client = Arc::new(MockHttpClient::new());

        // Add responses for 5 requests
        for i in 0..5 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 200,
                    body: format!(r#"{{"result":"success{}"}}"#, i),
                }),
            );
        }

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            ..Default::default()
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client.clone(),
            )
            .with_config(config),
        );

        // Create file with 5 templates
        let templates: Vec<_> = (0..5)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("req-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: format!(r#"{{"prompt":"test{}"}}"#, i),
                model: "gpt-4".to_string(),
                api_key: "test-key".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file("pagination-test".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        // Run daemon
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for completion
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");
            if requests.iter().all(|r| r.is_terminal()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_token.cancel();

        // Get all results
        let all_results = collect_batch_results(&manager, batch.id).await;
        assert_eq!(all_results.len(), 5, "Should have 5 results");

        // Get with offset 2
        let stream = manager.get_batch_results_stream(batch.id, 2, None, None);
        let offset_results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(
            offset_results.len(),
            3,
            "Should have 3 results with offset 2"
        );

        // Verify the offset results are the last 3
        let offset_ids: Vec<_> = offset_results
            .iter()
            .filter_map(|r| r.custom_id.as_ref())
            .collect();
        assert!(offset_ids.contains(&&"req-2".to_string()));
        assert!(offset_ids.contains(&&"req-3".to_string()));
        assert!(offset_ids.contains(&&"req-4".to_string()));
    }

    #[sqlx::test]
    async fn test_batch_results_status_filter(pool: sqlx::PgPool) {
        // Test: Status filter works correctly
        let http_client = Arc::new(MockHttpClient::new());

        // First 2 succeed, third fails
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"success1"}"#.to_string(),
            }),
        );
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"success2"}"#.to_string(),
            }),
        );
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 500,
                body: r#"{"error":"server error"}"#.to_string(),
            }),
        );

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            max_retries: Some(0), // No retries so it fails immediately
            ..Default::default()
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client.clone(),
            )
            .with_config(config),
        );

        let templates: Vec<_> = (0..3)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("req-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: format!(r#"{{"prompt":"test{}"}}"#, i),
                model: "gpt-4".to_string(),
                api_key: "test-key".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file("filter-test".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for completion
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");
            if requests.iter().all(|r| r.is_terminal()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_token.cancel();

        // Filter by completed
        let stream =
            manager.get_batch_results_stream(batch.id, 0, None, Some("completed".to_string()));
        let completed_results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(
            completed_results.len(),
            2,
            "Should have 2 completed results"
        );
        for r in &completed_results {
            assert_eq!(r.status, fusillade::batch::BatchResultStatus::Completed);
        }

        // Filter by failed
        let stream =
            manager.get_batch_results_stream(batch.id, 0, None, Some("failed".to_string()));
        let failed_results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(failed_results.len(), 1, "Should have 1 failed result");
        assert_eq!(
            failed_results[0].status,
            fusillade::batch::BatchResultStatus::Failed
        );
    }

    #[sqlx::test]
    async fn test_batch_results_search_filter(pool: sqlx::PgPool) {
        // Test: Search filter works correctly (case-insensitive)
        let http_client = Arc::new(MockHttpClient::new());

        for _ in 0..3 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 200,
                    body: r#"{"result":"success"}"#.to_string(),
                }),
            );
        }

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            ..Default::default()
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client.clone(),
            )
            .with_config(config),
        );

        let file_id = manager
            .create_file(
                "search-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("Alpha-Request".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "test-key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("Beta-Request".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "test-key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("Gamma-Item".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "test-key".to_string(),
                    },
                ],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for completion
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");
            if requests.iter().all(|r| r.is_terminal()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_token.cancel();

        // Search for "request" (case-insensitive)
        let stream =
            manager.get_batch_results_stream(batch.id, 0, Some("request".to_string()), None);
        let search_results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(
            search_results.len(),
            2,
            "Should find 2 results containing 'request'"
        );

        let custom_ids: Vec<_> = search_results
            .iter()
            .filter_map(|r| r.custom_id.as_ref())
            .collect();
        assert!(custom_ids.contains(&&"Alpha-Request".to_string()));
        assert!(custom_ids.contains(&&"Beta-Request".to_string()));

        // Search for "ALPHA" (case-insensitive)
        let stream = manager.get_batch_results_stream(batch.id, 0, Some("ALPHA".to_string()), None);
        let alpha_results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(alpha_results.len(), 1, "Should find 1 result for 'ALPHA'");
        assert_eq!(
            alpha_results[0].custom_id,
            Some("Alpha-Request".to_string())
        );
    }
}
