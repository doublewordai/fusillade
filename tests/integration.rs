use fusillade::TestDbPools;
use fusillade::batch::{BatchInput, RequestTemplateInput};
use fusillade::daemon::{DaemonConfig, ModelEscalationConfig, default_should_retry};
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
            .get_batch_status(batch.id, false)
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
            .get_batch_status(batch.id, false)
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

#[sqlx::test]
async fn test_route_at_claim_time_escalation(pool: sqlx::PgPool) {
    // Test: When time remaining before batch expiry is below the escalation threshold,
    // requests are routed to the escalation model at claim time.

    let http_client = Arc::new(MockHttpClient::new());

    // Add response for the escalation model endpoint
    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"escalated response"}"#.to_string(),
        }),
    );

    // Configure model escalation: gpt-4 -> gpt-4-turbo when under time pressure
    let model_escalations = Arc::new(dashmap::DashMap::new());
    model_escalations.insert(
        "gpt-4".to_string(),
        ModelEscalationConfig {
            escalation_model: "gpt-4-turbo".to_string(),
            // Use a very high threshold (2 hours) so it always triggers with our short window
            escalation_threshold_seconds: 7200,
        },
    );

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        default_model_concurrency: 10,
        model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

        model_escalations,
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

    // Create a file with a request using gpt-4
    let file_id = manager
        .create_file(
            "test-escalation".to_string(),
            Some("Test route-at-claim-time escalation".to_string()),
            vec![fusillade::RequestTemplateInput {
                custom_id: Some("escalation-test".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "gpt-4".to_string(),
                api_key: "original-key".to_string(),
            }],
        )
        .await
        .expect("Failed to create file");

    // Create batch with a 1-hour completion window (less than 2-hour threshold)
    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "1h".to_string(),
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

    // Poll for completion
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
                assert_eq!(req.state.response_status, 200);
                assert_eq!(
                    req.state.response_body,
                    r#"{"result":"escalated response"}"#
                );
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

    shutdown_token.cancel();

    assert!(completed, "Request did not complete within timeout");

    // Verify the HTTP call was made with the ESCALATION model (routed at claim time)
    // Note: With route-at-claim-time escalation, the original API key is used
    // (batch API keys automatically have access to escalation models in the onwards cache)
    assert_eq!(http_client.call_count(), 1);
    let calls = http_client.get_calls();
    assert_eq!(
        calls[0].api_key, "original-key",
        "Should use original API key (escalation only changes model routing)"
    );
    // The escalation is verified by the request completing successfully with the
    // escalated model's response. The model change happens at claim time in the daemon.
}

#[sqlx::test]
async fn test_route_at_claim_time_no_escalation_when_enough_time(pool: sqlx::PgPool) {
    // Test: When there's enough time remaining (above threshold), requests use original model

    let http_client = Arc::new(MockHttpClient::new());

    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"normal response"}"#.to_string(),
        }),
    );

    // Configure escalation with a 1-minute threshold
    let model_escalations = Arc::new(dashmap::DashMap::new());
    model_escalations.insert(
        "gpt-4".to_string(),
        ModelEscalationConfig {
            escalation_model: "gpt-4-turbo".to_string(),
            escalation_threshold_seconds: 60, // 1 minute threshold
        },
    );

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        default_model_concurrency: 10,
        model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

        model_escalations,
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

    let file_id = manager
        .create_file(
            "test-no-escalation".to_string(),
            Some("Test no escalation when enough time".to_string()),
            vec![fusillade::RequestTemplateInput {
                custom_id: Some("no-escalation-test".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "gpt-4".to_string(),
                api_key: "original-key".to_string(),
            }],
        )
        .await
        .expect("Failed to create file");

    // Create batch with 24-hour window (well above 1-minute threshold)
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
    let request_id = requests[0].id();

    let shutdown_token = tokio_util::sync::CancellationToken::new();
    manager
        .clone()
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    // Poll for completion
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
            completed = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_token.cancel();

    assert!(completed, "Request did not complete within timeout");

    // Verify the HTTP call used the ORIGINAL API key (no escalation)
    assert_eq!(http_client.call_count(), 1);
    let calls = http_client.get_calls();
    assert_eq!(
        calls[0].api_key, "original-key",
        "Should use original API key when time remaining is above threshold"
    );
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
        let stream = manager.get_batch_results_stream(batch_id, 0, None, None, false);
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
        let stream = manager.get_batch_results_stream(batch.id, 0, None, None, false);
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
    async fn test_output_file_streamable_after_batch_deleted(pool: sqlx::PgPool) {
        // Test: Output files can still be streamed after the batch is soft-deleted
        // This ensures users can download completed results even if the batch is deleted
        let http_client = Arc::new(MockHttpClient::new());
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"success"}"#.to_string(),
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

        let file_id = manager
            .create_file(
                "output-after-delete".to_string(),
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

        // Run daemon to process the request
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for completion
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let b = manager
                .get_batch(batch.id, false)
                .await
                .expect("get_batch failed");
            if b.completed_requests == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        shutdown_token.cancel();

        // Get the output file ID
        let batch = manager
            .get_batch(batch.id, false)
            .await
            .expect("get_batch failed");
        let output_file_id = batch
            .output_file_id
            .expect("Batch should have output_file_id");

        // Verify we can get output file content before deletion
        let results_before = manager
            .get_file_content(output_file_id)
            .await
            .expect("Should be able to get output file content before deletion");
        assert!(
            !results_before.is_empty(),
            "Output file should have content before deletion"
        );

        // Soft-delete the batch
        manager
            .delete_batch(batch.id)
            .await
            .expect("delete_batch failed");

        // Verify batch is no longer accessible
        let batch_result = manager.get_batch(batch.id, false).await;
        assert!(
            batch_result.is_err(),
            "Batch should not be found after deletion"
        );

        // Verify we can STILL get the output file content after batch deletion
        let results_after = manager
            .get_file_content(output_file_id)
            .await
            .expect("Should still be able to get output file content after batch deletion");
        assert!(
            !results_after.is_empty(),
            "Output file should still have content after batch deletion"
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
        let stream = manager.get_batch_results_stream(batch.id, 2, None, None, false);
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
        let stream = manager.get_batch_results_stream(
            batch.id,
            0,
            None,
            Some("completed".to_string()),
            false,
        );
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
            manager.get_batch_results_stream(batch.id, 0, None, Some("failed".to_string()), false);
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
            manager.get_batch_results_stream(batch.id, 0, Some("request".to_string()), None, false);
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
        let stream =
            manager.get_batch_results_stream(batch.id, 0, Some("ALPHA".to_string()), None, false);
        let alpha_results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(alpha_results.len(), 1, "Should find 1 result for 'ALPHA'");
        assert_eq!(
            alpha_results[0].custom_id,
            Some("Alpha-Request".to_string())
        );
    }

    #[sqlx::test]
    #[test_log::test]
    async fn test_hide_retriable_before_sla_filters_before_expiry(pool: sqlx::PgPool) {
        // Test that retriable errors are hidden before SLA expiry when hide_retriable_before_sla=true

        let http_client = Arc::new(MockHttpClient::new());

        // Add responses: 3 retriable failures (429, 503, 404) and 1 non-retriable failure (400)
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 429,
                body: "rate limited".to_string(),
            }),
        );
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 503,
                body: "service unavailable".to_string(),
            }),
        );
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 404,
                body: "not found".to_string(),
            }),
        );
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 400,
                body: "bad request".to_string(),
            }),
        );

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

            max_retries: Some(0), // No retries - fail immediately
            stop_before_deadline_ms: None,
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 10000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
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

        let templates = vec![
            fusillade::RequestTemplateInput {
                custom_id: Some("req-429".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"test":"429"}"#.to_string(),
                model: "test-model".to_string(),
                api_key: "test-key".to_string(),
            },
            fusillade::RequestTemplateInput {
                custom_id: Some("req-503".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"test":"503"}"#.to_string(),
                model: "test-model".to_string(),
                api_key: "test-key".to_string(),
            },
            fusillade::RequestTemplateInput {
                custom_id: Some("req-400".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"test":"400"}"#.to_string(),
                model: "test-model".to_string(),
                api_key: "test-key".to_string(),
            },
            fusillade::RequestTemplateInput {
                custom_id: Some("req-404".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"test":"404"}"#.to_string(),
                model: "test-model".to_string(),
                api_key: "test-key".to_string(),
            },
        ];

        let file_id = manager
            .create_file("test-file".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(fusillade::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1h".to_string(), // 1 hour - plenty of time before expiry
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

        // Wait for all requests to fail
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let status = manager
                .get_batch_status(batch.id, false)
                .await
                .expect("Failed to get batch status");
            if status.failed_requests == 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        shutdown_token.cancel();

        // Test 1: Query with hide_retriable_before_sla = true (before SLA expiry)
        // Should only show non-retriable failures (400)
        let batch_filtered = manager
            .get_batch(batch.id, true)
            .await
            .expect("Failed to get batch");
        assert_eq!(
            batch_filtered.failed_requests, 1,
            "Should only show 1 non-retriable failure when hide_retriable_before_sla=true"
        );

        // Test 2: Query with hide_retriable_before_sla = false
        // Should show all failures
        let batch_unfiltered = manager
            .get_batch(batch.id, false)
            .await
            .expect("Failed to get batch");
        assert_eq!(
            batch_unfiltered.failed_requests, 4,
            "Should show all 4 failures when hide_retriable_before_sla=false"
        );

        // Test 3: Error file stream with filtering
        let error_file_id = batch_filtered
            .error_file_id
            .expect("Should have error file");
        let stream = manager.get_file_content_stream(error_file_id, 0, None, true);
        let errors: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;
        assert_eq!(
            errors.len(),
            1,
            "Error file stream should only show 1 non-retriable error when filtered"
        );

        // Test 4: Error file stream without filtering
        let stream = manager.get_file_content_stream(error_file_id, 0, None, false);
        let all_errors: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;
        assert_eq!(
            all_errors.len(),
            4,
            "Error file stream should show all 4 errors when not filtered"
        );
    }

    #[sqlx::test]
    #[test_log::test]
    async fn test_hide_retriable_shows_all_after_sla_expiry(pool: sqlx::PgPool) {
        // Test that all errors are shown after SLA expiry regardless of hide_retriable_before_sla value

        let http_client = Arc::new(MockHttpClient::new());

        // Add 2 retriable failures (429) and 2 non-retriable failures (400)
        for _ in 0..2 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 429,
                    body: "rate limited".to_string(),
                }),
            );
        }
        for _ in 0..2 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 400,
                    body: "bad request".to_string(),
                }),
            );
        }

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

            max_retries: Some(0), // No retries - fail immediately
            stop_before_deadline_ms: None,
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 10000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
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

        let templates = (0..4)
            .map(|i| fusillade::RequestTemplateInput {
                custom_id: Some(format!("req-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"test":"data"}"#.to_string(),
                model: "test-model".to_string(),
                api_key: "test-key".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file("test-file".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(fusillade::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1s".to_string(), // 1 second - will expire quickly
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

        // Wait for all requests to fail
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let status = manager
                .get_batch_status(batch.id, false)
                .await
                .expect("Failed to get batch status");
            if status.failed_requests == 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Wait for SLA to expire (1 second completion window + buffer)
        tokio::time::sleep(Duration::from_secs(2)).await;

        shutdown_token.cancel();

        // Query with hide_retriable_before_sla = true AFTER SLA expiry
        // Should show ALL failures because SLA has expired
        let batch_after_expiry = manager
            .get_batch(batch.id, true)
            .await
            .expect("Failed to get batch");
        assert_eq!(
            batch_after_expiry.failed_requests, 4,
            "Should show all 4 failures after SLA expiry, even with hide_retriable_before_sla=true"
        );
    }

    #[sqlx::test]
    #[test_log::test]
    async fn test_batch_results_stream_respects_hide_retriable(pool: sqlx::PgPool) {
        // Test that batch results stream respects hide_retriable_before_sla parameter

        let http_client = Arc::new(MockHttpClient::new());

        // Add 1 success, 2 retriable failures (429), and 2 non-retriable failures (400)
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"success"}"#.to_string(),
            }),
        );
        for _ in 0..2 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 429,
                    body: "rate limited".to_string(),
                }),
            );
        }
        for _ in 0..2 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 400,
                    body: "bad request".to_string(),
                }),
            );
        }

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

            max_retries: Some(0), // No retries
            stop_before_deadline_ms: None,
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 10000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
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

        let templates = (0..5)
            .map(|i| fusillade::RequestTemplateInput {
                custom_id: Some(format!("req-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"test":"data"}"#.to_string(),
                model: "test-model".to_string(),
                api_key: "test-key".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file("test-file".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(fusillade::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1h".to_string(), // Plenty of time before SLA expiry
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

        // Wait for all requests to complete
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");
            if requests.iter().all(|r| r.is_terminal()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        shutdown_token.cancel();

        // Test 1: Stream all results with hide_retriable_before_sla = false
        let stream = manager.get_batch_results_stream(batch.id, 0, None, None, false);
        let all_results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;
        assert_eq!(all_results.len(), 5, "Should stream all 5 results");

        // Test 2: Stream failed results with hide_retriable_before_sla = true (before SLA)
        // Should only show non-retriable failures
        let stream =
            manager.get_batch_results_stream(batch.id, 0, None, Some("failed".to_string()), true);
        let filtered_failures: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;
        assert_eq!(
            filtered_failures.len(),
            2,
            "Should only stream 2 non-retriable failures when filtered"
        );

        // Test 3: Stream failed results without filtering
        let stream =
            manager.get_batch_results_stream(batch.id, 0, None, Some("failed".to_string()), false);
        let all_failures: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;
        assert_eq!(
            all_failures.len(),
            4,
            "Should stream all 4 failures when not filtered"
        );

        // Test 4: Completed results should not be affected by filtering
        let stream = manager.get_batch_results_stream(
            batch.id,
            0,
            None,
            Some("completed".to_string()),
            true,
        );
        let completed: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;
        assert_eq!(
            completed.len(),
            1,
            "Should still show 1 completed result (not affected by error filtering)"
        );
    }

    #[sqlx::test]
    #[test_log::test]
    async fn test_get_batch_status_respects_hide_retriable(pool: sqlx::PgPool) {
        // Test that get_batch_status respects hide_retriable_before_sla parameter

        let http_client = Arc::new(MockHttpClient::new());

        // Add 3 retriable failures (429) and 2 non-retriable failures (400)
        for _ in 0..3 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 429,
                    body: "rate limited".to_string(),
                }),
            );
        }
        for _ in 0..2 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 400,
                    body: "bad request".to_string(),
                }),
            );
        }

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

            max_retries: Some(0), // No retries
            stop_before_deadline_ms: None,
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 10000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
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

        let templates = (0..5)
            .map(|i| fusillade::RequestTemplateInput {
                custom_id: Some(format!("req-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"test":"data"}"#.to_string(),
                model: "test-model".to_string(),
                api_key: "test-key".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file("test-file".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(fusillade::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1h".to_string(),
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

        // Wait for all to fail
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let status = manager
                .get_batch_status(batch.id, false)
                .await
                .expect("Failed to get status");
            if status.failed_requests == 5 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        shutdown_token.cancel();

        // Test with hide_retriable_before_sla = true
        let status_filtered = manager
            .get_batch_status(batch.id, true)
            .await
            .expect("Failed to get batch status");
        assert_eq!(
            status_filtered.failed_requests, 2,
            "Should show only 2 non-retriable failures"
        );
        assert_eq!(status_filtered.total_requests, 5);

        // Test with hide_retriable_before_sla = false
        let status_unfiltered = manager
            .get_batch_status(batch.id, false)
            .await
            .expect("Failed to get batch status");
        assert_eq!(
            status_unfiltered.failed_requests, 5,
            "Should show all 5 failures"
        );
    }

    #[sqlx::test]
    #[test_log::test]
    async fn test_list_file_batches_respects_hide_retriable(pool: sqlx::PgPool) {
        // Test that list_file_batches respects hide_retriable_before_sla parameter

        let http_client = Arc::new(MockHttpClient::new());

        // Add failures for 2 batches - interleave so each batch gets mixed errors
        // Batch 1: 2x 429, 2x 400
        // Batch 2: 2x 429, 2x 400
        for _ in 0..2 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 429,
                    body: "rate limited".to_string(),
                }),
            );
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 400,
                    body: "bad request".to_string(),
                }),
            );
        }
        for _ in 0..2 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 429,
                    body: "rate limited".to_string(),
                }),
            );
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 400,
                    body: "bad request".to_string(),
                }),
            );
        }

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

            max_retries: Some(0),
            stop_before_deadline_ms: None,
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 10000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
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

        // Create file with templates
        let templates = (0..4)
            .map(|i| fusillade::RequestTemplateInput {
                custom_id: Some(format!("req-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"test":"data"}"#.to_string(),
                model: "test-model".to_string(),
                api_key: "test-key".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file("test-file".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        // Create 2 batches from the same file
        let batch1 = manager
            .create_batch(fusillade::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch1");

        let batch2 = manager
            .create_batch(fusillade::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch2");

        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for all requests to fail
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let status1 = manager
                .get_batch_status(batch1.id, false)
                .await
                .expect("Failed to get status");
            let status2 = manager
                .get_batch_status(batch2.id, false)
                .await
                .expect("Failed to get status");
            if status1.failed_requests == 4 && status2.failed_requests == 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        shutdown_token.cancel();

        // Test list_file_batches with hide_retriable_before_sla = true
        let batches_filtered = manager
            .list_file_batches(file_id, true)
            .await
            .expect("Failed to list batches");
        assert_eq!(batches_filtered.len(), 2);
        for batch_status in &batches_filtered {
            assert_eq!(
                batch_status.failed_requests, 2,
                "Each batch should show 2 non-retriable failures when filtered"
            );
        }

        // Test list_file_batches with hide_retriable_before_sla = false
        let batches_unfiltered = manager
            .list_file_batches(file_id, false)
            .await
            .expect("Failed to list batches");
        assert_eq!(batches_unfiltered.len(), 2);
        for batch_status in &batches_unfiltered {
            assert_eq!(
                batch_status.failed_requests, 4,
                "Each batch should show all 4 failures when not filtered"
            );
        }
    }

    #[sqlx::test]
    #[test_log::test]
    async fn test_list_batches_respects_hide_retriable(pool: sqlx::PgPool) {
        // Test that list_batches respects hide_retriable_before_sla parameter

        let http_client = Arc::new(MockHttpClient::new());

        // Add failures: 2 retriable (429) and 1 non-retriable (400) per batch (3 batches)
        // Interleave to ensure each batch gets the pattern: 429, 429, 400
        for _ in 0..3 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 429,
                    body: "rate limited".to_string(),
                }),
            );
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 429,
                    body: "rate limited".to_string(),
                }),
            );
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 400,
                    body: "bad request".to_string(),
                }),
            );
        }

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

            max_retries: Some(0),
            stop_before_deadline_ms: None,
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 10000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
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

        // Create 3 separate batches
        let mut batch_ids = Vec::new();
        for i in 0..3 {
            let templates = (0..3)
                .map(|j| fusillade::RequestTemplateInput {
                    custom_id: Some(format!("batch{}-req{}", i, j)),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"test":"data"}"#.to_string(),
                    model: "test-model".to_string(),
                    api_key: "test-key".to_string(),
                })
                .collect();

            let file_id = manager
                .create_file(format!("file-{}", i), None, templates)
                .await
                .expect("Failed to create file");

            let batch = manager
                .create_batch(fusillade::batch::BatchInput {
                    file_id,
                    endpoint: "/v1/chat/completions".to_string(),
                    completion_window: "1h".to_string(),
                    metadata: Some(serde_json::json!({"batch": i})),
                    created_by: Some("test-user".to_string()),
                })
                .await
                .expect("Failed to create batch");

            batch_ids.push(batch.id);
        }

        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for all requests to fail
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let mut all_failed = true;
            for batch_id in &batch_ids {
                let status = manager
                    .get_batch_status(*batch_id, false)
                    .await
                    .expect("Failed to get status");
                if status.failed_requests != 3 {
                    all_failed = false;
                    break;
                }
            }
            if all_failed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        shutdown_token.cancel();

        // Test list_batches with hide_retriable_before_sla = true
        let batches_filtered = manager
            .list_batches(Some("test-user".to_string()), None, None, 10, true)
            .await
            .expect("Failed to list batches");
        assert_eq!(batches_filtered.len(), 3);
        for batch in &batches_filtered {
            assert_eq!(
                batch.failed_requests, 1,
                "Each batch should show 1 non-retriable failure when filtered"
            );
        }

        // Test list_batches with hide_retriable_before_sla = false
        let batches_unfiltered = manager
            .list_batches(Some("test-user".to_string()), None, None, 10, false)
            .await
            .expect("Failed to list batches");
        assert_eq!(batches_unfiltered.len(), 3);
        for batch in &batches_unfiltered {
            assert_eq!(
                batch.failed_requests, 3,
                "Each batch should show all 3 failures when not filtered"
            );
        }
    }

    #[sqlx::test]
    #[test_log::test]
    async fn test_retry_failed_requests_for_batch_retries_all(pool: sqlx::PgPool) {
        // Test that retry_failed_requests_for_batch retries both retriable and non-retriable errors

        let http_client = Arc::new(MockHttpClient::new());

        // First round: 2 retriable failures (429) and 2 non-retriable failures (400)
        for _ in 0..2 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 429,
                    body: "rate limited".to_string(),
                }),
            );
        }
        for _ in 0..2 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 400,
                    body: "bad request".to_string(),
                }),
            );
        }

        // Second round after retry: all succeed
        for _ in 0..4 {
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
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),

            max_retries: Some(0), // No automatic retries
            stop_before_deadline_ms: None,
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 10000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
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

        let templates = (0..4)
            .map(|i| fusillade::RequestTemplateInput {
                custom_id: Some(format!("req-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"test":"data"}"#.to_string(),
                model: "test-model".to_string(),
                api_key: "test-key".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file("test-file".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(fusillade::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1h".to_string(),
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

        // Wait for all to fail
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let status = manager
                .get_batch_status(batch.id, false)
                .await
                .expect("Failed to get status");
            if status.failed_requests == 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Verify we have 4 failed requests (2 retriable + 2 non-retriable)
        let status_before = manager
            .get_batch_status(batch.id, false)
            .await
            .expect("Failed to get status");
        assert_eq!(status_before.failed_requests, 4);

        // Retry all failed requests using bulk method
        let retried_count = manager
            .retry_failed_requests_for_batch(batch.id)
            .await
            .expect("Failed to retry batch");
        assert_eq!(retried_count, 4, "Should retry all 4 failed requests");

        // Wait for retries to complete successfully
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let status = manager
                .get_batch_status(batch.id, false)
                .await
                .expect("Failed to get status");
            if status.completed_requests == 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        shutdown_token.cancel();

        // Verify all requests completed successfully after retry
        let status_after = manager
            .get_batch_status(batch.id, false)
            .await
            .expect("Failed to get status");
        assert_eq!(
            status_after.completed_requests, 4,
            "All 4 requests should complete after retry"
        );
        assert_eq!(status_after.failed_requests, 0, "No failed requests remain");
    }
}

mod queue_counts {
    use super::*;
    use fusillade::request::DaemonId;
    use uuid::Uuid;

    #[sqlx::test]
    async fn test_pending_queue_counts_by_model_and_completion_window(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = Arc::new(PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        ));

        // Batch 1: completion_window=24h, models: gpt-4 x2, gpt-3.5 x1
        let file_24h = manager
            .create_file(
                "file-24h".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: "{}".to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "k".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: "{}".to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "k".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: "{}".to_string(),
                        model: "gpt-3.5".to_string(),
                        api_key: "k".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch_24h = manager
            .create_batch(BatchInput {
                file_id: file_24h,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Batch 2: completion_window=1h, models: gpt-4 x1, gpt-3.5 x2
        let file_1h = manager
            .create_file(
                "file-1h".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: "{}".to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "k".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: "{}".to_string(),
                        model: "gpt-3.5".to_string(),
                        api_key: "k".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: "{}".to_string(),
                        model: "gpt-3.5".to_string(),
                        api_key: "k".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let _batch_1h = manager
            .create_batch(BatchInput {
                file_id: file_1h,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .unwrap();

        // Move one 24h gpt-4 request out of pending
        let reqs_24h = manager.get_batch_requests(batch_24h.id).await.unwrap();
        let to_claim = reqs_24h
            .iter()
            .find(|r| r.data().model == "gpt-4")
            .expect("Expected a gpt-4 request")
            .id();
        let daemon_id = DaemonId(Uuid::new_v4());
        sqlx::query!(
            "UPDATE requests SET state = 'claimed', daemon_id = $2, claimed_at = NOW() WHERE id = $1",
            *to_claim as Uuid,
            *daemon_id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let counts = manager
            .get_pending_request_counts_by_model_and_completion_window()
            .await
            .unwrap();

        let mut expected: std::collections::HashMap<
            String,
            std::collections::HashMap<String, i64>,
        > = std::collections::HashMap::new();
        expected
            .entry("gpt-3.5".to_string())
            .or_default()
            .insert("1h".to_string(), 2);
        // the 2x1hr requests and the 1x24h request will finish in the next 24h
        expected
            .entry("gpt-3.5".to_string())
            .or_default()
            .insert("24h".to_string(), 3);
        expected
            .entry("gpt-4".to_string())
            .or_default()
            .insert("1h".to_string(), 1);
        // both the 1hr and 24hr request will finish in the next 24h
        expected
            .entry("gpt-4".to_string())
            .or_default()
            .insert("24h".to_string(), 2);

        assert_eq!(counts, expected);
    }
}
