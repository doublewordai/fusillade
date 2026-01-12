use fusillade::batch::{BatchInput, RequestTemplateInput};
use fusillade::daemon::{
    DaemonConfig, PriorityEndpointConfig, SlaAction, SlaThreshold, default_should_retry,
};
use fusillade::http::{HttpResponse, MockHttpClient};
use fusillade::manager::postgres::PostgresRequestManager;
use fusillade::manager::{DaemonExecutor, Storage};
use fusillade::request::{AnyRequest, RequestStateFilter};
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
        PostgresRequestManager::with_client(pool.clone(), http_client.clone()).with_config(config),
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
        PostgresRequestManager::with_client(pool.clone(), http_client.clone()).with_config(config),
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
        PostgresRequestManager::with_client(pool.clone(), http_client.clone()).with_config(config),
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
        PostgresRequestManager::with_client(pool.clone(), http_client.clone()).with_config(config),
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
        PostgresRequestManager::with_client(pool.clone(), http_client.clone()).with_config(config),
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

        if let Some(Ok(req)) = res.first() {
            if req.is_terminal() {
                results = Some(res);
                break;
            }
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
        PostgresRequestManager::with_client(pool.clone(), http_client.clone()).with_config(config),
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

        if let Some(Ok(req)) = res.first() {
            if req.is_terminal() {
                results = Some(res);
                break;
            }
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

mod sla {
    use super::*;

    /// Helper function to run SLA escalation end-to-end test with configurable response delays.
    ///
    /// # Arguments
    /// * `pool` - Database connection pool
    /// * `original_delay_ms` - Delay before original request completes
    /// * `escalated_delay_ms` - Delay before escalated request completes
    /// * `expected_winner` - "original" or "escalated"
    async fn run_sla_escalation_race_test(
        pool: sqlx::PgPool,
        original_delay_ms: u64,
        escalated_delay_ms: u64,
        expected_winner: &str,
    ) {
        // Setup: Create HTTP client with triggered responses to control timing
        let http_client = Arc::new(MockHttpClient::new());

        // Triggered response for priority endpoint (escalated request)
        let escalated_trigger = http_client.add_response_with_trigger(
            "POST /priority/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"priority_success"}"#.to_string(),
            }),
        );

        // Triggered response for regular endpoint (original request)
        let original_trigger = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"regular_success"}"#.to_string(),
            }),
        );

        // Spawn tasks to trigger responses with specified delays
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(original_delay_ms)).await;
            let _ = original_trigger.send(());
        });

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(escalated_delay_ms)).await;
            let _ = escalated_trigger.send(());
        });

        // Setup: Configure daemon with SLA escalation
        let priority_endpoints = Arc::new(dashmap::DashMap::new());
        priority_endpoints.insert(
            "gpt-4".to_string(),
            PriorityEndpointConfig {
                endpoint: "https://priority.openai.com".to_string(),
                api_key: None, // Use original API key
                path_override: Some("/priority/test".to_string()),
                model_override: None,
            },
        );

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10, // Very fast for testing
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
            max_retries: Some(3),
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 100,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 1000,
            processing_timeout_ms: 5000,
            cancellation_poll_interval_ms: 10,
            sla_check_interval_seconds: 1, // 1 second for fast testing
            sla_thresholds: vec![SlaThreshold {
                name: "test-escalation".to_string(),
                threshold_seconds: 3600, // 1 hour - we'll manipulate DB to make batch at-risk
                action: SlaAction::Escalate,
                allowed_states: vec![RequestStateFilter::Pending],
            }],
            priority_endpoints,
            stop_before_deadline_ms: None,
            batch_metadata_fields: vec![],
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(pool.clone(), http_client.clone())
                .with_config(config),
        );

        // Setup: Create batch that will be at-risk
        let file_id = manager
            .create_file(
                "escalation-test".to_string(),
                Some("SLA escalation test".to_string()),
                vec![RequestTemplateInput {
                    custom_id: Some("original-req".to_string()),
                    endpoint: "https://api.openai.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"model":"gpt-4","prompt":"test"}"#.to_string(),
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
                completion_window: "2h".to_string(), // 2 hour window
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        // Manipulate DB: Make batch at-risk by backdating created_at
        // Batch created 90 minutes ago, expires in 30 minutes -> at risk for 1hr threshold
        sqlx::query!(
            r#"
            UPDATE batches
            SET created_at = NOW() - INTERVAL '90 minutes',
                expires_at = NOW() + INTERVAL '30 minutes'
            WHERE id = $1
            "#,
            *batch.id as sqlx::types::Uuid
        )
        .execute(&pool)
        .await
        .expect("Failed to backdate batch");

        // Get the original request ID
        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get batch requests");
        assert_eq!(requests.len(), 1);
        let original_id = requests[0].id();

        // Start the daemon (which will also start SLA checking)
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Poll for escalation to be created
        let start = tokio::time::Instant::now();
        let all_requests = loop {
            if start.elapsed() > Duration::from_secs(5) {
                panic!("Timeout waiting for escalation to be created");
            }

            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");

            if requests.len() == 2 {
                break requests;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        // Should have 2 requests now: original + escalated
        assert_eq!(
            all_requests.len(),
            2,
            "Should have original + escalated request"
        );

        // Find the escalated request (could be in any state: pending, claimed, processing, etc.)
        let (escalated_req, escalated_data) = all_requests
            .iter()
            .find_map(|r| {
                let data = match r {
                    AnyRequest::Pending(req) => &req.data,
                    AnyRequest::Claimed(req) => &req.data,
                    AnyRequest::Processing(req) => &req.data,
                    AnyRequest::Completed(req) => &req.data,
                    AnyRequest::Failed(req) => &req.data,
                    AnyRequest::Canceled(req) => &req.data,
                    AnyRequest::Superseded(req) => &req.data,
                };
                if data.is_escalated {
                    Some((r, data))
                } else {
                    None
                }
            })
            .expect("Should find escalated request");

        let escalated_id = escalated_req.id();

        // Verify: Escalated request has correct properties
        assert!(
            escalated_data.is_escalated,
            "Escalated request should have is_escalated=true"
        );
        assert_eq!(
            escalated_data.escalated_from_request_id,
            Some(original_id),
            "Escalated should link to original"
        );
        assert_eq!(escalated_data.model, "gpt-4");
        // Note: endpoint doesn't change in DB - daemon uses priority_endpoints config at runtime

        // Wait for the original request to complete
        let start = tokio::time::Instant::now();
        let timeout = Duration::from_secs(3);
        let mut original_completed = false;

        while start.elapsed() < timeout {
            let results = manager
                .get_requests(vec![original_id])
                .await
                .expect("Failed to get original request");

            if let Some(Ok(req)) = results.first()
                && req.is_terminal()
            {
                original_completed = true;
                break;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(original_completed, "Original request should have completed");

        // Check DB state directly to verify supersession happened
        println!("\n=== DB CHECK: State immediately after original completed ===");
        let db_check = sqlx::query!(
            r#"
            SELECT id, state, superseded_at, superseded_by_request_id
            FROM requests
            WHERE id = ANY($1)
            ORDER BY is_escalated
            "#,
            &[*original_id, *escalated_id] as &[uuid::Uuid]
        )
        .fetch_all(&pool)
        .await
        .expect("Failed to query DB");

        for row in &db_check {
            println!(
                "Request {:?}: state={}, superseded_at={:?}, superseded_by={:?}",
                row.id, row.state, row.superseded_at, row.superseded_by_request_id
            );
        }

        // Get daemon's view of request states (from storage layer)
        println!("\n=== DAEMON VIEW: State from manager.get_requests() ===");
        let daemon_view = manager
            .get_requests(vec![original_id, escalated_id])
            .await
            .expect("Failed to get daemon view");

        for (i, result) in daemon_view.iter().enumerate() {
            let id = if i == 0 { original_id } else { escalated_id };
            println!(
                "Request {:?}: variant={:?}",
                id,
                result.as_ref().unwrap().variant()
            );
        }
        println!("===\n");

        // Stop the daemon
        shutdown_token.cancel();

        // Get final states of both requests
        let final_results = manager
            .get_requests(vec![original_id, escalated_id])
            .await
            .expect("Failed to get final request states");

        let original_final = final_results[0].as_ref().unwrap();
        let escalated_final = final_results[1].as_ref().unwrap();

        println!("\n=== FINAL STATES ===");
        println!("Original: {:?}", original_final.variant());
        println!("Escalated: {:?}", escalated_final.variant());

        // Extract data from both requests
        let original_data = original_final.data();
        let escalated_data = escalated_final.data();

        // Determine who won based on expected_winner
        let (winner_id, winner_variant, _loser_id, loser_data) = if expected_winner == "original" {
            (
                original_id,
                original_final.variant(),
                escalated_id,
                escalated_data,
            )
        } else {
            (
                escalated_id,
                escalated_final.variant(),
                original_id,
                original_data,
            )
        };

        // Verify: Winner is completed with 200 response
        assert_eq!(
            winner_variant, "Completed",
            "Expected {} (winner) to be Completed, got {:?}",
            expected_winner, winner_variant
        );

        // Verify: Loser is superseded
        assert_eq!(
            loser_data.superseded_by_request_id,
            Some(winner_id),
            "Loser should be superseded by winner"
        );
        assert!(
            loser_data.superseded_at.is_some(),
            "Loser should have superseded_at timestamp"
        );

        // Verify: HTTP client called (1 or 2 times depending on race timing)
        // At least 1 call for the winner
        assert!(
            http_client.call_count() >= 1,
            "HTTP client should have been called at least once"
        );

        // Verify: Batch progress counts only the winner (escalated requests don't count)
        let batch_status = manager
            .get_batch_status(batch.id)
            .await
            .expect("Failed to get batch status");

        // Batch should show 1/1 completed (not 2/2)
        assert_eq!(
            batch_status.total_requests, 1,
            "Batch should count only non-escalated requests"
        );
        assert_eq!(
            batch_status.completed_requests, 1,
            "Batch should show winner as completed"
        );
    }

    #[sqlx::test]
    async fn test_daemon_sla_escalation_original_wins(pool: sqlx::PgPool) {
        // Original request completes quickly (50ms), escalated request is slower (300ms)
        run_sla_escalation_race_test(pool, 50, 300, "original").await;
    }

    #[sqlx::test]
    async fn test_daemon_sla_escalation_escalated_wins(pool: sqlx::PgPool) {
        // Escalated request completes quickly (50ms), original request is slower (300ms)
        run_sla_escalation_race_test(pool, 300, 50, "escalated").await;
    }

    #[sqlx::test]
    async fn test_sla_escalation_no_priority_endpoint(pool: sqlx::PgPool) {
        // Test: Escalate action configured but NO priority endpoints
        // Expected: No escalations created, original request completes normally
        let http_client = Arc::new(MockHttpClient::new());

        // Only one response needed (original request only)
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"success"}"#.to_string(),
            }),
        );

        // NO priority_endpoints configured
        let priority_endpoints = Arc::new(dashmap::DashMap::new());

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
            max_retries: Some(3),
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 100,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 1000,
            processing_timeout_ms: 5000,
            cancellation_poll_interval_ms: 10,
            sla_check_interval_seconds: 1,
            sla_thresholds: vec![SlaThreshold {
                name: "test-escalation".to_string(),
                threshold_seconds: 3600,
                action: SlaAction::Escalate,
                allowed_states: vec![RequestStateFilter::Pending],
            }],
            priority_endpoints,
            stop_before_deadline_ms: None,
            batch_metadata_fields: vec![],
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(pool.clone(), http_client.clone())
                .with_config(config),
        );

        let file_id = manager
            .create_file(
                "no-priority-endpoint".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("original".to_string()),
                    endpoint: "https://api.openai.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"model":"gpt-4","prompt":"test"}"#.to_string(),
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
                completion_window: "2h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        // Backdate batch to make it at-risk
        sqlx::query!(
            r#"UPDATE batches SET created_at = NOW() - INTERVAL '90 minutes', expires_at = NOW() + INTERVAL '30 minutes' WHERE id = $1"#,
            *batch.id as sqlx::types::Uuid
        )
        .execute(&pool)
        .await
        .expect("Failed to backdate batch");

        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Poll to verify no escalations are created (SLA checker runs but creates nothing)
        let start = tokio::time::Instant::now();
        let check_duration = Duration::from_millis(1500);

        while start.elapsed() < check_duration {
            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");

            assert_eq!(
                requests.len(),
                1,
                "Should not create escalations when no priority endpoint is configured"
            );

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let all_requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get requests");

        // Verify: No escalations created
        assert_eq!(
            all_requests.len(),
            1,
            "Should only have original request, no escalation"
        );
        assert!(
            !all_requests[0].data().is_escalated,
            "Request should not be escalated"
        );

        let original_id = all_requests[0].id();

        // Wait for completion
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            let results = manager
                .get_requests(vec![original_id])
                .await
                .expect("Failed to get request");
            if results.first().unwrap().as_ref().unwrap().is_terminal() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_token.cancel();

        let final_result = manager
            .get_requests(vec![original_id])
            .await
            .expect("Failed to get final state");

        // Verify: Original completes normally
        assert_eq!(
            final_result[0].as_ref().unwrap().variant(),
            "Completed",
            "Original should complete normally"
        );

        // Verify batch status
        let batch_status = manager
            .get_batch_status(batch.id)
            .await
            .expect("Failed to get batch status");
        assert_eq!(batch_status.total_requests, 1);
        assert_eq!(batch_status.completed_requests, 1);
    }

    #[sqlx::test]
    async fn test_sla_escalation_both_fail(pool: sqlx::PgPool) {
        // Test: Both original and escalated fail with 500
        // Expected: Both in Failed state, neither supersedes the other
        let http_client = Arc::new(MockHttpClient::new());

        // Both endpoints return 500
        http_client.add_response(
            "POST /priority/test",
            Ok(HttpResponse {
                status: 500,
                body: r#"{"error":"server error"}"#.to_string(),
            }),
        );
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 500,
                body: r#"{"error":"server error"}"#.to_string(),
            }),
        );

        let priority_endpoints = Arc::new(dashmap::DashMap::new());
        priority_endpoints.insert(
            "gpt-4".to_string(),
            PriorityEndpointConfig {
                endpoint: "https://priority.openai.com".to_string(),
                api_key: None,
                path_override: Some("/priority/test".to_string()),
                model_override: None,
            },
        );

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
            max_retries: Some(0), // No retries for this test
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 100,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 1000,
            processing_timeout_ms: 5000,
            cancellation_poll_interval_ms: 10,
            sla_check_interval_seconds: 1,
            sla_thresholds: vec![SlaThreshold {
                name: "test-escalation".to_string(),
                threshold_seconds: 3600,
                action: SlaAction::Escalate,
                allowed_states: vec![RequestStateFilter::Pending],
            }],
            priority_endpoints,
            stop_before_deadline_ms: None,
            batch_metadata_fields: vec![],
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(pool.clone(), http_client.clone())
                .with_config(config),
        );

        let file_id = manager
            .create_file(
                "both-fail".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("original".to_string()),
                    endpoint: "https://api.openai.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"model":"gpt-4","prompt":"test"}"#.to_string(),
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
                completion_window: "2h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        sqlx::query!(
            r#"UPDATE batches SET created_at = NOW() - INTERVAL '90 minutes', expires_at = NOW() + INTERVAL '30 minutes' WHERE id = $1"#,
            *batch.id as sqlx::types::Uuid
        )
        .execute(&pool)
        .await
        .expect("Failed to backdate batch");

        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get requests");
        let original_id = requests[0].id();

        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Poll for escalation to be created
        let start = tokio::time::Instant::now();
        let all_requests = loop {
            if start.elapsed() > Duration::from_secs(5) {
                panic!("Timeout waiting for escalation to be created");
            }

            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");

            if requests.len() == 2 {
                break requests;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        assert_eq!(all_requests.len(), 2, "Should have original + escalated");

        let escalated_id = all_requests
            .iter()
            .find(|r| r.data().is_escalated)
            .expect("Should find escalated")
            .id();

        // Wait for both to fail
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            let results = manager
                .get_requests(vec![original_id, escalated_id])
                .await
                .expect("Failed to get requests");
            if results.iter().all(|r| r.as_ref().unwrap().is_terminal()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_token.cancel();

        let final_results = manager
            .get_requests(vec![original_id, escalated_id])
            .await
            .expect("Failed to get final states");

        println!(
            "Original: {:?}",
            final_results[0].as_ref().unwrap().variant()
        );
        println!(
            "Escalated: {:?}",
            final_results[1].as_ref().unwrap().variant()
        );

        // Both should be Failed
        assert_eq!(
            final_results[0].as_ref().unwrap().variant(),
            "Failed",
            "Original should be Failed"
        );
        assert_eq!(
            final_results[1].as_ref().unwrap().variant(),
            "Failed",
            "Escalated should be Failed"
        );

        // Batch should show 0/1 completed, 1/1 failed
        let batch_status = manager
            .get_batch_status(batch.id)
            .await
            .expect("Failed to get batch status");
        assert_eq!(
            batch_status.completed_requests, 0,
            "Should have 0 completed"
        );
        assert_eq!(batch_status.failed_requests, 1, "Should have 1 failed");
    }

    #[sqlx::test]
    async fn test_sla_escalation_escalated_fails_original_wins(pool: sqlx::PgPool) {
        // Test: Escalated gets 500, original gets 200
        // Expected: Original completes, escalated fails
        let http_client = Arc::new(MockHttpClient::new());

        let escalated_trigger = http_client.add_response_with_trigger(
            "POST /priority/test",
            Ok(HttpResponse {
                status: 500,
                body: r#"{"error":"server error"}"#.to_string(),
            }),
        );
        let original_trigger = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"success"}"#.to_string(),
            }),
        );

        // Original completes first
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = original_trigger.send(());
        });
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = escalated_trigger.send(());
        });

        let priority_endpoints = Arc::new(dashmap::DashMap::new());
        priority_endpoints.insert(
            "gpt-4".to_string(),
            PriorityEndpointConfig {
                endpoint: "https://priority.openai.com".to_string(),
                api_key: None,
                path_override: Some("/priority/test".to_string()),
                model_override: None,
            },
        );

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
            max_retries: Some(0),
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 100,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 1000,
            processing_timeout_ms: 5000,
            cancellation_poll_interval_ms: 10,
            sla_check_interval_seconds: 1,
            sla_thresholds: vec![SlaThreshold {
                name: "test-escalation".to_string(),
                threshold_seconds: 3600,
                action: SlaAction::Escalate,
                allowed_states: vec![RequestStateFilter::Pending],
            }],
            priority_endpoints,
            stop_before_deadline_ms: None,
            batch_metadata_fields: vec![],
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(pool.clone(), http_client.clone())
                .with_config(config),
        );

        let file_id = manager
            .create_file(
                "escalated-fails".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("original".to_string()),
                    endpoint: "https://api.openai.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"model":"gpt-4","prompt":"test"}"#.to_string(),
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
                completion_window: "2h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        sqlx::query!(
            r#"UPDATE batches SET created_at = NOW() - INTERVAL '90 minutes', expires_at = NOW() + INTERVAL '30 minutes' WHERE id = $1"#,
            *batch.id as sqlx::types::Uuid
        )
        .execute(&pool)
        .await
        .expect("Failed to backdate batch");

        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get requests");
        let original_id = requests[0].id();

        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Poll for escalation to be created
        let start = tokio::time::Instant::now();
        let all_requests = loop {
            if start.elapsed() > Duration::from_secs(5) {
                panic!("Timeout waiting for escalation to be created");
            }

            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");

            if requests.len() == 2 {
                break requests;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        let escalated_id = all_requests
            .iter()
            .find(|r| r.data().is_escalated)
            .expect("Should find escalated")
            .id();

        // Wait for completion
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            let results = manager
                .get_requests(vec![original_id])
                .await
                .expect("Failed to get request");
            if results.first().unwrap().as_ref().unwrap().is_terminal() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_token.cancel();

        let final_results = manager
            .get_requests(vec![original_id, escalated_id])
            .await
            .expect("Failed to get final states");

        println!(
            "Original: {:?}",
            final_results[0].as_ref().unwrap().variant()
        );
        println!(
            "Escalated: {:?}",
            final_results[1].as_ref().unwrap().variant()
        );

        // Original wins, escalated gets superseded
        assert_eq!(final_results[0].as_ref().unwrap().variant(), "Completed");
        assert_eq!(final_results[1].as_ref().unwrap().variant(), "Superseded");

        // Batch shows 1/1 completed
        let batch_status = manager
            .get_batch_status(batch.id)
            .await
            .expect("Failed to get batch status");
        assert_eq!(batch_status.completed_requests, 1);
        assert_eq!(batch_status.failed_requests, 0);
    }

    #[sqlx::test]
    async fn test_sla_escalation_multiple_requests_batch(pool: sqlx::PgPool) {
        // Test: 3 separate batches, only 1 is at-risk
        // Expected: Only the at-risk batch gets its request escalated
        let http_client = Arc::new(MockHttpClient::new());

        // Responses for 3 batches + 1 escalated request = 4 total
        for _ in 0..3 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 200,
                    body: r#"{"result":"success"}"#.to_string(),
                }),
            );
        }
        http_client.add_response(
            "POST /priority/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"priority_success"}"#.to_string(),
            }),
        );

        let priority_endpoints = Arc::new(dashmap::DashMap::new());
        priority_endpoints.insert(
            "gpt-4".to_string(),
            PriorityEndpointConfig {
                endpoint: "https://priority.openai.com".to_string(),
                api_key: None,
                path_override: Some("/priority/test".to_string()),
                model_override: None,
            },
        );

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
            max_retries: Some(3),
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 100,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 1000,
            processing_timeout_ms: 5000,
            cancellation_poll_interval_ms: 10,
            sla_check_interval_seconds: 1,
            sla_thresholds: vec![SlaThreshold {
                name: "test-escalation".to_string(),
                threshold_seconds: 3600,
                action: SlaAction::Escalate,
                allowed_states: vec![RequestStateFilter::Pending],
            }],
            priority_endpoints,
            stop_before_deadline_ms: None,
            batch_metadata_fields: vec![],
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(pool.clone(), http_client.clone())
                .with_config(config),
        );

        // Create 1 file that we'll reuse for all 3 batches
        let file_id = manager
            .create_file(
                "multiple-batches".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("req".to_string()),
                    endpoint: "https://api.openai.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"model":"gpt-4","prompt":"test"}"#.to_string(),
                    model: "gpt-4".to_string(),
                    api_key: "test-key".to_string(),
                }],
            )
            .await
            .expect("Failed to create file");

        // Create 3 separate batches from the same file
        let batch1 = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "2h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch1");

        let batch2 = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "2h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch2");

        let batch3 = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "2h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch3");

        // Only backdate batch1 to make it at-risk
        sqlx::query!(
            r#"UPDATE batches SET created_at = NOW() - INTERVAL '90 minutes', expires_at = NOW() + INTERVAL '30 minutes' WHERE id = $1"#,
            *batch1.id as sqlx::types::Uuid
        )
        .execute(&pool)
        .await
        .expect("Failed to backdate batch1");

        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Poll until escalation is created in batch1
        let start = tokio::time::Instant::now();
        let (batch1_requests, batch2_requests, batch3_requests) = loop {
            if start.elapsed() > Duration::from_secs(5) {
                panic!("Timeout waiting for escalation to be created in batch1");
            }

            let b1 = manager
                .get_batch_requests(batch1.id)
                .await
                .expect("Failed to get batch1 requests");
            let b2 = manager
                .get_batch_requests(batch2.id)
                .await
                .expect("Failed to get batch2 requests");
            let b3 = manager
                .get_batch_requests(batch3.id)
                .await
                .expect("Failed to get batch3 requests");

            let b1_escalated = b1.iter().filter(|r| r.data().is_escalated).count();

            if b1_escalated > 0 {
                break (b1, b2, b3);
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        let escalated_count = batch1_requests
            .iter()
            .filter(|r| r.data().is_escalated)
            .count()
            + batch2_requests
                .iter()
                .filter(|r| r.data().is_escalated)
                .count()
            + batch3_requests
                .iter()
                .filter(|r| r.data().is_escalated)
                .count();

        assert_eq!(
            escalated_count, 1,
            "Should only escalate 1 at-risk request (from batch1)"
        );
        assert_eq!(
            batch1_requests.len(),
            2,
            "Batch1 should have original + escalated"
        );
        assert_eq!(batch2_requests.len(), 1, "Batch2 should only have original");
        assert_eq!(batch3_requests.len(), 1, "Batch3 should only have original");

        // Wait for all batches to complete
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            let status1 = manager
                .get_batch_status(batch1.id)
                .await
                .expect("Failed to get batch1 status");
            let status2 = manager
                .get_batch_status(batch2.id)
                .await
                .expect("Failed to get batch2 status");
            let status3 = manager
                .get_batch_status(batch3.id)
                .await
                .expect("Failed to get batch3 status");
            if status1.completed_requests == 1
                && status2.completed_requests == 1
                && status3.completed_requests == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_token.cancel();

        // All 3 batches should show 1/1 completed
        let batch1_status = manager
            .get_batch_status(batch1.id)
            .await
            .expect("Failed to get batch1 status");
        let batch2_status = manager
            .get_batch_status(batch2.id)
            .await
            .expect("Failed to get batch2 status");
        let batch3_status = manager
            .get_batch_status(batch3.id)
            .await
            .expect("Failed to get batch3 status");

        assert_eq!(batch1_status.total_requests, 1);
        assert_eq!(batch1_status.completed_requests, 1);
        assert_eq!(batch2_status.total_requests, 1);
        assert_eq!(batch2_status.completed_requests, 1);
        assert_eq!(batch3_status.total_requests, 1);
        assert_eq!(batch3_status.completed_requests, 1);
    }

    #[sqlx::test]
    async fn test_sla_escalation_multiple_requests_in_batch(pool: sqlx::PgPool) {
        // Test: Single batch with 3 requests, all at-risk
        // Expected: All 3 get escalated, creating 6 total requests (3 original + 3 escalated)
        let http_client = Arc::new(MockHttpClient::new());

        // Use triggers to control timing - escalated requests complete faster (50ms)
        // Original requests complete slower (300ms)
        let triggers: Vec<_> = (0..3)
            .map(|_| {
                let original_trigger = http_client.add_response_with_trigger(
                    "POST /v1/test",
                    Ok(HttpResponse {
                        status: 200,
                        body: r#"{"result":"success"}"#.to_string(),
                    }),
                );
                let escalated_trigger = http_client.add_response_with_trigger(
                    "POST /priority/test",
                    Ok(HttpResponse {
                        status: 200,
                        body: r#"{"result":"priority_success"}"#.to_string(),
                    }),
                );
                (original_trigger, escalated_trigger)
            })
            .collect();

        // Spawn tasks to trigger responses
        // Escalated complete at 50ms, originals at 300ms
        for (original_trigger, escalated_trigger) in triggers {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let _ = original_trigger.send(());
            });
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let _ = escalated_trigger.send(());
            });
        }

        let priority_endpoints = Arc::new(dashmap::DashMap::new());
        priority_endpoints.insert(
            "gpt-4".to_string(),
            PriorityEndpointConfig {
                endpoint: "https://priority.openai.com".to_string(),
                api_key: None,
                path_override: Some("/priority/test".to_string()),
                model_override: None,
            },
        );

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
            max_retries: Some(3),
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            timeout_ms: 5000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 100,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 1000,
            processing_timeout_ms: 5000,
            cancellation_poll_interval_ms: 10,
            sla_check_interval_seconds: 1,
            sla_thresholds: vec![SlaThreshold {
                name: "test-escalation".to_string(),
                threshold_seconds: 3600,
                action: SlaAction::Escalate,
                allowed_states: vec![RequestStateFilter::Pending],
            }],
            priority_endpoints,
            stop_before_deadline_ms: None,
            batch_metadata_fields: vec![],
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(pool.clone(), http_client.clone())
                .with_config(config),
        );

        // Create 1 batch with 3 requests
        let file_id = manager
            .create_file(
                "multiple-requests-in-batch".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("req1".to_string()),
                        endpoint: "https://api.openai.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"model":"gpt-4","prompt":"test1"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "test-key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req2".to_string()),
                        endpoint: "https://api.openai.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"model":"gpt-4","prompt":"test2"}"#.to_string(),
                        model: "gpt-4".to_string(),
                        api_key: "test-key".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req3".to_string()),
                        endpoint: "https://api.openai.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"model":"gpt-4","prompt":"test3"}"#.to_string(),
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
                completion_window: "2h".to_string(),
                metadata: None,
                created_by: None,
            })
            .await
            .expect("Failed to create batch");

        // Backdate batch to make all requests at-risk
        sqlx::query!(
            r#"UPDATE batches SET created_at = NOW() - INTERVAL '90 minutes', expires_at = NOW() + INTERVAL '30 minutes' WHERE id = $1"#,
            *batch.id as sqlx::types::Uuid
        )
        .execute(&pool)
        .await
        .expect("Failed to backdate batch");

        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Poll until all 3 escalations are created (3 original + 3 escalated = 6 total)
        let start = tokio::time::Instant::now();
        let all_requests = loop {
            if start.elapsed() > Duration::from_secs(5) {
                panic!("Timeout waiting for escalations to be created");
            }

            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");

            if requests.len() == 6 {
                break requests;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        // Verify: 3 original + 3 escalated = 6 total
        assert_eq!(
            all_requests.len(),
            6,
            "Should have 3 original + 3 escalated requests"
        );

        let original_count = all_requests
            .iter()
            .filter(|r| !r.data().is_escalated)
            .count();
        let escalated_count = all_requests
            .iter()
            .filter(|r| r.data().is_escalated)
            .count();

        assert_eq!(original_count, 3, "Should have 3 original requests");
        assert_eq!(escalated_count, 3, "Should have 3 escalated requests");

        // Wait for all to complete
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            let status = manager
                .get_batch_status(batch.id)
                .await
                .expect("Failed to get status");
            if status.completed_requests == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_token.cancel();

        // Verify batch status: 3/3 completed (superseded requests don't count)
        let batch_status = manager
            .get_batch_status(batch.id)
            .await
            .expect("Failed to get batch status");
        assert_eq!(batch_status.total_requests, 3);
        assert_eq!(batch_status.completed_requests, 3);

        // Verify we have 3 completed and 3 superseded
        let final_requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get final requests");
        let completed_count = final_requests
            .iter()
            .filter(|r| r.variant() == "Completed")
            .count();
        let superseded_count = final_requests
            .iter()
            .filter(|r| r.variant() == "Superseded")
            .count();

        assert_eq!(
            completed_count, 3,
            "Should have 3 completed (winners of each race)"
        );
        assert_eq!(
            superseded_count, 3,
            "Should have 3 superseded (losers of each race)"
        );
    }
}

#[sqlx::test]
async fn test_sla_escalation_model_override(pool: sqlx::PgPool) {
    // Test: Model override changes the escalated request's model
    // Expected: Escalated request has the overridden model name
    let http_client = Arc::new(MockHttpClient::new());

    // Response for original model
    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"success"}"#.to_string(),
        }),
    );

    // Response for priority model with different model name
    http_client.add_response(
        "POST /priority/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"priority_success"}"#.to_string(),
        }),
    );

    let priority_endpoints = Arc::new(dashmap::DashMap::new());
    priority_endpoints.insert(
        "gpt-4".to_string(),
        PriorityEndpointConfig {
            endpoint: "https://priority.openai.com".to_string(),
            api_key: None,
            path_override: Some("/priority/test".to_string()),
            model_override: Some("gpt-4-priority".to_string()), // Override to different model
        },
    );

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        default_model_concurrency: 10,
        model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
        max_retries: Some(3),
        backoff_ms: 100,
        backoff_factor: 2,
        max_backoff_ms: 1000,
        timeout_ms: 5000,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 1000,
        processing_timeout_ms: 5000,
        cancellation_poll_interval_ms: 100,
        sla_check_interval_seconds: 1,
        sla_thresholds: vec![SlaThreshold {
            name: "test-escalation".to_string(),
            threshold_seconds: 3600,
            action: SlaAction::Escalate,
            allowed_states: vec![RequestStateFilter::Pending],
        }],
        priority_endpoints,
        stop_before_deadline_ms: None,
        batch_metadata_fields: vec![],
    };

    let manager = Arc::new(
        PostgresRequestManager::with_client(pool.clone(), http_client.clone()).with_config(config),
    );

    let file_id = manager
        .create_file(
            "model-override-test".to_string(),
            None,
            vec![RequestTemplateInput {
                custom_id: Some("original".to_string()),
                endpoint: "https://api.openai.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"model":"gpt-4","prompt":"test"}"#.to_string(),
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
            completion_window: "2h".to_string(),
            metadata: None,
            created_by: None,
        })
        .await
        .expect("Failed to create batch");

    // Backdate batch to make it at-risk
    sqlx::query!(
            r#"UPDATE batches SET created_at = NOW() - INTERVAL '90 minutes', expires_at = NOW() + INTERVAL '30 minutes' WHERE id = $1"#,
            *batch.id as sqlx::types::Uuid
        )
        .execute(&pool)
        .await
        .expect("Failed to backdate batch");

    let shutdown_token = tokio_util::sync::CancellationToken::new();
    manager
        .clone()
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    // Poll for escalation to be created
    let start = tokio::time::Instant::now();
    let all_requests = loop {
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Timeout waiting for escalation to be created");
        }

        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get requests");

        if requests.len() == 2 {
            break requests;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Should have 2 requests: original + escalated
    assert_eq!(all_requests.len(), 2, "Should have original + escalated");

    // Find the escalated request
    let escalated = all_requests
        .iter()
        .find(|r| r.data().is_escalated)
        .expect("Should find escalated request");

    let original = all_requests
        .iter()
        .find(|r| !r.data().is_escalated)
        .expect("Should find original request");

    // Verify: Original has original model
    assert_eq!(
        original.data().model,
        "gpt-4",
        "Original should have gpt-4 model"
    );

    // Verify: Escalated keeps original model in DB (override applied at runtime)
    assert_eq!(
        escalated.data().model,
        "gpt-4",
        "Escalated should keep original model in DB (override applied at runtime)"
    );

    // Poll until one request completes (batch shows completion)
    let start = tokio::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(3) {
            panic!("Timeout waiting for batch to show completed requests");
        }

        let status = manager
            .get_batch_status(batch.id)
            .await
            .expect("Failed to get batch status");

        if status.completed_requests > 0 {
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_token.cancel();

    // Verify batch status
    let batch_status = manager
        .get_batch_status(batch.id)
        .await
        .expect("Failed to get batch status");
    assert_eq!(batch_status.total_requests, 1);

    // CRITICAL: Verify the actual HTTP requests sent to mock client
    let calls = http_client.get_calls();
    println!("\n=== HTTP CALLS MADE ===");
    for (i, call) in calls.iter().enumerate() {
        println!(
            "Call {}: {} {} - Body: {}",
            i, call.method, call.path, call.body
        );
    }
    println!("======================\n");

    // Find calls to each endpoint
    let original_calls: Vec<_> = calls.iter().filter(|c| c.path == "/v1/test").collect();
    let escalated_calls: Vec<_> = calls
        .iter()
        .filter(|c| c.path == "/priority/test")
        .collect();

    assert_eq!(
        original_calls.len(),
        1,
        "Should have 1 call to original endpoint"
    );
    assert_eq!(
        escalated_calls.len(),
        1,
        "Should have 1 call to escalated endpoint"
    );

    // Verify the original request body has gpt-4
    assert!(
        original_calls[0].body.contains(r#""model":"gpt-4""#),
        "Original request should contain model gpt-4 in body: {}",
        original_calls[0].body
    );

    // Verify the escalated request body has gpt-4-priority (model override applied)
    assert!(
        escalated_calls[0]
            .body
            .contains(r#""model":"gpt-4-priority""#),
        "Escalated request should contain model override gpt-4-priority in body: {}",
        escalated_calls[0].body
    );
}

#[sqlx::test]
async fn test_sla_escalation_uses_priority_api_key(pool: sqlx::PgPool) {
    // Test: Verify escalated requests use priority endpoint's API key when configured
    // Expected: Original uses "original-api-key", escalated uses "priority-api-key"
    let http_client = Arc::new(MockHttpClient::new());

    // Mock responses for both endpoints
    http_client.add_response(
        "POST /priority/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"priority_success"}"#.to_string(),
        }),
    );
    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"regular_success"}"#.to_string(),
        }),
    );

    // Configure priority endpoint with custom API key
    let priority_endpoints = Arc::new(dashmap::DashMap::new());
    priority_endpoints.insert(
        "gpt-4".to_string(),
        PriorityEndpointConfig {
            endpoint: "https://priority.openai.com".to_string(),
            api_key: Some("priority-api-key".to_string()), // Different API key for priority endpoint
            path_override: Some("/priority/test".to_string()),
            model_override: None,
        },
    );

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        default_model_concurrency: 10,
        model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
        max_retries: Some(0),
        backoff_ms: 100,
        backoff_factor: 2,
        max_backoff_ms: 1000,
        timeout_ms: 5000,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 100,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 1000,
        processing_timeout_ms: 5000,
        cancellation_poll_interval_ms: 10,
        sla_check_interval_seconds: 1,
        sla_thresholds: vec![SlaThreshold {
            name: "test-api-key".to_string(),
            threshold_seconds: 3600,
            action: SlaAction::Escalate,
            allowed_states: vec![RequestStateFilter::Pending],
        }],
        priority_endpoints,
        stop_before_deadline_ms: None,
        batch_metadata_fields: vec![],
    };

    let manager = Arc::new(
        PostgresRequestManager::with_client(pool.clone(), http_client.clone()).with_config(config),
    );

    // Create batch with original API key
    let file_id = manager
        .create_file(
            "api-key-test".to_string(),
            None,
            vec![RequestTemplateInput {
                custom_id: Some("original".to_string()),
                endpoint: "https://api.openai.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"model":"gpt-4","prompt":"test"}"#.to_string(),
                model: "gpt-4".to_string(),
                api_key: "original-api-key".to_string(), // Original API key
            }],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "2h".to_string(),
            metadata: None,
            created_by: None,
        })
        .await
        .expect("Failed to create batch");

    // Make batch at-risk
    sqlx::query!(
        r#"UPDATE batches SET created_at = NOW() - INTERVAL '90 minutes', expires_at = NOW() + INTERVAL '30 minutes' WHERE id = $1"#,
        *batch.id as sqlx::types::Uuid
    )
    .execute(&pool)
    .await
    .expect("Failed to backdate batch");

    let requests = manager
        .get_batch_requests(batch.id)
        .await
        .expect("Failed to get requests");
    let original_id = requests[0].id();

    // Start daemon
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    manager
        .clone()
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    // Poll for escalation to be created
    let start = tokio::time::Instant::now();
    let all_requests = loop {
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Timeout waiting for escalation to be created");
        }

        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get requests");

        if requests.len() == 2 {
            break requests;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(all_requests.len(), 2, "Should have original + escalated");

    let escalated_id = all_requests
        .iter()
        .find(|r| {
            let data = match r {
                AnyRequest::Pending(req) => &req.data,
                AnyRequest::Claimed(req) => &req.data,
                AnyRequest::Processing(req) => &req.data,
                AnyRequest::Completed(req) => &req.data,
                AnyRequest::Failed(req) => &req.data,
                AnyRequest::Superseded(req) => &req.data,
                AnyRequest::Canceled(req) => &req.data,
            };
            data.is_escalated
        })
        .map(|r| r.id())
        .expect("Should have escalated request");

    // Wait for both requests to complete
    let start = tokio::time::Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        let results = manager
            .get_requests(vec![original_id, escalated_id])
            .await
            .expect("Failed to get requests");
        if results.iter().any(|r| r.as_ref().unwrap().is_terminal()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_token.cancel();

    // Verify API keys used in HTTP calls
    let calls = http_client.get_calls();

    // At least one call should have been made
    assert!(
        !calls.is_empty(),
        "Expected at least one HTTP call to have been made"
    );

    // Find calls by path to identify which is which
    let original_calls: Vec<_> = calls.iter().filter(|c| c.path == "/v1/test").collect();
    let escalated_calls: Vec<_> = calls
        .iter()
        .filter(|c| c.path == "/priority/test")
        .collect();

    // Verify escalated request used priority API key
    if !escalated_calls.is_empty() {
        assert_eq!(
            escalated_calls[0].api_key, "priority-api-key",
            "Escalated request should use priority endpoint's API key"
        );
    }

    // Verify original request used original API key (if it was called)
    if !original_calls.is_empty() {
        assert_eq!(
            original_calls[0].api_key, "original-api-key",
            "Original request should use original API key"
        );
    }

    // At minimum, verify the escalated request was made with the correct API key
    assert!(
        !escalated_calls.is_empty(),
        "Expected escalated request to have been processed"
    );
}

/// Tests for `get_batch_results_stream` and `stream_batch_results`.
///
/// These tests verify that batch results correctly handle SLA escalation scenarios,
/// returning one result per template with the "winning" request's data.
mod batch_results_stream {
    use super::*;
    use futures::StreamExt;

    /// Helper to collect all results from a batch results stream
    async fn collect_batch_results(
        manager: &PostgresRequestManager<MockHttpClient>,
        batch_id: fusillade::batch::BatchId,
    ) -> Vec<fusillade::batch::BatchResultItem> {
        let stream = manager.get_batch_results_stream(batch_id, 0, None, None);
        stream
            .filter_map(|r| async { r.ok() })
            .collect::<Vec<_>>()
            .await
    }

    #[sqlx::test]
    async fn test_batch_results_no_escalation(pool: sqlx::PgPool) {
        // Test: Basic batch with no escalation returns all results
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
            PostgresRequestManager::with_client(pool.clone(), http_client.clone())
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
    async fn test_batch_results_escalated_wins(pool: sqlx::PgPool) {
        // Test: When escalated request wins, batch results show escalated request's data
        let http_client = Arc::new(MockHttpClient::new());

        // Escalated completes quickly with different response
        let escalated_trigger = http_client.add_response_with_trigger(
            "POST /priority/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"escalated_success"}"#.to_string(),
            }),
        );

        // Original is slower
        let original_trigger = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"original_success"}"#.to_string(),
            }),
        );

        // Escalated completes at 50ms, original at 300ms
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = escalated_trigger.send(());
        });
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = original_trigger.send(());
        });

        let priority_endpoints = Arc::new(dashmap::DashMap::new());
        priority_endpoints.insert(
            "gpt-4".to_string(),
            PriorityEndpointConfig {
                endpoint: "https://priority.example.com".to_string(),
                path_override: Some("/priority/test".to_string()),
                api_key: None,
                model_override: None,
            },
        );

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            priority_endpoints,
            sla_check_interval_seconds: 1,
            sla_thresholds: vec![SlaThreshold {
                name: "test-escalation".to_string(),
                threshold_seconds: 3600,
                action: SlaAction::Escalate,
                allowed_states: vec![RequestStateFilter::Pending],
            }],
            ..Default::default()
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(pool.clone(), http_client.clone())
                .with_config(config),
        );

        let file_id = manager
            .create_file(
                "escalated-wins-results".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("test-req".to_string()),
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

        // Make batch at-risk by backdating expires_at
        sqlx::query!(
            "UPDATE batches SET expires_at = NOW() + INTERVAL '30 minutes' WHERE id = $1",
            *batch.id as uuid::Uuid
        )
        .execute(&pool)
        .await
        .expect("Failed to update batch");

        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for escalation and completion
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");

            // Need 2 requests (original + escalated) with at least one terminal
            if requests.len() >= 2 && requests.iter().any(|r| r.is_terminal()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Wait a bit more for supersession to complete
        tokio::time::sleep(Duration::from_millis(400)).await;

        shutdown_token.cancel();

        // Get batch results stream
        let results = collect_batch_results(&manager, batch.id).await;

        // Should have exactly 1 result (one per template, not per request)
        assert_eq!(
            results.len(),
            1,
            "Should have 1 result per template, not duplicates from escalation"
        );

        // Result should be from the escalated request (which won)
        let result = &results[0];
        assert_eq!(result.custom_id, Some("test-req".to_string()));
        assert_eq!(
            result.status,
            fusillade::batch::BatchResultStatus::Completed
        );

        // Verify it's the escalated response
        let response_body = result.response_body.as_ref().expect("Should have response");
        assert!(
            response_body
                .as_object()
                .and_then(|o| o.get("result"))
                .and_then(|v| v.as_str())
                == Some("escalated_success"),
            "Should contain escalated request's response, got: {:?}",
            response_body
        );
    }

    #[sqlx::test]
    async fn test_batch_results_original_wins(pool: sqlx::PgPool) {
        // Test: When original request wins, batch results show original request's data
        let http_client = Arc::new(MockHttpClient::new());

        // Original completes quickly
        let original_trigger = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"original_success"}"#.to_string(),
            }),
        );

        // Escalated is slower
        let escalated_trigger = http_client.add_response_with_trigger(
            "POST /priority/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"escalated_success"}"#.to_string(),
            }),
        );

        // Original completes at 50ms, escalated at 300ms
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = original_trigger.send(());
        });
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = escalated_trigger.send(());
        });

        let priority_endpoints = Arc::new(dashmap::DashMap::new());
        priority_endpoints.insert(
            "gpt-4".to_string(),
            PriorityEndpointConfig {
                endpoint: "https://priority.example.com".to_string(),
                path_override: Some("/priority/test".to_string()),
                api_key: None,
                model_override: None,
            },
        );

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            priority_endpoints,
            sla_check_interval_seconds: 1,
            sla_thresholds: vec![SlaThreshold {
                name: "test-escalation".to_string(),
                threshold_seconds: 3600,
                action: SlaAction::Escalate,
                allowed_states: vec![RequestStateFilter::Pending],
            }],
            ..Default::default()
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(pool.clone(), http_client.clone())
                .with_config(config),
        );

        let file_id = manager
            .create_file(
                "original-wins-results".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("test-req".to_string()),
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

        // Make batch at-risk
        sqlx::query!(
            "UPDATE batches SET expires_at = NOW() + INTERVAL '30 minutes' WHERE id = $1",
            *batch.id as uuid::Uuid
        )
        .execute(&pool)
        .await
        .expect("Failed to update batch");

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

            if requests.len() >= 2 && requests.iter().any(|r| r.is_terminal()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tokio::time::sleep(Duration::from_millis(400)).await;
        shutdown_token.cancel();

        let results = collect_batch_results(&manager, batch.id).await;

        assert_eq!(results.len(), 1, "Should have 1 result per template");

        let result = &results[0];

        // Original won, so result should NOT have superseded_by set
        // and should contain original's response
        assert_eq!(
            result.status,
            fusillade::batch::BatchResultStatus::Completed
        );

        let response_body = result.response_body.as_ref().expect("Should have response");
        assert!(
            response_body
                .as_object()
                .and_then(|o| o.get("result"))
                .and_then(|v| v.as_str())
                == Some("original_success"),
            "Should contain original request's response, got: {:?}",
            response_body
        );
    }

    #[sqlx::test]
    async fn test_batch_results_multiple_templates_with_escalation(pool: sqlx::PgPool) {
        // Test: 2 templates with same model, both get escalated
        // Expected: 2 results total (one per template), each showing the winning request's data
        let http_client = Arc::new(MockHttpClient::new());

        // Use triggers for all requests to control timing precisely
        // Template 1: original wins (completes at 50ms), escalated slower (300ms)
        let original1_trigger = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"req1_original"}"#.to_string(),
            }),
        );
        let escalated1_trigger = http_client.add_response_with_trigger(
            "POST /priority/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"req1_escalated"}"#.to_string(),
            }),
        );

        // Template 2: escalated wins (completes at 50ms), original slower (300ms)
        let original2_trigger = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"req2_original"}"#.to_string(),
            }),
        );
        let escalated2_trigger = http_client.add_response_with_trigger(
            "POST /priority/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"req2_escalated"}"#.to_string(),
            }),
        );

        // Template 1: original wins
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = original1_trigger.send(());
        });
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = escalated1_trigger.send(());
        });

        // Template 2: escalated wins
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = original2_trigger.send(());
        });
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = escalated2_trigger.send(());
        });

        let priority_endpoints = Arc::new(dashmap::DashMap::new());
        priority_endpoints.insert(
            "gpt-4".to_string(),
            PriorityEndpointConfig {
                endpoint: "https://priority.example.com".to_string(),
                path_override: Some("/priority/test".to_string()),
                api_key: None,
                model_override: None,
            },
        );

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            default_model_concurrency: 10,
            priority_endpoints,
            sla_check_interval_seconds: 1,
            sla_thresholds: vec![SlaThreshold {
                name: "test-escalation".to_string(),
                threshold_seconds: 3600,
                action: SlaAction::Escalate,
                allowed_states: vec![RequestStateFilter::Pending],
            }],
            ..Default::default()
        };

        let manager = Arc::new(
            PostgresRequestManager::with_client(pool.clone(), http_client.clone())
                .with_config(config),
        );

        let file_id = manager
            .create_file(
                "multi-escalation".to_string(),
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

        // Make batch at-risk for escalation
        sqlx::query!(
            "UPDATE batches SET expires_at = NOW() + INTERVAL '30 minutes' WHERE id = $1",
            *batch.id as uuid::Uuid
        )
        .execute(&pool)
        .await
        .expect("Failed to update batch");

        let shutdown_token = tokio_util::sync::CancellationToken::new();
        manager
            .clone()
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        // Wait for escalations to be created and races to complete
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");

            // 2 originals + 2 escalated = 4 requests
            // Need at least 2 terminal (the winners)
            let terminal_count = requests.iter().filter(|r| r.is_terminal()).count();
            if requests.len() >= 4 && terminal_count >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Wait for supersession to complete
        tokio::time::sleep(Duration::from_millis(400)).await;
        shutdown_token.cancel();

        let results = collect_batch_results(&manager, batch.id).await;

        // Should have exactly 2 results (one per template, not 4 for all requests)
        assert_eq!(
            results.len(),
            2,
            "Should have 2 results (one per template), got: {:?}",
            results.iter().map(|r| &r.custom_id).collect::<Vec<_>>()
        );

        // Verify both custom IDs are present
        let custom_ids: Vec<_> = results
            .iter()
            .filter_map(|r| r.custom_id.as_ref())
            .collect();
        assert!(custom_ids.contains(&&"req-1".to_string()));
        assert!(custom_ids.contains(&&"req-2".to_string()));

        // Both should be completed
        for result in &results {
            assert_eq!(
                result.status,
                fusillade::batch::BatchResultStatus::Completed,
                "Result {:?} should be completed",
                result.custom_id
            );
            assert!(
                result.response_body.is_some(),
                "Result {:?} should have response body",
                result.custom_id
            );
        }
    }

    #[sqlx::test]
    async fn test_batch_results_deleted_file_returns_error(pool: sqlx::PgPool) {
        // Test: When batch's file is deleted, stream returns error
        let http_client = Arc::new(MockHttpClient::new());

        let manager = Arc::new(PostgresRequestManager::with_client(
            pool.clone(),
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
            PostgresRequestManager::with_client(pool.clone(), http_client.clone())
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
            PostgresRequestManager::with_client(pool.clone(), http_client.clone())
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
            PostgresRequestManager::with_client(pool.clone(), http_client.clone())
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
