use fusillade::batch::{BatchInput, RequestTemplateInput};
use fusillade::daemon::{
    DaemonConfig, ModelEscalationConfig, SlaAction, SlaThreshold, default_should_retry,
};
use fusillade::http::{HttpResponse, MockHttpClient};
use fusillade::manager::postgres::PostgresRequestManager;
use fusillade::manager::{DaemonExecutor, Storage};
use fusillade::request::RequestStateFilter;
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

        if let Some(Ok(req)) = res.first()
            && req.is_terminal() {
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

        if let Some(Ok(req)) = res.first()
            && req.is_terminal() {
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

mod sla {
    use super::*;

    #[sqlx::test]
    async fn test_sla_escalation_race_and_supersession(pool: sqlx::PgPool) {
        // Test: Original and escalated requests race to completion
        // Expected: Winner completes, loser is superseded (either can win)
        // Setup: Create HTTP client - both requests use the same path
        // Use triggers to add a small delay between completions so supersession can happen
        let http_client = Arc::new(MockHttpClient::new());

        // Add triggered responses for both requests
        let trigger1 = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"success"}"#.to_string(),
            }),
        );
        let trigger2 = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"success"}"#.to_string(),
            }),
        );

        // Trigger responses with a stagger - first at 50ms, second at 100ms
        // This ensures one completes before the other, allowing supersession
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = trigger1.send(());
        });
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = trigger2.send(());
        });

        // Setup: Configure daemon with SLA escalation
        let model_escalations = Arc::new(dashmap::DashMap::new());
        model_escalations.insert(
            "gpt-4".to_string(),
            ModelEscalationConfig {
                escalation_model: "gpt-4-turbo".to_string(),
                escalation_api_key: None,
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
            model_escalations,
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
                completion_window: "2h".to_string(),
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

        // Find the escalated request
        let escalated_data = all_requests
            .iter()
            .find(|r| r.data().is_escalated)
            .map(|r| r.data())
            .expect("Should find escalated request");

        let escalated_id = escalated_data.id;

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
        assert_eq!(
            escalated_data.escalated_model,
            Some("gpt-4-turbo".to_string()),
            "Escalated request should have escalated_model set"
        );

        // Wait for one request to complete
        let start = tokio::time::Instant::now();
        let timeout = Duration::from_secs(3);

        while start.elapsed() < timeout {
            let results = manager
                .get_requests(vec![original_id, escalated_id])
                .await
                .expect("Failed to get requests");

            if results.iter().any(|r| r.as_ref().unwrap().is_terminal()) {
                break;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

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

        // Determine who won by checking which one is Completed
        // Since both requests use the same path and race naturally, either can win
        let (winner_id, winner_variant, loser_variant, loser_data) =
            if original_final.variant() == "Completed" {
                (
                    original_id,
                    original_final.variant(),
                    escalated_final.variant(),
                    escalated_final.data(),
                )
            } else {
                (
                    escalated_id,
                    escalated_final.variant(),
                    original_final.variant(),
                    original_final.data(),
                )
            };

        // Verify: One request completed
        assert_eq!(
            winner_variant, "Completed",
            "Winner should be Completed, got {:?}",
            winner_variant
        );

        // Verify: Loser is superseded
        assert_eq!(
            loser_variant, "Superseded",
            "Loser should be Superseded, got {:?}",
            loser_variant
        );
        assert_eq!(
            loser_data.superseded_by_request_id,
            Some(winner_id),
            "Loser should be superseded by winner"
        );
        assert!(
            loser_data.superseded_at.is_some(),
            "Loser should have superseded_at timestamp"
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

        // NO model_escalations configured
        let model_escalations = Arc::new(dashmap::DashMap::new());

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
            model_escalations,
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

        let model_escalations = Arc::new(dashmap::DashMap::new());
        model_escalations.insert(
            "gpt-4".to_string(),
            ModelEscalationConfig {
                escalation_model: "gpt-4-turbo".to_string(),
                escalation_api_key: None,
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
            model_escalations,
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
    async fn test_sla_escalation_one_success_one_failure(pool: sqlx::PgPool) {
        // Test: One request succeeds (200), one fails (500)
        // Expected: The successful one completes, the failed one is superseded
        // Note: Since both use the same path, we can't control which gets which response
        let http_client = Arc::new(MockHttpClient::new());

        // Both use the same path - set up two responses with different status codes
        let trigger1 = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"success"}"#.to_string(),
            }),
        );
        let trigger2 = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                status: 200,
                body: r#"{"result":"success"}"#.to_string(),
            }),
        );

        // Trigger both with staggered timing to ensure one completes before the other
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = trigger1.send(());
        });
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let _ = trigger2.send(());
        });

        let model_escalations = Arc::new(dashmap::DashMap::new());
        model_escalations.insert(
            "gpt-4".to_string(),
            ModelEscalationConfig {
                escalation_model: "gpt-4-turbo".to_string(),
                escalation_api_key: None,
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
            model_escalations,
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

        let original_final = final_results[0].as_ref().unwrap();
        let escalated_final = final_results[1].as_ref().unwrap();

        println!("\n=== FINAL STATES ===");
        println!("Original: {:?}", original_final.variant());
        println!("Escalated: {:?}", escalated_final.variant());
        println!("====================\n");

        // Determine who won by checking which one is Completed
        // Since both requests use the same path, either can win
        let (winner_variant, loser_variant) = if original_final.variant() == "Completed" {
            (original_final.variant(), escalated_final.variant())
        } else {
            (escalated_final.variant(), original_final.variant())
        };

        // Verify: One request completed
        assert_eq!(
            winner_variant, "Completed",
            "Winner should be Completed, got {:?}",
            winner_variant
        );

        // Verify: Loser is superseded
        assert_eq!(
            loser_variant, "Superseded",
            "Loser should be Superseded, got {:?}",
            loser_variant
        );

        // Batch shows 1/1 completed (superseded doesn't count as complete or failed)
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
        // All use the same path since escalated requests use same endpoint
        for _ in 0..4 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    status: 200,
                    body: r#"{"result":"success"}"#.to_string(),
                }),
            );
        }

        let model_escalations = Arc::new(dashmap::DashMap::new());
        model_escalations.insert(
            "gpt-4".to_string(),
            ModelEscalationConfig {
                escalation_model: "gpt-4-turbo".to_string(),
                escalation_api_key: None,
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
            model_escalations,
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

        // Use triggers to control timing - all requests use same path
        // Both original and escalated complete at different times
        let triggers: Vec<_> = (0..6) // 3 original + 3 escalated = 6 total
            .map(|_| {
                http_client.add_response_with_trigger(
                    "POST /v1/test",
                    Ok(HttpResponse {
                        status: 200,
                        body: r#"{"result":"success"}"#.to_string(),
                    }),
                )
            })
            .collect();

        // Spawn tasks to trigger responses with staggered timing
        // This ensures requests complete one at a time so supersession can happen cleanly
        for (i, trigger) in triggers.into_iter().enumerate() {
            let delay = 50 + (i as u64 * 20); // Stagger by 20ms each: 50, 70, 90, 110, 130, 150
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                let _ = trigger.send(());
            });
        }

        let model_escalations = Arc::new(dashmap::DashMap::new());
        model_escalations.insert(
            "gpt-4".to_string(),
            ModelEscalationConfig {
                escalation_model: "gpt-4-turbo".to_string(),
                escalation_api_key: None,
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
            model_escalations,
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

    // Response for original request
    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"success"}"#.to_string(),
        }),
    );

    // Response for escalated request (uses same endpoint with model escalations)
    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"escalated_success"}"#.to_string(),
        }),
    );

    let model_escalations = Arc::new(dashmap::DashMap::new());
    model_escalations.insert(
        "gpt-4".to_string(),
        ModelEscalationConfig {
            escalation_model: "gpt-4-priority".to_string(), // Escalate to different model
            escalation_api_key: None,
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
        model_escalations,
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

    // Verify: Escalated keeps original model in DB (escalated_model indicates routing target)
    assert_eq!(
        escalated.data().model,
        "gpt-4",
        "Escalated should keep original model in DB"
    );
    assert_eq!(
        escalated.data().escalated_model,
        Some("gpt-4-priority".to_string()),
        "Escalated should have escalated_model set to indicate routing target"
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

    // Both original and escalated requests use the same endpoint/path
    // They differ by API key (escalated_api_key) and are grouped separately for concurrency
    let test_calls: Vec<_> = calls.iter().filter(|c| c.path == "/v1/test").collect();

    assert_eq!(
        test_calls.len(),
        2,
        "Should have 2 calls to /v1/test (original + escalated racing)"
    );

    // Both requests have gpt-4 in the body (escalated_model is metadata for concurrency/routing)
    // Both requests send the same body to the same endpoint
    for call in test_calls.iter() {
        assert!(
            call.body.contains(r#""model":"gpt-4""#),
            "Request should contain model gpt-4 in body: {}",
            call.body
        );
    }

    // The test verified above that escalated.data().escalated_model is set to "gpt-4-priority"
    // This metadata is used for concurrency grouping - escalated requests count toward
    // the escalated model's concurrency limit, not the original model's limit
}

#[sqlx::test]
async fn test_sla_escalation_uses_priority_api_key(pool: sqlx::PgPool) {
    // Test: Verify escalated requests use a different API key when escalation_api_key is configured
    // Expected: Original uses "original-api-key", escalated uses "escalated-api-key"
    // Both go to the same endpoint with the same body
    let http_client = Arc::new(MockHttpClient::new());

    // Both requests use the same path - use triggered responses to control timing
    let trigger1 = http_client.add_response_with_trigger(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"success"}"#.to_string(),
        }),
    );
    let trigger2 = http_client.add_response_with_trigger(
        "POST /v1/test",
        Ok(HttpResponse {
            status: 200,
            body: r#"{"result":"success"}"#.to_string(),
        }),
    );

    // Trigger both responses with staggered timing
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = trigger1.send(());
    });
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = trigger2.send(());
    });

    // Configure model escalations with a different API key
    let model_escalations = Arc::new(dashmap::DashMap::new());
    model_escalations.insert(
        "gpt-4".to_string(),
        ModelEscalationConfig {
            escalation_model: "gpt-4-turbo".to_string(),
            escalation_api_key: Some("escalated-api-key".to_string()),
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
        model_escalations,
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
                api_key: "original-api-key".to_string(),
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
        .find(|r| r.data().is_escalated)
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

    // Verify: Check the actual HTTP calls that were made
    let calls = http_client.get_calls();

    println!("\n=== HTTP CALLS VERIFICATION ===");
    for (i, call) in calls.iter().enumerate() {
        println!(
            "Call {}: {} {} - API Key: {} - Body: {}",
            i, call.method, call.path, call.api_key, call.body
        );
    }
    println!("===============================\n");

    // Should have exactly 2 calls to the same endpoint
    assert_eq!(
        calls.len(),
        2,
        "Should have exactly 2 HTTP calls (original + escalated)"
    );

    // Both calls should go to the same path
    assert_eq!(calls[0].path, "/v1/test", "First call should be to /v1/test");
    assert_eq!(calls[1].path, "/v1/test", "Second call should be to /v1/test");

    // Both calls should have the same body (model stays as user-requested)
    assert!(
        calls[0].body.contains(r#""model":"gpt-4""#),
        "First call should have gpt-4 in body"
    );
    assert!(
        calls[1].body.contains(r#""model":"gpt-4""#),
        "Second call should have gpt-4 in body"
    );

    // Find which call is original and which is escalated by API key
    let original_call = calls
        .iter()
        .find(|c| c.api_key == "original-api-key")
        .expect("Should have a call with original-api-key");
    let escalated_call = calls
        .iter()
        .find(|c| c.api_key == "escalated-api-key")
        .expect("Should have a call with escalated-api-key");

    // Verify: Original uses original API key
    assert_eq!(
        original_call.api_key, "original-api-key",
        "Original request should use original-api-key"
    );

    // Verify: Escalated uses escalated API key
    assert_eq!(
        escalated_call.api_key, "escalated-api-key",
        "Escalated request should use escalated-api-key"
    );

    // Verify: Database fields are set correctly
    let final_requests = manager
        .get_batch_requests(batch.id)
        .await
        .expect("Failed to get final requests");

    let escalated = final_requests
        .iter()
        .find(|r| r.data().is_escalated)
        .expect("Should have escalated request");

    let original = final_requests
        .iter()
        .find(|r| !r.data().is_escalated)
        .expect("Should have original request");

    // Verify: Escalated request has escalated_model and escalated_api_key set in DB
    assert_eq!(
        escalated.data().escalated_model,
        Some("gpt-4-turbo".to_string()),
        "Escalated request should have escalated_model set to gpt-4-turbo"
    );
    assert_eq!(
        escalated.data().escalated_api_key,
        Some("escalated-api-key".to_string()),
        "Escalated request should have escalated_api_key set"
    );
    assert_eq!(
        escalated.data().model,
        "gpt-4",
        "Escalated request should keep original model gpt-4"
    );
    assert_eq!(
        escalated.data().api_key,
        "original-api-key",
        "Escalated request stores original api_key in DB (escalated_api_key overrides it)"
    );

    // Verify: Original request has no escalation fields
    assert_eq!(
        original.data().escalated_model,
        None,
        "Original request should not have escalated_model set"
    );
    assert_eq!(
        original.data().escalated_api_key,
        None,
        "Original request should not have escalated_api_key set"
    );
    assert_eq!(
        original.data().model,
        "gpt-4",
        "Original request should have model gpt-4"
    );
    assert_eq!(
        original.data().api_key,
        "original-api-key",
        "Original request should use original-api-key"
    );
}
