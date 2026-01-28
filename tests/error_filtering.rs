use fusillade::TestDbPools;
use fusillade::batch::{BatchInput, ErrorFilter, RequestTemplateInput};
use fusillade::daemon::DaemonConfig;
use fusillade::http::MockHttpClient;
use fusillade::manager::postgres::PostgresRequestManager;
use fusillade::manager::Storage;
use fusillade::request::FailureReason;
use futures::StreamExt;
use std::sync::Arc;

/// Helper to create a batch with mixed retriable and non-retriable failures
async fn setup_batch_with_mixed_failures(
    pool: sqlx::PgPool,
) -> (
    Arc<PostgresRequestManager<fusillade::TestDbPools, MockHttpClient>>,
    fusillade::batch::BatchId,
    fusillade::batch::FileId,
) {
    let http_client = Arc::new(MockHttpClient::new());
    let config = DaemonConfig::default();

    let manager = Arc::new(
        PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_config(config),
    );

    // Create file with 6 templates
    let file_id = manager
        .create_file(
            "error-filter-test".to_string(),
            None,
            (0..6)
                .map(|i| RequestTemplateInput {
                    custom_id: Some(format!("req-{}", i)),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: format!(r#"{{"n":{}}}"#, i),
                    model: "test".to_string(),
                    api_key: "key".to_string(),
                })
                .collect(),
        )
        .await
        .unwrap();

    let batch = manager
        .create_batch(BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: Some("test-user".to_string()),
        })
        .await
        .unwrap();

    let requests = manager.get_batch_requests(batch.id).await.unwrap();
    assert_eq!(requests.len(), 6);

    // Manually fail requests with a mix of retriable and non-retriable errors
    // 2 retriable (429, 503), 2 non-retriable (400, 404), 1 completed, 1 pending
    for (i, req) in requests.iter().enumerate() {
        let request_id = req.id();

        match i {
            // Retriable: 429 Rate Limit
            0 => {
                sqlx::query!(
                    r#"
                    UPDATE requests
                    SET state = 'failed',
                        error = $2,
                        failed_at = NOW(),
                        is_retriable_error = true
                    WHERE id = $1
                    "#,
                    *request_id as uuid::Uuid,
                    serde_json::to_string(&FailureReason::RetriableHttpStatus {
                        status: 429,
                        body: "Rate limit exceeded".to_string()
                    })
                    .unwrap(),
                )
                .execute(&pool)
                .await
                .unwrap();
            }
            // Retriable: 503 Service Unavailable
            1 => {
                sqlx::query!(
                    r#"
                    UPDATE requests
                    SET state = 'failed',
                        error = $2,
                        failed_at = NOW(),
                        is_retriable_error = true
                    WHERE id = $1
                    "#,
                    *request_id as uuid::Uuid,
                    serde_json::to_string(&FailureReason::RetriableHttpStatus {
                        status: 503,
                        body: "Service unavailable".to_string()
                    })
                    .unwrap(),
                )
                .execute(&pool)
                .await
                .unwrap();
            }
            // Non-retriable: 400 Bad Request
            2 => {
                sqlx::query!(
                    r#"
                    UPDATE requests
                    SET state = 'failed',
                        error = $2,
                        failed_at = NOW(),
                        is_retriable_error = false
                    WHERE id = $1
                    "#,
                    *request_id as uuid::Uuid,
                    serde_json::to_string(&FailureReason::NonRetriableHttpStatus {
                        status: 400,
                        body: "Bad request".to_string()
                    })
                    .unwrap(),
                )
                .execute(&pool)
                .await
                .unwrap();
            }
            // Non-retriable: 404 Not Found
            3 => {
                sqlx::query!(
                    r#"
                    UPDATE requests
                    SET state = 'failed',
                        error = $2,
                        failed_at = NOW(),
                        is_retriable_error = false
                    WHERE id = $1
                    "#,
                    *request_id as uuid::Uuid,
                    serde_json::to_string(&FailureReason::NonRetriableHttpStatus {
                        status: 404,
                        body: "Not found".to_string()
                    })
                    .unwrap(),
                )
                .execute(&pool)
                .await
                .unwrap();
            }
            // Completed
            4 => {
                sqlx::query!(
                    r#"
                    UPDATE requests
                    SET state = 'completed',
                        response_status = 200,
                        response_body = '{"result":"ok"}',
                        completed_at = NOW()
                    WHERE id = $1
                    "#,
                    *request_id as uuid::Uuid,
                )
                .execute(&pool)
                .await
                .unwrap();
            }
            // Leave 5 as pending
            _ => {}
        }
    }

    // Manually update batch counts since we bypassed the normal flow
    // In production, the trigger handles this, but for testing we need to sync manually
    sqlx::query!(
        r#"
        UPDATE batches
        SET failed_requests_retriable = 2,
            failed_requests_non_retriable = 2
        WHERE id = $1
        "#,
        *batch.id as uuid::Uuid,
    )
    .execute(&pool)
    .await
    .unwrap();

    (manager, batch.id, file_id)
}

#[sqlx::test]
#[test_log::test]
async fn test_get_batch_error_filter_all(pool: sqlx::PgPool) {
    let (manager, batch_id, _) = setup_batch_with_mixed_failures(pool).await;

    let batch = manager
        .get_batch(batch_id, ErrorFilter::All)
        .await
        .unwrap();

    assert_eq!(batch.total_requests, 6);
    assert_eq!(batch.completed_requests, 1);
    assert_eq!(batch.pending_requests, 1);
    assert_eq!(batch.failed_requests, 4, "All filter should show all 4 failures");
    assert_eq!(
        batch.failed_requests_retriable, 2,
        "Should have 2 retriable failures"
    );
    assert_eq!(
        batch.failed_requests_non_retriable, 2,
        "Should have 2 non-retriable failures"
    );
}

#[sqlx::test]
#[test_log::test]
async fn test_get_batch_error_filter_only_retriable(pool: sqlx::PgPool) {
    let (manager, batch_id, _) = setup_batch_with_mixed_failures(pool).await;

    let batch = manager
        .get_batch(batch_id, ErrorFilter::OnlyRetriable)
        .await
        .unwrap();

    assert_eq!(batch.total_requests, 6);
    assert_eq!(batch.completed_requests, 1);
    assert_eq!(batch.pending_requests, 1);
    assert_eq!(
        batch.failed_requests, 2,
        "OnlyRetriable filter should show only 2 retriable failures"
    );
    // Always present full counts
    assert_eq!(batch.failed_requests_retriable, 2);
    assert_eq!(batch.failed_requests_non_retriable, 2);
}

#[sqlx::test]
#[test_log::test]
async fn test_get_batch_error_filter_only_non_retriable(pool: sqlx::PgPool) {
    let (manager, batch_id, _) = setup_batch_with_mixed_failures(pool).await;

    let batch = manager
        .get_batch(batch_id, ErrorFilter::OnlyNonRetriable)
        .await
        .unwrap();

    assert_eq!(batch.total_requests, 6);
    assert_eq!(batch.completed_requests, 1);
    assert_eq!(batch.pending_requests, 1);
    assert_eq!(
        batch.failed_requests, 2,
        "OnlyNonRetriable filter should show only 2 non-retriable failures"
    );
    // Always present full counts
    assert_eq!(batch.failed_requests_retriable, 2);
    assert_eq!(batch.failed_requests_non_retriable, 2);
}

#[sqlx::test]
#[test_log::test]
async fn test_get_batch_status_error_filter_all(pool: sqlx::PgPool) {
    let (manager, batch_id, _) = setup_batch_with_mixed_failures(pool).await;

    let status = manager
        .get_batch_status(batch_id, ErrorFilter::All)
        .await
        .unwrap();

    assert_eq!(status.total_requests, 6);
    assert_eq!(status.failed_requests, 4);
    assert_eq!(status.failed_requests_retriable, 2);
    assert_eq!(status.failed_requests_non_retriable, 2);
}

#[sqlx::test]
#[test_log::test]
async fn test_get_batch_status_error_filter_only_retriable(pool: sqlx::PgPool) {
    let (manager, batch_id, _) = setup_batch_with_mixed_failures(pool).await;

    let status = manager
        .get_batch_status(batch_id, ErrorFilter::OnlyRetriable)
        .await
        .unwrap();

    assert_eq!(status.total_requests, 6);
    assert_eq!(status.failed_requests, 2);
    assert_eq!(status.failed_requests_retriable, 2);
    assert_eq!(status.failed_requests_non_retriable, 2);
}

#[sqlx::test]
#[test_log::test]
async fn test_get_batch_status_error_filter_only_non_retriable(pool: sqlx::PgPool) {
    let (manager, batch_id, _) = setup_batch_with_mixed_failures(pool).await;

    let status = manager
        .get_batch_status(batch_id, ErrorFilter::OnlyNonRetriable)
        .await
        .unwrap();

    assert_eq!(status.total_requests, 6);
    assert_eq!(status.failed_requests, 2);
    assert_eq!(status.failed_requests_retriable, 2);
    assert_eq!(status.failed_requests_non_retriable, 2);
}

#[sqlx::test]
#[test_log::test]
async fn test_list_file_batches_error_filter(pool: sqlx::PgPool) {
    let (manager, batch_id, file_id) = setup_batch_with_mixed_failures(pool).await;

    // Test All filter
    let batches = manager
        .list_file_batches(file_id, ErrorFilter::All)
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].batch_id, batch_id);
    assert_eq!(batches[0].failed_requests, 4);

    // Test OnlyRetriable filter
    let batches = manager
        .list_file_batches(file_id, ErrorFilter::OnlyRetriable)
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].failed_requests, 2);

    // Test OnlyNonRetriable filter
    let batches = manager
        .list_file_batches(file_id, ErrorFilter::OnlyNonRetriable)
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].failed_requests, 2);
}

#[sqlx::test]
#[test_log::test]
async fn test_list_batches_error_filter(pool: sqlx::PgPool) {
    let (manager, batch_id, _) = setup_batch_with_mixed_failures(pool).await;

    // Test All filter
    let batches = manager
        .list_batches(Some("test-user".to_string()), None, None, 10, ErrorFilter::All)
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].id, batch_id);
    assert_eq!(batches[0].failed_requests, 4);

    // Test OnlyRetriable filter
    let batches = manager
        .list_batches(
            Some("test-user".to_string()),
            None,
            None,
            10,
            ErrorFilter::OnlyRetriable,
        )
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].failed_requests, 2);

    // Test OnlyNonRetriable filter
    let batches = manager
        .list_batches(
            Some("test-user".to_string()),
            None,
            None,
            10,
            ErrorFilter::OnlyNonRetriable,
        )
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].failed_requests, 2);
}

#[sqlx::test]
#[test_log::test]
async fn test_get_file_content_stream_error_filter(pool: sqlx::PgPool) {
    let (manager, batch_id, _) = setup_batch_with_mixed_failures(pool).await;

    // Get the error file ID
    let batch = manager
        .get_batch(batch_id, ErrorFilter::All)
        .await
        .unwrap();
    let error_file_id = batch.error_file_id.expect("Batch should have error file");

    // Test All filter - should get all 4 failures
    let stream = manager.get_file_content_stream(error_file_id, 0, None, ErrorFilter::All);
    let items: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;
    assert_eq!(items.len(), 4, "All filter should return all 4 failures");

    // Test OnlyRetriable filter - should get 2 retriable failures
    let stream =
        manager.get_file_content_stream(error_file_id, 0, None, ErrorFilter::OnlyRetriable);
    let items: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;
    assert_eq!(
        items.len(),
        2,
        "OnlyRetriable filter should return 2 retriable failures"
    );

    // Test OnlyNonRetriable filter - should get 2 non-retriable failures
    let stream =
        manager.get_file_content_stream(error_file_id, 0, None, ErrorFilter::OnlyNonRetriable);
    let items: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;
    assert_eq!(
        items.len(),
        2,
        "OnlyNonRetriable filter should return 2 non-retriable failures"
    );
}

#[sqlx::test]
#[test_log::test]
async fn test_get_batch_results_stream_error_filter(pool: sqlx::PgPool) {
    let (manager, batch_id, _) = setup_batch_with_mixed_failures(pool).await;

    // Test All filter with status="failed" - should get all 4 failures
    let stream = manager.get_batch_results_stream(
        batch_id,
        0,
        None,
        Some("failed".to_string()),
        ErrorFilter::All,
    );
    let items: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;
    assert_eq!(items.len(), 4, "All filter should return all 4 failures");

    // Test OnlyRetriable filter with status="failed" - should get 2 retriable failures
    let stream = manager.get_batch_results_stream(
        batch_id,
        0,
        None,
        Some("failed".to_string()),
        ErrorFilter::OnlyRetriable,
    );
    let items: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;
    assert_eq!(
        items.len(),
        2,
        "OnlyRetriable filter should return 2 retriable failures"
    );

    // Test OnlyNonRetriable filter with status="failed" - should get 2 non-retriable failures
    let stream = manager.get_batch_results_stream(
        batch_id,
        0,
        None,
        Some("failed".to_string()),
        ErrorFilter::OnlyNonRetriable,
    );
    let items: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;
    assert_eq!(
        items.len(),
        2,
        "OnlyNonRetriable filter should return 2 non-retriable failures"
    );

    // Test that error filter doesn't affect non-failed statuses
    let stream = manager.get_batch_results_stream(
        batch_id,
        0,
        None,
        Some("completed".to_string()),
        ErrorFilter::OnlyRetriable,
    );
    let items: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;
    assert_eq!(
        items.len(),
        1,
        "Error filter shouldn't affect completed requests"
    );
}

#[sqlx::test]
#[test_log::test]
async fn test_error_filter_with_null_is_retriable_error(pool: sqlx::PgPool) {
    // Test that NULL is_retriable_error is treated as non-retriable (historical data)
    let http_client = Arc::new(MockHttpClient::new());
    let config = DaemonConfig::default();
    let manager = Arc::new(
        PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_config(config),
    );

    let file_id = manager
        .create_file(
            "null-test".to_string(),
            None,
            vec![RequestTemplateInput {
                custom_id: Some("null-req".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"test":1}"#.to_string(),
                model: "test".to_string(),
                api_key: "key".to_string(),
            }],
        )
        .await
        .unwrap();

    let batch = manager
        .create_batch(BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: None,
        })
        .await
        .unwrap();

    let requests = manager.get_batch_requests(batch.id).await.unwrap();
    let request_id = requests[0].id();

    // Manually set a failed request with NULL is_retriable_error (simulating historical data)
    sqlx::query!(
        r#"
        UPDATE requests
        SET state = 'failed',
            error = '{"HttpError":{"status":500}}',
            failed_at = NOW(),
            is_retriable_error = NULL
        WHERE id = $1
        "#,
        *request_id as uuid::Uuid,
    )
    .execute(&pool)
    .await
    .unwrap();

    // NULL should be treated as non-retriable
    let batch = manager
        .get_batch(batch.id, ErrorFilter::OnlyRetriable)
        .await
        .unwrap();
    assert_eq!(
        batch.failed_requests, 0,
        "NULL should not be counted as retriable"
    );

    let batch = manager
        .get_batch(batch.id, ErrorFilter::OnlyNonRetriable)
        .await
        .unwrap();
    assert_eq!(
        batch.failed_requests, 1,
        "NULL should be counted as non-retriable"
    );

    let batch = manager
        .get_batch(batch.id, ErrorFilter::All)
        .await
        .unwrap();
    assert_eq!(batch.failed_requests, 1, "All should include NULL");
}
