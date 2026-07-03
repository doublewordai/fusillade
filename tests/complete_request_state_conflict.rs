//! Regression tests for [`Storage::complete_request`] / [`Storage::fail_request`]
//! disambiguating "row missing" from "row in wrong state".
//!
//! Background: previously both cases returned `RequestNotFound`, which made
//! it impossible for callers (specifically `dwctl`'s `complete_response_idempotent`)
//! to be properly idempotent against concurrent writers — a zombie task
//! attempt completing the row out from under a fresh attempt looked
//! identical to "row was never created".

use fusillade::manager::Storage;
use fusillade::response_step::{CreateStepInput, ResponseStepStore, StepKind};
use fusillade::{
    BatchInput, CreateRealtimeInput, FusilladeError, MockHttpClient, PostgresRequestManager,
    PostgresResponseStepManager, RequestId, RequestTemplateInput, TestDbPools,
};
use serde_json::json;
use sqlx::Row;
use std::sync::Arc;
use uuid::Uuid;

async fn manager(pool: sqlx::PgPool) -> Arc<PostgresRequestManager<TestDbPools, MockHttpClient>> {
    Arc::new(PostgresRequestManager::with_client(
        TestDbPools::new(pool).await.unwrap(),
        Arc::new(MockHttpClient::new()),
    ))
}

fn processing_input(request_id: Uuid) -> CreateRealtimeInput {
    CreateRealtimeInput {
        request_id,
        body: r#"{"messages":[]}"#.to_string(),
        model: "test-model".to_string(),
        endpoint: "http://localhost".to_string(),
        method: "POST".to_string(),
        path: "/v1/chat/completions".to_string(),
        api_key: "test-key".to_string(),
        created_by: "test-user".to_string(),
    }
}

#[sqlx::test]
async fn complete_request_succeeds_for_processing_row(pool: sqlx::PgPool) {
    let m = manager(pool).await;
    let id = Uuid::new_v4();
    m.create_realtime(processing_input(id)).await.unwrap();

    m.complete_request(RequestId(id), r#"{"ok":true}"#, 200)
        .await
        .expect("first complete_request should succeed");
}

#[sqlx::test]
async fn complete_request_returns_state_conflict_when_already_completed(pool: sqlx::PgPool) {
    let m = manager(pool).await;
    let id = Uuid::new_v4();
    m.create_realtime(processing_input(id)).await.unwrap();
    m.complete_request(RequestId(id), r#"{"ok":true}"#, 200)
        .await
        .unwrap();

    let err = m
        .complete_request(RequestId(id), r#"{"ok":true}"#, 200)
        .await
        .expect_err("second complete_request should error");

    match err {
        FusilladeError::RequestStateConflict {
            id: got_id,
            current_state,
            expected,
        } => {
            assert_eq!(got_id, RequestId(id));
            assert_eq!(current_state, "completed");
            assert_eq!(expected, "processing");
        }
        other => panic!("expected RequestStateConflict, got {other:?}"),
    }
}

#[sqlx::test]
async fn complete_request_returns_state_conflict_when_already_failed(pool: sqlx::PgPool) {
    let m = manager(pool).await;
    let id = Uuid::new_v4();
    m.create_realtime(processing_input(id)).await.unwrap();
    m.fail_request(RequestId(id), "boom", 500).await.unwrap();

    let err = m
        .complete_request(RequestId(id), r#"{"ok":true}"#, 200)
        .await
        .expect_err("complete_request on a failed row should error");

    match err {
        FusilladeError::RequestStateConflict { current_state, .. } => {
            assert_eq!(current_state, "failed");
        }
        other => panic!("expected RequestStateConflict, got {other:?}"),
    }
}

#[sqlx::test]
async fn complete_request_returns_not_found_for_missing_row(pool: sqlx::PgPool) {
    let m = manager(pool).await;
    let id = Uuid::new_v4();

    let err = m
        .complete_request(RequestId(id), r#"{"ok":true}"#, 200)
        .await
        .expect_err("complete_request on missing row should error");

    match err {
        FusilladeError::RequestNotFound(got_id) => assert_eq!(got_id, RequestId(id)),
        other => panic!("expected RequestNotFound, got {other:?}"),
    }
}

#[sqlx::test]
async fn fail_request_returns_state_conflict_when_already_completed(pool: sqlx::PgPool) {
    let m = manager(pool).await;
    let id = Uuid::new_v4();
    m.create_realtime(processing_input(id)).await.unwrap();
    m.complete_request(RequestId(id), r#"{"ok":true}"#, 200)
        .await
        .unwrap();

    let err = m
        .fail_request(RequestId(id), "boom", 500)
        .await
        .expect_err("fail_request on completed row should error");

    match err {
        FusilladeError::RequestStateConflict { current_state, .. } => {
            assert_eq!(current_state, "completed");
        }
        other => panic!("expected RequestStateConflict, got {other:?}"),
    }
}

#[sqlx::test]
async fn fail_request_returns_not_found_for_missing_row(pool: sqlx::PgPool) {
    let m = manager(pool).await;
    let id = Uuid::new_v4();

    let err = m
        .fail_request(RequestId(id), "boom", 500)
        .await
        .expect_err("fail_request on missing row should error");

    match err {
        FusilladeError::RequestNotFound(got_id) => assert_eq!(got_id, RequestId(id)),
        other => panic!("expected RequestNotFound, got {other:?}"),
    }
}

#[sqlx::test]
async fn complete_request_keeps_realtime_row_and_preserves_response_step(pool: sqlx::PgPool) {
    let m = manager(pool.clone()).await;
    let id = Uuid::new_v4();
    m.create_realtime(processing_input(id)).await.unwrap();

    let step_store =
        PostgresResponseStepManager::new(TestDbPools::new(pool.clone()).await.unwrap());
    let step_id = step_store
        .create_step(CreateStepInput {
            id: None,
            request_id: Some(RequestId(id)),
            prev_step_id: None,
            parent_step_id: None,
            step_kind: StepKind::ModelCall,
            step_sequence: 1,
            request_payload: json!({"messages": []}),
        })
        .await
        .expect("step should reference the active request entity");

    m.complete_request(RequestId(id), r#"{"ok":true}"#, 200)
        .await
        .expect("complete_request should persist the terminal realtime row");

    let active_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM requests WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        active_count, 1,
        "realtime terminal rows stay in requests until the response-store slice"
    );

    let archive_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM requests_archive WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(archive_count, 0, "realtime rows are not batch-archived");

    let step = step_store.get_step(step_id).await.unwrap().unwrap();
    assert_eq!(step.request_id, Some(RequestId(id)));

    let detail = m.get_request_detail(RequestId(id)).await.unwrap();
    assert_eq!(detail.status, "completed");
    assert_eq!(detail.response_body.as_deref(), Some(r#"{"ok":true}"#));
}

#[sqlx::test]
async fn batch_request_is_not_archived_until_batch_is_terminal(pool: sqlx::PgPool) {
    let m = manager(pool.clone()).await;
    let file_id = m
        .create_file(
            "batch-archive-finalization-test".to_string(),
            None,
            vec![
                RequestTemplateInput {
                    custom_id: Some("custom-1".to_string()),
                    endpoint: "http://localhost".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/chat/completions".to_string(),
                    body: r#"{"messages":[]}"#.to_string(),
                    model: "test-model".to_string(),
                    api_key: "test-key".to_string(),
                },
                RequestTemplateInput {
                    custom_id: Some("custom-2".to_string()),
                    endpoint: "http://localhost".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/chat/completions".to_string(),
                    body: r#"{"messages":[]}"#.to_string(),
                    model: "test-model".to_string(),
                    api_key: "test-key".to_string(),
                },
            ],
        )
        .await
        .unwrap();
    let batch = m
        .create_batch(BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: Some("test-user".to_string()),
            api_key_id: None,
            api_key: None,
            total_requests: None,
        })
        .await
        .unwrap();

    let request_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM requests WHERE batch_id = $1 ORDER BY custom_id ASC")
            .bind(batch.id.0)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(request_ids.len(), 2);

    sqlx::query(
        "UPDATE requests \
         SET state = 'completed', response_status = 200, response_body = '{}', \
             claimed_at = NOW(), started_at = NOW(), completed_at = NOW() \
         WHERE id = $1",
    )
    .bind(request_ids[0])
    .execute(&pool)
    .await
    .unwrap();

    let active_after_one: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM requests WHERE batch_id = $1")
            .bind(batch.id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        active_after_one, 2,
        "single terminal request must not be archived before the batch is final"
    );

    let archived_after_one: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM requests_archive WHERE batch_id = $1")
            .bind(batch.id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(archived_after_one, 0);

    sqlx::query(
        "UPDATE requests \
         SET state = 'failed', failed_at = NOW(), error = $2, response_size = length($2) \
         WHERE id = $1",
    )
    .bind(request_ids[1])
    .bind(r#"{"type":"BatchTerminated"}"#)
    .execute(&pool)
    .await
    .unwrap();

    m.get_batch(batch.id)
        .await
        .expect("fetching a terminal batch should freeze counts and archive");

    let active_after_terminal: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM requests WHERE batch_id = $1")
            .bind(batch.id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(active_after_terminal, 0);

    let archived_after_terminal: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM requests_archive WHERE batch_id = $1")
            .bind(batch.id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(archived_after_terminal, 2);

    let archived_requests = m.get_batch_requests(batch.id).await.unwrap();
    assert_eq!(
        archived_requests.len(),
        2,
        "terminal batch detail should read archived rows"
    );
    assert!(
        archived_requests
            .iter()
            .any(|request| matches!(request, fusillade::AnyRequest::Completed(_)))
    );
    assert!(
        archived_requests
            .iter()
            .any(|request| matches!(request, fusillade::AnyRequest::Failed(_)))
    );

    let frozen = sqlx::query(
        "SELECT completed_requests, failed_requests, canceled_requests, in_progress_requests \
         FROM batches WHERE id = $1",
    )
    .bind(batch.id.0)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(frozen.get::<i64, _>("completed_requests"), 1);
    assert_eq!(frozen.get::<i64, _>("failed_requests"), 1);
    assert_eq!(frozen.get::<i64, _>("canceled_requests"), 0);
    assert_eq!(frozen.get::<i64, _>("in_progress_requests"), 0);
}

#[sqlx::test]
async fn retry_failed_requests_for_batch_restores_archive_rows_to_active(pool: sqlx::PgPool) {
    let m = manager(pool.clone()).await;
    let file_id = m
        .create_file(
            "retry-history-test".to_string(),
            None,
            vec![RequestTemplateInput {
                custom_id: Some("custom-1".to_string()),
                endpoint: "http://localhost".to_string(),
                method: "POST".to_string(),
                path: "/v1/chat/completions".to_string(),
                body: r#"{"messages":[]}"#.to_string(),
                model: "test-model".to_string(),
                api_key: "test-key".to_string(),
            }],
        )
        .await
        .unwrap();
    let batch = m
        .create_batch(BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: Some("test-user".to_string()),
            api_key_id: None,
            api_key: None,
            total_requests: None,
        })
        .await
        .unwrap();

    let row = sqlx::query("SELECT id FROM requests WHERE batch_id = $1")
        .bind(batch.id.0)
        .fetch_one(&pool)
        .await
        .unwrap();
    let request_id: Uuid = row.get("id");

    sqlx::query(
        "UPDATE requests \
         SET state = 'failed', failed_at = NOW(), error = $2, response_size = length($2) \
         WHERE id = $1",
    )
    .bind(request_id)
    .bind(r#"{"type":"BatchTerminated"}"#)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE batches SET failed_at = NOW() WHERE id = $1")
        .bind(batch.id.0)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("SELECT archive_terminal_batch_requests($1)")
        .bind(batch.id.0)
        .execute(&pool)
        .await
        .unwrap();

    let active_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM requests WHERE id = $1")
        .bind(request_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(active_before, 0, "setup should move failed row to archive");

    let retried = m.retry_failed_requests_for_batch(batch.id).await.unwrap();
    assert_eq!(retried, 1);

    let state: String = sqlx::query_scalar("SELECT state FROM requests WHERE id = $1")
        .bind(request_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "pending");

    let archive_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM requests_archive WHERE id = $1")
            .bind(request_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        archive_count, 0,
        "retry should consume the archived failure"
    );

    let terminal =
        sqlx::query("SELECT completed_at, failed_at, finalizing_at FROM batches WHERE id = $1")
            .bind(batch.id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        terminal
            .get::<Option<chrono::DateTime<chrono::Utc>>, _>("completed_at")
            .is_none()
    );
    assert!(
        terminal
            .get::<Option<chrono::DateTime<chrono::Utc>>, _>("failed_at")
            .is_none()
    );
    assert!(
        terminal
            .get::<Option<chrono::DateTime<chrono::Utc>>, _>("finalizing_at")
            .is_none()
    );
}
