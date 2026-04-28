//! Integration tests for the response_steps storage layer.
//!
//! Each test gets a fresh sqlx pool via `#[sqlx::test]`, which automatically
//! runs every migration in `migrations/`. We insert a parent request row by
//! hand (avoiding the full file/template/batch chain) since these tests are
//! scoped to the response_step module only.

use fusillade::request::RequestId;
use fusillade::response_step::{CreateStepInput, ResponseStepStore, StepKind, StepState};
use fusillade::{PostgresResponseStepManager, TestDbPools};
use serde_json::json;
use uuid::Uuid;

/// Insert a minimal parent `requests` row so `response_steps.request_id`'s
/// foreign key is satisfied. Returns the new request id.
async fn insert_parent_request(pool: &sqlx::PgPool) -> RequestId {
    let template_id = Uuid::new_v4();
    let request_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO request_templates \
         (id, file_id, custom_id, endpoint, method, path, body, model, api_key, body_byte_size) \
         VALUES ($1, NULL, NULL, $2, 'POST', '/v1/responses', '{}', 'test-model', '', 0)",
    )
    .bind(template_id)
    .bind("http://upstream")
    .execute(pool)
    .await
    .expect("insert template");

    sqlx::query(
        "INSERT INTO requests \
         (id, batch_id, template_id, model, custom_id, state) \
         VALUES ($1, NULL, $2, 'test-model', NULL, 'pending')",
    )
    .bind(request_id)
    .bind(template_id)
    .execute(pool)
    .await
    .expect("insert request");

    RequestId(request_id)
}

#[sqlx::test]
async fn create_and_fetch_top_level_step(pool: sqlx::PgPool) {
    let request_id = insert_parent_request(&pool).await;
    let pools = TestDbPools::new(pool).await.unwrap();
    let store = PostgresResponseStepManager::new(pools);

    let step_id = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: None,
            parent_step_id: None,
            step_kind: StepKind::ModelCall,
            step_sequence: 1,
            request_payload: json!({"prompt": "hello"}),
        })
        .await
        .expect("create_step");

    let fetched = store
        .get_step(step_id)
        .await
        .expect("get_step")
        .expect("step exists");

    assert_eq!(fetched.id, step_id);
    assert_eq!(fetched.request_id, request_id);
    assert_eq!(fetched.step_kind, StepKind::ModelCall);
    assert_eq!(fetched.state, StepState::Pending);
    assert_eq!(fetched.step_sequence, 1);
    assert_eq!(fetched.retry_attempt, 0);
    assert!(fetched.response_payload.is_none());
    assert!(fetched.started_at.is_none());
    assert_eq!(fetched.request_payload, json!({"prompt": "hello"}));
}

#[sqlx::test]
async fn list_chain_returns_steps_in_sequence_order(pool: sqlx::PgPool) {
    let request_id = insert_parent_request(&pool).await;
    let pools = TestDbPools::new(pool).await.unwrap();
    let store = PostgresResponseStepManager::new(pools);

    // Insert in reverse to confirm ordering is by step_sequence, not insert time
    let step_3 = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: None,
            parent_step_id: None,
            step_kind: StepKind::ModelCall,
            step_sequence: 3,
            request_payload: json!({"step": 3}),
        })
        .await
        .unwrap();

    let step_1 = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: None,
            parent_step_id: None,
            step_kind: StepKind::ToolCall,
            step_sequence: 1,
            request_payload: json!({"step": 1}),
        })
        .await
        .unwrap();

    let step_2 = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: Some(step_1),
            parent_step_id: None,
            step_kind: StepKind::ModelCall,
            step_sequence: 2,
            request_payload: json!({"step": 2}),
        })
        .await
        .unwrap();

    let chain = store.list_chain(request_id).await.unwrap();
    assert_eq!(chain.len(), 3);
    assert_eq!(chain[0].id, step_1);
    assert_eq!(chain[1].id, step_2);
    assert_eq!(chain[2].id, step_3);
    assert_eq!(chain[1].prev_step_id, Some(step_1));
}

#[sqlx::test]
async fn list_scope_separates_top_level_from_sub_loop(pool: sqlx::PgPool) {
    let request_id = insert_parent_request(&pool).await;
    let pools = TestDbPools::new(pool).await.unwrap();
    let store = PostgresResponseStepManager::new(pools);

    // Top-level: a model_call followed by a tool_call (which spawns a sub-agent)
    let top_1 = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: None,
            parent_step_id: None,
            step_kind: StepKind::ModelCall,
            step_sequence: 1,
            request_payload: json!({"top": 1}),
        })
        .await
        .unwrap();
    let top_2 = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: Some(top_1),
            parent_step_id: None,
            step_kind: StepKind::ToolCall,
            step_sequence: 2,
            request_payload: json!({"top": 2, "tool": "delegate"}),
        })
        .await
        .unwrap();

    // Sub-loop under top_2: a model_call.
    let sub_1 = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: None,
            parent_step_id: Some(top_2),
            step_kind: StepKind::ModelCall,
            step_sequence: 3,
            request_payload: json!({"sub": 1}),
        })
        .await
        .unwrap();

    let top = store.list_scope(request_id, None).await.unwrap();
    assert_eq!(top.len(), 2);
    assert_eq!(top[0].id, top_1);
    assert_eq!(top[1].id, top_2);

    let sub = store.list_scope(request_id, Some(top_2)).await.unwrap();
    assert_eq!(sub.len(), 1);
    assert_eq!(sub[0].id, sub_1);

    let other_scope = store.list_scope(request_id, Some(top_1)).await.unwrap();
    assert!(other_scope.is_empty());
}

#[sqlx::test]
async fn lifecycle_transitions(pool: sqlx::PgPool) {
    let request_id = insert_parent_request(&pool).await;
    let pools = TestDbPools::new(pool).await.unwrap();
    let store = PostgresResponseStepManager::new(pools);

    let step_id = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: None,
            parent_step_id: None,
            step_kind: StepKind::ModelCall,
            step_sequence: 1,
            request_payload: json!({"p": 1}),
        })
        .await
        .unwrap();

    store.mark_step_processing(step_id).await.unwrap();
    let s = store.get_step(step_id).await.unwrap().unwrap();
    assert_eq!(s.state, StepState::Processing);
    assert!(s.started_at.is_some());

    // mark_step_processing is idempotent — no error when already processing
    store.mark_step_processing(step_id).await.unwrap();

    store
        .complete_step(step_id, json!({"output": "ok"}))
        .await
        .unwrap();
    let s = store.get_step(step_id).await.unwrap().unwrap();
    assert_eq!(s.state, StepState::Completed);
    assert_eq!(s.response_payload, Some(json!({"output": "ok"})));
    assert!(s.completed_at.is_some());

    // Cannot complete an already-terminal step.
    let err = store.complete_step(step_id, json!({})).await;
    assert!(err.is_err());
}

#[sqlx::test]
async fn fail_step_records_error_payload(pool: sqlx::PgPool) {
    let request_id = insert_parent_request(&pool).await;
    let pools = TestDbPools::new(pool).await.unwrap();
    let store = PostgresResponseStepManager::new(pools);

    let step_id = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: None,
            parent_step_id: None,
            step_kind: StepKind::ToolCall,
            step_sequence: 1,
            request_payload: json!({"tool": "search"}),
        })
        .await
        .unwrap();

    store.mark_step_processing(step_id).await.unwrap();

    let err_payload = json!({"type": "max_iterations_exceeded", "details": "stuck"});
    store.fail_step(step_id, err_payload.clone()).await.unwrap();

    let s = store.get_step(step_id).await.unwrap().unwrap();
    assert_eq!(s.state, StepState::Failed);
    assert_eq!(s.error, Some(err_payload));
    assert!(s.failed_at.is_some());
}

#[sqlx::test]
async fn requeue_for_retry_increments_attempt(pool: sqlx::PgPool) {
    let request_id = insert_parent_request(&pool).await;
    let pools = TestDbPools::new(pool).await.unwrap();
    let store = PostgresResponseStepManager::new(pools);

    let step_id = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: None,
            parent_step_id: None,
            step_kind: StepKind::ModelCall,
            step_sequence: 1,
            request_payload: json!({}),
        })
        .await
        .unwrap();

    store.mark_step_processing(step_id).await.unwrap();
    store.requeue_step_for_retry(step_id).await.unwrap();

    let s = store.get_step(step_id).await.unwrap().unwrap();
    assert_eq!(s.state, StepState::Pending);
    assert_eq!(s.retry_attempt, 1);
    assert!(s.started_at.is_none());

    // Still pending — cannot requeue without going processing first.
    let err = store.requeue_step_for_retry(step_id).await;
    assert!(err.is_err());
}

#[sqlx::test]
async fn unique_constraint_blocks_duplicate_successors(pool: sqlx::PgPool) {
    let request_id = insert_parent_request(&pool).await;
    let pools = TestDbPools::new(pool).await.unwrap();
    let store = PostgresResponseStepManager::new(pools);

    let parent = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: None,
            parent_step_id: None,
            step_kind: StepKind::ModelCall,
            step_sequence: 1,
            request_payload: json!({}),
        })
        .await
        .unwrap();

    // First successor: prev=parent, scope=top-level, kind=tool_call
    store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: Some(parent),
            parent_step_id: None,
            step_kind: StepKind::ToolCall,
            step_sequence: 2,
            request_payload: json!({"first": true}),
        })
        .await
        .unwrap();

    // Re-running the transition function under crash recovery must not
    // produce a duplicate row with the same (request, parent, prev, kind)
    // tuple.
    let dup = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: Some(parent),
            parent_step_id: None,
            step_kind: StepKind::ToolCall,
            step_sequence: 3,
            request_payload: json!({"first": false}),
        })
        .await;
    assert!(
        dup.is_err(),
        "expected UNIQUE constraint violation on duplicate successor"
    );

    // Different step_kind under the same predecessor is fine.
    store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: Some(parent),
            parent_step_id: None,
            step_kind: StepKind::ModelCall,
            step_sequence: 4,
            request_payload: json!({}),
        })
        .await
        .unwrap();
}

#[sqlx::test]
async fn parallel_tool_calls_share_prev_but_unique_kinds(pool: sqlx::PgPool) {
    // The orchestration loop fans out N tool_call rows after a model_call
    // emits N tool calls. The UNIQUE constraint says
    // (request_id, parent_step_id, prev_step_id, step_kind) must be
    // unique, so multiple parallel tool_calls under the same predecessor
    // are NOT allowed by this constraint alone — the storage layer relies
    // on each parallel tool_call having a distinct prev_step_id chain
    // (the fan-out linearizes them via prev_step_id pointing at the
    // previous sibling, not all at the same parent).
    //
    // This test documents that intent: parallel siblings form a linear
    // chain by prev_step_id, even though the orchestration loop fires
    // their HTTP calls concurrently.

    let request_id = insert_parent_request(&pool).await;
    let pools = TestDbPools::new(pool).await.unwrap();
    let store = PostgresResponseStepManager::new(pools);

    let model = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: None,
            parent_step_id: None,
            step_kind: StepKind::ModelCall,
            step_sequence: 1,
            request_payload: json!({}),
        })
        .await
        .unwrap();

    let tool_a = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: Some(model),
            parent_step_id: None,
            step_kind: StepKind::ToolCall,
            step_sequence: 2,
            request_payload: json!({"tool": "a"}),
        })
        .await
        .unwrap();

    let tool_b = store
        .create_step(CreateStepInput {
            id: None,
            request_id,
            prev_step_id: Some(tool_a),
            parent_step_id: None,
            step_kind: StepKind::ToolCall,
            step_sequence: 3,
            request_payload: json!({"tool": "b"}),
        })
        .await
        .unwrap();

    let chain = store.list_chain(request_id).await.unwrap();
    assert_eq!(chain.len(), 3);
    assert_eq!(chain[0].id, model);
    assert_eq!(chain[1].id, tool_a);
    assert_eq!(chain[2].id, tool_b);
    assert_eq!(chain[2].prev_step_id, Some(tool_a));
}
