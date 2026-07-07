INSERT INTO requests (
    id, batch_id, template_id, state, retry_attempt, not_before,
    daemon_id, claimed_at, started_at, response_status, response_body,
    completed_at, error, failed_at, canceled_at, created_at, updated_at,
    custom_id, response_size, model, routed_model, service_tier, created_by
)
SELECT
    a.id, a.batch_id, a.template_id, a.state, a.retry_attempt,
    a.not_before, a.daemon_id, a.claimed_at, a.started_at,
    a.response_status, a.response_body, a.completed_at, a.error,
    a.failed_at, a.canceled_at, a.created_at, a.updated_at,
    a.custom_id, a.response_size, a.model, a.routed_model,
    a.service_tier, a.created_by
FROM requests_archive a
ON CONFLICT (id) DO NOTHING;

DROP FUNCTION IF EXISTS archive_terminal_batch_requests(UUID);
DROP TABLE IF EXISTS requests_archive;

ALTER TABLE batches
    DROP COLUMN IF EXISTS in_progress_requests,
    DROP COLUMN IF EXISTS completed_requests,
    DROP COLUMN IF EXISTS failed_requests,
    DROP COLUMN IF EXISTS canceled_requests;
