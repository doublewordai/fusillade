-- Expand phase for moving terminal batch rows out of the hot requests table.
--
-- `requests` remains the live scratch table. This migration intentionally
-- creates only a batch archive: realtime/flex batchless rows stay in requests
-- until the separate responses-store slice.

ALTER TABLE batches
    ADD COLUMN IF NOT EXISTS in_progress_requests BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS completed_requests BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS failed_requests BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS canceled_requests BIGINT NOT NULL DEFAULT 0;

COMMENT ON COLUMN batches.in_progress_requests IS
    'Frozen terminal snapshot written before archiving requests. Zero for finalized batches; active batch in-progress counts are computed from requests.';
COMMENT ON COLUMN batches.completed_requests IS
    'Frozen terminal snapshot written before archiving requests. Active batch completed counts are computed from requests.';
COMMENT ON COLUMN batches.failed_requests IS
    'Frozen terminal snapshot written before archiving requests. Active batch failed counts are computed from requests.';
COMMENT ON COLUMN batches.canceled_requests IS
    'Frozen terminal snapshot written before archiving requests. Active batch canceled counts are computed from requests.';

CREATE TABLE IF NOT EXISTS requests_archive (
    LIKE requests INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS,
    CONSTRAINT requests_archive_batch_terminal_check
        CHECK (
            batch_id IS NOT NULL
            AND created_by IS NULL
            AND state IN ('completed', 'failed', 'canceled')
        )
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'requests_archive_pkey'
          AND conrelid = 'requests_archive'::regclass
    ) THEN
        ALTER TABLE requests_archive
            ADD CONSTRAINT requests_archive_pkey PRIMARY KEY (id);
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_requests_archive_completed_by_batch
    ON requests_archive (batch_id, completed_at, id)
    WHERE state = 'completed';

CREATE INDEX IF NOT EXISTS idx_requests_archive_failed_by_batch
    ON requests_archive (batch_id, failed_at, id)
    WHERE state = 'failed';

CREATE INDEX IF NOT EXISTS idx_requests_archive_batch_template
    ON requests_archive (batch_id, template_id);

CREATE OR REPLACE FUNCTION archive_terminal_batch_requests(target_batch_id UUID)
RETURNS BIGINT AS $$
DECLARE
    archived_count BIGINT := 0;
BEGIN
    WITH all_rows AS (
        SELECT r.id, r.state
        FROM requests r
        WHERE r.batch_id = target_batch_id

        UNION ALL

        SELECT a.id, a.state
        FROM requests_archive a
        WHERE a.batch_id = target_batch_id
          AND NOT EXISTS (
              SELECT 1 FROM requests r WHERE r.id = a.id
          )
    ),
    counts AS (
        SELECT
            COUNT(*) FILTER (WHERE state = 'completed')::BIGINT AS completed,
            COUNT(*) FILTER (WHERE state = 'failed')::BIGINT AS failed,
            COUNT(*) FILTER (WHERE state = 'canceled')::BIGINT AS canceled
        FROM all_rows
    ),
    ready_batch AS (
        SELECT
            b.id,
            c.completed,
            c.failed,
            c.canceled
        FROM batches b
        CROSS JOIN counts c
        WHERE b.id = target_batch_id
          AND b.total_requests > 0
          AND (
              b.completed_at IS NOT NULL
              OR b.failed_at IS NOT NULL
              OR b.cancelled_at IS NOT NULL
          )
          AND (c.completed + c.failed + c.canceled) = b.total_requests
    ),
    frozen AS (
        UPDATE batches b
        SET completed_requests = rb.completed,
            failed_requests = rb.failed,
            canceled_requests = rb.canceled,
            in_progress_requests = 0
        FROM ready_batch rb
        WHERE b.id = rb.id
        RETURNING b.id
    ),
    candidates AS (
        SELECT r.*
        FROM requests r
        JOIN frozen f ON f.id = r.batch_id
        WHERE r.state IN ('completed', 'failed', 'canceled')
          -- Keep any row that backs a model_call response_step in requests.
          -- The responses-store slice will give those rows their own storage;
          -- until then, deleting them would cascade the step through the FK.
          AND NOT EXISTS (
              SELECT 1
              FROM response_steps s
              WHERE s.request_id = r.id
          )
    ),
    upserted AS (
        INSERT INTO requests_archive (
            id, batch_id, template_id, state, retry_attempt, not_before,
            daemon_id, claimed_at, started_at, response_status, response_body,
            completed_at, error, failed_at, canceled_at, created_at, updated_at,
            custom_id, response_size, model, routed_model, service_tier,
            created_by
        )
        SELECT
            c.id, c.batch_id, c.template_id, c.state, c.retry_attempt,
            c.not_before, c.daemon_id, c.claimed_at, c.started_at,
            c.response_status, c.response_body, c.completed_at, c.error,
            c.failed_at, c.canceled_at, c.created_at, c.updated_at,
            c.custom_id, c.response_size, c.model, c.routed_model,
            c.service_tier, c.created_by
        FROM candidates c
        ON CONFLICT (id) DO UPDATE
        SET batch_id = EXCLUDED.batch_id,
            template_id = EXCLUDED.template_id,
            state = EXCLUDED.state,
            retry_attempt = EXCLUDED.retry_attempt,
            not_before = EXCLUDED.not_before,
            daemon_id = EXCLUDED.daemon_id,
            claimed_at = EXCLUDED.claimed_at,
            started_at = EXCLUDED.started_at,
            response_status = EXCLUDED.response_status,
            response_body = EXCLUDED.response_body,
            completed_at = EXCLUDED.completed_at,
            error = EXCLUDED.error,
            failed_at = EXCLUDED.failed_at,
            canceled_at = EXCLUDED.canceled_at,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            custom_id = EXCLUDED.custom_id,
            response_size = EXCLUDED.response_size,
            model = EXCLUDED.model,
            routed_model = EXCLUDED.routed_model,
            service_tier = EXCLUDED.service_tier,
            created_by = EXCLUDED.created_by
        RETURNING id
    ),
    deleted_active AS (
        DELETE FROM requests r
        USING upserted u
        WHERE r.id = u.id
        RETURNING r.id
    )
    SELECT COUNT(*)::BIGINT
    FROM deleted_active
    INTO archived_count;

    RETURN archived_count;
END;
$$ LANGUAGE plpgsql;
