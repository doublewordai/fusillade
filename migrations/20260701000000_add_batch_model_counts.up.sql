-- Per-batch model totals for fast queue-pressure monitoring.
--
-- `get_pending_request_counts_by_model_and_window` is used by control-layer
-- monitoring/scheduling. Exact active-state counts for batch-backed requests
-- require scanning high-churn `requests` rows and can time out on large queues.
-- This summary lets the read path count batch-backed pressure from small,
-- stable rows and reserve `requests` scans for batchless rows only.

CREATE TABLE IF NOT EXISTS batch_model_counts (
    batch_id UUID NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    model TEXT NOT NULL,
    total_requests BIGINT NOT NULL CHECK (total_requests >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, model)
);

CREATE INDEX IF NOT EXISTS idx_batch_model_counts_model_batch
ON batch_model_counts (model, batch_id);

-- Backfill only batches that can currently affect queue-depth monitoring.
--
-- Keep this deliberately cheap for production deploys: use the existing
-- `batches.total_requests` value and a single indexed model lookup per active
-- batch. That may attribute already-live mixed-model batches to one model, but
-- avoids a deployment-time aggregation over large `request_templates`. Batches
-- created after this migration get exact per-model counts from the write path.
INSERT INTO batch_model_counts (batch_id, model, total_requests)
SELECT
    b.id,
    first_template.model,
    b.total_requests
FROM batches b
JOIN LATERAL (
    SELECT t.model
    FROM request_templates t
    WHERE t.file_id = b.file_id
    ORDER BY t.model
    LIMIT 1
) first_template ON TRUE
WHERE b.cancelling_at IS NULL
  AND b.deleted_at IS NULL
  AND b.completed_at IS NULL
  AND b.failed_at IS NULL
  AND b.cancelled_at IS NULL
  AND b.file_id IS NOT NULL
  AND b.total_requests > 0
ON CONFLICT (batch_id, model) DO UPDATE
SET total_requests = EXCLUDED.total_requests,
    updated_at = NOW();
