-- Add composite index for SLA-based claim query optimization
-- The claim query pattern is:
--   WHERE b.cancelling_at IS NULL
--   ORDER BY b.expires_at ASC NULLS LAST
--
-- This composite index supports both the filter and the sort in a single index scan.
-- Using a partial index (WHERE cancelling_at IS NULL) to only index active batches,
-- which reduces index size and improves performance for the common case.
CREATE INDEX idx_batches_active_by_expiration
ON batches(expires_at ASC NULLS LAST)
WHERE cancelling_at IS NULL;

COMMENT ON INDEX idx_batches_active_by_expiration IS
'Optimized for SLA-based claim queries: filters non-cancelled batches and sorts by expiration';

-- Add second index for SLA monitoring query optimization
-- The SLA monitoring query pattern is:
--   WHERE b.completed_at IS NULL
--     AND b.failed_at IS NULL
--     AND b.cancelled_at IS NULL
--   ORDER BY b.expires_at ASC
--
-- This partial index only includes non-terminal batches (not completed, failed, or cancelled).
-- The SLA checker runs less frequently (every minute) than claim queries (every second),
-- but still benefits from this optimized index.
CREATE INDEX idx_batches_sla_monitoring
ON batches(expires_at ASC NULLS LAST)
WHERE completed_at IS NULL
  AND failed_at IS NULL
  AND cancelled_at IS NULL;

COMMENT ON INDEX idx_batches_sla_monitoring IS
'Optimized for SLA monitoring queries: filters active (non-terminal) batches and sorts by expiration';

-- Note: We maintain two separate indexes because:
-- 1. idx_batches_active_by_expiration: optimizes claim queries (WHERE cancelling_at IS NULL)
-- 2. idx_batches_sla_monitoring: optimizes SLA queries (WHERE completed/failed/cancelled_at IS NULL)
-- The difference is that cancelling_at is set when cancellation starts (non-terminal),
-- while cancelled_at is set when cancellation completes (terminal state).
