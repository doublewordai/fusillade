-- SLA-based queue prioritization and monitoring
-- This migration adds:
-- 1. Indexes for SLA-based claim queries and monitoring
-- 2. NOT NULL constraint on expires_at (required for queue prioritization)

-- First, ensure there are no NULL expires_at values
-- If any exist, this will fail and prevent the migration - intentional!
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM batches WHERE expires_at IS NULL) THEN
        RAISE EXCEPTION 'Cannot make expires_at NOT NULL: found batches with NULL expires_at. These batches are unprocessable and should be investigated.';
    END IF;
END $$;

-- Add NOT NULL constraint to expires_at
-- Batches without expires_at will never be processed (NULLS LAST in queue)
ALTER TABLE batches ALTER COLUMN expires_at SET NOT NULL;

-- Drop the old partial index (only indexed non-NULL values)
DROP INDEX IF EXISTS idx_batches_expires_at;

-- Add composite index for SLA-based claim query optimization
-- The claim query pattern is:
--   WHERE b.cancelling_at IS NULL
--   ORDER BY b.expires_at ASC NULLS LAST
--
-- This composite index supports both the filter and the sort in a single index scan.
-- Using a partial index (WHERE cancelling_at IS NULL) to only index active batches,
-- which reduces index size and improves performance for the common case.
-- Note: No NULLS LAST needed now that expires_at is NOT NULL
CREATE INDEX idx_batches_active_by_expiration
ON batches(expires_at ASC)
WHERE cancelling_at IS NULL;

COMMENT ON INDEX idx_batches_active_by_expiration IS
'Optimized for SLA-based claim queries: filters non-cancelled batches and sorts by expiration';

-- Add index for SLA monitoring request-level queries
-- This partial index helps find pending requests efficiently and supports:
-- - Filtering: state = 'pending' (via partial index WHERE clause)
-- - JOIN: batch_id for joining to batches table
-- - Ordering: created_at for FIFO within batch (after sorting by batch expires_at)
CREATE INDEX idx_requests_pending_sla
ON requests(batch_id, created_at)
WHERE state = 'pending';

COMMENT ON INDEX idx_requests_pending_sla IS
'Optimized for SLA monitoring: finds pending requests, supports JOIN to batches and FIFO ordering';
