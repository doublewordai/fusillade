-- Denormalize batches.deleted_at onto requests for cross-batch pagination performance.
--
-- Why: list_requests orders by requests.created_at DESC but filters on
-- batches.deleted_at IS NULL. These are anti-correlated in practice (recently
-- soft-deleted batches have their many recent requests all fail the filter),
-- so scanning the requests index backwards reads many "dead" pages before
-- finding LIMIT matches. Denormalizing lets us use a partial index aligned
-- with both the sort and the filter.

ALTER TABLE requests ADD COLUMN batch_deleted_at TIMESTAMPTZ;

-- Install the sync trigger BEFORE backfilling. If a batch were soft-deleted
-- between the backfill and trigger creation, its requests would keep
-- batch_deleted_at = NULL forever (query stays correct via b.deleted_at IS NULL
-- in the WHERE clause, but the partial index can't skip them). With the trigger
-- in place first, any concurrent soft-delete during the backfill is captured
-- automatically.
CREATE OR REPLACE FUNCTION sync_requests_batch_deleted_at()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.deleted_at IS DISTINCT FROM OLD.deleted_at THEN
        UPDATE requests
        SET batch_deleted_at = NEW.deleted_at
        WHERE batch_id = NEW.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sync_batch_deleted_at_to_requests
AFTER UPDATE OF deleted_at ON batches
FOR EACH ROW
EXECUTE FUNCTION sync_requests_batch_deleted_at();

-- Backfill: only requests whose batch is already soft-deleted need updating.
-- For large production databases, consider running this as a separate batched
-- backfill script rather than inside the migration.
UPDATE requests r
SET batch_deleted_at = b.deleted_at
FROM batches b
WHERE r.batch_id = b.id
  AND b.deleted_at IS NOT NULL;

-- Partial index aligned with list_requests pagination (created_at DESC, id DESC).
-- batch_id IS NOT NULL excludes orphaned rows (where FK cascade set batch_id to NULL).
--
-- NOTE: CREATE INDEX without CONCURRENTLY takes an ACCESS EXCLUSIVE lock on the
-- table for the duration of the build. On large tables this can be significant.
-- Create the index on production FIRST using CONCURRENTLY before deploying this
-- migration, then the IF NOT EXISTS below is a no-op:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_requests_active_pagination
--   ON requests (created_at DESC, id DESC)
--   WHERE batch_deleted_at IS NULL AND batch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_requests_active_pagination
ON requests (created_at DESC, id DESC)
WHERE batch_deleted_at IS NULL AND batch_id IS NOT NULL;
