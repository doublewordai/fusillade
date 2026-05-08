-- One-shot cleanup that converts pre-existing realtime/flex responses from
-- virtual-batch storage into the new batchless shape introduced in migration
-- 20260507000000.
--
-- Background. Until that migration, every response (priority/realtime and
-- flex) was stored as a single-row "virtual" batch: a fresh `files` row with
-- name='single_request' and purpose='batch', a `batches` row pointing at it,
-- and one `requests` row pointing at the batch. The new code path inserts
-- those responses without a batch — `requests.created_by` is set on the row
-- itself and `requests.batch_id` is NULL.
--
-- The migration adds the column and relaxes the NOT NULL but doesn't migrate
-- existing data, so anyone who deploys it ends up with their old responses
-- invisible to the new listing query (which scopes on `r.created_by != ''`).
-- Run this script once post-deploy to fix that.
--
-- Behaviour:
--   1. Hard-delete request rows whose virtual batch was soft-deleted
--      (`b.deleted_at IS NOT NULL`). The pre-migration listing filtered
--      these out via `b.deleted_at IS NULL`, so dropping the row preserves
--      the tombstone.
--   2. For live virtual batches, copy `b.created_by` onto the request and
--      null out `batch_id`. The row then satisfies the new listing's
--      `r.created_by != ''` predicate and stops joining batches.
--   3. Virtual `batches` and `files` rows are intentionally left in place;
--      they're orphan rows now (no children) but harmless. A separate
--      cleanup pass can prune them later.
--
-- Idempotent: re-running is a no-op once all virtual-batch responses have
-- been processed (the WHERE clauses match nothing on a second pass).

BEGIN;

-- 1. Tombstoned responses: hard-delete.
DELETE FROM requests
WHERE batch_id IN (
    SELECT b.id FROM batches b
    JOIN files f ON b.file_id = f.id
    WHERE f.name = 'single_request'
      AND f.purpose = 'batch'
      AND b.deleted_at IS NOT NULL
);

-- 2. Live virtual-batch responses: copy attribution and detach from the batch.
UPDATE requests r
SET created_by = b.created_by, batch_id = NULL
FROM batches b
JOIN files f ON b.file_id = f.id
WHERE r.batch_id = b.id
  AND f.name = 'single_request'
  AND f.purpose = 'batch'
  AND b.deleted_at IS NULL;

COMMIT;
