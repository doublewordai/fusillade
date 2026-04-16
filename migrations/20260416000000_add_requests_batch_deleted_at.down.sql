DROP INDEX IF EXISTS idx_requests_active_pagination;
DROP TRIGGER IF EXISTS sync_batch_deleted_at_to_requests ON batches;
DROP FUNCTION IF EXISTS sync_requests_batch_deleted_at();
ALTER TABLE requests DROP COLUMN IF EXISTS batch_deleted_at;
