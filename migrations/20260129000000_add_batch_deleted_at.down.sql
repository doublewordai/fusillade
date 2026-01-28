-- Remove deleted_at column from batches table
DROP INDEX IF EXISTS idx_batches_deleted_at;
ALTER TABLE batches DROP COLUMN IF EXISTS deleted_at;
