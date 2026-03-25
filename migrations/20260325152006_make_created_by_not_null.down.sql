-- Revert created_by to nullable

ALTER TABLE batches ALTER COLUMN created_by DROP NOT NULL;
ALTER TABLE batches ALTER COLUMN created_by DROP DEFAULT;

-- Restore the partial index
DROP INDEX IF EXISTS idx_batches_created_by;
CREATE INDEX idx_batches_created_by ON batches(created_by) WHERE created_by IS NOT NULL;
