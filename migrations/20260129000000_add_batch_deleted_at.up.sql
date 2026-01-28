-- Add deleted_at column to batches table for soft delete support
-- This enables proper soft deletes instead of orphaning child records via ON DELETE SET NULL

ALTER TABLE batches ADD COLUMN deleted_at TIMESTAMPTZ;

-- Partial index for efficient filtering of non-deleted batches
CREATE INDEX idx_batches_deleted_at ON batches(deleted_at) WHERE deleted_at IS NULL;

COMMENT ON COLUMN batches.deleted_at IS 'When batch was soft-deleted. NULL means active. Set when batch or parent file is deleted.';
