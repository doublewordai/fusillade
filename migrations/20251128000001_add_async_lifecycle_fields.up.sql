-- Add fields to support asynchronous batch initialization and file deletion

-- Add initialized_at to batches table
-- NULL = batch needs initialization (bulk insert of requests not yet done)
-- NOT NULL = batch has been initialized
ALTER TABLE batches ADD COLUMN initialized_at TIMESTAMPTZ;

-- Add initializing_daemon_id to track which daemon is initializing a batch
ALTER TABLE batches ADD COLUMN initializing_daemon_id UUID;

-- Add deleting_at to files table
-- NULL = file is active
-- NOT NULL = file is marked for deletion
ALTER TABLE files ADD COLUMN deleting_at TIMESTAMPTZ;

-- Add deleting_daemon_id to track which daemon is deleting a file
ALTER TABLE files ADD COLUMN deleting_daemon_id UUID;

-- Set initialized_at for all existing batches (they're already initialized)
UPDATE batches SET initialized_at = created_at WHERE initialized_at IS NULL;

-- Add index to efficiently find batches needing initialization
CREATE INDEX idx_batches_needs_initialization ON batches(created_at) WHERE initialized_at IS NULL;

-- Add index to efficiently find files needing deletion
CREATE INDEX idx_files_needs_deletion ON files(deleting_at) WHERE deleting_at IS NOT NULL;
