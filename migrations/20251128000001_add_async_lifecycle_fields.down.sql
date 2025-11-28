-- Revert asynchronous lifecycle fields

DROP INDEX IF EXISTS idx_files_needs_deletion;
DROP INDEX IF EXISTS idx_batches_needs_initialization;

ALTER TABLE files DROP COLUMN IF EXISTS deleting_daemon_id;
ALTER TABLE files DROP COLUMN IF EXISTS deleting_at;
ALTER TABLE batches DROP COLUMN IF EXISTS initializing_daemon_id;
ALTER TABLE batches DROP COLUMN IF EXISTS initialized_at;
