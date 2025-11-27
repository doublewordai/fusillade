-- Remove size finalization tracking from files
DROP INDEX IF EXISTS idx_files_size_finalized;

ALTER TABLE files
DROP COLUMN IF EXISTS size_finalized;

-- Remove response size tracking from requests
DROP INDEX IF EXISTS idx_requests_batch_state_size;

ALTER TABLE requests
DROP COLUMN IF EXISTS response_size;