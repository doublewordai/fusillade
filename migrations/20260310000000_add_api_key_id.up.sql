-- Add api_key_id to batches for per-member usage attribution within orgs.
-- When a batch is created via the dashboard, the hidden batch API key used
-- is recorded here. JOIN through api_keys.created_by to get the individual.
ALTER TABLE batches ADD COLUMN api_key_id UUID;
CREATE INDEX idx_batches_api_key_id ON batches(api_key_id) WHERE api_key_id IS NOT NULL;

-- Add api_key_id to files for the same reason.
ALTER TABLE files ADD COLUMN api_key_id UUID;
CREATE INDEX idx_files_api_key_id ON files(api_key_id) WHERE api_key_id IS NOT NULL;
