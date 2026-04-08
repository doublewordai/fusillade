DROP INDEX IF EXISTS idx_requests_reasoning_tokens;

ALTER TABLE requests
DROP COLUMN IF EXISTS reasoning_tokens,
DROP COLUMN IF EXISTS reasoning_artifact;
