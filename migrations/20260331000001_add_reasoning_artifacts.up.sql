ALTER TABLE requests
ADD COLUMN reasoning_artifact JSONB NULL,
ADD COLUMN reasoning_tokens BIGINT NULL;

CREATE INDEX idx_requests_reasoning_tokens
ON requests (reasoning_tokens)
WHERE reasoning_tokens IS NOT NULL AND reasoning_tokens > 0;
