-- Partial index for efficiently cascading batch terminal state to in-flight requests.
-- Covers the WHERE batch_id = $1 AND state IN ('pending', 'claimed', 'processing') pattern
-- used by cascade_batch_state_to_requests.
CREATE INDEX IF NOT EXISTS idx_requests_batch_inflight
ON requests(batch_id)
WHERE state IN ('pending', 'claimed', 'processing');
