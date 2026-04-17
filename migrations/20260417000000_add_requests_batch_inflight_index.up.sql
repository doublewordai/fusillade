-- Partial index for efficiently cascading batch terminal state to in-flight requests.
-- Covers the WHERE batch_id = $1 AND state IN ('pending', 'claimed', 'processing') pattern
-- used by cascade_batch_state_to_requests.
--
-- Rollout note: on large requests tables, pre-create this index outside the transactional
-- migration with:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_requests_batch_inflight
--   ON requests(batch_id)
--   WHERE state IN ('pending', 'claimed', 'processing');
-- The migration then becomes a no-op via IF NOT EXISTS, avoiding locks during deploys.
CREATE INDEX IF NOT EXISTS idx_requests_batch_inflight
ON requests(batch_id)
WHERE state IN ('pending', 'claimed', 'processing');
