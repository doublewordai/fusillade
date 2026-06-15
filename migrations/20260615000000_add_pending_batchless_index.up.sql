-- Index for the claim query's BATCHLESS (flex/async) arms.
--
-- `claim_requests` has two batchless arms (Source A full-capacity and Source B
-- leaky-bucket) that scan pending rows with `batch_id IS NULL` per model. No
-- existing index covers `batch_id IS NULL`, so Postgres uses idx_requests_state
-- (or idx_requests_state_model) and filters `batch_id IS NULL` row-by-row. When
-- a model has a large BATCHED backlog (e.g. millions of pending rows in a few
-- big batches), each batchless arm scans the entire pending set to find the
-- (often zero) batchless rows — measured at ~4s per scan on a 53M-row table,
-- and the claim runs two such arms per model every cycle.
--
-- This partial index contains ONLY batchless pending rows (normally a tiny set),
-- so both arms become an instant index range scan instead of filtering the
-- whole pending heap. `(model, created_at)` serves the per-model lookup and the
-- arms' `ORDER BY created_at` / oldest-first pull.
--
-- On large production tables, create this CONCURRENTLY before deploying so the
-- IF NOT EXISTS statement below becomes a no-op:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_requests_pending_batchless
--   ON requests (model, created_at)
--   WHERE state = 'pending' AND batch_id IS NULL AND template_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_requests_pending_batchless
ON requests (model, created_at)
WHERE state = 'pending' AND batch_id IS NULL AND template_id IS NOT NULL;
