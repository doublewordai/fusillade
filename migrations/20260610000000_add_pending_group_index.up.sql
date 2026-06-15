-- Bounded-claim support for large single-model backlogs.
--
-- The claim query selects the top-`capacity` pending requests per model by an
-- effective-deadline (D_eff) ranking. The old shape read + full-sorted the
-- entire per-model pending set every cycle (SKIP LOCKED + a runtime sort key
-- can't be index-served), costing seconds for a model with millions pending.
--
-- The claim now ranks BATCHES (whose sort key is constant across their rows)
-- and pulls rows only from the winning batches. This index serves that pull:
-- for a given (model, batch_id) it returns the oldest pending rows by
-- created_at via an index-ordered scan (no per-batch sort), and it also backs
-- the batchless (batch_id IS NULL) per-model scan. Batch enumeration itself
-- (the loose index scan over distinct batch_ids) uses the existing
-- idx_requests_pending (model, batch_id).
CREATE INDEX idx_requests_pending_claim_pull
    ON requests (model, batch_id, created_at)
    WHERE state = 'pending' AND template_id IS NOT NULL;
