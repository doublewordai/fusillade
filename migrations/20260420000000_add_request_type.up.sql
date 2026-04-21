-- Add column: metadata-only operation in PG11+, instant on any table size.
ALTER TABLE requests ADD COLUMN request_type text NOT NULL DEFAULT 'batch';

-- NOT VALID skips the full-table validation scan (was 6m22s on 38M rows).
-- New inserts are checked immediately; existing rows are validated after
-- the backfill via a separate VALIDATE CONSTRAINT statement.
ALTER TABLE requests ADD CONSTRAINT requests_request_type_check
  CHECK (request_type IN ('async', 'batch')) NOT VALID;

-- Expression index supporting list_requests filtered to async requests.
--
-- NOTE: CREATE INDEX without CONCURRENTLY takes an ACCESS EXCLUSIVE lock for
-- the duration of the build. On large tables create this CONCURRENTLY ahead
-- of applying the migration; the IF NOT EXISTS below makes the migration a
-- no-op in that case:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_requests_active_first_async
--   ON requests (
--     (CASE state WHEN 'processing' THEN 0 WHEN 'claimed' THEN 1 WHEN 'pending' THEN 2 ELSE 3 END),
--     created_at DESC, id DESC
--   ) WHERE request_type = 'async' AND batch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_requests_active_first_async ON requests
  USING btree (
    (CASE state WHEN 'processing' THEN 0 WHEN 'claimed' THEN 1 WHEN 'pending' THEN 2 ELSE 3 END),
    created_at DESC, id DESC
  ) WHERE request_type = 'async' AND batch_id IS NOT NULL;
