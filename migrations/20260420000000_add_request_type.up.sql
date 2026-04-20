ALTER TABLE requests ADD COLUMN request_type text NOT NULL DEFAULT 'batch'
  CHECK (request_type IN ('async', 'batch'));

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
--   ) WHERE request_type = 'async';
CREATE INDEX IF NOT EXISTS idx_requests_active_first_async ON requests
  USING btree (
    (CASE state WHEN 'processing' THEN 0 WHEN 'claimed' THEN 1 WHEN 'pending' THEN 2 ELSE 3 END),
    created_at DESC, id DESC
  ) WHERE request_type = 'async';
