-- Add column: metadata-only operation in PG11+, instant on any table size.
ALTER TABLE requests ADD COLUMN service_tier text NOT NULL DEFAULT 'flex';

-- NOT VALID skips the full-table validation scan.
-- New inserts are checked immediately; existing rows are validated after
-- the backfill via a separate VALIDATE CONSTRAINT statement.
ALTER TABLE requests ADD CONSTRAINT requests_service_tier_check
  CHECK (service_tier IN ('auto', 'default', 'flex', 'priority')) NOT VALID;

-- Partial index for default-tier requests with active_first=true ordering.
--
-- NOTE: CREATE INDEX without CONCURRENTLY takes an ACCESS EXCLUSIVE lock for
-- the duration of the build. On large tables create this CONCURRENTLY ahead
-- of applying the migration; the IF NOT EXISTS below makes the migration a
-- no-op in that case:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_requests_active_first_default
--   ON requests (
--     (CASE state WHEN 'processing' THEN 0 WHEN 'claimed' THEN 1 WHEN 'pending' THEN 2 ELSE 3 END),
--     created_at DESC, id DESC
--   ) WHERE service_tier = 'default' AND batch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_requests_active_first_default ON requests
  USING btree (
    (CASE state WHEN 'processing' THEN 0 WHEN 'claimed' THEN 1 WHEN 'pending' THEN 2 ELSE 3 END),
    created_at DESC, id DESC
  ) WHERE service_tier = 'default' AND batch_id IS NOT NULL;

-- Partial index for default-tier requests with active_first=false ordering
-- (created_at DESC, id DESC).
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_requests_default_created
--   ON requests (created_at DESC, id DESC)
--   WHERE service_tier = 'default' AND batch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_requests_default_created ON requests
  USING btree (created_at DESC, id DESC)
  WHERE service_tier = 'default' AND batch_id IS NOT NULL;
