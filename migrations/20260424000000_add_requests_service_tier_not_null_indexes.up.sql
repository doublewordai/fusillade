-- Indexes for priority-tier requests, mirroring the existing flex-tier indexes.
--
-- The Responses page filters for `service_tier IN ('flex', 'priority')`.
-- Flex indexes already exist (idx_requests_active_first_flex, idx_requests_flex_created).
-- These priority indexes let Postgres combine both via BitmapOr.
--
-- NOTE: On large tables, create these CONCURRENTLY before deploying:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_requests_active_first_priority
--   ON requests (
--     (CASE state WHEN 'processing' THEN 0 WHEN 'claimed' THEN 1 WHEN 'pending' THEN 2 ELSE 3 END),
--     created_at DESC, id DESC
--   ) WHERE service_tier = 'priority';
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_requests_priority_created
--   ON requests (created_at DESC, id DESC)
--   WHERE service_tier = 'priority';

CREATE INDEX IF NOT EXISTS idx_requests_active_first_priority ON requests
  USING btree (
    (CASE state WHEN 'processing' THEN 0 WHEN 'claimed' THEN 1 WHEN 'pending' THEN 2 ELSE 3 END),
    created_at DESC, id DESC
  ) WHERE service_tier = 'priority';

CREATE INDEX IF NOT EXISTS idx_requests_priority_created ON requests
  USING btree (created_at DESC, id DESC)
  WHERE service_tier = 'priority';
