DROP INDEX IF EXISTS idx_requests_active_first_async;
ALTER TABLE requests DROP COLUMN IF EXISTS request_type;
