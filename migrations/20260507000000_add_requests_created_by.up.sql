-- Denormalize created_by onto requests so the dashboard listing query can
-- filter by user without joining batches. The cross-table indirection prevents
-- Postgres from building a composite index on (created_by, sort_keys), which
-- means even users with no rows pay a full sort-index scan today.
--
-- Also relaxes requests.batch_id to NULL so responses can exist without a
-- parent batch row. The data migration below converts existing virtual-batch
-- responses (one batch per response, used as a workaround) into batchless
-- requests carrying their own created_by.

ALTER TABLE requests ADD COLUMN IF NOT EXISTS created_by TEXT NOT NULL DEFAULT '';

ALTER TABLE requests ALTER COLUMN batch_id DROP NOT NULL;

-- Two partial indexes covering the active-first and recency sort orderings of
-- list_requests, scoped to rows that own a created_by directly (i.e. the new
-- batchless responses; rows belonging to real batches still go through the
-- existing batch-driven path). Empty-string created_by ('') represents an
-- unattributed row and is excluded from the per-user listing indexes.
CREATE INDEX IF NOT EXISTS idx_requests_user_active_sort ON requests (
    created_by,
    (CASE state WHEN 'processing' THEN 0 WHEN 'claimed' THEN 1 WHEN 'pending' THEN 2 ELSE 3 END),
    created_at DESC,
    id DESC,
    service_tier
) WHERE created_by != '';

CREATE INDEX IF NOT EXISTS idx_requests_user_created_sort ON requests (
    created_by,
    created_at DESC,
    id DESC,
    service_tier
) WHERE created_by != '';

-- Update active_request_templates to include templates without a parent file.
-- Batchless responses use templates with file_id IS NULL; the daemon's
-- claim_requests join needs to see them.
DROP VIEW IF EXISTS active_request_templates;
CREATE VIEW active_request_templates AS
SELECT rt.*
FROM request_templates rt
LEFT JOIN files f ON rt.file_id = f.id
WHERE rt.file_id IS NULL OR f.deleted_at IS NULL;

-- Mirror the same change on active_requests (currently unused but kept in sync).
DROP VIEW IF EXISTS active_requests;
CREATE VIEW active_requests AS
SELECT r.*
FROM requests r
LEFT JOIN batches b ON r.batch_id = b.id
WHERE r.batch_id IS NULL OR b.deleted_at IS NULL;

-- Backfill of pre-existing virtual-batch responses lives in a separate
-- script (`scripts/backfill_responses_to_batchless.sql`) so the migration
-- itself only changes schema. Run it once after deploying this migration
-- to make older responses visible in the new listing.
