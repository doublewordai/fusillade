-- Revert escalation support changes
-- This rollback removes all escalation-related columns, indexes, and constraints

-- Drop indexes (in reverse order of creation)
DROP INDEX IF EXISTS idx_requests_model_escalated;
DROP INDEX IF EXISTS idx_requests_is_escalated;
DROP INDEX IF EXISTS idx_requests_superseded;
DROP INDEX IF EXISTS idx_requests_escalated_from;

-- Drop constraints
ALTER TABLE requests DROP CONSTRAINT IF EXISTS no_double_escalation;
ALTER TABLE requests DROP CONSTRAINT IF EXISTS escalated_must_have_original;

-- Drop columns (in reverse order of addition)
ALTER TABLE requests
    DROP COLUMN IF EXISTS superseded_by_request_id,
    DROP COLUMN IF EXISTS superseded_at,
    DROP COLUMN IF EXISTS is_escalated,
    DROP COLUMN IF EXISTS escalated_from_request_id;
