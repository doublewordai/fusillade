-- Remove escalation support - replace with route-at-claim-time approach
-- This migration removes the SLA racing/supersession system in favor of
-- routing requests to escalated models at claim time based on time remaining.

-- First, clean up any escalated or superseded requests
-- These are infrastructure artifacts that won't exist under the new approach
DELETE FROM requests WHERE is_escalated = true OR state = 'superseded';

-- Drop the views that depend on the columns we're removing
DROP VIEW IF EXISTS active_requests;

-- Drop check constraints
ALTER TABLE requests DROP CONSTRAINT IF EXISTS only_escalated_has_parent;
ALTER TABLE requests DROP CONSTRAINT IF EXISTS escalated_must_have_original;

-- Drop indexes that reference the columns we're removing
DROP INDEX IF EXISTS idx_requests_model_escalated_state;
DROP INDEX IF EXISTS idx_requests_escalated_from_state;

-- Drop foreign key constraints on escalation columns
ALTER TABLE requests DROP CONSTRAINT IF EXISTS requests_escalated_from_request_id_fkey;
ALTER TABLE requests DROP CONSTRAINT IF EXISTS requests_superseded_by_request_id_fkey;

-- Now drop the columns
ALTER TABLE requests
    DROP COLUMN IF EXISTS superseded_by_request_id,
    DROP COLUMN IF EXISTS superseded_at,
    DROP COLUMN IF EXISTS is_escalated,
    DROP COLUMN IF EXISTS escalated_from_request_id;

-- Revert state_check constraint to original states (remove 'superseded')
ALTER TABLE requests DROP CONSTRAINT state_check;
ALTER TABLE requests ADD CONSTRAINT state_check CHECK (
    state IN ('pending', 'claimed', 'processing', 'completed', 'failed', 'canceled')
);

-- Recreate the active_requests view without the removed columns
CREATE VIEW active_requests AS
SELECT
    id,
    batch_id,
    template_id,
    state,
    retry_attempt,
    not_before,
    daemon_id,
    claimed_at,
    started_at,
    response_status,
    response_body,
    completed_at,
    error,
    failed_at,
    canceled_at,
    created_at,
    updated_at,
    custom_id,
    model,
    response_size
FROM requests
WHERE batch_id IS NOT NULL;
