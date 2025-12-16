-- Add escalation support for SLA race-based priority endpoint routing
-- This migration adds columns and indexes to track request escalation pairs
-- and race completion (supersession) handling.

-- Add escalation tracking columns to requests table
ALTER TABLE requests
    ADD COLUMN escalated_from_request_id UUID REFERENCES requests(id) ON DELETE SET NULL,
    ADD COLUMN is_escalated BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN superseded_at TIMESTAMPTZ NULL,
    ADD COLUMN superseded_by_request_id UUID REFERENCES requests(id) ON DELETE SET NULL;

-- Index for finding escalation pairs efficiently
-- Used by supersede_racing_request to find the other request in a race
CREATE INDEX idx_requests_escalated_from
ON requests(escalated_from_request_id)
WHERE escalated_from_request_id IS NOT NULL;

-- Index for finding superseded requests
-- Useful for filtering out superseded requests from batch status queries
CREATE INDEX idx_requests_superseded
ON requests(superseded_at)
WHERE superseded_at IS NOT NULL;

-- Index for finding escalated requests (priority queue)
-- Helps filter escalated requests when counting batch progress
CREATE INDEX idx_requests_is_escalated
ON requests(is_escalated)
WHERE is_escalated = true AND state = 'pending';

-- Composite index for escalation queries (model + escalation status)
-- Supports priority endpoint lookup and escalation filtering
CREATE INDEX idx_requests_model_escalated
ON requests(model, is_escalated, state)
WHERE state IN ('pending', 'claimed', 'processing');

-- Add constraint: escalated requests must reference an original request
-- Prevents orphaned escalated requests
ALTER TABLE requests
    ADD CONSTRAINT escalated_must_have_original
    CHECK ((is_escalated = false) OR (escalated_from_request_id IS NOT NULL));

-- Add constraint: cannot be both escalated and have an escalated_from reference
-- Prevents circular escalations (original can't reference escalated, escalated can't be original)
ALTER TABLE requests
    ADD CONSTRAINT no_double_escalation
    CHECK ((escalated_from_request_id IS NULL) OR (is_escalated = false));

-- Documentation comments
COMMENT ON COLUMN requests.escalated_from_request_id IS
    'If this is an escalated request (is_escalated=true), references the original request that was escalated';

COMMENT ON COLUMN requests.is_escalated IS
    'True if this request was created as an SLA escalation to a priority endpoint. Escalated requests are infrastructure and not counted in batch progress.';

COMMENT ON COLUMN requests.superseded_at IS
    'When this request was superseded by its racing pair completing first. Superseded requests are terminal and do not count toward batch completion.';

COMMENT ON COLUMN requests.superseded_by_request_id IS
    'Which request (original or escalated) completed first and superseded this one in the race';
