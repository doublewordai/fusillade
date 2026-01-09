-- Add escalated_model and escalated_api_key columns to support model-based escalation
-- When not null, these override the original model and api_key fields
-- The 'model' field remains as the user-requested model (shown to user)
-- This enables escalation to different models while maintaining user transparency

ALTER TABLE requests
    ADD COLUMN escalated_model TEXT NULL,
    ADD COLUMN escalated_api_key TEXT NULL;

-- Add index for routing efficiency
-- Daemon needs to quickly find which model to actually use
CREATE INDEX idx_requests_escalated_model
ON requests(escalated_model)
WHERE escalated_model IS NOT NULL;

-- Documentation
COMMENT ON COLUMN requests.escalated_model IS
    'When set, the actual model to send the request to (overrides model field). Used for SLA escalation to more powerful models while keeping the original model visible to the user.';

COMMENT ON COLUMN requests.escalated_api_key IS
    'When set, the API key to use for the escalated request (overrides api_key field). Used together with escalated_model to allow escalation to models requiring different credentials.';
