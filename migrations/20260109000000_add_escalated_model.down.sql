-- Revert escalated_model and escalated_api_key column additions

DROP INDEX IF EXISTS idx_requests_escalated_model;

ALTER TABLE requests
    DROP COLUMN escalated_model,
    DROP COLUMN escalated_api_key;
