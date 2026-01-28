-- Add support for filtering errors by retryability
-- This migration adds columns to track retriable vs non-retriable failures
-- Note: No backfill for historical data - this feature only works for new failures

-- 1. Add is_retriable_error column to requests table
-- NULL for historical requests, will be set for new failures
ALTER TABLE requests
ADD COLUMN is_retriable_error BOOLEAN NULL;

-- 2. Add separate failure count columns to batches table
-- Historical batches will have 0 for both (not backfilled)
ALTER TABLE batches
ADD COLUMN failed_requests_retriable BIGINT NOT NULL DEFAULT 0,
ADD COLUMN failed_requests_non_retriable BIGINT NOT NULL DEFAULT 0;

-- 3. Update the trigger to handle the new columns
CREATE OR REPLACE FUNCTION update_batch_on_request_change()
RETURNS TRIGGER AS $$
DECLARE
    old_state TEXT;
    new_state TEXT;
BEGIN
    -- Handle INSERT
    IF TG_OP = 'INSERT' THEN
        UPDATE batches
        SET total_requests = total_requests + 1,
            pending_requests = pending_requests + CASE WHEN NEW.state = 'pending' THEN 1 ELSE 0 END,
            in_progress_requests = in_progress_requests + CASE WHEN NEW.state IN ('claimed', 'processing') THEN 1 ELSE 0 END,
            completed_requests = completed_requests + CASE WHEN NEW.state = 'completed' THEN 1 ELSE 0 END,
            failed_requests = failed_requests + CASE WHEN NEW.state = 'failed' THEN 1 ELSE 0 END,
            failed_requests_retriable = failed_requests_retriable + CASE WHEN NEW.state = 'failed' AND NEW.is_retriable_error = true THEN 1 ELSE 0 END,
            failed_requests_non_retriable = failed_requests_non_retriable + CASE WHEN NEW.state = 'failed' AND (NEW.is_retriable_error = false OR NEW.is_retriable_error IS NULL) THEN 1 ELSE 0 END,
            canceled_requests = canceled_requests + CASE WHEN NEW.state = 'canceled' THEN 1 ELSE 0 END,
            requests_started_at = CASE WHEN requests_started_at IS NULL THEN NEW.created_at ELSE requests_started_at END,
            requests_last_updated_at = NEW.updated_at
        WHERE id = NEW.batch_id;
        RETURN NEW;
    END IF;

    -- Handle UPDATE (state transition)
    IF TG_OP = 'UPDATE' AND OLD.state != NEW.state THEN
        old_state := OLD.state;
        new_state := NEW.state;

        UPDATE batches
        SET
            -- Combined decrement/increment for each state
            pending_requests = pending_requests
                - CASE WHEN old_state = 'pending' THEN 1 ELSE 0 END
                + CASE WHEN new_state = 'pending' THEN 1 ELSE 0 END,
            in_progress_requests = in_progress_requests
                - CASE WHEN old_state IN ('claimed', 'processing') THEN 1 ELSE 0 END
                + CASE WHEN new_state IN ('claimed', 'processing') THEN 1 ELSE 0 END,
            completed_requests = completed_requests
                - CASE WHEN old_state = 'completed' THEN 1 ELSE 0 END
                + CASE WHEN new_state = 'completed' THEN 1 ELSE 0 END,
            failed_requests = failed_requests
                - CASE WHEN old_state = 'failed' THEN 1 ELSE 0 END
                + CASE WHEN new_state = 'failed' THEN 1 ELSE 0 END,
            -- Update retriable/non-retriable counts
            failed_requests_retriable = failed_requests_retriable
                - CASE WHEN old_state = 'failed' AND OLD.is_retriable_error = true THEN 1 ELSE 0 END
                + CASE WHEN new_state = 'failed' AND NEW.is_retriable_error = true THEN 1 ELSE 0 END,
            failed_requests_non_retriable = failed_requests_non_retriable
                - CASE WHEN old_state = 'failed' AND (OLD.is_retriable_error = false OR OLD.is_retriable_error IS NULL) THEN 1 ELSE 0 END
                + CASE WHEN new_state = 'failed' AND (NEW.is_retriable_error = false OR NEW.is_retriable_error IS NULL) THEN 1 ELSE 0 END,
            canceled_requests = canceled_requests
                - CASE WHEN old_state = 'canceled' THEN 1 ELSE 0 END
                + CASE WHEN new_state = 'canceled' THEN 1 ELSE 0 END,
            requests_last_updated_at = NEW.updated_at
        WHERE id = NEW.batch_id;
        RETURN NEW;
    END IF;

    -- Handle DELETE
    IF TG_OP = 'DELETE' THEN
        UPDATE batches
        SET total_requests = total_requests - 1,
            pending_requests = pending_requests - CASE WHEN OLD.state = 'pending' THEN 1 ELSE 0 END,
            in_progress_requests = in_progress_requests - CASE WHEN OLD.state IN ('claimed', 'processing') THEN 1 ELSE 0 END,
            completed_requests = completed_requests - CASE WHEN OLD.state = 'completed' THEN 1 ELSE 0 END,
            failed_requests = failed_requests - CASE WHEN OLD.state = 'failed' THEN 1 ELSE 0 END,
            failed_requests_retriable = failed_requests_retriable - CASE WHEN OLD.state = 'failed' AND OLD.is_retriable_error = true THEN 1 ELSE 0 END,
            failed_requests_non_retriable = failed_requests_non_retriable - CASE WHEN OLD.state = 'failed' AND (OLD.is_retriable_error = false OR OLD.is_retriable_error IS NULL) THEN 1 ELSE 0 END,
            canceled_requests = canceled_requests - CASE WHEN OLD.state = 'canceled' THEN 1 ELSE 0 END,
            requests_last_updated_at = NOW()
        WHERE id = OLD.batch_id;
        RETURN OLD;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 6. Create index for fast filtering on failed requests by retryability
CREATE INDEX idx_requests_failed_retriable ON requests(batch_id, is_retriable_error) WHERE state = 'failed';
