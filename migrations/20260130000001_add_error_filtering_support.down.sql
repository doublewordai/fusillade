-- Rollback error filtering support

-- 1. Drop the index
DROP INDEX IF EXISTS idx_requests_failed_retriable;

-- 2. Restore the original trigger function (without error filtering)
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
            canceled_requests = canceled_requests - CASE WHEN OLD.state = 'canceled' THEN 1 ELSE 0 END,
            requests_last_updated_at = NOW()
        WHERE id = OLD.batch_id;
        RETURN OLD;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 3. Drop the new count columns from batches
ALTER TABLE batches
DROP COLUMN IF EXISTS failed_requests_retriable,
DROP COLUMN IF EXISTS failed_requests_non_retriable;

-- 4. Drop the is_retriable_error column from requests
ALTER TABLE requests
DROP COLUMN IF EXISTS is_retriable_error;
