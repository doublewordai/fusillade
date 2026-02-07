-- Add notification_sent_at column to track when batch completion notifications were sent
ALTER TABLE batches ADD COLUMN notification_sent_at TIMESTAMPTZ;

-- Backfill existing finalized batches so the notification daemon doesn't
-- try to send emails for historical batches
UPDATE batches SET notification_sent_at = COALESCE(completed_at, failed_at, cancelled_at)
    WHERE completed_at IS NOT NULL OR failed_at IS NOT NULL OR cancelled_at IS NOT NULL;

-- Index for efficiently finding batches that need notification
-- Covers both batches finalized by get_batch() and batches terminal by count
CREATE INDEX idx_batches_pending_notification
    ON batches (id)
    WHERE (completed_at IS NOT NULL OR failed_at IS NOT NULL OR cancelled_at IS NOT NULL)
      AND notification_sent_at IS NULL
      AND deleted_at IS NULL;
