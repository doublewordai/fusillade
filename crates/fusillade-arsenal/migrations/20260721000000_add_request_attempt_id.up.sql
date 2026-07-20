-- This migration follows the shipped archive-table migrations. Adding the
-- column to both tables keeps their named request-column shapes identical,
-- while archive moves use explicit mappings because archive_bucket precedes
-- attempt_id on databases upgraded from an earlier release.
ALTER TABLE requests
ADD COLUMN attempt_id UUID;

ALTER TABLE batch_requests_archive
ADD COLUMN attempt_id UUID;

COMMENT ON COLUMN requests.attempt_id IS
    'Unique ownership token for the currently claimed daemon execution';

COMMENT ON COLUMN batch_requests_archive.attempt_id IS
    'Ownership token mirrored from requests; terminal archived rows normally store NULL';
