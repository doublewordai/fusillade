ALTER TABLE batch_requests_archive
DROP COLUMN attempt_id;

ALTER TABLE requests
DROP COLUMN attempt_id;
