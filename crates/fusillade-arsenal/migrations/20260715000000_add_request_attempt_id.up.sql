ALTER TABLE requests
ADD COLUMN attempt_id UUID;

COMMENT ON COLUMN requests.attempt_id IS
    'Unique ownership token for the currently claimed daemon execution';
