ALTER TABLE requests ADD COLUMN request_type text NOT NULL DEFAULT 'batch'
  CHECK (request_type IN ('async', 'batch'));

CREATE INDEX idx_requests_active_first_async ON requests
  USING btree (
    (CASE state WHEN 'processing' THEN 0 WHEN 'claimed' THEN 1 WHEN 'pending' THEN 2 ELSE 3 END),
    created_at DESC, id DESC
  ) WHERE request_type = 'async';
