-- Add cached_tokens column to requests to track prompt-prefix cache hits.
-- Tracking-only for now: records how many leading "tokens" of this request's
-- body matched a previously-seen request in the same (created_by, model, path)
-- scope. No discount is applied yet. Populated at dispatch time (inline for
-- realtime, in the daemon process task for flex/batch), so it defaults to 0
-- and is filled in as requests are processed.

ALTER TABLE requests
ADD COLUMN cached_tokens BIGINT NOT NULL DEFAULT 0;

COMMENT ON COLUMN requests.cached_tokens IS
    'Approximate number of leading prompt tokens served from the prefix cache for this request (shared with a prior request in the same user/model/route scope). Tracking-only; no billing discount applied.';
