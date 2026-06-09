-- Async model-filters claim gate (Solution 5, COR-432).
--
-- `model_filters` is an APPEND-ONLY EVENT LOG of per-model internal liveness
-- transitions. Each row records that, at `created_at`, a model became `live`
-- (serving now), `coming` (will serve, with an ETA in `expected_ready_at`), or
-- `absent` (an explicit tombstone: scouter is no longer deploying the model).
--
-- The CURRENT state of a model is the LATEST event (max `created_at`) for that
-- model. A model is treated as absent if its latest event has `state='absent'`
-- OR it has no events at all. The daemon's claim cycle consults the latest
-- event per model to decide, per request against an effective deadline,
-- whether to claim now (dispatch -> onwards routes internal/OpenRouter) or hold
-- the request in the queue waiting for imminent internal capacity.
--
-- Append-only (rather than current-state upsert) lets us learn model load
-- durations from the log: pairing each `coming` event with the next `live`
-- event for that model yields observed load times (see `model_load_estimate`),
-- which scouter uses to set future ETAs. Retraction is appending an `absent`
-- event, never a DELETE.
--
-- scouter is responsible for appending only on a state CHANGE (so the log
-- stays a transition log, not a poll log). Old events are purged by the daemon
-- while always retaining the latest event per model (see the purge task).
CREATE TABLE model_filters (
    id                BIGSERIAL PRIMARY KEY,
    model             TEXT NOT NULL,
    state             TEXT NOT NULL CHECK (state IN ('live', 'coming', 'absent')),
    expected_ready_at TIMESTAMPTZ, -- set when state = 'coming'
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Latest-event lookup per model (the claim query's LATERAL ORDER BY
-- created_at DESC LIMIT 1) and the load-estimate scan both walk events for a
-- single model newest-first.
CREATE INDEX idx_model_filters_model_created_at
    ON model_filters (model, created_at DESC);

-- Singleton sync heartbeat so the daemon can distinguish "scouter is alive,
-- absence is meaningful" from "scouter is dead, the table is frozen". scouter
-- bumps `updated_at` on every sync; the daemon fails closed (only the
-- deadline-release clause claims) when the heartbeat is stale.
CREATE TABLE model_filters_sync (
    id         BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO model_filters_sync (id, updated_at) VALUES (true, now());
