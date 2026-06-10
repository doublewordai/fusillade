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
--
-- The producer (scouter) owns the `state`/`expected_ready_at` invariant
-- (`expected_ready_at` set iff `state='coming'`). We deliberately do NOT add a
-- hard CHECK for it: this is an append-only event log of historical
-- transitions, and the claim gate only reads `expected_ready_at` when
-- `state='coming'`, so a stray value on a non-`coming` event is inert.
CREATE TABLE model_filters (
    id                BIGSERIAL PRIMARY KEY,
    model             TEXT NOT NULL,
    state             TEXT NOT NULL CHECK (state IN ('live', 'coming', 'absent')),
    expected_ready_at TIMESTAMPTZ, -- set when state = 'coming' (producer-owned invariant)
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Latest-event lookup per model (the claim query's LATERAL ORDER BY
-- created_at DESC, id DESC LIMIT 1) and the load-estimate scan both walk events
-- for a single model newest-first. `id DESC` is in the index so the latest
-- event is deterministic (and the ORDER BY is index-satisfiable) when a batch
-- append shares one `created_at` across rows.
CREATE INDEX idx_model_filters_model_created_at
    ON model_filters (model, created_at DESC, id DESC);

-- Singleton sync heartbeat so the daemon can tell whether scouter is actively
-- syncing. scouter bumps `updated_at` on every sync; when the heartbeat is
-- stale (or absent) the claim gate fails OPEN — the `coming`-hold predicate is
-- gated on a fresh heartbeat, so a quiet/absent scouter reverts the claim query
-- to its pre-feature baseline (claim normally) rather than holding work.
CREATE TABLE model_filters_sync (
    id         BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO model_filters_sync (id, updated_at) VALUES (true, now());
