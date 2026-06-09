-- Async model-filters claim gate (Solution 5, COR-432).
--
-- `model_filters` describes, per model, whether internal infrastructure is
-- `live` (serving now) or `coming` (will serve, with an ETA). An absent row
-- means scouter is not deploying the model. The daemon's claim cycle consults
-- this table to decide, per request against an effective deadline, whether to
-- claim now (dispatch -> onwards routes internal/OpenRouter) or hold the
-- request in the queue waiting for imminent internal capacity.
CREATE TABLE model_filters (
    model             TEXT PRIMARY KEY,
    state             TEXT NOT NULL CHECK (state IN ('live', 'coming')),
    expected_ready_at TIMESTAMPTZ, -- set when state = 'coming'
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Singleton sync heartbeat so the daemon can distinguish "scouter is alive,
-- absence is meaningful" from "scouter is dead, the table is frozen". scouter
-- bumps `updated_at` on every sync; the daemon fails closed (only the
-- deadline-release clause claims) when the heartbeat is stale.
CREATE TABLE model_filters_sync (
    id         BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO model_filters_sync (id, updated_at) VALUES (true, now());
