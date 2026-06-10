# Async Model-Filters Claim Gate (Solution 5)

**Date:** 2026-06-09
**Status:** Planned
**Linear:** COR-431 (parent), COR-432 (this — fusillade), COR-433 (the controller), COR-434 (control-layer onwards)
**Design doc:** Linear — "Architecture Options: Hard Guarantees on Async Workloads" (Solution 5)

> **Update (append-only event log).** This plan originally framed `model_filters`
> as a **current-state table** (one row per model, primary key on `model`, a
> full-replace `set_model_filters`, claim joining the unique row). It has since
> been reworked to an **append-only event log**: `model_filters` records every
> liveness transition, the current state of a model is its **latest event**, and
> retraction is an explicit `absent` **tombstone** event (never a DELETE). This
> supersedes the current-state framing throughout. The benefit is that load
> durations can be **learned from the log** (`coming -> live` gaps) to set future
> ETAs (`model_load_estimate`), and the history is auditable.
>
> **Update (no heartbeat table).** An earlier revision had a separate
> `model_filters_sync` heartbeat table and made the gate fail-open on its
> staleness. That table has been **removed**: the gate now simply trusts the
> latest `model_filters` event per model and **degrades per request** — a
> `coming` model frozen by a dead/quiet controller stops being held once its ETA
> passes (`expected_ready_at > now`) or once the request reaches its own
> deadline, so held work is always eventually released to OpenRouter without any
> heartbeat or freshness TTL. **All `model_filters_sync` / heartbeat /
> `model_filters_ttl_ms` / `hb.fresh` mentions below are superseded** — one
> append-only table, no liveness gate. This also keeps the PR inert to deploy
> ahead of the controller: with no events, the gate is a no-op.

## Overview

This is the fusillade half of **Solution 5** for the "Hard Guarantees on Async Workloads" project. The goal is to **maximise use of our own GPU infrastructure** for async/batch: don't dispatch work to OpenRouter while internal capacity is live or imminent, but never miss a request's TTFT target by waiting too long.

The mechanism: fusillade gains a `model_filters` table describing, per model, whether it is **live**, **coming** (with an ETA), or **absent** on our infrastructure. The daemon's claim cycle consults it and decides — **per request, against an effective deadline `D_eff`** — whether to claim now (dispatch, which control-layer's onwards routes to internal or OpenRouter) or **hold** the request in the queue to wait for imminent internal capacity.

fusillade does **not** route internal-vs-OpenRouter — it dispatches to `request.endpoint` and control-layer's onwards decides (see Cross-Component Contract). fusillade's only new responsibility is the **claim/hold decision**.

## Context (current behaviour)

- **Claim query** — `Storage::claim_requests` (`src/manager/mod.rs:407`) and its Postgres impl (`src/manager/postgres.rs:1068-1147`). It already: joins `batches`, computes `effective_expires_at = COALESCE(b.expires_at, r.created_at + flex_expiry_ms)`, orders by a blended **user-fairness + SLA-urgency** score (the `user_priority` CTE from `$6/$7` arrays + `urgency_weight` `$8`), and claims per-model up to capacity with `FOR UPDATE … SKIP LOCKED`.
- **Per-user fair scheduling** (`docs/plans/2026-03-25-per-user-fair-scheduling.md`) — the daemon keeps `user_requests_in_flight: DashMap<String, AtomicUsize>` (`src/daemon/mod.rs:320-347`), snapshots it to a `HashMap` each cycle (`:771-782`), and passes it as `user_active_counts`. **We reuse this snapshot→arrays→CTE pattern.**
- **Model escalation** (`src/daemon/mod.rs:806-844`) — already swaps a claimed request's `model` to an `escalation_model` when `batch_expires_at - now < escalation_threshold_seconds`. This is the existing near-deadline "use the premium/OR path" hook.
- **Dispatch** (`src/http.rs:261`) — `format!("{}{}", request.endpoint, request.path)`; no routing logic in fusillade.
- **Deadline at claim time** — `Claimed.batch_expires_at` (`src/request/types.rs:148`), from the query's `effective_expires_at`.
- **Migrations** — `sqlx::migrate!("./migrations")` via `migrator()` in `src/lib.rs`; **not auto-run** — the embedding application runs them. Naming: `YYYYMMDDHHmmss_name.{up,down}.sql`.

## Cross-component contract (for context — not all in this repo)

```
   the controller (COR-433, embeds this crate, NO daemon)
     • polls dynamo-frontend /v1/models for liveness
     • computes/retracts expected_ready_at for models it plans to deploy
     • writes model_filters events (this crate's fns) on state change
     • writes active/inactive to control-layer (for onwards)
                         │ writes
                         ▼
   fusillade DB:  model_filters (NEW)  ◄── daemon reads in claim cycle (THIS PLAN)
                         │
   control-layer-fusillade daemon: per-request claim/hold using model_filters + D_eff
                         │ claims → dispatch to request.endpoint
                         ▼
   control-layer onwards (COR-434): skip not-live component, default OpenRouter fallback
                         ▼
            internal backend   |   OpenRouter
```

`model_filters` is owned by this crate (migrations included). control-layer-fusillade runs migrations; **the controller embeds the crate with migrations disabled** and a schema-compatible version pin.

## Design

### 1. `model_filters` — append-only event log

```sql
CREATE TABLE model_filters (
    id                BIGSERIAL PRIMARY KEY,                                -- event id
    model             TEXT NOT NULL,                                        -- NOT unique
    state             TEXT NOT NULL CHECK (state IN ('live','coming','absent')),
    expected_ready_at TIMESTAMPTZ,                                          -- set when state = 'coming'
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_model_filters_model_created_at
    ON model_filters (model, created_at DESC);
```

Each row is one **liveness transition** appended by the controller. There is **no
`model` primary key / uniqueness** — many events per model accumulate over time.

- **Current state = latest event per model** (max `created_at`, tie-broken by
  `id`). The claim query reads it via a `LEFT JOIN LATERAL (… ORDER BY
  created_at DESC LIMIT 1)`, served by the `(model, created_at DESC)` index.
- **Absent** = the model's **latest event is `state='absent'`** (an explicit
  **tombstone**) **OR** the model has **no events at all**. Both are treated
  identically by the gate: claim now (route to OpenRouter).
- **Append-on-change is the caller's (the controller's) responsibility.** The write
  functions always insert; the controller must append only when a model's state
  actually changes, so the log stays a *transition* log rather than a *poll*
  log. Retraction = append an `absent` event; there is never a DELETE in the
  hot path (only the retention purge removes old rows — see §6).

No separate liveness/heartbeat table: the gate trusts the latest event per model
and degrades per request (see the per-request decision and the top "no heartbeat
table" note).

### 2. Per-request claim decision

For a pending request for model X with effective deadline `D_eff` (below), let `serve` = `serve_margin_ms` (estimated time to actually serve once live):

`model_filters[X]` below means **the latest event for model X** (NULL if no
events). `absent` covers both "latest event is an `absent` tombstone" and "no
events at all".

| `model_filters[X]` (latest event) | Decision |
|---|---|
| `live` | **claim** (→ onwards → internal) |
| `coming(T)` and `T > now` and `T + serve ≤ D_eff` and `now + serve < D_eff` | **hold** (wait for internal) |
| `coming(T)` and `T + serve > D_eff` | **claim** (can't wait → onwards → OR) |
| `coming(T)` and `T ≤ now` (past/stuck ETA) | **claim** (self-heal — controller likely gone → OR) |
| absent (tombstone latest **or** no events) | **claim** (not coming → onwards → OR) |
| any, and `now + serve ≥ D_eff` | **claim** (deadline release — onwards/escalation → OR) |

There is **no heartbeat/freshness gate** — degradation when the controller is gone is per-request (the past-ETA and deadline-release rows), so held work is always eventually released and nothing is held forever.

The hold predicate keys off `mf.state = 'coming'`, so a latest event of `live`,
`absent`, or NULL never holds — exactly the append-only equivalent of the old
current-state logic.

So a row is **excluded from the claim** (held) iff: `state='coming'` **and** `expected_ready_at > now` **and** `expected_ready_at + serve ≤ D_eff` **and** `now + serve < D_eff`. Everything else is claimable. Held requests stay `pending`, so they keep counting as demand for the controller.

### 3. Effective deadline `D_eff` — TTFT target × fair-share (closes brief ①)

Instead of the raw `effective_expires_at`, hold/release uses an effective deadline that is **tight for idle users and relaxes for sustained consumers**:

```
D_eff = now + min_async_ttft + (effective_expires_at - now - min_async_ttft) * g(recent_claims[user])
        clamped to [now + min_async_ttft, effective_expires_at]
```

- `min_async_ttft` — new config, **start 1 min**. The floor TTFT target for a user who deserves full resource. Internal config, **never a published flex/batch SLA**.
- `recent_claims[user]` — a **decaying per-user recent-CLAIM score** (see §5). Keys off resource *consumed*, not backlog or lifetime, so a 50k batch and a few sequential requests get the same fast initial burst, then relax identically.
- `g(score) ∈ [0,1]`, increasing, `g(0)=0` — e.g. `score / (score + k)` (saturating) or `LEAST(score / S_max, 1)`. Exact curve is a tuning knob (mirror how `urgency_weight` was introduced as a dial).
- `effective_expires_at` is the hard ceiling; if it is < `now + min_async_ttft`, it wins.

Idle user → `D_eff ≈ now + min_async_ttft` → almost nothing qualifies to hold → served internally if X already live, else OR (responsive). Sustained consumer → `D_eff` near real expiry → work held for the cheap internal path their own volume is provisioning.

### 4. Relationship to existing `model_escalations`

The **deadline-release** clause (`now + serve ≥ D_eff`) is the new, `D_eff`-aware analogue of the existing escalation trigger. Under Solution 5, a released request dispatched to the composite is routed to OpenRouter by onwards (internal component excluded), so the `model`-swap in `model_escalations` (`:806-844`) becomes **redundant for the OR path** — but harmless. Decision for implementation: either (a) leave `model_escalations` as-is and rely on onwards routing, or (b) retire it once onwards' default-OR fallback (COR-434) is in place. Recommend (a) for the first cut (no behavioural removal), revisit in COR-434.

### 5. Load-time learning from the log (`model_load_estimate`)

Because the log retains every transition, we can **learn how long a model takes
to load** internally and feed it back as future ETAs:

- For each model, walk its events in time order and pair every `coming` event
  with the **next `live`** event. The gap `live.created_at − coming.created_at`
  is one observed load duration (we use the events' own timestamps — when
  the controller saw the transition — not `expected_ready_at`, so the estimate
  reflects *actual* loads).
- `model_load_estimate(model) -> Option<Duration>` returns an **EWMA** over
  those samples, newest-weighted (alpha 0.5), or `None` if no `coming → live`
  transition has been observed. The controller calls this to set the next
  `expected_ready_at` it publishes.
- The SQL uses `LEAD()` window functions over the model's events; the Rust side
  folds the EWMA so the curve stays one tunable place.

### 6. Retention (purge integration)

The append-only log must be bounded. The existing daemon purge task
(`purge_interval_ms` / `purge_batch_size` / `purge_throttle_ms`) gains a second
drain loop calling `purge_model_filter_events(batch_size, keep_per_model,
retention_secs)`:

- A row is eligible for deletion iff it is **both** beyond the most-recent
  `keep_per_model` events for its model (ranked `created_at DESC, id DESC`)
  **and** older than `retention_secs`.
- `keep_per_model` is clamped to `>= 1`, so the **latest event per model is
  never purged** — the claim gate can never lose a model's current state. The
  extra history window feeds `model_load_estimate`.
- New `DaemonConfig` knobs: `model_filters_keep_per_model` (default 50) and
  `model_filters_retention_ms` (default 7 days), both `#[serde(default)]`.

## Implementation steps

### Step 1 — Migration: `model_filters` event log + heartbeat
- `migrations/<ts>_add_model_filters.{up,down}.sql` — append-only event-log
  table (`id BIGSERIAL` PK, non-unique `model`, `state IN
  ('live','coming','absent')`, `created_at`), the `(model, created_at DESC)`
  index, and the `model_filters_sync` heartbeat singleton. `down` drops both.

### Step 2 — Storage trait + Postgres impl (`src/manager/mod.rs`, `src/manager/postgres.rs`)
- `ModelFilterState` gains an `Absent` variant (the tombstone). `ModelFilter {
  model, state: ModelFilterState, expected_ready_at: Option<DateTime<Utc>> }`
  is an **event**, not a unique row.
- Append-only write API (replaces the old `set_model_filters` /
  `upsert_model_filter` / `delete_model_filter`):
  - `append_model_filter_event(&self, entry: &ModelFilter) -> Result<()>` —
    insert one event + bump the heartbeat.
  - `append_model_filter_events(&self, entries: &[ModelFilter]) -> Result<()>`
    — insert a batch of events (one row each, caller order preserved via
    `WITH ORDINALITY`) + bump the heartbeat once.
  - **No upsert, no delete.** Retraction = append an `Absent` event. Appending
    only on *change* is the controller's responsibility.
- `list_model_filters` — returns the **latest event per model**
  (`DISTINCT ON (model) … ORDER BY created_at DESC, id DESC`), excluding models
  whose latest event is an `absent` tombstone (observability/tests).
- `model_load_estimate(&self, model) -> Result<Option<Duration>>` — see §5.
- heartbeat read (observability/tests) — unchanged.
- Extend `claim_requests` signature with the per-user recent-claim snapshot, mirroring `user_active_counts`:
  ```rust
  async fn claim_requests(
      &self,
      limit: usize,
      daemon_id: DaemonId,
      available_capacity: &HashMap<String, usize>,
      user_active_counts: &HashMap<String, usize>,
      user_recent_claims: &HashMap<String, f64>,   // NEW
  ) -> Result<Vec<Request<Claimed>>>;
  ```
  (`min_async_ttft_ms`, `serve_margin_ms`, `model_filters_ttl_ms` come from `self.config` like `urgency_weight`/`flex_expiry_ms` already do.)

### Step 3 — Claim SQL (`src/manager/postgres.rs`)
- Pass new bind params: `$10` recent-claim user_ids `TEXT[]`, `$11` scores `DOUBLE PRECISION[]`, `$12` `min_async_ttft` seconds, `$13` `serve_margin` seconds, `$14` `model_filters_ttl` seconds.
- In the per-model `LATERAL`, replace the unique-row join with a **latest-event
  lookup**: `LEFT JOIN LATERAL (SELECT state, expected_ready_at FROM
  model_filters WHERE model = m.model ORDER BY created_at DESC LIMIT 1) mf ON
  true` (served by `idx_model_filters_model_created_at`). NULL (no events) and a
  latest `absent` event both behave as absent because the hold predicate
  requires `mf.state = 'coming'`. Add the heartbeat scalar subquery as before.
- Add a recent-claim CTE (like `user_priority`) and compute `D_eff` inline from `effective_expires_at`, `$12`, and `g(score)`.
- Add the **hold** exclusion to the `WHERE` (a row is skipped iff held):
  ```sql
  AND NOT (
      (hb.updated_at > $3 - make_interval(secs => $14))       -- heartbeat fresh
      AND mf.state = 'coming'
      AND mf.expected_ready_at + make_interval(secs => $13) <= <D_eff>
      AND $3 + make_interval(secs => $13) < <D_eff>           -- not yet at deadline
  )
  -- fail-OPEN: there is NO extra heartbeat clause. The hold predicate above is
  -- gated on `hb.fresh`, so when the heartbeat is stale nothing is held and the
  -- claim query reverts to baseline (claim normally).
  ```
  where `<D_eff>` is the clamped expression from §3. Keep the existing ORDER BY (fairness + urgency) for *ordering among claimable rows*.
- Regenerate the sqlx cache: `cargo sqlx prepare`.

### Step 4 — `DaemonConfig` (`src/daemon/mod.rs`, defaults `:274-308`)
- `min_async_ttft_ms: u64` (default `60_000`)
- `serve_margin_ms: u64` (default e.g. `0`–`5_000`; start small)
- `model_filters_ttl_ms: u64` (staleness threshold; default e.g. `30_000`, aligned to the controller poll cadence)
- `recent_claims_halflife_ms: u64` (decay time constant; default e.g. `120_000`)
- `recent_claims_curve_k: f64` (the `k`/`S_max` for `g()`; tuning dial, default conservative)
All `#[serde(default)]`, backward-compatible.

### Step 5 — Daemon recent-claim counter (`src/daemon/mod.rs`)
- Add `user_recent_claims: Arc<DashMap<String, (f64 /*score*/, Instant /*last*/)>>` to `Daemon` (`:320-347`).
- **Increment on claim** at the same point `user_requests_in_flight` is incremented (`:947-952`): decay-then-add (`score = score * 0.5^(Δt/halflife) + 1.0`).
- **Snapshot before claim** (next to `:771-782`): read each entry, apply decay-on-read to `now`, drop entries that have decayed below ε (keeps the map bounded), produce `HashMap<String, f64>` → pass as `user_recent_claims`.
- No periodic task needed (decay-on-read).

### Step 6 — Embedding support for the controller (no code change expected, verify)
- Confirm `PostgresRequestManager` / `Storage` is usable without constructing a `Daemon` (it already is — manager and daemon are separate). The controller calls `set_model_filters` / heartbeat only.
- Ensure migrations are **opt-in** (caller invokes `migrator().run()`); the controller must not run them. Document the version-pin requirement against control-layer-fusillade's deployed schema.

## Out of scope (tracked elsewhere)
- **Counters table** — *not built*. Adds nothing to the claim decision (which needs `model_filters` + each request's deadline + current backlog); the controller's demand input stays the existing `get_pending_request_counts_by_model_and_window` query.
- **the controller** liveness poll, ETA computation, `model_filters` writes, control-layer active/inactive — COR-433.
- **control-layer** onwards skip-not-live + default OpenRouter fallback — COR-434.
- **Native `service_tier` ingestion** on chat-completions/open-responses — COR-436 (standalone).

## Tests (`src/manager/postgres.rs`)
- `test_claim_holds_for_coming_model_when_deadline_allows` — append `coming(T)` with `T+serve ≤ D_eff` ⇒ not claimed; append `live` event ⇒ latest is live ⇒ claimed.
- `test_claim_releases_coming_model_near_deadline` — `coming(T)` but request near `D_eff` ⇒ claimed (→ OR path).
- `test_absent_model_claimed_immediately` — no events ⇒ claimed (no hold).
- `test_tombstone_absent_hides_model` — append `live` then `absent` tombstone ⇒ `list_model_filters` excludes it; both events remain in the log.
- `test_tombstone_absent_model_is_claimed` — append `coming` then `absent` ⇒ latest is tombstone ⇒ claimed (not held).
- `test_coming_past_eta_is_released_no_heartbeat` — a `coming` model with a past ETA is released (self-heal), while an in-window `coming` model is still held — no heartbeat involved.
- `test_d_eff_idle_vs_busy_user` — idle user (no recent claims) gets tight `D_eff` (claimed/released fast); high recent-claim user holds for `coming` model.
- `test_recent_claims_decay_relaxes_then_tightens` — score decays over time so an idle-again user regains the tight target.
- `test_append_model_filter_events` — append-only: later events supersede earlier per model (latest wins), every event retained; single-event helper.
- `test_model_load_estimate` — EWMA of observed `coming → live` gaps; `None` when no transition seen.
- `test_purge_model_filter_events_retention` — old events purged while the latest event per model (and the retention window) is always kept; `keep_per_model` clamped to ≥ 1.
- All existing claim tests pass with an empty `model_filters` log + empty `user_recent_claims` (behaviour unchanged when the controller isn't writing).

## Version strategy
`Storage::claim_requests` signature changes (new parameter) and `RequestData`/config evolve → `feat!:` **major bump**, consistent with the per-user fair-scheduling rollout. Bundle Steps 1–5 in one PR.

## Verification
1. `just db-start && just db-setup`
2. `just test` — existing tests green with empty `model_filters`/`user_recent_claims`
3. `just test` new tests above
4. `just lint`
5. `cargo sqlx prepare --check`

## Files to modify
| File | Change |
|------|--------|
| `migrations/<ts>_add_model_filters.{up,down}.sql` | append-only `model_filters` event log + `(model, created_at DESC)` index + `model_filters_sync` heartbeat |
| `src/manager/mod.rs` | `claim_requests` signature (+`user_recent_claims`); `ModelFilterState` (+`Absent`) / `ModelFilter` event types; `append_model_filter_event(s)`, `list_model_filters` (latest-per-model), `model_load_estimate` trait fns; `DaemonStorage::purge_model_filter_events` |
| `src/manager/postgres.rs` | claim SQL: latest-event `LEFT JOIN LATERAL`, `D_eff`, hold predicate (per-request self-heal, no heartbeat), new binds; append/list/load-estimate/purge impls; tests |
| `src/daemon/mod.rs` | `DaemonConfig` fields (incl. `model_filters_keep_per_model`, `model_filters_retention_ms`); purge task drains `model_filters` events; `user_recent_claims` DashMap |
| `src/daemon/transitions.rs` | `MockDaemonStorage::purge_model_filter_events` |
| `.sqlx/query-*.json` | regenerated via `cargo sqlx prepare` |
