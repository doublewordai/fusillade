# Async Model-Filters Claim Gate (Solution 5)

**Date:** 2026-06-09
**Status:** Planned
**Linear:** COR-431 (parent), COR-432 (this — fusillade), COR-433 (scouter), COR-434 (control-layer onwards)
**Design doc:** Linear — "Architecture Options: Hard Guarantees on Async Workloads" (Solution 5)

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
   scouter (COR-433, embeds this crate, NO daemon)
     • polls dynamo-frontend /v1/models for liveness
     • computes/retracts expected_ready_at for models it plans to deploy
     • writes model_filters (this crate's fns) + a sync heartbeat
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

`model_filters` is owned by this crate (migrations included). control-layer-fusillade runs migrations; **scouter embeds the crate with migrations disabled** and a schema-compatible version pin.

## Design

### 1. `model_filters` table

```sql
CREATE TABLE model_filters (
    model             TEXT PRIMARY KEY,
    state             TEXT NOT NULL CHECK (state IN ('live', 'coming')),
    expected_ready_at TIMESTAMPTZ,            -- set when state = 'coming'
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Absent row = scouter is not deploying this model.
```

Plus a singleton **sync heartbeat** so the daemon can tell "scouter is alive, absence is meaningful" from "scouter is dead, the table is frozen":

```sql
CREATE TABLE model_filters_sync ( id BOOLEAN PRIMARY KEY DEFAULT true CHECK (id), updated_at TIMESTAMPTZ NOT NULL );
INSERT INTO model_filters_sync (id, updated_at) VALUES (true, now());
```

scouter bumps `model_filters_sync.updated_at` on every sync.

### 2. Per-request claim decision

For a pending request for model X with effective deadline `D_eff` (below), let `serve` = `serve_margin_ms` (estimated time to actually serve once live):

| `model_filters[X]` (with fresh heartbeat) | Decision |
|---|---|
| `live` | **claim** (→ onwards → internal) |
| `coming(T)` and `T + serve ≤ D_eff` and `now + serve < D_eff` | **hold** (wait for internal) |
| `coming(T)` and `T + serve > D_eff` | **claim** (can't wait → onwards → OR) |
| absent | **claim** (not coming → onwards → OR) |
| any, and `now + serve ≥ D_eff` | **claim** (deadline release — onwards/escalation → OR) |
| heartbeat **stale** | **fail closed**: only the deadline-release clause claims; everything else holds |

So a row is **excluded from the claim** (held) iff: heartbeat fresh **and** `state='coming'` **and** `expected_ready_at + serve ≤ D_eff` **and** `now + serve < D_eff`. Everything else is claimable. Held requests stay `pending`, so they keep counting as demand for scouter.

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

## Implementation steps

### Step 1 — Migration: `model_filters` + heartbeat
- `migrations/<ts>_add_model_filters.{up,down}.sql` — tables above. `down` drops both.

### Step 2 — Storage trait + Postgres impl (`src/manager/mod.rs`, `src/manager/postgres.rs`)
- Add typed functions (used by the daemon's claim query and by scouter):
  - `set_model_filters(&self, entries: &[ModelFilter]) -> Result<()>` — transactional **replace** (upsert present, delete absent) + bump `model_filters_sync.updated_at`. `ModelFilter { model, state: ModelFilterState, expected_ready_at: Option<DateTime<Utc>> }`.
  - `upsert_model_filter` / `delete_model_filter` for incremental updates (scouter may prefer deltas).
  - `list_model_filters` / heartbeat read (observability/tests).
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
- In the per-model `LATERAL`, `LEFT JOIN model_filters mf ON mf.model = m.model`, and add a `CROSS JOIN model_filters_sync hb` (or scalar subquery) for the heartbeat.
- Add a recent-claim CTE (like `user_priority`) and compute `D_eff` inline from `effective_expires_at`, `$12`, and `g(score)`.
- Add the **hold** exclusion to the `WHERE` (a row is skipped iff held):
  ```sql
  AND NOT (
      (hb.updated_at > $3 - make_interval(secs => $14))       -- heartbeat fresh
      AND mf.state = 'coming'
      AND mf.expected_ready_at + make_interval(secs => $13) <= <D_eff>
      AND $3 + make_interval(secs => $13) < <D_eff>           -- not yet at deadline
  )
  -- fail-closed: when heartbeat stale, also require deadline-release to claim:
  AND ( hb.updated_at > $3 - make_interval(secs => $14)
        OR $3 + make_interval(secs => $13) >= <D_eff> )
  ```
  where `<D_eff>` is the clamped expression from §3. Keep the existing ORDER BY (fairness + urgency) for *ordering among claimable rows*.
- Regenerate the sqlx cache: `cargo sqlx prepare`.

### Step 4 — `DaemonConfig` (`src/daemon/mod.rs`, defaults `:274-308`)
- `min_async_ttft_ms: u64` (default `60_000`)
- `serve_margin_ms: u64` (default e.g. `0`–`5_000`; start small)
- `model_filters_ttl_ms: u64` (staleness threshold; default e.g. `30_000`, aligned to scouter poll cadence)
- `recent_claims_halflife_ms: u64` (decay time constant; default e.g. `120_000`)
- `recent_claims_curve_k: f64` (the `k`/`S_max` for `g()`; tuning dial, default conservative)
All `#[serde(default)]`, backward-compatible.

### Step 5 — Daemon recent-claim counter (`src/daemon/mod.rs`)
- Add `user_recent_claims: Arc<DashMap<String, (f64 /*score*/, Instant /*last*/)>>` to `Daemon` (`:320-347`).
- **Increment on claim** at the same point `user_requests_in_flight` is incremented (`:947-952`): decay-then-add (`score = score * 0.5^(Δt/halflife) + 1.0`).
- **Snapshot before claim** (next to `:771-782`): read each entry, apply decay-on-read to `now`, drop entries that have decayed below ε (keeps the map bounded), produce `HashMap<String, f64>` → pass as `user_recent_claims`.
- No periodic task needed (decay-on-read).

### Step 6 — Embedding support for scouter (no code change expected, verify)
- Confirm `PostgresRequestManager` / `Storage` is usable without constructing a `Daemon` (it already is — manager and daemon are separate). scouter calls `set_model_filters` / heartbeat only.
- Ensure migrations are **opt-in** (caller invokes `migrator().run()`); scouter must not run them. Document the version-pin requirement against control-layer-fusillade's deployed schema.

## Out of scope (tracked elsewhere)
- **Counters table** — *not built*. Adds nothing to the claim decision (which needs `model_filters` + each request's deadline + current backlog); scouter's demand input stays the existing `get_pending_request_counts_by_model_and_window` query.
- **scouter** liveness poll, ETA computation, `model_filters` writes, control-layer active/inactive — COR-433.
- **control-layer** onwards skip-not-live + default OpenRouter fallback — COR-434.
- **Native `service_tier` ingestion** on chat-completions/open-responses — COR-436 (standalone).

## Tests (`src/manager/postgres.rs`)
- `test_claim_holds_for_coming_model_when_deadline_allows` — `coming(T)` with `T+serve ≤ D_eff` ⇒ not claimed; flip model to `live` ⇒ claimed.
- `test_claim_releases_coming_model_near_deadline` — same `coming(T)` but request near `D_eff` ⇒ claimed (→ OR path).
- `test_absent_model_claimed_immediately` — absent + fresh heartbeat ⇒ claimed (no hold).
- `test_stale_heartbeat_fails_closed` — stale heartbeat ⇒ only near-`D_eff` rows claimed; others held.
- `test_d_eff_idle_vs_busy_user` — idle user (no recent claims) gets tight `D_eff` (claimed/released fast); high recent-claim user holds for `coming` model.
- `test_recent_claims_decay` — score decays over time so an idle-again user regains the tight target.
- All existing claim tests pass with empty `model_filters` + empty `user_recent_claims` (i.e. behaviour is unchanged when scouter isn't writing).

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
| `migrations/<ts>_add_model_filters.{up,down}.sql` | `model_filters` + `model_filters_sync` tables |
| `src/manager/mod.rs` | `claim_requests` signature (+`user_recent_claims`); new `set_model_filters`/`upsert`/`delete`/heartbeat trait fns; `ModelFilter`/`ModelFilterState` types |
| `src/manager/postgres.rs` | claim SQL: `model_filters` join, `D_eff`, hold/fail-closed predicate, new binds; model-filter write/read impls; tests |
| `src/daemon/mod.rs` | `DaemonConfig` fields; `user_recent_claims` DashMap + increment/snapshot/decay; pass new arg/config |
| `.sqlx/query-*.json` | regenerated via `cargo sqlx prepare` |
