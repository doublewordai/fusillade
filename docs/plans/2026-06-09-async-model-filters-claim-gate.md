# Async Model-Filters Claim Gate (Solution 5)

**Date:** 2026-06-09 (design revised 2026-06-11)
**Status:** Planned (claim-gate model revised — see below)
**Linear:** COR-431 (parent), COR-432 (this — fusillade), COR-433 (the controller), COR-434 (control-layer onwards)
**Design doc:** Linear — "Architecture Options: Hard Guarantees on Async Workloads" (Solution 5)

> **Update (2026-06-11) — liveness-gated leaky bucket supersedes the D_eff/ETA hold gate.**
> The original plan gated claims on an **effective deadline `D_eff`** (a per-user
> recent-claims relaxation) and on a model's **`coming(ETA)`** state, with the gate
> reading `expected_ready_at` and a learned `model_load_estimate` (EWMA). The team
> has since chosen a simpler, more robust model:
>
> - The gate needs only a **binary "is the model live on our infra?"**. The ETA
>   is no longer used to decide claims.
> - **Live, OR no `model_filters` events at all → claim at full model capacity.**
>   A model with no events is *unmanaged* by the controller — there is no internal
>   capacity to wait for, so it claims straight through to OpenRouter. Crucially
>   this makes the gate a **no-op until the controller (COR-433) starts writing
>   events**, so the change is safe to ship standalone (an empty `model_filters`
>   table does not throttle anything). *(Revised 2026-06-15 — the earlier "no
>   events ⇒ not-live" would have throttled ALL async traffic in production before
>   the controller existed.)*
> - **Explicit not-live event (`coming`/`absent`) → a per-(user, completion-window)
>   leaky bucket** that trickles
>   requests out (→ OpenRouter) at a window-scaled rate, **ramping to full
>   capacity** as the request nears its completion window. This is **"fail
>   partially open"** — neither hold-everything (fail closed) nor dump-everything
>   (fail open).
>
> **Retired from the claim decision:** `D_eff`, the recent-claims relaxation and
> its config (`min_async_ttft_ms`, `serve_margin_ms`, `recent_claims_halflife_ms`,
> `recent_claims_curve_k`), `expected_ready_at`/ETA in the gate, and
> `model_load_estimate`/the EWMA. **Retained:** the `model_filters` append-only log
> (gate reads latest event = `live` vs not), the rank-then-pull claim + its
> `idx_requests_pending_claim_pull` index, and the per-user fair-scheduling blend
> (used only to order *claimable* rows for live models).
>
> Earlier "append-only event log" and "no heartbeat table" updates still hold and
> are folded into the design below.

## Overview

This is the fusillade half of **Solution 5** for "Hard Guarantees on Async Workloads". The goal is to **maximise use of our own GPU infrastructure** for async work: don't pay OpenRouter while internal capacity is (or is about to be) live, but never miss a request's completion window.

fusillade gains a `model_filters` table describing, per model, whether it is **live** on our infrastructure. The daemon's claim cycle reads it and decides, per request:

- **model live** → claim at full model capacity (dispatch → onwards → internal).
- **model not-live** → **per-(user, completion-window) leaky bucket**: trickle ≤ 1 request per user per leak-interval to OpenRouter, **ramping to full capacity** once the request is within `ramp(window)` of its completion-window deadline.

fusillade does **not** route internal-vs-OpenRouter — it dispatches to `request.endpoint` and control-layer's onwards decides (see Cross-Component Contract). fusillade's only new responsibility is the **claim/throttle decision**.

**Important:** fusillade has no notion of "batch" vs "flex/async" — everything is requests with a **completion window** (`effective_expires_at = COALESCE(b.expires_at, r.created_at + flex_expiry_ms)`). The leak rate and the ramp both derive from that window, so a 24 h "batch" naturally leaks ~negligibly slowly (≈ waits for live) while a 1 h request leaks faster — one uniform mechanism.

## Context (current behaviour)

- **Claim query** — `Storage::claim_requests` (`src/manager/mod.rs`) and its Postgres impl (`src/manager/postgres.rs`). Already computes `effective_expires_at`, ranks **batches** by a fair-scheduling + urgency blend, and pulls per-model up to capacity with `FOR UPDATE … SKIP LOCKED` (the **rank-then-pull** structure + `idx_requests_pending_claim_pull (model, batch_id, created_at)`, already shipped).
- **Per-user fair scheduling** — the daemon keeps `user_requests_in_flight` and passes it as `user_active_counts`; used to order claimable rows. **Reused for live-model ordering.**
- **Model escalation** (`src/daemon/mod.rs`) — swaps a claimed request's `model` to an `escalation_model` near deadline. Under Solution 5 the ramp-to-full-capacity + onwards default-OR fallback subsumes this; leave as-is for the first cut (harmless), revisit in COR-434.
- **Dispatch** (`src/http.rs`) — `format!("{}{}", request.endpoint, request.path)`; no routing in fusillade.
- **Migrations** — `sqlx::migrate!("./migrations")`; **not auto-run** — the embedding app runs them. Naming `YYYYMMDDHHmmss_name.{up,down}.sql`.

## Cross-component contract

```
   the controller (COR-433, embeds this crate, NO daemon)
     • polls dynamo-frontend /v1/models for liveness
     • writes model_filters events (live / not-live) on state change
     • reacts to new demand fast (LISTEN/NOTIFY) so not-live → live quickly
     • writes active/inactive to control-layer (for onwards)
                         │ writes
                         ▼
   fusillade DB:  model_filters  ◄── daemon reads latest-event liveness in claim cycle (THIS PLAN)
                         │
   control-layer-fusillade daemon: per-request claim (live=full / not-live=leaky bucket)
                         │ claims → dispatch to request.endpoint
                         ▼
   control-layer onwards (COR-434): skip not-live component, default OpenRouter fallback
                         ▼
            internal backend   |   OpenRouter
```

`model_filters` is owned by this crate (migrations included). The controller embeds the crate with **migrations disabled** and a schema-compatible version pin. Making the controller **react to new demand quickly** (NOTIFY rather than only a 30 s poll) shrinks the window where a just-arrived model is still "not-live", so the bucket front-loads less to OR.

## Design

### 1. `model_filters` — append-only event log (unchanged schema)

```sql
CREATE TABLE model_filters (
    id                BIGSERIAL PRIMARY KEY,
    model             TEXT NOT NULL,                                         -- NOT unique
    state             TEXT NOT NULL CHECK (state IN ('live','coming','absent')),
    expected_ready_at TIMESTAMPTZ,                                           -- no longer read by the gate
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_model_filters_model_created_at ON model_filters (model, created_at DESC);
```

- **Current state = latest event per model** (`ORDER BY created_at DESC, id DESC LIMIT 1`), via the index.
- The gate reads **`state = 'live'` OR no events ⇒ claim at full capacity**; an **explicit `coming`/`absent` event ⇒ not-live** (leaky-bucket / ramp path). `expected_ready_at` and the `coming` vs `absent` distinction are **no longer used by the claim gate** (kept for the controller's own use / observability; could be dropped in a later migration). The "no events ⇒ claim full" rule keeps the gate a no-op until the controller writes events.
- Append-on-change remains the controller's responsibility; retraction = append `absent`; no DELETE in the hot path (only the retention purge — §6).

### 2. Per-request claim decision

For a pending request with completion-window deadline `expires = effective_expires_at` and window duration `W = expires − created_at`:

| Model live? | Within `ramp(W)` of `expires`? | Decision |
|---|---|---|
| **live** | — | **Claim** at full model capacity (→ internal) |
| not-live | **yes** | **Claim** at full model capacity (→ OR) — *deadline ramp* |
| not-live | no | **Leaky bucket**: claim iff the request's `(user, window-class)` bucket has a token; else **hold** |

Held requests stay `pending`, so they keep counting as demand for the controller. There is no fail-open/closed binary — not-live work is **partially** released at the leaky-bucket rate, with the ramp guaranteeing completion.

### 3. Ramp function

`ramp(W)` = how long before the deadline we abandon the trickle and drain at full capacity. Anchored on the team's two points (1 h → final 10 min, 24 h → final 60 min), which define a sub-linear power curve:

```
ramp_minutes = W_minutes ^ ramp_exponent          (default ramp_exponent = 0.56)
```

- 1 h (60 m) → 60^0.56 ≈ **10 min**; 24 h (1440 m) → 1440^0.56 ≈ **59 min**.
- Sub-linear: longer windows get a proportionally *smaller* ramp.
- **Degrades gracefully at the short end**: `ramp(W) → W` as `W → ~1 min`, so a sub-minute window is "always ramping" (full capacity) with no special-case floor.
- `ramp_exponent` is a config dial (default 0.56 = the two anchors).
- The `W^exponent` math runs on the **bounded candidate set** the rank-then-pull produces (~capacity rows), never in the per-row scan.

### 4. Leaky bucket (per user, per completion-window)

- **Key:** `(user, window-class)`, where `window-class` is the discrete completion window. Small, finite key space → daemon and SQL agree on it.
- **Window source (no `flex_expiry_ms`, no service-tier semantics in the gate):** `W` derives only from a completion window. **Batch rows** use the batch's real `completion_window`/`expires_at`. **Batchless rows** have no stored window, so `W` is mapped from `requests.service_tier`: `'flex' → 1 h`, NULL/other → 24 h (the only two that reach the claim query — realtime tiers `auto`/`default`/`priority` are inserted `processing`, never `pending`, so the daemon never claims them). This replaces the retired `flex_expiry_ms` scalar; the map lives in `DaemonConfig` (`flex_completion_window_ms` = 1 h, `default_completion_window_ms` = 24 h) and is trivially widened to a full tier map later. fusillade never interprets a tier as a latency/cost class — only as a window lookup.
- **Capacity 1, refill 1 token per `leak_interval`:**
  ```
  leak_interval = W / leaks_per_window               (default leaks_per_window = 60)
  ```
  → 1 h window = 1 token/min, 24 h window = 1 token/24 min (≈ 60 leaks per window per user). A long window therefore leaks ≈ negligibly (a 24 h backlog drips ~60 to OR over a day, the rest waits for live or rides the ramp) — this is how "batches effectively wait for live" *emerges* without a type distinction.
- **Per-user global across models:** one bucket per `(user, window-class)` spans all of that user's not-live models in that window class.
- **Daemon owns the buckets and the math** — no leak math in SQL. Each cycle the daemon passes in the **cooldown set**: `(user, window-class)` pairs that have leaked recently and are not yet refilled. After a leak it stamps `next_token_at = now + leak_interval` for that pair. The SQL claims **≤ 1 per `(user, window-class)` not in cooldown**.

### 5. Claim query structure (single round-trip, keeps SKIP LOCKED)

Liveness is per requested model: latest `model_filters` event `= 'live'` (the existing `mf` lateral, now reading `state` only — `expected_ready_at` dropped from the gate). The rank-then-pull bounds candidates cheaply; on that bounded set we draw from two sources and UNION them. Each arm keeps **its own** `FOR UPDATE … SKIP LOCKED` (as the existing batch/batchless arms already do — there was never a single literal locked scan), and the result is one round-trip.

Because batch and batchless rows are already separate arms, this is **four arms**, each tagging a `leaked BOOL` carried through `to_claim` → `RETURNING` (the daemon stamps buckets only for `leaked` rows):

- **Source A (full capacity), batch + batchless** — existing arms, **hold predicate replaced** by the qualifier `is_live(model) OR now + ramp(W) ≥ expires`; `leaked = false`. Ordered by the existing fair-scheduling + urgency blend.
- **Source B (leaky bucket), batch + batchless** — not-live, before-ramp rows, excluding the daemon's cooldown `(user, window-class)` pairs, **≤ 1 per `(user, window-class)`** via `DISTINCT ON (created_by, window_class)`; `leaked = true`. The batch arm rides `ranked_batches` (a batch is one `(user, window-class)`, so the bounded set is reused — no new unbounded scan); the batchless arm keeps the per-row scan (low flex volume). Batch and batchless window-classes are disjoint (`'flex'` sentinel vs real completion windows), so the two Source-B arms never double-count a bucket.

**Compromise — strict ≤1 is per-model-per-cycle, not single-cycle-global.** Source B sits inside the per-model `LATERAL`, so within one cycle a user with the same window-class across *N* not-live models can leak up to *N* (not 1). The daemon's cooldown set converges it to ≤1 from the next cycle (leak intervals are minute-scale, cycles second-scale), so the steady-state error is bounded and immaterial for a trickle-to-OR valve. Lifting Source B out of the per-model lateral to enforce single-cycle-global ≤1 is possible but materially more complex and deferred.

The window/ramp math is applied **after** the index scans (on the bounded candidate set), per the efficiency requirement.

### 6. Retention (purge integration)

Unchanged: the append-only log is bounded by the existing purge task via `purge_model_filter_events(batch_size, keep_per_model, retention_secs)` — a row is deletable iff it is beyond the most-recent `keep_per_model` events for its model **and** older than `retention_secs`; `keep_per_model ≥ 1` so the latest event per model is never purged. (`model_load_estimate` is retired, but retaining a little history remains useful for the controller/observability.)

## Implementation steps

### Step 1 — Liveness in the claim query
- Add a `live_models` CTE (or per-model lateral) over `model_filters`: latest event `state = 'live'`. Replace the existing `D_eff` hold predicate with the **Source A qualifier** `is_live(model) OR now + ramp(W) ≥ expires`.

### Step 2 — Source B (leaky bucket) in the claim query
- Add the per-`(user, window-class)` `LIMIT 1` pull for not-live, before-ramp requests, excluding the passed-in cooldown pairs. UNION with Source A; dedupe; apply capacity + global limit; one `FOR UPDATE OF r SKIP LOCKED`.
- New bind params: cooldown `(user, window-class)` arrays; `ramp_exponent`; `leaks_per_window` (if any window math is done SQL-side) — finalise in the prototype.
- Regenerate the sqlx cache (`cargo sqlx prepare`).

### Step 3 — `Storage::claim_requests` signature
- **Remove** `user_recent_claims`. **Add** the leak **cooldown set** (e.g. parallel `user[]` / `window_class[]` arrays). Keep `user_active_counts` (live-model ordering). The daemon also needs each claimed row's `(user, window-class)` back to stamp buckets — ensure it's in the returned `Request`/`RETURNING`.

### Step 4 — `DaemonConfig` (`src/daemon/mod.rs`)
- **Remove:** `min_async_ttft_ms`, `serve_margin_ms`, `recent_claims_halflife_ms`, `recent_claims_curve_k`, **`flex_expiry_ms`**.
- **Add:** `claim_ramp_exponent: f64` (default `0.56`), `leaks_per_window: f64` (default `60.0`), **`flex_completion_window_ms: u64`** (default `3_600_000` = 1 h), **`default_completion_window_ms: u64`** (default `86_400_000` = 24 h). All `#[serde(default)]`. The two window scalars are the service-tier→`W` map (§4); batch rows ignore them and use their real `completion_window`.

### Step 5 — Daemon leaky-bucket state (`src/daemon/mod.rs`)
- Replace `user_recent_claims` with `leak_buckets: DashMap<(String /*user*/, String /*window-class*/), Instant /*next_token_at*/>`.
- **Before claim:** derive the cooldown set (pairs with `next_token_at > now`); pass to `claim_requests`.
- **After claim:** for each claimed *not-live, before-ramp* (leaked) row, set `next_token_at = now + W / leaks_per_window` for its `(user, window-class)`. (Live / ramped claims do not consume a token.) Decay-on-read / prune stale entries to bound the map.

### Step 6 — Embedding support for the controller (verify only)
- `PostgresRequestManager` usable without a `Daemon` (already true). Migrations opt-in. Version-pin documented.

## Out of scope (tracked elsewhere)
- **Controller NOTIFY / fast reaction**, liveness poll, `model_filters` writes, control-layer active/inactive — COR-433 (the NOTIFY reaction is newly motivated by this design — it shrinks the not-live window).
- **control-layer** onwards skip-not-live + default-OR fallback — COR-434.
- **Native `service_tier` ingestion** — COR-436.

## Tests (`src/manager/postgres.rs`)
- `test_live_model_claims_full_capacity` — live model drains its backlog at capacity (existing rank-then-pull behaviour, no throttle).
- `test_not_live_leaky_bucket_one_per_user_window` — a not-live model with a far deadline claims ≤ 1 per `(user, window-class)` per cycle; pairs in the cooldown set are skipped.
- `test_not_live_within_ramp_claims_full_capacity` — a not-live request inside `ramp(W)` of its deadline is claimed at full capacity (→ OR).
- `test_ramp_scales_with_window` — `ramp(1h) ≈ 10 min`, `ramp(24h) ≈ 60 min`; sub-minute window is always-ramping.
- `test_leak_rate_scales_with_window` — 1 h window refills 1/min, 24 h window 1/24 min (daemon-side bucket test).
- `test_live_overrides_everything` — a live model is claimed at full capacity regardless of cooldown/ramp (keep the existing live-claim invariant test).
- All existing claim tests pass **unchanged** with an empty `model_filters` log: no events ⇒ claim at full capacity (the gate is a no-op). Leaky-bucket tests append an explicit `coming` event to engage Source B.

## Version strategy
`Storage::claim_requests` signature changes and `DaemonConfig` fields change → `feat!:` **major bump**. This *replaces* the D_eff mechanism in the same PR (#281) — net deletion of the ETA/EWMA/recent-claims machinery.

## Verification
1. `just db-start && just db-setup`
2. `just test` — existing + new tests green
3. `just lint`
4. `cargo sqlx prepare --check`

## Files to modify
| File | Change |
|------|--------|
| `src/manager/postgres.rs` | claim SQL: `live_models` CTE; Source A qualifier (`is_live OR within ramp`) replacing the hold predicate; Source B leaky-bucket arm (≤1 per `(user, window-class)` not in cooldown); window/ramp math on the bounded candidate set; tests |
| `src/manager/mod.rs` | `claim_requests` signature (−`user_recent_claims`, +cooldown arrays); return `(user, window-class)` per claimed row |
| `src/daemon/mod.rs` | `DaemonConfig` (−D_eff configs, +`claim_ramp_exponent`, +`leaks_per_window`); `leak_buckets` DashMap; derive cooldown before claim, stamp after |
| `.sqlx/query-*.json` | regenerated via `cargo sqlx prepare` |

> The `model_filters` table, the append-only write API, the retention purge, and the rank-then-pull claim + `idx_requests_pending_claim_pull` index are **already implemented** on the branch and are retained; this revision changes the *gate logic on top of them*.
