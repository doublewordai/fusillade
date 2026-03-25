# Per-User Fair Scheduling

**Date:** 2026-03-25
**Status:** In Progress
**Last revised:** 2026-03-25

## Overview

Fusillade currently schedules requests globally by batch deadline (`expires_at ASC`). A single high-volume user can monopolise model capacity. The "Enabling Background Agents" project requires per-user fair scheduling so throughput is distributed evenly across users (TTFT guarantee).

Linear issues: COR-226 (parent), COR-227, COR-228, COR-229, COR-230.

## Key Insight: No New Column Needed

The `claim_requests` query already joins `batches b` and returns `b.created_by`. The `active_batch_ids` CTE just needs to include `created_by` and the ordering needs to incorporate per-user priority. No migration to add `user_id` to `requests`.

## Architecture: DashMap-Driven Fair Scheduling

Cross-model user fairness requires coordination between the SQL claim query and the daemon's runtime state. Pure SQL approaches (window functions, per-user CTEs) either break under concurrent daemons (`SKIP LOCKED` + `ROW_NUMBER()` interaction) or can't coordinate across models within a single query.

**Chosen approach:** The daemon maintains a `DashMap<String, AtomicUsize>` tracking per-user in-flight request counts. Before each claim cycle, this is snapshotted into a `HashMap` and passed to the SQL query as two arrays (user IDs, active counts). The query uses these to order batches by `COALESCE(active_count, 0) ASC` — users with fewer in-flight requests get their batches visited first within each model's LATERAL.

```
Daemon (Rust)                         PostgreSQL
─────────────                         ──────────
user_requests_in_flight: DashMap
        │
        ├─ snapshot to HashMap ──────► $6::TEXT[] (user_ids)
        │                              $7::BIGINT[] (active_counts)
        │                                      │
        │                              user_priority CTE ──► LEFT JOIN on active_batch_ids
        │                                      │
        │                              ORDER BY active_count ASC, expires_at ASC
        │                                      │
        │                              LATERAL (FOR UPDATE SKIP LOCKED)
        │                                      │
        ◄── claimed requests ─────────── RETURNING + UPDATE
        │
        ├─ increment DashMap (per user, on spawn)
        └─ decrement DashMap (per user, on completion via scopeguard)
```

**Why this approach over alternatives:**
- **Pure SQL interleaving (ROW_NUMBER PARTITION BY created_by):** Window functions are evaluated before `SKIP LOCKED`, so concurrent daemons compute identical rankings. One daemon gets fewer rows than capacity.
- **Per-model SQL queries (one claim per model):** N database round trips instead of 1. Per-model limits can't be expressed as LATERAL LIMITs. Loses atomic multi-model claiming.
- **DB-computed user counts (COUNT in-flight per user):** Full scan of in-flight requests on every claim cycle. DashMap is O(1) with eventual cross-daemon convergence.

**Tradeoffs:**
- Fairness is per-daemon, not global across the cluster (each daemon has its own DashMap).
- Cold start: first few cycles have no fairness signal (empty DashMap). Converges quickly.
- Users whose models appear earlier in the randomised model loop get a slight head start per cycle, but this balances across cycles.

---

## Implementation Order

### Step 1: COR-227 — Ensure `created_by` is reliable ✅

**Goal:** Make `batches.created_by` non-nullable so the scheduler always has a user key.

**Changes (implemented):**
- `migrations/20260325152006_make_created_by_not_null` — `SET DEFAULT ''`, `SET NOT NULL`, replace partial index with full index. No backfill needed (confirmed no NULLs in production).
- `src/batch/mod.rs` — `Batch.created_by: Option<String>` → `String`. `BatchInput.created_by` stays `Option<String>` for API backwards compat.
- `src/manager/postgres.rs` — `create_batch` INSERT uses `COALESCE($5, '')` so `None` inputs become `""`. Removed `COALESCE` wrapper in `claim_requests` (column is now NOT NULL). Virtual file functions take `&str` instead of `&Option<String>`.

**Breaking change:** `Batch.created_by: Option<String>` → `String`.

---

### Step 2: COR-228 + COR-229 — Per-user fair scheduling with DashMap tracking ✅

**Goal:** Distribute claim capacity fairly across users using daemon-side in-flight tracking.

COR-228 and COR-229 were merged because the DashMap (originally COR-229 for tracking) is the fairness mechanism itself (COR-228), not just observability.

**Changes (implemented):**

#### Storage trait (`src/manager/mod.rs`)
- Added `user_active_counts: &HashMap<String, usize>` parameter to `Storage::claim_requests`.

#### Claim query (`src/manager/postgres.rs`)
- Added `user_priority` CTE from `unnest($6::TEXT[], $7::BIGINT[])`.
- `active_batch_ids` joined to `user_priority` via `LEFT JOIN ON created_by = user_id`.
- Batch iteration ordered by `COALESCE(up.active_count, 0) ASC, expires_at ASC` — users with fewer in-flight requests get batches visited first.
- Preserves: atomic multi-model claim, nested LATERAL with `FOR UPDATE SKIP LOCKED`, per-model capacity limits, batch-level early termination.

#### Request data (`src/request/types.rs`)
- Added `created_by: String` to `RequestData`. Populated from `batch_created_by` in the RETURNING clause. The daemon reads this directly — no need to route through batch metadata (which is user-visible).

#### Daemon (`src/daemon/mod.rs`)
- Added `user_requests_in_flight: Arc<DashMap<String, AtomicUsize>>` to `Daemon` struct.
- Before each claim cycle: snapshot DashMap to `HashMap`, pass as `user_active_counts`.
- On spawn: increment `user_requests_in_flight[user_id]`.
- On completion (scopeguard): decrement `user_requests_in_flight[user_id]`.

**Breaking changes:** `Storage::claim_requests` signature change (added parameter), `RequestData` new field.

**Tests:**
- `test_per_user_fair_scheduling` — Cold start (empty DashMap, deadline fallback) and populated (user-a has 5 in-flight, B+C prioritised).
- `test_per_user_deadline_ordering_preserved` — Within a single user, most urgent batch claimed first.
- All existing tests pass with empty `user_active_counts`.

---

### ~~Step 3: COR-229 — Per-user concurrency limits~~ (Cancelled)

Unnecessary — model concurrency limits already constrain capacity; fair scheduling (Step 2) distributes users within those limits. DashMap tracking was merged into Step 2.

---

### Step 3: COR-230 — Per-user throughput via OTel

**Goal:** Emit per-user throughput from the daemon's processing loop via structured logs.

**Files:**
- `src/daemon/mod.rs`:
  - Add `user_throughput: Arc<DashMap<String, UserThroughputStats>>` to `Daemon`
  - New struct `UserThroughputStats { completed: AtomicU64, failed: AtomicU64, last_reset: Instant }`
  - On request completion, increment the user's counter
  - Periodic emission using `AtomicU64::swap(0, Ordering::Relaxed)` for race-free counter reset
  - Eviction for inactive users (remove entries with zero counts)
  - Add `throughput_log_interval_ms: Option<u64>` to `DaemonConfig` (default: 60_000)

**No breaking changes** — purely additive.

---

## Version Strategy

Bundle COR-227 + COR-228/229 into a single PR with `feat!:` (major version bump, `13.0.1` → `14.0.0`). COR-229 remaining work (per-user limits, eviction) and COR-230 can be separate `feat:` PRs.

**Downstream:** dwctl will need to update its `fusillade` dependency from `13.x` to `14.x` after the major release.

## Verification

1. `just db-start && just db-setup` — run migrations
2. `just test` — all 123 tests pass (106 lib + 16 integration + 1 doctest)
3. `just lint` — no warnings
4. `cargo sqlx prepare` — offline query cache up to date

## Open Questions

1. **DashMap eviction cadence:** How often should the zero-count sweep run? Alongside heartbeat polling (~30s) seems reasonable.
2. **Weighted fairness:** Current design is equal-share. Should users with higher-priority SLAs get proportionally more capacity? Not in scope for this iteration.
3. **Cross-daemon fairness:** Each daemon's DashMap is independent. For stronger global guarantees, consider periodic DB queries for cluster-wide per-user counts, sampled infrequently to avoid hot-path overhead.
