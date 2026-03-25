# Per-User Fair Scheduling

**Date:** 2026-03-25
**Status:** Planned
**Last revised:** 2026-03-25

## Overview

Fusillade currently schedules requests globally by batch deadline (`expires_at ASC`). A single high-volume user can monopolise model capacity. The "Enabling Background Agents" project requires per-user fair scheduling so throughput is distributed evenly across users (TTFT guarantee).

Linear issues: COR-226 (parent), COR-227, COR-228, COR-229, COR-230.

## Key Insight: No New Column Needed

The `claim_requests` query already joins `batches b` and returns `b.created_by` (line 887 of `postgres.rs`). The `active_batch_ids` CTE just needs to include `created_by` and the ordering needs to partition by it. No migration to add `user_id` to `requests`.

---

## Implementation Order

### Step 1: COR-227 — Ensure `created_by` is reliable

**Goal:** Make `batches.created_by` non-nullable so the scheduler always has a user key.

**Files:**
- `migrations/` — New migration:
  ```sql
  -- Backfill legacy rows with a unique sentinel so they don't all collapse
  -- into a single scheduling bucket. Using batch ID makes each distinct.
  UPDATE batches SET created_by = 'unknown-' || id::text WHERE created_by IS NULL;
  UPDATE batches SET created_by = 'unknown-' || id::text WHERE TRIM(created_by) = '';
  ALTER TABLE batches ALTER COLUMN created_by SET NOT NULL;
  ALTER TABLE batches ALTER COLUMN created_by SET DEFAULT '';
  ```
- `src/batch/mod.rs` lines 364, 412, 438 — Keep `BatchInput.created_by` as `Option<String>` for API backwards compat, but change `Batch.created_by` to `String`
- `src/manager/postgres.rs` — Update `batch_from_dynamic_row!` macro (line ~106) and `create_batch` INSERT (line ~2118) to coalesce `None` → `""`

**Why `'unknown-' || id` instead of empty string:** If we default NULLs to `''`, all legacy batches collapse into a single user for scheduling purposes. A user with zero `created_by` attribution who happens to have 10,000 legacy pending requests would monopolise capacity — exactly the problem we're solving. Using `'unknown-' || id` ensures each legacy batch is treated independently and doesn't starve real users.

**Breaking change:** `Batch.created_by: Option<String>` → `String`. This is a minor breaking change for downstream consumers that pattern-match on it.

---

### Step 2: COR-228 — Rewrite `claim_requests` for per-user fair scheduling

**Goal:** Round-robin across users within each model's capacity, maintaining deadline ordering within each user.

**Files:**
- `src/manager/postgres.rs` — Rewrite the SQL in `claim_requests` (lines 829-893)

#### Approach: LATERAL with interleaved ordering

The current query uses `CROSS JOIN LATERAL` with `FOR UPDATE SKIP LOCKED` directly inside the lateral subquery. This pattern is critical — the lock is acquired as rows are scanned, so skipped (already-locked) rows are naturally replaced by the next candidate. **We must preserve this property.**

A previous draft used a separate `ranked` CTE with `ROW_NUMBER()` followed by a join back to `requests` for locking. This is **incorrect under concurrent daemons**: the ranking is computed before locks are acquired, so two daemons compute identical rankings, one succeeds and the other skips rows, getting fewer than `capacity` results and breaking fairness guarantees.

**Correct approach:** Compute `user_rank` inside a subquery that feeds into the LATERAL, so ranking and locking happen in the same scan:

```sql
WITH active_batch_ids AS MATERIALIZED (
    SELECT b.id, b.expires_at, b.created_by
    FROM batches b
    WHERE b.cancelling_at IS NULL
        AND b.deleted_at IS NULL
        AND b.completed_at IS NULL
        AND b.failed_at IS NULL
        AND b.cancelled_at IS NULL
        AND EXISTS (
            SELECT 1 FROM requests r
            WHERE r.batch_id = b.id AND r.state = 'pending'
        )
),
to_claim AS (
    SELECT claimed.id, claimed.template_id, claimed.batch_id
    FROM unnest($4::TEXT[], $5::BIGINT[]) AS m(model, capacity)
    CROSS JOIN LATERAL (
        SELECT r3.id, r3.template_id, r3.batch_id
        FROM requests r3
        JOIN active_batch_ids ab ON ab.id = r3.batch_id
        WHERE r3.state = 'pending'
            AND r3.model = m.model
            AND r3.template_id IS NOT NULL
            AND (r3.not_before IS NULL OR r3.not_before <= $3)
        ORDER BY
            -- Primary: interleave users by their per-user rank
            ROW_NUMBER() OVER (
                PARTITION BY ab.created_by
                ORDER BY ab.expires_at ASC, ab.id ASC, r3.id ASC
            ),
            -- Secondary: break ties by deadline across users
            ab.expires_at ASC,
            ab.id ASC
        LIMIT m.capacity
        FOR UPDATE OF r3 SKIP LOCKED
    ) claimed
    LIMIT $2::BIGINT
)
UPDATE requests r SET ...
```

**Important caveat about window functions + SKIP LOCKED:** PostgreSQL evaluates `ROW_NUMBER()` *before* lock acquisition. When a concurrent daemon has already locked some rows, `SKIP LOCKED` skips them but the window function ranks were computed assuming they were present. This means rank numbers can have gaps, but the *relative ordering* (interleave by user, deadline within user) is preserved — a user's rank-2 request is still ordered after all users' rank-1 requests. The `LIMIT` still fills correctly because `SKIP LOCKED` continues scanning past locked rows.

**Edge case:** Under very high contention (many concurrent daemons), some daemons may get slightly fewer rows than `capacity` because skipped rows still count against the window function's partition. This is acceptable — the alternative (application-level claiming) has worse tradeoffs. **Test with concurrent daemons to validate behaviour is acceptable at expected contention levels.**

**Performance:** This approach keeps the LATERAL pattern, avoiding the full materialization of all pending requests across all models that the previous `ranked` CTE approach required. The query planner only scans up to `capacity` rows per model (plus skipped locked rows), same as today.

**Testing:**
- New integration test: 3 users with varying request counts, verify interleaved claiming
- **New concurrency test: 2+ concurrent `claim_requests` calls, verify both get results and fairness is approximately maintained**
- Existing deadline-ordering tests should still pass for single-user scenarios
- **Benchmark: compare query plan and execution time against current query with realistic data volumes**

**`cargo sqlx prepare`** must be run after changing this query to update the offline query cache in `.sqlx/`.

---

### Step 3: COR-229 — Per-user in-flight tracking

**Goal:** Track per-user in-flight counts; optionally enforce per-user concurrency limits.

**Files:**
- `src/daemon/mod.rs`:
  - Add `user_requests_in_flight: Arc<DashMap<String, AtomicUsize>>` to `Daemon` struct (line ~275)
  - Add `per_user_concurrency_limit: Option<usize>` to `DaemonConfig` (line ~78), default `None`
  - Extract owner from `request.data.batch_metadata.get("created_by")` before spawning (line ~684). Add `"created_by"` to `default_batch_metadata_fields()` (line 221).
  - Increment/decrement `user_requests_in_flight` in spawn/scopeguard (lines 712-750)
  - **Add periodic eviction:** sweep `user_requests_in_flight` and remove entries where count is 0. Run alongside heartbeat/cancellation polling to prevent unbounded DashMap growth under high user churn.
- `src/manager/mod.rs` line 292 — Refactor `claim_requests` to accept a `ClaimOptions` struct:
  ```rust
  pub struct ClaimOptions {
      pub limit: usize,
      pub daemon_id: DaemonId,
      pub available_capacity: HashMap<String, usize>,
      pub per_user_limit: Option<usize>,
  }
  ```
- `src/manager/postgres.rs` — Add `WHERE user_rank <= $6` inside the LATERAL when `per_user_limit` is `Some`

**Why `ClaimOptions` instead of positional params:** The `Storage::claim_requests` trait is public. Adding positional parameters is fragile — the next feature that needs a claim option would break the signature again. A struct is extensible: new fields with defaults don't break existing implementors.

**Breaking change:** `Storage::claim_requests` signature change (positional params → `ClaimOptions` struct). This is a trait method on a public trait — major version bump. Bundle with COR-227's breaking change.

**DashMap eviction:** Without cleanup, the `user_requests_in_flight` map accumulates entries for every user who has ever had a request processed. Under high user churn (e.g., thousands of API key holders), this becomes a memory leak. The eviction sweep is cheap — iterate and remove zeros — and runs on the existing periodic task cadence.

---

### Step 4: COR-230 — Per-user throughput via OTel

**Goal:** Emit per-user throughput from the daemon's processing loop via structured logs.

**Files:**
- `src/daemon/mod.rs`:
  - Add `user_throughput: Arc<DashMap<String, UserThroughputStats>>` to `Daemon`
  - New struct `UserThroughputStats { completed: AtomicU64, failed: AtomicU64, last_reset: Instant }`
  - On request completion (lines 797-818), increment the user's counter
  - New periodic task (alongside heartbeat/cancellation polling): every N seconds, iterate the map and emit structured logs:
    ```rust
    tracing::info!(
        user = %user_id,
        completed = completed_count,
        failed = failed_count,
        throughput_rpm = requests_per_minute,
        window_seconds = elapsed,
        "fusillade.user_throughput"
    );
    ```
  - **Use `AtomicU64::swap(0, Ordering::Relaxed)` to atomically read-and-reset counters.** A non-atomic read-then-reset has a race window where increments between read and reset are lost.
  - **Add eviction:** remove entries from `user_throughput` where both completed and failed are 0 and `last_reset` is older than 2× the emission interval. Same rationale as Step 3 — prevent unbounded map growth.
  - Add `throughput_log_interval_ms: Option<u64>` to `DaemonConfig` (default: 60_000)

**No breaking changes** — purely additive.

---

## Version Strategy

Bundle COR-227 through COR-229 into a single PR with `feat!:` (major version bump, `13.0.1` → `14.0.0`). COR-230 can be a separate `feat:` PR (minor bump, `14.1.0`) since it's purely additive.

Or: ship all four as one major version release.

**Downstream:** dwctl will need to update its `fusillade` dependency from `13.x` to `14.x` after the major release. Follow the library release flow in the workspace CLAUDE.md.

## Verification

1. `just db-start && just db-setup` — run migrations
2. `just test` — all existing tests pass
3. New integration tests:
   - Multi-user fair scheduling: 3 users, verify interleaved claims
   - **Concurrent daemon claiming: 2+ tasks calling `claim_requests` simultaneously, verify both get results**
   - Per-user concurrency limit: verify limit enforcement
   - Throughput logging: verify structured log emission after processing
4. `just lint` — no warnings
5. `cargo sqlx prepare` — update offline query cache
6. **Benchmark `claim_requests` query plan with `EXPLAIN ANALYZE` on realistic data (10k+ pending requests, 50+ users, 5+ models)**

## Open Questions

1. **Contention threshold:** At what daemon concurrency level does the window-function-before-SKIP-LOCKED edge case cause meaningful capacity under-fill? Benchmark to determine if application-level round-robin fallback is needed.
2. **Weighted fairness:** Current design is equal-share. Should users with higher-priority SLAs get proportionally more capacity? Not in scope for this iteration but worth noting for future work.
