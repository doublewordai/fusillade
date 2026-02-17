# Purge Task Replica Coordination

**Date:** 2026-02-11
**Status:** ✅ Completed

## Problem

The orphaned row purge task runs independently on every daemon replica. Each
replica periodically deletes `requests` and `request_templates` whose parent
batch or file has been soft-deleted (right-to-erasure compliance).

With N replicas all firing `DELETE ... LIMIT 1000` against the same orphaned
rows, the current implementation causes:

- **Wasted work** — multiple replicas select and attempt to delete the same rows
- **Lock contention** — concurrent deletes on overlapping row sets cause
  serialization waits in PostgreSQL

## Solution

Add `FOR UPDATE SKIP LOCKED` to the subquery in each purge delete. This is the
same row-locking pattern used by `claim_requests` for partitioning work across
replicas.

### Requests purge query

```sql
DELETE FROM requests WHERE id IN (
    SELECT r.id
    FROM requests r
    LEFT JOIN batches b ON r.batch_id = b.id
    WHERE r.batch_id IS NULL OR b.deleted_at IS NOT NULL
    LIMIT $1
    FOR UPDATE OF r SKIP LOCKED   -- added
)
```

### Templates purge query

```sql
DELETE FROM request_templates WHERE id IN (
    SELECT rt.id
    FROM request_templates rt
    LEFT JOIN files f ON rt.file_id = f.id
    WHERE rt.file_id IS NULL OR f.deleted_at IS NOT NULL
    LIMIT $1
    FOR UPDATE OF rt SKIP LOCKED  -- added
)
```

No `NOT EXISTS` guard is needed — `delete_file` already cancels dependent
batches and unlinks them (`file_id = NULL` on batches) without deleting the
batches or their requests, so users can still download results. Deleting
templates triggers `ON DELETE SET NULL` on `requests.template_id`, which is
harmless since requests are self-contained once created (all template data is
copied at claim time).

### Behavior

When two replicas run purge concurrently:

1. Replica A's `SELECT ... FOR UPDATE` locks up to 1000 orphaned rows
2. Replica B's `SELECT ... FOR UPDATE SKIP LOCKED` skips A's locked rows and
   selects the *next* 1000 orphaned rows
3. Both replicas delete disjoint batches in parallel — no contention, no wasted
   work

When only one replica runs (the common case), behavior is identical to before.

## Multi-Replica Considerations

### No interference with request processing

The purge and claim/read paths operate on **disjoint row sets** by definition:

- Purge targets rows where `b.deleted_at IS NOT NULL` (or `batch_id IS NULL`)
- The `active_requests` view (used by claim and all read queries) filters to
  `b.deleted_at IS NULL`

A `FOR UPDATE` lock held by the purge task will never block a claim query or
view read, because they never touch the same rows. Additionally, PostgreSQL's
MVCC means plain `SELECT` reads are never blocked by row locks regardless.

### Staggered polling reduces overlap

The purge task runs on a configurable interval (default: `purge_interval_ms =
3,600,000` — 1 hour). With N replicas starting at slightly different times, the
average interval between purge cycles across the cluster is `interval / N`:

| Replicas | Avg gap between purge cycles | Overlap likelihood |
|----------|-----------------------------|--------------------|
| 1        | 60 min                      | None               |
| 2        | 30 min                      | Very rare          |
| 3        | 20 min                      | Very rare          |
| 5        | 12 min                      | Rare               |

Each purge cycle completes in milliseconds (batch of 1000 deletes), so even
when two cycles coincide, the overlap window is tiny. `SKIP LOCKED` handles
the rare overlap gracefully.

### Existing throttling still applies

The daemon's drain loop already limits purge throughput:

- `purge_batch_size` (default 1000) — caps rows per iteration
- `purge_throttle_ms` (default 100ms) — delay between iterations within a drain
- Drain stops when `purge_orphaned_rows` returns 0

These controls prevent any single replica from generating sustained DB load,
and `SKIP LOCKED` ensures multiple replicas don't duplicate the same work.

## Safety: File Deletion, Purging, and Claiming

### No requests are claimed after their file is deleted

`delete_file` runs as a **single atomic transaction** that:

1. Sets `cancelling_at` on all non-terminal batches for the file
2. Clears `output_file_id` / `error_file_id` references on batches
3. Soft-deletes the file (`deleted_at = NOW()`)

All three steps commit together. The claim query filters
`WHERE b.cancelling_at IS NULL`, so once the transaction commits, no daemon
can claim requests from those batches. The purge task only sees
`f.deleted_at IS NOT NULL` *after* the same transaction has committed, so
there is no window where templates could be purged before the batch is marked
as cancelled.

Ordering guarantee:

```
1. delete_file commits
   → batches.cancelling_at set, files.deleted_at set (atomic)

2. claim_requests sees cancelling_at
   → stops claiming requests from those batches (immediate)

3. Purge task (hours later) sees deleted_at
   → deletes orphaned templates
```

### Batch results remain downloadable after file deletion

`delete_file` cancels batches and sets `file_id = NULL` on them, but does
**not** delete batches or their requests. This means:

- **Batch is still queryable** — `get_batch()` returns the batch (cancelled,
  `file_id = NULL`)
- **Output file stream works** — `stream_batch_output` queries `requests`
  directly by `batch_id` with no template join, so completed results are
  fully accessible
- **Error file stream works** — `stream_batch_error` also queries `requests`
  directly by `batch_id`
- **Input file is inaccessible** — `get_file_content_stream` checks
  `WHERE deleted_at IS NULL`, so the soft-deleted input file returns an error
- **Batch results stream is empty** — `stream_batch_results` joins on
  `request_templates` (for the merged input+output view), which returns no
  rows after templates are purged. This is expected — the input data has been
  erased per the deletion request

Template deletion triggers `ON DELETE SET NULL` on `requests.template_id`.
This is harmless because requests are self-contained once created — all
template data (endpoint, method, path, body, model, api_key) is copied onto
the request row at claim time.

## Alternative Considered: Leader Election

An alternative approach would use PostgreSQL advisory locks to elect a single
purge leader:

```rust
let acquired = sqlx::query_scalar!(
    "SELECT pg_try_advisory_lock(hashtext('fusillade_purge'))"
).fetch_one(pool).await?;

if !acquired { return Ok(0); }
// ... purge ...
sqlx::query!("SELECT pg_advisory_unlock(hashtext('fusillade_purge'))")
    .execute(pool).await?;
```

### Why we chose SKIP LOCKED instead

| Factor | SKIP LOCKED | Advisory Lock |
|--------|-------------|---------------|
| Parallelism | All replicas contribute | Only leader purges |
| Complexity | 1 SQL clause per query | Lock acquire/release lifecycle, error handling |
| Connection pool safety | No concerns | Advisory locks are session-scoped — risk of leaked locks with pooled connections |
| Failure mode | Locks auto-release on transaction end | Must explicitly release or risk blocking all replicas |
| Consistency | Same pattern as `claim_requests` | New pattern to maintain |

Leader election would make sense if the purge task were expensive and should run
on exactly one node. But purge is already lightweight (batched, throttled) and
the `SKIP LOCKED` approach lets all replicas share the work when there's a large
backlog (e.g., after a bulk file deletion).

## Files Changed

- `src/manager/postgres.rs` — Added `FOR UPDATE SKIP LOCKED` to both purge
  subqueries in `purge_orphaned_rows()`
- `.sqlx/` — Regenerated prepared query cache
