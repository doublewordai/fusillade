# Proposal: Lazy Request Creation

## Problem

Batch creation currently requires a mass INSERT from `request_templates` into
`requests`:

```sql
INSERT INTO requests (batch_id, template_id, state, custom_id, retry_attempt, model)
SELECT $1, id, 'pending', custom_id, 0, model
FROM request_templates
WHERE file_id = $2
```

For a file with 100k templates, this inserts 100k rows (~30MB) in a single
transaction. This is slow and blocks other operations. It also uses a
connection for the duration of the creation, which is bad.

## Proposal

Eliminate the mass insert by creating request rows lazily — only when templates
are actually claimed.

With this change, the `pending` and `canceled` states are removed from requests.
"Pending" is implicit — templates not in `batches_active_in` for a given batch.
"Canceled" is derived from `batch.cancelling_at`. Request rows only exist for:
claimed, processing, completed, failed.

### Key Insight

Instead of checking "does a request row exist?" (which requires NOT EXISTS or
anti-join), track active batches directly on the template row using an array of
batch IDs.

## Schema Changes

### 1. Add `batches_active_in` array to templates

```sql
ALTER TABLE request_templates
    ADD COLUMN batches_active_in UUID[] NOT NULL DEFAULT '{}';
```

The array contains batch IDs that have an active request for this template. When
claiming, we check `NOT (batch_id = ANY(batches_active_in))` which is
O(array_length) — fast when the array is small.

### 2. Add `retry_attempts` table

```sql
CREATE TABLE retry_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    template_id UUID REFERENCES request_templates(id) ON DELETE SET NULL,
    batch_id UUID REFERENCES batches(id) ON DELETE SET NULL,
    attempt INTEGER NOT NULL,
    error_code TEXT,
    not_before TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX retry_attempts_latest
    ON retry_attempts (template_id, batch_id, attempt DESC);
```

This table is append-only — each retry inserts a new row. This preserves full
history of retry attempts (timing, backoff, error codes) for debugging and
analytics. The table (should) stay small since retries are rare.

Using `ON DELETE SET NULL` avoids blocking deletes — see [Orphan Cleanup](#orphan-cleanup).

## Operations

### Batch Creation

Now O(1) — just create the batch row:

```sql
INSERT INTO batches (file_id, endpoint, completion_window, metadata, created_by, expires_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;
```

No request rows are created. The "pending" work is implicit: all templates in
the file that aren't in `batches_active_in` for this batch.

### Claiming Requests

Single pool — templates not currently active for any batch, respecting retry
backoff from `retry_attempts`:

```sql
WITH
-- Get latest retry per template/batch to check not_before. 
latest_retry AS (
    SELECT DISTINCT ON (template_id, batch_id)
        template_id, batch_id, not_before
    FROM retry_attempts
    ORDER BY template_id, batch_id, attempt DESC
),

-- Find claimable templates
claimable AS (
    SELECT t.id as template_id, b.id as batch_id
    FROM request_templates t
    JOIN batches b ON b.file_id = t.file_id
    LEFT JOIN latest_retry lr ON lr.template_id = t.id AND lr.batch_id = b.id
    WHERE t.model = $1
      AND b.cancelling_at IS NULL -- no cancelled requests
      AND NOT (b.id = ANY(t.batches_active_in)) -- i.e. not already active?
      AND (lr.not_before IS NULL OR lr.not_before <= $2) -- retry data doesn't tell us to wait
    ORDER BY b.expires_at ASC NULLS LAST, t.line_number
    LIMIT $3
    FOR UPDATE OF t SKIP LOCKED
),

-- Mark templates as active (batch_id guaranteed not present due to WHERE clause above)
marked AS (
    UPDATE request_templates t
    SET batches_active_in = array_append(t.batches_active_in, c.batch_id)
    FROM claimable c
    WHERE t.id = c.template_id
    RETURNING c.template_id, c.batch_id
),

-- Create request rows
created AS (
    INSERT INTO requests (batch_id, template_id, state, daemon_id, claimed_at, model)
    SELECT m.batch_id, m.template_id, 'claimed', $4, $2, t.model
    FROM marked m
    JOIN request_templates t ON t.id = m.template_id
    RETURNING id
)

SELECT * FROM created;
```

**Parameters:**

- `$1` — model (TEXT)
- `$2` — now (TIMESTAMPTZ)
- `$3` — limit (INTEGER)
- `$4` — daemon_id (UUID)

### Retry Handling

When a request fails and should be retried:

```sql
WITH
deleted AS (
    DELETE FROM requests
    WHERE id = $1
    RETURNING template_id, batch_id
),
updated_template AS (
    UPDATE request_templates t
    SET batches_active_in = array_remove(t.batches_active_in, d.batch_id)
    FROM deleted d
    WHERE t.id = d.template_id
),
prev_attempt AS (
    SELECT COALESCE(MAX(attempt), 0) as attempt
    FROM retry_attempts ra, deleted d
    WHERE ra.template_id = d.template_id AND ra.batch_id = d.batch_id
)
INSERT INTO retry_attempts (template_id, batch_id, attempt, error_code, not_before)
SELECT d.template_id, d.batch_id, p.attempt + 1, $2, $3
FROM deleted d, prev_attempt p;
```

**Parameters:**

- `$1` — request_id (UUID)
- `$2` — error_code (TEXT)
- `$3` — not_before (TIMESTAMPTZ)

The template is now "inactive" for that batch and will be picked up by the
claiming query once `not_before` has passed.

### Unclaiming

When a claimed request needs to be released without retry (e.g., daemon shutdown,
timeout, or batch cancellation discovered mid-flight):

```sql
WITH
deleted AS (
    DELETE FROM requests
    WHERE id = $1
    RETURNING template_id, batch_id
)
UPDATE request_templates t
SET batches_active_in = array_remove(t.batches_active_in, d.batch_id)
FROM deleted d
WHERE t.id = d.template_id;
```

**Parameters:**

- `$1` — request_id (UUID)

The template returns to the implicit "pending" pool and can be claimed again
immediately.

### Request Completion

When a request completes (success or terminal failure):

```sql
WITH
updated AS (
    UPDATE requests
    SET state = $2, completed_at = $3
    WHERE id = $1
    RETURNING template_id, batch_id
)
UPDATE request_templates t
SET batches_active_in = array_remove(t.batches_active_in, u.batch_id)
FROM updated u
WHERE t.id = u.template_id;
```

**Parameters:**

- `$1` — request_id (UUID)
- `$2` — state ('completed' or 'failed')
- `$3` — completed_at (TIMESTAMPTZ)

This removes the batch ID from the template's array immediately, keeping arrays
small and containing only genuinely in-flight requests.

### Batch Status

```sql
SELECT
    b.id as batch_id,
    b.cancelling_at,
    (SELECT COUNT(*) FROM request_templates WHERE file_id = b.file_id) as total,
    COUNT(r.id) FILTER (WHERE r.state IN ('claimed', 'processing')) as in_progress,
    COUNT(r.id) FILTER (WHERE r.state = 'completed') as completed,
    COUNT(r.id) FILTER (WHERE r.state = 'failed') as failed
FROM batches b
LEFT JOIN requests r ON r.batch_id = b.id
WHERE b.id = $1
GROUP BY b.id;
```

Application logic derives pending/canceled based on batch state:

```
if cancelling_at IS NULL:
    pending = total - (in_progress + completed + failed)
    canceled = 0
else:
    pending = 0
    canceled = total - (completed + failed)
```

The `retry_attempts` table can be queried separately if detailed retry history is
needed, but it doesn't factor into the main status calculation.

### Batch Cancellation

Cancellation is O(1) — just set the timestamp:

```sql
UPDATE batches SET cancelling_at = NOW() WHERE id = $1;
```

This excludes the batch from claiming (the claim query checks `cancelling_at IS NULL`).
In-flight requests complete naturally — the daemon checks batch state when finishing
and handles accordingly. No request rows need to be updated.

The status query returns `cancelling_at`, and application logic derives the
canceled count as `total - completed - failed`.

## Array Accumulation

The `batches_active_in` array only contains batch IDs for genuinely in-flight
requests. Entries are removed when:

1. A request completes (success or terminal failure)
2. A request is retried (removed, then re-added on next claim)
3. A request is unclaimed (daemon shutdown, timeout)

This keeps arrays small without requiring periodic cleanup.

## Orphan Cleanup

This proposal uses `ON DELETE SET NULL` for foreign keys, consistent with the
rest of the codebase. This avoids large cascading deletes blocking the database
when batches or files are deleted. The trade-off is orphaned data that needs
periodic cleanup.

### Sources of Orphaned Data

1. **`batches_active_in` arrays** — May contain IDs of deleted batches. Harmless
   for correctness (deleted batch IDs won't match any active batch), but arrays
   could grow unbounded.

2. **`retry_attempts` rows** — When a batch or template is deleted, the foreign
   key becomes NULL. These rows are no longer useful.

### Cleanup Strategy (optional)

A single background job handles all orphan cleanup:

```sql
-- 1. Clean stale batch IDs from batches_active_in arrays
UPDATE request_templates t
SET batches_active_in = (
    SELECT COALESCE(array_agg(bid), '{}')
    FROM unnest(t.batches_active_in) AS bid
    WHERE EXISTS (SELECT 1 FROM batches WHERE id = bid)
)
WHERE array_length(t.batches_active_in, 1) > 0
  AND EXISTS (
    SELECT 1 FROM unnest(t.batches_active_in) AS bid
    WHERE NOT EXISTS (SELECT 1 FROM batches WHERE id = bid)
  );

-- 2. Delete orphaned retry_attempts
DELETE FROM retry_attempts
WHERE template_id IS NULL OR batch_id IS NULL;
```

This can run during quiet periods (e.g., nightly) and is not on any critical
path. The cleanup is distributed across many small writes rather than one large
cascade.

## Performance Characteristics

| Operation | Before | After |
|-----------|--------|-------|
| Batch creation | O(n) — insert n request rows | O(1) — insert 1 batch row |
| Claim | O(log n) — index scan on requests | O(log n) — index scan on templates + O(k) array check |
| Batch status | O(n) — count request states | O(n) — count templates + requests |

**Trade-off:** The main win is batch creation going from O(n) to O(1). Claiming
and status queries have roughly equivalent complexity. Claim performance has an
additional O(k) factor for `batches_active_in` array checks, where k is the
number of active batches for each template (typically very small).

## Migration Plan

This migration requires a maintenance window. The core issue is that old code
creates request rows on batch creation, while new code doesn't — mixing them
during a rolling deployment would cause requests to be claimed twice or not at
all.

### Phase 1: Schema Additions

These changes are backward-compatible and can be deployed without downtime.

```sql
-- 1. Add batches_active_in column
ALTER TABLE request_templates
    ADD COLUMN batches_active_in UUID[] NOT NULL DEFAULT '{}';

-- 2. Create retry_attempts table
CREATE TABLE retry_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    template_id UUID REFERENCES request_templates(id) ON DELETE SET NULL,
    batch_id UUID REFERENCES batches(id) ON DELETE SET NULL,
    attempt INTEGER NOT NULL,
    error_code TEXT,
    not_before TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX retry_attempts_latest
    ON retry_attempts (template_id, batch_id, attempt DESC);
```

### Phase 2: Maintenance Window

1. **Stop all daemons** — No new requests should be claimed during migration.

2. **Backfill `batches_active_in`** for in-flight requests:

   ```sql
   UPDATE request_templates t
   SET batches_active_in = (
       SELECT COALESCE(array_agg(DISTINCT r.batch_id), '{}')
       FROM requests r
       WHERE r.template_id = t.id
         AND r.state IN ('claimed', 'processing')
   )
   WHERE EXISTS (
       SELECT 1 FROM requests r
       WHERE r.template_id = t.id
         AND r.state IN ('claimed', 'processing')
   );
   ```

3. **Delete pending request rows** — These become implicit:

   ```sql
   DELETE FROM requests WHERE state = 'pending';
   ```

4. **Delete canceled request rows** — These are now derived from `batch.cancelling_at`: TODO: Are we really confident these should be deleted? It does make sense they would be.

   ```sql
   DELETE FROM requests WHERE state = 'canceled';
   ```

5. **Deploy new application code** with all logic changes:
   - Batch creation: no longer inserts request rows
   - Claiming: uses new query (creates request rows lazily)
   - Request completion: removes from `batches_active_in`
   - Retry handling: deletes request, removes from array, inserts `retry_attempt`
   - Unclaiming: deletes request, removes from array
   - Batch status: derives pending/canceled from counts

6. **Restart daemons**

### Rollback Plan

If issues are discovered after deployment:

1. Stop daemons
2. Deploy old application code
3. Re-create pending request rows for incomplete batches:

   ```sql
   INSERT INTO requests (batch_id, template_id, state, custom_id, model)
   SELECT b.id, t.id, 'pending', t.custom_id, t.model
   FROM batches b
   JOIN request_templates t ON t.file_id = b.file_id
   WHERE b.cancelling_at IS NULL
     AND NOT EXISTS (
       SELECT 1 FROM requests r
       WHERE r.batch_id = b.id AND r.template_id = t.id
     );
   ```

4. Restart daemons

The `batches_active_in` column and `retry_attempts` table can remain — they're
unused by old code but harmless.

## Open Questions

1. **Index on `batches_active_in`?**
   - A GIN index would speed up cleanup queries but isn't needed for claiming
   - Recommend: Add later if cleanup becomes a bottleneck

## Deferred Work

These items are not required for the initial migration but should be addressed
later.

### Orphan Cleanup Job

Schedule a periodic job (e.g., nightly) to clean up orphaned data. See
[Orphan Cleanup](#orphan-cleanup) for the queries.

### State Column Cleanup

The `state` column is `TEXT`, so `pending` and `canceled` values are no longer
written but the schema doesn't enforce this. Options:

1. **Do nothing** — The column allows any text, old values are harmless
2. **Add CHECK constraint** — `CHECK (state IN ('claimed', 'processing', 'completed', 'failed'))`
3. **Convert to enum** — More restrictive but requires migration

Recommendation: Option 1 for now, revisit if it causes confusion.
