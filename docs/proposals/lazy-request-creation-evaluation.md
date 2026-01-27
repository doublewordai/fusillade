# Evaluation: Lazy Request Creation Proposal

## Executive Summary

The proposal addresses a real performance bottleneck (O(n) batch creation) with a sound architectural approach. However, several aspects need refinement before implementation, particularly around interactions with the existing denormalized batch status tracking and escalation system.

**Recommendation**: Approve with modifications. The core idea is solid, but the proposal should be updated to address the issues identified below.

---

## 1. Problem Validation

**Verdict: Valid and well-characterized**

The problem is real and measurable:
- Current batch creation (`postgres.rs:2190-2203`) inserts N rows in a single transaction
- For 100k templates: ~30MB data, long-held connection, blocking behavior
- This conflicts with the system's goal of high throughput

Evidence from codebase:
```rust
// postgres.rs:2190-2203 - The problematic mass INSERT
INSERT INTO requests (batch_id, template_id, state, custom_id, retry_attempt, model)
SELECT $1, id, 'pending', custom_id, 0, model
FROM request_templates
WHERE file_id = $2
```

---

## 2. Solution Assessment

### 2.1 Core Approach: `batches_active_in` Array

**Verdict: Sound design choice**

The array-based tracking is clever:
- Avoids expensive NOT EXISTS anti-joins during claiming
- O(k) array operations where k = concurrent batches per template (typically 1-2)
- PostgreSQL handles small arrays efficiently

**Potential issue**: The proposal assumes templates participate in few concurrent batches. If the same file is batched many times without completion, arrays could grow. Consider adding a maximum array size check.

### 2.2 Removing `pending` and `canceled` States

**Verdict: Correct approach**

Making these states implicit/derived is the right call:
- `pending` = template not in `batches_active_in` for batch
- `canceled` = derived from `batch.cancelling_at`

This eliminates the need for request rows that exist only to represent "not yet started" or "won't be started" states.

---

## 3. Critical Issues to Address

### 3.1 Interaction with Denormalized Batch Status (CRITICAL)

**Problem**: The codebase has a trigger-based denormalized status system (`20250107000008_add_batch_status_columns.up.sql`) that updates `batches.pending_requests`, `in_progress_requests`, etc. on every INSERT/UPDATE/DELETE to `requests`.

The proposal's status query (lines 234-257) ignores this and computes counts on-demand:
```sql
SELECT
    (SELECT COUNT(*) FROM request_templates WHERE file_id = b.file_id) as total,
    COUNT(r.id) FILTER (WHERE r.state IN ('claimed', 'processing')) as in_progress,
    ...
```

**Impact**:
1. The existing trigger would decrement `pending_requests` when requests are deleted (proposal deletes during retry), causing negative counts
2. The trigger expects `state = 'pending'` rows to exist

**Required Changes**:
1. Update or remove the `update_batch_on_request_change()` trigger
2. Either:
   - (a) Store `total_templates` on batch at creation time and derive pending, OR
   - (b) Modify trigger to handle lazy creation semantics

**Recommendation**: Option (a) - store `total_templates` at batch creation time. The batch already stores `total_requests` (line 2222), so this is consistent.

### 3.2 Interaction with Escalation System (MODERATE)

**Problem**: The escalation system (`postgres.rs:2604-2744`) creates new templates AND new request rows:

```sql
INSERT INTO requests (
    id, batch_id, template_id, state, custom_id, retry_attempt, model,
    escalated_from_request_id, is_escalated, ...
)
```

**Impact**: Escalation currently relies on creating `pending` request rows immediately. With lazy creation, this would need modification.

**Solution**: The proposal's approach actually simplifies escalation:
1. Create escalated template (as now)
2. The escalated template will be claimed naturally via the new claiming query
3. Add `is_escalated` flag to templates (not requests)

**However**: The `escalated_from_request_id` linkage becomes problematic - the original request may not exist yet (if still pending). Need to track escalation at template level instead.

### 3.3 `retry_attempts` Table and Existing Retry Tracking

**Verdict: Good addition, but consider migration**

The new `retry_attempts` table preserves retry history, which the current system loses (only `retry_attempt` counter survives).

**Current behavior** (`postgres.rs:960-983`):
```sql
UPDATE requests SET
    state = 'pending',
    retry_attempt = retry_attempt + 1,
    not_before = $3
WHERE id = $1
```

**Consideration**: The proposal's retry handling deletes the request row entirely and creates a `retry_attempt` row. This loses the request's `claimed_at`, `started_at` history from the failed attempt. Consider whether this matters for debugging/analytics.

---

## 4. Query Correctness Review

### 4.1 Claiming Query (lines 92-135)

**Issue 1**: Missing `FOR UPDATE OF t SKIP LOCKED`

The proposal shows:
```sql
FOR UPDATE OF t SKIP LOCKED
```

But the current codebase uses `FOR UPDATE OF r SKIP LOCKED` (locking requests). With lazy creation, we need to lock templates instead. This is correct in the proposal.

**Issue 2**: Missing template join for denormalized data

Current claim returns rich data:
```rust
// postgres.rs:807-833
SELECT r.id, r.batch_id, t.endpoint, t.method, t.path, t.body, t.api_key, ...
```

The proposal's `created AS` CTE only returns `id`. Need to also return the full request data for the daemon.

### 4.2 Retry Query (lines 149-168)

**Issue**: The query uses a comma join with `deleted`:
```sql
FROM retry_attempts ra, deleted d
WHERE ra.template_id = d.template_id AND ra.batch_id = d.batch_id
```

This will fail if `deleted` is empty. Should use:
```sql
FROM deleted d
LEFT JOIN retry_attempts ra ON ra.template_id = d.template_id AND ra.batch_id = d.batch_id
```

### 4.3 Batch Status Query (lines 234-257)

**Issue**: The subquery `COUNT(*) FROM request_templates WHERE file_id = b.file_id` counts ALL templates in the file, but escalated templates are in a different file (`escalation_templates` file). This is actually correct behavior (escalated requests shouldn't count toward total).

---

## 5. Migration Strategy Review

### 5.1 Phase 1: Schema Additions

**Verdict: Safe**

Adding a column with `DEFAULT '{}'` and creating a new table are non-breaking.

### 5.2 Phase 2: Maintenance Window

**Verdict: Correct but incomplete**

The backfill query (lines 382-394) is correct. However, the proposal should also address:

1. **Trigger modification**: The `update_batch_on_request_change()` trigger needs updating before deleting pending rows, otherwise batch counts go negative.

2. **Escalation link migration**: Existing `escalated_from_request_id` links need to be converted to template-level tracking.

### 5.3 Rollback Plan

**Verdict: Sound**

The rollback query (lines 427-435) correctly recreates pending rows using NOT EXISTS.

---

## 6. Performance Analysis

### 6.1 Batch Creation

| Metric | Before | After |
|--------|--------|-------|
| Rows inserted | N | 1 |
| Transaction size | O(N × row_size) | O(1) |
| Connection hold time | Seconds (large files) | Milliseconds |

**Verdict: Significant improvement**

### 6.2 Claiming

| Metric | Before | After |
|--------|--------|-------|
| Index scanned | `idx_requests_pending_claim` | `idx_request_templates_file_id` + `batches_active_in` |
| Locks | Request rows | Template rows |
| Per-claim overhead | UPDATE | UPDATE + INSERT |

**Verdict: Similar complexity, slightly more work per claim**

The extra INSERT per claim is acceptable - claims are rate-limited by concurrency controls anyway.

### 6.3 Batch Status

**Verdict: Potentially worse**

The proposal's status query joins `request_templates` (full table) with `requests`. For large files, this could be expensive.

**Recommendation**: Store `total_templates` on batch at creation time:
```sql
UPDATE batches SET total_templates = (
    SELECT COUNT(*) FROM request_templates WHERE file_id = $1
) WHERE id = $2;
```

Then status is O(1) read + O(active requests) count.

---

## 7. Missing Considerations

### 7.1 Supersession Handling

The `superseded` state and `superseded_by_request_id` column aren't addressed. With lazy creation:
- Supersession should work naturally (both original and escalated requests exist when racing)
- But the escalation linkage needs redesign (see 3.2)

### 7.2 `is_escalated` Flag

The proposal doesn't address where `is_escalated` lives. Currently on `requests`, it should move to `request_templates` with lazy creation.

### 7.3 Request Denormalization

Current requests snapshot template data (`endpoint`, `method`, `path`, `body`, `api_key`). The proposal's claiming query doesn't show this denormalization happening at claim time.

**Required**: The `INSERT INTO requests` in the claiming CTE needs to include the snapshot fields, or join them from templates.

### 7.4 Index Requirements

The proposal mentions a GIN index on `batches_active_in` might be needed. Actually:
- For claiming: No GIN needed - the `ANY()` check is O(k) per row
- For cleanup: GIN would help `WHERE batch_id = ANY(batches_active_in)`

Consider adding: `CREATE INDEX idx_templates_active ON request_templates USING GIN (batches_active_in)` for cleanup queries only.

---

## 8. Open Questions Resolution

### Q1: Index on `batches_active_in`?

**Answer**: Not needed initially. The array check during claiming is O(k) where k is small. Add GIN index later only if cleanup becomes slow.

### Q2 (unasked): What about the `custom_id` field?

The proposal's claiming query doesn't set `custom_id` on created requests. This needs to be populated from templates.

---

## 9. Recommended Changes to Proposal

1. **Add trigger migration section**: The `update_batch_on_request_change()` trigger must be modified to handle lazy creation semantics.

2. **Store `total_templates` on batch**: Add to batch creation query and use for status derivation.

3. **Redesign escalation linkage**: Move from `escalated_from_request_id` (request-level) to `escalated_from_template_id` (template-level).

4. **Complete claiming query**: Include all denormalized fields (endpoint, method, path, body, api_key, custom_id) in the INSERT and RETURNING clauses.

5. **Fix retry query**: Use explicit JOIN instead of comma join to handle empty `deleted` CTE.

6. **Address `is_escalated`**: Either:
   - (a) Move to templates, or
   - (b) Accept that escalated requests always have request rows (created by escalation, not claiming)

7. **Add `superseded` state handling**: Clarify that superseded requests continue to work as-is (they only exist after being claimed and completing).

---

## 10. Conclusion

The lazy request creation proposal is architecturally sound and addresses a real scalability issue. The core insight - using `batches_active_in` arrays instead of existence checks - is elegant and efficient.

However, the proposal needs updates to properly integrate with:
1. The existing denormalized batch status trigger system
2. The escalation/supersession tracking system
3. Request denormalization (template snapshots)

With these modifications, I recommend proceeding with implementation. The maintenance window requirement is acceptable given the performance benefits for large-scale usage.

**Priority for implementation**:
1. HIGH: Trigger modification (blocks everything else)
2. HIGH: Claiming query completion (core functionality)
3. MEDIUM: Escalation redesign (affects SLA features)
4. LOW: Cleanup job (can be added post-launch)
