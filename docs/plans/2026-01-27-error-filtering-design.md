# Error Filtering Implementation

**Date:** 2026-01-27
**Status:** ✅ Completed

## Overview

This document describes the SLA-aware error filtering feature added to Fusillade, which allows clients to hide transient retriable errors (rate limits, network failures) before their batch SLA expires, while always showing permanent non-retriable errors (validation failures, bad requests).

## Motivation

Previously, all failed requests were aggregated into a single `failed_requests` count. This created several problems for clients:

1. **Noisy monitoring** - Batch health monitoring included transient errors like rate limits and network timeouts that would be retried automatically, making it difficult to identify batches with genuine problems requiring user attention.

2. **Premature alerts** - Clients couldn't distinguish between errors that indicated problems with their requests (400 Bad Request, validation errors) versus temporary infrastructure issues (429 Rate Limit, 503 Service Unavailable) that were still within the retry window.

3. **Confusing error streams** - When streaming error file contents before batch completion, clients received all failures including those that might still succeed on retry, creating confusion about which errors were actionable.

The SLA-aware error filtering feature addresses these issues by allowing clients to hide retriable errors that are still within their retry window (before the batch SLA expires).

## What Changed

### New hide_retriable_before_sla Parameter

Introduced a boolean `hide_retriable_before_sla` parameter across all batch query and streaming methods:

- **`false`** - Show all errors regardless of retryability or SLA status (complete picture)
- **`true`** - Hide retriable errors if the batch SLA hasn't expired yet (cleaner monitoring view)

The filtering is SLA-aware:
- **Before SLA expiry**: When `true`, retriable failures are hidden (they might still succeed on retry)
- **After SLA expiry**: All failures are shown regardless of the parameter value (retries have been exhausted)

This gives clients a cleaner view during batch execution while ensuring all errors become visible once the batch is complete.

### Breaking API Changes

All batch and file query methods now require an explicit `hide_retriable_before_sla` parameter:

- `get_batch(batch_id, hide_retriable_before_sla)`
- `get_batch_status(batch_id, hide_retriable_before_sla)`
- `list_batches(..., hide_retriable_before_sla)`
- `list_file_batches(file_id, hide_retriable_before_sla)`
- `get_file_content_stream(file_id, offset, search, hide_retriable_before_sla)`
- `get_batch_results_stream(batch_id, offset, search, status, hide_retriable_before_sla)`

This is intentionally a required parameter (not `Option<bool>`) to force clients to make an explicit choice about whether they want the filtered view.

### Data Model

The `Batch` and `BatchStatus` structs have a single `failed_requests` field whose meaning depends on the query parameters:

- When `hide_retriable_before_sla = false`: Total failures
- When `hide_retriable_before_sla = true` AND before SLA expiry: Only non-retriable failures
- When `hide_retriable_before_sla = true` AND after SLA expiry: Total failures (SLA expired, all errors now actionable)

### Database Changes

Added a new `is_retriable_error` boolean column to the `requests` table:
- Set when a request transitions to the failed state
- Computed from the existing `FailureReason::is_retriable()` method
- NULL values (for historical data) are treated as non-retriable (safe default)
- Indexed for efficient filtering: `idx_requests_failed_retriable`

No changes to the `batches` table - all filtering is performed dynamically at query time using SQL CASE expressions that check batch SLA status.

## Key Architectural Decisions

### 1. Boolean Parameter Instead of Enum

**Decision:** Use a simple boolean flag instead of introducing an `ErrorFilter` enum with multiple variants.

**Rationale:**
- Simpler API surface area - no new enum type to maintain
- The use case is specifically about hiding transient noise during monitoring
- The SLA-aware behavior naturally maps to a boolean (hide before SLA, show after SLA)
- Easier for clients to understand: "Should I hide retriable errors before SLA?" Yes/No

**Trade-off:** Less flexible than an enum, but covers the primary use case cleanly.

### 2. SLA-Aware Filtering

**Decision:** Make the filtering conditional on batch SLA status (expires_at timestamp).

**Rationale:**
- Retriable errors are only "noise" while they're still being retried
- Once the SLA expires, all errors are permanent and actionable
- Allows a single boolean to give sensible behavior across the batch lifecycle
- Prevents confusion where errors seem to "appear" - they're always shown after SLA expiry

**Implementation:** SQL queries use CASE expressions:
```sql
CASE WHEN b.expires_at > NOW() THEN
  COUNT(*) FILTER (WHERE state = 'failed' AND (is_retriable_error = false OR is_retriable_error IS NULL))
ELSE
  COUNT(*) FILTER (WHERE state = 'failed')
END
```

**Trade-off:** More complex SQL, but better user experience.

### 3. Required Parameter

**Decision:** The `hide_retriable_before_sla` parameter is required, not optional.

**Rationale:**
- Forces intentional choice - clients must decide what view they want
- Makes code self-documenting - looking at a call site reveals filtering intent
- Prevents "wrong default" bugs where code implicitly assumes one behavior but gets another

**Trade-off:** More verbose API, but improved clarity and correctness.

### 4. Dynamic SQL with QueryBuilder

**Decision:** Use sqlx's `QueryBuilder` for dynamic SQL construction instead of compile-time checked `query!()` macros.

**Rationale:**
- The SQL varies based on the hide_retriable_before_sla parameter and batch SLA status
- QueryBuilder provides SQL injection protection through parameterized binding
- Cannot use compile-time macros when query structure (CASE expressions) varies at runtime
- The CASE expressions need to reference batch table columns (b.expires_at) which aren't available in all contexts

**Trade-off:** Lose some compile-time checking, but gain necessary flexibility while maintaining safety.

### 5. On-Demand Counting

**Decision:** All error counts are computed dynamically using SQL `COUNT(*) FILTER` expressions rather than maintaining denormalized count columns.

**Rationale:**
- Aligns with the existing architecture (main branch removed denormalized counts)
- Single source of truth reduces complexity and eliminates potential consistency issues
- Proper indexing keeps query performance acceptable
- Makes it easy to add new filter dimensions in the future

**Trade-off:** Queries are slightly more complex, but this is offset by simpler data model and no trigger maintenance.

### 6. Intentional Terminal State Behavior

**Decision:** Terminal state detection uses the filtered `failed_requests` count, meaning batches may not be marked as terminal when `hide_retriable_before_sla = true` if there are still pending retriable failures within the SLA window.

**Rationale:**
- The filter is meant to affect the entire view of the batch, including completion status
- A batch with only retriable errors before SLA should not be considered "failed" yet
- Only non-retriable failures should trigger finalization before SLA expiry
- After SLA expiry, all failures count and the batch will finalize normally

This is intentional behavior: the filter affects both error visibility and batch completion semantics.

## Breaking Changes

This is a **major breaking change** requiring a version bump:

1. **All query methods require hide_retriable_before_sla parameter** - Every method that queries batch or file data now requires an explicit boolean argument.

2. **Terminal state semantics** - Batches with only retriable failures may not enter terminal states (finalizing/completed/failed) until after SLA expiry when queried with `hide_retriable_before_sla = true`.

## Migration Path

Clients need to update all batch and file query calls to pass the boolean flag:

```rust
// Before:
let batch = manager.get_batch(batch_id).await?;
let status = manager.get_batch_status(batch_id).await?;

// After - preserve previous behavior (show all errors):
let batch = manager.get_batch(batch_id, false).await?;
let status = manager.get_batch_status(batch_id, false).await?;

// Or hide transient errors for cleaner monitoring:
let batch = manager.get_batch(batch_id, true).await?;

// Note: After SLA expiry, both return the same result (all errors visible)
```

## Use Cases

This feature enables several important workflows:

**Clean monitoring dashboards** - Query with `hide_retriable_before_sla = true` to show only actionable errors during batch execution, reducing noise from rate limits and network timeouts that will resolve on retry.

**Error analysis** - Stream error file contents with `hide_retriable_before_sla = true` to focus on errors that indicate problems with request data or configuration, ignoring transient infrastructure issues.

**Complete error picture** - Use `hide_retriable_before_sla = false` when you need to see everything, regardless of retryability or SLA status.

**Time-aware views** - The same query automatically adapts as the batch ages - transient errors are hidden during execution but become visible once the SLA expires.

## Future Enhancements

Potential future additions not included in this implementation:

- Filter by specific `FailureReason` variants (e.g., only 429 rate limits)
- Separate counts for retriable vs non-retriable errors in Batch/BatchStatus structs
- Time-series tracking of retriable vs non-retriable failure rates
