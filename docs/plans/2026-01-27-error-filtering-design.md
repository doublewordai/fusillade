# Error Filtering Implementation

**Date:** 2026-01-27
**Status:** ✅ Completed

## Overview

This document describes the error filtering feature added to Fusillade, which allows clients to distinguish between temporary failures (retriable errors like rate limits) and permanent failures (non-retriable errors like validation failures) when querying batch status and streaming error data.

## Motivation

Previously, all failed requests were aggregated into a single `failed_requests` count. This created several problems for clients:

1. **Lack of actionability** - Clients couldn't distinguish between errors that indicated problems with their requests (400 Bad Request, validation errors) versus temporary infrastructure issues (429 Rate Limit, 503 Service Unavailable).

2. **Noisy monitoring** - Batch health monitoring included transient errors that would resolve on retry, making it difficult to identify batches with genuine problems.

3. **Limited error analysis** - When streaming error file contents, clients received all failures without the ability to filter to only the errors requiring their attention.

The error filtering feature addresses these issues by allowing clients to explicitly choose which types of failures they want to see in queries and streams.

## What Changed

### New ErrorFilter Type

Introduced a public `ErrorFilter` enum for specifying which error types to include:

- **`All`** - Include all failures regardless of retryability (default behavior)
- **`OnlyRetriable`** - Only retriable failures (rate limits, service errors, network timeouts)
- **`OnlyNonRetriable`** - Only non-retriable failures (bad requests, validation errors, auth failures)

### Breaking API Changes

All batch and file query methods now require an explicit `error_filter` parameter:

- `get_batch(batch_id, error_filter)`
- `get_batch_status(batch_id, error_filter)`
- `list_batches(..., error_filter)`
- `list_file_batches(file_id, error_filter)`
- `get_file_content_stream(file_id, offset, search, error_filter)`
- `get_batch_results_stream(batch_id, offset, search, status, error_filter)`

This is intentionally a required parameter (not `Option<ErrorFilter>`) to force clients to make an explicit choice about which errors they want to see.

### Simplified Data Model

During implementation, we identified and removed redundant fields that were creating confusion. The `Batch` and `BatchStatus` structs now use a simpler model:

- `failed_requests` - The count of failed requests, filtered based on the `ErrorFilter` parameter passed to the query method
- No separate `failed_requests_retriable` or `failed_requests_non_retriable` fields

The meaning of `failed_requests` changes based on the filter:
- With `ErrorFilter::All`: total failures
- With `ErrorFilter::OnlyRetriable`: only retriable failures
- With `ErrorFilter::OnlyNonRetriable`: only non-retriable failures

This design eliminates redundancy and makes the API more intuitive - the filter controls what you see, and the counts reflect that filter.

### Database Changes

Added a new `is_retriable_error` boolean column to the `requests` table:
- Set when a request transitions to the failed state
- Computed from the existing `FailureReason::is_retriable()` method
- NULL values (for historical data) are treated as non-retriable (safe default)
- Indexed for efficient filtering: `idx_requests_failed_retriable`

No changes to the `batches` table - all filtering is performed dynamically at query time.

## Key Architectural Decisions

### 1. On-Demand Counting

**Decision:** All error counts are computed dynamically using SQL `COUNT(*) FILTER` expressions rather than maintaining denormalized count columns.

**Rationale:**
- Aligns with the existing architecture (main branch removed denormalized counts)
- Single source of truth reduces complexity and eliminates potential consistency issues
- Proper indexing keeps query performance acceptable
- Makes it easy to add new filter dimensions in the future

**Trade-off:** Queries are slightly more complex, but this is offset by simpler data model and no trigger maintenance.

### 2. Required Parameter

**Decision:** The `error_filter` parameter is required, not optional.

**Rationale:**
- Forces intentional choice - clients must decide what errors they care about
- Makes code self-documenting - looking at a call site reveals filtering intent
- Prevents "wrong default" bugs where code implicitly assumes one filter but gets another

**Trade-off:** More verbose API, but improved clarity and correctness.

### 3. Dynamic SQL with QueryBuilder

**Decision:** Use sqlx's `QueryBuilder` for dynamic SQL construction instead of compile-time checked `query!()` macros.

**Rationale:**
- The SQL varies based on the filter parameter (different COUNT expressions)
- QueryBuilder provides SQL injection protection through parameterized binding
- Cannot use compile-time macros when query structure varies at runtime

**Trade-off:** Lose some compile-time checking, but gain necessary flexibility while maintaining safety.

### 4. Simplified Design

**Decision:** During code review, we identified and removed the `failed_requests_retriable` and `failed_requests_non_retriable` fields that existed in an earlier iteration.

**Rationale:**
- These fields were redundant when combined with the `error_filter` parameter
- Created confusion about what they represented (always total counts? filtered counts?)
- The filter parameter already controls what the caller wants to see
- Simpler model: one count field whose meaning is determined by the filter

This decision came from recognizing that returning both filtered and unfiltered counts was overcomplicating the API without adding value.

## Breaking Changes

This is a **major breaking change** requiring a version bump:

1. **All query methods require error_filter parameter** - Every method that queries batch or file data now requires an explicit `ErrorFilter` argument.

2. **Batch/BatchStatus field removal** - The `failed_requests_retriable` and `failed_requests_non_retriable` fields no longer exist. Use the `error_filter` parameter to control what `failed_requests` represents.

## Migration Path

Clients need to update all batch and file query calls to pass an explicit filter:

```rust
// Before:
let batch = manager.get_batch(batch_id).await?;
let status = manager.get_batch_status(batch_id).await?;

// After - preserve previous behavior:
let batch = manager.get_batch(batch_id, ErrorFilter::All).await?;
let status = manager.get_batch_status(batch_id, ErrorFilter::All).await?;

// Or filter to only actionable errors:
let batch = manager.get_batch(batch_id, ErrorFilter::OnlyNonRetriable).await?;

// Or only transient failures:
let batch = manager.get_batch(batch_id, ErrorFilter::OnlyRetriable).await?;
```

Clients should also remove any code that references the deleted `failed_requests_retriable` or `failed_requests_non_retriable` fields.

## Use Cases

This feature enables several important workflows:

**Monitoring batch health** - Query with `ErrorFilter::OnlyNonRetriable` to see if there are any permanent failures requiring attention, filtering out transient rate limits and network errors.

**Error analysis** - Stream error file contents with `ErrorFilter::OnlyNonRetriable` to focus on errors that indicate problems with request data or configuration.

**Retry decision making** - Query with `ErrorFilter::OnlyRetriable` to understand how many failures are transient and may resolve on retry.

**Complete error picture** - Use `ErrorFilter::All` (the previous behavior) when you need total failure counts regardless of retryability.

## Future Enhancements

Potential future additions not included in this implementation:

- Filter by specific `FailureReason` variants (e.g., only 429 rate limits)
- Separate `retried_requests` count (failures that succeeded on retry)
- Time-series tracking of retriable vs non-retriable failure rates
