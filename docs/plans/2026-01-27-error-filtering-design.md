# Error Filtering Design

**Date:** 2026-01-27
**Status:** ✅ Completed

## Overview

Add support for filtering batch and file error counts/streams by error retryability. This allows clients to distinguish between temporary failures (retriable) and permanent failures (non-retriable).

## Motivation

Currently, all failed requests are counted together in a single `failed_requests` count. This makes it difficult for clients to:
- Understand what portion of failures are actionable (non-retriable) vs temporary (retriable)
- Filter error file contents to show only permanent failures
- Monitor batch health without noise from transient errors

## Design

### 1. ErrorFilter Enum

New client-facing enum for specifying which error types to include:

```rust
pub enum ErrorFilter {
    All,                  // Include all failures (default)
    OnlyRetriable,        // Only retriable failures (429, 503, network errors)
    OnlyNonRetriable,     // Only non-retriable failures (400, 404, validation errors)
}
```

**Key Decision:** Required parameter (not `Option<ErrorFilter>`) to force intentional choice.

### 2. Database Schema Changes

Migration `20260127000000_add_error_filtering_support.up.sql`:

**Requests table:**
- Add `is_retriable_error BOOLEAN NULL` - Set when request enters failed state
- Computed in Rust using existing `FailureReason::is_retriable()` method
- Indexed for fast filtering: `CREATE INDEX idx_requests_failed_retriable ON requests(batch_id, is_retriable_error) WHERE state = 'failed'`

**Batches table:**
- Add `failed_requests_retriable BIGINT NOT NULL DEFAULT 0`
- Add `failed_requests_non_retriable BIGINT NOT NULL DEFAULT 0`
- Keep existing `failed_requests` as total (may be sum or filtered based on ErrorFilter)
- Maintained by updated trigger `update_batch_on_request_change()`

**Trigger Logic:**
- When request transitions to failed state, check `is_retriable_error`
- Increment appropriate counter (`failed_requests_retriable` or `failed_requests_non_retriable`)
- Handle INSERT, UPDATE (state transition), and DELETE

### 3. API Changes

**Breaking changes - all methods now require `error_filter` parameter:**

```rust
// Storage trait methods updated:
async fn get_batch(&self, batch_id: BatchId, error_filter: ErrorFilter) -> Result<Batch>;
async fn get_batch_status(&self, batch_id: BatchId, error_filter: ErrorFilter) -> Result<BatchStatus>;
async fn list_file_batches(&self, file_id: FileId, error_filter: ErrorFilter) -> Result<Vec<BatchStatus>>;
async fn list_batches(&self, ..., error_filter: ErrorFilter) -> Result<Vec<Batch>>;

fn get_file_content_stream(&self, file_id: FileId, offset: usize, search: Option<String>,
                           error_filter: ErrorFilter) -> Pin<Box<dyn Stream<...>>>;

fn get_batch_results_stream(&self, batch_id: BatchId, offset: usize, search: Option<String>,
                            status: Option<String>, error_filter: ErrorFilter) -> Pin<Box<dyn Stream<...>>>;
```

**Struct Updates:**
```rust
pub struct Batch {
    // ... existing fields ...
    pub failed_requests: i64,                    // Filtered based on ErrorFilter
    pub failed_requests_retriable: i64,          // Always present (full count)
    pub failed_requests_non_retriable: i64,      // Always present (full count)
    // ... rest of fields ...
}

pub struct BatchStatus {
    // Same fields as Batch for consistency
    pub failed_requests: i64,
    pub failed_requests_retriable: i64,
    pub failed_requests_non_retriable: i64,
    // ...
}
```

### 4. Query Implementation

**Approach for get_batch_from_pool:**
- Use dynamic SQL with string formatting for the filter clause
- Switch between different `COUNT(*) FILTER` expressions based on ErrorFilter
- Read denormalized counts from batches table for retriable/non-retriable
- Compute filtered `failed_requests` count dynamically from requests table

**Error Filter Logic:**
```sql
-- ErrorFilter::All
COUNT(*) FILTER (WHERE state = 'failed') as failed

-- ErrorFilter::OnlyRetriable
COUNT(*) FILTER (WHERE state = 'failed' AND is_retriable_error = true) as failed

-- ErrorFilter::OnlyNonRetriable
COUNT(*) FILTER (WHERE state = 'failed' AND (is_retriable_error = false OR is_retriable_error IS NULL)) as failed
```

**Note:** NULL is treated as non-retriable (defensive default for data migration).

### 5. Request Persistence

**Updated `persist` method for `Failed` state:**
```rust
AnyRequest::Failed(req) => {
    let error_json = serde_json::to_string(&req.state.reason)?;
    let is_retriable_error = req.state.reason.is_retriable();  // Compute in Rust

    sqlx::query!(
        r#"UPDATE requests SET
           state = 'failed',
           retry_attempt = $2,
           error = $3,
           failed_at = $4,
           response_size = $5,
           is_retriable_error = $6  -- New field
           WHERE id = $1 AND superseded_at IS NULL"#,
        *req.data.id as Uuid,
        req.state.retry_attempt as i32,
        error_json,
        req.state.failed_at,
        response_size,
        is_retriable_error  // Set from Rust computation
    ).execute(self.pools.write()).await?;
}
```

## Implementation Status

### Completed
- ✅ ErrorFilter enum added to `batch/mod.rs`
- ✅ Batch and BatchStatus structs updated with new count fields
- ✅ Migration files created (up and down)
- ✅ Storage trait updated with error_filter parameters
- ✅ Request persistence updated to set is_retriable_error
- ✅ `get_batch` and `get_batch_from_pool` updated
- ✅ `batch_from_row` macro updated

### Completed Work
- ✅ Update `get_batch_status` implementation
- ✅ Update `list_file_batches` implementation
- ✅ Update `list_batches` implementation
- ✅ Update `get_file_content_stream` implementation
- ✅ Update `get_batch_results_stream` implementation
- ✅ Run migration on test database
- ✅ Update all calling code to pass ErrorFilter
- ✅ Run tests and fix any issues
- ✅ Add comprehensive tests for error filtering (11 new tests)

### Optional Remaining Work
- ⏳ Update CHANGELOG.md

## Testing Plan

1. **Migration Testing:**
   - Run migration on test database with existing data
   - Verify backfill correctly sets is_retriable_error
   - Verify trigger maintains counts correctly

2. **Unit Tests:**
   - Test ErrorFilter::All returns all failures
   - Test ErrorFilter::OnlyRetriable filters correctly
   - Test ErrorFilter::OnlyNonRetriable filters correctly
   - Test counts match filtered streams

3. **Integration Tests:**
   - Create batch with mix of retriable/non-retriable failures
   - Query with different filters, verify counts match
   - Test error file streaming with filters
   - Test batch results streaming with filters

## Breaking Changes

This is a **major breaking change** (requires version bump):
- All batch/file query methods now require `error_filter` parameter
- Client code must be updated to pass explicit filter choice
- Default behavior (ErrorFilter::All) preserves semantics but requires code changes

## Migration Path for Clients

```rust
// Before:
let batch = manager.get_batch(batch_id).await?;

// After (explicit choice required):
let batch = manager.get_batch(batch_id, ErrorFilter::All).await?;

// Or filter to only actionable errors:
let batch = manager.get_batch(batch_id, ErrorFilter::OnlyNonRetriable).await?;
```

## Future Enhancements

Potential future additions (not in this PR):
- Add `FailureReason` as a filter (filter by specific error types)
- Add separate `retried_requests` count (failures that were retried successfully)
- Historical tracking of retriable vs non-retriable failure rates
