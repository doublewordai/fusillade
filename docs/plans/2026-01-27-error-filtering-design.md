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

Migration `20260130000001_add_error_filtering_support.up.sql`:

**Key Architectural Decision: On-Demand Counting (No Denormalized Columns)**

After merging with main (which removed denormalized counts and triggers in migration `20250118000000`), we adopted the on-demand counting approach:

**Requests table:**
- Add `is_retriable_error BOOLEAN NULL` - Set when request enters failed state
- Computed in Rust using existing `FailureReason::is_retriable()` method
- Indexed for fast filtering: `CREATE INDEX idx_requests_failed_retriable ON requests(batch_id, is_retriable_error) WHERE state = 'failed'`

**No changes to batches table:**
- No `failed_requests_retriable` or `failed_requests_non_retriable` columns
- All counts computed dynamically using `COUNT(*) FILTER` in SQL queries
- This follows the pattern established by main for all request state counts

**No triggers:**
- Eliminated complexity and race conditions from denormalized counts
- Simpler mental model: single source of truth (requests table)
- Performance remains acceptable with proper indexing

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

**Key Decision: Dynamic SQL with format!()**

Since we're using on-demand counting, all queries that accept `error_filter` must use dynamic SQL to inject the appropriate COUNT expressions. We cannot use `sqlx::query!()` macros because the SQL varies based on the filter parameter.

**Helper Function:**
```rust
fn error_filter_sql_fragments(
    filter: crate::batch::ErrorFilter,
) -> (&'static str, &'static str, &'static str, &'static str) {
    // These are always the same regardless of filter
    const FAILED_RETRIABLE: &str =
        "COUNT(*) FILTER (WHERE state = 'failed' AND is_retriable_error = true)";
    const FAILED_NON_RETRIABLE: &str =
        "COUNT(*) FILTER (WHERE state = 'failed' AND (is_retriable_error = false OR is_retriable_error IS NULL))";

    let (where_clause, failed_count) = match filter {
        ErrorFilter::All => (
            "",
            "COUNT(*) FILTER (WHERE state = 'failed')",
        ),
        ErrorFilter::OnlyRetriable => (
            "AND is_retriable_error = true",
            "COUNT(*) FILTER (WHERE state = 'failed' AND is_retriable_error = true)",
        ),
        ErrorFilter::OnlyNonRetriable => (
            "AND (is_retriable_error = false OR is_retriable_error IS NULL)",
            "COUNT(*) FILTER (WHERE state = 'failed' AND (is_retriable_error = false OR is_retriable_error IS NULL))",
        ),
    };

    (where_clause, failed_count, FAILED_RETRIABLE, FAILED_NON_RETRIABLE)
}
```

**Query Pattern:**
```rust
let (_, failed_count, failed_retriable, failed_non_retriable) =
    Self::error_filter_sql_fragments(error_filter);

let query = format!(
    r#"
    SELECT
        b.id, b.file_id, ...,
        COALESCE(counts.failed, 0)::BIGINT as failed_requests,
        COALESCE(counts.failed_retriable, 0)::BIGINT as failed_requests_retriable,
        COALESCE(counts.failed_non_retriable, 0)::BIGINT as failed_requests_non_retriable,
        ...
    FROM batches b
    LEFT JOIN LATERAL (
        SELECT
            {} as failed,
            {} as failed_retriable,
            {} as failed_non_retriable,
            ...
        FROM requests
        WHERE batch_id = b.id
    ) counts ON TRUE
    WHERE b.id = $1
    "#,
    failed_count, failed_retriable, failed_non_retriable
);

let row = sqlx::query(&query)
    .bind(*batch_id as Uuid)
    .fetch_optional(pool)
    .await?;
```

**Safety:** All SQL fragments are compile-time string constants from match arms, preventing SQL injection.

**Note:** NULL is treated as non-retriable (defensive default for historical data).

### 5. Request Persistence

**Updated `persist` method for `Failed` state:**
```rust
AnyRequest::Failed(req) => {
    let error_json = serde_json::to_string(&req.state.reason)?;
    let response_size = calculate_error_message_size(&error_json)?;

    // Determine if this is a retriable error (computed in Rust)
    let is_retriable_error = req.state.reason.is_retriable();

    sqlx::query!(
        r#"UPDATE requests SET
           state = 'failed',
           retry_attempt = $2,
           error = $3,
           failed_at = $4,
           response_size = $5,
           routed_model = $6,
           is_retriable_error = $7  -- New field
           WHERE id = $1"#,
        *req.data.id as Uuid,
        req.state.retry_attempt as i32,
        error_json,
        req.state.failed_at,
        response_size,
        req.state.routed_model,
        is_retriable_error  // Set from Rust computation
    ).execute(self.pools.write()).await?;
}
```

**Key point:** The `is_retriable_error` field is set once when the request transitions to failed state, using the existing `FailureReason::is_retriable()` method.

## Implementation Details: Merge with Main

### Context

During implementation, main branch removed the Superseded/escalation feature and switched to on-demand counting (migration `20250118000000_remove_wal_use_on_demand_counting`). This required merging our error filtering feature with a significantly different codebase.

### Strategy

Instead of attempting to fix merge conflicts surgically, we took main as the base and re-applied only the error filtering changes:

1. **Started fresh:** `git checkout origin/main -- src/manager/postgres.rs`
2. **Re-applied changes systematically** following the updated architecture
3. **Adopted on-demand counting** instead of denormalized columns with triggers

### Row Mapping Macros

Created three macros for simplified row extraction:

**batch_from_row!** - For typed sqlx::query!() results:
```rust
macro_rules! batch_from_row {
    ($row:expr) => {
        Batch {
            id: BatchId($row.id),
            // ... direct field access ...
            failed_requests_retriable: $row.failed_requests_retriable,
            failed_requests_non_retriable: $row.failed_requests_non_retriable,
        }
    };
}
```

**batch_from_dynamic_row!** - For dynamic sqlx::query() results:
```rust
macro_rules! batch_from_dynamic_row {
    ($row:expr) => {
        Batch {
            id: BatchId($row.get("id")),
            // ... .get() access ...
            failed_requests_retriable: $row.get("failed_requests_retriable"),
            failed_requests_non_retriable: $row.get("failed_requests_non_retriable"),
        }
    };
}
```

**batch_status_from_dynamic_row!** - For BatchStatus from dynamic queries:
```rust
macro_rules! batch_status_from_dynamic_row {
    ($row:expr) => {
        BatchStatus {
            batch_id: BatchId($row.get("batch_id")),
            // ... all BatchStatus fields ...
            failed_requests_retriable: $row.get("failed_requests_retriable"),
            failed_requests_non_retriable: $row.get("failed_requests_non_retriable"),
        }
    };
}
```

### Methods Converted to Dynamic SQL

All methods accepting `error_filter` were converted from `sqlx::query!()` to `sqlx::query()` with `format!()`:

1. **get_batch_status()** - Computes filtered counts on-demand
2. **list_batches()** - Handles pagination with filtered counts
3. **list_file_batches()** - Returns BatchStatus with filtered counts
4. **get_batch()** and **get_batch_from_pool()** - Full batch data with counts
5. **get_batch_by_output_file_id()** - Both Output and Error file cases
6. **get_batch_results_stream()** and **stream_batch_results()** - Streaming with filters
7. **get_file_content_stream()** and **stream_batch_error()** - Error file streaming with filters

### Test Updates

Used sed for bulk updates of test calls:
```bash
sed -i '' 's/\.get_batch_status(\(batch\.id\))/.get_batch_status(\1, crate::batch::ErrorFilter::All)/g'
sed -i '' 's/\.get_batch(\([^,)]*\))/.get_batch(\1, crate::batch::ErrorFilter::All)/g'
sed -i '' 's/\.list_file_batches(\([^)]*\))/.list_file_batches(\1, crate::batch::ErrorFilter::All)/g'
sed -i '' 's/\.get_file_content_stream(\([^,]*\), 0, None)/.get_file_content_stream(\1, 0, None, crate::batch::ErrorFilter::All)/g'
```

## Implementation Status

### ✅ All Work Completed (2026-01-30)

**Database Schema:**
- ✅ Migration `20260130000001_add_error_filtering_support` created (up and down)
- ✅ Added `is_retriable_error BOOLEAN NULL` to requests table
- ✅ Created index `idx_requests_failed_retriable` for fast filtering
- ✅ Ran migration on test database
- ✅ Regenerated .sqlx metadata files

**Core Implementation:**
- ✅ ErrorFilter enum added to `batch/mod.rs`
- ✅ Batch and BatchStatus structs updated with new count fields
- ✅ Storage trait updated with error_filter parameters
- ✅ Created `error_filter_sql_fragments()` helper function
- ✅ Created row mapping macros (batch_from_dynamic_row!, batch_status_from_dynamic_row!)
- ✅ Request persistence updated to set is_retriable_error in persist() method

**Query Methods (All converted to dynamic SQL):**
- ✅ `get_batch()` and `get_batch_from_pool()`
- ✅ `get_batch_status()`
- ✅ `list_batches()`
- ✅ `list_file_batches()`
- ✅ `get_batch_by_output_file_id()` (both Output and Error cases)
- ✅ `get_batch_results_stream()` and `stream_batch_results()`
- ✅ `get_file_content_stream()` and `stream_batch_error()`
- ✅ Updated all test calls to pass ErrorFilter::All

**Testing:**
- ✅ All 73 lib tests passing
- ✅ All 11 error filtering integration tests passing
- ✅ Tests cover:
  - Error filtering in batch status queries
  - Error filtering in batch list queries
  - Error filtering in file batch list queries
  - Error filtering in result streams
  - Error filtering in file content streams
  - NULL handling for is_retriable_error

**Documentation:**
- ✅ Implementation plan updated with merge details

## Testing Results

### Migration Testing
- ✅ Migration `20260130000001` applied successfully to test database
- ✅ Index `idx_requests_failed_retriable` created successfully
- ✅ All historical requests have `is_retriable_error = NULL` (treated as non-retriable)
- ✅ New failures correctly set `is_retriable_error` based on `FailureReason::is_retriable()`

### Integration Tests (tests/error_filtering.rs)

**Test Suite:** 11 tests, all passing

1. **test_get_batch_status_error_filter_all** ✅
   - Verifies ErrorFilter::All returns total count of 4 failures
   - Verifies retriable breakdown: 2 retriable, 2 non-retriable

2. **test_get_batch_status_error_filter_only_retriable** ✅
   - Verifies ErrorFilter::OnlyRetriable returns failed_requests = 2
   - Verifies breakdown fields still show full counts

3. **test_get_batch_status_error_filter_only_non_retriable** ✅
   - Verifies ErrorFilter::OnlyNonRetriable returns failed_requests = 2
   - Verifies breakdown fields still show full counts

4. **test_get_batch_error_filter_all** ✅
   - Tests full Batch object (not just status)
   - Verifies all count fields populated correctly

5. **test_get_batch_error_filter_only_retriable** ✅
   - Tests filtered Batch with only retriable failures counted

6. **test_get_batch_error_filter_only_non_retriable** ✅
   - Tests filtered Batch with only non-retriable failures counted

7. **test_list_batches_error_filter** ✅
   - Verifies list_batches() respects error_filter parameter
   - Tests filtering across multiple batches

8. **test_list_file_batches_error_filter** ✅
   - Verifies list_file_batches() respects error_filter parameter
   - Tests BatchStatus returned with correct filtered counts

9. **test_get_batch_results_stream_error_filter** ✅
   - Verifies result stream filtering by status="failed" AND error_filter
   - Tests that ErrorFilter::OnlyRetriable excludes non-retriable failures
   - Verifies completed results unaffected by error filter

10. **test_get_file_content_stream_error_filter** ✅
    - Verifies error file streaming with ErrorFilter
    - Tests All (4 items), OnlyRetriable (2 items), OnlyNonRetriable (2 items)

11. **test_error_filter_with_null_is_retriable_error** ✅
    - Verifies NULL is_retriable_error treated as non-retriable
    - Ensures backward compatibility with historical data

### Unit Tests (src/manager/postgres.rs)

All 73 existing lib tests continue to pass:
- ✅ File creation and management
- ✅ Batch creation and status queries
- ✅ Request lifecycle (claim, process, complete, fail)
- ✅ Daemon registration and heartbeats
- ✅ SLA-based prioritization
- ✅ Pagination and streaming
- ✅ Virtual file finalization
- ✅ Cancellation and retry logic

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

## Key Architectural Decisions

### 1. On-Demand Counting vs Denormalized Columns

**Decision:** Compute all error counts on-demand using `COUNT(*) FILTER` in SQL queries.

**Rationale:**
- Main branch removed denormalized counts and triggers to eliminate complexity
- On-demand counting provides single source of truth (requests table)
- Proper indexing (`idx_requests_failed_retriable`) keeps performance acceptable
- Eliminates potential race conditions from trigger-based updates

**Trade-offs:**
- ✅ Simpler mental model and debugging
- ✅ No trigger maintenance or race conditions
- ✅ Easier to add new filter dimensions in the future
- ⚠️ Slightly more complex queries (multiple COUNT expressions)
- ⚠️ Query performance dependent on index effectiveness

### 2. Dynamic SQL with format!()

**Decision:** Use `sqlx::query()` with `format!()` for all error-filtered queries.

**Rationale:**
- Cannot use `sqlx::query!()` macros when SQL varies based on parameters
- Need different COUNT expressions based on ErrorFilter variant
- All SQL fragments are compile-time constants, preventing injection

**Implementation:**
```rust
// Helper returns static string fragments only
fn error_filter_sql_fragments(filter: ErrorFilter)
    -> (&'static str, &'static str, &'static str, &'static str)

// Build query with format!()
let query = format!(
    "SELECT ... {} as failed, {} as failed_retriable ...",
    failed_count, failed_retriable
);

// Use sqlx::query() for runtime SQL
sqlx::query(&query).bind(param).fetch_all().await
```

**Trade-offs:**
- ✅ Type-safe fragments (static strings only)
- ✅ Flexible query building
- ⚠️ Lose compile-time SQL validation from sqlx::query!()
- ⚠️ Must manually extract values with .get() instead of typed fields
- ⚠️ Need careful testing to catch SQL errors

### 3. Row Mapping Macros

**Decision:** Created three macros for different query types.

**Rationale:**
- Reduce repetition across 8+ methods
- Centralize field extraction logic
- Macros adapt to typed vs dynamic queries

**Types:**
- `batch_from_row!` - For `sqlx::query!()` results (typed fields)
- `batch_from_dynamic_row!` - For `sqlx::query()` results (manual .get())
- `batch_status_from_dynamic_row!` - For BatchStatus from dynamic queries

### 4. Required error_filter Parameter

**Decision:** Make `error_filter` a required parameter (not `Option<ErrorFilter>`).

**Rationale:**
- Forces intentional choice about which errors to include
- Prevents accidental "wrong default" bugs
- Makes client intent explicit in code

**Migration path:**
```rust
// Old: implicit behavior
get_batch(id)

// New: explicit choice required
get_batch(id, ErrorFilter::All)  // or OnlyRetriable/OnlyNonRetriable
```

## Future Enhancements

Potential future additions (not in this PR):
- Add `FailureReason` as a filter (filter by specific error types)
- Add separate `retried_requests` count (failures that were retried successfully)
- Historical tracking of retriable vs non-retriable failure rates
