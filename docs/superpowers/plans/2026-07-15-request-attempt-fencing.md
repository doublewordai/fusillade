# Request Attempt Fencing Implementation Plan

> Implement test-first, keeping each schema and lifecycle change independently
> reviewable.

**Goal:** Prevent stale or reclaimed request executions from starting or
overwriting a newer execution while retaining dead-daemon recovery.

**Architecture:** PostgreSQL assigns a UUID attempt token atomically during
claim. The core typestate carries it through claimed and processing states.
Daemon-owned storage operations compare-and-set on request ID, expected state,
and attempt ID. Reclaim is based only on owner liveness and revokes the token.

**Tech stack:** Rust, Tokio, SQLx, PostgreSQL.

---

## Task 1: Add the attempt identity to schema and domain types

**Files:**

- Add: `crates/fusillade-arsenal/migrations/20260715000000_add_request_attempt_id.up.sql`
- Add: `crates/fusillade-arsenal/migrations/20260715000000_add_request_attempt_id.down.sql`
- Modify: `crates/fusillade-core/src/request/types.rs`
- Modify: `crates/fusillade-arsenal/src/postgres.rs`

1. Add a database test asserting a claim returns the same non-null attempt ID
   stored on the row.
2. Run the focused test and confirm it fails.
3. Add `AttemptId`, add it to `Claimed` and `Processing`, and generate/return it
   in both batchless and batched claim SQL.
4. Apply the migration, update request reconstruction, and rerun the test.

## Task 2: Add fenced persistence operations

**Files:**

- Modify: `crates/fusillade-core/src/manager.rs`
- Modify: `crates/fusillade-arsenal/src/postgres.rs`
- Modify: `crates/fusillade-core/src/request/transitions.rs`
- Modify: `src/daemon/mod.rs`

1. Add tests that an old attempt cannot begin processing, complete, fail,
   cancel, or reschedule after a newer attempt is installed.
2. Run the focused tests and confirm they fail under ID-only/daemon-only
   persistence.
3. Add a `persist_attempt` storage operation returning a compare-and-set result.
4. Implement state-specific SQL predicates and ownership-loss metrics.
5. Pass the attempt ID into retry rescheduling and terminal persistence.
6. Rerun focused tests.

## Task 3: Gate upstream execution on processing persistence

**Files:**

- Modify: `crates/fusillade-core/src/request/transitions.rs`
- Modify: `tests/request_processor.rs`

1. Add a processor test whose response future records when first polled and
   whose storage processing write is deliberately blocked or rejected.
2. Confirm the test demonstrates that the future must remain unpolled.
3. Gate the spawned response task until the fenced processing write succeeds.
4. Abort/drop the gated task on database error or ownership loss.
5. Rerun processor and core tests.

## Task 4: Make reclaim owner-liveness-only

**Files:**

- Modify: `crates/fusillade-arsenal/src/postgres.rs`
- Modify: `crates/fusillade-arsenal/src/lib.rs`
- Modify: `src/daemon/config.rs`

1. Change the healthy-daemon age tests to assert no reclaim even beyond the old
   claim and processing timeouts.
2. Add assertions that dead/stale-owner reclaim clears `attempt_id`.
3. Confirm the updated tests fail against the current time-based fallback.
4. Remove the age-only SQL branch and raise both public defaults to 300,000 ms.
5. Rerun all stale-reclaim tests.

## Task 5: Verify and publish

**Files:**

- Update: `.sqlx/*` generated query metadata
- Update: `Cargo.lock` only if dependency resolution requires it

1. Run `cargo fmt --check`.
2. Run `cargo clippy --workspace --all-targets -- -D warnings`.
3. Run `DATABASE_URL=... cargo test --workspace`.
4. Run `cd crates/fusillade-arsenal && DATABASE_URL=... cargo sqlx prepare --check`.
5. Review `git diff`, `git diff --check`, and migration rollback.
6. Commit with a conventional `fix:` subject, push `agent/fence-request-attempts`,
   and open a PR describing guarantees, rollout caveat, and at-least-once
   failover semantics.

