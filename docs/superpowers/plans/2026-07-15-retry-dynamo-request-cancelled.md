# Retry Dynamo Request-Cancelled Responses Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make only Dynamo-shaped `499 request_cancelled` responses eligible for Fusillade's existing bounded retry flow.

**Architecture:** Extend the default response predicate with a private JSON-envelope classifier. Keep the existing predicate signature and preserve Fusillade's separate user-cancellation and shutdown branches.

**Tech Stack:** Rust 2024, serde_json, Cargo tests, Clippy

## Global Constraints

- Require effective response status `499`, numeric `error.code` `499`, and exact `error.type` `"request_cancelled"`.
- Keep generic 499 responses non-retriable.
- Do not change status codes, persisted bodies, public predicate signatures, cancellation APIs, retry limits, backoff, or deadline enforcement.
- Do not infer retryability from `error.message`.

---

### Task 1: Classify Dynamo request cancellations

**Files:**
- Modify: `src/daemon/config.rs`
- Test: `src/daemon/config.rs`

**Interfaces:**
- Consumes: `crate::http::HttpResponse { status: u16, body: String }`
- Produces: unchanged `pub fn default_should_retry(response: &HttpResponse) -> bool`

- [ ] **Step 1: Add regression and boundary tests**

Append this test module to `src/daemon/config.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn response(status: u16, body: &str) -> HttpResponse {
        HttpResponse {
            status,
            body: body.to_string(),
        }
    }

    #[test]
    fn retries_dynamo_request_cancelled_499() {
        let response = response(
            499,
            r#"{"error":{"code":499,"message":"CancelledError: ","type":"request_cancelled"}}"#,
        );

        assert!(default_should_retry(&response));
    }

    #[test]
    fn does_not_retry_other_499_responses() {
        let bodies = [
            r#"{"error":{"code":499,"type":"other"}}"#,
            r#"{"error":{"code":500,"type":"request_cancelled"}}"#,
            r#"{"error":{"code":"499","type":"request_cancelled"}}"#,
            r#"{"error":{"type":"request_cancelled"}}"#,
            r#"{"error":{"code":499}}"#,
            r#"{"error":{"code":499,"message":"CancelledError: "}}"#,
            r#"{"error":"request_cancelled"}"#,
            "not json",
            "",
        ];

        for body in bodies {
            assert!(
                !default_should_retry(&response(499, body)),
                "unexpected retry for body: {body}"
            );
        }
    }

    #[test]
    fn request_cancelled_body_does_not_override_other_statuses() {
        let body = r#"{"error":{"code":499,"type":"request_cancelled"}}"#;

        assert!(!default_should_retry(&response(400, body)));
    }

    #[test]
    fn preserves_existing_default_retry_statuses() {
        for status in [404, 408, 429, 500, 503] {
            assert!(default_should_retry(&response(status, "")));
        }

        for status in [200, 400, 401, 403, 422, 498, 499] {
            assert!(!default_should_retry(&response(status, "")));
        }
    }
}
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
cargo test -p fusillade --lib daemon::config::tests
```

Expected: `retries_dynamo_request_cancelled_499` fails because status 499 is not currently retried; all boundary tests pass.

- [ ] **Step 3: Add the minimal private classifier**

Add this helper above `default_should_retry` and include it in the predicate:

```rust
fn is_dynamo_request_cancelled(response: &HttpResponse) -> bool {
    if response.status != 499 {
        return false;
    }

    let Ok(body) = serde_json::from_str::<serde_json::Value>(&response.body) else {
        return false;
    };

    body.pointer("/error/code").and_then(serde_json::Value::as_u64) == Some(499)
        && body.pointer("/error/type").and_then(serde_json::Value::as_str)
            == Some("request_cancelled")
}

pub fn default_should_retry(response: &HttpResponse) -> bool {
    response.status >= 500
        || response.status == 429
        || response.status == 408
        || response.status == 404
        || is_dynamo_request_cancelled(response)
}
```

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run:

```bash
cargo test -p fusillade --lib daemon::config::tests
```

Expected: all four tests pass.

- [ ] **Step 5: Verify cancellation isolation and repository quality gates**

Run:

```bash
DATABASE_URL=postgres://postgres:password@localhost:5432/fusillade_dynamo_499_retry cargo test --test request_processor
cargo fmt --check
DATABASE_URL=postgres://postgres:password@localhost:5432/fusillade_dynamo_499_retry cargo test --workspace
DATABASE_URL=postgres://postgres:password@localhost:5432/fusillade_dynamo_499_retry cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Expected: every command exits 0 with no failed tests or Clippy warnings.

- [ ] **Step 6: Commit the implementation**

```bash
git add src/daemon/config.rs docs/superpowers/plans/2026-07-15-retry-dynamo-request-cancelled.md
git commit -m "fix: retry Dynamo request cancellations"
```
