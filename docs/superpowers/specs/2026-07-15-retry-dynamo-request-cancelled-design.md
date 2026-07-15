# Retry Dynamo Request-Cancelled Responses

## Context

Dynamo can report an upstream cancellation as an OpenAI-compatible error envelope with an effective HTTP status of `499`:

```json
{
  "error": {
    "code": 499,
    "message": "CancelledError: ",
    "type": "request_cancelled"
  }
}
```

Fusillade currently treats every `499` response as non-retriable. A transient Dynamo cancellation therefore becomes a terminal batch failure even when the retry budget and batch deadline would allow another attempt.

## Design

Extend `default_should_retry` with one additional, narrowly scoped condition. A response is retryable when all of the following are true:

1. The effective response status is `499`.
2. The response body is valid JSON with numeric `error.code` equal to `499`.
3. The same error object has `error.type` equal to `"request_cancelled"`.

The existing retry conditions for 5xx, 429, 408, and 404 remain unchanged. The predicate will parse a response body only after observing status 499.

## Cancellation Isolation

This change applies only after an upstream HTTP response has completed. Fusillade's user-cancellation and daemon-shutdown branches resolve through `CancellationReason` before HTTP retry classification, so they never invoke this new condition. Their abort and persistence behavior will not change.

Existing retry controls still apply to matching Dynamo responses, including maximum attempts, exponential backoff, and the batch deadline. The change does not create an unbounded special retry loop.

## Non-goals

- Do not make generic 499 responses retryable.
- Do not infer a retry from the error message alone.
- Do not accept missing, string-valued, or mismatched error codes.
- Do not change HTTP statuses or persisted error bodies.
- Do not change public retry-predicate signatures or cancellation APIs.

## Testing

Add unit coverage around `default_should_retry` that proves:

- The exact Dynamo cancellation signature is retryable.
- A generic 499 is not retryable.
- A 499 with the wrong or missing `error.code` is not retryable.
- A 499 with the wrong or missing `error.type` is not retryable.
- A malformed JSON body is not retryable.
- Existing retryable and non-retryable statuses retain their current behavior.

Run the focused unit tests first, then formatting, workspace tests, and Clippy before publishing the PR.
