# Request Attempt Ownership and Cancellation

**Date:** 2026-07-13
**Status:** Draft

## Problem

Fusillade can currently return a `processing` request to `pending` solely
because its processing timestamp is old. That transition is performed by a
different task from the one that owns the in-flight HTTP future, so it cannot
abort the original work before making the row claimable again.

If the original request is slow rather than dead, another claim can start a
second upstream request while the first request is still running. The database
row has a daemon owner, but that owner is not unique per attempt. If the same
daemon claims the row again, the old and new attempts also share the same
`daemon_id`, so the existing retry ownership check cannot distinguish them.

Individual cancellation has the inverse problem. A caller can update the row,
but the storage layer reconstructs a processing request without the real Tokio
abort handle. Only the daemon process that started the HTTP future can stop it.

Together, these behaviours allow three races:

1. A healthy but slow processing attempt is requeued and executed twice.
2. A cancellation updates database state without stopping the live HTTP task.
3. A stale attempt completes, fails, or retries after ownership has moved and
   overwrites the newer state.

## Goals

- Never requeue work owned by a healthy daemon based only on elapsed request
  time.
- Give every claim a unique persisted identity, even when the same daemon
  reclaims the request.
- Let any Fusillade API process request cancellation while ensuring that only
  the owning daemon aborts the corresponding HTTP task.
- Fence every daemon-owned completion, failure, cancellation, and retry on the
  exact attempt that produced it.
- Preserve crash recovery for requests whose owning daemon is dead or stale.
- Keep batchless background requests durable when their submitting client
  disconnects.

## Non-goals

- Detecting HTTP client disconnects in an embedding application. The embedding
  application will call the Fusillade cancellation API when its blocking or
  streaming client goes away.
- Cancelling durable background work when its submitting client disconnects.
- Guaranteeing that a remote provider stops work after its TCP connection is
  closed.
- Exactly-once upstream execution after an ungraceful daemon failure. Recovery
  after a dead or stale owner remains at-least-once.
- Replacing the existing batch cancellation API.

## Design

### 1. Persist a unique attempt identity

Add nullable ownership fields to `requests`:

```sql
attempt_id UUID NULL,
cancellation_requested_at TIMESTAMPTZ NULL
```

Each successful pending-to-claimed transition generates a new `attempt_id` in
the same SQL statement that claims the row. `Claimed` and `Processing` carry an
`AttemptId` alongside `daemon_id`.

The field remains nullable because proxy-owned realtime rows can be inserted
directly in `processing` state without passing through the daemon claim loop.
The new attempt lifecycle applies only to daemon-claimed work; existing direct
realtime completion APIs keep their current state guards.

Moving an attempt back to `pending`, or into any terminal state, clears its
active ownership fields. A later claim always receives a different
`attempt_id`.

### 2. Fence daemon-owned transitions

All daemon-owned state changes after claim use a compare-and-set predicate:

```sql
WHERE id = $request_id
  AND state = $expected_state
  AND daemon_id = $daemon_id
  AND attempt_id = $attempt_id
```

Non-cancellation transitions out of `processing` also require
`cancellation_requested_at IS NULL`. Once cancellation intent is recorded, a
normal completion, failure, or retry cannot win while the cancellation poll is
catching up. A cancellation that races a terminal write has simple row-lock
ordering: whichever transition commits first wins, and the loser observes an
ownership/state conflict.

This applies to:

- `claimed` to `processing`
- `processing` to `completed`
- `processing` to terminal `failed`
- `processing` to `canceled`
- `processing` to retryable `pending`
- owner-driven `claimed` to `pending`

The storage contract returns whether the compare-and-set succeeded. Losing
ownership is an expected race outcome, not a database failure. The daemon logs
and counts it, then discards the stale result.

The existing `daemon_id` retry guard remains useful for operational context,
but `attempt_id` is the authoritative attempt fence.

The claimed-to-processing compare-and-set happens **before** the HTTP future is
spawned or polled. This reverses the current ordering, which starts the task and
then persists `processing`. If a claimed request was canceled or reclaimed, the
compare-and-set fails and the unpolled response future is dropped, so no
upstream request starts. If the daemon exits after persisting `processing` but
before spawning, normal dead-owner recovery handles the row.

### 3. Signal processing-request cancellation through storage

`cancel_requests` handles each current state atomically:

| Current state | Cancellation action |
|---------------|---------------------|
| `pending` | Transition directly to `canceled` |
| `claimed` | Transition directly to `canceled`; a racing claimed-to-processing compare-and-set fails before the HTTP future is spawned |
| `processing` | Set `cancellation_requested_at` without making the row claimable |
| terminal | Return the existing invalid-state result |

The daemon maintains a request-scoped cancellation token keyed by
`(request_id, attempt_id)` for each local processing attempt. The existing
cancellation poll also checks active request attempts in one bounded bulk query.
When it observes `cancellation_requested_at` for the same attempt, it cancels
the local token.

The processing task then:

1. aborts its real Tokio HTTP task,
2. transitions `processing` to `canceled` using the full attempt fence, and
3. removes the request-scoped token from the local registry.

The database poll is the reliable cross-process mechanism. A later notification
can reduce cancellation latency, but correctness must not depend on delivery of
a notification.

The cancellation API continues to be best-effort: success means cancellation
was recorded. A processing request can remain visible as `processing` until its
owner acknowledges it, normally within the configured cancellation poll
interval.

### 4. Reclaim only work whose owner is unavailable

Remove the wall-clock-only reclaim path for both `claimed` and `processing`
requests. Request duration is not evidence that the owner or HTTP future is
dead.

Automatic reclaim remains available when the owning daemon is explicitly dead
or its heartbeat is stale:

- A row without cancellation intent returns to `pending` and clears ownership.
- A row with `cancellation_requested_at` transitions to `canceled` instead of
  being retried.

Attempt fencing prevents a stale owner from writing database results after
reclaim. It cannot guarantee that a partitioned or paused process has stopped
remote provider work, so stale-heartbeat recovery remains explicitly
at-least-once.

The existing claim and processing timeout configuration fields are retained as
deprecated no-ops for compatibility in this change. They can be removed in a
future breaking release. Operators should use daemon heartbeat health and
in-flight age metrics to detect wedged work instead of silently duplicating it.

### 5. Reuse the owner-side cancellation path

Batch cancellation and graceful daemon shutdown already resolve cancellation
futures inside the owning process. They continue to use their existing batch
and shutdown tokens, but all user cancellation paths share the same owner-side
abort behaviour:

- user cancellation aborts and attempt-fences a terminal `canceled` write;
- shutdown aborts without marking the request terminal, allowing dead-owner
  recovery to requeue it;
- retry begins only after the current HTTP future has completed or been
  aborted.

Request-scoped cancellation composes with batch cancellation by selecting the
first user cancellation signal. Removing one token never affects another
attempt for the same request.

## State Sequences

### Cross-process cancellation

```text
API process                 PostgreSQL                 owner daemon
    |                           |                           |
    | cancel(request_id)        |                           |
    |-------------------------->|                           |
    |                           | mark cancellation intent  |
    |<--------------------------|                           |
    |                           |<-------- bulk poll --------|
    |                           |--------- attempt id ------>|
    |                           |                           | abort HTTP task
    |                           |<-- fenced canceled write --|
```

### Dead-owner recovery

```text
reclaimer                   PostgreSQL                 stale owner
    |                           |                           |
    | owner unavailable?        |                           |
    |-------------------------->|                           |
    | requeue + clear attempt   |                           |
    |-------------------------->|                           |
    |                           |<-- stale completion -------|
    |                           | reject: attempt mismatch   |
```

## Error Handling and Observability

Add counters for:

- cancellation intents recorded,
- owner-side request cancellations observed,
- attempt-fenced writes rejected after ownership loss,
- requests reclaimed from dead or stale daemons,
- canceled requests finalized during dead-owner recovery.

Logs for ownership loss include request, daemon, and attempt identifiers. They
must not include request or response bodies.

Cancellation polling remains bounded by the existing poll query timeout. A
poll failure leaves requests in `processing` with intent recorded; it does not
make them claimable or create duplicate work.

## Test Strategy

Database and daemon tests cover the concurrency boundaries directly:

1. An old processing timestamp is not enough to reclaim work from a healthy
   daemon.
2. Work owned by a dead or stale daemon is reclaimed.
3. Dead-owner recovery finalizes a cancellation intent instead of requeueing it.
4. Every claim receives a new attempt ID, including consecutive claims by the
   same daemon.
5. A completion, failure, cancellation, or retry from an old attempt cannot
   modify a newly claimed row.
6. Canceling a processing request in another manager instance causes the owner
   daemon to abort the real HTTP future and persist `canceled`.
7. Once processing cancellation intent is recorded, a concurrent normal
   completion, failure, or retry cannot commit.
8. Canceling a claimed request races safely with process startup: a failed
   processing compare-and-set drops the unpolled HTTP future.
9. Batch cancellation and graceful shutdown retain their existing behaviour.
10. Proxy-owned realtime rows still complete and fail through their existing
   APIs without an attempt ID.

Async tests use triggers, barriers, and bounded polling rather than fixed sleeps.

## Rollout and Compatibility

The migration is additive and nullable, so old binaries can continue reading
and writing rows during a rolling deployment. New binaries only rely on
`attempt_id` for claims they create themselves.

During a mixed-version rollout, an old daemon does not write attempt-fenced
terminal states and may still run wall-clock-only reclaim. The ownership
guarantee therefore applies only after every daemon is upgraded. Request-scoped
cancellation should not be enabled by an embedding application until rollout
is complete.

## Alternatives Considered

### Only remove processing-timeout reclaim

This closes the known duplicate-start path with a small change, but stale
attempts can still overwrite newer state after dead-owner recovery, and
individual cancellation still cannot reach the real HTTP task.

### Fence only by daemon ID

This is the current retry approach. It fails when the same daemon claims a row
again because both attempts have the same owner ID.

### Store an abort handle in PostgreSQL

Tokio abort handles are process-local and cannot be serialized. The database
can record intent and identity, but the owner process must perform the abort.

### Use notifications without polling

Notifications provide lower latency but are not durable. A disconnect or
listener restart can miss one, leaving work running. Polling provides the
source-of-truth fallback.

### Requeue immediately when a client disconnects

Requeueing would execute work for which the original caller is no longer
waiting and can overlap the original attempt. Blocking and streaming clients
should request cancellation; durable background requests should continue.
