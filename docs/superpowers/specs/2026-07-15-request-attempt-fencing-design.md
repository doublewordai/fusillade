# Request Attempt Fencing

**Date:** 2026-07-15
**Status:** Approved for implementation

## Problem

Fusillade currently requeues `claimed` and `processing` requests when their
request timestamps exceed fixed timeouts, even if the owning daemon is still
healthy. Requeueing only changes PostgreSQL state; it cannot abort the HTTP
future that is still running in the owner's process. A later claim can
therefore start a second upstream execution.

Ownership is recorded only as `daemon_id`. That is insufficient when the same
daemon reclaims the row: both the old and new executions have the same owner.
Most terminal writes also match only the request ID, so the old execution can
overwrite the newer execution's result.

The current claimed-to-processing transition compounds this race by polling
the HTTP future before `processing` ownership has been persisted.

## Goals

- Assign every daemon claim a unique persisted attempt identity.
- Fence every daemon-owned state transition on the exact attempt that produced
  it.
- Persist `processing` ownership before the upstream HTTP future is polled.
- Never reclaim work from a healthy daemon based only on request age.
- Preserve recovery when the owning daemon is explicitly dead or its heartbeat
  is stale.

## Non-goals

- Exactly-once upstream execution after an ungraceful daemon failure. A request
  may have reached the provider before its owner died, so failover remains
  intentionally at-least-once.
- Cross-process cancellation of an individual request. This change preserves
  the existing batch and shutdown cancellation paths; request-scoped
  cancellation is a separate feature.
- A dedicated heartbeat connection or pool. The larger stale threshold makes
  transient shared-pool starvation safe; isolating heartbeats remains a useful
  follow-up latency improvement.
- Removing the existing claim and processing timeout configuration fields in a
  breaking API change. They remain accepted but no longer drive reclamation.

## Design

### Persist a unique attempt ID

Add a nullable `attempt_id UUID` column to `requests`. It is nullable for
proxy-owned realtime rows that enter `processing` without passing through a
daemon claim.

Every successful pending-to-claimed transition generates a UUID in the same
SQL statement that changes the state. The returned `Request<Claimed>` carries
that exact value. `Request<Processing>` carries it forward.

Moving a daemon-owned request to `pending` or a terminal state clears active
ownership, including `attempt_id`. A later claim always gets a new value,
including when the same daemon performs both claims.

### Compare-and-set daemon transitions

Daemon-owned writes use the predicate:

```sql
WHERE id = $request_id
  AND state = $expected_state
  AND attempt_id = $attempt_id
```

The database's attempt ID is the authoritative fence. `daemon_id` remains for
operational ownership and heartbeat lookup, but cannot distinguish two
executions by the same daemon.

The fenced operations are:

- `claimed` to `processing`;
- owner-driven `claimed` to `pending`;
- `processing` to `completed`;
- `processing` to terminal `failed`;
- `processing` to `canceled`;
- `processing` to retryable `pending`.

The storage API returns whether the compare-and-set won. A false result means
the in-memory execution has lost ownership. It is an expected concurrency
outcome: the caller discards that result, increments an ownership-loss metric,
and does not retry the stale write.

Manual administrative writes and proxy-owned realtime transitions keep their
existing APIs. They do not pretend to be daemon attempts.

### Persist processing before upstream execution

`Request<Claimed>::process` creates the response task behind a start gate. It
first performs the claimed-to-processing compare-and-set. Only after that
write succeeds does it release the gate and allow the response future to be
polled.

If the compare-and-set loses ownership, the gated task is aborted and the
response future is dropped without contacting the upstream provider. If the
daemon dies after persisting `processing` but before releasing the task, normal
dead-owner recovery eventually requeues the row.

### Reclaim only unavailable owners

Remove the wall-clock-only claim/processing timeout branch from stale-request
reclamation. Request duration is not evidence that the owner is dead.

Reclamation remains available for requests whose daemon row is explicitly
`dead` or whose `last_heartbeat` exceeds `stale_daemon_threshold_ms`. The
default stale threshold increases from 30 seconds to 5 minutes. Reclaim clears
the attempt ID before the row becomes claimable, so any old worker that later
returns is fenced out.

The threshold remains configurable for operators that can guarantee tighter
heartbeat isolation. The default favors avoiding duplicate paid inference over
fast recovery from a crashed pod.

## Race Semantics

Reclaim and a terminal write serialize on the request row:

- If the terminal write commits first, reclaim no longer matches an in-flight
  state.
- If reclaim commits first, it clears the attempt ID and the terminal write's
  compare-and-set affects zero rows.

After a new claim, the old and new executions have different attempt IDs even
if their `daemon_id` values match. The old execution can neither terminalize
nor reschedule the new attempt.

## Observability

Add a counter for fenced attempt writes that lose ownership. Logs include the
request and attempt IDs and the attempted transition, but never request or
response bodies.

Existing stale-reclaim metrics remain, now representing only dead or
heartbeat-stale owners.

## Test Strategy

Database and processing tests cover the concurrency boundaries:

1. Each claim returns and persists a non-null attempt ID.
2. Reclaiming and reclaiming again under the same daemon produces a different
   attempt ID.
3. A stale completion, failure, cancellation, or retry cannot modify a newer
   attempt.
4. Claimed-to-processing requires the matching attempt.
5. The HTTP future is not polled until processing persistence succeeds.
6. A healthy daemon's old claimed or processing request is not reclaimed.
7. A dead or heartbeat-stale daemon's work is reclaimed and its attempt ID is
   cleared.
8. Proxy-owned realtime rows retain their existing lifecycle.

## Rollout

The migration is additive and nullable, so schema deployment is backward
compatible. The full fencing guarantee begins only after all daemon pods run
the new binary: an old binary neither creates nor checks attempt IDs and still
contains the request-age reclaim path. Deployments must therefore complete the
rollout before treating attempt fencing as active.

