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
- Never reclaim `processing` work from a healthy daemon based only on request
  age. A timed-out `claimed` row is safe to recover because its upstream start
  gate has not opened.
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
- Removing the existing claim and processing timeout configuration fields.
  `claim_timeout_ms` remains the safe pre-dispatch recovery bound;
  `processing_timeout_ms` remains accepted for configuration compatibility.

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
existing APIs. Ordinary persistence refuses to mutate any row with a non-null
attempt ID, so a custom processor cannot silently bypass the fence. Bulk
administrative transitions revoke ownership explicitly before changing state;
realtime complete/fail APIs require a null attempt ID.

Attempt-aware Storage methods are required rather than fail-open defaults. This
keeps a custom backend from silently claiming fencing support while performing
ID-only writes.

### Persist processing before upstream execution

`Request<Claimed>::process` creates the response task behind a start gate. It
first performs the claimed-to-processing compare-and-set. Only after that
write succeeds does it release the gate and allow the response future to be
polled.

If the compare-and-set loses ownership, the gated task is aborted and the
response future is dropped without contacting the upstream provider. If the
daemon dies after persisting `processing` but before releasing the task, normal
dead-owner recovery eventually requeues the row.

The HTTP task also watches for its result receiver to close. If a custom
processor unwinds, dropping `Request<Processing>` cancels the upstream future
as the daemon begins recovery of that exact attempt.

### Reclaim unavailable owners and abandoned pre-dispatch claims

Remove the wall-clock-only `processing` timeout branch from stale-request
reclamation. Processing duration is not evidence that the owner is dead.

Keep age recovery for `claimed` rows. The upstream future cannot be polled
until the claimed-to-processing CAS wins, so reclaiming an old claim only
causes its old task to lose that CAS; it cannot create concurrent upstream
work. The reclaimer carries the recovery reason through its locking CTE and
rechecks the current row state: an age-only candidate must still be `claimed`
when locked, while a candidate with a proven unavailable owner may also be
reclaimed from `processing`. This closes the READ COMMITTED race where the
attempt advances between candidate selection and row locking.

Reclamation remains available for requests whose daemon row is explicitly
`dead` or whose `last_heartbeat` exceeds `stale_daemon_threshold_ms`. The
default stale threshold increases from 30 seconds to 5 minutes. Reclaim clears
the attempt ID before the row becomes claimable, so any old worker that later
returns is fenced out.

The threshold remains configurable for operators that can guarantee tighter
heartbeat isolation. The default favors avoiding duplicate paid inference over
fast recovery from a crashed pod.

If a per-request processor returns an unexpected error or panics, the daemon
converts the outcome into a retriable failure and applies the normal retry
count, exponential backoff, deadline, and terminal-failure policy. Attempt
outcome writes themselves retry for as long as the daemon remains running.
Shutdown leaves the row owned so dead-owner recovery can take over. Normal
ownership loss is handled as a clean outcome rather than a background-task
error.

## Race Semantics

Reclaim and a terminal write serialize on the request row:

- If the terminal write commits first, reclaim no longer matches an in-flight
  state.
- If reclaim commits first, it clears the attempt ID and the terminal write's
  compare-and-set affects zero rows.

The reclaimer selects candidates with `FOR UPDATE SKIP LOCKED` and updates
those locked rows in the same statement. This prevents a candidate selected
as in-flight from being reset after a concurrent terminal writer commits.

After a new claim, the old and new executions have different attempt IDs even
if their `daemon_id` values match. The old execution can neither terminalize
nor reschedule the new attempt.

## Observability

Add a counter for fenced attempt writes that lose ownership. Logs include the
request and attempt IDs and the attempted transition, but never request or
response bodies.

Existing stale-reclaim metrics represent expired pre-dispatch claims plus dead
or heartbeat-stale owners. Processor error and panic counters record whether
the exact attempt was rescheduled or terminalized and whether its CAS applied.

## Test Strategy

Database and processing tests cover the concurrency boundaries:

1. Each claim returns and persists a non-null attempt ID.
2. Reclaiming and reclaiming again under the same daemon produces a different
   attempt ID.
3. A stale completion, failure, cancellation, or retry cannot modify a newer
   attempt.
4. Claimed-to-processing requires the matching attempt.
5. The HTTP future is not polled until processing persistence succeeds.
6. A healthy daemon's old claim is safely reclaimed without polling its old
   upstream future, while old processing work is not age-reclaimed.
7. A dead or heartbeat-stale daemon's work is reclaimed and its attempt ID is
   cleared.
8. Processor errors and panics recover the exact attempt with normal
   backoff/retry limits; panic unwinding cancels the old upstream future.
9. Attempt outcome writes survive transient storage errors without leaving a
   live-owner orphan.
10. Ordinary and realtime persistence refuse to overwrite a daemon-owned
    attempt.
11. Proxy-owned realtime rows retain their existing lifecycle.

## Rollout

The migration is additive and nullable, so it can be applied before the new
binary and supports a rolling deployment. Legacy retry writes are restricted
to null-attempt rows, while new binaries use attempt-aware APIs.

Adding attempt identity to the public `Claimed` and `Processing` typestates,
the ownership-loss error, and required attempt-aware Storage methods is a
source-breaking API change and requires major releases of the published crates.
The full fencing guarantee begins only after all daemon pods run the new
binary: an old binary neither creates nor checks attempt IDs and retains
processing-age reclaim. Complete the rollout before treating fencing as active.
