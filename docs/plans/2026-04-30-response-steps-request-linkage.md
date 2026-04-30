# Response Steps ↔ Requests Linkage

**Date:** 2026-04-30
**Status:** Proposed
**Supersedes (in part):** [2026-04-28-multi-step-responses.md](2026-04-28-multi-step-responses.md) §C7 ("Step storage" — chain identification, request_id semantic).

## Summary

Re-anchor `response_steps.request_id` so it points at the **sub-request
fusillade row created for that step's HTTP fire** instead of the
parent `/v1/responses` request, restoring the 1:1 contract between
`requests` and `http_analytics` and unblocking per-step granular
analytics. `parent_step_id` becomes the chain identifier (semantically
"root_step_id"): NULL on the head step, set to the head's id on every
non-head step. The parent `/v1/responses` request row goes away; the
response is purely the chain.

## Motivation

The original multi-step plan ([§C7](2026-04-28-multi-step-responses.md))
chose `response_steps.request_id` as the chain identifier — every step
of one user-visible response shared the same `request_id` (= the
parent `/v1/responses` fusillade row). This was simple to query and
matched the daemon path's "claim the parent, walk the chain" pattern.

In practice it broke two operational properties:

1. **Analytics fan-out.** When the multi-step loop fires a model_call
   via the loopback `/v1/chat/completions`, dwctl's responses
   middleware creates a *new* fusillade `requests` row for that proxied
   call (so onwards' outlet captures usage and writes one
   `http_analytics` row per upstream HTTP). Those sub-request rows
   have their own UUIDs; `response_steps` knows nothing about them.
   The result: a parent `/v1/responses` row has **N** `http_analytics`
   rows that nominally belong to it — but to find them you'd have to
   either join through a non-existent column or aggregate by some
   external grouping (which dwctl's dashboard doesn't do today).
   Token counts, costs, and latency for parent rows show as zero.

2. **No granular per-step analytics.** Even if we could find the
   `http_analytics` rows, attributing them back to a specific step
   requires another lookup. The natural place for that link is on
   `response_steps` itself.

The dashboard responses view also needed a way to *exclude* sub-request
fusillade rows from the listing, since they are internal artifacts of
the multi-step orchestration and not what the user submitted.

## Design

### Semantic shift

| | Before | After |
|---|---|---|
| `request_id` | Parent `/v1/responses` row (chain identifier) | Sub-request row for *this* step's HTTP fire |
| `prev_step_id` | Linear predecessor within scope | Tree edge — multiple steps may share a prev_step_id |
| `parent_step_id` | Sub-agent nesting (NULL = top-level user-visible step) | Head step of the chain (NULL only on the head itself) |
| Chain identifier | `request_id` | `parent_step_id` (= head's id) + the head's primary key for itself |
| Parent `/v1/responses` row | Existed (anchor for `resp_<id>`, `GET /v1/responses/{id}`) | Removed; response is purely the chain |
| `request_id` cardinality | One row per N steps in chain | One row per step (model_call); NULL for tool_call |

`request_id` becomes nullable because **tool_call steps don't have a
fusillade `requests` row** — their dispatch goes through dwctl's
`HttpToolExecutor` directly, and their analytics live in
`tool_call_analytics`. Only `model_call` steps fire a fusillade-tracked
HTTP request and therefore have an associated `requests` row.

### `resp_<id>` identity

`resp_<id>` is the head step's `id`. `GET /v1/responses/{id}`:

1. Looks up the head step by `id`.
2. Walks descendants via the `response_steps_chain_walk` index
   (`parent_step_id = head_id ORDER BY step_sequence`) to assemble
   the response tree.

Single-step requests (`/v1/chat/completions`, `/v1/embeddings`, etc.)
have no associated `response_steps` rows; they're untouched by this
plan.

### Sub-agent recursion (nesting via prev_step_id)

The original `parent_step_id` semantic of "nesting pointer for sub-agent
loops" is dropped. Nesting is instead expressed through the
`prev_step_id` tree — `prev_step_id` is not a linear successor pointer
but a **tree edge**: a single step can be the predecessor of multiple
children. A tool_call that dispatches a sub-agent has two successors:

- the **sub-agent's head step** (a `model_call` that runs against
  whatever model the sub-agent is configured for), and
- the **outer continuation** (the next step in the user-visible
  response, run after the sub-agent returns its result).

All steps — including everything inside a sub-agent invocation — share
the same `parent_step_id` (the user-visible head's id), so the
listing-query anti-join still excludes their `request_id`s correctly
and the chain-walk index `(parent_step_id, step_sequence)` returns
every step of the response in one range scan. Walking the tree
shape is then a client-side traversal of `prev_step_id` from the head,
ordered within each branch by `step_sequence`.

#### Why the chain idempotency constraint goes away

Branching also occurs in the basic (non-nested) flow whenever a
`model_call` returns multiple `tool_calls` — N `tool_call` children
share `(root, model_call_id, 'tool_call')`. So branching is intrinsic
to the data model, not just a sub-agent feature. Any
`(parent_step_id, prev_step_id, step_kind)` unique constraint rejects
ordinary parallel tool calls.

Idempotency falls entirely to the transition function's chain-walk
frontier check in `next_action_for`: walk the existing chain, only
emit successors that aren't already present. The migration
section above documents the constraint drop alongside the rest of
the schema change.

## Schema migration

```sql
-- 1. request_id becomes nullable (tool_call steps).
ALTER TABLE response_steps
    ALTER COLUMN request_id DROP NOT NULL;

-- 2. Drop the chain idempotency constraint entirely (see "Why the
-- chain idempotency constraint goes away" above).
ALTER TABLE response_steps
    DROP CONSTRAINT response_steps_chain_unique;

-- 3. 1:1 between a model_call step and its sub-request fusillade row.
-- Doubles as the FK-supporting index for request_id lookups, replacing
-- the role response_steps_chain used to play.
CREATE UNIQUE INDEX response_steps_request_id_unique
    ON response_steps (request_id)
    WHERE request_id IS NOT NULL;

-- 4. Drop the old chain index (anchored on old request_id semantic).
DROP INDEX IF EXISTS response_steps_chain;

-- 5. New chain walk index: from a head step's id, range-scan every
-- non-head step in the chain by step_sequence. Used by GET
-- /v1/responses/{id} (tree assembly) and by next_action_for (frontier
-- discovery during crash-recovery resume).
CREATE INDEX response_steps_chain_walk
    ON response_steps (parent_step_id, step_sequence)
    WHERE parent_step_id IS NOT NULL;

-- 6. Drop the simple parent_step_id index — subsumed by chain_walk.
DROP INDEX IF EXISTS response_steps_parent;
```

The `response_steps_prev` partial index on `prev_step_id` stays as-is —
it still serves frontier-detection during crash recovery.

### Rationale: which indexes, where

The dashboard's responses listing is the perf-critical query because:

- **Cardinality.** `requests` is the largest table (one row per
  upstream HTTP fire; multi-step + single-step combined). A multi-step
  chain of N model_calls inflates row count by N×.
- **Frequency.** Every dashboard refresh + every API list call. Cold
  scan would be ruinous on production data.

Two query forms support that listing — both work, both are O(scanned
requests × index_lookup):

```sql
-- Anti-join (preferred)
SELECT r.* FROM requests r
WHERE NOT EXISTS (
    SELECT 1 FROM response_steps s
    WHERE s.request_id = r.id
      AND s.parent_step_id IS NOT NULL
)
ORDER BY r.created_at DESC LIMIT N;

-- LEFT JOIN with NULL filter (equivalent plan)
SELECT r.* FROM requests r
LEFT JOIN response_steps s
    ON s.request_id = r.id AND s.parent_step_id IS NOT NULL
WHERE s.id IS NULL
ORDER BY r.created_at DESC LIMIT N;
```

Both use the unique partial index `response_steps_request_id_unique`
to confirm presence/absence in O(log n). Postgres applies the
`parent_step_id IS NOT NULL` predicate after fetching the matching
row from the index — but because `request_id` is unique, only one row
is fetched per request, making the per-request overhead constant.

We considered a more specialized partial index
`(request_id) WHERE parent_step_id IS NOT NULL AND request_id IS NOT NULL`
but rejected it: the existing unique partial index already gives O(log n)
lookup, the filter step is constant-time, and the additional index
would double the write cost of every step insert without measurable
read-side benefit.

The chain walk (`GET /v1/responses/{id}`) is read-light and small
(one lookup + a small range scan): `response_steps_chain_walk`'s
`(parent_step_id, step_sequence)` is naturally aligned to the access
pattern.

### `EXPLAIN` verification (pending)

The migration ships with a follow-up `EXPLAIN ANALYZE` script run in
staging against representative dataset sizes (target: 10M `requests`,
2M `response_steps`) to confirm:

1. Listing query: index scan on `requests` (`idx_requests_created_at`
   or analogous) + nested loop with index-only-lookups against
   `response_steps_request_id_unique` per row.
2. Chain walk: PK lookup for head + index range on
   `response_steps_chain_walk` returning ≤ 16 rows for typical chains.

If the planner picks a sequential scan on either at production
cardinalities, we'll add a covering composite or hint via a planner
config knob.

## Storage / write path

### Sub-request row creation

dwctl's `FusilladeResponseStore::record_step` for a `model_call`:

1. Generate `sub_request_id = Uuid::new_v4()`.
2. **Synchronously** call `Storage::create_single_request_batch` with
   `request_id = sub_request_id` so the FK is satisfied at the
   `response_steps` insert.
3. Insert `response_steps` (`request_id = sub_request_id`,
   `parent_step_id = head_step_id_or_NULL`, `prev_step_id = …`).
4. Stash `sub_request_id` in a per-loop slot the wrapped `HttpClient`
   reads.

The synchronous variant differs from the realtime path's "enqueue
underway job" (CreateResponseInput) optimization. The reason: the
multi-step path is already blocking on multiple upstream HTTP fires
serially — an extra DB round-trip per step is comparatively cheap, and
synchronous creation eliminates the FK-violation race that otherwise
forces FK relaxation. We can revisit and switch to the async pattern
later if record_step latency becomes a bottleneck (deferred FK or
explicit FK-drop would be the unblock).

### `tool_call` steps

`request_id = NULL`. `tool_call_analytics` already carries
`response_step_id` (added in dwctl migration 096), so the per-step
attribution path for tools is independent of `requests`.

### Loop's HTTP fire

dwctl wraps onwards' `HttpClient` with a per-loop adapter that, on each
`request()` call, drains the per-loop "next request_id" slot set by
`record_step` and injects `x-fusillade-request-id: <id>` on the
outgoing request. The `responses_middleware` already short-circuits
its own row creation when this header is present (line 71 of
`middleware.rs`). The outlet's `complete-response` job updates the
existing row.

## Application changes (dwctl, summarized)

Tracked in the dwctl PR; outlined here for plan completeness:

1. **Drop the parent `/v1/responses` row creation** in the warm-path
   helpers (`try_warm_path_*`).
2. **Migrate** `assemble_response`, `list_chain`, `next_action_for`,
   the daemon path's parent lookup, `GET /v1/responses/{id}`, and the
   chain-shape integration tests from "key on parent request_id" to
   "key on head step_id (parent_step_id NULL on the head)".
3. **`record_step`** pre-generates `sub_request_id` + creates the
   row + inserts the step.
4. **Wrapped `HttpClient`** injects `x-fusillade-request-id` per
   model_call HTTP fire.
5. **Listing endpoint** filters via the anti-join above.

## Risks

- **Migration on a populated table.** `response_steps` is small in
  production today (the feature is new and not yet broadly enabled),
  so the partial-index rebuilds and constraint swaps are cheap. If
  the table grows before this lands, switch to `CREATE INDEX
  CONCURRENTLY` and `ALTER ... DROP CONSTRAINT` becomes online (still
  O(table) for the constraint validation; consider `NOT VALID +
  VALIDATE CONSTRAINT` if the row count grows).

- **Crash mid-step (record vs HTTP fire).** With synchronous
  sub-request row creation, a crash between step 2 and step 3 of
  `record_step` leaves an orphan `requests` row with no
  `response_steps` row pointing at it. This is harmless: the row is
  in `processing` state with this onwards instance's daemon_id, and
  fusillade's stale-daemon detection will reclaim it after
  `stale_daemon_threshold_ms`. The SIGTERM drain (COR-353) accelerates
  the typical case.

- **`parent_step_id` semantic change for existing rows.** Any
  `response_steps` rows already in production were inserted under the
  old "nesting pointer" semantic. Pre-deploy: confirm `response_steps`
  is empty in every environment receiving the migration (the feature
  is gated behind the multi-step processor wiring; if the processor
  has never been wired in an environment, the table is empty there).
  If non-empty, hand-migrate or truncate before apply.

## Out of scope

- **Wiring sub-agent dispatch in the transition function.** The
  schema *supports* nesting via `prev_step_id` branching (and parallel
  `tool_call` siblings), but the loop's transition function still
  emits one successor per chain-walk pass. Wiring branch-point
  emission is a follow-up.
- **Backfilling analytics for already-completed multi-step responses.**
  The fix is forward-only.
- **Changing the `resp_<id>` user-visible identifier scheme.** The
  value binds to the head step's `id`, which is a UUID just like the
  old parent request_id, so external consumers see the same shape.
