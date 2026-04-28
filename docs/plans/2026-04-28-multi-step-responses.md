# Multi-Step Responses with Persistent Tool-Call Orchestration

**Date:** 2026-04-28
**Status:** Proposed

## Overview

This plan extends the existing `/v1/responses` (Open Responses API) implementation
to support multi-step agentic loops — model calls that emit tool calls, server-side
tool execution, and subsequent model calls that incorporate tool results — with
each step persisted as a discrete, observable, resumable unit of work.

The architecture reuses fusillade's existing batch daemon infrastructure unchanged,
introduces one new table (`response_steps`), and adds a per-row processing hook so
that the daemon can resume an in-progress multi-step response after a crash without
needing a separate daemon process or table-level discriminator. Onwards remains
the primary executor while alive; fusillade is the durable fallback.

## Motivation

Today's `/v1/responses` flow treats each response as a degenerate single-row batch:
the user's prompt is fired once, the model's reply is stored verbatim, and the
response is marked complete. This works for prompt-only workloads but does not
support the tool-calling control flow that the Open Responses API contract
promises:

1. **No server-side tool execution.** The dwctl tool registry (`tool_sources`,
   `HttpToolExecutor`) is wired into onwards via `tool_injection_middleware`, but
   the in-process tool loop in onwards is not durable. Any agentic flow that the
   loop drives is lost if the onwards instance crashes mid-execution.
2. **No per-step observability.** A response that internally fired five model
   calls and three tool calls is recorded as one opaque row. Per-step latency,
   tool errors, token splits, and reasoning traces are not queryable.
3. **No resumability contract.** The Open Responses API's `response_id` is meant
   to be a durable handle. Today, an in-flight response on a crashed pod is lost,
   not resumed.
4. **Streaming and persistence are entangled.** Onwards' streaming SSE handler
   emits events synthesized from in-process state. Reconnection after a dropped
   connection is not possible.

The tool-calling scaffolding in onwards (`ToolExecutor` trait, `RequestContext`,
`ResponseStore`) and dwctl (`tool_sources`, `HttpToolExecutor`,
`tool_call_analytics`) is already in place. What is missing is the persistent
multi-step orchestration layer that ties them together.

## Design Constraints

The following constraints were identified during design and are load-bearing for
the chosen architecture. Any future revision should re-evaluate against them.

### C1 — Resumability after crash or graceful shutdown

A response in-flight at the moment a control-layer pod crashes (or receives
SIGTERM during a deploy) must complete to terminal state without manual
intervention. The orchestrator that picks up the work must not require any
in-memory state from the original executor.

### C2 — Onwards remains the primary executor for priority responses

Latency-sensitive responses (priority service tier, real-time client streaming)
must run inline in the onwards process to keep tail latency low and stream
events to the client without an extra hop. Fusillade is the fallback, not the
primary execution path.

### C3 — Per-step observability and analytics

Each model call and each tool call must be a discrete, queryable record with its
own duration, status, error, token counts (for model calls), and analytics
linkage. The dashboard must be able to show a per-step timeline for any
response. KPI rollups (total tokens, cost, etc.) must continue to aggregate over
top-level requests without double-counting steps.

### C4 — No data duplication between holistic response and individual steps

The aggregated final response returned via `GET /v1/responses/{id}` must be
materialized from the step rows on read, not stored as a denormalized copy. Step
rows are the source of truth; the parent envelope stores only the request
metadata and final status.

### C5 — Storage and orchestration responsibilities cleanly separated

Fusillade owns durable storage and the daemon framework. Dwctl owns
response-API-specific domain logic (the transition function that decides what
the next step is given a completed step's output, and the assembly logic that
materializes the final response). Onwards owns the orchestration loop and a
trait that abstracts storage. No domain knowledge of Open Responses leaks into
fusillade; no orchestration logic leaks into dwctl's storage layer.

### C6 — Atomic, idempotent step transitions

The transition function — given a completed step's output, decide whether to
append another step or mark the parent complete — must be idempotent and safe
to re-run. A crash between writing step N's result and inserting step N+1 must
not produce duplicate or missing steps when the daemon picks up the work.

### C7 — Stream resumability across executor handoff

Streaming responses that drop their HTTP connection (because the original
executor pod was terminated or the network partitioned) must be reconnectable
via standard SSE `Last-Event-ID` resumption, with the daemon delivering the
remaining events as it completes the work.

### C8 — No regression to the existing batch path

The existing batch daemon, claim query, and request-template/batch ingestion
must continue to function identically. Changes must be additive. KPI rollups,
existing dashboard views, and external integrations must not require migration.

### C9 — Throughput and concurrency must remain tunable

A daemon worker that runs a multi-step response holds its claim for the entire
multi-step duration. The architecture must not preclude future per-tool-source
or per-response-step concurrency tuning, even if such tuning is out of scope
for the initial implementation.

### C10 — Intra-response parallelism

When a single model call emits N independent tool calls, they must execute
concurrently within the worker that owns the parent claim. Parallelism is
bounded only by upstream rate limits (per-tool-source, future) and the
worker's own resources, never by step-level lease contention. The schema and
orchestration code must permit in-process fan-out without coordinating across
daemon workers.

### C11 — Nested orchestration without surface-level fragmentation

A tool that itself spawns an agent loop (e.g., a "delegate to sub-agent" tool)
must record its sub-steps under the same top-level `request_id` as the parent
response. The user must see exactly one response in their list and one
top-level event stream regardless of nesting depth. Sub-agent steps must
remain queryable for analytics and dashboard drill-down, but must not be
exposed in the user-facing `output: [...]` array or the user's SSE stream by
default.

The architecture imposes no shape on the recursion tree (e.g., one layer per
iteration vs. unbounded depth) — tool authors control how their sub-agents
spawn further work via the prompts and tool wiring they supply. Two
independent configurable caps exist purely as safety:

- **`max_response_step_depth`** — caps the depth of the sub-agent recursion
  tree. A `tool_call` step that would exceed this depth fails with a
  `max_depth_exceeded` error in its `function_call_output` rather than
  entering the sub-loop. The orchestrator handles this like any other tool
  failure.
- **`max_response_iterations`** — caps the number of model_call ↔ tool_call
  iterations within a single loop level (the existing onwards 10-iteration
  cap, made explicit). When a single loop level reaches this cap, the parent
  step is failed with `max_iterations_exceeded`.

These limits compose: the absolute worst-case work for one response is
bounded by `max_response_step_depth × max_response_iterations × fan_out_width`,
which with reasonable defaults (depth 8, iterations 10, fan-out cap from
the model's tool-emit) keeps the bound well below pathological.

## Architecture

### High-level shape

```
┌─────────────────────────────────────────────────────────────────────┐
│  POST /v1/responses          (priority, streaming or non-streaming) │
│        │                                                            │
│        ▼                                                            │
│  control-layer pod (onwards inline)                                 │
│        │                                                            │
│        │ inserts requests row (state='processing', daemon_id=self)  │
│        │ runs run_response_loop:                                    │
│        │   for each step:                                           │
│        │     - fire upstream model OR tool                          │
│        │     - persist result to response_steps                     │
│        │     - call transition fn → next step or complete           │
│        │     - emit SSE events to client                            │
│        │ marks requests row 'completed'                             │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼ on crash / SIGTERM unclaim
                              │
┌─────────────────────────────────────────────────────────────────────┐
│  fusillade pod (existing batch daemon)                              │
│        │                                                            │
│        │ claims requests row (existing claim query, unchanged)      │
│        │ per-row processor (new hook):                              │
│        │   - if response_steps exist for request_id:                │
│        │       walk chain, assemble next HTTP payload from accumulated state  │
│        │   - else: use request.body as today                        │
│        │   - fire HTTP via HttpClient                               │
│        │   - record response                                        │
│        │   - call transition fn → loop or mark complete             │
└─────────────────────────────────────────────────────────────────────┘

shared: run_response_loop (lives in onwards), used by both executors
```

### Core insight: differentiation is emergent, not declared

A response parent row in `fusillade.requests` looks structurally identical to a
batch-of-1 request: it has a `template_id` (storing the original prompt) and a
`batch_id` (providing user attribution). The existing batch daemon's claim
query, partial indexes, and SLA-weighted ordering all work on it without any
modification.

What makes a row a multi-step response, at processing time, is the presence of
rows in `response_steps` referencing it. The per-row processor checks for those
rows before building its HTTP payload:

- **No steps yet** (first claim, simple prompt): fire `request.body` as today.
- **Steps exist** (resuming after a crash, or mid-loop): walk the chain to
  assemble the accumulated context, build the next HTTP payload, fire it.
- **After response**: dwctl's transition function decides whether to write
  another step row and re-loop, or mark the parent complete.

No `kind` discriminator column is needed. No second daemon. No claim-query
changes.

### Why a single daemon works

Three observations make this collapse possible:

1. **Onwards holds the priority claim while alive.** Onwards inserts the parent
   row in `state='processing'` with its own `daemon_id`. The fusillade daemon's
   claim query targets `state='pending'` and never sees these rows.
2. **SIGTERM unclaim is fast.** When a control-layer pod receives SIGTERM, it
   explicitly UPDATEs any rows it owns back to `state='pending'` before
   shutdown. The fusillade daemon picks them up on its next claim cycle (within
   the daemon's `claim_interval_ms`, typically 1 second in production).
3. **SLA-weighted ordering prioritizes expiring work.** Response batches have
   short completion windows relative to long-running batch jobs. Fusillade's
   existing SLA-weighted claim ordering (see
   [2026-03-26-sla-weighted-fair-scheduling](2026-03-26-sla-weighted-fair-scheduling.md))
   pushes recovered priority responses ahead of batch work automatically.

The 4-hour `processing_timeout_ms` fallback exists for true edge cases (hard
crash with no SIGTERM and somehow undetected by the 30-second daemon-dead
liveness check); it is not the expected recovery path.

### Why one shared `run_response_loop` function

The orchestration logic — given a parent request and zero-or-more completed
steps, determine the next step (model call or tool call) or terminal state — is
purely a function of state. It does not depend on whether the caller is the
onwards inline POST handler or a fusillade daemon worker. By implementing it
once as a free function over an abstract `ResponseStore` trait, both executors
share identical behavior.

## Schema Changes

### New table: `fusillade.response_steps`

```sql
-- migrations/<timestamp>_add_response_steps.up.sql

CREATE TABLE fusillade.response_steps (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id      UUID NOT NULL REFERENCES fusillade.requests(id) ON DELETE CASCADE,

    -- Linear predecessor within a single chain (NULL = first step in its scope).
    -- Scope is (request_id, parent_step_id): the chain restarts inside each
    -- nested sub-loop. See "Nested sub-agents" below.
    prev_step_id    UUID NULL REFERENCES fusillade.response_steps(id),

    -- Nesting pointer for sub-agent loops (NULL = top-level step in the
    -- user's response). See C11 + "Nested sub-agents".
    parent_step_id  UUID NULL REFERENCES fusillade.response_steps(id),

    step_kind       TEXT NOT NULL CHECK (step_kind IN ('model_call', 'tool_call')),

    -- Monotonic per request_id (global across all nesting levels).
    -- Doubles as the Last-Event-ID stream cursor for top-level events.
    step_sequence   BIGINT NOT NULL,

    -- Step inputs (instructions; full HTTP body is assembled at fire time
    -- from the chain walk, not denormalized into each row)
    request_payload JSONB NOT NULL,

    -- Step outputs (NULL until completed)
    response_payload JSONB NULL,

    -- State machine — mirrors fusillade.requests.state values.
    -- No claim columns: serialized access is provided by the parent
    -- requests row's lease (held by onwards inline or by the fusillade
    -- daemon worker). See C10 + "Parallelism within a response".
    state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'processing',
                                     'completed', 'failed', 'canceled')),

    started_at      TIMESTAMPTZ NULL,
    completed_at    TIMESTAMPTZ NULL,
    failed_at       TIMESTAMPTZ NULL,
    canceled_at     TIMESTAMPTZ NULL,
    retry_attempt   INT NOT NULL DEFAULT 0,
    error           JSONB NULL,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Idempotency safety net: a re-running transition function under crash
    -- recovery must not produce duplicate successor rows. Recovery primarily
    -- relies on a chain-walk that identifies the existing frontier; this
    -- constraint backstops any race.
    UNIQUE (request_id, parent_step_id, prev_step_id, step_kind)
);

-- Chain walk for a given scope (top-level or sub-loop): the request_id +
-- parent_step_id pair identifies the chain, step_sequence orders it.
-- Postgres treats NULL parent_step_id as its own group (top-level).
CREATE INDEX response_steps_chain
    ON fusillade.response_steps (request_id, parent_step_id, step_sequence);

-- Predecessor lookup: find a step's successor (or detect leaf during recovery).
CREATE INDEX response_steps_prev
    ON fusillade.response_steps (prev_step_id)
    WHERE prev_step_id IS NOT NULL;

-- Sub-loop traversal: gather all sub-steps of a given parent step
-- (e.g., when rendering the dashboard's expandable sub-agent tree).
CREATE INDEX response_steps_parent
    ON fusillade.response_steps (parent_step_id)
    WHERE parent_step_id IS NOT NULL;
```

### Analytics linkage in dwctl

Two columns added to existing dwctl tables, paralleling the existing
`fusillade_request_id` / `fusillade_batch_id` correlation pattern.

```sql
-- dwctl/migrations/<n>_add_response_step_id.sql

ALTER TABLE http_analytics
    ADD COLUMN response_step_id UUID NULL;

CREATE INDEX idx_analytics_response_step_id
    ON http_analytics (response_step_id)
    WHERE response_step_id IS NOT NULL;

ALTER TABLE tool_call_analytics
    ADD COLUMN response_step_id UUID NULL;

CREATE INDEX idx_tool_call_analytics_response_step_id
    ON tool_call_analytics (response_step_id)
    WHERE response_step_id IS NOT NULL;
```

### No schema changes to `fusillade.requests` or `fusillade.batches`

Response parents continue to use the existing `requests` table with a
single-row batch wrapper for prompt storage and user attribution, exactly as
today's `FusilladeResponseStore::create_batch_of_1` does. No new columns. No
new partial indexes. No claim-query changes. The existing `requests` indexes
remain healthy because no new row population is added to the table beyond what
already happens for response parents today.

### Naming conventions

Foreign-key columns are named after their target table for unambiguous reads:

| Column                                  | Target                          |
|-----------------------------------------|---------------------------------|
| `response_steps.request_id`             | `fusillade.requests.id`         |
| `response_steps.prev_step_id`           | `fusillade.response_steps.id`   |
| `response_steps.parent_step_id`         | `fusillade.response_steps.id`   |
| `http_analytics.response_step_id`       | `fusillade.response_steps.id`   |
| `tool_call_analytics.response_step_id`  | `fusillade.response_steps.id`   |

This avoids confusing names like `response_id` for an FK that points at a
`requests` row.

## Service Responsibilities

The boundaries between the three Rust components are deliberate and load-bearing
for the architecture. They are codified by traits at the `onwards` and
`fusillade` library boundaries.

### fusillade — pure infrastructure

**Owns:**
- The `response_steps` table schema and indexes.
- A `ResponseStepStore` trait providing CRUD + claim primitives over the new
  table (mirroring the existing `Storage` trait shape).
- A `PostgresResponseStepManager` implementing `ResponseStepStore` against
  PostgreSQL.
- An additive extension to the daemon's per-row processing path: a hook trait
  (working name `RequestProcessor`) that defaults to today's
  fire-once-and-store behavior, but can be overridden by consumers (dwctl) to
  inject custom orchestration before and after the HTTP fire.

**Does not own:**
- Any knowledge of Open Responses API semantics, tool-call protocols, or step
  transitions.
- The decision of *what* the next step is. That logic is injected.
- Stream event projection or assembly.

**Public Rust API delta:**

```rust
// New trait, parallel to the existing Storage trait.
pub trait ResponseStepStore: Send + Sync {
    async fn create_step(&self, input: CreateStepInput) -> Result<StepId>;
    async fn get_step(&self, id: StepId) -> Result<Option<ResponseStep>>;
    async fn list_chain(&self, request_id: RequestId) -> Result<Vec<ResponseStep>>;
    async fn complete_step(&self, id: StepId, response: serde_json::Value) -> Result<()>;
    async fn fail_step(&self, id: StepId, error: serde_json::Value) -> Result<()>;
    // ... lease, claim, unclaim_stale, list, etc., mirroring DaemonStorage.
}

// Hook trait the daemon calls per claimed row.
// Default impl: today's behavior — fire request.body, store response, done.
pub trait RequestProcessor: Send + Sync {
    async fn process(
        &self,
        request: &Request<Claimed>,
        http: &dyn HttpClient,
    ) -> Result<ProcessOutcome>;
}
```

### dwctl — orchestration owner and domain glue

**Owns:**
- `FusilladeResponseStore`, extended to wrap both `PostgresRequestManager`
  (existing) and `PostgresResponseStepManager` (new).
- The implementation of onwards' extended `ResponseStore` trait (see below) —
  this is where the storage primitives are bridged to onwards' orchestration
  loop.
- The **transition function**: given a completed step's output, return
  `NextAction::AppendStep(...)` | `Complete(...)` | `Fail(...)`. This is the
  only place that knows about Open Responses tool-call semantics and the dwctl
  tool registry.
- The **assembly function**: walk the chain for a `request_id` and materialize
  the final `Response` JSON object returned by `GET /v1/responses/{id}`.
- The implementation of `fusillade::RequestProcessor` that, for endpoints
  matching `/v1/responses`, calls onwards' `run_response_loop`. For all other
  endpoints, delegates to the default fire-and-store behavior.
- The SIGTERM drain handler in the control-layer pod: marks the local daemon
  registration as dead and explicit-unclaims any `processing` rows owned by
  this instance so the fusillade daemon picks them up immediately.

**Does not own:**
- The orchestration loop itself (lives in onwards as a free function).
- Direct SQL against `response_steps` (goes through fusillade's
  `ResponseStepStore`).

### onwards — the loop and the trait

**Owns:**
- An extended `ResponseStore` trait with new methods for step CRUD, transition,
  and assembly. Existing methods (`store`, `get_context`, etc.) remain.
- The free function `run_response_loop<S: ResponseStore>(store: &S,
  request_id: RequestId, resume_from: Option<StepId>) -> Result<()>`. This is
  the single source of orchestration truth, called both by:
  - The `POST /v1/responses` strict-router handler (warm path, streaming).
  - The dwctl-provided `RequestProcessor::process` impl when fusillade picks up
    a row (cold path, after onwards crash).
- Stream event projection: turning a step's `response_payload` into the right
  sequence of `response.output_item.added` / `response.output_text.delta` /
  `response.output_item.done` / `response.completed` events.
- `Last-Event-ID` resume support: encoding `step_sequence` as the cursor and
  replaying events for steps strictly after the cursor on reconnect.

**Does not own:**
- Storage. All persistence goes through the `ResponseStore` trait.
- The transition function or assembly logic — these live in dwctl's trait
  impl.
- Knowledge of fusillade's daemon, claim queries, or recovery mechanism.

## Detailed Flows

### Flow 1: Priority response, streaming, no crash

1. Client sends `POST /v1/responses` with `stream: true`.
2. dwctl's onwards request handler creates a parent row in `fusillade.requests`
   (template + single-row batch wrapper, exactly as today's
   `create_batch_of_1`), with `state='processing'` and `daemon_id` set to the
   onwards-instance ID.
3. Onwards opens an SSE stream and calls `run_response_loop`.
4. The loop calls `ResponseStore::next_action_for(request_id, last_step:
   None)`. Dwctl's transition function returns `AppendStep(model_call, body)`.
5. Loop creates step 1 (`model_call`) in `response_steps` via
   `ResponseStore::record_step`, fires the upstream model, persists
   `response_payload` via `mark_step_complete`. Streams
   `response.output_item.added/delta/done` events from the step result.
6. Loop calls `next_action_for(request_id, last_step: 1)`. Transition function
   inspects step 1's response, sees `tool_calls`, returns
   `AppendStep(tool_call, args)` for each.
7. Loop fans out the tool calls: inserts a `tool_call` step row per `tool_call`
   (atomically, ordered linearly via `prev_step_id` for transcript stability),
   then executes them concurrently via `futures::future::join_all`. Each
   future invokes `HttpToolExecutor` (which writes to `tool_call_analytics`
   with `response_step_id` set), persists the result, and emits
   `function_call_output` events as each tool finishes. See "Parallelism
   within a Response" for the mechanics.
8. Loop continues — once all sibling tool steps reach a terminal state, the
   transition function emits the next `model_call` step (which is necessarily
   serial because it depends on the tool outputs).
9. Loop continues until `next_action_for` returns `Complete`.
10. Loop sets parent `requests` row to `state='completed'`, emits
    `response.completed` SSE event, closes stream.

### Flow 2: Priority response, onwards crashes mid-loop

1. State of the world: steps 1 and 2 in `response_steps` are
   `state='completed'`. Steps 3a, 3b, 3c are three parallel `tool_call`s the
   model emitted from step 2; the worker had inserted all three rows and was
   firing them concurrently. 3a is `completed`, 3b is `processing`, 3c is
   `pending` (worker died before transitioning it). The parent `requests` row
   is `state='processing'` claimed by onwards.
2. Control-layer pod receives SIGTERM (deploy / scale-down). Drain handler:
   - Sets the in-process drain flag so no new responses are claimed.
   - Waits for in-flight upstream HTTP calls to finish (best-effort, within
     `terminationGracePeriodSeconds`).
   - UPDATEs `fusillade.daemons` to mark this daemon dead.
   - UPDATEs `fusillade.requests` to release any claimed rows owned by this
     daemon back to `state='pending'`.
3. Fusillade's batch daemon (in the fusillade pod) claims the parent
   `requests` row on its next claim cycle.
4. Daemon's per-row processor (the dwctl-provided `RequestProcessor::process`)
   sees `endpoint = /v1/responses` and calls `run_response_loop` with
   `resume_from: latest completed leaf`.
5. Loop walks the chain, identifies the frontier per step state:
   - `completed` steps: use their `response_payload`.
   - `processing` steps (3b): re-fire. The per-step row was already inserted,
     so `UPDATE … SET retry_attempt = retry_attempt + 1, started_at = NOW()`
     and re-execute. Duplicate upstream invocation is the cost; upstream
     `Idempotency-Key` propagation is the documented future mitigation.
   - `pending` steps (3c): fire as if fresh.
6. Loop fans 3b and 3c out concurrently again (parallelism preserved across
   handoff). Once all of 3a/3b/3c reach a terminal state, the transition
   function emits the next model_call step. Loop continues to completion.
7. Reconnecting clients fetch `GET /v1/responses/{id}` for status, or
   reconnect the SSE stream with `Last-Event-ID: <step_sequence>` and resume.

### Flow 3: Flex-tier response

1. Client sends `POST /v1/responses` with `service_tier: flex`.
2. Onwards creates the parent row in `state='pending'` (no claim by onwards).
   Returns `response_id` to the client immediately.
3. Fusillade's batch daemon claims the row on its next cycle, prioritized by
   SLA-weighted ordering against any other queued work.
4. Daemon runs `run_response_loop` from scratch (no completed steps yet),
   identical mechanics to Flow 1, but on the daemon side.
5. Client polls `GET /v1/responses/{id}` (or reconnects the stream once the
   daemon emits events to a NOTIFY channel — see "Streaming under handoff"
   below).

### Flow 4: Batch of response requests

1. User uploads a JSONL file of `/v1/responses` request bodies (with optional
   `tools` field).
2. Standard fusillade batch ingestion: one `request_template` per line, one
   `batch` row, N `requests` rows referencing the templates.
3. Daemon claims rows in SLA-weighted order. For each claimed row: per-row
   processor sees `endpoint = /v1/responses` and runs the full multi-step
   loop, identical to Flow 3 but at scale.
4. Each batched response is independently retrievable via `GET
   /v1/responses/{id}` while the batch is in progress, or via the standard
   batch results file when the batch completes.
5. Step-level analytics (`response_steps`, `tool_call_analytics`,
   `http_analytics`) are populated per row, queryable across the entire batch.

## Parallelism within a Response

The parent `requests` row's lease (held by either onwards inline or a fusillade
daemon worker) provides serialized access to the entire response. Within the
worker holding that lease, multiple `response_steps` may execute concurrently
without cross-worker coordination. This is the mechanism that lets a single
model response with N independent `tool_call`s fan out N upstream tool
invocations simultaneously.

```rust
// Inside run_response_loop, after a model_call step returns N tool_calls:
let tool_steps = transition.fan_out_tool_calls(parent_step_id, &tool_calls);
//   ^ inserts N tool_call rows in 'pending' atomically. The UNIQUE
//     (request_id, parent_step_id, prev_step_id, step_kind) constraint
//     plus an existence-check on chain walk ensures crash-recovery
//     re-entry does not produce duplicates.

let results = futures::future::join_all(
    tool_steps.iter().map(|step| async {
        store.mark_step_processing(step.id).await?;
        let result = http_tool_executor.execute(&step.request_payload).await;
        match result {
            Ok(payload) => store.mark_step_complete(step.id, payload).await,
            Err(e)      => store.mark_step_failed(step.id, e).await,
        }
    })
).await;

// Once all N tool steps are terminal, transition fn produces the next
// model_call step (which is necessarily serial — it depends on the tool
// outputs).
```

Per-step retry (`retry_attempt`, `error`) lives inside the worker's logic and
does not require daemon-level claim cycles. A transient 429 on a tool call is
backed off and retried in-process; only on permanent failure does the step
move to `state='failed'` and the transition function decide whether the parent
response continues, fails, or is partially recoverable.

Total upstream concurrency for a response is bounded by the model's emitted
fan-out width times any per-tool-source concurrency caps (the latter is C9
future work). Across responses, the daemon's worker pool size sets the global
concurrency ceiling.

## Nested Sub-agents

A tool that itself spawns an agent loop (e.g., a `delegate_to_subagent` tool
calling out to a more capable model with its own toolset) records its
sub-steps under the same top-level `request_id` as the parent response. The
nesting is expressed in the schema by `parent_step_id`:

- `parent_step_id IS NULL`: top-level step in the user-visible response. These
  steps' outputs become the items in the `output: [...]` array returned by
  `GET /v1/responses/{id}` and the events surfaced in the user's SSE stream.
- `parent_step_id = X`: step lives inside the sub-loop spawned by step X
  (necessarily a `tool_call` step pointing at a sub-agent-style tool).
  `prev_step_id` chains within this sub-loop; the chain restarts at each
  nesting level.

`step_sequence` remains monotonic per `request_id` globally (across all
nesting levels) so it stays a valid `Last-Event-ID` cursor over the
user-visible event stream — sub-agent step numbers are interleaved into the
sequence but their events are not emitted at the top level by default.

### Recursion in `run_response_loop`

When the loop encounters a `tool_call` step whose tool is registered as a
sub-agent (e.g., `tool_sources.kind = 'agent'`, a follow-up addition to the
tool registry), instead of dispatching to `HttpToolExecutor.execute()` it
recurses:

```rust
match step.step_kind {
    StepKind::ToolCall if tool.is_agent() => {
        // Sub-loop runs under the same request_id; every step it inserts
        // gets parent_step_id = step.id. When it returns, the sub-agent's
        // final aggregated output is set as this tool_call step's response.
        let sub_output = run_response_loop(
            store,
            request_id,
            parent_step_id: Some(step.id),
            initial_input: tool.spawn_input(&step.request_payload),
        ).await?;
        store.mark_step_complete(step.id, sub_output).await?;
    }
    StepKind::ToolCall => { /* existing HttpToolExecutor path */ }
    StepKind::ModelCall => { /* existing model fire path */ }
}
```

Recursion uses the same loop function, the same parallelism mechanics
(multiple sub-agents from a single fan-out execute concurrently), and the
same crash-recovery semantics (parent-row lease covers all nesting depths).

Two independent safety caps bound the work (see C11):

- `max_response_step_depth` — caps recursion depth. Default e.g. 8.
- `max_response_iterations` — caps per-loop-level iterations. Default 10
  (matching today's onwards behavior).

When a sub-agent's recursion would exceed `max_response_step_depth`, the
spawning tool's step is failed with `max_depth_exceeded` rather than the
sub-loop being entered. When a single loop level exceeds
`max_response_iterations`, that level's terminal `model_call` step fails
with `max_iterations_exceeded`. The orchestrator receives either as a
normal tool/step failure and decides how to proceed. Neither is a hard
architectural constraint on the recursion tree's shape — tool authors
shape their sub-agents' recursion through prompting and the toolsets they
expose to each layer.

### What surfaces where

| Surface | Default behaviour |
|---|---|
| `GET /v1/responses/{id}` `output: [...]` | Top-level steps only (`parent_step_id IS NULL`). Sub-agent calls appear as `function_call` + `function_call_output` items. |
| User SSE stream | Top-level events only. Sub-agent token deltas and intermediate reasoning are not emitted at the user level. |
| Dashboard timeline | Top-level steps with expandable per-step drill-down. Tool-call rows that have nested rows render an inline sub-agent tree. |
| Analytics queries | All steps queryable across nesting depth. `tool_call_analytics` and `http_analytics` are populated for every step regardless of level. |

This matches the surfacing pattern of OpenAI's hosted tools (web_search,
code_interpreter): the call and its output are first-class items in the
user's response, but the tool's internal mechanics are not exposed in the API
contract.

### What ships with v1

Sub-agent recursion is part of v1, not deferred. Specifically:

- **`tool_sources.kind`** — new column on the dwctl tool registry
  distinguishing standard HTTP tools (`'http'`, default) from sub-agents
  (`'agent'`). Migration is additive.
- **Recursion in `run_response_loop`** — when the loop encounters a
  `tool_call` step whose tool is `kind='agent'`, it recurses with the
  current step's id as the sub-loop's `parent_step_id` (instead of
  invoking `HttpToolExecutor`).
- **Dwctl admin endpoints** for registering and managing sub-agent tools
  (extension of the existing `/tool-sources` CRUD).
- **API endpoint to fetch the full step tree** — `GET
  /v1/responses/{id}/steps` returns the recursive nested tree of steps
  for a response (Doubleword extension; not OpenAI-compatible). The
  standard `GET /v1/responses/{id}` continues to return only top-level
  output items per the OpenAI spec.
- **Dashboard tree view** — the per-step timeline renders nested
  sub-agent steps as a recursive tree, collapsed by default at each
  level, expandable to drill into any sub-loop.

What does *not* ship in v1: live event streaming as sub-agents progress.
The dashboard reflects sub-agent activity by polling
`/v1/responses/{id}/steps`; live tailing via `LISTEN/NOTIFY` is the
documented follow-up (see Out of Scope).

## Streaming under Executor Handoff

Stream events are not persisted as a separate ledger. Each step's
`response_payload` is sufficient to deterministically project the full SSE
event sequence for that step (`output_item.added` / `output_text.delta` /
`output_item.done`).

`step_sequence` (a monotonic BIGINT per `request_id`) serves as the
`Last-Event-ID` cursor. On reconnect:

1. Client sends `GET /v1/responses/{id}?stream=true` with `Last-Event-ID:
   <step_sequence>` header.
2. Onwards walks `response_steps` for `request_id` where
   `step_sequence > cursor` and re-emits all events for those steps from their
   stored payloads.
3. Once the projection catches up, onwards polls `list_chain` (or queries
   for new step rows since the last seen `step_sequence`) on a short
   interval until the parent row reaches a terminal state.

For v1, polling is sufficient. The original (pre-disconnect) stream has no
polling cost because the producer (onwards inline) and consumer (the SSE
handler) are the same process — events are emitted directly as steps
complete. Polling only matters on reconnect after executor handoff, where
the daemon worker is now the producer. A `LISTEN/NOTIFY`-based live-tailing
upgrade is a documented follow-up area (see Out of Scope) that would lower
reconnect latency and unlock real-time nested progress events without
changing the on-the-wire contract.

## Dashboard Changes

| Change | Path |
|---|---|
| Rename `AsyncRequests.tsx` → `Responses.tsx` | `dashboard/src/components/features/async-requests/` → `dashboard/src/components/features/responses/` |
| New unified `RequestDetail.tsx` | drill-down route shared by batch and response IDs; LEFT JOINs `response_steps` to render the timeline section (empty for batch rows, populated for responses) |
| Per-step timeline component | renders one row per `response_steps` row: kind icon, tool name (if applicable), duration (`completed_at - started_at`), tokens (extracted from `response_payload` JSON path), status badge, expandable raw JSON, link to the `http_analytics` row via `response_step_id` |

The `Responses.tsx` list view continues to query `fusillade.requests` filtered
by endpoint; no schema discriminator is needed because response parents are
distinguishable by their `endpoint` value.

## Benefits

### Architectural

- **One daemon, one claim path, one orchestration loop** — no parallel
  infrastructure to maintain.
- **Clean three-way separation**: fusillade is pure storage + scheduling, dwctl
  owns domain logic, onwards owns the loop and the trait. Each can be tested
  and reasoned about in isolation.
- **No breaking changes** to the published `fusillade` crate's existing API.
  All extensions are additive: a new trait (`ResponseStepStore`), a new hook
  trait (`RequestProcessor` with a default impl), a new table.
- **No regression risk** to the existing batch path. Batch requests continue to
  flow through the same claim query, the same SLA-weighted ordering, and the
  same default `RequestProcessor` impl.

### Operational

- **Fast handoff**: SIGTERM unclaim makes recovery effectively instant for
  graceful shutdowns; the existing 30-second daemon-dead detection covers hard
  crashes. The 4-hour `processing_timeout_ms` is a true edge-case fallback.
- **No new daemon process or pod** — recovery work runs on the existing
  fusillade pod's daemon worker pool.
- **Observability falls out for free**: every upstream HTTP call (model or
  tool) goes through outlet middleware, which writes to `http_analytics` with
  `response_step_id` set; tool calls also write to `tool_call_analytics`. No
  separate tracing infrastructure.

### Product

- **Resumable agentic responses** become a first-class feature for both
  priority and flex tiers without client-side complexity.
- **Batches of agentic responses** emerge for free: a JSONL file of
  `/v1/responses` requests, each with their own tool loop, all running through
  the existing batch infrastructure.
- **Per-step drill-down** in the dashboard becomes possible without any new
  data model — the timeline is a join from `requests` to `response_steps`.
- **Stream resumption** via `Last-Event-ID` matches the OpenAI Responses API
  contract, so existing client SDKs that support it work without
  customization.

### Future-proofing

- **Idempotency-Key dedup** for real-time `/chat/completions` and `/embeddings`
  becomes a small additive feature using the same persistence primitives.
- **Per-tool-source concurrency limits** can be added later as a constraint on
  daemon worker scheduling without touching the orchestration loop.
- **Sub-agent recursion** ships behind a single configurable `max_depth` cap
  with no protocol changes — only a new `tool_sources.kind = 'agent'` registry
  entry and recursion in `run_response_loop`.
- **Live nested progress events and faster reconnect** become a forward-
  compatible upgrade — the schema and step-as-event projection already
  support it; only the cross-pod live-tailing transport is deferred.

## Out of Scope

The following items are intentionally not part of this plan and would be
addressed in follow-ups if and when they prove necessary:

- **`Idempotency-Key` support on real-time APIs.** The persistence primitives
  introduced here would make this straightforward, but it is independent of
  multi-step responses and has its own design questions (TTL, key scope,
  failure semantics).
- **Per-tool-source concurrency limits.** Today, all tool calls fire through
  `HttpToolExecutor` with no central concurrency cap. High-volume batch
  workloads may need this; it can be added by extending the daemon's
  scheduling logic.
- **`LISTEN/NOTIFY`-based live event tailing across executor handoff.**
  Polling-based reconnect is sufficient for v1; the dashboard's tree view
  reflects sub-agent activity via periodic polling of
  `/v1/responses/{id}/steps` rather than push events. NOTIFY would lower
  reconnect latency and unlock a live nested-progress event vocabulary
  (`response.progress.step_started` / `step_completed` / `tree_snapshot`,
  with an opt-in `progress=true` stream parameter) so dashboards and
  custom clients can render the live activity tree as work happens at any
  depth. Standalone exploration area; design notes in this plan history.
- **Mid-execution visibility for the orchestrator model.** The orchestrator
  LLM does not see sub-agent or tool-call progress until the call returns.
  If a use case demands it, a `check_subagent_status` tool reading from
  `response_steps` is the cheapest first step; tool-result streaming into
  the model's context is a much larger separate problem.
- **Upstream `Idempotency-Key` propagation** to handle the rare case where
  onwards completes an upstream model call but crashes before persisting the
  result, causing the daemon to re-fire the call. The `UNIQUE` constraint on
  `(request_id, parent_step_id, prev_step_id, step_kind)` prevents duplicate
  step rows; this follow-up addresses duplicate upstream charges.
- **Sub-token-level stream resumption.** Reconnect resumes at step boundaries;
  a client may see a few duplicate token deltas at worst.

## Implementation Order

The work splits cleanly into four review-sized changes, in dependency order:

1. **fusillade**: migration (table + indexes), `ResponseStepStore` trait,
   `PostgresResponseStepManager`, `RequestProcessor` hook with default impl
   matching today's behavior. No daemon framework changes; the new code is
   reachable only via the new hook. Released as a minor version bump.

2. **onwards**: extended `ResponseStore` trait with step CRUD + transition +
   assembly methods (additive, default impls return errors so existing
   consumers compile). `run_response_loop` free function with parallel
   fan-out via `join_all`, recursion entry for sub-agent tool kinds, and
   both safety caps (`max_response_step_depth`, `max_response_iterations`).
   Streaming event projection (standard Responses API events).
   `Last-Event-ID` step_sequence resumption via polling on reconnect.
   Released as a minor version bump.

3. **dwctl** (Cargo bump for fusillade + onwards): extend
   `FusilladeResponseStore` to implement onwards' new trait methods over
   `PostgresResponseStepManager`. Implement `RequestProcessor::process` to
   dispatch on endpoint. Implement transition function (handles both
   regular tool calls and sub-agent recursion) and assembly logic. Wire
   SIGTERM drain handler. Add analytics columns (`response_step_id` on
   `http_analytics` and `tool_call_analytics`). Migration: add
   `tool_sources.kind` column. Extend `/tool-sources` admin endpoints to
   accept the new column. Add `GET /v1/responses/{id}/steps` endpoint
   returning the recursive step tree. Migrate `create_batch_of_1` to
   support multi-step. Surface both safety caps
   (`max_response_step_depth`, `max_response_iterations`) as config knobs.

4. **dashboard**: rename `AsyncRequests.tsx` → `Responses.tsx`. Build
   unified `RequestDetail.tsx` with the per-step timeline section,
   including a recursive sub-agent tree view (collapsed by default at each
   nesting level, expandable to drill into any sub-loop). Tree data
   sourced via polling `GET /v1/responses/{id}/steps`. Wire the new
   `response_step_id` analytics drill-downs.

## Initial Tool Registry

Once v1 ships, these tools are the first set we want registered in the
`tool_sources` registry so that customers immediately have a useful baseline
of capabilities for their orchestrators and sub-agents. Each is listed
here for traceability and as targets for follow-up registration work — they
are *not* part of the platform build itself.

| Tool | Kind | Notes |
|---|---|---|
| `web_search` | `http` | Search the live web; returns ranked results with snippets |
| `view_webpage` | `http` | Fetch and clean a webpage's contents into model-readable text |
| `create_subagent` | `agent` | Spawn a nested response loop with its own model + toolset (the canonical sub-agent dispatch tool, exercises the recursion path) |
| `create_batch` | `http` | Submit a JSONL file as a Doubleword batch and return a batch_id; lets a sub-agent itself fan out work to the batch tier |
| `github_*` | `http` | GitHub API surface — likely several discrete tools (read PR, post review comment, fetch file, list checks) to support a full PR-review flow |

The `create_subagent` tool is the one that exercises the recursion code
path on day one and validates the depth/iteration safety caps end-to-end.
The GitHub tool family will likely need its own scoping decisions (one
generic `github_request` tool vs. several narrowly-typed tools) once we
start designing the PR-review workflow concretely.

## Backstory - Pete's thoughts:

```
Developer
    │
    ▼
┌─────────────────────────────────┐
│  Primary Agent (realtime)       │
│  Claude Code / Codex / Cursor   │
│  Fast, interactive responses    │
└─────────┬───────────────────────┘
          │ spawns sub-agents
          ▼
┌─────────────────────────────────────────────────┐
│  Sub-Agent Pool (async/batch via Doubleword)    │
│                                                 │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐          │
│  │test-1│ │test-2│ │test-3│ │test-N│  ...×100  │
│  └──────┘ └──────┘ └──────┘ └──────┘          │
│                                                 │
│  Each sub-agent hits Doubleword /ai/v1/*        │
│  Routed to async tier (~10min, 50% cheaper)     │
│  or batch tier (24h, 75% cheaper)               │
└─────────────────────┬───────────────────────────┘
                      │ results stream back
                      ▼
┌─────────────────────────────────┐
│  Primary Agent aggregates       │
│  results and continues          │
└─────────────────────────────────┘
```

