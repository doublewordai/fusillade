//! Response step storage primitives for multi-step Open Responses orchestration.
//!
//! A response step is a discrete unit of work inside a multi-step response: a
//! single upstream model call or tool call. Steps are linked into a linear chain
//! via `prev_step_id` and may be nested under a `parent_step_id` to express
//! sub-agent recursion. The orchestration loop (in `onwards`) decides what the
//! next step is given the chain so far; the storage layer here is purely
//! infrastructure — no Open Responses domain knowledge.
//!
//! See `docs/plans/2026-04-28-multi-step-responses.md`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Result;
use crate::request::RequestId;

/// Identifier of a single response step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StepId(pub Uuid);

impl std::fmt::Display for StepId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0.to_string()[..8])
    }
}

impl From<Uuid> for StepId {
    fn from(uuid: Uuid) -> Self {
        StepId(uuid)
    }
}

impl std::ops::Deref for StepId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Discrete kinds of work a response step can represent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    /// Upstream LLM invocation.
    ModelCall,
    /// Server-side tool invocation (HTTP tool or sub-agent dispatch).
    ToolCall,
}

impl StepKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            StepKind::ModelCall => "model_call",
            StepKind::ToolCall => "tool_call",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "model_call" => Some(StepKind::ModelCall),
            "tool_call" => Some(StepKind::ToolCall),
            _ => None,
        }
    }
}

/// Lifecycle state of a step. Mirrors `requests.state` values exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepState {
    Pending,
    Processing,
    Completed,
    Failed,
    Canceled,
}

impl StepState {
    pub fn as_str(&self) -> &'static str {
        match self {
            StepState::Pending => "pending",
            StepState::Processing => "processing",
            StepState::Completed => "completed",
            StepState::Failed => "failed",
            StepState::Canceled => "canceled",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(StepState::Pending),
            "processing" => Some(StepState::Processing),
            "completed" => Some(StepState::Completed),
            "failed" => Some(StepState::Failed),
            "canceled" => Some(StepState::Canceled),
            _ => None,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            StepState::Completed | StepState::Failed | StepState::Canceled
        )
    }
}

/// A row from the `response_steps` table.
#[derive(Debug, Clone, Serialize)]
pub struct ResponseStep {
    pub id: StepId,
    pub request_id: RequestId,
    pub prev_step_id: Option<StepId>,
    pub parent_step_id: Option<StepId>,
    pub step_kind: StepKind,
    pub step_sequence: i64,
    pub request_payload: serde_json::Value,
    pub response_payload: Option<serde_json::Value>,
    pub state: StepState,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub canceled_at: Option<DateTime<Utc>>,
    pub retry_attempt: i32,
    pub error: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Input to [`ResponseStepStore::create_step`].
///
/// `step_sequence` is monotonic per `request_id` across all nesting levels and
/// doubles as the `Last-Event-ID` cursor for top-level events. Callers
/// (the orchestration loop) are responsible for picking the next sequence
/// value — the storage layer does not enforce monotonicity beyond what the
/// `UNIQUE (request_id, parent_step_id, prev_step_id, step_kind)` constraint
/// implicitly provides.
#[derive(Debug, Clone)]
pub struct CreateStepInput {
    /// Optional pre-generated step UUID. When `Some`, becomes the step's
    /// primary key — useful when the caller needs to reference the id
    /// before the row is committed (e.g., emitting an SSE event with the
    /// step id while the row is still being inserted).
    pub id: Option<Uuid>,
    pub request_id: RequestId,
    pub prev_step_id: Option<StepId>,
    pub parent_step_id: Option<StepId>,
    pub step_kind: StepKind,
    pub step_sequence: i64,
    pub request_payload: serde_json::Value,
}

/// Storage trait for response step persistence.
///
/// Mirrors the shape of [`crate::Storage`] for requests but scoped to the
/// `response_steps` table. Implementations must be safe to call from multiple
/// concurrent tasks within a single worker (tool fan-out within a single
/// response holds the parent request lease, so no cross-worker coordination
/// is required).
#[async_trait]
pub trait ResponseStepStore: Send + Sync {
    /// Insert a new step in `pending` state.
    ///
    /// The `UNIQUE (request_id, parent_step_id, prev_step_id, step_kind)`
    /// constraint is the idempotency safety net: a re-running transition
    /// function under crash recovery will not produce duplicate successor
    /// rows. Implementations should surface that as a typed conflict so
    /// callers can fall back to reading the existing row.
    async fn create_step(&self, input: CreateStepInput) -> Result<StepId>;

    /// Fetch a single step by id. Returns `None` if not present.
    async fn get_step(&self, id: StepId) -> Result<Option<ResponseStep>>;

    /// List every step for a request, ordered by `step_sequence`.
    ///
    /// Includes both top-level and nested (sub-agent) steps. Callers that
    /// only want the user-visible chain should filter on
    /// `parent_step_id IS NULL`.
    async fn list_chain(&self, request_id: RequestId) -> Result<Vec<ResponseStep>>;

    /// List steps inside a specific scope ordered by `step_sequence`.
    ///
    /// `scope_parent` selects the chain: `None` = the top-level chain
    /// (user-visible response), `Some(step_id)` = the sub-loop spawned by
    /// that tool_call step.
    async fn list_scope(
        &self,
        request_id: RequestId,
        scope_parent: Option<StepId>,
    ) -> Result<Vec<ResponseStep>>;

    /// Mark a `pending` step as `processing`, recording `started_at`.
    ///
    /// Idempotent: if the step is already in a non-pending state the call
    /// returns `Ok(())` without modifying the row, so crash recovery can
    /// resume safely.
    async fn mark_step_processing(&self, id: StepId) -> Result<()>;

    /// Mark a step as `completed` with the given `response_payload`.
    async fn complete_step(&self, id: StepId, response: serde_json::Value) -> Result<()>;

    /// Mark a step as `failed` with the given structured error payload.
    async fn fail_step(&self, id: StepId, error: serde_json::Value) -> Result<()>;

    /// Mark a step as `canceled`.
    async fn cancel_step(&self, id: StepId) -> Result<()>;

    /// Increment a step's `retry_attempt` and reset it to `pending` for re-
    /// firing under crash recovery. Used when a worker picks up a step
    /// that was left in `processing` by a dead worker.
    async fn requeue_step_for_retry(&self, id: StepId) -> Result<()>;
}
