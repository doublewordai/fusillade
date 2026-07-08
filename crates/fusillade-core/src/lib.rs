//! Core domain types and storage traits for Fusillade.
//!
//! This crate contains the stable type universe shared by storage
//! implementations and the scheduling daemon.

pub mod batch;
pub mod daemon;
pub mod error;
pub mod http;
pub mod manager;
pub mod processor;
pub mod request;
pub mod response_step;
pub mod transform;

pub use batch::*;
pub use daemon::{
    AnyDaemonRecord, DaemonConfig, DaemonData, DaemonRecord, DaemonState, DaemonStats,
    DaemonStatus, Dead, Initializing, ModelEscalationConfig, Running, ShouldRetryFn,
    default_should_retry, get_hostname, get_pid, get_version,
};
pub use error::{FusilladeError, Result};
pub use http::{
    HttpClient, HttpResponse, MockHttpClient, ReqwestHttpClient, StreamEvent, StreamEventCallback,
    StreamReassembler,
};
pub use manager::{DaemonStorage, ModelFilter, ModelFilterState, Storage};
pub use processor::{CancellationFuture, DefaultRequestProcessor, RequestProcessor, ShouldRetry};
pub use request::*;
pub use response_step::{
    CreateStepInput, ResponseStep, ResponseStepStore, StepId, StepKind, StepState,
};
pub use transform::ResponseTransformer;
