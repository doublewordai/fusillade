//! Daemon lifecycle record types and transitions.

pub mod config;
pub mod transitions;
pub mod types;

pub use config::{DaemonConfig, ModelEscalationConfig, ShouldRetryFn, default_should_retry};
pub use types::{
    AnyDaemonRecord, DaemonData, DaemonRecord, DaemonState, DaemonStats, DaemonStatus, Dead,
    Initializing, Running, get_hostname, get_pid, get_version,
};
