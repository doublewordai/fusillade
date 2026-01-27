//! Request aggregate - domain model and state transitions.
//!
//! This module contains the core domain logic for requests:
//! - Request types and states (typestate pattern)
//! - State transition methods
//! - Value objects (RequestData, etc.)

pub mod state;
pub mod transitions;

// Re-export commonly used types
pub use state::*;
pub use transitions::CancellationReason;
