//! Daemon state types and transitions using the typestate pattern.
//!
//! This module defines type-safe daemon lifecycle management. Each daemon
//! progresses through distinct states, enforced at compile time.

pub mod state;
pub mod transitions;

// Re-export commonly used types
pub use state::*;
