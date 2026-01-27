//! Main traits for the batching system (backwards compatibility re-exports).
//!
//! This module re-exports from `crate::storage` for backwards compatibility.
//! New code should use `crate::storage` directly.

// Re-export everything from storage for backwards compatibility
pub use crate::storage::*;

#[cfg(feature = "postgres")]
pub use crate::storage::postgres;
