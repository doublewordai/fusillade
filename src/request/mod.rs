//! Request aggregate - domain model and state transitions.
//!
//! This module re-exports types from `domain::request`.
//! See that module for the actual implementations.

// Re-export all request types and transitions
pub use crate::domain::request::{
    state::*, transitions::CancellationReason, transitions::RetryConfig,
};
