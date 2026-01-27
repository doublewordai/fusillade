//! Core domain types for the fusillade batching system.
//!
//! This module contains pure domain types with no persistence dependencies:
//! - Files and request templates
//! - Batches and batch status
//! - Request typestate machine
//! - Daemon typestate machine

pub mod batch;
pub mod daemon;
pub mod file;
pub mod request;

// Re-export commonly used types from each submodule
// Note: We don't use glob re-exports to avoid module name collisions
// (e.g., both daemon and request have `state` and `transitions` submodules)
