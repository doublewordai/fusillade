//! Metrics for fusillade daemon monitoring.
//!
//! Fusillade uses the `metrics` facade crate for all metrics.
//! The actual metrics backend (Prometheus, StatsD, etc.) is configured by the application
//! that uses fusillade by installing a global recorder.
//!
//! ## Metrics Emitted
//!
//! ### Counters
//! - `fusillade_claims_total` - Total number of batch claims by the daemon
//! - `fusillade_requests_total{model, endpoint, status}` - Request completions by status (completed/failed/canceled)
//! - `fusillade_request_retries_total{model, endpoint, reason}` - Request retry attempts
//! - `fusillade_sla_escalations_total{model, endpoint, sla_name}` - SLA-triggered escalations
//!
//! ### Gauges
//! - `fusillade_requests_in_flight{model, endpoint}` - Current number of in-flight requests
//! - `fusillade_sla_at_risk_requests{model, endpoint, sla_name}` - Requests at risk of missing SLA
//!
//! ### Histograms
//! - `fusillade_request_duration_seconds{model, endpoint}` - Request processing duration
//!
//! All metrics are emitted using the `metrics` crate macros (e.g., `counter!`, `gauge!`, `histogram!`)
//! directly in the code where they occur.
