//! Metrics for fusillade daemon monitoring.
//!
//! This module provides metrics recording for the fusillade daemon using the `metrics` facade.
//! Metrics include request throughput, SLA monitoring, and retry tracking.
//!
//! The actual metrics backend (Prometheus, StatsD, etc.) is configured by the application
//! that uses fusillade.

use std::time::Duration;

/// Set the current number of requests in flight for a model/endpoint.
#[cfg(feature = "metrics")]
pub fn set_requests_in_flight(model: &str, endpoint: &str, count: usize) {
    metrics::gauge!("fusillade_requests_in_flight", "model" => model.to_string(), "endpoint" => endpoint.to_string())
        .set(count as f64);
}

/// Record a successfully completed request.
#[cfg(feature = "metrics")]
pub fn record_request_completed(model: &str, endpoint: &str, duration: Duration) {
    metrics::counter!("fusillade_requests_total", "model" => model.to_string(), "endpoint" => endpoint.to_string(), "status" => "completed")
        .increment(1);

    metrics::histogram!("fusillade_request_duration_seconds", "model" => model.to_string(), "endpoint" => endpoint.to_string())
        .record(duration.as_secs_f64());
}

/// Record a failed request.
#[cfg(feature = "metrics")]
pub fn record_request_failed(model: &str, endpoint: &str, duration: Duration) {
    metrics::counter!("fusillade_requests_total", "model" => model.to_string(), "endpoint" => endpoint.to_string(), "status" => "failed")
        .increment(1);

    metrics::histogram!("fusillade_request_duration_seconds", "model" => model.to_string(), "endpoint" => endpoint.to_string())
        .record(duration.as_secs_f64());
}

/// Record a canceled request.
#[cfg(feature = "metrics")]
pub fn record_request_canceled(model: &str, endpoint: &str) {
    metrics::counter!("fusillade_requests_total", "model" => model.to_string(), "endpoint" => endpoint.to_string(), "status" => "canceled")
        .increment(1);
}

/// Set the number of requests at risk of missing SLA.
#[cfg(feature = "metrics")]
pub fn record_sla_at_risk(model: &str, endpoint: &str, sla_name: &str, count: usize) {
    metrics::gauge!("fusillade_sla_at_risk_requests", "model" => model.to_string(), "endpoint" => endpoint.to_string(), "sla_name" => sla_name.to_string())
        .set(count as f64);
}

/// Record an SLA escalation.
#[cfg(feature = "metrics")]
pub fn record_sla_escalation(model: &str, endpoint: &str, sla_name: &str) {
    metrics::counter!("fusillade_sla_escalations_total", "model" => model.to_string(), "endpoint" => endpoint.to_string(), "sla_name" => sla_name.to_string())
        .increment(1);
}

/// Record a request retry.
#[cfg(feature = "metrics")]
pub fn record_retry(model: &str, endpoint: &str, reason: &str) {
    metrics::counter!("fusillade_request_retries_total", "model" => model.to_string(), "endpoint" => endpoint.to_string(), "reason" => reason.to_string())
        .increment(1);
}

// No-op implementations when metrics feature is disabled
#[cfg(not(feature = "metrics"))]
pub fn set_requests_in_flight(_model: &str, _endpoint: &str, _count: usize) {}

#[cfg(not(feature = "metrics"))]
pub fn record_request_completed(_model: &str, _endpoint: &str, _duration: Duration) {}

#[cfg(not(feature = "metrics"))]
pub fn record_request_failed(_model: &str, _endpoint: &str, _duration: Duration) {}

#[cfg(not(feature = "metrics"))]
pub fn record_request_canceled(_model: &str, _endpoint: &str) {}

#[cfg(not(feature = "metrics"))]
pub fn record_sla_at_risk(_model: &str, _endpoint: &str, _sla_name: &str, _count: usize) {}

#[cfg(not(feature = "metrics"))]
pub fn record_sla_escalation(_model: &str, _endpoint: &str, _sla_name: &str) {}

#[cfg(not(feature = "metrics"))]
pub fn record_retry(_model: &str, _endpoint: &str, _reason: &str) {}
