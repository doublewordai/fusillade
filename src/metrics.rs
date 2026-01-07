//! Prometheus metrics for fusillade daemon monitoring.
//!
//! This module provides Prometheus metrics exposition for the fusillade daemon,
//! including request throughput, SLA monitoring, and retry tracking.
//!
//! Metrics are organized into three categories:
//! - **Gauges**: Point-in-time state (requests in flight, at-risk requests)
//! - **Counters**: Cumulative totals (requests completed/failed, escalations, retries)
//! - **Histograms**: Distributions (request duration)
//!
//! All metrics use labels for drill-down by model, endpoint, and other dimensions.

#[cfg(feature = "metrics")]
use prometheus::{CounterVec, GaugeVec, HistogramVec, Opts, Registry};
#[cfg(feature = "metrics")]
use std::time::Duration;

#[cfg(feature = "metrics")]
use crate::error::Result;

/// Prometheus metrics registry for fusillade daemon.
///
/// Tracks request processing, SLA monitoring, and retry behavior with
/// labels for model-level and endpoint-level drill-down.
#[cfg(feature = "metrics")]
#[derive(Clone)]
pub struct FusilladeMetrics {
    registry: Registry,

    // Gauges (point-in-time state)
    requests_in_flight: GaugeVec,
    sla_at_risk_requests: GaugeVec,

    // Counters (cumulative totals)
    requests_total: CounterVec,
    sla_escalations_total: CounterVec,
    request_retries_total: CounterVec,

    // Histograms (distributions)
    request_duration_seconds: HistogramVec,
}

#[cfg(feature = "metrics")]
impl FusilladeMetrics {
    /// Create a new FusilladeMetrics instance with the given registry.
    ///
    /// Registers all metrics with the provided Prometheus registry.
    ///
    /// # Errors
    ///
    /// Returns an error if metrics fail to register (e.g., duplicate registration).
    pub fn new(registry: Registry) -> Result<Self> {
        // Gauge: requests currently being processed
        let requests_in_flight = GaugeVec::new(
            Opts::new(
                "fusillade_requests_in_flight",
                "Number of requests currently being processed",
            ),
            &["model", "endpoint"],
        )
        .map_err(|e| anyhow::anyhow!("Failed to create requests_in_flight gauge: {}", e))?;

        // Gauge: requests at risk of missing SLA
        let sla_at_risk_requests = GaugeVec::new(
            Opts::new(
                "fusillade_sla_at_risk_requests",
                "Number of requests at risk of missing SLA deadline",
            ),
            &["model", "endpoint", "sla_name"],
        )
        .map_err(|e| anyhow::anyhow!("Failed to create sla_at_risk_requests gauge: {}", e))?;

        // Counter: total requests by status
        let requests_total = CounterVec::new(
            Opts::new(
                "fusillade_requests_total",
                "Total number of requests processed by status",
            ),
            &["model", "endpoint", "status"],
        )
        .map_err(|e| anyhow::anyhow!("Failed to create requests_total counter: {}", e))?;

        // Counter: SLA escalations
        let sla_escalations_total = CounterVec::new(
            Opts::new(
                "fusillade_sla_escalations_total",
                "Total number of requests escalated due to SLA threshold",
            ),
            &["model", "endpoint", "sla_name"],
        )
        .map_err(|e| anyhow::anyhow!("Failed to create sla_escalations_total counter: {}", e))?;

        // Counter: request retries
        let request_retries_total = CounterVec::new(
            Opts::new(
                "fusillade_request_retries_total",
                "Total number of request retries by reason",
            ),
            &["model", "endpoint", "reason"],
        )
        .map_err(|e| anyhow::anyhow!("Failed to create request_retries_total counter: {}", e))?;

        // Histogram: request duration
        let request_duration_seconds = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "fusillade_request_duration_seconds",
                "Request processing duration in seconds",
            )
            .buckets(vec![
                0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
            ]),
            &["model", "endpoint"],
        )
        .map_err(|e| anyhow::anyhow!("Failed to create request_duration_seconds histogram: {}", e))?;

        // Register all metrics
        registry
            .register(Box::new(requests_in_flight.clone()))
            .map_err(|e| anyhow::anyhow!("Failed to register requests_in_flight: {}", e))?;
        registry
            .register(Box::new(sla_at_risk_requests.clone()))
            .map_err(|e| anyhow::anyhow!("Failed to register sla_at_risk_requests: {}", e))?;
        registry
            .register(Box::new(requests_total.clone()))
            .map_err(|e| anyhow::anyhow!("Failed to register requests_total: {}", e))?;
        registry
            .register(Box::new(sla_escalations_total.clone()))
            .map_err(|e| anyhow::anyhow!("Failed to register sla_escalations_total: {}", e))?;
        registry
            .register(Box::new(request_retries_total.clone()))
            .map_err(|e| anyhow::anyhow!("Failed to register request_retries_total: {}", e))?;
        registry
            .register(Box::new(request_duration_seconds.clone()))
            .map_err(|e| anyhow::anyhow!("Failed to register request_duration_seconds: {}", e))?;

        Ok(Self {
            registry,
            requests_in_flight,
            sla_at_risk_requests,
            requests_total,
            sla_escalations_total,
            request_retries_total,
            request_duration_seconds,
        })
    }

    /// Get the underlying Prometheus registry.
    ///
    /// Useful for exporting metrics via HTTP endpoint.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Set the current number of requests in flight for a model/endpoint.
    pub fn set_requests_in_flight(&self, model: &str, endpoint: &str, count: usize) {
        self.requests_in_flight
            .with_label_values(&[model, endpoint])
            .set(count as f64);
    }

    /// Record a successfully completed request.
    ///
    /// Updates both the requests_total counter and the duration histogram.
    pub fn record_request_completed(&self, model: &str, endpoint: &str, duration: Duration) {
        self.requests_total
            .with_label_values(&[model, endpoint, "completed"])
            .inc();

        self.request_duration_seconds
            .with_label_values(&[model, endpoint])
            .observe(duration.as_secs_f64());
    }

    /// Record a failed request.
    ///
    /// Updates both the requests_total counter and the duration histogram.
    pub fn record_request_failed(&self, model: &str, endpoint: &str, duration: Duration) {
        self.requests_total
            .with_label_values(&[model, endpoint, "failed"])
            .inc();

        self.request_duration_seconds
            .with_label_values(&[model, endpoint])
            .observe(duration.as_secs_f64());
    }

    /// Record a canceled request.
    pub fn record_request_canceled(&self, model: &str, endpoint: &str) {
        self.requests_total
            .with_label_values(&[model, endpoint, "canceled"])
            .inc();
    }

    /// Set the number of requests at risk of missing SLA.
    ///
    /// This is a gauge that should be updated periodically by the SLA monitoring task.
    pub fn record_sla_at_risk(&self, model: &str, endpoint: &str, sla_name: &str, count: usize) {
        self.sla_at_risk_requests
            .with_label_values(&[model, endpoint, sla_name])
            .set(count as f64);
    }

    /// Record an SLA escalation.
    ///
    /// Called when a request is escalated to a priority endpoint due to SLA threshold.
    pub fn record_sla_escalation(&self, model: &str, endpoint: &str, sla_name: &str) {
        self.sla_escalations_total
            .with_label_values(&[model, endpoint, sla_name])
            .inc();
    }

    /// Record a request retry.
    ///
    /// The reason should be a low-cardinality value like "timeout", "rate_limit", etc.
    pub fn record_retry(&self, model: &str, endpoint: &str, reason: &str) {
        self.request_retries_total
            .with_label_values(&[model, endpoint, reason])
            .inc();
    }
}

#[cfg(all(test, feature = "metrics"))]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let registry = Registry::new();
        let metrics = FusilladeMetrics::new(registry.clone()).unwrap();

        // Verify metrics are registered
        let families = registry.gather();
        assert!(families.len() >= 6); // Should have at least our 6 metrics

        let metric_names: Vec<String> = families.iter().map(|f| f.get_name().to_string()).collect();
        assert!(metric_names.contains(&"fusillade_requests_in_flight".to_string()));
        assert!(metric_names.contains(&"fusillade_requests_total".to_string()));
        assert!(metric_names.contains(&"fusillade_sla_at_risk_requests".to_string()));
    }

    #[test]
    fn test_record_request_metrics() {
        let registry = Registry::new();
        let metrics = FusilladeMetrics::new(registry.clone()).unwrap();

        // Record some metrics
        metrics.set_requests_in_flight("gpt-4", "https://api.openai.com", 5);
        metrics.record_request_completed(
            "gpt-4",
            "https://api.openai.com",
            Duration::from_secs(2),
        );
        metrics.record_request_failed(
            "gpt-4",
            "https://api.openai.com",
            Duration::from_secs(1),
        );

        let families = registry.gather();

        // Verify requests_total counter
        let requests_total = families
            .iter()
            .find(|f| f.get_name() == "fusillade_requests_total")
            .expect("requests_total metric not found");

        let completed = requests_total
            .get_metric()
            .iter()
            .find(|m| {
                m.get_label()
                    .iter()
                    .any(|l| l.get_name() == "status" && l.get_value() == "completed")
            })
            .expect("completed status not found");

        assert_eq!(completed.get_counter().get_value(), 1.0);

        // Verify requests_in_flight gauge
        let requests_in_flight = families
            .iter()
            .find(|f| f.get_name() == "fusillade_requests_in_flight")
            .expect("requests_in_flight metric not found");

        assert_eq!(
            requests_in_flight.get_metric()[0].get_gauge().get_value(),
            5.0
        );
    }

    #[test]
    fn test_sla_metrics() {
        let registry = Registry::new();
        let metrics = FusilladeMetrics::new(registry.clone()).unwrap();

        // Record SLA metrics
        metrics.record_sla_at_risk("gpt-4", "https://api.openai.com", "critical", 10);
        metrics.record_sla_escalation("gpt-4", "https://api.openai.com", "critical");

        let families = registry.gather();

        // Verify SLA at-risk gauge
        let sla_at_risk = families
            .iter()
            .find(|f| f.get_name() == "fusillade_sla_at_risk_requests")
            .expect("sla_at_risk_requests metric not found");

        assert_eq!(sla_at_risk.get_metric()[0].get_gauge().get_value(), 10.0);

        // Verify SLA escalations counter
        let sla_escalations = families
            .iter()
            .find(|f| f.get_name() == "fusillade_sla_escalations_total")
            .expect("sla_escalations_total metric not found");

        assert_eq!(
            sla_escalations.get_metric()[0].get_counter().get_value(),
            1.0
        );
    }

    #[test]
    fn test_retry_metrics() {
        let registry = Registry::new();
        let metrics = FusilladeMetrics::new(registry.clone()).unwrap();

        metrics.record_retry("gpt-4", "https://api.openai.com", "timeout");
        metrics.record_retry("gpt-4", "https://api.openai.com", "timeout");
        metrics.record_retry("gpt-4", "https://api.openai.com", "rate_limit");

        let families = registry.gather();
        let retries = families
            .iter()
            .find(|f| f.get_name() == "fusillade_request_retries_total")
            .expect("request_retries_total metric not found");

        // Should have 2 metrics: timeout and rate_limit
        assert_eq!(retries.get_metric().len(), 2);
    }
}
