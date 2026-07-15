//! Shared daemon configuration.

use std::collections::HashMap;
use std::sync::Arc;

use crate::http::HttpResponse;

/// Predicate function to determine if a response should be retried.
pub type ShouldRetryFn = Arc<dyn Fn(&HttpResponse) -> bool + Send + Sync>;

fn is_dynamo_request_cancelled(response: &HttpResponse) -> bool {
    if response.status != 499 {
        return false;
    }

    let Ok(body) = serde_json::from_str::<serde_json::Value>(&response.body) else {
        return false;
    };

    body.pointer("/error/code")
        .and_then(serde_json::Value::as_u64)
        == Some(499)
        && body
            .pointer("/error/type")
            .and_then(serde_json::Value::as_str)
            == Some("request_cancelled")
}

/// Default retry predicate: retry on server errors, rate limits, timeouts, not found, and
/// Dynamo-shaped 499 request-cancelled errors.
pub fn default_should_retry(response: &HttpResponse) -> bool {
    response.status >= 500
        || response.status == 429
        || response.status == 408
        || response.status == 404
        || is_dynamo_request_cancelled(response)
}

fn default_should_retry_fn() -> ShouldRetryFn {
    Arc::new(default_should_retry)
}

fn default_model_escalations() -> Arc<dashmap::DashMap<String, ModelEscalationConfig>> {
    Arc::new(dashmap::DashMap::new())
}

fn default_model_concurrency_limits() -> Arc<dashmap::DashMap<String, usize>> {
    Arc::new(dashmap::DashMap::new())
}

fn serialize_model_concurrency_limits<S>(
    limits: &Arc<dashmap::DashMap<String, usize>>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::Serialize;

    let limits: HashMap<String, usize> = limits
        .iter()
        .map(|entry| (entry.key().clone(), *entry.value()))
        .collect();
    limits.serialize(serializer)
}

fn deserialize_model_concurrency_limits<'de, D>(
    deserializer: D,
) -> std::result::Result<Arc<dashmap::DashMap<String, usize>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    let limits = HashMap::<String, usize>::deserialize(deserializer)?;
    let map = dashmap::DashMap::new();
    for (model, limit) in limits {
        map.insert(model, limit);
    }
    Ok(Arc::new(map))
}

fn default_escalation_threshold_seconds() -> i64 {
    900
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModelEscalationConfig {
    pub escalation_model: String,
    #[serde(default = "default_escalation_threshold_seconds")]
    pub escalation_threshold_seconds: i64,
}

/// Which claim loops a daemon process should run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonMode {
    /// Run both batchless request claims and batch claims.
    #[default]
    Both,
    /// Run only the batchless request claim loop.
    RequestOnly,
    /// Run only the batch claim loop.
    BatchOnly,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DaemonConfig {
    /// Claim-loop mode for this daemon process.
    #[serde(default)]
    pub mode: DaemonMode,
    pub claim_batch_size: usize,
    #[serde(
        default = "default_model_concurrency_limits",
        serialize_with = "serialize_model_concurrency_limits",
        deserialize_with = "deserialize_model_concurrency_limits"
    )]
    pub model_concurrency_limits: Arc<dashmap::DashMap<String, usize>>,
    #[serde(skip, default = "default_model_escalations")]
    pub model_escalations: Arc<dashmap::DashMap<String, ModelEscalationConfig>>,
    #[serde(default)]
    pub inject_deadline_priority: bool,
    pub claim_interval_ms: u64,
    #[serde(default = "default_batch_claim_size")]
    pub batch_claim_size: usize,
    #[serde(default = "default_batch_claim_batch_size")]
    pub batch_claim_batch_size: usize,
    #[serde(default)]
    pub batch_claim_require_live: bool,
    #[serde(default = "default_batch_claim_interval_ms")]
    pub batch_claim_interval_ms: u64,
    #[serde(default = "default_claim_loop_max_consecutive_failures")]
    pub claim_loop_max_consecutive_failures: u32,
    /// Upper bound on the daemon's periodic database queries, in milliseconds.
    ///
    /// This bounds silently severed database connections so claim, heartbeat,
    /// cancellation poll, and purge loops can surface a transient failure and
    /// continue on a fresh connection instead of waiting for TCP keepalive.
    #[serde(default = "default_claim_query_timeout_ms")]
    pub claim_query_timeout_ms: u64,
    pub max_retries: Option<u32>,
    pub stop_before_deadline_ms: Option<i64>,
    pub backoff_ms: u64,
    pub backoff_factor: u64,
    pub max_backoff_ms: u64,
    pub first_chunk_timeout_ms: u64,
    pub chunk_timeout_ms: u64,
    pub body_timeout_ms: u64,
    pub status_log_interval_ms: Option<u64>,
    pub heartbeat_interval_ms: u64,
    #[serde(skip, default = "default_should_retry_fn")]
    pub should_retry: ShouldRetryFn,
    pub claim_timeout_ms: u64,
    pub processing_timeout_ms: u64,
    #[serde(default = "default_pending_request_counts_timeout_ms")]
    pub pending_request_counts_timeout_ms: u64,
    pub stale_daemon_threshold_ms: u64,
    pub unclaim_batch_size: usize,
    pub cancellation_poll_interval_ms: u64,
    #[serde(default = "default_batch_metadata_fields")]
    pub batch_metadata_fields: Vec<String>,
    pub purge_interval_ms: u64,
    pub purge_batch_size: i64,
    pub purge_throttle_ms: u64,
    pub throughput_log_interval_ms: Option<u64>,
    #[serde(default)]
    pub streamable_endpoints: Vec<String>,
    #[serde(default)]
    pub urgency_weight: f64,
    #[serde(default = "default_service_tier_completion_windows_ms")]
    pub service_tier_completion_windows_ms: HashMap<String, u64>,
    #[serde(default = "default_completion_window_ms")]
    pub default_completion_window_ms: u64,
    #[serde(default = "default_claim_ramp_exponent")]
    pub claim_ramp_exponent: f64,
    #[serde(default = "default_leaks_per_window")]
    pub leaks_per_window: f64,
    #[serde(default = "default_model_filters_keep_per_model")]
    pub model_filters_keep_per_model: i64,
    #[serde(default = "default_model_filters_retention_ms")]
    pub model_filters_retention_ms: u64,
}

fn default_batch_metadata_fields() -> Vec<String> {
    vec![
        "id".to_string(),
        "endpoint".to_string(),
        "created_at".to_string(),
        "completion_window".to_string(),
    ]
}

fn default_service_tier_completion_windows_ms() -> HashMap<String, u64> {
    HashMap::from([("flex".to_string(), 3_600_000)])
}

fn default_completion_window_ms() -> u64 {
    86_400_000
}

fn default_pending_request_counts_timeout_ms() -> u64 {
    60_000
}

fn default_batch_claim_size() -> usize {
    0
}

fn default_batch_claim_batch_size() -> usize {
    4
}

fn default_batch_claim_interval_ms() -> u64 {
    0
}

fn default_claim_loop_max_consecutive_failures() -> u32 {
    10
}

fn default_claim_query_timeout_ms() -> u64 {
    180_000
}

fn default_claim_ramp_exponent() -> f64 {
    0.56
}

fn default_leaks_per_window() -> f64 {
    60.0
}

fn default_model_filters_keep_per_model() -> i64 {
    50
}

fn default_model_filters_retention_ms() -> u64 {
    604_800_000
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            mode: DaemonMode::default(),
            claim_batch_size: 100,
            model_concurrency_limits: Arc::new(dashmap::DashMap::new()),
            model_escalations: default_model_escalations(),
            inject_deadline_priority: false,
            claim_interval_ms: 1000,
            batch_claim_size: default_batch_claim_size(),
            batch_claim_batch_size: default_batch_claim_batch_size(),
            batch_claim_require_live: false,
            batch_claim_interval_ms: default_batch_claim_interval_ms(),
            claim_loop_max_consecutive_failures: default_claim_loop_max_consecutive_failures(),
            claim_query_timeout_ms: default_claim_query_timeout_ms(),
            max_retries: Some(1000),
            stop_before_deadline_ms: Some(0),
            backoff_ms: 1000,
            backoff_factor: 2,
            max_backoff_ms: 10000,
            first_chunk_timeout_ms: 540_000,
            chunk_timeout_ms: 540_000,
            body_timeout_ms: 60_000,
            status_log_interval_ms: Some(2000),
            heartbeat_interval_ms: 5000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
            processing_timeout_ms: 600000,
            pending_request_counts_timeout_ms: default_pending_request_counts_timeout_ms(),
            stale_daemon_threshold_ms: 30_000,
            unclaim_batch_size: 100,
            cancellation_poll_interval_ms: 5000,
            batch_metadata_fields: default_batch_metadata_fields(),
            purge_interval_ms: 600_000,
            purge_batch_size: 1000,
            purge_throttle_ms: 100,
            throughput_log_interval_ms: Some(60_000),
            streamable_endpoints: Vec::new(),
            urgency_weight: 0.0,
            service_tier_completion_windows_ms: default_service_tier_completion_windows_ms(),
            default_completion_window_ms: default_completion_window_ms(),
            claim_ramp_exponent: default_claim_ramp_exponent(),
            leaks_per_window: default_leaks_per_window(),
            model_filters_keep_per_model: default_model_filters_keep_per_model(),
            model_filters_retention_ms: default_model_filters_retention_ms(),
        }
    }
}

impl From<&DaemonConfig> for crate::request::transitions::RetryConfig {
    fn from(config: &DaemonConfig) -> Self {
        Self {
            max_retries: config.max_retries,
            stop_before_deadline_ms: config.stop_before_deadline_ms,
            backoff_ms: config.backoff_ms,
            backoff_factor: config.backoff_factor,
            max_backoff_ms: config.max_backoff_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(status: u16, body: &str) -> HttpResponse {
        HttpResponse {
            status,
            body: body.to_string(),
        }
    }

    #[test]
    fn retries_dynamo_request_cancelled_499() {
        let bodies = [
            r#"{"error":{"code":499,"message":"CancelledError: ","type":"request_cancelled"}}"#,
            r#"{"error":{"code":499,"type":"request_cancelled"}}"#,
            r#"{"error":{"code":499,"message":"arbitrary message","type":"request_cancelled"}}"#,
        ];

        for body in bodies {
            assert!(
                default_should_retry(&response(499, body)),
                "expected retry for body: {body}"
            );
        }
    }

    #[test]
    fn does_not_retry_other_499_responses() {
        let bodies = [
            r#"{"error":{"code":499,"type":"other"}}"#,
            r#"{"error":{"code":500,"type":"request_cancelled"}}"#,
            r#"{"error":{"code":"499","type":"request_cancelled"}}"#,
            r#"{"error":{"type":"request_cancelled"}}"#,
            r#"{"error":{"code":499}}"#,
            r#"{"error":{"code":499,"message":"CancelledError: "}}"#,
            r#"{"error":"request_cancelled"}"#,
            "not json",
            "",
        ];

        for body in bodies {
            assert!(
                !default_should_retry(&response(499, body)),
                "unexpected retry for body: {body}"
            );
        }
    }

    #[test]
    fn request_cancelled_body_does_not_override_other_statuses() {
        let body = r#"{"error":{"code":499,"type":"request_cancelled"}}"#;

        assert!(!default_should_retry(&response(400, body)));
    }

    #[test]
    fn preserves_existing_default_retry_statuses() {
        for status in [404, 408, 429, 500, 503] {
            assert!(default_should_retry(&response(status, "")));
        }

        for status in [200, 400, 401, 403, 422, 498, 499] {
            assert!(!default_should_retry(&response(status, "")));
        }
    }
}
