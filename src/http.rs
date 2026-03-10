//! HTTP client abstraction for making requests.
//!
//! This module defines the `HttpClient` trait to abstract HTTP request execution,
//! enabling testability with mock implementations.

use crate::error::Result;
use crate::types::RequestData;
use async_trait::async_trait;
use opentelemetry::trace::TraceContextExt;
use std::time::Duration;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Response from an HTTP request.
/// TODO: How will we deal with streaming responses? Right now we buffer the whole response before
/// writing it back
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    /// HTTP status code
    pub status: u16,
    /// Response body as a string
    pub body: String,
}

/// Trait for executing HTTP requests.
///
/// This abstraction allows for different implementations (production vs. testing)
/// and makes the daemon processing logic testable without making real HTTP calls.
///
/// # Example
/// ```ignore
/// let client = ReqwestHttpClient::new(Duration::from_secs(300), Duration::from_secs(120));
/// let response = client.execute(&request_data, "api-key").await?;
/// println!("Status: {}, Body: {}", response.status, response.body);
/// ```
#[async_trait]
pub trait HttpClient: Send + Sync + Clone {
    /// Execute an HTTP request.
    ///
    /// Timeout behavior is configured at client construction time, not per-request.
    ///
    /// # Arguments
    /// * `request` - The request data containing endpoint, method, path, and body
    /// * `api_key` - API key to include in Authorization: Bearer header
    ///
    /// # Errors
    /// Returns an error if:
    /// - The request fails due to network issues
    /// - The request times out (either waiting for headers or between body chunks)
    /// - The URL is invalid
    async fn execute(&self, request: &RequestData, api_key: &str) -> Result<HttpResponse>;
}

// ============================================================================
// Production Implementation using reqwest
// ============================================================================

/// Production HTTP client using reqwest.
///
/// This implementation makes real HTTP requests to external endpoints.
/// Timeouts are configured at construction time:
/// - `header_timeout`: max time to wait for response headers (connect + time-to-first-token)
/// - `chunk_timeout`: max idle time between body chunks
/// - `body_timeout`: max total time for the entire response body
#[derive(Clone)]
pub struct ReqwestHttpClient {
    client: reqwest::Client,
    header_timeout: Duration,
    chunk_timeout: Duration,
    body_timeout: Duration,
}

impl ReqwestHttpClient {
    /// Create a new reqwest-based HTTP client with the given timeouts.
    pub fn new(header_timeout: Duration, chunk_timeout: Duration, body_timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::new(),
            header_timeout,
            chunk_timeout,
            body_timeout,
        }
    }
}

impl Default for ReqwestHttpClient {
    fn default() -> Self {
        Self::new(NO_TIMEOUT, NO_TIMEOUT, NO_TIMEOUT)
    }
}

/// Effectively infinite timeout used when no timeout is configured.
const NO_TIMEOUT: Duration = Duration::from_secs(86_400);

#[async_trait]
impl HttpClient for ReqwestHttpClient {
    #[tracing::instrument(skip(self, request, api_key), fields(
        otel.name = %format!("{} {}", request.method, request.path),
    ))]
    async fn execute(&self, request: &RequestData, api_key: &str) -> Result<HttpResponse> {
        let url = format!("{}{}", request.endpoint, request.path);
        let span = tracing::Span::current();
        span.set_attribute("otel.kind", "Client");
        span.set_attribute("http.request.method", request.method.clone());
        span.set_attribute("url.path", request.path.clone());
        span.set_attribute("url.full", url.clone());

        tracing::debug!(
            url.full = %url,
            header_timeout_ms = self.header_timeout.as_millis() as u64,
            chunk_timeout_ms = self.chunk_timeout.as_millis() as u64,
            body_timeout_ms = self.body_timeout.as_millis() as u64,
            "Executing HTTP request"
        );

        let mut req = self.client.request(
            request.method.parse().map_err(|e| {
                tracing::error!(method = %request.method, error = %e, "Invalid HTTP method");
                anyhow::anyhow!("Invalid HTTP method '{}': {}", request.method, e)
            })?,
            &url,
        );

        // Only add Authorization header if api_key is not empty
        if !api_key.is_empty() {
            req = req.header("Authorization", format!("Bearer {}", api_key));
            tracing::trace!(request_id = %request.id, "Added Authorization header");
        }

        // Add fusillade request ID header for analytics correlation in dwctl
        // Use the full UUID (request.id.0) instead of the Display impl which only shows 8 chars
        req = req.header("X-Fusillade-Request-Id", request.id.0.to_string());

        // Add batch metadata as headers (x-fusillade-batch-COLUMN-NAME)
        // This includes id, created_by, endpoint, completion_window, etc.
        // Convert underscores to hyphens for standard HTTP header naming
        for (key, value) in &request.batch_metadata {
            let header_name = format!("x-fusillade-batch-{}", key.replace('_', "-"));
            req = req.header(&header_name, value);
        }

        // Add custom_id header if present for analytics correlation
        if let Some(custom_id) = &request.custom_id {
            req = req.header("X-Fusillade-Custom-Id", custom_id.clone());
            tracing::trace!(request_id = %request.id, custom_id = %custom_id, "Added X-Fusillade-Custom-Id header");
        }

        // Inject W3C traceparent header for distributed tracing.
        // dwctl extracts this in its TraceLayer to parent its request span
        // under this execute span, producing one continuous trace.
        let ctx = tracing::Span::current().context();
        let span_ref = ctx.span();
        let span_ctx = span_ref.span_context();
        if span_ctx.is_valid() {
            let traceparent = format!(
                "00-{}-{}-{:02x}",
                span_ctx.trace_id(),
                span_ctx.span_id(),
                span_ctx.trace_flags().to_u8()
            );
            req = req.header("traceparent", &traceparent);
            tracing::trace!(request_id = %request.id, traceparent = %traceparent, "Added traceparent header for distributed tracing");
        }

        // Only add body and Content-Type for methods that support a body
        let method_upper = request.method.to_uppercase();
        if method_upper != "GET"
            && method_upper != "HEAD"
            && method_upper != "DELETE"
            && !request.body.is_empty()
        {
            req = req
                .header("Content-Type", "application/json")
                .body(request.body.clone());
            tracing::trace!(
                request_id = %request.id,
                body_len = request.body.len(),
                "Added request body"
            );
        }

        let mut response = tokio::time::timeout(self.header_timeout, req.send())
            .await
            .map_err(|_| {
                crate::error::FusilladeError::HeaderTimeout(format!(
                    "No response headers from {} within {}ms",
                    url,
                    self.header_timeout.as_millis()
                ))
            })?
            .map_err(|e| -> crate::error::FusilladeError { e.into() })
            .inspect_err(|e| {
                tracing::error!(
                    request_id = %request.id,
                    url.full = %url,
                    error = %e,
                    custom_id = ?request.custom_id,
                    batch_metadata_keys = ?request.batch_metadata.keys().collect::<Vec<_>>(),
                    "HTTP request failed"
                );
            })?;

        let status = response.status().as_u16();

        let body_bytes = tokio::time::timeout(self.body_timeout, async {
            let mut buf: Vec<u8> = Vec::new();
            loop {
                match tokio::time::timeout(self.chunk_timeout, response.chunk()).await {
                    Ok(Ok(Some(chunk))) => buf.extend_from_slice(&chunk),
                    Ok(Ok(None)) => break,
                    Ok(Err(e)) => return Err(e.into()),
                    Err(_) => {
                        return Err(crate::error::FusilladeError::TokensTimeout(format!(
                            "Body read stalled from {} after {}ms ({} bytes received)",
                            url,
                            self.chunk_timeout.as_millis(),
                            buf.len()
                        )));
                    }
                }
            }
            Ok(buf)
        })
        .await
        .map_err(|_| {
            crate::error::FusilladeError::BodyTimeout(format!(
                "Total body read from {} exceeded {}ms",
                url,
                self.body_timeout.as_millis()
            ))
        })??;
        let body = String::from_utf8(body_bytes)
            .map_err(|e| anyhow::anyhow!("Response body from {} is not valid UTF-8: {}", url, e))?;

        tracing::debug!(
            request_id = %request.id,
            status = status,
            response_len = body.len(),
            "HTTP request completed"
        );

        Ok(HttpResponse { status, body })
    }
}

// ============================================================================
// Test/Mock Implementation
// ============================================================================

// TODO: this should be a separate file within an http/ module.
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::oneshot;

/// Mock HTTP client for testing.
///
/// Allows configuring predetermined responses for specific requests without
/// making actual HTTP calls.
///
/// # Example
/// ```ignore
/// let mock = MockHttpClient::new();
/// mock.add_response(
///     "POST /v1/chat/completions",
///     HttpResponse {
///         status: 200,
///         body: r#"{"result": "success"}"#.to_string(),
///     },
/// );
/// ```
#[derive(Clone)]
pub struct MockHttpClient {
    responses: Arc<Mutex<HashMap<String, Vec<MockResponse>>>>,
    calls: Arc<Mutex<Vec<MockCall>>>,
    in_flight: Arc<AtomicUsize>,
}

/// A mock response that can optionally wait for a trigger before completing.
enum MockResponse {
    /// Immediate response
    Immediate(Result<HttpResponse>),
    /// Response that waits for a trigger signal before completing
    Triggered {
        response: Result<HttpResponse>,
        trigger: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    },
}

/// Record of a call made to the mock HTTP client.
#[derive(Debug, Clone)]
pub struct MockCall {
    pub method: String,
    pub endpoint: String,
    pub path: String,
    pub body: String,
    pub api_key: String,
    pub batch_metadata: std::collections::HashMap<String, String>,
}

impl MockHttpClient {
    /// Create a new mock HTTP client.
    pub fn new() -> Self {
        Self {
            responses: Arc::new(Mutex::new(HashMap::new())),
            calls: Arc::new(Mutex::new(Vec::new())),
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Add a predetermined response for a specific method and path.
    ///
    /// The key is formatted as "{method} {path}". Multiple responses can be
    /// added for the same key - they will be returned in FIFO order.
    pub fn add_response(&self, key: &str, response: Result<HttpResponse>) {
        self.responses
            .lock()
            .entry(key.to_string())
            .or_default()
            .push(MockResponse::Immediate(response));
    }

    /// Add a response that will wait for a manual trigger before completing.
    ///
    /// Returns a sender that when triggered (by sending `()` or dropping) will
    /// cause the HTTP request to complete with the given response.
    ///
    /// # Example
    /// ```ignore
    /// let trigger = mock.add_response_with_trigger(
    ///     "POST /test",
    ///     Ok(HttpResponse { status: 200, body: "ok".to_string() })
    /// );
    /// // ... request is now blocked waiting ...
    /// trigger.send(()).unwrap(); // Now it completes
    /// ```
    pub fn add_response_with_trigger(
        &self,
        key: &str,
        response: Result<HttpResponse>,
    ) -> oneshot::Sender<()> {
        let (tx, rx) = oneshot::channel();
        self.responses
            .lock()
            .entry(key.to_string())
            .or_default()
            .push(MockResponse::Triggered {
                response,
                trigger: Arc::new(Mutex::new(Some(rx))),
            });
        tx
    }

    /// Get all calls that have been made to this mock client.
    pub fn get_calls(&self) -> Vec<MockCall> {
        self.calls.lock().clone()
    }

    /// Clear all recorded calls.
    pub fn clear_calls(&self) {
        self.calls.lock().clear();
    }

    /// Get the number of calls made.
    pub fn call_count(&self) -> usize {
        self.calls.lock().len()
    }

    /// Get the number of requests currently in-flight (executing).
    ///
    /// This is useful for testing cancellation - if a request is aborted,
    /// the in-flight count will decrease.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }
}

impl Default for MockHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HttpClient for MockHttpClient {
    async fn execute(&self, request: &RequestData, api_key: &str) -> Result<HttpResponse> {
        // Increment in-flight counter
        self.in_flight.fetch_add(1, Ordering::SeqCst);

        // Guard to ensure we decrement even if cancelled/panicked
        let in_flight = self.in_flight.clone();
        let _guard = InFlightGuard { in_flight };

        // Record this call
        self.calls.lock().push(MockCall {
            method: request.method.clone(),
            endpoint: request.endpoint.clone(),
            path: request.path.clone(),
            body: request.body.clone(),
            api_key: api_key.to_string(),
            batch_metadata: request.batch_metadata.clone(),
        });

        // Look up the response
        let key = format!("{} {}", request.method, request.path);
        let mock_response = {
            let mut responses = self.responses.lock();
            if let Some(response_queue) = responses.get_mut(&key) {
                if !response_queue.is_empty() {
                    Some(response_queue.remove(0))
                } else {
                    None
                }
            } else {
                None
            }
        };

        match mock_response {
            Some(MockResponse::Immediate(response)) => response,
            Some(MockResponse::Triggered { response, trigger }) => {
                // Wait for the trigger signal before returning the response
                let rx = {
                    let mut trigger_guard = trigger.lock();
                    trigger_guard.take()
                };

                if let Some(rx) = rx {
                    // Wait for trigger (ignore the result - we proceed either way)
                    let _ = rx.await;
                }

                response
            }
            None => {
                // No response configured - return a default error
                Err(crate::error::FusilladeError::Other(anyhow::anyhow!(
                    "No mock response configured for {} {}",
                    request.method,
                    request.path
                )))
            }
        }
    }
}

/// Guard that decrements the in-flight counter when dropped.
/// This ensures the counter is decremented even if the task is cancelled or panics.
struct InFlightGuard {
    in_flight: Arc<AtomicUsize>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RequestId;

    #[tokio::test]
    async fn test_mock_client_basic() {
        let mock = MockHttpClient::new();
        mock.add_response(
            "POST /test",
            Ok(HttpResponse {
                status: 200,
                body: "success".to_string(),
            }),
        );

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: crate::batch::BatchId::from(uuid::Uuid::new_v4()),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: "{}".to_string(),
            model: "test-model".to_string(),
            api_key: "test-key".to_string(),
            batch_metadata: std::collections::HashMap::new(),
        };

        let response = mock.execute(&request, "test-key").await.unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, "success");

        // Verify call was recorded
        let calls = mock.get_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].method, "POST");
        assert_eq!(calls[0].path, "/test");
        assert_eq!(calls[0].api_key, "test-key");
    }

    #[tokio::test]
    async fn test_mock_client_multiple_responses() {
        let mock = MockHttpClient::new();
        mock.add_response(
            "GET /status",
            Ok(HttpResponse {
                status: 200,
                body: "first".to_string(),
            }),
        );
        mock.add_response(
            "GET /status",
            Ok(HttpResponse {
                status: 200,
                body: "second".to_string(),
            }),
        );

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: crate::batch::BatchId::from(uuid::Uuid::new_v4()),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "GET".to_string(),
            path: "/status".to_string(),
            body: "".to_string(),
            model: "test-model".to_string(),
            api_key: "test-key".to_string(),
            batch_metadata: std::collections::HashMap::new(),
        };

        let response1 = mock.execute(&request, "key").await.unwrap();
        assert_eq!(response1.body, "first");

        let response2 = mock.execute(&request, "key").await.unwrap();
        assert_eq!(response2.body, "second");

        assert_eq!(mock.call_count(), 2);
    }

    #[tokio::test]
    async fn test_mock_client_no_response() {
        let mock = MockHttpClient::new();

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: crate::batch::BatchId::from(uuid::Uuid::new_v4()),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/unknown".to_string(),
            body: "{}".to_string(),
            model: "test-model".to_string(),
            api_key: "test-key".to_string(),
            batch_metadata: std::collections::HashMap::new(),
        };

        let result = mock.execute(&request, "key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_client_with_trigger() {
        let mock = MockHttpClient::new();

        let trigger = mock.add_response_with_trigger(
            "POST /test",
            Ok(HttpResponse {
                status: 200,
                body: "triggered".to_string(),
            }),
        );

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: crate::batch::BatchId::from(uuid::Uuid::new_v4()),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: "{}".to_string(),
            model: "test-model".to_string(),
            api_key: "test-key".to_string(),
            batch_metadata: std::collections::HashMap::new(),
        };

        // Spawn the request execution (it will block waiting for trigger)
        let mock_clone = mock.clone();
        let handle = tokio::spawn(async move { mock_clone.execute(&request, "key").await });

        // Give it a moment to start executing
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify it hasn't completed yet
        assert!(!handle.is_finished());

        // Now trigger the response
        trigger.send(()).unwrap();

        // Wait for completion
        let response = handle.await.unwrap().unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, "triggered");
    }

    #[tokio::test]
    async fn test_mock_client_records_batch_metadata() {
        let mock = MockHttpClient::new();
        mock.add_response(
            "POST /test",
            Ok(HttpResponse {
                status: 200,
                body: "success".to_string(),
            }),
        );

        let mut batch_metadata = std::collections::HashMap::new();
        batch_metadata.insert("id".to_string(), "batch-123".to_string());
        batch_metadata.insert(
            "endpoint".to_string(),
            "https://api.example.com".to_string(),
        );
        batch_metadata.insert("created_at".to_string(), "2025-12-19T12:00:00Z".to_string());
        batch_metadata.insert("completion_window".to_string(), "2s".to_string());

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: crate::batch::BatchId::from(uuid::Uuid::new_v4()),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: r#"{"key":"value"}"#.to_string(),
            model: "test-model".to_string(),
            api_key: "test-key".to_string(),
            batch_metadata: batch_metadata.clone(),
        };

        let response = mock.execute(&request, "test-key").await.unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, "success");

        // Verify batch metadata was recorded
        let calls = mock.get_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].batch_metadata.len(), 4);
        assert_eq!(
            calls[0].batch_metadata.get("id"),
            Some(&"batch-123".to_string())
        );
        assert_eq!(
            calls[0].batch_metadata.get("endpoint"),
            Some(&"https://api.example.com".to_string())
        );
        assert_eq!(
            calls[0].batch_metadata.get("created_at"),
            Some(&"2025-12-19T12:00:00Z".to_string())
        );
        assert_eq!(
            calls[0].batch_metadata.get("completion_window"),
            Some(&"2s".to_string())
        );
    }

    #[tokio::test]
    async fn test_reqwest_client_sets_batch_metadata_headers() {
        use axum::{Router, extract::Request, http::StatusCode, routing::post};

        // Create a test server that captures headers
        let app = Router::new().route(
            "/test",
            post(|request: Request| async move {
                let headers = request.headers();

                // Verify batch metadata headers are present and correct
                assert_eq!(
                    headers
                        .get("x-fusillade-batch-id")
                        .and_then(|h| h.to_str().ok()),
                    Some("batch-456"),
                    "Missing or incorrect x-fusillade-batch-id header"
                );
                assert_eq!(
                    headers
                        .get("x-fusillade-batch-endpoint")
                        .and_then(|h| h.to_str().ok()),
                    Some("/v1/completions"),
                    "Missing or incorrect x-fusillade-batch-endpoint header"
                );
                assert_eq!(
                    headers
                        .get("x-fusillade-batch-created-at")
                        .and_then(|h| h.to_str().ok()),
                    Some("2025-12-19T13:00:00Z"),
                    "Missing or incorrect x-fusillade-batch-created-at header"
                );
                assert_eq!(
                    headers
                        .get("x-fusillade-batch-completion-window")
                        .and_then(|h| h.to_str().ok()),
                    Some("24h"),
                    "Missing or incorrect x-fusillade-batch-completion-window header"
                );

                // Also verify standard headers
                assert_eq!(
                    headers.get("authorization").and_then(|h| h.to_str().ok()),
                    Some("Bearer test-api-key"),
                    "Missing or incorrect authorization header"
                );
                assert!(
                    headers.get("x-fusillade-request-id").is_some(),
                    "Missing x-fusillade-request-id header"
                );

                (StatusCode::OK, r#"{"result":"ok"}"#)
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Give server time to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Create request with batch metadata
        let mut batch_metadata = std::collections::HashMap::new();
        batch_metadata.insert("id".to_string(), "batch-456".to_string());
        batch_metadata.insert("endpoint".to_string(), "/v1/completions".to_string());
        batch_metadata.insert("created_at".to_string(), "2025-12-19T13:00:00Z".to_string());
        batch_metadata.insert("completion_window".to_string(), "24h".to_string());

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: crate::batch::BatchId::from(uuid::Uuid::new_v4()),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: format!("http://{}", addr),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: r#"{"prompt":"test"}"#.to_string(),
            model: "test-model".to_string(),
            api_key: "test-api-key".to_string(),
            batch_metadata,
        };

        // Use real HTTP client
        let client = ReqwestHttpClient::default();
        let response = client.execute(&request, "test-api-key").await.unwrap();

        assert_eq!(response.status, 200);
        assert_eq!(response.body, r#"{"result":"ok"}"#);
    }

    #[tokio::test]
    async fn test_read_timeout_on_stalled_body() {
        use axum::{Router, http::StatusCode, routing::post};

        // Server sends headers + partial body, then stalls forever
        let app = Router::new().route(
            "/test",
            post(|| async {
                use futures::StreamExt;
                let stream = futures::stream::once(async {
                    Ok::<_, std::convert::Infallible>("partial".to_string().into_bytes())
                })
                .chain(futures::stream::pending());

                let body = axum::body::Body::from_stream(stream);
                (StatusCode::OK, body)
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Give server time to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: crate::batch::BatchId::from(uuid::Uuid::new_v4()),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: format!("http://{}", addr),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: "{}".to_string(),
            model: "test-model".to_string(),
            api_key: "".to_string(),
            batch_metadata: std::collections::HashMap::new(),
        };

        let timeout = Duration::from_millis(200);
        let client = ReqwestHttpClient::new(timeout, timeout, NO_TIMEOUT);
        let result = client.execute(&request, "").await;
        let err = result.expect_err("Expected TokensTimeout for stalled body");

        match err {
            crate::error::FusilladeError::TokensTimeout(msg) => {
                assert!(msg.contains("Body read stalled"));
            }
            other => panic!("Expected TokensTimeout, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_read_timeout_on_stalled_headers() {
        // Server accepts connection but never sends headers
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                // Hold the connection open but never respond
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                drop(socket);
            }
        });

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: crate::batch::BatchId::from(uuid::Uuid::new_v4()),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: format!("http://{}", addr),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: "{}".to_string(),
            model: "test-model".to_string(),
            api_key: "".to_string(),
            batch_metadata: std::collections::HashMap::new(),
        };

        let timeout = Duration::from_millis(200);
        let client = ReqwestHttpClient::new(timeout, timeout, NO_TIMEOUT);
        let result = client.execute(&request, "").await;
        let err = result.expect_err("Expected HeaderTimeout for stalled headers");

        match err {
            crate::error::FusilladeError::HeaderTimeout(msg) => {
                assert!(msg.contains("No response headers from"));
            }
            other => panic!("Expected HeaderTimeout, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_body_timeout_on_slow_drip() {
        use axum::{Router, http::StatusCode, routing::post};
        use futures::StreamExt;

        // Server sends one chunk every 50ms — never trips the 200ms chunk timeout,
        // but exceeds the 300ms body timeout.
        let app = Router::new().route(
            "/test",
            post(|| async {
                let stream = futures::stream::unfold(0u32, |i| async move {
                    if i >= 20 {
                        return None;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    Some((
                        Ok::<_, std::convert::Infallible>(format!("chunk-{i}").into_bytes()),
                        i + 1,
                    ))
                })
                .boxed();
                let body = axum::body::Body::from_stream(stream);
                (StatusCode::OK, body)
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: crate::batch::BatchId::from(uuid::Uuid::new_v4()),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: format!("http://{}", addr),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: "{}".to_string(),
            model: "test-model".to_string(),
            api_key: "".to_string(),
            batch_metadata: std::collections::HashMap::new(),
        };

        // chunk_timeout=200ms (never trips), body_timeout=300ms (trips after ~6 chunks)
        let client = ReqwestHttpClient::new(
            NO_TIMEOUT,
            Duration::from_millis(200),
            Duration::from_millis(300),
        );
        let result = client.execute(&request, "").await;
        let err = result.expect_err("Expected BodyTimeout for slow-drip response");

        match err {
            crate::error::FusilladeError::BodyTimeout(msg) => {
                assert!(msg.contains("Total body read from"));
            }
            other => panic!("Expected BodyTimeout, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_custom_id_with_newline_is_not_retriable() {
        use crate::request::types::FailureReason;

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: crate::batch::BatchId::from(uuid::Uuid::new_v4()),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: Some("invalid\ncustom_id".to_string()), // Contains newline
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: "{}".to_string(),
            model: "test-model".to_string(),
            api_key: "test-key".to_string(),
            batch_metadata: std::collections::HashMap::new(),
        };

        let client = ReqwestHttpClient::default();
        let result = client.execute(&request, "test-key").await;
        let err = result.expect_err("Expected builder error for invalid header value");

        // Verify it's a builder error and map to FailureReason (same logic as transitions.rs)
        let reason = match err {
            crate::error::FusilladeError::HttpClient(ref reqwest_err)
                if reqwest_err.is_builder() =>
            {
                FailureReason::RequestBuilderError {
                    error: reqwest_err.to_string(),
                }
            }
            _ => panic!("Expected HttpClient builder error, got: {:?}", err),
        };

        assert!(
            !reason.is_retriable(),
            "Builder errors should not be retriable"
        );
    }
}
