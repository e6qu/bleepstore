//! Prometheus metrics for BleepStore.
//!
//! Installs a global Prometheus recorder using `metrics-exporter-prometheus`,
//! defines metric name constants, provides a Tower-compatible middleware for
//! HTTP RED metrics, and exposes the `/metrics` endpoint handler.

use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;
use std::time::Instant;

// -- Metric name constants ----------------------------------------------------

/// Total HTTP requests (counter). Labels: method, path, status.
pub const HTTP_REQUESTS_TOTAL: &str = "bleepstore_http_requests_total";

/// HTTP request duration in seconds (histogram). Labels: method, path.
pub const HTTP_REQUEST_DURATION_SECONDS: &str = "bleepstore_http_request_duration_seconds";

/// Total S3 operations (counter). Labels: operation, status.
pub const S3_OPERATIONS_TOTAL: &str = "bleepstore_s3_operations_total";

/// Total objects across all buckets (gauge).
pub const OBJECTS_TOTAL: &str = "bleepstore_objects_total";

/// Total buckets (gauge).
pub const BUCKETS_TOTAL: &str = "bleepstore_buckets_total";

/// Total bytes received in request bodies (counter).
pub const BYTES_RECEIVED_TOTAL: &str = "bleepstore_bytes_received_total";

/// Total bytes sent in response bodies (counter).
pub const BYTES_SENT_TOTAL: &str = "bleepstore_bytes_sent_total";

// -- Global recorder installation ---------------------------------------------

/// Singleton handle to the Prometheus recorder.
static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the global Prometheus metrics recorder. Idempotent -- safe to call
/// multiple times (e.g. in tests). Returns a reference to the global handle.
pub fn init_metrics() -> &'static PrometheusHandle {
    PROMETHEUS_HANDLE.get_or_init(|| {
        PrometheusBuilder::new()
            .install_recorder()
            .expect("failed to install Prometheus recorder")
    })
}

/// Register metric descriptions with the global recorder. Call once after
/// `init_metrics()`.
pub fn describe_metrics() {
    describe_counter!(HTTP_REQUESTS_TOTAL, "Total HTTP requests");
    describe_histogram!(
        HTTP_REQUEST_DURATION_SECONDS,
        "HTTP request duration in seconds"
    );
    describe_counter!(S3_OPERATIONS_TOTAL, "Total S3 operations by type");
    describe_gauge!(OBJECTS_TOTAL, "Total objects across all buckets");
    describe_gauge!(BUCKETS_TOTAL, "Total buckets");
    describe_counter!(BYTES_RECEIVED_TOTAL, "Total bytes received (request bodies)");
    describe_counter!(BYTES_SENT_TOTAL, "Total bytes sent (response bodies)");
}

// -- Metrics middleware -------------------------------------------------------

/// Axum middleware that records HTTP RED metrics for every request.
///
/// Excludes `/metrics` from self-instrumentation to avoid feedback loops.
/// Must be the outermost layer so it captures the full request lifecycle.
pub async fn metrics_middleware(
    req: Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Response {
    let method = req.method().to_string();
    let path = normalize_path(req.uri().path());

    // Do not instrument the metrics endpoint itself.
    if req.uri().path() == "/metrics" {
        return next.run(req).await;
    }

    let start = Instant::now();
    let response = next.run(req).await;
    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    counter!(HTTP_REQUESTS_TOTAL, "method" => method.clone(), "path" => path.clone(), "status" => status).increment(1);
    histogram!(HTTP_REQUEST_DURATION_SECONDS, "method" => method, "path" => path).record(duration);

    response
}

// -- Path normalization -------------------------------------------------------

/// Normalize an actual request path to a route template for metric labels.
///
/// This prevents high-cardinality labels from unique bucket/key names.
///
/// Examples:
/// - `/health` -> `/health`
/// - `/docs` -> `/docs`
/// - `/openapi.json` -> `/openapi.json`
/// - `/my-bucket` -> `/{bucket}`
/// - `/my-bucket/path/to/key` -> `/{bucket}/{key}`
/// - `/` -> `/`
fn normalize_path(path: &str) -> String {
    match path {
        "/" | "/health" | "/docs" | "/openapi.json" | "/metrics" => path.to_string(),
        _ => {
            // Strip leading slash then count segments.
            let trimmed = path.trim_start_matches('/');
            if trimmed.is_empty() {
                return "/".to_string();
            }
            match trimmed.find('/') {
                None => "/{bucket}".to_string(),
                Some(_) => "/{bucket}/{key}".to_string(),
            }
        }
    }
}

// -- Metrics endpoint handler -------------------------------------------------

/// `GET /metrics` -- Render Prometheus exposition format text.
pub async fn metrics_handler() -> impl IntoResponse {
    let handle = PROMETHEUS_HANDLE
        .get()
        .expect("Prometheus recorder not initialized");
    let body = handle.render();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path_root() {
        assert_eq!(normalize_path("/"), "/");
    }

    #[test]
    fn test_normalize_path_health() {
        assert_eq!(normalize_path("/health"), "/health");
    }

    #[test]
    fn test_normalize_path_docs() {
        assert_eq!(normalize_path("/docs"), "/docs");
    }

    #[test]
    fn test_normalize_path_openapi() {
        assert_eq!(normalize_path("/openapi.json"), "/openapi.json");
    }

    #[test]
    fn test_normalize_path_bucket() {
        assert_eq!(normalize_path("/my-bucket"), "/{bucket}");
        assert_eq!(normalize_path("/test-bucket-123"), "/{bucket}");
    }

    #[test]
    fn test_normalize_path_object() {
        assert_eq!(normalize_path("/my-bucket/key"), "/{bucket}/{key}");
        assert_eq!(
            normalize_path("/my-bucket/path/to/object.txt"),
            "/{bucket}/{key}"
        );
    }
}
