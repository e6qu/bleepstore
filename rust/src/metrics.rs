//! Prometheus metrics for BleepStore.
//!
//! Installs a global Prometheus recorder using `metrics-exporter-prometheus`,
//! defines metric name constants, provides a Tower-compatible middleware for
//! HTTP RED metrics, and exposes the `/metrics` endpoint handler.

use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
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

/// HTTP request body size in bytes (histogram). Labels: method, path.
pub const HTTP_REQUEST_SIZE_BYTES: &str = "bleepstore_http_request_size_bytes";

/// HTTP response body size in bytes (histogram). Labels: method, path.
pub const HTTP_RESPONSE_SIZE_BYTES: &str = "bleepstore_http_response_size_bytes";

/// Histogram bucket boundaries for body size metrics (bytes).
pub const SIZE_HISTOGRAM_BUCKETS: [f64; 10] = [
    256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0, 16777216.0, 67108864.0,
];

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
    describe_counter!(
        BYTES_RECEIVED_TOTAL,
        "Total bytes received (request bodies)"
    );
    describe_counter!(BYTES_SENT_TOTAL, "Total bytes sent (response bodies)");
    describe_histogram!(HTTP_REQUEST_SIZE_BYTES, "HTTP request body size in bytes");
    describe_histogram!(HTTP_RESPONSE_SIZE_BYTES, "HTTP response body size in bytes");

    // Seed all metrics so they appear in /metrics output immediately,
    // even before any requests have been processed.
    counter!(S3_OPERATIONS_TOTAL, "operation" => "ListBuckets", "status" => "success").increment(0);
    gauge!(OBJECTS_TOTAL).set(0.0);
    gauge!(BUCKETS_TOTAL).set(0.0);
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

    // Capture request body size by consuming and reconstructing the body.
    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .unwrap_or_default();
    let req_size = body_bytes.len() as f64;
    let req = Request::from_parts(parts, axum::body::Body::from(body_bytes));

    let start = Instant::now();
    let response = next.run(req).await;
    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    // Capture response body size by consuming and reconstructing the response.
    let (resp_parts, resp_body) = response.into_parts();
    let resp_bytes = axum::body::to_bytes(resp_body, usize::MAX)
        .await
        .unwrap_or_default();
    let resp_size = resp_bytes.len() as f64;
    let response = Response::from_parts(resp_parts, axum::body::Body::from(resp_bytes));

    counter!(HTTP_REQUESTS_TOTAL, "method" => method.clone(), "path" => path.clone(), "status" => status).increment(1);
    histogram!(HTTP_REQUEST_DURATION_SECONDS, "method" => method.clone(), "path" => path.clone())
        .record(duration);
    histogram!(HTTP_REQUEST_SIZE_BYTES, "method" => method.clone(), "path" => path.clone())
        .record(req_size);
    histogram!(HTTP_RESPONSE_SIZE_BYTES, "method" => method.clone(), "path" => path.clone())
        .record(resp_size);
    counter!(BYTES_RECEIVED_TOTAL).increment(req_size as u64);
    counter!(BYTES_SENT_TOTAL).increment(resp_size as u64);

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
        "/" | "/health" | "/healthz" | "/readyz" | "/docs" | "/openapi.json" | "/metrics" => {
            path.to_string()
        }
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

    #[test]
    fn test_normalize_path_healthz() {
        assert_eq!(normalize_path("/healthz"), "/healthz");
    }

    #[test]
    fn test_normalize_path_readyz() {
        assert_eq!(normalize_path("/readyz"), "/readyz");
    }

    #[test]
    fn test_metric_constants_exist() {
        assert_eq!(HTTP_REQUESTS_TOTAL, "bleepstore_http_requests_total");
        assert_eq!(
            HTTP_REQUEST_DURATION_SECONDS,
            "bleepstore_http_request_duration_seconds"
        );
        assert_eq!(S3_OPERATIONS_TOTAL, "bleepstore_s3_operations_total");
        assert_eq!(OBJECTS_TOTAL, "bleepstore_objects_total");
        assert_eq!(BUCKETS_TOTAL, "bleepstore_buckets_total");
        assert_eq!(BYTES_RECEIVED_TOTAL, "bleepstore_bytes_received_total");
        assert_eq!(BYTES_SENT_TOTAL, "bleepstore_bytes_sent_total");
        assert_eq!(
            HTTP_REQUEST_SIZE_BYTES,
            "bleepstore_http_request_size_bytes"
        );
        assert_eq!(
            HTTP_RESPONSE_SIZE_BYTES,
            "bleepstore_http_response_size_bytes"
        );
    }

    #[test]
    fn test_size_histogram_buckets() {
        assert_eq!(SIZE_HISTOGRAM_BUCKETS.len(), 10);
        assert_eq!(SIZE_HISTOGRAM_BUCKETS[0], 256.0);
        assert_eq!(SIZE_HISTOGRAM_BUCKETS[9], 67108864.0);
        // Verify buckets are in ascending order.
        for i in 1..SIZE_HISTOGRAM_BUCKETS.len() {
            assert!(SIZE_HISTOGRAM_BUCKETS[i] > SIZE_HISTOGRAM_BUCKETS[i - 1]);
        }
    }
}
