// Package metrics defines custom Prometheus metrics for BleepStore.
package metrics

import (
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// registerOnce ensures Register() is idempotent.
var registerOnce sync.Once

// sizeBuckets are exponential buckets for request/response size histograms (bytes).
var sizeBuckets = []float64{256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864}

// HTTP metrics (RED: Rate, Errors, Duration).
var (
	// HTTPRequestsTotal counts total HTTP requests by method, path, and status.
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bleepstore_http_requests_total",
			Help: "Total HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	// HTTPRequestDuration observes request latency in seconds by method and path.
	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bleepstore_http_request_duration_seconds",
			Help:    "Request latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	// HTTPRequestSize observes request body size in bytes.
	HTTPRequestSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bleepstore_http_request_size_bytes",
			Help:    "Request body size in bytes",
			Buckets: sizeBuckets,
		},
		[]string{"method", "path"},
	)

	// HTTPResponseSize observes response body size in bytes.
	HTTPResponseSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bleepstore_http_response_size_bytes",
			Help:    "Response body size in bytes",
			Buckets: sizeBuckets,
		},
		[]string{"method", "path"},
	)
)

// S3 operation metrics.
var (
	// S3OperationsTotal counts S3 operations by operation name and status.
	S3OperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bleepstore_s3_operations_total",
			Help: "S3 operations by type",
		},
		[]string{"operation", "status"},
	)

	// ObjectsTotal is a gauge tracking total objects across all buckets.
	ObjectsTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "bleepstore_objects_total",
			Help: "Total objects across all buckets",
		},
	)

	// BucketsTotal is a gauge tracking total buckets.
	BucketsTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "bleepstore_buckets_total",
			Help: "Total buckets",
		},
	)

	// BytesReceivedTotal counts total bytes received in request bodies.
	BytesReceivedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "bleepstore_bytes_received_total",
			Help: "Total bytes received (request bodies)",
		},
	)

	// BytesSentTotal counts total bytes sent in response bodies.
	BytesSentTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "bleepstore_bytes_sent_total",
			Help: "Total bytes sent (response bodies)",
		},
	)
)

// Register registers all Prometheus collectors with the default registry.
// This must be called explicitly (typically from main) so that metrics
// registration can be made conditional on configuration. It is safe to call
// multiple times; subsequent calls are no-ops.
func Register() {
	registerOnce.Do(func() {
		prometheus.MustRegister(
			HTTPRequestsTotal,
			HTTPRequestDuration,
			HTTPRequestSize,
			HTTPResponseSize,
			S3OperationsTotal,
			ObjectsTotal,
			BucketsTotal,
			BytesReceivedTotal,
			BytesSentTotal,
		)
		// Initialize S3OperationsTotal so it appears in /metrics output
		// even before any S3 operations have been performed.
		S3OperationsTotal.WithLabelValues("ListBuckets", "success")
	})
}

// NormalizePath maps actual request paths to normalized path templates
// suitable for use as Prometheus metric labels. This avoids high-cardinality
// labels from individual bucket/object names.
func NormalizePath(path string) string {
	// Known fixed paths.
	switch path {
	case "/health":
		return "/health"
	case "/healthz":
		return "/healthz"
	case "/readyz":
		return "/readyz"
	case "/docs", "/docs/":
		return "/docs"
	case "/metrics":
		return "/metrics"
	case "/openapi.json":
		return "/openapi.json"
	case "/", "":
		return "/"
	}

	// Starts with /docs (Stoplight Elements assets).
	if strings.HasPrefix(path, "/docs") {
		return "/docs"
	}

	// Strip leading slash and split.
	trimmed := path
	if len(trimmed) > 0 && trimmed[0] == '/' {
		trimmed = trimmed[1:]
	}
	if trimmed == "" {
		return "/"
	}

	// Find first slash to separate bucket from key.
	idx := strings.IndexByte(trimmed, '/')
	if idx < 0 {
		// Only bucket, no key.
		return "/{bucket}"
	}
	// Check if key portion is empty (trailing slash only).
	keyPart := trimmed[idx+1:]
	if keyPart == "" {
		return "/{bucket}"
	}
	// Has both bucket and key.
	return "/{bucket}/{key}"
}
