package metrics

import (
	"testing"
)

func TestNormalizePath(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"/health", "/health"},
		{"/healthz", "/healthz"},
		{"/readyz", "/readyz"},
		{"/docs", "/docs"},
		{"/docs/", "/docs"},
		{"/docs/something", "/docs"},
		{"/metrics", "/metrics"},
		{"/openapi.json", "/openapi.json"},
		{"/", "/"},
		{"", "/"},
		{"/my-bucket", "/{bucket}"},
		{"/my-bucket/", "/{bucket}"}, // trailing slash, no key
		{"/my-bucket/my-key", "/{bucket}/{key}"},
		{"/my-bucket/path/to/object", "/{bucket}/{key}"},
		{"/test-bucket", "/{bucket}"},
		{"/a/b/c/d", "/{bucket}/{key}"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := NormalizePath(tt.path)
			if got != tt.want {
				t.Errorf("NormalizePath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestMetricsRegistered(t *testing.T) {
	// Register metrics explicitly (replaces former init() auto-registration).
	Register()

	// Verify that calling Inc/Set on metrics does not panic.
	HTTPRequestsTotal.WithLabelValues("GET", "/health", "200").Inc()
	HTTPRequestDuration.WithLabelValues("GET", "/health").Observe(0.001)
	HTTPRequestSize.WithLabelValues("PUT", "/{bucket}/{key}").Observe(1024)
	HTTPResponseSize.WithLabelValues("GET", "/{bucket}/{key}").Observe(2048)
	S3OperationsTotal.WithLabelValues("ListBuckets", "success").Inc()
	ObjectsTotal.Set(42)
	BucketsTotal.Set(3)
	BytesReceivedTotal.Add(1024)
	BytesSentTotal.Add(2048)
}
