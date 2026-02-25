package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bleepstore/bleepstore/internal/config"
	"github.com/bleepstore/bleepstore/internal/metadata"
	"github.com/bleepstore/bleepstore/internal/metrics"
	"github.com/bleepstore/bleepstore/internal/storage"
)

func init() {
	// Register metrics once for the entire test binary so that tests
	// checking /metrics output see the expected collectors.
	metrics.Register()
}

// newTestServer creates a Server for testing with default config.
// Observability is enabled by default.
func newTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := &config.Config{
		Server: config.ServerConfig{
			Host:   "0.0.0.0",
			Port:   9011,
			Region: "us-east-1",
		},
		Auth: config.AuthConfig{
			AccessKey: "bleepstore",
			SecretKey: "bleepstore-secret",
		},
		Observability: config.ObservabilityConfig{
			Metrics:     true,
			HealthCheck: true,
		},
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	return srv
}

// newTestServerWithConfig creates a Server for testing with a custom config.
func newTestServerWithConfig(t *testing.T, cfg *config.Config) *Server {
	t.Helper()
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	return srv
}

// newTestServerWithBackends creates a Server with real metadata and storage backends.
func newTestServerWithBackends(t *testing.T) *Server {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "metadata.db")
	storageDir := filepath.Join(tmpDir, "objects")
	os.MkdirAll(storageDir, 0o755)

	cfg := &config.Config{
		Server: config.ServerConfig{
			Host:   "0.0.0.0",
			Port:   9011,
			Region: "us-east-1",
		},
		Auth: config.AuthConfig{
			AccessKey: "bleepstore",
			SecretKey: "bleepstore-secret",
		},
		Observability: config.ObservabilityConfig{
			Metrics:     true,
			HealthCheck: true,
		},
	}

	metaStore, err := metadata.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("creating metadata store: %v", err)
	}
	t.Cleanup(func() { metaStore.Close() })

	storageBackend, err := storage.NewLocalBackend(storageDir)
	if err != nil {
		t.Fatalf("creating storage backend: %v", err)
	}

	srv, err := New(cfg, metaStore, WithStorageBackend(storageBackend))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	return srv
}

// testRequest performs an HTTP request against the test server's handler
// (with the full middleware chain: metricsMiddleware -> commonHeaders -> router).
func testRequest(t *testing.T, srv *Server, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	var handler http.Handler = commonHeaders(srv.router)
	if srv.cfg.Observability.Metrics {
		handler = metricsMiddleware(handler)
	}
	handler.ServeHTTP(rec, req)
	return rec
}

func TestHealthEndpoint(t *testing.T) {
	srv := newTestServer(t)
	rec := testRequest(t, srv, "GET", "/health")

	if rec.Code != http.StatusOK {
		t.Errorf("GET /health status = %d, want %d", rec.Code, http.StatusOK)
	}

	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Errorf("GET /health Content-Type = %q, want application/json", ct)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("GET /health body unmarshal error: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("GET /health status = %q, want %q", body["status"], "ok")
	}
}

func TestHealthEndpointWithBackends(t *testing.T) {
	srv := newTestServerWithBackends(t)
	rec := testRequest(t, srv, "GET", "/health")

	if rec.Code != http.StatusOK {
		t.Errorf("GET /health status = %d, want %d", rec.Code, http.StatusOK)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("GET /health body unmarshal error: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("GET /health status = %q, want %q", body["status"], "ok")
	}

	// With health_check enabled and backends, should have checks.
	checks, ok := body["checks"].(map[string]interface{})
	if !ok {
		t.Fatal("GET /health response missing 'checks' field")
	}

	metaCheck, ok := checks["metadata"].(map[string]interface{})
	if !ok {
		t.Fatal("GET /health missing 'metadata' check")
	}
	if metaCheck["status"] != "ok" {
		t.Errorf("metadata check status = %q, want %q", metaCheck["status"], "ok")
	}

	storageCheck, ok := checks["storage"].(map[string]interface{})
	if !ok {
		t.Fatal("GET /health missing 'storage' check")
	}
	if storageCheck["status"] != "ok" {
		t.Errorf("storage check status = %q, want %q", storageCheck["status"], "ok")
	}
}

func TestHealthHeadEndpoint(t *testing.T) {
	srv := newTestServer(t)
	rec := testRequest(t, srv, "HEAD", "/health")

	if rec.Code != http.StatusOK {
		t.Errorf("HEAD /health status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestDocsEndpoint(t *testing.T) {
	srv := newTestServer(t)
	rec := testRequest(t, srv, "GET", "/docs")

	// Huma may return 200 directly or redirect to /docs/.
	if rec.Code != http.StatusOK && rec.Code != http.StatusMovedPermanently && rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("GET /docs status = %d, want 200 or redirect", rec.Code)
	}

	// If redirect, follow it.
	if rec.Code == http.StatusMovedPermanently || rec.Code == http.StatusTemporaryRedirect {
		loc := rec.Header().Get("Location")
		if loc == "" {
			t.Fatal("GET /docs returned redirect but no Location header")
		}
		rec = testRequest(t, srv, "GET", loc)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want %d", loc, rec.Code, http.StatusOK)
		}
	}

	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Errorf("GET /docs Content-Type = %q, want text/html", ct)
	}

	body := rec.Body.String()
	bodyLower := strings.ToLower(body)
	if !strings.Contains(bodyLower, "stoplight") && !strings.Contains(bodyLower, "elements") && !strings.Contains(bodyLower, "openapi") {
		t.Errorf("GET /docs body does not contain expected Swagger UI / Stoplight Elements content")
	}
}

func TestOpenAPIEndpoint(t *testing.T) {
	srv := newTestServer(t)

	rec := testRequest(t, srv, "GET", "/openapi.json")

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /openapi.json status = %d, want %d", rec.Code, http.StatusOK)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("GET /openapi.json body is not valid JSON: %v", err)
	}

	if _, ok := body["openapi"]; !ok {
		t.Errorf("GET /openapi.json response does not contain 'openapi' key")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv := newTestServer(t)

	// Make a request to /health first so that HTTP metrics get recorded.
	// CounterVec and HistogramVec only appear in Prometheus output after
	// at least one observation.
	testRequest(t, srv, "GET", "/health")

	rec := testRequest(t, srv, "GET", "/metrics")

	if rec.Code != http.StatusOK {
		t.Errorf("GET /metrics status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "bleepstore_http_requests_total") {
		t.Error("GET /metrics does not contain bleepstore_http_requests_total")
	}
	if !strings.Contains(body, "bleepstore_http_request_duration_seconds") {
		t.Error("GET /metrics does not contain bleepstore_http_request_duration_seconds")
	}
	// Note: bleepstore_s3_operations_total only appears after an S3 operation
	// is recorded. Since no handler increments it yet, check for the gauge
	// and counter metrics that are always present instead.
	if !strings.Contains(body, "bleepstore_objects_total") {
		t.Error("GET /metrics does not contain bleepstore_objects_total")
	}
	if !strings.Contains(body, "bleepstore_buckets_total") {
		t.Error("GET /metrics does not contain bleepstore_buckets_total")
	}
	if !strings.Contains(body, "bleepstore_bytes_received_total") {
		t.Error("GET /metrics does not contain bleepstore_bytes_received_total")
	}
	if !strings.Contains(body, "bleepstore_bytes_sent_total") {
		t.Error("GET /metrics does not contain bleepstore_bytes_sent_total")
	}
}

func TestMetricsDisabled(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Host:   "0.0.0.0",
			Port:   9011,
			Region: "us-east-1",
		},
		Auth: config.AuthConfig{
			AccessKey: "bleepstore",
			SecretKey: "bleepstore-secret",
		},
		Observability: config.ObservabilityConfig{
			Metrics:     false,
			HealthCheck: true,
		},
	}
	srv := newTestServerWithConfig(t, cfg)
	rec := testRequest(t, srv, "GET", "/metrics")

	// When metrics disabled, /metrics route is not registered.
	// The catch-all S3 dispatch will handle it (returning 500 since no metadata store).
	if rec.Code == http.StatusOK {
		t.Errorf("GET /metrics with metrics disabled should not return 200, got %d", rec.Code)
	}
}

func TestHealthzEndpoint(t *testing.T) {
	srv := newTestServer(t)
	rec := testRequest(t, srv, "GET", "/healthz")

	if rec.Code != http.StatusOK {
		t.Errorf("GET /healthz status = %d, want %d", rec.Code, http.StatusOK)
	}

	// Body should be empty.
	body := rec.Body.String()
	if body != "" {
		t.Errorf("GET /healthz body = %q, want empty", body)
	}
}

func TestReadyzEndpoint(t *testing.T) {
	srv := newTestServerWithBackends(t)
	rec := testRequest(t, srv, "GET", "/readyz")

	if rec.Code != http.StatusOK {
		t.Errorf("GET /readyz status = %d, want %d", rec.Code, http.StatusOK)
	}

	// Body should be empty.
	body := rec.Body.String()
	if body != "" {
		t.Errorf("GET /readyz body = %q, want empty", body)
	}
}

func TestHealthCheckDisabled(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Host:   "0.0.0.0",
			Port:   9011,
			Region: "us-east-1",
		},
		Auth: config.AuthConfig{
			AccessKey: "bleepstore",
			SecretKey: "bleepstore-secret",
		},
		Observability: config.ObservabilityConfig{
			Metrics:     true,
			HealthCheck: false,
		},
	}
	srv := newTestServerWithConfig(t, cfg)

	// /healthz should not be registered when health_check is disabled.
	rec := testRequest(t, srv, "GET", "/healthz")
	if rec.Code == http.StatusOK {
		t.Errorf("GET /healthz with health_check disabled should not return 200, got %d", rec.Code)
	}

	// /readyz should not be registered when health_check is disabled.
	rec = testRequest(t, srv, "GET", "/readyz")
	if rec.Code == http.StatusOK {
		t.Errorf("GET /readyz with health_check disabled should not return 200, got %d", rec.Code)
	}

	// /health should still work but return static response without checks.
	rec = testRequest(t, srv, "GET", "/health")
	if rec.Code != http.StatusOK {
		t.Errorf("GET /health status = %d, want %d", rec.Code, http.StatusOK)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("GET /health body unmarshal error: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("GET /health status = %q, want %q", body["status"], "ok")
	}
	// Should NOT have checks field when health_check is disabled.
	if _, ok := body["checks"]; ok {
		t.Errorf("GET /health with health_check disabled should not contain 'checks' field")
	}
}

func TestCommonHeaders(t *testing.T) {
	srv := newTestServer(t)
	rec := testRequest(t, srv, "GET", "/health")

	reqID := rec.Header().Get("x-amz-request-id")
	if reqID == "" {
		t.Error("Missing x-amz-request-id header")
	}
	if len(reqID) != 16 {
		t.Errorf("x-amz-request-id length = %d, want 16", len(reqID))
	}

	if rec.Header().Get("x-amz-id-2") == "" {
		t.Error("Missing x-amz-id-2 header")
	}

	if rec.Header().Get("Date") == "" {
		t.Error("Missing Date header")
	}

	if rec.Header().Get("Server") != "BleepStore" {
		t.Errorf("Server header = %q, want %q", rec.Header().Get("Server"), "BleepStore")
	}
}

// TestS3StubRoutes verifies that all S3 API routes return appropriate error codes.
// When no metadata store is configured, implemented handlers return 500 InternalError.
// CompleteMultipartUpload is still 501 NotImplemented (Stage 8).
func TestS3StubRoutes(t *testing.T) {
	tests := []struct {
		method     string
		path       string
		wantStatus int
		wantXML    bool   // true if we expect XML error body, false for HEAD
		wantCode   string // expected error code in XML
	}{
		// Service level (bucket handler, no meta = 500)
		{"GET", "/", 500, true, "InternalError"},

		// Bucket level (bucket handlers are implemented, no meta = 500)
		{"PUT", "/test-bucket", 500, true, "InternalError"},
		{"DELETE", "/test-bucket", 500, true, "InternalError"},
		{"HEAD", "/test-bucket", 500, false, ""},
		{"GET", "/test-bucket?location", 500, true, "InternalError"},
		{"GET", "/test-bucket?acl", 500, true, "InternalError"},
		{"PUT", "/test-bucket?acl", 500, true, "InternalError"},

		// Bucket level (multipart handlers implemented, no meta = 500)
		{"GET", "/test-bucket?uploads", 500, true, "InternalError"},
		{"GET", "/test-bucket?list-type=2", 500, true, "InternalError"},
		{"GET", "/test-bucket", 500, true, "InternalError"}, // ListObjects v1
		{"POST", "/test-bucket?delete", 500, true, "InternalError"},

		// Object level (object handlers are implemented, no meta = 500 for CRUD, 501 for not-yet-implemented)
		{"PUT", "/test-bucket/test-key", 500, true, "InternalError"},
		{"GET", "/test-bucket/test-key", 500, true, "InternalError"},
		{"HEAD", "/test-bucket/test-key", 500, false, ""},
		{"DELETE", "/test-bucket/test-key", 500, true, "InternalError"},
		{"GET", "/test-bucket/test-key?acl", 500, true, "InternalError"},
		{"PUT", "/test-bucket/test-key?acl", 500, true, "InternalError"},

		// Multipart (handlers implemented, no meta/store = 500)
		{"POST", "/test-bucket/test-key?uploads", 500, true, "InternalError"},
		{"PUT", "/test-bucket/test-key?partNumber=1&uploadId=abc", 500, true, "InternalError"},
		{"POST", "/test-bucket/test-key?uploadId=abc", 500, true, "InternalError"},
		{"DELETE", "/test-bucket/test-key?uploadId=abc", 500, true, "InternalError"},
		{"GET", "/test-bucket/test-key?uploadId=abc", 500, true, "InternalError"},
	}

	srv := newTestServer(t)

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			rec := testRequest(t, srv, tt.method, tt.path)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}

			if tt.wantXML {
				body, _ := io.ReadAll(rec.Body)
				bodyStr := string(body)
				if !strings.Contains(bodyStr, "<Error>") {
					t.Errorf("expected XML error body, got: %s", bodyStr)
				}
				if tt.wantCode != "" && !strings.Contains(bodyStr, "<Code>"+tt.wantCode+"</Code>") {
					t.Errorf("expected %s code, got: %s", tt.wantCode, bodyStr)
				}
			}
		})
	}
}

// TestParsePath verifies path parsing for bucket and key extraction.
func TestParsePath(t *testing.T) {
	tests := []struct {
		path       string
		wantBucket string
		wantKey    string
	}{
		{"/", "", ""},
		{"", "", ""},
		{"/my-bucket", "my-bucket", ""},
		{"/my-bucket/", "my-bucket", ""},
		{"/my-bucket/my-key", "my-bucket", "my-key"},
		{"/my-bucket/path/to/object", "my-bucket", "path/to/object"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			bucket, key := parsePath(tt.path)
			if bucket != tt.wantBucket {
				t.Errorf("parsePath(%q) bucket = %q, want %q", tt.path, bucket, tt.wantBucket)
			}
			if key != tt.wantKey {
				t.Errorf("parsePath(%q) key = %q, want %q", tt.path, key, tt.wantKey)
			}
		})
	}
}
