// Package server implements the BleepStore HTTP server and S3-compatible route multiplexer.
package server

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/bleepstore/bleepstore/internal/auth"
	"github.com/bleepstore/bleepstore/internal/config"
	s3err "github.com/bleepstore/bleepstore/internal/errors"
	"github.com/bleepstore/bleepstore/internal/handlers"
	"github.com/bleepstore/bleepstore/internal/metadata"
	"github.com/bleepstore/bleepstore/internal/storage"
	"github.com/bleepstore/bleepstore/internal/xmlutil"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//go:embed s3-api.openapi.json
var canonicalSpec []byte

const swaggerUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>BleepStore API - Swagger UI</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({ url: '/openapi.json', dom_id: '#swagger-ui',
      presets: [SwaggerUIBundle.presets.apis], layout: 'BaseLayout' });
  </script>
</body>
</html>`

// Server is the BleepStore HTTP server. It routes incoming requests to the
// appropriate S3-compatible handler based on the request method and path.
type Server struct {
	cfg         *config.Config
	router      chi.Router
	api         huma.API
	meta        metadata.MetadataStore
	store       storage.StorageBackend
	verifier    *auth.SigV4Verifier
	bucket      *handlers.BucketHandler
	object      *handlers.ObjectHandler
	multi       *handlers.MultipartHandler
	httpServer  *http.Server
	patchedSpec []byte
}

// HealthBody is the JSON body returned by the health check endpoint.
type HealthBody struct {
	Status string `json:"status" example:"ok" doc:"Health status"`
}

// HealthOutput is the Huma output struct for the health check endpoint.
type HealthOutput struct {
	Body HealthBody
}

// componentCheck represents the health status of a single component.
type componentCheck struct {
	Status    string `json:"status"`
	LatencyMs int64  `json:"latency_ms"`
}

// healthDetailResponse is the enhanced health response with component checks.
type healthDetailResponse struct {
	Status string                    `json:"status"`
	Checks map[string]componentCheck `json:"checks"`
}

// ServerOption is a functional option for configuring the Server.
type ServerOption func(*Server)

// WithMetadataStore sets the metadata store for the server.
func WithMetadataStore(meta metadata.MetadataStore) ServerOption {
	return func(s *Server) {
		s.meta = meta
	}
}

// WithStorageBackend sets the storage backend for the server.
func WithStorageBackend(store storage.StorageBackend) ServerOption {
	return func(s *Server) {
		s.store = store
	}
}

// New creates a new Server with the given configuration and wires up all
// S3-compatible routes on the Chi router with Huma API.
// Use ServerOption functions to provide metadata store and storage backend.
// For backward compatibility, variadic metadata.MetadataStore arguments are also accepted.
func New(cfg *config.Config, args ...interface{}) (*Server, error) {
	router := chi.NewMux()

	humaConfig := huma.DefaultConfig("BleepStore S3 API", "1.0.0")
	humaConfig.DocsPath = ""
	humaConfig.OpenAPIPath = ""
	api := humachi.New(router, humaConfig)

	// Parse canonical OpenAPI spec and patch the servers array with the
	// configured port so Swagger UI points at the running instance.
	var specMap map[string]interface{}
	if err := json.Unmarshal(canonicalSpec, &specMap); err != nil {
		return nil, fmt.Errorf("parsing embedded OpenAPI spec: %w", err)
	}
	specMap["servers"] = []interface{}{
		map[string]interface{}{
			"url":         fmt.Sprintf("http://localhost:%d", cfg.Server.Port),
			"description": "BleepStore Go",
		},
	}
	patchedBytes, err := json.Marshal(specMap)
	if err != nil {
		return nil, fmt.Errorf("marshaling patched OpenAPI spec: %w", err)
	}

	s := &Server{
		cfg:         cfg,
		router:      router,
		api:         api,
		patchedSpec: patchedBytes,
	}

	// Process arguments: support both old-style (MetadataStore) and new-style (ServerOption).
	for _, arg := range args {
		switch v := arg.(type) {
		case metadata.MetadataStore:
			s.meta = v
		case ServerOption:
			v(s)
		}
	}

	// Determine owner info from config.
	ownerID := cfg.Auth.AccessKey
	ownerDisplay := cfg.Auth.AccessKey
	region := cfg.Server.Region

	// Create SigV4 verifier if metadata store is available.
	if s.meta != nil {
		s.verifier = auth.NewSigV4Verifier(s.meta, region)
	}

	// Create handlers with injected dependencies.
	maxObjectSize := cfg.Server.MaxObjectSize
	s.bucket = handlers.NewBucketHandler(s.meta, s.store, ownerID, ownerDisplay, region)
	s.object = handlers.NewObjectHandler(s.meta, s.store, ownerID, ownerDisplay, maxObjectSize)
	s.multi = handlers.NewMultipartHandler(s.meta, s.store, ownerID, ownerDisplay, maxObjectSize)

	s.registerRoutes()
	return s, nil
}

// ListenAndServe starts the HTTP server on the given address.
// The returned http.Server is stored so it can be shut down gracefully.
// Middleware chain: metricsMiddleware -> commonHeaders -> authMiddleware -> router.
func (s *Server) ListenAndServe(addr string) error {
	var handler http.Handler = s.router
	// Rewrite x-amz-meta-* headers to lowercase (must be innermost wrapper).
	handler = metadataHeaderMiddleware(handler)
	// Wrap with auth middleware if verifier is available.
	if s.verifier != nil {
		handler = auth.Middleware(s.verifier)(handler)
	}
	handler = transferEncodingCheck(handler)
	handler = commonHeaders(handler)
	if s.cfg.Observability.Metrics {
		handler = metricsMiddleware(handler)
	}

	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: handler,
	}
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server, waiting for in-flight
// requests to complete within the given context deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}

// registerRoutes configures all routes on the Chi router.
// Huma routes (/health, /docs, /openapi.json) and /metrics are registered first.
// The S3 catch-all /* is registered last. Chi matches more specific routes first.
func (s *Server) registerRoutes() {
	// Register /health with enhanced JSON when health_check is enabled.
	s.router.Get("/health", s.handleHealth)
	s.router.Head("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	})

	// Register /healthz and /readyz liveness/readiness probes (conditional).
	if s.cfg.Observability.HealthCheck {
		s.router.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		s.router.Head("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		s.router.Get("/readyz", s.handleReadyz)
		s.router.Head("/readyz", s.handleReadyz)
	}

	// Serve the canonical OpenAPI spec (patched with the configured port).
	s.router.Get("/openapi.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(s.patchedSpec)
	})

	// Serve Swagger UI docs page pointing at the canonical spec.
	s.router.Get("/docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(swaggerUIHTML))
	})

	// Register /metrics via promhttp.Handler() (conditional).
	if s.cfg.Observability.Metrics {
		s.router.Handle("/metrics", promhttp.Handler())
	}

	// S3 catch-all: all remaining requests go through the dispatch function.
	// Chi matches more specific routes (health, docs, metrics, openapi) first,
	// then falls through to the catch-all.
	s.router.HandleFunc("/*", s.dispatch)
}

// handleHealth returns enhanced health JSON with component checks when
// health_check is enabled, or a static {"status": "ok"} when disabled.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if !s.cfg.Observability.HealthCheck {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		return
	}

	checks := make(map[string]componentCheck)
	allOK := true

	// Probe metadata store.
	if s.meta != nil {
		start := time.Now()
		err := s.meta.Ping(r.Context())
		latency := time.Since(start).Milliseconds()
		if err != nil {
			checks["metadata"] = componentCheck{Status: "error", LatencyMs: latency}
			allOK = false
		} else {
			checks["metadata"] = componentCheck{Status: "ok", LatencyMs: latency}
		}
	}

	// Probe storage backend.
	if s.store != nil {
		start := time.Now()
		err := s.store.HealthCheck(r.Context())
		latency := time.Since(start).Milliseconds()
		if err != nil {
			checks["storage"] = componentCheck{Status: "error", LatencyMs: latency}
			allOK = false
		} else {
			checks["storage"] = componentCheck{Status: "ok", LatencyMs: latency}
		}
	}

	status := "ok"
	httpStatus := http.StatusOK
	if !allOK {
		status = "degraded"
		httpStatus = http.StatusServiceUnavailable
	}

	resp := healthDetailResponse{
		Status: status,
		Checks: checks,
	}
	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(resp)
}

// handleReadyz checks whether all backend dependencies are reachable.
// Returns 200 with empty body if all pass, 503 with empty body if any fail.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	allOK := true

	if s.meta != nil {
		if err := s.meta.Ping(ctx); err != nil {
			allOK = false
		}
	}

	if s.store != nil {
		if err := s.store.HealthCheck(ctx); err != nil {
			allOK = false
		}
	}

	if allOK {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

// parsePath extracts bucket and object key from the request path.
// Returns ("", "") for root "/", ("bucket", "") for "/{bucket}",
// and ("bucket", "key/path") for "/{bucket}/{key...}".
func parsePath(path string) (bucket, key string) {
	// Trim leading slash
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	if path == "" {
		return "", ""
	}
	// Find first slash after bucket name
	idx := -1
	for i := 0; i < len(path); i++ {
		if path[i] == '/' {
			idx = i
			break
		}
	}
	if idx < 0 {
		return path, ""
	}
	return path[:idx], path[idx+1:]
}

// dispatch is the main request dispatcher. It parses the path to extract
// bucket and object key, then routes by HTTP method and query parameters.
func (s *Server) dispatch(w http.ResponseWriter, r *http.Request) {
	bucket, key := parsePath(r.URL.Path)
	q := r.URL.Query()

	// Service-level operations (no bucket in path).
	if bucket == "" {
		switch r.Method {
		case http.MethodGet:
			s.bucket.ListBuckets(w, r)
		default:
			xmlutil.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
		}
		return
	}

	// Object-level operations (bucket + key in path).
	if key != "" {
		switch r.Method {
		case http.MethodPut:
			switch {
			case q.Has("partNumber") && q.Has("uploadId"):
				s.multi.UploadPart(w, r)
			case r.Header.Get("X-Amz-Copy-Source") != "":
				s.object.CopyObject(w, r)
			case q.Has("acl"):
				s.object.PutObjectAcl(w, r)
			default:
				s.object.PutObject(w, r)
			}
		case http.MethodGet:
			switch {
			case q.Has("acl"):
				s.object.GetObjectAcl(w, r)
			case q.Has("uploadId"):
				s.multi.ListParts(w, r)
			default:
				s.object.GetObject(w, r)
			}
		case http.MethodHead:
			s.object.HeadObject(w, r)
		case http.MethodDelete:
			if q.Has("uploadId") {
				s.multi.AbortMultipartUpload(w, r)
			} else {
				s.object.DeleteObject(w, r)
			}
		case http.MethodPost:
			switch {
			case q.Has("uploadId"):
				s.multi.CompleteMultipartUpload(w, r)
			case q.Has("uploads"):
				s.multi.CreateMultipartUpload(w, r)
			default:
				xmlutil.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
			}
		default:
			xmlutil.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
		}
		return
	}

	// Bucket-level operations (bucket in path, no key).
	switch r.Method {
	case http.MethodPut:
		if q.Has("acl") {
			s.bucket.PutBucketAcl(w, r)
		} else {
			s.bucket.CreateBucket(w, r)
		}
	case http.MethodGet:
		switch {
		case q.Has("location"):
			s.bucket.GetBucketLocation(w, r)
		case q.Has("acl"):
			s.bucket.GetBucketAcl(w, r)
		case q.Has("uploads"):
			s.multi.ListMultipartUploads(w, r)
		case q.Has("list-type"):
			s.object.ListObjectsV2(w, r)
		default:
			s.object.ListObjects(w, r)
		}
	case http.MethodHead:
		s.bucket.HeadBucket(w, r)
	case http.MethodDelete:
		s.bucket.DeleteBucket(w, r)
	case http.MethodPost:
		if q.Has("delete") {
			s.object.DeleteObjects(w, r)
		} else {
			xmlutil.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
		}
	default:
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
	}
}
