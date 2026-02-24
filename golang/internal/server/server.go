// Package server implements the BleepStore HTTP server and S3-compatible route multiplexer.
package server

import (
	"context"
	"net/http"

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

// Server is the BleepStore HTTP server. It routes incoming requests to the
// appropriate S3-compatible handler based on the request method and path.
type Server struct {
	cfg        *config.Config
	router     chi.Router
	api        huma.API
	meta       metadata.MetadataStore
	store      storage.StorageBackend
	verifier   *auth.SigV4Verifier
	bucket     *handlers.BucketHandler
	object     *handlers.ObjectHandler
	multi      *handlers.MultipartHandler
	httpServer *http.Server
}

// HealthBody is the JSON body returned by the health check endpoint.
type HealthBody struct {
	Status string `json:"status" example:"ok" doc:"Health status"`
}

// HealthOutput is the Huma output struct for the health check endpoint.
type HealthOutput struct {
	Body HealthBody
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
	humaConfig.DocsPath = "/docs"
	humaConfig.OpenAPIPath = "/openapi"
	api := humachi.New(router, humaConfig)

	s := &Server{
		cfg:    cfg,
		router: router,
		api:    api,
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
	handler = metricsMiddleware(handler)

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
	// Register /health via Huma for auto-OpenAPI documentation.
	huma.Register(s.api, huma.Operation{
		OperationID: "get-health",
		Method:      http.MethodGet,
		Path:        "/health",
		Summary:     "Health check",
		Description: "Returns the health status of the BleepStore server.",
		Tags:        []string{"System"},
	}, func(ctx context.Context, input *struct{}) (*HealthOutput, error) {
		return &HealthOutput{Body: HealthBody{Status: "ok"}}, nil
	})

	// Register HEAD /health separately (Huma only does one method per registration).
	s.router.Head("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	})

	// Register /metrics via promhttp.Handler().
	s.router.Handle("/metrics", promhttp.Handler())

	// S3 catch-all: all remaining requests go through the dispatch function.
	// Chi matches more specific routes (health, docs, metrics, openapi) first,
	// then falls through to the catch-all.
	s.router.HandleFunc("/*", s.dispatch)
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
