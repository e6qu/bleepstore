# Stage 1b: Framework Migration to Huma, OpenAPI & Observability

**Date:** 2026-02-23
**Status:** Complete

## What Was Implemented

### New Files
- `internal/metrics/metrics.go` — All Prometheus metric definitions with `init()` registration
- `internal/metrics/metrics_test.go` — Unit tests for NormalizePath and metric registration
- `internal/server/middleware.go` — Common headers, response recorder, and metrics middleware
- `internal/server/server_test.go` — Comprehensive server tests (health, docs, openapi, metrics, S3 stubs)

### Modified Files
- `go.mod` — Added huma/v2, chi/v5, prometheus/client_golang dependencies
- `internal/server/server.go` — Major rewrite: replaced http.ServeMux with Chi router + Huma API

### Unmodified Files (intentionally)
- `cmd/bleepstore/main.go` — Server interface preserved (New, ListenAndServe, Shutdown)
- `internal/handlers/bucket.go` — No changes needed
- `internal/handlers/object.go` — No changes needed
- `internal/handlers/multipart.go` — No changes needed
- `internal/errors/errors.go` — No changes needed
- `internal/xmlutil/xmlutil.go` — No changes needed
- `internal/config/config.go` — No changes needed

## Architecture Decision

**Hybrid approach:** Chi as primary router with raw http.Handler for S3 XML routes. Huma used selectively for /health (JSON, benefits from OpenAPI docs). S3 routes go through catch-all `/*` on Chi with existing dispatch pattern preserved.

### Route Registration Order (critical)
1. Huma registers `/health`, `/docs`, `/openapi.json` (via humachi adapter)
2. `/metrics` registered via `router.Handle("/metrics", promhttp.Handler())`
3. S3 catch-all `/*` registered last via `router.HandleFunc("/*", s.dispatch)`
4. Chi matches more specific routes first, so Huma/metrics routes take priority

### Middleware Chain
```
Request -> metricsMiddleware -> commonHeaders -> Chi router -> Handler -> Response
```

## Prometheus Metrics

### HTTP RED Metrics
| Metric | Type | Labels |
|--------|------|--------|
| `bleepstore_http_requests_total` | Counter | method, path, status |
| `bleepstore_http_request_duration_seconds` | Histogram | method, path |
| `bleepstore_http_request_size_bytes` | Histogram | method, path |
| `bleepstore_http_response_size_bytes` | Histogram | method, path |

### S3 Operation Metrics
| Metric | Type | Labels |
|--------|------|--------|
| `bleepstore_s3_operations_total` | Counter | operation, status |
| `bleepstore_objects_total` | Gauge | (none) |
| `bleepstore_buckets_total` | Gauge | (none) |
| `bleepstore_bytes_received_total` | Counter | (none) |
| `bleepstore_bytes_sent_total` | Counter | (none) |

### Path Normalization
- `/health` -> `/health`
- `/docs*` -> `/docs`
- `/metrics` -> `/metrics`
- `/openapi.json` -> `/openapi`
- `/` -> `/`
- `/{anything}` -> `/{bucket}`
- `/{anything}/{anything}` -> `/{bucket}/{key}`

## Issues Encountered
- None significant. The hybrid approach worked cleanly.
- `commonHeaders` and `generateRequestID` moved from server.go to middleware.go to keep server.go focused on routing.

## Endpoints After Stage 1b
| Endpoint | Method | Response |
|----------|--------|----------|
| `/health` | GET | `{"status":"ok"}` (JSON) |
| `/health` | HEAD | 200 OK |
| `/docs` | GET | Stoplight Elements HTML |
| `/openapi.json` | GET | OpenAPI 3.x JSON |
| `/metrics` | GET | Prometheus exposition format |
| All S3 routes | * | 501 NotImplemented XML |
