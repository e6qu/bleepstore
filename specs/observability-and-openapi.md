# Observability & OpenAPI

## Overview

Every BleepStore implementation must expose **OpenAPI documentation**, **request validation**,
and **Prometheus metrics** from Stage 1b onward. These are cross-cutting concerns that apply
to every stage, similar to crash-only design.

**Consistent endpoints across all implementations:**

| Endpoint | Purpose | Response |
|----------|---------|----------|
| `/docs` | Swagger UI (interactive API documentation) | HTML |
| `/openapi.json` | OpenAPI 3.x JSON specification | `application/json` |
| `/metrics` | Prometheus exposition format metrics | `text/plain; version=0.0.4` |
| `/health` | Health check (existing from Stage 1) | `application/json` |

---

## OpenAPI Serving

### Specification

- Each implementation serves an OpenAPI 3.x specification at `/openapi.json`
- The spec must reflect all currently implemented routes (auto-generated where possible)
- Reference `schemas/s3-api.openapi.yaml` as the canonical S3 API specification
- The spec grows as new stages are implemented (Stage 1b starts with health + stubs)

### Swagger UI

- Interactive API documentation served at `/docs`
- Must render the `/openapi.json` spec
- Supports "Try It Out" functionality for testing endpoints directly

### Requirements

- `GET /docs` returns `200` with `Content-Type: text/html`
- `GET /openapi.json` returns `200` with `Content-Type: application/json`
- The OpenAPI JSON must be valid per the OpenAPI 3.x specification
- The spec must include at minimum: info block, server URL, all implemented paths

---

## Request Validation

### Purpose

Reject malformed requests early with proper S3 error XML responses. Validation occurs
before handler logic, catching issues like:

- Invalid bucket names (wrong characters, too short/long)
- Missing required headers
- Invalid query parameter values (e.g., non-integer `max-keys`)
- Malformed XML request bodies (e.g., `DeleteObjects`, `CompleteMultipartUpload`)
- Invalid content types where required

### Per-Language Approach

| Language | Validation Library | Approach |
|----------|-------------------|----------|
| Python | Pydantic (built-in with FastAPI) | Pydantic models on request bodies and query params; FastAPI auto-validates |
| Go | Huma (built-in JSON Schema) | Struct tags with Huma validation; auto-rejects with proper errors |
| Rust | garde + axum-valid | `#[derive(Validate)]` on input structs; axum extractor integration |
| Zig | Hand-written | Explicit validation functions for each input type |

### Error Responses

Validation failures must return proper S3 error XML, not framework-default error formats.
Map validation errors to the appropriate S3 error code:

| Validation Failure | S3 Error Code | HTTP Status |
|-------------------|---------------|-------------|
| Invalid bucket name | `InvalidBucketName` | 400 |
| Invalid argument value | `InvalidArgument` | 400 |
| Malformed XML body | `MalformedXML` | 400 |
| Missing required header | `MissingContentLength` | 411 |
| Value out of range | `InvalidArgument` | 400 |

---

## Prometheus Metrics

### Endpoint

- `GET /metrics` returns metrics in Prometheus exposition format (`text/plain; version=0.0.4`)
- The metrics endpoint itself is **not** counted in HTTP metrics (exclude from instrumentation)
- The `/health` endpoint **is** counted in HTTP metrics

### Metric Names

All metrics use the `bleepstore_` prefix for namespace isolation.

#### HTTP Metrics (RED: Rate, Errors, Duration)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `bleepstore_http_requests_total` | Counter | `method`, `path`, `status` | Total HTTP requests |
| `bleepstore_http_request_duration_seconds` | Histogram | `method`, `path` | Request latency in seconds |
| `bleepstore_http_request_size_bytes` | Histogram | `method`, `path` | Request body size |
| `bleepstore_http_response_size_bytes` | Histogram | `method`, `path` | Response body size |

**Label conventions:**
- `method`: HTTP method (GET, PUT, POST, DELETE, HEAD)
- `path`: Normalized path template (e.g., `/{bucket}`, `/{bucket}/{key}`, `/health`, `/docs`)
- `status`: HTTP status code as string (e.g., `200`, `404`, `501`)

#### S3 Operation Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `bleepstore_s3_operations_total` | Counter | `operation`, `status` | S3 operations by type |
| `bleepstore_objects_total` | Gauge | | Total objects across all buckets |
| `bleepstore_buckets_total` | Gauge | | Total buckets |
| `bleepstore_bytes_received_total` | Counter | | Total bytes received (request bodies) |
| `bleepstore_bytes_sent_total` | Counter | | Total bytes sent (response bodies) |

**S3 operation labels:**
- `operation`: S3 operation name (e.g., `ListBuckets`, `PutObject`, `GetObject`, `CreateMultipartUpload`)
- `status`: `success` or `error`

#### Runtime Metrics (Language-Specific)

| Language | Metrics |
|----------|---------|
| Python | `process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_open_fds`, `python_gc_collections_total` |
| Go | `go_goroutines`, `go_memstats_alloc_bytes`, `go_gc_duration_seconds` (auto-registered by `prometheus/client_golang`) |
| Rust | `process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_open_fds` |
| Zig | `process_resident_memory_bytes`, `process_uptime_seconds` |

### Histogram Buckets

Use Prometheus default histogram buckets for duration:
`[.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10]` seconds.

Use exponential buckets for size:
`[256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864]` bytes.

### Best-Effort Metrics

Per crash-only design: **metrics are best-effort**. Never block a request to update a counter.
If a counter increment fails (unlikely with in-memory counters), log and continue.
The `/metrics` scrape endpoint is read-only and safe at any time.

---

## Language-Specific Library Table

| Language | HTTP Framework | OpenAPI Library | Validation | Prometheus Library |
|----------|---------------|-----------------|------------|-------------------|
| Python | FastAPI + uvicorn | Built-in (FastAPI generates OpenAPI 3.1) | Pydantic (built-in) | `prometheus-fastapi-instrumentator` + `prometheus_client` |
| Go | Huma (+ Chi or stdlib router) | Built-in (Huma generates OpenAPI 3.1) | Built-in (Huma JSON Schema from struct tags) | `prometheus/client_golang` |
| Rust | axum (no change) | utoipa + utoipa-swagger-ui | garde + axum-valid | `metrics` + `metrics-exporter-prometheus` |
| Zig | tokamak (on httpz) | Built-in (tokamak basic OpenAPI 3.0) | Hand-written validation functions | Hand-rolled `/metrics` text output |

---

## Integration Requirements

### Middleware Ordering

Metrics middleware should wrap the entire request pipeline to capture accurate timing:

```
Request -> Metrics Start -> Auth -> Validation -> Handler -> Metrics End -> Response
```

The `/metrics` and `/docs` and `/openapi.json` endpoints should be excluded from
authentication middleware but included in HTTP metrics.

### Startup Behavior

- Metrics counters initialize to zero on startup (crash-only: no persistent counter state)
- Object/bucket gauge values should be populated from the metadata store on startup
  (query `SELECT COUNT(*) FROM objects` and `SELECT COUNT(*) FROM buckets`)
- If the metadata store is not yet initialized (Stage 1b), gauges remain at zero

### Testing

Stage 1b tests must verify:

1. `GET /docs` returns HTML containing "swagger" (case-insensitive)
2. `GET /openapi.json` returns valid JSON with `openapi` key
3. `GET /metrics` returns text containing `bleepstore_http_requests_total`
4. `GET /health` still returns `{"status":"ok"}`
5. All existing Stage 1 tests still pass (501 NotImplemented for S3 routes)
6. Malformed requests return S3 error XML (not framework error JSON)

---

## Crash-Only Considerations

| Concern | Approach |
|---------|----------|
| Counter persistence | None -- counters reset on restart. Prometheus handles gaps via `rate()`. |
| Metric update failure | Best-effort -- never block a request for metrics. |
| Swagger UI availability | Static asset or CDN -- no external dependency required at runtime. |
| OpenAPI spec generation | Generated at startup or on first request. No persistent state needed. |
| Validation errors | Must produce S3 error XML, not framework-specific error format. |
