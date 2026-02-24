# Stage 1b: Framework Augmentation, OpenAPI & Observability

## Date: 2026-02-23

## What Was Implemented

### 1. Dependencies (Cargo.toml)
Added 5 new crates:
- `utoipa = { version = "5", features = ["axum_extras"] }` -- OpenAPI spec generation from proc macros
- `utoipa-swagger-ui = { version = "8", features = ["axum"] }` -- Swagger UI serving
- `garde = { version = "0.22", features = ["derive", "regex"] }` -- Struct validation with regex support
- `metrics = "0.24"` -- Metrics facade (counter, gauge, histogram)
- `metrics-exporter-prometheus = "0.16"` -- Prometheus recorder and text renderer

### 2. Prometheus Metrics (src/metrics.rs -- new file)
- **Global recorder**: `PrometheusBuilder` installed via `OnceLock` for idempotent initialization (safe for tests)
- **7 metric name constants**: `bleepstore_http_requests_total`, `bleepstore_http_request_duration_seconds`, `bleepstore_s3_operations_total`, `bleepstore_objects_total`, `bleepstore_buckets_total`, `bleepstore_bytes_received_total`, `bleepstore_bytes_sent_total`
- **`describe_metrics()`**: Registers descriptions for all metrics
- **`metrics_middleware`**: Axum middleware (Tower layer) that:
  - Times each request and records duration histogram
  - Increments request counter with method/path/status labels
  - Excludes `/metrics` from self-instrumentation
- **`normalize_path()`**: Maps actual request paths to templates to prevent high-cardinality labels
- **`metrics_handler`**: Returns Prometheus text format via `PrometheusHandle::render()`
- **Unit tests**: 6 tests for path normalization

### 3. OpenAPI (utoipa)
- **`ApiDoc` struct** in server.rs with `#[derive(OpenApi)]` collecting all 24 handler paths
- **Tags**: Health, Bucket, Object, Multipart
- **Swagger UI** at `/docs` via `SwaggerUi::new("/docs").url("/openapi.json", openapi)`
- All handler functions annotated with `#[utoipa::path]` including:
  - HTTP method and path
  - Tag and operation_id
  - Path/query parameters where applicable
  - Response status codes and descriptions

### 4. Validation (garde -- preparation)
- `BucketNameInput` struct with `#[derive(garde::Validate)]`
- Regex pattern: `^[a-z0-9][a-z0-9.\-]*[a-z0-9]$`
- Length constraint: min 3, max 63
- Not yet wired into handlers (will be activated in Stage 3)

### 5. Middleware Layering
- **Outer**: `metrics_middleware` (captures full request lifecycle)
- **Inner**: `common_headers_middleware` (adds x-amz-request-id, Date, Server)

### 6. main.rs Integration
- `init_metrics()` and `describe_metrics()` called before building the app

## Files Changed
- `Cargo.toml` -- Added 5 dependencies
- `src/lib.rs` -- Added `pub mod metrics;`
- `src/metrics.rs` -- New file (Prometheus metrics module)
- `src/server.rs` -- Added ApiDoc, SwaggerUi, /metrics route, metrics middleware layer
- `src/main.rs` -- Added metrics initialization
- `src/handlers/bucket.rs` -- utoipa annotations + BucketNameInput + garde validation struct
- `src/handlers/object.rs` -- utoipa annotations
- `src/handlers/multipart.rs` -- utoipa annotations

## Key Decisions
- Used `OnceLock` instead of `lazy_static` for the Prometheus handle (modern, stdlib-based)
- `metrics` 0.24 API requires explicit `.increment(1)` on counter! return value
- garde `regex` feature must be explicitly enabled for `#[garde(pattern(...))]`
- Path normalization uses simple segment counting rather than complex regex matching
- Kept axum `Router` (not `OpenApiRouter` from utoipa-axum) since utoipa 5 derive approach works well with manual router construction

## Issues Encountered
- garde 0.22 requires explicit `regex` feature flag for pattern validation (error: "regex feature must be enabled to use literal patterns")
- metrics 0.24 `counter!()` macro returns a `Counter` object that must have `.increment(1)` called (unlike older versions that auto-increment)
