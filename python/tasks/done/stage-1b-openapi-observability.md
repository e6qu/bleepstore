# Stage 1b: OpenAPI, Observability & Validation

**Completed:** 2026-02-23

## What Was Implemented

### 1. Prometheus Metrics (src/bleepstore/metrics.py)
New file defining 6 custom BleepStore Prometheus metrics:
- `bleepstore_s3_operations_total` -- Counter with `operation` and `status` labels
- `bleepstore_objects_total` -- Gauge for total objects across all buckets
- `bleepstore_buckets_total` -- Gauge for total buckets
- `bleepstore_bytes_received_total` -- Counter for total bytes received in request bodies
- `bleepstore_bytes_sent_total` -- Counter for total bytes sent in response bodies
- `bleepstore_http_request_duration_seconds` -- Histogram with custom buckets [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]

### 2. Instrumentator Integration (src/bleepstore/server.py)
- App title changed from "BleepStore" to "BleepStore S3 API"
- `prometheus-fastapi-instrumentator` wired in `create_app()`:
  - Auto HTTP metrics (requests_total, duration, request/response sizes)
  - `/metrics` endpoint excluded from instrumentation
  - In-progress request tracking enabled
- `bleepstore.metrics` module imported to register custom metrics in default registry

### 3. RequestValidationError Handler (src/bleepstore/server.py)
- Added `@app.exception_handler(RequestValidationError)` that maps Pydantic validation errors to S3 error XML
- Returns `InvalidArgument` (400) with human-readable error details
- HEAD requests return 400 with no body
- Prevents FastAPI's default JSON error format from leaking through

### 4. Input Validation Functions (src/bleepstore/validation.py)
New file with three validation functions:
- `validate_bucket_name(name)` -- Full S3 bucket naming rules:
  - 3-63 characters
  - Lowercase letters, digits, hyphens, periods only
  - Must start and end with letter or digit
  - Must not be IP address format
  - Must not start with `xn--`
  - Must not end with `-s3alias` or `--ol-s3`
  - No consecutive periods (`..`)
- `validate_object_key(key)` -- Max 1024 bytes (UTF-8 encoded)
- `validate_max_keys(value)` -- Integer in [0, 1000] range

**Note:** Validation functions are defined but NOT wired into stub handlers. They will be wired in Stage 3 (bucket CRUD) and Stage 4 (object CRUD).

### 5. Dependencies (pyproject.toml)
Added:
- `prometheus-client` -- Custom Prometheus metrics
- `prometheus-fastapi-instrumentator` -- Auto HTTP metrics for FastAPI

## Files Changed
- `pyproject.toml` -- Added 2 dependencies
- `src/bleepstore/server.py` -- Title, Instrumentator, RequestValidationError handler, metrics import
- `src/bleepstore/metrics.py` -- **New** -- Custom Prometheus metrics
- `src/bleepstore/validation.py` -- **New** -- S3 input validation functions
- `tests/test_metrics.py` -- **New** -- 10 tests for /metrics endpoint
- `tests/test_openapi.py` -- **New** -- 8 tests for /docs and /openapi.json
- `tests/test_validation.py` -- **New** -- 24 tests for validation functions

## Key Decisions
1. Validation functions are standalone (not coupled to handlers) for testability and reuse
2. Not wiring validation into 501-stub handlers per instructions -- deferred to Stage 3+
3. Using module-level metrics (singletons in prometheus_client default registry)
4. RequestValidationError handler produces S3 error XML, not JSON
5. Custom histogram buckets match the observability spec exactly

## Issues Encountered
None.
