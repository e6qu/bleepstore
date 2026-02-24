# Stage 1b: Framework Migration to tokamak, OpenAPI & Observability

## Date: 2026-02-23

## Summary
Migrated from std.http.Server to tokamak (on httpz). Added Swagger UI at /docs,
OpenAPI 3.0 JSON spec at /openapi.json, hand-rolled Prometheus metrics at /metrics,
and S3 input validation functions. All existing Stage 1 behavior preserved (501 stubs,
error XML, common headers).

## Files Created
- `src/metrics.zig` — Prometheus metrics with atomic counters
- `src/validation.zig` — S3 input validation (bucket names, object keys, max-keys, part numbers)

## Files Modified
- `build.zig.zon` — Added tokamak dependency
- `build.zig` — tokamak.setup() for exe and test targets
- `src/server.zig` — Major rewrite: tokamak routes, infrastructure handlers, S3 catch-all
- `src/handlers/bucket.zig` — Updated signatures for httpz Response
- `src/handlers/object.zig` — Updated signatures for httpz Response
- `src/handlers/multipart.zig` — Updated signatures for httpz Response
- `src/main.zig` — Added metrics init, validation/metrics module imports

## Key Decisions
1. **S3 routing as catch-all**: tokamak's path-based routing doesn't handle S3's query-param dispatch,
   so all S3 requests go through a single Context handler that does internal dispatch.
2. **HEAD via catch-all**: tokamak has no .head() route method; using Route{ .handler, .method=null }
   to match all methods including HEAD.
3. **CDN Swagger UI**: No embedded assets; Swagger UI loaded from unpkg CDN.
4. **Hand-built OpenAPI**: Static JSON string, not auto-generated from tokamak's OpenAPI support.
5. **Arena-allocated headers**: httpz stores header values as slices without copying, so dynamic
   values (Date, request-id) must be allocated via res.arena.

## Architecture
```
Request -> tokamak router
  /health        -> handleHealth (JSON)
  /metrics       -> handleMetrics (Prometheus text)
  /docs          -> handleSwaggerUi (HTML)
  /openapi.json  -> handleOpenApiJson (JSON)
  * (any method) -> handleS3CatchAll -> dispatchS3 -> handler stubs (501 XML)
```

## Metrics Provided
- bleepstore_http_requests_total (counter)
- bleepstore_s3_operations_total (counter)
- bleepstore_objects_total (gauge)
- bleepstore_buckets_total (gauge)
- bleepstore_bytes_received_total (counter)
- bleepstore_bytes_sent_total (counter)
- bleepstore_http_request_duration_seconds_sum (counter)
- process_uptime_seconds (gauge)

## Validation Functions
- isValidBucketName(name) -> ?S3Error (14 test cases)
- isValidObjectKey(key) -> ?S3Error
- validateMaxKeys(value) -> ?S3Error
- validatePartNumber(value) -> ?S3Error

## Tests
- metrics.zig: 3 test blocks (init, increment, render format)
- validation.zig: 14 test blocks (bucket name edge cases, object key, max-keys, part number)
- server.zig: 3 test blocks (hasQueryParam, generateRequestId, formatRfc1123Date)

## Bug Fixes (Session 6)
1. **hostname not passed** — Server was not passing config host to tokamak ListenOptions.hostname.
   Default was "127.0.0.1" instead of "0.0.0.0". Fixed.
2. **httpz Server moved after init** — httpz.Server contains self-referential pointers (router->handler).
   Storing in optional field and assigning moved the struct, invalidating pointers. Fixed by creating
   tk_server on the stack in run() instead of storing as a field.

## API Verification (Session 6)
All tokamak and httpz APIs verified against actual source code from GitHub:
- Route helpers (.get, .post, etc.), Route struct fields, Context struct
- httpz Response (status, body, content_type, arena, header method)
- httpz ContentType enum (.JSON, .HTML, .XML, .TEXT)
- httpz Method enum (.GET, .HEAD, .POST, .PUT, .DELETE)
- httpz Request (url.raw, url.path, url.query — query does NOT include ?)
- DI injector registration and handler wrapping (routeHandler + ctx.send)

## Note
Code written without bash access. Requires `zig fetch --save`, `zig build`, and `zig build test`
to verify. See DO_NEXT.md for exact steps.
