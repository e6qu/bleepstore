# Stage 1: Server Bootstrap & Configuration

**Date:** 2026-02-22
**Status:** Implemented (pending build verification)

## What Was Implemented

### 1. Configuration (`src/config.rs`)
- Rewrote Config structs to match `bleepstore.example.yaml` format
- `ServerConfig` now has `host`, `port`, `region` with defaults
- `AuthConfig` uses `access_key`/`secret_key` (with `alias` for `access_key_id`/`secret_access_key`)
- `MetadataConfig` has nested `SqliteConfig` under `metadata.sqlite.path`
- `StorageConfig` has nested `LocalStorageConfig` under `storage.local.root_dir`
- Added `AwsStorageConfig`, `GcpStorageConfig`, `AzureStorageConfig` stubs
- Default port changed from 3000 to **9012**
- All top-level config sections have `#[serde(default)]` so missing sections use defaults

### 2. Error Types (`src/errors.rs`)
- Added missing error variants: `BucketAlreadyOwnedByYou`, `EntityTooSmall`, `MalformedXML`, `InvalidAccessKeyId`, `NotImplemented`, `MethodNotAllowed`, `MissingContentLength`, `InvalidRange`
- Added `generate_request_id()` function: generates 16-char uppercase hex string using `rand::random::<[u8; 8]>()`
- `IntoResponse` impl now includes common headers: `x-amz-request-id`, `Date` (RFC 7231 via httpdate), `Server: BleepStore`, `Content-Type: application/xml`

### 3. Router & Routing (`src/server.rs`)
- Complete rewrite with query-parameter-based dispatch
- Health check: `GET /health` returns `{"status":"ok"}`
- Service-level: `GET /` -> ListBuckets
- Bucket-level: single handler per method dispatches based on query params
  - `GET /{bucket}` -> dispatches to: `?location`, `?acl`, `?uploads`, `?list-type=2`, or default ListObjects
  - `PUT /{bucket}` -> dispatches to: `?acl` or default CreateBucket
  - `DELETE /{bucket}` -> DeleteBucket
  - `HEAD /{bucket}` -> HeadBucket
  - `POST /{bucket}` -> `?delete` or NotImplemented
- Object-level: single handler per method dispatches based on query params + headers
  - `GET /{bucket}/{*key}` -> `?acl`, `?uploadId`, or default GetObject
  - `PUT /{bucket}/{*key}` -> `?acl`, `?partNumber&uploadId`, `x-amz-copy-source`, or default PutObject
  - `DELETE /{bucket}/{*key}` -> `?uploadId` or default DeleteObject
  - `HEAD /{bucket}/{*key}` -> HeadObject
  - `POST /{bucket}/{*key}` -> `?uploads`, `?uploadId`, or NotImplemented
- Common headers middleware via `axum::middleware::from_fn`: adds `x-amz-request-id`, `Date`, `Server: BleepStore` to every response

### 4. Handlers (`src/handlers/`)
- All handler stubs changed from `todo!()` to `Err(S3Error::NotImplemented)`
- All return `Result<impl IntoResponse, S3Error>` with 501 + proper S3 error XML

### 5. Main Entry Point (`src/main.rs`)
- CLI args: `--config` (default `bleepstore.example.yaml`), `--bind` (optional override)
- Config loading from YAML
- Crash-only startup log message (placeholder for future recovery steps)
- Graceful shutdown via SIGTERM/SIGINT handlers: stop accepting connections, wait for in-flight, exit. No cleanup.

### 6. XML Rendering (`src/xml.rs`)
- No changes needed -- `render_error` already works correctly

### 7. Dependencies Added (`Cargo.toml`)
- `rand = "0.8"` -- for request ID generation
- `httpdate = "1"` -- for RFC 7231 date formatting

## Key Decisions
- Default port is 9012 (per Rust implementation assignment)
- Query parameter dispatch happens in `server.rs` routing layer, not in individual handlers
- Common headers applied via middleware layer (runs on all responses including errors)
- Error responses include both the middleware headers AND explicit headers from `IntoResponse` (middleware skips `x-amz-request-id` if already set)
- `rand::random` used for request IDs (simple, no dependency on uuid for this)
- Used `httpdate::fmt_http_date` for RFC 7231 dates (lightweight, correct)

## Files Changed
- `Cargo.toml` -- added rand, httpdate dependencies
- `src/config.rs` -- complete rewrite for YAML compatibility
- `src/errors.rs` -- added 8 error variants, request ID generation, common headers on errors
- `src/server.rs` -- complete rewrite with query-based routing, health check, middleware
- `src/main.rs` -- graceful shutdown, crash-only startup, default config path
- `src/handlers/bucket.rs` -- replaced todo!() with NotImplemented
- `src/handlers/object.rs` -- replaced todo!() with NotImplemented
- `src/handlers/multipart.rs` -- replaced todo!() with NotImplemented

## Not Changed (intentionally)
- `src/xml.rs` -- already works correctly for error rendering
- `src/auth.rs` -- not needed in Stage 1 (todo!() stubs are fine, never called)
- `src/metadata/` -- not needed in Stage 1 (never called)
- `src/storage/` -- not needed in Stage 1 (never called)
- `src/cluster/` -- not needed in Stage 1 (never called)
- `src/lib.rs` -- module structure unchanged
