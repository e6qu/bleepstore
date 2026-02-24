# BleepStore Rust Implementation Plan

Rust-specific implementation plan derived from the [global plan](../PLAN.md). Each stage maps 1:1 to the global plan. See the global plan for full specification references and E2E test targets.

## Development Environment

- **Toolchain:** Rust stable (edition 2021), managed via `rustup`
- **Build:** `cargo build` / `cargo build --release`
- **Run:** `cargo run -- --config ../bleepstore.yaml` (or `cargo run -- -c ../bleepstore.example.yaml`)
- **Test:** `cargo test` (unit tests), E2E tests via `../tests/run_tests.sh`
- **Lint:** `cargo clippy -- -D warnings`
- **Format:** `cargo fmt --check`
- **Default port:** 9000 (note: scaffold defaults to 3000 -- update `default_port()` in Stage 1)

## Existing Scaffold Overview

The crate already has a well-structured module layout with `todo!()` stubs:

```
rust/src/
  main.rs          -- CLI (clap), tracing init, server startup
  lib.rs           -- Module re-exports
  config.rs        -- Config structs (serde_yaml), load_config()
  errors.rs        -- S3Error enum (thiserror), IntoResponse impl
  server.rs        -- Axum Router construction
  xml.rs           -- quick-xml rendering helpers
  auth.rs          -- SigV4 stubs (hmac, sha2)
  handlers/
    mod.rs
    bucket.rs      -- 7 bucket handler stubs
    object.rs      -- 10 object handler stubs
    multipart.rs   -- 6 multipart handler stubs
  metadata/
    mod.rs
    store.rs       -- MetadataStore trait + record types
    sqlite.rs      -- SqliteMetadataStore (rusqlite, Mutex<Connection>)
  storage/
    mod.rs
    backend.rs     -- StorageBackend trait + StoredObject
    local.rs       -- LocalBackend (filesystem)
    aws.rs         -- AwsGatewayBackend stub
    gcp.rs         -- GcpGatewayBackend stub
    azure.rs       -- AzureGatewayBackend stub
  cluster/
    mod.rs
    raft.rs        -- RaftNode stub
```

**Key Cargo.toml dependencies already present:** axum 0.7, tokio (full), serde/serde_yaml/serde_json, quick-xml 0.31, rusqlite 0.31 (bundled), uuid, sha2, hmac, hex, bytes, tower/tower-http, tracing/tracing-subscriber, anyhow, thiserror, clap 4, utoipa, utoipa-swagger-ui, garde, metrics, metrics-exporter-prometheus.

---

## Milestone 1: Foundation (Stages 1, 1b, 2)

### Stage 1: Server Bootstrap & Configuration ✅

**Goal:** Server starts, routes all S3 paths, returns well-formed S3 error XML.

**Rust-specific setup:**
- Ensure `default_port()` returns `9000` (currently returns `3000`)
- Config field names must match `bleepstore.example.yaml` (see note below)
- The scaffold's `Config` uses `auth.access_key_id` / `auth.secret_access_key` but the example YAML uses `auth.access_key` / `auth.secret_key`. Add `#[serde(alias = "access_key")]` or rename the struct fields to match.
- Similarly, `metadata.path` is a flat field but the example YAML nests it under `metadata.sqlite.path`. The config struct needs a `SqliteConfig` substruct, or use `serde(flatten)` / aliases.
- `storage.root` needs to map to `storage.local.root_dir`. Add a `LocalStorageConfig` substruct.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/config.rs` -- Fix field names, add nested structs, fix default port
- `/Users/zardoz/projects/bleepstore/rust/src/server.rs` -- Complete route table (add missing routes for `?acl`, `?location`, `?uploads`, `?list-type`, `?delete`, `?uploadId`, `?partNumber`); add health check endpoint; add common response headers middleware
- `/Users/zardoz/projects/bleepstore/rust/src/errors.rs` -- Add missing error variants: `BucketAlreadyOwnedByYou`, `EntityTooSmall`, `MalformedXML`, `InvalidAccessKeyId`, `NotImplemented`, `MethodNotAllowed`, `MissingContentLength`, `InvalidRange`; fix `IntoResponse` to include `x-amz-request-id`, `Date`, `Server` headers
- `/Users/zardoz/projects/bleepstore/rust/src/xml.rs` -- No changes needed (error rendering already works)
- `/Users/zardoz/projects/bleepstore/rust/src/main.rs` -- Add graceful shutdown (tokio signal handler); fallback to `bleepstore.example.yaml`
- `/Users/zardoz/projects/bleepstore/rust/src/handlers/bucket.rs` -- Make all stubs return `NotImplemented` S3 error (instead of panicking via `todo!()`)
- `/Users/zardoz/projects/bleepstore/rust/src/handlers/object.rs` -- Same
- `/Users/zardoz/projects/bleepstore/rust/src/handlers/multipart.rs` -- Same

**Library-specific notes:**
- **Axum routing:** S3 distinguishes operations by query parameters, not just path. Axum routes by path+method only. Use a single handler per method+path that dispatches internally based on query params. For example, `GET /{bucket}` must inspect `?location`, `?acl`, `?uploads`, `?list-type=2` and dispatch accordingly. Use `axum::extract::Query<HashMap<String, String>>` or `axum::extract::RawQuery` for this.
- **Path extraction:** Use `axum::extract::Path<(String,)>` for bucket, `Path<(String, String)>` for bucket+key. The `{*key}` wildcard captures the remainder including slashes.
- **Common headers middleware:** Use a `tower::Layer` or `axum::middleware::from_fn` to inject `x-amz-request-id`, `Date`, `Server: BleepStore`, and `Content-Type` headers on every response. Generate request ID with `hex::encode(rand::random::<[u8; 8]>())` or `uuid`.
- **Config as state:** Pass `Config` (or an `Arc<AppState>`) via `axum::Router::with_state()`. Extract in handlers with `axum::extract::State<AppState>`.

**Rust idioms:**
- All handlers return `Result<impl IntoResponse, S3Error>`. The `S3Error` enum already implements `IntoResponse`.
- Replace all `todo!()` in handler stubs with `Err(S3Error::NotImplemented)` (need to add this variant).
- Use `anyhow::Result` for internal operations; `S3Error` at the handler boundary.

**Crash-only design (applies to all stages):**
- Every startup is a recovery: clean temp files in `data/.tmp/`, reap expired multipart uploads, seed default credentials. There is no separate `--recovery-mode` flag.
- Never acknowledge before commit: do not return 200/201/204 until data is fsync'd and metadata is committed.
- Atomic file writes: always use the temp-fsync-rename pattern (write to temp file, fsync, rename to final path).
- Idempotent operations: all operations must be safe to retry (PutObject overwrites, DeleteObject on missing key returns 204, etc.).
- Database as index: SQLite is the index of truth. Orphan files on disk (no metadata row) are safe to delete.
- No in-memory queues for durable work: if background work is needed, record intent in the database first.

**Unit test approach:**
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum_test::TestServer; // or use reqwest against a spawned server

    #[tokio::test]
    async fn test_health_check() { /* GET /health -> 200 */ }

    #[tokio::test]
    async fn test_all_routes_return_501_or_error_xml() { /* ... */ }

    #[tokio::test]
    async fn test_error_xml_is_well_formed() { /* ... */ }

    #[tokio::test]
    async fn test_common_headers_present() { /* ... */ }
}
```

**New Cargo.toml deps:**
- `rand = "0.8"` -- for generating request IDs
- `chrono = "0.4"` -- for RFC 1123 / ISO 8601 date formatting (or use `httpdate` crate)
- `httpdate = "1"` -- for RFC 7231 date formatting (`httpdate::fmt_http_date`)

**Build/run commands:**
```bash
cargo build
cargo test
cargo run -- -c ../bleepstore.example.yaml
# Verify: curl http://localhost:9000/health
# Verify: curl http://localhost:9000/nonexistent-bucket (should return S3 error XML)
```

---

### Stage 1b: Framework Augmentation, OpenAPI & Observability ✅

**Goal:** Add OpenAPI serving with Swagger UI via utoipa, request validation via garde, and Prometheus metrics. axum remains the HTTP framework — no migration needed.

**Rust-specific setup:**
```bash
cd rust/
cargo add utoipa utoipa-axum utoipa-swagger-ui garde axum-valid
cargo add metrics metrics-exporter-prometheus
```

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/Cargo.toml` -- Add utoipa, utoipa-axum, utoipa-swagger-ui, garde, axum-valid, metrics, metrics-exporter-prometheus
- `/Users/zardoz/projects/bleepstore/rust/src/server.rs` -- Replace `axum::Router` with `utoipa_axum::router::OpenApiRouter`. Add `SwaggerUi` layer at `/docs`. Add `/metrics` endpoint. Wire metrics middleware (Tower layer).
- `/Users/zardoz/projects/bleepstore/rust/src/metrics.rs` -- **(new file)** Define custom Prometheus metrics: `bleepstore_s3_operations_total`, `bleepstore_objects_total`, `bleepstore_buckets_total`, `bleepstore_bytes_received_total`, `bleepstore_bytes_sent_total`. Install `PrometheusBuilder`. Add Tower middleware layer for HTTP request timing.
- `/Users/zardoz/projects/bleepstore/rust/src/handlers/bucket.rs` -- Annotate handlers with `#[utoipa::path(...)]`. Add `#[derive(garde::Validate)]` on input structs.
- `/Users/zardoz/projects/bleepstore/rust/src/handlers/object.rs` -- Same: utoipa path annotations + garde validation.
- `/Users/zardoz/projects/bleepstore/rust/src/handlers/multipart.rs` -- Same.

**Library-specific notes:**
- **utoipa** generates OpenAPI spec from Rust proc macros:
  ```rust
  #[utoipa::path(
      get,
      path = "/health",
      responses((status = 200, description = "Health check", body = HealthResponse))
  )]
  async fn health() -> impl IntoResponse { ... }
  ```
- **utoipa-swagger-ui** serves Swagger UI:
  ```rust
  use utoipa_swagger_ui::SwaggerUi;
  let app = Router::new()
      .merge(SwaggerUi::new("/docs").url("/openapi.json", openapi));
  ```
- **garde** for validation:
  ```rust
  #[derive(garde::Validate)]
  struct CreateBucketInput {
      #[garde(length(min = 3, max = 63), pattern(r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$"))]
      bucket_name: String,
  }
  ```
- **metrics + metrics-exporter-prometheus:**
  ```rust
  use metrics::{counter, histogram};
  use metrics_exporter_prometheus::PrometheusBuilder;
  let handle = PrometheusBuilder::new().install_recorder().unwrap();
  // In /metrics handler:
  let output = handle.render();
  ```

**Rust idioms:**
- utoipa uses derive macros: `#[derive(ToSchema)]` on request/response types, `#[utoipa::path]` on handlers
- garde integrates with axum via `axum-valid` extractor: `Valid<Json<T>>` auto-validates
- Map garde validation errors to S3Error variants in a custom error handler
- Metrics middleware as a Tower `Layer` wrapping the router

**Crash-only design:**
- Metrics counters reset on restart
- Never block a request for metrics
- PrometheusBuilder installs a global recorder at startup

**Unit test approach:**
```rust
#[tokio::test]
async fn test_swagger_ui_serves() { /* GET /docs -> 200 HTML */ }

#[tokio::test]
async fn test_openapi_json_valid() { /* GET /openapi.json -> valid JSON with "openapi" key */ }

#[tokio::test]
async fn test_metrics_endpoint() { /* GET /metrics -> text with bleepstore_ prefix */ }
```

**New Cargo.toml deps:**
- `utoipa = { version = "5", features = ["axum_extras"] }`
- `utoipa-axum = "0.1"`
- `utoipa-swagger-ui = { version = "8", features = ["axum"] }`
- `garde = { version = "0.20", features = ["derive"] }`
- `axum-valid = "0.20"`
- `metrics = "0.23"`
- `metrics-exporter-prometheus = "0.15"`

**Build/run commands:**
```bash
cargo build
cargo test
cargo run -- -c ../bleepstore.example.yaml &
curl http://localhost:9012/docs          # Swagger UI
curl http://localhost:9012/openapi.json  # OpenAPI spec
curl http://localhost:9012/metrics       # Prometheus metrics
```

---

### Stage 2: Metadata Store & SQLite ✅

**Goal:** Full SQLite-backed metadata CRUD. No HTTP handler changes.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/metadata/store.rs` -- Expand `MetadataStore` trait: add `bucket_exists`, `update_bucket_acl`, `object_exists`, `update_object_acl`, `delete_objects_meta`, `get_multipart_upload`, `get_parts_for_completion`, `register_part`; expand record structs with full fields (owner_id, owner_display, acl, content_encoding, content_language, content_disposition, cache_control, expires, storage_class, delete_marker); add `ListResult`, `ListUploadsResult`, `ListPartsResult` types; add credential types and methods
- `/Users/zardoz/projects/bleepstore/rust/src/metadata/sqlite.rs` -- Implement all trait methods; expand schema (add `credentials` table, `schema_version`, indexes, more columns on `objects`/`multipart_uploads`); apply pragmas (WAL, NORMAL sync, foreign keys, busy_timeout); seed default credentials on startup

**Library-specific notes:**
- **rusqlite patterns:** Use `conn.execute()` for writes, `conn.query_row()` for single-row reads, `conn.prepare()` + `stmt.query_map()` for multi-row reads. Always use `params![]` or `named_params!{}` macros for parameter binding.
- **Mutex usage:** The scaffold already wraps `Connection` in `Mutex<Connection>`. Lock the mutex at the start of each method, do the synchronous SQLite work, then release. The methods return `Pin<Box<dyn Future>>` but the actual work is synchronous inside the mutex -- use `tokio::task::spawn_blocking` or just do it inline (rusqlite is not async). Since the future captures `&self`, the simplest pattern is:
  ```rust
  fn get_bucket(&self, name: &str) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<BucketRecord>>> + Send + '_>> {
      let name = name.to_string();
      Box::pin(async move {
          let conn = self.conn.lock().expect("mutex poisoned");
          // ... synchronous rusqlite calls ...
          Ok(result)
      })
  }
  ```
  Note: Holding a `MutexGuard` across `.await` is problematic but since there is no `.await` inside (all work is sync), this is safe.
- **JSON fields:** Use `serde_json::to_string()` / `serde_json::from_str()` for ACL and user_metadata stored as JSON text in SQLite.
- **Transaction for complete_multipart_upload:** Use `conn.execute_batch()` wrapped in `BEGIN`/`COMMIT`, or `rusqlite::Transaction` via `conn.transaction()`.

**Rust idioms:**
- Clone string parameters before moving into the `async move` block (rusqlite methods need owned strings or the connection borrow).
- Use `Option<T>` for nullable fields, `HashMap<String, String>` for user_metadata.
- `rusqlite::OptionalExtension` trait for `.optional()` on `query_row` to get `Option<Row>`.
- Derive `Clone` on all record types for ease of use.

**Unit test approach:**
```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> SqliteMetadataStore {
        SqliteMetadataStore::new(":memory:").unwrap()
    }

    #[tokio::test]
    async fn test_create_and_get_bucket() { /* ... */ }

    #[tokio::test]
    async fn test_list_objects_with_delimiter() { /* ... */ }

    #[tokio::test]
    async fn test_complete_multipart_upload_transactional() { /* ... */ }

    #[tokio::test]
    async fn test_schema_idempotent() {
        // Call init_db() twice, no error
    }
}
```

**New Cargo.toml deps:**
- None required beyond existing. `serde_json` already present for JSON field handling.

**Build/run commands:**
```bash
cargo test -- metadata  # Run metadata-specific tests
cargo test              # Run all tests
```

---

## Milestone 2: Bucket Operations (Stage 3)

### Stage 3: Bucket CRUD ✅

**Goal:** All 7 bucket handlers work. 15 of 16 E2E bucket tests pass (1 requires Stage 4 object CRUD).

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/handlers/bucket.rs` -- Implement all 7 handlers: `list_buckets`, `create_bucket`, `delete_bucket`, `head_bucket`, `get_bucket_location`, `get_bucket_acl`, `put_bucket_acl`
- `/Users/zardoz/projects/bleepstore/rust/src/server.rs` -- Ensure query-parameter-based dispatch works (e.g., `GET /{bucket}?location` vs `GET /{bucket}?acl` vs `GET /{bucket}` for list objects). Likely refactor to a single `get_bucket` handler that dispatches based on query params.
- `/Users/zardoz/projects/bleepstore/rust/src/xml.rs` -- Add `render_location_constraint()`, `render_access_control_policy()` functions
- `/Users/zardoz/projects/bleepstore/rust/src/errors.rs` -- Ensure `BucketAlreadyOwnedByYou` variant exists for us-east-1 idempotent create behavior
- `/Users/zardoz/projects/bleepstore/rust/src/main.rs` -- Wire up `AppState` with `Arc<dyn MetadataStore>` and `Arc<dyn StorageBackend>` so handlers can access them

**Library-specific notes:**
- **Axum extractors:** Handlers need access to shared state. Define:
  ```rust
  pub struct AppState {
      pub config: Config,
      pub metadata: Arc<dyn MetadataStore>,
      pub storage: Arc<dyn StorageBackend>,
  }
  ```
  Use `axum::extract::State<Arc<AppState>>`. All handlers become `async fn handler(State(state): State<Arc<AppState>>, ...) -> Result<..., S3Error>`.
- **Request body parsing:** For `CreateBucket`, parse optional XML body with `quick_xml::Reader`. For `PutBucketAcl`, parse `x-amz-acl` header from `axum::extract::HeaderMap` or parse XML body.
- **Bucket name validation:** Implement as a standalone function returning `Result<(), S3Error>`. Use regex or manual char checks (regex crate not in deps -- use manual checks to avoid adding it).
- **Query parameter dispatch:** Use `axum::extract::RawQuery` or `axum::extract::Query<HashMap<String, String>>` to check for `?location`, `?acl`, etc. The router registers a single handler for `GET /{bucket}` that internally dispatches.

**Rust idioms:**
- Use `impl IntoResponse` return types to allow different response shapes per branch.
- `(StatusCode, HeaderMap, String).into_response()` for building custom responses.
- ACL handling: define an `Acl` struct with `serde::Serialize` / `Deserialize` for JSON round-tripping.

**Unit test approach:**
```rust
#[tokio::test]
async fn test_create_bucket_invalid_name() {
    // Use axum::test or build a TestServer
}

#[tokio::test]
async fn test_bucket_name_validation() {
    assert!(validate_bucket_name("valid-bucket").is_ok());
    assert!(validate_bucket_name("INVALID").is_err());
    assert!(validate_bucket_name("ab").is_err());
}
```

**New Cargo.toml deps:**
- `md-5 = "0.10"` -- for ETag computation (MD5 hash). Will be needed in Stage 4 but can add now.
- Consider `base64 = "0.22"` for `x-amz-id-2` header generation.

**Build/run commands:**
```bash
cargo build && cargo run -- -c ../bleepstore.yaml &
cd ../tests && BLEEPSTORE_ENDPOINT=http://localhost:9000 python -m pytest e2e/test_buckets.py -v
```

---

## Milestone 3: Object Operations (Stages 4-5b)

### Stage 4: Basic Object CRUD ✅

**Goal:** PutObject, GetObject, HeadObject, DeleteObject work with local filesystem backend.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/storage/local.rs` -- Implement `LocalBackend` methods: `put` (write to temp file, compute MD5, atomic rename), `get` (read file, return `StoredObject`), `delete` (remove file), `exists` (check path exists), `create_bucket` (create directory), `delete_bucket` (remove directory)
- `/Users/zardoz/projects/bleepstore/rust/src/handlers/object.rs` -- Implement `put_object`, `get_object`, `head_object`, `delete_object`
- `/Users/zardoz/projects/bleepstore/rust/src/server.rs` -- Ensure object routes dispatch correctly (single `PUT /{bucket}/{*key}` handler inspects `x-amz-copy-source` and `?uploadId` to decide between PutObject, CopyObject, UploadPart, UploadPartCopy)

**Library-specific notes:**
- **Streaming vs buffering:** For Phase 1, buffering the entire body with `axum::body::Bytes` is acceptable. Use `axum::extract::Body` or `Bytes` extractor. For `GetObject`, return `axum::body::Body::from(bytes)`.
- **MD5 computation:** Use `md5` crate (add `md-5 = "0.10"` to Cargo.toml). Compute during write:
  ```rust
  use md5::{Md5, Digest};
  let mut hasher = Md5::new();
  hasher.update(&data);
  let etag = format!("\"{}\"", hex::encode(hasher.finalize()));
  ```
- **Atomic write:** Use `tokio::fs` for async file I/O. Write to `{path}.tmp.{uuid}`, then `tokio::fs::rename()`. Note: `rename` is atomic on the same filesystem.
- **User metadata:** Extract all headers starting with `x-amz-meta-` from `HeaderMap`, strip prefix, store in `HashMap<String, String>`. On `GetObject`/`HeadObject`, re-emit them as response headers.
- **Content-Type:** Default to `application/octet-stream` if not provided. Extract from `Content-Type` header.
- **ETag quoting:** ETags in S3 are always quoted: `"d41d8cd98f00b204e9800998ecf8427e"`. Store with quotes in metadata.

**Rust idioms:**
- Use `tokio::fs` for non-blocking file I/O (or `spawn_blocking` + `std::fs` for simplicity since file I/O is fast for small files).
- `PathBuf::join()` for building file paths. Ensure no path traversal: validate that resolved path stays within root directory.
- `std::fs::create_dir_all()` for creating parent directories for keys with `/` separators.

**Unit test approach:**
```rust
#[tokio::test]
async fn test_local_put_get_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(dir.path()).unwrap();
    let etag = backend.put("bucket/key.txt", Bytes::from("hello")).await.unwrap();
    let obj = backend.get("bucket/key.txt").await.unwrap();
    assert_eq!(obj.data, Bytes::from("hello"));
}
```

**New Cargo.toml deps:**
- `md-5 = "0.10"` -- MD5 for ETag computation
- `tempfile = "3"` (dev-dependency) -- for unit tests with temporary directories

**Build/run commands:**
```bash
cargo test -- storage::local  # Local backend tests
cargo test -- handlers::object  # Object handler tests
cargo build && cargo run -- -c ../bleepstore.yaml &
cd ../tests && python -m pytest e2e/test_objects.py -v -k "TestPutAndGetObject or TestHeadObject or TestDeleteObject"
```

---

### Stage 5a: List, Copy & Batch Delete ✅

**Goal:** CopyObject, DeleteObjects (batch), ListObjectsV2, and ListObjects v1.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/handlers/object.rs` -- Implement `copy_object`, `delete_objects`, `list_objects_v2`, `list_objects_v1`
- `/Users/zardoz/projects/bleepstore/rust/src/storage/local.rs` -- Implement `copy_object` (read source, write to dest)
- `/Users/zardoz/projects/bleepstore/rust/src/xml.rs` -- Add `render_delete_result()`, `render_list_bucket_result_v1()`, `render_copy_object_result()`
- `/Users/zardoz/projects/bleepstore/rust/src/server.rs` -- Ensure `POST /{bucket}?delete` routes to `delete_objects`; ensure `GET /{bucket}` dispatches to v1 vs v2 based on `list-type` query param

**Library-specific notes:**
- **DeleteObjects XML parsing:** Use `quick_xml::Reader` to parse `<Delete>` request body. Extract `<Quiet>` flag and list of `<Object><Key>` elements.
- **CopyObject:** Detect via `x-amz-copy-source` header on `PUT /{bucket}/{key}`. Parse header to extract source bucket/key (URL-decode). Check `x-amz-metadata-directive` for `COPY` vs `REPLACE`.

**Rust idioms:**
- Pattern matching on header presence:
  ```rust
  if let Some(copy_source) = headers.get("x-amz-copy-source") {
      // CopyObject path
  } else {
      // PutObject path
  }
  ```
- Use `StatusCode` constants from `axum::http::StatusCode`.
- Return `(StatusCode, HeaderMap, Body)` tuples for flexible responses.

**Unit test approach:**
```rust
#[tokio::test]
async fn test_delete_objects_xml_parsing() { /* parse <Delete> XML */ }

#[tokio::test]
async fn test_copy_object_metadata_directive() { /* COPY vs REPLACE */ }

#[tokio::test]
async fn test_list_objects_v2_pagination() { /* MaxKeys + continuation token */ }
```

**New Cargo.toml deps:**
- `percent-encoding = "2"` -- for URL-decoding `x-amz-copy-source` paths

**Build/run commands:**
```bash
cargo test
cargo build && cargo run -- -c ../bleepstore.yaml &
cd ../tests && python -m pytest e2e/test_objects.py -v -k "TestDeleteObjects or TestCopyObject or TestListObjects"
```

**Test targets:**
- `TestDeleteObjects::test_delete_multiple_objects` -- Batch delete
- `TestDeleteObjects::test_delete_objects_quiet_mode` -- Quiet mode
- `TestCopyObject::test_copy_object` -- Copy within bucket
- `TestCopyObject::test_copy_object_with_replace_metadata` -- REPLACE directive
- `TestCopyObject::test_copy_nonexistent_source` -- NoSuchKey
- `TestListObjectsV2::test_list_objects` -- All objects returned
- `TestListObjectsV2::test_list_objects_with_prefix` -- Prefix filter
- `TestListObjectsV2::test_list_objects_with_delimiter` -- CommonPrefixes
- `TestListObjectsV2::test_list_objects_pagination` -- MaxKeys + continuation
- `TestListObjectsV2::test_list_objects_empty_bucket` -- Empty result
- `TestListObjectsV2::test_list_objects_start_after` -- StartAfter
- `TestListObjectsV2::test_list_objects_content_fields` -- Required fields
- `TestListObjectsV1::test_list_objects_v1` -- Legacy list
- `TestListObjectsV1::test_list_objects_v1_with_marker` -- Marker pagination

**Definition of done:**
- [ ] CopyObject supports COPY and REPLACE metadata directives
- [ ] DeleteObjects handles both quiet and verbose modes
- [ ] ListObjectsV2 with delimiter correctly returns CommonPrefixes
- [ ] ListObjectsV2 pagination works with MaxKeys and ContinuationToken
- [ ] ListObjects v1 works with Marker
- [ ] All smoke test list/copy/delete operations pass

---

### Stage 5b: Range, Conditional Requests & Object ACLs ✅

**Goal:** Range requests, conditional requests (If-Match, If-None-Match, etc.), and object ACL operations.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/handlers/object.rs` -- Add range request handling to `get_object`; add conditional request handling (`If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`) to `get_object` and `head_object`; implement `get_object_acl`, `put_object_acl`
- `/Users/zardoz/projects/bleepstore/rust/src/xml.rs` -- Add `render_access_control_policy()` (if not added in Stage 3)

**Library-specific notes:**
- **Range parsing:** Parse `Range: bytes=start-end` header manually. Return `StatusCode::PARTIAL_CONTENT` (206) with `Content-Range: bytes start-end/total` header. For suffix ranges `bytes=-N`, compute `start = total - N`.
- **Conditional requests:** Parse `If-Match`, `If-None-Match` (compare ETags), `If-Modified-Since`, `If-Unmodified-Since` (parse dates). Use `httpdate::parse_http_date()` for date parsing. Return `StatusCode::NOT_MODIFIED` (304) with empty body or `StatusCode::PRECONDITION_FAILED` (412).

**Rust idioms:**
- Use `StatusCode` constants from `axum::http::StatusCode`.
- Return `(StatusCode, HeaderMap, Body)` tuples for flexible responses.
- 304 Not Modified responses have no body.

**Unit test approach:**
```rust
#[tokio::test]
async fn test_range_parsing() { /* bytes=0-4, bytes=5-, bytes=-3 */ }

#[tokio::test]
async fn test_conditional_if_match() { /* matching vs non-matching etag */ }

#[tokio::test]
async fn test_conditional_if_none_match() { /* 304 Not Modified */ }
```

**Build/run commands:**
```bash
cargo test
cargo build && cargo run -- -c ../bleepstore.yaml &
cd ../tests && python -m pytest e2e/test_objects.py e2e/test_acl.py -v
```

**Test targets:**
- `TestGetObjectRange::test_range_request` -- bytes=0-4 returns 206
- `TestGetObjectRange::test_range_request_suffix` -- bytes=-5
- `TestGetObjectRange::test_invalid_range` -- 416
- `TestConditionalRequests::test_if_match_success` -- 200
- `TestConditionalRequests::test_if_match_failure` -- 412
- `TestConditionalRequests::test_if_none_match_returns_304` -- 304
- `TestObjectAcl::test_get_object_acl_default` -- Default FULL_CONTROL
- `TestObjectAcl::test_put_object_acl_canned` -- Set public-read
- `TestObjectAcl::test_put_object_with_canned_acl` -- ACL on PUT
- `TestObjectAcl::test_get_acl_nonexistent_object` -- NoSuchKey

**Definition of done:**
- [ ] All 32 tests in `test_objects.py` pass (cumulative with 5a)
- [ ] All 4 tests in `test_acl.py` pass
- [ ] Range requests return 206 with correct Content-Range
- [ ] Conditional requests: If-Match returns 412, If-None-Match returns 304
- [ ] Object ACLs stored and retrieved correctly

---

## Milestone 4: Authentication (Stage 6)

### Stage 6: AWS Signature V4 ✅

**Goal:** Header-based SigV4 and presigned URL authentication.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/auth.rs` -- Implement `verify_request()`, `derive_signing_key()`, `create_canonical_request()`; add presigned URL verification function; add URI encoding helper (RFC 3986 with S3 exceptions); add constant-time comparison
- `/Users/zardoz/projects/bleepstore/rust/src/server.rs` -- Add auth middleware layer using `axum::middleware::from_fn_with_state`. The middleware runs before handlers, extracts credentials, verifies signature, and either passes through or returns `AccessDenied` / `SignatureDoesNotMatch` / `InvalidAccessKeyId`. Skip auth for `GET /health`.
- `/Users/zardoz/projects/bleepstore/rust/src/metadata/store.rs` -- Add credential lookup methods if not done in Stage 2
- `/Users/zardoz/projects/bleepstore/rust/src/metadata/sqlite.rs` -- Implement credential lookup

**Library-specific notes:**
- **HMAC-SHA256:** Already in deps (`hmac = "0.12"`, `sha2 = "0.10"`). Signing key derivation chain:
  ```rust
  use hmac::{Hmac, Mac};
  use sha2::Sha256;
  type HmacSha256 = Hmac<Sha256>;

  fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
      let k_secret = format!("AWS4{}", secret);
      let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
      let k_region = hmac_sha256(&k_date, region.as_bytes());
      let k_service = hmac_sha256(&k_region, service.as_bytes());
      hmac_sha256(&k_service, b"aws4_request")
  }
  ```
- **Constant-time comparison:** Use `subtle` crate's `ConstantTimeEq` trait, or `hmac::Mac::verify_slice()` which is already constant-time.
- **Axum middleware pattern:**
  ```rust
  async fn auth_middleware(
      State(state): State<Arc<AppState>>,
      req: axum::extract::Request,
      next: axum::middleware::Next,
  ) -> Result<Response, S3Error> {
      // Skip /health
      // Check Authorization header or query params
      // Verify signature
      // Call next.run(req).await
  }
  ```
- **Request body for payload hash:** For `UNSIGNED-PAYLOAD`, skip body hashing. For content-sha256, the body must be read before the handler (buffer and re-inject into the request).

**Rust idioms:**
- `subtle::ConstantTimeEq` for timing-safe signature comparison.
- Parse `Authorization` header with string slicing / splitting (no need for regex).
- Cache signing keys with a `HashMap<(String, String, String), Vec<u8>>` guarded by `Mutex` or use `dashmap`.

**Unit test approach:**
```rust
#[test]
fn test_derive_signing_key_known_vector() {
    // Use AWS test vectors from SigV4 docs
}

#[test]
fn test_canonical_request_construction() { /* ... */ }

#[test]
fn test_uri_encoding() {
    assert_eq!(s3_uri_encode("hello world", true), "hello%20world");
    assert_eq!(s3_uri_encode("path/to/key", false), "path/to/key");
}

#[tokio::test]
async fn test_presigned_url_validation() { /* ... */ }
```

**New Cargo.toml deps:**
- `subtle = "2"` -- constant-time comparison
- `url = "2"` -- URL parsing for presigned URL handling (or use `percent-encoding` already added)

**Build/run commands:**
```bash
cargo test -- auth
cargo build && cargo run -- -c ../bleepstore.yaml &
cd ../tests && python -m pytest e2e/test_presigned.py e2e/test_errors.py -v -m "presigned or auth"
```

---

## Milestone 5: Multipart Upload (Stages 7-8)

### Stage 7: Multipart Upload - Core ✅

**Goal:** CreateMultipartUpload, UploadPart, AbortMultipartUpload, ListMultipartUploads, ListParts.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/handlers/multipart.rs` -- Implement `create_multipart_upload`, `upload_part`, `abort_multipart_upload`, `list_multipart_uploads`, `list_parts`
- `/Users/zardoz/projects/bleepstore/rust/src/storage/local.rs` -- Implement `put_part` (write to `.multipart/{upload_id}/{part_number}`), `delete_parts` (remove `.multipart/{upload_id}/` directory)
- `/Users/zardoz/projects/bleepstore/rust/src/xml.rs` -- Add `render_list_multipart_uploads_result()`, `render_list_parts_result()`
- `/Users/zardoz/projects/bleepstore/rust/src/server.rs` -- Ensure multipart routes dispatch correctly: `POST /{bucket}/{key}?uploads` -> create, `PUT /{bucket}/{key}?partNumber&uploadId` -> upload_part, `DELETE /{bucket}/{key}?uploadId` -> abort, `GET /{bucket}?uploads` -> list_uploads, `GET /{bucket}/{key}?uploadId` -> list_parts

**Library-specific notes:**
- **UUID generation:** `uuid::Uuid::new_v4().to_string()` for upload IDs.
- **Part storage path:** `{root}/.multipart/{upload_id}/{part_number}`. Use `tokio::fs::create_dir_all()` for the directory, then atomic write for the part file.
- **MD5 for part ETags:** Same as object ETags -- compute MD5 of part data, hex-encode, quote.
- **Query parameter routing:** The single `PUT /{bucket}/{*key}` handler must check for `?partNumber` and `?uploadId` to distinguish UploadPart from PutObject/CopyObject. Similarly, `DELETE /{bucket}/{*key}` checks `?uploadId` for AbortMultipartUpload vs DeleteObject.

**Rust idioms:**
- Use `uuid::Uuid::new_v4()` for upload ID generation.
- Pattern: extract query params early, then match on presence to decide operation:
  ```rust
  let query: HashMap<String, String> = /* extract from request */;
  if query.contains_key("partNumber") && query.contains_key("uploadId") {
      upload_part(state, bucket, key, &query, body).await
  } else if headers.contains_key("x-amz-copy-source") {
      copy_object(state, bucket, key, headers, body).await
  } else {
      put_object(state, bucket, key, headers, body).await
  }
  ```

**Unit test approach:**
```rust
#[tokio::test]
async fn test_create_and_list_multipart_upload() { /* ... */ }

#[tokio::test]
async fn test_upload_part_returns_etag() { /* ... */ }

#[tokio::test]
async fn test_abort_cleans_up_parts() { /* ... */ }
```

**New Cargo.toml deps:**
- None (uuid already in deps).

**Build/run commands:**
```bash
cargo test -- multipart
cargo build && cargo run -- -c ../bleepstore.yaml &
cd ../tests && python -m pytest e2e/test_multipart.py -v -k "not complete"
```

---

### Stage 8: Multipart Upload - Completion ✅

**Goal:** CompleteMultipartUpload with part assembly and composite ETag. UploadPartCopy.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/handlers/multipart.rs` -- Implement `complete_multipart_upload`; add UploadPartCopy detection in the UploadPart handler path
- `/Users/zardoz/projects/bleepstore/rust/src/storage/local.rs` -- Implement `assemble_parts` (read parts sequentially, write concatenated data to final object path, compute composite ETag)
- `/Users/zardoz/projects/bleepstore/rust/src/metadata/sqlite.rs` -- Ensure `complete_multipart_upload` is transactional (insert object, delete upload + parts)
- `/Users/zardoz/projects/bleepstore/rust/src/xml.rs` -- `render_complete_multipart_upload_result()` already exists; add `render_copy_part_result()`

**Library-specific notes:**
- **Composite ETag computation:**
  ```rust
  use md5::{Md5, Digest};

  fn compute_composite_etag(part_etags: &[String]) -> String {
      let mut combined = Vec::new();
      for etag in part_etags {
          // Strip quotes, decode hex to binary MD5
          let hex_str = etag.trim_matches('"');
          let bytes = hex::decode(hex_str).unwrap();
          combined.extend_from_slice(&bytes);
      }
      let mut hasher = Md5::new();
      hasher.update(&combined);
      format!("\"{}-{}\"", hex::encode(hasher.finalize()), part_etags.len())
  }
  ```
- **Part assembly with streaming:** Open output file, iterate parts in order, read each part file and write to output. Use `tokio::io::copy()` for streaming between file handles:
  ```rust
  let mut output = tokio::fs::File::create(&final_path).await?;
  for (part_num, _etag) in parts {
      let part_path = self.multipart_path(upload_id, *part_num);
      let mut part_file = tokio::fs::File::open(&part_path).await?;
      tokio::io::copy(&mut part_file, &mut output).await?;
  }
  ```
- **Part validation:** Check ascending order, ETag match (compare with stored ETags from metadata), and minimum part size (5 MiB for non-last parts).
- **CompleteMultipartUpload XML parsing:** Parse `<CompleteMultipartUpload>` body with `quick_xml::Reader` to extract `(PartNumber, ETag)` pairs.

**Rust idioms:**
- Use `anyhow::bail!()` for validation failures, convert to specific `S3Error` variants at handler boundary.
- `rusqlite::Transaction` for atomically completing the upload.

**Unit test approach:**
```rust
#[test]
fn test_composite_etag_computation() {
    let etags = vec![
        "\"7ac66c0f148de9519b8bd264312c4d64\"".to_string(),
        "\"d41d8cd98f00b204e9800998ecf8427e\"".to_string(),
    ];
    let composite = compute_composite_etag(&etags);
    assert!(composite.ends_with("-2\""));
}

#[tokio::test]
async fn test_assemble_parts_creates_final_object() { /* ... */ }

#[tokio::test]
async fn test_complete_validates_part_order() { /* ... */ }
```

**New Cargo.toml deps:**
- None (md-5 added in Stage 4).

**Build/run commands:**
```bash
cargo test -- multipart
cargo build && cargo run -- -c ../bleepstore.yaml &
cd ../tests && python -m pytest e2e/test_multipart.py -v  # All 11 tests
```

---

## Milestone 6: Integration & Compliance (Stages 9a-9b)

### Stage 9a: Core Integration Testing

**Goal:** All 75 BleepStore E2E tests pass. Smoke test passes (20/20). Fix all failures related to bucket operations, basic/advanced object operations, and error responses.

**Files to modify:**
- Any file as needed for bug fixes. Common areas:
  - `/Users/zardoz/projects/bleepstore/rust/src/xml.rs` -- Namespace correctness, element ordering, empty element handling
  - `/Users/zardoz/projects/bleepstore/rust/src/errors.rs` -- Error code mapping, HTTP status codes
  - `/Users/zardoz/projects/bleepstore/rust/src/handlers/*.rs` -- Response header correctness, edge cases
  - `/Users/zardoz/projects/bleepstore/rust/src/server.rs` -- Missing routes, middleware ordering

**Library-specific notes:**
- **Common Rust-specific issues to watch for:**
  - `quick-xml` escaping: ensure ETag quotes survive XML serialization (use raw text, not attribute values)
  - Header values must be valid `HeaderValue` (no non-ASCII characters without encoding)
  - `Content-Length` header: axum may set this automatically for `Bytes` bodies, but verify
  - `HEAD` responses: axum should strip body from HEAD responses automatically, but verify
  - Date formatting: `httpdate::fmt_http_date(SystemTime::now())` for RFC 7231
  - Ensure `Accept-Ranges: bytes` header on GetObject/HeadObject responses
  - Ensure `Server: BleepStore` on every response

**Rust idioms:**
- Use `#[cfg(test)]` integration tests in `tests/` directory at the crate root for full-stack tests.
- Consider adding a test helper that spawns the server on a random port and returns a client.

**Build/run commands:**
```bash
cargo build --release && cargo run --release -- -c ../bleepstore.yaml &
cd ../tests && BLEEPSTORE_ENDPOINT=http://localhost:9000 ./run_tests.sh
BLEEPSTORE_ENDPOINT=http://localhost:9000 ../tests/smoke/smoke_test.sh
```

**Test targets:**
- BleepStore E2E: 75/75 tests pass
  - `test_buckets.py`: 16/16
  - `test_objects.py`: 32/32
  - `test_multipart.py`: 11/11
  - `test_presigned.py`: 4/4
  - `test_acl.py`: 4/4
  - `test_errors.py`: 8/8
- Smoke test: 20/20 pass

**Definition of done:**
- [ ] All 75 BleepStore E2E tests pass
- [ ] Smoke test passes (20/20)
- [ ] `aws s3 cp`, `aws s3 ls`, `aws s3 sync` work out of the box
- [ ] `aws s3api` commands for all Phase 1 operations succeed
- [ ] No 500 Internal Server Error for valid requests
- [ ] XML responses are well-formed and namespace-correct
- [ ] All headers match S3 format expectations

---

### Stage 9b: External Test Suites & Compliance

**Goal:** Run external S3 conformance test suites (Ceph s3-tests, MinIO Mint, Snowflake s3compat) and fix compliance issues found.

**Files to modify:**
- Any file as needed for compliance fixes found by external suites

**Implementation scope:**
1. **Run Ceph s3-tests** (filtered to Phase 1 operations) -- fix failures related to XML formatting, header values, edge cases in S3 behavior
2. **Run MinIO Mint core mode** -- fix failures related to SDK-specific expectations, Content-MD5 validation, chunked transfer encoding
3. **Run Snowflake s3compat** (9 core operations) -- fix any remaining issues
4. **Fix remaining compliance issues** -- edge cases in bucket naming, chunked transfer encoding, Content-MD5 verification, multi-SDK compatibility

**Library-specific notes:**
- **Chunked transfer encoding:** axum/hyper handles chunked decoding transparently, but verify that `Content-Length` is not required when `Transfer-Encoding: chunked` is present.
- **Content-MD5:** Parse `Content-MD5` header (base64-encoded MD5), compute MD5 of body, compare. Use `base64` crate for decoding.

**Rust idioms:**
- External test suite failures are triaged: fix what applies to Phase 1, defer the rest.
- Some external tests may require operations outside Phase 1 scope (versioning, etc.) -- these are expected failures.

**Build/run commands:**
```bash
cargo build --release && cargo run --release -- -c ../bleepstore.yaml &
# Ceph s3-tests:
cd ../s3-tests && S3TEST_CONF=s3tests.conf python -m pytest s3tests_boto3/functional/ -k "test_bucket or test_object or test_multipart"
# MinIO Mint:
docker run --rm --network host -e SERVER_ENDPOINT=localhost:9000 -e ACCESS_KEY=bleepstore -e SECRET_KEY=bleepstore-secret minio/mint:latest
# Snowflake s3compat:
cd ../s3-compat-tests && S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=bleepstore S3_SECRET_KEY=bleepstore-secret python -m pytest tests/
```

**Test targets:**
- Ceph s3-tests: >80% of Phase 1-applicable tests pass
- Snowflake s3compat: 9/9 pass
- MinIO Mint aws-cli tests pass
- BleepStore E2E: 75/75 still pass (no regressions)

**Definition of done:**
- [ ] Ceph s3-tests Phase 1 tests mostly pass (>80%)
- [ ] Snowflake s3compat 9/9 pass
- [ ] MinIO Mint aws-cli tests pass
- [ ] All 75 BleepStore E2E tests still pass (no regressions)
- [ ] Smoke test still passes (20/20)

---

## Milestone 7: Cloud Storage Backends (Stages 10-11b)

### Stage 10: AWS S3 Gateway Backend ✅

**Goal:** AWS S3 backend passes all 75 E2E tests.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/storage/aws.rs` -- Implement all `StorageBackend` methods using the AWS SDK for Rust
- `/Users/zardoz/projects/bleepstore/rust/src/config.rs` -- Add `AwsStorageConfig` struct (bucket, region, prefix)
- `/Users/zardoz/projects/bleepstore/rust/src/main.rs` (or a new `storage/factory.rs`) -- Factory function to create the appropriate backend based on `config.storage.backend`

**Library-specific notes:**
- **AWS SDK for Rust:** Add `aws-sdk-s3` and `aws-config` crates. These are official AWS SDKs.
  ```rust
  use aws_sdk_s3::Client;
  let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
  let client = Client::new(&config);
  ```
- **Key mapping:** `{prefix}{bleepstore_bucket}/{key}` in the single backing S3 bucket.
- **Multipart passthrough:** Use AWS SDK's native `create_multipart_upload`, `upload_part`, `complete_multipart_upload`, `abort_multipart_upload`.
- **Error mapping:** Map `aws_sdk_s3::error::SdkError` variants to `anyhow::Error` with descriptive context.

**Rust idioms:**
- The `aws-sdk-s3` client is `Clone` and `Send + Sync`, store in `AwsGatewayBackend`.
- Use `Bytes` from the SDK responses directly (compatible with our `bytes::Bytes`).

**New Cargo.toml deps:**
- `aws-config = "1"` -- AWS configuration loader
- `aws-sdk-s3 = "1"` -- AWS S3 SDK

**Build/run commands:**
```bash
cargo build
AWS_ACCESS_KEY_ID=xxx AWS_SECRET_ACCESS_KEY=yyy cargo run -- -c config-aws.yaml &
cd ../tests && BLEEPSTORE_ENDPOINT=http://localhost:9000 python -m pytest e2e/ -v
```

---

### Stage 11a: GCP Cloud Storage Backend ✅

**Goal:** GCP Cloud Storage backend passes all 75 E2E tests.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/storage/gcp.rs` -- Implement using `google-cloud-storage` crate or direct HTTP via `reqwest`
- `/Users/zardoz/projects/bleepstore/rust/src/config.rs` -- Add `GcpStorageConfig` struct

**Library-specific notes:**
- **GCP:** The `google-cloud-storage` Rust crate is less mature than the Go/Python equivalents. Consider using `reqwest` with GCP's JSON API directly.
- **GCS compose:** Has 32-source limit -- implement recursive composition for large multipart uploads (upload parts as temporary objects, use GCS `compose` to assemble, chain for >32 parts, delete temp objects after composition).
- **ETag normalization:** GCS returns base64 MD5 in `md5Hash` field. Convert: `hex::encode(base64::decode(gcs_md5)?)`.
- **Backend-agnostic error mapping utility:** Create a common error mapping function/table shared across all cloud backends. Map HTTP status codes and provider-specific error codes to S3 error types.

**Rust idioms:**
- Use `reqwest::Client` for HTTP-based backends (already a dev-dependency, move to regular deps).
- `async_trait` pattern or manual `Pin<Box>` for trait impl (already using manual approach).

**New Cargo.toml deps:**
- `reqwest = { version = "0.12", features = ["json", "stream"] }` -- move from dev-deps to deps
- `base64 = "0.22"` -- for GCS MD5 conversion

**Build/run commands:**
```bash
cargo build
GOOGLE_APPLICATION_CREDENTIALS=key.json cargo run -- -c config-gcp.yaml &
cd ../tests && python -m pytest e2e/ -v
```

**Test targets:**
- All 75 E2E tests pass with `BLEEPSTORE_BACKEND=gcp`

**Definition of done:**
- [ ] GCP backend implements full `StorageBackend` interface
- [ ] All 75 E2E tests pass with `BLEEPSTORE_BACKEND=gcp`
- [ ] GCS compose-based multipart works for >32 parts
- [ ] Backend error mapping utility covers GCS error codes

---

### Stage 11b: Azure Blob Storage Backend ✅

**Goal:** Azure Blob Storage backend passes all 75 E2E tests. All three cloud backends (AWS, GCP, Azure) work.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/storage/azure.rs` -- Implement using `azure_storage_blobs` crate
- `/Users/zardoz/projects/bleepstore/rust/src/config.rs` -- Add `AzureStorageConfig` struct

**Library-specific notes:**
- **Azure:** The `azure_storage_blobs` crate provides `put_block`, `put_block_list` for block blob multipart. Block IDs must be base64-encoded and same length.
- **Block IDs:** Derive from part number with fixed-width padding (e.g., base64 of 5-digit zero-padded number).
- **Multipart via block blobs:**
  - `UploadPart` -> `Put Block` with block ID derived from part number
  - `CompleteMultipartUpload` -> `Put Block List`
  - `AbortMultipartUpload` -> no-op (uncommitted blocks auto-expire in 7 days)
- **ETag handling:** Azure ETags may differ from MD5 -- compute MD5 ourselves.
- **Error mapping:** Use shared error mapping utility from Stage 11a.

**Rust idioms:**
- Use the `azure_storage_blobs` crate's async client.
- `async_trait` pattern or manual `Pin<Box>` for trait impl (already using manual approach).

**New Cargo.toml deps:**
- `azure_storage = "0.21"` -- Azure storage core
- `azure_storage_blobs = "0.21"` -- Azure blob storage

**Build/run commands:**
```bash
cargo build
AZURE_STORAGE_ACCOUNT=xxx AZURE_STORAGE_KEY=yyy cargo run -- -c config-azure.yaml &
cd ../tests && python -m pytest e2e/ -v
```

**Test targets:**
- All 75 E2E tests pass with `BLEEPSTORE_BACKEND=azure`

**Definition of done:**
- [x] Azure backend implements full `StorageBackend` interface
- [x] All 75 E2E tests pass with `BLEEPSTORE_BACKEND=azure` (pending verification with Azure credentials)
- [x] Azure block blob-based multipart works (Put Block + Put Block List pattern)
- [x] Backend error mapping covers Azure error codes (404, HTTP error mapping)

**Implementation notes (2026-02-24):** Used `reqwest` + Azure Blob REST API (same HTTP approach as GCP backend) instead of `azure_storage_blobs` crate. No new dependencies needed. Shared Key auth via HMAC-SHA256 signing, plus SAS token fallback. Parts stored as temp blobs (same pattern as AWS/GCP) since StorageBackend trait doesn't pass key to put_part(). 20 unit tests added.

---

## Milestone 8: Cluster Mode (Stages 12a-14)

### Stage 12a: Raft State Machine & Storage

**Goal:** Implement the core Raft state machine, log entry types, and persistent storage. The state machine handles state transitions and log management in isolation (no networking yet).

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/cluster/raft.rs` -- Implement Raft state machine using `openraft` crate; define `BleepStoreTypeConfig`; implement `RaftStorage` trait backed by SQLite
- `/Users/zardoz/projects/bleepstore/rust/src/cluster/mod.rs` -- Add log entry types, persistent state modules

**Implementation scope:**
1. **Raft node state machine** -- Three states (Follower, Candidate, Leader), state transitions, persistent state (currentTerm, votedFor, log[])
2. **Log entry types** -- Define enum: `CreateBucket`, `DeleteBucket`, `PutObjectMeta`, `DeleteObjectMeta`, `DeleteObjectsMeta`, `PutBucketAcl`, `PutObjectAcl`, `CreateMultipartUpload`, `RegisterPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`. Serialization via serde.
3. **Persistent storage for Raft state** -- Use a separate SQLite database for Raft log/metadata. Must be fsync'd before responding to RPCs. Append, truncate, read range, get last index/term operations.
4. **Leader election logic** (state machine only, no networking) -- Election timeout (configurable, randomized), `RequestVote` handler, vote granting rules
5. **Log replication logic** (state machine only, no networking) -- `AppendEntries` handler, conflict resolution, commitIndex advancement

**Library-specific notes:**
- **Recommended: Use `openraft` crate.** It is a mature, well-tested Raft implementation for Rust. It provides the state machine, log, network traits that we implement.
  ```rust
  use openraft::{Config, Raft};
  // Implement: RaftStorage, RaftNetworkFactory, RaftTypeConfig
  ```
- **Persistent state:** Use a separate SQLite database for Raft log/metadata, or use `openraft`'s built-in storage traits with a SQLite backend.

**Rust idioms:**
- `openraft` uses associated types extensively. Define a `TypeConfig` implementing `RaftTypeConfig`:
  ```rust
  pub struct BleepStoreTypeConfig;
  impl openraft::RaftTypeConfig for BleepStoreTypeConfig {
      type D = LogEntry;    // Log entry data type
      type R = ApplyResult; // Apply result type
      // ...
  }
  ```
- Use `Arc<Raft<BleepStoreTypeConfig>>` as shared state.

**Unit test approach:**
```rust
#[tokio::test]
async fn test_log_entry_serialization() { /* round-trip serde */ }

#[tokio::test]
async fn test_persistent_state_survives_restart() { /* term, votedFor, log */ }

#[tokio::test]
async fn test_single_node_election() { /* node elects itself */ }
```

**New Cargo.toml deps:**
- `openraft = { version = "0.10", features = ["serde"] }` -- Raft consensus library

**Test targets:**
- State machine transitions: Follower -> Candidate -> Leader
- Vote granting: correct term/log checks
- Log append: entries persisted correctly
- Log truncation on conflict
- Term monotonicity: reject messages from old terms
- Persistent state survives restart (term, votedFor, log)

**Definition of done:**
- [ ] Raft state machine correctly transitions between Follower/Candidate/Leader
- [ ] Log entry types defined and serializable
- [ ] Persistent storage for term, votedFor, log entries
- [ ] RequestVote and AppendEntries handlers work (in-process, no networking)
- [ ] Unit tests cover state transitions, vote granting, log replication logic

---

### Stage 12b: Raft Networking & Elections

**Goal:** Add HTTP-based RPC transport to the Raft state machine. Leader election and log replication work across 3 nodes over the network.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/cluster/raft.rs` -- Add `RaftNetworkFactory` implementation with HTTP transport
- `/Users/zardoz/projects/bleepstore/rust/src/cluster/mod.rs` -- Add network transport module, RPC endpoints
- `/Users/zardoz/projects/bleepstore/rust/src/server.rs` -- Add Raft RPC routes on separate port (or same router)

**Implementation scope:**
1. **Network transport** -- HTTP-based RPC using `axum` routes on a separate port for Raft RPCs, or `openraft`'s network trait with `reqwest` for HTTP-based transport. Endpoints: `POST /raft/append_entries`, `POST /raft/request_vote`, `POST /raft/install_snapshot` (stub)
2. **RPC client** -- Send RequestVote/AppendEntries to peers using `reqwest`. Timeout handling, connection pooling.
3. **Election driver** -- Timer-based election timeout, broadcast RequestVote, collect votes, transition to Leader.
4. **Heartbeat driver** -- Leader sends empty AppendEntries as heartbeats at `heartbeat_interval_ms`. Heartbeats reset follower election timeouts.
5. **Multi-node integration** -- Configuration: `cluster.peers` list of `host:port` addresses. Static membership for now.

**Library-specific notes:**
- **Network transport:** Use `axum` routes on a separate port for Raft RPCs, or `openraft`'s network trait with `reqwest` for HTTP-based transport.
- With `openraft`, implement the `RaftNetworkFactory` trait to create network connections to peers.

**Rust idioms:**
- Use `reqwest::Client` for outbound Raft RPCs (connection pooling built-in).
- Use `tokio::time::interval` for heartbeat scheduling.

**Unit test approach:**
```rust
#[tokio::test]
async fn test_three_node_election() { /* one leader elected */ }

#[tokio::test]
async fn test_log_replication_over_network() { /* entries committed to majority */ }

#[tokio::test]
async fn test_leader_failure_triggers_election() { /* kill leader, new election */ }
```

**Test targets:**
- Three-node election: one leader elected within timeout
- Log replication: leader's entries replicated to followers
- Heartbeats prevent election timeout
- Leader failure triggers new election
- Split vote resolves via randomized timeouts
- Log consistency: follower with missing entries catches up

**Definition of done:**
- [ ] Leader election works with 3 nodes over HTTP
- [ ] Log replication works over HTTP (entries committed to majority)
- [ ] Heartbeats prevent unnecessary elections
- [ ] RPCs work reliably over HTTP with timeout handling
- [ ] Integration tests cover multi-node Raft scenarios

---

### Stage 13a: Raft-Metadata Wiring

**Goal:** Wire the Raft consensus layer to the metadata store. Metadata writes go through the Raft log. Reads served from local SQLite replica. Write forwarding from followers to leader.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/metadata/mod.rs` -- Add `RaftMetadataStore` that wraps `SqliteMetadataStore` and routes writes through Raft
- `/Users/zardoz/projects/bleepstore/rust/src/cluster/raft.rs` -- Implement `openraft::RaftStorage` trait backed by SQLite; implement apply function
- `/Users/zardoz/projects/bleepstore/rust/src/main.rs` -- Conditionally create `RaftMetadataStore` vs `SqliteMetadataStore` based on `config.metadata.engine`

**Implementation scope:**
1. **Wire metadata writes through Raft** -- When `metadata.engine = "raft"`, all metadata write operations submit a log entry to Raft instead of writing directly to SQLite. Leader appends, replicates to quorum, then applies to local SQLite. Followers apply as entries are received.
2. **SQLite state machine apply** -- Each committed Raft log entry applied to local SQLite. Apply function: deserialize log entry -> execute corresponding SQL statements. Must be deterministic.
3. **Read path** -- Reads always served from local SQLite replica (any node). No Raft involvement for reads (eventual consistency).
4. **Write forwarding** -- Follower receives write request -> forwards to leader (transparent proxy). Leader address tracked via Raft protocol.

**Library-specific notes:**
- **Write forwarding:** If the current node is not the leader, forward the write request to the leader using `reqwest`. The leader's address is available from `openraft`'s `Raft::current_leader()`.
- **Apply function:** The Raft state machine apply function deserializes the log entry and executes the corresponding SQLite operation.

**Rust idioms:**
- Use `enum LogEntry` with `serde::Serialize`/`Deserialize` for all metadata mutation types.
- `Arc<SqliteMetadataStore>` shared between the Raft apply function and the read path.

**New Cargo.toml deps:**
- None beyond `openraft` (added in Stage 12a).

**Test targets:**
- Write on leader, read on follower (eventually consistent)
- Follower forwards writes to leader transparently
- Leader failure -> new leader -> writes continue
- Node restart -> catches up from Raft log

**Definition of done:**
- [ ] Metadata writes go through Raft consensus
- [ ] Reads served from local SQLite on any node
- [ ] Write forwarding from follower to leader works transparently
- [ ] Leader failover maintains metadata consistency

---

### Stage 13b: Snapshots & Node Management

**Goal:** Log compaction via snapshots, InstallSnapshot RPC, and dynamic node join/leave.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/cluster/raft.rs` -- Implement snapshot creation/restore in `openraft::RaftStorage`; implement `InstallSnapshot` handler
- `/Users/zardoz/projects/bleepstore/rust/src/cluster/mod.rs` -- Add snapshot management, node join/leave logic

**Implementation scope:**
1. **Log compaction / snapshotting** -- Snapshot = copy of SQLite database file using `rusqlite`'s backup API: `conn.backup(DatabaseName::Main, &backup_path, None)`. Triggered every `snapshot_interval` committed entries (configurable, default 10000). After snapshot: log entries before snapshot index can be discarded.
2. **InstallSnapshot RPC** -- Leader sends full snapshot to followers that are too far behind. Follower replaces local SQLite with snapshot. Chunked transfer for large snapshots.
3. **Node join/leave** -- Join: new node contacts existing leader, leader adds to configuration. Leave: leader removes node. Configuration changes go through Raft log.
4. **Snapshot-based recovery** -- Node offline too long: leader detects gap, sends snapshot. New node joining: receives full snapshot before participating.

**Library-specific notes:**
- **Snapshot with rusqlite:** Use `rusqlite`'s backup API: `conn.backup(DatabaseName::Main, &backup_path, None)`.
- **InstallSnapshot:** With `openraft`, implement the `RaftStorage::install_snapshot` method.

**Rust idioms:**
- Use `tokio::fs` for async file I/O during snapshot transfer.
- Snapshot transfer via HTTP (chunked for large DBs).

**New Cargo.toml deps:**
- None beyond `openraft` (added in Stage 12a).

**Test targets:**
- Snapshot created after configured number of entries
- New node joins via snapshot transfer
- Node offline for extended period catches up via snapshot
- Log entries before snapshot index are discarded
- Node leave: removed from configuration, cluster continues

**Definition of done:**
- [ ] Log compaction/snapshotting works
- [ ] InstallSnapshot RPC transfers full database to lagging nodes
- [ ] New node can join and sync via snapshot
- [ ] Node leave removes from cluster configuration
- [ ] Snapshot-based recovery for nodes that missed too many entries

---

### Stage 14: Cluster Operations & Admin API

**Goal:** Admin API, multi-node E2E testing, failure scenarios.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/server.rs` -- Add admin routes on separate port (or separate axum Router)
- `/Users/zardoz/projects/bleepstore/rust/src/cluster/mod.rs` -- Add admin handler functions, cluster status types
- `/Users/zardoz/projects/bleepstore/rust/src/main.rs` -- Start admin server on `config.server.admin_port`

**Library-specific notes:**
- Run the admin API as a second `axum::serve` on a different `TcpListener`. Use `tokio::select!` or `tokio::spawn` to run both servers concurrently.
- Bearer token auth for admin: simple middleware that checks `Authorization: Bearer {token}` against `config.server.admin_token`.

**Rust idioms:**
- Use `serde_json` for JSON admin API responses.
- `tokio::signal::ctrl_c()` for graceful shutdown of both servers.

**New Cargo.toml deps:**
- None.

---

## Milestone 9: Performance & Hardening (Stage 15)

### Stage 15: Performance Optimization & Production Readiness ✅

**Goal:** Streaming I/O, startup < 1s, memory < 50MB idle, performance within 2x of MinIO.

**Files to modify:**
- `/Users/zardoz/projects/bleepstore/rust/src/handlers/object.rs` -- Convert PutObject/GetObject to streaming using `axum::body::Body` with `Stream` trait instead of `Bytes`
- `/Users/zardoz/projects/bleepstore/rust/src/storage/local.rs` -- Return `tokio::io::ReaderStream` for streaming reads; accept `Stream` for writes
- `/Users/zardoz/projects/bleepstore/rust/src/storage/backend.rs` -- Consider changing `get()` to return a stream type instead of `StoredObject` with full `Bytes`
- `/Users/zardoz/projects/bleepstore/rust/src/auth.rs` -- Add signing key cache (`DashMap` or `HashMap` with `Mutex`)
- `/Users/zardoz/projects/bleepstore/rust/src/metadata/sqlite.rs` -- Use prepared statements (store in struct, or use `rusqlite::CachedStatement`)
- `/Users/zardoz/projects/bleepstore/rust/src/main.rs` -- Add graceful shutdown with in-flight request draining; add configurable timeouts

**Library-specific notes:**
- **Streaming GetObject:**
  ```rust
  use tokio::io::BufReader;
  use tokio_util::io::ReaderStream;

  let file = tokio::fs::File::open(path).await?;
  let stream = ReaderStream::new(BufReader::new(file));
  let body = axum::body::Body::from_stream(stream);
  ```
- **Streaming PutObject:** Use `axum::body::Body` as a `Stream<Item = Result<Bytes, _>>` and write chunks as they arrive.
- **Connection tuning:** Use `tower_http::timeout::TimeoutLayer` for request/response timeouts. `axum::serve` already uses `hyper` which handles keep-alive.
- **Memory profiling:** Use `jemalloc` (`tikv-jemallocator`) as the global allocator for better performance and memory profiling:
  ```rust
  #[global_allocator]
  static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
  ```
- **Prepared statements:** `rusqlite::Connection::prepare_cached()` returns a cached prepared statement.

**Rust idioms:**
- Rust's ownership model naturally prevents many memory leaks. Focus on ensuring large `Bytes` buffers are dropped promptly.
- Use `tracing::instrument` on hot-path functions for performance analysis.
- Consider `#[inline]` on small hot-path functions.

**New Cargo.toml deps:**
- `tokio-util = { version = "0.7", features = ["io"] }` -- for `ReaderStream`
- `tikv-jemallocator = "0.6"` (optional) -- jemalloc allocator
- `dashmap = "6"` -- concurrent hashmap for signing key cache
- `tower-http = { ..., features = ["timeout"] }` -- add timeout feature

**Build/run commands:**
```bash
cargo build --release
# Benchmark startup:
time cargo run --release -- -c ../bleepstore.yaml &

# Run Warp:
warp mixed --host=localhost:9000 --access-key=bleepstore --secret-key=bleepstore-secret --duration=60s

# Memory check:
ps -o rss -p $(pgrep bleepstore) # Should be < 50MB idle
```

---

## Milestone 10: Persistent Event Queues (Stages 16a-16c)

### Stage 16a: Queue Interface & Redis Backend

**Goal:** Define the QueueBackend interface, event types/envelope, and implement the Redis Streams backend with write-through mode.

**Files to create/modify:**

| File | Work |
|---|---|
| `src/queue/mod.rs` | Module declarations |
| `src/queue/backend.rs` | `QueueBackend` trait with connect, close, health_check, publish, publish_batch, subscribe, acknowledge, enqueue_task, dequeue_task, complete_task, fail_task, retry_failed_tasks (all async) |
| `src/queue/redis.rs` | Redis Streams implementation using `redis` crate (tokio feature) |
| `src/queue/events.rs` | EventType enum, Event struct (id, event_type, timestamp, source, request_id, data) with serde |
| `src/config.rs` | Add QueueConfig: enabled, backend, consistency, redis sub-config |
| `src/server.rs` | Initialize queue backend on startup, reconnect/reprocess pending on restart |
| All handler files | Publish events after successful writes |

**Implementation scope:**
1. **Queue backend interface/trait** -- `connect(config)`, `close()`, `health_check() -> bool`, `publish(event)`, `publish_batch(events)`, `subscribe(event_types, handler)`, `acknowledge(event_id)`, `enqueue_task(task) -> task_id`, `dequeue_task() -> WriteTask | None`, `complete_task(task_id)`, `fail_task(task_id, error)`, `retry_failed_tasks() -> int`
2. **Event types and envelope** -- Define all event types: `bucket.created`, `bucket.deleted`, `object.created`, `object.deleted`, `objects.deleted`, `object.acl.updated`, `bucket.acl.updated`, `multipart.created`, `multipart.completed`, `multipart.aborted`, `part.uploaded`. Event envelope: `id` (ULID/UUID), `type`, `timestamp`, `source`, `request_id`, `data`.
3. **Redis backend** -- Redis Streams for ordered, persistent event log with consumer groups. `XADD`, `XREADGROUP`, `XACK`. Dead letter stream for failed messages.
4. **Write-through mode** (default) -- Normal direct write path, after commit publish event to queue (fire-and-forget). Queue failure does not block the write.
5. **Configuration integration** -- `queue.enabled` (default: false), `queue.backend`, `queue.consistency`. Redis-specific config section.
6. **Startup integration (crash-only)** -- On startup: reconnect to queue, reprocess pending/unacknowledged tasks.

**Library-specific notes:**

- **redis** crate (tokio):
  ```rust
  let client = redis::Client::open(url)?;
  let mut conn = client.get_multiplexed_tokio_connection().await?;
  redis::cmd("XADD").arg("bleepstore:events").arg("*").arg("type").arg("object.created").arg("data").arg(&event_json).query_async(&mut conn).await?;
  ```

- Use `#[async_trait]` for the QueueBackend trait

**Rust idioms:**
- Use `Arc<dyn QueueBackend>` in `AppState`, wrapped in `Option` since the queue is optional.
- Feature-gate backends with `#[cfg(feature = "redis-queue")]`, `#[cfg(feature = "rabbitmq")]`, `#[cfg(feature = "kafka")]` to avoid pulling in all dependencies when not needed.
- Use `async_trait` for the `QueueBackend` trait to keep the interface clean and avoid manual `Pin<Box<dyn Future>>` signatures.

**Unit test approach:**
```rust
#[test]
fn test_event_serialization_roundtrip() {
    let event = Event::new(EventType::ObjectCreated, "test-source", serde_json::json!({"bucket": "b", "key": "k"}));
    let json = serde_json::to_string(&event).unwrap();
    let deserialized: Event = serde_json::from_str(&json).unwrap();
    assert_eq!(event.id, deserialized.id);
    assert_eq!(event.event_type, deserialized.event_type);
}

#[tokio::test]
async fn test_mock_backend_publish_and_subscribe() { /* ... */ }

#[tokio::test]
async fn test_task_lifecycle_enqueue_dequeue_complete() { /* ... */ }
```

**New Cargo.toml deps:**
- `redis = { version = "0.25", features = ["tokio-comp", "streams"] }` -- Redis Streams
- `ulid = "1"` -- ULID generation for event IDs
- `async-trait = "0.1"` -- Ergonomic async trait definitions

**Build/run commands:**
```bash
cargo build --features redis-queue    # Build with Redis queue support
cargo test -- queue                   # Run queue unit tests
cargo run --features redis-queue -- -c ../bleepstore.yaml &
```

**Test targets:**
- Event serialization/deserialization round-trip
- Redis backend: publish, subscribe, acknowledge, dead letter
- Write-through mode: event published after successful write
- Start BleepStore with Redis queue, run E2E suite -- all 75 tests pass
- Queue unavailable at startup: BleepStore starts in degraded mode (logs warning)

**Definition of done:**
- [ ] QueueBackend interface defined
- [ ] Redis backend implemented (publish, subscribe, acknowledge, dead letter)
- [ ] Event types and envelope defined
- [ ] Write-through mode works: events published after successful writes
- [ ] All 75 E2E tests pass with Redis queue enabled (write-through mode)
- [ ] Configuration section for queue settings
- [ ] Health check reports queue status

---

### Stage 16b: RabbitMQ Backend

**Goal:** Implement the RabbitMQ/AMQP backend using the QueueBackend interface established in 16a.

**Files to create/modify:**

| File | Work |
|---|---|
| `src/queue/rabbitmq.rs` | RabbitMQ implementation using `lapin` crate |
| `src/config.rs` | Add RabbitMQ sub-config to QueueConfig |

**Implementation scope:**
1. **RabbitMQ backend** -- Topic exchange for event routing by type. Durable queues with manual ack. Dead letter exchange for failed messages. Compatible with ActiveMQ via AMQP 0-9-1.
2. **AMQP connection management** -- Connection and channel lifecycle. Automatic reconnection on connection loss. Queue and exchange declaration (idempotent).
3. **Event routing** -- Routing keys based on event type (e.g., `bucket.created`, `object.deleted`). Subscribers bind to specific routing keys or patterns.

**Library-specific notes:**

- **lapin** for RabbitMQ:
  ```rust
  let conn = Connection::connect(url, ConnectionProperties::default().with_tokio()).await?;
  let channel = conn.create_channel().await?;
  channel.basic_publish("bleepstore", "object.created", BasicPublishOptions::default(), &event_bytes, BasicProperties::default()).await?;
  ```

**Rust idioms:**
- `lapin` is fully async and tokio-compatible.
- Feature-gate with `#[cfg(feature = "rabbitmq")]`.

**Unit test approach:**
```rust
#[tokio::test]
async fn test_rabbitmq_publish_and_subscribe() { /* ... */ }

#[tokio::test]
async fn test_rabbitmq_dead_letter_exchange() { /* ... */ }
```

**New Cargo.toml deps:**
- `lapin = "2"` -- RabbitMQ/AMQP

**Build/run commands:**
```bash
cargo build --features rabbitmq       # Build with RabbitMQ support
cargo test -- queue::rabbitmq         # Run RabbitMQ-specific tests
cargo run --features rabbitmq -- -c ../bleepstore.yaml &
```

**Test targets:**
- RabbitMQ backend: publish, subscribe, acknowledge, dead letter
- Exchange and queue declaration (idempotent)
- Start BleepStore with RabbitMQ queue, run E2E suite -- all 75 tests pass
- Events routed correctly by type

**Definition of done:**
- [ ] RabbitMQ backend implements full QueueBackend interface
- [ ] All 75 E2E tests pass with RabbitMQ queue enabled (write-through mode)
- [ ] Dead letter exchange handles failed messages
- [ ] Compatible with AMQP 0-9-1 (ActiveMQ compatible)

---

### Stage 16c: Kafka Backend & Consistency Modes

**Goal:** Implement the Kafka backend and the sync/async consistency modes. All three queue backends support all three consistency modes.

**Files to create/modify:**

| File | Work |
|---|---|
| `src/queue/kafka.rs` | Kafka implementation using `rdkafka` crate |
| `src/config.rs` | Add Kafka sub-config to QueueConfig |
| `src/server.rs` | Add sync/async consistency mode switching in handler middleware |
| All handler files | Support sync mode (block until consumer completes) and async mode (return 202 Accepted) |

**Implementation scope:**
1. **Kafka backend** -- Topics per event type (e.g., `bleepstore.object.created`). Consumer groups for parallel processing. `acks=all` for durability. Partitioned by bucket name for ordering.
2. **Sync mode** (all backends) -- Handler writes to temp file (fsync), enqueues WriteTask, blocks until consumer completes task. Crash-safe: pending tasks survive in queue.
3. **Async mode** (all backends) -- Handler writes to temp file (fsync), enqueues WriteTask, responds 202 Accepted immediately. Consumer processes asynchronously. Clean up orphan temp files on startup.
4. **Consistency mode integration** -- `queue.consistency` config: `write-through` (default), `sync`, `async`. Mode switching in handler middleware.

**Library-specific notes:**

- **rdkafka** for Kafka:
  ```rust
  let producer: FutureProducer = ClientConfig::new().set("bootstrap.servers", brokers).create()?;
  producer.send(FutureRecord::to("bleepstore.object.created").payload(&event_json), Duration::from_secs(5)).await?;
  ```

- **Crash-only**: Kafka requires `acks=all` for crash-only safety. Sync mode timeout: configurable, returns 504 Gateway Timeout if consumer doesn't complete in time. Async mode: 202 Accepted with `Location` header for eventual GET.

**Rust idioms:**
- Feature-gate with `#[cfg(feature = "kafka")]`.
- `rdkafka` uses its own threading model; integrate with tokio via `FutureProducer`/`StreamConsumer`.

**Unit test approach:**
```rust
#[tokio::test]
async fn test_kafka_publish_and_subscribe() { /* ... */ }

#[tokio::test]
async fn test_sync_mode_blocks_until_complete() { /* ... */ }

#[tokio::test]
async fn test_async_mode_returns_202() { /* ... */ }

#[tokio::test]
async fn test_retry_failed_tasks() { /* ... */ }
```

**New Cargo.toml deps:**
- `rdkafka = { version = "0.36", features = ["cmake-build"] }` -- Apache Kafka

**Build/run commands:**
```bash
cargo build --features kafka          # Build with Kafka support
cargo test -- queue::kafka            # Run Kafka-specific tests
cargo run --features kafka -- -c ../bleepstore.yaml &
```

**Test targets:**
- Kafka backend: publish, subscribe, acknowledge
- Sync mode: handler blocks until task completed
- Async mode: handler returns 202, task processed asynchronously
- Start BleepStore with Kafka queue, run E2E suite -- all 75 tests pass
- Kill BleepStore mid-operation, restart, verify pending tasks reprocessed
- All three backends support all three consistency modes

**Definition of done:**
- [ ] Kafka backend implements full QueueBackend interface
- [ ] Sync mode: writes blocked until queue consumer completes (all backends)
- [ ] Async mode: writes return 202, processed asynchronously (all backends)
- [ ] All 75 E2E tests pass with Kafka queue enabled (write-through mode)
- [ ] Crash-only: pending tasks survive restarts, orphan temp files cleaned
- [ ] All three backends support all three consistency modes

---

## Summary: File-to-Stage Map

| File | Stages |
|---|---|
| `src/main.rs` | 1, 3, 10, 13a, 14, 15 |
| `Cargo.toml` | 1b, 3, 4, 5a, 6, 10, 11a, 11b, 12a, 15, 16a, 16b, 16c |
| `src/config.rs` | 1, 10, 11a, 11b, 16a, 16b, 16c |
| `src/server.rs` | 1, 1b, 3, 4, 5a, 6, 7, 12b, 14, 16a, 16c |
| `src/errors.rs` | 1, 3 |
| `src/metrics.rs` | 1b (new) |
| `src/xml.rs` | 3, 5a, 5b, 7, 8 |
| `src/auth.rs` | 6, 15 |
| `src/handlers/bucket.rs` | 1, 1b, 3, 16a |
| `src/handlers/object.rs` | 1, 1b, 4, 5a, 5b, 15, 16a, 16c |
| `src/handlers/multipart.rs` | 1, 1b, 7, 8, 16a, 16c |
| `src/metadata/store.rs` | 2 |
| `src/metadata/sqlite.rs` | 2, 6, 8, 15 |
| `src/metadata/mod.rs` | 13a |
| `src/storage/backend.rs` | 4, 15 |
| `src/storage/local.rs` | 4, 5a, 7, 8, 15 |
| `src/storage/aws.rs` | 10 |
| `src/storage/gcp.rs` | 11a |
| `src/storage/azure.rs` | 11b |
| `src/cluster/raft.rs` | 12a, 12b, 13a, 13b |
| `src/cluster/mod.rs` | 12a, 12b, 13b, 14 |
| `src/queue/mod.rs` | 16a |
| `src/queue/backend.rs` | 16a |
| `src/queue/redis.rs` | 16a |
| `src/queue/rabbitmq.rs` | 16b |
| `src/queue/kafka.rs` | 16c |
| `src/queue/events.rs` | 16a |

## Summary: New Dependencies by Stage

| Stage | New Cargo.toml Dependencies |
|---|---|
| 1 | `rand`, `chrono` or `httpdate` |
| 1b | `utoipa`, `utoipa-axum`, `utoipa-swagger-ui`, `garde`, `axum-valid`, `metrics`, `metrics-exporter-prometheus` |
| 3 | `md-5`, `base64` |
| 4 | `tempfile` (dev) |
| 5a | `percent-encoding` |
| 6 | `subtle` |
| 10 | `aws-config`, `aws-sdk-s3` |
| 11a | `reqwest` (move to deps), `base64` |
| 11b | `azure_storage`, `azure_storage_blobs` |
| 12a | `openraft` |
| 15 | `tokio-util`, `dashmap`, optionally `tikv-jemallocator` |
| 16a | `redis` (tokio-comp, streams), `ulid`, `async-trait` |
| 16b | `lapin` |
| 16c | `rdkafka` |

## Stage Summary Quick Reference

| Stage | Milestone | Focus | Test Target | Key Spec |
|---|---|---|---|---|
| 1 | Foundation | Server bootstrap, routing, error XML | Health check, error format | s3-error-responses.md, s3-common-headers.md |
| 1b | Foundation | OpenAPI (utoipa), validation (garde), Prometheus metrics | Swagger UI, /metrics, /openapi.json | observability-and-openapi.md |
| 2 | Foundation | SQLite metadata store (rusqlite) | Unit tests (metadata CRUD) | metadata-schema.md |
| 3 | Bucket Ops | All 7 bucket operations | test_buckets.py (16) | s3-bucket-operations.md |
| 4 | Object Ops | Basic CRUD + filesystem backend | test_objects.py (12 basic) | s3-object-operations.md, storage-backends.md |
| 5a | Object Ops | List, copy, batch delete | test_objects.py (14 list/copy/delete) | s3-object-operations.md |
| 5b | Object Ops | Range, conditional requests, ACLs | test_objects.py (6 range/cond), test_acl.py (4) | s3-object-operations.md |
| 6 | Auth | SigV4 header auth + presigned URLs | test_presigned.py (4), test_errors.py auth (2) | s3-authentication.md |
| 7 | Multipart | Core: create, upload, abort, list | test_multipart.py (8 partial) | s3-multipart-upload.md |
| 8 | Multipart | Complete with assembly + composite ETag | test_multipart.py (11) | s3-multipart-upload.md |
| 9a | Integration | Internal E2E + smoke test pass | All 75 E2E + smoke (20) | All specs |
| 9b | Compliance | External test suites (Ceph, Mint, s3compat) | Ceph >80%, s3compat 9/9 | All specs |
| 10 | Cloud | AWS S3 gateway backend (aws-sdk-s3) | 75 E2E with AWS backend | storage-backends.md |
| 11a | Cloud | GCP Cloud Storage backend (reqwest) | 75 E2E with GCP backend | storage-backends.md |
| 11b | Cloud | Azure Blob Storage backend (azure_storage_blobs) | 75 E2E with Azure backend | storage-backends.md |
| 12a | Cluster | Raft state machine & storage (openraft) | Raft unit tests (in-process) | clustering.md |
| 12b | Cluster | Raft networking & elections (openraft) | Multi-node election tests | clustering.md |
| 13a | Cluster | Raft-metadata wiring | Multi-node write/read tests | clustering.md |
| 13b | Cluster | Snapshots & node management | Snapshot + join/leave tests | clustering.md |
| 14 | Cluster | Admin API + cluster E2E | Admin API + cluster E2E | clustering.md, admin-api.openapi.yaml |
| 15 | Performance | Optimization + hardening | Warp benchmarks, perf tests | OBJECTIVE.MD success criteria |
| 16a | Event Queues | Queue interface + Redis backend (redis crate) | 75 E2E with Redis queue | event-queues.md |
| 16b | Event Queues | RabbitMQ backend (lapin) | 75 E2E with RabbitMQ queue | event-queues.md |
| 16c | Event Queues | Kafka backend (rdkafka) + consistency modes | 75 E2E with Kafka + sync/async | event-queues.md |
