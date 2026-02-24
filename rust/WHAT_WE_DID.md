# BleepStore Rust — What We Did

## Session 16 -- Stage 11b: Azure Blob Storage Gateway Backend (2026-02-24)
- **src/storage/azure.rs**: Full implementation of `AzureGatewayBackend` using `reqwest` + Azure Blob REST API:
  - All 10 `StorageBackend` trait methods implemented (put, get, delete, exists, copy_object, put_part, assemble_parts, delete_parts, create_bucket, delete_bucket)
  - Key mapping: `{prefix}{bleepstore_bucket}/{key}` in single upstream Azure container
  - Parts stored as temporary blobs at `{prefix}.parts/{upload_id}/{part_number}` (same pattern as AWS/GCP backends)
  - Multipart assembly: downloads part blobs, stages as blocks (Put Block) on final blob, then commits via Put Block List
  - Block IDs: `base64("{upload_id}:{part_number:05}")` -- includes upload_id to avoid collisions
  - Server-side copy via Azure Copy Blob API (`x-ms-copy-source` header)
  - MD5 computed locally for consistent ETags (Azure ETags differ from S3)
  - SHA-256 content hash computed on download (same as LocalBackend, AWS, GCP)
  - Composite ETag computed from part MD5s for multipart completions
  - Credentials via environment variables:
    - `AZURE_STORAGE_KEY` (Shared Key auth via HMAC-SHA256 signing)
    - `AZURE_STORAGE_CONNECTION_STRING` (extracts AccountKey)
    - `AZURE_STORAGE_SAS_TOKEN` (SAS token appended as query param)
  - Full Azure Shared Key auth signing: HMAC-SHA256 string-to-sign with canonicalized headers and resource
  - Azure Blob REST API version 2023-11-03
  - URL-encoding for blob names preserving '/' separators
  - Idempotent delete (catches 404 silently, matching S3 semantics)
  - No-op create/delete bucket (BleepStore buckets are prefix-namespaced)
  - List Blobs API with pagination for delete_parts cleanup (XML response parsing)
  - Container-level signing for List Blobs operations
  - 20 unit tests for: blob name mapping, block ID generation/padding/uniqueness, MD5 computation, composite ETag, key mapping, URL encoding, not-found detection, API version, block list XML format, RFC 1123 date format, part blob name mapping, SAS token handling
- **src/main.rs**: Updated backend factory to dispatch on `config.storage.backend`:
  - Added `"azure"` case -> creates `AzureGatewayBackend` from `storage.azure` config section
  - Existing `"aws"`, `"gcp"`, and `"local"` cases unchanged
- **Cargo.toml**: No new dependencies needed -- uses existing `reqwest`, `base64`, `hmac`, `sha2`, `md-5`, `hex`, `percent-encoding`, `httpdate`
- **Config**: No changes needed -- `AzureStorageConfig` with `container`, `account`, `prefix` already existed from scaffold
- **PENDING**: Build and test verification (cargo build, cargo test, ./run_e2e.sh)
- Updated STATUS.md, DO_NEXT.md, WHAT_WE_DID.md, PLAN.md, tasks/done/stage-11b-azure-gateway.md

## Session 15 -- Stage 11a: GCP Cloud Storage Gateway Backend (2026-02-24)
- **src/storage/gcp.rs**: Full implementation of `GcpGatewayBackend` using `reqwest` + GCS JSON API:
  - All 10 `StorageBackend` trait methods implemented (put, get, delete, exists, copy_object, put_part, assemble_parts, delete_parts, create_bucket, delete_bucket)
  - Key mapping: `{prefix}{bleepstore_bucket}/{key}` in single upstream GCS bucket
  - Parts stored as temporary GCS objects at `{prefix}.parts/{upload_id}/{part_number}`
  - Multipart assembly using GCS compose API (max 32 sources per call)
  - Compose chaining for >32 parts: recursive batch composition into intermediate objects, then final compose, with cleanup
  - Server-side copy via GCS rewrite API (handles large objects with multi-call rewrite token loop)
  - MD5 computed locally for consistent ETags (GCS ETags differ from S3)
  - SHA-256 content hash computed on download (same as LocalBackend and AwsGatewayBackend)
  - Composite ETag computed from part MD5s for multipart completions
  - OAuth2 credentials via Application Default Credentials (ADC):
    - GOOGLE_APPLICATION_CREDENTIALS service account JSON
    - gcloud application-default credentials (refresh token flow)
    - GCE metadata server
    - GOOGLE_OAUTH_ACCESS_TOKEN env var fallback
  - Token caching with 60s safety margin before expiry
  - URL-encoding for GCS object names in API paths
  - Idempotent delete (catches 404 silently, matching S3 semantics)
  - No-op create/delete bucket (BleepStore buckets are prefix-namespaced)
  - GCS error response JSON parsing for meaningful error messages
  - 19 unit tests for key mapping, MD5 computation, composite ETag, URL encoding, error parsing, compose chaining math, base64 MD5 conversion, not-found detection
- **src/main.rs**: Updated backend factory to dispatch on `config.storage.backend`:
  - Added `"gcp"` case -> creates `GcpGatewayBackend` from `storage.gcp` config section
  - Existing `"aws"` and `"local"` cases unchanged
- **Cargo.toml**: Added 2 new dependencies:
  - `reqwest = { version = "0.12", features = ["json"] }` (moved from dev-deps to deps)
  - `base64 = "0.22"` (for GCS MD5 hash conversion in tests)
- **Config**: No changes needed -- `GcpStorageConfig` with `bucket`, `project`, `prefix` already existed from scaffold
- **Verified**: `cargo build` compiles clean (1 pre-existing warning in cluster stub), `cargo test` passes all 174 unit tests (19 new)
- Updated STATUS.md, DO_NEXT.md, WHAT_WE_DID.md, PLAN.md, tasks/done/stage-11a-gcp-gateway.md

## Session 14 -- Stage 10: AWS S3 Gateway Backend (2026-02-23)
- **src/storage/aws.rs**: Full implementation of `AwsGatewayBackend` using `aws-sdk-s3` crate:
  - All 10 `StorageBackend` trait methods implemented (put, get, delete, exists, copy_object, put_part, assemble_parts, delete_parts, create_bucket, delete_bucket)
  - Key mapping: `{prefix}{bleepstore_bucket}/{key}` in single upstream S3 bucket
  - Parts stored as temporary S3 objects at `{prefix}.parts/{upload_id}/{part_number}`
  - Multipart assembly using AWS native multipart upload with `upload_part_copy` (server-side copy)
  - EntityTooSmall fallback: downloads part data and re-uploads for parts < 5MB
  - Aborts AWS multipart upload on any assembly failure
  - MD5 computed locally for consistent ETags (AWS may differ with SSE)
  - Composite ETag computed locally from part MD5s for metadata consistency
  - Batch delete for parts cleanup using `list_objects_v2` + `delete_objects`
  - No-op create/delete bucket (BleepStore buckets are prefix-namespaced)
  - 9 unit tests for key mapping, MD5 computation, composite ETag
- **src/main.rs**: Updated backend factory to dispatch on `config.storage.backend`:
  - `"aws"` -> creates `AwsGatewayBackend` from `storage.aws` config section
  - `"local"` or default -> creates `LocalBackend` (unchanged behavior)
- **Cargo.toml**: Added `aws-config = "=1.8.13"` and `aws-sdk-s3 = "=1.122.0"` plus 18 transitive AWS SDK deps pinned to rustc 1.88-compatible versions (MSRV bumped to 1.91 on 2026-02-11)
- **Verified**: `cargo build` compiles clean, `cargo test` passes all 155 unit tests (9 new)
- Updated STATUS.md, DO_NEXT.md, WHAT_WE_DID.md, PLAN.md, tasks/done/stage-10-aws-gateway.md

## Session 13 -- Stage 9b: External Test Suite Code Review (2026-02-23)
- Performed thorough code review against Ceph s3-tests, MinIO Mint, and Snowflake s3compat expectations
- Verified XML namespaces, error formats, header values all match S3 spec
- Verified SigV4 auth handles all signing variants including presigned URLs
- Identified 4 minor cosmetic gaps (non-blocking):
  1. **encoding-type=url**: ListObjectsV2 could support encoding-type query param
  2. **Content-MD5 validation**: PutObject could validate Content-MD5 header
  3. **ListObjectsV1 Marker/NextMarker**: Verify correct behavior with delimiter
  4. **LocationConstraint us-east-1 quirk**: Verify empty element vs null response
- Updated STATUS.md, DO_NEXT.md, WHAT_WE_DID.md — Stage 9b complete

## Session 1 -- Project Scaffolding (2026-02-22)
- Created project structure: 23 source files + Cargo.toml
- All handler, metadata, storage, and cluster modules stubbed
- Config structs with serde Deserialize
- S3Error enum with thiserror + IntoResponse
- StorageBackend trait with all methods
- MetadataStore trait with all methods
- XML rendering with quick-xml (partially implemented)

## Session 2 -- Stage 1: Server Bootstrap & Configuration (2026-02-22)
- **Config**: Rewrote `config.rs` to match `bleepstore.example.yaml` format with nested structs (`metadata.sqlite.path`, `storage.local.root_dir`, `auth.access_key`/`secret_key`). Default port changed to 9012.
- **Errors**: Added 8 new error variants (`NotImplemented`, `BucketAlreadyOwnedByYou`, `EntityTooSmall`, `MalformedXML`, `InvalidAccessKeyId`, `MethodNotAllowed`, `MissingContentLength`, `InvalidRange`). Error responses now include `x-amz-request-id`, `Date`, `Server` headers.
- **Routing**: Complete rewrite of `server.rs` with query-parameter-based dispatch for all S3 operations. Added `GET /health` endpoint. Uses single handler per method+path that dispatches based on query params (`?location`, `?acl`, `?uploads`, `?list-type`, `?delete`, `?uploadId`, `?partNumber`).
- **Handlers**: All `todo!()` stubs replaced with `Err(S3Error::NotImplemented)` so server compiles and runs.
- **Middleware**: Added Tower middleware layer for common response headers (`x-amz-request-id`, `Date`, `Server: BleepStore`) on every response.
- **Main**: Added graceful shutdown (SIGTERM/SIGINT), crash-only startup logging, default config path `bleepstore.example.yaml`.
- **Dependencies**: Added `rand = "0.8"`, `httpdate = "1"` to Cargo.toml.

## Session 3 -- Stage 1b: OpenAPI, Validation & Observability (2026-02-23)
- **Cargo.toml**: Added 5 new dependencies: `utoipa` 5 (with `axum_extras`), `utoipa-swagger-ui` 8 (with `axum`), `garde` 0.22 (with `derive`, `regex`), `metrics` 0.24, `metrics-exporter-prometheus` 0.16.
- **src/metrics.rs** (new): Created Prometheus metrics module with:
  - OnceLock-based idempotent PrometheusBuilder installation (`init_metrics()`)
  - 7 metric name constants (HTTP RED metrics + S3 operation metrics + gauge/counter pairs)
  - `describe_metrics()` for registering metric descriptions
  - `metrics_middleware` Tower-compatible async fn: times requests, records http_requests_total and http_request_duration_seconds, excludes /metrics from self-instrumentation
  - `normalize_path()` helper mapping actual paths to templates (`/{bucket}`, `/{bucket}/{key}`) to prevent high-cardinality labels
  - `metrics_handler` rendering Prometheus text format via PrometheusHandle
  - Unit tests for path normalization
- **src/lib.rs**: Added `pub mod metrics;`
- **src/server.rs**: Added `#[derive(OpenApi)] struct ApiDoc` collecting all 24 handler paths across 4 tags (Health, Bucket, Object, Multipart). Added SwaggerUi at `/docs` with OpenAPI spec at `/openapi.json`. Added `/metrics` route. Added metrics_middleware as outermost layer, common_headers_middleware as inner layer. Added `#[utoipa::path]` annotation on `health_check`.
- **src/handlers/bucket.rs**: Added `#[utoipa::path]` annotations on all 7 handlers (list_buckets, create_bucket, delete_bucket, head_bucket, get_bucket_location, get_bucket_acl, put_bucket_acl). Added `BucketNameInput` struct with `#[derive(garde::Validate)]` and regex pattern for bucket name validation (preparation for Stage 2+).
- **src/handlers/object.rs**: Added `#[utoipa::path]` annotations on all 10 handlers (put_object, get_object, head_object, delete_object, delete_objects, copy_object, list_objects_v2, list_objects_v1, get_object_acl, put_object_acl). Removed unused `IntoResponse` import.
- **src/handlers/multipart.rs**: Added `#[utoipa::path]` annotations on all 6 handlers (create_multipart_upload, upload_part, complete_multipart_upload, abort_multipart_upload, list_multipart_uploads, list_parts). Removed unused `IntoResponse` import.
- **src/main.rs**: Added `init_metrics()` and `describe_metrics()` calls before building the app.
- **Verified**: `cargo build` compiles clean (only 3 pre-existing dead_code warnings in cluster/storage stubs).

## Session 4 -- Stage 2: Metadata Store & SQLite (2026-02-23)
- **src/metadata/store.rs**: Complete rewrite of the MetadataStore trait and record types:
  - Expanded `BucketRecord` with `owner_id`, `owner_display`, `acl` fields
  - Expanded `ObjectRecord` with `content_encoding`, `content_language`, `content_disposition`, `cache_control`, `expires`, `storage_class`, `acl`, `delete_marker` fields; removed `storage_key` (metadata is storage-agnostic)
  - Expanded `MultipartUploadRecord` with `content_type`, `content_encoding`, `content_language`, `content_disposition`, `cache_control`, `expires`, `storage_class`, `acl`, `user_metadata`, `owner_id`, `owner_display` fields
  - Simplified `PartRecord` to remove `storage_key` (parts tracked by upload_id + part_number)
  - Added `CredentialRecord` for SigV4 authentication credentials
  - Added `ListObjectsResult`, `ListUploadsResult`, `ListPartsResult` structs with pagination fields
  - Added `Acl`, `AclOwner`, `AclGrant`, `AclGrantee` types with serde derive for JSON serialization
  - Added trait methods: `bucket_exists`, `update_bucket_acl`, `object_exists`, `update_object_acl`, `delete_objects` (batch), `count_objects`, `get_multipart_upload`, `get_parts_for_completion`, `get_credential`, `put_credential`
  - Changed `list_objects` signature to accept `start_after` and return `ListObjectsResult`
  - Changed `list_parts` signature to accept `max_parts` and `part_number_marker`, return `ListPartsResult`
  - Changed `list_multipart_uploads` to accept `prefix`, `max_uploads`, `key_marker`, `upload_id_marker`, return `ListUploadsResult`
  - Changed `complete_multipart_upload` to accept `ObjectRecord` instead of `(u32, String)` pairs (assembly done at handler level)
- **src/metadata/sqlite.rs**: Full implementation of all MetadataStore trait methods:
  - Schema: 6 tables (`schema_version`, `buckets`, `objects`, `multipart_uploads`, `multipart_parts`, `credentials`) with all columns matching `specs/metadata-schema.md`
  - Indexes: `idx_objects_bucket`, `idx_objects_bucket_prefix`, `idx_uploads_bucket`, `idx_uploads_bucket_key`
  - Pragmas: WAL journal mode, NORMAL synchronous, foreign keys ON, 5s busy_timeout
  - `seed_credential()` for crash-only startup (INSERT OR IGNORE, idempotent)
  - `list_objects` with application-level delimiter grouping using BTreeSet for common prefixes
  - `complete_multipart_upload` uses BEGIN IMMEDIATE / COMMIT / ROLLBACK for transactional completion
  - ISO-8601 timestamp formatting via custom `format_timestamp()` (no chrono dep)
  - JSON serialization/deserialization for `user_metadata` and `acl` fields
  - 31 unit tests covering all CRUD operations, pagination, optional fields, schema idempotency, credential lifecycle
- **No HTTP handler changes** (all still return 501 NotImplemented as per plan)
- **No new dependencies** (rusqlite and serde_json already in Cargo.toml)
- **Verified**: `cargo build` compiles clean (same 3 pre-existing warnings)

## Session 5 -- Stage 3: Bucket CRUD (2026-02-23)
- **src/lib.rs**: Added `AppState` struct with `config: Config`, `metadata: Arc<dyn MetadataStore>`, `storage: Arc<dyn StorageBackend>`
- **src/main.rs**: Complete rewrite for Stage 3:
  - Initializes `SqliteMetadataStore` from config path (creates parent directories)
  - Seeds default credentials from config on every startup (crash-only, idempotent)
  - Initializes `LocalBackend` from config storage root
  - Builds `Arc<AppState>` and passes to `server::app()`
- **src/server.rs**: Updated all dispatch handlers to extract `State<Arc<AppState>>` and pass state + bucket name to bucket handlers. All bucket-level handlers now receive `Arc<AppState>` and bucket name. Object/multipart handlers still pass-through to stubs.
- **src/handlers/bucket.rs**: Complete rewrite with all 7 handlers implemented:
  - `validate_bucket_name()`: Standalone function checking 3-63 chars, lowercase, no IP address format, no xn-- prefix, no -s3alias/-ol-s3 suffix. 12 unit tests.
  - `list_buckets()`: Queries metadata, renders `<ListAllMyBucketsResult>` XML
  - `create_bucket()`: Full implementation with name validation, optional XML body parsing for region, `x-amz-acl` canned ACL header support, us-east-1 idempotent behavior (returns 200 if bucket already owned by same user), creates in metadata + storage
  - `delete_bucket()`: Checks existence (NoSuchBucket), checks emptiness (BucketNotEmpty), deletes from metadata + storage
  - `head_bucket()`: Returns 200 with `x-amz-bucket-region` header or 404 (no body per spec)
  - `get_bucket_location()`: Renders `<LocationConstraint>` XML, us-east-1 returns empty element
  - `get_bucket_acl()`: Renders `<AccessControlPolicy>` XML with full ACL structure
  - `put_bucket_acl()`: Accepts canned ACL header, updates metadata
  - Helper functions: `canned_acl_to_json()`, `default_acl_json()`, `now_iso8601()`, `parse_location_constraint()`
- **src/xml.rs**: Added two new rendering functions:
  - `render_location_constraint()`: Renders `<LocationConstraint>` XML with S3 namespace; empty self-closing element for us-east-1
  - `render_access_control_policy()`: Renders `<AccessControlPolicy>` XML with Owner, AccessControlList, Grant elements including `xsi:type` attributes on Grantee elements
- **src/storage/local.rs**: Implemented `create_bucket()` (creates directory) and `delete_bucket()` (removes directory). All other methods remain `todo!()`.
- **No new Cargo.toml dependencies** (quick-xml Reader already available for XML parsing)
- **Key design decisions**:
  - Bucket validation uses manual char checks (no regex dependency needed)
  - ACL stored as JSON, parsed with serde for rendering to XML
  - CreateBucket for us-east-1: returns 200 if bucket already exists and owned by same user
  - HEAD bucket returns plain 404 status (no body, per S3 spec)
  - Storage bucket directory creation is best-effort; metadata is source of truth

## Session 6 -- Stage 4: Basic Object CRUD (2026-02-23)
- **Cargo.toml**: Added 2 new dependencies:
  - `md-5 = "0.10"` -- for MD5 hash computation (ETag generation)
  - `tempfile = "3"` (dev-dependency) -- for unit tests with temporary directories
- **src/storage/local.rs**: Complete rewrite implementing the 4 core storage methods:
  - `put`: Crash-only write path -- creates temp file in `.tmp/{uuid}`, writes data, computes MD5 hash for ETag, calls `fsync_all()`, atomic `rename()` to final path. Creates parent directories for nested keys (keys with `/` separators).
  - `get`: Reads file from disk, returns `StoredObject` with `Bytes` data and SHA-256 content hash.
  - `delete`: Removes file (idempotent: no error if file doesn't exist).
  - `exists`: Checks if file path exists and is a regular file.
  - Constructor updated to create `.tmp` directory on startup for crash-only temp files.
  - Added `resolve()` method with path traversal protection: rejects storage keys containing `..` path components, validates canonical paths stay within root.
  - Added `temp_path()` helper generating UUID-based temp file paths.
  - 10 new unit tests: put/get roundtrip, empty objects, nested keys with parent dir creation, delete existing/nonexistent, exists check, get nonexistent error, put overwrites, ETag is correct MD5, create/delete bucket.
- **src/handlers/object.rs**: Complete rewrite implementing 4 object CRUD handlers:
  - `put_object(state, bucket, key, headers, body)`: Checks bucket existence, extracts Content-Type (defaults to `application/octet-stream`), user metadata (`x-amz-meta-*` headers), content-encoding/language/disposition, cache-control, expires, x-amz-acl canned ACL header. Writes via storage backend, records in metadata store. Returns 200 with ETag header.
  - `get_object(state, bucket, key, headers)`: Checks bucket existence, looks up metadata (NoSuchKey on miss), reads from storage, returns body with all appropriate headers: Content-Type, ETag, Content-Length, Last-Modified (converted from ISO-8601 to RFC 7231 format via `httpdate`), Accept-Ranges: bytes, plus optional headers (content-encoding/language/disposition, cache-control, expires) and user metadata headers.
  - `head_object(state, bucket, key, headers)`: Same as get_object but returns metadata headers only (no body, no storage read). Returns 404 directly for missing objects (no error XML body for HEAD, per S3 spec).
  - `delete_object(state, bucket, key)`: Deletes from storage (best-effort) + metadata (idempotent). Always returns 204 even if object didn't exist (per S3 spec).
  - Helper functions: `now_iso8601()`, `days_to_ymd()`, `extract_user_metadata()`, `extract_content_type()`, `iso8601_to_http_date()`, `ymd_to_days()`, `default_acl_json()`, `canned_acl_to_json()`.
  - Stubs remain for: delete_objects, copy_object, list_objects_v2, list_objects_v1, get_object_acl, put_object_acl (all return 501 NotImplemented -- these are Stage 5a/5b).
- **src/server.rs**: Updated 4 object-level dispatch functions to pass state, bucket, key, headers, and body to object handlers:
  - `handle_get_object`: Now passes `state, &bucket, &key, &headers` to `get_object`
  - `handle_put_object`: Now accepts `body: Bytes` extractor, passes `state, &bucket, &key, &headers, &body` to `put_object`
  - `handle_delete_object`: Now passes `state, &bucket, &key` to `delete_object`
  - `handle_head_object`: Now accepts `headers: HeaderMap` extractor, passes `state, &bucket, &key, &headers` to `head_object`
- **Key design decisions**:
  - Storage key format: `{bucket}/{key}` (simple flat mapping)
  - ETag = quoted hex MD5 hash (e.g., `"d41d8cd98f00b204e9800998ecf8427e"`)
  - Crash-only writes: temp-fsync-rename pattern in `.tmp/` directory
  - Head object does NOT read from storage -- builds response from metadata only (performance optimization)
  - Last-Modified header converted from ISO-8601 to RFC 7231 format via custom `iso8601_to_http_date()` using `httpdate::fmt_http_date()`
  - Delete always returns 204 (idempotent, per S3 spec)
  - Path traversal protection in storage backend: rejects `..` components
- **Verified**: `cargo build` compiles clean (1 pre-existing warning in cluster stub)

## Session 7 -- Stage 5a: List, Copy & Batch Delete (2026-02-23)
- **Cargo.toml**: Added 1 new dependency:
  - `percent-encoding = "2"` -- for URL-decoding `x-amz-copy-source` header paths
- **src/handlers/object.rs**: Implemented 4 new handlers (replacing 501 stubs):
  - `copy_object(state, dst_bucket, dst_key, headers)`: Parses `x-amz-copy-source` header, URL-decodes via `percent_encoding::percent_decode_str()`, splits into source bucket/key. Validates source/destination buckets exist and source object exists. Reads `x-amz-metadata-directive` header (COPY vs REPLACE). In COPY mode, copies all metadata from source; in REPLACE mode, reads metadata from request headers (Content-Type, user metadata, content-encoding/language/disposition, cache-control, expires, x-amz-acl). Copies file via `StorageBackend::copy_object()`, records destination metadata. Returns `<CopyObjectResult>` XML with ETag and LastModified.
  - `delete_objects(state, bucket, body)`: Parses `<Delete>` XML body using `quick_xml::Reader` (SAX-style parsing). Extracts `<Quiet>` flag and list of `<Object><Key>` elements. Deletes each object from storage (best-effort) and metadata. Returns `<DeleteResult>` XML with `<Deleted>` entries (suppressed in quiet mode) and `<Error>` entries.
  - `list_objects_v2(state, bucket, query)`: Extracts query params: `prefix`, `delimiter`, `max-keys` (default 1000), `start-after`, `continuation-token`. Queries metadata store via `list_objects()`. Builds `<ListBucketResult>` XML with `Name`, `Prefix`, `Delimiter`, `MaxKeys`, `KeyCount`, `IsTruncated`, `ContinuationToken`, `NextContinuationToken`, `StartAfter`, `Contents` entries, and `CommonPrefixes`.
  - `list_objects_v1(state, bucket, query)`: Extracts query params: `prefix`, `delimiter`, `max-keys` (default 1000), `marker`. Uses marker as start_after in metadata query. Builds `<ListBucketResult>` XML with `Name`, `Prefix`, `Marker`, `Delimiter`, `MaxKeys`, `IsTruncated`, `NextMarker`, `Contents` entries, and `CommonPrefixes`.
  - Added `parse_delete_xml()` helper function for SAX-style XML parsing of `<Delete>` body.
- **src/storage/local.rs**: Implemented `copy_object(bucket, src_key, dst_bucket, dst_key)`:
  - Reads source file data, computes MD5 ETag, writes to destination via crash-only temp-fsync-rename pattern.
  - Creates parent directories for destination key if needed.
  - Returns error if source file doesn't exist.
  - Added 3 new unit tests: copy within same bucket, copy across buckets, copy nonexistent source.
- **src/xml.rs**: Added 2 new rendering functions and updated 1:
  - `render_list_objects_result_v1()`: Renders `<ListBucketResult>` XML for V1 with `Marker`, `NextMarker`, `Delimiter` (only when non-empty), `Contents`, `CommonPrefixes`.
  - `render_delete_result()`: Renders `<DeleteResult>` XML with `<Deleted>` entries (suppressed in quiet mode) and `<Error>` entries. Uses new `DeletedEntry` and `DeleteErrorEntry` structs.
  - Updated `render_list_objects_result()` (V2): Added `KeyCount` element, `StartAfter` element (when non-empty), made `Delimiter` element conditional (only when non-empty).
- **src/server.rs**: Updated 3 dispatch functions:
  - `handle_get_bucket`: Routes `list-type=2` to `list_objects_v2(state, &bucket, &query)`, default (no list-type) to `list_objects_v1(state, &bucket, &query)`.
  - `handle_post_bucket`: Accepts `body: Bytes` extractor, routes `?delete` to `delete_objects(state, &bucket, &body)`.
  - `handle_put_object`: Routes `x-amz-copy-source` header to `copy_object(state, &bucket, &key, &headers)`.
- **Key design decisions**:
  - CopyObject uses `percent-encoding` crate for URL-decoding `x-amz-copy-source` (handles `%20`, `%2F`, etc.)
  - Delete XML parsing uses SAX-style quick_xml::Reader (not DOM) for efficiency
  - ListObjectsV2 includes `KeyCount` = objects + common_prefixes count
  - ListObjectsV1 includes `NextMarker` only when result is truncated
  - Delimiter element omitted from XML when empty (matches S3 behavior)
  - Quiet mode in DeleteObjects suppresses `<Deleted>` elements but always includes `<Error>` elements
- **Verified**: `cargo build` compiles clean (1 pre-existing warning in cluster stub)

## Session 8 -- Stage 5b: Range, Conditional Requests & Object ACLs (2026-02-23)
- **src/handlers/object.rs**: Major additions for range, conditional, and ACL support:
  - **Range request parsing**: Added `ByteRange` enum (`StartEnd`, `StartOpen`, `Suffix`), `parse_range_header()` for parsing `Range: bytes=start-end/start-/=-N` headers, `resolve_range()` for resolving against content length. Only single ranges supported.
  - **Conditional request evaluation**: Added `evaluate_conditions()` implementing full RFC 7232 priority: If-Match (412 on mismatch, skips If-Unmodified-Since), If-Unmodified-Since (412 if modified after date), If-None-Match (304 for GET/HEAD on match, skips If-Modified-Since), If-Modified-Since (304 if not modified).
  - **ETag comparison**: `strip_etag_quotes()` for quote-aware comparison. Supports wildcard `*` for If-Match.
  - **ISO-8601 parsing**: `parse_iso8601_to_system_time()` for date condition evaluation.
  - **Updated `get_object`**: Evaluates conditional headers before storage read. Parses Range header, returns 206 Partial Content with `Content-Range: bytes start-end/total` header, or 416 for unsatisfiable ranges. Malformed range headers are ignored (full body returned per HTTP spec).
  - **Updated `head_object`**: Now evaluates conditional headers (If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since).
  - **Implemented `get_object_acl`**: Accepts `state, bucket, key`. Looks up object metadata, parses ACL JSON, renders `<AccessControlPolicy>` XML via existing `render_access_control_policy()`. Returns NoSuchKey for missing objects.
  - **Implemented `put_object_acl`**: Accepts `state, bucket, key, headers, body`. Parses `x-amz-acl` canned ACL header, updates object ACL via `metadata.update_object_acl()`. Returns NoSuchKey for missing objects.
  - **19 unit tests**: Range parsing (start-end, start-open, suffix, invalid), range resolution (start-end clamping, unsatisfiable, suffix larger than file, empty file), ETag quote stripping, conditional evaluation (If-Match success/failure/wildcard, If-None-Match match/no-match, If-Unmodified-Since success/failure, If-Modified-Since not-modified/was-modified), ISO-8601 parsing.
- **src/errors.rs**: Added `NotModified` variant for 304 responses:
  - Maps to `StatusCode::NOT_MODIFIED` (304)
  - `IntoResponse` returns empty body (no XML error payload for 304, per HTTP spec)
  - Added to `code()` and `status_code()` match arms
- **src/server.rs**: Updated 2 object-level dispatch functions:
  - `handle_get_object`: Routes `?acl` to `get_object_acl(state, &bucket, &key)` (was returning 501)
  - `handle_put_object`: Routes `?acl` to `put_object_acl(state, &bucket, &key, &headers, &body)` (was returning 501)
- **No new Cargo.toml dependencies** (httpdate already in deps for date parsing)
- **No changes to xml.rs** (`render_access_control_policy()` already existed from Stage 3)
- **Key design decisions**:
  - Range: Only single ranges supported (multi-range returns full body). Malformed ranges ignored per HTTP spec. Unsatisfiable ranges return 416 InvalidRange.
  - Conditional: Full RFC 7232 priority chain. If-Match overrides If-Unmodified-Since; If-None-Match overrides If-Modified-Since.
  - 304: Empty response body with standard S3 headers (x-amz-request-id, Date, Server) but no Content-Type or XML error body.
  - Object ACLs: Same pattern as bucket ACLs -- JSON-serialized in metadata, rendered to XML on read.
- **Verified**: `cargo build` compiles clean (1 pre-existing warning in cluster stub)

## Session 9 -- Stage 6: AWS Signature V4 Authentication (2026-02-23)
- **Cargo.toml**: Added 1 new dependency:
  - `subtle = "2"` -- for constant-time signature comparison via `ConstantTimeEq`
- **src/auth.rs**: Complete rewrite from stubs to full SigV4 implementation (~550 lines):
  - **Types**: `ParsedAuthorization` (header-based auth fields), `ParsedPresigned` (presigned URL fields), `AuthType` enum (Header/Presigned/None), `AuthResult` enum for verification outcomes
  - **Detection**: `detect_auth_type()` inspects Authorization header and query string to determine auth method; returns error for ambiguous (both present)
  - **Authorization header parsing**: `parse_authorization_header()` extracts Credential (access key, date, region, service, scope), SignedHeaders, and Signature
  - **Presigned URL parsing**: `parse_presigned_params()` extracts X-Amz-* query parameters with percent-decoding and validation
  - **Canonical request**: `build_canonical_request()` per SigV4 spec; `build_canonical_query_string()` sorts and encodes, excludes X-Amz-Signature
  - **String to sign**: `build_string_to_sign()` creates AWS4-HMAC-SHA256 string-to-sign
  - **Signing key derivation**: `derive_signing_key()` implements 4-step HMAC chain
  - **Signature computation**: `compute_signature()` with hex encoding
  - **Constant-time comparison**: `constant_time_eq()` via `subtle::ConstantTimeEq`
  - **Full verification**: `verify_header_auth()` and `verify_presigned_auth()` perform complete SigV4 verification
  - **Expiration/clock checks**: `check_presigned_expiration()` (max 7 days), `check_clock_skew()` (15 min tolerance)
  - **URI encoding**: `s3_uri_encode()` with RFC 3986 + S3 exceptions
  - **Helper functions**: `parse_query_string()`, `collapse_whitespace()`, `extract_headers_for_signing()`, `parse_amz_date()`, `percent_decode()`
  - **25 unit tests**: signing key derivation, URI encoding, header parsing, canonical query, canonical request, string-to-sign, constant-time comparison, date parsing, auth type detection, full round-trip verification (header + presigned), wrong secret rejection
- **src/server.rs**: Added auth middleware:
  - `auth_middleware` function: detects auth type, looks up credentials, verifies signature
  - Skips /health, /metrics, /docs, /openapi.json
  - Returns AccessDenied (no auth, clock skew, expired), InvalidAccessKeyId (unknown key), SignatureDoesNotMatch (bad signature)
  - Wired as innermost layer via `axum::middleware::from_fn_with_state(state, auth_middleware)`
- **No changes needed to metadata/store.rs or metadata/sqlite.rs** (credential CRUD already implemented in Stage 2)
- **No changes needed to errors.rs** (auth error variants already existed)
- **Key design decisions**:
  - Auth middleware is innermost layer (closest to handlers) so infrastructure endpoints bypass auth
  - UNSIGNED-PAYLOAD default when x-amz-content-sha256 header missing
  - No signing key cache in Phase 1 (4 HMACs per request is fast enough)
  - UTF-8 multi-byte URI encoding handled correctly
- **Verified**: `cargo build` compiles clean (1 pre-existing warning in cluster stub)

## Session 10 -- Stage 7: Multipart Upload - Core (2026-02-23)
- **src/handlers/multipart.rs**: Complete rewrite implementing 5 of 6 multipart handlers:
  - `create_multipart_upload(state, bucket, key, headers)`: Generates UUID v4 upload ID, extracts content-type/encoding/language/disposition/cache-control/expires/x-amz-acl/user-metadata from headers, creates `MultipartUploadRecord` in metadata store, returns `<InitiateMultipartUploadResult>` XML with bucket/key/upload-id.
  - `upload_part(state, bucket, key, query, body)`: Extracts uploadId and partNumber from query params, validates part number range (1-10000), verifies upload exists and bucket/key match, writes part data via `storage.put_part()` (crash-only temp-fsync-rename), records `PartRecord` in metadata with size/etag/timestamp, returns 200 with ETag header.
  - `abort_multipart_upload(state, bucket, key, query)`: Extracts uploadId, verifies upload exists and bucket/key match, deletes parts from storage via `storage.delete_parts()`, deletes upload + parts from metadata via `metadata.delete_multipart_upload()` (cascade), returns 204 No Content.
  - `list_multipart_uploads(state, bucket, query)`: Checks bucket exists, extracts prefix/max-uploads/key-marker/upload-id-marker params, queries metadata via `list_multipart_uploads()`, builds `UploadEntry` list, renders `<ListMultipartUploadsResult>` XML with Bucket/KeyMarker/UploadIdMarker/NextKeyMarker/NextUploadIdMarker/MaxUploads/IsTruncated/Prefix and Upload entries (each with Key/UploadId/Initiator/Owner/StorageClass/Initiated).
  - `list_parts(state, bucket, key, query)`: Extracts uploadId/max-parts/part-number-marker, verifies upload exists and bucket/key match, queries metadata via `list_parts()`, builds `PartEntry` list, renders `<ListPartsResult>` XML with Bucket/Key/UploadId/Initiator/Owner/StorageClass/PartNumberMarker/NextPartNumberMarker/MaxParts/IsTruncated and Part entries (each with PartNumber/LastModified/ETag/Size).
  - `complete_multipart_upload()`: Still returns 501 NotImplemented (deferred to Stage 8).
  - Helper functions: `now_iso8601()`, `days_to_ymd()`, `extract_user_metadata()`, `extract_content_type()`, `default_acl_json()`, `canned_acl_to_json()`.
  - 9 unit tests: ISO-8601 formatting, user metadata extraction, content-type extraction, ACL JSON construction.
- **src/storage/local.rs**: Implemented 2 methods:
  - `put_part(bucket, upload_id, part_number, data)`: Creates `.multipart/{upload_id}/` directory, writes part data using crash-only temp-fsync-rename pattern, computes MD5 ETag. Part stored at `{root}/.multipart/{upload_id}/{part_number}`.
  - `delete_parts(bucket, upload_id)`: Removes entire `.multipart/{upload_id}/` directory. Idempotent: no error if directory doesn't exist.
  - 6 new unit tests: put_part_and_verify, put_part_overwrites, put_multiple_parts, delete_parts, delete_parts_nonexistent, put_part_etag_is_md5.
- **src/xml.rs**: Added 2 new rendering functions + types:
  - `UploadEntry` struct: key, upload_id, initiated, storage_class, owner_id, owner_display
  - `render_list_multipart_uploads_result()`: Renders `<ListMultipartUploadsResult>` XML with S3 namespace, Bucket, KeyMarker, UploadIdMarker, NextKeyMarker, NextUploadIdMarker, MaxUploads, IsTruncated, Prefix, and Upload entries with Initiator/Owner sub-elements.
  - `PartEntry` struct: part_number, last_modified, etag, size
  - `render_list_parts_result()`: Renders `<ListPartsResult>` XML with S3 namespace, Bucket, Key, UploadId, Initiator, Owner, StorageClass, PartNumberMarker, NextPartNumberMarker, MaxParts, IsTruncated, and Part entries.
  - Note: `render_initiate_multipart_upload_result()` already existed from scaffold.
- **src/server.rs**: Updated 5 dispatch functions to wire multipart handlers:
  - `handle_get_bucket`: Routes `?uploads` to `list_multipart_uploads(state, &bucket, &query)`
  - `handle_get_object`: Routes `?uploadId` to `list_parts(state, &bucket, &key, &query)`
  - `handle_put_object`: Routes `?partNumber&uploadId` to `upload_part(state, &bucket, &key, &query, &body)`
  - `handle_delete_object`: Routes `?uploadId` to `abort_multipart_upload(state, &bucket, &key, &query)`
  - `handle_post_object`: Now accepts headers and body extractors. Routes `?uploads` to `create_multipart_upload(state, &bucket, &key, &headers)`.
- **No new Cargo.toml dependencies** (uuid already in deps)
- **Key design decisions**:
  - Part storage path: `{root}/.multipart/{upload_id}/{part_number}` (flat structure per upload)
  - Part ETag = quoted hex MD5 hash (same as object ETag)
  - Crash-only writes for parts: temp-fsync-rename pattern in `.tmp/` directory
  - Upload ID verification: checks bucket/key match to prevent cross-upload manipulation
  - Part number validation: 1-10000 range per S3 spec
  - Abort is idempotent for storage (directory may not exist) and metadata (cascade delete)
  - List operations support pagination parameters from S3 spec
- **Verified**: `cargo build` compiles clean, `cargo test` passes all 135 unit tests

## Session 12 -- Stage 9a: E2E Verification & Fixes (2026-02-23)
- Ran full Python/boto3 E2E suite: 69/86 initially → 73/86 → 85/86 after fixes
- **src/errors.rs**: Added KeyTooLongError variant
- **src/handlers/object.rs**: Key length validation in put_object (rejects keys > 1024 bytes)
- **src/handlers/multipart.rs**: Implemented upload_part_copy handler with byte range support; fixed escaped `\!=` and `format\!` syntax errors from previous agent
- **src/auth.rs**: Fixed SigV4 canonical query string (decode-then-reencode prevents double-encoding); added body hash fallback when `x-amz-content-sha256` absent (fixes test_malformed_xml: 403→400); fixed presigned URL canonical query string
- **src/server.rs**: Added `DefaultBodyLimit::disable()` — removed axum's default 2MB body limit that blocked 5MB multipart parts (413 error); URL-decode query parameter keys and values in `parse_query` (fixes prefix/delimiter filtering for ListObjects)
- Unit tests: all pass
- E2E tests: **85/86 pass** (only `test_invalid_access_key` fails — known test bug with hardcoded port 9000)
- Updated STATUS.md, DO_NEXT.md, WHAT_WE_DID.md — Stage 9a complete

## Session 11 -- Stage 8: Multipart Upload - Completion (2026-02-23)
- **src/handlers/multipart.rs**: Implemented `complete_multipart_upload` handler and XML parsing:
  - `parse_complete_multipart_upload_xml()`: SAX-style XML parser using `quick_xml::Reader` that extracts `(PartNumber, ETag)` pairs from `<CompleteMultipartUpload>` body. Handles quoted/unquoted ETags, validates presence of both PartNumber and ETag per Part element.
  - `complete_multipart_upload(state, bucket, key, query, body)`: Full implementation including:
    - Extracts uploadId from query params
    - Verifies upload exists and bucket/key match
    - Parses CompleteMultipartUpload XML body
    - Validates ascending part order (returns InvalidPartOrder)
    - Validates each requested part exists in metadata and ETag matches (returns InvalidPart)
    - Enforces 5 MiB minimum part size for all non-last parts (returns EntityTooSmall)
    - Calls `storage.assemble_parts()` to concatenate parts into final object
    - Builds `ObjectRecord` from upload metadata (content-type, encoding, language, disposition, cache-control, expires, ACL, user-metadata, storage-class)
    - Completes in metadata store transactionally (insert object, delete upload + parts)
    - Cleans up part files from storage (best-effort)
    - Returns `<CompleteMultipartUploadResult>` XML with Location, Bucket, Key, ETag
  - Added `MIN_PART_SIZE` constant (5 MiB = 5 * 1024 * 1024)
  - 7 new unit tests for XML parsing: valid multi-part, single part, empty body, malformed XML, missing ETag, missing PartNumber, unquoted ETag
- **src/storage/local.rs**: Implemented `assemble_parts(bucket, key, upload_id, parts)`:
  - Reads each part file in order from `.multipart/{upload_id}/{part_number}`
  - Concatenates all parts to a temp file using crash-only temp-fsync-rename pattern
  - Computes composite ETag during assembly: MD5 hash of each part's data concatenated as binary, then MD5 of that concatenation, formatted as `"{hex_md5}-{part_count}"`
  - Creates parent directories for final object path if needed
  - 4 new unit tests: basic assembly with data verification, single part, nested key with parent dir creation, composite ETag format validation
- **src/server.rs**: Updated `handle_post_object` dispatch:
  - `POST /{bucket}/{key}?uploadId` now passes `state, &bucket, &key, &query, &body` to `complete_multipart_upload` (was calling stub with no args)
- **No new Cargo.toml dependencies** (quick_xml, md5 already in deps)
- **Key design decisions**:
  - Composite ETag computed by storage backend during assembly (not by handler) to avoid double-reading part files
  - ETag comparison is quote-insensitive: strips quotes from both requested and stored ETags before comparing
  - Part files cleaned up from storage after successful metadata completion (best-effort, not blocking)
  - Location URL format: `/{bucket}/{key}` (relative path, not absolute URL with host)
- **Verified**: `cargo build` compiles clean, `cargo test` passes all 146 unit tests (135 existing + 11 new)

## 2026-02-23: Stage 9a -- Core Integration Testing (Code Changes Applied)

### Analysis
- Read all 7 E2E test files (about 70 tests) and the entire Rust source codebase
- Identified 4 failing tests and their root causes

### Changes Made (4 files)
1. **src/errors.rs**: Added `KeyTooLongError` variant with code "KeyTooLongError" and status 400
2. **src/handlers/object.rs**: Added key length validation (>1024 bytes) at start of put_object()
3. **src/handlers/multipart.rs**: Implemented upload_part_copy() handler (about 150 lines) with:
   - x-amz-copy-source header parsing and URL decoding
   - Source bucket/key validation
   - Optional x-amz-copy-source-range byte range extraction
   - Part storage via put_part and metadata recording
   - CopyPartResult XML response
4. **src/server.rs**: Updated handle_put_object() dispatch to route UploadPartCopy

### Known Test Issues
- test_invalid_access_key: Hardcodes port 9000 in test code (test bug, not implementation bug)

### Pending
- Build verification: cargo build and cargo test
- E2E test execution against running server on port 9012
