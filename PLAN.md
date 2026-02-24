# BleepStore Implementation Plan

## Overview

This document defines a staged, language-agnostic implementation plan for BleepStore. Each stage is scoped to fit within approximately 100,000 tokens of Claude context (~300-500 lines of focused implementation work). Stages are grouped into milestones that represent meaningful functional boundaries.

**How to use this plan:**

1. This plan is duplicated into language-specific plans (e.g., `python/PLAN.md`, `golang/PLAN.md`) with language-specific details added.
2. Implementations for different languages proceed independently and in parallel.
3. Each stage has a clear "Definition of Done" with specific E2E tests that must pass.
4. Complete stages in order within each language -- later stages depend on earlier ones.
5. After each stage, run the specified test targets to verify correctness before proceeding.

**Token budget:** Each stage targets ~100k tokens of implementation context. This means the full spec references, existing code, and new code for one stage should fit comfortably within a single Claude session.

**Test-driven approach:** Every stage ends with concrete, runnable test targets. No stage is complete until its tests pass.

---

## Cross-Cutting Requirements

### Crash-Only Software Design (All Stages)

**Every implementation must follow crash-only methodology** (see `specs/crash-only.md`).
This is not a single stage — it is a design discipline that applies to every stage:

| Stage | Crash-Only Requirement |
|-------|----------------------|
| 1 (Server Bootstrap) | Every startup = crash recovery. No distinction between first boot and restart. Optional SIGTERM handler stops listener only — no cleanup. |
| 1b (Framework/Observability) | Metrics counters are best-effort — never block a request to update a counter. Prometheus scrape is read-only. |
| 2 (Metadata Store) | SQLite in WAL mode (`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL`). Schema creation is idempotent (`CREATE TABLE IF NOT EXISTS`). |
| 4 (Object CRUD) | **Temp-fsync-rename pattern** for all file writes. Never write directly to final object path. Clean orphan temp files on startup. |
| 5a-5b (Advanced Objects) | Delete metadata before deleting files (orphan files are harmless; missing files with metadata are not). |
| 7-8 (Multipart) | **Lease-based uploads** with configurable TTL. Reap expired uploads on startup. upload_id is natural idempotency key. |
| 9a (Integration) | Verify crash-only correctness: kill -9 during operations, restart, verify consistency. |
| 12a-14 (Cluster) | Raft state fsync'd before responding. Timeout-based failure detection, not disconnect notifications. |
| 15 (Performance) | SIGTERM handler: stop accepting connections, wait for in-flight with timeout, exit. No finalization. |

**Key rule: Never acknowledge before commit.** No 200/201/204 response until data is fsync'd and metadata is committed.

### Persistent Event Queues (Optional, Stages 16a-16c)

**Optional pluggable queue system** for event propagation and write-ahead consistency
(see `specs/event-queues.md`). Supports Redis, RabbitMQ/ActiveMQ, and Kafka.
Three consistency modes: `write-through` (default), `sync`, and `async`.
When disabled (default), BleepStore operates in direct mode with no queue involvement.

### Observability & OpenAPI (All Stages from 1b onward)

**Every implementation must expose OpenAPI docs, request validation, and Prometheus metrics**
(see `specs/observability-and-openapi.md`).

| Endpoint | Purpose |
|----------|---------|
| `/docs` | Swagger UI (interactive API documentation) |
| `/openapi.json` | OpenAPI 3.x JSON specification |
| `/metrics` | Prometheus exposition format metrics |
| `/health` | Health check (existing from Stage 1) |

---

## Prerequisites

### E2E Test Suite (tests/e2e/)

The E2E test suite is ready and shared across all implementations. It tests any BleepStore implementation via its S3-compatible HTTP endpoint using boto3.

**Setup:**
```bash
cd tests/
./run_tests.sh  # Auto-installs deps via uv, runs pytest
```

**Environment variables:**
```bash
BLEEPSTORE_ENDPOINT=http://localhost:9000
BLEEPSTORE_ACCESS_KEY=bleepstore
BLEEPSTORE_SECRET_KEY=bleepstore-secret
BLEEPSTORE_REGION=us-east-1
```

**Test files and counts:**
| File | Tests | Marker | Scope |
|---|---|---|---|
| `tests/e2e/test_buckets.py` | 16 | `bucket_ops` | Bucket CRUD, location, ACL |
| `tests/e2e/test_objects.py` | 32 | `object_ops` | Object CRUD, copy, list, range, conditional |
| `tests/e2e/test_multipart.py` | 11 | `multipart_ops` | Multipart upload lifecycle |
| `tests/e2e/test_presigned.py` | 4 | `presigned` | Presigned GET/PUT URLs |
| `tests/e2e/test_acl.py` | 4 | `acl_ops` | Object ACL operations |
| `tests/e2e/test_errors.py` | 8 | `error_handling`, `auth` | Error responses, auth errors |
| **Total** | **75** | | |

**Running specific markers:**
```bash
python -m pytest tests/e2e/ -v -m bucket_ops
python -m pytest tests/e2e/ -v -m object_ops
python -m pytest tests/e2e/ -v -m multipart_ops
```

### External Test Suites

These are optional but recommended for compliance validation:

| Suite | License | Purpose | Setup |
|---|---|---|---|
| **Ceph s3-tests** | MIT | ~400 S3 conformance tests | `git clone https://github.com/ceph/s3-tests && cd s3-tests && ./bootstrap` |
| **MinIO Mint** | Apache 2.0 | Multi-SDK validation (aws-cli, Go, Java, Python, Ruby) | `docker run -e SERVER_ENDPOINT=host:9000 -e ACCESS_KEY=bleepstore -e SECRET_KEY=bleepstore-secret minio/mint` |
| **Snowflake s3compat** | Apache 2.0 | 9 core operations quick check | `git clone https://github.com/Snowflake-Labs/s3-compat-tests` |
| **MinIO Warp** | AGPL | Performance benchmarks | `warp mixed --host=localhost:9000 --access-key=bleepstore --secret-key=bleepstore-secret` |

### Smoke Test

```bash
BLEEPSTORE_ENDPOINT=http://localhost:9000 tests/smoke/smoke_test.sh
```

Requires `aws` CLI v2 configured with BleepStore credentials. Tests 20 operations including bucket CRUD, object CRUD, multipart upload, ACLs, and error handling.

### Development Environment

Language-specific plans will detail:
- Required compiler/runtime version
- Dependency installation
- Build commands
- How to start the server
- How to run unit tests

---

## Milestone 1: Foundation (Stages 1, 1b, 2)

**Goal:** Server boots, accepts HTTP connections, returns proper S3 error XML for all routes, and has a working metadata store ready for bucket/object operations.

---

### Stage 1: Server Bootstrap & Configuration (~100k tokens)

**Goal:** A running HTTP server that loads configuration, routes all S3 API paths, and returns well-formed S3 error XML responses. Every request gets a unique request ID. The server does not yet do anything useful -- it just rejects everything with proper S3 error formatting.

**Inputs:** None (first stage).

**Spec references:**
- `specs/s3-error-responses.md` -- Error XML format, all error codes, common response headers
- `specs/s3-common-headers.md` -- Common request/response headers, date formats
- `specs/s3-bucket-operations.md` -- Bucket naming rules (for route parsing)
- `bleepstore.example.yaml` -- Configuration format

**Implementation scope:**

1. **Configuration loader**
   - Parse `bleepstore.yaml` (or `bleepstore.example.yaml` as fallback)
   - Load: `server.host`, `server.port`, `server.region`, `auth.access_key`, `auth.secret_key`, `metadata.engine`, `metadata.sqlite.path`, `storage.backend`, `storage.local.root_dir`
   - Provide defaults for all fields
   - Configuration struct/class accessible throughout the application

2. **HTTP server startup**
   - Listen on configured `host:port`
   - Health check endpoint: `GET /health` returns `200 OK` with `{"status": "ok"}`
   - **Crash-only startup**: every startup runs the full recovery sequence (clean temp files, check WAL, seed credentials). No `--recovery-mode` flag.
   - **Optional SIGTERM handler**: stop accepting connections, wait for in-flight requests with timeout, exit. Do NOT perform any cleanup that the startup path doesn't also do.

3. **Request router**
   - Parse incoming requests into S3 operation types based on method + path + query parameters
   - Route table covering all Phase 1 operations:
     - `GET /` -> ListBuckets
     - `PUT /{Bucket}` -> CreateBucket
     - `DELETE /{Bucket}` -> DeleteBucket
     - `HEAD /{Bucket}` -> HeadBucket
     - `GET /{Bucket}?location` -> GetBucketLocation
     - `GET /{Bucket}?acl` -> GetBucketAcl
     - `PUT /{Bucket}?acl` -> PutBucketAcl
     - `GET /{Bucket}?list-type=2` -> ListObjectsV2
     - `GET /{Bucket}` (no list-type) -> ListObjectsV1
     - `GET /{Bucket}?uploads` -> ListMultipartUploads
     - `PUT /{Bucket}/{Key+}` (no `x-amz-copy-source`, no `uploadId`) -> PutObject
     - `PUT /{Bucket}/{Key+}` (with `x-amz-copy-source`, no `uploadId`) -> CopyObject
     - `PUT /{Bucket}/{Key+}?partNumber=N&uploadId=ID` (no `x-amz-copy-source`) -> UploadPart
     - `PUT /{Bucket}/{Key+}?partNumber=N&uploadId=ID` (with `x-amz-copy-source`) -> UploadPartCopy
     - `PUT /{Bucket}/{Key+}?acl` -> PutObjectAcl
     - `GET /{Bucket}/{Key+}` (no `?acl`, no `?uploadId`) -> GetObject
     - `GET /{Bucket}/{Key+}?acl` -> GetObjectAcl
     - `GET /{Bucket}/{Key+}?uploadId=ID` -> ListParts
     - `HEAD /{Bucket}/{Key+}` -> HeadObject
     - `DELETE /{Bucket}/{Key+}` (no `?uploadId`) -> DeleteObject
     - `DELETE /{Bucket}/{Key+}?uploadId=ID` -> AbortMultipartUpload
     - `POST /{Bucket}?delete` -> DeleteObjects
     - `POST /{Bucket}/{Key+}?uploads` -> CreateMultipartUpload
     - `POST /{Bucket}/{Key+}?uploadId=ID` -> CompleteMultipartUpload
   - Extract bucket name and object key from URL path
   - Distinguish operations by query parameters (`?acl`, `?location`, `?uploads`, `?uploadId`, `?list-type=2`, `?delete`, `?partNumber`)

4. **S3 error response formatting**
   - `S3Error` type with fields: `code`, `message`, `http_status`, `resource`, `request_id`
   - XML serialization matching exact S3 format:
     ```xml
     <?xml version="1.0" encoding="UTF-8"?>
     <Error>
       <Code>NoSuchBucket</Code>
       <Message>The specified bucket does not exist</Message>
       <Resource>/bucket-name</Resource>
       <RequestId>XXXXXXXXXXXXXXXX</RequestId>
     </Error>
     ```
   - Error XML has NO xmlns namespace (unlike success responses)
   - Content-Type: `application/xml`
   - Define all Phase 1 error codes with correct HTTP status codes:
     - `NoSuchBucket` (404), `NoSuchKey` (404), `NoSuchUpload` (404)
     - `BucketAlreadyExists` (409), `BucketAlreadyOwnedByYou` (409), `BucketNotEmpty` (409)
     - `InvalidBucketName` (400), `InvalidArgument` (400), `InvalidPart` (400), `InvalidPartOrder` (400)
     - `EntityTooSmall` (400), `EntityTooLarge` (400), `MalformedXML` (400)
     - `AccessDenied` (403), `InvalidAccessKeyId` (403), `SignatureDoesNotMatch` (403)
     - `PreconditionFailed` (412), `InvalidRange` (416)
     - `NotImplemented` (501), `InternalError` (500)
     - `MethodNotAllowed` (405), `MissingContentLength` (411)

5. **Common response headers**
   - `x-amz-request-id`: Random hex string (16 characters) generated per request
   - `x-amz-id-2`: Random base64 string generated per request
   - `Date`: RFC 1123 format (e.g., `Sun, 22 Feb 2026 12:00:00 GMT`)
   - `Server`: `BleepStore`
   - `Content-Type`: Set appropriately (`application/xml` for XML responses)
   - `Content-Length`: Set for all responses with a body
   - These headers must appear on ALL responses (success and error)

6. **Stub handlers for all routes**
   - Every route returns `NotImplemented` (501) S3 error initially
   - This ensures the router is complete and error formatting works for all paths

**Key decisions:**
- Path-style addressing only for Phase 1 (virtual-hosted-style is optional, deferred)
- Configuration via YAML file in working directory (command-line override optional)
- Request ID: 16-character random hex string (e.g., `4442587FB7D0A2F9`)

**Test targets:**
- **Manual/unit tests:**
  - Server starts and listens on configured port
  - `GET /health` returns 200
  - All routed paths return 501 or proper S3 error XML
  - Error XML is well-formed with correct Content-Type
  - `x-amz-request-id` header present on all responses
  - `Date` header present on all responses

- **E2E tests (expected results at this stage):**
  - `tests/e2e/test_errors.py::TestErrorResponses::test_error_has_request_id` -- PASS (request-id header present, even on error)
  - Other E2E tests will fail with 501/NotImplemented or connection errors, which is expected

**Definition of done:**
- [x] Server starts on configured port from YAML config
- [x] Health check endpoint works
- [x] All S3 API routes are registered and reachable
- [x] Every route returns well-formed S3 error XML
- [x] `x-amz-request-id` and `Date` headers present on every response
- [x] Configuration loads from `bleepstore.yaml`

---

### Stage 1b: Framework Upgrade, OpenAPI & Observability (~100k tokens)

**Goal:** Upgrade HTTP frameworks, add OpenAPI serving with Swagger UI, request validation,
and Prometheus metrics. After this stage every implementation serves /docs, /openapi.json,
and /metrics alongside the existing /health endpoint.

**Inputs:** Stage 1 complete (all routes return 501 with proper S3 error XML).

**Spec references:** `specs/observability-and-openapi.md`, `schemas/s3-api.openapi.yaml`

**Implementation scope:**
1. Framework migration (Go: net/http → Huma, Zig: std.http.Server → tokamak)
2. OpenAPI integration (Python: FastAPI built-in, Go: Huma built-in, Rust: utoipa, Zig: tokamak built-in)
3. Request validation wiring (Python: Pydantic, Go: Huma JSON Schema, Rust: garde, Zig: hand-written)
4. Prometheus metrics endpoint with HTTP and S3 operation counters
5. Swagger UI serving at /docs
6. All existing Stage 1 tests must still pass (health check, 501 error XML, common headers)

**Key decisions:**
- Python stays on FastAPI (already the target stack)
- Go switches to Huma with Chi or stdlib router adapter
- Rust stays on axum, adds utoipa (proc macros) + garde (validation) + utoipa-swagger-ui
- Zig switches to tokamak (built on httpz), gains basic OpenAPI 3.0 + Swagger UI
- Consistent endpoint paths: /docs, /openapi.json, /metrics, /health

**Test targets:**
- GET /docs returns HTML (Swagger UI)
- GET /openapi.json returns valid OpenAPI JSON
- GET /metrics returns Prometheus text format with bleepstore_* metrics
- GET /health still returns {"status":"ok"}
- All existing S3 route stubs still return 501 NotImplemented XML

**Definition of done:**
- [ ] Framework migrated (Go, Zig) or augmented (Rust, Python)
- [ ] /docs serves Swagger UI
- [ ] /openapi.json returns valid OpenAPI spec
- [ ] /metrics returns Prometheus metrics (at minimum: request count, latency histogram)
- [ ] Request validation rejects malformed input with S3 error XML
- [ ] All Stage 1 tests still pass
- [ ] Unit tests for metrics increment and OpenAPI spec validity

---

### Stage 2: Metadata Store & SQLite (~100k tokens)

**Goal:** A working SQLite-backed metadata store with full CRUD for buckets, objects, multipart uploads, and credentials. This is a data-layer-only stage -- no HTTP handlers are modified.

**Inputs:** Stage 1 complete (configuration loading provides `metadata.sqlite.path`).

**Spec references:**
- `specs/metadata-schema.md` -- Complete SQLite schema, queries, ACL JSON format, concurrency model

**Implementation scope:**

1. **SQLite database initialization**
   - Open/create SQLite database at configured path (`metadata.sqlite.path`)
   - Create parent directories if needed
   - Apply pragmas:
     ```sql
     PRAGMA journal_mode = WAL;
     PRAGMA synchronous = NORMAL;
     PRAGMA foreign_keys = ON;
     PRAGMA busy_timeout = 5000;
     ```

2. **Schema creation** -- Create all tables if not exists:
   - `buckets` (name TEXT PK, region, owner_id, owner_display, acl JSON, created_at ISO8601)
   - `objects` (bucket+key composite PK, size, etag, content_type, content_encoding, content_language, content_disposition, cache_control, expires, storage_class, acl JSON, user_metadata JSON, last_modified, delete_marker)
   - `multipart_uploads` (upload_id TEXT PK, bucket, key, content_type, content_encoding, content_language, content_disposition, cache_control, expires, storage_class, acl, user_metadata, owner_id, owner_display, initiated_at)
   - `multipart_parts` (upload_id+part_number composite PK, size, etag, last_modified)
   - `credentials` (access_key_id TEXT PK, secret_key, owner_id, display_name, active, created_at)
   - `schema_version` (version INTEGER PK, applied_at)
   - All indexes: `idx_objects_bucket`, `idx_objects_bucket_prefix`, `idx_uploads_bucket`, `idx_uploads_bucket_key`

3. **Metadata store interface/trait/protocol** -- Define the abstract interface:
   - **Bucket operations:**
     - `create_bucket(name, region, owner_id, owner_display, acl) -> void`
     - `get_bucket(name) -> BucketMeta | None`
     - `list_buckets(owner_id) -> List[BucketMeta]`
     - `delete_bucket(name) -> void`
     - `bucket_exists(name) -> bool`
     - `update_bucket_acl(name, acl) -> void`
   - **Object metadata operations:**
     - `put_object_meta(bucket, key, size, etag, content_type, content_encoding, content_language, content_disposition, cache_control, expires, storage_class, acl, user_metadata) -> void`
     - `get_object_meta(bucket, key) -> ObjectMeta | None`
     - `delete_object_meta(bucket, key) -> void`
     - `delete_objects_meta(bucket, keys) -> List[DeleteResult]`
     - `list_objects(bucket, prefix, delimiter, max_keys, start_after/marker, continuation_token) -> ListResult`
     - `object_exists(bucket, key) -> bool`
     - `update_object_acl(bucket, key, acl) -> void`
   - **Multipart upload operations:**
     - `create_multipart_upload(upload_id, bucket, key, content_type, ..., owner_id) -> void`
     - `get_multipart_upload(upload_id) -> UploadMeta | None`
     - `list_multipart_uploads(bucket, prefix, delimiter, key_marker, upload_id_marker, max_uploads) -> ListUploadsResult`
     - `delete_multipart_upload(upload_id) -> void`
     - `register_part(upload_id, part_number, size, etag) -> void`
     - `list_parts(upload_id, max_parts, part_number_marker) -> ListPartsResult`
     - `get_parts_for_completion(upload_id) -> List[PartMeta]`
     - `complete_multipart_upload(upload_id, bucket, key, final_size, final_etag, content_type, ..., acl, user_metadata) -> void`
   - **Credential operations:**
     - `get_credential(access_key_id) -> Credential | None`
     - `put_credential(access_key_id, secret_key, owner_id, display_name) -> void`

4. **SQLite implementation of the metadata store interface**
   - Implement all methods above using the queries from `specs/metadata-schema.md`
   - `list_objects` with delimiter support requires application-level CommonPrefixes grouping:
     - Query all matching keys with prefix filter
     - Group keys by delimiter to produce CommonPrefixes
     - Implement pagination via continuation tokens (use last key as token)
   - `list_objects` v1 vs v2 differences handled at this level:
     - v2: `continuation_token`, `start_after`, `KeyCount`
     - v1: `marker`, `NextMarker` (only when delimiter used)
   - Upsert semantics for `put_object_meta` (INSERT OR REPLACE)
   - Upsert semantics for `register_part` (same part_number overwrites)
   - `complete_multipart_upload`:
     - Insert into `objects` table
     - Delete from `multipart_uploads` and `multipart_parts`
     - Wrap in a transaction

5. **Seed default credentials on startup**
   - Insert configured `auth.access_key` / `auth.secret_key` into credentials table
   - Owner ID derived from access key (e.g., SHA-256 hash of access key)
   - Skip if credential already exists (idempotent startup)

6. **Data types for metadata results**
   - `BucketMeta`: name, region, owner_id, owner_display, acl (parsed), created_at
   - `ObjectMeta`: bucket, key, size, etag, content_type, content_encoding, content_language, content_disposition, cache_control, expires, storage_class, acl, user_metadata (parsed dict/map), last_modified
   - `UploadMeta`: upload_id, bucket, key, content_type, ..., owner_id, initiated_at
   - `PartMeta`: upload_id, part_number, size, etag, last_modified
   - `ListResult`: contents (list of ObjectMeta), common_prefixes (list of strings), is_truncated, next_continuation_token/next_marker, key_count
   - `ListUploadsResult`: uploads, common_prefixes, is_truncated, next_key_marker, next_upload_id_marker
   - `ListPartsResult`: parts, is_truncated, next_part_number_marker

**Key decisions:**
- ACLs stored as JSON strings in SQLite (not normalized tables) for simplicity
- User metadata stored as JSON strings
- Continuation tokens for ListObjectsV2 are opaque strings (last key works fine)
- Owner ID derived deterministically from access key
- Write concurrency: single writer via SQLite WAL mode + application-level mutex/lock

**Test targets:**
- **Unit tests (language-specific, in-process):**
  - Create/get/list/delete buckets -- verify round-trip
  - Bucket exists / does not exist
  - Put/get/delete object metadata -- verify all fields round-trip
  - List objects with prefix, delimiter, pagination, start_after
  - CommonPrefixes grouping with delimiter `/`
  - Create multipart upload, register parts, list parts
  - Complete multipart upload -- object metadata created, upload/parts deleted
  - Abort multipart upload -- upload and parts deleted
  - Credential lookup
  - Schema creation is idempotent (run twice)

- **E2E tests (expected results at this stage):**
  - No new E2E tests pass yet (HTTP handlers still return NotImplemented)

**Definition of done:**
- [x] SQLite database created at configured path with correct schema
- [x] All pragma settings applied
- [x] All metadata store methods implemented and tested with unit tests
- [x] ListObjects with delimiter correctly computes CommonPrefixes
- [x] Pagination works for all list operations
- [x] Default credentials seeded on startup
- [x] All unit tests pass

---

## Milestone 2: Bucket Operations (Stage 3)

**Goal:** All 7 bucket operations work end-to-end. The BleepStore E2E bucket tests pass.

---

### Stage 3: Bucket CRUD (~100k tokens)

**Goal:** Implement all bucket operation handlers, wiring them to the metadata store. After this stage, bucket creation, deletion, listing, head, location, and ACLs all work.

**Inputs:** Stages 1-2 complete (server running, metadata store ready).

**Spec references:**
- `specs/s3-bucket-operations.md` -- All 7 bucket operations in detail
- `specs/s3-error-responses.md` -- Error codes for bucket operations
- `specs/s3-common-headers.md` -- Response headers

**Implementation scope:**

1. **XML response serialization utilities**
   - XML declaration: `<?xml version="1.0" encoding="UTF-8"?>`
   - S3 namespace: `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`
   - Functions/methods to build XML elements for all bucket responses
   - Helper for ISO 8601 dates: `2026-02-22T12:00:00.000Z`
   - Helper for RFC 7231 dates: `Sun, 22 Feb 2026 12:00:00 GMT`
   - Content-Type for XML responses: `application/xml`

2. **CreateBucket handler** (`PUT /{Bucket}`)
   - Validate bucket name against naming rules:
     - 3-63 characters
     - Lowercase letters, numbers, hyphens, periods only
     - Must begin and end with letter or number
     - Not formatted as IP address
     - Must not start with `xn--` or end with `-s3alias` or `--ol-s3`
   - Parse optional request body for `<LocationConstraint>`
   - If bucket already owned by caller (us-east-1 behavior): return 200 OK
   - Create bucket in metadata store
   - Response: 200 OK, `Location: /{BucketName}`, no body
   - Errors: `InvalidBucketName` (400), `BucketAlreadyExists` (409), `BucketAlreadyOwnedByYou` (200 in us-east-1)

3. **DeleteBucket handler** (`DELETE /{Bucket}`)
   - Check bucket exists -> `NoSuchBucket` (404)
   - Check bucket is empty (no objects, no in-progress multipart uploads) -> `BucketNotEmpty` (409)
   - Delete bucket from metadata store
   - Response: **204 No Content** (not 200)

4. **HeadBucket handler** (`HEAD /{Bucket}`)
   - Check bucket exists
   - Response: 200 OK with `x-amz-bucket-region` header, NO body
   - Error: 404 status code only (no body on HEAD errors)

5. **ListBuckets handler** (`GET /`)
   - Query metadata store for all buckets owned by current user
   - Build `ListAllMyBucketsResult` XML response:
     ```xml
     <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
       <Owner><ID>...</ID><DisplayName>...</DisplayName></Owner>
       <Buckets>
         <Bucket><Name>...</Name><CreationDate>...</CreationDate></Bucket>
       </Buckets>
     </ListAllMyBucketsResult>
     ```
   - Response: 200 OK

6. **GetBucketLocation handler** (`GET /{Bucket}?location`)
   - Check bucket exists -> `NoSuchBucket` (404)
   - Build `LocationConstraint` XML response
   - us-east-1 quirk: return empty `<LocationConstraint/>` (not the string `us-east-1`)
   - Response: 200 OK

7. **GetBucketAcl handler** (`GET /{Bucket}?acl`)
   - Check bucket exists -> `NoSuchBucket` (404)
   - Parse stored ACL JSON into `AccessControlPolicy` XML:
     ```xml
     <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
       <Owner><ID>...</ID><DisplayName>...</DisplayName></Owner>
       <AccessControlList>
         <Grant>
           <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
             <ID>...</ID><DisplayName>...</DisplayName>
           </Grantee>
           <Permission>FULL_CONTROL</Permission>
         </Grant>
       </AccessControlList>
     </AccessControlPolicy>
     ```
   - Response: 200 OK

8. **PutBucketAcl handler** (`PUT /{Bucket}?acl`)
   - Check bucket exists -> `NoSuchBucket` (404)
   - Three mutually exclusive modes:
     - Canned ACL via `x-amz-acl` header
     - Explicit grants via `x-amz-grant-*` headers
     - XML body with `<AccessControlPolicy>`
   - Parse and store ACL in metadata store
   - Response: 200 OK, no body

9. **ACL helper functions**
   - Parse canned ACL names to grant lists: `private`, `public-read`, `public-read-write`, `authenticated-read`
   - Build default ACL (owner FULL_CONTROL) for new buckets
   - Serialize ACL to JSON for storage
   - Deserialize ACL from JSON for XML response

**Key decisions:**
- Authentication is skipped in this stage (all requests treated as authenticated). Auth is Stage 6.
- Owner ID used for ListBuckets filtering comes from configuration (single-user for now).
- ACL enforcement is not done yet (just store/retrieve). Enforcement deferred until auth is in place.

**Test targets:**

- **E2E tests (16 tests in `tests/e2e/test_buckets.py`):**
  - `TestCreateBucket::test_create_bucket` -- Create and verify
  - `TestCreateBucket::test_create_bucket_already_exists` -- Idempotent create
  - `TestCreateBucket::test_create_bucket_invalid_name` -- Uppercase rejected
  - `TestCreateBucket::test_create_bucket_too_short_name` -- "ab" rejected
  - `TestDeleteBucket::test_delete_bucket` -- Delete empty bucket, 204
  - `TestDeleteBucket::test_delete_nonexistent_bucket` -- NoSuchBucket
  - `TestDeleteBucket::test_delete_nonempty_bucket` -- BucketNotEmpty
  - `TestHeadBucket::test_head_existing_bucket` -- 200 OK
  - `TestHeadBucket::test_head_nonexistent_bucket` -- 404
  - `TestListBuckets::test_list_buckets` -- Contains created bucket
  - `TestListBuckets::test_list_buckets_has_owner` -- Owner in response
  - `TestListBuckets::test_list_buckets_creation_date` -- CreationDate present
  - `TestGetBucketLocation::test_get_bucket_location` -- Returns region
  - `TestGetBucketLocation::test_get_location_nonexistent_bucket` -- NoSuchBucket
  - `TestBucketAcl::test_get_bucket_acl_default` -- Owner has FULL_CONTROL
  - `TestBucketAcl::test_put_bucket_acl_canned` -- Set public-read

- **E2E tests (partial from `tests/e2e/test_errors.py`):**
  - `TestErrorResponses::test_nosuchbucket_error` -- NoSuchBucket error format
  - `TestErrorResponses::test_invalid_bucket_name_error` -- InvalidBucketName
  - `TestErrorResponses::test_error_has_request_id` -- x-amz-request-id present

- **Smoke test (partial):**
  - `create-bucket`, `head-bucket`, `get-bucket-location`, `list-buckets` -- PASS

**Definition of done:**
- [x] All 16 tests in `test_buckets.py` pass
- [x] `test_errors.py::TestErrorResponses::test_nosuchbucket_error` passes
- [x] `test_errors.py::TestErrorResponses::test_invalid_bucket_name_error` passes
- [x] `test_errors.py::TestErrorResponses::test_error_has_request_id` passes
- [x] Smoke test bucket operations pass
- [x] XML responses have correct namespace (`xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`)
- [x] Error XML has no namespace

---

## Milestone 3: Object Operations (Stages 4-5b)

**Goal:** All object operations work: put, get, head, delete, copy, list (v1 and v2), batch delete, range requests, conditional requests, and object ACLs. The full `test_objects.py` and `test_acl.py` suites pass.

---

### Stage 4: Basic Object CRUD (~100k tokens)

**Goal:** Implement the local filesystem storage backend and basic object operations: PutObject, GetObject, HeadObject, DeleteObject. Objects can be stored and retrieved with correct ETags, content types, and user metadata.

**Inputs:** Stages 1-3 complete (server running, metadata store working, buckets can be created).

**Spec references:**
- `specs/s3-object-operations.md` -- Sections 1-4: PutObject, GetObject, HeadObject, DeleteObject
- `specs/storage-backends.md` -- Local filesystem backend, StorageBackend interface
- `specs/s3-common-headers.md` -- Object-specific response headers, ETag format

**Implementation scope:**

1. **Storage backend interface/trait/protocol**
   - Define the abstract interface matching `specs/storage-backends.md`:
     - `put_object(bucket, key, data_stream, content_length) -> etag`
     - `get_object(bucket, key, byte_range) -> data_stream`
     - `delete_object(bucket, key) -> void`
     - `head_object(bucket, key) -> ObjectInfo (size, etag, last_modified)`
     - `copy_object(src_bucket, src_key, dst_bucket, dst_key) -> etag`
     - `delete_objects(bucket, keys) -> List[DeleteResult]`
     - `create_bucket(bucket) -> void`
     - `delete_bucket(bucket) -> void`
     - `bucket_exists(bucket) -> bool`
     - `object_exists(bucket, key) -> bool`

2. **Local filesystem storage backend**
   - Root directory from config: `storage.local.root_dir`
   - Create root directory on startup if it does not exist
   - Data layout: `{root_dir}/{bucket-name}/{key}` (keys with `/` create subdirectories)
   - `put_object`:
     - Create bucket directory if needed
     - Create parent directories for keys with `/`
     - **Crash-only: temp-fsync-rename pattern**:
       1. Write to temp file in `{root_dir}/.tmp/{uuid}`
       2. fsync() the temp file
       3. rename() to final path `{root_dir}/{bucket}/{key}` (atomic on POSIX)
       4. Commit metadata to SQLite
     - If process crashes at any step: orphan temp files cleaned on next startup
     - Compute MD5 hash during write (streaming), return quoted hex ETag
   - `get_object`:
     - Open file at `{root_dir}/{bucket}/{key}`
     - Return streaming read
     - Support byte ranges: `bytes=start-end`, `bytes=start-`, `bytes=-suffix`
   - `delete_object`:
     - Remove file at `{root_dir}/{bucket}/{key}`
     - Clean up empty parent directories (optional, not required)
   - `head_object`:
     - Stat file for size and modification time
     - ETag must be stored/retrievable (compute on write, store in metadata DB)
   - `create_bucket`: Create directory `{root_dir}/{bucket}`
   - `delete_bucket`: Remove directory `{root_dir}/{bucket}` (must be empty)
   - `object_exists`: Check file exists at path
   - Multipart storage (for later stages): `{root_dir}/.multipart/{upload_id}/{part_number}`

3. **PutObject handler** (`PUT /{Bucket}/{Key+}`)
   - Check bucket exists -> `NoSuchBucket` (404)
   - Read request body (streaming preferred)
   - Extract headers: `Content-Type` (default `application/octet-stream`), `Content-Length`, `x-amz-meta-*` (user metadata), `x-amz-acl`, `Cache-Control`, `Content-Encoding`, `Content-Disposition`, `Content-Language`, `Expires`
   - Write to storage backend -> get ETag (MD5)
   - Write object metadata to metadata store (upsert)
   - Response: 200 OK with `ETag` header (quoted MD5 hex)

4. **GetObject handler** (`GET /{Bucket}/{Key+}`)
   - Check bucket exists -> `NoSuchBucket` (404)
   - Get object metadata from metadata store -> `NoSuchKey` (404) if not found
   - Read from storage backend and stream to response
   - Set response headers: `Content-Type`, `Content-Length`, `ETag`, `Last-Modified` (RFC 7231), `Accept-Ranges: bytes`, `x-amz-meta-*`, `Cache-Control`, `Content-Encoding`, `Content-Disposition`, `Content-Language`, `Expires`
   - Response: 200 OK with body

5. **HeadObject handler** (`HEAD /{Bucket}/{Key+}`)
   - Same logic as GetObject but NO response body
   - All headers identical to GetObject response
   - Error: 404 status code only (no XML body for HEAD)

6. **DeleteObject handler** (`DELETE /{Bucket}/{Key+}`)
   - Delete from storage backend and metadata store
   - **Idempotent**: return 204 even if key does not exist
   - Response: 204 No Content

**Key decisions:**
- ETag is computed during write as MD5 of the raw content, stored in metadata DB
- Streaming is preferred but buffering entire objects is acceptable for Phase 1 (optimize in Stage 15)
- Storage backend and metadata store operations are NOT in a single transaction; handle partial failures gracefully (metadata is source of truth)

**Test targets:**

- **E2E tests (partial from `tests/e2e/test_objects.py`, 16 tests):**
  - `TestPutAndGetObject::test_put_and_get_small_object` -- PUT then GET
  - `TestPutAndGetObject::test_put_object_etag` -- ETag is MD5
  - `TestPutAndGetObject::test_put_object_with_metadata` -- User metadata round-trip
  - `TestPutAndGetObject::test_put_object_overwrite` -- Overwrite existing key
  - `TestPutAndGetObject::test_put_object_with_slash_in_key` -- Nested key paths
  - `TestPutAndGetObject::test_put_empty_object` -- Zero-byte object
  - `TestPutAndGetObject::test_get_nonexistent_object` -- NoSuchKey
  - `TestPutAndGetObject::test_get_object_in_nonexistent_bucket` -- NoSuchBucket
  - `TestHeadObject::test_head_object` -- Returns metadata without body
  - `TestHeadObject::test_head_nonexistent_object` -- 404
  - `TestDeleteObject::test_delete_object` -- Delete and verify gone
  - `TestDeleteObject::test_delete_nonexistent_object` -- 204 idempotent

- **E2E tests (from `tests/e2e/test_errors.py`):**
  - `TestErrorResponses::test_nosuchkey_error` -- NoSuchKey format
  - `TestErrorResponses::test_bucket_not_empty_error` -- BucketNotEmpty (now testable since objects can be created)

- **Smoke test (partial):**
  - `put-object (s3 cp)`, `put-object (s3api)`, `get-object (s3 cp)`, `get-object content match`, `head-object`, `delete-object` -- PASS

**Definition of done:**
- [x] PutObject stores object data and metadata
- [x] GetObject retrieves object data with correct headers
- [x] HeadObject returns metadata without body
- [x] DeleteObject is idempotent (204 for both existing and non-existing)
- [x] ETags are correctly computed MD5 hex digests, quoted
- [x] User metadata (`x-amz-meta-*`) round-trips correctly
- [x] Content-Type defaults to `application/octet-stream` and is preserved
- [x] 12 basic object tests pass in `test_objects.py`
- [x] Storage backend writes atomically (temp file + rename)

---

### Stage 5a: List, Copy & Batch Delete (~100k tokens)

**Goal:** Implement CopyObject, DeleteObjects (batch), ListObjectsV2, and ListObjects v1.

**Inputs:** Stage 4 complete (basic object CRUD works).

**Spec references:**
- `specs/s3-object-operations.md` -- Sections 5-8: DeleteObjects, CopyObject, ListObjectsV2, ListObjectsV1
- `specs/s3-common-headers.md` -- Content-Range (for reference only, implemented in 5b)

**Implementation scope:**

1. **CopyObject handler** (`PUT /{Bucket}/{Key+}` with `x-amz-copy-source`)
   - Parse `x-amz-copy-source` header: `/{source-bucket}/{source-key}` (URL-decode)
   - Check source exists -> `NoSuchKey` (404)
   - Parse `x-amz-metadata-directive`: `COPY` (default) or `REPLACE`
     - `COPY`: preserve source metadata
     - `REPLACE`: use metadata from request headers
   - Copy data via storage backend `copy_object`
   - Create/update destination object metadata
   - Response: 200 OK with `CopyObjectResult` XML:
     ```xml
     <CopyObjectResult>
       <ETag>"..."</ETag>
       <LastModified>2026-02-22T12:00:00.000Z</LastModified>
     </CopyObjectResult>
     ```

2. **DeleteObjects handler** (`POST /{Bucket}?delete`)
   - Parse XML request body:
     ```xml
     <Delete><Quiet>false</Quiet><Object><Key>...</Key></Object>...</Delete>
     ```
   - Max 1000 objects per request
   - Delete each object (metadata + storage)
   - Build response XML:
     ```xml
     <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
       <Deleted><Key>...</Key></Deleted>
       <Error><Key>...</Key><Code>...</Code><Message>...</Message></Error>
     </DeleteResult>
     ```
   - `Quiet=true`: only include `<Error>` elements (no `<Deleted>`)
   - HTTP status is always 200 even if individual deletions fail

3. **ListObjectsV2 handler** (`GET /{Bucket}?list-type=2`)
   - Parse query parameters: `prefix`, `delimiter`, `max-keys` (default 1000), `continuation-token`, `start-after`, `fetch-owner`, `encoding-type`
   - Query metadata store `list_objects` method
   - Build `ListBucketResult` XML:
     ```xml
     <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
       <Name>bucket</Name><Prefix>...</Prefix><Delimiter>...</Delimiter>
       <MaxKeys>1000</MaxKeys><KeyCount>N</KeyCount>
       <IsTruncated>false</IsTruncated>
       <Contents><Key>...</Key><LastModified>...</LastModified><ETag>...</ETag><Size>N</Size><StorageClass>STANDARD</StorageClass></Contents>
       <CommonPrefixes><Prefix>photos/</Prefix></CommonPrefixes>
     </ListBucketResult>
     ```
   - Pagination: when `IsTruncated=true`, include `NextContinuationToken`

4. **ListObjects v1 handler** (`GET /{Bucket}` without `list-type`)
   - Parse query parameters: `prefix`, `delimiter`, `max-keys`, `marker`, `encoding-type`
   - Key differences from v2:
     - Uses `Marker`/`NextMarker` instead of `ContinuationToken`
     - No `KeyCount` element
     - Owner always included
     - `NextMarker` only returned when delimiter is used
   - Build response XML with `<ListBucketResult>` (same root element as v2)

**Key decisions:**
- CopyObject copies actual data via storage backend (no hard links or references)
- DeleteObjects processes deletions sequentially (parallelism is an optimization for later)
- ListObjects pagination: continuation token is the last key seen (opaque to client)

**Test targets:**

- **E2E tests (`tests/e2e/test_objects.py`, 13 tests):**
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

- **Smoke test (partial):**
  - `list-objects (s3 ls)`, `list-objects-v2`, `copy-object`, `delete-objects` -- PASS

**Definition of done:**
- [x] CopyObject supports COPY and REPLACE metadata directives
- [x] DeleteObjects handles both quiet and verbose modes
- [x] ListObjectsV2 with delimiter correctly returns CommonPrefixes
- [x] ListObjectsV2 pagination works with MaxKeys and ContinuationToken
- [x] ListObjects v1 works with Marker
- [x] All smoke test list/copy/delete operations pass

---

### Stage 5b: Range, Conditional Requests & Object ACLs (~100k tokens)

**Goal:** Implement range requests, conditional requests (If-Match, If-None-Match, etc.), and object ACL operations.

**Inputs:** Stage 5a complete (list, copy, batch delete work).

**Spec references:**
- `specs/s3-object-operations.md` -- Sections 9-10: GetObjectAcl, PutObjectAcl
- `specs/s3-bucket-operations.md` -- ACL XML format (shared with object ACLs)
- `specs/s3-common-headers.md` -- Conditional request headers, Content-Range

**Implementation scope:**

1. **Range requests** (in GetObject handler)
   - Parse `Range` header: `bytes=start-end`, `bytes=start-`, `bytes=-suffix`
   - Single range only (S3 does not support multi-range)
   - Valid range: return 206 Partial Content with `Content-Range: bytes start-end/total`
   - Invalid range (start > object size): return 416 with `InvalidRange` error
   - Suffix range (`bytes=-N`): return last N bytes

2. **Conditional requests** (in GetObject and HeadObject handlers)
   - `If-Match`: return 200 if ETag matches, else 412 Precondition Failed
   - `If-None-Match`: return 304 Not Modified if ETag matches, else 200
   - `If-Modified-Since`: return 304 if not modified after date, else 200
   - `If-Unmodified-Since`: return 412 if modified after date, else 200
   - Priority: `If-Match` > `If-Unmodified-Since`; `If-None-Match` > `If-Modified-Since`

3. **GetObjectAcl handler** (`GET /{Bucket}/{Key+}?acl`)
   - Check object exists -> `NoSuchKey` (404)
   - Return `AccessControlPolicy` XML (same format as bucket ACL)

4. **PutObjectAcl handler** (`PUT /{Bucket}/{Key+}?acl`)
   - Check object exists -> `NoSuchKey` (404)
   - Three mutually exclusive modes (same as PutBucketAcl)
   - Store ACL in metadata
   - Response: 200 OK, no body

**Key decisions:**
- 304 Not Modified responses have no body

**Test targets:**

- **E2E tests (`tests/e2e/test_objects.py`, 6 tests):**
  - `TestGetObjectRange::test_range_request` -- bytes=0-4 returns 206
  - `TestGetObjectRange::test_range_request_suffix` -- bytes=-5
  - `TestGetObjectRange::test_invalid_range` -- 416
  - `TestConditionalRequests::test_if_match_success` -- 200
  - `TestConditionalRequests::test_if_match_failure` -- 412
  - `TestConditionalRequests::test_if_none_match_returns_304` -- 304

- **E2E tests (`tests/e2e/test_acl.py`, 4 tests):**
  - `TestObjectAcl::test_get_object_acl_default` -- Default FULL_CONTROL
  - `TestObjectAcl::test_put_object_acl_canned` -- Set public-read
  - `TestObjectAcl::test_put_object_with_canned_acl` -- ACL on PUT
  - `TestObjectAcl::test_get_acl_nonexistent_object` -- NoSuchKey

- **E2E tests (`tests/e2e/test_errors.py`):**
  - `TestErrorResponses::test_method_not_allowed` -- (pass-through, always passes)

- **Smoke test (remaining):**
  - `get-object-acl`, `get-bucket-acl` -- PASS

**Definition of done:**
- [x] All 32 tests in `test_objects.py` pass (cumulative with 5a)
- [x] All 4 tests in `test_acl.py` pass
- [x] Range requests return 206 with correct Content-Range header
- [x] Conditional requests return 304/412 as appropriate
- [x] All smoke test object operations pass

---

## Milestone 4: Authentication (Stage 6)

**Goal:** Full AWS Signature V4 authentication for both header-based and presigned URL requests. Invalid credentials are properly rejected.

---

### Stage 6: AWS Signature V4 (~100k tokens)

**Goal:** Implement SigV4 signature verification for header-based auth and presigned URL validation. After this stage, requests with invalid credentials are rejected, and presigned URLs work correctly.

**Inputs:** Stages 1-5 complete (all operations work without auth).

**Spec references:**
- `specs/s3-authentication.md` -- Complete SigV4 specification: canonical request, string-to-sign, signing key derivation, presigned URLs, URI encoding rules, edge cases
- `specs/s3-error-responses.md` -- Auth-related error codes
- `specs/s3-common-headers.md` -- Auth-related request headers

**Implementation scope:**

1. **Auth detection middleware/interceptor**
   - Check every incoming request for authentication method:
     - Query string contains `X-Amz-Algorithm` -> presigned URL auth
     - `Authorization` header starts with `AWS4-HMAC-SHA256` -> header-based SigV4
     - Both present -> reject with `InvalidArgument`
     - Neither present -> reject with `AccessDenied`
   - Health check endpoint (`/health`) is excluded from auth

2. **Header-based SigV4 verification**
   - Parse `Authorization` header:
     ```
     AWS4-HMAC-SHA256 Credential=AKID/20260222/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abcdef...
     ```
   - Extract: access_key_id, credential_date, region, service, signed_headers, provided_signature
   - Validate credential scope: region matches config, service is `s3`, terminator is `aws4_request`
   - Look up secret key from metadata store credentials table
     - Not found -> `InvalidAccessKeyId` (403)

3. **Canonical request construction**
   - `HTTPMethod`: uppercase
   - `CanonicalURI`: URI-encoded path (RFC 3986), `/` not encoded, S3 single-encoding (no double-encode)
   - `CanonicalQueryString`: all query params sorted by name (byte-order), each name and value URI-encoded, joined with `&`. Omit `X-Amz-Signature` for presigned.
   - `CanonicalHeaders`: headers in `SignedHeaders`, lowercased, trimmed, sorted, `name:value\n` format with trailing newline
   - `SignedHeaders`: semicolon-separated, lowercased, sorted
   - `HashedPayload`: value of `x-amz-content-sha256` header (or `UNSIGNED-PAYLOAD`)

4. **String-to-sign computation**
   ```
   AWS4-HMAC-SHA256\n
   YYYYMMDDTHHMMSSZ\n
   YYYYMMDD/region/s3/aws4_request\n
   SHA256(CanonicalRequest)
   ```

5. **Signing key derivation (HMAC chain)**
   ```
   DateKey    = HMAC-SHA256("AWS4" + SecretKey, YYYYMMDD)
   RegionKey  = HMAC-SHA256(DateKey, region)
   ServiceKey = HMAC-SHA256(RegionKey, "s3")
   SigningKey  = HMAC-SHA256(ServiceKey, "aws4_request")
   ```
   - Initial key: `"AWS4"` prepended to secret key as UTF-8 bytes
   - All intermediate values: raw binary (32 bytes), never hex-encoded between steps
   - Optional: cache signing key per day/region/service

6. **Signature verification**
   - Compute expected signature: `HexEncode(HMAC-SHA256(SigningKey, StringToSign))`
   - Compare with provided signature using **constant-time comparison**
   - Mismatch -> `SignatureDoesNotMatch` (403)

7. **Presigned URL validation**
   - Extract query parameters: `X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`, `X-Amz-SignedHeaders`, `X-Amz-Signature`
   - Validate algorithm = `AWS4-HMAC-SHA256`
   - Parse credential: access_key_id, date, region, service, terminator
   - Validate `X-Amz-Expires` in range [1, 604800]
   - Check expiration: `now <= parse(X-Amz-Date) + X-Amz-Expires`
   - Verify credential date matches `X-Amz-Date` date portion
   - Look up secret key
   - Reconstruct canonical request (exclude `X-Amz-Signature` from query string, `UNSIGNED-PAYLOAD` as HashedPayload)
   - Verify signature

8. **URI encoding function**
   - Characters NOT encoded: `A-Z`, `a-z`, `0-9`, `-`, `_`, `.`, `~`
   - All other characters: percent-encoded with uppercase hex (`%2F`, `%20`)
   - Spaces: `%20` (not `+`)
   - `/` in URI path: NOT encoded
   - `/` in query values: encoded as `%2F`

9. **Clock skew handling**
   - Header-based auth: ~15 minutes tolerance
   - Presigned: exact expiration check via `X-Amz-Expires`
   - Skew too large -> `RequestTimeTooSkewed` (403)

10. **Date matching**
    - Credential date (YYYYMMDD) must match `X-Amz-Date` date portion
    - Mismatch -> `SignatureDoesNotMatch`

**Key decisions:**
- `UNSIGNED-PAYLOAD` is accepted and common (most S3 clients use it)
- Chunked transfer signing (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`) is deferred (not required for Phase 1 E2E tests)
- Signing key caching is optional but recommended for performance
- Constant-time comparison is security-critical (prevents timing attacks)

**Test targets:**

- **E2E tests (`tests/e2e/test_presigned.py`, 4 tests):**
  - `TestPresignedGetUrl::test_presigned_get` -- Generate presigned GET, download via plain HTTP
  - `TestPresignedGetUrl::test_presigned_get_nonexistent_key` -- 404 via presigned URL
  - `TestPresignedPutUrl::test_presigned_put` -- Generate presigned PUT, upload via plain HTTP
  - `TestPresignedUrlExpiration::test_presigned_url_contains_expected_params` -- URL format check

- **E2E tests (`tests/e2e/test_errors.py`, auth tests):**
  - `TestAuthErrors::test_invalid_access_key` -- InvalidAccessKeyId or equivalent
  - `TestAuthErrors::test_signature_mismatch` -- SignatureDoesNotMatch

- **All previous E2E tests must still pass** (normal boto3 requests include SigV4)

- **External (optional):**
  - Ceph s3-tests auth tests should pass (basic SigV4 verification)

**Definition of done:**
- [x] All 4 tests in `test_presigned.py` pass
- [x] Both auth error tests in `test_errors.py` pass
- [x] All previous E2E tests still pass (SigV4 header auth works transparently)
- [x] Invalid access keys return `InvalidAccessKeyId` (403)
- [x] Wrong secret keys return `SignatureDoesNotMatch` (403)
- [x] Presigned GET URLs allow unauthenticated download
- [x] Presigned PUT URLs allow unauthenticated upload
- [x] Expired presigned URLs are rejected
- [x] Constant-time signature comparison used

---

## Milestone 5: Multipart Upload (Stages 7-8)

**Goal:** Full multipart upload support including create, upload parts, complete with assembly, abort, list uploads, and list parts. All multipart E2E tests pass.

---

### Stage 7: Multipart Upload - Core (~100k tokens)

**Goal:** Implement the core multipart upload lifecycle: create, upload parts, abort, list uploads, and list parts. Part data is stored but assembly (completion) is in the next stage.

**Inputs:** Stages 1-6 complete (all basic operations and auth working).

**Spec references:**
- `specs/s3-multipart-upload.md` -- Sections 1, 2, 5, 6, 7: Create, UploadPart, Abort, ListMultipartUploads, ListParts
- `specs/storage-backends.md` -- Multipart storage layout

**Implementation scope:**

1. **CreateMultipartUpload handler** (`POST /{Bucket}/{Key+}?uploads`)
   - Check bucket exists -> `NoSuchBucket` (404)
   - Generate unique upload ID (UUID or similar)
   - Extract headers: `Content-Type`, `x-amz-meta-*`, `x-amz-acl`, `Cache-Control`, `Content-Encoding`, `Content-Disposition`, `Content-Language`, `Expires`, `x-amz-storage-class`
   - Store upload metadata in metadata store
   - Response: 200 OK with `InitiateMultipartUploadResult` XML:
     ```xml
     <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
       <Bucket>bucket</Bucket>
       <Key>key</Key>
       <UploadId>upload-id</UploadId>
     </InitiateMultipartUploadResult>
     ```

2. **UploadPart handler** (`PUT /{Bucket}/{Key+}?partNumber=N&uploadId=ID`)
   - Parse query parameters: `partNumber` (1-10000), `uploadId`
   - Check upload exists -> `NoSuchUpload` (404)
   - Validate part number range -> `InvalidArgument` (400)
   - Store part data to storage backend at: `{root_dir}/.multipart/{upload_id}/{part_number}`
   - Compute MD5 ETag of part data
   - Register part in metadata store (upsert -- same part number overwrites)
   - Response: 200 OK with `ETag` header (quoted MD5 hex of part data)
   - Part size is NOT validated here (enforced at CompleteMultipartUpload time)

3. **AbortMultipartUpload handler** (`DELETE /{Bucket}/{Key+}?uploadId=ID`)
   - Check upload exists -> `NoSuchUpload` (404)
   - Delete all part files from storage: `{root_dir}/.multipart/{upload_id}/`
   - Delete upload and parts from metadata store
   - Response: **204 No Content**

4. **ListMultipartUploads handler** (`GET /{Bucket}?uploads`)
   - Parse query parameters: `prefix`, `delimiter`, `key-marker`, `upload-id-marker`, `max-uploads` (default 1000), `encoding-type`
   - Query metadata store
   - Build `ListMultipartUploadsResult` XML:
     ```xml
     <ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
       <Bucket>bucket</Bucket>
       <KeyMarker>...</KeyMarker><UploadIdMarker>...</UploadIdMarker>
       <MaxUploads>1000</MaxUploads><IsTruncated>false</IsTruncated>
       <Upload>
         <Key>key</Key><UploadId>id</UploadId>
         <Initiator><ID>...</ID><DisplayName>...</DisplayName></Initiator>
         <Owner><ID>...</ID><DisplayName>...</DisplayName></Owner>
         <StorageClass>STANDARD</StorageClass>
         <Initiated>2026-02-22T12:00:00.000Z</Initiated>
       </Upload>
     </ListMultipartUploadsResult>
     ```
   - Support `prefix` and `delimiter` filtering with `CommonPrefixes`
   - Pagination via `NextKeyMarker` and `NextUploadIdMarker`

5. **ListParts handler** (`GET /{Bucket}/{Key+}?uploadId=ID`)
   - Parse query parameters: `uploadId`, `max-parts` (default 1000), `part-number-marker`
   - Check upload exists -> `NoSuchUpload` (404)
   - Query metadata store for parts
   - Build `ListPartsResult` XML:
     ```xml
     <ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
       <Bucket>bucket</Bucket><Key>key</Key><UploadId>id</UploadId>
       <Initiator>...</Initiator><Owner>...</Owner>
       <StorageClass>STANDARD</StorageClass>
       <PartNumberMarker>0</PartNumberMarker>
       <MaxParts>1000</MaxParts><IsTruncated>false</IsTruncated>
       <Part>
         <PartNumber>1</PartNumber><LastModified>...</LastModified>
         <ETag>"..."</ETag><Size>5242880</Size>
       </Part>
     </ListPartsResult>
     ```

6. **Multipart file storage in local backend**
   - Create directory: `{root_dir}/.multipart/{upload_id}/`
   - Part files: `{root_dir}/.multipart/{upload_id}/{part_number}`
   - Atomic writes (temp file + rename)
   - Cleanup: delete entire upload directory on abort

**Key decisions:**
- Upload ID is a UUID (language-native UUID generation)
- Part data stored separately from final objects (in `.multipart/` subdirectory)
- Part size validation happens at completion time, not upload time (per S3 spec)
- Parts can be overwritten (same upload_id + part_number)

**Test targets:**

- **E2E tests (partial from `tests/e2e/test_multipart.py`, 7 tests):**
  - `TestMultipartUpload::test_upload_part_etag` -- Part ETag is MD5
  - `TestMultipartUpload::test_overwrite_part` -- Part number overwrite
  - `TestAbortMultipartUpload::test_abort_upload` -- 204 response
  - `TestAbortMultipartUpload::test_abort_nonexistent_upload` -- NoSuchUpload
  - `TestListMultipartUploads::test_list_uploads` -- Two uploads listed
  - `TestListMultipartUploads::test_list_uploads_with_prefix` -- Prefix filter
  - `TestListParts::test_list_parts` -- 3 parts listed with fields

- **E2E tests (`tests/e2e/test_multipart.py`, error tests):**
  - `TestMultipartUploadErrors::test_upload_to_nonexistent_upload_id` -- NoSuchUpload

**Definition of done:**
- [x] CreateMultipartUpload returns upload ID
- [x] UploadPart stores part data and returns MD5 ETag
- [x] Part overwrite works (same part number replaces previous)
- [x] AbortMultipartUpload cleans up parts and returns 204
- [x] ListMultipartUploads returns all in-progress uploads
- [x] ListParts returns all parts with required fields
- [x] 8 specified multipart tests pass

---

### Stage 8: Multipart Upload - Completion (~100k tokens)

**Goal:** Implement CompleteMultipartUpload with part assembly, composite ETag computation, part validation, and UploadPartCopy. All multipart tests pass.

**Inputs:** Stage 7 complete (core multipart operations work).

**Spec references:**
- `specs/s3-multipart-upload.md` -- Sections 3, 4: UploadPartCopy, CompleteMultipartUpload; ETag computation, constants and limits

**Implementation scope:**

1. **CompleteMultipartUpload handler** (`POST /{Bucket}/{Key+}?uploadId=ID`)
   - Parse XML request body:
     ```xml
     <CompleteMultipartUpload>
       <Part><PartNumber>1</PartNumber><ETag>"..."</ETag></Part>
       <Part><PartNumber>2</PartNumber><ETag>"..."</ETag></Part>
     </CompleteMultipartUpload>
     ```
   - Validate upload exists -> `NoSuchUpload` (404)
   - Validate parts:
     - Parts must be in ascending `PartNumber` order -> `InvalidPartOrder` (400)
     - Each ETag must match stored part ETag -> `InvalidPart` (400)
     - All parts except the last must be >= 5 MiB (5,242,880 bytes) -> `EntityTooSmall` (400)
   - Assemble final object:
     - Read part files in order: `{root_dir}/.multipart/{upload_id}/{part_number}`
     - Write concatenated data to final object path: `{root_dir}/{bucket}/{key}`
     - Use streaming/chunked approach to avoid holding entire object in memory
   - Compute composite ETag:
     ```
     MD5(concat(binary_MD5_part1, binary_MD5_part2, ..., binary_MD5_partN))
     ```
     Result: hex-encoded with `-N` suffix where N = number of parts
     Example: `"3858f62230ac3c915f300c664312c11f-9"`
   - Compute total size (sum of part sizes)
   - Write final object metadata to metadata store via `complete_multipart_upload`:
     - Insert object record with composite ETag and total size
     - Delete multipart_upload and multipart_parts records
     - Transaction-safe
   - Delete part files from `.multipart/{upload_id}/`
   - Response: 200 OK with `CompleteMultipartUploadResult` XML:
     ```xml
     <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
       <Location>http://host:port/bucket/key</Location>
       <Bucket>bucket</Bucket>
       <Key>key</Key>
       <ETag>"3858f62230ac3c915f300c664312c11f-2"</ETag>
     </CompleteMultipartUploadResult>
     ```

2. **Composite ETag computation function**
   - Input: ordered list of part ETags (MD5 hex strings)
   - For each part: convert hex ETag to 16-byte binary MD5
   - Concatenate all binary MD5 values
   - Compute MD5 of the concatenation
   - Result: `"` + hex(result) + `-` + str(num_parts) + `"`
   - This is NOT the MD5 of the assembled object data

3. **Part size validation**
   - Minimum: 5 MiB (5,242,880 bytes) for all parts except the last
   - Maximum: 5 GiB (5,368,709,120 bytes)
   - Last part: no minimum size (can be 0 bytes)
   - Maximum number of parts: 10,000
   - Validation happens at CompleteMultipartUpload time, not at UploadPart time

4. **UploadPartCopy handler** (`PUT /{Bucket}/{Key+}?partNumber=N&uploadId=ID` with `x-amz-copy-source`)
   - Parse `x-amz-copy-source`: `/{source-bucket}/{source-key}`
   - Optional `x-amz-copy-source-range`: `bytes=start-end`
   - Check upload exists -> `NoSuchUpload` (404)
   - Check source object exists -> `NoSuchKey` (404)
   - Copy source data (or range) to part storage
   - Register part in metadata store
   - Response: 200 OK with `CopyPartResult` XML:
     ```xml
     <CopyPartResult>
       <ETag>"..."</ETag>
       <LastModified>...</LastModified>
     </CopyPartResult>
     ```

5. **Cleanup on completion/abort**
   - After successful completion: delete `.multipart/{upload_id}/` directory and all part files
   - After abort: same cleanup
   - Handle partial failures gracefully (log warnings, continue cleanup)

**Key decisions:**
- Part assembly uses streaming: read each part file sequentially and write to output file
- Composite ETag uses raw binary MD5 concatenation (not hex strings)
- Object metadata from CreateMultipartUpload is transferred to the final object on completion

**Test targets:**

- **E2E tests (remaining from `tests/e2e/test_multipart.py`, 3 tests):**
  - `TestMultipartUpload::test_basic_multipart_upload` -- Full lifecycle: create, upload 2 parts, complete, verify content
  - `TestMultipartUploadErrors::test_complete_with_invalid_part_order` -- InvalidPartOrder
  - `TestMultipartUploadErrors::test_complete_with_wrong_etag` -- InvalidPart

- **All 11 tests in `test_multipart.py` should now pass**

- **Smoke test (multipart):**
  - `multipart upload (s3 cp)` -- 11MB file auto-multipart
  - `multipart download` -- Download and verify
  - `multipart content match` -- Content integrity check

**Definition of done:**
- [x] CompleteMultipartUpload assembles parts into final object
- [x] Composite ETag computed correctly (MD5 of MD5s + "-N")
- [x] Part size validation: non-last parts must be >= 5 MiB
- [x] InvalidPartOrder returned for non-ascending parts
- [x] InvalidPart returned for ETag mismatch
- [x] UploadPartCopy works with source range
- [x] All 11 tests in `test_multipart.py` pass
- [x] Multipart smoke tests pass (11MB file upload/download/verify)
- [x] Part files cleaned up after completion and abort

---

## Milestone 6: Integration & Compliance (Stages 9a-9b)

**Goal:** All BleepStore E2E tests pass (75/75). Smoke test passes. External test suites run clean for Phase 1 operations. Compliance issues fixed.

---

### Stage 9a: Core Integration Testing (~100k tokens)

**Goal:** Run the full BleepStore E2E suite and smoke test. Fix all failures related to bucket operations, basic/advanced object operations, and error responses to achieve 100% pass rate on the internal test suite.

**Inputs:** Stages 1-8 complete (all operations implemented).

**Spec references:**
- `specs/s3-error-responses.md` -- Error XML format correctness
- `specs/s3-common-headers.md` -- Header format correctness
- `specs/s3-bucket-operations.md` -- Bucket edge cases
- `specs/s3-object-operations.md` -- Object edge cases

**Implementation scope:**

1. **Run full BleepStore E2E suite and fix failures**
   ```bash
   python -m pytest tests/e2e/ -v --tb=long
   ```
   - Target: all 75 tests pass
   - Common issues to check and fix:
     - XML namespace correctness (`xmlns="http://s3.amazonaws.com/doc/2006-03-01/"` on success, none on error)
     - ETag quoting (must be `"d41d8cd98f..."` with literal double quotes)
     - Date format correctness (RFC 7231 for headers, ISO 8601 for XML)
     - Missing headers (`Date`, `x-amz-request-id`, `Content-Type`, `Accept-Ranges`)
     - Content-Type: `application/xml` (not `text/xml`)
     - Empty list results (no `<Contents>` element when 0 objects)
     - `Server: BleepStore` header
     - HEAD responses have no body (including error cases)
     - 204 No Content for DeleteBucket, DeleteObject, AbortMultipartUpload
     - 200 for all other success responses (not 201, not 202)

2. **Run smoke test and fix failures**
   ```bash
   BLEEPSTORE_ENDPOINT=http://localhost:9000 tests/smoke/smoke_test.sh
   ```
   - Target: 20/20 tests pass (4 bucket + 10 object + 3 multipart + 2 ACL + 2 error tests)

3. **Fix common compliance issues:**
   - **XML namespace**: Success responses use `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`, error responses have no namespace
   - **ETag quoting**: Always quoted with `"` in headers and XML
   - **Content-Type defaults**: `application/octet-stream` for objects, `application/xml` for XML responses
   - **Content-Length**: Always set for responses with a body; 0 for empty responses
   - **x-amz-request-id**: 16-character hex string on every response
   - **Date header**: RFC 1123 format on every response
   - **Accept-Ranges: bytes**: On GetObject and HeadObject responses
   - **Last-Modified**: RFC 7231 format in HTTP headers
   - **CreationDate**: ISO 8601 format in XML
   - **Empty elements**: `<Contents>` omitted (not empty) when no objects; `<CommonPrefixes>` omitted when none
   - **StorageClass**: `STANDARD` default in list responses
   - **Owner**: Included in list responses with correct ID and DisplayName

4. **Error handling edge cases:**
   - Non-existent bucket in all operations returns `NoSuchBucket` (not 500)
   - Malformed XML in body returns `MalformedXML` (400)
   - Missing required query parameters handled gracefully
   - Very long keys (>1024 bytes) return `KeyTooLongError`
   - Unicode keys handled correctly
   - URL-encoded keys decoded properly

**Key decisions:**
- This stage is primarily debugging and fixing -- minimal new code
- Focus on internal tests first (E2E + smoke) before external suites in 9b

**Test targets:**

- **BleepStore E2E: 75/75 tests pass**
  - `test_buckets.py`: 16/16
  - `test_objects.py`: 32/32
  - `test_multipart.py`: 11/11
  - `test_presigned.py`: 4/4
  - `test_acl.py`: 4/4
  - `test_errors.py`: 8/8

- **Smoke test: 20/20 pass**

**Definition of done:**
- [x] All 75 BleepStore E2E tests pass
- [x] Smoke test passes (20/20)
- [x] `aws s3 cp`, `aws s3 ls`, `aws s3 sync` work out of the box
- [x] `aws s3api` commands for all Phase 1 operations succeed
- [x] No 500 Internal Server Error for valid requests
- [x] XML responses are well-formed and namespace-correct
- [x] All headers match S3 format expectations

---

### Stage 9b: External Test Suites & Compliance (~100k tokens)

**Goal:** Run external S3 conformance test suites (Ceph s3-tests, MinIO Mint, Snowflake s3compat) and fix compliance issues found.

**Inputs:** Stage 9a complete (all 75 internal E2E tests + smoke test pass).

**Spec references:**
- All specs -- for fixing compliance issues discovered by external suites

**Implementation scope:**

1. **Run Ceph s3-tests (filtered to Phase 1 operations)**
   - Filter to bucket CRUD, object CRUD, multipart, ACL, presigned URL tests
   - Skip versioning, lifecycle, replication, CORS, website, notification tests
   - Fix failures related to:
     - XML response formatting
     - Header values and formats
     - Edge cases in S3 behavior (e.g., us-east-1 LocationConstraint quirk)
     - Error code mapping

2. **Run MinIO Mint core mode**
   - Uses aws-cli, mc (MinIO client), and various SDK tests
   - Fix failures related to SDK-specific expectations
   - Common issues: Content-MD5 validation, chunked transfer encoding

3. **Run Snowflake s3compat (9 core operations)**
   - Quick conformance check for the 9 most important operations
   - Fix any remaining issues

4. **Fix remaining compliance issues found by external suites**
   - Edge cases in bucket naming validation
   - Chunked transfer encoding handling
   - Content-MD5 verification
   - Multi-SDK compatibility (Python boto3, AWS CLI, Go SDK, Java SDK)

**Key decisions:**
- External test suite failures are triaged: fix what applies to Phase 1, defer the rest
- Some external tests may require operations outside Phase 1 scope (versioning, etc.) -- these are expected failures

**Test targets:**

- **External suites (targets, not blockers):**
  - Ceph s3-tests: >80% of Phase 1-applicable tests pass
  - MinIO Mint: aws-cli tests pass
  - Snowflake s3compat: 9/9 pass

- **BleepStore E2E: 75/75 still pass** (no regressions from compliance fixes)

**Definition of done:**
- [x] Ceph s3-tests Phase 1 tests mostly pass (>80%)
- [x] Snowflake s3compat 9/9 pass
- [x] MinIO Mint aws-cli tests pass
- [x] All 75 BleepStore E2E tests still pass (no regressions)
- [x] Smoke test still passes (20/20)

---

## Milestone 7: Cloud Storage Backends (Stages 10-11b)

**Goal:** BleepStore can proxy to AWS S3, GCP Cloud Storage, and Azure Blob Storage. The same E2E tests pass against each backend.

---

### Stage 10: AWS S3 Gateway Backend (~100k tokens)

**Goal:** Implement the AWS S3 storage backend that proxies all data operations to a real AWS S3 bucket. BleepStore handles auth, metadata, and routing; AWS S3 handles data storage.

**Inputs:** Stage 9 complete (full local backend working and tested).

**Spec references:**
- `specs/storage-backends.md` -- AWS S3 backend: configuration, mapping, multipart passthrough, error mapping

**Implementation scope:**

1. **AWS S3 backend implementation** (implements `StorageBackend` interface)
   - Use official AWS SDK for the target language (boto3, aws-sdk-go-v2, aws-sdk-rust, direct HTTP for Zig)
   - Configuration: `storage.aws.bucket`, `storage.aws.region`, `storage.aws.prefix`
   - Credential chain: environment variables, config file (`~/.aws/credentials`), IAM role

2. **Operation mapping:**
   - `put_object` -> AWS `PutObject` to backing bucket with key `{prefix}{bleepstore_bucket}/{key}`
   - `get_object` -> AWS `GetObject` with optional range
   - `delete_object` -> AWS `DeleteObject`
   - `head_object` -> AWS `HeadObject`
   - `copy_object` -> AWS `CopyObject` (server-side copy within backing bucket)
   - `delete_objects` -> AWS `DeleteObjects` (batch)
   - `create_bucket` -> Create prefix marker (or no-op if using prefix-per-bucket)
   - `delete_bucket` -> Delete prefix marker

3. **Multipart passthrough:**
   - `CreateMultipartUpload` -> AWS `CreateMultipartUpload`
   - `UploadPart` -> AWS `UploadPart`
   - `CompleteMultipartUpload` -> AWS `CompleteMultipartUpload`
   - `AbortMultipartUpload` -> AWS `AbortMultipartUpload`
   - ETags from AWS used as-is (no need to recompute)
   - Upload IDs from AWS used as-is

4. **Error mapping:**
   - AWS `NoSuchKey` / 404 -> `NoSuchKey`
   - AWS `AccessDenied` / 403 -> `AccessDenied` (log details)
   - AWS `SlowDown` / 503 -> `SlowDown`
   - AWS `InternalError` / 500 -> `InternalError`
   - Network errors -> `InternalError`

5. **Backend selection at startup:**
   - Factory pattern: read `storage.backend` from config
   - `"local"` -> LocalStorageBackend (existing)
   - `"aws"` -> AwsStorageBackend (new)
   - Backend interface ensures rest of application is unchanged

**Key decisions:**
- Key mapping: `{prefix}{bleepstore_bucket}/{key}` -- all BleepStore data in a single AWS bucket with prefix isolation
- Metadata still stored in local SQLite (AWS S3 is data-only backend)
- For multipart, use AWS-native multipart (passthrough) rather than local assembly

**Test targets:**
- **E2E suite with AWS backend:**
  ```bash
  BLEEPSTORE_BACKEND=aws python -m pytest tests/e2e/ -v
  ```
  - All 75 tests should pass against AWS backend
  - Requires AWS credentials and a backing bucket configured

**Definition of done:**
- [x] AWS S3 backend implements full `StorageBackend` interface
- [x] Backend selection via config works
- [x] All 75 E2E tests pass with `BLEEPSTORE_BACKEND=aws`
- [x] Multipart upload works end-to-end through AWS
- [x] Error mapping covers common AWS error codes

---

### Stage 11a: GCP Cloud Storage Backend (~100k tokens)

**Goal:** Implement GCP Cloud Storage backend. Two cloud backends (AWS + GCP) pass the E2E suite.

**Inputs:** Stage 10 complete (AWS backend pattern established).

**Spec references:**
- `specs/storage-backends.md` -- GCP backend specification, key differences from S3

**Implementation scope:**

1. **GCP Cloud Storage backend**
   - Use GCP client library (google-cloud-storage, cloud.google.com/go/storage, google-cloud-rust, or direct HTTP for Zig)
   - Configuration: `storage.gcp.bucket`, `storage.gcp.project`, `storage.gcp.prefix`
   - Credential chain: Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS, gcloud auth, metadata server)
   - Key mapping: same pattern as AWS (`{prefix}{bleepstore_bucket}/{key}`)
   - ETag handling: GCS returns `md5Hash` (base64) -- convert to hex for S3 compatibility
   - Multipart via GCS compose:
     - Upload parts as temporary objects
     - Use GCS `compose` to assemble (max 32 sources per call, chain for >32 parts)
     - Delete temporary objects after composition
   - Error mapping: GCS errors -> S3 error codes

2. **Backend-agnostic error mapping utility**
   - Common error mapping function/table shared across all cloud backends
   - Map HTTP status codes and provider-specific error codes to S3 error types
   - Logging: log original provider error for debugging, return S3 error to client

**Key decisions:**
- GCS compose has 32-source limit: implement recursive composition for >32 parts
- All backends use same key mapping pattern for consistency
- ETag computation may need to be done locally if the provider's ETag format differs from S3

**Test targets:**
- **E2E suite with GCP backend:**
  ```bash
  BLEEPSTORE_BACKEND=gcp python -m pytest tests/e2e/ -v
  ```
- All 75 tests should pass against GCP backend

**Definition of done:**
- [x] GCP backend implements full `StorageBackend` interface
- [x] All 75 E2E tests pass with `BLEEPSTORE_BACKEND=gcp`
- [x] GCS compose-based multipart works for >32 parts
- [x] Backend error mapping utility covers GCS error codes

---

### Stage 11b: Azure Blob Storage Backend (~100k tokens)

**Goal:** Implement Azure Blob Storage backend. All three cloud backends (AWS, GCP, Azure) pass the E2E suite.

**Inputs:** Stage 11a complete (GCP backend working, error mapping utility exists).

**Spec references:**
- `specs/storage-backends.md` -- Azure backend specification, key differences from S3

**Implementation scope:**

1. **Azure Blob Storage backend**
   - Use Azure SDK (azure-storage-blob, azure-sdk-for-go, azure_storage, or direct HTTP for Zig)
   - Configuration: `storage.azure.container`, `storage.azure.account`, `storage.azure.prefix`
   - Credential chain: DefaultAzureCredential (env vars, managed identity, Azure CLI)
   - Key mapping: `{prefix}{bleepstore_bucket}/{key}` within single Azure container
   - Multipart via Azure block blobs:
     - `UploadPart` -> `Put Block` with block ID derived from part number
     - Block IDs must be same length, base64-encoded
     - `CompleteMultipartUpload` -> `Put Block List`
     - `AbortMultipartUpload` -> no-op (uncommitted blocks auto-expire in 7 days)
   - ETag handling: Azure ETags may differ from MD5 -- compute MD5 ourselves
   - Error mapping: Azure errors -> S3 error codes (using shared error mapping utility from 11a)

**Key decisions:**
- Azure block IDs: derive from part number with fixed-width padding (e.g., base64 of 5-digit zero-padded number)
- All backends use same key mapping pattern for consistency

**Test targets:**
- **E2E suite with Azure backend:**
  ```bash
  BLEEPSTORE_BACKEND=azure python -m pytest tests/e2e/ -v
  ```
- All 75 tests should pass against Azure backend

**Definition of done:**
- [x] Azure backend implements full `StorageBackend` interface
- [x] All 75 E2E tests pass with `BLEEPSTORE_BACKEND=azure`
- [x] Azure block blob-based multipart works
- [x] Backend error mapping covers Azure error codes

---

## Milestone 8: Cluster Mode (Stages 12a-14)

**Goal:** Multi-node BleepStore cluster with Raft-replicated metadata. Metadata writes go through Raft consensus. Reads served locally for eventual consistency. Admin API for cluster management.

---

### Stage 12a: Raft State Machine & Storage (~100k tokens)

**Goal:** Implement the core Raft state machine, log entry types, and persistent storage. The state machine handles state transitions and log management in isolation (no networking yet).

**Inputs:** Stage 9b complete (full single-node implementation working).

**Spec references:**
- `specs/clustering.md` -- Raft state machine, log entries, persistence requirements

**Implementation scope:**

1. **Raft node state machine**
   - Three states: `Follower`, `Candidate`, `Leader`
   - State transitions per Raft protocol rules
   - Persistent state: `currentTerm`, `votedFor`, `log[]`
   - Volatile state: `commitIndex`, `lastApplied`
   - Leader volatile state: `nextIndex[]`, `matchIndex[]`

2. **Log entry types**
   - Define the Raft log entry enum (per `specs/clustering.md`):
     ```
     CreateBucket, DeleteBucket, PutObjectMeta, DeleteObjectMeta,
     DeleteObjectsMeta, PutBucketAcl, PutObjectAcl,
     CreateMultipartUpload, RegisterPart, CompleteMultipartUpload,
     AbortMultipartUpload
     ```
   - Log entry: `{term, index, type, data}`
   - Serialization: JSON or binary encoding

3. **Persistent storage for Raft state**
   - Current term and voted_for: persisted to disk (SQLite table or flat file)
   - Log entries: persisted (SQLite table or append-only file)
   - Must be fsync'd before responding to RPCs
   - Append, truncate, read range, get last index/term operations

4. **Leader election logic** (state machine only, no networking)
   - Election timeout: configurable (default 1000ms), randomized (e.g., 1000-2000ms)
   - Follower -> Candidate on timeout: increment term, vote for self
   - `RequestVote` handler: validate term, check log up-to-dateness, grant/deny vote
   - Vote granting rules: only vote once per term, candidate's log must be at least as up-to-date
   - State transition rules: Candidate -> Leader on majority, Candidate -> Follower on higher term

5. **Log replication logic** (state machine only, no networking)
   - `AppendEntries` handler: validate prev_log_index/term match, append new entries, update commit index
   - On mismatch: reject (leader will decrement nextIndex and retry)
   - On success: update matchIndex, advance commitIndex when majority have entry
   - Heartbeat = empty AppendEntries (heartbeat_interval_ms default 150ms)

**Key decisions:**
- Use library if available and mature (hashicorp/raft for Go, openraft for Rust). Custom implementation for Python and Zig.
- Log entry serialization: JSON for simplicity, binary for performance (language-specific choice)
- State machine is fully testable without networking (accept/produce message structs)

**Test targets:**
- **Unit tests:**
  - State machine transitions: Follower -> Candidate -> Leader
  - Vote granting: correct term/log checks
  - Log append: entries persisted correctly
  - Log truncation on conflict
  - Term monotonicity: reject messages from old terms
  - Persistent state survives restart (term, votedFor, log)

**Definition of done:**
- [x] Raft state machine correctly transitions between Follower/Candidate/Leader
- [x] Log entry types defined and serializable
- [x] Persistent storage for term, votedFor, log entries
- [x] RequestVote and AppendEntries handlers work (in-process, no networking)
- [x] Unit tests cover state transitions, vote granting, log replication logic

---

### Stage 12b: Raft Networking & Elections (~100k tokens)

**Goal:** Add HTTP-based RPC transport to the Raft state machine. Leader election and log replication work across 3 nodes over the network.

**Inputs:** Stage 12a complete (Raft state machine and storage working in-process).

**Spec references:**
- `specs/clustering.md` -- RPC formats, network transport, failure handling

**Implementation scope:**

1. **Network transport**
   - HTTP-based RPC (or gRPC if the language ecosystem supports it well)
   - Endpoints:
     - `POST /raft/append_entries` -- AppendEntries RPC
     - `POST /raft/request_vote` -- RequestVote RPC
     - `POST /raft/install_snapshot` -- InstallSnapshot RPC (stub for now)
   - JSON or binary serialization for RPC messages
   - Bind address from config: `cluster.bind_addr`

2. **RPC client**
   - Send RequestVote to all peers in parallel
   - Send AppendEntries to each follower (with retries on network failure)
   - Timeout handling for unresponsive peers
   - Connection pooling (optional, language-dependent)

3. **Election driver**
   - Timer-based election timeout (reset on heartbeat received)
   - On timeout: trigger candidate transition, broadcast RequestVote
   - Collect votes, transition to Leader on majority
   - On receiving AppendEntries from valid leader: revert to Follower

4. **Heartbeat driver**
   - Leader sends empty AppendEntries as heartbeats at `heartbeat_interval_ms`
   - Heartbeats reset follower election timeouts
   - Heartbeat interval < election timeout (by ~10x)

5. **Multi-node integration**
   - Configuration: `cluster.peers` list of `host:port` addresses
   - Node startup: begin as Follower, start election timer
   - Peer discovery from config (static membership for now)

**Key decisions:**
- HTTP-based transport is simpler and more portable across languages
- Static peer membership (dynamic join/leave deferred to Stage 13)

**Test targets:**
- **Integration tests (3 nodes):**
  - Three-node election: one leader elected within timeout
  - Log replication: leader's entries replicated to followers
  - Heartbeats prevent election timeout
  - Leader failure triggers new election
  - Split vote resolves via randomized timeouts
  - Log consistency: follower with missing entries catches up

**Definition of done:**
- [x] Leader election works with 3 nodes over HTTP
- [x] Log replication works over HTTP (entries committed to majority)
- [x] Heartbeats prevent unnecessary elections
- [x] RPCs work reliably over HTTP with timeout handling
- [x] Integration tests cover multi-node Raft scenarios

---

### Stage 13a: Raft-Metadata Wiring (~100k tokens)

**Goal:** Wire the Raft consensus layer to the metadata store. Metadata writes go through the Raft log. Reads served from local SQLite replica. Write forwarding from followers to leader.

**Inputs:** Stage 12b complete (Raft consensus working over network).

**Spec references:**
- `specs/clustering.md` -- Write flow, read flow, transition from embedded

**Implementation scope:**

1. **Wire metadata writes through Raft**
   - When `metadata.engine = "raft"`:
     - All metadata write operations (create_bucket, put_object_meta, delete_object_meta, etc.) submit a log entry to Raft instead of writing directly to SQLite
     - Leader appends entry to log, replicates to quorum, then applies to local SQLite
     - Followers apply entries to their local SQLite as they receive them
   - When `metadata.engine = "sqlite"`:
     - Direct SQLite writes (existing behavior, unchanged)

2. **SQLite state machine apply**
   - Each committed Raft log entry is applied to the local SQLite database
   - Apply function: deserialize log entry -> execute corresponding SQL statements
   - Must be deterministic (same entries produce same state on every node)
   - Apply happens in order of log index

3. **Read path**
   - Reads always served from local SQLite replica (any node)
   - No Raft involvement for reads (eventual consistency)
   - Follower reads may be slightly stale

4. **Write forwarding**
   - Follower receives write request -> forwards to leader (transparent proxy)
   - Client does not need to know which node is leader
   - Leader address tracked via Raft protocol (heartbeat responses)
   - If leader unknown: return `ServiceUnavailable` (503) and client retries

**Key decisions:**
- All metadata writes are synchronous through Raft (write is acknowledged only after quorum commit)
- Reads are local-only (eventual consistency, no read-index optimization)
- Write forwarding is transparent (not redirect-based)

**Test targets:**
- **Integration tests (3-node cluster):**
  - Write on leader, read on follower (eventually consistent)
  - Follower forwards writes to leader transparently
  - Leader failure -> new leader -> writes continue
  - Node restart -> catches up from Raft log

**Definition of done:**
- [x] Metadata writes go through Raft consensus
- [x] Reads served from local SQLite on any node
- [x] Write forwarding from follower to leader works transparently
- [x] Leader failover maintains metadata consistency

---

### Stage 13b: Snapshots & Node Management (~100k tokens)

**Goal:** Implement log compaction via snapshots, InstallSnapshot RPC, and dynamic node join/leave.

**Inputs:** Stage 13a complete (Raft-metadata wiring working).

**Spec references:**
- `specs/clustering.md` -- Snapshots, InstallSnapshot RPC, membership changes

**Implementation scope:**

1. **Log compaction / snapshotting**
   - Snapshot = copy of SQLite database file
   - Triggered every `snapshot_interval` committed entries (configurable, default 10000)
   - After snapshot: log entries before snapshot index can be discarded
   - Snapshot stored in `cluster.data_dir`

2. **InstallSnapshot RPC**
   - Leader sends full snapshot to followers that are too far behind
   - Follower replaces local SQLite with snapshot, updates Raft state
   - Used for: new nodes joining, nodes that were offline for too long
   - Chunked transfer for large snapshots

3. **Node join/leave**
   - Join: new node contacts existing leader, leader adds to configuration
   - Leave: leader removes node from configuration
   - Configuration changes go through Raft log (joint consensus or single-step)

4. **Snapshot-based recovery**
   - Node that was offline too long: leader detects gap, sends snapshot
   - New node joining: receives full snapshot before participating in consensus

**Key decisions:**
- Snapshot is full SQLite database copy (simple, not incremental)
- Snapshot transfer via HTTP (chunked for large DBs)
- Single-step configuration changes (not joint consensus) for simplicity

**Test targets:**
- **Integration tests:**
  - Snapshot created after configured number of entries
  - New node joins via snapshot transfer
  - Node offline for extended period catches up via snapshot
  - Log entries before snapshot index are discarded
  - Node leave: removed from configuration, cluster continues

**Definition of done:**
- [x] Log compaction/snapshotting works
- [x] InstallSnapshot RPC transfers full database to lagging nodes
- [x] New node can join and sync via snapshot
- [x] Node leave removes from cluster configuration
- [x] Snapshot-based recovery for nodes that missed too many entries

---

### Stage 14: Cluster Operations & Admin API (~100k tokens)

**Goal:** Admin API for cluster management, health checking, and multi-node E2E testing.

**Inputs:** Stage 13 complete (Raft integrated with metadata store).

**Spec references:**
- `specs/clustering.md` -- Admin API endpoints, cluster status response
- `schemas/admin-api.openapi.yaml` -- Admin API schema

**Implementation scope:**

1. **Admin API endpoints** (on separate port: `server.admin_port`, default 9001)
   - `GET /admin/cluster/status` -- Cluster health: node state, leader info, term, commit index
   - `GET /admin/cluster/nodes` -- List all nodes with ID, address, state, last_contact
   - `POST /admin/cluster/nodes` -- Add a new node (body: `{id, addr}`)
   - `DELETE /admin/cluster/nodes/{id}` -- Remove a node
   - `GET /admin/cluster/raft/stats` -- Raft protocol statistics (log size, snapshot info, etc.)
   - `POST /admin/cluster/raft/snapshot` -- Trigger manual snapshot
   - Auth: bearer token from config (`server.admin_token`)

2. **Health checking**
   - `GET /health` -- Returns node health status
   - In cluster mode: include cluster state (leader/follower, leader address)
   - Health check does not require authentication

3. **Leader forwarding headers**
   - When forwarding to leader: add `X-Forwarded-For` and `X-BleepStore-Forwarded-By` headers
   - Response includes `X-BleepStore-Leader` header with leader node ID

4. **Multi-node E2E testing**
   - Script/config to start 3 BleepStore nodes locally (ports 9000, 9002, 9004; Raft ports 9001, 9003, 9005)
   - Run full E2E suite against any node
   - Verify writes on one node are eventually visible on other nodes
   - Test leader failover (kill leader, verify new leader elected, operations continue)

5. **Failure scenario testing**
   - Leader failure: kill leader process, verify election completes, operations resume
   - Follower failure: kill one follower, verify cluster still operates (quorum maintained)
   - Network partition simulation: block Raft port between nodes, verify partition behavior

**Key decisions:**
- Admin API on separate port for security isolation
- Bearer token auth for admin API (simple but effective)
- Multi-node testing uses local processes (not containers) for simplicity

**Test targets:**
- **Admin API tests:**
  - `GET /admin/cluster/status` returns valid JSON with node state and leader info
  - `GET /admin/cluster/nodes` lists all configured nodes
  - `POST /admin/cluster/raft/snapshot` triggers snapshot without error

- **Multi-node E2E:**
  - Start 3-node cluster, run full E2E suite against node 1
  - Write on node 1, read on node 2 (verify eventual consistency)
  - Kill leader, verify new leader elected, run E2E suite again

- **Failure scenarios:**
  - Leader kill -> election within 2x election_timeout
  - Follower kill -> writes still succeed (quorum)
  - Restart killed node -> catches up and serves reads

**Definition of done:**
- [x] Admin API endpoints work with bearer token auth
- [x] Cluster status correctly reports node states and leader
- [x] Multi-node E2E tests pass
- [x] Leader failover works (new leader elected, operations resume)
- [x] Follower failure does not disrupt writes (quorum maintained)
- [x] Node restart and catch-up work correctly

---

## Milestone 9: Performance & Hardening (Stage 15)

**Goal:** Production-ready performance and operational characteristics. Startup < 1 second, memory < 50MB for embedded mode. Performance within 2x of MinIO.

---

### Stage 15: Performance Optimization & Production Readiness (~100k tokens)

**Goal:** Optimize hot paths, improve resource usage, and harden for production use. Run benchmarks and compare with MinIO.

**Inputs:** All previous stages complete.

**Spec references:**
- `OBJECTIVE.MD` -- Success criteria: startup < 1s, memory < 50MB, performance within 2x of MinIO

**Implementation scope:**

1. **Run MinIO Warp benchmarks**
   ```bash
   warp mixed --host=localhost:9000 --access-key=bleepstore --secret-key=bleepstore-secret --duration=60s
   ```
   - Baseline: capture throughput and latency numbers
   - Compare with MinIO on equivalent hardware
   - Target: within 2x of MinIO throughput

2. **Run BleepStore performance tests**
   ```bash
   python -m pytest tests/performance/ -v
   ```
   - `bench_throughput.py`: objects/second for 1KB, 1MB, 100MB objects
   - `bench_latency.py`: p50, p95, p99 for GET/PUT/DELETE/LIST
   - `bench_multipart.py`: upload speed for large files

3. **Optimize hot paths:**
   - **Streaming I/O**: Ensure GetObject and PutObject stream data without buffering entire object in memory
   - **Memory allocation**: Reduce per-request allocations, reuse buffers where possible
   - **ETag computation**: Stream MD5 computation during I/O (don't read data twice)
   - **SQL query optimization**: Use prepared statements, avoid N+1 queries in list operations
   - **Connection handling**: HTTP keep-alive, connection pooling
   - **SigV4**: Cache signing keys (one per day/region/service)
   - **XML generation**: Use efficient XML builder (not string concatenation)

4. **Connection handling and timeouts:**
   - Request read timeout: configurable (default 60s)
   - Response write timeout: configurable (default 60s)
   - Idle connection timeout: configurable (default 120s)
   - Max concurrent connections: configurable (default 1000)

5. **Crash-only shutdown (SIGTERM optimization):**
   - On SIGINT/SIGTERM: stop accepting new connections
   - Wait for in-flight requests to complete (with timeout, e.g., 30s)
   - Exit (do NOT flush, finalize, or clean up — crash-only: startup handles recovery)
   - Must also be safe to `kill -9` at any point with no data corruption

6. **Startup time optimization:**
   - Target: < 1 second for embedded mode
   - Lazy-initialize SQLite schema (check once, skip if exists)
   - Minimize dependency loading at startup
   - Profile startup and eliminate bottlenecks

7. **Memory footprint optimization:**
   - Target: < 50MB RSS for embedded mode with no load
   - Profile memory usage, identify leaks
   - Streaming responses prevent buffering large objects
   - SQLite memory: WAL mode, limited page cache

8. **Logging and observability:**
   - Structured logging (JSON or key=value format)
   - Log levels: ERROR, WARN, INFO, DEBUG
   - Per-request logging: method, path, status, duration, request_id
   - Configurable log level
   - Metrics endpoint (optional): request count, latency histogram, error rate

9. **Error handling hardening:**
   - No panics/crashes on malformed requests
   - Graceful handling of disk full, SQLite busy, network errors
   - Proper cleanup on partial failures
   - Request body size limits (5TB max object, configurable)

**Key decisions:**
- Performance targets are for single-node embedded mode
- Streaming is the most impactful optimization (prevents memory issues with large objects)
- Signing key caching eliminates 4 HMAC operations per request (most requests)
- Prepared SQL statements eliminate query parsing overhead

**Test targets:**
- **Performance benchmarks:**
  - Throughput: > 1000 objects/second for 1KB objects (single node)
  - Latency: p99 < 50ms for GET/PUT of 1KB objects
  - Large objects: > 100 MB/s throughput for GET/PUT of 100MB objects
  - Memory: < 50MB RSS idle, < 200MB under load

- **Warp comparison:**
  - BleepStore throughput > 50% of MinIO for equivalent workload

- **Operational:**
  - Server starts in < 1 second
  - Graceful shutdown completes within 30 seconds
  - No crashes after 1 hour of Warp continuous load
  - No memory leaks (RSS stable after warmup)

**Definition of done:**
- [x] MinIO Warp benchmarks run and results documented
- [x] BleepStore performance tests pass with acceptable numbers
- [x] Streaming I/O for GET and PUT (no full buffering)
- [x] Startup < 1 second for embedded mode
- [x] Memory < 50MB idle for embedded mode
- [x] Graceful shutdown works
- [x] Structured logging with configurable level
- [x] No crashes under load
- [x] Performance within 2x of MinIO on equivalent hardware

---

## Milestone 10: Persistent Event Queues (Stages 16a-16c)

**Goal:** Optional pluggable queue system for event propagation and write-ahead consistency. Supports Redis, RabbitMQ/ActiveMQ, and Kafka with three consistency modes.

---

### Stage 16a: Queue Interface & Redis Backend (~100k tokens)

**Goal:** Define the QueueBackend interface, event types/envelope, and implement the Redis Streams backend with write-through mode.

**Inputs:** Stages 1-9 complete (full single-node implementation working). Can be done in parallel with Stages 10-15.

**Spec references:**
- `specs/event-queues.md` -- Queue architecture, event types, backend interface, Redis specifics

**Implementation scope:**

1. **Queue backend interface/trait/protocol**
   - `connect(config)`, `close()`, `health_check() -> bool`
   - `publish(event)`, `publish_batch(events)`
   - `subscribe(event_types, handler)`, `acknowledge(event_id)`
   - `enqueue_task(task) -> task_id`, `dequeue_task() -> WriteTask | None`
   - `complete_task(task_id)`, `fail_task(task_id, error)`
   - `retry_failed_tasks() -> int`

2. **Event types and envelope**
   - Define all event types: `bucket.created`, `bucket.deleted`, `object.created`, `object.deleted`, `objects.deleted`, `object.acl.updated`, `bucket.acl.updated`, `multipart.created`, `multipart.completed`, `multipart.aborted`, `part.uploaded`
   - Event envelope: `id` (ULID/UUID), `type`, `timestamp`, `source`, `request_id`, `data`
   - Idempotent consumers using event ID

3. **Redis backend**
   - Redis Streams for ordered, persistent event log with consumer groups
   - `XADD`, `XREADGROUP`, `XACK` for publish/subscribe/acknowledge
   - Dead letter stream for failed messages after max retries
   - Configuration: `queue.redis.url`, `queue.redis.stream_prefix`, `queue.redis.consumer_group`

4. **Write-through mode** (default when queue enabled)
   - Normal direct write path (storage + metadata)
   - After commit, publish event to queue (fire-and-forget)
   - Queue failure does not block the write
   - Used for: replication, webhooks, audit logging, cache invalidation

5. **Configuration integration**
   - `queue.enabled` (default: false), `queue.backend`, `queue.consistency`
   - Redis-specific configuration section
   - Backend selection via factory pattern (same as storage backends)

6. **Startup integration (crash-only)**
   - On startup: reconnect to queue, reprocess any pending/unacknowledged tasks
   - Health check includes queue connectivity status

**Key decisions:**
- Queue is entirely optional — disabled by default, BleepStore works fine without it
- write-through is the default consistency mode (safest, least disruptive)
- All queue operations are idempotent (event ID as idempotency key)
- Follows crash-only principles: no in-memory buffering, startup reprocesses pending tasks

**Test targets:**
- **Unit tests:**
  - Event serialization/deserialization round-trip
  - Redis backend: publish, subscribe, acknowledge, dead letter
  - Write-through mode: event published after successful write

- **Integration tests:**
  - Start BleepStore with Redis queue, run E2E suite — all 75 tests pass
  - Verify events published for each write operation
  - Queue unavailable at startup: BleepStore starts in degraded mode (logs warning)

**Definition of done:**
- [x] QueueBackend interface defined
- [x] Redis backend implemented (publish, subscribe, acknowledge, dead letter)
- [x] Event types and envelope defined
- [x] Write-through mode works: events published after successful writes
- [x] All 75 E2E tests pass with Redis queue enabled (write-through mode)
- [x] Configuration section for queue settings
- [x] Health check reports queue status

---

### Stage 16b: RabbitMQ Backend (~100k tokens)

**Goal:** Implement the RabbitMQ/AMQP backend using the QueueBackend interface established in 16a.

**Inputs:** Stage 16a complete (queue interface and Redis backend working).

**Spec references:**
- `specs/event-queues.md` -- RabbitMQ/AMQP specifics, dead letter exchange

**Implementation scope:**

1. **RabbitMQ backend**
   - Topic exchange for event routing by type
   - Durable queues with manual ack
   - Dead letter exchange for failed messages
   - Compatible with ActiveMQ via AMQP 0-9-1
   - Configuration: `queue.rabbitmq.url`, `queue.rabbitmq.exchange`, `queue.rabbitmq.queue_prefix`

2. **AMQP connection management**
   - Connection and channel lifecycle
   - Automatic reconnection on connection loss
   - Queue and exchange declaration (idempotent)

3. **Event routing**
   - Routing keys based on event type (e.g., `bucket.created`, `object.deleted`)
   - Subscribers bind to specific routing keys or patterns
   - Dead letter routing for failed messages after max retries

**Key decisions:**
- AMQP 0-9-1 protocol for compatibility with both RabbitMQ and ActiveMQ
- Topic exchange for flexible event routing by type

**Test targets:**
- **Unit tests:**
  - RabbitMQ backend: publish, subscribe, acknowledge, dead letter
  - Exchange and queue declaration

- **Integration tests:**
  - Start BleepStore with RabbitMQ queue, run E2E suite — all 75 tests pass
  - Verify events routed correctly by type

**Definition of done:**
- [x] RabbitMQ backend implements full QueueBackend interface
- [x] All 75 E2E tests pass with RabbitMQ queue enabled (write-through mode)
- [x] Dead letter exchange handles failed messages
- [x] Compatible with AMQP 0-9-1 (ActiveMQ compatible)

---

### Stage 16c: Kafka Backend & Consistency Modes (~100k tokens)

**Goal:** Implement the Kafka backend and the sync/async consistency modes. All three queue backends support all three consistency modes.

**Inputs:** Stage 16b complete (RabbitMQ backend working).

**Spec references:**
- `specs/event-queues.md` -- Kafka specifics, sync/async consistency modes, crash-only integration

**Implementation scope:**

1. **Kafka backend**
   - Topics per event type (e.g., `bleepstore.object.created`)
   - Consumer groups for parallel processing
   - `acks=all` for durability
   - Partitioned by bucket name for ordering within a bucket
   - Configuration: `queue.kafka.brokers`, `queue.kafka.topic_prefix`, `queue.kafka.consumer_group`

2. **Sync mode** (all backends)
   - Handler writes request body to temp file (fsync)
   - Handler enqueues WriteTask to queue
   - Handler blocks waiting for consumer to complete task
   - Consumer writes to storage, commits metadata, marks task complete
   - Crash-safe: pending tasks survive in queue, reprocessed on reconnect

3. **Async mode** (all backends)
   - Handler writes request body to temp file (fsync)
   - Handler enqueues WriteTask to queue
   - Handler responds 202 Accepted immediately
   - Consumer processes task asynchronously
   - Eventually consistent reads
   - Clean up orphan temp files from async writes on startup

4. **Consistency mode integration**
   - `queue.consistency` config: `write-through` (default), `sync`, `async`
   - Mode switching in handler middleware
   - Sync/async modes use WriteTask structure from spec

**Key decisions:**
- Kafka requires `acks=all` for crash-only safety
- Sync mode timeout: configurable, returns 504 Gateway Timeout if consumer doesn't complete in time
- Async mode: 202 Accepted with `Location` header for eventual GET

**Test targets:**
- **Unit tests:**
  - Kafka backend: publish, subscribe, acknowledge
  - Sync mode: handler blocks until task completed
  - Async mode: handler returns 202, task processed asynchronously

- **Integration tests:**
  - Start BleepStore with Kafka queue, run E2E suite — all 75 tests pass
  - Kill BleepStore mid-operation, restart, verify pending tasks reprocessed
  - Sync mode: write completes only after consumer processes task
  - Async mode: write returns 202, object eventually available

**Definition of done:**
- [x] Kafka backend implements full QueueBackend interface
- [x] Sync mode: writes blocked until queue consumer completes (all backends)
- [x] Async mode: writes return 202, processed asynchronously (all backends)
- [x] All 75 E2E tests pass with Kafka queue enabled (write-through mode)
- [x] Crash-only: pending tasks survive restarts, orphan temp files cleaned
- [x] All three backends support all three consistency modes

---

## Appendix

### A. External Test Suite Setup

#### Ceph s3-tests
```bash
git clone https://github.com/ceph/s3-tests.git
cd s3-tests
./bootstrap

# Configure for BleepStore
cat > s3tests.conf <<EOF
[DEFAULT]
host = localhost
port = 9000
is_secure = no

[fixtures]
bucket prefix = s3test-

[s3 main]
access_key = bleepstore
secret_key = bleepstore-secret
display_name = BleepStore Test User
user_id = bleepstore-user
email = test@bleepstore.local

[s3 alt]
access_key = bleepstore-alt
secret_key = bleepstore-alt-secret
display_name = BleepStore Alt User
user_id = bleepstore-alt-user
email = alt@bleepstore.local
EOF

# Run (filter to Phase 1 ops)
S3TEST_CONF=s3tests.conf python -m pytest s3tests_boto3/functional/ \
  -k "test_bucket or test_object or test_multipart" \
  --ignore=s3tests_boto3/functional/test_s3_versioning.py \
  --ignore=s3tests_boto3/functional/test_s3_lifecycle.py
```

#### MinIO Mint
```bash
docker run --rm \
  --network host \
  -e SERVER_ENDPOINT=localhost:9000 \
  -e ACCESS_KEY=bleepstore \
  -e SECRET_KEY=bleepstore-secret \
  minio/mint:latest
```

#### Snowflake s3compat
```bash
git clone https://github.com/Snowflake-Labs/s3-compat-tests.git
cd s3-compat-tests
pip install -r requirements.txt

S3_ENDPOINT=http://localhost:9000 \
S3_ACCESS_KEY=bleepstore \
S3_SECRET_KEY=bleepstore-secret \
python -m pytest tests/
```

#### MinIO Warp
```bash
# Install warp
go install github.com/minio/warp@latest

# Run mixed workload benchmark
warp mixed \
  --host=localhost:9000 \
  --access-key=bleepstore \
  --secret-key=bleepstore-secret \
  --duration=60s \
  --concurrent=10 \
  --obj.size=1KiB
```

### B. CI Pipeline Configuration

The CI pipeline should test each language implementation independently:

```yaml
# Example GitHub Actions matrix
strategy:
  matrix:
    language: [python, golang, rust, zig]
    backend: [local]
    mode: [embedded]
    include:
      # Cloud backend tests (require credentials)
      - language: python
        backend: aws
        mode: embedded
      # Cluster mode tests
      - language: golang
        backend: local
        mode: cluster

steps:
  - name: Build ${{ matrix.language }} implementation
    run: make -C ${{ matrix.language }} build

  - name: Start BleepStore
    run: make -C ${{ matrix.language }} run &

  - name: Wait for server
    run: |
      for i in $(seq 1 30); do
        curl -s http://localhost:9000/health && break
        sleep 1
      done

  - name: Run E2E tests
    run: |
      cd tests
      BLEEPSTORE_ENDPOINT=http://localhost:9000 ./run_tests.sh

  - name: Run smoke test
    run: |
      BLEEPSTORE_ENDPOINT=http://localhost:9000 tests/smoke/smoke_test.sh
```

### C. Test Matrix

| | Python | Go | Rust | Zig |
|---|---|---|---|---|
| **Local + Embedded** | E2E + Smoke + Perf | E2E + Smoke + Perf | E2E + Smoke + Perf | E2E + Smoke + Perf |
| **AWS + Embedded** | E2E + Smoke | E2E + Smoke | E2E + Smoke | E2E + Smoke |
| **GCP + Embedded** | E2E + Smoke | E2E + Smoke | E2E + Smoke | E2E + Smoke |
| **Azure + Embedded** | E2E + Smoke | E2E + Smoke | E2E + Smoke | E2E + Smoke |
| **Local + Cluster** | E2E + Smoke | E2E + Smoke | E2E + Smoke | E2E + Smoke |
| **Ceph s3-tests** | Phase 1 subset | Phase 1 subset | Phase 1 subset | Phase 1 subset |
| **MinIO Mint** | Core mode | Core mode | Core mode | Core mode |
| **Snowflake s3compat** | 9 ops | 9 ops | 9 ops | 9 ops |
| **MinIO Warp** | Benchmarks | Benchmarks | Benchmarks | Benchmarks |

### D. Compatibility Tracking

Track test results across implementations using a table like:

| Test | Python | Go | Rust | Zig |
|---|---|---|---|---|
| test_buckets.py (16) | 0/16 | 0/16 | 0/16 | 0/16 |
| test_objects.py (32) | 0/32 | 0/32 | 0/32 | 0/32 |
| test_multipart.py (11) | 0/11 | 0/11 | 0/11 | 0/11 |
| test_presigned.py (4) | 0/4 | 0/4 | 0/4 | 0/4 |
| test_acl.py (4) | 0/4 | 0/4 | 0/4 | 0/4 |
| test_errors.py (8) | 0/8 | 0/8 | 0/8 | 0/8 |
| **Total (75)** | **0/75** | **0/75** | **0/75** | **0/75** |
| smoke_test.sh (20) | 0/20 | 0/20 | 0/20 | 0/20 |

Update after each stage is completed for each language.

### E. Stage Summary Quick Reference

| Stage | Milestone | Focus | Test Target | Key Spec |
|---|---|---|---|---|
| 1 | Foundation | Server bootstrap, routing, error XML | Health check, error format | s3-error-responses.md, s3-common-headers.md |
| 1b | Foundation | Framework upgrade, OpenAPI, Prometheus | /docs, /openapi.json, /metrics | observability-and-openapi.md |
| 2 | Foundation | SQLite metadata store | Unit tests (metadata CRUD) | metadata-schema.md |
| 3 | Bucket Ops | All 7 bucket operations | test_buckets.py (16) | s3-bucket-operations.md |
| 4 | Object Ops | Basic CRUD + filesystem backend | test_objects.py (12 basic) | s3-object-operations.md, storage-backends.md |
| 5a | Object Ops | List, copy, batch delete | test_objects.py (14 list/copy/delete) | s3-object-operations.md |
| 5b | Object Ops | Range, conditional requests, ACLs | test_objects.py (6 range/cond), test_acl.py (4) | s3-object-operations.md |
| 6 | Auth | SigV4 header auth + presigned URLs | test_presigned.py (4), test_errors.py auth (2) | s3-authentication.md |
| 7 | Multipart | Core: create, upload, abort, list | test_multipart.py (8 partial) | s3-multipart-upload.md |
| 8 | Multipart | Complete with assembly + composite ETag | test_multipart.py (11) | s3-multipart-upload.md |
| 9a | Integration | Internal E2E + smoke test pass | All 75 E2E + smoke (20) | All specs |
| 9b | Compliance | External test suites (Ceph, Mint, s3compat) | Ceph >80%, s3compat 9/9 | All specs |
| 10 | Cloud | AWS S3 gateway backend | 75 E2E with AWS backend | storage-backends.md |
| 11a | Cloud | GCP Cloud Storage backend | 75 E2E with GCP backend | storage-backends.md |
| 11b | Cloud | Azure Blob Storage backend | 75 E2E with Azure backend | storage-backends.md |
| 12a | Cluster | Raft state machine & storage | Raft unit tests (in-process) | clustering.md |
| 12b | Cluster | Raft networking & elections | Multi-node election tests | clustering.md |
| 13a | Cluster | Raft-metadata wiring | Multi-node write/read tests | clustering.md |
| 13b | Cluster | Snapshots & node management | Snapshot + join/leave tests | clustering.md |
| 14 | Cluster | Admin API + cluster E2E | Admin API + cluster E2E | clustering.md, admin-api.openapi.yaml |
| 15 | Performance | Optimization + hardening | Warp benchmarks, perf tests | OBJECTIVE.MD success criteria |
| 16a | Event Queues | Queue interface + Redis backend | 75 E2E with Redis queue | event-queues.md |
| 16b | Event Queues | RabbitMQ backend | 75 E2E with RabbitMQ queue | event-queues.md |
| 16c | Event Queues | Kafka backend + consistency modes | 75 E2E with Kafka + sync/async | event-queues.md |
