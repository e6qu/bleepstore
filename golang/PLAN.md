# BleepStore Go Implementation Plan

This document is the Go-specific implementation plan derived from the [global plan](../PLAN.md). It preserves the same stage numbering and milestone structure. Each stage adds Go-specific setup instructions, exact file paths, library notes, idioms, test approach, build/run commands, and dependency information.

**Go version:** 1.22+ (required for `http.ServeMux` method-based routing: `"GET /{bucket}"`)

**Module:** `github.com/bleepstore/bleepstore`

**Reference:** See `../PLAN.md` for full spec references, detailed implementation scope, and definition-of-done checklists.

---

## Development Environment

```bash
# Build
cd golang/
go build -o bleepstore ./cmd/bleepstore

# Run
./bleepstore -config ../bleepstore.example.yaml -port 9000

# Run all unit tests
go test ./...

# Run tests with verbose output and race detector
go test -v -race ./...

# Run E2E tests (from repo root, server must be running on :9000)
cd tests/ && ./run_tests.sh
```

---

## Milestone 1: Foundation (Stages 1-2)

### Stage 1: Server Bootstrap & Configuration ✅

**Goal:** HTTP server boots, routes all S3 API paths, returns well-formed S3 error XML for every route. Health check works. Every response carries `x-amz-request-id` and `Date` headers.

**Go-specific setup:**
```bash
cd golang/
go mod tidy
go build -o bleepstore ./cmd/bleepstore
./bleepstore -config ../bleepstore.example.yaml -port 9000
```

**Files to modify:**
- `golang/cmd/bleepstore/main.go` -- Add graceful shutdown via `os/signal` + `context.WithCancel`; wire `SIGINT`/`SIGTERM`.
- `golang/internal/config/config.go` -- Already loads YAML via `gopkg.in/yaml.v3`. Add fallback to `bleepstore.example.yaml` if primary path fails. Validate required fields.
- `golang/internal/server/server.go` -- Add `GET /health` handler. Add a middleware (`http.Handler` wrapper) that injects common response headers (`x-amz-request-id`, `x-amz-id-2`, `Date`, `Server: BleepStore`) on every response. All existing stub routes already registered.
- `golang/internal/errors/errors.go` -- Add missing error codes: `NotImplemented`, `InvalidAccessKeyId`, `InvalidArgument`, `EntityTooSmall`, `PreconditionFailed`, `InvalidRange`, `MissingContentLength`, `BucketAlreadyOwnedByYou`, `RequestTimeTooSkewed`.
- `golang/internal/xmlutil/xmlutil.go` -- Ensure `RenderError` produces error XML with NO `xmlns` namespace. Success response structs must include `xmlns` attribute. Fix `FormatTimeS3` to use millisecond-precision ISO 8601 (`2006-01-02T15:04:05.000Z`). Add `FormatTimeHTTP` for RFC 7231 (`Mon, 02 Jan 2006 15:04:05 GMT`).
- `golang/internal/handlers/bucket.go` -- Make all handler methods return `NotImplemented` S3 error XML via `xmlutil.RenderError`.
- `golang/internal/handlers/object.go` -- Same: return `NotImplemented`.
- `golang/internal/handlers/multipart.go` -- Same: return `NotImplemented`.

**Library-specific notes:**
- Go 1.22 `http.ServeMux` supports `"GET /{bucket}"` and `"PUT /{bucket}/{object...}"` patterns natively. The scaffold already uses this -- no external router needed.
- `r.PathValue("bucket")` and `r.PathValue("object")` extract path parameters.
- Query parameter dispatch (e.g., `?location`, `?acl`, `?uploads`) is already handled in `server.go` dispatch methods.
- XML marshaling: use `encoding/xml` with struct tags. The `XMLName` field controls the root element name.
- `x-amz-request-id`: generate via `crypto/rand` -> 16-char hex string.

**Go idioms:**
- Graceful shutdown: `signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)` then `httpServer.Shutdown(ctx)`.
- Middleware as a function returning `http.Handler`:
  ```go
  func commonHeaders(next http.Handler) http.Handler {
      return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
          w.Header().Set("x-amz-request-id", generateRequestID())
          w.Header().Set("Server", "BleepStore")
          // ...
          next.ServeHTTP(w, r)
      })
  }
  ```
- Use `ResponseWriter` wrapper to ensure headers are set before `WriteHeader` is called.

**Crash-only design:**
- Every startup is a recovery. There is no separate `--recovery-mode` flag.
- On startup: open SQLite (WAL auto-recovers), clean temp files in `data/.tmp/`, reap expired multipart uploads, seed default credentials.
- No special "clean shutdown" logic -- SIGTERM stops accepting connections but does not perform cleanup that startup wouldn't also do.
- Atomic file writes: always use temp-file + `fsync` + `os.Rename` pattern. Never write directly to the final object path.

**Unit test approach:**
- File: `golang/internal/server/server_test.go`
- Use `httptest.NewServer` or `httptest.NewRecorder` for handler tests.
- Table-driven tests for route matching:
  ```go
  tests := []struct {
      method string
      path   string
      status int
  }{
      {"GET", "/health", 200},
      {"GET", "/", 501},
      {"PUT", "/my-bucket", 501},
      // ...
  }
  ```
- File: `golang/internal/xmlutil/xmlutil_test.go` -- Verify error XML format, namespace absence, date formatting.
- File: `golang/internal/config/config_test.go` -- Verify YAML parsing, defaults, fallback path.

**Build/run commands:**
```bash
go build -o bleepstore ./cmd/bleepstore
go test -v -race ./internal/server/ ./internal/xmlutil/ ./internal/config/ ./internal/errors/
```

**Dependencies to add:** None (stdlib + `gopkg.in/yaml.v3` already present).

---

### Stage 1b: Framework Upgrade, OpenAPI & Observability ✅

**Goal:** Replace net/http ServeMux with Huma framework. Add OpenAPI 3.1 serving with Swagger UI (Stoplight Elements), request validation via Huma struct tags, and Prometheus metrics at /metrics.

**Go-specific setup:**
```bash
cd golang/
go get github.com/danielgtaylor/huma/v2
go get github.com/go-chi/chi/v5        # Chi adapter for Huma
go get github.com/prometheus/client_golang/prometheus
go get github.com/prometheus/client_golang/prometheus/promhttp
go mod tidy
```

**Files to modify:**
- `golang/internal/server/server.go` -- **Major rewrite.** Replace `http.ServeMux` routing with Huma operations. Define Huma input/output structs for each S3 operation. Huma auto-generates OpenAPI spec and serves Swagger UI (Stoplight Elements) at `/docs`. Wire `/metrics` endpoint via `promhttp.Handler()`.
- `golang/internal/server/middleware.go` -- **(new file)** Prometheus middleware: wrap Huma router with metrics collection (request count, duration histogram). Keep existing common headers middleware.
- `golang/internal/metrics/metrics.go` -- **(new file)** Define custom Prometheus metrics: `bleepstore_s3_operations_total`, `bleepstore_objects_total`, `bleepstore_buckets_total`, `bleepstore_bytes_received_total`, `bleepstore_bytes_sent_total`, `bleepstore_http_request_duration_seconds`.
- `golang/go.mod` -- Add Huma, Chi, prometheus client dependencies.

**Library-specific notes:**
- **Huma** (`github.com/danielgtaylor/huma/v2`) provides:
  - Automatic OpenAPI 3.1 spec generation from Go struct tags
  - Built-in request validation via JSON Schema (from struct tags)
  - Swagger UI (Stoplight Elements) at `/docs`
  - `/openapi.json` endpoint
  ```go
  import "github.com/danielgtaylor/huma/v2"
  import "github.com/danielgtaylor/huma/v2/adapters/humachi"

  router := chi.NewMux()
  api := humachi.New(router, huma.DefaultConfig("BleepStore S3 API", "1.0.0"))
  huma.Register(api, huma.Operation{
      OperationID: "get-health",
      Method:      http.MethodGet,
      Path:        "/health",
  }, func(ctx context.Context, input *struct{}) (*HealthOutput, error) {
      return &HealthOutput{Body: HealthBody{Status: "ok"}}, nil
  })
  ```
- **Prometheus** (`prometheus/client_golang`):
  ```go
  import "github.com/prometheus/client_golang/prometheus/promhttp"
  router.Handle("/metrics", promhttp.Handler())
  ```
  Go runtime metrics (goroutines, memory, GC) are auto-registered.

**Go idioms:**
- Huma operations use struct-based I/O: input structs define path params, query params, headers, and body. Output structs define response body and headers. Validation tags: `minLength`, `maxLength`, `pattern`, `minimum`, `maximum`.
- Huma auto-rejects invalid requests with proper error responses. Override the error transformer to return S3 error XML instead of Huma's default JSON errors.

**Crash-only design:**
- Metrics counters reset on restart (Prometheus handles gaps)
- Never block a request for metrics
- `/metrics` endpoint is read-only

**Unit test approach:**
- `golang/internal/server/server_test.go` -- Update existing tests for Huma-based routing. Verify `/docs` returns HTML, `/openapi.json` returns valid JSON, `/metrics` returns Prometheus text format.
- `golang/internal/metrics/metrics_test.go` -- Verify custom metric registration and increment.

**Build/run commands:**
```bash
go mod tidy
go build -o bleepstore ./cmd/bleepstore
go test -v -race ./internal/server/ ./internal/metrics/
```

**Dependencies to add:**
```
github.com/danielgtaylor/huma/v2
github.com/go-chi/chi/v5
github.com/prometheus/client_golang
```

---

### Stage 2: Metadata Store & SQLite ✅

**Goal:** SQLite-backed metadata store with full CRUD for buckets, objects, multipart uploads, and credentials. All unit-tested. No HTTP handler changes.

**Go-specific setup:**
```bash
go get github.com/mattn/go-sqlite3
# or for a pure-Go driver (no CGO required):
go get modernc.org/sqlite
go mod tidy
```

**Files to modify:**
- `golang/internal/metadata/store.go` -- Expand `MetadataStore` interface: add `UpdateBucketAcl`, `UpdateObjectAcl`, `ObjectExists`, `DeleteObjectsMeta`, `GetPartsForCompletion`, `GetCredential`, `PutCredential` methods. Expand record structs to include all fields from the spec (ACL as `json.RawMessage`, `UserMetadata` as `map[string]string`, `ContentEncoding`, `ContentLanguage`, `ContentDisposition`, `CacheControl`, `Expires`, `StorageClass`, `DeleteMarker`). Add `CredentialRecord` struct.
- `golang/internal/metadata/sqlite.go` -- Full implementation: open `*sql.DB`, apply PRAGMAs (`journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=ON`, `busy_timeout=5000`), create all 6 tables (`buckets`, `objects`, `multipart_uploads`, `multipart_parts`, `credentials`, `schema_version`) with indexes. Implement every `MetadataStore` method using `database/sql` prepared statements.
- `golang/internal/metadata/sqlite_test.go` -- **(new file)** Comprehensive unit tests.
- `golang/cmd/bleepstore/main.go` -- Initialize `SQLiteStore` on startup, pass to `server.New`. Seed default credentials.

**Library-specific notes:**
- **`database/sql`** is the standard Go database abstraction. Use `db.QueryRowContext`, `db.ExecContext`, `db.QueryContext` with `context.Context` throughout.
- **SQLite driver choice:**
  - `modernc.org/sqlite` -- pure Go, no CGO, cross-compiles easily. Recommended.
  - `github.com/mattn/go-sqlite3` -- CGO-based, more mature, slightly faster. Requires C compiler.
- JSON columns: store ACL and user_metadata as `TEXT` in SQLite, marshal/unmarshal with `encoding/json`. Use `json.RawMessage` for ACL field in structs to defer parsing.
- `INSERT OR REPLACE` for upsert semantics on objects and parts.
- Wrap `complete_multipart_upload` in a `db.BeginTx` transaction.

**Go idioms:**
- `sql.NullString`, `sql.NullInt64`, `sql.NullTime` for nullable columns.
- `defer rows.Close()` after every `db.QueryContext`.
- Error wrapping: `fmt.Errorf("creating bucket %q: %w", name, err)`.
- `context.Context` as first parameter on every method.
- `ListObjects` with delimiter: query all matching keys, then group in Go using `strings.SplitN(key, delimiter, 2)` to compute `CommonPrefixes`.
- Continuation token = last key from previous page (base64-encode for opacity).

**Unit test approach:**
- File: `golang/internal/metadata/sqlite_test.go`
- Use `t.TempDir()` for each test's SQLite file -- automatic cleanup.
- Table-driven tests for CRUD round-trips:
  ```go
  func TestBucketCRUD(t *testing.T) {
      store := newTestStore(t)
      defer store.Close()
      // create, get, list, delete, verify not found
  }
  ```
- Test `ListObjects` with prefix, delimiter, pagination, CommonPrefixes.
- Test multipart lifecycle: create upload, register parts, list parts, complete, verify object created and upload/parts deleted.
- Test idempotent schema creation (call `InitDB` twice).

**Build/run commands:**
```bash
go test -v -race ./internal/metadata/
```

**Dependencies to add:**
```
modernc.org/sqlite   # pure-Go SQLite driver (or github.com/mattn/go-sqlite3)
```

---

## Milestone 2: Bucket Operations (Stage 3)

### Stage 3: Bucket CRUD ✅

**Goal:** All 7 bucket handlers implemented and wired to metadata store. 16 E2E bucket tests pass.

**Files to modify:**
- `golang/internal/handlers/bucket.go` -- Inject `MetadataStore`, `StorageBackend`, and owner info into `BucketHandler` struct. Implement all 7 handler methods: `ListBuckets`, `CreateBucket`, `DeleteBucket`, `HeadBucket`, `GetBucketLocation`, `GetBucketAcl`, `PutBucketAcl`.
- `golang/internal/handlers/helpers.go` -- **(new file)** Shared handler utilities: `validateBucketName(name string) *S3Error`, `extractOwner(r *http.Request) (id, display string)`, ACL parsing/serialization helpers, `parseCannedACL(name string) []Grant`.
- `golang/internal/server/server.go` -- Pass `MetadataStore` and `StorageBackend` into handler constructors.
- `golang/internal/xmlutil/xmlutil.go` -- Ensure `ListAllMyBucketsResult`, `LocationConstraint`, and `AccessControlPolicy` structs have correct `xmlns` attributes. Verify Grantee XML includes `xmlns:xsi` and `xsi:type` attributes (may need custom `MarshalXML`).
- `golang/cmd/bleepstore/main.go` -- Initialize `LocalBackend`, pass to server. Create storage root directory.

**Library-specific notes:**
- Bucket name validation: use `regexp.MustCompile` for the pattern, compile once at package level.
- `encoding/xml` handles the `xmlns` attribute via the `XMLName` field:
  ```go
  XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
  ```
- For the `Grantee` type with `xsi:type` attribute, you may need a custom `MarshalXML` method because `encoding/xml` does not natively support namespace-prefixed attributes. Alternative: use `xml:",innerxml"` with pre-rendered XML string.
- `GetBucketLocation` for `us-east-1`: return empty `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`.

**Go idioms:**
- Handler structs with injected dependencies (constructor injection, no global state):
  ```go
  type BucketHandler struct {
      meta    metadata.MetadataStore
      storage storage.StorageBackend
      region  string
  }
  ```
- Errors returned from metadata store checked with `errors.Is` or type assertions.
- `r.PathValue("bucket")` to extract bucket name.
- `w.WriteHeader(http.StatusNoContent)` for DeleteBucket (204).

**Unit test approach:**
- File: `golang/internal/handlers/bucket_test.go`
- Use `httptest.NewRecorder` with a real (in-memory SQLite) metadata store.
- Table-driven tests for bucket name validation edge cases.
- Verify XML response structure by unmarshaling response body.

**Build/run commands:**
```bash
go test -v -race ./internal/handlers/ ./internal/xmlutil/
# E2E:
cd ../tests && python -m pytest e2e/test_buckets.py -v
```

**Dependencies to add:** None.

---

## Milestone 3: Object Operations (Stages 4-5b)

### Stage 4: Basic Object CRUD ✅

**Goal:** Local filesystem storage backend implemented. PutObject, GetObject, HeadObject, DeleteObject work end-to-end.

**Files to modify:**
- `golang/internal/storage/local.go` -- Full implementation of `LocalBackend`: `PutObject` (write to temp file, compute MD5 with `crypto/md5` via `io.TeeReader`, atomic rename via `os.Rename`), `GetObject` (open file, `os.Stat` for size), `DeleteObject` (remove file), `CreateBucket`/`DeleteBucket` (mkdir/rmdir), `ObjectExists` (`os.Stat`).
- `golang/internal/handlers/object.go` -- Inject `MetadataStore` and `StorageBackend`. Implement `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`.
- `golang/internal/handlers/helpers.go` -- Add `extractUserMetadata(r *http.Request) map[string]string` (scan `x-amz-meta-*` headers), `setObjectResponseHeaders(w, objMeta)`.
- `golang/internal/server/server.go` -- Pass dependencies to `ObjectHandler`.

**Library-specific notes:**
- **MD5 computation during write:** Use `io.TeeReader` to hash while writing:
  ```go
  h := md5.New()
  tee := io.TeeReader(reader, h)
  // write tee to file
  etag := fmt.Sprintf(`"%x"`, h.Sum(nil))
  ```
- **Atomic writes:** Write to `path + ".tmp." + uuid`, then `os.Rename`. Go's `os.Rename` is atomic on the same filesystem.
- **Streaming:** `io.Copy(file, reader)` streams without buffering the entire object.
- **Path construction:** Use `filepath.Join(rootDir, bucket, key)`, but be aware that `filepath.Join` cleans paths. Keys with `/` naturally create subdirectories via `os.MkdirAll(filepath.Dir(path), 0o755)`.
- **Content-Type default:** `application/octet-stream` when header not provided.
- `GetObject`: use `http.ServeContent` for basic range support, or manually set headers and `io.Copy`.

**Go idioms:**
- `defer file.Close()` after opening files.
- `io.ReadCloser` for GetObject return -- caller closes when done streaming.
- `os.IsNotExist(err)` or `errors.Is(err, os.ErrNotExist)` to detect missing files.
- Use `context.Context` for cancellation -- pass `ctx` through to I/O operations where applicable.

**Unit test approach:**
- File: `golang/internal/storage/local_test.go`
- Use `t.TempDir()` for storage root.
- Table-driven: put object, get back, verify content and ETag match.
- Test nested keys with `/`.
- Test delete idempotency.
- File: `golang/internal/handlers/object_test.go`
- Integration-style tests with `httptest.NewRecorder`, real SQLite store, real local backend (temp dir).

**Build/run commands:**
```bash
go test -v -race ./internal/storage/ ./internal/handlers/
# E2E (partial -- basic object tests):
cd ../tests && python -m pytest e2e/test_objects.py -v -k "TestPutAndGetObject or TestHeadObject or TestDeleteObject"
```

**Dependencies to add:** None (all stdlib: `crypto/md5`, `io`, `os`, `path/filepath`).

---

### Stage 5a: List, Copy & Batch Delete ✅

**Goal:** Implement CopyObject, DeleteObjects (batch), ListObjectsV2, and ListObjects v1.

**Files to modify:**
- `golang/internal/handlers/object.go` -- Implement: `CopyObject` (parse `X-Amz-Copy-Source`, handle `x-amz-metadata-directive`), `DeleteObjects` (parse XML body, iterate, build `DeleteResult`), `ListObjectsV2`, `ListObjects` (v1).
- `golang/internal/storage/local.go` -- Implement `CopyObject` (open source, create dest, `io.Copy`, compute ETag).
- `golang/internal/handlers/helpers.go` -- Add XML request body parser for `<Delete>`.
- `golang/internal/xmlutil/xmlutil.go` -- Add `DeleteRequest` struct for parsing `<Delete>` XML input. Ensure all list result structs have proper xmlns.

**Library-specific notes:**
- **XML body parsing:** Use `xml.NewDecoder(r.Body).Decode(&deleteReq)` for `DeleteObjects` XML bodies.
- **`x-amz-copy-source`** URL-decoding: use `url.PathUnescape`.

**Go idioms:**
- String parsing of `x-amz-copy-source`: `strings.TrimPrefix`, `strings.SplitN(source, "/", 2)`.
- `strconv.ParseInt` for `max-keys` query parameter.

**Unit test approach:**
- Expand `golang/internal/handlers/object_test.go` with table-driven tests for:
  - CopyObject with COPY vs REPLACE directive.
  - DeleteObjects quiet vs verbose mode.
  - ListObjectsV2 pagination, prefix, delimiter.
  - ListObjectsV1 marker.

**Build/run commands:**
```bash
go test -v -race ./internal/handlers/ ./internal/storage/
# E2E:
cd ../tests && python -m pytest e2e/test_objects.py -v -k "TestCopyObject or TestDeleteObjects or TestListObjectsV2 or TestListObjectsV1"
```

**Dependencies to add:** None.

---

### Stage 5b: Range, Conditional Requests & Object ACLs ✅

**Goal:** Implement range requests, conditional requests (If-Match, If-None-Match, etc.), and object ACL operations.

**Files to modify:**
- `golang/internal/handlers/object.go` -- Add range request handling and conditional request logic to `GetObject` and `HeadObject`. Implement `GetObjectAcl`, `PutObjectAcl`.
- `golang/internal/handlers/helpers.go` -- Add `parseRange(rangeHeader string, objectSize int64) (start, end int64, err error)`, `checkConditionalHeaders(r *http.Request, etag string, lastModified time.Time) (statusCode int, skip bool)`.

**Library-specific notes:**
- **Range parsing:** Parse `Range: bytes=0-4`, `bytes=5-`, `bytes=-10`. Go's `net/http` does not auto-parse Range for non-`http.ServeContent` responses. Parse manually with string splitting.
- **Conditional headers:** `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`. Parse ETags from header, compare. Parse dates with `http.ParseTime`.
- **XML body parsing:** Use `xml.NewDecoder(r.Body).Decode(&aclReq)` for `PutObjectAcl` XML bodies.
- **304 Not Modified:** `w.WriteHeader(304)` -- no body, no Content-Type.
- **416 Range Not Satisfiable:** Return `InvalidRange` S3 error.

**Go idioms:**
- Error sentinel comparison for conditional request failures.
- `io.LimitReader` combined with `io.NewSectionReader` for range responses.

**Unit test approach:**
- Expand `golang/internal/handlers/object_test.go` with table-driven tests for:
  - Range parsing edge cases.
  - Conditional header combinations.
  - Object ACL get/put round-trip.

**Build/run commands:**
```bash
go test -v -race ./internal/handlers/ ./internal/storage/
# E2E:
cd ../tests && python -m pytest e2e/test_objects.py e2e/test_acl.py -v
```

**Dependencies to add:** None.

---

## Milestone 4: Authentication (Stage 6)

### Stage 6: AWS Signature V4 ✅

**Goal:** SigV4 header auth and presigned URL validation. Invalid credentials rejected. All previous tests still pass.

**Files to modify:**
- `golang/internal/auth/sigv4.go` -- Full implementation: parse `Authorization` header, build canonical request, compute string-to-sign, derive signing key (HMAC-SHA256 chain), verify signature with `crypto/subtle.ConstantTimeCompare`. Add presigned URL validation (parse `X-Amz-Algorithm`, `X-Amz-Credential`, etc. from query params). Add `URIEncode(s string, encodeSlash bool) string` function.
- `golang/internal/auth/sigv4_test.go` -- **(new file)** Extensive tests.
- `golang/internal/auth/middleware.go` -- **(new file)** HTTP middleware that wraps all routes (except `/health`). Detects auth method, calls `VerifyRequest` or `VerifyPresigned`, returns appropriate S3 error on failure. Sets owner identity on the request context.
- `golang/internal/server/server.go` -- Wrap `s.mux` with auth middleware. Exclude `/health`.
- `golang/internal/metadata/store.go` -- Add `CredentialRecord` if not already present, and `GetCredential`/`PutCredential` methods.
- `golang/internal/metadata/sqlite.go` -- Implement credential lookup from `credentials` table.

**Library-specific notes:**
- **HMAC-SHA256:** `crypto/hmac` + `crypto/sha256`:
  ```go
  func hmacSHA256(key []byte, data string) []byte {
      h := hmac.New(sha256.New, key)
      h.Write([]byte(data))
      return h.Sum(nil)
  }
  ```
- **Constant-time comparison:** `crypto/subtle.ConstantTimeCompare([]byte(expected), []byte(provided))`.
- **URI encoding:** Must match S3 behavior exactly. Go's `url.PathEscape` over-encodes. Write a custom encoder that only encodes characters outside `A-Za-z0-9-_.~`.
- **Date parsing:** `time.Parse("20060102T150405Z", amzDate)` for `X-Amz-Date`.
- **Request context for owner identity:** Use `context.WithValue` to attach the authenticated owner to the request context. Handlers retrieve via `r.Context().Value(ownerKey)`.

**Go idioms:**
- Middleware pattern:
  ```go
  func AuthMiddleware(verifier *SigV4Verifier, next http.Handler) http.Handler {
      return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
          if r.URL.Path == "/health" {
              next.ServeHTTP(w, r)
              return
          }
          if err := verifier.VerifyRequest(r); err != nil {
              // render S3 error
              return
          }
          next.ServeHTTP(w, r)
      })
  }
  ```
- Context key as unexported type to avoid collisions.
- `strings.Builder` for efficient canonical request string building.

**Unit test approach:**
- File: `golang/internal/auth/sigv4_test.go`
- Use AWS SigV4 test suite vectors if available.
- Table-driven tests: known request -> expected canonical request -> expected string-to-sign -> expected signature.
- Test presigned URL generation/validation round-trip.
- Test expired presigned URLs.
- Test invalid access key, wrong secret key.
- Test URI encoding edge cases (spaces, unicode, slashes).

**Build/run commands:**
```bash
go test -v -race ./internal/auth/
# E2E:
cd ../tests && python -m pytest e2e/test_presigned.py e2e/test_errors.py -v
# Also re-run all previous tests to verify SigV4 is transparent:
cd ../tests && python -m pytest e2e/ -v
```

**Dependencies to add:** None (all stdlib: `crypto/hmac`, `crypto/sha256`, `crypto/subtle`, `encoding/hex`).

---

## Milestone 5: Multipart Upload (Stages 7-8)

### Stage 7: Multipart Upload - Core ✅

**Goal:** Create, upload parts, abort, list uploads, list parts. Part data stored but not yet assembled.

**Files to modify:**
- `golang/internal/handlers/multipart.go` -- Inject dependencies. Implement `CreateMultipartUpload` (generate UUID via `crypto/rand` or `github.com/google/uuid`, store metadata, render XML), `UploadPart` (validate upload exists, store part data via backend, register in metadata, return ETag), `AbortMultipartUpload` (delete parts from storage and metadata, 204), `ListMultipartUploads` (query metadata, render XML), `ListParts` (query metadata, render XML).
- `golang/internal/storage/local.go` -- Implement `PutPart` (write to `{rootDir}/.multipart/{uploadID}/{partNumber}`, atomic temp+rename), `DeleteParts` (remove entire `{rootDir}/.multipart/{uploadID}/` directory).
- `golang/internal/metadata/sqlite.go` -- Ensure `CreateMultipartUpload`, `GetMultipartUpload`, `PutPart`, `ListParts`, `AbortMultipartUpload`, `ListMultipartUploads` are fully implemented.

**Library-specific notes:**
- **UUID generation:** Either use `crypto/rand` with manual formatting, or add `github.com/google/uuid` dependency.
- **Part number validation:** `strconv.Atoi(r.URL.Query().Get("partNumber"))`, check range 1-10000.
- **Directory cleanup on abort:** `os.RemoveAll(filepath.Join(rootDir, ".multipart", uploadID))`.

**Go idioms:**
- `uuid.New().String()` for upload ID (if using `github.com/google/uuid`).
- `strconv.Atoi` for query parameter parsing, return `InvalidArgument` error on failure.
- `os.MkdirAll` to create `.multipart/{uploadID}/` directory tree.

**Unit test approach:**
- Extend `golang/internal/handlers/multipart_test.go` **(new file)**.
- Test full lifecycle: create upload, upload 3 parts, list parts, abort, verify cleaned up.
- Test part overwrite (same part number twice).
- Test NoSuchUpload error.

**Build/run commands:**
```bash
go test -v -race ./internal/handlers/ ./internal/storage/ ./internal/metadata/
# E2E:
cd ../tests && python -m pytest e2e/test_multipart.py -v -k "not test_basic_multipart_upload and not test_complete"
```

**Dependencies to add:**
```
github.com/google/uuid  # (optional -- can use crypto/rand instead)
```

---

### Stage 8: Multipart Upload - Completion ✅

**Goal:** CompleteMultipartUpload with part assembly, composite ETag, part validation. UploadPartCopy. All 11 multipart tests pass.

**Files to modify:**
- `golang/internal/handlers/multipart.go` -- Implement `CompleteMultipartUpload`: parse `<CompleteMultipartUpload>` XML body, validate part order and ETags, call storage backend `AssembleParts`, compute composite ETag, call metadata `CompleteMultipartUpload`, render `CompleteMultipartUploadResult` XML.
- `golang/internal/handlers/helpers.go` -- Add `computeCompositeETag(partETags []string) string` function, `parseCompleteMultipartXML(body io.Reader) ([]CompletePart, error)`.
- `golang/internal/storage/local.go` -- Implement `AssembleParts`: open each part file in order, stream-concatenate to final object path (temp file + rename), compute composite ETag from part MD5s, delete part files.
- `golang/internal/server/server.go` -- Wire `UploadPartCopy` dispatch (already present: checks `X-Amz-Copy-Source` in `dispatchObjectPut` with `partNumber`+`uploadId`). May need a `MultipartHandler.UploadPartCopy` method or handle within `UploadPart`.

**Library-specific notes:**
- **Composite ETag computation:**
  ```go
  func computeCompositeETag(partETags []string) string {
      h := md5.New()
      for _, etag := range partETags {
          hex := strings.Trim(etag, `"`)
          raw, _ := hexPkg.DecodeString(hex)
          h.Write(raw)
      }
      return fmt.Sprintf(`"%x-%d"`, h.Sum(nil), len(partETags))
  }
  ```
- **Part size validation:** All parts except last must be >= 5 MiB (5,242,880 bytes). Check by querying stored part sizes from metadata.
- **Streaming assembly:** Open output file, loop over part numbers, open each part file, `io.Copy` to output, close part file. Avoids holding all data in memory.

**Go idioms:**
- Transaction: `tx, _ := db.BeginTx(ctx, nil)` then `tx.Commit()` or `tx.Rollback()` with `defer`.
- XML input parsing: define `CompletePart` struct with `PartNumber int` and `ETag string`, decode from request body.
- Error accumulation: check each validation condition, return first error found.

**Unit test approach:**
- Test composite ETag computation against known values.
- Test part size validation (parts too small, last part allowed to be small).
- Test invalid part order (descending part numbers).
- Test ETag mismatch.
- Integration test: create upload, upload parts, complete, verify assembled content.

**Build/run commands:**
```bash
go test -v -race ./internal/handlers/ ./internal/storage/
# E2E (all 11 multipart tests):
cd ../tests && python -m pytest e2e/test_multipart.py -v
```

**Dependencies to add:** None.

---

## Milestone 6: Integration & Compliance (Stages 9a-9b)

### Stage 9a: Core Integration Testing ✅

**Goal:** All 75 E2E tests pass. Smoke test passes (20/20). Fix compliance issues found by internal tests.

**Completed 2026-02-23:** Added ErrKeyTooLongError and key length validation. Created 37 in-process integration tests covering all E2E scenarios. 202 total tests passing. See `tasks/done/stage-09a-integration-testing.md`.

**Files to modify:** Potentially any file -- this stage is primarily debugging and fixing.

**Common Go-specific compliance issues to check:**
- XML namespace: success responses need `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"` in the root element. Error XML must NOT have xmlns. In `encoding/xml`, set namespace via `XMLName`:
  ```go
  XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
  ```
- ETag quoting: ensure all ETags include literal `"` in the string value. Some `encoding/xml` serialization may escape quotes -- verify raw output.
- `Content-Length` header: Go's `http.ResponseWriter` auto-sets this if the response is fully buffered before `Write`. For streaming responses, set it manually via `w.Header().Set("Content-Length", strconv.FormatInt(size, 10))`.
- Date formats:
  - HTTP headers: `time.Now().UTC().Format(http.TimeFormat)` (RFC 7231).
  - XML bodies: `t.UTC().Format("2006-01-02T15:04:05.000Z")` (ISO 8601 with milliseconds).
- `Accept-Ranges: bytes` header on GetObject and HeadObject responses.
- HEAD responses: Go's `net/http` automatically suppresses the body for HEAD requests when using `http.HandlerFunc`, but make sure no XML body is written on HEAD errors.
- Empty XML elements: when `Contents` is empty (0 objects), `encoding/xml` with `omitempty` on a nil/empty slice will omit the element entirely. Verify this matches S3 behavior.

**Go idioms:**
- Use `go test -race ./...` to catch any data races introduced across stages.
- Use `go vet ./...` for static analysis.
- Profiling: `go test -cpuprofile cpu.prof -memprofile mem.prof ./...` for spotting hot paths early.

**Build/run commands:**
```bash
# Full unit test suite
go test -v -race ./...

# Build and start server
go build -o bleepstore ./cmd/bleepstore && ./bleepstore -config ../bleepstore.example.yaml -port 9000

# Full E2E suite
cd ../tests && python -m pytest e2e/ -v --tb=long

# Smoke test
BLEEPSTORE_ENDPOINT=http://localhost:9000 ../tests/smoke/smoke_test.sh
```

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
- [ ] All 75 BleepStore E2E tests pass
- [ ] Smoke test passes (20/20)
- [ ] `aws s3 cp`, `aws s3 ls`, `aws s3 sync` work out of the box
- [ ] No 500 Internal Server Error for valid requests
- [ ] XML responses are well-formed and namespace-correct
- [ ] All headers match S3 format expectations

**Dependencies to add:** None.

---

### Stage 9b: External Test Suites & Compliance

**Goal:** Run external S3 conformance test suites (Ceph s3-tests, MinIO Mint, Snowflake s3compat) and fix compliance issues found.

**Files to modify:** Potentially any file -- this stage is primarily debugging and fixing compliance issues discovered by external suites.

**Implementation scope:**

1. **Run Ceph s3-tests** (filtered to Phase 1 operations)
   - Filter to bucket CRUD, object CRUD, multipart, ACL, presigned URL tests
   - Skip versioning, lifecycle, replication, CORS, website, notification tests
   - Fix failures related to XML formatting, header values, edge cases

2. **Run MinIO Mint** core mode
   - Uses aws-cli, mc (MinIO client), and various SDK tests
   - Fix failures related to SDK-specific expectations

3. **Run Snowflake s3compat** (9 core operations)
   - Quick conformance check for the 9 most important operations

**Go-specific compliance notes:**
- Content-MD5 validation may be expected by external suites -- verify `encoding/base64` + `crypto/md5`.
- Chunked transfer encoding: Go's `net/http` handles this transparently for reads, but some external tests may expect specific chunked upload behavior.
- Multi-SDK compatibility: ensure Go's `encoding/xml` output is parseable by all major SDK XML parsers.

**Build/run commands:**
```bash
# Ceph s3-tests (see Appendix in global PLAN.md for setup)
S3TEST_CONF=s3tests.conf python -m pytest s3tests_boto3/functional/ \
  -k "test_bucket or test_object or test_multipart"

# MinIO Mint
docker run --rm --network host \
  -e SERVER_ENDPOINT=localhost:9000 \
  -e ACCESS_KEY=bleepstore \
  -e SECRET_KEY=bleepstore-secret \
  minio/mint:latest

# Snowflake s3compat
S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=bleepstore S3_SECRET_KEY=bleepstore-secret \
  python -m pytest tests/
```

**Test targets:**
- Ceph s3-tests: >80% of Phase 1-applicable tests pass
- Snowflake s3compat: 9/9 pass
- MinIO Mint aws-cli tests pass
- All 75 BleepStore E2E tests still pass (no regressions)

**Definition of done:**
- [ ] Ceph s3-tests Phase 1 tests mostly pass (>80%)
- [ ] Snowflake s3compat 9/9 pass
- [ ] MinIO Mint aws-cli tests pass
- [ ] All 75 BleepStore E2E tests still pass (no regressions)
- [ ] Smoke test still passes (20/20)

**Dependencies to add:** None.

---

## Milestone 7: Cloud Storage Backends (Stages 10-11b)

### Stage 10: AWS S3 Gateway Backend ✅

**Goal:** AWS S3 storage backend proxying data operations to a real S3 bucket.

**Files to modify:**
- `golang/internal/storage/aws.go` -- Full implementation using `aws-sdk-go-v2`. Initialize client with `config.LoadDefaultConfig(ctx)`. Implement all `StorageBackend` methods by mapping to AWS S3 API calls. Key mapping: `{prefix}{bleepstore_bucket}/{key}`.
- `golang/internal/storage/factory.go` -- **(new file)** Backend factory: read `storage.backend` from config, return `LocalBackend`, `AWSGatewayBackend`, etc.
- `golang/internal/config/config.go` -- Ensure `StorageConfig` fields for AWS (`AWSBucket`, `AWSRegion`) are correctly mapped.
- `golang/cmd/bleepstore/main.go` -- Use factory to create backend.

**Library-specific notes:**
- **aws-sdk-go-v2** is the current AWS SDK for Go. Use modular packages:
  - `github.com/aws/aws-sdk-go-v2/config` -- Load default config.
  - `github.com/aws/aws-sdk-go-v2/service/s3` -- S3 client.
- Credential chain: env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), shared config, IAM role -- all handled by `config.LoadDefaultConfig`.
- Multipart passthrough: use AWS `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload` directly. AWS upload IDs passed through.
- Error mapping: use `smithy` error types to match AWS error codes.

**Go idioms:**
- AWS SDK v2 uses `context.Context` throughout -- aligns with our interface.
- Error type assertion: `var ae smithy.APIError; errors.As(err, &ae)` to get error code.
- `*s3.Client` as field on `AWSGatewayBackend`.

**Build/run commands:**
```bash
go test -v -race ./internal/storage/
# E2E with AWS backend (requires AWS credentials and config):
BLEEPSTORE_BACKEND=aws go build -o bleepstore ./cmd/bleepstore
./bleepstore -config aws-config.yaml -port 9000
cd ../tests && python -m pytest e2e/ -v
```

**Dependencies to add:**
```
github.com/aws/aws-sdk-go-v2
github.com/aws/aws-sdk-go-v2/config
github.com/aws/aws-sdk-go-v2/service/s3
```

---

### Stage 11a: GCP Cloud Storage Backend ✅

**Goal:** Implement GCP Cloud Storage backend. Two cloud backends (AWS + GCP) pass the E2E suite.

**Files to modify:**
- `golang/internal/storage/gcp.go` -- Full implementation using `cloud.google.com/go/storage`. Use `storage.NewClient(ctx)` with Application Default Credentials. Multipart via GCS `Compose` (max 32 sources per call; implement recursive compose for >32 parts).
- `golang/internal/storage/factory.go` -- Add `"gcp"` case to factory.

**Library-specific notes:**
- **GCS ETag handling:** GCS returns `md5Hash` as base64. Convert to hex:
  ```go
  raw, _ := base64.StdEncoding.DecodeString(attrs.MD5)
  etag := fmt.Sprintf(`"%x"`, raw)
  ```
- **GCS Compose limit:** 32 sources per call. For >32 parts, implement tree-based recursive composition.

**Go idioms:**
- GCS SDK uses `context.Context` throughout.
- GCS: `client.Bucket(name).Object(key).NewWriter(ctx)` / `.NewReader(ctx)`.

**Build/run commands:**
```bash
BLEEPSTORE_BACKEND=gcp ./bleepstore -config gcp-config.yaml -port 9000
cd ../tests && python -m pytest e2e/ -v
```

**Test targets:**
- All 75 E2E tests pass with `BLEEPSTORE_BACKEND=gcp`

**Definition of done:**
- [x] GCP backend implements full `StorageBackend` interface
- [x] All 75 E2E tests pass with `BLEEPSTORE_BACKEND=gcp`
- [x] GCS compose-based multipart works for >32 parts
- [x] Backend error mapping utility covers GCS error codes

**Dependencies to add:**
```
cloud.google.com/go/storage
google.golang.org/api
```

---

### Stage 11b: Azure Blob Storage Backend ✅

**Goal:** Implement Azure Blob Storage backend. All three cloud backends (AWS, GCP, Azure) pass the E2E suite.

**Files to modify:**
- `golang/internal/storage/azure.go` -- Full implementation using `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob`. Use `azblob.NewClient` with `azidentity.NewDefaultAzureCredential`. Multipart via block blobs: `StageBlock` + `CommitBlockList`. Block IDs: base64-encode zero-padded part number.
- `golang/internal/storage/factory.go` -- Add `"azure"` case to factory.

**Library-specific notes:**
- **Azure block IDs:** Must all be the same length, base64-encoded:
  ```go
  blockID := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%05d", partNumber)))
  ```
- **Azure abort:** Uncommitted blocks auto-expire in 7 days, so `AbortMultipartUpload` on the storage side is mostly a no-op for Azure.

**Go idioms:**
- Azure SDK uses `context.Context` throughout.
- Azure: `client.UploadStream(ctx, container, blob, body, nil)`.

**Build/run commands:**
```bash
BLEEPSTORE_BACKEND=azure ./bleepstore -config azure-config.yaml -port 9000
cd ../tests && python -m pytest e2e/ -v
```

**Test targets:**
- All 75 E2E tests pass with `BLEEPSTORE_BACKEND=azure`

**Definition of done:**
- [x] Azure backend implements full `StorageBackend` interface
- [x] All 86 E2E tests pass (85/86 — same Go runtime limitation as before)
- [x] Azure block blob-based multipart works (StageBlock + CommitBlockList)
- [x] Backend error mapping covers Azure error codes

**Dependencies to add:**
```
github.com/Azure/azure-sdk-for-go/sdk/storage/azblob
github.com/Azure/azure-sdk-for-go/sdk/azidentity
```

---

## Milestone 8: Cluster Mode (Stages 12a-14)

### Stage 12a: Raft State Machine & Storage

**Goal:** Implement the core Raft state machine (FSM), log entry types, command serialization, and persistent storage. The FSM handles state transitions and log management in isolation (no networking yet). Uses `hashicorp/raft` library.

**Files to modify:**
- `golang/internal/cluster/fsm.go` -- **(new file)** `MetadataFSM` implementing `raft.FSM`: `Apply(log *raft.Log)`, `Snapshot()`, `Restore(io.ReadCloser)`. Log entry types as enum. Deserialize commands, apply to SQLite.
- `golang/internal/cluster/commands.go` -- **(new file)** Define command types (CreateBucket, DeleteBucket, PutObjectMeta, DeleteObjectMeta, DeleteObjectsMeta, PutBucketAcl, PutObjectAcl, CreateMultipartUpload, RegisterPart, CompleteMultipartUpload, AbortMultipartUpload), serialization (JSON or `encoding/gob`).
- `golang/internal/cluster/raft_test.go` -- **(new file)** Unit tests for FSM and commands.

**Library-specific notes:**
- **`github.com/hashicorp/raft`** is the de facto Raft library for Go. Battle-tested in Consul, Nomad, Vault.
- **`github.com/hashicorp/raft-boltdb/v2`** for persistent log and stable store.
- `raft.FSM` interface: `Apply(*raft.Log) interface{}`, `Snapshot() (raft.FSMSnapshot, error)`, `Restore(io.ReadCloser) error`.
- Log entry serialization: JSON for simplicity with `encoding/json`.

**Go idioms:**
- Use `raft.ServerID` and `raft.ServerAddress` types.
- Command dispatch via `switch` on a `CommandType` enum.
- `raft.FSMSnapshot` interface: `Persist(sink raft.SnapshotSink) error`, `Release()`.

**Unit test approach:**
- File: `golang/internal/cluster/raft_test.go`
- Use `raft.NewInmemStore` for log/stable store in tests.
- Test FSM apply: serialize command -> apply via FSM -> verify SQLite state.
- Test snapshot/restore round-trip.
- Test command serialization/deserialization round-trip.

**Build/run commands:**
```bash
go test -v -race ./internal/cluster/
```

**Test targets:**
- FSM state machine correctly applies all command types
- Log entry types defined and serializable
- Persistent storage for Raft state (via BoltDB)
- Snapshot creation and restore produce identical SQLite state

**Definition of done:**
- [ ] Raft FSM correctly applies all log entry types to SQLite
- [ ] Log entry command types defined and serializable
- [ ] BoltDB-based persistent storage for log and stable store
- [ ] Snapshot creates SQLite database copy; restore replaces local state
- [ ] Unit tests cover FSM apply, snapshot/restore, command serialization

**Dependencies to add:**
```
github.com/hashicorp/raft
github.com/hashicorp/raft-boltdb/v2
go.etcd.io/bbolt   # transitive dependency of raft-boltdb
```

---

### Stage 12b: Raft Networking & Elections

**Goal:** Wire `hashicorp/raft` with TCP transport for leader election and log replication across 3 nodes over the network.

**Files to modify:**
- `golang/internal/cluster/raft.go` -- Wire `hashicorp/raft` library: create `raft.Raft` instance with TCP transport, BoltDB log/stable store, file snapshot store. Expose `Apply`, `LeaderAddr`, `State`, `Stats` methods.
- `golang/internal/cluster/transport.go` -- **(new file)** Configure `raft.NewTCPTransport`.
- `golang/internal/config/config.go` -- Ensure `ClusterConfig` fields for `Enabled`, `NodeID`, `BindAddr`, `Peers` are mapped.

**Library-specific notes:**
- `raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)`.
- `raft.DefaultConfig()` provides sensible defaults; override `ElectionTimeout`, `HeartbeatTimeout`.
- Bootstrap: `raft.BootstrapCluster(config, logStore, stableStore, snapshotStore, transport, servers)` for first-time cluster init.
- `raft.NewTCPTransport(bindAddr, advertise, maxPool, timeout, logOutput)` for production transport.

**Go idioms:**
- Goroutine for Raft background operations (election, heartbeat) managed by the `hashicorp/raft` library.
- Channel-based futures: `applyFuture := r.Apply(data, timeout); if err := applyFuture.Error(); err != nil { ... }`.

**Unit test approach:**
- File: `golang/internal/cluster/raft_test.go`
- Use `raft.NewInmemTransport` for multi-node tests (no real TCP needed in unit tests).
- Spin up 3 in-memory Raft nodes, verify leader election.
- Apply log entries on leader, verify replication to followers.
- Kill leader, verify re-election.
- Test heartbeats prevent unnecessary elections.

**Build/run commands:**
```bash
go test -v -race -timeout 60s ./internal/cluster/
```

**Test targets:**
- Three-node election: one leader elected within timeout
- Log replication: leader's entries replicated to followers
- Leader failure triggers new election
- Log consistency: follower with missing entries catches up

**Definition of done:**
- [ ] Leader election works with 3 nodes (in-memory transport for tests)
- [ ] Log replication works (entries committed to majority)
- [ ] Heartbeats prevent unnecessary elections
- [ ] Leader failure triggers re-election
- [ ] Integration tests cover multi-node Raft scenarios

**Dependencies to add:** None beyond Stage 12a.

---

### Stage 13a: Raft-Metadata Wiring

**Goal:** Wire the Raft consensus layer to the metadata store. Metadata writes go through the Raft log. Reads served from local SQLite replica. Write forwarding from followers to leader.

**Files to modify:**
- `golang/internal/metadata/raft_store.go` -- **(new file)** `RaftMetadataStore` wrapping `SQLiteStore`. Write methods serialize command and call `raft.Apply`. Read methods delegate directly to `SQLiteStore`.
- `golang/internal/cluster/raft.go` -- Add write forwarding: if not leader, HTTP-forward the request to the leader address. Use `httputil.NewSingleHostReverseProxy(leaderURL)`.
- `golang/internal/server/server.go` -- If `cluster.enabled`, use `RaftMetadataStore` instead of `SQLiteStore`.

**Library-specific notes:**
- `raft.Apply(cmd, timeout)` returns a `raft.ApplyFuture`. Call `.Error()` to check for errors and `.Response()` for the FSM result.
- Write forwarding: use `net/http` to proxy the request to leader. `httputil.NewSingleHostReverseProxy(leaderURL)`.
- `raft.ErrNotLeader`: returned when `Apply` is called on a non-leader node.

**Go idioms:**
- Interface composition: `RaftMetadataStore` embeds `MetadataStore` interface, delegates reads, intercepts writes.
- `select` with context cancellation for timeouts.

**Unit test approach:**
- 3-node in-memory cluster test.
- Write on leader, read on follower, verify consistency.
- Kill leader, verify new leader, writes resume.
- Follower forwards writes to leader transparently.

**Build/run commands:**
```bash
go test -v -race -timeout 60s ./internal/cluster/ ./internal/metadata/
```

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

**Dependencies to add:** None beyond Stage 12a.

---

### Stage 13b: Snapshots & Node Management

**Goal:** Implement log compaction via snapshots, InstallSnapshot RPC, and dynamic node join/leave.

**Files to modify:**
- `golang/internal/cluster/fsm.go` -- Implement `Snapshot`: copy SQLite database file. `Restore`: replace local SQLite with received snapshot.
- `golang/internal/cluster/raft.go` -- Add snapshot trigger (every N committed entries), node add/remove methods.

**Library-specific notes:**
- Snapshot: `raft.FileSnapshotStore` for production, configurable retain count.
- `hashicorp/raft` handles `InstallSnapshot` RPC automatically via the transport layer.
- `raft.AddVoter(id, addr, prevIndex, timeout)` to add a node.
- `raft.RemoveServer(id, prevIndex, timeout)` to remove.
- `raft.Snapshot()` to trigger manual snapshot.

**Go idioms:**
- `sync.Once` for one-time initialization of snapshot store.
- SQLite snapshot = copy of database file (simple, not incremental).

**Unit test approach:**
- Snapshot created after configured number of entries.
- New node joins and syncs via snapshot transfer.
- Node offline for extended period catches up via snapshot.
- Log entries before snapshot index are discarded.
- Node leave: removed from configuration, cluster continues.

**Build/run commands:**
```bash
go test -v -race -timeout 60s ./internal/cluster/ ./internal/metadata/
```

**Test targets:**
- Snapshot creation and restore produce identical state
- New node joins via snapshot transfer
- Node leave removes from cluster configuration
- Log compaction discards old entries

**Definition of done:**
- [ ] Log compaction/snapshotting works
- [ ] InstallSnapshot transfers full database to lagging nodes
- [ ] New node can join and sync via snapshot
- [ ] Node leave removes from cluster configuration
- [ ] Snapshot-based recovery for nodes that missed too many entries

**Dependencies to add:** None beyond Stage 12a.

---

### Stage 14: Cluster Operations & Admin API

**Goal:** Admin API for cluster management. Multi-node E2E testing.

**Files to modify:**
- `golang/internal/server/admin.go` -- **(new file)** Admin API handlers on separate `http.ServeMux`:
  - `GET /admin/cluster/status`
  - `GET /admin/cluster/nodes`
  - `POST /admin/cluster/nodes`
  - `DELETE /admin/cluster/nodes/{id}`
  - `GET /admin/cluster/raft/stats`
  - `POST /admin/cluster/raft/snapshot`
  Bearer token auth middleware.
- `golang/cmd/bleepstore/main.go` -- Start admin HTTP server on `server.admin_port` (default 9001) if cluster enabled.
- `golang/internal/server/server.go` -- Enhance `/health` to include cluster state in cluster mode.
- `golang/internal/cluster/raft.go` -- Expose stats, add/remove node methods.

**Library-specific notes:**
- `raft.Stats()` returns `map[string]string` with protocol statistics.
- `raft.AddVoter(id, addr, prevIndex, timeout)` to add a node.
- `raft.RemoveServer(id, prevIndex, timeout)` to remove.
- `raft.Snapshot()` to trigger manual snapshot.
- Admin API uses `encoding/json` for responses.

**Go idioms:**
- Separate `http.Server` for admin port.
- Bearer token check: `strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")`.
- `encoding/json.NewEncoder(w).Encode(data)` for JSON responses.

**Unit test approach:**
- Admin endpoint tests with `httptest`.
- Multi-node integration test script (shell or Go `TestMain`).

**Build/run commands:**
```bash
go test -v -race ./internal/server/
# Multi-node local test:
./bleepstore -config node1.yaml -port 9000 &
./bleepstore -config node2.yaml -port 9002 &
./bleepstore -config node3.yaml -port 9004 &
cd ../tests && python -m pytest e2e/ -v
```

**Dependencies to add:** None.

---

## Milestone 9: Performance & Hardening (Stage 15)

### Stage 15: Performance Optimization & Production Readiness

**Goal:** Production-ready. Startup < 1s, memory < 50MB, throughput within 2x of MinIO.

**Files to modify:**
- `golang/cmd/bleepstore/main.go` -- Add HTTP server timeouts (`ReadTimeout`, `WriteTimeout`, `IdleTimeout`, `MaxHeaderBytes`). Add structured logging setup.
- `golang/internal/server/server.go` -- Request logging middleware (method, path, status, duration, request_id). Connection limit via `netutil.LimitListener` or custom.
- `golang/internal/auth/sigv4.go` -- Add signing key cache (keyed by date+region+service, `sync.Map` or simple map with mutex).
- `golang/internal/metadata/sqlite.go` -- Use prepared statements (`db.PrepareContext`) for hot-path queries. Store as struct fields, close on `Close()`.
- `golang/internal/storage/local.go` -- Ensure streaming I/O (no full buffering). Verify `io.Copy` is used throughout.
- `golang/internal/handlers/object.go` -- Verify `GetObject` streams directly from file to `ResponseWriter` without intermediate buffer.
- `golang/internal/server/logging.go` -- **(new file)** Structured logging middleware. Use `log/slog` (Go 1.21+) for structured JSON output.

**Library-specific notes:**
- **`log/slog`** (stdlib since Go 1.21): structured logging with JSON handler:
  ```go
  handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
  slog.SetDefault(slog.New(handler))
  ```
- **Prepared statements:** `stmt, _ := db.PrepareContext(ctx, "SELECT ...")`, reuse across requests.
- **`net/http` timeouts:**
  ```go
  srv := &http.Server{
      ReadTimeout:    60 * time.Second,
      WriteTimeout:   60 * time.Second,
      IdleTimeout:    120 * time.Second,
      MaxHeaderBytes: 1 << 20, // 1 MB
  }
  ```
- **Benchmarking:** Use Go's built-in `testing.B` for micro-benchmarks:
  ```go
  func BenchmarkPutObject(b *testing.B) { ... }
  ```

**Go idioms:**
- Graceful shutdown with `context.WithTimeout` for draining:
  ```go
  ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
  defer cancel()
  srv.Shutdown(ctx)
  ```
- `sync.Pool` for reusing buffers in hot paths.
- `runtime.GOMAXPROCS` defaults to all CPUs -- good for concurrent request handling.
- `go tool pprof` for CPU/memory profiling.

**Unit test approach:**
- Benchmark tests: `func BenchmarkGetObject(b *testing.B)` in handlers and storage packages.
- Memory allocation tests: `b.ReportAllocs()`.
- Race detector: `go test -race ./...` (should already pass from previous stages).

**Build/run commands:**
```bash
# Build optimized binary
go build -ldflags="-s -w" -o bleepstore ./cmd/bleepstore

# Run benchmarks
go test -bench=. -benchmem ./internal/storage/ ./internal/handlers/

# Profile
go test -cpuprofile cpu.prof -memprofile mem.prof -bench=. ./internal/handlers/
go tool pprof cpu.prof

# MinIO Warp benchmark
warp mixed --host=localhost:9000 --access-key=bleepstore --secret-key=bleepstore-secret --duration=60s

# Performance E2E tests
cd ../tests && python -m pytest performance/ -v

# Startup time measurement
time ./bleepstore -config ../bleepstore.example.yaml -port 9000 &
# kill immediately after listening message

# Memory measurement
./bleepstore -config ../bleepstore.example.yaml -port 9000 &
sleep 2 && ps -o rss= -p $!   # should be < 50MB (in KB)
```

**Dependencies to add:** None.

---

## Milestone 10: Event Infrastructure (Stages 16a-16c)

### Stage 16a: Queue Interface & Redis Backend

**Goal:** Define the QueueBackend interface, event types/envelope, and implement the Redis Streams backend with write-through mode.

**Files to create/modify:**

| File | Work |
|---|---|
| `golang/internal/queue/backend.go` | `QueueBackend` interface with `Connect`, `Close`, `HealthCheck`, `Publish`, `PublishBatch`, `Subscribe`, `Acknowledge`, `EnqueueTask`, `DequeueTask`, `CompleteTask`, `FailTask`, `RetryFailedTasks` |
| `golang/internal/queue/redis.go` | Redis Streams implementation using `github.com/redis/go-redis/v9` |
| `golang/internal/queue/events.go` | Event types, `Event` struct (`ID`, `Type`, `Timestamp`, `Source`, `RequestID`, `Data`) |
| `golang/internal/config/config.go` | Add `QueueConfig` struct: `Enabled`, `Backend`, `Consistency`, Redis sub-config |
| `golang/internal/server/server.go` | Initialize queue backend on startup, reconnect/reprocess pending on restart |
| All handler files | Publish events after successful writes (write-through mode) |

**Key patterns:**

- **go-redis** for Redis Streams:
  ```go
  rdb := redis.NewClient(&redis.Options{Addr: url})
  rdb.XAdd(ctx, &redis.XAddArgs{Stream: "bleepstore:events", Values: map[string]interface{}{"type": "object.created", "data": eventJSON}})
  ```

- **Write-through mode** (default when queue enabled): normal direct write path (storage + metadata), then publish event to queue after commit (fire-and-forget). Queue failure does not block the write.
- **Crash-only:** startup reconnects to queue, reprocesses pending tasks. No special recovery flag -- every startup is recovery.

**Library-specific notes:**
- `github.com/redis/go-redis/v9` uses `context.Context` throughout and supports Redis Streams via `XAdd`, `XReadGroup`, `XAck`.

**Go idioms:**
- Interface-based backend selection via factory pattern (similar to `storage/factory.go`):
  ```go
  func NewQueueBackend(cfg config.QueueConfig) (QueueBackend, error) {
      switch cfg.Backend {
      case "redis":
          return NewRedisBackend(cfg.Redis)
      // ...
      }
  }
  ```
- Use `context.Context` for all queue operations to support cancellation and timeouts.
- Event publishing in handlers: call `queue.Publish(ctx, event)` after successful write operations.

**Unit test approach:**
- Mock queue backends using the `QueueBackend` interface.
- Table-driven tests for event serialization/deserialization.
- Integration tests with testcontainers (optional): spin up Redis in Docker.
- Test write-through mode: event published after successful write.

**Build/run commands:**
```bash
go test -v -race ./internal/queue/
# With integration tests (requires Docker):
go test -v -race -tags=integration ./internal/queue/
```

**Test targets:**
- All 75 E2E tests pass with Redis queue enabled (write-through mode)
- Events published for each write operation
- Queue unavailable at startup: BleepStore starts in degraded mode

**Definition of done:**
- [ ] QueueBackend interface defined
- [ ] Redis backend implemented (publish, subscribe, acknowledge, dead letter)
- [ ] Event types and envelope defined
- [ ] Write-through mode works: events published after successful writes
- [ ] All 75 E2E tests pass with Redis queue enabled
- [ ] Configuration section for queue settings
- [ ] Health check reports queue status

**Dependencies to add:**

| Package | Purpose |
|---|---|
| `github.com/redis/go-redis/v9` | Redis Streams |

---

### Stage 16b: RabbitMQ Backend

**Goal:** Implement the RabbitMQ/AMQP backend using the QueueBackend interface established in 16a.

**Files to create/modify:**

| File | Work |
|---|---|
| `golang/internal/queue/rabbitmq.go` | RabbitMQ implementation using `github.com/rabbitmq/amqp091-go` |
| `golang/internal/config/config.go` | Add RabbitMQ sub-config to `QueueConfig` |

**Key patterns:**

- **amqp091-go** for RabbitMQ:
  ```go
  conn, _ := amqp091.Dial(url)
  ch, _ := conn.Channel()
  ch.Publish("bleepstore", "object.created", false, false, amqp091.Publishing{Body: eventJSON})
  ```

- Topic exchange for event routing by type.
- Durable queues with manual ack.
- Dead letter exchange for failed messages.
- Compatible with ActiveMQ via AMQP 0-9-1.

**Library-specific notes:**
- `github.com/rabbitmq/amqp091-go` is the maintained AMQP 0-9-1 client (fork of `streadway/amqp`). Use `Channel.PublishWithContext` for context-aware publishing.
- Automatic reconnection on connection loss.

**Go idioms:**
- AMQP connection and channel lifecycle management.
- Queue and exchange declaration (idempotent).
- Routing keys based on event type (e.g., `bucket.created`, `object.deleted`).

**Unit test approach:**
- RabbitMQ backend: publish, subscribe, acknowledge, dead letter.
- Exchange and queue declaration.
- Integration tests with testcontainers (optional): spin up RabbitMQ in Docker.

**Build/run commands:**
```bash
go test -v -race ./internal/queue/
# With integration tests (requires Docker):
go test -v -race -tags=integration ./internal/queue/
```

**Test targets:**
- All 75 E2E tests pass with RabbitMQ queue enabled (write-through mode)
- Events routed correctly by type

**Definition of done:**
- [ ] RabbitMQ backend implements full QueueBackend interface
- [ ] All 75 E2E tests pass with RabbitMQ queue enabled (write-through mode)
- [ ] Dead letter exchange handles failed messages
- [ ] Compatible with AMQP 0-9-1 (ActiveMQ compatible)

**Dependencies to add:**

| Package | Purpose |
|---|---|
| `github.com/rabbitmq/amqp091-go` | RabbitMQ/AMQP |

---

### Stage 16c: Kafka Backend & Consistency Modes

**Goal:** Implement the Kafka backend and the sync/async consistency modes. All three queue backends support all three consistency modes.

**Files to create/modify:**

| File | Work |
|---|---|
| `golang/internal/queue/kafka.go` | Kafka implementation using `github.com/segmentio/kafka-go` |
| `golang/internal/config/config.go` | Add Kafka sub-config to `QueueConfig` |
| All handler files | Add sync/async consistency mode logic |

**Key patterns:**

- **kafka-go** for Kafka:
  ```go
  w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: "bleepstore.object.created"}
  w.WriteMessages(ctx, kafka.Message{Value: eventJSON})
  ```

- Topics per event type (e.g., `bleepstore.object.created`).
- Consumer groups for parallel processing.
- `acks=all` for durability.
- Partitioned by bucket name for ordering within a bucket.

**Consistency modes (all backends):**
- **write-through** (default): normal write, then publish event (already implemented in 16a).
- **sync**: handler writes to temp file, enqueues WriteTask, blocks until consumer completes.
- **async**: handler writes to temp file, enqueues WriteTask, responds 202 Accepted immediately.

**Library-specific notes:**
- `github.com/segmentio/kafka-go` provides a high-level `Writer` and `Reader` with automatic partition balancing.
- Kafka requires `acks=all` for crash-only safety.

**Go idioms:**
- Sync mode timeout: configurable, returns 504 Gateway Timeout if consumer doesn't complete in time.
- Async mode: 202 Accepted with `Location` header for eventual GET.
- Mode switching in handler middleware.

**Unit test approach:**
- Kafka backend: publish, subscribe, acknowledge.
- Sync mode: handler blocks until task completed.
- Async mode: handler returns 202, task processed asynchronously.
- Integration tests with testcontainers (optional): spin up Kafka in Docker.

**Build/run commands:**
```bash
go test -v -race ./internal/queue/
# With integration tests (requires Docker):
go test -v -race -tags=integration ./internal/queue/
```

**Test targets:**
- All 75 E2E tests pass with Kafka queue enabled (write-through mode)
- Sync mode: write completes only after consumer processes task
- Async mode: write returns 202, object eventually available
- Crash recovery: pending tasks survive restarts

**Definition of done:**
- [ ] Kafka backend implements full QueueBackend interface
- [ ] Sync mode: writes blocked until queue consumer completes (all backends)
- [ ] Async mode: writes return 202, processed asynchronously (all backends)
- [ ] All 75 E2E tests pass with Kafka queue enabled (write-through mode)
- [ ] Crash-only: pending tasks survive restarts, orphan temp files cleaned
- [ ] All three backends support all three consistency modes

**Dependencies to add:**

| Package | Purpose |
|---|---|
| `github.com/segmentio/kafka-go` | Apache Kafka |

---

## Summary: File Map

| Package | Key Files | Purpose |
|---|---|---|
| `cmd/bleepstore` | `main.go` | Entry point, config, signal handling, server startup |
| `internal/config` | `config.go` | YAML config loading, defaults |
| `internal/server` | `server.go`, `admin.go`, `logging.go` | HTTP routing, middleware, admin API |
| `internal/errors` | `errors.go` | S3 error types and codes |
| `internal/xmlutil` | `xmlutil.go` | XML response types and rendering |
| `internal/metrics` | `metrics.go` | Custom Prometheus metric definitions |
| `internal/handlers` | `bucket.go`, `object.go`, `multipart.go`, `helpers.go` | S3 operation handlers |
| `internal/auth` | `sigv4.go`, `middleware.go` | SigV4 verification, auth middleware |
| `internal/metadata` | `store.go`, `sqlite.go`, `raft_store.go` | Metadata interface, SQLite impl, Raft wrapper |
| `internal/storage` | `backend.go`, `local.go`, `aws.go`, `gcp.go`, `azure.go`, `factory.go` | Storage interface, all backend implementations |
| `internal/cluster` | `raft.go`, `fsm.go`, `transport.go`, `commands.go` | Raft consensus, FSM, commands |
| `internal/queue` | `backend.go`, `redis.go`, `rabbitmq.go`, `kafka.go`, `events.go` | Queue interface, Redis/RabbitMQ/Kafka backends, event types |

## Summary: Dependencies by Stage

| Stage | New Dependencies |
|---|---|
| 1 | *(none -- stdlib + yaml.v3)* |
| 1b | `github.com/danielgtaylor/huma/v2`, `github.com/go-chi/chi/v5`, `github.com/prometheus/client_golang` |
| 2 | `modernc.org/sqlite` (or `github.com/mattn/go-sqlite3`) |
| 3-6 | *(none)* |
| 7 | `github.com/google/uuid` *(optional)* |
| 8-9b | *(none)* |
| 10 | `github.com/aws/aws-sdk-go-v2`, `aws-sdk-go-v2/config`, `aws-sdk-go-v2/service/s3` |
| 11a | `cloud.google.com/go/storage`, `google.golang.org/api` |
| 11b | `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob`, `github.com/Azure/azure-sdk-for-go/sdk/azidentity` |
| 12a | `github.com/hashicorp/raft`, `github.com/hashicorp/raft-boltdb/v2` |
| 12b-15 | *(none)* |
| 16a | `github.com/redis/go-redis/v9` |
| 16b | `github.com/rabbitmq/amqp091-go` |
| 16c | `github.com/segmentio/kafka-go` |
