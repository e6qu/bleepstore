# Stage 1: Server Bootstrap & Configuration

**Date:** 2026-02-22
**Status:** Complete (pending manual verification)

## What Was Implemented

### 1. Config Loading (internal/config/config.go)
- Restructured config types to match `bleepstore.example.yaml` YAML nesting
- Added `ServerConfig.Region` field
- Added nested `MetadataConfig.SQLite.Path` (was flat `DSN`)
- Added nested `StorageConfig.Local.RootDir` (was flat `LocalPath`)
- Added `AuthConfig` with `AccessKey` and `SecretKey` (removed `Region` from auth, it's on server)
- Fallback loading: if primary config path fails, tries `bleepstore.example.yaml` in same dir or parent dir
- `defaultConfig()` function returns sensible defaults (port 9000, region us-east-1, etc.)
- `applyDefaults()` fills zero-value fields after YAML unmarshal

### 2. CLI Flag Parsing (cmd/bleepstore/main.go)
- `--config` (default: `config.yaml`)
- `--port` (overrides config value)
- `--host` (overrides config value)

### 3. HTTP Server Startup (internal/server/server.go)
- Uses `net/http.Server` with handler set to `commonHeaders(s.mux)`
- Stored `*http.Server` on Server struct for graceful shutdown
- Added `Shutdown(ctx)` method

### 4. Health Check
- `GET /health` returns `{"status": "ok"}` with `Content-Type: application/json`

### 5. Request Routing
All S3 API paths registered on Go 1.22+ `http.ServeMux`:
- `GET /` -> ListBuckets
- `PUT /{bucket}` -> CreateBucket
- `DELETE /{bucket}` -> DeleteBucket
- `HEAD /{bucket}` -> HeadBucket
- `GET /{bucket}` -> dispatcher (location, acl, uploads, list-type, default=ListObjects)
- `PUT /{bucket}/` -> dispatcher (acl)
- `PUT /{bucket}/{object...}` -> dispatcher (partNumber+uploadId, X-Amz-Copy-Source, acl, default=PutObject)
- `GET /{bucket}/{object...}` -> dispatcher (acl, uploadId=ListParts, default=GetObject)
- `HEAD /{bucket}/{object...}` -> HeadObject
- `DELETE /{bucket}/{object...}` -> dispatcher (uploadId=AbortMultipartUpload, default=DeleteObject)
- `POST /{bucket}/{object...}` -> dispatcher (uploadId=Complete, uploads=Create)
- `POST /{bucket}` -> dispatcher (delete=DeleteObjects)

### 6. S3 Error XML
- All handlers return 501 NotImplemented with proper S3 error XML
- Error XML format: `<Error><Code>NotImplemented</Code><Message>...</Message><Resource>/path</Resource><RequestId>hex16</RequestId></Error>`
- No `xmlns` on error XML (per S3 spec)
- HEAD requests (HeadBucket, HeadObject) return 501 with no body

### 7. Common Response Headers
Every response includes (set by `commonHeaders` middleware):
- `x-amz-request-id`: 16-char hex from `crypto/rand`
- `x-amz-id-2`: same value as request-id (simplified)
- `Date`: RFC 7231 format (`Mon, 02 Jan 2006 15:04:05 GMT`)
- `Server`: `BleepStore`

### 8. Error Codes Added
New error codes added to `internal/errors/errors.go`:
- `NotImplemented` (501)
- `BucketAlreadyOwnedByYou` (409)
- `EntityTooSmall` (400)
- `InvalidAccessKeyId` (403)
- `InvalidArgument` (400)
- `PreconditionFailed` (412)
- `InvalidRange` (416)
- `MissingContentLength` (411)
- `RequestTimeTooSkewed` (403)
- `ServiceUnavailable` (503)

### 9. Time Formatting (internal/xmlutil/xmlutil.go)
- `FormatTimeS3()`: `2006-01-02T15:04:05.000Z` (millisecond precision)
- `FormatTimeHTTP()`: `Mon, 02 Jan 2006 15:04:05 GMT` (RFC 7231)

### 10. Graceful Shutdown (cmd/bleepstore/main.go)
- SIGINT/SIGTERM handler stops accepting new connections
- Waits up to 30 seconds for in-flight requests
- No cleanup on shutdown (crash-only design)

## Files Changed
| File | Changes |
|------|---------|
| `cmd/bleepstore/main.go` | Rewrote with signal handling, graceful shutdown |
| `internal/config/config.go` | Restructured types, fallback loading, defaults |
| `internal/server/server.go` | Health check, middleware, shutdown, improved dispatch |
| `internal/errors/errors.go` | Added 10 new S3 error codes |
| `internal/xmlutil/xmlutil.go` | RenderError with RequestId, WriteErrorResponse, FormatTimeHTTP, fixed FormatTimeS3 |
| `internal/handlers/bucket.go` | All handlers return NotImplemented XML |
| `internal/handlers/object.go` | All handlers return NotImplemented XML |
| `internal/handlers/multipart.go` | All handlers return NotImplemented XML |

## Key Decisions

1. **Request ID format**: 16-char hex from 8 random bytes via `crypto/rand`. Matches AWS-style format. Fallback to timestamp if `crypto/rand` fails (should never happen).

2. **x-amz-id-2**: Set to same value as `x-amz-request-id` for simplicity. Real AWS uses a longer base64-encoded value, but the spec just says "extended request identifier."

3. **Config restructuring**: Changed from flat config (`MetadataConfig.DSN`) to nested structure (`MetadataConfig.SQLite.Path`) to match the actual YAML format in `bleepstore.example.yaml`. This avoids a mismatch between YAML keys and Go struct tags.

4. **HEAD error responses**: Return HTTP 501 status code with no body (per S3 spec: HEAD responses have no body).

5. **Middleware placement**: `commonHeaders` wraps the entire `mux`, including `/health`. This means health check responses also get request ID and Date headers, which is correct behavior.

6. **Fallback dispatch**: Changed `http.NotFound` fallback in sub-resource dispatchers to return proper S3 NotImplemented error XML instead.

## Issues Encountered
- No `go.sum` file exists yet — needs `go mod tidy` to be run before first build
