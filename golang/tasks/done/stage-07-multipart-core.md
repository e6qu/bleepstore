# Stage 7: Multipart Upload - Core

**Completed:** 2026-02-23

## What Was Implemented

### Handlers (internal/handlers/multipart.go)
- Full rewrite from 6 stub handlers to 5 implemented handlers + 1 stub (CompleteMultipartUpload)
- `NewMultipartHandler(meta, store, ownerID, ownerDisplay)` constructor with dependency injection
- **CreateMultipartUpload**: Validates bucket, extracts content-type/headers/ACL/user-metadata, creates upload record, returns InitiateMultipartUploadResult XML
- **UploadPart**: Validates upload ID + part number (1-10000), writes via store.PutPart (atomic), records metadata, returns ETag. Detects X-Amz-Copy-Source and delegates to uploadPartCopy
- **uploadPartCopy** (private): Copies data from existing object into a part, supports optional X-Amz-Copy-Source-Range, returns CopyPartResult XML
- **AbortMultipartUpload**: Deletes part files (best-effort), deletes metadata, returns 204
- **ListMultipartUploads**: Parses query params (prefix, delimiter, key-marker, upload-id-marker, max-uploads), renders XML
- **ListParts**: Parses pagination params (part-number-marker, max-parts), renders ListPartsResult XML
- **CompleteMultipartUpload**: Still returns 501 NotImplemented (Stage 8)

### XML Types (internal/xmlutil/xmlutil.go)
- Added `CopyPartResult` struct with S3 namespace
- Added `RenderCopyPartResult` function
- All other multipart XML types were already present from scaffolding

### Server Wiring (internal/server/server.go)
- Changed MultipartHandler creation to use `NewMultipartHandler` with full dependency injection

### Tests (internal/handlers/multipart_test.go) - NEW FILE
- 14 test functions covering:
  - Create multipart upload (success + no such bucket)
  - Upload part (success + invalid part numbers + no such upload + overwrite + ETag format)
  - Abort multipart upload (success + no such upload)
  - List multipart uploads (success + prefix filter + no such bucket)
  - List parts (success + no such upload + XML structure)
  - Full lifecycle (create -> upload 3 parts -> abort -> verify cleanup)
  - Content-Type preservation

### Test Updates (internal/server/server_test.go)
- Updated TestS3StubRoutes: multipart routes now return 500 (InternalError, nil deps) instead of 501 (NotImplemented)
- CompleteMultipartUpload remains 501

## Files Changed
| File | Change |
|------|--------|
| `internal/handlers/multipart.go` | Full rewrite: 6 stubs -> 5 implemented + 1 stub |
| `internal/handlers/multipart_test.go` | New: 14 test functions |
| `internal/xmlutil/xmlutil.go` | Added CopyPartResult + RenderCopyPartResult |
| `internal/server/server.go` | Wire MultipartHandler with dependencies |
| `internal/server/server_test.go` | Update expected status codes for multipart routes |

## Files NOT Changed (Already Complete from Previous Stages)
- `internal/storage/local.go` — PutPart, DeleteParts, AssembleParts already implemented (Stage 4)
- `internal/storage/backend.go` — StorageBackend interface already includes multipart methods
- `internal/metadata/store.go` — MetadataStore interface already has all multipart methods
- `internal/metadata/sqlite.go` — All multipart SQLite methods implemented (Stage 2)

## Key Decisions
1. UploadPartCopy handled inside UploadPart (detects X-Amz-Copy-Source header) rather than as a separate dispatch route
2. CopyPartResult added to xmlutil package for consistency with other XML types
3. Nil dependency checks return 500 InternalError (graceful degradation, not panic)
4. Storage delete on abort is best-effort; metadata deletion is authoritative
5. No new external dependencies
6. Owner identity from auth context when available, falls back to handler defaults

## Issues Encountered
1. **CopyPartResult type location**: Initially tried defining inline in handler — moved to xmlutil for consistency
2. **Server test failures**: Multipart routes changed from 501 to 500 after implementation; updated test expectations

## Test Results
- `go build ./cmd/bleepstore` — success
- `go test -v -count=1 ./...` — all tests pass (80+ tests across all packages)
- All 14 new multipart handler tests pass
- All pre-existing tests pass (no regressions)
