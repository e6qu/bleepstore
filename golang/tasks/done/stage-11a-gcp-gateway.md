# Stage 11a: GCP Cloud Storage Gateway Backend

**Date:** 2026-02-24
**Status:** COMPLETE

## What Was Implemented

### internal/storage/gcp.go (fully implemented from stub)
Complete GCP Cloud Storage gateway backend using `cloud.google.com/go/storage`.

**Interfaces:**
- `GCSAPI`: Mock-friendly interface for GCS client operations (NewWriter, NewReader, Delete, Attrs, Copy, Compose, ListObjects)
- `GCSWriter`: Wraps `io.WriteCloser` for GCS object writers
- `GCSAttrs`: Object attributes (Size, raw MD5 bytes)

**Real client wrapper:**
- `realGCSClient`: Wraps official `*gcs.Client` to satisfy GCSAPI interface

**Backend struct and constructors:**
- `GCPGatewayBackend`: Holds bucket, project, prefix, and GCSAPI client
- `NewGCPGatewayBackend(ctx, bucket, project, prefix)`: Production constructor with ADC
- `NewGCPGatewayBackendWithClient(bucket, project, prefix, client)`: Test constructor

**StorageBackend methods:**
- `PutObject`: Read all + local MD5 + upload via NewWriter
- `GetObject`: Attrs for size + NewReader for streaming
- `DeleteObject`: Idempotent (catches 404)
- `CopyObject`: Server-side copy + download for MD5 ETag
- `PutPart`: Temp GCS object at `.parts/{uploadID}/{partNumber}`
- `AssembleParts`: GCS Compose (single call <=32, chain compose >32)
- `DeleteParts`: ListObjects prefix + individual Delete
- `CreateBucket` / `DeleteBucket`: No-ops
- `ObjectExists`: Attrs API, 404 -> false

**Helper functions:**
- `gcsKey(bucket, key)`: `{prefix}{bucket}/{key}` mapping
- `partKey(uploadID, partNumber)`: `{prefix}.parts/{uploadID}/{partNumber}`
- `chainCompose`: Recursive tree-based composition for >32 parts
- `isGCSNotFound`: Error classification (ErrObjectNotExist, ErrBucketNotExist, message fallback)

### internal/storage/gcp_test.go (new file)
24 test functions with comprehensive mock-based coverage:
- Full put/get round-trip, empty body, not-found errors
- Delete with idempotency, copy with ETag verification
- ObjectExists, CreateBucket/DeleteBucket no-ops
- Key mapping (with and without prefix)
- Part upload, delete, and key mapping
- Single compose (<=32 parts), chain compose (>32 parts)
- ETag consistency (PutObject, PutPart)
- Overwrite behavior
- Interface compliance check
- Error classification tests

### internal/config/config.go (updated)
- Added `GCPPrefix string` field with `yaml:"gcp_prefix"` tag

### cmd/bleepstore/main.go (updated)
- Added `"gcp"` case in backend factory switch
- Validates `gcp_bucket` required, creates GCPGatewayBackend

### go.mod / go.sum (updated)
- Added `cloud.google.com/go/storage v1.60.0`
- Added `google.golang.org/api v0.268.0`
- Plus transitive dependencies

## Key Decisions

1. **GCSAPI interface pattern**: Same mock-friendly approach as AWS S3API. Defined 7 methods covering all GCS operations used. Tests use a full in-memory mock.

2. **Local MD5 computation**: Always compute MD5 locally. GCS may not return MD5 for composite objects. Matches Python reference implementation.

3. **GCS Compose for multipart**: Max 32 sources per call. Recursive tree-based chaining: batch into 32s, compose each batch to intermediate, repeat. Intermediate cleanup after final compose.

4. **Idempotent delete**: GCS errors on deleting non-existent objects. Backend catches 404 and returns nil.

5. **CopyObject downloads for ETag**: Server-side copy, then download to compute MD5. Matches Python reference.

6. **Factory in main.go**: Followed existing pattern (no separate factory.go file).

## Files Changed
- `golang/internal/storage/gcp.go` — full implementation
- `golang/internal/storage/gcp_test.go` — 24 unit tests
- `golang/internal/config/config.go` — GCPPrefix field
- `golang/cmd/bleepstore/main.go` — GCP backend factory case
- `golang/go.mod` / `golang/go.sum` — GCS dependencies

## Test Results
- 226 unit + integration tests passing
- 85/86 E2E tests passing (1 Go runtime limitation)
