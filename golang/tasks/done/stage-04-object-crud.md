# Stage 4: Basic Object CRUD

**Date:** 2026-02-23
**Status:** Complete

## What Was Implemented

### 1. internal/storage/local.go (fully implemented)

Full implementation of `LocalBackend` for all object storage operations:

- **PutObject**: Crash-only atomic write pattern: write to temp file in `.tmp/`, compute MD5 via `io.TeeReader`, `fsync`, then `os.Rename` to final path. Returns bytes written and quoted ETag (`"hex-md5"`).
- **GetObject**: Opens file, stats for size, returns `io.ReadCloser` for streaming.
- **DeleteObject**: Removes file, then cleans up empty parent directories up to bucket root.
- **CopyObject**: Opens source, calls PutObject with source reader for atomic copy.
- **PutPart**: Writes part to `.multipart/{uploadID}/{partNumber}` with atomic temp-fsync-rename.
- **AssembleParts**: Concatenates parts into final object file, computes composite ETag, cleans up part directory.
- **DeleteParts**: Removes entire `.multipart/{uploadID}/` directory.
- **CleanTempFiles**: Removes all files in `.tmp/` directory (crash-only recovery on startup).
- **ObjectExists**: Enhanced to check file is not a directory.

Helper functions: `objectPath`, `tempPath`, `cleanEmptyParents`.

### 2. internal/handlers/object.go (4 operations implemented)

Replaced stub implementations with full handlers using constructor injection:

- **NewObjectHandler**: Constructor accepting MetadataStore, StorageBackend, ownerID, ownerDisplay.
- **PutObject**: Validates bucket existence, extracts Content-Type (default: application/octet-stream), user metadata (x-amz-meta-*), content headers, canned ACL. Writes to storage, then commits metadata. Returns ETag header.
- **GetObject**: Validates bucket and object existence, opens storage reader, sets all response headers, streams data to client.
- **HeadObject**: Same as GetObject but no body. Returns all metadata headers (Content-Type, ETag, Last-Modified, Content-Length, Accept-Ranges, user metadata).
- **DeleteObject**: Validates bucket, deletes metadata (authoritative), then storage (best-effort). Always returns 204 (idempotent).

Remaining stubs (Stage 5a/5b): CopyObject, DeleteObjects, ListObjectsV2, ListObjects, GetObjectAcl, PutObjectAcl.

### 3. internal/handlers/helpers.go (new helpers)

- **extractUserMetadata**: Scans request headers for `x-amz-meta-*`, strips prefix, lowercases key.
- **setObjectResponseHeaders**: Sets Content-Type, ETag, Last-Modified, Accept-Ranges, Content-Length, Content-Encoding, Content-Language, Content-Disposition, Cache-Control, Expires, x-amz-storage-class, x-amz-meta-* headers from ObjectRecord.
- **extractObjectKey** (in object.go): Extracts key from URL path after bucket name.

### 4. internal/uid/uid.go (new package)

Unique ID generation using crypto/rand for temp file names.

### 5. internal/server/server.go (updated)

Changed `s.object = &handlers.ObjectHandler{}` to `s.object = handlers.NewObjectHandler(s.meta, s.store, ownerID, ownerDisplay)` for dependency injection.

### 6. cmd/bleepstore/main.go (updated)

Added crash-only recovery step: `storageBackend.CleanTempFiles()` on every startup to remove orphan temp files from incomplete writes.

### 7. Test files

- **internal/handlers/object_test.go** (new): 16 test functions covering PutObject, GetObject, HeadObject, DeleteObject, overwrite, user metadata, default content type, nested keys, empty body, extractObjectKey, extractUserMetadata.
- **internal/storage/local_test.go** (new): 15 test functions covering PutObject, GetObject, DeleteObject, CopyObject, ObjectExists, nested keys, atomic writes, empty directories cleanup, temp file cleanup, idempotent delete, overwrite, empty body.
- **internal/server/server_test.go** (updated): Updated expected status codes for object routes from 501 to 500 (now implemented handlers check for nil deps and return InternalError).

## Key Design Decisions

1. **Atomic writes**: All writes use temp-fsync-rename pattern per crash-only spec. Temp files go to `{rootDir}/.tmp/` directory, not alongside the object.
2. **ETag computation**: MD5 hex digest computed during write via `io.TeeReader`. Always quoted (`"hex"`).
3. **Metadata as authority**: GetObject reads metadata first for headers (size, content-type, etc.), then opens storage for streaming. If file missing but metadata exists, returns 500 (not 404).
4. **Delete order**: Metadata deleted first (authoritative record), storage deletion is best-effort.
5. **Empty directory cleanup**: After DeleteObject, empty parent directories are removed up to (but not including) the bucket root.
6. **Crash recovery**: On startup, `CleanTempFiles()` removes all files in `.tmp/` directory.
7. **Constructor injection**: ObjectHandler follows same pattern as BucketHandler: NewObjectHandler with explicit dependencies.

## Files Changed

- `internal/storage/local.go` -- Full implementation
- `internal/handlers/object.go` -- PutObject, GetObject, HeadObject, DeleteObject implemented
- `internal/handlers/helpers.go` -- Added extractUserMetadata, setObjectResponseHeaders
- `internal/server/server.go` -- Wire ObjectHandler with dependencies
- `cmd/bleepstore/main.go` -- Add temp file cleanup on startup
- `internal/uid/uid.go` -- New package for unique ID generation
- `internal/handlers/object_test.go` -- New test file
- `internal/storage/local_test.go` -- New test file
- `internal/server/server_test.go` -- Updated expectations

## Issues Encountered

- None. Build compiles cleanly. Test file structure follows existing patterns.
