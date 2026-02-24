# Stage 4: Basic Object CRUD

**Completed:** 2026-02-23
**Tests:** 80/80 pass (6 new: 4 in local.zig + 2 in object.zig)

## What Was Implemented

### PutObject (`PUT /<bucket>/<key>`)
- Reads request body via `req.body()`
- Checks bucket exists (returns NoSuchBucket if not)
- Writes to local storage via atomic temp-fsync-rename pattern
- Computes MD5 ETag using `std.crypto.hash.Md5`
- Extracts `x-amz-meta-*` user metadata from request headers, stores as JSON
- Gets Content-Type from request header (defaults to `application/octet-stream`)
- Upserts ObjectMeta in SQLite metadata store
- Updates metrics: objects_total gauge, bytes_received counter
- Returns 200 with ETag header

### GetObject (`GET /<bucket>/<key>`)
- Checks bucket exists (NoSuchBucket)
- Gets ObjectMeta from metadata store (NoSuchKey if missing)
- Reads body from local storage backend
- Sets response headers: Content-Type, ETag, Last-Modified (RFC 7231), Content-Length, Accept-Ranges
- Emits user metadata as `x-amz-meta-*` response headers
- Returns body

### HeadObject (`HEAD /<bucket>/<key>`)
- Same metadata lookup as GetObject
- Sets all metadata headers (same as GetObject)
- Returns 200 with no body for existing objects
- Returns 404 with no body (no error XML) for missing objects

### DeleteObject (`DELETE /<bucket>/<key>`)
- Always returns 204 (idempotent)
- Deletes from storage (ignores errors if file missing)
- Deletes from metadata store
- Decrements objects_total metric only if metadata row existed

### Local Storage Backend (Rewritten)
- **Atomic writes**: Write to `<root>/.tmp/<random_hex>`, fsync, rename to final path
- **Crash-only startup**: `cleanTempDir()` removes all files in `.tmp/` on init
- **MD5 ETag**: Proper `std.crypto.hash.Md5` (was incorrectly using SHA256 truncated)
- **Nested keys**: Keys with slashes create subdirectories automatically
- **Idempotent delete**: Ignores FileNotFound
- **createBucket/deleteBucket**: Directory create/remove operations

## Files Changed

| File | Change |
|------|--------|
| `src/storage/local.zig` | Complete rewrite: MD5, atomic write, crash-only startup, 4 tests |
| `src/handlers/object.zig` | Complete rewrite: 4 handlers + helpers + 2 tests |
| `src/server.zig` | Added global_storage_backend, updated Server.init, updated dispatch |
| `src/main.zig` | Added LocalBackend init, pass storage_backend to Server |

## Key Decisions

1. **Global storage backend pointer** (`global_storage_backend`): Same pattern as `global_metadata_store`. Handlers access via import.
2. **User metadata as JSON**: x-amz-meta-* headers extracted, stored as JSON string in metadata, parsed back on GET/HEAD.
3. **RFC 7231 Last-Modified**: ISO 8601 from metadata converted to RFC 7231 format. Day-of-week computed from epoch days: `(epoch_day + 4) % 7`.
4. **httpz header iteration**: No `iterateHeaders()` method. Used `req.headers.keys[]/values[]` parallel arrays.
5. **HeadObject 404**: Plain 404 status with no body (not S3 error XML), matching actual S3 behavior.
6. **putObject takes *tk.Request**: Needs request for body and header access; other handlers only need Response + allocator.

## Issues Encountered

1. **httpz Request API**: `iterateHeaders` does not exist. Discovered correct API (`req.headers.keys`/`req.headers.values`) through compilation errors.
2. **getDaysInMonth type mismatch**: Zig 0.15 changed `getDaysInMonth` signature. Removed unused call.
3. **Optional type on header array**: `req.headers.keys[i]` returns `[]const u8` not optional. Removed `orelse continue`.
