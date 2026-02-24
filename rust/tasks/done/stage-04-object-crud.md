# Stage 4: Basic Object CRUD

**Completed:** 2026-02-23

## Goal
PutObject, GetObject, HeadObject, DeleteObject work with local filesystem backend.

## Files Changed

### src/storage/local.rs (complete rewrite)
- Implemented `put()`: crash-only write path (temp file in `.tmp/{uuid}`, write, fsync, rename). Computes MD5 hash for ETag. Creates parent directories for nested keys.
- Implemented `get()`: reads file, returns `StoredObject` with data + SHA-256 content hash.
- Implemented `delete()`: removes file, idempotent (no error if missing).
- Implemented `exists()`: checks file exists and is a regular file.
- Constructor creates `.tmp` directory on startup.
- `resolve()` method validates path traversal (rejects `..` components).
- 10 unit tests covering all operations.

### src/handlers/object.rs (complete rewrite)
- Implemented `put_object(state, bucket, key, headers, body)`: bucket existence check, extracts Content-Type, user metadata (`x-amz-meta-*`), content-encoding/language/disposition, cache-control, expires, canned ACL. Storage write + metadata record. Returns 200 + ETag.
- Implemented `get_object(state, bucket, key, headers)`: metadata lookup (NoSuchKey), storage read, returns body with all S3 headers (Content-Type, ETag, Content-Length, Last-Modified in RFC 7231, Accept-Ranges, optional headers, user metadata).
- Implemented `head_object(state, bucket, key, headers)`: metadata-only response (no storage read, no body). Returns 404 directly for missing objects.
- Implemented `delete_object(state, bucket, key)`: storage delete (best-effort) + metadata delete (idempotent). Always returns 204.
- Helper functions: `now_iso8601()`, `extract_user_metadata()`, `extract_content_type()`, `iso8601_to_http_date()`, `ymd_to_days()`, `default_acl_json()`, `canned_acl_to_json()`.
- Remaining stubs (Stage 5a/5b): delete_objects, copy_object, list_objects_v2, list_objects_v1, get_object_acl, put_object_acl.

### src/server.rs (dispatch updates)
- `handle_get_object`: passes state, bucket, key, headers to `get_object`.
- `handle_put_object`: added `body: Bytes` extractor, passes to `put_object`.
- `handle_delete_object`: passes state, bucket, key to `delete_object`.
- `handle_head_object`: added `headers: HeaderMap` extractor, passes to `head_object`.

### Cargo.toml
- Added `md-5 = "0.10"` for ETag computation.
- Added `tempfile = "3"` as dev-dependency for unit tests.

## Key Design Decisions

1. **Storage key format**: `{bucket}/{key}` -- simple flat mapping under root directory.
2. **Crash-only writes**: All PutObject writes use temp-fsync-rename pattern through `.tmp/` directory.
3. **ETag**: Quoted hex MD5 hash (e.g., `"d41d8cd98f00b204e9800998ecf8427e"`).
4. **HeadObject optimization**: Reads only from metadata (no storage I/O) since all necessary headers are stored in SQLite.
5. **Last-Modified format**: ISO-8601 stored in metadata, converted to RFC 7231 for HTTP headers via `iso8601_to_http_date()`.
6. **Delete idempotency**: DeleteObject always returns 204, even if the object didn't exist (per S3 spec).
7. **Path traversal protection**: Storage backend rejects keys containing `..` path components.
8. **Default Content-Type**: `application/octet-stream` when no Content-Type header provided.

## Issues Encountered
- None -- clean implementation matching existing patterns from bucket handlers.

## Test Coverage
- 10 new LocalBackend unit tests (put/get roundtrip, empty objects, nested keys, delete existing/nonexistent, exists, get nonexistent error, put overwrites, ETag MD5 verification, bucket lifecycle)
- All existing tests continue to pass (31 metadata + 12 bucket validation + 3 ACL + 2 XML)
