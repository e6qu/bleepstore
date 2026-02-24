# Stage 4: Basic Object CRUD

**Completed:** 2026-02-23
**Tests:** 260 total (65 new), all passing

## What Was Implemented

### Local Filesystem Storage Backend (`src/bleepstore/storage/local.py`)
Full implementation of `LocalStorageBackend` following crash-only design:

- **`init()`**: Creates root directory, walks directory tree to clean orphan `.tmp.*` files from interrupted writes (crash recovery)
- **`put(bucket, key, data)`**: Atomic write using temp-fsync-rename pattern. Uses low-level `os.open()`/`os.write()`/`os.fsync()` for reliable data commitment before rename. Computes and returns MD5 hex digest. Cleans up temp file on failure.
- **`get(bucket, key)`**: Returns file bytes via `Path.read_bytes()`. Raises `FileNotFoundError` for missing files.
- **`get_stream(bucket, key, offset, length)`**: Async generator yielding 64 KB chunks. Supports byte offset via `f.seek()` and length limiting via remaining counter.
- **`delete(bucket, key)`**: Removes file (idempotent -- catches `FileNotFoundError`). Cleans up empty parent directories up to the bucket directory.
- **`exists(bucket, key)`**: Returns `Path.is_file()` result.
- **`copy_object(src_bucket, src_key, dst_bucket, dst_key)`**: Read source bytes, write to destination atomically.
- **`put_part()`, `assemble_parts()`, `delete_parts()`**: Multipart part storage for future stages.

Objects stored at `{root}/{bucket}/{key}`. Multipart parts at `{root}/.parts/{upload_id}/{part_number}`.

### Object Handler (`src/bleepstore/handlers/object.py`)
Full implementation of 4 basic S3 object operations:

- **`put_object(request, bucket, key)`**: Validates bucket exists (NoSuchBucket) and key length (KeyTooLongError). Reads request body. Extracts Content-Type (default: `application/octet-stream`), optional content headers, and `x-amz-meta-*` user metadata. Writes to storage atomically, then commits metadata to SQLite. Returns 200 with quoted ETag (`'"' + md5hex + '"'`).
- **`get_object(request, bucket, key)`**: Validates bucket and key exist. Returns `StreamingResponse` streaming body in 64 KB chunks. Response includes ETag, Last-Modified, Content-Length, Content-Type, Accept-Ranges, optional content headers, and user metadata headers.
- **`head_object(request, bucket, key)`**: Same validation as GetObject. Returns 200 with metadata headers and empty body.
- **`delete_object(request, bucket, key)`**: Validates bucket exists. Deletes from storage (logs warning on failure, doesn't block). Deletes metadata. Always returns 204 (idempotent, even for non-existent keys).

Shared helpers:
- `_ensure_bucket_exists()`: Raises NoSuchBucket if bucket not in metadata
- `_extract_user_metadata()`: Parses `x-amz-meta-*` headers
- `_build_object_headers()`: Builds response headers dict from metadata

### Server Updates (`src/bleepstore/server.py`)
- Added `_create_storage_backend()` factory function
- Updated lifespan hook to initialize and close storage backend
- Created `ObjectHandler` instance and wired 4 routes:
  - PUT `/{bucket}/{key}` -> PutObject (or CopyObject if `x-amz-copy-source`)
  - GET `/{bucket}/{key}` -> GetObject
  - HEAD `/{bucket}/{key}` -> HeadObject
  - DELETE `/{bucket}/{key}` -> DeleteObject

## Files Changed
| File | Change |
|------|--------|
| `src/bleepstore/storage/local.py` | Full implementation (was stubs) |
| `src/bleepstore/handlers/object.py` | 4 handlers implemented (was NotImplementedError) |
| `src/bleepstore/server.py` | Storage init + object route wiring |
| `tests/conftest.py` | Added storage backend init to client fixture |
| `tests/test_handlers_bucket.py` | Added storage backend to bucket_client fixture |
| `tests/test_server.py` | Updated for new object route behavior |
| `tests/test_storage_local.py` | **New** -- 23 storage backend unit tests |
| `tests/test_handlers_object.py` | **New** -- 39 object handler integration tests |

## Key Decisions
1. **Atomic writes**: Used `os.open()`/`os.write()`/`os.fsync()` (not Python's `open()`) for guaranteed fsync before rename
2. **ETag quoting**: Always `'"' + md5hex + '"'` per S3 convention
3. **Content-Type default**: `application/octet-stream` when not provided
4. **DeleteObject idempotent**: Always 204, even for non-existent keys
5. **Bucket existence check**: All 4 operations verify bucket exists first (NoSuchBucket)
6. **StreamingResponse**: GetObject uses FastAPI's StreamingResponse for memory-efficient large file handling
7. **Storage factory pattern**: `_create_storage_backend()` in server.py prepares for future AWS/GCP/Azure backends
8. **Test isolation**: Each test gets fresh metadata store + storage backend via tmp_path

## Issues Encountered
- None significant. The implementation was straightforward following existing patterns from bucket handlers.
- Test fixtures needed updating to initialize `app.state.storage` since lifespan doesn't auto-run with httpx's ASGITransport.
