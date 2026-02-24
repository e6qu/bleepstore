# Stage 11a: GCP Cloud Storage Gateway Backend

**Completed:** 2026-02-23

## What Was Implemented

Full GCP Cloud Storage gateway backend implementing all 12 `StorageBackend` protocol methods via `gcloud-aio-storage`.

## Files Changed

| File | Action | Lines |
|------|--------|-------|
| `pyproject.toml` | Modified | Added `gcp = ["gcloud-aio-storage>=9.0.0"]` optional dep + dev dep |
| `src/bleepstore/storage/gcp.py` | Rewritten | ~280 lines, full implementation |
| `src/bleepstore/server.py` | Modified | Added `elif backend == "gcp":` factory branch |
| `tests/test_storage_gcp.py` | Created | ~330 lines, 43 unit tests |

## Key Decisions

1. **SDK**: `gcloud-aio-storage` (async GCS client over aiohttp) — matches project's async-first architecture
2. **Key mapping**: `{prefix}{bleepstore_bucket}/{key}` — same pattern as AWS backend
3. **Parts mapping**: `{prefix}.parts/{upload_id}/{part_number}` — temporary GCS objects
4. **Multipart assembly**: GCS `compose()` with chaining for >32 parts:
   - Batch sources into groups of 32, compose each batch into an intermediate object
   - Repeat until ≤32 sources remain, then final compose
   - Clean up intermediate composite objects after assembly
5. **ETag**: Compute MD5 locally in `put()`, `put_part()`, `copy_object()`, and `assemble_parts()` for consistency (don't rely on GCS md5Hash which is base64)
6. **Error mapping**: `_is_not_found()` helper checks `exc.status == 404` or "404"/"not found" in message — maps to `FileNotFoundError`
7. **Delete idempotency**: GCS errors on delete of non-existent objects (unlike S3) — catch 404 silently
8. **exists()**: Uses `download()` with `Range: bytes=0-0` header to avoid downloading full objects
9. **copy_object()**: Uses GCS server-side `copy()` then downloads result to compute MD5
10. **Session management**: `Storage()` created in `init()`, `await client.close()` in `close()`

## Test Results

- **Unit tests**: 542/542 pass (43 new GCP tests)
- **E2E tests**: 86/86 pass (no regressions)

## Test Coverage (43 tests across 14 classes)

- `TestKeyMapping` (5): gcs_name and part_name with/without prefix
- `TestInit` (4): bucket verification, missing bucket, close, close noop
- `TestPut` (3): returns MD5, prefix, empty data
- `TestGet` (3): returns bytes, 404 mapping, error propagation
- `TestGetStream` (4): chunks, offset, offset+length, not found
- `TestDelete` (3): basic, idempotent 404, error propagation
- `TestExists` (4): true, false, Range header usage, error propagation
- `TestCopyObject` (2): server-side copy, prefix
- `TestPutPart` (1): returns MD5
- `TestAssembleParts` (5): single compose, single part, >32 chaining, 64 parts, prefix
- `TestDeleteParts` (3): deletes all, empty, 404 tolerance
- `TestIsNotFound` (4): status checks and message parsing
- `TestServerFactory` (2): validation and instance creation
