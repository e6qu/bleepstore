# Stage 10: AWS S3 Gateway Backend

**Date:** 2026-02-23
**Status:** COMPLETE

## What Was Implemented

Full AWS S3 gateway storage backend that proxies all data operations to an upstream AWS S3 bucket via aiobotocore (async boto3).

## Key Decisions

1. **SDK**: aiobotocore as optional dependency (`pip install bleepstore[aws]`)
2. **Key mapping**: `{prefix}{bleepstore_bucket}/{key}` — all data in one upstream S3 bucket
3. **Parts mapping**: `{prefix}.parts/{upload_id}/{part_number}` — temporary S3 objects
4. **Multipart assembly**: AWS native multipart upload with `upload_part_copy` (server-side copy, no data download). Single-part uses `copy_object`. Fallback to download+re-upload on `EntityTooSmall`.
5. **ETag**: Compute MD5 locally in `put()` and `put_part()` for consistency
6. **Error mapping**: `NoSuchKey`/404 → `FileNotFoundError` (matches local backend contract)
7. **Credentials**: Standard AWS credential chain (env vars, ~/.aws/credentials, IAM role)

## Files Changed

| File | Action | Lines |
|------|--------|-------|
| `pyproject.toml` | Modified | Added `aws` optional deps, aiobotocore to dev deps |
| `src/bleepstore/storage/aws.py` | Rewritten | ~280 lines, all 12 protocol methods |
| `src/bleepstore/server.py` | Modified | `_create_storage_backend()` → `StorageBackend` return type, `"aws"` branch |
| `tests/test_storage_aws.py` | Created | 35 tests, 11 test classes |

## Test Results

- **Unit tests**: 499/499 pass (35 new)
- **E2E tests**: 85/86 pass (same as before — 1 known test bug)

## Issues Encountered

None — clean implementation. All mocked tests passed after fixing 3 minor test issues:
1. Close test needed to capture context ref before close sets it to None
2. EntityTooSmall fallback test needed 2+ parts to trigger multipart path
3. Regex match pattern needed adjustment for error message format
