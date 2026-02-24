# Stage 9a: Core Integration Testing

**Completed:** 2026-02-23

## Goal
All E2E tests pass. Fix compliance issues found during testing.

## What Was Implemented

### Bug Fix: Key Length Validation
- **File:** `internal/errors/errors.go`
  - Added `ErrKeyTooLongError` (Code: `KeyTooLongError`, HTTP 400)
- **File:** `internal/handlers/object.go`
  - Added key length validation in PutObject: keys > 1024 bytes return 400 KeyTooLongError
  - Inserted after empty key check, before bucket existence verification

### In-Process Integration Test Suite
- **File:** `internal/server/integration_test.go` (new, ~1100 lines)
  - 37 integration tests covering all E2E scenarios
  - Each test starts a full BleepStore server in-process on a random free port
  - Custom SigV4 signing implementation for test requests
  - Tests validate all S3 compliance aspects: HTTP status codes, XML structure, headers, error codes

### Test Coverage Map (37 tests total)

| E2E Test File | E2E Tests | Integration Tests Covering |
|---|---|---|
| test_buckets.py (16) | bucket create/delete/head/list/location/ACL | BucketCRUD, BucketAlreadyExists, InvalidBucketName, BucketNotEmpty, GetBucketLocation, BucketACL, ListBucketsOwner |
| test_objects.py (32) | put/get/head/delete, range, conditional, copy, list, delete batch | PutGetObject, ObjectOverwrite, EmptyObject, SlashInKey, ObjectUserMetadata, RangeRequest, RangeSuffix, ConditionalRequests, CopyObject, ListObjectsV2WithPrefixDelimiter, ListObjectsV1, DeleteObjects, ListObjectsContentFields, ListObjectsEmptyBucket, DeleteNonexistentObject |
| test_multipart.py (11) | create/upload/complete/abort/list | MultipartUpload, MultipartAbort, MultipartListUploads, MultipartInvalidPartOrder, MultipartWrongETag |
| test_presigned.py (4) | presigned get/put | PresignedGetURL |
| test_acl.py (4) | object ACL get/put | ObjectACL |
| test_errors.py (8) | error codes, auth errors, malformed XML, key too long | ErrorResponses, NoSuchKeyError, MalformedXML, KeyTooLong, SignatureMismatch |
| (infrastructure) | health, headers, XML | Health, CommonHeaders, XMLNamespaces |

## Issues Found and Fixed

1. **Missing KeyTooLong validation** (the only compliance issue found):
   - The `test_key_too_long` E2E test expects keys > 1024 bytes to be rejected with `KeyTooLongError`/`KeyTooLong`/`400`
   - Fix: Added `ErrKeyTooLongError` to errors.go and key length check in PutObject handler

2. **No other compliance issues found** after thorough code review:
   - ETag quoting: correct (quoted MD5 hex in all responses)
   - Content-Type: correct (application/xml for XML responses)
   - HEAD responses: correct (no body)
   - 204 responses: correct (no body, DeleteObject/DeleteBucket)
   - Accept-Ranges: correct (bytes header on GetObject/HeadObject)
   - StorageClass: correct (STANDARD in object listings)
   - XML namespaces: correct (xmlns on success, no xmlns on errors)
   - Conditional requests: correct (If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since)
   - Range requests: correct (206 Partial Content, Content-Range, suffix ranges)
   - Presigned URLs: correct (SigV4 query parameter verification)
   - Multipart uploads: correct (all validations, composite ETag)
   - User metadata: correct (x-amz-meta-* round-trip)
   - Bucket validation: correct (3-63 chars, lowercase, no IP format)
   - ACLs: correct (bucket and object, canned and XML body)

## Known Test Bug (Not Fixed — Per Rules)
- `test_invalid_access_key` in `tests/e2e/test_errors.py` has `endpoint_url="http://localhost:9000"` hardcoded on line 82 instead of using `BLEEPSTORE_ENDPOINT` env var. This test will fail when run against any port other than 9000.

## Sandbox Limitation
- Could not start the server process in the sandbox environment (all attempts to run `./bleepstore ...` or `go run ...` were blocked)
- Workaround: created Go-based integration tests that start the server in-process within `go test`
- The actual Python E2E tests and smoke test still need to be run manually by the user

## Files Changed
- `internal/errors/errors.go` — Added ErrKeyTooLongError
- `internal/handlers/object.go` — Added key length validation in PutObject
- `internal/server/integration_test.go` — New file with 37 integration tests

## Test Results
- 202 total tests passing (165 unit + 37 integration)
- All tests pass with `-race` flag
- No data races detected

## Key Decisions
- Used in-process testing to work around sandbox limitations
- Named types `integrationServer`/`newIntegrationServer` to avoid conflicts with existing `testServer` in `server_test.go`
- Custom SigV4 signing helpers prefixed with `int` to avoid conflicts with unexported auth package functions
- Each test gets its own isolated server with fresh temp dirs and SQLite database
