# Stage 3: Bucket CRUD -- Completed 2026-02-23

## Summary
Implemented all 7 S3 bucket operations, wiring them to the SQLite metadata store initialized in a lifespan hook. All bucket handlers are fully functional with proper XML responses, error handling, and S3-compatible behavior.

## What was implemented

### New files
- `src/bleepstore/handlers/acl.py` -- ACL helper module (6 functions: build_default_acl, parse_canned_acl, acl_to_json, acl_from_json, render_acl_xml, plus constants for S3 group URIs)
- `tests/test_handlers_bucket.py` -- 38 integration tests across 7 test classes

### Modified files
- `src/bleepstore/handlers/bucket.py` -- Complete rewrite from stubs to 7 working handlers
- `src/bleepstore/xml_utils.py` -- Added render_list_buckets() and render_location_constraint()
- `src/bleepstore/server.py` -- Added lifespan hook (metadata init + credential seeding), wired BucketHandler
- `tests/conftest.py` -- Updated client fixture to initialize metadata store for tests
- `tests/test_server.py` -- Updated to reflect bucket routes being wired (removed bucket 501 tests, added TestBucketRoutesWired)

## Handlers implemented
1. **ListBuckets** (GET /) -- Returns ListAllMyBucketsResult XML
2. **CreateBucket** (PUT /{bucket}) -- Validates name, parses LocationConstraint, supports canned ACLs, idempotent
3. **DeleteBucket** (DELETE /{bucket}) -- Checks existence, checks empty, returns 204
4. **HeadBucket** (HEAD /{bucket}) -- Returns x-amz-bucket-region header, no body
5. **GetBucketLocation** (GET /{bucket}?location) -- us-east-1 quirk handled
6. **GetBucketAcl** (GET /{bucket}?acl) -- Returns AccessControlPolicy XML
7. **PutBucketAcl** (PUT /{bucket}?acl) -- Supports canned ACL header, XML body, default private

## Key decisions
- Owner ID derived from SHA-256 of access key (32 chars)
- ACL stored as JSON, rendered as XML on output
- CreateBucket idempotency follows us-east-1 behavior (existing bucket returns 200)
- _find_elem() helper avoids ElementTree deprecated truth-value warnings
- Each bucket test gets fresh in-memory SQLite to avoid state pollution
- Lifespan hook seeds credentials on every startup (crash-only)

## Test results
- 195 tests total, all passing, zero warnings
- 38 new tests in test_handlers_bucket.py
- All 157 pre-existing tests still pass (no regressions)
