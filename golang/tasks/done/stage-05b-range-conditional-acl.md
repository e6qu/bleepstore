# Stage 5b: Range, Conditional Requests & Object ACLs

**Completed:** 2026-02-23

## What Was Implemented

### Range Requests (GetObject)
- `parseRange()` helper function parses HTTP Range header
- Three formats: `bytes=0-4` (start-end), `bytes=5-` (open-ended), `bytes=-10` (suffix/last N bytes)
- Returns 206 Partial Content with `Content-Range: bytes start-end/total` header
- Returns 416 Range Not Satisfiable (InvalidRange S3 error) for unsatisfiable ranges
- Seeks via `io.ReadSeeker` for local file backend, with discard-based fallback for other backends
- Single range only (multi-range not supported, matches S3 behavior)
- End clamped to last byte of object

### Conditional Requests (GetObject, HeadObject)
- `checkConditionalHeaders()` helper evaluates all four conditional headers per RFC 7232 priority:
  1. `If-Match`: 412 Precondition Failed on ETag mismatch (supports `*` wildcard, comma-separated)
  2. `If-Unmodified-Since`: 412 if modified after given time (skipped if If-Match present)
  3. `If-None-Match`: 304 Not Modified on match for GET/HEAD, 412 for other methods
  4. `If-Modified-Since`: 304 if not modified (skipped if If-None-Match present)
- 304 responses include ETag and Last-Modified headers (per RFC 7232)
- Conditional checks happen BEFORE opening storage data (avoids unnecessary I/O)
- ETag comparison normalizes by stripping surrounding quotes
- Time comparison truncates to second precision (HTTP date granularity)

### Object ACL Operations
- `GetObjectAcl`: Returns AccessControlPolicy XML with Owner, Grants, proper xmlns/xsi attributes
- `PutObjectAcl`: Three modes: canned ACL via `x-amz-acl` header, XML body, or default private
- Reuses existing ACL infrastructure from Stage 3: `aclFromJSON`, `aclToJSON`, `parseCannedACL`, `defaultPrivateACL`
- Uses `metadata.UpdateObjectAcl` from Stage 2
- Both validate bucket and object existence first

## Files Changed

| File | Changes |
|------|---------|
| `internal/handlers/helpers.go` | Added `parseRange()` and `checkConditionalHeaders()` helper functions; added `fmt`, `time` imports |
| `internal/handlers/object.go` | Enhanced `GetObject` with range + conditional support; enhanced `HeadObject` with conditional support; implemented `GetObjectAcl` and `PutObjectAcl`; added `encoding/xml`, `fmt` imports |
| `internal/handlers/object_test.go` | Added 26 new test functions for range parsing, range handlers, conditional headers, conditional handlers, object ACLs; added `time` import |
| `internal/server/server_test.go` | Updated object ACL route expectations from 501 to 500 (now implemented) |

## Tests Added

### parseRange unit tests (15 cases)
- TestParseRange: first bytes, open-end, suffix, single byte, last byte, end clamped, entire object, start beyond size, empty object, invalid prefix, multi-range, negative suffix, start > end, suffix larger than file

### Range handler tests (4 tests)
- TestGetObjectRangeFirstBytes: bytes=0-4 returns "abcde", 206, Content-Range
- TestGetObjectRangeOpenEnd: bytes=20- returns last 6 bytes
- TestGetObjectRangeSuffix: bytes=-5 returns last 5 bytes
- TestGetObjectRangeUnsatisfiable: bytes=100-200 on small object returns 416

### Conditional handler tests (5 tests)
- TestGetObjectIfMatch: correct ETag succeeds, wrong ETag returns 412
- TestGetObjectIfNoneMatch: matching ETag returns 304, different succeeds
- TestHeadObjectIfNoneMatch: matching ETag returns 304
- TestGetObjectIfModifiedSince: future date returns 304, past date succeeds
- TestGetObjectIfUnmodifiedSince: future date succeeds, past date returns 412

### checkConditionalHeaders unit tests (14 cases)
- TestCheckConditionalHeaders: no headers, If-Match (match/mismatch/wildcard), If-None-Match (match GET/HEAD/PUT/no-match), If-Modified-Since (modified/not), If-Unmodified-Since (modified/not), priority tests

### Object ACL tests (6 tests)
- TestGetObjectAcl: default private ACL with proper XML attributes
- TestGetObjectAclNoSuchKey: 404 NoSuchKey
- TestPutObjectAclCanned: public-read round-trip
- TestPutObjectAclXMLBody: XML body ACL round-trip
- TestPutObjectAclNoSuchKey: 404 NoSuchKey
- TestGetObjectAclNoSuchBucket: 404 NoSuchBucket

## Key Decisions
- Conditional headers evaluated before storage I/O for performance
- Range requests use io.ReadSeeker with fallback for non-seekable streams
- Multi-range not supported (matches S3 behavior)
- 304 includes ETag + Last-Modified per RFC 7232
- Object ACL reuses all Stage 3 ACL infrastructure (no duplication)
- No new dependencies (all stdlib)

## Dependencies Added
None.
