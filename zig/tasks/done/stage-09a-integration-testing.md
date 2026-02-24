# Stage 9a: Core Integration Testing

**Date:** 2026-02-23
**Sessions:** 16 (proactive fixes) + 17 (E2E testing & bug fixes)
**Status:** Zig E2E complete (34/34), Python E2E pending manual run

## What Was Implemented

### Zig E2E Test Suite (NEW)

Created `src/e2e_test.zig` -- a standalone Zig program with 34 integration tests
that exercises all S3 operations against the running server on port 9013.

**Architecture:**
- Raw TCP sockets (`std.net.tcpConnectToAddress`) -- avoids unstable `std.http.Client` API in Zig 0.15
- AWS SigV4 request signing using the existing `auth.zig` module
- Chunked transfer encoding decoder for HTTP responses
- `Connection: close` per request for clean socket lifecycle
- Full HTTP/1.1 request construction with proper headers

**Test Categories (34 tests):**
- Bucket (8): create/delete, duplicate, head existing, head nonexistent, list, delete nonexistent, get location, invalid name
- Object (15): put/get, head, delete, delete nonexistent, copy, list v2, list v2 prefix, list v2 delimiter, get nonexistent, large body (1MB), range, conditional if-none-match, batch delete, unicode key, list v1
- Multipart (4): basic upload + complete, abort, list uploads, list parts
- Error (5): NoSuchBucket, NoSuchKey, BucketNotEmpty, request ID in response, key too long
- ACL (2): get bucket ACL, get object ACL

### Build Step

Added `e2e` build step to `build.zig`:
```bash
zig build e2e   # server must be running on port 9013
```

## Critical Bugs Found and Fixed

### Bug #1: Header Array Iteration Segfault

**Severity:** Critical (server crash on any PUT with body)

**Symptom:** Server segfaulted at address `0xaaaaaaaaaaaaaaaa` in `extractUserMetadata` when processing any PUT object request with a body.

**Root Cause:** In httpz, `req.headers.keys` is a fixed-capacity backing array. `.keys.len` returns the compile-time capacity (e.g., 32), not the number of populated headers. Entries beyond the populated count contain freed/sentinel memory -- the GPA fills freed memory with `0xAA` bytes, so iterating past the populated count reads `0xAA` pointers, causing a segfault.

**Fix:** Changed `@min(header_keys.len, header_values.len)` to `req.headers.len` (the runtime populated count) in:
- `src/handlers/object.zig` (extractUserMetadata function)
- `src/server.zig` (authenticateRequest function)

### Bug #2: httpz max_body_size Too Small for Multipart Uploads

**Severity:** Critical (all multipart part uploads > ~1MB fail)

**Symptom:** Multipart part uploads sending 5MB of data got `BrokenPipe` -- the server closed the TCP connection before the client finished sending the request body.

**Root Cause:** httpz has a default `max_body_size` that is smaller than 5MB. When a request body exceeds this limit, httpz closes the connection without reading the full body.

**Fix:** Added explicit `max_body_size` configuration to the tokamak server init in `src/server.zig`:
```zig
.request = .{
    .max_body_size = 128 * 1024 * 1024, // 128 MB
},
```

## Session 16 Proactive Fixes (Earlier Session)

Prior to E2E testing, thorough code analysis identified and fixed:
1. **UploadPartCopy handler**: Copies data from existing object into a multipart upload part
2. **URL-decoding for object keys**: Percent-encoded characters in dispatch path properly decoded
3. **Memory leak fixes**: GPA-allocated ObjectMeta fields and source object body freed after use
4. **renderCopyPartResult**: New XML rendering function for CopyPartResult
5. **UploadPartCopy routing**: Detected via x-amz-copy-source header on PUT with partNumber+uploadId

## Files Changed

| File | Change |
|------|--------|
| `src/e2e_test.zig` | NEW -- 34 E2E integration tests |
| `build.zig` | Added `e2e` build step |
| `src/handlers/object.zig` | Bug fix: `req.headers.len` instead of `@min(header_keys.len, header_values.len)` |
| `src/server.zig` | Bug fix: same header iteration fix + `max_body_size = 128MB` |
| `src/handlers/multipart.zig` | Session 16: UploadPartCopy handler + memory leak fixes |
| `src/xml.zig` | Session 16: renderCopyPartResult |

## Key Decisions

1. **Raw TCP sockets for E2E tests**: `std.http.Client` does not have a stable `open()` API in Zig 0.15.2. Raw TCP sockets with manual HTTP/1.1 construction work reliably.
2. **Connection: close per request**: Each E2E test opens a new TCP connection and closes it after the response. This avoids keep-alive complexity and ensures clean test isolation.
3. **SigV4 signing reuse**: The E2E test program imports `auth.zig` and uses the same signing functions that the server uses for verification. This ensures the signing is correct by construction.
4. **128MB max_body_size**: Generous limit allows multipart parts up to 128MB. S3's maximum part size is 5GB, but 128MB is adequate for Phase 1 testing.

## Test Results

- `zig build test` -- 133/133 unit tests pass, 0 memory leaks
- `zig build e2e` -- 34/34 Zig E2E tests pass
- Server remains healthy after all E2E tests (no crashes, no leaks)

## Remaining Work

- Run Python E2E tests (75 tests) manually -- sandbox blocks Python/pytest execution
- Run smoke test (20 tests) manually -- sandbox blocks AWS CLI execution
- If Python tests reveal additional bugs, fix them and re-run both Zig and Python E2E suites
