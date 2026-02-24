# Stage 5b: Range, Conditional Requests & Object ACLs

**Date:** 2026-02-23
**Status:** Complete
**Tests:** 104/104 pass, 0 memory leaks

## What Was Implemented

### Range Requests (`GET /<bucket>/<key>` with Range header)
- Parses `Range: bytes=start-end`, `bytes=start-`, `bytes=-suffix` formats
- Returns 206 Partial Content with `Content-Range: bytes start-end/total` header
- Returns 416 InvalidRange for unsatisfiable ranges (start >= total size)
- Suffix range (`bytes=-N`) returns last N bytes
- Open-ended range (`bytes=N-`) returns from offset N to end
- Full body read then slice approach (adequate for Phase 1; streaming range reads can be added in Stage 15)

### Conditional Requests
- **If-Match**: Returns 200 if ETag matches, 412 Precondition Failed if not
- **If-None-Match**: Returns 304 Not Modified if ETag matches
- **If-Modified-Since**: Returns 304 if object not modified since date
- **If-Unmodified-Since**: Returns 412 if object modified since date
- **Priority rules**: If-Match > If-Unmodified-Since; If-None-Match > If-Modified-Since
- ETag comparison with quote-stripping normalization and wildcard ("*") support
- HTTP date parsing via custom RFC 7231 parser (`parseHttpDateToEpoch`)
- ISO 8601 date parsing for stored Last-Modified comparison (`parseIso8601ToEpoch`)

### Object ACLs
- **GetObjectAcl** (`GET /<bucket>/<key>?acl`): Reads ACL from metadata, renders AccessControlPolicy XML
- **PutObjectAcl** (`PUT /<bucket>/<key>?acl`): Supports canned ACLs (private, public-read, public-read-write, authenticated-read) via x-amz-acl header
- **PutObject** with x-amz-acl: Respects canned ACL header on object creation
- Reuses `renderAccessControlPolicy` from xml.zig (same XML format as bucket ACLs)
- Default ACL: FULL_CONTROL for owner (same as S3 default)

## Files Changed

| File | Changes |
|------|---------|
| `src/handlers/object.zig` | Rewrote `getObject` to accept `req` parameter for header access. Added conditional request evaluation (If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since with priority rules). Added range request handling with 206 response. Implemented `getObjectAcl` (was 501 stub). Implemented `putObjectAcl` (was 501 stub, now accepts `req`). Updated `putObject` to support x-amz-acl. Added helper functions: `parseRangeHeader`, `etagMatch`, `stripQuotes`, `isModifiedSince`, `parseIso8601ToEpoch`, `parseHttpDateToEpoch`, `monthNameToNumber`, `dateToEpoch`, `buildCannedAclJson`. Added file-level `xml_mod` import and removed 4 local imports. Added 18 unit tests. |
| `src/server.zig` | Updated routing to pass `req` to `getObject` and `putObjectAcl`. Object-level `?acl` dispatch was already wired from Stage 5a. |

## Key Decisions

1. Range reads use full body read then slice (no seek-based reads in storage backend) -- adequate for Phase 1
2. Conditional request priority follows HTTP/S3 spec: If-Match takes precedence over If-Unmodified-Since; If-None-Match takes precedence over If-Modified-Since
3. ETag comparison normalizes by stripping surrounding quotes and supports wildcard "*"
4. Custom RFC 7231 date parser and ISO 8601 parser convert to epoch seconds for comparison
5. Object ACLs reuse `renderAccessControlPolicy` from xml.zig (originally built for bucket ACLs)
6. `buildCannedAclJson` duplicated in object.zig (same logic as bucket.zig) to avoid cross-module coupling
7. `getObject` and `putObjectAcl` signatures changed to accept `req: *tk.Request` for header access
8. File-level `xml_mod` import replaced 4 function-local imports to avoid shadowing

## Issues Encountered

1. **Local constant shadows file-level declaration**: Adding a file-level `const xml_mod = @import("../xml.zig")` caused build errors because `deleteObjects`, `copyObject`, `listObjectsV2`, and `listObjectsV1` each had their own local `xml_mod` import. Fixed by removing all 4 local imports with `replace_all`.

## No Changes Needed

- `src/storage/local.zig` -- Range reads handled by slicing full body in handler (no storage backend change)
- `src/xml.zig` -- `renderAccessControlPolicy` already exists and works for object ACLs
- `src/metadata/store.zig` -- `updateObjectAcl` and `objectExists` already in vtable
- `src/metadata/sqlite.zig` -- All needed metadata operations already implemented
- `src/errors.zig` -- `InvalidRange` (416) and `PreconditionFailed` (412) already present
