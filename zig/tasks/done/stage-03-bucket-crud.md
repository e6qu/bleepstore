# Stage 3: Bucket CRUD

**Completed:** 2026-02-23

## What Was Implemented

All 7 bucket CRUD handlers were implemented and wired to the SQLite metadata store:

### Handlers (`src/handlers/bucket.zig`)

| Operation | Method | Path | Description |
|-----------|--------|------|-------------|
| ListBuckets | GET | `/` | Returns ListAllMyBucketsResult XML with all buckets and owner info |
| CreateBucket | PUT | `/<bucket>` | Validates name, parses optional LocationConstraint, supports canned ACLs, idempotent |
| DeleteBucket | DELETE | `/<bucket>` | Checks exists + empty, returns 204 |
| HeadBucket | HEAD | `/<bucket>` | Returns 200 with x-amz-bucket-region header |
| GetBucketLocation | GET | `/<bucket>?location` | Returns LocationConstraint XML (empty for us-east-1) |
| GetBucketAcl | GET | `/<bucket>?acl` | Returns AccessControlPolicy XML from stored JSON ACL |
| PutBucketAcl | PUT | `/<bucket>?acl` | Supports canned ACLs via x-amz-acl header |

### Helper Functions in bucket.zig

- `deriveOwnerId(alloc, access_key)` -- SHA-256 hash, first 32 hex chars
- `buildDefaultAclJson(alloc, owner_id, owner_display)` -- Private ACL (FULL_CONTROL)
- `buildCannedAclJson(alloc, canned_acl, owner_id, owner_display)` -- Canned ACL to JSON
- `parseLocationConstraint(body)` -- Extract region from CreateBucketConfiguration XML
- `formatIso8601(alloc)` -- Current time as ISO 8601 string

### XML Rendering (`src/xml.zig`)

- `renderLocationConstraint(alloc, region)` -- us-east-1 returns empty element per S3 spec
- `renderAccessControlPolicy(alloc, owner_id, owner_display, acl_json)` -- Full ACL XML with xsi:type attributes
- `parseAclGrants(alloc, acl_json)` -- JSON-to-struct ACL grant parser (std.json)
- `XmlWriter.emptyElementWithNs()` -- Self-closing tag with xmlns attribute
- `XmlWriter.raw()` -- Write raw XML content (for complex attributes)

### Server Changes (`src/server.zig`)

- Added `global_region` and `global_access_key` for handler config access
- Made `setCommonHeaders` public
- Updated routing to pass `req: *tk.Request` to createBucket and putBucketAcl

## Files Changed

| File | Change |
|------|--------|
| `src/handlers/bucket.zig` | Complete rewrite -- 7 handlers + helpers + 7 tests |
| `src/xml.zig` | Added 2 render functions + parser + 2 XmlWriter methods + 6 tests |
| `src/server.zig` | Added globals, made setCommonHeaders public, updated routing |

## Key Decisions

1. **Global config values over pointer**: `global_region` and `global_access_key` as `[]const u8` globals because Server is returned by value (pointer would dangle)
2. **ACL as JSON**: Stored in SQLite, converted to XML on demand
3. **Simple XML parsing**: `std.mem.indexOf` for LocationConstraint tags
4. **Canned ACL via header**: Primary ACL mechanism; XML body accepted but not fully parsed
5. **BucketNotEmpty check**: `listObjectsMeta(bucket, "", "", "", 1)` to check for any objects
6. **Idempotent create**: Returns 200 with Location header if bucket exists
7. **Owner derivation**: SHA-256 of access key, first 32 hex chars

## Test Results

- `zig build` -- clean
- `zig build test` -- 74/74 pass, 0 memory leaks
- New tests:
  - 7 in bucket.zig (parseLocationConstraint variants, deriveOwnerId, ACL JSON building)
  - 6 in xml.zig (LocationConstraint rendering, AccessControlPolicy rendering)

## Issues Encountered

1. **Return type mismatch**: `std.fmt.allocPrint` returns `![]u8` but `buildCannedAclJson` needed `!?[]u8`. Fixed by assigning to const first, then returning.
2. **Memory leaks in ACL tests**: `parseAclGrants` allocated strings for grants but `renderAccessControlPolicy` didn't free them. Fixed by adding `defer` block to free all grant fields and the grant slice.
