# BleepStore Zig -- Do Next

## Current State: Stage 15 Complete -- Performance Optimization

Stage 15 (Performance Optimization) is complete. 160/160 unit tests pass. 86/86 E2E tests pass.

- `zig build` -- clean
- `zig build test` -- 160/160 pass, 0 leaks
- Python E2E -- **86/86 pass**

## Next: Stage 16 -- S3 API Completeness

### Goal
Close the remaining S3 API gaps identified in `S3_GAP_REMAINING.md` to achieve full S3 compliance for Phase 1 scope.

### Priority 1: Compliance Fixes (Required)

#### 1. PutBucketAcl XML Body Parsing
- **Current**: Accepts XML body but doesn't parse `<AccessControlPolicy>` elements
- **Location**: `src/handlers/bucket.zig:502-517`
- **Scope**: Parse `<AccessControlPolicy><Owner>...</Owner><AccessControlList><Grant>...</Grant></AccessControlList></AccessControlPolicy>` XML and apply grants to bucket ACL

#### 2. PutObjectAcl XML Body Parsing
- **Current**: Same as PutBucketAcl -- accepts XML but doesn't parse
- **Location**: `src/handlers/object.zig:1022-1030`
- **Scope**: Same XML parsing as PutBucketAcl, applied to object ACL

### Priority 2: Feature Completeness (Nice to Have)

#### 3. Expired Multipart Upload Reaping
- **Current**: No cleanup of stale multipart uploads on startup
- **Impact**: Storage leak over time if uploads are abandoned
- **Scope**: On startup, query `multipart_uploads` for uploads where `created_at < (now - 7 days)`, delete their parts and metadata

#### 4. encoding-type=url for List Operations
- **Current**: Returns keys as-is (no URL encoding)
- **Scope**: Support `encoding-type=url` query parameter on:
  - ListObjectsV2
  - ListObjectsV1
  - ListMultipartUploads
- **Behavior**: URL-encode keys and common prefixes in response XML when `encoding-type=url` is present

#### 5. x-amz-copy-source-if-* Conditional Headers for UploadPartCopy
- **Current**: UploadPartCopy doesn't validate conditional headers on source
- **Scope**: Support these headers on UploadPartCopy:
  - `x-amz-copy-source-if-match`
  - `x-amz-copy-source-if-none-match`
  - `x-amz-copy-source-if-modified-since`
  - `x-amz-copy-source-if-unmodified-since`
- **Location**: `src/handlers/multipart.zig:uploadPartCopy`

#### 6. response-* Query Parameter Overrides for GetObject
- **Current**: GetObject ignores response-* query params
- **Scope**: Support these query params on GetObject:
  - `response-content-type`
  - `response-content-language`
  - `response-expires`
  - `response-cache-control`
  - `response-content-disposition`
  - `response-content-encoding`
- **Behavior**: Override corresponding response headers with provided values

### Files to Modify

| File | Priority 1 | Priority 2 |
|------|------------|------------|
| `src/handlers/bucket.zig` | PutBucketAcl XML parsing | - |
| `src/handlers/object.zig` | PutObjectAcl XML parsing | response-* params |
| `src/handlers/multipart.zig` | - | x-amz-copy-source-if-*, reaping |
| `src/metadata/sqlite.zig` | - | reapExpiredMultipartUploads |
| `src/xml.zig` | parseAccessControlPolicy helper | - |
| `src/main.zig` | - | call reap on startup |

### Run Tests
```bash
cd /Users/zardoz/projects/bleepstore/zig
zig build test         # unit tests (should be 160+)
zig build e2e          # Zig E2E tests (34 tests)
./run_e2e.sh           # Python E2E tests (86 tests)
```

### Acceptance Criteria
- [ ] PutBucketAcl with XML body correctly parses and applies ACL grants
- [ ] PutObjectAcl with XML body correctly parses and applies ACL grants
- [ ] Malformed ACL XML returns `MalformedACLError` (400)
- [ ] Expired multipart uploads (>7 days) are reaped on startup
- [ ] `encoding-type=url` URL-encodes keys in list responses
- [ ] `x-amz-copy-source-if-*` headers work on UploadPartCopy
- [ ] `response-*` query params override GetObject response headers
- [ ] All 160+ unit tests pass
- [ ] All 86 E2E tests pass

## Known Issues
- `test_invalid_access_key` has hardcoded `endpoint_url="http://localhost:9000"` (test bug, per CLAUDE.md rule 6)
