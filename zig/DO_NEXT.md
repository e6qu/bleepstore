# BleepStore Zig -- Do Next

## Current State: Stage 17-18 Complete -- Pluggable & Cloud Metadata Backends

Stage 17-18 (Pluggable & Cloud Metadata Backends) is complete. 160/160 unit tests pass. 86/86 E2E tests pass.

- `zig build` -- clean
- `zig build test` -- 160/160 pass, 0 leaks
- Python E2E -- **86/86 pass**

### Metadata Backends Status

| Backend | Description | Status |
|---------|-------------|--------|
| `sqlite` | SQLite file (default) | ✅ Complete |
| `memory` | In-memory hash maps | ✅ Fixed for Zig 0.15 |
| `local` | JSONL append-only files | ✅ Fixed for Zig 0.15 |
| `dynamodb` | AWS DynamoDB | 🔲 Stub (NotImplemented) |
| `firestore` | GCP Firestore | ✅ Full HTTP implementation |
| `cosmos` | Azure Cosmos DB | ✅ Full HTTP implementation |

---

## Phase 1: Complete Cloud Metadata Backends

### Task 1.3: Implement DynamoDB Metadata Backend

**Goal:** Full AWS DynamoDB implementation using HTTP client (no native Zig SDK exists).

**Files to modify:**
- `src/metadata/dynamodb.zig` -- Replace stub with full implementation

**Implementation notes:**
- Single-table PK/SK design (same as Python/Go/Rust)
- HTTP client with AWS SigV4 signing (reuse existing auth.zig functions)
- All 22 MetadataStore vtable methods

**Test requirements:**
- Skipped by default (require `DYNAMODB_TEST_ENDPOINT`)
- Reference: Python implementation at `python/src/bleepstore/metadata/dynamodb.py`

**Note:** Since Zig has no native AWS SDK, we must use HTTP client with SigV4 signing. The auth.zig module already has the necessary signing functions.

---

## Phase 2: S3 Minor Gaps (After Phase 1)

### Task 2.1: PutBucketAcl XML Body Parsing

**Current**: Accepts XML body but doesn't parse `<AccessControlPolicy>` elements
**Location**: `src/handlers/bucket.zig`

### Task 2.2: PutObjectAcl XML Body Parsing

**Current**: Same as PutBucketAcl -- accepts XML but doesn't parse
**Location**: `src/handlers/object.zig`

### Task 2.3: Expired Multipart Upload Reaping

**Current**: No cleanup of stale multipart uploads on startup
**Scope**: On startup, delete uploads where `created_at < (now - 7 days)`
**Files**: `src/metadata/sqlite.zig`, `src/main.zig`

### Task 2.4: encoding-type=url for List Operations

**Current**: Returns keys as-is (no URL encoding)
**Scope**: Support on ListObjectsV2, ListObjectsV1, ListMultipartUploads
**Files**: `src/handlers/object.zig`, `src/handlers/multipart.zig`

### Task 2.5: x-amz-copy-source-if-* Conditional Headers for UploadPartCopy

**Current**: UploadPartCopy doesn't validate conditional headers on source
**Files**: `src/handlers/multipart.zig`

### Task 2.6: response-* Query Parameter Overrides for GetObject

**Current**: GetObject ignores response-* query params
**Scope**: Override response headers via query params
**Files**: `src/handlers/object.zig`

---

## Files to Modify

| File | Phase 1 | Phase 2 |
|------|---------|---------|
| `src/metadata/dynamodb.zig` | Full implementation | - |
| `src/handlers/bucket.zig` | - | PutBucketAcl XML |
| `src/handlers/object.zig` | - | PutObjectAcl XML, response-* |
| `src/handlers/multipart.zig` | - | Conditional headers, encoding-type |
| `src/metadata/sqlite.zig` | - | Reaping |
| `src/main.zig` | - | Call reap on startup |
| `src/xml.zig` | - | parseAccessControlPolicy |

---

## Definition of Done

**Phase 1:**
- [ ] DynamoDB: All 22 MetadataStore methods work
- [ ] Unit tests (skipped by default)
- [ ] All E2E tests still pass

**Phase 2:**
- [ ] PutBucketAcl with XML body parses and applies ACL
- [ ] PutObjectAcl with XML body parses and applies ACL
- [ ] Expired multipart uploads reaped on startup
- [ ] encoding-type=url works in list operations
- [ ] x-amz-copy-source-if-* headers work on UploadPartCopy
- [ ] response-* params override GetObject headers
- [ ] All 160 unit tests pass
- [ ] All 86 E2E tests pass

---

## Run Tests
```bash
cd /Users/zardoz/projects/bleepstore/zig
zig build test
zig build e2e
./run_e2e.sh
```
