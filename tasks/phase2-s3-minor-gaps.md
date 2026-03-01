# Phase 2: S3 Minor Gaps

**Status:** PENDING (after Phase 1)
**Created:** 2026-03-01
**Prerequisites:** Phase 1 COMPLETE

---

## Objective

Close remaining S3 API compliance gaps across all implementations.

---

## Current Status

| Feature | Python | Go | Rust | Zig |
|---------|--------|-----|------|-----|
| encoding-type in ListMultipartUploads | ✅ | 🔲 | ? | 🔲 |
| response-* params on GetObject | ✅ | 🔲 | ? | 🔲 |
| PutBucketAcl XML parsing | ✅ | ✅ | ✅ | 🔲 |
| PutObjectAcl XML parsing | ✅ | ✅ | ✅ | 🔲 |
| Expired multipart reaping | ✅ | ✅ | ? | 🔲 |
| x-amz-copy-source-if-* on UploadPartCopy | ✅ | ✅ | ? | 🔲 |

**Note:** `?` means status is unclear - need to verify.

---

## Tasks by Implementation

### Go Tasks

#### Task 2.1: encoding-type=url in ListMultipartUploads

**Files to modify:**
- `golang/internal/handlers/multipart.go`

**Acceptance criteria:**
- [ ] Parse `encoding-type` query param in ListMultipartUploads
- [ ] URL-encode keys when `encoding-type=url`
- [ ] Return `EncodingType` element in response XML

---

#### Task 2.2: response-* Query Params on GetObject

**Files to modify:**
- `golang/internal/handlers/object.go`

**Query params to support:**
- `response-content-type`
- `response-content-language`
- `response-expires`
- `response-cache-control`
- `response-content-disposition`
- `response-content-encoding`

**Acceptance criteria:**
- [ ] Parse response-* query params
- [ ] Override corresponding response headers
- [ ] All 86 E2E tests pass

---

### Zig Tasks

#### Task 2.3: PutBucketAcl XML Body Parsing

**Files to modify:**
- `zig/src/handlers/bucket.zig`
- `zig/src/xml.zig`

**Acceptance criteria:**
- [ ] Parse `<AccessControlPolicy>` XML body
- [ ] Apply grants to bucket ACL
- [ ] Return `MalformedACLError` on parse failure

---

#### Task 2.4: PutObjectAcl XML Body Parsing

**Files to modify:**
- `zig/src/handlers/object.zig`
- `zig/src/xml.zig`

**Acceptance criteria:**
- [ ] Same as PutBucketAcl
- [ ] Apply grants to object ACL

---

#### Task 2.5: Expired Multipart Upload Reaping

**Files to modify:**
- `zig/src/metadata/sqlite.zig` -- Add `reapExpiredMultipartUploads()` method
- `zig/src/main.zig` -- Call reaper on startup

**Acceptance criteria:**
- [ ] On startup, delete uploads older than 7 days
- [ ] Also delete associated parts
- [ ] Log count of reaped uploads

---

#### Task 2.6: encoding-type=url for List Operations

**Files to modify:**
- `zig/src/handlers/object.zig` -- ListObjectsV2, ListObjectsV1
- `zig/src/handlers/multipart.zig` -- ListMultipartUploads

**Acceptance criteria:**
- [ ] URL-encode keys when `encoding-type=url`
- [ ] Return `EncodingType` element in response XML

---

#### Task 2.7: x-amz-copy-source-if-* on UploadPartCopy

**Files to modify:**
- `zig/src/handlers/multipart.zig`

**Headers to support:**
- `x-amz-copy-source-if-match`
- `x-amz-copy-source-if-none-match`
- `x-amz-copy-source-if-modified-since`
- `x-amz-copy-source-if-unmodified-since`

**Acceptance criteria:**
- [ ] Evaluate conditionals against source object
- [ ] Return 412 PreconditionFailed on failure

---

#### Task 2.8: response-* Query Params on GetObject

**Files to modify:**
- `zig/src/handlers/object.zig`

**Acceptance criteria:**
- [ ] Same as Go Task 2.2

---

## Definition of Done

- [ ] All Go tasks complete (2.1, 2.2)
- [ ] All Zig tasks complete (2.3-2.8)
- [ ] Rust gaps verified/implemented
- [ ] All 86 E2E tests pass for each implementation
- [ ] S3 API coverage ~99% for all implementations
