# Phase 2: S3 Minor Gaps

**Status:** IN PROGRESS
**Created:** 2026-03-01
**Updated:** 2026-03-01
**Prerequisites:** Phase 1 COMPLETE ✅

---

## Objective

Close remaining S3 API compliance gaps across all implementations.

---

## Current Status (Verified 2026-03-01)

| Feature | Python | Go | Rust | Zig |
|---------|--------|-----|------|-----|
| encoding-type in ListMultipartUploads | ✅ | ✅ | 🔲 | ✅ |
| response-* params on GetObject | ✅ | ✅ | ✅ | ✅ |
| PutBucketAcl XML parsing | ✅ | ✅ | ✅ | ✅ |
| PutObjectAcl XML parsing | ✅ | ✅ | ✅ | ✅ |
| Expired multipart reaping | ✅ | ✅ | ✅ | ✅ |
| x-amz-copy-source-if-* on UploadPartCopy | ✅ | ✅ | 🔲 | ✅ |

**Only 2 remaining gaps in Rust:**
1. encoding-type in ListMultipartUploads
2. x-amz-copy-source-if-* on UploadPartCopy

---

## Remaining Tasks

### Rust Tasks

#### Task 2.1: encoding-type=url in ListMultipartUploads

**Files to modify:**
- `rust/src/handlers/multipart.rs`

**Implementation:**
- Parse `encoding-type` query param
- URL-encode keys when `encoding-type=url`
- Return `EncodingType` element in response XML

**Acceptance criteria:**
- [ ] Parse `encoding-type` query param in ListMultipartUploads
- [ ] URL-encode keys when `encoding-type=url`
- [ ] Return `EncodingType` element in response XML
- [ ] All 86 E2E tests pass

---

#### Task 2.2: x-amz-copy-source-if-* on UploadPartCopy

**Files to modify:**
- `rust/src/handlers/multipart.rs`

**Headers to support:**
- `x-amz-copy-source-if-match`
- `x-amz-copy-source-if-none-match`
- `x-amz-copy-source-if-modified-since`
- `x-amz-copy-source-if-unmodified-since`

**Implementation:**
- Reuse conditional evaluation logic from CopyObject
- Evaluate conditionals against source object metadata
- Return 412 PreconditionFailed on failure

**Acceptance criteria:**
- [ ] Evaluate conditionals against source object
- [ ] Return 412 PreconditionFailed on failure
- [ ] All 86 E2E tests pass

---

## Already Complete (No Action Needed)

- **Go:** All features complete
- **Zig:** All features complete  
- **Python:** All features complete (reference implementation)
- **Rust:** response-* params, ACL XML parsing, multipart reaping

---

## Definition of Done

- [ ] Rust Task 2.1 complete (encoding-type in ListMultipartUploads)
- [ ] Rust Task 2.2 complete (conditional headers on UploadPartCopy)
- [ ] All 86 E2E tests pass for Rust
- [ ] S3 API coverage ~99% for all implementations
