# Stage 16: S3 API Completeness

**Status:** ✅ Gap Analysis Complete (2026-02-27)

**Goal:** Close remaining S3 API gaps identified in `S3_GAP_REMAINING.md`.

## Summary

The core S3 API is feature-complete (86/86 E2E tests passing). This stage documents optional enhancements identified in the gap analysis. Implementation is optional—the current implementation meets spec requirements.

## Gap Analysis Results

### Fully Implemented (No Gaps)

| Category | Status |
|----------|--------|
| Bucket Operations (7/7) | Complete |
| Object Operations (10/10) | Complete |
| Multipart Upload (7/7) | Complete |
| SigV4 Authentication | Complete |
| Common Headers (6/6) | Complete |
| Error Codes (27/41) | 66% (missing codes for out-of-scope features) |

### Optional Enhancements (Medium Priority)

#### 1. response-* Query Params on GetObject

Presigned URLs can override response headers via query parameters.

**Query params:**
- `response-content-type`
- `response-content-language`
- `response-expires`
- `response-cache-control`
- `response-content-disposition`
- `response-content-encoding`

**Files to modify:**
- `src/bleepstore/handlers/object.py`
  - Parse response-* params in `get_object()`
  - Override corresponding response headers

**Acceptance criteria:**
- [ ] Presigned URL with `?response-content-type=text/plain` returns `Content-Type: text/plain`
- [ ] `response-content-disposition` sets `Content-Disposition` header
- [ ] Response overrides only apply to presigned URLs (not authenticated requests)

#### 2. Conditional CopyObject

Support `x-amz-copy-source-if-*` conditional headers.

**Headers:**
- `x-amz-copy-source-if-match: "etag"`
- `x-amz-copy-source-if-none-match: "etag"`
- `x-amz-copy-source-if-modified-since: <date>`
- `x-amz-copy-source-if-unmodified-since: <date>`

**Files to modify:**
- `src/bleepstore/handlers/object.py`
  - Add conditional evaluation in `copy_object()`
  - Reuse `evaluate_conditionals()` pattern from `get_object()`
  - Return 412 PreconditionFailed on failure

**Acceptance criteria:**
- [ ] `x-amz-copy-source-if-match` returns 412 if source ETag doesn't match
- [ ] `x-amz-copy-source-if-none-match` returns 412 if source ETag matches
- [ ] Date-based conditionals work correctly
- [ ] Response includes `x-amz-copy-source-version-id` (placeholder if no versioning)

#### 3. Conditional UploadPartCopy

Same conditional headers for UploadPartCopy.

**Files to modify:**
- `src/bleepstore/handlers/multipart.py`
  - Add conditional evaluation in `upload_part_copy()`

**Acceptance criteria:**
- [ ] Conditional headers work same as CopyObject
- [ ] CopyPartResult XML includes conditional info on success

#### 4. EncodingType in List Operations

Support `encoding-type=url` parameter in ListObjects/ListObjectsV2.

**Files to modify:**
- `src/bleepstore/handlers/object.py`
  - Handle `encoding-type` query param in `list_objects_v1/v2()`
- `src/bleepstore/xml_utils.py`
  - URL-encode `Key` and `Prefix` elements when `encoding-type=url`

**Acceptance criteria:**
- [ ] `?encoding-type=url` returns URL-encoded keys in XML
- [ ] `EncodingType` element included in response XML
- [ ] Keys with special characters (spaces, unicode) handled correctly

### Low Priority (Future Consideration)

#### 5. x-amz-storage-class Enforcement

Parse and enforce `x-amz-storage-class` header.

- Currently parsed but not enforced
- Would require storage tier infrastructure

#### 6. x-amz-tagging Support

Parse and store object tags.

- Would require tag storage schema
- Tag-based access control requires IAM

#### 7. ListBuckets Pagination

Add `continuation-token` and `max-buckets` support.

- Rarely needed (few users have >1000 buckets)

#### 8. RequestTimeout Enforcement

Reject slow clients with `RequestTimeout` error.

- Requires request-level timeout tracking

## Out of Scope

These features are explicitly excluded:

| Feature | Reason |
|---------|--------|
| Object Versioning | Future stage |
| Lifecycle Configuration | Not planned |
| Server-Side Encryption | Not planned |
| Replication | Not planned |
| Glacier/Archive | Not planned |
| Bucket Policy/IAM | Not planned |
| Rate Limiting | Not planned |
| Redirect Support | Not planned |

## Testing Requirements

If implementing enhancements:

```bash
# Run full test suite
uv run pytest tests/ -v

# Run E2E tests
./run_e2e.sh

# Verify no regressions
# All 86 E2E tests must still pass
```

## Definition of Done

- [x] Gap analysis documented in `S3_GAP_REMAINING.md`
- [ ] Optional: response-* query params implemented
- [ ] Optional: x-amz-copy-source-if-* headers implemented
- [ ] Optional: EncodingType parameter supported
- [ ] All 86 E2E tests still pass
- [ ] Unit tests added for new features

## Files Changed (If Implementing)

| File | Changes |
|------|---------|
| `src/bleepstore/handlers/object.py` | response-* params, conditional copy, encoding-type |
| `src/bleepstore/handlers/multipart.py` | conditional upload-part-copy |
| `src/bleepstore/xml_utils.py` | URL encoding in list responses |
| `tests/test_handlers_object.py` | tests for response-* params |
| `tests/test_handlers_object_advanced.py` | tests for conditional copy |
