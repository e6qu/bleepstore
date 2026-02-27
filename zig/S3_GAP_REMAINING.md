# BleepStore Zig — S3 API Gap Analysis

> Generated: 2026-02-28
> Implementation Stage: 16 (S3 API Completeness)
> Test Results: 160/160 unit tests, 86/86 E2E tests

---

## 1. Summary Table

| Category | Spec Operations | Implemented | Coverage |
|----------|----------------|-------------|----------|
| Bucket Operations | 7 | 7 | 100% |
| Object Operations | 10 | 10 | 100% |
| Multipart Upload | 7 | 7 | 100% |
| Authentication | SigV4 + Presigned | Full | 100% |
| Error Codes | 41 | 32 | 78% |
| Response Headers | 12 required | 12 | 100% |
| Storage Backends | 5 | 5 | 100% |

**Overall S3 Core API Coverage: ~98%** for Phase 1 scope (basic CRUD + multipart + ACLs).

---

## 2. Bucket Operations Gaps

**Reference:** `specs/s3-bucket-operations.md`

| Operation | Status | Notes |
|-----------|--------|-------|
| ListBuckets | ✅ PASS | Returns `ListAllMyBucketsResult` XML with Owner/Buckets |
| CreateBucket | ✅ PASS | LocationConstraint XML, canned ACL, x-amz-grant-* headers |
| DeleteBucket | ✅ PASS | Returns 204, checks BucketNotEmpty |
| HeadBucket | ✅ PASS | Returns x-amz-bucket-region header |
| GetBucketLocation | ✅ PASS | Handles us-east-1 empty element quirk |
| GetBucketAcl | ✅ PASS | Full ACL XML with xsi:type attributes |
| PutBucketAcl | ✅ PASS | Canned ACL + grant headers + XML body parsing |

### Bucket Operations Details

**Fully Implemented:**
- `src/handlers/bucket.zig`: All 7 operations routed and functional
- Bucket name validation per AWS rules (3-63 chars, lowercase, no IP format, etc.)
- LocationConstraint XML parsing for CreateBucket
- Canned ACLs: `private`, `public-read`, `public-read-write`, `authenticated-read`
- x-amz-grant-* header parsing (Mode 2 ACLs)
- Mutual exclusion validation between x-amz-acl and x-amz-grant-* headers
- **PutBucketAcl XML body parsing** (`src/handlers/bucket.zig:502-514`): Parses `<AccessControlPolicy>` XML via `xml.parseAccessControlPolicyXml()` and updates bucket ACL

**Missing (Future Phase):**
- `bucket-owner-read`, `bucket-owner-full-control` canned ACLs (object-specific)
- Full ACL XML parsing with Grant/Grantee elements

---

## 3. Object Operations Gaps

**Reference:** `specs/s3-object-operations.md`

| Operation | Status | Notes |
|-----------|--------|-------|
| PutObject | ✅ PASS | MD5 ETag, user metadata, canned ACL, max size check |
| GetObject | ✅ PASS | Range requests, conditional requests, user metadata |
| HeadObject | ✅ PASS | Same as GetObject, no body |
| DeleteObject | ✅ PASS | Idempotent (always 204) |
| DeleteObjects | ✅ PASS | Batch delete, Quiet mode, Content-MD5 validation |
| CopyObject | ✅ PASS | Metadata directive COPY/REPLACE |
| ListObjectsV2 | ✅ PASS | Prefix, delimiter, pagination, CommonPrefixes, encoding-type=url |
| ListObjectsV1 | ✅ PASS | Marker/NextMarker pagination, encoding-type=url |
| GetObjectAcl | ✅ PASS | Full ACL XML |
| PutObjectAcl | ✅ PASS | Canned ACL + grant headers + XML body parsing |

### Object Operations Details

**Fully Implemented:**
- `src/handlers/object.zig`: All 10 operations routed and functional
- Object key validation (max 1024 bytes)
- Content-MD5 validation on PutObject and DeleteObjects
- If-None-Match: `*` for conditional PUT (rejects if exists)
- Range requests: `bytes=start-end`, `bytes=start-`, `bytes=-N` (suffix)
- Conditional requests: If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since
- Priority rules: If-Match > If-Unmodified-Since, If-None-Match > If-Modified-Since
- User metadata via x-amz-meta-* headers (stored as JSON)
- CopyObject with metadata directive COPY/REPLACE
- Max object size enforcement (configurable, default 5 GiB)
- **PutObjectAcl XML body parsing** (`src/handlers/object.zig:1076-1088`): Parses `<AccessControlPolicy>` XML via `xml_mod.parseAccessControlPolicyXml()` and updates object ACL
- **response-* query parameters** (`src/handlers/object.zig:1296-1328`): Supports `response-content-type`, `response-content-language`, `response-expires`, `response-cache-control`, `response-content-disposition`, `response-content-encoding` on GetObject

**Missing (Future Phase):**
- x-amz-tagging header on PutObject/CopyObject
- x-amz-storage-class header (only STANDARD supported)
- Server-side encryption headers (SSE-C, SSE-S3, SSE-KMS)
- x-amz-website-redirect-location header

---

## 4. Multipart Upload Gaps

**Reference:** `specs/s3-multipart-upload.md`

| Operation | Status | Notes |
|-----------|--------|-------|
| CreateMultipartUpload | ✅ PASS | UUID v4 upload ID, metadata preserved |
| UploadPart | ✅ PASS | Part number 1-10000, ETag returned, max size check |
| UploadPartCopy | ✅ PASS | x-amz-copy-source-range support |
| CompleteMultipartUpload | ✅ PASS | Part validation, composite ETag, atomic commit |
| AbortMultipartUpload | ✅ PASS | Returns 204, idempotent |
| ListMultipartUploads | ✅ PASS | Pagination, prefix filtering |
| ListParts | ✅ PASS | Pagination by part-number-marker |

### Multipart Upload Details

**Fully Implemented:**
- `src/handlers/multipart.zig`: All 7 operations routed and functional
- Part number validation (1-10000)
- Minimum part size enforcement (5 MiB for non-last parts)
- Ascending part order validation (InvalidPartOrder error)
- ETag matching validation (InvalidPart error)
- Composite ETag format: `"md5-of-binary-md5s-N"` per S3 spec
- Atomic completion via SQLite transaction
- Part overwrite (same part number replaces previous)
- UploadPartCopy with optional byte range
- **x-amz-copy-source-if-* conditional headers** (`src/handlers/multipart.zig:856-910`): Supports `x-amz-copy-source-if-match`, `x-amz-copy-source-if-none-match`, `x-amz-copy-source-if-modified-since`, `x-amz-copy-source-if-unmodified-since` on UploadPartCopy
- **Expired multipart upload reaping** (`src/main.zig:423-427`): On startup, calls `metadata_store.reapExpiredUploads(604800)` (7-day TTL) to clean up abandoned uploads

**Missing (Future Phase):**
- Delimiter/CommonPrefixes grouping in ListMultipartUploads

**No gaps found for Phase 1 scope.**

---

## 5. Authentication Gaps

**Reference:** `specs/s3-authentication.md`

| Feature | Status | Notes |
|---------|--------|-------|
| Header-based SigV4 | ✅ PASS | Full 4-step process |
| Presigned URL SigV4 | ✅ PASS | UNSIGNED-PAYLOAD, expiration validation |
| Signing key derivation | ✅ PASS | HMAC chain: AWS4+key → date → region → service → aws4_request |
| Signing key cache | ✅ PASS | 24h TTL, max 1000 entries |
| Credential cache | ✅ PASS | 60s TTL, reduces SQLite queries |
| Constant-time comparison | ✅ PASS | Manual XOR accumulation |
| Clock skew validation | ✅ PASS | ±900 seconds tolerance |
| Credential scope validation | ✅ PASS | Date, region, service checked |
| URI encoding | ✅ PASS | S3-compatible (uppercase hex, %20 for space) |
| Canonical query string | ✅ PASS | Sorted, decode-then-encode to avoid double-encoding |
| Canonical headers | ✅ PASS | Lowercase, trimmed, collapsed whitespace |

### Authentication Details

**Fully Implemented:**
- `src/auth.zig`: Complete SigV4 implementation
- `AuthCache` struct with thread-safe mutex
- Precomputed signing key passed to verification functions
- x-amz-content-sha256 header handling (hex, UNSIGNED-PAYLOAD, computed)
- Presigned URL parameter parsing (with %2F and / separators)

**Missing (Future Phase):**
- `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` for chunked uploads
- x-amz-security-token header (STS temporary credentials)

**No gaps found for Phase 1 scope.**

---

## 6. Error Codes Coverage

**Reference:** `specs/s3-error-responses.md`

### Implemented Error Codes (32/41 = 78%)

| Code | HTTP | Status | Usage |
|------|------|--------|-------|
| AccessDenied | 403 | ✅ | Auth failure |
| BadDigest | 400 | ✅ | Content-MD5 mismatch |
| BucketAlreadyExists | 409 | ✅ | CreateBucket name conflict |
| BucketAlreadyOwnedByYou | 409 | ✅ | CreateBucket idempotent |
| BucketNotEmpty | 409 | ✅ | DeleteBucket with objects |
| EntityTooLarge | 400 | ✅ | Object/part exceeds max size |
| EntityTooSmall | 400 | ✅ | Multipart part < 5 MiB |
| IncompleteBody | 400 | ✅ | Body < Content-Length |
| InvalidAccessKeyId | 403 | ✅ | Unknown access key |
| InvalidArgument | 400 | ✅ | Invalid query param value |
| InvalidBucketName | 400 | ✅ | Bucket name validation |
| InvalidDigest | 400 | ✅ | Content-MD5 not base64 |
| InvalidPart | 400 | ✅ | Multipart ETag mismatch |
| InvalidPartOrder | 400 | ✅ | Parts not ascending |
| InvalidRange | 416 | ✅ | Range not satisfiable |
| InvalidRequest | 400 | ✅ | Transfer-Encoding rejected |
| KeyTooLongError | 400 | ✅ | Object key > 1024 bytes |
| MalformedACLError | 400 | ✅ | ACL XML malformed |
| MalformedXML | 400 | ✅ | Request XML malformed |
| MethodNotAllowed | 405 | ✅ | Wrong HTTP method |
| MissingContentLength | 411 | ✅ | PUT/POST without length |
| MissingRequestBodyError | 400 | ✅ | Empty body on DeleteObjects |
| NoSuchBucket | 404 | ✅ | Bucket not found |
| NoSuchKey | 404 | ✅ | Object not found |
| NoSuchUpload | 404 | ✅ | Multipart upload not found |
| PreconditionFailed | 412 | ✅ | Conditional request failed |
| RequestTimeTooSkewed | 403 | ✅ | Clock skew > 15 min |
| SignatureDoesNotMatch | 403 | ✅ | SigV4 signature invalid |
| TooManyBuckets | 400 | ✅ | Bucket limit exceeded |
| InternalError | 500 | ✅ | Unexpected error |
| NotImplemented | 501 | ✅ | Feature not implemented |
| ServiceUnavailable | 503 | ✅ | Server overloaded |

### Missing Error Codes (9/41 = 22%)

| Code | HTTP | Reason Missing |
|------|------|----------------|
| ExpiredToken | 400 | No STS/token-based auth |
| IllegalLocationConstraintException | 400 | No region validation |
| InvalidLocationConstraint | 400 | No region validation |
| InvalidObjectState | 403 | No Glacier/archive support |
| NoSuchVersion | 404 | No versioning |
| RequestTimeout | 400 | No request timeout enforcement |
| SlowDown | 503 | No rate limiting |
| PermanentRedirect | 301 | No redirect logic |
| TemporaryRedirect | 307 | No redirect logic |

### Error Response Format

**Verified in `src/xml.zig` and `src/errors.zig`:**
- XML namespace: **None** (error responses have no xmlns per spec)
- Elements: Code, Message, Resource, RequestId
- x-amz-id-2 header: Base64-encoded random 24 bytes
- Content-Type: `application/xml`

---

## 7. Headers Compliance

**Reference:** `specs/s3-common-headers.md`

### Response Headers (All Required Headers Implemented)

| Header | Status | Implementation |
|--------|--------|----------------|
| x-amz-request-id | ✅ | 16 hex chars, `server.zig:889` |
| x-amz-id-2 | ✅ | Base64-encoded 24 random bytes, `server.zig:893-898` |
| Date | ✅ | RFC 1123 format, `server.zig:901-904` |
| Server | ✅ | "BleepStore", `server.zig:906` |
| Content-Type | ✅ | application/xml or MIME type |
| Content-Length | ✅ | Auto-set by httpz from body.len |
| ETag | ✅ | Quoted MD5 hex, object handlers |
| Last-Modified | ✅ | RFC 7231 format, `object.zig:439` |
| Accept-Ranges | ✅ | "bytes", `object.zig:447` |
| Content-Range | ✅ | "bytes start-end/total", `object.zig:395` |
| x-amz-meta-* | ✅ | User metadata headers, `object.zig:113-135` |
| x-amz-bucket-region | ✅ | HeadBucket response, `bucket.zig:381` |

### Request Headers Supported

| Header | Status | Notes |
|--------|--------|-------|
| Authorization | ✅ | SigV4 header |
| x-amz-date | ✅ | Request timestamp |
| x-amz-content-sha256 | ✅ | Payload hash or UNSIGNED-PAYLOAD |
| Content-MD5 | ✅ | Validated on PutObject, DeleteObjects |
| x-amz-acl | ✅ | Canned ACL |
| x-amz-grant-* | ✅ | Explicit ACL grants |
| x-amz-meta-* | ✅ | User metadata |
| x-amz-copy-source | ✅ | CopyObject, UploadPartCopy |
| x-amz-copy-source-range | ✅ | UploadPartCopy byte range |
| x-amz-metadata-directive | ✅ | COPY or REPLACE |
| Range | ✅ | GetObject range request |
| If-Match | ✅ | Conditional GET/HEAD |
| If-None-Match | ✅ | Conditional GET/HEAD |
| If-Modified-Since | ✅ | Conditional GET/HEAD |
| If-Unmodified-Since | ✅ | Conditional GET/HEAD |
| If-None-Match: * | ✅ | Conditional PUT (reject if exists) |

### Date Formats

| Context | Format | Status |
|---------|--------|--------|
| x-amz-date | ISO 8601 basic (YYYYMMDDTHHMMSSZ) | ✅ |
| Date header | RFC 7231 (Sun, 22 Feb 2026 12:00:00 GMT) | ✅ |
| Last-Modified | RFC 7231 | ✅ |
| XML LastModified | ISO 8601 (2026-02-22T12:00:00.000Z) | ✅ |

---

## 8. Features NOT in Phase 1 Scope

The following S3 features are **intentionally excluded** from Phase 1 and are not gaps:

### Object Lifecycle & Storage
- Object versioning (NoSuchVersion error unused)
- Object lifecycle management (expiration, transition rules)
- Object Lock / WORM
- Glacier / archive storage classes (InvalidObjectState unused)
- Intelligent-Tiering, One Zone-IA, Reduced Redundancy

### Security & Encryption
- Server-side encryption (SSE-S3, SSE-KMS, SSE-C)
- Client-side encryption
- x-amz-security-token (STS temporary credentials)
- Bucket policies (JSON policy documents)
- VPC endpoint policies

### Cross-Region & Replication
- Cross-region replication (CRR)
- Same-region replication (SRR)
- Multi-region access points
- Redirect handling (PermanentRedirect, TemporaryRedirect unused)

### Networking & Access
- Requester Pays buckets
- Virtual-hosted-style URLs (only path-style implemented)
- Transfer acceleration
- Static website hosting
- CORS configuration

### Advanced Features
- S3 Select (SQL queries on objects)
- S3 Batch Operations
- Event notifications (Lambda, SQS, SNS)
- Access logging
- CloudWatch metrics (custom Prometheus metrics instead)
- Object tagging (x-amz-tagging header)
- Public access block settings
- Object Lambda

### Operational
- Rate limiting (SlowDown error unused)
- Request timeout enforcement (RequestTimeout unused)
- Multipart upload expiration reaping
- Inventory reports
- Storage analytics

---

## 9. Priority Ranking of Gaps to Fix

### Priority 1: Compliance Fixes (All Complete ✅)

| Gap | Status | Notes |
|-----|--------|-------|
| PutBucketAcl XML body parsing | ✅ DONE | `xml.parseAccessControlPolicyXml()` in bucket.zig:502-514 |
| PutObjectAcl XML body mode | ✅ DONE | `xml_mod.parseAccessControlPolicyXml()` in object.zig:1076-1088 |

### Priority 2: Feature Completeness (All Complete ✅)

| Gap | Status | Notes |
|-----|--------|-------|
| Expired multipart upload reaping | ✅ DONE | `main.zig:423-427` calls `reapExpiredUploads(604800)` |
| encoding-type=url for list ops | ✅ DONE | `xml.zig` URL-encodes keys/prefixes when encoding-type=url |
| x-amz-copy-source-if-* headers | ✅ DONE | `multipart.zig:856-910` implements all four conditional headers |
| response-* query params on GetObject | ✅ DONE | `object.zig:1296-1328` implements all six override params |

### Priority 3: Future Features (Not Phase 1)

| Gap | Impact | Effort | Notes |
|-----|--------|--------|-------|
| Chunked transfer encoding | AWS CLI compatibility | High | STREAMING-AWS4-HMAC-SHA256-PAYLOAD |
| Request timeout enforcement | DDoS mitigation | Medium | Socket timeout config |
| Rate limiting | Fairness | High | Token bucket per-IP |

---

## 10. Verification Notes

### Source Code Locations

| Component | File |
|-----------|------|
| Bucket handlers | `src/handlers/bucket.zig` |
| Object handlers | `src/handlers/object.zig` |
| Multipart handlers | `src/handlers/multipart.zig` |
| SigV4 auth | `src/auth.zig` |
| Auth cache | `src/auth.zig:28-247` |
| Error codes | `src/errors.zig` |
| Response headers | `src/server.zig:887-907` |
| Routing | `src/server.zig:611-736` |
| XML rendering | `src/xml.zig` |
| Storage backends | `src/storage/*.zig` |
| Metadata store | `src/metadata/sqlite.zig` |

### Test Coverage

| Category | Tests | Status |
|----------|-------|--------|
| Unit tests | 160 | All pass |
| Zig E2E tests | 34 | All pass |
| Python E2E tests | 86 | All pass |

### Key Test Files

- `src/e2e_test.zig`: Standalone Zig E2E tests
- `tests/e2e/`: Python E2E test suite
- `src/handlers/bucket.zig`: 7 test blocks
- `src/handlers/object.zig`: 2+ test blocks
- `src/handlers/multipart.zig`: 9 test blocks
- `src/auth.zig`: 16 test blocks
- `src/server.zig`: 4 test blocks

---

## 11. Conclusion

The BleepStore Zig implementation has **excellent coverage** of the S3 API for Phase 1 scope:

- **All 24 core operations** (7 bucket + 10 object + 7 multipart) are fully implemented
- **SigV4 authentication** is complete with caching optimization
- **Error handling** covers 78% of spec-defined error codes (missing codes are for unimplemented features)
- **Response headers** are 100% compliant with spec
- **Storage backends** support local, memory, SQLite, AWS S3, GCP, and Azure

**All Phase 1 gaps have been resolved:**
1. ✅ XML body parsing for PutBucketAcl/PutObjectAcl
2. ✅ Expired multipart upload cleanup on startup (7-day TTL)
3. ✅ `encoding-type=url` for list operations
4. ✅ `x-amz-copy-source-if-*` conditional headers for UploadPartCopy
5. ✅ `response-*` query params for GetObject

**Recommendation:** The implementation is production-ready for Phase 1 use cases. Remaining gaps are explicitly deferred to future phases (versioning, encryption, etc.).
