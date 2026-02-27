# Stage 16: S3 API Completeness

**Date:** 2026-02-27
**Status:** NOT STARTED
**Milestone:** 9b — S3 API Completeness

## Objective

Close remaining S3 API gaps identified in `S3_GAP_REMAINING.md` to increase in-scope S3 API coverage from 96% to ~99%.

All 24 core S3 operations are implemented. This stage adds:
- Optional parameters for list operations
- Additional error codes
- Conditional copy headers
- Multipart upload expiration
- Response override parameters

## Tasks

### 1. Error Codes (P2)

- [ ] **Add `InvalidRequest` error code** (`src/errors.rs`)
  - HTTP 400
  - Use for generic validation failures not covered by specific codes
  - Add unit test

- [ ] **Add `InvalidLocationConstraint` error code** (`src/errors.rs`, `src/handlers/bucket.rs`)
  - HTTP 400
  - Return when region validation fails in CreateBucket
  - Add unit test

### 2. Multipart Upload Expiration (P2)

- [ ] **Add config option** (`src/config.rs`)
  - `multipart_upload_expiry_days: u64` (default: 7)

- [ ] **Implement expiration on startup** (`src/main.rs`, `src/metadata/sqlite.rs`)
  - On startup: `DELETE FROM multipart_uploads WHERE created_at < datetime('now', '-N days')`
  - Also delete associated parts from `multipart_parts` table (cascade or explicit)
  - Clean up orphaned part files from storage (call `storage.delete_parts()` for each expired upload)

- [ ] **Add unit tests**
  - Test expired uploads are deleted
  - Test non-expired uploads remain

### 3. encoding-type=url for List Operations (P3)

- [ ] **Parse encoding-type parameter** (`src/handlers/object.rs`)
  - `encoding-type` query param in `list_objects_v1` and `list_objects_v2`
  - Values: `url` or absent

- [ ] **URL-encode keys when requested** (`src/xml.rs`)
  - When `encoding-type=url`, URL-encode `<Key>` and `<Prefix>` elements
  - Use `percent-encoding` crate (already in deps)

- [ ] **Return EncodingType element** (`src/xml.rs`)
  - Include `<EncodingType>url</EncodingType>` in response when encoding-type=url

- [ ] **Add unit tests**
  - Test keys with special characters (spaces, unicode) are URL-encoded
  - Test normal keys unchanged when encoding-type absent

### 4. Conditional Copy Headers (P3)

- [ ] **Parse conditional headers** (`src/handlers/object.rs`)
  - `x-amz-copy-source-if-match`: ETag list
  - `x-amz-copy-source-if-none-match`: ETag list
  - `x-amz-copy-source-if-modified-since`: RFC 7231 date
  - `x-amz-copy-source-if-unmodified-since`: RFC 7231 date

- [ ] **Evaluate conditions against source object** (`src/handlers/object.rs`)
  - Reuse existing `evaluate_conditions()` helper
  - Return 412 PreconditionFailed on failure
  - Return 304 Not Modified on If-None-Match match (for GET semantics)

- [ ] **Add unit tests**
  - Test if-match success/failure
  - Test if-none-match success/304
  - Test if-modified-since
  - Test if-unmodified-since

### 5. fetch-owner for ListObjectsV2 (P4)

- [ ] **Parse fetch-owner parameter** (`src/handlers/object.rs`)
  - `fetch-owner` query param (boolean)

- [ ] **Include Owner element in Contents** (`src/xml.rs`)
  - When `fetch-owner=true`, add `<Owner><ID>...</ID><DisplayName>...</DisplayName></Owner>` to each `<Contents>`

- [ ] **Add unit tests**
  - Test Owner element present when fetch-owner=true
  - Test Owner element absent when fetch-owner=false/absent

### 6. response-* Override Parameters (P4)

- [ ] **Parse response-* query params** (`src/handlers/object.rs`)
  - `response-content-type`
  - `response-content-language`
  - `response-expires`
  - `response-cache-control`
  - `response-content-disposition`
  - `response-content-encoding`

- [ ] **Override headers in GetObject response** (`src/handlers/object.rs`)
  - If param present, override corresponding response header
  - Primarily useful for presigned URLs

- [ ] **Add unit tests**
  - Test each override param sets correct header
  - Test override takes precedence over stored metadata

### 7. ListBuckets Pagination (P4)

- [ ] **Parse pagination params** (`src/handlers/bucket.rs`)
  - `continuation-token`: base64-encoded bucket name to start after
  - `max-buckets`: max results (default 1000)
  - `prefix`: filter by prefix

- [ ] **Update metadata query** (`src/metadata/sqlite.rs`)
  - `list_buckets_paginated(prefix, start_after, max_buckets) -> ListBucketsResult`
  - Return `is_truncated`, `next_continuation_token`

- [ ] **Update XML rendering** (`src/xml.rs`)
  - Add `ContinuationToken`, `NextContinuationToken`, `MaxBuckets`, `IsTruncated` elements

- [ ] **Add unit tests**
  - Test pagination with continuation token
  - Test prefix filtering
  - Test max-buckets limit

## Files to Modify

| File | Changes |
|------|---------|
| `src/errors.rs` | Add `InvalidRequest`, `InvalidLocationConstraint` |
| `src/config.rs` | Add `multipart_upload_expiry_days` |
| `src/main.rs` | Call multipart expiration on startup |
| `src/handlers/object.rs` | encoding-type, fetch-owner, response-*, conditional copy |
| `src/handlers/bucket.rs` | ListBuckets pagination, InvalidLocationConstraint |
| `src/metadata/sqlite.rs` | multipart expiration query, list_buckets_paginated |
| `src/xml.rs` | encoding-type, fetch-owner, ListBuckets pagination |

## Testing Requirements

### Unit Tests

Each task has associated unit tests (see checklist above). Run with:
```bash
cargo test
```

### E2E Tests

All 86 E2E tests must pass with no regressions:
```bash
./run_e2e.sh
```

### Manual Verification

```bash
# encoding-type=url
aws s3api list-objects-v2 --bucket test --encoding-type url

# fetch-owner
aws s3api list-objects-v2 --bucket test --fetch-owner

# Conditional copy
aws s3api copy-object --copy-source bucket/key --destination bucket/key2 \
  --copy-source-if-match '"etag..."'

# Response overrides (presigned URL example)
# Generate presigned URL with response-content-type override
```

## Acceptance Criteria

- [ ] All 8 gaps implemented
- [ ] All unit tests pass
- [ ] All 86 E2E tests pass (no regressions)
- [ ] S3 API coverage reaches ~99% (per `S3_GAP_REMAINING.md`)
- [ ] No new dependencies required

## Out of Scope

The following are explicitly out of scope for this stage:
- Versioning (GetBucketVersioning, PutBucketVersioning, ListObjectVersions)
- Lifecycle management (GetBucketLifecycle, PutBucketLifecycle)
- Server-side encryption (SSE-S3, SSE-KMS, SSE-C)
- Replication (GetBucketReplication, PutBucketReplication)
- Clustering (Raft, leader election, log replication)
- Event queues (Redis, RabbitMQ, Kafka)
