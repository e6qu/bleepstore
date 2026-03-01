# Stage 16: S3 API Completeness

**Status:** NOT STARTED
**Prerequisites:** Stage 15 COMPLETE (86/86 E2E tests passing)
**Reference:** `S3_GAP_REMAINING.md`

---

## Objective

Close remaining S3 API gaps to improve SDK compatibility and operational hygiene. This stage addresses the high and medium priority gaps identified in the gap analysis, excluding features explicitly out of scope (versioning, lifecycle, SSE, replication, clustering).

---

## Tasks

### Task 1: CopyObject Conditional Headers

**Priority:** MEDIUM
**Effort:** LOW (~2-3 hours)

Implement conditional copy headers that allow clients to copy objects only if certain conditions are met.

#### Headers to Support

| Header | Type | Behavior |
|--------|------|----------|
| `x-amz-copy-source-if-match` | ETag list | Copy only if source object ETag matches one in list |
| `x-amz-copy-source-if-none-match` | ETag list | Copy only if source object ETag matches NONE in list |
| `x-amz-copy-source-if-modified-since` | Timestamp | Copy only if source modified after timestamp |
| `x-amz-copy-source-if-unmodified-since` | Timestamp | Copy only if source NOT modified after timestamp |

#### Conditional Priority (per S3 spec)

1. If both `If-Match` and `If-Unmodified-Since` are present, only `If-Match` is evaluated
2. If both `If-None-Match` and `If-Modified-Since` are present, only `If-None-Match` is evaluated

#### Files to Modify

| File | Changes |
|------|---------|
| `internal/handlers/object.go` | Add conditional checks in `CopyObject()` before performing copy |
| `internal/handlers/multipart.go` | Add conditional checks in `UploadPartCopy()` |
| `internal/handlers/helpers.go` | Add `CheckCopySourceConditionals(r, sourceETag, sourceLastMod) (int, bool)` |

#### Implementation Details

```go
// helpers.go
func CheckCopySourceConditionals(r *http.Request, etag string, lastMod time.Time) (statusCode int, shouldProceed bool) {
    ifMatch := r.Header.Get("x-amz-copy-source-if-match")
    ifNoneMatch := r.Header.Get("x-amz-copy-source-if-none-match")
    ifModSince := r.Header.Get("x-amz-copy-source-if-modified-since")
    ifUnmodSince := r.Header.Get("x-amz-copy-source-if-unmodified-since")
    
    // Priority: If-Match > If-Unmodified-Since
    if ifMatch != "" {
        if !etagMatchesList(etag, ifMatch) {
            return http.StatusPreconditionFailed, false // 412 PreconditionFailed
        }
        return 0, true // Proceed
    }
    
    if ifUnmodSince != "" {
        ts, _ := http.ParseTime(ifUnmodSince)
        if lastMod.After(ts) {
            return http.StatusPreconditionFailed, false // 412 PreconditionFailed
        }
    }
    
    // Priority: If-None-Match > If-Modified-Since
    if ifNoneMatch != "" {
        if etagMatchesList(etag, ifNoneMatch) {
            return http.StatusPreconditionFailed, false // 412 PreconditionFailed
        }
        return 0, true // Proceed
    }
    
    if ifModSince != "" {
        ts, _ := http.ParseTime(ifModSince)
        if !lastMod.After(ts) {
            return http.StatusPreconditionFailed, false // 412 PreconditionFailed
        }
    }
    
    return 0, true // Proceed
}
```

#### Error Response

- **Code:** `PreconditionFailed`
- **HTTP Status:** 412
- **Message:** "At least one of the pre-conditions you specified did not hold"

#### Unit Tests

- `TestCopyObjectIfMatchSuccess` -- ETag matches, copy proceeds
- `TestCopyObjectIfMatchFail` -- ETag doesn't match, 412 error
- `TestCopyObjectIfNoneMatchSuccess` -- ETag doesn't match, copy proceeds
- `TestCopyObjectIfNoneMatchFail` -- ETag matches, 412 error
- `TestCopyObjectIfModifiedSinceSuccess` -- Modified after timestamp, copy proceeds
- `TestCopyObjectIfModifiedSinceFail` -- Not modified after timestamp, 412 error
- `TestCopyObjectIfUnmodifiedSinceSuccess` -- Not modified after timestamp, copy proceeds
- `TestCopyObjectIfUnmodifiedSinceFail` -- Modified after timestamp, 412 error
- `TestCopyObjectConditionalPriority` -- If-Match takes precedence over If-Unmodified-since

---

### Task 2: Expired Multipart Upload Reaping

**Priority:** MEDIUM
**Effort:** MEDIUM (~4-6 hours)

Implement automatic cleanup of stale multipart uploads on server startup (crash-only design principle: every startup is recovery).

#### Design

| Parameter | Default | Config Key |
|-----------|---------|------------|
| TTL | 7 days | `multipart_upload_ttl_days` |
| Trigger | Server startup | Automatic |

#### Files to Modify

| File | Changes |
|------|---------|
| `internal/metadata/store.go` | Add `DeleteExpiredUploads(ctx, ttl time.Duration) ([]string, error)` |
| `internal/metadata/sqlite.go` | Implement: SELECT expired uploads, DELETE parts, DELETE uploads in transaction |
| `internal/storage/backend.go` | Add `DeletePartsForUploads(ctx, uploadIDs []string) error` |
| `internal/storage/local.go` | Implement: Remove `.multipart/{uploadID}/` directories for each upload ID |
| `internal/storage/memory.go` | Implement: Remove parts for each upload ID |
| `internal/storage/sqlite.go` | Implement: Remove part blobs for each upload ID |
| `internal/storage/aws.go` | Implement: List and delete parts for each upload ID |
| `internal/storage/gcp.go` | Implement: List and delete parts for each upload ID |
| `internal/storage/azure.go` | Implement: No-op (Azure auto-expires uncommitted blocks) |
| `internal/config/config.go` | Add `MultipartUploadTTLDays int` with default 7 |
| `cmd/bleepstore/main.go` | Call reaper after storage/metadata init, log count cleaned |

#### Implementation Details

```go
// metadata/sqlite.go
func (s *SQLiteStore) DeleteExpiredUploads(ctx context.Context, ttl time.Duration) ([]string, error) {
    cutoff := time.Now().UTC().Add(-ttl)
    
    // Start transaction
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, err
    }
    defer tx.Rollback()
    
    // Get expired upload IDs
    rows, err := tx.QueryContext(ctx, 
        "SELECT upload_id FROM multipart_uploads WHERE created_at < ?", cutoff)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var uploadIDs []string
    for rows.Next() {
        var id string
        if err := rows.Scan(&id); err != nil {
            return nil, err
        }
        uploadIDs = append(uploadIDs, id)
    }
    
    if len(uploadIDs) == 0 {
        return nil, nil
    }
    
    // Delete parts
    _, err = tx.ExecContext(ctx,
        "DELETE FROM multipart_parts WHERE upload_id IN ("+placeholders(len(uploadIDs))+")",
        uploadIDs...)
    if err != nil {
        return nil, err
    }
    
    // Delete uploads
    _, err = tx.ExecContext(ctx,
        "DELETE FROM multipart_uploads WHERE upload_id IN ("+placeholders(len(uploadIDs))+")",
        uploadIDs...)
    if err != nil {
        return nil, err
    }
    
    if err := tx.Commit(); err != nil {
        return nil, err
    }
    
    return uploadIDs, nil
}
```

#### Unit Tests

- `TestDeleteExpiredUploadsNone` -- No expired uploads
- `TestDeleteExpiredUploadsOne` -- One expired upload deleted
- `TestDeleteExpiredUploadsMultiple` -- Multiple expired uploads deleted
- `TestDeleteExpiredUploadsPreservesRecent` -- Recent uploads not deleted
- `TestStorageDeletePartsForUploads` -- Part directories removed

---

### Task 3: encoding-type=url Support

**Priority:** LOW
**Effort:** LOW (~2 hours)

Support URL-encoding of object keys in list operations when `encoding-type=url` is specified.

#### Operations to Update

| Operation | Query Param | Response Field |
|-----------|-------------|----------------|
| `ListObjectsV2` | `encoding-type` | `EncodingType` in XML + URL-encoded `Key` |
| `ListObjects` | `encoding-type` | `EncodingType` in XML + URL-encoded `Key` |
| `ListMultipartUploads` | `encoding-type` | `EncodingType` in XML + URL-encoded `Key` |
| `ListParts` | `encoding-type` | `EncodingType` in XML (parts don't have keys) |

#### Files to Modify

| File | Changes |
|------|---------|
| `internal/handlers/object.go` | Parse `encoding-type` in `ListObjectsV2`, `ListObjects`; apply URL encoding |
| `internal/handlers/multipart.go` | Parse `encoding-type` in `ListMultipartUploads` |
| `internal/xmlutil/xmlutil.go` | Add `EncodingType string` field to list result structs |

#### Implementation Details

```go
// In handler, check encoding-type
encodingType := r.URL.Query().Get("encoding-type")
if encodingType != "" && encodingType != "url" {
    // S3 returns InvalidEncodingType for anything other than "url"
    xmlutil.WriteErrorResponse(w, r, errors.ErrInvalidEncodingType)
    return
}

// When building response
if encodingType == "url" {
    result.EncodingType = "url"
    for i, obj := range result.Contents {
        result.Contents[i].Key = url.QueryEscape(obj.Key)
    }
    for i, prefix := range result.CommonPrefixes {
        result.CommonPrefixes[i].Prefix = url.QueryEscape(prefix.Prefix)
    }
}
```

#### Error to Add

```go
// errors/errors.go
var ErrInvalidEncodingType = &S3Error{
    Code:           "InvalidEncodingType",
    HTTPStatusCode: 400,
    Message:        "Invalid encoding type specified",
}
```

#### Unit Tests

- `TestListObjectsV2EncodingTypeURL` -- Keys are URL-encoded
- `TestListObjectsV2EncodingTypeNone` -- Keys are NOT encoded (default)
- `TestListObjectsV2EncodingTypeInvalid` -- Returns `InvalidEncodingType` error
- `TestListMultipartUploadsEncodingTypeURL` -- Keys are URL-encoded

---

### Task 4: Missing Error Codes (In-Scope)

**Priority:** LOW
**Effort:** LOW (~30 min)

Add error code that can be used without implementing out-of-scope features.

#### Error Code to Add

| Code | HTTP | Description |
|------|------|-------------|
| `RequestTimeout` | 400 | Request did not complete within timeout |

#### Files to Modify

| File | Changes |
|------|---------|
| `internal/errors/errors.go` | Add `ErrRequestTimeout` |

#### Implementation

```go
var ErrRequestTimeout = &S3Error{
    Code:           "RequestTimeout",
    HTTPStatusCode: 400,
    Message:        "Your socket connection to the server was not read from or written to within the timeout period.",
}
```

**Note:** This just defines the error code. Actual timeout enforcement is optional and can be done via HTTP server timeouts (already configured in Stage 15).

---

## Files Summary

| File | Task | Changes |
|------|------|---------|
| `internal/handlers/object.go` | 1, 3 | Conditional headers, encoding-type |
| `internal/handlers/multipart.go` | 1, 3 | Conditional headers, encoding-type |
| `internal/handlers/helpers.go` | 1 | `CheckCopySourceConditionals()` helper |
| `internal/metadata/store.go` | 2 | `DeleteExpiredUploads()` interface method |
| `internal/metadata/sqlite.go` | 2 | Implement expired upload cleanup |
| `internal/storage/backend.go` | 2 | `DeletePartsForUploads()` interface method |
| `internal/storage/local.go` | 2 | Implement bulk part cleanup |
| `internal/storage/memory.go` | 2 | Implement bulk part cleanup |
| `internal/storage/sqlite.go` | 2 | Implement bulk part cleanup |
| `internal/storage/aws.go` | 2 | Implement bulk part cleanup |
| `internal/storage/gcp.go` | 2 | Implement bulk part cleanup |
| `internal/storage/azure.go` | 2 | No-op (Azure auto-expires) |
| `internal/config/config.go` | 2 | `MultipartUploadTTLDays` config |
| `internal/xmlutil/xmlutil.go` | 3 | `EncodingType` field in structs |
| `internal/errors/errors.go` | 3, 4 | `InvalidEncodingType`, `RequestTimeout` |
| `cmd/bleepstore/main.go` | 2 | Call reaper on startup |

---

## Testing Requirements

### Unit Tests

```bash
go test -v -race ./internal/handlers/
go test -v -race ./internal/metadata/
go test -v -race ./internal/storage/
go test -v -race ./internal/errors/
```

### E2E Tests

```bash
./run_e2e.sh
# Must still pass: 86/86
```

### New E2E Tests to Write (Optional)

If time permits, add E2E tests for:
- `test_copy_object_if_match`
- `test_copy_object_if_none_match`
- `test_list_objects_encoding_type_url`

---

## Definition of Done

- [ ] CopyObject respects all 4 `x-amz-copy-source-if-*` headers
- [ ] UploadPartCopy respects all 4 `x-amz-copy-source-if-*` headers
- [ ] Expired multipart uploads (>7 days) cleaned on startup
- [ ] Configurable TTL for multipart upload expiration
- [ ] `encoding-type=url` supported in ListObjectsV2
- [ ] `encoding-type=url` supported in ListObjects V1
- [ ] `encoding-type=url` supported in ListMultipartUploads
- [ ] `InvalidEncodingType` error for invalid encoding-type values
- [ ] `RequestTimeout` error code defined
- [ ] All 86 E2E tests still pass
- [ ] All new unit tests pass with race detector

---

## Out of Scope

The following are explicitly NOT part of this stage (see `S3_GAP_REMAINING.md` Section 8):

- Object versioning (`x-amz-version-id`)
- Lifecycle configuration
- Server-side encryption (SSE-S3, SSE-KMS, SSE-C)
- Replication (CRR/SRR)
- Clustering / Raft consensus
- Glacier/archive storage
- Rate limiting (`SlowDown` error)
- STS session tokens
- aws-chunked transfer encoding
- Redirects (301/307)
