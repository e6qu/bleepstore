# Stage 5a: List, Copy & Batch Delete

## Date: 2026-02-23

## Goal
Implement CopyObject, DeleteObjects (batch), ListObjectsV2, and ListObjects v1.

## What Was Implemented

### internal/handlers/object.go
- **CopyObject**: Parses `X-Amz-Copy-Source` header (URL-decoded via `url.PathUnescape`), splits into source bucket/key. Verifies both source and destination buckets exist and source object exists in metadata. Copies file data via `storage.CopyObject` (atomic temp-fsync-rename). Supports `x-amz-metadata-directive`: COPY (default, copies source metadata including Content-Type, user metadata, ACL) or REPLACE (uses request headers for new metadata). Commits destination metadata to SQLite. Returns `CopyObjectResult` XML with ETag and LastModified.
- **DeleteObjects**: Parses `<Delete>` XML body via `xml.NewDecoder(r.Body).Decode()`. Iterates keys, deleting metadata first (authoritative) then storage (best-effort). Supports `<Quiet>true</Quiet>` mode: in quiet mode, only errors are returned; in verbose mode (default), deleted keys are listed. Returns `DeleteResult` XML.
- **ListObjectsV2**: Reads query params `prefix`, `delimiter`, `max-keys` (default 1000), `start-after`, `continuation-token`, `encoding-type`. Calls `meta.ListObjects` with `ListObjectsOptions`. Renders `ListBucketV2Result` XML with `KeyCount`, `MaxKeys`, `IsTruncated`, `NextContinuationToken`, `Contents`, `CommonPrefixes`.
- **ListObjects (v1)**: Reads query params `prefix`, `delimiter`, `max-keys` (default 1000), `marker`. Calls `meta.ListObjects` with `ListObjectsOptions`. Renders `ListBucketResult` XML with `Marker`, `NextMarker`, `MaxKeys`, `IsTruncated`, `Contents`, `CommonPrefixes`.

### internal/handlers/helpers.go
- **parseDeleteRequest**: New helper that parses a Delete XML request body into a `DeleteRequest` struct using `xml.NewDecoder`.
- **parseCopySource**: New helper that parses the `X-Amz-Copy-Source` header. URL-decodes with `url.PathUnescape`, trims leading slash, splits into bucket/key at first `/`. Returns `(bucket, key, ok)`.
- Added imports: `encoding/xml`, `io`, `net/url`.

### internal/xmlutil/xmlutil.go
- **DeleteRequest**: New struct for parsing `<Delete>` XML input body with `Quiet` bool and `Objects` slice of `DeleteRequestObj` (each with `Key`).
- **DeleteRequestObj**: New struct representing a single object key in a delete request.
- **ListBucketResult**: Added `NextMarker` field (omitempty) and `Delimiter` field (omitempty).
- **ListBucketV2Result**: Added `Delimiter` field (omitempty) and `EncodingType` field (omitempty).

### internal/handlers/object_test.go
Added 18 new test functions:
- **CopyObject tests**: TestCopyObject (basic copy with COPY directive), TestCopyObjectWithReplaceDirective (REPLACE metadata), TestCopyObjectNonexistentSource (404 NoSuchKey), TestCopyObjectInvalidSource (400 InvalidArgument)
- **DeleteObjects tests**: TestDeleteObjects (batch delete 3 objects, verify all reported deleted and actually gone), TestDeleteObjectsQuietMode (quiet mode suppresses Deleted entries), TestDeleteObjectsMalformedXML (400 MalformedXML)
- **ListObjectsV2 tests**: TestListObjectsV2 (8 objects, verify all keys and KeyCount), TestListObjectsV2WithPrefix (filter by prefix), TestListObjectsV2WithDelimiter (CommonPrefixes), TestListObjectsV2Pagination (MaxKeys=2, IsTruncated, NextContinuationToken), TestListObjectsV2EmptyBucket (KeyCount=0), TestListObjectsV2StartAfter (skip objects), TestListObjectsV2ContentFields (Key, LastModified, ETag, Size, StorageClass), TestListObjectsV2NoSuchBucket (404)
- **ListObjects V1 tests**: TestListObjectsV1 (basic listing), TestListObjectsV1WithMarker (pagination with MaxKeys=2)
- **Helper tests**: TestParseCopySource (7 table-driven cases including URL encoding)
- **putTestObjects**: New test helper to create multiple test objects.

## Key Decisions
- CopyObject defaults to COPY directive (matches S3 behavior). Only COPY and REPLACE are supported.
- DeleteObjects processes keys sequentially. Metadata deletion is authoritative; storage deletion is best-effort.
- Quiet mode in DeleteObjects: successful deletes are not reported, only errors.
- ListObjectsV2 uses `start-after` for initial page and `continuation-token` for subsequent pages (continuation token takes precedence in the metadata layer).
- ListObjects v1 uses `marker` for pagination.
- `max-keys` defaults to 1000 for both list variants (S3 default).
- No new dependencies added -- all stdlib (`encoding/xml`, `net/url`, `strconv`).

## Files Changed
- `internal/handlers/object.go` -- 4 handlers implemented (CopyObject, DeleteObjects, ListObjectsV2, ListObjects)
- `internal/handlers/helpers.go` -- 2 new helpers (parseDeleteRequest, parseCopySource) + 3 new imports
- `internal/xmlutil/xmlutil.go` -- 2 new structs (DeleteRequest, DeleteRequestObj) + 4 new fields on list result structs
- `internal/handlers/object_test.go` -- 18 new test functions + 1 test helper

## Dependencies
- No new dependencies. All implementations use stdlib: `encoding/xml`, `net/url`, `strconv`.

## Verification
```bash
cd golang/
go build ./cmd/bleepstore
go test -v -count=1 ./...
```
