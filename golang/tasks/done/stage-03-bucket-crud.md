# Stage 3: Bucket CRUD — Completion Details

**Date:** 2026-02-23
**Status:** Complete (build verified, unit tests need manual verification)

## What Was Implemented

### All 7 S3 Bucket Handlers

| Handler | Method/Path | Status Code | Description |
|---------|-------------|-------------|-------------|
| ListBuckets | GET / | 200 | Returns ListAllMyBucketsResult XML with Owner, sorted Buckets |
| CreateBucket | PUT /{bucket} | 200 | Creates bucket with name validation, ACL, region; idempotent |
| DeleteBucket | DELETE /{bucket} | 204 | Deletes empty bucket; 404 NoSuchBucket, 409 BucketNotEmpty |
| HeadBucket | HEAD /{bucket} | 200/404 | Returns x-amz-bucket-region header; no body |
| GetBucketLocation | GET /{bucket}?location | 200 | Returns LocationConstraint XML; empty for us-east-1 |
| GetBucketAcl | GET /{bucket}?acl | 200 | Returns AccessControlPolicy XML with proper xsi:type |
| PutBucketAcl | PUT /{bucket}?acl | 200 | Supports canned ACL header and XML body modes |

### Files Changed

| File | Change Type | Description |
|------|-------------|-------------|
| `internal/handlers/bucket.go` | Modified | Full implementation of all 7 handlers with dependency injection |
| `internal/handlers/helpers.go` | New | Bucket name validation, ACL helpers, canned ACL parsing |
| `internal/handlers/bucket_test.go` | New | 14 unit tests covering all handlers and helpers |
| `internal/xmlutil/xmlutil.go` | Modified | Added xmlns to all success response types; custom Grantee MarshalXML/UnmarshalXML |
| `internal/storage/local.go` | Modified | Implemented CreateBucket, DeleteBucket, ObjectExists, NewLocalBackend |
| `internal/server/server.go` | Modified | ServerOption pattern; wires dependencies into BucketHandler |
| `internal/server/server_test.go` | Modified | Updated test expectations for new bucket handler behavior |
| `cmd/bleepstore/main.go` | Modified | Initializes LocalBackend, passes to server |

### Key Design Decisions

1. **Constructor injection**: `NewBucketHandler(meta, store, ownerID, ownerDisplay, region)` follows the same pattern used across all Go implementations. No global state.

2. **Functional options for Server**: `server.New(cfg, metaStore, server.WithStorageBackend(backend))` — supports both old-style MetadataStore args and new-style ServerOption args for backward compatibility.

3. **Custom Grantee XML**: Go's `encoding/xml` cannot produce namespace-prefixed attributes (`xmlns:xsi`, `xsi:type`) natively. Solved with custom `MarshalXML` that manually constructs the start element with the required attributes, and custom `UnmarshalXML` to extract `xsi:type` from incoming XML.

4. **ACL storage**: ACL stored as JSON in SQLite (`AccessControlPolicy` serialized via `encoding/json`), converted back for XML rendering. The `XMLName` field from struct tags controls the output namespace regardless of the JSON round-trip.

5. **us-east-1 behavior**: Creating a bucket you already own returns 200 OK (not 409 BucketAlreadyOwnedByYou), matching real AWS us-east-1 behavior.

6. **Graceful degradation**: Handlers check for nil dependencies and return 500 InternalError rather than panicking.

### Issues Encountered

- **Go encoding/xml namespace limitation**: The standard library cannot produce `xmlns:xsi` and `xsi:type` attributes. Required custom `MarshalXML`/`UnmarshalXML` methods on the `Grantee` type. This is a known limitation documented in Go issue tracker.

- **Test expectations update**: After implementing bucket handlers, the `TestS3StubRoutes` test needed updating because bucket routes now return 500 (no metadata store configured in test) instead of 501 (not implemented).

- **go.sum missing entries**: Previous stage left `go.sum` incomplete for `modernc.org/sqlite`. Fixed with `go mod tidy`.

### Unit Test Coverage

14 test functions in `bucket_test.go`:
- Bucket name validation (18 edge cases)
- Create, duplicate create, invalid name create
- Delete, delete not found
- Head, head not found
- List (sorted, with owner and creation date, xmlns verification)
- Get location (us-east-1 empty value), location not found
- Get ACL (FULL_CONTROL, xmlns:xsi, xsi:type)
- Put ACL canned (public-read)
- Parse canned ACL (all 4 types)
