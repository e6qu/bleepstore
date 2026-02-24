# Stage 2: Metadata Store & SQLite

**Completed:** 2026-02-23

## Goal
SQLite-backed metadata store with full CRUD for buckets, objects, multipart uploads, and credentials. All unit-tested. No HTTP handler changes.

## Files Changed

| File | Change |
|------|--------|
| `go.mod` | Added `modernc.org/sqlite v1.34.5` dependency |
| `internal/metadata/store.go` | Expanded interface: 7 new methods, expanded record structs, added CredentialRecord |
| `internal/metadata/sqlite.go` | Full implementation: 6 tables, 20 methods, PRAGMAs, indexes |
| `internal/metadata/sqlite_test.go` | New file: 25+ test functions covering all operations |
| `internal/server/server.go` | Added `meta` field to Server, variadic MetadataStore parameter on New() |
| `cmd/bleepstore/main.go` | Initialize SQLiteStore on startup, seed default credentials |

## Implementation Details

### Database Schema
- 6 tables: `buckets`, `objects`, `multipart_uploads`, `multipart_parts`, `credentials`, `schema_version`
- PRAGMAs: WAL (concurrent readers), synchronous=NORMAL, foreign_keys=ON, busy_timeout=5000
- Indexes: `idx_objects_bucket`, `idx_objects_bucket_prefix`, `idx_uploads_bucket`, `idx_uploads_bucket_key`
- All timestamps as ISO 8601 TEXT with millisecond precision
- ACL and user_metadata as JSON TEXT columns

### MetadataStore Interface (20 methods)
- **Buckets (6):** CreateBucket, GetBucket, DeleteBucket, ListBuckets, BucketExists, UpdateBucketAcl
- **Objects (7):** PutObject, GetObject, DeleteObject, ObjectExists, DeleteObjectsMeta, UpdateObjectAcl, ListObjects
- **Multipart (7):** CreateMultipartUpload, GetMultipartUpload, PutPart, ListParts, GetPartsForCompletion, CompleteMultipartUpload, AbortMultipartUpload
- **Multipart list (1):** ListMultipartUploads
- **Credentials (2):** GetCredential, PutCredential

### Record Structs
- `BucketRecord`: Name, Region, OwnerID, OwnerDisplay, ACL (json.RawMessage), CreatedAt
- `ObjectRecord`: Bucket, Key, Size, ETag, ContentType, ContentEncoding, ContentLanguage, ContentDisposition, CacheControl, Expires, StorageClass, ACL, UserMetadata, LastModified, DeleteMarker
- `MultipartUploadRecord`: UploadID, Bucket, Key, ContentType, all content headers, StorageClass, ACL, UserMetadata, OwnerID, OwnerDisplay, InitiatedAt
- `PartRecord`: UploadID, PartNumber, Size, ETag, LastModified
- `CredentialRecord`: AccessKeyID, SecretKey, OwnerID, DisplayName, Active, CreatedAt

### Key Design Decisions
1. **Pure Go SQLite** (`modernc.org/sqlite`): No CGO required, cross-compiles easily
2. **Upload ID**: 32-char hex via `crypto/rand` (no external UUID dependency)
3. **CompleteMultipartUpload signature**: Accepts `*ObjectRecord` rather than computing inside metadata layer — handlers own business logic
4. **ListObjects with delimiter**: Application-level grouping for CommonPrefixes (not SQL-level)
5. **server.New() backward compatible**: Variadic MetadataStore parameter, existing tests unchanged
6. **Nullable columns**: `sql.NullString` for optional content headers (ContentEncoding, etc.)
7. **Delete bucket**: Checks both objects table and multipart_uploads table

### Test Coverage
- Bucket CRUD: create, get, delete, list (sorted), exists, duplicate create error
- Bucket constraints: delete non-empty (has objects), delete non-existent
- Bucket ACL: update, non-existent bucket error
- Object CRUD: full round-trip with all fields, upsert, idempotent delete
- Object batch: DeleteObjectsMeta with mixed existing/non-existing keys
- Object ACL: update, non-existent object error
- ListObjects: basic, prefix, delimiter with CommonPrefixes, pagination (3 pages), marker, empty bucket
- Multipart lifecycle: create, upload parts, list parts, get for completion, complete (transactional), verify cleanup
- Multipart abort: create, upload parts, abort, verify cleanup
- Multipart edge cases: abort not found, part overwrite, list parts pagination
- ListMultipartUploads: prefix, pagination
- Credential CRUD: create, get, update (upsert), active flag toggle
- Schema idempotency: NewSQLiteStore twice on same DB
- Object defaults: minimal put gets correct default values

## Issues Encountered
- None significant. The pure Go SQLite driver works well with `database/sql`.

## Dependencies Added
- `modernc.org/sqlite v1.34.5` (+ transitive dependencies resolved by `go mod tidy`)
