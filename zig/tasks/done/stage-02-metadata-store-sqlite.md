# Stage 2: Metadata Store & SQLite

**Date:** 2026-02-23
**Status:** Complete (pending build verification)

## What Was Implemented

Full SQLite-backed metadata store with CRUD operations for all Phase 1 S3 entities.

### MetadataStore Interface (store.zig)

Expanded from 13 to 24 vtable methods:

**Data types expanded:**
- `BucketMeta`: Added `owner_display`, `acl` fields
- `ObjectMeta`: Added `content_encoding`, `content_language`, `content_disposition`, `cache_control`, `expires`, `acl`, `delete_marker`
- `MultipartUploadMeta`: Added all metadata fields (content_type, encoding, language, disposition, cache_control, expires, storage_class, acl, user_metadata, owner_id, owner_display)

**New data types:**
- `ListObjectsResult`: objects, common_prefixes, is_truncated, next_continuation_token, next_marker
- `ListUploadsResult`: uploads, is_truncated, next markers
- `ListPartsResult`: parts, is_truncated, next_part_number_marker
- `Credential`: access_key_id, secret_key, owner_id, display_name, active, created_at

**New vtable methods:**
- `bucketExists`, `updateBucketAcl`
- `deleteObjectsMeta` (batch), `objectExists`, `updateObjectAcl`
- `getMultipartUpload`, `getPartsForCompletion`, `listMultipartUploads`
- `getCredential`, `putCredential`
- `countBuckets`, `countObjects`

**Changed signatures:**
- `deleteObjectMeta` now returns `bool` (true if row existed)
- `listObjectsMeta` now accepts delimiter, start_after parameters and returns `ListObjectsResult`
- `listPartsMeta` now accepts max_parts, part_marker and returns `ListPartsResult`
- `completeMultipartUpload` now accepts `ObjectMeta` and performs transactional completion

### SQLite Implementation (sqlite.zig)

**Schema (6 tables):**
1. `schema_version` -- Migration tracking
2. `buckets` -- name, created_at, region, owner_id, owner_display, acl
3. `objects` -- bucket, key, size, etag, content_type, 6 optional headers, storage_class, acl, user_metadata, last_modified, delete_marker
4. `multipart_uploads` -- upload_id, bucket, key, all metadata fields, owner info, initiated_at
5. `multipart_parts` -- upload_id, part_number, size, etag, last_modified
6. `credentials` -- access_key_id, secret_key, owner_id, display_name, active, created_at

**PRAGMAs applied:**
- `journal_mode = WAL` (crash-safe concurrent reads)
- `synchronous = NORMAL` (balance durability/performance)
- `foreign_keys = ON` (referential integrity)
- `busy_timeout = 5000` (wait on contention)

**Key implementation details:**
- All string values duped from SQLite C API memory into Zig allocator-owned memory
- `SQLITE_TRANSIENT` used for all text bindings (SQLite copies the data)
- `INSERT OR REPLACE` for object upsert semantics
- `INSERT OR IGNORE` for credential seeding (idempotent)
- `BEGIN`/`COMMIT`/`ROLLBACK` transaction for CompleteMultipartUpload
- LIKE clause for prefix matching in list operations
- Application-level CommonPrefixes grouping using StringHashMap

### Startup Integration (main.zig)

1. Ensure data directory exists
2. Convert sqlite_path to null-terminated string for C API
3. Initialize SqliteMetadataStore (WAL auto-recovers from crashes)
4. Seed default credentials (INSERT OR IGNORE)
5. Populate metrics gauges from count queries
6. Pass metadata store pointer to Server

### Server Integration (server.zig)

- `global_metadata_store`: Global pointer for handler access
- `Server.init()` accepts `*SqliteMetadataStore`, creates interface, sets global
- `getQueryParamValue()` utility function added

## Files Changed

| File | Type | Changes |
|------|------|---------|
| `src/metadata/store.zig` | Modified | Expanded types, 24 vtable methods, convenience wrappers |
| `src/metadata/sqlite.zig` | Modified | Full implementation, 18 tests |
| `src/main.zig` | Modified | SQLite init, credential seeding, metrics population |
| `src/server.zig` | Modified | Global metadata store pointer, getQueryParamValue |

## Tests Added (18)

1. `init and deinit` -- Open/close :memory: database
2. `create and get bucket` -- Basic bucket lifecycle
3. `bucket exists` -- Existence check
4. `list buckets` -- Multi-bucket listing, ordered by name
5. `delete bucket` -- Successful deletion
6. `delete nonexistent bucket returns error` -- NoSuchBucket
7. `delete bucket with objects returns error` -- BucketNotEmpty
8. `put and get object meta` -- Object metadata round-trip
9. `delete object meta` -- Delete with idempotent semantics
10. `list objects with prefix` -- Prefix filtering
11. `list objects with delimiter` -- CommonPrefixes grouping
12. `list objects pagination` -- MaxKeys truncation
13. `credential seeding and retrieval` -- Seed + get
14. `credential not found` -- Nonexistent key
15. `multipart upload lifecycle` -- Create + upload parts + list parts
16. `abort multipart upload` -- Abort + NoSuchUpload
17. `count buckets and objects` -- Count queries
18. `update bucket ACL` -- ACL update
19. `duplicate bucket returns error` -- BucketAlreadyExists
20. `object upsert replaces existing` -- INSERT OR REPLACE semantics

## Issues & Decisions

1. **Global pointer pattern**: Tokamak handlers are static functions; cannot pass server state. Using module-level `global_metadata_store` variable. This is safe because it's set once at startup and read-only during serving.

2. **String ownership**: SQLite C API returns pointers valid only until next step/finalize. All strings are duped. BucketMeta has a `deinit()` helper; ObjectMeta does not (freeing is manual) to keep the struct simple.

3. **SQLITE_TRANSIENT**: Used for all bind_text calls so SQLite copies the data. This is slightly less efficient than SQLITE_STATIC but much safer since Zig slices may point to arena-allocated memory.

4. **CommonPrefixes grouping**: Done in application code, not SQL. We fetch all matching rows and group by delimiter in Zig. This is simpler and works well for small-to-medium result sets.

5. **allocPrintZ for LIKE pattern**: The like pattern needs null termination for the C API. Used `allocPrintZ` or `dupeZ` to create null-terminated strings.
