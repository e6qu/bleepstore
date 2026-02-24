# Stage 2: Metadata Store & SQLite

**Date:** 2026-02-23
**Status:** Complete

## What Was Implemented

Full SQLite-backed metadata CRUD layer using `aiosqlite`. No HTTP handler changes in this stage -- purely the metadata layer.

### New Files
- `src/bleepstore/metadata/models.py` -- 8 dataclass types for all metadata entities and list result containers
- `tests/test_metadata_sqlite.py` -- 64 tests across 7 test classes

### Modified Files
- `src/bleepstore/metadata/store.py` -- Expanded `MetadataStore` protocol with all CRUD methods (was minimal stubs)
- `src/bleepstore/metadata/sqlite.py` -- Full implementation (was all `raise NotImplementedError`)
- `src/bleepstore/metadata/__init__.py` -- Updated exports
- `pyproject.toml` -- Added `aiosqlite` dependency

## Key Decisions

1. **ACL and user_metadata as JSON text** -- Stored as JSON strings for flexibility, parsed at the application level. This avoids a separate ACL table and allows arbitrary metadata structures.

2. **Continuation token = last key** -- For ListObjectsV2, the continuation token is just the last returned key. This is simple, opaque to the caller, and works correctly with SQLite's `key > ?` ordering.

3. **Simplified put_part signature** -- `put_part(upload_id, part_number, size, etag)` instead of passing bucket/key since they are already on the upload record. This avoids redundancy.

4. **complete_multipart_upload accepts final object metadata** -- The complete method takes the full set of object fields (size, etag, content_type, etc.) rather than reading them from the upload record. This lets handlers compute composite ETags and total sizes before calling the metadata store.

5. **Explicit transaction in complete_multipart_upload** -- Uses `BEGIN`/`COMMIT`/`ROLLBACK` to atomically insert the final object and delete the upload+parts. This satisfies crash-only design: either the whole operation completes or nothing changes.

6. **max_keys=0 short-circuit** -- Returns empty results immediately instead of running a query. Edge case that would otherwise return results due to the LIMIT clause.

7. **Foreign key CASCADE** -- Bucket deletion automatically cascades to objects and multipart uploads, matching S3 behavior and simplifying cleanup.

## Schema

6 tables created:
- `buckets` (PK: name)
- `objects` (PK: bucket, key; FK: bucket -> buckets)
- `multipart_uploads` (PK: upload_id; FK: bucket -> buckets)
- `multipart_parts` (PK: upload_id, part_number; FK: upload_id -> multipart_uploads)
- `credentials` (PK: access_key_id)
- `schema_version` (PK: version)

4 indexes:
- `idx_objects_bucket`, `idx_objects_bucket_prefix`
- `idx_uploads_bucket`, `idx_uploads_bucket_key`

Pragmas: WAL journal mode, NORMAL synchronous, foreign keys ON, busy timeout 5000ms.

## Test Coverage

64 new tests (157 total, all passing):
- Schema idempotency (3)
- Bucket CRUD (12)
- Object CRUD (12)
- List objects with prefix/delimiter/pagination (11)
- Multipart lifecycle (14)
- Credentials (5)
- Edge cases (7)

## Issues Encountered

1. **max_keys=0 bug** -- Initial implementation did not handle max_keys=0, causing a test failure. Fixed by adding a short-circuit return at the top of `list_objects()`.

2. **list_objects truncation detection** -- Determining `is_truncated` with delimiter grouping is complex because multiple rows can collapse into a single CommonPrefix. The solution over-fetches rows and then runs a verification query to check if more data exists after the last returned key.
