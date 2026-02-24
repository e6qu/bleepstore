# Stage 2: Metadata Store & SQLite

## Date: 2026-02-23

## Goal
Full SQLite-backed metadata CRUD. No HTTP handler changes.

## What Was Implemented

### src/metadata/store.rs -- MetadataStore trait expansion

**Record types expanded:**
- `BucketRecord`: Added `owner_id`, `owner_display`, `acl` fields
- `ObjectRecord`: Added `content_encoding`, `content_language`, `content_disposition`, `cache_control`, `expires`, `storage_class`, `acl`, `delete_marker` fields. Removed `storage_key` (metadata layer is storage-backend-agnostic).
- `MultipartUploadRecord`: Added full field set (`content_type`, `content_encoding`, `content_language`, `content_disposition`, `cache_control`, `expires`, `storage_class`, `acl`, `user_metadata`, `owner_id`, `owner_display`).
- `PartRecord`: Simplified to `part_number`, `size`, `etag`, `last_modified`. Removed `storage_key`.

**New types added:**
- `CredentialRecord`: `access_key_id`, `secret_key`, `owner_id`, `display_name`, `active`, `created_at`
- `Acl`, `AclOwner`, `AclGrant`, `AclGrantee` -- JSON-serializable ACL types with serde
- `ListObjectsResult`: `objects`, `common_prefixes`, `next_continuation_token`, `is_truncated`
- `ListUploadsResult`: `uploads`, `is_truncated`, `next_key_marker`, `next_upload_id_marker`
- `ListPartsResult`: `parts`, `is_truncated`, `next_part_number_marker`

**Trait methods added:**
- `bucket_exists(name) -> bool`
- `update_bucket_acl(name, acl)`
- `object_exists(bucket, key) -> bool`
- `update_object_acl(bucket, key, acl)`
- `delete_objects(bucket, keys) -> Vec<String>` (batch delete)
- `count_objects(bucket) -> u64`
- `get_multipart_upload(upload_id) -> Option<MultipartUploadRecord>`
- `get_parts_for_completion(upload_id) -> Vec<PartRecord>`
- `get_credential(access_key_id) -> Option<CredentialRecord>`
- `put_credential(record)`

**Trait method signatures changed:**
- `list_objects`: Added `start_after` param, returns `ListObjectsResult` instead of tuple
- `list_parts`: Added `max_parts` and `part_number_marker` params, returns `ListPartsResult`
- `list_multipart_uploads`: Added `prefix`, `max_uploads`, `key_marker`, `upload_id_marker` params, returns `ListUploadsResult`
- `complete_multipart_upload`: Now accepts `ObjectRecord` (the assembled final object) instead of part tuples. Assembly logic belongs at the handler/storage level, not metadata level.

### src/metadata/sqlite.rs -- Full SQLite implementation

**Schema (6 tables):**
1. `schema_version(version INTEGER PK, applied_at TEXT)` -- Migration tracking
2. `buckets(name TEXT PK, region, owner_id, owner_display, acl, created_at)` -- Full spec
3. `objects(bucket, key, size, etag, content_type, content_encoding, content_language, content_disposition, cache_control, expires, storage_class, acl, user_metadata, last_modified, delete_marker)` -- PK(bucket, key), FK(bucket)
4. `multipart_uploads(upload_id TEXT PK, bucket, key, content_type, ..., owner_id, owner_display, initiated_at)` -- FK(bucket)
5. `multipart_parts(upload_id, part_number, size, etag, last_modified)` -- PK(upload_id, part_number), FK(upload_id) CASCADE
6. `credentials(access_key_id TEXT PK, secret_key, owner_id, display_name, active, created_at)`

**Indexes:**
- `idx_objects_bucket` ON objects(bucket)
- `idx_objects_bucket_prefix` ON objects(bucket, key)
- `idx_uploads_bucket` ON multipart_uploads(bucket)
- `idx_uploads_bucket_key` ON multipart_uploads(bucket, key)

**Pragmas:**
- `journal_mode = WAL` (concurrent readers)
- `synchronous = NORMAL` (durability/performance balance)
- `foreign_keys = ON` (referential integrity)
- `busy_timeout = 5000` (5s contention wait)

**Key implementation decisions:**
1. **Timestamp formatting without chrono**: Implemented custom `format_timestamp()` using Howard Hinnant's date algorithm to avoid adding `chrono` as a dependency. Produces ISO-8601 with milliseconds.
2. **Delimiter grouping at application level**: The `list_objects` method fetches all matching rows from SQLite, then groups by common prefix using a `BTreeSet` in Rust. This matches the spec's note that delimiter grouping requires application-level logic.
3. **Transactional complete_multipart_upload**: Uses `BEGIN IMMEDIATE` / `COMMIT` / `ROLLBACK` pattern rather than `rusqlite::Transaction` to keep the code synchronous within the mutex.
4. **Foreign key CASCADE**: Parts are automatically deleted when their upload is deleted. Objects are cascaded when a bucket is deleted.
5. **Closure type issue**: Rust closures have unique types even when identical. The `list_multipart_uploads` method uses a named `fn` instead of closures to share the row-mapping logic across multiple `query_map` calls with different parameters.

## Files Changed
- `src/metadata/store.rs` -- Complete rewrite (trait + types)
- `src/metadata/sqlite.rs` -- Complete rewrite (implementation + 31 tests)

## Files NOT Changed
- No HTTP handlers changed (all still return 501)
- No new Cargo.toml dependencies
- No changes to server.rs, main.rs, config.rs, xml.rs, errors.rs

## Tests Added (31 total)
- Schema: `test_schema_idempotent`
- Buckets: `test_create_and_get_bucket`, `test_bucket_exists`, `test_list_buckets`, `test_delete_bucket`, `test_update_bucket_acl`, `test_get_nonexistent_bucket`
- Objects: `test_put_and_get_object`, `test_object_exists`, `test_delete_object`, `test_delete_objects_batch`, `test_count_objects`, `test_update_object_acl`, `test_put_object_upsert`, `test_object_optional_fields`
- List objects: `test_list_objects_basic`, `test_list_objects_with_prefix`, `test_list_objects_with_delimiter`, `test_list_objects_pagination`, `test_list_objects_empty_bucket`, `test_list_objects_start_after`
- Multipart: `test_create_and_get_multipart_upload`, `test_put_and_list_parts`, `test_get_parts_for_completion`, `test_complete_multipart_upload_transactional`, `test_delete_multipart_upload`, `test_list_multipart_uploads`, `test_list_parts_pagination`
- Credentials: `test_seed_and_get_credential`, `test_seed_credential_idempotent`, `test_put_credential`, `test_get_nonexistent_credential`
- Timestamp: `test_format_timestamp`, `test_format_timestamp_recent`, `test_chrono_now_format`

## Issues Encountered
- **Rust closure uniqueness**: `MappedRows<'_, closure1>` and `MappedRows<'_, closure2>` are different types even if the closures are identical. Solved by using a named `fn` item instead of closures.
- **`cargo test` permission**: Unable to run `cargo test` in this session due to sandbox permissions. Build verified with `cargo build`. User must run `cargo test` manually to confirm all 31 tests pass.

## Build Status
- `cargo build` compiles clean (3 pre-existing warnings in cluster/storage stubs)
- `cargo test` needs to be run manually to verify
