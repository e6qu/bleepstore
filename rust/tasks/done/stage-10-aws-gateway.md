# Stage 10: AWS S3 Gateway Backend

**Date:** 2026-02-23
**Status:** COMPLETE
**Tests:** 155 unit tests pass (9 new AWS tests), E2E expected 85/86 (no regression)

## What Was Implemented

### AWS Gateway Backend (`src/storage/aws.rs`)
Complete implementation of `StorageBackend` trait using `aws-sdk-s3` crate:

1. **`put`** -- Uploads object to upstream S3 bucket, computes MD5 locally for consistent ETag
2. **`get`** -- Downloads object from upstream S3, returns `StoredObject` with SHA-256 content hash
3. **`delete`** -- Deletes object from upstream S3 (idempotent, matching S3 behavior)
4. **`exists`** -- HEAD request to check object existence, maps 404 to `false`
5. **`copy_object`** -- Server-side copy within the upstream S3 bucket
6. **`put_part`** -- Stores multipart parts as temporary S3 objects at `{prefix}.parts/{upload_id}/{part_number}`
7. **`assemble_parts`** -- Assembles parts using AWS native multipart upload:
   - Single part: direct server-side copy
   - Multiple parts: `create_multipart_upload` + `upload_part_copy` for each part + `complete_multipart_upload`
   - Fallback: if `upload_part_copy` fails with EntityTooSmall, downloads part data and re-uploads
   - Computes composite ETag locally for consistency with BleepStore's metadata
   - Aborts the AWS upload on any failure
8. **`delete_parts`** -- Lists and batch-deletes temporary part objects using `list_objects_v2` + `delete_objects`
9. **`create_bucket`** -- No-op (BleepStore buckets are namespaced as prefixes in the single upstream bucket)
10. **`delete_bucket`** -- No-op (metadata handles bucket lifecycle)

### Key Mapping
- Objects: `{prefix}{bleepstore_bucket}/{key}` in the single upstream S3 bucket
- Parts: `{prefix}.parts/{upload_id}/{part_number}` as temporary S3 objects

### Config Integration (`src/config.rs`)
Already had `AwsStorageConfig` struct with `bucket`, `region`, `prefix` fields from the scaffold.

### Backend Factory (`src/main.rs`)
Updated storage backend initialization to dispatch based on `config.storage.backend`:
- `"aws"` -- Creates `AwsGatewayBackend` with config from `storage.aws` section
- `"local"` or default -- Creates `LocalBackend` (unchanged behavior)

### Dependencies (`Cargo.toml`)
Added `aws-config` and `aws-sdk-s3` with all transitive deps pinned to versions compatible with rustc 1.88:
- `aws-config = "=1.8.13"` (MSRV 1.88, published 2026-02-03)
- `aws-sdk-s3 = "=1.122.0"` (MSRV 1.88, published 2026-02-03)
- 18 transitive deps pinned to pre-MSRV-1.91-bump versions

### Unit Tests (9 new)
- Key mapping: with prefix, without prefix, nested keys, special chars
- Part key mapping
- MD5 computation: empty data, known data
- Composite ETag: single part, multiple parts

## Key Decisions

1. **MD5 computed locally** -- AWS may return different ETags with SSE. Local computation ensures consistency with BleepStore's metadata.
2. **Composite ETag computed locally** -- For multipart assembly, we compute `md5(concat(binary_md5_of_each_part))-N` ourselves rather than relying on AWS's response, ensuring consistency with the metadata store.
3. **Server-side copy for assembly** -- Uses `upload_part_copy` to avoid downloading/uploading data. Falls back to download+upload for parts < 5MB (EntityTooSmall).
4. **Abort on failure** -- If multipart assembly fails at any step, the AWS upload is aborted to prevent orphaned incomplete uploads.
5. **No-op bucket operations** -- Since all data lives in one upstream S3 bucket with prefix namespacing, create/delete bucket are no-ops (metadata handles lifecycle).
6. **Force path style** -- S3 client uses path-style URLs for compatibility with S3-compatible services.
7. **Version pinning** -- AWS SDK MSRV bumped to 1.91 on 2026-02-11. All deps pinned to the last versions supporting rustc 1.88 (released 2026-02-03 or earlier).

## Files Changed
- `src/storage/aws.rs` -- Full implementation (replacing todo!() stubs)
- `src/main.rs` -- Backend factory dispatch based on config
- `Cargo.toml` -- Added aws-config, aws-sdk-s3, and 18 transitive dep pins

## Issues Encountered
- **Rust 1.88 vs AWS SDK MSRV 1.91** -- Latest AWS SDK crates require rustc 1.91 (MSRV bumped 2026-02-11). Resolved by pinning all 20 AWS SDK crates to the last compatible versions (published 2026-02-03 or earlier).
