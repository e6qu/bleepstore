# Stage 5a: List, Copy & Batch Delete

**Date:** 2026-02-23
**Status:** Complete

## What Was Implemented

### 1. CopyObject Handler (`src/handlers/object.rs`)
- Detects copy via `x-amz-copy-source` header on `PUT /{bucket}/{key}`
- URL-decodes source path using `percent-encoding` crate (handles `%20`, `%2F`, etc.)
- Strips leading `/` from source, splits into source bucket/key
- Validates source bucket exists, source object exists, destination bucket exists
- Supports `x-amz-metadata-directive` header:
  - `COPY` (default): copies all metadata from source object
  - `REPLACE`: reads metadata from request headers (Content-Type, user metadata, ACL, etc.)
- Copies file data via `StorageBackend::copy_object()`
- Records destination object metadata in metadata store
- Returns `<CopyObjectResult>` XML with `<ETag>` and `<LastModified>`

### 2. DeleteObjects Handler (`src/handlers/object.rs`)
- Parses `<Delete>` XML body using SAX-style `quick_xml::Reader`
- Extracts `<Quiet>` flag and list of `<Object><Key>` elements via `parse_delete_xml()` helper
- Deletes each object from storage (best-effort) and metadata (idempotent)
- Returns `<DeleteResult>` XML:
  - `<Deleted><Key>...</Key></Deleted>` for each successful delete (suppressed in quiet mode)
  - `<Error><Key>...<Code>...<Message>...</Error>` for failures
- Returns MalformedXML error if no keys are found in the body

### 3. ListObjectsV2 Handler (`src/handlers/object.rs`)
- Extracts query parameters: `prefix`, `delimiter`, `max-keys` (default 1000), `start-after`, `continuation-token`
- Queries metadata store via `list_objects()` (implemented in Stage 2)
- Renders `<ListBucketResult>` XML with:
  - `Name`, `Prefix`, `Delimiter` (only when non-empty), `MaxKeys`, `KeyCount`
  - `IsTruncated`, `ContinuationToken`, `NextContinuationToken`, `StartAfter`
  - `<Contents>` entries with `Key`, `LastModified`, `ETag`, `Size`, `StorageClass`
  - `<CommonPrefixes>` entries with `<Prefix>`

### 4. ListObjectsV1 Handler (`src/handlers/object.rs`)
- Extracts query parameters: `prefix`, `delimiter`, `max-keys` (default 1000), `marker`
- Uses marker as start_after in metadata query (same underlying `list_objects()`)
- Renders `<ListBucketResult>` XML with:
  - `Name`, `Prefix`, `Marker`, `Delimiter` (only when non-empty), `MaxKeys`
  - `IsTruncated`, `NextMarker` (when truncated)
  - `<Contents>` and `<CommonPrefixes>` entries

### 5. LocalBackend copy_object (`src/storage/local.rs`)
- Reads source file data from disk
- Computes MD5 hash for ETag
- Writes to destination via crash-only temp-fsync-rename pattern
- Creates parent directories for destination key if needed
- Returns error if source file doesn't exist

### 6. XML Rendering Functions (`src/xml.rs`)
- `render_list_objects_result_v1()`: New function for V1 list with `Marker`/`NextMarker`
- `render_delete_result()`: New function with `DeletedEntry` and `DeleteErrorEntry` structs
- Updated `render_list_objects_result()` (V2): Added `KeyCount`, `StartAfter`, conditional `Delimiter`

### 7. Server Dispatch Updates (`src/server.rs`)
- `handle_get_bucket`: Routes `list-type=2` to `list_objects_v2`, default to `list_objects_v1`
- `handle_post_bucket`: Accepts body, routes `?delete` to `delete_objects`
- `handle_put_object`: Routes `x-amz-copy-source` to `copy_object`

## Files Changed
| File | Change |
|------|--------|
| `Cargo.toml` | Added `percent-encoding = "2"` |
| `src/handlers/object.rs` | Implemented `copy_object`, `delete_objects`, `list_objects_v2`, `list_objects_v1`, added `parse_delete_xml` helper |
| `src/storage/local.rs` | Implemented `copy_object`, added 3 unit tests |
| `src/xml.rs` | Added `render_list_objects_result_v1`, `render_delete_result`, `DeletedEntry`, `DeleteErrorEntry`; updated `render_list_objects_result` |
| `src/server.rs` | Updated `handle_get_bucket`, `handle_post_bucket`, `handle_put_object` dispatch |

## New Dependencies
- `percent-encoding = "2"` -- URL-decoding for `x-amz-copy-source` header

## Key Design Decisions
1. **SAX-style XML parsing** for `<Delete>` body (not DOM) -- efficient for potentially large batch deletes
2. **`percent-encoding` crate** for URL-decoding copy source -- handles all percent-encoded characters correctly
3. **Delimiter element omitted** from XML when empty -- matches S3 behavior
4. **KeyCount in V2** = objects count + common prefixes count (per S3 spec)
5. **NextMarker in V1** included whenever result is truncated (convenience for clients)
6. **Quiet mode** in DeleteObjects suppresses `<Deleted>` elements but always shows `<Error>` elements
7. **Copy object storage** uses same crash-only temp-fsync-rename pattern as PutObject

## Unit Tests Added
- `test_copy_object_same_bucket` -- Copy within same bucket, verify ETags match
- `test_copy_object_different_buckets` -- Copy across buckets
- `test_copy_object_nonexistent_source` -- Verify error on missing source

## E2E Test Targets
- `TestDeleteObjects::test_delete_multiple_objects`
- `TestDeleteObjects::test_delete_objects_quiet_mode`
- `TestCopyObject::test_copy_object`
- `TestCopyObject::test_copy_object_with_replace_metadata`
- `TestCopyObject::test_copy_nonexistent_source`
- `TestListObjectsV2::test_list_objects` (and 6 more V2 tests)
- `TestListObjectsV1::test_list_objects_v1`
- `TestListObjectsV1::test_list_objects_v1_with_marker`
