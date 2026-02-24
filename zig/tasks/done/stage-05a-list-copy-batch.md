# Stage 5a: List, Copy & Batch Delete

**Date:** 2026-02-23
**Status:** Complete
**Tests:** 87/87 pass, 0 memory leaks

## What Was Implemented

### ListObjectsV2 (`GET /<bucket>?list-type=2`)
- Parses query parameters: prefix, delimiter, start-after, continuation-token, max-keys
- Queries SQLite metadata store via listObjectsMeta (already supports all parameters)
- ContinuationToken takes priority over StartAfter for pagination
- KeyCount includes both Contents entries and CommonPrefixes count
- max-keys capped at 1000 per S3 spec
- Full XML response: Name, Prefix, KeyCount, MaxKeys, Delimiter, IsTruncated, ContinuationToken, NextContinuationToken, StartAfter, Contents (Key, LastModified, ETag, Size, StorageClass, Owner), CommonPrefixes

### ListObjectsV1 (`GET /<bucket>`)
- Parses query parameters: prefix, delimiter, marker, max-keys
- Uses Marker for pagination (equivalent to start_after)
- No KeyCount field (V1 difference)
- Full XML response: Name, Prefix, Marker, NextMarker, MaxKeys, Delimiter, IsTruncated, Contents, CommonPrefixes

### CopyObject (`PUT /<bucket>/<key>` with x-amz-copy-source)
- Parses x-amz-copy-source header: URL-decoded, leading slash stripped, split into bucket/key
- Checks source and destination buckets exist
- Checks source object exists (NoSuchKey if not)
- Copies file via storage backend copyObject
- Re-reads destination to compute MD5 ETag
- Supports metadata directives:
  - COPY (default): copies all source metadata (content_type, user_metadata, acl, storage_class)
  - REPLACE: uses request headers for metadata (content-type, x-amz-meta-* headers)
- Returns CopyObjectResult XML (ETag, LastModified)

### DeleteObjects (`POST /<bucket>?delete`)
- Parses Delete XML body to extract Key elements and Quiet flag
- Deletes each key from storage and metadata
- Always reports success for each key (even if key didn't exist, matching S3 behavior)
- Quiet mode: returns empty Deleted list (only Errors, if any)
- Verbose mode: returns all deleted keys in Deleted list
- Updates objects_total metric for each deleted object

## Files Changed

| File | Changes |
|------|---------|
| `src/handlers/object.zig` | Implemented listObjectsV2, listObjectsV1, copyObject, deleteObjects. Added helper functions: parseQuietFlag, extractXmlElements, uriDecode, hexCharToNibble. Added 7 unit tests. |
| `src/xml.zig` | Rewrote renderListObjectsV2Result with full S3 fields. New renderListObjectsV1Result (separate from V2). New ListObjectEntry struct. New renderCopyObjectResult. |
| `src/server.zig` | Updated PUT object dispatch to detect x-amz-copy-source header for copy. Pass query parameter to listObjectsV2/V1. Pass req to deleteObjects. |

## Key Decisions

1. CopyObject dispatched by checking x-amz-copy-source header on PUT before putObject
2. DeleteObjects uses simple XML tag extraction (no full parser needed)
3. CopyObject re-reads destination after copy to compute correct MD5 ETag
4. URI decoding for copy source handles percent-encoded characters
5. ListObjectsV2 KeyCount = Contents.len + CommonPrefixes.len
6. max-keys capped at 1000 regardless of client request
7. Quiet mode implemented by passing empty deleted_keys to renderDeleteResult

## No Changes Needed

- `src/metadata/sqlite.zig` -- listObjectsMeta already supports all parameters (prefix, delimiter, start_after, max_keys, pagination)
- `src/storage/local.zig` -- copyObject already implemented
- `src/metadata/store.zig` -- MetadataStore interface unchanged
