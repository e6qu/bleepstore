# Stage 7: Multipart Upload - Core

**Date:** 2026-02-23
**Status:** Complete

## What Was Implemented

5 of 6 multipart upload handlers (all except CompleteMultipartUpload which is Stage 8):

### 1. CreateMultipartUpload
- Checks bucket exists (NoSuchBucket error if not)
- Generates UUID v4 upload ID: 16 random bytes, version bits (0100), variant bits (10xx)
- Extracts Content-Type from request header (defaults to application/octet-stream)
- Creates metadata record via metadata store's createMultipartUpload
- Returns InitiateMultipartUploadResult XML with Bucket, Key, UploadId

### 2. UploadPart
- Extracts uploadId and partNumber from query string
- Validates part number (1-10000 via validation.zig)
- Verifies upload exists via getMultipartUpload (NoSuchUpload if not)
- Reads request body via req.body()
- Writes part via storage backend's putPart (atomic temp-fsync-rename)
- Computes MD5 ETag
- Upserts part metadata via putPartMeta
- Returns 200 with ETag header
- Properly frees GPA-allocated upload metadata after copying to arena

### 3. AbortMultipartUpload
- Extracts uploadId from query string
- Deletes part files from storage via deleteParts (idempotent)
- Deletes metadata records via abortMultipartUpload (NoSuchUpload if not found)
- Returns 204

### 4. ListMultipartUploads
- Checks bucket exists
- Parses query params: prefix, delimiter, key-marker, upload-id-marker, max-uploads (capped at 1000)
- Queries metadata store via listMultipartUploads
- Builds MultipartUploadEntry array for XML renderer
- Renders ListMultipartUploadsResult XML with full S3 fields

### 5. ListParts
- Extracts uploadId from query string
- Verifies upload exists via getMultipartUpload
- Copies owner/storage_class fields to arena before freeing GPA allocations
- Parses max-parts and part-number-marker from query
- Queries part metadata via listPartsMeta
- Renders ListPartsResult XML with full S3 fields

### 6. CompleteMultipartUpload (Stage 8)
- Remains as 501 NotImplemented stub

## Storage Backend Changes (local.zig)

- **init**: Creates .multipart/ directory on startup (alongside .tmp/)
- **putPart**: Builds path `.multipart/{upload_id}/{part_number}`, creates upload directory, atomic write (temp-fsync-rename), returns MD5 ETag
- **deleteParts**: Uses `deleteTree` on `.multipart/{upload_id}/` directory, fully idempotent
- **assembleParts**: Changed from `@panic` to `return error.NotImplemented` (Stage 8)

## XML Rendering (xml.zig)

- **MultipartUploadEntry struct**: key, upload_id, owner_id, owner_display, storage_class, initiated
- **renderListMultipartUploadsResult**: Full S3-compliant XML with Upload, Initiator, Owner, StorageClass, CommonPrefixes
- **PartEntry struct**: part_number, last_modified, etag, size
- **renderListPartsResult**: Full S3-compliant XML with Part elements including PartNumber, LastModified, ETag, Size

## Routing Updates (server.zig)

- listMultipartUploads: added `query` parameter
- createMultipartUpload: added `req` parameter
- uploadPart: added `req` parameter

## Key Decisions

1. UUID v4: 16 random bytes with version/variant bit manipulation, formatted as standard UUID string
2. Part storage: `.multipart/{upload_id}/{part_number}` -- one directory per upload
3. GPA metadata lifecycle: copy needed fields to arena, free GPA originals (prevents arena from freeing GPA memory)
4. deleteParts: `catch {}` on deleteTree for idempotency
5. XML test allocator: ArenaAllocator wrapping testing.allocator to handle allocPrint intermediates

## Issues Encountered

1. **deleteTree error set**: `error.FileNotFound` not in deleteTree's error set in Zig 0.15.2. Fixed with `catch {}`.
2. **XML test memory leaks**: allocPrint intermediates leaked with bare testing.allocator. Fixed with ArenaAllocator wrapper.

## Files Changed

- `src/handlers/multipart.zig` -- Complete rewrite (5 handlers + 3 helpers + 2 tests)
- `src/storage/local.zig` -- putPart, deleteParts, .multipart/ init + 3 tests
- `src/xml.zig` -- 2 new structs, 2 new render functions + 3 tests
- `src/server.zig` -- 3 routing parameter updates

## Test Results

- **126/126 tests pass**, 0 memory leaks
- 8 new tests added (3 storage, 3 XML, 2 multipart handler)
