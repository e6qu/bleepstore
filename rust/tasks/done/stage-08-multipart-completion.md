# Stage 8: Multipart Upload - Completion

## Date: 2026-02-23

## Summary
Implemented `CompleteMultipartUpload` handler and `assemble_parts` storage backend method, completing all 6 multipart upload S3 operations.

## What Was Implemented

### 1. `complete_multipart_upload` handler (src/handlers/multipart.rs)
- Full `CompleteMultipartUpload` implementation with:
  - XML body parsing via `parse_complete_multipart_upload_xml()` (SAX-style quick_xml)
  - Part validation: ascending order, ETag match (quote-insensitive), 5 MiB minimum for non-last parts
  - Part assembly via `storage.assemble_parts()`
  - Object record construction from upload metadata
  - Transactional metadata completion (insert object, delete upload + parts)
  - Part file cleanup (best-effort)
  - `<CompleteMultipartUploadResult>` XML response

### 2. `assemble_parts` storage backend (src/storage/local.rs)
- Reads part files in order from `.multipart/{upload_id}/{part_number}`
- Concatenates to temp file using crash-only temp-fsync-rename pattern
- Computes composite ETag: MD5 of concatenated binary part MD5s + "-{part_count}"
- Creates parent directories for final object path

### 3. Server routing update (src/server.rs)
- `POST /{bucket}/{key}?uploadId` now passes all required params to handler

## Files Changed
- `src/handlers/multipart.rs` -- Added complete_multipart_upload handler + XML parser + 7 tests
- `src/storage/local.rs` -- Implemented assemble_parts + 4 tests
- `src/server.rs` -- Updated handle_post_object dispatch

## Key Design Decisions
- Composite ETag computed by storage backend during assembly (avoids double-reading parts)
- ETag comparison strips quotes from both sides before comparing
- Part cleanup from storage is best-effort (non-blocking after metadata commit)
- Location URL uses relative path format: `/{bucket}/{key}`

## Unit Tests Added (11 new, 146 total)
- XML parsing: valid multi-part, single part, empty body, malformed, missing ETag, missing PartNumber, unquoted ETag (7 tests)
- Storage assembly: basic roundtrip, single part, nested keys, composite ETag format (4 tests)

## Error Handling
- `MalformedXML` -- Invalid or empty XML body
- `InvalidPartOrder` -- Parts not in ascending order
- `InvalidPart` -- Part not found or ETag mismatch
- `EntityTooSmall` -- Non-last part smaller than 5 MiB
- `NoSuchUpload` -- Upload ID not found or bucket/key mismatch

## Issues Encountered
- `quick_xml` 0.31 uses `reader.trim_text(true)` (not `reader.config_mut().trim_text(true)` which is the 0.36+ API)
