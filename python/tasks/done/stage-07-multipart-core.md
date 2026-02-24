# Stage 7: Multipart Upload - Core

## Date: 2026-02-23

## What Was Implemented

### 5 Multipart Upload Handlers (`src/bleepstore/handlers/multipart.py`)

1. **CreateMultipartUpload** (`POST /{bucket}/{key}?uploads`)
   - Generates UUID upload_id via `str(uuid.uuid4())`
   - Extracts Content-Type, Content-Encoding, Content-Language, Content-Disposition, Cache-Control, Expires headers for final object
   - Extracts `x-amz-meta-*` user metadata
   - Derives owner info from request.state (auth middleware) or config fallback
   - Stores upload metadata via `metadata.create_multipart_upload()`
   - Returns InitiateMultipartUploadResult XML (Bucket, Key, UploadId)

2. **UploadPart** (`PUT /{bucket}/{key}?partNumber={n}&uploadId={id}`)
   - Validates part number is 1-10000
   - Verifies bucket and upload exist
   - Reads body, writes to `.parts/{upload_id}/{part_number}` via `storage.put_part()` (atomic)
   - Computes MD5 ETag
   - Records part metadata via `metadata.put_part()` (upsert)
   - Returns 200 with quoted ETag header

3. **AbortMultipartUpload** (`DELETE /{bucket}/{key}?uploadId={id}`)
   - Verifies bucket and upload exist
   - Deletes part files via `storage.delete_parts()`
   - Deletes upload and part metadata via `metadata.abort_multipart_upload()`
   - Returns 204 No Content

4. **ListMultipartUploads** (`GET /{bucket}?uploads`)
   - Supports prefix, delimiter, key-marker, upload-id-marker, max-uploads query params
   - Queries `metadata.list_multipart_uploads()`
   - Returns ListMultipartUploadsResult XML with Upload elements and CommonPrefixes

5. **ListParts** (`GET /{bucket}/{key}?uploadId={id}`)
   - Supports part-number-marker, max-parts query params
   - Verifies upload exists
   - Queries `metadata.list_parts()`
   - Returns ListPartsResult XML with Part elements

### 3 XML Rendering Functions (`src/bleepstore/xml_utils.py`)

- `render_initiate_multipart_upload()`: InitiateMultipartUploadResult with S3 namespace
- `render_list_multipart_uploads()`: ListMultipartUploadsResult with Upload/CommonPrefixes elements, pagination markers
- `render_list_parts()`: ListPartsResult with Part elements, Initiator/Owner, pagination

### Route Wiring (`src/bleepstore/server.py`)

- `MultipartHandler` instantiated alongside `BucketHandler` and `ObjectHandler`
- 5 routes wired to multipart handler (previously raised NotImplementedS3Error):
  - `GET /{bucket}?uploads` -> list_uploads
  - `POST /{bucket}/{key}?uploads` -> create_multipart_upload
  - `PUT /{bucket}/{key}?partNumber&uploadId` -> upload_part
  - `GET /{bucket}/{key}?uploadId` -> list_parts
  - `DELETE /{bucket}/{key}?uploadId` -> abort_multipart_upload
- `POST /{bucket}/{key}?uploadId` still raises NotImplementedS3Error (Stage 8)

### Tests (`tests/test_multipart.py`)

- ~48 tests across 6 test classes
- Per-test fresh metadata store and storage backend to avoid state leaks
- Full lifecycle test: create -> upload 3 parts -> list parts -> verify ETags -> list uploads -> abort -> verify cleanup
- Error cases: NoSuchBucket, NoSuchUpload, invalid part numbers

## Key Decisions

- Upload ID: `str(uuid.uuid4())` for crash-safe uniqueness
- Part storage: `{root}/.parts/{upload_id}/{part_number}` (already scaffolded in storage backend)
- Part overwrite: same upload_id + part_number does upsert in both storage and metadata
- Owner info: prefers auth middleware request.state, falls back to config-derived owner
- All metadata and storage methods were pre-implemented in Stage 2 and Stage 4 respectively

## Files Changed

| File | Change |
|------|--------|
| `src/bleepstore/handlers/multipart.py` | Full rewrite: 5 handlers implemented |
| `src/bleepstore/xml_utils.py` | 3 rendering functions implemented (were stubs) |
| `src/bleepstore/server.py` | Imported MultipartHandler, wired 5 routes |
| `tests/test_multipart.py` | New: ~48 tests |
| `tests/test_server.py` | Updated 7 tests for newly wired routes |

## Issues Encountered

- None. All metadata and storage methods were already well-tested from Stage 2 and Stage 4.
