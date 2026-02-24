# Stage 8: Multipart Upload - Completion

## Date: 2026-02-23

## What Was Implemented

### CompleteMultipartUpload Handler (`src/bleepstore/handlers/multipart.py`)

**`complete_multipart_upload()`** -- `POST /{bucket}/{key}?uploadId={id}`
- Parses XML body (`<CompleteMultipartUpload><Part><PartNumber>N</PartNumber><ETag>...</ETag></Part>...</CompleteMultipartUpload>`)
- Handles XML namespace if present
- Validates part order is strictly ascending (raises `InvalidPartOrder`)
- Validates each requested part exists in metadata store
- Validates ETags match stored parts (raises `InvalidPart`)
- Validates part sizes: all parts except last must be >= 5 MiB (raises `EntityTooSmall`)
- Calls `storage.assemble_parts()` to concatenate parts to final object atomically
- Computes composite ETag: `MD5(concat(binary_md5s))` + `-{part_count}`
- Calls `metadata.complete_multipart_upload()` (atomic: insert object, delete upload+parts)
- Cleans up part files from storage after successful completion
- Returns `CompleteMultipartUploadResult` XML (Location, Bucket, Key, ETag)

**`upload_part_copy()`** -- `PUT /{bucket}/{key}?partNumber={n}&uploadId={id}` with `x-amz-copy-source`
- Parses `x-amz-copy-source` header (URL-decoded, leading slash stripped)
- Validates source bucket and object exist
- Reads source data, optionally applying byte range from `x-amz-copy-source-range` header
- Writes data as a part using same atomic storage pattern as `upload_part()`
- Records part metadata (upsert)
- Returns `CopyPartResult` XML (ETag, LastModified)

**`_compute_composite_etag()`** -- Static helper method
- Strips quotes from each part ETag
- Concatenates binary MD5 digests via `binascii.unhexlify()`
- Computes MD5 of concatenation
- Returns quoted string: `'"' + md5hex + '-' + str(part_count) + '"'`

### XML Rendering (`src/bleepstore/xml_utils.py`)

- `render_complete_multipart_upload()`: Renders CompleteMultipartUploadResult with Location, Bucket, Key, ETag and S3 namespace (was previously raising `NotImplementedError`)

### Route Wiring (`src/bleepstore/server.py`)

- `POST /{bucket}/{key}?uploadId` now routes to `multipart_handler.complete_multipart_upload()` (previously raised `NotImplementedS3Error`)
- `PUT /{bucket}/{key}?partNumber&uploadId` with `x-amz-copy-source` header now routes to `multipart_handler.upload_part_copy()` (new dispatch)

### Tests (`tests/test_multipart_complete.py`)

~31 tests across 8 test classes:
- `TestCompleteMultipartUpload`: 5 tests -- basic response validation, XML structure, namespace, error cases
- `TestCompositeETag`: 3 tests -- single part format, multi-part computation, part count suffix
- `TestCompletedObject`: 5 tests -- GET assembled object, correct size, composite ETag on HEAD, content-type preserved, nested keys
- `TestPartCleanup`: 2 tests -- part files removed, upload metadata removed after completion
- `TestCompleteMultipartErrors`: 10 tests -- InvalidPartOrder, duplicate parts, ETag mismatch, non-existent part, EntityTooSmall, single small part OK, last part small OK, MalformedXML, empty parts, subset of parts
- `TestMultipartCompleteLifecycle`: 2 tests -- full lifecycle with disk verification, common headers
- `TestUploadPartCopy`: 4 tests -- basic copy, copy with range, source not found, cross-bucket copy

## Key Decisions

- **Composite ETag**: `'"' + md5(concat_binary_md5s).hexdigest() + '-' + str(part_count) + '"'`
- **Part size validation**: 5 MiB minimum for all parts except last (5 * 1024 * 1024 bytes)
- **Part subset support**: Completion request can reference a subset of uploaded parts; only listed parts are assembled
- **Location URL**: Set to `http://localhost/{bucket}/{key}` (placeholder; would use actual server URL in production)
- **Metadata atomicity**: Uses pre-existing `metadata.complete_multipart_upload()` which runs in a single SQLite transaction (BEGIN/INSERT/DELETE/COMMIT)
- **Part cleanup order**: First assemble parts, then commit metadata, then clean up part files. Cleanup failure is logged but does not fail the request.
- **UploadPartCopy**: Returns CopyPartResult XML (different from UploadPart which just returns ETag header)

## Files Changed

| File | Change |
|------|--------|
| `src/bleepstore/handlers/multipart.py` | Added `complete_multipart_upload()`, `upload_part_copy()`, `_compute_composite_etag()` |
| `src/bleepstore/xml_utils.py` | Implemented `render_complete_multipart_upload()` (was stub) |
| `src/bleepstore/server.py` | Wired CompleteMultipartUpload route, added UploadPartCopy dispatch |
| `tests/test_multipart_complete.py` | New: ~31 tests |
| `tests/test_server.py` | Updated 4 tests for newly wired route |

## Issues Encountered

- **EntityTooSmall in tests**: Multi-part unit tests initially failed because test data was < 5 MiB. Fixed by using `MIN_PART_SIZE` (5 MiB) data for non-last parts in tests that verify completion behavior.
- **test_server.py breakage**: ErrorXMLFormat tests used `POST /{bucket}/{key}?uploadId=abc` which previously returned 501. Route is now wired, so updated tests to use `POST /{bucket}` without query params (still returns 501 NotImplemented).
