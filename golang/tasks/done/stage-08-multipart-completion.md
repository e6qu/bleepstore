# Stage 8: Multipart Upload - Completion

## Date: 2026-02-23

## What was implemented

### CompleteMultipartUpload handler (`internal/handlers/multipart.go`)
- Full implementation replacing the 501 NotImplemented stub
- Validates bucket, key, and uploadId query parameter
- Verifies upload exists in metadata store (404 NoSuchUpload if not)
- Parses `<CompleteMultipartUpload>` XML request body
- Validates part order: ascending by PartNumber, no duplicates (400 InvalidPartOrder)
- Fetches stored part records from metadata via `GetPartsForCompletion`
- Validates each requested part exists (400 InvalidPart if missing)
- Validates ETag match between request and stored parts (400 InvalidPart on mismatch)
- Validates part sizes: all non-last parts must be >= 5 MiB (400 EntityTooSmall)
- Calls `storage.AssembleParts` for atomic part concatenation into final object
- Computes total object size from stored part sizes
- Builds final ObjectRecord from upload metadata (content type, headers, ACL, user metadata)
- Calls `metadata.CompleteMultipartUpload` (transactional: insert object, delete parts, delete upload)
- Returns `CompleteMultipartUploadResult` XML with Location, Bucket, Key, ETag

### Helper functions (`internal/handlers/helpers.go`)
- `CompletePart` struct with PartNumber and ETag XML tags
- `CompleteMultipartUploadRequest` struct for XML parsing
- `parseCompleteMultipartXML(body io.Reader)` - parses XML body into []CompletePart
- `computeCompositeETag(partETags []string)` - computes S3-style composite ETag:
  - Strips quotes from each part ETag
  - Decodes hex to raw MD5 bytes
  - Concatenates raw bytes
  - MD5 of concatenation
  - Format: `"hexdigest-N"` where N = part count

### Server test update (`internal/server/server_test.go`)
- Updated `TestS3StubRoutes`: CompleteMultipartUpload now returns 500 InternalError (nil deps) instead of 501 NotImplemented

## Files changed
- `internal/handlers/multipart.go` - CompleteMultipartUpload implementation
- `internal/handlers/helpers.go` - New helper types and functions
- `internal/handlers/multipart_test.go` - 14 new test functions
- `internal/server/server_test.go` - Updated route test expectation

## Tests added (14 new, 165 total)
- `TestCompleteMultipartUpload` - Full 3-part completion with content verification
- `TestCompleteMultipartUploadInvalidPartOrder` - Descending order rejected
- `TestCompleteMultipartUploadDuplicatePartNumber` - Duplicate rejected
- `TestCompleteMultipartUploadWrongETag` - ETag mismatch rejected
- `TestCompleteMultipartUploadMissingPart` - Non-existent part rejected
- `TestCompleteMultipartUploadNoSuchUpload` - Non-existent upload returns 404
- `TestCompleteMultipartUploadEmptyBody` - Empty body returns 400 MalformedXML
- `TestCompleteMultipartUploadEntityTooSmall` - Small non-last part rejected
- `TestCompleteMultipartUploadSinglePart` - Single small part succeeds
- `TestCompleteMultipartUploadCompositeETag` - Verifies ETag computation
- `TestCompleteMultipartUploadXMLStructure` - Verifies XML format
- `TestParseCompleteMultipartXML` - Valid XML parsing
- `TestParseCompleteMultipartXMLInvalid` - Invalid XML error
- `TestCompleteMultipartUploadFullLifecycle` - End-to-end with GetObject verification

## Key decisions
- ETag comparison normalizes by stripping quotes for flexibility
- Part size minimum 5 MiB only enforced on non-last parts (matching AWS S3)
- Storage.AssembleParts computes the composite ETag (already implemented in Stage 4)
- computeCompositeETag helper added separately for unit testability
- Location format: `/{bucket}/{key}` (relative path)
- Upload metadata propagated to final object: content-type, ACL, user-metadata, content headers
- No new external dependencies

## Issues encountered
- Unit tests initially failed because multi-part tests used small (< 5 MiB) parts for non-last positions
- Fixed by using 5 MiB+ parts in multi-part completion tests, keeping single-part tests with small data
- Server stub route test needed updating since CompleteMultipartUpload is no longer 501
