# Stage 8: Multipart Upload - Completion

**Completed:** 2026-02-23
**Tests:** 133/133 pass (8 new), 0 memory leaks

## What Was Implemented

### CompleteMultipartUpload Handler (`src/handlers/multipart.zig`)
- Full implementation replacing the 501 NotImplemented stub
- Parses CompleteMultipartUpload XML body: extracts `<Part><PartNumber>N</PartNumber><ETag>"..."</ETag></Part>` elements
- Validates part order: part numbers must be strictly ascending (InvalidPartOrder)
- Validates ETag match: each request ETag must match the stored part ETag (InvalidPart)
- Validates part size: all non-last parts must be >= 5MiB / 5,242,880 bytes (EntityTooSmall)
- Assembles parts via storage backend's assembleParts method
- Computes composite ETag: MD5 of concatenated binary MD5 hashes, formatted as `"hex-N"` where N = part count
- Creates object metadata record atomically via SQLite transaction (completeMultipartUpload)
- Cleans up part files via storage backend's deleteParts
- Updates metrics (objects_total increment)
- Returns CompleteMultipartUploadResult XML (Location, Bucket, Key, ETag)

### assembleParts Storage Backend (`src/storage/local.zig`)
- Replaced `return error.NotImplemented` with full streaming assembly
- Builds final object path with directory creation for nested keys
- Uses atomic temp-fsync-rename pattern for crash safety
- Streams part files in order using 64KB read buffer (no full-object memory buffering)
- Computes composite ETag during assembly
- Returns AssemblePartsResult with etag and total_size

### Supporting Changes
- `src/storage/backend.zig`: Added `total_size: u64 = 0` to AssemblePartsResult
- `src/server.zig`: Updated routing to pass `req` to completeMultipartUpload

## Helper Functions Added
- `parseCompleteMultipartUploadXml`: XML body parser for Part elements
- `computeCompositeEtag`: Composite ETag computation from hex ETags
- `stripQuotes`: Quote removal helper
- `RequestPart` struct: part_number + etag
- `MIN_PART_SIZE` constant: 5 * 1024 * 1024

## New Tests (8 total)
1. `parseCompleteMultipartUploadXml: basic` -- 2 parts
2. `parseCompleteMultipartUploadXml: empty body` -- returns empty slice
3. `parseCompleteMultipartUploadXml: single part` -- 1 part
4. `computeCompositeEtag: known value` -- verifies correct hash
5. `computeCompositeEtag: single part` -- format with -1 suffix
6. `stripQuotes: with quotes` -- removes surrounding quotes
7. `stripQuotes: without quotes` -- returns unchanged
8. `LocalBackend: assembleParts basic` -- full put/assemble/verify cycle

## Key Decisions
1. Composite ETag = MD5(concat(binary_md5_of_each_part)), formatted as "hex-N"
2. 64KB read buffer for streaming part assembly (no buffered writer -- removed in Zig 0.15)
3. Direct file writes instead of std.io.bufferedWriter (removed in Zig 0.15.2)
4. GPA metadata lifecycle: copy needed fields to arena, free GPA originals

## Issues Encountered
1. `unused local constant` for owner_id/owner_display -- ObjectMeta doesn't have these fields
2. `incrementObjectsTotal` not in metrics -- use `objects_total.fetchAdd(1, .monotonic)` directly
3. `std.io.bufferedWriter` not found in Zig 0.15 -- write directly with 64KB manual buffer

## Files Changed
- `src/storage/backend.zig` -- Added total_size to AssemblePartsResult
- `src/storage/local.zig` -- Implemented assembleParts
- `src/handlers/multipart.zig` -- Implemented completeMultipartUpload + helpers
- `src/server.zig` -- Updated routing to pass req
