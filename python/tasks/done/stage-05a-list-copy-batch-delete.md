# Stage 5a: List, Copy & Batch Delete

## Completed: 2026-02-23

## What Was Implemented

### CopyObject (PUT /{bucket}/{key} with x-amz-copy-source)
- Parses `x-amz-copy-source` header: URL-decodes, strips leading slash, splits on first `/` into source bucket and key
- Validates source bucket/key exist (NoSuchBucket, NoSuchKey errors)
- Validates destination bucket exists and key is valid
- Copies data via `storage.copy_object()` (read source, atomic write to destination)
- Supports `x-amz-metadata-directive` header:
  - `COPY` (default): preserves source object metadata (content-type, user metadata, etc.)
  - `REPLACE`: uses metadata from the copy request headers
- Returns `<CopyObjectResult>` XML with ETag and LastModified

### DeleteObjects (POST /{bucket}?delete)
- Parses `<Delete>` XML body using `xml.etree.ElementTree`
- Handles XML namespace automatically (detects from root element)
- Supports quiet mode (`<Quiet>true</Quiet>` omits `<Deleted>` elements from response)
- Iterates over `<Object><Key>` elements, deletes each from both storage and metadata
- Storage deletion failures are logged but do not prevent metadata cleanup
- Returns `<DeleteResult>` XML with `<Deleted>` and `<Error>` elements

### ListObjectsV2 (GET /{bucket}?list-type=2)
- Query params: `prefix`, `delimiter`, `max-keys`, `continuation-token`, `start-after`, `encoding-type`
- Validates max-keys via `validate_max_keys()` (0-1000 range)
- Delegates to `metadata.list_objects()` for prefix/delimiter/pagination logic
- Returns `<ListBucketResult>` XML with: Name, Prefix, Delimiter, MaxKeys, KeyCount, IsTruncated, ContinuationToken, NextContinuationToken, StartAfter, Contents (Key, LastModified, ETag, Size, StorageClass), CommonPrefixes

### ListObjectsV1 (GET /{bucket})
- Query params: `prefix`, `delimiter`, `max-keys`, `marker`
- Returns `<ListBucketResult>` XML with Marker, NextMarker, and marker-based pagination
- Same metadata store query as v2, just different XML rendering

### ListObjects Dispatch
- `list_objects(request, bucket)` routes based on `list-type` query param
- `list-type=2` -> ListObjectsV2
- Otherwise -> ListObjectsV1

## Files Changed

| File | Change |
|------|--------|
| `src/bleepstore/handlers/object.py` | Implemented copy_object, delete_objects, list_objects, list_objects_v1, list_objects_v2. Added imports for urllib.parse, xml.etree, MalformedXML, validate_max_keys, and xml rendering functions. Removed duplicate delete_objects stub. |
| `src/bleepstore/xml_utils.py` | Implemented render_list_objects_v2, render_list_objects_v1, render_copy_object_result, render_delete_result (replaced NotImplementedError stubs). Added start_after param to render_list_objects_v2. |
| `src/bleepstore/server.py` | Wired ListObjects from handle_bucket_get, DeleteObjects from handle_bucket_post, updated CopyObject call to pass bucket+key. |
| `tests/test_handlers_object_advanced.py` | New file: 29 integration tests (7 copy, 6 delete, 9 list v2, 5 list v1, 2 dispatch). |
| `tests/test_server.py` | Updated 2 stub tests to reflect newly wired routes. |

## Key Decisions

1. **xml.etree over xmltodict**: Used stdlib `xml.etree.ElementTree` for parsing DeleteObjects XML body, avoiding xmltodict's single-item list normalization bug and keeping a lighter dependency footprint.

2. **Namespace-aware XML parsing**: The DeleteObjects parser detects XML namespace from the root element tag and prepends it to all element lookups, handling both namespaced and non-namespaced input.

3. **Manual XML rendering**: Consistent with existing xml_utils.py pattern -- all XML is built via string concatenation with proper escaping via `xml.sax.saxutils.escape()`.

4. **CopyObject source parsing**: URL-decode first (handles `%2F` encoded slashes), strip leading `/`, split on first `/` to get source bucket and key. Handles both `/bucket/key` and `bucket/key` formats.

5. **Metadata store reuse**: The existing `metadata.list_objects()` from Stage 2 already handles prefix, delimiter, pagination, and CommonPrefixes grouping. The handler just passes query params through and renders the XML result.

## Test Results

- 289 tests total (260 existing + 29 new)
- All passing
- Zero warnings
- Test execution time: ~0.85s

## Issues Encountered

1. **Duplicate method definition**: Initially had two `delete_objects()` methods in the ObjectHandler class -- the new implementation at line 451 and the old stub at line 702. Python uses the last definition, so the stub was silently overriding the implementation. Fixed by removing the old stub.

2. **CopyObject signature mismatch**: The server.py dispatch was calling `object_handler.copy_object(request)` (old stub signature) but the new implementation required `(request, bucket, key)`. Updated the dispatch call to pass all three arguments.
