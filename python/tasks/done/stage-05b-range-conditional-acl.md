# Stage 5b: Range, Conditional Requests & Object ACLs

**Completed:** 2026-02-23
**Test count:** 334 total (289 existing + 45 new)

## What Was Implemented

### 1. Range Requests (GET /{bucket}/{key} with Range header)

**File:** `src/bleepstore/handlers/object.py`

Added `parse_range_header(header, total)` function that supports three Range forms:
- `bytes=start-end` -- closed range (e.g., `bytes=0-4` returns first 5 bytes)
- `bytes=-suffix` -- suffix range (e.g., `bytes=-5` returns last 5 bytes)
- `bytes=start-` -- open-ended range (e.g., `bytes=10-` returns from byte 10 to end)

Updated `get_object()` to:
- Parse the Range header after conditional evaluation
- Return 206 Partial Content with `Content-Range: bytes start-end/total` header
- Use `storage.get_stream(bucket, key, offset=start, length=content_length)` for efficient partial reads
- Return 416 Range Not Satisfiable via `InvalidRange` error for unsatisfiable ranges
- Clamp end to total-1 when range extends past file size

### 2. Conditional Requests

**File:** `src/bleepstore/handlers/object.py`

Added `evaluate_conditionals(request, etag, last_modified_str, is_get_or_head)` that follows HTTP/1.1 evaluation order:

1. **If-Match** -> 412 Precondition Failed if ETag does not match (supports `*` wildcard and comma-separated list)
2. **If-Unmodified-Since** -> 412 if object modified after the given date (only evaluated if If-Match is absent)
3. **If-None-Match** -> 304 Not Modified for GET/HEAD if ETag matches, 412 for other methods (supports `*` and comma-separated list)
4. **If-Modified-Since** -> 304 if not modified since date (GET/HEAD only, only if If-None-Match is absent)

Helper functions:
- `_strip_etag_quotes()` -- strips surrounding `"` and optional `W/` prefix
- `_parse_http_date()` -- parses HTTP dates via `email.utils.parsedate_to_datetime()`
- `_parse_last_modified()` -- parses ISO 8601 timestamps from metadata store

Updated `get_object()` and `head_object()` to evaluate conditionals before returning data.
304 responses include only ETag and Last-Modified headers.

### 3. Object ACLs

**Files:** `src/bleepstore/handlers/object.py`, `src/bleepstore/server.py`

Implemented two new handlers:

- **`get_object_acl(request, bucket, key)`**: Validates bucket+key exist, loads ACL from object metadata (`acl` column), fills in default owner info if ACL has no owner, renders AccessControlPolicy XML using existing `render_acl_xml()`.

- **`put_object_acl(request, bucket, key)`**: Validates bucket+key exist, supports three modes:
  1. Canned ACL via `x-amz-acl` header (private, public-read, public-read-write, authenticated-read)
  2. XML body with full AccessControlPolicy
  3. Default to private if neither provided

Wired routes in `server.py`:
- `GET /{bucket}/{key}?acl` -> `object_handler.get_object_acl()`
- `PUT /{bucket}/{key}?acl` -> `object_handler.put_object_acl()`

Reuses all ACL infrastructure from Stage 3: `build_default_acl()`, `parse_canned_acl()`, `acl_to_json()`, `acl_from_json()`, `render_acl_xml()`.

## Files Changed

| File | Changes |
|------|---------|
| `src/bleepstore/handlers/object.py` | Added range parsing, conditional evaluation, object ACL handlers; updated get_object/head_object |
| `src/bleepstore/server.py` | Wired GET/PUT /{bucket}/{key}?acl routes |
| `tests/test_range.py` | **New.** 20 unit tests for range parser |
| `tests/test_handlers_object_advanced.py` | Added 25 integration tests (range, conditional, ACL) |
| `tests/test_server.py` | Updated 5 tests for newly wired ACL routes |
| `STATUS.md` | Updated to Stage 5b Complete |
| `PLAN.md` | Marked Stage 5b with checkmark |
| `WHAT_WE_DID.md` | Added Session 9 entry |
| `DO_NEXT.md` | Updated to Stage 6 |

## Key Decisions

1. **Range parsing regex**: Uses compiled `^bytes=(\d*)-(\d*)$` regex, handles empty groups for suffix/open-ended forms
2. **Conditional evaluation order**: Strictly follows HTTP/1.1 spec (RFC 7232 Section 6)
3. **304 response headers**: Only includes ETag and Last-Modified (not full object headers)
4. **Object ACL owner derivation**: Uses SHA-256 of access key (same pattern as bucket handler)
5. **ACL XML parsing**: Includes `_parse_acl_xml()` helper duplicated from bucket handler (not refactored to shared module to keep changes minimal)

## Issues Encountered

- **test_server.py test_error_xml_structure**: Had to update 5 tests that used `?acl` route for error testing since it's no longer 501. Changed to use `?uploadId=abc` route which is still unimplemented.
