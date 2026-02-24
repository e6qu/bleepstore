# Stage 5b: Range, Conditional Requests & Object ACLs

## Date: 2026-02-23

## Summary
Implemented range requests, conditional request headers (If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since), and object ACL operations (GetObjectAcl, PutObjectAcl).

## Files Modified

### `src/handlers/object.rs`
- **Range request parsing**: Added `ByteRange` enum, `parse_range_header()`, and `resolve_range()` functions to parse and resolve `Range: bytes=start-end`, `bytes=start-`, and `bytes=-N` headers.
- **Conditional request evaluation**: Added `evaluate_conditions()` function implementing the full HTTP/S3 conditional request priority:
  1. If-Match (412 on mismatch)
  2. If-Unmodified-Since (412 if modified after date) -- only if If-Match absent
  3. If-None-Match (304 for GET/HEAD on match)
  4. If-Modified-Since (304 if not modified since) -- only if If-None-Match absent
- **Updated `get_object`**: Now evaluates conditional headers before reading storage. Parses Range header and returns 206 Partial Content with `Content-Range` header for valid ranges, 416 for unsatisfiable ranges.
- **Updated `head_object`**: Now evaluates conditional headers (same logic as get_object, minus range).
- **Implemented `get_object_acl`**: Looks up object metadata, parses ACL JSON, renders `<AccessControlPolicy>` XML using existing `render_access_control_policy()`.
- **Implemented `put_object_acl`**: Accepts `x-amz-acl` canned ACL header, updates object ACL via `metadata.update_object_acl()`.
- **Helper functions**: `strip_etag_quotes()`, `parse_iso8601_to_system_time()` for conditional request evaluation.
- **Unit tests**: 19 new tests covering range parsing, range resolution, ETag comparison, conditional request evaluation (If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since), and ISO-8601 parsing.

### `src/errors.rs`
- Added `NotModified` variant for 304 responses.
- Updated `code()`, `status_code()`, and `IntoResponse` impl.
- 304 responses return empty body (no XML error payload).

### `src/server.rs`
- Updated `handle_get_object` to pass `state, &bucket, &key` to `get_object_acl`.
- Updated `handle_put_object` to pass `state, &bucket, &key, &headers, &body` to `put_object_acl`.

## Key Design Decisions

1. **Range parsing**: Only single ranges supported (no multi-range). Malformed Range headers are ignored (full body returned per HTTP spec). Invalid/unsatisfiable ranges return 416.
2. **Conditional request priority**: Follows RFC 7232 priority order. If-Match takes precedence over If-Unmodified-Since; If-None-Match takes precedence over If-Modified-Since.
3. **ETag comparison**: Strips surrounding quotes before comparing. Supports wildcard `*` for If-Match.
4. **304 Not Modified**: Returns empty body with no XML error payload (per HTTP spec).
5. **Object ACLs**: Use the same `render_access_control_policy()` XML renderer as bucket ACLs (from Stage 3).

## Test Targets
- `TestGetObjectRange::test_range_request` -- bytes=0-4 returns 206
- `TestGetObjectRange::test_range_request_suffix` -- bytes=-5
- `TestGetObjectRange::test_invalid_range` -- 416
- `TestConditionalRequests::test_if_match_success` -- 200
- `TestConditionalRequests::test_if_match_failure` -- 412
- `TestConditionalRequests::test_if_none_match_returns_304` -- 304
- `TestConditionalRequests::test_if_modified_since_returns_304` -- 304
- `TestConditionalRequests::test_if_unmodified_since_precondition_failed` -- 412
- `TestObjectAcl::test_get_object_acl_default` -- Default FULL_CONTROL
- `TestObjectAcl::test_put_object_acl_canned` -- Set public-read
- `TestObjectAcl::test_put_object_with_canned_acl` -- ACL on PUT
- `TestObjectAcl::test_get_acl_nonexistent_object` -- NoSuchKey
