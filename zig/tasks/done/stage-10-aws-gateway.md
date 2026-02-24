# Stage 10: AWS S3 Gateway Backend

**Date:** 2026-02-23
**Session:** 21
**Status:** Complete

## What Was Implemented

### AwsGatewayBackend (`src/storage/aws.zig`)

Full implementation of all 10 StorageBackend vtable methods, proxying requests to
a real AWS S3 bucket via `std.http.Client`. The backend turns BleepStore into an
S3-compatible gateway/proxy in front of AWS S3.

**Vtable methods implemented (10/10):**
1. `putObject` — PUT to `{prefix}{bucket}/{key}` in upstream S3 bucket, compute MD5 ETag locally
2. `getObject` — GET from upstream S3, return body + locally-computed MD5 ETag
3. `deleteObject` — DELETE from upstream S3
4. `headObject` — HEAD to upstream S3, return size + locally-computed MD5 ETag
5. `copyObject` — GET source + PUT destination in upstream S3
6. `putPart` — PUT to `{prefix}.parts/{upload_id}/{part_number}` in upstream S3
7. `assembleParts` — Download all parts, concatenate locally, PUT final object, compute composite ETag
8. `deleteParts` — DELETE all part objects for a given upload ID
9. `createBucket` — No-op (logical operation; all buckets share one upstream S3 bucket)
10. `deleteBucket` — No-op (logical operation only)

**Key mapping:**
- Objects: `{prefix}{bleepstore_bucket}/{key}` in the single upstream S3 bucket
- Multipart parts: `{prefix}.parts/{upload_id}/{part_number}` in the upstream S3 bucket
- No per-bucket containers in upstream S3 — all namespaced via prefix

### SigV4 Signing for Outgoing Requests

All outgoing HTTP requests to AWS S3 are signed with AWS Signature V4, reusing
existing functions from `auth.zig`:
- `deriveSigningKey` — compute per-day HMAC signing key
- `buildCanonicalUri` — S3 URI encoding for path
- `buildCanonicalQueryString` — sorted, encoded query parameters
- `createCanonicalRequest` — assemble canonical request string
- `computeStringToSign` — compute string-to-sign from canonical request hash

Custom `formatAmzDate` function formats timestamps as `YYYYMMDDTHHMMSSZ`.

### HTTP Client Integration

Uses `std.http.Client.fetch()` (Zig 0.15.2 API) with response body collection via:
```zig
var response_body_list = std.ArrayList(u8).empty;
var gw = response_body_list.writer(allocator);
var adapter_buf: [8192]u8 = undefined;
var adapter = gw.adaptToNewApi(&adapter_buf);

const result = client.fetch(.{
    .location = .{ .url = url_str },
    .method = method_enum,
    .extra_headers = extra_headers,
    .payload = body,
    .response_writer = &adapter.new_interface,
});
```

### Config Integration

- New config fields in `config.zig`: `aws_access_key_id`, `aws_secret_access_key`, `aws_bucket`, `aws_region`, `aws_prefix`
- Backend selection: `storage.backend = aws` in config file
- Environment variable fallback: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
- Config takes priority over environment variables

### Backend Factory (`main.zig`)

Switch on `cfg.storage.backend` selects:
- `.local` — `LocalBackend` (existing)
- `.aws` — `AwsGatewayBackend` (new)
- `.gcp` — Error: "not yet implemented (Stage 11a)"
- `.azure` — Error: "not yet implemented (Stage 11b)"

### Init Verification

On startup, the AWS backend makes a HEAD request to the upstream S3 bucket to
verify it exists and the credentials are valid.

## Files Changed

| File | Change |
|------|--------|
| `src/storage/aws.zig` | REWRITTEN — full AWS gateway backend (848 lines, was stubs) |
| `src/config.zig` | Added AWS credential fields: `aws_access_key_id`, `aws_secret_access_key` |
| `src/main.zig` | Backend factory with AWS credential resolution and environment variable fallback |

## Key Technical Challenges

### Zig 0.15.2 `std.http.Client` API Discovery

The biggest challenge was discovering the correct `std.http.Client.fetch()` API in
Zig 0.15.2. The API changed significantly from 0.13/0.14:

- `client.open()` no longer exists
- `client.fetch()` is the single entry point
- `FetchOptions` has 12 fields (discovered via `@typeInfo` comptime inspection)
- Response body collection uses `response_writer: ?*Io.Writer` (a vtable-based interface)
- Creating an `Io.Writer` from `ArrayList(u8)`:
  1. `list.writer(allocator)` returns a `GenericWriter`
  2. `.adaptToNewApi(&buffer)` returns an `Adapter`
  3. `.new_interface` on the `Adapter` is the `Io.Writer`

System library source files could not be read directly due to sandbox permissions,
so the API was discovered through 10+ rounds of `@compileError` + `@typeInfo`
introspection at comptime.

### Errors Fixed

1. **`response_storage` not a field**: Previous session speculatively used this field name based on web search results. Correct field is `response_writer`.
2. **`adaptToNewApi()` needs buffer argument**: The function signature is `fn adaptToNewApi(self: *const Self, buffer: []u8) Adapter`. Fixed by adding `var adapter_buf: [8192]u8 = undefined;`.

## Key Decisions

1. **Single upstream S3 bucket**: All BleepStore buckets map to prefixes within one upstream S3 bucket. createBucket/deleteBucket are no-ops.
2. **Local MD5 computation**: ETags are computed locally (not from upstream headers) for consistency with the local backend.
3. **Composite ETags**: Multipart assembly downloads all parts locally, concatenates, computes composite ETag (`"hex-N"` format), then uploads the final object.
4. **Parts as temporary objects**: Multipart parts stored at `{prefix}.parts/{upload_id}/{part_number}` in upstream S3.
5. **Config > env vars**: AWS credentials from config file take priority over environment variables.
6. **`std.http.Client.fetch()` with `response_writer`**: Uses the `GenericWriter.adaptToNewApi()` pattern to bridge `ArrayList(u8).writer()` to the `Io.Writer` vtable interface.

## Unit Tests Added (7)

1. `test "AwsGatewayBackend: s3Key with prefix"` — prefix + bucket + key
2. `test "AwsGatewayBackend: s3Key without prefix"` — bucket + key only
3. `test "AwsGatewayBackend: s3Key with nested key"` — keys containing slashes
4. `test "AwsGatewayBackend: partKey"` — `.parts/{upload_id}/{part_number}`
5. `test "AwsGatewayBackend: formatAmzDate"` — ISO 8601 compact format
6. `test "AwsGatewayBackend: formatAmzDate date portion"` — YYYYMMDD extraction
7. `test "AwsGatewayBackend: vtable completeness"` — all 10 vtable function pointers non-null

## Test Results

- `zig build test` — **141/141 unit tests pass**, 0 memory leaks
- `zig build` — clean, no errors
- E2E tests not re-run (AWS backend requires real AWS credentials; local backend E2E is 85/86)

## Architecture Notes

The AWS gateway backend follows the same vtable pattern as `LocalBackend`:

```
AwsGatewayBackend
  ├── allocator: Allocator
  ├── client: std.http.Client (manages TLS, connection pooling)
  ├── region: []const u8
  ├── bucket: []const u8
  ├── prefix: []const u8
  ├── access_key: []const u8
  ├── secret_key: []const u8
  ├── storageBackend() → StorageBackend (vtable wrapper)
  └── makeSignedRequest(method, path, query, body, content_type) → HttpResult
        ├── Builds URL: https://{bucket}.s3.{region}.amazonaws.com/{path}?{query}
        ├── Signs with SigV4 (reuses auth.zig functions)
        └── Executes via std.http.Client.fetch()
```

All 10 vtable methods delegate to `makeSignedRequest()` with appropriate HTTP
methods and S3 REST API paths.
