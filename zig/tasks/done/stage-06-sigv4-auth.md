# Stage 6: AWS Signature V4 Authentication

**Date:** 2026-02-23
**Status:** Complete
**Tests:** 118/118 pass (14 new auth tests)

## What Was Implemented

### 1. Full SigV4 Header-Based Authentication (`src/auth.zig`)

Complete rewrite of `auth.zig` from a stub (`verifyRequest` panicked) to a full AWS SigV4 implementation.

**Core flow:**
1. Parse `Authorization: AWS4-HMAC-SHA256 Credential=AKID/date/region/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=hex`
2. Build canonical request: method + URI + query string (sorted) + headers (lowercase, sorted) + signed headers + payload hash
3. Compute string-to-sign: `AWS4-HMAC-SHA256\ntimestamp\nscope\nhash(canonical_request)`
4. Derive signing key: HMAC chain `AWS4+secret -> date -> region -> s3 -> aws4_request`
5. Compute expected signature and compare with constant-time XOR comparison

**Functions implemented:**
- `detectAuthType` -- Determines auth method from request headers/query
- `verifyHeaderAuth` -- Full header-based SigV4 verification
- `verifyPresignedAuth` -- Presigned URL verification
- `parseAuthorizationHeader` -- Extracts Credential, SignedHeaders, Signature
- `parsePresignedParams` -- Extracts X-Amz-* query parameters
- `buildCanonicalUri` -- S3-compatible URI encoding of path
- `buildCanonicalQueryString` -- Sorted, URI-encoded query parameters
- `buildPresignedCanonicalQueryString` -- Same but excludes X-Amz-Signature
- `buildCanonicalHeaders` -- Lowercase, trimmed, sorted headers
- `computeStringToSign` -- Algorithm + date + scope + hash
- `buildScope` -- `date/region/s3/aws4_request`
- `s3UriEncode` / `s3UriEncodeAppend` -- RFC 3986 URI encoding
- `constantTimeEql` -- XOR accumulation to prevent timing attacks
- `isTimestampWithinSkew` -- 15-minute clock skew validation
- `parseAmzTimestampToEpoch` -- Parses `YYYYMMDDTHHMMSSZ` to epoch
- `extractAccessKeyFromHeader` / `extractAccessKeyFromQuery` -- Access key extraction

### 2. Presigned URL Authentication

- Extracts query params: X-Amz-Algorithm, X-Amz-Credential, X-Amz-Date, X-Amz-Expires, X-Amz-SignedHeaders, X-Amz-Signature
- Builds canonical query string excluding X-Amz-Signature
- Uses `UNSIGNED-PAYLOAD` as payload hash
- Validates expiration (1-604800 seconds)
- Supports both `/` and `%2F` as credential field separators

### 3. Auth Middleware (`src/server.zig`)

- `authenticateRequest` function runs before S3 routing in `handleS3CatchAll`
- Infrastructure endpoints (/health, /metrics, /docs, /openapi.json) are exempt (separate tokamak routes)
- Credential lookup from SQLite metadata store via `getCredential`
- Proper memory management: credential secret key copied to arena, GPA originals freed
- Error mapping: `InvalidAccessKeyId`, `SignatureDoesNotMatch`, `RequestTimeTooSkewed`, `AccessDenied`
- Auth can be disabled via config (`auth.enabled = false`)

## Files Changed

| File | Changes |
|------|---------|
| `src/auth.zig` | Complete rewrite -- full SigV4 implementation with 14 tests |
| `src/server.zig` | Added `authenticateRequest`, `httpMethodToString`, `global_auth_enabled`, `global_allocator` |
| `STATUS.md` | Updated to Stage 6 complete |

## Key Decisions

1. **Auth in S3 catch-all, not as tokamak middleware**: tokamak middleware doesn't give enough control over error responses. Auth runs as the first step in `handleS3CatchAll`.
2. **Constant-time comparison via XOR**: Custom implementation rather than `std.crypto.utils.timingSafeEql` due to type constraints with dynamically-sized slices.
3. **Credential memory lifecycle**: SQLite allocates credential strings with GPA. Secret key is duped to request arena, then GPA originals freed via `global_allocator`.
4. **httpz method type inference**: `@TypeOf(@as(tk.Request, undefined).method)` since `tk.Request.Method` isn't directly accessible.
5. **S3 URI encoding**: Custom implementation following RFC 3986 (unreserved chars not encoded, spaces as `%20`, uppercase hex digits).
6. **Presigned credential separator**: Handles both `/` and `%2F` since some clients URL-encode the credential field.

## Issues Encountered

1. **`u8` vs `u4` type mismatch**: `ch >> 4` produces `u8` but `hexDigitUpper` expects `u4`. Fixed with `@as(u4, @truncate(ch >> 4))`.
2. **`tk.Request.Method` not accessible**: httpz doesn't expose the method enum as a direct type path. Fixed with `@TypeOf`.
3. **Complex credential parsing**: Initial presigned params parsing was overly complex. Simplified with consistent `findCredSep` helper.

## Test Coverage

14 auth tests added (12 new for Stage 6, 2 pre-existing):
- `deriveSigningKey` -- AWS reference test vector
- `createCanonicalRequest` -- Canonical request format
- `parseAuthorizationHeader` -- Header parsing
- `buildCanonicalHeaders` -- Header normalization
- `buildCanonicalQueryString` -- Query sorting/encoding
- `s3UriEncode` -- URI encoding edge cases
- `constantTimeEql` -- Equality and inequality
- `isTimestampWithinSkew` -- Clock skew validation
- `parseAmzTimestampToEpoch` -- Timestamp parsing
- `detectAuthType` -- Auth type detection
- `extractAccessKeyFromHeader` -- Access key extraction
- `verifyHeaderAuth` -- End-to-end header auth
- `parsePresignedParams` -- Presigned parameter extraction
- `buildScope` -- Scope string construction
