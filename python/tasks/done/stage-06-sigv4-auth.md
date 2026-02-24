# Stage 6: AWS Signature V4 Authentication

## Completed: 2026-02-23

## What was implemented

### src/bleepstore/auth.py (complete rewrite from stub)
Full AWS Signature Version 4 authentication supporting both header-based auth
and presigned URL auth.

**SigV4Authenticator class:**
- `verify_request(request)` -- dispatches to header or presigned auth
- `_verify_header_auth(request)` -- full header-based verification flow
- `_verify_presigned(request)` -- full presigned URL verification flow
- `_parse_authorization_header(header)` -- regex parsing of Authorization header
- `_build_canonical_request(...)` -- canonical request construction per spec
- `_build_canonical_query_string_for_presigned(...)` -- excludes X-Amz-Signature
- `_build_string_to_sign(...)` -- string-to-sign assembly
- `_derive_signing_key(...)` -- HMAC-SHA256 chain with caching
- `_compute_signature(...)` -- final HMAC-SHA256 hex digest
- `_check_clock_skew(...)` -- 15-minute tolerance

**Module-level utilities:**
- `derive_signing_key()` -- standalone signing key derivation
- `_uri_encode(s, encode_slash)` -- S3-compatible URI encoding
- `_uri_encode_path(path)` -- path encoding preserving slashes
- `_build_canonical_query_string(query_string)` -- sorted, encoded query params
- `_trim_header_value(value)` -- whitespace normalization

### src/bleepstore/errors.py (4 new error classes)
- `InvalidAccessKeyId` (403)
- `AuthorizationQueryParametersError` (400)
- `RequestTimeTooSkewed` (403)
- `ExpiredPresignedUrl` (403, code="AccessDenied")

### src/bleepstore/server.py (auth middleware added)
- Added `auth_middleware` to `_register_middleware()` function
- Middleware runs after common-headers (reverse registration order)
- Skip paths: /health, /metrics, /docs, /docs/oauth2-redirect, /openapi.json, /redoc
- Skips when `auth.enabled=False` or no metadata store available
- Catches S3Error internally (middleware exceptions bypass FastAPI exception handlers)
- Stores access_key, owner_id, display_name on request.state on success

### tests/test_auth.py (58 new tests)
12 test classes covering:
- Signing key derivation (5 tests)
- URI encoding (7 tests)
- URI path encoding (5 tests)
- Canonical query string (7 tests)
- Header value trimming (3 tests)
- Authorization header parsing (3 tests)
- Canonical request construction (4 tests)
- String-to-sign (2 tests)
- Signature computation (3 tests)
- Header auth integration (9 tests)
- Presigned URL integration (6 tests)
- Auth disabled (1 test)
- Signing key cache (2 tests)

### tests/conftest.py (updated)
- Set `auth.enabled=False` in session config so existing tests pass without signing

## Key decisions

1. **Middleware error handling**: Auth middleware catches `S3Error` internally and returns
   XML response directly, because FastAPI exception handlers only catch exceptions from
   route handlers, not middleware.

2. **Test isolation**: Existing tests use `auth.enabled=False`. Auth tests use an
   `auth_client` fixture that temporarily enables auth on the session-scoped app and
   swaps in fresh metadata, then restores the original state. This avoids creating a
   second `create_app()` which would cause Prometheus duplicate metric registration.

3. **Signing key cache**: Simple dict keyed by `(access_key, date, region, service)`.
   Auto-evicts when exceeding 100 entries. No TTL -- keys naturally expire when the
   date changes.

4. **URI encoding**: Uses `urllib.parse.quote()` with `safe` parameter. Path encoding
   splits on `/`, encodes each segment individually, then rejoins.

5. **Presigned URL canonical query**: Excludes `X-Amz-Signature` from the query string
   during signature computation, per the SigV4 spec. Decodes then re-encodes all params.

6. **Clock skew**: 900 seconds (15 minutes) tolerance, matching AWS behavior.

## Files changed
- `src/bleepstore/auth.py` -- complete rewrite
- `src/bleepstore/errors.py` -- 4 new error classes
- `src/bleepstore/server.py` -- auth middleware added
- `tests/conftest.py` -- auth disabled in session config
- `tests/test_auth.py` -- new (58 tests)

## Test results
- 392 tests total (334 existing + 58 new)
- All passing, zero warnings
- Existing tests unaffected (auth disabled in config)
