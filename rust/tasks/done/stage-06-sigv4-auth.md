# Stage 6: AWS Signature V4 Authentication

## Summary
Implemented full AWS Signature Version 4 authentication for both header-based and presigned URL requests. Added auth middleware to the axum router that intercepts all S3 API requests and verifies signatures against credentials stored in the SQLite metadata store.

## Files Changed

### Modified
- **Cargo.toml**: Added `subtle = "2"` for constant-time comparison
- **src/auth.rs**: Complete rewrite from stubs (~90 lines) to full implementation (~550 lines)
- **src/server.rs**: Added `auth_middleware` function and wired it as innermost layer

### Not Changed (already complete from Stage 2)
- **src/metadata/store.rs**: `CredentialRecord` type and `get_credential()`/`put_credential()` trait methods already existed
- **src/metadata/sqlite.rs**: `get_credential()`, `put_credential()`, `seed_credential()` already implemented
- **src/errors.rs**: `AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch` variants already existed

## Implementation Details

### auth.rs: SigV4 Core Algorithm
1. **Auth type detection**: `detect_auth_type()` checks for Authorization header (starts with `AWS4-HMAC-SHA256`) or X-Amz-Algorithm query parameter. Both present = error.
2. **Authorization header parsing**: `parse_authorization_header()` extracts Credential (splits on `/` to get access_key, date, region, service, terminator), SignedHeaders, and Signature.
3. **Presigned URL parsing**: `parse_presigned_params()` extracts all X-Amz-* parameters, percent-decodes credential, validates algorithm, date matching, and expiration range.
4. **Canonical request construction**: `build_canonical_request()` assembles method + URI + sorted query string + canonical headers + signed headers + payload hash. `build_canonical_query_string()` sorts parameters, URI-encodes, excludes X-Amz-Signature.
5. **String to sign**: `build_string_to_sign()` = `AWS4-HMAC-SHA256\n{timestamp}\n{scope}\nSHA256({canonical_request})`.
6. **Signing key derivation**: `derive_signing_key()` = 4-step HMAC-SHA256 chain: `AWS4{secret}` -> date -> region -> service -> `aws4_request`.
7. **Signature computation**: `compute_signature()` = `hex(HMAC-SHA256(signing_key, string_to_sign))`.
8. **Constant-time comparison**: `constant_time_eq()` uses `subtle::ConstantTimeEq` to prevent timing attacks.
9. **URI encoding**: `s3_uri_encode()` encodes per RFC 3986 with S3 exceptions (unreserved chars pass through, slash encoding optional).

### server.rs: Auth Middleware
- **Skip paths**: /health, /metrics, /docs, /openapi.json, /docs/*
- **Header-based auth flow**:
  1. Extract Authorization header
  2. Parse into ParsedAuthorization
  3. Look up credential by access_key_id in metadata store
  4. Check clock skew (15 min tolerance)
  5. Validate credential date matches x-amz-date
  6. Extract headers for signing, get payload hash
  7. Verify signature via `verify_header_auth()`
- **Presigned URL auth flow**:
  1. Extract X-Amz-* query parameters
  2. Parse into ParsedPresigned
  3. Look up credential
  4. Check expiration
  5. Extract headers, verify signature via `verify_presigned_auth()`
- **Error responses**: AccessDenied (no auth, clock skew, expired), InvalidAccessKeyId (unknown key), SignatureDoesNotMatch (bad signature)
- **Layer ordering**: metrics (outer) -> common_headers -> auth (inner) -> handlers

## Key Design Decisions
- Auth middleware is the innermost layer so /health, /metrics, /docs bypass auth
- UNSIGNED-PAYLOAD is default when x-amz-content-sha256 header is missing
- No signing key cache in Phase 1 (simple, correct, fast enough for single-node)
- Presigned URLs always use UNSIGNED-PAYLOAD as payload hash
- Clock skew tolerance: 900 seconds (15 minutes) per AWS spec
- Max presigned expiration: 604800 seconds (7 days) per AWS spec
- Credential date must match x-amz-date date portion (first 8 chars)
- Multiple header values for same name are joined with comma per canonical header spec

## Unit Tests (25 new tests in auth.rs)
- derive_signing_key: correctness via manual HMAC chain, determinism, different dates/regions produce different keys
- s3_uri_encode: unreserved chars, spaces (%20), slashes (encode_slash flag), special chars
- parse_authorization_header: valid parse, missing credential, bad algorithm prefix
- build_canonical_query_string: empty, sorted, no-value params, excludes X-Amz-Signature
- build_canonical_request: correct structure
- build_string_to_sign: correct structure
- constant_time_eq: same, different, different length
- parse_amz_date: valid date, epoch, invalid format
- collapse_whitespace: multiple spaces collapsed
- detect_auth_type: none, header, presigned, both=error
- verify_header_auth_roundtrip: full end-to-end sign + verify cycle
- verify_header_auth_wrong_secret: wrong secret fails verification
- percent_decode: %20, %2F, no encoding
- verify_presigned_roundtrip: full end-to-end presigned sign + verify cycle

## New Dependency
- `subtle = "2"` -- provides `ConstantTimeEq` trait for timing-safe comparison

## Issues/Notes
- The existing credential seeding in main.rs (`seed_credential()`) already handles crash-only startup: seeds default credentials from config on every startup using INSERT OR IGNORE (idempotent).
- boto3 SDK automatically signs all requests with SigV4, so existing E2E tests should pass without modification once the server is running with matching credentials in bleepstore.example.yaml.
