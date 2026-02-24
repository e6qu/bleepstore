# Stage 6: AWS Signature V4 Authentication

**Completed:** 2026-02-23

## Summary

Implemented full AWS Signature Version 4 authentication for the BleepStore Go implementation. This includes header-based SigV4 verification, presigned URL validation, and HTTP auth middleware that wraps all S3 routes.

## Files Created

### `internal/auth/sigv4.go` (full rewrite from stub)
- `SigV4Verifier` struct with `Meta metadata.MetadataStore` and `Region string`
- `NewSigV4Verifier(meta, region)` constructor
- `VerifyRequest(r *http.Request) (*CredentialRecord, error)` — header-based SigV4 verification
- `VerifyPresigned(r *http.Request) (*CredentialRecord, error)` — presigned URL validation
- `URIEncode(s string, encodeSlash bool) string` — S3-compatible URI encoding
- `DetectAuthMethod(r *http.Request) string` — detects header/presigned/ambiguous/none
- `AuthError` type with S3-compatible error codes
- `parseAuthorizationHeader` — parses AWS4-HMAC-SHA256 Authorization header
- `buildCanonicalRequest` / `buildPresignedCanonicalRequest` — canonical request construction
- `buildStringToSign` — string-to-sign construction
- `deriveSigningKey` — HMAC-SHA256 chain for signing key derivation
- `canonicalURI`, `canonicalQueryString`, `canonicalHeaders` — helper functions
- `hmacSHA256` — HMAC-SHA256 helper
- Context functions: `OwnerFromContext`, `contextWithOwner` with unexported key types

### `internal/auth/middleware.go` (new)
- `Middleware(verifier)` returns middleware `func(http.Handler) http.Handler`
- Skips: /health, /metrics, /docs, /openapi, /openapi.json
- Detects auth method and calls appropriate verifier method
- Sets authenticated owner on request context
- Maps AuthError to S3 error XML responses
- `writeAuthError` — maps error codes to pre-defined S3 errors

### `internal/auth/sigv4_test.go` (new)
- 23+ test functions with real SQLite metadata store
- URIEncode: 11 table-driven cases
- HMAC-SHA256 known test vector
- DeriveSigningKey AWS documentation test vector
- CanonicalURI: 6 cases
- CanonicalQueryString: 5 cases
- ParseAuthorizationHeader: valid, wrong algorithm, missing credential, invalid format
- DetectAuthMethod: 4 cases
- VerifyRequest round-trips: valid signature, wrong secret, invalid access key, missing auth, clock skew, PUT with payload hash, query params
- VerifyPresigned: valid, expired, invalid expires
- OwnerFromContext: empty and populated
- BuildStringToSign: format verification
- Multiple credentials: two users, correct lookup

## Files Modified

### `internal/server/server.go`
- Added `auth` import
- Added `verifier *auth.SigV4Verifier` field to Server struct
- `New()`: creates SigV4Verifier when metadata store available
- `ListenAndServe()`: middleware chain now metricsMiddleware -> commonHeaders -> authMiddleware -> router
- Auth middleware only applied when verifier is non-nil (backward compatible)

## Key Decisions

1. **Credential lookup from MetadataStore**: Rather than hardcoding credentials, the verifier looks up access keys from the credentials table in SQLite. This supports multiple access keys and matches the credential seeding done in main.go.

2. **Auth middleware placement**: Auth middleware wraps the router inside commonHeaders, so common headers (x-amz-request-id, Date, Server) are set even on auth failure responses. This matches real S3 behavior.

3. **Excluded paths**: /health, /metrics, /docs, /openapi.json are infrastructure endpoints that should not require SigV4 authentication.

4. **Context-based owner identity**: Uses unexported context key types (`ownerIDKey`, `ownerDisplayKey`) to avoid collisions. Handlers can retrieve the authenticated owner via `auth.OwnerFromContext(r.Context())`.

5. **Custom URI encoding**: Go's `url.PathEscape` over-encodes for S3 purposes. Custom `URIEncode` only encodes characters outside A-Za-z0-9-_.~ and optionally preserves forward slashes.

6. **Constant-time comparison**: `crypto/subtle.ConstantTimeCompare` prevents timing-based signature guessing attacks.

7. **No new dependencies**: All implementation uses stdlib packages: `crypto/hmac`, `crypto/sha256`, `crypto/subtle`, `encoding/hex`, `strings`, `time`.

8. **Backward compatibility**: Tests without metadata store create a nil verifier; auth middleware is not applied, preserving existing test behavior.

## Dependencies Added
None — all stdlib.

## Testing Notes
- Unit tests use real SQLite metadata store in temp directories
- `signRequest` helper implements full SigV4 signing for test requests
- AWS documentation test vectors used for `deriveSigningKey` validation
- Tests cover both positive (valid signatures) and negative (wrong key, expired, clock skew) cases
