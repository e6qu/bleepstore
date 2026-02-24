# Stage 11a: GCP Cloud Storage Gateway Backend

**Date:** 2026-02-24
**Session:** 22

## Summary

Implemented the full GCP Cloud Storage gateway backend in `src/storage/gcp.zig`,
replacing the stub implementation with a complete StorageBackend vtable that
proxies all object storage operations to a real upstream GCS bucket via
`std.http.Client` and the GCS JSON API. Updated `main.zig` backend factory to
support `backend = gcp`.

## What Was Implemented

### GcpGatewayBackend (`src/storage/gcp.zig`)
- Full 10-method StorageBackend vtable implementation
- GCS JSON API integration via raw HTTP calls (`std.http.Client.fetch()`)
- Bearer token authentication via `GCS_ACCESS_TOKEN` / `GOOGLE_ACCESS_TOKEN` env vars
- URL-encoded object names for GCS API paths (slashes -> `%2F`)
- GCS upload: `POST /upload/storage/v1/b/{bucket}/o?uploadType=media&name={name}`
- GCS download: `GET /storage/v1/b/{bucket}/o/{object}?alt=media`
- GCS metadata: `GET /storage/v1/b/{bucket}/o/{object}` (JSON response)
- GCS delete: `DELETE /storage/v1/b/{bucket}/o/{object}`
- GCS copy: `POST .../rewriteTo/...` (server-side rewrite API)
- GCS compose: `POST .../compose` with JSON body for multipart assembly
- Chain compose for >32 parts (batches of 32, intermediate objects cleaned up)
- List + delete for deleteParts (GCS list API with prefix filter)
- Simple JSON parsers: `parseGcsSize`, `parseGcsListNames`
- `gcsEncodeObjectName`: RFC 3986 percent-encoding for object names
- Init: verifies upstream bucket exists via list objects request
- createBucket/deleteBucket: no-ops (gateway mode)

### Backend Factory (`src/main.zig`)
- Added `GcpGatewayBackend` import
- Added `.gcp` case to backend switch
- Validates `gcp_bucket` config is set
- Added gcp_backend to defer cleanup

### Config (`src/config.zig`)
- No changes needed -- `gcp_bucket`, `gcp_project`, `gcp_prefix` fields were
  already present from the initial scaffold

## Key Decisions

1. **Bearer token auth** (not service account JWT): Simplest approach, avoids
   RSA signing complexity. User provides a pre-obtained OAuth2 token.
2. **GCS JSON API** throughout: GCS uses JSON (not XML like AWS S3). Simple
   string-based parsing used instead of `std.json.parseFromSlice` for small
   field extractions.
3. **Percent-encoding object names**: GCS API requires object names in URL path
   segments to have slashes encoded as `%2F`. Dedicated `gcsEncodeObjectName`
   helper handles this.
4. **Chain compose**: GCS limits compose to 32 sources. For >32 parts, compose
   in batches of 32 into intermediate objects, then compose intermediates,
   repeating until single object remains. Intermediates cleaned up.
5. **deleteParts via list**: Lists objects with `.parts/{upload_id}/` prefix
   and deletes each. Falls back to brute-force part number iteration (1-100)
   on JSON parse failure.
6. **headObject via metadata endpoint**: GCS has no HEAD-specific object
   endpoint. Uses GET metadata (no `?alt=media`) and parses `"size"` from JSON.

## Files Changed

| File | Change |
|------|--------|
| `src/storage/gcp.zig` | Complete rewrite from stub to full implementation |
| `src/main.zig` | Added GCP backend import and factory case |

## Test Results

- `zig build test` -- 150/150 pass, 0 memory leaks (+9 new GCP tests)
- `zig build` -- clean, no errors
- New tests: gcsName mapping (3), gcsEncodeObjectName, parseGcsSize,
  parseGcsListNames (2), vtable completeness, isUnreserved

## Issues Encountered

None. The implementation followed the same patterns established by the AWS
gateway backend in Stage 10. Config fields were already scaffolded.
