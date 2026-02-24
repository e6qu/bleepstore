# Stage 11a: GCP Cloud Storage Gateway Backend

**Date:** 2026-02-24
**Status:** Complete

## What Was Implemented

Full GCP Cloud Storage gateway backend that proxies all storage operations to an
upstream GCS bucket via the GCS JSON API using `reqwest`.

### Files Changed

| File | Changes |
|------|---------|
| `src/storage/gcp.rs` | Complete rewrite from stub to full implementation (~650 lines) |
| `src/main.rs` | Added `"gcp"` case to backend factory |
| `Cargo.toml` | Added `reqwest` (moved from dev-deps) and `base64` dependencies |
| `STATUS.md` | Updated to Stage 11a COMPLETE |
| `DO_NEXT.md` | Updated to point at Stage 11b |
| `WHAT_WE_DID.md` | Appended session entry |
| `PLAN.md` | Marked Stage 11a with checkmark |

### Key Design Decisions

1. **reqwest + GCS JSON API** chosen over `google-cloud-storage` crate for simplicity and
   control. The GCS JSON API is well-documented and reqwest is already a dependency.

2. **OAuth2 credential resolution** follows the Application Default Credentials (ADC) chain:
   - `GOOGLE_APPLICATION_CREDENTIALS` env var (service account JSON key file)
   - gcloud application-default credentials file (refresh token flow)
   - GCE metadata server (when running on Google Cloud)
   - `GOOGLE_OAUTH_ACCESS_TOKEN` env var as ultimate fallback
   - JWT signing for service accounts falls back to env var/metadata (no RSA crate needed)

3. **Token caching** with Mutex-guarded cache and 60-second safety margin before expiry.
   Avoids redundant OAuth2 token requests.

4. **GCS compose for multipart assembly**: GCS compose supports max 32 source objects.
   For >32 parts, implements recursive chain composition:
   - Batch sources into groups of 32
   - Compose each batch into an intermediate object
   - Repeat until <= 32 intermediates remain
   - Final compose into destination
   - Clean up all intermediate objects

5. **Server-side copy via GCS rewrite API**: Handles large objects transparently via
   rewrite token loop (GCS may require multiple calls for large objects).

6. **MD5 computed locally** for consistent ETags (GCS ETags differ from S3 ETags).
   Composite ETag computed from part MD5s for multipart completions.

7. **Idempotent operations**: Delete catches 404 silently. Create/delete bucket are no-ops
   (BleepStore buckets are prefix-namespaced within single upstream GCS bucket).

8. **Same patterns as AWS backend**: Key mapping, part mapping, error handling, content hash
   computation all follow the same conventions established in Stage 10.

### Test Results

- `cargo test`: 174 passed (19 new GCP tests + 155 existing)
- Unit tests cover: key mapping, MD5 computation, composite ETag, URL encoding,
  GCS error JSON parsing, compose chaining math, base64 MD5 conversion, not-found detection

### New Dependencies

| Dependency | Version | Purpose |
|-----------|---------|---------|
| `reqwest` | 0.12 (json feature) | HTTP client for GCS JSON API (moved from dev-deps to deps) |
| `base64` | 0.22 | Base64 encoding/decoding for GCS MD5 hash conversion |

### API Mapping

| StorageBackend method | GCS API call |
|----------------------|--------------|
| `put` | POST upload/storage/v1/b/{bucket}/o (media upload) |
| `get` | GET storage/v1/b/{bucket}/o/{object}?alt=media |
| `delete` | DELETE storage/v1/b/{bucket}/o/{object} |
| `exists` | GET storage/v1/b/{bucket}/o/{object}?fields=name |
| `copy_object` | POST storage/v1/b/{bucket}/o/{src}/rewriteTo/b/{bucket}/o/{dst} |
| `put_part` | POST upload (same as put, stores at .parts/ path) |
| `assemble_parts` | POST storage/v1/b/{bucket}/o/{dst}/compose |
| `delete_parts` | GET (list) + DELETE (each part) |
| `create_bucket` | no-op (prefix-namespaced) |
| `delete_bucket` | no-op (prefix-namespaced) |
