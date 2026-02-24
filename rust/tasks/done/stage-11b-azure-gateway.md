# Stage 11b: Azure Blob Storage Gateway Backend

**Date:** 2026-02-24
**Status:** Implementation complete, build/test verification pending

## What Was Implemented

Full Azure Blob Storage gateway backend that proxies all storage operations to an upstream Azure Blob Storage container via the Azure Blob REST API.

### Files Changed

| File | Change |
|------|--------|
| `src/storage/azure.rs` | Complete rewrite from stub to full implementation (~700 lines) |
| `src/main.rs` | Added `"azure"` case to backend factory |
| `STATUS.md` | Updated to Stage 11b |
| `DO_NEXT.md` | Updated to Stage 12a |
| `WHAT_WE_DID.md` | Added Session 16 entry |
| `PLAN.md` | Marked Stage 11b complete |
| `tasks/done/stage-11b-azure-gateway.md` | This file |

### Key Design Decisions

1. **reqwest + Azure Blob REST API** instead of `azure_storage_blobs` crate
   - Same approach as GCP backend (reqwest HTTP calls)
   - Avoids adding heavy Azure SDK dependencies
   - All existing crate dependencies suffice (reqwest, base64, hmac, sha2, md5, hex, percent-encoding, httpdate)

2. **Azure Shared Key authentication** via HMAC-SHA256
   - Full string-to-sign construction per Azure REST API spec
   - Canonicalized headers (x-ms-* sorted)
   - Canonicalized resource (account/container/blob + sorted query params)
   - Also supports SAS token auth (appended as query parameter)

3. **Parts stored as temporary blobs** (same as AWS/GCP backends)
   - StorageBackend trait doesn't pass object key to `put_part()`, so we can't stage blocks directly on the final blob
   - Parts stored at `{prefix}.parts/{upload_id}/{part_number}`
   - At assembly time: download parts -> Put Block on final blob -> Put Block List to commit
   - Block IDs: `base64("{upload_id}:{part_number:05}")` includes upload_id for collision avoidance

4. **Delete parts**: Best-effort cleanup via List Blobs API with prefix filter
   - Lists blobs with `{prefix}.parts/{upload_id}/` prefix
   - Deletes each found blob individually
   - Robust: handles pagination, logs failures as warnings

5. **Azure REST API version**: 2023-11-03

6. **Credential resolution order**:
   - `AZURE_STORAGE_KEY` environment variable (Shared Key)
   - `AZURE_STORAGE_CONNECTION_STRING` (extracts AccountKey)
   - `AZURE_STORAGE_SAS_TOKEN` (SAS token)

### Unit Tests Added (20 total)

- Blob name mapping (with and without prefix)
- Block ID generation, padding, uniqueness across uploads
- MD5 computation (empty, "hello world")
- Composite ETag (single part, multiple parts)
- Key mapping with nested keys and special characters
- Not-found status detection
- API version constant
- URL encoding (preserves '/', encodes spaces)
- Block list XML format
- RFC 1123 date format
- Part blob name mapping
- SAS token prefix handling

### Azure Blob REST API Endpoints Used

| Operation | HTTP Method | URL Pattern |
|-----------|------------|-------------|
| Put Blob | PUT | `/{container}/{blob}` |
| Get Blob | GET | `/{container}/{blob}` |
| Delete Blob | DELETE | `/{container}/{blob}` |
| Head Blob | HEAD | `/{container}/{blob}` |
| Put Block | PUT | `/{container}/{blob}?comp=block&blockid={id}` |
| Put Block List | PUT | `/{container}/{blob}?comp=blocklist` |
| Copy Blob | PUT | `/{container}/{blob}` + `x-ms-copy-source` header |
| List Blobs | GET | `/{container}?restype=container&comp=list&prefix={p}` |

### Differences from Python Reference

The Python Azure backend uses the `azure-storage-blob` SDK and stages blocks directly on the final blob (since Python's backend receives bucket+key at put_part time). Our Rust implementation uses the same temporary blob pattern as AWS/GCP because the StorageBackend trait only passes bucket (not key) to put_part(). At assembly time, parts are downloaded, staged as blocks, and committed.
