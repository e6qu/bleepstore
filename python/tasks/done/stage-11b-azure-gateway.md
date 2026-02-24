# Stage 11b: Azure Blob Storage Gateway Backend

**Completed:** 2026-02-23

## What Was Implemented

Azure Blob Storage gateway backend — the third and final cloud backend, completing the
AWS/GCP/Azure trio. Proxies all data operations to an upstream Azure Blob Storage container.

## Key Design Decisions

1. **SDK**: `azure-storage-blob` with async transport + `azure-identity` for `DefaultAzureCredential`
2. **Client model**: `ContainerClient` as primary client (all blobs in one Azure container), `BlobClient` via `get_blob_client()` for per-blob operations
3. **Key mapping**: `{prefix}{bleepstore_bucket}/{key}` — same pattern as AWS/GCP
4. **Multipart strategy**: Azure Block Blob approach — **no temporary part objects**:
   - `put_part()` → `stage_block(block_id, data)` directly on the final blob
   - `assemble_parts()` → `commit_block_list(block_list)` to finalize
   - `delete_parts()` → no-op (uncommitted blocks auto-expire in 7 days)
5. **Block IDs**: `base64(upload_id:part_number)` — includes upload_id to avoid collisions between concurrent multipart uploads to the same key
6. **ETag**: Compute MD5 locally for consistency (Azure ETags may differ from MD5)
7. **Error mapping**: `ResourceNotFoundError` → `FileNotFoundError`
8. **Credentials**: `DefaultAzureCredential` (env vars, managed identity, Azure CLI)

## Files Changed

| File | Change |
|------|--------|
| `pyproject.toml` | Added `azure = ["azure-storage-blob>=12.19.0", "azure-identity>=1.15.0"]` optional deps, added to dev deps |
| `src/bleepstore/storage/azure.py` | Full rewrite: 12 protocol methods (~260 lines) |
| `src/bleepstore/server.py` | Added `elif backend == "azure":` branch in factory |
| `tests/test_storage_azure.py` | New: 40 tests across 13 test classes |

## Differences from AWS/GCP Backends

- **No temporary part objects**: AWS/GCP store parts as separate objects (`{prefix}.parts/{upload_id}/{part_number}`), Azure stages blocks directly on the final blob
- **delete_parts is a no-op**: No cleanup needed — uncommitted blocks auto-expire in 7 days
- **Native exists()**: Uses `blob_client.exists()` (HEAD request) instead of download with Range header (GCP) or head_object (AWS)
- **copy_object**: Uses `start_copy_from_url()` (URL-based server-side copy) instead of CopySource dict (AWS) or copy() (GCP)

## Test Results

- **Azure unit tests**: 40/40 pass
- **All unit tests**: 582/582 pass
- **E2E tests**: 86/86 pass (local backend, unchanged)
