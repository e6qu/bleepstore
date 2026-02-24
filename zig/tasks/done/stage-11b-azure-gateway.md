# Stage 11b: Azure Blob Storage Gateway Backend

**Date:** 2026-02-24
**Session:** 23

## Summary

Implemented the full Azure Blob Storage gateway backend, replacing the stub
implementation with a complete StorageBackend vtable that proxies all object
storage operations to a real upstream Azure Blob Storage container via
`std.http.Client` and the Azure Blob REST API.

## What Was Implemented

### AzureGatewayBackend (`src/storage/azure.zig`)

Complete rewrite from stub (all methods were `@panic("not implemented")`):

- **Initialization**: Resolves `AZURE_ACCESS_TOKEN` from environment, builds
  Azure host URL, creates HTTP client, verifies upstream container exists.
- **10 StorageBackend vtable methods**:
  - `putObject`: PUT blob with `x-ms-blob-type: BlockBlob`, local MD5 ETag
  - `getObject`: GET blob, 404 maps to `error.NoSuchKey`
  - `deleteObject`: DELETE blob, idempotent (202/200/404 all OK)
  - `headObject`: HEAD blob, returns content_length
  - `copyObject`: Server-side copy via `x-ms-copy-source`, download for MD5
  - `putPart`: Store as temporary blob at `{prefix}.parts/{upload_id}/{part_number}`
  - `assembleParts`: Download temps, stage blocks, commit via Put Block List XML
  - `deleteParts`: Delete temp blobs (iterate 1-100, idempotent)
  - `createBucket`: No-op (gateway mode)
  - `deleteBucket`: No-op (gateway mode)
- **Block IDs**: `base64("{upload_id}:{part_number:05}")` format
- **HTTP helpers**: `makeAzureRequest`, `makeAzureRequestWithBlobType`,
  `makeAzureRequestWithCopySource`
- **Base64 helpers**: `base64Encode`, `base64Decode`

### Backend Factory (`src/main.zig`)

- Added `AzureGatewayBackend` import
- Added `.azure` case to backend switch with validation of required config
- Added cleanup in defer block

## Key Decisions

1. **Bearer token auth** via `AZURE_ACCESS_TOKEN` env var (avoids SharedKey HMAC complexity)
2. **Temp blob strategy for parts**: StorageBackend vtable's `putPart` does not
   receive the object key, so parts are stored as temporary blobs and downloaded
   during assembly to stage as blocks on the final blob
3. **Block ID format**: `base64("{upload_id}:{part_number:05}")` ensures uniqueness
   across concurrent uploads and consistent base64 length
4. **API version**: `x-ms-version: 2023-11-03` on all requests
5. **Azure-specific HTTP status codes**: DELETE returns 202, PUT blob returns 201

## Files Changed

- `src/storage/azure.zig` -- Complete rewrite (~580 lines)
- `src/main.zig` -- Azure backend factory wiring

## Test Results

- `zig build` -- clean, no errors
- `zig build test` -- 160/160 pass, 0 memory leaks (+10 new Azure tests)
- New tests: blobName mapping (2), blobPath mapping, blockId format, blockId
  consistency, blockId different upload IDs, base64Encode, base64 round-trip,
  vtable completeness, part blob path mapping

## Issues Encountered

None -- the implementation followed the established patterns from AWS and GCP
backends closely. The Azure Blob REST API is well-documented and straightforward.
