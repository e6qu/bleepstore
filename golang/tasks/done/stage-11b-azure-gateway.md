# Stage 11b: Azure Blob Storage Gateway Backend

## Date: 2026-02-24

## Summary
Implemented the Azure Blob Storage gateway backend that proxies BleepStore data operations to an upstream Azure Blob Storage container. This is the third and final cloud gateway backend (after AWS and GCP).

## Key Design Differences from AWS/GCP
- **No temporary part objects**: Azure Block Blob primitives allow staging blocks directly on the final blob. PutPart -> StageBlock, AssembleParts -> CommitBlockList.
- **DeleteParts is a no-op**: Uncommitted Azure blocks auto-expire in 7 days.
- **Block ID includes uploadID**: `base64("{uploadID}:{05d partNumber}")` to avoid collisions between concurrent multipart uploads to the same key.

## Files Changed

### New Files
- `internal/storage/azure_client.go` -- Real Azure SDK client wrapper (UploadBuffer, DownloadStream, DeleteBlob, GetProperties, StartCopyFromURL, StageBlock, CommitBlockList)
- `internal/storage/azure_test.go` -- 25 unit tests with mock-based coverage

### Modified Files
- `internal/storage/azure.go` -- Full implementation from stub (was all TODOs)
- `internal/config/config.go` -- Added AzureAccountURL, AzurePrefix fields
- `cmd/bleepstore/main.go` -- Added "azure" case to backend factory
- `go.mod` -- Added Azure SDK dependencies

## Interface Design
- `AzureBlobAPI` interface with 8 methods: UploadBlob, DownloadBlob, DeleteBlob, BlobExists, GetBlobProperties, StartCopyFromURL, StageBlock, CommitBlockList
- Mock implementation stores blobs in-memory, tracks staged blocks separately
- Real implementation uses `azblob.Client` with `azidentity.NewDefaultAzureCredential`

## Config Options
```yaml
storage:
  backend: azure
  azure_container: my-container    # required
  azure_account: mystorageaccount  # used to construct URL
  azure_account_url: https://mystorageaccount.blob.core.windows.net  # optional override
  azure_prefix: bp/               # optional key prefix
```

## Test Count
- 25 new Azure gateway tests
- Total: 251+ unit/integration tests passing

## Dependencies Added
- `github.com/Azure/azure-sdk-for-go/sdk/azcore v1.21.0`
- `github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.9.0`
- `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.1`

## Issues
- None. E2E should remain at 85/86 (same Go runtime limitation with Transfer-Encoding: identity).
- User must run `go get` + `go mod tidy` to fetch Azure SDK dependencies and update go.sum.
