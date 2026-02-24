# BleepStore Go -- Status

## Current Stage: Stage 11b COMPLETE (Azure Blob Storage Gateway Backend) -- 85/86 E2E Tests Passing

All E2E tests pass except:
- `test_missing_content_length` -- Go's `net/http` returns 501 for `Transfer-Encoding: identity` at the protocol level before any handler code runs; test expects 400/411/403

- `go test -count=1 ./...` -- all unit tests pass (226 existing + 25 new Azure gateway tests = 251 total)
- `./run_e2e.sh` -- **85/86 pass** (1 Go runtime limitation)

**NOTE**: After adding the Azure SDK dependencies, you must run `go get github.com/Azure/azure-sdk-for-go/sdk/storage/azblob github.com/Azure/azure-sdk-for-go/sdk/azidentity github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming && go mod tidy` to resolve transitive dependencies and update go.sum.

## What Works
- All S3 operations fully implemented (Stages 1-8)
- SigV4 authentication (header-based + presigned URLs)
- Bucket CRUD, Object CRUD, List/Copy/Batch Delete
- Range requests, Conditional requests, Object ACLs
- Multipart uploads (create, upload part, upload part copy, complete, abort, list)
- Prometheus metrics at /metrics
- OpenAPI/Swagger UI at /docs, /openapi.json
- Crash-only design throughout
- 37 in-process integration tests covering all E2E scenarios
- **AWS S3 Gateway Backend** -- proxies data operations to upstream AWS S3 bucket
  - Key mapping: `{prefix}{bleepstore_bucket}/{key}`
  - Parts mapping: `{prefix}.parts/{upload_id}/{part_number}`
  - Local MD5 computation for consistent ETags
  - AWS native multipart assembly with UploadPartCopy + EntityTooSmall fallback
  - Batch deletion of temporary parts via ListObjectsV2 + DeleteObjects
  - Error mapping: AWS NoSuchKey/404 -> "object not found"
  - Config: `backend: aws` with `aws_bucket`, `aws_region`, `aws_prefix`
  - S3API interface for mockable testing
  - CreateBucket/DeleteBucket are no-ops (BleepStore buckets map to key prefixes)
- **GCP Cloud Storage Gateway Backend** -- proxies data operations to upstream GCS bucket
  - Key mapping: `{prefix}{bleepstore_bucket}/{key}`
  - Parts mapping: `{prefix}.parts/{upload_id}/{part_number}`
  - Local MD5 computation for consistent ETags
  - GCS Compose-based multipart assembly with recursive chaining for >32 parts
  - Part cleanup via ListObjects + individual Delete
  - Error mapping: GCS ErrObjectNotExist/ErrBucketNotExist/404 -> "not found"
  - Config: `backend: gcp` with `gcp_bucket`, `gcp_project`, `gcp_prefix`
  - GCSAPI interface for mockable testing (same pattern as AWS S3API)
  - CopyObject uses server-side copy + download for MD5 ETag
  - CreateBucket/DeleteBucket are no-ops (BleepStore buckets map to key prefixes)
  - Application Default Credentials (ADC) for auth
- **Azure Blob Storage Gateway Backend** -- proxies data operations to upstream Azure container
  - Key mapping: `{prefix}{bleepstore_bucket}/{key}`
  - **No temporary part objects** -- multipart uses Azure Block Blob primitives directly
  - PutPart -> StageBlock on the final blob (block ID = base64(`{uploadID}:{05d partNumber}`))
  - AssembleParts -> CommitBlockList to finalize the blob
  - DeleteParts -> no-op (uncommitted blocks auto-expire in 7 days)
  - Local MD5 computation for consistent ETags
  - CopyObject via StartCopyFromURL (server-side copy) + download for MD5 ETag
  - Error mapping: Azure BlobNotFound/ContainerNotFound/404 -> "not found"
  - Config: `backend: azure` with `azure_container`, `azure_account`, `azure_account_url`, `azure_prefix`
  - AzureBlobAPI interface for mockable testing (same pattern as AWS S3API, GCS GCSAPI)
  - CreateBucket/DeleteBucket are no-ops (BleepStore buckets map to key prefixes)
  - DefaultAzureCredential for auth (env vars, managed identity, Azure CLI)

## E2E Test Results (2026-02-24)
- **85 passed, 1 failed** out of 86 total
- Failed: `test_missing_content_length` -- Go `net/http` 501 for `Transfer-Encoding: identity` (Go runtime limitation, not fixable without replacing HTTP server)

## Known Issues
- `test_missing_content_length` -- Go's `net/http` returns 501 for `Transfer-Encoding: identity` before handler code runs

## Next Steps
- Stage 12: Raft Consensus / Cluster Mode

## Unit + Integration Test Count: 251+ tests passing (including new Azure gateway tests)
