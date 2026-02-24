# BleepStore Python -- Status

## Current Stage: Stage 11b COMPLETE (Azure Gateway Backend) — 86/86 E2E Tests Passing

- `uv run pytest tests/ -v` — 582/582 pass (40 new Azure backend tests)
- `./run_e2e.sh` — **86/86 pass**
- Stage 11b: Azure Blob Storage gateway backend fully implemented and tested

## Framework: FastAPI + Pydantic + uvicorn

## What Works
- All S3 operations fully implemented (Stages 1–8)
- SigV4 authentication (header-based + presigned URLs)
- Bucket CRUD, Object CRUD, List/Copy/Batch Delete
- Range requests, Conditional requests, Object ACLs
- Multipart uploads (create, upload part, upload part copy, complete, abort, list)
- **AWS S3 gateway backend** (Stage 10) — proxies data to upstream AWS S3 bucket
- **GCP Cloud Storage gateway backend** (Stage 11a) — proxies data to upstream GCS bucket
- **Azure Blob Storage gateway backend** (Stage 11b) — proxies data to upstream Azure container
- Prometheus metrics at /metrics
- OpenAPI/Swagger UI at /docs, /openapi.json
- Crash-only design throughout

## E2E Test Results (2026-02-23)
- **86 passed, 0 failed** out of 86 total
- All bucket, object, multipart, presigned, ACL, and error tests pass

## AWS Gateway Backend (Stage 10)
- `AWSGatewayBackend` implements all 12 `StorageBackend` protocol methods via aiobotocore
- Key mapping: `{prefix}{bleepstore_bucket}/{key}` (all data in one upstream AWS bucket)
- Parts mapping: `{prefix}.parts/{upload_id}/{part_number}` (temporary S3 objects)
- Multipart assembly: AWS native multipart upload with `upload_part_copy` (server-side copy, no data download)
- Error mapping: `NoSuchKey`/404 → `FileNotFoundError` (matches local backend contract)
- Credentials: Standard AWS credential chain (env vars, ~/.aws/credentials, IAM role)
- Optional dependency: `pip install bleepstore[aws]` (aiobotocore)
- 35 unit tests with mocked aiobotocore client

## GCP Gateway Backend (Stage 11a)
- `GCPGatewayBackend` implements all 12 `StorageBackend` protocol methods via gcloud-aio-storage
- Key mapping: `{prefix}{bleepstore_bucket}/{key}` (all data in one upstream GCS bucket)
- Parts mapping: `{prefix}.parts/{upload_id}/{part_number}` (temporary GCS objects)
- Multipart assembly: GCS `compose()` with chaining for >32 parts (batches of 32, compose intermediates, repeat)
- Error mapping: HTTP 404 → `FileNotFoundError`, delete idempotent (catches 404)
- Credentials: GCS Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS, gcloud auth, metadata server)
- Optional dependency: `pip install bleepstore[gcp]` (gcloud-aio-storage)
- 43 unit tests with mocked gcloud-aio-storage client

## Azure Gateway Backend (Stage 11b)
- `AzureGatewayBackend` implements all 12 `StorageBackend` protocol methods via azure-storage-blob
- Key mapping: `{prefix}{bleepstore_bucket}/{key}` (all data in one upstream Azure container)
- Multipart strategy: Azure Block Blob — `stage_block()` directly on final blob, `commit_block_list()` to finalize
- No temporary part objects — uncommitted blocks auto-expire in 7 days (`delete_parts` is a no-op)
- Block IDs: `base64(upload_id:part_number)` — includes upload_id to avoid collisions
- Error mapping: `ResourceNotFoundError` → `FileNotFoundError`, delete idempotent
- Credentials: `DefaultAzureCredential` from azure-identity (env vars, managed identity, Azure CLI)
- Optional dependency: `pip install bleepstore[azure]` (azure-storage-blob, azure-identity)
- 40 unit tests with mocked azure-storage-blob client

## Known Test Issues
- None — all 86 E2E tests pass

## Next Steps
- Stage 12: Raft Consensus / Clustering
