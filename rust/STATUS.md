# BleepStore Rust -- Status

## Current Stage: Stage 11b COMPLETE (Azure Blob Storage Gateway Backend) -- PENDING BUILD VERIFICATION

Implementation complete. Build and test verification pending (cargo build, cargo test, ./run_e2e.sh).

- Stage 11b: Full Azure Blob Storage gateway backend implemented using `reqwest` + Azure Blob REST API
- Previous: `cargo test` -- all 174 pass (Stage 11a), expecting ~194 with 20 new Azure tests
- Previous: `./run_e2e.sh` -- **85/86 pass** (1 known test bug)

## What Works
- All S3 operations fully implemented (Stages 1-8)
- SigV4 authentication (header-based + presigned URLs)
- Bucket CRUD, Object CRUD, List/Copy/Batch Delete
- Range requests, Conditional requests, Object ACLs
- Multipart uploads (create, upload part, upload part copy, complete, abort, list)
- Prometheus metrics at /metrics
- OpenAPI/Swagger UI at /docs, /openapi.json
- Crash-only design throughout
- **AWS S3 gateway backend** (Stage 10) -- proxies all operations to upstream AWS S3 bucket
- **GCP Cloud Storage gateway backend** (Stage 11a) -- proxies all operations to upstream GCS bucket
- **Azure Blob Storage gateway backend** (Stage 11b) -- proxies all operations to upstream Azure container

## Storage Backends
- **Local filesystem** (default) -- stores objects on disk, used for E2E testing
- **AWS S3 gateway** -- proxies to upstream AWS S3 bucket via `aws-sdk-s3`
  - Key mapping: `{prefix}{bleepstore_bucket}/{key}` in single upstream bucket
  - Parts: `{prefix}.parts/{upload_id}/{part_number}` as temporary S3 objects
  - Multipart assembly via native AWS multipart upload with server-side copy
  - MD5 computed locally for consistent ETags
- **GCP Cloud Storage gateway** -- proxies to upstream GCS bucket via `reqwest` + GCS JSON API
  - Key mapping: `{prefix}{bleepstore_bucket}/{key}` in single upstream GCS bucket
  - Parts: `{prefix}.parts/{upload_id}/{part_number}` as temporary GCS objects
  - Multipart assembly via GCS compose (max 32 sources per call, chaining for >32 parts)
  - MD5 computed locally for consistent ETags
  - Credentials via ADC (env var, gcloud CLI, GCE metadata server)
  - Server-side copy via GCS rewrite API
- **Azure Blob Storage gateway** (new) -- proxies to upstream Azure container via `reqwest` + Azure Blob REST API
  - Key mapping: `{prefix}{bleepstore_bucket}/{key}` in single upstream Azure container
  - Parts: `{prefix}.parts/{upload_id}/{part_number}` as temporary Azure blobs
  - Multipart assembly via Azure Block Blob API (Put Block + Put Block List)
  - Block IDs: `base64("{upload_id}:{part_number:05}")` for collision avoidance
  - MD5 computed locally for consistent ETags
  - Credentials: AZURE_STORAGE_KEY (Shared Key), AZURE_STORAGE_CONNECTION_STRING, AZURE_STORAGE_SAS_TOKEN
  - Server-side copy via Azure Copy Blob API

## E2E Test Results (2026-02-24)
- **85 passed, 1 failed** out of 86 total (with local backend, Stage 11a verified)
- Failed: `test_invalid_access_key` -- hardcoded `http://localhost:9000` in test (known test bug)
- Stage 11b: build/test verification pending

## Known Issues
- `test_invalid_access_key` uses hardcoded `http://localhost:9000` -- test bug, do NOT modify
- AWS SDK crates pinned to specific versions for rustc 1.88 compatibility (MSRV bumped to 1.91 on 2026-02-11)

## Next Steps
- Verify build and tests pass (cargo build, cargo test, ./run_e2e.sh)
- Stage 12a: Raft State Machine & Storage
