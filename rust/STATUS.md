# BleepStore Rust -- Status

## Current Stage: Stage 15 COMPLETE (Performance Optimization & Production Readiness)

- Stage 15: Auth caching, batch SQL, structured logging, production config
- `cargo test` -- all 194 pass
- `./run_e2e.sh` -- **86/86 pass**

## What Works
- All S3 operations fully implemented (Stages 1-8)
- SigV4 authentication (header-based + presigned URLs)
- **SigV4 signing key cache** (24h TTL) and **credential cache** (60s TTL)
- Bucket CRUD, Object CRUD, List/Copy/Batch Delete
- **Batch DeleteObjects** using single SQL DELETE...IN statement (998 keys per batch)
- Range requests, Conditional requests, Object ACLs
- Multipart uploads (create, upload part, upload part copy, complete, abort, list)
- Prometheus metrics at /metrics
- OpenAPI/Swagger UI at /docs, /openapi.json
- Crash-only design throughout
- **AWS S3 gateway backend** (Stage 10)
- **GCP Cloud Storage gateway backend** (Stage 11a)
- **Azure Blob Storage gateway backend** (Stage 11b)
- **Structured logging** with `--log-level` and `--log-format` CLI flags (text/json)
- **Production config**: `--shutdown-timeout` (default 30s), `--max-object-size` (default 5 GiB)
- **Max object size enforcement** in PutObject and UploadPart handlers

## Storage Backends
- **Local filesystem** (default) -- stores objects on disk, used for E2E testing
- **Memory** -- in-memory HashMap-based storage with tokio::sync::RwLock, optional `max_size_bytes` limit, snapshot persistence
- **SQLite** -- object BLOBs stored in the same SQLite database as metadata (`object_data`, `part_data` tables)
- **AWS S3 gateway** -- proxies to upstream AWS S3 bucket via `aws-sdk-s3`
  - Enhanced config: `endpoint_url`, `use_path_style`, `access_key_id`, `secret_access_key`
- **GCP Cloud Storage gateway** -- proxies to upstream GCS bucket via `reqwest` + GCS JSON API
  - Enhanced config: `credentials_file`
- **Azure Blob Storage gateway** -- proxies to upstream Azure container via `reqwest` + Azure Blob REST API
  - Enhanced config: `connection_string`, `use_managed_identity`

## E2E Test Results (2026-02-24)
- **86 passed, 0 failed** out of 86 total (with local backend)

## Known Issues
- AWS SDK crates pinned to specific versions for rustc 1.88 compatibility (MSRV bumped to 1.91 on 2026-02-11)

## Next Steps
- Stage 12: Raft State Machine & Storage
