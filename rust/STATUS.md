# BleepStore Rust -- Status

## Current Stage: Stage 17/18 COMPLETE (Pluggable Metadata Backends)

- Stage 17: Metadata backend selection (`memory`, `local`, `sqlite`)
- Stage 18: Cloud metadata backends (`dynamodb`, `firestore`, `cosmos`) - stubs implemented
- `cargo test` -- all 294 pass
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
- **Pluggable metadata backends** via `metadata.engine` config:
  - `sqlite` (default): SQLite database with WAL mode
  - `memory`: In-memory hashmaps (non-persistent)
  - `local`: JSONL files with tombstone deletion
  - `dynamodb`: AWS DynamoDB single-table design (full implementation)
  - `firestore`: GCP Firestore (stub for future implementation)
  - `cosmos`: Azure Cosmos DB (stub for future implementation)

## Cross-Language Storage Identity (2026-02-25)
- SQLite storage schema normalized from single TEXT PK to composite PKs: `(bucket, key)` for object_data, `(upload_id, part_number)` for part_data
- Memory snapshot schema normalized to composite PKs to match
- All unit tests pass (294), E2E tests pass (86/86)

## Storage Backends
- **Local filesystem** (default) -- stores objects on disk, used for E2E testing
- **Memory** -- in-memory HashMap-based storage with tokio::sync::RwLock, optional `max_size_bytes` limit, snapshot persistence
- **SQLite** -- object BLOBs stored in the same SQLite database as metadata (`object_data`, `part_data` tables, composite PKs)
- **AWS S3 gateway** -- proxies to upstream AWS S3 bucket via `aws-sdk-s3`
  - Enhanced config: `endpoint_url`, `use_path_style`, `access_key_id`, `secret_access_key`
- **GCP Cloud Storage gateway** -- proxies to upstream GCS bucket via `reqwest` + GCS JSON API
  - Enhanced config: `credentials_file`
- **Azure Blob Storage gateway** -- proxies to upstream Azure container via `reqwest` + Azure Blob REST API
  - Enhanced config: `connection_string`, `use_managed_identity`

## Metadata Backends
- **SQLite** (default): Full SQL database with transactions, WAL mode for crash recovery
- **Memory**: In-memory HashMaps, non-persistent, useful for testing
- **Local**: JSONL files with tombstone-based deletion, optional compaction
- **DynamoDB**: AWS DynamoDB single-table design with PK/SK pattern
- **Firestore**: GCP Firestore (stub)
- **Cosmos DB**: Azure Cosmos DB (stub)

## E2E Test Results (2026-03-01)
- **86 passed, 0 failed** out of 86 total (with local backend + sqlite metadata)

## Known Issues
- AWS SDK crates pinned to specific versions for rustc 1.88 compatibility (MSRV bumped to 1.91 on 2026-02-11)
- Firestore and Cosmos DB backends are stubs (return empty results)

## Next Steps

- **Stage 19:** Full Firestore implementation
- **Stage 20:** Full Cosmos DB implementation
- **Stage 21:** Raft State Machine & Storage
