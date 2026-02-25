# BleepStore Python -- Status

## Current Stage: Stage 15 COMPLETE (Performance Optimization & Production Readiness) — 86/86 E2E Tests Passing

- `uv run pytest tests/ -v` — 582/582 pass
- `./run_e2e.sh` — **86/86 pass**
- Stage 15: Performance optimization and production hardening complete

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

## Stage 15 Improvements
- **SigV4 authenticator cache fix**: Authenticator created once in lifespan, signing key cache persists across requests (eliminates 4 HMAC-SHA256 ops per request after first of the day)
- **Streaming PutObject/UploadPart**: `put_stream()` and `put_part_stream()` methods on all backends; request body streamed directly to disk (memory O(chunk_size) not O(object_size))
- **SQL batch delete**: `delete_objects_meta` reduced from 2N queries to 2 queries for any batch size
- **Structured logging**: `--log-level` and `--log-format text|json` CLI flags; JSONFormatter for production; per-request structured logging (method, path, status, duration, request_id)
- **Graceful shutdown**: `--shutdown-timeout` CLI flag (default: 30s); `timeout_graceful_shutdown` and `timeout_keep_alive=5` passed to uvicorn
- **List objects query optimization**: Fetch max_keys+1 rows, detect truncation from extra row — eliminates separate SELECT query
- **Startup optimization**: Schema existence check in sqlite_master skips DDL on warm starts; temp file cleanup skips hidden directories
- **Error handling hardening**: Disk-full (ENOSPC) logging in put/put_stream/copy_object; aiosqlite OperationalError logging in put_object
- **Request body size limits**: Configurable max_object_size (default: 5TB), early rejection via Content-Length check
- **Copy object streaming**: Streaming file copy with incremental MD5 (memory O(chunk_size) not O(object_size))

## E2E Test Results (2026-02-24)
- **86 passed, 0 failed** out of 86 total
- All bucket, object, multipart, presigned, ACL, and error tests pass

## Known Test Issues
- None — all 86 E2E tests pass

## Storage Backends
- **Local filesystem** (`storage/local.py`): Default backend, stores objects as files on disk
- **Memory** (`storage/memory.py`): In-memory dict-based storage with optional SQLite snapshot persistence. Supports `max_size_bytes` limit and `persistence: "none"|"snapshot"` modes.
- **SQLite** (`storage/sqlite.py`): Object BLOBs stored in the same SQLite database as metadata. Tables: `object_data`, `part_data`. Uses aiosqlite with WAL mode.
- **AWS S3 gateway** (`storage/aws.py`): Proxies to upstream AWS S3
- **GCP Cloud Storage gateway** (`storage/gcp.py`): Proxies to upstream GCS
- **Azure Blob Storage gateway** (`storage/azure.py`): Proxies to upstream Azure container

Cloud backends now support enhanced configuration:
- **AWS**: `endpoint_url`, `use_path_style`, `access_key_id`, `secret_access_key`
- **GCP**: `credentials_file`
- **Azure**: `connection_string`, `use_managed_identity`

## Next Steps
- Stage 12: Raft Consensus / Clustering
