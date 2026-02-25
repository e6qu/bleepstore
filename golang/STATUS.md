# BleepStore Go -- Status

## Current Stage: Stage 15 COMPLETE (Performance Optimization & Production Readiness) -- 86/86 E2E Tests Passing

All E2E tests pass (the former `test_missing_content_length` failure has been resolved in the E2E suite).

- `go test -count=1 -race ./...` -- all unit tests pass (274 total)
- `./run_e2e.sh` -- **86/86 pass**

## What Was Added in Stage 15

### Phase 1: SigV4 Signing Key & Credential Cache
- `sync.RWMutex`-protected caches in `SigV4Verifier` for signing keys (24h TTL) and credentials (60s TTL)
- Avoids redundant HMAC-SHA256 chain (4 ops) on every request (key only changes daily)
- Avoids DB query per request for credential lookup
- Max 1000 entries per cache; full clear on overflow

### Phase 2: Batch DeleteObjects SQL
- `DeleteObjectsMeta()` now uses `DELETE ... WHERE bucket=? AND key IN (?,?,...)` instead of per-key DELETE
- Batch size 998 (SQLite's 999-variable limit minus 1 for bucket param)
- Handler collects all keys, calls batch delete once, then loops only for storage file deletion

### Phase 3: Structured Logging with `log/slog`
- New `internal/logging/logging.go` package with `Setup(level, format, writer)` function
- `LoggingConfig{Level, Format}` added to config
- `--log-level` and `--log-format` CLI flags
- All `log.Printf` calls across all files converted to `slog.Info/Warn/Error/Debug` with structured key-value pairs
- Files converted: main.go, object.go, multipart.go, bucket.go, aws.go, gcp.go, azure.go, raft.go

### Phase 4: Production Config (Shutdown Timeout, Max Object Size)
- `ServerConfig` gains `ShutdownTimeout int` (default 30s) and `MaxObjectSize int64` (default 5 GiB)
- `--shutdown-timeout` and `--max-object-size` CLI flags
- PutObject enforces max object size via Content-Length check (returns `EntityTooLarge`)
- UploadPart also enforces max object size per part

## What Works
- All S3 operations fully implemented (Stages 1-8)
- SigV4 authentication (header-based + presigned URLs) with signing key cache
- Bucket CRUD, Object CRUD, List/Copy/Batch Delete (batch SQL)
- Range requests, Conditional requests, Object ACLs
- Multipart uploads (create, upload part, upload part copy, complete, abort, list)
- Prometheus metrics at /metrics
- OpenAPI/Swagger UI at /docs, /openapi.json
- Crash-only design throughout
- Structured logging via log/slog (text or JSON format)
- Configurable shutdown timeout and max object size
- **AWS S3 Gateway Backend** -- proxies to upstream AWS S3
- **GCP Cloud Storage Gateway Backend** -- proxies to upstream GCS
- **Azure Blob Storage Gateway Backend** -- proxies to upstream Azure container

## Cross-Language Storage Identity (2026-02-25)
- Part file naming normalized from zero-padded `%05d` to plain `%d` (matches Python/Rust/Zig)
- All unit tests pass (274), E2E tests pass (86/86)

## Storage Backends

- **Local filesystem** (default) -- objects stored in `data/` directory
- **Memory backend** -- in-memory map-based storage with `sync.RWMutex`, configurable `max_size_bytes` limit, SQLite snapshot persistence, graceful `Close()`
- **SQLite backend** -- object BLOBs stored in the same SQLite database as metadata (`object_data`, `part_data` tables), uses `modernc.org/sqlite` with WAL mode
- **AWS S3 Gateway** -- proxies to upstream AWS S3. Enhanced config: `endpoint_url`, `use_path_style`, `access_key_id`, `secret_access_key`
- **GCP Cloud Storage Gateway** -- proxies to upstream GCS. Enhanced config: `credentials_file`
- **Azure Blob Storage Gateway** -- proxies to upstream Azure container. Enhanced config: `connection_string`, `use_managed_identity`

## Known Issues
- None -- all 86 E2E tests pass

## Next Steps
- Stage 12: Raft Consensus / Cluster Mode

## Unit + Integration Test Count: 274 tests passing
