# Stage 15: Performance Optimization & Production Readiness

**Completed:** 2026-02-24
**Unit tests:** 582/582 pass
**E2E tests:** 86/86 pass

## What Was Implemented

### Phase 1: SigV4 Authenticator Cache Bug Fix
- **Problem:** `server.py` created a new `SigV4Authenticator` per-request, so the `_signing_key_cache` dict was always empty (4 wasted HMAC-SHA256 ops/request)
- **Fix:** Create authenticator once in `lifespan()`, store on `app.state.authenticator`, reuse in auth middleware
- **Files:** `server.py`

### Phase 2: Streaming PutObject & UploadPart
- Added `put_stream()` and `put_part_stream()` to `StorageBackend` protocol
- Local backend: writes chunks to temp file via `os.open`/`os.write`, computes MD5 incrementally, fsync + atomic rename
- Gateway backends (AWS, GCP, Azure): collect stream into bytes and delegate to existing `put()` (acceptable since cloud SDK handles upload)
- Handler changes: `put_object` and `upload_part` use `request.stream()` when body hasn't been pre-consumed by auth
- **Files:** `backend.py`, `local.py`, `aws.py`, `gcp.py`, `azure.py`, `object.py`, `multipart.py`

### Phase 3: SQL Batch Delete Fix
- **Problem:** `delete_objects_meta` ran SELECT+DELETE per key (2N queries for batch of N)
- **Fix:** Single SELECT with IN clause to find existing keys, single DELETE with IN clause
- **Files:** `sqlite.py`

### Phase 4: Structured Logging
- New `logging_config.py` with `JSONFormatter` class and `configure_logging()` function
- Added `--log-level` and `--log-format text|json` CLI arguments
- Added `log_level` and `log_format` to `ServerConfig`
- Per-request structured logging in middleware: method, path, status, duration_ms, request_id
- Skips /metrics and /health to reduce noise
- **Files:** `logging_config.py` (NEW), `cli.py`, `config.py`, `server.py`

### Phase 5: Graceful Shutdown
- Added `--shutdown-timeout` CLI arg (default: 30s)
- Added `shutdown_timeout` to `ServerConfig`
- Passes `timeout_graceful_shutdown=N` and `timeout_keep_alive=5` to `uvicorn.run()`
- **Files:** `cli.py`, `config.py`

### Phase 6: List Objects Query Optimization
- **Problem:** Separate `SELECT 1` query to check truncation after every list operation
- **Fix:** Fetch `max_keys+1` rows (non-delimiter) or `max_keys*3+100` (delimiter), detect truncation by whether consumed rows < total fetched rows
- **Files:** `sqlite.py`

### Phase 7: Startup Time Optimization
- Schema check: `_create_tables()` checks `sqlite_master` for `schema_version` table before running full DDL executescript
- Temp file cleanup: `_clean_temp_files()` now skips hidden directories (except `.parts`)
- **Files:** `sqlite.py`, `local.py`

### Phase 8: Error Handling Hardening
- Local backend: disk-full (errno.ENOSPC) logging in `put()`, `put_stream()`, `copy_object()`
- SQLite metadata: `aiosqlite.OperationalError` catch-and-log in `put_object()`
- Partial write cleanup already handled by existing temp file patterns
- **Files:** `local.py`, `sqlite.py`

### Phase 9: Request Body Size Limits
- Added configurable `max_object_size` to `ServerConfig` (default: 5TB)
- PutObject handler checks Content-Length against max_object_size, raises EntityTooLarge
- **Files:** `object.py`, `config.py`

### Phase 10: Copy Object Streaming
- **Problem:** `copy_object` in local backend read entire source into memory
- **Fix:** Streaming file copy reading source in 64KB chunks, writing to temp file with incremental MD5, atomic rename
- **Files:** `local.py`

## Key Decisions
- Streaming writes check `request._body` to detect if body was pre-consumed by auth middleware; if so, fall back to buffered put (Starlette caches the body)
- Gateway backends use collect-and-delegate for streaming (cloud SDKs manage their own upload buffering)
- Structured logging uses stdlib `logging` with custom JSONFormatter rather than a third-party library

## Files Changed
| File | Changes |
|------|---------|
| `server.py` | SigV4 cache fix, per-request logging, time import |
| `storage/backend.py` | Added put_stream, put_part_stream to protocol |
| `storage/local.py` | Streaming put/part/copy, ENOSPC logging, temp cleanup optimization |
| `storage/aws.py` | put_stream, put_part_stream (collect-delegate) |
| `storage/gcp.py` | put_stream, put_part_stream (collect-delegate) |
| `storage/azure.py` | put_stream, put_part_stream (collect-delegate) |
| `handlers/object.py` | Streaming put, size limit check |
| `handlers/multipart.py` | Streaming upload_part |
| `metadata/sqlite.py` | Batch delete, list optimization, startup skip, error logging |
| `config.py` | log_level, log_format, shutdown_timeout, max_object_size |
| `cli.py` | --log-level, --log-format, --shutdown-timeout, configure_logging |
| `logging_config.py` | NEW: JSONFormatter, configure_logging() |
