# Stage 15: Performance Optimization & Production Readiness

**Date:** 2026-02-24
**Status:** COMPLETE

## What Was Implemented

### Phase 1: SigV4 Signing Key & Credential Cache
- `AuthCache` struct in `auth.rs` with two `RwLock<HashMap>` caches:
  - **Signing key cache**: Key = `"{secret}\0{date}\0{region}\0{service}"`, TTL 24h
  - **Credential cache**: Key = access_key_id, TTL 60s
  - Max 1000 entries each; full clear on overflow
- Added `auth_cache` field to `AppState` in `lib.rs`
- Auth middleware in `server.rs` checks caches before DB/HMAC computation
- Inline signature verification avoids redundant `derive_signing_key()` calls

### Phase 2: Batch DeleteObjects SQL
- `delete_objects()` in `sqlite.rs` uses `DELETE FROM objects WHERE bucket = ?1 AND key IN (?2, ...)`
- Batch size 998 (SQLite 999-param limit minus 1 for bucket)
- Handler calls batch delete once, then loops only for storage file deletion

### Phase 3: Structured Logging CLI Flags & JSON Format
- `LoggingConfig { level, format }` in `config.rs` (defaults: info/text)
- `--log-level` and `--log-format` CLI args in `main.rs`
- `tracing-subscriber` JSON feature enabled in `Cargo.toml`
- Registry-based init: text or JSON format based on config
- `RUST_LOG` env var still works (EnvFilter priority)

### Phase 4: Production Config (Shutdown Timeout, Max Object Size)
- `shutdown_timeout` (default 30s) and `max_object_size` (default 5 GiB) in `ServerConfig`
- `--shutdown-timeout` and `--max-object-size` CLI args
- Hard exit after shutdown timeout via spawned background task
- `EntityTooLarge` check in `put_object()` and `upload_part()`

## Files Changed

| File | Changes |
|------|---------|
| `src/auth.rs` | Added `AuthCache` struct, cache types, `find_header_value_pub()` |
| `src/lib.rs` | Added `auth_cache` field to `AppState` |
| `src/main.rs` | CLI args, logging init, cache creation, shutdown timeout |
| `src/server.rs` | Auth middleware uses caches, inline signing |
| `src/config.rs` | `LoggingConfig`, `shutdown_timeout`, `max_object_size` |
| `src/metadata/sqlite.rs` | Batch `DELETE...IN` for `delete_objects()` |
| `src/handlers/object.rs` | Max object size check, batch delete handler |
| `src/handlers/multipart.rs` | Max object size check in `upload_part()` |
| `Cargo.toml` | Added `json` feature to `tracing-subscriber` |

## Verification
- `cargo test` -- 194 unit tests pass
- `./run_e2e.sh` -- 86/86 E2E tests pass
