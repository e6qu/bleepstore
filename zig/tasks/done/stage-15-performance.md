# Stage 15: Performance Optimization & Production Readiness

## Date: 2026-02-24

## Summary
Implemented performance optimization and production hardening features for the Zig
BleepStore implementation, matching Rust/Go/Python Stage 15.

## What Was Implemented

### Phase 1: SigV4 Signing Key & Credential Cache
- `AuthCache` struct in `auth.zig` with thread-safe `std.Thread.Mutex`
- Signing key cache: 24h TTL, stack-based `[32]u8` values (no allocation)
- Credential cache: 60s TTL, owned string copies, `CredSnapshot` return type
- Cache key format: `"{secret}\x00{date}\x00{region}\x00{service}"` (stack buffer)
- Evict-all strategy on overflow (max 1000 entries)
- `verifyHeaderAuth`/`verifyPresignedAuth` accept optional precomputed signing key
- `authenticateRequest` checks caches before DB/HMAC, populates on miss

### Phase 2: Batch DeleteObjects SQL
- `deleteObjectsMeta` rewritten to use `DELETE ... WHERE key IN (?2, ?3, ...)`
- Batch size 998 (SQLite 999-param limit minus 1 for bucket)
- Dynamic SQL via `std.ArrayList` with null terminator
- `deleteObjects` handler calls batch method, loops only for storage file deletion

### Phase 3: Structured Logging
- `pub const std_options` overrides default log function and sets compile-time max to `.debug`
- `customLogFn` checks `runtime_log_level` for runtime filtering
- JSON format: `{"level":"info","scope":"default","ts":epoch,"msg":"..."}`
- Text format: delegates to `std.log.defaultLog`
- `LoggingConfig` struct in config.zig with `level` and `format` fields
- CLI args: `--log-level`, `--log-format`

### Phase 4: Production Config
- `shutdown_timeout` (default 30s) and `max_object_size` (default 5 GiB) in `ServerConfig`
- Shutdown watchdog thread: polls `shutdown_requested`, sleeps timeout, hard exit
- Max object size check in `putObject` and `uploadPart` handlers (EntityTooLarge)
- `global_max_object_size` global in server.zig, set from config
- CLI args: `--shutdown-timeout`, `--max-object-size`

## Files Changed
- `src/auth.zig` -- AuthCache struct, precomputed_signing_key param, pub parsePresignedParams
- `src/server.zig` -- global_auth_cache, global_max_object_size, cache integration in authenticateRequest
- `src/metadata/sqlite.zig` -- Batch deleteObjectsMeta with SQL IN clause
- `src/handlers/object.zig` -- Batch deleteObjects handler, max object size check
- `src/handlers/multipart.zig` -- Max object size check in uploadPart
- `src/config.zig` -- LoggingConfig, shutdown_timeout, max_object_size, new config keys
- `src/main.zig` -- std_options override, customLogFn, new CLI args, auth cache init, watchdog thread

## Key Decisions
- Cache uses evict-all strategy (simple, avoids LRU complexity for 1000 entries)
- Credential cache returns secret_key duped to caller's arena allocator
- Signing key cache uses stack buffer for cache key (no allocation)
- `std.Thread.sleep` (not `std.time.sleep`) in Zig 0.15
- Shutdown watchdog is a detached thread (no join needed)
- JSON log escapes special chars for valid JSON output

## Test Results
- `zig build test` -- 160/160 pass, 0 leaks
- `./run_e2e.sh` -- 86/86 pass
