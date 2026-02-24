# Stage 15: Performance Optimization & Production Readiness

## Date: 2026-02-24

## Summary
Implemented performance optimizations and production hardening for the Go BleepStore implementation. Scope was narrower than Python Stage 15 because Go already streams PUT/GET/CopyObject, uses maxKeys+1 for list truncation, and starts fast (compiled binary).

## What Was Implemented

### Phase 1: SigV4 Signing Key & Credential Cache
- Added `sync.RWMutex`-protected cache maps to `SigV4Verifier`
- `signingKeys` map: keyed by `secretKey\x00dateStr\x00region\x00service`, 24h TTL
- `credCache` map: keyed by accessKeyID, 60s TTL
- Max 1000 entries each; full clear on overflow
- `cachedDeriveSigningKey()` and `cachedGetCredential()` methods
- Updated `VerifyRequest()` and `VerifyPresigned()` to use cached versions

### Phase 2: Batch DeleteObjects SQL
- Rewrote `DeleteObjectsMeta()` to use batch `DELETE ... WHERE bucket=? AND key IN (?,...)`
- Batch size 998 (SQLite 999-variable limit minus 1 for bucket)
- Handler collects all keys → batch metadata delete → loop only for storage file deletion

### Phase 3: Structured Logging with `log/slog`
- New `internal/logging/logging.go` package
- `LoggingConfig{Level, Format}` in config
- `--log-level` and `--log-format` CLI flags
- All `log.Printf` → `slog.Info/Warn/Error/Debug` across 8 files (~86 call sites)

### Phase 4: Production Config
- `ShutdownTimeout int` (default 30s) in ServerConfig
- `MaxObjectSize int64` (default 5 GiB) in ServerConfig
- `--shutdown-timeout` and `--max-object-size` CLI flags
- PutObject and UploadPart enforce max object size

## Files Changed
- `internal/auth/sigv4.go`
- `internal/metadata/sqlite.go`
- `internal/handlers/object.go`
- `internal/handlers/multipart.go`
- `internal/handlers/bucket.go`
- `internal/storage/aws.go`
- `internal/storage/gcp.go`
- `internal/storage/azure.go`
- `internal/cluster/raft.go`
- `internal/config/config.go`
- `internal/server/server.go`
- `cmd/bleepstore/main.go`
- NEW `internal/logging/logging.go`

## Test Results
- Unit tests: 274 passing (with -race)
- E2E tests: 86/86 passing

## Key Decisions
- Signing key cache uses simple map + RWMutex (not sync.Map) for better read-heavy performance
- Credential cache TTL of 60s balances freshness with DB load reduction
- Cache overflow strategy: full clear (simple, avoids LRU complexity)
- Batch delete uses 998-size batches to stay within SQLite's 999-variable limit
- Max object size defaults to 5 GiB (AWS S3 single PUT limit)
