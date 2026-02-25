# BleepStore Python -- Do Next

## Current State: Stage 15 COMPLETE + Pluggable Storage Backends — 86/86 E2E Tests Passing

- `uv run pytest tests/ -v` — 582/582 pass
- `./run_e2e.sh` — **86/86 pass**
- Stage 15 complete: streaming I/O, structured logging, graceful shutdown, batch SQL, startup optimization
- Pluggable storage backends complete: memory, sqlite, cloud config enhancements

## Storage Backends Done

Memory and SQLite backends are implemented alongside the existing local filesystem and cloud gateway backends. Test them with:

```bash
cd /Users/zardoz/projects/bleepstore/python
./run_e2e.sh --backend memory
./run_e2e.sh --backend sqlite
./run_e2e.sh              # default: local filesystem
```

## Next: Stage 12 — Raft Consensus / Clustering

### Goal
Implement Raft-based clustering for multi-node BleepStore deployments.

### Pattern to Follow
- See `specs/clustering.md` and `PLAN.md` Stage 12 for full specification
- Build on crash-only design — every startup is recovery
- Write-through via Raft, reads from any replica (eventual consistency)

### Run Tests
```bash
cd /Users/zardoz/projects/bleepstore/python
./run_e2e.sh
```

## Known Issues
- None — all 86 E2E tests pass

## Stage 15 Notes
- Streaming write uses `request.stream()` when body hasn't been pre-consumed by auth
- SigV4 authenticator cache is on `app.state.authenticator` — persist across requests
- JSON logging enabled via `--log-format json`
- Graceful shutdown via `--shutdown-timeout N` (default 30s)
- max_object_size configurable in config (server.max_object_size, default 5TB)
