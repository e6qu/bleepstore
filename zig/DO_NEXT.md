# BleepStore Zig -- Do Next

## Current State: Pluggable Storage Backends COMPLETE

Stage 15 (Performance Optimization) plus pluggable storage backends (memory, sqlite, cloud
config enhancements) are done. Memory and SQLite backends are fully implemented with unit
tests. Cloud backends (AWS, GCP, Azure) now support enhanced configuration options.

- `zig build` -- clean
- `zig build test` -- pass, 0 leaks
- Python E2E -- **86/86 pass**
- `./run_e2e.sh --backend memory` -- test with in-memory storage backend
- `./run_e2e.sh --backend sqlite` -- test with SQLite storage backend

## Next: Stage 12a -- Raft State Machine & Storage

### Goal
Implement the core Raft state machine, log entry types, and persistent storage.
No networking yet -- all testing is in-process.

### Implementation Scope (per PLAN.md)
1. **Raft state machine** -- Follower/Candidate/Leader state transitions, RequestVote handler, AppendEntries handler
2. **Log entry types** -- Tagged union for all metadata operations (CreateBucket, DeleteBucket, PutObjectMeta, etc.) with JSON serialization
3. **Persistent log storage** -- SQLite `raft_log` table for log entries, persistent `currentTerm` and `votedFor`
4. **Log operations** -- Append, truncate, read range, get last index/term

### Reference
- Current Raft scaffold: `src/cluster/raft.zig` (basic state machine with 3 tests)
- Python Raft implementation (if exists): reference for log entry types
- Zig 0.15.2 `std.json` for serialization
- SQLite integration via existing `@cImport` pattern in `src/metadata/sqlite.zig`

### Run Tests
```bash
cd /Users/zardoz/projects/bleepstore/zig
zig build test         # unit tests
zig build e2e          # Zig E2E tests
./run_e2e.sh           # Python E2E tests (default local backend)
./run_e2e.sh --backend memory   # E2E with memory backend
./run_e2e.sh --backend sqlite   # E2E with SQLite backend
```

## Known Issues
- `test_invalid_access_key` has hardcoded `endpoint_url="http://localhost:9000"` (test bug, per CLAUDE.md rule 6)
- PutBucketAcl/PutObjectAcl with XML body not fully parsed (canned ACL via header works)
