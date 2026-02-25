# BleepStore Rust -- Do Next

## Current State: Stage 15 COMPLETE + Pluggable Storage Backends COMPLETE

All performance and production hardening changes are verified.
Pluggable storage backends (memory, sqlite, cloud config enhancements) are implemented.
- `cargo test` -- all pass
- `./run_e2e.sh` -- 86/86 pass (local backend)
- `./run_e2e.sh --backend memory` -- test memory backend
- `./run_e2e.sh --backend sqlite` -- test sqlite backend

## Next: Stage 12 -- Raft State Machine & Storage

### Goal
Implement the core Raft state machine, log entry types, and persistent storage. The state machine handles state transitions and log management in isolation (no networking yet).

### Implementation Scope (per PLAN.md)
1. **Raft node state machine** -- Three states (Follower, Candidate, Leader), state transitions, persistent state
2. **Log entry types** -- Define enum for all metadata operations (CreateBucket, DeleteBucket, PutObjectMeta, etc.)
3. **Persistent storage for Raft state** -- Separate SQLite database for Raft log/metadata
4. **Leader election logic** (state machine only, no networking)
5. **Log replication logic** (state machine only, no networking)

### Key Crates
- `openraft` -- mature Raft implementation for Rust
- Implement: `RaftStorage`, `RaftNetworkFactory`, `RaftTypeConfig`

### Run Tests
```bash
cd /Users/zardoz/projects/bleepstore/rust
cargo test
./run_e2e.sh
```

## Known Issues
- AWS SDK crates pinned for rustc 1.88 compatibility (see Cargo.toml comments)
