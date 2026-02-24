# BleepStore Rust -- Do Next

## Current State: Stage 11b COMPLETE (Azure Gateway Backend) -- BUILD VERIFICATION PENDING

Implementation is complete but needs build/test verification.

## Immediate: Verify Build and Tests

```bash
cd /Users/zardoz/projects/bleepstore/rust

# 1. Build
cargo build

# 2. Run unit tests (expect ~194 total: 174 existing + 20 new Azure tests)
cargo test

# 3. Run E2E tests (expect 85/86 pass, same as Stage 11a -- no regression)
./run_e2e.sh
```

If any compilation issues, fix them. The azure.rs implementation uses no new Cargo.toml dependencies -- all imports (`reqwest`, `base64`, `hmac`, `sha2`, `md5`, `hex`, `percent-encoding`, `httpdate`) are already in deps.

## After Verification: Update Status Files

Once tests pass, update STATUS.md to confirm "VERIFIED" and update WHAT_WE_DID.md to remove "PENDING" note.

## Next: Stage 12a -- Raft State Machine & Storage

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
- `test_invalid_access_key` has hardcoded `endpoint_url="http://localhost:9000"` (test bug, per CLAUDE.md rule 6)
- AWS SDK crates pinned for rustc 1.88 compatibility (see Cargo.toml comments)
