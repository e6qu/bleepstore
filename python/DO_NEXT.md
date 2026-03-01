# BleepStore Python -- Do Next

## Current State: Cloud Metadata Backends COMPLETE (Stage 18)

- **Unit tests:** 642/642 pass
- **E2E tests:** 86/86 pass
- **Metadata backends:** sqlite, memory, local, dynamodb, firestore, cosmos (all complete)

### Metadata Backends (Reference Implementation)

| Backend | Status | File |
|---------|--------|------|
| `sqlite` | ✅ Complete | `metadata/sqlite.py` |
| `memory` | ✅ Complete | `metadata/memory.py` |
| `local` | ✅ Complete | `metadata/local.py` |
| `dynamodb` | ✅ Complete | `metadata/dynamodb.py` |
| `firestore` | ✅ Complete | `metadata/firestore.py` |
| `cosmos` | ✅ Complete | `metadata/cosmos.py` |

**Note:** Python is the reference implementation for all cloud metadata backends. Other implementations (Go, Rust, Zig) should follow these patterns.

---

## Phase 1 & 2: COMPLETE

Python has completed all cloud metadata backends and S3 minor gaps. See other implementations for remaining work.

---

## Next: Stage 19 — Raft Consensus / Clustering

Implement multi-node deployment with Raft-based consensus.

### Files to create

| File | Purpose |
|------|---------|
| `cluster/raft.py` | Raft state machine implementation |
| `cluster/transport.py` | HTTP RPC transport |
| `cluster/state_machine.py` | Apply Raft log entries to metadata |
| `metadata/raft_store.py` | Raft-aware metadata wrapper |

### Reference

- `../specs/clustering.md` — Clustering specification

---

## Future

- **Stage 20:** Event Queues (Redis, RabbitMQ, Kafka)

---

## Run Tests

```bash
cd /Users/zardoz/projects/bleepstore/python
uv run pytest tests/ -v
./run_e2e.sh
```
