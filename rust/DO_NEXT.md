# BleepStore Rust -- Do Next

## Current State: Stage 15 COMPLETE + Pluggable Storage Backends COMPLETE

All performance and production hardening changes are verified.
Pluggable storage backends (memory, sqlite, cloud config enhancements) are implemented.
- `cargo test` -- all pass
- `./run_e2e.sh` -- 86/86 pass (local backend)
- `./run_e2e.sh --backend memory` -- test memory backend
- `./run_e2e.sh --backend sqlite` -- test sqlite backend

---

## Next: Stage 17 — Pluggable Metadata Backends

For parity with Python implementation, implement pluggable metadata backends.

### Goal

Support multiple metadata storage backends beyond SQLite.

### Backends to Implement

| Backend | Description | Status |
|---------|-------------|--------|
| `sqlite` | SQLite file (default) | ✅ Exists |
| `memory` | In-memory hash maps | 🔲 New |
| `local` | JSONL append-only files | 🔲 New |
| `dynamodb` | AWS DynamoDB | 🔲 New |
| `firestore` | GCP Firestore | 🔲 New |
| `cosmos` | Azure Cosmos DB | 🔲 New |

### Files to Create/Modify

| File | Work |
|------|------|
| `src/metadata/memory.rs` | In-memory MetadataStore implementation |
| `src/metadata/local.rs` | JSONL file-based MetadataStore |
| `src/metadata/dynamodb.rs` | DynamoDB implementation |
| `src/metadata/firestore.rs` | Firestore implementation |
| `src/metadata/cosmos.rs` | Cosmos DB implementation |
| `src/config.rs` | Add `metadata.engine` selector |

### Configuration

```yaml
metadata:
  engine: "sqlite"  # sqlite | memory | local | dynamodb | firestore | cosmos
  sqlite:
    path: "./data/metadata.db"
  dynamodb:
    table: "bleepstore-metadata"
    region: "us-east-1"
  firestore:
    collection: "bleepstore-metadata"
    project: "my-project"
  cosmos:
    database: "bleepstore"
    container: "metadata"
```

### Definition of Done

- [ ] `MetadataStore` trait defines all 22 methods
- [ ] `MemoryMetadataStore` implemented
- [ ] `LocalMetadataStore` implemented (JSONL files)
- [ ] DynamoDB backend fully implemented
- [ ] Firestore backend fully implemented
- [ ] Cosmos DB backend fully implemented
- [ ] All E2E tests pass with each backend
- [ ] Unit tests for each backend

---

## Future

- **Stage 18:** Cloud Metadata Backends (DynamoDB, Firestore, Cosmos DB)
- **Stage 19:** Raft Consensus / Clustering
- **Stage 20:** Event Queues (Redis, RabbitMQ, Kafka)

---

## Run Tests
```bash
cd /Users/zardoz/projects/bleepstore/rust
cargo test
./run_e2e.sh
```

## Known Issues
- AWS SDK crates pinned for rustc 1.88 compatibility (see Cargo.toml comments)
