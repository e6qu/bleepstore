# BleepStore Rust -- Do Next

## Current State: Stage 17-18 COMPLETE (Pluggable Metadata Backends)

All pluggable metadata backends implemented:
- `cargo test` -- 294 pass
- `./run_e2e.sh` -- 105/105 pass (local backend + sqlite metadata)

### Completed Backends

| Backend | Description | Status |
|---------|-------------|--------|
| `sqlite` | SQLite file (default) | ✅ Complete |
| `memory` | In-memory hash maps | ✅ Complete |
| `local` | JSONL append-only files | ✅ Complete |
| `dynamodb` | AWS DynamoDB | ✅ Complete |
| `firestore` | GCP Firestore | 🔲 Stub (returns empty) |
| `cosmos` | Azure Cosmos DB | 🔲 Stub (returns empty) |

---

## Next: Stage 19 — Full Cloud Metadata Backends

Complete the Firestore and Cosmos DB implementations.

### Goal

Implement full REST API clients for GCP Firestore and Azure Cosmos DB.

### Files to Complete

| File | Work |
|------|------|
| `src/metadata/firestore.rs` | Full GCP Firestore REST API implementation |
| `src/metadata/cosmos.rs` | Full Azure Cosmos DB REST API implementation |

### Configuration

```yaml
metadata:
  engine: "dynamodb"  # sqlite | memory | local | dynamodb | firestore | cosmos
  dynamodb:
    table_prefix: "bleepstore"
    region: "us-east-1"
    endpoint_url: ""  # For local DynamoDB testing
  firestore:
    collection_prefix: "bleepstore"
    project_id: "my-project"
    credentials_file: "/path/to/service-account.json"
  cosmos:
    database: "bleepstore"
    container_prefix: "metadata"
    endpoint: "https://myaccount.documents.azure.com:443/"
    connection_string: ""  # Alternative to endpoint
```

### Definition of Done

- [ ] Firestore backend fully implemented with REST API
- [ ] Cosmos DB backend fully implemented with REST API
- [ ] Authentication via service account / managed identity
- [ ] All E2E tests pass with each backend (when cloud resources available)

---

## Future

- **Stage 20:** Raft Consensus / Clustering
- **Stage 21:** Event Queues (Redis, RabbitMQ, Kafka)

---

## Run Tests
```bash
cd /Users/zardoz/projects/bleepstore/rust
cargo test
./run_e2e.sh
```

## Known Issues
- AWS SDK crates pinned for rustc 1.88 compatibility (see Cargo.toml comments)
- Firestore and Cosmos DB backends are stubs (return empty results)
