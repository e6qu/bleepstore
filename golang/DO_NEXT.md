# BleepStore Go -- Do Next

## Current State: Stage 16 COMPLETE (S3 API Completeness) -- 86/86 E2E Tests Passing

- `go test -count=1 -race ./...` -- all unit tests pass (274+ total)
- `./run_e2e.sh` -- **86/86 pass**

## Completed in Stage 16

- [x] CopyObject respects `x-amz-copy-source-if-*` headers
- [x] UploadPartCopy respects `x-amz-copy-source-if-*` headers
- [x] Expired multipart uploads cleaned on startup (7-day TTL)
- [x] `encoding-type=url` works for ListObjectsV2 and ListObjects V1
- [x] `RequestTimeout` and `InvalidLocationConstraint` error codes available

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
| `dynamodb` | AWS DynamoDB | 🔲 Stub → Full |
| `firestore` | GCP Firestore | 🔲 Stub → Full |
| `cosmos` | Azure Cosmos DB | 🔲 Stub → Full |

### Files to Create/Modify

| File | Work |
|------|------|
| `internal/metadata/memory.go` | In-memory MetadataStore implementation |
| `internal/metadata/local.go` | JSONL file-based MetadataStore |
| `internal/metadata/dynamodb.go` | DynamoDB implementation |
| `internal/metadata/firestore.go` | Firestore implementation |
| `internal/metadata/cosmos.go` | Cosmos DB implementation |
| `internal/config/config.go` | Add `metadata.engine` selector |

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

- [ ] `MetadataStore` interface defines all 22 methods
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
