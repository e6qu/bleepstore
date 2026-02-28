# BleepStore Python Implementation Plan

Derived from the [global plan](../PLAN.md).

## Setup

- **Python 3.11+** (required by `pyproject.toml`)
- **Package manager:** `uv`
- **Commands:**
  ```bash
  cd python/
  uv venv .venv && source .venv/bin/activate
  uv pip install -e ".[dev]"
  uv run pytest tests/ -v           # Unit tests
  ./run_e2e.sh                      # E2E tests (86 tests)
  bleepstore --config ../bleepstore.example.yaml --port 9010
  ```

---

## Completed Stages (1-16)

| Stage | Description | Status |
|-------|-------------|--------|
| 1 | Server Bootstrap & Configuration | ✅ |
| 1b | OpenAPI & Prometheus Metrics | ✅ |
| 2 | Metadata Store & SQLite | ✅ |
| 3 | Bucket CRUD | ✅ |
| 4 | Basic Object CRUD | ✅ |
| 5a | List, Copy & Batch Delete | ✅ |
| 5b | Range, Conditional Requests & ACLs | ✅ |
| 6 | AWS Signature V4 | ✅ |
| 7 | Multipart Upload - Core | ✅ |
| 8 | Multipart Upload - Completion | ✅ |
| 9a | Core Integration Testing (86/86 E2E) | ✅ |
| 10 | AWS S3 Gateway Backend | ✅ |
| 11a | GCP Cloud Storage Backend | ✅ |
| 11b | Azure Blob Storage Backend | ✅ |
| 15 | Performance Optimization | ✅ |
| 16 | S3 API Completeness | ✅ |

**Current State:** 86/86 E2E tests pass, 642/642 unit tests pass.

---

## Stage 17: Pluggable Metadata Backends ✅

**Goal:** Support multiple metadata storage backends.

### Completed Backends

| Backend | Description | File |
|---------|-------------|------|
| `sqlite` | SQLite file (default) | `metadata/sqlite.py` |
| `memory` | In-memory hash maps | `metadata/memory.py` |
| `local` | JSONL append-only files | `metadata/local.py` |
| `dynamodb` | AWS DynamoDB | `metadata/dynamodb.py` |
| `firestore` | GCP Firestore | `metadata/firestore.py` |
| `cosmos` | Azure Cosmos DB | `metadata/cosmos.py` |

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

---

## Stage 18: Cloud Metadata Backends ✅

**Goal:** Implement real cloud-native metadata stores.

All three cloud backends are now fully implemented with all 22 `MetadataStore` methods.

| Stage | Backend | Status | PR |
|-------|---------|--------|-----|
| 18a | AWS DynamoDB | ✅ Merged | #17 |
| 18b | GCP Firestore | ✅ Merged | #18 |
| 18c | Azure Cosmos DB | ✅ Merged | #19 |

### Test Requirements

Each backend's tests are skipped by default and require environment variables:

| Backend | Env Vars Required |
|---------|-------------------|
| DynamoDB | `DYNAMODB_TEST_ENDPOINT` |
| Firestore | `FIRESTORE_EMULATOR_HOST` |
| Cosmos DB | `COSMOS_TEST_ENDPOINT` + `COSMOS_TEST_KEY` |

### Stage 18a: AWS DynamoDB Backend ✅

**PR:** #17 (merged)

- Single-table PK/SK design
- All 22 MetadataStore methods implemented
- Tests with moto mock (skipped by default)

### Stage 18b: GCP Firestore Backend ✅

**PR:** #18 (merged)

- Collection/document design with subcollections for parts
- URL-safe base64 encoding for object keys
- Tests with Firestore emulator (skipped by default)

### Stage 18c: Azure Cosmos DB Backend ✅

**PR:** #19 (merged)

- Single-container with `/type` partition key
- SQL queries with `STARTSWITH()` for prefix matching
- Tests with Cosmos emulator (skipped by default)

---

## Stage 19: Raft Consensus / Clustering

**Goal:** Multi-node deployment with Raft-based consensus.

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

## Stage 20: Event Queues

**Goal:** Persistent event notifications for bucket/object changes.

### Backends

- Redis (Streams)
- RabbitMQ
- Kafka

### Reference

- `../specs/event-queues.md` — Event queue specification
