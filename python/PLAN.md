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
| `dynamodb` | AWS DynamoDB (stub) | `metadata/dynamodb.py` |
| `firestore` | GCP Firestore (stub) | `metadata/firestore.py` |
| `cosmos` | Azure Cosmos DB (stub) | `metadata/cosmos.py` |

### Configuration

```yaml
metadata:
  engine: "sqlite"  # sqlite | memory | local | dynamodb | firestore | cosmos
  sqlite:
    path: "./data/metadata.db"
  local:
    root_dir: "./data/metadata"
    compact_on_startup: true
```

---

## Stage 18: Cloud Metadata Backends

**Goal:** Implement real cloud-native metadata stores.

### Stage 18a: AWS DynamoDB Backend

**Goal:** Full DynamoDB implementation for AWS-native deployments.

#### Table Design

```
Table: bleepstore-metadata
PK: pk (STRING) - Entity type + ID
SK: sk (STRING) - Sub-entity or empty
Attributes: All entity fields + type discriminator
```

**Key Patterns:**
| Entity | PK | SK |
|--------|----|----|
| Bucket | `BUCKET#{name}` | `#METADATA` |
| Object | `OBJECT#{bucket}#{key}` | `#METADATA` |
| Upload | `UPLOAD#{upload_id}` | `#METADATA` |
| Part | `UPLOAD#{upload_id}` | `PART#{part_number}` |
| Credential | `CRED#{access_key}` | `#METADATA` |

#### Files to modify

| File | Work |
|------|------|
| `metadata/dynamodb.py` | Full implementation of all `MetadataStore` protocol methods using `aioboto3` |
| `pyproject.toml` | Add `aioboto3` dependency |

#### Key patterns

```python
# Put bucket
await client.put_item(
    TableName=self._table_name,
    Item={
        "pk": f"BUCKET#{name}",
        "sk": "#METADATA",
        "type": "bucket",
        "name": name,
        "region": region,
        "owner_id": owner_id,
        "created_at": created_at,
    }
)

# Get object
resp = await client.get_item(
    TableName=self._table_name,
    Key={"pk": f"OBJECT#{bucket}#{key}", "sk": "#METADATA"}
)

# List objects (query with prefix)
resp = await client.query(
    TableName=self._table_name,
    KeyConditionExpression="pk = :pk AND begins_with(sk, :prefix)",
    ExpressionAttributeValues={":pk": f"OBJECT#{bucket}#", ":prefix": prefix}
)
```

#### Definition of done

- All `MetadataStore` methods implemented
- Pagination via `LastEvaluatedKey`
- Error mapping (ConditionalCheckFailed → BucketAlreadyExists)
- Unit tests with moto mock
- E2E tests pass

---

### Stage 18b: GCP Firestore Backend

**Goal:** Full Firestore implementation for GCP-native deployments.

#### Collection Design

```
Collection: bleepstore (configurable)
Documents:
  - bucket_{name}
  - object_{bucket}_{urlsafe_key}
  - upload_{upload_id}
  - cred_{access_key}
Subcollection: parts (under upload document)
```

#### Files to modify

| File | Work |
|------|------|
| `metadata/firestore.py` | Full implementation using `google-cloud-firestore` async client |
| `pyproject.toml` | Add `google-cloud-firestore` dependency |

#### Key patterns

```python
# Put bucket
doc_ref = client.collection(self._collection).document(f"bucket_{name}")
await doc_ref.set({
    "type": "bucket",
    "name": name,
    "region": region,
    "owner_id": owner_id,
    "created_at": created_at,
})

# List objects
query = client.collection(self._collection) \
    .where("type", "==", "object") \
    .where("bucket", "==", bucket) \
    .where("key", ">=", prefix) \
    .where("key", "<", prefix + "\uf8ff") \
    .limit(max_keys + 1)
docs = query.stream()
```

#### Definition of done

- All `MetadataStore` methods implemented
- Pagination via cursors
- Transactions for atomic operations
- Unit tests with firestore emulator
- E2E tests pass

---

### Stage 18c: Azure Cosmos DB Backend

**Goal:** Full Cosmos DB implementation for Azure-native deployments.

#### Container Design

```
Container: metadata
Partition Key: /pk (same pattern as DynamoDB)
```

#### Files to modify

| File | Work |
|------|------|
| `metadata/cosmos.py` | Full implementation using `azure-cosmos` async client |
| `pyproject.toml` | Add `azure-cosmos` dependency |

#### Key patterns

```python
# Put bucket
container.create_item({
    "id": f"bucket_{name}",
    "pk": f"BUCKET#{name}",
    "type": "bucket",
    "name": name,
    ...
})

# List objects
query = f"SELECT * FROM c WHERE c.pk = @pk AND STARTSWITH(c.key, @prefix)"
items = container.query_items(query, parameters=[...])
```

#### Definition of done

- All `MetadataStore` methods implemented
- Pagination via continuation tokens
- Stored procedures for transactions
- Unit tests with Cosmos emulator
- E2E tests pass

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
