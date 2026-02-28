# BleepStore Python -- Do Next

## Current State: S3 API + Pluggable Metadata COMPLETE

- **Unit tests:** 642/642 pass
- **E2E tests:** 86/86 pass
- **Metadata backends:** sqlite, memory, local (complete); dynamodb, firestore, cosmos (stubs)

---

## Next: Stage 18 — Cloud Metadata Backends

Implement real cloud-native metadata stores for AWS, GCP, Azure.

### Stage 18a: AWS DynamoDB

**Goal:** Full DynamoDB implementation for AWS-native deployments.

**Files to modify:**
- `metadata/dynamodb.py` — Full implementation using `aioboto3`
- `pyproject.toml` — Add `aioboto3` dependency

**Table design:**
```
PK: BUCKET#name | OBJECT#bucket#key | UPLOAD#id | CRED#access_key
SK: #METADATA | PART#part_number
```

**Definition of done:**
- [ ] All MetadataStore methods implemented
- [ ] Pagination via LastEvaluatedKey
- [ ] Error mapping
- [ ] Unit tests with moto
- [ ] E2E tests pass

---

### Stage 18b: GCP Firestore

**Goal:** Full Firestore implementation for GCP-native deployments.

**Files to modify:**
- `metadata/firestore.py` — Full implementation using `google-cloud-firestore`
- `pyproject.toml` — Add `google-cloud-firestore` dependency

**Collection design:**
```
bleepstore/
  bucket_{name}
  object_{bucket}_{key}
  upload_{upload_id}
    parts/ (subcollection)
      part_{number}
  cred_{access_key}
```

**Definition of done:**
- [ ] All MetadataStore methods implemented
- [ ] Pagination via cursors
- [ ] Transactions for atomic ops
- [ ] Unit tests with emulator
- [ ] E2E tests pass

---

### Stage 18c: Azure Cosmos DB

**Goal:** Full Cosmos DB implementation for Azure-native deployments.

**Files to modify:**
- `metadata/cosmos.py` — Full implementation using `azure-cosmos`
- `pyproject.toml` — Add `azure-cosmos` dependency

**Container design:**
```
Partition Key: /pk (same pattern as DynamoDB)
```

**Definition of done:**
- [ ] All MetadataStore methods implemented
- [ ] Pagination via continuation tokens
- [ ] Unit tests with emulator
- [ ] E2E tests pass

---

## Run Tests

```bash
cd /Users/zardoz/projects/bleepstore/python
uv run pytest tests/ -v
./run_e2e.sh
```

---

## Future

- **Stage 19:** Raft Consensus / Clustering
- **Stage 20:** Event Queues (Redis, RabbitMQ, Kafka)
