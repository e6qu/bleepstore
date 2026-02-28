# BleepStore Python -- Status

## Current Stage: S3 API + Pluggable Metadata COMPLETE

- **Unit tests:** 642/642 pass
- **E2E tests:** 86/86 pass
- **Framework:** FastAPI + Pydantic + uvicorn

## What Works

- Full S3 API (buckets, objects, multipart, presigned URLs, ACLs)
- SigV4 authentication (header + presigned)
- Range requests, conditional requests
- Multipart uploads (create, upload, copy, complete, abort, list)
- Storage backends: local, memory, sqlite, AWS S3, GCP GCS, Azure Blob

## Metadata Backends (Stage 17)

| Backend | Status | File |
|---------|--------|------|
| `sqlite` | ✅ Production | `metadata/sqlite.py` |
| `memory` | ✅ Complete | `metadata/memory.py` |
| `local` | ✅ Complete | `metadata/local.py` |
| `dynamodb` | 🔲 Stub | `metadata/dynamodb.py` |
| `firestore` | 🔲 Stub | `metadata/firestore.py` |
| `cosmos` | 🔲 Stub | `metadata/cosmos.py` |

## Next Milestone: Stage 18 — Cloud Metadata Backends

Implement real cloud-native metadata stores:

1. **Stage 18a:** AWS DynamoDB
2. **Stage 18b:** GCP Firestore
3. **Stage 18c:** Azure Cosmos DB

## Future Milestones

- **Stage 19:** Raft Consensus / Clustering
- **Stage 20:** Event Queues (Redis, RabbitMQ, Kafka)

## Run Tests

```bash
cd /Users/zardoz/projects/bleepstore/python
uv run pytest tests/ -v
./run_e2e.sh
```
