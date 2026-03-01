# BleepStore Python -- Status

## Current Stage: Cloud Metadata Backends COMPLETE (Stage 18)

- **Unit tests:** 642/642 pass
- **E2E tests:** 86/86 pass
- **Framework:** FastAPI + Pydantic + uvicorn

## What Works

- Full S3 API (buckets, objects, multipart, presigned URLs, ACLs)
- SigV4 authentication (header + presigned)
- Range requests, conditional requests
- Multipart uploads (create, upload, copy, complete, abort, list)
- Storage backends: local, memory, sqlite, AWS S3, GCP GCS, Azure Blob

## Metadata Backends (Stage 17-18)

| Backend | Status | File | Notes |
|---------|--------|------|-------|
| `sqlite` | ✅ Production | `metadata/sqlite.py` | Default |
| `memory` | ✅ Complete | `metadata/memory.py` | Testing |
| `local` | ✅ Complete | `metadata/local.py` | JSONL files |
| `dynamodb` | ✅ Complete | `metadata/dynamodb.py` | PR #17 |
| `firestore` | ✅ Complete | `metadata/firestore.py` | PR #18 |
| `cosmos` | ✅ Complete | `metadata/cosmos.py` | PR #19 |

## Next Milestone: Stage 19 — Raft Consensus / Clustering

Implement multi-node deployment with Raft-based consensus.

## Future Milestones

- **Stage 20:** Event Queues (Redis, RabbitMQ, Kafka)

## Run Tests

```bash
cd /Users/zardoz/projects/bleepstore/python
uv run pytest tests/ -v
./run_e2e.sh
```
