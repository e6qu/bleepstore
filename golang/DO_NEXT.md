# BleepStore Go -- Do Next

## Current State: Stage 17-18 COMPLETE (Pluggable & Cloud Metadata Backends)

- `go test -count=1 -race ./...` -- all unit tests pass (274+ total)
- `./run_e2e.sh` -- **86/86 pass**

### Metadata Backends Status

| Backend | Description | Status |
|---------|-------------|--------|
| `sqlite` | SQLite file (default) | ✅ Complete |
| `memory` | In-memory hash maps | ✅ Complete |
| `local` | JSONL append-only files | ✅ Complete |
| `dynamodb` | AWS DynamoDB | ✅ Complete |
| `firestore` | GCP Firestore | ✅ Complete |
| `cosmos` | Azure Cosmos DB | ✅ Complete |

---

## Phase 2: S3 Minor Gaps

Go already completed most S3 API features in Stage 16. Remaining gaps:

### Task 2.1: encoding-type=url in ListMultipartUploads

**Current**: Not implemented
**Files to modify:**
- `internal/handlers/multipart.go`

### Task 2.2: response-* Query Params on GetObject

**Current**: Not implemented
**Files to modify:**
- `internal/handlers/object.go`

---

## Definition of Done

**Phase 2:**
- [ ] encoding-type=url works in ListMultipartUploads
- [ ] response-* params override GetObject headers
- [ ] All 86 E2E tests pass

---

## Future

- **Stage 19:** Raft Consensus / Clustering
- **Stage 20:** Event Queues (Redis, RabbitMQ, Kafka)

---

## Run Tests

```bash
cd /Users/zardoz/projects/bleepstore/golang
go test -count=1 -race ./...
./run_e2e.sh
```
