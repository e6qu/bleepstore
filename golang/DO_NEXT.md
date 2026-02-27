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

## Remaining Minor Items

### encoding-type=url in ListMultipartUploads (LOW priority)

**Goal:** Support `encoding-type=url` query parameter for ListMultipartUploads.

**Files to modify:**
- `internal/handlers/multipart.go` -- Parse `encoding-type` param in `ListMultipartUploads`

---

## Next: Stage 17 (Event Queues) or Stage 12 (Raft Clustering)

- Stage 17a-c: Event Infrastructure (Redis, RabbitMQ, Kafka)
- Stage 12a-b, 13a-b, 14: Raft Consensus / Cluster Mode
