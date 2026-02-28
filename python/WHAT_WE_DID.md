# BleepStore Python -- What We Did

## 2026-03-01: Stage 18 — Cloud Metadata Backends

Implemented full cloud-native metadata stores for AWS, GCP, and Azure.

**Stage 18a: DynamoDB (PR #17)**
- Single-table PK/SK design
- All 22 MetadataStore methods using `aiobotocore`
- Tests with moto mock (skipped by default)

**Stage 18b: Firestore (PR #18)**
- Collection/document design with subcollections for parts
- URL-safe base64 encoding for object keys in document IDs
- All 22 MetadataStore methods using `google-cloud-firestore`
- Tests with Firestore emulator (skipped by default)

**Stage 18c: Cosmos DB (PR #19)**
- Single-container with `/type` partition key
- SQL queries with `STARTSWITH()` for prefix matching
- All 22 MetadataStore methods using `azure-cosmos`
- Tests with Cosmos emulator (skipped by default)

## 2026-02-28: Stage 17 — Pluggable Metadata Backends

Added support for multiple metadata storage backends:

**New backends:**
- `memory`: In-memory hash maps for testing
- `local`: JSONL append-only files with tombstones
- `dynamodb`, `firestore`, `cosmos`: Cloud backend stubs

**Files created:**
- `metadata/memory.py` — MemoryMetadataStore
- `metadata/local.py` — LocalMetadataStore (JSONL files)
- `metadata/dynamodb.py` — DynamoDBMetadataStore (stub)
- `metadata/firestore.py` — FirestoreMetadataStore (stub)
- `metadata/cosmos.py` — CosmosMetadataStore (stub)

**Config updated:**
- `metadata.engine` selects backend
- Per-backend config sections

## 2026-02-28: Stage 16 — S3 API Completeness

Closed remaining S3 API gaps:
- response-* query params on GetObject
- x-amz-copy-source-if-* conditional headers
- encoding-type URL encoding
- Multipart reaping on startup

## 2026-02-25: Cross-Language Storage Identity

Normalized multipart temp directory from `.parts` to `.multipart`

## 2026-02-24: Stage 15 — Performance Optimization

- Streaming PutObject/UploadPart
- SigV4 signing key cache
- SQL batch delete optimization
- Structured logging
- Graceful shutdown

## 2026-02-23: Stages 10-11 — Cloud Storage Gateways

- AWS S3 gateway backend
- GCP Cloud Storage gateway backend
- Azure Blob Storage gateway backend

## 2026-02-23: Stages 6-9 — Auth & Testing

- SigV4 authentication (header + presigned)
- Multipart uploads (core + completion)
- 86/86 E2E tests passing

## 2026-02-22: Stages 1-5 — Core Implementation

- Server bootstrap, OpenAPI, Prometheus
- SQLite metadata store
- Bucket/object CRUD
- List, copy, batch delete
- Range requests, conditionals, ACLs
