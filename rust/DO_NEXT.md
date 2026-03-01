# BleepStore Rust -- Do Next

## Current State: Stage 17-18 COMPLETE (Pluggable Metadata Backends)

All pluggable metadata backends implemented:
- `cargo test` -- 294 pass
- `./run_e2e.sh` -- 86/86 pass

### Metadata Backends Status

| Backend | Description | Status |
|---------|-------------|--------|
| `sqlite` | SQLite file (default) | ✅ Complete |
| `memory` | In-memory hash maps | ✅ Complete |
| `local` | JSONL append-only files | ✅ Complete |
| `dynamodb` | AWS DynamoDB | ✅ Complete |
| `firestore` | GCP Firestore | 🔲 Stub |
| `cosmos` | Azure Cosmos DB | 🔲 Stub |

---

## Phase 1: Complete Cloud Metadata Backends

### Task 1.1: Implement Firestore Metadata Backend

**Goal:** Full GCP Firestore implementation using `gcloud-sdk` crate or HTTP client.

**Files to modify:**
- `src/metadata/firestore.rs` -- Replace stub with full implementation
- `Cargo.toml` -- Add `gcloud-sdk` or use HTTP with `reqwest`

**Implementation notes:**
- Collection/document design with subcollections for parts
- URL-safe base64 encoding for object keys (Firestore doesn't allow `/`)
- All 22 MetadataStore trait methods

**Test requirements:**
- Skipped by default (require `FIRESTORE_EMULATOR_HOST`)
- Reference: Python implementation at `python/src/bleepstore/metadata/firestore.py`

---

### Task 1.2: Implement Cosmos DB Metadata Backend

**Goal:** Full Azure Cosmos DB implementation using `azure_data_cosmos` crate.

**Files to modify:**
- `src/metadata/cosmos.rs` -- Replace stub with full implementation
- `Cargo.toml` -- Add `azure_data_cosmos` crate

**Implementation notes:**
- Single-container with `/type` partition key
- SQL queries with `STARTSWITH()` for prefix matching
- No base64 encoding needed (Cosmos allows `/` in IDs)
- All 22 MetadataStore trait methods

**Test requirements:**
- Skipped by default (require `COSMOS_TEST_ENDPOINT` + `COSMOS_TEST_KEY`)
- Reference: Python implementation at `python/src/bleepstore/metadata/cosmos.py`

---

## Phase 2: S3 Minor Gaps (After Phase 1)

### Task 2.1: Verify/Implement encoding-type=url in ListMultipartUploads

Check if implemented; if not, add support.

**Files to modify:**
- `src/handlers/multipart.rs`

### Task 2.2: Verify/Implement response-* Query Params on GetObject

Check if implemented; if not, add support.

**Files to modify:**
- `src/handlers/object.rs`

---

## Definition of Done

**Phase 1:**
- [ ] Firestore: All 22 MetadataStore methods work
- [ ] Cosmos DB: All 22 MetadataStore methods work
- [ ] Unit tests for each (skipped by default)
- [ ] All E2E tests still pass

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
cd /Users/zardoz/projects/bleepstore/rust
cargo test
./run_e2e.sh
```
