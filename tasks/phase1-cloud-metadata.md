# Phase 1: Complete Cloud Metadata Backends

**Status:** IN PROGRESS
**Created:** 2026-03-01
**Prerequisites:** Stage 17-18 COMPLETE (pluggable metadata backends)

---

## Objective

Ensure all 4 implementations have full cloud metadata parity.

---

## Current Status

| Implementation | DynamoDB | Firestore | Cosmos DB |
|----------------|----------|-----------|-----------|
| **Python** | ✅ Complete | ✅ Complete | ✅ Complete |
| **Go** | ✅ Complete | ✅ Complete | ✅ Complete |
| **Rust** | ✅ Complete | 🔲 Stub | 🔲 Stub |
| **Zig** | 🔲 Stub | ✅ Complete | ✅ Complete |

---

## Remaining Tasks

### Task 1.1: Rust Firestore Metadata Backend

**Priority:** High
**Effort:** Medium (~4-6 hours)

**Files to modify:**
- `rust/src/metadata/firestore.rs` -- Replace stub with full implementation
- `rust/Cargo.toml` -- Add `gcloud-sdk` crate or HTTP client

**Implementation notes:**
- Collection/document design with subcollections for parts
- URL-safe base64 encoding for object keys (Firestore doesn't allow `/`)
- All 22 MetadataStore trait methods

**Test requirements:**
- Skipped by default (require `FIRESTORE_EMULATOR_HOST`)
- Reference: `python/src/bleepstore/metadata/firestore.py`

**Acceptance criteria:**
- [ ] All 22 MetadataStore methods work with real Firestore
- [ ] Unit tests pass (skipped without emulator)
- [ ] E2E tests still pass

---

### Task 1.2: Rust Cosmos DB Metadata Backend

**Priority:** High
**Effort:** Medium (~4-6 hours)

**Files to modify:**
- `rust/src/metadata/cosmos.rs` -- Replace stub with full implementation
- `rust/Cargo.toml` -- Add `azure_data_cosmos` crate

**Implementation notes:**
- Single-container with `/type` partition key
- SQL queries with `STARTSWITH()` for prefix matching
- No base64 encoding needed (Cosmos allows `/` in IDs)
- All 22 MetadataStore trait methods

**Test requirements:**
- Skipped by default (require `COSMOS_TEST_ENDPOINT` + `COSMOS_TEST_KEY`)
- Reference: `python/src/bleepstore/metadata/cosmos.py`

**Acceptance criteria:**
- [ ] All 22 MetadataStore methods work with real Cosmos DB
- [ ] Unit tests pass (skipped without emulator)
- [ ] E2E tests still pass

---

### Task 1.3: Zig DynamoDB Metadata Backend

**Priority:** High
**Effort:** Medium (~4-6 hours)

**Files to modify:**
- `zig/src/metadata/dynamodb.zig` -- Replace stub with full implementation

**Implementation notes:**
- Single-table PK/SK design (same as Python/Go/Rust)
- HTTP client with AWS SigV4 signing (reuse existing auth.zig functions)
- All 22 MetadataStore vtable methods

**Test requirements:**
- Skipped by default (require `DYNAMODB_TEST_ENDPOINT`)
- Reference: `python/src/bleepstore/metadata/dynamodb.py`

**Note:** Zig has no native AWS SDK. Must use HTTP client with SigV4 signing.

**Acceptance criteria:**
- [ ] All 22 MetadataStore methods work with real DynamoDB
- [ ] Unit tests pass (skipped without endpoint)
- [ ] E2E tests still pass

---

## Definition of Done

- [ ] Rust Firestore: Full implementation
- [ ] Rust Cosmos DB: Full implementation
- [ ] Zig DynamoDB: Full implementation
- [ ] All implementations have all 6 metadata backends complete
- [ ] All E2E tests pass for each implementation
