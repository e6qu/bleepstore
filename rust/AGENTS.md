# BleepStore Rust — Agent Instructions

## How This Project Works

This is the **Rust implementation** of BleepStore, an S3-compatible object store.
It is one of four independent implementations (Python, Go, Rust, Zig) that all pass
the same E2E test suite.

**Port assignment:** This project uses port **9012** by default.

---

## Implementation Rules

### 1. Proceed One Stage at a Time

Follow the stages in `PLAN.md` strictly in order. Do not skip ahead.
Each stage has a clear "Definition of Done" — complete it before moving on.

### 2. Always Save State

After **every task** (or when context reaches ~80%), update these four files:

| File | Purpose |
|------|---------|
| `PLAN.md` | Mark completed stages with ✅, add task summaries inline |
| `STATUS.md` | Current implementation status: what works, what doesn't, current stage |
| `WHAT_WE_DID.md` | Append a dated entry describing what was accomplished in this session |
| `DO_NEXT.md` | Clear instructions for the next session: what to implement next, blockers, context |

**This is critical.** These files are how context is preserved across sessions.
Update them BEFORE running out of context, not after.

### 3. Record Task Details

When a stage is complete, create a detail file in `tasks/done/`:
- Naming: `tasks/done/stage-NN-description.md` (e.g., `stage-01-server-bootstrap.md`)
- Contents: what was implemented, key decisions made, files changed, issues encountered

### 4. Run Tests After Every Stage

After completing a stage, verify it works:

```bash
# Run the project's own E2E tests
./run_e2e.sh

# Or manually:
cd .. && BLEEPSTORE_ENDPOINT=http://localhost:9012 tests/run_tests.sh
```

### 5. Implement Crash-Only Design

Every component must follow the **crash-only software** methodology (see `../specs/crash-only.md`).
Key rules:

- **No clean shutdown path.** SIGTERM handler may stop accepting connections but must NOT perform cleanup that startup doesn't also do.
- **Every startup = crash recovery.** On startup: open SQLite (WAL auto-recovers), clean temp files in `data/.tmp/`, reap expired multipart uploads, seed credentials.
- **Never acknowledge before commit.** Do not return 200/201/204 until data is fsync'd to disk and metadata is committed to SQLite.
- **Atomic file writes.** Always use the temp-fsync-rename pattern: write to temp file, fsync, rename to final path. Never write directly to the final object path.
- **Idempotent operations.** All operations must be safe to retry (PutObject overwrites, DeleteObject on missing key returns 204, etc.).
- **Database as index.** SQLite is the index of truth. Orphan files on disk (no metadata row) are safe to delete. Missing files (metadata exists, file gone) are errors to log.
- **No in-memory queues for durable work.** If background work is needed, record intent in the database first.

### 6. Do Not Modify Other Projects

This implementation is independent. Do not modify files outside `rust/`,
`tests/`, or `specs/`. The E2E tests in `tests/e2e/` are shared — if you
find a test bug, note it in `STATUS.md` but don't fix it here.

---

## Build & Run

### Setup
```bash
cd rust/
cargo build
```

### Build
```bash
cargo build          # debug
cargo build --release  # optimized
```

### Run the Server
```bash
cargo run -- --config ../bleepstore.example.yaml --bind 0.0.0.0:9012

# Or after building:
./target/debug/bleepstore --config ../bleepstore.example.yaml --bind 0.0.0.0:9012
```

### Run Unit Tests
```bash
cargo test
cargo test -- --nocapture   # see println output
```

### Run E2E Tests (against this project)
```bash
# Use the project-specific E2E runner (starts server, runs tests, stops server)
./run_e2e.sh

# Or manually (server must be running on port 9012):
cd ..
BLEEPSTORE_ENDPOINT=http://localhost:9012 tests/run_tests.sh
```

### Run Specific Test Categories
```bash
cd ..
BLEEPSTORE_ENDPOINT=http://localhost:9012 tests/run_tests.sh -m bucket_ops
BLEEPSTORE_ENDPOINT=http://localhost:9012 tests/run_tests.sh -m object_ops
```

---

## E2E Test Isolation

This project uses **port 9012** to avoid conflicts with other implementations:

| Language | Port |
|----------|------|
| Python | 9010 |
| Go | 9011 |
| Rust | 9012 |
| Zig | 9013 |

The E2E tests use unique bucket names (timestamp-based) so parallel runs
against different ports do not interfere with each other. Logs are written
to `rust/logs/` which is in `.gitignore`.

---

## Key Files

| File | Purpose |
|------|---------|
| `PLAN.md` | Staged implementation plan with Rust-specific details |
| `STATUS.md` | Current status — read this first in any new session |
| `WHAT_WE_DID.md` | History of completed work |
| `DO_NEXT.md` | Next steps — read this to know what to implement |
| `AGENTS.md` | This file — instructions for agents |
| `CLAUDE.md` | Symlink to AGENTS.md (auto-loaded by Claude Code) |
| `tasks/done/` | Completed stage detail files |
| `Cargo.toml` | Package config, dependencies |
| `src/` | Source code |
| `logs/` | Runtime and test logs (gitignored) |

---

## Architecture Quick Reference

```
rust/
├── Cargo.toml
├── src/
│   ├── main.rs              # Entry point (tokio, clap)
│   ├── lib.rs               # Module declarations
│   ├── config.rs            # YAML config (serde_yaml)
│   ├── server.rs            # axum Router, routing
│   ├── auth.rs              # SigV4 authentication
│   ├── errors.rs            # S3Error enum (thiserror, IntoResponse)
│   ├── xml.rs               # XML rendering (quick-xml)
│   ├── metrics.rs          # Prometheus metrics (metrics + prometheus exporter)
│   ├── handlers/
│   │   ├── mod.rs
│   │   ├── bucket.rs        # Bucket operation handlers
│   │   ├── object.rs        # Object operation handlers
│   │   └── multipart.rs     # Multipart upload handlers
│   ├── metadata/
│   │   ├── mod.rs
│   │   ├── store.rs         # MetadataStore trait
│   │   └── sqlite.rs        # rusqlite implementation
│   ├── storage/
│   │   ├── mod.rs
│   │   ├── backend.rs       # StorageBackend trait
│   │   ├── local.rs         # Local filesystem
│   │   ├── aws.rs           # AWS S3 gateway
│   │   ├── gcp.rs           # GCP Cloud Storage gateway
│   │   └── azure.rs         # Azure Blob gateway
│   └── cluster/
│       ├── mod.rs
│       └── raft.rs          # Raft consensus (openraft)
```

**Framework:** axum + tokio + utoipa (OpenAPI) + garde (validation) + metrics-exporter-prometheus
**SQLite:** rusqlite (with bundled feature)
**XML:** quick-xml
**Config:** serde_yaml
**Error handling:** thiserror + anyhow

---

## Spec References

All specs are in `../specs/`:
- `s3-bucket-operations.md`, `s3-object-operations.md`, `s3-multipart-upload.md`
- `s3-authentication.md`, `s3-error-responses.md`, `s3-common-headers.md`
- `storage-backends.md`, `clustering.md`, `metadata-schema.md`
- `crash-only.md` — Crash-only software design rules (mandatory)
- `event-queues.md` — Optional persistent event queues (Redis, RabbitMQ, Kafka)
- `observability-and-openapi.md` — OpenAPI serving, validation, Prometheus metrics
