# BleepStore Go — Agent Instructions

## How This Project Works

This is the **Go implementation** of BleepStore, an S3-compatible object store.
It is one of four independent implementations (Python, Go, Rust, Zig) that all pass
the same E2E test suite.

**Port assignment:** This project uses port **9011** by default.

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
cd .. && BLEEPSTORE_ENDPOINT=http://localhost:9011 tests/run_tests.sh
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

This implementation is independent. Do not modify files outside `golang/`,
`tests/`, or `specs/`. The E2E tests in `tests/e2e/` are shared — if you
find a test bug, note it in `STATUS.md` but don't fix it here.

### 7. Git Workflow

When working with git, always follow these rules:

- **Creating a branch:** Always branch from `origin/main`, not local main:
  ```bash
  git fetch origin
  git checkout -b feat/my-feature origin/main
  ```

- **Creating a PR:** Before creating a PR, always rebase your branch on `origin/main`:
  ```bash
  git fetch origin
  git rebase origin/main
  git push --force-with-lease origin feat/my-feature
  gh pr create ...
  ```

- **Switching back to main:** Always sync local main with `origin/main`:
  ```bash
  git checkout main
  git pull origin main
  ```

- **Never use `git pull` on feature branches** — use `git rebase origin/main` instead.

---

## Build & Run

### Setup
```bash
cd golang/
go mod download
```

### Build
```bash
go build -o bleepstore ./cmd/bleepstore
```

### Run the Server
```bash
./bleepstore --config ../bleepstore.example.yaml --port 9011

# Or directly with go run
go run ./cmd/bleepstore --config ../bleepstore.example.yaml --port 9011
```

### Run Unit Tests
```bash
go test ./... -v
go test ./... -v -race   # with race detector
```

### Run E2E Tests (against this project)
```bash
# Use the project-specific E2E runner (starts server, runs tests, stops server)
./run_e2e.sh

# Or manually (server must be running on port 9011):
cd ..
BLEEPSTORE_ENDPOINT=http://localhost:9011 tests/run_tests.sh
```

### Run Specific Test Categories
```bash
cd ..
BLEEPSTORE_ENDPOINT=http://localhost:9011 tests/run_tests.sh -m bucket_ops
BLEEPSTORE_ENDPOINT=http://localhost:9011 tests/run_tests.sh -m object_ops
```

---

## E2E Test Isolation

This project uses **port 9011** to avoid conflicts with other implementations:

| Language | Port |
|----------|------|
| Python | 9010 |
| Go | 9011 |
| Rust | 9012 |
| Zig | 9013 |

The E2E tests use unique bucket names (timestamp-based) so parallel runs
against different ports do not interfere with each other. Logs are written
to `golang/logs/` which is in `.gitignore`.

---

## Key Files

| File | Purpose |
|------|---------|
| `PLAN.md` | Staged implementation plan with Go-specific details |
| `STATUS.md` | Current status — read this first in any new session |
| `WHAT_WE_DID.md` | History of completed work |
| `DO_NEXT.md` | Next steps — read this to know what to implement |
| `AGENTS.md` | This file — instructions for agents |
| `CLAUDE.md` | Symlink to AGENTS.md (auto-loaded by Claude Code) |
| `tasks/done/` | Completed stage detail files |
| `go.mod` | Module and dependencies |
| `cmd/bleepstore/` | Main entry point |
| `internal/` | All packages |
| `logs/` | Runtime and test logs (gitignored) |

---

## Architecture Quick Reference

```
golang/
├── cmd/bleepstore/main.go    # Entry point
├── internal/
│   ├── server/server.go       # HTTP server, routing (Huma + Chi)
│   ├── server/middleware.go   # Prometheus + common headers middleware
│   ├── auth/sigv4.go          # SigV4 authentication
│   ├── config/config.go       # YAML config loading
│   ├── errors/errors.go       # S3Error types
│   ├── xmlutil/xmlutil.go     # XML rendering (encoding/xml)
│   ├── handlers/
│   │   ├── bucket.go          # Bucket operation handlers
│   │   ├── object.go          # Object operation handlers
│   │   └── multipart.go       # Multipart upload handlers
│   ├── metadata/
│   │   ├── store.go           # MetadataStore interface
│   │   └── sqlite.go          # SQLite implementation
│   ├── metrics/metrics.go     # Custom Prometheus metrics
│   ├── storage/
│   │   ├── backend.go         # StorageBackend interface
│   │   ├── local.go           # Local filesystem
│   │   ├── aws.go             # AWS S3 gateway
│   │   ├── gcp.go             # GCP Cloud Storage gateway
│   │   └── azure.go           # Azure Blob gateway
│   └── cluster/
│       └── raft.go            # Raft consensus (hashicorp/raft)
└── go.mod
```

**Framework:** Huma v2 (+ Chi router adapter), prometheus/client_golang
**SQLite:** modernc.org/sqlite (pure Go, no CGO)
**XML:** encoding/xml stdlib
**Config:** gopkg.in/yaml.v3

---

## Spec References

All specs are in `../specs/`:
- `s3-bucket-operations.md`, `s3-object-operations.md`, `s3-multipart-upload.md`
- `s3-authentication.md`, `s3-error-responses.md`, `s3-common-headers.md`
- `storage-backends.md`, `clustering.md`, `metadata-schema.md`
- `crash-only.md` — Crash-only software design rules (mandatory)
- `event-queues.md` — Optional persistent event queues (Redis, RabbitMQ, Kafka)
- `observability-and-openapi.md` — OpenAPI serving, validation, Prometheus metrics
