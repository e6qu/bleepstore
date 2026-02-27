# BleepStore Zig — Agent Instructions

## How This Project Works

This is the **Zig implementation** of BleepStore, an S3-compatible object store.
It is one of four independent implementations (Python, Go, Rust, Zig) that all pass
the same E2E test suite.

**Port assignment:** This project uses port **9013** by default.

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
cd .. && BLEEPSTORE_ENDPOINT=http://localhost:9013 tests/run_tests.sh
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

This implementation is independent. Do not modify files outside `zig/`,
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
cd zig/
zig build
```

### Build
```bash
zig build              # debug
zig build -Doptimize=ReleaseFast  # optimized
```

### Run the Server
```bash
zig build run -- --config ../bleepstore.example.yaml --port 9013

# Or after building:
./zig-out/bin/bleepstore --config ../bleepstore.example.yaml --port 9013
```

### Run Unit Tests
```bash
zig build test
```

### Run E2E Tests (against this project)
```bash
# Use the project-specific E2E runner (starts server, runs tests, stops server)
./run_e2e.sh

# Or manually (server must be running on port 9013):
cd .. && BLEEPSTORE_ENDPOINT=http://localhost:9013 tests/run_tests.sh
```

### Run Specific Test Categories
```bash
cd ..
BLEEPSTORE_ENDPOINT=http://localhost:9013 tests/run_tests.sh -m bucket_ops
BLEEPSTORE_ENDPOINT=http://localhost:9013 tests/run_tests.sh -m object_ops
```

---

## E2E Test Isolation

This project uses **port 9013** to avoid conflicts with other implementations:

| Language | Port |
|----------|------|
| Python | 9010 |
| Go | 9011 |
| Rust | 9012 |
| Zig | 9013 |

The E2E tests use unique bucket names (timestamp-based) so parallel runs
against different ports do not interfere with each other. Logs are written
to `zig/logs/` which is in `.gitignore`.

---

## Key Files

| File | Purpose |
|------|---------|
| `PLAN.md` | Staged implementation plan with Zig-specific details |
| `STATUS.md` | Current status — read this first in any new session |
| `WHAT_WE_DID.md` | History of completed work |
| `DO_NEXT.md` | Next steps — read this to know what to implement |
| `AGENTS.md` | This file — instructions for agents |
| `CLAUDE.md` | Symlink to AGENTS.md (auto-loaded by Claude Code) |
| `tasks/done/` | Completed stage detail files |
| `build.zig` | Build system |
| `src/` | Source code |
| `logs/` | Runtime and test logs (gitignored) |

---

## Architecture Quick Reference

```
zig/
├── build.zig
├── build.zig.zon
├── src/
│   ├── main.zig             # Entry point, CLI args, GPA
│   ├── config.zig           # Config parsing (key=value)
│   ├── server.zig           # HTTP server (tokamak), routing, OpenAPI
│   ├── auth.zig             # SigV4 (std.crypto.auth.hmac)
│   ├── errors.zig           # S3Error enum
│   ├── xml.zig              # XmlWriter helper, render functions
│   ├── metrics.zig          # Hand-rolled Prometheus metrics
│   ├── validation.zig       # S3 input validation functions
│   ├── handlers/
│   │   ├── bucket.zig       # Bucket operation handlers
│   │   ├── object.zig       # Object operation handlers
│   │   └── multipart.zig    # Multipart upload handlers
│   ├── metadata/
│   │   ├── store.zig        # MetadataStore vtable interface
│   │   └── sqlite.zig       # SQLite via @cImport
│   ├── storage/
│   │   ├── backend.zig      # StorageBackend vtable interface
│   │   ├── local.zig        # Local filesystem (std.fs)
│   │   ├── aws.zig          # AWS S3 gateway (std.http.Client)
│   │   ├── gcp.zig          # GCP Cloud Storage gateway
│   │   └── azure.zig        # Azure Blob gateway
│   └── cluster/
│       └── raft.zig         # Custom Raft implementation
```

**Framework:** tokamak (on httpz) — routing, OpenAPI, Swagger UI
**SQLite:** @cImport(@cInclude("sqlite3.h")) — links system libsqlite3
**XML:** Hand-rolled XmlWriter (no XML library)
**Config:** Custom key=value parser (maps to bleepstore.example.yaml keys)
**Memory:** GeneralPurposeAllocator + per-request ArenaAllocator

---

## Zig Version & Tooling

**Zig version:** 0.15.2 (minimum: 0.15.0, set in `build.zig.zon`)
**ZLS version:** 0.15.1

### Language Server — ZLS

[ZLS](https://github.com/zigtools/zls) (Zig Language Server) is the official LSP for Zig.

- **Install:** `brew install zls` (macOS) or download from https://github.com/zigtools/zls/releases
- **Features:** Autocomplete, go-to-definition, hover docs, signature help, diagnostics, semantic tokens, inlay hints, code actions
- **Config:** `~/.config/zls.json` — can set `zig_exe_path`, `enable_semantic_tokens`, etc.
- **Editor support:** VS Code (ziglang.vscode-zig), Neovim (nvim-lspconfig), Emacs, Sublime Text, Helix

### Formatter — `zig fmt`

Built-in, no separate install needed:

```bash
zig fmt src/              # format all .zig files in src/
zig fmt src/server.zig    # format a single file
zig fmt --check src/      # check without modifying (CI mode)
```

### Linter — zlint

[zlint](https://github.com/DonIsaac/zlint) is the primary third-party linter for Zig.

```bash
# Install
brew install zlint
# or: cargo install zlint

# Run
zlint src/                # lint all files
zlint --format json src/  # JSON output for CI
```

### Testing — Built-in

Zig has built-in test blocks (`test "name" { ... }`) compiled via `zig build test`.

```bash
zig build test                        # run all tests
zig test src/metadata/sqlite.zig      # test a single file directly
```

- Use `std.testing.expect`, `std.testing.expectEqual`, `std.testing.expectError`
- `std.testing.allocator` detects memory leaks in tests

### Debugging

Zig produces DWARF debug info in debug builds. Use:

- **LLDB** (recommended on macOS): `lldb ./zig-out/bin/bleepstore`
- **GDB**: `gdb ./zig-out/bin/bleepstore`
- **VS Code:** CodeLLDB extension with launch.json pointing to the binary

### Package Management

Zig uses `build.zig.zon` for dependencies (fetched at build time, content-addressed):

```bash
zig fetch --save <url>    # add a dependency
zig build                 # fetches dependencies automatically
```

No separate package manager binary — `zig build` handles everything.

### Documentation

- **Language Reference (0.15.x):** https://ziglang.org/documentation/0.15.0/
- **Standard Library Docs:** https://ziglang.org/documentation/0.15.0/std/
- **Build System Docs:** https://ziglang.org/learn/build-system/
- **Release Notes:** https://ziglang.org/download/0.15.0/release-notes.html (covers breaking changes from 0.14)

### Key 0.15 Breaking Changes (from 0.13/0.14)

- `build.zig.zon`: `.name` is now an enum literal (`.bleepstore` not `"bleepstore"`), `.fingerprint` required
- `build.zig`: `root_source_file` replaced with `root_module` using `b.createModule(.{...})`
- `CallingConvention.C` → `.c` (lowercase enum literal)
- `std.http.Server` API changes — init signature changed
- `@ptrCast`/`@alignCast` syntax changes in some contexts

---

## Zig-Specific Challenges

1. **No HTTP framework** — routing is manual in `server.zig`
2. **No XML library** — `xml.zig` has a custom `XmlWriter` helper
3. **Manual memory management** — ArenaAllocator per request, defer/errdefer everywhere
4. **C interop for SQLite** — `@cImport` with `@ptrCast`/`@alignCast` for opaque pointers
5. **No YAML parser** — config uses flat `key = value` format
6. **Error unions** — all errors propagated via `!` return types, mapped to S3Error for responses

---

## Spec References

All specs are in `../specs/`:
- `s3-bucket-operations.md`, `s3-object-operations.md`, `s3-multipart-upload.md`
- `s3-authentication.md`, `s3-error-responses.md`, `s3-common-headers.md`
- `storage-backends.md`, `clustering.md`, `metadata-schema.md`
- `crash-only.md` — Crash-only software design rules (mandatory)
- `event-queues.md` — Optional persistent event queues (Redis, RabbitMQ, Kafka)
- `observability-and-openapi.md` — OpenAPI serving, validation, Prometheus metrics
