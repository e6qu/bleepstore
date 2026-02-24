# Crash-Only Software Design

## Overview

BleepStore follows the **crash-only software** methodology (Candea & Fox, 2003).
Every implementation must treat crash as the only shutdown mechanism: there is no
separate "clean shutdown" code path. The system is always safe to kill with
`kill -9` at any point, and always recovers correctly on the next startup.

**Paper reference:** Candea, G. & Fox, A. (2003). "Crash-Only Software."
*9th Workshop on Hot Topics in Operating Systems (HotOS IX).*

---

## Core Principles

### 1. Crash = Shutdown

- There is **no clean shutdown procedure**. The only way to stop is to crash.
- A SIGTERM/SIGINT handler is allowed **only** as a performance optimization
  (e.g., to close network listeners sooner), but it must NOT perform any cleanup
  that the startup path doesn't also perform.
- Never rely on shutdown hooks to finalize writes, flush caches, or release
  resources. If the process dies mid-shutdown-hook, data must still be consistent.

### 2. Startup = Recovery

- Every startup is a crash recovery. There is no distinction between "first boot"
  and "restart after crash."
- On startup, the server must:
  1. Open the SQLite database (WAL mode recovers automatically)
  2. Clean up incomplete temporary files (anything in temp directories)
  3. Reap expired multipart uploads / leases
  4. Rebuild any in-memory caches from durable state
  5. Re-initialize the WAL checkpoint if needed
  6. Begin accepting requests
- **No startup flag** like `--recovery-mode`. Every startup IS recovery mode.

### 3. All State in Durable Storage

- The only source of truth is the SQLite database and the object storage files.
- In-memory state is always rebuildable from durable state.
- Never keep state only in memory that would be lost on crash.

---

## Design Rules

### Rule 1: Never Acknowledge Before Commit

- A success response (200/201/204) must NOT be sent until all data is committed
  to durable storage.
- For `PutObject`: data must be fully written to disk (fsync'd) and metadata
  must be committed to SQLite before returning 200.
- For `DeleteObject`: metadata must be deleted from SQLite before returning 204.
- For `CompleteMultipartUpload`: the assembled object must be fsync'd and the
  metadata transaction committed before returning 200.

### Rule 2: Atomic File Writes (Temp-Fsync-Rename)

All object data writes must follow the **temp-fsync-rename** pattern:

```
1. Write to a temporary file in the same filesystem (e.g., data/.tmp/{uuid})
2. fsync() the temporary file
3. fsync() the parent directory (on Linux/macOS)
4. rename() the temporary file to its final path (atomic on POSIX)
5. Commit metadata to SQLite
```

If the process crashes at any point:
- Steps 1-3: Temp file is orphaned. Startup cleanup removes it.
- Step 4: File exists at final path but metadata not committed. Startup reconciliation handles this (orphan file, safe to delete since no metadata references it, or re-derive metadata from the file).
- Step 5: Both file and metadata exist. Consistent state.

**Never** write directly to the final path. A crash mid-write would leave a
corrupted partial file.

### Rule 3: SQLite WAL Mode with Proper Pragmas

```sql
PRAGMA journal_mode = WAL;      -- Write-ahead log for crash safety
PRAGMA synchronous = NORMAL;    -- fsync WAL on checkpoint (good balance of safety/perf)
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;     -- Wait up to 5s for locks
```

WAL mode ensures:
- Readers never block writers and vice versa
- Crash recovery is automatic on next open
- No manual recovery code needed

### Rule 4: Lease-Based Multipart Uploads

- Each multipart upload has a `created_at` / `initiated_at` timestamp.
- Uploads that exceed a configurable TTL (default: 7 days) are considered expired.
- On startup, sweep for expired uploads and clean them up (delete parts from storage, delete metadata records).
- **Never** rely on the client calling `AbortMultipartUpload`. The client may crash too.
- The upload_id serves as a natural idempotency key.

### Rule 5: Idempotent Operations

All operations must be safe to retry:

| Operation | Idempotency mechanism |
|-----------|----------------------|
| `PutObject` | Overwrite semantics — same key always replaces |
| `DeleteObject` | Deleting non-existent key returns 204 (not error) |
| `CreateBucket` | `BucketAlreadyOwnedByYou` returns 200 |
| `DeleteBucket` | Deleting non-existent bucket returns `NoSuchBucket` |
| `CreateMultipartUpload` | Returns new upload_id each time (caller retries get new ID) |
| `UploadPart` | Same part_number overwrites previous (upsert) |
| `CompleteMultipartUpload` | If already completed, return the existing object's ETag |
| `AbortMultipartUpload` | Aborting non-existent upload returns `NoSuchUpload` |

### Rule 6: Database as Index, Storage as Source

- The SQLite database is an **index** of what's in object storage.
- If a file exists on disk but has no metadata row, it's an orphan (safe to delete).
- If a metadata row exists but the file is missing, the object is corrupted
  (return `InternalError` on access, log for operator attention).
- On startup, optionally reconcile storage vs. metadata (configurable, off by default
  for fast startup — can be triggered via admin API).

### Rule 7: No Persistent In-Memory Queues

- Never queue work in memory that hasn't been persisted.
- If a background task needs to happen (e.g., cleanup), record the intent in
  the database first, then process it. If the process crashes, the intent
  survives and is processed on next startup.

### Rule 8: Timeouts Over Notifications

- Use timeouts and polling for distributed coordination, not clean-disconnect
  notifications.
- Health checks should be timeout-based.
- Raft leader election uses timeouts (inherent in Raft protocol).
- Never assume a peer shut down cleanly.

---

## Startup Sequence

Every implementation must follow this startup sequence:

```
1. Load configuration
2. Open SQLite database (WAL auto-recovery happens here)
3. Run schema migrations (CREATE TABLE IF NOT EXISTS)
4. Seed default credentials (idempotent upsert)
5. Clean up temp directory (delete all files in data/.tmp/)
6. Reap expired multipart uploads (delete parts + metadata for expired uploads)
7. [Optional] Reconcile storage vs. metadata
8. Start HTTP listener
9. Begin accepting requests
```

Steps 2-7 are the recovery phase. They run identically whether this is the
first boot or a restart after a crash.

---

## Shutdown Behavior

### SIGTERM/SIGINT Handler (Optional Optimization)

```
1. Stop accepting new connections
2. Wait for in-flight requests to complete (with timeout, e.g., 30s)
3. Exit process (do NOT flush, finalize, or clean up)
```

That's it. No cleanup, no finalization. If the timeout expires, just exit.
The next startup will recover any in-flight state.

### kill -9 (Always Safe)

The system must be safe to `kill -9` at any point. This is the true test
of crash-only design. If your implementation can survive `kill -9` during:

- Mid-PutObject write
- Mid-CompleteMultipartUpload
- Mid-DeleteBucket
- Mid-schema-migration
- Mid-startup-recovery

...then the crash-only implementation is correct.

---

## Anti-Patterns (DO NOT DO)

| Anti-Pattern | Why It's Wrong | Correct Approach |
|-------------|----------------|------------------|
| Shutdown hook that flushes writes | Crash before hook runs = data loss | Write-through, never buffer |
| In-memory write buffer | Lost on crash | Write directly to WAL/disk |
| Lock files for mutual exclusion | Stale lock after crash = deadlock | SQLite WAL handles concurrency; use timeouts |
| "Graceful" shutdown flag in code | Two code paths = bugs | One path: crash recovery on every startup |
| Relying on `atexit()` / `defer` for durability | Process may be killed | Only use for resource cleanup (fd, memory) |
| Writing directly to final file path | Crash mid-write = corrupt file | Temp-fsync-rename pattern |
| Deleting file before deleting metadata | Crash = metadata points to missing file | Delete metadata first, then file. Orphan files are harmless. |
| Acknowledging before commit | Crash after ack = client thinks data is saved | Commit to durable storage, then respond |

---

## Testing Crash-Only Correctness

Each implementation should include unit/integration tests that verify crash-only
behavior:

1. **Kill during PutObject** — restart, verify no partial objects visible
2. **Kill during CompleteMultipartUpload** — restart, verify either old state
   or completed state (never partial)
3. **Kill during startup recovery** — restart again, recovery still works
4. **Temp file cleanup** — create orphan temp files, restart, verify they're
   cleaned up
5. **Expired upload reaping** — create old uploads, restart, verify they're
   reaped

These can be tested by:
- Injecting crashes (kill -9 in a subprocess)
- Creating artificial dirty state and verifying recovery
- Running repeated crash-restart cycles under load
