# Metadata Schema — SQLite

## Overview

SQLite stores all metadata for buckets, objects, ACLs, and multipart uploads.
In embedded mode, this is a single SQLite file. In cluster mode, each node
maintains a local SQLite replica updated via Raft log application.

## Database Configuration

```sql
PRAGMA journal_mode = WAL;          -- Write-Ahead Logging for concurrent reads
PRAGMA synchronous = NORMAL;        -- Balance durability and performance
PRAGMA foreign_keys = ON;           -- Enforce referential integrity
PRAGMA busy_timeout = 5000;         -- Wait up to 5s on lock contention
```

---

## Tables

### buckets

```sql
CREATE TABLE buckets (
    name           TEXT PRIMARY KEY,
    region         TEXT NOT NULL DEFAULT 'us-east-1',
    owner_id       TEXT NOT NULL,
    owner_display  TEXT NOT NULL DEFAULT '',
    acl            TEXT NOT NULL DEFAULT '{}',      -- JSON-serialized ACL
    created_at     TEXT NOT NULL                     -- ISO 8601: 2026-02-22T12:00:00.000Z
);
```

### objects

```sql
CREATE TABLE objects (
    bucket         TEXT NOT NULL,
    key            TEXT NOT NULL,
    size           INTEGER NOT NULL,
    etag           TEXT NOT NULL,                    -- Quoted: "d41d8cd98f..."
    content_type   TEXT NOT NULL DEFAULT 'application/octet-stream',
    content_encoding TEXT,
    content_language TEXT,
    content_disposition TEXT,
    cache_control  TEXT,
    expires        TEXT,                             -- RFC 7231 date string
    storage_class  TEXT NOT NULL DEFAULT 'STANDARD',
    acl            TEXT NOT NULL DEFAULT '{}',       -- JSON-serialized ACL
    user_metadata  TEXT NOT NULL DEFAULT '{}',       -- JSON: {"key": "value"}
    last_modified  TEXT NOT NULL,                    -- ISO 8601
    delete_marker  INTEGER NOT NULL DEFAULT 0,       -- 0 or 1

    PRIMARY KEY (bucket, key),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

CREATE INDEX idx_objects_bucket ON objects(bucket);
CREATE INDEX idx_objects_bucket_prefix ON objects(bucket, key);
```

### multipart_uploads

```sql
CREATE TABLE multipart_uploads (
    upload_id      TEXT PRIMARY KEY,
    bucket         TEXT NOT NULL,
    key            TEXT NOT NULL,
    content_type   TEXT NOT NULL DEFAULT 'application/octet-stream',
    content_encoding TEXT,
    content_language TEXT,
    content_disposition TEXT,
    cache_control  TEXT,
    expires        TEXT,
    storage_class  TEXT NOT NULL DEFAULT 'STANDARD',
    acl            TEXT NOT NULL DEFAULT '{}',
    user_metadata  TEXT NOT NULL DEFAULT '{}',
    owner_id       TEXT NOT NULL,
    owner_display  TEXT NOT NULL DEFAULT '',
    initiated_at   TEXT NOT NULL,                    -- ISO 8601

    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

CREATE INDEX idx_uploads_bucket ON multipart_uploads(bucket);
CREATE INDEX idx_uploads_bucket_key ON multipart_uploads(bucket, key);
```

### multipart_parts

```sql
CREATE TABLE multipart_parts (
    upload_id      TEXT NOT NULL,
    part_number    INTEGER NOT NULL,
    size           INTEGER NOT NULL,
    etag           TEXT NOT NULL,                    -- Quoted MD5 hex
    last_modified  TEXT NOT NULL,                    -- ISO 8601

    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
);
```

### credentials

```sql
CREATE TABLE credentials (
    access_key_id  TEXT PRIMARY KEY,
    secret_key     TEXT NOT NULL,
    owner_id       TEXT NOT NULL,
    display_name   TEXT NOT NULL DEFAULT '',
    active         INTEGER NOT NULL DEFAULT 1,       -- 0 or 1
    created_at     TEXT NOT NULL                      -- ISO 8601
);
```

---

## ACL JSON Format

ACLs are stored as JSON for flexibility:

```json
{
  "owner": {
    "id": "canonical-user-id",
    "display_name": "user@example.com"
  },
  "grants": [
    {
      "grantee": {
        "type": "CanonicalUser",
        "id": "canonical-user-id",
        "display_name": "user@example.com"
      },
      "permission": "FULL_CONTROL"
    },
    {
      "grantee": {
        "type": "Group",
        "uri": "http://acs.amazonaws.com/groups/global/AllUsers"
      },
      "permission": "READ"
    }
  ]
}
```

### Grantee Types
- `CanonicalUser`: `id`, `display_name`
- `Group`: `uri`

### Permission Values
- `FULL_CONTROL`
- `READ`
- `WRITE`
- `READ_ACP`
- `WRITE_ACP`

---

## User Metadata JSON Format

```json
{
  "x-amz-meta-author": "John Doe",
  "x-amz-meta-version": "1.0"
}
```

Keys stored lowercase. Total serialized size must not exceed 2 KB.

---

## Common Queries

### ListBuckets
```sql
SELECT name, created_at, region, owner_id, owner_display
FROM buckets
WHERE owner_id = ?
ORDER BY name;
```

### ListObjectsV2
```sql
-- Without delimiter
SELECT key, size, etag, last_modified, storage_class
FROM objects
WHERE bucket = ? AND key > ? AND key LIKE ? || '%'
ORDER BY key
LIMIT ?;

-- With delimiter (requires application-level grouping for CommonPrefixes)
```

### HeadObject / GetObject metadata
```sql
SELECT key, size, etag, content_type, content_encoding, content_language,
       content_disposition, cache_control, expires, storage_class,
       user_metadata, last_modified, acl
FROM objects
WHERE bucket = ? AND key = ?;
```

### PutObject (upsert)
```sql
INSERT OR REPLACE INTO objects
    (bucket, key, size, etag, content_type, content_encoding, content_language,
     content_disposition, cache_control, expires, storage_class, acl,
     user_metadata, last_modified)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
```

### DeleteObject
```sql
DELETE FROM objects WHERE bucket = ? AND key = ?;
```

### ListMultipartUploads
```sql
SELECT upload_id, key, initiated_at, storage_class, owner_id, owner_display
FROM multipart_uploads
WHERE bucket = ? AND key > ? AND key LIKE ? || '%'
ORDER BY key, initiated_at
LIMIT ?;
```

### ListParts
```sql
SELECT part_number, size, etag, last_modified
FROM multipart_parts
WHERE upload_id = ? AND part_number > ?
ORDER BY part_number
LIMIT ?;
```

---

## Migration Strategy

Schema versioning via a simple version table:

```sql
CREATE TABLE schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);
```

Migrations applied on startup. Each implementation maintains the same schema
version sequence.

---

## Concurrency

- WAL mode allows concurrent readers with one writer
- In embedded mode: application serializes writes via mutex/lock
- In cluster mode: Raft ensures single-writer semantics (only leader writes)
- Read queries never block writes and vice versa (WAL mode)
