# Metadata Serialization Specification

## Overview

BleepStore supports exporting and importing metadata (buckets, objects, multipart
uploads, parts, credentials) between SQLite databases and JSON files. This enables
backup, migration, and cross-instance data transfer.

A standalone `bleepstore-meta` tool (separate entry point, same codebase) provides
the CLI interface in each language implementation.

## Canonical JSON Schema

All 4 implementations produce identical JSON output for the same data.

```json
{
  "bleepstore_export": {
    "version": 1,
    "exported_at": "2026-02-25T14:30:45.000Z",
    "schema_version": 1,
    "source": "python/0.1.0"
  },
  "buckets": [
    {
      "name": "my-bucket",
      "region": "us-east-1",
      "owner_id": "bleepstore",
      "owner_display": "bleepstore",
      "acl": { "owner": { "id": "bleepstore" }, "grants": [] },
      "created_at": "2026-02-25T12:00:00.000Z"
    }
  ],
  "objects": [
    {
      "bucket": "my-bucket",
      "key": "photos/cat.jpg",
      "size": 142857,
      "etag": "\"d41d8cd98f00b204e9800998ecf8427e\"",
      "content_type": "image/jpeg",
      "content_encoding": null,
      "content_language": null,
      "content_disposition": null,
      "cache_control": null,
      "expires": null,
      "storage_class": "STANDARD",
      "acl": {},
      "user_metadata": { "x-amz-meta-author": "John" },
      "last_modified": "2026-02-25T14:30:45.000Z",
      "delete_marker": false
    }
  ],
  "multipart_uploads": [
    {
      "upload_id": "abc123",
      "bucket": "my-bucket",
      "key": "large-file.bin",
      "content_type": "application/octet-stream",
      "content_encoding": null,
      "content_language": null,
      "content_disposition": null,
      "cache_control": null,
      "expires": null,
      "storage_class": "STANDARD",
      "acl": {},
      "user_metadata": {},
      "owner_id": "bleepstore",
      "owner_display": "bleepstore",
      "initiated_at": "2026-02-25T13:00:00.000Z"
    }
  ],
  "multipart_parts": [
    {
      "upload_id": "abc123",
      "part_number": 1,
      "size": 5242880,
      "etag": "\"098f6bcd4621d373cade4e832627b4f6\"",
      "last_modified": "2026-02-25T13:05:00.000Z"
    }
  ],
  "credentials": [
    {
      "access_key_id": "bleepstore",
      "secret_key": "REDACTED",
      "owner_id": "bleepstore",
      "display_name": "bleepstore",
      "active": true,
      "created_at": "2026-02-25T12:00:00.000Z"
    }
  ]
}
```

## Format Rules

1. **Envelope** (`bleepstore_export`): `version: 1`, `source` identifies implementation (e.g. `python/0.1.0`, `go/0.1.0`, `rust/0.1.0`, `zig/0.1.0`)
2. **Null vs empty**: Nullable fields explicitly `null` (not omitted). Distinguishes SQL NULL from `""`
3. **JSON fields expanded**: `acl` and `user_metadata` are real JSON objects (not escaped strings)
4. **Booleans**: `delete_marker` and `active` are JSON booleans (not 0/1 integers)
5. **Credentials**: `secret_key` is `"REDACTED"` unless `--include-credentials`
6. **Sorted keys**: JSON output uses sorted keys + 2-space indent for cross-language diffability
7. **Empty collections**: `[]` (not omitted)
8. **Partial export**: Omitted tables don't appear in JSON at all

## CLI Interface

```
bleepstore-meta export --config bleepstore.yaml --format json --output metadata.json \
    [--tables buckets,objects] [--include-credentials]

bleepstore-meta import --config bleepstore.yaml --input metadata.json \
    [--merge|--replace]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | `bleepstore.yaml` | Config file (to find `metadata.sqlite.path`) |
| `--format` | `json` | Export format (only `json` for now) |
| `--output` | `-` (stdout) | Output file path |
| `--input` | `-` (stdin) | Input file path |
| `--tables` | all | Comma-separated: `buckets,objects,multipart_uploads,multipart_parts,credentials` |
| `--include-credentials` | false | Include real secret keys |
| `--merge` | true | INSERT OR IGNORE — keeps existing records |
| `--replace` | false | DELETE existing rows first, then INSERT |

## Import Semantics

### Merge mode (default)
- `INSERT OR IGNORE` for each record
- Existing data preserved, new records added
- Safe and idempotent

### Replace mode
- Wraps in transaction
- `DELETE FROM <table>` for each table present in import
- `INSERT` all records from import
- Deletion order respects FK constraints: parts -> uploads -> objects -> buckets
- Insert order: buckets -> objects -> uploads -> parts -> credentials

### Foreign key handling
- Import processes tables in dependency order: buckets -> objects -> multipart_uploads -> multipart_parts -> credentials
- Records referencing non-existent parents are skipped with a warning (merge mode)
- Credentials with `"REDACTED"` secret_key are skipped

## Direct SQLite Access

The serialization module opens its own SQLite connection (read-only for export,
read-write for import). It does NOT go through the MetadataStore interface. Bulk
SELECT/INSERT is simpler and the tool runs offline.

## Valid Tables

The following table names are recognized:
- `buckets`
- `objects`
- `multipart_uploads`
- `multipart_parts`
- `credentials`

## Cross-Language Identity Guarantees

All 4 implementations (Python, Go, Rust, Zig) are normalized to produce
identical output at three levels:

### Level 1: JSON Export Identity

The `bleepstore-meta export` command produces byte-identical JSON (modulo the
`bleepstore_export.source` field) across all implementations for the same
database content. Verified by `tests/serialization/test_cross_language.sh`.

### Level 2: Metadata SQLite Identity

Importing the same reference fixture via `bleepstore-meta import` produces
identical SQLite row data across all implementations (after normalizing
JSON whitespace in embedded fields). Verified by
`tests/serialization/test_metadata_identity.sh`.

### Level 3: Storage Identity

For the same sequence of S3 operations, all implementations produce:

- **Local filesystem**: Identical file trees — same paths, same content.
  Multipart temp directory: `.multipart/`. Part file naming: plain decimal.
- **SQLite storage**: Identical `object_data` and `part_data` table rows.
  Uses composite PKs: `(bucket, key)` and `(upload_id, part_number)`.

Verified by `tests/serialization/test_local_storage_identity.py` and
`tests/serialization/test_sqlite_storage_identity.py`.

### Normalization Conventions

| Aspect | Convention | Notes |
|--------|-----------|-------|
| Multipart temp dir (local) | `.multipart` | All 4 implementations |
| Part file naming (local) | Plain decimal (`1`, `2`, ...) | Not zero-padded |
| SQLite storage PK (object_data) | `PRIMARY KEY (bucket, key)` | Composite |
| SQLite storage PK (part_data) | `PRIMARY KEY (upload_id, part_number)` | Composite |
| Memory snapshot PK | Same as SQLite storage | Composite columns |
| ETag computation | MD5 hex, quoted | Identical algorithm |
| Timestamps | ISO 8601, `.000Z` suffix | Millisecond precision |
| ACL/user_metadata | JSON string in SQLite | `{}` default |
