"""Generate the reference metadata export fixture for cross-language testing."""

import json
import sqlite3
import sys
from pathlib import Path

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);
INSERT OR IGNORE INTO schema_version (version, applied_at) VALUES (1, '2026-01-01T00:00:00.000Z');

CREATE TABLE IF NOT EXISTS buckets (
    name TEXT PRIMARY KEY, region TEXT NOT NULL DEFAULT 'us-east-1',
    owner_id TEXT NOT NULL, owner_display TEXT NOT NULL DEFAULT '',
    acl TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS objects (
    bucket TEXT NOT NULL, key TEXT NOT NULL, size INTEGER NOT NULL,
    etag TEXT NOT NULL, content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    content_encoding TEXT, content_language TEXT, content_disposition TEXT,
    cache_control TEXT, expires TEXT,
    storage_class TEXT NOT NULL DEFAULT 'STANDARD',
    acl TEXT NOT NULL DEFAULT '{}', user_metadata TEXT NOT NULL DEFAULT '{}',
    last_modified TEXT NOT NULL, delete_marker INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket, key),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS multipart_uploads (
    upload_id TEXT PRIMARY KEY, bucket TEXT NOT NULL, key TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    content_encoding TEXT, content_language TEXT, content_disposition TEXT,
    cache_control TEXT, expires TEXT,
    storage_class TEXT NOT NULL DEFAULT 'STANDARD',
    acl TEXT NOT NULL DEFAULT '{}', user_metadata TEXT NOT NULL DEFAULT '{}',
    owner_id TEXT NOT NULL, owner_display TEXT NOT NULL DEFAULT '',
    initiated_at TEXT NOT NULL,
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS multipart_parts (
    upload_id TEXT NOT NULL, part_number INTEGER NOT NULL,
    size INTEGER NOT NULL, etag TEXT NOT NULL, last_modified TEXT NOT NULL,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS credentials (
    access_key_id TEXT PRIMARY KEY, secret_key TEXT NOT NULL,
    owner_id TEXT NOT NULL, display_name TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL
);
"""

SEED_DATA = [
    ("INSERT INTO buckets VALUES (?, ?, ?, ?, ?, ?)",
     ("alpha-bucket", "us-east-1", "bleepstore", "BleepStore Admin",
      '{"owner":{"id":"bleepstore"},"grants":[]}', "2026-01-15T08:00:00.000Z")),
    ("INSERT INTO buckets VALUES (?, ?, ?, ?, ?, ?)",
     ("beta-bucket", "eu-west-1", "user1", "User One",
      '{"owner":{"id":"user1"},"grants":[{"permission":"READ","grantee":{"type":"CanonicalUser","id":"user2"}}]}',
      "2026-02-01T10:30:00.000Z")),
    ("INSERT INTO objects VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
     ("alpha-bucket", "docs/readme.txt", 1024,
      '"5d41402abc4b2a76b9719d911017c592"', "text/plain",
      None, None, None, None, None, "STANDARD",
      '{}', '{}', "2026-01-15T09:00:00.000Z", 0)),
    ("INSERT INTO objects VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
     ("alpha-bucket", "images/photo.jpg", 2048576,
      '"098f6bcd4621d373cade4e832627b4f6"', "image/jpeg",
      None, "en-US", "attachment; filename=\"photo.jpg\"", "max-age=3600", None,
      "STANDARD", '{}',
      '{"x-amz-meta-camera":"Nikon","x-amz-meta-location":"Paris"}',
      "2026-01-20T14:30:00.000Z", 0)),
    ("INSERT INTO objects VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
     ("beta-bucket", "deleted-file.txt", 0,
      '"d41d8cd98f00b204e9800998ecf8427e"', "application/octet-stream",
      None, None, None, None, None, "STANDARD",
      '{}', '{}', "2026-02-10T16:00:00.000Z", 1)),
    ("INSERT INTO multipart_uploads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
     ("upload-001", "alpha-bucket", "large-backup.tar.gz",
      "application/gzip", "gzip", None, None, None, None,
      "STANDARD", '{}', '{"x-amz-meta-source":"server-1"}',
      "bleepstore", "BleepStore Admin", "2026-02-20T11:00:00.000Z")),
    ("INSERT INTO multipart_parts VALUES (?, ?, ?, ?, ?)",
     ("upload-001", 1, 5242880, '"part1etag"', "2026-02-20T11:05:00.000Z")),
    ("INSERT INTO multipart_parts VALUES (?, ?, ?, ?, ?)",
     ("upload-001", 2, 3145728, '"part2etag"', "2026-02-20T11:10:00.000Z")),
    ("INSERT INTO credentials VALUES (?, ?, ?, ?, ?, ?)",
     ("bleepstore", "bleepstore-secret", "bleepstore", "BleepStore Admin", 1,
      "2026-01-01T00:00:00.000Z")),
    ("INSERT INTO credentials VALUES (?, ?, ?, ?, ?, ?)",
     ("testuser", "test-secret-key-123", "user1", "User One", 1,
      "2026-02-01T10:30:00.000Z")),
    ("INSERT INTO credentials VALUES (?, ?, ?, ?, ?, ?)",
     ("disabled-user", "old-secret", "user2", "User Two", 0,
      "2026-01-10T06:00:00.000Z")),
]


def main() -> None:
    # Create temp DB with reference data.
    db_path = "/tmp/bleepstore_reference.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_SQL)
    for sql, params in SEED_DATA:
        conn.execute(sql, params)
    conn.commit()
    conn.close()

    # Use the serialization module to export.
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python" / "src"))
    from bleepstore.serialization import ExportOptions, export_metadata

    # Export with credentials included for the reference fixture.
    opts = ExportOptions(include_credentials=True)
    result = export_metadata(db_path, opts)

    # Rewrite exported_at to a fixed value for determinism.
    data = json.loads(result)
    data["bleepstore_export"]["exported_at"] = "2026-02-25T00:00:00.000Z"
    result = json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False)

    out_path = Path(__file__).resolve().parent / "metadata-export-reference.json"
    out_path.write_text(result + "\n", encoding="utf-8")
    print(f"Generated: {out_path}")


if __name__ == "__main__":
    main()
