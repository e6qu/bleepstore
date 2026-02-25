"""Tests for metadata serialization (export/import)."""

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from bleepstore.serialization import (
    ALL_TABLES,
    ExportOptions,
    ImportOptions,
    ImportResult,
    export_metadata,
    import_metadata,
)

# Schema DDL matching specs/metadata-schema.md
SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);
INSERT OR IGNORE INTO schema_version (version, applied_at) VALUES (1, '2026-01-01T00:00:00.000Z');

CREATE TABLE IF NOT EXISTS buckets (
    name           TEXT PRIMARY KEY,
    region         TEXT NOT NULL DEFAULT 'us-east-1',
    owner_id       TEXT NOT NULL,
    owner_display  TEXT NOT NULL DEFAULT '',
    acl            TEXT NOT NULL DEFAULT '{}',
    created_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS objects (
    bucket              TEXT NOT NULL,
    key                 TEXT NOT NULL,
    size                INTEGER NOT NULL,
    etag                TEXT NOT NULL,
    content_type        TEXT NOT NULL DEFAULT 'application/octet-stream',
    content_encoding    TEXT,
    content_language    TEXT,
    content_disposition TEXT,
    cache_control       TEXT,
    expires             TEXT,
    storage_class       TEXT NOT NULL DEFAULT 'STANDARD',
    acl                 TEXT NOT NULL DEFAULT '{}',
    user_metadata       TEXT NOT NULL DEFAULT '{}',
    last_modified       TEXT NOT NULL,
    delete_marker       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket, key),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS multipart_uploads (
    upload_id           TEXT PRIMARY KEY,
    bucket              TEXT NOT NULL,
    key                 TEXT NOT NULL,
    content_type        TEXT NOT NULL DEFAULT 'application/octet-stream',
    content_encoding    TEXT,
    content_language    TEXT,
    content_disposition TEXT,
    cache_control       TEXT,
    expires             TEXT,
    storage_class       TEXT NOT NULL DEFAULT 'STANDARD',
    acl                 TEXT NOT NULL DEFAULT '{}',
    user_metadata       TEXT NOT NULL DEFAULT '{}',
    owner_id            TEXT NOT NULL,
    owner_display       TEXT NOT NULL DEFAULT '',
    initiated_at        TEXT NOT NULL,
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS multipart_parts (
    upload_id      TEXT NOT NULL,
    part_number    INTEGER NOT NULL,
    size           INTEGER NOT NULL,
    etag           TEXT NOT NULL,
    last_modified  TEXT NOT NULL,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS credentials (
    access_key_id  TEXT PRIMARY KEY,
    secret_key     TEXT NOT NULL,
    owner_id       TEXT NOT NULL,
    display_name   TEXT NOT NULL DEFAULT '',
    active         INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL
);
"""


def _create_test_db(db_path: str, seed: bool = True) -> None:
    """Create a test SQLite database with the schema and optional seed data."""
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_SQL)
    if seed:
        conn.execute(
            "INSERT INTO buckets VALUES (?, ?, ?, ?, ?, ?)",
            ("test-bucket", "us-east-1", "bleepstore", "bleepstore",
             '{"owner":{"id":"bleepstore"},"grants":[]}', "2026-02-25T12:00:00.000Z"),
        )
        conn.execute(
            "INSERT INTO objects VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("test-bucket", "photos/cat.jpg", 142857,
             '"d41d8cd98f00b204e9800998ecf8427e"', "image/jpeg",
             None, None, None, None, None, "STANDARD",
             '{}', '{"x-amz-meta-author":"John"}',
             "2026-02-25T14:30:45.000Z", 0),
        )
        conn.execute(
            "INSERT INTO multipart_uploads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("upload-abc123", "test-bucket", "large-file.bin",
             "application/octet-stream", None, None, None, None, None,
             "STANDARD", '{}', '{}', "bleepstore", "bleepstore",
             "2026-02-25T13:00:00.000Z"),
        )
        conn.execute(
            "INSERT INTO multipart_parts VALUES (?, ?, ?, ?, ?)",
            ("upload-abc123", 1, 5242880,
             '"098f6bcd4621d373cade4e832627b4f6"',
             "2026-02-25T13:05:00.000Z"),
        )
        conn.execute(
            "INSERT INTO credentials VALUES (?, ?, ?, ?, ?, ?)",
            ("bleepstore", "bleepstore-secret", "bleepstore", "bleepstore", 1,
             "2026-02-25T12:00:00.000Z"),
        )
    conn.commit()
    conn.close()


class TestExport:
    def test_export_all_tables(self, tmp_path: Path) -> None:
        db = str(tmp_path / "test.db")
        _create_test_db(db)
        result = export_metadata(db)
        data = json.loads(result)

        assert "bleepstore_export" in data
        assert data["bleepstore_export"]["version"] == 1
        assert data["bleepstore_export"]["schema_version"] == 1
        assert data["bleepstore_export"]["source"] == "python/0.1.0"

        assert len(data["buckets"]) == 1
        assert data["buckets"][0]["name"] == "test-bucket"

        assert len(data["objects"]) == 1
        assert data["objects"][0]["key"] == "photos/cat.jpg"
        assert data["objects"][0]["size"] == 142857

        assert len(data["multipart_uploads"]) == 1
        assert len(data["multipart_parts"]) == 1
        assert len(data["credentials"]) == 1

    def test_export_acl_expanded(self, tmp_path: Path) -> None:
        db = str(tmp_path / "test.db")
        _create_test_db(db)
        data = json.loads(export_metadata(db))

        acl = data["buckets"][0]["acl"]
        assert isinstance(acl, dict)
        assert acl["owner"]["id"] == "bleepstore"
        assert acl["grants"] == []

    def test_export_user_metadata_expanded(self, tmp_path: Path) -> None:
        db = str(tmp_path / "test.db")
        _create_test_db(db)
        data = json.loads(export_metadata(db))

        meta = data["objects"][0]["user_metadata"]
        assert isinstance(meta, dict)
        assert meta["x-amz-meta-author"] == "John"

    def test_export_bool_fields(self, tmp_path: Path) -> None:
        db = str(tmp_path / "test.db")
        _create_test_db(db)
        data = json.loads(export_metadata(db))

        assert data["objects"][0]["delete_marker"] is False
        assert data["credentials"][0]["active"] is True

    def test_export_null_fields(self, tmp_path: Path) -> None:
        db = str(tmp_path / "test.db")
        _create_test_db(db)
        data = json.loads(export_metadata(db))

        obj = data["objects"][0]
        assert obj["content_encoding"] is None
        assert obj["content_language"] is None
        assert obj["cache_control"] is None

    def test_export_credentials_redacted(self, tmp_path: Path) -> None:
        db = str(tmp_path / "test.db")
        _create_test_db(db)
        data = json.loads(export_metadata(db))

        assert data["credentials"][0]["secret_key"] == "REDACTED"

    def test_export_credentials_included(self, tmp_path: Path) -> None:
        db = str(tmp_path / "test.db")
        _create_test_db(db)
        opts = ExportOptions(include_credentials=True)
        data = json.loads(export_metadata(db, opts))

        assert data["credentials"][0]["secret_key"] == "bleepstore-secret"

    def test_export_partial_tables(self, tmp_path: Path) -> None:
        db = str(tmp_path / "test.db")
        _create_test_db(db)
        opts = ExportOptions(tables=["buckets", "objects"])
        data = json.loads(export_metadata(db, opts))

        assert "buckets" in data
        assert "objects" in data
        assert "credentials" not in data
        assert "multipart_uploads" not in data

    def test_export_sorted_keys(self, tmp_path: Path) -> None:
        db = str(tmp_path / "test.db")
        _create_test_db(db)
        result = export_metadata(db)

        # Verify sorted keys by checking the top-level key order in the raw JSON.
        data = json.loads(result)
        keys = list(data.keys())
        assert keys == sorted(keys)

    def test_export_empty_tables(self, tmp_path: Path) -> None:
        db = str(tmp_path / "test.db")
        _create_test_db(db, seed=False)
        data = json.loads(export_metadata(db))

        assert data["buckets"] == []
        assert data["objects"] == []
        assert data["credentials"] == []

    def test_export_two_space_indent(self, tmp_path: Path) -> None:
        db = str(tmp_path / "test.db")
        _create_test_db(db, seed=False)
        result = export_metadata(db)
        # Second line should be indented with 2 spaces.
        lines = result.split("\n")
        assert lines[1].startswith("  ")
        assert not lines[1].startswith("    ")


class TestImport:
    def test_round_trip(self, tmp_path: Path) -> None:
        """Export from one DB, import into another, export again, compare."""
        db1 = str(tmp_path / "source.db")
        db2 = str(tmp_path / "target.db")
        _create_test_db(db1)
        _create_test_db(db2, seed=False)

        opts = ExportOptions(include_credentials=True)
        exported = export_metadata(db1, opts)

        result = import_metadata(db2, exported)
        assert result.counts["buckets"] == 1
        assert result.counts["objects"] == 1
        assert result.counts["credentials"] == 1

        re_exported = export_metadata(db2, opts)

        # Compare data sections (envelope timestamps differ).
        data1 = json.loads(exported)
        data2 = json.loads(re_exported)
        del data1["bleepstore_export"]
        del data2["bleepstore_export"]
        assert data1 == data2

    def test_import_merge_idempotent(self, tmp_path: Path) -> None:
        db = str(tmp_path / "test.db")
        _create_test_db(db)

        opts = ExportOptions(include_credentials=True)
        exported = export_metadata(db, opts)

        # Import again into same DB — should be idempotent.
        result = import_metadata(db, exported)
        assert result.counts["buckets"] == 0  # Already exists, skipped.
        assert result.skipped["buckets"] == 1

    def test_import_replace(self, tmp_path: Path) -> None:
        db1 = str(tmp_path / "source.db")
        db2 = str(tmp_path / "target.db")
        _create_test_db(db1)
        _create_test_db(db2)

        opts = ExportOptions(include_credentials=True)
        exported = export_metadata(db1, opts)

        result = import_metadata(db2, exported, ImportOptions(replace=True))
        assert result.counts["buckets"] == 1

    def test_import_skips_redacted_credentials(self, tmp_path: Path) -> None:
        db1 = str(tmp_path / "source.db")
        db2 = str(tmp_path / "target.db")
        _create_test_db(db1)
        _create_test_db(db2, seed=False)

        # Export without credentials (redacted).
        exported = export_metadata(db1)

        result = import_metadata(db2, exported)
        assert result.skipped.get("credentials", 0) == 1
        assert len(result.warnings) == 1
        assert "REDACTED" in result.warnings[0]

    def test_import_invalid_version(self, tmp_path: Path) -> None:
        db = str(tmp_path / "test.db")
        _create_test_db(db, seed=False)

        bad_json = json.dumps({"bleepstore_export": {"version": 99}})
        with pytest.raises(ValueError, match="Unsupported export version"):
            import_metadata(db, bad_json)

    def test_import_dependency_order(self, tmp_path: Path) -> None:
        """Objects depend on buckets — import should handle order correctly."""
        db1 = str(tmp_path / "source.db")
        db2 = str(tmp_path / "target.db")
        _create_test_db(db1)
        _create_test_db(db2, seed=False)

        opts = ExportOptions(include_credentials=True)
        exported = export_metadata(db1, opts)

        result = import_metadata(db2, exported)
        assert result.counts["buckets"] == 1
        assert result.counts["objects"] == 1
        assert result.counts["multipart_uploads"] == 1
        assert result.counts["multipart_parts"] == 1

    def test_import_partial(self, tmp_path: Path) -> None:
        """Import only buckets — objects should not appear."""
        db1 = str(tmp_path / "source.db")
        db2 = str(tmp_path / "target.db")
        _create_test_db(db1)
        _create_test_db(db2, seed=False)

        opts = ExportOptions(tables=["buckets"])
        exported = export_metadata(db1, opts)

        result = import_metadata(db2, exported)
        assert result.counts["buckets"] == 1
        assert "objects" not in result.counts


class TestMetaCLI:
    def test_export_to_file(self, tmp_path: Path) -> None:
        from bleepstore.meta_cli import main

        db = str(tmp_path / "test.db")
        _create_test_db(db)
        output = str(tmp_path / "export.json")

        rc = main(["export", "--db", db, "--output", output])
        assert rc == 0

        data = json.loads(Path(output).read_text())
        assert "bleepstore_export" in data
        assert len(data["buckets"]) == 1

    def test_import_from_file(self, tmp_path: Path) -> None:
        from bleepstore.meta_cli import main

        db1 = str(tmp_path / "source.db")
        db2 = str(tmp_path / "target.db")
        _create_test_db(db1)
        _create_test_db(db2, seed=False)

        export_file = str(tmp_path / "export.json")
        main(["export", "--db", db1, "--output", export_file, "--include-credentials"])

        rc = main(["import", "--db", db2, "--input", export_file])
        assert rc == 0

    def test_export_specific_tables(self, tmp_path: Path) -> None:
        from bleepstore.meta_cli import main

        db = str(tmp_path / "test.db")
        _create_test_db(db)
        output = str(tmp_path / "export.json")

        rc = main(["export", "--db", db, "--output", output, "--tables", "buckets"])
        assert rc == 0

        data = json.loads(Path(output).read_text())
        assert "buckets" in data
        assert "objects" not in data

    def test_invalid_table_name(self, tmp_path: Path) -> None:
        from bleepstore.meta_cli import main

        db = str(tmp_path / "test.db")
        _create_test_db(db)

        rc = main(["export", "--db", db, "--tables", "invalid_table"])
        assert rc == 1
