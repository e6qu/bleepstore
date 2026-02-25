"""Metadata serialization: export/import between SQLite and JSON."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

VERSION = "0.1.0"
EXPORT_VERSION = 1
ALL_TABLES = ["buckets", "objects", "multipart_uploads", "multipart_parts", "credentials"]

# Fields that store JSON strings in SQLite but should be expanded in export.
JSON_FIELDS = {"acl", "user_metadata"}
# Fields that are integer booleans in SQLite but should be JSON booleans.
BOOL_FIELDS = {"delete_marker", "active"}


@dataclass
class ExportOptions:
    tables: list[str] = field(default_factory=lambda: list(ALL_TABLES))
    include_credentials: bool = False


@dataclass
class ImportOptions:
    replace: bool = False


@dataclass
class ImportResult:
    counts: dict[str, int] = field(default_factory=dict)
    skipped: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def _get_schema_version(conn: sqlite3.Connection) -> int:
    """Read the schema_version from the database."""
    try:
        row = conn.execute(
            "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else 1
    except sqlite3.OperationalError:
        return 1


def _expand_row(row: dict[str, Any]) -> dict[str, Any]:
    """Expand JSON string fields and convert int booleans to real booleans."""
    result: dict[str, Any] = {}
    for key, value in row.items():
        if key in JSON_FIELDS:
            if value is None:
                result[key] = None
            elif isinstance(value, str):
                try:
                    result[key] = json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    result[key] = {}
            else:
                result[key] = value
        elif key in BOOL_FIELDS:
            result[key] = bool(value) if value is not None else None
        else:
            result[key] = value
    return result


def _collapse_row(row: dict[str, Any]) -> dict[str, Any]:
    """Collapse JSON object fields to strings and convert booleans to ints."""
    result: dict[str, Any] = {}
    for key, value in row.items():
        if key in JSON_FIELDS:
            if value is None:
                result[key] = None
            elif isinstance(value, (dict, list)):
                result[key] = json.dumps(value, separators=(",", ":"), sort_keys=True)
            else:
                result[key] = str(value)
        elif key in BOOL_FIELDS:
            if value is None:
                result[key] = None
            else:
                result[key] = 1 if value else 0
        else:
            result[key] = value
    return result


# Column orders for each table (matches SQLite schema).
TABLE_COLUMNS = {
    "buckets": ["name", "region", "owner_id", "owner_display", "acl", "created_at"],
    "objects": [
        "bucket",
        "key",
        "size",
        "etag",
        "content_type",
        "content_encoding",
        "content_language",
        "content_disposition",
        "cache_control",
        "expires",
        "storage_class",
        "acl",
        "user_metadata",
        "last_modified",
        "delete_marker",
    ],
    "multipart_uploads": [
        "upload_id",
        "bucket",
        "key",
        "content_type",
        "content_encoding",
        "content_language",
        "content_disposition",
        "cache_control",
        "expires",
        "storage_class",
        "acl",
        "user_metadata",
        "owner_id",
        "owner_display",
        "initiated_at",
    ],
    "multipart_parts": ["upload_id", "part_number", "size", "etag", "last_modified"],
    "credentials": [
        "access_key_id",
        "secret_key",
        "owner_id",
        "display_name",
        "active",
        "created_at",
    ],
}

# Primary key columns for ORDER BY in export.
TABLE_ORDER_BY = {
    "buckets": "name",
    "objects": "bucket, key",
    "multipart_uploads": "upload_id",
    "multipart_parts": "upload_id, part_number",
    "credentials": "access_key_id",
}

# Deletion order for replace mode (respects FK constraints).
DELETE_ORDER = ["multipart_parts", "multipart_uploads", "objects", "buckets", "credentials"]
# Insertion order (parents before children).
INSERT_ORDER = ["buckets", "objects", "multipart_uploads", "multipart_parts", "credentials"]


def export_metadata(db_path: str, options: ExportOptions | None = None) -> str:
    """Export metadata from SQLite to JSON string.

    Args:
        db_path: Path to the SQLite database file.
        options: Export options (tables, credentials).

    Returns:
        JSON string with sorted keys and 2-space indent.
    """
    if options is None:
        options = ExportOptions()

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        schema_version = _get_schema_version(conn)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

        result: dict[str, Any] = {
            "bleepstore_export": {
                "version": EXPORT_VERSION,
                "exported_at": now,
                "schema_version": schema_version,
                "source": f"python/{VERSION}",
            },
        }

        for table in options.tables:
            if table not in TABLE_COLUMNS:
                continue
            order_by = TABLE_ORDER_BY[table]
            cursor = conn.execute(f"SELECT * FROM {table} ORDER BY {order_by}")  # noqa: S608
            rows = []
            for row in cursor:
                expanded = _expand_row(dict(row))
                if table == "credentials" and not options.include_credentials:
                    expanded["secret_key"] = "REDACTED"
                rows.append(expanded)
            result[table] = rows

        return json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False)
    finally:
        conn.close()


def import_metadata(
    db_path: str,
    json_str: str,
    options: ImportOptions | None = None,
) -> ImportResult:
    """Import metadata from JSON string into SQLite.

    Args:
        db_path: Path to the SQLite database file.
        json_str: JSON string to import.
        options: Import options (merge vs replace).

    Returns:
        ImportResult with counts per table and any warnings.
    """
    if options is None:
        options = ImportOptions()

    data = json.loads(json_str)

    # Validate envelope.
    envelope = data.get("bleepstore_export", {})
    export_version = envelope.get("version", 0)
    if export_version < 1 or export_version > EXPORT_VERSION:
        raise ValueError(f"Unsupported export version: {export_version}")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    result = ImportResult()

    try:
        conn.execute("BEGIN")

        # Determine which tables are present in the import data.
        tables_to_import = [t for t in INSERT_ORDER if t in data]

        if options.replace:
            # Delete in reverse dependency order.
            for table in DELETE_ORDER:
                if table in data:
                    conn.execute(f"DELETE FROM {table}")  # noqa: S608

        for table in tables_to_import:
            rows = data[table]
            if not isinstance(rows, list):
                continue

            columns = TABLE_COLUMNS.get(table)
            if not columns:
                continue

            inserted = 0
            skipped = 0

            for row in rows:
                # Skip credentials with REDACTED secret_key.
                if table == "credentials" and row.get("secret_key") == "REDACTED":
                    skipped += 1
                    result.warnings.append(
                        f"Skipped credential '{row.get('access_key_id')}': REDACTED secret_key"
                    )
                    continue

                collapsed = _collapse_row(row)
                values = [collapsed.get(col) for col in columns]

                placeholders = ", ".join(["?"] * len(columns))
                col_names = ", ".join(columns)

                if options.replace:
                    sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"  # noqa: S608
                else:
                    sql = f"INSERT OR IGNORE INTO {table} ({col_names}) VALUES ({placeholders})"  # noqa: S608

                try:
                    cursor = conn.execute(sql, values)
                    if cursor.rowcount > 0:
                        inserted += 1
                    else:
                        skipped += 1
                except sqlite3.IntegrityError as e:
                    skipped += 1
                    result.warnings.append(f"Skipped {table} row: {e}")

            result.counts[table] = inserted
            result.skipped[table] = skipped

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    return result
