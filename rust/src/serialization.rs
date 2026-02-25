//! Metadata serialization: export/import between SQLite and JSON.

use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub const VERSION: &str = "0.1.0";
pub const EXPORT_VERSION: i64 = 1;

pub const ALL_TABLES: &[&str] = &[
    "buckets",
    "objects",
    "multipart_uploads",
    "multipart_parts",
    "credentials",
];

const JSON_FIELDS: &[&str] = &["acl", "user_metadata"];
const BOOL_FIELDS: &[&str] = &["delete_marker", "active"];

const TABLE_COLUMNS: &[(&str, &[&str])] = &[
    (
        "buckets",
        &["name", "region", "owner_id", "owner_display", "acl", "created_at"],
    ),
    (
        "objects",
        &[
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
    ),
    (
        "multipart_uploads",
        &[
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
    ),
    (
        "multipart_parts",
        &[
            "upload_id",
            "part_number",
            "size",
            "etag",
            "last_modified",
        ],
    ),
    (
        "credentials",
        &[
            "access_key_id",
            "secret_key",
            "owner_id",
            "display_name",
            "active",
            "created_at",
        ],
    ),
];

const TABLE_ORDER_BY: &[(&str, &str)] = &[
    ("buckets", "name"),
    ("objects", "bucket, key"),
    ("multipart_uploads", "upload_id"),
    ("multipart_parts", "upload_id, part_number"),
    ("credentials", "access_key_id"),
];

const DELETE_ORDER: &[&str] = &[
    "multipart_parts",
    "multipart_uploads",
    "objects",
    "buckets",
    "credentials",
];
const INSERT_ORDER: &[&str] = &[
    "buckets",
    "objects",
    "multipart_uploads",
    "multipart_parts",
    "credentials",
];

pub struct ExportOptions {
    pub tables: Vec<String>,
    pub include_credentials: bool,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            tables: ALL_TABLES.iter().map(|s| s.to_string()).collect(),
            include_credentials: false,
        }
    }
}

pub struct ImportOptions {
    pub replace: bool,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self { replace: false }
    }
}

pub struct ImportResult {
    pub counts: BTreeMap<String, usize>,
    pub skipped: BTreeMap<String, usize>,
    pub warnings: Vec<String>,
}

fn is_json_field(col: &str) -> bool {
    JSON_FIELDS.contains(&col)
}

fn is_bool_field(col: &str) -> bool {
    BOOL_FIELDS.contains(&col)
}

fn get_columns(table: &str) -> Option<&'static [&'static str]> {
    TABLE_COLUMNS
        .iter()
        .find(|(t, _)| *t == table)
        .map(|(_, cols)| *cols)
}

fn get_order_by(table: &str) -> &'static str {
    TABLE_ORDER_BY
        .iter()
        .find(|(t, _)| *t == table)
        .map(|(_, o)| *o)
        .unwrap_or("rowid")
}

fn get_schema_version(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1",
        [],
        |row| row.get(0),
    )
    .unwrap_or(1)
}

/// Read a column value from a rusqlite Row and convert to serde_json Value.
fn read_column(row: &rusqlite::Row, idx: usize, col: &str) -> Value {
    // Try to get as different types. rusqlite columns are dynamically typed.
    if is_json_field(col) {
        let s: Option<String> = row.get(idx).ok();
        match s {
            None => Value::Null,
            Some(s) => serde_json::from_str(&s).unwrap_or(Value::Object(Map::new())),
        }
    } else if is_bool_field(col) {
        let v: Option<i64> = row.get(idx).ok();
        match v {
            None => Value::Null,
            Some(v) => Value::Bool(v != 0),
        }
    } else {
        // Try integer first, then string, then null.
        if let Ok(v) = row.get::<_, i64>(idx) {
            Value::Number(v.into())
        } else if let Ok(v) = row.get::<_, f64>(idx) {
            serde_json::Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        } else if let Ok(v) = row.get::<_, String>(idx) {
            Value::String(v)
        } else {
            Value::Null
        }
    }
}

pub fn export_metadata(db_path: &str, opts: &ExportOptions) -> anyhow::Result<String> {
    let conn = Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let schema_version = get_schema_version(&conn);
    let now = chrono_now();

    let mut result = BTreeMap::<String, Value>::new();
    result.insert(
        "bleepstore_export".to_string(),
        json!({
            "exported_at": now,
            "schema_version": schema_version,
            "source": format!("rust/{}", VERSION),
            "version": EXPORT_VERSION,
        }),
    );

    for table in &opts.tables {
        let columns = match get_columns(table) {
            Some(c) => c,
            None => continue,
        };
        let order_by = get_order_by(table);
        let query = format!("SELECT * FROM {} ORDER BY {}", table, order_by);
        let mut stmt = conn.prepare(&query)?;

        let mut rows_out: Vec<Value> = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let mut obj = Map::new();
            for (i, col) in columns.iter().enumerate() {
                obj.insert(col.to_string(), read_column(row, i, col));
            }
            if table == "credentials" && !opts.include_credentials {
                obj.insert("secret_key".to_string(), Value::String("REDACTED".to_string()));
            }
            rows_out.push(Value::Object(obj));
        }
        result.insert(table.to_string(), Value::Array(rows_out));
    }

    // BTreeMap serializes with sorted keys. Use 2-space indent.
    let json = serde_json::to_string_pretty(&result)?;
    Ok(json)
}

pub fn import_metadata(
    db_path: &str,
    json_str: &str,
    opts: &ImportOptions,
) -> anyhow::Result<ImportResult> {
    let data: BTreeMap<String, Value> = serde_json::from_str(json_str)?;

    let envelope = data
        .get("bleepstore_export")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow::anyhow!("missing bleepstore_export envelope"))?;
    let version = envelope
        .get("version")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    if version < 1 || version > EXPORT_VERSION {
        anyhow::bail!("unsupported export version: {}", version);
    }

    let conn = Connection::open(db_path)?;
    conn.execute_batch("PRAGMA foreign_keys = ON")?;

    let mut result = ImportResult {
        counts: BTreeMap::new(),
        skipped: BTreeMap::new(),
        warnings: Vec::new(),
    };

    let tx = conn.unchecked_transaction()?;

    if opts.replace {
        for table in DELETE_ORDER {
            if data.contains_key(*table) {
                tx.execute(&format!("DELETE FROM {}", table), [])?;
            }
        }
    }

    for table in INSERT_ORDER {
        let rows_data = match data.get(*table) {
            Some(Value::Array(arr)) => arr,
            _ => continue,
        };
        let columns = match get_columns(table) {
            Some(c) => c,
            None => continue,
        };

        let mut inserted = 0usize;
        let mut skipped = 0usize;

        for row_val in rows_data {
            let row = match row_val.as_object() {
                Some(m) => m,
                None => {
                    skipped += 1;
                    continue;
                }
            };

            if *table == "credentials" {
                if let Some(Value::String(sk)) = row.get("secret_key") {
                    if sk == "REDACTED" {
                        skipped += 1;
                        let ak = row
                            .get("access_key_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("?");
                        result.warnings.push(format!(
                            "Skipped credential '{}': REDACTED secret_key",
                            ak
                        ));
                        continue;
                    }
                }
            }

            let col_names = columns.join(", ");
            let placeholders: Vec<&str> = columns.iter().map(|_| "?").collect();
            let ph = placeholders.join(", ");

            let sql = if opts.replace {
                format!("INSERT INTO {} ({}) VALUES ({})", table, col_names, ph)
            } else {
                format!(
                    "INSERT OR IGNORE INTO {} ({}) VALUES ({})",
                    table, col_names, ph
                )
            };

            let values: Vec<Box<dyn rusqlite::types::ToSql>> = columns
                .iter()
                .map(|col| collapse_value(row.get(*col).cloned().unwrap_or(Value::Null), col))
                .collect();

            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                values.iter().map(|v| v.as_ref()).collect();

            match tx.execute(&sql, param_refs.as_slice()) {
                Ok(n) if n > 0 => inserted += 1,
                Ok(_) => skipped += 1,
                Err(e) => {
                    skipped += 1;
                    result
                        .warnings
                        .push(format!("Skipped {} row: {}", table, e));
                }
            }
        }

        result.counts.insert(table.to_string(), inserted);
        result.skipped.insert(table.to_string(), skipped);
    }

    tx.commit()?;
    Ok(result)
}

fn collapse_value(val: Value, col: &str) -> Box<dyn rusqlite::types::ToSql> {
    if is_json_field(col) {
        match val {
            Value::Null => Box::new(Option::<String>::None),
            _ => Box::new(serde_json::to_string(&val).unwrap_or_else(|_| "{}".to_string())),
        }
    } else if is_bool_field(col) {
        match val {
            Value::Null => Box::new(Option::<i64>::None),
            Value::Bool(b) => Box::new(if b { 1i64 } else { 0i64 }),
            _ => Box::new(0i64),
        }
    } else {
        match val {
            Value::Null => Box::new(Option::<String>::None),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Box::new(i)
                } else if let Some(f) = n.as_f64() {
                    Box::new(f)
                } else {
                    Box::new(n.to_string())
                }
            }
            Value::String(s) => Box::new(s),
            _ => Box::new(serde_json::to_string(&val).unwrap_or_default()),
        }
    }
}

fn chrono_now() -> String {
    // Simple UTC timestamp without chrono dependency.
    use std::time::SystemTime;
    let dur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let secs = dur.as_secs();
    // Convert epoch seconds to ISO 8601.
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Simple date calculation from days since epoch (1970-01-01).
    let (year, month, day) = days_to_date(days as i64);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.000Z",
        year, month, day, hours, minutes, seconds
    )
}

fn days_to_date(days: i64) -> (i64, i64, i64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{params, Connection};
    use std::path::PathBuf;

    const SCHEMA_SQL: &str = r#"
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL
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
"#;

    fn create_test_db(path: &str, seed: bool) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(SCHEMA_SQL).unwrap();
        if seed {
            conn.execute(
                "INSERT INTO buckets VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    "test-bucket", "us-east-1", "bleepstore", "bleepstore",
                    r#"{"owner":{"id":"bleepstore"},"grants":[]}"#,
                    "2026-02-25T12:00:00.000Z"
                ],
            ).unwrap();
            conn.execute(
                "INSERT INTO objects VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                params![
                    "test-bucket", "photos/cat.jpg", 142857i64,
                    r#""d41d8cd98f00b204e9800998ecf8427e""#, "image/jpeg",
                    Option::<String>::None, Option::<String>::None, Option::<String>::None,
                    Option::<String>::None, Option::<String>::None, "STANDARD",
                    "{}", r#"{"x-amz-meta-author":"John"}"#,
                    "2026-02-25T14:30:45.000Z", 0i64
                ],
            ).unwrap();
            conn.execute(
                "INSERT INTO multipart_uploads VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                params![
                    "upload-abc123", "test-bucket", "large-file.bin",
                    "application/octet-stream", Option::<String>::None, Option::<String>::None,
                    Option::<String>::None, Option::<String>::None, Option::<String>::None,
                    "STANDARD", "{}", "{}",
                    "bleepstore", "bleepstore", "2026-02-25T13:00:00.000Z"
                ],
            ).unwrap();
            conn.execute(
                "INSERT INTO multipart_parts VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    "upload-abc123", 1i64, 5242880i64,
                    r#""098f6bcd4621d373cade4e832627b4f6""#,
                    "2026-02-25T13:05:00.000Z"
                ],
            ).unwrap();
            conn.execute(
                "INSERT INTO credentials VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    "bleepstore", "bleepstore-secret", "bleepstore", "bleepstore",
                    1i64, "2026-02-25T12:00:00.000Z"
                ],
            ).unwrap();
        }
    }

    #[test]
    fn test_export_all_tables() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        let db_str = db.to_str().unwrap();
        create_test_db(db_str, true);

        let result = export_metadata(db_str, &ExportOptions::default()).unwrap();
        let data: BTreeMap<String, Value> = serde_json::from_str(&result).unwrap();

        let envelope = data["bleepstore_export"].as_object().unwrap();
        assert_eq!(envelope["version"], 1);
        assert_eq!(envelope["source"], "rust/0.1.0");

        assert_eq!(data["buckets"].as_array().unwrap().len(), 1);
        assert_eq!(data["objects"].as_array().unwrap().len(), 1);
        assert_eq!(data["credentials"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_export_acl_expanded() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        let db_str = db.to_str().unwrap();
        create_test_db(db_str, true);

        let result = export_metadata(db_str, &ExportOptions::default()).unwrap();
        let data: BTreeMap<String, Value> = serde_json::from_str(&result).unwrap();

        let bucket = &data["buckets"][0];
        let acl = bucket["acl"].as_object().unwrap();
        assert_eq!(acl["owner"]["id"], "bleepstore");
    }

    #[test]
    fn test_export_bool_fields() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        let db_str = db.to_str().unwrap();
        create_test_db(db_str, true);

        let result = export_metadata(db_str, &ExportOptions::default()).unwrap();
        let data: BTreeMap<String, Value> = serde_json::from_str(&result).unwrap();

        assert_eq!(data["objects"][0]["delete_marker"], false);
        assert_eq!(data["credentials"][0]["active"], true);
    }

    #[test]
    fn test_export_null_fields() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        let db_str = db.to_str().unwrap();
        create_test_db(db_str, true);

        let result = export_metadata(db_str, &ExportOptions::default()).unwrap();
        let data: BTreeMap<String, Value> = serde_json::from_str(&result).unwrap();

        assert!(data["objects"][0]["content_encoding"].is_null());
    }

    #[test]
    fn test_export_credentials_redacted() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        let db_str = db.to_str().unwrap();
        create_test_db(db_str, true);

        let result = export_metadata(db_str, &ExportOptions::default()).unwrap();
        let data: BTreeMap<String, Value> = serde_json::from_str(&result).unwrap();

        assert_eq!(data["credentials"][0]["secret_key"], "REDACTED");
    }

    #[test]
    fn test_export_credentials_included() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        let db_str = db.to_str().unwrap();
        create_test_db(db_str, true);

        let opts = ExportOptions {
            include_credentials: true,
            ..ExportOptions::default()
        };
        let result = export_metadata(db_str, &opts).unwrap();
        let data: BTreeMap<String, Value> = serde_json::from_str(&result).unwrap();

        assert_eq!(data["credentials"][0]["secret_key"], "bleepstore-secret");
    }

    #[test]
    fn test_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let db1 = dir.path().join("source.db");
        let db2 = dir.path().join("target.db");
        create_test_db(db1.to_str().unwrap(), true);
        create_test_db(db2.to_str().unwrap(), false);

        let opts = ExportOptions {
            include_credentials: true,
            ..ExportOptions::default()
        };
        let exported = export_metadata(db1.to_str().unwrap(), &opts).unwrap();
        let result = import_metadata(db2.to_str().unwrap(), &exported, &ImportOptions::default()).unwrap();

        assert_eq!(*result.counts.get("buckets").unwrap(), 1);
        assert_eq!(*result.counts.get("objects").unwrap(), 1);

        let re_exported = export_metadata(db2.to_str().unwrap(), &opts).unwrap();
        let mut data1: BTreeMap<String, Value> = serde_json::from_str(&exported).unwrap();
        let mut data2: BTreeMap<String, Value> = serde_json::from_str(&re_exported).unwrap();
        data1.remove("bleepstore_export");
        data2.remove("bleepstore_export");
        assert_eq!(data1, data2);
    }

    #[test]
    fn test_import_merge_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        create_test_db(db.to_str().unwrap(), true);

        let opts = ExportOptions {
            include_credentials: true,
            ..ExportOptions::default()
        };
        let exported = export_metadata(db.to_str().unwrap(), &opts).unwrap();
        let result = import_metadata(db.to_str().unwrap(), &exported, &ImportOptions::default()).unwrap();

        assert_eq!(*result.counts.get("buckets").unwrap(), 0);
    }

    #[test]
    fn test_import_skips_redacted() {
        let dir = tempfile::tempdir().unwrap();
        let db1 = dir.path().join("source.db");
        let db2 = dir.path().join("target.db");
        create_test_db(db1.to_str().unwrap(), true);
        create_test_db(db2.to_str().unwrap(), false);

        let exported = export_metadata(db1.to_str().unwrap(), &ExportOptions::default()).unwrap();
        let result = import_metadata(db2.to_str().unwrap(), &exported, &ImportOptions::default()).unwrap();

        assert_eq!(*result.skipped.get("credentials").unwrap(), 1);
        assert_eq!(result.warnings.len(), 1);
        assert!(result.warnings[0].contains("REDACTED"));
    }

    #[test]
    fn test_import_invalid_version() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        create_test_db(db.to_str().unwrap(), false);

        let err = import_metadata(
            db.to_str().unwrap(),
            r#"{"bleepstore_export":{"version":99}}"#,
            &ImportOptions::default(),
        );
        assert!(err.is_err());
    }

    #[test]
    fn test_reference_fixture() {
        let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../tests/fixtures/metadata-export-reference.json");
        let fixture_data = match std::fs::read_to_string(&fixture_path) {
            Ok(d) => d,
            Err(_) => {
                eprintln!("Skipping: reference fixture not found at {:?}", fixture_path);
                return;
            }
        };

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        create_test_db(db.to_str().unwrap(), false);

        let result = import_metadata(db.to_str().unwrap(), &fixture_data, &ImportOptions::default()).unwrap();
        assert_eq!(*result.counts.get("buckets").unwrap(), 2);
        assert_eq!(*result.counts.get("objects").unwrap(), 3);

        let opts = ExportOptions {
            include_credentials: true,
            ..ExportOptions::default()
        };
        let re_exported = export_metadata(db.to_str().unwrap(), &opts).unwrap();

        let ref_data: BTreeMap<String, Value> = serde_json::from_str(&fixture_data).unwrap();
        let rust_data: BTreeMap<String, Value> = serde_json::from_str(&re_exported).unwrap();

        for table in ALL_TABLES {
            assert_eq!(
                ref_data.get(*table),
                rust_data.get(*table),
                "table {} mismatch",
                table
            );
        }
    }
}
