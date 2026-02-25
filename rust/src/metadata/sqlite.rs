//! SQLite-backed metadata store.
//!
//! Uses `rusqlite` with the `bundled` feature so no system SQLite
//! library is required.  All async trait methods are thin wrappers
//! around synchronous rusqlite calls executed under a `Mutex`.
//!
//! Schema matches the spec in `specs/metadata-schema.md`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use rusqlite::{params, Connection, OptionalExtension};

use super::store::{
    BucketRecord, CredentialRecord, ListObjectsResult, ListPartsResult, ListUploadsResult,
    MetadataStore, MultipartUploadRecord, ObjectRecord, PartRecord,
};

/// Current schema version. Bumped when migrations are added.
const SCHEMA_VERSION: i64 = 1;

/// Metadata store backed by a single SQLite database file.
pub struct SqliteMetadataStore {
    /// The database connection, guarded by a mutex for Send + Sync.
    conn: Mutex<Connection>,
}

impl SqliteMetadataStore {
    /// Open (or create) the database at `path` and initialize the schema.
    ///
    /// Passing `":memory:"` creates an in-memory database (useful for tests).
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.apply_pragmas()?;
        store.init_db()?;
        Ok(store)
    }

    /// Apply recommended SQLite pragmas for performance and safety.
    fn apply_pragmas(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;
            PRAGMA busy_timeout = 5000;
            ",
        )?;
        Ok(())
    }

    /// Create the required tables and indexes if they do not already exist.
    /// This is idempotent -- safe to call on every startup (crash-only design).
    fn init_db(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");
        conn.execute_batch(
            "
            -- Schema version tracking
            CREATE TABLE IF NOT EXISTS schema_version (
                version    INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );

            -- Buckets
            CREATE TABLE IF NOT EXISTS buckets (
                name           TEXT PRIMARY KEY,
                region         TEXT NOT NULL DEFAULT 'us-east-1',
                owner_id       TEXT NOT NULL,
                owner_display  TEXT NOT NULL DEFAULT '',
                acl            TEXT NOT NULL DEFAULT '{}',
                created_at     TEXT NOT NULL
            );

            -- Objects
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

            CREATE INDEX IF NOT EXISTS idx_objects_bucket
                ON objects(bucket);
            CREATE INDEX IF NOT EXISTS idx_objects_bucket_prefix
                ON objects(bucket, key);

            -- Multipart uploads
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

            CREATE INDEX IF NOT EXISTS idx_uploads_bucket
                ON multipart_uploads(bucket);
            CREATE INDEX IF NOT EXISTS idx_uploads_bucket_key
                ON multipart_uploads(bucket, key);

            -- Multipart parts
            CREATE TABLE IF NOT EXISTS multipart_parts (
                upload_id      TEXT NOT NULL,
                part_number    INTEGER NOT NULL,
                size           INTEGER NOT NULL,
                etag           TEXT NOT NULL,
                last_modified  TEXT NOT NULL,

                PRIMARY KEY (upload_id, part_number),
                FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
            );

            -- Credentials
            CREATE TABLE IF NOT EXISTS credentials (
                access_key_id  TEXT PRIMARY KEY,
                secret_key     TEXT NOT NULL,
                owner_id       TEXT NOT NULL,
                display_name   TEXT NOT NULL DEFAULT '',
                active         INTEGER NOT NULL DEFAULT 1,
                created_at     TEXT NOT NULL
            );
            ",
        )?;

        // Record schema version if not already present.
        let existing: Option<i64> = conn
            .query_row("SELECT MAX(version) FROM schema_version", [], |row| {
                row.get(0)
            })
            .optional()?
            .flatten();

        if existing.is_none() || existing.unwrap() < SCHEMA_VERSION {
            let now = chrono_now();
            conn.execute(
                "INSERT OR REPLACE INTO schema_version (version, applied_at) VALUES (?1, ?2)",
                params![SCHEMA_VERSION, now],
            )?;
        }

        Ok(())
    }

    /// Seed the default credential from config on startup (crash-only: every
    /// startup is recovery). This is idempotent.
    pub fn seed_credential(&self, access_key: &str, secret_key: &str) -> anyhow::Result<()> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let now = chrono_now();
        conn.execute(
            "INSERT OR IGNORE INTO credentials (access_key_id, secret_key, owner_id, display_name, active, created_at)
             VALUES (?1, ?2, ?3, ?4, 1, ?5)",
            params![access_key, secret_key, access_key, access_key, now],
        )?;
        Ok(())
    }

    /// Reap expired multipart uploads older than `ttl_seconds`.
    ///
    /// Deletes both multipart_parts and multipart_uploads rows for each
    /// expired upload in a single transaction. Returns the number of
    /// reaped uploads and their (upload_id, bucket, key) tuples so the
    /// caller can also clean up storage.
    pub fn reap_expired_uploads(
        &self,
        ttl_seconds: i64,
    ) -> anyhow::Result<Vec<(String, String, String)>> {
        let conn = self.conn.lock().expect("mutex poisoned");
        let tx = conn.unchecked_transaction()?;

        // Find expired uploads. Scope the statement so it's dropped before commit.
        let expired: Vec<(String, String, String)> = {
            let mut stmt = tx.prepare(
                "SELECT upload_id, bucket, key FROM multipart_uploads
                 WHERE initiated_at < datetime('now', '-' || ?1 || ' seconds')",
            )?;

            let result = stmt
                .query_map(params![ttl_seconds], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            result
        };

        // Delete parts and uploads for each expired upload.
        for (upload_id, _, _) in &expired {
            tx.execute(
                "DELETE FROM multipart_parts WHERE upload_id = ?1",
                params![upload_id],
            )?;
            tx.execute(
                "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id],
            )?;
        }

        tx.commit()?;
        Ok(expired)
    }
}

/// Get current time as ISO-8601 string (e.g., "2026-02-23T12:00:00.000Z").
fn chrono_now() -> String {
    // Use SystemTime -> format manually to avoid chrono dep.
    let now = std::time::SystemTime::now();
    let since_epoch = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = since_epoch.as_secs();
    let millis = since_epoch.subsec_millis();

    // Convert to date-time components.
    // Using a simple algorithm; for production we'd use chrono, but
    // keeping minimal deps for Stage 2.
    format_timestamp(secs, millis)
}

/// Format a unix timestamp (secs + millis) into ISO-8601 string.
fn format_timestamp(secs: u64, millis: u32) -> String {
    // Days since epoch, accounting for leap years.
    let days = secs / 86400;
    let day_secs = secs % 86400;
    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let seconds = day_secs % 60;

    let (year, month, day) = days_to_ymd(days);

    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z")
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(days: u64) -> (i32, u32, u32) {
    // Algorithm from Howard Hinnant's date algorithms.
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year as i32, m as u32, d as u32)
}

/// Serialize user_metadata HashMap to JSON string.
fn serialize_user_metadata(meta: &HashMap<String, String>) -> String {
    serde_json::to_string(meta).unwrap_or_else(|_| "{}".to_string())
}

/// Deserialize user_metadata JSON string to HashMap.
fn deserialize_user_metadata(json: &str) -> HashMap<String, String> {
    serde_json::from_str(json).unwrap_or_default()
}

// ── MetadataStore implementation ───────────────────────────────────

impl MetadataStore for SqliteMetadataStore {
    // ── Buckets ─────────────────────────────────────────────────────

    fn create_bucket(
        &self,
        record: BucketRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            conn.execute(
                "INSERT INTO buckets (name, region, owner_id, owner_display, acl, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    record.name,
                    record.region,
                    record.owner_id,
                    record.owner_display,
                    record.acl,
                    record.created_at,
                ],
            )?;
            Ok(())
        })
    }

    fn get_bucket(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<BucketRecord>>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            let result = conn
                .query_row(
                    "SELECT name, region, owner_id, owner_display, acl, created_at
                     FROM buckets WHERE name = ?1",
                    params![name],
                    |row| {
                        Ok(BucketRecord {
                            name: row.get(0)?,
                            region: row.get(1)?,
                            owner_id: row.get(2)?,
                            owner_display: row.get(3)?,
                            acl: row.get(4)?,
                            created_at: row.get(5)?,
                        })
                    },
                )
                .optional()?;
            Ok(result)
        })
    }

    fn bucket_exists(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM buckets WHERE name = ?1",
                params![name],
                |row| row.get(0),
            )?;
            Ok(count > 0)
        })
    }

    fn list_buckets(
        &self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<BucketRecord>>> + Send + '_>> {
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            let mut stmt = conn.prepare(
                "SELECT name, region, owner_id, owner_display, acl, created_at
                 FROM buckets ORDER BY name",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(BucketRecord {
                    name: row.get(0)?,
                    region: row.get(1)?,
                    owner_id: row.get(2)?,
                    owner_display: row.get(3)?,
                    acl: row.get(4)?,
                    created_at: row.get(5)?,
                })
            })?;
            let mut buckets = Vec::new();
            for row in rows {
                buckets.push(row?);
            }
            Ok(buckets)
        })
    }

    fn delete_bucket(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            conn.execute("DELETE FROM buckets WHERE name = ?1", params![name])?;
            Ok(())
        })
    }

    fn update_bucket_acl(
        &self,
        name: &str,
        acl: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let name = name.to_string();
        let acl = acl.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            conn.execute(
                "UPDATE buckets SET acl = ?1 WHERE name = ?2",
                params![acl, name],
            )?;
            Ok(())
        })
    }

    // ── Objects ─────────────────────────────────────────────────────

    fn put_object(
        &self,
        record: ObjectRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            let user_meta_json = serialize_user_metadata(&record.user_metadata);
            conn.execute(
                "INSERT OR REPLACE INTO objects
                    (bucket, key, size, etag, content_type, content_encoding,
                     content_language, content_disposition, cache_control, expires,
                     storage_class, acl, user_metadata, last_modified, delete_marker)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                params![
                    record.bucket,
                    record.key,
                    record.size as i64,
                    record.etag,
                    record.content_type,
                    record.content_encoding,
                    record.content_language,
                    record.content_disposition,
                    record.cache_control,
                    record.expires,
                    record.storage_class,
                    record.acl,
                    user_meta_json,
                    record.last_modified,
                    record.delete_marker as i32,
                ],
            )?;
            Ok(())
        })
    }

    fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<ObjectRecord>>> + Send + '_>> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            let result = conn
                .query_row(
                    "SELECT bucket, key, size, etag, content_type, content_encoding,
                            content_language, content_disposition, cache_control, expires,
                            storage_class, acl, user_metadata, last_modified, delete_marker
                     FROM objects WHERE bucket = ?1 AND key = ?2",
                    params![bucket, key],
                    |row| {
                        let size: i64 = row.get(2)?;
                        let user_meta_json: String = row.get(12)?;
                        let delete_marker: i32 = row.get(14)?;
                        Ok(ObjectRecord {
                            bucket: row.get(0)?,
                            key: row.get(1)?,
                            size: size as u64,
                            etag: row.get(3)?,
                            content_type: row.get(4)?,
                            content_encoding: row.get(5)?,
                            content_language: row.get(6)?,
                            content_disposition: row.get(7)?,
                            cache_control: row.get(8)?,
                            expires: row.get(9)?,
                            storage_class: row.get(10)?,
                            acl: row.get(11)?,
                            last_modified: row.get(13)?,
                            user_metadata: deserialize_user_metadata(&user_meta_json),
                            delete_marker: delete_marker != 0,
                        })
                    },
                )
                .optional()?;
            Ok(result)
        })
    }

    fn object_exists(
        &self,
        bucket: &str,
        key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM objects WHERE bucket = ?1 AND key = ?2",
                params![bucket, key],
                |row| row.get(0),
            )?;
            Ok(count > 0)
        })
    }

    fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: &str,
        max_keys: u32,
        start_after: &str,
        continuation_token: Option<&str>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<ListObjectsResult>> + Send + '_>> {
        let bucket = bucket.to_string();
        let prefix = prefix.to_string();
        let delimiter = delimiter.to_string();
        let start_after = start_after.to_string();
        let continuation_token = continuation_token.map(|s| s.to_string());
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");

            // Determine the effective start key.
            let effective_start = if let Some(ref token) = continuation_token {
                token.as_str()
            } else {
                start_after.as_str()
            };

            // Build the like pattern from the prefix.
            let like_pattern = format!("{prefix}%");

            // Fetch one extra row to determine if truncated.
            let fetch_limit = max_keys as i64 + 1;

            let mut stmt = conn.prepare(
                "SELECT bucket, key, size, etag, content_type, content_encoding,
                        content_language, content_disposition, cache_control, expires,
                        storage_class, acl, user_metadata, last_modified, delete_marker
                 FROM objects
                 WHERE bucket = ?1 AND key > ?2 AND key LIKE ?3
                 ORDER BY key
                 LIMIT ?4",
            )?;

            let rows = stmt.query_map(
                params![bucket, effective_start, like_pattern, fetch_limit],
                |row| {
                    let size: i64 = row.get(2)?;
                    let user_meta_json: String = row.get(12)?;
                    let delete_marker: i32 = row.get(14)?;
                    Ok(ObjectRecord {
                        bucket: row.get(0)?,
                        key: row.get(1)?,
                        size: size as u64,
                        etag: row.get(3)?,
                        content_type: row.get(4)?,
                        content_encoding: row.get(5)?,
                        content_language: row.get(6)?,
                        content_disposition: row.get(7)?,
                        cache_control: row.get(8)?,
                        expires: row.get(9)?,
                        storage_class: row.get(10)?,
                        acl: row.get(11)?,
                        last_modified: row.get(13)?,
                        user_metadata: deserialize_user_metadata(&user_meta_json),
                        delete_marker: delete_marker != 0,
                    })
                },
            )?;

            let mut all_objects: Vec<ObjectRecord> = Vec::new();
            for row in rows {
                all_objects.push(row?);
            }

            // Apply delimiter grouping at the application level.
            if delimiter.is_empty() {
                // No delimiter: simple pagination.
                let is_truncated = all_objects.len() > max_keys as usize;
                let objects: Vec<ObjectRecord> =
                    all_objects.into_iter().take(max_keys as usize).collect();
                let next_token = if is_truncated {
                    objects.last().map(|o| o.key.clone())
                } else {
                    None
                };
                Ok(ListObjectsResult {
                    objects,
                    common_prefixes: Vec::new(),
                    next_continuation_token: next_token,
                    is_truncated,
                })
            } else {
                // With delimiter: group keys by common prefix.
                let mut objects = Vec::new();
                let mut common_prefixes = std::collections::BTreeSet::new();
                let mut count = 0u32;

                for obj in all_objects {
                    if count >= max_keys {
                        break;
                    }

                    // Check if the key has the delimiter after the prefix.
                    let after_prefix = &obj.key[prefix.len()..];
                    if let Some(pos) = after_prefix.find(&delimiter) {
                        // This key belongs to a common prefix group.
                        let cp = format!("{}{}{}", prefix, &after_prefix[..pos], delimiter);
                        if common_prefixes.insert(cp) {
                            count += 1;
                        }
                    } else {
                        objects.push(obj);
                        count += 1;
                    }
                }

                // Determine truncation: we fetched max_keys+1 rows, and after
                // grouping we check if there's more.
                let is_truncated = count >= max_keys;
                let next_token = if is_truncated {
                    // Use the last object key (or last common prefix) as the continuation token.
                    if let Some(last_obj) = objects.last() {
                        Some(last_obj.key.clone())
                    } else {
                        common_prefixes.iter().last().cloned()
                    }
                } else {
                    None
                };

                Ok(ListObjectsResult {
                    objects,
                    common_prefixes: common_prefixes.into_iter().collect(),
                    next_continuation_token: next_token,
                    is_truncated,
                })
            }
        })
    }

    fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            conn.execute(
                "DELETE FROM objects WHERE bucket = ?1 AND key = ?2",
                params![bucket, key],
            )?;
            Ok(())
        })
    }

    fn delete_objects(
        &self,
        bucket: &str,
        keys: &[String],
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<String>>> + Send + '_>> {
        let bucket = bucket.to_string();
        let keys = keys.to_vec();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            // Batch delete using DELETE...WHERE key IN (...).
            // SQLite has a 999-variable limit; reserve 1 for bucket = 998 keys per batch.
            const BATCH_SIZE: usize = 998;
            for chunk in keys.chunks(BATCH_SIZE) {
                let placeholders: Vec<String> =
                    (0..chunk.len()).map(|i| format!("?{}", i + 2)).collect();
                let sql = format!(
                    "DELETE FROM objects WHERE bucket = ?1 AND key IN ({})",
                    placeholders.join(", ")
                );
                let mut params: Vec<Box<dyn rusqlite::types::ToSql>> =
                    Vec::with_capacity(chunk.len() + 1);
                params.push(Box::new(bucket.clone()));
                for k in chunk {
                    params.push(Box::new(k.clone()));
                }
                let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                    params.iter().map(|p| p.as_ref()).collect();
                conn.execute(&sql, param_refs.as_slice())?;
            }
            // S3 always reports all keys as deleted regardless of whether they existed.
            Ok(keys)
        })
    }

    fn update_object_acl(
        &self,
        bucket: &str,
        key: &str,
        acl: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        let acl = acl.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            conn.execute(
                "UPDATE objects SET acl = ?1 WHERE bucket = ?2 AND key = ?3",
                params![acl, bucket, key],
            )?;
            Ok(())
        })
    }

    fn count_objects(
        &self,
        bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<u64>> + Send + '_>> {
        let bucket = bucket.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM objects WHERE bucket = ?1",
                params![bucket],
                |row| row.get(0),
            )?;
            Ok(count as u64)
        })
    }

    // ── Multipart uploads ───────────────────────────────────────────

    fn create_multipart_upload(
        &self,
        record: MultipartUploadRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            let user_meta_json = serialize_user_metadata(&record.user_metadata);
            conn.execute(
                "INSERT INTO multipart_uploads
                    (upload_id, bucket, key, content_type, content_encoding,
                     content_language, content_disposition, cache_control, expires,
                     storage_class, acl, user_metadata, owner_id, owner_display, initiated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                params![
                    record.upload_id,
                    record.bucket,
                    record.key,
                    record.content_type,
                    record.content_encoding,
                    record.content_language,
                    record.content_disposition,
                    record.cache_control,
                    record.expires,
                    record.storage_class,
                    record.acl,
                    user_meta_json,
                    record.owner_id,
                    record.owner_display,
                    record.initiated_at,
                ],
            )?;
            Ok(())
        })
    }

    fn get_multipart_upload(
        &self,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<MultipartUploadRecord>>> + Send + '_>>
    {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            let result = conn
                .query_row(
                    "SELECT upload_id, bucket, key, content_type, content_encoding,
                            content_language, content_disposition, cache_control, expires,
                            storage_class, acl, user_metadata, owner_id, owner_display, initiated_at
                     FROM multipart_uploads WHERE upload_id = ?1",
                    params![upload_id],
                    |row| {
                        let user_meta_json: String = row.get(11)?;
                        Ok(MultipartUploadRecord {
                            upload_id: row.get(0)?,
                            bucket: row.get(1)?,
                            key: row.get(2)?,
                            content_type: row.get(3)?,
                            content_encoding: row.get(4)?,
                            content_language: row.get(5)?,
                            content_disposition: row.get(6)?,
                            cache_control: row.get(7)?,
                            expires: row.get(8)?,
                            storage_class: row.get(9)?,
                            acl: row.get(10)?,
                            user_metadata: deserialize_user_metadata(&user_meta_json),
                            owner_id: row.get(12)?,
                            owner_display: row.get(13)?,
                            initiated_at: row.get(14)?,
                        })
                    },
                )
                .optional()?;
            Ok(result)
        })
    }

    fn put_part(
        &self,
        upload_id: &str,
        part: PartRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            conn.execute(
                "INSERT OR REPLACE INTO multipart_parts
                    (upload_id, part_number, size, etag, last_modified)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    upload_id,
                    part.part_number,
                    part.size as i64,
                    part.etag,
                    part.last_modified,
                ],
            )?;
            Ok(())
        })
    }

    fn list_parts(
        &self,
        upload_id: &str,
        max_parts: u32,
        part_number_marker: u32,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<ListPartsResult>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            let fetch_limit = max_parts as i64 + 1;
            let mut stmt = conn.prepare(
                "SELECT part_number, size, etag, last_modified
                 FROM multipart_parts
                 WHERE upload_id = ?1 AND part_number > ?2
                 ORDER BY part_number
                 LIMIT ?3",
            )?;
            let rows =
                stmt.query_map(params![upload_id, part_number_marker, fetch_limit], |row| {
                    let size: i64 = row.get(1)?;
                    Ok(PartRecord {
                        part_number: row.get(0)?,
                        size: size as u64,
                        etag: row.get(2)?,
                        last_modified: row.get(3)?,
                    })
                })?;
            let mut parts = Vec::new();
            for row in rows {
                parts.push(row?);
            }
            let is_truncated = parts.len() > max_parts as usize;
            if is_truncated {
                parts.truncate(max_parts as usize);
            }
            let next_marker = if is_truncated {
                parts.last().map(|p| p.part_number)
            } else {
                None
            };
            Ok(ListPartsResult {
                parts,
                is_truncated,
                next_part_number_marker: next_marker,
            })
        })
    }

    fn get_parts_for_completion(
        &self,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<PartRecord>>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            let mut stmt = conn.prepare(
                "SELECT part_number, size, etag, last_modified
                 FROM multipart_parts
                 WHERE upload_id = ?1
                 ORDER BY part_number",
            )?;
            let rows = stmt.query_map(params![upload_id], |row| {
                let size: i64 = row.get(1)?;
                Ok(PartRecord {
                    part_number: row.get(0)?,
                    size: size as u64,
                    etag: row.get(2)?,
                    last_modified: row.get(3)?,
                })
            })?;
            let mut parts = Vec::new();
            for row in rows {
                parts.push(row?);
            }
            Ok(parts)
        })
    }

    fn complete_multipart_upload(
        &self,
        upload_id: &str,
        final_object: ObjectRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            // Use a transaction for atomicity.
            conn.execute_batch("BEGIN IMMEDIATE")?;

            let user_meta_json = serialize_user_metadata(&final_object.user_metadata);

            // Insert the final object record.
            let insert_result = conn.execute(
                "INSERT OR REPLACE INTO objects
                    (bucket, key, size, etag, content_type, content_encoding,
                     content_language, content_disposition, cache_control, expires,
                     storage_class, acl, user_metadata, last_modified, delete_marker)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                params![
                    final_object.bucket,
                    final_object.key,
                    final_object.size as i64,
                    final_object.etag,
                    final_object.content_type,
                    final_object.content_encoding,
                    final_object.content_language,
                    final_object.content_disposition,
                    final_object.cache_control,
                    final_object.expires,
                    final_object.storage_class,
                    final_object.acl,
                    user_meta_json,
                    final_object.last_modified,
                    final_object.delete_marker as i32,
                ],
            );

            if let Err(e) = insert_result {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(e.into());
            }

            // Delete the parts.
            let del_parts = conn.execute(
                "DELETE FROM multipart_parts WHERE upload_id = ?1",
                params![upload_id],
            );
            if let Err(e) = del_parts {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(e.into());
            }

            // Delete the upload record.
            let del_upload = conn.execute(
                "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id],
            );
            if let Err(e) = del_upload {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(e.into());
            }

            conn.execute_batch("COMMIT")?;
            Ok(())
        })
    }

    fn delete_multipart_upload(
        &self,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            // Foreign key cascade will delete parts too.
            conn.execute(
                "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id],
            )?;
            Ok(())
        })
    }

    fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        max_uploads: u32,
        key_marker: &str,
        upload_id_marker: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<ListUploadsResult>> + Send + '_>> {
        let bucket = bucket.to_string();
        let prefix = prefix.to_string();
        let key_marker = key_marker.to_string();
        let upload_id_marker = upload_id_marker.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            let like_pattern = format!("{prefix}%");
            let fetch_limit = max_uploads as i64 + 1;

            // Build the query with key+upload_id marker support.
            let mut stmt = if key_marker.is_empty() {
                conn.prepare(
                    "SELECT upload_id, bucket, key, content_type, content_encoding,
                            content_language, content_disposition, cache_control, expires,
                            storage_class, acl, user_metadata, owner_id, owner_display, initiated_at
                     FROM multipart_uploads
                     WHERE bucket = ?1 AND key LIKE ?2
                     ORDER BY key, initiated_at
                     LIMIT ?3",
                )?
            } else if upload_id_marker.is_empty() {
                conn.prepare(
                    "SELECT upload_id, bucket, key, content_type, content_encoding,
                            content_language, content_disposition, cache_control, expires,
                            storage_class, acl, user_metadata, owner_id, owner_display, initiated_at
                     FROM multipart_uploads
                     WHERE bucket = ?1 AND key LIKE ?2 AND key > ?4
                     ORDER BY key, initiated_at
                     LIMIT ?3",
                )?
            } else {
                conn.prepare(
                    "SELECT upload_id, bucket, key, content_type, content_encoding,
                            content_language, content_disposition, cache_control, expires,
                            storage_class, acl, user_metadata, owner_id, owner_display, initiated_at
                     FROM multipart_uploads
                     WHERE bucket = ?1 AND key LIKE ?2
                       AND (key > ?4 OR (key = ?4 AND upload_id > ?5))
                     ORDER BY key, initiated_at
                     LIMIT ?3",
                )?
            };

            // Map row to MultipartUploadRecord -- shared closure.
            fn map_upload_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MultipartUploadRecord> {
                let user_meta_json: String = row.get(11)?;
                Ok(MultipartUploadRecord {
                    upload_id: row.get(0)?,
                    bucket: row.get(1)?,
                    key: row.get(2)?,
                    content_type: row.get(3)?,
                    content_encoding: row.get(4)?,
                    content_language: row.get(5)?,
                    content_disposition: row.get(6)?,
                    cache_control: row.get(7)?,
                    expires: row.get(8)?,
                    storage_class: row.get(9)?,
                    acl: row.get(10)?,
                    user_metadata: deserialize_user_metadata(&user_meta_json),
                    owner_id: row.get(12)?,
                    owner_display: row.get(13)?,
                    initiated_at: row.get(14)?,
                })
            }

            let mut uploads = Vec::new();
            if key_marker.is_empty() {
                let rows =
                    stmt.query_map(params![bucket, like_pattern, fetch_limit], map_upload_row)?;
                for row in rows {
                    uploads.push(row?);
                }
            } else if upload_id_marker.is_empty() {
                let rows = stmt.query_map(
                    params![bucket, like_pattern, fetch_limit, key_marker],
                    map_upload_row,
                )?;
                for row in rows {
                    uploads.push(row?);
                }
            } else {
                let rows = stmt.query_map(
                    params![
                        bucket,
                        like_pattern,
                        fetch_limit,
                        key_marker,
                        upload_id_marker
                    ],
                    map_upload_row,
                )?;
                for row in rows {
                    uploads.push(row?);
                }
            }

            let is_truncated = uploads.len() > max_uploads as usize;
            if is_truncated {
                uploads.truncate(max_uploads as usize);
            }

            let (next_key_marker, next_upload_id_marker) = if is_truncated {
                if let Some(last) = uploads.last() {
                    (Some(last.key.clone()), Some(last.upload_id.clone()))
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };

            Ok(ListUploadsResult {
                uploads,
                is_truncated,
                next_key_marker,
                next_upload_id_marker,
            })
        })
    }

    // ── Credentials ─────────────────────────────────────────────────

    fn get_credential(
        &self,
        access_key_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<CredentialRecord>>> + Send + '_>> {
        let access_key_id = access_key_id.to_string();
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            let result = conn
                .query_row(
                    "SELECT access_key_id, secret_key, owner_id, display_name, active, created_at
                     FROM credentials WHERE access_key_id = ?1 AND active = 1",
                    params![access_key_id],
                    |row| {
                        let active: i32 = row.get(4)?;
                        Ok(CredentialRecord {
                            access_key_id: row.get(0)?,
                            secret_key: row.get(1)?,
                            owner_id: row.get(2)?,
                            display_name: row.get(3)?,
                            active: active != 0,
                            created_at: row.get(5)?,
                        })
                    },
                )
                .optional()?;
            Ok(result)
        })
    }

    fn put_credential(
        &self,
        record: CredentialRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let conn = self.conn.lock().expect("mutex poisoned");
            conn.execute(
                "INSERT OR REPLACE INTO credentials
                    (access_key_id, secret_key, owner_id, display_name, active, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    record.access_key_id,
                    record.secret_key,
                    record.owner_id,
                    record.display_name,
                    record.active as i32,
                    record.created_at,
                ],
            )?;
            Ok(())
        })
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> SqliteMetadataStore {
        SqliteMetadataStore::new(":memory:").expect("failed to create in-memory store")
    }

    fn make_bucket(name: &str) -> BucketRecord {
        BucketRecord {
            name: name.to_string(),
            created_at: "2026-02-23T00:00:00.000Z".to_string(),
            region: "us-east-1".to_string(),
            owner_id: "test-owner".to_string(),
            owner_display: "Test Owner".to_string(),
            acl: "{}".to_string(),
        }
    }

    fn make_object(bucket: &str, key: &str, size: u64) -> ObjectRecord {
        ObjectRecord {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size,
            etag: format!("\"etag-{key}\""),
            content_type: "application/octet-stream".to_string(),
            content_encoding: None,
            content_language: None,
            content_disposition: None,
            cache_control: None,
            expires: None,
            storage_class: "STANDARD".to_string(),
            acl: "{}".to_string(),
            last_modified: "2026-02-23T00:00:00.000Z".to_string(),
            user_metadata: HashMap::new(),
            delete_marker: false,
        }
    }

    // ── Schema tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_schema_idempotent() {
        let store = test_store();
        // Call init_db() again -- should not fail.
        store.init_db().expect("second init_db failed");
        // And a third time for good measure.
        store.init_db().expect("third init_db failed");
    }

    // ── Bucket tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_create_and_get_bucket() {
        let store = test_store();
        let bucket = make_bucket("test-bucket");
        store.create_bucket(bucket).await.unwrap();

        let fetched = store.get_bucket("test-bucket").await.unwrap();
        assert!(fetched.is_some());
        let b = fetched.unwrap();
        assert_eq!(b.name, "test-bucket");
        assert_eq!(b.region, "us-east-1");
        assert_eq!(b.owner_id, "test-owner");
    }

    #[tokio::test]
    async fn test_bucket_exists() {
        let store = test_store();
        assert!(!store.bucket_exists("nonexistent").await.unwrap());
        store.create_bucket(make_bucket("exists")).await.unwrap();
        assert!(store.bucket_exists("exists").await.unwrap());
    }

    #[tokio::test]
    async fn test_list_buckets() {
        let store = test_store();
        store.create_bucket(make_bucket("alpha")).await.unwrap();
        store.create_bucket(make_bucket("beta")).await.unwrap();
        store.create_bucket(make_bucket("gamma")).await.unwrap();

        let buckets = store.list_buckets().await.unwrap();
        assert_eq!(buckets.len(), 3);
        // Should be sorted by name.
        assert_eq!(buckets[0].name, "alpha");
        assert_eq!(buckets[1].name, "beta");
        assert_eq!(buckets[2].name, "gamma");
    }

    #[tokio::test]
    async fn test_delete_bucket() {
        let store = test_store();
        store.create_bucket(make_bucket("to-delete")).await.unwrap();
        assert!(store.bucket_exists("to-delete").await.unwrap());

        store.delete_bucket("to-delete").await.unwrap();
        assert!(!store.bucket_exists("to-delete").await.unwrap());
    }

    #[tokio::test]
    async fn test_update_bucket_acl() {
        let store = test_store();
        store
            .create_bucket(make_bucket("acl-bucket"))
            .await
            .unwrap();

        let new_acl = r#"{"owner":{"id":"test","display_name":"test"},"grants":[]}"#;
        store
            .update_bucket_acl("acl-bucket", new_acl)
            .await
            .unwrap();

        let b = store.get_bucket("acl-bucket").await.unwrap().unwrap();
        assert_eq!(b.acl, new_acl);
    }

    #[tokio::test]
    async fn test_get_nonexistent_bucket() {
        let store = test_store();
        let result = store.get_bucket("no-such-bucket").await.unwrap();
        assert!(result.is_none());
    }

    // ── Object tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_put_and_get_object() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();

        let mut obj = make_object("mybucket", "hello.txt", 5);
        obj.content_type = "text/plain".to_string();
        obj.user_metadata
            .insert("x-amz-meta-author".to_string(), "tester".to_string());

        store.put_object(obj).await.unwrap();

        let fetched = store.get_object("mybucket", "hello.txt").await.unwrap();
        assert!(fetched.is_some());
        let o = fetched.unwrap();
        assert_eq!(o.key, "hello.txt");
        assert_eq!(o.size, 5);
        assert_eq!(o.content_type, "text/plain");
        assert_eq!(o.user_metadata.get("x-amz-meta-author").unwrap(), "tester");
    }

    #[tokio::test]
    async fn test_object_exists() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        assert!(!store.object_exists("mybucket", "key").await.unwrap());

        store
            .put_object(make_object("mybucket", "key", 10))
            .await
            .unwrap();
        assert!(store.object_exists("mybucket", "key").await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_object() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .put_object(make_object("mybucket", "key", 10))
            .await
            .unwrap();

        store.delete_object("mybucket", "key").await.unwrap();
        assert!(!store.object_exists("mybucket", "key").await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_objects_batch() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .put_object(make_object("mybucket", "a", 1))
            .await
            .unwrap();
        store
            .put_object(make_object("mybucket", "b", 2))
            .await
            .unwrap();
        store
            .put_object(make_object("mybucket", "c", 3))
            .await
            .unwrap();

        let deleted = store
            .delete_objects("mybucket", &["a".to_string(), "b".to_string()])
            .await
            .unwrap();
        assert_eq!(deleted.len(), 2);
        assert!(!store.object_exists("mybucket", "a").await.unwrap());
        assert!(!store.object_exists("mybucket", "b").await.unwrap());
        assert!(store.object_exists("mybucket", "c").await.unwrap());
    }

    #[tokio::test]
    async fn test_count_objects() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        assert_eq!(store.count_objects("mybucket").await.unwrap(), 0);

        store
            .put_object(make_object("mybucket", "a", 1))
            .await
            .unwrap();
        store
            .put_object(make_object("mybucket", "b", 2))
            .await
            .unwrap();
        assert_eq!(store.count_objects("mybucket").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_update_object_acl() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .put_object(make_object("mybucket", "key", 10))
            .await
            .unwrap();

        let new_acl = r#"{"owner":{"id":"o","display_name":"o"},"grants":[]}"#;
        store
            .update_object_acl("mybucket", "key", new_acl)
            .await
            .unwrap();

        let obj = store.get_object("mybucket", "key").await.unwrap().unwrap();
        assert_eq!(obj.acl, new_acl);
    }

    #[tokio::test]
    async fn test_put_object_upsert() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .put_object(make_object("mybucket", "key", 10))
            .await
            .unwrap();

        // Overwrite with a bigger object.
        store
            .put_object(make_object("mybucket", "key", 20))
            .await
            .unwrap();

        let obj = store.get_object("mybucket", "key").await.unwrap().unwrap();
        assert_eq!(obj.size, 20);
    }

    // ── List objects tests ──────────────────────────────────────────

    #[tokio::test]
    async fn test_list_objects_basic() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .put_object(make_object("mybucket", "a.txt", 1))
            .await
            .unwrap();
        store
            .put_object(make_object("mybucket", "b.txt", 2))
            .await
            .unwrap();
        store
            .put_object(make_object("mybucket", "c.txt", 3))
            .await
            .unwrap();

        let result = store
            .list_objects("mybucket", "", "", 1000, "", None)
            .await
            .unwrap();
        assert_eq!(result.objects.len(), 3);
        assert!(!result.is_truncated);
        assert!(result.next_continuation_token.is_none());
    }

    #[tokio::test]
    async fn test_list_objects_with_prefix() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .put_object(make_object("mybucket", "docs/a.txt", 1))
            .await
            .unwrap();
        store
            .put_object(make_object("mybucket", "docs/b.txt", 2))
            .await
            .unwrap();
        store
            .put_object(make_object("mybucket", "images/c.png", 3))
            .await
            .unwrap();

        let result = store
            .list_objects("mybucket", "docs/", "", 1000, "", None)
            .await
            .unwrap();
        assert_eq!(result.objects.len(), 2);
        assert!(result.objects.iter().all(|o| o.key.starts_with("docs/")));
    }

    #[tokio::test]
    async fn test_list_objects_with_delimiter() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .put_object(make_object("mybucket", "docs/a.txt", 1))
            .await
            .unwrap();
        store
            .put_object(make_object("mybucket", "docs/b.txt", 2))
            .await
            .unwrap();
        store
            .put_object(make_object("mybucket", "images/c.png", 3))
            .await
            .unwrap();
        store
            .put_object(make_object("mybucket", "root.txt", 4))
            .await
            .unwrap();

        let result = store
            .list_objects("mybucket", "", "/", 1000, "", None)
            .await
            .unwrap();
        // root.txt should be a direct object.
        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].key, "root.txt");
        // docs/ and images/ should be common prefixes.
        assert_eq!(result.common_prefixes.len(), 2);
        assert!(result.common_prefixes.contains(&"docs/".to_string()));
        assert!(result.common_prefixes.contains(&"images/".to_string()));
    }

    #[tokio::test]
    async fn test_list_objects_pagination() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .put_object(make_object("mybucket", "a", 1))
            .await
            .unwrap();
        store
            .put_object(make_object("mybucket", "b", 2))
            .await
            .unwrap();
        store
            .put_object(make_object("mybucket", "c", 3))
            .await
            .unwrap();

        // Page 1: max_keys=2
        let result = store
            .list_objects("mybucket", "", "", 2, "", None)
            .await
            .unwrap();
        assert_eq!(result.objects.len(), 2);
        assert!(result.is_truncated);
        assert!(result.next_continuation_token.is_some());

        // Page 2: continuation token
        let result2 = store
            .list_objects(
                "mybucket",
                "",
                "",
                2,
                "",
                result.next_continuation_token.as_deref(),
            )
            .await
            .unwrap();
        assert_eq!(result2.objects.len(), 1);
        assert!(!result2.is_truncated);
    }

    #[tokio::test]
    async fn test_list_objects_empty_bucket() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();

        let result = store
            .list_objects("mybucket", "", "", 1000, "", None)
            .await
            .unwrap();
        assert_eq!(result.objects.len(), 0);
        assert!(!result.is_truncated);
    }

    #[tokio::test]
    async fn test_list_objects_start_after() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .put_object(make_object("mybucket", "a", 1))
            .await
            .unwrap();
        store
            .put_object(make_object("mybucket", "b", 2))
            .await
            .unwrap();
        store
            .put_object(make_object("mybucket", "c", 3))
            .await
            .unwrap();

        let result = store
            .list_objects("mybucket", "", "", 1000, "a", None)
            .await
            .unwrap();
        assert_eq!(result.objects.len(), 2);
        assert_eq!(result.objects[0].key, "b");
        assert_eq!(result.objects[1].key, "c");
    }

    // ── Multipart upload tests ──────────────────────────────────────

    fn make_upload(bucket: &str, key: &str, upload_id: &str) -> MultipartUploadRecord {
        MultipartUploadRecord {
            upload_id: upload_id.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: "application/octet-stream".to_string(),
            content_encoding: None,
            content_language: None,
            content_disposition: None,
            cache_control: None,
            expires: None,
            storage_class: "STANDARD".to_string(),
            acl: "{}".to_string(),
            user_metadata: HashMap::new(),
            owner_id: "test-owner".to_string(),
            owner_display: "Test Owner".to_string(),
            initiated_at: "2026-02-23T00:00:00.000Z".to_string(),
        }
    }

    #[tokio::test]
    async fn test_create_and_get_multipart_upload() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();

        let upload = make_upload("mybucket", "big-file.dat", "upload-001");
        store.create_multipart_upload(upload).await.unwrap();

        let fetched = store.get_multipart_upload("upload-001").await.unwrap();
        assert!(fetched.is_some());
        let u = fetched.unwrap();
        assert_eq!(u.upload_id, "upload-001");
        assert_eq!(u.bucket, "mybucket");
        assert_eq!(u.key, "big-file.dat");
    }

    #[tokio::test]
    async fn test_put_and_list_parts() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .create_multipart_upload(make_upload("mybucket", "key", "upload-002"))
            .await
            .unwrap();

        let part1 = PartRecord {
            part_number: 1,
            size: 5_000_000,
            etag: "\"etag1\"".to_string(),
            last_modified: "2026-02-23T00:01:00.000Z".to_string(),
        };
        let part2 = PartRecord {
            part_number: 2,
            size: 3_000_000,
            etag: "\"etag2\"".to_string(),
            last_modified: "2026-02-23T00:02:00.000Z".to_string(),
        };

        store.put_part("upload-002", part1).await.unwrap();
        store.put_part("upload-002", part2).await.unwrap();

        let result = store.list_parts("upload-002", 100, 0).await.unwrap();
        assert_eq!(result.parts.len(), 2);
        assert_eq!(result.parts[0].part_number, 1);
        assert_eq!(result.parts[1].part_number, 2);
        assert!(!result.is_truncated);
    }

    #[tokio::test]
    async fn test_get_parts_for_completion() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .create_multipart_upload(make_upload("mybucket", "key", "upload-003"))
            .await
            .unwrap();

        for i in 1..=3 {
            store
                .put_part(
                    "upload-003",
                    PartRecord {
                        part_number: i,
                        size: 5_000_000,
                        etag: format!("\"etag{i}\""),
                        last_modified: "2026-02-23T00:00:00.000Z".to_string(),
                    },
                )
                .await
                .unwrap();
        }

        let parts = store.get_parts_for_completion("upload-003").await.unwrap();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[1].part_number, 2);
        assert_eq!(parts[2].part_number, 3);
    }

    #[tokio::test]
    async fn test_complete_multipart_upload_transactional() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .create_multipart_upload(make_upload("mybucket", "final.dat", "upload-004"))
            .await
            .unwrap();

        store
            .put_part(
                "upload-004",
                PartRecord {
                    part_number: 1,
                    size: 5_000_000,
                    etag: "\"etag1\"".to_string(),
                    last_modified: "2026-02-23T00:00:00.000Z".to_string(),
                },
            )
            .await
            .unwrap();

        let final_obj = ObjectRecord {
            bucket: "mybucket".to_string(),
            key: "final.dat".to_string(),
            size: 5_000_000,
            etag: "\"composite-etag-1\"".to_string(),
            content_type: "application/octet-stream".to_string(),
            content_encoding: None,
            content_language: None,
            content_disposition: None,
            cache_control: None,
            expires: None,
            storage_class: "STANDARD".to_string(),
            acl: "{}".to_string(),
            last_modified: "2026-02-23T00:05:00.000Z".to_string(),
            user_metadata: HashMap::new(),
            delete_marker: false,
        };

        store
            .complete_multipart_upload("upload-004", final_obj)
            .await
            .unwrap();

        // Object should now exist.
        let obj = store.get_object("mybucket", "final.dat").await.unwrap();
        assert!(obj.is_some());
        assert_eq!(obj.unwrap().etag, "\"composite-etag-1\"");

        // Upload should be gone.
        let upload = store.get_multipart_upload("upload-004").await.unwrap();
        assert!(upload.is_none());

        // Parts should be gone.
        let parts = store.get_parts_for_completion("upload-004").await.unwrap();
        assert!(parts.is_empty());
    }

    #[tokio::test]
    async fn test_delete_multipart_upload() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .create_multipart_upload(make_upload("mybucket", "key", "upload-005"))
            .await
            .unwrap();
        store
            .put_part(
                "upload-005",
                PartRecord {
                    part_number: 1,
                    size: 100,
                    etag: "\"e\"".to_string(),
                    last_modified: "2026-02-23T00:00:00.000Z".to_string(),
                },
            )
            .await
            .unwrap();

        store.delete_multipart_upload("upload-005").await.unwrap();

        let upload = store.get_multipart_upload("upload-005").await.unwrap();
        assert!(upload.is_none());

        // Parts should be cascade-deleted.
        let parts = store.get_parts_for_completion("upload-005").await.unwrap();
        assert!(parts.is_empty());
    }

    #[tokio::test]
    async fn test_list_multipart_uploads() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .create_multipart_upload(make_upload("mybucket", "a.dat", "u1"))
            .await
            .unwrap();
        store
            .create_multipart_upload(make_upload("mybucket", "b.dat", "u2"))
            .await
            .unwrap();

        let result = store
            .list_multipart_uploads("mybucket", "", 100, "", "")
            .await
            .unwrap();
        assert_eq!(result.uploads.len(), 2);
        assert!(!result.is_truncated);
    }

    #[tokio::test]
    async fn test_list_parts_pagination() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .create_multipart_upload(make_upload("mybucket", "key", "upload-006"))
            .await
            .unwrap();

        for i in 1..=5 {
            store
                .put_part(
                    "upload-006",
                    PartRecord {
                        part_number: i,
                        size: 1000,
                        etag: format!("\"e{i}\""),
                        last_modified: "2026-02-23T00:00:00.000Z".to_string(),
                    },
                )
                .await
                .unwrap();
        }

        // Page 1: max_parts=2.
        let page1 = store.list_parts("upload-006", 2, 0).await.unwrap();
        assert_eq!(page1.parts.len(), 2);
        assert!(page1.is_truncated);
        assert_eq!(page1.next_part_number_marker, Some(2));

        // Page 2.
        let page2 = store
            .list_parts("upload-006", 2, page1.next_part_number_marker.unwrap())
            .await
            .unwrap();
        assert_eq!(page2.parts.len(), 2);
        assert!(page2.is_truncated);

        // Page 3.
        let page3 = store
            .list_parts("upload-006", 2, page2.next_part_number_marker.unwrap())
            .await
            .unwrap();
        assert_eq!(page3.parts.len(), 1);
        assert!(!page3.is_truncated);
    }

    // ── Credential tests ────────────────────────────────────────────

    #[tokio::test]
    async fn test_seed_and_get_credential() {
        let store = test_store();
        store.seed_credential("mykey", "mysecret").unwrap();

        let cred = store.get_credential("mykey").await.unwrap();
        assert!(cred.is_some());
        let c = cred.unwrap();
        assert_eq!(c.access_key_id, "mykey");
        assert_eq!(c.secret_key, "mysecret");
        assert!(c.active);
    }

    #[tokio::test]
    async fn test_seed_credential_idempotent() {
        let store = test_store();
        store.seed_credential("mykey", "mysecret").unwrap();
        // Call again -- should not fail or duplicate.
        store.seed_credential("mykey", "mysecret").unwrap();

        let cred = store.get_credential("mykey").await.unwrap();
        assert!(cred.is_some());
    }

    #[tokio::test]
    async fn test_put_credential() {
        let store = test_store();
        let cred = CredentialRecord {
            access_key_id: "ak1".to_string(),
            secret_key: "sk1".to_string(),
            owner_id: "owner1".to_string(),
            display_name: "Owner One".to_string(),
            active: true,
            created_at: "2026-02-23T00:00:00.000Z".to_string(),
        };
        store.put_credential(cred).await.unwrap();

        let fetched = store.get_credential("ak1").await.unwrap();
        assert!(fetched.is_some());
        let c = fetched.unwrap();
        assert_eq!(c.owner_id, "owner1");
        assert_eq!(c.display_name, "Owner One");
    }

    #[tokio::test]
    async fn test_get_nonexistent_credential() {
        let store = test_store();
        let result = store.get_credential("no-such-key").await.unwrap();
        assert!(result.is_none());
    }

    // ── Timestamp helper tests ──────────────────────────────────────

    #[test]
    fn test_format_timestamp() {
        // Unix epoch = 0 -> 1970-01-01T00:00:00.000Z
        assert_eq!(format_timestamp(0, 0), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn test_format_timestamp_recent() {
        // 2026-02-23 roughly = 1771891200 seconds since epoch.
        // Just check it produces a valid-looking ISO string.
        let ts = format_timestamp(1771891200, 500);
        assert!(ts.starts_with("2026-02-"));
        assert!(ts.ends_with("Z"));
        assert!(ts.contains("T"));
    }

    #[test]
    fn test_chrono_now_format() {
        let now = chrono_now();
        // Should be ISO-8601 format.
        assert!(now.contains("T"));
        assert!(now.ends_with("Z"));
        // Should have milliseconds.
        assert!(now.contains("."));
    }

    // ── Object optional fields test ─────────────────────────────────

    #[tokio::test]
    async fn test_object_optional_fields() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();

        let obj = ObjectRecord {
            bucket: "mybucket".to_string(),
            key: "with-opts.txt".to_string(),
            size: 100,
            etag: "\"etag\"".to_string(),
            content_type: "text/plain".to_string(),
            content_encoding: Some("gzip".to_string()),
            content_language: Some("en-US".to_string()),
            content_disposition: Some("attachment".to_string()),
            cache_control: Some("max-age=3600".to_string()),
            expires: Some("Thu, 01 Dec 2026 16:00:00 GMT".to_string()),
            storage_class: "STANDARD".to_string(),
            acl: "{}".to_string(),
            last_modified: "2026-02-23T00:00:00.000Z".to_string(),
            user_metadata: HashMap::new(),
            delete_marker: false,
        };

        store.put_object(obj).await.unwrap();

        let fetched = store
            .get_object("mybucket", "with-opts.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(fetched.content_language.as_deref(), Some("en-US"));
        assert_eq!(fetched.content_disposition.as_deref(), Some("attachment"));
        assert_eq!(fetched.cache_control.as_deref(), Some("max-age=3600"));
        assert_eq!(
            fetched.expires.as_deref(),
            Some("Thu, 01 Dec 2026 16:00:00 GMT")
        );
    }
}
