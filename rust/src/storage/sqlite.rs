//! SQLite storage backend.
//!
//! Objects and multipart parts are stored directly in a SQLite database.
//! This backend is useful for embedded/single-node deployments where all
//! data should live in a single file.
//!
//! Because `rusqlite::Connection` is `!Send`, we wrap it in a
//! `std::sync::Mutex` behind an `Arc` and use `tokio::task::spawn_blocking`
//! for every database operation.

use bytes::Bytes;
use md5::{Digest, Md5};
use rusqlite::{params, Connection};
use sha2::Sha256;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use super::backend::{StorageBackend, StoredObject};

/// SQLite-backed object and part storage.
///
/// All data (objects and multipart parts) is stored in two tables inside
/// a single SQLite database.  The connection is protected by a
/// `Mutex<Connection>` behind an `Arc`, making the struct `Send + Sync`.
pub struct SqliteBackend {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteBackend {
    /// Open (or create) a SQLite database at `db_path` and initialise
    /// the required tables.
    ///
    /// Configures WAL journal mode and a 5-second busy timeout for
    /// improved concurrent-read performance.
    pub fn new(db_path: &str) -> anyhow::Result<Self> {
        let conn = Connection::open(db_path)?;

        // Pragmas for performance and reliability.
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;\
             PRAGMA busy_timeout=5000;\
             PRAGMA synchronous=NORMAL;",
        )?;

        // Object data table — composite PK for cross-language identity.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS object_data (\
                 bucket TEXT NOT NULL,\
                 key    TEXT NOT NULL,\
                 data   BLOB NOT NULL,\
                 etag   TEXT NOT NULL,\
                 PRIMARY KEY (bucket, key)\
             );",
        )?;

        // Part data table — composite PK for cross-language identity.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS part_data (\
                 upload_id   TEXT NOT NULL,\
                 part_number INTEGER NOT NULL,\
                 data        BLOB NOT NULL,\
                 etag        TEXT NOT NULL,\
                 PRIMARY KEY (upload_id, part_number)\
             );",
        )?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    // ── Hash helpers ────────────────────────────────────────────────

    /// Compute the quoted MD5-hex ETag for a byte slice.
    fn compute_etag(data: &[u8]) -> String {
        let mut hasher = Md5::new();
        hasher.update(data);
        let md5_bytes = hasher.finalize();
        format!("\"{}\"", hex::encode(md5_bytes))
    }

    /// Compute the SHA-256 hex content hash for a byte slice.
    fn compute_content_hash(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash_bytes = hasher.finalize();
        hex::encode(hash_bytes)
    }
}

/// Split a `"bucket/key"` storage key into `(bucket, key)`.
///
/// The bucket is everything before the first `/`; the key is the rest.
fn split_storage_key(storage_key: &str) -> (String, String) {
    match storage_key.find('/') {
        Some(pos) => (
            storage_key[..pos].to_string(),
            storage_key[pos + 1..].to_string(),
        ),
        None => (storage_key.to_string(), String::new()),
    }
}

// ── StorageBackend implementation ──────────────────────────────────────

impl StorageBackend for SqliteBackend {
    fn put(
        &self,
        storage_key: &str,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>> {
        // Split "bucket/key" into separate columns.
        let (bucket, key) = split_storage_key(storage_key);
        let data = data.clone();
        let conn = Arc::clone(&self.conn);
        Box::pin(async move {
            let etag = Self::compute_etag(&data);
            let data_vec: Vec<u8> = data.to_vec();
            let etag_clone = etag.clone();
            tokio::task::spawn_blocking(move || {
                let conn = conn.lock().map_err(|e| anyhow::anyhow!("Mutex poisoned: {e}"))?;
                conn.execute(
                    "INSERT OR REPLACE INTO object_data (bucket, key, data, etag) VALUES (?1, ?2, ?3, ?4)",
                    params![bucket, key, data_vec, etag_clone],
                )?;
                Ok::<(), anyhow::Error>(())
            })
            .await??;
            Ok(etag)
        })
    }

    fn get(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<StoredObject>> + Send + '_>> {
        let (bucket, key) = split_storage_key(storage_key);
        let conn = Arc::clone(&self.conn);
        Box::pin(async move {
            let result = tokio::task::spawn_blocking(move || {
                let conn = conn
                    .lock()
                    .map_err(|e| anyhow::anyhow!("Mutex poisoned: {e}"))?;
                let mut stmt =
                    conn.prepare("SELECT data FROM object_data WHERE bucket = ?1 AND key = ?2")?;
                let row = stmt.query_row(params![bucket, key], |row| {
                    let data: Vec<u8> = row.get(0)?;
                    Ok(data)
                });
                match row {
                    Ok(data) => Ok(data),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Err(anyhow::anyhow!(
                        "Object not found at storage key: {bucket}/{key}"
                    )),
                    Err(e) => Err(anyhow::anyhow!(e)),
                }
            })
            .await??;

            let data = Bytes::from(result);
            let content_hash = Self::compute_content_hash(&data);
            Ok(StoredObject { data, content_hash })
        })
    }

    fn delete(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let (bucket, key) = split_storage_key(storage_key);
        let conn = Arc::clone(&self.conn);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let conn = conn
                    .lock()
                    .map_err(|e| anyhow::anyhow!("Mutex poisoned: {e}"))?;
                conn.execute(
                    "DELETE FROM object_data WHERE bucket = ?1 AND key = ?2",
                    params![bucket, key],
                )?;
                Ok::<(), anyhow::Error>(())
            })
            .await??;
            Ok(())
        })
    }

    fn exists(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>> {
        let (bucket, key) = split_storage_key(storage_key);
        let conn = Arc::clone(&self.conn);
        Box::pin(async move {
            let exists = tokio::task::spawn_blocking(move || {
                let conn = conn
                    .lock()
                    .map_err(|e| anyhow::anyhow!("Mutex poisoned: {e}"))?;
                let mut stmt =
                    conn.prepare("SELECT 1 FROM object_data WHERE bucket = ?1 AND key = ?2 LIMIT 1")?;
                let found = stmt.exists(params![bucket, key])?;
                Ok::<bool, anyhow::Error>(found)
            })
            .await??;
            Ok(exists)
        })
    }

    fn copy_object(
        &self,
        bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>> {
        let src_bucket = bucket.to_string();
        let src_key = src_key.to_string();
        let dst_bucket = dst_bucket.to_string();
        let dst_key = dst_key.to_string();
        let conn = Arc::clone(&self.conn);
        Box::pin(async move {
            let etag = tokio::task::spawn_blocking(move || {
                let conn = conn.lock().map_err(|e| anyhow::anyhow!("Mutex poisoned: {e}"))?;

                // Read source object.
                let mut stmt = conn.prepare(
                    "SELECT data, etag FROM object_data WHERE bucket = ?1 AND key = ?2",
                )?;
                let (data, etag): (Vec<u8>, String) = stmt
                    .query_row(params![src_bucket, src_key], |row| {
                        let data: Vec<u8> = row.get(0)?;
                        let etag: String = row.get(1)?;
                        Ok((data, etag))
                    })
                    .map_err(|e| match e {
                        rusqlite::Error::QueryReturnedNoRows => {
                            anyhow::anyhow!("Source object not found: {src_bucket}/{src_key}")
                        }
                        other => anyhow::anyhow!(other),
                    })?;

                // Insert destination object.
                conn.execute(
                    "INSERT OR REPLACE INTO object_data (bucket, key, data, etag) VALUES (?1, ?2, ?3, ?4)",
                    params![dst_bucket, dst_key, data, etag],
                )?;

                Ok::<String, anyhow::Error>(etag)
            })
            .await??;
            Ok(etag)
        })
    }

    fn put_part(
        &self,
        _bucket: &str,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        let data = data.clone();
        let conn = Arc::clone(&self.conn);
        Box::pin(async move {
            let etag = Self::compute_etag(&data);
            let data_vec: Vec<u8> = data.to_vec();
            let etag_clone = etag.clone();
            tokio::task::spawn_blocking(move || {
                let conn = conn
                    .lock()
                    .map_err(|e| anyhow::anyhow!("Mutex poisoned: {e}"))?;
                conn.execute(
                    "INSERT OR REPLACE INTO part_data (upload_id, part_number, data, etag) VALUES (?1, ?2, ?3, ?4)",
                    params![upload_id, part_number, data_vec, etag_clone],
                )?;
                Ok::<(), anyhow::Error>(())
            })
            .await??;
            Ok(etag)
        })
    }

    fn assemble_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(u32, String)],
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        let parts = parts.to_vec();
        let conn = Arc::clone(&self.conn);
        Box::pin(async move {
            let result = tokio::task::spawn_blocking(move || {
                let conn = conn.lock().map_err(|e| anyhow::anyhow!("Mutex poisoned: {e}"))?;

                let mut combined_data: Vec<u8> = Vec::new();
                let mut combined_md5_bytes: Vec<u8> = Vec::new();

                let mut stmt = conn.prepare(
                    "SELECT data FROM part_data WHERE upload_id = ?1 AND part_number = ?2",
                )?;

                for (part_number, _etag) in &parts {
                    let part_data: Vec<u8> = stmt
                        .query_row(params![upload_id, part_number], |row| {
                            let data: Vec<u8> = row.get(0)?;
                            Ok(data)
                        })
                        .map_err(|e| match e {
                            rusqlite::Error::QueryReturnedNoRows => {
                                anyhow::anyhow!("Part not found: {upload_id}/{part_number}")
                            }
                            other => anyhow::anyhow!(other),
                        })?;

                    // Compute MD5 of this part for composite ETag.
                    let mut part_hasher = Md5::new();
                    part_hasher.update(&part_data);
                    let part_md5 = part_hasher.finalize();
                    combined_md5_bytes.extend_from_slice(&part_md5);

                    combined_data.extend_from_slice(&part_data);
                }

                // Compute composite ETag: MD5 of concatenated binary MD5s + "-{part_count}"
                let mut composite_hasher = Md5::new();
                composite_hasher.update(&combined_md5_bytes);
                let composite_md5 = composite_hasher.finalize();
                let composite_etag =
                    format!("\"{}-{}\"", hex::encode(composite_md5), parts.len());

                // Store the assembled object.
                conn.execute(
                    "INSERT OR REPLACE INTO object_data (bucket, key, data, etag) VALUES (?1, ?2, ?3, ?4)",
                    params![bucket, key, combined_data, composite_etag],
                )?;

                Ok::<String, anyhow::Error>(composite_etag)
            })
            .await??;
            Ok(result)
        })
    }

    fn delete_parts(
        &self,
        _bucket: &str,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        let conn = Arc::clone(&self.conn);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let conn = conn
                    .lock()
                    .map_err(|e| anyhow::anyhow!("Mutex poisoned: {e}"))?;
                conn.execute(
                    "DELETE FROM part_data WHERE upload_id = ?1",
                    params![upload_id],
                )?;
                Ok::<(), anyhow::Error>(())
            })
            .await??;
            Ok(())
        })
    }

    fn create_bucket(
        &self,
        _bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        // Buckets are implicit in the storage key; nothing to create.
        Box::pin(async move { Ok(()) })
    }

    fn delete_bucket(
        &self,
        _bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        // Buckets are implicit in the storage key; nothing to delete.
        Box::pin(async move { Ok(()) })
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_backend() -> (tempfile::TempDir, SqliteBackend) {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let db_path = dir.path().join("test.db");
        let backend =
            SqliteBackend::new(db_path.to_str().unwrap()).expect("failed to create backend");
        (dir, backend)
    }

    #[tokio::test]
    async fn test_put_and_get_roundtrip() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        let data = Bytes::from("hello world");
        let etag = backend
            .put("test-bucket/key.txt", data.clone())
            .await
            .unwrap();

        assert!(etag.starts_with('"'));
        assert!(etag.ends_with('"'));

        let obj = backend.get("test-bucket/key.txt").await.unwrap();
        assert_eq!(obj.data, data);
        assert!(!obj.content_hash.is_empty());
    }

    #[tokio::test]
    async fn test_put_empty_object() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        let data = Bytes::new();
        let etag = backend
            .put("test-bucket/empty.txt", data.clone())
            .await
            .unwrap();
        assert!(etag.starts_with('"'));

        let obj = backend.get("test-bucket/empty.txt").await.unwrap();
        assert_eq!(obj.data.len(), 0);
    }

    #[tokio::test]
    async fn test_delete_existing() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        backend
            .put("test-bucket/key.txt", Bytes::from("data"))
            .await
            .unwrap();
        assert!(backend.exists("test-bucket/key.txt").await.unwrap());

        backend.delete("test-bucket/key.txt").await.unwrap();
        assert!(!backend.exists("test-bucket/key.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_is_ok() {
        let (_dir, backend) = test_backend();
        backend.delete("test-bucket/no-such-key").await.unwrap();
    }

    #[tokio::test]
    async fn test_exists() {
        let (_dir, backend) = test_backend();
        assert!(!backend.exists("test-bucket/key.txt").await.unwrap());

        backend
            .put("test-bucket/key.txt", Bytes::from("data"))
            .await
            .unwrap();
        assert!(backend.exists("test-bucket/key.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_get_nonexistent_returns_error() {
        let (_dir, backend) = test_backend();
        let result = backend.get("test-bucket/no-such-key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_overwrites() {
        let (_dir, backend) = test_backend();

        let etag1 = backend
            .put("test-bucket/key.txt", Bytes::from("version 1"))
            .await
            .unwrap();
        let etag2 = backend
            .put("test-bucket/key.txt", Bytes::from("version 2"))
            .await
            .unwrap();

        assert_ne!(etag1, etag2);

        let obj = backend.get("test-bucket/key.txt").await.unwrap();
        assert_eq!(obj.data, Bytes::from("version 2"));
    }

    #[tokio::test]
    async fn test_etag_is_md5() {
        let (_dir, backend) = test_backend();

        // Known MD5 of empty string: d41d8cd98f00b204e9800998ecf8427e
        let etag = backend
            .put("test-bucket/empty", Bytes::new())
            .await
            .unwrap();
        assert_eq!(etag, "\"d41d8cd98f00b204e9800998ecf8427e\"");
    }

    #[tokio::test]
    async fn test_create_and_delete_bucket_noop() {
        let (_dir, backend) = test_backend();
        // create_bucket and delete_bucket are no-ops for SQLite backend.
        backend.create_bucket("mybucket").await.unwrap();
        backend.delete_bucket("mybucket").await.unwrap();
    }

    #[tokio::test]
    async fn test_copy_object_same_bucket() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        let data = Bytes::from("copy me");
        let src_etag = backend
            .put("test-bucket/original.txt", data.clone())
            .await
            .unwrap();

        let dst_etag = backend
            .copy_object("test-bucket", "original.txt", "test-bucket", "copy.txt")
            .await
            .unwrap();

        assert_eq!(src_etag, dst_etag);

        let obj = backend.get("test-bucket/copy.txt").await.unwrap();
        assert_eq!(obj.data, data);

        assert!(backend.exists("test-bucket/original.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_copy_object_different_buckets() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("src-bucket").await.unwrap();
        backend.create_bucket("dst-bucket").await.unwrap();

        let data = Bytes::from("cross-bucket copy");
        backend
            .put("src-bucket/key.txt", data.clone())
            .await
            .unwrap();

        let etag = backend
            .copy_object("src-bucket", "key.txt", "dst-bucket", "key.txt")
            .await
            .unwrap();

        assert!(etag.starts_with('"'));
        let obj = backend.get("dst-bucket/key.txt").await.unwrap();
        assert_eq!(obj.data, data);
    }

    #[tokio::test]
    async fn test_copy_object_nonexistent_source() {
        let (_dir, backend) = test_backend();

        let result = backend
            .copy_object("test-bucket", "no-such-key", "test-bucket", "dest.txt")
            .await;
        assert!(result.is_err());
    }

    // -- Multipart part storage tests -----------------------------------------

    #[tokio::test]
    async fn test_put_part_and_verify() {
        let (_dir, backend) = test_backend();

        let data = Bytes::from("part data here");
        let etag = backend
            .put_part("test-bucket", "upload-001", 1, data.clone())
            .await
            .unwrap();

        assert!(etag.starts_with('"'));
        assert!(etag.ends_with('"'));
    }

    #[tokio::test]
    async fn test_put_part_overwrites() {
        let (_dir, backend) = test_backend();

        let etag1 = backend
            .put_part("test-bucket", "upload-001", 1, Bytes::from("v1"))
            .await
            .unwrap();
        let etag2 = backend
            .put_part("test-bucket", "upload-001", 1, Bytes::from("v2"))
            .await
            .unwrap();

        assert_ne!(etag1, etag2);
    }

    #[tokio::test]
    async fn test_put_multiple_parts() {
        let (_dir, backend) = test_backend();

        backend
            .put_part("test-bucket", "upload-001", 1, Bytes::from("part1"))
            .await
            .unwrap();
        backend
            .put_part("test-bucket", "upload-001", 2, Bytes::from("part2"))
            .await
            .unwrap();
        backend
            .put_part("test-bucket", "upload-001", 3, Bytes::from("part3"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_delete_parts() {
        let (_dir, backend) = test_backend();

        backend
            .put_part("test-bucket", "upload-001", 1, Bytes::from("part1"))
            .await
            .unwrap();
        backend
            .put_part("test-bucket", "upload-001", 2, Bytes::from("part2"))
            .await
            .unwrap();

        backend
            .delete_parts("test-bucket", "upload-001")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_delete_parts_nonexistent_is_ok() {
        let (_dir, backend) = test_backend();

        backend
            .delete_parts("test-bucket", "no-such-upload")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_put_part_etag_is_md5() {
        let (_dir, backend) = test_backend();

        let etag = backend
            .put_part("test-bucket", "upload-001", 1, Bytes::new())
            .await
            .unwrap();
        assert_eq!(etag, "\"d41d8cd98f00b204e9800998ecf8427e\"");
    }

    // -- assemble_parts tests ------------------------------------------------

    #[tokio::test]
    async fn test_assemble_parts_basic() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        let etag1 = backend
            .put_part("test-bucket", "upload-001", 1, Bytes::from("hello "))
            .await
            .unwrap();
        let etag2 = backend
            .put_part("test-bucket", "upload-001", 2, Bytes::from("world"))
            .await
            .unwrap();

        let parts = vec![(1u32, etag1.clone()), (2u32, etag2.clone())];
        let composite_etag = backend
            .assemble_parts("test-bucket", "assembled.txt", "upload-001", &parts)
            .await
            .unwrap();

        assert!(composite_etag.starts_with('"'));
        assert!(composite_etag.ends_with("-2\""));

        let obj = backend.get("test-bucket/assembled.txt").await.unwrap();
        assert_eq!(obj.data, Bytes::from("hello world"));
    }

    #[tokio::test]
    async fn test_assemble_parts_single_part() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        let etag1 = backend
            .put_part("test-bucket", "upload-002", 1, Bytes::from("solo"))
            .await
            .unwrap();

        let parts = vec![(1u32, etag1.clone())];
        let composite_etag = backend
            .assemble_parts("test-bucket", "single.txt", "upload-002", &parts)
            .await
            .unwrap();

        assert!(composite_etag.ends_with("-1\""));

        let obj = backend.get("test-bucket/single.txt").await.unwrap();
        assert_eq!(obj.data, Bytes::from("solo"));
    }

    #[tokio::test]
    async fn test_assemble_parts_composite_etag_format() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        let etag1 = backend
            .put_part("test-bucket", "upload-004", 1, Bytes::from("aaa"))
            .await
            .unwrap();
        let etag2 = backend
            .put_part("test-bucket", "upload-004", 2, Bytes::from("bbb"))
            .await
            .unwrap();
        let etag3 = backend
            .put_part("test-bucket", "upload-004", 3, Bytes::from("ccc"))
            .await
            .unwrap();

        let parts = vec![
            (1u32, etag1.clone()),
            (2u32, etag2.clone()),
            (3u32, etag3.clone()),
        ];
        let composite_etag = backend
            .assemble_parts("test-bucket", "three.txt", "upload-004", &parts)
            .await
            .unwrap();

        assert!(composite_etag.starts_with('"'));
        assert!(composite_etag.ends_with("-3\""));
        let inner = composite_etag.trim_matches('"');
        let dash_pos = inner.rfind('-').unwrap();
        let hex_part = &inner[..dash_pos];
        assert_eq!(hex_part.len(), 32);
        assert_eq!(&inner[dash_pos + 1..], "3");
    }
}
