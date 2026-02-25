//! In-memory storage backend with optional SQLite snapshot persistence.
//!
//! Objects and multipart parts are held in `tokio::sync::RwLock<HashMap<...>>`
//! maps.  An optional snapshot mechanism periodically serialises the full
//! in-memory state to a SQLite database so that data can survive restarts.
//!
//! A configurable memory limit (`max_size_bytes`) caps total stored bytes.

use bytes::Bytes;
use md5::{Digest, Md5};
use rusqlite::{params, Connection};
use sha2::Sha256;
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::backend::{StorageBackend, StoredObject};

/// In-memory storage backend.
///
/// Stores all object and part data in hash maps protected by async
/// read-write locks.  Optionally snapshots state to a SQLite file on a
/// configurable interval and at shutdown.
pub struct MemoryBackend {
    /// Main object store: storage_key -> (data, etag).
    objects: tokio::sync::RwLock<HashMap<String, (Bytes, String)>>,
    /// Part store: "upload_id/part_number" -> (data, etag).
    parts: tokio::sync::RwLock<HashMap<String, (Bytes, String)>>,
    /// Current total bytes stored (objects + parts).
    current_size: tokio::sync::RwLock<u64>,
    /// Maximum bytes allowed.  0 means unlimited.
    max_size_bytes: u64,
    /// Persistence mode: "none" or "snapshot".
    persistence: String,
    /// Path to the snapshot SQLite file.
    snapshot_path: PathBuf,
    /// Interval (in seconds) between background snapshots.
    snapshot_interval_seconds: u64,
    /// Flag used to signal the background snapshot task to stop.
    shutdown: Arc<AtomicBool>,
}

impl MemoryBackend {
    /// Create a new `MemoryBackend`.
    ///
    /// If `persistence` is `"snapshot"` and a snapshot file exists at
    /// `snapshot_path`, the previous state is restored from it.
    ///
    /// After construction, call [`start_background_snapshot`] to enable
    /// periodic persistence (if desired).
    pub fn new(
        max_size_bytes: u64,
        persistence: &str,
        snapshot_path: &str,
        snapshot_interval_seconds: u64,
    ) -> anyhow::Result<Self> {
        // If a snapshot exists, load data into plain collections first,
        // then move them into the RwLocks.  This avoids calling
        // `blocking_write()` on a tokio lock (which panics if called
        // from within a tokio runtime context).
        let (objects_map, parts_map, total_size) =
            if persistence == "snapshot" && Path::new(snapshot_path).exists() {
                Self::read_snapshot_into_maps(snapshot_path)?
            } else {
                (HashMap::new(), HashMap::new(), 0u64)
            };

        let backend = Self {
            objects: tokio::sync::RwLock::new(objects_map),
            parts: tokio::sync::RwLock::new(parts_map),
            current_size: tokio::sync::RwLock::new(total_size),
            max_size_bytes,
            persistence: persistence.to_string(),
            snapshot_path: PathBuf::from(snapshot_path),
            snapshot_interval_seconds,
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        if total_size > 0 {
            tracing::info!("Loaded snapshot from {:?}", snapshot_path);
        }

        Ok(backend)
    }

    // ── ETag helpers ────────────────────────────────────────────────

    /// Compute the quoted MD5-hex ETag for a byte slice.
    fn compute_etag(data: &[u8]) -> String {
        let mut hasher = Md5::new();
        hasher.update(data);
        let md5_bytes = hasher.finalize();
        format!("\"{}\"", hex::encode(md5_bytes))
    }

    /// Compute the SHA-256 content hash for a byte slice.
    fn compute_content_hash(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash_bytes = hasher.finalize();
        hex::encode(hash_bytes)
    }

    // ── Memory accounting ──────────────────────────────────────────

    /// Check whether adding `additional` bytes would exceed the memory limit.
    /// If the limit is 0 (unlimited), always returns Ok.
    async fn check_capacity(&self, additional: u64) -> anyhow::Result<()> {
        if self.max_size_bytes == 0 {
            return Ok(());
        }
        let current = *self.current_size.read().await;
        if current + additional > self.max_size_bytes {
            anyhow::bail!(
                "Memory limit exceeded: current={current}, additional={additional}, max={}",
                self.max_size_bytes
            );
        }
        Ok(())
    }

    /// Adjust the tracked size by a signed delta.
    async fn adjust_size(&self, delta: i64) {
        let mut size = self.current_size.write().await;
        if delta >= 0 {
            *size = size.saturating_add(delta as u64);
        } else {
            *size = size.saturating_sub((-delta) as u64);
        }
    }

    // ── Snapshot persistence ───────────────────────────────────────

    /// Write the current in-memory state to the snapshot SQLite file.
    ///
    /// Uses write-to-temp + rename for crash safety.
    pub fn snapshot(&self) -> anyhow::Result<()> {
        // We need synchronous access to the data, so we use try_read.
        // In the background task context the locks should normally be
        // available; if they are not we skip this snapshot cycle.
        let objects = match self.objects.try_read() {
            Ok(guard) => guard,
            Err(_) => {
                tracing::warn!("Snapshot skipped: could not acquire objects lock");
                return Ok(());
            }
        };
        let parts = match self.parts.try_read() {
            Ok(guard) => guard,
            Err(_) => {
                tracing::warn!("Snapshot skipped: could not acquire parts lock");
                return Ok(());
            }
        };

        let tmp_path = self.snapshot_path.with_extension("tmp");

        // Ensure parent directory exists.
        if let Some(parent) = tmp_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Remove stale temp file if present.
        let _ = std::fs::remove_file(&tmp_path);

        let conn = Connection::open(&tmp_path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS object_snapshots (
                 storage_key TEXT PRIMARY KEY,
                 data        BLOB NOT NULL,
                 etag        TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS part_snapshots (
                 part_key    TEXT PRIMARY KEY,
                 data        BLOB NOT NULL,
                 etag        TEXT NOT NULL
             );",
        )?;

        // Write objects.
        {
            let mut stmt = conn.prepare(
                "INSERT OR REPLACE INTO object_snapshots (storage_key, data, etag) VALUES (?1, ?2, ?3)",
            )?;
            for (key, (data, etag)) in objects.iter() {
                stmt.execute(params![key, data.as_ref(), etag])?;
            }
        }

        // Write parts.
        {
            let mut stmt = conn.prepare(
                "INSERT OR REPLACE INTO part_snapshots (part_key, data, etag) VALUES (?1, ?2, ?3)",
            )?;
            for (key, (data, etag)) in parts.iter() {
                stmt.execute(params![key, data.as_ref(), etag])?;
            }
        }

        drop(objects);
        drop(parts);

        // Ensure everything is flushed.
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        drop(conn);

        // Atomic rename.
        std::fs::rename(&tmp_path, &self.snapshot_path)?;
        // Also clean up the WAL/SHM files from the temp path if they linger.
        let _ = std::fs::remove_file(tmp_path.with_extension("tmp-wal"));
        let _ = std::fs::remove_file(tmp_path.with_extension("tmp-shm"));

        tracing::debug!("Snapshot written to {:?}", self.snapshot_path);
        Ok(())
    }

    /// Read a snapshot SQLite file into plain collections.
    ///
    /// Returns `(objects_map, parts_map, total_size)`.  This is a pure
    /// function that does not touch any `tokio::sync` primitives, so it is
    /// safe to call from both sync and async contexts.
    fn read_snapshot_into_maps(
        snapshot_path: &str,
    ) -> anyhow::Result<(HashMap<String, (Bytes, String)>, HashMap<String, (Bytes, String)>, u64)>
    {
        let conn = Connection::open(snapshot_path)?;
        let mut objects_map: HashMap<String, (Bytes, String)> = HashMap::new();
        let mut parts_map: HashMap<String, (Bytes, String)> = HashMap::new();
        let mut total_size: u64 = 0;

        // Load objects.
        {
            let mut stmt =
                conn.prepare("SELECT storage_key, data, etag FROM object_snapshots")?;
            let rows = stmt.query_map([], |row| {
                let key: String = row.get(0)?;
                let data: Vec<u8> = row.get(1)?;
                let etag: String = row.get(2)?;
                Ok((key, data, etag))
            })?;
            for row in rows {
                let (key, data, etag) = row?;
                total_size += data.len() as u64;
                objects_map.insert(key, (Bytes::from(data), etag));
            }
        }

        // Load parts.
        {
            let mut stmt =
                conn.prepare("SELECT part_key, data, etag FROM part_snapshots")?;
            let rows = stmt.query_map([], |row| {
                let key: String = row.get(0)?;
                let data: Vec<u8> = row.get(1)?;
                let etag: String = row.get(2)?;
                Ok((key, data, etag))
            })?;
            for row in rows {
                let (key, data, etag) = row?;
                total_size += data.len() as u64;
                parts_map.insert(key, (Bytes::from(data), etag));
            }
        }

        tracing::info!(
            "Read snapshot from {:?} ({} objects, {} parts, {} bytes)",
            snapshot_path,
            objects_map.len(),
            parts_map.len(),
            total_size,
        );

        Ok((objects_map, parts_map, total_size))
    }

    /// Load state from the snapshot SQLite file (async, for runtime use).
    pub async fn load_snapshot(&self) -> anyhow::Result<()> {
        let path = self.snapshot_path.to_str().unwrap_or("").to_string();

        // Read snapshot in a blocking task to avoid blocking the runtime.
        let (objects_map, parts_map, total_size) =
            tokio::task::spawn_blocking(move || Self::read_snapshot_into_maps(&path))
                .await??;

        let mut objects = self.objects.write().await;
        let mut parts = self.parts.write().await;
        let mut size = self.current_size.write().await;

        *objects = objects_map;
        *parts = parts_map;
        *size = total_size;

        Ok(())
    }

    /// Start a background tokio task that periodically writes snapshots.
    ///
    /// The task runs until [`close`] is called (which sets the shutdown flag).
    pub fn start_background_snapshot(self: &Arc<Self>) {
        if self.persistence != "snapshot" || self.snapshot_interval_seconds == 0 {
            return;
        }

        let backend = Arc::clone(self);
        let interval_secs = self.snapshot_interval_seconds;

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            // The first tick completes immediately; skip it.
            interval.tick().await;

            loop {
                interval.tick().await;
                if backend.shutdown.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(e) = backend.snapshot() {
                    tracing::error!("Background snapshot failed: {e}");
                }
            }
        });
    }

    /// Perform a final snapshot (if persistence is enabled) and signal the
    /// background task to stop.
    pub async fn close(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if self.persistence == "snapshot" {
            if let Err(e) = self.snapshot() {
                tracing::error!("Final snapshot on close failed: {e}");
            }
        }
    }
}

// ── StorageBackend implementation ──────────────────────────────────────

impl StorageBackend for MemoryBackend {
    fn put(
        &self,
        storage_key: &str,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        let data = data.clone();
        Box::pin(async move {
            let new_len = data.len() as u64;

            // If the key already exists, account for the size difference.
            let old_len = {
                let objects = self.objects.read().await;
                objects.get(&storage_key).map(|(d, _)| d.len() as u64)
            };

            let delta = new_len as i64 - old_len.unwrap_or(0) as i64;
            if delta > 0 {
                self.check_capacity(delta as u64).await?;
            }

            let etag = Self::compute_etag(&data);

            {
                let mut objects = self.objects.write().await;
                objects.insert(storage_key, (data, etag.clone()));
            }

            self.adjust_size(delta).await;

            Ok(etag)
        })
    }

    fn get(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<StoredObject>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let objects = self.objects.read().await;
            match objects.get(&storage_key) {
                Some((data, _etag)) => {
                    let content_hash = Self::compute_content_hash(data);
                    Ok(StoredObject {
                        data: data.clone(),
                        content_hash,
                    })
                }
                None => anyhow::bail!("Object not found at storage key: {storage_key}"),
            }
        })
    }

    fn delete(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let mut objects = self.objects.write().await;
            if let Some((data, _etag)) = objects.remove(&storage_key) {
                drop(objects);
                self.adjust_size(-(data.len() as i64)).await;
            }
            Ok(())
        })
    }

    fn exists(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let objects = self.objects.read().await;
            Ok(objects.contains_key(&storage_key))
        })
    }

    fn copy_object(
        &self,
        bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>> {
        let src_storage_key = format!("{bucket}/{src_key}");
        let dst_storage_key = format!("{dst_bucket}/{dst_key}");
        Box::pin(async move {
            let (data, etag) = {
                let objects = self.objects.read().await;
                match objects.get(&src_storage_key) {
                    Some((data, etag)) => (data.clone(), etag.clone()),
                    None => {
                        anyhow::bail!(
                            "Source object not found at storage key: {src_storage_key}"
                        );
                    }
                }
            };

            // Account for size change at destination.
            let new_len = data.len() as u64;
            let old_len = {
                let objects = self.objects.read().await;
                objects.get(&dst_storage_key).map(|(d, _)| d.len() as u64)
            };
            let delta = new_len as i64 - old_len.unwrap_or(0) as i64;
            if delta > 0 {
                self.check_capacity(delta as u64).await?;
            }

            {
                let mut objects = self.objects.write().await;
                objects.insert(dst_storage_key, (data, etag.clone()));
            }

            self.adjust_size(delta).await;

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
        let part_key = format!("{upload_id}/{part_number}");
        let data = data.clone();
        Box::pin(async move {
            let new_len = data.len() as u64;

            let old_len = {
                let parts = self.parts.read().await;
                parts.get(&part_key).map(|(d, _)| d.len() as u64)
            };

            let delta = new_len as i64 - old_len.unwrap_or(0) as i64;
            if delta > 0 {
                self.check_capacity(delta as u64).await?;
            }

            let etag = Self::compute_etag(&data);

            {
                let mut parts = self.parts.write().await;
                parts.insert(part_key, (data, etag.clone()));
            }

            self.adjust_size(delta).await;

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
        Box::pin(async move {
            let final_storage_key = format!("{bucket}/{key}");

            // Read all parts and concatenate.
            let mut combined_data: Vec<u8> = Vec::new();
            let mut combined_md5_bytes: Vec<u8> = Vec::new();

            {
                let parts_map = self.parts.read().await;
                for (part_number, _etag) in &parts {
                    let part_key = format!("{upload_id}/{part_number}");
                    let (part_data, _part_etag) = parts_map.get(&part_key).ok_or_else(|| {
                        anyhow::anyhow!("Part not found: {part_key}")
                    })?;

                    // Compute MD5 of this part for composite ETag.
                    let mut part_hasher = Md5::new();
                    part_hasher.update(part_data.as_ref());
                    let part_md5 = part_hasher.finalize();
                    combined_md5_bytes.extend_from_slice(&part_md5);

                    combined_data.extend_from_slice(part_data.as_ref());
                }
            }

            let assembled = Bytes::from(combined_data);

            // Compute composite ETag: MD5 of concatenated binary MD5s + "-{part_count}"
            let mut composite_hasher = Md5::new();
            composite_hasher.update(&combined_md5_bytes);
            let composite_md5 = composite_hasher.finalize();
            let composite_etag =
                format!("\"{}-{}\"", hex::encode(composite_md5), parts.len());

            // Store the assembled object (accounting for size).
            let new_len = assembled.len() as u64;
            let old_len = {
                let objects = self.objects.read().await;
                objects
                    .get(&final_storage_key)
                    .map(|(d, _)| d.len() as u64)
            };
            let delta = new_len as i64 - old_len.unwrap_or(0) as i64;
            if delta > 0 {
                self.check_capacity(delta as u64).await?;
            }

            {
                let mut objects = self.objects.write().await;
                objects.insert(
                    final_storage_key,
                    (assembled, composite_etag.clone()),
                );
            }

            self.adjust_size(delta).await;

            Ok(composite_etag)
        })
    }

    fn delete_parts(
        &self,
        _bucket: &str,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let prefix = format!("{upload_id}/");
            let mut parts = self.parts.write().await;

            // Collect keys to remove.
            let keys_to_remove: Vec<String> = parts
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();

            let mut freed: u64 = 0;
            for key in keys_to_remove {
                if let Some((data, _etag)) = parts.remove(&key) {
                    freed += data.len() as u64;
                }
            }

            drop(parts);

            if freed > 0 {
                self.adjust_size(-(freed as i64)).await;
            }

            Ok(())
        })
    }

    fn create_bucket(
        &self,
        _bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        // In-memory backend: buckets are implicit in the storage key.
        // Nothing to create.
        Box::pin(async move { Ok(()) })
    }

    fn delete_bucket(
        &self,
        bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let bucket = bucket.to_string();
        Box::pin(async move {
            let prefix = format!("{bucket}/");

            // Remove all objects with this bucket prefix.
            let mut objects = self.objects.write().await;
            let keys_to_remove: Vec<String> = objects
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();

            let mut freed: u64 = 0;
            for key in keys_to_remove {
                if let Some((data, _etag)) = objects.remove(&key) {
                    freed += data.len() as u64;
                }
            }
            drop(objects);

            if freed > 0 {
                self.adjust_size(-(freed as i64)).await;
            }

            Ok(())
        })
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_backend() -> MemoryBackend {
        MemoryBackend::new(0, "none", "", 0).expect("failed to create backend")
    }

    fn test_backend_with_limit(max_bytes: u64) -> MemoryBackend {
        MemoryBackend::new(max_bytes, "none", "", 0).expect("failed to create backend")
    }

    #[tokio::test]
    async fn test_put_and_get_roundtrip() {
        let backend = test_backend();
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
        let backend = test_backend();
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
        let backend = test_backend();
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
        let backend = test_backend();
        backend.delete("test-bucket/no-such-key").await.unwrap();
    }

    #[tokio::test]
    async fn test_exists() {
        let backend = test_backend();
        assert!(!backend.exists("test-bucket/key.txt").await.unwrap());

        backend
            .put("test-bucket/key.txt", Bytes::from("data"))
            .await
            .unwrap();
        assert!(backend.exists("test-bucket/key.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_get_nonexistent_returns_error() {
        let backend = test_backend();
        let result = backend.get("test-bucket/no-such-key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_overwrites() {
        let backend = test_backend();

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
        let backend = test_backend();

        // Known MD5 of empty string: d41d8cd98f00b204e9800998ecf8427e
        let etag = backend
            .put("test-bucket/empty", Bytes::new())
            .await
            .unwrap();
        assert_eq!(etag, "\"d41d8cd98f00b204e9800998ecf8427e\"");
    }

    #[tokio::test]
    async fn test_create_and_delete_bucket() {
        let backend = test_backend();
        backend.create_bucket("mybucket").await.unwrap();

        backend
            .put("mybucket/obj.txt", Bytes::from("data"))
            .await
            .unwrap();

        backend.delete_bucket("mybucket").await.unwrap();

        assert!(!backend.exists("mybucket/obj.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_copy_object_same_bucket() {
        let backend = test_backend();
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
        let backend = test_backend();
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
        let backend = test_backend();

        let result = backend
            .copy_object("test-bucket", "no-such-key", "test-bucket", "dest.txt")
            .await;
        assert!(result.is_err());
    }

    // -- Multipart part storage tests -----------------------------------------

    #[tokio::test]
    async fn test_put_part_and_verify() {
        let backend = test_backend();

        let data = Bytes::from("part data here");
        let etag = backend
            .put_part("test-bucket", "upload-001", 1, data.clone())
            .await
            .unwrap();

        assert!(etag.starts_with('"'));
        assert!(etag.ends_with('"'));

        // Verify the part is stored.
        let parts = backend.parts.read().await;
        let (stored_data, stored_etag) = parts.get("upload-001/1").unwrap();
        assert_eq!(stored_data.as_ref(), data.as_ref());
        assert_eq!(stored_etag, &etag);
    }

    #[tokio::test]
    async fn test_put_part_overwrites() {
        let backend = test_backend();

        let etag1 = backend
            .put_part("test-bucket", "upload-001", 1, Bytes::from("v1"))
            .await
            .unwrap();
        let etag2 = backend
            .put_part("test-bucket", "upload-001", 1, Bytes::from("v2"))
            .await
            .unwrap();

        assert_ne!(etag1, etag2);

        let parts = backend.parts.read().await;
        let (stored_data, _) = parts.get("upload-001/1").unwrap();
        assert_eq!(stored_data.as_ref(), b"v2");
    }

    #[tokio::test]
    async fn test_put_multiple_parts() {
        let backend = test_backend();

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

        let parts = backend.parts.read().await;
        assert!(parts.contains_key("upload-001/1"));
        assert!(parts.contains_key("upload-001/2"));
        assert!(parts.contains_key("upload-001/3"));
    }

    #[tokio::test]
    async fn test_delete_parts() {
        let backend = test_backend();

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

        let parts = backend.parts.read().await;
        assert!(!parts.contains_key("upload-001/1"));
        assert!(!parts.contains_key("upload-001/2"));
    }

    #[tokio::test]
    async fn test_delete_parts_nonexistent_is_ok() {
        let backend = test_backend();

        backend
            .delete_parts("test-bucket", "no-such-upload")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_put_part_etag_is_md5() {
        let backend = test_backend();

        let etag = backend
            .put_part("test-bucket", "upload-001", 1, Bytes::new())
            .await
            .unwrap();
        assert_eq!(etag, "\"d41d8cd98f00b204e9800998ecf8427e\"");
    }

    // -- assemble_parts tests ------------------------------------------------

    #[tokio::test]
    async fn test_assemble_parts_basic() {
        let backend = test_backend();
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
        let backend = test_backend();
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
        let backend = test_backend();
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

    // -- Memory limit tests --------------------------------------------------

    #[tokio::test]
    async fn test_memory_limit_put() {
        let backend = test_backend_with_limit(10);

        // This should succeed (5 bytes < 10).
        backend
            .put("test-bucket/a.txt", Bytes::from("hello"))
            .await
            .unwrap();

        // This should fail (5 + 6 = 11 > 10).
        let result = backend
            .put("test-bucket/b.txt", Bytes::from("world!"))
            .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Memory limit exceeded"));
    }

    #[tokio::test]
    async fn test_memory_limit_overwrite_same_size() {
        let backend = test_backend_with_limit(10);

        backend
            .put("test-bucket/a.txt", Bytes::from("hello"))
            .await
            .unwrap();

        // Overwriting with same-size data should succeed (delta = 0).
        backend
            .put("test-bucket/a.txt", Bytes::from("world"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_memory_limit_delete_frees_space() {
        let backend = test_backend_with_limit(10);

        backend
            .put("test-bucket/a.txt", Bytes::from("hello"))
            .await
            .unwrap();

        // This would fail without deleting first.
        backend.delete("test-bucket/a.txt").await.unwrap();

        // Now we have space again.
        backend
            .put("test-bucket/b.txt", Bytes::from("world!!!!"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_unlimited_memory() {
        let backend = test_backend();

        // With max_size_bytes = 0, we can store as much as we want.
        let big_data = Bytes::from(vec![0u8; 1_000_000]);
        backend
            .put("test-bucket/big.bin", big_data)
            .await
            .unwrap();
    }

    // -- Snapshot tests -------------------------------------------------------

    #[tokio::test]
    async fn test_snapshot_and_restore() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let snap_path = dir.path().join("snapshot.db");
        let snap_str = snap_path.to_str().unwrap();

        // Create backend, store some data, snapshot.
        {
            let backend =
                MemoryBackend::new(0, "snapshot", snap_str, 0).unwrap();
            backend
                .put("bucket/key1.txt", Bytes::from("data one"))
                .await
                .unwrap();
            backend
                .put("bucket/key2.txt", Bytes::from("data two"))
                .await
                .unwrap();
            backend
                .put_part("bucket", "upload-1", 1, Bytes::from("part-a"))
                .await
                .unwrap();
            backend.snapshot().unwrap();
        }

        // Create new backend from the snapshot.
        {
            let backend =
                MemoryBackend::new(0, "snapshot", snap_str, 0).unwrap();

            let obj1 = backend.get("bucket/key1.txt").await.unwrap();
            assert_eq!(obj1.data, Bytes::from("data one"));

            let obj2 = backend.get("bucket/key2.txt").await.unwrap();
            assert_eq!(obj2.data, Bytes::from("data two"));

            let parts = backend.parts.read().await;
            assert!(parts.contains_key("upload-1/1"));
            let (part_data, _) = parts.get("upload-1/1").unwrap();
            assert_eq!(part_data.as_ref(), b"part-a");
        }
    }

    #[tokio::test]
    async fn test_close_snapshots() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let snap_path = dir.path().join("close-snapshot.db");
        let snap_str = snap_path.to_str().unwrap();

        let backend =
            MemoryBackend::new(0, "snapshot", snap_str, 0).unwrap();
        backend
            .put("bucket/key.txt", Bytes::from("close data"))
            .await
            .unwrap();

        // close() should trigger a final snapshot.
        backend.close().await;

        assert!(snap_path.exists());

        // Verify the snapshot contains the data.
        let backend2 =
            MemoryBackend::new(0, "snapshot", snap_str, 0).unwrap();
        let obj = backend2.get("bucket/key.txt").await.unwrap();
        assert_eq!(obj.data, Bytes::from("close data"));
    }

    #[tokio::test]
    async fn test_no_snapshot_when_persistence_none() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let snap_path = dir.path().join("should-not-exist.db");
        let snap_str = snap_path.to_str().unwrap();

        let backend = MemoryBackend::new(0, "none", snap_str, 0).unwrap();
        backend
            .put("bucket/key.txt", Bytes::from("data"))
            .await
            .unwrap();

        backend.close().await;

        assert!(!snap_path.exists());
    }
}
