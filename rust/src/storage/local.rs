//! Local filesystem storage backend.
//!
//! Objects are stored as flat files under a configurable root directory.
//! The storage key is used directly as a relative path (e.g., "bucket/key").
//!
//! All writes follow crash-only design: write to temp file, fsync, rename.

use bytes::Bytes;
use md5::{Digest, Md5};
use std::future::Future;
use std::io::Write;
use std::path::PathBuf;
use std::pin::Pin;

use super::backend::{StorageBackend, StoredObject};

/// Stores objects on the local filesystem.
pub struct LocalBackend {
    /// Root directory for all stored objects.
    root: PathBuf,
}

impl LocalBackend {
    /// Create a new `LocalBackend` rooted at `root`.
    ///
    /// The directory will be created if it does not exist.
    pub fn new(root: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        // Also create the .tmp directory for atomic writes.
        std::fs::create_dir_all(root.join(".tmp"))?;
        Ok(Self { root })
    }

    /// Resolve a storage key to an absolute file path.
    ///
    /// Also validates that the resolved path stays within the root directory
    /// to prevent path traversal attacks.
    fn resolve(&self, storage_key: &str) -> anyhow::Result<PathBuf> {
        let path = self.root.join(storage_key);
        // Canonicalize the root for comparison. The target path may not exist
        // yet, so we canonicalize the root and check the joined path starts
        // with it.
        let canonical_root = self.root.canonicalize().unwrap_or_else(|_| self.root.clone());
        // We can't canonicalize a non-existent path, so we check each
        // component is not ".." after joining.
        for component in std::path::Path::new(storage_key).components() {
            if let std::path::Component::ParentDir = component {
                anyhow::bail!("Path traversal detected in storage key: {}", storage_key);
            }
        }
        // Additional safety: if path somehow resolves outside root, reject.
        // For existing paths, do a canonical check.
        if path.exists() {
            let canonical_path = path.canonicalize()?;
            if !canonical_path.starts_with(&canonical_root) {
                anyhow::bail!("Path traversal detected in storage key: {}", storage_key);
            }
        }
        Ok(path)
    }

    /// Generate a temp file path under .tmp/ for atomic writes.
    fn temp_path(&self) -> PathBuf {
        let id = uuid::Uuid::new_v4();
        self.root.join(".tmp").join(format!("tmp-{}", id))
    }
}

impl StorageBackend for LocalBackend {
    fn put(
        &self,
        storage_key: &str,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        let data = data.clone();
        Box::pin(async move {
            let final_path = self.resolve(&storage_key)?;

            // Ensure parent directory exists (handles keys with '/' separators).
            if let Some(parent) = final_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            // Compute MD5 hash for ETag.
            let mut hasher = Md5::new();
            hasher.update(&data);
            let md5_bytes = hasher.finalize();
            let etag = format!("\"{}\"", hex::encode(md5_bytes));

            // Crash-only: temp-fsync-rename pattern.
            let tmp_path = self.temp_path();

            // Ensure .tmp directory exists.
            if let Some(parent) = tmp_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            // Write to temp file.
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(&data)?;
            file.sync_all()?; // fsync

            // Atomic rename to final path.
            std::fs::rename(&tmp_path, &final_path)?;

            Ok(etag)
        })
    }

    fn get(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<StoredObject>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let path = self.resolve(&storage_key)?;

            if !path.exists() {
                anyhow::bail!("Object not found at storage key: {}", storage_key);
            }

            let data = std::fs::read(&path)?;
            let data = Bytes::from(data);

            // Compute SHA-256 content hash.
            use sha2::{Digest as Sha2Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(&data);
            let hash_bytes = hasher.finalize();
            let content_hash = hex::encode(hash_bytes);

            Ok(StoredObject { data, content_hash })
        })
    }

    fn delete(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let path = self.resolve(&storage_key)?;

            // Idempotent: if the file doesn't exist, that's fine.
            if path.exists() {
                std::fs::remove_file(&path)?;
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
            let path = self.resolve(&storage_key)?;
            Ok(path.exists() && path.is_file())
        })
    }

    fn copy_object(
        &self,
        bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>> {
        let src_storage_key = format!("{}/{}", bucket, src_key);
        let dst_storage_key = format!("{}/{}", dst_bucket, dst_key);
        Box::pin(async move {
            let src_path = self.resolve(&src_storage_key)?;
            if !src_path.exists() {
                anyhow::bail!("Source object not found at storage key: {}", src_storage_key);
            }

            let dst_path = self.resolve(&dst_storage_key)?;

            // Ensure parent directory exists for destination.
            if let Some(parent) = dst_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            // Read source data.
            let data = std::fs::read(&src_path)?;

            // Compute MD5 hash for ETag.
            let mut hasher = Md5::new();
            hasher.update(&data);
            let md5_bytes = hasher.finalize();
            let etag = format!("\"{}\"", hex::encode(md5_bytes));

            // Crash-only: temp-fsync-rename pattern.
            let tmp_path = self.temp_path();
            if let Some(parent) = tmp_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(&data)?;
            file.sync_all()?;
            std::fs::rename(&tmp_path, &dst_path)?;

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
        Box::pin(async move {
            // Part storage path: {root}/.multipart/{upload_id}/{part_number}
            let part_dir = self.root.join(".multipart").join(&upload_id);
            std::fs::create_dir_all(&part_dir)?;

            let final_path = part_dir.join(part_number.to_string());

            // Compute MD5 hash for ETag.
            let mut hasher = Md5::new();
            hasher.update(&data);
            let md5_bytes = hasher.finalize();
            let etag = format!("\"{}\"", hex::encode(md5_bytes));

            // Crash-only: temp-fsync-rename pattern.
            let tmp_path = self.temp_path();
            if let Some(parent) = tmp_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(&data)?;
            file.sync_all()?; // fsync

            // Atomic rename to final path.
            std::fs::rename(&tmp_path, &final_path)?;

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
            // Final object path: {root}/{bucket}/{key}
            let final_storage_key = format!("{}/{}", bucket, key);
            let final_path = self.resolve(&final_storage_key)?;

            // Ensure parent directory exists.
            if let Some(parent) = final_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            // Crash-only: write to temp file, fsync, rename.
            let tmp_path = self.temp_path();
            if let Some(parent) = tmp_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let mut output_file = std::fs::File::create(&tmp_path)?;

            // Compute composite ETag: MD5 of concatenated binary MD5s + "-{part_count}"
            let mut combined_md5_bytes: Vec<u8> = Vec::new();
            let mut total_size: u64 = 0;

            for (part_number, _etag) in &parts {
                // Read part file.
                let part_path = self.root.join(".multipart").join(&upload_id).join(part_number.to_string());
                let part_data = std::fs::read(&part_path)
                    .map_err(|e| anyhow::anyhow!("Failed to read part {}: {}", part_number, e))?;

                total_size += part_data.len() as u64;

                // Compute MD5 of this part's data.
                let mut part_hasher = Md5::new();
                part_hasher.update(&part_data);
                let part_md5 = part_hasher.finalize();
                combined_md5_bytes.extend_from_slice(&part_md5);

                // Write part data to output file.
                output_file.write_all(&part_data)?;
            }

            // fsync the output file.
            output_file.sync_all()?;
            drop(output_file);

            // Atomic rename to final path.
            std::fs::rename(&tmp_path, &final_path)?;

            // Compute composite ETag.
            let mut composite_hasher = Md5::new();
            composite_hasher.update(&combined_md5_bytes);
            let composite_md5 = composite_hasher.finalize();
            let composite_etag = format!("\"{}-{}\"", hex::encode(composite_md5), parts.len());

            let _ = total_size; // available if needed later
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
            // Part storage directory: {root}/.multipart/{upload_id}/
            let part_dir = self.root.join(".multipart").join(&upload_id);

            // Idempotent: if the directory doesn't exist, that's fine.
            if part_dir.exists() {
                std::fs::remove_dir_all(&part_dir)?;
            }

            Ok(())
        })
    }

    fn create_bucket(
        &self,
        bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let path = self.root.join(bucket);
        Box::pin(async move {
            std::fs::create_dir_all(&path)?;
            Ok(())
        })
    }

    fn delete_bucket(
        &self,
        bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let path = self.root.join(bucket);
        Box::pin(async move {
            // Best-effort removal. If the directory does not exist, ignore.
            if path.exists() {
                std::fs::remove_dir_all(&path)?;
            }
            Ok(())
        })
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_backend() -> (tempfile::TempDir, LocalBackend) {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let backend = LocalBackend::new(dir.path()).expect("failed to create backend");
        (dir, backend)
    }

    #[tokio::test]
    async fn test_put_and_get_roundtrip() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        let data = Bytes::from("hello world");
        let etag = backend.put("test-bucket/key.txt", data.clone()).await.unwrap();

        // ETag should be quoted hex MD5.
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
        let etag = backend.put("test-bucket/empty.txt", data.clone()).await.unwrap();
        assert!(etag.starts_with('"'));

        let obj = backend.get("test-bucket/empty.txt").await.unwrap();
        assert_eq!(obj.data.len(), 0);
    }

    #[tokio::test]
    async fn test_put_creates_parent_dirs() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        let data = Bytes::from("nested content");
        let etag = backend
            .put("test-bucket/a/b/c/deep.txt", data.clone())
            .await
            .unwrap();
        assert!(etag.starts_with('"'));

        let obj = backend.get("test-bucket/a/b/c/deep.txt").await.unwrap();
        assert_eq!(obj.data, data);
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
        backend.create_bucket("test-bucket").await.unwrap();

        // Deleting a non-existent key should succeed (idempotent).
        backend.delete("test-bucket/no-such-key").await.unwrap();
    }

    #[tokio::test]
    async fn test_exists() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

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
        backend.create_bucket("test-bucket").await.unwrap();

        let result = backend.get("test-bucket/no-such-key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_overwrites() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        let etag1 = backend
            .put("test-bucket/key.txt", Bytes::from("version 1"))
            .await
            .unwrap();
        let etag2 = backend
            .put("test-bucket/key.txt", Bytes::from("version 2"))
            .await
            .unwrap();

        // Different content -> different ETags.
        assert_ne!(etag1, etag2);

        let obj = backend.get("test-bucket/key.txt").await.unwrap();
        assert_eq!(obj.data, Bytes::from("version 2"));
    }

    #[tokio::test]
    async fn test_etag_is_md5() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        // Known MD5 of empty string: d41d8cd98f00b204e9800998ecf8427e
        let etag = backend
            .put("test-bucket/empty", Bytes::new())
            .await
            .unwrap();
        assert_eq!(etag, "\"d41d8cd98f00b204e9800998ecf8427e\"");
    }

    #[tokio::test]
    async fn test_create_and_delete_bucket() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("mybucket").await.unwrap();

        // Bucket directory should exist.
        // Put an object to confirm.
        backend
            .put("mybucket/obj.txt", Bytes::from("data"))
            .await
            .unwrap();

        backend.delete_bucket("mybucket").await.unwrap();

        // Object should no longer be accessible.
        assert!(!backend.exists("mybucket/obj.txt").await.unwrap());
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

        // ETags should match (same data).
        assert_eq!(src_etag, dst_etag);

        // Copy should be readable.
        let obj = backend.get("test-bucket/copy.txt").await.unwrap();
        assert_eq!(obj.data, data);

        // Original should still exist.
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
        backend.create_bucket("test-bucket").await.unwrap();

        let result = backend
            .copy_object("test-bucket", "no-such-key", "test-bucket", "dest.txt")
            .await;
        assert!(result.is_err());
    }

    // -- Multipart part storage tests -----------------------------------------

    #[tokio::test]
    async fn test_put_part_and_verify() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        let data = Bytes::from("part data here");
        let etag = backend
            .put_part("test-bucket", "upload-001", 1, data.clone())
            .await
            .unwrap();

        // ETag should be quoted hex MD5.
        assert!(etag.starts_with('"'));
        assert!(etag.ends_with('"'));

        // Part file should exist on disk.
        let part_path = backend
            .root
            .join(".multipart")
            .join("upload-001")
            .join("1");
        assert!(part_path.exists());

        let stored = std::fs::read(&part_path).unwrap();
        assert_eq!(stored, data.as_ref());
    }

    #[tokio::test]
    async fn test_put_part_overwrites() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        let etag1 = backend
            .put_part("test-bucket", "upload-001", 1, Bytes::from("v1"))
            .await
            .unwrap();
        let etag2 = backend
            .put_part("test-bucket", "upload-001", 1, Bytes::from("v2"))
            .await
            .unwrap();

        // Different content -> different ETags.
        assert_ne!(etag1, etag2);

        // Should contain v2.
        let part_path = backend
            .root
            .join(".multipart")
            .join("upload-001")
            .join("1");
        let stored = std::fs::read(&part_path).unwrap();
        assert_eq!(stored, b"v2");
    }

    #[tokio::test]
    async fn test_put_multiple_parts() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

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

        // All three parts should exist on disk.
        for i in 1..=3 {
            let part_path = backend
                .root
                .join(".multipart")
                .join("upload-001")
                .join(i.to_string());
            assert!(part_path.exists());
        }
    }

    #[tokio::test]
    async fn test_delete_parts() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        // Upload some parts.
        backend
            .put_part("test-bucket", "upload-001", 1, Bytes::from("part1"))
            .await
            .unwrap();
        backend
            .put_part("test-bucket", "upload-001", 2, Bytes::from("part2"))
            .await
            .unwrap();

        let part_dir = backend.root.join(".multipart").join("upload-001");
        assert!(part_dir.exists());

        // Delete all parts.
        backend.delete_parts("test-bucket", "upload-001").await.unwrap();

        // Directory should be gone.
        assert!(!part_dir.exists());
    }

    #[tokio::test]
    async fn test_delete_parts_nonexistent_is_ok() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        // Deleting parts for a nonexistent upload should succeed (idempotent).
        backend
            .delete_parts("test-bucket", "no-such-upload")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_put_part_etag_is_md5() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        // Known MD5 of empty string: d41d8cd98f00b204e9800998ecf8427e
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

        // Upload two parts.
        let etag1 = backend
            .put_part("test-bucket", "upload-001", 1, Bytes::from("hello "))
            .await
            .unwrap();
        let etag2 = backend
            .put_part("test-bucket", "upload-001", 2, Bytes::from("world"))
            .await
            .unwrap();

        // Assemble parts.
        let parts = vec![(1u32, etag1.clone()), (2u32, etag2.clone())];
        let composite_etag = backend
            .assemble_parts("test-bucket", "assembled.txt", "upload-001", &parts)
            .await
            .unwrap();

        // Composite ETag should end with "-2" (2 parts).
        assert!(composite_etag.starts_with('"'));
        assert!(composite_etag.ends_with("-2\""));

        // The assembled object should contain concatenated data.
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

        // Should end with "-1".
        assert!(composite_etag.ends_with("-1\""));

        let obj = backend.get("test-bucket/single.txt").await.unwrap();
        assert_eq!(obj.data, Bytes::from("solo"));
    }

    #[tokio::test]
    async fn test_assemble_parts_creates_parent_dirs() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        let etag1 = backend
            .put_part("test-bucket", "upload-003", 1, Bytes::from("data"))
            .await
            .unwrap();

        let parts = vec![(1u32, etag1.clone())];
        let _composite_etag = backend
            .assemble_parts("test-bucket", "a/b/c/deep.txt", "upload-003", &parts)
            .await
            .unwrap();

        let obj = backend.get("test-bucket/a/b/c/deep.txt").await.unwrap();
        assert_eq!(obj.data, Bytes::from("data"));
    }

    #[tokio::test]
    async fn test_assemble_parts_composite_etag_format() {
        let (_dir, backend) = test_backend();
        backend.create_bucket("test-bucket").await.unwrap();

        // Upload 3 parts.
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

        // Should be quoted, contain a dash, and end with "-3".
        assert!(composite_etag.starts_with('"'));
        assert!(composite_etag.ends_with("-3\""));
        // Format: "hex_md5-3"
        let inner = composite_etag.trim_matches('"');
        let dash_pos = inner.rfind('-').unwrap();
        let hex_part = &inner[..dash_pos];
        assert_eq!(hex_part.len(), 32); // MD5 hex is 32 chars
        assert_eq!(&inner[dash_pos + 1..], "3");
    }
}
