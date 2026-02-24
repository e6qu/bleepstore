//! Abstract storage backend trait.
//!
//! Every storage backend must implement [`StorageBackend`].  The trait
//! works in terms of opaque byte streams so callers do not need to know
//! the underlying medium.

use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;

/// A stored object's data plus its content hash.
#[derive(Debug, Clone)]
pub struct StoredObject {
    /// Raw bytes of the object.
    pub data: Bytes,
    /// Hex-encoded content hash (typically SHA-256).
    pub content_hash: String,
}

/// Async object storage contract.
pub trait StorageBackend: Send + Sync + 'static {
    /// Write `data` to `storage_key`, returning the content hash.
    fn put(
        &self,
        storage_key: &str,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>>;

    /// Read the full object at `storage_key`.
    fn get(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<StoredObject>> + Send + '_>>;

    /// Delete the object at `storage_key`.
    fn delete(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;

    /// Check whether an object exists at `storage_key`.
    fn exists(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>>;

    /// Copy an object from `src_key` in `bucket` to `dst_key` in `dst_bucket`,
    /// returning the ETag of the new object.
    fn copy_object(
        &self,
        bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>>;

    /// Write a single part for a multipart upload, returning the part ETag.
    fn put_part(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>>;

    /// Assemble previously uploaded parts into a final object, returning
    /// the ETag of the completed object.
    fn assemble_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(u32, String)],
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>>;

    /// Delete all part data associated with a multipart upload.
    fn delete_parts(
        &self,
        bucket: &str,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;

    /// Create a new storage bucket.
    fn create_bucket(
        &self,
        bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;

    /// Delete a storage bucket.
    fn delete_bucket(
        &self,
        bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;
}
