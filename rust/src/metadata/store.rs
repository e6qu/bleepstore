//! Abstract metadata store trait.
//!
//! Any metadata backend must implement [`MetadataStore`].  The trait
//! uses `async_trait`-style methods (manual desugaring with pinned
//! futures) so it can be used with both SQLite and future remote stores.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

// ── ACL types ──────────────────────────────────────────────────────

/// Represents an S3 Access Control List stored as JSON.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Acl {
    /// Owner of the resource.
    #[serde(default)]
    pub owner: AclOwner,
    /// List of access grants.
    #[serde(default)]
    pub grants: Vec<AclGrant>,
}

/// Owner portion of an ACL.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AclOwner {
    /// Canonical user ID.
    #[serde(default)]
    pub id: String,
    /// Display name.
    #[serde(default)]
    pub display_name: String,
}

/// A single ACL grant entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclGrant {
    /// The grantee receiving the permission.
    pub grantee: AclGrantee,
    /// The permission being granted.
    pub permission: String,
}

/// A grantee in an ACL grant.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AclGrantee {
    /// A canonical user grantee.
    CanonicalUser {
        id: String,
        #[serde(default)]
        display_name: String,
    },
    /// A group grantee.
    Group { uri: String },
}

impl Acl {
    /// Create a default FULL_CONTROL ACL for the given owner.
    pub fn full_control(owner_id: &str, display_name: &str) -> Self {
        Acl {
            owner: AclOwner {
                id: owner_id.to_string(),
                display_name: display_name.to_string(),
            },
            grants: vec![AclGrant {
                grantee: AclGrantee::CanonicalUser {
                    id: owner_id.to_string(),
                    display_name: display_name.to_string(),
                },
                permission: "FULL_CONTROL".to_string(),
            }],
        }
    }
}

// ── Credential types ───────────────────────────────────────────────

/// Stored credential record for SigV4 authentication.
#[derive(Debug, Clone)]
pub struct CredentialRecord {
    /// AWS-style access key ID.
    pub access_key_id: String,
    /// Secret key (plaintext, used for HMAC signing).
    pub secret_key: String,
    /// Canonical owner ID associated with this credential.
    pub owner_id: String,
    /// Display name for the owner.
    pub display_name: String,
    /// Whether this credential is active.
    pub active: bool,
    /// ISO-8601 creation timestamp.
    pub created_at: String,
}

// ── Record types ───────────────────────────────────────────────────

/// Metadata record for a bucket.
#[derive(Debug, Clone)]
pub struct BucketRecord {
    /// Bucket name.
    pub name: String,
    /// ISO-8601 creation timestamp.
    pub created_at: String,
    /// Region the bucket is placed in.
    pub region: String,
    /// Canonical owner ID.
    pub owner_id: String,
    /// Owner display name.
    pub owner_display: String,
    /// Access control list (JSON-serialized).
    pub acl: String,
}

/// Metadata record for an object.
#[derive(Debug, Clone)]
pub struct ObjectRecord {
    /// Bucket the object belongs to.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Size in bytes.
    pub size: u64,
    /// Quoted ETag string (e.g., `"d41d8cd98f00b204e9800998ecf8427e"`).
    pub etag: String,
    /// MIME content type.
    pub content_type: String,
    /// Content-Encoding header value, if any.
    pub content_encoding: Option<String>,
    /// Content-Language header value, if any.
    pub content_language: Option<String>,
    /// Content-Disposition header value, if any.
    pub content_disposition: Option<String>,
    /// Cache-Control header value, if any.
    pub cache_control: Option<String>,
    /// Expires header value (RFC 7231 date string), if any.
    pub expires: Option<String>,
    /// Storage class (default STANDARD).
    pub storage_class: String,
    /// Access control list (JSON-serialized).
    pub acl: String,
    /// ISO-8601 last-modified timestamp.
    pub last_modified: String,
    /// User-defined metadata headers.
    pub user_metadata: HashMap<String, String>,
    /// Whether this is a delete marker (versioning placeholder).
    pub delete_marker: bool,
}

/// Metadata record for an in-progress multipart upload.
#[derive(Debug, Clone)]
pub struct MultipartUploadRecord {
    /// Unique upload identifier.
    pub upload_id: String,
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// MIME content type.
    pub content_type: String,
    /// Content-Encoding, if any.
    pub content_encoding: Option<String>,
    /// Content-Language, if any.
    pub content_language: Option<String>,
    /// Content-Disposition, if any.
    pub content_disposition: Option<String>,
    /// Cache-Control, if any.
    pub cache_control: Option<String>,
    /// Expires, if any.
    pub expires: Option<String>,
    /// Storage class.
    pub storage_class: String,
    /// ACL (JSON-serialized).
    pub acl: String,
    /// User-defined metadata (JSON-serialized).
    pub user_metadata: HashMap<String, String>,
    /// Owner ID.
    pub owner_id: String,
    /// Owner display name.
    pub owner_display: String,
    /// ISO-8601 initiation timestamp.
    pub initiated_at: String,
}

/// Metadata record for a single uploaded part.
#[derive(Debug, Clone)]
pub struct PartRecord {
    /// Part number (1-based).
    pub part_number: u32,
    /// Size in bytes.
    pub size: u64,
    /// Quoted ETag string.
    pub etag: String,
    /// ISO-8601 last-modified timestamp.
    pub last_modified: String,
}

// ── List result types ──────────────────────────────────────────────

/// Result of a ListObjects operation.
#[derive(Debug, Clone)]
pub struct ListObjectsResult {
    /// The objects matching the query.
    pub objects: Vec<ObjectRecord>,
    /// Common prefixes when a delimiter is used.
    pub common_prefixes: Vec<String>,
    /// Next continuation token for pagination, if truncated.
    pub next_continuation_token: Option<String>,
    /// Whether the result set was truncated.
    pub is_truncated: bool,
}

/// Result of a ListMultipartUploads operation.
#[derive(Debug, Clone)]
pub struct ListUploadsResult {
    /// The in-progress uploads matching the query.
    pub uploads: Vec<MultipartUploadRecord>,
    /// Whether the result set was truncated.
    pub is_truncated: bool,
    /// Next key marker for pagination, if truncated.
    pub next_key_marker: Option<String>,
    /// Next upload ID marker for pagination, if truncated.
    pub next_upload_id_marker: Option<String>,
}

/// Result of a ListParts operation.
#[derive(Debug, Clone)]
pub struct ListPartsResult {
    /// The parts matching the query.
    pub parts: Vec<PartRecord>,
    /// Whether the result set was truncated.
    pub is_truncated: bool,
    /// Next part number marker for pagination, if truncated.
    pub next_part_number_marker: Option<u32>,
}

// ── Trait ───────────────────────────────────────────────────────────

/// Async metadata store contract.
///
/// Implementors must provide all CRUD operations needed by the S3 API.
pub trait MetadataStore: Send + Sync + 'static {
    // ── Buckets ─────────────────────────────────────────────────────

    /// Create a new bucket record.
    fn create_bucket(
        &self,
        record: BucketRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;

    /// Get a bucket by name.
    fn get_bucket(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<BucketRecord>>> + Send + '_>>;

    /// Check whether a bucket exists.
    fn bucket_exists(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>>;

    /// List all buckets.
    fn list_buckets(
        &self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<BucketRecord>>> + Send + '_>>;

    /// Delete a bucket by name.
    fn delete_bucket(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;

    /// Update the ACL on a bucket.
    fn update_bucket_acl(
        &self,
        name: &str,
        acl: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;

    // ── Objects ─────────────────────────────────────────────────────

    /// Insert or update an object record (upsert).
    fn put_object(
        &self,
        record: ObjectRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;

    /// Get a single object record.
    fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<ObjectRecord>>> + Send + '_>>;

    /// Check whether an object exists.
    fn object_exists(
        &self,
        bucket: &str,
        key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>>;

    /// List objects in a bucket with optional prefix and delimiter.
    fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: &str,
        max_keys: u32,
        start_after: &str,
        continuation_token: Option<&str>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<ListObjectsResult>> + Send + '_>>;

    /// Delete an object record.
    fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;

    /// Delete multiple object records (batch delete).
    /// Returns the list of keys that were successfully deleted.
    fn delete_objects(
        &self,
        bucket: &str,
        keys: &[String],
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<String>>> + Send + '_>>;

    /// Update the ACL on an object.
    fn update_object_acl(
        &self,
        bucket: &str,
        key: &str,
        acl: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;

    /// Count the number of objects in a bucket (for BucketNotEmpty checks).
    fn count_objects(
        &self,
        bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<u64>> + Send + '_>>;

    // ── Multipart uploads ───────────────────────────────────────────

    /// Create a multipart upload record.
    fn create_multipart_upload(
        &self,
        record: MultipartUploadRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;

    /// Get a multipart upload record by upload ID.
    fn get_multipart_upload(
        &self,
        upload_id: &str,
    ) -> Pin<
        Box<dyn Future<Output = anyhow::Result<Option<MultipartUploadRecord>>> + Send + '_>,
    >;

    /// Record an uploaded part (insert or replace).
    fn put_part(
        &self,
        upload_id: &str,
        part: PartRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;

    /// List parts belonging to an upload.
    fn list_parts(
        &self,
        upload_id: &str,
        max_parts: u32,
        part_number_marker: u32,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<ListPartsResult>> + Send + '_>>;

    /// Get the parts for completion validation. Returns parts in ascending
    /// part_number order.
    fn get_parts_for_completion(
        &self,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<PartRecord>>> + Send + '_>>;

    /// Complete a multipart upload: insert the final object record and
    /// clean up upload and part metadata. This must be transactional.
    ///
    /// `final_object` is the assembled ObjectRecord to insert.
    /// Returns the inserted object record.
    fn complete_multipart_upload(
        &self,
        upload_id: &str,
        final_object: ObjectRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;

    /// Delete a multipart upload and all its parts (abort).
    fn delete_multipart_upload(
        &self,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;

    /// List all in-progress multipart uploads for a bucket.
    fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        max_uploads: u32,
        key_marker: &str,
        upload_id_marker: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<ListUploadsResult>> + Send + '_>>;

    // ── Credentials ─────────────────────────────────────────────────

    /// Look up a credential by access key ID.
    fn get_credential(
        &self,
        access_key_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<CredentialRecord>>> + Send + '_>>;

    /// Insert or update a credential record.
    fn put_credential(
        &self,
        record: CredentialRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;
}
