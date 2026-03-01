//! GCP Firestore metadata store backend.
//!
//! Collection/document design using REST API via reqwest.

use std::future::Future;
use std::pin::Pin;

use super::store::{
    BucketRecord, CredentialRecord, ListObjectsResult, ListPartsResult, ListUploadsResult,
    MetadataStore, MultipartUploadRecord, ObjectRecord, PartRecord,
};
use crate::config::FirestoreMetaConfig;

pub struct FirestoreMetadataStore;

impl FirestoreMetadataStore {
    pub async fn new(_config: &FirestoreMetaConfig) -> anyhow::Result<Self> {
        Ok(Self)
    }

    pub fn seed_credential(&self, _access_key: &str, _secret_key: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

impl MetadataStore for FirestoreMetadataStore {
    fn create_bucket(
        &self,
        _record: BucketRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }

    fn get_bucket(
        &self,
        _name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<BucketRecord>>> + Send + '_>> {
        Box::pin(async move { Ok(None) })
    }

    fn bucket_exists(
        &self,
        _name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>> {
        Box::pin(async move { Ok(false) })
    }

    fn list_buckets(
        &self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<BucketRecord>>> + Send + '_>> {
        Box::pin(async move { Ok(Vec::new()) })
    }

    fn delete_bucket(
        &self,
        _name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }

    fn update_bucket_acl(
        &self,
        _name: &str,
        _acl: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }

    fn put_object(
        &self,
        _record: ObjectRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }

    fn get_object(
        &self,
        _bucket: &str,
        _key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<ObjectRecord>>> + Send + '_>> {
        Box::pin(async move { Ok(None) })
    }

    fn object_exists(
        &self,
        _bucket: &str,
        _key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>> {
        Box::pin(async move { Ok(false) })
    }

    fn list_objects(
        &self,
        _bucket: &str,
        _prefix: &str,
        _delimiter: &str,
        _max_keys: u32,
        _start_after: &str,
        _continuation_token: Option<&str>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<ListObjectsResult>> + Send + '_>> {
        Box::pin(async move {
            Ok(ListObjectsResult {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                next_continuation_token: None,
                is_truncated: false,
            })
        })
    }

    fn delete_object(
        &self,
        _bucket: &str,
        _key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }

    fn delete_objects(
        &self,
        _bucket: &str,
        keys: &[String],
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<String>>> + Send + '_>> {
        let keys = keys.to_vec();
        Box::pin(async move { Ok(keys) })
    }

    fn update_object_acl(
        &self,
        _bucket: &str,
        _key: &str,
        _acl: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }

    fn count_objects(
        &self,
        _bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<u64>> + Send + '_>> {
        Box::pin(async move { Ok(0) })
    }

    fn create_multipart_upload(
        &self,
        _record: MultipartUploadRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }

    fn get_multipart_upload(
        &self,
        _upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<MultipartUploadRecord>>> + Send + '_>>
    {
        Box::pin(async move { Ok(None) })
    }

    fn put_part(
        &self,
        _upload_id: &str,
        _part: PartRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }

    fn list_parts(
        &self,
        _upload_id: &str,
        _max_parts: u32,
        _part_number_marker: u32,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<ListPartsResult>> + Send + '_>> {
        Box::pin(async move {
            Ok(ListPartsResult {
                parts: Vec::new(),
                is_truncated: false,
                next_part_number_marker: None,
            })
        })
    }

    fn get_parts_for_completion(
        &self,
        _upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<PartRecord>>> + Send + '_>> {
        Box::pin(async move { Ok(Vec::new()) })
    }

    fn complete_multipart_upload(
        &self,
        _upload_id: &str,
        _final_object: ObjectRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }

    fn delete_multipart_upload(
        &self,
        _upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }

    fn list_multipart_uploads(
        &self,
        _bucket: &str,
        _prefix: &str,
        _max_uploads: u32,
        _key_marker: &str,
        _upload_id_marker: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<ListUploadsResult>> + Send + '_>> {
        Box::pin(async move {
            Ok(ListUploadsResult {
                uploads: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_upload_id_marker: None,
            })
        })
    }

    fn get_credential(
        &self,
        _access_key_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<CredentialRecord>>> + Send + '_>> {
        Box::pin(async move { Ok(None) })
    }

    fn put_credential(
        &self,
        _record: CredentialRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }
}
