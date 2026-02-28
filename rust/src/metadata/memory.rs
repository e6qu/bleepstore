//! In-memory metadata store.
//!
//! Stores all metadata in memory with no persistence. Useful for testing
//! and ephemeral deployments. Uses `RwLock<HashMap>` for thread-safe access.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::RwLock;

use super::store::{
    BucketRecord, CredentialRecord, ListObjectsResult, ListPartsResult, ListUploadsResult,
    MetadataStore, MultipartUploadRecord, ObjectRecord, PartRecord,
};

type BucketKey = String;
type ObjectKey = (String, String);
type UploadKey = String;
type CredentialKey = String;
type PartKey = (String, u32);

#[derive(Debug, Default)]
struct Inner {
    buckets: HashMap<BucketKey, BucketRecord>,
    objects: HashMap<ObjectKey, ObjectRecord>,
    uploads: HashMap<UploadKey, MultipartUploadRecord>,
    parts: HashMap<PartKey, PartRecord>,
    credentials: HashMap<CredentialKey, CredentialRecord>,
}

pub struct MemoryMetadataStore {
    inner: RwLock<Inner>,
}

impl MemoryMetadataStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner::default()),
        }
    }

    pub fn seed_credential(&self, access_key: &str, secret_key: &str) -> anyhow::Result<()> {
        let now = chrono_now();
        let record = CredentialRecord {
            access_key_id: access_key.to_string(),
            secret_key: secret_key.to_string(),
            owner_id: access_key.to_string(),
            display_name: access_key.to_string(),
            active: true,
            created_at: now,
        };
        let mut inner = self.inner.write().expect("rwlock poisoned");
        inner.credentials.entry(access_key.to_string()).or_insert(record);
        Ok(())
    }
}

fn chrono_now() -> String {
    let now = std::time::SystemTime::now();
    let since_epoch = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format_timestamp(since_epoch.as_secs(), since_epoch.subsec_millis())
}

fn format_timestamp(secs: u64, millis: u32) -> String {
    let days = secs / 86400;
    let day_secs = secs % 86400;
    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let seconds = day_secs % 60;
    let (year, month, day) = days_to_ymd(days);
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z")
}

fn days_to_ymd(days: u64) -> (i32, u32, u32) {
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

impl MetadataStore for MemoryMetadataStore {
    fn create_bucket(
        &self,
        record: BucketRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut inner = self.inner.write().expect("rwlock poisoned");
            if inner.buckets.contains_key(&record.name) {
                return Err(anyhow::anyhow!("Bucket already exists: {}", record.name));
            }
            inner.buckets.insert(record.name.clone(), record);
            Ok(())
        })
    }

    fn get_bucket(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<BucketRecord>>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let inner = self.inner.read().expect("rwlock poisoned");
            Ok(inner.buckets.get(&name).cloned())
        })
    }

    fn bucket_exists(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let inner = self.inner.read().expect("rwlock poisoned");
            Ok(inner.buckets.contains_key(&name))
        })
    }

    fn list_buckets(
        &self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<BucketRecord>>> + Send + '_>> {
        Box::pin(async move {
            let inner = self.inner.read().expect("rwlock poisoned");
            let mut buckets: Vec<_> = inner.buckets.values().cloned().collect();
            buckets.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(buckets)
        })
    }

    fn delete_bucket(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let mut inner = self.inner.write().expect("rwlock poisoned");
            inner.buckets.remove(&name);
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
            let mut inner = self.inner.write().expect("rwlock poisoned");
            if let Some(bucket) = inner.buckets.get_mut(&name) {
                bucket.acl = acl;
            }
            Ok(())
        })
    }

    fn put_object(
        &self,
        record: ObjectRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut inner = self.inner.write().expect("rwlock poisoned");
            let key = (record.bucket.clone(), record.key.clone());
            inner.objects.insert(key, record);
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
            let inner = self.inner.read().expect("rwlock poisoned");
            Ok(inner.objects.get(&(bucket, key)).cloned())
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
            let inner = self.inner.read().expect("rwlock poisoned");
            Ok(inner.objects.contains_key(&(bucket, key)))
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
            let inner = self.inner.read().expect("rwlock poisoned");
            
            let effective_start = continuation_token.as_deref().unwrap_or(&start_after);
            
            let mut all_objects: Vec<ObjectRecord> = inner
                .objects
                .iter()
                .filter(|((b, k), _)| *b == bucket && k.as_str() > effective_start && k.starts_with(&prefix))
                .map(|(_, obj)| obj.clone())
                .collect();
            
            all_objects.sort_by(|a, b| a.key.cmp(&b.key));
            
            if delimiter.is_empty() {
                let is_truncated = all_objects.len() > max_keys as usize;
                let objects: Vec<ObjectRecord> = all_objects.into_iter().take(max_keys as usize).collect();
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
                let mut objects = Vec::new();
                let mut common_prefixes = std::collections::BTreeSet::new();
                let mut count = 0u32;

                for obj in all_objects {
                    if count >= max_keys {
                        break;
                    }
                    let after_prefix = &obj.key[prefix.len()..];
                    if let Some(pos) = after_prefix.find(&delimiter) {
                        let cp = format!("{}{}{}", prefix, &after_prefix[..pos], delimiter);
                        if common_prefixes.insert(cp) {
                            count += 1;
                        }
                    } else {
                        objects.push(obj);
                        count += 1;
                    }
                }

                let is_truncated = count >= max_keys;
                let next_token = if is_truncated {
                    objects.last().map(|o| o.key.clone())
                        .or_else(|| common_prefixes.iter().last().cloned())
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
            let mut inner = self.inner.write().expect("rwlock poisoned");
            inner.objects.remove(&(bucket, key));
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
            let mut inner = self.inner.write().expect("rwlock poisoned");
            for key in &keys {
                inner.objects.remove(&(bucket.clone(), key.clone()));
            }
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
            let mut inner = self.inner.write().expect("rwlock poisoned");
            if let Some(obj) = inner.objects.get_mut(&(bucket, key)) {
                obj.acl = acl;
            }
            Ok(())
        })
    }

    fn count_objects(
        &self,
        bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<u64>> + Send + '_>> {
        let bucket = bucket.to_string();
        Box::pin(async move {
            let inner = self.inner.read().expect("rwlock poisoned");
            let count = inner.objects.keys().filter(|(b, _)| *b == bucket).count();
            Ok(count as u64)
        })
    }

    fn create_multipart_upload(
        &self,
        record: MultipartUploadRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut inner = self.inner.write().expect("rwlock poisoned");
            inner.uploads.insert(record.upload_id.clone(), record);
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
            let inner = self.inner.read().expect("rwlock poisoned");
            Ok(inner.uploads.get(&upload_id).cloned())
        })
    }

    fn put_part(
        &self,
        upload_id: &str,
        part: PartRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let mut inner = self.inner.write().expect("rwlock poisoned");
            let key = (upload_id, part.part_number);
            inner.parts.insert(key, part);
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
            let inner = self.inner.read().expect("rwlock poisoned");
            let mut parts: Vec<PartRecord> = inner
                .parts
                .iter()
                .filter(|((uid, pn), _)| *uid == upload_id && *pn > part_number_marker)
                .map(|(_, p)| p.clone())
                .collect();
            parts.sort_by_key(|p| p.part_number);
            
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
            let inner = self.inner.read().expect("rwlock poisoned");
            let mut parts: Vec<PartRecord> = inner
                .parts
                .iter()
                .filter(|((uid, _), _)| *uid == upload_id)
                .map(|(_, p)| p.clone())
                .collect();
            parts.sort_by_key(|p| p.part_number);
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
            let mut inner = self.inner.write().expect("rwlock poisoned");
            let object_key = (final_object.bucket.clone(), final_object.key.clone());
            inner.objects.insert(object_key, final_object);
            inner.parts.retain(|(uid, _), _| uid != &upload_id);
            inner.uploads.remove(&upload_id);
            Ok(())
        })
    }

    fn delete_multipart_upload(
        &self,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let mut inner = self.inner.write().expect("rwlock poisoned");
            inner.parts.retain(|(uid, _), _| uid != &upload_id);
            inner.uploads.remove(&upload_id);
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
            let inner = self.inner.read().expect("rwlock poisoned");
            
            let mut uploads: Vec<MultipartUploadRecord> = inner
                .uploads
                .iter()
                .filter(|(_, u)| {
                    u.bucket == bucket && u.key.starts_with(&prefix) && 
                    (key_marker.is_empty() || u.key > key_marker || 
                     (u.key == key_marker && !upload_id_marker.is_empty() && u.upload_id > upload_id_marker))
                })
                .map(|(_, u)| u.clone())
                .collect();
            
            uploads.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.upload_id.cmp(&b.upload_id)));
            
            let is_truncated = uploads.len() > max_uploads as usize;
            if is_truncated {
                uploads.truncate(max_uploads as usize);
            }
            
            let (next_key_marker, next_upload_id_marker) = if is_truncated {
                uploads.last()
                    .map(|u| (Some(u.key.clone()), Some(u.upload_id.clone())))
                    .unwrap_or((None, None))
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

    fn get_credential(
        &self,
        access_key_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<CredentialRecord>>> + Send + '_>> {
        let access_key_id = access_key_id.to_string();
        Box::pin(async move {
            let inner = self.inner.read().expect("rwlock poisoned");
            Ok(inner.credentials.get(&access_key_id).cloned())
        })
    }

    fn put_credential(
        &self,
        record: CredentialRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut inner = self.inner.write().expect("rwlock poisoned");
            inner.credentials.insert(record.access_key_id.clone(), record);
            Ok(())
        })
    }
}

impl Default for MemoryMetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> MemoryMetadataStore {
        MemoryMetadataStore::new()
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

    #[tokio::test]
    async fn test_create_and_get_bucket() {
        let store = test_store();
        let bucket = make_bucket("test-bucket");
        store.create_bucket(bucket).await.unwrap();

        let fetched = store.get_bucket("test-bucket").await.unwrap();
        assert!(fetched.is_some());
        let b = fetched.unwrap();
        assert_eq!(b.name, "test-bucket");
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

        let buckets = store.list_buckets().await.unwrap();
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].name, "alpha");
        assert_eq!(buckets[1].name, "beta");
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
    async fn test_put_and_get_object() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();

        let obj = make_object("mybucket", "hello.txt", 5);
        store.put_object(obj).await.unwrap();

        let fetched = store.get_object("mybucket", "hello.txt").await.unwrap();
        assert!(fetched.is_some());
        let o = fetched.unwrap();
        assert_eq!(o.key, "hello.txt");
        assert_eq!(o.size, 5);
    }

    #[tokio::test]
    async fn test_list_objects() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();

        store.put_object(make_object("mybucket", "a/1.txt", 1)).await.unwrap();
        store.put_object(make_object("mybucket", "a/2.txt", 2)).await.unwrap();
        store.put_object(make_object("mybucket", "b/1.txt", 3)).await.unwrap();

        let result = store.list_objects("mybucket", "", "", 10, "", None).await.unwrap();
        assert_eq!(result.objects.len(), 3);
        assert!(!result.is_truncated);
    }

    #[tokio::test]
    async fn test_list_objects_with_prefix() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();

        store.put_object(make_object("mybucket", "a/1.txt", 1)).await.unwrap();
        store.put_object(make_object("mybucket", "a/2.txt", 2)).await.unwrap();
        store.put_object(make_object("mybucket", "b/1.txt", 3)).await.unwrap();

        let result = store.list_objects("mybucket", "a/", "", 10, "", None).await.unwrap();
        assert_eq!(result.objects.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_object() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store.put_object(make_object("mybucket", "delete-me.txt", 1)).await.unwrap();
        
        assert!(store.object_exists("mybucket", "delete-me.txt").await.unwrap());
        store.delete_object("mybucket", "delete-me.txt").await.unwrap();
        assert!(!store.object_exists("mybucket", "delete-me.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_credentials() {
        let store = test_store();
        store.seed_credential("test-key", "test-secret").unwrap();

        let cred = store.get_credential("test-key").await.unwrap();
        assert!(cred.is_some());
        let c = cred.unwrap();
        assert_eq!(c.secret_key, "test-secret");
        assert!(c.active);
    }

    #[tokio::test]
    async fn test_multipart_upload() {
        let store = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();

        let upload = MultipartUploadRecord {
            upload_id: "upload-123".to_string(),
            bucket: "mybucket".to_string(),
            key: "large-file.bin".to_string(),
            content_type: "application/octet-stream".to_string(),
            content_encoding: None,
            content_language: None,
            content_disposition: None,
            cache_control: None,
            expires: None,
            storage_class: "STANDARD".to_string(),
            acl: "{}".to_string(),
            user_metadata: HashMap::new(),
            owner_id: "owner".to_string(),
            owner_display: "Owner".to_string(),
            initiated_at: "2026-02-23T00:00:00.000Z".to_string(),
        };
        store.create_multipart_upload(upload).await.unwrap();

        let fetched = store.get_multipart_upload("upload-123").await.unwrap();
        assert!(fetched.is_some());

        let part = PartRecord {
            part_number: 1,
            size: 1024,
            etag: "\"etag-1\"".to_string(),
            last_modified: "2026-02-23T00:00:00.000Z".to_string(),
        };
        store.put_part("upload-123", part).await.unwrap();

        let parts = store.list_parts("upload-123", 100, 0).await.unwrap();
        assert_eq!(parts.parts.len(), 1);

        let final_obj = make_object("mybucket", "large-file.bin", 2048);
        store.complete_multipart_upload("upload-123", final_obj).await.unwrap();

        let obj = store.get_object("mybucket", "large-file.bin").await.unwrap();
        assert!(obj.is_some());

        let upload_after = store.get_multipart_upload("upload-123").await.unwrap();
        assert!(upload_after.is_none());
    }
}
