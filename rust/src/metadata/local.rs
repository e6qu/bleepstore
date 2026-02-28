//! Local JSONL file metadata store.
//!
//! Stores entities as JSONL (JSON Lines) files for simple file-based persistence.
//! Uses tombstone markers (`_deleted: true`) for deletions, with optional
//! compaction on startup. File locking via `fs2` crate for concurrent access.

use std::collections::HashMap;
use std::future::Future;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

use super::store::{
    BucketRecord, CredentialRecord, ListObjectsResult, ListPartsResult, ListUploadsResult,
    MetadataStore, MultipartUploadRecord, ObjectRecord, PartRecord,
};
use crate::config::LocalMetaConfig;

#[derive(Debug, Serialize, Deserialize)]
struct JsonBucket {
    name: String,
    created_at: String,
    region: String,
    owner_id: String,
    owner_display: String,
    acl: String,
    #[serde(default)]
    _deleted: bool,
}

impl From<BucketRecord> for JsonBucket {
    fn from(r: BucketRecord) -> Self {
        JsonBucket {
            name: r.name,
            created_at: r.created_at,
            region: r.region,
            owner_id: r.owner_id,
            owner_display: r.owner_display,
            acl: r.acl,
            _deleted: false,
        }
    }
}

impl From<JsonBucket> for BucketRecord {
    fn from(j: JsonBucket) -> Self {
        BucketRecord {
            name: j.name,
            created_at: j.created_at,
            region: j.region,
            owner_id: j.owner_id,
            owner_display: j.owner_display,
            acl: j.acl,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonObject {
    bucket: String,
    key: String,
    size: u64,
    etag: String,
    content_type: String,
    content_encoding: Option<String>,
    content_language: Option<String>,
    content_disposition: Option<String>,
    cache_control: Option<String>,
    expires: Option<String>,
    storage_class: String,
    acl: String,
    last_modified: String,
    user_metadata: HashMap<String, String>,
    delete_marker: bool,
    #[serde(default)]
    _deleted: bool,
}

impl From<ObjectRecord> for JsonObject {
    fn from(r: ObjectRecord) -> Self {
        JsonObject {
            bucket: r.bucket,
            key: r.key,
            size: r.size,
            etag: r.etag,
            content_type: r.content_type,
            content_encoding: r.content_encoding,
            content_language: r.content_language,
            content_disposition: r.content_disposition,
            cache_control: r.cache_control,
            expires: r.expires,
            storage_class: r.storage_class,
            acl: r.acl,
            last_modified: r.last_modified,
            user_metadata: r.user_metadata,
            delete_marker: r.delete_marker,
            _deleted: false,
        }
    }
}

impl From<JsonObject> for ObjectRecord {
    fn from(j: JsonObject) -> Self {
        ObjectRecord {
            bucket: j.bucket,
            key: j.key,
            size: j.size,
            etag: j.etag,
            content_type: j.content_type,
            content_encoding: j.content_encoding,
            content_language: j.content_language,
            content_disposition: j.content_disposition,
            cache_control: j.cache_control,
            expires: j.expires,
            storage_class: j.storage_class,
            acl: j.acl,
            last_modified: j.last_modified,
            user_metadata: j.user_metadata,
            delete_marker: j.delete_marker,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonUpload {
    upload_id: String,
    bucket: String,
    key: String,
    content_type: String,
    content_encoding: Option<String>,
    content_language: Option<String>,
    content_disposition: Option<String>,
    cache_control: Option<String>,
    expires: Option<String>,
    storage_class: String,
    acl: String,
    user_metadata: HashMap<String, String>,
    owner_id: String,
    owner_display: String,
    initiated_at: String,
    #[serde(default)]
    _deleted: bool,
}

impl From<MultipartUploadRecord> for JsonUpload {
    fn from(r: MultipartUploadRecord) -> Self {
        JsonUpload {
            upload_id: r.upload_id,
            bucket: r.bucket,
            key: r.key,
            content_type: r.content_type,
            content_encoding: r.content_encoding,
            content_language: r.content_language,
            content_disposition: r.content_disposition,
            cache_control: r.cache_control,
            expires: r.expires,
            storage_class: r.storage_class,
            acl: r.acl,
            user_metadata: r.user_metadata,
            owner_id: r.owner_id,
            owner_display: r.owner_display,
            initiated_at: r.initiated_at,
            _deleted: false,
        }
    }
}

impl From<JsonUpload> for MultipartUploadRecord {
    fn from(j: JsonUpload) -> Self {
        MultipartUploadRecord {
            upload_id: j.upload_id,
            bucket: j.bucket,
            key: j.key,
            content_type: j.content_type,
            content_encoding: j.content_encoding,
            content_language: j.content_language,
            content_disposition: j.content_disposition,
            cache_control: j.cache_control,
            expires: j.expires,
            storage_class: j.storage_class,
            acl: j.acl,
            user_metadata: j.user_metadata,
            owner_id: j.owner_id,
            owner_display: j.owner_display,
            initiated_at: j.initiated_at,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonPart {
    upload_id: String,
    part_number: u32,
    size: u64,
    etag: String,
    last_modified: String,
    #[serde(default)]
    _deleted: bool,
}

impl From<(String, PartRecord)> for JsonPart {
    fn from((upload_id, r): (String, PartRecord)) -> Self {
        JsonPart {
            upload_id,
            part_number: r.part_number,
            size: r.size,
            etag: r.etag,
            last_modified: r.last_modified,
            _deleted: false,
        }
    }
}

impl From<JsonPart> for (String, PartRecord) {
    fn from(j: JsonPart) -> Self {
        (
            j.upload_id,
            PartRecord {
                part_number: j.part_number,
                size: j.size,
                etag: j.etag,
                last_modified: j.last_modified,
            },
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonCredential {
    access_key_id: String,
    secret_key: String,
    owner_id: String,
    display_name: String,
    active: bool,
    created_at: String,
    #[serde(default)]
    _deleted: bool,
}

impl From<CredentialRecord> for JsonCredential {
    fn from(r: CredentialRecord) -> Self {
        JsonCredential {
            access_key_id: r.access_key_id,
            secret_key: r.secret_key,
            owner_id: r.owner_id,
            display_name: r.display_name,
            active: r.active,
            created_at: r.created_at,
            _deleted: false,
        }
    }
}

impl From<JsonCredential> for CredentialRecord {
    fn from(j: JsonCredential) -> Self {
        CredentialRecord {
            access_key_id: j.access_key_id,
            secret_key: j.secret_key,
            owner_id: j.owner_id,
            display_name: j.display_name,
            active: j.active,
            created_at: j.created_at,
        }
    }
}

struct Inner {
    buckets: HashMap<String, BucketRecord>,
    objects: HashMap<(String, String), ObjectRecord>,
    uploads: HashMap<String, MultipartUploadRecord>,
    parts: HashMap<(String, u32), PartRecord>,
    credentials: HashMap<String, CredentialRecord>,
}

pub struct LocalMetadataStore {
    root_dir: PathBuf,
    inner: Mutex<Inner>,
}

impl LocalMetadataStore {
    pub fn new(config: &LocalMetaConfig) -> anyhow::Result<Self> {
        let root_dir = PathBuf::from(&config.root_dir);
        std::fs::create_dir_all(&root_dir)?;

        let mut inner = Inner {
            buckets: HashMap::new(),
            objects: HashMap::new(),
            uploads: HashMap::new(),
            parts: HashMap::new(),
            credentials: HashMap::new(),
        };

        Self::load_buckets(&root_dir, &mut inner)?;
        Self::load_objects(&root_dir, &mut inner)?;
        Self::load_uploads(&root_dir, &mut inner)?;
        Self::load_parts(&root_dir, &mut inner)?;
        Self::load_credentials(&root_dir, &mut inner)?;

        if config.compact_on_startup {
            let store = Self {
                root_dir: root_dir.clone(),
                inner: Mutex::new(inner),
            };
            store.compact_all()?;
            return Ok(store);
        }

        Ok(Self {
            root_dir,
            inner: Mutex::new(inner),
        })
    }

    fn load_buckets(root_dir: &Path, inner: &mut Inner) -> anyhow::Result<()> {
        let path = root_dir.join("buckets.jsonl");
        if !path.exists() {
            return Ok(());
        }
        let file = std::fs::File::open(&path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let bucket: JsonBucket = serde_json::from_str(&line)?;
            if bucket._deleted {
                inner.buckets.remove(&bucket.name);
            } else {
                inner.buckets.insert(bucket.name.clone(), bucket.into());
            }
        }
        Ok(())
    }

    fn load_objects(root_dir: &Path, inner: &mut Inner) -> anyhow::Result<()> {
        let path = root_dir.join("objects.jsonl");
        if !path.exists() {
            return Ok(());
        }
        let file = std::fs::File::open(&path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let obj: JsonObject = serde_json::from_str(&line)?;
            let key = (obj.bucket.clone(), obj.key.clone());
            if obj._deleted {
                inner.objects.remove(&key);
            } else {
                inner.objects.insert(key, obj.into());
            }
        }
        Ok(())
    }

    fn load_uploads(root_dir: &Path, inner: &mut Inner) -> anyhow::Result<()> {
        let path = root_dir.join("uploads.jsonl");
        if !path.exists() {
            return Ok(());
        }
        let file = std::fs::File::open(&path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let upload: JsonUpload = serde_json::from_str(&line)?;
            if upload._deleted {
                inner.uploads.remove(&upload.upload_id);
            } else {
                inner
                    .uploads
                    .insert(upload.upload_id.clone(), upload.into());
            }
        }
        Ok(())
    }

    fn load_parts(root_dir: &Path, inner: &mut Inner) -> anyhow::Result<()> {
        let path = root_dir.join("parts.jsonl");
        if !path.exists() {
            return Ok(());
        }
        let file = std::fs::File::open(&path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let part: JsonPart = serde_json::from_str(&line)?;
            let key = (part.upload_id.clone(), part.part_number);
            if part._deleted {
                inner.parts.remove(&key);
            } else {
                let (_, pr): (String, PartRecord) = part.into();
                inner.parts.insert(key, pr);
            }
        }
        Ok(())
    }

    fn load_credentials(root_dir: &Path, inner: &mut Inner) -> anyhow::Result<()> {
        let path = root_dir.join("credentials.jsonl");
        if !path.exists() {
            return Ok(());
        }
        let file = std::fs::File::open(&path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let cred: JsonCredential = serde_json::from_str(&line)?;
            if cred._deleted {
                inner.credentials.remove(&cred.access_key_id);
            } else {
                inner
                    .credentials
                    .insert(cred.access_key_id.clone(), cred.into());
            }
        }
        Ok(())
    }

    fn append_line(&self, filename: &str, line: &str) -> anyhow::Result<()> {
        let path = self.root_dir.join(filename);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let mut writer = BufWriter::new(file);
        writeln!(writer, "{line}")?;
        writer.flush()?;
        Ok(())
    }

    fn compact_all(&self) -> anyhow::Result<()> {
        let inner = self.inner.lock().expect("mutex poisoned");
        self.compact_file(
            "buckets.jsonl",
            inner
                .buckets
                .values()
                .map(|b| serde_json::to_string(&JsonBucket::from(b.clone())).unwrap())
                .collect(),
        )?;
        self.compact_file(
            "objects.jsonl",
            inner
                .objects
                .values()
                .map(|o| serde_json::to_string(&JsonObject::from(o.clone())).unwrap())
                .collect(),
        )?;
        self.compact_file(
            "uploads.jsonl",
            inner
                .uploads
                .values()
                .map(|u| serde_json::to_string(&JsonUpload::from(u.clone())).unwrap())
                .collect(),
        )?;
        let parts: Vec<String> = inner
            .parts
            .iter()
            .map(|((upload_id, _), p)| {
                serde_json::to_string(&JsonPart::from((upload_id.clone(), p.clone()))).unwrap()
            })
            .collect();
        self.compact_file("parts.jsonl", parts)?;
        self.compact_file(
            "credentials.jsonl",
            inner
                .credentials
                .values()
                .map(|c| serde_json::to_string(&JsonCredential::from(c.clone())).unwrap())
                .collect(),
        )?;
        Ok(())
    }

    fn compact_file(&self, filename: &str, lines: Vec<String>) -> anyhow::Result<()> {
        let path = self.root_dir.join(filename);
        let parent = path.parent().expect("no parent dir");
        let temp = NamedTempFile::new_in(parent)?;
        {
            let mut writer = BufWriter::new(&temp);
            for line in &lines {
                writeln!(writer, "{line}")?;
            }
            writer.flush()?;
        }
        temp.persist(&path)?;
        Ok(())
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

        let mut inner = self.inner.lock().expect("mutex poisoned");
        if inner.credentials.contains_key(access_key) {
            return Ok(());
        }

        let json = serde_json::to_string(&JsonCredential::from(record.clone()))?;
        self.append_line("credentials.jsonl", &json)?;
        inner.credentials.insert(access_key.to_string(), record);
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

impl MetadataStore for LocalMetadataStore {
    fn create_bucket(
        &self,
        record: BucketRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let json = serde_json::to_string(&JsonBucket::from(record.clone()))?;
            self.append_line("buckets.jsonl", &json)?;
            let mut inner = self.inner.lock().expect("mutex poisoned");
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
            let inner = self.inner.lock().expect("mutex poisoned");
            Ok(inner.buckets.get(&name).cloned())
        })
    }

    fn bucket_exists(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let inner = self.inner.lock().expect("mutex poisoned");
            Ok(inner.buckets.contains_key(&name))
        })
    }

    fn list_buckets(
        &self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<BucketRecord>>> + Send + '_>> {
        Box::pin(async move {
            let inner = self.inner.lock().expect("mutex poisoned");
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
            let tombstone = JsonBucket {
                name: name.clone(),
                created_at: String::new(),
                region: String::new(),
                owner_id: String::new(),
                owner_display: String::new(),
                acl: String::new(),
                _deleted: true,
            };
            let json = serde_json::to_string(&tombstone)?;
            self.append_line("buckets.jsonl", &json)?;
            let mut inner = self.inner.lock().expect("mutex poisoned");
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
            let mut inner = self.inner.lock().expect("mutex poisoned");
            if let Some(bucket) = inner.buckets.get_mut(&name) {
                bucket.acl = acl.clone();
                let json = serde_json::to_string(&JsonBucket::from(bucket.clone()))?;
                drop(inner);
                self.append_line("buckets.jsonl", &json)?;
            }
            Ok(())
        })
    }

    fn put_object(
        &self,
        record: ObjectRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let json = serde_json::to_string(&JsonObject::from(record.clone()))?;
            self.append_line("objects.jsonl", &json)?;
            let mut inner = self.inner.lock().expect("mutex poisoned");
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
            let inner = self.inner.lock().expect("mutex poisoned");
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
            let inner = self.inner.lock().expect("mutex poisoned");
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
            let inner = self.inner.lock().expect("mutex poisoned");

            let effective_start = continuation_token.as_deref().unwrap_or(&start_after);

            let mut all_objects: Vec<ObjectRecord> = inner
                .objects
                .iter()
                .filter(|((b, k), _)| {
                    *b == bucket && k.as_str() > effective_start && k.starts_with(&prefix)
                })
                .map(|(_, obj)| obj.clone())
                .collect();

            all_objects.sort_by(|a, b| a.key.cmp(&b.key));

            if delimiter.is_empty() {
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
                    objects
                        .last()
                        .map(|o| o.key.clone())
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
            let tombstone = JsonObject {
                bucket: bucket.clone(),
                key: key.clone(),
                size: 0,
                etag: String::new(),
                content_type: String::new(),
                content_encoding: None,
                content_language: None,
                content_disposition: None,
                cache_control: None,
                expires: None,
                storage_class: String::new(),
                acl: String::new(),
                last_modified: String::new(),
                user_metadata: HashMap::new(),
                delete_marker: false,
                _deleted: true,
            };
            let json = serde_json::to_string(&tombstone)?;
            self.append_line("objects.jsonl", &json)?;
            let mut inner = self.inner.lock().expect("mutex poisoned");
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
            let mut inner = self.inner.lock().expect("mutex poisoned");
            for key in &keys {
                let tombstone = JsonObject {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    size: 0,
                    etag: String::new(),
                    content_type: String::new(),
                    content_encoding: None,
                    content_language: None,
                    content_disposition: None,
                    cache_control: None,
                    expires: None,
                    storage_class: String::new(),
                    acl: String::new(),
                    last_modified: String::new(),
                    user_metadata: HashMap::new(),
                    delete_marker: false,
                    _deleted: true,
                };
                let json = serde_json::to_string(&tombstone)?;
                drop(inner);
                self.append_line("objects.jsonl", &json)?;
                inner = self.inner.lock().expect("mutex poisoned");
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
            let mut inner = self.inner.lock().expect("mutex poisoned");
            if let Some(obj) = inner.objects.get_mut(&(bucket, key)) {
                obj.acl = acl.clone();
                let json = serde_json::to_string(&JsonObject::from(obj.clone()))?;
                drop(inner);
                self.append_line("objects.jsonl", &json)?;
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
            let inner = self.inner.lock().expect("mutex poisoned");
            let count = inner.objects.keys().filter(|(b, _)| *b == bucket).count();
            Ok(count as u64)
        })
    }

    fn create_multipart_upload(
        &self,
        record: MultipartUploadRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let json = serde_json::to_string(&JsonUpload::from(record.clone()))?;
            self.append_line("uploads.jsonl", &json)?;
            let mut inner = self.inner.lock().expect("mutex poisoned");
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
            let inner = self.inner.lock().expect("mutex poisoned");
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
            let jp = JsonPart::from((upload_id.clone(), part.clone()));
            let json = serde_json::to_string(&jp)?;
            self.append_line("parts.jsonl", &json)?;
            let mut inner = self.inner.lock().expect("mutex poisoned");
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
            let inner = self.inner.lock().expect("mutex poisoned");
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
            let inner = self.inner.lock().expect("mutex poisoned");
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
            let obj_json = serde_json::to_string(&JsonObject::from(final_object.clone()))?;
            self.append_line("objects.jsonl", &obj_json)?;

            let upload_tombstone = JsonUpload {
                upload_id: upload_id.clone(),
                bucket: String::new(),
                key: String::new(),
                content_type: String::new(),
                content_encoding: None,
                content_language: None,
                content_disposition: None,
                cache_control: None,
                expires: None,
                storage_class: String::new(),
                acl: String::new(),
                user_metadata: HashMap::new(),
                owner_id: String::new(),
                owner_display: String::new(),
                initiated_at: String::new(),
                _deleted: true,
            };
            let upload_json = serde_json::to_string(&upload_tombstone)?;
            self.append_line("uploads.jsonl", &upload_json)?;

            let mut inner = self.inner.lock().expect("mutex poisoned");
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
            let tombstone = JsonUpload {
                upload_id: upload_id.clone(),
                bucket: String::new(),
                key: String::new(),
                content_type: String::new(),
                content_encoding: None,
                content_language: None,
                content_disposition: None,
                cache_control: None,
                expires: None,
                storage_class: String::new(),
                acl: String::new(),
                user_metadata: HashMap::new(),
                owner_id: String::new(),
                owner_display: String::new(),
                initiated_at: String::new(),
                _deleted: true,
            };
            let json = serde_json::to_string(&tombstone)?;
            self.append_line("uploads.jsonl", &json)?;

            let mut inner = self.inner.lock().expect("mutex poisoned");
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
            let inner = self.inner.lock().expect("mutex poisoned");

            let mut uploads: Vec<MultipartUploadRecord> = inner
                .uploads
                .iter()
                .filter(|(_, u)| {
                    u.bucket == bucket
                        && u.key.starts_with(&prefix)
                        && (key_marker.is_empty()
                            || u.key > key_marker
                            || (u.key == key_marker
                                && !upload_id_marker.is_empty()
                                && u.upload_id > upload_id_marker))
                })
                .map(|(_, u)| u.clone())
                .collect();

            uploads.sort_by(|a, b| {
                a.key
                    .cmp(&b.key)
                    .then_with(|| a.upload_id.cmp(&b.upload_id))
            });

            let is_truncated = uploads.len() > max_uploads as usize;
            if is_truncated {
                uploads.truncate(max_uploads as usize);
            }

            let (next_key_marker, next_upload_id_marker) = if is_truncated {
                uploads
                    .last()
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
            let inner = self.inner.lock().expect("mutex poisoned");
            Ok(inner.credentials.get(&access_key_id).cloned())
        })
    }

    fn put_credential(
        &self,
        record: CredentialRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let json = serde_json::to_string(&JsonCredential::from(record.clone()))?;
            self.append_line("credentials.jsonl", &json)?;
            let mut inner = self.inner.lock().expect("mutex poisoned");
            inner
                .credentials
                .insert(record.access_key_id.clone(), record);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_store() -> (LocalMetadataStore, TempDir) {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let config = LocalMetaConfig {
            root_dir: tmp.path().to_str().unwrap().to_string(),
            compact_on_startup: false,
        };
        let store = LocalMetadataStore::new(&config).expect("failed to create store");
        (store, tmp)
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
        let (store, _tmp) = test_store();
        let bucket = make_bucket("test-bucket");
        store.create_bucket(bucket).await.unwrap();

        let fetched = store.get_bucket("test-bucket").await.unwrap();
        assert!(fetched.is_some());
        let b = fetched.unwrap();
        assert_eq!(b.name, "test-bucket");
    }

    #[tokio::test]
    async fn test_bucket_persistence() {
        let (store, tmp) = test_store();
        store
            .create_bucket(make_bucket("persist-bucket"))
            .await
            .unwrap();

        let config = LocalMetaConfig {
            root_dir: tmp.path().to_str().unwrap().to_string(),
            compact_on_startup: false,
        };
        let store2 = LocalMetadataStore::new(&config).expect("failed to reload");

        let fetched = store2.get_bucket("persist-bucket").await.unwrap();
        assert!(fetched.is_some());
    }

    #[tokio::test]
    async fn test_delete_bucket() {
        let (store, _tmp) = test_store();
        store.create_bucket(make_bucket("to-delete")).await.unwrap();
        assert!(store.bucket_exists("to-delete").await.unwrap());

        store.delete_bucket("to-delete").await.unwrap();
        assert!(!store.bucket_exists("to-delete").await.unwrap());
    }

    #[tokio::test]
    async fn test_put_and_get_object() {
        let (store, _tmp) = test_store();
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
    async fn test_delete_object() {
        let (store, _tmp) = test_store();
        store.create_bucket(make_bucket("mybucket")).await.unwrap();
        store
            .put_object(make_object("mybucket", "delete-me.txt", 1))
            .await
            .unwrap();

        assert!(store
            .object_exists("mybucket", "delete-me.txt")
            .await
            .unwrap());
        store
            .delete_object("mybucket", "delete-me.txt")
            .await
            .unwrap();
        assert!(!store
            .object_exists("mybucket", "delete-me.txt")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_credentials() {
        let (store, _tmp) = test_store();
        store.seed_credential("test-key", "test-secret").unwrap();

        let cred = store.get_credential("test-key").await.unwrap();
        assert!(cred.is_some());
        let c = cred.unwrap();
        assert_eq!(c.secret_key, "test-secret");
        assert!(c.active);
    }

    #[tokio::test]
    async fn test_compaction() {
        let (store, tmp) = test_store();
        store
            .create_bucket(make_bucket("compact-test"))
            .await
            .unwrap();
        store.delete_bucket("compact-test").await.unwrap();

        let buckets_path = tmp.path().join("buckets.jsonl");
        let content = std::fs::read_to_string(&buckets_path).unwrap();
        assert!(content.contains("\"_deleted\":true"));

        store.compact_all().unwrap();

        let content_after = std::fs::read_to_string(&buckets_path).unwrap();
        assert!(!content_after.contains("\"_deleted\":true"));
    }
}
