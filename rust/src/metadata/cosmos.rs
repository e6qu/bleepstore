//! Azure Cosmos DB metadata store backend.
//!
//! Single-container design with partition key /type:
//! - Bucket:     id=bucket_{name}, type=bucket
//! - Object:     id=object_{bucket}_{key}, type=object
//! - Upload:     id=upload_{upload_id}, type=upload
//! - Part:       id=part_{upload_id}_{number:05d}, type=upload (same partition)
//! - Credential: id=cred_{access_key}, type=credential

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use super::store::{
    BucketRecord, CredentialRecord, ListObjectsResult, ListPartsResult, ListUploadsResult,
    MetadataStore, MultipartUploadRecord, ObjectRecord, PartRecord,
};
use crate::config::CosmosMetaConfig;

fn doc_id_bucket(bucket: &str) -> String {
    format!("bucket_{bucket}")
}

fn doc_id_object(bucket: &str, key: &str) -> String {
    format!("object_{bucket}_{key}")
}

fn doc_id_upload(upload_id: &str) -> String {
    format!("upload_{upload_id}")
}

fn doc_id_part(upload_id: &str, part_number: u32) -> String {
    format!("part_{upload_id}_{part_number:05}")
}

fn doc_id_credential(access_key: &str) -> String {
    format!("cred_{access_key}")
}

fn now_iso() -> String {
    let now = std::time::SystemTime::now();
    let since_epoch = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = since_epoch.as_secs();
    let millis = since_epoch.subsec_millis();
    format_timestamp(secs, millis)
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

#[derive(Serialize, Deserialize)]
struct BucketDoc {
    id: String,
    #[serde(rename = "type")]
    doc_type: String,
    name: String,
    region: String,
    owner_id: String,
    owner_display: String,
    acl: String,
    created_at: String,
}

#[derive(Serialize, Deserialize)]
struct ObjectDoc {
    id: String,
    #[serde(rename = "type")]
    doc_type: String,
    bucket: String,
    key: String,
    size: u64,
    etag: String,
    content_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_disposition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires: Option<String>,
    storage_class: String,
    acl: String,
    user_metadata: String,
    last_modified: String,
}

#[derive(Serialize, Deserialize)]
struct UploadDoc {
    id: String,
    #[serde(rename = "type")]
    doc_type: String,
    upload_id: String,
    bucket: String,
    key: String,
    content_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_disposition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires: Option<String>,
    storage_class: String,
    acl: String,
    user_metadata: String,
    owner_id: String,
    owner_display: String,
    initiated_at: String,
}

#[derive(Serialize, Deserialize)]
struct PartDoc {
    id: String,
    #[serde(rename = "type")]
    doc_type: String,
    upload_id: String,
    part_number: u32,
    size: u64,
    etag: String,
    last_modified: String,
}

#[derive(Serialize, Deserialize)]
struct CredentialDoc {
    id: String,
    #[serde(rename = "type")]
    doc_type: String,
    access_key_id: String,
    secret_key: String,
    owner_id: String,
    display_name: String,
    active: bool,
    created_at: String,
}

fn serialize_user_metadata(meta: &HashMap<String, String>) -> String {
    serde_json::to_string(meta).unwrap_or_else(|_| "{}".to_string())
}

fn deserialize_user_metadata(json: &str) -> HashMap<String, String> {
    serde_json::from_str(json).unwrap_or_default()
}

pub struct CosmosMetadataStore {
    client: reqwest::Client,
    endpoint: String,
    database: String,
    container: String,
    master_key: Option<String>,
}

impl CosmosMetadataStore {
    pub async fn new(config: &CosmosMetaConfig) -> anyhow::Result<Self> {
        let endpoint = config.endpoint.clone().unwrap_or_default();
        let database = config
            .database
            .clone()
            .unwrap_or_else(|| "bleepstore".to_string());
        let container = config
            .container_prefix
            .clone()
            .unwrap_or_else(|| "metadata".to_string());

        let master_key = config.connection_string.as_ref().and_then(|cs| {
            cs.split(';').find_map(|part| {
                if part.starts_with("AccountKey=") {
                    Some(part[11..].to_string())
                } else {
                    None
                }
            })
        });

        let store = Self {
            client: reqwest::Client::new(),
            endpoint: endpoint.trim_end_matches('/').to_string(),
            database,
            container,
            master_key,
        };

        store.ensure_container().await?;
        Ok(store)
    }

    async fn ensure_container(&self) -> anyhow::Result<()> {
        let url = format!(
            "{}/dbs/{}/colls/{}",
            self.endpoint, self.database, self.container
        );

        let date = httpdate::fmt_http_date(std::time::SystemTime::now());
        let auth = self.auth_header(
            "GET",
            &format!("dbs/{}/colls/{}", self.database, self.container),
            &date,
        );

        let resp = self
            .client
            .get(&url)
            .header("Authorization", &auth)
            .header("x-ms-date", &date)
            .header("x-ms-version", "2018-12-31")
            .send()
            .await?;

        if resp.status().as_u16() == 404 {
            self.create_database_and_container().await?;
        }

        Ok(())
    }

    async fn create_database_and_container(&self) -> anyhow::Result<()> {
        let db_url = format!("{}/dbs/{}", self.endpoint, self.database);
        let date = httpdate::fmt_http_date(std::time::SystemTime::now());
        let auth = self.auth_header("GET", &format!("dbs/{}", self.database), &date);

        let resp = self
            .client
            .get(&db_url)
            .header("Authorization", &auth)
            .header("x-ms-date", &date)
            .header("x-ms-version", "2018-12-31")
            .send()
            .await?;

        if resp.status().as_u16() == 404 {
            let create_db_url = format!("{}/dbs", self.endpoint);
            let date = httpdate::fmt_http_date(std::time::SystemTime::now());
            let auth = self.auth_header("POST", "dbs", &date);

            self.client
                .post(&create_db_url)
                .header("Authorization", &auth)
                .header("x-ms-date", &date)
                .header("x-ms-version", "2018-12-31")
                .header("Content-Type", "application/json")
                .json(&serde_json::json!({ "id": self.database }))
                .send()
                .await?;
        }

        let colls_url = format!("{}/dbs/{}/colls", self.endpoint, self.database);
        let date = httpdate::fmt_http_date(std::time::SystemTime::now());
        let auth = self.auth_header("POST", &format!("dbs/{}", self.database), &date);

        self.client
            .post(&colls_url)
            .header("Authorization", &auth)
            .header("x-ms-date", &date)
            .header("x-ms-version", "2018-12-31")
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "id": self.container,
                "partitionKey": {
                    "paths": ["/type"],
                    "kind": "Hash"
                }
            }))
            .send()
            .await?;

        Ok(())
    }

    fn auth_header(&self, method: &str, resource: &str, date: &str) -> String {
        if let Some(ref key) = self.master_key {
            let string_to_sign = format!(
                "{}\n{}\n{}\n{}\n{}\n",
                method.to_lowercase(),
                resource.to_lowercase(),
                date,
                "",
                ""
            );

            use base64::Engine;
            let key_bytes = base64::engine::general_purpose::STANDARD
                .decode(key)
                .unwrap_or_default();
            let hmac = hmac_sha256(&key_bytes, string_to_sign.as_bytes());
            let sig = base64::engine::general_purpose::STANDARD.encode(&hmac);
            format!(
                "type=master&ver=1.0&sig={}",
                percent_encoding::percent_encode(
                    sig.as_bytes(),
                    percent_encoding::NON_ALPHANUMERIC
                )
            )
        } else {
            String::new()
        }
    }

    async fn read_item<T: for<'de> Deserialize<'de>>(
        &self,
        id: &str,
        partition_key: &str,
    ) -> anyhow::Result<Option<T>> {
        let url = format!(
            "{}/dbs/{}/colls/{}/docs/{}",
            self.endpoint, self.database, self.container, id
        );

        let date = httpdate::fmt_http_date(std::time::SystemTime::now());
        let resource = format!("dbs/{}/colls/{}/docs", self.database, self.container);
        let auth = self.auth_header("GET", &resource, &date);

        let resp = self
            .client
            .get(&url)
            .header("Authorization", &auth)
            .header("x-ms-date", &date)
            .header("x-ms-version", "2018-12-31")
            .header(
                "x-ms-documentdb-partitionkey",
                format!("[\"{}\"]", partition_key),
            )
            .send()
            .await?;

        if resp.status().as_u16() == 404 {
            return Ok(None);
        }

        let doc: T = resp.json().await?;
        Ok(Some(doc))
    }

    async fn create_item<T: Serialize>(&self, item: &T) -> anyhow::Result<()> {
        let url = format!(
            "{}/dbs/{}/colls/{}/docs",
            self.endpoint, self.database, self.container
        );

        let date = httpdate::fmt_http_date(std::time::SystemTime::now());
        let resource = format!("dbs/{}/colls/{}/docs", self.database, self.container);
        let auth = self.auth_header("POST", &resource, &date);

        let resp = self
            .client
            .post(&url)
            .header("Authorization", &auth)
            .header("x-ms-date", &date)
            .header("x-ms-version", "2018-12-31")
            .header("Content-Type", "application/json")
            .header("x-ms-documentdb-is-upsert", "false")
            .json(item)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Failed to create item: {} - {}", status, body);
        }

        Ok(())
    }

    async fn upsert_item<T: Serialize>(&self, item: &T) -> anyhow::Result<()> {
        let url = format!(
            "{}/dbs/{}/colls/{}/docs",
            self.endpoint, self.database, self.container
        );

        let date = httpdate::fmt_http_date(std::time::SystemTime::now());
        let resource = format!("dbs/{}/colls/{}/docs", self.database, self.container);
        let auth = self.auth_header("POST", &resource, &date);

        let resp = self
            .client
            .post(&url)
            .header("Authorization", &auth)
            .header("x-ms-date", &date)
            .header("x-ms-version", "2018-12-31")
            .header("Content-Type", "application/json")
            .header("x-ms-documentdb-is-upsert", "true")
            .json(item)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Failed to upsert item: {} - {}", status, body);
        }

        Ok(())
    }

    async fn replace_item<T: Serialize>(
        &self,
        id: &str,
        partition_key: &str,
        item: &T,
    ) -> anyhow::Result<()> {
        let url = format!(
            "{}/dbs/{}/colls/{}/docs/{}",
            self.endpoint, self.database, self.container, id
        );

        let date = httpdate::fmt_http_date(std::time::SystemTime::now());
        let resource = format!("dbs/{}/colls/{}/docs", self.database, self.container);
        let auth = self.auth_header("PUT", &resource, &date);

        let resp = self
            .client
            .put(&url)
            .header("Authorization", &auth)
            .header("x-ms-date", &date)
            .header("x-ms-version", "2018-12-31")
            .header("Content-Type", "application/json")
            .header(
                "x-ms-documentdb-partitionkey",
                format!("[\"{}\"]", partition_key),
            )
            .json(item)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Failed to replace item: {} - {}", status, body);
        }

        Ok(())
    }

    async fn delete_item(&self, id: &str, partition_key: &str) -> anyhow::Result<()> {
        let url = format!(
            "{}/dbs/{}/colls/{}/docs/{}",
            self.endpoint, self.database, self.container, id
        );

        let date = httpdate::fmt_http_date(std::time::SystemTime::now());
        let resource = format!("dbs/{}/colls/{}/docs", self.database, self.container);
        let auth = self.auth_header("DELETE", &resource, &date);

        let resp = self
            .client
            .delete(&url)
            .header("Authorization", &auth)
            .header("x-ms-date", &date)
            .header("x-ms-version", "2018-12-31")
            .header(
                "x-ms-documentdb-partitionkey",
                format!("[\"{}\"]", partition_key),
            )
            .send()
            .await?;

        if !resp.status().is_success() && resp.status().as_u16() != 404 {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Failed to delete item: {} - {}", status, body);
        }

        Ok(())
    }

    async fn query_items<T: for<'de> Deserialize<'de>>(
        &self,
        query: &str,
        params: Vec<(&str, serde_json::Value)>,
        partition_key: Option<&str>,
    ) -> anyhow::Result<Vec<T>> {
        let url = format!(
            "{}/dbs/{}/colls/{}/docs",
            self.endpoint, self.database, self.container
        );

        let date = httpdate::fmt_http_date(std::time::SystemTime::now());
        let resource = format!("dbs/{}/colls/{}/docs", self.database, self.container);
        let auth = self.auth_header("POST", &resource, &date);

        let parameters: Vec<serde_json::Value> = params
            .into_iter()
            .map(|(name, value)| serde_json::json!({ "name": name, "value": value }))
            .collect();

        let body = serde_json::json!({
            "query": query,
            "parameters": parameters
        });

        let mut req = self
            .client
            .post(&url)
            .header("Authorization", &auth)
            .header("x-ms-date", &date)
            .header("x-ms-version", "2018-12-31")
            .header("Content-Type", "application/query+json")
            .header("x-ms-documentdb-isquery", "true")
            .header("x-ms-max-item-count", "1000")
            .json(&body);

        if let Some(pk) = partition_key {
            req = req.header("x-ms-documentdb-partitionkey", format!("[\"{}\"]", pk));
        }

        let resp = req.send().await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Query failed: {} - {}", status, body);
        }

        #[derive(Deserialize)]
        struct QueryResponse<T> {
            #[serde(rename = "Documents")]
            documents: Vec<T>,
        }

        let result: QueryResponse<T> = resp.json().await?;
        Ok(result.documents)
    }

    pub fn seed_credential(&self, _access_key: &str, _secret_key: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn get_parts_for_completion_internal(
        &self,
        upload_id: &str,
    ) -> anyhow::Result<Vec<PartDoc>> {
        let prefix = format!("part_{upload_id}_");
        let query = "SELECT * FROM c WHERE c.type = 'upload' AND STARTSWITH(c.id, @prefix)";
        let params = vec![("@prefix", serde_json::Value::String(prefix))];

        let parts: Vec<PartDoc> = self.query_items(query, params, Some("upload")).await?;

        let mut sorted_parts = parts;
        sorted_parts.sort_by_key(|p| p.part_number);
        Ok(sorted_parts)
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

impl MetadataStore for CosmosMetadataStore {
    fn create_bucket(
        &self,
        record: BucketRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let doc = BucketDoc {
                id: doc_id_bucket(&record.name),
                doc_type: "bucket".to_string(),
                name: record.name,
                region: record.region,
                owner_id: record.owner_id,
                owner_display: record.owner_display,
                acl: record.acl,
                created_at: record.created_at,
            };
            self.create_item(&doc).await
        })
    }

    fn get_bucket(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<BucketRecord>>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let doc: Option<BucketDoc> = self.read_item(&doc_id_bucket(&name), "bucket").await?;
            Ok(doc.map(|d| BucketRecord {
                name: d.name,
                region: d.region,
                owner_id: d.owner_id,
                owner_display: d.owner_display,
                acl: d.acl,
                created_at: d.created_at,
            }))
        })
    }

    fn bucket_exists(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let doc: Option<BucketDoc> = self.read_item(&doc_id_bucket(&name), "bucket").await?;
            Ok(doc.is_some())
        })
    }

    fn list_buckets(
        &self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<BucketRecord>>> + Send + '_>> {
        Box::pin(async move {
            let query = "SELECT * FROM c WHERE c.type = 'bucket'";
            let docs: Vec<BucketDoc> = self.query_items(query, vec![], None).await?;
            Ok(docs
                .into_iter()
                .map(|d| BucketRecord {
                    name: d.name,
                    region: d.region,
                    owner_id: d.owner_id,
                    owner_display: d.owner_display,
                    acl: d.acl,
                    created_at: d.created_at,
                })
                .collect())
        })
    }

    fn delete_bucket(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let _ = self.delete_item(&doc_id_bucket(&name), "bucket").await;
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
            let mut doc: BucketDoc = self
                .read_item(&doc_id_bucket(&name), "bucket")
                .await?
                .ok_or_else(|| anyhow::anyhow!("Bucket not found"))?;
            doc.acl = acl;
            self.replace_item(&doc.id, "bucket", &doc).await
        })
    }

    fn put_object(
        &self,
        record: ObjectRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let doc = ObjectDoc {
                id: doc_id_object(&record.bucket, &record.key),
                doc_type: "object".to_string(),
                bucket: record.bucket,
                key: record.key,
                size: record.size,
                etag: record.etag,
                content_type: record.content_type,
                content_encoding: record.content_encoding,
                content_language: record.content_language,
                content_disposition: record.content_disposition,
                cache_control: record.cache_control,
                expires: record.expires,
                storage_class: record.storage_class,
                acl: record.acl,
                user_metadata: serialize_user_metadata(&record.user_metadata),
                last_modified: record.last_modified,
            };
            self.upsert_item(&doc).await
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
            let doc: Option<ObjectDoc> = self
                .read_item(&doc_id_object(&bucket, &key), "object")
                .await?;
            Ok(doc.map(|d| ObjectRecord {
                bucket: d.bucket,
                key: d.key,
                size: d.size,
                etag: d.etag,
                content_type: d.content_type,
                content_encoding: d.content_encoding,
                content_language: d.content_language,
                content_disposition: d.content_disposition,
                cache_control: d.cache_control,
                expires: d.expires,
                storage_class: d.storage_class,
                acl: d.acl,
                last_modified: d.last_modified,
                user_metadata: deserialize_user_metadata(&d.user_metadata),
                delete_marker: false,
            }))
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
            let doc: Option<ObjectDoc> = self
                .read_item(&doc_id_object(&bucket, &key), "object")
                .await?;
            Ok(doc.is_some())
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
            if max_keys == 0 {
                return Ok(ListObjectsResult {
                    objects: Vec::new(),
                    common_prefixes: Vec::new(),
                    next_continuation_token: None,
                    is_truncated: false,
                });
            }

            let effective_start = continuation_token.as_ref().unwrap_or(&start_after);
            let prefix_filter = format!("object_{}_{prefix}", bucket);

            let mut query =
                "SELECT * FROM c WHERE c.type = 'object' AND c.bucket = @bucket".to_string();
            let mut params: Vec<(&str, serde_json::Value)> =
                vec![("@bucket", serde_json::Value::String(bucket.clone()))];

            if !prefix.is_empty() {
                query.push_str(" AND STARTSWITH(c.id, @prefix)");
                params.push(("@prefix", serde_json::Value::String(prefix_filter)));
            }

            if !effective_start.is_empty() {
                query.push_str(" AND c.id > @start_after");
                params.push((
                    "@start_after",
                    serde_json::Value::String(doc_id_object(&bucket, effective_start)),
                ));
            }

            query.push_str(" ORDER BY c.id");

            let docs: Vec<ObjectDoc> = self.query_items(&query, params, Some("object")).await?;

            let mut all_objects: Vec<ObjectRecord> = docs
                .into_iter()
                .take(max_keys as usize + 1)
                .map(|d| ObjectRecord {
                    bucket: d.bucket,
                    key: d.key,
                    size: d.size,
                    etag: d.etag,
                    content_type: d.content_type,
                    content_encoding: d.content_encoding,
                    content_language: d.content_language,
                    content_disposition: d.content_disposition,
                    cache_control: d.cache_control,
                    expires: d.expires,
                    storage_class: d.storage_class,
                    acl: d.acl,
                    last_modified: d.last_modified,
                    user_metadata: deserialize_user_metadata(&d.user_metadata),
                    delete_marker: false,
                })
                .collect();

            if !delimiter.is_empty() {
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
                } else {
                    None
                };

                return Ok(ListObjectsResult {
                    objects,
                    common_prefixes: common_prefixes.into_iter().collect(),
                    next_continuation_token: next_token,
                    is_truncated,
                });
            }

            let is_truncated = all_objects.len() > max_keys as usize;
            if is_truncated {
                all_objects.truncate(max_keys as usize);
            }

            let next_token = if is_truncated {
                all_objects.last().map(|o| o.key.clone())
            } else {
                None
            };

            Ok(ListObjectsResult {
                objects: all_objects,
                common_prefixes: Vec::new(),
                next_continuation_token: next_token,
                is_truncated,
            })
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
            let _ = self
                .delete_item(&doc_id_object(&bucket, &key), "object")
                .await;
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
            let mut deleted = Vec::new();
            for key in &keys {
                let _ = self
                    .delete_item(&doc_id_object(&bucket, key), "object")
                    .await;
                deleted.push(key.clone());
            }
            Ok(deleted)
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
            let mut doc: ObjectDoc = self
                .read_item(&doc_id_object(&bucket, &key), "object")
                .await?
                .ok_or_else(|| anyhow::anyhow!("Object not found"))?;
            doc.acl = acl;
            self.replace_item(&doc.id, "object", &doc).await
        })
    }

    fn count_objects(
        &self,
        bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<u64>> + Send + '_>> {
        let bucket = bucket.to_string();
        Box::pin(async move {
            let query =
                "SELECT VALUE COUNT(1) FROM c WHERE c.type = 'object' AND c.bucket = @bucket";
            let params = vec![("@bucket", serde_json::Value::String(bucket))];

            let results: Vec<serde_json::Value> = self.query_items(query, params, None).await?;
            let count = results.first().and_then(|v| v.as_u64()).unwrap_or(0);
            Ok(count)
        })
    }

    fn create_multipart_upload(
        &self,
        record: MultipartUploadRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let doc = UploadDoc {
                id: doc_id_upload(&record.upload_id),
                doc_type: "upload".to_string(),
                upload_id: record.upload_id,
                bucket: record.bucket,
                key: record.key,
                content_type: record.content_type,
                content_encoding: record.content_encoding,
                content_language: record.content_language,
                content_disposition: record.content_disposition,
                cache_control: record.cache_control,
                expires: record.expires,
                storage_class: record.storage_class,
                acl: record.acl,
                user_metadata: serialize_user_metadata(&record.user_metadata),
                owner_id: record.owner_id,
                owner_display: record.owner_display,
                initiated_at: record.initiated_at,
            };
            self.create_item(&doc).await
        })
    }

    fn get_multipart_upload(
        &self,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<MultipartUploadRecord>>> + Send + '_>>
    {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let doc: Option<UploadDoc> =
                self.read_item(&doc_id_upload(&upload_id), "upload").await?;
            Ok(doc.map(|d| MultipartUploadRecord {
                upload_id: d.upload_id,
                bucket: d.bucket,
                key: d.key,
                content_type: d.content_type,
                content_encoding: d.content_encoding,
                content_language: d.content_language,
                content_disposition: d.content_disposition,
                cache_control: d.cache_control,
                expires: d.expires,
                storage_class: d.storage_class,
                acl: d.acl,
                user_metadata: deserialize_user_metadata(&d.user_metadata),
                owner_id: d.owner_id,
                owner_display: d.owner_display,
                initiated_at: d.initiated_at,
            }))
        })
    }

    fn put_part(
        &self,
        upload_id: &str,
        part: PartRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let doc = PartDoc {
                id: doc_id_part(&upload_id, part.part_number),
                doc_type: "upload".to_string(),
                upload_id,
                part_number: part.part_number,
                size: part.size,
                etag: part.etag,
                last_modified: part.last_modified,
            };
            self.upsert_item(&doc).await
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
            let prefix = format!("part_{upload_id}_");
            let mut query =
                "SELECT * FROM c WHERE c.type = 'upload' AND STARTSWITH(c.id, @prefix)".to_string();
            let mut params: Vec<(&str, serde_json::Value)> =
                vec![("@prefix", serde_json::Value::String(prefix))];

            if part_number_marker > 0 {
                query.push_str(" AND c.id > @start_after");
                params.push((
                    "@start_after",
                    serde_json::Value::String(doc_id_part(&upload_id, part_number_marker)),
                ));
            }

            query.push_str(" ORDER BY c.id");

            let docs: Vec<PartDoc> = self.query_items(&query, params, Some("upload")).await?;

            let mut parts: Vec<PartRecord> = docs
                .into_iter()
                .take(max_parts as usize + 1)
                .map(|d| PartRecord {
                    part_number: d.part_number,
                    size: d.size,
                    etag: d.etag,
                    last_modified: d.last_modified,
                })
                .collect();

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
            let docs = self.get_parts_for_completion_internal(&upload_id).await?;
            Ok(docs
                .into_iter()
                .map(|d| PartRecord {
                    part_number: d.part_number,
                    size: d.size,
                    etag: d.etag,
                    last_modified: d.last_modified,
                })
                .collect())
        })
    }

    fn complete_multipart_upload(
        &self,
        upload_id: &str,
        final_object: ObjectRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            self.put_object(final_object).await?;

            let parts = self.get_parts_for_completion_internal(&upload_id).await?;
            for part in parts {
                let _ = self.delete_item(&part.id, "upload").await;
            }

            let _ = self.delete_item(&doc_id_upload(&upload_id), "upload").await;

            Ok(())
        })
    }

    fn delete_multipart_upload(
        &self,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let parts = self.get_parts_for_completion_internal(&upload_id).await?;
            for part in parts {
                let _ = self.delete_item(&part.id, "upload").await;
            }

            let _ = self.delete_item(&doc_id_upload(&upload_id), "upload").await;

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
            let mut query = "SELECT * FROM c WHERE c.type = 'upload' AND c.bucket = @bucket AND c.upload_id IS NOT NULL".to_string();
            let mut params: Vec<(&str, serde_json::Value)> =
                vec![("@bucket", serde_json::Value::String(bucket.clone()))];

            if !prefix.is_empty() {
                query.push_str(" AND STARTSWITH(c.key, @prefix)");
                params.push(("@prefix", serde_json::Value::String(prefix)));
            }

            if !key_marker.is_empty() {
                query.push_str(" AND (c.key > @key_marker OR (c.key = @key_marker AND c.upload_id > @upload_id_marker))");
                params.push(("@key_marker", serde_json::Value::String(key_marker)));
                params.push((
                    "@upload_id_marker",
                    serde_json::Value::String(upload_id_marker),
                ));
            }

            query.push_str(" ORDER BY c.key, c.upload_id");

            let docs: Vec<UploadDoc> = self.query_items(&query, params, None).await?;

            let mut uploads: Vec<MultipartUploadRecord> = docs
                .into_iter()
                .take(max_uploads as usize + 1)
                .map(|d| MultipartUploadRecord {
                    upload_id: d.upload_id,
                    bucket: d.bucket,
                    key: d.key,
                    content_type: d.content_type,
                    content_encoding: d.content_encoding,
                    content_language: d.content_language,
                    content_disposition: d.content_disposition,
                    cache_control: d.cache_control,
                    expires: d.expires,
                    storage_class: d.storage_class,
                    acl: d.acl,
                    user_metadata: deserialize_user_metadata(&d.user_metadata),
                    owner_id: d.owner_id,
                    owner_display: d.owner_display,
                    initiated_at: d.initiated_at,
                })
                .collect();

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
            let doc: Option<CredentialDoc> = self
                .read_item(&doc_id_credential(&access_key_id), "credential")
                .await?;

            Ok(doc.and_then(|d| {
                if d.active {
                    Some(CredentialRecord {
                        access_key_id: d.access_key_id,
                        secret_key: d.secret_key,
                        owner_id: d.owner_id,
                        display_name: d.display_name,
                        active: d.active,
                        created_at: d.created_at,
                    })
                } else {
                    None
                }
            }))
        })
    }

    fn put_credential(
        &self,
        record: CredentialRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let doc = CredentialDoc {
                id: doc_id_credential(&record.access_key_id),
                doc_type: "credential".to_string(),
                access_key_id: record.access_key_id,
                secret_key: record.secret_key,
                owner_id: record.owner_id,
                display_name: record.display_name,
                active: record.active,
                created_at: record.created_at,
            };
            self.upsert_item(&doc).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_test_config() -> Option<CosmosMetaConfig> {
        let endpoint = std::env::var("COSMOS_TEST_ENDPOINT").ok()?;
        let key = std::env::var("COSMOS_TEST_KEY").ok()?;

        Some(CosmosMetaConfig {
            database: Some("bleepstore_test".to_string()),
            container_prefix: Some("metadata_test".to_string()),
            endpoint: Some(endpoint),
            connection_string: Some(format!("AccountKey={}", key)),
        })
    }

    fn make_bucket(name: &str) -> BucketRecord {
        BucketRecord {
            name: name.to_string(),
            created_at: now_iso(),
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
            last_modified: now_iso(),
            user_metadata: HashMap::new(),
            delete_marker: false,
        }
    }

    #[tokio::test]
    async fn test_bucket_crud() {
        let config = match get_test_config() {
            Some(c) => c,
            None => {
                eprintln!("Skipping test: COSMOS_TEST_ENDPOINT and COSMOS_TEST_KEY not set");
                return;
            }
        };

        let store = CosmosMetadataStore::new(&config)
            .await
            .expect("Failed to create store");

        let bucket_name = format!("test-bucket-{}", std::process::id());
        let bucket = make_bucket(&bucket_name);

        store
            .create_bucket(bucket.clone())
            .await
            .expect("Failed to create bucket");

        let exists = store
            .bucket_exists(&bucket_name)
            .await
            .expect("Failed to check bucket");
        assert!(exists);

        let fetched = store
            .get_bucket(&bucket_name)
            .await
            .expect("Failed to get bucket");
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().name, bucket_name);

        store
            .delete_bucket(&bucket_name)
            .await
            .expect("Failed to delete bucket");

        let exists = store
            .bucket_exists(&bucket_name)
            .await
            .expect("Failed to check bucket");
        assert!(!exists);
    }

    #[tokio::test]
    async fn test_object_crud() {
        let config = match get_test_config() {
            Some(c) => c,
            None => {
                eprintln!("Skipping test: COSMOS_TEST_ENDPOINT and COSMOS_TEST_KEY not set");
                return;
            }
        };

        let store = CosmosMetadataStore::new(&config)
            .await
            .expect("Failed to create store");

        let bucket_name = format!("test-obj-bucket-{}", std::process::id());
        store
            .create_bucket(make_bucket(&bucket_name))
            .await
            .expect("Failed to create bucket");

        let obj = make_object(&bucket_name, "test/key.txt", 100);
        store
            .put_object(obj.clone())
            .await
            .expect("Failed to put object");

        let exists = store
            .object_exists(&bucket_name, "test/key.txt")
            .await
            .expect("Failed to check object");
        assert!(exists);

        let fetched = store
            .get_object(&bucket_name, "test/key.txt")
            .await
            .expect("Failed to get object");
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().key, "test/key.txt");

        store
            .delete_object(&bucket_name, "test/key.txt")
            .await
            .expect("Failed to delete object");

        let exists = store
            .object_exists(&bucket_name, "test/key.txt")
            .await
            .expect("Failed to check object");
        assert!(!exists);

        store
            .delete_bucket(&bucket_name)
            .await
            .expect("Failed to delete bucket");
    }

    #[tokio::test]
    async fn test_multipart_upload() {
        let config = match get_test_config() {
            Some(c) => c,
            None => {
                eprintln!("Skipping test: COSMOS_TEST_ENDPOINT and COSMOS_TEST_KEY not set");
                return;
            }
        };

        let store = CosmosMetadataStore::new(&config)
            .await
            .expect("Failed to create store");

        let bucket_name = format!("test-mp-bucket-{}", std::process::id());
        store
            .create_bucket(make_bucket(&bucket_name))
            .await
            .expect("Failed to create bucket");

        let upload_id = uuid::Uuid::new_v4().to_string();
        let upload = MultipartUploadRecord {
            upload_id: upload_id.clone(),
            bucket: bucket_name.clone(),
            key: "test/multipart.dat".to_string(),
            content_type: "application/octet-stream".to_string(),
            content_encoding: None,
            content_language: None,
            content_disposition: None,
            cache_control: None,
            expires: None,
            storage_class: "STANDARD".to_string(),
            acl: "{}".to_string(),
            user_metadata: HashMap::new(),
            owner_id: "test".to_string(),
            owner_display: "Test".to_string(),
            initiated_at: now_iso(),
        };

        store
            .create_multipart_upload(upload.clone())
            .await
            .expect("Failed to create upload");

        let fetched = store
            .get_multipart_upload(&upload_id)
            .await
            .expect("Failed to get upload");
        assert!(fetched.is_some());

        let part = PartRecord {
            part_number: 1,
            size: 1024,
            etag: "\"etag-1\"".to_string(),
            last_modified: now_iso(),
        };
        store
            .put_part(&upload_id, part)
            .await
            .expect("Failed to put part");

        let parts = store
            .get_parts_for_completion(&upload_id)
            .await
            .expect("Failed to get parts");
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, 1);

        let final_obj = make_object(&bucket_name, "test/multipart.dat", 1024);
        store
            .complete_multipart_upload(&upload_id, final_obj)
            .await
            .expect("Failed to complete upload");

        let exists = store
            .object_exists(&bucket_name, "test/multipart.dat")
            .await
            .expect("Failed to check object");
        assert!(exists);

        let fetched = store
            .get_multipart_upload(&upload_id)
            .await
            .expect("Failed to get upload");
        assert!(fetched.is_none());

        store
            .delete_object(&bucket_name, "test/multipart.dat")
            .await
            .expect("Failed to delete object");
        store
            .delete_bucket(&bucket_name)
            .await
            .expect("Failed to delete bucket");
    }

    #[tokio::test]
    async fn test_credentials() {
        let config = match get_test_config() {
            Some(c) => c,
            None => {
                eprintln!("Skipping test: COSMOS_TEST_ENDPOINT and COSMOS_TEST_KEY not set");
                return;
            }
        };

        let store = CosmosMetadataStore::new(&config)
            .await
            .expect("Failed to create store");

        let access_key = format!("test-key-{}", std::process::id());
        let cred = CredentialRecord {
            access_key_id: access_key.clone(),
            secret_key: "secret123".to_string(),
            owner_id: "test".to_string(),
            display_name: "Test User".to_string(),
            active: true,
            created_at: now_iso(),
        };

        store
            .put_credential(cred)
            .await
            .expect("Failed to put credential");

        let fetched = store
            .get_credential(&access_key)
            .await
            .expect("Failed to get credential");
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().secret_key, "secret123");
    }

    #[tokio::test]
    async fn test_list_objects_with_prefix() {
        let config = match get_test_config() {
            Some(c) => c,
            None => {
                eprintln!("Skipping test: COSMOS_TEST_ENDPOINT and COSMOS_TEST_KEY not set");
                return;
            }
        };

        let store = CosmosMetadataStore::new(&config)
            .await
            .expect("Failed to create store");

        let bucket_name = format!("test-list-bucket-{}", std::process::id());
        store
            .create_bucket(make_bucket(&bucket_name))
            .await
            .expect("Failed to create bucket");

        for i in 0..5 {
            let obj = make_object(&bucket_name, &format!("prefix/obj{}.txt", i), 100);
            store.put_object(obj).await.expect("Failed to put object");
        }
        for i in 0..3 {
            let obj = make_object(&bucket_name, &format!("other/obj{}.txt", i), 100);
            store.put_object(obj).await.expect("Failed to put object");
        }

        let result = store
            .list_objects(&bucket_name, "prefix/", "", 1000, "", None)
            .await
            .expect("Failed to list objects");
        assert_eq!(result.objects.len(), 5);

        let result = store
            .list_objects(&bucket_name, "other/", "", 1000, "", None)
            .await
            .expect("Failed to list objects");
        assert_eq!(result.objects.len(), 3);

        for i in 0..5 {
            store
                .delete_object(&bucket_name, &format!("prefix/obj{}.txt", i))
                .await
                .expect("Failed to delete");
        }
        for i in 0..3 {
            store
                .delete_object(&bucket_name, &format!("other/obj{}.txt", i))
                .await
                .expect("Failed to delete");
        }
        store
            .delete_bucket(&bucket_name)
            .await
            .expect("Failed to delete bucket");
    }
}
