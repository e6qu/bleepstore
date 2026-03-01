//! AWS DynamoDB metadata store backend.
//!
//! Single-table design with PK/SK pattern:
//! - Bucket:    PK=BUCKET#{name},         SK=#METADATA
//! - Object:    PK=OBJECT#{bucket}#{key}, SK=#METADATA
//! - Upload:    PK=UPLOAD#{upload_id},    SK=#METADATA
//! - Part:      PK=UPLOAD#{upload_id},    SK=PART#{part_number:05d}
//! - Credential: PK=CRED#{access_key},    SK=#METADATA

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;

use super::store::{
    BucketRecord, CredentialRecord, ListObjectsResult, ListPartsResult, ListUploadsResult,
    MetadataStore, MultipartUploadRecord, ObjectRecord, PartRecord,
};
use crate::config::DynamoDbMetaConfig;

fn pk_bucket(bucket: &str) -> String {
    format!("BUCKET#{bucket}")
}

fn pk_object(bucket: &str, key: &str) -> String {
    format!("OBJECT#{bucket}#{key}")
}

fn pk_upload(upload_id: &str) -> String {
    format!("UPLOAD#{upload_id}")
}

fn pk_credential(access_key: &str) -> String {
    format!("CRED#{access_key}")
}

fn sk_metadata() -> &'static str {
    "#METADATA"
}

fn sk_part(part_number: u32) -> String {
    format!("PART#{part_number:05}")
}

fn item_to_record(item: &HashMap<String, AttributeValue>) -> HashMap<String, String> {
    let mut result = HashMap::new();
    for (key, value) in item {
        if let AttributeValue::S(s) = value {
            result.insert(key.clone(), s.clone());
        } else if let AttributeValue::N(n) = value {
            result.insert(key.clone(), n.clone());
        } else if let AttributeValue::Bool(b) = value {
            result.insert(key.clone(), b.to_string());
        }
    }
    result
}

pub struct DynamoDbMetadataStore {
    client: Client,
    table_name: String,
}

impl DynamoDbMetadataStore {
    pub async fn new(config: &DynamoDbMetaConfig) -> anyhow::Result<Self> {
        let mut builder = aws_config::defaults(aws_config::BehaviorVersion::latest());

        if let Some(region) = &config.region {
            builder = builder.region(aws_config::Region::new(region.clone()));
        }

        if let Some(endpoint_url) = &config.endpoint_url {
            builder = builder.endpoint_url(endpoint_url);
        }

        let cfg = builder.load().await;
        let client = Client::new(&cfg);

        let table_name = config
            .table_prefix
            .as_ref()
            .map(|p| format!("{}_metadata", p))
            .unwrap_or_else(|| "bleepstore_metadata".to_string());

        Ok(Self { client, table_name })
    }

    pub fn seed_credential(&self, _access_key: &str, _secret_key: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

impl MetadataStore for DynamoDbMetadataStore {
    fn create_bucket(
        &self,
        record: BucketRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut item = HashMap::new();
            item.insert("pk".to_string(), AttributeValue::S(pk_bucket(&record.name)));
            item.insert("sk".to_string(), AttributeValue::S(sk_metadata().to_string()));
            item.insert("type".to_string(), AttributeValue::S("bucket".to_string()));
            item.insert("name".to_string(), AttributeValue::S(record.name));
            item.insert("region".to_string(), AttributeValue::S(record.region));
            item.insert("owner_id".to_string(), AttributeValue::S(record.owner_id));
            item.insert(
                "owner_display".to_string(),
                AttributeValue::S(record.owner_display),
            );
            item.insert("acl".to_string(), AttributeValue::S(record.acl));
            item.insert(
                "created_at".to_string(),
                AttributeValue::S(record.created_at),
            );

            self.client
                .put_item()
                .table_name(&self.table_name)
                .set_item(Some(item))
                .condition_expression("attribute_not_exists(pk)")
                .send()
                .await?;

            Ok(())
        })
    }

    fn get_bucket(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<BucketRecord>>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let result = self
                .client
                .get_item()
                .table_name(&self.table_name)
                .key("pk", AttributeValue::S(pk_bucket(&name)))
                .key("sk", AttributeValue::S(sk_metadata().to_string()))
                .send()
                .await?;

            if let Some(item) = result.item() {
                let map = item_to_record(item);
                Ok(Some(BucketRecord {
                    name: map.get("name").cloned().unwrap_or_default(),
                    created_at: map.get("created_at").cloned().unwrap_or_default(),
                    region: map.get("region").cloned().unwrap_or_else(|| "us-east-1".to_string()),
                    owner_id: map.get("owner_id").cloned().unwrap_or_default(),
                    owner_display: map.get("owner_display").cloned().unwrap_or_default(),
                    acl: map.get("acl").cloned().unwrap_or_else(|| "{}".to_string()),
                }))
            } else {
                Ok(None)
            }
        })
    }

    fn bucket_exists(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let result = self
                .client
                .get_item()
                .table_name(&self.table_name)
                .key("pk", AttributeValue::S(pk_bucket(&name)))
                .key("sk", AttributeValue::S(sk_metadata().to_string()))
                .projection_expression("pk")
                .send()
                .await?;

            Ok(result.item().is_some())
        })
    }

    fn list_buckets(
        &self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<BucketRecord>>> + Send + '_>> {
        Box::pin(async move {
            let mut buckets = Vec::new();
            let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

            loop {
                let mut query = self
                    .client
                    .scan()
                    .table_name(&self.table_name)
                    .filter_expression("begins_with(pk, :prefix) AND sk = :metadata")
                    .expression_attribute_values(":prefix", AttributeValue::S("BUCKET#".to_string()))
                    .expression_attribute_values(":metadata", AttributeValue::S(sk_metadata().to_string()));

                if let Some(key) = &exclusive_start_key {
                    query = query.set_exclusive_start_key(Some(key.clone()));
                }

                let result = query.send().await?;

                for item in result.items() {
                    let map = item_to_record(item);
                    buckets.push(BucketRecord {
                        name: map.get("name").cloned().unwrap_or_default(),
                        created_at: map.get("created_at").cloned().unwrap_or_default(),
                        region: map.get("region").cloned().unwrap_or_else(|| "us-east-1".to_string()),
                        owner_id: map.get("owner_id").cloned().unwrap_or_default(),
                        owner_display: map.get("owner_display").cloned().unwrap_or_default(),
                        acl: map.get("acl").cloned().unwrap_or_else(|| "{}".to_string()),
                    });
                }

                if result.last_evaluated_key().is_none() {
                    break;
                }
                exclusive_start_key = result.last_evaluated_key().cloned();
            }

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
            self.client
                .delete_item()
                .table_name(&self.table_name)
                .key("pk", AttributeValue::S(pk_bucket(&name)))
                .key("sk", AttributeValue::S(sk_metadata().to_string()))
                .send()
                .await?;
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
            self.client
                .update_item()
                .table_name(&self.table_name)
                .key("pk", AttributeValue::S(pk_bucket(&name)))
                .key("sk", AttributeValue::S(sk_metadata().to_string()))
                .update_expression("SET acl = :acl")
                .expression_attribute_values(":acl", AttributeValue::S(acl))
                .send()
                .await?;
            Ok(())
        })
    }

    fn put_object(
        &self,
        record: ObjectRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut item = HashMap::new();
            item.insert(
                "pk".to_string(),
                AttributeValue::S(pk_object(&record.bucket, &record.key)),
            );
            item.insert("sk".to_string(), AttributeValue::S(sk_metadata().to_string()));
            item.insert("type".to_string(), AttributeValue::S("object".to_string()));
            item.insert("bucket".to_string(), AttributeValue::S(record.bucket));
            item.insert("key".to_string(), AttributeValue::S(record.key));
            item.insert("size".to_string(), AttributeValue::N(record.size.to_string()));
            item.insert("etag".to_string(), AttributeValue::S(record.etag));
            item.insert(
                "content_type".to_string(),
                AttributeValue::S(record.content_type),
            );
            item.insert(
                "storage_class".to_string(),
                AttributeValue::S(record.storage_class),
            );
            item.insert("acl".to_string(), AttributeValue::S(record.acl));

            let user_meta: String =
                serde_json::to_string(&record.user_metadata).unwrap_or_else(|_| "{}".to_string());
            item.insert("user_metadata".to_string(), AttributeValue::S(user_meta));
            item.insert(
                "last_modified".to_string(),
                AttributeValue::S(record.last_modified),
            );

            if let Some(ce) = record.content_encoding {
                item.insert("content_encoding".to_string(), AttributeValue::S(ce));
            }
            if let Some(cl) = record.content_language {
                item.insert("content_language".to_string(), AttributeValue::S(cl));
            }
            if let Some(cd) = record.content_disposition {
                item.insert("content_disposition".to_string(), AttributeValue::S(cd));
            }
            if let Some(cc) = record.cache_control {
                item.insert("cache_control".to_string(), AttributeValue::S(cc));
            }
            if let Some(e) = record.expires {
                item.insert("expires".to_string(), AttributeValue::S(e));
            }

            self.client
                .put_item()
                .table_name(&self.table_name)
                .set_item(Some(item))
                .send()
                .await?;

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
            let result = self
                .client
                .get_item()
                .table_name(&self.table_name)
                .key("pk", AttributeValue::S(pk_object(&bucket, &key)))
                .key("sk", AttributeValue::S(sk_metadata().to_string()))
                .send()
                .await?;

            if let Some(item) = result.item() {
                let map = item_to_record(item);
                let user_meta: HashMap<String, String> = map
                    .get("user_metadata")
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_default();

                Ok(Some(ObjectRecord {
                    bucket: map.get("bucket").cloned().unwrap_or_default(),
                    key: map.get("key").cloned().unwrap_or_default(),
                    size: map.get("size").and_then(|s| s.parse().ok()).unwrap_or(0),
                    etag: map.get("etag").cloned().unwrap_or_default(),
                    content_type: map
                        .get("content_type")
                        .cloned()
                        .unwrap_or_else(|| "application/octet-stream".to_string()),
                    content_encoding: map.get("content_encoding").cloned(),
                    content_language: map.get("content_language").cloned(),
                    content_disposition: map.get("content_disposition").cloned(),
                    cache_control: map.get("cache_control").cloned(),
                    expires: map.get("expires").cloned(),
                    storage_class: map
                        .get("storage_class")
                        .cloned()
                        .unwrap_or_else(|| "STANDARD".to_string()),
                    acl: map.get("acl").cloned().unwrap_or_else(|| "{}".to_string()),
                    last_modified: map.get("last_modified").cloned().unwrap_or_default(),
                    user_metadata: user_meta,
                    delete_marker: map.get("delete_marker").map(|s| s == "true").unwrap_or(false),
                }))
            } else {
                Ok(None)
            }
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
            let result = self
                .client
                .get_item()
                .table_name(&self.table_name)
                .key("pk", AttributeValue::S(pk_object(&bucket, &key)))
                .key("sk", AttributeValue::S(sk_metadata().to_string()))
                .projection_expression("pk")
                .send()
                .await?;

            Ok(result.item().is_some())
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

            let effective_start = continuation_token.as_deref().unwrap_or(&start_after);
            let prefix_filter = format!("OBJECT#{bucket}#{prefix}");

            let mut objects: Vec<ObjectRecord> = Vec::new();
            let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

            while objects.len() <= max_keys as usize {
                let mut query = self
                    .client
                    .scan()
                    .table_name(&self.table_name)
                    .filter_expression("begins_with(pk, :prefix) AND sk = :metadata")
                    .expression_attribute_values(":prefix", AttributeValue::S(prefix_filter.clone()))
                    .expression_attribute_values(":metadata", AttributeValue::S(sk_metadata().to_string()))
                    .limit((max_keys + 1) as i32);

                if let Some(key) = &exclusive_start_key {
                    query = query.set_exclusive_start_key(Some(key.clone()));
                }

                let result = query.send().await?;

                for item in result.items() {
                    let map = item_to_record(item);
                    let key = map.get("key").cloned().unwrap_or_default();
                    if key.as_str() > effective_start {
                        let user_meta: HashMap<String, String> = map
                            .get("user_metadata")
                            .and_then(|s| serde_json::from_str(s).ok())
                            .unwrap_or_default();

                        objects.push(ObjectRecord {
                            bucket: map.get("bucket").cloned().unwrap_or_default(),
                            key: key.clone(),
                            size: map.get("size").and_then(|s| s.parse().ok()).unwrap_or(0),
                            etag: map.get("etag").cloned().unwrap_or_default(),
                            content_type: map
                                .get("content_type")
                                .cloned()
                                .unwrap_or_else(|| "application/octet-stream".to_string()),
                            content_encoding: map.get("content_encoding").cloned(),
                            content_language: map.get("content_language").cloned(),
                            content_disposition: map.get("content_disposition").cloned(),
                            cache_control: map.get("cache_control").cloned(),
                            expires: map.get("expires").cloned(),
                            storage_class: map
                                .get("storage_class")
                                .cloned()
                                .unwrap_or_else(|| "STANDARD".to_string()),
                            acl: map.get("acl").cloned().unwrap_or_else(|| "{}".to_string()),
                            last_modified: map.get("last_modified").cloned().unwrap_or_default(),
                            user_metadata: user_meta,
                            delete_marker: map.get("delete_marker").map(|s| s == "true").unwrap_or(false),
                        });
                    }
                    if objects.len() > max_keys as usize {
                        break;
                    }
                }

                if result.last_evaluated_key().is_none() {
                    break;
                }
                exclusive_start_key = result.last_evaluated_key().cloned();

                if objects.len() > max_keys as usize {
                    break;
                }
            }

            if !delimiter.is_empty() {
                let mut common_prefixes = std::collections::BTreeSet::new();
                let mut final_objects = Vec::new();
                let mut count = 0u32;

                for obj in objects {
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
                        final_objects.push(obj);
                        count += 1;
                    }
                }

                let is_truncated = count >= max_keys;
                let next_token = if is_truncated {
                    final_objects.last().map(|o| o.key.clone())
                } else {
                    None
                };

                return Ok(ListObjectsResult {
                    objects: final_objects,
                    common_prefixes: common_prefixes.into_iter().collect(),
                    next_continuation_token: next_token,
                    is_truncated,
                });
            }

            let is_truncated = objects.len() > max_keys as usize;
            if is_truncated {
                objects.truncate(max_keys as usize);
            }

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
            self.client
                .delete_item()
                .table_name(&self.table_name)
                .key("pk", AttributeValue::S(pk_object(&bucket, &key)))
                .key("sk", AttributeValue::S(sk_metadata().to_string()))
                .send()
                .await?;
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
            let deleted: Vec<String> = keys.clone();
            for chunk in keys.chunks(25) {
                let write_requests: Vec<_> = chunk
                    .iter()
                    .map(|key| {
                        aws_sdk_dynamodb::types::WriteRequest::builder()
                            .delete_request(
                                aws_sdk_dynamodb::types::DeleteRequest::builder()
                                    .key("pk", AttributeValue::S(pk_object(&bucket, key)))
                                    .key("sk", AttributeValue::S(sk_metadata().to_string()))
                                    .build()
                                    .expect("failed to build delete request"),
                            )
                            .build()
                    })
                    .collect();

                let _ = self
                    .client
                    .batch_write_item()
                    .request_items(&self.table_name, write_requests)
                    .send()
                    .await;
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
            self.client
                .update_item()
                .table_name(&self.table_name)
                .key("pk", AttributeValue::S(pk_object(&bucket, &key)))
                .key("sk", AttributeValue::S(sk_metadata().to_string()))
                .update_expression("SET acl = :acl")
                .expression_attribute_values(":acl", AttributeValue::S(acl))
                .send()
                .await?;
            Ok(())
        })
    }

    fn count_objects(
        &self,
        bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<u64>> + Send + '_>> {
        let bucket = bucket.to_string();
        Box::pin(async move {
            let prefix = format!("OBJECT#{bucket}#");
            let mut count = 0u64;
            let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

            loop {
                let mut query = self
                    .client
                    .scan()
                    .table_name(&self.table_name)
                    .filter_expression("begins_with(pk, :prefix) AND sk = :metadata")
                    .expression_attribute_values(":prefix", AttributeValue::S(prefix.clone()))
                    .expression_attribute_values(":metadata", AttributeValue::S(sk_metadata().to_string()))
                    .select(aws_sdk_dynamodb::types::Select::Count);

                if let Some(key) = &exclusive_start_key {
                    query = query.set_exclusive_start_key(Some(key.clone()));
                }

                let result = query.send().await?;
                count += result.count() as u64;

                if result.last_evaluated_key().is_none() {
                    break;
                }
                exclusive_start_key = result.last_evaluated_key().cloned();
            }

            Ok(count)
        })
    }

    fn create_multipart_upload(
        &self,
        record: MultipartUploadRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut item = HashMap::new();
            item.insert(
                "pk".to_string(),
                AttributeValue::S(pk_upload(&record.upload_id)),
            );
            item.insert("sk".to_string(), AttributeValue::S(sk_metadata().to_string()));
            item.insert("type".to_string(), AttributeValue::S("upload".to_string()));
            item.insert("upload_id".to_string(), AttributeValue::S(record.upload_id));
            item.insert("bucket".to_string(), AttributeValue::S(record.bucket));
            item.insert("key".to_string(), AttributeValue::S(record.key));
            item.insert(
                "content_type".to_string(),
                AttributeValue::S(record.content_type),
            );
            item.insert(
                "storage_class".to_string(),
                AttributeValue::S(record.storage_class),
            );
            item.insert("acl".to_string(), AttributeValue::S(record.acl));

            let user_meta: String =
                serde_json::to_string(&record.user_metadata).unwrap_or_else(|_| "{}".to_string());
            item.insert("user_metadata".to_string(), AttributeValue::S(user_meta));
            item.insert("owner_id".to_string(), AttributeValue::S(record.owner_id));
            item.insert(
                "owner_display".to_string(),
                AttributeValue::S(record.owner_display),
            );
            item.insert(
                "initiated_at".to_string(),
                AttributeValue::S(record.initiated_at),
            );

            if let Some(ce) = record.content_encoding {
                item.insert("content_encoding".to_string(), AttributeValue::S(ce));
            }
            if let Some(cl) = record.content_language {
                item.insert("content_language".to_string(), AttributeValue::S(cl));
            }
            if let Some(cd) = record.content_disposition {
                item.insert("content_disposition".to_string(), AttributeValue::S(cd));
            }
            if let Some(cc) = record.cache_control {
                item.insert("cache_control".to_string(), AttributeValue::S(cc));
            }
            if let Some(e) = record.expires {
                item.insert("expires".to_string(), AttributeValue::S(e));
            }

            self.client
                .put_item()
                .table_name(&self.table_name)
                .set_item(Some(item))
                .send()
                .await?;

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
            let result = self
                .client
                .get_item()
                .table_name(&self.table_name)
                .key("pk", AttributeValue::S(pk_upload(&upload_id)))
                .key("sk", AttributeValue::S(sk_metadata().to_string()))
                .send()
                .await?;

            if let Some(item) = result.item() {
                let map = item_to_record(item);
                let user_meta: HashMap<String, String> = map
                    .get("user_metadata")
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_default();

                Ok(Some(MultipartUploadRecord {
                    upload_id: map.get("upload_id").cloned().unwrap_or_default(),
                    bucket: map.get("bucket").cloned().unwrap_or_default(),
                    key: map.get("key").cloned().unwrap_or_default(),
                    content_type: map
                        .get("content_type")
                        .cloned()
                        .unwrap_or_else(|| "application/octet-stream".to_string()),
                    content_encoding: map.get("content_encoding").cloned(),
                    content_language: map.get("content_language").cloned(),
                    content_disposition: map.get("content_disposition").cloned(),
                    cache_control: map.get("cache_control").cloned(),
                    expires: map.get("expires").cloned(),
                    storage_class: map
                        .get("storage_class")
                        .cloned()
                        .unwrap_or_else(|| "STANDARD".to_string()),
                    acl: map.get("acl").cloned().unwrap_or_else(|| "{}".to_string()),
                    user_metadata: user_meta,
                    owner_id: map.get("owner_id").cloned().unwrap_or_default(),
                    owner_display: map.get("owner_display").cloned().unwrap_or_default(),
                    initiated_at: map.get("initiated_at").cloned().unwrap_or_default(),
                }))
            } else {
                Ok(None)
            }
        })
    }

    fn put_part(
        &self,
        upload_id: &str,
        part: PartRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let mut item = HashMap::new();
            item.insert("pk".to_string(), AttributeValue::S(pk_upload(&upload_id)));
            item.insert(
                "sk".to_string(),
                AttributeValue::S(sk_part(part.part_number)),
            );
            item.insert("type".to_string(), AttributeValue::S("part".to_string()));
            item.insert("upload_id".to_string(), AttributeValue::S(upload_id));
            item.insert(
                "part_number".to_string(),
                AttributeValue::N(part.part_number.to_string()),
            );
            item.insert("size".to_string(), AttributeValue::N(part.size.to_string()));
            item.insert("etag".to_string(), AttributeValue::S(part.etag));
            item.insert(
                "last_modified".to_string(),
                AttributeValue::S(part.last_modified),
            );

            self.client
                .put_item()
                .table_name(&self.table_name)
                .set_item(Some(item))
                .send()
                .await?;

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
            let start_sk = if part_number_marker > 0 {
                sk_part(part_number_marker + 1)
            } else {
                "PART#".to_string()
            };

            let mut parts: Vec<PartRecord> = Vec::new();
            let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

            while parts.len() <= max_parts as usize {
                let mut query = self
                    .client
                    .query()
                    .table_name(&self.table_name)
                    .key_condition_expression("pk = :pk AND sk >= :start_sk")
                    .expression_attribute_values(":pk", AttributeValue::S(pk_upload(&upload_id)))
                    .expression_attribute_values(":start_sk", AttributeValue::S(start_sk.clone()))
                    .limit((max_parts + 1) as i32);

                if let Some(key) = &exclusive_start_key {
                    query = query.set_exclusive_start_key(Some(key.clone()));
                }

                let result = query.send().await?;

                for item in result.items() {
                    let map = item_to_record(item);
                    if map.get("type").map(|t| t == "part").unwrap_or(false) {
                        parts.push(PartRecord {
                            part_number: map
                                .get("part_number")
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0),
                            size: map.get("size").and_then(|s| s.parse().ok()).unwrap_or(0),
                            etag: map.get("etag").cloned().unwrap_or_default(),
                            last_modified: map.get("last_modified").cloned().unwrap_or_default(),
                        });
                    }
                    if parts.len() > max_parts as usize {
                        break;
                    }
                }

                if result.last_evaluated_key().is_none() {
                    break;
                }
                exclusive_start_key = result.last_evaluated_key().cloned();
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
            let mut parts: Vec<PartRecord> = Vec::new();
            let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

            loop {
                let mut query = self
                    .client
                    .query()
                    .table_name(&self.table_name)
                    .key_condition_expression("pk = :pk AND begins_with(sk, :part_prefix)")
                    .expression_attribute_values(":pk", AttributeValue::S(pk_upload(&upload_id)))
                    .expression_attribute_values(":part_prefix", AttributeValue::S("PART#".to_string()));

                if let Some(key) = &exclusive_start_key {
                    query = query.set_exclusive_start_key(Some(key.clone()));
                }

                let result = query.send().await?;

                for item in result.items() {
                    let map = item_to_record(item);
                    if map.get("type").map(|t| t == "part").unwrap_or(false) {
                        parts.push(PartRecord {
                            part_number: map
                                .get("part_number")
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0),
                            size: map.get("size").and_then(|s| s.parse().ok()).unwrap_or(0),
                            etag: map.get("etag").cloned().unwrap_or_default(),
                            last_modified: map.get("last_modified").cloned().unwrap_or_default(),
                        });
                    }
                }

                if result.last_evaluated_key().is_none() {
                    break;
                }
                exclusive_start_key = result.last_evaluated_key().cloned();
            }

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
            self.put_object(final_object).await?;

            let parts = self.get_parts_for_completion(&upload_id).await?;

            for chunk in parts.chunks(25) {
                let write_requests: Vec<_> = chunk
                    .iter()
                    .map(|part| {
                        aws_sdk_dynamodb::types::WriteRequest::builder()
                            .delete_request(
                                aws_sdk_dynamodb::types::DeleteRequest::builder()
                                    .key("pk", AttributeValue::S(pk_upload(&upload_id)))
                                    .key("sk", AttributeValue::S(sk_part(part.part_number)))
                                    .build()
                                    .expect("failed to build delete request"),
                            )
                            .build()
                    })
                    .collect();

                let _ = self
                    .client
                    .batch_write_item()
                    .request_items(&self.table_name, write_requests)
                    .send()
                    .await;
            }

            self.client
                .delete_item()
                .table_name(&self.table_name)
                .key("pk", AttributeValue::S(pk_upload(&upload_id)))
                .key("sk", AttributeValue::S(sk_metadata().to_string()))
                .send()
                .await?;

            Ok(())
        })
    }

    fn delete_multipart_upload(
        &self,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let parts = self.get_parts_for_completion(&upload_id).await?;

            for chunk in parts.chunks(25) {
                let write_requests: Vec<_> = chunk
                    .iter()
                    .map(|part| {
                        aws_sdk_dynamodb::types::WriteRequest::builder()
                            .delete_request(
                                aws_sdk_dynamodb::types::DeleteRequest::builder()
                                    .key("pk", AttributeValue::S(pk_upload(&upload_id)))
                                    .key("sk", AttributeValue::S(sk_part(part.part_number)))
                                    .build()
                                    .expect("failed to build delete request"),
                            )
                            .build()
                    })
                    .collect();

                let _ = self
                    .client
                    .batch_write_item()
                    .request_items(&self.table_name, write_requests)
                    .send()
                    .await;
            }

            self.client
                .delete_item()
                .table_name(&self.table_name)
                .key("pk", AttributeValue::S(pk_upload(&upload_id)))
                .key("sk", AttributeValue::S(sk_metadata().to_string()))
                .send()
                .await?;

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
            let mut uploads: Vec<MultipartUploadRecord> = Vec::new();
            let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

            while uploads.len() <= max_uploads as usize {
                let mut filter =
                    "begins_with(pk, :upload_prefix) AND sk = :metadata AND #bucket = :bucket"
                        .to_string();
                let mut expr_values = HashMap::new();
                let mut expr_names = HashMap::new();

                expr_values.insert(
                    ":upload_prefix".to_string(),
                    AttributeValue::S("UPLOAD#".to_string()),
                );
                expr_values.insert(
                    ":metadata".to_string(),
                    AttributeValue::S(sk_metadata().to_string()),
                );
                expr_values.insert(":bucket".to_string(), AttributeValue::S(bucket.clone()));
                expr_names.insert("#bucket".to_string(), "bucket".to_string());

                if !prefix.is_empty() {
                    filter += " AND begins_with(#key, :prefix)";
                    expr_values.insert(":prefix".to_string(), AttributeValue::S(prefix.clone()));
                    expr_names.insert("#key".to_string(), "key".to_string());
                }

                let mut query = self
                    .client
                    .scan()
                    .table_name(&self.table_name)
                    .filter_expression(filter)
                    .set_expression_attribute_values(Some(expr_values))
                    .set_expression_attribute_names(Some(expr_names))
                    .limit((max_uploads + 1) as i32);

                if let Some(key) = &exclusive_start_key {
                    query = query.set_exclusive_start_key(Some(key.clone()));
                }

                let result = query.send().await?;

                for item in result.items() {
                    let map = item_to_record(item);
                    let key = map.get("key").cloned().unwrap_or_default();
                    let uid = map.get("upload_id").cloned().unwrap_or_default();

                    let passes_marker = if !key_marker.is_empty() {
                        key.as_str() > key_marker.as_str()
                            || (key == key_marker && uid.as_str() > upload_id_marker.as_str())
                    } else {
                        true
                    };

                    if passes_marker {
                        let user_meta: HashMap<String, String> = map
                            .get("user_metadata")
                            .and_then(|s| serde_json::from_str(s).ok())
                            .unwrap_or_default();

                        uploads.push(MultipartUploadRecord {
                            upload_id: uid,
                            bucket: map.get("bucket").cloned().unwrap_or_default(),
                            key: key.clone(),
                            content_type: map
                                .get("content_type")
                                .cloned()
                                .unwrap_or_else(|| "application/octet-stream".to_string()),
                            content_encoding: map.get("content_encoding").cloned(),
                            content_language: map.get("content_language").cloned(),
                            content_disposition: map.get("content_disposition").cloned(),
                            cache_control: map.get("cache_control").cloned(),
                            expires: map.get("expires").cloned(),
                            storage_class: map
                                .get("storage_class")
                                .cloned()
                                .unwrap_or_else(|| "STANDARD".to_string()),
                            acl: map.get("acl").cloned().unwrap_or_else(|| "{}".to_string()),
                            user_metadata: user_meta,
                            owner_id: map.get("owner_id").cloned().unwrap_or_default(),
                            owner_display: map.get("owner_display").cloned().unwrap_or_default(),
                            initiated_at: map.get("initiated_at").cloned().unwrap_or_default(),
                        });
                    }

                    if uploads.len() > max_uploads as usize {
                        break;
                    }
                }

                if result.last_evaluated_key().is_none() {
                    break;
                }
                exclusive_start_key = result.last_evaluated_key().cloned();

                if uploads.len() > max_uploads as usize {
                    break;
                }
            }

            uploads.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.upload_id.cmp(&b.upload_id)));

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
            let result = self
                .client
                .get_item()
                .table_name(&self.table_name)
                .key("pk", AttributeValue::S(pk_credential(&access_key_id)))
                .key("sk", AttributeValue::S(sk_metadata().to_string()))
                .send()
                .await?;

            if let Some(item) = result.item() {
                let map = item_to_record(item);
                let active = map
                    .get("active")
                    .map(|s| s == "true")
                    .unwrap_or(true);
                if !active {
                    return Ok(None);
                }
                Ok(Some(CredentialRecord {
                    access_key_id: map.get("access_key_id").cloned().unwrap_or_default(),
                    secret_key: map.get("secret_key").cloned().unwrap_or_default(),
                    owner_id: map.get("owner_id").cloned().unwrap_or_default(),
                    display_name: map.get("display_name").cloned().unwrap_or_default(),
                    active,
                    created_at: map.get("created_at").cloned().unwrap_or_default(),
                }))
            } else {
                Ok(None)
            }
        })
    }

    fn put_credential(
        &self,
        record: CredentialRecord,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut item = HashMap::new();
            item.insert(
                "pk".to_string(),
                AttributeValue::S(pk_credential(&record.access_key_id)),
            );
            item.insert("sk".to_string(), AttributeValue::S(sk_metadata().to_string()));
            item.insert("type".to_string(), AttributeValue::S("credential".to_string()));
            item.insert(
                "access_key_id".to_string(),
                AttributeValue::S(record.access_key_id),
            );
            item.insert("secret_key".to_string(), AttributeValue::S(record.secret_key));
            item.insert("owner_id".to_string(), AttributeValue::S(record.owner_id));
            item.insert(
                "display_name".to_string(),
                AttributeValue::S(record.display_name),
            );
            item.insert("active".to_string(), AttributeValue::Bool(record.active));
            item.insert(
                "created_at".to_string(),
                AttributeValue::S(record.created_at),
            );

            self.client
                .put_item()
                .table_name(&self.table_name)
                .set_item(Some(item))
                .send()
                .await?;

            Ok(())
        })
    }
}
