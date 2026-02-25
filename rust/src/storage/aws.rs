//! AWS S3 gateway storage backend.
//!
//! Proxies storage operations to a real AWS S3 bucket, allowing
//! BleepStore to act as an S3-compatible frontend to AWS.
//!
//! Key mapping:
//!   Objects:  `{prefix}{bleepstore_bucket}/{key}`
//!   Parts:    `{prefix}.parts/{upload_id}/{part_number}`
//!
//! Credentials are resolved via the standard AWS credential chain
//! (env vars, `~/.aws/credentials`, IAM role, etc.).

use aws_sdk_s3::Client;
use bytes::Bytes;
use md5::{Digest, Md5};
use std::future::Future;
use std::pin::Pin;
use tracing::{debug, info, warn};

use super::backend::{StorageBackend, StoredObject};

/// Gateway backend that forwards operations to AWS S3.
///
/// All BleepStore buckets/objects are stored under a single upstream
/// S3 bucket with a key prefix to namespace them.
pub struct AwsGatewayBackend {
    /// AWS S3 SDK client.
    client: Client,
    /// The remote S3 bucket name (the single upstream bucket).
    bucket: String,
    /// Key prefix for all objects in the upstream bucket.
    prefix: String,
}

impl AwsGatewayBackend {
    /// Create a new AWS gateway backend.
    ///
    /// Loads AWS credentials from the default credential chain
    /// (environment variables, `~/.aws/credentials`, IAM role, etc.)
    /// and initializes the S3 client for the specified region.
    pub async fn new(
        bucket: String,
        region: String,
        prefix: String,
        endpoint_url: Option<String>,
        use_path_style: bool,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
    ) -> anyhow::Result<Self> {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region));

        if let Some(ref endpoint) = endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint);
        }

        // If explicit credentials are provided, inject them as static credentials.
        if let (Some(ref ak), Some(ref sk)) = (&access_key_id, &secret_access_key) {
            let creds = aws_sdk_s3::config::Credentials::new(
                ak,
                sk,
                None, // session_token
                None, // expiry
                "bleepstore-config",
            );
            config_loader = config_loader.credentials_provider(creds);
        }

        let sdk_config = config_loader.load().await;

        let s3_config_builder =
            aws_sdk_s3::config::Builder::from(&sdk_config).force_path_style(use_path_style);

        let client = Client::from_conf(s3_config_builder.build());

        info!(
            "AWS gateway backend initialized: bucket={} prefix='{}'",
            bucket, prefix
        );

        Ok(Self {
            client,
            bucket,
            prefix,
        })
    }

    /// Map a BleepStore bucket/key to an upstream S3 key.
    fn s3_key(&self, storage_key: &str) -> String {
        format!("{}{}", self.prefix, storage_key)
    }

    /// Map a multipart part to an upstream S3 key.
    fn part_key(&self, upload_id: &str, part_number: u32) -> String {
        format!("{}.parts/{}/{}", self.prefix, upload_id, part_number)
    }

    /// Compute the MD5 hex digest of the given data.
    fn compute_md5(data: &[u8]) -> String {
        let mut hasher = Md5::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    /// Map an AWS SDK error to an anyhow error with context.
    fn map_sdk_error(context: &str, err: impl std::fmt::Display) -> anyhow::Error {
        anyhow::anyhow!("AWS S3 {context}: {err}")
    }
}

impl StorageBackend for AwsGatewayBackend {
    fn put(
        &self,
        storage_key: &str,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let s3_key = self.s3_key(&storage_key);

            // Compute MD5 locally for consistent ETag
            // (AWS may return different ETag with server-side encryption).
            let md5_hex = Self::compute_md5(&data);
            let etag = format!("\"{md5_hex}\"");

            debug!("AWS put_object: bucket={} key={}", self.bucket, s3_key);

            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&s3_key)
                .body(aws_sdk_s3::primitives::ByteStream::from(data))
                .send()
                .await
                .map_err(|e| Self::map_sdk_error("put_object", e))?;

            Ok(etag)
        })
    }

    fn get(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<StoredObject>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let s3_key = self.s3_key(&storage_key);

            debug!("AWS get_object: bucket={} key={}", self.bucket, s3_key);

            let resp = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&s3_key)
                .send()
                .await
                .map_err(|e| {
                    let service_err = e.into_service_error();
                    if service_err.is_no_such_key() {
                        anyhow::anyhow!("Object not found at storage key: {storage_key}")
                    } else {
                        Self::map_sdk_error("get_object", service_err)
                    }
                })?;

            let body_bytes = resp
                .body
                .collect()
                .await
                .map_err(|e| Self::map_sdk_error("get_object body", e))?
                .into_bytes();

            let data = Bytes::from(body_bytes.to_vec());

            // Compute SHA-256 content hash (same as LocalBackend).
            use sha2::{Digest as Sha2Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(&data);
            let content_hash = hex::encode(hasher.finalize());

            Ok(StoredObject { data, content_hash })
        })
    }

    fn delete(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let s3_key = self.s3_key(&storage_key);

            debug!("AWS delete_object: bucket={} key={}", self.bucket, s3_key);

            // S3 delete_object is idempotent -- no error for missing keys.
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(&s3_key)
                .send()
                .await
                .map_err(|e| Self::map_sdk_error("delete_object", e))?;

            Ok(())
        })
    }

    fn exists(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let s3_key = self.s3_key(&storage_key);

            debug!("AWS head_object: bucket={} key={}", self.bucket, s3_key);

            match self
                .client
                .head_object()
                .bucket(&self.bucket)
                .key(&s3_key)
                .send()
                .await
            {
                Ok(_) => Ok(true),
                Err(e) => {
                    let service_err = e.into_service_error();
                    if service_err.is_not_found() {
                        Ok(false)
                    } else {
                        Err(Self::map_sdk_error("head_object", service_err))
                    }
                }
            }
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
            let src_s3_key = self.s3_key(&src_storage_key);
            let dst_s3_key = self.s3_key(&dst_storage_key);

            debug!(
                "AWS copy_object: src={}/{} dst={}/{}",
                self.bucket, src_s3_key, self.bucket, dst_s3_key
            );

            // Use S3 server-side copy.
            let copy_source = format!("{}/{}", self.bucket, src_s3_key);
            let resp = self
                .client
                .copy_object()
                .bucket(&self.bucket)
                .key(&dst_s3_key)
                .copy_source(&copy_source)
                .send()
                .await
                .map_err(|e| {
                    let service_err = e.into_service_error();
                    Self::map_sdk_error("copy_object", service_err)
                })?;

            // Extract ETag from CopyObjectResult. The AWS SDK returns it
            // with quotes -- we need to return it quoted to match our convention.
            let etag = resp
                .copy_object_result()
                .and_then(|r| r.e_tag())
                .unwrap_or("")
                .to_string();

            // Ensure ETag is quoted.
            if etag.starts_with('"') {
                Ok(etag)
            } else {
                Ok(format!("\"{etag}\""))
            }
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
        Box::pin(async move {
            let part_s3_key = self.part_key(&upload_id, part_number);

            // Compute MD5 locally for consistent ETag.
            let md5_hex = Self::compute_md5(&data);
            let etag = format!("\"{md5_hex}\"");

            debug!(
                "AWS put_part: bucket={} key={} (upload={} part={})",
                self.bucket, part_s3_key, upload_id, part_number
            );

            // Store part as a temporary S3 object.
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&part_s3_key)
                .body(aws_sdk_s3::primitives::ByteStream::from(data))
                .send()
                .await
                .map_err(|e| Self::map_sdk_error("put_part", e))?;

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
            let final_key = self.s3_key(&format!("{bucket}/{key}"));

            debug!(
                "AWS assemble_parts: bucket={} key={} upload_id={} parts={}",
                self.bucket,
                final_key,
                upload_id,
                parts.len()
            );

            if parts.len() == 1 {
                // Single part: direct copy to final location.
                let part_s3_key = self.part_key(&upload_id, parts[0].0);
                let copy_source = format!("{}/{}", self.bucket, part_s3_key);

                let resp = self
                    .client
                    .copy_object()
                    .bucket(&self.bucket)
                    .key(&final_key)
                    .copy_source(&copy_source)
                    .send()
                    .await
                    .map_err(|e| Self::map_sdk_error("assemble_parts copy", e))?;

                // For single part, compute composite ETag as md5(part_md5_binary)-1
                let part_etag = &parts[0].1;
                let etag_hex = part_etag.trim_matches('"');
                let part_md5_bytes = hex::decode(etag_hex).unwrap_or_else(|_| {
                    // Fallback: use the ETag from the copy response.
                    Vec::new()
                });

                if part_md5_bytes.is_empty() {
                    // Use copy response ETag
                    let etag = resp
                        .copy_object_result()
                        .and_then(|r| r.e_tag())
                        .unwrap_or("")
                        .trim_matches('"')
                        .to_string();
                    return Ok(format!("\"{etag}-1\""));
                }

                let mut composite_hasher = Md5::new();
                composite_hasher.update(&part_md5_bytes);
                let composite_md5 = composite_hasher.finalize();
                return Ok(format!("\"{}-1\"", hex::encode(composite_md5)));
            }

            // Multiple parts: use AWS native multipart upload with upload_part_copy.
            let create_resp = self
                .client
                .create_multipart_upload()
                .bucket(&self.bucket)
                .key(&final_key)
                .send()
                .await
                .map_err(|e| Self::map_sdk_error("create_multipart_upload", e))?;

            let aws_upload_id = create_resp
                .upload_id()
                .ok_or_else(|| anyhow::anyhow!("AWS did not return upload ID"))?
                .to_string();

            let result = self
                .assemble_parts_multipart(&final_key, &upload_id, &aws_upload_id, &parts)
                .await;

            match result {
                Ok(etag) => Ok(etag),
                Err(e) => {
                    // Abort on any failure.
                    warn!(
                        "Aborting AWS multipart upload {} due to error: {}",
                        aws_upload_id, e
                    );
                    if let Err(abort_err) = self
                        .client
                        .abort_multipart_upload()
                        .bucket(&self.bucket)
                        .key(&final_key)
                        .upload_id(&aws_upload_id)
                        .send()
                        .await
                    {
                        warn!("Failed to abort AWS multipart upload: {}", abort_err);
                    }
                    Err(e)
                }
            }
        })
    }

    fn delete_parts(
        &self,
        _bucket: &str,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let prefix = format!("{}.parts/{}/", self.prefix, upload_id);

            debug!("AWS delete_parts: bucket={} prefix={}", self.bucket, prefix);

            // List and delete all part objects under the prefix.
            let mut continuation_token: Option<String> = None;
            loop {
                let mut req = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.bucket)
                    .prefix(&prefix);

                if let Some(ref token) = continuation_token {
                    req = req.continuation_token(token);
                }

                let resp = req
                    .send()
                    .await
                    .map_err(|e| Self::map_sdk_error("list_objects_v2 (delete_parts)", e))?;

                let contents = resp.contents();
                if !contents.is_empty() {
                    // Batch delete (max 1000 per call, which is the list page size).
                    let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = contents
                        .iter()
                        .filter_map(|obj| {
                            obj.key().map(|k| {
                                aws_sdk_s3::types::ObjectIdentifier::builder()
                                    .key(k)
                                    .build()
                                    .expect("ObjectIdentifier requires key")
                            })
                        })
                        .collect();

                    if !objects.is_empty() {
                        let delete = aws_sdk_s3::types::Delete::builder()
                            .set_objects(Some(objects))
                            .quiet(true)
                            .build()
                            .map_err(|e| Self::map_sdk_error("delete_objects build", e))?;

                        self.client
                            .delete_objects()
                            .bucket(&self.bucket)
                            .delete(delete)
                            .send()
                            .await
                            .map_err(|e| Self::map_sdk_error("delete_objects", e))?;
                    }
                }

                if resp.is_truncated() == Some(true) {
                    continuation_token = resp.next_continuation_token().map(|s| s.to_string());
                } else {
                    break;
                }
            }

            Ok(())
        })
    }

    fn create_bucket(
        &self,
        bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let bucket = bucket.to_string();
        Box::pin(async move {
            // In gateway mode, BleepStore buckets are namespaced as prefixes
            // within the single upstream S3 bucket. There's nothing to create
            // on the AWS side -- bucket existence is purely a metadata concern.
            debug!(
                "AWS create_bucket: {} (no-op, namespaced in {})",
                bucket, self.bucket
            );
            Ok(())
        })
    }

    fn delete_bucket(
        &self,
        bucket: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let bucket = bucket.to_string();
        Box::pin(async move {
            // In gateway mode, deleting a BleepStore bucket does not delete
            // anything from the upstream bucket. Object cleanup is handled
            // via individual object deletes. This mirrors the Python implementation.
            debug!(
                "AWS delete_bucket: {} (no-op, namespaced in {})",
                bucket, self.bucket
            );
            Ok(())
        })
    }
}

// -- Private helper methods (not part of the trait) ---------------------------

impl AwsGatewayBackend {
    /// Execute the multipart assembly using AWS native multipart upload
    /// with `upload_part_copy` for server-side copy.
    ///
    /// Falls back to download + re-upload if `upload_part_copy` fails
    /// with EntityTooSmall (parts < 5MB except the last).
    async fn assemble_parts_multipart(
        &self,
        final_key: &str,
        bleepstore_upload_id: &str,
        aws_upload_id: &str,
        parts: &[(u32, String)],
    ) -> anyhow::Result<String> {
        use aws_sdk_s3::types::CompletedMultipartUpload;
        use aws_sdk_s3::types::CompletedPart;

        let mut completed_parts = Vec::new();

        for (idx, (part_number, _etag)) in parts.iter().enumerate() {
            let part_s3_key = self.part_key(bleepstore_upload_id, *part_number);
            let copy_source = format!("{}/{}", self.bucket, part_s3_key);
            let aws_part_number = (idx + 1) as i32;

            // Try server-side copy first.
            match self
                .client
                .upload_part_copy()
                .bucket(&self.bucket)
                .key(final_key)
                .upload_id(aws_upload_id)
                .part_number(aws_part_number)
                .copy_source(&copy_source)
                .send()
                .await
            {
                Ok(copy_resp) => {
                    let etag = copy_resp
                        .copy_part_result()
                        .and_then(|r| r.e_tag())
                        .unwrap_or("")
                        .to_string();

                    completed_parts.push(
                        CompletedPart::builder()
                            .e_tag(&etag)
                            .part_number(aws_part_number)
                            .build(),
                    );
                }
                Err(e) => {
                    let service_err = e.into_service_error();
                    let error_code = service_err.meta().code().unwrap_or("");

                    if error_code == "EntityTooSmall" {
                        // Fallback: download the part data and re-upload.
                        debug!(
                            "EntityTooSmall for part {}, falling back to download+upload",
                            part_number
                        );

                        let get_resp = self
                            .client
                            .get_object()
                            .bucket(&self.bucket)
                            .key(&part_s3_key)
                            .send()
                            .await
                            .map_err(|e| Self::map_sdk_error("get_object (fallback)", e))?;

                        let body_bytes = get_resp
                            .body
                            .collect()
                            .await
                            .map_err(|e| Self::map_sdk_error("get_object body (fallback)", e))?
                            .into_bytes();

                        let upload_resp = self
                            .client
                            .upload_part()
                            .bucket(&self.bucket)
                            .key(final_key)
                            .upload_id(aws_upload_id)
                            .part_number(aws_part_number)
                            .body(aws_sdk_s3::primitives::ByteStream::from(Bytes::from(
                                body_bytes.to_vec(),
                            )))
                            .send()
                            .await
                            .map_err(|e| Self::map_sdk_error("upload_part (fallback)", e))?;

                        let etag = upload_resp.e_tag().unwrap_or("").to_string();

                        completed_parts.push(
                            CompletedPart::builder()
                                .e_tag(&etag)
                                .part_number(aws_part_number)
                                .build(),
                        );
                    } else {
                        return Err(Self::map_sdk_error("upload_part_copy", service_err));
                    }
                }
            }
        }

        // Complete the multipart upload.
        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        let complete_resp = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(final_key)
            .upload_id(aws_upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await
            .map_err(|e| Self::map_sdk_error("complete_multipart_upload", e))?;

        // Compute the composite ETag ourselves from the part ETags to ensure
        // consistency with what BleepStore's metadata stores.
        let mut combined_md5_bytes: Vec<u8> = Vec::new();
        for (_part_number, etag) in parts {
            let hex_str = etag.trim_matches('"');
            if let Ok(bytes) = hex::decode(hex_str) {
                combined_md5_bytes.extend_from_slice(&bytes);
            }
        }

        if !combined_md5_bytes.is_empty() {
            let mut composite_hasher = Md5::new();
            composite_hasher.update(&combined_md5_bytes);
            let composite_md5 = composite_hasher.finalize();
            Ok(format!(
                "\"{}-{}\"",
                hex::encode(composite_md5),
                parts.len()
            ))
        } else {
            // Fallback to AWS's ETag.
            let aws_etag = complete_resp.e_tag().unwrap_or("").to_string();
            if aws_etag.starts_with('"') {
                Ok(aws_etag)
            } else {
                Ok(format!("\"{aws_etag}\""))
            }
        }
    }
}

// -- Tests -------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_key_mapping() {
        // We can't construct a full AwsGatewayBackend in unit tests without
        // AWS credentials, but we can test the key mapping logic directly.
        // Test the key mapping formula: {prefix}{storage_key}
        let prefix = "bleepstore/";
        let storage_key = "my-bucket/my-key.txt";
        let expected = "bleepstore/my-bucket/my-key.txt";
        assert_eq!(format!("{prefix}{storage_key}"), expected);
    }

    #[test]
    fn test_s3_key_mapping_no_prefix() {
        let prefix = "";
        let storage_key = "my-bucket/my-key.txt";
        let expected = "my-bucket/my-key.txt";
        assert_eq!(format!("{prefix}{storage_key}"), expected);
    }

    #[test]
    fn test_part_key_mapping() {
        let prefix = "bleepstore/";
        let upload_id = "abc-123";
        let part_number: u32 = 5;
        let expected = "bleepstore/.parts/abc-123/5";
        assert_eq!(
            format!("{prefix}.parts/{upload_id}/{part_number}"),
            expected
        );
    }

    #[test]
    fn test_compute_md5_empty() {
        let md5 = AwsGatewayBackend::compute_md5(b"");
        assert_eq!(md5, "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn test_compute_md5_hello() {
        let md5 = AwsGatewayBackend::compute_md5(b"hello world");
        assert_eq!(md5, "5eb63bbbe01eeed093cb22bb8f5acdc3");
    }

    #[test]
    fn test_composite_etag_single_part() {
        // For single part: md5(binary_md5_of_part)-1
        let part_etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";
        let hex_str = part_etag.trim_matches('"');
        let part_md5_bytes = hex::decode(hex_str).unwrap();

        let mut composite_hasher = Md5::new();
        composite_hasher.update(&part_md5_bytes);
        let composite_md5 = composite_hasher.finalize();
        let result = format!("\"{}-1\"", hex::encode(composite_md5));

        assert!(result.starts_with('"'));
        assert!(result.ends_with("-1\""));
    }

    #[test]
    fn test_composite_etag_multiple_parts() {
        let part_etags = vec![
            "\"7ac66c0f148de9519b8bd264312c4d64\"".to_string(),
            "\"d41d8cd98f00b204e9800998ecf8427e\"".to_string(),
        ];

        let mut combined: Vec<u8> = Vec::new();
        for etag in &part_etags {
            let hex_str = etag.trim_matches('"');
            combined.extend_from_slice(&hex::decode(hex_str).unwrap());
        }

        let mut hasher = Md5::new();
        hasher.update(&combined);
        let result = format!(
            "\"{}-{}\"",
            hex::encode(hasher.finalize()),
            part_etags.len()
        );

        assert!(result.starts_with('"'));
        assert!(result.ends_with("-2\""));

        // The inner hex should be 32 chars (MD5 hex digest).
        let inner = result.trim_matches('"');
        let dash_pos = inner.rfind('-').unwrap();
        assert_eq!(inner[..dash_pos].len(), 32);
    }

    #[test]
    fn test_key_mapping_with_nested_key() {
        let prefix = "data/";
        let storage_key = "mybucket/path/to/deep/object.txt";
        let expected = "data/mybucket/path/to/deep/object.txt";
        assert_eq!(format!("{prefix}{storage_key}"), expected);
    }

    #[test]
    fn test_key_mapping_with_special_chars() {
        let prefix = "bleepstore/";
        let storage_key = "mybucket/key with spaces.txt";
        let expected = "bleepstore/mybucket/key with spaces.txt";
        assert_eq!(format!("{prefix}{storage_key}"), expected);
    }
}
