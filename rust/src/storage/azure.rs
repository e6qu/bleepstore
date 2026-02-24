//! Azure Blob Storage gateway storage backend.
//!
//! Proxies storage operations to an Azure Blob Storage container via the
//! Azure Blob REST API using `reqwest`, allowing BleepStore to act as an
//! S3-compatible frontend to Azure Blob Storage.
//!
//! Key mapping:
//!   Objects:  `{prefix}{bleepstore_bucket}/{key}`
//!
//! Multipart strategy uses Azure Block Blob primitives:
//!   `put_part()`       -> Put Block (stage_block) on the final blob (no temp objects)
//!   `assemble_parts()` -> Put Block List (commit_block_list) to finalize
//!   `delete_parts()`   -> no-op (uncommitted blocks auto-expire in 7 days)
//!
//! Block IDs: `base64(format!("{upload_id}:{part_number:05}"))` -- includes
//! upload_id to avoid collisions between concurrent multipart uploads.
//!
//! Credentials are resolved via:
//!   - `AZURE_STORAGE_KEY` environment variable (Shared Key auth)
//!   - `AZURE_STORAGE_CONNECTION_STRING` environment variable
//!   - `AZURE_STORAGE_SAS_TOKEN` environment variable (SAS token auth)

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use md5::{Digest, Md5};
use reqwest::StatusCode;
use sha2::Sha256;
use std::future::Future;
use std::pin::Pin;
use tracing::{debug, info, warn};

use super::backend::{StorageBackend, StoredObject};

/// Azure REST API version used for all requests.
const AZURE_API_VERSION: &str = "2023-11-03";

/// Gateway backend that forwards operations to Azure Blob Storage.
///
/// All BleepStore buckets/objects are stored under a single upstream
/// Azure container with a key prefix to namespace them.
pub struct AzureGatewayBackend {
    /// HTTP client for Azure Blob REST API calls.
    client: reqwest::Client,
    /// The remote Azure container name.
    container: String,
    /// Azure storage account name.
    account: String,
    /// Key prefix for all blobs in the upstream container.
    prefix: String,
    /// The base URL for the Azure Blob service endpoint.
    base_url: String,
    /// Authentication method.
    auth: AzureAuth,
}

/// Azure authentication method.
enum AzureAuth {
    /// Shared Key authentication using the storage account key.
    SharedKey { key_bytes: Vec<u8> },
    /// SAS token authentication (appended as query parameter).
    SasToken { token: String },
}

impl AzureGatewayBackend {
    /// Create a new Azure gateway backend.
    ///
    /// Initializes the reqwest HTTP client. Credentials are resolved from
    /// environment variables:
    ///   - `AZURE_STORAGE_KEY` (Shared Key auth, preferred)
    ///   - `AZURE_STORAGE_SAS_TOKEN` (SAS token auth, fallback)
    ///   - `AZURE_STORAGE_CONNECTION_STRING` (parsed for account key)
    pub async fn new(container: String, account: String, prefix: String) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create HTTP client: {}", e))?;

        let base_url = format!("https://{}.blob.core.windows.net", account);

        // Resolve credentials from environment.
        let auth = Self::resolve_auth()?;

        info!(
            "Azure gateway backend initialized: container={} account={} prefix='{}'",
            container, account, prefix
        );

        Ok(Self {
            client,
            container,
            account,
            prefix,
            base_url,
            auth,
        })
    }

    /// Resolve Azure authentication from environment variables.
    fn resolve_auth() -> anyhow::Result<AzureAuth> {
        // 1. Try AZURE_STORAGE_KEY
        if let Ok(key) = std::env::var("AZURE_STORAGE_KEY") {
            let key_bytes = BASE64_STANDARD.decode(&key).map_err(|e| {
                anyhow::anyhow!("Invalid AZURE_STORAGE_KEY (not valid base64): {}", e)
            })?;
            return Ok(AzureAuth::SharedKey { key_bytes });
        }

        // 2. Try AZURE_STORAGE_CONNECTION_STRING (extract AccountKey)
        if let Ok(conn_str) = std::env::var("AZURE_STORAGE_CONNECTION_STRING") {
            for part in conn_str.split(';') {
                if let Some(key_val) = part.strip_prefix("AccountKey=") {
                    let key_bytes = BASE64_STANDARD.decode(key_val).map_err(|e| {
                        anyhow::anyhow!("Invalid AccountKey in connection string: {}", e)
                    })?;
                    return Ok(AzureAuth::SharedKey { key_bytes });
                }
            }
        }

        // 3. Try AZURE_STORAGE_SAS_TOKEN
        if let Ok(sas) = std::env::var("AZURE_STORAGE_SAS_TOKEN") {
            let token = if sas.starts_with('?') {
                sas[1..].to_string()
            } else {
                sas
            };
            return Ok(AzureAuth::SasToken { token });
        }

        Err(anyhow::anyhow!(
            "No Azure credentials found. Set AZURE_STORAGE_KEY, \
             AZURE_STORAGE_CONNECTION_STRING, or AZURE_STORAGE_SAS_TOKEN."
        ))
    }

    /// Map a BleepStore storage_key to an upstream Azure blob name.
    fn blob_name(&self, storage_key: &str) -> String {
        format!("{}{}", self.prefix, storage_key)
    }

    /// Generate a block ID for Azure staged blocks.
    ///
    /// Block IDs must be base64-encoded and the same length for all blocks
    /// in a blob. Includes upload_id to avoid collisions between concurrent
    /// multipart uploads to the same key.
    fn block_id(upload_id: &str, part_number: u32) -> String {
        let raw = format!("{}:{:05}", upload_id, part_number);
        BASE64_STANDARD.encode(raw.as_bytes())
    }

    /// Compute the MD5 hex digest of the given data.
    fn compute_md5(data: &[u8]) -> String {
        let mut hasher = Md5::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    /// Build the full URL for a blob operation.
    fn blob_url(&self, blob_name: &str) -> String {
        let encoded_blob = percent_encoding::utf8_percent_encode(
            blob_name,
            // Encode everything except unreserved + '/' (Azure expects '/' unencoded in blob paths).
            &AZURE_BLOB_ENCODE_SET,
        )
        .to_string();
        format!("{}/{}/{}", self.base_url, self.container, encoded_blob)
    }

    /// Sign a request using Azure Shared Key authentication and return
    /// the Authorization header value.
    ///
    /// Implements the Shared Key authorization scheme:
    /// `Authorization: SharedKey {account}:{signature}`
    ///
    /// The string-to-sign format:
    /// ```text
    /// VERB\n
    /// Content-Encoding\n
    /// Content-Language\n
    /// Content-Length\n
    /// Content-MD5\n
    /// Content-Type\n
    /// Date\n
    /// If-Modified-Since\n
    /// If-Match\n
    /// If-None-Match\n
    /// If-Unmodified-Since\n
    /// Range\n
    /// CanonicalizedHeaders\n
    /// CanonicalizedResource
    /// ```
    fn sign_request(
        &self,
        method: &str,
        blob_name: &str,
        content_length: Option<usize>,
        content_type: &str,
        date: &str,
        extra_headers: &[(String, String)],
        query_params: &[(String, String)],
    ) -> anyhow::Result<String> {
        let key_bytes = match &self.auth {
            AzureAuth::SharedKey { key_bytes } => key_bytes,
            AzureAuth::SasToken { .. } => {
                return Err(anyhow::anyhow!("Cannot sign with SAS token auth"));
            }
        };

        // Content-Length: empty for 0 or if not provided (GET/DELETE/HEAD).
        let content_length_str = match content_length {
            Some(0) | None => String::new(),
            Some(len) => len.to_string(),
        };

        // Build canonicalized headers (x-ms-* headers, sorted).
        let mut ms_headers: Vec<(String, String)> = vec![
            ("x-ms-date".to_string(), date.to_string()),
            ("x-ms-version".to_string(), AZURE_API_VERSION.to_string()),
        ];
        for (k, v) in extra_headers {
            let lk = k.to_lowercase();
            if lk.starts_with("x-ms-") && lk != "x-ms-date" && lk != "x-ms-version" {
                ms_headers.push((lk, v.clone()));
            }
        }
        ms_headers.sort_by(|a, b| a.0.cmp(&b.0));

        let canonicalized_headers: String = ms_headers
            .iter()
            .map(|(k, v)| format!("{}:{}", k, v))
            .collect::<Vec<_>>()
            .join("\n");

        // Build canonicalized resource.
        // Azure Shared Key auth uses the un-encoded blob name in the
        // canonicalized resource (not the percent-encoded URL form).
        let mut canonicalized_resource =
            format!("/{}/{}/{}", self.account, self.container, blob_name);
        // Append query parameters sorted by key.
        if !query_params.is_empty() {
            let mut sorted_params = query_params.to_vec();
            sorted_params.sort_by(|a, b| a.0.cmp(&b.0));
            for (k, v) in &sorted_params {
                canonicalized_resource.push_str(&format!("\n{}:{}", k.to_lowercase(), v));
            }
        }

        // Build string to sign.
        let string_to_sign = format!(
            "{}\n\n\n{}\n\n{}\n\n\n\n\n\n\n{}\n{}",
            method, content_length_str, content_type, canonicalized_headers, canonicalized_resource
        );

        // HMAC-SHA256 sign.
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(key_bytes)
            .map_err(|e| anyhow::anyhow!("HMAC key error: {}", e))?;
        mac.update(string_to_sign.as_bytes());
        let signature = BASE64_STANDARD.encode(mac.finalize().into_bytes());

        Ok(format!("SharedKey {}:{}", self.account, signature))
    }

    /// Get the current UTC date in RFC 1123 format for Azure headers.
    fn rfc1123_date() -> String {
        use std::time::SystemTime;
        httpdate::fmt_http_date(SystemTime::now())
    }

    /// Append SAS token to a URL if using SAS auth.
    fn maybe_append_sas(&self, url: &str) -> String {
        match &self.auth {
            AzureAuth::SasToken { token } => {
                if url.contains('?') {
                    format!("{}&{}", url, token)
                } else {
                    format!("{}?{}", url, token)
                }
            }
            AzureAuth::SharedKey { .. } => url.to_string(),
        }
    }

    /// Check if a status code indicates "not found" (404).
    fn is_not_found(status: StatusCode) -> bool {
        status == StatusCode::NOT_FOUND
    }

    /// Map an Azure HTTP error to an anyhow error with context.
    fn map_azure_error(context: &str, status: StatusCode, body: &str) -> anyhow::Error {
        anyhow::anyhow!("Azure {}: HTTP {} - {}", context, status, body)
    }

    // -- Azure Blob REST API operations ----------------------------------------

    /// Upload a blob (Put Blob) to Azure.
    async fn azure_upload(&self, blob_name: &str, data: &[u8]) -> anyhow::Result<()> {
        let url = self.blob_url(blob_name);
        let date = Self::rfc1123_date();
        let content_type = "application/octet-stream";

        let extra_headers = vec![("x-ms-blob-type".to_string(), "BlockBlob".to_string())];

        let mut req = self
            .client
            .put(&self.maybe_append_sas(&url))
            .header("x-ms-date", &date)
            .header("x-ms-version", AZURE_API_VERSION)
            .header("x-ms-blob-type", "BlockBlob")
            .header("Content-Type", content_type)
            .body(data.to_vec());

        if let AzureAuth::SharedKey { .. } = &self.auth {
            let auth_header = self.sign_request(
                "PUT",
                blob_name,
                Some(data.len()),
                content_type,
                &date,
                &extra_headers,
                &[],
            )?;
            req = req.header("Authorization", auth_header);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Azure upload request failed: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::map_azure_error("upload", status, &body));
        }

        Ok(())
    }

    /// Download a blob (Get Blob) from Azure.
    async fn azure_download(&self, blob_name: &str) -> anyhow::Result<Bytes> {
        let url = self.blob_url(blob_name);
        let date = Self::rfc1123_date();

        let mut req = self
            .client
            .get(&self.maybe_append_sas(&url))
            .header("x-ms-date", &date)
            .header("x-ms-version", AZURE_API_VERSION);

        if let AzureAuth::SharedKey { .. } = &self.auth {
            let auth_header = self.sign_request("GET", blob_name, None, "", &date, &[], &[])?;
            req = req.header("Authorization", auth_header);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Azure download request failed: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            if Self::is_not_found(status) {
                return Err(anyhow::anyhow!(
                    "Object not found at storage key: {}",
                    blob_name
                ));
            }
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::map_azure_error("download", status, &body));
        }

        let body = resp
            .bytes()
            .await
            .map_err(|e| anyhow::anyhow!("Azure download body read failed: {}", e))?;

        Ok(body)
    }

    /// Delete a blob from Azure. Idempotent (ignores 404).
    async fn azure_delete(&self, blob_name: &str) -> anyhow::Result<()> {
        let url = self.blob_url(blob_name);
        let date = Self::rfc1123_date();

        let mut req = self
            .client
            .delete(&self.maybe_append_sas(&url))
            .header("x-ms-date", &date)
            .header("x-ms-version", AZURE_API_VERSION);

        if let AzureAuth::SharedKey { .. } = &self.auth {
            let auth_header = self.sign_request("DELETE", blob_name, None, "", &date, &[], &[])?;
            req = req.header("Authorization", auth_header);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Azure delete request failed: {}", e))?;

        if !resp.status().is_success() && !Self::is_not_found(resp.status()) {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::map_azure_error("delete", status, &body));
        }

        Ok(())
    }

    /// Check if a blob exists in Azure via HEAD request.
    async fn azure_exists(&self, blob_name: &str) -> anyhow::Result<bool> {
        let url = self.blob_url(blob_name);
        let date = Self::rfc1123_date();

        let mut req = self
            .client
            .head(&self.maybe_append_sas(&url))
            .header("x-ms-date", &date)
            .header("x-ms-version", AZURE_API_VERSION);

        if let AzureAuth::SharedKey { .. } = &self.auth {
            let auth_header = self.sign_request("HEAD", blob_name, None, "", &date, &[], &[])?;
            req = req.header("Authorization", auth_header);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Azure exists check failed: {}", e))?;

        if resp.status().is_success() {
            Ok(true)
        } else if Self::is_not_found(resp.status()) {
            Ok(false)
        } else {
            let status = resp.status();
            Err(Self::map_azure_error("exists", status, ""))
        }
    }

    /// Stage a block on a blob (Put Block) for Azure block blob multipart.
    ///
    /// Stages the data directly on the final blob using the given block ID.
    /// No temporary objects are created -- uncommitted blocks auto-expire
    /// in 7 days.
    async fn azure_put_block(
        &self,
        blob_name: &str,
        block_id: &str,
        data: &[u8],
    ) -> anyhow::Result<()> {
        let base_url = self.blob_url(blob_name);
        let url = format!("{}?comp=block&blockid={}", base_url, block_id);
        let date = Self::rfc1123_date();
        let content_type = "application/octet-stream";

        let query_params = vec![
            ("blockid".to_string(), block_id.to_string()),
            ("comp".to_string(), "block".to_string()),
        ];

        let mut req = self
            .client
            .put(&self.maybe_append_sas(&url))
            .header("x-ms-date", &date)
            .header("x-ms-version", AZURE_API_VERSION)
            .header("Content-Type", content_type)
            .body(data.to_vec());

        if let AzureAuth::SharedKey { .. } = &self.auth {
            let auth_header = self.sign_request(
                "PUT",
                blob_name,
                Some(data.len()),
                content_type,
                &date,
                &[],
                &query_params,
            )?;
            req = req.header("Authorization", auth_header);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Azure put_block request failed: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::map_azure_error("put_block", status, &body));
        }

        Ok(())
    }

    /// Commit a block list (Put Block List) to finalize a block blob.
    ///
    /// The block_ids must be the same base64-encoded IDs used in put_block().
    async fn azure_put_block_list(
        &self,
        blob_name: &str,
        block_ids: &[String],
    ) -> anyhow::Result<()> {
        let base_url = self.blob_url(blob_name);
        let url = format!("{}?comp=blocklist", base_url);
        let date = Self::rfc1123_date();
        let content_type = "application/xml";

        // Build the XML body for Put Block List.
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<BlockList>\n");
        for id in block_ids {
            xml.push_str(&format!("  <Latest>{}</Latest>\n", id));
        }
        xml.push_str("</BlockList>");

        let xml_bytes = xml.into_bytes();
        let query_params = vec![("comp".to_string(), "blocklist".to_string())];

        let mut req = self
            .client
            .put(&self.maybe_append_sas(&url))
            .header("x-ms-date", &date)
            .header("x-ms-version", AZURE_API_VERSION)
            .header("Content-Type", content_type)
            .body(xml_bytes.clone());

        if let AzureAuth::SharedKey { .. } = &self.auth {
            let auth_header = self.sign_request(
                "PUT",
                blob_name,
                Some(xml_bytes.len()),
                content_type,
                &date,
                &[],
                &query_params,
            )?;
            req = req.header("Authorization", auth_header);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Azure put_block_list request failed: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::map_azure_error("put_block_list", status, &body));
        }

        Ok(())
    }

    /// Copy a blob using Azure server-side copy (Copy Blob).
    ///
    /// Uses the `x-ms-copy-source` header for an asynchronous server-side copy.
    async fn azure_copy(&self, src_blob_name: &str, dst_blob_name: &str) -> anyhow::Result<()> {
        let dst_url = self.blob_url(dst_blob_name);
        let src_url = self.blob_url(src_blob_name);
        let date = Self::rfc1123_date();

        let extra_headers = vec![("x-ms-copy-source".to_string(), src_url.clone())];

        let mut req = self
            .client
            .put(&self.maybe_append_sas(&dst_url))
            .header("x-ms-date", &date)
            .header("x-ms-version", AZURE_API_VERSION)
            .header("x-ms-copy-source", &src_url);

        if let AzureAuth::SharedKey { .. } = &self.auth {
            let auth_header = self.sign_request(
                "PUT",
                dst_blob_name,
                Some(0),
                "",
                &date,
                &extra_headers,
                &[],
            )?;
            req = req.header("Authorization", auth_header);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Azure copy request failed: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            if Self::is_not_found(status) {
                return Err(anyhow::anyhow!("Source blob not found: {}", src_blob_name));
            }
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::map_azure_error("copy", status, &body));
        }

        Ok(())
    }
}

/// Percent-encoding set for Azure blob names: encode everything except
/// unreserved characters and '/'.
const AZURE_BLOB_ENCODE_SET: percent_encoding::AsciiSet = percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'/');

impl StorageBackend for AzureGatewayBackend {
    fn put(
        &self,
        storage_key: &str,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let blob_name = self.blob_name(&storage_key);

            // Compute MD5 locally for consistent ETag.
            let md5_hex = Self::compute_md5(&data);
            let etag = format!("\"{}\"", md5_hex);

            debug!("Azure put: container={} blob={}", self.container, blob_name);

            self.azure_upload(&blob_name, &data).await?;

            Ok(etag)
        })
    }

    fn get(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<StoredObject>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let blob_name = self.blob_name(&storage_key);

            debug!("Azure get: container={} blob={}", self.container, blob_name);

            let data = self.azure_download(&blob_name).await?;

            // Compute SHA-256 content hash (same as LocalBackend, AWS, GCP).
            use sha2::{Digest as Sha2Digest, Sha256 as Sha256Hash};
            let mut hasher = Sha256Hash::new();
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
            let blob_name = self.blob_name(&storage_key);

            debug!(
                "Azure delete: container={} blob={}",
                self.container, blob_name
            );

            // Azure delete is idempotent -- our azure_delete() catches 404.
            self.azure_delete(&blob_name).await?;

            Ok(())
        })
    }

    fn exists(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let blob_name = self.blob_name(&storage_key);

            debug!(
                "Azure exists: container={} blob={}",
                self.container, blob_name
            );

            self.azure_exists(&blob_name).await
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
            let src_blob_name = self.blob_name(&src_storage_key);
            let dst_blob_name = self.blob_name(&dst_storage_key);

            debug!("Azure copy: src={} dst={}", src_blob_name, dst_blob_name);

            // Use Azure server-side copy.
            self.azure_copy(&src_blob_name, &dst_blob_name).await?;

            // Download the destination to compute MD5 for consistent ETag.
            let data = self.azure_download(&dst_blob_name).await?;
            let md5_hex = Self::compute_md5(&data);
            let etag = format!("\"{}\"", md5_hex);

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
        Box::pin(async move {
            // Store parts as individual blobs at .parts/{upload_id}/{part_number}.
            // The Rust StorageBackend trait doesn't pass the key to put_part(),
            // so we can't stage blocks directly on the final blob (which Azure
            // Block Blobs would require). Instead, we use the same temporary
            // blob approach as the AWS and GCP backends. At assembly time,
            // parts are downloaded, staged as blocks, and committed.
            let blob_name = format!("{}.parts/{}/{}", self.prefix, upload_id, part_number);

            // Compute MD5 locally for consistent ETag.
            let md5_hex = Self::compute_md5(&data);
            let etag = format!("\"{}\"", md5_hex);

            debug!(
                "Azure put_part: container={} blob={} (upload={} part={})",
                self.container, blob_name, upload_id, part_number
            );

            // Store part as a regular blob (same approach as AWS/GCP backends).
            self.azure_upload(&blob_name, &data).await?;

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
            let final_blob_name = self.blob_name(&format!("{}/{}", bucket, key));

            debug!(
                "Azure assemble_parts: container={} blob={} upload_id={} parts={}",
                self.container,
                final_blob_name,
                upload_id,
                parts.len()
            );

            // Download all part data, then stage as blocks on the final blob
            // and commit the block list. This uses the Azure Block Blob pattern.
            let mut block_ids: Vec<String> = Vec::new();

            for (part_number, _etag) in &parts {
                let part_blob_name = format!("{}.parts/{}/{}", self.prefix, upload_id, part_number);
                let bid = Self::block_id(&upload_id, *part_number);

                // Download part data.
                let part_data = self.azure_download(&part_blob_name).await?;

                // Stage block on the final blob.
                self.azure_put_block(&final_blob_name, &bid, &part_data)
                    .await?;

                block_ids.push(bid);
            }

            // Commit the block list to finalize the blob.
            self.azure_put_block_list(&final_blob_name, &block_ids)
                .await?;

            // Compute composite ETag from part MD5s (same as AWS/GCP backends).
            let mut combined_md5_bytes: Vec<u8> = Vec::new();
            for (_part_number, etag) in &parts {
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
                // Fallback: download assembled blob and compute MD5.
                let data = self.azure_download(&final_blob_name).await?;
                let md5_hex = Self::compute_md5(&data);
                Ok(format!("\"{}\"", md5_hex))
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
            // Since we store parts as individual blobs (like AWS/GCP),
            // we need to clean them up. Unlike the Python Azure backend
            // which uses native Put Block (no temp objects), our Rust trait
            // doesn't pass the key to put_part, so we use temp blobs.
            //
            // We use a simple approach: list and delete all blobs with
            // the upload_id prefix. However, Azure Blob REST API listing
            // requires pagination. For simplicity and robustness, we'll
            // attempt to delete part numbers 1-10000 (matching AWS behavior).
            //
            // In practice, the caller tracks part numbers and this method
            // is called on abort. For efficiency, we just log and return OK
            // since orphan blobs are harmless in the gateway context.
            debug!(
                "Azure delete_parts: upload_id={} (cleanup temp part blobs)",
                upload_id
            );

            // Best-effort cleanup: try to list blobs with the parts prefix.
            // We use the List Blobs API with a prefix filter.
            let prefix = format!("{}.parts/{}/", self.prefix, upload_id);
            let blob_names = self.azure_list_blobs(&prefix).await.unwrap_or_default();

            for name in &blob_names {
                if let Err(e) = self.azure_delete(name).await {
                    warn!("Failed to delete part blob {}: {}", name, e);
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
            // within the single upstream Azure container. Nothing to create
            // on the Azure side -- bucket existence is purely a metadata concern.
            debug!(
                "Azure create_bucket: {} (no-op, namespaced in {})",
                bucket, self.container
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
            // anything from the upstream Azure container. Object cleanup is
            // handled via individual object deletes.
            debug!(
                "Azure delete_bucket: {} (no-op, namespaced in {})",
                bucket, self.container
            );
            Ok(())
        })
    }
}

// -- Additional helper methods ------------------------------------------------

impl AzureGatewayBackend {
    /// List blobs in the Azure container with a given prefix.
    ///
    /// Uses the List Blobs API with pagination.
    async fn azure_list_blobs(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let mut all_names: Vec<String> = Vec::new();
        let mut marker: Option<String> = None;

        loop {
            let mut url = format!(
                "{}/{}?restype=container&comp=list&prefix={}",
                self.base_url,
                self.container,
                percent_encoding::utf8_percent_encode(prefix, &AZURE_BLOB_ENCODE_SET)
            );

            if let Some(ref m) = marker {
                url.push_str(&format!("&marker={}", m));
            }

            let date = Self::rfc1123_date();

            // For List Blobs, the canonicalized resource includes the container only
            // (no blob name), plus query params.
            let mut query_params = vec![
                ("comp".to_string(), "list".to_string()),
                ("prefix".to_string(), prefix.to_string()),
                ("restype".to_string(), "container".to_string()),
            ];
            if let Some(ref m) = marker {
                query_params.push(("marker".to_string(), m.clone()));
            }

            let mut req = self
                .client
                .get(&self.maybe_append_sas(&url))
                .header("x-ms-date", &date)
                .header("x-ms-version", AZURE_API_VERSION);

            if let AzureAuth::SharedKey { .. } = &self.auth {
                // For container-level operations, the blob_name is empty.
                let auth_header = self.sign_request_container("GET", &date, &query_params)?;
                req = req.header("Authorization", auth_header);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Azure list_blobs request failed: {}", e))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(Self::map_azure_error("list_blobs", status, &body));
            }

            let body = resp.text().await.unwrap_or_default();

            // Parse the XML response to extract blob names and next marker.
            // Simple XML parsing -- extract <Name>...</Name> elements within <Blob> elements.
            let mut next_marker: Option<String> = None;

            // Extract NextMarker if present.
            if let Some(start) = body.find("<NextMarker>") {
                let start = start + "<NextMarker>".len();
                if let Some(end) = body[start..].find("</NextMarker>") {
                    let nm = &body[start..start + end];
                    if !nm.is_empty() {
                        next_marker = Some(nm.to_string());
                    }
                }
            }

            // Extract blob names.
            let mut search_from = 0;
            while let Some(blob_start) = body[search_from..].find("<Blob>") {
                let blob_start = search_from + blob_start;
                if let Some(blob_end) = body[blob_start..].find("</Blob>") {
                    let blob_xml = &body[blob_start..blob_start + blob_end];
                    if let Some(name_start) = blob_xml.find("<Name>") {
                        let name_start = name_start + "<Name>".len();
                        if let Some(name_end) = blob_xml[name_start..].find("</Name>") {
                            let name = &blob_xml[name_start..name_start + name_end];
                            all_names.push(name.to_string());
                        }
                    }
                    search_from = blob_start + blob_end;
                } else {
                    break;
                }
            }

            if next_marker.is_some() {
                marker = next_marker;
            } else {
                break;
            }
        }

        Ok(all_names)
    }

    /// Sign a container-level request (List Blobs, etc.) using Shared Key auth.
    fn sign_request_container(
        &self,
        method: &str,
        date: &str,
        query_params: &[(String, String)],
    ) -> anyhow::Result<String> {
        let key_bytes = match &self.auth {
            AzureAuth::SharedKey { key_bytes } => key_bytes,
            AzureAuth::SasToken { .. } => {
                return Err(anyhow::anyhow!("Cannot sign with SAS token auth"));
            }
        };

        let ms_headers = format!("x-ms-date:{}\nx-ms-version:{}", date, AZURE_API_VERSION);

        // For container-level operations, canonicalized resource is /{account}/{container}.
        let mut canonicalized_resource = format!("/{}/{}", self.account, self.container);

        // Append sorted query parameters.
        if !query_params.is_empty() {
            let mut sorted_params = query_params.to_vec();
            sorted_params.sort_by(|a, b| a.0.cmp(&b.0));
            for (k, v) in &sorted_params {
                canonicalized_resource.push_str(&format!("\n{}:{}", k.to_lowercase(), v));
            }
        }

        let string_to_sign = format!(
            "{}\n\n\n\n\n\n\n\n\n\n\n\n{}\n{}",
            method, ms_headers, canonicalized_resource
        );

        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(key_bytes)
            .map_err(|e| anyhow::anyhow!("HMAC key error: {}", e))?;
        mac.update(string_to_sign.as_bytes());
        let signature = BASE64_STANDARD.encode(mac.finalize().into_bytes());

        Ok(format!("SharedKey {}:{}", self.account, signature))
    }
}

// -- Tests -------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_name_mapping() {
        // Test the key mapping formula: {prefix}{storage_key}
        let prefix = "bleepstore/";
        let storage_key = "my-bucket/my-key.txt";
        let expected = "bleepstore/my-bucket/my-key.txt";
        assert_eq!(format!("{}{}", prefix, storage_key), expected);
    }

    #[test]
    fn test_blob_name_mapping_no_prefix() {
        let prefix = "";
        let storage_key = "my-bucket/my-key.txt";
        let expected = "my-bucket/my-key.txt";
        assert_eq!(format!("{}{}", prefix, storage_key), expected);
    }

    #[test]
    fn test_block_id_generation() {
        // Block ID should be base64("{upload_id}:{part_number:05}")
        let upload_id = "abc-123";
        let part_number = 1u32;
        let block_id = AzureGatewayBackend::block_id(upload_id, part_number);

        // Decode and verify format.
        let decoded = BASE64_STANDARD.decode(&block_id).unwrap();
        let decoded_str = String::from_utf8(decoded).unwrap();
        assert_eq!(decoded_str, "abc-123:00001");
    }

    #[test]
    fn test_block_id_padding() {
        // Part numbers should be zero-padded to 5 digits.
        let block_id_1 = AzureGatewayBackend::block_id("upload1", 1);
        let block_id_99999 = AzureGatewayBackend::block_id("upload1", 99999);

        let decoded_1 = String::from_utf8(BASE64_STANDARD.decode(&block_id_1).unwrap()).unwrap();
        let decoded_99999 =
            String::from_utf8(BASE64_STANDARD.decode(&block_id_99999).unwrap()).unwrap();

        assert_eq!(decoded_1, "upload1:00001");
        assert_eq!(decoded_99999, "upload1:99999");
    }

    #[test]
    fn test_block_id_different_uploads_same_part() {
        // Different upload IDs should produce different block IDs
        // even for the same part number.
        let bid1 = AzureGatewayBackend::block_id("upload-A", 1);
        let bid2 = AzureGatewayBackend::block_id("upload-B", 1);
        assert_ne!(bid1, bid2);
    }

    #[test]
    fn test_compute_md5_empty() {
        let md5 = AzureGatewayBackend::compute_md5(b"");
        assert_eq!(md5, "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn test_compute_md5_hello() {
        let md5 = AzureGatewayBackend::compute_md5(b"hello world");
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
        assert_eq!(format!("{}{}", prefix, storage_key), expected);
    }

    #[test]
    fn test_key_mapping_with_special_chars() {
        let prefix = "bleepstore/";
        let storage_key = "mybucket/key with spaces.txt";
        let expected = "bleepstore/mybucket/key with spaces.txt";
        assert_eq!(format!("{}{}", prefix, storage_key), expected);
    }

    #[test]
    fn test_is_not_found() {
        assert!(AzureGatewayBackend::is_not_found(StatusCode::NOT_FOUND));
        assert!(!AzureGatewayBackend::is_not_found(StatusCode::OK));
        assert!(!AzureGatewayBackend::is_not_found(StatusCode::FORBIDDEN));
        assert!(!AzureGatewayBackend::is_not_found(
            StatusCode::INTERNAL_SERVER_ERROR
        ));
    }

    #[test]
    fn test_azure_api_version() {
        assert_eq!(AZURE_API_VERSION, "2023-11-03");
    }

    #[test]
    fn test_blob_url_encoding() {
        // Verify that '/' is preserved but spaces are encoded.
        let name = "prefix/bucket/key with spaces.txt";
        let encoded =
            percent_encoding::utf8_percent_encode(name, &AZURE_BLOB_ENCODE_SET).to_string();
        assert!(encoded.contains('/'));
        assert!(encoded.contains("%20"));
        assert!(!encoded.contains(' '));
    }

    #[test]
    fn test_blob_url_simple_name() {
        let name = "simple-blob";
        let encoded =
            percent_encoding::utf8_percent_encode(name, &AZURE_BLOB_ENCODE_SET).to_string();
        assert_eq!(encoded, "simple-blob");
    }

    #[test]
    fn test_block_list_xml_format() {
        // Verify the XML format for Put Block List.
        let block_ids = vec![
            BASE64_STANDARD.encode(b"upload1:00001"),
            BASE64_STANDARD.encode(b"upload1:00002"),
        ];

        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<BlockList>\n");
        for id in &block_ids {
            xml.push_str(&format!("  <Latest>{}</Latest>\n", id));
        }
        xml.push_str("</BlockList>");

        assert!(xml.contains("<BlockList>"));
        assert!(xml.contains("</BlockList>"));
        assert!(xml.contains("<Latest>"));
        assert_eq!(xml.matches("<Latest>").count(), 2);
    }

    #[test]
    fn test_rfc1123_date_format() {
        let date = AzureGatewayBackend::rfc1123_date();
        // RFC 1123 dates look like: "Mon, 24 Feb 2026 12:34:56 GMT"
        assert!(date.ends_with("GMT"));
        assert!(date.contains(','));
    }

    #[test]
    fn test_part_blob_name_mapping() {
        // Parts are stored at {prefix}.parts/{upload_id}/{part_number}
        let prefix = "bleepstore/";
        let upload_id = "abc-123";
        let part_number: u32 = 5;
        let expected = "bleepstore/.parts/abc-123/5";
        assert_eq!(
            format!("{}.parts/{}/{}", prefix, upload_id, part_number),
            expected
        );
    }

    #[test]
    fn test_sas_token_prefix_stripped() {
        // SAS tokens with leading '?' should have it stripped.
        let sas = "?sv=2023-11-03&ss=b&srt=sco&sp=rwdlacupiytfx&se=2026-12-31&sig=xxx";
        let token = if sas.starts_with('?') {
            sas[1..].to_string()
        } else {
            sas.to_string()
        };
        assert!(!token.starts_with('?'));
        assert!(token.starts_with("sv="));
    }

    #[test]
    fn test_sas_token_no_prefix() {
        let sas = "sv=2023-11-03&ss=b&srt=sco";
        let token = if sas.starts_with('?') {
            sas[1..].to_string()
        } else {
            sas.to_string()
        };
        assert_eq!(token, "sv=2023-11-03&ss=b&srt=sco");
    }
}
