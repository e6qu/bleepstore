//! GCP Cloud Storage gateway storage backend.
//!
//! Proxies storage operations to a Google Cloud Storage bucket via the
//! GCS JSON API using `reqwest`, allowing BleepStore to act as an
//! S3-compatible frontend to GCS.
//!
//! Key mapping:
//!   Objects:  `{prefix}{bleepstore_bucket}/{key}`
//!   Parts:    `{prefix}.parts/{upload_id}/{part_number}`
//!
//! Credentials are resolved via Application Default Credentials (ADC):
//!   - `GOOGLE_APPLICATION_CREDENTIALS` environment variable (service account JSON)
//!   - gcloud CLI auth (`gcloud auth application-default login`)
//!   - GCE metadata server (when running on Google Cloud)

use bytes::Bytes;
use md5::{Digest, Md5};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use tracing::{debug, info, warn};

use super::backend::{StorageBackend, StoredObject};

/// GCS compose() supports at most 32 source objects per call.
const MAX_COMPOSE_SOURCES: usize = 32;

/// GCS JSON API base URL.
const GCS_API_BASE: &str = "https://storage.googleapis.com";

/// GCS upload base URL (for media uploads).
const GCS_UPLOAD_BASE: &str = "https://storage.googleapis.com/upload/storage/v1";

// -- GCS JSON API response types -----------------------------------------------

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct GcsObject {
    name: Option<String>,
    #[serde(rename = "md5Hash")]
    md5_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GcsListResponse {
    items: Option<Vec<GcsObject>>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Debug, Serialize)]
struct ComposeSourceObject {
    name: String,
}

#[derive(Debug, Serialize)]
struct ComposeDestination {
    #[serde(rename = "contentType")]
    content_type: String,
}

#[derive(Debug, Serialize)]
struct ComposeRequest {
    #[serde(rename = "sourceObjects")]
    source_objects: Vec<ComposeSourceObject>,
    destination: ComposeDestination,
}

#[derive(Debug, Deserialize)]
struct GcsErrorDetail {
    code: Option<u16>,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GcsErrorResponse {
    error: Option<GcsErrorDetail>,
}

// -- Token management ---------------------------------------------------------

/// Cached access token with expiry.
struct CachedToken {
    access_token: String,
    expiry: std::time::Instant,
}

/// Gateway backend that forwards operations to GCP Cloud Storage.
///
/// All BleepStore buckets/objects are stored under a single upstream
/// GCS bucket with a key prefix to namespace them.
pub struct GcpGatewayBackend {
    /// HTTP client for GCS JSON API calls.
    client: reqwest::Client,
    /// The remote GCS bucket name (the single upstream bucket).
    bucket: String,
    /// GCP project ID (used for bucket operations if needed).
    #[allow(dead_code)]
    project: String,
    /// Key prefix for all objects in the upstream bucket.
    prefix: String,
    /// Cached OAuth2 access token.
    token_cache: Mutex<Option<CachedToken>>,
}

impl GcpGatewayBackend {
    /// Create a new GCP gateway backend.
    ///
    /// Initializes the reqwest HTTP client. Credentials are resolved
    /// lazily on first API call via Application Default Credentials.
    pub async fn new(
        bucket: String,
        project: String,
        prefix: String,
        credentials_file: Option<String>,
    ) -> anyhow::Result<Self> {
        // If an explicit credentials file is provided, set the env var so that
        // Application Default Credentials (ADC) picks it up when resolving tokens.
        if let Some(ref creds_path) = credentials_file {
            info!("Setting GOOGLE_APPLICATION_CREDENTIALS={}", creds_path);
            std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", creds_path);
        }

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create HTTP client: {e}"))?;

        info!(
            "GCP gateway backend initialized: bucket={} project={} prefix='{}'",
            bucket, project, prefix
        );

        Ok(Self {
            client,
            bucket,
            project,
            prefix,
            token_cache: Mutex::new(None),
        })
    }

    /// Map a BleepStore storage_key to an upstream GCS object name.
    fn gcs_name(&self, storage_key: &str) -> String {
        format!("{}{}", self.prefix, storage_key)
    }

    /// Map a multipart part to an upstream GCS object name.
    fn part_name(&self, upload_id: &str, part_number: u32) -> String {
        format!("{}.parts/{}/{}", self.prefix, upload_id, part_number)
    }

    /// Compute the MD5 hex digest of the given data.
    fn compute_md5(data: &[u8]) -> String {
        let mut hasher = Md5::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    /// Get an OAuth2 access token via Application Default Credentials.
    ///
    /// Attempts the following credential sources in order:
    /// 1. `GOOGLE_APPLICATION_CREDENTIALS` (service account JSON key file)
    /// 2. `gcloud auth application-default` user credentials
    /// 3. GCE metadata server (when running on Google Cloud)
    ///
    /// Returns a cached token if it hasn't expired (with 60s safety margin).
    async fn get_access_token(&self) -> anyhow::Result<String> {
        // Check cache first.
        {
            let cache = self.token_cache.lock().expect("token cache mutex poisoned");
            if let Some(ref cached) = *cache {
                if cached.expiry > std::time::Instant::now() {
                    return Ok(cached.access_token.clone());
                }
            }
        }

        // Try to get a fresh token.
        let (token, expires_in) = self.fetch_access_token().await?;

        // Cache with 60s safety margin.
        let expiry = std::time::Instant::now()
            + std::time::Duration::from_secs(expires_in.saturating_sub(60));

        {
            let mut cache = self.token_cache.lock().expect("token cache mutex poisoned");
            *cache = Some(CachedToken {
                access_token: token.clone(),
                expiry,
            });
        }

        Ok(token)
    }

    /// Fetch a fresh access token from the credential source.
    async fn fetch_access_token(&self) -> anyhow::Result<(String, u64)> {
        // 1. Try GOOGLE_APPLICATION_CREDENTIALS (service account JSON)
        if let Ok(creds_path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            return self.token_from_service_account(&creds_path).await;
        }

        // 2. Try gcloud application-default credentials
        let adc_path = Self::application_default_credentials_path();
        if let Ok(true) = tokio::fs::try_exists(&adc_path).await {
            return self.token_from_adc_file(&adc_path).await;
        }

        // 3. Try GCE metadata server
        self.token_from_metadata_server().await
    }

    /// Get the path to gcloud application-default credentials.
    fn application_default_credentials_path() -> String {
        if let Ok(config_dir) = std::env::var("CLOUDSDK_CONFIG") {
            return format!("{config_dir}/application_default_credentials.json");
        }
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/.config/gcloud/application_default_credentials.json");
        }
        // Fallback (unlikely to work but try anyway)
        ".config/gcloud/application_default_credentials.json".to_string()
    }

    /// Obtain an access token from a service account JSON key file.
    async fn token_from_service_account(&self, creds_path: &str) -> anyhow::Result<(String, u64)> {
        let contents = tokio::fs::read_to_string(creds_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read service account key {creds_path}: {e}"))?;

        let creds: serde_json::Value = serde_json::from_str(&contents)
            .map_err(|e| anyhow::anyhow!("Failed to parse service account key: {e}"))?;

        let cred_type = creds.get("type").and_then(|v| v.as_str()).unwrap_or("");

        if cred_type == "service_account" {
            // Service account: need JWT-based token exchange
            let client_email = creds
                .get("client_email")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing client_email in service account key"))?;
            let private_key = creds
                .get("private_key")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing private_key in service account key"))?;
            let token_uri = creds
                .get("token_uri")
                .and_then(|v| v.as_str())
                .unwrap_or("https://oauth2.googleapis.com/token");

            self.exchange_jwt_for_token(client_email, private_key, token_uri)
                .await
        } else if cred_type == "authorized_user" {
            // User credentials from gcloud auth: use refresh token
            self.token_from_refresh(
                creds
                    .get("client_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                creds
                    .get("client_secret")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                creds
                    .get("refresh_token")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
            )
            .await
        } else {
            Err(anyhow::anyhow!(
                "Unsupported credential type in {creds_path}: {cred_type}"
            ))
        }
    }

    /// Obtain an access token from application default credentials file.
    async fn token_from_adc_file(&self, adc_path: &str) -> anyhow::Result<(String, u64)> {
        let contents = tokio::fs::read_to_string(adc_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read ADC file {adc_path}: {e}"))?;

        let creds: serde_json::Value = serde_json::from_str(&contents)
            .map_err(|e| anyhow::anyhow!("Failed to parse ADC file: {e}"))?;

        let cred_type = creds.get("type").and_then(|v| v.as_str()).unwrap_or("");

        if cred_type == "authorized_user" {
            self.token_from_refresh(
                creds
                    .get("client_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                creds
                    .get("client_secret")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                creds
                    .get("refresh_token")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
            )
            .await
        } else if cred_type == "service_account" {
            self.token_from_service_account(adc_path).await
        } else {
            Err(anyhow::anyhow!(
                "Unsupported credential type in ADC file: {cred_type}"
            ))
        }
    }

    /// Exchange a refresh token for an access token.
    async fn token_from_refresh(
        &self,
        client_id: &str,
        client_secret: &str,
        refresh_token: &str,
    ) -> anyhow::Result<(String, u64)> {
        let resp = self
            .client
            .post("https://oauth2.googleapis.com/token")
            .form(&[
                ("client_id", client_id),
                ("client_secret", client_secret),
                ("refresh_token", refresh_token),
                ("grant_type", "refresh_token"),
            ])
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Token refresh request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("Token refresh failed ({status}): {body}"));
        }

        let token_resp: serde_json::Value = resp.json().await?;
        let access_token = token_resp
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("No access_token in token response"))?
            .to_string();
        let expires_in = token_resp
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(3600);

        Ok((access_token, expires_in))
    }

    /// Exchange a JWT assertion for an access token (service account flow).
    async fn exchange_jwt_for_token(
        &self,
        _client_email: &str,
        _private_key_pem: &str,
        _token_uri: &str,
    ) -> anyhow::Result<(String, u64)> {
        // Sign with RSA-SHA256. Since we don't have an RSA crate in deps,
        // fall back to using the gcloud metadata server or environment token
        // if direct JWT signing is not available.
        // For service accounts, try the GOOGLE_OAUTH_ACCESS_TOKEN env var first.
        if let Ok(token) = std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN") {
            return Ok((token, 3600));
        }

        // If we can't sign the JWT directly (no RSA crate), attempt metadata server
        // as a fallback. In production, users would typically have the gcloud SDK
        // configured or run on GCE.
        warn!(
            "JWT signing for service account not supported without RSA crate. \
             Falling back to metadata server or GOOGLE_OAUTH_ACCESS_TOKEN env var."
        );
        self.token_from_metadata_server().await
    }

    /// Obtain an access token from the GCE metadata server.
    async fn token_from_metadata_server(&self) -> anyhow::Result<(String, u64)> {
        // Also check GOOGLE_OAUTH_ACCESS_TOKEN env var.
        if let Ok(token) = std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN") {
            return Ok((token, 3600));
        }

        let resp = self
            .client
            .get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token")
            .header("Metadata-Flavor", "Google")
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Metadata server request failed: {e}. \
                Set GOOGLE_APPLICATION_CREDENTIALS, run 'gcloud auth application-default login', \
                or set GOOGLE_OAUTH_ACCESS_TOKEN env var."))?;

        if !resp.status().is_success() {
            return Err(anyhow::anyhow!(
                "Metadata server returned {}: set GOOGLE_APPLICATION_CREDENTIALS or \
                 GOOGLE_OAUTH_ACCESS_TOKEN",
                resp.status()
            ));
        }

        let token_resp: serde_json::Value = resp.json().await?;
        let access_token = token_resp
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("No access_token in metadata response"))?
            .to_string();
        let expires_in = token_resp
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(3600);

        Ok((access_token, expires_in))
    }

    /// Build authorization headers for GCS API calls.
    async fn auth_headers(&self) -> anyhow::Result<HeaderMap> {
        let token = self.get_access_token().await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|e| anyhow::anyhow!("Invalid auth header value: {e}"))?,
        );
        Ok(headers)
    }

    /// URL-encode a GCS object name for use in API paths.
    fn url_encode_object_name(name: &str) -> String {
        percent_encoding::utf8_percent_encode(name, percent_encoding::NON_ALPHANUMERIC).to_string()
    }

    /// Map a GCS HTTP error to an anyhow error with context.
    fn map_gcs_error(context: &str, status: StatusCode, body: &str) -> anyhow::Error {
        // Try to parse as GCS error JSON
        if let Ok(err_resp) = serde_json::from_str::<GcsErrorResponse>(body) {
            if let Some(err) = err_resp.error {
                return anyhow::anyhow!(
                    "GCS {}: {} (code {})",
                    context,
                    err.message.unwrap_or_default(),
                    err.code.unwrap_or(status.as_u16())
                );
            }
        }
        anyhow::anyhow!("GCS {context}: HTTP {status} - {body}")
    }

    /// Check if a GCS error response indicates "not found" (404).
    fn is_not_found(status: StatusCode) -> bool {
        status == StatusCode::NOT_FOUND
    }

    // -- GCS API operations ---------------------------------------------------

    /// Upload an object to GCS via media upload.
    async fn gcs_upload(&self, object_name: &str, data: &[u8]) -> anyhow::Result<()> {
        let auth = self.auth_headers().await?;
        let encoded_name = Self::url_encode_object_name(object_name);

        let url = format!(
            "{}/b/{}/o?uploadType=media&name={}",
            GCS_UPLOAD_BASE,
            Self::url_encode_object_name(&self.bucket),
            encoded_name
        );

        let resp = self
            .client
            .post(&url)
            .headers(auth)
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("GCS upload request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::map_gcs_error("upload", status, &body));
        }

        Ok(())
    }

    /// Download an object from GCS.
    async fn gcs_download(&self, object_name: &str) -> anyhow::Result<Bytes> {
        let auth = self.auth_headers().await?;
        let encoded_name = Self::url_encode_object_name(object_name);

        let url = format!(
            "{}/storage/v1/b/{}/o/{}?alt=media",
            GCS_API_BASE,
            Self::url_encode_object_name(&self.bucket),
            encoded_name
        );

        let resp = self
            .client
            .get(&url)
            .headers(auth)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("GCS download request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            if Self::is_not_found(status) {
                return Err(anyhow::anyhow!(
                    "Object not found at storage key: {object_name}"
                ));
            }
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::map_gcs_error("download", status, &body));
        }

        let body = resp
            .bytes()
            .await
            .map_err(|e| anyhow::anyhow!("GCS download body read failed: {e}"))?;

        Ok(body)
    }

    /// Delete an object from GCS. Idempotent (ignores 404).
    async fn gcs_delete(&self, object_name: &str) -> anyhow::Result<()> {
        let auth = self.auth_headers().await?;
        let encoded_name = Self::url_encode_object_name(object_name);

        let url = format!(
            "{}/storage/v1/b/{}/o/{}",
            GCS_API_BASE,
            Self::url_encode_object_name(&self.bucket),
            encoded_name
        );

        let resp = self
            .client
            .delete(&url)
            .headers(auth)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("GCS delete request failed: {e}"))?;

        if !resp.status().is_success() && !Self::is_not_found(resp.status()) {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::map_gcs_error("delete", status, &body));
        }

        Ok(())
    }

    /// Check if an object exists in GCS via HEAD-like metadata request.
    async fn gcs_exists(&self, object_name: &str) -> anyhow::Result<bool> {
        let auth = self.auth_headers().await?;
        let encoded_name = Self::url_encode_object_name(object_name);

        let url = format!(
            "{}/storage/v1/b/{}/o/{}",
            GCS_API_BASE,
            Self::url_encode_object_name(&self.bucket),
            encoded_name
        );

        let resp = self
            .client
            .get(&url)
            .headers(auth)
            .query(&[("fields", "name")])
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("GCS exists check failed: {e}"))?;

        if resp.status().is_success() {
            Ok(true)
        } else if Self::is_not_found(resp.status()) {
            Ok(false)
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            Err(Self::map_gcs_error("exists", status, &body))
        }
    }

    /// Copy an object within GCS using the rewrite API.
    async fn gcs_copy(&self, src_object: &str, dst_object: &str) -> anyhow::Result<()> {
        let auth = self.auth_headers().await?;
        let encoded_src = Self::url_encode_object_name(src_object);
        let encoded_dst = Self::url_encode_object_name(dst_object);
        let encoded_bucket = Self::url_encode_object_name(&self.bucket);

        // GCS rewrite API handles large objects transparently.
        let url = format!(
            "{GCS_API_BASE}/storage/v1/b/{encoded_bucket}/o/{encoded_src}/rewriteTo/b/{encoded_bucket}/o/{encoded_dst}"
        );

        // Rewrite may require multiple calls for large objects.
        let mut rewrite_token: Option<String> = None;
        loop {
            let mut req = self.client.post(&url).headers(auth.clone());

            if let Some(ref token) = rewrite_token {
                req = req.query(&[("rewriteToken", token.as_str())]);
            }

            // Empty JSON body required for rewrite.
            req = req.header(CONTENT_TYPE, "application/json").body("{}");

            let resp = req
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("GCS rewrite request failed: {e}"))?;

            if !resp.status().is_success() {
                let status = resp.status();
                if Self::is_not_found(status) {
                    return Err(anyhow::anyhow!("Source object not found: {src_object}"));
                }
                let body = resp.text().await.unwrap_or_default();
                return Err(Self::map_gcs_error("rewrite", status, &body));
            }

            let body: serde_json::Value = resp.json().await?;
            let done = body.get("done").and_then(|v| v.as_bool()).unwrap_or(false);

            if done {
                break;
            }

            rewrite_token = body
                .get("rewriteToken")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if rewrite_token.is_none() {
                break; // No token and not done -- treat as complete
            }
        }

        Ok(())
    }

    /// Compose multiple GCS objects into a single destination object.
    async fn gcs_compose(
        &self,
        source_names: &[String],
        destination_name: &str,
    ) -> anyhow::Result<()> {
        let auth = self.auth_headers().await?;
        let encoded_dst = Self::url_encode_object_name(destination_name);
        let encoded_bucket = Self::url_encode_object_name(&self.bucket);

        let url = format!("{GCS_API_BASE}/storage/v1/b/{encoded_bucket}/o/{encoded_dst}/compose");

        let compose_req = ComposeRequest {
            source_objects: source_names
                .iter()
                .map(|name| ComposeSourceObject { name: name.clone() })
                .collect(),
            destination: ComposeDestination {
                content_type: "application/octet-stream".to_string(),
            },
        };

        let resp = self
            .client
            .post(&url)
            .headers(auth)
            .json(&compose_req)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("GCS compose request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::map_gcs_error("compose", status, &body));
        }

        Ok(())
    }

    /// List objects in GCS with a given prefix.
    async fn gcs_list_objects(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let auth = self.auth_headers().await?;
        let encoded_bucket = Self::url_encode_object_name(&self.bucket);
        let mut all_names: Vec<String> = Vec::new();
        let mut page_token: Option<String> = None;

        loop {
            let url = format!("{GCS_API_BASE}/storage/v1/b/{encoded_bucket}/o");

            let mut req = self
                .client
                .get(&url)
                .headers(auth.clone())
                .query(&[("prefix", prefix), ("fields", "items(name),nextPageToken")]);

            if let Some(ref token) = page_token {
                req = req.query(&[("pageToken", token.as_str())]);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("GCS list request failed: {e}"))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(Self::map_gcs_error("list", status, &body));
            }

            let list_resp: GcsListResponse = resp.json().await?;

            if let Some(items) = list_resp.items {
                for item in items {
                    if let Some(name) = item.name {
                        all_names.push(name);
                    }
                }
            }

            if let Some(token) = list_resp.next_page_token {
                page_token = Some(token);
            } else {
                break;
            }
        }

        Ok(all_names)
    }

    /// Chain GCS compose calls for >32 sources.
    ///
    /// Composes in batches of MAX_COMPOSE_SOURCES, creating intermediate
    /// objects, then composes the intermediates, repeating until a single
    /// object remains at `final_name`.
    ///
    /// Returns a list of intermediate object names that should be cleaned up.
    async fn chain_compose(
        &self,
        source_names: &[String],
        final_name: &str,
    ) -> anyhow::Result<Vec<String>> {
        let mut all_intermediates: Vec<String> = Vec::new();
        let mut current_sources: Vec<String> = source_names.to_vec();
        let mut generation: u32 = 0;

        while current_sources.len() > MAX_COMPOSE_SOURCES {
            let mut next_sources: Vec<String> = Vec::new();

            for (batch_idx, chunk) in current_sources.chunks(MAX_COMPOSE_SOURCES).enumerate() {
                if chunk.len() == 1 {
                    // Single source -- no compose needed, pass through.
                    next_sources.push(chunk[0].clone());
                    continue;
                }

                let intermediate_name = format!(
                    "{}.__compose_tmp_{}_{}",
                    final_name,
                    generation,
                    batch_idx * MAX_COMPOSE_SOURCES
                );

                self.gcs_compose(chunk, &intermediate_name).await?;

                next_sources.push(intermediate_name.clone());
                all_intermediates.push(intermediate_name);
            }

            current_sources = next_sources;
            generation += 1;
        }

        // Final compose.
        self.gcs_compose(&current_sources, final_name).await?;

        Ok(all_intermediates)
    }
}

impl StorageBackend for GcpGatewayBackend {
    fn put(
        &self,
        storage_key: &str,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let gcs_name = self.gcs_name(&storage_key);

            // Compute MD5 locally for consistent ETag.
            let md5_hex = Self::compute_md5(&data);
            let etag = format!("\"{md5_hex}\"");

            debug!("GCS put: bucket={} name={}", self.bucket, gcs_name);

            self.gcs_upload(&gcs_name, &data).await?;

            Ok(etag)
        })
    }

    fn get(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<StoredObject>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let gcs_name = self.gcs_name(&storage_key);

            debug!("GCS get: bucket={} name={}", self.bucket, gcs_name);

            let data = self.gcs_download(&gcs_name).await?;

            // Compute SHA-256 content hash (same as LocalBackend and AWS).
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
            let gcs_name = self.gcs_name(&storage_key);

            debug!("GCS delete: bucket={} name={}", self.bucket, gcs_name);

            // GCS delete is NOT idempotent (returns 404 for missing objects),
            // but our gcs_delete() catches 404 silently.
            self.gcs_delete(&gcs_name).await?;

            Ok(())
        })
    }

    fn exists(
        &self,
        storage_key: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>> {
        let storage_key = storage_key.to_string();
        Box::pin(async move {
            let gcs_name = self.gcs_name(&storage_key);

            debug!("GCS exists: bucket={} name={}", self.bucket, gcs_name);

            self.gcs_exists(&gcs_name).await
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
            let src_gcs_name = self.gcs_name(&src_storage_key);
            let dst_gcs_name = self.gcs_name(&dst_storage_key);

            debug!("GCS copy: src={} dst={}", src_gcs_name, dst_gcs_name);

            // Use GCS server-side copy (rewrite API).
            self.gcs_copy(&src_gcs_name, &dst_gcs_name).await?;

            // Download the result to compute MD5 for consistent ETag.
            let data = self.gcs_download(&dst_gcs_name).await?;
            let md5_hex = Self::compute_md5(&data);
            let etag = format!("\"{md5_hex}\"");

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
            let part_gcs_name = self.part_name(&upload_id, part_number);

            // Compute MD5 locally for consistent ETag.
            let md5_hex = Self::compute_md5(&data);
            let etag = format!("\"{md5_hex}\"");

            debug!(
                "GCS put_part: bucket={} name={} (upload={} part={})",
                self.bucket, part_gcs_name, upload_id, part_number
            );

            // Store part as a temporary GCS object.
            self.gcs_upload(&part_gcs_name, &data).await?;

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
            let final_name = self.gcs_name(&format!("{bucket}/{key}"));

            debug!(
                "GCS assemble_parts: bucket={} name={} upload_id={} parts={}",
                self.bucket,
                final_name,
                upload_id,
                parts.len()
            );

            let source_names: Vec<String> = parts
                .iter()
                .map(|(pn, _etag)| self.part_name(&upload_id, *pn))
                .collect();

            if source_names.len() <= MAX_COMPOSE_SOURCES {
                // Simple case: single compose call.
                self.gcs_compose(&source_names, &final_name).await?;
            } else {
                // Chain compose in batches of 32.
                let intermediates = self.chain_compose(&source_names, &final_name).await?;

                // Clean up intermediate composite objects.
                for name in &intermediates {
                    if let Err(e) = self.gcs_delete(name).await {
                        warn!("Failed to clean up intermediate {}: {}", name, e);
                    }
                }
            }

            // Compute MD5 of the final assembled object by downloading it.
            let data = self.gcs_download(&final_name).await?;
            let md5_hex = Self::compute_md5(&data);

            // Compute composite ETag from part MD5s (same as AWS backend).
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
                // Fallback: use MD5 of full assembled data.
                Ok(format!("\"{md5_hex}\""))
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

            debug!("GCS delete_parts: bucket={} prefix={}", self.bucket, prefix);

            // List all part objects under the prefix.
            let object_names = self.gcs_list_objects(&prefix).await?;

            // Delete each part object.
            for name in &object_names {
                if let Err(e) = self.gcs_delete(name).await {
                    warn!("Failed to delete part object {}: {}", name, e);
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
            // within the single upstream GCS bucket. Nothing to create on
            // the GCS side -- bucket existence is purely a metadata concern.
            debug!(
                "GCS create_bucket: {} (no-op, namespaced in {})",
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
            // anything from the upstream GCS bucket. Object cleanup is handled
            // via individual object deletes.
            debug!(
                "GCS delete_bucket: {} (no-op, namespaced in {})",
                bucket, self.bucket
            );
            Ok(())
        })
    }
}

// -- Tests -------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gcs_name_mapping() {
        // Test the key mapping formula: {prefix}{storage_key}
        let prefix = "bleepstore/";
        let storage_key = "my-bucket/my-key.txt";
        let expected = "bleepstore/my-bucket/my-key.txt";
        assert_eq!(format!("{prefix}{storage_key}"), expected);
    }

    #[test]
    fn test_gcs_name_mapping_no_prefix() {
        let prefix = "";
        let storage_key = "my-bucket/my-key.txt";
        let expected = "my-bucket/my-key.txt";
        assert_eq!(format!("{prefix}{storage_key}"), expected);
    }

    #[test]
    fn test_part_name_mapping() {
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
        let md5 = GcpGatewayBackend::compute_md5(b"");
        assert_eq!(md5, "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn test_compute_md5_hello() {
        let md5 = GcpGatewayBackend::compute_md5(b"hello world");
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

    #[test]
    fn test_url_encode_object_name() {
        let name = "path/to/my object.txt";
        let encoded = GcpGatewayBackend::url_encode_object_name(name);
        // All non-alphanumeric chars should be percent-encoded
        assert!(encoded.contains("%2F")); // /
        assert!(encoded.contains("%20")); // space
        assert!(!encoded.contains(' '));
    }

    #[test]
    fn test_url_encode_simple_name() {
        let name = "simple";
        let encoded = GcpGatewayBackend::url_encode_object_name(name);
        assert_eq!(encoded, "simple");
    }

    #[test]
    fn test_max_compose_sources_constant() {
        assert_eq!(MAX_COMPOSE_SOURCES, 32);
    }

    #[test]
    fn test_is_not_found() {
        assert!(GcpGatewayBackend::is_not_found(StatusCode::NOT_FOUND));
        assert!(!GcpGatewayBackend::is_not_found(StatusCode::OK));
        assert!(!GcpGatewayBackend::is_not_found(StatusCode::FORBIDDEN));
        assert!(!GcpGatewayBackend::is_not_found(
            StatusCode::INTERNAL_SERVER_ERROR
        ));
    }

    #[test]
    fn test_application_default_credentials_path() {
        // The function should return a path containing "application_default_credentials.json"
        let path = GcpGatewayBackend::application_default_credentials_path();
        assert!(path.ends_with("application_default_credentials.json"));
    }

    #[test]
    fn test_compose_chaining_batch_count() {
        // With 65 sources and MAX_COMPOSE_SOURCES=32:
        // Round 1: 3 batches (32, 32, 1) -> 3 intermediates (2 composed, 1 passthrough)
        // Round 2: 3 <= 32, final compose
        let num_sources: usize = 65;
        let num_batches_round1 = num_sources.div_ceil(MAX_COMPOSE_SOURCES);
        assert_eq!(num_batches_round1, 3);
        // After round 1 we have 3 sources, which is <= 32, so final compose
        assert!(num_batches_round1 <= MAX_COMPOSE_SOURCES);
    }

    #[test]
    fn test_compose_chaining_large() {
        // With 1025 sources:
        // Round 1: ceil(1025/32) = 33 intermediates
        // Round 2: ceil(33/32) = 2 intermediates
        // Round 3: 2 <= 32, final compose
        let mut count = 1025usize;
        let mut rounds = 0;
        while count > MAX_COMPOSE_SOURCES {
            count = count.div_ceil(MAX_COMPOSE_SOURCES);
            rounds += 1;
        }
        assert_eq!(rounds, 2); // 2 intermediate rounds before final
    }

    #[test]
    fn test_gcs_error_parsing() {
        let error_json = r#"{"error":{"code":404,"message":"No such object: bucket/key"}}"#;
        let parsed: GcsErrorResponse = serde_json::from_str(error_json).unwrap();
        assert_eq!(parsed.error.as_ref().unwrap().code, Some(404));
        assert_eq!(
            parsed.error.as_ref().unwrap().message.as_ref().unwrap(),
            "No such object: bucket/key"
        );
    }

    #[test]
    fn test_gcs_error_parsing_empty() {
        let error_json = r#"{"error":{}}"#;
        let parsed: GcsErrorResponse = serde_json::from_str(error_json).unwrap();
        assert!(parsed.error.is_some());
        assert_eq!(parsed.error.as_ref().unwrap().code, None);
    }

    #[test]
    fn test_base64_md5_conversion() {
        use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
        use base64::Engine;
        // GCS returns base64-encoded MD5 in md5Hash field.
        // Example: base64 of the 16-byte MD5 of empty string
        // MD5 of "" = d41d8cd98f00b204e9800998ecf8427e (hex)
        // which is 1BIMSf...(base64)
        let md5_bytes = hex::decode("d41d8cd98f00b204e9800998ecf8427e").unwrap();
        let gcs_md5_base64 = BASE64_STANDARD.encode(&md5_bytes);
        // Convert back to hex
        let decoded = BASE64_STANDARD.decode(&gcs_md5_base64).unwrap();
        let hex_md5 = hex::encode(&decoded);
        assert_eq!(hex_md5, "d41d8cd98f00b204e9800998ecf8427e");
    }
}
