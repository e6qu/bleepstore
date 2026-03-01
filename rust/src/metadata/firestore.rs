//! GCP Firestore metadata store backend.
//!
//! Collection/document design using REST API via reqwest:
//! - bucket_{name}              # Bucket metadata
//! - object_{bucket}_{key_b64}  # Object metadata (base64-encoded key)
//! - upload_{upload_id}         # Upload metadata
//!   └── parts/part_{number:05d}  # Part subcollection
//! - cred_{access_key}          # Credential

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::store::{
    BucketRecord, CredentialRecord, ListObjectsResult, ListPartsResult, ListUploadsResult,
    MetadataStore, MultipartUploadRecord, ObjectRecord, PartRecord,
};
use crate::config::FirestoreMetaConfig;

const FIRESTORE_EMULATOR_HOST: &str = "FIRESTORE_EMULATOR_HOST";
const DEFAULT_COLLECTION: &str = "bleepstore";
const UNICODE_SENTINEL: char = '\u{f8ff}';

#[allow(dead_code)]
fn now_iso() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S.000Z")
        .to_string()
}

fn encode_key(key: &str) -> String {
    URL_SAFE_NO_PAD.encode(key.as_bytes())
}

fn _decode_key(encoded: &str) -> Result<String> {
    let bytes = URL_SAFE_NO_PAD.decode(encoded)?;
    String::from_utf8(bytes).map_err(|e| anyhow!("Invalid UTF-8 in key: {e}"))
}

fn doc_id_bucket(bucket: &str) -> String {
    format!("bucket_{bucket}")
}

fn doc_id_object(bucket: &str, key: &str) -> String {
    format!("object_{}_{}", bucket, encode_key(key))
}

fn doc_id_upload(upload_id: &str) -> String {
    format!("upload_{upload_id}")
}

fn doc_id_part(part_number: u32) -> String {
    format!("part_{part_number:05}")
}

fn doc_id_credential(access_key: &str) -> String {
    format!("cred_{access_key}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreValue {
    #[serde(rename = "stringValue")]
    string_value: Option<String>,
    #[serde(rename = "integerValue")]
    integer_value: Option<String>,
    #[serde(rename = "booleanValue")]
    boolean_value: Option<bool>,
    #[serde(rename = "mapValue")]
    map_value: Option<FirestoreMap>,
    #[serde(rename = "arrayValue")]
    array_value: Option<FirestoreArray>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct FirestoreMap {
    fields: HashMap<String, FirestoreValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreArray {
    values: Vec<FirestoreValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreDocument {
    name: String,
    fields: HashMap<String, FirestoreValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreWrite {
    update: Option<FirestoreDocument>,
    delete: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreCommitRequest {
    writes: Vec<FirestoreWrite>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreQuery {
    r#struct: Option<String>,
    from: Vec<FirestoreCollectionSelector>,
    r#where: Option<FirestoreFilter>,
    order_by: Vec<FirestoreOrder>,
    limit: Option<i32>,
    start_at: Option<FirestoreCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreCollectionSelector {
    collection_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreFilter {
    field_filter: Option<FirestoreFieldFilter>,
    composite_filter: Option<FirestoreCompositeFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreFieldFilter {
    field: FirestoreFieldReference,
    op: String,
    value: FirestoreValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreCompositeFilter {
    op: String,
    filters: Vec<FirestoreFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreFieldReference {
    field_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreOrder {
    field: FirestoreFieldReference,
    direction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreCursor {
    values: Vec<FirestoreValue>,
    before: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreRunQueryRequest {
    structured_query: FirestoreQuery,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirestoreRunQueryResponse {
    document: Option<FirestoreDocument>,
}

pub struct FirestoreMetadataStore {
    client: Client,
    project_id: String,
    database_id: String,
    collection: String,
    base_url: String,
    token: Arc<RwLock<Option<String>>>,
    credentials_file: Option<String>,
}

impl FirestoreMetadataStore {
    pub async fn new(config: &FirestoreMetaConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        let project_id = config
            .project_id
            .clone()
            .or_else(|| std::env::var("GCP_PROJECT").ok())
            .or_else(|| std::env::var("GOOGLE_CLOUD_PROJECT").ok())
            .ok_or_else(|| anyhow!("GCP project_id is required"))?;

        let collection = config
            .collection_prefix
            .clone()
            .unwrap_or_else(|| DEFAULT_COLLECTION.to_string());

        let (base_url, database_id) =
            if let Ok(emulator_host) = std::env::var(FIRESTORE_EMULATOR_HOST) {
                (format!("http://{emulator_host}"), "(default)".to_string())
            } else {
                (
                    "https://firestore.googleapis.com".to_string(),
                    "(default)".to_string(),
                )
            };

        let store = Self {
            client,
            project_id,
            database_id,
            collection,
            base_url,
            token: Arc::new(RwLock::new(None)),
            credentials_file: config.credentials_file.clone(),
        };

        Ok(store)
    }

    pub fn seed_credential(&self, _access_key: &str, _secret_key: &str) -> Result<()> {
        Ok(())
    }

    fn doc_path(&self, doc_id: &str) -> String {
        format!(
            "projects/{}/databases/{}/documents/{}/{}",
            self.project_id, self.database_id, self.collection, doc_id
        )
    }

    fn subcollection_path(&self, doc_id: &str, _subcollection: &str, sub_doc_id: &str) -> String {
        format!(
            "projects/{}/databases/{}/documents/{}/{}/{}",
            self.project_id, self.database_id, self.collection, doc_id, sub_doc_id
        )
    }

    fn subcollection_parent(&self, doc_id: &str, subcollection: &str) -> String {
        format!(
            "projects/{}/databases/{}/documents/{}/{}/{}",
            self.project_id, self.database_id, self.collection, doc_id, subcollection
        )
    }

    async fn get_token(&self) -> Result<String> {
        if std::env::var(FIRESTORE_EMULATOR_HOST).is_ok() {
            return Ok("owner".to_string());
        }

        {
            let token_guard = self.token.read().await;
            if let Some(token) = token_guard.as_ref() {
                return Ok(token.clone());
            }
        }

        let token = self.fetch_gcp_token().await?;
        {
            let mut token_guard = self.token.write().await;
            *token_guard = Some(token.clone());
        }
        Ok(token)
    }

    async fn fetch_gcp_token(&self) -> Result<String> {
        if let Some(creds_file) = &self.credentials_file {
            let creds = std::fs::read_to_string(creds_file)?;
            let creds_json: serde_json::Value = serde_json::from_str(&creds)?;

            let client_email = creds_json["client_email"]
                .as_str()
                .ok_or_else(|| anyhow!("Missing client_email in credentials"))?
                .to_string();
            let private_key = creds_json["private_key"]
                .as_str()
                .ok_or_else(|| anyhow!("Missing private_key in credentials"))?
                .to_string();

            let jwt = self.sign_jwt(&private_key, &client_email)?;

            let response = self
                .client
                .post("https://oauth2.googleapis.com/token")
                .form(&[
                    ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                    ("assertion", &jwt),
                ])
                .send()
                .await?
                .json::<serde_json::Value>()
                .await?;

            response["access_token"]
                .as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| anyhow!("No access_token in response"))
        } else {
            self.fetch_metadata_token().await
        }
    }

    fn sign_jwt(&self, private_key: &str, client_email: &str) -> Result<String> {
        use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;

        let claims = serde_json::json!({
            "iss": client_email,
            "scope": "https://www.googleapis.com/auth/datastore",
            "aud": "https://oauth2.googleapis.com/token",
            "iat": now,
            "exp": now + 3600,
        });

        let key = EncodingKey::from_rsa_pem(private_key.as_bytes())
            .map_err(|e| anyhow!("Failed to parse RSA key: {e}"))?;

        let token = encode(&Header::new(Algorithm::RS256), &claims, &key)
            .map_err(|e| anyhow!("Failed to encode JWT: {e}"))?;

        Ok(token)
    }

    async fn fetch_metadata_token(&self) -> Result<String> {
        let response = self
            .client
            .get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token")
            .header("Metadata-Flavor", "Google")
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        response["access_token"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("No access_token in metadata response"))
    }

    async fn get_document(&self, doc_path: &str) -> Result<Option<FirestoreDocument>> {
        let token = self.get_token().await?;
        let url = format!("{}/v1/{}", self.base_url, doc_path);

        let response = self.client.get(&url).bearer_auth(&token).send().await?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Firestore get failed: {status} - {body}"));
        }

        let doc: FirestoreDocument = response.json().await?;
        Ok(Some(doc))
    }

    async fn create_document(
        &self,
        doc_id: &str,
        fields: HashMap<String, FirestoreValue>,
    ) -> Result<()> {
        let token = self.get_token().await?;
        let parent = format!(
            "projects/{}/databases/{}/documents/{}",
            self.project_id, self.database_id, self.collection
        );
        let url = format!(
            "{}/v1/{}/documents?documentId={}",
            self.base_url, parent, doc_id
        );

        let body = serde_json::json!({
            "fields": fields
        });

        let response = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Firestore create failed: {status} - {body}"));
        }

        Ok(())
    }

    async fn set_document(
        &self,
        doc_path: &str,
        fields: HashMap<String, FirestoreValue>,
    ) -> Result<()> {
        let token = self.get_token().await?;
        let url = format!(
            "{}/v1/{}?currentDocument.exists=false",
            self.base_url, doc_path
        );

        let body = serde_json::json!({
            "fields": fields
        });

        let response = self
            .client
            .patch(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let token = self.get_token().await?;
            let url = format!("{}/v1/{}", self.base_url, doc_path);

            let response = self
                .client
                .patch(&url)
                .bearer_auth(&token)
                .json(&body)
                .send()
                .await?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow!("Firestore set failed: {status} - {body}"));
            }
        }

        Ok(())
    }

    async fn update_document(
        &self,
        doc_path: &str,
        fields: HashMap<String, FirestoreValue>,
    ) -> Result<()> {
        let token = self.get_token().await?;
        let url = format!("{}/v1/{}", self.base_url, doc_path);

        let body = serde_json::json!({
            "fields": fields
        });

        let response = self
            .client
            .patch(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Firestore update failed: {status} - {body}"));
        }

        Ok(())
    }

    async fn delete_document(&self, doc_path: &str) -> Result<()> {
        let token = self.get_token().await?;
        let url = format!("{}/v1/{}", self.base_url, doc_path);

        let response = self.client.delete(&url).bearer_auth(&token).send().await?;

        if !response.status().is_success() && response.status() != reqwest::StatusCode::NOT_FOUND {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Firestore delete failed: {status} - {body}"));
        }

        Ok(())
    }

    async fn commit(&self, writes: Vec<FirestoreWrite>) -> Result<()> {
        let token = self.get_token().await?;
        let url = format!(
            "{}/v1/projects/{}/databases/{}/documents:commit",
            self.base_url, self.project_id, self.database_id
        );

        let body = FirestoreCommitRequest { writes };

        let response = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body_text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Firestore commit failed: {status} - {body_text}"));
        }

        Ok(())
    }

    async fn run_query(&self, query: FirestoreQuery) -> Result<Vec<FirestoreDocument>> {
        let token = self.get_token().await?;
        let url = format!(
            "{}/v1/projects/{}/databases/{}/documents:runQuery",
            self.base_url, self.project_id, self.database_id
        );

        let body = FirestoreRunQueryRequest {
            structured_query: query,
        };

        let response = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body_text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Firestore query failed: {status} - {body_text}"));
        }

        let responses: Vec<FirestoreRunQueryResponse> = response.json().await?;
        let documents: Vec<FirestoreDocument> =
            responses.into_iter().filter_map(|r| r.document).collect();

        Ok(documents)
    }

    async fn run_subcollection_query(
        &self,
        parent_path: &str,
        query: FirestoreQuery,
    ) -> Result<Vec<FirestoreDocument>> {
        let token = self.get_token().await?;
        let url = format!("{}/v1/{}:runQuery", self.base_url, parent_path);

        let body = FirestoreRunQueryRequest {
            structured_query: query,
        };

        let response = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body_text = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Firestore subcollection query failed: {status} - {body_text}"
            ));
        }

        let responses: Vec<FirestoreRunQueryResponse> = response.json().await?;
        let documents: Vec<FirestoreDocument> =
            responses.into_iter().filter_map(|r| r.document).collect();

        Ok(documents)
    }

    fn extract_string(value: &FirestoreValue) -> Option<String> {
        value.string_value.clone()
    }

    fn extract_u64(value: &FirestoreValue) -> Option<u64> {
        value.integer_value.as_ref().and_then(|s| s.parse().ok())
    }

    fn extract_bool(value: &FirestoreValue) -> bool {
        value.boolean_value.unwrap_or(false)
    }

    fn string_value(s: String) -> FirestoreValue {
        FirestoreValue {
            string_value: Some(s),
            integer_value: None,
            boolean_value: None,
            map_value: None,
            array_value: None,
        }
    }

    fn integer_value(n: u64) -> FirestoreValue {
        FirestoreValue {
            string_value: None,
            integer_value: Some(n.to_string()),
            boolean_value: None,
            map_value: None,
            array_value: None,
        }
    }

    fn bool_value(b: bool) -> FirestoreValue {
        FirestoreValue {
            string_value: None,
            integer_value: None,
            boolean_value: Some(b),
            map_value: None,
            array_value: None,
        }
    }

    fn doc_to_bucket(doc: &FirestoreDocument) -> Option<BucketRecord> {
        let fields = &doc.fields;
        Some(BucketRecord {
            name: Self::extract_string(fields.get("name")?)?,
            created_at: Self::extract_string(fields.get("created_at")?).unwrap_or_default(),
            region: Self::extract_string(fields.get("region")?)
                .unwrap_or_else(|| "us-east-1".to_string()),
            owner_id: Self::extract_string(fields.get("owner_id")?).unwrap_or_default(),
            owner_display: Self::extract_string(fields.get("owner_display")?).unwrap_or_default(),
            acl: Self::extract_string(fields.get("acl")?).unwrap_or_else(|| "{}".to_string()),
        })
    }

    fn doc_to_object(doc: &FirestoreDocument) -> Option<ObjectRecord> {
        let fields = &doc.fields;
        let user_meta_str =
            Self::extract_string(fields.get("user_metadata")?).unwrap_or_else(|| "{}".to_string());
        let user_metadata: HashMap<String, String> =
            serde_json::from_str(&user_meta_str).unwrap_or_default();

        Some(ObjectRecord {
            bucket: Self::extract_string(fields.get("bucket")?)?,
            key: Self::extract_string(fields.get("key")?)?,
            size: Self::extract_u64(fields.get("size")?).unwrap_or(0),
            etag: Self::extract_string(fields.get("etag")?).unwrap_or_default(),
            content_type: Self::extract_string(fields.get("content_type")?)
                .unwrap_or_else(|| "application/octet-stream".to_string()),
            content_encoding: Self::extract_string(fields.get("content_encoding")?),
            content_language: Self::extract_string(fields.get("content_language")?),
            content_disposition: Self::extract_string(fields.get("content_disposition")?),
            cache_control: Self::extract_string(fields.get("cache_control")?),
            expires: Self::extract_string(fields.get("expires")?),
            storage_class: Self::extract_string(fields.get("storage_class")?)
                .unwrap_or_else(|| "STANDARD".to_string()),
            acl: Self::extract_string(fields.get("acl")?).unwrap_or_else(|| "{}".to_string()),
            last_modified: Self::extract_string(fields.get("last_modified")?).unwrap_or_default(),
            user_metadata,
            delete_marker: fields
                .get("delete_marker")
                .map(Self::extract_bool)
                .unwrap_or(false),
        })
    }

    fn doc_to_upload(doc: &FirestoreDocument) -> Option<MultipartUploadRecord> {
        let fields = &doc.fields;
        let user_meta_str =
            Self::extract_string(fields.get("user_metadata")?).unwrap_or_else(|| "{}".to_string());
        let user_metadata: HashMap<String, String> =
            serde_json::from_str(&user_meta_str).unwrap_or_default();

        Some(MultipartUploadRecord {
            upload_id: Self::extract_string(fields.get("upload_id")?)?,
            bucket: Self::extract_string(fields.get("bucket")?)?,
            key: Self::extract_string(fields.get("key")?)?,
            content_type: Self::extract_string(fields.get("content_type")?)
                .unwrap_or_else(|| "application/octet-stream".to_string()),
            content_encoding: Self::extract_string(fields.get("content_encoding")?),
            content_language: Self::extract_string(fields.get("content_language")?),
            content_disposition: Self::extract_string(fields.get("content_disposition")?),
            cache_control: Self::extract_string(fields.get("cache_control")?),
            expires: Self::extract_string(fields.get("expires")?),
            storage_class: Self::extract_string(fields.get("storage_class")?)
                .unwrap_or_else(|| "STANDARD".to_string()),
            acl: Self::extract_string(fields.get("acl")?).unwrap_or_else(|| "{}".to_string()),
            user_metadata,
            owner_id: Self::extract_string(fields.get("owner_id")?).unwrap_or_default(),
            owner_display: Self::extract_string(fields.get("owner_display")?).unwrap_or_default(),
            initiated_at: Self::extract_string(fields.get("initiated_at")?).unwrap_or_default(),
        })
    }

    fn doc_to_part(doc: &FirestoreDocument) -> Option<PartRecord> {
        let fields = &doc.fields;
        Some(PartRecord {
            part_number: Self::extract_u64(fields.get("part_number")?)? as u32,
            size: Self::extract_u64(fields.get("size")?).unwrap_or(0),
            etag: Self::extract_string(fields.get("etag")?).unwrap_or_default(),
            last_modified: Self::extract_string(fields.get("last_modified")?).unwrap_or_default(),
        })
    }

    fn doc_to_credential(doc: &FirestoreDocument) -> Option<CredentialRecord> {
        let fields = &doc.fields;
        let active = fields.get("active").map(Self::extract_bool).unwrap_or(true);

        if !active {
            return None;
        }

        Some(CredentialRecord {
            access_key_id: Self::extract_string(fields.get("access_key_id")?)?,
            secret_key: Self::extract_string(fields.get("secret_key")?)?,
            owner_id: Self::extract_string(fields.get("owner_id")?).unwrap_or_default(),
            display_name: Self::extract_string(fields.get("display_name")?).unwrap_or_default(),
            active,
            created_at: Self::extract_string(fields.get("created_at")?).unwrap_or_default(),
        })
    }
}

impl MetadataStore for FirestoreMetadataStore {
    fn create_bucket(
        &self,
        record: BucketRecord,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            let doc_id = doc_id_bucket(&record.name);
            let mut fields = HashMap::new();
            fields.insert("type".to_string(), Self::string_value("bucket".to_string()));
            fields.insert("name".to_string(), Self::string_value(record.name));
            fields.insert("region".to_string(), Self::string_value(record.region));
            fields.insert("owner_id".to_string(), Self::string_value(record.owner_id));
            fields.insert(
                "owner_display".to_string(),
                Self::string_value(record.owner_display),
            );
            fields.insert("acl".to_string(), Self::string_value(record.acl));
            fields.insert(
                "created_at".to_string(),
                Self::string_value(record.created_at),
            );

            self.create_document(&doc_id, fields).await
        })
    }

    fn get_bucket(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<BucketRecord>>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let doc_id = doc_id_bucket(&name);
            let doc_path = self.doc_path(&doc_id);

            match self.get_document(&doc_path).await? {
                Some(doc) => Ok(Self::doc_to_bucket(&doc)),
                None => Ok(None),
            }
        })
    }

    fn bucket_exists(&self, name: &str) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let doc_id = doc_id_bucket(&name);
            let doc_path = self.doc_path(&doc_id);
            Ok(self.get_document(&doc_path).await?.is_some())
        })
    }

    fn list_buckets(&self) -> Pin<Box<dyn Future<Output = Result<Vec<BucketRecord>>> + Send + '_>> {
        Box::pin(async move {
            let query = FirestoreQuery {
                r#struct: None,
                from: vec![FirestoreCollectionSelector {
                    collection_id: self.collection.clone(),
                }],
                r#where: Some(FirestoreFilter {
                    field_filter: Some(FirestoreFieldFilter {
                        field: FirestoreFieldReference {
                            field_path: "type".to_string(),
                        },
                        op: "EQUAL".to_string(),
                        value: Self::string_value("bucket".to_string()),
                    }),
                    composite_filter: None,
                }),
                order_by: vec![],
                limit: None,
                start_at: None,
            };

            let docs = self.run_query(query).await?;
            let mut buckets: Vec<BucketRecord> =
                docs.iter().filter_map(Self::doc_to_bucket).collect();
            buckets.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(buckets)
        })
    }

    fn delete_bucket(&self, name: &str) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let doc_id = doc_id_bucket(&name);
            let doc_path = self.doc_path(&doc_id);
            self.delete_document(&doc_path).await
        })
    }

    fn update_bucket_acl(
        &self,
        name: &str,
        acl: &str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let name = name.to_string();
        let acl = acl.to_string();
        Box::pin(async move {
            let doc_id = doc_id_bucket(&name);
            let doc_path = self.doc_path(&doc_id);
            let mut fields = HashMap::new();
            fields.insert("acl".to_string(), Self::string_value(acl));
            self.update_document(&doc_path, fields).await
        })
    }

    fn put_object(
        &self,
        record: ObjectRecord,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            let doc_id = doc_id_object(&record.bucket, &record.key);
            let doc_path = self.doc_path(&doc_id);

            let user_meta =
                serde_json::to_string(&record.user_metadata).unwrap_or_else(|_| "{}".to_string());

            let mut fields = HashMap::new();
            fields.insert("type".to_string(), Self::string_value("object".to_string()));
            fields.insert("bucket".to_string(), Self::string_value(record.bucket));
            fields.insert("key".to_string(), Self::string_value(record.key));
            fields.insert("size".to_string(), Self::integer_value(record.size));
            fields.insert("etag".to_string(), Self::string_value(record.etag));
            fields.insert(
                "content_type".to_string(),
                Self::string_value(record.content_type),
            );
            fields.insert(
                "storage_class".to_string(),
                Self::string_value(record.storage_class),
            );
            fields.insert("acl".to_string(), Self::string_value(record.acl));
            fields.insert("user_metadata".to_string(), Self::string_value(user_meta));
            fields.insert(
                "last_modified".to_string(),
                Self::string_value(record.last_modified),
            );
            fields.insert(
                "delete_marker".to_string(),
                Self::bool_value(record.delete_marker),
            );

            if let Some(ce) = record.content_encoding {
                fields.insert("content_encoding".to_string(), Self::string_value(ce));
            }
            if let Some(cl) = record.content_language {
                fields.insert("content_language".to_string(), Self::string_value(cl));
            }
            if let Some(cd) = record.content_disposition {
                fields.insert("content_disposition".to_string(), Self::string_value(cd));
            }
            if let Some(cc) = record.cache_control {
                fields.insert("cache_control".to_string(), Self::string_value(cc));
            }
            if let Some(e) = record.expires {
                fields.insert("expires".to_string(), Self::string_value(e));
            }

            self.set_document(&doc_path, fields).await
        })
    }

    fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ObjectRecord>>> + Send + '_>> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        Box::pin(async move {
            let doc_id = doc_id_object(&bucket, &key);
            let doc_path = self.doc_path(&doc_id);

            match self.get_document(&doc_path).await? {
                Some(doc) => Ok(Self::doc_to_object(&doc)),
                None => Ok(None),
            }
        })
    }

    fn object_exists(
        &self,
        bucket: &str,
        key: &str,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + '_>> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        Box::pin(async move {
            let doc_id = doc_id_object(&bucket, &key);
            let doc_path = self.doc_path(&doc_id);
            Ok(self.get_document(&doc_path).await?.is_some())
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
    ) -> Pin<Box<dyn Future<Output = Result<ListObjectsResult>> + Send + '_>> {
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

            let mut filters = vec![
                FirestoreFieldFilter {
                    field: FirestoreFieldReference {
                        field_path: "type".to_string(),
                    },
                    op: "EQUAL".to_string(),
                    value: Self::string_value("object".to_string()),
                },
                FirestoreFieldFilter {
                    field: FirestoreFieldReference {
                        field_path: "bucket".to_string(),
                    },
                    op: "EQUAL".to_string(),
                    value: Self::string_value(bucket.clone()),
                },
            ];

            if !prefix.is_empty() {
                filters.push(FirestoreFieldFilter {
                    field: FirestoreFieldReference {
                        field_path: "key".to_string(),
                    },
                    op: "GREATER_THAN_OR_EQUAL".to_string(),
                    value: Self::string_value(prefix.clone()),
                });
                filters.push(FirestoreFieldFilter {
                    field: FirestoreFieldReference {
                        field_path: "key".to_string(),
                    },
                    op: "LESS_THAN".to_string(),
                    value: Self::string_value(format!("{prefix}{UNICODE_SENTINEL}")),
                });
            }

            let mut start_at = None;
            if !effective_start.is_empty() {
                start_at = Some(FirestoreCursor {
                    values: vec![Self::string_value(effective_start.to_string())],
                    before: false,
                });
            }

            let query = FirestoreQuery {
                r#struct: None,
                from: vec![FirestoreCollectionSelector {
                    collection_id: self.collection.clone(),
                }],
                r#where: Some(FirestoreFilter {
                    field_filter: None,
                    composite_filter: Some(FirestoreCompositeFilter {
                        op: "AND".to_string(),
                        filters: filters
                            .into_iter()
                            .map(|ff| FirestoreFilter {
                                field_filter: Some(ff),
                                composite_filter: None,
                            })
                            .collect(),
                    }),
                }),
                order_by: vec![FirestoreOrder {
                    field: FirestoreFieldReference {
                        field_path: "key".to_string(),
                    },
                    direction: "ASCENDING".to_string(),
                }],
                limit: Some((max_keys + 1) as i32),
                start_at,
            };

            let docs = self.run_query(query).await?;

            let mut objects: Vec<ObjectRecord> = docs
                .iter()
                .filter_map(Self::doc_to_object)
                .filter(|obj| obj.key.as_str() > effective_start)
                .collect();

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
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        Box::pin(async move {
            let doc_id = doc_id_object(&bucket, &key);
            let doc_path = self.doc_path(&doc_id);
            self.delete_document(&doc_path).await
        })
    }

    fn delete_objects(
        &self,
        bucket: &str,
        keys: &[String],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + '_>> {
        let bucket = bucket.to_string();
        let keys = keys.to_vec();
        Box::pin(async move {
            let deleted = keys.clone();

            for chunk in keys.chunks(100) {
                let writes: Vec<FirestoreWrite> = chunk
                    .iter()
                    .map(|key| {
                        let doc_id = doc_id_object(&bucket, key);
                        let doc_path = self.doc_path(&doc_id);
                        FirestoreWrite {
                            update: None,
                            delete: Some(doc_path),
                        }
                    })
                    .collect();

                if !writes.is_empty() {
                    let _ = self.commit(writes).await;
                }
            }

            Ok(deleted)
        })
    }

    fn update_object_acl(
        &self,
        bucket: &str,
        key: &str,
        acl: &str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        let acl = acl.to_string();
        Box::pin(async move {
            let doc_id = doc_id_object(&bucket, &key);
            let doc_path = self.doc_path(&doc_id);
            let mut fields = HashMap::new();
            fields.insert("acl".to_string(), Self::string_value(acl));
            self.update_document(&doc_path, fields).await
        })
    }

    fn count_objects(
        &self,
        bucket: &str,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + '_>> {
        let bucket = bucket.to_string();
        Box::pin(async move {
            let query = FirestoreQuery {
                r#struct: None,
                from: vec![FirestoreCollectionSelector {
                    collection_id: self.collection.clone(),
                }],
                r#where: Some(FirestoreFilter {
                    composite_filter: Some(FirestoreCompositeFilter {
                        op: "AND".to_string(),
                        filters: vec![
                            FirestoreFilter {
                                field_filter: Some(FirestoreFieldFilter {
                                    field: FirestoreFieldReference {
                                        field_path: "type".to_string(),
                                    },
                                    op: "EQUAL".to_string(),
                                    value: Self::string_value("object".to_string()),
                                }),
                                composite_filter: None,
                            },
                            FirestoreFilter {
                                field_filter: Some(FirestoreFieldFilter {
                                    field: FirestoreFieldReference {
                                        field_path: "bucket".to_string(),
                                    },
                                    op: "EQUAL".to_string(),
                                    value: Self::string_value(bucket.clone()),
                                }),
                                composite_filter: None,
                            },
                        ],
                    }),
                    field_filter: None,
                }),
                order_by: vec![],
                limit: None,
                start_at: None,
            };

            let docs = self.run_query(query).await?;
            Ok(docs.len() as u64)
        })
    }

    fn create_multipart_upload(
        &self,
        record: MultipartUploadRecord,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            let doc_id = doc_id_upload(&record.upload_id);
            let user_meta =
                serde_json::to_string(&record.user_metadata).unwrap_or_else(|_| "{}".to_string());

            let mut fields = HashMap::new();
            fields.insert("type".to_string(), Self::string_value("upload".to_string()));
            fields.insert(
                "upload_id".to_string(),
                Self::string_value(record.upload_id),
            );
            fields.insert("bucket".to_string(), Self::string_value(record.bucket));
            fields.insert("key".to_string(), Self::string_value(record.key));
            fields.insert(
                "content_type".to_string(),
                Self::string_value(record.content_type),
            );
            fields.insert(
                "storage_class".to_string(),
                Self::string_value(record.storage_class),
            );
            fields.insert("acl".to_string(), Self::string_value(record.acl));
            fields.insert("user_metadata".to_string(), Self::string_value(user_meta));
            fields.insert("owner_id".to_string(), Self::string_value(record.owner_id));
            fields.insert(
                "owner_display".to_string(),
                Self::string_value(record.owner_display),
            );
            fields.insert(
                "initiated_at".to_string(),
                Self::string_value(record.initiated_at),
            );

            if let Some(ce) = record.content_encoding {
                fields.insert("content_encoding".to_string(), Self::string_value(ce));
            }
            if let Some(cl) = record.content_language {
                fields.insert("content_language".to_string(), Self::string_value(cl));
            }
            if let Some(cd) = record.content_disposition {
                fields.insert("content_disposition".to_string(), Self::string_value(cd));
            }
            if let Some(cc) = record.cache_control {
                fields.insert("cache_control".to_string(), Self::string_value(cc));
            }
            if let Some(e) = record.expires {
                fields.insert("expires".to_string(), Self::string_value(e));
            }

            self.create_document(&doc_id, fields).await
        })
    }

    fn get_multipart_upload(
        &self,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<MultipartUploadRecord>>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let doc_id = doc_id_upload(&upload_id);
            let doc_path = self.doc_path(&doc_id);

            match self.get_document(&doc_path).await? {
                Some(doc) => Ok(Self::doc_to_upload(&doc)),
                None => Ok(None),
            }
        })
    }

    fn put_part(
        &self,
        upload_id: &str,
        part: PartRecord,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let parent_doc_id = doc_id_upload(&upload_id);
            let part_doc_id = doc_id_part(part.part_number);
            let part_path = self.subcollection_path(&parent_doc_id, "parts", &part_doc_id);

            let mut fields = HashMap::new();
            fields.insert("type".to_string(), Self::string_value("part".to_string()));
            fields.insert("upload_id".to_string(), Self::string_value(upload_id));
            fields.insert(
                "part_number".to_string(),
                Self::integer_value(part.part_number as u64),
            );
            fields.insert("size".to_string(), Self::integer_value(part.size));
            fields.insert("etag".to_string(), Self::string_value(part.etag));
            fields.insert(
                "last_modified".to_string(),
                Self::string_value(part.last_modified),
            );

            self.set_document(&part_path, fields).await
        })
    }

    fn list_parts(
        &self,
        upload_id: &str,
        max_parts: u32,
        part_number_marker: u32,
    ) -> Pin<Box<dyn Future<Output = Result<ListPartsResult>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let parent_doc_id = doc_id_upload(&upload_id);
            let parent_path = self.subcollection_parent(&parent_doc_id, "parts");

            let mut start_at = None;
            if part_number_marker > 0 {
                start_at = Some(FirestoreCursor {
                    values: vec![Self::integer_value((part_number_marker + 1) as u64)],
                    before: false,
                });
            }

            let query = FirestoreQuery {
                r#struct: None,
                from: vec![FirestoreCollectionSelector {
                    collection_id: "parts".to_string(),
                }],
                r#where: None,
                order_by: vec![FirestoreOrder {
                    field: FirestoreFieldReference {
                        field_path: "part_number".to_string(),
                    },
                    direction: "ASCENDING".to_string(),
                }],
                limit: Some((max_parts + 1) as i32),
                start_at,
            };

            let docs = self.run_subcollection_query(&parent_path, query).await?;

            let mut parts: Vec<PartRecord> = docs
                .into_iter()
                .filter_map(|doc| Self::doc_to_part(&doc))
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
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PartRecord>>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let parent_doc_id = doc_id_upload(&upload_id);
            let parent_path = self.subcollection_parent(&parent_doc_id, "parts");

            let query = FirestoreQuery {
                r#struct: None,
                from: vec![FirestoreCollectionSelector {
                    collection_id: "parts".to_string(),
                }],
                r#where: None,
                order_by: vec![FirestoreOrder {
                    field: FirestoreFieldReference {
                        field_path: "part_number".to_string(),
                    },
                    direction: "ASCENDING".to_string(),
                }],
                limit: None,
                start_at: None,
            };

            let docs = self.run_subcollection_query(&parent_path, query).await?;

            let mut parts: Vec<PartRecord> = docs
                .into_iter()
                .filter_map(|doc| Self::doc_to_part(&doc))
                .collect();

            parts.sort_by_key(|p| p.part_number);
            Ok(parts)
        })
    }

    fn complete_multipart_upload(
        &self,
        upload_id: &str,
        final_object: ObjectRecord,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            self.put_object(final_object).await?;

            let parts = self.get_parts_for_completion(&upload_id).await?;

            let upload_doc_id = doc_id_upload(&upload_id);
            let upload_doc_path = self.doc_path(&upload_doc_id);

            let mut writes: Vec<FirestoreWrite> = Vec::new();

            for part in &parts {
                let part_doc_id = doc_id_part(part.part_number);
                let part_path = self.subcollection_path(&upload_doc_id, "parts", &part_doc_id);
                writes.push(FirestoreWrite {
                    update: None,
                    delete: Some(part_path),
                });
            }

            writes.push(FirestoreWrite {
                update: None,
                delete: Some(upload_doc_path),
            });

            for chunk in writes.chunks(500) {
                self.commit(chunk.to_vec()).await?;
            }

            Ok(())
        })
    }

    fn delete_multipart_upload(
        &self,
        upload_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let upload_id = upload_id.to_string();
        Box::pin(async move {
            let parts = self.get_parts_for_completion(&upload_id).await?;

            let upload_doc_id = doc_id_upload(&upload_id);
            let upload_doc_path = self.doc_path(&upload_doc_id);

            let mut writes: Vec<FirestoreWrite> = Vec::new();

            for part in &parts {
                let part_doc_id = doc_id_part(part.part_number);
                let part_path = self.subcollection_path(&upload_doc_id, "parts", &part_doc_id);
                writes.push(FirestoreWrite {
                    update: None,
                    delete: Some(part_path),
                });
            }

            writes.push(FirestoreWrite {
                update: None,
                delete: Some(upload_doc_path),
            });

            for chunk in writes.chunks(500) {
                self.commit(chunk.to_vec()).await?;
            }

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
    ) -> Pin<Box<dyn Future<Output = Result<ListUploadsResult>> + Send + '_>> {
        let bucket = bucket.to_string();
        let prefix = prefix.to_string();
        let key_marker = key_marker.to_string();
        let upload_id_marker = upload_id_marker.to_string();
        Box::pin(async move {
            let mut filters = vec![
                FirestoreFieldFilter {
                    field: FirestoreFieldReference {
                        field_path: "type".to_string(),
                    },
                    op: "EQUAL".to_string(),
                    value: Self::string_value("upload".to_string()),
                },
                FirestoreFieldFilter {
                    field: FirestoreFieldReference {
                        field_path: "bucket".to_string(),
                    },
                    op: "EQUAL".to_string(),
                    value: Self::string_value(bucket.clone()),
                },
            ];

            if !prefix.is_empty() {
                filters.push(FirestoreFieldFilter {
                    field: FirestoreFieldReference {
                        field_path: "key".to_string(),
                    },
                    op: "GREATER_THAN_OR_EQUAL".to_string(),
                    value: Self::string_value(prefix.clone()),
                });
                filters.push(FirestoreFieldFilter {
                    field: FirestoreFieldReference {
                        field_path: "key".to_string(),
                    },
                    op: "LESS_THAN".to_string(),
                    value: Self::string_value(format!("{prefix}{UNICODE_SENTINEL}")),
                });
            }

            let query = FirestoreQuery {
                r#struct: None,
                from: vec![FirestoreCollectionSelector {
                    collection_id: self.collection.clone(),
                }],
                r#where: Some(FirestoreFilter {
                    field_filter: None,
                    composite_filter: Some(FirestoreCompositeFilter {
                        op: "AND".to_string(),
                        filters: filters
                            .into_iter()
                            .map(|ff| FirestoreFilter {
                                field_filter: Some(ff),
                                composite_filter: None,
                            })
                            .collect(),
                    }),
                }),
                order_by: vec![
                    FirestoreOrder {
                        field: FirestoreFieldReference {
                            field_path: "key".to_string(),
                        },
                        direction: "ASCENDING".to_string(),
                    },
                    FirestoreOrder {
                        field: FirestoreFieldReference {
                            field_path: "upload_id".to_string(),
                        },
                        direction: "ASCENDING".to_string(),
                    },
                ],
                limit: Some((max_uploads + 1) as i32),
                start_at: None,
            };

            let docs = self.run_query(query).await?;

            let mut uploads: Vec<MultipartUploadRecord> = docs
                .iter()
                .filter_map(Self::doc_to_upload)
                .filter(|u| {
                    if key_marker.is_empty() {
                        true
                    } else {
                        u.key.as_str() > key_marker.as_str()
                            || (u.key == key_marker
                                && u.upload_id.as_str() > upload_id_marker.as_str())
                    }
                })
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
    ) -> Pin<Box<dyn Future<Output = Result<Option<CredentialRecord>>> + Send + '_>> {
        let access_key_id = access_key_id.to_string();
        Box::pin(async move {
            let doc_id = doc_id_credential(&access_key_id);
            let doc_path = self.doc_path(&doc_id);

            match self.get_document(&doc_path).await? {
                Some(doc) => Ok(Self::doc_to_credential(&doc)),
                None => Ok(None),
            }
        })
    }

    fn put_credential(
        &self,
        record: CredentialRecord,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            let doc_id = doc_id_credential(&record.access_key_id);
            let doc_path = self.doc_path(&doc_id);

            let mut fields = HashMap::new();
            fields.insert(
                "type".to_string(),
                Self::string_value("credential".to_string()),
            );
            fields.insert(
                "access_key_id".to_string(),
                Self::string_value(record.access_key_id),
            );
            fields.insert(
                "secret_key".to_string(),
                Self::string_value(record.secret_key),
            );
            fields.insert("owner_id".to_string(), Self::string_value(record.owner_id));
            fields.insert(
                "display_name".to_string(),
                Self::string_value(record.display_name),
            );
            fields.insert("active".to_string(), Self::bool_value(record.active));
            fields.insert(
                "created_at".to_string(),
                Self::string_value(record.created_at),
            );

            self.set_document(&doc_path, fields).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn should_run_firestore_tests() -> bool {
        std::env::var(FIRESTORE_EMULATOR_HOST).is_ok()
    }

    fn get_test_config() -> FirestoreMetaConfig {
        FirestoreMetaConfig {
            collection_prefix: Some(format!("test_{}", chrono::Utc::now().timestamp())),
            project_id: Some("test-project".to_string()),
            credentials_file: None,
        }
    }

    #[test]
    fn test_encode_decode_key() {
        let key = "path/to/object.txt";
        let encoded = encode_key(key);
        let decoded = _decode_key(&encoded).unwrap();
        assert_eq!(key, decoded);

        let key_with_special = "hello world!@#$%";
        let encoded = encode_key(key_with_special);
        let decoded = _decode_key(&encoded).unwrap();
        assert_eq!(key_with_special, decoded);
    }

    #[test]
    fn test_doc_id_generation() {
        assert_eq!(doc_id_bucket("my-bucket"), "bucket_my-bucket");
        assert_eq!(doc_id_upload("abc123"), "upload_abc123");
        assert_eq!(doc_id_part(1), "part_00001");
        assert_eq!(doc_id_part(42), "part_00042");
        assert_eq!(
            doc_id_credential("AKIAIOSFODNN7EXAMPLE"),
            "cred_AKIAIOSFODNN7EXAMPLE"
        );

        let object_id = doc_id_object("my-bucket", "path/to/file.txt");
        assert!(object_id.starts_with("object_my-bucket_"));
    }

    #[tokio::test]
    async fn test_firestore_bucket_crud() {
        if !should_run_firestore_tests() {
            eprintln!("Skipping Firestore test: FIRESTORE_EMULATOR_HOST not set");
            return;
        }

        let config = get_test_config();
        let store = FirestoreMetadataStore::new(&config).await.unwrap();

        let bucket_name = format!("test-bucket-{}", chrono::Utc::now().timestamp());
        let record = BucketRecord {
            name: bucket_name.clone(),
            created_at: now_iso(),
            region: "us-east-1".to_string(),
            owner_id: "owner123".to_string(),
            owner_display: "Test Owner".to_string(),
            acl: "{}".to_string(),
        };

        store.create_bucket(record.clone()).await.unwrap();

        assert!(store.bucket_exists(&bucket_name).await.unwrap());

        let retrieved = store.get_bucket(&bucket_name).await.unwrap().unwrap();
        assert_eq!(retrieved.name, bucket_name);
        assert_eq!(retrieved.region, "us-east-1");

        let buckets = store.list_buckets().await.unwrap();
        assert!(buckets.iter().any(|b| b.name == bucket_name));

        store
            .update_bucket_acl(&bucket_name, r#"{"owner":"test"}"#)
            .await
            .unwrap();
        let updated = store.get_bucket(&bucket_name).await.unwrap().unwrap();
        assert_eq!(updated.acl, r#"{"owner":"test"}"#);

        store.delete_bucket(&bucket_name).await.unwrap();
        assert!(!store.bucket_exists(&bucket_name).await.unwrap());
    }

    #[tokio::test]
    async fn test_firestore_object_crud() {
        if !should_run_firestore_tests() {
            eprintln!("Skipping Firestore test: FIRESTORE_EMULATOR_HOST not set");
            return;
        }

        let config = get_test_config();
        let store = FirestoreMetadataStore::new(&config).await.unwrap();

        let bucket = format!("test-bucket-{}", chrono::Utc::now().timestamp());
        let bucket_record = BucketRecord {
            name: bucket.clone(),
            created_at: now_iso(),
            region: "us-east-1".to_string(),
            owner_id: "owner123".to_string(),
            owner_display: "Test Owner".to_string(),
            acl: "{}".to_string(),
        };
        store.create_bucket(bucket_record).await.unwrap();

        let key = "path/to/object.txt";
        let record = ObjectRecord {
            bucket: bucket.clone(),
            key: key.to_string(),
            size: 1024,
            etag: "\"abc123\"".to_string(),
            content_type: "text/plain".to_string(),
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
        };

        store.put_object(record.clone()).await.unwrap();

        assert!(store.object_exists(&bucket, key).await.unwrap());

        let retrieved = store.get_object(&bucket, key).await.unwrap().unwrap();
        assert_eq!(retrieved.key, key);
        assert_eq!(retrieved.size, 1024);
        assert_eq!(retrieved.etag, "\"abc123\"");

        let count = store.count_objects(&bucket).await.unwrap();
        assert_eq!(count, 1);

        store.delete_object(&bucket, key).await.unwrap();
        assert!(!store.object_exists(&bucket, key).await.unwrap());

        store.delete_bucket(&bucket).await.unwrap();
    }

    #[tokio::test]
    async fn test_firestore_credential() {
        if !should_run_firestore_tests() {
            eprintln!("Skipping Firestore test: FIRESTORE_EMULATOR_HOST not set");
            return;
        }

        let config = get_test_config();
        let store = FirestoreMetadataStore::new(&config).await.unwrap();

        let access_key = format!("test-key-{}", chrono::Utc::now().timestamp());
        let record = CredentialRecord {
            access_key_id: access_key.clone(),
            secret_key: "secret123".to_string(),
            owner_id: "owner123".to_string(),
            display_name: "Test Owner".to_string(),
            active: true,
            created_at: now_iso(),
        };

        store.put_credential(record.clone()).await.unwrap();

        let retrieved = store.get_credential(&access_key).await.unwrap().unwrap();
        assert_eq!(retrieved.access_key_id, access_key);
        assert_eq!(retrieved.secret_key, "secret123");
        assert!(retrieved.active);
    }
}
