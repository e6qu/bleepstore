//! Axum router construction and S3 route mapping.
//!
//! The [`app`] function wires every S3-compatible endpoint to its handler
//! and returns a ready-to-serve [`axum::Router`].
//!
//! S3 distinguishes operations by query parameters, not just path+method.
//! For example, `GET /:bucket` could be ListObjectsV2 (no special query),
//! GetBucketLocation (`?location`), GetBucketAcl (`?acl`), or
//! ListMultipartUploads (`?uploads`). We use a single handler per
//! method+path that dispatches internally based on query params.

use axum::{
    extract::{DefaultBodyLimit, Path, RawQuery, State},
    http::{HeaderMap, HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, head, post, put},
    Router,
};
use sha2::Digest;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::auth;
use crate::errors::{generate_request_id, S3Error};
use crate::metrics::{metrics_handler, metrics_middleware};
use crate::AppState;

// -- OpenAPI specification ----------------------------------------------------

/// OpenAPI documentation for the BleepStore S3-compatible API.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "BleepStore S3-Compatible API",
        version = "0.1.0",
        description = "S3-compatible object storage server"
    ),
    paths(
        // Health check
        health_check,
        // Bucket operations
        crate::handlers::bucket::list_buckets,
        crate::handlers::bucket::create_bucket,
        crate::handlers::bucket::delete_bucket,
        crate::handlers::bucket::head_bucket,
        crate::handlers::bucket::get_bucket_location,
        crate::handlers::bucket::get_bucket_acl,
        crate::handlers::bucket::put_bucket_acl,
        // Object operations
        crate::handlers::object::put_object,
        crate::handlers::object::get_object,
        crate::handlers::object::head_object,
        crate::handlers::object::delete_object,
        crate::handlers::object::delete_objects,
        crate::handlers::object::copy_object,
        crate::handlers::object::list_objects_v2,
        crate::handlers::object::list_objects_v1,
        crate::handlers::object::get_object_acl,
        crate::handlers::object::put_object_acl,
        // Multipart operations
        crate::handlers::multipart::create_multipart_upload,
        crate::handlers::multipart::upload_part,
        crate::handlers::multipart::complete_multipart_upload,
        crate::handlers::multipart::abort_multipart_upload,
        crate::handlers::multipart::list_multipart_uploads,
        crate::handlers::multipart::list_parts,
    ),
    tags(
        (name = "Health", description = "Health check endpoints"),
        (name = "Bucket", description = "S3 bucket operations"),
        (name = "Object", description = "S3 object operations"),
        (name = "Multipart", description = "S3 multipart upload operations"),
    )
)]
struct ApiDoc;

/// Build the axum [`Router`] with all S3-compatible routes.
///
/// The returned router is ready to be passed to `axum::serve`.
pub fn app(state: Arc<AppState>) -> Router {
    let openapi = ApiDoc::openapi();

    Router::new()
        // Health check endpoint (not part of S3 API).
        .route("/health", get(health_check))
        // Prometheus metrics endpoint.
        .route("/metrics", get(metrics_handler))
        // Service-level: GET / -> ListBuckets
        .route("/", get(handle_get_service))
        // Bucket-level routes
        .route("/:bucket", get(handle_get_bucket))
        .route("/:bucket", put(handle_put_bucket))
        .route("/:bucket", delete(handle_delete_bucket))
        .route("/:bucket", head(handle_head_bucket))
        .route("/:bucket", post(handle_post_bucket))
        // Object-level routes (wildcard key captures slashes)
        .route("/:bucket/*key", get(handle_get_object))
        .route("/:bucket/*key", put(handle_put_object))
        .route("/:bucket/*key", delete(handle_delete_object))
        .route("/:bucket/*key", head(handle_head_object))
        .route("/:bucket/*key", post(handle_post_object))
        // Swagger UI at /docs, OpenAPI spec at /openapi.json
        .merge(SwaggerUi::new("/docs").url("/openapi.json", openapi))
        // Application state shared across all handlers.
        .with_state(state.clone())
        // Layer ordering: inner layers run first, outer layers wrap them.
        // auth_middleware is innermost (closest to handlers, after routing).
        .layer(middleware::from_fn_with_state(state, auth_middleware))
        // common_headers_middleware is next (adds standard S3 headers).
        .layer(middleware::from_fn(common_headers_middleware))
        // metrics_middleware is outer (captures full request lifecycle).
        .layer(middleware::from_fn(metrics_middleware))
        // Disable the default 2MB body size limit (S3 objects can be large).
        .layer(DefaultBodyLimit::disable())
}

// -- Common headers middleware -----------------------------------------------

/// Tower middleware that adds common S3 response headers to every response:
/// - `x-amz-request-id`: 16-character uppercase hex string
/// - `Date`: RFC 7231 formatted timestamp
/// - `Server`: `BleepStore`
async fn common_headers_middleware(req: Request<axum::body::Body>, next: Next) -> Response {
    let mut response = next.run(req).await;
    let headers = response.headers_mut();

    // Only set x-amz-request-id if not already present (error handler may set it)
    if !headers.contains_key("x-amz-request-id") {
        let request_id = generate_request_id();
        headers.insert(
            "x-amz-request-id",
            HeaderValue::from_str(&request_id).unwrap(),
        );
    }

    let date = httpdate::fmt_http_date(std::time::SystemTime::now());
    // Always overwrite Date and Server to ensure consistency
    headers.insert("date", HeaderValue::from_str(&date).unwrap());
    headers.insert("server", HeaderValue::from_static("BleepStore"));

    response
}

// -- Auth middleware ---------------------------------------------------------

/// Paths that bypass authentication.
const AUTH_SKIP_PATHS: &[&str] = &["/health", "/metrics", "/docs", "/openapi.json"];

/// SigV4 authentication middleware.
///
/// Runs before handlers. Detects auth type (header, presigned, or none),
/// looks up credentials in the metadata store, and verifies the signature.
/// Returns `AccessDenied`, `InvalidAccessKeyId`, or `SignatureDoesNotMatch`
/// on failure.
///
/// Skips auth for /health, /metrics, /docs, /openapi.json.
async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut req: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, S3Error> {
    let path = req.uri().path().to_string();

    // Skip auth for infrastructure/doc endpoints and Swagger UI assets.
    if AUTH_SKIP_PATHS.iter().any(|skip| path == *skip)
        || path.starts_with("/docs/")
        || path.starts_with("/docs?")
    {
        return Ok(next.run(req).await);
    }

    // Extract the Authorization header.
    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Extract the raw query string.
    let query_string = req.uri().query().unwrap_or("").to_string();

    // Detect auth type.
    let auth_type = match auth::detect_auth_type(auth_header.as_deref(), &query_string) {
        Ok(t) => t,
        Err(msg) => {
            warn!("Auth detection error: {}", msg);
            return Err(S3Error::AccessDenied { message: msg });
        }
    };

    match auth_type {
        auth::AuthType::None => {
            // No authentication provided: deny access.
            return Err(S3Error::AccessDenied {
                message: "No authentication information provided".to_string(),
            });
        }
        auth::AuthType::Header(parsed) => {
            // Look up credential (cache first, then DB).
            let credential = if let Some(cached) =
                state.auth_cache.get_credential(&parsed.access_key_id)
            {
                cached
            } else {
                let db_cred = state
                    .metadata
                    .get_credential(&parsed.access_key_id)
                    .await
                    .map_err(S3Error::InternalError)?;
                match db_cred {
                    Some(c) => {
                        state
                            .auth_cache
                            .put_credential(&parsed.access_key_id, c.clone());
                        c
                    }
                    None => {
                        debug!("Unknown access key: {}", parsed.access_key_id);
                        return Err(S3Error::InvalidAccessKeyId);
                    }
                }
            };

            // Check clock skew using x-amz-date header.
            let amz_date = req
                .headers()
                .get("x-amz-date")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if !amz_date.is_empty() && !auth::check_clock_skew(amz_date) {
                warn!(
                    "Clock skew too large for access key {}: {}",
                    parsed.access_key_id, amz_date
                );
                return Err(S3Error::AccessDenied {
                    message:
                        "The difference between the request time and the server's time is too large"
                            .to_string(),
                });
            }

            // Validate credential date matches x-amz-date date portion.
            if !amz_date.is_empty() && amz_date.len() >= 8 && parsed.date_stamp != amz_date[..8] {
                return Err(S3Error::AccessDenied {
                    message: "Credential date does not match x-amz-date".to_string(),
                });
            }

            // Extract headers for signing.
            let headers = auth::extract_headers_for_signing(req.headers());

            // Get payload hash from x-amz-content-sha256 header.
            // If the header is absent (e.g., non-S3 SigV4 clients like botocore SigV4Auth),
            // we must compute SHA256(body) since that's what the client used for signing.
            let has_content_sha256 = req.headers().contains_key("x-amz-content-sha256");
            let payload_hash = if has_content_sha256 {
                req.headers()
                    .get("x-amz-content-sha256")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("UNSIGNED-PAYLOAD")
                    .to_string()
            } else {
                // Read the body, compute SHA256, and reconstruct the request.
                let (parts, body) = req.into_parts();
                let body_bytes = axum::body::to_bytes(body, usize::MAX)
                    .await
                    .unwrap_or_default();
                let hash = hex::encode(sha2::Sha256::digest(&body_bytes));
                req = Request::from_parts(parts, axum::body::Body::from(body_bytes));
                hash
            };

            // Derive signing key (cache first, then compute).
            let signing_key = if let Some(cached) = state.auth_cache.get_signing_key(
                &credential.secret_key,
                &parsed.date_stamp,
                &parsed.region,
                &parsed.service,
            ) {
                cached
            } else {
                let derived = auth::derive_signing_key(
                    &credential.secret_key,
                    &parsed.date_stamp,
                    &parsed.region,
                    &parsed.service,
                );
                state.auth_cache.put_signing_key(
                    &credential.secret_key,
                    &parsed.date_stamp,
                    &parsed.region,
                    &parsed.service,
                    derived.clone(),
                );
                derived
            };

            // Build canonical request and string to sign.
            let method = req.method().as_str().to_string();
            let uri = req.uri().path().to_string();

            let canonical_request = auth::build_canonical_request(
                &method,
                &uri,
                &query_string,
                &headers,
                &parsed.signed_headers,
                &payload_hash,
            );

            let timestamp = auth::find_header_value_pub(&headers, "x-amz-date")
                .or_else(|| auth::find_header_value_pub(&headers, "date"))
                .unwrap_or_default();

            let string_to_sign = auth::build_string_to_sign(
                timestamp,
                &parsed.credential_scope,
                &canonical_request,
            );

            let computed = auth::compute_signature(&signing_key, &string_to_sign);
            let valid = auth::constant_time_eq(&computed, &parsed.signature);

            if !valid {
                debug!("Signature mismatch for access key {}", parsed.access_key_id);
                return Err(S3Error::SignatureDoesNotMatch);
            }

            debug!("Auth OK for access key {}", parsed.access_key_id);
        }
        auth::AuthType::Presigned(parsed) => {
            // Look up credential (cache first, then DB).
            let credential = if let Some(cached) =
                state.auth_cache.get_credential(&parsed.access_key_id)
            {
                cached
            } else {
                let db_cred = state
                    .metadata
                    .get_credential(&parsed.access_key_id)
                    .await
                    .map_err(S3Error::InternalError)?;
                match db_cred {
                    Some(c) => {
                        state
                            .auth_cache
                            .put_credential(&parsed.access_key_id, c.clone());
                        c
                    }
                    None => {
                        debug!("Unknown access key: {}", parsed.access_key_id);
                        return Err(S3Error::InvalidAccessKeyId);
                    }
                }
            };

            // Check presigned URL expiration.
            if !auth::check_presigned_expiration(&parsed.amz_date, parsed.expires) {
                warn!(
                    "Presigned URL expired for access key {}",
                    parsed.access_key_id
                );
                return Err(S3Error::AccessDenied {
                    message: "Request has expired".to_string(),
                });
            }

            // Derive signing key (cache first, then compute).
            let signing_key = if let Some(cached) = state.auth_cache.get_signing_key(
                &credential.secret_key,
                &parsed.date_stamp,
                &parsed.region,
                &parsed.service,
            ) {
                cached
            } else {
                let derived = auth::derive_signing_key(
                    &credential.secret_key,
                    &parsed.date_stamp,
                    &parsed.region,
                    &parsed.service,
                );
                state.auth_cache.put_signing_key(
                    &credential.secret_key,
                    &parsed.date_stamp,
                    &parsed.region,
                    &parsed.service,
                    derived.clone(),
                );
                derived
            };

            // Extract headers for signing.
            let headers = auth::extract_headers_for_signing(req.headers());

            // Verify the signature.
            let method = req.method().as_str().to_string();
            let uri = req.uri().path().to_string();

            let canonical_request = auth::build_canonical_request(
                &method,
                &uri,
                &query_string,
                &headers,
                &parsed.signed_headers,
                "UNSIGNED-PAYLOAD",
            );

            let string_to_sign = auth::build_string_to_sign(
                &parsed.amz_date,
                &parsed.credential_scope,
                &canonical_request,
            );

            let computed = auth::compute_signature(&signing_key, &string_to_sign);
            let valid = auth::constant_time_eq(&computed, &parsed.signature);

            if !valid {
                debug!(
                    "Presigned signature mismatch for access key {}",
                    parsed.access_key_id
                );
                return Err(S3Error::SignatureDoesNotMatch);
            }

            debug!("Presigned auth OK for access key {}", parsed.access_key_id);
        }
    }

    Ok(next.run(req).await)
}

// -- Health check ------------------------------------------------------------

/// `GET /health` -- Returns `{"status": "ok"}` with 200 OK.
#[utoipa::path(
    get,
    path = "/health",
    tag = "Health",
    operation_id = "HealthCheck",
    responses(
        (status = 200, description = "Health check OK")
    )
)]
async fn health_check() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        r#"{"status":"ok"}"#,
    )
}

// -- Query parameter parsing helper ------------------------------------------

/// Parse raw query string into a HashMap.
fn parse_query(raw: Option<String>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(qs) = raw {
        for part in qs.split('&') {
            if let Some((k, v)) = part.split_once('=') {
                let decoded_k = percent_encoding::percent_decode_str(k)
                    .decode_utf8_lossy()
                    .into_owned();
                let decoded_v = percent_encoding::percent_decode_str(v)
                    .decode_utf8_lossy()
                    .into_owned();
                map.insert(decoded_k, decoded_v);
            } else if !part.is_empty() {
                // Query params without value (e.g., `?location`, `?acl`, `?uploads`)
                let decoded = percent_encoding::percent_decode_str(part)
                    .decode_utf8_lossy()
                    .into_owned();
                map.insert(decoded, String::new());
            }
        }
    }
    map
}

// -- Service-level dispatch --------------------------------------------------

/// `GET /` -- ListBuckets
async fn handle_get_service(State(state): State<Arc<AppState>>) -> Result<Response, S3Error> {
    crate::handlers::bucket::list_buckets(state).await
}

// -- Bucket-level dispatch ---------------------------------------------------

/// `GET /:bucket` -- dispatches based on query params:
/// - `?location` -> GetBucketLocation
/// - `?acl` -> GetBucketAcl
/// - `?uploads` -> ListMultipartUploads
/// - `?list-type=2` -> ListObjectsV2
/// - default -> ListObjectsV1
async fn handle_get_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Result<Response, S3Error> {
    let query = parse_query(raw_query);

    if query.contains_key("location") {
        crate::handlers::bucket::get_bucket_location(state, &bucket).await
    } else if query.contains_key("acl") {
        crate::handlers::bucket::get_bucket_acl(state, &bucket).await
    } else if query.contains_key("uploads") {
        crate::handlers::multipart::list_multipart_uploads(state, &bucket, &query).await
    } else if query.get("list-type").is_some_and(|v| v == "2") {
        crate::handlers::object::list_objects_v2(state, &bucket, &query).await
    } else {
        // Default: ListObjectsV1 (no list-type parameter).
        crate::handlers::object::list_objects_v1(state, &bucket, &query).await
    }
}

/// `PUT /:bucket` -- dispatches based on query params:
/// - `?acl` -> PutBucketAcl
/// - default -> CreateBucket
async fn handle_put_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, S3Error> {
    let query = parse_query(raw_query);

    if query.contains_key("acl") {
        crate::handlers::bucket::put_bucket_acl(state, &bucket, &headers, &body).await
    } else {
        crate::handlers::bucket::create_bucket(state, &bucket, &headers, &body).await
    }
}

/// `DELETE /:bucket` -- DeleteBucket
async fn handle_delete_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
) -> Result<Response, S3Error> {
    crate::handlers::bucket::delete_bucket(state, &bucket).await
}

/// `HEAD /:bucket` -- HeadBucket
async fn handle_head_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
) -> Result<Response, S3Error> {
    crate::handlers::bucket::head_bucket(state, &bucket).await
}

/// `POST /:bucket` -- dispatches based on query params:
/// - `?delete` -> DeleteObjects (batch delete)
/// - default -> NotImplemented
async fn handle_post_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    RawQuery(raw_query): RawQuery,
    body: axum::body::Bytes,
) -> Result<Response, S3Error> {
    let query = parse_query(raw_query);

    if query.contains_key("delete") {
        crate::handlers::object::delete_objects(state, &bucket, &body).await
    } else {
        Err(S3Error::NotImplemented)
    }
}

// -- Object-level dispatch ---------------------------------------------------

/// `GET /:bucket/*key` -- dispatches based on query params:
/// - `?acl` -> GetObjectAcl
/// - `?uploadId=...` -> ListParts
/// - default -> GetObject
async fn handle_get_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    let query = parse_query(raw_query);

    if query.contains_key("acl") {
        crate::handlers::object::get_object_acl(state, &bucket, &key).await
    } else if query.contains_key("uploadId") {
        crate::handlers::multipart::list_parts(state, &bucket, &key, &query).await
    } else {
        crate::handlers::object::get_object(state, &bucket, &key, &headers).await
    }
}

/// `PUT /:bucket/*key` -- dispatches based on query params and headers:
/// - `?acl` -> PutObjectAcl
/// - `?partNumber=...&uploadId=...` -> UploadPart
/// - `x-amz-copy-source` header -> CopyObject (or UploadPartCopy)
/// - default -> PutObject
async fn handle_put_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, S3Error> {
    let query = parse_query(raw_query);

    if query.contains_key("acl") {
        crate::handlers::object::put_object_acl(state, &bucket, &key, &headers, &body).await
    } else if query.contains_key("partNumber") && query.contains_key("uploadId") {
        // Check for UploadPartCopy (copy-source header + part params)
        if headers.contains_key("x-amz-copy-source") {
            crate::handlers::multipart::upload_part_copy(state, &bucket, &key, &query, &headers)
                .await
        } else {
            crate::handlers::multipart::upload_part(state, &bucket, &key, &query, &body).await
        }
    } else if headers.contains_key("x-amz-copy-source") {
        crate::handlers::object::copy_object(state, &bucket, &key, &headers).await
    } else {
        crate::handlers::object::put_object(state, &bucket, &key, &headers, &body).await
    }
}

/// `DELETE /:bucket/*key` -- dispatches based on query params:
/// - `?uploadId=...` -> AbortMultipartUpload
/// - default -> DeleteObject
async fn handle_delete_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    RawQuery(raw_query): RawQuery,
) -> Result<Response, S3Error> {
    let query = parse_query(raw_query);

    if query.contains_key("uploadId") {
        crate::handlers::multipart::abort_multipart_upload(state, &bucket, &key, &query).await
    } else {
        crate::handlers::object::delete_object(state, &bucket, &key).await
    }
}

/// `HEAD /:bucket/*key` -- HeadObject
async fn handle_head_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    crate::handlers::object::head_object(state, &bucket, &key, &headers).await
}

/// `POST /:bucket/*key` -- dispatches based on query params:
/// - `?uploads` -> CreateMultipartUpload
/// - `?uploadId=...` -> CompleteMultipartUpload
/// - default -> NotImplemented
async fn handle_post_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, S3Error> {
    let query = parse_query(raw_query);

    if query.contains_key("uploads") {
        crate::handlers::multipart::create_multipart_upload(state, &bucket, &key, &headers).await
    } else if query.contains_key("uploadId") {
        crate::handlers::multipart::complete_multipart_upload(state, &bucket, &key, &query, &body)
            .await
    } else {
        Err(S3Error::NotImplemented)
    }
}
