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
    response::{Html, IntoResponse, Response},
    routing::{delete, get, head, post, put},
    Json, Router,
};
use sha2::Digest;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, warn};

use crate::auth;
use crate::errors::{generate_request_id, S3Error};
use crate::metrics::{metrics_handler, metrics_middleware};
use crate::AppState;

// -- Canonical OpenAPI specification ------------------------------------------

/// Canonical OpenAPI spec embedded from `schemas/s3-api.openapi.json`.
const CANONICAL_SPEC: &str = include_str!("../../schemas/s3-api.openapi.json");

/// Patch the canonical OpenAPI spec's `servers` array with the actual port.
fn patch_openapi_spec(port: u16) -> String {
    let mut spec: serde_json::Value =
        serde_json::from_str(CANONICAL_SPEC).expect("invalid canonical OpenAPI JSON");
    spec["servers"] = serde_json::json!([
        {
            "url": format!("http://localhost:{}", port),
            "description": "BleepStore Rust"
        }
    ]);
    serde_json::to_string(&spec).expect("failed to serialize patched OpenAPI spec")
}

/// Swagger UI HTML page that loads the spec from `/openapi.json`.
const SWAGGER_UI_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>BleepStore API - Swagger UI</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({ url: '/openapi.json', dom_id: '#swagger-ui',
      presets: [SwaggerUIBundle.presets.apis], layout: 'BaseLayout' });
  </script>
</body>
</html>"#;

/// Build the axum [`Router`] with all S3-compatible routes.
///
/// The returned router is ready to be passed to `axum::serve`.
/// Routes are conditionally registered based on `config.observability`.
pub fn app(state: Arc<AppState>) -> Router {
    // Patch the canonical spec with the configured port, then leak it so we
    // can hand out a `&'static str` to the handler without cloning per request.
    let port = state.config.server.port;
    let spec_string = patch_openapi_spec(port);
    let spec_static: &'static str = Box::leak(spec_string.into_boxed_str());

    let metrics_enabled = state.config.observability.metrics;
    let health_check_enabled = state.config.observability.health_check;

    // Phase 1: build the stateful router (Router<Arc<AppState>>).
    let mut stateful = Router::new()
        // Health check endpoint (always served, but depth depends on config).
        .route("/health", get(health_check));

    // Prometheus metrics endpoint (conditional).
    if metrics_enabled {
        stateful = stateful.route("/metrics", get(metrics_handler));
    }

    // Kubernetes-style health probes (conditional).
    if health_check_enabled {
        stateful = stateful
            .route("/healthz", get(healthz_handler))
            .route("/readyz", get(readyz_handler));
    }

    stateful = stateful
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
        // OpenAPI spec and Swagger UI (served from canonical spec)
        .route(
            "/openapi.json",
            get(move || async move {
                (
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    spec_static,
                )
            }),
        )
        .route("/docs", get(|| async { Html(SWAGGER_UI_HTML) }));

    // Phase 2: apply state and layers (converts to Router<()>).
    let mut router = stateful
        .with_state(state.clone())
        // Layer ordering: inner layers run first, outer layers wrap them.
        // auth_middleware is innermost (closest to handlers, after routing).
        .layer(middleware::from_fn_with_state(state, auth_middleware))
        // common_headers_middleware is next (adds standard S3 headers).
        .layer(middleware::from_fn(common_headers_middleware));

    // metrics_middleware is outer (captures full request lifecycle) -- conditional.
    if metrics_enabled {
        router = router.layer(middleware::from_fn(metrics_middleware));
    }

    // Disable the default 2MB body size limit (S3 objects can be large).
    router.layer(DefaultBodyLimit::disable())
}

// -- Common headers middleware -----------------------------------------------

/// Tower middleware that adds common S3 response headers to every response:
/// - `x-amz-request-id`: 16-character uppercase hex string
/// - `x-amz-id-2`: Base64-encoded 24-byte random value (extended request ID)
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

    // Generate x-amz-id-2: Base64-encoded 24 random bytes.
    if !headers.contains_key("x-amz-id-2") {
        let random_bytes: [u8; 24] = rand::random();
        let id2 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, random_bytes);
        headers.insert("x-amz-id-2", HeaderValue::from_str(&id2).unwrap());
    }

    let date = httpdate::fmt_http_date(std::time::SystemTime::now());
    // Always overwrite Date and Server to ensure consistency
    headers.insert("date", HeaderValue::from_str(&date).unwrap());
    headers.insert("server", HeaderValue::from_static("BleepStore"));

    response
}

// -- Auth middleware ---------------------------------------------------------

/// Paths that bypass authentication.
const AUTH_SKIP_PATHS: &[&str] = &[
    "/health",
    "/healthz",
    "/readyz",
    "/metrics",
    "/docs",
    "/openapi.json",
];

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
            let credential =
                if let Some(cached) = state.auth_cache.get_credential(&parsed.access_key_id) {
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

            // Check clock skew using x-amz-date header (fail fast before signature computation).
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
                return Err(S3Error::RequestTimeTooSkewed);
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

            let string_to_sign =
                auth::build_string_to_sign(timestamp, &parsed.credential_scope, &canonical_request);

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
            let credential =
                if let Some(cached) = state.auth_cache.get_credential(&parsed.access_key_id) {
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

// -- Health check endpoints ---------------------------------------------------

/// `GET /health` -- Returns JSON health status with component checks.
///
/// When `observability.health_check` is enabled, performs deep checks on
/// metadata store and storage backend, returning latency information.
/// When disabled, returns a static `{"status":"ok"}` response.
/// Returns 503 with `"status":"degraded"` if any component check fails.
#[utoipa::path(
    get,
    path = "/health",
    tag = "Health",
    operation_id = "HealthCheck",
    responses(
        (status = 200, description = "Health check OK"),
        (status = 503, description = "Health check degraded")
    )
)]
async fn health_check(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if !state.config.observability.health_check {
        // Static response when deep health checks are disabled.
        return (StatusCode::OK, Json(serde_json::json!({"status": "ok"})));
    }

    // Deep check: probe metadata store.
    let meta_start = Instant::now();
    let meta_ok = state.metadata.list_buckets().await.is_ok();
    let meta_latency = meta_start.elapsed().as_millis() as u64;

    // Deep check: probe storage backend.
    let storage_start = Instant::now();
    let storage_ok = state.storage.exists("__health_probe__").await.is_ok();
    let storage_latency = storage_start.elapsed().as_millis() as u64;

    let all_ok = meta_ok && storage_ok;
    let status_str = if all_ok { "ok" } else { "degraded" };
    let http_status = if all_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let meta_check = if meta_ok {
        serde_json::json!({"status": "ok", "latency_ms": meta_latency})
    } else {
        serde_json::json!({"status": "error", "latency_ms": meta_latency})
    };

    let storage_check = if storage_ok {
        serde_json::json!({"status": "ok", "latency_ms": storage_latency})
    } else {
        serde_json::json!({"status": "error", "latency_ms": storage_latency})
    };

    let body = serde_json::json!({
        "status": status_str,
        "checks": {
            "metadata": meta_check,
            "storage": storage_check,
        }
    });

    (http_status, Json(body))
}

/// `GET /healthz` -- Kubernetes liveness probe.
///
/// Returns 200 with empty body. Confirms the process is running.
async fn healthz_handler() -> impl IntoResponse {
    StatusCode::OK
}

/// `GET /readyz` -- Kubernetes readiness probe.
///
/// Probes metadata store and storage backend. Returns 200 if all pass,
/// 503 if any fail. Empty body in both cases.
async fn readyz_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let meta_ok = state.metadata.list_buckets().await.is_ok();
    let storage_ok = state.storage.exists("__health_probe__").await.is_ok();

    if meta_ok && storage_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
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
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, S3Error> {
    let query = parse_query(raw_query);

    if query.contains_key("delete") {
        crate::handlers::object::delete_objects(state, &bucket, &headers, &body).await
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

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::metadata::sqlite::SqliteMetadataStore;
    use crate::storage::local::LocalBackend;
    use axum::body::Body;
    use http::Request as HttpRequest;
    use tower::ServiceExt;

    /// Create a test `AppState` with in-memory SQLite and a temp local storage.
    fn test_state(metrics: bool, health_check: bool) -> (Arc<AppState>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("failed to create temp dir");
        let storage_root = tmp.path().join("objects");
        std::fs::create_dir_all(&storage_root).expect("create objects dir");

        let metadata =
            SqliteMetadataStore::new(":memory:").expect("failed to create in-memory store");
        let storage =
            LocalBackend::new(storage_root.to_str().unwrap()).expect("failed to create backend");

        let mut config: Config = serde_yaml::from_str("{}").expect("failed to parse empty config");
        config.observability.metrics = metrics;
        config.observability.health_check = health_check;

        let state = Arc::new(AppState {
            config,
            metadata: Arc::new(metadata),
            storage: Arc::new(storage),
            auth_cache: crate::auth::AuthCache::new(),
        });

        (state, tmp)
    }

    // -- /healthz tests -------------------------------------------------------

    #[tokio::test]
    async fn test_healthz_returns_200_empty_body() {
        let (state, _tmp) = test_state(true, true);
        let router = app(state);

        let req = HttpRequest::builder()
            .uri("/healthz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn test_healthz_disabled_returns_404() {
        let (state, _tmp) = test_state(true, false);
        let router = app(state);

        let req = HttpRequest::builder()
            .uri("/healthz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // -- /readyz tests --------------------------------------------------------

    #[tokio::test]
    async fn test_readyz_returns_200_empty_body() {
        let (state, _tmp) = test_state(true, true);
        let router = app(state);

        let req = HttpRequest::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn test_readyz_disabled_returns_404() {
        let (state, _tmp) = test_state(true, false);
        let router = app(state);

        let req = HttpRequest::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // -- /health tests --------------------------------------------------------

    #[tokio::test]
    async fn test_health_returns_json_with_checks() {
        let (state, _tmp) = test_state(true, true);
        let router = app(state);

        let req = HttpRequest::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ok");
        assert!(json["checks"]["metadata"]["status"] == "ok");
        assert!(json["checks"]["storage"]["status"] == "ok");
        assert!(json["checks"]["metadata"]["latency_ms"].is_number());
        assert!(json["checks"]["storage"]["latency_ms"].is_number());
    }

    #[tokio::test]
    async fn test_health_disabled_returns_static_json() {
        let (state, _tmp) = test_state(true, false);
        let router = app(state);

        let req = HttpRequest::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ok");
        // Should not have checks when disabled.
        assert!(json.get("checks").is_none());
    }

    // -- /metrics tests -------------------------------------------------------

    #[tokio::test]
    async fn test_metrics_disabled_returns_404() {
        let (state, _tmp) = test_state(false, true);
        let router = app(state);

        let req = HttpRequest::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}

#[cfg(test)]
mod openapi_tests {
    use super::*;

    #[test]
    fn test_spec_matches_canonical() {
        let embedded: serde_json::Value =
            serde_json::from_str(CANONICAL_SPEC).expect("failed to parse embedded spec");

        let canonical_bytes =
            std::fs::read_to_string("../schemas/s3-api.openapi.json").expect("read canonical spec");
        let canonical: serde_json::Value =
            serde_json::from_str(&canonical_bytes).expect("failed to parse canonical spec");

        // Strip servers array for comparison (patched at runtime).
        let mut embedded_map = embedded.as_object().unwrap().clone();
        let mut canonical_map = canonical.as_object().unwrap().clone();
        embedded_map.remove("servers");
        canonical_map.remove("servers");

        assert_eq!(
            serde_json::Value::Object(embedded_map),
            serde_json::Value::Object(canonical_map),
            "Embedded OpenAPI spec does not match canonical schema"
        );
    }
}
