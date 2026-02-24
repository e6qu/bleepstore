//! Multipart-upload S3 API handlers.

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::errors::S3Error;
use crate::metadata::store::{
    Acl, AclGrant, AclGrantee, AclOwner, MultipartUploadRecord, PartRecord,
};
use crate::AppState;

// -- Helper functions ---------------------------------------------------------

/// Get current time as ISO-8601 string.
fn now_iso8601() -> String {
    let now = std::time::SystemTime::now();
    let since_epoch = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = since_epoch.as_secs();
    let millis = since_epoch.subsec_millis();

    let days = secs / 86400;
    let day_secs = secs % 86400;
    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let seconds = day_secs % 60;

    let (year, month, day) = days_to_ymd(days);

    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z")
}

/// Convert days since Unix epoch to (year, month, day).
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

/// Extract user metadata from request headers.
fn extract_user_metadata(headers: &HeaderMap) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    for (name, value) in headers.iter() {
        let name_str = name.as_str().to_lowercase();
        if name_str.starts_with("x-amz-meta-") {
            if let Ok(val) = value.to_str() {
                meta.insert(name_str, val.to_string());
            }
        }
    }
    meta
}

/// Extract Content-Type from headers, defaulting to application/octet-stream.
fn extract_content_type(headers: &HeaderMap) -> String {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string()
}

/// Build a default FULL_CONTROL ACL JSON for the given owner.
fn default_acl_json(owner_id: &str, display_name: &str) -> String {
    let acl = Acl::full_control(owner_id, display_name);
    serde_json::to_string(&acl).unwrap_or_else(|_| "{}".to_string())
}

/// Build ACL JSON from a canned ACL header value.
fn canned_acl_to_json(canned: &str, owner_id: &str, display_name: &str) -> Result<String, S3Error> {
    let mut grants = vec![AclGrant {
        grantee: AclGrantee::CanonicalUser {
            id: owner_id.to_string(),
            display_name: display_name.to_string(),
        },
        permission: "FULL_CONTROL".to_string(),
    }];

    match canned {
        "private" => {}
        "public-read" => {
            grants.push(AclGrant {
                grantee: AclGrantee::Group {
                    uri: "http://acs.amazonaws.com/groups/global/AllUsers".to_string(),
                },
                permission: "READ".to_string(),
            });
        }
        "public-read-write" => {
            grants.push(AclGrant {
                grantee: AclGrantee::Group {
                    uri: "http://acs.amazonaws.com/groups/global/AllUsers".to_string(),
                },
                permission: "READ".to_string(),
            });
            grants.push(AclGrant {
                grantee: AclGrantee::Group {
                    uri: "http://acs.amazonaws.com/groups/global/AllUsers".to_string(),
                },
                permission: "WRITE".to_string(),
            });
        }
        "authenticated-read" => {
            grants.push(AclGrant {
                grantee: AclGrantee::Group {
                    uri: "http://acs.amazonaws.com/groups/global/AuthenticatedUsers".to_string(),
                },
                permission: "READ".to_string(),
            });
        }
        _ => {
            return Err(S3Error::InvalidArgument {
                message: format!("Invalid canned ACL: {canned}"),
            });
        }
    }

    let acl = Acl {
        owner: AclOwner {
            id: owner_id.to_string(),
            display_name: display_name.to_string(),
        },
        grants,
    };

    Ok(serde_json::to_string(&acl).unwrap_or_else(|_| "{}".to_string()))
}

// -- Handlers -----------------------------------------------------------------

/// `POST /{bucket}/{key}?uploads` -- Initiate a multipart upload.
#[utoipa::path(
    post,
    path = "/{bucket}/{key}?uploads",
    tag = "Multipart",
    operation_id = "CreateMultipartUpload",
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Object key"),
    ),
    responses(
        (status = 200, description = "Multipart upload initiated"),
        (status = 404, description = "Bucket not found")
    )
)]
pub async fn create_multipart_upload(
    state: Arc<AppState>,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    // Check bucket exists.
    if !state.metadata.bucket_exists(bucket).await? {
        return Err(S3Error::NoSuchBucket {
            bucket: bucket.to_string(),
        });
    }

    // Generate a unique upload ID.
    let upload_id = uuid::Uuid::new_v4().to_string();

    // Extract metadata from headers.
    let content_type = extract_content_type(headers);
    let user_metadata = extract_user_metadata(headers);
    let content_encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let content_language = headers
        .get("content-language")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let content_disposition = headers
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let cache_control = headers
        .get("cache-control")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let expires = headers
        .get("expires")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Determine ACL from x-amz-acl header.
    let owner_id = state.config.auth.access_key.clone();
    let owner_display = state.config.auth.access_key.clone();
    let acl_json = if let Some(canned) = headers.get("x-amz-acl") {
        let canned_str = canned.to_str().unwrap_or("private");
        canned_acl_to_json(canned_str, &owner_id, &owner_display)?
    } else {
        default_acl_json(&owner_id, &owner_display)
    };

    let now = now_iso8601();

    // Record the multipart upload in metadata.
    let record = MultipartUploadRecord {
        upload_id: upload_id.clone(),
        bucket: bucket.to_string(),
        key: key.to_string(),
        content_type,
        content_encoding,
        content_language,
        content_disposition,
        cache_control,
        expires,
        storage_class: "STANDARD".to_string(),
        acl: acl_json,
        user_metadata,
        owner_id,
        owner_display,
        initiated_at: now,
    };

    state.metadata.create_multipart_upload(record).await?;

    // Return InitiateMultipartUploadResult XML.
    let xml = crate::xml::render_initiate_multipart_upload_result(bucket, key, &upload_id);

    Ok((
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        )],
        xml,
    )
        .into_response())
}

/// `PUT /{bucket}/{key}?partNumber={n}&uploadId={id}` -- Upload a single part.
#[utoipa::path(
    put,
    path = "/{bucket}/{key}?partNumber&uploadId",
    tag = "Multipart",
    operation_id = "UploadPart",
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Object key"),
        ("partNumber" = i32, Query, description = "Part number"),
        ("uploadId" = String, Query, description = "Upload ID"),
    ),
    responses(
        (status = 200, description = "Part uploaded"),
        (status = 404, description = "Upload not found")
    )
)]
pub async fn upload_part(
    state: Arc<AppState>,
    bucket: &str,
    key: &str,
    query: &HashMap<String, String>,
    body: &[u8],
) -> Result<Response, S3Error> {
    // Extract upload ID and part number from query params.
    let upload_id = query
        .get("uploadId")
        .ok_or_else(|| S3Error::InvalidArgument {
            message: "Missing uploadId parameter".to_string(),
        })?;

    let part_number: u32 = query
        .get("partNumber")
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| S3Error::InvalidArgument {
            message: "Missing or invalid partNumber parameter".to_string(),
        })?;

    // Validate part number range (1-10000).
    if !(1..=10000).contains(&part_number) {
        return Err(S3Error::InvalidArgument {
            message: format!("Part number must be between 1 and 10000, got {part_number}"),
        });
    }

    // Check that the multipart upload exists.
    let upload = state
        .metadata
        .get_multipart_upload(upload_id)
        .await?
        .ok_or_else(|| S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        })?;

    // Verify the bucket and key match.
    if upload.bucket != bucket || upload.key != key {
        return Err(S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        });
    }

    let data = bytes::Bytes::copy_from_slice(body);
    let size = data.len() as u64;

    // Write part to storage backend (crash-only: temp-fsync-rename).
    let etag = state
        .storage
        .put_part(bucket, upload_id, part_number, data)
        .await?;

    let now = now_iso8601();

    // Record part in metadata.
    let part_record = PartRecord {
        part_number,
        size,
        etag: etag.clone(),
        last_modified: now,
    };

    state.metadata.put_part(upload_id, part_record).await?;

    // Return 200 with ETag header.
    let mut response = StatusCode::OK.into_response();
    response
        .headers_mut()
        .insert("etag", HeaderValue::from_str(&etag).unwrap());
    Ok(response)
}

/// `PUT /{bucket}/{key}?partNumber={n}&uploadId={id}` with `x-amz-copy-source` -- Copy a part from an existing object.
pub async fn upload_part_copy(
    state: Arc<AppState>,
    bucket: &str,
    key: &str,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    // Extract upload ID and part number from query params.
    let upload_id = query
        .get("uploadId")
        .ok_or_else(|| S3Error::InvalidArgument {
            message: "Missing uploadId parameter".to_string(),
        })?;

    let part_number: u32 = query
        .get("partNumber")
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| S3Error::InvalidArgument {
            message: "Missing or invalid partNumber parameter".to_string(),
        })?;

    if !(1..=10000).contains(&part_number) {
        return Err(S3Error::InvalidArgument {
            message: format!("Part number must be between 1 and 10000, got {part_number}"),
        });
    }

    // Check that the multipart upload exists.
    let upload = state
        .metadata
        .get_multipart_upload(upload_id)
        .await?
        .ok_or_else(|| S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        })?;

    if upload.bucket != bucket || upload.key != key {
        return Err(S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        });
    }
    // Parse x-amz-copy-source header.
    let copy_source = headers
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| S3Error::InvalidArgument {
            message: "Missing x-amz-copy-source header".to_string(),
        })?;

    let decoded_source = percent_encoding::percent_decode_str(copy_source).decode_utf8_lossy();
    let source_path = decoded_source.trim_start_matches('/');

    let (src_bucket, src_key) =
        source_path
            .split_once('/')
            .ok_or_else(|| S3Error::InvalidArgument {
                message: format!("Invalid x-amz-copy-source: {copy_source}"),
            })?;

    // Check source bucket exists.
    if !state.metadata.bucket_exists(src_bucket).await? {
        return Err(S3Error::NoSuchBucket {
            bucket: src_bucket.to_string(),
        });
    }

    // Check source object exists.
    let _src_record = state
        .metadata
        .get_object(src_bucket, src_key)
        .await?
        .ok_or_else(|| S3Error::NoSuchKey {
            key: src_key.to_string(),
        })?;

    // Read source object data from storage.
    let src_storage_key = format!("{src_bucket}/{src_key}");
    let stored = state.storage.get(&src_storage_key).await?;
    let full_data = stored.data;

    // Handle optional x-amz-copy-source-range header.
    let part_data = if let Some(range_str) = headers
        .get("x-amz-copy-source-range")
        .and_then(|v| v.to_str().ok())
    {
        let range_str = range_str.trim();
        if let Some(spec) = range_str.strip_prefix("bytes=") {
            if let Some((start_s, end_s)) = spec.split_once('-') {
                let start: usize = start_s.parse().map_err(|_| S3Error::InvalidArgument {
                    message: "Invalid copy source range".to_string(),
                })?;
                let end: usize = end_s.parse().map_err(|_| S3Error::InvalidArgument {
                    message: "Invalid copy source range".to_string(),
                })?;
                if start > end || end >= full_data.len() {
                    return Err(S3Error::InvalidRange);
                }
                full_data.slice(start..=end)
            } else {
                return Err(S3Error::InvalidArgument {
                    message: "Invalid copy source range format".to_string(),
                });
            }
        } else {
            return Err(S3Error::InvalidArgument {
                message: "Invalid copy source range format".to_string(),
            });
        }
    } else {
        full_data
    };

    let size = part_data.len() as u64;

    // Write part data to storage.
    let etag = state
        .storage
        .put_part(bucket, upload_id, part_number, part_data)
        .await?;

    let now = now_iso8601();

    // Record part in metadata.
    let part_record = PartRecord {
        part_number,
        size,
        etag: etag.clone(),
        last_modified: now.clone(),
    };
    state.metadata.put_part(upload_id, part_record).await?;

    // Return XML response with CopyPartResult.
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CopyPartResult><ETag>{etag}</ETag><LastModified>{now}</LastModified></CopyPartResult>"
    );

    Ok((
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        )],
        xml,
    )
        .into_response())
}
/// Minimum part size (5 MiB) for all parts except the last.
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Parse the `<CompleteMultipartUpload>` XML body to extract `(PartNumber, ETag)` pairs.
fn parse_complete_multipart_upload_xml(body: &[u8]) -> Result<Vec<(u32, String)>, S3Error> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_reader(body);
    reader.trim_text(true);

    let mut parts: Vec<(u32, String)> = Vec::new();
    let mut current_part_number: Option<u32> = None;
    let mut current_etag: Option<String> = None;
    let mut in_part = false;
    let mut current_tag = String::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                match tag_name.as_str() {
                    "Part" => {
                        in_part = true;
                        current_part_number = None;
                        current_etag = None;
                    }
                    _ => {
                        if in_part {
                            current_tag = tag_name;
                        }
                    }
                }
            }
            Ok(Event::Text(ref e)) => {
                if in_part {
                    let text = e.unescape().map_err(|_| S3Error::MalformedXML)?.to_string();
                    match current_tag.as_str() {
                        "PartNumber" => {
                            current_part_number = text.parse::<u32>().ok();
                        }
                        "ETag" => {
                            current_etag = Some(text);
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if tag_name == "Part" {
                    in_part = false;
                    match (current_part_number, current_etag.take()) {
                        (Some(pn), Some(etag)) => {
                            parts.push((pn, etag));
                        }
                        _ => {
                            return Err(S3Error::MalformedXML);
                        }
                    }
                    current_tag.clear();
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(S3Error::MalformedXML),
            _ => {}
        }
        buf.clear();
    }

    if parts.is_empty() {
        return Err(S3Error::MalformedXML);
    }

    Ok(parts)
}

/// `POST /{bucket}/{key}?uploadId={id}` -- Complete a multipart upload.
#[utoipa::path(
    post,
    path = "/{bucket}/{key}?uploadId",
    tag = "Multipart",
    operation_id = "CompleteMultipartUpload",
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Object key"),
        ("uploadId" = String, Query, description = "Upload ID"),
    ),
    responses(
        (status = 200, description = "Multipart upload completed"),
        (status = 400, description = "Invalid part or malformed XML"),
        (status = 404, description = "Upload not found")
    )
)]
pub async fn complete_multipart_upload(
    state: Arc<AppState>,
    bucket: &str,
    key: &str,
    query: &HashMap<String, String>,
    body: &[u8],
) -> Result<Response, S3Error> {
    // Extract upload ID from query params.
    let upload_id = query
        .get("uploadId")
        .ok_or_else(|| S3Error::InvalidArgument {
            message: "Missing uploadId parameter".to_string(),
        })?;

    // Check that the multipart upload exists.
    let upload = state
        .metadata
        .get_multipart_upload(upload_id)
        .await?
        .ok_or_else(|| S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        })?;

    // Verify the bucket and key match.
    if upload.bucket != bucket || upload.key != key {
        return Err(S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        });
    }

    // Parse the CompleteMultipartUpload XML body.
    let requested_parts = parse_complete_multipart_upload_xml(body)?;

    // Validate ascending part order.
    for i in 1..requested_parts.len() {
        if requested_parts[i].0 <= requested_parts[i - 1].0 {
            return Err(S3Error::InvalidPartOrder);
        }
    }

    // Get all stored parts from metadata for validation.
    let stored_parts = state.metadata.get_parts_for_completion(upload_id).await?;

    // Build a map of stored parts for quick lookup: part_number -> PartRecord.
    let stored_map: HashMap<u32, &crate::metadata::store::PartRecord> =
        stored_parts.iter().map(|p| (p.part_number, p)).collect();

    // Validate each requested part: must exist and ETag must match.
    let mut validated_parts: Vec<(u32, String)> = Vec::new();
    let mut total_size: u64 = 0;

    for (i, (part_number, requested_etag)) in requested_parts.iter().enumerate() {
        let stored = stored_map.get(part_number).ok_or_else(|| S3Error::InvalidPart {
            message: "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.".to_string(),
        })?;

        // Normalize ETags for comparison: strip quotes if present.
        let norm_requested = requested_etag.trim_matches('"');
        let norm_stored = stored.etag.trim_matches('"');

        if norm_requested != norm_stored {
            return Err(S3Error::InvalidPart {
                message: "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.".to_string(),
            });
        }

        // Validate minimum part size (5 MiB) for all parts except the last.
        let is_last = i == requested_parts.len() - 1;
        if !is_last && stored.size < MIN_PART_SIZE {
            return Err(S3Error::EntityTooSmall);
        }

        total_size += stored.size;
        validated_parts.push((*part_number, stored.etag.clone()));
    }

    // Assemble parts into final object via storage backend.
    let composite_etag = state
        .storage
        .assemble_parts(bucket, key, upload_id, &validated_parts)
        .await?;

    let now = now_iso8601();

    // Build the final object record from the upload's metadata.
    let final_object = crate::metadata::store::ObjectRecord {
        bucket: bucket.to_string(),
        key: key.to_string(),
        size: total_size,
        etag: composite_etag.clone(),
        content_type: upload.content_type,
        content_encoding: upload.content_encoding,
        content_language: upload.content_language,
        content_disposition: upload.content_disposition,
        cache_control: upload.cache_control,
        expires: upload.expires,
        storage_class: upload.storage_class,
        acl: upload.acl,
        last_modified: now,
        user_metadata: upload.user_metadata,
        delete_marker: false,
    };

    // Complete in metadata store (transactional: insert object, delete upload + parts).
    state
        .metadata
        .complete_multipart_upload(upload_id, final_object)
        .await?;

    // Clean up part files from storage (best-effort).
    let _ = state.storage.delete_parts(bucket, upload_id).await;

    // Build the Location URL.
    let location = format!("/{bucket}/{key}");

    // Return CompleteMultipartUploadResult XML.
    let xml = crate::xml::render_complete_multipart_upload_result(
        &location,
        bucket,
        key,
        &composite_etag,
    );

    Ok((
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        )],
        xml,
    )
        .into_response())
}

/// `DELETE /{bucket}/{key}?uploadId={id}` -- Abort a multipart upload.
#[utoipa::path(
    delete,
    path = "/{bucket}/{key}?uploadId",
    tag = "Multipart",
    operation_id = "AbortMultipartUpload",
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Object key"),
        ("uploadId" = String, Query, description = "Upload ID"),
    ),
    responses(
        (status = 204, description = "Multipart upload aborted"),
        (status = 404, description = "Upload not found")
    )
)]
pub async fn abort_multipart_upload(
    state: Arc<AppState>,
    bucket: &str,
    key: &str,
    query: &HashMap<String, String>,
) -> Result<Response, S3Error> {
    // Extract upload ID from query params.
    let upload_id = query
        .get("uploadId")
        .ok_or_else(|| S3Error::InvalidArgument {
            message: "Missing uploadId parameter".to_string(),
        })?;

    // Check that the multipart upload exists.
    let upload = state
        .metadata
        .get_multipart_upload(upload_id)
        .await?
        .ok_or_else(|| S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        })?;

    // Verify the bucket and key match.
    if upload.bucket != bucket || upload.key != key {
        return Err(S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        });
    }

    // Delete parts from storage (best-effort).
    let _ = state.storage.delete_parts(bucket, upload_id).await;

    // Delete upload + parts from metadata (cascade delete).
    state.metadata.delete_multipart_upload(upload_id).await?;

    // Return 204 No Content.
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `GET /{bucket}?uploads` -- List in-progress multipart uploads.
#[utoipa::path(
    get,
    path = "/{bucket}?uploads",
    tag = "Multipart",
    operation_id = "ListMultipartUploads",
    params(("bucket" = String, Path, description = "Bucket name")),
    responses(
        (status = 200, description = "Multipart uploads list"),
        (status = 404, description = "Bucket not found")
    )
)]
pub async fn list_multipart_uploads(
    state: Arc<AppState>,
    bucket: &str,
    query: &HashMap<String, String>,
) -> Result<Response, S3Error> {
    // Check bucket exists.
    if !state.metadata.bucket_exists(bucket).await? {
        return Err(S3Error::NoSuchBucket {
            bucket: bucket.to_string(),
        });
    }

    // Extract query parameters.
    let prefix = query.get("prefix").map(|s| s.as_str()).unwrap_or("");
    let max_uploads: u32 = query
        .get("max-uploads")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let key_marker = query.get("key-marker").map(|s| s.as_str()).unwrap_or("");
    let upload_id_marker = query
        .get("upload-id-marker")
        .map(|s| s.as_str())
        .unwrap_or("");

    // Query metadata store.
    let result = state
        .metadata
        .list_multipart_uploads(bucket, prefix, max_uploads, key_marker, upload_id_marker)
        .await?;

    // Build UploadEntry list for XML rendering.
    let entries: Vec<crate::xml::UploadEntry<'_>> = result
        .uploads
        .iter()
        .map(|u| crate::xml::UploadEntry {
            key: &u.key,
            upload_id: &u.upload_id,
            initiated: &u.initiated_at,
            storage_class: &u.storage_class,
            owner_id: &u.owner_id,
            owner_display: &u.owner_display,
        })
        .collect();

    let xml = crate::xml::render_list_multipart_uploads_result(
        bucket,
        key_marker,
        upload_id_marker,
        max_uploads,
        result.is_truncated,
        &entries,
        result.next_key_marker.as_deref(),
        result.next_upload_id_marker.as_deref(),
        prefix,
    );

    Ok((
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        )],
        xml,
    )
        .into_response())
}

/// `GET /{bucket}/{key}?uploadId={id}` -- List parts of an in-progress upload.
#[utoipa::path(
    get,
    path = "/{bucket}/{key}?uploadId",
    tag = "Multipart",
    operation_id = "ListParts",
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Object key"),
        ("uploadId" = String, Query, description = "Upload ID"),
    ),
    responses(
        (status = 200, description = "Parts list"),
        (status = 404, description = "Upload not found")
    )
)]
pub async fn list_parts(
    state: Arc<AppState>,
    bucket: &str,
    key: &str,
    query: &HashMap<String, String>,
) -> Result<Response, S3Error> {
    // Extract upload ID from query params.
    let upload_id = query
        .get("uploadId")
        .ok_or_else(|| S3Error::InvalidArgument {
            message: "Missing uploadId parameter".to_string(),
        })?;

    // Check that the multipart upload exists.
    let upload = state
        .metadata
        .get_multipart_upload(upload_id)
        .await?
        .ok_or_else(|| S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        })?;

    // Verify the bucket and key match.
    if upload.bucket != bucket || upload.key != key {
        return Err(S3Error::NoSuchUpload {
            upload_id: upload_id.to_string(),
        });
    }

    // Extract query parameters.
    let max_parts: u32 = query
        .get("max-parts")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let part_number_marker: u32 = query
        .get("part-number-marker")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Query metadata store.
    let result = state
        .metadata
        .list_parts(upload_id, max_parts, part_number_marker)
        .await?;

    // Build PartEntry list for XML rendering.
    let entries: Vec<crate::xml::PartEntry<'_>> = result
        .parts
        .iter()
        .map(|p| crate::xml::PartEntry {
            part_number: p.part_number,
            last_modified: &p.last_modified,
            etag: &p.etag,
            size: p.size,
        })
        .collect();

    let xml = crate::xml::render_list_parts_result(
        bucket,
        key,
        upload_id,
        part_number_marker,
        max_parts,
        result.is_truncated,
        &entries,
        result.next_part_number_marker,
        &upload.storage_class,
        &upload.owner_id,
        &upload.owner_display,
    );

    Ok((
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        )],
        xml,
    )
        .into_response())
}

// -- Unit tests ---------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_iso8601_format() {
        let now = now_iso8601();
        assert!(now.contains("T"));
        assert!(now.ends_with("Z"));
        assert!(now.contains("."));
    }

    #[test]
    fn test_extract_user_metadata_empty() {
        let headers = HeaderMap::new();
        let meta = extract_user_metadata(&headers);
        assert!(meta.is_empty());
    }

    #[test]
    fn test_extract_user_metadata_with_entries() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-author", HeaderValue::from_static("tester"));
        headers.insert("x-amz-meta-version", HeaderValue::from_static("1.0"));
        headers.insert("content-type", HeaderValue::from_static("text/plain"));

        let meta = extract_user_metadata(&headers);
        assert_eq!(meta.len(), 2);
        assert_eq!(meta.get("x-amz-meta-author").unwrap(), "tester");
        assert_eq!(meta.get("x-amz-meta-version").unwrap(), "1.0");
    }

    #[test]
    fn test_extract_content_type_default() {
        let headers = HeaderMap::new();
        assert_eq!(extract_content_type(&headers), "application/octet-stream");
    }

    #[test]
    fn test_extract_content_type_custom() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("image/png"));
        assert_eq!(extract_content_type(&headers), "image/png");
    }

    #[test]
    fn test_default_acl_json() {
        let json = default_acl_json("owner1", "Owner One");
        assert!(json.contains("FULL_CONTROL"));
        assert!(json.contains("owner1"));
    }

    #[test]
    fn test_canned_acl_private() {
        let json = canned_acl_to_json("private", "owner1", "Owner One").unwrap();
        let acl: Acl = serde_json::from_str(&json).unwrap();
        assert_eq!(acl.grants.len(), 1);
        assert_eq!(acl.grants[0].permission, "FULL_CONTROL");
    }

    #[test]
    fn test_canned_acl_public_read() {
        let json = canned_acl_to_json("public-read", "owner1", "Owner One").unwrap();
        let acl: Acl = serde_json::from_str(&json).unwrap();
        assert_eq!(acl.grants.len(), 2);
    }

    #[test]
    fn test_canned_acl_invalid() {
        let result = canned_acl_to_json("invalid-acl", "owner1", "Owner One");
        assert!(result.is_err());
    }

    // -- CompleteMultipartUpload XML parsing tests ----------------------------

    #[test]
    fn test_parse_complete_multipart_upload_xml_valid() {
        let xml = br#"
            <CompleteMultipartUpload>
                <Part>
                    <PartNumber>1</PartNumber>
                    <ETag>"a54357aff0632cce46d942af68356b38"</ETag>
                </Part>
                <Part>
                    <PartNumber>2</PartNumber>
                    <ETag>"0c78aef83f66abc1fa1e8477f296d394"</ETag>
                </Part>
            </CompleteMultipartUpload>
        "#;

        let parts = parse_complete_multipart_upload_xml(xml).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].0, 1);
        assert_eq!(parts[0].1, "\"a54357aff0632cce46d942af68356b38\"");
        assert_eq!(parts[1].0, 2);
        assert_eq!(parts[1].1, "\"0c78aef83f66abc1fa1e8477f296d394\"");
    }

    #[test]
    fn test_parse_complete_multipart_upload_xml_single_part() {
        let xml = br#"
            <CompleteMultipartUpload>
                <Part>
                    <PartNumber>1</PartNumber>
                    <ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
                </Part>
            </CompleteMultipartUpload>
        "#;

        let parts = parse_complete_multipart_upload_xml(xml).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].0, 1);
    }

    #[test]
    fn test_parse_complete_multipart_upload_xml_empty_body() {
        let xml = br#"<CompleteMultipartUpload></CompleteMultipartUpload>"#;
        let result = parse_complete_multipart_upload_xml(xml);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_complete_multipart_upload_xml_malformed() {
        let xml = b"not xml at all";
        let result = parse_complete_multipart_upload_xml(xml);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_complete_multipart_upload_xml_missing_etag() {
        let xml = br#"
            <CompleteMultipartUpload>
                <Part>
                    <PartNumber>1</PartNumber>
                </Part>
            </CompleteMultipartUpload>
        "#;

        let result = parse_complete_multipart_upload_xml(xml);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_complete_multipart_upload_xml_missing_part_number() {
        let xml = br#"
            <CompleteMultipartUpload>
                <Part>
                    <ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
                </Part>
            </CompleteMultipartUpload>
        "#;

        let result = parse_complete_multipart_upload_xml(xml);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_complete_multipart_upload_xml_unquoted_etag() {
        // Some clients may send ETags without quotes
        let xml = br#"
            <CompleteMultipartUpload>
                <Part>
                    <PartNumber>1</PartNumber>
                    <ETag>a54357aff0632cce46d942af68356b38</ETag>
                </Part>
            </CompleteMultipartUpload>
        "#;

        let parts = parse_complete_multipart_upload_xml(xml).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].1, "a54357aff0632cce46d942af68356b38");
    }
}
