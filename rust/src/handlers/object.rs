//! Object-level S3 API handlers.

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::errors::S3Error;
use crate::metadata::store::{Acl, AclGrant, AclGrantee, AclOwner, ObjectRecord};
use crate::AppState;

// -- Range parsing ------------------------------------------------------------

/// Parsed byte range from a Range header.
#[derive(Debug, Clone, PartialEq)]
enum ByteRange {
    /// bytes=start-end (inclusive both ends)
    StartEnd(u64, u64),
    /// bytes=start-  (from start to end of file)
    StartOpen(u64),
    /// bytes=-N  (last N bytes)
    Suffix(u64),
}

/// Parse a Range header value like "bytes=0-4", "bytes=5-", "bytes=-3".
/// Returns None if the header is not a valid bytes range.
fn parse_range_header(range_str: &str) -> Option<ByteRange> {
    let range_str = range_str.trim();
    if !range_str.starts_with("bytes=") {
        return None;
    }
    let spec = &range_str[6..];

    // Only support a single range (no multi-range).
    if spec.contains(',') {
        return None;
    }

    if let Some(suffix) = spec.strip_prefix('-') {
        // bytes=-N (suffix range)
        let n: u64 = suffix.parse().ok()?;
        if n == 0 {
            return None;
        }
        Some(ByteRange::Suffix(n))
    } else if let Some(stripped) = spec.strip_suffix('-') {
        // bytes=N- (open-ended range)
        let start: u64 = stripped.parse().ok()?;
        Some(ByteRange::StartOpen(start))
    } else if let Some((start_s, end_s)) = spec.split_once('-') {
        // bytes=start-end
        let start: u64 = start_s.parse().ok()?;
        let end: u64 = end_s.parse().ok()?;
        if start > end {
            return None;
        }
        Some(ByteRange::StartEnd(start, end))
    } else {
        None
    }
}

/// Resolve a ByteRange against a total content length.
/// Returns (start, end) where both are inclusive, or None if unsatisfiable.
fn resolve_range(range: &ByteRange, total: u64) -> Option<(u64, u64)> {
    if total == 0 {
        return None;
    }
    match range {
        ByteRange::StartEnd(start, end) => {
            if *start >= total {
                return None;
            }
            let end = std::cmp::min(*end, total - 1);
            Some((*start, end))
        }
        ByteRange::StartOpen(start) => {
            if *start >= total {
                return None;
            }
            Some((*start, total - 1))
        }
        ByteRange::Suffix(n) => {
            if *n >= total {
                Some((0, total - 1))
            } else {
                Some((total - n, total - 1))
            }
        }
    }
}

// -- Conditional request evaluation -------------------------------------------

/// Strip surrounding double quotes from an ETag string for comparison.
fn strip_etag_quotes(etag: &str) -> &str {
    let etag = etag.trim();
    if etag.starts_with('"') && etag.ends_with('"') && etag.len() >= 2 {
        &etag[1..etag.len() - 1]
    } else {
        etag
    }
}

/// Check If-Match / If-None-Match / If-Modified-Since / If-Unmodified-Since
/// conditions against an object record.
///
/// Returns:
///   Ok(()) -- all conditions pass, proceed with the response
///   Err(S3Error::PreconditionFailed) -- If-Match or If-Unmodified-Since failed (412)
///   Err(S3Error) with status 304 -- If-None-Match or If-Modified-Since signals "not modified"
///
/// Per the S3/HTTP spec, the evaluation priority is:
///   1. If-Match (412 on failure)
///   2. If-Unmodified-Since (412 on failure) -- only evaluated if If-Match is absent
///   3. If-None-Match (304 for GET/HEAD on match)
///   4. If-Modified-Since (304 on no-change) -- only evaluated if If-None-Match is absent
fn evaluate_conditions(
    headers: &HeaderMap,
    record: &ObjectRecord,
    is_get_or_head: bool,
) -> Result<(), S3Error> {
    let record_etag_inner = strip_etag_quotes(&record.etag);

    // Parse Last-Modified from record for date comparisons.
    let last_modified_time = parse_iso8601_to_system_time(&record.last_modified);

    // 1. If-Match: ETag must match. If not, 412.
    if let Some(if_match) = headers.get("if-match").and_then(|v| v.to_str().ok()) {
        let if_match_inner = strip_etag_quotes(if_match);
        // Wildcard "*" always matches.
        if if_match_inner != "*" && if_match_inner != record_etag_inner {
            return Err(S3Error::PreconditionFailed);
        }
        // If If-Match passes, skip If-Unmodified-Since (per spec).
    } else {
        // 2. If-Unmodified-Since: 412 if object was modified after the given date.
        if let Some(if_unmodified) = headers
            .get("if-unmodified-since")
            .and_then(|v| v.to_str().ok())
        {
            if let (Some(obj_time), Ok(threshold)) =
                (last_modified_time, httpdate::parse_http_date(if_unmodified))
            {
                if obj_time > threshold {
                    return Err(S3Error::PreconditionFailed);
                }
            }
        }
    }

    // 3. If-None-Match: For GET/HEAD, return 304 if ETag matches.
    if let Some(if_none_match) = headers.get("if-none-match").and_then(|v| v.to_str().ok()) {
        let if_none_match_inner = strip_etag_quotes(if_none_match);
        let matches = if_none_match_inner == "*" || if_none_match_inner == record_etag_inner;
        if matches {
            if is_get_or_head {
                // Return 304 Not Modified.
                return Err(S3Error::NotModified);
            } else {
                return Err(S3Error::PreconditionFailed);
            }
        }
        // If If-None-Match is present and doesn't match, skip If-Modified-Since.
    } else {
        // 4. If-Modified-Since: 304 if object has not been modified since the date.
        if is_get_or_head {
            if let Some(if_modified) = headers
                .get("if-modified-since")
                .and_then(|v| v.to_str().ok())
            {
                if let (Some(obj_time), Ok(threshold)) =
                    (last_modified_time, httpdate::parse_http_date(if_modified))
                {
                    if obj_time <= threshold {
                        return Err(S3Error::NotModified);
                    }
                }
            }
        }
    }

    Ok(())
}

/// Parse an ISO-8601 timestamp to SystemTime for conditional request evaluation.
fn parse_iso8601_to_system_time(iso: &str) -> Option<std::time::SystemTime> {
    if iso.len() < 19 {
        return None;
    }
    let year: i32 = iso[0..4].parse().ok()?;
    let month: u32 = iso[5..7].parse().ok()?;
    let day: u32 = iso[8..10].parse().ok()?;
    let hours: u32 = iso[11..13].parse().ok()?;
    let minutes: u32 = iso[14..16].parse().ok()?;
    let seconds: u32 = iso[17..19].parse().ok()?;

    let days_since_epoch = ymd_to_days(year, month, day);
    let total_secs = days_since_epoch as u64 * 86400
        + hours as u64 * 3600
        + minutes as u64 * 60
        + seconds as u64;

    Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(total_secs))
}

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
/// User metadata headers start with `x-amz-meta-` (case-insensitive).
/// Returns a map of lowercased full header names to their values.
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

/// Validate the Content-MD5 header against the request body if present.
///
/// - Base64-decode the header value; return `InvalidDigest` if decode fails or result is not 16 bytes.
/// - Compute MD5 of the body and compare; return `BadDigest` on mismatch.
/// - If the header is absent, this is a no-op (returns Ok).
fn validate_content_md5(headers: &HeaderMap, body: &[u8]) -> Result<(), S3Error> {
    let md5_header = match headers.get("content-md5").and_then(|v| v.to_str().ok()) {
        Some(v) => v,
        None => return Ok(()),
    };

    // Base64-decode the provided MD5.
    let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, md5_header)
        .map_err(|_| S3Error::InvalidDigest)?;

    // MD5 digest must be exactly 16 bytes.
    if decoded.len() != 16 {
        return Err(S3Error::InvalidDigest);
    }

    // Compute MD5 of the body.
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(body);
    let computed = hasher.finalize();

    // Compare.
    if computed.as_slice() != decoded.as_slice() {
        return Err(S3Error::BadDigest);
    }

    Ok(())
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

/// Check whether any `x-amz-grant-*` headers are present.
fn has_grant_headers(headers: &HeaderMap) -> bool {
    headers.contains_key("x-amz-grant-full-control")
        || headers.contains_key("x-amz-grant-read")
        || headers.contains_key("x-amz-grant-read-acp")
        || headers.contains_key("x-amz-grant-write")
        || headers.contains_key("x-amz-grant-write-acp")
}

/// Validate that `x-amz-acl` and `x-amz-grant-*` headers are not both present.
fn validate_acl_mode(headers: &HeaderMap) -> Result<(), S3Error> {
    if headers.contains_key("x-amz-acl") && has_grant_headers(headers) {
        return Err(S3Error::InvalidArgument {
            message: "Specifying both x-amz-acl and x-amz-grant headers is not allowed".to_string(),
        });
    }
    Ok(())
}

/// Parse `x-amz-grant-*` headers into an ACL JSON string.
///
/// Each header value is a comma-separated list of grantees in the form:
///   `id="canonical-user-id"` or `uri="http://acs.amazonaws.com/groups/..."`
///
/// Returns `None` if no grant headers are present.
fn parse_grant_headers(headers: &HeaderMap, owner_id: &str, display_name: &str) -> Option<String> {
    if !has_grant_headers(headers) {
        return None;
    }

    let mut grants = vec![AclGrant {
        grantee: AclGrantee::CanonicalUser {
            id: owner_id.to_string(),
            display_name: display_name.to_string(),
        },
        permission: "FULL_CONTROL".to_string(),
    }];

    let grant_header_map: &[(&str, &str)] = &[
        ("x-amz-grant-full-control", "FULL_CONTROL"),
        ("x-amz-grant-read", "READ"),
        ("x-amz-grant-read-acp", "READ_ACP"),
        ("x-amz-grant-write", "WRITE"),
        ("x-amz-grant-write-acp", "WRITE_ACP"),
    ];

    for (header_name, permission) in grant_header_map {
        if let Some(value) = headers.get(*header_name).and_then(|v| v.to_str().ok()) {
            for grantee_str in value.split(',') {
                let grantee_str = grantee_str.trim();
                if let Some(grant) = parse_single_grantee(grantee_str, permission) {
                    grants.push(grant);
                }
            }
        }
    }

    let acl = Acl {
        owner: AclOwner {
            id: owner_id.to_string(),
            display_name: display_name.to_string(),
        },
        grants,
    };

    Some(serde_json::to_string(&acl).unwrap_or_else(|_| "{}".to_string()))
}

/// Parse a single grantee expression like `id="abc123"` or
/// `uri="http://acs.amazonaws.com/groups/global/AllUsers"`.
fn parse_single_grantee(grantee_str: &str, permission: &str) -> Option<AclGrant> {
    let grantee_str = grantee_str.trim();

    if let Some(rest) = grantee_str.strip_prefix("id=") {
        let id = rest.trim_matches('"').trim_matches('\'').to_string();
        Some(AclGrant {
            grantee: AclGrantee::CanonicalUser {
                id: id.clone(),
                display_name: id,
            },
            permission: permission.to_string(),
        })
    } else if let Some(rest) = grantee_str.strip_prefix("uri=") {
        let uri = rest.trim_matches('"').trim_matches('\'').to_string();
        Some(AclGrant {
            grantee: AclGrantee::Group { uri },
            permission: permission.to_string(),
        })
    } else {
        None
    }
}

/// Convert an ISO-8601 timestamp to RFC 7231 format for Last-Modified header.
/// Input: "2026-02-23T12:00:00.000Z"
/// Output: "Sun, 23 Feb 2026 12:00:00 GMT"
fn iso8601_to_http_date(iso: &str) -> String {
    // Parse the ISO-8601 timestamp. If parsing fails, return the current time.
    // Format: YYYY-MM-DDThh:mm:ss.sssZ
    if iso.len() < 19 {
        return httpdate::fmt_http_date(std::time::SystemTime::now());
    }

    let year: i32 = iso[0..4].parse().unwrap_or(1970);
    let month: u32 = iso[5..7].parse().unwrap_or(1);
    let day: u32 = iso[8..10].parse().unwrap_or(1);
    let hours: u32 = iso[11..13].parse().unwrap_or(0);
    let minutes: u32 = iso[14..16].parse().unwrap_or(0);
    let seconds: u32 = iso[17..19].parse().unwrap_or(0);

    // Convert back to SystemTime via seconds since epoch.
    // Use a rough calculation (doesn't need to be perfect for Last-Modified).
    let days_since_epoch = ymd_to_days(year, month, day);
    let total_secs = days_since_epoch as u64 * 86400
        + hours as u64 * 3600
        + minutes as u64 * 60
        + seconds as u64;

    let system_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(total_secs);
    httpdate::fmt_http_date(system_time)
}

/// Convert (year, month, day) to days since Unix epoch.
fn ymd_to_days(year: i32, month: u32, day: u32) -> i64 {
    // Inverse of the days_to_ymd algorithm (Howard Hinnant).
    let y = if month <= 2 {
        year as i64 - 1
    } else {
        year as i64
    };
    let m = if month <= 2 {
        month as i64 + 9
    } else {
        month as i64 - 3
    };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * m as u64 + 2) / 5 + day as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;

    era * 146097 + doe as i64 - 719468
}

// -- Handlers -----------------------------------------------------------------

/// `PUT /{bucket}/{key}` -- Upload an object (or copy if `x-amz-copy-source` is present).
#[utoipa::path(
    put,
    path = "/{bucket}/{key}",
    tag = "Object",
    operation_id = "PutObject",
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Object key"),
    ),
    responses(
        (status = 200, description = "Object stored"),
        (status = 404, description = "Bucket not found"),
        (status = 500, description = "Internal error")
    )
)]
pub async fn put_object(
    state: Arc<AppState>,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response, S3Error> {
    // Check bucket exists.
    if !state.metadata.bucket_exists(bucket).await? {
        return Err(S3Error::NoSuchBucket {
            bucket: bucket.to_string(),
        });
    }

    // If-None-Match: * — fail if object already exists (conditional PUT).
    if let Some(if_none_match) = headers.get("if-none-match").and_then(|v| v.to_str().ok()) {
        if if_none_match.trim() == "*" {
            if state.metadata.object_exists(bucket, key).await? {
                return Err(S3Error::PreconditionFailed);
            }
        }
    }

    // Validate key length (max 1024 bytes).
    if key.len() > 1024 {
        return Err(S3Error::KeyTooLongError);
    }

    // Check max object size.
    if body.len() as u64 > state.config.server.max_object_size {
        return Err(S3Error::EntityTooLarge);
    }

    // Validate Content-MD5 header if present.
    validate_content_md5(headers, body)?;

    let data = bytes::Bytes::copy_from_slice(body);
    let size = data.len() as u64;

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

    // Validate mutually exclusive ACL modes.
    validate_acl_mode(headers)?;

    // Determine ACL from x-amz-acl header, x-amz-grant-* headers, or default.
    let owner_id = state.config.auth.access_key.clone();
    let owner_display = state.config.auth.access_key.clone();
    let acl_json = if let Some(canned) = headers.get("x-amz-acl") {
        let canned_str = canned.to_str().unwrap_or("private");
        canned_acl_to_json(canned_str, &owner_id, &owner_display)?
    } else if let Some(grant_acl) = parse_grant_headers(headers, &owner_id, &owner_display) {
        grant_acl
    } else {
        default_acl_json(&owner_id, &owner_display)
    };

    // Storage key = bucket/key.
    let storage_key = format!("{bucket}/{key}");

    // Write to storage backend (crash-only: temp-fsync-rename).
    let etag = state.storage.put(&storage_key, data).await?;

    let now = now_iso8601();

    // Record in metadata store.
    let record = ObjectRecord {
        bucket: bucket.to_string(),
        key: key.to_string(),
        size,
        etag: etag.clone(),
        content_type,
        content_encoding,
        content_language,
        content_disposition,
        cache_control,
        expires,
        storage_class: "STANDARD".to_string(),
        acl: acl_json,
        last_modified: now,
        user_metadata,
        delete_marker: false,
    };

    state.metadata.put_object(record).await?;

    // Return 200 OK with ETag header.
    let mut response = StatusCode::OK.into_response();
    response
        .headers_mut()
        .insert("etag", HeaderValue::from_str(&etag).unwrap());
    Ok(response)
}

/// `GET /{bucket}/{key}` -- Retrieve an object.
#[utoipa::path(
    get,
    path = "/{bucket}/{key}",
    tag = "Object",
    operation_id = "GetObject",
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Object key"),
    ),
    responses(
        (status = 200, description = "Object data"),
        (status = 206, description = "Partial content (range request)"),
        (status = 304, description = "Not modified"),
        (status = 404, description = "Object not found"),
        (status = 412, description = "Precondition failed"),
        (status = 416, description = "Range not satisfiable"),
        (status = 500, description = "Internal error")
    )
)]
pub async fn get_object(
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

    // Look up object metadata.
    let record = state
        .metadata
        .get_object(bucket, key)
        .await?
        .ok_or_else(|| S3Error::NoSuchKey {
            key: key.to_string(),
        })?;

    // Evaluate conditional request headers (If-Match, If-None-Match, etc.).
    evaluate_conditions(headers, &record, true)?;

    // Read from storage backend.
    let storage_key = format!("{bucket}/{key}");
    let stored = state.storage.get(&storage_key).await?;
    let full_data = stored.data;
    let total_size = full_data.len() as u64;

    // Handle Range request if present.
    let (status, response_body, content_range, content_length) =
        if let Some(range_hdr) = headers.get("range").and_then(|v| v.to_str().ok()) {
            if let Some(byte_range) = parse_range_header(range_hdr) {
                if let Some((start, end)) = resolve_range(&byte_range, total_size) {
                    let slice = full_data.slice(start as usize..(end + 1) as usize);
                    let slice_len = slice.len() as u64;
                    let content_range = format!("bytes {start}-{end}/{total_size}");
                    (
                        StatusCode::PARTIAL_CONTENT,
                        slice.to_vec(),
                        Some(content_range),
                        slice_len,
                    )
                } else {
                    // Unsatisfiable range.
                    return Err(S3Error::InvalidRange);
                }
            } else {
                // Malformed range header -- ignore per HTTP spec, return full body.
                (StatusCode::OK, full_data.to_vec(), None, total_size)
            }
        } else {
            (StatusCode::OK, full_data.to_vec(), None, total_size)
        };

    // Build response.
    let mut response = (status, response_body).into_response();
    let hdrs = response.headers_mut();

    hdrs.insert(
        "content-type",
        HeaderValue::from_str(&record.content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    hdrs.insert("etag", HeaderValue::from_str(&record.etag).unwrap());

    // Content-Length is the size of the returned body (may differ from total for range requests).
    hdrs.insert(
        "content-length",
        HeaderValue::from_str(&content_length.to_string()).unwrap(),
    );
    if let Some(ref cr) = content_range {
        hdrs.insert("content-range", HeaderValue::from_str(cr).unwrap());
    }

    hdrs.insert(
        "last-modified",
        HeaderValue::from_str(&iso8601_to_http_date(&record.last_modified)).unwrap(),
    );
    hdrs.insert("accept-ranges", HeaderValue::from_static("bytes"));

    // Optional headers.
    if let Some(ref enc) = record.content_encoding {
        if let Ok(val) = HeaderValue::from_str(enc) {
            hdrs.insert("content-encoding", val);
        }
    }
    if let Some(ref lang) = record.content_language {
        if let Ok(val) = HeaderValue::from_str(lang) {
            hdrs.insert("content-language", val);
        }
    }
    if let Some(ref disp) = record.content_disposition {
        if let Ok(val) = HeaderValue::from_str(disp) {
            hdrs.insert("content-disposition", val);
        }
    }
    if let Some(ref cc) = record.cache_control {
        if let Ok(val) = HeaderValue::from_str(cc) {
            hdrs.insert("cache-control", val);
        }
    }
    if let Some(ref exp) = record.expires {
        if let Ok(val) = HeaderValue::from_str(exp) {
            hdrs.insert("expires", val);
        }
    }

    // Emit user metadata as response headers.
    for (name, value) in &record.user_metadata {
        if let (Ok(hname), Ok(hval)) = (
            axum::http::header::HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            hdrs.insert(hname, hval);
        }
    }

    Ok(response)
}

/// `HEAD /{bucket}/{key}` -- Retrieve object metadata without the body.
#[utoipa::path(
    head,
    path = "/{bucket}/{key}",
    tag = "Object",
    operation_id = "HeadObject",
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Object key"),
    ),
    responses(
        (status = 200, description = "Object metadata"),
        (status = 304, description = "Not modified"),
        (status = 404, description = "Object not found"),
        (status = 412, description = "Precondition failed"),
        (status = 500, description = "Internal error")
    )
)]
pub async fn head_object(
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

    // Look up object metadata.
    let record = match state.metadata.get_object(bucket, key).await? {
        Some(r) => r,
        None => {
            // HEAD responses should not have a body. Return 404 directly.
            return Ok(StatusCode::NOT_FOUND.into_response());
        }
    };

    // Evaluate conditional request headers.
    evaluate_conditions(headers, &record, true)?;

    // Build response with metadata headers but no body.
    let mut response = StatusCode::OK.into_response();
    let hdrs = response.headers_mut();

    hdrs.insert(
        "content-type",
        HeaderValue::from_str(&record.content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    hdrs.insert("etag", HeaderValue::from_str(&record.etag).unwrap());
    hdrs.insert(
        "content-length",
        HeaderValue::from_str(&record.size.to_string()).unwrap(),
    );
    hdrs.insert(
        "last-modified",
        HeaderValue::from_str(&iso8601_to_http_date(&record.last_modified)).unwrap(),
    );
    hdrs.insert("accept-ranges", HeaderValue::from_static("bytes"));

    // Optional headers.
    if let Some(ref enc) = record.content_encoding {
        if let Ok(val) = HeaderValue::from_str(enc) {
            hdrs.insert("content-encoding", val);
        }
    }
    if let Some(ref lang) = record.content_language {
        if let Ok(val) = HeaderValue::from_str(lang) {
            hdrs.insert("content-language", val);
        }
    }
    if let Some(ref disp) = record.content_disposition {
        if let Ok(val) = HeaderValue::from_str(disp) {
            hdrs.insert("content-disposition", val);
        }
    }
    if let Some(ref cc) = record.cache_control {
        if let Ok(val) = HeaderValue::from_str(cc) {
            hdrs.insert("cache-control", val);
        }
    }
    if let Some(ref exp) = record.expires {
        if let Ok(val) = HeaderValue::from_str(exp) {
            hdrs.insert("expires", val);
        }
    }

    // Emit user metadata as response headers.
    for (name, value) in &record.user_metadata {
        if let (Ok(hname), Ok(hval)) = (
            axum::http::header::HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            hdrs.insert(hname, hval);
        }
    }

    Ok(response)
}

/// `DELETE /{bucket}/{key}` -- Delete a single object.
#[utoipa::path(
    delete,
    path = "/{bucket}/{key}",
    tag = "Object",
    operation_id = "DeleteObject",
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Object key"),
    ),
    responses(
        (status = 204, description = "Object deleted"),
        (status = 404, description = "Bucket not found"),
        (status = 500, description = "Internal error")
    )
)]
pub async fn delete_object(
    state: Arc<AppState>,
    bucket: &str,
    key: &str,
) -> Result<Response, S3Error> {
    // Check bucket exists.
    if !state.metadata.bucket_exists(bucket).await? {
        return Err(S3Error::NoSuchBucket {
            bucket: bucket.to_string(),
        });
    }

    // Delete from storage backend (best-effort; idempotent).
    let storage_key = format!("{bucket}/{key}");
    let _ = state.storage.delete(&storage_key).await;

    // Delete from metadata store (idempotent: no error if not found).
    state.metadata.delete_object(bucket, key).await?;

    // S3 DeleteObject always returns 204, even if the object didn't exist.
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /{bucket}?delete` -- Delete multiple objects in a single request.
#[utoipa::path(
    post,
    path = "/{bucket}?delete",
    tag = "Object",
    operation_id = "DeleteObjects",
    params(("bucket" = String, Path, description = "Bucket name")),
    responses(
        (status = 200, description = "Delete result"),
        (status = 404, description = "Bucket not found"),
        (status = 400, description = "Malformed XML")
    )
)]
pub async fn delete_objects(
    state: Arc<AppState>,
    bucket: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response, S3Error> {
    // Check bucket exists.
    if !state.metadata.bucket_exists(bucket).await? {
        return Err(S3Error::NoSuchBucket {
            bucket: bucket.to_string(),
        });
    }

    // Validate Content-MD5 if present (technically required by AWS, but allow missing for compatibility).
    validate_content_md5(headers, body)?;

    // Parse the <Delete> XML body.
    let (keys, quiet) = parse_delete_xml(body)?;

    if keys.is_empty() {
        return Err(S3Error::MalformedXML);
    }

    // Batch delete from metadata (single SQL statement per batch).
    let mut deleted_keys: Vec<String> = Vec::new();
    let mut error_keys: Vec<String> = Vec::new();
    let mut error_messages: Vec<String> = Vec::new();

    match state.metadata.delete_objects(bucket, &keys).await {
        Ok(deleted) => {
            deleted_keys = deleted;
        }
        Err(e) => {
            // If batch delete fails, report all keys as errors.
            for key in &keys {
                error_keys.push(key.clone());
                error_messages.push(e.to_string());
            }
        }
    }

    // Delete from storage (best-effort, idempotent) — loop only for file deletion.
    for key in &deleted_keys {
        let storage_key = format!("{bucket}/{key}");
        let _ = state.storage.delete(&storage_key).await;
    }

    // Build response XML.
    let deleted_entries: Vec<crate::xml::DeletedEntry<'_>> = deleted_keys
        .iter()
        .map(|k| crate::xml::DeletedEntry { key: k })
        .collect();

    let error_entries: Vec<crate::xml::DeleteErrorEntry<'_>> = error_keys
        .iter()
        .zip(error_messages.iter())
        .map(|(k, m)| crate::xml::DeleteErrorEntry {
            key: k,
            code: "InternalError",
            message: m,
        })
        .collect();

    let xml = crate::xml::render_delete_result(&deleted_entries, &error_entries, quiet);

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

/// Parse `<Delete>` XML body for DeleteObjects.
/// Returns a list of keys to delete and the quiet flag.
fn parse_delete_xml(body: &[u8]) -> Result<(Vec<String>, bool), S3Error> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_reader(body);
    reader.trim_text(true);

    let mut keys = Vec::new();
    let mut quiet = false;
    let mut current_tag = String::new();
    let mut in_object = false;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let name_bytes = e.name().as_ref().to_vec();
                let tag_name = String::from_utf8_lossy(&name_bytes).to_string();
                current_tag = tag_name.clone();
                if tag_name == "Object" {
                    in_object = true;
                }
            }
            Ok(Event::End(ref e)) => {
                let name_bytes = e.name().as_ref().to_vec();
                let tag_name = String::from_utf8_lossy(&name_bytes).to_string();
                if tag_name == "Object" {
                    in_object = false;
                }
                current_tag.clear();
            }
            Ok(Event::Text(ref e)) => {
                let text = e.unescape().unwrap_or_default().to_string();
                if in_object && current_tag == "Key" {
                    keys.push(text);
                } else if current_tag == "Quiet" {
                    quiet = text == "true";
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(S3Error::MalformedXML),
            _ => {}
        }
        buf.clear();
    }

    Ok((keys, quiet))
}

/// `PUT /{bucket}/{key}` with `x-amz-copy-source` -- Copy an object.
#[utoipa::path(
    put,
    path = "/{bucket}/{key}?copy",
    tag = "Object",
    operation_id = "CopyObject",
    params(
        ("bucket" = String, Path, description = "Destination bucket name"),
        ("key" = String, Path, description = "Destination object key"),
    ),
    responses(
        (status = 200, description = "Copy result"),
        (status = 404, description = "Source not found"),
        (status = 400, description = "Bad request")
    )
)]
pub async fn copy_object(
    state: Arc<AppState>,
    dst_bucket: &str,
    dst_key: &str,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    // Check destination bucket exists.
    if !state.metadata.bucket_exists(dst_bucket).await? {
        return Err(S3Error::NoSuchBucket {
            bucket: dst_bucket.to_string(),
        });
    }

    // Parse x-amz-copy-source header to get source bucket/key.
    let copy_source = headers
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| S3Error::InvalidArgument {
            message: "Missing x-amz-copy-source header".to_string(),
        })?;

    // URL-decode the copy source path.
    let decoded_source = percent_encoding::percent_decode_str(copy_source).decode_utf8_lossy();
    let source_path = decoded_source.trim_start_matches('/');

    // Split into bucket/key.
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

    // Get source object metadata.
    let src_record = state
        .metadata
        .get_object(src_bucket, src_key)
        .await?
        .ok_or_else(|| S3Error::NoSuchKey {
            key: src_key.to_string(),
        })?;

    // Determine metadata directive: COPY (default) or REPLACE.
    let metadata_directive = headers
        .get("x-amz-metadata-directive")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("COPY");

    // Copy the file in storage backend.
    let etag = state
        .storage
        .copy_object(src_bucket, src_key, dst_bucket, dst_key)
        .await?;

    let now = now_iso8601();

    // Build destination metadata record.
    let dst_record = if metadata_directive.eq_ignore_ascii_case("REPLACE") {
        // REPLACE: use headers from this request for metadata.
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

        let owner_id = state.config.auth.access_key.clone();
        let owner_display = state.config.auth.access_key.clone();
        let acl_json = if let Some(canned) = headers.get("x-amz-acl") {
            let canned_str = canned.to_str().unwrap_or("private");
            canned_acl_to_json(canned_str, &owner_id, &owner_display)?
        } else {
            default_acl_json(&owner_id, &owner_display)
        };

        ObjectRecord {
            bucket: dst_bucket.to_string(),
            key: dst_key.to_string(),
            size: src_record.size,
            etag: etag.clone(),
            content_type,
            content_encoding,
            content_language,
            content_disposition,
            cache_control,
            expires,
            storage_class: "STANDARD".to_string(),
            acl: acl_json,
            last_modified: now.clone(),
            user_metadata,
            delete_marker: false,
        }
    } else {
        // COPY (default): copy metadata from source.
        ObjectRecord {
            bucket: dst_bucket.to_string(),
            key: dst_key.to_string(),
            size: src_record.size,
            etag: etag.clone(),
            content_type: src_record.content_type.clone(),
            content_encoding: src_record.content_encoding.clone(),
            content_language: src_record.content_language.clone(),
            content_disposition: src_record.content_disposition.clone(),
            cache_control: src_record.cache_control.clone(),
            expires: src_record.expires.clone(),
            storage_class: src_record.storage_class.clone(),
            acl: src_record.acl.clone(),
            last_modified: now.clone(),
            user_metadata: src_record.user_metadata.clone(),
            delete_marker: false,
        }
    };

    // Record in metadata store.
    state.metadata.put_object(dst_record).await?;

    // Return CopyObjectResult XML.
    let xml = crate::xml::render_copy_object_result(&etag, &now);

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

/// `GET /{bucket}?list-type=2` -- List objects using the V2 API.
#[utoipa::path(
    get,
    path = "/{bucket}?list-type=2",
    tag = "Object",
    operation_id = "ListObjectsV2",
    params(("bucket" = String, Path, description = "Bucket name")),
    responses(
        (status = 200, description = "Object list"),
        (status = 404, description = "Bucket not found")
    )
)]
pub async fn list_objects_v2(
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
    let delimiter = query.get("delimiter").map(|s| s.as_str()).unwrap_or("");
    let max_keys: u32 = query
        .get("max-keys")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let start_after = query.get("start-after").map(|s| s.as_str()).unwrap_or("");
    let continuation_token = query.get("continuation-token").map(|s| s.as_str());

    // Query metadata store.
    let result = state
        .metadata
        .list_objects(
            bucket,
            prefix,
            delimiter,
            max_keys,
            start_after,
            continuation_token,
        )
        .await?;

    // Build ObjectEntry list for XML rendering.
    let entries: Vec<crate::xml::ObjectEntry<'_>> = result
        .objects
        .iter()
        .map(|obj| crate::xml::ObjectEntry {
            key: &obj.key,
            last_modified: &obj.last_modified,
            etag: &obj.etag,
            size: obj.size,
            storage_class: &obj.storage_class,
        })
        .collect();

    let common_prefix_refs: Vec<&str> = result.common_prefixes.iter().map(|s| s.as_str()).collect();

    let key_count = (entries.len() + common_prefix_refs.len()) as u32;

    let xml = crate::xml::render_list_objects_result(
        bucket,
        prefix,
        delimiter,
        max_keys,
        result.is_truncated,
        key_count,
        &entries,
        &common_prefix_refs,
        continuation_token,
        result.next_continuation_token.as_deref(),
        Some(start_after),
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

/// `GET /{bucket}` -- List objects using the V1 API.
#[utoipa::path(
    get,
    path = "/{bucket}?list-type=1",
    tag = "Object",
    operation_id = "ListObjectsV1",
    params(("bucket" = String, Path, description = "Bucket name")),
    responses(
        (status = 200, description = "Object list (v1)"),
        (status = 404, description = "Bucket not found")
    )
)]
pub async fn list_objects_v1(
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

    // Extract query parameters (v1 uses marker, not continuation-token).
    let prefix = query.get("prefix").map(|s| s.as_str()).unwrap_or("");
    let delimiter = query.get("delimiter").map(|s| s.as_str()).unwrap_or("");
    let max_keys: u32 = query
        .get("max-keys")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let marker = query.get("marker").map(|s| s.as_str()).unwrap_or("");

    // V1 marker works like start_after -- objects after this key.
    let result = state
        .metadata
        .list_objects(bucket, prefix, delimiter, max_keys, marker, None)
        .await?;

    // Build ObjectEntry list for XML rendering.
    let entries: Vec<crate::xml::ObjectEntry<'_>> = result
        .objects
        .iter()
        .map(|obj| crate::xml::ObjectEntry {
            key: &obj.key,
            last_modified: &obj.last_modified,
            etag: &obj.etag,
            size: obj.size,
            storage_class: &obj.storage_class,
        })
        .collect();

    let common_prefix_refs: Vec<&str> = result.common_prefixes.iter().map(|s| s.as_str()).collect();

    // Determine NextMarker for V1: only present when delimiter is set and result is truncated.
    let next_marker = if result.is_truncated {
        if !delimiter.is_empty() {
            // NextMarker is the key of the last returned entry (object or common prefix).
            if let Some(last_obj) = entries.last() {
                Some(last_obj.key.to_string())
            } else {
                common_prefix_refs.last().map(|last_cp| last_cp.to_string())
            }
        } else {
            // Without delimiter, NextMarker is not required (client uses last key as marker).
            // But we include it for convenience.
            entries.last().map(|e| e.key.to_string())
        }
    } else {
        None
    };

    let xml = crate::xml::render_list_objects_result_v1(
        bucket,
        prefix,
        delimiter,
        marker,
        max_keys,
        result.is_truncated,
        &entries,
        &common_prefix_refs,
        next_marker.as_deref(),
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

/// `GET /{bucket}/{key}?acl` -- Get the ACL for an object.
#[utoipa::path(
    get,
    path = "/{bucket}/{key}?acl",
    tag = "Object",
    operation_id = "GetObjectAcl",
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Object key"),
    ),
    responses(
        (status = 200, description = "Object ACL"),
        (status = 404, description = "Object not found")
    )
)]
pub async fn get_object_acl(
    state: Arc<AppState>,
    bucket: &str,
    key: &str,
) -> Result<Response, S3Error> {
    // Check bucket exists.
    if !state.metadata.bucket_exists(bucket).await? {
        return Err(S3Error::NoSuchBucket {
            bucket: bucket.to_string(),
        });
    }

    // Look up object metadata.
    let record = state
        .metadata
        .get_object(bucket, key)
        .await?
        .ok_or_else(|| S3Error::NoSuchKey {
            key: key.to_string(),
        })?;

    // Parse ACL JSON. If parsing fails, return a default FULL_CONTROL ACL.
    let owner_id = state.config.auth.access_key.clone();
    let owner_display = state.config.auth.access_key.clone();
    let acl: Acl = serde_json::from_str(&record.acl)
        .unwrap_or_else(|_| Acl::full_control(&owner_id, &owner_display));

    let body = crate::xml::render_access_control_policy(&acl);

    Ok((
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        )],
        body,
    )
        .into_response())
}

/// `PUT /{bucket}/{key}?acl` -- Set the ACL for an object.
#[utoipa::path(
    put,
    path = "/{bucket}/{key}?acl",
    tag = "Object",
    operation_id = "PutObjectAcl",
    params(
        ("bucket" = String, Path, description = "Bucket name"),
        ("key" = String, Path, description = "Object key"),
    ),
    responses(
        (status = 200, description = "ACL updated"),
        (status = 404, description = "Object not found")
    )
)]
pub async fn put_object_acl(
    state: Arc<AppState>,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    _body: &[u8],
) -> Result<Response, S3Error> {
    // Check bucket exists.
    if !state.metadata.bucket_exists(bucket).await? {
        return Err(S3Error::NoSuchBucket {
            bucket: bucket.to_string(),
        });
    }

    // Check object exists.
    let record = state
        .metadata
        .get_object(bucket, key)
        .await?
        .ok_or_else(|| S3Error::NoSuchKey {
            key: key.to_string(),
        })?;

    // Validate mutually exclusive ACL modes.
    validate_acl_mode(headers)?;

    // Determine new ACL from x-amz-acl header, x-amz-grant-* headers, or default.
    let owner_id = state.config.auth.access_key.clone();
    let owner_display = state.config.auth.access_key.clone();

    let acl_json = if let Some(canned) = headers.get("x-amz-acl") {
        let canned_str = canned.to_str().unwrap_or("private");
        canned_acl_to_json(canned_str, &owner_id, &owner_display)?
    } else if let Some(grant_acl) = parse_grant_headers(headers, &owner_id, &owner_display) {
        grant_acl
    } else {
        // If no canned ACL or grant headers, default to private.
        default_acl_json(&owner_id, &owner_display)
    };

    // Suppress unused variable warning.
    let _ = record;

    state
        .metadata
        .update_object_acl(bucket, key, &acl_json)
        .await?;

    Ok((StatusCode::OK, "").into_response())
}

// -- Unit tests ---------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Range parsing tests --------------------------------------------------

    #[test]
    fn test_parse_range_bytes_start_end() {
        assert_eq!(
            parse_range_header("bytes=0-4"),
            Some(ByteRange::StartEnd(0, 4))
        );
        assert_eq!(
            parse_range_header("bytes=10-20"),
            Some(ByteRange::StartEnd(10, 20))
        );
    }

    #[test]
    fn test_parse_range_bytes_start_open() {
        assert_eq!(
            parse_range_header("bytes=5-"),
            Some(ByteRange::StartOpen(5))
        );
        assert_eq!(
            parse_range_header("bytes=0-"),
            Some(ByteRange::StartOpen(0))
        );
    }

    #[test]
    fn test_parse_range_bytes_suffix() {
        assert_eq!(parse_range_header("bytes=-3"), Some(ByteRange::Suffix(3)));
        assert_eq!(
            parse_range_header("bytes=-100"),
            Some(ByteRange::Suffix(100))
        );
    }

    #[test]
    fn test_parse_range_invalid() {
        assert_eq!(parse_range_header("bytes=-0"), None); // zero suffix is invalid
        assert_eq!(parse_range_header("bytes=5-3"), None); // start > end
        assert_eq!(parse_range_header(""), None);
        assert_eq!(parse_range_header("chars=0-4"), None);
        assert_eq!(parse_range_header("bytes=0-4,6-8"), None); // multi-range not supported
    }

    #[test]
    fn test_resolve_range_start_end() {
        // bytes=0-4 on 16 bytes -> (0, 4)
        assert_eq!(resolve_range(&ByteRange::StartEnd(0, 4), 16), Some((0, 4)));
        // bytes=0-100 on 16 bytes -> (0, 15) -- clamped
        assert_eq!(
            resolve_range(&ByteRange::StartEnd(0, 100), 16),
            Some((0, 15))
        );
        // bytes=20-25 on 16 bytes -> None -- unsatisfiable
        assert_eq!(resolve_range(&ByteRange::StartEnd(20, 25), 16), None);
    }

    #[test]
    fn test_resolve_range_start_open() {
        // bytes=5- on 16 bytes -> (5, 15)
        assert_eq!(resolve_range(&ByteRange::StartOpen(5), 16), Some((5, 15)));
        // bytes=20- on 16 bytes -> None
        assert_eq!(resolve_range(&ByteRange::StartOpen(20), 16), None);
    }

    #[test]
    fn test_resolve_range_suffix() {
        // bytes=-5 on 16 bytes -> (11, 15)
        assert_eq!(resolve_range(&ByteRange::Suffix(5), 16), Some((11, 15)));
        // bytes=-100 on 16 bytes -> (0, 15) -- suffix larger than file
        assert_eq!(resolve_range(&ByteRange::Suffix(100), 16), Some((0, 15)));
    }

    #[test]
    fn test_resolve_range_empty_file() {
        assert_eq!(resolve_range(&ByteRange::StartEnd(0, 4), 0), None);
        assert_eq!(resolve_range(&ByteRange::Suffix(5), 0), None);
        assert_eq!(resolve_range(&ByteRange::StartOpen(0), 0), None);
    }

    // -- ETag comparison tests ------------------------------------------------

    #[test]
    fn test_strip_etag_quotes() {
        assert_eq!(strip_etag_quotes("\"abc123\""), "abc123");
        assert_eq!(strip_etag_quotes("abc123"), "abc123");
        assert_eq!(strip_etag_quotes("\"\""), "");
        assert_eq!(strip_etag_quotes(""), "");
    }

    // -- Conditional request evaluation tests ---------------------------------

    fn make_record(etag: &str, last_modified: &str) -> ObjectRecord {
        ObjectRecord {
            bucket: "test".to_string(),
            key: "test.txt".to_string(),
            size: 100,
            etag: etag.to_string(),
            content_type: "text/plain".to_string(),
            content_encoding: None,
            content_language: None,
            content_disposition: None,
            cache_control: None,
            expires: None,
            storage_class: "STANDARD".to_string(),
            acl: "{}".to_string(),
            last_modified: last_modified.to_string(),
            user_metadata: HashMap::new(),
            delete_marker: false,
        }
    }

    #[test]
    fn test_if_match_success() {
        let record = make_record("\"abc123\"", "2026-01-15T10:00:00.000Z");
        let mut headers = HeaderMap::new();
        headers.insert("if-match", HeaderValue::from_static("\"abc123\""));
        assert!(evaluate_conditions(&headers, &record, true).is_ok());
    }

    #[test]
    fn test_if_match_failure() {
        let record = make_record("\"abc123\"", "2026-01-15T10:00:00.000Z");
        let mut headers = HeaderMap::new();
        headers.insert("if-match", HeaderValue::from_static("\"wrong\""));
        let err = evaluate_conditions(&headers, &record, true).unwrap_err();
        assert!(matches!(err, S3Error::PreconditionFailed));
    }

    #[test]
    fn test_if_match_wildcard() {
        let record = make_record("\"abc123\"", "2026-01-15T10:00:00.000Z");
        let mut headers = HeaderMap::new();
        headers.insert("if-match", HeaderValue::from_static("*"));
        assert!(evaluate_conditions(&headers, &record, true).is_ok());
    }

    #[test]
    fn test_if_none_match_match_returns_not_modified() {
        let record = make_record("\"abc123\"", "2026-01-15T10:00:00.000Z");
        let mut headers = HeaderMap::new();
        headers.insert("if-none-match", HeaderValue::from_static("\"abc123\""));
        let err = evaluate_conditions(&headers, &record, true).unwrap_err();
        assert!(matches!(err, S3Error::NotModified));
    }

    #[test]
    fn test_if_none_match_no_match_passes() {
        let record = make_record("\"abc123\"", "2026-01-15T10:00:00.000Z");
        let mut headers = HeaderMap::new();
        headers.insert("if-none-match", HeaderValue::from_static("\"different\""));
        assert!(evaluate_conditions(&headers, &record, true).is_ok());
    }

    #[test]
    fn test_if_unmodified_since_success() {
        // Object modified at 2026-01-15, threshold at 2026-02-01 -> object was not modified after threshold
        let record = make_record("\"abc123\"", "2026-01-15T10:00:00.000Z");
        let mut headers = HeaderMap::new();
        headers.insert(
            "if-unmodified-since",
            HeaderValue::from_static("Sat, 01 Feb 2026 00:00:00 GMT"),
        );
        assert!(evaluate_conditions(&headers, &record, true).is_ok());
    }

    #[test]
    fn test_if_unmodified_since_failure() {
        // Object modified at 2026-01-15, threshold at 2025-01-01 -> object was modified after
        let record = make_record("\"abc123\"", "2026-01-15T10:00:00.000Z");
        let mut headers = HeaderMap::new();
        headers.insert(
            "if-unmodified-since",
            HeaderValue::from_static("Wed, 01 Jan 2025 00:00:00 GMT"),
        );
        let err = evaluate_conditions(&headers, &record, true).unwrap_err();
        assert!(matches!(err, S3Error::PreconditionFailed));
    }

    #[test]
    fn test_if_modified_since_not_modified() {
        // Object modified at 2026-01-15, threshold at 2026-02-01 -> not modified since threshold
        let record = make_record("\"abc123\"", "2026-01-15T10:00:00.000Z");
        let mut headers = HeaderMap::new();
        headers.insert(
            "if-modified-since",
            HeaderValue::from_static("Sun, 01 Feb 2026 00:00:00 GMT"),
        );
        let err = evaluate_conditions(&headers, &record, true).unwrap_err();
        assert!(matches!(err, S3Error::NotModified));
    }

    #[test]
    fn test_if_modified_since_was_modified() {
        // Object modified at 2026-01-15, threshold at 2025-01-01 -> was modified since threshold
        let record = make_record("\"abc123\"", "2026-01-15T10:00:00.000Z");
        let mut headers = HeaderMap::new();
        headers.insert(
            "if-modified-since",
            HeaderValue::from_static("Wed, 01 Jan 2025 00:00:00 GMT"),
        );
        assert!(evaluate_conditions(&headers, &record, true).is_ok());
    }

    // -- ISO-8601 to SystemTime tests -----------------------------------------

    #[test]
    fn test_parse_iso8601_to_system_time() {
        let time = parse_iso8601_to_system_time("2026-01-15T10:00:00.000Z");
        assert!(time.is_some());
        // Verify it round-trips through httpdate
        let formatted = httpdate::fmt_http_date(time.unwrap());
        assert!(formatted.contains("2026"));
    }

    #[test]
    fn test_parse_iso8601_to_system_time_invalid() {
        assert!(parse_iso8601_to_system_time("short").is_none());
        assert!(parse_iso8601_to_system_time("").is_none());
    }
}
