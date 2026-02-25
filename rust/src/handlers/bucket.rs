//! Bucket-level S3 API handlers.

use std::sync::Arc;

use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::errors::S3Error;
use crate::metadata::store::{Acl, AclGrant, AclGrantee, BucketRecord};
use crate::xml;
use crate::AppState;

// -- Bucket name validation ---------------------------------------------------

/// Validate that a bucket name conforms to S3 naming rules.
///
/// Rules:
/// - 3-63 characters long
/// - Only lowercase letters, numbers, hyphens, and periods
/// - Must begin and end with a letter or number
/// - Cannot be formatted as an IP address (e.g., 192.168.5.4)
/// - Must not start with `xn--` or end with `-s3alias` or `--ol-s3`
pub fn validate_bucket_name(name: &str) -> Result<(), S3Error> {
    let len = name.len();

    if !(3..=63).contains(&len) {
        return Err(S3Error::InvalidBucketName {
            name: name.to_string(),
        });
    }

    // Must only contain lowercase letters, digits, hyphens, periods.
    for ch in name.chars() {
        if !ch.is_ascii_lowercase() && !ch.is_ascii_digit() && ch != '-' && ch != '.' {
            return Err(S3Error::InvalidBucketName {
                name: name.to_string(),
            });
        }
    }

    // Must begin and end with a letter or digit.
    let first = name.chars().next().unwrap();
    let last = name.chars().last().unwrap();
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return Err(S3Error::InvalidBucketName {
            name: name.to_string(),
        });
    }
    if !(last.is_ascii_lowercase() || last.is_ascii_digit()) {
        return Err(S3Error::InvalidBucketName {
            name: name.to_string(),
        });
    }

    // Cannot look like an IP address.
    if looks_like_ip(name) {
        return Err(S3Error::InvalidBucketName {
            name: name.to_string(),
        });
    }

    // Must not start with xn--.
    if name.starts_with("xn--") {
        return Err(S3Error::InvalidBucketName {
            name: name.to_string(),
        });
    }

    // Must not end with -s3alias or --ol-s3.
    if name.ends_with("-s3alias") || name.ends_with("--ol-s3") {
        return Err(S3Error::InvalidBucketName {
            name: name.to_string(),
        });
    }

    Ok(())
}

/// Check whether a string looks like an IPv4 address (e.g., "192.168.5.4").
fn looks_like_ip(s: &str) -> bool {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 4 {
        return false;
    }
    parts.iter().all(|p| p.parse::<u8>().is_ok())
}

/// Build a default FULL_CONTROL ACL for the given owner.
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
        "private" => {
            // Just owner FULL_CONTROL, already added above.
        }
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
        owner: crate::metadata::store::AclOwner {
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

/// Parse `x-amz-grant-*` headers into an ACL structure.
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
        owner: crate::metadata::store::AclOwner {
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

/// Get current time as ISO-8601 string. Duplicates the one in sqlite.rs for
/// handler-level use (bucket creation timestamps).
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

// -- Handlers -----------------------------------------------------------------

/// `GET /` -- List all buckets owned by the authenticated sender.
#[utoipa::path(
    get,
    path = "/",
    tag = "Bucket",
    operation_id = "ListBuckets",
    responses(
        (status = 200, description = "Bucket list"),
        (status = 500, description = "Internal error")
    )
)]
pub async fn list_buckets(state: Arc<AppState>) -> Result<Response, S3Error> {
    let buckets = state.metadata.list_buckets().await?;

    // Use the config auth key as owner ID / display name.
    let owner_id = &state.config.auth.access_key;
    let owner_display = &state.config.auth.access_key;

    let bucket_refs: Vec<(&str, &str)> = buckets
        .iter()
        .map(|b| (b.name.as_str(), b.created_at.as_str()))
        .collect();

    let body = xml::render_list_buckets_result(owner_id, owner_display, &bucket_refs);

    Ok((StatusCode::OK, [("content-type", "application/xml")], body).into_response())
}

/// `PUT /{bucket}` -- Create a new bucket.
#[utoipa::path(
    put,
    path = "/{bucket}",
    tag = "Bucket",
    operation_id = "CreateBucket",
    params(("bucket" = String, Path, description = "Bucket name")),
    responses(
        (status = 200, description = "Bucket created"),
        (status = 409, description = "Bucket already exists"),
        (status = 400, description = "Invalid bucket name")
    )
)]
pub async fn create_bucket(
    state: Arc<AppState>,
    bucket: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response, S3Error> {
    // Validate bucket name.
    validate_bucket_name(bucket)?;

    // Determine region from body (if CreateBucketConfiguration is present).
    let region = if body.is_empty() {
        state.config.server.region.clone()
    } else {
        parse_location_constraint(body).unwrap_or_else(|| state.config.server.region.clone())
    };

    let owner_id = state.config.auth.access_key.clone();
    let owner_display = state.config.auth.access_key.clone();

    // Validate mutually exclusive ACL modes.
    validate_acl_mode(headers)?;

    // Determine ACL from x-amz-acl header, x-amz-grant-* headers, or default.
    let acl_json = if let Some(canned) = headers.get("x-amz-acl") {
        let canned_str = canned.to_str().unwrap_or("private");
        canned_acl_to_json(canned_str, &owner_id, &owner_display)?
    } else if let Some(grant_acl) = parse_grant_headers(headers, &owner_id, &owner_display) {
        grant_acl
    } else {
        default_acl_json(&owner_id, &owner_display)
    };

    let location = format!("/{bucket}");

    // Check if bucket already exists.
    if let Some(existing) = state.metadata.get_bucket(bucket).await? {
        // In us-east-1 (our default), if the bucket is owned by the same user,
        // return 200 (BucketAlreadyOwnedByYou treated as success).
        if existing.owner_id == owner_id && state.config.server.region == "us-east-1" {
            let mut response = (StatusCode::OK, "").into_response();
            response
                .headers_mut()
                .insert("location", HeaderValue::from_str(&location).unwrap());
            return Ok(response);
        } else {
            return Err(S3Error::BucketAlreadyOwnedByYou {
                bucket: bucket.to_string(),
            });
        }
    }

    let record = BucketRecord {
        name: bucket.to_string(),
        created_at: now_iso8601(),
        region,
        owner_id,
        owner_display,
        acl: acl_json,
    };

    // Create in metadata store.
    state.metadata.create_bucket(record).await?;

    // Create in storage backend.
    state.storage.create_bucket(bucket).await?;

    let mut response = (StatusCode::OK, "").into_response();
    response
        .headers_mut()
        .insert("location", HeaderValue::from_str(&location).unwrap());
    Ok(response)
}

/// `DELETE /{bucket}` -- Delete an existing bucket (must be empty).
#[utoipa::path(
    delete,
    path = "/{bucket}",
    tag = "Bucket",
    operation_id = "DeleteBucket",
    params(("bucket" = String, Path, description = "Bucket name")),
    responses(
        (status = 204, description = "Bucket deleted"),
        (status = 404, description = "Bucket not found"),
        (status = 409, description = "Bucket not empty")
    )
)]
pub async fn delete_bucket(state: Arc<AppState>, bucket: &str) -> Result<Response, S3Error> {
    // Check bucket exists.
    if !state.metadata.bucket_exists(bucket).await? {
        return Err(S3Error::NoSuchBucket {
            bucket: bucket.to_string(),
        });
    }

    // Check bucket is empty.
    let count = state.metadata.count_objects(bucket).await?;
    if count > 0 {
        return Err(S3Error::BucketNotEmpty {
            bucket: bucket.to_string(),
        });
    }

    // Delete from metadata store.
    state.metadata.delete_bucket(bucket).await?;

    // Delete from storage backend (best-effort; metadata is truth).
    let _ = state.storage.delete_bucket(bucket).await;

    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `HEAD /{bucket}` -- Check whether a bucket exists.
#[utoipa::path(
    head,
    path = "/{bucket}",
    tag = "Bucket",
    operation_id = "HeadBucket",
    params(("bucket" = String, Path, description = "Bucket name")),
    responses(
        (status = 200, description = "Bucket exists"),
        (status = 404, description = "Bucket not found")
    )
)]
pub async fn head_bucket(state: Arc<AppState>, bucket: &str) -> Result<Response, S3Error> {
    if let Some(record) = state.metadata.get_bucket(bucket).await? {
        let mut response = StatusCode::OK.into_response();
        response.headers_mut().insert(
            "x-amz-bucket-region",
            HeaderValue::from_str(&record.region)
                .unwrap_or_else(|_| HeaderValue::from_static("us-east-1")),
        );
        Ok(response)
    } else {
        // HEAD responses have no body. Return 404 status directly.
        // We cannot use S3Error here because HEAD must not have a body.
        Ok(StatusCode::NOT_FOUND.into_response())
    }
}

/// `GET /{bucket}?location` -- Return the region constraint of a bucket.
#[utoipa::path(
    get,
    path = "/{bucket}?location",
    tag = "Bucket",
    operation_id = "GetBucketLocation",
    params(("bucket" = String, Path, description = "Bucket name")),
    responses(
        (status = 200, description = "Location constraint"),
        (status = 404, description = "Bucket not found")
    )
)]
pub async fn get_bucket_location(state: Arc<AppState>, bucket: &str) -> Result<Response, S3Error> {
    let record = state
        .metadata
        .get_bucket(bucket)
        .await?
        .ok_or_else(|| S3Error::NoSuchBucket {
            bucket: bucket.to_string(),
        })?;

    let body = xml::render_location_constraint(&record.region);

    Ok((StatusCode::OK, [("content-type", "application/xml")], body).into_response())
}

/// `GET /{bucket}?acl` -- Return the access control list of a bucket.
#[utoipa::path(
    get,
    path = "/{bucket}?acl",
    tag = "Bucket",
    operation_id = "GetBucketAcl",
    params(("bucket" = String, Path, description = "Bucket name")),
    responses(
        (status = 200, description = "Bucket ACL"),
        (status = 404, description = "Bucket not found")
    )
)]
pub async fn get_bucket_acl(state: Arc<AppState>, bucket: &str) -> Result<Response, S3Error> {
    let record = state
        .metadata
        .get_bucket(bucket)
        .await?
        .ok_or_else(|| S3Error::NoSuchBucket {
            bucket: bucket.to_string(),
        })?;

    // Parse ACL JSON. If parsing fails, return a default FULL_CONTROL ACL.
    let acl: Acl = serde_json::from_str(&record.acl)
        .unwrap_or_else(|_| Acl::full_control(&record.owner_id, &record.owner_display));

    let body = xml::render_access_control_policy(&acl);

    Ok((StatusCode::OK, [("content-type", "application/xml")], body).into_response())
}

/// `PUT /{bucket}?acl` -- Set the access control list of a bucket.
#[utoipa::path(
    put,
    path = "/{bucket}?acl",
    tag = "Bucket",
    operation_id = "PutBucketAcl",
    params(("bucket" = String, Path, description = "Bucket name")),
    responses(
        (status = 200, description = "ACL updated"),
        (status = 404, description = "Bucket not found")
    )
)]
pub async fn put_bucket_acl(
    state: Arc<AppState>,
    bucket: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response, S3Error> {
    // Check bucket exists.
    let record = state
        .metadata
        .get_bucket(bucket)
        .await?
        .ok_or_else(|| S3Error::NoSuchBucket {
            bucket: bucket.to_string(),
        })?;

    let owner_id = &record.owner_id;
    let owner_display = &record.owner_display;

    // Validate mutually exclusive ACL modes.
    validate_acl_mode(headers)?;

    // Determine ACL: canned header takes priority, then grant headers, then XML body, then default.
    let acl_json = if let Some(canned) = headers.get("x-amz-acl") {
        let canned_str = canned.to_str().unwrap_or("private");
        canned_acl_to_json(canned_str, owner_id, owner_display)?
    } else if let Some(grant_acl) = parse_grant_headers(headers, owner_id, owner_display) {
        grant_acl
    } else if !body.is_empty() {
        // Parse AccessControlPolicy XML body.
        parse_acl_xml_body(body, owner_id, owner_display)?
    } else {
        // No ACL specified -- default to private.
        default_acl_json(owner_id, owner_display)
    };

    state.metadata.update_bucket_acl(bucket, &acl_json).await?;

    Ok((StatusCode::OK, "").into_response())
}

// -- XML parsing helpers ------------------------------------------------------

/// Parse an `<AccessControlPolicy>` XML body into ACL JSON.
///
/// Extracts Owner and Grants from the XML, converts them to our internal
/// ACL representation. Returns `MalformedACLError` if parsing fails.
fn parse_acl_xml_body(
    body: &[u8],
    default_owner_id: &str,
    default_owner_display: &str,
) -> Result<String, S3Error> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_reader(body);
    reader.trim_text(true);
    let mut buf = Vec::new();

    let mut owner_id = default_owner_id.to_string();
    let mut owner_display = default_owner_display.to_string();

    // State tracking for nested XML elements.
    let mut in_owner = false;
    let mut in_acl_list = false;
    let mut in_grant = false;
    let mut in_grantee = false;
    let mut current_tag = String::new();

    // Current grant being built.
    let mut grant_permission = String::new();
    let mut grantee_type = String::new(); // "CanonicalUser" or "Group"
    let mut grantee_id = String::new();
    let mut grantee_display_name = String::new();
    let mut grantee_uri = String::new();

    let mut grants: Vec<AclGrant> = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                // Strip namespace prefix if present (e.g., "s3:Owner" -> "Owner").
                let tag = tag.rsplit(':').next().unwrap_or(&tag).to_string();
                current_tag = tag.clone();

                match tag.as_str() {
                    "Owner" => in_owner = true,
                    "AccessControlList" => in_acl_list = true,
                    "Grant" => {
                        if in_acl_list {
                            in_grant = true;
                            grant_permission.clear();
                            grantee_type.clear();
                            grantee_id.clear();
                            grantee_display_name.clear();
                            grantee_uri.clear();
                        }
                    }
                    "Grantee" => {
                        if in_grant {
                            in_grantee = true;
                            // Extract xsi:type attribute.
                            for attr in e.attributes().flatten() {
                                let attr_name =
                                    String::from_utf8_lossy(attr.key.as_ref()).to_string();
                                if attr_name.ends_with("type") || attr_name == "type" {
                                    grantee_type = String::from_utf8_lossy(&attr.value).to_string();
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let tag = tag.rsplit(':').next().unwrap_or(&tag).to_string();

                match tag.as_str() {
                    "Owner" => in_owner = false,
                    "AccessControlList" => in_acl_list = false,
                    "Grant" => {
                        if in_grant {
                            // Build the grant and add to list.
                            let grantee = if grantee_type == "Group" || !grantee_uri.is_empty() {
                                AclGrantee::Group {
                                    uri: grantee_uri.clone(),
                                }
                            } else {
                                AclGrantee::CanonicalUser {
                                    id: grantee_id.clone(),
                                    display_name: grantee_display_name.clone(),
                                }
                            };
                            grants.push(AclGrant {
                                grantee,
                                permission: grant_permission.clone(),
                            });
                            in_grant = false;
                        }
                    }
                    "Grantee" => in_grantee = false,
                    _ => {}
                }
                current_tag.clear();
            }
            Ok(Event::Text(ref e)) => {
                let text = e.unescape().unwrap_or_default().to_string();
                if text.is_empty() {
                    buf.clear();
                    continue;
                }

                // Strip namespace prefix from current_tag for matching.
                let tag = current_tag.rsplit(':').next().unwrap_or(&current_tag);

                if in_owner && !in_grantee {
                    match tag {
                        "ID" => owner_id = text,
                        "DisplayName" => owner_display = text,
                        _ => {}
                    }
                } else if in_grantee {
                    match tag {
                        "ID" => grantee_id = text,
                        "DisplayName" => grantee_display_name = text,
                        "URI" => grantee_uri = text,
                        _ => {}
                    }
                } else if in_grant && tag == "Permission" {
                    grant_permission = text;
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(S3Error::MalformedACLError),
            _ => {}
        }
        buf.clear();
    }

    let acl = Acl {
        owner: crate::metadata::store::AclOwner {
            id: owner_id,
            display_name: owner_display,
        },
        grants,
    };

    Ok(serde_json::to_string(&acl).unwrap_or_else(|_| "{}".to_string()))
}

/// Parse `<CreateBucketConfiguration>` XML body to extract `<LocationConstraint>`.
fn parse_location_constraint(body: &[u8]) -> Option<String> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_reader(body);
    let mut buf = Vec::new();
    let mut in_location = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) if e.name().as_ref() == b"LocationConstraint" => {
                in_location = true;
            }
            Ok(Event::Text(ref e)) if in_location => {
                let text = e.unescape().ok()?.trim().to_string();
                if text.is_empty() {
                    return None;
                }
                return Some(text);
            }
            Ok(Event::End(ref e)) if e.name().as_ref() == b"LocationConstraint" => {
                if in_location {
                    return None; // Empty element (no text between start/end)
                }
            }
            Ok(Event::Eof) => return None,
            Err(_) => return None,
            _ => {}
        }
        buf.clear();
    }
}

// -- Validation struct (kept for garde/OpenAPI compatibility) -------------------

/// Validation struct for bucket names (preparation for garde integration).
#[derive(Debug, garde::Validate)]
pub struct BucketNameInput {
    /// Bucket name: 3-63 lowercase alphanumeric characters, dots, and hyphens.
    #[garde(length(min = 3, max = 63), pattern(r"^[a-z0-9][a-z0-9.\-]*[a-z0-9]$"))]
    pub bucket_name: String,
}

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_bucket_name_valid() {
        assert!(validate_bucket_name("valid-bucket").is_ok());
        assert!(validate_bucket_name("my.bucket.name").is_ok());
        assert!(validate_bucket_name("abc").is_ok());
        assert!(validate_bucket_name("a-b-c").is_ok());
        assert!(validate_bucket_name("123").is_ok());
        assert!(validate_bucket_name("a1b2c3").is_ok());
    }

    #[test]
    fn test_validate_bucket_name_too_short() {
        assert!(validate_bucket_name("ab").is_err());
        assert!(validate_bucket_name("a").is_err());
        assert!(validate_bucket_name("").is_err());
    }

    #[test]
    fn test_validate_bucket_name_too_long() {
        let long_name = "a".repeat(64);
        assert!(validate_bucket_name(&long_name).is_err());
    }

    #[test]
    fn test_validate_bucket_name_uppercase() {
        assert!(validate_bucket_name("INVALID").is_err());
        assert!(validate_bucket_name("InvalidBucket").is_err());
    }

    #[test]
    fn test_validate_bucket_name_bad_chars() {
        assert!(validate_bucket_name("bucket_name").is_err()); // underscore
        assert!(validate_bucket_name("bucket name").is_err()); // space
        assert!(validate_bucket_name("bucket!name").is_err()); // exclamation
    }

    #[test]
    fn test_validate_bucket_name_bad_start_end() {
        assert!(validate_bucket_name("-bucket").is_err());
        assert!(validate_bucket_name("bucket-").is_err());
        assert!(validate_bucket_name(".bucket").is_err());
        assert!(validate_bucket_name("bucket.").is_err());
    }

    #[test]
    fn test_validate_bucket_name_ip_address() {
        assert!(validate_bucket_name("192.168.1.1").is_err());
        assert!(validate_bucket_name("10.0.0.1").is_err());
    }

    #[test]
    fn test_validate_bucket_name_xn_prefix() {
        assert!(validate_bucket_name("xn--example").is_err());
    }

    #[test]
    fn test_validate_bucket_name_s3alias_suffix() {
        assert!(validate_bucket_name("example-s3alias").is_err());
        assert!(validate_bucket_name("example--ol-s3").is_err());
    }

    #[test]
    fn test_looks_like_ip() {
        assert!(looks_like_ip("192.168.1.1"));
        assert!(looks_like_ip("10.0.0.1"));
        assert!(!looks_like_ip("192.168.1"));
        assert!(!looks_like_ip("not.an.ip.address"));
        assert!(!looks_like_ip("abc"));
        assert!(!looks_like_ip("999.999.999.999")); // 999 > 255 so u8 parse fails
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
        assert!(acl.grants.iter().any(|g| g.permission == "FULL_CONTROL"));
        assert!(acl.grants.iter().any(|g| g.permission == "READ"));
    }

    #[test]
    fn test_canned_acl_invalid() {
        assert!(canned_acl_to_json("invalid-acl", "owner1", "Owner One").is_err());
    }

    #[test]
    fn test_parse_location_constraint() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <LocationConstraint>us-west-2</LocationConstraint>
</CreateBucketConfiguration>"#;
        assert_eq!(
            parse_location_constraint(xml),
            Some("us-west-2".to_string())
        );
    }

    #[test]
    fn test_parse_location_constraint_empty() {
        let xml = b"";
        assert_eq!(parse_location_constraint(xml), None);
    }
}
