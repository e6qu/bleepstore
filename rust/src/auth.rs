//! AWS Signature Version 4 authentication.
//!
//! Provides functions for verifying incoming S3 requests that carry either:
//! - `Authorization: AWS4-HMAC-SHA256 ...` headers (header-based auth)
//! - `X-Amz-Algorithm=AWS4-HMAC-SHA256` query parameters (presigned URLs)
//!
//! The core algorithm follows the AWS SigV4 specification:
//! 1. Build a canonical request
//! 2. Build a string-to-sign
//! 3. Derive a signing key via HMAC chain
//! 4. Compute and compare the signature

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Clock skew tolerance for header-based auth (15 minutes).
const CLOCK_SKEW_SECONDS: u64 = 900;

/// Maximum presigned URL expiration (7 days).
const MAX_PRESIGNED_EXPIRES: u64 = 604800;

/// SHA-256 of the empty string.
#[allow(dead_code)]
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

// ── Parsed types ────────────────────────────────────────────────────

/// Parsed components from an Authorization header.
#[derive(Debug, Clone)]
pub struct ParsedAuthorization {
    /// The access key ID from the Credential field.
    pub access_key_id: String,
    /// The date stamp (YYYYMMDD) from the Credential field.
    pub date_stamp: String,
    /// The region from the Credential field.
    pub region: String,
    /// The service from the Credential field (should be "s3").
    pub service: String,
    /// The signed headers (semicolon-separated, lowercase, sorted).
    pub signed_headers: String,
    /// The provided signature (64-char hex string).
    pub signature: String,
    /// The full credential scope string.
    pub credential_scope: String,
}

/// Parsed components from presigned URL query parameters.
#[derive(Debug, Clone)]
pub struct ParsedPresigned {
    /// The access key ID.
    pub access_key_id: String,
    /// The date stamp (YYYYMMDD).
    pub date_stamp: String,
    /// The region.
    pub region: String,
    /// The service.
    pub service: String,
    /// The credential scope.
    pub credential_scope: String,
    /// The signed headers (semicolon-separated).
    pub signed_headers: String,
    /// The provided signature.
    pub signature: String,
    /// The X-Amz-Date value.
    pub amz_date: String,
    /// The X-Amz-Expires value (seconds).
    pub expires: u64,
}

/// The type of authentication detected on a request.
#[derive(Debug)]
pub enum AuthType {
    /// Authorization header-based SigV4.
    Header(ParsedAuthorization),
    /// Presigned URL query parameter-based SigV4.
    Presigned(ParsedPresigned),
    /// No authentication present (anonymous request).
    None,
}

/// Result of authentication: either the verified access key ID or an error.
pub enum AuthResult {
    /// Authentication succeeded. Contains (access_key_id, owner_id, display_name).
    Ok {
        access_key_id: String,
        owner_id: String,
        display_name: String,
    },
    /// The access key was not found.
    InvalidAccessKeyId,
    /// The signature did not match.
    SignatureDoesNotMatch,
    /// The request was malformed (bad auth header, missing params, etc.).
    MalformedAuth(String),
    /// Presigned URL has expired.
    Expired,
}

// ── Detection ───────────────────────────────────────────────────────

/// Detect the authentication type from request headers and query string.
///
/// Returns `AuthType::Header` if an Authorization header starting with
/// `AWS4-HMAC-SHA256` is present, `AuthType::Presigned` if `X-Amz-Algorithm`
/// is in the query string, or `AuthType::None` if neither.
///
/// Returns an error string if both are present (ambiguous).
pub fn detect_auth_type(
    authorization_header: Option<&str>,
    query_string: &str,
) -> Result<AuthType, String> {
    let has_header = authorization_header
        .map(|h| h.starts_with("AWS4-HMAC-SHA256"))
        .unwrap_or(false);

    let query_params = parse_query_string(query_string);
    let has_presigned = query_params.contains_key("X-Amz-Algorithm");

    if has_header && has_presigned {
        return Err("Both Authorization header and presigned query parameters present".to_string());
    }

    if has_header {
        let auth_header = authorization_header.unwrap();
        let parsed = parse_authorization_header(auth_header)?;
        return Ok(AuthType::Header(parsed));
    }

    if has_presigned {
        let parsed = parse_presigned_params(&query_params)?;
        return Ok(AuthType::Presigned(parsed));
    }

    Ok(AuthType::None)
}

// ── Authorization header parsing ────────────────────────────────────

/// Parse the `Authorization` header value into its components.
///
/// Expected format:
/// ```text
/// AWS4-HMAC-SHA256 Credential=AKID/20260222/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abcdef...
/// ```
pub fn parse_authorization_header(header: &str) -> Result<ParsedAuthorization, String> {
    let header = header.trim();

    // Strip the algorithm prefix.
    let rest = header
        .strip_prefix("AWS4-HMAC-SHA256")
        .ok_or("Authorization header does not start with AWS4-HMAC-SHA256")?
        .trim();

    // Extract the three fields: Credential, SignedHeaders, Signature.
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;

    for part in rest.split(',') {
        let part = part.trim();
        if let Some(val) = part.strip_prefix("Credential=") {
            credential = Some(val.trim().to_string());
        } else if let Some(val) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(val.trim().to_string());
        } else if let Some(val) = part.strip_prefix("Signature=") {
            signature = Some(val.trim().to_string());
        }
    }

    let credential = credential.ok_or("Missing Credential in Authorization header")?;
    let signed_headers = signed_headers.ok_or("Missing SignedHeaders in Authorization header")?;
    let signature = signature.ok_or("Missing Signature in Authorization header")?;

    // Parse credential: AKID/YYYYMMDD/region/service/aws4_request
    let parts: Vec<&str> = credential.splitn(5, '/').collect();
    if parts.len() != 5 {
        return Err("Invalid Credential format in Authorization header".to_string());
    }
    if parts[4] != "aws4_request" {
        return Err("Credential must end with aws4_request".to_string());
    }

    let credential_scope = format!("{}/{}/{}/{}", parts[1], parts[2], parts[3], parts[4]);

    Ok(ParsedAuthorization {
        access_key_id: parts[0].to_string(),
        date_stamp: parts[1].to_string(),
        region: parts[2].to_string(),
        service: parts[3].to_string(),
        signed_headers,
        signature,
        credential_scope,
    })
}

// ── Presigned URL parsing ───────────────────────────────────────────

/// Parse presigned URL query parameters.
fn parse_presigned_params(params: &BTreeMap<String, String>) -> Result<ParsedPresigned, String> {
    let algorithm = params
        .get("X-Amz-Algorithm")
        .ok_or("Missing X-Amz-Algorithm")?;
    if algorithm != "AWS4-HMAC-SHA256" {
        return Err(format!("Unsupported algorithm: {algorithm}"));
    }

    let credential_raw = params
        .get("X-Amz-Credential")
        .ok_or("Missing X-Amz-Credential")?;
    // Credential may be URL-encoded (slashes as %2F).
    let credential = percent_decode(credential_raw);
    let cred_parts: Vec<&str> = credential.splitn(5, '/').collect();
    if cred_parts.len() != 5 {
        return Err("Invalid X-Amz-Credential format".to_string());
    }
    if cred_parts[4] != "aws4_request" {
        return Err("X-Amz-Credential must end with aws4_request".to_string());
    }

    let amz_date = params
        .get("X-Amz-Date")
        .ok_or("Missing X-Amz-Date")?
        .to_string();

    let expires_str = params.get("X-Amz-Expires").ok_or("Missing X-Amz-Expires")?;
    let expires: u64 = expires_str
        .parse()
        .map_err(|_| "Invalid X-Amz-Expires value")?;
    if expires == 0 || expires > MAX_PRESIGNED_EXPIRES {
        return Err(format!(
            "X-Amz-Expires must be between 1 and {MAX_PRESIGNED_EXPIRES}"
        ));
    }

    let signed_headers = params
        .get("X-Amz-SignedHeaders")
        .ok_or("Missing X-Amz-SignedHeaders")?
        .to_string();

    let signature = params
        .get("X-Amz-Signature")
        .ok_or("Missing X-Amz-Signature")?
        .to_string();

    let credential_scope = format!(
        "{}/{}/{}/{}",
        cred_parts[1], cred_parts[2], cred_parts[3], cred_parts[4]
    );

    // Validate that credential date matches X-Amz-Date date portion.
    if amz_date.len() < 8 {
        return Err("X-Amz-Date too short".to_string());
    }
    if cred_parts[1] != &amz_date[..8] {
        return Err("Credential date does not match X-Amz-Date".to_string());
    }

    Ok(ParsedPresigned {
        access_key_id: cred_parts[0].to_string(),
        date_stamp: cred_parts[1].to_string(),
        region: cred_parts[2].to_string(),
        service: cred_parts[3].to_string(),
        credential_scope,
        signed_headers,
        signature,
        amz_date,
        expires,
    })
}

// ── Canonical request construction ──────────────────────────────────

/// Build the canonical request string.
///
/// ```text
/// HTTPMethod + '\n' +
/// CanonicalURI + '\n' +
/// CanonicalQueryString + '\n' +
/// CanonicalHeaders + '\n' +
/// SignedHeaders + '\n' +
/// HashedPayload
/// ```
pub fn build_canonical_request(
    method: &str,
    uri: &str,
    query_string: &str,
    headers: &[(String, String)],
    signed_headers_str: &str,
    payload_hash: &str,
) -> String {
    // Canonical URI: the path component. Empty path becomes "/".
    // For S3 (as opposed to general SigV4), the canonical URI uses the raw
    // path as-is without double-encoding or normalization. This matches
    // what boto3's S3SigV4Auth does (it overrides _normalize_url_path to a no-op).
    let canonical_uri = if uri.is_empty() { "/" } else { uri };

    // Build canonical query string: parse, sort, re-encode.
    let canonical_query = build_canonical_query_string(query_string);

    // Build canonical headers: only those listed in signed_headers.
    let signed_names: Vec<&str> = signed_headers_str.split(';').collect();
    let mut canonical_headers = String::new();
    for name in &signed_names {
        // Find the matching header value. Headers are already (lowercase name, value).
        for (hname, hval) in headers {
            if hname == name {
                canonical_headers.push_str(hname);
                canonical_headers.push(':');
                canonical_headers.push_str(&collapse_whitespace(hval));
                canonical_headers.push('\n');
                break;
            }
        }
    }

    format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers_str}\n{payload_hash}"
    )
}

/// Build the canonical query string from raw query string.
///
/// All query parameters are sorted by name (byte-order, case-sensitive),
/// each name and value URI-encoded, and joined with `&`.
/// The `X-Amz-Signature` parameter is excluded (for presigned URLs).
/// Parameters with no value use empty value: `acl=`.
///
/// IMPORTANT: The raw query string from the HTTP request is already
/// percent-encoded. We must first decode each name/value, then re-encode
/// with S3's URI encoding rules to produce the canonical form. This
/// prevents double-encoding.
pub fn build_canonical_query_string(query_string: &str) -> String {
    if query_string.is_empty() {
        return String::new();
    }

    let mut params: Vec<(String, String)> = Vec::new();
    for part in query_string.split('&') {
        if part.is_empty() {
            continue;
        }
        if let Some((k, v)) = part.split_once('=') {
            // Skip X-Amz-Signature for presigned URL signing.
            if k == "X-Amz-Signature" {
                continue;
            }
            // Decode first (query params may already be percent-encoded),
            // then re-encode with S3 rules to get canonical form.
            let decoded_k = percent_decode(k);
            let decoded_v = percent_decode(v);
            params.push((
                s3_uri_encode(&decoded_k, true),
                s3_uri_encode(&decoded_v, true),
            ));
        } else {
            // Parameter with no value (e.g., `?acl`).
            if part == "X-Amz-Signature" {
                continue;
            }
            let decoded = percent_decode(part);
            params.push((s3_uri_encode(&decoded, true), String::new()));
        }
    }

    // Sort by name, then by value.
    params.sort();

    params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

// ── String to sign ──────────────────────────────────────────────────

/// Build the string to sign.
///
/// ```text
/// AWS4-HMAC-SHA256 + '\n' +
/// Timestamp + '\n' +
/// CredentialScope + '\n' +
/// HexEncode(SHA256(CanonicalRequest))
/// ```
pub fn build_string_to_sign(
    timestamp: &str,
    credential_scope: &str,
    canonical_request: &str,
) -> String {
    let hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    format!("AWS4-HMAC-SHA256\n{timestamp}\n{credential_scope}\n{hash}")
}

// ── Signing key derivation ──────────────────────────────────────────

/// Derive the signing key for a given date, region, and service.
///
/// ```text
/// kDate    = HMAC-SHA256("AWS4" + secret, dateStamp)
/// kRegion  = HMAC-SHA256(kDate, region)
/// kService = HMAC-SHA256(kRegion, "s3")
/// kSigning = HMAC-SHA256(kService, "aws4_request")
/// ```
pub fn derive_signing_key(
    secret_key: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
) -> Vec<u8> {
    let k_secret = format!("AWS4{secret_key}");
    let k_date = hmac_sha256(k_secret.as_bytes(), date_stamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Compute HMAC-SHA256.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

// ── Signature computation ───────────────────────────────────────────

/// Compute the signature: HexEncode(HMAC-SHA256(SigningKey, StringToSign)).
pub fn compute_signature(signing_key: &[u8], string_to_sign: &str) -> String {
    let sig = hmac_sha256(signing_key, string_to_sign.as_bytes());
    hex::encode(sig)
}

/// Compare two signature strings in constant time.
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

// ── Full verification: header-based ─────────────────────────────────

/// Verify a header-based SigV4 request.
///
/// Arguments:
/// - `method`: HTTP method
/// - `uri`: Request URI path
/// - `query_string`: Raw query string (without leading `?`)
/// - `request_headers`: All request headers as (name, value) pairs
/// - `payload_hash`: The value of `x-amz-content-sha256` or computed body hash
/// - `parsed`: The parsed Authorization header
/// - `secret_key`: The secret key to verify against
pub fn verify_header_auth(
    method: &str,
    uri: &str,
    query_string: &str,
    request_headers: &[(String, String)],
    payload_hash: &str,
    parsed: &ParsedAuthorization,
    secret_key: &str,
) -> bool {
    let canonical_request = build_canonical_request(
        method,
        uri,
        query_string,
        request_headers,
        &parsed.signed_headers,
        payload_hash,
    );

    // Find the x-amz-date or date header for timestamp.
    let timestamp = find_header_value(request_headers, "x-amz-date")
        .or_else(|| find_header_value(request_headers, "date"))
        .unwrap_or_default();

    let string_to_sign =
        build_string_to_sign(timestamp, &parsed.credential_scope, &canonical_request);

    let signing_key = derive_signing_key(
        secret_key,
        &parsed.date_stamp,
        &parsed.region,
        &parsed.service,
    );

    let computed = compute_signature(&signing_key, &string_to_sign);
    constant_time_eq(&computed, &parsed.signature)
}

/// Verify a presigned URL request.
///
/// Arguments:
/// - `method`: HTTP method
/// - `uri`: Request URI path
/// - `query_string`: Raw query string (without leading `?`)
/// - `request_headers`: All request headers as (name, value) pairs
/// - `parsed`: The parsed presigned URL parameters
/// - `secret_key`: The secret key to verify against
pub fn verify_presigned_auth(
    method: &str,
    uri: &str,
    query_string: &str,
    request_headers: &[(String, String)],
    parsed: &ParsedPresigned,
    secret_key: &str,
) -> bool {
    // For presigned URLs, the payload hash is always UNSIGNED-PAYLOAD.
    let payload_hash = "UNSIGNED-PAYLOAD";

    let canonical_request = build_canonical_request(
        method,
        uri,
        query_string,
        request_headers,
        &parsed.signed_headers,
        payload_hash,
    );

    let string_to_sign = build_string_to_sign(
        &parsed.amz_date,
        &parsed.credential_scope,
        &canonical_request,
    );

    let signing_key = derive_signing_key(
        secret_key,
        &parsed.date_stamp,
        &parsed.region,
        &parsed.service,
    );

    let computed = compute_signature(&signing_key, &string_to_sign);
    constant_time_eq(&computed, &parsed.signature)
}

/// Check whether a presigned URL has expired.
///
/// Returns true if the URL has NOT expired (still valid).
pub fn check_presigned_expiration(amz_date: &str, expires_seconds: u64) -> bool {
    let sign_time = match parse_amz_date(amz_date) {
        Some(t) => t,
        None => return false,
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    now <= sign_time + expires_seconds
}

/// Check whether a header-based request is within clock skew tolerance.
///
/// Returns true if the request is within tolerance (valid).
pub fn check_clock_skew(amz_date: &str) -> bool {
    let req_time = match parse_amz_date(amz_date) {
        Some(t) => t,
        None => return false,
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let diff = now.abs_diff(req_time);

    diff <= CLOCK_SKEW_SECONDS
}

// ── URI encoding ────────────────────────────────────────────────────

/// S3-compatible URI encoding (RFC 3986 with S3 exceptions).
///
/// - Characters A-Z, a-z, 0-9, -, _, ., ~ are NOT encoded.
/// - All other characters are percent-encoded with uppercase hex.
/// - If `encode_slash` is false, `/` is NOT encoded (for URI paths).
/// - If `encode_slash` is true, `/` is encoded as `%2F` (for query params).
pub fn s3_uri_encode(input: &str, encode_slash: bool) -> String {
    let mut encoded = String::with_capacity(input.len() * 2);
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' || ch == '~' {
            encoded.push(ch);
        } else if ch == '/' && !encode_slash {
            encoded.push('/');
        } else {
            // Percent-encode each byte of the UTF-8 representation.
            for byte in ch.to_string().as_bytes() {
                encoded.push_str(&format!("%{byte:02X}"));
            }
        }
    }
    encoded
}

/// URI-encode a path for S3 canonical requests.
///
/// The raw path from the HTTP request may already be percent-encoded.
/// We decode it first, then re-encode each path segment (preserving `/`).
/// This ensures consistent canonical form regardless of how the client
/// encoded the path.
pub fn s3_uri_encode_path(raw_path: &str) -> String {
    if raw_path.is_empty() || raw_path == "/" {
        return "/".to_string();
    }
    // Decode the entire path first to undo any existing percent-encoding.
    let decoded = percent_decode(raw_path);
    // Split by `/`, encode each segment individually, rejoin.
    let segments: Vec<String> = decoded
        .split('/')
        .map(|seg| s3_uri_encode(seg, false))
        .collect();
    let result = segments.join("/");
    if result.starts_with('/') {
        result
    } else {
        format!("/{result}")
    }
}

// ── Helper functions ────────────────────────────────────────────────

/// Parse a raw query string into a BTreeMap (preserving order for sorting).
pub fn parse_query_string(query: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    if query.is_empty() {
        return map;
    }
    for part in query.split('&') {
        if part.is_empty() {
            continue;
        }
        if let Some((k, v)) = part.split_once('=') {
            map.insert(k.to_string(), v.to_string());
        } else {
            map.insert(part.to_string(), String::new());
        }
    }
    map
}

/// Collapse consecutive whitespace in a header value to a single space,
/// and trim leading/trailing whitespace.
fn collapse_whitespace(s: &str) -> String {
    let trimmed = s.trim();
    let mut result = String::with_capacity(trimmed.len());
    let mut last_was_space = false;
    for ch in trimmed.chars() {
        if ch.is_whitespace() {
            if !last_was_space {
                result.push(' ');
                last_was_space = true;
            }
        } else {
            result.push(ch);
            last_was_space = false;
        }
    }
    result
}

/// Find a header value by lowercase name from a list of (name, value) pairs.
fn find_header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v.as_str())
}

/// Parse an X-Amz-Date string (YYYYMMDDTHHMMSSZ) into Unix timestamp.
fn parse_amz_date(date: &str) -> Option<u64> {
    // Format: YYYYMMDDTHHMMSSZ (16 chars)
    if date.len() != 16 || !date.ends_with('Z') || date.as_bytes()[8] != b'T' {
        return None;
    }

    let year: u64 = date[0..4].parse().ok()?;
    let month: u64 = date[4..6].parse().ok()?;
    let day: u64 = date[6..8].parse().ok()?;
    let hour: u64 = date[9..11].parse().ok()?;
    let min: u64 = date[11..13].parse().ok()?;
    let sec: u64 = date[13..15].parse().ok()?;

    // Simple conversion to Unix timestamp.
    // Days from year.
    let mut days: u64 = 0;
    for y in 1970..year {
        days += if is_leap_year(y) { 366 } else { 365 };
    }
    // Days from month.
    let month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for (m, &md) in month_days.iter().enumerate().take(month as usize - 1) {
        days += md as u64;
        if m == 1 && is_leap_year(year) {
            days += 1;
        }
    }
    // Days from day.
    days += day - 1;

    Some(days * 86400 + hour * 3600 + min * 60 + sec)
}

/// Check if a year is a leap year.
fn is_leap_year(year: u64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Simple percent-decoding.
fn percent_decode(s: &str) -> String {
    let mut result = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) =
                u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""), 16)
            {
                result.push(byte);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&result).to_string()
}

/// Extract headers from an axum HeaderMap as sorted (lowercase-name, trimmed-value) pairs.
pub fn extract_headers_for_signing(header_map: &axum::http::HeaderMap) -> Vec<(String, String)> {
    let mut headers: Vec<(String, String)> = Vec::new();

    // Collect all headers, grouping multiple values for the same name.
    let mut header_values: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in header_map.iter() {
        let name_lower = name.as_str().to_lowercase();
        let val_str = value.to_str().unwrap_or("").to_string();
        header_values.entry(name_lower).or_default().push(val_str);
    }

    // Join multiple values for the same header with comma.
    for (name, values) in header_values {
        let joined = values.join(",");
        headers.push((name, joined));
    }

    headers
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── derive_signing_key ──────────────────────────────────────────

    #[test]
    fn test_derive_signing_key() {
        // Using AWS example credentials to ensure the HMAC chain works correctly.
        let key = derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
            "s3",
        );
        // The signing key is a 32-byte HMAC-SHA256 value.
        assert_eq!(key.len(), 32);
        // Verify the key is deterministic (same inputs produce same output).
        let key2 = derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
            "s3",
        );
        assert_eq!(key, key2);

        // Verify by manually computing the HMAC chain.
        let secret = "AWS4wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let k_date = hmac_sha256(secret.as_bytes(), b"20130524");
        let k_region = hmac_sha256(&k_date, b"us-east-1");
        let k_service = hmac_sha256(&k_region, b"s3");
        let expected = hmac_sha256(&k_service, b"aws4_request");
        assert_eq!(key, expected);
    }

    #[test]
    fn test_derive_signing_key_different_date() {
        let key1 = derive_signing_key("secret", "20260222", "us-east-1", "s3");
        let key2 = derive_signing_key("secret", "20260223", "us-east-1", "s3");
        // Different dates should produce different keys.
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_derive_signing_key_different_region() {
        let key1 = derive_signing_key("secret", "20260222", "us-east-1", "s3");
        let key2 = derive_signing_key("secret", "20260222", "eu-west-1", "s3");
        assert_ne!(key1, key2);
    }

    // ── s3_uri_encode ───────────────────────────────────────────────

    #[test]
    fn test_uri_encode_unreserved() {
        assert_eq!(s3_uri_encode("hello", true), "hello");
        assert_eq!(s3_uri_encode("A-Z_a-z.0~9", true), "A-Z_a-z.0~9");
    }

    #[test]
    fn test_uri_encode_spaces() {
        assert_eq!(s3_uri_encode("hello world", true), "hello%20world");
    }

    #[test]
    fn test_uri_encode_slash() {
        assert_eq!(s3_uri_encode("path/to/key", true), "path%2Fto%2Fkey");
        assert_eq!(s3_uri_encode("path/to/key", false), "path/to/key");
    }

    #[test]
    fn test_uri_encode_special() {
        assert_eq!(s3_uri_encode("foo=bar&baz", true), "foo%3Dbar%26baz");
    }

    // ── parse_authorization_header ──────────────────────────────────

    #[test]
    fn test_parse_authorization_header() {
        let header = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260222/us-east-1/s3/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let parsed = parse_authorization_header(header).unwrap();
        assert_eq!(parsed.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(parsed.date_stamp, "20260222");
        assert_eq!(parsed.region, "us-east-1");
        assert_eq!(parsed.service, "s3");
        assert_eq!(
            parsed.signed_headers,
            "content-type;host;x-amz-content-sha256;x-amz-date"
        );
        assert_eq!(
            parsed.signature,
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        );
        assert_eq!(
            parsed.credential_scope,
            "20260222/us-east-1/s3/aws4_request"
        );
    }

    #[test]
    fn test_parse_authorization_header_missing_credential() {
        let header = "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=abc";
        assert!(parse_authorization_header(header).is_err());
    }

    #[test]
    fn test_parse_authorization_header_bad_prefix() {
        let header = "AWS4-HMAC-SHA512 Credential=x/20260222/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc";
        assert!(parse_authorization_header(header).is_err());
    }

    // ── build_canonical_query_string ────────────────────────────────

    #[test]
    fn test_canonical_query_empty() {
        assert_eq!(build_canonical_query_string(""), "");
    }

    #[test]
    fn test_canonical_query_sorted() {
        assert_eq!(build_canonical_query_string("z=3&a=1&m=2"), "a=1&m=2&z=3");
    }

    #[test]
    fn test_canonical_query_no_value() {
        assert_eq!(build_canonical_query_string("acl"), "acl=");
    }

    #[test]
    fn test_canonical_query_excludes_signature() {
        assert_eq!(
            build_canonical_query_string("a=1&X-Amz-Signature=abc&b=2"),
            "a=1&b=2"
        );
    }

    // ── build_canonical_request ─────────────────────────────────────

    #[test]
    fn test_build_canonical_request() {
        let headers = vec![
            ("host".to_string(), "mybucket.s3.amazonaws.com".to_string()),
            (
                "x-amz-content-sha256".to_string(),
                "UNSIGNED-PAYLOAD".to_string(),
            ),
            ("x-amz-date".to_string(), "20260222T120000Z".to_string()),
        ];
        let result = build_canonical_request(
            "GET",
            "/",
            "",
            &headers,
            "host;x-amz-content-sha256;x-amz-date",
            "UNSIGNED-PAYLOAD",
        );
        assert!(result.starts_with("GET\n/\n\n"));
        assert!(result.contains("host:mybucket.s3.amazonaws.com\n"));
        assert!(result.ends_with("UNSIGNED-PAYLOAD"));
    }

    // ── build_string_to_sign ────────────────────────────────────────

    #[test]
    fn test_build_string_to_sign() {
        let canonical = "GET\n/\n\nhost:example.com\n\nhost\nUNSIGNED-PAYLOAD";
        let result = build_string_to_sign(
            "20260222T120000Z",
            "20260222/us-east-1/s3/aws4_request",
            canonical,
        );
        assert!(result.starts_with("AWS4-HMAC-SHA256\n20260222T120000Z\n"));
    }

    // ── constant_time_eq ────────────────────────────────────────────

    #[test]
    fn test_constant_time_eq_same() {
        assert!(constant_time_eq("abc123", "abc123"));
    }

    #[test]
    fn test_constant_time_eq_different() {
        assert!(!constant_time_eq("abc123", "abc124"));
    }

    #[test]
    fn test_constant_time_eq_different_length() {
        assert!(!constant_time_eq("abc", "abcd"));
    }

    // ── parse_amz_date ──────────────────────────────────────────────

    #[test]
    fn test_parse_amz_date() {
        // 2026-02-22T12:00:00Z
        let ts = parse_amz_date("20260222T120000Z").unwrap();
        // Just verify it's a reasonable value (around 1771848000).
        assert!(ts > 1700000000);
        assert!(ts < 1800000000);
    }

    #[test]
    fn test_parse_amz_date_epoch() {
        let ts = parse_amz_date("19700101T000000Z").unwrap();
        assert_eq!(ts, 0);
    }

    #[test]
    fn test_parse_amz_date_invalid() {
        assert!(parse_amz_date("not-a-date").is_none());
        assert!(parse_amz_date("").is_none());
    }

    // ── collapse_whitespace ─────────────────────────────────────────

    #[test]
    fn test_collapse_whitespace() {
        assert_eq!(collapse_whitespace("  hello   world  "), "hello world");
        assert_eq!(collapse_whitespace("no-extra"), "no-extra");
    }

    // ── detect_auth_type ────────────────────────────────────────────

    #[test]
    fn test_detect_auth_type_none() {
        let result = detect_auth_type(None, "").unwrap();
        assert!(matches!(result, AuthType::None));
    }

    #[test]
    fn test_detect_auth_type_header() {
        let header = "AWS4-HMAC-SHA256 Credential=AKID/20260222/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc123";
        let result = detect_auth_type(Some(header), "").unwrap();
        assert!(matches!(result, AuthType::Header(_)));
    }

    #[test]
    fn test_detect_auth_type_presigned() {
        let qs = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F20260222%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260222T120000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=abc123";
        let result = detect_auth_type(None, qs).unwrap();
        assert!(matches!(result, AuthType::Presigned(_)));
    }

    #[test]
    fn test_detect_auth_type_both_is_error() {
        let header = "AWS4-HMAC-SHA256 Credential=AKID/20260222/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc123";
        let qs = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID/20260222/us-east-1/s3/aws4_request&X-Amz-Date=20260222T120000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=abc123";
        let result = detect_auth_type(Some(header), qs);
        assert!(result.is_err());
    }

    // ── Full signature verification (header-based) ──────────────────

    #[test]
    fn test_verify_header_auth_roundtrip() {
        // Simulate a GET / request signed with known credentials.
        let secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let date_stamp = "20130524";
        let region = "us-east-1";
        let service = "s3";
        let timestamp = "20130524T000000Z";
        let payload_hash = "UNSIGNED-PAYLOAD";

        let headers = vec![
            (
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            ),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("x-amz-date".to_string(), timestamp.to_string()),
        ];

        let signed_headers = "host;x-amz-content-sha256;x-amz-date";

        let canonical_request =
            build_canonical_request("GET", "/", "", &headers, signed_headers, payload_hash);

        let credential_scope = format!("{date_stamp}/{region}/{service}/aws4_request");
        let string_to_sign = build_string_to_sign(timestamp, &credential_scope, &canonical_request);
        let signing_key = derive_signing_key(secret, date_stamp, region, service);
        let signature = compute_signature(&signing_key, &string_to_sign);

        let parsed = ParsedAuthorization {
            access_key_id: access_key.to_string(),
            date_stamp: date_stamp.to_string(),
            region: region.to_string(),
            service: service.to_string(),
            signed_headers: signed_headers.to_string(),
            signature,
            credential_scope,
        };

        assert!(verify_header_auth(
            "GET",
            "/",
            "",
            &headers,
            payload_hash,
            &parsed,
            secret,
        ));
    }

    #[test]
    fn test_verify_header_auth_wrong_secret() {
        let secret = "correct-secret";
        let date_stamp = "20260222";
        let region = "us-east-1";
        let service = "s3";
        let timestamp = "20260222T120000Z";
        let payload_hash = "UNSIGNED-PAYLOAD";

        let headers = vec![
            ("host".to_string(), "localhost:9012".to_string()),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("x-amz-date".to_string(), timestamp.to_string()),
        ];

        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_request =
            build_canonical_request("GET", "/", "", &headers, signed_headers, payload_hash);
        let credential_scope = format!("{date_stamp}/{region}/{service}/aws4_request");
        let string_to_sign = build_string_to_sign(timestamp, &credential_scope, &canonical_request);
        let signing_key = derive_signing_key(secret, date_stamp, region, service);
        let signature = compute_signature(&signing_key, &string_to_sign);

        let parsed = ParsedAuthorization {
            access_key_id: "test".to_string(),
            date_stamp: date_stamp.to_string(),
            region: region.to_string(),
            service: service.to_string(),
            signed_headers: signed_headers.to_string(),
            signature,
            credential_scope,
        };

        // Verify with wrong secret should fail.
        assert!(!verify_header_auth(
            "GET",
            "/",
            "",
            &headers,
            payload_hash,
            &parsed,
            "wrong-secret"
        ));
    }

    // ── percent_decode ──────────────────────────────────────────────

    #[test]
    fn test_percent_decode() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("a%2Fb%2Fc"), "a/b/c");
        assert_eq!(percent_decode("no-encoding"), "no-encoding");
    }

    // ── Full presigned verification roundtrip ───────────────────────

    #[test]
    fn test_verify_presigned_roundtrip() {
        let secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let date_stamp = "20260222";
        let region = "us-east-1";
        let service = "s3";
        let timestamp = "20260222T120000Z";

        let headers = vec![("host".to_string(), "mybucket.s3.amazonaws.com".to_string())];

        let signed_headers = "host";

        // Build the query string for signing (without X-Amz-Signature).
        let credential = format!("AKID/{date_stamp}/{region}/{service}/aws4_request");
        let qs = format!(
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}&X-Amz-Date={timestamp}&X-Amz-Expires=3600&X-Amz-SignedHeaders={signed_headers}",
            s3_uri_encode(&credential, true)
        );

        let canonical_request = build_canonical_request(
            "GET",
            "/test-key",
            &qs,
            &headers,
            signed_headers,
            "UNSIGNED-PAYLOAD",
        );

        let credential_scope = format!("{date_stamp}/{region}/{service}/aws4_request");
        let string_to_sign = build_string_to_sign(timestamp, &credential_scope, &canonical_request);
        let signing_key = derive_signing_key(secret, date_stamp, region, service);
        let signature = compute_signature(&signing_key, &string_to_sign);

        // Now verify with the full query string (including X-Amz-Signature).
        let full_qs = format!("{qs}&X-Amz-Signature={signature}");

        let parsed = ParsedPresigned {
            access_key_id: "AKID".to_string(),
            date_stamp: date_stamp.to_string(),
            region: region.to_string(),
            service: service.to_string(),
            credential_scope,
            signed_headers: signed_headers.to_string(),
            signature: signature.clone(),
            amz_date: timestamp.to_string(),
            expires: 3600,
        };

        assert!(verify_presigned_auth(
            "GET",
            "/test-key",
            &full_qs,
            &headers,
            &parsed,
            secret
        ));
    }
}
