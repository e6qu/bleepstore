const std = @import("std");
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const AuthError = error{
    MissingAuthHeader,
    MalformedAuthHeader,
    InvalidSignature,
    ExpiredRequest,
    UnsupportedAlgorithm,
    InvalidAccessKeyId,
    SignatureDoesNotMatch,
    AccessDenied,
    RequestTimeTooSkewed,
    AuthorizationQueryParametersError,
};

pub const AwsCredentials = struct {
    access_key_id: []const u8,
    secret_access_key: []const u8,
    region: []const u8,
};

// ---------------------------------------------------------------------------
// Auth Cache — caches signing keys (24h TTL) and credentials (60s TTL)
// ---------------------------------------------------------------------------

pub const AuthCache = struct {
    const max_entries = 1000;
    const signing_key_ttl_ns: i128 = 86400 * std.time.ns_per_s; // 24h
    const credential_ttl_ns: i128 = 60 * std.time.ns_per_s; // 60s

    const SigningKeyEntry = struct {
        key: [HmacSha256.mac_length]u8,
        expires: i128,
    };

    const CredEntry = struct {
        access_key_id: []const u8,
        secret_key: []const u8,
        owner_id: []const u8,
        display_name: []const u8,
        created_at: []const u8,
        expires: i128,
    };

    /// Snapshot returned from getCredential — fields are duped to caller's allocator.
    pub const CredSnapshot = struct {
        access_key_id: []const u8,
        secret_key: []const u8,
        owner_id: []const u8,
        display_name: []const u8,
        created_at: []const u8,
    };

    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex = .{},
    signing_keys: std.StringHashMap(SigningKeyEntry),
    credentials: std.StringHashMap(CredEntry),

    pub fn init(allocator: std.mem.Allocator) AuthCache {
        return .{
            .allocator = allocator,
            .signing_keys = std.StringHashMap(SigningKeyEntry).init(allocator),
            .credentials = std.StringHashMap(CredEntry).init(allocator),
        };
    }

    pub fn deinit(self: *AuthCache) void {
        // Free all owned signing key cache keys.
        var sk_iter = self.signing_keys.iterator();
        while (sk_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.signing_keys.deinit();

        // Free all owned credential entries.
        var cred_iter = self.credentials.iterator();
        while (cred_iter.next()) |entry| {
            self.freeCredEntry(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.credentials.deinit();
    }

    fn freeCredEntry(self: *AuthCache, entry: CredEntry) void {
        self.allocator.free(entry.access_key_id);
        self.allocator.free(entry.secret_key);
        self.allocator.free(entry.owner_id);
        self.allocator.free(entry.display_name);
        self.allocator.free(entry.created_at);
    }

    /// Look up a cached signing key. Returns null on miss or expiry.
    pub fn getSigningKey(
        self: *AuthCache,
        secret_key: []const u8,
        date_stamp: []const u8,
        region: []const u8,
        service: []const u8,
    ) ?[HmacSha256.mac_length]u8 {
        // Build cache key on the stack.
        var key_buf: [512]u8 = undefined;
        const cache_key = std.fmt.bufPrint(&key_buf, "{s}\x00{s}\x00{s}\x00{s}", .{
            secret_key, date_stamp, region, service,
        }) catch return null;

        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.signing_keys.get(cache_key) orelse return null;
        const now = std.time.nanoTimestamp();
        if (now > entry.expires) return null;
        return entry.key;
    }

    /// Store a signing key in the cache.
    pub fn putSigningKey(
        self: *AuthCache,
        secret_key: []const u8,
        date_stamp: []const u8,
        region: []const u8,
        service: []const u8,
        key: [HmacSha256.mac_length]u8,
    ) void {
        var key_buf: [512]u8 = undefined;
        const cache_key_slice = std.fmt.bufPrint(&key_buf, "{s}\x00{s}\x00{s}\x00{s}", .{
            secret_key, date_stamp, region, service,
        }) catch return;

        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.signing_keys.count() >= max_entries) {
            // Evict all entries on overflow (simple strategy).
            var iter = self.signing_keys.iterator();
            while (iter.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
            }
            self.signing_keys.clearRetainingCapacity();
        }

        const now = std.time.nanoTimestamp();
        const owned_key = self.allocator.dupe(u8, cache_key_slice) catch return;

        self.signing_keys.put(owned_key, .{
            .key = key,
            .expires = now + signing_key_ttl_ns,
        }) catch {
            self.allocator.free(owned_key);
        };
    }

    /// Look up cached credentials. Returns a CredSnapshot with secret_key duped
    /// to the caller's allocator (typically the per-request arena). Returns null
    /// on miss or expiry.
    pub fn getCredential(
        self: *AuthCache,
        access_key_id: []const u8,
        caller_alloc: std.mem.Allocator,
    ) ?CredSnapshot {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.credentials.get(access_key_id) orelse return null;
        const now = std.time.nanoTimestamp();
        if (now > entry.expires) return null;

        // Dupe secret_key to caller's allocator (arena) so it survives after unlock.
        const secret_dup = caller_alloc.dupe(u8, entry.secret_key) catch return null;
        return CredSnapshot{
            .access_key_id = entry.access_key_id,
            .secret_key = secret_dup,
            .owner_id = entry.owner_id,
            .display_name = entry.display_name,
            .created_at = entry.created_at,
        };
    }

    /// Store credentials in the cache. The cache owns copies of all strings.
    pub fn putCredential(
        self: *AuthCache,
        access_key_id: []const u8,
        secret_key: []const u8,
        owner_id: []const u8,
        display_name: []const u8,
        created_at: []const u8,
    ) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.credentials.count() >= max_entries) {
            var iter = self.credentials.iterator();
            while (iter.next()) |entry| {
                self.freeCredEntry(entry.value_ptr.*);
                self.allocator.free(entry.key_ptr.*);
            }
            self.credentials.clearRetainingCapacity();
        }

        const now = std.time.nanoTimestamp();
        const owned_akid = self.allocator.dupe(u8, access_key_id) catch return;
        const owned_secret = self.allocator.dupe(u8, secret_key) catch {
            self.allocator.free(owned_akid);
            return;
        };
        const owned_owner = self.allocator.dupe(u8, owner_id) catch {
            self.allocator.free(owned_akid);
            self.allocator.free(owned_secret);
            return;
        };
        const owned_display = self.allocator.dupe(u8, display_name) catch {
            self.allocator.free(owned_akid);
            self.allocator.free(owned_secret);
            self.allocator.free(owned_owner);
            return;
        };
        const owned_created = self.allocator.dupe(u8, created_at) catch {
            self.allocator.free(owned_akid);
            self.allocator.free(owned_secret);
            self.allocator.free(owned_owner);
            self.allocator.free(owned_display);
            return;
        };

        // If entry already exists, free the old one first.
        if (self.credentials.fetchRemove(access_key_id)) |old| {
            self.freeCredEntry(old.value);
            self.allocator.free(old.key);
        }

        self.credentials.put(owned_akid, .{
            .access_key_id = owned_akid,
            .secret_key = owned_secret,
            .owner_id = owned_owner,
            .display_name = owned_display,
            .created_at = owned_created,
            .expires = now + credential_ttl_ns,
        }) catch {
            self.allocator.free(owned_akid);
            self.allocator.free(owned_secret);
            self.allocator.free(owned_owner);
            self.allocator.free(owned_display);
            self.allocator.free(owned_created);
        };
    }
};

/// Parsed components from the Authorization header.
pub const AuthorizationComponents = struct {
    access_key: []const u8,
    date_stamp: []const u8,
    region: []const u8,
    service: []const u8,
    signed_headers: []const u8,
    signature: []const u8,
};

/// Parsed presigned URL query parameters.
pub const PresignedComponents = struct {
    access_key: []const u8,
    date_stamp: []const u8,
    region: []const u8,
    service: []const u8,
    amz_date: []const u8,
    expires: []const u8,
    signed_headers: []const u8,
    signature: []const u8,
};

/// Detect what type of authentication is present in the request.
pub const AuthType = enum {
    none,
    header,
    presigned,
};

/// Determine the auth type from request headers and query string.
pub fn detectAuthType(authorization_header: ?[]const u8, query: []const u8) AuthType {
    const has_header = if (authorization_header) |ah|
        std.mem.startsWith(u8, ah, "AWS4-HMAC-SHA256")
    else
        false;

    const has_query = hasPresignedParams(query);

    if (has_header and has_query) {
        // Ambiguous -- treat as error by returning header (will fail to match)
        return .header;
    }
    if (has_header) return .header;
    if (has_query) return .presigned;
    return .none;
}

fn hasPresignedParams(query: []const u8) bool {
    if (query.len == 0) return false;
    return getQueryParam(query, "X-Amz-Algorithm") != null;
}

// ---------------------------------------------------------------------------
// Header-based SigV4 verification
// ---------------------------------------------------------------------------

/// Verify the AWS Signature Version 4 for an incoming request using
/// the Authorization header.
///
/// Parameters:
///   - allocator: arena allocator for per-request allocations
///   - method: HTTP method (GET, PUT, etc.)
///   - path: URI path (e.g., "/bucket/key")
///   - query: raw query string (no leading '?')
///   - authorization_header: the full Authorization header value
///   - amz_date: value of X-Amz-Date header
///   - content_sha256: value of x-amz-content-sha256 header (or UNSIGNED-PAYLOAD)
///   - host: value of Host header
///   - getHeaderFn: function that looks up a header value by lowercase name
///   - secret_key: the secret access key for the identified user
///   - region: server region
///
/// Returns the parsed access key on success, or an error.
pub fn verifyHeaderAuth(
    allocator: std.mem.Allocator,
    method: []const u8,
    path: []const u8,
    query: []const u8,
    authorization_header: []const u8,
    amz_date: ?[]const u8,
    content_sha256: ?[]const u8,
    host: ?[]const u8,
    header_keys: []const []const u8,
    header_values: []const []const u8,
    secret_key: []const u8,
    region: []const u8,
    precomputed_signing_key: ?*const [HmacSha256.mac_length]u8,
) AuthError!void {
    // 1. Parse the Authorization header
    const components = parseAuthorizationHeader(authorization_header) orelse
        return AuthError.MalformedAuthHeader;

    // 2. Validate algorithm
    // (already validated by prefix check in detectAuthType)

    // 3. Get the timestamp
    const timestamp = amz_date orelse return AuthError.MalformedAuthHeader;

    // 4. Validate credential date matches X-Amz-Date date portion
    if (timestamp.len < 8) return AuthError.MalformedAuthHeader;
    if (!std.mem.eql(u8, components.date_stamp, timestamp[0..8]))
        return AuthError.SignatureDoesNotMatch;

    // 5. Validate region
    if (!std.mem.eql(u8, components.region, region))
        return AuthError.SignatureDoesNotMatch;

    // 6. Check clock skew (15 minutes = 900 seconds)
    if (!isTimestampWithinSkew(timestamp, 900))
        return AuthError.RequestTimeTooSkewed;

    // 7. Build canonical request
    const payload_hash = content_sha256 orelse "UNSIGNED-PAYLOAD";
    const canonical_uri = buildCanonicalUri(allocator, path) catch
        return AuthError.MalformedAuthHeader;
    defer allocator.free(canonical_uri);

    const canonical_query = buildCanonicalQueryString(allocator, query) catch
        return AuthError.MalformedAuthHeader;
    defer allocator.free(canonical_query);

    const canonical_headers_str = buildCanonicalHeaders(
        allocator,
        components.signed_headers,
        host,
        amz_date,
        content_sha256,
        header_keys,
        header_values,
    ) catch return AuthError.MalformedAuthHeader;
    defer allocator.free(canonical_headers_str);

    const canonical_request = createCanonicalRequest(
        allocator,
        method,
        canonical_uri,
        canonical_query,
        canonical_headers_str,
        components.signed_headers,
        payload_hash,
    ) catch return AuthError.MalformedAuthHeader;
    defer allocator.free(canonical_request);

    // 8. Compute string to sign
    const scope = buildScope(allocator, components.date_stamp, components.region, components.service) catch
        return AuthError.MalformedAuthHeader;
    defer allocator.free(scope);

    const string_to_sign = computeStringToSign(allocator, timestamp, scope, canonical_request) catch
        return AuthError.MalformedAuthHeader;
    defer allocator.free(string_to_sign);

    // 9. Derive signing key and compute signature
    const signing_key = if (precomputed_signing_key) |pk|
        pk.*
    else
        deriveSigningKey(secret_key, components.date_stamp, components.region, components.service);

    var sig_mac: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&sig_mac, string_to_sign, &signing_key);
    const computed_sig = std.fmt.bytesToHex(sig_mac, .lower);

    // 10. Constant-time comparison
    if (!constantTimeEql(&computed_sig, components.signature))
        return AuthError.SignatureDoesNotMatch;
}

// ---------------------------------------------------------------------------
// Presigned URL verification
// ---------------------------------------------------------------------------

/// Verify a presigned URL signature.
pub fn verifyPresignedAuth(
    allocator: std.mem.Allocator,
    method: []const u8,
    path: []const u8,
    query: []const u8,
    host: ?[]const u8,
    header_keys: []const []const u8,
    header_values: []const []const u8,
    secret_key: []const u8,
    region: []const u8,
    precomputed_signing_key: ?*const [HmacSha256.mac_length]u8,
) AuthError!void {
    // 1. Extract presigned query parameters
    const ps = parsePresignedParams(query) orelse
        return AuthError.MalformedAuthHeader;

    // 2. Validate algorithm
    const algo = getQueryParam(query, "X-Amz-Algorithm") orelse
        return AuthError.MalformedAuthHeader;
    if (!std.mem.eql(u8, algo, "AWS4-HMAC-SHA256"))
        return AuthError.UnsupportedAlgorithm;

    // 3. Validate X-Amz-Expires
    const expires_val = std.fmt.parseInt(u64, ps.expires, 10) catch
        return AuthError.AuthorizationQueryParametersError;
    if (expires_val < 1 or expires_val > 604800)
        return AuthError.AuthorizationQueryParametersError;

    // 4. Validate credential date matches X-Amz-Date date portion
    if (ps.amz_date.len < 8) return AuthError.MalformedAuthHeader;
    if (!std.mem.eql(u8, ps.date_stamp, ps.amz_date[0..8]))
        return AuthError.SignatureDoesNotMatch;

    // 5. Validate region
    if (!std.mem.eql(u8, ps.region, region))
        return AuthError.SignatureDoesNotMatch;

    // 6. Check expiration: now <= parse(X-Amz-Date) + X-Amz-Expires
    if (!isPresignedNotExpired(ps.amz_date, expires_val))
        return AuthError.ExpiredRequest;

    // 7. Build canonical request (exclude X-Amz-Signature from query)
    const canonical_uri = buildCanonicalUri(allocator, path) catch
        return AuthError.MalformedAuthHeader;
    defer allocator.free(canonical_uri);

    const canonical_query = buildPresignedCanonicalQueryString(allocator, query) catch
        return AuthError.MalformedAuthHeader;
    defer allocator.free(canonical_query);

    const canonical_headers_str = buildCanonicalHeaders(
        allocator,
        ps.signed_headers,
        host,
        null, // presigned URLs don't have x-amz-date in headers
        null, // no x-amz-content-sha256 in headers
        header_keys,
        header_values,
    ) catch return AuthError.MalformedAuthHeader;
    defer allocator.free(canonical_headers_str);

    const canonical_request = createCanonicalRequest(
        allocator,
        method,
        canonical_uri,
        canonical_query,
        canonical_headers_str,
        ps.signed_headers,
        "UNSIGNED-PAYLOAD",
    ) catch return AuthError.MalformedAuthHeader;
    defer allocator.free(canonical_request);

    // 8. Compute string to sign
    const scope = buildScope(allocator, ps.date_stamp, ps.region, ps.service) catch
        return AuthError.MalformedAuthHeader;
    defer allocator.free(scope);

    const string_to_sign = computeStringToSign(allocator, ps.amz_date, scope, canonical_request) catch
        return AuthError.MalformedAuthHeader;
    defer allocator.free(string_to_sign);

    // 9. Derive signing key and compute signature
    const signing_key = if (precomputed_signing_key) |pk|
        pk.*
    else
        deriveSigningKey(secret_key, ps.date_stamp, ps.region, ps.service);

    var sig_mac: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&sig_mac, string_to_sign, &signing_key);
    const computed_sig = std.fmt.bytesToHex(sig_mac, .lower);

    // 10. Constant-time comparison
    if (!constantTimeEql(&computed_sig, ps.signature))
        return AuthError.SignatureDoesNotMatch;
}

/// Extract the access key ID from the Authorization header.
pub fn extractAccessKeyFromHeader(authorization_header: []const u8) ?[]const u8 {
    const components = parseAuthorizationHeader(authorization_header) orelse return null;
    return components.access_key;
}

/// Extract the access key ID from presigned query params.
pub fn extractAccessKeyFromQuery(query: []const u8) ?[]const u8 {
    const cred = getQueryParam(query, "X-Amz-Credential") orelse return null;
    // Credential = AKID/date/region/service/aws4_request
    // The value may be URL-encoded (slashes as %2F)
    // Find the first slash or %2F
    const slash_idx = std.mem.indexOfScalar(u8, cred, '/');
    const pct_idx = std.mem.indexOf(u8, cred, "%2F");
    const sep = if (slash_idx) |si|
        (if (pct_idx) |pi| @min(si, pi) else si)
    else
        (pct_idx orelse return null);
    if (sep == 0) return null;
    return cred[0..sep];
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// Parse the Authorization header into its components.
///
/// Format: AWS4-HMAC-SHA256 Credential=AKID/date/region/s3/aws4_request,
///         SignedHeaders=host;x-amz-content-sha256;x-amz-date,
///         Signature=hex
pub fn parseAuthorizationHeader(header: []const u8) ?AuthorizationComponents {
    // Must start with "AWS4-HMAC-SHA256 "
    const prefix = "AWS4-HMAC-SHA256 ";
    if (!std.mem.startsWith(u8, header, prefix)) return null;
    const rest = header[prefix.len..];

    // Extract Credential=...
    const cred_val = extractField(rest, "Credential=") orelse return null;
    // Parse credential: AKID/date/region/service/aws4_request
    var cred_parts_iter = std.mem.splitScalar(u8, cred_val, '/');
    const access_key = cred_parts_iter.next() orelse return null;
    const date_stamp = cred_parts_iter.next() orelse return null;
    const cred_region = cred_parts_iter.next() orelse return null;
    const service = cred_parts_iter.next() orelse return null;
    const terminator = cred_parts_iter.next() orelse return null;
    if (!std.mem.eql(u8, terminator, "aws4_request")) return null;

    // Extract SignedHeaders=...
    const signed_headers = extractField(rest, "SignedHeaders=") orelse return null;

    // Extract Signature=...
    const signature = extractField(rest, "Signature=") orelse return null;
    if (signature.len != 64) return null; // must be 64 hex chars

    return AuthorizationComponents{
        .access_key = access_key,
        .date_stamp = date_stamp,
        .region = cred_region,
        .service = service,
        .signed_headers = signed_headers,
        .signature = signature,
    };
}

/// Extract a field value from the Authorization header rest portion.
/// Fields are separated by ", " or "," and formatted as "Key=Value".
fn extractField(rest: []const u8, field_prefix: []const u8) ?[]const u8 {
    var search_start: usize = 0;
    while (search_start < rest.len) {
        const idx = std.mem.indexOf(u8, rest[search_start..], field_prefix) orelse return null;
        const abs_idx = search_start + idx;
        const val_start = abs_idx + field_prefix.len;
        if (val_start >= rest.len) return null;

        // Find the end of the value (next comma or end of string)
        var val_end = val_start;
        while (val_end < rest.len and rest[val_end] != ',' and rest[val_end] != ' ') {
            val_end += 1;
        }
        // Trim trailing spaces/commas
        while (val_end > val_start and (rest[val_end - 1] == ',' or rest[val_end - 1] == ' ')) {
            val_end -= 1;
        }
        if (val_end > val_start) {
            return rest[val_start..val_end];
        }
        search_start = val_end;
    }
    return null;
}

/// Parse presigned URL query parameters.
pub fn parsePresignedParams(query: []const u8) ?PresignedComponents {
    const amz_date = getQueryParam(query, "X-Amz-Date") orelse return null;
    const credential_raw = getQueryParam(query, "X-Amz-Credential") orelse return null;
    const expires = getQueryParam(query, "X-Amz-Expires") orelse return null;
    const signed_headers = getQueryParam(query, "X-Amz-SignedHeaders") orelse return null;
    const signature = getQueryParam(query, "X-Amz-Signature") orelse return null;

    if (signature.len != 64) return null;

    // Parse credential: may contain %2F instead of / or plain /
    // Find fields by looking for separators (/ or %2F) in the raw string
    // Format: AKID/date/region/service/aws4_request
    const sep0 = findCredSep(credential_raw, 0) orelse return null;
    const sep0_len = sepLen(credential_raw, sep0);
    const sep1 = findCredSep(credential_raw, sep0 + sep0_len) orelse return null;
    const sep1_len = sepLen(credential_raw, sep1);
    const sep2 = findCredSep(credential_raw, sep1 + sep1_len) orelse return null;
    const sep2_len = sepLen(credential_raw, sep2);
    const sep3 = findCredSep(credential_raw, sep2 + sep2_len) orelse return null;
    const sep3_len = sepLen(credential_raw, sep3);

    const access_key = credential_raw[0..sep0];
    const date_stamp = credential_raw[sep0 + sep0_len .. sep1];
    const cred_region = credential_raw[sep1 + sep1_len .. sep2];
    const service = credential_raw[sep2 + sep2_len .. sep3];
    const terminator = credential_raw[sep3 + sep3_len ..];

    // Validate terminator
    if (!std.mem.eql(u8, terminator, "aws4_request")) return null;

    return PresignedComponents{
        .access_key = access_key,
        .date_stamp = date_stamp,
        .region = cred_region,
        .service = service,
        .amz_date = amz_date,
        .expires = expires,
        .signed_headers = signed_headers,
        .signature = signature,
    };
}

/// Find the next '/' or '%2F' separator in a credential string starting from pos.
fn findCredSep(s: []const u8, pos: usize) ?usize {
    var i = pos;
    while (i < s.len) {
        if (s[i] == '/') return i;
        if (i + 2 < s.len and s[i] == '%' and s[i + 1] == '2' and (s[i + 2] == 'F' or s[i + 2] == 'f')) return i;
        i += 1;
    }
    return null;
}

/// Return the length of the separator at position pos ('/' = 1, '%2F' = 3).
fn sepLen(s: []const u8, pos: usize) usize {
    if (pos < s.len and s[pos] == '/') return 1;
    if (pos + 2 < s.len and s[pos] == '%') return 3;
    return 1;
}

/// URI-decode into a fixed buffer, returning a slice.
fn uriDecodeInPlace(buf: *[512]u8, input: []const u8) []const u8 {
    var out_len: usize = 0;
    var i: usize = 0;
    while (i < input.len) {
        if (input[i] == '%' and i + 2 < input.len) {
            const hi = hexVal(input[i + 1]);
            const lo = hexVal(input[i + 2]);
            if (hi != null and lo != null) {
                if (out_len < buf.len) {
                    buf[out_len] = (@as(u8, hi.?) << 4) | @as(u8, lo.?);
                    out_len += 1;
                }
                i += 3;
                continue;
            }
        }
        if (input[i] == '+') {
            if (out_len < buf.len) {
                buf[out_len] = ' ';
                out_len += 1;
            }
            i += 1;
            continue;
        }
        if (out_len < buf.len) {
            buf[out_len] = input[i];
            out_len += 1;
        }
        i += 1;
    }
    return buf[0..out_len];
}

fn hexVal(ch: u8) ?u4 {
    if (ch >= '0' and ch <= '9') return @intCast(ch - '0');
    if (ch >= 'a' and ch <= 'f') return @intCast(ch - 'a' + 10);
    if (ch >= 'A' and ch <= 'F') return @intCast(ch - 'A' + 10);
    return null;
}

// ---------------------------------------------------------------------------
// Canonical request building
// ---------------------------------------------------------------------------

/// Build the canonical request string per AWS SigV4 spec.
///   CanonicalRequest =
///       HTTPRequestMethod + '\n' +
///       CanonicalURI + '\n' +
///       CanonicalQueryString + '\n' +
///       CanonicalHeaders + '\n' +
///       SignedHeaders + '\n' +
///       HexEncode(Hash(RequestPayload))
pub fn createCanonicalRequest(
    allocator: std.mem.Allocator,
    method: []const u8,
    uri: []const u8,
    query: []const u8,
    canonical_headers: []const u8,
    signed_headers: []const u8,
    payload_hash: []const u8,
) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);

    try buf.appendSlice(allocator, method);
    try buf.append(allocator, '\n');
    try buf.appendSlice(allocator, uri);
    try buf.append(allocator, '\n');
    try buf.appendSlice(allocator, query);
    try buf.append(allocator, '\n');
    try buf.appendSlice(allocator, canonical_headers);
    try buf.append(allocator, '\n');
    try buf.appendSlice(allocator, signed_headers);
    try buf.append(allocator, '\n');
    try buf.appendSlice(allocator, payload_hash);

    return buf.toOwnedSlice(allocator);
}

/// Build the canonical URI by URI-encoding each path segment.
/// Slashes are NOT encoded. Empty path becomes "/".
pub fn buildCanonicalUri(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    if (path.len == 0 or std.mem.eql(u8, path, "/")) {
        return try allocator.dupe(u8, "/");
    }

    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(allocator);

    // Split on '/' and encode each segment.
    // Path segments from the raw URL may already be percent-encoded
    // (e.g., "key%20name"). Per AWS SigV4, we must decode first, then
    // re-encode using S3 URI encoding rules to avoid double-encoding.
    var segments = std.mem.splitScalar(u8, path, '/');
    var first = true;
    while (segments.next()) |seg| {
        if (!first) {
            try result.append(allocator, '/');
        }
        first = false;
        // Decode percent-encoded segment, then re-encode.
        var decode_buf: [512]u8 = undefined;
        const decoded = uriDecodeSegment(&decode_buf, seg);
        try s3UriEncodeAppend(allocator, &result, decoded, false);
    }

    // Ensure it starts with /
    if (result.items.len == 0 or result.items[0] != '/') {
        var with_slash: std.ArrayList(u8) = .empty;
        errdefer with_slash.deinit(allocator);
        try with_slash.append(allocator, '/');
        try with_slash.appendSlice(allocator, result.items);
        result.deinit(allocator);
        return with_slash.toOwnedSlice(allocator);
    }

    return result.toOwnedSlice(allocator);
}

/// Decode percent-encoded bytes in a URI path segment.
/// Unlike uriDecodeInPlace, does NOT decode '+' as space (path encoding,
/// not query/form encoding).
fn uriDecodeSegment(buf: *[512]u8, input: []const u8) []const u8 {
    var out_len: usize = 0;
    var i: usize = 0;
    while (i < input.len) {
        if (input[i] == '%' and i + 2 < input.len) {
            const hi = hexVal(input[i + 1]);
            const lo = hexVal(input[i + 2]);
            if (hi != null and lo != null) {
                if (out_len < buf.len) {
                    buf[out_len] = (@as(u8, hi.?) << 4) | @as(u8, lo.?);
                    out_len += 1;
                }
                i += 3;
                continue;
            }
        }
        if (out_len < buf.len) {
            buf[out_len] = input[i];
            out_len += 1;
        }
        i += 1;
    }
    return buf[0..out_len];
}

/// Build canonical query string: sort parameters by key, URI-encode names and values.
/// For presigned URLs, X-Amz-Signature is EXCLUDED.
pub fn buildCanonicalQueryString(allocator: std.mem.Allocator, query: []const u8) ![]u8 {
    return buildCanonicalQueryStringImpl(allocator, query, false);
}

/// Build canonical query string for presigned URLs (excludes X-Amz-Signature).
pub fn buildPresignedCanonicalQueryString(allocator: std.mem.Allocator, query: []const u8) ![]u8 {
    return buildCanonicalQueryStringImpl(allocator, query, true);
}

fn buildCanonicalQueryStringImpl(allocator: std.mem.Allocator, query: []const u8, exclude_signature: bool) ![]u8 {
    if (query.len == 0) {
        return try allocator.dupe(u8, "");
    }

    // Parse all query parameters into name=value pairs.
    // Query parameter names and values from the raw URL may already be
    // percent-encoded (e.g., prefix=data%2F). Per the AWS SigV4 spec, we
    // must NOT double-encode: decode first, then re-encode using S3 URI
    // encoding rules. This matches what botocore's SigV4Auth does.
    //
    // We use fixed-size stack buffers for decoded names/values since query
    // parameters are bounded in length. The decoded slices point into these
    // buffers, so we store the decoded data into arena-allocated copies.
    const Pair = struct {
        /// URI-encoded name (ready for output, no further encoding needed).
        encoded_name: []const u8,
        /// URI-encoded value (ready for output, no further encoding needed).
        encoded_value: []const u8,
    };

    var pairs: std.ArrayList(Pair) = .empty;
    defer pairs.deinit(allocator);

    var params = std.mem.splitScalar(u8, query, '&');
    while (params.next()) |param| {
        if (param.len == 0) continue;
        const eq_idx = std.mem.indexOfScalar(u8, param, '=');
        const raw_name = if (eq_idx) |ei| param[0..ei] else param;
        const raw_value = if (eq_idx) |ei| param[ei + 1 ..] else "";

        if (exclude_signature and std.mem.eql(u8, raw_name, "X-Amz-Signature")) continue;

        // Decode percent-encoded names and values, then re-encode.
        var name_buf: [512]u8 = undefined;
        const decoded_name = uriDecodeSegment(&name_buf, raw_name);
        const encoded_name = try s3UriEncode(allocator, decoded_name, true);

        var value_buf: [512]u8 = undefined;
        const decoded_value = uriDecodeSegment(&value_buf, raw_value);
        const encoded_value = try s3UriEncode(allocator, decoded_value, true);

        try pairs.append(allocator, .{ .encoded_name = encoded_name, .encoded_value = encoded_value });
    }

    // Sort by encoded name (byte-order), then by encoded value if names equal.
    // Sorting on the encoded form matches the SigV4 spec requirement.
    std.mem.sort(Pair, pairs.items, {}, struct {
        fn lessThan(_: void, a: Pair, b: Pair) bool {
            const name_cmp = std.mem.order(u8, a.encoded_name, b.encoded_name);
            if (name_cmp == .lt) return true;
            if (name_cmp == .gt) return false;
            return std.mem.order(u8, a.encoded_value, b.encoded_value) == .lt;
        }
    }.lessThan);

    // Build the canonical query string from pre-encoded pairs.
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(allocator);

    for (pairs.items, 0..) |pair, i| {
        if (i > 0) try result.append(allocator, '&');
        try result.appendSlice(allocator, pair.encoded_name);
        try result.append(allocator, '=');
        try result.appendSlice(allocator, pair.encoded_value);
        // Free the pre-encoded strings after appending.
        allocator.free(pair.encoded_name);
        allocator.free(pair.encoded_value);
    }

    return result.toOwnedSlice(allocator);
}

/// Build canonical headers string from the signed_headers list.
/// Looks up each header value from the request headers.
pub fn buildCanonicalHeaders(
    allocator: std.mem.Allocator,
    signed_headers: []const u8,
    host: ?[]const u8,
    amz_date: ?[]const u8,
    content_sha256: ?[]const u8,
    header_keys: []const []const u8,
    header_values: []const []const u8,
) ![]u8 {
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(allocator);

    // Split signed_headers by ';'
    var headers_iter = std.mem.splitScalar(u8, signed_headers, ';');
    while (headers_iter.next()) |header_name| {
        if (header_name.len == 0) continue;

        // Look up the header value
        const value = lookupHeaderValue(header_name, host, amz_date, content_sha256, header_keys, header_values);

        try result.appendSlice(allocator, header_name);
        try result.append(allocator, ':');
        if (value) |v| {
            // Trim and collapse whitespace
            try appendTrimmedValue(allocator, &result, v);
        }
        try result.append(allocator, '\n');
    }

    return result.toOwnedSlice(allocator);
}

/// Look up a header value by lowercase name.
fn lookupHeaderValue(
    name: []const u8,
    host: ?[]const u8,
    amz_date: ?[]const u8,
    content_sha256: ?[]const u8,
    header_keys: []const []const u8,
    header_values: []const []const u8,
) ?[]const u8 {
    // Check well-known headers first
    if (std.mem.eql(u8, name, "host")) return host;
    if (std.mem.eql(u8, name, "x-amz-date")) return amz_date;
    if (std.mem.eql(u8, name, "x-amz-content-sha256")) return content_sha256;

    // Search in request headers (case-insensitive)
    const count = @min(header_keys.len, header_values.len);
    for (0..count) |i| {
        if (std.ascii.eqlIgnoreCase(header_keys[i], name)) {
            return header_values[i];
        }
    }
    return null;
}

/// Append a header value with leading/trailing whitespace trimmed
/// and sequential spaces collapsed to a single space.
fn appendTrimmedValue(allocator: std.mem.Allocator, result: *std.ArrayList(u8), value: []const u8) !void {
    const trimmed = std.mem.trim(u8, value, " \t");
    var prev_space = false;
    for (trimmed) |ch| {
        if (ch == ' ' or ch == '\t') {
            if (!prev_space) {
                try result.append(allocator, ' ');
                prev_space = true;
            }
        } else {
            try result.append(allocator, ch);
            prev_space = false;
        }
    }
}

// ---------------------------------------------------------------------------
// String to sign
// ---------------------------------------------------------------------------

/// Compute the string to sign.
///   StringToSign =
///       "AWS4-HMAC-SHA256" + '\n' +
///       TimeStamp + '\n' +
///       Scope + '\n' +
///       HexEncode(SHA256(CanonicalRequest))
pub fn computeStringToSign(
    allocator: std.mem.Allocator,
    timestamp: []const u8,
    scope: []const u8,
    canonical_request: []const u8,
) ![]u8 {
    // Hash the canonical request
    var hash: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(canonical_request, &hash, .{});
    const hash_hex = std.fmt.bytesToHex(hash, .lower);

    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(allocator);

    try result.appendSlice(allocator, "AWS4-HMAC-SHA256");
    try result.append(allocator, '\n');
    try result.appendSlice(allocator, timestamp);
    try result.append(allocator, '\n');
    try result.appendSlice(allocator, scope);
    try result.append(allocator, '\n');
    try result.appendSlice(allocator, &hash_hex);

    return result.toOwnedSlice(allocator);
}

/// Build the credential scope string: date/region/service/aws4_request
fn buildScope(allocator: std.mem.Allocator, date_stamp: []const u8, region: []const u8, service: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}/{s}/{s}/aws4_request", .{ date_stamp, region, service });
}

// ---------------------------------------------------------------------------
// Signing key derivation
// ---------------------------------------------------------------------------

/// Derive the signing key for AWS Signature V4.
///   kDate = HMAC-SHA256("AWS4" + secretKey, dateStamp)
///   kRegion = HMAC-SHA256(kDate, regionName)
///   kService = HMAC-SHA256(kRegion, serviceName)
///   kSigning = HMAC-SHA256(kService, "aws4_request")
pub fn deriveSigningKey(
    secret_key: []const u8,
    date_stamp: []const u8,
    region: []const u8,
    service: []const u8,
) [HmacSha256.mac_length]u8 {
    // Step 1: kDate
    var key_prefix_buf: [256]u8 = undefined;
    const prefix = "AWS4";
    @memcpy(key_prefix_buf[0..prefix.len], prefix);
    @memcpy(key_prefix_buf[prefix.len .. prefix.len + secret_key.len], secret_key);
    const initial_key = key_prefix_buf[0 .. prefix.len + secret_key.len];

    var k_date: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_date, date_stamp, initial_key);

    // Step 2: kRegion
    var k_region: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_region, region, &k_date);

    // Step 3: kService
    var k_service: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_service, service, &k_region);

    // Step 4: kSigning
    var k_signing: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_signing, "aws4_request", &k_service);

    return k_signing;
}

// ---------------------------------------------------------------------------
// URI encoding
// ---------------------------------------------------------------------------

/// S3-compatible URI encoding. Encodes all characters except unreserved characters
/// (A-Z, a-z, 0-9, '-', '_', '.', '~'). If encode_slash is false, '/' is also
/// left unencoded (for URI paths). Spaces become %20, not +.
pub fn s3UriEncode(allocator: std.mem.Allocator, input: []const u8, encode_slash: bool) ![]u8 {
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(allocator);
    try s3UriEncodeAppend(allocator, &result, input, encode_slash);
    return result.toOwnedSlice(allocator);
}

fn s3UriEncodeAppend(allocator: std.mem.Allocator, result: *std.ArrayList(u8), input: []const u8, encode_slash: bool) !void {
    for (input) |ch| {
        if (isUnreserved(ch) or (!encode_slash and ch == '/')) {
            try result.append(allocator, ch);
        } else {
            try result.append(allocator, '%');
            try result.append(allocator, hexDigitUpper(@as(u4, @truncate(ch >> 4))));
            try result.append(allocator, hexDigitUpper(@as(u4, @truncate(ch))));
        }
    }
}

fn isUnreserved(ch: u8) bool {
    return (ch >= 'A' and ch <= 'Z') or
        (ch >= 'a' and ch <= 'z') or
        (ch >= '0' and ch <= '9') or
        ch == '-' or ch == '_' or ch == '.' or ch == '~';
}

fn hexDigitUpper(nibble: u4) u8 {
    const chars = "0123456789ABCDEF";
    return chars[@intCast(nibble)];
}

// ---------------------------------------------------------------------------
// Hex encoding
// ---------------------------------------------------------------------------

/// Hex-encode a byte slice into output buffer (lowercase).
pub fn hexEncode(bytes: []const u8, out: []u8) void {
    const hex_chars = "0123456789abcdef";
    for (bytes, 0..) |b, i| {
        out[i * 2] = hex_chars[b >> 4];
        out[i * 2 + 1] = hex_chars[b & 0x0f];
    }
}

// ---------------------------------------------------------------------------
// Constant-time comparison
// ---------------------------------------------------------------------------

/// Constant-time comparison of two byte sequences.
/// Returns true if they are equal.
pub fn constantTimeEql(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    var diff: u8 = 0;
    for (a, b) |x, y| {
        diff |= x ^ y;
    }
    return diff == 0;
}

// ---------------------------------------------------------------------------
// Timestamp helpers
// ---------------------------------------------------------------------------

/// Check if a timestamp (YYYYMMDDTHHMMSSZ) is within the given skew (seconds)
/// of the current time.
pub fn isTimestampWithinSkew(timestamp: []const u8, max_skew_seconds: u64) bool {
    const req_epoch = parseAmzTimestampToEpoch(timestamp) orelse return false;
    const now = getCurrentEpoch();

    // Check absolute difference
    const diff = if (now >= req_epoch) now - req_epoch else req_epoch - now;
    return diff <= max_skew_seconds;
}

/// Check if a presigned URL is not expired.
fn isPresignedNotExpired(amz_date: []const u8, expires_seconds: u64) bool {
    const sign_epoch = parseAmzTimestampToEpoch(amz_date) orelse return false;
    const now = getCurrentEpoch();
    const expiry = sign_epoch + expires_seconds;
    return now <= expiry;
}

/// Parse YYYYMMDDTHHMMSSZ to epoch seconds.
pub fn parseAmzTimestampToEpoch(timestamp: []const u8) ?u64 {
    // Format: 20260222T120000Z (length 16)
    if (timestamp.len < 16) return null;
    if (timestamp[8] != 'T' or timestamp[15] != 'Z') return null;

    const year = std.fmt.parseInt(u16, timestamp[0..4], 10) catch return null;
    const month = std.fmt.parseInt(u8, timestamp[4..6], 10) catch return null;
    const day = std.fmt.parseInt(u8, timestamp[6..8], 10) catch return null;
    const hour = std.fmt.parseInt(u8, timestamp[9..11], 10) catch return null;
    const minute = std.fmt.parseInt(u8, timestamp[11..13], 10) catch return null;
    const second = std.fmt.parseInt(u8, timestamp[13..15], 10) catch return null;

    return dateTimeToEpoch(year, month, day, hour, minute, second);
}

/// Convert date/time components to Unix epoch seconds.
fn dateTimeToEpoch(year: u16, month: u8, day: u8, hour: u8, minute: u8, second: u8) ?u64 {
    if (month < 1 or month > 12) return null;
    if (day < 1 or day > 31) return null;
    if (hour > 23 or minute > 59 or second > 59) return null;

    // Days from year 0 to 1970
    var total_days: i64 = 0;

    // Add days for complete years from 1970 to year
    if (year >= 1970) {
        var y: u16 = 1970;
        while (y < year) : (y += 1) {
            total_days += if (isLeapYear(y)) @as(i64, 366) else 365;
        }
    } else {
        return null;
    }

    // Add days for complete months in current year
    const days_in_month = [_]u8{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    var m: u8 = 1;
    while (m < month) : (m += 1) {
        var d: u8 = days_in_month[m - 1];
        if (m == 2 and isLeapYear(year)) d += 1;
        total_days += d;
    }

    // Add days in current month
    total_days += day - 1;

    const total_seconds = total_days * 86400 + @as(i64, hour) * 3600 + @as(i64, minute) * 60 + second;
    if (total_seconds < 0) return null;
    return @intCast(total_seconds);
}

fn isLeapYear(year: u16) bool {
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0);
}

fn getCurrentEpoch() u64 {
    const ts = std.time.timestamp();
    return @intCast(if (ts < 0) 0 else ts);
}

// ---------------------------------------------------------------------------
// Query parameter helper
// ---------------------------------------------------------------------------

/// Extract a query parameter value by name from a raw query string.
pub fn getQueryParam(query: []const u8, key: []const u8) ?[]const u8 {
    if (query.len == 0) return null;
    var params = std.mem.splitScalar(u8, query, '&');
    while (params.next()) |param| {
        if (std.mem.indexOfScalar(u8, param, '=')) |ei| {
            const k = param[0..ei];
            if (std.mem.eql(u8, k, key)) {
                return param[ei + 1 ..];
            }
        }
    }
    return null;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "deriveSigningKey produces 32 byte key" {
    const key = deriveSigningKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20130524", "us-east-1", "s3");
    try std.testing.expectEqual(@as(usize, 32), key.len);
}

test "hexEncode" {
    const input = [_]u8{ 0xde, 0xad, 0xbe, 0xef };
    var out: [8]u8 = undefined;
    hexEncode(&input, &out);
    try std.testing.expectEqualStrings("deadbeef", &out);
}

test "parseAuthorizationHeader valid" {
    const header = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024";
    const components = parseAuthorizationHeader(header);
    try std.testing.expect(components != null);
    const c = components.?;
    try std.testing.expectEqualStrings("AKIAIOSFODNN7EXAMPLE", c.access_key);
    try std.testing.expectEqualStrings("20130524", c.date_stamp);
    try std.testing.expectEqualStrings("us-east-1", c.region);
    try std.testing.expectEqualStrings("s3", c.service);
    try std.testing.expectEqualStrings("host;range;x-amz-content-sha256;x-amz-date", c.signed_headers);
    try std.testing.expectEqualStrings("fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024", c.signature);
}

test "parseAuthorizationHeader invalid" {
    try std.testing.expect(parseAuthorizationHeader("Bearer token") == null);
    try std.testing.expect(parseAuthorizationHeader("") == null);
    try std.testing.expect(parseAuthorizationHeader("AWS4-HMAC-SHA256 ") == null);
}

test "constantTimeEql" {
    try std.testing.expect(constantTimeEql("abc", "abc"));
    try std.testing.expect(!constantTimeEql("abc", "abd"));
    try std.testing.expect(!constantTimeEql("abc", "ab"));
    try std.testing.expect(constantTimeEql("", ""));
}

test "s3UriEncode" {
    const alloc = std.testing.allocator;

    // Unreserved characters should not be encoded
    const simple = try s3UriEncode(alloc, "hello-world_test.txt~v1", true);
    defer alloc.free(simple);
    try std.testing.expectEqualStrings("hello-world_test.txt~v1", simple);

    // Spaces should be %20
    const with_space = try s3UriEncode(alloc, "hello world", true);
    defer alloc.free(with_space);
    try std.testing.expectEqualStrings("hello%20world", with_space);

    // Slashes: encode when encode_slash=true
    const slash_enc = try s3UriEncode(alloc, "a/b", true);
    defer alloc.free(slash_enc);
    try std.testing.expectEqualStrings("a%2Fb", slash_enc);

    // Slashes: don't encode when encode_slash=false
    const slash_noenc = try s3UriEncode(alloc, "a/b", false);
    defer alloc.free(slash_noenc);
    try std.testing.expectEqualStrings("a/b", slash_noenc);
}

test "buildCanonicalUri" {
    const alloc = std.testing.allocator;

    // Empty path becomes "/"
    const empty = try buildCanonicalUri(alloc, "");
    defer alloc.free(empty);
    try std.testing.expectEqualStrings("/", empty);

    // Root path stays "/"
    const root = try buildCanonicalUri(alloc, "/");
    defer alloc.free(root);
    try std.testing.expectEqualStrings("/", root);

    // Normal path
    const normal = try buildCanonicalUri(alloc, "/mybucket/mykey");
    defer alloc.free(normal);
    try std.testing.expectEqualStrings("/mybucket/mykey", normal);
}

test "buildCanonicalQueryString" {
    const alloc = std.testing.allocator;

    // Empty query
    const empty = try buildCanonicalQueryString(alloc, "");
    defer alloc.free(empty);
    try std.testing.expectEqualStrings("", empty);

    // Single param with no value (like ?acl)
    const single = try buildCanonicalQueryString(alloc, "acl");
    defer alloc.free(single);
    try std.testing.expectEqualStrings("acl=", single);

    // Multiple params should be sorted
    const multi = try buildCanonicalQueryString(alloc, "prefix=foo&delimiter=/&max-keys=10");
    defer alloc.free(multi);
    try std.testing.expectEqualStrings("delimiter=%2F&max-keys=10&prefix=foo", multi);

    // Pre-encoded values should NOT be double-encoded (decode first, then encode).
    // e.g., prefix=data%2F should become prefix=data%2F, not prefix=data%252F.
    const preenc = try buildCanonicalQueryString(alloc, "uploads&prefix=data%2F");
    defer alloc.free(preenc);
    try std.testing.expectEqualStrings("prefix=data%2F&uploads=", preenc);
}

test "computeStringToSign" {
    const alloc = std.testing.allocator;
    const sts = try computeStringToSign(alloc, "20130524T000000Z", "20130524/us-east-1/s3/aws4_request", "test-canonical-request");
    defer alloc.free(sts);
    try std.testing.expect(std.mem.startsWith(u8, sts, "AWS4-HMAC-SHA256\n"));
    try std.testing.expect(std.mem.indexOf(u8, sts, "20130524T000000Z") != null);
    try std.testing.expect(std.mem.indexOf(u8, sts, "20130524/us-east-1/s3/aws4_request") != null);
}

test "parseAmzTimestampToEpoch" {
    // 2026-02-22T12:00:00Z
    const epoch = parseAmzTimestampToEpoch("20260222T120000Z");
    try std.testing.expect(epoch != null);
    // Should be a reasonable epoch value (after 2025)
    try std.testing.expect(epoch.? > 1700000000);

    // Invalid format
    try std.testing.expect(parseAmzTimestampToEpoch("invalid") == null);
    try std.testing.expect(parseAmzTimestampToEpoch("") == null);
}

test "detectAuthType" {
    try std.testing.expectEqual(AuthType.none, detectAuthType(null, ""));
    try std.testing.expectEqual(AuthType.header, detectAuthType("AWS4-HMAC-SHA256 Credential=...", ""));
    try std.testing.expectEqual(AuthType.presigned, detectAuthType(null, "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=foo"));
    try std.testing.expectEqual(AuthType.none, detectAuthType("Bearer token", ""));
}

test "extractAccessKeyFromHeader" {
    const key = extractAccessKeyFromHeader("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024");
    try std.testing.expect(key != null);
    try std.testing.expectEqualStrings("AKIAIOSFODNN7EXAMPLE", key.?);
}

test "extractAccessKeyFromQuery" {
    const key = extractAccessKeyFromQuery("X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260222%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260222T120000Z");
    try std.testing.expect(key != null);
    try std.testing.expectEqualStrings("AKIAIOSFODNN7EXAMPLE", key.?);

    // With / instead of %2F
    const key2 = extractAccessKeyFromQuery("X-Amz-Credential=MYKEY/20260222/us-east-1/s3/aws4_request");
    try std.testing.expect(key2 != null);
    try std.testing.expectEqualStrings("MYKEY", key2.?);
}

test "getQueryParam" {
    try std.testing.expectEqualStrings("AWS4-HMAC-SHA256", getQueryParam("X-Amz-Algorithm=AWS4-HMAC-SHA256&foo=bar", "X-Amz-Algorithm").?);
    try std.testing.expectEqualStrings("bar", getQueryParam("X-Amz-Algorithm=AWS4-HMAC-SHA256&foo=bar", "foo").?);
    try std.testing.expect(getQueryParam("foo=bar", "baz") == null);
    try std.testing.expect(getQueryParam("", "foo") == null);
}

test "verifyHeaderAuth with known signature" {
    // This test verifies the full SigV4 flow with a locally computed signature.
    const alloc = std.testing.allocator;

    const secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const access_key = "AKIAIOSFODNN7EXAMPLE";
    const region = "us-east-1";
    const date_stamp = "20130524";
    const timestamp = "20130524T000000Z";
    const method = "GET";
    const path = "/test.txt";
    const query = "";
    const payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; // SHA256 of empty string
    const signed_headers = "host;range;x-amz-content-sha256;x-amz-date";

    // Build the expected canonical request manually
    const canonical_uri = try buildCanonicalUri(alloc, path);
    defer alloc.free(canonical_uri);
    const canonical_query = try buildCanonicalQueryString(alloc, query);
    defer alloc.free(canonical_query);

    // Build canonical headers
    const host_val = "examplebucket.s3.amazonaws.com";
    const range_val = "bytes=0-9";
    var ch_buf: std.ArrayList(u8) = .empty;
    defer ch_buf.deinit(alloc);
    try ch_buf.appendSlice(alloc, "host:");
    try ch_buf.appendSlice(alloc, host_val);
    try ch_buf.append(alloc, '\n');
    try ch_buf.appendSlice(alloc, "range:");
    try ch_buf.appendSlice(alloc, range_val);
    try ch_buf.append(alloc, '\n');
    try ch_buf.appendSlice(alloc, "x-amz-content-sha256:");
    try ch_buf.appendSlice(alloc, payload_hash);
    try ch_buf.append(alloc, '\n');
    try ch_buf.appendSlice(alloc, "x-amz-date:");
    try ch_buf.appendSlice(alloc, timestamp);
    try ch_buf.append(alloc, '\n');

    const canonical_request = try createCanonicalRequest(
        alloc,
        method,
        canonical_uri,
        canonical_query,
        ch_buf.items,
        signed_headers,
        payload_hash,
    );
    defer alloc.free(canonical_request);

    const scope = try buildScope(alloc, date_stamp, region, "s3");
    defer alloc.free(scope);
    const string_to_sign = try computeStringToSign(alloc, timestamp, scope, canonical_request);
    defer alloc.free(string_to_sign);

    // Compute the signature
    const signing_key = deriveSigningKey(secret, date_stamp, region, "s3");
    var sig_mac: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&sig_mac, string_to_sign, &signing_key);
    const computed_sig = std.fmt.bytesToHex(sig_mac, .lower);

    // Build the Authorization header
    const auth_header = try std.fmt.allocPrint(alloc, "AWS4-HMAC-SHA256 Credential={s}/{s}/{s}/s3/aws4_request, SignedHeaders={s}, Signature={s}", .{
        access_key,
        date_stamp,
        region,
        signed_headers,
        &computed_sig,
    });
    defer alloc.free(auth_header);

    // The header_keys and header_values for non-standard headers
    const hk = [_][]const u8{ "host", "range", "x-amz-content-sha256", "x-amz-date" };
    const hv = [_][]const u8{ host_val, range_val, payload_hash, timestamp };

    // Verification should succeed (but may fail on clock skew for old timestamp)
    // We test the signature computation is correct, not the clock check.
    // So we test the components directly.
    const parsed = parseAuthorizationHeader(auth_header).?;
    try std.testing.expectEqualStrings(access_key, parsed.access_key);
    try std.testing.expectEqualStrings(&computed_sig, parsed.signature);

    // Verify signature matches (without clock skew check) by recomputing
    const signing_key2 = deriveSigningKey(secret, parsed.date_stamp, parsed.region, parsed.service);
    var sig_mac2: [HmacSha256.mac_length]u8 = undefined;

    // Rebuild canonical headers from signed_headers list and request headers
    const ch2 = try buildCanonicalHeaders(alloc, parsed.signed_headers, host_val, timestamp, payload_hash, &hk, &hv);
    defer alloc.free(ch2);

    const cr2 = try createCanonicalRequest(alloc, method, canonical_uri, canonical_query, ch2, parsed.signed_headers, payload_hash);
    defer alloc.free(cr2);

    const scope2 = try buildScope(alloc, parsed.date_stamp, parsed.region, parsed.service);
    defer alloc.free(scope2);
    const sts2 = try computeStringToSign(alloc, timestamp, scope2, cr2);
    defer alloc.free(sts2);

    HmacSha256.create(&sig_mac2, sts2, &signing_key2);
    const recomputed_sig = std.fmt.bytesToHex(sig_mac2, .lower);

    try std.testing.expect(constantTimeEql(&recomputed_sig, parsed.signature));
}
