const std = @import("std");
const tk = @import("tokamak");
const server = @import("../server.zig");
const sendS3Error = server.sendS3Error;
const sendS3ErrorWithMessage = server.sendS3ErrorWithMessage;
const sendResponse = server.sendResponse;
const store = @import("../metadata/store.zig");
const metrics_mod = @import("../metrics.zig");
const xml_mod = @import("../xml.zig");
const validation = @import("../validation.zig");

/// Derive a canonical owner ID from an access key.
/// Uses SHA-256 hash of the access key, truncated to 32 hex characters.
fn deriveOwnerId(alloc: std.mem.Allocator, access_key: []const u8) ![]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(access_key, &hash, .{});
    const hex = std.fmt.bytesToHex(hash, .lower);
    return try alloc.dupe(u8, hex[0..32]);
}

/// Build a default private ACL JSON string (owner FULL_CONTROL).
fn buildDefaultAclJson(alloc: std.mem.Allocator, owner_id: []const u8, owner_display: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc,
        \\{{"owner":{{"id":"{s}","display_name":"{s}"}},"grants":[{{"grantee":{{"type":"CanonicalUser","id":"{s}","display_name":"{s}"}},"permission":"FULL_CONTROL"}}]}}
    , .{ owner_id, owner_display, owner_id, owner_display });
}

/// Format current time as ISO 8601 string: "2026-02-23T12:00:00.000Z"
fn formatIso8601(alloc: std.mem.Allocator) ![]u8 {
    const timestamp = std.time.timestamp();
    const epoch_secs: u64 = @intCast(if (timestamp < 0) 0 else timestamp);
    const es = std.time.epoch.EpochSeconds{ .secs = epoch_secs };
    const epoch_day = es.getEpochDay();
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = es.getDaySeconds();

    return std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.000Z", .{
        year_day.year,
        month_day.month.numeric(),
        @as(u8, month_day.day_index) + 1,
        day_seconds.getHoursIntoDay(),
        day_seconds.getMinutesIntoHour(),
        day_seconds.getSecondsIntoMinute(),
    });
}

/// Extract user metadata from request headers.
/// Returns a JSON object string like {"key":"value",...} or null if no user metadata.
///
/// httpz stores headers in a KeyValue structure with a `.headers` field.
/// We iterate using `.headers.keys` and `.headers.values` arrays.
fn extractUserMetadata(alloc: std.mem.Allocator, req: *tk.Request) !?[]u8 {
    const prefix = "x-amz-meta-";

    // httpz Request has a `.headers` field which is a KeyValue struct.
    // The `.keys` and `.values` are fixed-size backing arrays whose `.len` is the
    // compile-time capacity, NOT the number of populated entries. The actual
    // populated count is tracked by `req.headers.len` (runtime field).
    //
    // Accessing beyond `req.headers.len` hits freed/sentinel memory (0xAA bytes
    // from GPA), causing a segfault.
    const header_keys = req.headers.keys;
    const header_values = req.headers.values;
    const header_count = req.headers.len;

    var found_any = false;

    // First pass: check if any user metadata headers exist.
    for (0..header_count) |i| {
        const name = header_keys[i];
        if (name.len >= prefix.len and std.ascii.startsWithIgnoreCase(name, prefix)) {
            found_any = true;
            break;
        }
    }

    if (!found_any) return null;

    // Second pass: build JSON object.
    var json_buf = std.ArrayList(u8).empty;
    errdefer json_buf.deinit(alloc);

    try json_buf.append(alloc, '{');
    var first = true;

    for (0..header_count) |i| {
        const name = header_keys[i];
        if (name.len >= prefix.len and std.ascii.startsWithIgnoreCase(name, prefix)) {
            const meta_key = name[prefix.len..];
            if (meta_key.len == 0) continue;

            const value = header_values[i];

            if (!first) {
                try json_buf.append(alloc, ',');
            }
            first = false;

            try json_buf.append(alloc, '"');
            try json_buf.appendSlice(alloc, meta_key);
            try json_buf.appendSlice(alloc, "\":\"");
            try json_buf.appendSlice(alloc, value);
            try json_buf.append(alloc, '"');
        }
    }

    try json_buf.append(alloc, '}');
    return try json_buf.toOwnedSlice(alloc);
}

/// Emit user metadata as x-amz-meta-* response headers.
/// Parses the JSON user_metadata string and sets response headers.
fn emitUserMetadataHeaders(res: *tk.Response, alloc: std.mem.Allocator, user_metadata_json: []const u8) void {
    if (user_metadata_json.len < 3 or std.mem.eql(u8, user_metadata_json, "{}")) return;

    // Parse JSON to extract key-value pairs.
    const parsed = std.json.parseFromSlice(std.json.Value, alloc, user_metadata_json, .{}) catch return;
    defer parsed.deinit();

    if (parsed.value != .object) return;

    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        const value_str = switch (entry.value_ptr.*) {
            .string => |s| s,
            else => continue,
        };

        // Build "x-amz-meta-{key}" header name.
        const header_name = std.fmt.allocPrint(alloc, "x-amz-meta-{s}", .{entry.key_ptr.*}) catch continue;
        const header_value = alloc.dupe(u8, value_str) catch continue;
        res.header(header_name, header_value);
    }
}

/// PUT /<bucket>/<key> -- Store an object.
pub fn putObject(
    res: *tk.Response,
    req: *tk.Request,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    object_key: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);
    const sb = server.global_storage_backend orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Validate object key.
    if (validation.isValidObjectKey(object_key)) |err| {
        return sendS3Error(res, req_alloc, err, object_key, request_id);
    }

    // Check that the bucket exists.
    const bucket_exists = try ms.bucketExists(bucket_name);
    if (!bucket_exists) {
        return sendS3Error(res, req_alloc, .NoSuchBucket, bucket_name, request_id);
    }

    // If-None-Match: * -- reject if object already exists (conditional PUT).
    if (req.header("if-none-match")) |inm| {
        if (std.mem.eql(u8, std.mem.trim(u8, inm, " "), "*")) {
            const obj_exists = try ms.objectExists(bucket_name, object_key);
            if (obj_exists) {
                return sendS3Error(res, req_alloc, .PreconditionFailed, object_key, request_id);
            }
        }
    }

    // Read the request body.
    const body = req.body() orelse "";

    // Check max object size.
    if (body.len > server.global_max_object_size) {
        return sendS3Error(res, req_alloc, .EntityTooLarge, object_key, request_id);
    }

    // Validate Content-MD5 header if present.
    if (req.header("content-md5")) |content_md5| {
        // Base64-decode the provided MD5.
        var decoded_md5: [16]u8 = undefined;
        std.base64.standard.Decoder.decode(&decoded_md5, content_md5) catch {
            return sendS3Error(res, req_alloc, .InvalidDigest, object_key, request_id);
        };
        // Compute MD5 of the request body.
        var computed_md5: [std.crypto.hash.Md5.digest_length]u8 = undefined;
        std.crypto.hash.Md5.hash(body, &computed_md5, .{});
        // Compare digests.
        if (!std.mem.eql(u8, &decoded_md5, &computed_md5)) {
            return sendS3Error(res, req_alloc, .BadDigest, object_key, request_id);
        }
    }

    // Write to storage backend (atomic: temp + fsync + rename).
    const put_result = try sb.putObject(bucket_name, object_key, body, .{
        .content_type = req.header("content-type") orelse "application/octet-stream",
        .content_length = @intCast(body.len),
    });

    // Extract user metadata from request headers.
    const user_metadata = try extractUserMetadata(req_alloc, req);

    // Derive owner.
    const access_key = server.global_access_key;
    const owner_id = try deriveOwnerId(req_alloc, access_key);
    const owner_display = access_key;

    // --- ACL processing ---
    // 1D: Mutual exclusion -- x-amz-acl and x-amz-grant-* cannot coexist.
    const canned_acl_val = req.header("x-amz-acl");
    const has_grant_headers = hasAnyGrantHeader(req);
    if (canned_acl_val != null and has_grant_headers) {
        return sendS3ErrorWithMessage(res, req_alloc, .InvalidArgument, "Specifying both x-amz-acl and x-amz-grant headers is not allowed", object_key, request_id);
    }

    // Build ACL from canned header, grant headers, or default.
    var acl_json = try buildDefaultAclJson(req_alloc, owner_id, owner_display);
    if (canned_acl_val) |ca| {
        if (try buildCannedAclJson(req_alloc, ca, owner_id, owner_display)) |canned| {
            acl_json = canned;
        }
    } else if (has_grant_headers) {
        if (try parseGrantHeaders(req_alloc, req, owner_id, owner_display)) |grant_acl| {
            acl_json = grant_acl;
        }
    }

    // Get content type from request.
    const content_type = req.header("content-type") orelse "application/octet-stream";

    // Format timestamp.
    const last_modified = try formatIso8601(req_alloc);

    // Upsert object metadata.
    try ms.putObjectMeta(.{
        .bucket = bucket_name,
        .key = object_key,
        .size = @intCast(body.len),
        .etag = put_result.etag,
        .content_type = content_type,
        .last_modified = last_modified,
        .storage_class = "STANDARD",
        .user_metadata = user_metadata,
        .acl = acl_json,
    });

    // Update metrics.
    _ = metrics_mod.objects_total.fetchAdd(1, .monotonic);
    metrics_mod.addBytesReceived(@intCast(body.len));

    // Send 200 response with ETag header.
    server.setCommonHeaders(res, request_id);
    res.status = 200;
    const etag_value = try req_alloc.dupe(u8, put_result.etag);
    res.header("ETag", etag_value);
    res.body = "";
}

/// GET /<bucket>/<key> -- Retrieve an object.
pub fn getObject(
    res: *tk.Response,
    req: *tk.Request,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    object_key: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);
    const sb = server.global_storage_backend orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Check bucket exists.
    const bucket_exists = try ms.bucketExists(bucket_name);
    if (!bucket_exists) {
        return sendS3Error(res, req_alloc, .NoSuchBucket, bucket_name, request_id);
    }

    // Look up object metadata.
    const obj_meta = try ms.getObjectMeta(bucket_name, object_key);
    if (obj_meta == null) {
        return sendS3Error(res, req_alloc, .NoSuchKey, object_key, request_id);
    }
    const meta = obj_meta.?;

    // --- Conditional request evaluation ---
    // Priority rules per HTTP/S3 spec:
    //   If-Match > If-Unmodified-Since
    //   If-None-Match > If-Modified-Since

    // If-Match: must match the ETag, otherwise 412.
    const if_match = req.header("if-match");
    if (if_match) |etag_cond| {
        if (!etagMatch(meta.etag, etag_cond)) {
            server.setCommonHeaders(res, request_id);
            res.status = 412;
            res.body = "";
            return;
        }
    }

    // If-Unmodified-Since: only if If-Match was NOT present.
    if (if_match == null) {
        const if_unmodified_since = req.header("if-unmodified-since");
        if (if_unmodified_since) |date_str| {
            if (isModifiedSince(meta.last_modified, date_str)) {
                server.setCommonHeaders(res, request_id);
                res.status = 412;
                res.body = "";
                return;
            }
        }
    }

    // If-None-Match: if ETag matches, return 304.
    const if_none_match = req.header("if-none-match");
    if (if_none_match) |etag_cond| {
        if (etagMatch(meta.etag, etag_cond)) {
            server.setCommonHeaders(res, request_id);
            res.status = 304;
            const etag_hdr = try req_alloc.dupe(u8, meta.etag);
            res.header("ETag", etag_hdr);
            res.body = "";
            return;
        }
    }

    // If-Modified-Since: only if If-None-Match was NOT present.
    if (if_none_match == null) {
        const if_modified_since = req.header("if-modified-since");
        if (if_modified_since) |date_str| {
            if (!isModifiedSince(meta.last_modified, date_str)) {
                server.setCommonHeaders(res, request_id);
                res.status = 304;
                const etag_hdr = try req_alloc.dupe(u8, meta.etag);
                res.header("ETag", etag_hdr);
                res.body = "";
                return;
            }
        }
    }

    // --- Range request handling ---
    const range_header = req.header("range");
    if (range_header) |range_str| {
        // Parse the range header.
        const total_size = meta.size;
        const parsed = parseRangeHeader(range_str, total_size);
        if (parsed) |range| {
            // Valid range -- read the slice from storage.
            const obj_data = sb.getObject(bucket_name, object_key) catch |err| {
                return switch (err) {
                    error.NoSuchKey => sendS3Error(res, req_alloc, .NoSuchKey, object_key, request_id),
                    else => sendS3Error(res, req_alloc, .InternalError, object_key, request_id),
                };
            };
            const full_body = obj_data.body orelse "";

            const range_start = range.start;
            const range_end = range.end; // inclusive

            // Clamp to actual body size.
            if (range_start >= full_body.len) {
                // Set Content-Range header per S3 spec for 416 responses.
                const cr_hdr = try std.fmt.allocPrint(req_alloc, "bytes */{d}", .{total_size});
                res.header("Content-Range", cr_hdr);
                return sendS3Error(res, req_alloc, .InvalidRange, object_key, request_id);
            }
            const actual_end = @min(range_end, full_body.len - 1);

            const range_body = full_body[range_start .. actual_end + 1];

            server.setCommonHeaders(res, request_id);
            res.status = 206;

            // Content-Type from metadata.
            const ct = try req_alloc.dupe(u8, meta.content_type);
            res.header("Content-Type", ct);

            // ETag from metadata.
            const etag = try req_alloc.dupe(u8, meta.etag);
            res.header("ETag", etag);

            // Last-Modified.
            const last_mod_rfc = try formatLastModifiedRfc7231(req_alloc, meta.last_modified);
            res.header("Last-Modified", last_mod_rfc);

            // NOTE: Content-Length is set automatically by the httpz framework from res.body.len.
            // Do NOT set it explicitly here or it will appear twice in the response,
            // which breaks boto3 (it concatenates duplicate headers as "5243904, 5243904").

            // Content-Range: bytes start-end/total
            const cr = try std.fmt.allocPrint(req_alloc, "bytes {d}-{d}/{d}", .{ range_start, actual_end, total_size });
            res.header("Content-Range", cr);

            // Accept-Ranges.
            res.header("Accept-Ranges", "bytes");

            // User metadata headers.
            if (meta.user_metadata) |um| {
                emitUserMetadataHeaders(res, req_alloc, um);
            }

            res.body = range_body;
            return;
        } else {
            // Invalid range (unsatisfiable).
            // Set Content-Range header per S3 spec for 416 responses.
            const cr_hdr = try std.fmt.allocPrint(req_alloc, "bytes */{d}", .{meta.size});
            res.header("Content-Range", cr_hdr);
            return sendS3Error(res, req_alloc, .InvalidRange, object_key, request_id);
        }
    }

    // --- Normal (non-range) GET ---
    // Read from storage backend.
    const obj_data = sb.getObject(bucket_name, object_key) catch |err| {
        return switch (err) {
            error.NoSuchKey => sendS3Error(res, req_alloc, .NoSuchKey, object_key, request_id),
            else => sendS3Error(res, req_alloc, .InternalError, object_key, request_id),
        };
    };

    // Set response headers.
    server.setCommonHeaders(res, request_id);
    res.status = 200;

    // Content-Type from metadata.
    const ct = try req_alloc.dupe(u8, meta.content_type);
    res.header("Content-Type", ct);

    // ETag from metadata.
    const etag = try req_alloc.dupe(u8, meta.etag);
    res.header("ETag", etag);

    // Last-Modified: convert ISO 8601 to RFC 7231.
    const last_mod_rfc = try formatLastModifiedRfc7231(req_alloc, meta.last_modified);
    res.header("Last-Modified", last_mod_rfc);

    // NOTE: Content-Length is set automatically by the httpz framework from res.body.len.
    // Do NOT set it explicitly here or it will appear twice in the response,
    // which breaks boto3 (it concatenates duplicate headers as "5243904, 5243904").

    // Accept-Ranges: bytes (advertise range support).
    res.header("Accept-Ranges", "bytes");

    // User metadata headers.
    if (meta.user_metadata) |um| {
        emitUserMetadataHeaders(res, req_alloc, um);
    }

    // Body.
    res.body = obj_data.body orelse "";
}

/// HEAD /<bucket>/<key> -- Retrieve object metadata.
pub fn headObject(
    res: *tk.Response,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    object_key: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse {
        // HEAD responses have no body per S3 spec.
        sendResponse(res, "", 500, "application/xml", request_id);
        return;
    };

    // Check bucket exists.
    const bucket_exists = try ms.bucketExists(bucket_name);
    if (!bucket_exists) {
        // For HEAD, S3 returns 404 with no body.
        server.setCommonHeaders(res, request_id);
        res.status = 404;
        res.body = "";
        return;
    }

    // Look up object metadata.
    const obj_meta = try ms.getObjectMeta(bucket_name, object_key);
    if (obj_meta == null) {
        // HEAD on missing object: 404 with no body.
        server.setCommonHeaders(res, request_id);
        res.status = 404;
        res.body = "";
        return;
    }
    const meta = obj_meta.?;

    // Set response headers (no body for HEAD).
    server.setCommonHeaders(res, request_id);
    res.status = 200;

    // Content-Type from metadata.
    const ct = try req_alloc.dupe(u8, meta.content_type);
    res.header("Content-Type", ct);

    // ETag from metadata.
    const etag = try req_alloc.dupe(u8, meta.etag);
    res.header("ETag", etag);

    // Last-Modified.
    const last_mod_rfc = try formatLastModifiedRfc7231(req_alloc, meta.last_modified);
    res.header("Last-Modified", last_mod_rfc);

    // Content-Length: S3 HEAD must report the actual object size.
    // The httpz framework automatically writes Content-Length from res.body.len,
    // so we must NOT also set it via res.header() or it appears twice (breaking boto3).
    // For HEAD, we set res.body to a dummy buffer of the correct length so that
    // the framework computes the correct Content-Length. HTTP clients ignore
    // bodies in HEAD responses (RFC 7231 section 4.3.2).
    const cl = try std.fmt.allocPrint(req_alloc, "{d}", .{meta.size});
    res.header("Content-Length", cl);

    // Accept-Ranges.
    res.header("Accept-Ranges", "bytes");

    // User metadata headers.
    if (meta.user_metadata) |um| {
        emitUserMetadataHeaders(res, req_alloc, um);
    }

    // HEAD: no body.
    res.body = "";
}

/// DELETE /<bucket>/<key> -- Delete an object.
pub fn deleteObject(
    res: *tk.Response,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    object_key: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);
    const sb = server.global_storage_backend orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Delete is idempotent: always return 204.
    // Delete from storage (ignores FileNotFound).
    sb.deleteObject(bucket_name, object_key) catch {};

    // Delete from metadata store.
    const existed = ms.deleteObjectMeta(bucket_name, object_key) catch false;
    if (existed) {
        // Decrement object count only if a row was actually deleted.
        const current = metrics_mod.objects_total.load(.monotonic);
        if (current > 0) {
            _ = metrics_mod.objects_total.fetchSub(1, .monotonic);
        }
    }

    server.setCommonHeaders(res, request_id);
    res.status = 204;
    res.body = "";
}

/// POST /<bucket>?delete -- Delete multiple objects.
pub fn deleteObjects(
    res: *tk.Response,
    req: *tk.Request,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);
    const sb = server.global_storage_backend orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Check bucket exists.
    const bucket_exists = try ms.bucketExists(bucket_name);
    if (!bucket_exists) {
        return sendS3Error(res, req_alloc, .NoSuchBucket, bucket_name, request_id);
    }

    // Read and parse the Delete XML body.
    const body = req.body() orelse "";
    if (body.len == 0) {
        return sendS3Error(res, req_alloc, .MissingRequestBodyError, "/", request_id);
    }

    // Validate Content-MD5 header if present.
    if (req.header("content-md5")) |content_md5| {
        // Base64-decode the provided MD5.
        var decoded_md5: [16]u8 = undefined;
        std.base64.standard.Decoder.decode(&decoded_md5, content_md5) catch {
            return sendS3Error(res, req_alloc, .InvalidDigest, "/", request_id);
        };
        // Compute MD5 of the request body.
        var computed_md5: [std.crypto.hash.Md5.digest_length]u8 = undefined;
        std.crypto.hash.Md5.hash(body, &computed_md5, .{});
        // Compare digests.
        if (!std.mem.eql(u8, &decoded_md5, &computed_md5)) {
            return sendS3Error(res, req_alloc, .BadDigest, "/", request_id);
        }
    }

    // Extract <Quiet>true</Quiet> if present.
    const quiet = parseQuietFlag(body);

    // Extract all <Key>...</Key> elements from the body.
    const keys = try extractXmlElements(req_alloc, body, "Key");
    if (keys.len == 0) {
        return sendS3Error(res, req_alloc, .MalformedXML, "/", request_id);
    }

    // Batch delete from metadata store.
    const batch_results = ms.deleteObjectsMeta(bucket_name, keys) catch null;
    defer {
        if (batch_results) |br| {
            if (server.global_allocator) |gpa| gpa.free(br);
        }
    }

    // Delete from storage (per-key, since files are individual).
    var deleted_keys: std.ArrayList([]const u8) = .empty;
    var error_keys: std.ArrayList([]const u8) = .empty;

    for (keys, 0..) |key, i| {
        sb.deleteObject(bucket_name, key) catch {};

        // Update metrics if metadata row was deleted.
        if (batch_results) |br| {
            if (i < br.len and br[i]) {
                const current = metrics_mod.objects_total.load(.monotonic);
                if (current > 0) {
                    _ = metrics_mod.objects_total.fetchSub(1, .monotonic);
                }
            }
        }

        // S3 always reports success even for non-existent keys.
        try deleted_keys.append(req_alloc, key);
    }

    // Render XML response.

    const xml_body = if (quiet)
        try xml_mod.renderDeleteResult(req_alloc, &.{}, try error_keys.toOwnedSlice(req_alloc))
    else
        try xml_mod.renderDeleteResult(req_alloc, try deleted_keys.toOwnedSlice(req_alloc), try error_keys.toOwnedSlice(req_alloc));

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.content_type = .XML;
    res.body = xml_body;
}

/// PUT /<bucket>/<key> with x-amz-copy-source -- Copy an object.
pub fn copyObject(
    res: *tk.Response,
    req: *tk.Request,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    object_key: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);
    const sb = server.global_storage_backend orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Parse x-amz-copy-source header: "/<source-bucket>/<source-key>" or "<source-bucket>/<source-key>"
    const copy_source_raw = req.header("x-amz-copy-source") orelse
        return sendS3Error(res, req_alloc, .InvalidArgument, "/", request_id);

    // URL-decode the copy source.
    const copy_source_decoded = try uriDecode(req_alloc, copy_source_raw);

    // Trim leading slash.
    const copy_source = if (copy_source_decoded.len > 0 and copy_source_decoded[0] == '/')
        copy_source_decoded[1..]
    else
        copy_source_decoded;

    // Split into bucket and key.
    const slash_idx = std.mem.indexOfScalar(u8, copy_source, '/') orelse
        return sendS3Error(res, req_alloc, .InvalidArgument, "/", request_id);
    const src_bucket = copy_source[0..slash_idx];
    const src_key = copy_source[slash_idx + 1 ..];

    if (src_key.len == 0) {
        return sendS3Error(res, req_alloc, .InvalidArgument, "/", request_id);
    }

    // Check source bucket exists.
    const src_bucket_exists = try ms.bucketExists(src_bucket);
    if (!src_bucket_exists) {
        return sendS3Error(res, req_alloc, .NoSuchBucket, src_bucket, request_id);
    }

    // Check source object exists.
    const src_meta_opt = try ms.getObjectMeta(src_bucket, src_key);
    if (src_meta_opt == null) {
        return sendS3Error(res, req_alloc, .NoSuchKey, src_key, request_id);
    }
    const src_meta = src_meta_opt.?;

    // Check destination bucket exists.
    const dst_bucket_exists = try ms.bucketExists(bucket_name);
    if (!dst_bucket_exists) {
        return sendS3Error(res, req_alloc, .NoSuchBucket, bucket_name, request_id);
    }

    // Copy the file in storage backend.
    _ = sb.copyObject(src_bucket, src_key, bucket_name, object_key) catch |err| {
        return switch (err) {
            error.NoSuchKey => sendS3Error(res, req_alloc, .NoSuchKey, src_key, request_id),
            else => sendS3Error(res, req_alloc, .InternalError, object_key, request_id),
        };
    };

    // Now we need to re-read the destination file to compute the correct ETag.
    // Since copyObject in local backend does a file copy, we need the new MD5.
    const obj_data = sb.getObject(bucket_name, object_key) catch |err| {
        return switch (err) {
            error.NoSuchKey => sendS3Error(res, req_alloc, .InternalError, object_key, request_id),
            else => sendS3Error(res, req_alloc, .InternalError, object_key, request_id),
        };
    };
    const file_body = obj_data.body orelse "";

    // Compute MD5 ETag of the copied content.
    var md5_hash: [std.crypto.hash.Md5.digest_length]u8 = undefined;
    std.crypto.hash.Md5.hash(file_body, &md5_hash, .{});
    const hex = std.fmt.bytesToHex(md5_hash, .lower);
    const etag = try std.fmt.allocPrint(req_alloc, "\"{s}\"", .{@as([]const u8, &hex)});

    // Determine metadata directive: COPY (default) or REPLACE.
    const metadata_directive = req.header("x-amz-metadata-directive") orelse "COPY";

    const last_modified = try formatIso8601(req_alloc);

    // Build the destination metadata.
    if (std.ascii.eqlIgnoreCase(metadata_directive, "REPLACE")) {
        // REPLACE: use metadata from the request, not the source.
        const user_metadata = try extractUserMetadata(req_alloc, req);
        const content_type = req.header("content-type") orelse src_meta.content_type;

        // Derive owner.
        const access_key = server.global_access_key;
        const owner_id = try deriveOwnerId(req_alloc, access_key);
        const owner_display = access_key;
        const acl_json = try buildDefaultAclJson(req_alloc, owner_id, owner_display);

        try ms.putObjectMeta(.{
            .bucket = bucket_name,
            .key = object_key,
            .size = @intCast(file_body.len),
            .etag = etag,
            .content_type = content_type,
            .last_modified = last_modified,
            .storage_class = "STANDARD",
            .user_metadata = user_metadata,
            .acl = acl_json,
        });
    } else {
        // COPY: copy metadata from source.
        try ms.putObjectMeta(.{
            .bucket = bucket_name,
            .key = object_key,
            .size = src_meta.size,
            .etag = etag,
            .content_type = src_meta.content_type,
            .last_modified = last_modified,
            .storage_class = src_meta.storage_class,
            .user_metadata = src_meta.user_metadata,
            .acl = src_meta.acl,
        });
    }

    // Update metrics.
    _ = metrics_mod.objects_total.fetchAdd(1, .monotonic);

    // Render CopyObjectResult XML.

    const xml_body = try xml_mod.renderCopyObjectResult(req_alloc, etag, last_modified);

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.content_type = .XML;
    res.body = xml_body;
}

/// GET /<bucket>?list-type=2 -- List objects V2.
pub fn listObjectsV2(
    res: *tk.Response,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    query: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Check bucket exists.
    const bucket_exists = try ms.bucketExists(bucket_name);
    if (!bucket_exists) {
        return sendS3Error(res, req_alloc, .NoSuchBucket, bucket_name, request_id);
    }

    // Parse query parameters (URL-decoded for user-facing values).
    const prefix = server.getQueryParamDecoded(req_alloc, query, "prefix") orelse "";
    const delimiter = server.getQueryParamDecoded(req_alloc, query, "delimiter") orelse "";
    const start_after_raw = server.getQueryParamDecoded(req_alloc, query, "start-after") orelse "";
    const continuation_token = server.getQueryParamDecoded(req_alloc, query, "continuation-token") orelse "";
    const max_keys_str = server.getQueryParamValue(query, "max-keys") orelse "1000";

    // Parse max-keys.
    const max_keys: u32 = std.fmt.parseInt(u32, max_keys_str, 10) catch 1000;
    const effective_max_keys = if (max_keys > 1000) @as(u32, 1000) else max_keys;

    // Determine effective start_after: continuation-token takes priority.
    const start_after = if (continuation_token.len > 0) continuation_token else start_after_raw;

    // Query metadata store.
    const result = try ms.listObjectsMeta(bucket_name, prefix, delimiter, start_after, effective_max_keys);

    // Build ListObjectEntry array for the XML renderer.

    var entries: std.ArrayList(xml_mod.ListObjectEntry) = .empty;
    for (result.objects) |obj| {
        try entries.append(req_alloc, xml_mod.ListObjectEntry{
            .key = obj.key,
            .last_modified = obj.last_modified,
            .etag = obj.etag,
            .size = obj.size,
            .storage_class = obj.storage_class,
        });
    }

    const key_count = entries.items.len + result.common_prefixes.len;

    const xml_body = try xml_mod.renderListObjectsV2Result(
        req_alloc,
        bucket_name,
        prefix,
        delimiter,
        effective_max_keys,
        key_count,
        result.is_truncated,
        try entries.toOwnedSlice(req_alloc),
        result.common_prefixes,
        continuation_token,
        if (result.next_continuation_token) |t| t else "",
        start_after_raw,
    );

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.content_type = .XML;
    res.body = xml_body;
}

/// GET /<bucket> -- List objects V1.
pub fn listObjectsV1(
    res: *tk.Response,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    query: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Check bucket exists.
    const bucket_exists = try ms.bucketExists(bucket_name);
    if (!bucket_exists) {
        return sendS3Error(res, req_alloc, .NoSuchBucket, bucket_name, request_id);
    }

    // Parse query parameters (URL-decoded for user-facing values).
    const prefix = server.getQueryParamDecoded(req_alloc, query, "prefix") orelse "";
    const delimiter = server.getQueryParamDecoded(req_alloc, query, "delimiter") orelse "";
    const marker = server.getQueryParamDecoded(req_alloc, query, "marker") orelse "";
    const max_keys_str = server.getQueryParamValue(query, "max-keys") orelse "1000";

    // Parse max-keys.
    const max_keys: u32 = std.fmt.parseInt(u32, max_keys_str, 10) catch 1000;
    const effective_max_keys = if (max_keys > 1000) @as(u32, 1000) else max_keys;

    // Query metadata store. V1 uses "marker" as start_after.
    const result = try ms.listObjectsMeta(bucket_name, prefix, delimiter, marker, effective_max_keys);

    // Build ListObjectEntry array.

    var entries: std.ArrayList(xml_mod.ListObjectEntry) = .empty;
    for (result.objects) |obj| {
        try entries.append(req_alloc, xml_mod.ListObjectEntry{
            .key = obj.key,
            .last_modified = obj.last_modified,
            .etag = obj.etag,
            .size = obj.size,
            .storage_class = obj.storage_class,
        });
    }

    const xml_body = try xml_mod.renderListObjectsV1Result(
        req_alloc,
        bucket_name,
        prefix,
        delimiter,
        effective_max_keys,
        result.is_truncated,
        try entries.toOwnedSlice(req_alloc),
        result.common_prefixes,
        marker,
        if (result.next_marker) |m| m else "",
    );

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.content_type = .XML;
    res.body = xml_body;
}

/// GET /<bucket>/<key>?acl -- Get object ACL.
pub fn getObjectAcl(
    res: *tk.Response,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    object_key: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Check bucket exists.
    const bucket_exists = try ms.bucketExists(bucket_name);
    if (!bucket_exists) {
        return sendS3Error(res, req_alloc, .NoSuchBucket, bucket_name, request_id);
    }

    // Check object exists and get its metadata.
    const obj_meta = try ms.getObjectMeta(bucket_name, object_key);
    if (obj_meta == null) {
        return sendS3Error(res, req_alloc, .NoSuchKey, object_key, request_id);
    }
    const meta = obj_meta.?;

    // Derive owner from access key.
    const access_key = server.global_access_key;
    const owner_id = try deriveOwnerId(req_alloc, access_key);
    const owner_display = access_key;

    // Render ACL XML using the stored ACL JSON.
    const acl_xml = try xml_mod.renderAccessControlPolicy(
        req_alloc,
        owner_id,
        owner_display,
        meta.acl,
    );

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.content_type = .XML;
    res.body = acl_xml;
}

/// PUT /<bucket>/<key>?acl -- Set object ACL.
pub fn putObjectAcl(
    res: *tk.Response,
    req: *tk.Request,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    object_key: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Check bucket exists.
    const bucket_exists = try ms.bucketExists(bucket_name);
    if (!bucket_exists) {
        return sendS3Error(res, req_alloc, .NoSuchBucket, bucket_name, request_id);
    }

    // Check object exists.
    const obj_exists = try ms.objectExists(bucket_name, object_key);
    if (!obj_exists) {
        return sendS3Error(res, req_alloc, .NoSuchKey, object_key, request_id);
    }

    // Derive owner.
    const access_key = server.global_access_key;
    const owner_id = try deriveOwnerId(req_alloc, access_key);
    const owner_display = access_key;

    // 1D: Mutual exclusion -- x-amz-acl and x-amz-grant-* cannot coexist.
    const canned_acl_hdr = req.header("x-amz-acl");
    const has_grants = hasAnyGrantHeader(req);
    if (canned_acl_hdr != null and has_grants) {
        return sendS3ErrorWithMessage(res, req_alloc, .InvalidArgument, "Specifying both x-amz-acl and x-amz-grant headers is not allowed", object_key, request_id);
    }

    // Check for x-amz-acl canned ACL header.
    if (canned_acl_hdr) |ca| {
        if (try buildCannedAclJson(req_alloc, ca, owner_id, owner_display)) |acl_json| {
            try ms.updateObjectAcl(bucket_name, object_key, acl_json);
            server.setCommonHeaders(res, request_id);
            res.status = 200;
            res.body = "";
            return;
        }
        // Unknown canned ACL -- fall through to default.
    }

    // 1C: Check for x-amz-grant-* headers.
    if (has_grants) {
        if (try parseGrantHeaders(req_alloc, req, owner_id, owner_display)) |grant_acl| {
            try ms.updateObjectAcl(bucket_name, object_key, grant_acl);
            server.setCommonHeaders(res, request_id);
            res.status = 200;
            res.body = "";
            return;
        }
    }

    // No canned ACL or grant headers -- set default private ACL.
    const acl_json = try buildDefaultAclJson(req_alloc, owner_id, owner_display);
    try ms.updateObjectAcl(bucket_name, object_key, acl_json);

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.body = "";
}

// ---------------------------------------------------------------------------
// Range request helpers
// ---------------------------------------------------------------------------

/// Parsed range with start and end (both inclusive).
const ParsedRange = struct {
    start: usize,
    end: usize, // inclusive
};

/// Parse a Range header value like "bytes=0-499", "bytes=500-", "bytes=-500".
/// Returns null if the range is invalid or unsatisfiable for the given total_size.
fn parseRangeHeader(range_str: []const u8, total_size: u64) ?ParsedRange {
    // Must start with "bytes="
    if (!std.mem.startsWith(u8, range_str, "bytes=")) return null;
    const range_spec = range_str["bytes=".len..];
    if (range_spec.len == 0) return null;

    const total: usize = @intCast(total_size);
    if (total == 0) return null;

    // Find the dash separator.
    const dash_idx = std.mem.indexOfScalar(u8, range_spec, '-') orelse return null;

    const start_str = range_spec[0..dash_idx];
    const end_str = range_spec[dash_idx + 1 ..];

    if (start_str.len == 0 and end_str.len == 0) return null;

    if (start_str.len == 0) {
        // Suffix range: bytes=-N (last N bytes)
        const suffix_len = std.fmt.parseInt(usize, end_str, 10) catch return null;
        if (suffix_len == 0) return null;
        if (suffix_len >= total) {
            return ParsedRange{ .start = 0, .end = total - 1 };
        }
        return ParsedRange{ .start = total - suffix_len, .end = total - 1 };
    }

    const start = std.fmt.parseInt(usize, start_str, 10) catch return null;
    if (start >= total) return null;

    if (end_str.len == 0) {
        // Open-ended range: bytes=N- (from N to end)
        return ParsedRange{ .start = start, .end = total - 1 };
    }

    // Standard range: bytes=start-end
    const end = std.fmt.parseInt(usize, end_str, 10) catch return null;
    if (end < start) return null;

    return ParsedRange{ .start = start, .end = @min(end, total - 1) };
}

// ---------------------------------------------------------------------------
// Conditional request helpers
// ---------------------------------------------------------------------------

/// Check if the object's ETag matches a given ETag condition.
/// Handles wildcard "*" and optional quote wrapping differences.
fn etagMatch(object_etag: []const u8, condition_etag: []const u8) bool {
    if (std.mem.eql(u8, condition_etag, "*")) return true;

    // Normalize: strip optional whitespace.
    const cond = std.mem.trim(u8, condition_etag, " ");
    const obj = std.mem.trim(u8, object_etag, " ");

    // Direct comparison.
    if (std.mem.eql(u8, obj, cond)) return true;

    // Compare without surrounding quotes.
    const obj_stripped = stripQuotes(obj);
    const cond_stripped = stripQuotes(cond);
    return std.mem.eql(u8, obj_stripped, cond_stripped);
}

/// Strip surrounding double quotes from a string.
fn stripQuotes(s: []const u8) []const u8 {
    if (s.len >= 2 and s[0] == '"' and s[s.len - 1] == '"') {
        return s[1 .. s.len - 1];
    }
    return s;
}

/// Check if the object has been modified since a given HTTP date string.
/// Returns true if the object was modified AFTER the given date.
/// Compares using the ISO 8601 last_modified against a parsed RFC 7231 / RFC 2822 date.
///
/// We use a simple approach: parse the condition date to epoch seconds and
/// parse the object's last_modified ISO 8601 date to epoch seconds, then compare.
fn isModifiedSince(last_modified_iso: []const u8, condition_date: []const u8) bool {
    const obj_epoch = parseIso8601ToEpoch(last_modified_iso) orelse return false;
    const cond_epoch = parseHttpDateToEpoch(condition_date) orelse return false;
    return obj_epoch > cond_epoch;
}

/// Parse an ISO 8601 date "2026-02-23T12:00:00.000Z" to epoch seconds.
fn parseIso8601ToEpoch(iso: []const u8) ?i64 {
    if (iso.len < 19) return null;
    const year = std.fmt.parseInt(u16, iso[0..4], 10) catch return null;
    const month = std.fmt.parseInt(u8, iso[5..7], 10) catch return null;
    const day = std.fmt.parseInt(u8, iso[8..10], 10) catch return null;
    const hours = std.fmt.parseInt(u8, iso[11..13], 10) catch return null;
    const minutes = std.fmt.parseInt(u8, iso[14..16], 10) catch return null;
    const seconds = std.fmt.parseInt(u8, iso[17..19], 10) catch return null;

    return dateToEpoch(year, month, day, hours, minutes, seconds);
}

/// Parse an HTTP date (RFC 7231 / RFC 2822) to epoch seconds.
/// Format: "Sun, 23 Feb 2026 12:00:00 GMT" or similar.
fn parseHttpDateToEpoch(date_str: []const u8) ?i64 {
    const trimmed = std.mem.trim(u8, date_str, " ");

    // Skip day-of-week + comma + space: "Sun, "
    const comma_idx = std.mem.indexOfScalar(u8, trimmed, ',') orelse return null;
    if (comma_idx + 2 >= trimmed.len) return null;
    const rest = std.mem.trimLeft(u8, trimmed[comma_idx + 1 ..], " ");

    // Now parse: "23 Feb 2026 12:00:00 GMT"
    // Split by spaces.
    var parts_iter = std.mem.splitScalar(u8, rest, ' ');
    const day_str = parts_iter.next() orelse return null;
    const month_str = parts_iter.next() orelse return null;
    const year_str = parts_iter.next() orelse return null;
    const time_str = parts_iter.next() orelse return null;
    // "GMT" is optional to parse.

    const day = std.fmt.parseInt(u8, day_str, 10) catch return null;
    const month = monthNameToNumber(month_str) orelse return null;
    const year = std.fmt.parseInt(u16, year_str, 10) catch return null;

    // Parse time "HH:MM:SS"
    if (time_str.len < 8) return null;
    const hours = std.fmt.parseInt(u8, time_str[0..2], 10) catch return null;
    const minutes = std.fmt.parseInt(u8, time_str[3..5], 10) catch return null;
    const seconds = std.fmt.parseInt(u8, time_str[6..8], 10) catch return null;

    return dateToEpoch(year, month, day, hours, minutes, seconds);
}

/// Convert a month name abbreviation to its 1-based number.
fn monthNameToNumber(name: []const u8) ?u8 {
    const months = [12][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
    for (months, 0..) |m, i| {
        if (std.ascii.eqlIgnoreCase(name, m)) return @intCast(i + 1);
    }
    return null;
}

/// Convert date components to Unix epoch seconds.
fn dateToEpoch(year: u16, month: u8, day: u8, hours: u8, minutes: u8, seconds: u8) ?i64 {
    if (year < 1970 or month < 1 or month > 12 or day < 1 or day > 31) return null;

    const month_days_table = [12]u16{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    const year_type: std.time.epoch.Year = @intCast(year);
    const is_leap = std.time.epoch.isLeapYear(year_type);

    var total_days: i64 = 0;
    // Days from years.
    var y: u16 = 1970;
    while (y < year) : (y += 1) {
        total_days += if (std.time.epoch.isLeapYear(@intCast(y))) @as(i64, 366) else 365;
    }
    // Days from months.
    var m: u8 = 1;
    while (m < month) : (m += 1) {
        var md: i64 = month_days_table[m - 1];
        if (m == 2 and is_leap) md += 1;
        total_days += md;
    }
    total_days += @as(i64, day) - 1;

    return total_days * 86400 + @as(i64, hours) * 3600 + @as(i64, minutes) * 60 + @as(i64, seconds);
}

// ---------------------------------------------------------------------------
// Canned ACL helper (shared with bucket handlers)
// ---------------------------------------------------------------------------

/// Check whether any x-amz-grant-* header is present in the request.
fn hasAnyGrantHeader(req: *tk.Request) bool {
    const grant_headers = [_][]const u8{
        "x-amz-grant-full-control",
        "x-amz-grant-read",
        "x-amz-grant-read-acp",
        "x-amz-grant-write",
        "x-amz-grant-write-acp",
    };
    for (grant_headers) |hdr| {
        if (req.header(hdr) != null) return true;
    }
    return false;
}

/// Parse x-amz-grant-* headers and build an ACL JSON string.
/// Returns null if no grant headers are present.
/// Value format: `id="canonical-user-id"` or `uri="http://acs.amazonaws.com/groups/..."`, comma-separated.
fn parseGrantHeaders(alloc: std.mem.Allocator, req: *tk.Request, owner_id: []const u8, owner_display: []const u8) !?[]u8 {
    const GrantHeader = struct {
        header_name: []const u8,
        permission: []const u8,
    };
    const grant_mappings = [_]GrantHeader{
        .{ .header_name = "x-amz-grant-full-control", .permission = "FULL_CONTROL" },
        .{ .header_name = "x-amz-grant-read", .permission = "READ" },
        .{ .header_name = "x-amz-grant-read-acp", .permission = "READ_ACP" },
        .{ .header_name = "x-amz-grant-write", .permission = "WRITE" },
        .{ .header_name = "x-amz-grant-write-acp", .permission = "WRITE_ACP" },
    };

    var found_any = false;
    for (grant_mappings) |mapping| {
        if (req.header(mapping.header_name) != null) {
            found_any = true;
            break;
        }
    }
    if (!found_any) return null;

    // Build the grants array JSON.
    var grants_buf = std.ArrayList(u8).empty;
    defer grants_buf.deinit(alloc);
    var first_grant = true;

    for (grant_mappings) |mapping| {
        const header_val = req.header(mapping.header_name) orelse continue;

        // Parse comma-separated grantees: id="...", uri="..."
        var grantee_iter = std.mem.splitScalar(u8, header_val, ',');
        while (grantee_iter.next()) |raw_grantee| {
            const grantee = std.mem.trim(u8, raw_grantee, " ");
            if (grantee.len == 0) continue;

            if (!first_grant) {
                try grants_buf.append(alloc, ',');
            }
            first_grant = false;

            if (std.mem.startsWith(u8, grantee, "id=\"")) {
                // CanonicalUser: id="canonical-user-id"
                const id_start = 4; // len of `id="`
                const id_end = std.mem.indexOfScalarPos(u8, grantee, id_start, '"') orelse grantee.len;
                const user_id = grantee[id_start..id_end];

                const grant_json = try std.fmt.allocPrint(alloc,
                    \\{{"grantee":{{"type":"CanonicalUser","id":"{s}","display_name":"{s}"}},"permission":"{s}"}}
                , .{ user_id, user_id, mapping.permission });
                defer alloc.free(grant_json);
                try grants_buf.appendSlice(alloc, grant_json);
            } else if (std.mem.startsWith(u8, grantee, "uri=\"")) {
                // Group: uri="http://..."
                const uri_start = 5; // len of `uri="`
                const uri_end = std.mem.indexOfScalarPos(u8, grantee, uri_start, '"') orelse grantee.len;
                const uri = grantee[uri_start..uri_end];

                const grant_json = try std.fmt.allocPrint(alloc,
                    \\{{"grantee":{{"type":"Group","uri":"{s}"}},"permission":"{s}"}}
                , .{ uri, mapping.permission });
                defer alloc.free(grant_json);
                try grants_buf.appendSlice(alloc, grant_json);
            }
            // Skip unrecognized grantee formats.
        }
    }

    // Build full ACL JSON with owner.
    const grants_str = try grants_buf.toOwnedSlice(alloc);
    defer alloc.free(grants_str);

    return try std.fmt.allocPrint(alloc,
        \\{{"owner":{{"id":"{s}","display_name":"{s}"}},"grants":[{s}]}}
    , .{ owner_id, owner_display, grants_str });
}

/// Build a canned ACL JSON string. Returns null for unknown canned ACL names.
fn buildCannedAclJson(alloc: std.mem.Allocator, canned_acl: []const u8, owner_id: []const u8, owner_display: []const u8) !?[]u8 {
    if (std.mem.eql(u8, canned_acl, "private")) {
        return try buildDefaultAclJson(alloc, owner_id, owner_display);
    } else if (std.mem.eql(u8, canned_acl, "public-read")) {
        return try std.fmt.allocPrint(alloc,
            \\{{"owner":{{"id":"{s}","display_name":"{s}"}},"grants":[{{"grantee":{{"type":"CanonicalUser","id":"{s}","display_name":"{s}"}},"permission":"FULL_CONTROL"}},{{"grantee":{{"type":"Group","uri":"http://acs.amazonaws.com/groups/global/AllUsers"}},"permission":"READ"}}]}}
        , .{ owner_id, owner_display, owner_id, owner_display });
    } else if (std.mem.eql(u8, canned_acl, "public-read-write")) {
        return try std.fmt.allocPrint(alloc,
            \\{{"owner":{{"id":"{s}","display_name":"{s}"}},"grants":[{{"grantee":{{"type":"CanonicalUser","id":"{s}","display_name":"{s}"}},"permission":"FULL_CONTROL"}},{{"grantee":{{"type":"Group","uri":"http://acs.amazonaws.com/groups/global/AllUsers"}},"permission":"READ"}},{{"grantee":{{"type":"Group","uri":"http://acs.amazonaws.com/groups/global/AllUsers"}},"permission":"WRITE"}}]}}
        , .{ owner_id, owner_display, owner_id, owner_display });
    } else if (std.mem.eql(u8, canned_acl, "authenticated-read")) {
        return try std.fmt.allocPrint(alloc,
            \\{{"owner":{{"id":"{s}","display_name":"{s}"}},"grants":[{{"grantee":{{"type":"CanonicalUser","id":"{s}","display_name":"{s}"}},"permission":"FULL_CONTROL"}},{{"grantee":{{"type":"Group","uri":"http://acs.amazonaws.com/groups/global/AuthenticatedUsers"}},"permission":"READ"}}]}}
        , .{ owner_id, owner_display, owner_id, owner_display });
    }
    return null; // unknown canned ACL
}

// ---------------------------------------------------------------------------
// XML parsing helpers for DeleteObjects
// ---------------------------------------------------------------------------

/// Parse the <Quiet>true/false</Quiet> flag from Delete XML body.
fn parseQuietFlag(body: []const u8) bool {
    const open_tag = "<Quiet>";
    const close_tag = "</Quiet>";
    const start = std.mem.indexOf(u8, body, open_tag) orelse return false;
    const content_start = start + open_tag.len;
    const end = std.mem.indexOf(u8, body[content_start..], close_tag) orelse return false;
    const value = body[content_start .. content_start + end];
    return std.mem.eql(u8, value, "true");
}

/// Extract all occurrences of <tag>content</tag> from an XML string.
/// Returns a slice of content strings, allocated with the given allocator.
fn extractXmlElements(alloc: std.mem.Allocator, xml_body: []const u8, tag: []const u8) ![]const []const u8 {
    var results: std.ArrayList([]const u8) = .empty;
    errdefer results.deinit(alloc);

    const open_tag_start = "<";
    const open_tag_end = ">";
    const close_tag_start = "</";

    // Build open and close tag strings.
    const open_full = try std.fmt.allocPrint(alloc, "{s}{s}{s}", .{ open_tag_start, tag, open_tag_end });
    defer alloc.free(open_full);
    const close_full = try std.fmt.allocPrint(alloc, "{s}{s}{s}", .{ close_tag_start, tag, open_tag_end });
    defer alloc.free(close_full);

    var pos: usize = 0;
    while (pos < xml_body.len) {
        const start = std.mem.indexOf(u8, xml_body[pos..], open_full) orelse break;
        const content_start = pos + start + open_full.len;
        const end = std.mem.indexOf(u8, xml_body[content_start..], close_full) orelse break;
        const content = xml_body[content_start .. content_start + end];
        try results.append(alloc, content);
        pos = content_start + end + close_full.len;
    }

    return results.toOwnedSlice(alloc);
}

// ---------------------------------------------------------------------------
// URI decoding helper
// ---------------------------------------------------------------------------

/// Decode percent-encoded URI strings (e.g., "%2F" -> "/").
fn uriDecode(alloc: std.mem.Allocator, input: []const u8) ![]u8 {
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(alloc);

    var i: usize = 0;
    while (i < input.len) {
        if (input[i] == '%' and i + 2 < input.len) {
            const high = hexCharToNibble(input[i + 1]);
            const low = hexCharToNibble(input[i + 2]);
            if (high != null and low != null) {
                try result.append(alloc, (high.? << 4) | low.?);
                i += 3;
                continue;
            }
        }
        if (input[i] == '+') {
            try result.append(alloc, ' ');
        } else {
            try result.append(alloc, input[i]);
        }
        i += 1;
    }

    return result.toOwnedSlice(alloc);
}

fn hexCharToNibble(ch: u8) ?u8 {
    return switch (ch) {
        '0'...'9' => ch - '0',
        'a'...'f' => ch - 'a' + 10,
        'A'...'F' => ch - 'A' + 10,
        else => null,
    };
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/// Convert an ISO 8601 date string like "2026-02-23T12:00:00.000Z" to
/// RFC 7231 format like "Sun, 23 Feb 2026 12:00:00 GMT".
fn formatLastModifiedRfc7231(alloc: std.mem.Allocator, iso8601: []const u8) ![]u8 {
    // Parse the ISO 8601 string: YYYY-MM-DDThh:mm:ss.mmmZ
    if (iso8601.len < 19) {
        // If parsing fails, return the original string.
        return try alloc.dupe(u8, iso8601);
    }

    const year = std.fmt.parseInt(u16, iso8601[0..4], 10) catch return try alloc.dupe(u8, iso8601);
    const month = std.fmt.parseInt(u8, iso8601[5..7], 10) catch return try alloc.dupe(u8, iso8601);
    const day = std.fmt.parseInt(u8, iso8601[8..10], 10) catch return try alloc.dupe(u8, iso8601);
    const hours = std.fmt.parseInt(u8, iso8601[11..13], 10) catch return try alloc.dupe(u8, iso8601);
    const minutes = std.fmt.parseInt(u8, iso8601[14..16], 10) catch return try alloc.dupe(u8, iso8601);
    const seconds = std.fmt.parseInt(u8, iso8601[17..19], 10) catch return try alloc.dupe(u8, iso8601);

    // Compute day of week.
    // Calculate epoch days from date components.
    const year_type: std.time.epoch.Year = @intCast(year);
    const is_leap = std.time.epoch.isLeapYear(year_type);

    // Days from years since epoch (1970).
    var total_days: i64 = 0;
    if (year >= 1970) {
        var y: u16 = 1970;
        while (y < year) : (y += 1) {
            total_days += if (std.time.epoch.isLeapYear(@intCast(y))) @as(i64, 366) else 365;
        }
    }
    // Days from months in current year.
    const month_days_table = [12]u16{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    var m: u8 = 1;
    while (m < month) : (m += 1) {
        var md: u16 = month_days_table[m - 1];
        if (m == 2 and is_leap) md += 1;
        total_days += md;
    }
    total_days += day - 1;

    // Day of week: 1970-01-01 was Thursday (4).
    const dow_idx: usize = @intCast(@mod(total_days + 4, 7));

    const day_names = [7][]const u8{ "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
    const month_names = [12][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

    return std.fmt.allocPrint(alloc, "{s}, {d:0>2} {s} {d:0>4} {d:0>2}:{d:0>2}:{d:0>2} GMT", .{
        day_names[dow_idx],
        day,
        month_names[month - 1],
        year,
        hours,
        minutes,
        seconds,
    });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "formatLastModifiedRfc7231 converts ISO 8601 to RFC 7231" {
    const alloc = std.testing.allocator;

    // 2026-02-23 is a Monday.
    const result = try formatLastModifiedRfc7231(alloc, "2026-02-23T12:00:00.000Z");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("Mon, 23 Feb 2026 12:00:00 GMT", result);
}

test "formatLastModifiedRfc7231 handles short input" {
    const alloc = std.testing.allocator;
    const result = try formatLastModifiedRfc7231(alloc, "short");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("short", result);
}

test "parseQuietFlag: returns true for Quiet=true" {
    try std.testing.expect(parseQuietFlag("<Delete><Quiet>true</Quiet><Object><Key>a</Key></Object></Delete>"));
}

test "parseQuietFlag: returns false for Quiet=false" {
    try std.testing.expect(!parseQuietFlag("<Delete><Quiet>false</Quiet><Object><Key>a</Key></Object></Delete>"));
}

test "parseQuietFlag: returns false when absent" {
    try std.testing.expect(!parseQuietFlag("<Delete><Object><Key>a</Key></Object></Delete>"));
}

test "extractXmlElements: extracts Key elements" {
    const alloc = std.testing.allocator;
    const body = "<Delete><Object><Key>a.txt</Key></Object><Object><Key>b.txt</Key></Object></Delete>";
    const keys = try extractXmlElements(alloc, body, "Key");
    defer alloc.free(keys);

    try std.testing.expectEqual(@as(usize, 2), keys.len);
    try std.testing.expectEqualStrings("a.txt", keys[0]);
    try std.testing.expectEqualStrings("b.txt", keys[1]);
}

test "extractXmlElements: no matches returns empty" {
    const alloc = std.testing.allocator;
    const keys = try extractXmlElements(alloc, "<Delete></Delete>", "Key");
    defer alloc.free(keys);
    try std.testing.expectEqual(@as(usize, 0), keys.len);
}

test "uriDecode: decodes percent-encoded characters" {
    const alloc = std.testing.allocator;
    const result = try uriDecode(alloc, "foo%2Fbar%20baz");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("foo/bar baz", result);
}

test "uriDecode: passthrough for plain string" {
    const alloc = std.testing.allocator;
    const result = try uriDecode(alloc, "hello-world");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("hello-world", result);
}

// ---------------------------------------------------------------------------
// Range request tests
// ---------------------------------------------------------------------------

test "parseRangeHeader: standard range bytes=0-4" {
    const result = parseRangeHeader("bytes=0-4", 16);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 0), result.?.start);
    try std.testing.expectEqual(@as(usize, 4), result.?.end);
}

test "parseRangeHeader: suffix range bytes=-5" {
    const result = parseRangeHeader("bytes=-5", 16);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 11), result.?.start);
    try std.testing.expectEqual(@as(usize, 15), result.?.end);
}

test "parseRangeHeader: open-ended range bytes=10-" {
    const result = parseRangeHeader("bytes=10-", 16);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 10), result.?.start);
    try std.testing.expectEqual(@as(usize, 15), result.?.end);
}

test "parseRangeHeader: unsatisfiable range" {
    const result = parseRangeHeader("bytes=100-200", 5);
    try std.testing.expect(result == null);
}

test "parseRangeHeader: invalid format" {
    try std.testing.expect(parseRangeHeader("invalid", 100) == null);
    try std.testing.expect(parseRangeHeader("bytes=", 100) == null);
    try std.testing.expect(parseRangeHeader("bytes=-", 100) == null);
}

test "parseRangeHeader: suffix larger than total" {
    const result = parseRangeHeader("bytes=-100", 16);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 0), result.?.start);
    try std.testing.expectEqual(@as(usize, 15), result.?.end);
}

// ---------------------------------------------------------------------------
// Conditional request tests
// ---------------------------------------------------------------------------

test "etagMatch: exact match" {
    try std.testing.expect(etagMatch("\"abc123\"", "\"abc123\""));
}

test "etagMatch: wildcard" {
    try std.testing.expect(etagMatch("\"abc123\"", "*"));
}

test "etagMatch: mismatch" {
    try std.testing.expect(!etagMatch("\"abc123\"", "\"different\""));
}

test "etagMatch: without quotes" {
    try std.testing.expect(etagMatch("\"abc123\"", "abc123"));
}

test "isModifiedSince: object newer than condition" {
    try std.testing.expect(isModifiedSince("2026-02-23T12:00:00.000Z", "Sun, 22 Feb 2026 12:00:00 GMT"));
}

test "isModifiedSince: object older than condition" {
    try std.testing.expect(!isModifiedSince("2026-02-22T12:00:00.000Z", "Mon, 23 Feb 2026 12:00:00 GMT"));
}

test "parseIso8601ToEpoch: basic" {
    const result = parseIso8601ToEpoch("1970-01-01T00:00:00.000Z");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(i64, 0), result.?);
}

test "parseHttpDateToEpoch: basic" {
    const result = parseHttpDateToEpoch("Thu, 01 Jan 1970 00:00:00 GMT");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(i64, 0), result.?);
}

test "dateToEpoch: known date" {
    // 2026-02-23 00:00:00 UTC
    const result = dateToEpoch(2026, 2, 23, 0, 0, 0);
    try std.testing.expect(result != null);
    // Verify positive (after epoch)
    try std.testing.expect(result.? > 0);
}

test "buildCannedAclJson: private" {
    const alloc = std.testing.allocator;
    const result = try buildCannedAclJson(alloc, "private", "owner1", "user1");
    try std.testing.expect(result != null);
    defer alloc.free(result.?);
    try std.testing.expect(std.mem.indexOf(u8, result.?, "FULL_CONTROL") != null);
}

test "buildCannedAclJson: public-read" {
    const alloc = std.testing.allocator;
    const result = try buildCannedAclJson(alloc, "public-read", "owner1", "user1");
    try std.testing.expect(result != null);
    defer alloc.free(result.?);
    try std.testing.expect(std.mem.indexOf(u8, result.?, "AllUsers") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.?, "\"READ\"") != null);
}

test "buildCannedAclJson: unknown returns null" {
    const alloc = std.testing.allocator;
    const result = try buildCannedAclJson(alloc, "invalid-acl", "owner1", "user1");
    try std.testing.expect(result == null);
}
