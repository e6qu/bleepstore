const std = @import("std");
const tk = @import("tokamak");
const server = @import("../server.zig");
const sendS3Error = server.sendS3Error;
const sendResponse = server.sendResponse;
const store = @import("../metadata/store.zig");
const xml_mod = @import("../xml.zig");
const metrics_mod = @import("../metrics.zig");

/// Derive a canonical owner ID from an access key.
/// Uses SHA-256 hash of the access key, truncated to 32 hex characters.
fn deriveOwnerId(alloc: std.mem.Allocator, access_key: []const u8) ![]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(access_key, &hash, .{});
    const hex = std.fmt.bytesToHex(hash, .lower);
    return try alloc.dupe(u8, hex[0..32]);
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

/// Generate a UUID v4 string.
/// Fills 16 random bytes, sets version (4) and variant (10) bits,
/// then formats as xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx.
fn generateUuidV4(buf: *[36]u8) void {
    var random_bytes: [16]u8 = undefined;
    std.crypto.random.bytes(&random_bytes);

    // Set version: byte 6 top nibble = 0100 (version 4)
    random_bytes[6] = (random_bytes[6] & 0x0F) | 0x40;
    // Set variant: byte 8 top two bits = 10 (RFC 4122 variant)
    random_bytes[8] = (random_bytes[8] & 0x3F) | 0x80;

    // Format as UUID string.
    const hex_lower = "0123456789abcdef";
    var pos: usize = 0;
    for (random_bytes, 0..) |byte, i| {
        // Insert hyphens at positions: after bytes 3, 5, 7, 9
        if (i == 4 or i == 6 or i == 8 or i == 10) {
            buf[pos] = '-';
            pos += 1;
        }
        buf[pos] = hex_lower[byte >> 4];
        buf[pos + 1] = hex_lower[byte & 0x0F];
        pos += 2;
    }
}

/// POST /<bucket>/<key>?uploads -- Initiate a multipart upload.
pub fn createMultipartUpload(
    res: *tk.Response,
    req: *tk.Request,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    object_key: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Check that the bucket exists.
    const bucket_exists = try ms.bucketExists(bucket_name);
    if (!bucket_exists) {
        return sendS3Error(res, req_alloc, .NoSuchBucket, bucket_name, request_id);
    }

    // Generate UUID v4 upload ID.
    var uuid_buf: [36]u8 = undefined;
    generateUuidV4(&uuid_buf);
    const upload_id = try req_alloc.dupe(u8, &uuid_buf);

    // Format timestamp.
    const now = try formatIso8601(req_alloc);

    // Get content type from request header (for the eventual completed object).
    const content_type = req.header("content-type") orelse "application/octet-stream";

    // Derive owner.
    const access_key = server.global_access_key;
    const owner_id = try deriveOwnerId(req_alloc, access_key);

    // Create multipart upload metadata record.
    try ms.createMultipartUpload(.{
        .upload_id = upload_id,
        .bucket = bucket_name,
        .key = object_key,
        .initiated = now,
        .content_type = content_type,
        .owner_id = owner_id,
        .owner_display = access_key,
    });

    // Render InitiateMultipartUploadResult XML.
    const xml_body = try xml_mod.renderInitiateMultipartUploadResult(
        req_alloc,
        bucket_name,
        object_key,
        upload_id,
    );

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.content_type = .XML;
    res.body = xml_body;
}

/// PUT /<bucket>/<key>?partNumber=N&uploadId=X -- Upload a part.
pub fn uploadPart(
    res: *tk.Response,
    req: *tk.Request,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    object_key: []const u8,
    query: []const u8,
    request_id: *const [16]u8,
) !void {
    _ = object_key;

    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);
    const sb = server.global_storage_backend orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Extract uploadId from query.
    const upload_id = server.getQueryParamValue(query, "uploadId") orelse
        return sendS3Error(res, req_alloc, .InvalidArgument, "/", request_id);

    // Extract and validate partNumber from query.
    const part_number_str = server.getQueryParamValue(query, "partNumber") orelse
        return sendS3Error(res, req_alloc, .InvalidArgument, "/", request_id);
    const part_number = std.fmt.parseInt(u32, part_number_str, 10) catch
        return sendS3Error(res, req_alloc, .InvalidArgument, "/", request_id);
    if (part_number < 1 or part_number > 10000) {
        return sendS3Error(res, req_alloc, .InvalidArgument, "/", request_id);
    }

    // Verify the upload exists.
    const upload_meta = try ms.getMultipartUpload(upload_id);
    if (upload_meta == null) {
        return sendS3Error(res, req_alloc, .NoSuchUpload, "/", request_id);
    }
    // Free the upload meta fields (allocated by GPA in the metadata store).
    const um = upload_meta.?;
    if (server.global_allocator) |gpa| {
        gpa.free(um.upload_id);
        gpa.free(um.bucket);
        gpa.free(um.key);
        gpa.free(um.content_type);
        if (um.content_encoding) |v| gpa.free(v);
        if (um.content_language) |v| gpa.free(v);
        if (um.content_disposition) |v| gpa.free(v);
        if (um.cache_control) |v| gpa.free(v);
        if (um.expires) |v| gpa.free(v);
        gpa.free(um.storage_class);
        gpa.free(um.acl);
        gpa.free(um.user_metadata);
        gpa.free(um.owner_id);
        gpa.free(um.owner_display);
        gpa.free(um.initiated);
    }

    // Read the request body.
    const body = req.body() orelse "";

    // Check max object size.
    if (body.len > server.global_max_object_size) {
        return sendS3Error(res, req_alloc, .EntityTooLarge, "/", request_id);
    }

    // Write part to storage (atomic: temp + fsync + rename).
    const put_result = try sb.putPart(bucket_name, upload_id, part_number, body);
    // Copy etag to arena since it may be allocated by LocalBackend's allocator.
    const etag = try req_alloc.dupe(u8, put_result.etag);
    // Free the storage-allocated etag.
    if (server.global_allocator) |gpa| {
        gpa.free(put_result.etag);
    }

    // Format timestamp.
    const now = try formatIso8601(req_alloc);

    // Upsert part metadata (same part number overwrites previous).
    try ms.putPartMeta(upload_id, .{
        .part_number = part_number,
        .size = @intCast(body.len),
        .etag = etag,
        .last_modified = now,
    });

    // Return 200 with ETag header.
    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.header("ETag", etag);
    res.body = "";
}

/// Minimum part size: 5 MiB (all parts except the last).
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// A parsed part from CompleteMultipartUpload XML body.
const RequestPart = struct {
    part_number: u32,
    etag: []const u8,
};

/// Parse CompleteMultipartUpload XML body.
/// Extracts <Part><PartNumber>N</PartNumber><ETag>"..."</ETag></Part> elements.
fn parseCompleteMultipartUploadXml(alloc: std.mem.Allocator, body: []const u8) ![]RequestPart {
    var parts = std.ArrayList(RequestPart).empty;
    errdefer parts.deinit(alloc);

    var search_start: usize = 0;
    while (search_start < body.len) {
        // Find next <Part> element.
        const part_start = std.mem.indexOf(u8, body[search_start..], "<Part>") orelse break;
        const part_abs_start = search_start + part_start;
        const part_end = std.mem.indexOf(u8, body[part_abs_start..], "</Part>") orelse break;
        const part_abs_end = part_abs_start + part_end + "</Part>".len;
        const part_xml = body[part_abs_start..part_abs_end];

        // Extract PartNumber.
        const pn_start = std.mem.indexOf(u8, part_xml, "<PartNumber>") orelse {
            search_start = part_abs_end;
            continue;
        };
        const pn_content_start = pn_start + "<PartNumber>".len;
        const pn_end = std.mem.indexOf(u8, part_xml[pn_content_start..], "</PartNumber>") orelse {
            search_start = part_abs_end;
            continue;
        };
        const pn_str = part_xml[pn_content_start .. pn_content_start + pn_end];
        const part_number = std.fmt.parseInt(u32, pn_str, 10) catch {
            search_start = part_abs_end;
            continue;
        };

        // Extract ETag.
        const etag_start = std.mem.indexOf(u8, part_xml, "<ETag>") orelse {
            search_start = part_abs_end;
            continue;
        };
        const etag_content_start = etag_start + "<ETag>".len;
        const etag_end = std.mem.indexOf(u8, part_xml[etag_content_start..], "</ETag>") orelse {
            search_start = part_abs_end;
            continue;
        };
        const etag = part_xml[etag_content_start .. etag_content_start + etag_end];

        try parts.append(alloc, .{
            .part_number = part_number,
            .etag = etag,
        });

        search_start = part_abs_end;
    }

    return parts.toOwnedSlice(alloc);
}

/// Compute composite ETag from part ETags.
/// For each part, parse hex ETag to 16 bytes binary MD5, concatenate all,
/// compute MD5 of concatenation, format as "hex-N" where N = part count.
fn computeCompositeEtag(alloc: std.mem.Allocator, part_etags: []const []const u8) ![]u8 {
    var md5_concat = std.ArrayList(u8).empty;
    defer md5_concat.deinit(alloc);

    for (part_etags) |etag| {
        // Strip quotes from ETag: "hex" -> hex
        var etag_hex = etag;
        if (etag_hex.len >= 2 and etag_hex[0] == '"' and etag_hex[etag_hex.len - 1] == '"') {
            etag_hex = etag_hex[1 .. etag_hex.len - 1];
        }

        // Parse 32 hex chars to 16 bytes.
        if (etag_hex.len != 32) continue;
        var md5_bytes: [16]u8 = undefined;
        for (0..16) |i| {
            md5_bytes[i] = std.fmt.parseInt(u8, etag_hex[i * 2 .. i * 2 + 2], 16) catch 0;
        }
        try md5_concat.appendSlice(alloc, &md5_bytes);
    }

    // Compute MD5 of the concatenated binary MD5s.
    var composite_hash: [std.crypto.hash.Md5.digest_length]u8 = undefined;
    std.crypto.hash.Md5.hash(md5_concat.items, &composite_hash, .{});

    // Format as "hex-N" where N is the number of parts.
    const composite_hex = std.fmt.bytesToHex(composite_hash, .lower);
    return try std.fmt.allocPrint(alloc, "\"{s}-{d}\"", .{ @as([]const u8, &composite_hex), part_etags.len });
}

/// Strip surrounding double quotes from a string.
fn stripQuotes(s: []const u8) []const u8 {
    if (s.len >= 2 and s[0] == '"' and s[s.len - 1] == '"') {
        return s[1 .. s.len - 1];
    }
    return s;
}

/// Check if an object's ETag matches a conditional ETag header value.
/// Handles comma-separated list of ETags and wildcard "*".
fn etagMatchConditional(object_etag: []const u8, condition: []const u8) bool {
    if (std.mem.eql(u8, condition, "*")) return true;

    const obj_stripped = stripQuotes(object_etag);

    var iter = std.mem.splitScalar(u8, condition, ',');
    while (iter.next()) |etag_part| {
        const trimmed = std.mem.trim(u8, etag_part, " ");
        const cond_stripped = stripQuotes(trimmed);
        if (std.mem.eql(u8, obj_stripped, cond_stripped)) return true;
    }
    return false;
}

/// Check if the object has been modified since a given HTTP date string.
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
fn parseHttpDateToEpoch(date_str: []const u8) ?i64 {
    const trimmed = std.mem.trim(u8, date_str, " ");

    const comma_idx = std.mem.indexOfScalar(u8, trimmed, ',') orelse return null;
    if (comma_idx + 2 >= trimmed.len) return null;
    const rest = std.mem.trimLeft(u8, trimmed[comma_idx + 1 ..], " ");

    var parts_iter = std.mem.splitScalar(u8, rest, ' ');
    const day_str = parts_iter.next() orelse return null;
    const month_str = parts_iter.next() orelse return null;
    const year_str = parts_iter.next() orelse return null;
    const time_str = parts_iter.next() orelse return null;

    const day = std.fmt.parseInt(u8, day_str, 10) catch return null;
    const month = monthNameToNumber(month_str) orelse return null;
    const year = std.fmt.parseInt(u16, year_str, 10) catch return null;

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
    var y: u16 = 1970;
    while (y < year) : (y += 1) {
        total_days += if (std.time.epoch.isLeapYear(@intCast(y))) @as(i64, 366) else 365;
    }
    var m: u8 = 1;
    while (m < month) : (m += 1) {
        var md: i64 = month_days_table[m - 1];
        if (m == 2 and is_leap) md += 1;
        total_days += md;
    }
    total_days += @as(i64, day) - 1;

    return total_days * 86400 + @as(i64, hours) * 3600 + @as(i64, minutes) * 60 + @as(i64, seconds);
}

/// Free an ObjectMeta struct's GPA-allocated fields.
fn freeObjectMeta(meta: anytype) void {
    if (server.global_allocator) |gpa| {
        gpa.free(meta.bucket);
        gpa.free(meta.key);
        gpa.free(meta.etag);
        gpa.free(meta.content_type);
        gpa.free(meta.last_modified);
        gpa.free(meta.storage_class);
        gpa.free(meta.acl);
        if (meta.user_metadata) |v| gpa.free(v);
        if (meta.version_id) |v| gpa.free(v);
        if (meta.content_encoding) |v| gpa.free(v);
        if (meta.content_language) |v| gpa.free(v);
        if (meta.content_disposition) |v| gpa.free(v);
        if (meta.cache_control) |v| gpa.free(v);
        if (meta.expires) |v| gpa.free(v);
    }
}

/// POST /<bucket>/<key>?uploadId=X -- Complete a multipart upload.
pub fn completeMultipartUpload(
    res: *tk.Response,
    req: *tk.Request,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    object_key: []const u8,
    query: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);
    const sb = server.global_storage_backend orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Extract uploadId from query.
    const upload_id = server.getQueryParamValue(query, "uploadId") orelse
        return sendS3Error(res, req_alloc, .InvalidArgument, "/", request_id);

    // Verify the upload exists and get its metadata.
    const upload_meta = try ms.getMultipartUpload(upload_id);
    if (upload_meta == null) {
        return sendS3Error(res, req_alloc, .NoSuchUpload, "/", request_id);
    }
    // Copy fields we need to the request arena before freeing GPA originals.
    const um = upload_meta.?;
    const content_type = try req_alloc.dupe(u8, um.content_type);
    const acl = try req_alloc.dupe(u8, um.acl);
    const user_metadata = try req_alloc.dupe(u8, um.user_metadata);
    const storage_class = try req_alloc.dupe(u8, um.storage_class);
    // Copy optional fields.
    const content_encoding: ?[]const u8 = if (um.content_encoding) |v| try req_alloc.dupe(u8, v) else null;
    const content_language: ?[]const u8 = if (um.content_language) |v| try req_alloc.dupe(u8, v) else null;
    const content_disposition: ?[]const u8 = if (um.content_disposition) |v| try req_alloc.dupe(u8, v) else null;
    const cache_control: ?[]const u8 = if (um.cache_control) |v| try req_alloc.dupe(u8, v) else null;
    const expires: ?[]const u8 = if (um.expires) |v| try req_alloc.dupe(u8, v) else null;

    // Free GPA-allocated upload metadata.
    if (server.global_allocator) |gpa| {
        gpa.free(um.upload_id);
        gpa.free(um.bucket);
        gpa.free(um.key);
        gpa.free(um.content_type);
        if (um.content_encoding) |v| gpa.free(v);
        if (um.content_language) |v| gpa.free(v);
        if (um.content_disposition) |v| gpa.free(v);
        if (um.cache_control) |v| gpa.free(v);
        if (um.expires) |v| gpa.free(v);
        gpa.free(um.storage_class);
        gpa.free(um.acl);
        gpa.free(um.user_metadata);
        gpa.free(um.owner_id);
        gpa.free(um.owner_display);
        gpa.free(um.initiated);
    }

    // Read the request body (CompleteMultipartUpload XML).
    const body = req.body() orelse "";
    if (body.len == 0) {
        return sendS3Error(res, req_alloc, .MalformedXML, "/", request_id);
    }

    // Parse the XML body to extract Part elements.
    const request_parts = parseCompleteMultipartUploadXml(req_alloc, body) catch {
        return sendS3Error(res, req_alloc, .MalformedXML, "/", request_id);
    };

    if (request_parts.len == 0) {
        return sendS3Error(res, req_alloc, .MalformedXML, "/", request_id);
    }

    // Validate part order: parts must be in ascending part number order.
    for (1..request_parts.len) |i| {
        if (request_parts[i].part_number <= request_parts[i - 1].part_number) {
            return sendS3Error(res, req_alloc, .InvalidPartOrder, "/", request_id);
        }
    }

    // Get all uploaded parts from metadata store.
    const stored_parts = try ms.getPartsForCompletion(upload_id);
    defer {
        if (server.global_allocator) |gpa| {
            for (stored_parts) |p| {
                gpa.free(p.etag);
                gpa.free(p.last_modified);
            }
            gpa.free(stored_parts);
        }
    }

    // Build a map of stored parts by part number for validation.
    // Use a simple linear scan since part counts are bounded by 10000.
    // Validate each request part against stored parts.
    var validated_etags = std.ArrayList([]const u8).empty;
    defer validated_etags.deinit(req_alloc);

    for (request_parts, 0..) |rp, idx| {
        // Find the stored part with matching part number.
        var found = false;
        for (stored_parts) |sp| {
            if (sp.part_number == rp.part_number) {
                // Validate ETag match (compare without quotes).
                const req_etag_stripped = stripQuotes(rp.etag);
                const stored_etag_stripped = stripQuotes(sp.etag);
                if (!std.mem.eql(u8, req_etag_stripped, stored_etag_stripped)) {
                    return sendS3Error(res, req_alloc, .InvalidPart, "/", request_id);
                }

                // Part size validation: all non-last parts must be >= 5 MiB.
                if (idx < request_parts.len - 1 and sp.size < MIN_PART_SIZE) {
                    return sendS3Error(res, req_alloc, .EntityTooSmall, "/", request_id);
                }

                try validated_etags.append(req_alloc, sp.etag);
                found = true;
                break;
            }
        }
        if (!found) {
            return sendS3Error(res, req_alloc, .InvalidPart, "/", request_id);
        }
    }

    // Build PartInfo array for storage backend assembly.
    const backend_mod = @import("../storage/backend.zig");
    var part_infos = try req_alloc.alloc(backend_mod.PartInfo, request_parts.len);
    for (request_parts, 0..) |rp, i| {
        part_infos[i] = .{
            .part_number = rp.part_number,
            .etag = validated_etags.items[i],
        };
    }

    // Assemble parts into final object via storage backend.
    const assemble_result = sb.assembleParts(bucket_name, object_key, upload_id, part_infos) catch |err| {
        std.log.err("assembleParts failed: {}", .{err});
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);
    };
    // Copy etag to arena since it may be allocated by LocalBackend's allocator.
    const composite_etag = try req_alloc.dupe(u8, assemble_result.etag);
    const total_size = assemble_result.total_size;
    if (server.global_allocator) |gpa| {
        gpa.free(assemble_result.etag);
    }

    // Format timestamp.
    const now = try formatIso8601(req_alloc);

    // Create the final object metadata and delete upload + parts atomically.
    ms.completeMultipartUpload(upload_id, .{
        .bucket = bucket_name,
        .key = object_key,
        .size = total_size,
        .etag = composite_etag,
        .content_type = content_type,
        .content_encoding = content_encoding,
        .content_language = content_language,
        .content_disposition = content_disposition,
        .cache_control = cache_control,
        .expires = expires,
        .storage_class = storage_class,
        .acl = acl,
        .user_metadata = user_metadata,
        .last_modified = now,
    }) catch |err| {
        std.log.err("completeMultipartUpload metadata failed: {}", .{err});
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);
    };

    // Clean up part files from storage.
    sb.deleteParts(bucket_name, upload_id) catch {};

    // Update metrics.
    _ = metrics_mod.objects_total.fetchAdd(1, .monotonic);

    // Build Location URL.
    const location = try std.fmt.allocPrint(req_alloc, "http://{s}/{s}/{s}", .{
        server.global_region,
        bucket_name,
        object_key,
    });

    // Render CompleteMultipartUploadResult XML.
    const xml_body = try xml_mod.renderCompleteMultipartUploadResult(
        req_alloc,
        location,
        bucket_name,
        object_key,
        composite_etag,
    );

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.content_type = .XML;
    res.body = xml_body;
}

/// DELETE /<bucket>/<key>?uploadId=X -- Abort a multipart upload.
pub fn abortMultipartUpload(
    res: *tk.Response,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    object_key: []const u8,
    query: []const u8,
    request_id: *const [16]u8,
) !void {
    _ = object_key;

    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);
    const sb = server.global_storage_backend orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Extract uploadId from query.
    const upload_id = server.getQueryParamValue(query, "uploadId") orelse
        return sendS3Error(res, req_alloc, .InvalidArgument, "/", request_id);

    // Delete part files from storage (idempotent).
    try sb.deleteParts(bucket_name, upload_id);

    // Delete metadata records (upload + parts). Returns NoSuchUpload if not found.
    ms.abortMultipartUpload(upload_id) catch |err| {
        if (err == error.NoSuchUpload) {
            return sendS3Error(res, req_alloc, .NoSuchUpload, "/", request_id);
        }
        return err;
    };

    // Return 204 No Content.
    server.setCommonHeaders(res, request_id);
    res.status = 204;
    res.body = "";
}

/// GET /<bucket>?uploads -- List in-progress multipart uploads.
pub fn listMultipartUploads(
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
    const key_marker = server.getQueryParamDecoded(req_alloc, query, "key-marker") orelse "";
    const upload_id_marker = server.getQueryParamValue(query, "upload-id-marker") orelse "";
    const max_uploads_str = server.getQueryParamValue(query, "max-uploads") orelse "1000";
    const encoding_type = server.getQueryParamValue(query, "encoding-type") orelse "";
    var max_uploads: u32 = std.fmt.parseInt(u32, max_uploads_str, 10) catch 1000;
    if (max_uploads > 1000) max_uploads = 1000;

    // Query metadata store.
    const result = try ms.listMultipartUploads(bucket_name, prefix, max_uploads);

    // Free GPA-allocated metadata when done.
    defer {
        if (server.global_allocator) |gpa| {
            for (result.uploads) |u| {
                gpa.free(u.upload_id);
                gpa.free(u.bucket);
                gpa.free(u.key);
                gpa.free(u.content_type);
                if (u.content_encoding) |v| gpa.free(v);
                if (u.content_language) |v| gpa.free(v);
                if (u.content_disposition) |v| gpa.free(v);
                if (u.cache_control) |v| gpa.free(v);
                if (u.expires) |v| gpa.free(v);
                gpa.free(u.storage_class);
                gpa.free(u.acl);
                gpa.free(u.user_metadata);
                gpa.free(u.owner_id);
                gpa.free(u.owner_display);
                gpa.free(u.initiated);
            }
            gpa.free(result.uploads);
        }
    }

    // Build XML upload entries.
    var entries = std.ArrayList(xml_mod.MultipartUploadEntry).empty;
    defer entries.deinit(req_alloc);

    for (result.uploads) |u| {
        try entries.append(req_alloc, .{
            .key = u.key,
            .upload_id = u.upload_id,
            .owner_id = u.owner_id,
            .owner_display = u.owner_display,
            .storage_class = u.storage_class,
            .initiated = u.initiated,
        });
    }

    // Determine next markers for pagination.
    var next_key_marker: []const u8 = "";
    var next_upload_id_marker: []const u8 = "";
    if (result.is_truncated and result.uploads.len > 0) {
        const last = result.uploads[result.uploads.len - 1];
        next_key_marker = last.key;
        next_upload_id_marker = last.upload_id;
    }

    // Render XML.
    const xml_body = try xml_mod.renderListMultipartUploadsResult(
        req_alloc,
        bucket_name,
        key_marker,
        upload_id_marker,
        next_key_marker,
        next_upload_id_marker,
        max_uploads,
        result.is_truncated,
        entries.items,
        prefix,
        delimiter,
        &.{}, // common_prefixes (not implementing delimiter grouping for multipart uploads in Stage 7)
        encoding_type,
    );

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.content_type = .XML;
    res.body = xml_body;
}

/// PUT /<bucket>/<key>?partNumber=N&uploadId=X with x-amz-copy-source -- Upload Part Copy.
pub fn uploadPartCopy(
    res: *tk.Response,
    req: *tk.Request,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    object_key: []const u8,
    query: []const u8,
    request_id: *const [16]u8,
) !void {
    _ = object_key;

    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);
    const sb = server.global_storage_backend orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Extract uploadId from query.
    const upload_id = server.getQueryParamValue(query, "uploadId") orelse
        return sendS3Error(res, req_alloc, .InvalidArgument, "/", request_id);

    // Extract and validate partNumber from query.
    const part_number_str = server.getQueryParamValue(query, "partNumber") orelse
        return sendS3Error(res, req_alloc, .InvalidArgument, "/", request_id);
    const part_number = std.fmt.parseInt(u32, part_number_str, 10) catch
        return sendS3Error(res, req_alloc, .InvalidArgument, "/", request_id);
    if (part_number < 1 or part_number > 10000) {
        return sendS3Error(res, req_alloc, .InvalidArgument, "/", request_id);
    }

    // Verify the upload exists.
    const upload_meta = try ms.getMultipartUpload(upload_id);
    if (upload_meta == null) {
        return sendS3Error(res, req_alloc, .NoSuchUpload, "/", request_id);
    }
    // Free the upload meta fields (allocated by GPA in the metadata store).
    const um = upload_meta.?;
    if (server.global_allocator) |gpa| {
        gpa.free(um.upload_id);
        gpa.free(um.bucket);
        gpa.free(um.key);
        gpa.free(um.content_type);
        if (um.content_encoding) |v| gpa.free(v);
        if (um.content_language) |v| gpa.free(v);
        if (um.content_disposition) |v| gpa.free(v);
        if (um.cache_control) |v| gpa.free(v);
        if (um.expires) |v| gpa.free(v);
        gpa.free(um.storage_class);
        gpa.free(um.acl);
        gpa.free(um.user_metadata);
        gpa.free(um.owner_id);
        gpa.free(um.owner_display);
        gpa.free(um.initiated);
    }

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

    // --- x-amz-copy-source-if-* conditional header evaluation ---
    // These headers are analogous to GetObject conditional headers but apply to the copy source.

    // x-amz-copy-source-if-match: must match the ETag, otherwise 412.
    const copy_if_match = req.header("x-amz-copy-source-if-match");
    if (copy_if_match) |etag_cond| {
        if (!etagMatchConditional(src_meta.etag, etag_cond)) {
            // Free source metadata before returning.
            freeObjectMeta(src_meta);
            server.setCommonHeaders(res, request_id);
            res.status = 412;
            res.body = "";
            return;
        }
    }

    // x-amz-copy-source-if-none-match: if ETag matches, return 412.
    const copy_if_none_match = req.header("x-amz-copy-source-if-none-match");
    if (copy_if_none_match) |etag_cond| {
        if (etagMatchConditional(src_meta.etag, etag_cond)) {
            freeObjectMeta(src_meta);
            server.setCommonHeaders(res, request_id);
            res.status = 412;
            res.body = "";
            return;
        }
    }

    // x-amz-copy-source-if-unmodified-since: only if copy-if-match was NOT present.
    if (copy_if_match == null) {
        const copy_if_unmodified = req.header("x-amz-copy-source-if-unmodified-since");
        if (copy_if_unmodified) |date_str| {
            if (isModifiedSince(src_meta.last_modified, date_str)) {
                freeObjectMeta(src_meta);
                server.setCommonHeaders(res, request_id);
                res.status = 412;
                res.body = "";
                return;
            }
        }
    }

    // x-amz-copy-source-if-modified-since: only if copy-if-none-match was NOT present.
    if (copy_if_none_match == null) {
        const copy_if_modified = req.header("x-amz-copy-source-if-modified-since");
        if (copy_if_modified) |date_str| {
            if (!isModifiedSince(src_meta.last_modified, date_str)) {
                freeObjectMeta(src_meta);
                server.setCommonHeaders(res, request_id);
                res.status = 412;
                res.body = "";
                return;
            }
        }
    }

    // Free source object metadata (GPA-allocated strings).
    freeObjectMeta(src_meta);

    // Read the source object data from storage.
    const obj_data = sb.getObject(src_bucket, src_key) catch |err| {
        return switch (err) {
            error.NoSuchKey => sendS3Error(res, req_alloc, .NoSuchKey, src_key, request_id),
            else => sendS3Error(res, req_alloc, .InternalError, "/", request_id),
        };
    };
    const full_body = obj_data.body orelse "";

    // Handle optional x-amz-copy-source-range header: "bytes=start-end"
    const copy_range = req.header("x-amz-copy-source-range");
    const part_data = if (copy_range) |range_str| blk: {
        // Parse the range.
        if (std.mem.startsWith(u8, range_str, "bytes=")) {
            const range_spec = range_str["bytes=".len..];
            const dash_idx2 = std.mem.indexOfScalar(u8, range_spec, '-') orelse break :blk full_body;
            const start_str = range_spec[0..dash_idx2];
            const end_str = range_spec[dash_idx2 + 1 ..];

            const start = std.fmt.parseInt(usize, start_str, 10) catch break :blk full_body;
            const end = std.fmt.parseInt(usize, end_str, 10) catch break :blk full_body;

            if (start >= full_body.len) break :blk full_body;
            const actual_end = @min(end, full_body.len - 1);
            break :blk full_body[start .. actual_end + 1];
        } else {
            break :blk full_body;
        }
    } else full_body;

    // Write part to storage (atomic: temp + fsync + rename).
    const put_result = try sb.putPart(bucket_name, upload_id, part_number, part_data);

    // Free the source object body (GPA-allocated by LocalBackend).
    if (obj_data.body) |body_slice| {
        if (server.global_allocator) |gpa| {
            gpa.free(body_slice);
        }
    }

    // Copy etag to arena since it may be allocated by LocalBackend's allocator.
    const etag = try req_alloc.dupe(u8, put_result.etag);
    // Free the storage-allocated etag.
    if (server.global_allocator) |gpa| {
        gpa.free(put_result.etag);
    }

    // Format timestamp.
    const now = try formatIso8601(req_alloc);

    // Upsert part metadata.
    try ms.putPartMeta(upload_id, .{
        .part_number = part_number,
        .size = @intCast(part_data.len),
        .etag = etag,
        .last_modified = now,
    });

    // Render CopyPartResult XML.
    const xml_body = try xml_mod.renderCopyPartResult(req_alloc, etag, now);

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.content_type = .XML;
    res.body = xml_body;
}

/// URI-decode helper for copy source paths.
fn uriDecode(alloc: std.mem.Allocator, input: []const u8) ![]u8 {
    var result = std.ArrayList(u8).empty;
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

/// GET /<bucket>/<key>?uploadId=X -- List parts of a multipart upload.
pub fn listParts(
    res: *tk.Response,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    object_key: []const u8,
    query: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Extract uploadId from query.
    const upload_id = server.getQueryParamValue(query, "uploadId") orelse
        return sendS3Error(res, req_alloc, .InvalidArgument, "/", request_id);

    // Verify the upload exists.
    const upload_meta = try ms.getMultipartUpload(upload_id);
    if (upload_meta == null) {
        return sendS3Error(res, req_alloc, .NoSuchUpload, "/", request_id);
    }
    // Free upload meta GPA allocations.
    const um = upload_meta.?;
    var owner_id: []const u8 = "";
    var owner_display: []const u8 = "";
    var storage_class: []const u8 = "STANDARD";
    // Copy fields we need to the request arena before freeing.
    owner_id = try req_alloc.dupe(u8, um.owner_id);
    owner_display = try req_alloc.dupe(u8, um.owner_display);
    storage_class = try req_alloc.dupe(u8, um.storage_class);
    if (server.global_allocator) |gpa| {
        gpa.free(um.upload_id);
        gpa.free(um.bucket);
        gpa.free(um.key);
        gpa.free(um.content_type);
        if (um.content_encoding) |v| gpa.free(v);
        if (um.content_language) |v| gpa.free(v);
        if (um.content_disposition) |v| gpa.free(v);
        if (um.cache_control) |v| gpa.free(v);
        if (um.expires) |v| gpa.free(v);
        gpa.free(um.storage_class);
        gpa.free(um.acl);
        gpa.free(um.user_metadata);
        gpa.free(um.owner_id);
        gpa.free(um.owner_display);
        gpa.free(um.initiated);
    }

    // Parse query parameters.
    const max_parts_str = server.getQueryParamValue(query, "max-parts") orelse "1000";
    var max_parts: u32 = std.fmt.parseInt(u32, max_parts_str, 10) catch 1000;
    if (max_parts > 1000) max_parts = 1000;

    const part_marker_str = server.getQueryParamValue(query, "part-number-marker") orelse "0";
    const part_marker: u32 = std.fmt.parseInt(u32, part_marker_str, 10) catch 0;

    // Query parts from metadata store.
    const result = try ms.listPartsMeta(upload_id, max_parts, part_marker);

    // Free GPA-allocated part metadata when done.
    defer {
        if (server.global_allocator) |gpa| {
            for (result.parts) |p| {
                gpa.free(p.etag);
                gpa.free(p.last_modified);
            }
            gpa.free(result.parts);
        }
    }

    // Build XML part entries.
    var part_entries = std.ArrayList(xml_mod.PartEntry).empty;
    defer part_entries.deinit(req_alloc);

    for (result.parts) |p| {
        try part_entries.append(req_alloc, .{
            .part_number = p.part_number,
            .last_modified = p.last_modified,
            .etag = p.etag,
            .size = p.size,
        });
    }

    // Render XML.
    const xml_body = try xml_mod.renderListPartsResult(
        req_alloc,
        bucket_name,
        object_key,
        upload_id,
        owner_id,
        owner_display,
        storage_class,
        part_marker,
        result.next_part_number_marker,
        max_parts,
        result.is_truncated,
        part_entries.items,
    );

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.content_type = .XML;
    res.body = xml_body;
}

// =========================================================================
// Tests
// =========================================================================

test "parseCompleteMultipartUploadXml: basic" {
    const alloc = std.testing.allocator;
    const xml =
        \\<CompleteMultipartUpload>
        \\  <Part>
        \\    <PartNumber>1</PartNumber>
        \\    <ETag>"a54357faf0632cce46e942fa68356b38"</ETag>
        \\  </Part>
        \\  <Part>
        \\    <PartNumber>2</PartNumber>
        \\    <ETag>"0c78aef83f66abc1fa1e8477f296d394"</ETag>
        \\  </Part>
        \\</CompleteMultipartUpload>
    ;
    const parts = try parseCompleteMultipartUploadXml(alloc, xml);
    defer alloc.free(parts);

    try std.testing.expectEqual(@as(usize, 2), parts.len);
    try std.testing.expectEqual(@as(u32, 1), parts[0].part_number);
    try std.testing.expectEqualStrings("\"a54357faf0632cce46e942fa68356b38\"", parts[0].etag);
    try std.testing.expectEqual(@as(u32, 2), parts[1].part_number);
    try std.testing.expectEqualStrings("\"0c78aef83f66abc1fa1e8477f296d394\"", parts[1].etag);
}

test "parseCompleteMultipartUploadXml: empty body" {
    const alloc = std.testing.allocator;
    const parts = try parseCompleteMultipartUploadXml(alloc, "");
    defer alloc.free(parts);
    try std.testing.expectEqual(@as(usize, 0), parts.len);
}

test "parseCompleteMultipartUploadXml: single part" {
    const alloc = std.testing.allocator;
    const xml = "<CompleteMultipartUpload><Part><PartNumber>3</PartNumber><ETag>\"abc123def456abc123def456abc123de\"</ETag></Part></CompleteMultipartUpload>";
    const parts = try parseCompleteMultipartUploadXml(alloc, xml);
    defer alloc.free(parts);

    try std.testing.expectEqual(@as(usize, 1), parts.len);
    try std.testing.expectEqual(@as(u32, 3), parts[0].part_number);
}

test "computeCompositeEtag: known value" {
    const alloc = std.testing.allocator;
    // Two parts with known MD5 hex ETags.
    const etags = [_][]const u8{
        "\"a54357faf0632cce46e942fa68356b38\"",
        "\"0c78aef83f66abc1fa1e8477f296d394\"",
    };
    const result = try computeCompositeEtag(alloc, &etags);
    defer alloc.free(result);

    // Should be formatted as "hex-2"
    try std.testing.expect(result.len > 0);
    try std.testing.expect(result[0] == '"');
    try std.testing.expect(result[result.len - 1] == '"');
    // Should contain -2 suffix
    try std.testing.expect(std.mem.indexOf(u8, result, "-2\"") != null);
}

test "computeCompositeEtag: single part" {
    const alloc = std.testing.allocator;
    const etags = [_][]const u8{
        "\"d41d8cd98f00b204e9800998ecf8427e\"",
    };
    const result = try computeCompositeEtag(alloc, &etags);
    defer alloc.free(result);

    // Should end with -1"
    try std.testing.expect(std.mem.indexOf(u8, result, "-1\"") != null);
}

test "stripQuotes: with quotes" {
    try std.testing.expectEqualStrings("abc", stripQuotes("\"abc\""));
}

test "stripQuotes: without quotes" {
    try std.testing.expectEqualStrings("abc", stripQuotes("abc"));
}

test "generateUuidV4: format and version/variant bits" {
    var buf: [36]u8 = undefined;
    generateUuidV4(&buf);

    // Check length.
    try std.testing.expectEqual(@as(usize, 36), buf.len);

    // Check hyphens at positions 8, 13, 18, 23.
    try std.testing.expectEqual(@as(u8, '-'), buf[8]);
    try std.testing.expectEqual(@as(u8, '-'), buf[13]);
    try std.testing.expectEqual(@as(u8, '-'), buf[18]);
    try std.testing.expectEqual(@as(u8, '-'), buf[23]);

    // Check version: position 14 should be '4'.
    try std.testing.expectEqual(@as(u8, '4'), buf[14]);

    // Check variant: position 19 should be one of '8', '9', 'a', 'b'.
    const variant_char = buf[19];
    try std.testing.expect(variant_char == '8' or variant_char == '9' or variant_char == 'a' or variant_char == 'b');

    // All other chars should be hex digits.
    for (buf, 0..) |ch, i| {
        if (i == 8 or i == 13 or i == 18 or i == 23) continue;
        try std.testing.expect((ch >= '0' and ch <= '9') or (ch >= 'a' and ch <= 'f'));
    }
}

test "generateUuidV4: uniqueness" {
    var buf1: [36]u8 = undefined;
    var buf2: [36]u8 = undefined;
    generateUuidV4(&buf1);
    generateUuidV4(&buf2);

    // Two UUIDs should be different.
    try std.testing.expect(!std.mem.eql(u8, &buf1, &buf2));
}
