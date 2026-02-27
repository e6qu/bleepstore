const std = @import("std");
const tk = @import("tokamak");
const server = @import("../server.zig");
const sendS3Error = server.sendS3Error;
const sendS3ErrorWithMessage = server.sendS3ErrorWithMessage;
const sendResponse = server.sendResponse;
const xml = @import("../xml.zig");
const store = @import("../metadata/store.zig");
const validation = @import("../validation.zig");
const metrics_mod = @import("../metrics.zig");

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

/// Build a canned ACL JSON string based on the canned ACL name.
fn buildCannedAclJson(alloc: std.mem.Allocator, canned_acl: []const u8, owner_id: []const u8, owner_display: []const u8) !?[]u8 {
    if (std.mem.eql(u8, canned_acl, "private")) {
        const result = try buildDefaultAclJson(alloc, owner_id, owner_display);
        return result;
    } else if (std.mem.eql(u8, canned_acl, "public-read")) {
        const result = try std.fmt.allocPrint(alloc,
            \\{{"owner":{{"id":"{s}","display_name":"{s}"}},"grants":[{{"grantee":{{"type":"CanonicalUser","id":"{s}","display_name":"{s}"}},"permission":"FULL_CONTROL"}},{{"grantee":{{"type":"Group","uri":"http://acs.amazonaws.com/groups/global/AllUsers"}},"permission":"READ"}}]}}
        , .{ owner_id, owner_display, owner_id, owner_display });
        return result;
    } else if (std.mem.eql(u8, canned_acl, "public-read-write")) {
        const result = try std.fmt.allocPrint(alloc,
            \\{{"owner":{{"id":"{s}","display_name":"{s}"}},"grants":[{{"grantee":{{"type":"CanonicalUser","id":"{s}","display_name":"{s}"}},"permission":"FULL_CONTROL"}},{{"grantee":{{"type":"Group","uri":"http://acs.amazonaws.com/groups/global/AllUsers"}},"permission":"READ"}},{{"grantee":{{"type":"Group","uri":"http://acs.amazonaws.com/groups/global/AllUsers"}},"permission":"WRITE"}}]}}
        , .{ owner_id, owner_display, owner_id, owner_display });
        return result;
    } else if (std.mem.eql(u8, canned_acl, "authenticated-read")) {
        const result = try std.fmt.allocPrint(alloc,
            \\{{"owner":{{"id":"{s}","display_name":"{s}"}},"grants":[{{"grantee":{{"type":"CanonicalUser","id":"{s}","display_name":"{s}"}},"permission":"FULL_CONTROL"}},{{"grantee":{{"type":"Group","uri":"http://acs.amazonaws.com/groups/global/AuthenticatedUsers"}},"permission":"READ"}}]}}
        , .{ owner_id, owner_display, owner_id, owner_display });
        return result;
    }
    return null; // unknown canned ACL
}

/// Extract the LocationConstraint region from a CreateBucketConfiguration XML body.
/// Returns the region string if found, null if the body is empty or has no constraint.
fn parseLocationConstraint(body: []const u8) ?[]const u8 {
    if (body.len == 0) return null;

    // Find <LocationConstraint> and </LocationConstraint>
    const open_tag = "<LocationConstraint>";
    const close_tag = "</LocationConstraint>";

    const start = std.mem.indexOf(u8, body, open_tag) orelse return null;
    const content_start = start + open_tag.len;
    const end = std.mem.indexOf(u8, body[content_start..], close_tag) orelse return null;

    const region = body[content_start .. content_start + end];
    if (region.len == 0) return null;

    return region;
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

/// GET / -- List all buckets owned by the authenticated sender.
pub fn listBuckets(
    res: *tk.Response,
    req_alloc: std.mem.Allocator,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    const access_key = server.global_access_key;
    const owner_id = try deriveOwnerId(req_alloc, access_key);

    const buckets = try ms.listBuckets();

    // Build parallel arrays of names and creation dates for the XML renderer.
    var names: std.ArrayList([]const u8) = .empty;
    var dates: std.ArrayList([]const u8) = .empty;

    for (buckets) |bucket| {
        try names.append(req_alloc, bucket.name);
        try dates.append(req_alloc, bucket.creation_date);
    }

    const names_slice = try names.toOwnedSlice(req_alloc);
    const dates_slice = try dates.toOwnedSlice(req_alloc);

    const body = try xml.renderListBucketsResult(
        req_alloc,
        owner_id,
        access_key,
        names_slice,
        dates_slice,
    );

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.content_type = .XML;
    res.body = body;
}

/// PUT /<bucket> -- Create a new bucket.
pub fn createBucket(
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

    // Validate bucket name.
    if (validation.isValidBucketName(bucket_name)) |err| {
        return sendS3Error(res, req_alloc, err, bucket_name, request_id);
    }

    // Determine the region (default from config, or from request body).
    var region = server.global_region;

    // Read request body for optional CreateBucketConfiguration XML.
    const body = req.body();
    if (body) |b| {
        if (b.len > 0) {
            if (parseLocationConstraint(b)) |loc| {
                region = loc;
            }
        }
    }

    // Derive owner from access key.
    const access_key = server.global_access_key;
    const owner_id = try deriveOwnerId(req_alloc, access_key);
    const owner_display = access_key;

    // Check if bucket already exists (idempotent create for owned bucket).
    const existing = try ms.getBucket(bucket_name);
    if (existing != null) {
        // Bucket already exists and is owned by caller (single-user system).
        // us-east-1 behavior: return 200.
        const loc_header = try std.fmt.allocPrint(req_alloc, "/{s}", .{bucket_name});
        server.setCommonHeaders(res, request_id);
        res.status = 200;
        res.header("Location", loc_header);
        res.body = "";
        return;
    }

    // --- ACL processing ---
    // 1D: Mutual exclusion -- x-amz-acl and x-amz-grant-* cannot coexist.
    const canned_acl = req.header("x-amz-acl");
    const has_grants = hasAnyGrantHeader(req);
    if (canned_acl != null and has_grants) {
        return sendS3ErrorWithMessage(res, req_alloc, .InvalidArgument, "Specifying both x-amz-acl and x-amz-grant headers is not allowed", bucket_name, request_id);
    }

    // Build ACL from canned header, grant headers, or default.
    var acl_json = try buildDefaultAclJson(req_alloc, owner_id, owner_display);
    if (canned_acl) |ca| {
        if (try buildCannedAclJson(req_alloc, ca, owner_id, owner_display)) |canned| {
            acl_json = canned;
        }
    } else if (has_grants) {
        if (try parseGrantHeaders(req_alloc, req, owner_id, owner_display)) |grant_acl| {
            acl_json = grant_acl;
        }
    }

    // Format creation date.
    const creation_date = try formatIso8601(req_alloc);

    // Create the bucket in the metadata store.
    try ms.createBucket(.{
        .name = bucket_name,
        .creation_date = creation_date,
        .region = region,
        .owner_id = owner_id,
        .owner_display = owner_display,
        .acl = acl_json,
    });

    // Create bucket directory in storage backend.
    try sb.createBucket(bucket_name);

    // Update metrics.
    _ = metrics_mod.buckets_total.fetchAdd(1, .monotonic);

    const loc_header = try std.fmt.allocPrint(req_alloc, "/{s}", .{bucket_name});
    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.header("Location", loc_header);
    res.body = "";
}

/// DELETE /<bucket> -- Delete an existing bucket.
pub fn deleteBucket(
    res: *tk.Response,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    // Check if bucket exists.
    const exists = try ms.bucketExists(bucket_name);
    if (!exists) {
        return sendS3Error(res, req_alloc, .NoSuchBucket, bucket_name, request_id);
    }

    // Check if bucket has any objects (BucketNotEmpty check).
    // We use listObjectsMeta with max_keys=1 to check if any objects exist.
    const list_result = try ms.listObjectsMeta(bucket_name, "", "", "", 1);
    if (list_result.objects.len > 0) {
        return sendS3Error(res, req_alloc, .BucketNotEmpty, bucket_name, request_id);
    }

    // Delete the bucket from metadata store.
    try ms.deleteBucket(bucket_name);

    // Delete bucket directory from storage backend (idempotent).
    const sb_opt = server.global_storage_backend;
    if (sb_opt) |sb_val| {
        sb_val.deleteBucket(bucket_name) catch {};
    }

    // Update metrics.
    const current = metrics_mod.buckets_total.load(.monotonic);
    if (current > 0) {
        _ = metrics_mod.buckets_total.fetchSub(1, .monotonic);
    }

    server.setCommonHeaders(res, request_id);
    res.status = 204;
    res.body = "";
}

/// HEAD /<bucket> -- Check if a bucket exists.
pub fn headBucket(
    res: *tk.Response,
    _: std.mem.Allocator,
    bucket_name: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse {
        // HEAD responses have no body per S3 spec.
        sendResponse(res, "", 500, "application/xml", request_id);
        return;
    };

    const bucket_meta = try ms.getBucket(bucket_name);
    if (bucket_meta) |meta| {
        server.setCommonHeaders(res, request_id);
        res.status = 200;
        res.header("x-amz-bucket-region", meta.region);
        res.header("Content-Type", "application/xml");
        res.body = "";
    } else {
        // HEAD 404 -- no body, just status.
        // HEAD responses must not include a body per HTTP spec.
        server.setCommonHeaders(res, request_id);
        res.status = 404;
        res.body = "";
    }
}

/// GET /<bucket>?location -- Get the region of a bucket.
pub fn getBucketLocation(
    res: *tk.Response,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    const bucket_meta = try ms.getBucket(bucket_name);
    if (bucket_meta == null) {
        return sendS3Error(res, req_alloc, .NoSuchBucket, bucket_name, request_id);
    }

    const region = bucket_meta.?.region;
    const body = try xml.renderLocationConstraint(req_alloc, region);

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.content_type = .XML;
    res.body = body;
}

/// GET /<bucket>?acl -- Get the ACL of a bucket.
pub fn getBucketAcl(
    res: *tk.Response,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    const bucket_meta = try ms.getBucket(bucket_name);
    if (bucket_meta == null) {
        return sendS3Error(res, req_alloc, .NoSuchBucket, bucket_name, request_id);
    }

    const meta = bucket_meta.?;

    // If ACL has no owner info, fill it in from bucket metadata.
    const owner_id = meta.owner_id;
    const owner_display = meta.owner_display;
    const acl_json = meta.acl;

    const body = try xml.renderAccessControlPolicy(
        req_alloc,
        owner_id,
        owner_display,
        acl_json,
    );

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.content_type = .XML;
    res.body = body;
}

/// PUT /<bucket>?acl -- Set the ACL of a bucket.
pub fn putBucketAcl(
    res: *tk.Response,
    req: *tk.Request,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    request_id: *const [16]u8,
) !void {
    const ms = server.global_metadata_store orelse
        return sendS3Error(res, req_alloc, .InternalError, "/", request_id);

    const bucket_meta = try ms.getBucket(bucket_name);
    if (bucket_meta == null) {
        return sendS3Error(res, req_alloc, .NoSuchBucket, bucket_name, request_id);
    }

    const meta = bucket_meta.?;
    const owner_id = meta.owner_id;
    const owner_display = meta.owner_display;

    // 1D: Mutual exclusion -- x-amz-acl and x-amz-grant-* cannot coexist.
    const canned_acl = req.header("x-amz-acl");
    const has_grants = hasAnyGrantHeader(req);
    if (canned_acl != null and has_grants) {
        return sendS3ErrorWithMessage(res, req_alloc, .InvalidArgument, "Specifying both x-amz-acl and x-amz-grant headers is not allowed", bucket_name, request_id);
    }

    // Check for canned ACL header.
    if (canned_acl) |ca| {
        if (try buildCannedAclJson(req_alloc, ca, owner_id, owner_display)) |acl_json| {
            try ms.updateBucketAcl(bucket_name, acl_json);
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
            try ms.updateBucketAcl(bucket_name, grant_acl);
            server.setCommonHeaders(res, request_id);
            res.status = 200;
            res.body = "";
            return;
        }
    }

    // Check for XML body (AccessControlPolicy).
    const body = req.body();
    if (body) |b| {
        if (b.len > 0) {
            const acl_json = xml.parseAccessControlPolicyXml(req_alloc, b, owner_id, owner_display) catch {
                return sendS3Error(res, req_alloc, .MalformedACLError, bucket_name, request_id);
            };
            try ms.updateBucketAcl(bucket_name, acl_json);
            server.setCommonHeaders(res, request_id);
            res.status = 200;
            res.body = "";
            return;
        }
    }

    // No ACL specified -- default to private.
    const acl_json = try buildDefaultAclJson(req_alloc, owner_id, owner_display);
    try ms.updateBucketAcl(bucket_name, acl_json);

    server.setCommonHeaders(res, request_id);
    res.status = 200;
    res.body = "";
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "parseLocationConstraint: valid" {
    const body = "<CreateBucketConfiguration><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>";
    const result = parseLocationConstraint(body);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("eu-west-1", result.?);
}

test "parseLocationConstraint: with namespace" {
    const body =
        \\<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>ap-southeast-1</LocationConstraint></CreateBucketConfiguration>
    ;
    const result = parseLocationConstraint(body);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("ap-southeast-1", result.?);
}

test "parseLocationConstraint: empty body" {
    try std.testing.expect(parseLocationConstraint("") == null);
}

test "parseLocationConstraint: no LocationConstraint" {
    const body = "<CreateBucketConfiguration></CreateBucketConfiguration>";
    try std.testing.expect(parseLocationConstraint(body) == null);
}

test "parseLocationConstraint: empty LocationConstraint" {
    const body = "<CreateBucketConfiguration><LocationConstraint></LocationConstraint></CreateBucketConfiguration>";
    try std.testing.expect(parseLocationConstraint(body) == null);
}

test "deriveOwnerId returns 32 hex chars" {
    const owner_id = try deriveOwnerId(std.testing.allocator, "bleepstore");
    defer std.testing.allocator.free(owner_id);
    try std.testing.expectEqual(@as(usize, 32), owner_id.len);
    for (owner_id) |ch| {
        try std.testing.expect((ch >= '0' and ch <= '9') or (ch >= 'a' and ch <= 'f'));
    }
}

test "buildDefaultAclJson produces valid JSON" {
    const acl = try buildDefaultAclJson(std.testing.allocator, "owner123", "testuser");
    defer std.testing.allocator.free(acl);

    try std.testing.expect(std.mem.indexOf(u8, acl, "FULL_CONTROL") != null);
    try std.testing.expect(std.mem.indexOf(u8, acl, "owner123") != null);
    try std.testing.expect(std.mem.indexOf(u8, acl, "CanonicalUser") != null);
}

test "buildCannedAclJson: private" {
    const result = try buildCannedAclJson(std.testing.allocator, "private", "owner1", "user1");
    try std.testing.expect(result != null);
    defer std.testing.allocator.free(result.?);
    try std.testing.expect(std.mem.indexOf(u8, result.?, "FULL_CONTROL") != null);
}

test "buildCannedAclJson: public-read" {
    const result = try buildCannedAclJson(std.testing.allocator, "public-read", "owner1", "user1");
    try std.testing.expect(result != null);
    defer std.testing.allocator.free(result.?);
    try std.testing.expect(std.mem.indexOf(u8, result.?, "AllUsers") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.?, "\"READ\"") != null);
}

test "buildCannedAclJson: unknown returns null" {
    const result = try buildCannedAclJson(std.testing.allocator, "invalid-acl", "owner1", "user1");
    try std.testing.expect(result == null);
}
