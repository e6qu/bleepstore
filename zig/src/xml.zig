const std = @import("std");

pub const XmlWriter = struct {
    buf: std.ArrayList(u8),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) XmlWriter {
        return .{
            .buf = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *XmlWriter) void {
        self.buf.deinit(self.allocator);
    }

    pub fn toOwnedSlice(self: *XmlWriter) ![]u8 {
        return self.buf.toOwnedSlice(self.allocator);
    }

    fn appendSlice(self: *XmlWriter, data: []const u8) !void {
        try self.buf.appendSlice(self.allocator, data);
    }

    fn appendByte(self: *XmlWriter, byte: u8) !void {
        try self.buf.append(self.allocator, byte);
    }

    pub fn xmlDeclaration(self: *XmlWriter) !void {
        try self.appendSlice("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    }

    pub fn openTag(self: *XmlWriter, tag: []const u8) !void {
        try self.appendByte('<');
        try self.appendSlice(tag);
        try self.appendByte('>');
    }

    pub fn openTagWithNs(self: *XmlWriter, tag: []const u8, ns: []const u8) !void {
        try self.appendByte('<');
        try self.appendSlice(tag);
        try self.appendSlice(" xmlns=\"");
        try self.appendSlice(ns);
        try self.appendSlice("\">");
    }

    pub fn closeTag(self: *XmlWriter, tag: []const u8) !void {
        try self.appendSlice("</");
        try self.appendSlice(tag);
        try self.appendByte('>');
    }

    pub fn element(self: *XmlWriter, tag: []const u8, content: []const u8) !void {
        try self.appendByte('<');
        try self.appendSlice(tag);
        try self.appendByte('>');
        try self.appendEscaped(content);
        try self.appendSlice("</");
        try self.appendSlice(tag);
        try self.appendByte('>');
    }

    /// Append XML-escaped content: replaces &, <, >, ", ' with XML entities.
    fn appendEscaped(self: *XmlWriter, content: []const u8) !void {
        for (content) |ch| {
            switch (ch) {
                '&' => try self.appendSlice("&amp;"),
                '<' => try self.appendSlice("&lt;"),
                '>' => try self.appendSlice("&gt;"),
                '"' => try self.appendSlice("&quot;"),
                '\'' => try self.appendSlice("&apos;"),
                else => try self.appendByte(ch),
            }
        }
    }

    pub fn emptyElement(self: *XmlWriter, tag: []const u8) !void {
        try self.appendByte('<');
        try self.appendSlice(tag);
        try self.appendSlice("/>");
    }

    /// Open a tag with an xmlns namespace attribute and an empty element if self-closing.
    pub fn emptyElementWithNs(self: *XmlWriter, tag: []const u8, ns: []const u8) !void {
        try self.appendByte('<');
        try self.appendSlice(tag);
        try self.appendSlice(" xmlns=\"");
        try self.appendSlice(ns);
        try self.appendSlice("\"/>");
    }

    /// Write raw XML content directly (for complex attributes like xsi:type).
    pub fn raw(self: *XmlWriter, content: []const u8) !void {
        try self.appendSlice(content);
    }
};

const S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/";

/// Render an S3 error XML response.
pub fn renderError(
    allocator: std.mem.Allocator,
    code: []const u8,
    message: []const u8,
    resource: []const u8,
    request_id: []const u8,
) ![]u8 {
    var x = XmlWriter.init(allocator);
    defer x.deinit();

    try x.xmlDeclaration();
    try x.openTag("Error");
    try x.element("Code", code);
    try x.element("Message", message);
    try x.element("Resource", resource);
    try x.element("RequestId", request_id);
    try x.closeTag("Error");

    return x.toOwnedSlice();
}

/// Render ListAllMyBucketsResult.
pub fn renderListBucketsResult(
    allocator: std.mem.Allocator,
    owner_id: []const u8,
    owner_display_name: []const u8,
    bucket_names: []const []const u8,
    creation_dates: []const []const u8,
) ![]u8 {
    var x = XmlWriter.init(allocator);
    defer x.deinit();

    try x.xmlDeclaration();
    try x.openTagWithNs("ListAllMyBucketsResult", S3_NS);

    try x.openTag("Owner");
    try x.element("ID", owner_id);
    try x.element("DisplayName", owner_display_name);
    try x.closeTag("Owner");

    try x.openTag("Buckets");
    for (bucket_names, 0..) |name, i| {
        try x.openTag("Bucket");
        try x.element("Name", name);
        if (i < creation_dates.len) {
            try x.element("CreationDate", creation_dates[i]);
        }
        try x.closeTag("Bucket");
    }
    try x.closeTag("Buckets");

    try x.closeTag("ListAllMyBucketsResult");

    return x.toOwnedSlice();
}

/// Metadata fields for a single object entry in a list response.
pub const ListObjectEntry = struct {
    key: []const u8,
    last_modified: []const u8,
    etag: []const u8,
    size: u64,
    storage_class: []const u8,
    owner_id: []const u8 = "",
    owner_display: []const u8 = "",
};

/// Render ListBucketResult (ListObjectsV2) with full S3 fields.
pub fn renderListObjectsV2Result(
    allocator: std.mem.Allocator,
    bucket_name: []const u8,
    prefix: []const u8,
    delimiter: []const u8,
    max_keys: u32,
    key_count: usize,
    is_truncated: bool,
    entries: []const ListObjectEntry,
    common_prefixes: []const []const u8,
    continuation_token: []const u8,
    next_continuation_token: []const u8,
    start_after: []const u8,
) ![]u8 {
    var x = XmlWriter.init(allocator);
    defer x.deinit();

    try x.xmlDeclaration();
    try x.openTagWithNs("ListBucketResult", S3_NS);

    try x.element("Name", bucket_name);
    try x.element("Prefix", prefix);
    try x.element("KeyCount", try std.fmt.allocPrint(allocator, "{d}", .{key_count}));
    try x.element("MaxKeys", try std.fmt.allocPrint(allocator, "{d}", .{max_keys}));
    if (delimiter.len > 0) {
        try x.element("Delimiter", delimiter);
    }
    try x.element("IsTruncated", if (is_truncated) "true" else "false");

    if (continuation_token.len > 0) {
        try x.element("ContinuationToken", continuation_token);
    }
    if (next_continuation_token.len > 0) {
        try x.element("NextContinuationToken", next_continuation_token);
    }
    if (start_after.len > 0) {
        try x.element("StartAfter", start_after);
    }

    for (entries) |entry| {
        try x.openTag("Contents");
        try x.element("Key", entry.key);
        try x.element("LastModified", entry.last_modified);
        try x.element("ETag", entry.etag);
        try x.element("Size", try std.fmt.allocPrint(allocator, "{d}", .{entry.size}));
        try x.element("StorageClass", entry.storage_class);
        if (entry.owner_id.len > 0) {
            try x.openTag("Owner");
            try x.element("ID", entry.owner_id);
            try x.element("DisplayName", entry.owner_display);
            try x.closeTag("Owner");
        }
        try x.closeTag("Contents");
    }

    for (common_prefixes) |cp| {
        try x.openTag("CommonPrefixes");
        try x.element("Prefix", cp);
        try x.closeTag("CommonPrefixes");
    }

    try x.closeTag("ListBucketResult");

    return x.toOwnedSlice();
}

/// Render ListBucketResult (ListObjects V1) with full S3 fields.
pub fn renderListObjectsV1Result(
    allocator: std.mem.Allocator,
    bucket_name: []const u8,
    prefix: []const u8,
    delimiter: []const u8,
    max_keys: u32,
    is_truncated: bool,
    entries: []const ListObjectEntry,
    common_prefixes: []const []const u8,
    marker: []const u8,
    next_marker: []const u8,
) ![]u8 {
    var x = XmlWriter.init(allocator);
    defer x.deinit();

    try x.xmlDeclaration();
    try x.openTagWithNs("ListBucketResult", S3_NS);

    try x.element("Name", bucket_name);
    try x.element("Prefix", prefix);
    if (marker.len > 0) {
        try x.element("Marker", marker);
    } else {
        try x.emptyElement("Marker");
    }
    try x.element("MaxKeys", try std.fmt.allocPrint(allocator, "{d}", .{max_keys}));
    if (delimiter.len > 0) {
        try x.element("Delimiter", delimiter);
    }
    try x.element("IsTruncated", if (is_truncated) "true" else "false");
    if (next_marker.len > 0) {
        try x.element("NextMarker", next_marker);
    }

    for (entries) |entry| {
        try x.openTag("Contents");
        try x.element("Key", entry.key);
        try x.element("LastModified", entry.last_modified);
        try x.element("ETag", entry.etag);
        try x.element("Size", try std.fmt.allocPrint(allocator, "{d}", .{entry.size}));
        try x.element("StorageClass", entry.storage_class);
        if (entry.owner_id.len > 0) {
            try x.openTag("Owner");
            try x.element("ID", entry.owner_id);
            try x.element("DisplayName", entry.owner_display);
            try x.closeTag("Owner");
        }
        try x.closeTag("Contents");
    }

    for (common_prefixes) |cp| {
        try x.openTag("CommonPrefixes");
        try x.element("Prefix", cp);
        try x.closeTag("CommonPrefixes");
    }

    try x.closeTag("ListBucketResult");

    return x.toOwnedSlice();
}

/// Render CopyObjectResult XML.
pub fn renderCopyObjectResult(
    allocator: std.mem.Allocator,
    etag: []const u8,
    last_modified: []const u8,
) ![]u8 {
    var x = XmlWriter.init(allocator);
    defer x.deinit();

    try x.xmlDeclaration();
    try x.openTagWithNs("CopyObjectResult", S3_NS);
    try x.element("ETag", etag);
    try x.element("LastModified", last_modified);
    try x.closeTag("CopyObjectResult");

    return x.toOwnedSlice();
}

/// Render InitiateMultipartUploadResult.
pub fn renderInitiateMultipartUploadResult(
    allocator: std.mem.Allocator,
    bucket: []const u8,
    key: []const u8,
    upload_id: []const u8,
) ![]u8 {
    var x = XmlWriter.init(allocator);
    defer x.deinit();

    try x.xmlDeclaration();
    try x.openTagWithNs("InitiateMultipartUploadResult", S3_NS);
    try x.element("Bucket", bucket);
    try x.element("Key", key);
    try x.element("UploadId", upload_id);
    try x.closeTag("InitiateMultipartUploadResult");

    return x.toOwnedSlice();
}

/// Render CompleteMultipartUploadResult.
pub fn renderCompleteMultipartUploadResult(
    allocator: std.mem.Allocator,
    location: []const u8,
    bucket: []const u8,
    key: []const u8,
    etag: []const u8,
) ![]u8 {
    var x = XmlWriter.init(allocator);
    defer x.deinit();

    try x.xmlDeclaration();
    try x.openTagWithNs("CompleteMultipartUploadResult", S3_NS);
    try x.element("Location", location);
    try x.element("Bucket", bucket);
    try x.element("Key", key);
    try x.element("ETag", etag);
    try x.closeTag("CompleteMultipartUploadResult");

    return x.toOwnedSlice();
}

/// Entry for a multipart upload in ListMultipartUploadsResult.
pub const MultipartUploadEntry = struct {
    key: []const u8,
    upload_id: []const u8,
    owner_id: []const u8 = "",
    owner_display: []const u8 = "",
    storage_class: []const u8 = "STANDARD",
    initiated: []const u8 = "",
};

/// Render ListMultipartUploadsResult XML.
pub fn renderListMultipartUploadsResult(
    allocator: std.mem.Allocator,
    bucket_name: []const u8,
    key_marker: []const u8,
    upload_id_marker: []const u8,
    next_key_marker: []const u8,
    next_upload_id_marker: []const u8,
    max_uploads: u32,
    is_truncated: bool,
    uploads: []const MultipartUploadEntry,
    prefix: []const u8,
    delimiter: []const u8,
    common_prefixes: []const []const u8,
) ![]u8 {
    var x = XmlWriter.init(allocator);
    defer x.deinit();

    try x.xmlDeclaration();
    try x.openTagWithNs("ListMultipartUploadsResult", S3_NS);

    try x.element("Bucket", bucket_name);
    if (key_marker.len > 0) {
        try x.element("KeyMarker", key_marker);
    } else {
        try x.emptyElement("KeyMarker");
    }
    if (upload_id_marker.len > 0) {
        try x.element("UploadIdMarker", upload_id_marker);
    } else {
        try x.emptyElement("UploadIdMarker");
    }
    if (next_key_marker.len > 0) {
        try x.element("NextKeyMarker", next_key_marker);
    }
    if (next_upload_id_marker.len > 0) {
        try x.element("NextUploadIdMarker", next_upload_id_marker);
    }
    if (prefix.len > 0) {
        try x.element("Prefix", prefix);
    } else {
        try x.emptyElement("Prefix");
    }
    if (delimiter.len > 0) {
        try x.element("Delimiter", delimiter);
    }
    try x.element("MaxUploads", try std.fmt.allocPrint(allocator, "{d}", .{max_uploads}));
    try x.element("IsTruncated", if (is_truncated) "true" else "false");

    for (uploads) |upload| {
        try x.openTag("Upload");
        try x.element("Key", upload.key);
        try x.element("UploadId", upload.upload_id);
        if (upload.owner_id.len > 0) {
            try x.openTag("Initiator");
            try x.element("ID", upload.owner_id);
            try x.element("DisplayName", upload.owner_display);
            try x.closeTag("Initiator");
            try x.openTag("Owner");
            try x.element("ID", upload.owner_id);
            try x.element("DisplayName", upload.owner_display);
            try x.closeTag("Owner");
        }
        try x.element("StorageClass", upload.storage_class);
        try x.element("Initiated", upload.initiated);
        try x.closeTag("Upload");
    }

    for (common_prefixes) |cp| {
        try x.openTag("CommonPrefixes");
        try x.element("Prefix", cp);
        try x.closeTag("CommonPrefixes");
    }

    try x.closeTag("ListMultipartUploadsResult");

    return x.toOwnedSlice();
}

/// Entry for a part in ListPartsResult.
pub const PartEntry = struct {
    part_number: u32,
    last_modified: []const u8,
    etag: []const u8,
    size: u64,
};

/// Render ListPartsResult XML.
pub fn renderListPartsResult(
    allocator: std.mem.Allocator,
    bucket_name: []const u8,
    key: []const u8,
    upload_id: []const u8,
    owner_id: []const u8,
    owner_display: []const u8,
    storage_class: []const u8,
    part_number_marker: u32,
    next_part_number_marker: u32,
    max_parts: u32,
    is_truncated: bool,
    parts: []const PartEntry,
) ![]u8 {
    var x = XmlWriter.init(allocator);
    defer x.deinit();

    try x.xmlDeclaration();
    try x.openTagWithNs("ListPartsResult", S3_NS);

    try x.element("Bucket", bucket_name);
    try x.element("Key", key);
    try x.element("UploadId", upload_id);

    if (owner_id.len > 0) {
        try x.openTag("Initiator");
        try x.element("ID", owner_id);
        try x.element("DisplayName", owner_display);
        try x.closeTag("Initiator");
        try x.openTag("Owner");
        try x.element("ID", owner_id);
        try x.element("DisplayName", owner_display);
        try x.closeTag("Owner");
    }

    try x.element("StorageClass", storage_class);
    try x.element("PartNumberMarker", try std.fmt.allocPrint(allocator, "{d}", .{part_number_marker}));
    if (is_truncated) {
        try x.element("NextPartNumberMarker", try std.fmt.allocPrint(allocator, "{d}", .{next_part_number_marker}));
    }
    try x.element("MaxParts", try std.fmt.allocPrint(allocator, "{d}", .{max_parts}));
    try x.element("IsTruncated", if (is_truncated) "true" else "false");

    for (parts) |part| {
        try x.openTag("Part");
        try x.element("PartNumber", try std.fmt.allocPrint(allocator, "{d}", .{part.part_number}));
        try x.element("LastModified", part.last_modified);
        try x.element("ETag", part.etag);
        try x.element("Size", try std.fmt.allocPrint(allocator, "{d}", .{part.size}));
        try x.closeTag("Part");
    }

    try x.closeTag("ListPartsResult");

    return x.toOwnedSlice();
}

/// Render CopyPartResult for UploadPartCopy.
pub fn renderCopyPartResult(
    allocator: std.mem.Allocator,
    etag: []const u8,
    last_modified: []const u8,
) ![]u8 {
    var x = XmlWriter.init(allocator);
    defer x.deinit();

    try x.xmlDeclaration();
    try x.openTagWithNs("CopyPartResult", S3_NS);
    try x.element("ETag", etag);
    try x.element("LastModified", last_modified);
    try x.closeTag("CopyPartResult");

    return x.toOwnedSlice();
}

/// Render DeleteResult for multi-object delete.
pub fn renderDeleteResult(
    allocator: std.mem.Allocator,
    deleted_keys: []const []const u8,
    error_keys: []const []const u8,
) ![]u8 {
    var x = XmlWriter.init(allocator);
    defer x.deinit();

    try x.xmlDeclaration();
    try x.openTagWithNs("DeleteResult", S3_NS);

    for (deleted_keys) |key| {
        try x.openTag("Deleted");
        try x.element("Key", key);
        try x.closeTag("Deleted");
    }

    for (error_keys) |key| {
        try x.openTag("Error");
        try x.element("Key", key);
        try x.element("Code", "InternalError");
        try x.element("Message", "Internal error");
        try x.closeTag("Error");
    }

    try x.closeTag("DeleteResult");

    return x.toOwnedSlice();
}

/// Render a LocationConstraint XML response for GetBucketLocation.
/// The us-east-1 quirk: returns an empty <LocationConstraint/> element
/// instead of the string "us-east-1".
pub fn renderLocationConstraint(
    allocator: std.mem.Allocator,
    region: []const u8,
) ![]u8 {
    var x = XmlWriter.init(allocator);
    defer x.deinit();

    try x.xmlDeclaration();
    if (region.len == 0 or std.mem.eql(u8, region, "us-east-1")) {
        try x.emptyElementWithNs("LocationConstraint", S3_NS);
    } else {
        try x.openTagWithNs("LocationConstraint", S3_NS);
        try x.raw(region);
        try x.closeTag("LocationConstraint");
    }

    return x.toOwnedSlice();
}

/// Render an AccessControlPolicy XML response for GetBucketAcl / GetObjectAcl.
/// The ACL is stored as a JSON string with this structure:
///   {"owner":{"id":"...","display_name":"..."},"grants":[{"grantee":{"type":"CanonicalUser","id":"...","display_name":"..."},"permission":"FULL_CONTROL"},{"grantee":{"type":"Group","uri":"..."},"permission":"READ"}]}
pub fn renderAccessControlPolicy(
    allocator: std.mem.Allocator,
    owner_id: []const u8,
    owner_display: []const u8,
    acl_json: []const u8,
) ![]u8 {
    var x = XmlWriter.init(allocator);
    defer x.deinit();

    const XSI_NS = "http://www.w3.org/2001/XMLSchema-instance";

    try x.xmlDeclaration();
    try x.openTagWithNs("AccessControlPolicy", S3_NS);

    try x.openTag("Owner");
    try x.element("ID", owner_id);
    try x.element("DisplayName", owner_display);
    try x.closeTag("Owner");

    try x.openTag("AccessControlList");

    // Parse the ACL JSON to extract grants.
    // If parsing fails or no grants, emit a default FULL_CONTROL grant for the owner.
    const grants = parseAclGrants(allocator, acl_json) catch null;
    defer {
        if (grants) |grant_list| {
            for (grant_list) |grant| {
                if (grant.grantee_type.len > 0) allocator.free(grant.grantee_type);
                if (grant.grantee_id.len > 0) allocator.free(grant.grantee_id);
                if (grant.grantee_display.len > 0) allocator.free(grant.grantee_display);
                if (grant.uri.len > 0) allocator.free(grant.uri);
                if (grant.permission.len > 0) allocator.free(grant.permission);
            }
            allocator.free(grant_list);
        }
    }

    if (grants) |grant_list| {
        for (grant_list) |grant| {
            try x.openTag("Grant");

            if (std.mem.eql(u8, grant.grantee_type, "Group")) {
                try x.raw("<Grantee xmlns:xsi=\"");
                try x.raw(XSI_NS);
                try x.raw("\" xsi:type=\"Group\">");
                try x.element("URI", grant.uri);
                try x.raw("</Grantee>");
            } else {
                // CanonicalUser (default)
                try x.raw("<Grantee xmlns:xsi=\"");
                try x.raw(XSI_NS);
                try x.raw("\" xsi:type=\"CanonicalUser\">");
                try x.element("ID", grant.grantee_id);
                try x.element("DisplayName", grant.grantee_display);
                try x.raw("</Grantee>");
            }

            try x.element("Permission", grant.permission);
            try x.closeTag("Grant");
        }
    } else {
        // Default: owner gets FULL_CONTROL
        try x.openTag("Grant");
        try x.raw("<Grantee xmlns:xsi=\"");
        try x.raw(XSI_NS);
        try x.raw("\" xsi:type=\"CanonicalUser\">");
        try x.element("ID", owner_id);
        try x.element("DisplayName", owner_display);
        try x.raw("</Grantee>");
        try x.element("Permission", "FULL_CONTROL");
        try x.closeTag("Grant");
    }

    try x.closeTag("AccessControlList");
    try x.closeTag("AccessControlPolicy");

    return x.toOwnedSlice();
}

/// A parsed grant from ACL JSON.
const AclGrant = struct {
    grantee_type: []const u8, // "CanonicalUser" or "Group"
    grantee_id: []const u8, // for CanonicalUser
    grantee_display: []const u8, // for CanonicalUser
    uri: []const u8, // for Group
    permission: []const u8,
};

/// Parse ACL grants from JSON string. Returns null if parsing fails.
/// Uses simple JSON string scanning since Zig's std.json requires type definitions.
fn parseAclGrants(allocator: std.mem.Allocator, acl_json: []const u8) !?[]AclGrant {
    if (acl_json.len < 3 or std.mem.eql(u8, acl_json, "{}")) return null;

    // Use std.json to parse
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, acl_json, .{}) catch return null;
    defer parsed.deinit();

    const root = parsed.value;
    if (root != .object) return null;

    const grants_val = root.object.get("grants") orelse return null;
    if (grants_val != .array) return null;

    const grants_array = grants_val.array;
    if (grants_array.items.len == 0) return null;

    var result: std.ArrayList(AclGrant) = .empty;
    defer result.deinit(allocator);

    for (grants_array.items) |grant_val| {
        if (grant_val != .object) continue;

        const grantee_val = grant_val.object.get("grantee") orelse continue;
        if (grantee_val != .object) continue;

        const perm_val = grant_val.object.get("permission") orelse continue;
        const permission = switch (perm_val) {
            .string => |s| s,
            else => continue,
        };

        const grantee_type_val = grantee_val.object.get("type") orelse continue;
        const grantee_type = switch (grantee_type_val) {
            .string => |s| s,
            else => continue,
        };

        if (std.mem.eql(u8, grantee_type, "Group")) {
            const uri_val = grantee_val.object.get("uri") orelse continue;
            const uri = switch (uri_val) {
                .string => |s| s,
                else => continue,
            };
            try result.append(allocator, AclGrant{
                .grantee_type = try allocator.dupe(u8, grantee_type),
                .grantee_id = "",
                .grantee_display = "",
                .uri = try allocator.dupe(u8, uri),
                .permission = try allocator.dupe(u8, permission),
            });
        } else {
            // CanonicalUser
            const id_val = grantee_val.object.get("id");
            const display_val = grantee_val.object.get("display_name");
            const grantee_id = if (id_val) |v| switch (v) {
                .string => |s| s,
                else => "",
            } else "";
            const grantee_display = if (display_val) |v| switch (v) {
                .string => |s| s,
                else => "",
            } else "";

            try result.append(allocator, AclGrant{
                .grantee_type = try allocator.dupe(u8, grantee_type),
                .grantee_id = try allocator.dupe(u8, grantee_id),
                .grantee_display = try allocator.dupe(u8, grantee_display),
                .uri = "",
                .permission = try allocator.dupe(u8, permission),
            });
        }
    }

    if (result.items.len == 0) return null;
    return try result.toOwnedSlice(allocator);
}

test "renderError" {
    const result = try renderError(
        std.testing.allocator,
        "NoSuchBucket",
        "The specified bucket does not exist",
        "/my-bucket",
        "test-request-id",
    );
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "<Code>NoSuchBucket</Code>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<?xml version") != null);
}

test "renderLocationConstraint: us-east-1 returns empty element" {
    const result = try renderLocationConstraint(std.testing.allocator, "us-east-1");
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "<?xml version") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "LocationConstraint") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "/>") != null);
    // Should NOT contain a region value between tags
    try std.testing.expect(std.mem.indexOf(u8, result, ">us-east-1<") == null);
}

test "renderLocationConstraint: non-us-east-1 returns region" {
    const result = try renderLocationConstraint(std.testing.allocator, "eu-west-1");
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "<?xml version") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ">eu-west-1</LocationConstraint>") != null);
}

test "renderLocationConstraint: empty region returns empty element" {
    const result = try renderLocationConstraint(std.testing.allocator, "");
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "/>") != null);
}

test "renderAccessControlPolicy: default ACL" {
    const acl_json =
        \\{"owner":{"id":"owner123","display_name":"testuser"},"grants":[{"grantee":{"type":"CanonicalUser","id":"owner123","display_name":"testuser"},"permission":"FULL_CONTROL"}]}
    ;
    const result = try renderAccessControlPolicy(std.testing.allocator, "owner123", "testuser", acl_json);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "<?xml version") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<AccessControlPolicy") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<Owner>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<ID>owner123</ID>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<DisplayName>testuser</DisplayName>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "xsi:type=\"CanonicalUser\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<Permission>FULL_CONTROL</Permission>") != null);
}

test "renderAccessControlPolicy: empty ACL uses default" {
    const result = try renderAccessControlPolicy(std.testing.allocator, "owner123", "testuser", "{}");
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "xsi:type=\"CanonicalUser\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<Permission>FULL_CONTROL</Permission>") != null);
}

test "renderAccessControlPolicy: Group grantee" {
    const acl_json =
        \\{"owner":{"id":"owner123","display_name":"testuser"},"grants":[{"grantee":{"type":"CanonicalUser","id":"owner123","display_name":"testuser"},"permission":"FULL_CONTROL"},{"grantee":{"type":"Group","uri":"http://acs.amazonaws.com/groups/global/AllUsers"},"permission":"READ"}]}
    ;
    const result = try renderAccessControlPolicy(std.testing.allocator, "owner123", "testuser", acl_json);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "xsi:type=\"Group\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<Permission>READ</Permission>") != null);
}

test "renderListMultipartUploadsResult: basic" {
    // Use an arena to avoid tracking intermediate allocPrint strings.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const uploads = [_]MultipartUploadEntry{
        .{
            .key = "big-file.bin",
            .upload_id = "upload-123",
            .owner_id = "owner1",
            .owner_display = "testuser",
            .storage_class = "STANDARD",
            .initiated = "2026-01-01T12:00:00.000Z",
        },
    };
    const result = try renderListMultipartUploadsResult(
        alloc,
        "my-bucket",
        "",
        "",
        "",
        "",
        1000,
        false,
        &uploads,
        "",
        "",
        &.{},
    );

    try std.testing.expect(std.mem.indexOf(u8, result, "<?xml version") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<ListMultipartUploadsResult") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<Bucket>my-bucket</Bucket>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<Key>big-file.bin</Key>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<UploadId>upload-123</UploadId>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<Initiator>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<Owner>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<MaxUploads>1000</MaxUploads>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<IsTruncated>false</IsTruncated>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<Initiated>2026-01-01T12:00:00.000Z</Initiated>") != null);
}

test "renderListMultipartUploadsResult: empty" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const result = try renderListMultipartUploadsResult(
        alloc,
        "empty-bucket",
        "",
        "",
        "",
        "",
        1000,
        false,
        &.{},
        "",
        "",
        &.{},
    );

    try std.testing.expect(std.mem.indexOf(u8, result, "<Bucket>empty-bucket</Bucket>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<IsTruncated>false</IsTruncated>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<Upload>") == null);
}

test "renderListPartsResult: basic" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const parts = [_]PartEntry{
        .{ .part_number = 1, .last_modified = "2026-01-01T12:01:00.000Z", .etag = "\"abc123\"", .size = 5242880 },
        .{ .part_number = 2, .last_modified = "2026-01-01T12:02:00.000Z", .etag = "\"def456\"", .size = 1048576 },
    };
    const result = try renderListPartsResult(
        alloc,
        "my-bucket",
        "big-file.bin",
        "upload-123",
        "owner1",
        "testuser",
        "STANDARD",
        0,
        0,
        1000,
        false,
        &parts,
    );

    try std.testing.expect(std.mem.indexOf(u8, result, "<?xml version") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<ListPartsResult") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<Bucket>my-bucket</Bucket>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<Key>big-file.bin</Key>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<UploadId>upload-123</UploadId>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<PartNumber>1</PartNumber>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<PartNumber>2</PartNumber>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<ETag>&quot;abc123&quot;</ETag>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<Size>5242880</Size>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<MaxParts>1000</MaxParts>") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "<IsTruncated>false</IsTruncated>") != null);
}
