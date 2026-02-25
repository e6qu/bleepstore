const std = @import("std");
const backend = @import("backend.zig");
const StorageBackend = backend.StorageBackend;
const ObjectData = backend.ObjectData;
const PutObjectOptions = backend.PutObjectOptions;
const PutObjectResult = backend.PutObjectResult;
const PartInfo = backend.PartInfo;
const PutPartResult = backend.PutPartResult;
const AssemblePartsResult = backend.AssemblePartsResult;
const Md5 = std.crypto.hash.Md5;

/// AzureGatewayBackend proxies object storage operations to an upstream Azure
/// Blob Storage container via `std.http.Client` and the Azure Blob REST API.
///
/// All BleepStore buckets/objects are stored under a single upstream Azure
/// container with a key prefix to namespace them.
///
/// Key mapping:
///   Objects:  {prefix}{bleepstore_bucket}/{key}
///
/// Multipart strategy uses Azure Block Blob primitives:
///   put_part()       -> Put Block on the final blob (no temp objects)
///   assemble_parts() -> Put Block List to finalize
///   delete_parts()   -> no-op (uncommitted blocks auto-expire in 7 days)
///
/// Authentication: Bearer token via AZURE_ACCESS_TOKEN environment variable.
pub const AzureGatewayBackend = struct {
    allocator: std.mem.Allocator,
    container: []const u8,
    account_name: []const u8,
    prefix: []const u8,
    access_token: []const u8,
    /// The Azure Blob Storage host: {account_name}.blob.core.windows.net
    host: []const u8,
    http_client: std.http.Client,

    /// True if access_token was allocated by us (from env var) and must be freed.
    token_owned: bool,
    /// True if host was allocated by us and must be freed.
    host_owned: bool,

    const Self = @This();

    pub fn init(
        allocator: std.mem.Allocator,
        container: []const u8,
        account_name: []const u8,
        prefix: []const u8,
    ) !Self {
        // Resolve access token from environment.
        const token_result = std.process.getEnvVarOwned(allocator, "AZURE_ACCESS_TOKEN") catch |err| {
            std.log.err("Azure backend requires AZURE_ACCESS_TOKEN env var: {}", .{err});
            return error.InvalidConfiguration;
        };

        // Build the Azure Blob Storage host: {account_name}.blob.core.windows.net
        const host = try std.fmt.allocPrint(allocator, "{s}.blob.core.windows.net", .{account_name});
        errdefer allocator.free(host);

        var client = std.http.Client{ .allocator = allocator };

        // Verify the upstream container exists with a list call (maxresults=1).
        const verify_path = try std.fmt.allocPrint(allocator, "/{s}?restype=container&comp=list&maxresults=1", .{container});
        defer allocator.free(verify_path);

        const verify_result = makeAzureRequest(
            allocator,
            &client,
            host,
            "GET",
            verify_path,
            null,
            null,
            token_result,
        ) catch |err| {
            std.log.err("Azure gateway: failed to verify upstream container '{s}': {}", .{ container, err });
            client.deinit();
            allocator.free(token_result);
            allocator.free(host);
            return error.InvalidConfiguration;
        };
        defer allocator.free(verify_result.body);

        if (verify_result.status != 200) {
            std.log.err("Azure gateway: upstream container '{s}' returned status {d}", .{ container, verify_result.status });
            client.deinit();
            allocator.free(token_result);
            allocator.free(host);
            return error.InvalidConfiguration;
        }

        std.log.info("Azure gateway backend initialized: container={s} account={s} prefix='{s}'", .{
            container, account_name, prefix,
        });

        return Self{
            .allocator = allocator,
            .container = container,
            .account_name = account_name,
            .prefix = prefix,
            .access_token = token_result,
            .host = host,
            .http_client = client,
            .token_owned = true,
            .host_owned = true,
        };
    }

    pub fn deinit(self: *Self) void {
        self.http_client.deinit();
        if (self.token_owned) {
            self.allocator.free(self.access_token);
        }
        if (self.host_owned) {
            self.allocator.free(self.host);
        }
    }

    /// Map a BleepStore bucket/key to an upstream Azure blob name.
    fn blobName(self: *const Self, allocator: std.mem.Allocator, bucket_name: []const u8, key: []const u8) ![]u8 {
        return std.fmt.allocPrint(allocator, "{s}{s}/{s}", .{ self.prefix, bucket_name, key });
    }

    /// Build the Azure Blob Storage path: /{container}/{blob_name}
    fn blobPath(self: *const Self, allocator: std.mem.Allocator, blob_name_val: []const u8) ![]u8 {
        return std.fmt.allocPrint(allocator, "/{s}/{s}", .{ self.container, blob_name_val });
    }

    /// Generate a block ID for Azure staged blocks.
    ///
    /// Block IDs must be base64-encoded and the same length for all
    /// blocks in a blob. Includes upload_id to avoid collisions between
    /// concurrent multipart uploads to the same key.
    ///
    /// Format: base64("{upload_id}:{part_number:05}")
    fn blockId(allocator: std.mem.Allocator, upload_id: []const u8, part_number: u32) ![]u8 {
        // Build the raw block ID string: "{upload_id}:{part_number:05}"
        const raw = try std.fmt.allocPrint(allocator, "{s}:{d:0>5}", .{ upload_id, part_number });
        defer allocator.free(raw);

        // Base64 encode it.
        return base64Encode(allocator, raw);
    }

    // --- Vtable implementations ---

    fn putObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8, data: []const u8, opts: PutObjectOptions) anyerror!PutObjectResult {
        const self = getSelf(ctx);
        _ = opts;

        // Compute MD5 locally for a consistent ETag.
        var md5_hash: [Md5.digest_length]u8 = undefined;
        Md5.hash(data, &md5_hash, .{});
        const hex = std.fmt.bytesToHex(md5_hash, .lower);
        const etag = try std.fmt.allocPrint(self.allocator, "\"{s}\"", .{@as([]const u8, &hex)});
        errdefer self.allocator.free(etag);

        // Build upstream blob name and path.
        const blob = try self.blobName(self.allocator, bucket_name, key);
        defer self.allocator.free(blob);

        const path = try self.blobPath(self.allocator, blob);
        defer self.allocator.free(path);

        // PUT the blob with BlockBlob type.
        const result = try makeAzureRequestWithBlobType(
            self.allocator,
            &self.http_client,
            self.host,
            "PUT",
            path,
            "application/octet-stream",
            data,
            self.access_token,
            "BlockBlob",
        );
        defer self.allocator.free(result.body);

        if (result.status != 201 and result.status != 200) {
            std.log.err("Azure PUT blob failed: status={d} body={s}", .{ result.status, result.body });
            return error.UpstreamError;
        }

        return PutObjectResult{ .etag = etag };
    }

    fn getObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8) anyerror!ObjectData {
        const self = getSelf(ctx);

        const blob = try self.blobName(self.allocator, bucket_name, key);
        defer self.allocator.free(blob);

        const path = try self.blobPath(self.allocator, blob);
        defer self.allocator.free(path);

        const result = try makeAzureRequest(
            self.allocator,
            &self.http_client,
            self.host,
            "GET",
            path,
            null,
            null,
            self.access_token,
        );

        if (result.status == 404) {
            self.allocator.free(result.body);
            return error.NoSuchKey;
        }
        if (result.status != 200) {
            std.log.err("Azure GET blob failed: status={d}", .{result.status});
            self.allocator.free(result.body);
            return error.UpstreamError;
        }

        return ObjectData{
            .body = result.body,
            .content_length = result.body.len,
            .content_type = "application/octet-stream",
            .etag = "",
            .last_modified = "",
        };
    }

    fn deleteObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8) anyerror!void {
        const self = getSelf(ctx);

        const blob = try self.blobName(self.allocator, bucket_name, key);
        defer self.allocator.free(blob);

        const path = try self.blobPath(self.allocator, blob);
        defer self.allocator.free(path);

        const result = try makeAzureRequest(
            self.allocator,
            &self.http_client,
            self.host,
            "DELETE",
            path,
            null,
            null,
            self.access_token,
        );
        defer self.allocator.free(result.body);

        // Azure returns 202 on successful delete. 404 is fine (idempotent).
        if (result.status != 202 and result.status != 200 and result.status != 404) {
            std.log.err("Azure DELETE blob failed: status={d}", .{result.status});
            return error.UpstreamError;
        }
    }

    fn headObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8) anyerror!ObjectData {
        const self = getSelf(ctx);

        const blob = try self.blobName(self.allocator, bucket_name, key);
        defer self.allocator.free(blob);

        const path = try self.blobPath(self.allocator, blob);
        defer self.allocator.free(path);

        const result = try makeAzureRequest(
            self.allocator,
            &self.http_client,
            self.host,
            "HEAD",
            path,
            null,
            null,
            self.access_token,
        );
        defer self.allocator.free(result.body);

        if (result.status == 404) {
            return error.NoSuchKey;
        }
        if (result.status != 200) {
            std.log.err("Azure HEAD blob failed: status={d}", .{result.status});
            return error.UpstreamError;
        }

        return ObjectData{
            .body = null,
            .content_length = result.content_length,
            .content_type = "application/octet-stream",
            .etag = "",
            .last_modified = "",
        };
    }

    fn copyObject(ctx: *anyopaque, src_bucket: []const u8, src_key: []const u8, dst_bucket: []const u8, dst_key: []const u8) anyerror!PutObjectResult {
        const self = getSelf(ctx);

        // Build source and destination blob names.
        const src_blob = try self.blobName(self.allocator, src_bucket, src_key);
        defer self.allocator.free(src_blob);

        const dst_blob = try self.blobName(self.allocator, dst_bucket, dst_key);
        defer self.allocator.free(dst_blob);

        const dst_path = try self.blobPath(self.allocator, dst_blob);
        defer self.allocator.free(dst_path);

        // Build the source URL for server-side copy.
        const source_url = try std.fmt.allocPrint(
            self.allocator,
            "https://{s}/{s}/{s}",
            .{ self.host, self.container, src_blob },
        );
        defer self.allocator.free(source_url);

        // Execute server-side copy via x-ms-copy-source header.
        const copy_result = try makeAzureRequestWithCopySource(
            self.allocator,
            &self.http_client,
            self.host,
            dst_path,
            self.access_token,
            source_url,
        );
        defer self.allocator.free(copy_result.body);

        if (copy_result.status == 404) {
            return error.NoSuchKey;
        }
        if (copy_result.status != 202 and copy_result.status != 200 and copy_result.status != 201) {
            std.log.err("Azure copy blob failed: status={d} body={s}", .{ copy_result.status, copy_result.body });
            return error.UpstreamError;
        }

        // Download destination to compute MD5 for consistent ETag.
        const get_path = try self.blobPath(self.allocator, dst_blob);
        defer self.allocator.free(get_path);

        const get_result = try makeAzureRequest(
            self.allocator,
            &self.http_client,
            self.host,
            "GET",
            get_path,
            null,
            null,
            self.access_token,
        );
        defer self.allocator.free(get_result.body);

        if (get_result.status != 200) {
            return error.UpstreamError;
        }

        var md5_hash: [Md5.digest_length]u8 = undefined;
        Md5.hash(get_result.body, &md5_hash, .{});
        const hex_val = std.fmt.bytesToHex(md5_hash, .lower);
        const etag = try std.fmt.allocPrint(self.allocator, "\"{s}\"", .{@as([]const u8, &hex_val)});

        return PutObjectResult{ .etag = etag };
    }

    fn putPart(ctx: *anyopaque, bucket_name: []const u8, upload_id: []const u8, part_number: u32, data: []const u8) anyerror!PutPartResult {
        const self = getSelf(ctx);

        // Compute MD5 locally for consistent ETag.
        var md5_hash: [Md5.digest_length]u8 = undefined;
        Md5.hash(data, &md5_hash, .{});
        const hex = std.fmt.bytesToHex(md5_hash, .lower);
        const etag = try std.fmt.allocPrint(self.allocator, "\"{s}\"", .{@as([]const u8, &hex)});
        errdefer self.allocator.free(etag);

        // Azure Block Blob strategy: stage block directly on the final blob.
        // No temporary part objects are created. Uncommitted blocks auto-expire
        // in 7 days.

        // We need the object key to stage blocks on the final blob. The vtable
        // passes bucket_name but not key. However, we can derive the blob name
        // from the upload_id. Actually, looking at the vtable interface, putPart
        // receives bucket_name (not key). We need to stage on the correct blob.
        //
        // The vtable for putPart does not include the key parameter. In the AWS
        // and GCP backends, parts are stored as temporary objects keyed by
        // upload_id. For Azure, we need to do the same -- store parts as
        // temporary blobs since we don't know the final key at put_part time.
        //
        // The assembleParts method receives both bucket and key, so it can
        // download the temporary parts and then commit them as blocks on the
        // final blob.
        //
        // Strategy: Store parts as temporary blobs at {prefix}.parts/{upload_id}/{part_number}
        // Then in assembleParts, download each, stage as blocks on the final blob,
        // and commit the block list.
        _ = bucket_name;

        const part_blob = try std.fmt.allocPrint(self.allocator, "{s}.parts/{s}/{d}", .{ self.prefix, upload_id, part_number });
        defer self.allocator.free(part_blob);

        const part_path = try std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.container, part_blob });
        defer self.allocator.free(part_path);

        const result = try makeAzureRequestWithBlobType(
            self.allocator,
            &self.http_client,
            self.host,
            "PUT",
            part_path,
            "application/octet-stream",
            data,
            self.access_token,
            "BlockBlob",
        );
        defer self.allocator.free(result.body);

        if (result.status != 201 and result.status != 200) {
            std.log.err("Azure PUT part blob failed: status={d}", .{result.status});
            return error.UpstreamError;
        }

        return PutPartResult{ .etag = etag };
    }

    fn assembleParts(ctx: *anyopaque, bucket_name: []const u8, key: []const u8, upload_id: []const u8, parts: []const PartInfo) anyerror!AssemblePartsResult {
        const self = getSelf(ctx);

        // Strategy: download each temporary part blob, stage as blocks on the
        // final blob using Put Block, then commit with Put Block List.
        // This is necessary because putPart stores parts as temp blobs
        // (the vtable doesn't pass the key to putPart).

        const final_blob = try self.blobName(self.allocator, bucket_name, key);
        defer self.allocator.free(final_blob);

        const final_base_path = try self.blobPath(self.allocator, final_blob);
        defer self.allocator.free(final_base_path);

        // Track block IDs for the commit.
        var block_ids = std.ArrayList([]u8).empty;
        defer {
            for (block_ids.items) |bid| self.allocator.free(bid);
            block_ids.deinit(self.allocator);
        }

        // Track MD5 concatenation for composite ETag.
        var md5_concat = std.ArrayList(u8).empty;
        defer md5_concat.deinit(self.allocator);

        var total_size: u64 = 0;

        for (parts) |part| {
            // Download the temporary part blob.
            const part_blob = try std.fmt.allocPrint(self.allocator, "{s}.parts/{s}/{d}", .{ self.prefix, upload_id, part.part_number });
            defer self.allocator.free(part_blob);

            const part_path = try std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.container, part_blob });
            defer self.allocator.free(part_path);

            const get_result = try makeAzureRequest(
                self.allocator,
                &self.http_client,
                self.host,
                "GET",
                part_path,
                null,
                null,
                self.access_token,
            );
            defer self.allocator.free(get_result.body);

            if (get_result.status != 200) {
                std.log.err("Azure GET part blob failed: status={d}", .{get_result.status});
                return error.InvalidPart;
            }

            total_size += get_result.body.len;

            // Generate block ID for this part.
            const bid = try blockId(self.allocator, upload_id, part.part_number);
            errdefer self.allocator.free(bid);

            // Stage the block on the final blob: PUT {path}?comp=block&blockid={bid}
            const block_path = try std.fmt.allocPrint(
                self.allocator,
                "{s}?comp=block&blockid={s}",
                .{ final_base_path, bid },
            );
            defer self.allocator.free(block_path);

            const block_result = try makeAzureRequest(
                self.allocator,
                &self.http_client,
                self.host,
                "PUT",
                block_path,
                "application/octet-stream",
                get_result.body,
                self.access_token,
            );
            defer self.allocator.free(block_result.body);

            if (block_result.status != 201 and block_result.status != 200) {
                std.log.err("Azure PUT block failed: status={d}", .{block_result.status});
                return error.UpstreamError;
            }

            try block_ids.append(self.allocator, bid);

            // Parse part ETag to binary MD5 for composite ETag computation.
            var etag_hex = part.etag;
            if (etag_hex.len >= 2 and etag_hex[0] == '"' and etag_hex[etag_hex.len - 1] == '"') {
                etag_hex = etag_hex[1 .. etag_hex.len - 1];
            }
            if (etag_hex.len == 32) {
                var md5_bytes: [16]u8 = undefined;
                for (0..16) |i| {
                    md5_bytes[i] = std.fmt.parseInt(u8, etag_hex[i * 2 .. i * 2 + 2], 16) catch 0;
                }
                try md5_concat.appendSlice(self.allocator, &md5_bytes);
            }
        }

        // Commit the block list: PUT {path}?comp=blocklist with XML body.
        const blocklist_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}?comp=blocklist",
            .{final_base_path},
        );
        defer self.allocator.free(blocklist_path);

        // Build the block list XML.
        var xml_buf = std.ArrayList(u8).empty;
        defer xml_buf.deinit(self.allocator);

        try xml_buf.appendSlice(self.allocator, "<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>");
        for (block_ids.items) |bid| {
            try xml_buf.appendSlice(self.allocator, "<Latest>");
            try xml_buf.appendSlice(self.allocator, bid);
            try xml_buf.appendSlice(self.allocator, "</Latest>");
        }
        try xml_buf.appendSlice(self.allocator, "</BlockList>");

        const commit_result = try makeAzureRequest(
            self.allocator,
            &self.http_client,
            self.host,
            "PUT",
            blocklist_path,
            "application/xml",
            xml_buf.items,
            self.access_token,
        );
        defer self.allocator.free(commit_result.body);

        if (commit_result.status != 201 and commit_result.status != 200) {
            std.log.err("Azure PUT block list failed: status={d} body={s}", .{ commit_result.status, commit_result.body });
            return error.UpstreamError;
        }

        // Compute composite ETag: MD5 of concatenated binary MD5s.
        var composite_hash: [Md5.digest_length]u8 = undefined;
        Md5.hash(md5_concat.items, &composite_hash, .{});
        const composite_hex = std.fmt.bytesToHex(composite_hash, .lower);
        const etag = try std.fmt.allocPrint(self.allocator, "\"{s}-{d}\"", .{ @as([]const u8, &composite_hex), parts.len });

        return AssemblePartsResult{
            .etag = etag,
            .total_size = total_size,
        };
    }

    fn deleteParts(ctx: *anyopaque, bucket_name: []const u8, upload_id: []const u8) anyerror!void {
        const self = getSelf(ctx);
        _ = bucket_name;

        // Delete temporary part blobs stored at {prefix}.parts/{upload_id}/{part_number}.
        // Try parts 1..100, stop after cleanup. S3 delete is idempotent.
        for (1..101) |pn| {
            const part_blob = std.fmt.allocPrint(self.allocator, "{s}.parts/{s}/{d}", .{ self.prefix, upload_id, @as(u32, @intCast(pn)) }) catch continue;
            defer self.allocator.free(part_blob);

            const part_path = std.fmt.allocPrint(self.allocator, "/{s}/{s}", .{ self.container, part_blob }) catch continue;
            defer self.allocator.free(part_path);

            const result = makeAzureRequest(
                self.allocator,
                &self.http_client,
                self.host,
                "DELETE",
                part_path,
                null,
                null,
                self.access_token,
            ) catch continue;
            self.allocator.free(result.body);
        }
    }

    fn createBucket(ctx: *anyopaque, bucket_name: []const u8) anyerror!void {
        // In gateway mode, "creating a bucket" is a logical operation.
        // All BleepStore buckets map to prefixes within the single upstream Azure container.
        // No upstream action needed -- metadata creation is done by the handler.
        _ = getSelf(ctx);
        _ = bucket_name;
    }

    fn deleteBucket(ctx: *anyopaque, bucket_name: []const u8) anyerror!void {
        // In gateway mode, "deleting a bucket" is a logical operation.
        // Upstream blobs under this prefix remain (eventual cleanup is the
        // responsibility of the operator). Metadata deletion is done by the handler.
        _ = getSelf(ctx);
        _ = bucket_name;
    }

    fn healthCheck(ctx: *anyopaque) anyerror!void {
        // Gateway mode: assume upstream is reachable (no-op probe).
        _ = getSelf(ctx);
    }

    const vtable_instance = StorageBackend.VTable{
        .putObject = putObject,
        .getObject = getObject,
        .deleteObject = deleteObject,
        .headObject = headObject,
        .copyObject = copyObject,
        .putPart = putPart,
        .assembleParts = assembleParts,
        .deleteParts = deleteParts,
        .createBucket = createBucket,
        .deleteBucket = deleteBucket,
        .healthCheck = healthCheck,
    };

    pub fn storageBackend(self: *Self) StorageBackend {
        return .{
            .ctx = @ptrCast(self),
            .vtable = &vtable_instance,
        };
    }

    fn getSelf(ctx: *anyopaque) *Self {
        return @ptrCast(@alignCast(ctx));
    }
};

// ===========================================================================
// Azure HTTP request helpers
// ===========================================================================

/// Result of an HTTP request to Azure Blob Storage.
const HttpResult = struct {
    status: u16,
    body: []u8,
    content_length: u64,
};

/// Make an authenticated HTTP request to Azure Blob Storage.
///
/// Uses Bearer token authentication. All requests go to
/// https://{host}/{path}.
fn makeAzureRequest(
    allocator: std.mem.Allocator,
    client: *std.http.Client,
    host: []const u8,
    method: []const u8,
    path: []const u8,
    content_type: ?[]const u8,
    body: ?[]const u8,
    access_token: []const u8,
) !HttpResult {
    // Build the full URL.
    const url_str = try std.fmt.allocPrint(allocator, "https://{s}{s}", .{ host, path });
    defer allocator.free(url_str);

    // Build Authorization header.
    const auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{access_token});
    defer allocator.free(auth_header);

    // Azure API version header.
    const api_version = "2023-11-03";

    // Determine the HTTP method enum.
    const method_enum: std.http.Method = if (std.mem.eql(u8, method, "GET"))
        .GET
    else if (std.mem.eql(u8, method, "POST"))
        .POST
    else if (std.mem.eql(u8, method, "PUT"))
        .PUT
    else if (std.mem.eql(u8, method, "DELETE"))
        .DELETE
    else if (std.mem.eql(u8, method, "HEAD"))
        .HEAD
    else
        .GET;

    // Build extra headers.
    var headers_buf: [4]std.http.Header = undefined;
    var header_count: usize = 0;

    headers_buf[header_count] = .{ .name = "Authorization", .value = auth_header };
    header_count += 1;

    headers_buf[header_count] = .{ .name = "x-ms-version", .value = api_version };
    header_count += 1;

    if (content_type) |ct| {
        headers_buf[header_count] = .{ .name = "Content-Type", .value = ct };
        header_count += 1;
    }

    const extra_headers = headers_buf[0..header_count];

    // Use std.http.Client.fetch() -- same pattern as AWS/GCP backends.
    var response_body_list = std.ArrayList(u8).empty;
    defer response_body_list.deinit(allocator);

    var gw = response_body_list.writer(allocator);
    var adapter_buf: [8192]u8 = undefined;
    var adapter = gw.adaptToNewApi(&adapter_buf);

    const result = client.fetch(.{
        .location = .{ .url = url_str },
        .method = method_enum,
        .extra_headers = extra_headers,
        .payload = body,
        .response_writer = &adapter.new_interface,
    }) catch |err| {
        std.log.err("Azure request error: {} for {s} {s}", .{ err, method, url_str });
        return err;
    };

    const status: u16 = @intFromEnum(result.status);

    // Copy the response body to an owned slice.
    const response_body = try allocator.dupe(u8, response_body_list.items);

    return HttpResult{
        .status = status,
        .body = response_body,
        .content_length = response_body.len,
    };
}

/// Make an Azure PUT request with x-ms-blob-type header (for creating BlockBlob).
fn makeAzureRequestWithBlobType(
    allocator: std.mem.Allocator,
    client: *std.http.Client,
    host: []const u8,
    method: []const u8,
    path: []const u8,
    content_type: ?[]const u8,
    body: ?[]const u8,
    access_token: []const u8,
    blob_type: []const u8,
) !HttpResult {
    // Build the full URL.
    const url_str = try std.fmt.allocPrint(allocator, "https://{s}{s}", .{ host, path });
    defer allocator.free(url_str);

    // Build Authorization header.
    const auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{access_token});
    defer allocator.free(auth_header);

    // Azure API version header.
    const api_version = "2023-11-03";

    // Determine the HTTP method enum.
    const method_enum: std.http.Method = if (std.mem.eql(u8, method, "GET"))
        .GET
    else if (std.mem.eql(u8, method, "PUT"))
        .PUT
    else if (std.mem.eql(u8, method, "DELETE"))
        .DELETE
    else
        .GET;

    // Build extra headers.
    var headers_buf: [5]std.http.Header = undefined;
    var header_count: usize = 0;

    headers_buf[header_count] = .{ .name = "Authorization", .value = auth_header };
    header_count += 1;

    headers_buf[header_count] = .{ .name = "x-ms-version", .value = api_version };
    header_count += 1;

    headers_buf[header_count] = .{ .name = "x-ms-blob-type", .value = blob_type };
    header_count += 1;

    if (content_type) |ct| {
        headers_buf[header_count] = .{ .name = "Content-Type", .value = ct };
        header_count += 1;
    }

    const extra_headers = headers_buf[0..header_count];

    // Use std.http.Client.fetch().
    var response_body_list = std.ArrayList(u8).empty;
    defer response_body_list.deinit(allocator);

    var gw = response_body_list.writer(allocator);
    var adapter_buf: [8192]u8 = undefined;
    var adapter = gw.adaptToNewApi(&adapter_buf);

    const result = client.fetch(.{
        .location = .{ .url = url_str },
        .method = method_enum,
        .extra_headers = extra_headers,
        .payload = body,
        .response_writer = &adapter.new_interface,
    }) catch |err| {
        std.log.err("Azure request error: {} for {s} {s}", .{ err, method, url_str });
        return err;
    };

    const status: u16 = @intFromEnum(result.status);
    const response_body = try allocator.dupe(u8, response_body_list.items);

    return HttpResult{
        .status = status,
        .body = response_body,
        .content_length = response_body.len,
    };
}

/// Make an Azure PUT request with x-ms-copy-source header (for server-side copy).
fn makeAzureRequestWithCopySource(
    allocator: std.mem.Allocator,
    client: *std.http.Client,
    host: []const u8,
    path: []const u8,
    access_token: []const u8,
    copy_source: []const u8,
) !HttpResult {
    // Build the full URL.
    const url_str = try std.fmt.allocPrint(allocator, "https://{s}{s}", .{ host, path });
    defer allocator.free(url_str);

    // Build Authorization header.
    const auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{access_token});
    defer allocator.free(auth_header);

    // Azure API version header.
    const api_version = "2023-11-03";

    const extra_headers: []const std.http.Header = &.{
        .{ .name = "Authorization", .value = auth_header },
        .{ .name = "x-ms-version", .value = api_version },
        .{ .name = "x-ms-copy-source", .value = copy_source },
    };

    // Use std.http.Client.fetch().
    var response_body_list = std.ArrayList(u8).empty;
    defer response_body_list.deinit(allocator);

    var gw = response_body_list.writer(allocator);
    var adapter_buf: [8192]u8 = undefined;
    var adapter = gw.adaptToNewApi(&adapter_buf);

    const result = client.fetch(.{
        .location = .{ .url = url_str },
        .method = .PUT,
        .extra_headers = extra_headers,
        .payload = null,
        .response_writer = &adapter.new_interface,
    }) catch |err| {
        std.log.err("Azure copy request error: {} for PUT {s}", .{ err, url_str });
        return err;
    };

    const status: u16 = @intFromEnum(result.status);
    const response_body = try allocator.dupe(u8, response_body_list.items);

    return HttpResult{
        .status = status,
        .body = response_body,
        .content_length = response_body.len,
    };
}

// ===========================================================================
// Base64 encoding helper
// ===========================================================================

/// Base64 encode a byte slice, returning an allocator-owned string.
fn base64Encode(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    const encoder = std.base64.standard;
    const encoded_len = encoder.Encoder.calcSize(data.len);
    const buf = try allocator.alloc(u8, encoded_len);
    _ = encoder.Encoder.encode(buf, data);
    return buf;
}

/// Base64 decode a string, returning an allocator-owned byte slice.
fn base64Decode(allocator: std.mem.Allocator, encoded: []const u8) ![]u8 {
    const decoder = std.base64.standard;
    const decoded_len = decoder.Decoder.calcSizeForSlice(encoded) catch return error.InvalidBase64;
    const buf = try allocator.alloc(u8, decoded_len);
    decoder.Decoder.decode(buf, encoded) catch return error.InvalidBase64;
    return buf;
}

// ===========================================================================
// Tests
// ===========================================================================

test "AzureGatewayBackend: blobName mapping" {
    const allocator = std.testing.allocator;

    const prefix = "bleepstore/";
    const bucket_name = "my-bucket";
    const key = "my/object.txt";
    const result = try std.fmt.allocPrint(allocator, "{s}{s}/{s}", .{ prefix, bucket_name, key });
    defer allocator.free(result);

    try std.testing.expectEqualStrings("bleepstore/my-bucket/my/object.txt", result);
}

test "AzureGatewayBackend: blobName with empty prefix" {
    const allocator = std.testing.allocator;

    const prefix = "";
    const bucket_name = "test";
    const key = "data.bin";
    const result = try std.fmt.allocPrint(allocator, "{s}{s}/{s}", .{ prefix, bucket_name, key });
    defer allocator.free(result);

    try std.testing.expectEqualStrings("test/data.bin", result);
}

test "AzureGatewayBackend: blobPath mapping" {
    const allocator = std.testing.allocator;

    const container = "my-container";
    const blob_name_val = "bleepstore/my-bucket/file.txt";
    const result = try std.fmt.allocPrint(allocator, "/{s}/{s}", .{ container, blob_name_val });
    defer allocator.free(result);

    try std.testing.expectEqualStrings("/my-container/bleepstore/my-bucket/file.txt", result);
}

test "AzureGatewayBackend: blockId format" {
    const allocator = std.testing.allocator;

    const bid = try AzureGatewayBackend.blockId(allocator, "upload-abc-123", 5);
    defer allocator.free(bid);

    // The raw string should be "upload-abc-123:00005", base64-encoded.
    // Verify it's valid base64 by decoding.
    const decoded = try base64Decode(allocator, bid);
    defer allocator.free(decoded);

    try std.testing.expectEqualStrings("upload-abc-123:00005", decoded);
}

test "AzureGatewayBackend: blockId consistency" {
    const allocator = std.testing.allocator;

    // Same inputs should produce same output.
    const bid1 = try AzureGatewayBackend.blockId(allocator, "uid-1", 1);
    defer allocator.free(bid1);

    const bid2 = try AzureGatewayBackend.blockId(allocator, "uid-1", 1);
    defer allocator.free(bid2);

    try std.testing.expectEqualStrings(bid1, bid2);

    // Different part numbers should produce different block IDs.
    const bid3 = try AzureGatewayBackend.blockId(allocator, "uid-1", 2);
    defer allocator.free(bid3);

    try std.testing.expect(!std.mem.eql(u8, bid1, bid3));
}

test "AzureGatewayBackend: blockId with different upload IDs" {
    const allocator = std.testing.allocator;

    const bid1 = try AzureGatewayBackend.blockId(allocator, "upload-A", 1);
    defer allocator.free(bid1);

    const bid2 = try AzureGatewayBackend.blockId(allocator, "upload-B", 1);
    defer allocator.free(bid2);

    // Different upload IDs should produce different block IDs for the same part number.
    try std.testing.expect(!std.mem.eql(u8, bid1, bid2));
}

test "AzureGatewayBackend: base64Encode" {
    const allocator = std.testing.allocator;

    const result = try base64Encode(allocator, "Hello, World!");
    defer allocator.free(result);

    try std.testing.expectEqualStrings("SGVsbG8sIFdvcmxkIQ==", result);
}

test "AzureGatewayBackend: base64 round-trip" {
    const allocator = std.testing.allocator;

    const original = "test-upload-id:00042";
    const encoded = try base64Encode(allocator, original);
    defer allocator.free(encoded);

    const decoded = try base64Decode(allocator, encoded);
    defer allocator.free(decoded);

    try std.testing.expectEqualStrings(original, decoded);
}

test "AzureGatewayBackend: vtable is complete" {
    const vt = &AzureGatewayBackend.vtable_instance;
    try std.testing.expect(@intFromPtr(vt.putObject) != 0);
    try std.testing.expect(@intFromPtr(vt.getObject) != 0);
    try std.testing.expect(@intFromPtr(vt.deleteObject) != 0);
    try std.testing.expect(@intFromPtr(vt.headObject) != 0);
    try std.testing.expect(@intFromPtr(vt.copyObject) != 0);
    try std.testing.expect(@intFromPtr(vt.putPart) != 0);
    try std.testing.expect(@intFromPtr(vt.assembleParts) != 0);
    try std.testing.expect(@intFromPtr(vt.deleteParts) != 0);
    try std.testing.expect(@intFromPtr(vt.createBucket) != 0);
    try std.testing.expect(@intFromPtr(vt.deleteBucket) != 0);
    try std.testing.expect(@intFromPtr(vt.healthCheck) != 0);
}

test "AzureGatewayBackend: part blob path mapping" {
    const allocator = std.testing.allocator;

    const prefix = "bp/";
    const upload_id = "abc-123";
    const part_number: u32 = 7;
    const result = try std.fmt.allocPrint(allocator, "{s}.parts/{s}/{d}", .{ prefix, upload_id, part_number });
    defer allocator.free(result);

    try std.testing.expectEqualStrings("bp/.parts/abc-123/7", result);
}
