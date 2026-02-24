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

/// GcpGatewayBackend proxies object storage operations to an upstream Google
/// Cloud Storage bucket via `std.http.Client` and the GCS JSON API.
///
/// All BleepStore buckets/objects are stored under a single upstream GCS bucket
/// with a key prefix to namespace them.
///
/// Key mapping:
///   Objects:  {prefix}{bleepstore_bucket}/{key}
///   Parts:    {prefix}.parts/{upload_id}/{part_number}
///
/// Authentication: Bearer token via GCS_ACCESS_TOKEN environment variable,
/// or from a metadata server / application default credentials file.
pub const GcpGatewayBackend = struct {
    allocator: std.mem.Allocator,
    bucket: []const u8,
    project: []const u8,
    prefix: []const u8,
    access_token: []const u8,
    http_client: std.http.Client,

    /// True if access_token was allocated by us (from env var) and must be freed.
    token_owned: bool,

    const Self = @This();

    /// GCS JSON API base host.
    const gcs_host = "storage.googleapis.com";

    /// GCS compose supports at most 32 source objects per call.
    const max_compose_sources: usize = 32;

    pub fn init(
        allocator: std.mem.Allocator,
        gcp_bucket: []const u8,
        project: []const u8,
        prefix: []const u8,
    ) !Self {
        // Resolve access token from environment.
        const token_result = std.process.getEnvVarOwned(allocator, "GCS_ACCESS_TOKEN") catch |err| blk: {
            // Try GOOGLE_ACCESS_TOKEN as alternative
            break :blk std.process.getEnvVarOwned(allocator, "GOOGLE_ACCESS_TOKEN") catch {
                std.log.err("GCP backend requires GCS_ACCESS_TOKEN or GOOGLE_ACCESS_TOKEN env var: {}", .{err});
                return error.InvalidConfiguration;
            };
        };

        var client = std.http.Client{ .allocator = allocator };

        // Verify the upstream bucket exists with a list call (maxResults=1).
        const verify_path = try std.fmt.allocPrint(allocator, "/storage/v1/b/{s}/o?maxResults=1", .{gcp_bucket});
        defer allocator.free(verify_path);

        const verify_result = makeGcsRequest(
            allocator,
            &client,
            "GET",
            verify_path,
            null,
            null,
            token_result,
        ) catch |err| {
            std.log.err("GCP gateway: failed to verify upstream bucket '{s}': {}", .{ gcp_bucket, err });
            client.deinit();
            allocator.free(token_result);
            return error.InvalidConfiguration;
        };
        defer allocator.free(verify_result.body);

        if (verify_result.status != 200) {
            std.log.err("GCP gateway: upstream bucket '{s}' returned status {d}", .{ gcp_bucket, verify_result.status });
            client.deinit();
            allocator.free(token_result);
            return error.InvalidConfiguration;
        }

        std.log.info("GCP gateway backend initialized: bucket={s} project={s} prefix='{s}'", .{
            gcp_bucket, project, prefix,
        });

        return Self{
            .allocator = allocator,
            .bucket = gcp_bucket,
            .project = project,
            .prefix = prefix,
            .access_token = token_result,
            .http_client = client,
            .token_owned = true,
        };
    }

    pub fn deinit(self: *Self) void {
        self.http_client.deinit();
        if (self.token_owned) {
            self.allocator.free(self.access_token);
        }
    }

    /// Map a BleepStore bucket/key to an upstream GCS object name.
    fn gcsName(self: *const Self, allocator: std.mem.Allocator, bucket_name: []const u8, key: []const u8) ![]u8 {
        return std.fmt.allocPrint(allocator, "{s}{s}/{s}", .{ self.prefix, bucket_name, key });
    }

    /// Map a multipart part to an upstream GCS object name.
    fn partName(self: *const Self, allocator: std.mem.Allocator, upload_id: []const u8, part_number: u32) ![]u8 {
        return std.fmt.allocPrint(allocator, "{s}.parts/{s}/{d}", .{ self.prefix, upload_id, part_number });
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

        // Build GCS object name and upload path.
        const obj_name = try self.gcsName(self.allocator, bucket_name, key);
        defer self.allocator.free(obj_name);

        const encoded_name = try gcsEncodeObjectName(self.allocator, obj_name);
        defer self.allocator.free(encoded_name);

        const path = try std.fmt.allocPrint(
            self.allocator,
            "/upload/storage/v1/b/{s}/o?uploadType=media&name={s}",
            .{ self.bucket, encoded_name },
        );
        defer self.allocator.free(path);

        const result = try makeGcsRequest(
            self.allocator,
            &self.http_client,
            "POST",
            path,
            "application/octet-stream",
            data,
            self.access_token,
        );
        defer self.allocator.free(result.body);

        if (result.status != 200) {
            std.log.err("GCS upload failed: status={d} body={s}", .{ result.status, result.body });
            return error.UpstreamError;
        }

        return PutObjectResult{ .etag = etag };
    }

    fn getObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8) anyerror!ObjectData {
        const self = getSelf(ctx);

        const obj_name = try self.gcsName(self.allocator, bucket_name, key);
        defer self.allocator.free(obj_name);

        const encoded_name = try gcsEncodeObjectName(self.allocator, obj_name);
        defer self.allocator.free(encoded_name);

        const path = try std.fmt.allocPrint(
            self.allocator,
            "/storage/v1/b/{s}/o/{s}?alt=media",
            .{ self.bucket, encoded_name },
        );
        defer self.allocator.free(path);

        const result = try makeGcsRequest(
            self.allocator,
            &self.http_client,
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
            std.log.err("GCS GET failed: status={d}", .{result.status});
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

        const obj_name = try self.gcsName(self.allocator, bucket_name, key);
        defer self.allocator.free(obj_name);

        const encoded_name = try gcsEncodeObjectName(self.allocator, obj_name);
        defer self.allocator.free(encoded_name);

        const path = try std.fmt.allocPrint(
            self.allocator,
            "/storage/v1/b/{s}/o/{s}",
            .{ self.bucket, encoded_name },
        );
        defer self.allocator.free(path);

        const result = try makeGcsRequest(
            self.allocator,
            &self.http_client,
            "DELETE",
            path,
            null,
            null,
            self.access_token,
        );
        defer self.allocator.free(result.body);

        // GCS returns 204 on successful delete, 404 if not found.
        // S3 delete is idempotent -- both are acceptable.
        if (result.status != 204 and result.status != 200 and result.status != 404) {
            std.log.err("GCS DELETE failed: status={d}", .{result.status});
            return error.UpstreamError;
        }
    }

    fn headObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8) anyerror!ObjectData {
        const self = getSelf(ctx);

        // GCS does not have a HEAD-specific endpoint for objects. We use the
        // metadata endpoint (GET without ?alt=media) to get object info without
        // downloading the body.
        const obj_name = try self.gcsName(self.allocator, bucket_name, key);
        defer self.allocator.free(obj_name);

        const encoded_name = try gcsEncodeObjectName(self.allocator, obj_name);
        defer self.allocator.free(encoded_name);

        const path = try std.fmt.allocPrint(
            self.allocator,
            "/storage/v1/b/{s}/o/{s}",
            .{ self.bucket, encoded_name },
        );
        defer self.allocator.free(path);

        const result = try makeGcsRequest(
            self.allocator,
            &self.http_client,
            "GET",
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
            std.log.err("GCS HEAD (metadata) failed: status={d}", .{result.status});
            return error.UpstreamError;
        }

        // Parse the JSON response to extract size.
        const content_length = parseGcsSize(result.body) orelse 0;

        return ObjectData{
            .body = null,
            .content_length = content_length,
            .content_type = "application/octet-stream",
            .etag = "",
            .last_modified = "",
        };
    }

    fn copyObject(ctx: *anyopaque, src_bucket: []const u8, src_key: []const u8, dst_bucket: []const u8, dst_key: []const u8) anyerror!PutObjectResult {
        const self = getSelf(ctx);

        const src_name = try self.gcsName(self.allocator, src_bucket, src_key);
        defer self.allocator.free(src_name);

        const dst_name = try self.gcsName(self.allocator, dst_bucket, dst_key);
        defer self.allocator.free(dst_name);

        const encoded_src = try gcsEncodeObjectName(self.allocator, src_name);
        defer self.allocator.free(encoded_src);

        const encoded_dst = try gcsEncodeObjectName(self.allocator, dst_name);
        defer self.allocator.free(encoded_dst);

        // Use the GCS rewrite API for server-side copy.
        const path = try std.fmt.allocPrint(
            self.allocator,
            "/storage/v1/b/{s}/o/{s}/rewriteTo/b/{s}/o/{s}",
            .{ self.bucket, encoded_src, self.bucket, encoded_dst },
        );
        defer self.allocator.free(path);

        const rewrite_result = try makeGcsRequest(
            self.allocator,
            &self.http_client,
            "POST",
            path,
            "application/json",
            "{}",
            self.access_token,
        );
        defer self.allocator.free(rewrite_result.body);

        if (rewrite_result.status == 404) {
            return error.NoSuchKey;
        }
        if (rewrite_result.status != 200) {
            std.log.err("GCS rewrite failed: status={d} body={s}", .{ rewrite_result.status, rewrite_result.body });
            return error.UpstreamError;
        }

        // Download the destination to compute MD5 for consistent ETag.
        const get_path = try std.fmt.allocPrint(
            self.allocator,
            "/storage/v1/b/{s}/o/{s}?alt=media",
            .{ self.bucket, encoded_dst },
        );
        defer self.allocator.free(get_path);

        const get_result = try makeGcsRequest(
            self.allocator,
            &self.http_client,
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
        const hex = std.fmt.bytesToHex(md5_hash, .lower);
        const etag = try std.fmt.allocPrint(self.allocator, "\"{s}\"", .{@as([]const u8, &hex)});

        return PutObjectResult{ .etag = etag };
    }

    fn putPart(ctx: *anyopaque, bucket_name: []const u8, upload_id: []const u8, part_number: u32, data: []const u8) anyerror!PutPartResult {
        const self = getSelf(ctx);
        _ = bucket_name;

        // Compute MD5 locally for consistent ETag.
        var md5_hash: [Md5.digest_length]u8 = undefined;
        Md5.hash(data, &md5_hash, .{});
        const hex = std.fmt.bytesToHex(md5_hash, .lower);
        const etag = try std.fmt.allocPrint(self.allocator, "\"{s}\"", .{@as([]const u8, &hex)});
        errdefer self.allocator.free(etag);

        // Store part as a temporary GCS object.
        const pn = try self.partName(self.allocator, upload_id, part_number);
        defer self.allocator.free(pn);

        const encoded_name = try gcsEncodeObjectName(self.allocator, pn);
        defer self.allocator.free(encoded_name);

        const path = try std.fmt.allocPrint(
            self.allocator,
            "/upload/storage/v1/b/{s}/o?uploadType=media&name={s}",
            .{ self.bucket, encoded_name },
        );
        defer self.allocator.free(path);

        const result = try makeGcsRequest(
            self.allocator,
            &self.http_client,
            "POST",
            path,
            "application/octet-stream",
            data,
            self.access_token,
        );
        defer self.allocator.free(result.body);

        if (result.status != 200) {
            std.log.err("GCS PUT part failed: status={d}", .{result.status});
            return error.UpstreamError;
        }

        return PutPartResult{ .etag = etag };
    }

    fn assembleParts(ctx: *anyopaque, bucket_name: []const u8, key: []const u8, upload_id: []const u8, parts: []const PartInfo) anyerror!AssemblePartsResult {
        const self = getSelf(ctx);

        // Build list of source object names for each part.
        var source_names = std.ArrayList([]u8).empty;
        defer {
            for (source_names.items) |name| self.allocator.free(name);
            source_names.deinit(self.allocator);
        }

        for (parts) |part| {
            const pn = try self.partName(self.allocator, upload_id, part.part_number);
            try source_names.append(self.allocator, pn);
        }

        // Build final object name.
        const final_name = try self.gcsName(self.allocator, bucket_name, key);
        defer self.allocator.free(final_name);

        if (source_names.items.len <= max_compose_sources) {
            // Simple case: single compose call.
            try self.gcsCompose(final_name, source_names.items);
        } else {
            // Chain compose in batches of 32.
            try self.chainCompose(source_names.items, final_name);
        }

        // Download the composed object to compute composite ETag.
        // Also compute the multipart composite ETag from individual part ETags.
        var md5_concat = std.ArrayList(u8).empty;
        defer md5_concat.deinit(self.allocator);

        var total_size: u64 = 0;

        for (parts) |part| {
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

        // Download the final object to get total size.
        const encoded_final = try gcsEncodeObjectName(self.allocator, final_name);
        defer self.allocator.free(encoded_final);

        const meta_path = try std.fmt.allocPrint(
            self.allocator,
            "/storage/v1/b/{s}/o/{s}",
            .{ self.bucket, encoded_final },
        );
        defer self.allocator.free(meta_path);

        const meta_result = try makeGcsRequest(
            self.allocator,
            &self.http_client,
            "GET",
            meta_path,
            null,
            null,
            self.access_token,
        );
        defer self.allocator.free(meta_result.body);

        if (meta_result.status == 200) {
            total_size = parseGcsSize(meta_result.body) orelse 0;
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

        // List objects under {prefix}.parts/{upload_id}/ and delete each one.
        const list_prefix = try std.fmt.allocPrint(self.allocator, "{s}.parts/{s}/", .{ self.prefix, upload_id });
        defer self.allocator.free(list_prefix);

        const encoded_prefix = try gcsEncodeObjectName(self.allocator, list_prefix);
        defer self.allocator.free(encoded_prefix);

        const list_path = try std.fmt.allocPrint(
            self.allocator,
            "/storage/v1/b/{s}/o?prefix={s}",
            .{ self.bucket, encoded_prefix },
        );
        defer self.allocator.free(list_path);

        const list_result = try makeGcsRequest(
            self.allocator,
            &self.http_client,
            "GET",
            list_path,
            null,
            null,
            self.access_token,
        );
        defer self.allocator.free(list_result.body);

        if (list_result.status != 200) {
            // Non-critical: parts may not exist.
            return;
        }

        // Parse the JSON response to extract object names from "items".
        var names = std.ArrayList([]const u8).empty;
        defer names.deinit(self.allocator);

        parseGcsListNames(list_result.body, &names, self.allocator) catch {
            // If parsing fails, fall back to deleting known part numbers.
            for (1..101) |pn| {
                const pk = self.partName(self.allocator, upload_id, @intCast(pn)) catch continue;
                defer self.allocator.free(pk);

                const enc = gcsEncodeObjectName(self.allocator, pk) catch continue;
                defer self.allocator.free(enc);

                const del_path = std.fmt.allocPrint(
                    self.allocator,
                    "/storage/v1/b/{s}/o/{s}",
                    .{ self.bucket, enc },
                ) catch continue;
                defer self.allocator.free(del_path);

                const del_result = makeGcsRequest(
                    self.allocator,
                    &self.http_client,
                    "DELETE",
                    del_path,
                    null,
                    null,
                    self.access_token,
                ) catch continue;
                self.allocator.free(del_result.body);
            }
            return;
        };
        defer {
            for (names.items) |n| self.allocator.free(n);
        }

        // Delete each listed object.
        for (names.items) |obj_name| {
            const enc = gcsEncodeObjectName(self.allocator, obj_name) catch continue;
            defer self.allocator.free(enc);

            const del_path = std.fmt.allocPrint(
                self.allocator,
                "/storage/v1/b/{s}/o/{s}",
                .{ self.bucket, enc },
            ) catch continue;
            defer self.allocator.free(del_path);

            const del_result = makeGcsRequest(
                self.allocator,
                &self.http_client,
                "DELETE",
                del_path,
                null,
                null,
                self.access_token,
            ) catch continue;
            self.allocator.free(del_result.body);
        }
    }

    fn createBucket(ctx: *anyopaque, bucket_name: []const u8) anyerror!void {
        // In gateway mode, "creating a bucket" is a logical operation.
        // All BleepStore buckets map to prefixes within the single upstream GCS bucket.
        // No upstream action needed -- metadata creation is done by the handler.
        _ = getSelf(ctx);
        _ = bucket_name;
    }

    fn deleteBucket(ctx: *anyopaque, bucket_name: []const u8) anyerror!void {
        // In gateway mode, "deleting a bucket" is a logical operation.
        // Upstream objects under this prefix remain (eventual cleanup is the
        // responsibility of the operator). Metadata deletion is done by the handler.
        _ = getSelf(ctx);
        _ = bucket_name;
    }

    // --- GCS compose helper ---

    /// Execute a GCS compose request to combine source objects into a destination object.
    fn gcsCompose(self: *Self, dest_name: []const u8, source_names: []const []u8) !void {
        const encoded_dest = try gcsEncodeObjectName(self.allocator, dest_name);
        defer self.allocator.free(encoded_dest);

        const path = try std.fmt.allocPrint(
            self.allocator,
            "/storage/v1/b/{s}/o/{s}/compose",
            .{ self.bucket, encoded_dest },
        );
        defer self.allocator.free(path);

        // Build JSON body: {"sourceObjects": [{"name": "..."}, ...]}
        var json_buf = std.ArrayList(u8).empty;
        defer json_buf.deinit(self.allocator);

        try json_buf.appendSlice(self.allocator, "{\"sourceObjects\":[");
        for (source_names, 0..) |name, i| {
            if (i > 0) try json_buf.append(self.allocator, ',');
            try json_buf.appendSlice(self.allocator, "{\"name\":\"");
            // Escape JSON string characters in the name.
            for (name) |c| {
                switch (c) {
                    '"' => try json_buf.appendSlice(self.allocator, "\\\""),
                    '\\' => try json_buf.appendSlice(self.allocator, "\\\\"),
                    '\n' => try json_buf.appendSlice(self.allocator, "\\n"),
                    '\r' => try json_buf.appendSlice(self.allocator, "\\r"),
                    '\t' => try json_buf.appendSlice(self.allocator, "\\t"),
                    else => try json_buf.append(self.allocator, c),
                }
            }
            try json_buf.appendSlice(self.allocator, "\"}");
        }
        try json_buf.appendSlice(self.allocator, "]}");

        const result = try makeGcsRequest(
            self.allocator,
            &self.http_client,
            "POST",
            path,
            "application/json",
            json_buf.items,
            self.access_token,
        );
        defer self.allocator.free(result.body);

        if (result.status != 200) {
            std.log.err("GCS compose failed: status={d} body={s}", .{ result.status, result.body });
            return error.UpstreamError;
        }
    }

    /// Chain GCS compose calls for >32 sources.
    /// Composes in batches of 32, using intermediate objects, then composes
    /// the intermediates until a single object remains.
    fn chainCompose(self: *Self, source_names: []const []u8, final_name: []const u8) !void {
        // Track intermediate objects for cleanup.
        var intermediates = std.ArrayList([]u8).empty;
        defer {
            // Clean up intermediate objects.
            for (intermediates.items) |name| {
                const enc = gcsEncodeObjectName(self.allocator, name) catch {
                    self.allocator.free(name);
                    continue;
                };
                defer self.allocator.free(enc);

                const del_path = std.fmt.allocPrint(
                    self.allocator,
                    "/storage/v1/b/{s}/o/{s}",
                    .{ self.bucket, enc },
                ) catch {
                    self.allocator.free(name);
                    continue;
                };
                defer self.allocator.free(del_path);

                const del_result = makeGcsRequest(
                    self.allocator,
                    &self.http_client,
                    "DELETE",
                    del_path,
                    null,
                    null,
                    self.access_token,
                ) catch {
                    self.allocator.free(name);
                    continue;
                };
                self.allocator.free(del_result.body);
                self.allocator.free(name);
            }
            intermediates.deinit(self.allocator);
        }

        // Copy source names into a mutable working list.
        var current_sources = std.ArrayList([]u8).empty;
        defer current_sources.deinit(self.allocator);

        for (source_names) |name| {
            const copy = try self.allocator.dupe(u8, name);
            try current_sources.append(self.allocator, copy);
        }

        var generation: u32 = 0;
        while (current_sources.items.len > max_compose_sources) {
            var next_sources = std.ArrayList([]u8).empty;

            var i: usize = 0;
            while (i < current_sources.items.len) {
                const end = @min(i + max_compose_sources, current_sources.items.len);
                const batch = current_sources.items[i..end];

                if (batch.len == 1) {
                    // Single source -- no compose needed, pass through.
                    const copy = try self.allocator.dupe(u8, batch[0]);
                    try next_sources.append(self.allocator, copy);
                } else {
                    const intermediate_name = try std.fmt.allocPrint(
                        self.allocator,
                        "{s}.__compose_tmp_{d}_{d}",
                        .{ final_name, generation, i },
                    );
                    errdefer self.allocator.free(intermediate_name);

                    try self.gcsCompose(intermediate_name, batch);

                    const copy = try self.allocator.dupe(u8, intermediate_name);
                    try next_sources.append(self.allocator, copy);
                    try intermediates.append(self.allocator, intermediate_name);
                }

                i = end;
            }

            // Free old current sources.
            for (current_sources.items) |name| self.allocator.free(name);
            current_sources.deinit(self.allocator);
            current_sources = next_sources;
            generation += 1;
        }

        // Final compose into destination.
        try self.gcsCompose(final_name, current_sources.items);

        // Free current sources.
        for (current_sources.items) |name| self.allocator.free(name);
        current_sources.clearAndFree(self.allocator);
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
// GCS HTTP request helper
// ===========================================================================

/// Result of an HTTP request to GCS.
const HttpResult = struct {
    status: u16,
    body: []u8,
};

/// Make an authenticated HTTP request to the GCS JSON API.
///
/// Uses Bearer token authentication. All requests go to
/// https://storage.googleapis.com/{path}.
fn makeGcsRequest(
    allocator: std.mem.Allocator,
    client: *std.http.Client,
    method: []const u8,
    path: []const u8,
    content_type: ?[]const u8,
    body: ?[]const u8,
    access_token: []const u8,
) !HttpResult {
    // Build the full URL.
    const url_str = try std.fmt.allocPrint(allocator, "https://{s}{s}", .{ GcpGatewayBackend.gcs_host, path });
    defer allocator.free(url_str);

    // Build Authorization header.
    const auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{access_token});
    defer allocator.free(auth_header);

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
    var headers_buf: [3]std.http.Header = undefined;
    var header_count: usize = 0;

    headers_buf[header_count] = .{ .name = "Authorization", .value = auth_header };
    header_count += 1;

    if (content_type) |ct| {
        headers_buf[header_count] = .{ .name = "Content-Type", .value = ct };
        header_count += 1;
    }

    const extra_headers = headers_buf[0..header_count];

    // Use std.http.Client.fetch() -- same pattern as AWS backend.
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
        std.log.err("GCS request error: {} for {s} {s}", .{ err, method, url_str });
        return err;
    };

    const status: u16 = @intFromEnum(result.status);

    // Copy the response body to an owned slice.
    const response_body = try allocator.dupe(u8, response_body_list.items);

    return HttpResult{
        .status = status,
        .body = response_body,
    };
}

// ===========================================================================
// GCS URL encoding helper
// ===========================================================================

/// Percent-encode a GCS object name for use in URL path segments.
/// Slashes and other special characters in object names must be encoded
/// so they appear as part of the object name, not as path separators.
fn gcsEncodeObjectName(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    var encoded = std.ArrayList(u8).empty;
    defer encoded.deinit(allocator);

    for (name) |c| {
        if (isUnreserved(c)) {
            try encoded.append(allocator, c);
        } else {
            // Percent-encode
            var hex_buf: [3]u8 = undefined;
            _ = std.fmt.bufPrint(&hex_buf, "%{X:0>2}", .{c}) catch unreachable;
            try encoded.appendSlice(allocator, &hex_buf);
        }
    }

    return try allocator.dupe(u8, encoded.items);
}

/// RFC 3986 unreserved characters: A-Z a-z 0-9 - . _ ~
fn isUnreserved(c: u8) bool {
    return switch (c) {
        'A'...'Z', 'a'...'z', '0'...'9', '-', '.', '_', '~' => true,
        else => false,
    };
}

// ===========================================================================
// GCS JSON response parsers
// ===========================================================================

/// Parse the "size" field from a GCS object metadata JSON response.
/// GCS returns size as a string (e.g., "size": "12345").
fn parseGcsSize(json_body: []const u8) ?u64 {
    // Simple string search for "size" field.
    const size_key = "\"size\"";
    const idx = std.mem.indexOf(u8, json_body, size_key) orelse return null;
    const after_key = json_body[idx + size_key.len ..];

    // Skip whitespace and colon.
    var pos: usize = 0;
    while (pos < after_key.len and (after_key[pos] == ' ' or after_key[pos] == ':' or after_key[pos] == '\t')) {
        pos += 1;
    }
    if (pos >= after_key.len) return null;

    // Expect a quoted string value.
    if (after_key[pos] == '"') {
        pos += 1;
        const start = pos;
        while (pos < after_key.len and after_key[pos] != '"') {
            pos += 1;
        }
        const size_str = after_key[start..pos];
        return std.fmt.parseInt(u64, size_str, 10) catch null;
    }

    // Try unquoted number.
    const start = pos;
    while (pos < after_key.len and after_key[pos] >= '0' and after_key[pos] <= '9') {
        pos += 1;
    }
    if (pos > start) {
        return std.fmt.parseInt(u64, after_key[start..pos], 10) catch null;
    }

    return null;
}

/// Parse object names from a GCS list objects JSON response.
/// Extracts "name" fields from the "items" array.
fn parseGcsListNames(json_body: []const u8, names: *std.ArrayList([]const u8), allocator: std.mem.Allocator) !void {
    // Find "items" array.
    const items_key = "\"items\"";
    const items_idx = std.mem.indexOf(u8, json_body, items_key) orelse return;
    var pos = items_idx + items_key.len;

    // Skip to array start '['.
    while (pos < json_body.len and json_body[pos] != '[') {
        pos += 1;
    }
    if (pos >= json_body.len) return;
    pos += 1; // skip '['

    // Find each "name" field within the array.
    const name_key = "\"name\"";
    while (pos < json_body.len) {
        // Check for end of array.
        if (json_body[pos] == ']') break;

        const name_idx = std.mem.indexOf(u8, json_body[pos..], name_key);
        if (name_idx == null) break;
        pos += name_idx.? + name_key.len;

        // Skip whitespace and colon.
        while (pos < json_body.len and (json_body[pos] == ' ' or json_body[pos] == ':' or json_body[pos] == '\t' or json_body[pos] == '\n' or json_body[pos] == '\r')) {
            pos += 1;
        }

        // Extract quoted string value.
        if (pos < json_body.len and json_body[pos] == '"') {
            pos += 1;
            const start = pos;
            while (pos < json_body.len and json_body[pos] != '"') {
                if (json_body[pos] == '\\' and pos + 1 < json_body.len) {
                    pos += 2; // Skip escaped character
                } else {
                    pos += 1;
                }
            }
            const name_value = json_body[start..pos];
            const owned = try allocator.dupe(u8, name_value);
            try names.append(allocator, owned);
            if (pos < json_body.len) pos += 1; // skip closing quote
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

test "GcpGatewayBackend: gcsName mapping" {
    const allocator = std.testing.allocator;

    const prefix = "bleepstore/";
    const bucket_name = "my-bucket";
    const key = "my/object.txt";
    const result = try std.fmt.allocPrint(allocator, "{s}{s}/{s}", .{ prefix, bucket_name, key });
    defer allocator.free(result);

    try std.testing.expectEqualStrings("bleepstore/my-bucket/my/object.txt", result);
}

test "GcpGatewayBackend: partName mapping" {
    const allocator = std.testing.allocator;

    const prefix = "bleepstore/";
    const upload_id = "abc-123";
    const part_number: u32 = 5;
    const result = try std.fmt.allocPrint(allocator, "{s}.parts/{s}/{d}", .{ prefix, upload_id, part_number });
    defer allocator.free(result);

    try std.testing.expectEqualStrings("bleepstore/.parts/abc-123/5", result);
}

test "GcpGatewayBackend: gcsName with empty prefix" {
    const allocator = std.testing.allocator;

    const prefix = "";
    const bucket_name = "test";
    const key = "data.bin";
    const result = try std.fmt.allocPrint(allocator, "{s}{s}/{s}", .{ prefix, bucket_name, key });
    defer allocator.free(result);

    try std.testing.expectEqualStrings("test/data.bin", result);
}

test "GcpGatewayBackend: gcsEncodeObjectName" {
    const allocator = std.testing.allocator;

    // Simple name (no special chars)
    {
        const result = try gcsEncodeObjectName(allocator, "simple-name.txt");
        defer allocator.free(result);
        try std.testing.expectEqualStrings("simple-name.txt", result);
    }

    // Name with slashes
    {
        const result = try gcsEncodeObjectName(allocator, "path/to/object.txt");
        defer allocator.free(result);
        try std.testing.expectEqualStrings("path%2Fto%2Fobject.txt", result);
    }

    // Name with spaces
    {
        const result = try gcsEncodeObjectName(allocator, "my file.txt");
        defer allocator.free(result);
        try std.testing.expectEqualStrings("my%20file.txt", result);
    }
}

test "GcpGatewayBackend: parseGcsSize" {
    // Quoted size string (GCS format)
    {
        const json = "{\"name\": \"test.txt\", \"size\": \"12345\", \"kind\": \"storage#object\"}";
        const size = parseGcsSize(json);
        try std.testing.expectEqual(@as(?u64, 12345), size);
    }

    // No size field
    {
        const json = "{\"name\": \"test.txt\"}";
        const size = parseGcsSize(json);
        try std.testing.expectEqual(@as(?u64, null), size);
    }

    // Zero size
    {
        const json = "{\"size\": \"0\"}";
        const size = parseGcsSize(json);
        try std.testing.expectEqual(@as(?u64, 0), size);
    }
}

test "GcpGatewayBackend: parseGcsListNames" {
    const allocator = std.testing.allocator;

    const json =
        \\{"kind": "storage#objects", "items": [
        \\  {"name": "prefix/.parts/upload1/1", "size": "100"},
        \\  {"name": "prefix/.parts/upload1/2", "size": "200"}
        \\]}
    ;

    var names = std.ArrayList([]const u8).empty;
    defer {
        for (names.items) |n| allocator.free(n);
        names.deinit(allocator);
    }

    try parseGcsListNames(json, &names, allocator);
    try std.testing.expectEqual(@as(usize, 2), names.items.len);
    try std.testing.expectEqualStrings("prefix/.parts/upload1/1", names.items[0]);
    try std.testing.expectEqualStrings("prefix/.parts/upload1/2", names.items[1]);
}

test "GcpGatewayBackend: parseGcsListNames empty" {
    const allocator = std.testing.allocator;

    const json = "{\"kind\": \"storage#objects\"}";

    var names = std.ArrayList([]const u8).empty;
    defer names.deinit(allocator);

    try parseGcsListNames(json, &names, allocator);
    try std.testing.expectEqual(@as(usize, 0), names.items.len);
}

test "GcpGatewayBackend: vtable is complete" {
    const vt = &GcpGatewayBackend.vtable_instance;
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
}

test "GcpGatewayBackend: isUnreserved" {
    try std.testing.expect(isUnreserved('A'));
    try std.testing.expect(isUnreserved('z'));
    try std.testing.expect(isUnreserved('0'));
    try std.testing.expect(isUnreserved('-'));
    try std.testing.expect(isUnreserved('.'));
    try std.testing.expect(isUnreserved('_'));
    try std.testing.expect(isUnreserved('~'));
    try std.testing.expect(!isUnreserved('/'));
    try std.testing.expect(!isUnreserved(' '));
    try std.testing.expect(!isUnreserved('%'));
    try std.testing.expect(!isUnreserved('+'));
}
