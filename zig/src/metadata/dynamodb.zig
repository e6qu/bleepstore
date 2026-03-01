const std = @import("std");
const meta_store = @import("store.zig");
const auth = @import("../auth.zig");
const MetadataStore = meta_store.MetadataStore;
const BucketMeta = meta_store.BucketMeta;
const ObjectMeta = meta_store.ObjectMeta;
const MultipartUploadMeta = meta_store.MultipartUploadMeta;
const PartMeta = meta_store.PartMeta;
const ListObjectsResult = meta_store.ListObjectsResult;
const ListUploadsResult = meta_store.ListUploadsResult;
const ListPartsResult = meta_store.ListPartsResult;
const Credential = meta_store.Credential;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const DynamoDBConfig = struct {
    table: []const u8,
    region: []const u8,
    endpoint_url: []const u8 = "",
    access_key_id: []const u8,
    secret_access_key: []const u8,
};

pub const DynamoDBMetadataStore = struct {
    allocator: std.mem.Allocator,
    config: DynamoDBConfig,
    mutex: std.Thread.Mutex,
    http_client: ?std.http.Client,
    signing_key_cache: std.StringHashMap([32]u8),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, config: DynamoDBConfig) !Self {
        std.log.info("DynamoDB metadata store initialized: table={s} region={s}", .{ config.table, config.region });
        return Self{
            .allocator = allocator,
            .config = config,
            .mutex = .{},
            .http_client = null,
            .signing_key_cache = std.StringHashMap([32]u8).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.http_client) |*client| {
            client.deinit();
        }
        var iter = self.signing_key_cache.iterator();
        while (iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.signing_key_cache.deinit();
    }

    fn getOrCreateHttpClient(self: *Self) !*std.http.Client {
        if (self.http_client == null) {
            self.http_client = .{ .allocator = self.allocator };
        }
        return &self.http_client.?;
    }

    fn nowIso(allocator: std.mem.Allocator) ![]const u8 {
        const ts = std.time.timestamp();
        const epoch: i64 = @intCast(ts);
        const days = @divTrunc(epoch, 86400);
        const secs_in_day = @mod(epoch, 86400);
        const hour = @divTrunc(secs_in_day, 3600);
        const minute = @divTrunc(@mod(secs_in_day, 3600), 60);
        const second = @mod(secs_in_day, 60);

        var year: i64 = 1970;
        var remaining_days = days;
        while (true) {
            const days_in_year: i64 = if (isLeapYear(@intCast(year))) 366 else 365;
            if (remaining_days < days_in_year) break;
            remaining_days -= days_in_year;
            year += 1;
        }

        const month_days = [_]i64{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
        var month: usize = 0;
        var days_this_month: i64 = 0;
        for (month_days, 0..) |dm, i| {
            days_this_month = dm;
            if (i == 1 and isLeapYear(@intCast(year))) days_this_month = 29;
            if (remaining_days < days_this_month) {
                month = i;
                break;
            }
            remaining_days -= days_this_month;
        }
        const day = remaining_days + 1;

        return std.fmt.allocPrint(allocator, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.000Z", .{
            year,
            month + 1,
            day,
            hour,
            minute,
            second,
        });
    }

    fn isLeapYear(year: i64) bool {
        return (@rem(year, 4) == 0 and @rem(year, 100) != 0) or (@rem(year, 400) == 0);
    }

    fn pkBucket(bucket: []const u8, buf: []u8) []const u8 {
        return std.fmt.bufPrint(buf, "BUCKET#{s}", .{bucket}) catch unreachable;
    }

    fn pkObject(bucket: []const u8, key: []const u8, buf: []u8) []const u8 {
        return std.fmt.bufPrint(buf, "OBJECT#{s}#{s}", .{ bucket, key }) catch unreachable;
    }

    fn pkUpload(upload_id: []const u8, buf: []u8) []const u8 {
        return std.fmt.bufPrint(buf, "UPLOAD#{s}", .{upload_id}) catch unreachable;
    }

    fn pkCredential(access_key: []const u8, buf: []u8) []const u8 {
        return std.fmt.bufPrint(buf, "CRED#{s}", .{access_key}) catch unreachable;
    }

    fn skMetadata() []const u8 {
        return "#METADATA";
    }

    fn skPart(part_number: u32, buf: []u8) []const u8 {
        return std.fmt.bufPrint(buf, "PART#{d:0>5}", .{part_number}) catch unreachable;
    }

    fn makeRequest(
        self: *Self,
        operation: []const u8,
        body: []const u8,
    ) ![]u8 {
        const client = try self.getOrCreateHttpClient();

        const endpoint = if (self.config.endpoint_url.len > 0)
            self.config.endpoint_url
        else
            try std.fmt.allocPrint(self.allocator, "https://dynamodb.{s}.amazonaws.com", .{self.config.region});
        defer if (self.config.endpoint_url.len == 0) self.allocator.free(@constCast(endpoint));

        const uri = try std.Uri.parse(endpoint);

        const now_ts = std.time.nanoTimestamp();
        const secs: i64 = @intCast(@divTrunc(now_ts, std.time.ns_per_s));
        var date_buf: [32]u8 = undefined;
        const amz_date = std.fmt.bufPrint(&date_buf, "{d:0>8}T000000Z", .{@divTrunc(secs, 86400) + 719528}) catch unreachable;
        const date_stamp = amz_date[0..8];

        const signing_key = blk: {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.signing_key_cache.get(date_stamp)) |key| {
                break :blk key;
            }
            const key = auth.deriveSigningKey(self.config.secret_access_key, date_stamp, self.config.region, "dynamodb");
            const owned = try self.allocator.dupe(u8, date_stamp);
            try self.signing_key_cache.put(owned, key);
            break :blk key;
        };

        const payload_hash = blk: {
            var hash: [32]u8 = undefined;
            Sha256.hash(body, &hash, .{});
            break :blk std.fmt.bytesToHex(hash, .lower);
        };

        const uri_path: []const u8 = switch (uri.path) {
            .raw => |r| r,
            .percent_encoded => |pe| pe,
        };
        const path_str = if (uri_path.len == 0) "/" else uri_path;
        const canonical_uri = try auth.buildCanonicalUri(self.allocator, path_str);
        defer self.allocator.free(canonical_uri);

        const uri_query: []const u8 = if (uri.query) |q| switch (q) {
            .raw => |r| r,
            .percent_encoded => |pe| pe,
        } else "";
        const canonical_query = try auth.buildCanonicalQueryString(self.allocator, uri_query);
        defer self.allocator.free(canonical_query);

        var headers_buf: [1024]u8 = undefined;
        const content_type = "application/x-amz-json-1.0";
        const host_str: []const u8 = if (uri.host) |h| switch (h) {
            .raw => |r| r,
            .percent_encoded => |pe| pe,
        } else "dynamodb.us-east-1.amazonaws.com";
        const host = if (host_str.len == 0) "dynamodb.us-east-1.amazonaws.com" else host_str;
        const canonical_headers = try std.fmt.bufPrint(&headers_buf, "content-type:{s}\nhost:{s}\nx-amz-content-sha256:{s}\nx-amz-date:{s}\nx-amz-target:DynamoDB_20120810.{s}\n", .{ content_type, host, &payload_hash, amz_date, operation });

        const signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date;x-amz-target";

        const canonical_request = try std.fmt.allocPrint(self.allocator, "POST\n{s}\n{s}\n{s}\n{s}\n{s}", .{ canonical_uri, canonical_query, canonical_headers, signed_headers, &payload_hash });
        defer self.allocator.free(canonical_request);

        const scope = try std.fmt.allocPrint(self.allocator, "{s}/{s}/dynamodb/aws4_request", .{ date_stamp, self.config.region });
        defer self.allocator.free(scope);

        const string_to_sign = try auth.computeStringToSign(self.allocator, amz_date, scope, canonical_request);
        defer self.allocator.free(string_to_sign);

        var sig_mac: [32]u8 = undefined;
        HmacSha256.create(&sig_mac, string_to_sign, &signing_key);
        const signature = std.fmt.bytesToHex(sig_mac, .lower);

        const auth_header = try std.fmt.allocPrint(self.allocator, "AWS4-HMAC-SHA256 Credential={s}/{s}/{s}/dynamodb/aws4_request, SignedHeaders={s}, Signature={s}", .{ self.config.access_key_id, date_stamp, self.config.region, signed_headers, signature });
        defer self.allocator.free(auth_header);

        const extra_headers = [_]std.http.Header{
            .{ .name = "x-amz-date", .value = amz_date },
            .{ .name = "x-amz-content-sha256", .value = &payload_hash },
            .{ .name = "x-amz-target", .value = try std.fmt.allocPrint(self.allocator, "DynamoDB_20120810.{s}", .{operation}) },
        };
        defer self.allocator.free(extra_headers[2].value);

        var req = try client.request(.POST, uri, .{
            .headers = .{
                .content_type = .{ .override = content_type },
                .authorization = .{ .override = auth_header },
            },
            .extra_headers = &extra_headers,
            .keep_alive = true,
        });
        defer req.deinit();

        req.transfer_encoding = .{ .content_length = body.len };
        var body_writer = try req.sendBodyUnflushed(&.{});
        try body_writer.writer.writeAll(body);
        try body_writer.end();
        try req.connection.?.flush();

        var redirect_buf: [8 * 1024]u8 = undefined;
        var response = try req.receiveHead(&redirect_buf);

        var transfer_buf: [64]u8 = undefined;
        var response_reader = response.reader(&transfer_buf);

        const response_body = try response_reader.allocRemaining(self.allocator, .unlimited);
        return response_body;
    }

    fn putItem(
        self: *Self,
        item: []const u8,
        condition: ?[]const u8,
    ) !void {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(self.allocator);

        if (condition) |c| {
            try std.fmt.format(body.writer(self.allocator), "{{\"TableName\":\"{s}\",\"Item\":{s},\"ConditionExpression\":\"{s}\"}}", .{ self.config.table, item, c });
        } else {
            try std.fmt.format(body.writer(self.allocator), "{{\"TableName\":\"{s}\",\"Item\":{s}}}", .{ self.config.table, item });
        }

        const response = try self.makeRequest("PutItem", body.items);
        defer self.allocator.free(response);
    }

    fn getItem(
        self: *Self,
        pk: []const u8,
        sk: []const u8,
        allocator: std.mem.Allocator,
    ) !?std.json.Value {
        const body = try std.fmt.allocPrint(self.allocator, "{{\"TableName\":\"{s}\",\"Key\":{{\"pk\":{{\"S\":\"{s}\"}},\"sk\":{{\"S\":\"{s}\"}}}}}}", .{ self.config.table, pk, sk });
        defer self.allocator.free(body);

        const response = try self.makeRequest("GetItem", body);
        defer self.allocator.free(response);

        var parsed = try std.json.parseFromSliceLeaky(std.json.Value, allocator, response, .{});

        if (parsed.object.get("Item")) |item| {
            return item;
        }
        return null;
    }

    fn deleteItem(
        self: *Self,
        pk: []const u8,
        sk: []const u8,
    ) !void {
        const body = try std.fmt.allocPrint(self.allocator, "{{\"TableName\":\"{s}\",\"Key\":{{\"pk\":{{\"S\":\"{s}\"}},\"sk\":{{\"S\":\"{s}\"}}}}}}", .{ self.config.table, pk, sk });
        defer self.allocator.free(body);

        const response = try self.makeRequest("DeleteItem", body);
        defer self.allocator.free(response);
    }

    fn updateItem(
        self: *Self,
        pk: []const u8,
        sk: []const u8,
        update_expr: []const u8,
        attr_values: []const u8,
    ) !void {
        const body = try std.fmt.allocPrint(self.allocator, "{{\"TableName\":\"{s}\",\"Key\":{{\"pk\":{{\"S\":\"{s}\"}},\"sk\":{{\"S\":\"{s}\"}}}},\"UpdateExpression\":\"{s}\",\"ExpressionAttributeValues\":{s}}}", .{ self.config.table, pk, sk, update_expr, attr_values });
        defer self.allocator.free(body);

        const response = try self.makeRequest("UpdateItem", body);
        defer self.allocator.free(response);
    }

    fn query(
        self: *Self,
        key_cond: []const u8,
        attr_values: []const u8,
        filter_expr: ?[]const u8,
        exclusive_start_key: ?[]const u8,
        limit: ?u32,
        allocator: std.mem.Allocator,
    ) !?std.json.Value {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(self.allocator);

        try std.fmt.format(body.writer(self.allocator), "{{\"TableName\":\"{s}\",\"KeyConditionExpression\":\"{s}\",\"ExpressionAttributeValues\":{s}", .{ self.config.table, key_cond, attr_values });

        if (filter_expr) |f| {
            try std.fmt.format(body.writer(self.allocator), ",\"FilterExpression\":\"{s}\"", .{f});
        }
        if (limit) |l| {
            try std.fmt.format(body.writer(self.allocator), ",\"Limit\":{d}", .{l});
        }
        if (exclusive_start_key) |esk| {
            try std.fmt.format(body.writer(self.allocator), ",\"ExclusiveStartKey\":{s}", .{esk});
        }
        try body.appendSlice(self.allocator, "}");

        const response = try self.makeRequest("Query", body.items);
        defer self.allocator.free(response);

        var parsed = try std.json.parseFromSliceLeaky(std.json.Value, allocator, response, .{});

        if (parsed.object.get("Count")) |count| {
            if (count.integer > 0) {
                return parsed;
            }
        }
        if (parsed.object.get("Items")) |items| {
            if (items.array.items.len > 0) {
                return parsed;
            }
        }
        return null;
    }

    fn scan(
        self: *Self,
        filter_expr: []const u8,
        attr_values: []const u8,
        attr_names: ?[]const u8,
        exclusive_start_key: ?[]const u8,
        limit: ?u32,
        select_count: bool,
        allocator: std.mem.Allocator,
    ) !?std.json.Value {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(self.allocator);

        try std.fmt.format(body.writer(self.allocator), "{{\"TableName\":\"{s}\",\"FilterExpression\":\"{s}\",\"ExpressionAttributeValues\":{s}", .{ self.config.table, filter_expr, attr_values });

        if (attr_names) |an| {
            try std.fmt.format(body.writer(self.allocator), ",\"ExpressionAttributeNames\":{s}", .{an});
        }
        if (limit) |l| {
            try std.fmt.format(body.writer(self.allocator), ",\"Limit\":{d}", .{l});
        }
        if (exclusive_start_key) |esk| {
            try std.fmt.format(body.writer(self.allocator), ",\"ExclusiveStartKey\":{s}", .{esk});
        }
        if (select_count) {
            try body.appendSlice(self.allocator, ",\"Select\":\"COUNT\"");
        }
        try body.appendSlice(self.allocator, "}");

        const response = try self.makeRequest("Scan", body.items);
        defer self.allocator.free(response);

        const parsed = try std.json.parseFromSliceLeaky(std.json.Value, allocator, response, .{});
        return parsed;
    }

    fn batchWriteItem(
        self: *Self,
        requests: []const u8,
    ) !void {
        const body = try std.fmt.allocPrint(self.allocator, "{{\"RequestItems\":{{\"{s}\":[{s}]}}}}", .{ self.config.table, requests });
        defer self.allocator.free(body);

        const response = try self.makeRequest("BatchWriteItem", body);
        defer self.allocator.free(response);
    }

    fn getStringFromItem(item: *const std.json.Value, key: []const u8) ?[]const u8 {
        if (item.object.get(key)) |val| {
            if (val.object.get("S")) |s| {
                return s.string;
            }
        }
        return null;
    }

    fn getNumberFromItem(item: *const std.json.Value, key: []const u8) ?i64 {
        if (item.object.get(key)) |val| {
            if (val.object.get("N")) |n| {
                return std.fmt.parseInt(i64, n.string, 10) catch null;
            }
        }
        return null;
    }

    fn getBoolFromItem(item: *const std.json.Value, key: []const u8) bool {
        if (item.object.get(key)) |val| {
            if (val.object.get("BOOL")) |b| {
                return b.bool;
            }
            if (val.object.get("S")) |s| {
                return std.mem.eql(u8, s.string, "true");
            }
        }
        return false;
    }

    fn createBucket(ctx: *anyopaque, meta: BucketMeta) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = try nowIso(self.allocator);
        defer self.allocator.free(now);

        var pk_buf: [512]u8 = undefined;
        const pk = pkBucket(meta.name, &pk_buf);

        const item = try std.fmt.allocPrint(self.allocator, "{{\"pk\":{{\"S\":\"{s}\"}},\"sk\":{{\"S\":\"#METADATA\"}},\"type\":{{\"S\":\"bucket\"}},\"name\":{{\"S\":\"{s}\"}},\"region\":{{\"S\":\"{s}\"}},\"owner_id\":{{\"S\":\"{s}\"}},\"owner_display\":{{\"S\":\"{s}\"}},\"acl\":{{\"S\":\"{s}\"}},\"created_at\":{{\"S\":\"{s}\"}}}}", .{ pk, meta.name, meta.region, meta.owner_id, meta.owner_display, meta.acl, now });
        defer self.allocator.free(item);

        try self.putItem(item, "attribute_not_exists(pk)");
    }

    fn deleteBucket(ctx: *anyopaque, name: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var pk_buf: [512]u8 = undefined;
        const pk = pkBucket(name, &pk_buf);
        try self.deleteItem(pk, "#METADATA");
    }

    fn getBucket(ctx: *anyopaque, name: []const u8) anyerror!?BucketMeta {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var pk_buf: [512]u8 = undefined;
        const pk = pkBucket(name, &pk_buf);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const item = try self.getItem(pk, "#METADATA", arena.allocator()) orelse return null;

        const name_val = getStringFromItem(&item, "name") orelse return error.JsonError;
        const created_at = getStringFromItem(&item, "created_at") orelse "";
        const region = getStringFromItem(&item, "region") orelse "us-east-1";
        const owner_id = getStringFromItem(&item, "owner_id") orelse "";
        const owner_display = getStringFromItem(&item, "owner_display") orelse "";
        const acl = getStringFromItem(&item, "acl") orelse "{}";

        return BucketMeta{
            .name = try self.allocator.dupe(u8, name_val),
            .creation_date = try self.allocator.dupe(u8, created_at),
            .region = try self.allocator.dupe(u8, region),
            .owner_id = try self.allocator.dupe(u8, owner_id),
            .owner_display = try self.allocator.dupe(u8, owner_display),
            .acl = try self.allocator.dupe(u8, acl),
        };
    }

    fn listBuckets(ctx: *anyopaque) anyerror![]BucketMeta {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var list: std.ArrayList(BucketMeta) = .empty;
        errdefer {
            for (list.items) |*item| item.deinit(self.allocator);
            list.deinit(self.allocator);
        }

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        var last_key: ?[]const u8 = null;
        while (true) {
            var esk: ?[]const u8 = null;
            if (last_key) |lk| {
                esk = lk;
            }

            const result = try self.scan(
                "begins_with(pk, :prefix) AND sk = :metadata",
                "{{\":prefix\":{{\"S\":\"BUCKET#\"}},\":metadata\":{{\"S\":\"#METADATA\"}}}}",
                null,
                esk,
                100,
                false,
                arena.allocator(),
            ) orelse break;

            if (result.object.get("Items")) |items| {
                for (items.array.items) |item| {
                    const name_val = getStringFromItem(&item, "name") orelse continue;
                    const created_at = getStringFromItem(&item, "created_at") orelse "";
                    const region = getStringFromItem(&item, "region") orelse "us-east-1";
                    const owner_id = getStringFromItem(&item, "owner_id") orelse "";
                    const owner_display = getStringFromItem(&item, "owner_display") orelse "";
                    const acl = getStringFromItem(&item, "acl") orelse "{}";

                    try list.append(self.allocator, .{
                        .name = try self.allocator.dupe(u8, name_val),
                        .creation_date = try self.allocator.dupe(u8, created_at),
                        .region = try self.allocator.dupe(u8, region),
                        .owner_id = try self.allocator.dupe(u8, owner_id),
                        .owner_display = try self.allocator.dupe(u8, owner_display),
                        .acl = try self.allocator.dupe(u8, acl),
                    });
                }
            }

            if (result.object.get("LastEvaluatedKey")) |lek| {
                last_key = try std.json.Stringify.valueAlloc(arena.allocator(), lek, .{});
            } else {
                break;
            }
        }

        return list.toOwnedSlice(self.allocator);
    }

    fn bucketExists(ctx: *anyopaque, name: []const u8) anyerror!bool {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var pk_buf: [512]u8 = undefined;
        const pk = pkBucket(name, &pk_buf);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const item = try self.getItem(pk, "#METADATA", arena.allocator());
        return item != null;
    }

    fn updateBucketAcl(ctx: *anyopaque, name: []const u8, acl: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var pk_buf: [512]u8 = undefined;
        const pk = pkBucket(name, &pk_buf);

        const attr_values = try std.fmt.allocPrint(self.allocator, "{{\":acl\":{{\"S\":\"{s}\"}}}}", .{acl});
        defer self.allocator.free(attr_values);

        try self.updateItem(pk, "#METADATA", "SET acl = :acl", attr_values);
    }

    fn putObjectMeta(ctx: *anyopaque, meta: ObjectMeta) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = try nowIso(self.allocator);
        defer self.allocator.free(now);

        var pk_buf: [1024]u8 = undefined;
        const pk = pkObject(meta.bucket, meta.key, &pk_buf);

        var item: std.ArrayList(u8) = .empty;
        defer item.deinit(self.allocator);

        try std.fmt.format(item.writer(self.allocator), "{{\"pk\":{{\"S\":\"{s}\"}},\"sk\":{{\"S\":\"#METADATA\"}},\"type\":{{\"S\":\"object\"}},\"bucket\":{{\"S\":\"{s}\"}},\"key\":{{\"S\":\"{s}\"}},\"size\":{{\"N\":\"{d}\"}},\"etag\":{{\"S\":\"{s}\"}},\"content_type\":{{\"S\":\"{s}\"}},\"storage_class\":{{\"S\":\"{s}\"}},\"acl\":{{\"S\":\"{s}\"}},\"user_metadata\":{{\"S\":\"{s}\"}},\"last_modified\":{{\"S\":\"{s}\"}}", .{ pk, meta.bucket, meta.key, meta.size, meta.etag, meta.content_type, meta.storage_class, meta.acl, meta.user_metadata orelse "{}", now });

        if (meta.content_encoding) |ce| {
            try std.fmt.format(item.writer(self.allocator), ",\"content_encoding\":{{\"S\":\"{s}\"}}", .{ce});
        }
        if (meta.content_language) |cl| {
            try std.fmt.format(item.writer(self.allocator), ",\"content_language\":{{\"S\":\"{s}\"}}", .{cl});
        }
        if (meta.content_disposition) |cd| {
            try std.fmt.format(item.writer(self.allocator), ",\"content_disposition\":{{\"S\":\"{s}\"}}", .{cd});
        }
        if (meta.cache_control) |cc| {
            try std.fmt.format(item.writer(self.allocator), ",\"cache_control\":{{\"S\":\"{s}\"}}", .{cc});
        }
        if (meta.expires) |e| {
            try std.fmt.format(item.writer(self.allocator), ",\"expires\":{{\"S\":\"{s}\"}}", .{e});
        }
        if (meta.delete_marker) {
            try item.appendSlice(self.allocator, ",\"delete_marker\":{\"BOOL\":true}");
        }
        try item.appendSlice(self.allocator, "}");

        try self.putItem(item.items, null);
    }

    fn getObjectMeta(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!?ObjectMeta {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var pk_buf: [1024]u8 = undefined;
        const pk = pkObject(bucket, key, &pk_buf);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const item = try self.getItem(pk, "#METADATA", arena.allocator()) orelse return null;

        const bucket_val = getStringFromItem(&item, "bucket") orelse return error.JsonError;
        const key_val = getStringFromItem(&item, "key") orelse return error.JsonError;
        const size = getNumberFromItem(&item, "size") orelse 0;
        const etag = getStringFromItem(&item, "etag") orelse "";
        const content_type = getStringFromItem(&item, "content_type") orelse "application/octet-stream";
        const last_modified = getStringFromItem(&item, "last_modified") orelse "";
        const storage_class = getStringFromItem(&item, "storage_class") orelse "STANDARD";
        const acl = getStringFromItem(&item, "acl") orelse "{}";

        return ObjectMeta{
            .bucket = try self.allocator.dupe(u8, bucket_val),
            .key = try self.allocator.dupe(u8, key_val),
            .size = @intCast(size),
            .etag = try self.allocator.dupe(u8, etag),
            .content_type = try self.allocator.dupe(u8, content_type),
            .last_modified = try self.allocator.dupe(u8, last_modified),
            .storage_class = try self.allocator.dupe(u8, storage_class),
            .user_metadata = if (getStringFromItem(&item, "user_metadata")) |um| try self.allocator.dupe(u8, um) else null,
            .content_encoding = if (getStringFromItem(&item, "content_encoding")) |ce| try self.allocator.dupe(u8, ce) else null,
            .content_language = if (getStringFromItem(&item, "content_language")) |cl| try self.allocator.dupe(u8, cl) else null,
            .content_disposition = if (getStringFromItem(&item, "content_disposition")) |cd| try self.allocator.dupe(u8, cd) else null,
            .cache_control = if (getStringFromItem(&item, "cache_control")) |cc| try self.allocator.dupe(u8, cc) else null,
            .expires = if (getStringFromItem(&item, "expires")) |e| try self.allocator.dupe(u8, e) else null,
            .acl = try self.allocator.dupe(u8, acl),
            .delete_marker = getBoolFromItem(&item, "delete_marker"),
        };
    }

    fn deleteObjectMeta(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!bool {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var pk_buf: [1024]u8 = undefined;
        const pk = pkObject(bucket, key, &pk_buf);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const exists = (try self.getItem(pk, "#METADATA", arena.allocator())) != null;
        try self.deleteItem(pk, "#METADATA");
        return exists;
    }

    fn deleteObjectsMeta(ctx: *anyopaque, bucket: []const u8, keys: []const []const u8) anyerror![]bool {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const results = try self.allocator.alloc(bool, keys.len);
        @memset(results, true);

        var pk_buf: [1024]u8 = undefined;
        var i: usize = 0;
        while (i < keys.len) : (i += 25) {
            const batch_end = @min(i + 25, keys.len);
            var requests: std.ArrayList(u8) = .empty;
            defer requests.deinit(self.allocator);

            for (keys[i..batch_end]) |k| {
                if (requests.items.len > 0) {
                    try requests.appendSlice(self.allocator, ",");
                }
                const pk = pkObject(bucket, k, &pk_buf);
                try std.fmt.format(requests.writer(self.allocator), "{{\"DeleteRequest\":{{\"Key\":{{\"pk\":{{\"S\":\"{s}\"}},\"sk\":{{\"S\":\"#METADATA\"}}}}}}}}", .{pk});
            }

            try self.batchWriteItem(requests.items);
        }

        return results;
    }

    fn listObjectsMeta(ctx: *anyopaque, bucket: []const u8, prefix: []const u8, delimiter: []const u8, start_after: []const u8, max_keys: u32) anyerror!ListObjectsResult {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        if (max_keys == 0) {
            return ListObjectsResult{
                .objects = &.{},
                .common_prefixes = &.{},
                .is_truncated = false,
            };
        }

        var objects_list: std.ArrayList(ObjectMeta) = .empty;
        var common_prefixes: std.ArrayList([]const u8) = .empty;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const prefix_filter = try std.fmt.allocPrint(arena.allocator(), "OBJECT#{s}#{s}", .{ bucket, prefix });
        var last_key: ?[]const u8 = null;

        while (objects_list.items.len <= max_keys) {
            var esk: ?[]const u8 = null;
            if (last_key) |lk| {
                esk = lk;
            }

            const result = try self.scan(
                "begins_with(pk, :prefix) AND sk = :metadata",
                try std.fmt.allocPrint(arena.allocator(), "{{\":prefix\":{{\"S\":\"{s}\"}},\":metadata\":{{\"S\":\"#METADATA\"}}}}", .{prefix_filter}),
                null,
                esk,
                max_keys + 1,
                false,
                arena.allocator(),
            ) orelse break;

            if (result.object.get("Items")) |items| {
                for (items.array.items) |item| {
                    const key_val = getStringFromItem(&item, "key") orelse continue;

                    if (std.mem.order(u8, key_val, start_after) != .gt) continue;

                    if (delimiter.len > 0) {
                        const after_prefix = key_val[prefix.len..];
                        if (std.mem.indexOf(u8, after_prefix, delimiter)) |idx| {
                            const cp = try std.fmt.allocPrint(self.allocator, "{s}{s}{s}", .{ prefix, after_prefix[0..idx], delimiter });
                            var found = false;
                            for (common_prefixes.items) |existing| {
                                if (std.mem.eql(u8, existing, cp)) {
                                    found = true;
                                    self.allocator.free(cp);
                                    break;
                                }
                            }
                            if (!found) {
                                try common_prefixes.append(self.allocator, cp);
                            }
                            continue;
                        }
                    }

                    if (objects_list.items.len >= max_keys) {
                        break;
                    }

                    const size = getNumberFromItem(&item, "size") orelse 0;
                    const etag = getStringFromItem(&item, "etag") orelse "";
                    const content_type = getStringFromItem(&item, "content_type") orelse "application/octet-stream";
                    const last_modified = getStringFromItem(&item, "last_modified") orelse "";
                    const storage_class = getStringFromItem(&item, "storage_class") orelse "STANDARD";
                    const acl = getStringFromItem(&item, "acl") orelse "{}";

                    try objects_list.append(self.allocator, .{
                        .bucket = try self.allocator.dupe(u8, bucket),
                        .key = try self.allocator.dupe(u8, key_val),
                        .size = @intCast(size),
                        .etag = try self.allocator.dupe(u8, etag),
                        .content_type = try self.allocator.dupe(u8, content_type),
                        .last_modified = try self.allocator.dupe(u8, last_modified),
                        .storage_class = try self.allocator.dupe(u8, storage_class),
                        .acl = try self.allocator.dupe(u8, acl),
                        .user_metadata = if (getStringFromItem(&item, "user_metadata")) |um| try self.allocator.dupe(u8, um) else null,
                    });
                }
            }

            if (result.object.get("LastEvaluatedKey")) |lek| {
                last_key = try std.json.Stringify.valueAlloc(arena.allocator(), lek, .{});
            } else {
                break;
            }

            if (objects_list.items.len > max_keys) {
                break;
            }
        }

        const is_truncated = objects_list.items.len > max_keys or common_prefixes.items.len > max_keys;
        if (is_truncated and objects_list.items.len > max_keys) {
            for (objects_list.items[max_keys..]) |*obj| {
                self.allocator.free(obj.bucket);
                self.allocator.free(obj.key);
                self.allocator.free(obj.etag);
                self.allocator.free(obj.content_type);
                self.allocator.free(obj.last_modified);
                self.allocator.free(obj.storage_class);
                self.allocator.free(obj.acl);
            }
            objects_list.shrinkRetainingCapacity(max_keys);
        }

        const next_token = if (is_truncated and objects_list.items.len > 0)
            try self.allocator.dupe(u8, objects_list.items[objects_list.items.len - 1].key)
        else
            null;

        return ListObjectsResult{
            .objects = try objects_list.toOwnedSlice(self.allocator),
            .common_prefixes = try common_prefixes.toOwnedSlice(self.allocator),
            .is_truncated = is_truncated,
            .next_continuation_token = next_token,
            .next_marker = next_token,
        };
    }

    fn objectExists(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!bool {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var pk_buf: [1024]u8 = undefined;
        const pk = pkObject(bucket, key, &pk_buf);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const item = try self.getItem(pk, "#METADATA", arena.allocator());
        return item != null;
    }

    fn updateObjectAcl(ctx: *anyopaque, bucket: []const u8, key: []const u8, acl: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var pk_buf: [1024]u8 = undefined;
        const pk = pkObject(bucket, key, &pk_buf);

        const attr_values = try std.fmt.allocPrint(self.allocator, "{{\":acl\":{{\"S\":\"{s}\"}}}}", .{acl});
        defer self.allocator.free(attr_values);

        try self.updateItem(pk, "#METADATA", "SET acl = :acl", attr_values);
    }

    fn createMultipartUpload(ctx: *anyopaque, meta: MultipartUploadMeta) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = try nowIso(self.allocator);
        defer self.allocator.free(now);

        var pk_buf: [512]u8 = undefined;
        const pk = pkUpload(meta.upload_id, &pk_buf);

        var item: std.ArrayList(u8) = .empty;
        defer item.deinit(self.allocator);

        try std.fmt.format(item.writer(self.allocator), "{{\"pk\":{{\"S\":\"{s}\"}},\"sk\":{{\"S\":\"#METADATA\"}},\"type\":{{\"S\":\"upload\"}},\"upload_id\":{{\"S\":\"{s}\"}},\"bucket\":{{\"S\":\"{s}\"}},\"key\":{{\"S\":\"{s}\"}},\"content_type\":{{\"S\":\"{s}\"}},\"storage_class\":{{\"S\":\"{s}\"}},\"acl\":{{\"S\":\"{s}\"}},\"user_metadata\":{{\"S\":\"{s}\"}},\"owner_id\":{{\"S\":\"{s}\"}},\"owner_display\":{{\"S\":\"{s}\"}},\"initiated_at\":{{\"S\":\"{s}\"}}", .{ pk, meta.upload_id, meta.bucket, meta.key, meta.content_type, meta.storage_class, meta.acl, meta.user_metadata, meta.owner_id, meta.owner_display, now });

        if (meta.content_encoding) |ce| {
            try std.fmt.format(item.writer(self.allocator), ",\"content_encoding\":{{\"S\":\"{s}\"}}", .{ce});
        }
        if (meta.content_language) |cl| {
            try std.fmt.format(item.writer(self.allocator), ",\"content_language\":{{\"S\":\"{s}\"}}", .{cl});
        }
        if (meta.content_disposition) |cd| {
            try std.fmt.format(item.writer(self.allocator), ",\"content_disposition\":{{\"S\":\"{s}\"}}", .{cd});
        }
        if (meta.cache_control) |cc| {
            try std.fmt.format(item.writer(self.allocator), ",\"cache_control\":{{\"S\":\"{s}\"}}", .{cc});
        }
        if (meta.expires) |e| {
            try std.fmt.format(item.writer(self.allocator), ",\"expires\":{{\"S\":\"{s}\"}}", .{e});
        }
        try item.appendSlice(self.allocator, "}");

        try self.putItem(item.items, null);
    }

    fn getMultipartUpload(ctx: *anyopaque, upload_id: []const u8) anyerror!?MultipartUploadMeta {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var pk_buf: [512]u8 = undefined;
        const pk = pkUpload(upload_id, &pk_buf);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const item = try self.getItem(pk, "#METADATA", arena.allocator()) orelse return null;

        const bucket_val = getStringFromItem(&item, "bucket") orelse return error.JsonError;
        const key_val = getStringFromItem(&item, "key") orelse return error.JsonError;
        const upload_id_val = getStringFromItem(&item, "upload_id") orelse return error.JsonError;
        const initiated = getStringFromItem(&item, "initiated_at") orelse "";
        const content_type = getStringFromItem(&item, "content_type") orelse "application/octet-stream";
        const storage_class = getStringFromItem(&item, "storage_class") orelse "STANDARD";
        const acl = getStringFromItem(&item, "acl") orelse "{}";
        const user_metadata = getStringFromItem(&item, "user_metadata") orelse "{}";
        const owner_id = getStringFromItem(&item, "owner_id") orelse "";
        const owner_display = getStringFromItem(&item, "owner_display") orelse "";

        return MultipartUploadMeta{
            .upload_id = try self.allocator.dupe(u8, upload_id_val),
            .bucket = try self.allocator.dupe(u8, bucket_val),
            .key = try self.allocator.dupe(u8, key_val),
            .initiated = try self.allocator.dupe(u8, initiated),
            .content_type = try self.allocator.dupe(u8, content_type),
            .storage_class = try self.allocator.dupe(u8, storage_class),
            .acl = try self.allocator.dupe(u8, acl),
            .user_metadata = try self.allocator.dupe(u8, user_metadata),
            .owner_id = try self.allocator.dupe(u8, owner_id),
            .owner_display = try self.allocator.dupe(u8, owner_display),
            .content_encoding = if (getStringFromItem(&item, "content_encoding")) |ce| try self.allocator.dupe(u8, ce) else null,
            .content_language = if (getStringFromItem(&item, "content_language")) |cl| try self.allocator.dupe(u8, cl) else null,
            .content_disposition = if (getStringFromItem(&item, "content_disposition")) |cd| try self.allocator.dupe(u8, cd) else null,
            .cache_control = if (getStringFromItem(&item, "cache_control")) |cc| try self.allocator.dupe(u8, cc) else null,
            .expires = if (getStringFromItem(&item, "expires")) |e| try self.allocator.dupe(u8, e) else null,
        };
    }

    fn abortMultipartUpload(ctx: *anyopaque, upload_id: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var pk_buf: [512]u8 = undefined;
        const pk = pkUpload(upload_id, &pk_buf);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        var parts: std.ArrayList(PartMeta) = .empty;
        defer {
            for (parts.items) |*p| {
                self.allocator.free(p.etag);
                self.allocator.free(p.last_modified);
            }
            parts.deinit(self.allocator);
        }

        var last_key: ?[]const u8 = null;
        while (true) {
            var esk: ?[]const u8 = null;
            if (last_key) |lk| {
                esk = lk;
            }

            const result = try self.query(
                "pk = :pk AND begins_with(sk, :part_prefix)",
                try std.fmt.allocPrint(arena.allocator(), "{{\":pk\":{{\"S\":\"{s}\"}},\":part_prefix\":{{\"S\":\"PART#\"}}}}", .{pk}),
                null,
                esk,
                null,
                arena.allocator(),
            ) orelse break;

            if (result.object.get("Items")) |items| {
                for (items.array.items) |item| {
                    const part_number = getNumberFromItem(&item, "part_number") orelse 0;
                    const size = getNumberFromItem(&item, "size") orelse 0;
                    const etag = getStringFromItem(&item, "etag") orelse "";
                    const last_modified = getStringFromItem(&item, "last_modified") orelse "";

                    try parts.append(self.allocator, .{
                        .part_number = @intCast(part_number),
                        .size = @intCast(size),
                        .etag = try self.allocator.dupe(u8, etag),
                        .last_modified = try self.allocator.dupe(u8, last_modified),
                    });
                }
            }

            if (result.object.get("LastEvaluatedKey")) |lek| {
                last_key = try std.json.Stringify.valueAlloc(arena.allocator(), lek, .{});
            } else {
                break;
            }
        }

        for (parts.items) |part| {
            var sk_buf: [32]u8 = undefined;
            const sk = skPart(part.part_number, &sk_buf);
            try self.deleteItem(pk, sk);
        }

        try self.deleteItem(pk, "#METADATA");
    }

    fn putPartMeta(ctx: *anyopaque, upload_id: []const u8, part: PartMeta) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = try nowIso(self.allocator);
        defer self.allocator.free(now);

        var pk_buf: [512]u8 = undefined;
        const pk = pkUpload(upload_id, &pk_buf);

        var sk_buf: [32]u8 = undefined;
        const sk = skPart(part.part_number, &sk_buf);

        const item = try std.fmt.allocPrint(self.allocator, "{{\"pk\":{{\"S\":\"{s}\"}},\"sk\":{{\"S\":\"{s}\"}},\"type\":{{\"S\":\"part\"}},\"upload_id\":{{\"S\":\"{s}\"}},\"part_number\":{{\"N\":\"{d}\"}},\"size\":{{\"N\":\"{d}\"}},\"etag\":{{\"S\":\"{s}\"}},\"last_modified\":{{\"S\":\"{s}\"}}}}", .{ pk, sk, upload_id, part.part_number, part.size, part.etag, now });
        defer self.allocator.free(item);

        try self.putItem(item, null);
    }

    fn listPartsMeta(ctx: *anyopaque, upload_id: []const u8, max_parts: u32, part_marker: u32) anyerror!ListPartsResult {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var pk_buf: [512]u8 = undefined;
        const pk = pkUpload(upload_id, &pk_buf);

        var parts_list: std.ArrayList(PartMeta) = .empty;
        errdefer {
            for (parts_list.items) |*p| {
                self.allocator.free(p.etag);
                self.allocator.free(p.last_modified);
            }
            parts_list.deinit(self.allocator);
        }

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        var start_sk_buf: [32]u8 = undefined;
        const start_sk = if (part_marker > 0) skPart(part_marker + 1, &start_sk_buf) else "PART#";

        var last_key: ?[]const u8 = null;
        while (parts_list.items.len <= max_parts) {
            var esk: ?[]const u8 = null;
            if (last_key) |lk| {
                esk = lk;
            }

            const result = try self.query(
                "pk = :pk AND sk >= :start_sk",
                try std.fmt.allocPrint(arena.allocator(), "{{\":pk\":{{\"S\":\"{s}\"}},\":start_sk\":{{\"S\":\"{s}\"}}}}", .{ pk, start_sk }),
                null,
                esk,
                max_parts + 1,
                arena.allocator(),
            ) orelse break;

            if (result.object.get("Items")) |items| {
                for (items.array.items) |item| {
                    const type_val = getStringFromItem(&item, "type") orelse continue;
                    if (!std.mem.eql(u8, type_val, "part")) continue;

                    if (parts_list.items.len > max_parts) break;

                    const part_number = getNumberFromItem(&item, "part_number") orelse 0;
                    const size = getNumberFromItem(&item, "size") orelse 0;
                    const etag = getStringFromItem(&item, "etag") orelse "";
                    const last_modified = getStringFromItem(&item, "last_modified") orelse "";

                    try parts_list.append(self.allocator, .{
                        .part_number = @intCast(part_number),
                        .size = @intCast(size),
                        .etag = try self.allocator.dupe(u8, etag),
                        .last_modified = try self.allocator.dupe(u8, last_modified),
                    });
                }
            }

            if (result.object.get("LastEvaluatedKey")) |lek| {
                last_key = try std.json.Stringify.valueAlloc(arena.allocator(), lek, .{});
            } else {
                break;
            }

            if (parts_list.items.len > max_parts) {
                break;
            }
        }

        const is_truncated = parts_list.items.len > max_parts;
        if (is_truncated) {
            for (parts_list.items[max_parts..]) |*p| {
                self.allocator.free(p.etag);
                self.allocator.free(p.last_modified);
            }
            parts_list.shrinkRetainingCapacity(max_parts);
        }

        const next_marker = if (is_truncated and parts_list.items.len > 0)
            parts_list.items[parts_list.items.len - 1].part_number
        else
            0;

        return ListPartsResult{
            .parts = try parts_list.toOwnedSlice(self.allocator),
            .is_truncated = is_truncated,
            .next_part_number_marker = next_marker,
        };
    }

    fn getPartsForCompletion(ctx: *anyopaque, upload_id: []const u8) anyerror![]PartMeta {
        const result = try listPartsMeta(ctx, upload_id, 10000, 0);
        return result.parts;
    }

    fn completeMultipartUpload(ctx: *anyopaque, upload_id: []const u8, object_meta: ObjectMeta) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        try putObjectMeta(ctx, object_meta);

        var pk_buf: [512]u8 = undefined;
        const pk = pkUpload(upload_id, &pk_buf);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        var parts: std.ArrayList(PartMeta) = .empty;
        defer {
            for (parts.items) |*p| {
                self.allocator.free(p.etag);
                self.allocator.free(p.last_modified);
            }
            parts.deinit(self.allocator);
        }

        var last_key: ?[]const u8 = null;
        while (true) {
            var esk: ?[]const u8 = null;
            if (last_key) |lk| {
                esk = lk;
            }

            const result = try self.query(
                "pk = :pk AND begins_with(sk, :part_prefix)",
                try std.fmt.allocPrint(arena.allocator(), "{{\":pk\":{{\"S\":\"{s}\"}},\":part_prefix\":{{\"S\":\"PART#\"}}}}", .{pk}),
                null,
                esk,
                null,
                arena.allocator(),
            ) orelse break;

            if (result.object.get("Items")) |items| {
                for (items.array.items) |item| {
                    const type_val = getStringFromItem(&item, "type") orelse continue;
                    if (!std.mem.eql(u8, type_val, "part")) continue;

                    const part_number = getNumberFromItem(&item, "part_number") orelse 0;
                    const size = getNumberFromItem(&item, "size") orelse 0;
                    const etag = getStringFromItem(&item, "etag") orelse "";
                    const last_modified = getStringFromItem(&item, "last_modified") orelse "";

                    try parts.append(self.allocator, .{
                        .part_number = @intCast(part_number),
                        .size = @intCast(size),
                        .etag = try self.allocator.dupe(u8, etag),
                        .last_modified = try self.allocator.dupe(u8, last_modified),
                    });
                }
            }

            if (result.object.get("LastEvaluatedKey")) |lek| {
                last_key = try std.json.Stringify.valueAlloc(arena.allocator(), lek, .{});
            } else {
                break;
            }
        }

        for (parts.items) |part| {
            var sk_buf: [32]u8 = undefined;
            const sk = skPart(part.part_number, &sk_buf);
            try self.deleteItem(pk, sk);
        }

        try self.deleteItem(pk, "#METADATA");
    }

    fn listMultipartUploads(ctx: *anyopaque, bucket: []const u8, prefix: []const u8, max_uploads: u32) anyerror!ListUploadsResult {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var uploads_list: std.ArrayList(MultipartUploadMeta) = .empty;
        errdefer {
            for (uploads_list.items) |*u| {
                self.allocator.free(u.upload_id);
                self.allocator.free(u.bucket);
                self.allocator.free(u.key);
                self.allocator.free(u.initiated);
                self.allocator.free(u.content_type);
                self.allocator.free(u.storage_class);
                self.allocator.free(u.acl);
                self.allocator.free(u.user_metadata);
            }
            uploads_list.deinit(self.allocator);
        }

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        var filter_expr: []const u8 = "begins_with(pk, :upload_prefix) AND sk = :metadata AND #bucket = :bucket";
        var attr_values: []const u8 = undefined;

        if (prefix.len > 0) {
            filter_expr = "begins_with(pk, :upload_prefix) AND sk = :metadata AND #bucket = :bucket AND begins_with(#key, :prefix)";
            attr_values = try std.fmt.allocPrint(arena.allocator(), "{{\":upload_prefix\":{{\"S\":\"UPLOAD#\"}},\":metadata\":{{\"S\":\"#METADATA\"}},\":bucket\":{{\"S\":\"{s}\"}},\":prefix\":{{\"S\":\"{s}\"}}}}", .{ bucket, prefix });
        } else {
            attr_values = try std.fmt.allocPrint(arena.allocator(), "{{\":upload_prefix\":{{\"S\":\"UPLOAD#\"}},\":metadata\":{{\"S\":\"#METADATA\"}},\":bucket\":{{\"S\":\"{s}\"}}}}", .{bucket});
        }

        const attr_names = try std.fmt.allocPrint(arena.allocator(), "{{\"#bucket\":\"bucket\",\"#key\":\"key\"}}", .{});

        var last_key: ?[]const u8 = null;
        while (uploads_list.items.len <= max_uploads) {
            var esk: ?[]const u8 = null;
            if (last_key) |lk| {
                esk = lk;
            }

            const result = try self.scan(
                filter_expr,
                attr_values,
                attr_names,
                esk,
                max_uploads + 1,
                false,
                arena.allocator(),
            ) orelse break;

            if (result.object.get("Items")) |items| {
                for (items.array.items) |item| {
                    const bucket_val = getStringFromItem(&item, "bucket") orelse continue;
                    const key_val = getStringFromItem(&item, "key") orelse continue;
                    const upload_id_val = getStringFromItem(&item, "upload_id") orelse continue;
                    const initiated = getStringFromItem(&item, "initiated_at") orelse "";
                    const content_type = getStringFromItem(&item, "content_type") orelse "application/octet-stream";
                    const storage_class = getStringFromItem(&item, "storage_class") orelse "STANDARD";
                    const acl = getStringFromItem(&item, "acl") orelse "{}";
                    const user_metadata = getStringFromItem(&item, "user_metadata") orelse "{}";

                    if (uploads_list.items.len > max_uploads) break;

                    try uploads_list.append(self.allocator, .{
                        .upload_id = try self.allocator.dupe(u8, upload_id_val),
                        .bucket = try self.allocator.dupe(u8, bucket_val),
                        .key = try self.allocator.dupe(u8, key_val),
                        .initiated = try self.allocator.dupe(u8, initiated),
                        .content_type = try self.allocator.dupe(u8, content_type),
                        .storage_class = try self.allocator.dupe(u8, storage_class),
                        .acl = try self.allocator.dupe(u8, acl),
                        .user_metadata = try self.allocator.dupe(u8, user_metadata),
                    });
                }
            }

            if (result.object.get("LastEvaluatedKey")) |lek| {
                last_key = try std.json.Stringify.valueAlloc(arena.allocator(), lek, .{});
            } else {
                break;
            }

            if (uploads_list.items.len > max_uploads) {
                break;
            }
        }

        std.mem.sort(MultipartUploadMeta, uploads_list.items, {}, struct {
            fn lessThan(_: void, a: MultipartUploadMeta, b: MultipartUploadMeta) bool {
                const key_cmp = std.mem.order(u8, a.key, b.key);
                if (key_cmp == .lt) return true;
                if (key_cmp == .gt) return false;
                return std.mem.order(u8, a.upload_id, b.upload_id) == .lt;
            }
        }.lessThan);

        const is_truncated = uploads_list.items.len > max_uploads;
        if (is_truncated) {
            for (uploads_list.items[max_uploads..]) |*u| {
                self.allocator.free(u.upload_id);
                self.allocator.free(u.bucket);
                self.allocator.free(u.key);
                self.allocator.free(u.initiated);
                self.allocator.free(u.content_type);
                self.allocator.free(u.storage_class);
                self.allocator.free(u.acl);
                self.allocator.free(u.user_metadata);
            }
            uploads_list.shrinkRetainingCapacity(max_uploads);
        }

        const next_key_marker = if (is_truncated and uploads_list.items.len > 0)
            try self.allocator.dupe(u8, uploads_list.items[uploads_list.items.len - 1].key)
        else
            null;

        const next_upload_id_marker = if (is_truncated and uploads_list.items.len > 0)
            try self.allocator.dupe(u8, uploads_list.items[uploads_list.items.len - 1].upload_id)
        else
            null;

        return ListUploadsResult{
            .uploads = try uploads_list.toOwnedSlice(self.allocator),
            .is_truncated = is_truncated,
            .next_key_marker = next_key_marker,
            .next_upload_id_marker = next_upload_id_marker,
        };
    }

    fn getCredential(ctx: *anyopaque, access_key_id: []const u8) anyerror!?Credential {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var pk_buf: [512]u8 = undefined;
        const pk = pkCredential(access_key_id, &pk_buf);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const item = try self.getItem(pk, "#METADATA", arena.allocator()) orelse return null;

        const active = getBoolFromItem(&item, "active");
        if (!active) return null;

        const access_key = getStringFromItem(&item, "access_key_id") orelse return error.JsonError;
        const secret_key = getStringFromItem(&item, "secret_key") orelse return error.JsonError;
        const owner_id = getStringFromItem(&item, "owner_id") orelse "";
        const display_name = getStringFromItem(&item, "display_name") orelse "";
        const created_at = getStringFromItem(&item, "created_at") orelse "";

        return Credential{
            .access_key_id = try self.allocator.dupe(u8, access_key),
            .secret_key = try self.allocator.dupe(u8, secret_key),
            .owner_id = try self.allocator.dupe(u8, owner_id),
            .display_name = try self.allocator.dupe(u8, display_name),
            .active = active,
            .created_at = try self.allocator.dupe(u8, created_at),
        };
    }

    fn putCredential(ctx: *anyopaque, cred: Credential) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = try nowIso(self.allocator);
        defer self.allocator.free(now);

        var pk_buf: [512]u8 = undefined;
        const pk = pkCredential(cred.access_key_id, &pk_buf);

        const item = try std.fmt.allocPrint(self.allocator, "{{\"pk\":{{\"S\":\"{s}\"}},\"sk\":{{\"S\":\"#METADATA\"}},\"type\":{{\"S\":\"credential\"}},\"access_key_id\":{{\"S\":\"{s}\"}},\"secret_key\":{{\"S\":\"{s}\"}},\"owner_id\":{{\"S\":\"{s}\"}},\"display_name\":{{\"S\":\"{s}\"}},\"active\":{{\"BOOL\":{s}}},\"created_at\":{{\"S\":\"{s}\"}}}}", .{ pk, cred.access_key_id, cred.secret_key, cred.owner_id, cred.display_name, if (cred.active) "true" else "false", now });
        defer self.allocator.free(item);

        try self.putItem(item, null);
    }

    fn countBuckets(ctx: *anyopaque) anyerror!u64 {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        var count: u64 = 0;
        var last_key: ?[]const u8 = null;

        while (true) {
            var esk: ?[]const u8 = null;
            if (last_key) |lk| {
                esk = lk;
            }

            const result = try self.scan(
                "begins_with(pk, :prefix) AND sk = :metadata",
                "{{\":prefix\":{{\"S\":\"BUCKET#\"}},\":metadata\":{{\"S\":\"#METADATA\"}}}}",
                null,
                esk,
                null,
                true,
                arena.allocator(),
            ) orelse break;

            if (result.object.get("Count")) |c| {
                count += @intCast(c.integer);
            }

            if (result.object.get("LastEvaluatedKey")) |lek| {
                last_key = try std.json.Stringify.valueAlloc(arena.allocator(), lek, .{});
            } else {
                break;
            }
        }

        return count;
    }

    fn countObjects(ctx: *anyopaque) anyerror!u64 {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        var count: u64 = 0;
        var last_key: ?[]const u8 = null;

        while (true) {
            var esk: ?[]const u8 = null;
            if (last_key) |lk| {
                esk = lk;
            }

            const result = try self.scan(
                "begins_with(pk, :prefix) AND sk = :metadata",
                "{{\":prefix\":{{\"S\":\"OBJECT#\"}},\":metadata\":{{\"S\":\"#METADATA\"}}}}",
                null,
                esk,
                null,
                true,
                arena.allocator(),
            ) orelse break;

            if (result.object.get("Count")) |c| {
                count += @intCast(c.integer);
            }

            if (result.object.get("LastEvaluatedKey")) |lek| {
                last_key = try std.json.Stringify.valueAlloc(arena.allocator(), lek, .{});
            } else {
                break;
            }
        }

        return count;
    }

    fn getSelf(ctx: *anyopaque) *Self {
        return @ptrCast(@alignCast(ctx));
    }

    const vtable = MetadataStore.VTable{
        .createBucket = createBucket,
        .deleteBucket = deleteBucket,
        .getBucket = getBucket,
        .listBuckets = listBuckets,
        .bucketExists = bucketExists,
        .updateBucketAcl = updateBucketAcl,
        .putObjectMeta = putObjectMeta,
        .getObjectMeta = getObjectMeta,
        .deleteObjectMeta = deleteObjectMeta,
        .deleteObjectsMeta = deleteObjectsMeta,
        .listObjectsMeta = listObjectsMeta,
        .objectExists = objectExists,
        .updateObjectAcl = updateObjectAcl,
        .createMultipartUpload = createMultipartUpload,
        .getMultipartUpload = getMultipartUpload,
        .abortMultipartUpload = abortMultipartUpload,
        .putPartMeta = putPartMeta,
        .listPartsMeta = listPartsMeta,
        .getPartsForCompletion = getPartsForCompletion,
        .completeMultipartUpload = completeMultipartUpload,
        .listMultipartUploads = listMultipartUploads,
        .getCredential = getCredential,
        .putCredential = putCredential,
        .countBuckets = countBuckets,
        .countObjects = countObjects,
    };

    pub fn metadataStore(self: *Self) MetadataStore {
        return .{ .ctx = @ptrCast(self), .vtable = &vtable };
    }
};

test "DynamoDBMetadataStore init/deinit" {
    const config = DynamoDBConfig{
        .table = "test-table",
        .region = "us-east-1",
        .access_key_id = "test",
        .secret_access_key = "test",
    };
    var ddb_store = try DynamoDBMetadataStore.init(std.testing.allocator, config);
    defer ddb_store.deinit();
    try std.testing.expectEqualStrings("test-table", ddb_store.config.table);
}
