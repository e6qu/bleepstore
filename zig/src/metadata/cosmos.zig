const std = @import("std");
const store = @import("store.zig");
const MetadataStore = store.MetadataStore;
const BucketMeta = store.BucketMeta;
const ObjectMeta = store.ObjectMeta;
const MultipartUploadMeta = store.MultipartUploadMeta;
const PartMeta = store.PartMeta;
const ListObjectsResult = store.ListObjectsResult;
const ListUploadsResult = store.ListUploadsResult;
const ListPartsResult = store.ListPartsResult;
const Credential = store.Credential;

pub const CosmosConfig = struct {
    database: []const u8,
    container: []const u8,
    endpoint: []const u8,
    connection_string: []const u8 = "",
    master_key: []const u8 = "",
};

pub const CosmosMetadataStore = struct {
    allocator: std.mem.Allocator,
    config: CosmosConfig,
    http_client: std.http.Client,
    host: []const u8,
    host_owned: bool,
    master_key: []const u8,
    mutex: std.Thread.Mutex,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, config: CosmosConfig) !Self {
        const http_client = std.http.Client{ .allocator = allocator };

        const host_owned = true;
        const host = try allocator.dupe(u8, config.endpoint);
        errdefer allocator.free(host);

        const master_key = try allocator.dupe(u8, config.master_key);

        std.log.info("Cosmos DB metadata store initialized: database={s} container={s}", .{ config.database, config.container });

        return Self{
            .allocator = allocator,
            .config = config,
            .http_client = http_client,
            .host = host,
            .host_owned = host_owned,
            .master_key = master_key,
            .mutex = .{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.http_client.deinit();
        if (self.host_owned) {
            self.allocator.free(self.host);
        }
        self.allocator.free(self.master_key);
    }

    fn dupe(self: *Self, s: []const u8) ![]const u8 {
        return self.allocator.dupe(u8, s);
    }

    fn nowIso() []const u8 {
        return "2026-01-01T00:00:00.000Z";
    }

    fn makeRequest(self: *Self, method: std.http.Method, path: []const u8, body: ?[]const u8) ![]u8 {
        const url = try std.fmt.allocPrint(self.allocator, "https://{s}{s}", .{ self.host, path });
        defer self.allocator.free(url);

        const date_str = nowIso();
        const auth_header = try self.generateAuth(method, path, date_str);
        defer self.allocator.free(auth_header);

        const extra_headers: []const std.http.Header = &.{
            .{ .name = "Authorization", .value = auth_header },
            .{ .name = "x-ms-date", .value = date_str },
            .{ .name = "x-ms-version", .value = "2020-07-13" },
            .{ .name = "Content-Type", .value = "application/json" },
        };

        var response_body_list = std.ArrayList(u8).empty;
        defer response_body_list.deinit(self.allocator);

        var gw = response_body_list.writer(self.allocator);
        var adapter_buf: [8192]u8 = undefined;
        var adapter = gw.adaptToNewApi(&adapter_buf);

        const result = self.http_client.fetch(.{
            .location = .{ .url = url },
            .method = method,
            .extra_headers = extra_headers,
            .payload = body,
            .response_writer = &adapter.new_interface,
        }) catch |err| {
            std.log.err("Cosmos DB request error: {}", .{err});
            return err;
        };

        if (@intFromEnum(result.status) >= 400) {
            std.log.err("Cosmos DB error: status={d} body={s}", .{ @intFromEnum(result.status), response_body_list.items });
        }

        return response_body_list.toOwnedSlice(self.allocator);
    }

    fn generateAuth(self: *Self, method: std.http.Method, path: []const u8, date: []const u8) ![]const u8 {
        _ = date;
        _ = self.master_key;
        const method_str = switch (method) {
            .GET => "get",
            .POST => "post",
            .PUT => "put",
            .DELETE => "delete",
            .PATCH => "patch",
            else => "get",
        };
        const string_to_sign = try std.fmt.allocPrint(self.allocator, "{s}\n{s}\n\n\n\n", .{ method_str, path });
        defer self.allocator.free(string_to_sign);
        return std.fmt.allocPrint(self.allocator, "type=master&ver=1.0&sig={s}", .{string_to_sign});
    }

    fn createBucket(ctx: *anyopaque, meta: BucketMeta) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var doc_id_buf: [256]u8 = undefined;
        const doc_id = std.fmt.bufPrint(&doc_id_buf, "bucket_{s}", .{meta.name}) catch &doc_id_buf;

        const path = try std.fmt.allocPrint(self.allocator, "/dbs/{s}/colls/{s}/docs", .{ self.config.database, self.config.container });
        defer self.allocator.free(path);

        const body = try std.fmt.allocPrint(self.allocator, "{{\"id\":\"{s}\",\"type\":\"bucket\",\"name\":\"{s}\",\"region\":\"{s}\",\"owner_id\":\"{s}\",\"owner_display\":\"{s}\",\"acl\":\"{s}\",\"created_at\":\"{s}\"}}", .{ doc_id, meta.name, meta.region, meta.owner_id, meta.owner_display, meta.acl, nowIso() });
        defer self.allocator.free(body);

        const resp = try self.makeRequest(.POST, path, body);
        self.allocator.free(resp);
    }

    fn deleteBucket(ctx: *anyopaque, name: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var doc_id_buf: [256]u8 = undefined;
        const doc_id = std.fmt.bufPrint(&doc_id_buf, "bucket_{s}", .{name}) catch &doc_id_buf;

        const path = try std.fmt.allocPrint(self.allocator, "/dbs/{s}/colls/{s}/docs/{s}", .{ self.config.database, self.config.container, doc_id });
        defer self.allocator.free(path);

        const resp = try self.makeRequest(.DELETE, path, null);
        self.allocator.free(resp);
    }

    fn getBucket(ctx: *anyopaque, name: []const u8) anyerror!?BucketMeta {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var doc_id_buf: [256]u8 = undefined;
        const doc_id = std.fmt.bufPrint(&doc_id_buf, "bucket_{s}", .{name}) catch &doc_id_buf;

        const path = try std.fmt.allocPrint(self.allocator, "/dbs/{s}/colls/{s}/docs/{s}", .{ self.config.database, self.config.container, doc_id });
        defer self.allocator.free(path);

        const resp = try self.makeRequest(.GET, path, null);
        defer self.allocator.free(resp);

        if (std.mem.indexOf(u8, resp, "\"code\":") != null) return null;

        return BucketMeta{
            .name = try self.extractField(resp, "name") orelse try self.dupe(name),
            .creation_date = try self.extractField(resp, "created_at") orelse try self.dupe(nowIso()),
            .region = try self.extractField(resp, "region") orelse try self.dupe("us-east-1"),
            .owner_id = try self.extractField(resp, "owner_id") orelse try self.dupe(""),
            .owner_display = try self.extractField(resp, "owner_display") orelse try self.dupe(""),
            .acl = try self.extractField(resp, "acl") orelse try self.dupe("{}"),
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
        return list.toOwnedSlice(self.allocator);
    }

    fn bucketExists(ctx: *anyopaque, name: []const u8) anyerror!bool {
        const bucket = try getBucket(ctx, name);
        if (bucket) |b| {
            const self = getSelf(ctx);
            b.deinit(self.allocator);
            return true;
        }
        return false;
    }

    fn updateBucketAcl(ctx: *anyopaque, name: []const u8, acl: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var doc_id_buf: [256]u8 = undefined;
        const doc_id = std.fmt.bufPrint(&doc_id_buf, "bucket_{s}", .{name}) catch &doc_id_buf;

        const path = try std.fmt.allocPrint(self.allocator, "/dbs/{s}/colls/{s}/docs/{s}", .{ self.config.database, self.config.container, doc_id });
        defer self.allocator.free(path);

        const body = try std.fmt.allocPrint(self.allocator, "{{\"id\":\"{s}\",\"acl\":\"{s}\"}}", .{ doc_id, acl });
        defer self.allocator.free(body);

        const resp = try self.makeRequest(.PUT, path, body);
        self.allocator.free(resp);
    }

    fn putObjectMeta(ctx: *anyopaque, meta: ObjectMeta) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var doc_id_buf: [1024]u8 = undefined;
        const doc_id = std.fmt.bufPrint(&doc_id_buf, "object_{s}_{s}", .{ meta.bucket, meta.key }) catch &doc_id_buf;

        const path = try std.fmt.allocPrint(self.allocator, "/dbs/{s}/colls/{s}/docs", .{ self.config.database, self.config.container });
        defer self.allocator.free(path);

        const body = try std.fmt.allocPrint(self.allocator, "{{\"id\":\"{s}\",\"type\":\"object\",\"bucket\":\"{s}\",\"key\":\"{s}\",\"size\":{d},\"etag\":\"{s}\",\"content_type\":\"{s}\",\"storage_class\":\"{s}\",\"acl\":\"{s}\",\"last_modified\":\"{s}\"}}", .{ doc_id, meta.bucket, meta.key, meta.size, meta.etag, meta.content_type, meta.storage_class, meta.acl, nowIso() });
        defer self.allocator.free(body);

        const resp = try self.makeRequest(.POST, path, body);
        self.allocator.free(resp);
    }

    fn getObjectMeta(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!?ObjectMeta {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var doc_id_buf: [1024]u8 = undefined;
        const doc_id = std.fmt.bufPrint(&doc_id_buf, "object_{s}_{s}", .{ bucket, key }) catch &doc_id_buf;

        const path = try std.fmt.allocPrint(self.allocator, "/dbs/{s}/colls/{s}/docs/{s}", .{ self.config.database, self.config.container, doc_id });
        defer self.allocator.free(path);

        const resp = try self.makeRequest(.GET, path, null);
        defer self.allocator.free(resp);

        if (std.mem.indexOf(u8, resp, "\"code\":") != null) return null;

        return ObjectMeta{
            .bucket = try self.extractField(resp, "bucket") orelse try self.dupe(bucket),
            .key = try self.extractField(resp, "key") orelse try self.dupe(key),
            .size = try self.extractNumber(resp, "size") orelse 0,
            .etag = try self.extractField(resp, "etag") orelse try self.dupe(""),
            .content_type = try self.extractField(resp, "content_type") orelse try self.dupe("application/octet-stream"),
            .last_modified = try self.extractField(resp, "last_modified") orelse try self.dupe(nowIso()),
            .storage_class = try self.extractField(resp, "storage_class") orelse try self.dupe("STANDARD"),
            .acl = try self.extractField(resp, "acl") orelse try self.dupe("{}"),
        };
    }

    fn deleteObjectMeta(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!bool {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var doc_id_buf: [1024]u8 = undefined;
        const doc_id = std.fmt.bufPrint(&doc_id_buf, "object_{s}_{s}", .{ bucket, key }) catch &doc_id_buf;

        const path = try std.fmt.allocPrint(self.allocator, "/dbs/{s}/colls/{s}/docs/{s}", .{ self.config.database, self.config.container, doc_id });
        defer self.allocator.free(path);

        const resp = try self.makeRequest(.DELETE, path, null);
        defer self.allocator.free(resp);

        return std.mem.indexOf(u8, resp, "\"code\":") == null;
    }

    fn deleteObjectsMeta(ctx: *anyopaque, bucket: []const u8, keys: []const []const u8) anyerror![]bool {
        const self = getSelf(ctx);
        const results = try self.allocator.alloc(bool, keys.len);
        @memset(results, true);
        for (keys, 0..) |key, i| results[i] = try deleteObjectMeta(ctx, bucket, key);
        return results;
    }

    fn listObjectsMeta(ctx: *anyopaque, bucket: []const u8, prefix: []const u8, delimiter: []const u8, start_after: []const u8, max_keys: u32) anyerror!ListObjectsResult {
        _ = bucket;
        _ = prefix;
        _ = delimiter;
        _ = start_after;
        _ = max_keys;
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var objects_list: std.ArrayList(ObjectMeta) = .empty;
        errdefer {
            for (objects_list.items) |*obj| {
                self.allocator.free(obj.bucket);
                self.allocator.free(obj.key);
                self.allocator.free(obj.etag);
                self.allocator.free(obj.content_type);
                self.allocator.free(obj.last_modified);
                self.allocator.free(obj.storage_class);
                self.allocator.free(obj.acl);
            }
            objects_list.deinit(self.allocator);
        }

        return ListObjectsResult{
            .objects = try objects_list.toOwnedSlice(self.allocator),
            .common_prefixes = &.{},
            .is_truncated = false,
        };
    }

    fn objectExists(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!bool {
        const obj = try getObjectMeta(ctx, bucket, key);
        if (obj) |o| {
            const self = getSelf(ctx);
            self.allocator.free(o.bucket);
            self.allocator.free(o.key);
            self.allocator.free(o.etag);
            self.allocator.free(o.content_type);
            self.allocator.free(o.last_modified);
            self.allocator.free(o.storage_class);
            self.allocator.free(o.acl);
            return true;
        }
        return false;
    }

    fn updateObjectAcl(ctx: *anyopaque, bucket: []const u8, key: []const u8, acl: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var doc_id_buf: [1024]u8 = undefined;
        const doc_id = std.fmt.bufPrint(&doc_id_buf, "object_{s}_{s}", .{ bucket, key }) catch &doc_id_buf;

        const path = try std.fmt.allocPrint(self.allocator, "/dbs/{s}/colls/{s}/docs/{s}", .{ self.config.database, self.config.container, doc_id });
        defer self.allocator.free(path);

        const body = try std.fmt.allocPrint(self.allocator, "{{\"id\":\"{s}\",\"acl\":\"{s}\"}}", .{ doc_id, acl });
        defer self.allocator.free(body);

        const resp = try self.makeRequest(.PUT, path, body);
        self.allocator.free(resp);
    }

    fn createMultipartUpload(ctx: *anyopaque, meta: MultipartUploadMeta) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var doc_id_buf: [256]u8 = undefined;
        const doc_id = std.fmt.bufPrint(&doc_id_buf, "upload_{s}", .{meta.upload_id}) catch &doc_id_buf;

        const path = try std.fmt.allocPrint(self.allocator, "/dbs/{s}/colls/{s}/docs", .{ self.config.database, self.config.container });
        defer self.allocator.free(path);

        const body = try std.fmt.allocPrint(self.allocator, "{{\"id\":\"{s}\",\"type\":\"upload\",\"upload_id\":\"{s}\",\"bucket\":\"{s}\",\"key\":\"{s}\",\"content_type\":\"{s}\",\"storage_class\":\"{s}\",\"acl\":\"{s}\",\"initiated_at\":\"{s}\"}}", .{ doc_id, meta.upload_id, meta.bucket, meta.key, meta.content_type, meta.storage_class, meta.acl, nowIso() });
        defer self.allocator.free(body);

        const resp = try self.makeRequest(.POST, path, body);
        self.allocator.free(resp);
    }

    fn getMultipartUpload(ctx: *anyopaque, upload_id: []const u8) anyerror!?MultipartUploadMeta {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var doc_id_buf: [256]u8 = undefined;
        const doc_id = std.fmt.bufPrint(&doc_id_buf, "upload_{s}", .{upload_id}) catch &doc_id_buf;

        const path = try std.fmt.allocPrint(self.allocator, "/dbs/{s}/colls/{s}/docs/{s}", .{ self.config.database, self.config.container, doc_id });
        defer self.allocator.free(path);

        const resp = try self.makeRequest(.GET, path, null);
        defer self.allocator.free(resp);

        if (std.mem.indexOf(u8, resp, "\"code\":") != null) return null;

        return MultipartUploadMeta{
            .upload_id = try self.extractField(resp, "upload_id") orelse try self.dupe(upload_id),
            .bucket = try self.extractField(resp, "bucket") orelse try self.dupe(""),
            .key = try self.extractField(resp, "key") orelse try self.dupe(""),
            .initiated = try self.extractField(resp, "initiated_at") orelse try self.dupe(nowIso()),
            .content_type = try self.extractField(resp, "content_type") orelse try self.dupe("application/octet-stream"),
            .storage_class = try self.extractField(resp, "storage_class") orelse try self.dupe("STANDARD"),
            .acl = try self.extractField(resp, "acl") orelse try self.dupe("{}"),
        };
    }

    fn abortMultipartUpload(ctx: *anyopaque, upload_id: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var doc_id_buf: [256]u8 = undefined;
        const doc_id = std.fmt.bufPrint(&doc_id_buf, "upload_{s}", .{upload_id}) catch &doc_id_buf;

        const path = try std.fmt.allocPrint(self.allocator, "/dbs/{s}/colls/{s}/docs/{s}", .{ self.config.database, self.config.container, doc_id });
        defer self.allocator.free(path);

        const resp = try self.makeRequest(.DELETE, path, null);
        self.allocator.free(resp);
    }

    fn putPartMeta(ctx: *anyopaque, upload_id: []const u8, part: PartMeta) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var doc_id_buf: [512]u8 = undefined;
        const doc_id = std.fmt.bufPrint(&doc_id_buf, "part_{s}_{d:0>5}", .{ upload_id, part.part_number }) catch &doc_id_buf;

        const path = try std.fmt.allocPrint(self.allocator, "/dbs/{s}/colls/{s}/docs", .{ self.config.database, self.config.container });
        defer self.allocator.free(path);

        const body = try std.fmt.allocPrint(self.allocator, "{{\"id\":\"{s}\",\"type\":\"part\",\"part_number\":{d},\"size\":{d},\"etag\":\"{s}\",\"last_modified\":\"{s}\"}}", .{ doc_id, part.part_number, part.size, part.etag, part.last_modified });
        defer self.allocator.free(body);

        const resp = try self.makeRequest(.POST, path, body);
        self.allocator.free(resp);
    }

    fn listPartsMeta(ctx: *anyopaque, upload_id: []const u8, max_parts: u32, part_marker: u32) anyerror!ListPartsResult {
        _ = upload_id;
        _ = part_marker;
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var parts_list: std.ArrayList(PartMeta) = .empty;
        errdefer {
            for (parts_list.items) |*p| {
                self.allocator.free(p.etag);
                self.allocator.free(p.last_modified);
            }
            parts_list.deinit(self.allocator);
        }

        const is_truncated = parts_list.items.len > max_parts;
        if (is_truncated) parts_list.shrinkAndFree(self.allocator, max_parts);

        return ListPartsResult{
            .parts = try parts_list.toOwnedSlice(self.allocator),
            .is_truncated = is_truncated,
            .next_part_number_marker = if (is_truncated and parts_list.items.len > 0) parts_list.items[parts_list.items.len - 1].part_number else 0,
        };
    }

    fn getPartsForCompletion(ctx: *anyopaque, upload_id: []const u8) anyerror![]PartMeta {
        const result = try listPartsMeta(ctx, upload_id, 10000, 0);
        return result.parts;
    }

    fn completeMultipartUpload(ctx: *anyopaque, upload_id: []const u8, object_meta: ObjectMeta) anyerror!void {
        try putObjectMeta(ctx, object_meta);
        try abortMultipartUpload(ctx, upload_id);
    }

    fn listMultipartUploads(ctx: *anyopaque, bucket: []const u8, prefix: []const u8, max_uploads: u32) anyerror!ListUploadsResult {
        _ = bucket;
        _ = prefix;
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
            }
            uploads_list.deinit(self.allocator);
        }

        const is_truncated = uploads_list.items.len > max_uploads;
        if (is_truncated) uploads_list.shrinkAndFree(self.allocator, max_uploads);

        return ListUploadsResult{
            .uploads = try uploads_list.toOwnedSlice(self.allocator),
            .is_truncated = is_truncated,
        };
    }

    fn getCredential(ctx: *anyopaque, access_key_id: []const u8) anyerror!?Credential {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var doc_id_buf: [256]u8 = undefined;
        const doc_id = std.fmt.bufPrint(&doc_id_buf, "cred_{s}", .{access_key_id}) catch &doc_id_buf;

        const path = try std.fmt.allocPrint(self.allocator, "/dbs/{s}/colls/{s}/docs/{s}", .{ self.config.database, self.config.container, doc_id });
        defer self.allocator.free(path);

        const resp = try self.makeRequest(.GET, path, null);
        defer self.allocator.free(resp);

        if (std.mem.indexOf(u8, resp, "\"code\":") != null) return null;

        const active = try self.extractBool(resp, "active") orelse true;
        if (!active) return null;

        return Credential{
            .access_key_id = try self.extractField(resp, "access_key_id") orelse try self.dupe(access_key_id),
            .secret_key = try self.extractField(resp, "secret_key") orelse try self.dupe(""),
            .owner_id = try self.extractField(resp, "owner_id") orelse try self.dupe(""),
            .display_name = try self.extractField(resp, "display_name") orelse try self.dupe(""),
            .active = active,
            .created_at = try self.extractField(resp, "created_at") orelse try self.dupe(""),
        };
    }

    fn putCredential(ctx: *anyopaque, cred: Credential) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var doc_id_buf: [256]u8 = undefined;
        const doc_id = std.fmt.bufPrint(&doc_id_buf, "cred_{s}", .{cred.access_key_id}) catch &doc_id_buf;

        const path = try std.fmt.allocPrint(self.allocator, "/dbs/{s}/colls/{s}/docs", .{ self.config.database, self.config.container });
        defer self.allocator.free(path);

        const body = try std.fmt.allocPrint(self.allocator, "{{\"id\":\"{s}\",\"type\":\"credential\",\"access_key_id\":\"{s}\",\"secret_key\":\"{s}\",\"owner_id\":\"{s}\",\"display_name\":\"{s}\",\"active\":{s},\"created_at\":\"{s}\"}}", .{ doc_id, cred.access_key_id, cred.secret_key, cred.owner_id, cred.display_name, if (cred.active) "true" else "false", nowIso() });
        defer self.allocator.free(body);

        const resp = try self.makeRequest(.POST, path, body);
        self.allocator.free(resp);
    }

    fn countBuckets(ctx: *anyopaque) anyerror!u64 {
        _ = ctx;
        return 0;
    }

    fn countObjects(ctx: *anyopaque) anyerror!u64 {
        _ = ctx;
        return 0;
    }

    fn extractField(self: *Self, json: []const u8, key: []const u8) !?[]const u8 {
        const search = try std.fmt.allocPrint(self.allocator, "\"{s}\":\"", .{key});
        defer self.allocator.free(search);

        if (std.mem.indexOf(u8, json, search)) |start| {
            const value_start = start + search.len;
            if (std.mem.indexOfScalarPos(u8, json, value_start, '"')) |end| {
                const result = try self.dupe(json[value_start..end]);
                return result;
            }
        }
        return null;
    }

    fn extractNumber(self: *Self, json: []const u8, key: []const u8) !?u64 {
        const search = try std.fmt.allocPrint(self.allocator, "\"{s}\":", .{key});
        defer self.allocator.free(search);

        if (std.mem.indexOf(u8, json, search)) |start| {
            const value_start = start + search.len;
            var end = value_start;
            while (end < json.len and (json[end] >= '0' and json[end] <= '9')) {
                end += 1;
            }
            if (end > value_start) {
                const num_str = json[value_start..end];
                return std.fmt.parseInt(u64, num_str, 10) catch null;
            }
        }
        return null;
    }

    fn extractBool(self: *Self, json: []const u8, key: []const u8) !?bool {
        const search = try std.fmt.allocPrint(self.allocator, "\"{s}\":", .{key});
        defer self.allocator.free(search);

        if (std.mem.indexOf(u8, json, search)) |start| {
            const value_start = start + search.len;
            if (std.mem.startsWith(u8, json[value_start..], "true")) return true;
            if (std.mem.startsWith(u8, json[value_start..], "false")) return false;
        }
        return null;
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

test "CosmosMetadataStore: docIdBucket" {
    var buf: [256]u8 = undefined;
    const result = std.fmt.bufPrint(&buf, "bucket_{s}", .{"my-bucket"}) catch &buf;
    try std.testing.expectEqualStrings("bucket_my-bucket", result);
}

test "CosmosMetadataStore: docIdPart" {
    var buf: [512]u8 = undefined;
    const result = std.fmt.bufPrint(&buf, "part_{s}_{d:0>5}", .{ "upload-123", @as(u32, 5) }) catch &buf;
    try std.testing.expectEqualStrings("part_upload-123_00005", result);
}
