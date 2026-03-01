const std = @import("std");
const store = @import("store.zig");
const BucketMeta = store.BucketMeta;
const ObjectMeta = store.ObjectMeta;
const MultipartUploadMeta = store.MultipartUploadMeta;
const PartMeta = store.PartMeta;
const Credential = store.Credential;
const ListObjectsResult = store.ListObjectsResult;
const ListUploadsResult = store.ListUploadsResult;
const ListPartsResult = store.ListPartsResult;
const MetadataStore = store.MetadataStore;

pub const LocalConfig = struct {
    root_dir: []const u8,
    compact_on_startup: bool,
};

const Inner = struct {
    buckets: std.StringHashMap(BucketMeta),
    objects: std.HashMap(struct { []const u8, []const u8 }, ObjectMeta, struct {
        pub fn hash(_: @This(), key: struct { []const u8, []const u8 }) u64 {
            var hasher = std.hash.Wyhash.init(0);
            hasher.update(key.@"0");
            hasher.update(":");
            hasher.update(key.@"1");
            return hasher.final();
        }
        pub fn eql(_: @This(), a: struct { []const u8, []const u8 }, b: struct { []const u8, []const u8 }) bool {
            return std.mem.eql(u8, a.@"0", b.@"0") and std.mem.eql(u8, a.@"1", b.@"1");
        }
    }, std.hash_map.default_max_load_percentage),
    uploads: std.StringHashMap(MultipartUploadMeta),
    parts: std.HashMap(struct { []const u8, u32 }, PartMeta, struct {
        pub fn hash(_: @This(), key: struct { []const u8, u32 }) u64 {
            var hasher = std.hash.Wyhash.init(0);
            hasher.update(key.@"0");
            hasher.update(":");
            hasher.update(std.mem.asBytes(&key.@"1"));
            return hasher.final();
        }
        pub fn eql(_: @This(), a: struct { []const u8, u32 }, b: struct { []const u8, u32 }) bool {
            return std.mem.eql(u8, a.@"0", b.@"0") and a.@"1" == b.@"1";
        }
    }, std.hash_map.default_max_load_percentage),
    credentials: std.StringHashMap(Credential),
};

pub const LocalStore = struct {
    allocator: std.mem.Allocator,
    config: LocalConfig,
    inner: std.Thread.RwLock,
    inner_data: Inner,

    pub fn init(allocator: std.mem.Allocator, config: LocalConfig) !LocalStore {
        var store_impl = LocalStore{
            .allocator = allocator,
            .config = config,
            .inner = .{},
            .inner_data = undefined,
        };
        store_impl.inner_data = Inner{
            .buckets = std.StringHashMap(BucketMeta).init(allocator),
            .objects = @TypeOf(store_impl.inner_data.objects).init(allocator),
            .uploads = std.StringHashMap(MultipartUploadMeta).init(allocator),
            .parts = @TypeOf(store_impl.inner_data.parts).init(allocator),
            .credentials = std.StringHashMap(Credential).init(allocator),
        };
        try store_impl.loadFromFiles();
        if (config.compact_on_startup) {
            try store_impl.compact();
        }
        return store_impl;
    }

    pub fn deinit(self: *LocalStore) void {
        self.inner.lock();
        defer self.inner.unlock();
        var iter = self.inner_data.buckets.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.inner_data.buckets.deinit();
        self.inner_data.objects.deinit();
        var upload_iter = self.inner_data.uploads.iterator();
        while (upload_iter.next()) |entry| {
            self.allocator.free(entry.value_ptr.upload_id);
            self.allocator.free(entry.value_ptr.bucket);
            self.allocator.free(entry.value_ptr.key);
            self.allocator.free(entry.value_ptr.initiated);
        }
        self.inner_data.uploads.deinit();
        self.inner_data.parts.deinit();
        self.inner_data.credentials.deinit();
    }

    fn loadFromFiles(self: *LocalStore) !void {
        try std.fs.cwd().makePath(self.config.root_dir);
        try self.loadFile("buckets.jsonl", .bucket);
        try self.loadFile("objects.jsonl", .object);
        try self.loadFile("uploads.jsonl", .upload);
        try self.loadFile("parts.jsonl", .part);
        try self.loadFile("credentials.jsonl", .credential);
    }

    const EntityType = enum { bucket, object, upload, part, credential };

    fn loadFile(self: *LocalStore, filename: []const u8, entity_type: EntityType) !void {
        const path = try std.fs.path.join(self.allocator, &.{ self.config.root_dir, filename });
        defer self.allocator.free(path);

        const file = std.fs.cwd().openFile(path, .{}) catch |err| {
            if (err == error.FileNotFound) return;
            return err;
        };
        defer file.close();

        const reader = file.deprecatedReader();

        while (reader.readUntilDelimiterOrEofAlloc(self.allocator, '\n', 1024 * 1024)) |maybe_line| {
            const line = maybe_line orelse break;
            defer self.allocator.free(line);

            const parsed = std.json.parseFromSlice(std.json.Value, self.allocator, line, .{}) catch continue;
            defer parsed.deinit();

            const obj = parsed.value.object;
            if (obj.get("_deleted")) |del| {
                if (del == .bool and del.bool) continue;
            }

            switch (entity_type) {
                .bucket => try self.parseBucket(obj),
                .object => try self.parseObject(obj),
                .upload => try self.parseUpload(obj),
                .part => try self.parsePart(obj),
                .credential => try self.parseCredential(obj),
            }
        } else |err| {
            if (err != error.EndOfStream) return err;
        }
    }

    fn parseBucket(self: *LocalStore, obj: std.json.ObjectMap) !void {
        const name = obj.get("name").?.string;
        const duped_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(duped_name);

        var meta: BucketMeta = .{
            .name = duped_name,
            .creation_date = try self.allocator.dupe(u8, obj.get("created_at").?.string),
            .region = try self.allocator.dupe(u8, obj.get("region").?.string),
            .owner_id = try self.allocator.dupe(u8, obj.get("owner_id").?.string),
        };
        if (obj.get("owner_display")) |od| meta.owner_display = try self.allocator.dupe(u8, od.string);
        if (obj.get("acl")) |acl| meta.acl = try self.allocator.dupe(u8, acl.string);

        try self.inner_data.buckets.put(duped_name, meta);
    }

    fn parseObject(self: *LocalStore, obj: std.json.ObjectMap) !void {
        const bucket = obj.get("bucket").?.string;
        const key = obj.get("key").?.string;

        var meta: ObjectMeta = .{
            .bucket = try self.allocator.dupe(u8, bucket),
            .key = try self.allocator.dupe(u8, key),
            .size = @intCast(obj.get("size").?.integer),
            .etag = try self.allocator.dupe(u8, obj.get("etag").?.string),
            .content_type = try self.allocator.dupe(u8, obj.get("content_type").?.string),
            .last_modified = try self.allocator.dupe(u8, obj.get("last_modified").?.string),
            .storage_class = try self.allocator.dupe(u8, obj.get("storage_class").?.string),
        };

        if (obj.get("content_encoding")) |ce| meta.content_encoding = try self.allocator.dupe(u8, ce.string);
        if (obj.get("content_language")) |cl| meta.content_language = try self.allocator.dupe(u8, cl.string);
        if (obj.get("content_disposition")) |cd| meta.content_disposition = try self.allocator.dupe(u8, cd.string);
        if (obj.get("cache_control")) |cc| meta.cache_control = try self.allocator.dupe(u8, cc.string);
        if (obj.get("expires")) |e| meta.expires = try self.allocator.dupe(u8, e.string);
        if (obj.get("user_metadata")) |um| meta.user_metadata = try self.allocator.dupe(u8, um.string);
        if (obj.get("acl")) |acl| meta.acl = try self.allocator.dupe(u8, acl.string);

        try self.inner_data.objects.put(.{ meta.bucket, meta.key }, meta);
    }

    fn parseUpload(self: *LocalStore, obj: std.json.ObjectMap) !void {
        var meta: MultipartUploadMeta = .{
            .upload_id = try self.allocator.dupe(u8, obj.get("upload_id").?.string),
            .bucket = try self.allocator.dupe(u8, obj.get("bucket").?.string),
            .key = try self.allocator.dupe(u8, obj.get("key").?.string),
            .initiated = try self.allocator.dupe(u8, obj.get("initiated_at").?.string),
        };

        if (obj.get("content_type")) |ct| meta.content_type = try self.allocator.dupe(u8, ct.string);
        if (obj.get("storage_class")) |sc| meta.storage_class = try self.allocator.dupe(u8, sc.string);
        if (obj.get("owner_id")) |oi| meta.owner_id = try self.allocator.dupe(u8, oi.string);

        try self.inner_data.uploads.put(meta.upload_id, meta);
    }

    fn parsePart(self: *LocalStore, obj: std.json.ObjectMap) !void {
        const upload_id = obj.get("upload_id").?.string;
        const duped_id = try self.allocator.dupe(u8, upload_id);

        const meta: PartMeta = .{
            .part_number = @intCast(obj.get("part_number").?.integer),
            .etag = try self.allocator.dupe(u8, obj.get("etag").?.string),
            .size = @intCast(obj.get("size").?.integer),
            .last_modified = try self.allocator.dupe(u8, obj.get("last_modified").?.string),
        };

        try self.inner_data.parts.put(.{ duped_id, meta.part_number }, meta);
    }

    fn parseCredential(self: *LocalStore, obj: std.json.ObjectMap) !void {
        var cred: Credential = .{
            .access_key_id = try self.allocator.dupe(u8, obj.get("access_key_id").?.string),
            .secret_key = try self.allocator.dupe(u8, obj.get("secret_key").?.string),
            .owner_id = try self.allocator.dupe(u8, obj.get("owner_id").?.string),
        };

        if (obj.get("display_name")) |dn| cred.display_name = try self.allocator.dupe(u8, dn.string);
        if (obj.get("active")) |a| cred.active = a.bool;
        if (obj.get("created_at")) |ca| cred.created_at = try self.allocator.dupe(u8, ca.string);

        try self.inner_data.credentials.put(cred.access_key_id, cred);
    }

    fn compact(self: *LocalStore) !void {
        _ = self;
    }

    fn appendLine(self: *LocalStore, filename: []const u8, line: []const u8) !void {
        const path = try std.fs.path.join(self.allocator, &.{ self.config.root_dir, filename });
        defer self.allocator.free(path);

        const file = try std.fs.cwd().createFile(path, .{ .truncate = false });
        defer file.close();
        try file.seekFromEnd(0);
        try file.deprecatedWriter().writeAll(line);
        try file.deprecatedWriter().writeByte('\n');
    }

    // --- MetadataStore implementation via vtable wrappers ---

    pub fn createBucket(ctx: *anyopaque, meta: BucketMeta) anyerror!void {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lock();
        defer self.inner.unlock();

        const duped_name = try self.allocator.dupe(u8, meta.name);
        errdefer self.allocator.free(duped_name);

        const owned_meta: BucketMeta = .{
            .name = duped_name,
            .creation_date = try self.allocator.dupe(u8, meta.creation_date),
            .region = try self.allocator.dupe(u8, meta.region),
            .owner_id = try self.allocator.dupe(u8, meta.owner_id),
            .owner_display = if (meta.owner_display.len > 0) try self.allocator.dupe(u8, meta.owner_display) else "",
            .acl = try self.allocator.dupe(u8, meta.acl),
        };

        try self.inner_data.buckets.put(duped_name, owned_meta);

        const json_str = try std.json.Stringify.valueAlloc(self.allocator, .{
            .type = "bucket",
            .name = meta.name,
            .created_at = meta.creation_date,
            .region = meta.region,
            .owner_id = meta.owner_id,
            .owner_display = meta.owner_display,
            .acl = meta.acl,
        }, .{});
        defer self.allocator.free(json_str);
        try self.appendLine("buckets.jsonl", json_str);
    }

    pub fn deleteBucket(ctx: *anyopaque, name: []const u8) anyerror!void {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lock();
        defer self.inner.unlock();

        _ = self.inner_data.buckets.fetchRemove(name);

        const json_str = try std.json.Stringify.valueAlloc(self.allocator, .{
            .type = "bucket",
            .name = name,
            ._deleted = true,
        }, .{});
        defer self.allocator.free(json_str);
        try self.appendLine("buckets.jsonl", json_str);
    }

    pub fn getBucket(ctx: *anyopaque, name: []const u8) anyerror!?BucketMeta {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lockShared();
        defer self.inner.unlockShared();
        return if (self.inner_data.buckets.get(name)) |m| m else null;
    }

    pub fn listBuckets(ctx: *anyopaque) anyerror![]BucketMeta {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lockShared();
        defer self.inner.unlockShared();

        var list: std.ArrayList(BucketMeta) = .empty;
        var iter = self.inner_data.buckets.iterator();
        while (iter.next()) |entry| {
            try list.append(self.allocator, entry.value_ptr.*);
        }
        return list.toOwnedSlice(self.allocator);
    }

    pub fn bucketExists(ctx: *anyopaque, name: []const u8) anyerror!bool {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lockShared();
        defer self.inner.unlockShared();
        return self.inner_data.buckets.contains(name);
    }

    pub fn updateBucketAcl(ctx: *anyopaque, name: []const u8, acl: []const u8) anyerror!void {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lock();
        defer self.inner.unlock();

        if (self.inner_data.buckets.getPtr(name)) |bucket| {
            self.allocator.free(bucket.acl);
            bucket.acl = try self.allocator.dupe(u8, acl);
        }
    }

    pub fn putObjectMeta(ctx: *anyopaque, meta: ObjectMeta) anyerror!void {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lock();
        defer self.inner.unlock();

        const bucket = try self.allocator.dupe(u8, meta.bucket);
        const key = try self.allocator.dupe(u8, meta.key);

        const owned: ObjectMeta = .{
            .bucket = bucket,
            .key = key,
            .size = meta.size,
            .etag = try self.allocator.dupe(u8, meta.etag),
            .content_type = try self.allocator.dupe(u8, meta.content_type),
            .last_modified = try self.allocator.dupe(u8, meta.last_modified),
            .storage_class = try self.allocator.dupe(u8, meta.storage_class),
            .user_metadata = if (meta.user_metadata) |um| try self.allocator.dupe(u8, um) else null,
            .content_encoding = if (meta.content_encoding) |ce| try self.allocator.dupe(u8, ce) else null,
            .content_language = if (meta.content_language) |cl| try self.allocator.dupe(u8, cl) else null,
            .content_disposition = if (meta.content_disposition) |cd| try self.allocator.dupe(u8, cd) else null,
            .cache_control = if (meta.cache_control) |cc| try self.allocator.dupe(u8, cc) else null,
            .expires = if (meta.expires) |e| try self.allocator.dupe(u8, e) else null,
            .acl = try self.allocator.dupe(u8, meta.acl),
        };

        try self.inner_data.objects.put(.{ bucket, key }, owned);

        const json_str = try std.json.Stringify.valueAlloc(self.allocator, .{
            .type = "object",
            .bucket = meta.bucket,
            .key = meta.key,
            .size = meta.size,
            .etag = meta.etag,
            .content_type = meta.content_type,
            .last_modified = meta.last_modified,
            .storage_class = meta.storage_class,
        }, .{});
        defer self.allocator.free(json_str);
        try self.appendLine("objects.jsonl", json_str);
    }

    pub fn getObjectMeta(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!?ObjectMeta {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lockShared();
        defer self.inner.unlockShared();
        return if (self.inner_data.objects.get(.{ bucket, key })) |m| m else null;
    }

    pub fn deleteObjectMeta(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!bool {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lock();
        defer self.inner.unlock();

        const existed = self.inner_data.objects.remove(.{ bucket, key });

        const json_str = try std.json.Stringify.valueAlloc(self.allocator, .{
            .type = "object",
            .bucket = bucket,
            .key = key,
            ._deleted = true,
        }, .{});
        defer self.allocator.free(json_str);
        try self.appendLine("objects.jsonl", json_str);

        return existed;
    }

    pub fn deleteObjectsMeta(ctx: *anyopaque, bucket: []const u8, keys: []const []const u8) anyerror![]bool {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        var results = try self.allocator.alloc(bool, keys.len);

        self.inner.lock();
        defer self.inner.unlock();

        for (keys, 0..) |key, i| {
            results[i] = self.inner_data.objects.remove(.{ bucket, key });
        }
        return results;
    }

    pub fn listObjectsMeta(ctx: *anyopaque, bucket: []const u8, prefix: []const u8, delimiter: []const u8, start_after: []const u8, max_keys: u32) anyerror!ListObjectsResult {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lockShared();
        defer self.inner.unlockShared();

        var objects: std.ArrayList(ObjectMeta) = .empty;
        var iter = self.inner_data.objects.iterator();

        while (iter.next()) |entry| {
            const obj_key = entry.key_ptr.@"1";
            if (!std.mem.eql(u8, entry.key_ptr.@"0", bucket)) continue;
            if (obj_key.len <= start_after.len and std.mem.order(u8, obj_key, start_after) != .gt) continue;
            if (prefix.len > 0 and !std.mem.startsWith(u8, obj_key, prefix)) continue;
            if (delimiter.len > 0) {
                if (std.mem.indexOf(u8, obj_key[prefix.len..], delimiter) != null) continue;
            }
            try objects.append(self.allocator, entry.value_ptr.*);
        }

        std.sort.block(ObjectMeta, objects.items, {}, struct {
            fn lessThan(_: void, a: ObjectMeta, b: ObjectMeta) bool {
                return std.mem.order(u8, a.key, b.key) == .lt;
            }
        }.lessThan);

        const is_truncated = objects.items.len > max_keys;
        if (is_truncated) {
            objects.shrinkAndFree(self.allocator, max_keys);
        }

        return .{
            .objects = objects.items,
            .common_prefixes = &.{},
            .is_truncated = is_truncated,
            .next_continuation_token = if (is_truncated and objects.items.len > 0) objects.items[objects.items.len - 1].key else null,
        };
    }

    pub fn objectExists(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!bool {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lockShared();
        defer self.inner.unlockShared();
        return self.inner_data.objects.contains(.{ bucket, key });
    }

    pub fn updateObjectAcl(ctx: *anyopaque, bucket: []const u8, key: []const u8, acl: []const u8) anyerror!void {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lock();
        defer self.inner.unlock();

        if (self.inner_data.objects.getPtr(.{ bucket, key })) |obj| {
            self.allocator.free(obj.acl);
            obj.acl = try self.allocator.dupe(u8, acl);
        }
    }

    pub fn createMultipartUpload(ctx: *anyopaque, meta: MultipartUploadMeta) anyerror!void {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lock();
        defer self.inner.unlock();

        const upload_id = try self.allocator.dupe(u8, meta.upload_id);
        const owned: MultipartUploadMeta = .{
            .upload_id = upload_id,
            .bucket = try self.allocator.dupe(u8, meta.bucket),
            .key = try self.allocator.dupe(u8, meta.key),
            .initiated = try self.allocator.dupe(u8, meta.initiated),
            .content_type = try self.allocator.dupe(u8, meta.content_type),
            .storage_class = try self.allocator.dupe(u8, meta.storage_class),
        };

        try self.inner_data.uploads.put(upload_id, owned);
    }

    pub fn getMultipartUpload(ctx: *anyopaque, upload_id: []const u8) anyerror!?MultipartUploadMeta {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lockShared();
        defer self.inner.unlockShared();
        return if (self.inner_data.uploads.get(upload_id)) |m| m else null;
    }

    pub fn abortMultipartUpload(ctx: *anyopaque, upload_id: []const u8) anyerror!void {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lock();
        defer self.inner.unlock();

        _ = self.inner_data.uploads.fetchRemove(upload_id);

        var iter = self.inner_data.parts.iterator();
        while (iter.next()) |entry| {
            if (std.mem.eql(u8, entry.key_ptr.@"0", upload_id)) {
                _ = self.inner_data.parts.remove(entry.key_ptr.*);
            }
        }
    }

    pub fn putPartMeta(ctx: *anyopaque, upload_id: []const u8, part: PartMeta) anyerror!void {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lock();
        defer self.inner.unlock();

        const duped_id = try self.allocator.dupe(u8, upload_id);
        const owned: PartMeta = .{
            .part_number = part.part_number,
            .etag = try self.allocator.dupe(u8, part.etag),
            .size = part.size,
            .last_modified = try self.allocator.dupe(u8, part.last_modified),
        };

        try self.inner_data.parts.put(.{ duped_id, part.part_number }, owned);
    }

    pub fn listPartsMeta(ctx: *anyopaque, upload_id: []const u8, max_parts: u32, part_marker: u32) anyerror!ListPartsResult {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lockShared();
        defer self.inner.unlockShared();

        var parts: std.ArrayList(PartMeta) = .empty;
        var iter = self.inner_data.parts.iterator();

        while (iter.next()) |entry| {
            if (std.mem.eql(u8, entry.key_ptr.@"0", upload_id) and entry.key_ptr.@"1" > part_marker) {
                try parts.append(self.allocator, entry.value_ptr.*);
            }
        }

        std.sort.block(PartMeta, parts.items, {}, struct {
            fn lessThan(_: void, a: PartMeta, b: PartMeta) bool {
                return a.part_number < b.part_number;
            }
        }.lessThan);

        const is_truncated = parts.items.len > max_parts;
        if (is_truncated) {
            parts.shrinkAndFree(self.allocator, max_parts);
        }

        return .{
            .parts = parts.items,
            .is_truncated = is_truncated,
            .next_part_number_marker = if (is_truncated and parts.items.len > 0) parts.items[parts.items.len - 1].part_number else 0,
        };
    }

    pub fn getPartsForCompletion(ctx: *anyopaque, upload_id: []const u8) anyerror![]PartMeta {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lockShared();
        defer self.inner.unlockShared();

        var parts: std.ArrayList(PartMeta) = .empty;
        var iter = self.inner_data.parts.iterator();

        while (iter.next()) |entry| {
            if (std.mem.eql(u8, entry.key_ptr.@"0", upload_id)) {
                try parts.append(self.allocator, entry.value_ptr.*);
            }
        }

        std.sort.block(PartMeta, parts.items, {}, struct {
            fn lessThan(_: void, a: PartMeta, b: PartMeta) bool {
                return a.part_number < b.part_number;
            }
        }.lessThan);

        return parts.toOwnedSlice(self.allocator);
    }

    pub fn completeMultipartUpload(ctx: *anyopaque, upload_id: []const u8, object_meta: ObjectMeta) anyerror!void {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lock();
        defer self.inner.unlock();

        try @This().putObjectMeta(ctx, object_meta);
        _ = self.inner_data.uploads.fetchRemove(upload_id);

        var iter = self.inner_data.parts.iterator();
        while (iter.next()) |entry| {
            if (std.mem.eql(u8, entry.key_ptr.@"0", upload_id)) {
                _ = self.inner_data.parts.remove(entry.key_ptr.*);
            }
        }
    }

    pub fn listMultipartUploads(ctx: *anyopaque, bucket: []const u8, prefix: []const u8, max_uploads: u32) anyerror!ListUploadsResult {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lockShared();
        defer self.inner.unlockShared();

        var uploads: std.ArrayList(MultipartUploadMeta) = .empty;
        var iter = self.inner_data.uploads.iterator();

        while (iter.next()) |entry| {
            const upload = entry.value_ptr;
            if (!std.mem.eql(u8, upload.bucket, bucket)) continue;
            if (prefix.len > 0 and !std.mem.startsWith(u8, upload.key, prefix)) continue;
            try uploads.append(self.allocator, upload.*);
        }

        const is_truncated = uploads.items.len > max_uploads;
        if (is_truncated) {
            uploads.shrinkAndFree(self.allocator, max_uploads);
        }

        return .{
            .uploads = uploads.items,
            .is_truncated = is_truncated,
        };
    }

    pub fn getCredential(ctx: *anyopaque, access_key_id: []const u8) anyerror!?Credential {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lockShared();
        defer self.inner.unlockShared();
        return if (self.inner_data.credentials.get(access_key_id)) |c| c else null;
    }

    pub fn putCredential(ctx: *anyopaque, cred: Credential) anyerror!void {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lock();
        defer self.inner.unlock();

        const key = try self.allocator.dupe(u8, cred.access_key_id);
        const owned: Credential = .{
            .access_key_id = key,
            .secret_key = try self.allocator.dupe(u8, cred.secret_key),
            .owner_id = try self.allocator.dupe(u8, cred.owner_id),
            .display_name = if (cred.display_name.len > 0) try self.allocator.dupe(u8, cred.display_name) else "",
            .active = cred.active,
            .created_at = if (cred.created_at.len > 0) try self.allocator.dupe(u8, cred.created_at) else "",
        };

        try self.inner_data.credentials.put(key, owned);
    }

    pub fn countBuckets(ctx: *anyopaque) anyerror!u64 {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lockShared();
        defer self.inner.unlockShared();
        return @intCast(self.inner_data.buckets.count());
    }

    pub fn countObjects(ctx: *anyopaque) anyerror!u64 {
        const self: *LocalStore = @ptrCast(@alignCast(ctx));
        self.inner.lockShared();
        defer self.inner.unlockShared();
        return @intCast(self.inner_data.objects.count());
    }

    pub fn asMetadataStore(self: *LocalStore) MetadataStore {
        return .{
            .ctx = self,
            .vtable = &.{
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
            },
        };
    }
};
