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

pub const MemoryMetadataStore = struct {
    allocator: std.mem.Allocator,
    buckets: std.StringHashMap(BucketEntry),
    objects: std.StringHashMap(ObjectEntry),
    uploads: std.StringHashMap(UploadEntry),
    parts: std.StringHashMap(std.ArrayList(PartEntry)),
    credentials: std.StringHashMap(CredentialEntry),
    mutex: std.Thread.Mutex,

    const Self = @This();

    const BucketEntry = struct {
        name: []const u8,
        creation_date: []const u8,
        region: []const u8,
        owner_id: []const u8,
        owner_display: []const u8,
        acl: []const u8,
    };

    const ObjectEntry = struct {
        bucket: []const u8,
        key: []const u8,
        size: u64,
        etag: []const u8,
        content_type: []const u8,
        last_modified: []const u8,
        storage_class: []const u8,
        user_metadata: ?[]const u8,
        version_id: ?[]const u8,
        content_encoding: ?[]const u8,
        content_language: ?[]const u8,
        content_disposition: ?[]const u8,
        cache_control: ?[]const u8,
        expires: ?[]const u8,
        acl: []const u8,
        delete_marker: bool,
    };

    const UploadEntry = struct {
        upload_id: []const u8,
        bucket: []const u8,
        key: []const u8,
        initiated: []const u8,
        content_type: []const u8,
        content_encoding: ?[]const u8,
        content_language: ?[]const u8,
        content_disposition: ?[]const u8,
        cache_control: ?[]const u8,
        expires: ?[]const u8,
        storage_class: []const u8,
        acl: []const u8,
        user_metadata: []const u8,
        owner_id: []const u8,
        owner_display: []const u8,
    };

    const PartEntry = struct {
        part_number: u32,
        etag: []const u8,
        size: u64,
        last_modified: []const u8,
    };

    const CredentialEntry = struct {
        access_key_id: []const u8,
        secret_key: []const u8,
        owner_id: []const u8,
        display_name: []const u8,
        active: bool,
        created_at: []const u8,
    };

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .allocator = allocator,
            .buckets = std.StringHashMap(BucketEntry).init(allocator),
            .objects = std.StringHashMap(ObjectEntry).init(allocator),
            .uploads = std.StringHashMap(UploadEntry).init(allocator),
            .parts = std.StringHashMap(std.ArrayList(PartEntry)).init(allocator),
            .credentials = std.StringHashMap(CredentialEntry).init(allocator),
            .mutex = .{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var bucket_iter = self.buckets.iterator();
        while (bucket_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.freeBucketEntry(entry.value_ptr.*);
        }
        self.buckets.deinit();

        var obj_iter = self.objects.iterator();
        while (obj_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.freeObjectEntry(entry.value_ptr.*);
        }
        self.objects.deinit();

        var upload_iter = self.uploads.iterator();
        while (upload_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.freeUploadEntry(entry.value_ptr.*);
        }
        self.uploads.deinit();

        var parts_iter = self.parts.iterator();
        while (parts_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            for (entry.value_ptr.items) |part| {
                self.freePartEntry(part);
            }
            entry.value_ptr.deinit();
        }
        self.parts.deinit();

        var cred_iter = self.credentials.iterator();
        while (cred_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.freeCredentialEntry(entry.value_ptr.*);
        }
        self.credentials.deinit();
    }

    fn freeBucketEntry(self: *Self, entry: BucketEntry) void {
        self.allocator.free(entry.name);
        self.allocator.free(entry.creation_date);
        self.allocator.free(entry.region);
        self.allocator.free(entry.owner_id);
        self.allocator.free(entry.owner_display);
        self.allocator.free(entry.acl);
    }

    fn freeObjectEntry(self: *Self, entry: ObjectEntry) void {
        self.allocator.free(entry.bucket);
        self.allocator.free(entry.key);
        self.allocator.free(entry.etag);
        self.allocator.free(entry.content_type);
        self.allocator.free(entry.last_modified);
        self.allocator.free(entry.storage_class);
        self.allocator.free(entry.acl);
        if (entry.user_metadata) |um| self.allocator.free(um);
        if (entry.version_id) |v| self.allocator.free(v);
        if (entry.content_encoding) |ce| self.allocator.free(ce);
        if (entry.content_language) |cl| self.allocator.free(cl);
        if (entry.content_disposition) |cd| self.allocator.free(cd);
        if (entry.cache_control) |cc| self.allocator.free(cc);
        if (entry.expires) |ex| self.allocator.free(ex);
    }

    fn freeUploadEntry(self: *Self, entry: UploadEntry) void {
        self.allocator.free(entry.upload_id);
        self.allocator.free(entry.bucket);
        self.allocator.free(entry.key);
        self.allocator.free(entry.initiated);
        self.allocator.free(entry.content_type);
        self.allocator.free(entry.storage_class);
        self.allocator.free(entry.acl);
        self.allocator.free(entry.user_metadata);
        self.allocator.free(entry.owner_id);
        self.allocator.free(entry.owner_display);
        if (entry.content_encoding) |ce| self.allocator.free(ce);
        if (entry.content_language) |cl| self.allocator.free(cl);
        if (entry.content_disposition) |cd| self.allocator.free(cd);
        if (entry.cache_control) |cc| self.allocator.free(cc);
        if (entry.expires) |ex| self.allocator.free(ex);
    }

    fn freePartEntry(self: *Self, entry: PartEntry) void {
        self.allocator.free(entry.etag);
        self.allocator.free(entry.last_modified);
    }

    fn freeCredentialEntry(self: *Self, entry: CredentialEntry) void {
        self.allocator.free(entry.access_key_id);
        self.allocator.free(entry.secret_key);
        self.allocator.free(entry.owner_id);
        self.allocator.free(entry.display_name);
        self.allocator.free(entry.created_at);
    }

    fn makeObjectKey(self: *Self, bucket: []const u8, key: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.allocator, "{s}\x00{s}", .{ bucket, key });
    }

    fn dupe(self: *Self, s: []const u8) ![]const u8 {
        return self.allocator.dupe(u8, s);
    }

    fn dupeOpt(self: *Self, s: ?[]const u8) !?[]const u8 {
        if (s) |str| return self.allocator.dupe(u8, str);
        return null;
    }

    fn createBucket(ctx: *anyopaque, meta: BucketMeta) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.buckets.contains(meta.name)) {
            return error.BucketAlreadyExists;
        }

        const key = try self.dupe(meta.name);
        errdefer self.allocator.free(key);

        const entry = BucketEntry{
            .name = try self.dupe(meta.name),
            .creation_date = try self.dupe(meta.creation_date),
            .region = try self.dupe(meta.region),
            .owner_id = try self.dupe(meta.owner_id),
            .owner_display = try self.dupe(meta.owner_display),
            .acl = try self.dupe(meta.acl),
        };

        try self.buckets.put(key, entry);
    }

    fn deleteBucket(ctx: *anyopaque, name: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var obj_iter = self.objects.iterator();
        while (obj_iter.next()) |entry| {
            if (std.mem.eql(u8, entry.value_ptr.bucket, name)) {
                return error.BucketNotEmpty;
            }
        }

        if (self.buckets.fetchRemove(name)) |removed| {
            self.allocator.free(removed.key);
            self.freeBucketEntry(removed.value);
        } else {
            return error.NoSuchBucket;
        }
    }

    fn getBucket(ctx: *anyopaque, name: []const u8) anyerror!?BucketMeta {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.buckets.get(name)) |entry| {
            return BucketMeta{
                .name = try self.dupe(entry.name),
                .creation_date = try self.dupe(entry.creation_date),
                .region = try self.dupe(entry.region),
                .owner_id = try self.dupe(entry.owner_id),
                .owner_display = try self.dupe(entry.owner_display),
                .acl = try self.dupe(entry.acl),
            };
        }
        return null;
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

        var iter = self.buckets.iterator();
        while (iter.next()) |entry| {
            const meta = BucketMeta{
                .name = try self.dupe(entry.value_ptr.name),
                .creation_date = try self.dupe(entry.value_ptr.creation_date),
                .region = try self.dupe(entry.value_ptr.region),
                .owner_id = try self.dupe(entry.value_ptr.owner_id),
                .owner_display = try self.dupe(entry.value_ptr.owner_display),
                .acl = try self.dupe(entry.value_ptr.acl),
            };
            try list.append(self.allocator, meta);
        }

        return list.toOwnedSlice(self.allocator);
    }

    fn bucketExists(ctx: *anyopaque, name: []const u8) anyerror!bool {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.buckets.contains(name);
    }

    fn updateBucketAcl(ctx: *anyopaque, name: []const u8, acl: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.buckets.getPtr(name)) |entry| {
            self.allocator.free(entry.acl);
            entry.acl = try self.dupe(acl);
        } else {
            return error.NoSuchBucket;
        }
    }

    fn putObjectMeta(ctx: *anyopaque, meta: ObjectMeta) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const key = try self.makeObjectKey(meta.bucket, meta.key);
        errdefer self.allocator.free(key);

        if (self.objects.fetchSwap(key, undefined)) |old| {
            self.allocator.free(old.key);
            self.freeObjectEntry(old.value);
        }

        const entry = ObjectEntry{
            .bucket = try self.dupe(meta.bucket),
            .key = try self.dupe(meta.key),
            .size = meta.size,
            .etag = try self.dupe(meta.etag),
            .content_type = try self.dupe(meta.content_type),
            .last_modified = try self.dupe(meta.last_modified),
            .storage_class = try self.dupe(meta.storage_class),
            .user_metadata = try self.dupeOpt(meta.user_metadata),
            .version_id = try self.dupeOpt(meta.version_id),
            .content_encoding = try self.dupeOpt(meta.content_encoding),
            .content_language = try self.dupeOpt(meta.content_language),
            .content_disposition = try self.dupeOpt(meta.content_disposition),
            .cache_control = try self.dupeOpt(meta.cache_control),
            .expires = try self.dupeOpt(meta.expires),
            .acl = try self.dupe(meta.acl),
            .delete_marker = meta.delete_marker,
        };

        try self.objects.put(key, entry);
    }

    fn getObjectMeta(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!?ObjectMeta {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const obj_key = try self.makeObjectKey(bucket, key);
        defer self.allocator.free(obj_key);

        if (self.objects.get(obj_key)) |entry| {
            return ObjectMeta{
                .bucket = try self.dupe(entry.bucket),
                .key = try self.dupe(entry.key),
                .size = entry.size,
                .etag = try self.dupe(entry.etag),
                .content_type = try self.dupe(entry.content_type),
                .last_modified = try self.dupe(entry.last_modified),
                .storage_class = try self.dupe(entry.storage_class),
                .user_metadata = try self.dupeOpt(entry.user_metadata),
                .version_id = try self.dupeOpt(entry.version_id),
                .content_encoding = try self.dupeOpt(entry.content_encoding),
                .content_language = try self.dupeOpt(entry.content_language),
                .content_disposition = try self.dupeOpt(entry.content_disposition),
                .cache_control = try self.dupeOpt(entry.cache_control),
                .expires = try self.dupeOpt(entry.expires),
                .acl = try self.dupe(entry.acl),
                .delete_marker = entry.delete_marker,
            };
        }
        return null;
    }

    fn deleteObjectMeta(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!bool {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const obj_key = try self.makeObjectKey(bucket, key);
        defer self.allocator.free(obj_key);

        if (self.objects.fetchRemove(obj_key)) |removed| {
            self.allocator.free(removed.key);
            self.freeObjectEntry(removed.value);
            return true;
        }
        return false;
    }

    fn deleteObjectsMeta(ctx: *anyopaque, bucket: []const u8, keys: []const []const u8) anyerror![]bool {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const results = try self.allocator.alloc(bool, keys.len);
        @memset(results, true);

        for (keys) |key| {
            const obj_key = try self.makeObjectKey(bucket, key);
            defer self.allocator.free(obj_key);

            if (self.objects.fetchRemove(obj_key)) |removed| {
                self.allocator.free(removed.key);
                self.freeObjectEntry(removed.value);
            }
        }

        return results;
    }

    fn listObjectsMeta(ctx: *anyopaque, bucket: []const u8, prefix: []const u8, delimiter: []const u8, start_after: []const u8, max_keys: u32) anyerror!ListObjectsResult {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        var objects_list: std.ArrayList(ObjectMeta) = .empty;
        errdefer {
            for (objects_list.items) |*obj| self.freeObjectMetaFromMeta(obj);
            objects_list.deinit(self.allocator);
        }

        var common_prefixes: std.ArrayList([]const u8) = .empty;
        errdefer {
            for (common_prefixes.items) |cp| self.allocator.free(cp);
            common_prefixes.deinit(self.allocator);
        }

        var count: u32 = 0;
        var is_truncated = false;
        var last_key: ?[]const u8 = null;
        var seen_prefixes = std.StringHashMap(void).init(self.allocator);
        defer {
            var iter = seen_prefixes.iterator();
            while (iter.next()) |entry| self.allocator.free(entry.key_ptr.*);
            seen_prefixes.deinit();
        }

        var iter = self.objects.iterator();
        while (iter.next()) |entry| {
            if (!std.mem.eql(u8, entry.value_ptr.bucket, bucket)) continue;

            const obj_key = entry.value_ptr.key;
            if (obj_key.len > 0 and obj_key[0] == 0) continue;

            if (prefix.len > 0 and !std.mem.startsWith(u8, obj_key, prefix)) continue;
            if (start_after.len > 0 and std.mem.order(u8, obj_key, start_after) != .gt) continue;

            if (count >= max_keys) {
                is_truncated = true;
                break;
            }

            if (delimiter.len > 0) {
                const key_after_prefix = if (prefix.len > 0 and std.mem.startsWith(u8, obj_key, prefix))
                    obj_key[prefix.len..]
                else
                    obj_key;

                if (std.mem.indexOf(u8, key_after_prefix, delimiter)) |delim_pos| {
                    const cp_end = prefix.len + delim_pos + delimiter.len;
                    const cp = obj_key[0..cp_end];

                    if (!seen_prefixes.contains(cp)) {
                        const cp_dupe = try self.dupe(cp);
                        try seen_prefixes.put(cp_dupe, {});
                        try common_prefixes.append(self.allocator, cp_dupe);
                    }
                    count += 1;
                    continue;
                }
            }

            const meta = ObjectMeta{
                .bucket = try self.dupe(entry.value_ptr.bucket),
                .key = try self.dupe(entry.value_ptr.key),
                .size = entry.value_ptr.size,
                .etag = try self.dupe(entry.value_ptr.etag),
                .content_type = try self.dupe(entry.value_ptr.content_type),
                .last_modified = try self.dupe(entry.value_ptr.last_modified),
                .storage_class = try self.dupe(entry.value_ptr.storage_class),
                .user_metadata = try self.dupeOpt(entry.value_ptr.user_metadata),
                .version_id = try self.dupeOpt(entry.value_ptr.version_id),
                .content_encoding = try self.dupeOpt(entry.value_ptr.content_encoding),
                .content_language = try self.dupeOpt(entry.value_ptr.content_language),
                .content_disposition = try self.dupeOpt(entry.value_ptr.content_disposition),
                .cache_control = try self.dupeOpt(entry.value_ptr.cache_control),
                .expires = try self.dupeOpt(entry.value_ptr.expires),
                .acl = try self.dupe(entry.value_ptr.acl),
                .delete_marker = entry.value_ptr.delete_marker,
            };

            last_key = meta.key;
            try objects_list.append(self.allocator, meta);
            count += 1;
        }

        var next_continuation_token: ?[]const u8 = null;
        var next_marker: ?[]const u8 = null;
        if (is_truncated and last_key != null) {
            next_continuation_token = try self.dupe(last_key.?);
            next_marker = try self.dupe(last_key.?);
        }

        return ListObjectsResult{
            .objects = try objects_list.toOwnedSlice(self.allocator),
            .common_prefixes = try common_prefixes.toOwnedSlice(self.allocator),
            .is_truncated = is_truncated,
            .next_continuation_token = next_continuation_token,
            .next_marker = next_marker,
        };
    }

    fn freeObjectMetaFromMeta(self: *Self, meta: *const ObjectMeta) void {
        self.allocator.free(meta.bucket);
        self.allocator.free(meta.key);
        self.allocator.free(meta.etag);
        self.allocator.free(meta.content_type);
        self.allocator.free(meta.last_modified);
        self.allocator.free(meta.storage_class);
        self.allocator.free(meta.acl);
        if (meta.user_metadata) |um| self.allocator.free(um);
        if (meta.version_id) |v| self.allocator.free(v);
        if (meta.content_encoding) |ce| self.allocator.free(ce);
        if (meta.content_language) |cl| self.allocator.free(cl);
        if (meta.content_disposition) |cd| self.allocator.free(cd);
        if (meta.cache_control) |cc| self.allocator.free(cc);
        if (meta.expires) |ex| self.allocator.free(ex);
    }

    fn objectExists(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!bool {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const obj_key = try self.makeObjectKey(bucket, key);
        defer self.allocator.free(obj_key);
        return self.objects.contains(obj_key);
    }

    fn updateObjectAcl(ctx: *anyopaque, bucket: []const u8, key: []const u8, acl: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const obj_key = try self.makeObjectKey(bucket, key);
        defer self.allocator.free(obj_key);

        if (self.objects.getPtr(obj_key)) |entry| {
            self.allocator.free(entry.acl);
            entry.acl = try self.dupe(acl);
        } else {
            return error.NoSuchKey;
        }
    }

    fn createMultipartUpload(ctx: *anyopaque, meta: MultipartUploadMeta) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const key = try self.dupe(meta.upload_id);
        errdefer self.allocator.free(key);

        const entry = UploadEntry{
            .upload_id = try self.dupe(meta.upload_id),
            .bucket = try self.dupe(meta.bucket),
            .key = try self.dupe(meta.key),
            .initiated = try self.dupe(meta.initiated),
            .content_type = try self.dupe(meta.content_type),
            .content_encoding = try self.dupeOpt(meta.content_encoding),
            .content_language = try self.dupeOpt(meta.content_language),
            .content_disposition = try self.dupeOpt(meta.content_disposition),
            .cache_control = try self.dupeOpt(meta.cache_control),
            .expires = try self.dupeOpt(meta.expires),
            .storage_class = try self.dupe(meta.storage_class),
            .acl = try self.dupe(meta.acl),
            .user_metadata = try self.dupe(meta.user_metadata),
            .owner_id = try self.dupe(meta.owner_id),
            .owner_display = try self.dupe(meta.owner_display),
        };

        try self.uploads.put(key, entry);

        const parts_list = std.ArrayList(PartEntry).init(self.allocator);
        try self.parts.put(try self.dupe(meta.upload_id), parts_list);
    }

    fn getMultipartUpload(ctx: *anyopaque, upload_id: []const u8) anyerror!?MultipartUploadMeta {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.uploads.get(upload_id)) |entry| {
            return MultipartUploadMeta{
                .upload_id = try self.dupe(entry.upload_id),
                .bucket = try self.dupe(entry.bucket),
                .key = try self.dupe(entry.key),
                .initiated = try self.dupe(entry.initiated),
                .content_type = try self.dupe(entry.content_type),
                .content_encoding = try self.dupeOpt(entry.content_encoding),
                .content_language = try self.dupeOpt(entry.content_language),
                .content_disposition = try self.dupeOpt(entry.content_disposition),
                .cache_control = try self.dupeOpt(entry.cache_control),
                .expires = try self.dupeOpt(entry.expires),
                .storage_class = try self.dupe(entry.storage_class),
                .acl = try self.dupe(entry.acl),
                .user_metadata = try self.dupe(entry.user_metadata),
                .owner_id = try self.dupe(entry.owner_id),
                .owner_display = try self.dupe(entry.owner_display),
            };
        }
        return null;
    }

    fn abortMultipartUpload(ctx: *anyopaque, upload_id: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.uploads.fetchRemove(upload_id)) |removed| {
            self.allocator.free(removed.key);
            self.freeUploadEntry(removed.value);
        } else {
            return error.NoSuchUpload;
        }

        if (self.parts.fetchRemove(upload_id)) |removed| {
            self.allocator.free(removed.key);
            for (removed.value.items) |part| {
                self.freePartEntry(part);
            }
            removed.value.deinit();
        }
    }

    fn putPartMeta(ctx: *anyopaque, upload_id: []const u8, part: PartMeta) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.parts.getPtr(upload_id)) |parts_list| {
            const entry = PartEntry{
                .part_number = part.part_number,
                .etag = try self.dupe(part.etag),
                .size = part.size,
                .last_modified = try self.dupe(part.last_modified),
            };

            for (parts_list.items) |*existing| {
                if (existing.part_number == part.part_number) {
                    self.freePartEntry(existing.*);
                    existing.* = entry;
                    return;
                }
            }

            try parts_list.append(self.allocator, entry);
        } else {
            return error.NoSuchUpload;
        }
    }

    fn listPartsMeta(ctx: *anyopaque, upload_id: []const u8, max_parts: u32, part_marker: u32) anyerror!ListPartsResult {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.parts.get(upload_id)) |parts_list| {
            var result_list: std.ArrayList(PartMeta) = .empty;
            errdefer {
                for (result_list.items) |*p| {
                    self.allocator.free(p.etag);
                    self.allocator.free(p.last_modified);
                }
                result_list.deinit(self.allocator);
            }

            var count: u32 = 0;
            var is_truncated = false;
            var last_part_number: u32 = 0;

            var sorted_parts: std.ArrayList(PartEntry) = .empty;
            defer sorted_parts.deinit(self.allocator);
            try sorted_parts.appendSlice(self.allocator, parts_list.items);
            std.mem.sort(PartEntry, sorted_parts.items, {}, struct {
                fn lessThan(_: void, a: PartEntry, b: PartEntry) bool {
                    return a.part_number < b.part_number;
                }
            }.lessThan);

            for (sorted_parts.items) |entry| {
                if (entry.part_number <= part_marker) continue;

                if (count >= max_parts) {
                    is_truncated = true;
                    break;
                }

                const meta = PartMeta{
                    .part_number = entry.part_number,
                    .etag = try self.dupe(entry.etag),
                    .size = entry.size,
                    .last_modified = try self.dupe(entry.last_modified),
                };

                last_part_number = meta.part_number;
                try result_list.append(self.allocator, meta);
                count += 1;
            }

            return ListPartsResult{
                .parts = try result_list.toOwnedSlice(self.allocator),
                .is_truncated = is_truncated,
                .next_part_number_marker = if (is_truncated) last_part_number else 0,
            };
        }
        return ListPartsResult{
            .parts = &.{},
            .is_truncated = false,
        };
    }

    fn getPartsForCompletion(ctx: *anyopaque, upload_id: []const u8) anyerror![]PartMeta {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.parts.get(upload_id)) |parts_list| {
            var result_list: std.ArrayList(PartMeta) = .empty;
            errdefer {
                for (result_list.items) |*p| {
                    self.allocator.free(p.etag);
                    self.allocator.free(p.last_modified);
                }
                result_list.deinit(self.allocator);
            }

            for (parts_list.items) |entry| {
                const meta = PartMeta{
                    .part_number = entry.part_number,
                    .etag = try self.dupe(entry.etag),
                    .size = entry.size,
                    .last_modified = try self.dupe(entry.last_modified),
                };
                try result_list.append(self.allocator, meta);
            }

            return result_list.toOwnedSlice(self.allocator);
        }
        return &.{};
    }

    fn completeMultipartUpload(ctx: *anyopaque, upload_id: []const u8, object_meta: ObjectMeta) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        try putObjectMeta(ctx, object_meta);

        if (self.uploads.fetchRemove(upload_id)) |removed| {
            self.allocator.free(removed.key);
            self.freeUploadEntry(removed.value);
        }

        if (self.parts.fetchRemove(upload_id)) |removed| {
            self.allocator.free(removed.key);
            for (removed.value.items) |part| {
                self.freePartEntry(part);
            }
            removed.value.deinit();
        }
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
                self.allocator.free(u.owner_id);
                self.allocator.free(u.owner_display);
                if (u.content_encoding) |ce| self.allocator.free(ce);
                if (u.content_language) |cl| self.allocator.free(cl);
                if (u.content_disposition) |cd| self.allocator.free(cd);
                if (u.cache_control) |cc| self.allocator.free(cc);
                if (u.expires) |ex| self.allocator.free(ex);
            }
            uploads_list.deinit(self.allocator);
        }

        var count: u32 = 0;
        var is_truncated = false;

        var iter = self.uploads.iterator();
        while (iter.next()) |entry| {
            if (!std.mem.eql(u8, entry.value_ptr.bucket, bucket)) continue;

            if (prefix.len > 0 and !std.mem.startsWith(u8, entry.value_ptr.key, prefix)) continue;

            if (count >= max_uploads) {
                is_truncated = true;
                break;
            }

            const meta = MultipartUploadMeta{
                .upload_id = try self.dupe(entry.value_ptr.upload_id),
                .bucket = try self.dupe(entry.value_ptr.bucket),
                .key = try self.dupe(entry.value_ptr.key),
                .initiated = try self.dupe(entry.value_ptr.initiated),
                .content_type = try self.dupe(entry.value_ptr.content_type),
                .content_encoding = try self.dupeOpt(entry.value_ptr.content_encoding),
                .content_language = try self.dupeOpt(entry.value_ptr.content_language),
                .content_disposition = try self.dupeOpt(entry.value_ptr.content_disposition),
                .cache_control = try self.dupeOpt(entry.value_ptr.cache_control),
                .expires = try self.dupeOpt(entry.value_ptr.expires),
                .storage_class = try self.dupe(entry.value_ptr.storage_class),
                .acl = try self.dupe(entry.value_ptr.acl),
                .user_metadata = try self.dupe(entry.value_ptr.user_metadata),
                .owner_id = try self.dupe(entry.value_ptr.owner_id),
                .owner_display = try self.dupe(entry.value_ptr.owner_display),
            };

            try uploads_list.append(self.allocator, meta);
            count += 1;
        }

        return ListUploadsResult{
            .uploads = try uploads_list.toOwnedSlice(self.allocator),
            .is_truncated = is_truncated,
        };
    }

    fn getCredential(ctx: *anyopaque, access_key_id: []const u8) anyerror!?Credential {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.credentials.get(access_key_id)) |entry| {
            if (!entry.active) return null;
            return Credential{
                .access_key_id = try self.dupe(entry.access_key_id),
                .secret_key = try self.dupe(entry.secret_key),
                .owner_id = try self.dupe(entry.owner_id),
                .display_name = try self.dupe(entry.display_name),
                .active = entry.active,
                .created_at = try self.dupe(entry.created_at),
            };
        }
        return null;
    }

    fn putCredential(ctx: *anyopaque, cred: Credential) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const key = try self.dupe(cred.access_key_id);
        errdefer self.allocator.free(key);

        if (self.credentials.fetchSwap(key, undefined)) |old| {
            self.allocator.free(old.key);
            self.freeCredentialEntry(old.value);
        }

        const entry = CredentialEntry{
            .access_key_id = try self.dupe(cred.access_key_id),
            .secret_key = try self.dupe(cred.secret_key),
            .owner_id = try self.dupe(cred.owner_id),
            .display_name = try self.dupe(cred.display_name),
            .active = cred.active,
            .created_at = try self.dupe(cred.created_at),
        };

        try self.credentials.put(key, entry);
    }

    fn countBuckets(ctx: *anyopaque) anyerror!u64 {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();
        return @intCast(self.buckets.size);
    }

    fn countObjects(ctx: *anyopaque) anyerror!u64 {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();
        return @intCast(self.objects.size);
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
        return .{
            .ctx = @ptrCast(self),
            .vtable = &vtable,
        };
    }
};

test "MemoryMetadataStore: init and deinit" {
    var ms = MemoryMetadataStore.init(std.testing.allocator);
    defer ms.deinit();
    try std.testing.expectEqual(@as(usize, 0), ms.buckets.size);
}

test "MemoryMetadataStore: create and get bucket" {
    var ms = MemoryMetadataStore.init(std.testing.allocator);
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{
        .name = "test-bucket",
        .creation_date = "2026-01-01T00:00:00.000Z",
        .region = "us-east-1",
        .owner_id = "owner123",
    });

    const bucket = try iface.getBucket("test-bucket");
    try std.testing.expect(bucket != null);
    const b = bucket.?;
    defer b.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("test-bucket", b.name);
    try std.testing.expectEqualStrings("us-east-1", b.region);
}

test "MemoryMetadataStore: bucket exists" {
    var ms = MemoryMetadataStore.init(std.testing.allocator);
    defer ms.deinit();
    const iface = ms.metadataStore();

    try std.testing.expect(!try iface.bucketExists("nonexistent"));

    try iface.createBucket(.{
        .name = "exists-bucket",
        .creation_date = "2026-01-01T00:00:00.000Z",
        .region = "us-east-1",
        .owner_id = "owner",
    });

    try std.testing.expect(try iface.bucketExists("exists-bucket"));
}

test "MemoryMetadataStore: put and get object" {
    var ms = MemoryMetadataStore.init(std.testing.allocator);
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{
        .name = "obj-bucket",
        .creation_date = "2026-01-01T00:00:00.000Z",
        .region = "us-east-1",
        .owner_id = "owner",
    });

    try iface.putObjectMeta(.{
        .bucket = "obj-bucket",
        .key = "test/key.txt",
        .size = 100,
        .etag = "\"abc123\"",
        .content_type = "text/plain",
        .last_modified = "2026-01-01T00:00:00.000Z",
        .storage_class = "STANDARD",
    });

    const obj = try iface.getObjectMeta("obj-bucket", "test/key.txt");
    try std.testing.expect(obj != null);
    const o = obj.?;
    defer {
        std.testing.allocator.free(o.bucket);
        std.testing.allocator.free(o.key);
        std.testing.allocator.free(o.etag);
        std.testing.allocator.free(o.content_type);
        std.testing.allocator.free(o.last_modified);
        std.testing.allocator.free(o.storage_class);
        std.testing.allocator.free(o.acl);
    }
    try std.testing.expectEqual(@as(u64, 100), o.size);
    try std.testing.expectEqualStrings("\"abc123\"", o.etag);
}

test "MemoryMetadataStore: credentials" {
    var ms = MemoryMetadataStore.init(std.testing.allocator);
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.putCredential(.{
        .access_key_id = "test-key",
        .secret_key = "test-secret",
        .owner_id = "owner-1",
        .display_name = "Test User",
        .active = true,
        .created_at = "2026-01-01T00:00:00.000Z",
    });

    const cred = try iface.getCredential("test-key");
    try std.testing.expect(cred != null);
    const c = cred.?;
    defer {
        std.testing.allocator.free(c.access_key_id);
        std.testing.allocator.free(c.secret_key);
        std.testing.allocator.free(c.owner_id);
        std.testing.allocator.free(c.display_name);
        std.testing.allocator.free(c.created_at);
    }
    try std.testing.expectEqualStrings("test-secret", c.secret_key);
}
