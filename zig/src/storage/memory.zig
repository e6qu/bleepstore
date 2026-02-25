const std = @import("std");
const backend = @import("backend.zig");
const StorageBackend = backend.StorageBackend;
const ObjectData = backend.ObjectData;
const PutObjectOptions = backend.PutObjectOptions;
const PutObjectResult = backend.PutObjectResult;
const PartInfo = backend.PartInfo;
const PutPartResult = backend.PutPartResult;
const AssemblePartsResult = backend.AssemblePartsResult;

pub const MemoryBackend = struct {
    allocator: std.mem.Allocator,
    objects: std.StringHashMap(StoredEntry),
    parts: std.StringHashMap(StoredEntry),
    buckets: std.StringHashMap(void),
    current_size: u64,
    max_size_bytes: u64,
    mutex: std.Thread.Mutex,

    const Self = @This();

    const StoredEntry = struct {
        data: []u8,
        etag: []u8,
        content_type: []u8,
    };

    pub fn init(allocator: std.mem.Allocator, max_size_bytes: u64) !Self {
        return Self{
            .allocator = allocator,
            .objects = std.StringHashMap(StoredEntry).init(allocator),
            .parts = std.StringHashMap(StoredEntry).init(allocator),
            .buckets = std.StringHashMap(void).init(allocator),
            .current_size = 0,
            .max_size_bytes = max_size_bytes,
            .mutex = .{},
        };
    }

    pub fn deinit(self: *Self) void {
        // Free all object entries.
        var obj_iter = self.objects.iterator();
        while (obj_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.data);
            self.allocator.free(entry.value_ptr.etag);
            self.allocator.free(entry.value_ptr.content_type);
        }
        self.objects.deinit();

        // Free all part entries.
        var part_iter = self.parts.iterator();
        while (part_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.data);
            self.allocator.free(entry.value_ptr.etag);
            self.allocator.free(entry.value_ptr.content_type);
        }
        self.parts.deinit();

        // Free all bucket names.
        var bucket_iter = self.buckets.iterator();
        while (bucket_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.buckets.deinit();
    }

    /// Compute a quoted MD5 hex ETag for the given data and return an allocated copy.
    fn computeEtag(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
        var md5_hash: [std.crypto.hash.Md5.digest_length]u8 = undefined;
        std.crypto.hash.Md5.hash(data, &md5_hash, .{});
        const hex = std.fmt.bytesToHex(md5_hash, .lower);
        var etag_buf: [34]u8 = undefined;
        const etag = std.fmt.bufPrint(&etag_buf, "\"{s}\"", .{@as([]const u8, &hex)}) catch unreachable;
        return try allocator.dupe(u8, etag);
    }

    /// Build a composite key "bucket/key" for the objects map.
    fn makeObjectKey(allocator: std.mem.Allocator, bucket: []const u8, key: []const u8) ![]u8 {
        return try std.fmt.allocPrint(allocator, "{s}/{s}", .{ bucket, key });
    }

    /// Build a composite key "upload_id/part_number" for the parts map.
    fn makePartKey(allocator: std.mem.Allocator, upload_id: []const u8, part_number: u32) ![]u8 {
        return try std.fmt.allocPrint(allocator, "{s}/{d}", .{ upload_id, part_number });
    }

    /// Free a StoredEntry's owned memory.
    fn freeEntry(self: *Self, entry: StoredEntry) void {
        self.allocator.free(entry.data);
        self.allocator.free(entry.etag);
        self.allocator.free(entry.content_type);
    }

    // --- Vtable implementations ---

    fn putObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8, data: []const u8, opts: PutObjectOptions) anyerror!PutObjectResult {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const composite_key = try makeObjectKey(self.allocator, bucket_name, key);

        // Use a single lookup via getEntry to check for existing key.
        if (self.objects.getEntry(composite_key)) |existing| {
            // Overwrite: free the composite_key we just allocated (map already owns a key).
            self.allocator.free(composite_key);

            const old_data_len: u64 = existing.value_ptr.data.len;

            // Check memory limit.
            const new_size = self.current_size - old_data_len + data.len;
            if (self.max_size_bytes > 0 and new_size > self.max_size_bytes) {
                return error.OutOfMemory;
            }

            // Compute ETag and allocate new data.
            const etag_owned = try computeEtag(self.allocator, data);
            const data_owned = try self.allocator.dupe(u8, data);
            const content_type_owned = try self.allocator.dupe(u8, opts.content_type);

            // Free old entry values.
            self.allocator.free(existing.value_ptr.data);
            self.allocator.free(existing.value_ptr.etag);
            self.allocator.free(existing.value_ptr.content_type);

            existing.value_ptr.* = StoredEntry{
                .data = data_owned,
                .etag = etag_owned,
                .content_type = content_type_owned,
            };

            self.current_size = new_size;

            const etag_result = try self.allocator.dupe(u8, etag_owned);
            return PutObjectResult{ .etag = etag_result };
        } else {
            // New key: check memory limit.
            const new_size = self.current_size + data.len;
            if (self.max_size_bytes > 0 and new_size > self.max_size_bytes) {
                self.allocator.free(composite_key);
                return error.OutOfMemory;
            }

            // Compute ETag and allocate new data.
            const etag_owned = try computeEtag(self.allocator, data);
            const data_owned = try self.allocator.dupe(u8, data);
            const content_type_owned = try self.allocator.dupe(u8, opts.content_type);

            self.objects.put(composite_key, StoredEntry{
                .data = data_owned,
                .etag = etag_owned,
                .content_type = content_type_owned,
            }) catch |err| {
                self.allocator.free(data_owned);
                self.allocator.free(etag_owned);
                self.allocator.free(content_type_owned);
                self.allocator.free(composite_key);
                return err;
            };

            self.current_size = new_size;

            const etag_result = try self.allocator.dupe(u8, etag_owned);
            return PutObjectResult{ .etag = etag_result };
        }
    }

    fn getObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8) anyerror!ObjectData {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const composite_key = try makeObjectKey(self.allocator, bucket_name, key);
        defer self.allocator.free(composite_key);

        const entry = self.objects.get(composite_key) orelse return error.NoSuchKey;

        // Return copies so the caller owns the memory.
        const body = try self.allocator.dupe(u8, entry.data);
        const etag = try self.allocator.dupe(u8, entry.etag);

        return ObjectData{
            .body = body,
            .content_length = entry.data.len,
            .content_type = entry.content_type,
            .etag = etag,
            .last_modified = "2024-01-01T00:00:00.000Z",
        };
    }

    fn deleteObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const composite_key = try makeObjectKey(self.allocator, bucket_name, key);
        defer self.allocator.free(composite_key);

        if (self.objects.fetchRemove(composite_key)) |kv| {
            self.current_size -= kv.value.data.len;
            self.allocator.free(kv.key);
            self.allocator.free(kv.value.data);
            self.allocator.free(kv.value.etag);
            self.allocator.free(kv.value.content_type);
        }
        // Idempotent: no error if key doesn't exist.
    }

    fn headObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8) anyerror!ObjectData {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const composite_key = try makeObjectKey(self.allocator, bucket_name, key);
        defer self.allocator.free(composite_key);

        const entry = self.objects.get(composite_key) orelse return error.NoSuchKey;

        const etag = try self.allocator.dupe(u8, entry.etag);

        return ObjectData{
            .body = null,
            .content_length = entry.data.len,
            .content_type = entry.content_type,
            .etag = etag,
            .last_modified = "2024-01-01T00:00:00.000Z",
        };
    }

    fn copyObject(ctx: *anyopaque, src_bucket: []const u8, src_key: []const u8, dst_bucket: []const u8, dst_key: []const u8) anyerror!PutObjectResult {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        const src_composite = try makeObjectKey(self.allocator, src_bucket, src_key);
        defer self.allocator.free(src_composite);

        const src_entry = self.objects.get(src_composite) orelse return error.NoSuchKey;

        // Copy the source data so we can work with it safely.
        const data_copy = try self.allocator.dupe(u8, src_entry.data);
        defer self.allocator.free(data_copy);
        const content_type_copy = try self.allocator.dupe(u8, src_entry.content_type);
        defer self.allocator.free(content_type_copy);

        // Build destination key.
        const dst_composite = try makeObjectKey(self.allocator, dst_bucket, dst_key);

        // Use a single lookup via getEntry.
        if (self.objects.getEntry(dst_composite)) |existing| {
            // Overwrite: free the dst_composite we just allocated.
            self.allocator.free(dst_composite);

            const old_data_len: u64 = existing.value_ptr.data.len;
            const new_size = self.current_size - old_data_len + data_copy.len;
            if (self.max_size_bytes > 0 and new_size > self.max_size_bytes) {
                return error.OutOfMemory;
            }

            const etag_owned = try computeEtag(self.allocator, data_copy);
            const data_owned = try self.allocator.dupe(u8, data_copy);
            const ct_owned = try self.allocator.dupe(u8, content_type_copy);

            self.allocator.free(existing.value_ptr.data);
            self.allocator.free(existing.value_ptr.etag);
            self.allocator.free(existing.value_ptr.content_type);

            existing.value_ptr.* = StoredEntry{
                .data = data_owned,
                .etag = etag_owned,
                .content_type = ct_owned,
            };

            self.current_size = new_size;

            const etag_result = try self.allocator.dupe(u8, etag_owned);
            return PutObjectResult{ .etag = etag_result };
        } else {
            // New key.
            const new_size = self.current_size + data_copy.len;
            if (self.max_size_bytes > 0 and new_size > self.max_size_bytes) {
                self.allocator.free(dst_composite);
                return error.OutOfMemory;
            }

            const etag_owned = try computeEtag(self.allocator, data_copy);
            const data_owned = try self.allocator.dupe(u8, data_copy);
            const ct_owned = try self.allocator.dupe(u8, content_type_copy);

            self.objects.put(dst_composite, StoredEntry{
                .data = data_owned,
                .etag = etag_owned,
                .content_type = ct_owned,
            }) catch |err| {
                self.allocator.free(data_owned);
                self.allocator.free(etag_owned);
                self.allocator.free(ct_owned);
                self.allocator.free(dst_composite);
                return err;
            };

            self.current_size = new_size;

            const etag_result = try self.allocator.dupe(u8, etag_owned);
            return PutObjectResult{ .etag = etag_result };
        }
    }

    fn putPart(ctx: *anyopaque, bucket: []const u8, upload_id: []const u8, part_number: u32, data: []const u8) anyerror!PutPartResult {
        const self = getSelf(ctx);
        _ = bucket;
        self.mutex.lock();
        defer self.mutex.unlock();

        const part_key = try makePartKey(self.allocator, upload_id, part_number);

        // Use a single lookup via getEntry.
        if (self.parts.getEntry(part_key)) |existing| {
            // Overwrite: free the part_key we just allocated.
            self.allocator.free(part_key);

            const old_data_len: u64 = existing.value_ptr.data.len;
            const new_size = self.current_size - old_data_len + data.len;
            if (self.max_size_bytes > 0 and new_size > self.max_size_bytes) {
                return error.OutOfMemory;
            }

            const etag_owned = try computeEtag(self.allocator, data);
            const data_owned = try self.allocator.dupe(u8, data);
            const ct_owned = try self.allocator.dupe(u8, "application/octet-stream");

            self.allocator.free(existing.value_ptr.data);
            self.allocator.free(existing.value_ptr.etag);
            self.allocator.free(existing.value_ptr.content_type);

            existing.value_ptr.* = StoredEntry{
                .data = data_owned,
                .etag = etag_owned,
                .content_type = ct_owned,
            };

            self.current_size = new_size;

            const etag_result = try self.allocator.dupe(u8, etag_owned);
            return PutPartResult{ .etag = etag_result };
        } else {
            // New part.
            const new_size = self.current_size + data.len;
            if (self.max_size_bytes > 0 and new_size > self.max_size_bytes) {
                self.allocator.free(part_key);
                return error.OutOfMemory;
            }

            const etag_owned = try computeEtag(self.allocator, data);
            const data_owned = try self.allocator.dupe(u8, data);
            const ct_owned = try self.allocator.dupe(u8, "application/octet-stream");

            self.parts.put(part_key, StoredEntry{
                .data = data_owned,
                .etag = etag_owned,
                .content_type = ct_owned,
            }) catch |err| {
                self.allocator.free(data_owned);
                self.allocator.free(etag_owned);
                self.allocator.free(ct_owned);
                self.allocator.free(part_key);
                return err;
            };

            self.current_size = new_size;

            const etag_result = try self.allocator.dupe(u8, etag_owned);
            return PutPartResult{ .etag = etag_result };
        }
    }

    fn assembleParts(ctx: *anyopaque, bucket: []const u8, key: []const u8, upload_id: []const u8, parts_list: []const PartInfo) anyerror!AssemblePartsResult {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        // Concatenate all part data.
        var total_size: u64 = 0;
        var assembled = std.ArrayList(u8).empty;
        defer assembled.deinit(self.allocator);

        // Also collect binary MD5s for composite ETag.
        var md5_concat = std.ArrayList(u8).empty;
        defer md5_concat.deinit(self.allocator);

        for (parts_list) |part| {
            const pk = try makePartKey(self.allocator, upload_id, part.part_number);
            defer self.allocator.free(pk);

            const entry = self.parts.get(pk) orelse return error.InvalidPart;

            try assembled.appendSlice(self.allocator, entry.data);
            total_size += entry.data.len;

            // Parse part ETag hex to binary MD5 for composite ETag.
            var etag_hex: []const u8 = part.etag;
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

        // Check memory limit for the assembled object.
        // Note: parts will be removed from current_size by deleteParts later,
        // but the assembled object adds to it.
        const assembled_data = assembled.items;

        // Build the composite key for the assembled object.
        const composite_key = try makeObjectKey(self.allocator, bucket, key);

        // Compute composite ETag: MD5 of concatenated binary MD5s, formatted as "hex-N".
        var composite_hash: [std.crypto.hash.Md5.digest_length]u8 = undefined;
        std.crypto.hash.Md5.hash(md5_concat.items, &composite_hash, .{});
        const composite_hex = std.fmt.bytesToHex(composite_hash, .lower);

        // Use a single lookup via getEntry.
        if (self.objects.getEntry(composite_key)) |existing| {
            // Overwrite: free the composite_key we just allocated.
            self.allocator.free(composite_key);

            const old_data_len: u64 = existing.value_ptr.data.len;
            const new_size = self.current_size - old_data_len + assembled_data.len;
            if (self.max_size_bytes > 0 and new_size > self.max_size_bytes) {
                return error.OutOfMemory;
            }

            const etag_owned = try std.fmt.allocPrint(self.allocator, "\"{s}-{d}\"", .{ @as([]const u8, &composite_hex), parts_list.len });
            const data_owned = try self.allocator.dupe(u8, assembled_data);
            const ct_owned = try self.allocator.dupe(u8, "application/octet-stream");

            self.allocator.free(existing.value_ptr.data);
            self.allocator.free(existing.value_ptr.etag);
            self.allocator.free(existing.value_ptr.content_type);

            existing.value_ptr.* = StoredEntry{
                .data = data_owned,
                .etag = etag_owned,
                .content_type = ct_owned,
            };

            self.current_size = new_size;

            const etag_result = try self.allocator.dupe(u8, etag_owned);
            return AssemblePartsResult{ .etag = etag_result, .total_size = total_size };
        } else {
            // New key.
            const new_size = self.current_size + assembled_data.len;
            if (self.max_size_bytes > 0 and new_size > self.max_size_bytes) {
                self.allocator.free(composite_key);
                return error.OutOfMemory;
            }

            const etag_owned = try std.fmt.allocPrint(self.allocator, "\"{s}-{d}\"", .{ @as([]const u8, &composite_hex), parts_list.len });
            const data_owned = try self.allocator.dupe(u8, assembled_data);
            const ct_owned = try self.allocator.dupe(u8, "application/octet-stream");

            self.objects.put(composite_key, StoredEntry{
                .data = data_owned,
                .etag = etag_owned,
                .content_type = ct_owned,
            }) catch |err| {
                self.allocator.free(data_owned);
                self.allocator.free(etag_owned);
                self.allocator.free(ct_owned);
                self.allocator.free(composite_key);
                return err;
            };

            self.current_size = new_size;

            const etag_result = try self.allocator.dupe(u8, etag_owned);
            return AssemblePartsResult{ .etag = etag_result, .total_size = total_size };
        }
    }

    fn deleteParts(ctx: *anyopaque, bucket: []const u8, upload_id: []const u8) anyerror!void {
        const self = getSelf(ctx);
        _ = bucket;
        self.mutex.lock();
        defer self.mutex.unlock();

        // Build the prefix to match: "upload_id/"
        const prefix = try std.fmt.allocPrint(self.allocator, "{s}/", .{upload_id});
        defer self.allocator.free(prefix);

        // Collect keys to remove (cannot mutate map while iterating).
        var keys_to_remove = std.ArrayList([]const u8).empty;
        defer keys_to_remove.deinit(self.allocator);

        var iter = self.parts.iterator();
        while (iter.next()) |entry| {
            if (std.mem.startsWith(u8, entry.key_ptr.*, prefix)) {
                try keys_to_remove.append(self.allocator, entry.key_ptr.*);
            }
        }

        for (keys_to_remove.items) |k| {
            if (self.parts.fetchRemove(k)) |kv| {
                self.current_size -= kv.value.data.len;
                self.allocator.free(kv.key);
                self.allocator.free(kv.value.data);
                self.allocator.free(kv.value.etag);
                self.allocator.free(kv.value.content_type);
            }
        }
    }

    fn createBucket(ctx: *anyopaque, bucket: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.buckets.contains(bucket)) return;

        const bucket_owned = try self.allocator.dupe(u8, bucket);
        self.buckets.put(bucket_owned, {}) catch |err| {
            self.allocator.free(bucket_owned);
            return err;
        };
    }

    fn deleteBucket(ctx: *anyopaque, bucket: []const u8) anyerror!void {
        const self = getSelf(ctx);
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.buckets.fetchRemove(bucket)) |kv| {
            self.allocator.free(kv.key);
        }
    }

    fn healthCheck(ctx: *anyopaque) anyerror!void {
        const self = getSelf(ctx);
        _ = self;
        // Memory backend is always healthy.
    }

    // --- vtable + interface ---

    const vtable = StorageBackend.VTable{
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

    /// Obtain a StorageBackend interface backed by this memory implementation.
    pub fn storageBackend(self: *Self) StorageBackend {
        return .{
            .ctx = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    fn getSelf(ctx: *anyopaque) *Self {
        return @ptrCast(@alignCast(ctx));
    }
};

// =========================================================================
// Tests
// =========================================================================

test "MemoryBackend: putObject computes correct MD5 ETag" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    try sb.createBucket("test-bucket");

    const data = "test content";
    const result = try sb.putObject("test-bucket", "test.txt", data, .{});
    defer allocator.free(result.etag);

    // Expected MD5 of "test content"
    var expected_md5: [std.crypto.hash.Md5.digest_length]u8 = undefined;
    std.crypto.hash.Md5.hash(data, &expected_md5, .{});
    const expected_hex = std.fmt.bytesToHex(expected_md5, .lower);
    var expected_etag_buf: [34]u8 = undefined;
    const expected_etag = std.fmt.bufPrint(&expected_etag_buf, "\"{s}\"", .{@as([]const u8, &expected_hex)}) catch unreachable;

    try std.testing.expectEqualStrings(expected_etag, result.etag);
}

test "MemoryBackend: put, get, delete lifecycle" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    try sb.createBucket("mybucket");

    // Put
    const result = try sb.putObject("mybucket", "hello.txt", "hello world", .{});
    defer allocator.free(result.etag);
    try std.testing.expect(result.etag.len > 0);

    // Get
    const obj = try sb.getObject("mybucket", "hello.txt");
    defer allocator.free(obj.body.?);
    defer allocator.free(obj.etag);
    try std.testing.expectEqualStrings("hello world", obj.body.?);
    try std.testing.expectEqual(@as(u64, 11), obj.content_length);

    // Delete
    try sb.deleteObject("mybucket", "hello.txt");

    // Get after delete should fail
    try std.testing.expectError(error.NoSuchKey, sb.getObject("mybucket", "hello.txt"));
}

test "MemoryBackend: delete nonexistent object is idempotent" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    try sb.createBucket("emptybucket");

    // Should not error
    try sb.deleteObject("emptybucket", "nonexistent.txt");
}

test "MemoryBackend: headObject returns metadata without body" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    try sb.createBucket("head-bucket");

    const result = try sb.putObject("head-bucket", "file.txt", "some data", .{});
    defer allocator.free(result.etag);

    const obj = try sb.headObject("head-bucket", "file.txt");
    defer allocator.free(obj.etag);

    try std.testing.expect(obj.body == null);
    try std.testing.expectEqual(@as(u64, 9), obj.content_length);
    try std.testing.expect(obj.etag.len > 0);
}

test "MemoryBackend: headObject on missing key returns NoSuchKey" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    try std.testing.expectError(error.NoSuchKey, sb.headObject("no-bucket", "no-key"));
}

test "MemoryBackend: copyObject" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    try sb.createBucket("src-bucket");
    try sb.createBucket("dst-bucket");

    const put_result = try sb.putObject("src-bucket", "original.txt", "copy me", .{});
    defer allocator.free(put_result.etag);

    const copy_result = try sb.copyObject("src-bucket", "original.txt", "dst-bucket", "copied.txt");
    defer allocator.free(copy_result.etag);

    const obj = try sb.getObject("dst-bucket", "copied.txt");
    defer allocator.free(obj.body.?);
    defer allocator.free(obj.etag);
    try std.testing.expectEqualStrings("copy me", obj.body.?);
}

test "MemoryBackend: copyObject from missing source returns NoSuchKey" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    try std.testing.expectError(error.NoSuchKey, sb.copyObject("a", "missing", "b", "dest"));
}

test "MemoryBackend: putObject overwrite updates data" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    try sb.createBucket("overwrite-bucket");

    const result1 = try sb.putObject("overwrite-bucket", "key.txt", "version 1", .{});
    defer allocator.free(result1.etag);

    const result2 = try sb.putObject("overwrite-bucket", "key.txt", "version 2", .{});
    defer allocator.free(result2.etag);

    const obj = try sb.getObject("overwrite-bucket", "key.txt");
    defer allocator.free(obj.body.?);
    defer allocator.free(obj.etag);
    try std.testing.expectEqualStrings("version 2", obj.body.?);
}

test "MemoryBackend: memory limit enforcement" {
    const allocator = std.testing.allocator;

    // Set max_size_bytes to 20 bytes.
    var mb = try MemoryBackend.init(allocator, 20);
    defer mb.deinit();
    const sb = mb.storageBackend();

    try sb.createBucket("limited");

    // First put: 10 bytes, should succeed.
    const r1 = try sb.putObject("limited", "a.txt", "0123456789", .{});
    defer allocator.free(r1.etag);

    // Second put: 11 bytes, total would be 21 > 20, should fail.
    try std.testing.expectError(error.OutOfMemory, sb.putObject("limited", "b.txt", "01234567890", .{}));

    // Second put: 10 bytes, total would be 20 == 20, should succeed.
    const r2 = try sb.putObject("limited", "b.txt", "0123456789", .{});
    defer allocator.free(r2.etag);
}

test "MemoryBackend: putPart and deleteParts lifecycle" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    try sb.createBucket("mp-bucket");

    // Put two parts.
    const result1 = try sb.putPart("mp-bucket", "upload-abc", 1, "part one data");
    defer allocator.free(result1.etag);
    try std.testing.expect(result1.etag.len > 0);
    try std.testing.expect(result1.etag[0] == '"');

    const result2 = try sb.putPart("mp-bucket", "upload-abc", 2, "part two data");
    defer allocator.free(result2.etag);
    try std.testing.expect(result2.etag.len > 0);

    // Delete parts.
    try sb.deleteParts("mp-bucket", "upload-abc");

    // Parts should be gone now — no way to verify directly, but size should be 0.
    try std.testing.expectEqual(@as(u64, 0), mb.current_size);
}

test "MemoryBackend: deleteParts is idempotent" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    // Deleting parts for a nonexistent upload should not error.
    try sb.deleteParts("mybucket", "nonexistent-upload");
}

test "MemoryBackend: assembleParts basic" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    try sb.createBucket("assemble-bucket");

    // Put two parts.
    const result1 = try sb.putPart("assemble-bucket", "upload-asm", 1, "hello ");
    defer allocator.free(result1.etag);
    const result2 = try sb.putPart("assemble-bucket", "upload-asm", 2, "world");
    defer allocator.free(result2.etag);

    // Assemble parts.
    const parts = [_]backend.PartInfo{
        .{ .part_number = 1, .etag = result1.etag },
        .{ .part_number = 2, .etag = result2.etag },
    };
    const asm_result = try sb.assembleParts("assemble-bucket", "test.txt", "upload-asm", &parts);
    defer allocator.free(asm_result.etag);

    // Verify the assembled object.
    const obj = try sb.getObject("assemble-bucket", "test.txt");
    defer allocator.free(obj.body.?);
    defer allocator.free(obj.etag);
    try std.testing.expectEqualStrings("hello world", obj.body.?);
    try std.testing.expectEqual(@as(u64, 11), asm_result.total_size);

    // Verify composite ETag format: "hex-2"
    try std.testing.expect(asm_result.etag.len > 0);
    try std.testing.expect(asm_result.etag[0] == '"');
    try std.testing.expect(asm_result.etag[asm_result.etag.len - 1] == '"');
    try std.testing.expect(std.mem.indexOf(u8, asm_result.etag, "-2\"") != null);

    // Clean up.
    try sb.deleteParts("assemble-bucket", "upload-asm");
}

test "MemoryBackend: putPart overwrites existing part" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    try sb.createBucket("overwrite-bucket");

    // Put a part.
    const result1 = try sb.putPart("overwrite-bucket", "upload-xyz", 1, "original data");
    defer allocator.free(result1.etag);

    // Overwrite with new data.
    const result2 = try sb.putPart("overwrite-bucket", "upload-xyz", 1, "new data");
    defer allocator.free(result2.etag);

    // ETags should be different.
    try std.testing.expect(!std.mem.eql(u8, result1.etag, result2.etag));

    // Size should reflect the new data only.
    try std.testing.expectEqual(@as(u64, 8), mb.current_size); // "new data" = 8 bytes

    // Clean up.
    try sb.deleteParts("overwrite-bucket", "upload-xyz");
}

test "MemoryBackend: healthCheck always succeeds" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    try sb.healthCheck();
}

test "MemoryBackend: createBucket and deleteBucket" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    // Create a bucket.
    try sb.createBucket("my-bucket");

    // Creating the same bucket again should be idempotent.
    try sb.createBucket("my-bucket");

    // Delete the bucket.
    try sb.deleteBucket("my-bucket");

    // Deleting a nonexistent bucket should be idempotent.
    try sb.deleteBucket("nonexistent-bucket");
}

test "MemoryBackend: current_size tracks correctly across operations" {
    const allocator = std.testing.allocator;

    var mb = try MemoryBackend.init(allocator, 0);
    defer mb.deinit();
    const sb = mb.storageBackend();

    try sb.createBucket("size-bucket");

    try std.testing.expectEqual(@as(u64, 0), mb.current_size);

    // Put 5 bytes.
    const r1 = try sb.putObject("size-bucket", "a.txt", "hello", .{});
    defer allocator.free(r1.etag);
    try std.testing.expectEqual(@as(u64, 5), mb.current_size);

    // Put 6 bytes.
    const r2 = try sb.putObject("size-bucket", "b.txt", "world!", .{});
    defer allocator.free(r2.etag);
    try std.testing.expectEqual(@as(u64, 11), mb.current_size);

    // Overwrite a.txt with 3 bytes.
    const r3 = try sb.putObject("size-bucket", "a.txt", "hey", .{});
    defer allocator.free(r3.etag);
    try std.testing.expectEqual(@as(u64, 9), mb.current_size);

    // Delete b.txt.
    try sb.deleteObject("size-bucket", "b.txt");
    try std.testing.expectEqual(@as(u64, 3), mb.current_size);

    // Delete a.txt.
    try sb.deleteObject("size-bucket", "a.txt");
    try std.testing.expectEqual(@as(u64, 0), mb.current_size);
}
