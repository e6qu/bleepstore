const std = @import("std");
const backend = @import("backend.zig");
const StorageBackend = backend.StorageBackend;
const ObjectData = backend.ObjectData;
const PutObjectOptions = backend.PutObjectOptions;
const PutObjectResult = backend.PutObjectResult;
const PartInfo = backend.PartInfo;
const PutPartResult = backend.PutPartResult;
const AssemblePartsResult = backend.AssemblePartsResult;

pub const LocalBackend = struct {
    allocator: std.mem.Allocator,
    root_path: []const u8,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, root_path: []const u8) !Self {
        // Ensure the root directory exists.
        std.fs.cwd().makePath(root_path) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };

        // Ensure the .tmp directory exists for atomic writes.
        const tmp_dir = try std.fs.path.join(allocator, &.{ root_path, ".tmp" });
        defer allocator.free(tmp_dir);
        std.fs.cwd().makePath(tmp_dir) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };

        // Ensure the .multipart directory exists for part storage.
        const mp_dir = try std.fs.path.join(allocator, &.{ root_path, ".multipart" });
        defer allocator.free(mp_dir);
        std.fs.cwd().makePath(mp_dir) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };

        // Crash-only startup: clean stale temp files.
        cleanTempDir(allocator, root_path);

        return Self{
            .allocator = allocator,
            .root_path = root_path,
        };
    }

    pub fn deinit(self: *Self) void {
        _ = self;
    }

    /// Crash-only startup: remove all files in <root>/.tmp/
    fn cleanTempDir(allocator: std.mem.Allocator, root_path: []const u8) void {
        const tmp_dir = std.fs.path.join(allocator, &.{ root_path, ".tmp" }) catch return;
        defer allocator.free(tmp_dir);

        var dir = std.fs.cwd().openDir(tmp_dir, .{ .iterate = true }) catch return;
        defer dir.close();

        var iter = dir.iterate();
        while (iter.next() catch null) |entry| {
            if (entry.kind == .file) {
                dir.deleteFile(entry.name) catch {};
            }
        }
    }

    // --- Vtable implementations ---

    fn putObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8, data: []const u8, opts: PutObjectOptions) anyerror!PutObjectResult {
        const self = getSelf(ctx);
        _ = opts;

        // Build path: <root>/<bucket>/<key>
        const dir_path = try std.fs.path.join(self.allocator, &.{ self.root_path, bucket_name });
        defer self.allocator.free(dir_path);

        // Ensure the directory for the key exists (keys can have slashes).
        if (std.fs.path.dirname(key)) |key_dir| {
            const full_dir = try std.fs.path.join(self.allocator, &.{ dir_path, key_dir });
            defer self.allocator.free(full_dir);
            std.fs.cwd().makePath(full_dir) catch |err| {
                if (err != error.PathAlreadyExists) return err;
            };
        } else {
            std.fs.cwd().makePath(dir_path) catch |err| {
                if (err != error.PathAlreadyExists) return err;
            };
        }

        // Compute MD5 ETag.
        var md5_hash: [std.crypto.hash.Md5.digest_length]u8 = undefined;
        std.crypto.hash.Md5.hash(data, &md5_hash, .{});
        const hex = std.fmt.bytesToHex(md5_hash, .lower);
        var etag_buf: [34]u8 = undefined; // "hex_md5" = 1 + 32 + 1
        const etag = std.fmt.bufPrint(&etag_buf, "\"{s}\"", .{@as([]const u8, &hex)}) catch unreachable;
        const etag_owned = try self.allocator.dupe(u8, etag);

        // Atomic write: temp file + fsync + rename.
        // Write to <root>/.tmp/<random>
        var random_bytes: [8]u8 = undefined;
        std.crypto.random.bytes(&random_bytes);
        const random_hex = std.fmt.bytesToHex(random_bytes, .lower);

        const tmp_path = try std.fs.path.join(self.allocator, &.{ self.root_path, ".tmp", &random_hex });
        defer self.allocator.free(tmp_path);

        const final_path = try std.fs.path.join(self.allocator, &.{ dir_path, key });
        defer self.allocator.free(final_path);

        // Write to temp file.
        const tmp_file = try std.fs.cwd().createFile(tmp_path, .{});
        errdefer {
            tmp_file.close();
            std.fs.cwd().deleteFile(tmp_path) catch {};
        }

        try tmp_file.writeAll(data);

        // fsync the data to disk.
        try tmp_file.sync();
        tmp_file.close();

        // Rename temp file to final path (atomic on POSIX).
        try std.fs.cwd().rename(tmp_path, final_path);

        return PutObjectResult{
            .etag = etag_owned,
        };
    }

    fn getObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8) anyerror!ObjectData {
        const self = getSelf(ctx);

        const file_path = try std.fs.path.join(self.allocator, &.{ self.root_path, bucket_name, key });
        defer self.allocator.free(file_path);

        const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
            return switch (err) {
                error.FileNotFound => error.NoSuchKey,
                else => err,
            };
        };
        defer file.close();

        const stat = try file.stat();
        const body = try file.readToEndAlloc(self.allocator, std.math.maxInt(usize));

        return ObjectData{
            .body = body,
            .content_length = stat.size,
            .content_type = "application/octet-stream",
            .etag = "",
            .last_modified = "",
        };
    }

    fn deleteObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8) anyerror!void {
        const self = getSelf(ctx);

        const file_path = try std.fs.path.join(self.allocator, &.{ self.root_path, bucket_name, key });
        defer self.allocator.free(file_path);

        std.fs.cwd().deleteFile(file_path) catch |err| {
            // Idempotent: ignore FileNotFound.
            if (err != error.FileNotFound) return err;
        };
    }

    fn headObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8) anyerror!ObjectData {
        const self = getSelf(ctx);

        const file_path = try std.fs.path.join(self.allocator, &.{ self.root_path, bucket_name, key });
        defer self.allocator.free(file_path);

        const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
            return switch (err) {
                error.FileNotFound => error.NoSuchKey,
                else => err,
            };
        };
        defer file.close();

        const stat = try file.stat();

        return ObjectData{
            .body = null,
            .content_length = stat.size,
            .content_type = "application/octet-stream",
            .etag = "",
            .last_modified = "",
        };
    }

    fn copyObject(ctx: *anyopaque, src_bucket: []const u8, src_key: []const u8, dst_bucket: []const u8, dst_key: []const u8) anyerror!PutObjectResult {
        const self = getSelf(ctx);

        const src_path = try std.fs.path.join(self.allocator, &.{ self.root_path, src_bucket, src_key });
        defer self.allocator.free(src_path);

        const dst_dir = try std.fs.path.join(self.allocator, &.{ self.root_path, dst_bucket });
        defer self.allocator.free(dst_dir);

        std.fs.cwd().makePath(dst_dir) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };

        const dst_path = try std.fs.path.join(self.allocator, &.{ self.root_path, dst_bucket, dst_key });
        defer self.allocator.free(dst_path);

        try std.fs.cwd().copyFile(src_path, std.fs.cwd(), dst_path, .{});

        return PutObjectResult{
            .etag = "",
        };
    }

    fn putPart(ctx: *anyopaque, bucket: []const u8, upload_id: []const u8, part_number: u32, data: []const u8) anyerror!PutPartResult {
        const self = getSelf(ctx);
        _ = bucket;

        // Compute MD5 ETag.
        var md5_hash: [std.crypto.hash.Md5.digest_length]u8 = undefined;
        std.crypto.hash.Md5.hash(data, &md5_hash, .{});
        const hex = std.fmt.bytesToHex(md5_hash, .lower);
        var etag_buf: [34]u8 = undefined;
        const etag = std.fmt.bufPrint(&etag_buf, "\"{s}\"", .{@as([]const u8, &hex)}) catch unreachable;
        const etag_owned = try self.allocator.dupe(u8, etag);

        // Build part directory: <root>/.multipart/<upload_id>/
        const part_dir = try std.fs.path.join(self.allocator, &.{ self.root_path, ".multipart", upload_id });
        defer self.allocator.free(part_dir);
        std.fs.cwd().makePath(part_dir) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };

        // Format part number as string for filename.
        var pn_buf: [10]u8 = undefined;
        const pn_str = std.fmt.bufPrint(&pn_buf, "{d}", .{part_number}) catch unreachable;

        // Atomic write: temp file + fsync + rename.
        var random_bytes: [8]u8 = undefined;
        std.crypto.random.bytes(&random_bytes);
        const random_hex = std.fmt.bytesToHex(random_bytes, .lower);

        const tmp_path = try std.fs.path.join(self.allocator, &.{ self.root_path, ".tmp", &random_hex });
        defer self.allocator.free(tmp_path);

        const final_path = try std.fs.path.join(self.allocator, &.{ part_dir, pn_str });
        defer self.allocator.free(final_path);

        // Write to temp file.
        const tmp_file = try std.fs.cwd().createFile(tmp_path, .{});
        errdefer {
            tmp_file.close();
            std.fs.cwd().deleteFile(tmp_path) catch {};
        }

        try tmp_file.writeAll(data);
        try tmp_file.sync();
        tmp_file.close();

        // Rename temp file to final path (atomic on POSIX).
        try std.fs.cwd().rename(tmp_path, final_path);

        return PutPartResult{
            .etag = etag_owned,
        };
    }

    fn assembleParts(ctx: *anyopaque, bucket: []const u8, key: []const u8, upload_id: []const u8, parts: []const PartInfo) anyerror!AssemblePartsResult {
        const self = getSelf(ctx);

        // Build the final object path: <root>/<bucket>/<key>
        const dir_path = try std.fs.path.join(self.allocator, &.{ self.root_path, bucket });
        defer self.allocator.free(dir_path);

        // Ensure the directory for the key exists (keys can have slashes).
        if (std.fs.path.dirname(key)) |key_dir| {
            const full_dir = try std.fs.path.join(self.allocator, &.{ dir_path, key_dir });
            defer self.allocator.free(full_dir);
            std.fs.cwd().makePath(full_dir) catch |err| {
                if (err != error.PathAlreadyExists) return err;
            };
        } else {
            std.fs.cwd().makePath(dir_path) catch |err| {
                if (err != error.PathAlreadyExists) return err;
            };
        }

        // Atomic write: assemble into temp file, fsync, rename to final path.
        var random_bytes: [8]u8 = undefined;
        std.crypto.random.bytes(&random_bytes);
        const random_hex = std.fmt.bytesToHex(random_bytes, .lower);

        const tmp_path = try std.fs.path.join(self.allocator, &.{ self.root_path, ".tmp", &random_hex });
        defer self.allocator.free(tmp_path);

        const final_path = try std.fs.path.join(self.allocator, &.{ dir_path, key });
        defer self.allocator.free(final_path);

        // Open the temp file for writing.
        const tmp_file = try std.fs.cwd().createFile(tmp_path, .{});
        errdefer {
            tmp_file.close();
            std.fs.cwd().deleteFile(tmp_path) catch {};
        }

        var total_size: u64 = 0;

        // For each part in order, read from the part file and write to output.
        const part_base_dir = try std.fs.path.join(self.allocator, &.{ self.root_path, ".multipart", upload_id });
        defer self.allocator.free(part_base_dir);

        var read_buf: [65536]u8 = undefined; // 64KB read buffer

        for (parts) |part| {
            var pn_buf: [10]u8 = undefined;
            const pn_str = std.fmt.bufPrint(&pn_buf, "{d}", .{part.part_number}) catch unreachable;

            const part_path = try std.fs.path.join(self.allocator, &.{ part_base_dir, pn_str });
            defer self.allocator.free(part_path);

            const part_file = std.fs.cwd().openFile(part_path, .{}) catch |err| {
                return switch (err) {
                    error.FileNotFound => error.InvalidPart,
                    else => err,
                };
            };
            defer part_file.close();

            // Stream part data to output.
            while (true) {
                const bytes_read = try part_file.read(&read_buf);
                if (bytes_read == 0) break;
                try tmp_file.writeAll(read_buf[0..bytes_read]);
                total_size += bytes_read;
            }
        }

        // fsync the assembled data to disk.
        try tmp_file.sync();
        tmp_file.close();

        // Rename temp file to final path (atomic on POSIX).
        try std.fs.cwd().rename(tmp_path, final_path);

        // Compute the composite ETag: MD5 of concatenated binary MD5s.
        // For each part, parse the hex ETag to 16 bytes of binary MD5.
        var md5_concat = std.ArrayList(u8).empty;
        defer md5_concat.deinit(self.allocator);

        for (parts) |part| {
            // Strip quotes from ETag: "hex" -> hex
            var etag_hex = part.etag;
            if (etag_hex.len >= 2 and etag_hex[0] == '"' and etag_hex[etag_hex.len - 1] == '"') {
                etag_hex = etag_hex[1 .. etag_hex.len - 1];
            }

            // Parse 32 hex chars to 16 bytes.
            if (etag_hex.len != 32) continue; // skip malformed etags
            var md5_bytes: [16]u8 = undefined;
            for (0..16) |i| {
                md5_bytes[i] = std.fmt.parseInt(u8, etag_hex[i * 2 .. i * 2 + 2], 16) catch 0;
            }
            try md5_concat.appendSlice(self.allocator, &md5_bytes);
        }

        // Compute MD5 of the concatenated binary MD5s.
        var composite_hash: [std.crypto.hash.Md5.digest_length]u8 = undefined;
        std.crypto.hash.Md5.hash(md5_concat.items, &composite_hash, .{});

        // Format as "hex-N" where N is the number of parts.
        const composite_hex = std.fmt.bytesToHex(composite_hash, .lower);
        const etag = try std.fmt.allocPrint(self.allocator, "\"{s}-{d}\"", .{ @as([]const u8, &composite_hex), parts.len });

        return AssemblePartsResult{
            .etag = etag,
            .total_size = total_size,
        };
    }

    fn deleteParts(ctx: *anyopaque, bucket: []const u8, upload_id: []const u8) anyerror!void {
        const self = getSelf(ctx);
        _ = bucket;

        // Delete the entire <root>/.multipart/<upload_id>/ directory tree.
        const part_dir = try std.fs.path.join(self.allocator, &.{ self.root_path, ".multipart", upload_id });
        defer self.allocator.free(part_dir);

        // Idempotent: deleteTree does not fail on non-existent paths,
        // but catch any errors that do occur.
        std.fs.cwd().deleteTree(part_dir) catch {};
    }

    fn createBucket(ctx: *anyopaque, bucket: []const u8) anyerror!void {
        const self = getSelf(ctx);

        const bucket_dir = try std.fs.path.join(self.allocator, &.{ self.root_path, bucket });
        defer self.allocator.free(bucket_dir);

        std.fs.cwd().makePath(bucket_dir) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };
    }

    fn deleteBucket(ctx: *anyopaque, bucket: []const u8) anyerror!void {
        const self = getSelf(ctx);

        const bucket_dir = try std.fs.path.join(self.allocator, &.{ self.root_path, bucket });
        defer self.allocator.free(bucket_dir);

        // Try to delete the directory. If it's not empty or doesn't exist, that's OK.
        std.fs.cwd().deleteDir(bucket_dir) catch {};
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
    };

    /// Obtain a StorageBackend interface backed by this local implementation.
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

test "LocalBackend: putObject computes correct MD5 ETag" {
    const allocator = std.testing.allocator;

    // Create a temp directory for the test
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const tmp_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmp_path);

    var lb = try LocalBackend.init(allocator, tmp_path);
    defer lb.deinit();
    const sb = lb.storageBackend();

    // Create bucket directory
    try sb.createBucket("test-bucket");

    // Put an object
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

test "LocalBackend: put, get, delete lifecycle" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const tmp_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmp_path);

    var lb = try LocalBackend.init(allocator, tmp_path);
    defer lb.deinit();
    const sb = lb.storageBackend();

    try sb.createBucket("mybucket");

    // Put
    const result = try sb.putObject("mybucket", "hello.txt", "hello world", .{});
    defer allocator.free(result.etag);
    try std.testing.expect(result.etag.len > 0);

    // Get
    const obj = try sb.getObject("mybucket", "hello.txt");
    defer allocator.free(obj.body.?);
    try std.testing.expectEqualStrings("hello world", obj.body.?);
    try std.testing.expectEqual(@as(u64, 11), obj.content_length);

    // Delete
    try sb.deleteObject("mybucket", "hello.txt");

    // Get after delete should fail
    try std.testing.expectError(error.NoSuchKey, sb.getObject("mybucket", "hello.txt"));
}

test "LocalBackend: delete nonexistent object is idempotent" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const tmp_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmp_path);

    var lb = try LocalBackend.init(allocator, tmp_path);
    defer lb.deinit();
    const sb = lb.storageBackend();

    try sb.createBucket("emptybucket");

    // Should not error
    try sb.deleteObject("emptybucket", "nonexistent.txt");
}

test "LocalBackend: putObject with nested key" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const tmp_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmp_path);

    var lb = try LocalBackend.init(allocator, tmp_path);
    defer lb.deinit();
    const sb = lb.storageBackend();

    try sb.createBucket("nested-bucket");

    const result = try sb.putObject("nested-bucket", "a/b/c/file.txt", "nested content", .{});
    defer allocator.free(result.etag);

    const obj = try sb.getObject("nested-bucket", "a/b/c/file.txt");
    defer allocator.free(obj.body.?);
    try std.testing.expectEqualStrings("nested content", obj.body.?);
}

test "LocalBackend: putPart and deleteParts lifecycle" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const tmp_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmp_path);

    var lb = try LocalBackend.init(allocator, tmp_path);
    defer lb.deinit();
    const sb = lb.storageBackend();

    try sb.createBucket("mp-bucket");

    // Put two parts.
    const result1 = try sb.putPart("mp-bucket", "upload-abc", 1, "part one data");
    defer allocator.free(result1.etag);
    try std.testing.expect(result1.etag.len > 0);
    try std.testing.expect(result1.etag[0] == '"');

    const result2 = try sb.putPart("mp-bucket", "upload-abc", 2, "part two data");
    defer allocator.free(result2.etag);
    try std.testing.expect(result2.etag.len > 0);

    // Verify part files exist.
    const part1_path = try std.fs.path.join(allocator, &.{ tmp_path, ".multipart", "upload-abc", "1" });
    defer allocator.free(part1_path);
    const part2_path = try std.fs.path.join(allocator, &.{ tmp_path, ".multipart", "upload-abc", "2" });
    defer allocator.free(part2_path);

    // Read part file to verify contents.
    const part1_file = try std.fs.cwd().openFile(part1_path, .{});
    defer part1_file.close();
    const part1_body = try part1_file.readToEndAlloc(allocator, 1024);
    defer allocator.free(part1_body);
    try std.testing.expectEqualStrings("part one data", part1_body);

    // Delete parts.
    try sb.deleteParts("mp-bucket", "upload-abc");

    // Part directory should be gone. Verify by attempting to open a part file.
    const result = std.fs.cwd().openFile(part1_path, .{});
    try std.testing.expectError(error.FileNotFound, result);
}

test "LocalBackend: deleteParts is idempotent" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const tmp_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmp_path);

    var lb = try LocalBackend.init(allocator, tmp_path);
    defer lb.deinit();
    const sb = lb.storageBackend();

    // Deleting parts for a nonexistent upload should not error.
    try sb.deleteParts("mybucket", "nonexistent-upload");
}

test "LocalBackend: assembleParts basic" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const tmp_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmp_path);

    var lb = try LocalBackend.init(allocator, tmp_path);
    defer lb.deinit();
    const sb = lb.storageBackend();

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

test "LocalBackend: putPart overwrites existing part" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const tmp_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmp_path);

    var lb = try LocalBackend.init(allocator, tmp_path);
    defer lb.deinit();
    const sb = lb.storageBackend();

    try sb.createBucket("overwrite-bucket");

    // Put a part.
    const result1 = try sb.putPart("overwrite-bucket", "upload-xyz", 1, "original data");
    defer allocator.free(result1.etag);

    // Overwrite with new data.
    const result2 = try sb.putPart("overwrite-bucket", "upload-xyz", 1, "new data");
    defer allocator.free(result2.etag);

    // ETags should be different.
    try std.testing.expect(!std.mem.eql(u8, result1.etag, result2.etag));

    // Read the part file to verify it contains the new data.
    const part_path = try std.fs.path.join(allocator, &.{ tmp_path, ".multipart", "upload-xyz", "1" });
    defer allocator.free(part_path);
    const file = try std.fs.cwd().openFile(part_path, .{});
    defer file.close();
    const body = try file.readToEndAlloc(allocator, 1024);
    defer allocator.free(body);
    try std.testing.expectEqualStrings("new data", body);

    // Clean up.
    try sb.deleteParts("overwrite-bucket", "upload-xyz");
}
