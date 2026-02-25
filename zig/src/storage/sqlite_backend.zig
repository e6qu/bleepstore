const std = @import("std");
const backend = @import("backend.zig");
const StorageBackend = backend.StorageBackend;
const ObjectData = backend.ObjectData;
const PutObjectOptions = backend.PutObjectOptions;
const PutObjectResult = backend.PutObjectResult;
const PartInfo = backend.PartInfo;
const PutPartResult = backend.PutPartResult;
const AssemblePartsResult = backend.AssemblePartsResult;

const c = @cImport({
    @cInclude("sqlite3.h");
});

/// Bind text with SQLITE_TRANSIENT semantics (SQLite copies the data).
/// We use an extern declaration with isize for the destructor parameter to
/// avoid Zig 0.15's alignment check on function pointers (SQLITE_TRANSIENT
/// is -1 cast to a function pointer, which has no valid alignment).
extern "c" fn sqlite3_bind_text(stmt: *c.sqlite3_stmt, index: c_int, text: [*]const u8, len: c_int, destructor: isize) c_int;

/// Bind blob with SQLITE_TRANSIENT semantics (SQLite copies the data).
extern "c" fn sqlite3_bind_blob(stmt: *c.sqlite3_stmt, index: c_int, data: ?[*]const u8, len: c_int, destructor: isize) c_int;

pub const SqliteBackend = struct {
    allocator: std.mem.Allocator,
    db: *c.sqlite3,

    const Self = @This();

    /// Open (or create) the SQLite database and initialize the schema for object/part storage.
    pub fn init(allocator: std.mem.Allocator, db_path: []const u8) !Self {
        // Create a null-terminated copy of the path for the C API.
        const path_z = try allocator.dupeZ(u8, db_path);
        defer allocator.free(path_z);

        var db: ?*c.sqlite3 = null;
        const rc = c.sqlite3_open_v2(
            path_z.ptr,
            &db,
            c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE | c.SQLITE_OPEN_NOMUTEX,
            null,
        );
        if (rc != c.SQLITE_OK) {
            if (db) |d| _ = c.sqlite3_close(d);
            return error.SqliteOpenFailed;
        }

        const valid_db = db orelse return error.SqliteOpenFailed;

        var self = Self{
            .allocator = allocator,
            .db = valid_db,
        };
        try self.applyPragmas();
        try self.initSchema();
        return self;
    }

    pub fn deinit(self: *Self) void {
        _ = c.sqlite3_close(self.db);
    }

    /// Apply SQLite PRAGMAs for performance and safety.
    fn applyPragmas(self: *Self) !void {
        try self.execSql("PRAGMA journal_mode=WAL;");
        try self.execSql("PRAGMA busy_timeout=5000;");
    }

    /// Create tables if they do not exist.
    fn initSchema(self: *Self) !void {
        try self.execSql(
            \\CREATE TABLE IF NOT EXISTS object_data (
            \\    bucket TEXT NOT NULL,
            \\    key TEXT NOT NULL,
            \\    data BLOB NOT NULL,
            \\    etag TEXT NOT NULL,
            \\    PRIMARY KEY (bucket, key)
            \\);
        );

        try self.execSql(
            \\CREATE TABLE IF NOT EXISTS part_data (
            \\    upload_id TEXT NOT NULL,
            \\    part_number INTEGER NOT NULL,
            \\    data BLOB NOT NULL,
            \\    etag TEXT NOT NULL,
            \\    PRIMARY KEY (upload_id, part_number)
            \\);
        );
    }

    // =========================================================================
    // Vtable implementations
    // =========================================================================

    fn putObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8, data: []const u8, opts: PutObjectOptions) anyerror!PutObjectResult {
        const self = getSelf(ctx);
        _ = opts;

        // Compute MD5 ETag.
        var md5_hash: [std.crypto.hash.Md5.digest_length]u8 = undefined;
        std.crypto.hash.Md5.hash(data, &md5_hash, .{});
        const hex = std.fmt.bytesToHex(md5_hash, .lower);
        var etag_buf: [34]u8 = undefined;
        const etag = std.fmt.bufPrint(&etag_buf, "\"{s}\"", .{@as([]const u8, &hex)}) catch unreachable;

        // INSERT OR REPLACE into object_data.
        const sql =
            \\INSERT OR REPLACE INTO object_data (bucket, key, data, etag)
            \\VALUES (?1, ?2, ?3, ?4);
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        self.bindText(stmt, 1, bucket_name);
        self.bindText(stmt, 2, key);
        self.bindBlob(stmt, 3, data);
        self.bindText(stmt, 4, etag);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }

        const etag_owned = try self.allocator.dupe(u8, etag);
        return PutObjectResult{
            .etag = etag_owned,
        };
    }

    fn getObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8) anyerror!ObjectData {
        const self = getSelf(ctx);

        const sql = "SELECT data, etag FROM object_data WHERE bucket = ?1 AND key = ?2;";
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        self.bindText(stmt, 1, bucket_name);
        self.bindText(stmt, 2, key);

        if (c.sqlite3_step(stmt) != c.SQLITE_ROW) {
            return error.NoSuchKey;
        }

        // Read BLOB data.
        const blob_ptr = c.sqlite3_column_blob(stmt, 0);
        const blob_len: usize = @intCast(c.sqlite3_column_bytes(stmt, 0));
        const body = if (blob_ptr) |ptr| blk: {
            const slice: [*]const u8 = @ptrCast(ptr);
            break :blk try self.allocator.dupe(u8, slice[0..blob_len]);
        } else blk: {
            break :blk try self.allocator.dupe(u8, "");
        };

        // Read etag text.
        const etag_owned = try self.columnTextDup(stmt, 1);

        return ObjectData{
            .body = body,
            .content_length = @intCast(blob_len),
            .content_type = "application/octet-stream",
            .etag = etag_owned,
            .last_modified = "",
        };
    }

    fn deleteObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8) anyerror!void {
        const self = getSelf(ctx);

        const sql = "DELETE FROM object_data WHERE bucket = ?1 AND key = ?2;";
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        self.bindText(stmt, 1, bucket_name);
        self.bindText(stmt, 2, key);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }
        // Idempotent: no error if row didn't exist.
    }

    fn headObject(ctx: *anyopaque, bucket_name: []const u8, key: []const u8) anyerror!ObjectData {
        const self = getSelf(ctx);

        const sql = "SELECT length(data), etag FROM object_data WHERE bucket = ?1 AND key = ?2;";
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        self.bindText(stmt, 1, bucket_name);
        self.bindText(stmt, 2, key);

        if (c.sqlite3_step(stmt) != c.SQLITE_ROW) {
            return error.NoSuchKey;
        }

        const content_length: u64 = @intCast(c.sqlite3_column_int64(stmt, 0));
        const etag_owned = try self.columnTextDup(stmt, 1);

        return ObjectData{
            .body = null,
            .content_length = content_length,
            .content_type = "application/octet-stream",
            .etag = etag_owned,
            .last_modified = "",
        };
    }

    fn copyObject(ctx: *anyopaque, src_bucket: []const u8, src_key: []const u8, dst_bucket: []const u8, dst_key: []const u8) anyerror!PutObjectResult {
        const self = getSelf(ctx);

        // Read source object data and etag.
        const select_sql = "SELECT data, etag FROM object_data WHERE bucket = ?1 AND key = ?2;";
        const select_stmt = try self.prepareStmt(select_sql);
        defer self.finalizeStmt(select_stmt);

        self.bindText(select_stmt, 1, src_bucket);
        self.bindText(select_stmt, 2, src_key);

        if (c.sqlite3_step(select_stmt) != c.SQLITE_ROW) {
            return error.NoSuchKey;
        }

        // Read source BLOB.
        const blob_ptr = c.sqlite3_column_blob(select_stmt, 0);
        const blob_len: usize = @intCast(c.sqlite3_column_bytes(select_stmt, 0));

        // We need to copy the blob data before finalizing the select statement,
        // because sqlite3_column_blob returns a pointer into sqlite's internal buffer.
        const data_copy = if (blob_ptr) |ptr| blk: {
            const slice: [*]const u8 = @ptrCast(ptr);
            break :blk try self.allocator.dupe(u8, slice[0..blob_len]);
        } else blk: {
            break :blk try self.allocator.dupe(u8, "");
        };
        defer self.allocator.free(data_copy);

        // Compute new ETag from source data.
        var md5_hash: [std.crypto.hash.Md5.digest_length]u8 = undefined;
        std.crypto.hash.Md5.hash(data_copy, &md5_hash, .{});
        const hex = std.fmt.bytesToHex(md5_hash, .lower);
        var etag_buf: [34]u8 = undefined;
        const etag = std.fmt.bufPrint(&etag_buf, "\"{s}\"", .{@as([]const u8, &hex)}) catch unreachable;

        // Insert destination.
        const insert_sql =
            \\INSERT OR REPLACE INTO object_data (bucket, key, data, etag)
            \\VALUES (?1, ?2, ?3, ?4);
        ;
        const insert_stmt = try self.prepareStmt(insert_sql);
        defer self.finalizeStmt(insert_stmt);

        self.bindText(insert_stmt, 1, dst_bucket);
        self.bindText(insert_stmt, 2, dst_key);
        self.bindBlob(insert_stmt, 3, data_copy);
        self.bindText(insert_stmt, 4, etag);

        const rc = c.sqlite3_step(insert_stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }

        const etag_owned = try self.allocator.dupe(u8, etag);
        return PutObjectResult{
            .etag = etag_owned,
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

        const sql =
            \\INSERT OR REPLACE INTO part_data (upload_id, part_number, data, etag)
            \\VALUES (?1, ?2, ?3, ?4);
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        self.bindText(stmt, 1, upload_id);
        self.bindInt64(stmt, 2, @intCast(part_number));
        self.bindBlob(stmt, 3, data);
        self.bindText(stmt, 4, etag);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }

        const etag_owned = try self.allocator.dupe(u8, etag);
        return PutPartResult{
            .etag = etag_owned,
        };
    }

    fn assembleParts(ctx: *anyopaque, bucket: []const u8, key: []const u8, upload_id: []const u8, parts: []const PartInfo) anyerror!AssemblePartsResult {
        const self = getSelf(ctx);

        // Collect all part data in order, concatenating into a single buffer.
        var assembled = std.ArrayList(u8).empty;
        defer assembled.deinit(self.allocator);

        for (parts) |part| {
            const select_sql = "SELECT data FROM part_data WHERE upload_id = ?1 AND part_number = ?2;";
            const select_stmt = try self.prepareStmt(select_sql);
            defer self.finalizeStmt(select_stmt);

            self.bindText(select_stmt, 1, upload_id);
            self.bindInt64(select_stmt, 2, @intCast(part.part_number));

            if (c.sqlite3_step(select_stmt) != c.SQLITE_ROW) {
                return error.InvalidPart;
            }

            const blob_ptr = c.sqlite3_column_blob(select_stmt, 0);
            const blob_len: usize = @intCast(c.sqlite3_column_bytes(select_stmt, 0));
            if (blob_ptr) |ptr| {
                const slice: [*]const u8 = @ptrCast(ptr);
                try assembled.appendSlice(self.allocator, slice[0..blob_len]);
            }
        }

        const total_size: u64 = @intCast(assembled.items.len);

        // Compute the composite ETag: MD5 of concatenated binary MD5s + "-N".
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

        var composite_hash: [std.crypto.hash.Md5.digest_length]u8 = undefined;
        std.crypto.hash.Md5.hash(md5_concat.items, &composite_hash, .{});
        const composite_hex = std.fmt.bytesToHex(composite_hash, .lower);
        const composite_etag = try std.fmt.allocPrint(self.allocator, "\"{s}-{d}\"", .{ @as([]const u8, &composite_hex), parts.len });
        errdefer self.allocator.free(composite_etag);

        // Insert assembled object into object_data.
        const insert_sql =
            \\INSERT OR REPLACE INTO object_data (bucket, key, data, etag)
            \\VALUES (?1, ?2, ?3, ?4);
        ;
        const insert_stmt = try self.prepareStmt(insert_sql);
        defer self.finalizeStmt(insert_stmt);

        self.bindText(insert_stmt, 1, bucket);
        self.bindText(insert_stmt, 2, key);
        self.bindBlob(insert_stmt, 3, assembled.items);
        self.bindText(insert_stmt, 4, composite_etag);

        const rc = c.sqlite3_step(insert_stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }

        return AssemblePartsResult{
            .etag = composite_etag,
            .total_size = total_size,
        };
    }

    fn deleteParts(ctx: *anyopaque, bucket: []const u8, upload_id: []const u8) anyerror!void {
        const self = getSelf(ctx);
        _ = bucket;

        const sql = "DELETE FROM part_data WHERE upload_id = ?1;";
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        self.bindText(stmt, 1, upload_id);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }
        // Idempotent: no error if no rows deleted.
    }

    fn createBucket(ctx: *anyopaque, bucket: []const u8) anyerror!void {
        // SqliteBackend stores objects in a flat table keyed by (bucket, key).
        // No physical bucket creation needed. This is a no-op.
        _ = ctx;
        _ = bucket;
    }

    fn deleteBucket(ctx: *anyopaque, bucket: []const u8) anyerror!void {
        // No physical bucket to delete. Objects are keyed by (bucket, key) in the table.
        // Deletion of objects is handled separately. This is a no-op.
        _ = ctx;
        _ = bucket;
    }

    fn healthCheck(ctx: *anyopaque) anyerror!void {
        const self = getSelf(ctx);

        const sql = "SELECT 1;";
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_ROW) {
            return error.StorageUnavailable;
        }
    }

    // =========================================================================
    // Vtable + interface
    // =========================================================================

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

    /// Obtain a StorageBackend interface backed by this SQLite implementation.
    pub fn storageBackend(self: *Self) StorageBackend {
        return .{
            .ctx = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    fn getSelf(ctx: *anyopaque) *Self {
        return @ptrCast(@alignCast(ctx));
    }

    // =========================================================================
    // SQLite helper functions
    // =========================================================================

    /// Execute a simple SQL statement (no parameters, no results).
    fn execSql(self: *Self, sql: [*:0]const u8) !void {
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(self.db, sql, null, null, &err_msg);
        if (rc != c.SQLITE_OK) {
            if (err_msg) |msg| c.sqlite3_free(msg);
            return error.SqliteExecFailed;
        }
    }

    /// Prepare a SQL statement.
    fn prepareStmt(self: *Self, sql: [*:0]const u8) !*c.sqlite3_stmt {
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) {
            return error.SqlitePrepareFailed;
        }
        return stmt orelse return error.SqlitePrepareFailed;
    }

    /// Finalize a statement (always succeeds, even if already finalized).
    fn finalizeStmt(_: *Self, stmt: *c.sqlite3_stmt) void {
        _ = c.sqlite3_finalize(stmt);
    }

    /// Bind a text parameter. Uses SQLITE_TRANSIENT (-1) so SQLite copies the data.
    fn bindText(_: *Self, stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) void {
        _ = sqlite3_bind_text(stmt, index, value.ptr, @intCast(value.len), -1);
    }

    /// Bind a blob parameter. Uses SQLITE_TRANSIENT (-1) so SQLite copies the data.
    /// For empty blobs, we bind a zero-length blob with a non-null pointer to avoid
    /// SQLite treating it as SQL NULL (which would violate NOT NULL constraints).
    fn bindBlob(_: *Self, stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) void {
        // value.ptr is always valid for a Zig slice, even when len == 0.
        _ = sqlite3_bind_blob(stmt, index, value.ptr, @intCast(value.len), -1);
    }

    /// Bind an integer parameter.
    fn bindInt64(_: *Self, stmt: *c.sqlite3_stmt, index: c_int, value: i64) void {
        _ = c.sqlite3_bind_int64(stmt, index, value);
    }

    /// Read a non-null text column and duplicate it into the allocator.
    fn columnTextDup(self: *Self, stmt: *c.sqlite3_stmt, col: c_int) ![]const u8 {
        const raw = c.sqlite3_column_text(stmt, col);
        if (raw == null) {
            return try self.allocator.dupe(u8, "");
        }
        const len: usize = @intCast(c.sqlite3_column_bytes(stmt, col));
        const slice: []const u8 = raw[0..len];
        return try self.allocator.dupe(u8, slice);
    }
};

// =========================================================================
// Tests
// =========================================================================

test "SqliteBackend: putObject computes correct MD5 ETag" {
    const allocator = std.testing.allocator;

    var sb_impl = try SqliteBackend.init(allocator, ":memory:");
    defer sb_impl.deinit();
    const sb = sb_impl.storageBackend();

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

test "SqliteBackend: put, get, delete lifecycle" {
    const allocator = std.testing.allocator;

    var sb_impl = try SqliteBackend.init(allocator, ":memory:");
    defer sb_impl.deinit();
    const sb = sb_impl.storageBackend();

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

test "SqliteBackend: delete nonexistent object is idempotent" {
    const allocator = std.testing.allocator;

    var sb_impl = try SqliteBackend.init(allocator, ":memory:");
    defer sb_impl.deinit();
    const sb = sb_impl.storageBackend();

    try sb.createBucket("emptybucket");

    // Should not error
    try sb.deleteObject("emptybucket", "nonexistent.txt");
}

test "SqliteBackend: headObject returns correct metadata" {
    const allocator = std.testing.allocator;

    var sb_impl = try SqliteBackend.init(allocator, ":memory:");
    defer sb_impl.deinit();
    const sb = sb_impl.storageBackend();

    try sb.createBucket("head-bucket");

    const data = "head test data";
    const put_result = try sb.putObject("head-bucket", "head.txt", data, .{});
    defer allocator.free(put_result.etag);

    const obj = try sb.headObject("head-bucket", "head.txt");
    defer allocator.free(obj.etag);
    try std.testing.expect(obj.body == null);
    try std.testing.expectEqual(@as(u64, 14), obj.content_length);
    try std.testing.expectEqualStrings(put_result.etag, obj.etag);
}

test "SqliteBackend: headObject returns NoSuchKey for missing object" {
    const allocator = std.testing.allocator;

    var sb_impl = try SqliteBackend.init(allocator, ":memory:");
    defer sb_impl.deinit();
    const sb = sb_impl.storageBackend();

    try std.testing.expectError(error.NoSuchKey, sb.headObject("anybucket", "missing.txt"));
}

test "SqliteBackend: copyObject" {
    const allocator = std.testing.allocator;

    var sb_impl = try SqliteBackend.init(allocator, ":memory:");
    defer sb_impl.deinit();
    const sb = sb_impl.storageBackend();

    try sb.createBucket("src-bucket");
    try sb.createBucket("dst-bucket");

    const put_result = try sb.putObject("src-bucket", "original.txt", "copy me", .{});
    defer allocator.free(put_result.etag);

    const copy_result = try sb.copyObject("src-bucket", "original.txt", "dst-bucket", "copied.txt");
    defer allocator.free(copy_result.etag);

    // Verify destination exists with correct content.
    const obj = try sb.getObject("dst-bucket", "copied.txt");
    defer allocator.free(obj.body.?);
    defer allocator.free(obj.etag);
    try std.testing.expectEqualStrings("copy me", obj.body.?);

    // ETags should match (same content).
    try std.testing.expectEqualStrings(put_result.etag, copy_result.etag);
}

test "SqliteBackend: copyObject returns NoSuchKey for missing source" {
    const allocator = std.testing.allocator;

    var sb_impl = try SqliteBackend.init(allocator, ":memory:");
    defer sb_impl.deinit();
    const sb = sb_impl.storageBackend();

    try std.testing.expectError(error.NoSuchKey, sb.copyObject("src", "missing", "dst", "out"));
}

test "SqliteBackend: putPart and deleteParts lifecycle" {
    const allocator = std.testing.allocator;

    var sb_impl = try SqliteBackend.init(allocator, ":memory:");
    defer sb_impl.deinit();
    const sb = sb_impl.storageBackend();

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
}

test "SqliteBackend: deleteParts is idempotent" {
    const allocator = std.testing.allocator;

    var sb_impl = try SqliteBackend.init(allocator, ":memory:");
    defer sb_impl.deinit();
    const sb = sb_impl.storageBackend();

    // Deleting parts for a nonexistent upload should not error.
    try sb.deleteParts("mybucket", "nonexistent-upload");
}

test "SqliteBackend: assembleParts basic" {
    const allocator = std.testing.allocator;

    var sb_impl = try SqliteBackend.init(allocator, ":memory:");
    defer sb_impl.deinit();
    const sb = sb_impl.storageBackend();

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

test "SqliteBackend: putPart overwrites existing part" {
    const allocator = std.testing.allocator;

    var sb_impl = try SqliteBackend.init(allocator, ":memory:");
    defer sb_impl.deinit();
    const sb = sb_impl.storageBackend();

    try sb.createBucket("overwrite-bucket");

    // Put a part.
    const result1 = try sb.putPart("overwrite-bucket", "upload-xyz", 1, "original data");
    defer allocator.free(result1.etag);

    // Overwrite with new data.
    const result2 = try sb.putPart("overwrite-bucket", "upload-xyz", 1, "new data");
    defer allocator.free(result2.etag);

    // ETags should be different.
    try std.testing.expect(!std.mem.eql(u8, result1.etag, result2.etag));
}

test "SqliteBackend: healthCheck succeeds" {
    const allocator = std.testing.allocator;

    var sb_impl = try SqliteBackend.init(allocator, ":memory:");
    defer sb_impl.deinit();
    const sb = sb_impl.storageBackend();

    try sb.healthCheck();
}

test "SqliteBackend: putObject with empty data" {
    const allocator = std.testing.allocator;

    var sb_impl = try SqliteBackend.init(allocator, ":memory:");
    defer sb_impl.deinit();
    const sb = sb_impl.storageBackend();

    try sb.createBucket("empty-bucket");

    const result = try sb.putObject("empty-bucket", "empty.txt", "", .{});
    defer allocator.free(result.etag);

    const obj = try sb.getObject("empty-bucket", "empty.txt");
    defer allocator.free(obj.body.?);
    defer allocator.free(obj.etag);
    try std.testing.expectEqualStrings("", obj.body.?);
    try std.testing.expectEqual(@as(u64, 0), obj.content_length);
}

test "SqliteBackend: putObject overwrites existing object" {
    const allocator = std.testing.allocator;

    var sb_impl = try SqliteBackend.init(allocator, ":memory:");
    defer sb_impl.deinit();
    const sb = sb_impl.storageBackend();

    try sb.createBucket("overwrite-bucket");

    const result1 = try sb.putObject("overwrite-bucket", "file.txt", "version 1", .{});
    defer allocator.free(result1.etag);

    const result2 = try sb.putObject("overwrite-bucket", "file.txt", "version 2", .{});
    defer allocator.free(result2.etag);

    const obj = try sb.getObject("overwrite-bucket", "file.txt");
    defer allocator.free(obj.body.?);
    defer allocator.free(obj.etag);
    try std.testing.expectEqualStrings("version 2", obj.body.?);
}
