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

const c = @cImport({
    @cInclude("sqlite3.h");
});

/// Bind text with SQLITE_TRANSIENT semantics (SQLite copies the data).
/// We use an extern declaration with isize for the destructor parameter to
/// avoid Zig 0.15's alignment check on function pointers (SQLITE_TRANSIENT
/// is -1 cast to a function pointer, which has no valid alignment).
extern "c" fn sqlite3_bind_text(stmt: *c.sqlite3_stmt, index: c_int, text: [*]const u8, len: c_int, destructor: isize) c_int;

pub const SqliteMetadataStore = struct {
    allocator: std.mem.Allocator,
    db: ?*c.sqlite3,

    const Self = @This();

    /// Open (or create) the SQLite database and initialize the schema.
    pub fn init(allocator: std.mem.Allocator, db_path: [*:0]const u8) !Self {
        var db: ?*c.sqlite3 = null;
        const rc = c.sqlite3_open(db_path, &db);
        if (rc != c.SQLITE_OK) {
            if (db) |d| _ = c.sqlite3_close(d);
            return error.SqliteOpenFailed;
        }

        var self = Self{
            .allocator = allocator,
            .db = db,
        };
        try self.applyPragmas();
        try self.initSchema();
        return self;
    }

    pub fn deinit(self: *Self) void {
        if (self.db) |db| {
            _ = c.sqlite3_close(db);
            self.db = null;
        }
    }

    /// Apply SQLite PRAGMAs for performance and safety.
    fn applyPragmas(self: *Self) !void {
        try self.execSql("PRAGMA journal_mode = WAL;");
        try self.execSql("PRAGMA synchronous = NORMAL;");
        try self.execSql("PRAGMA foreign_keys = ON;");
        try self.execSql("PRAGMA busy_timeout = 5000;");
    }

    /// Create tables and indexes if they do not exist.
    fn initSchema(self: *Self) !void {
        try self.execSql(
            \\CREATE TABLE IF NOT EXISTS schema_version (
            \\    version INTEGER PRIMARY KEY,
            \\    applied_at TEXT NOT NULL
            \\);
        );

        try self.execSql(
            \\CREATE TABLE IF NOT EXISTS buckets (
            \\    name TEXT PRIMARY KEY,
            \\    created_at TEXT NOT NULL,
            \\    region TEXT NOT NULL DEFAULT 'us-east-1',
            \\    owner_id TEXT NOT NULL DEFAULT '',
            \\    owner_display TEXT NOT NULL DEFAULT '',
            \\    acl TEXT NOT NULL DEFAULT '{}'
            \\);
        );

        try self.execSql(
            \\CREATE TABLE IF NOT EXISTS objects (
            \\    bucket TEXT NOT NULL,
            \\    key TEXT NOT NULL,
            \\    size INTEGER NOT NULL,
            \\    etag TEXT NOT NULL,
            \\    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
            \\    content_encoding TEXT,
            \\    content_language TEXT,
            \\    content_disposition TEXT,
            \\    cache_control TEXT,
            \\    expires TEXT,
            \\    storage_class TEXT NOT NULL DEFAULT 'STANDARD',
            \\    acl TEXT NOT NULL DEFAULT '{}',
            \\    user_metadata TEXT NOT NULL DEFAULT '{}',
            \\    last_modified TEXT NOT NULL,
            \\    delete_marker INTEGER NOT NULL DEFAULT 0,
            \\    PRIMARY KEY (bucket, key),
            \\    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
            \\);
        );

        try self.execSql(
            \\CREATE INDEX IF NOT EXISTS idx_objects_bucket ON objects(bucket);
        );
        try self.execSql(
            \\CREATE INDEX IF NOT EXISTS idx_objects_bucket_prefix ON objects(bucket, key);
        );

        try self.execSql(
            \\CREATE TABLE IF NOT EXISTS multipart_uploads (
            \\    upload_id TEXT PRIMARY KEY,
            \\    bucket TEXT NOT NULL,
            \\    key TEXT NOT NULL,
            \\    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
            \\    content_encoding TEXT,
            \\    content_language TEXT,
            \\    content_disposition TEXT,
            \\    cache_control TEXT,
            \\    expires TEXT,
            \\    storage_class TEXT NOT NULL DEFAULT 'STANDARD',
            \\    acl TEXT NOT NULL DEFAULT '{}',
            \\    user_metadata TEXT NOT NULL DEFAULT '{}',
            \\    owner_id TEXT NOT NULL DEFAULT '',
            \\    owner_display TEXT NOT NULL DEFAULT '',
            \\    initiated_at TEXT NOT NULL,
            \\    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
            \\);
        );

        try self.execSql(
            \\CREATE INDEX IF NOT EXISTS idx_uploads_bucket ON multipart_uploads(bucket);
        );
        try self.execSql(
            \\CREATE INDEX IF NOT EXISTS idx_uploads_bucket_key ON multipart_uploads(bucket, key);
        );

        try self.execSql(
            \\CREATE TABLE IF NOT EXISTS multipart_parts (
            \\    upload_id TEXT NOT NULL,
            \\    part_number INTEGER NOT NULL,
            \\    size INTEGER NOT NULL,
            \\    etag TEXT NOT NULL,
            \\    last_modified TEXT NOT NULL,
            \\    PRIMARY KEY (upload_id, part_number),
            \\    FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
            \\);
        );

        try self.execSql(
            \\CREATE TABLE IF NOT EXISTS credentials (
            \\    access_key_id TEXT PRIMARY KEY,
            \\    secret_key TEXT NOT NULL,
            \\    owner_id TEXT NOT NULL,
            \\    display_name TEXT NOT NULL DEFAULT '',
            \\    active INTEGER NOT NULL DEFAULT 1,
            \\    created_at TEXT NOT NULL
            \\);
        );

        // Insert initial schema version if not present
        try self.execSql(
            \\INSERT OR IGNORE INTO schema_version (version, applied_at)
            \\VALUES (1, datetime('now'));
        );
    }

    // =========================================================================
    // Bucket operations
    // =========================================================================

    fn createBucket(ctx: *anyopaque, meta: BucketMeta) anyerror!void {
        const self = getSelf(ctx);
        const sql =
            \\INSERT INTO buckets (name, created_at, region, owner_id, owner_display, acl)
            \\VALUES (?1, ?2, ?3, ?4, ?5, ?6);
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        self.bindText(stmt, 1, meta.name);
        self.bindText(stmt, 2, meta.creation_date);
        self.bindText(stmt, 3, meta.region);
        self.bindText(stmt, 4, meta.owner_id);
        self.bindText(stmt, 5, meta.owner_display);
        self.bindText(stmt, 6, meta.acl);

        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_CONSTRAINT) {
            return error.BucketAlreadyExists;
        }
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }
    }

    fn deleteBucket(ctx: *anyopaque, name: []const u8) anyerror!void {
        const self = getSelf(ctx);

        // Check if bucket has objects (BucketNotEmpty)
        const count_sql = "SELECT COUNT(*) FROM objects WHERE bucket = ?1;";
        const count_stmt = try self.prepareStmt(count_sql);
        defer self.finalizeStmt(count_stmt);
        self.bindText(count_stmt, 1, name);
        if (c.sqlite3_step(count_stmt) == c.SQLITE_ROW) {
            const obj_count = c.sqlite3_column_int64(count_stmt, 0);
            if (obj_count > 0) return error.BucketNotEmpty;
        }

        const sql = "DELETE FROM buckets WHERE name = ?1;";
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, name);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }

        // Check that a row was actually deleted
        if (c.sqlite3_changes(self.db) == 0) {
            return error.NoSuchBucket;
        }
    }

    fn getBucket(ctx: *anyopaque, name: []const u8) anyerror!?BucketMeta {
        const self = getSelf(ctx);
        const sql =
            \\SELECT name, created_at, region, owner_id, owner_display, acl
            \\FROM buckets WHERE name = ?1;
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, name);

        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            return BucketMeta{
                .name = try self.columnTextDup(stmt, 0),
                .creation_date = try self.columnTextDup(stmt, 1),
                .region = try self.columnTextDup(stmt, 2),
                .owner_id = try self.columnTextDup(stmt, 3),
                .owner_display = try self.columnTextDup(stmt, 4),
                .acl = try self.columnTextDup(stmt, 5),
            };
        }
        return null;
    }

    fn listBuckets(ctx: *anyopaque) anyerror![]BucketMeta {
        const self = getSelf(ctx);
        const sql =
            \\SELECT name, created_at, region, owner_id, owner_display, acl
            \\FROM buckets ORDER BY name;
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        var list: std.ArrayList(BucketMeta) = .empty;
        errdefer {
            for (list.items) |*item| {
                item.deinit(self.allocator);
            }
            list.deinit(self.allocator);
        }

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const bucket = BucketMeta{
                .name = try self.columnTextDup(stmt, 0),
                .creation_date = try self.columnTextDup(stmt, 1),
                .region = try self.columnTextDup(stmt, 2),
                .owner_id = try self.columnTextDup(stmt, 3),
                .owner_display = try self.columnTextDup(stmt, 4),
                .acl = try self.columnTextDup(stmt, 5),
            };
            try list.append(self.allocator, bucket);
        }

        return list.toOwnedSlice(self.allocator);
    }

    fn bucketExists(ctx: *anyopaque, name: []const u8) anyerror!bool {
        const self = getSelf(ctx);
        const sql = "SELECT 1 FROM buckets WHERE name = ?1;";
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, name);

        return c.sqlite3_step(stmt) == c.SQLITE_ROW;
    }

    fn updateBucketAcl(ctx: *anyopaque, name: []const u8, acl: []const u8) anyerror!void {
        const self = getSelf(ctx);
        const sql = "UPDATE buckets SET acl = ?1 WHERE name = ?2;";
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, acl);
        self.bindText(stmt, 2, name);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }
        if (c.sqlite3_changes(self.db) == 0) {
            return error.NoSuchBucket;
        }
    }

    // =========================================================================
    // Object operations
    // =========================================================================

    fn putObjectMeta(ctx: *anyopaque, meta: ObjectMeta) anyerror!void {
        const self = getSelf(ctx);
        const sql =
            \\INSERT OR REPLACE INTO objects
            \\    (bucket, key, size, etag, content_type, content_encoding,
            \\     content_language, content_disposition, cache_control, expires,
            \\     storage_class, acl, user_metadata, last_modified, delete_marker)
            \\VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15);
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        self.bindText(stmt, 1, meta.bucket);
        self.bindText(stmt, 2, meta.key);
        self.bindInt64(stmt, 3, @intCast(meta.size));
        self.bindText(stmt, 4, meta.etag);
        self.bindText(stmt, 5, meta.content_type);
        self.bindOptionalText(stmt, 6, meta.content_encoding);
        self.bindOptionalText(stmt, 7, meta.content_language);
        self.bindOptionalText(stmt, 8, meta.content_disposition);
        self.bindOptionalText(stmt, 9, meta.cache_control);
        self.bindOptionalText(stmt, 10, meta.expires);
        self.bindText(stmt, 11, meta.storage_class);
        self.bindText(stmt, 12, meta.acl);
        self.bindOptionalText(stmt, 13, meta.user_metadata);
        self.bindText(stmt, 14, meta.last_modified);
        self.bindInt64(stmt, 15, if (meta.delete_marker) 1 else 0);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }
    }

    fn getObjectMeta(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!?ObjectMeta {
        const self = getSelf(ctx);
        const sql =
            \\SELECT bucket, key, size, etag, content_type, content_encoding,
            \\       content_language, content_disposition, cache_control, expires,
            \\       storage_class, acl, user_metadata, last_modified, delete_marker
            \\FROM objects WHERE bucket = ?1 AND key = ?2;
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, bucket);
        self.bindText(stmt, 2, key);

        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            return try self.readObjectMetaFromRow(stmt);
        }
        return null;
    }

    fn deleteObjectMeta(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!bool {
        const self = getSelf(ctx);
        const sql = "DELETE FROM objects WHERE bucket = ?1 AND key = ?2;";
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, bucket);
        self.bindText(stmt, 2, key);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }
        return c.sqlite3_changes(self.db) > 0;
    }

    fn deleteObjectsMeta(ctx: *anyopaque, bucket: []const u8, keys: []const []const u8) anyerror![]bool {
        const self = getSelf(ctx);
        const results = try self.allocator.alloc(bool, keys.len);
        errdefer self.allocator.free(results);

        // S3 always reports all keys as deleted, so init all to true.
        @memset(results, true);

        if (keys.len == 0) return results;

        // Batch delete using DELETE ... WHERE key IN (?, ?, ...).
        // SQLite max params = 999; reserve ?1 for bucket, so batch size = 998.
        const batch_size: usize = 998;
        var offset: usize = 0;
        while (offset < keys.len) {
            const end = @min(offset + batch_size, keys.len);
            const batch = keys[offset..end];
            const count = batch.len;

            // Build SQL: "DELETE FROM objects WHERE bucket = ?1 AND key IN (?2, ?3, ...)"
            var sql_buf = std.ArrayList(u8).empty;
            defer sql_buf.deinit(self.allocator);
            sql_buf.appendSlice(self.allocator, "DELETE FROM objects WHERE bucket = ?1 AND key IN (") catch {
                offset = end;
                continue;
            };
            for (0..count) |i| {
                if (i > 0) sql_buf.append(self.allocator, ',') catch {};
                const param_str = std.fmt.allocPrint(self.allocator, "?{d}", .{i + 2}) catch {
                    break;
                };
                defer self.allocator.free(param_str);
                sql_buf.appendSlice(self.allocator, param_str) catch {};
            }
            sql_buf.appendSlice(self.allocator, ");") catch {};
            sql_buf.append(self.allocator, 0) catch {}; // null terminator

            // Prepare the dynamic SQL (raw C API since prepareStmt needs [*:0]const u8).
            var stmt: ?*c.sqlite3_stmt = null;
            const rc_prep = c.sqlite3_prepare_v2(self.db, @ptrCast(sql_buf.items.ptr), @intCast(sql_buf.items.len), &stmt, null);
            if (rc_prep != c.SQLITE_OK or stmt == null) {
                offset = end;
                continue;
            }
            defer _ = c.sqlite3_finalize(stmt.?);

            // Bind bucket as ?1.
            self.bindText(stmt.?, 1, bucket);
            // Bind each key.
            for (batch, 0..) |key, i| {
                self.bindText(stmt.?, @intCast(i + 2), key);
            }

            _ = c.sqlite3_step(stmt.?);
            offset = end;
        }
        return results;
    }

    fn listObjectsMeta(ctx: *anyopaque, bucket: []const u8, prefix: []const u8, delimiter: []const u8, start_after: []const u8, max_keys: u32) anyerror!ListObjectsResult {
        const self = getSelf(ctx);

        // Build prefix-matching pattern for LIKE clause
        const like_pattern = if (prefix.len > 0)
            try std.fmt.allocPrint(self.allocator, "{s}%", .{prefix})
        else
            try self.allocator.dupe(u8, "%");
        defer self.allocator.free(like_pattern);

        // We fetch max_keys + 1 to detect truncation
        const fetch_limit: u32 = if (max_keys < std.math.maxInt(u32)) max_keys + 1 else max_keys;

        const sql =
            \\SELECT bucket, key, size, etag, content_type, content_encoding,
            \\       content_language, content_disposition, cache_control, expires,
            \\       storage_class, acl, user_metadata, last_modified, delete_marker
            \\FROM objects
            \\WHERE bucket = ?1 AND key LIKE ?2 AND key > ?3
            \\ORDER BY key
            \\LIMIT ?4;
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, bucket);
        self.bindText(stmt, 2, like_pattern);
        self.bindText(stmt, 3, start_after);
        self.bindInt64(stmt, 4, @intCast(fetch_limit));

        var objects_list: std.ArrayList(ObjectMeta) = .empty;
        errdefer {
            for (objects_list.items) |*obj| {
                self.freeObjectMeta(obj);
            }
            objects_list.deinit(self.allocator);
        }

        // For delimiter-based common prefix grouping.
        // Uses a simple ArrayList for dedup (common prefix count is typically small).
        var common_prefixes: std.ArrayList([]const u8) = .empty;
        errdefer {
            for (common_prefixes.items) |cp_str| self.allocator.free(cp_str);
            common_prefixes.deinit(self.allocator);
        }

        var count: u32 = 0;
        var is_truncated = false;
        var last_key: ?[]const u8 = null;

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            if (count >= max_keys) {
                is_truncated = true;
                break;
            }

            const obj = try self.readObjectMetaFromRow(stmt);

            // Handle delimiter-based grouping
            if (delimiter.len > 0) {
                // Check if the key (after prefix) contains the delimiter
                const key_after_prefix = if (prefix.len > 0 and std.mem.startsWith(u8, obj.key, prefix))
                    obj.key[prefix.len..]
                else
                    obj.key;

                if (std.mem.indexOf(u8, key_after_prefix, delimiter)) |delim_pos| {
                    // This key falls under a common prefix
                    const cp_end = prefix.len + delim_pos + delimiter.len;
                    const cp = obj.key[0..cp_end];

                    // Add to common prefixes list (only if not already seen)
                    const already_seen = for (common_prefixes.items) |existing| {
                        if (std.mem.eql(u8, existing, cp)) break true;
                    } else false;

                    if (!already_seen) {
                        const cp_dupe = try self.allocator.dupe(u8, cp);
                        errdefer self.allocator.free(cp_dupe);
                        try common_prefixes.append(self.allocator, cp_dupe);
                    }
                    // Free the object since we're grouping it under a common prefix
                    self.freeObjectMeta(&obj);
                    count += 1;
                    continue;
                }
            }

            last_key = obj.key;
            try objects_list.append(self.allocator, obj);
            count += 1;
        }

        const cp_slice = try common_prefixes.toOwnedSlice(self.allocator);
        errdefer {
            for (cp_slice) |cp_str| self.allocator.free(cp_str);
            self.allocator.free(cp_slice);
        }

        // Build continuation/marker tokens from last key.
        // V1 uses next_marker, V2 uses next_continuation_token.
        // Both are independently allocated so the caller can free them separately.
        var next_continuation_token: ?[]const u8 = null;
        errdefer if (next_continuation_token) |t| self.allocator.free(t);
        var next_marker: ?[]const u8 = null;
        errdefer if (next_marker) |t| self.allocator.free(t);
        if (is_truncated) {
            if (last_key) |lk| {
                next_continuation_token = try self.allocator.dupe(u8, lk);
                next_marker = try self.allocator.dupe(u8, lk);
            }
        }

        const obj_slice = try objects_list.toOwnedSlice(self.allocator);

        return ListObjectsResult{
            .objects = obj_slice,
            .common_prefixes = cp_slice,
            .is_truncated = is_truncated,
            .next_continuation_token = next_continuation_token,
            .next_marker = next_marker,
        };
    }

    fn objectExists(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!bool {
        const self = getSelf(ctx);
        const sql = "SELECT 1 FROM objects WHERE bucket = ?1 AND key = ?2;";
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, bucket);
        self.bindText(stmt, 2, key);

        return c.sqlite3_step(stmt) == c.SQLITE_ROW;
    }

    fn updateObjectAcl(ctx: *anyopaque, bucket: []const u8, key: []const u8, acl: []const u8) anyerror!void {
        const self = getSelf(ctx);
        const sql = "UPDATE objects SET acl = ?1 WHERE bucket = ?2 AND key = ?3;";
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, acl);
        self.bindText(stmt, 2, bucket);
        self.bindText(stmt, 3, key);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }
        if (c.sqlite3_changes(self.db) == 0) {
            return error.NoSuchKey;
        }
    }

    // =========================================================================
    // Multipart operations
    // =========================================================================

    fn createMultipartUpload(ctx: *anyopaque, meta: MultipartUploadMeta) anyerror!void {
        const self = getSelf(ctx);
        const sql =
            \\INSERT INTO multipart_uploads
            \\    (upload_id, bucket, key, content_type, content_encoding,
            \\     content_language, content_disposition, cache_control, expires,
            \\     storage_class, acl, user_metadata, owner_id, owner_display, initiated_at)
            \\VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15);
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        self.bindText(stmt, 1, meta.upload_id);
        self.bindText(stmt, 2, meta.bucket);
        self.bindText(stmt, 3, meta.key);
        self.bindText(stmt, 4, meta.content_type);
        self.bindOptionalText(stmt, 5, meta.content_encoding);
        self.bindOptionalText(stmt, 6, meta.content_language);
        self.bindOptionalText(stmt, 7, meta.content_disposition);
        self.bindOptionalText(stmt, 8, meta.cache_control);
        self.bindOptionalText(stmt, 9, meta.expires);
        self.bindText(stmt, 10, meta.storage_class);
        self.bindText(stmt, 11, meta.acl);
        self.bindText(stmt, 12, meta.user_metadata);
        self.bindText(stmt, 13, meta.owner_id);
        self.bindText(stmt, 14, meta.owner_display);
        self.bindText(stmt, 15, meta.initiated);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }
    }

    fn getMultipartUpload(ctx: *anyopaque, upload_id: []const u8) anyerror!?MultipartUploadMeta {
        const self = getSelf(ctx);
        const sql =
            \\SELECT upload_id, bucket, key, content_type, content_encoding,
            \\       content_language, content_disposition, cache_control, expires,
            \\       storage_class, acl, user_metadata, owner_id, owner_display, initiated_at
            \\FROM multipart_uploads WHERE upload_id = ?1;
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, upload_id);

        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            return MultipartUploadMeta{
                .upload_id = try self.columnTextDup(stmt, 0),
                .bucket = try self.columnTextDup(stmt, 1),
                .key = try self.columnTextDup(stmt, 2),
                .content_type = try self.columnTextDup(stmt, 3),
                .content_encoding = try self.columnOptionalTextDup(stmt, 4),
                .content_language = try self.columnOptionalTextDup(stmt, 5),
                .content_disposition = try self.columnOptionalTextDup(stmt, 6),
                .cache_control = try self.columnOptionalTextDup(stmt, 7),
                .expires = try self.columnOptionalTextDup(stmt, 8),
                .storage_class = try self.columnTextDup(stmt, 9),
                .acl = try self.columnTextDup(stmt, 10),
                .user_metadata = try self.columnTextDup(stmt, 11),
                .owner_id = try self.columnTextDup(stmt, 12),
                .owner_display = try self.columnTextDup(stmt, 13),
                .initiated = try self.columnTextDup(stmt, 14),
            };
        }
        return null;
    }

    fn abortMultipartUpload(ctx: *anyopaque, upload_id: []const u8) anyerror!void {
        const self = getSelf(ctx);

        // Delete parts first (foreign key cascade would also handle this, but explicit is clearer)
        const parts_sql = "DELETE FROM multipart_parts WHERE upload_id = ?1;";
        const parts_stmt = try self.prepareStmt(parts_sql);
        defer self.finalizeStmt(parts_stmt);
        self.bindText(parts_stmt, 1, upload_id);
        _ = c.sqlite3_step(parts_stmt);

        // Delete the upload record
        const sql = "DELETE FROM multipart_uploads WHERE upload_id = ?1;";
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, upload_id);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }
        if (c.sqlite3_changes(self.db) == 0) {
            return error.NoSuchUpload;
        }
    }

    fn putPartMeta(ctx: *anyopaque, upload_id: []const u8, part: PartMeta) anyerror!void {
        const self = getSelf(ctx);
        const sql =
            \\INSERT OR REPLACE INTO multipart_parts
            \\    (upload_id, part_number, size, etag, last_modified)
            \\VALUES (?1, ?2, ?3, ?4, ?5);
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        self.bindText(stmt, 1, upload_id);
        self.bindInt64(stmt, 2, @intCast(part.part_number));
        self.bindInt64(stmt, 3, @intCast(part.size));
        self.bindText(stmt, 4, part.etag);
        self.bindText(stmt, 5, part.last_modified);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }
    }

    fn listPartsMeta(ctx: *anyopaque, upload_id: []const u8, max_parts: u32, part_marker: u32) anyerror!ListPartsResult {
        const self = getSelf(ctx);

        // Fetch max_parts + 1 to detect truncation
        const fetch_limit: u32 = if (max_parts < std.math.maxInt(u32)) max_parts + 1 else max_parts;

        const sql =
            \\SELECT part_number, size, etag, last_modified
            \\FROM multipart_parts
            \\WHERE upload_id = ?1 AND part_number > ?2
            \\ORDER BY part_number
            \\LIMIT ?3;
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, upload_id);
        self.bindInt64(stmt, 2, @intCast(part_marker));
        self.bindInt64(stmt, 3, @intCast(fetch_limit));

        var parts_list: std.ArrayList(PartMeta) = .empty;
        errdefer parts_list.deinit(self.allocator);

        var count: u32 = 0;
        var is_truncated = false;
        var last_part_number: u32 = 0;

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            if (count >= max_parts) {
                is_truncated = true;
                break;
            }

            const part = PartMeta{
                .part_number = @intCast(c.sqlite3_column_int(stmt, 0)),
                .size = @intCast(c.sqlite3_column_int64(stmt, 1)),
                .etag = try self.columnTextDup(stmt, 2),
                .last_modified = try self.columnTextDup(stmt, 3),
            };
            last_part_number = part.part_number;
            try parts_list.append(self.allocator, part);
            count += 1;
        }

        return ListPartsResult{
            .parts = try parts_list.toOwnedSlice(self.allocator),
            .is_truncated = is_truncated,
            .next_part_number_marker = if (is_truncated) last_part_number else 0,
        };
    }

    fn getPartsForCompletion(ctx: *anyopaque, upload_id: []const u8) anyerror![]PartMeta {
        const self = getSelf(ctx);
        const sql =
            \\SELECT part_number, size, etag, last_modified
            \\FROM multipart_parts
            \\WHERE upload_id = ?1
            \\ORDER BY part_number;
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, upload_id);

        var parts_list: std.ArrayList(PartMeta) = .empty;
        errdefer parts_list.deinit(self.allocator);

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const part = PartMeta{
                .part_number = @intCast(c.sqlite3_column_int(stmt, 0)),
                .size = @intCast(c.sqlite3_column_int64(stmt, 1)),
                .etag = try self.columnTextDup(stmt, 2),
                .last_modified = try self.columnTextDup(stmt, 3),
            };
            try parts_list.append(self.allocator, part);
        }

        return parts_list.toOwnedSlice(self.allocator);
    }

    fn completeMultipartUpload(ctx: *anyopaque, upload_id: []const u8, object_meta: ObjectMeta) anyerror!void {
        const self = getSelf(ctx);

        // Use a transaction for atomicity
        try self.execSql("BEGIN;");
        errdefer self.execSql("ROLLBACK;") catch {};

        // Insert the object metadata
        const obj_sql =
            \\INSERT OR REPLACE INTO objects
            \\    (bucket, key, size, etag, content_type, content_encoding,
            \\     content_language, content_disposition, cache_control, expires,
            \\     storage_class, acl, user_metadata, last_modified, delete_marker)
            \\VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15);
        ;
        const obj_stmt = try self.prepareStmt(obj_sql);
        defer self.finalizeStmt(obj_stmt);

        self.bindText(obj_stmt, 1, object_meta.bucket);
        self.bindText(obj_stmt, 2, object_meta.key);
        self.bindInt64(obj_stmt, 3, @intCast(object_meta.size));
        self.bindText(obj_stmt, 4, object_meta.etag);
        self.bindText(obj_stmt, 5, object_meta.content_type);
        self.bindOptionalText(obj_stmt, 6, object_meta.content_encoding);
        self.bindOptionalText(obj_stmt, 7, object_meta.content_language);
        self.bindOptionalText(obj_stmt, 8, object_meta.content_disposition);
        self.bindOptionalText(obj_stmt, 9, object_meta.cache_control);
        self.bindOptionalText(obj_stmt, 10, object_meta.expires);
        self.bindText(obj_stmt, 11, object_meta.storage_class);
        self.bindText(obj_stmt, 12, object_meta.acl);
        self.bindOptionalText(obj_stmt, 13, object_meta.user_metadata);
        self.bindText(obj_stmt, 14, object_meta.last_modified);
        self.bindInt64(obj_stmt, 15, if (object_meta.delete_marker) 1 else 0);

        if (c.sqlite3_step(obj_stmt) != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }

        // Delete parts
        const parts_del_sql = "DELETE FROM multipart_parts WHERE upload_id = ?1;";
        const parts_del_stmt = try self.prepareStmt(parts_del_sql);
        defer self.finalizeStmt(parts_del_stmt);
        self.bindText(parts_del_stmt, 1, upload_id);
        _ = c.sqlite3_step(parts_del_stmt);

        // Delete the upload record
        const upload_del_sql = "DELETE FROM multipart_uploads WHERE upload_id = ?1;";
        const upload_del_stmt = try self.prepareStmt(upload_del_sql);
        defer self.finalizeStmt(upload_del_stmt);
        self.bindText(upload_del_stmt, 1, upload_id);
        _ = c.sqlite3_step(upload_del_stmt);

        try self.execSql("COMMIT;");
    }

    fn listMultipartUploads(ctx: *anyopaque, bucket: []const u8, prefix: []const u8, max_uploads: u32) anyerror!ListUploadsResult {
        const self = getSelf(ctx);

        const like_pattern = if (prefix.len > 0)
            try std.fmt.allocPrint(self.allocator, "{s}%", .{prefix})
        else
            try self.allocator.dupe(u8, "%");
        defer self.allocator.free(like_pattern);

        const fetch_limit: u32 = if (max_uploads < std.math.maxInt(u32)) max_uploads + 1 else max_uploads;

        const sql =
            \\SELECT upload_id, bucket, key, content_type, content_encoding,
            \\       content_language, content_disposition, cache_control, expires,
            \\       storage_class, acl, user_metadata, owner_id, owner_display, initiated_at
            \\FROM multipart_uploads
            \\WHERE bucket = ?1 AND key LIKE ?2
            \\ORDER BY key, initiated_at
            \\LIMIT ?3;
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, bucket);
        self.bindText(stmt, 2, like_pattern);
        self.bindInt64(stmt, 3, @intCast(fetch_limit));

        var uploads_list: std.ArrayList(MultipartUploadMeta) = .empty;
        errdefer uploads_list.deinit(self.allocator);

        var count: u32 = 0;
        var is_truncated = false;

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            if (count >= max_uploads) {
                is_truncated = true;
                break;
            }

            const upload = MultipartUploadMeta{
                .upload_id = try self.columnTextDup(stmt, 0),
                .bucket = try self.columnTextDup(stmt, 1),
                .key = try self.columnTextDup(stmt, 2),
                .content_type = try self.columnTextDup(stmt, 3),
                .content_encoding = try self.columnOptionalTextDup(stmt, 4),
                .content_language = try self.columnOptionalTextDup(stmt, 5),
                .content_disposition = try self.columnOptionalTextDup(stmt, 6),
                .cache_control = try self.columnOptionalTextDup(stmt, 7),
                .expires = try self.columnOptionalTextDup(stmt, 8),
                .storage_class = try self.columnTextDup(stmt, 9),
                .acl = try self.columnTextDup(stmt, 10),
                .user_metadata = try self.columnTextDup(stmt, 11),
                .owner_id = try self.columnTextDup(stmt, 12),
                .owner_display = try self.columnTextDup(stmt, 13),
                .initiated = try self.columnTextDup(stmt, 14),
            };
            try uploads_list.append(self.allocator, upload);
            count += 1;
        }

        return ListUploadsResult{
            .uploads = try uploads_list.toOwnedSlice(self.allocator),
            .is_truncated = is_truncated,
        };
    }

    // =========================================================================
    // Credential operations
    // =========================================================================

    fn getCredential(ctx: *anyopaque, access_key_id: []const u8) anyerror!?Credential {
        const self = getSelf(ctx);
        const sql =
            \\SELECT access_key_id, secret_key, owner_id, display_name, active, created_at
            \\FROM credentials WHERE access_key_id = ?1 AND active = 1;
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);
        self.bindText(stmt, 1, access_key_id);

        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            return Credential{
                .access_key_id = try self.columnTextDup(stmt, 0),
                .secret_key = try self.columnTextDup(stmt, 1),
                .owner_id = try self.columnTextDup(stmt, 2),
                .display_name = try self.columnTextDup(stmt, 3),
                .active = c.sqlite3_column_int(stmt, 4) != 0,
                .created_at = try self.columnTextDup(stmt, 5),
            };
        }
        return null;
    }

    fn putCredential(ctx: *anyopaque, cred: Credential) anyerror!void {
        const self = getSelf(ctx);
        const sql =
            \\INSERT OR REPLACE INTO credentials
            \\    (access_key_id, secret_key, owner_id, display_name, active, created_at)
            \\VALUES (?1, ?2, ?3, ?4, ?5, ?6);
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        self.bindText(stmt, 1, cred.access_key_id);
        self.bindText(stmt, 2, cred.secret_key);
        self.bindText(stmt, 3, cred.owner_id);
        self.bindText(stmt, 4, cred.display_name);
        self.bindInt64(stmt, 5, if (cred.active) 1 else 0);
        self.bindText(stmt, 6, cred.created_at);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }
    }

    // =========================================================================
    // Count operations
    // =========================================================================

    fn countBuckets(ctx: *anyopaque) anyerror!u64 {
        const self = getSelf(ctx);
        const sql = "SELECT COUNT(*) FROM buckets;";
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            return @intCast(c.sqlite3_column_int64(stmt, 0));
        }
        return 0;
    }

    fn countObjects(ctx: *anyopaque) anyerror!u64 {
        const self = getSelf(ctx);
        const sql = "SELECT COUNT(*) FROM objects;";
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            return @intCast(c.sqlite3_column_int64(stmt, 0));
        }
        return 0;
    }

    // =========================================================================
    // Credential seeding
    // =========================================================================

    /// Seed default credentials from config. Only inserts if the access key
    /// does not already exist.
    pub fn seedCredentials(self: *Self, access_key: []const u8, secret_key: []const u8) !void {
        // Derive owner ID from access key using SHA-256, take first 16 hex chars
        var hash: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(access_key, &hash, .{});
        const owner_hex = std.fmt.bytesToHex(hash[0..8].*, .lower);

        const sql =
            \\INSERT OR IGNORE INTO credentials
            \\    (access_key_id, secret_key, owner_id, display_name, active, created_at)
            \\VALUES (?1, ?2, ?3, ?4, 1, datetime('now'));
        ;
        const stmt = try self.prepareStmt(sql);
        defer self.finalizeStmt(stmt);

        self.bindText(stmt, 1, access_key);
        self.bindText(stmt, 2, secret_key);
        self.bindText(stmt, 3, &owner_hex);
        self.bindText(stmt, 4, access_key);

        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) {
            return error.SqliteStepFailed;
        }
    }

    // =========================================================================
    // vtable + interface
    // =========================================================================

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

    /// Obtain a MetadataStore interface backed by this SQLite implementation.
    pub fn metadataStore(self: *Self) MetadataStore {
        return .{
            .ctx = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    // =========================================================================
    // SQLite helper functions
    // =========================================================================

    fn getSelf(ctx: *anyopaque) *Self {
        return @ptrCast(@alignCast(ctx));
    }

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

    /// Bind a text parameter.  Uses SQLITE_TRANSIENT (-1) so SQLite copies the data.
    fn bindText(_: *Self, stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) void {
        _ = sqlite3_bind_text(stmt, index, value.ptr, @intCast(value.len), -1);
    }

    /// Bind an optional text parameter (NULL if none).
    fn bindOptionalText(self: *Self, stmt: *c.sqlite3_stmt, index: c_int, value: ?[]const u8) void {
        if (value) |v| {
            self.bindText(stmt, index, v);
        } else {
            _ = c.sqlite3_bind_null(stmt, index);
        }
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

    /// Read a potentially null text column and duplicate it.
    fn columnOptionalTextDup(self: *Self, stmt: *c.sqlite3_stmt, col: c_int) !?[]const u8 {
        if (c.sqlite3_column_type(stmt, col) == c.SQLITE_NULL) {
            return null;
        }
        return try self.columnTextDup(stmt, col);
    }

    /// Free all allocator-owned fields of an ObjectMeta.
    fn freeObjectMeta(self: *Self, obj: *const ObjectMeta) void {
        self.allocator.free(obj.bucket);
        self.allocator.free(obj.key);
        self.allocator.free(obj.etag);
        self.allocator.free(obj.content_type);
        self.allocator.free(obj.last_modified);
        self.allocator.free(obj.storage_class);
        self.allocator.free(obj.acl);
        if (obj.user_metadata) |um| self.allocator.free(um);
        if (obj.content_encoding) |ce| self.allocator.free(ce);
        if (obj.content_language) |cl| self.allocator.free(cl);
        if (obj.content_disposition) |cd| self.allocator.free(cd);
        if (obj.cache_control) |cc| self.allocator.free(cc);
        if (obj.expires) |ex| self.allocator.free(ex);
    }

    /// Read an ObjectMeta from the current row of a SELECT statement.
    /// Column order must match the standard SELECT for objects table.
    fn readObjectMetaFromRow(self: *Self, stmt: *c.sqlite3_stmt) !ObjectMeta {
        return ObjectMeta{
            .bucket = try self.columnTextDup(stmt, 0),
            .key = try self.columnTextDup(stmt, 1),
            .size = @intCast(c.sqlite3_column_int64(stmt, 2)),
            .etag = try self.columnTextDup(stmt, 3),
            .content_type = try self.columnTextDup(stmt, 4),
            .content_encoding = try self.columnOptionalTextDup(stmt, 5),
            .content_language = try self.columnOptionalTextDup(stmt, 6),
            .content_disposition = try self.columnOptionalTextDup(stmt, 7),
            .cache_control = try self.columnOptionalTextDup(stmt, 8),
            .expires = try self.columnOptionalTextDup(stmt, 9),
            .storage_class = try self.columnTextDup(stmt, 10),
            .acl = try self.columnTextDup(stmt, 11),
            .user_metadata = try self.columnOptionalTextDup(stmt, 12),
            .last_modified = try self.columnTextDup(stmt, 13),
            .delete_marker = c.sqlite3_column_int(stmt, 14) != 0,
        };
    }
};

// =========================================================================
// Tests
// =========================================================================

test "SqliteMetadataStore: init and deinit" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    try std.testing.expect(ms.db != null);
}

test "SqliteMetadataStore: create and get bucket" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
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
    try std.testing.expectEqualStrings("2026-01-01T00:00:00.000Z", b.creation_date);
    try std.testing.expectEqualStrings("us-east-1", b.region);
    try std.testing.expectEqualStrings("owner123", b.owner_id);
}

test "SqliteMetadataStore: bucket exists" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
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

test "SqliteMetadataStore: list buckets" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{ .name = "alpha", .creation_date = "2026-01-01T00:00:00.000Z", .region = "us-east-1", .owner_id = "owner" });
    try iface.createBucket(.{ .name = "beta", .creation_date = "2026-01-02T00:00:00.000Z", .region = "us-west-2", .owner_id = "owner" });

    const buckets = try iface.listBuckets();
    defer {
        for (buckets) |*b| {
            b.deinit(std.testing.allocator);
        }
        std.testing.allocator.free(buckets);
    }

    try std.testing.expectEqual(@as(usize, 2), buckets.len);
    try std.testing.expectEqualStrings("alpha", buckets[0].name);
    try std.testing.expectEqualStrings("beta", buckets[1].name);
}

test "SqliteMetadataStore: delete bucket" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{ .name = "del-bucket", .creation_date = "2026-01-01T00:00:00.000Z", .region = "us-east-1", .owner_id = "owner" });

    try iface.deleteBucket("del-bucket");
    try std.testing.expect(!try iface.bucketExists("del-bucket"));
}

test "SqliteMetadataStore: delete nonexistent bucket returns error" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try std.testing.expectError(error.NoSuchBucket, iface.deleteBucket("ghost"));
}

test "SqliteMetadataStore: delete bucket with objects returns error" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{ .name = "full-bucket", .creation_date = "2026-01-01T00:00:00.000Z", .region = "us-east-1", .owner_id = "owner" });
    try iface.putObjectMeta(.{
        .bucket = "full-bucket",
        .key = "file.txt",
        .size = 5,
        .etag = "\"abc\"",
        .content_type = "text/plain",
        .last_modified = "2026-01-01T00:00:00.000Z",
        .storage_class = "STANDARD",
    });

    try std.testing.expectError(error.BucketNotEmpty, iface.deleteBucket("full-bucket"));
}

test "SqliteMetadataStore: put and get object meta" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{ .name = "obj-bucket", .creation_date = "2026-01-01T00:00:00.000Z", .region = "us-east-1", .owner_id = "owner" });

    try iface.putObjectMeta(.{
        .bucket = "obj-bucket",
        .key = "hello.txt",
        .size = 13,
        .etag = "\"d41d8cd98f00b204e9800998ecf8427e\"",
        .content_type = "text/plain",
        .last_modified = "2026-01-01T12:00:00.000Z",
        .storage_class = "STANDARD",
        .user_metadata = "{\"x-amz-meta-author\":\"test\"}",
    });

    const obj = try iface.getObjectMeta("obj-bucket", "hello.txt");
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
        if (o.user_metadata) |um| std.testing.allocator.free(um);
    }
    try std.testing.expectEqualStrings("obj-bucket", o.bucket);
    try std.testing.expectEqualStrings("hello.txt", o.key);
    try std.testing.expectEqual(@as(u64, 13), o.size);
    try std.testing.expectEqualStrings("text/plain", o.content_type);
    try std.testing.expectEqualStrings("{\"x-amz-meta-author\":\"test\"}", o.user_metadata.?);
}

test "SqliteMetadataStore: delete object meta" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{ .name = "del-obj-bucket", .creation_date = "2026-01-01T00:00:00.000Z", .region = "us-east-1", .owner_id = "owner" });
    try iface.putObjectMeta(.{
        .bucket = "del-obj-bucket",
        .key = "delete-me.txt",
        .size = 5,
        .etag = "\"abc\"",
        .content_type = "text/plain",
        .last_modified = "2026-01-01T00:00:00.000Z",
        .storage_class = "STANDARD",
    });

    const deleted = try iface.deleteObjectMeta("del-obj-bucket", "delete-me.txt");
    try std.testing.expect(deleted);

    // Deleting again returns false (idempotent)
    const deleted2 = try iface.deleteObjectMeta("del-obj-bucket", "delete-me.txt");
    try std.testing.expect(!deleted2);
}

test "SqliteMetadataStore: list objects with prefix" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{ .name = "list-bucket", .creation_date = "2026-01-01T00:00:00.000Z", .region = "us-east-1", .owner_id = "owner" });

    try iface.putObjectMeta(.{ .bucket = "list-bucket", .key = "a.txt", .size = 1, .etag = "\"a\"", .content_type = "text/plain", .last_modified = "2026-01-01T00:00:00.000Z", .storage_class = "STANDARD" });
    try iface.putObjectMeta(.{ .bucket = "list-bucket", .key = "b.txt", .size = 2, .etag = "\"b\"", .content_type = "text/plain", .last_modified = "2026-01-01T00:00:00.000Z", .storage_class = "STANDARD" });
    try iface.putObjectMeta(.{ .bucket = "list-bucket", .key = "dir/c.txt", .size = 3, .etag = "\"c\"", .content_type = "text/plain", .last_modified = "2026-01-01T00:00:00.000Z", .storage_class = "STANDARD" });

    // List all
    const result = try iface.listObjectsMeta("list-bucket", "", "", "", 1000);
    defer {
        for (result.objects) |o| {
            std.testing.allocator.free(o.bucket);
            std.testing.allocator.free(o.key);
            std.testing.allocator.free(o.etag);
            std.testing.allocator.free(o.content_type);
            std.testing.allocator.free(o.last_modified);
            std.testing.allocator.free(o.storage_class);
            std.testing.allocator.free(o.acl);
            if (o.user_metadata) |um| std.testing.allocator.free(um);
        }
        std.testing.allocator.free(result.objects);
        std.testing.allocator.free(result.common_prefixes);
    }
    try std.testing.expectEqual(@as(usize, 3), result.objects.len);

    // List with prefix
    const result2 = try iface.listObjectsMeta("list-bucket", "dir/", "", "", 1000);
    defer {
        for (result2.objects) |o| {
            std.testing.allocator.free(o.bucket);
            std.testing.allocator.free(o.key);
            std.testing.allocator.free(o.etag);
            std.testing.allocator.free(o.content_type);
            std.testing.allocator.free(o.last_modified);
            std.testing.allocator.free(o.storage_class);
            std.testing.allocator.free(o.acl);
            if (o.user_metadata) |um| std.testing.allocator.free(um);
        }
        std.testing.allocator.free(result2.objects);
        std.testing.allocator.free(result2.common_prefixes);
    }
    try std.testing.expectEqual(@as(usize, 1), result2.objects.len);
    try std.testing.expectEqualStrings("dir/c.txt", result2.objects[0].key);
}

test "SqliteMetadataStore: list objects with delimiter" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{ .name = "delim-bucket", .creation_date = "2026-01-01T00:00:00.000Z", .region = "us-east-1", .owner_id = "owner" });

    try iface.putObjectMeta(.{ .bucket = "delim-bucket", .key = "photos/2024/jan.jpg", .size = 100, .etag = "\"1\"", .content_type = "image/jpeg", .last_modified = "2026-01-01T00:00:00.000Z", .storage_class = "STANDARD" });
    try iface.putObjectMeta(.{ .bucket = "delim-bucket", .key = "photos/2024/feb.jpg", .size = 200, .etag = "\"2\"", .content_type = "image/jpeg", .last_modified = "2026-01-01T00:00:00.000Z", .storage_class = "STANDARD" });
    try iface.putObjectMeta(.{ .bucket = "delim-bucket", .key = "photos/2025/mar.jpg", .size = 300, .etag = "\"3\"", .content_type = "image/jpeg", .last_modified = "2026-01-01T00:00:00.000Z", .storage_class = "STANDARD" });
    try iface.putObjectMeta(.{ .bucket = "delim-bucket", .key = "readme.txt", .size = 50, .etag = "\"4\"", .content_type = "text/plain", .last_modified = "2026-01-01T00:00:00.000Z", .storage_class = "STANDARD" });

    // List with delimiter "/" at root level
    const result = try iface.listObjectsMeta("delim-bucket", "", "/", "", 1000);
    defer {
        for (result.objects) |o| {
            std.testing.allocator.free(o.bucket);
            std.testing.allocator.free(o.key);
            std.testing.allocator.free(o.etag);
            std.testing.allocator.free(o.content_type);
            std.testing.allocator.free(o.last_modified);
            std.testing.allocator.free(o.storage_class);
            std.testing.allocator.free(o.acl);
            if (o.user_metadata) |um| std.testing.allocator.free(um);
        }
        std.testing.allocator.free(result.objects);
        for (result.common_prefixes) |cp| std.testing.allocator.free(cp);
        std.testing.allocator.free(result.common_prefixes);
    }

    // Should have 1 object (readme.txt) and 1 common prefix (photos/)
    try std.testing.expectEqual(@as(usize, 1), result.objects.len);
    try std.testing.expectEqualStrings("readme.txt", result.objects[0].key);
    try std.testing.expectEqual(@as(usize, 1), result.common_prefixes.len);
    try std.testing.expectEqualStrings("photos/", result.common_prefixes[0]);
}

test "SqliteMetadataStore: list objects pagination" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{ .name = "page-bucket", .creation_date = "2026-01-01T00:00:00.000Z", .region = "us-east-1", .owner_id = "owner" });

    try iface.putObjectMeta(.{ .bucket = "page-bucket", .key = "a.txt", .size = 1, .etag = "\"a\"", .content_type = "text/plain", .last_modified = "2026-01-01T00:00:00.000Z", .storage_class = "STANDARD" });
    try iface.putObjectMeta(.{ .bucket = "page-bucket", .key = "b.txt", .size = 2, .etag = "\"b\"", .content_type = "text/plain", .last_modified = "2026-01-01T00:00:00.000Z", .storage_class = "STANDARD" });
    try iface.putObjectMeta(.{ .bucket = "page-bucket", .key = "c.txt", .size = 3, .etag = "\"c\"", .content_type = "text/plain", .last_modified = "2026-01-01T00:00:00.000Z", .storage_class = "STANDARD" });

    // List with max_keys=2 (should be truncated)
    const result = try iface.listObjectsMeta("page-bucket", "", "", "", 2);
    defer {
        for (result.objects) |o| {
            std.testing.allocator.free(o.bucket);
            std.testing.allocator.free(o.key);
            std.testing.allocator.free(o.etag);
            std.testing.allocator.free(o.content_type);
            std.testing.allocator.free(o.last_modified);
            std.testing.allocator.free(o.storage_class);
            std.testing.allocator.free(o.acl);
            if (o.user_metadata) |um| std.testing.allocator.free(um);
        }
        std.testing.allocator.free(result.objects);
        std.testing.allocator.free(result.common_prefixes);
        if (result.next_continuation_token) |t| std.testing.allocator.free(t);
        if (result.next_marker) |m| std.testing.allocator.free(m);
    }

    try std.testing.expectEqual(@as(usize, 2), result.objects.len);
    try std.testing.expect(result.is_truncated);
    try std.testing.expect(result.next_continuation_token != null);
    try std.testing.expect(result.next_marker != null);
}

test "SqliteMetadataStore: credential seeding and retrieval" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();

    try ms.seedCredentials("bleepstore", "bleepstore-secret");

    const iface = ms.metadataStore();
    const cred = try iface.getCredential("bleepstore");
    try std.testing.expect(cred != null);
    const cr = cred.?;
    defer {
        std.testing.allocator.free(cr.access_key_id);
        std.testing.allocator.free(cr.secret_key);
        std.testing.allocator.free(cr.owner_id);
        std.testing.allocator.free(cr.display_name);
        std.testing.allocator.free(cr.created_at);
    }
    try std.testing.expectEqualStrings("bleepstore", cr.access_key_id);
    try std.testing.expectEqualStrings("bleepstore-secret", cr.secret_key);
    try std.testing.expect(cr.active);
}

test "SqliteMetadataStore: credential not found" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    const cred = try iface.getCredential("nonexistent");
    try std.testing.expect(cred == null);
}

test "SqliteMetadataStore: multipart upload lifecycle" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{ .name = "mp-bucket", .creation_date = "2026-01-01T00:00:00.000Z", .region = "us-east-1", .owner_id = "owner" });

    // Create multipart upload
    try iface.createMultipartUpload(.{
        .upload_id = "upload-123",
        .bucket = "mp-bucket",
        .key = "big-file.bin",
        .initiated = "2026-01-01T12:00:00.000Z",
        .owner_id = "owner",
    });

    // Get the upload
    const upload = try iface.getMultipartUpload("upload-123");
    try std.testing.expect(upload != null);
    const u = upload.?;
    defer {
        std.testing.allocator.free(u.upload_id);
        std.testing.allocator.free(u.bucket);
        std.testing.allocator.free(u.key);
        std.testing.allocator.free(u.content_type);
        std.testing.allocator.free(u.storage_class);
        std.testing.allocator.free(u.acl);
        std.testing.allocator.free(u.user_metadata);
        std.testing.allocator.free(u.owner_id);
        std.testing.allocator.free(u.owner_display);
        std.testing.allocator.free(u.initiated);
    }
    try std.testing.expectEqualStrings("upload-123", u.upload_id);

    // Upload parts
    try iface.putPartMeta("upload-123", .{ .part_number = 1, .size = 5242880, .etag = "\"part1-etag\"", .last_modified = "2026-01-01T12:01:00.000Z" });
    try iface.putPartMeta("upload-123", .{ .part_number = 2, .size = 1048576, .etag = "\"part2-etag\"", .last_modified = "2026-01-01T12:02:00.000Z" });

    // List parts
    const parts_result = try iface.listPartsMeta("upload-123", 1000, 0);
    defer {
        for (parts_result.parts) |p| {
            std.testing.allocator.free(p.etag);
            std.testing.allocator.free(p.last_modified);
        }
        std.testing.allocator.free(parts_result.parts);
    }
    try std.testing.expectEqual(@as(usize, 2), parts_result.parts.len);
    try std.testing.expectEqual(@as(u32, 1), parts_result.parts[0].part_number);
    try std.testing.expectEqual(@as(u32, 2), parts_result.parts[1].part_number);
}

test "SqliteMetadataStore: abort multipart upload" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{ .name = "abort-bucket", .creation_date = "2026-01-01T00:00:00.000Z", .region = "us-east-1", .owner_id = "owner" });

    try iface.createMultipartUpload(.{
        .upload_id = "abort-upload",
        .bucket = "abort-bucket",
        .key = "aborted.bin",
        .initiated = "2026-01-01T12:00:00.000Z",
        .owner_id = "owner",
    });
    try iface.putPartMeta("abort-upload", .{ .part_number = 1, .size = 100, .etag = "\"e\"", .last_modified = "2026-01-01T12:01:00.000Z" });

    try iface.abortMultipartUpload("abort-upload");

    // Upload should be gone
    const upload = try iface.getMultipartUpload("abort-upload");
    try std.testing.expect(upload == null);

    // Abort nonexistent upload should error
    try std.testing.expectError(error.NoSuchUpload, iface.abortMultipartUpload("abort-upload"));
}

test "SqliteMetadataStore: count buckets and objects" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try std.testing.expectEqual(@as(u64, 0), try iface.countBuckets());
    try std.testing.expectEqual(@as(u64, 0), try iface.countObjects());

    try iface.createBucket(.{ .name = "count-bucket", .creation_date = "2026-01-01T00:00:00.000Z", .region = "us-east-1", .owner_id = "owner" });
    try std.testing.expectEqual(@as(u64, 1), try iface.countBuckets());

    try iface.putObjectMeta(.{ .bucket = "count-bucket", .key = "a.txt", .size = 1, .etag = "\"a\"", .content_type = "text/plain", .last_modified = "2026-01-01T00:00:00.000Z", .storage_class = "STANDARD" });
    try iface.putObjectMeta(.{ .bucket = "count-bucket", .key = "b.txt", .size = 2, .etag = "\"b\"", .content_type = "text/plain", .last_modified = "2026-01-01T00:00:00.000Z", .storage_class = "STANDARD" });
    try std.testing.expectEqual(@as(u64, 2), try iface.countObjects());
}

test "SqliteMetadataStore: update bucket ACL" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{ .name = "acl-bucket", .creation_date = "2026-01-01T00:00:00.000Z", .region = "us-east-1", .owner_id = "owner" });

    const new_acl = "{\"owner\":{\"id\":\"owner\"},\"grants\":[{\"permission\":\"READ\"}]}";
    try iface.updateBucketAcl("acl-bucket", new_acl);

    const bucket = try iface.getBucket("acl-bucket");
    try std.testing.expect(bucket != null);
    const b = bucket.?;
    defer b.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(new_acl, b.acl);
}

test "SqliteMetadataStore: duplicate bucket returns error" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{ .name = "dup-bucket", .creation_date = "2026-01-01T00:00:00.000Z", .region = "us-east-1", .owner_id = "owner" });

    try std.testing.expectError(error.BucketAlreadyExists, iface.createBucket(.{
        .name = "dup-bucket",
        .creation_date = "2026-01-02T00:00:00.000Z",
        .region = "us-west-2",
        .owner_id = "owner2",
    }));
}

test "SqliteMetadataStore: object upsert replaces existing" {
    var ms = try SqliteMetadataStore.init(std.testing.allocator, ":memory:");
    defer ms.deinit();
    const iface = ms.metadataStore();

    try iface.createBucket(.{ .name = "upsert-bucket", .creation_date = "2026-01-01T00:00:00.000Z", .region = "us-east-1", .owner_id = "owner" });

    try iface.putObjectMeta(.{ .bucket = "upsert-bucket", .key = "file.txt", .size = 10, .etag = "\"old\"", .content_type = "text/plain", .last_modified = "2026-01-01T00:00:00.000Z", .storage_class = "STANDARD" });
    try iface.putObjectMeta(.{ .bucket = "upsert-bucket", .key = "file.txt", .size = 20, .etag = "\"new\"", .content_type = "text/html", .last_modified = "2026-01-02T00:00:00.000Z", .storage_class = "STANDARD" });

    const obj = try iface.getObjectMeta("upsert-bucket", "file.txt");
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
        if (o.user_metadata) |um| std.testing.allocator.free(um);
    }
    try std.testing.expectEqual(@as(u64, 20), o.size);
    try std.testing.expectEqualStrings("\"new\"", o.etag);
    try std.testing.expectEqualStrings("text/html", o.content_type);
}
