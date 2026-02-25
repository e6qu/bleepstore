const std = @import("std");
const c = @cImport({
    @cInclude("sqlite3.h");
});

/// Bind text with SQLITE_TRANSIENT semantics.
extern "c" fn sqlite3_bind_text(stmt: *c.sqlite3_stmt, index: c_int, text: [*]const u8, len: c_int, destructor: isize) c_int;

pub const VERSION = "0.1.0";
pub const EXPORT_VERSION: i32 = 1;

pub const ALL_TABLES = [_][]const u8{
    "buckets",
    "objects",
    "multipart_uploads",
    "multipart_parts",
    "credentials",
};

const JSON_FIELDS = [_][]const u8{ "acl", "user_metadata" };
const BOOL_FIELDS = [_][]const u8{ "delete_marker", "active" };

const ColumnDef = struct {
    table: []const u8,
    columns: []const []const u8,
    order_by: []const u8,
};

const TABLE_DEFS = [_]ColumnDef{
    .{ .table = "buckets", .columns = &.{ "name", "region", "owner_id", "owner_display", "acl", "created_at" }, .order_by = "name" },
    .{ .table = "objects", .columns = &.{ "bucket", "key", "size", "etag", "content_type", "content_encoding", "content_language", "content_disposition", "cache_control", "expires", "storage_class", "acl", "user_metadata", "last_modified", "delete_marker" }, .order_by = "bucket, key" },
    .{ .table = "multipart_uploads", .columns = &.{ "upload_id", "bucket", "key", "content_type", "content_encoding", "content_language", "content_disposition", "cache_control", "expires", "storage_class", "acl", "user_metadata", "owner_id", "owner_display", "initiated_at" }, .order_by = "upload_id" },
    .{ .table = "multipart_parts", .columns = &.{ "upload_id", "part_number", "size", "etag", "last_modified" }, .order_by = "upload_id, part_number" },
    .{ .table = "credentials", .columns = &.{ "access_key_id", "secret_key", "owner_id", "display_name", "active", "created_at" }, .order_by = "access_key_id" },
};

const DELETE_ORDER = [_][]const u8{ "multipart_parts", "multipart_uploads", "objects", "buckets", "credentials" };
const INSERT_ORDER = [_][]const u8{ "buckets", "objects", "multipart_uploads", "multipart_parts", "credentials" };

fn isJsonField(col: []const u8) bool {
    for (JSON_FIELDS) |f| {
        if (std.mem.eql(u8, col, f)) return true;
    }
    return false;
}

fn isBoolField(col: []const u8) bool {
    for (BOOL_FIELDS) |f| {
        if (std.mem.eql(u8, col, f)) return true;
    }
    return false;
}

fn getTableDef(table: []const u8) ?ColumnDef {
    for (TABLE_DEFS) |def| {
        if (std.mem.eql(u8, table, def.table)) return def;
    }
    return null;
}

pub const ExportOptions = struct {
    tables: []const []const u8 = &ALL_TABLES,
    include_credentials: bool = false,
};

pub const ImportOptions = struct {
    replace: bool = false,
};

pub const ImportResult = struct {
    counts: [5]usize = .{ 0, 0, 0, 0, 0 },
    skipped: [5]usize = .{ 0, 0, 0, 0, 0 },
    warning_count: usize = 0,

    fn tableIndex(table: []const u8) ?usize {
        for (ALL_TABLES, 0..) |t, i| {
            if (std.mem.eql(u8, table, t)) return i;
        }
        return null;
    }

    pub fn getCount(self: *const ImportResult, table: []const u8) usize {
        if (tableIndex(table)) |idx| return self.counts[idx];
        return 0;
    }

    pub fn getSkipped(self: *const ImportResult, table: []const u8) usize {
        if (tableIndex(table)) |idx| return self.skipped[idx];
        return 0;
    }
};

fn openDb(path: [*:0]const u8, readonly: bool) !*c.sqlite3 {
    var db: ?*c.sqlite3 = null;
    const flags: c_int = if (readonly) c.SQLITE_OPEN_READONLY else c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE;
    const rc = c.sqlite3_open_v2(path, &db, flags, null);
    if (rc != c.SQLITE_OK) {
        if (db) |d| _ = c.sqlite3_close(d);
        return error.SqliteOpenFailed;
    }
    return db.?;
}

fn execSql(db: *c.sqlite3, sql: [*:0]const u8) !void {
    const rc = c.sqlite3_exec(db, sql, null, null, null);
    if (rc != c.SQLITE_OK) return error.SqliteExecFailed;
}

fn getSchemaVersion(db: *c.sqlite3) i32 {
    var stmt: ?*c.sqlite3_stmt = null;
    const rc = c.sqlite3_prepare_v2(db, "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1", -1, &stmt, null);
    if (rc != c.SQLITE_OK) return 1;
    defer _ = c.sqlite3_finalize(stmt);
    if (c.sqlite3_step(stmt.?) != c.SQLITE_ROW) return 1;
    return c.sqlite3_column_int(stmt.?, 0);
}

/// A simple growable byte buffer using an allocator.
const Buffer = struct {
    data: []u8,
    len: usize,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) Buffer {
        return .{ .data = &.{}, .len = 0, .allocator = allocator };
    }

    fn deinit(self: *Buffer) void {
        if (self.data.len > 0) self.allocator.free(self.data);
    }

    fn toOwnedSlice(self: *Buffer) []u8 {
        const result = self.allocator.alloc(u8, self.len) catch return &.{};
        @memcpy(result, self.data[0..self.len]);
        self.deinit();
        self.data = &.{};
        self.len = 0;
        return result;
    }

    fn ensureCapacity(self: *Buffer, needed: usize) !void {
        const required = self.len + needed;
        if (required <= self.data.len) return;
        var new_cap = if (self.data.len == 0) @as(usize, 4096) else self.data.len;
        while (new_cap < required) new_cap *= 2;
        const new_data = try self.allocator.alloc(u8, new_cap);
        if (self.len > 0) @memcpy(new_data[0..self.len], self.data[0..self.len]);
        if (self.data.len > 0) self.allocator.free(self.data);
        self.data = new_data;
    }

    fn writeAll(self: *Buffer, bytes: []const u8) !void {
        try self.ensureCapacity(bytes.len);
        @memcpy(self.data[self.len..][0..bytes.len], bytes);
        self.len += bytes.len;
    }

    fn print(self: *Buffer, comptime fmt: []const u8, args: anytype) !void {
        // Format into temp buffer, then append.
        var tmp: [4096]u8 = undefined;
        const result = std.fmt.bufPrint(&tmp, fmt, args) catch {
            // Fallback: allocate.
            const allocated = try std.fmt.allocPrint(self.allocator, fmt, args);
            defer self.allocator.free(allocated);
            try self.writeAll(allocated);
            return;
        };
        try self.writeAll(result);
    }
};

/// Export metadata from SQLite to a JSON string.
pub fn exportMetadata(allocator: std.mem.Allocator, db_path: [*:0]const u8, opts: ExportOptions) ![]u8 {
    const db = try openDb(db_path, true);
    defer _ = c.sqlite3_close(db);

    const schema_version = getSchemaVersion(db);

    const now_str = try formatNow(allocator);
    defer allocator.free(now_str);

    var buf = Buffer.init(allocator);
    errdefer buf.deinit();

    try buf.writeAll("{\n");

    // Sort present table names for sorted JSON keys.
    var present: [5][]const u8 = undefined;
    var present_count: usize = 0;
    for (opts.tables) |table| {
        if (getTableDef(table) != null) {
            present[present_count] = table;
            present_count += 1;
        }
    }
    // Simple insertion sort for up to 5 elements.
    var si: usize = 1;
    while (si < present_count) : (si += 1) {
        const key = present[si];
        var sj: usize = si;
        while (sj > 0 and std.mem.order(u8, present[sj - 1], key) == .gt) : (sj -= 1) {
            present[sj] = present[sj - 1];
        }
        present[sj] = key;
    }

    // Write envelope.
    try buf.writeAll("  \"bleepstore_export\": {\n");
    try buf.print("    \"exported_at\": \"{s}\",\n", .{now_str});
    try buf.print("    \"schema_version\": {d},\n", .{schema_version});
    try buf.print("    \"source\": \"zig/{s}\",\n", .{VERSION});
    try buf.print("    \"version\": {d}\n", .{EXPORT_VERSION});
    try buf.writeAll("  }");

    // Write each table.
    for (present[0..present_count]) |table| {
        try buf.writeAll(",\n");
        const def = getTableDef(table).?;
        try exportTable(allocator, db, def, table, opts.include_credentials, &buf);
    }

    try buf.writeAll("\n}");

    return buf.toOwnedSlice();
}

fn exportTable(allocator: std.mem.Allocator, db: *c.sqlite3, def: ColumnDef, table: []const u8, include_credentials: bool, buf: *Buffer) !void {
    var query_buf: [512]u8 = undefined;
    const query = try std.fmt.bufPrintZ(&query_buf, "SELECT * FROM {s} ORDER BY {s}", .{ def.table, def.order_by });

    var stmt: ?*c.sqlite3_stmt = null;
    const rc = c.sqlite3_prepare_v2(db, query.ptr, -1, &stmt, null);
    if (rc != c.SQLITE_OK) return error.SqlitePrepareFailed;
    defer _ = c.sqlite3_finalize(stmt);

    try buf.print("  \"{s}\": [", .{table});

    // Sort columns for JSON key ordering.
    var sorted_cols: [20]usize = undefined; // indices into def.columns
    for (0..def.columns.len) |i| sorted_cols[i] = i;
    // Insertion sort by column name.
    var sci: usize = 1;
    while (sci < def.columns.len) : (sci += 1) {
        const key_idx = sorted_cols[sci];
        var scj: usize = sci;
        while (scj > 0 and std.mem.order(u8, def.columns[sorted_cols[scj - 1]], def.columns[key_idx]) == .gt) : (scj -= 1) {
            sorted_cols[scj] = sorted_cols[scj - 1];
        }
        sorted_cols[scj] = key_idx;
    }

    var row_idx: usize = 0;
    while (c.sqlite3_step(stmt.?) == c.SQLITE_ROW) {
        if (row_idx > 0) try buf.writeAll(",");
        try buf.writeAll("\n    {");

        var first = true;
        for (sorted_cols[0..def.columns.len]) |col_idx| {
            const col = def.columns[col_idx];

            if (!first) try buf.writeAll(",");
            first = false;
            try buf.writeAll("\n      ");
            try buf.print("\"{s}\": ", .{col});

            // Handle credential redaction.
            if (std.mem.eql(u8, table, "credentials") and std.mem.eql(u8, col, "secret_key") and !include_credentials) {
                try buf.writeAll("\"REDACTED\"");
                continue;
            }

            try writeColumnValue(allocator, stmt.?, @intCast(col_idx), col, buf);
        }

        try buf.writeAll("\n    }");
        row_idx += 1;
    }

    if (row_idx > 0) {
        try buf.writeAll("\n  ]");
    } else {
        try buf.writeAll("]");
    }
}

fn writeColumnValue(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, col_idx: c_int, col_name: []const u8, buf: *Buffer) !void {
    const col_type = c.sqlite3_column_type(stmt, col_idx);

    if (col_type == c.SQLITE_NULL) {
        try buf.writeAll("null");
        return;
    }

    if (isJsonField(col_name)) {
        const text_ptr = c.sqlite3_column_text(stmt, col_idx);
        if (text_ptr == null) {
            try buf.writeAll("null");
            return;
        }
        const len: usize = @intCast(c.sqlite3_column_bytes(stmt, col_idx));
        const text = text_ptr[0..len];

        const parsed = std.json.parseFromSlice(std.json.Value, allocator, text, .{}) catch {
            try buf.writeAll("{}");
            return;
        };
        defer parsed.deinit();
        try writeJsonValueSorted(allocator, parsed.value, buf);
        return;
    }

    if (isBoolField(col_name)) {
        const val = c.sqlite3_column_int(stmt, col_idx);
        try buf.writeAll(if (val != 0) "true" else "false");
        return;
    }

    if (col_type == c.SQLITE_INTEGER) {
        const val = c.sqlite3_column_int64(stmt, col_idx);
        try buf.print("{d}", .{val});
    } else {
        const text_ptr = c.sqlite3_column_text(stmt, col_idx);
        if (text_ptr == null) {
            try buf.writeAll("null");
            return;
        }
        const len: usize = @intCast(c.sqlite3_column_bytes(stmt, col_idx));
        const text = text_ptr[0..len];
        try writeJsonString(text, buf);
    }
}

fn writeJsonString(s: []const u8, buf: *Buffer) !void {
    try buf.writeAll("\"");
    for (s) |ch| {
        switch (ch) {
            '"' => try buf.writeAll("\\\""),
            '\\' => try buf.writeAll("\\\\"),
            '\n' => try buf.writeAll("\\n"),
            '\r' => try buf.writeAll("\\r"),
            '\t' => try buf.writeAll("\\t"),
            else => {
                if (ch < 0x20) {
                    try buf.print("\\u{x:0>4}", .{@as(u16, ch)});
                } else {
                    var tmp: [1]u8 = .{ch};
                    try buf.writeAll(&tmp);
                }
            },
        }
    }
    try buf.writeAll("\"");
}

fn writeJsonValueSorted(allocator: std.mem.Allocator, value: std.json.Value, buf: *Buffer) !void {
    switch (value) {
        .null => try buf.writeAll("null"),
        .bool => |b| try buf.writeAll(if (b) "true" else "false"),
        .integer => |i| try buf.print("{d}", .{i}),
        .float => |f| try buf.print("{d}", .{f}),
        .string => |s| try writeJsonString(s, buf),
        .array => |arr| {
            try buf.writeAll("[");
            for (arr.items, 0..) |item, i| {
                if (i > 0) try buf.writeAll(", ");
                try writeJsonValueSorted(allocator, item, buf);
            }
            try buf.writeAll("]");
        },
        .object => |obj| {
            // Sort keys.
            const keys = obj.keys();
            const sorted_keys = try allocator.alloc([]const u8, keys.len);
            defer allocator.free(sorted_keys);
            @memcpy(sorted_keys, keys);
            std.mem.sort([]const u8, sorted_keys, {}, struct {
                fn lessThan(_: void, a: []const u8, b: []const u8) bool {
                    return std.mem.order(u8, a, b) == .lt;
                }
            }.lessThan);

            try buf.writeAll("{");
            var first = true;
            for (sorted_keys) |k| {
                if (!first) try buf.writeAll(", ");
                first = false;
                try writeJsonString(k, buf);
                try buf.writeAll(": ");
                try writeJsonValueSorted(allocator, obj.get(k).?, buf);
            }
            try buf.writeAll("}");
        },
        .number_string => |s| try buf.writeAll(s),
    }
}

/// Import metadata from JSON into SQLite.
pub fn importMetadata(allocator: std.mem.Allocator, db_path: [*:0]const u8, json_str: []const u8, opts: ImportOptions) !ImportResult {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_str, .{});
    defer parsed.deinit();

    const root = parsed.value.object;

    // Validate envelope.
    const envelope = root.get("bleepstore_export") orelse return error.MissingEnvelope;
    const version_val = envelope.object.get("version") orelse return error.MissingVersion;
    const version = version_val.integer;
    if (version < 1 or version > EXPORT_VERSION) return error.UnsupportedVersion;

    const db = try openDb(db_path, false);
    defer _ = c.sqlite3_close(db);

    try execSql(db, "PRAGMA foreign_keys = ON;");
    try execSql(db, "BEGIN;");
    errdefer _ = c.sqlite3_exec(db, "ROLLBACK;", null, null, null);

    var result = ImportResult{};

    if (opts.replace) {
        for (DELETE_ORDER) |table| {
            if (root.get(table) != null) {
                var del_buf: [128]u8 = undefined;
                const del_sql = try std.fmt.bufPrintZ(&del_buf, "DELETE FROM {s}", .{table});
                try execSql(db, del_sql.ptr);
            }
        }
    }

    for (INSERT_ORDER) |table| {
        const rows_val = root.get(table) orelse continue;
        const rows = rows_val.array.items;
        const def = getTableDef(table) orelse continue;
        const tidx = ImportResult.tableIndex(table) orelse continue;

        for (rows) |row_val| {
            const row = row_val.object;

            // Skip redacted credentials.
            if (std.mem.eql(u8, table, "credentials")) {
                if (row.get("secret_key")) |sk| {
                    if (sk == .string and std.mem.eql(u8, sk.string, "REDACTED")) {
                        result.skipped[tidx] += 1;
                        result.warning_count += 1;
                        continue;
                    }
                }
            }

            // Build INSERT statement.
            var sql_buf: [1024]u8 = undefined;
            var sql_pos: usize = 0;
            if (opts.replace) {
                const prefix = try std.fmt.bufPrint(sql_buf[sql_pos..], "INSERT INTO {s} (", .{table});
                sql_pos += prefix.len;
            } else {
                const prefix = try std.fmt.bufPrint(sql_buf[sql_pos..], "INSERT OR IGNORE INTO {s} (", .{table});
                sql_pos += prefix.len;
            }
            for (def.columns, 0..) |col, i| {
                if (i > 0) {
                    sql_buf[sql_pos] = ',';
                    sql_buf[sql_pos + 1] = ' ';
                    sql_pos += 2;
                }
                @memcpy(sql_buf[sql_pos..][0..col.len], col);
                sql_pos += col.len;
            }
            const suffix = try std.fmt.bufPrint(sql_buf[sql_pos..], ") VALUES ({s})", .{placeholders(def.columns.len)});
            sql_pos += suffix.len;
            sql_buf[sql_pos] = 0;
            const sql_z: [*:0]const u8 = @ptrCast(sql_buf[0..sql_pos :0]);

            var stmt: ?*c.sqlite3_stmt = null;
            const prepare_rc = c.sqlite3_prepare_v2(db, sql_z, -1, &stmt, null);
            if (prepare_rc != c.SQLITE_OK) {
                result.skipped[tidx] += 1;
                continue;
            }
            defer _ = c.sqlite3_finalize(stmt);

            // Bind values.
            for (def.columns, 0..) |col, i| {
                const bind_idx: c_int = @intCast(i + 1);
                const val = row.get(col) orelse std.json.Value{ .null = {} };
                try bindJsonValue(allocator, stmt.?, bind_idx, col, val);
            }

            const step_rc = c.sqlite3_step(stmt.?);
            if (step_rc == c.SQLITE_DONE) {
                if (c.sqlite3_changes(db) > 0) {
                    result.counts[tidx] += 1;
                } else {
                    result.skipped[tidx] += 1;
                }
            } else {
                result.skipped[tidx] += 1;
            }
        }
    }

    try execSql(db, "COMMIT;");
    return result;
}

fn placeholders(n: usize) []const u8 {
    const all = "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?";
    if (n == 0) return "";
    const len = n * 3 - 2;
    return all[0..len];
}

fn bindJsonValue(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, idx: c_int, col: []const u8, val: std.json.Value) !void {
    if (val == .null) {
        _ = c.sqlite3_bind_null(stmt, idx);
        return;
    }

    if (isJsonField(col)) {
        // Serialize JSON value to a compact string.
        var json_buf = Buffer.init(allocator);
        defer json_buf.deinit();
        try writeJsonValueSorted(allocator, val, &json_buf);
        const json_str = json_buf.data[0..json_buf.len];
        _ = sqlite3_bind_text(stmt, idx, json_str.ptr, @intCast(json_str.len), -1);
        return;
    }

    if (isBoolField(col)) {
        const b: c_int = switch (val) {
            .bool => |bv| if (bv) 1 else 0,
            else => 0,
        };
        _ = c.sqlite3_bind_int(stmt, idx, b);
        return;
    }

    switch (val) {
        .integer => |i| _ = c.sqlite3_bind_int64(stmt, idx, i),
        .float => |f| _ = c.sqlite3_bind_double(stmt, idx, f),
        .string => |s| _ = sqlite3_bind_text(stmt, idx, s.ptr, @intCast(s.len), -1),
        .bool => |b| _ = c.sqlite3_bind_int(stmt, idx, if (b) 1 else 0),
        else => _ = c.sqlite3_bind_null(stmt, idx),
    }
}

fn formatNow(allocator: std.mem.Allocator) ![]u8 {
    const epoch_secs: u64 = @intCast(std.time.timestamp());
    const es = std.time.epoch.EpochSeconds{ .secs = epoch_secs };
    const day_seconds = es.getDaySeconds();
    const year_day = es.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();

    return try std.fmt.allocPrint(allocator, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.000Z", .{
        year_day.year,
        @intFromEnum(month_day.month),
        month_day.day_index + 1,
        day_seconds.getHoursIntoDay(),
        day_seconds.getMinutesIntoHour(),
        day_seconds.getSecondsIntoMinute(),
    });
}

test "placeholders" {
    try std.testing.expectEqualStrings("?", placeholders(1));
    try std.testing.expectEqualStrings("?, ?", placeholders(2));
    try std.testing.expectEqualStrings("?, ?, ?", placeholders(3));
    try std.testing.expectEqualStrings("?, ?, ?, ?, ?", placeholders(5));
}
