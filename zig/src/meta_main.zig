const std = @import("std");
const serialization = @import("serialization.zig");

fn writeAll(fd: std.posix.fd_t, data: []const u8) !void {
    var written: usize = 0;
    while (written < data.len) {
        const n = try std.posix.write(fd, data[written..]);
        written += n;
    }
}

fn printErr(comptime fmt: []const u8, args: anytype) void {
    var buf: [4096]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, fmt, args) catch return;
    writeAll(std.posix.STDERR_FILENO, msg) catch {};
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        printErr("Usage: bleepstore-meta <export|import> [flags]\n", .{});
        std.process.exit(1);
    }

    const command = args[1];
    if (std.mem.eql(u8, command, "export")) {
        runExport(allocator, args[2..]) catch |err| {
            printErr("Error: {}\n", .{err});
            std.process.exit(1);
        };
    } else if (std.mem.eql(u8, command, "import")) {
        runImport(allocator, args[2..]) catch |err| {
            printErr("Error: {}\n", .{err});
            std.process.exit(1);
        };
    } else {
        printErr("Unknown command: {s}\n", .{command});
        std.process.exit(1);
    }
}

fn runExport(allocator: std.mem.Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var output_path: []const u8 = "-";
    var include_credentials = false;

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--db") and i + 1 < args.len) {
            i += 1;
            db_path = args[i];
        } else if (std.mem.eql(u8, args[i], "--output") and i + 1 < args.len) {
            i += 1;
            output_path = args[i];
        } else if (std.mem.eql(u8, args[i], "--include-credentials")) {
            include_credentials = true;
        }
    }

    if (db_path == null) {
        printErr("Error: --db is required\n", .{});
        std.process.exit(1);
    }

    const db_z = try allocator.dupeZ(u8, db_path.?);
    defer allocator.free(db_z);

    const opts = serialization.ExportOptions{
        .include_credentials = include_credentials,
    };

    const result = try serialization.exportMetadata(allocator, db_z, opts);
    defer allocator.free(result);

    if (std.mem.eql(u8, output_path, "-")) {
        try writeAll(std.posix.STDOUT_FILENO, result);
        try writeAll(std.posix.STDOUT_FILENO, "\n");
    } else {
        const file = try std.fs.cwd().createFile(output_path, .{});
        defer file.close();
        try file.writeAll(result);
        try file.writeAll("\n");
        printErr("Exported to {s}\n", .{output_path});
    }
}

fn runImport(allocator: std.mem.Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var input_path: []const u8 = "-";
    var replace = false;

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--db") and i + 1 < args.len) {
            i += 1;
            db_path = args[i];
        } else if (std.mem.eql(u8, args[i], "--input") and i + 1 < args.len) {
            i += 1;
            input_path = args[i];
        } else if (std.mem.eql(u8, args[i], "--replace")) {
            replace = true;
        }
    }

    if (db_path == null) {
        printErr("Error: --db is required\n", .{});
        std.process.exit(1);
    }

    const db_z = try allocator.dupeZ(u8, db_path.?);
    defer allocator.free(db_z);

    var json_data: []u8 = undefined;
    if (std.mem.eql(u8, input_path, "-")) {
        const stdin = std.fs.File{ .handle = std.posix.STDIN_FILENO };
        json_data = try stdin.readToEndAlloc(allocator, 100 * 1024 * 1024);
    } else {
        const file = try std.fs.cwd().openFile(input_path, .{});
        defer file.close();
        json_data = try file.readToEndAlloc(allocator, 100 * 1024 * 1024);
    }
    defer allocator.free(json_data);

    const opts = serialization.ImportOptions{ .replace = replace };
    const result = try serialization.importMetadata(allocator, db_z, json_data, opts);

    for (serialization.ALL_TABLES, 0..) |table, idx| {
        const count = result.counts[idx];
        const skipped = result.skipped[idx];
        if (count > 0 or skipped > 0) {
            printErr("  {s}: {d} imported", .{ table, count });
            if (skipped > 0) printErr(", {d} skipped", .{skipped});
            printErr("\n", .{});
        }
    }
    if (result.warning_count > 0) {
        printErr("  {d} warning(s)\n", .{result.warning_count});
    }
}
