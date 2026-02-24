/// Integration tests for the BleepStore Zig implementation.
///
/// These tests make real HTTP requests to a running BleepStore server on port 9013.
/// They sign requests with AWS SigV4 and verify responses match S3 API expectations.
///
/// Usage:
///   1. Start the server: zig build run -- --config ../bleepstore.example.yaml --port 9013
///   2. Run these tests: zig build e2e
///
/// Uses raw TCP sockets for HTTP to avoid std.http.Client API instability.
const std = @import("std");
const auth_mod = @import("auth.zig");

const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const Sha256 = std.crypto.hash.sha2.Sha256;

// Server connection details
const HOST = "localhost";
const PORT: u16 = 9013;
const ACCESS_KEY = "bleepstore";
const SECRET_KEY = "bleepstore-secret";
const REGION = "us-east-1";
const HOST_HEADER = "localhost:9013";

// ---- Raw HTTP Client with SigV4 ----

const Response = struct {
    allocator: std.mem.Allocator,
    status: u16,
    body: []const u8,
    headers_raw: []const u8, // Raw header block for searching

    fn deinit(self: *Response) void {
        self.allocator.free(self.body);
        self.allocator.free(self.headers_raw);
    }

    fn containsStr(self: *const Response, needle: []const u8) bool {
        return std.mem.indexOf(u8, self.body, needle) != null;
    }

    /// Find a header value by name (case-insensitive search in raw headers).
    fn getHeader(self: *const Response, name: []const u8) ?[]const u8 {
        // Search for "\r\nname: " in the raw headers
        var search_buf: [256]u8 = undefined;
        const search = std.fmt.bufPrint(&search_buf, "\r\n{s}: ", .{name}) catch return null;

        if (std.mem.indexOf(u8, self.headers_raw, search)) |idx| {
            const val_start = idx + search.len;
            const val_end = std.mem.indexOf(u8, self.headers_raw[val_start..], "\r\n") orelse
                (self.headers_raw.len - val_start);
            return self.headers_raw[val_start .. val_start + val_end];
        }

        // Also try lowercase
        var lower_search_buf: [256]u8 = undefined;
        const lower_name = blk: {
            if (name.len > 200) return null;
            for (name, 0..) |ch, i| {
                lower_search_buf[i] = std.ascii.toLower(ch);
            }
            break :blk lower_search_buf[0..name.len];
        };
        const lower_search = std.fmt.bufPrint(&search_buf, "\r\n{s}: ", .{lower_name}) catch return null;

        if (std.mem.indexOf(u8, self.headers_raw, lower_search)) |idx| {
            const val_start = idx + lower_search.len;
            const val_end = std.mem.indexOf(u8, self.headers_raw[val_start..], "\r\n") orelse
                (self.headers_raw.len - val_start);
            return self.headers_raw[val_start .. val_start + val_end];
        }

        return null;
    }
};

fn makeRequest(
    alloc: std.mem.Allocator,
    method: []const u8,
    path: []const u8,
    body: ?[]const u8,
    extra_headers: ?[]const [2][]const u8,
) !Response {
    // Parse path and query from the full path
    const query_start = std.mem.indexOfScalar(u8, path, '?');
    const path_only = if (query_start) |qs| path[0..qs] else path;
    const raw_query = if (query_start) |qs| path[qs + 1 ..] else "";

    // Compute timestamp
    const now_ts = std.time.timestamp();
    const epoch_secs: u64 = @intCast(if (now_ts < 0) 0 else now_ts);
    var date_buf: [16]u8 = undefined;
    const amz_date = formatAmzDate(&date_buf, epoch_secs);
    const date_stamp = amz_date[0..8];

    // Compute payload hash
    const payload = body orelse "";
    var payload_hash_bytes: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(payload, &payload_hash_bytes, .{});
    const payload_hash = std.fmt.bytesToHex(payload_hash_bytes, .lower);

    // Collect all headers that will be signed
    const ExtraH = struct { name: []const u8, value: []const u8 };
    var sign_headers: std.ArrayList(ExtraH) = .empty;
    defer sign_headers.deinit(alloc);

    try sign_headers.append(alloc, .{ .name = "host", .value = HOST_HEADER });
    try sign_headers.append(alloc, .{ .name = "x-amz-content-sha256", .value = &payload_hash });
    try sign_headers.append(alloc, .{ .name = "x-amz-date", .value = amz_date });

    // Add extra headers (lowercased)
    var extra_owned: std.ArrayList([]const u8) = .empty;
    defer {
        for (extra_owned.items) |s| alloc.free(s);
        extra_owned.deinit(alloc);
    }

    if (extra_headers) |hdrs| {
        for (hdrs) |h| {
            const lower = try std.ascii.allocLowerString(alloc, h[0]);
            try extra_owned.append(alloc, lower);
            try sign_headers.append(alloc, .{ .name = lower, .value = h[1] });
        }
    }

    // Sort by name
    std.mem.sort(ExtraH, sign_headers.items, {}, struct {
        fn lt(_: void, a: ExtraH, b: ExtraH) bool {
            return std.mem.order(u8, a.name, b.name) == .lt;
        }
    }.lt);

    // Build canonical headers and signed headers string
    var ch_buf: std.ArrayList(u8) = .empty;
    defer ch_buf.deinit(alloc);
    var sh_buf: std.ArrayList(u8) = .empty;
    defer sh_buf.deinit(alloc);

    for (sign_headers.items, 0..) |h, i| {
        try ch_buf.appendSlice(alloc, h.name);
        try ch_buf.append(alloc, ':');
        try ch_buf.appendSlice(alloc, std.mem.trim(u8, h.value, " \t"));
        try ch_buf.append(alloc, '\n');
        if (i > 0) try sh_buf.append(alloc, ';');
        try sh_buf.appendSlice(alloc, h.name);
    }

    const signed_headers = try alloc.dupe(u8, sh_buf.items);
    defer alloc.free(signed_headers);

    // Build canonical URI
    const canonical_uri = try auth_mod.buildCanonicalUri(alloc, path_only);
    defer alloc.free(canonical_uri);

    // Build canonical query string
    const canonical_query = try auth_mod.buildCanonicalQueryString(alloc, raw_query);
    defer alloc.free(canonical_query);

    // Build canonical request
    const canonical_request = try auth_mod.createCanonicalRequest(
        alloc,
        method,
        canonical_uri,
        canonical_query,
        ch_buf.items,
        signed_headers,
        &payload_hash,
    );
    defer alloc.free(canonical_request);

    // String to sign
    const scope = try std.fmt.allocPrint(alloc, "{s}/{s}/s3/aws4_request", .{ date_stamp, REGION });
    defer alloc.free(scope);
    const string_to_sign = try auth_mod.computeStringToSign(alloc, amz_date, scope, canonical_request);
    defer alloc.free(string_to_sign);

    // Compute signature
    const signing_key = auth_mod.deriveSigningKey(SECRET_KEY, date_stamp, REGION, "s3");
    var sig_mac: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&sig_mac, string_to_sign, &signing_key);
    const signature = std.fmt.bytesToHex(sig_mac, .lower);

    // Build Authorization header
    const auth_header = try std.fmt.allocPrint(alloc, "AWS4-HMAC-SHA256 Credential={s}/{s}/{s}/s3/aws4_request, SignedHeaders={s}, Signature={s}", .{
        ACCESS_KEY, date_stamp, REGION, signed_headers, &signature,
    });
    defer alloc.free(auth_header);

    // Build the raw HTTP request
    var req_buf: std.ArrayList(u8) = .empty;
    defer req_buf.deinit(alloc);

    // Request line
    try req_buf.appendSlice(alloc, method);
    try req_buf.append(alloc, ' ');
    try req_buf.appendSlice(alloc, path);
    try req_buf.appendSlice(alloc, " HTTP/1.1\r\n");

    // Headers
    try appendHeader(alloc, &req_buf, "Host", HOST_HEADER);
    try appendHeader(alloc, &req_buf, "Authorization", auth_header);
    try appendHeader(alloc, &req_buf, "x-amz-date", amz_date);
    try appendHeader(alloc, &req_buf, "x-amz-content-sha256", &payload_hash);

    if (extra_headers) |hdrs| {
        for (hdrs) |h| {
            try appendHeader(alloc, &req_buf, h[0], h[1]);
        }
    }

    if (body) |b| {
        const cl = try std.fmt.allocPrint(alloc, "{d}", .{b.len});
        defer alloc.free(cl);
        try appendHeader(alloc, &req_buf, "Content-Length", cl);
    }

    try appendHeader(alloc, &req_buf, "Connection", "close");
    try req_buf.appendSlice(alloc, "\r\n");

    // Connect and send (headers and body separately to handle large bodies)
    const address = try std.net.Address.resolveIp("127.0.0.1", PORT);
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    // Send headers
    try stream.writeAll(req_buf.items);

    // Send body in chunks if present (avoids memory duplication for large bodies)
    if (body) |b| {
        var offset: usize = 0;
        while (offset < b.len) {
            const chunk_size = @min(b.len - offset, 65536);
            try stream.writeAll(b[offset .. offset + chunk_size]);
            offset += chunk_size;
        }
    }

    // Read response
    var resp_buf: std.ArrayList(u8) = .empty;
    defer resp_buf.deinit(alloc);

    var read_buf: [8192]u8 = undefined;
    while (true) {
        const n = stream.read(&read_buf) catch break;
        if (n == 0) break;
        try resp_buf.appendSlice(alloc, read_buf[0..n]);
    }

    // Parse response
    const resp_data = try resp_buf.toOwnedSlice(alloc);
    errdefer alloc.free(resp_data);

    // Find end of headers
    const header_end = std.mem.indexOf(u8, resp_data, "\r\n\r\n") orelse {
        alloc.free(resp_data);
        return error.InvalidResponse;
    };

    // Parse status code from first line: "HTTP/1.1 200 OK\r\n..."
    const first_line_end = std.mem.indexOf(u8, resp_data, "\r\n") orelse {
        alloc.free(resp_data);
        return error.InvalidResponse;
    };
    const first_line = resp_data[0..first_line_end];

    // Extract status code (chars 9-12 in "HTTP/1.1 200 ...")
    var status: u16 = 0;
    if (first_line.len >= 12 and first_line[8] == ' ') {
        status = std.fmt.parseInt(u16, first_line[9..12], 10) catch 0;
    }

    // Save headers block
    const headers_raw = try alloc.dupe(u8, resp_data[0..header_end]);
    errdefer alloc.free(headers_raw);

    // Extract body (handle chunked transfer encoding)
    const raw_body_start = header_end + 4;
    const raw_body = resp_data[raw_body_start..];

    // Check for chunked transfer encoding
    const is_chunked = std.mem.indexOf(u8, headers_raw, "Transfer-Encoding: chunked") != null or
        std.mem.indexOf(u8, headers_raw, "transfer-encoding: chunked") != null;

    const resp_body = if (is_chunked)
        try decodeChunked(alloc, raw_body)
    else
        try alloc.dupe(u8, raw_body);

    alloc.free(resp_data);

    return Response{
        .allocator = alloc,
        .status = status,
        .body = resp_body,
        .headers_raw = headers_raw,
    };
}

fn appendHeader(alloc: std.mem.Allocator, buf: *std.ArrayList(u8), name: []const u8, value: []const u8) !void {
    try buf.appendSlice(alloc, name);
    try buf.appendSlice(alloc, ": ");
    try buf.appendSlice(alloc, value);
    try buf.appendSlice(alloc, "\r\n");
}

fn decodeChunked(alloc: std.mem.Allocator, data: []const u8) ![]u8 {
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(alloc);

    var pos: usize = 0;
    while (pos < data.len) {
        // Find end of chunk size line
        const line_end = std.mem.indexOf(u8, data[pos..], "\r\n") orelse break;
        const size_str = std.mem.trim(u8, data[pos .. pos + line_end], " \t");
        if (size_str.len == 0) break;

        // Parse chunk size (hex)
        const chunk_size = std.fmt.parseInt(usize, size_str, 16) catch break;
        if (chunk_size == 0) break; // Final chunk

        pos += line_end + 2; // Skip past size line + \r\n
        if (pos + chunk_size > data.len) break;

        try result.appendSlice(alloc, data[pos .. pos + chunk_size]);
        pos += chunk_size + 2; // Skip chunk data + trailing \r\n
    }

    return result.toOwnedSlice(alloc);
}

// ---- Time formatting ----

fn formatAmzDate(buf: *[16]u8, epoch_secs: u64) []const u8 {
    const SECS_PER_DAY: u64 = 86400;
    const SECS_PER_HOUR: u64 = 3600;
    const SECS_PER_MIN: u64 = 60;

    var remaining = epoch_secs;
    var days = remaining / SECS_PER_DAY;
    remaining %= SECS_PER_DAY;
    const hour = remaining / SECS_PER_HOUR;
    remaining %= SECS_PER_HOUR;
    const minute = remaining / SECS_PER_MIN;
    const second = remaining % SECS_PER_MIN;

    var year: u16 = 1970;
    while (true) {
        const days_in_year: u64 = if (isLeapYear(year)) 366 else 365;
        if (days < days_in_year) break;
        days -= days_in_year;
        year += 1;
    }

    const days_in_months = [_]u8{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    var month: u8 = 1;
    while (month <= 12) : (month += 1) {
        var dm: u64 = days_in_months[month - 1];
        if (month == 2 and isLeapYear(year)) dm += 1;
        if (days < dm) break;
        days -= dm;
    }
    const day: u8 = @intCast(days + 1);

    _ = std.fmt.bufPrint(buf, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{
        year, month, day, hour, minute, second,
    }) catch unreachable;

    return buf;
}

fn isLeapYear(year: u16) bool {
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0);
}

// ---- Test Helpers ----

fn uniqueBucketName(alloc: std.mem.Allocator) ![]const u8 {
    const ts = std.time.milliTimestamp();
    return std.fmt.allocPrint(alloc, "zig-e2e-{d}", .{@as(u64, @intCast(ts))});
}

fn expectStatus(resp: *const Response, expected: u16) !void {
    if (resp.status != expected) {
        std.debug.print("FAIL: expected status {d}, got {d}\n", .{ expected, resp.status });
        if (resp.body.len > 0 and resp.body.len < 2000) {
            std.debug.print("  body: {s}\n", .{resp.body});
        }
        return error.TestUnexpectedResult;
    }
}

// ---- Test Runner ----

const TestResult = struct {
    name: []const u8,
    passed: bool,
    err_msg: ?[]const u8 = null,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    // First, verify server is reachable
    {
        const address = std.net.Address.resolveIp("127.0.0.1", PORT) catch {
            std.debug.print("ERROR: Cannot resolve address\n", .{});
            std.process.exit(1);
        };
        const stream = std.net.tcpConnectToAddress(address) catch {
            std.debug.print("ERROR: Cannot connect to server at localhost:{d}\n", .{PORT});
            std.debug.print("Please start the server first:\n  zig build run -- --config ../bleepstore.example.yaml --port 9013\n", .{});
            std.process.exit(1);
        };
        stream.close();
    }

    std.debug.print("\n=== BleepStore Zig E2E Integration Tests ===\n\n", .{});

    var results: std.ArrayList(TestResult) = .empty;
    defer results.deinit(alloc);

    // Run all test categories
    try runBucketTests(alloc, &results);
    try runObjectTests(alloc, &results);
    try runMultipartTests(alloc, &results);
    try runErrorTests(alloc, &results);
    try runAclTests(alloc, &results);

    // Summary
    var passed: usize = 0;
    var failed: usize = 0;
    std.debug.print("\n=== Results ===\n\n", .{});
    for (results.items) |r| {
        if (r.passed) {
            std.debug.print("  PASS: {s}\n", .{r.name});
            passed += 1;
        } else {
            std.debug.print("  FAIL: {s}", .{r.name});
            if (r.err_msg) |msg| {
                std.debug.print(" -- {s}", .{msg});
            }
            std.debug.print("\n", .{});
            failed += 1;
        }
    }

    std.debug.print("\n{d}/{d} passed, {d} failed\n\n", .{ passed, passed + failed, failed });

    if (failed > 0) {
        std.process.exit(1);
    }
}

fn runTest(alloc: std.mem.Allocator, results: *std.ArrayList(TestResult), name: []const u8, testFn: *const fn (std.mem.Allocator) anyerror!void) !void {
    std.debug.print("  running: {s}...", .{name});
    testFn(alloc) catch |err| {
        const err_msg = try std.fmt.allocPrint(alloc, "{}", .{err});
        try results.append(alloc, .{ .name = name, .passed = false, .err_msg = err_msg });
        std.debug.print(" FAIL ({s})\n", .{err_msg});
        return;
    };
    try results.append(alloc, .{ .name = name, .passed = true });
    std.debug.print(" ok\n", .{});
}

// ==========================================
// BUCKET TESTS
// ==========================================

fn runBucketTests(alloc: std.mem.Allocator, results: *std.ArrayList(TestResult)) !void {
    std.debug.print("--- Bucket Tests ---\n", .{});
    try runTest(alloc, results, "bucket: create and delete", testCreateAndDeleteBucket);
    try runTest(alloc, results, "bucket: create duplicate", testCreateDuplicateBucket);
    try runTest(alloc, results, "bucket: head existing", testHeadBucket);
    try runTest(alloc, results, "bucket: head nonexistent", testHeadNonexistentBucket);
    try runTest(alloc, results, "bucket: list buckets", testListBuckets);
    try runTest(alloc, results, "bucket: delete nonexistent", testDeleteNonexistentBucket);
    try runTest(alloc, results, "bucket: get location", testGetBucketLocation);
    try runTest(alloc, results, "bucket: invalid name", testInvalidBucketName);
}

fn testCreateAndDeleteBucket(alloc: std.mem.Allocator) !void {
    const bucket = try uniqueBucketName(alloc);
    defer alloc.free(bucket);
    const path = try std.fmt.allocPrint(alloc, "/{s}", .{bucket});
    defer alloc.free(path);

    var resp = try makeRequest(alloc, "PUT", path, null, null);
    defer resp.deinit();
    try expectStatus(&resp, 200);

    var resp2 = try makeRequest(alloc, "DELETE", path, null, null);
    defer resp2.deinit();
    try expectStatus(&resp2, 204);
}

fn testCreateDuplicateBucket(alloc: std.mem.Allocator) !void {
    const bucket = try uniqueBucketName(alloc);
    defer alloc.free(bucket);
    const path = try std.fmt.allocPrint(alloc, "/{s}", .{bucket});
    defer alloc.free(path);

    var resp1 = try makeRequest(alloc, "PUT", path, null, null);
    defer resp1.deinit();
    try expectStatus(&resp1, 200);

    var resp2 = try makeRequest(alloc, "PUT", path, null, null);
    defer resp2.deinit();
    if (resp2.status != 409 and resp2.status != 200) {
        return error.TestUnexpectedResult;
    }

    var resp3 = try makeRequest(alloc, "DELETE", path, null, null);
    defer resp3.deinit();
}

fn testHeadBucket(alloc: std.mem.Allocator) !void {
    const bucket = try uniqueBucketName(alloc);
    defer alloc.free(bucket);
    const path = try std.fmt.allocPrint(alloc, "/{s}", .{bucket});
    defer alloc.free(path);

    var resp1 = try makeRequest(alloc, "PUT", path, null, null);
    defer resp1.deinit();

    var resp2 = try makeRequest(alloc, "HEAD", path, null, null);
    defer resp2.deinit();
    try expectStatus(&resp2, 200);

    var resp3 = try makeRequest(alloc, "DELETE", path, null, null);
    defer resp3.deinit();
}

fn testHeadNonexistentBucket(alloc: std.mem.Allocator) !void {
    var resp = try makeRequest(alloc, "HEAD", "/nonexistent-bucket-zig-e2e-9999", null, null);
    defer resp.deinit();
    try expectStatus(&resp, 404);
}

fn testListBuckets(alloc: std.mem.Allocator) !void {
    const bucket = try uniqueBucketName(alloc);
    defer alloc.free(bucket);
    const path = try std.fmt.allocPrint(alloc, "/{s}", .{bucket});
    defer alloc.free(path);

    var resp1 = try makeRequest(alloc, "PUT", path, null, null);
    defer resp1.deinit();

    var resp2 = try makeRequest(alloc, "GET", "/", null, null);
    defer resp2.deinit();
    try expectStatus(&resp2, 200);

    if (!resp2.containsStr(bucket)) {
        std.debug.print("  bucket name '{s}' not found in ListBuckets response\n", .{bucket});
        return error.TestUnexpectedResult;
    }
    if (!resp2.containsStr("ListAllMyBucketsResult")) {
        return error.TestUnexpectedResult;
    }

    var resp3 = try makeRequest(alloc, "DELETE", path, null, null);
    defer resp3.deinit();
}

fn testDeleteNonexistentBucket(alloc: std.mem.Allocator) !void {
    var resp = try makeRequest(alloc, "DELETE", "/nonexistent-bucket-zig-e2e-del-9999", null, null);
    defer resp.deinit();
    try expectStatus(&resp, 404);
}

fn testGetBucketLocation(alloc: std.mem.Allocator) !void {
    const bucket = try uniqueBucketName(alloc);
    defer alloc.free(bucket);
    const path = try std.fmt.allocPrint(alloc, "/{s}", .{bucket});
    defer alloc.free(path);
    const loc_path = try std.fmt.allocPrint(alloc, "/{s}?location", .{bucket});
    defer alloc.free(loc_path);

    var resp1 = try makeRequest(alloc, "PUT", path, null, null);
    defer resp1.deinit();

    var resp2 = try makeRequest(alloc, "GET", loc_path, null, null);
    defer resp2.deinit();
    try expectStatus(&resp2, 200);
    if (!resp2.containsStr("LocationConstraint")) {
        return error.TestUnexpectedResult;
    }

    var resp3 = try makeRequest(alloc, "DELETE", path, null, null);
    defer resp3.deinit();
}

fn testInvalidBucketName(alloc: std.mem.Allocator) !void {
    var resp = try makeRequest(alloc, "PUT", "/AB", null, null);
    defer resp.deinit();
    try expectStatus(&resp, 400);
    if (!resp.containsStr("InvalidBucketName")) {
        return error.TestUnexpectedResult;
    }
}

// ==========================================
// OBJECT TESTS
// ==========================================

fn runObjectTests(alloc: std.mem.Allocator, results: *std.ArrayList(TestResult)) !void {
    std.debug.print("--- Object Tests ---\n", .{});
    try runTest(alloc, results, "object: put and get", testPutAndGetObject);
    try runTest(alloc, results, "object: head object", testHeadObject);
    try runTest(alloc, results, "object: delete object", testDeleteObject);
    try runTest(alloc, results, "object: delete nonexistent", testDeleteNonexistentObject);
    try runTest(alloc, results, "object: copy object", testCopyObject);
    try runTest(alloc, results, "object: list objects v2", testListObjectsV2);
    try runTest(alloc, results, "object: list objects v2 with prefix", testListObjectsV2WithPrefix);
    try runTest(alloc, results, "object: list objects v2 with delimiter", testListObjectsV2WithDelimiter);
    try runTest(alloc, results, "object: get nonexistent", testGetNonexistentObject);
    try runTest(alloc, results, "object: put large body", testPutLargeObject);
    try runTest(alloc, results, "object: get range", testGetObjectRange);
    try runTest(alloc, results, "object: conditional if-none-match", testConditionalIfNoneMatch);
    try runTest(alloc, results, "object: delete multiple objects", testDeleteMultipleObjects);
    try runTest(alloc, results, "object: unicode key", testUnicodeKey);
    try runTest(alloc, results, "object: list objects v1", testListObjectsV1);
}

/// Helper to create a bucket and return its path
fn createTestBucket(alloc: std.mem.Allocator) !struct { name: []const u8, path: []const u8 } {
    const bucket = try uniqueBucketName(alloc);
    const path = try std.fmt.allocPrint(alloc, "/{s}", .{bucket});
    var resp = try makeRequest(alloc, "PUT", path, null, null);
    resp.deinit();
    return .{ .name = bucket, .path = path };
}

fn cleanupBucket(alloc: std.mem.Allocator, path: []const u8) void {
    var resp = makeRequest(alloc, "DELETE", path, null, null) catch return;
    resp.deinit();
}

fn testPutAndGetObject(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const obj_path = try std.fmt.allocPrint(alloc, "/{s}/test-key.txt", .{b.name});
    defer alloc.free(obj_path);
    const body = "Hello, BleepStore!";

    var resp2 = try makeRequest(alloc, "PUT", obj_path, body, null);
    defer resp2.deinit();
    try expectStatus(&resp2, 200);

    // Check ETag
    if (resp2.getHeader("ETag") == null and resp2.getHeader("etag") == null) {
        std.debug.print("  Missing ETag header on PUT response\n", .{});
        return error.TestUnexpectedResult;
    }

    // Get object
    var resp3 = try makeRequest(alloc, "GET", obj_path, null, null);
    defer resp3.deinit();
    try expectStatus(&resp3, 200);
    if (!std.mem.eql(u8, resp3.body, body)) {
        std.debug.print("  body mismatch: got {d} bytes, expected {d} bytes\n", .{ resp3.body.len, body.len });
        return error.TestUnexpectedResult;
    }

    // Cleanup object
    var resp4 = try makeRequest(alloc, "DELETE", obj_path, null, null);
    defer resp4.deinit();
}

fn testHeadObject(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const obj_path = try std.fmt.allocPrint(alloc, "/{s}/head-test.txt", .{b.name});
    defer alloc.free(obj_path);

    var resp2 = try makeRequest(alloc, "PUT", obj_path, "head test data", null);
    defer resp2.deinit();

    var resp3 = try makeRequest(alloc, "HEAD", obj_path, null, null);
    defer resp3.deinit();
    try expectStatus(&resp3, 200);

    // Cleanup
    var resp4 = try makeRequest(alloc, "DELETE", obj_path, null, null);
    defer resp4.deinit();
}

fn testDeleteObject(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const obj_path = try std.fmt.allocPrint(alloc, "/{s}/del-test.txt", .{b.name});
    defer alloc.free(obj_path);

    var resp2 = try makeRequest(alloc, "PUT", obj_path, "delete me", null);
    defer resp2.deinit();

    var resp3 = try makeRequest(alloc, "DELETE", obj_path, null, null);
    defer resp3.deinit();
    try expectStatus(&resp3, 204);

    var resp4 = try makeRequest(alloc, "GET", obj_path, null, null);
    defer resp4.deinit();
    try expectStatus(&resp4, 404);
}

fn testDeleteNonexistentObject(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const obj_path = try std.fmt.allocPrint(alloc, "/{s}/nonexistent.txt", .{b.name});
    defer alloc.free(obj_path);

    var resp = try makeRequest(alloc, "DELETE", obj_path, null, null);
    defer resp.deinit();
    try expectStatus(&resp, 204);
}

fn testCopyObject(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const src_path = try std.fmt.allocPrint(alloc, "/{s}/source.txt", .{b.name});
    defer alloc.free(src_path);
    var resp2 = try makeRequest(alloc, "PUT", src_path, "copy source data", null);
    defer resp2.deinit();

    const dst_path = try std.fmt.allocPrint(alloc, "/{s}/dest.txt", .{b.name});
    defer alloc.free(dst_path);
    const copy_source = try std.fmt.allocPrint(alloc, "/{s}/source.txt", .{b.name});
    defer alloc.free(copy_source);

    const extra_hdrs = [_][2][]const u8{
        .{ "x-amz-copy-source", copy_source },
    };
    var resp3 = try makeRequest(alloc, "PUT", dst_path, null, &extra_hdrs);
    defer resp3.deinit();
    try expectStatus(&resp3, 200);
    if (!resp3.containsStr("CopyObjectResult")) {
        std.debug.print("  missing CopyObjectResult in response\n", .{});
        return error.TestUnexpectedResult;
    }

    // Verify copy content
    var resp4 = try makeRequest(alloc, "GET", dst_path, null, null);
    defer resp4.deinit();
    if (!std.mem.eql(u8, resp4.body, "copy source data")) {
        return error.TestUnexpectedResult;
    }

    // Cleanup
    var d1 = try makeRequest(alloc, "DELETE", src_path, null, null);
    d1.deinit();
    var d2 = try makeRequest(alloc, "DELETE", dst_path, null, null);
    d2.deinit();
}

fn testListObjectsV2(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const keys = [_][]const u8{ "a.txt", "b.txt", "c.txt" };
    for (keys) |key| {
        const p = try std.fmt.allocPrint(alloc, "/{s}/{s}", .{ b.name, key });
        defer alloc.free(p);
        var r = try makeRequest(alloc, "PUT", p, "data", null);
        r.deinit();
    }

    const list_path = try std.fmt.allocPrint(alloc, "/{s}?list-type=2", .{b.name});
    defer alloc.free(list_path);
    var resp2 = try makeRequest(alloc, "GET", list_path, null, null);
    defer resp2.deinit();
    try expectStatus(&resp2, 200);

    if (!resp2.containsStr("ListBucketResult")) return error.TestUnexpectedResult;
    if (!resp2.containsStr("a.txt") or !resp2.containsStr("b.txt") or !resp2.containsStr("c.txt"))
        return error.TestUnexpectedResult;

    // Cleanup
    for (keys) |key| {
        const p = try std.fmt.allocPrint(alloc, "/{s}/{s}", .{ b.name, key });
        defer alloc.free(p);
        var r = try makeRequest(alloc, "DELETE", p, null, null);
        r.deinit();
    }
}

fn testListObjectsV2WithPrefix(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const keys = [_][]const u8{ "photos/cat.jpg", "photos/dog.jpg", "docs/readme.md" };
    for (keys) |key| {
        const p = try std.fmt.allocPrint(alloc, "/{s}/{s}", .{ b.name, key });
        defer alloc.free(p);
        var r = try makeRequest(alloc, "PUT", p, "data", null);
        r.deinit();
    }

    const list_path = try std.fmt.allocPrint(alloc, "/{s}?list-type=2&prefix=photos%2F", .{b.name});
    defer alloc.free(list_path);
    var resp2 = try makeRequest(alloc, "GET", list_path, null, null);
    defer resp2.deinit();
    try expectStatus(&resp2, 200);

    if (!resp2.containsStr("photos/cat.jpg") or !resp2.containsStr("photos/dog.jpg"))
        return error.TestUnexpectedResult;
    if (resp2.containsStr("docs/readme.md")) return error.TestUnexpectedResult;

    for (keys) |key| {
        const p = try std.fmt.allocPrint(alloc, "/{s}/{s}", .{ b.name, key });
        defer alloc.free(p);
        var r = try makeRequest(alloc, "DELETE", p, null, null);
        r.deinit();
    }
}

fn testListObjectsV2WithDelimiter(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const keys = [_][]const u8{ "photos/cat.jpg", "photos/dog.jpg", "docs/readme.md", "root.txt" };
    for (keys) |key| {
        const p = try std.fmt.allocPrint(alloc, "/{s}/{s}", .{ b.name, key });
        defer alloc.free(p);
        var r = try makeRequest(alloc, "PUT", p, "data", null);
        r.deinit();
    }

    const list_path = try std.fmt.allocPrint(alloc, "/{s}?list-type=2&delimiter=%2F", .{b.name});
    defer alloc.free(list_path);
    var resp2 = try makeRequest(alloc, "GET", list_path, null, null);
    defer resp2.deinit();
    try expectStatus(&resp2, 200);

    if (!resp2.containsStr("CommonPrefixes")) return error.TestUnexpectedResult;
    if (!resp2.containsStr("root.txt")) return error.TestUnexpectedResult;

    for (keys) |key| {
        const p = try std.fmt.allocPrint(alloc, "/{s}/{s}", .{ b.name, key });
        defer alloc.free(p);
        var r = try makeRequest(alloc, "DELETE", p, null, null);
        r.deinit();
    }
}

fn testGetNonexistentObject(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const obj_path = try std.fmt.allocPrint(alloc, "/{s}/nonexistent.txt", .{b.name});
    defer alloc.free(obj_path);
    var resp = try makeRequest(alloc, "GET", obj_path, null, null);
    defer resp.deinit();
    try expectStatus(&resp, 404);
    if (!resp.containsStr("NoSuchKey")) return error.TestUnexpectedResult;
}

fn testPutLargeObject(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const large_body = try alloc.alloc(u8, 1024 * 1024);
    defer alloc.free(large_body);
    @memset(large_body, 'X');

    const obj_path = try std.fmt.allocPrint(alloc, "/{s}/large.bin", .{b.name});
    defer alloc.free(obj_path);

    var resp2 = try makeRequest(alloc, "PUT", obj_path, large_body, null);
    defer resp2.deinit();
    try expectStatus(&resp2, 200);

    var resp3 = try makeRequest(alloc, "GET", obj_path, null, null);
    defer resp3.deinit();
    try expectStatus(&resp3, 200);
    if (resp3.body.len != large_body.len) {
        std.debug.print("  size mismatch: got {d}, expected {d}\n", .{ resp3.body.len, large_body.len });
        return error.TestUnexpectedResult;
    }

    var d = try makeRequest(alloc, "DELETE", obj_path, null, null);
    d.deinit();
}

fn testGetObjectRange(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const obj_path = try std.fmt.allocPrint(alloc, "/{s}/range-test.txt", .{b.name});
    defer alloc.free(obj_path);

    var resp2 = try makeRequest(alloc, "PUT", obj_path, "0123456789", null);
    defer resp2.deinit();

    const range_hdrs = [_][2][]const u8{.{ "Range", "bytes=0-4" }};
    var resp3 = try makeRequest(alloc, "GET", obj_path, null, &range_hdrs);
    defer resp3.deinit();
    try expectStatus(&resp3, 206);
    if (!std.mem.eql(u8, resp3.body, "01234")) {
        std.debug.print("  range body: got '{s}', expected '01234'\n", .{resp3.body});
        return error.TestUnexpectedResult;
    }

    var d = try makeRequest(alloc, "DELETE", obj_path, null, null);
    d.deinit();
}

fn testConditionalIfNoneMatch(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const obj_path = try std.fmt.allocPrint(alloc, "/{s}/cond-test.txt", .{b.name});
    defer alloc.free(obj_path);

    var resp2 = try makeRequest(alloc, "PUT", obj_path, "conditional data", null);
    defer resp2.deinit();
    try expectStatus(&resp2, 200);

    const etag = resp2.getHeader("ETag") orelse resp2.getHeader("etag") orelse {
        std.debug.print("  missing ETag on PUT response\n", .{});
        return error.TestUnexpectedResult;
    };

    // Need to dupe because etag points into resp2's memory
    const etag_owned = try alloc.dupe(u8, etag);
    defer alloc.free(etag_owned);

    const inm_hdrs = [_][2][]const u8{.{ "If-None-Match", etag_owned }};
    var resp3 = try makeRequest(alloc, "GET", obj_path, null, &inm_hdrs);
    defer resp3.deinit();
    try expectStatus(&resp3, 304);

    var d = try makeRequest(alloc, "DELETE", obj_path, null, null);
    d.deinit();
}

fn testDeleteMultipleObjects(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const keys = [_][]const u8{ "del1.txt", "del2.txt", "del3.txt" };
    for (keys) |key| {
        const p = try std.fmt.allocPrint(alloc, "/{s}/{s}", .{ b.name, key });
        defer alloc.free(p);
        var r = try makeRequest(alloc, "PUT", p, "data", null);
        r.deinit();
    }

    const delete_xml =
        \\<?xml version="1.0" encoding="UTF-8"?>
        \\<Delete>
        \\  <Quiet>false</Quiet>
        \\  <Object><Key>del1.txt</Key></Object>
        \\  <Object><Key>del2.txt</Key></Object>
        \\  <Object><Key>del3.txt</Key></Object>
        \\</Delete>
    ;

    const delete_path = try std.fmt.allocPrint(alloc, "/{s}?delete", .{b.name});
    defer alloc.free(delete_path);
    var resp2 = try makeRequest(alloc, "POST", delete_path, delete_xml, null);
    defer resp2.deinit();
    try expectStatus(&resp2, 200);
    if (!resp2.containsStr("DeleteResult")) return error.TestUnexpectedResult;
}

fn testUnicodeKey(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const obj_path = try std.fmt.allocPrint(alloc, "/{s}/caf%C3%A9.txt", .{b.name});
    defer alloc.free(obj_path);

    var resp2 = try makeRequest(alloc, "PUT", obj_path, "unicode data", null);
    defer resp2.deinit();
    try expectStatus(&resp2, 200);

    var resp3 = try makeRequest(alloc, "GET", obj_path, null, null);
    defer resp3.deinit();
    try expectStatus(&resp3, 200);
    if (!std.mem.eql(u8, resp3.body, "unicode data")) return error.TestUnexpectedResult;

    var d = try makeRequest(alloc, "DELETE", obj_path, null, null);
    d.deinit();
}

fn testListObjectsV1(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const obj_path = try std.fmt.allocPrint(alloc, "/{s}/v1-test.txt", .{b.name});
    defer alloc.free(obj_path);
    var resp2 = try makeRequest(alloc, "PUT", obj_path, "v1 data", null);
    defer resp2.deinit();

    // GET bucket without list-type triggers v1 listing
    var resp3 = try makeRequest(alloc, "GET", b.path, null, null);
    defer resp3.deinit();
    try expectStatus(&resp3, 200);
    if (!resp3.containsStr("ListBucketResult")) return error.TestUnexpectedResult;
    if (!resp3.containsStr("v1-test.txt")) return error.TestUnexpectedResult;

    var d = try makeRequest(alloc, "DELETE", obj_path, null, null);
    d.deinit();
}

// ==========================================
// MULTIPART TESTS
// ==========================================

fn runMultipartTests(alloc: std.mem.Allocator, results: *std.ArrayList(TestResult)) !void {
    std.debug.print("--- Multipart Tests ---\n", .{});
    try runTest(alloc, results, "multipart: basic upload", testBasicMultipartUpload);
    try runTest(alloc, results, "multipart: abort upload", testAbortMultipartUpload);
    try runTest(alloc, results, "multipart: list uploads", testListMultipartUploads);
    try runTest(alloc, results, "multipart: list parts", testListParts);
}

fn testBasicMultipartUpload(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    // Initiate
    const init_path = try std.fmt.allocPrint(alloc, "/{s}/multipart-test.bin?uploads", .{b.name});
    defer alloc.free(init_path);
    var resp2 = try makeRequest(alloc, "POST", init_path, null, null);
    defer resp2.deinit();
    try expectStatus(&resp2, 200);
    if (!resp2.containsStr("InitiateMultipartUploadResult")) return error.TestUnexpectedResult;

    const upload_id = extractXmlValue(resp2.body, "UploadId") orelse return error.TestUnexpectedResult;

    // Upload part 1 (5MB)
    const part1 = try alloc.alloc(u8, 5 * 1024 * 1024);
    defer alloc.free(part1);
    @memset(part1, 'A');

    const part1_path = try std.fmt.allocPrint(alloc, "/{s}/multipart-test.bin?partNumber=1&uploadId={s}", .{ b.name, upload_id });
    defer alloc.free(part1_path);
    var resp3 = try makeRequest(alloc, "PUT", part1_path, part1, null);
    defer resp3.deinit();
    try expectStatus(&resp3, 200);
    const etag1 = resp3.getHeader("ETag") orelse resp3.getHeader("etag") orelse return error.TestUnexpectedResult;
    const etag1_owned = try alloc.dupe(u8, etag1);
    defer alloc.free(etag1_owned);

    // Upload part 2 (smaller, last part)
    const part2_path = try std.fmt.allocPrint(alloc, "/{s}/multipart-test.bin?partNumber=2&uploadId={s}", .{ b.name, upload_id });
    defer alloc.free(part2_path);
    var resp4 = try makeRequest(alloc, "PUT", part2_path, "last part data", null);
    defer resp4.deinit();
    try expectStatus(&resp4, 200);
    const etag2 = resp4.getHeader("ETag") orelse resp4.getHeader("etag") orelse return error.TestUnexpectedResult;
    const etag2_owned = try alloc.dupe(u8, etag2);
    defer alloc.free(etag2_owned);

    // Complete
    const complete_xml = try std.fmt.allocPrint(alloc,
        \\<?xml version="1.0" encoding="UTF-8"?>
        \\<CompleteMultipartUpload>
        \\  <Part><PartNumber>1</PartNumber><ETag>{s}</ETag></Part>
        \\  <Part><PartNumber>2</PartNumber><ETag>{s}</ETag></Part>
        \\</CompleteMultipartUpload>
    , .{ etag1_owned, etag2_owned });
    defer alloc.free(complete_xml);

    const complete_path = try std.fmt.allocPrint(alloc, "/{s}/multipart-test.bin?uploadId={s}", .{ b.name, upload_id });
    defer alloc.free(complete_path);
    var resp5 = try makeRequest(alloc, "POST", complete_path, complete_xml, null);
    defer resp5.deinit();
    try expectStatus(&resp5, 200);
    if (!resp5.containsStr("CompleteMultipartUploadResult")) {
        std.debug.print("  body: {s}\n", .{resp5.body[0..@min(resp5.body.len, 500)]});
        return error.TestUnexpectedResult;
    }

    // Cleanup object
    const del_path = try std.fmt.allocPrint(alloc, "/{s}/multipart-test.bin", .{b.name});
    defer alloc.free(del_path);
    var d = try makeRequest(alloc, "DELETE", del_path, null, null);
    d.deinit();
}

fn testAbortMultipartUpload(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const init_path = try std.fmt.allocPrint(alloc, "/{s}/abort-test.bin?uploads", .{b.name});
    defer alloc.free(init_path);
    var resp2 = try makeRequest(alloc, "POST", init_path, null, null);
    defer resp2.deinit();
    try expectStatus(&resp2, 200);
    const upload_id = extractXmlValue(resp2.body, "UploadId") orelse return error.TestUnexpectedResult;

    const abort_path = try std.fmt.allocPrint(alloc, "/{s}/abort-test.bin?uploadId={s}", .{ b.name, upload_id });
    defer alloc.free(abort_path);
    var resp3 = try makeRequest(alloc, "DELETE", abort_path, null, null);
    defer resp3.deinit();
    try expectStatus(&resp3, 204);
}

fn testListMultipartUploads(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    // Start an upload
    const init_path = try std.fmt.allocPrint(alloc, "/{s}/list-test.bin?uploads", .{b.name});
    defer alloc.free(init_path);
    var resp2 = try makeRequest(alloc, "POST", init_path, null, null);
    defer resp2.deinit();
    const upload_id = extractXmlValue(resp2.body, "UploadId") orelse return error.TestUnexpectedResult;

    // List uploads
    const list_path = try std.fmt.allocPrint(alloc, "/{s}?uploads", .{b.name});
    defer alloc.free(list_path);
    var resp3 = try makeRequest(alloc, "GET", list_path, null, null);
    defer resp3.deinit();
    try expectStatus(&resp3, 200);
    if (!resp3.containsStr("ListMultipartUploadsResult")) return error.TestUnexpectedResult;
    if (!resp3.containsStr("list-test.bin")) return error.TestUnexpectedResult;

    // Abort
    const abort_path = try std.fmt.allocPrint(alloc, "/{s}/list-test.bin?uploadId={s}", .{ b.name, upload_id });
    defer alloc.free(abort_path);
    var d = try makeRequest(alloc, "DELETE", abort_path, null, null);
    d.deinit();
}

fn testListParts(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const init_path = try std.fmt.allocPrint(alloc, "/{s}/parts-test.bin?uploads", .{b.name});
    defer alloc.free(init_path);
    var resp2 = try makeRequest(alloc, "POST", init_path, null, null);
    defer resp2.deinit();
    const upload_id = extractXmlValue(resp2.body, "UploadId") orelse return error.TestUnexpectedResult;

    // Upload a part
    const part = try alloc.alloc(u8, 5 * 1024 * 1024);
    defer alloc.free(part);
    @memset(part, 'P');

    const part_path = try std.fmt.allocPrint(alloc, "/{s}/parts-test.bin?partNumber=1&uploadId={s}", .{ b.name, upload_id });
    defer alloc.free(part_path);
    var resp3 = try makeRequest(alloc, "PUT", part_path, part, null);
    defer resp3.deinit();

    // List parts
    const list_path = try std.fmt.allocPrint(alloc, "/{s}/parts-test.bin?uploadId={s}", .{ b.name, upload_id });
    defer alloc.free(list_path);
    var resp4 = try makeRequest(alloc, "GET", list_path, null, null);
    defer resp4.deinit();
    try expectStatus(&resp4, 200);
    if (!resp4.containsStr("ListPartsResult")) return error.TestUnexpectedResult;

    // Abort
    const abort_path = try std.fmt.allocPrint(alloc, "/{s}/parts-test.bin?uploadId={s}", .{ b.name, upload_id });
    defer alloc.free(abort_path);
    var d = try makeRequest(alloc, "DELETE", abort_path, null, null);
    d.deinit();
}

// ==========================================
// ERROR TESTS
// ==========================================

fn runErrorTests(alloc: std.mem.Allocator, results: *std.ArrayList(TestResult)) !void {
    std.debug.print("--- Error Tests ---\n", .{});
    try runTest(alloc, results, "error: NoSuchBucket", testNoSuchBucket);
    try runTest(alloc, results, "error: NoSuchKey", testNoSuchKey);
    try runTest(alloc, results, "error: BucketNotEmpty", testBucketNotEmpty);
    try runTest(alloc, results, "error: request ID in response", testRequestIdInError);
    try runTest(alloc, results, "error: key too long", testKeyTooLong);
}

fn testNoSuchBucket(alloc: std.mem.Allocator) !void {
    var resp = try makeRequest(alloc, "GET", "/nonexistent-bucket-zig-err-9999/key.txt", null, null);
    defer resp.deinit();
    try expectStatus(&resp, 404);
    if (!resp.containsStr("NoSuchBucket")) return error.TestUnexpectedResult;
}

fn testNoSuchKey(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const p = try std.fmt.allocPrint(alloc, "/{s}/nonexistent.txt", .{b.name});
    defer alloc.free(p);
    var resp = try makeRequest(alloc, "GET", p, null, null);
    defer resp.deinit();
    try expectStatus(&resp, 404);
    if (!resp.containsStr("NoSuchKey")) return error.TestUnexpectedResult;
}

fn testBucketNotEmpty(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);

    const p = try std.fmt.allocPrint(alloc, "/{s}/blocker.txt", .{b.name});
    defer alloc.free(p);
    var r = try makeRequest(alloc, "PUT", p, "blocks delete", null);
    r.deinit();

    var resp = try makeRequest(alloc, "DELETE", b.path, null, null);
    defer resp.deinit();
    try expectStatus(&resp, 409);
    if (!resp.containsStr("BucketNotEmpty")) return error.TestUnexpectedResult;

    // Real cleanup
    var d1 = try makeRequest(alloc, "DELETE", p, null, null);
    d1.deinit();
    var d2 = try makeRequest(alloc, "DELETE", b.path, null, null);
    d2.deinit();
}

fn testRequestIdInError(alloc: std.mem.Allocator) !void {
    var resp = try makeRequest(alloc, "GET", "/nonexistent-reqid-9999/key.txt", null, null);
    defer resp.deinit();
    try expectStatus(&resp, 404);
    if (!resp.containsStr("RequestId")) return error.TestUnexpectedResult;
}

fn testKeyTooLong(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const long_key = try alloc.alloc(u8, 1100);
    defer alloc.free(long_key);
    @memset(long_key, 'k');

    const p = try std.fmt.allocPrint(alloc, "/{s}/{s}", .{ b.name, long_key });
    defer alloc.free(p);
    var resp = try makeRequest(alloc, "PUT", p, "data", null);
    defer resp.deinit();
    try expectStatus(&resp, 400);
}

// ==========================================
// ACL TESTS
// ==========================================

fn runAclTests(alloc: std.mem.Allocator, results: *std.ArrayList(TestResult)) !void {
    std.debug.print("--- ACL Tests ---\n", .{});
    try runTest(alloc, results, "acl: get bucket ACL", testGetBucketAcl);
    try runTest(alloc, results, "acl: get object ACL", testGetObjectAcl);
}

fn testGetBucketAcl(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const acl_path = try std.fmt.allocPrint(alloc, "/{s}?acl", .{b.name});
    defer alloc.free(acl_path);
    var resp = try makeRequest(alloc, "GET", acl_path, null, null);
    defer resp.deinit();
    try expectStatus(&resp, 200);
    if (!resp.containsStr("AccessControlPolicy")) return error.TestUnexpectedResult;
    if (!resp.containsStr("FULL_CONTROL")) return error.TestUnexpectedResult;
}

fn testGetObjectAcl(alloc: std.mem.Allocator) !void {
    const b = try createTestBucket(alloc);
    defer alloc.free(b.name);
    defer alloc.free(b.path);
    defer cleanupBucket(alloc, b.path);

    const p = try std.fmt.allocPrint(alloc, "/{s}/acl-test.txt", .{b.name});
    defer alloc.free(p);
    var r = try makeRequest(alloc, "PUT", p, "acl test", null);
    r.deinit();

    const acl_path = try std.fmt.allocPrint(alloc, "/{s}/acl-test.txt?acl", .{b.name});
    defer alloc.free(acl_path);
    var resp = try makeRequest(alloc, "GET", acl_path, null, null);
    defer resp.deinit();
    try expectStatus(&resp, 200);
    if (!resp.containsStr("AccessControlPolicy")) return error.TestUnexpectedResult;

    var d = try makeRequest(alloc, "DELETE", p, null, null);
    d.deinit();
}

// ---- XML Helper ----

fn extractXmlValue(xml_data: []const u8, tag: []const u8) ?[]const u8 {
    var open_buf: [128]u8 = undefined;
    var close_buf: [128]u8 = undefined;
    const open_tag = std.fmt.bufPrint(&open_buf, "<{s}>", .{tag}) catch return null;
    const close_tag = std.fmt.bufPrint(&close_buf, "</{s}>", .{tag}) catch return null;

    const start = (std.mem.indexOf(u8, xml_data, open_tag) orelse return null) + open_tag.len;
    const end = std.mem.indexOf(u8, xml_data[start..], close_tag) orelse return null;
    return xml_data[start .. start + end];
}
