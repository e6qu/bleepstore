const std = @import("std");

/// Hand-rolled Prometheus metrics using atomic counters for thread-safe increments.
/// All metrics use the `bleepstore_` prefix for namespace isolation.

// ---------------------------------------------------------------------------
// Atomic counters (thread-safe)
// ---------------------------------------------------------------------------

/// Total HTTP requests received.
pub var http_requests_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

/// Total S3 operations processed (success + error).
pub var s3_operations_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

/// Current number of objects across all buckets (gauge).
pub var objects_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

/// Current number of buckets (gauge).
pub var buckets_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

/// Total bytes received in request bodies.
pub var bytes_received_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

/// Total bytes sent in response bodies.
pub var bytes_sent_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

/// Server start time (epoch seconds).
var start_time_secs: u64 = 0;

// ---------------------------------------------------------------------------
// Duration histogram
// ---------------------------------------------------------------------------

/// Prometheus default duration bucket boundaries in seconds.
/// 11 boundaries => 12 buckets (11 finite + 1 +Inf).
pub const duration_boundaries = [_]f64{ 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0 };
const duration_bucket_count = duration_boundaries.len + 1; // includes +Inf

/// Atomic bucket counters for the duration histogram.
pub var duration_buckets: [duration_bucket_count]std.atomic.Value(u64) = initBuckets(duration_bucket_count);
/// Total observation count for the duration histogram.
pub var duration_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);
/// Sum of observed durations in microseconds (converted to seconds on render).
pub var duration_sum_us: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

// ---------------------------------------------------------------------------
// Request size histogram
// ---------------------------------------------------------------------------

/// Size bucket boundaries in bytes.
pub const size_boundaries = [_]u64{ 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864 };
const size_bucket_count = size_boundaries.len + 1; // includes +Inf

/// Atomic bucket counters for the request size histogram.
pub var request_size_buckets: [size_bucket_count]std.atomic.Value(u64) = initBuckets(size_bucket_count);
/// Total observation count for the request size histogram.
pub var request_size_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);
/// Sum of observed request sizes in bytes.
pub var request_size_sum: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

// ---------------------------------------------------------------------------
// Response size histogram
// ---------------------------------------------------------------------------

/// Atomic bucket counters for the response size histogram.
pub var response_size_buckets: [size_bucket_count]std.atomic.Value(u64) = initBuckets(size_bucket_count);
/// Total observation count for the response size histogram.
pub var response_size_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);
/// Sum of observed response sizes in bytes.
pub var response_size_sum: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

/// Initialize an array of atomic u64 values to zero (comptime).
fn initBuckets(comptime n: usize) [n]std.atomic.Value(u64) {
    var arr: [n]std.atomic.Value(u64) = undefined;
    for (&arr) |*slot| {
        slot.* = std.atomic.Value(u64).init(0);
    }
    return arr;
}

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

/// Record the server start time. Call this once at startup.
pub fn initMetrics() void {
    const ts = std.time.timestamp();
    start_time_secs = @intCast(if (ts < 0) 0 else ts);
}

// ---------------------------------------------------------------------------
// Counter helpers
// ---------------------------------------------------------------------------

pub fn incrementHttpRequests() void {
    _ = http_requests_total.fetchAdd(1, .monotonic);
}

pub fn incrementS3Operations() void {
    _ = s3_operations_total.fetchAdd(1, .monotonic);
}

pub fn addBytesReceived(n: u64) void {
    _ = bytes_received_total.fetchAdd(n, .monotonic);
}

pub fn addBytesSent(n: u64) void {
    _ = bytes_sent_total.fetchAdd(n, .monotonic);
}

pub fn setBucketsTotal(n: u64) void {
    buckets_total.store(n, .monotonic);
}

pub fn setObjectsTotal(n: u64) void {
    objects_total.store(n, .monotonic);
}

// ---------------------------------------------------------------------------
// Histogram observation helpers
// ---------------------------------------------------------------------------

/// Observe a request duration in microseconds.
/// Increments the appropriate bucket, count, and sum.
pub fn observeDuration(us: u64) void {
    // Convert microseconds to seconds for bucket matching.
    const secs: f64 = @as(f64, @floatFromInt(us)) / 1_000_000.0;

    // Find the first bucket boundary >= secs and increment all buckets from there.
    // In Prometheus histograms, each bucket is cumulative (le="X" counts all obs <= X).
    for (duration_boundaries, 0..) |boundary, i| {
        if (secs <= boundary) {
            _ = duration_buckets[i].fetchAdd(1, .monotonic);
            // Also increment all higher buckets (cumulative).
            var j = i + 1;
            while (j < duration_bucket_count) : (j += 1) {
                _ = duration_buckets[j].fetchAdd(1, .monotonic);
            }
            _ = duration_count.fetchAdd(1, .monotonic);
            _ = duration_sum_us.fetchAdd(us, .monotonic);
            return;
        }
    }
    // Greater than all boundaries => only +Inf bucket.
    _ = duration_buckets[duration_bucket_count - 1].fetchAdd(1, .monotonic);
    _ = duration_count.fetchAdd(1, .monotonic);
    _ = duration_sum_us.fetchAdd(us, .monotonic);
}

/// Observe a request body size in bytes.
pub fn observeRequestSize(bytes: u64) void {
    observeSizeHistogram(bytes, &request_size_buckets, &request_size_count, &request_size_sum);
}

/// Observe a response body size in bytes.
pub fn observeResponseSize(bytes: u64) void {
    observeSizeHistogram(bytes, &response_size_buckets, &response_size_count, &response_size_sum);
}

fn observeSizeHistogram(
    bytes: u64,
    buckets: *[size_bucket_count]std.atomic.Value(u64),
    count: *std.atomic.Value(u64),
    sum: *std.atomic.Value(u64),
) void {
    for (size_boundaries, 0..) |boundary, i| {
        if (bytes <= boundary) {
            // Increment this and all higher buckets (cumulative).
            var j = i;
            while (j < size_bucket_count) : (j += 1) {
                _ = buckets[j].fetchAdd(1, .monotonic);
            }
            _ = count.fetchAdd(1, .monotonic);
            _ = sum.fetchAdd(bytes, .monotonic);
            return;
        }
    }
    // Greater than all boundaries => only +Inf bucket.
    _ = buckets[size_bucket_count - 1].fetchAdd(1, .monotonic);
    _ = count.fetchAdd(1, .monotonic);
    _ = sum.fetchAdd(bytes, .monotonic);
}

// ---------------------------------------------------------------------------
// Prometheus exposition format rendering
// ---------------------------------------------------------------------------

/// Format a u64 microsecond value as seconds with 6 decimal places into a buffer.
fn formatUsMicrosAsSeconds(buf: []u8, us: u64) []const u8 {
    const whole = us / 1_000_000;
    const frac = us % 1_000_000;
    return std.fmt.bufPrint(buf, "{d}.{d:0>6}", .{ whole, frac }) catch "0.000000";
}

/// Render all metrics in Prometheus exposition format (`text/plain; version=0.0.4`).
/// Caller owns the returned slice and must free it with the provided allocator.
pub fn renderMetrics(allocator: std.mem.Allocator) ![]u8 {
    const ts = std.time.timestamp();
    const now_secs: u64 = @intCast(if (ts < 0) 0 else ts);
    const uptime_secs: u64 = if (now_secs >= start_time_secs) now_secs - start_time_secs else 0;

    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(allocator);

    const writer = buf.writer(allocator);

    // --- Simple counters and gauges ---
    try writer.writeAll("# HELP bleepstore_http_requests_total Total HTTP requests received.\n");
    try writer.writeAll("# TYPE bleepstore_http_requests_total counter\n");
    try std.fmt.format(writer, "bleepstore_http_requests_total {d}\n", .{http_requests_total.load(.monotonic)});

    try writer.writeAll("\n# HELP bleepstore_s3_operations_total Total S3 operations processed.\n");
    try writer.writeAll("# TYPE bleepstore_s3_operations_total counter\n");
    try std.fmt.format(writer, "bleepstore_s3_operations_total {d}\n", .{s3_operations_total.load(.monotonic)});

    try writer.writeAll("\n# HELP bleepstore_objects_total Current number of objects across all buckets.\n");
    try writer.writeAll("# TYPE bleepstore_objects_total gauge\n");
    try std.fmt.format(writer, "bleepstore_objects_total {d}\n", .{objects_total.load(.monotonic)});

    try writer.writeAll("\n# HELP bleepstore_buckets_total Current number of buckets.\n");
    try writer.writeAll("# TYPE bleepstore_buckets_total gauge\n");
    try std.fmt.format(writer, "bleepstore_buckets_total {d}\n", .{buckets_total.load(.monotonic)});

    try writer.writeAll("\n# HELP bleepstore_bytes_received_total Total bytes received in request bodies.\n");
    try writer.writeAll("# TYPE bleepstore_bytes_received_total counter\n");
    try std.fmt.format(writer, "bleepstore_bytes_received_total {d}\n", .{bytes_received_total.load(.monotonic)});

    try writer.writeAll("\n# HELP bleepstore_bytes_sent_total Total bytes sent in response bodies.\n");
    try writer.writeAll("# TYPE bleepstore_bytes_sent_total counter\n");
    try std.fmt.format(writer, "bleepstore_bytes_sent_total {d}\n", .{bytes_sent_total.load(.monotonic)});

    // --- Duration histogram ---
    try writer.writeAll("\n# HELP bleepstore_http_request_duration_seconds Request latency in seconds.\n");
    try writer.writeAll("# TYPE bleepstore_http_request_duration_seconds histogram\n");

    for (duration_boundaries, 0..) |boundary, i| {
        // Format boundary as a float string.
        var boundary_buf: [32]u8 = undefined;
        const boundary_str = std.fmt.bufPrint(&boundary_buf, "{d}", .{boundary}) catch "0";
        try std.fmt.format(writer, "bleepstore_http_request_duration_seconds_bucket{{le=\"{s}\"}} {d}\n", .{
            boundary_str,
            duration_buckets[i].load(.monotonic),
        });
    }
    try std.fmt.format(writer, "bleepstore_http_request_duration_seconds_bucket{{le=\"+Inf\"}} {d}\n", .{
        duration_buckets[duration_bucket_count - 1].load(.monotonic),
    });
    {
        var sec_buf: [32]u8 = undefined;
        const sec_str = formatUsMicrosAsSeconds(&sec_buf, duration_sum_us.load(.monotonic));
        try std.fmt.format(writer, "bleepstore_http_request_duration_seconds_sum {s}\n", .{sec_str});
    }
    try std.fmt.format(writer, "bleepstore_http_request_duration_seconds_count {d}\n", .{duration_count.load(.monotonic)});

    // --- Request size histogram ---
    try writer.writeAll("\n# HELP bleepstore_http_request_size_bytes Request body size in bytes.\n");
    try writer.writeAll("# TYPE bleepstore_http_request_size_bytes histogram\n");

    for (size_boundaries, 0..) |boundary, i| {
        try std.fmt.format(writer, "bleepstore_http_request_size_bytes_bucket{{le=\"{d}\"}} {d}\n", .{
            boundary,
            request_size_buckets[i].load(.monotonic),
        });
    }
    try std.fmt.format(writer, "bleepstore_http_request_size_bytes_bucket{{le=\"+Inf\"}} {d}\n", .{
        request_size_buckets[size_bucket_count - 1].load(.monotonic),
    });
    try std.fmt.format(writer, "bleepstore_http_request_size_bytes_sum {d}\n", .{request_size_sum.load(.monotonic)});
    try std.fmt.format(writer, "bleepstore_http_request_size_bytes_count {d}\n", .{request_size_count.load(.monotonic)});

    // --- Response size histogram ---
    try writer.writeAll("\n# HELP bleepstore_http_response_size_bytes Response body size in bytes.\n");
    try writer.writeAll("# TYPE bleepstore_http_response_size_bytes histogram\n");

    for (size_boundaries, 0..) |boundary, i| {
        try std.fmt.format(writer, "bleepstore_http_response_size_bytes_bucket{{le=\"{d}\"}} {d}\n", .{
            boundary,
            response_size_buckets[i].load(.monotonic),
        });
    }
    try std.fmt.format(writer, "bleepstore_http_response_size_bytes_bucket{{le=\"+Inf\"}} {d}\n", .{
        response_size_buckets[size_bucket_count - 1].load(.monotonic),
    });
    try std.fmt.format(writer, "bleepstore_http_response_size_bytes_sum {d}\n", .{response_size_sum.load(.monotonic)});
    try std.fmt.format(writer, "bleepstore_http_response_size_bytes_count {d}\n", .{response_size_count.load(.monotonic)});

    // --- Uptime gauge ---
    try writer.writeAll("\n# HELP process_uptime_seconds Time since server start in seconds.\n");
    try writer.writeAll("# TYPE process_uptime_seconds gauge\n");
    try std.fmt.format(writer, "process_uptime_seconds {d}\n", .{uptime_secs});

    return buf.toOwnedSlice(allocator);
}

// ---------------------------------------------------------------------------
// Reset helpers (for tests)
// ---------------------------------------------------------------------------

fn resetHistogram(
    comptime n: usize,
    buckets: *[n]std.atomic.Value(u64),
    count: *std.atomic.Value(u64),
    sum: *std.atomic.Value(u64),
) void {
    for (buckets) |*b| b.store(0, .monotonic);
    count.store(0, .monotonic);
    sum.store(0, .monotonic);
}

fn resetAll() void {
    http_requests_total.store(0, .monotonic);
    s3_operations_total.store(0, .monotonic);
    objects_total.store(0, .monotonic);
    buckets_total.store(0, .monotonic);
    bytes_received_total.store(0, .monotonic);
    bytes_sent_total.store(0, .monotonic);
    resetHistogram(duration_bucket_count, &duration_buckets, &duration_count, &duration_sum_us);
    resetHistogram(size_bucket_count, &request_size_buckets, &request_size_count, &request_size_sum);
    resetHistogram(size_bucket_count, &response_size_buckets, &response_size_count, &response_size_sum);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "initMetrics sets start time" {
    initMetrics();
    try std.testing.expect(start_time_secs > 0);
}

test "incrementHttpRequests" {
    // Reset for test isolation
    http_requests_total.store(0, .monotonic);
    incrementHttpRequests();
    incrementHttpRequests();
    try std.testing.expectEqual(@as(u64, 2), http_requests_total.load(.monotonic));
}

test "addBytesReceived" {
    bytes_received_total.store(0, .monotonic);
    addBytesReceived(100);
    addBytesReceived(200);
    try std.testing.expectEqual(@as(u64, 300), bytes_received_total.load(.monotonic));
}

test "observeDuration bucket placement" {
    resetAll();

    // 500us = 0.0005s => should go into the 0.005 bucket (index 0) and all higher
    observeDuration(500);
    try std.testing.expectEqual(@as(u64, 1), duration_buckets[0].load(.monotonic)); // le=0.005
    try std.testing.expectEqual(@as(u64, 1), duration_buckets[duration_bucket_count - 1].load(.monotonic)); // +Inf

    // 50ms = 50000us = 0.05s => should go into the 0.05 bucket (index 3) and higher
    observeDuration(50_000);
    try std.testing.expectEqual(@as(u64, 1), duration_buckets[0].load(.monotonic)); // le=0.005 unchanged
    try std.testing.expectEqual(@as(u64, 1), duration_buckets[2].load(.monotonic)); // le=0.025 unchanged
    try std.testing.expectEqual(@as(u64, 2), duration_buckets[3].load(.monotonic)); // le=0.05 (both obs)
    try std.testing.expectEqual(@as(u64, 2), duration_buckets[duration_bucket_count - 1].load(.monotonic)); // +Inf

    // 15s => > 10, only +Inf
    observeDuration(15_000_000);
    try std.testing.expectEqual(@as(u64, 2), duration_buckets[10].load(.monotonic)); // le=10 unchanged
    try std.testing.expectEqual(@as(u64, 3), duration_buckets[duration_bucket_count - 1].load(.monotonic)); // +Inf

    // Verify count and sum
    try std.testing.expectEqual(@as(u64, 3), duration_count.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 500 + 50_000 + 15_000_000), duration_sum_us.load(.monotonic));
}

test "observeRequestSize bucket placement" {
    resetAll();

    // 100 bytes => le=256 (index 0) and all higher
    observeRequestSize(100);
    try std.testing.expectEqual(@as(u64, 1), request_size_buckets[0].load(.monotonic)); // le=256
    try std.testing.expectEqual(@as(u64, 1), request_size_buckets[size_bucket_count - 1].load(.monotonic)); // +Inf

    // 5000 bytes => le=16384 (index 3) and higher
    observeRequestSize(5000);
    try std.testing.expectEqual(@as(u64, 1), request_size_buckets[0].load(.monotonic)); // le=256 unchanged
    try std.testing.expectEqual(@as(u64, 1), request_size_buckets[2].load(.monotonic)); // le=4096 unchanged
    try std.testing.expectEqual(@as(u64, 2), request_size_buckets[3].load(.monotonic)); // le=16384 (both)
    try std.testing.expectEqual(@as(u64, 2), request_size_buckets[size_bucket_count - 1].load(.monotonic)); // +Inf

    try std.testing.expectEqual(@as(u64, 2), request_size_count.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 5100), request_size_sum.load(.monotonic));
}

test "observeResponseSize bucket placement" {
    resetAll();

    observeResponseSize(512);
    try std.testing.expectEqual(@as(u64, 0), response_size_buckets[0].load(.monotonic)); // le=256 not hit
    try std.testing.expectEqual(@as(u64, 1), response_size_buckets[1].load(.monotonic)); // le=1024
    try std.testing.expectEqual(@as(u64, 1), response_size_buckets[size_bucket_count - 1].load(.monotonic)); // +Inf

    try std.testing.expectEqual(@as(u64, 1), response_size_count.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 512), response_size_sum.load(.monotonic));
}

test "renderMetrics produces histogram _bucket/_count/_sum format" {
    resetAll();
    initMetrics();

    // Set up some counters.
    http_requests_total.store(42, .monotonic);
    s3_operations_total.store(10, .monotonic);
    objects_total.store(5, .monotonic);
    buckets_total.store(2, .monotonic);
    bytes_received_total.store(1024, .monotonic);
    bytes_sent_total.store(2048, .monotonic);

    // Observe a duration of 1.5 seconds (1_500_000 us).
    observeDuration(1_500_000);

    // Observe request and response sizes.
    observeRequestSize(500);
    observeResponseSize(2000);

    const output = try renderMetrics(std.testing.allocator);
    defer std.testing.allocator.free(output);

    // Verify key metric lines are present.
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_requests_total 42") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_s3_operations_total 10") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_objects_total 5") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_buckets_total 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_bytes_received_total 1024") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_bytes_sent_total 2048") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "process_uptime_seconds") != null);

    // HELP and TYPE lines
    try std.testing.expect(std.mem.indexOf(u8, output, "# HELP bleepstore_http_requests_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE bleepstore_http_requests_total counter") != null);

    // Duration histogram format
    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE bleepstore_http_request_duration_seconds histogram") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_request_duration_seconds_bucket{le=\"0.005\"} 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_request_duration_seconds_bucket{le=\"2.5\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_request_duration_seconds_bucket{le=\"+Inf\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_request_duration_seconds_sum 1.500000") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_request_duration_seconds_count 1") != null);

    // Request size histogram format
    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE bleepstore_http_request_size_bytes histogram") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_request_size_bytes_bucket{le=\"256\"} 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_request_size_bytes_bucket{le=\"1024\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_request_size_bytes_bucket{le=\"+Inf\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_request_size_bytes_sum 500") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_request_size_bytes_count 1") != null);

    // Response size histogram format
    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE bleepstore_http_response_size_bytes histogram") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_response_size_bytes_bucket{le=\"4096\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_response_size_bytes_bucket{le=\"+Inf\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_response_size_bytes_sum 2000") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bleepstore_http_response_size_bytes_count 1") != null);
}
