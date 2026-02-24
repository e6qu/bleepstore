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

/// Sum of request durations in microseconds (for computing average latency).
pub var http_request_duration_us_sum: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

/// Server start time (epoch seconds).
var start_time_secs: u64 = 0;

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

pub fn addRequestDuration(us: u64) void {
    _ = http_request_duration_us_sum.fetchAdd(us, .monotonic);
}

pub fn setBucketsTotal(n: u64) void {
    buckets_total.store(n, .monotonic);
}

pub fn setObjectsTotal(n: u64) void {
    objects_total.store(n, .monotonic);
}

// ---------------------------------------------------------------------------
// Prometheus exposition format rendering
// ---------------------------------------------------------------------------

/// Render all metrics in Prometheus exposition format (`text/plain; version=0.0.4`).
/// Caller owns the returned slice and must free it with the provided allocator.
pub fn renderMetrics(allocator: std.mem.Allocator) ![]u8 {
    const ts = std.time.timestamp();
    const now_secs: u64 = @intCast(if (ts < 0) 0 else ts);
    const uptime_secs: u64 = if (now_secs >= start_time_secs) now_secs - start_time_secs else 0;

    // Convert duration sum from microseconds to seconds (as float string).
    const duration_us = http_request_duration_us_sum.load(.monotonic);
    const duration_secs_whole = duration_us / 1_000_000;
    const duration_secs_frac = (duration_us % 1_000_000) / 1_000; // milliseconds part

    return std.fmt.allocPrint(allocator,
        \\# HELP bleepstore_http_requests_total Total HTTP requests received.
        \\# TYPE bleepstore_http_requests_total counter
        \\bleepstore_http_requests_total {d}
        \\
        \\# HELP bleepstore_s3_operations_total Total S3 operations processed.
        \\# TYPE bleepstore_s3_operations_total counter
        \\bleepstore_s3_operations_total {d}
        \\
        \\# HELP bleepstore_objects_total Current number of objects across all buckets.
        \\# TYPE bleepstore_objects_total gauge
        \\bleepstore_objects_total {d}
        \\
        \\# HELP bleepstore_buckets_total Current number of buckets.
        \\# TYPE bleepstore_buckets_total gauge
        \\bleepstore_buckets_total {d}
        \\
        \\# HELP bleepstore_bytes_received_total Total bytes received in request bodies.
        \\# TYPE bleepstore_bytes_received_total counter
        \\bleepstore_bytes_received_total {d}
        \\
        \\# HELP bleepstore_bytes_sent_total Total bytes sent in response bodies.
        \\# TYPE bleepstore_bytes_sent_total counter
        \\bleepstore_bytes_sent_total {d}
        \\
        \\# HELP bleepstore_http_request_duration_seconds_sum Sum of HTTP request durations in seconds.
        \\# TYPE bleepstore_http_request_duration_seconds_sum counter
        \\bleepstore_http_request_duration_seconds_sum {d}.{d:0>3}
        \\
        \\# HELP process_uptime_seconds Time since server start in seconds.
        \\# TYPE process_uptime_seconds gauge
        \\process_uptime_seconds {d}
        \\
    , .{
        http_requests_total.load(.monotonic),
        s3_operations_total.load(.monotonic),
        objects_total.load(.monotonic),
        buckets_total.load(.monotonic),
        bytes_received_total.load(.monotonic),
        bytes_sent_total.load(.monotonic),
        duration_secs_whole,
        duration_secs_frac,
        uptime_secs,
    });
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

test "renderMetrics produces Prometheus format" {
    // Reset all counters for deterministic output.
    http_requests_total.store(42, .monotonic);
    s3_operations_total.store(10, .monotonic);
    objects_total.store(5, .monotonic);
    buckets_total.store(2, .monotonic);
    bytes_received_total.store(1024, .monotonic);
    bytes_sent_total.store(2048, .monotonic);
    http_request_duration_us_sum.store(1_500_000, .monotonic); // 1.5 seconds
    initMetrics();

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
}
