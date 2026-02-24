const std = @import("std");
const S3Error = @import("errors.zig").S3Error;

/// Validate an S3 bucket name per AWS naming rules.
/// Returns null if valid, or the appropriate S3Error if invalid.
///
/// Rules:
///  - Length 3..63
///  - Only lowercase letters, digits, hyphens, and dots
///  - Must start and end with a letter or digit
///  - Must not be formatted as an IP address (e.g. 192.168.0.1)
///  - Must not start with "xn--" (internationalized domain name prefix)
///  - Must not end with "-s3alias" or "--ol-s3"
///  - No consecutive dots (..) or adjacent dot-hyphen (.- or -.)
pub fn isValidBucketName(name: []const u8) ?S3Error {
    if (name.len < 3 or name.len > 63) return .InvalidBucketName;

    // Must start with lowercase letter or digit.
    if (!isLowerAlphaNum(name[0])) return .InvalidBucketName;

    // Must end with lowercase letter or digit.
    if (!isLowerAlphaNum(name[name.len - 1])) return .InvalidBucketName;

    // Check all characters and patterns.
    var prev: u8 = 0;
    for (name) |ch| {
        if (ch != '-' and ch != '.' and !isLowerAlphaNum(ch)) return .InvalidBucketName;

        // No consecutive dots.
        if (ch == '.' and prev == '.') return .InvalidBucketName;
        // No adjacent dot-hyphen or hyphen-dot.
        if ((ch == '.' and prev == '-') or (ch == '-' and prev == '.')) return .InvalidBucketName;

        prev = ch;
    }

    // Must not look like an IP address (N.N.N.N where each N is 1-3 digits).
    if (looksLikeIpAddress(name)) return .InvalidBucketName;

    // Must not start with "xn--".
    if (name.len >= 4 and std.mem.eql(u8, name[0..4], "xn--")) return .InvalidBucketName;

    // Must not end with "-s3alias" or "--ol-s3".
    if (std.mem.endsWith(u8, name, "-s3alias")) return .InvalidBucketName;
    if (std.mem.endsWith(u8, name, "--ol-s3")) return .InvalidBucketName;

    return null; // valid
}

/// Validate an S3 object key.
/// Returns null if valid, or the appropriate S3Error if invalid.
///
/// Rules:
///  - Max 1024 bytes
///  - Must not be empty
pub fn isValidObjectKey(key: []const u8) ?S3Error {
    if (key.len == 0) return .InvalidArgument;
    if (key.len > 1024) return .KeyTooLongError;
    return null;
}

/// Validate a max-keys query parameter value.
/// Must be a positive integer between 0 and 1000.
/// Returns null if valid, or InvalidArgument if invalid.
pub fn validateMaxKeys(value: []const u8) ?S3Error {
    const n = std.fmt.parseInt(u32, value, 10) catch return .InvalidArgument;
    if (n > 1000) return .InvalidArgument;
    return null;
}

/// Validate a part-number query parameter value.
/// Must be an integer between 1 and 10000.
/// Returns null if valid, or InvalidArgument if invalid.
pub fn validatePartNumber(value: []const u8) ?S3Error {
    const n = std.fmt.parseInt(u32, value, 10) catch return .InvalidArgument;
    if (n < 1 or n > 10000) return .InvalidArgument;
    return null;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn isLowerAlphaNum(ch: u8) bool {
    return (ch >= 'a' and ch <= 'z') or (ch >= '0' and ch <= '9');
}

fn looksLikeIpAddress(name: []const u8) bool {
    var parts: u32 = 0;
    var iter = std.mem.splitScalar(u8, name, '.');
    while (iter.next()) |segment| {
        if (segment.len == 0 or segment.len > 3) return false;
        for (segment) |ch| {
            if (ch < '0' or ch > '9') return false;
        }
        // Validate range 0-255.
        const val = std.fmt.parseInt(u16, segment, 10) catch return false;
        if (val > 255) return false;
        parts += 1;
    }
    return parts == 4;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "isValidBucketName: valid names" {
    try std.testing.expect(isValidBucketName("my-bucket") == null);
    try std.testing.expect(isValidBucketName("abc") == null);
    try std.testing.expect(isValidBucketName("my.bucket.name") == null);
    try std.testing.expect(isValidBucketName("bucket123") == null);
    try std.testing.expect(isValidBucketName("a-b-c") == null);
    try std.testing.expect(isValidBucketName("a" ** 63) == null);
}

test "isValidBucketName: too short" {
    try std.testing.expect(isValidBucketName("ab") != null);
    try std.testing.expect(isValidBucketName("a") != null);
    try std.testing.expect(isValidBucketName("") != null);
}

test "isValidBucketName: too long" {
    try std.testing.expect(isValidBucketName("a" ** 64) != null);
}

test "isValidBucketName: uppercase rejected" {
    try std.testing.expect(isValidBucketName("MyBucket") != null);
    try std.testing.expect(isValidBucketName("BUCKET") != null);
}

test "isValidBucketName: invalid start/end characters" {
    try std.testing.expect(isValidBucketName("-bucket") != null);
    try std.testing.expect(isValidBucketName("bucket-") != null);
    try std.testing.expect(isValidBucketName(".bucket") != null);
    try std.testing.expect(isValidBucketName("bucket.") != null);
}

test "isValidBucketName: consecutive dots rejected" {
    try std.testing.expect(isValidBucketName("my..bucket") != null);
}

test "isValidBucketName: adjacent dot-hyphen rejected" {
    try std.testing.expect(isValidBucketName("my.-bucket") != null);
    try std.testing.expect(isValidBucketName("my-.bucket") != null);
}

test "isValidBucketName: IP address format rejected" {
    try std.testing.expect(isValidBucketName("192.168.0.1") != null);
    try std.testing.expect(isValidBucketName("10.0.0.1") != null);
}

test "isValidBucketName: xn-- prefix rejected" {
    try std.testing.expect(isValidBucketName("xn--bucket") != null);
}

test "isValidBucketName: -s3alias suffix rejected" {
    try std.testing.expect(isValidBucketName("bucket-s3alias") != null);
}

test "isValidBucketName: --ol-s3 suffix rejected" {
    try std.testing.expect(isValidBucketName("bucket--ol-s3") != null);
}

test "isValidObjectKey: valid keys" {
    try std.testing.expect(isValidObjectKey("hello.txt") == null);
    try std.testing.expect(isValidObjectKey("path/to/file.txt") == null);
    try std.testing.expect(isValidObjectKey("a") == null);
}

test "isValidObjectKey: empty key rejected" {
    try std.testing.expect(isValidObjectKey("") != null);
}

test "isValidObjectKey: too long key rejected" {
    const long_key = "x" ** 1025;
    try std.testing.expect(isValidObjectKey(long_key) != null);
}

test "isValidObjectKey: max length key accepted" {
    const max_key = "x" ** 1024;
    try std.testing.expect(isValidObjectKey(max_key) == null);
}

test "validateMaxKeys: valid values" {
    try std.testing.expect(validateMaxKeys("0") == null);
    try std.testing.expect(validateMaxKeys("1") == null);
    try std.testing.expect(validateMaxKeys("500") == null);
    try std.testing.expect(validateMaxKeys("1000") == null);
}

test "validateMaxKeys: invalid values" {
    try std.testing.expect(validateMaxKeys("1001") != null);
    try std.testing.expect(validateMaxKeys("-1") != null);
    try std.testing.expect(validateMaxKeys("abc") != null);
    try std.testing.expect(validateMaxKeys("") != null);
}

test "validatePartNumber: valid values" {
    try std.testing.expect(validatePartNumber("1") == null);
    try std.testing.expect(validatePartNumber("5000") == null);
    try std.testing.expect(validatePartNumber("10000") == null);
}

test "validatePartNumber: invalid values" {
    try std.testing.expect(validatePartNumber("0") != null);
    try std.testing.expect(validatePartNumber("10001") != null);
    try std.testing.expect(validatePartNumber("abc") != null);
    try std.testing.expect(validatePartNumber("") != null);
}
