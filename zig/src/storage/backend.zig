const std = @import("std");

/// Result of a getObject or headObject call.
pub const ObjectData = struct {
    body: ?[]const u8,
    content_length: u64,
    content_type: []const u8,
    etag: []const u8,
    last_modified: []const u8,
};

/// Options for a putObject call.
pub const PutObjectOptions = struct {
    content_type: []const u8 = "application/octet-stream",
    content_length: u64 = 0,
};

/// Result of a putObject call.
pub const PutObjectResult = struct {
    etag: []const u8,
};

/// Describes a single completed part in a multipart upload.
pub const PartInfo = struct {
    part_number: u32,
    etag: []const u8,
};

/// Result of a putPart call.
pub const PutPartResult = struct {
    etag: []const u8,
};

/// Result of an assembleParts call.
pub const AssemblePartsResult = struct {
    etag: []const u8,
    total_size: u64 = 0,
};

/// StorageBackend interface using a vtable pattern.
///
/// Concrete implementations (LocalBackend, AwsGatewayBackend, etc.) populate
/// the vtable and store their own state behind the `ctx` opaque pointer.
pub const StorageBackend = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        putObject: *const fn (ctx: *anyopaque, bucket: []const u8, key: []const u8, data: []const u8, opts: PutObjectOptions) anyerror!PutObjectResult,
        getObject: *const fn (ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!ObjectData,
        deleteObject: *const fn (ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!void,
        headObject: *const fn (ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!ObjectData,
        copyObject: *const fn (ctx: *anyopaque, src_bucket: []const u8, src_key: []const u8, dst_bucket: []const u8, dst_key: []const u8) anyerror!PutObjectResult,
        putPart: *const fn (ctx: *anyopaque, bucket: []const u8, upload_id: []const u8, part_number: u32, data: []const u8) anyerror!PutPartResult,
        assembleParts: *const fn (ctx: *anyopaque, bucket: []const u8, key: []const u8, upload_id: []const u8, parts: []const PartInfo) anyerror!AssemblePartsResult,
        deleteParts: *const fn (ctx: *anyopaque, bucket: []const u8, upload_id: []const u8) anyerror!void,
        createBucket: *const fn (ctx: *anyopaque, bucket: []const u8) anyerror!void,
        deleteBucket: *const fn (ctx: *anyopaque, bucket: []const u8) anyerror!void,
    };

    // --- Convenience wrappers ---

    pub fn putObject(self: StorageBackend, bucket: []const u8, key: []const u8, data: []const u8, opts: PutObjectOptions) !PutObjectResult {
        return self.vtable.putObject(self.ctx, bucket, key, data, opts);
    }

    pub fn getObject(self: StorageBackend, bucket: []const u8, key: []const u8) !ObjectData {
        return self.vtable.getObject(self.ctx, bucket, key);
    }

    pub fn deleteObject(self: StorageBackend, bucket: []const u8, key: []const u8) !void {
        return self.vtable.deleteObject(self.ctx, bucket, key);
    }

    pub fn headObject(self: StorageBackend, bucket: []const u8, key: []const u8) !ObjectData {
        return self.vtable.headObject(self.ctx, bucket, key);
    }

    pub fn copyObject(self: StorageBackend, src_bucket: []const u8, src_key: []const u8, dst_bucket: []const u8, dst_key: []const u8) !PutObjectResult {
        return self.vtable.copyObject(self.ctx, src_bucket, src_key, dst_bucket, dst_key);
    }

    pub fn putPart(self: StorageBackend, bucket: []const u8, upload_id: []const u8, part_number: u32, data: []const u8) !PutPartResult {
        return self.vtable.putPart(self.ctx, bucket, upload_id, part_number, data);
    }

    pub fn assembleParts(self: StorageBackend, bucket: []const u8, key: []const u8, upload_id: []const u8, parts: []const PartInfo) !AssemblePartsResult {
        return self.vtable.assembleParts(self.ctx, bucket, key, upload_id, parts);
    }

    pub fn deleteParts(self: StorageBackend, bucket: []const u8, upload_id: []const u8) !void {
        return self.vtable.deleteParts(self.ctx, bucket, upload_id);
    }

    pub fn createBucket(self: StorageBackend, bucket: []const u8) !void {
        return self.vtable.createBucket(self.ctx, bucket);
    }

    pub fn deleteBucket(self: StorageBackend, bucket: []const u8) !void {
        return self.vtable.deleteBucket(self.ctx, bucket);
    }
};
