const std = @import("std");

/// Metadata for a stored bucket.
pub const BucketMeta = struct {
    name: []const u8,
    creation_date: []const u8,
    region: []const u8,
    owner_id: []const u8,
    owner_display: []const u8 = "",
    acl: []const u8 = "{}",

    /// Free all heap-allocated slices using the given allocator.
    /// Only call this on BucketMeta instances returned from the metadata store
    /// (where all fields are allocator-owned). Do NOT call on stack-constructed
    /// instances with string literal defaults.
    pub fn deinit(self: *const BucketMeta, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.creation_date);
        allocator.free(self.region);
        allocator.free(self.owner_id);
        allocator.free(self.owner_display);
        allocator.free(self.acl);
    }
};

/// Metadata for a stored object.
pub const ObjectMeta = struct {
    bucket: []const u8,
    key: []const u8,
    size: u64,
    etag: []const u8,
    content_type: []const u8,
    last_modified: []const u8,
    storage_class: []const u8,
    user_metadata: ?[]const u8 = null,
    version_id: ?[]const u8 = null,
    content_encoding: ?[]const u8 = null,
    content_language: ?[]const u8 = null,
    content_disposition: ?[]const u8 = null,
    cache_control: ?[]const u8 = null,
    expires: ?[]const u8 = null,
    acl: []const u8 = "{}",
    delete_marker: bool = false,
};

/// Metadata for a multipart upload.
pub const MultipartUploadMeta = struct {
    upload_id: []const u8,
    bucket: []const u8,
    key: []const u8,
    initiated: []const u8,
    content_type: []const u8 = "application/octet-stream",
    content_encoding: ?[]const u8 = null,
    content_language: ?[]const u8 = null,
    content_disposition: ?[]const u8 = null,
    cache_control: ?[]const u8 = null,
    expires: ?[]const u8 = null,
    storage_class: []const u8 = "STANDARD",
    acl: []const u8 = "{}",
    user_metadata: []const u8 = "{}",
    owner_id: []const u8 = "",
    owner_display: []const u8 = "",
};

/// Metadata for a single part in a multipart upload.
pub const PartMeta = struct {
    part_number: u32,
    etag: []const u8,
    size: u64,
    last_modified: []const u8,
};

/// Result of a listObjects call.
pub const ListObjectsResult = struct {
    objects: []ObjectMeta,
    common_prefixes: [][]const u8,
    is_truncated: bool,
    next_continuation_token: ?[]const u8 = null,
    next_marker: ?[]const u8 = null,
};

/// Result of a listMultipartUploads call.
pub const ListUploadsResult = struct {
    uploads: []MultipartUploadMeta,
    is_truncated: bool,
    next_key_marker: ?[]const u8 = null,
    next_upload_id_marker: ?[]const u8 = null,
};

/// Result of a listParts call.
pub const ListPartsResult = struct {
    parts: []PartMeta,
    is_truncated: bool,
    next_part_number_marker: u32 = 0,
};

/// Credential record.
pub const Credential = struct {
    access_key_id: []const u8,
    secret_key: []const u8,
    owner_id: []const u8,
    display_name: []const u8 = "",
    active: bool = true,
    created_at: []const u8 = "",
};

/// MetadataStore interface using a vtable pattern.
///
/// Consumers call methods via the function pointers. Concrete implementations
/// (e.g., SqliteMetadataStore) populate the vtable and store their own state
/// behind the `ctx` opaque pointer.
pub const MetadataStore = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        // --- Buckets ---
        createBucket: *const fn (ctx: *anyopaque, meta: BucketMeta) anyerror!void,
        deleteBucket: *const fn (ctx: *anyopaque, name: []const u8) anyerror!void,
        getBucket: *const fn (ctx: *anyopaque, name: []const u8) anyerror!?BucketMeta,
        listBuckets: *const fn (ctx: *anyopaque) anyerror![]BucketMeta,
        bucketExists: *const fn (ctx: *anyopaque, name: []const u8) anyerror!bool,
        updateBucketAcl: *const fn (ctx: *anyopaque, name: []const u8, acl: []const u8) anyerror!void,

        // --- Objects ---
        putObjectMeta: *const fn (ctx: *anyopaque, meta: ObjectMeta) anyerror!void,
        getObjectMeta: *const fn (ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!?ObjectMeta,
        deleteObjectMeta: *const fn (ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!bool,
        deleteObjectsMeta: *const fn (ctx: *anyopaque, bucket: []const u8, keys: []const []const u8) anyerror![]bool,
        listObjectsMeta: *const fn (ctx: *anyopaque, bucket: []const u8, prefix: []const u8, delimiter: []const u8, start_after: []const u8, max_keys: u32) anyerror!ListObjectsResult,
        objectExists: *const fn (ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!bool,
        updateObjectAcl: *const fn (ctx: *anyopaque, bucket: []const u8, key: []const u8, acl: []const u8) anyerror!void,

        // --- Multipart ---
        createMultipartUpload: *const fn (ctx: *anyopaque, meta: MultipartUploadMeta) anyerror!void,
        getMultipartUpload: *const fn (ctx: *anyopaque, upload_id: []const u8) anyerror!?MultipartUploadMeta,
        abortMultipartUpload: *const fn (ctx: *anyopaque, upload_id: []const u8) anyerror!void,
        putPartMeta: *const fn (ctx: *anyopaque, upload_id: []const u8, part: PartMeta) anyerror!void,
        listPartsMeta: *const fn (ctx: *anyopaque, upload_id: []const u8, max_parts: u32, part_marker: u32) anyerror!ListPartsResult,
        getPartsForCompletion: *const fn (ctx: *anyopaque, upload_id: []const u8) anyerror![]PartMeta,
        completeMultipartUpload: *const fn (ctx: *anyopaque, upload_id: []const u8, object_meta: ObjectMeta) anyerror!void,
        listMultipartUploads: *const fn (ctx: *anyopaque, bucket: []const u8, prefix: []const u8, max_uploads: u32) anyerror!ListUploadsResult,

        // --- Credentials ---
        getCredential: *const fn (ctx: *anyopaque, access_key_id: []const u8) anyerror!?Credential,
        putCredential: *const fn (ctx: *anyopaque, cred: Credential) anyerror!void,

        // --- Counts ---
        countBuckets: *const fn (ctx: *anyopaque) anyerror!u64,
        countObjects: *const fn (ctx: *anyopaque) anyerror!u64,
    };

    // --- Convenience wrappers ---

    pub fn createBucket(self: MetadataStore, meta: BucketMeta) !void {
        return self.vtable.createBucket(self.ctx, meta);
    }

    pub fn deleteBucket(self: MetadataStore, name: []const u8) !void {
        return self.vtable.deleteBucket(self.ctx, name);
    }

    pub fn getBucket(self: MetadataStore, name: []const u8) !?BucketMeta {
        return self.vtable.getBucket(self.ctx, name);
    }

    pub fn listBuckets(self: MetadataStore) ![]BucketMeta {
        return self.vtable.listBuckets(self.ctx);
    }

    pub fn bucketExists(self: MetadataStore, name: []const u8) !bool {
        return self.vtable.bucketExists(self.ctx, name);
    }

    pub fn updateBucketAcl(self: MetadataStore, name: []const u8, acl: []const u8) !void {
        return self.vtable.updateBucketAcl(self.ctx, name, acl);
    }

    pub fn putObjectMeta(self: MetadataStore, meta: ObjectMeta) !void {
        return self.vtable.putObjectMeta(self.ctx, meta);
    }

    pub fn getObjectMeta(self: MetadataStore, bucket: []const u8, key: []const u8) !?ObjectMeta {
        return self.vtable.getObjectMeta(self.ctx, bucket, key);
    }

    pub fn deleteObjectMeta(self: MetadataStore, bucket: []const u8, key: []const u8) !bool {
        return self.vtable.deleteObjectMeta(self.ctx, bucket, key);
    }

    pub fn deleteObjectsMeta(self: MetadataStore, bucket: []const u8, keys: []const []const u8) ![]bool {
        return self.vtable.deleteObjectsMeta(self.ctx, bucket, keys);
    }

    pub fn listObjectsMeta(self: MetadataStore, bucket: []const u8, prefix: []const u8, delimiter: []const u8, start_after: []const u8, max_keys: u32) !ListObjectsResult {
        return self.vtable.listObjectsMeta(self.ctx, bucket, prefix, delimiter, start_after, max_keys);
    }

    pub fn objectExists(self: MetadataStore, bucket: []const u8, key: []const u8) !bool {
        return self.vtable.objectExists(self.ctx, bucket, key);
    }

    pub fn updateObjectAcl(self: MetadataStore, bucket: []const u8, key: []const u8, acl: []const u8) !void {
        return self.vtable.updateObjectAcl(self.ctx, bucket, key, acl);
    }

    pub fn createMultipartUpload(self: MetadataStore, meta: MultipartUploadMeta) !void {
        return self.vtable.createMultipartUpload(self.ctx, meta);
    }

    pub fn getMultipartUpload(self: MetadataStore, upload_id: []const u8) !?MultipartUploadMeta {
        return self.vtable.getMultipartUpload(self.ctx, upload_id);
    }

    pub fn abortMultipartUpload(self: MetadataStore, upload_id: []const u8) !void {
        return self.vtable.abortMultipartUpload(self.ctx, upload_id);
    }

    pub fn putPartMeta(self: MetadataStore, upload_id: []const u8, part: PartMeta) !void {
        return self.vtable.putPartMeta(self.ctx, upload_id, part);
    }

    pub fn listPartsMeta(self: MetadataStore, upload_id: []const u8, max_parts: u32, part_marker: u32) !ListPartsResult {
        return self.vtable.listPartsMeta(self.ctx, upload_id, max_parts, part_marker);
    }

    pub fn getPartsForCompletion(self: MetadataStore, upload_id: []const u8) ![]PartMeta {
        return self.vtable.getPartsForCompletion(self.ctx, upload_id);
    }

    pub fn completeMultipartUpload(self: MetadataStore, upload_id: []const u8, object_meta: ObjectMeta) !void {
        return self.vtable.completeMultipartUpload(self.ctx, upload_id, object_meta);
    }

    pub fn listMultipartUploads(self: MetadataStore, bucket: []const u8, prefix: []const u8, max_uploads: u32) !ListUploadsResult {
        return self.vtable.listMultipartUploads(self.ctx, bucket, prefix, max_uploads);
    }

    pub fn getCredential(self: MetadataStore, access_key_id: []const u8) !?Credential {
        return self.vtable.getCredential(self.ctx, access_key_id);
    }

    pub fn putCredential(self: MetadataStore, cred: Credential) !void {
        return self.vtable.putCredential(self.ctx, cred);
    }

    pub fn countBuckets(self: MetadataStore) !u64 {
        return self.vtable.countBuckets(self.ctx);
    }

    pub fn countObjects(self: MetadataStore) !u64 {
        return self.vtable.countObjects(self.ctx);
    }
};
