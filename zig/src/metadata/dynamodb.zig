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

pub const DynamoDBConfig = struct {
    table: []const u8,
    region: []const u8,
    endpoint_url: []const u8 = "",
    access_key_id: []const u8,
    secret_access_key: []const u8,
};

pub const DynamoDBMetadataStore = struct {
    allocator: std.mem.Allocator,
    config: DynamoDBConfig,
    mutex: std.Thread.Mutex,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, config: DynamoDBConfig) !Self {
        std.log.info("DynamoDB metadata store initialized: table={s} region={s}", .{ config.table, config.region });
        return Self{ .allocator = allocator, .config = config, .mutex = .{} };
    }

    pub fn deinit(self: *Self) void {
        _ = self;
    }

    fn dupe(self: *Self, s: []const u8) ![]const u8 {
        return self.allocator.dupe(u8, s);
    }

    fn createBucket(ctx: *anyopaque, meta: BucketMeta) anyerror!void {
        _ = ctx;
        _ = meta;
        return error.NotImplemented;
    }

    fn deleteBucket(ctx: *anyopaque, name: []const u8) anyerror!void {
        _ = ctx;
        _ = name;
        return error.NotImplemented;
    }

    fn getBucket(ctx: *anyopaque, name: []const u8) anyerror!?BucketMeta {
        _ = ctx;
        _ = name;
        return null;
    }

    fn listBuckets(ctx: *anyopaque) anyerror![]BucketMeta {
        const self = getSelf(ctx);
        var list: std.ArrayList(BucketMeta) = .empty;
        errdefer {
            for (list.items) |*item| item.deinit(self.allocator);
            list.deinit(self.allocator);
        }
        return list.toOwnedSlice(self.allocator);
    }

    fn bucketExists(ctx: *anyopaque, name: []const u8) anyerror!bool {
        _ = ctx;
        _ = name;
        return false;
    }

    fn updateBucketAcl(ctx: *anyopaque, name: []const u8, acl: []const u8) anyerror!void {
        _ = ctx;
        _ = name;
        _ = acl;
        return error.NotImplemented;
    }

    fn putObjectMeta(ctx: *anyopaque, meta: ObjectMeta) anyerror!void {
        _ = ctx;
        _ = meta;
        return error.NotImplemented;
    }

    fn getObjectMeta(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!?ObjectMeta {
        _ = ctx;
        _ = bucket;
        _ = key;
        return null;
    }

    fn deleteObjectMeta(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!bool {
        _ = ctx;
        _ = bucket;
        _ = key;
        return false;
    }

    fn deleteObjectsMeta(ctx: *anyopaque, bucket: []const u8, keys: []const []const u8) anyerror![]bool {
        _ = bucket;
        const self = getSelf(ctx);
        const results = try self.allocator.alloc(bool, keys.len);
        @memset(results, false);
        return results;
    }

    fn listObjectsMeta(ctx: *anyopaque, bucket: []const u8, prefix: []const u8, delimiter: []const u8, start_after: []const u8, max_keys: u32) anyerror!ListObjectsResult {
        _ = bucket;
        _ = prefix;
        _ = delimiter;
        _ = start_after;
        _ = max_keys;
        const self = getSelf(ctx);
        var objects_list: std.ArrayList(ObjectMeta) = .empty;
        errdefer {
            for (objects_list.items) |*obj| {
                self.allocator.free(obj.bucket);
                self.allocator.free(obj.key);
                self.allocator.free(obj.etag);
                self.allocator.free(obj.content_type);
                self.allocator.free(obj.last_modified);
                self.allocator.free(obj.storage_class);
                self.allocator.free(obj.acl);
            }
            objects_list.deinit(self.allocator);
        }
        return ListObjectsResult{ .objects = try objects_list.toOwnedSlice(self.allocator), .common_prefixes = &.{}, .is_truncated = false };
    }

    fn objectExists(ctx: *anyopaque, bucket: []const u8, key: []const u8) anyerror!bool {
        _ = ctx;
        _ = bucket;
        _ = key;
        return false;
    }

    fn updateObjectAcl(ctx: *anyopaque, bucket: []const u8, key: []const u8, acl: []const u8) anyerror!void {
        _ = ctx;
        _ = bucket;
        _ = key;
        _ = acl;
        return error.NotImplemented;
    }

    fn createMultipartUpload(ctx: *anyopaque, meta: MultipartUploadMeta) anyerror!void {
        _ = ctx;
        _ = meta;
        return error.NotImplemented;
    }

    fn getMultipartUpload(ctx: *anyopaque, upload_id: []const u8) anyerror!?MultipartUploadMeta {
        _ = ctx;
        _ = upload_id;
        return null;
    }

    fn abortMultipartUpload(ctx: *anyopaque, upload_id: []const u8) anyerror!void {
        _ = ctx;
        _ = upload_id;
        return error.NotImplemented;
    }

    fn putPartMeta(ctx: *anyopaque, upload_id: []const u8, part: PartMeta) anyerror!void {
        _ = ctx;
        _ = upload_id;
        _ = part;
        return error.NotImplemented;
    }

    fn listPartsMeta(ctx: *anyopaque, upload_id: []const u8, max_parts: u32, part_marker: u32) anyerror!ListPartsResult {
        _ = upload_id;
        _ = max_parts;
        _ = part_marker;
        const self = getSelf(ctx);
        var parts_list: std.ArrayList(PartMeta) = .empty;
        errdefer {
            for (parts_list.items) |*p| {
                self.allocator.free(p.etag);
                self.allocator.free(p.last_modified);
            }
            parts_list.deinit(self.allocator);
        }
        return ListPartsResult{ .parts = try parts_list.toOwnedSlice(self.allocator), .is_truncated = false };
    }

    fn getPartsForCompletion(ctx: *anyopaque, upload_id: []const u8) anyerror![]PartMeta {
        const result = try listPartsMeta(ctx, upload_id, 10000, 0);
        return result.parts;
    }

    fn completeMultipartUpload(ctx: *anyopaque, upload_id: []const u8, object_meta: ObjectMeta) anyerror!void {
        _ = ctx;
        _ = upload_id;
        _ = object_meta;
        return error.NotImplemented;
    }

    fn listMultipartUploads(ctx: *anyopaque, bucket: []const u8, prefix: []const u8, max_uploads: u32) anyerror!ListUploadsResult {
        _ = bucket;
        _ = prefix;
        _ = max_uploads;
        const self = getSelf(ctx);
        var uploads_list: std.ArrayList(MultipartUploadMeta) = .empty;
        errdefer {
            for (uploads_list.items) |*u| {
                self.allocator.free(u.upload_id);
                self.allocator.free(u.bucket);
                self.allocator.free(u.key);
                self.allocator.free(u.initiated);
                self.allocator.free(u.content_type);
                self.allocator.free(u.storage_class);
                self.allocator.free(u.acl);
            }
            uploads_list.deinit(self.allocator);
        }
        return ListUploadsResult{ .uploads = try uploads_list.toOwnedSlice(self.allocator), .is_truncated = false };
    }

    fn getCredential(ctx: *anyopaque, access_key_id: []const u8) anyerror!?Credential {
        _ = ctx;
        _ = access_key_id;
        return null;
    }

    fn putCredential(ctx: *anyopaque, cred: Credential) anyerror!void {
        _ = ctx;
        _ = cred;
        return error.NotImplemented;
    }

    fn countBuckets(ctx: *anyopaque) anyerror!u64 {
        _ = ctx;
        return 0;
    }

    fn countObjects(ctx: *anyopaque) anyerror!u64 {
        _ = ctx;
        return 0;
    }

    fn getSelf(ctx: *anyopaque) *Self {
        return @ptrCast(@alignCast(ctx));
    }

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

    pub fn metadataStore(self: *Self) MetadataStore {
        return .{ .ctx = @ptrCast(self), .vtable = &vtable };
    }
};
