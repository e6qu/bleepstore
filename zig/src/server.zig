const std = @import("std");
const tk = @import("tokamak");

const bucket_handlers = @import("handlers/bucket.zig");
const object_handlers = @import("handlers/object.zig");
const multipart_handlers = @import("handlers/multipart.zig");
const MetadataStore = @import("metadata/store.zig").MetadataStore;
const SqliteMetadataStore = @import("metadata/sqlite.zig").SqliteMetadataStore;
const StorageBackend = @import("storage/backend.zig").StorageBackend;
const s3err = @import("errors.zig");
const xml = @import("xml.zig");
const config_mod = @import("config.zig");
const metrics_mod = @import("metrics.zig");
const auth_mod = @import("auth.zig");

/// Global shutdown flag, set by SIGTERM/SIGINT handler.
pub var shutdown_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

/// Global pointer to the metadata store. Set by Server.init(), used by handlers
/// via the static catch-all function (tokamak route handlers are static functions
/// with no direct access to server state).
pub var global_metadata_store: ?MetadataStore = null;

/// Global pointer to the storage backend. Set by Server.init().
pub var global_storage_backend: ?StorageBackend = null;

/// Global configuration values for handler access. Set by Server.init().
/// Stored as copies rather than pointers to avoid lifetime issues (Server
/// struct is returned by value from init).
pub var global_region: []const u8 = "us-east-1";
pub var global_access_key: []const u8 = "bleepstore";

/// Global auth configuration. Set by Server.init().
pub var global_auth_enabled: bool = true;

/// Global allocator (GPA) for freeing credential lookups.
/// The MetadataStore allocates credential strings using its internal allocator
/// (the GPA passed at SqliteMetadataStore.init). We need access to this
/// allocator in the auth middleware to free credentials after verification.
pub var global_allocator: ?std.mem.Allocator = null;

pub const ServerState = struct {
    allocator: std.mem.Allocator,
    config: config_mod.Config,
    metadata_store: ?MetadataStore = null,
    storage_backend: ?StorageBackend = null,
};

// ---------------------------------------------------------------------------
// Route table
// ---------------------------------------------------------------------------

/// The tokamak route table for BleepStore.
/// Infrastructure endpoints are registered with specific paths.
/// S3 routes use a catch-all handler at the end.
pub const routes: []const tk.Route = &.{
    // Infrastructure endpoints
    .get("/health", handleHealth),
    .get("/metrics", handleMetrics),
    .get("/docs", handleSwaggerUi),
    .get("/openapi.json", handleOpenApiJson),

    // S3 API catch-all: matches any method, any path not matched above.
    // Uses a Context handler since tokamak lacks .head() and S3 routing
    // is too complex for path-based patterns (query param dispatch, etc.).
    tk.Route{ .handler = handleS3CatchAll },
};

// ---------------------------------------------------------------------------
// Infrastructure handlers
//
// These use tokamak's DI-based handler signatures.
// ---------------------------------------------------------------------------

fn handleHealth(res: *tk.Response) void {
    res.status = 200;
    res.content_type = .JSON;
    const request_id = generateRequestId();
    setCommonHeaders(res, &request_id);
    res.body = "{\"status\":\"ok\"}";
}

fn handleMetrics(res: *tk.Response) void {
    res.status = 200;
    res.header("Content-Type", "text/plain; version=0.0.4");
    const body = metrics_mod.renderMetrics(res.arena) catch {
        res.status = 500;
        res.body = "Error rendering metrics";
        return;
    };
    res.body = body;
}

fn handleSwaggerUi(res: *tk.Response) void {
    res.status = 200;
    res.content_type = .HTML;
    res.body = swagger_ui_html;
}

fn handleOpenApiJson(res: *tk.Response) void {
    res.status = 200;
    res.content_type = .JSON;
    res.body = openapi_json;
}

// ---------------------------------------------------------------------------
// S3 catch-all handler
//
// This is a raw tokamak Context handler that dispatches all S3 requests.
// ---------------------------------------------------------------------------

fn handleS3CatchAll(ctx: *tk.Context) anyerror!void {
    const req = ctx.req;
    const res = ctx.res;

    // Mark as responded so tokamak doesn't return 404 after us.
    ctx.responded = true;

    // Handle Expect: 100-continue. boto3 sends this on PutObject requests.
    // Without this, the client waits for "100 Continue" before sending the body,
    // but the server processes the request immediately. The response gets
    // misinterpreted by the client as it expects the 100 Continue first.
    if (req.header("expect")) |expect_val| {
        if (std.mem.eql(u8, expect_val, "100-continue")) {
            res.conn.writeAll("HTTP/1.1 100 Continue\r\n\r\n") catch {};
        }
    }

    // Increment HTTP request counter for S3 routes.
    metrics_mod.incrementHttpRequests();

    // Per-request arena allocator (httpz provides res.arena).
    const req_alloc = res.arena;

    // Generate request ID.
    const request_id = generateRequestId();

    // --- Pre-auth validation ---
    // Check object key length BEFORE auth to avoid signature mismatches
    // on keys that exceed the 1024-byte S3 limit. AWS S3 also rejects
    // overly-long keys before checking auth.
    {
        const path = req.url.path;
        const trimmed = if (path.len > 0 and path[0] == '/') path[1..] else path;
        const slash_index = std.mem.indexOfScalar(u8, trimmed, '/');
        const raw_object_key = if (slash_index) |si| trimmed[si + 1 ..] else "";
        // URL-decode the key to get the actual byte length.
        const object_key = if (raw_object_key.len > 0)
            uriDecodePath(req_alloc, raw_object_key) catch raw_object_key
        else
            raw_object_key;
        if (object_key.len > 1024) {
            sendS3Error(res, req_alloc, .KeyTooLongError, object_key, &request_id) catch {};
            return;
        }
    }

    // Reject requests with unsupported Transfer-Encoding.
    // S3 only supports chunked transfer encoding. "identity" or other values
    // without a Content-Length should be rejected.
    if (req.header("transfer-encoding")) |te| {
        if (!std.mem.eql(u8, te, "chunked")) {
            sendS3Error(res, req_alloc, .InvalidRequest, te, &request_id) catch {};
            return;
        }
    }

    // --- Auth middleware ---
    // Check authentication before routing.
    if (global_auth_enabled) {
        authenticateRequest(req, res, req_alloc, &request_id) catch |err| {
            switch (err) {
                error.InvalidAccessKeyId => {
                    sendS3Error(res, req_alloc, .InvalidAccessKeyId, req.url.raw, &request_id) catch {};
                    return;
                },
                error.SignatureDoesNotMatch => {
                    sendS3Error(res, req_alloc, .SignatureDoesNotMatch, req.url.raw, &request_id) catch {};
                    return;
                },
                error.RequestTimeTooSkewed => {
                    sendS3Error(res, req_alloc, .RequestTimeTooSkewed, req.url.raw, &request_id) catch {};
                    return;
                },
                error.AccessDenied, error.MalformedAuthHeader, error.UnsupportedAlgorithm => {
                    sendS3Error(res, req_alloc, .AccessDenied, req.url.raw, &request_id) catch {};
                    return;
                },
                error.ExpiredRequest => {
                    sendS3Error(res, req_alloc, .AccessDenied, req.url.raw, &request_id) catch {};
                    return;
                },
                error.AuthorizationQueryParametersError => {
                    sendS3Error(res, req_alloc, .InvalidArgument, req.url.raw, &request_id) catch {};
                    return;
                },
                else => {
                    sendS3Error(res, req_alloc, .AccessDenied, req.url.raw, &request_id) catch {};
                    return;
                },
            }
        };
    }

    // Dispatch based on method, path, and query string.
    dispatchS3(req, res, req_alloc, &request_id) catch |err| {
        std.log.err("request error: {} for {s}", .{ err, req.url.raw });
        sendS3Error(res, req_alloc, .InternalError, req.url.raw, &request_id) catch {};
    };
}

/// Authenticate the incoming request using AWS SigV4 (header or presigned URL).
/// Returns void on success, error on authentication failure.
fn authenticateRequest(
    req: *tk.Request,
    res: *tk.Response,
    req_alloc: std.mem.Allocator,
    request_id: *const [16]u8,
) !void {
    _ = res;
    _ = request_id;

    const query = req.url.query;
    const path = req.url.path;

    // Get Authorization header and detect auth type
    const auth_header = req.header("authorization");
    const auth_type = auth_mod.detectAuthType(auth_header, query);

    // No auth present = anonymous request, deny access
    if (auth_type == .none) {
        return error.AccessDenied;
    }

    // Get the metadata store for credential lookup
    const ms = global_metadata_store orelse return error.AccessDenied;

    // Extract the access key ID
    const access_key_id = switch (auth_type) {
        .header => auth_mod.extractAccessKeyFromHeader(auth_header.?) orelse
            return error.MalformedAuthHeader,
        .presigned => auth_mod.extractAccessKeyFromQuery(query) orelse
            return error.MalformedAuthHeader,
        .none => unreachable,
    };

    // Look up credentials from the metadata store
    const cred_opt = ms.getCredential(access_key_id) catch {
        return error.AccessDenied;
    };
    const cred = cred_opt orelse return error.InvalidAccessKeyId;

    // Copy the secret key to the arena allocator (lives for the request duration).
    // Then free the GPA-allocated credential strings to avoid leaks.
    const secret_key = try req_alloc.dupe(u8, cred.secret_key);

    // Free all GPA-allocated credential fields.
    // The credential was allocated by SqliteMetadataStore using the GPA.
    if (global_allocator) |gpa| {
        gpa.free(cred.access_key_id);
        gpa.free(cred.secret_key);
        gpa.free(cred.owner_id);
        gpa.free(cred.display_name);
        gpa.free(cred.created_at);
    }

    // Get the HTTP method as string
    const method_str = httpMethodToString(req.method);

    // Get the host header
    const host = req.header("host");

    // Get the header key/value arrays for lookup.
    // req.headers.keys and .values are fixed-capacity backing arrays.
    // The actual populated count is req.headers.len (NOT keys.len).
    const header_keys = req.headers.keys;
    const header_values = req.headers.values;
    const header_count = req.headers.len;

    // Perform verification based on auth type
    switch (auth_type) {
        .header => {
            const amz_date = req.header("x-amz-date");
            var content_sha256 = req.header("x-amz-content-sha256");

            // If x-amz-content-sha256 is not present (non-S3-specific SigV4 clients),
            // compute the SHA256 of the request body so the signature verification
            // uses the same payload hash the client computed during signing.
            var computed_hash_buf: [64]u8 = undefined;
            if (content_sha256 == null) {
                const body_data = req.body() orelse "";
                var hash: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(body_data, &hash, .{});
                const hex = std.fmt.bytesToHex(hash, .lower);
                @memcpy(&computed_hash_buf, &hex);
                content_sha256 = &computed_hash_buf;
            }

            auth_mod.verifyHeaderAuth(
                req_alloc,
                method_str,
                path,
                query,
                auth_header.?,
                amz_date,
                content_sha256,
                host,
                header_keys[0..header_count],
                header_values[0..header_count],
                secret_key,
                global_region,
            ) catch |err| {
                return err;
            };
        },
        .presigned => {
            auth_mod.verifyPresignedAuth(
                req_alloc,
                method_str,
                path,
                query,
                host,
                header_keys[0..header_count],
                header_values[0..header_count],
                secret_key,
                global_region,
            ) catch |err| {
                return err;
            };
        },
        .none => unreachable,
    }
}

/// Convert httpz Method enum to string.
fn httpMethodToString(method: @TypeOf(@as(tk.Request, undefined).method)) []const u8 {
    return switch (method) {
        .GET => "GET",
        .POST => "POST",
        .PUT => "PUT",
        .DELETE => "DELETE",
        .HEAD => "HEAD",
        .PATCH => "PATCH",
        .OPTIONS => "OPTIONS",
        else => "GET",
    };
}

fn dispatchS3(
    req: *tk.Request,
    res: *tk.Response,
    req_alloc: std.mem.Allocator,
    request_id: *const [16]u8,
) !void {
    const raw_target = req.url.raw;

    // Use the parsed path (without query string) and query separately.
    const path = req.url.path;
    const query = req.url.query;

    // Parse the S3-style path: /<bucket> or /<bucket>/<key...>
    const trimmed = if (path.len > 0 and path[0] == '/') path[1..] else path;

    // Split into bucket and key.
    const slash_index = std.mem.indexOfScalar(u8, trimmed, '/');
    const bucket_name = if (slash_index) |si| trimmed[0..si] else trimmed;
    const raw_object_key = if (slash_index) |si| trimmed[si + 1 ..] else "";

    // URL-decode the object key (percent-encoded characters like %20, %2F, etc.)
    const object_key = if (raw_object_key.len > 0)
        uriDecodePath(req_alloc, raw_object_key) catch raw_object_key
    else
        raw_object_key;

    const method = req.method;

    // --- Routing ---
    if (bucket_name.len == 0) {
        // Service-level operations (e.g., GET / => ListBuckets).
        if (method == .GET) {
            return bucket_handlers.listBuckets(res, req_alloc, request_id);
        }
        return sendS3Error(res, req_alloc, .MethodNotAllowed, raw_target, request_id);
    } else if (object_key.len == 0) {
        // Bucket-level operations.
        if (hasQueryParam(query, "uploads")) {
            return multipart_handlers.listMultipartUploads(res, req_alloc, bucket_name, query, request_id);
        }
        if (hasQueryParam(query, "delete") and method == .POST) {
            return object_handlers.deleteObjects(res, req, req_alloc, bucket_name, request_id);
        }
        if (hasQueryParam(query, "acl")) {
            return switch (method) {
                .GET => bucket_handlers.getBucketAcl(res, req_alloc, bucket_name, request_id),
                .PUT => bucket_handlers.putBucketAcl(res, req, req_alloc, bucket_name, request_id),
                else => sendS3Error(res, req_alloc, .MethodNotAllowed, raw_target, request_id),
            };
        }
        if (hasQueryParam(query, "location")) {
            return bucket_handlers.getBucketLocation(res, req_alloc, bucket_name, request_id);
        }
        return switch (method) {
            .GET => {
                if (hasQueryParam(query, "list-type")) {
                    return object_handlers.listObjectsV2(res, req_alloc, bucket_name, query, request_id);
                }
                // Default GET on bucket = ListObjects V1
                return object_handlers.listObjectsV1(res, req_alloc, bucket_name, query, request_id);
            },
            .PUT => bucket_handlers.createBucket(res, req, req_alloc, bucket_name, request_id),
            .DELETE => bucket_handlers.deleteBucket(res, req_alloc, bucket_name, request_id),
            .HEAD => bucket_handlers.headBucket(res, req_alloc, bucket_name, request_id),
            .POST => {
                // POST on bucket with ?delete handled above
                return sendS3Error(res, req_alloc, .MethodNotAllowed, raw_target, request_id);
            },
            else => sendS3Error(res, req_alloc, .MethodNotAllowed, raw_target, request_id),
        };
    } else {
        // Object-level operations.
        if (hasQueryParam(query, "uploadId")) {
            return routeMultipart(req, res, req_alloc, bucket_name, object_key, query, request_id);
        }
        if (hasQueryParam(query, "uploads") and method == .POST) {
            return multipart_handlers.createMultipartUpload(res, req, req_alloc, bucket_name, object_key, request_id);
        }
        if (hasQueryParam(query, "acl")) {
            return switch (method) {
                .GET => object_handlers.getObjectAcl(res, req_alloc, bucket_name, object_key, request_id),
                .PUT => object_handlers.putObjectAcl(res, req, req_alloc, bucket_name, object_key, request_id),
                else => sendS3Error(res, req_alloc, .MethodNotAllowed, raw_target, request_id),
            };
        }
        return switch (method) {
            .GET => object_handlers.getObject(res, req, req_alloc, bucket_name, object_key, request_id),
            .PUT => {
                // Check for x-amz-copy-source header to dispatch to copyObject.
                if (req.header("x-amz-copy-source") != null) {
                    return object_handlers.copyObject(res, req, req_alloc, bucket_name, object_key, request_id);
                }
                return object_handlers.putObject(res, req, req_alloc, bucket_name, object_key, request_id);
            },
            .DELETE => object_handlers.deleteObject(res, req_alloc, bucket_name, object_key, request_id),
            .HEAD => object_handlers.headObject(res, req_alloc, bucket_name, object_key, request_id),
            else => sendS3Error(res, req_alloc, .MethodNotAllowed, raw_target, request_id),
        };
    }
}

fn routeMultipart(
    req: *tk.Request,
    res: *tk.Response,
    req_alloc: std.mem.Allocator,
    bucket_name: []const u8,
    object_key: []const u8,
    query: []const u8,
    request_id: *const [16]u8,
) !void {
    const method = req.method;
    if (method == .PUT) {
        // Check for x-amz-copy-source header to dispatch to UploadPartCopy.
        if (req.header("x-amz-copy-source") != null) {
            return multipart_handlers.uploadPartCopy(res, req, req_alloc, bucket_name, object_key, query, request_id);
        }
        return multipart_handlers.uploadPart(res, req, req_alloc, bucket_name, object_key, query, request_id);
    } else if (method == .POST) {
        return multipart_handlers.completeMultipartUpload(res, req, req_alloc, bucket_name, object_key, query, request_id);
    } else if (method == .DELETE) {
        return multipart_handlers.abortMultipartUpload(res, req_alloc, bucket_name, object_key, query, request_id);
    } else if (method == .GET) {
        return multipart_handlers.listParts(res, req_alloc, bucket_name, object_key, query, request_id);
    }
    return sendS3Error(res, req_alloc, .MethodNotAllowed, req.url.raw, request_id);
}

// ---------------------------------------------------------------------------
// Server wrapper
// ---------------------------------------------------------------------------

pub const Server = struct {
    allocator: std.mem.Allocator,
    state: ServerState,

    pub fn init(allocator: std.mem.Allocator, cfg: config_mod.Config, metadata_store_ptr: *SqliteMetadataStore, storage_backend: ?StorageBackend) Server {
        const ms = metadata_store_ptr.metadataStore();
        // Set the global metadata store pointer so handlers can access it.
        global_metadata_store = ms;

        // Set the global storage backend pointer.
        global_storage_backend = storage_backend;

        // Set global config values for handler access.
        // These are slices into the config file buffer or string literals,
        // which outlive the Server and its handlers.
        global_region = cfg.server.region;
        global_access_key = cfg.auth.access_key;
        global_auth_enabled = cfg.auth.enabled;
        global_allocator = allocator;

        return .{
            .allocator = allocator,
            .state = ServerState{
                .allocator = allocator,
                .config = cfg,
                .metadata_store = ms,
                .storage_backend = storage_backend,
            },
        };
    }

    pub fn deinit(self: *Server) void {
        _ = self;
    }

    /// Start the HTTP server. This function BLOCKS until the server is stopped.
    /// The tokamak/httpz server is created on the stack here (not stored in a
    /// field) to avoid moving the struct after init -- httpz.Server contains
    /// self-referential pointers that are invalidated by moves.
    pub fn run(self: *Server) !void {
        const port = self.state.config.server.port;
        const host = self.state.config.server.host;
        std.log.info("listening on {s}:{d}", .{ host, port });

        var tk_server = tk.Server.init(self.allocator, routes, .{
            .listen = .{
                .port = port,
                .hostname = host,
            },
            .request = .{
                // Allow large request bodies for multipart uploads.
                // Default httpz max_body_size is too small for 5MB+ parts.
                .max_body_size = 128 * 1024 * 1024, // 128 MB
            },
        }) catch |err| {
            std.log.err("failed to init tokamak server: {}", .{err});
            return err;
        };
        defer tk_server.deinit();

        tk_server.start() catch |err| {
            std.log.err("failed to start tokamak server: {}", .{err});
            return err;
        };
    }
};

// ---------------------------------------------------------------------------
// Shared response helpers
// ---------------------------------------------------------------------------

/// Send a well-formed S3 error XML response with common headers.
pub fn sendS3Error(
    res: *tk.Response,
    alloc: std.mem.Allocator,
    s3error: s3err.S3Error,
    resource: []const u8,
    request_id: *const [16]u8,
) !void {
    const body = try xml.renderError(
        alloc,
        s3error.code(),
        s3error.message(),
        resource,
        request_id,
    );

    setCommonHeaders(res, request_id);
    res.status = @intCast(@intFromEnum(s3error.httpStatus()));
    res.content_type = .XML;
    res.body = body;
}

/// Send a response with common S3 headers.
/// content_type must be a string literal or arena-allocated (httpz stores slices).
pub fn sendResponse(
    res: *tk.Response,
    body: []const u8,
    status: u16,
    content_type: []const u8,
    request_id: *const [16]u8,
) void {
    setCommonHeaders(res, request_id);
    res.status = status;
    // content_type is always a string literal in our code, so safe to pass directly.
    res.header("Content-Type", content_type);
    res.body = body;
}

/// Set common S3 response headers on a Response.
/// Allocates date and request-id strings via the response arena so they
/// outlive the handler stack frame (httpz stores slices, not copies).
pub fn setCommonHeaders(res: *tk.Response, request_id: *const [16]u8) void {
    // Copy request ID into the arena so the slice survives.
    const rid = std.fmt.allocPrint(res.arena, "{s}", .{@as([]const u8, request_id)}) catch "0000000000000000";
    res.header("x-amz-request-id", rid);

    // Format date into an arena-allocated buffer.
    var date_buf: [29]u8 = undefined;
    _ = formatRfc1123Date(&date_buf);
    const date_str = std.fmt.allocPrint(res.arena, "{s}", .{@as([]const u8, &date_buf)}) catch "Thu, 01 Jan 1970 00:00:00 GMT";
    res.header("Date", date_str);

    res.header("Server", "BleepStore");
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/// Generate a 16-character hex request ID from 8 random bytes.
pub fn generateRequestId() [16]u8 {
    var bytes: [8]u8 = undefined;
    std.crypto.random.bytes(&bytes);
    var hex: [16]u8 = undefined;
    const hex_chars = "0123456789ABCDEF";
    for (bytes, 0..) |b, i| {
        hex[i * 2] = hex_chars[b >> 4];
        hex[i * 2 + 1] = hex_chars[b & 0x0f];
    }
    return hex;
}

/// Format the current time as RFC 1123 date into the provided buffer.
/// Returns a slice of the buffer. Format: "Sun, 22 Feb 2026 12:00:00 GMT"
pub fn formatRfc1123Date(buf: *[29]u8) []const u8 {
    const timestamp = std.time.timestamp();
    const epoch_secs: u64 = @intCast(if (timestamp < 0) 0 else timestamp);
    const es = std.time.epoch.EpochSeconds{ .secs = epoch_secs };
    const epoch_day = es.getEpochDay();
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = es.getDaySeconds();

    const year: u16 = year_day.year;
    const month: u4 = month_day.month.numeric();
    const day: u8 = month_day.day_index + 1;

    const hours: u8 = @intCast(day_seconds.getHoursIntoDay());
    const minutes: u8 = @intCast(day_seconds.getMinutesIntoHour());
    const seconds: u8 = @intCast(day_seconds.getSecondsIntoMinute());

    // Day of week: 1970-01-01 was Thursday.
    const day_number: u64 = epoch_day.day;
    const dow_idx: usize = @intCast((day_number + 4) % 7);

    const day_names = [7][]const u8{ "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
    const month_names = [12][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

    _ = std.fmt.bufPrint(buf, "{s}, {d:0>2} {s} {d:0>4} {d:0>2}:{d:0>2}:{d:0>2} GMT", .{
        day_names[dow_idx],
        day,
        month_names[month - 1],
        year,
        hours,
        minutes,
        seconds,
    }) catch {
        const fallback = "Thu, 01 Jan 1970 00:00:00 GMT";
        @memcpy(buf[0..fallback.len], fallback);
    };
    return buf;
}

pub fn hasQueryParam(query: []const u8, key: []const u8) bool {
    if (query.len == 0) return false;
    var params = std.mem.splitScalar(u8, query, '&');
    while (params.next()) |param| {
        const k = if (std.mem.indexOfScalar(u8, param, '=')) |ei| param[0..ei] else param;
        if (std.mem.eql(u8, k, key)) return true;
    }
    return false;
}

/// Decode percent-encoded URI path characters (e.g., "%20" -> " ", "%2F" -> "/").
/// Does NOT decode '+' as space (path encoding, not query encoding).
fn uriDecodePath(alloc: std.mem.Allocator, input: []const u8) ![]u8 {
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(alloc);

    var i: usize = 0;
    while (i < input.len) {
        if (input[i] == '%' and i + 2 < input.len) {
            const high = hexNibble(input[i + 1]);
            const low = hexNibble(input[i + 2]);
            if (high != null and low != null) {
                try result.append(alloc, (high.? << 4) | low.?);
                i += 3;
                continue;
            }
        }
        try result.append(alloc, input[i]);
        i += 1;
    }

    return result.toOwnedSlice(alloc);
}

fn hexNibble(ch: u8) ?u8 {
    return switch (ch) {
        '0'...'9' => ch - '0',
        'a'...'f' => ch - 'a' + 10,
        'A'...'F' => ch - 'A' + 10,
        else => null,
    };
}

/// Extract the value of a query parameter by key. Returns null if not found.
/// Returns the raw (possibly percent-encoded) value.
pub fn getQueryParamValue(query: []const u8, key: []const u8) ?[]const u8 {
    if (query.len == 0) return null;
    var params = std.mem.splitScalar(u8, query, '&');
    while (params.next()) |param| {
        if (std.mem.indexOfScalar(u8, param, '=')) |ei| {
            const k = param[0..ei];
            if (std.mem.eql(u8, k, key)) {
                return param[ei + 1 ..];
            }
        }
    }
    return null;
}

/// Extract and URL-decode a query parameter value. Returns null if not found.
/// Allocates the decoded string with the given allocator.
pub fn getQueryParamDecoded(alloc: std.mem.Allocator, query: []const u8, key: []const u8) ?[]const u8 {
    const raw = getQueryParamValue(query, key) orelse return null;
    return uriDecodePath(alloc, raw) catch raw;
}

// ---------------------------------------------------------------------------
// Embedded static content
// ---------------------------------------------------------------------------

/// Swagger UI HTML page served at /docs.
/// Uses Swagger UI from CDN (unpkg) to render the /openapi.json spec.
const swagger_ui_html =
    \\<!DOCTYPE html>
    \\<html lang="en">
    \\<head>
    \\  <meta charset="UTF-8">
    \\  <title>BleepStore API - Swagger UI</title>
    \\  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css">
    \\</head>
    \\<body>
    \\  <div id="swagger-ui"></div>
    \\  <script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
    \\  <script>
    \\    SwaggerUIBundle({
    \\      url: '/openapi.json',
    \\      dom_id: '#swagger-ui',
    \\      presets: [SwaggerUIBundle.presets.apis],
    \\      layout: 'BaseLayout'
    \\    });
    \\  </script>
    \\</body>
    \\</html>
;

/// Minimal OpenAPI 3.0 JSON specification for current BleepStore routes.
/// This is a hand-built spec reflecting Stage 1b (health + 501 stubs + observability).
const openapi_json =
    \\{
    \\  "openapi": "3.0.0",
    \\  "info": {
    \\    "title": "BleepStore S3-Compatible API",
    \\    "version": "0.1.0",
    \\    "description": "S3-compatible object store (Zig implementation). All S3 operations currently return 501 NotImplemented."
    \\  },
    \\  "servers": [
    \\    { "url": "http://localhost:9013", "description": "Local development" }
    \\  ],
    \\  "paths": {
    \\    "/health": {
    \\      "get": {
    \\        "summary": "Health check",
    \\        "operationId": "HealthCheck",
    \\        "responses": { "200": { "description": "Server is healthy", "content": { "application/json": { "schema": { "type": "object", "properties": { "status": { "type": "string" } } } } } } }
    \\      }
    \\    },
    \\    "/metrics": {
    \\      "get": {
    \\        "summary": "Prometheus metrics",
    \\        "operationId": "GetMetrics",
    \\        "responses": { "200": { "description": "Metrics in Prometheus exposition format", "content": { "text/plain": {} } } }
    \\      }
    \\    },
    \\    "/docs": {
    \\      "get": {
    \\        "summary": "Swagger UI",
    \\        "operationId": "SwaggerUI",
    \\        "responses": { "200": { "description": "Interactive API documentation", "content": { "text/html": {} } } }
    \\      }
    \\    },
    \\    "/": {
    \\      "get": {
    \\        "summary": "List all buckets",
    \\        "operationId": "ListBuckets",
    \\        "tags": ["Bucket"],
    \\        "responses": { "200": { "description": "Bucket list" }, "501": { "description": "Not implemented" } }
    \\      }
    \\    },
    \\    "/{Bucket}": {
    \\      "put": { "summary": "Create bucket", "operationId": "CreateBucket", "tags": ["Bucket"], "responses": { "200": { "description": "Bucket created" }, "501": { "description": "Not implemented" } } },
    \\      "delete": { "summary": "Delete bucket", "operationId": "DeleteBucket", "tags": ["Bucket"], "responses": { "204": { "description": "Bucket deleted" }, "501": { "description": "Not implemented" } } },
    \\      "head": { "summary": "Head bucket", "operationId": "HeadBucket", "tags": ["Bucket"], "responses": { "200": { "description": "Bucket exists" }, "501": { "description": "Not implemented" } } },
    \\      "get": { "summary": "List objects or get bucket location/ACL", "operationId": "GetBucket", "tags": ["Bucket", "Object"], "responses": { "200": { "description": "Object list" }, "501": { "description": "Not implemented" } } }
    \\    },
    \\    "/{Bucket}/{Key}": {
    \\      "put": { "summary": "Put object", "operationId": "PutObject", "tags": ["Object"], "responses": { "200": { "description": "Object stored" }, "501": { "description": "Not implemented" } } },
    \\      "get": { "summary": "Get object", "operationId": "GetObject", "tags": ["Object"], "responses": { "200": { "description": "Object data" }, "501": { "description": "Not implemented" } } },
    \\      "delete": { "summary": "Delete object", "operationId": "DeleteObject", "tags": ["Object"], "responses": { "204": { "description": "Object deleted" }, "501": { "description": "Not implemented" } } },
    \\      "head": { "summary": "Head object", "operationId": "HeadObject", "tags": ["Object"], "responses": { "200": { "description": "Object metadata" }, "501": { "description": "Not implemented" } } }
    \\    }
    \\  }
    \\}
;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "hasQueryParam" {
    try std.testing.expect(hasQueryParam("uploads", "uploads"));
    try std.testing.expect(hasQueryParam("uploadId=abc&partNumber=1", "uploadId"));
    try std.testing.expect(!hasQueryParam("foo=bar", "uploads"));
    try std.testing.expect(!hasQueryParam("", "uploads"));
    try std.testing.expect(hasQueryParam("list-type=2&prefix=foo", "list-type"));
    try std.testing.expect(hasQueryParam("acl", "acl"));
    try std.testing.expect(hasQueryParam("delete", "delete"));
    try std.testing.expect(hasQueryParam("location", "location"));
}

test "generateRequestId returns 16 hex chars" {
    const id = generateRequestId();
    try std.testing.expectEqual(@as(usize, 16), id.len);
    for (id) |ch| {
        try std.testing.expect((ch >= '0' and ch <= '9') or (ch >= 'A' and ch <= 'F'));
    }
}

test "getQueryParamValue" {
    try std.testing.expectEqualStrings("2", getQueryParamValue("list-type=2&prefix=foo", "list-type").?);
    try std.testing.expectEqualStrings("foo", getQueryParamValue("list-type=2&prefix=foo", "prefix").?);
    try std.testing.expect(getQueryParamValue("acl", "acl") == null); // no value
    try std.testing.expect(getQueryParamValue("", "key") == null);
    try std.testing.expectEqualStrings("abc-123", getQueryParamValue("uploadId=abc-123&partNumber=1", "uploadId").?);
    try std.testing.expectEqualStrings("1", getQueryParamValue("uploadId=abc-123&partNumber=1", "partNumber").?);
}

test "formatRfc1123Date returns 29 chars" {
    var buf: [29]u8 = undefined;
    const date = formatRfc1123Date(&buf);
    try std.testing.expectEqual(@as(usize, 29), date.len);
    // Should end with "GMT"
    try std.testing.expectEqualStrings("GMT", date[26..29]);
}
