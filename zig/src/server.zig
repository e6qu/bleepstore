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

/// Global auth cache for signing keys and credentials (set in main.zig).
pub var global_auth_cache: ?*auth_mod.AuthCache = null;

/// Global max object size (5 GiB default, configurable via server.max_object_size).
pub var global_max_object_size: u64 = 5368709120;

/// Global observability flags (set from config in main.zig).
pub var global_metrics_enabled: bool = true;
pub var global_health_check_enabled: bool = true;

/// Canonical OpenAPI spec embedded from schemas/s3-api.openapi.json (via symlink).
const canonical_spec = @embedFile("s3-api.openapi.json");

/// Patched OpenAPI spec with servers array replaced at startup.
/// Points to a buffer allocated during Server.init() that lives for the
/// process lifetime (never freed -- crash-only design).
pub var global_openapi_json: []const u8 = canonical_spec;

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
    .get("/healthz", handleHealthz),
    .get("/readyz", handleReadyz),
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
    if (global_metrics_enabled) metrics_mod.incrementHttpRequests();

    const start_us = getTimestampUs();

    res.content_type = .JSON;
    const request_id = generateRequestId();
    setCommonHeaders(res, &request_id);

    if (!global_health_check_enabled) {
        // Disabled: return static response, no deep checks.
        res.status = 200;
        res.body = "{\"status\":\"ok\"}";
        observeHandlerMetrics(start_us, 0, @as(usize, "{\"status\":\"ok\"}".len));
        return;
    }

    // Deep health check: probe metadata and storage.
    var all_ok = true;

    // Metadata check
    var meta_status: []const u8 = "ok";
    const meta_start = getTimestampUs();
    if (global_metadata_store) |ms| {
        _ = ms.countBuckets() catch {
            meta_status = "error";
            all_ok = false;
        };
    }
    const meta_latency_ms = (getTimestampUs() - meta_start) / 1000;

    // Storage check
    var storage_status: []const u8 = "ok";
    const storage_start = getTimestampUs();
    if (global_storage_backend) |sb| {
        sb.healthCheck() catch {
            storage_status = "error";
            all_ok = false;
        };
    }
    const storage_latency_ms = (getTimestampUs() - storage_start) / 1000;

    const overall_status: []const u8 = if (all_ok) "ok" else "degraded";
    res.status = if (all_ok) 200 else 503;

    const body = std.fmt.allocPrint(res.arena,
        "{{\"status\":\"{s}\",\"checks\":{{\"metadata\":{{\"status\":\"{s}\",\"latency_ms\":{d}}},\"storage\":{{\"status\":\"{s}\",\"latency_ms\":{d}}}}}}}", .{
        overall_status,
        meta_status,
        meta_latency_ms,
        storage_status,
        storage_latency_ms,
    }) catch "{\"status\":\"ok\"}";
    res.body = body;

    observeHandlerMetrics(start_us, 0, body.len);
}

fn handleHealthz(res: *tk.Response) void {
    if (global_metrics_enabled) metrics_mod.incrementHttpRequests();
    const start_us = getTimestampUs();

    if (!global_health_check_enabled) {
        res.status = 404;
        res.body = "";
        observeHandlerMetrics(start_us, 0, 0);
        return;
    }

    res.status = 200;
    res.body = "";
    observeHandlerMetrics(start_us, 0, 0);
}

fn handleReadyz(res: *tk.Response) void {
    if (global_metrics_enabled) metrics_mod.incrementHttpRequests();
    const start_us = getTimestampUs();

    if (!global_health_check_enabled) {
        res.status = 404;
        res.body = "";
        observeHandlerMetrics(start_us, 0, 0);
        return;
    }

    // Probe metadata store.
    if (global_metadata_store) |ms| {
        _ = ms.countBuckets() catch {
            res.status = 503;
            res.body = "";
            observeHandlerMetrics(start_us, 0, 0);
            return;
        };
    }

    // Probe storage backend.
    if (global_storage_backend) |sb| {
        sb.healthCheck() catch {
            res.status = 503;
            res.body = "";
            observeHandlerMetrics(start_us, 0, 0);
            return;
        };
    }

    res.status = 200;
    res.body = "";
    observeHandlerMetrics(start_us, 0, 0);
}

fn handleMetrics(res: *tk.Response) void {
    // The metrics endpoint itself is NOT counted in HTTP metrics (per spec).
    if (!global_metrics_enabled) {
        res.status = 404;
        res.body = "";
        return;
    }

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
    if (global_metrics_enabled) metrics_mod.incrementHttpRequests();
    const start_us = getTimestampUs();
    res.status = 200;
    res.content_type = .HTML;
    res.body = swagger_ui_html;
    observeHandlerMetrics(start_us, 0, swagger_ui_html.len);
}

fn handleOpenApiJson(res: *tk.Response) void {
    if (global_metrics_enabled) metrics_mod.incrementHttpRequests();
    const start_us = getTimestampUs();
    res.status = 200;
    res.content_type = .JSON;
    res.body = global_openapi_json;
    observeHandlerMetrics(start_us, 0, global_openapi_json.len);
}

/// Get current timestamp in microseconds for duration measurement.
fn getTimestampUs() u64 {
    const nanos = std.time.nanoTimestamp();
    const safe_nanos: u64 = @intCast(if (nanos < 0) 0 else nanos);
    return safe_nanos / 1000;
}

/// Observe duration and size metrics for a handler. No-op when metrics are disabled.
fn observeHandlerMetrics(start_us: u64, request_size: usize, response_size: usize) void {
    if (!global_metrics_enabled) return;
    const end_us = getTimestampUs();
    const elapsed = if (end_us >= start_us) end_us - start_us else 0;
    metrics_mod.observeDuration(elapsed);
    metrics_mod.observeRequestSize(@intCast(request_size));
    metrics_mod.observeResponseSize(@intCast(response_size));
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

    // Handle Expect: 100-continue. boto3 sends this on PUT/POST requests.
    // We use lazy_read_size=1 so httpz doesn't block reading the body before
    // calling us. Send "100 Continue" first, then read the body ourselves.
    if (req.header("expect")) |expect_val| {
        if (std.mem.eql(u8, expect_val, "100-continue")) {
            res.conn.writeAll("HTTP/1.1 100 Continue\r\n\r\n") catch {};
        }
    }

    // With lazy_read_size enabled, httpz defers body reading. Read the full
    // body now (after 100-continue was sent) so req.body() works for handlers.
    if (req.unread_body > 0) {
        const content_length = req.body_len;
        const req_alloc_body = res.arena;
        const full_buf = try req_alloc_body.alloc(u8, content_length);

        // Copy any partial body already received with headers.
        var pos: usize = 0;
        if (req.body_buffer) |bb| {
            const partial = bb.data[0 .. content_length - req.unread_body];
            @memcpy(full_buf[0..partial.len], partial);
            pos = partial.len;
        }

        // Ensure socket is in blocking mode (required for lazy_read).
        try req.conn.blockingMode();

        // Read remaining bytes from the socket.
        const socket = req.conn.stream.handle;
        while (pos < content_length) {
            const n = std.posix.read(socket, full_buf[pos..content_length]) catch |err| {
                std.log.err("body read error: {}", .{err});
                return err;
            };
            if (n == 0) break;
            pos += n;
        }

        // Update request so req.body() returns the full body.
        req.body_buffer = .{ .type = .static, .data = full_buf };
        req.body_len = pos;
        req.unread_body = 0;
    }

    // Start timing for metrics.
    const s3_start_us = getTimestampUs();

    // Increment HTTP request counter for S3 routes.
    if (global_metrics_enabled) metrics_mod.incrementHttpRequests();

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

    // Observe S3 request metrics: duration and sizes.
    if (global_metrics_enabled) {
        const req_body_len: usize = if (req.body()) |b| b.len else 0;
        const res_body_len: usize = res.body.len;
        observeHandlerMetrics(s3_start_us, req_body_len, res_body_len);
    }
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

    // Look up credentials — try cache first, then DB.
    const secret_key: []const u8 = blk: {
        if (global_auth_cache) |cache| {
            if (cache.getCredential(access_key_id, req_alloc)) |snap| {
                break :blk snap.secret_key; // duped to arena by getCredential
            }
        }
        // Cache miss — query the metadata store.
        const cred_opt = ms.getCredential(access_key_id) catch {
            return error.AccessDenied;
        };
        const cred = cred_opt orelse return error.InvalidAccessKeyId;

        // Copy the secret key to the arena allocator (lives for the request duration).
        const sk = try req_alloc.dupe(u8, cred.secret_key);

        // Populate cache before freeing GPA strings.
        if (global_auth_cache) |cache| {
            cache.putCredential(
                cred.access_key_id,
                cred.secret_key,
                cred.owner_id,
                cred.display_name,
                cred.created_at,
            );
        }

        // Free all GPA-allocated credential fields.
        if (global_allocator) |gpa| {
            gpa.free(cred.access_key_id);
            gpa.free(cred.secret_key);
            gpa.free(cred.owner_id);
            gpa.free(cred.display_name);
            gpa.free(cred.created_at);
        }
        break :blk sk;
    };

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

    // Extract auth components for signing key cache lookup.
    var date_stamp: []const u8 = "";
    var region_for_cache: []const u8 = global_region;
    var service_for_cache: []const u8 = "s3";

    switch (auth_type) {
        .header => {
            if (auth_mod.parseAuthorizationHeader(auth_header.?)) |components| {
                date_stamp = components.date_stamp;
                region_for_cache = components.region;
                service_for_cache = components.service;
            }
        },
        .presigned => {
            if (auth_mod.parsePresignedParams(query)) |ps| {
                date_stamp = ps.date_stamp;
                region_for_cache = ps.region;
                service_for_cache = ps.service;
            }
        },
        .none => unreachable,
    }

    // Try signing key cache.
    var cached_key_buf: [32]u8 = undefined;
    var precomputed_key: ?*const [32]u8 = null;
    if (global_auth_cache) |cache| {
        if (cache.getSigningKey(secret_key, date_stamp, region_for_cache, service_for_cache)) |k| {
            cached_key_buf = k;
            precomputed_key = &cached_key_buf;
        }
    }

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
                precomputed_key,
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
                precomputed_key,
            ) catch |err| {
                return err;
            };
        },
        .none => unreachable,
    }

    // On successful verification, cache the signing key for next request.
    if (precomputed_key == null and date_stamp.len > 0) {
        if (global_auth_cache) |cache| {
            const derived = auth_mod.deriveSigningKey(secret_key, date_stamp, region_for_cache, service_for_cache);
            cache.putSigningKey(secret_key, date_stamp, region_for_cache, service_for_cache, derived);
        }
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

        // Patch the canonical OpenAPI spec's "servers" array to point at this
        // instance's actual host:port.  We do simple string surgery: find the
        // byte range of the "servers":[...] value and splice in our replacement.
        patchOpenApiServers(allocator, cfg.server.port);

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
                // Defer body reading so we can send "100 Continue" before
                // the client sends the body. Without this, httpz blocks on
                // socket read while the client waits for 100 Continue,
                // causing a ~1s delay (boto3's Expect timeout).
                .lazy_read_size = 1,
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

/// Send an S3 error response with a custom message (overriding the default).
pub fn sendS3ErrorWithMessage(
    res: *tk.Response,
    alloc: std.mem.Allocator,
    s3error: s3err.S3Error,
    custom_message: []const u8,
    resource: []const u8,
    request_id: *const [16]u8,
) !void {
    const body = try xml.renderError(
        alloc,
        s3error.code(),
        custom_message,
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

    // Generate x-amz-id-2: Base64-encoded 24 random bytes (32 base64 chars).
    var id2_raw: [24]u8 = undefined;
    std.crypto.random.bytes(&id2_raw);
    var id2_b64: [std.base64.standard.Encoder.calcSize(24)]u8 = undefined;
    const id2_encoded = std.base64.standard.Encoder.encode(&id2_b64, &id2_raw);
    const id2_str = std.fmt.allocPrint(res.arena, "{s}", .{id2_encoded}) catch "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    res.header("x-amz-id-2", id2_str);

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

/// Patch the embedded canonical OpenAPI spec's "servers" array to reflect
/// this instance's actual port.  Uses simple string surgery on the JSON text:
/// find the `"servers": [...]` byte range and splice in a replacement array
/// pointing at `http://localhost:{port}`.
///
/// The patched buffer is allocated with `allocator` and stored in the global
/// `global_openapi_json`.  It is never freed (crash-only design).
fn patchOpenApiServers(allocator: std.mem.Allocator, port: u16) void {
    // Locate the "servers" key in the canonical JSON.
    const servers_key = "\"servers\":";
    const servers_key_spaced = "\"servers\": ";
    const spec = canonical_spec;

    // Find the start of the "servers" key (try both with and without space).
    const key_pos = std.mem.indexOf(u8, spec, servers_key_spaced) orelse
        std.mem.indexOf(u8, spec, servers_key) orelse {
        // If not found, serve the canonical spec unpatched.
        std.log.warn("OpenAPI spec: could not find \"servers\" key, serving unpatched", .{});
        return;
    };

    // Find the opening '[' after the key.
    const after_key = if (std.mem.indexOf(u8, spec, servers_key_spaced) != null)
        key_pos + servers_key_spaced.len
    else
        key_pos + servers_key.len;

    // Skip whitespace to find '['.
    var bracket_start = after_key;
    while (bracket_start < spec.len and (spec[bracket_start] == ' ' or spec[bracket_start] == '\n' or spec[bracket_start] == '\r' or spec[bracket_start] == '\t')) {
        bracket_start += 1;
    }
    if (bracket_start >= spec.len or spec[bracket_start] != '[') {
        std.log.warn("OpenAPI spec: expected '[' after \"servers\" key, serving unpatched", .{});
        return;
    }

    // Find the matching ']' (simple bracket counting -- no nested arrays expected).
    var depth: usize = 0;
    var bracket_end: usize = bracket_start;
    var in_string = false;
    var escaped = false;
    while (bracket_end < spec.len) {
        const ch = spec[bracket_end];
        if (escaped) {
            escaped = false;
        } else if (ch == '\\' and in_string) {
            escaped = true;
        } else if (ch == '"') {
            in_string = !in_string;
        } else if (!in_string) {
            if (ch == '[') depth += 1;
            if (ch == ']') {
                depth -= 1;
                if (depth == 0) {
                    bracket_end += 1; // include the ']'
                    break;
                }
            }
        }
        bracket_end += 1;
    }

    // Build the replacement servers array.
    var port_buf: [5]u8 = undefined;
    const port_str = std.fmt.bufPrint(&port_buf, "{d}", .{port}) catch "9013";
    const replacement_prefix = "[{\"url\":\"http://localhost:";
    const replacement_suffix = "\",\"description\":\"BleepStore Zig\"}]";

    const prefix = spec[0..key_pos];
    // Re-emit the key exactly as found.
    const key_text = spec[key_pos..after_key];
    const suffix = spec[bracket_end..];

    const total_len = prefix.len + key_text.len + replacement_prefix.len + port_str.len + replacement_suffix.len + suffix.len;
    const buf = allocator.alloc(u8, total_len) catch {
        std.log.warn("OpenAPI spec: allocation failed, serving unpatched", .{});
        return;
    };

    var offset: usize = 0;
    @memcpy(buf[offset .. offset + prefix.len], prefix);
    offset += prefix.len;
    @memcpy(buf[offset .. offset + key_text.len], key_text);
    offset += key_text.len;
    @memcpy(buf[offset .. offset + replacement_prefix.len], replacement_prefix);
    offset += replacement_prefix.len;
    @memcpy(buf[offset .. offset + port_str.len], port_str);
    offset += port_str.len;
    @memcpy(buf[offset .. offset + replacement_suffix.len], replacement_suffix);
    offset += replacement_suffix.len;
    @memcpy(buf[offset .. offset + suffix.len], suffix);

    global_openapi_json = buf;
    std.log.info("OpenAPI spec patched: serving canonical spec on port {d} ({d} bytes)", .{ port, total_len });
}

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

test "openapi spec matches canonical" {
    const embedded = @embedFile("s3-api.openapi.json");
    // The embedded file IS the canonical spec (via symlink), so parsing should succeed.
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, embedded, .{});
    defer parsed.deinit();

    // Verify it's valid JSON with expected top-level keys.
    const root = parsed.value.object;
    try std.testing.expect(root.contains("openapi"));
    try std.testing.expect(root.contains("info"));
    try std.testing.expect(root.contains("paths"));
    try std.testing.expect(root.contains("components"));
    try std.testing.expect(root.contains("security"));
    try std.testing.expect(root.contains("tags"));

    // Verify the openapi version.
    const version = root.get("openapi").?.string;
    try std.testing.expectEqualStrings("3.1.0", version);

    // Verify title.
    const info = root.get("info").?.object;
    const title = info.get("title").?.string;
    try std.testing.expectEqualStrings("BleepStore S3-Compatible API", title);
}

test "patchOpenApiServers produces valid JSON with correct port" {
    // Use the testing allocator so leaks are detected.
    const allocator = std.testing.allocator;

    // Save and restore the global to avoid polluting other tests.
    const saved = global_openapi_json;
    defer global_openapi_json = saved;

    // Patch to port 9999.
    patchOpenApiServers(allocator, 9999);

    // The patched spec should be valid JSON.
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, global_openapi_json, .{});
    defer parsed.deinit();

    // Verify servers array was patched.
    const root = parsed.value.object;
    const servers = root.get("servers").?.array;
    try std.testing.expectEqual(@as(usize, 1), servers.items.len);

    const server_obj = servers.items[0].object;
    const url = server_obj.get("url").?.string;
    try std.testing.expectEqualStrings("http://localhost:9999", url);

    const desc = server_obj.get("description").?.string;
    try std.testing.expectEqualStrings("BleepStore Zig", desc);

    // The rest of the spec should still be intact.
    try std.testing.expect(root.contains("paths"));
    try std.testing.expect(root.contains("openapi"));

    // Free the patched buffer (testing allocator requires it).
    allocator.free(global_openapi_json);
    global_openapi_json = saved;
}
