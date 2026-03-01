const std = @import("std");
const config_mod = @import("config.zig");
const server_mod = @import("server.zig");
const metrics_mod = @import("metrics.zig");
const SqliteMetadataStore = @import("metadata/sqlite.zig").SqliteMetadataStore;
const MemoryMetadataStore = @import("metadata/memory.zig").MemoryMetadataStore;
const LocalStore = @import("metadata/local.zig").LocalStore;
const DynamoDBMetadataStore = @import("metadata/dynamodb.zig").DynamoDBMetadataStore;
const FirestoreMetadataStore = @import("metadata/firestore.zig").FirestoreMetadataStore;
const CosmosMetadataStore = @import("metadata/cosmos.zig").CosmosMetadataStore;
const MetadataStore = @import("metadata/store.zig").MetadataStore;
const LocalBackend = @import("storage/local.zig").LocalBackend;
const MemoryBackend = @import("storage/memory.zig").MemoryBackend;
const SqliteBackend = @import("storage/sqlite_backend.zig").SqliteBackend;
const AwsGatewayBackend = @import("storage/aws.zig").AwsGatewayBackend;
const GcpGatewayBackend = @import("storage/gcp.zig").GcpGatewayBackend;
const AzureGatewayBackend = @import("storage/azure.zig").AzureGatewayBackend;
const StorageBackend = @import("storage/backend.zig").StorageBackend;

pub const handlers = struct {
    pub const bucket = @import("handlers/bucket.zig");
    pub const object = @import("handlers/object.zig");
    pub const multipart = @import("handlers/multipart.zig");
};

pub const metadata = struct {
    pub const store = @import("metadata/store.zig");
    pub const sqlite = @import("metadata/sqlite.zig");
    pub const memory = @import("metadata/memory.zig");
    pub const local = @import("metadata/local.zig");
    pub const dynamodb = @import("metadata/dynamodb.zig");
    pub const firestore = @import("metadata/firestore.zig");
    pub const cosmos = @import("metadata/cosmos.zig");
};

pub const storage = struct {
    pub const backend = @import("storage/backend.zig");
    pub const local = @import("storage/local.zig");
    pub const memory = @import("storage/memory.zig");
    pub const sqlite_backend = @import("storage/sqlite_backend.zig");
    pub const aws = @import("storage/aws.zig");
    pub const gcp = @import("storage/gcp.zig");
    pub const azure = @import("storage/azure.zig");
};

pub const cluster = struct {
    pub const raft = @import("cluster/raft.zig");
};

pub const auth = @import("auth.zig");
const auth_mod = @import("auth.zig");
pub const xml = @import("xml.zig");
pub const errors = @import("errors.zig");
pub const validation = @import("validation.zig");
pub const metrics = @import("metrics.zig");

// ---------------------------------------------------------------------------
// Runtime logging configuration
// ---------------------------------------------------------------------------

var runtime_log_level: std.log.Level = .info;
var runtime_log_json: bool = false;

/// Override std_options to allow all log levels at comptime and use our custom logFn.
pub const std_options: std.Options = .{
    .log_level = .debug, // compile-time max — runtime filtering in customLogFn
    .logFn = customLogFn,
};

fn customLogFn(
    comptime level: std.log.Level,
    comptime scope: @TypeOf(.enum_literal),
    comptime format: []const u8,
    args: anytype,
) void {
    // Runtime level filter.
    if (@intFromEnum(level) > @intFromEnum(runtime_log_level)) return;

    if (runtime_log_json) {
        // JSON format: {"level":"info","scope":"default","msg":"...","ts":epoch_s}
        const level_str = comptime level.asText();
        const scope_str = if (scope == .default) "default" else @tagName(scope);
        const now = std.time.timestamp();

        // Format the message into a stack buffer.
        var msg_buf: [4096]u8 = undefined;
        const msg = std.fmt.bufPrint(&msg_buf, format, args) catch "(message too long)";

        // Use lockStderrWriter (Zig 0.15 API).
        var lock_buf: [64]u8 = undefined;
        const stderr = std.debug.lockStderrWriter(&lock_buf);
        defer std.debug.unlockStderrWriter();

        // Escape any quotes/backslashes in message for valid JSON.
        nosuspend stderr.print("{{\"level\":\"{s}\",\"scope\":\"{s}\",\"ts\":{d},\"msg\":\"", .{ level_str, scope_str, now }) catch return;
        for (msg) |ch| {
            nosuspend switch (ch) {
                '"' => stderr.writeAll("\\\"") catch return,
                '\\' => stderr.writeAll("\\\\") catch return,
                '\n' => stderr.writeAll("\\n") catch return,
                '\r' => stderr.writeAll("\\r") catch return,
                '\t' => stderr.writeAll("\\t") catch return,
                else => stderr.writeByte(ch) catch return,
            };
        }
        nosuspend stderr.writeAll("\"}\n") catch return;
    } else {
        // Default text format (matches std.log.defaultLog).
        std.log.defaultLog(level, scope, format, args);
    }
}

fn parseLogLevel(level_str: []const u8) std.log.Level {
    if (std.mem.eql(u8, level_str, "debug")) return .debug;
    if (std.mem.eql(u8, level_str, "info")) return .info;
    if (std.mem.eql(u8, level_str, "warn") or std.mem.eql(u8, level_str, "warning")) return .warn;
    if (std.mem.eql(u8, level_str, "err") or std.mem.eql(u8, level_str, "error")) return .err;
    return .info;
}

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

const CliArgs = struct {
    config_path: []const u8 = "bleepstore.yaml",
    port: ?u16 = null,
    host: ?[]const u8 = null,
    log_level: ?[]const u8 = null,
    log_format: ?[]const u8 = null,
    shutdown_timeout: ?u64 = null,
    max_object_size: ?u64 = null,
};

fn parseArgs(allocator: std.mem.Allocator) !CliArgs {
    var args_iter = try std.process.argsWithAllocator(allocator);
    defer args_iter.deinit();

    // Skip the program name.
    _ = args_iter.next();

    var cli = CliArgs{};

    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--config")) {
            cli.config_path = args_iter.next() orelse {
                std.log.err("--config requires a value", .{});
                return error.InvalidArgument;
            };
        } else if (std.mem.eql(u8, arg, "--port")) {
            const port_str = args_iter.next() orelse {
                std.log.err("--port requires a value", .{});
                return error.InvalidArgument;
            };
            cli.port = std.fmt.parseInt(u16, port_str, 10) catch {
                std.log.err("invalid port: {s}", .{port_str});
                return error.InvalidArgument;
            };
        } else if (std.mem.eql(u8, arg, "--host")) {
            cli.host = args_iter.next() orelse {
                std.log.err("--host requires a value", .{});
                return error.InvalidArgument;
            };
        } else if (std.mem.eql(u8, arg, "--log-level")) {
            cli.log_level = args_iter.next() orelse {
                std.log.err("--log-level requires a value", .{});
                return error.InvalidArgument;
            };
        } else if (std.mem.eql(u8, arg, "--log-format")) {
            cli.log_format = args_iter.next() orelse {
                std.log.err("--log-format requires a value", .{});
                return error.InvalidArgument;
            };
        } else if (std.mem.eql(u8, arg, "--shutdown-timeout")) {
            const val = args_iter.next() orelse {
                std.log.err("--shutdown-timeout requires a value", .{});
                return error.InvalidArgument;
            };
            cli.shutdown_timeout = std.fmt.parseInt(u64, val, 10) catch {
                std.log.err("invalid shutdown-timeout: {s}", .{val});
                return error.InvalidArgument;
            };
        } else if (std.mem.eql(u8, arg, "--max-object-size")) {
            const val = args_iter.next() orelse {
                std.log.err("--max-object-size requires a value", .{});
                return error.InvalidArgument;
            };
            cli.max_object_size = std.fmt.parseInt(u64, val, 10) catch {
                std.log.err("invalid max-object-size: {s}", .{val});
                return error.InvalidArgument;
            };
        } else {
            std.log.warn("unknown argument: {s}", .{arg});
        }
    }

    return cli;
}

/// Install SIGTERM and SIGINT handlers to request graceful shutdown.
/// Crash-only design: handlers only set a flag to stop accepting connections.
/// No cleanup is performed -- the next startup is always recovery.
fn installSignalHandlers() void {
    // Only available on POSIX systems (Linux, macOS)
    if (comptime @hasDecl(std.posix, "Sigaction")) {
        const handler_fn = struct {
            fn handler(_: c_int) callconv(.c) void {
                server_mod.shutdown_requested.store(true, .release);
            }
        }.handler;

        const sa = std.posix.Sigaction{
            .handler = .{ .handler = handler_fn },
            .mask = std.mem.zeroes(std.posix.sigset_t),
            .flags = 0,
        };

        std.posix.sigaction(std.posix.SIG.TERM, &sa, null);
        std.posix.sigaction(std.posix.SIG.INT, &sa, null);
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const cli = parseArgs(allocator) catch |err| {
        std.log.err("failed to parse arguments: {}", .{err});
        std.process.exit(1);
    };

    std.log.info("BleepStore v0.1.0 (Zig)", .{});

    // --- Crash-only startup: every startup IS recovery ---
    // Step 1: Load configuration
    std.log.info("loading config from: {s}", .{cli.config_path});
    var cfg = config_mod.loadConfig(allocator, cli.config_path) catch |err| blk: {
        std.log.warn("could not load config file ({s}): {}, using defaults", .{ cli.config_path, err });
        break :blk config_mod.Config.default();
    };
    defer cfg.deinit(allocator);

    // CLI args override config file values
    if (cli.port) |p| cfg.server.port = p;
    if (cli.host) |h| cfg.server.host = h;
    if (cli.log_level) |l| cfg.logging.level = l;
    if (cli.log_format) |f| cfg.logging.format = f;
    if (cli.shutdown_timeout) |t| cfg.server.shutdown_timeout = t;
    if (cli.max_object_size) |s| cfg.server.max_object_size = s;

    // Apply runtime log settings from config.
    runtime_log_level = parseLogLevel(cfg.logging.level);
    runtime_log_json = std.mem.eql(u8, cfg.logging.format, "json");

    // Set global max object size for handler access.
    server_mod.global_max_object_size = cfg.server.max_object_size;

    // Set observability flags from config.
    server_mod.global_metrics_enabled = cfg.observability.metrics;
    server_mod.global_health_check_enabled = cfg.observability.health_check;

    // Step 2: Initialize metrics (record start time) — only when metrics enabled.
    if (cfg.observability.metrics) {
        metrics_mod.initMetrics();
        std.log.info("metrics initialized", .{});
    } else {
        std.log.info("metrics disabled by config", .{});
    }

    // Step 3: Initialize SQLite metadata store
    // Crash-only: SQLite WAL mode auto-recovers from crashes.
    // Ensure the data directory exists for the SQLite database.
    ensureDataDir(cfg.metadata.sqlite_path);

    // Convert the sqlite path to a null-terminated string for the C API.
    const db_path_z = try allocator.dupeZ(u8, cfg.metadata.sqlite_path);
    defer allocator.free(db_path_z);

    var metadata_store = SqliteMetadataStore.init(allocator, db_path_z) catch |err| {
        std.log.err("failed to initialize SQLite metadata store at '{s}': {}", .{ cfg.metadata.sqlite_path, err });
        std.process.exit(1);
    };
    defer metadata_store.deinit();

    std.log.info("SQLite metadata store initialized at: {s}", .{cfg.metadata.sqlite_path});

    // Step 4: Seed default credentials from config (crash-only: idempotent INSERT OR IGNORE)
    metadata_store.seedCredentials(cfg.auth.access_key, cfg.auth.secret_key) catch |err| {
        std.log.warn("failed to seed credentials: {}", .{err});
    };
    std.log.info("credentials seeded for access key: {s}", .{cfg.auth.access_key});

    // Step 5: Initialize storage backend based on configuration.
    // Crash-only: LocalBackend.init cleans stale temp files on startup.
    var local_backend: ?LocalBackend = null;
    var memory_backend: ?MemoryBackend = null;
    var sqlite_storage_backend: ?SqliteBackend = null;
    var aws_backend: ?AwsGatewayBackend = null;
    var gcp_backend: ?GcpGatewayBackend = null;
    var azure_backend: ?AzureGatewayBackend = null;
    var storage_backend: StorageBackend = undefined;

    switch (cfg.storage.backend) {
        .local => {
            local_backend = LocalBackend.init(allocator, cfg.storage.local_root) catch |err| {
                std.log.err("failed to initialize local storage backend at '{s}': {}", .{ cfg.storage.local_root, err });
                std.process.exit(1);
            };
            storage_backend = local_backend.?.storageBackend();
            std.log.info("local storage backend initialized at: {s}", .{cfg.storage.local_root});
        },
        .aws => {
            // Resolve AWS credentials: config file > environment variables.
            const aws_key = if (cfg.storage.aws_access_key_id.len > 0)
                cfg.storage.aws_access_key_id
            else
                std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch {
                    std.log.err("AWS backend requires AWS_ACCESS_KEY_ID (config or env var)", .{});
                    std.process.exit(1);
                };
            const aws_secret = if (cfg.storage.aws_secret_access_key.len > 0)
                cfg.storage.aws_secret_access_key
            else
                std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch {
                    std.log.err("AWS backend requires AWS_SECRET_ACCESS_KEY (config or env var)", .{});
                    std.process.exit(1);
                };
            const aws_region = if (cfg.storage.aws_region.len > 0)
                cfg.storage.aws_region
            else blk: {
                break :blk std.process.getEnvVarOwned(allocator, "AWS_REGION") catch "us-east-1";
            };

            if (cfg.storage.aws_bucket.len == 0) {
                std.log.err("AWS backend requires storage.aws.bucket in config", .{});
                std.process.exit(1);
            }

            aws_backend = AwsGatewayBackend.init(
                allocator,
                aws_region,
                cfg.storage.aws_bucket,
                cfg.storage.aws_prefix,
                aws_key,
                aws_secret,
                cfg.storage.aws_endpoint_url,
            ) catch |err| {
                std.log.err("failed to initialize AWS gateway backend: {}", .{err});
                std.process.exit(1);
            };
            storage_backend = aws_backend.?.storageBackend();
            std.log.info("AWS gateway backend initialized: bucket={s} region={s}", .{
                cfg.storage.aws_bucket, aws_region,
            });
        },
        .gcp => {
            if (cfg.storage.gcp_bucket.len == 0) {
                std.log.err("GCP backend requires storage.gcp.bucket in config", .{});
                std.process.exit(1);
            }

            gcp_backend = GcpGatewayBackend.init(
                allocator,
                cfg.storage.gcp_bucket,
                cfg.storage.gcp_project,
                cfg.storage.gcp_prefix,
                cfg.storage.gcp_credentials_file,
            ) catch |err| {
                std.log.err("failed to initialize GCP gateway backend: {}", .{err});
                std.process.exit(1);
            };
            storage_backend = gcp_backend.?.storageBackend();
            std.log.info("GCP gateway backend initialized: bucket={s} project={s}", .{
                cfg.storage.gcp_bucket, cfg.storage.gcp_project,
            });
        },
        .azure => {
            if (cfg.storage.azure_container.len == 0) {
                std.log.err("Azure backend requires storage.azure.container in config", .{});
                std.process.exit(1);
            }
            if (cfg.storage.azure_account.len == 0) {
                std.log.err("Azure backend requires storage.azure.account in config", .{});
                std.process.exit(1);
            }

            azure_backend = AzureGatewayBackend.init(
                allocator,
                cfg.storage.azure_container,
                cfg.storage.azure_account,
                cfg.storage.azure_prefix,
                cfg.storage.azure_connection_string,
            ) catch |err| {
                std.log.err("failed to initialize Azure gateway backend: {}", .{err});
                std.process.exit(1);
            };
            storage_backend = azure_backend.?.storageBackend();
            std.log.info("Azure gateway backend initialized: container={s} account={s}", .{
                cfg.storage.azure_container, cfg.storage.azure_account,
            });
        },
        .memory => {
            memory_backend = MemoryBackend.init(allocator, cfg.storage.memory_max_size_bytes) catch |err| {
                std.log.err("failed to initialize memory storage backend: {}", .{err});
                std.process.exit(1);
            };
            storage_backend = memory_backend.?.storageBackend();
            std.log.info("memory storage backend initialized (max_size_bytes={})", .{cfg.storage.memory_max_size_bytes});
        },
        .sqlite => {
            sqlite_storage_backend = SqliteBackend.init(allocator, cfg.metadata.sqlite_path) catch |err| {
                std.log.err("failed to initialize SQLite storage backend: {}", .{err});
                std.process.exit(1);
            };
            storage_backend = sqlite_storage_backend.?.storageBackend();
            std.log.info("SQLite storage backend initialized at: {s}", .{cfg.metadata.sqlite_path});
        },
    }
    defer {
        if (local_backend) |*lb| lb.deinit();
        if (memory_backend) |*mb| mb.deinit();
        if (sqlite_storage_backend) |*sb| sb.deinit();
        if (aws_backend) |*ab| ab.deinit();
        if (gcp_backend) |*gb| gb.deinit();
        if (azure_backend) |*azb| azb.deinit();
    }

    // Step 6: Initialize auth cache for signing key and credential caching.
    var auth_cache = auth_mod.AuthCache.init(allocator);
    defer auth_cache.deinit();
    server_mod.global_auth_cache = &auth_cache;

    // Step 7: Reap expired multipart uploads (crash-only startup recovery).
    // TTL = 604800 seconds (7 days).
    const reaped = metadata_store.reapExpiredUploads(604800) catch 0;
    if (reaped > 0) {
        std.log.info("reaped {d} expired multipart uploads", .{reaped});
    }

    // Step 8: Update metrics gauges from metadata store (only when metrics enabled)
    if (cfg.observability.metrics) {
        if (metadata_store.metadataStore().countBuckets()) |count| {
            metrics_mod.setBucketsTotal(count);
        } else |_| {}
        if (metadata_store.metadataStore().countObjects()) |count| {
            metrics_mod.setObjectsTotal(count);
        } else |_| {}
    }

    // Install signal handlers (SIGTERM/SIGINT -> stop accepting, exit)
    installSignalHandlers();

    // Spawn shutdown timeout watchdog thread: waits for shutdown_requested,
    // then enforces a hard exit after shutdown_timeout seconds.
    const shutdown_timeout_secs = cfg.server.shutdown_timeout;
    const watchdog = std.Thread.spawn(.{}, shutdownWatchdog, .{shutdown_timeout_secs}) catch null;
    if (watchdog) |t| t.detach();

    std.log.info("starting server on {s}:{d} (region: {s})", .{
        cfg.server.host,
        cfg.server.port,
        cfg.server.region,
    });

    var srv = server_mod.Server.init(allocator, cfg, &metadata_store, storage_backend);
    defer srv.deinit();

    srv.run() catch |err| {
        std.log.err("server error: {}", .{err});
        std.process.exit(1);
    };
}

/// Watchdog thread: polls shutdown_requested, then enforces hard exit.
fn shutdownWatchdog(timeout_secs: u64) void {
    // Poll until shutdown is requested (100ms intervals).
    while (!server_mod.shutdown_requested.load(.acquire)) {
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }
    // Shutdown requested — allow graceful drain for timeout_secs, then hard exit.
    std.log.info("shutdown requested, allowing {d}s for graceful drain", .{timeout_secs});
    std.Thread.sleep(timeout_secs * std.time.ns_per_s);
    std.log.warn("shutdown timeout expired, forcing exit", .{});
    std.process.exit(1);
}

/// Ensure the parent directory for a given file path exists.
fn ensureDataDir(path: []const u8) void {
    if (std.fs.path.dirname(path)) |dir| {
        std.fs.cwd().makePath(dir) catch |err| {
            if (err != error.PathAlreadyExists) {
                std.log.warn("could not create data directory '{s}': {}", .{ dir, err });
            }
        };
    }
}

test {
    // Pull in all module tests.
    @import("std").testing.refAllDeclsRecursive(@This());
}
