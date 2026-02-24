const std = @import("std");
const config_mod = @import("config.zig");
const server_mod = @import("server.zig");
const metrics_mod = @import("metrics.zig");
const SqliteMetadataStore = @import("metadata/sqlite.zig").SqliteMetadataStore;
const LocalBackend = @import("storage/local.zig").LocalBackend;
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
};

pub const storage = struct {
    pub const backend = @import("storage/backend.zig");
    pub const local = @import("storage/local.zig");
    pub const aws = @import("storage/aws.zig");
    pub const gcp = @import("storage/gcp.zig");
    pub const azure = @import("storage/azure.zig");
};

pub const cluster = struct {
    pub const raft = @import("cluster/raft.zig");
};

pub const auth = @import("auth.zig");
pub const xml = @import("xml.zig");
pub const errors = @import("errors.zig");
pub const validation = @import("validation.zig");
pub const metrics = @import("metrics.zig");

const CliArgs = struct {
    config_path: []const u8 = "bleepstore.yaml",
    port: ?u16 = null,
    host: ?[]const u8 = null,
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

    // Step 2: Initialize metrics (record start time)
    metrics_mod.initMetrics();
    std.log.info("metrics initialized", .{});

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
            ) catch |err| {
                std.log.err("failed to initialize Azure gateway backend: {}", .{err});
                std.process.exit(1);
            };
            storage_backend = azure_backend.?.storageBackend();
            std.log.info("Azure gateway backend initialized: container={s} account={s}", .{
                cfg.storage.azure_container, cfg.storage.azure_account,
            });
        },
    }
    defer {
        if (local_backend) |*lb| lb.deinit();
        if (aws_backend) |*ab| ab.deinit();
        if (gcp_backend) |*gb| gb.deinit();
        if (azure_backend) |*azb| azb.deinit();
    }

    // Step 6: Update metrics gauges from metadata store
    if (metadata_store.metadataStore().countBuckets()) |count| {
        metrics_mod.setBucketsTotal(count);
    } else |_| {}
    if (metadata_store.metadataStore().countObjects()) |count| {
        metrics_mod.setObjectsTotal(count);
    } else |_| {}

    // Install signal handlers (SIGTERM/SIGINT -> stop accepting, exit)
    installSignalHandlers();

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
