const std = @import("std");

pub const ServerConfig = struct {
    host: []const u8 = "0.0.0.0",
    port: u16 = 9013,
    region: []const u8 = "us-east-1",
    max_connections: u32 = 1024,
    request_timeout_ms: u64 = 30_000,
    shutdown_timeout: u64 = 30,
    max_object_size: u64 = 5368709120, // 5 GiB
};

pub const AuthConfig = struct {
    enabled: bool = true,
    access_key: []const u8 = "bleepstore",
    secret_key: []const u8 = "bleepstore-secret",
};

pub const MetadataConfig = struct {
    engine: MetadataEngineType = .sqlite,
    sqlite_path: []const u8 = "./data/metadata.db",
    local_root: []const u8 = "./data/metadata",
    local_compact_on_startup: bool = true,
    dynamodb_table: []const u8 = "bleepstore-metadata",
    dynamodb_region: []const u8 = "us-east-1",
    dynamodb_endpoint_url: []const u8 = "",
    dynamodb_access_key_id: []const u8 = "",
    dynamodb_secret_access_key: []const u8 = "",
    firestore_collection: []const u8 = "bleepstore",
    firestore_project: []const u8 = "",
    firestore_credentials_file: []const u8 = "",
    cosmos_database: []const u8 = "bleepstore",
    cosmos_container: []const u8 = "metadata",
    cosmos_endpoint: []const u8 = "",
    cosmos_connection_string: []const u8 = "",

    pub const MetadataEngineType = enum {
        sqlite,
        memory,
        local,
        dynamodb,
        firestore,
        cosmos,
        raft,
    };
};

pub const StorageConfig = struct {
    backend: StorageBackendType = .local,
    local_root: []const u8 = "./data/objects",
    // Memory backend
    memory_max_size_bytes: u64 = 0,
    memory_persistence: []const u8 = "none",
    memory_snapshot_path: []const u8 = "./data/memory.snap",
    memory_snapshot_interval_seconds: u64 = 300,
    // AWS S3
    aws_bucket: []const u8 = "",
    aws_region: []const u8 = "us-east-1",
    aws_prefix: []const u8 = "",
    aws_endpoint_url: []const u8 = "",
    aws_use_path_style: bool = false,
    aws_access_key_id: []const u8 = "",
    aws_secret_access_key: []const u8 = "",
    // GCP Cloud Storage
    gcp_bucket: []const u8 = "",
    gcp_project: []const u8 = "",
    gcp_prefix: []const u8 = "",
    gcp_credentials_file: []const u8 = "",
    // Azure Blob
    azure_container: []const u8 = "",
    azure_account: []const u8 = "",
    azure_prefix: []const u8 = "",
    azure_connection_string: []const u8 = "",
    azure_use_managed_identity: bool = false,

    pub const StorageBackendType = enum {
        local,
        memory,
        sqlite,
        aws,
        gcp,
        azure,
    };
};

pub const LoggingConfig = struct {
    level: []const u8 = "info",
    format: []const u8 = "text",
};

pub const ObservabilityConfig = struct {
    metrics: bool = true,
    health_check: bool = true,
};

pub const ClusterConfig = struct {
    enabled: bool = false,
    node_id: []const u8 = "",
    peers: []const []const u8 = &.{},
    bind_addr: []const u8 = "0.0.0.0:9001",
    data_dir: []const u8 = "./data/raft",
    election_timeout_ms: u64 = 1000,
    heartbeat_interval_ms: u64 = 150,
};

pub const Config = struct {
    server: ServerConfig = .{},
    auth: AuthConfig = .{},
    metadata: MetadataConfig = .{},
    storage: StorageConfig = .{},
    cluster: ClusterConfig = .{},
    logging: LoggingConfig = .{},
    observability: ObservabilityConfig = .{},

    /// Backing buffer for string values loaded from file.
    /// When non-null, string fields in sub-configs point into this buffer.
    /// Must remain alive as long as the Config is in use.
    _file_contents: ?[]const u8 = null,

    pub fn default() Config {
        return Config{};
    }

    /// Free the backing file contents buffer if one exists.
    pub fn deinit(self: *Config, allocator: std.mem.Allocator) void {
        if (self._file_contents) |contents| {
            allocator.free(contents);
            self._file_contents = null;
        }
    }
};

/// Load configuration from a simple key=value file.
///
/// The file format supports two styles:
///   1. Flat key=value: `server.port = 9000`
///   2. YAML-like indented: `  port: 9000` (under a `server:` section)
///
/// Lines starting with # are comments. Empty lines are skipped.
/// For YAML-style files (like bleepstore.example.yaml), we parse sections
/// and build the dotted key from section + field.
///
/// Note: String values in the returned Config point into an internal buffer.
/// The Config owns this buffer. Call `Config.deinit(allocator)` to free it.
pub fn loadConfig(allocator: std.mem.Allocator, path: []const u8) !Config {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    const contents = try file.readToEndAlloc(allocator, 1024 * 1024);
    // Do NOT free contents -- string slices in Config will point into it.
    // Ownership is transferred to Config._file_contents.

    var cfg = Config.default();
    cfg._file_contents = contents;

    // Track current YAML section for indented style
    var current_section: []const u8 = "";

    var lines = std.mem.splitScalar(u8, contents, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0 or trimmed[0] == '#') continue;

        // Check if this is a YAML-style section header (e.g., "server:")
        // A section header has no '=' and ends with ':'
        // And is NOT indented
        const is_indented = line.len > 0 and (line[0] == ' ' or line[0] == '\t');

        if (!is_indented and std.mem.endsWith(u8, trimmed, ":") and
            std.mem.indexOfScalar(u8, trimmed, '=') == null)
        {
            // Strip trailing ':'
            current_section = std.mem.trim(u8, trimmed[0 .. trimmed.len - 1], " \t");
            continue;
        }

        // Determine key and value.
        // Support both `key = value` (flat) and `key: value` (YAML-like)
        var key: []const u8 = "";
        var value: []const u8 = "";

        if (std.mem.indexOfScalar(u8, trimmed, '=')) |eq_idx| {
            key = std.mem.trim(u8, trimmed[0..eq_idx], " \t");
            value = std.mem.trim(u8, trimmed[eq_idx + 1 ..], " \t");
        } else if (std.mem.indexOfScalar(u8, trimmed, ':')) |colon_idx| {
            key = std.mem.trim(u8, trimmed[0..colon_idx], " \t");
            value = std.mem.trim(u8, trimmed[colon_idx + 1 ..], " \t");
        } else {
            continue;
        }

        // Strip surrounding quotes from value
        if (value.len >= 2 and value[0] == '"' and value[value.len - 1] == '"') {
            value = value[1 .. value.len - 1];
        }

        // Build dotted key if we're in a YAML section and the key isn't already dotted
        var full_key_buf: [256]u8 = undefined;
        var full_key: []const u8 = key;
        if (is_indented and current_section.len > 0 and
            std.mem.indexOfScalar(u8, key, '.') == null)
        {
            const written = std.fmt.bufPrint(&full_key_buf, "{s}.{s}", .{ current_section, key }) catch continue;
            full_key = written;
        }

        // Apply the key-value pair to the config
        applyConfigValue(&cfg, full_key, value);
    }

    return cfg;
}

fn applyConfigValue(cfg: *Config, key: []const u8, value: []const u8) void {
    // Server settings
    if (std.mem.eql(u8, key, "server.host")) {
        cfg.server.host = value;
    } else if (std.mem.eql(u8, key, "server.port")) {
        cfg.server.port = std.fmt.parseInt(u16, value, 10) catch return;
    } else if (std.mem.eql(u8, key, "server.region")) {
        cfg.server.region = value;
    } else if (std.mem.eql(u8, key, "server.shutdown_timeout")) {
        cfg.server.shutdown_timeout = std.fmt.parseInt(u64, value, 10) catch return;
    } else if (std.mem.eql(u8, key, "server.max_object_size")) {
        cfg.server.max_object_size = std.fmt.parseInt(u64, value, 10) catch return;
    }
    // Auth settings
    else if (std.mem.eql(u8, key, "auth.enabled")) {
        cfg.auth.enabled = std.mem.eql(u8, value, "true");
    } else if (std.mem.eql(u8, key, "auth.access_key") or std.mem.eql(u8, key, "auth.access_key_id")) {
        cfg.auth.access_key = value;
    } else if (std.mem.eql(u8, key, "auth.secret_key") or std.mem.eql(u8, key, "auth.secret_access_key")) {
        cfg.auth.secret_key = value;
    }
    // Metadata settings
    else if (std.mem.eql(u8, key, "metadata.engine")) {
        if (std.mem.eql(u8, value, "sqlite")) {
            cfg.metadata.engine = .sqlite;
        } else if (std.mem.eql(u8, value, "memory")) {
            cfg.metadata.engine = .memory;
        } else if (std.mem.eql(u8, value, "local")) {
            cfg.metadata.engine = .local;
        } else if (std.mem.eql(u8, value, "dynamodb")) {
            cfg.metadata.engine = .dynamodb;
        } else if (std.mem.eql(u8, value, "firestore")) {
            cfg.metadata.engine = .firestore;
        } else if (std.mem.eql(u8, value, "cosmos")) {
            cfg.metadata.engine = .cosmos;
        } else if (std.mem.eql(u8, value, "raft")) {
            cfg.metadata.engine = .raft;
        }
    } else if (std.mem.eql(u8, key, "metadata.sqlite_path") or std.mem.eql(u8, key, "metadata.sqlite.path") or std.mem.eql(u8, key, "sqlite.path")) {
        cfg.metadata.sqlite_path = value;
    } else if (std.mem.eql(u8, key, "metadata.local_root") or std.mem.eql(u8, key, "metadata.local.root_dir")) {
        cfg.metadata.local_root = value;
    } else if (std.mem.eql(u8, key, "metadata.local_compact_on_startup")) {
        cfg.metadata.local_compact_on_startup = std.mem.eql(u8, value, "true");
    } else if (std.mem.eql(u8, key, "metadata.dynamodb_table") or std.mem.eql(u8, key, "dynamodb.table")) {
        cfg.metadata.dynamodb_table = value;
    } else if (std.mem.eql(u8, key, "metadata.dynamodb_region") or std.mem.eql(u8, key, "dynamodb.region")) {
        cfg.metadata.dynamodb_region = value;
    } else if (std.mem.eql(u8, key, "metadata.dynamodb_endpoint_url") or std.mem.eql(u8, key, "dynamodb.endpoint_url")) {
        cfg.metadata.dynamodb_endpoint_url = value;
    } else if (std.mem.eql(u8, key, "metadata.dynamodb_access_key_id") or std.mem.eql(u8, key, "dynamodb.access_key_id")) {
        cfg.metadata.dynamodb_access_key_id = value;
    } else if (std.mem.eql(u8, key, "metadata.dynamodb_secret_access_key") or std.mem.eql(u8, key, "dynamodb.secret_access_key")) {
        cfg.metadata.dynamodb_secret_access_key = value;
    } else if (std.mem.eql(u8, key, "metadata.firestore_collection") or std.mem.eql(u8, key, "firestore.collection")) {
        cfg.metadata.firestore_collection = value;
    } else if (std.mem.eql(u8, key, "metadata.firestore_project") or std.mem.eql(u8, key, "firestore.project")) {
        cfg.metadata.firestore_project = value;
    } else if (std.mem.eql(u8, key, "metadata.firestore_credentials_file") or std.mem.eql(u8, key, "firestore.credentials_file")) {
        cfg.metadata.firestore_credentials_file = value;
    } else if (std.mem.eql(u8, key, "metadata.cosmos_database") or std.mem.eql(u8, key, "cosmos.database")) {
        cfg.metadata.cosmos_database = value;
    } else if (std.mem.eql(u8, key, "metadata.cosmos_container") or std.mem.eql(u8, key, "cosmos.container")) {
        cfg.metadata.cosmos_container = value;
    } else if (std.mem.eql(u8, key, "metadata.cosmos_endpoint") or std.mem.eql(u8, key, "cosmos.endpoint")) {
        cfg.metadata.cosmos_endpoint = value;
    } else if (std.mem.eql(u8, key, "metadata.cosmos_connection_string") or std.mem.eql(u8, key, "cosmos.connection_string")) {
        cfg.metadata.cosmos_connection_string = value;
    }
    // Storage settings
    else if (std.mem.eql(u8, key, "storage.backend")) {
        if (std.mem.eql(u8, value, "local")) {
            cfg.storage.backend = .local;
        } else if (std.mem.eql(u8, value, "memory")) {
            cfg.storage.backend = .memory;
        } else if (std.mem.eql(u8, value, "sqlite")) {
            cfg.storage.backend = .sqlite;
        } else if (std.mem.eql(u8, value, "aws")) {
            cfg.storage.backend = .aws;
        } else if (std.mem.eql(u8, value, "gcp")) {
            cfg.storage.backend = .gcp;
        } else if (std.mem.eql(u8, value, "azure")) {
            cfg.storage.backend = .azure;
        }
    } else if (std.mem.eql(u8, key, "storage.local_root") or std.mem.eql(u8, key, "storage.local.root_dir") or std.mem.eql(u8, key, "local.root_dir")) {
        cfg.storage.local_root = value;
    } else if (std.mem.eql(u8, key, "storage.memory.max_size_bytes") or std.mem.eql(u8, key, "memory.max_size_bytes")) {
        cfg.storage.memory_max_size_bytes = std.fmt.parseInt(u64, value, 10) catch return;
    } else if (std.mem.eql(u8, key, "storage.memory.persistence") or std.mem.eql(u8, key, "memory.persistence")) {
        cfg.storage.memory_persistence = value;
    } else if (std.mem.eql(u8, key, "storage.memory.snapshot_path") or std.mem.eql(u8, key, "memory.snapshot_path")) {
        cfg.storage.memory_snapshot_path = value;
    } else if (std.mem.eql(u8, key, "storage.memory.snapshot_interval_seconds") or std.mem.eql(u8, key, "memory.snapshot_interval_seconds")) {
        cfg.storage.memory_snapshot_interval_seconds = std.fmt.parseInt(u64, value, 10) catch return;
    } else if (std.mem.eql(u8, key, "storage.aws.bucket") or std.mem.eql(u8, key, "aws.bucket")) {
        cfg.storage.aws_bucket = value;
    } else if (std.mem.eql(u8, key, "storage.aws.region") or std.mem.eql(u8, key, "aws.region")) {
        cfg.storage.aws_region = value;
    } else if (std.mem.eql(u8, key, "storage.aws.prefix") or std.mem.eql(u8, key, "aws.prefix")) {
        cfg.storage.aws_prefix = value;
    } else if (std.mem.eql(u8, key, "storage.aws.access_key_id") or std.mem.eql(u8, key, "aws.access_key_id")) {
        cfg.storage.aws_access_key_id = value;
    } else if (std.mem.eql(u8, key, "storage.aws.secret_access_key") or std.mem.eql(u8, key, "aws.secret_access_key")) {
        cfg.storage.aws_secret_access_key = value;
    } else if (std.mem.eql(u8, key, "storage.aws.endpoint_url") or std.mem.eql(u8, key, "aws.endpoint_url")) {
        cfg.storage.aws_endpoint_url = value;
    } else if (std.mem.eql(u8, key, "storage.aws.use_path_style") or std.mem.eql(u8, key, "aws.use_path_style")) {
        cfg.storage.aws_use_path_style = std.mem.eql(u8, value, "true");
    } else if (std.mem.eql(u8, key, "storage.gcp.bucket") or std.mem.eql(u8, key, "gcp.bucket")) {
        cfg.storage.gcp_bucket = value;
    } else if (std.mem.eql(u8, key, "storage.gcp.project") or std.mem.eql(u8, key, "gcp.project")) {
        cfg.storage.gcp_project = value;
    } else if (std.mem.eql(u8, key, "storage.gcp.prefix") or std.mem.eql(u8, key, "gcp.prefix")) {
        cfg.storage.gcp_prefix = value;
    } else if (std.mem.eql(u8, key, "storage.gcp.credentials_file") or std.mem.eql(u8, key, "gcp.credentials_file")) {
        cfg.storage.gcp_credentials_file = value;
    } else if (std.mem.eql(u8, key, "storage.azure.container") or std.mem.eql(u8, key, "azure.container")) {
        cfg.storage.azure_container = value;
    } else if (std.mem.eql(u8, key, "storage.azure.account") or std.mem.eql(u8, key, "azure.account")) {
        cfg.storage.azure_account = value;
    } else if (std.mem.eql(u8, key, "storage.azure.prefix") or std.mem.eql(u8, key, "azure.prefix")) {
        cfg.storage.azure_prefix = value;
    } else if (std.mem.eql(u8, key, "storage.azure.connection_string") or std.mem.eql(u8, key, "azure.connection_string")) {
        cfg.storage.azure_connection_string = value;
    } else if (std.mem.eql(u8, key, "storage.azure.use_managed_identity") or std.mem.eql(u8, key, "azure.use_managed_identity")) {
        cfg.storage.azure_use_managed_identity = std.mem.eql(u8, value, "true");
    }
    // Cluster settings
    else if (std.mem.eql(u8, key, "cluster.enabled")) {
        cfg.cluster.enabled = std.mem.eql(u8, value, "true");
    } else if (std.mem.eql(u8, key, "cluster.node_id")) {
        cfg.cluster.node_id = value;
    } else if (std.mem.eql(u8, key, "cluster.bind_addr")) {
        cfg.cluster.bind_addr = value;
    } else if (std.mem.eql(u8, key, "cluster.data_dir")) {
        cfg.cluster.data_dir = value;
    } else if (std.mem.eql(u8, key, "cluster.election_timeout_ms")) {
        cfg.cluster.election_timeout_ms = std.fmt.parseInt(u64, value, 10) catch return;
    } else if (std.mem.eql(u8, key, "cluster.heartbeat_interval_ms")) {
        cfg.cluster.heartbeat_interval_ms = std.fmt.parseInt(u64, value, 10) catch return;
    }
    // Logging settings
    else if (std.mem.eql(u8, key, "logging.level")) {
        cfg.logging.level = value;
    } else if (std.mem.eql(u8, key, "logging.format")) {
        cfg.logging.format = value;
    }
    // Observability settings
    else if (std.mem.eql(u8, key, "observability.metrics")) {
        cfg.observability.metrics = std.mem.eql(u8, value, "true");
    } else if (std.mem.eql(u8, key, "observability.health_check")) {
        cfg.observability.health_check = std.mem.eql(u8, value, "true");
    }
}

test "default config" {
    const cfg = Config.default();
    try std.testing.expectEqual(@as(u16, 9013), cfg.server.port);
    try std.testing.expect(cfg.auth.enabled);
    try std.testing.expectEqualStrings("./data/metadata.db", cfg.metadata.sqlite_path);
    try std.testing.expectEqualStrings("us-east-1", cfg.server.region);
    try std.testing.expectEqualStrings("bleepstore", cfg.auth.access_key);
    try std.testing.expectEqualStrings("bleepstore-secret", cfg.auth.secret_key);
}
