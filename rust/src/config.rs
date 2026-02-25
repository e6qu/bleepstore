//! Configuration loading and types for BleepStore.
//!
//! Configuration is read from a YAML file and deserialized into the
//! [`Config`] struct.  Each subsection governs a different part of the
//! system: networking, authentication, metadata persistence, object
//! storage, and cluster coordination.

use serde::Deserialize;
use std::path::Path;

/// Top-level configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// HTTP server settings.
    #[serde(default)]
    pub server: ServerConfig,

    /// Authentication / authorization settings.
    #[serde(default)]
    pub auth: AuthConfig,

    /// Metadata store settings.
    #[serde(default)]
    pub metadata: MetadataConfig,

    /// Object storage backend settings.
    #[serde(default)]
    pub storage: StorageConfig,

    /// Cluster / replication settings.
    #[serde(default)]
    pub cluster: ClusterConfig,

    /// Logging settings.
    #[serde(default)]
    pub logging: LoggingConfig,

    /// Observability settings (metrics + health probes).
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

/// HTTP listener configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Bind host address.
    #[serde(default = "default_host")]
    pub host: String,

    /// Bind port.
    #[serde(default = "default_port")]
    pub port: u16,

    /// AWS region to present (e.g. `us-east-1`).
    #[serde(default = "default_region")]
    pub region: String,

    /// Graceful shutdown timeout in seconds.
    #[serde(default = "default_shutdown_timeout")]
    pub shutdown_timeout: u64,

    /// Maximum object size in bytes (default 5 GiB).
    #[serde(default = "default_max_object_size")]
    pub max_object_size: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            region: default_region(),
            shutdown_timeout: default_shutdown_timeout(),
            max_object_size: default_max_object_size(),
        }
    }
}

/// Logging configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    /// Log level: trace, debug, info, warn, error.
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Log format: text or json.
    #[serde(default = "default_log_format")]
    pub format: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

/// Observability settings.
///
/// Controls Prometheus metrics collection and Kubernetes-style health probes.
/// Both are enabled by default.
#[derive(Debug, Clone, Deserialize)]
pub struct ObservabilityConfig {
    /// Enable Prometheus metrics collection and `/metrics` endpoint.
    #[serde(default = "default_true")]
    pub metrics: bool,

    /// Enable `/healthz` and `/readyz` probes, and deep `/health` checks.
    #[serde(default = "default_true")]
    pub health_check: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            metrics: true,
            health_check: true,
        }
    }
}

/// Authentication settings.
///
/// Field names match `bleepstore.example.yaml`:
/// `auth.access_key` and `auth.secret_key`.
#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    /// Access key (maps to `access_key` in YAML, also accepts `access_key_id`).
    #[serde(alias = "access_key_id", default = "default_access_key")]
    pub access_key: String,

    /// Secret access key (maps to `secret_key` in YAML, also accepts `secret_access_key`).
    #[serde(alias = "secret_access_key", default = "default_secret_key")]
    pub secret_key: String,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            access_key: default_access_key(),
            secret_key: default_secret_key(),
        }
    }
}

/// Metadata store configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct MetadataConfig {
    /// Backend type: `sqlite` or `raft`.
    #[serde(default = "default_metadata_engine")]
    pub engine: String,

    /// SQLite-specific configuration.
    #[serde(default)]
    pub sqlite: SqliteConfig,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            engine: default_metadata_engine(),
            sqlite: SqliteConfig::default(),
        }
    }
}

/// SQLite-specific metadata configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct SqliteConfig {
    /// Path to the SQLite database file.
    #[serde(default = "default_metadata_path")]
    pub path: String,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: default_metadata_path(),
        }
    }
}

/// Object storage backend configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    /// Backend type: `local`, `memory`, `sqlite`, `aws`, `gcp`, `azure`.
    #[serde(default = "default_storage_backend")]
    pub backend: String,

    /// Local storage configuration.
    #[serde(default)]
    pub local: LocalStorageConfig,

    /// Memory storage configuration.
    #[serde(default)]
    pub memory: Option<MemoryStorageConfig>,

    /// AWS S3 gateway configuration.
    #[serde(default)]
    pub aws: Option<AwsStorageConfig>,

    /// GCP Cloud Storage gateway configuration.
    #[serde(default)]
    pub gcp: Option<GcpStorageConfig>,

    /// Azure Blob Storage gateway configuration.
    #[serde(default)]
    pub azure: Option<AzureStorageConfig>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: default_storage_backend(),
            local: LocalStorageConfig::default(),
            memory: None,
            aws: None,
            gcp: None,
            azure: None,
        }
    }
}

/// Local filesystem storage configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct LocalStorageConfig {
    /// Root directory for stored objects.
    #[serde(default = "default_storage_root")]
    pub root_dir: String,
}

impl Default for LocalStorageConfig {
    fn default() -> Self {
        Self {
            root_dir: default_storage_root(),
        }
    }
}

/// Memory storage backend configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct MemoryStorageConfig {
    /// Maximum total size in bytes (0 = unlimited).
    #[serde(default)]
    pub max_size_bytes: u64,
    /// Persistence mode: "none" or "snapshot".
    #[serde(default = "default_persistence_none")]
    pub persistence: String,
    /// File path for snapshot persistence.
    #[serde(default = "default_snapshot_path")]
    pub snapshot_path: String,
    /// Interval between periodic snapshots in seconds (0 = only on shutdown).
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval_seconds: u64,
}

/// AWS S3 gateway configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct AwsStorageConfig {
    /// Backing S3 bucket name.
    pub bucket: String,
    /// AWS region.
    #[serde(default = "default_region")]
    pub region: String,
    /// Key prefix in the backing bucket.
    #[serde(default)]
    pub prefix: String,
    /// Custom S3-compatible endpoint (e.g. MinIO, LocalStack).
    #[serde(default)]
    pub endpoint_url: String,
    /// Force path-style URL addressing.
    #[serde(default)]
    pub use_path_style: bool,
    /// Explicit AWS access key (falls back to env/credential chain).
    #[serde(default)]
    pub access_key_id: String,
    /// Explicit AWS secret key (falls back to env/credential chain).
    #[serde(default)]
    pub secret_access_key: String,
}

/// GCP Cloud Storage gateway configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct GcpStorageConfig {
    /// Backing GCS bucket name.
    pub bucket: String,
    /// GCP project ID.
    #[serde(default)]
    pub project: String,
    /// Key prefix in the backing bucket.
    #[serde(default)]
    pub prefix: String,
    /// Path to a service account JSON file.
    #[serde(default)]
    pub credentials_file: String,
}

/// Azure Blob Storage gateway configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct AzureStorageConfig {
    /// Backing Azure container name.
    pub container: String,
    /// Azure storage account name.
    pub account: String,
    /// Key prefix in the backing container.
    #[serde(default)]
    pub prefix: String,
    /// Alternative to account-based auth.
    #[serde(default)]
    pub connection_string: String,
    /// Enable Azure managed identity auth.
    #[serde(default)]
    pub use_managed_identity: bool,
}

/// Cluster / replication configuration.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ClusterConfig {
    /// Whether clustering is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// This node's unique identifier.
    pub node_id: Option<String>,

    /// List of peer addresses for Raft consensus.
    #[serde(default)]
    pub peers: Vec<String>,
}

// -- Defaults ----------------------------------------------------------------

fn default_true() -> bool {
    true
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    9012
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_access_key() -> String {
    "bleepstore".to_string()
}

fn default_secret_key() -> String {
    "bleepstore-secret".to_string()
}

fn default_metadata_engine() -> String {
    "sqlite".to_string()
}

fn default_metadata_path() -> String {
    "./data/metadata.db".to_string()
}

fn default_storage_backend() -> String {
    "local".to_string()
}

fn default_storage_root() -> String {
    "./data/objects".to_string()
}

fn default_persistence_none() -> String {
    "none".to_string()
}

fn default_snapshot_path() -> String {
    "./data/memory.snap".to_string()
}

fn default_snapshot_interval() -> u64 {
    300
}

fn default_shutdown_timeout() -> u64 {
    30
}

fn default_max_object_size() -> u64 {
    5_368_709_120 // 5 GiB
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "text".to_string()
}

// -- Loader ------------------------------------------------------------------

/// Load and parse configuration from a YAML file at `path`.
pub fn load_config<P: AsRef<Path>>(path: P) -> anyhow::Result<Config> {
    let contents = std::fs::read_to_string(path.as_ref())?;
    let config: Config = serde_yaml::from_str(&contents)?;
    Ok(config)
}
