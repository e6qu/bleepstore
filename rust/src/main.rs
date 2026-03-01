//! BleepStore -- S3-compatible object storage server.
//!
//! Crash-only design: every startup is a recovery. There is no separate
//! recovery mode. SIGTERM/SIGINT handlers only stop accepting connections
//! and wait with a timeout before exiting -- no cleanup.

use std::sync::Arc;

use clap::Parser;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Command-line arguments for the BleepStore server.
#[derive(Parser, Debug)]
#[command(
    name = "bleepstore",
    version,
    about = "S3-compatible object storage server"
)]
struct Cli {
    /// Path to the YAML configuration file.
    #[arg(short, long, default_value = "bleepstore.example.yaml")]
    config: String,

    /// Override the bind address (host:port).
    #[arg(short, long)]
    bind: Option<String>,

    /// Log level: trace, debug, info, warn, error.
    #[arg(long)]
    log_level: Option<String>,

    /// Log format: text or json.
    #[arg(long)]
    log_format: Option<String>,

    /// Graceful shutdown timeout in seconds.
    #[arg(long)]
    shutdown_timeout: Option<u64>,

    /// Maximum object size in bytes.
    #[arg(long)]
    max_object_size: Option<u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Load configuration, then apply CLI overrides before initializing logging.
    let mut config = bleepstore::config::load_config(&cli.config)?;

    // CLI overrides for logging.
    if let Some(level) = &cli.log_level {
        config.logging.level = level.clone();
    }
    if let Some(format) = &cli.log_format {
        config.logging.format = format.clone();
    }

    // CLI overrides for server settings.
    if let Some(timeout) = cli.shutdown_timeout {
        config.server.shutdown_timeout = timeout;
    }
    if let Some(max_size) = cli.max_object_size {
        config.server.max_object_size = max_size;
    }

    // Initialize tracing / logging.
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.logging.level));

    if config.logging.format == "json" {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

    info!("Loading configuration from {}", cli.config);

    let bind_addr = cli
        .bind
        .unwrap_or_else(|| format!("{}:{}", config.server.host, config.server.port));

    // Crash-only startup: every startup IS recovery.
    info!("Crash-only startup: performing recovery checks");

    // Initialize Prometheus metrics recorder and register metric descriptions (conditional).
    if config.observability.metrics {
        bleepstore::metrics::init_metrics();
        bleepstore::metrics::describe_metrics();
        info!("Prometheus metrics initialized");
    } else {
        info!("Prometheus metrics disabled by configuration");
    }

    // Initialize metadata store based on config.
    let expired_uploads: Vec<(String, String, String)> = Vec::new();
    let metadata: Arc<dyn bleepstore::metadata::store::MetadataStore> =
        match config.metadata.engine.as_str() {
            "memory" => {
                let store = bleepstore::metadata::memory::MemoryMetadataStore::new();
                store.seed_credential(&config.auth.access_key, &config.auth.secret_key)?;
                info!("Memory metadata store initialized");
                Arc::new(store)
            }
            "local" => {
                let store =
                    bleepstore::metadata::local::LocalMetadataStore::new(&config.metadata.local)?;
                store.seed_credential(&config.auth.access_key, &config.auth.secret_key)?;
                info!(
                    "Local JSONL metadata store initialized at {}",
                    config.metadata.local.root_dir
                );
                Arc::new(store)
            }
            "dynamodb" => {
                let store = bleepstore::metadata::dynamodb::DynamoDbMetadataStore::new(
                    &config.metadata.dynamodb,
                )
                .await?;
                store.seed_credential(&config.auth.access_key, &config.auth.secret_key)?;
                info!("DynamoDB metadata store initialized");
                Arc::new(store)
            }
            "firestore" => {
                let store = bleepstore::metadata::firestore::FirestoreMetadataStore::new(
                    &config.metadata.firestore,
                )
                .await?;
                store.seed_credential(&config.auth.access_key, &config.auth.secret_key)?;
                info!("Firestore metadata store initialized");
                Arc::new(store)
            }
            "cosmos" => {
                let store =
                    bleepstore::metadata::cosmos::CosmosMetadataStore::new(&config.metadata.cosmos)
                        .await?;
                store.seed_credential(&config.auth.access_key, &config.auth.secret_key)?;
                info!("Cosmos DB metadata store initialized");
                Arc::new(store)
            }
            _ => {
                let metadata_path = &config.metadata.sqlite.path;
                if let Some(parent) = std::path::Path::new(metadata_path).parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let store = bleepstore::metadata::sqlite::SqliteMetadataStore::new(metadata_path)?;
                store.seed_credential(&config.auth.access_key, &config.auth.secret_key)?;
                info!("SQLite metadata store initialized at {}", metadata_path);

                let expired = store.reap_expired_uploads(604800)?;
                if !expired.is_empty() {
                    info!("Reaped {} expired multipart uploads", expired.len());
                }

                Arc::new(store)
            }
        };

    // Initialize storage backend based on config.
    let storage: Arc<dyn bleepstore::storage::backend::StorageBackend> =
        match config.storage.backend.as_str() {
            "aws" => {
                let aws_config = config.storage.aws.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "storage.backend is 'aws' but storage.aws config section is missing"
                    )
                })?;
                let endpoint_url = if aws_config.endpoint_url.is_empty() {
                    None
                } else {
                    Some(aws_config.endpoint_url.clone())
                };
                let access_key = if aws_config.access_key_id.is_empty() {
                    None
                } else {
                    Some(aws_config.access_key_id.clone())
                };
                let secret_key = if aws_config.secret_access_key.is_empty() {
                    None
                } else {
                    Some(aws_config.secret_access_key.clone())
                };
                let backend = bleepstore::storage::aws::AwsGatewayBackend::new(
                    aws_config.bucket.clone(),
                    aws_config.region.clone(),
                    aws_config.prefix.clone(),
                    endpoint_url,
                    aws_config.use_path_style,
                    access_key,
                    secret_key,
                )
                .await?;
                info!(
                    "AWS gateway storage backend initialized: bucket={} region={} prefix='{}'",
                    aws_config.bucket, aws_config.region, aws_config.prefix
                );
                Arc::new(backend)
            }
            "gcp" => {
                let gcp_config = config.storage.gcp.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "storage.backend is 'gcp' but storage.gcp config section is missing"
                    )
                })?;
                let credentials_file = if gcp_config.credentials_file.is_empty() {
                    None
                } else {
                    Some(gcp_config.credentials_file.clone())
                };
                let backend = bleepstore::storage::gcp::GcpGatewayBackend::new(
                    gcp_config.bucket.clone(),
                    gcp_config.project.clone(),
                    gcp_config.prefix.clone(),
                    credentials_file,
                )
                .await?;
                info!(
                    "GCP gateway storage backend initialized: bucket={} project={} prefix='{}'",
                    gcp_config.bucket, gcp_config.project, gcp_config.prefix
                );
                Arc::new(backend)
            }
            "azure" => {
                let azure_config = config.storage.azure.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "storage.backend is 'azure' but storage.azure config section is missing"
                    )
                })?;
                let connection_string = if azure_config.connection_string.is_empty() {
                    None
                } else {
                    Some(azure_config.connection_string.clone())
                };
                let backend = bleepstore::storage::azure::AzureGatewayBackend::new(
                    azure_config.container.clone(),
                    azure_config.account.clone(),
                    azure_config.prefix.clone(),
                    connection_string,
                    azure_config.use_managed_identity,
                )
                .await?;
                info!(
                "Azure gateway storage backend initialized: container={} account={} prefix='{}'",
                azure_config.container, azure_config.account, azure_config.prefix
            );
                Arc::new(backend)
            }
            "memory" => {
                let mem_config = config.storage.memory.as_ref();
                let (max_size, persistence, snap_path, snap_interval) = match mem_config {
                    Some(mc) => (
                        mc.max_size_bytes,
                        mc.persistence.as_str(),
                        mc.snapshot_path.as_str(),
                        mc.snapshot_interval_seconds,
                    ),
                    None => (0, "none", "./data/memory.snap", 300),
                };
                let backend = bleepstore::storage::memory::MemoryBackend::new(
                    max_size,
                    persistence,
                    snap_path,
                    snap_interval,
                )?;
                info!(
                    "Memory storage backend initialized (max_size={}, persistence={})",
                    max_size, persistence
                );
                Arc::new(backend)
            }
            "sqlite" => {
                let db_path = &config.metadata.sqlite.path;
                let backend = bleepstore::storage::sqlite::SqliteBackend::new(db_path)?;
                info!("SQLite storage backend initialized at {}", db_path);
                Arc::new(backend)
            }
            _ => {
                let storage_root = &config.storage.local.root_dir;
                let local_backend = bleepstore::storage::local::LocalBackend::new(storage_root)?;
                info!("Local storage backend initialized at {}", storage_root);

                // Clean up storage for reaped expired multipart uploads.
                for (upload_id, bucket, _key) in &expired_uploads {
                    let _ = local_backend.delete_upload_parts(bucket, upload_id);
                }

                Arc::new(local_backend)
            }
        };

    // Build AppState.
    let state = Arc::new(bleepstore::AppState {
        config: config.clone(),
        metadata,
        storage,
        auth_cache: bleepstore::auth::AuthCache::new(),
    });

    let app = bleepstore::server::app(state);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    info!("BleepStore listening on {}", bind_addr);

    // Graceful shutdown: on SIGTERM/SIGINT, stop accepting new connections,
    // wait for in-flight requests to complete (with timeout), then exit.
    // No cleanup -- crash-only design means next startup handles recovery.
    let shutdown_timeout = config.server.shutdown_timeout;
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal_with_timeout(shutdown_timeout));

    server.await?;

    info!("BleepStore shut down");

    Ok(())
}

/// Wait for SIGTERM or SIGINT (Ctrl+C), then return to trigger graceful shutdown.
/// Spawns a background task that will force-exit after `timeout_secs` to enforce
/// a hard shutdown deadline.
async fn shutdown_signal_with_timeout(timeout_secs: u64) {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received SIGINT, shutting down");
        },
        _ = terminate => {
            tracing::info!("Received SIGTERM, shutting down");
        },
    }

    // Spawn a hard shutdown deadline: if graceful drain takes too long, force exit.
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)).await;
        tracing::warn!("Shutdown timeout ({timeout_secs}s) exceeded, forcing exit");
        std::process::exit(1);
    });
}
