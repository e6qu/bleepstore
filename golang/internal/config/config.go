// Package config handles loading and parsing of BleepStore configuration.
package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for BleepStore.
type Config struct {
	Server        ServerConfig        `yaml:"server"`
	Auth          AuthConfig          `yaml:"auth"`
	Metadata      MetadataConfig      `yaml:"metadata"`
	Storage       StorageConfig       `yaml:"storage"`
	Cluster       ClusterConfig       `yaml:"cluster"`
	Logging       LoggingConfig       `yaml:"logging"`
	Observability ObservabilityConfig `yaml:"observability"`
}

// ObservabilityConfig holds settings for metrics and health check endpoints.
type ObservabilityConfig struct {
	// Metrics enables the /metrics Prometheus endpoint.
	Metrics bool `yaml:"metrics"`
	// HealthCheck enables the /healthz and /readyz liveness/readiness probes.
	HealthCheck bool `yaml:"health_check"`
}

// LoggingConfig holds structured logging settings.
type LoggingConfig struct {
	// Level is the minimum log level: "debug", "info", "warn", "error".
	Level string `yaml:"level"`
	// Format is the log output format: "text" or "json".
	Format string `yaml:"format"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	Region          string `yaml:"region"`
	ShutdownTimeout int    `yaml:"shutdown_timeout"` // Graceful shutdown timeout in seconds (default: 30).
	MaxObjectSize   int64  `yaml:"max_object_size"`  // Maximum object size in bytes (default: 5 GiB).
}

// AuthConfig holds authentication and authorization settings.
type AuthConfig struct {
	// AccessKey is the S3 access key used for SigV4 authentication.
	AccessKey string `yaml:"access_key"`
	// SecretKey is the S3 secret key used for SigV4 authentication.
	SecretKey string `yaml:"secret_key"`
}

// MetadataConfig holds metadata store settings.
type MetadataConfig struct {
	// Engine is the metadata backend engine (e.g., "sqlite", "memory", "local", "dynamodb", "firestore", "cosmos").
	Engine string `yaml:"engine"`
	// SQLite holds SQLite-specific settings.
	SQLite SQLiteConfig `yaml:"sqlite"`
	// Local holds local JSONL-specific settings.
	Local LocalMetaConfig `yaml:"local"`
	// DynamoDB holds DynamoDB-specific settings.
	DynamoDB DynamoDBConfig `yaml:"dynamodb"`
	// Firestore holds Firestore-specific settings.
	Firestore FirestoreConfig `yaml:"firestore"`
	// Cosmos holds Cosmos DB-specific settings.
	Cosmos CosmosConfig `yaml:"cosmos"`
}

// SQLiteConfig holds SQLite-specific metadata store settings.
type SQLiteConfig struct {
	// Path is the filesystem path for the SQLite database file.
	Path string `yaml:"path"`
}

// LocalMetaConfig holds local JSONL file-based metadata store settings.
type LocalMetaConfig struct {
	// RootDir is the directory where JSONL files are stored.
	RootDir string `yaml:"root_dir"`
	// CompactOnStartup enables compaction of JSONL files on startup.
	CompactOnStartup bool `yaml:"compact_on_startup"`
}

// DynamoDBConfig holds DynamoDB-specific metadata store settings.
type DynamoDBConfig struct {
	// Table is the DynamoDB table name.
	Table string `yaml:"table"`
	// Region is the AWS region.
	Region string `yaml:"region"`
	// EndpointURL is a custom DynamoDB endpoint (for local testing).
	EndpointURL string `yaml:"endpoint_url"`
}

// FirestoreConfig holds Firestore-specific metadata store settings.
type FirestoreConfig struct {
	// ProjectID is the GCP project ID.
	ProjectID string `yaml:"project_id"`
	// Collection is the Firestore collection prefix.
	Collection string `yaml:"collection"`
	// CredentialsFile is the path to a service account JSON file.
	CredentialsFile string `yaml:"credentials_file"`
}

// CosmosConfig holds Azure Cosmos DB-specific metadata store settings.
type CosmosConfig struct {
	// Endpoint is the Cosmos DB account endpoint.
	Endpoint string `yaml:"endpoint"`
	// Database is the Cosmos DB database name.
	Database string `yaml:"database"`
	// Container is the Cosmos DB container name.
	Container string `yaml:"container"`
	// MasterKey is the Cosmos DB master key.
	MasterKey string `yaml:"master_key"`
}

// StorageConfig holds object storage backend settings.
type StorageConfig struct {
	// Backend is the storage backend type (e.g., "local", "memory", "sqlite", "aws", "gcp", "azure").
	Backend string       `yaml:"backend"`
	Local   LocalConfig  `yaml:"local"`
	Memory  MemoryConfig `yaml:"memory"`
	AWS     AWSConfig    `yaml:"aws"`
	GCP     GCPConfig    `yaml:"gcp"`
	Azure   AzureConfig  `yaml:"azure"`
}

// MemoryConfig holds in-memory storage backend settings.
type MemoryConfig struct {
	// MaxSizeBytes is the maximum total size in bytes (0 = unlimited).
	MaxSizeBytes int64 `yaml:"max_size_bytes"`
	// Persistence mode: "none" or "snapshot".
	Persistence string `yaml:"persistence"`
	// SnapshotPath is the file path for snapshot persistence.
	SnapshotPath string `yaml:"snapshot_path"`
	// SnapshotIntervalSeconds is the interval between periodic snapshots (0 = only on shutdown).
	SnapshotIntervalSeconds int `yaml:"snapshot_interval_seconds"`
}

// AWSConfig holds AWS S3 gateway backend settings.
type AWSConfig struct {
	// Bucket is the S3 bucket name.
	Bucket string `yaml:"bucket"`
	// Region is the AWS region.
	Region string `yaml:"region"`
	// Prefix is the optional key prefix for all objects.
	Prefix string `yaml:"prefix"`
	// EndpointURL is a custom S3-compatible endpoint (e.g. MinIO, LocalStack).
	EndpointURL string `yaml:"endpoint_url"`
	// UsePathStyle forces path-style URL addressing.
	UsePathStyle bool `yaml:"use_path_style"`
	// AccessKeyID is an explicit AWS access key (falls back to env/credential chain).
	AccessKeyID string `yaml:"access_key_id"`
	// SecretAccessKey is an explicit AWS secret key (falls back to env/credential chain).
	SecretAccessKey string `yaml:"secret_access_key"`
}

// GCPConfig holds GCP Cloud Storage gateway backend settings.
type GCPConfig struct {
	// Bucket is the GCS bucket name.
	Bucket string `yaml:"bucket"`
	// Project is the GCP project ID.
	Project string `yaml:"project"`
	// Prefix is the optional key prefix for all objects.
	Prefix string `yaml:"prefix"`
	// CredentialsFile is the path to a service account JSON file.
	CredentialsFile string `yaml:"credentials_file"`
}

// AzureConfig holds Azure Blob Storage gateway backend settings.
type AzureConfig struct {
	// Container is the Azure container name.
	Container string `yaml:"container"`
	// Account is the Azure storage account name.
	Account string `yaml:"account"`
	// AccountURL is the full Azure storage account URL.
	AccountURL string `yaml:"account_url"`
	// Prefix is the optional key prefix for all objects.
	Prefix string `yaml:"prefix"`
	// ConnectionString is an alternative to account-based auth.
	ConnectionString string `yaml:"connection_string"`
	// UseManagedIdentity enables Azure managed identity auth.
	UseManagedIdentity bool `yaml:"use_managed_identity"`
}

// LocalConfig holds local filesystem storage backend settings.
type LocalConfig struct {
	// RootDir is the base directory for local object storage.
	RootDir string `yaml:"root_dir"`
}

// ClusterConfig holds clustering and replication settings.
type ClusterConfig struct {
	// Enabled controls whether clustering is active.
	Enabled bool `yaml:"enabled"`
	// NodeID is the unique identifier for this node in the cluster.
	NodeID string `yaml:"node_id"`
	// BindAddr is the address the Raft transport binds to.
	BindAddr string `yaml:"bind_addr"`
	// Peers is the list of peer addresses for cluster bootstrap.
	Peers []string `yaml:"peers"`
}

// Load reads a YAML configuration file from the given path and returns
// a parsed Config. It applies sensible defaults for unset values.
// If the primary path fails, it falls back to bleepstore.example.yaml
// in the same directory or parent directory.
func Load(path string) (*Config, error) {
	cfg := defaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		// Try fallback paths
		fallbackPaths := []string{
			filepath.Join(filepath.Dir(path), "bleepstore.example.yaml"),
			filepath.Join(filepath.Dir(path), "..", "bleepstore.example.yaml"),
		}
		var fallbackErr error
		for _, fp := range fallbackPaths {
			data, fallbackErr = os.ReadFile(fp)
			if fallbackErr == nil {
				break
			}
		}
		if fallbackErr != nil {
			return nil, fmt.Errorf("reading config file: %w", err)
		}
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	// Apply defaults for empty fields that YAML didn't set
	applyDefaults(cfg)

	return cfg, nil
}

// defaultConfig returns a Config with sensible defaults.
func defaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Host:            "0.0.0.0",
			Port:            9000,
			Region:          "us-east-1",
			ShutdownTimeout: 30,
			MaxObjectSize:   5368709120, // 5 GiB
		},
		Auth: AuthConfig{
			AccessKey: "bleepstore",
			SecretKey: "bleepstore-secret",
		},
		Metadata: MetadataConfig{
			Engine: "sqlite",
			SQLite: SQLiteConfig{
				Path: "./data/metadata.db",
			},
		},
		Storage: StorageConfig{
			Backend: "local",
			Local: LocalConfig{
				RootDir: "./data/objects",
			},
			Memory: MemoryConfig{
				Persistence:             "none",
				SnapshotPath:            "./data/memory.snap",
				SnapshotIntervalSeconds: 300,
			},
			AWS: AWSConfig{
				Region: "us-east-1",
			},
		},
		Observability: ObservabilityConfig{
			Metrics:     true,
			HealthCheck: true,
		},
	}
}

// applyDefaults fills in any fields that are still at their zero value
// after YAML unmarshaling.
func applyDefaults(cfg *Config) {
	if cfg.Server.Host == "" {
		cfg.Server.Host = "0.0.0.0"
	}
	if cfg.Server.Port == 0 {
		cfg.Server.Port = 9000
	}
	if cfg.Server.Region == "" {
		cfg.Server.Region = "us-east-1"
	}
	if cfg.Auth.AccessKey == "" {
		cfg.Auth.AccessKey = "bleepstore"
	}
	if cfg.Auth.SecretKey == "" {
		cfg.Auth.SecretKey = "bleepstore-secret"
	}
	if cfg.Metadata.Engine == "" {
		cfg.Metadata.Engine = "sqlite"
	}
	if cfg.Metadata.SQLite.Path == "" {
		cfg.Metadata.SQLite.Path = "./data/metadata.db"
	}
	if cfg.Metadata.Local.RootDir == "" {
		cfg.Metadata.Local.RootDir = "./data/metadata"
	}
	if cfg.Server.ShutdownTimeout == 0 {
		cfg.Server.ShutdownTimeout = 30
	}
	if cfg.Server.MaxObjectSize == 0 {
		cfg.Server.MaxObjectSize = 5368709120 // 5 GiB
	}
	if cfg.Storage.Backend == "" {
		cfg.Storage.Backend = "local"
	}
	if cfg.Storage.Local.RootDir == "" {
		cfg.Storage.Local.RootDir = "./data/objects"
	}
	if cfg.Storage.Memory.Persistence == "" {
		cfg.Storage.Memory.Persistence = "none"
	}
	if cfg.Storage.Memory.SnapshotPath == "" {
		cfg.Storage.Memory.SnapshotPath = "./data/memory.snap"
	}
	if cfg.Storage.Memory.SnapshotIntervalSeconds == 0 && cfg.Storage.Memory.Persistence == "none" {
		cfg.Storage.Memory.SnapshotIntervalSeconds = 300
	}
	if cfg.Storage.AWS.Region == "" {
		cfg.Storage.AWS.Region = "us-east-1"
	}
}
