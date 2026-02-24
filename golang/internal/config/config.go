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
	Server   ServerConfig   `yaml:"server"`
	Auth     AuthConfig     `yaml:"auth"`
	Metadata MetadataConfig `yaml:"metadata"`
	Storage  StorageConfig  `yaml:"storage"`
	Cluster  ClusterConfig  `yaml:"cluster"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Host   string `yaml:"host"`
	Port   int    `yaml:"port"`
	Region string `yaml:"region"`
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
	// Engine is the metadata backend engine (e.g., "sqlite", "raft").
	Engine string       `yaml:"engine"`
	SQLite SQLiteConfig `yaml:"sqlite"`
}

// SQLiteConfig holds SQLite-specific metadata store settings.
type SQLiteConfig struct {
	// Path is the filesystem path for the SQLite database file.
	Path string `yaml:"path"`
}

// StorageConfig holds object storage backend settings.
type StorageConfig struct {
	// Backend is the storage backend type (e.g., "local", "aws", "gcp", "azure").
	Backend string      `yaml:"backend"`
	Local   LocalConfig `yaml:"local"`
	// AWSBucket is the S3 bucket name for the AWS gateway backend.
	AWSBucket string `yaml:"aws_bucket"`
	// AWSRegion is the AWS region for the AWS gateway backend.
	AWSRegion string `yaml:"aws_region"`
	// AWSPrefix is the optional key prefix for all objects in the upstream AWS bucket.
	AWSPrefix string `yaml:"aws_prefix"`
	// GCPBucket is the GCS bucket name for the GCP gateway backend.
	GCPBucket string `yaml:"gcp_bucket"`
	// GCPProject is the GCP project ID for the GCP gateway backend.
	GCPProject string `yaml:"gcp_project"`
	// GCPPrefix is the optional key prefix for all objects in the upstream GCS bucket.
	GCPPrefix string `yaml:"gcp_prefix"`
	// AzureContainer is the container name for the Azure gateway backend.
	AzureContainer string `yaml:"azure_container"`
	// AzureAccount is the storage account name for the Azure gateway backend.
	// Used to construct the account URL: https://{account}.blob.core.windows.net
	AzureAccount string `yaml:"azure_account"`
	// AzureAccountURL is the full Azure storage account URL. If empty, it is
	// constructed from AzureAccount as https://{account}.blob.core.windows.net.
	AzureAccountURL string `yaml:"azure_account_url"`
	// AzurePrefix is the optional key prefix for all objects in the upstream Azure container.
	AzurePrefix string `yaml:"azure_prefix"`
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
			Host:   "0.0.0.0",
			Port:   9000,
			Region: "us-east-1",
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
	if cfg.Storage.Backend == "" {
		cfg.Storage.Backend = "local"
	}
	if cfg.Storage.Local.RootDir == "" {
		cfg.Storage.Local.RootDir = "./data/objects"
	}
}
