// Package main is the entry point for the BleepStore S3-compatible object storage server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/bleepstore/bleepstore/internal/config"
	"github.com/bleepstore/bleepstore/internal/logging"
	"github.com/bleepstore/bleepstore/internal/metadata"
	"github.com/bleepstore/bleepstore/internal/metrics"
	"github.com/bleepstore/bleepstore/internal/server"
	"github.com/bleepstore/bleepstore/internal/storage"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to configuration file")
	port := flag.Int("port", 0, "override listening port (default: from config or 9000)")
	host := flag.String("host", "", "override listening host (default: from config or 0.0.0.0)")
	logLevel := flag.String("log-level", "", "log level: debug, info, warn, error (default: from config or info)")
	logFormat := flag.String("log-format", "", "log format: text, json (default: from config or text)")
	shutdownTimeout := flag.Int("shutdown-timeout", 0, "graceful shutdown timeout in seconds (default: from config or 30)")
	maxObjectSize := flag.Int64("max-object-size", 0, "maximum object size in bytes (default: from config or 5368709120)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Command-line flags override config file values.
	if *port != 0 {
		cfg.Server.Port = *port
	}
	if *host != "" {
		cfg.Server.Host = *host
	}
	if *logLevel != "" {
		cfg.Logging.Level = *logLevel
	}
	if *logFormat != "" {
		cfg.Logging.Format = *logFormat
	}
	if *shutdownTimeout != 0 {
		cfg.Server.ShutdownTimeout = *shutdownTimeout
	}
	if *maxObjectSize != 0 {
		cfg.Server.MaxObjectSize = *maxObjectSize
	}

	// Initialize structured logging.
	logging.Setup(cfg.Logging.Level, cfg.Logging.Format, os.Stderr)

	// Crash-only design: every startup is recovery.
	// No special recovery mode. Steps that would normally be "recovery" run on
	// every boot:
	// - SQLite WAL auto-recovers on open
	// - Temp file cleanup (below)
	// - Expired multipart reaping (Stage 7)
	// - Default credential seeding (below)

	// Initialize SQLite metadata store.
	dbPath := cfg.Metadata.SQLite.Path
	// Ensure parent directory exists.
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create metadata directory: %v\n", err)
		os.Exit(1)
	}
	metaStore, err := metadata.NewSQLiteStore(dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize metadata store: %v\n", err)
		os.Exit(1)
	}
	defer metaStore.Close()

	// Seed default credentials (idempotent — crash-only recovery step).
	if err := seedDefaultCredentials(metaStore, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "failed to seed credentials: %v\n", err)
		os.Exit(1)
	}

	// Initialize storage backend based on config.
	var storageBackend storage.StorageBackend
	switch cfg.Storage.Backend {
	case "aws":
		awsCfg := cfg.Storage.AWS
		if awsCfg.Bucket == "" {
			fmt.Fprintf(os.Stderr, "storage.aws.bucket is required when backend is 'aws'\n")
			os.Exit(1)
		}
		awsRegion := awsCfg.Region
		if awsRegion == "" {
			awsRegion = "us-east-1"
		}
		awsBackend, awsErr := storage.NewAWSGatewayBackend(context.Background(), awsCfg.Bucket, awsRegion, awsCfg.Prefix, awsCfg.EndpointURL, awsCfg.UsePathStyle, awsCfg.AccessKeyID, awsCfg.SecretAccessKey)
		if awsErr != nil {
			fmt.Fprintf(os.Stderr, "failed to initialize AWS storage backend: %v\n", awsErr)
			os.Exit(1)
		}
		storageBackend = awsBackend
		slog.Info("Storage backend initialized", "backend", "aws", "bucket", awsCfg.Bucket, "region", awsRegion, "prefix", awsCfg.Prefix)
	case "gcp":
		gcpCfg := cfg.Storage.GCP
		if gcpCfg.Bucket == "" {
			fmt.Fprintf(os.Stderr, "storage.gcp.bucket is required when backend is 'gcp'\n")
			os.Exit(1)
		}
		gcpBackend, gcpErr := storage.NewGCPGatewayBackend(context.Background(), gcpCfg.Bucket, gcpCfg.Project, gcpCfg.Prefix, gcpCfg.CredentialsFile)
		if gcpErr != nil {
			fmt.Fprintf(os.Stderr, "failed to initialize GCP storage backend: %v\n", gcpErr)
			os.Exit(1)
		}
		storageBackend = gcpBackend
		slog.Info("Storage backend initialized", "backend", "gcp", "bucket", gcpCfg.Bucket, "project", gcpCfg.Project, "prefix", gcpCfg.Prefix)
	case "azure":
		azureCfg := cfg.Storage.Azure
		if azureCfg.Container == "" {
			fmt.Fprintf(os.Stderr, "storage.azure.container is required when backend is 'azure'\n")
			os.Exit(1)
		}
		azureAccountURL := azureCfg.AccountURL
		if azureAccountURL == "" {
			if azureCfg.Account == "" {
				fmt.Fprintf(os.Stderr, "storage.azure.account or storage.azure.account_url is required when backend is 'azure'\n")
				os.Exit(1)
			}
			azureAccountURL = fmt.Sprintf("https://%s.blob.core.windows.net", azureCfg.Account)
		}
		azureBackend, azureErr := storage.NewAzureGatewayBackend(context.Background(), azureCfg.Container, azureAccountURL, azureCfg.Prefix, azureCfg.ConnectionString, azureCfg.UseManagedIdentity)
		if azureErr != nil {
			fmt.Fprintf(os.Stderr, "failed to initialize Azure storage backend: %v\n", azureErr)
			os.Exit(1)
		}
		storageBackend = azureBackend
		slog.Info("Storage backend initialized", "backend", "azure", "container", azureCfg.Container, "account", azureAccountURL, "prefix", azureCfg.Prefix)
	case "memory":
		memCfg := cfg.Storage.Memory
		memBackend, memErr := storage.NewMemoryBackend(
			memCfg.MaxSizeBytes,
			memCfg.Persistence,
			memCfg.SnapshotPath,
			memCfg.SnapshotIntervalSeconds,
		)
		if memErr != nil {
			fmt.Fprintf(os.Stderr, "failed to initialize memory storage backend: %v\n", memErr)
			os.Exit(1)
		}
		storageBackend = memBackend
		slog.Info("Storage backend initialized", "backend", "memory",
			"max_size_bytes", memCfg.MaxSizeBytes,
			"persistence", memCfg.Persistence)
	case "sqlite":
		sqliteBackend, sqliteErr := storage.NewSQLiteBackend(cfg.Metadata.SQLite.Path)
		if sqliteErr != nil {
			fmt.Fprintf(os.Stderr, "failed to initialize SQLite storage backend: %v\n", sqliteErr)
			os.Exit(1)
		}
		storageBackend = sqliteBackend
		slog.Info("Storage backend initialized", "backend", "sqlite", "path", cfg.Metadata.SQLite.Path)
	default:
		// Default to local filesystem backend.
		storageRoot := cfg.Storage.Local.RootDir
		if err := os.MkdirAll(storageRoot, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "failed to create storage root directory: %v\n", err)
			os.Exit(1)
		}
		localBackend, localErr := storage.NewLocalBackend(storageRoot)
		if localErr != nil {
			fmt.Fprintf(os.Stderr, "failed to initialize storage backend: %v\n", localErr)
			os.Exit(1)
		}
		// Crash-only recovery: clean orphan temp files from incomplete writes.
		if err := localBackend.CleanTempFiles(); err != nil {
			slog.Warn("Failed to clean temp files", "error", err)
		}
		storageBackend = localBackend
		slog.Info("Storage backend initialized", "backend", "local", "root", storageRoot)
	}

	// Crash-only recovery: reap expired multipart uploads (7-day TTL).
	expired, reapErr := metaStore.ReapExpiredUploads(604800)
	if reapErr != nil {
		slog.Warn("Failed to reap expired multipart uploads", "error", reapErr)
	} else if len(expired) > 0 {
		slog.Info(fmt.Sprintf("Reaped %d expired multipart uploads", len(expired)))
		// Clean up storage files for reaped uploads (local backend only).
		if localBackend, ok := storageBackend.(*storage.LocalBackend); ok {
			for _, u := range expired {
				if err := localBackend.DeleteUploadParts(u.UploadID); err != nil {
					slog.Warn("Failed to clean up parts for reaped upload",
						"upload_id", u.UploadID, "error", err)
				}
			}
		}
	}

	// Conditionally register Prometheus metrics and seed gauges.
	if cfg.Observability.Metrics {
		metrics.Register()
		metrics.ObjectsTotal.Set(0)
		metrics.BucketsTotal.Set(0)
	}

	srv, err := server.New(cfg, metaStore, server.WithStorageBackend(storageBackend))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create server: %v\n", err)
		os.Exit(1)
	}

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)

	// Start the server in a goroutine so we can handle shutdown signals.
	errCh := make(chan error, 1)
	go func() {
		slog.Info("BleepStore listening", "addr", addr)
		if err := srv.ListenAndServe(addr); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	// SIGTERM/SIGINT handler: stop accepting connections, wait for in-flight
	// requests with a timeout, then exit. No cleanup -- crash-only design.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		slog.Info("Received signal, shutting down", "signal", sig)

		// Give in-flight requests time to complete.
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Server.ShutdownTimeout)*time.Second)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			slog.Error("Shutdown error", "error", err)
		}
		slog.Info("Server stopped")

	case err := <-errCh:
		if err != nil {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
			os.Exit(1)
		}
	}
}

// seedDefaultCredentials creates the default credential record from the config
// if it does not already exist. This runs on every startup as part of
// crash-only recovery.
func seedDefaultCredentials(store *metadata.SQLiteStore, cfg *config.Config) error {
	ctx := context.Background()

	// Check if the default credential already exists.
	existing, err := store.GetCredential(ctx, cfg.Auth.AccessKey)
	if err != nil {
		return fmt.Errorf("checking default credential: %w", err)
	}
	if existing != nil {
		// Already seeded. Nothing to do.
		return nil
	}

	cred := &metadata.CredentialRecord{
		AccessKeyID: cfg.Auth.AccessKey,
		SecretKey:   cfg.Auth.SecretKey,
		OwnerID:     cfg.Auth.AccessKey,
		DisplayName: cfg.Auth.AccessKey,
		Active:      true,
		CreatedAt:   time.Now().UTC(),
	}
	if err := store.PutCredential(ctx, cred); err != nil {
		return fmt.Errorf("seeding default credential: %w", err)
	}
	slog.Info("Seeded default credentials", "access_key", cfg.Auth.AccessKey)
	return nil
}
