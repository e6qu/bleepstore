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
		awsBucket := cfg.Storage.AWSBucket
		awsRegion := cfg.Storage.AWSRegion
		awsPrefix := cfg.Storage.AWSPrefix
		if awsBucket == "" {
			fmt.Fprintf(os.Stderr, "storage.aws_bucket is required when backend is 'aws'\n")
			os.Exit(1)
		}
		if awsRegion == "" {
			awsRegion = "us-east-1"
		}
		awsBackend, awsErr := storage.NewAWSGatewayBackend(context.Background(), awsBucket, awsRegion, awsPrefix)
		if awsErr != nil {
			fmt.Fprintf(os.Stderr, "failed to initialize AWS storage backend: %v\n", awsErr)
			os.Exit(1)
		}
		storageBackend = awsBackend
		slog.Info("Storage backend initialized", "backend", "aws", "bucket", awsBucket, "region", awsRegion, "prefix", awsPrefix)
	case "gcp":
		gcpBucket := cfg.Storage.GCPBucket
		gcpProject := cfg.Storage.GCPProject
		gcpPrefix := cfg.Storage.GCPPrefix
		if gcpBucket == "" {
			fmt.Fprintf(os.Stderr, "storage.gcp_bucket is required when backend is 'gcp'\n")
			os.Exit(1)
		}
		gcpBackend, gcpErr := storage.NewGCPGatewayBackend(context.Background(), gcpBucket, gcpProject, gcpPrefix)
		if gcpErr != nil {
			fmt.Fprintf(os.Stderr, "failed to initialize GCP storage backend: %v\n", gcpErr)
			os.Exit(1)
		}
		storageBackend = gcpBackend
		slog.Info("Storage backend initialized", "backend", "gcp", "bucket", gcpBucket, "project", gcpProject, "prefix", gcpPrefix)
	case "azure":
		azureContainer := cfg.Storage.AzureContainer
		azureAccount := cfg.Storage.AzureAccount
		azureAccountURL := cfg.Storage.AzureAccountURL
		azurePrefix := cfg.Storage.AzurePrefix
		if azureContainer == "" {
			fmt.Fprintf(os.Stderr, "storage.azure_container is required when backend is 'azure'\n")
			os.Exit(1)
		}
		// Construct account URL from account name if not explicitly set.
		if azureAccountURL == "" {
			if azureAccount == "" {
				fmt.Fprintf(os.Stderr, "storage.azure_account or storage.azure_account_url is required when backend is 'azure'\n")
				os.Exit(1)
			}
			azureAccountURL = fmt.Sprintf("https://%s.blob.core.windows.net", azureAccount)
		}
		azureBackend, azureErr := storage.NewAzureGatewayBackend(context.Background(), azureContainer, azureAccountURL, azurePrefix)
		if azureErr != nil {
			fmt.Fprintf(os.Stderr, "failed to initialize Azure storage backend: %v\n", azureErr)
			os.Exit(1)
		}
		storageBackend = azureBackend
		slog.Info("Storage backend initialized", "backend", "azure", "container", azureContainer, "account", azureAccountURL, "prefix", azurePrefix)
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
