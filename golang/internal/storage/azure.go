// Package storage provides the Azure Blob Storage gateway backend for BleepStore.
//
// The Azure gateway backend proxies all data operations to an upstream Azure
// Blob Storage container via the official Azure SDK for Go. Metadata stays
// in local SQLite -- this backend handles raw bytes only.
//
// Key mapping:
//
//	Objects:  {prefix}{bleepstore_bucket}/{key}
//
// Multipart strategy uses Azure Block Blob primitives:
//
//	put_part()       → StageBlock() on the final blob (no temp objects)
//	assemble_parts() → CommitBlockList() to finalize
//	delete_parts()   → no-op (uncommitted blocks auto-expire in 7 days)
//
// Credentials are resolved via DefaultAzureCredential (env vars, managed
// identity, Azure CLI, etc.).
package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// AzureBlobAPI defines the subset of the Azure Blob Storage client interface
// that the gateway backend uses. This allows mocking in tests.
type AzureBlobAPI interface {
	// UploadBlob uploads data to a blob, overwriting if it already exists.
	UploadBlob(ctx context.Context, containerName, blobName string, data []byte) error
	// DownloadBlob downloads a blob's contents.
	DownloadBlob(ctx context.Context, containerName, blobName string) ([]byte, error)
	// DeleteBlob deletes a blob. Returns an error if the blob does not exist.
	DeleteBlob(ctx context.Context, containerName, blobName string) error
	// BlobExists checks if a blob exists.
	BlobExists(ctx context.Context, containerName, blobName string) (bool, error)
	// GetBlobProperties retrieves the size of a blob.
	GetBlobProperties(ctx context.Context, containerName, blobName string) (int64, error)
	// StartCopyFromURL copies a blob from a source URL.
	StartCopyFromURL(ctx context.Context, containerName, blobName, sourceURL string) error
	// StageBlock stages a block on a blob for later commit.
	StageBlock(ctx context.Context, containerName, blobName, blockID string, data []byte) error
	// CommitBlockList commits a list of block IDs to finalize a blob.
	CommitBlockList(ctx context.Context, containerName, blobName string, blockIDs []string) error
}

// AzureGatewayBackend implements the StorageBackend interface by proxying
// storage operations to Azure Blob Storage. This allows BleepStore to act
// as an S3-compatible gateway in front of Azure Blob.
//
// All BleepStore buckets/objects are stored under a single upstream Azure
// container with a key prefix to namespace them.
type AzureGatewayBackend struct {
	// Container is the upstream Azure Blob container name.
	Container string
	// AccountURL is the Azure storage account URL (e.g. https://account.blob.core.windows.net).
	AccountURL string
	// Prefix is the key prefix for all blobs in the upstream container.
	Prefix string
	// client is the Azure Blob client (satisfying AzureBlobAPI interface).
	client AzureBlobAPI
}

// NewAzureGatewayBackend creates a new AzureGatewayBackend configured to proxy
// to the specified Azure Blob container. It initializes the Azure SDK client
// using DefaultAzureCredential.
func NewAzureGatewayBackend(ctx context.Context, container, accountURL, prefix string) (*AzureGatewayBackend, error) {
	client, err := newRealAzureClient(accountURL)
	if err != nil {
		return nil, fmt.Errorf("creating Azure client: %w", err)
	}

	b := &AzureGatewayBackend{
		Container:  container,
		AccountURL: accountURL,
		Prefix:     prefix,
		client:     client,
	}

	// Verify the upstream container is accessible by checking if a non-existent blob exists.
	_, err = b.client.BlobExists(ctx, container, "\x00nonexistent\x00")
	if err != nil {
		return nil, fmt.Errorf("cannot access upstream Azure container %q: %w", container, err)
	}

	slog.Info("Azure gateway backend initialized", "container", container, "account", accountURL, "prefix", prefix)
	return b, nil
}

// NewAzureGatewayBackendWithClient creates an AzureGatewayBackend with a
// pre-configured Azure client. This is primarily used for testing with mock
// clients.
func NewAzureGatewayBackendWithClient(container, accountURL, prefix string, client AzureBlobAPI) *AzureGatewayBackend {
	return &AzureGatewayBackend{
		Container:  container,
		AccountURL: accountURL,
		Prefix:     prefix,
		client:     client,
	}
}

// blobName maps a BleepStore bucket/key to an upstream Azure blob name.
func (b *AzureGatewayBackend) blobName(bucket, key string) string {
	return b.Prefix + bucket + "/" + key
}

// blockID generates a block ID for Azure staged blocks.
// Block IDs must be base64-encoded and the same length for all blocks
// in a blob. Includes uploadID to avoid collisions between concurrent
// multipart uploads to the same key.
func blockID(uploadID string, partNumber int) string {
	return base64.StdEncoding.EncodeToString(
		[]byte(fmt.Sprintf("%s:%05d", uploadID, partNumber)),
	)
}

// PutObject uploads object data to the upstream Azure Blob container. It reads
// all data, computes MD5 locally for a consistent ETag, then uploads to Azure.
func (b *AzureGatewayBackend) PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64) (int64, string, error) {
	blobKey := b.blobName(bucket, key)

	// Read all data to compute MD5 locally. Azure may return different ETags,
	// so we compute our own for consistency.
	data, err := io.ReadAll(reader)
	if err != nil {
		return 0, "", fmt.Errorf("reading object data: %w", err)
	}

	h := md5.New()
	h.Write(data)
	etag := fmt.Sprintf(`"%x"`, h.Sum(nil))

	if err := b.client.UploadBlob(ctx, b.Container, blobKey, data); err != nil {
		return 0, "", fmt.Errorf("uploading to Azure Blob: %w", err)
	}

	return int64(len(data)), etag, nil
}

// GetObject retrieves object data from the upstream Azure Blob container.
// Returns the data stream, the object size, and an empty ETag (metadata store
// holds the authoritative ETag). The caller is responsible for closing the
// returned ReadCloser.
func (b *AzureGatewayBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, int64, string, error) {
	blobKey := b.blobName(bucket, key)

	// Get properties first for size.
	blobSize, err := b.client.GetBlobProperties(ctx, b.Container, blobKey)
	if err != nil {
		if isAzureNotFound(err) {
			return nil, 0, "", fmt.Errorf("object not found: %s/%s", bucket, key)
		}
		return nil, 0, "", fmt.Errorf("getting blob properties from Azure: %w", err)
	}

	data, err := b.client.DownloadBlob(ctx, b.Container, blobKey)
	if err != nil {
		if isAzureNotFound(err) {
			return nil, 0, "", fmt.Errorf("object not found: %s/%s", bucket, key)
		}
		return nil, 0, "", fmt.Errorf("getting object from Azure Blob: %w", err)
	}

	return io.NopCloser(bytes.NewReader(data)), blobSize, "", nil
}

// DeleteObject removes an object from the upstream Azure Blob container.
// Idempotent: catches not-found silently.
func (b *AzureGatewayBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	blobKey := b.blobName(bucket, key)

	err := b.client.DeleteBlob(ctx, b.Container, blobKey)
	if err != nil {
		if isAzureNotFound(err) {
			return nil // Idempotent: treat as success
		}
		return fmt.Errorf("deleting object from Azure Blob: %w", err)
	}
	return nil
}

// CopyObject copies an object within the upstream Azure Blob container using
// Azure server-side copy. Downloads the result to compute MD5 for a consistent
// ETag.
func (b *AzureGatewayBackend) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (string, error) {
	srcBlobName := b.blobName(srcBucket, srcKey)
	dstBlobName := b.blobName(dstBucket, dstKey)

	// Build source URL.
	sourceURL := fmt.Sprintf("%s/%s/%s", b.AccountURL, b.Container, srcBlobName)

	err := b.client.StartCopyFromURL(ctx, b.Container, dstBlobName, sourceURL)
	if err != nil {
		if isAzureNotFound(err) {
			return "", fmt.Errorf("source object not found: %s/%s", srcBucket, srcKey)
		}
		return "", fmt.Errorf("copying object in Azure Blob: %w", err)
	}

	// Download destination to compute MD5 for consistent ETag.
	data, err := b.client.DownloadBlob(ctx, b.Container, dstBlobName)
	if err != nil {
		return "", fmt.Errorf("reading copied object for ETag: %w", err)
	}

	h := md5.New()
	h.Write(data)
	etag := fmt.Sprintf(`"%x"`, h.Sum(nil))
	return etag, nil
}

// PutPart stages a block on the final blob (Azure Block Blob multipart).
// Unlike AWS/GCP, parts are staged directly on the final blob using
// StageBlock(). No temporary objects are created. Uncommitted blocks
// auto-expire in 7 days.
//
// Computes MD5 locally for a consistent ETag.
func (b *AzureGatewayBackend) PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, reader io.Reader, size int64) (string, error) {
	blobKey := b.blobName(bucket, key)
	blkID := blockID(uploadID, partNumber)

	// Read all data to compute MD5 locally.
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("reading part data: %w", err)
	}

	h := md5.New()
	h.Write(data)
	etag := fmt.Sprintf(`"%x"`, h.Sum(nil))

	if err := b.client.StageBlock(ctx, b.Container, blobKey, blkID, data); err != nil {
		return "", fmt.Errorf("staging block in Azure Blob: %w", err)
	}

	return etag, nil
}

// AssembleParts commits staged blocks into the final blob. Builds a block list
// from the upload_id and part numbers, then calls CommitBlockList() to finalize.
// Downloads the result to compute a consistent MD5 ETag.
func (b *AzureGatewayBackend) AssembleParts(ctx context.Context, bucket, key, uploadID string, partNumbers []int) (string, error) {
	blobKey := b.blobName(bucket, key)

	blockIDs := make([]string, len(partNumbers))
	for i, pn := range partNumbers {
		blockIDs[i] = blockID(uploadID, pn)
	}

	if err := b.client.CommitBlockList(ctx, b.Container, blobKey, blockIDs); err != nil {
		return "", fmt.Errorf("committing block list in Azure Blob: %w", err)
	}

	// Download the committed blob to compute MD5.
	data, err := b.client.DownloadBlob(ctx, b.Container, blobKey)
	if err != nil {
		return "", fmt.Errorf("reading assembled object for ETag: %w", err)
	}

	h := md5.New()
	h.Write(data)
	etag := fmt.Sprintf(`"%x"`, h.Sum(nil))
	return etag, nil
}

// DeleteParts is a no-op for Azure. Uncommitted Azure blocks auto-expire
// in 7 days. Unlike AWS/GCP, there are no temporary part objects to clean up.
func (b *AzureGatewayBackend) DeleteParts(ctx context.Context, bucket, key, uploadID string) error {
	// No-op: Azure automatically garbage-collects uncommitted blocks.
	return nil
}

// CreateBucket is a no-op for the Azure gateway backend. All BleepStore
// buckets share a single upstream Azure container with key prefixes, so
// there is nothing to create on the Azure side.
func (b *AzureGatewayBackend) CreateBucket(ctx context.Context, bucket string) error {
	return nil
}

// DeleteBucket is a no-op for the Azure gateway backend. Bucket data is scoped
// by key prefix, so there is nothing to delete on the Azure side. The metadata
// store handles the actual bucket record deletion.
func (b *AzureGatewayBackend) DeleteBucket(ctx context.Context, bucket string) error {
	return nil
}

// ObjectExists checks whether an object exists in the upstream Azure Blob container.
func (b *AzureGatewayBackend) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	blobKey := b.blobName(bucket, key)

	exists, err := b.client.BlobExists(ctx, b.Container, blobKey)
	if err != nil {
		return false, fmt.Errorf("checking object existence in Azure Blob: %w", err)
	}
	return exists, nil
}

// HealthCheck verifies that the upstream Azure Blob container is accessible.
func (b *AzureGatewayBackend) HealthCheck(ctx context.Context) error {
	_, err := b.client.BlobExists(ctx, b.Container, "\x00nonexistent\x00")
	return err
}

// isAzureNotFound checks if an Azure error is a not-found error.
func isAzureNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "not found") || strings.Contains(msg, "404") ||
		strings.Contains(msg, "blobnotfound") || strings.Contains(msg, "containernotfound") ||
		strings.Contains(msg, "the specified blob does not exist") ||
		strings.Contains(msg, "the specified container does not exist") {
		return true
	}
	return false
}

// Ensure AzureGatewayBackend implements StorageBackend at compile time.
var _ StorageBackend = (*AzureGatewayBackend)(nil)
