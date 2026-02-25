package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
)

// realAzureClient wraps the official Azure SDK client to satisfy AzureBlobAPI.
type realAzureClient struct {
	client *azblob.Client
}

// newRealAzureClient creates a real Azure Blob client. If connectionString is
// non-empty, it uses connection string auth. If useManagedIdentity is true, it
// uses managed identity credentials. Otherwise it falls back to
// DefaultAzureCredential.
func newRealAzureClient(accountURL, connectionString string, useManagedIdentity bool) (*realAzureClient, error) {
	if connectionString != "" {
		client, err := azblob.NewClientFromConnectionString(connectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("creating Azure Blob client from connection string: %w", err)
		}
		return &realAzureClient{client: client}, nil
	}

	if useManagedIdentity {
		cred, err := azidentity.NewManagedIdentityCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("creating Azure managed identity credential: %w", err)
		}
		client, err := azblob.NewClient(accountURL, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("creating Azure Blob client with managed identity: %w", err)
		}
		return &realAzureClient{client: client}, nil
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("creating Azure credential: %w", err)
	}

	client, err := azblob.NewClient(accountURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("creating Azure Blob client: %w", err)
	}

	return &realAzureClient{client: client}, nil
}

func (c *realAzureClient) UploadBlob(ctx context.Context, containerName, blobName string, data []byte) error {
	_, err := c.client.UploadBuffer(ctx, containerName, blobName, data, nil)
	return err
}

func (c *realAzureClient) DownloadBlob(ctx context.Context, containerName, blobName string) ([]byte, error) {
	resp, err := c.client.DownloadStream(ctx, containerName, blobName, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (c *realAzureClient) DeleteBlob(ctx context.Context, containerName, blobName string) error {
	_, err := c.client.DeleteBlob(ctx, containerName, blobName, nil)
	return err
}

func (c *realAzureClient) BlobExists(ctx context.Context, containerName, blobName string) (bool, error) {
	_, err := c.client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName).GetProperties(ctx, nil)
	if err != nil {
		if isAzureNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *realAzureClient) GetBlobProperties(ctx context.Context, containerName, blobName string) (int64, error) {
	resp, err := c.client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName).GetProperties(ctx, nil)
	if err != nil {
		return 0, err
	}
	if resp.ContentLength != nil {
		return *resp.ContentLength, nil
	}
	return 0, nil
}

func (c *realAzureClient) StartCopyFromURL(ctx context.Context, containerName, blobName, sourceURL string) error {
	_, err := c.client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName).StartCopyFromURL(ctx, sourceURL, nil)
	return err
}

func (c *realAzureClient) StageBlock(ctx context.Context, containerName, blobName, blockID string, data []byte) error {
	bbClient := c.client.ServiceClient().NewContainerClient(containerName).NewBlockBlobClient(blobName)
	body := streaming.NopCloser(bytes.NewReader(data))
	_, err := bbClient.StageBlock(ctx, blockID, body, nil)
	return err
}

func (c *realAzureClient) CommitBlockList(ctx context.Context, containerName, blobName string, blockIDs []string) error {
	bbClient := c.client.ServiceClient().NewContainerClient(containerName).NewBlockBlobClient(blobName)
	_, err := bbClient.CommitBlockList(ctx, blockIDs, &blockblob.CommitBlockListOptions{})
	return err
}
