// Package storage provides the GCP Cloud Storage gateway backend for BleepStore.
//
// The GCP gateway backend proxies all data operations to an upstream GCS
// bucket via the official Go Cloud Storage client library. Metadata stays
// in local SQLite -- this backend handles raw bytes only.
//
// Key mapping:
//
//	Objects:  {prefix}{bleepstore_bucket}/{key}
//	Parts:    {prefix}.parts/{upload_id}/{part_number}
//
// Credentials are resolved via Application Default Credentials
// (GOOGLE_APPLICATION_CREDENTIALS, gcloud auth, metadata server).
package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// maxComposeSources is the GCS limit on the number of source objects per
// Compose call.
const maxComposeSources = 32

// GCSAPI defines the subset of the GCS client interface that the gateway
// backend uses. This allows mocking in tests.
type GCSAPI interface {
	// NewWriter returns a writer for the given GCS object.
	NewWriter(ctx context.Context, bucket, object string) GCSWriter
	// NewReader returns a reader for the given GCS object.
	NewReader(ctx context.Context, bucket, object string) (io.ReadCloser, error)
	// Delete deletes the given GCS object.
	Delete(ctx context.Context, bucket, object string) error
	// Attrs returns the attributes of the given GCS object.
	Attrs(ctx context.Context, bucket, object string) (*GCSAttrs, error)
	// Copy copies a GCS object from src to dst within the same bucket.
	Copy(ctx context.Context, bucket, srcObject, dstObject string) (*GCSAttrs, error)
	// Compose composes multiple GCS source objects into a single destination object.
	Compose(ctx context.Context, bucket, dstObject string, srcObjects []string) (*GCSAttrs, error)
	// ListObjects lists objects with the given prefix.
	ListObjects(ctx context.Context, bucket, prefix string) ([]string, error)
}

// GCSWriter is a writer interface for writing to GCS objects.
type GCSWriter interface {
	io.WriteCloser
}

// GCSAttrs holds object attributes returned from GCS operations.
type GCSAttrs struct {
	Size int64
	MD5  []byte // raw MD5 hash bytes
}

// realGCSClient wraps the official GCS client to satisfy GCSAPI.
type realGCSClient struct {
	client *gcs.Client
}

func (c *realGCSClient) NewWriter(ctx context.Context, bucket, object string) GCSWriter {
	return c.client.Bucket(bucket).Object(object).NewWriter(ctx)
}

func (c *realGCSClient) NewReader(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	return c.client.Bucket(bucket).Object(object).NewReader(ctx)
}

func (c *realGCSClient) Delete(ctx context.Context, bucket, object string) error {
	return c.client.Bucket(bucket).Object(object).Delete(ctx)
}

func (c *realGCSClient) Attrs(ctx context.Context, bucket, object string) (*GCSAttrs, error) {
	attrs, err := c.client.Bucket(bucket).Object(object).Attrs(ctx)
	if err != nil {
		return nil, err
	}
	return &GCSAttrs{
		Size: attrs.Size,
		MD5:  attrs.MD5,
	}, nil
}

func (c *realGCSClient) Copy(ctx context.Context, bucket, srcObject, dstObject string) (*GCSAttrs, error) {
	src := c.client.Bucket(bucket).Object(srcObject)
	dst := c.client.Bucket(bucket).Object(dstObject)
	attrs, err := dst.CopierFrom(src).Run(ctx)
	if err != nil {
		return nil, err
	}
	return &GCSAttrs{
		Size: attrs.Size,
		MD5:  attrs.MD5,
	}, nil
}

func (c *realGCSClient) Compose(ctx context.Context, bucket, dstObject string, srcObjects []string) (*GCSAttrs, error) {
	dst := c.client.Bucket(bucket).Object(dstObject)
	var srcs []*gcs.ObjectHandle
	for _, name := range srcObjects {
		srcs = append(srcs, c.client.Bucket(bucket).Object(name))
	}
	attrs, err := dst.ComposerFrom(srcs...).Run(ctx)
	if err != nil {
		return nil, err
	}
	return &GCSAttrs{
		Size: attrs.Size,
		MD5:  attrs.MD5,
	}, nil
}

func (c *realGCSClient) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	it := c.client.Bucket(bucket).Objects(ctx, &gcs.Query{Prefix: prefix})
	var names []string
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		names = append(names, attrs.Name)
	}
	return names, nil
}

// GCPGatewayBackend implements the StorageBackend interface by proxying
// storage operations to Google Cloud Storage. This allows BleepStore to
// act as an S3-compatible gateway in front of GCS.
//
// All BleepStore buckets/objects are stored under a single upstream GCS bucket
// with a key prefix to namespace them.
type GCPGatewayBackend struct {
	// Bucket is the upstream GCS bucket name.
	Bucket string
	// Project is the GCP project ID.
	Project string
	// Prefix is the key prefix for all objects in the upstream bucket.
	Prefix string
	// client is the GCS client (satisfying GCSAPI interface).
	client GCSAPI
}

// NewGCPGatewayBackend creates a new GCPGatewayBackend configured to proxy
// to the specified GCS bucket. It initializes the GCS client using
// Application Default Credentials.
func NewGCPGatewayBackend(ctx context.Context, bucket, project, prefix string) (*GCPGatewayBackend, error) {
	client, err := gcs.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}

	b := &GCPGatewayBackend{
		Bucket:  bucket,
		Project: project,
		Prefix:  prefix,
		client:  &realGCSClient{client: client},
	}

	// Verify the upstream bucket is accessible by listing with a small limit.
	_, err = b.client.ListObjects(ctx, bucket, "\x00nonexistent\x00")
	if err != nil {
		return nil, fmt.Errorf("cannot access upstream GCS bucket %q: %w", bucket, err)
	}

	log.Printf("GCP gateway backend initialized: bucket=%s project=%s prefix=%q", bucket, project, prefix)
	return b, nil
}

// NewGCPGatewayBackendWithClient creates a GCPGatewayBackend with a
// pre-configured GCS client. This is primarily used for testing with mock
// clients.
func NewGCPGatewayBackendWithClient(bucket, project, prefix string, client GCSAPI) *GCPGatewayBackend {
	return &GCPGatewayBackend{
		Bucket:  bucket,
		Project: project,
		Prefix:  prefix,
		client:  client,
	}
}

// gcsKey maps a BleepStore bucket/key to an upstream GCS object name.
func (b *GCPGatewayBackend) gcsKey(bucket, key string) string {
	return b.Prefix + bucket + "/" + key
}

// partKey maps a multipart part to an upstream GCS object name.
func (b *GCPGatewayBackend) partKey(uploadID string, partNumber int) string {
	return fmt.Sprintf("%s.parts/%s/%d", b.Prefix, uploadID, partNumber)
}

// PutObject uploads object data to the upstream GCS bucket. It reads all data,
// computes MD5 locally for a consistent ETag, then uploads to GCS.
func (b *GCPGatewayBackend) PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64) (int64, string, error) {
	gcsName := b.gcsKey(bucket, key)

	// Read all data to compute MD5 locally. GCS may return different ETags
	// or no MD5 for composite objects, so we compute our own.
	data, err := io.ReadAll(reader)
	if err != nil {
		return 0, "", fmt.Errorf("reading object data: %w", err)
	}

	h := md5.New()
	h.Write(data)
	etag := fmt.Sprintf(`"%x"`, h.Sum(nil))

	w := b.client.NewWriter(ctx, b.Bucket, gcsName)
	if _, err := io.Copy(w, bytes.NewReader(data)); err != nil {
		_ = w.Close()
		return 0, "", fmt.Errorf("uploading to GCS: %w", err)
	}
	if err := w.Close(); err != nil {
		return 0, "", fmt.Errorf("finalizing GCS upload: %w", err)
	}

	return int64(len(data)), etag, nil
}

// GetObject retrieves object data from the upstream GCS bucket.
// Returns the data stream, the object size, and an empty ETag (metadata store
// holds the authoritative ETag). The caller is responsible for closing the
// returned ReadCloser.
func (b *GCPGatewayBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, int64, string, error) {
	gcsName := b.gcsKey(bucket, key)

	// Get attributes first for size.
	attrs, err := b.client.Attrs(ctx, b.Bucket, gcsName)
	if err != nil {
		if isGCSNotFound(err) {
			return nil, 0, "", fmt.Errorf("object not found: %s/%s", bucket, key)
		}
		return nil, 0, "", fmt.Errorf("getting object attrs from GCS: %w", err)
	}

	reader, err := b.client.NewReader(ctx, b.Bucket, gcsName)
	if err != nil {
		if isGCSNotFound(err) {
			return nil, 0, "", fmt.Errorf("object not found: %s/%s", bucket, key)
		}
		return nil, 0, "", fmt.Errorf("getting object from GCS: %w", err)
	}

	return reader, attrs.Size, "", nil
}

// DeleteObject removes an object from the upstream GCS bucket.
// Idempotent: catches 404 silently (GCS errors on delete of non-existent
// objects unlike S3).
func (b *GCPGatewayBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	gcsName := b.gcsKey(bucket, key)

	err := b.client.Delete(ctx, b.Bucket, gcsName)
	if err != nil {
		if isGCSNotFound(err) {
			return nil // Idempotent: treat as success
		}
		return fmt.Errorf("deleting object from GCS: %w", err)
	}
	return nil
}

// CopyObject copies an object within the upstream GCS bucket using GCS
// server-side copy. Downloads the result to compute MD5 for a consistent ETag.
func (b *GCPGatewayBackend) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (string, error) {
	srcGCSName := b.gcsKey(srcBucket, srcKey)
	dstGCSName := b.gcsKey(dstBucket, dstKey)

	_, err := b.client.Copy(ctx, b.Bucket, srcGCSName, dstGCSName)
	if err != nil {
		if isGCSNotFound(err) {
			return "", fmt.Errorf("source object not found: %s/%s", srcBucket, srcKey)
		}
		return "", fmt.Errorf("copying object in GCS: %w", err)
	}

	// Download the copied object to compute MD5 for consistent ETag.
	reader, err := b.client.NewReader(ctx, b.Bucket, dstGCSName)
	if err != nil {
		return "", fmt.Errorf("reading copied object for ETag: %w", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		return "", fmt.Errorf("reading copied object data: %w", err)
	}

	h := md5.New()
	h.Write(data)
	etag := fmt.Sprintf(`"%x"`, h.Sum(nil))
	return etag, nil
}

// PutPart stores a multipart upload part as a temporary GCS object.
// Parts are stored at {prefix}.parts/{upload_id}/{part_number}.
// Computes MD5 locally for a consistent ETag.
func (b *GCPGatewayBackend) PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, reader io.Reader, size int64) (string, error) {
	pk := b.partKey(uploadID, partNumber)

	// Read all data to compute MD5 locally.
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("reading part data: %w", err)
	}

	h := md5.New()
	h.Write(data)
	etag := fmt.Sprintf(`"%x"`, h.Sum(nil))

	w := b.client.NewWriter(ctx, b.Bucket, pk)
	if _, err := io.Copy(w, bytes.NewReader(data)); err != nil {
		_ = w.Close()
		return "", fmt.Errorf("uploading part to GCS: %w", err)
	}
	if err := w.Close(); err != nil {
		return "", fmt.Errorf("finalizing part upload to GCS: %w", err)
	}

	return etag, nil
}

// AssembleParts composes the specified parts into a single GCS object using
// GCS Compose. GCS compose supports at most 32 source objects per call.
// For >32 parts, chains compose in batches of 32: compose each batch into
// an intermediate object, then compose the intermediates, repeating until
// a single object remains.
//
// Returns the composite ETag computed by downloading the final object.
func (b *GCPGatewayBackend) AssembleParts(ctx context.Context, bucket, key, uploadID string, partNumbers []int) (string, error) {
	finalName := b.gcsKey(bucket, key)
	sourceNames := make([]string, len(partNumbers))
	for i, pn := range partNumbers {
		sourceNames[i] = b.partKey(uploadID, pn)
	}

	if len(sourceNames) <= maxComposeSources {
		// Simple case: single compose call.
		_, err := b.client.Compose(ctx, b.Bucket, finalName, sourceNames)
		if err != nil {
			return "", fmt.Errorf("composing parts in GCS: %w", err)
		}
	} else {
		// Chain compose in batches of 32.
		intermediates, err := b.chainCompose(ctx, sourceNames, finalName)
		if err != nil {
			return "", err
		}
		// Clean up intermediate composite objects.
		for _, name := range intermediates {
			if delErr := b.client.Delete(ctx, b.Bucket, name); delErr != nil {
				log.Printf("Warning: failed to clean up intermediate: %s: %v", name, delErr)
			}
		}
	}

	// Compute MD5 of the final assembled object by downloading it.
	reader, err := b.client.NewReader(ctx, b.Bucket, finalName)
	if err != nil {
		return "", fmt.Errorf("reading assembled object for ETag: %w", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		return "", fmt.Errorf("reading assembled object data: %w", err)
	}

	h := md5.New()
	h.Write(data)
	etag := fmt.Sprintf(`"%x"`, h.Sum(nil))
	return etag, nil
}

// chainCompose chains GCS compose calls for >32 sources.
// Returns a list of intermediate object names that should be cleaned up.
func (b *GCPGatewayBackend) chainCompose(ctx context.Context, sourceNames []string, finalName string) ([]string, error) {
	var allIntermediates []string
	currentSources := sourceNames

	generation := 0
	for len(currentSources) > maxComposeSources {
		var nextSources []string
		for i := 0; i < len(currentSources); i += maxComposeSources {
			end := i + maxComposeSources
			if end > len(currentSources) {
				end = len(currentSources)
			}
			batch := currentSources[i:end]
			if len(batch) == 1 {
				// Single source: no compose needed, just pass through.
				nextSources = append(nextSources, batch[0])
				continue
			}
			intermediateName := fmt.Sprintf("%s.__compose_tmp_%d_%d", finalName, generation, i)
			_, err := b.client.Compose(ctx, b.Bucket, intermediateName, batch)
			if err != nil {
				return allIntermediates, fmt.Errorf("composing intermediate batch (gen=%d, offset=%d): %w", generation, i, err)
			}
			nextSources = append(nextSources, intermediateName)
			allIntermediates = append(allIntermediates, intermediateName)
		}
		currentSources = nextSources
		generation++
	}

	// Final compose.
	_, err := b.client.Compose(ctx, b.Bucket, finalName, currentSources)
	if err != nil {
		return allIntermediates, fmt.Errorf("final compose in GCS: %w", err)
	}
	return allIntermediates, nil
}

// DeleteParts removes all temporary part objects for a multipart upload.
// Lists objects under .parts/{upload_id}/ and deletes each one.
func (b *GCPGatewayBackend) DeleteParts(ctx context.Context, bucket, key, uploadID string) error {
	prefix := b.Prefix + ".parts/" + uploadID + "/"

	names, err := b.client.ListObjects(ctx, b.Bucket, prefix)
	if err != nil {
		return fmt.Errorf("listing parts for upload %s: %w", uploadID, err)
	}

	for _, name := range names {
		if delErr := b.client.Delete(ctx, b.Bucket, name); delErr != nil {
			if !isGCSNotFound(delErr) {
				return fmt.Errorf("deleting part %s: %w", name, delErr)
			}
		}
	}

	return nil
}

// CreateBucket is a no-op for the GCP gateway backend. All BleepStore buckets
// share a single upstream GCS bucket with key prefixes, so there is nothing
// to create on the GCS side.
func (b *GCPGatewayBackend) CreateBucket(ctx context.Context, bucket string) error {
	return nil
}

// DeleteBucket is a no-op for the GCP gateway backend. Bucket data is scoped
// by key prefix, so there is nothing to delete on the GCS side. The metadata
// store handles the actual bucket record deletion.
func (b *GCPGatewayBackend) DeleteBucket(ctx context.Context, bucket string) error {
	return nil
}

// ObjectExists checks whether an object exists in the upstream GCS bucket.
func (b *GCPGatewayBackend) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	gcsName := b.gcsKey(bucket, key)

	_, err := b.client.Attrs(ctx, b.Bucket, gcsName)
	if err != nil {
		if isGCSNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("checking object existence in GCS: %w", err)
	}
	return true, nil
}

// isGCSNotFound checks if a GCS error is a 404/not-found error.
func isGCSNotFound(err error) bool {
	if errors.Is(err, gcs.ErrObjectNotExist) {
		return true
	}
	if errors.Is(err, gcs.ErrBucketNotExist) {
		return true
	}
	// Check error message as fallback.
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "not found") || strings.Contains(msg, "404") {
			return true
		}
	}
	return false
}

// Ensure GCPGatewayBackend implements StorageBackend at compile time.
var _ StorageBackend = (*GCPGatewayBackend)(nil)
