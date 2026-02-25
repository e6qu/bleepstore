// Package storage defines the interface and implementations for BleepStore's
// object data storage layer.
package storage

import (
	"context"
	"io"
)

// StorageBackend defines the interface for reading and writing raw object data.
// Implementations provide the underlying storage mechanism (local filesystem,
// cloud provider, etc.). All methods must be safe for concurrent use.
type StorageBackend interface {
	// PutObject writes the data from the reader to the storage backend at the
	// specified bucket and key. It returns the number of bytes written and the
	// computed ETag (typically an MD5 hex digest), or an error.
	PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64) (bytesWritten int64, etag string, err error)

	// GetObject retrieves the object data from the storage backend. The caller
	// is responsible for closing the returned ReadCloser. Returns the data
	// stream, the object size in bytes, and the ETag.
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, int64, string, error)

	// DeleteObject removes the object data from the storage backend.
	DeleteObject(ctx context.Context, bucket, key string) error

	// CopyObject copies an object from the source bucket/key to the destination
	// bucket/key within the storage backend. Returns the new ETag.
	CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (string, error)

	// PutPart writes a single part of a multipart upload.
	PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, reader io.Reader, size int64) (etag string, err error)

	// AssembleParts concatenates the specified parts into a single object.
	// The parts are identified by upload ID and part numbers. Returns the
	// final ETag for the assembled object.
	AssembleParts(ctx context.Context, bucket, key, uploadID string, partNumbers []int) (string, error)

	// DeleteParts removes all parts associated with the given multipart upload.
	DeleteParts(ctx context.Context, bucket, key, uploadID string) error

	// CreateBucket creates the backing storage for a new bucket (e.g., a
	// directory on local disk or a prefix in cloud storage).
	CreateBucket(ctx context.Context, bucket string) error

	// DeleteBucket removes the backing storage for a bucket.
	DeleteBucket(ctx context.Context, bucket string) error

	// ObjectExists checks whether an object exists in the storage backend
	// at the specified bucket and key. Returns true if the object exists.
	ObjectExists(ctx context.Context, bucket, key string) (bool, error)

	// HealthCheck verifies that the storage backend is operational.
	HealthCheck(ctx context.Context) error
}
