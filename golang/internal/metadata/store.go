// Package metadata defines the interface and implementations for BleepStore's
// metadata storage layer, which tracks buckets, objects, and multipart uploads.
package metadata

import (
	"context"
	"encoding/json"
	"io"
	"time"
)

// BucketRecord represents the metadata for a single bucket.
type BucketRecord struct {
	Name         string
	Region       string
	OwnerID      string
	OwnerDisplay string
	ACL          json.RawMessage // JSON-serialized ACL
	CreatedAt    time.Time
}

// ObjectRecord represents the metadata for a single stored object.
type ObjectRecord struct {
	Bucket             string
	Key                string
	Size               int64
	ETag               string
	ContentType        string
	ContentEncoding    string
	ContentLanguage    string
	ContentDisposition string
	CacheControl       string
	Expires            string
	StorageClass       string
	ACL                json.RawMessage // JSON-serialized ACL
	UserMetadata       map[string]string
	LastModified       time.Time
	DeleteMarker       bool
}

// MultipartUploadRecord represents the metadata for an in-progress multipart upload.
type MultipartUploadRecord struct {
	UploadID           string
	Bucket             string
	Key                string
	ContentType        string
	ContentEncoding    string
	ContentLanguage    string
	ContentDisposition string
	CacheControl       string
	Expires            string
	StorageClass       string
	ACL                json.RawMessage
	UserMetadata       map[string]string
	OwnerID            string
	OwnerDisplay       string
	InitiatedAt        time.Time
}

// PartRecord represents the metadata for a single uploaded part.
type PartRecord struct {
	UploadID     string
	PartNumber   int
	Size         int64
	ETag         string
	LastModified time.Time
}

// CredentialRecord represents a set of S3 API credentials.
type CredentialRecord struct {
	AccessKeyID string
	SecretKey   string
	OwnerID     string
	DisplayName string
	Active      bool
	CreatedAt   time.Time
}

// ListObjectsOptions specifies filtering and pagination options for listing objects.
type ListObjectsOptions struct {
	Prefix            string
	Delimiter         string
	Marker            string
	StartAfter        string
	ContinuationToken string
	MaxKeys           int
}

// ListObjectsResult holds the result of a list objects operation.
type ListObjectsResult struct {
	Objects               []ObjectRecord
	CommonPrefixes        []string
	IsTruncated           bool
	NextMarker            string
	NextContinuationToken string
}

// ListUploadsOptions specifies filtering and pagination options for listing multipart uploads.
type ListUploadsOptions struct {
	KeyMarker      string
	UploadIDMarker string
	Prefix         string
	Delimiter      string
	MaxUploads     int
}

// ListUploadsResult holds the result of a list multipart uploads operation.
type ListUploadsResult struct {
	Uploads            []MultipartUploadRecord
	CommonPrefixes     []string
	IsTruncated        bool
	NextKeyMarker      string
	NextUploadIDMarker string
}

// ListPartsOptions specifies filtering and pagination options for listing parts.
type ListPartsOptions struct {
	PartNumberMarker int
	MaxParts         int
}

// ListPartsResult holds the result of a list parts operation.
type ListPartsResult struct {
	Parts                []PartRecord
	IsTruncated          bool
	NextPartNumberMarker int
}

// MetadataStore defines the interface for all metadata operations required by
// BleepStore. Implementations must be safe for concurrent use.
type MetadataStore interface {
	io.Closer

	// Ping checks connectivity to the metadata store.
	Ping(ctx context.Context) error

	// Bucket operations

	// CreateBucket creates a new bucket record.
	CreateBucket(ctx context.Context, bucket *BucketRecord) error

	// GetBucket retrieves the metadata for the named bucket.
	GetBucket(ctx context.Context, name string) (*BucketRecord, error)

	// DeleteBucket removes the named bucket. Returns an error if the bucket
	// is not empty.
	DeleteBucket(ctx context.Context, name string) error

	// ListBuckets returns all bucket records owned by the given owner.
	ListBuckets(ctx context.Context, owner string) ([]BucketRecord, error)

	// BucketExists checks whether the named bucket exists.
	BucketExists(ctx context.Context, name string) (bool, error)

	// UpdateBucketAcl updates the ACL for the named bucket.
	UpdateBucketAcl(ctx context.Context, name string, acl json.RawMessage) error

	// Object operations

	// PutObject creates or replaces the metadata for an object.
	PutObject(ctx context.Context, obj *ObjectRecord) error

	// GetObject retrieves the metadata for the specified object.
	GetObject(ctx context.Context, bucket, key string) (*ObjectRecord, error)

	// DeleteObject removes the metadata for the specified object.
	DeleteObject(ctx context.Context, bucket, key string) error

	// ObjectExists checks whether the named object exists.
	ObjectExists(ctx context.Context, bucket, key string) (bool, error)

	// DeleteObjectsMeta removes metadata for multiple objects. Returns the
	// list of keys that were successfully deleted and any errors.
	DeleteObjectsMeta(ctx context.Context, bucket string, keys []string) (deleted []string, errs []error)

	// UpdateObjectAcl updates the ACL for the specified object.
	UpdateObjectAcl(ctx context.Context, bucket, key string, acl json.RawMessage) error

	// ListObjects lists objects in the given bucket according to the provided options.
	ListObjects(ctx context.Context, bucket string, opts ListObjectsOptions) (*ListObjectsResult, error)

	// Multipart upload operations

	// CreateMultipartUpload creates a new multipart upload record and returns
	// the generated upload ID.
	CreateMultipartUpload(ctx context.Context, upload *MultipartUploadRecord) (string, error)

	// GetMultipartUpload retrieves the metadata for the specified multipart upload.
	GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*MultipartUploadRecord, error)

	// PutPart records metadata for an uploaded part.
	PutPart(ctx context.Context, part *PartRecord) error

	// ListParts lists parts for the specified multipart upload.
	ListParts(ctx context.Context, uploadID string, opts ListPartsOptions) (*ListPartsResult, error)

	// GetPartsForCompletion retrieves part records for the given part numbers,
	// used during CompleteMultipartUpload to validate and assemble parts.
	GetPartsForCompletion(ctx context.Context, uploadID string, partNumbers []int) ([]PartRecord, error)

	// CompleteMultipartUpload finalizes a multipart upload, creating the final
	// object record and cleaning up part records. Returns the final object metadata.
	CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, obj *ObjectRecord) error

	// AbortMultipartUpload cancels a multipart upload and removes all associated
	// part records.
	AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error

	// ListMultipartUploads lists in-progress multipart uploads for the given bucket.
	ListMultipartUploads(ctx context.Context, bucket string, opts ListUploadsOptions) (*ListUploadsResult, error)

	// Credential operations

	// GetCredential retrieves a credential record by access key ID.
	GetCredential(ctx context.Context, accessKeyID string) (*CredentialRecord, error)

	// PutCredential creates or updates a credential record.
	PutCredential(ctx context.Context, cred *CredentialRecord) error
}

// ExpiredUpload holds the identifying fields of an expired multipart upload,
// returned by ReapExpiredUploads so the caller can clean up storage files.
type ExpiredUpload struct {
	UploadID   string
	BucketName string
	ObjectKey  string
}

// UploadReaper is an optional interface for metadata stores that support
// reaping expired multipart uploads.
type UploadReaper interface {
	ReapExpiredUploads(ttlSeconds int) ([]ExpiredUpload, error)
}
