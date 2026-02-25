// Package storage provides the AWS S3 gateway backend for BleepStore.
//
// The AWS gateway backend proxies all data operations to an upstream AWS S3
// bucket via the AWS SDK for Go v2. Metadata stays in local SQLite -- this
// backend handles raw bytes only.
//
// Key mapping:
//
//	Objects:  {prefix}{bleepstore_bucket}/{key}
//	Parts:    {prefix}.parts/{upload_id}/{part_number}
//
// Credentials are resolved via the standard AWS credential chain
// (env vars, ~/.aws/credentials, IAM role, etc.).
package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3API defines the subset of the AWS S3 client interface that the gateway
// backend uses. This allows mocking in tests.
type S3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	UploadPartCopy(ctx context.Context, params *s3.UploadPartCopyInput, optFns ...func(*s3.Options)) (*s3.UploadPartCopyOutput, error)
	CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// AWSGatewayBackend implements the StorageBackend interface by proxying
// storage operations to an upstream Amazon S3 bucket. This allows BleepStore
// to act as an S3-compatible gateway in front of native AWS S3.
//
// All BleepStore buckets/objects are stored under a single upstream S3 bucket
// with a key prefix to namespace them.
type AWSGatewayBackend struct {
	// Bucket is the upstream S3 bucket name.
	Bucket string
	// Region is the AWS region of the upstream bucket.
	Region string
	// Prefix is the key prefix for all objects in the upstream bucket.
	Prefix string
	// client is the AWS S3 client (satisfying S3API interface).
	client S3API
}

// NewAWSGatewayBackend creates a new AWSGatewayBackend configured to proxy
// to the specified S3 bucket in the given region. It initializes the AWS SDK
// client using the default credential chain, with optional overrides for
// custom endpoint, path-style addressing, and static credentials.
func NewAWSGatewayBackend(ctx context.Context, bucket, region, prefix, endpointURL string, usePathStyle bool, accessKeyID, secretAccessKey string) (*AWSGatewayBackend, error) {
	var loadOpts []func(*awsconfig.LoadOptions) error
	loadOpts = append(loadOpts, awsconfig.WithRegion(region))

	// Use static credentials if provided, otherwise fall back to default chain.
	if accessKeyID != "" && secretAccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, ""),
		))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	// Build S3 client options for custom endpoint and path-style.
	var s3Opts []func(*s3.Options)
	if endpointURL != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpointURL)
		})
	}
	if usePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(cfg, s3Opts...)

	b := &AWSGatewayBackend{
		Bucket: bucket,
		Region: region,
		Prefix: prefix,
		client: client,
	}

	// Verify the upstream bucket is accessible.
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return nil, fmt.Errorf("cannot access upstream S3 bucket %q: %w", bucket, err)
	}

	slog.Info("AWS gateway backend initialized", "bucket", bucket, "region", region, "prefix", prefix)
	return b, nil
}

// NewAWSGatewayBackendWithClient creates an AWSGatewayBackend with a
// pre-configured S3 client. This is primarily used for testing with mock
// clients.
func NewAWSGatewayBackendWithClient(bucket, region, prefix string, client S3API) *AWSGatewayBackend {
	return &AWSGatewayBackend{
		Bucket: bucket,
		Region: region,
		Prefix: prefix,
		client: client,
	}
}

// s3Key maps a BleepStore bucket/key to an upstream S3 key.
func (b *AWSGatewayBackend) s3Key(bucket, key string) string {
	return b.Prefix + bucket + "/" + key
}

// partKey maps a multipart part to an upstream S3 key.
func (b *AWSGatewayBackend) partKey(uploadID string, partNumber int) string {
	return fmt.Sprintf("%s.parts/%s/%d", b.Prefix, uploadID, partNumber)
}

// PutObject uploads object data to the upstream S3 bucket. It reads all data,
// computes MD5 locally for a consistent ETag, then uploads to S3.
func (b *AWSGatewayBackend) PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64) (int64, string, error) {
	s3key := b.s3Key(bucket, key)

	// Read all data to compute MD5 locally. AWS may return different ETags
	// when server-side encryption is enabled, so we compute our own.
	data, err := io.ReadAll(reader)
	if err != nil {
		return 0, "", fmt.Errorf("reading object data: %w", err)
	}

	h := md5.New()
	h.Write(data)
	etag := fmt.Sprintf(`"%x"`, h.Sum(nil))

	_, err = b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(b.Bucket),
		Key:           aws.String(s3key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return 0, "", fmt.Errorf("uploading to S3: %w", err)
	}

	return int64(len(data)), etag, nil
}

// GetObject retrieves object data from the upstream S3 bucket.
// Returns the data stream, the object size, and an empty ETag (metadata store
// holds the authoritative ETag). The caller is responsible for closing the
// returned ReadCloser.
func (b *AWSGatewayBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, int64, string, error) {
	s3key := b.s3Key(bucket, key)

	resp, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(s3key),
	})
	if err != nil {
		if isAWSNotFound(err) {
			return nil, 0, "", fmt.Errorf("object not found: %s/%s", bucket, key)
		}
		return nil, 0, "", fmt.Errorf("getting object from S3: %w", err)
	}

	var objectSize int64
	if resp.ContentLength != nil {
		objectSize = *resp.ContentLength
	}

	return resp.Body, objectSize, "", nil
}

// DeleteObject removes an object from the upstream S3 bucket.
// Idempotent: S3 DeleteObject does not error on missing keys.
func (b *AWSGatewayBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	s3key := b.s3Key(bucket, key)

	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(s3key),
	})
	if err != nil {
		return fmt.Errorf("deleting object from S3: %w", err)
	}
	return nil
}

// CopyObject copies an object within the upstream S3 bucket using AWS
// server-side copy. Returns the new ETag computed locally by downloading
// the source to ensure consistent ETags.
func (b *AWSGatewayBackend) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (string, error) {
	srcS3Key := b.s3Key(srcBucket, srcKey)
	dstS3Key := b.s3Key(dstBucket, dstKey)

	copySource := b.Bucket + "/" + srcS3Key

	resp, err := b.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(b.Bucket),
		Key:        aws.String(dstS3Key),
		CopySource: aws.String(copySource),
	})
	if err != nil {
		if isAWSNotFound(err) {
			return "", fmt.Errorf("source object not found: %s/%s", srcBucket, srcKey)
		}
		return "", fmt.Errorf("copying object in S3: %w", err)
	}

	// Extract ETag from CopyObjectResult, stripping quotes.
	etag := ""
	if resp.CopyObjectResult != nil && resp.CopyObjectResult.ETag != nil {
		etag = strings.Trim(*resp.CopyObjectResult.ETag, `"`)
	}

	return fmt.Sprintf(`"%s"`, etag), nil
}

// PutPart stores a multipart upload part as a temporary S3 object.
// Parts are stored at {prefix}.parts/{upload_id}/{part_number}.
// Computes MD5 locally for a consistent ETag.
func (b *AWSGatewayBackend) PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, reader io.Reader, size int64) (string, error) {
	pk := b.partKey(uploadID, partNumber)

	// Read all data to compute MD5 locally.
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("reading part data: %w", err)
	}

	h := md5.New()
	h.Write(data)
	etag := fmt.Sprintf(`"%x"`, h.Sum(nil))

	_, err = b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(b.Bucket),
		Key:           aws.String(pk),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return "", fmt.Errorf("uploading part to S3: %w", err)
	}

	return etag, nil
}

// AssembleParts assembles uploaded parts into the final object using AWS
// native multipart upload with UploadPartCopy for server-side copy.
//
// For a single part, uses CopyObject directly. For multiple parts, creates
// a native AWS multipart upload and uses UploadPartCopy for server-side
// assembly (no data download). Falls back to download + re-upload if
// UploadPartCopy fails with EntityTooSmall.
//
// Returns the composite ETag.
func (b *AWSGatewayBackend) AssembleParts(ctx context.Context, bucket, key, uploadID string, partNumbers []int) (string, error) {
	finalKey := b.s3Key(bucket, key)

	if len(partNumbers) == 1 {
		// Single part: direct copy.
		pk := b.partKey(uploadID, partNumbers[0])
		copySource := b.Bucket + "/" + pk

		resp, err := b.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(b.Bucket),
			Key:        aws.String(finalKey),
			CopySource: aws.String(copySource),
		})
		if err != nil {
			return "", fmt.Errorf("copying single part to final object: %w", err)
		}

		etag := ""
		if resp.CopyObjectResult != nil && resp.CopyObjectResult.ETag != nil {
			etag = strings.Trim(*resp.CopyObjectResult.ETag, `"`)
		}
		return fmt.Sprintf(`"%s"`, etag), nil
	}

	// Multiple parts: native AWS multipart upload with server-side copy.
	createResp, err := b.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(finalKey),
	})
	if err != nil {
		return "", fmt.Errorf("creating AWS multipart upload: %w", err)
	}
	awsUploadID := aws.ToString(createResp.UploadId)

	var completedParts []types.CompletedPart
	abortOnError := func() {
		_, abortErr := b.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(b.Bucket),
			Key:      aws.String(finalKey),
			UploadId: aws.String(awsUploadID),
		})
		if abortErr != nil {
			slog.Warn("Failed to abort AWS multipart upload", "upload_id", awsUploadID, "error", abortErr)
		}
	}

	for idx, pn := range partNumbers {
		awsPartNumber := int32(idx + 1) // AWS part numbers are 1-indexed
		pk := b.partKey(uploadID, pn)
		copySource := b.Bucket + "/" + pk

		copyResp, copyErr := b.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:     aws.String(b.Bucket),
			Key:        aws.String(finalKey),
			UploadId:   aws.String(awsUploadID),
			PartNumber: aws.Int32(awsPartNumber),
			CopySource: aws.String(copySource),
		})

		var partETag string
		if copyErr != nil {
			// Check if it's EntityTooSmall -- fall back to download + re-upload.
			if isAWSEntityTooSmall(copyErr) {
				getResp, getErr := b.client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(b.Bucket),
					Key:    aws.String(pk),
				})
				if getErr != nil {
					abortOnError()
					return "", fmt.Errorf("downloading part %d for fallback upload: %w", pn, getErr)
				}
				partData, readErr := io.ReadAll(getResp.Body)
				getResp.Body.Close()
				if readErr != nil {
					abortOnError()
					return "", fmt.Errorf("reading part %d data: %w", pn, readErr)
				}

				uploadResp, uploadErr := b.client.UploadPart(ctx, &s3.UploadPartInput{
					Bucket:     aws.String(b.Bucket),
					Key:        aws.String(finalKey),
					UploadId:   aws.String(awsUploadID),
					PartNumber: aws.Int32(awsPartNumber),
					Body:       bytes.NewReader(partData),
				})
				if uploadErr != nil {
					abortOnError()
					return "", fmt.Errorf("uploading part %d fallback: %w", pn, uploadErr)
				}
				partETag = aws.ToString(uploadResp.ETag)
			} else {
				abortOnError()
				return "", fmt.Errorf("copying part %d: %w", pn, copyErr)
			}
		} else {
			if copyResp.CopyPartResult != nil && copyResp.CopyPartResult.ETag != nil {
				partETag = *copyResp.CopyPartResult.ETag
			}
		}

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       aws.String(partETag),
			PartNumber: aws.Int32(awsPartNumber),
		})
	}

	completeResp, err := b.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(b.Bucket),
		Key:      aws.String(finalKey),
		UploadId: aws.String(awsUploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		abortOnError()
		return "", fmt.Errorf("completing AWS multipart upload: %w", err)
	}

	etag := ""
	if completeResp.ETag != nil {
		etag = strings.Trim(*completeResp.ETag, `"`)
	}
	return fmt.Sprintf(`"%s"`, etag), nil
}

// DeleteParts removes all temporary part objects for a multipart upload.
// Lists objects under .parts/{upload_id}/ and batch-deletes them.
func (b *AWSGatewayBackend) DeleteParts(ctx context.Context, bucket, key, uploadID string) error {
	prefix := b.Prefix + ".parts/" + uploadID + "/"

	// List all part objects under this upload ID.
	for {
		listResp, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(b.Bucket),
			Prefix: aws.String(prefix),
		})
		if err != nil {
			return fmt.Errorf("listing parts for upload %s: %w", uploadID, err)
		}

		if len(listResp.Contents) == 0 {
			break
		}

		// Build the delete objects list.
		var objects []types.ObjectIdentifier
		for _, obj := range listResp.Contents {
			objects = append(objects, types.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		_, err = b.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(b.Bucket),
			Delete: &types.Delete{
				Objects: objects,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("batch-deleting parts for upload %s: %w", uploadID, err)
		}

		if !aws.ToBool(listResp.IsTruncated) {
			break
		}
	}

	return nil
}

// CreateBucket is a no-op for the AWS gateway backend. All BleepStore buckets
// share a single upstream S3 bucket with key prefixes, so there is nothing
// to create on the AWS side.
func (b *AWSGatewayBackend) CreateBucket(ctx context.Context, bucket string) error {
	// No-op: BleepStore buckets are mapped to key prefixes within the upstream
	// bucket, not to actual AWS S3 buckets.
	return nil
}

// DeleteBucket is a no-op for the AWS gateway backend. Bucket data is scoped
// by key prefix, so there is nothing to delete on the AWS side. The metadata
// store handles the actual bucket record deletion.
func (b *AWSGatewayBackend) DeleteBucket(ctx context.Context, bucket string) error {
	// No-op: same as CreateBucket. Objects under the prefix will be deleted
	// individually by DeleteObject calls.
	return nil
}

// ObjectExists checks whether an object exists in the upstream S3 bucket.
func (b *AWSGatewayBackend) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	s3key := b.s3Key(bucket, key)

	_, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(s3key),
	})
	if err != nil {
		if isAWSNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("checking object existence in S3: %w", err)
	}
	return true, nil
}

// HealthCheck verifies that the upstream S3 bucket is accessible.
func (b *AWSGatewayBackend) HealthCheck(ctx context.Context) error {
	_, err := b.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(b.Bucket),
	})
	return err
}

// isAWSNotFound checks if an AWS error is a 404/NoSuchKey/NotFound error.
func isAWSNotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		if code == "NoSuchKey" || code == "NotFound" || code == "404" || code == "NoSuchBucket" {
			return true
		}
	}
	// Also check for types.NoSuchKey.
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	// Check HTTP status code via ResponseError.
	var respErr interface{ HTTPStatusCode() int }
	if errors.As(err, &respErr) {
		if respErr.HTTPStatusCode() == 404 {
			return true
		}
	}
	return false
}

// isAWSEntityTooSmall checks if an AWS error is an EntityTooSmall error.
func isAWSEntityTooSmall(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "EntityTooSmall"
	}
	return false
}

// Ensure AWSGatewayBackend implements StorageBackend at compile time.
var _ StorageBackend = (*AWSGatewayBackend)(nil)
