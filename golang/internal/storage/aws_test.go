package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// mockS3Client implements S3API for unit testing.
type mockS3Client struct {
	// objects stores all objects keyed by their S3 key.
	objects map[string][]byte
	// multipartUploads tracks active multipart uploads.
	multipartUploads map[string]*mockMultipartUpload
	// nextUploadID is the counter for generating upload IDs.
	nextUploadID int
	// putObjectCalls tracks the number of PutObject calls for verification.
	putObjectCalls int
	// copyObjectCalls tracks the number of CopyObject calls.
	copyObjectCalls int
	// deleteObjectCalls tracks the number of DeleteObject calls.
	deleteObjectCalls int
	// headObjectCalls tracks the number of HeadObject calls.
	headObjectCalls int
	// forceEntityTooSmall makes UploadPartCopy return EntityTooSmall.
	forceEntityTooSmall bool
}

type mockMultipartUpload struct {
	key   string
	parts map[int32][]byte
}

func newMockS3Client() *mockS3Client {
	return &mockS3Client{
		objects:          make(map[string][]byte),
		multipartUploads: make(map[string]*mockMultipartUpload),
	}
}

func (m *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.putObjectCalls++
	key := aws.ToString(params.Key)
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	m.objects[key] = data
	h := md5.Sum(data)
	etag := fmt.Sprintf(`"%x"`, h)
	return &s3.PutObjectOutput{
		ETag: aws.String(etag),
	}, nil
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := aws.ToString(params.Key)
	data, ok := m.objects[key]
	if !ok {
		return nil, &mockAPIError{code: "NoSuchKey", message: "The specified key does not exist.", httpStatus: 404}
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: aws.Int64(int64(len(data))),
	}, nil
}

func (m *mockS3Client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.deleteObjectCalls++
	key := aws.ToString(params.Key)
	delete(m.objects, key)
	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockS3Client) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	for _, obj := range params.Delete.Objects {
		delete(m.objects, aws.ToString(obj.Key))
	}
	return &s3.DeleteObjectsOutput{}, nil
}

func (m *mockS3Client) CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	m.copyObjectCalls++
	// CopySource format: "bucket/key"
	copySource := aws.ToString(params.CopySource)
	// Remove bucket prefix to get the key.
	parts := strings.SplitN(copySource, "/", 2)
	if len(parts) < 2 {
		return nil, &mockAPIError{code: "NoSuchKey", message: "Invalid copy source", httpStatus: 404}
	}
	srcKey := parts[1]

	data, ok := m.objects[srcKey]
	if !ok {
		return nil, &mockAPIError{code: "NoSuchKey", message: "The specified key does not exist.", httpStatus: 404}
	}

	dstKey := aws.ToString(params.Key)
	m.objects[dstKey] = make([]byte, len(data))
	copy(m.objects[dstKey], data)

	h := md5.Sum(data)
	etag := fmt.Sprintf(`"%x"`, h)
	return &s3.CopyObjectOutput{
		CopyObjectResult: &types.CopyObjectResult{
			ETag: aws.String(etag),
		},
	}, nil
}

func (m *mockS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.headObjectCalls++
	key := aws.ToString(params.Key)
	data, ok := m.objects[key]
	if !ok {
		return nil, &mockAPIError{code: "NotFound", message: "Not Found", httpStatus: 404}
	}
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(data))),
	}, nil
}

func (m *mockS3Client) HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return &s3.HeadBucketOutput{}, nil
}

func (m *mockS3Client) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	m.nextUploadID++
	uploadID := fmt.Sprintf("mock-upload-%d", m.nextUploadID)
	m.multipartUploads[uploadID] = &mockMultipartUpload{
		key:   aws.ToString(params.Key),
		parts: make(map[int32][]byte),
	}
	return &s3.CreateMultipartUploadOutput{
		UploadId: aws.String(uploadID),
	}, nil
}

func (m *mockS3Client) UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	uploadID := aws.ToString(params.UploadId)
	upload, ok := m.multipartUploads[uploadID]
	if !ok {
		return nil, &mockAPIError{code: "NoSuchUpload", message: "No such upload", httpStatus: 404}
	}
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	upload.parts[aws.ToInt32(params.PartNumber)] = data

	h := md5.Sum(data)
	etag := fmt.Sprintf(`"%x"`, h)
	return &s3.UploadPartOutput{
		ETag: aws.String(etag),
	}, nil
}

func (m *mockS3Client) UploadPartCopy(ctx context.Context, params *s3.UploadPartCopyInput, optFns ...func(*s3.Options)) (*s3.UploadPartCopyOutput, error) {
	if m.forceEntityTooSmall {
		return nil, &mockAPIError{code: "EntityTooSmall", message: "Part too small", httpStatus: 400}
	}

	uploadID := aws.ToString(params.UploadId)
	upload, ok := m.multipartUploads[uploadID]
	if !ok {
		return nil, &mockAPIError{code: "NoSuchUpload", message: "No such upload", httpStatus: 404}
	}

	// Parse copy source.
	copySource := aws.ToString(params.CopySource)
	parts := strings.SplitN(copySource, "/", 2)
	if len(parts) < 2 {
		return nil, &mockAPIError{code: "NoSuchKey", message: "Invalid copy source", httpStatus: 404}
	}
	srcKey := parts[1]

	data, ok := m.objects[srcKey]
	if !ok {
		return nil, &mockAPIError{code: "NoSuchKey", message: "Not found", httpStatus: 404}
	}

	partNum := aws.ToInt32(params.PartNumber)
	upload.parts[partNum] = make([]byte, len(data))
	copy(upload.parts[partNum], data)

	h := md5.Sum(data)
	etag := fmt.Sprintf(`"%x"`, h)
	return &s3.UploadPartCopyOutput{
		CopyPartResult: &types.CopyPartResult{
			ETag: aws.String(etag),
		},
	}, nil
}

func (m *mockS3Client) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	uploadID := aws.ToString(params.UploadId)
	upload, ok := m.multipartUploads[uploadID]
	if !ok {
		return nil, &mockAPIError{code: "NoSuchUpload", message: "No such upload", httpStatus: 404}
	}

	// Assemble parts in order.
	var assembled bytes.Buffer
	compositeMD5 := md5.New()
	for _, cp := range params.MultipartUpload.Parts {
		partNum := aws.ToInt32(cp.PartNumber)
		partData, ok := upload.parts[partNum]
		if !ok {
			return nil, &mockAPIError{code: "InvalidPart", message: "Part not found", httpStatus: 400}
		}
		assembled.Write(partData)
		partHash := md5.Sum(partData)
		compositeMD5.Write(partHash[:])
	}

	// Store the assembled object.
	finalKey := upload.key
	m.objects[finalKey] = assembled.Bytes()
	delete(m.multipartUploads, uploadID)

	etag := fmt.Sprintf(`"%x-%d"`, compositeMD5.Sum(nil), len(params.MultipartUpload.Parts))
	return &s3.CompleteMultipartUploadOutput{
		ETag: aws.String(etag),
	}, nil
}

func (m *mockS3Client) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	uploadID := aws.ToString(params.UploadId)
	delete(m.multipartUploads, uploadID)
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (m *mockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefix := aws.ToString(params.Prefix)
	var contents []types.Object
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			contents = append(contents, types.Object{
				Key: aws.String(key),
			})
		}
	}
	return &s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: aws.Bool(false),
	}, nil
}

// mockAPIError implements smithy.APIError for the mock client.
type mockAPIError struct {
	code       string
	message    string
	httpStatus int
}

func (e *mockAPIError) Error() string {
	return fmt.Sprintf("%s: %s", e.code, e.message)
}

func (e *mockAPIError) ErrorCode() string {
	return e.code
}

func (e *mockAPIError) ErrorMessage() string {
	return e.message
}

func (e *mockAPIError) ErrorFault() smithy.ErrorFault {
	if e.httpStatus >= 500 {
		return smithy.FaultServer
	}
	return smithy.FaultClient
}

// Ensure mockAPIError satisfies smithy.APIError.
var _ smithy.APIError = (*mockAPIError)(nil)

// --- Test helpers ---

func newTestAWSBackend(t *testing.T) (*AWSGatewayBackend, *mockS3Client) {
	t.Helper()
	mock := newMockS3Client()
	backend := NewAWSGatewayBackendWithClient("test-upstream-bucket", "us-east-1", "bp/", mock)
	return backend, mock
}

// --- Tests ---

func TestAWSPutAndGetObject(t *testing.T) {
	backend, _ := newTestAWSBackend(t)
	ctx := context.Background()

	content := "Hello, AWS Gateway!"
	bytesWritten, etag, err := backend.PutObject(ctx, "my-bucket", "hello.txt", strings.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	if bytesWritten != int64(len(content)) {
		t.Errorf("bytesWritten = %d, want %d", bytesWritten, len(content))
	}
	if etag == "" {
		t.Error("ETag should not be empty")
	}
	if !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) {
		t.Errorf("ETag not quoted: %q", etag)
	}

	// Get it back.
	reader, size, _, err := backend.GetObject(ctx, "my-bucket", "hello.txt")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	if size != int64(len(content)) {
		t.Errorf("size = %d, want %d", size, len(content))
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(data) != content {
		t.Errorf("data = %q, want %q", string(data), content)
	}
}

func TestAWSPutObjectEmptyBody(t *testing.T) {
	backend, _ := newTestAWSBackend(t)
	ctx := context.Background()

	bytesWritten, etag, err := backend.PutObject(ctx, "my-bucket", "empty.txt", strings.NewReader(""), 0)
	if err != nil {
		t.Fatalf("PutObject (empty) failed: %v", err)
	}
	if bytesWritten != 0 {
		t.Errorf("bytesWritten = %d, want 0", bytesWritten)
	}
	if etag == "" {
		t.Error("ETag should not be empty even for empty object")
	}

	reader, size, _, err := backend.GetObject(ctx, "my-bucket", "empty.txt")
	if err != nil {
		t.Fatalf("GetObject (empty) failed: %v", err)
	}
	defer reader.Close()
	if size != 0 {
		t.Errorf("size = %d, want 0", size)
	}
}

func TestAWSGetObjectNotFound(t *testing.T) {
	backend, _ := newTestAWSBackend(t)
	ctx := context.Background()

	_, _, _, err := backend.GetObject(ctx, "my-bucket", "nonexistent.txt")
	if err == nil {
		t.Fatal("GetObject should fail for non-existent object")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should mention 'not found', got: %v", err)
	}
}

func TestAWSDeleteObject(t *testing.T) {
	backend, mock := newTestAWSBackend(t)
	ctx := context.Background()

	// Put an object.
	_, _, err := backend.PutObject(ctx, "my-bucket", "delete-me.txt", strings.NewReader("data"), 4)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Verify it exists.
	exists, err := backend.ObjectExists(ctx, "my-bucket", "delete-me.txt")
	if err != nil {
		t.Fatalf("ObjectExists failed: %v", err)
	}
	if !exists {
		t.Fatal("Object should exist before deletion")
	}

	// Delete it.
	if err := backend.DeleteObject(ctx, "my-bucket", "delete-me.txt"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Verify it's gone.
	exists, err = backend.ObjectExists(ctx, "my-bucket", "delete-me.txt")
	if err != nil {
		t.Fatalf("ObjectExists failed: %v", err)
	}
	if exists {
		t.Error("Object should not exist after deletion")
	}

	if mock.deleteObjectCalls != 1 {
		t.Errorf("expected 1 DeleteObject call, got %d", mock.deleteObjectCalls)
	}
}

func TestAWSDeleteObjectIdempotent(t *testing.T) {
	backend, _ := newTestAWSBackend(t)
	ctx := context.Background()

	// Delete a non-existent object: should not error (S3 is idempotent).
	if err := backend.DeleteObject(ctx, "my-bucket", "nonexistent.txt"); err != nil {
		t.Errorf("DeleteObject (non-existent) should not error, got: %v", err)
	}
}

func TestAWSCopyObject(t *testing.T) {
	backend, _ := newTestAWSBackend(t)
	ctx := context.Background()

	content := "copy me via AWS"
	_, etag1, err := backend.PutObject(ctx, "src-bucket", "original.txt", strings.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	etag2, err := backend.CopyObject(ctx, "src-bucket", "original.txt", "dst-bucket", "copied.txt")
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// ETags should match (same content).
	if etag1 != etag2 {
		t.Errorf("ETags should match: %q != %q", etag1, etag2)
	}

	// Verify the copy.
	reader, _, _, err := backend.GetObject(ctx, "dst-bucket", "copied.txt")
	if err != nil {
		t.Fatalf("GetObject (copy) failed: %v", err)
	}
	defer reader.Close()

	data, _ := io.ReadAll(reader)
	if string(data) != content {
		t.Errorf("Copied data = %q, want %q", string(data), content)
	}
}

func TestAWSCopyObjectNotFound(t *testing.T) {
	backend, _ := newTestAWSBackend(t)
	ctx := context.Background()

	_, err := backend.CopyObject(ctx, "src-bucket", "nonexistent.txt", "dst-bucket", "copy.txt")
	if err == nil {
		t.Fatal("CopyObject should fail for non-existent source")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should mention 'not found', got: %v", err)
	}
}

func TestAWSObjectExists(t *testing.T) {
	backend, _ := newTestAWSBackend(t)
	ctx := context.Background()

	// Should not exist.
	exists, err := backend.ObjectExists(ctx, "my-bucket", "nope.txt")
	if err != nil {
		t.Fatalf("ObjectExists failed: %v", err)
	}
	if exists {
		t.Error("ObjectExists should return false for non-existent object")
	}

	// Put it.
	_, _, err = backend.PutObject(ctx, "my-bucket", "yep.txt", strings.NewReader("data"), 4)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Should exist.
	exists, err = backend.ObjectExists(ctx, "my-bucket", "yep.txt")
	if err != nil {
		t.Fatalf("ObjectExists failed: %v", err)
	}
	if !exists {
		t.Error("ObjectExists should return true for existing object")
	}
}

func TestAWSCreateDeleteBucketNoOp(t *testing.T) {
	backend, _ := newTestAWSBackend(t)
	ctx := context.Background()

	// CreateBucket and DeleteBucket are no-ops for AWS gateway.
	if err := backend.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Errorf("CreateBucket should not error: %v", err)
	}
	if err := backend.DeleteBucket(ctx, "test-bucket"); err != nil {
		t.Errorf("DeleteBucket should not error: %v", err)
	}
}

func TestAWSKeyMapping(t *testing.T) {
	backend, mock := newTestAWSBackend(t)
	ctx := context.Background()

	// Verify key mapping: {prefix}{bucket}/{key}
	_, _, err := backend.PutObject(ctx, "my-bucket", "path/to/file.txt", strings.NewReader("data"), 4)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	expectedKey := "bp/my-bucket/path/to/file.txt"
	if _, ok := mock.objects[expectedKey]; !ok {
		t.Errorf("Object should be stored at key %q", expectedKey)
		t.Logf("Keys in mock: %v", keysOf(mock.objects))
	}
}

func TestAWSKeyMappingNoPrefix(t *testing.T) {
	mock := newMockS3Client()
	backend := NewAWSGatewayBackendWithClient("test-bucket", "us-east-1", "", mock)
	ctx := context.Background()

	_, _, err := backend.PutObject(ctx, "my-bucket", "file.txt", strings.NewReader("data"), 4)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	expectedKey := "my-bucket/file.txt"
	if _, ok := mock.objects[expectedKey]; !ok {
		t.Errorf("Object should be stored at key %q (no prefix)", expectedKey)
		t.Logf("Keys in mock: %v", keysOf(mock.objects))
	}
}

func TestAWSPutPartAndDeleteParts(t *testing.T) {
	backend, mock := newTestAWSBackend(t)
	ctx := context.Background()

	// Upload some parts.
	etag1, err := backend.PutPart(ctx, "my-bucket", "key", "upload-123", 1, strings.NewReader("part1-data"), 10)
	if err != nil {
		t.Fatalf("PutPart 1 failed: %v", err)
	}
	if etag1 == "" || !strings.HasPrefix(etag1, `"`) {
		t.Errorf("PutPart 1 ETag invalid: %q", etag1)
	}

	etag2, err := backend.PutPart(ctx, "my-bucket", "key", "upload-123", 2, strings.NewReader("part2-data"), 10)
	if err != nil {
		t.Fatalf("PutPart 2 failed: %v", err)
	}
	if etag2 == "" {
		t.Error("PutPart 2 ETag should not be empty")
	}

	// Verify parts are stored with correct keys.
	expectedKey1 := "bp/.parts/upload-123/1"
	expectedKey2 := "bp/.parts/upload-123/2"
	if _, ok := mock.objects[expectedKey1]; !ok {
		t.Errorf("Part 1 should be stored at key %q", expectedKey1)
	}
	if _, ok := mock.objects[expectedKey2]; !ok {
		t.Errorf("Part 2 should be stored at key %q", expectedKey2)
	}

	// Delete parts.
	if err := backend.DeleteParts(ctx, "my-bucket", "key", "upload-123"); err != nil {
		t.Fatalf("DeleteParts failed: %v", err)
	}

	// Verify parts are gone.
	if _, ok := mock.objects[expectedKey1]; ok {
		t.Error("Part 1 should be deleted")
	}
	if _, ok := mock.objects[expectedKey2]; ok {
		t.Error("Part 2 should be deleted")
	}
}

func TestAWSAssemblePartsSinglePart(t *testing.T) {
	backend, mock := newTestAWSBackend(t)
	ctx := context.Background()

	// Upload a single part.
	_, err := backend.PutPart(ctx, "my-bucket", "assembled.txt", "upload-single", 1, strings.NewReader("single-part-data"), 16)
	if err != nil {
		t.Fatalf("PutPart failed: %v", err)
	}

	// Assemble (single part uses CopyObject).
	etag, err := backend.AssembleParts(ctx, "my-bucket", "assembled.txt", "upload-single", []int{1})
	if err != nil {
		t.Fatalf("AssembleParts failed: %v", err)
	}
	if etag == "" {
		t.Error("ETag should not be empty after assembly")
	}

	// Verify the assembled object exists.
	finalKey := "bp/my-bucket/assembled.txt"
	if _, ok := mock.objects[finalKey]; !ok {
		t.Errorf("Assembled object should exist at %q", finalKey)
	}

	// Verify content.
	reader, _, _, err := backend.GetObject(ctx, "my-bucket", "assembled.txt")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if string(data) != "single-part-data" {
		t.Errorf("Assembled data = %q, want %q", string(data), "single-part-data")
	}
}

func TestAWSAssemblePartsMultiple(t *testing.T) {
	backend, mock := newTestAWSBackend(t)
	ctx := context.Background()

	// Upload two parts.
	_, err := backend.PutPart(ctx, "my-bucket", "multi.txt", "upload-multi", 1, strings.NewReader("part1"), 5)
	if err != nil {
		t.Fatalf("PutPart 1 failed: %v", err)
	}
	_, err = backend.PutPart(ctx, "my-bucket", "multi.txt", "upload-multi", 2, strings.NewReader("part2"), 5)
	if err != nil {
		t.Fatalf("PutPart 2 failed: %v", err)
	}

	// Assemble (multiple parts uses multipart upload with UploadPartCopy).
	etag, err := backend.AssembleParts(ctx, "my-bucket", "multi.txt", "upload-multi", []int{1, 2})
	if err != nil {
		t.Fatalf("AssembleParts failed: %v", err)
	}
	if etag == "" {
		t.Error("ETag should not be empty")
	}

	// ETag should be a composite ETag with -N suffix.
	if !strings.Contains(etag, "-2") {
		t.Errorf("Composite ETag should contain '-2', got %q", etag)
	}

	// Verify the assembled object content.
	finalKey := "bp/my-bucket/multi.txt"
	data, ok := mock.objects[finalKey]
	if !ok {
		t.Fatalf("Assembled object should exist at %q", finalKey)
	}
	if string(data) != "part1part2" {
		t.Errorf("Assembled data = %q, want %q", string(data), "part1part2")
	}
}

func TestAWSAssemblePartsEntityTooSmallFallback(t *testing.T) {
	mock := newMockS3Client()
	mock.forceEntityTooSmall = true
	backend := NewAWSGatewayBackendWithClient("test-upstream-bucket", "us-east-1", "bp/", mock)
	ctx := context.Background()

	// Upload two parts.
	_, err := backend.PutPart(ctx, "my-bucket", "small.txt", "upload-small", 1, strings.NewReader("aaa"), 3)
	if err != nil {
		t.Fatalf("PutPart 1 failed: %v", err)
	}
	_, err = backend.PutPart(ctx, "my-bucket", "small.txt", "upload-small", 2, strings.NewReader("bbb"), 3)
	if err != nil {
		t.Fatalf("PutPart 2 failed: %v", err)
	}

	// Assemble with UploadPartCopy forced to fail with EntityTooSmall.
	// Should fall back to download + re-upload via UploadPart.
	etag, err := backend.AssembleParts(ctx, "my-bucket", "small.txt", "upload-small", []int{1, 2})
	if err != nil {
		t.Fatalf("AssembleParts (fallback) failed: %v", err)
	}
	if etag == "" {
		t.Error("ETag should not be empty")
	}

	// Verify the assembled object exists.
	finalKey := "bp/my-bucket/small.txt"
	data, ok := mock.objects[finalKey]
	if !ok {
		t.Fatalf("Assembled object should exist at %q", finalKey)
	}
	if string(data) != "aaabbb" {
		t.Errorf("Assembled data = %q, want %q", string(data), "aaabbb")
	}
}

func TestAWSPutObjectETagConsistency(t *testing.T) {
	backend, _ := newTestAWSBackend(t)
	ctx := context.Background()

	content := "Hello, ETag!"
	_, etag, err := backend.PutObject(ctx, "my-bucket", "etag.txt", strings.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Compute expected MD5 directly.
	h := md5.Sum([]byte(content))
	expectedETag := fmt.Sprintf(`"%x"`, h)

	if etag != expectedETag {
		t.Errorf("ETag = %q, want %q", etag, expectedETag)
	}
}

func TestAWSPutPartETagConsistency(t *testing.T) {
	backend, _ := newTestAWSBackend(t)
	ctx := context.Background()

	content := "part data for ETag check"
	etag, err := backend.PutPart(ctx, "my-bucket", "key", "upload-etag", 1, strings.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("PutPart failed: %v", err)
	}

	// Compute expected MD5.
	h := md5.Sum([]byte(content))
	expectedETag := fmt.Sprintf(`"%x"`, h)

	if etag != expectedETag {
		t.Errorf("Part ETag = %q, want %q", etag, expectedETag)
	}
}

func TestAWSPutObjectOverwrite(t *testing.T) {
	backend, _ := newTestAWSBackend(t)
	ctx := context.Background()

	_, etag1, err := backend.PutObject(ctx, "my-bucket", "overwrite.txt", strings.NewReader("version 1"), 9)
	if err != nil {
		t.Fatalf("PutObject v1 failed: %v", err)
	}

	_, etag2, err := backend.PutObject(ctx, "my-bucket", "overwrite.txt", strings.NewReader("version 2!!"), 11)
	if err != nil {
		t.Fatalf("PutObject v2 failed: %v", err)
	}

	if etag1 == etag2 {
		t.Error("ETags should differ for different content")
	}

	reader, _, _, err := backend.GetObject(ctx, "my-bucket", "overwrite.txt")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	data, _ := io.ReadAll(reader)
	if string(data) != "version 2!!" {
		t.Errorf("data = %q, want %q", string(data), "version 2!!")
	}
}

func TestAWSS3KeyMapping(t *testing.T) {
	backend, _ := newTestAWSBackend(t)

	tests := []struct {
		bucket   string
		key      string
		expected string
	}{
		{"my-bucket", "file.txt", "bp/my-bucket/file.txt"},
		{"my-bucket", "path/to/file.txt", "bp/my-bucket/path/to/file.txt"},
		{"other-bucket", "key", "bp/other-bucket/key"},
	}

	for _, tc := range tests {
		got := backend.s3Key(tc.bucket, tc.key)
		if got != tc.expected {
			t.Errorf("s3Key(%q, %q) = %q, want %q", tc.bucket, tc.key, got, tc.expected)
		}
	}
}

func TestAWSPartKeyMapping(t *testing.T) {
	backend, _ := newTestAWSBackend(t)

	tests := []struct {
		uploadID   string
		partNumber int
		expected   string
	}{
		{"upload-123", 1, "bp/.parts/upload-123/1"},
		{"upload-123", 10, "bp/.parts/upload-123/10"},
		{"abc", 5, "bp/.parts/abc/5"},
	}

	for _, tc := range tests {
		got := backend.partKey(tc.uploadID, tc.partNumber)
		if got != tc.expected {
			t.Errorf("partKey(%q, %d) = %q, want %q", tc.uploadID, tc.partNumber, got, tc.expected)
		}
	}
}

func TestAWSInterfaceCompliance(t *testing.T) {
	// Verify at compile time that AWSGatewayBackend implements StorageBackend.
	var _ StorageBackend = (*AWSGatewayBackend)(nil)
}

func TestAWSDeletePartsNoParts(t *testing.T) {
	backend, _ := newTestAWSBackend(t)
	ctx := context.Background()

	// Deleting parts for a non-existent upload should not error.
	if err := backend.DeleteParts(ctx, "my-bucket", "key", "nonexistent-upload"); err != nil {
		t.Errorf("DeleteParts for non-existent upload should not error, got: %v", err)
	}
}

// keysOf returns the keys of a map[string][]byte.
func keysOf(m map[string][]byte) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
