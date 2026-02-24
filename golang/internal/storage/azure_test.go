package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
)

// mockAzureClient implements AzureBlobAPI for unit testing.
type mockAzureClient struct {
	// blobs stores all blobs keyed by "container/blobName".
	blobs map[string][]byte
	// stagedBlocks stores staged (uncommitted) blocks keyed by "container/blobName"
	// mapping to a map of blockID -> data.
	stagedBlocks map[string]map[string][]byte
	// uploadCalls tracks the number of upload operations.
	uploadCalls int
	// downloadCalls tracks the number of download operations.
	downloadCalls int
	// deleteCalls tracks the number of delete operations.
	deleteCalls int
	// copyCalls tracks the number of copy operations.
	copyCalls int
	// stageBlockCalls tracks the number of StageBlock operations.
	stageBlockCalls int
	// commitBlockListCalls tracks the number of CommitBlockList operations.
	commitBlockListCalls int
}

func newMockAzureClient() *mockAzureClient {
	return &mockAzureClient{
		blobs:        make(map[string][]byte),
		stagedBlocks: make(map[string]map[string][]byte),
	}
}

func (m *mockAzureClient) blobKey(containerName, blobName string) string {
	return containerName + "/" + blobName
}

func (m *mockAzureClient) UploadBlob(ctx context.Context, containerName, blobName string, data []byte) error {
	m.uploadCalls++
	copied := make([]byte, len(data))
	copy(copied, data)
	m.blobs[m.blobKey(containerName, blobName)] = copied
	return nil
}

func (m *mockAzureClient) DownloadBlob(ctx context.Context, containerName, blobName string) ([]byte, error) {
	m.downloadCalls++
	key := m.blobKey(containerName, blobName)
	data, ok := m.blobs[key]
	if !ok {
		return nil, fmt.Errorf("BlobNotFound: the specified blob does not exist")
	}
	copied := make([]byte, len(data))
	copy(copied, data)
	return copied, nil
}

func (m *mockAzureClient) DeleteBlob(ctx context.Context, containerName, blobName string) error {
	m.deleteCalls++
	key := m.blobKey(containerName, blobName)
	if _, ok := m.blobs[key]; !ok {
		return fmt.Errorf("BlobNotFound: the specified blob does not exist")
	}
	delete(m.blobs, key)
	return nil
}

func (m *mockAzureClient) BlobExists(ctx context.Context, containerName, blobName string) (bool, error) {
	key := m.blobKey(containerName, blobName)
	_, ok := m.blobs[key]
	return ok, nil
}

func (m *mockAzureClient) GetBlobProperties(ctx context.Context, containerName, blobName string) (int64, error) {
	key := m.blobKey(containerName, blobName)
	data, ok := m.blobs[key]
	if !ok {
		return 0, fmt.Errorf("BlobNotFound: the specified blob does not exist")
	}
	return int64(len(data)), nil
}

func (m *mockAzureClient) StartCopyFromURL(ctx context.Context, containerName, blobName, sourceURL string) error {
	m.copyCalls++
	// Parse the source URL to find the source blob.
	// URL format: {accountURL}/{container}/{blobName}
	// In tests, we just look up in our map.
	// Extract container/blobName from the sourceURL by finding the last two path segments
	// after the host part.
	parts := strings.SplitN(sourceURL, "/", 5) // scheme, "", host, container, blobName
	if len(parts) < 5 {
		return fmt.Errorf("invalid source URL: %s", sourceURL)
	}
	srcContainer := parts[3]
	srcBlobName := parts[4]
	srcKey := m.blobKey(srcContainer, srcBlobName)
	data, ok := m.blobs[srcKey]
	if !ok {
		return fmt.Errorf("BlobNotFound: the specified blob does not exist")
	}
	copied := make([]byte, len(data))
	copy(copied, data)
	dstKey := m.blobKey(containerName, blobName)
	m.blobs[dstKey] = copied
	return nil
}

func (m *mockAzureClient) StageBlock(ctx context.Context, containerName, blobName, blockID string, data []byte) error {
	m.stageBlockCalls++
	key := m.blobKey(containerName, blobName)
	if m.stagedBlocks[key] == nil {
		m.stagedBlocks[key] = make(map[string][]byte)
	}
	copied := make([]byte, len(data))
	copy(copied, data)
	m.stagedBlocks[key][blockID] = copied
	return nil
}

func (m *mockAzureClient) CommitBlockList(ctx context.Context, containerName, blobName string, blockIDs []string) error {
	m.commitBlockListCalls++
	key := m.blobKey(containerName, blobName)
	staged := m.stagedBlocks[key]
	if staged == nil {
		staged = make(map[string][]byte)
	}

	// Assemble the blob from the block IDs in order.
	var assembled bytes.Buffer
	for _, bid := range blockIDs {
		data, ok := staged[bid]
		if !ok {
			return fmt.Errorf("InvalidBlockList: block %s not found", bid)
		}
		assembled.Write(data)
	}

	m.blobs[key] = assembled.Bytes()

	// Clean up staged blocks for this blob.
	delete(m.stagedBlocks, key)

	return nil
}

// --- Test helpers ---

func newTestAzureBackend(t *testing.T) (*AzureGatewayBackend, *mockAzureClient) {
	t.Helper()
	mock := newMockAzureClient()
	backend := NewAzureGatewayBackendWithClient("test-container", "https://teststorage.blob.core.windows.net", "bp/", mock)
	return backend, mock
}

// --- Tests ---

func TestAzurePutAndGetObject(t *testing.T) {
	backend, _ := newTestAzureBackend(t)
	ctx := context.Background()

	content := "Hello, Azure Gateway!"
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

func TestAzurePutObjectEmptyBody(t *testing.T) {
	backend, _ := newTestAzureBackend(t)
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

func TestAzureGetObjectNotFound(t *testing.T) {
	backend, _ := newTestAzureBackend(t)
	ctx := context.Background()

	_, _, _, err := backend.GetObject(ctx, "my-bucket", "nonexistent.txt")
	if err == nil {
		t.Fatal("GetObject should fail for non-existent object")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should mention 'not found', got: %v", err)
	}
}

func TestAzureDeleteObject(t *testing.T) {
	backend, mock := newTestAzureBackend(t)
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

	if mock.deleteCalls != 1 {
		t.Errorf("expected 1 Delete call, got %d", mock.deleteCalls)
	}
}

func TestAzureDeleteObjectIdempotent(t *testing.T) {
	backend, _ := newTestAzureBackend(t)
	ctx := context.Background()

	// Delete a non-existent object: should not error (idempotent).
	if err := backend.DeleteObject(ctx, "my-bucket", "nonexistent.txt"); err != nil {
		t.Errorf("DeleteObject (non-existent) should not error, got: %v", err)
	}
}

func TestAzureCopyObject(t *testing.T) {
	backend, _ := newTestAzureBackend(t)
	ctx := context.Background()

	content := "copy me via Azure"
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

func TestAzureCopyObjectNotFound(t *testing.T) {
	backend, _ := newTestAzureBackend(t)
	ctx := context.Background()

	_, err := backend.CopyObject(ctx, "src-bucket", "nonexistent.txt", "dst-bucket", "copy.txt")
	if err == nil {
		t.Fatal("CopyObject should fail for non-existent source")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should mention 'not found', got: %v", err)
	}
}

func TestAzureObjectExists(t *testing.T) {
	backend, _ := newTestAzureBackend(t)
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

func TestAzureCreateDeleteBucketNoOp(t *testing.T) {
	backend, _ := newTestAzureBackend(t)
	ctx := context.Background()

	// CreateBucket and DeleteBucket are no-ops for Azure gateway.
	if err := backend.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Errorf("CreateBucket should not error: %v", err)
	}
	if err := backend.DeleteBucket(ctx, "test-bucket"); err != nil {
		t.Errorf("DeleteBucket should not error: %v", err)
	}
}

func TestAzureKeyMapping(t *testing.T) {
	backend, mock := newTestAzureBackend(t)
	ctx := context.Background()

	// Verify key mapping: {prefix}{bucket}/{key}
	_, _, err := backend.PutObject(ctx, "my-bucket", "path/to/file.txt", strings.NewReader("data"), 4)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	expectedKey := "test-container/bp/my-bucket/path/to/file.txt"
	if _, ok := mock.blobs[expectedKey]; !ok {
		t.Errorf("Object should be stored at key %q", expectedKey)
		t.Logf("Keys in mock: %v", azureKeysOf(mock.blobs))
	}
}

func TestAzureKeyMappingNoPrefix(t *testing.T) {
	mock := newMockAzureClient()
	backend := NewAzureGatewayBackendWithClient("test-container", "https://test.blob.core.windows.net", "", mock)
	ctx := context.Background()

	_, _, err := backend.PutObject(ctx, "my-bucket", "file.txt", strings.NewReader("data"), 4)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	expectedKey := "test-container/my-bucket/file.txt"
	if _, ok := mock.blobs[expectedKey]; !ok {
		t.Errorf("Object should be stored at key %q (no prefix)", expectedKey)
		t.Logf("Keys in mock: %v", azureKeysOf(mock.blobs))
	}
}

func TestAzurePutPartAndAssemble(t *testing.T) {
	backend, mock := newTestAzureBackend(t)
	ctx := context.Background()

	// Upload some parts using StageBlock.
	etag1, err := backend.PutPart(ctx, "my-bucket", "assembled.txt", "upload-123", 1, strings.NewReader("part1-data"), 10)
	if err != nil {
		t.Fatalf("PutPart 1 failed: %v", err)
	}
	if etag1 == "" || !strings.HasPrefix(etag1, `"`) {
		t.Errorf("PutPart 1 ETag invalid: %q", etag1)
	}

	etag2, err := backend.PutPart(ctx, "my-bucket", "assembled.txt", "upload-123", 2, strings.NewReader("part2-data"), 10)
	if err != nil {
		t.Fatalf("PutPart 2 failed: %v", err)
	}
	if etag2 == "" {
		t.Error("PutPart 2 ETag should not be empty")
	}

	// Verify StageBlock was called.
	if mock.stageBlockCalls != 2 {
		t.Errorf("Expected 2 StageBlock calls, got %d", mock.stageBlockCalls)
	}

	// Verify blocks are staged (not committed as blobs).
	blobKey := "test-container/bp/my-bucket/assembled.txt"
	if _, ok := mock.blobs[blobKey]; ok {
		t.Error("Blob should NOT exist yet (only staged blocks)")
	}

	// Assemble the parts.
	etag, err := backend.AssembleParts(ctx, "my-bucket", "assembled.txt", "upload-123", []int{1, 2})
	if err != nil {
		t.Fatalf("AssembleParts failed: %v", err)
	}
	if etag == "" {
		t.Error("ETag should not be empty after assembly")
	}

	// Verify CommitBlockList was called.
	if mock.commitBlockListCalls != 1 {
		t.Errorf("Expected 1 CommitBlockList call, got %d", mock.commitBlockListCalls)
	}

	// Verify the assembled blob exists.
	data, ok := mock.blobs[blobKey]
	if !ok {
		t.Fatalf("Assembled object should exist at %q", blobKey)
	}
	if string(data) != "part1-datapart2-data" {
		t.Errorf("Assembled data = %q, want %q", string(data), "part1-datapart2-data")
	}

	// Verify ETag matches content MD5.
	h := md5.Sum([]byte("part1-datapart2-data"))
	expectedETag := fmt.Sprintf(`"%x"`, h)
	if etag != expectedETag {
		t.Errorf("ETag = %q, want %q", etag, expectedETag)
	}
}

func TestAzureDeletePartsNoOp(t *testing.T) {
	backend, _ := newTestAzureBackend(t)
	ctx := context.Background()

	// DeleteParts is a no-op for Azure (uncommitted blocks auto-expire).
	if err := backend.DeleteParts(ctx, "my-bucket", "key", "upload-123"); err != nil {
		t.Errorf("DeleteParts should be a no-op, got error: %v", err)
	}
}

func TestAzureDeletePartsAfterUpload(t *testing.T) {
	backend, _ := newTestAzureBackend(t)
	ctx := context.Background()

	// Upload some parts.
	_, err := backend.PutPart(ctx, "my-bucket", "key", "upload-456", 1, strings.NewReader("data1"), 5)
	if err != nil {
		t.Fatalf("PutPart failed: %v", err)
	}

	// DeleteParts is still a no-op.
	if err := backend.DeleteParts(ctx, "my-bucket", "key", "upload-456"); err != nil {
		t.Errorf("DeleteParts should be a no-op even with staged blocks, got: %v", err)
	}
}

func TestAzureAssemblePartsSinglePart(t *testing.T) {
	backend, mock := newTestAzureBackend(t)
	ctx := context.Background()

	// Upload a single part.
	_, err := backend.PutPart(ctx, "my-bucket", "single.txt", "upload-one", 1, strings.NewReader("only-part"), 9)
	if err != nil {
		t.Fatalf("PutPart failed: %v", err)
	}

	// Assemble with single part.
	etag, err := backend.AssembleParts(ctx, "my-bucket", "single.txt", "upload-one", []int{1})
	if err != nil {
		t.Fatalf("AssembleParts (single) failed: %v", err)
	}
	if etag == "" {
		t.Error("ETag should not be empty")
	}

	// Verify the assembled object.
	blobKey := "test-container/bp/my-bucket/single.txt"
	data, ok := mock.blobs[blobKey]
	if !ok {
		t.Fatalf("Assembled object should exist at %q", blobKey)
	}
	if string(data) != "only-part" {
		t.Errorf("Assembled data = %q, want %q", string(data), "only-part")
	}
}

func TestAzureAssemblePartsThreeParts(t *testing.T) {
	backend, mock := newTestAzureBackend(t)
	ctx := context.Background()

	// Upload 3 parts.
	_, err := backend.PutPart(ctx, "my-bucket", "multi.txt", "upload-3", 1, strings.NewReader("part1"), 5)
	if err != nil {
		t.Fatalf("PutPart 1 failed: %v", err)
	}
	_, err = backend.PutPart(ctx, "my-bucket", "multi.txt", "upload-3", 2, strings.NewReader("part2"), 5)
	if err != nil {
		t.Fatalf("PutPart 2 failed: %v", err)
	}
	_, err = backend.PutPart(ctx, "my-bucket", "multi.txt", "upload-3", 3, strings.NewReader("part3"), 5)
	if err != nil {
		t.Fatalf("PutPart 3 failed: %v", err)
	}

	// Assemble.
	etag, err := backend.AssembleParts(ctx, "my-bucket", "multi.txt", "upload-3", []int{1, 2, 3})
	if err != nil {
		t.Fatalf("AssembleParts failed: %v", err)
	}

	// Verify assembled data.
	blobKey := "test-container/bp/my-bucket/multi.txt"
	data, ok := mock.blobs[blobKey]
	if !ok {
		t.Fatalf("Assembled object should exist at %q", blobKey)
	}
	if string(data) != "part1part2part3" {
		t.Errorf("Assembled data = %q, want %q", string(data), "part1part2part3")
	}

	// Verify only 1 CommitBlockList call.
	if mock.commitBlockListCalls != 1 {
		t.Errorf("Expected 1 CommitBlockList call, got %d", mock.commitBlockListCalls)
	}

	// Verify ETag.
	h := md5.Sum([]byte("part1part2part3"))
	expectedETag := fmt.Sprintf(`"%x"`, h)
	if etag != expectedETag {
		t.Errorf("ETag = %q, want %q", etag, expectedETag)
	}
}

func TestAzurePutObjectETagConsistency(t *testing.T) {
	backend, _ := newTestAzureBackend(t)
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

func TestAzurePutPartETagConsistency(t *testing.T) {
	backend, _ := newTestAzureBackend(t)
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

func TestAzurePutObjectOverwrite(t *testing.T) {
	backend, _ := newTestAzureBackend(t)
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

func TestAzureBlobKeyMapping(t *testing.T) {
	backend, _ := newTestAzureBackend(t)

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
		got := backend.blobName(tc.bucket, tc.key)
		if got != tc.expected {
			t.Errorf("blobName(%q, %q) = %q, want %q", tc.bucket, tc.key, got, tc.expected)
		}
	}
}

func TestAzureBlockIDFormat(t *testing.T) {
	// Block IDs include uploadID and are base64-encoded.
	tests := []struct {
		uploadID   string
		partNumber int
	}{
		{"upload-123", 1},
		{"upload-123", 10},
		{"abc", 5},
		{"upload-xyz", 99999},
	}

	for _, tc := range tests {
		bid := blockID(tc.uploadID, tc.partNumber)
		if bid == "" {
			t.Errorf("blockID(%q, %d) should not be empty", tc.uploadID, tc.partNumber)
		}

		// Verify it's valid base64.
		decoded, err := base64.StdEncoding.DecodeString(bid)
		if err != nil {
			t.Errorf("blockID(%q, %d) = %q is not valid base64: %v", tc.uploadID, tc.partNumber, bid, err)
		}

		// Verify the decoded string contains the upload ID and zero-padded part number.
		expected := fmt.Sprintf("%s:%05d", tc.uploadID, tc.partNumber)
		if string(decoded) != expected {
			t.Errorf("blockID(%q, %d) decoded = %q, want %q", tc.uploadID, tc.partNumber, string(decoded), expected)
		}
	}
}

func TestAzureBlockIDConsistentLength(t *testing.T) {
	// All block IDs for the same upload should be the same length.
	uploadID := "upload-consistency-test"
	ids := make([]string, 0, 100)
	for i := 1; i <= 100; i++ {
		ids = append(ids, blockID(uploadID, i))
	}

	firstLen := len(ids[0])
	for i, id := range ids {
		if len(id) != firstLen {
			t.Errorf("blockID length mismatch: part %d has length %d, expected %d", i+1, len(id), firstLen)
		}
	}
}

func TestAzureBlockIDNoCollision(t *testing.T) {
	// Different upload IDs should produce different block IDs for the same part number.
	bid1 := blockID("upload-A", 1)
	bid2 := blockID("upload-B", 1)
	if bid1 == bid2 {
		t.Errorf("blockID should differ for different uploadIDs, both = %q", bid1)
	}
}

func TestAzureCopyObjectETag(t *testing.T) {
	backend, _ := newTestAzureBackend(t)
	ctx := context.Background()

	content := "data for copy etag test"
	_, origETag, err := backend.PutObject(ctx, "src-bucket", "orig.txt", strings.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	copyETag, err := backend.CopyObject(ctx, "src-bucket", "orig.txt", "dst-bucket", "copy.txt")
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// ETags should match since content is the same.
	if origETag != copyETag {
		t.Errorf("Copy ETag %q should match original %q", copyETag, origETag)
	}
}

func TestAzureIsAzureNotFound(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"BlobNotFound", fmt.Errorf("BlobNotFound: the specified blob does not exist"), true},
		{"ContainerNotFound", fmt.Errorf("ContainerNotFound: container not accessible"), true},
		{"not found message", fmt.Errorf("resource not found"), true},
		{"404 message", fmt.Errorf("got HTTP 404"), true},
		{"random error", fmt.Errorf("connection refused"), false},
	}

	for _, tc := range tests {
		got := isAzureNotFound(tc.err)
		if got != tc.expected {
			t.Errorf("isAzureNotFound(%v) = %v, want %v", tc.err, got, tc.expected)
		}
	}
}

func TestAzureInterfaceCompliance(t *testing.T) {
	// Verify at compile time that AzureGatewayBackend implements StorageBackend.
	var _ StorageBackend = (*AzureGatewayBackend)(nil)
}

// azureKeysOf returns the keys of a map[string][]byte (used in test output).
func azureKeysOf(m map[string][]byte) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
