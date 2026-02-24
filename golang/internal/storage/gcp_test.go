package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
)

// mockGCSClient implements GCSAPI for unit testing.
type mockGCSClient struct {
	// objects stores all objects keyed by their GCS object name.
	objects map[string][]byte
	// putCalls tracks the number of write operations.
	putCalls int
	// deleteCalls tracks the number of delete calls.
	deleteCalls int
	// copyCalls tracks the number of copy calls.
	copyCalls int
	// composeCalls tracks the number of compose calls.
	composeCalls int
	// attrsCalls tracks the number of attrs calls.
	attrsCalls int
}

func newMockGCSClient() *mockGCSClient {
	return &mockGCSClient{
		objects: make(map[string][]byte),
	}
}

// mockGCSWriter implements GCSWriter for testing.
type mockGCSWriter struct {
	buf    *bytes.Buffer
	client *mockGCSClient
	key    string
}

func (w *mockGCSWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *mockGCSWriter) Close() error {
	w.client.objects[w.key] = w.buf.Bytes()
	w.client.putCalls++
	return nil
}

func (m *mockGCSClient) NewWriter(ctx context.Context, bucket, object string) GCSWriter {
	return &mockGCSWriter{
		buf:    &bytes.Buffer{},
		client: m,
		key:    object,
	}
}

func (m *mockGCSClient) NewReader(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	data, ok := m.objects[object]
	if !ok {
		return nil, fmt.Errorf("storage: object doesn't exist: not found")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockGCSClient) Delete(ctx context.Context, bucket, object string) error {
	m.deleteCalls++
	_, ok := m.objects[object]
	if !ok {
		return fmt.Errorf("storage: object doesn't exist: not found")
	}
	delete(m.objects, object)
	return nil
}

func (m *mockGCSClient) Attrs(ctx context.Context, bucket, object string) (*GCSAttrs, error) {
	m.attrsCalls++
	data, ok := m.objects[object]
	if !ok {
		return nil, fmt.Errorf("storage: object doesn't exist: not found")
	}
	h := md5.Sum(data)
	return &GCSAttrs{
		Size: int64(len(data)),
		MD5:  h[:],
	}, nil
}

func (m *mockGCSClient) Copy(ctx context.Context, bucket, srcObject, dstObject string) (*GCSAttrs, error) {
	m.copyCalls++
	data, ok := m.objects[srcObject]
	if !ok {
		return nil, fmt.Errorf("storage: object doesn't exist: not found")
	}
	copied := make([]byte, len(data))
	copy(copied, data)
	m.objects[dstObject] = copied

	h := md5.Sum(copied)
	return &GCSAttrs{
		Size: int64(len(copied)),
		MD5:  h[:],
	}, nil
}

func (m *mockGCSClient) Compose(ctx context.Context, bucket, dstObject string, srcObjects []string) (*GCSAttrs, error) {
	m.composeCalls++
	var assembled bytes.Buffer
	for _, src := range srcObjects {
		data, ok := m.objects[src]
		if !ok {
			return nil, fmt.Errorf("storage: object doesn't exist: %s: not found", src)
		}
		assembled.Write(data)
	}
	result := assembled.Bytes()
	m.objects[dstObject] = result

	h := md5.Sum(result)
	return &GCSAttrs{
		Size: int64(len(result)),
		MD5:  h[:],
	}, nil
}

func (m *mockGCSClient) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	var names []string
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			names = append(names, key)
		}
	}
	sort.Strings(names)
	return names, nil
}

// --- Test helpers ---

func newTestGCPBackend(t *testing.T) (*GCPGatewayBackend, *mockGCSClient) {
	t.Helper()
	mock := newMockGCSClient()
	backend := NewGCPGatewayBackendWithClient("test-upstream-bucket", "test-project", "bp/", mock)
	return backend, mock
}

// --- Tests ---

func TestGCPPutAndGetObject(t *testing.T) {
	backend, _ := newTestGCPBackend(t)
	ctx := context.Background()

	content := "Hello, GCP Gateway!"
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

func TestGCPPutObjectEmptyBody(t *testing.T) {
	backend, _ := newTestGCPBackend(t)
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

func TestGCPGetObjectNotFound(t *testing.T) {
	backend, _ := newTestGCPBackend(t)
	ctx := context.Background()

	_, _, _, err := backend.GetObject(ctx, "my-bucket", "nonexistent.txt")
	if err == nil {
		t.Fatal("GetObject should fail for non-existent object")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should mention 'not found', got: %v", err)
	}
}

func TestGCPDeleteObject(t *testing.T) {
	backend, mock := newTestGCPBackend(t)
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

func TestGCPDeleteObjectIdempotent(t *testing.T) {
	backend, _ := newTestGCPBackend(t)
	ctx := context.Background()

	// Delete a non-existent object: should not error (idempotent).
	if err := backend.DeleteObject(ctx, "my-bucket", "nonexistent.txt"); err != nil {
		t.Errorf("DeleteObject (non-existent) should not error, got: %v", err)
	}
}

func TestGCPCopyObject(t *testing.T) {
	backend, _ := newTestGCPBackend(t)
	ctx := context.Background()

	content := "copy me via GCP"
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

func TestGCPCopyObjectNotFound(t *testing.T) {
	backend, _ := newTestGCPBackend(t)
	ctx := context.Background()

	_, err := backend.CopyObject(ctx, "src-bucket", "nonexistent.txt", "dst-bucket", "copy.txt")
	if err == nil {
		t.Fatal("CopyObject should fail for non-existent source")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should mention 'not found', got: %v", err)
	}
}

func TestGCPObjectExists(t *testing.T) {
	backend, _ := newTestGCPBackend(t)
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

func TestGCPCreateDeleteBucketNoOp(t *testing.T) {
	backend, _ := newTestGCPBackend(t)
	ctx := context.Background()

	// CreateBucket and DeleteBucket are no-ops for GCP gateway.
	if err := backend.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Errorf("CreateBucket should not error: %v", err)
	}
	if err := backend.DeleteBucket(ctx, "test-bucket"); err != nil {
		t.Errorf("DeleteBucket should not error: %v", err)
	}
}

func TestGCPKeyMapping(t *testing.T) {
	backend, mock := newTestGCPBackend(t)
	ctx := context.Background()

	// Verify key mapping: {prefix}{bucket}/{key}
	_, _, err := backend.PutObject(ctx, "my-bucket", "path/to/file.txt", strings.NewReader("data"), 4)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	expectedKey := "bp/my-bucket/path/to/file.txt"
	if _, ok := mock.objects[expectedKey]; !ok {
		t.Errorf("Object should be stored at key %q", expectedKey)
		t.Logf("Keys in mock: %v", gcsKeysOf(mock.objects))
	}
}

func TestGCPKeyMappingNoPrefix(t *testing.T) {
	mock := newMockGCSClient()
	backend := NewGCPGatewayBackendWithClient("test-bucket", "test-project", "", mock)
	ctx := context.Background()

	_, _, err := backend.PutObject(ctx, "my-bucket", "file.txt", strings.NewReader("data"), 4)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	expectedKey := "my-bucket/file.txt"
	if _, ok := mock.objects[expectedKey]; !ok {
		t.Errorf("Object should be stored at key %q (no prefix)", expectedKey)
		t.Logf("Keys in mock: %v", gcsKeysOf(mock.objects))
	}
}

func TestGCPPutPartAndDeleteParts(t *testing.T) {
	backend, mock := newTestGCPBackend(t)
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

func TestGCPAssemblePartsSingleCompose(t *testing.T) {
	backend, mock := newTestGCPBackend(t)
	ctx := context.Background()

	// Upload 3 parts (under 32 limit).
	_, err := backend.PutPart(ctx, "my-bucket", "assembled.txt", "upload-single", 1, strings.NewReader("part1"), 5)
	if err != nil {
		t.Fatalf("PutPart 1 failed: %v", err)
	}
	_, err = backend.PutPart(ctx, "my-bucket", "assembled.txt", "upload-single", 2, strings.NewReader("part2"), 5)
	if err != nil {
		t.Fatalf("PutPart 2 failed: %v", err)
	}
	_, err = backend.PutPart(ctx, "my-bucket", "assembled.txt", "upload-single", 3, strings.NewReader("part3"), 5)
	if err != nil {
		t.Fatalf("PutPart 3 failed: %v", err)
	}

	// Assemble.
	etag, err := backend.AssembleParts(ctx, "my-bucket", "assembled.txt", "upload-single", []int{1, 2, 3})
	if err != nil {
		t.Fatalf("AssembleParts failed: %v", err)
	}
	if etag == "" {
		t.Error("ETag should not be empty after assembly")
	}

	// Verify the assembled object exists.
	finalKey := "bp/my-bucket/assembled.txt"
	data, ok := mock.objects[finalKey]
	if !ok {
		t.Fatalf("Assembled object should exist at %q", finalKey)
	}
	if string(data) != "part1part2part3" {
		t.Errorf("Assembled data = %q, want %q", string(data), "part1part2part3")
	}

	// Verify only 1 compose call was made (under 32 limit).
	if mock.composeCalls != 1 {
		t.Errorf("Expected 1 compose call, got %d", mock.composeCalls)
	}

	// Verify ETag matches content MD5.
	h := md5.Sum([]byte("part1part2part3"))
	expectedETag := fmt.Sprintf(`"%x"`, h)
	if etag != expectedETag {
		t.Errorf("ETag = %q, want %q", etag, expectedETag)
	}
}

func TestGCPAssemblePartsChainCompose(t *testing.T) {
	backend, mock := newTestGCPBackend(t)
	ctx := context.Background()

	// Upload 35 parts (over 32 limit, needs chain compose).
	var allData bytes.Buffer
	for i := 1; i <= 35; i++ {
		partData := fmt.Sprintf("p%02d", i)
		allData.WriteString(partData)
		_, err := backend.PutPart(ctx, "my-bucket", "big.txt", "upload-chain", i, strings.NewReader(partData), int64(len(partData)))
		if err != nil {
			t.Fatalf("PutPart %d failed: %v", i, err)
		}
	}

	partNumbers := make([]int, 35)
	for i := range partNumbers {
		partNumbers[i] = i + 1
	}

	etag, err := backend.AssembleParts(ctx, "my-bucket", "big.txt", "upload-chain", partNumbers)
	if err != nil {
		t.Fatalf("AssembleParts (chain) failed: %v", err)
	}
	if etag == "" {
		t.Error("ETag should not be empty")
	}

	// Verify the assembled data.
	finalKey := "bp/my-bucket/big.txt"
	data, ok := mock.objects[finalKey]
	if !ok {
		t.Fatalf("Assembled object should exist at %q", finalKey)
	}
	if string(data) != allData.String() {
		t.Errorf("Assembled data length = %d, want %d", len(data), allData.Len())
	}

	// Verify that chain compose was used (multiple compose calls).
	if mock.composeCalls < 2 {
		t.Errorf("Expected multiple compose calls for >32 parts, got %d", mock.composeCalls)
	}
}

func TestGCPPutObjectETagConsistency(t *testing.T) {
	backend, _ := newTestGCPBackend(t)
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

func TestGCPPutPartETagConsistency(t *testing.T) {
	backend, _ := newTestGCPBackend(t)
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

func TestGCPPutObjectOverwrite(t *testing.T) {
	backend, _ := newTestGCPBackend(t)
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

func TestGCPGCSKeyMapping(t *testing.T) {
	backend, _ := newTestGCPBackend(t)

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
		got := backend.gcsKey(tc.bucket, tc.key)
		if got != tc.expected {
			t.Errorf("gcsKey(%q, %q) = %q, want %q", tc.bucket, tc.key, got, tc.expected)
		}
	}
}

func TestGCPPartKeyMapping(t *testing.T) {
	backend, _ := newTestGCPBackend(t)

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

func TestGCPInterfaceCompliance(t *testing.T) {
	// Verify at compile time that GCPGatewayBackend implements StorageBackend.
	var _ StorageBackend = (*GCPGatewayBackend)(nil)
}

func TestGCPDeletePartsNoParts(t *testing.T) {
	backend, _ := newTestGCPBackend(t)
	ctx := context.Background()

	// Deleting parts for a non-existent upload should not error.
	if err := backend.DeleteParts(ctx, "my-bucket", "key", "nonexistent-upload"); err != nil {
		t.Errorf("DeleteParts for non-existent upload should not error, got: %v", err)
	}
}

func TestGCPAssembleSinglePart(t *testing.T) {
	backend, mock := newTestGCPBackend(t)
	ctx := context.Background()

	// Upload a single part.
	_, err := backend.PutPart(ctx, "my-bucket", "single.txt", "upload-one", 1, strings.NewReader("only-part"), 9)
	if err != nil {
		t.Fatalf("PutPart failed: %v", err)
	}

	// Assemble with single part should use single compose call.
	etag, err := backend.AssembleParts(ctx, "my-bucket", "single.txt", "upload-one", []int{1})
	if err != nil {
		t.Fatalf("AssembleParts (single) failed: %v", err)
	}
	if etag == "" {
		t.Error("ETag should not be empty")
	}

	// Verify the assembled object.
	finalKey := "bp/my-bucket/single.txt"
	data, ok := mock.objects[finalKey]
	if !ok {
		t.Fatalf("Assembled object should exist at %q", finalKey)
	}
	if string(data) != "only-part" {
		t.Errorf("Assembled data = %q, want %q", string(data), "only-part")
	}
}

func TestGCPCopyObjectETag(t *testing.T) {
	backend, _ := newTestGCPBackend(t)
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

func TestGCPIsGCSNotFound(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"not found message", fmt.Errorf("storage: object doesn't exist: not found"), true},
		{"404 message", fmt.Errorf("got HTTP 404"), true},
		{"random error", fmt.Errorf("connection refused"), false},
	}

	for _, tc := range tests {
		got := isGCSNotFound(tc.err)
		if got != tc.expected {
			t.Errorf("isGCSNotFound(%v) = %v, want %v", tc.err, got, tc.expected)
		}
	}
}

// gcsKeysOf returns the keys of a map[string][]byte (used in test output).
func gcsKeysOf(m map[string][]byte) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
