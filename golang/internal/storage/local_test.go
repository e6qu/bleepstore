package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestBackend(t *testing.T) *LocalBackend {
	t.Helper()
	rootDir := t.TempDir()
	backend, err := NewLocalBackend(rootDir)
	if err != nil {
		t.Fatalf("NewLocalBackend failed: %v", err)
	}
	return backend
}

func TestPutAndGetObject(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	// Create bucket directory.
	if err := backend.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Put object.
	content := "Hello, BleepStore!"
	bytesWritten, etag, err := backend.PutObject(ctx, "test-bucket", "hello.txt", strings.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if bytesWritten != int64(len(content)) {
		t.Errorf("bytesWritten = %d, want %d", bytesWritten, len(content))
	}

	if etag == "" {
		t.Error("PutObject: etag is empty")
	}

	// ETag should be quoted hex MD5.
	if !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) {
		t.Errorf("ETag not quoted: %q", etag)
	}

	// Get object.
	reader, size, _, err := backend.GetObject(ctx, "test-bucket", "hello.txt")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	if size != int64(len(content)) {
		t.Errorf("GetObject size = %d, want %d", size, len(content))
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(data) != content {
		t.Errorf("GetObject data = %q, want %q", string(data), content)
	}
}

func TestPutObjectNestedKey(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	if err := backend.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	content := "nested content"
	_, _, err := backend.PutObject(ctx, "test-bucket", "path/to/deep/file.txt", strings.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("PutObject (nested) failed: %v", err)
	}

	// Verify the file exists.
	reader, _, _, err := backend.GetObject(ctx, "test-bucket", "path/to/deep/file.txt")
	if err != nil {
		t.Fatalf("GetObject (nested) failed: %v", err)
	}
	defer reader.Close()

	data, _ := io.ReadAll(reader)
	if string(data) != content {
		t.Errorf("nested data = %q, want %q", string(data), content)
	}
}

func TestPutObjectAtomicWrite(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	if err := backend.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Put an object; verify no temp files remain in .tmp.
	content := "atomic write test"
	_, _, err := backend.PutObject(ctx, "test-bucket", "atomic.txt", strings.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Check the .tmp directory is clean.
	tmpDir := filepath.Join(backend.RootDir, ".tmp")
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir .tmp failed: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf(".tmp directory should be empty after PutObject, has %d entries", len(entries))
	}

	// Verify the object file exists at the expected path.
	objPath := filepath.Join(backend.RootDir, "test-bucket", "atomic.txt")
	if _, err := os.Stat(objPath); os.IsNotExist(err) {
		t.Error("Object file does not exist at expected path")
	}
}

func TestDeleteObject(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	if err := backend.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	content := "delete me"
	_, _, err := backend.PutObject(ctx, "test-bucket", "delete.txt", strings.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Delete the object.
	if err := backend.DeleteObject(ctx, "test-bucket", "delete.txt"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Verify it's gone.
	exists, err := backend.ObjectExists(ctx, "test-bucket", "delete.txt")
	if err != nil {
		t.Fatalf("ObjectExists failed: %v", err)
	}
	if exists {
		t.Error("Object should not exist after deletion")
	}
}

func TestDeleteObjectIdempotent(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	if err := backend.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Delete a non-existent object: should not error.
	if err := backend.DeleteObject(ctx, "test-bucket", "nonexistent.txt"); err != nil {
		t.Errorf("DeleteObject (non-existent) should not error, got: %v", err)
	}
}

func TestDeleteObjectCleansEmptyDirs(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	if err := backend.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	content := "nested delete"
	_, _, err := backend.PutObject(ctx, "test-bucket", "a/b/c/file.txt", strings.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if err := backend.DeleteObject(ctx, "test-bucket", "a/b/c/file.txt"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Empty parent directories a/b/c, a/b, a should be cleaned up.
	aDir := filepath.Join(backend.RootDir, "test-bucket", "a")
	if _, err := os.Stat(aDir); !os.IsNotExist(err) {
		t.Errorf("Expected empty parent dir %q to be removed", aDir)
	}

	// Bucket directory should still exist.
	bucketDir := filepath.Join(backend.RootDir, "test-bucket")
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		t.Error("Bucket directory should still exist")
	}
}

func TestObjectExists(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	if err := backend.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Should not exist.
	exists, err := backend.ObjectExists(ctx, "test-bucket", "nope.txt")
	if err != nil {
		t.Fatalf("ObjectExists failed: %v", err)
	}
	if exists {
		t.Error("ObjectExists should return false for non-existent object")
	}

	// Put it.
	_, _, err = backend.PutObject(ctx, "test-bucket", "yep.txt", strings.NewReader("data"), 4)
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Should exist.
	exists, err = backend.ObjectExists(ctx, "test-bucket", "yep.txt")
	if err != nil {
		t.Fatalf("ObjectExists failed: %v", err)
	}
	if !exists {
		t.Error("ObjectExists should return true for existing object")
	}
}

func TestCleanTempFiles(t *testing.T) {
	backend := newTestBackend(t)

	// Create some fake temp files in .tmp.
	tmpDir := filepath.Join(backend.RootDir, ".tmp")
	for _, name := range []string{"tmp-abc123", "tmp-def456"} {
		path := filepath.Join(tmpDir, name)
		if err := os.WriteFile(path, []byte("orphan"), 0o644); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}
	}

	// Verify files exist.
	entries, _ := os.ReadDir(tmpDir)
	if len(entries) != 2 {
		t.Fatalf("Expected 2 temp files, got %d", len(entries))
	}

	// Clean them.
	if err := backend.CleanTempFiles(); err != nil {
		t.Fatalf("CleanTempFiles failed: %v", err)
	}

	// Verify they're gone.
	entries, _ = os.ReadDir(tmpDir)
	if len(entries) != 0 {
		t.Errorf("Expected 0 temp files after cleanup, got %d", len(entries))
	}
}

func TestCopyObject(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	if err := backend.CreateBucket(ctx, "src-bucket"); err != nil {
		t.Fatalf("CreateBucket src failed: %v", err)
	}
	if err := backend.CreateBucket(ctx, "dst-bucket"); err != nil {
		t.Fatalf("CreateBucket dst failed: %v", err)
	}

	content := "copy me"
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

	// Verify the copied object has the right content.
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

func TestGetObjectNotFound(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	if err := backend.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, _, _, err := backend.GetObject(ctx, "test-bucket", "nonexistent.txt")
	if err == nil {
		t.Error("GetObject should return error for non-existent object")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("GetObject error should mention 'not found', got: %v", err)
	}
}

func TestPutObjectEmptyBody(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	if err := backend.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	bytesWritten, etag, err := backend.PutObject(ctx, "test-bucket", "empty.txt", strings.NewReader(""), 0)
	if err != nil {
		t.Fatalf("PutObject (empty) failed: %v", err)
	}

	if bytesWritten != 0 {
		t.Errorf("bytesWritten = %d, want 0", bytesWritten)
	}

	if etag == "" {
		t.Error("ETag should not be empty even for empty object")
	}

	// Get it back.
	reader, size, _, err := backend.GetObject(ctx, "test-bucket", "empty.txt")
	if err != nil {
		t.Fatalf("GetObject (empty) failed: %v", err)
	}
	defer reader.Close()

	if size != 0 {
		t.Errorf("size = %d, want 0", size)
	}
}

func TestCreateAndDeleteBucket(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	// Create.
	if err := backend.CreateBucket(ctx, "my-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	bucketDir := filepath.Join(backend.RootDir, "my-bucket")
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		t.Error("Bucket directory should exist after creation")
	}

	// Delete.
	if err := backend.DeleteBucket(ctx, "my-bucket"); err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}
	if _, err := os.Stat(bucketDir); !os.IsNotExist(err) {
		t.Error("Bucket directory should not exist after deletion")
	}
}

func TestPutObjectOverwrite(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	if err := backend.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Put first version.
	_, etag1, err := backend.PutObject(ctx, "test-bucket", "overwrite.txt", strings.NewReader("version 1"), 9)
	if err != nil {
		t.Fatalf("PutObject v1 failed: %v", err)
	}

	// Put second version (different content, same key).
	_, etag2, err := backend.PutObject(ctx, "test-bucket", "overwrite.txt", strings.NewReader("version 2!!"), 11)
	if err != nil {
		t.Fatalf("PutObject v2 failed: %v", err)
	}

	if etag1 == etag2 {
		t.Error("ETags should differ for different content")
	}

	// Verify the file contains the second version.
	reader, _, _, err := backend.GetObject(ctx, "test-bucket", "overwrite.txt")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	data, _ := io.ReadAll(reader)
	if string(data) != "version 2!!" {
		t.Errorf("data = %q, want %q", string(data), "version 2!!")
	}
}
