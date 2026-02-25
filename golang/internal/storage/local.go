package storage

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/bleepstore/bleepstore/internal/uid"
)

// LocalBackend implements the StorageBackend interface using the local
// filesystem. Objects are stored as files within a configurable root directory,
// organized by bucket and key path.
type LocalBackend struct {
	// RootDir is the base directory under which all bucket and object data
	// is stored.
	RootDir string
}

// NewLocalBackend creates a new LocalBackend rooted at the given directory.
// It creates the root directory and the temp directory if they do not exist.
func NewLocalBackend(rootDir string) (*LocalBackend, error) {
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating storage root directory %q: %w", rootDir, err)
	}
	// Create the .tmp directory for atomic writes.
	tmpDir := filepath.Join(rootDir, ".tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating temp directory %q: %w", tmpDir, err)
	}
	return &LocalBackend{RootDir: rootDir}, nil
}

// CleanTempFiles removes all files in the .tmp directory. This is called on
// startup as part of crash-only recovery. Any temp files left behind indicate
// incomplete writes from a previous crash.
func (b *LocalBackend) CleanTempFiles() error {
	tmpDir := filepath.Join(b.RootDir, ".tmp")
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("reading temp directory: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			os.Remove(filepath.Join(tmpDir, entry.Name()))
		}
	}
	return nil
}

// objectPath returns the full filesystem path for an object.
func (b *LocalBackend) objectPath(bucket, key string) string {
	return filepath.Join(b.RootDir, bucket, key)
}

// tempPath returns a unique temporary file path in the .tmp directory.
func (b *LocalBackend) tempPath() string {
	return filepath.Join(b.RootDir, ".tmp", "tmp-"+uid.New())
}

// PutObject writes object data to a file on the local filesystem using the
// crash-only atomic write pattern: write to temp file, fsync, rename.
// Returns the number of bytes written and the ETag (MD5 hex digest).
func (b *LocalBackend) PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64) (int64, string, error) {
	objPath := b.objectPath(bucket, key)

	// Ensure parent directories exist.
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return 0, "", fmt.Errorf("creating parent directories for %q/%q: %w", bucket, key, err)
	}

	// Write to a temp file, computing MD5 along the way.
	tmpPath := b.tempPath()
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return 0, "", fmt.Errorf("creating temp file: %w", err)
	}

	// Hash while writing via TeeReader.
	h := md5.New()
	tee := io.TeeReader(reader, h)

	bytesWritten, err := io.Copy(tmpFile, tee)
	if err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return 0, "", fmt.Errorf("writing object data: %w", err)
	}

	// Fsync before rename to guarantee durability.
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return 0, "", fmt.Errorf("syncing temp file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return 0, "", fmt.Errorf("closing temp file: %w", err)
	}

	// Atomic rename: temp -> final path.
	if err := os.Rename(tmpPath, objPath); err != nil {
		os.Remove(tmpPath)
		return 0, "", fmt.Errorf("renaming temp file to final path: %w", err)
	}

	etag := fmt.Sprintf(`"%x"`, h.Sum(nil))
	return bytesWritten, etag, nil
}

// GetObject opens the object file for reading. Returns the file as a
// ReadCloser, the file size, and an empty ETag (metadata store holds the ETag).
// The caller is responsible for closing the returned ReadCloser.
func (b *LocalBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, int64, string, error) {
	objPath := b.objectPath(bucket, key)

	file, err := os.Open(objPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, "", fmt.Errorf("object not found: %s/%s", bucket, key)
		}
		return nil, 0, "", fmt.Errorf("opening object file %q/%q: %w", bucket, key, err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, 0, "", fmt.Errorf("stat object file %q/%q: %w", bucket, key, err)
	}

	return file, info.Size(), "", nil
}

// DeleteObject removes the object file from the local filesystem.
// Idempotent: deleting a non-existent file is not an error.
// Also cleans up empty parent directories up to the bucket root.
func (b *LocalBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	objPath := b.objectPath(bucket, key)

	err := os.Remove(objPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing object file %q/%q: %w", bucket, key, err)
	}

	// Clean up empty parent directories up to the bucket root.
	bucketDir := filepath.Join(b.RootDir, bucket)
	dir := filepath.Dir(objPath)
	for dir != bucketDir && dir != b.RootDir {
		if err := os.Remove(dir); err != nil {
			// Directory not empty or other error: stop climbing.
			break
		}
		dir = filepath.Dir(dir)
	}

	return nil
}

// CopyObject copies an object file from source to destination on the local
// filesystem using the atomic write pattern. Returns the new ETag.
func (b *LocalBackend) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (string, error) {
	srcPath := b.objectPath(srcBucket, srcKey)

	srcFile, err := os.Open(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("source object not found: %s/%s", srcBucket, srcKey)
		}
		return "", fmt.Errorf("opening source object: %w", err)
	}
	defer srcFile.Close()

	info, err := srcFile.Stat()
	if err != nil {
		return "", fmt.Errorf("stat source object: %w", err)
	}

	_, etag, err := b.PutObject(ctx, dstBucket, dstKey, srcFile, info.Size())
	if err != nil {
		return "", fmt.Errorf("copying object data: %w", err)
	}

	return etag, nil
}

// PutPart writes a single multipart upload part to the local filesystem.
func (b *LocalBackend) PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, reader io.Reader, size int64) (string, error) {
	partDir := filepath.Join(b.RootDir, ".multipart", uploadID)
	if err := os.MkdirAll(partDir, 0o755); err != nil {
		return "", fmt.Errorf("creating part directory: %w", err)
	}

	partPath := filepath.Join(partDir, fmt.Sprintf("%05d", partNumber))

	// Write to temp file, compute MD5, atomic rename.
	tmpPath := b.tempPath()
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return "", fmt.Errorf("creating temp file for part: %w", err)
	}

	h := md5.New()
	tee := io.TeeReader(reader, h)

	if _, err := io.Copy(tmpFile, tee); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("writing part data: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("syncing part file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("closing part temp file: %w", err)
	}

	if err := os.Rename(tmpPath, partPath); err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("renaming part temp file: %w", err)
	}

	etag := fmt.Sprintf(`"%x"`, h.Sum(nil))
	return etag, nil
}

// AssembleParts concatenates the specified parts into a single object file.
// Uses atomic write pattern. Returns the composite ETag.
func (b *LocalBackend) AssembleParts(ctx context.Context, bucket, key, uploadID string, partNumbers []int) (string, error) {
	objPath := b.objectPath(bucket, key)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return "", fmt.Errorf("creating parent directories: %w", err)
	}

	partDir := filepath.Join(b.RootDir, ".multipart", uploadID)
	tmpPath := b.tempPath()
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return "", fmt.Errorf("creating temp file for assembly: %w", err)
	}

	// Concatenate parts and compute composite ETag from individual part MD5s.
	compositeMD5 := md5.New()
	for _, pn := range partNumbers {
		partPath := filepath.Join(partDir, fmt.Sprintf("%05d", pn))
		partFile, err := os.Open(partPath)
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return "", fmt.Errorf("opening part %d: %w", pn, err)
		}

		// Compute MD5 of this part while copying.
		partHash := md5.New()
		tee := io.TeeReader(partFile, partHash)
		if _, err := io.Copy(tmpFile, tee); err != nil {
			partFile.Close()
			tmpFile.Close()
			os.Remove(tmpPath)
			return "", fmt.Errorf("copying part %d: %w", pn, err)
		}
		partFile.Close()

		// Add part MD5 to composite hash.
		compositeMD5.Write(partHash.Sum(nil))
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("syncing assembled file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("closing assembled temp file: %w", err)
	}

	if err := os.Rename(tmpPath, objPath); err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("renaming assembled file: %w", err)
	}

	// Composite ETag format: "md5-of-concatenated-part-md5s-N"
	etag := fmt.Sprintf(`"%x-%d"`, compositeMD5.Sum(nil), len(partNumbers))

	// Clean up part files.
	os.RemoveAll(partDir)

	return etag, nil
}

// DeleteParts removes all part files associated with the given multipart upload.
func (b *LocalBackend) DeleteParts(ctx context.Context, bucket, key, uploadID string) error {
	partDir := filepath.Join(b.RootDir, ".multipart", uploadID)
	err := os.RemoveAll(partDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing part directory %q: %w", partDir, err)
	}

	// Best-effort cleanup: remove .multipart dir if empty.
	multipartDir := filepath.Join(b.RootDir, ".multipart")
	os.Remove(multipartDir) // Fails silently if not empty.

	return nil
}

// DeleteUploadParts removes the parts directory for a specific multipart upload.
// This is used during startup reaping of expired uploads to clean up orphaned
// part files on disk.
func (b *LocalBackend) DeleteUploadParts(uploadID string) error {
	partDir := filepath.Join(b.RootDir, ".multipart", uploadID)
	err := os.RemoveAll(partDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing part directory %q: %w", partDir, err)
	}

	// Best-effort cleanup: remove .multipart dir if empty.
	multipartDir := filepath.Join(b.RootDir, ".multipart")
	os.Remove(multipartDir) // Fails silently if not empty.

	return nil
}

// CreateBucket creates a directory for the bucket under the root directory.
func (b *LocalBackend) CreateBucket(ctx context.Context, bucket string) error {
	bucketDir := filepath.Join(b.RootDir, bucket)
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		return fmt.Errorf("creating bucket directory %q: %w", bucketDir, err)
	}
	return nil
}

// DeleteBucket removes the bucket directory from the local filesystem.
// The directory must be empty.
func (b *LocalBackend) DeleteBucket(ctx context.Context, bucket string) error {
	bucketDir := filepath.Join(b.RootDir, bucket)
	// os.Remove only removes empty directories, which is the desired behavior.
	err := os.Remove(bucketDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing bucket directory %q: %w", bucketDir, err)
	}
	return nil
}

// ObjectExists checks whether an object exists on the local filesystem.
func (b *LocalBackend) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	objPath := filepath.Join(b.RootDir, bucket, key)
	info, err := os.Stat(objPath)
	if err == nil {
		// Make sure it's a file, not a directory.
		if info.IsDir() {
			return false, nil
		}
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("checking object existence %q/%q: %w", bucket, key, err)
}

// HealthCheck verifies that the local storage root directory is accessible.
func (b *LocalBackend) HealthCheck(ctx context.Context) error {
	_, err := os.Stat(b.RootDir)
	return err
}

// cleanEmptyParents removes empty directories starting from dir up to (but not
// including) stopAt. This is useful for cleaning up after object deletion when
// keys contain "/" separators that create subdirectories.
func cleanEmptyParents(dir, stopAt string) {
	// Normalize paths for comparison.
	dir = filepath.Clean(dir)
	stopAt = filepath.Clean(stopAt)

	for dir != stopAt && strings.HasPrefix(dir, stopAt) {
		if err := os.Remove(dir); err != nil {
			break
		}
		dir = filepath.Dir(dir)
	}
}
