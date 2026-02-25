package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	_ "modernc.org/sqlite" // Pure-Go SQLite driver
)

// memObject holds the raw data and precomputed ETag for an in-memory object.
type memObject struct {
	Data []byte
	ETag string
}

// memPart holds the raw data and precomputed ETag for a single multipart
// upload part.
type memPart struct {
	Data []byte
	ETag string
}

// MemoryBackend implements the StorageBackend interface using in-memory maps.
// It optionally supports snapshot persistence to a SQLite file so that data
// survives restarts.
type MemoryBackend struct {
	mu          sync.RWMutex
	objects     map[string]memObject // key: "bucket/key"
	parts       map[string]memPart   // key: "uploadID/partNumber"
	currentSize int64
	maxSizeBytes int64

	persistence             string
	snapshotPath            string
	snapshotIntervalSeconds int
	stopCh                  chan struct{}
	wg                      sync.WaitGroup
}

// NewMemoryBackend creates a new MemoryBackend. If persistence is "snapshot",
// it loads any existing snapshot from snapshotPath and starts a background
// goroutine to write periodic snapshots.
func NewMemoryBackend(maxSizeBytes int64, persistence string, snapshotPath string, snapshotIntervalSeconds int) (*MemoryBackend, error) {
	b := &MemoryBackend{
		objects:                 make(map[string]memObject),
		parts:                   make(map[string]memPart),
		maxSizeBytes:            maxSizeBytes,
		persistence:             persistence,
		snapshotPath:            snapshotPath,
		snapshotIntervalSeconds: snapshotIntervalSeconds,
		stopCh:                  make(chan struct{}),
	}

	if persistence == "snapshot" && snapshotPath != "" {
		if err := b.loadSnapshot(); err != nil {
			return nil, fmt.Errorf("loading snapshot: %w", err)
		}

		if snapshotIntervalSeconds > 0 {
			b.wg.Add(1)
			go b.snapshotLoop()
		}
	}

	return b, nil
}

// objectKey builds the map key for an object from its bucket and key.
func objectKey(bucket, key string) string {
	return bucket + "/" + key
}

// partKey builds the map key for a multipart part from its upload ID and part number.
func partKey(uploadID string, partNumber int) string {
	return fmt.Sprintf("%s/%05d", uploadID, partNumber)
}

// computeETag returns the quoted MD5 hex digest of data.
func computeETag(data []byte) string {
	h := md5.Sum(data)
	return fmt.Sprintf(`"%x"`, h[:])
}

// PutObject reads all data from the reader and stores it in memory.
// Returns the number of bytes written and the computed ETag.
func (b *MemoryBackend) PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64) (int64, string, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return 0, "", fmt.Errorf("reading object data: %w", err)
	}

	dataLen := int64(len(data))
	ok := objectKey(bucket, key)
	etag := computeETag(data)

	b.mu.Lock()
	defer b.mu.Unlock()

	// Account for size change if replacing an existing object.
	delta := dataLen
	if existing, found := b.objects[ok]; found {
		delta -= int64(len(existing.Data))
	}

	if b.maxSizeBytes > 0 && b.currentSize+delta > b.maxSizeBytes {
		return 0, "", fmt.Errorf("memory limit exceeded: current=%d, delta=%d, max=%d", b.currentSize, delta, b.maxSizeBytes)
	}

	b.objects[ok] = memObject{Data: data, ETag: etag}
	b.currentSize += delta

	return dataLen, etag, nil
}

// GetObject returns a ReadCloser over the in-memory data, the object size,
// and its ETag. Returns an error if the object does not exist.
func (b *MemoryBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, int64, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ok := objectKey(bucket, key)
	obj, found := b.objects[ok]
	if !found {
		return nil, 0, "", fmt.Errorf("object not found: %s/%s", bucket, key)
	}

	// Return a copy of the data so callers cannot mutate the stored slice.
	dataCopy := make([]byte, len(obj.Data))
	copy(dataCopy, obj.Data)

	return io.NopCloser(bytes.NewReader(dataCopy)), int64(len(obj.Data)), obj.ETag, nil
}

// DeleteObject removes an object from memory. Idempotent: deleting a
// non-existent object is not an error.
func (b *MemoryBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	ok := objectKey(bucket, key)
	if obj, found := b.objects[ok]; found {
		b.currentSize -= int64(len(obj.Data))
		delete(b.objects, ok)
	}

	return nil
}

// CopyObject copies an object from source to destination within memory.
// Returns the ETag of the destination object (same data, same ETag).
func (b *MemoryBackend) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	srcOK := objectKey(srcBucket, srcKey)
	obj, found := b.objects[srcOK]
	if !found {
		return "", fmt.Errorf("source object not found: %s/%s", srcBucket, srcKey)
	}

	// Copy the data slice so source and destination are independent.
	dataCopy := make([]byte, len(obj.Data))
	copy(dataCopy, obj.Data)

	dstOK := objectKey(dstBucket, dstKey)
	delta := int64(len(dataCopy))
	if existing, found := b.objects[dstOK]; found {
		delta -= int64(len(existing.Data))
	}

	if b.maxSizeBytes > 0 && b.currentSize+delta > b.maxSizeBytes {
		return "", fmt.Errorf("memory limit exceeded: current=%d, delta=%d, max=%d", b.currentSize, delta, b.maxSizeBytes)
	}

	etag := computeETag(dataCopy)
	b.objects[dstOK] = memObject{Data: dataCopy, ETag: etag}
	b.currentSize += delta

	return etag, nil
}

// PutPart stores a single multipart upload part in memory.
func (b *MemoryBackend) PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, reader io.Reader, size int64) (string, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("reading part data: %w", err)
	}

	dataLen := int64(len(data))
	pk := partKey(uploadID, partNumber)
	etag := computeETag(data)

	b.mu.Lock()
	defer b.mu.Unlock()

	delta := dataLen
	if existing, found := b.parts[pk]; found {
		delta -= int64(len(existing.Data))
	}

	if b.maxSizeBytes > 0 && b.currentSize+delta > b.maxSizeBytes {
		return "", fmt.Errorf("memory limit exceeded: current=%d, delta=%d, max=%d", b.currentSize, delta, b.maxSizeBytes)
	}

	b.parts[pk] = memPart{Data: data, ETag: etag}
	b.currentSize += delta

	return etag, nil
}

// AssembleParts concatenates the specified parts into a single object and
// stores it in memory. The parts are removed after assembly. Returns the
// composite ETag in the standard S3 multipart format.
func (b *MemoryBackend) AssembleParts(ctx context.Context, bucket, key, uploadID string, partNumbers []int) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Collect parts and compute composite ETag.
	var assembled []byte
	compositeMD5 := md5.New()

	for _, pn := range partNumbers {
		pk := partKey(uploadID, pn)
		part, found := b.parts[pk]
		if !found {
			return "", fmt.Errorf("part not found: uploadID=%s partNumber=%d", uploadID, pn)
		}
		assembled = append(assembled, part.Data...)

		partHash := md5.Sum(part.Data)
		compositeMD5.Write(partHash[:])
	}

	// Store the assembled object.
	ok := objectKey(bucket, key)
	assembledLen := int64(len(assembled))

	// Calculate net size change: add assembled object, remove parts.
	delta := assembledLen
	if existing, found := b.objects[ok]; found {
		delta -= int64(len(existing.Data))
	}

	// Remove all parts for this upload and adjust size.
	partsRemoved := b.removePartsLocked(uploadID)
	delta -= partsRemoved

	if b.maxSizeBytes > 0 && b.currentSize+delta > b.maxSizeBytes {
		return "", fmt.Errorf("memory limit exceeded: current=%d, delta=%d, max=%d", b.currentSize, delta, b.maxSizeBytes)
	}

	etag := fmt.Sprintf(`"%x-%d"`, compositeMD5.Sum(nil), len(partNumbers))
	b.objects[ok] = memObject{Data: assembled, ETag: etag}
	b.currentSize += delta

	return etag, nil
}

// DeleteParts removes all parts associated with the given multipart upload.
func (b *MemoryBackend) DeleteParts(ctx context.Context, bucket, key, uploadID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	removed := b.removePartsLocked(uploadID)
	b.currentSize -= removed

	return nil
}

// DeleteUploadParts removes all parts for a specific multipart upload. This is
// used during startup reaping of expired uploads to clean up orphaned parts.
func (b *MemoryBackend) DeleteUploadParts(uploadID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	removed := b.removePartsLocked(uploadID)
	b.currentSize -= removed

	return nil
}

// removePartsLocked removes all parts matching the given uploadID from the
// parts map and returns the total bytes removed. The caller must hold b.mu.
func (b *MemoryBackend) removePartsLocked(uploadID string) int64 {
	prefix := uploadID + "/"
	var removed int64
	for k, part := range b.parts {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			removed += int64(len(part.Data))
			delete(b.parts, k)
		}
	}
	return removed
}

// CreateBucket is a no-op for the memory backend. Bucket existence is tracked
// by the metadata store, not the storage backend.
func (b *MemoryBackend) CreateBucket(ctx context.Context, bucket string) error {
	return nil
}

// DeleteBucket is a no-op for the memory backend. Bucket existence is tracked
// by the metadata store, not the storage backend.
func (b *MemoryBackend) DeleteBucket(ctx context.Context, bucket string) error {
	return nil
}

// ObjectExists checks whether an object exists in the in-memory map.
func (b *MemoryBackend) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	_, found := b.objects[objectKey(bucket, key)]
	return found, nil
}

// HealthCheck always returns nil for the memory backend since there is no
// external dependency to verify.
func (b *MemoryBackend) HealthCheck(ctx context.Context) error {
	return nil
}

// Close shuts down the memory backend. If snapshot persistence is enabled, it
// stops the background goroutine and writes a final snapshot.
func (b *MemoryBackend) Close() error {
	close(b.stopCh)
	b.wg.Wait()

	if b.persistence == "snapshot" && b.snapshotPath != "" {
		if err := b.writeSnapshot(); err != nil {
			return fmt.Errorf("writing final snapshot: %w", err)
		}
	}

	return nil
}

// snapshotLoop runs in a background goroutine and periodically writes
// snapshots at the configured interval.
func (b *MemoryBackend) snapshotLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(time.Duration(b.snapshotIntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			if err := b.writeSnapshot(); err != nil {
				log.Printf("ERROR: memory backend snapshot failed: %v", err)
			}
		}
	}
}

// loadSnapshot restores the in-memory state from a SQLite snapshot file.
// If the file does not exist, this is a no-op (fresh start).
func (b *MemoryBackend) loadSnapshot() error {
	if _, err := os.Stat(b.snapshotPath); os.IsNotExist(err) {
		return nil
	}

	db, err := sql.Open("sqlite", b.snapshotPath)
	if err != nil {
		return fmt.Errorf("opening snapshot database: %w", err)
	}
	defer db.Close()

	// Apply PRAGMAs for read performance.
	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		return fmt.Errorf("setting journal mode: %w", err)
	}

	// Check if tables exist before querying.
	var tableCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('object_snapshots', 'part_snapshots')`).Scan(&tableCount)
	if err != nil {
		return fmt.Errorf("checking snapshot tables: %w", err)
	}
	if tableCount == 0 {
		return nil
	}

	// Load objects.
	if tableCount >= 1 {
		rows, err := db.Query("SELECT bucket, key, data, etag FROM object_snapshots")
		if err != nil {
			return fmt.Errorf("querying object snapshots: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var bucket, key, etag string
			var data []byte
			if err := rows.Scan(&bucket, &key, &data, &etag); err != nil {
				return fmt.Errorf("scanning object snapshot row: %w", err)
			}
			ok := objectKey(bucket, key)
			b.objects[ok] = memObject{Data: data, ETag: etag}
			b.currentSize += int64(len(data))
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterating object snapshot rows: %w", err)
		}
	}

	// Load parts.
	if tableCount >= 2 {
		rows, err := db.Query("SELECT upload_id, part_number, data, etag FROM part_snapshots")
		if err != nil {
			return fmt.Errorf("querying part snapshots: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var uploadID, etag string
			var partNumber int
			var data []byte
			if err := rows.Scan(&uploadID, &partNumber, &data, &etag); err != nil {
				return fmt.Errorf("scanning part snapshot row: %w", err)
			}
			pk := partKey(uploadID, partNumber)
			b.parts[pk] = memPart{Data: data, ETag: etag}
			b.currentSize += int64(len(data))
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterating part snapshot rows: %w", err)
		}
	}

	return nil
}

// writeSnapshot atomically writes the current in-memory state to a SQLite
// snapshot file. It writes to a temporary file first, then renames it to
// the final path for crash safety.
func (b *MemoryBackend) writeSnapshot() error {
	b.mu.RLock()
	objectsCopy := make(map[string]memObject, len(b.objects))
	for k, v := range b.objects {
		objectsCopy[k] = v
	}
	partsCopy := make(map[string]memPart, len(b.parts))
	for k, v := range b.parts {
		partsCopy[k] = v
	}
	b.mu.RUnlock()

	// Ensure the parent directory exists.
	dir := filepath.Dir(b.snapshotPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating snapshot directory: %w", err)
	}

	tmpPath := b.snapshotPath + ".tmp"

	// Remove any stale temp file from a previous failed attempt.
	os.Remove(tmpPath)

	db, err := sql.Open("sqlite", tmpPath)
	if err != nil {
		return fmt.Errorf("creating temp snapshot database: %w", err)
	}

	// Create schema.
	schema := `
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = FULL;

		CREATE TABLE object_snapshots (
			bucket TEXT NOT NULL,
			key    TEXT NOT NULL,
			data   BLOB NOT NULL,
			etag   TEXT NOT NULL,
			PRIMARY KEY (bucket, key)
		);

		CREATE TABLE part_snapshots (
			upload_id   TEXT NOT NULL,
			part_number INTEGER NOT NULL,
			data        BLOB NOT NULL,
			etag        TEXT NOT NULL,
			PRIMARY KEY (upload_id, part_number)
		);
	`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("creating snapshot schema: %w", err)
	}

	// Write objects.
	tx, err := db.Begin()
	if err != nil {
		db.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("beginning snapshot transaction: %w", err)
	}

	objStmt, err := tx.Prepare("INSERT INTO object_snapshots (bucket, key, data, etag) VALUES (?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		db.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("preparing object insert: %w", err)
	}
	defer objStmt.Close()

	// Sort keys for deterministic output.
	objectKeys := make([]string, 0, len(objectsCopy))
	for k := range objectsCopy {
		objectKeys = append(objectKeys, k)
	}
	sort.Strings(objectKeys)

	for _, ok := range objectKeys {
		obj := objectsCopy[ok]
		// Split "bucket/key" back into bucket and key.
		bucket, key := splitObjectKey(ok)
		if _, err := objStmt.Exec(bucket, key, obj.Data, obj.ETag); err != nil {
			tx.Rollback()
			db.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("inserting object snapshot for %q: %w", ok, err)
		}
	}

	partStmt, err := tx.Prepare("INSERT INTO part_snapshots (upload_id, part_number, data, etag) VALUES (?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		db.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("preparing part insert: %w", err)
	}
	defer partStmt.Close()

	// Sort part keys for deterministic output.
	partKeys := make([]string, 0, len(partsCopy))
	for k := range partsCopy {
		partKeys = append(partKeys, k)
	}
	sort.Strings(partKeys)

	for _, pk := range partKeys {
		part := partsCopy[pk]
		uploadID, partNumber := splitPartKey(pk)
		if _, err := partStmt.Exec(uploadID, partNumber, part.Data, part.ETag); err != nil {
			tx.Rollback()
			db.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("inserting part snapshot for %q: %w", pk, err)
		}
	}

	if err := tx.Commit(); err != nil {
		db.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("committing snapshot transaction: %w", err)
	}

	if err := db.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("closing temp snapshot database: %w", err)
	}

	// Atomic rename: temp -> final path.
	if err := os.Rename(tmpPath, b.snapshotPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("renaming snapshot file: %w", err)
	}

	// Clean up WAL and SHM files from the temp database (they may linger
	// after rename on some platforms).
	os.Remove(tmpPath + "-wal")
	os.Remove(tmpPath + "-shm")

	return nil
}

// splitObjectKey splits a "bucket/key" string back into bucket and key.
// The bucket is everything before the first slash; the key is the rest.
func splitObjectKey(ok string) (bucket, key string) {
	for i := 0; i < len(ok); i++ {
		if ok[i] == '/' {
			return ok[:i], ok[i+1:]
		}
	}
	return ok, ""
}

// splitPartKey splits a "uploadID/partNumber" string back into its components.
// The upload ID is everything before the last slash; the part number is the rest.
func splitPartKey(pk string) (uploadID string, partNumber int) {
	for i := len(pk) - 1; i >= 0; i-- {
		if pk[i] == '/' {
			uploadID = pk[:i]
			fmt.Sscanf(pk[i+1:], "%d", &partNumber)
			return
		}
	}
	return pk, 0
}

// Ensure MemoryBackend implements StorageBackend at compile time.
var _ StorageBackend = (*MemoryBackend)(nil)
