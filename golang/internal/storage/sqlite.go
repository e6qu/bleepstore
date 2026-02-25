package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
	"sort"

	_ "modernc.org/sqlite" // Pure-Go SQLite driver
)

// SQLiteBackend implements the StorageBackend interface using SQLite as the
// underlying data store. Object and part data are stored as BLOBs directly
// in the database, making this suitable for small-to-medium objects in
// single-node or embedded deployments.
type SQLiteBackend struct {
	db *sql.DB
}

// NewSQLiteBackend creates a new SQLiteBackend backed by the given database
// file path. It opens the database, applies performance PRAGMAs, and creates
// the required tables.
func NewSQLiteBackend(dbPath string) (*SQLiteBackend, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening SQLite storage database: %w", err)
	}

	b := &SQLiteBackend{db: db}
	if err := b.initDB(); err != nil {
		db.Close()
		return nil, fmt.Errorf("initializing SQLite storage database: %w", err)
	}
	return b, nil
}

// initDB applies PRAGMAs and creates the required tables.
func (b *SQLiteBackend) initDB() error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA busy_timeout = 5000",
	}
	for _, p := range pragmas {
		if _, err := b.db.Exec(p); err != nil {
			return fmt.Errorf("executing %q: %w", p, err)
		}
	}

	schema := `
		CREATE TABLE IF NOT EXISTS object_data (
			bucket TEXT NOT NULL,
			key    TEXT NOT NULL,
			data   BLOB NOT NULL,
			etag   TEXT NOT NULL,
			PRIMARY KEY (bucket, key)
		);

		CREATE TABLE IF NOT EXISTS part_data (
			upload_id   TEXT    NOT NULL,
			part_number INTEGER NOT NULL,
			data        BLOB    NOT NULL,
			etag        TEXT    NOT NULL,
			PRIMARY KEY (upload_id, part_number)
		);
	`
	if _, err := b.db.Exec(schema); err != nil {
		return fmt.Errorf("creating storage schema: %w", err)
	}
	return nil
}

// Close closes the underlying SQLite database connection.
func (b *SQLiteBackend) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

// PutObject reads all data from the reader, computes an MD5 ETag, and stores
// the data as a BLOB in the object_data table. Uses INSERT OR REPLACE so that
// re-uploads overwrite the existing row.
func (b *SQLiteBackend) PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64) (int64, string, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return 0, "", fmt.Errorf("reading object data: %w", err)
	}

	etag := computeETag(data)

	_, err = b.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO object_data (bucket, key, data, etag) VALUES (?, ?, ?, ?)`,
		bucket, key, data, etag,
	)
	if err != nil {
		return 0, "", fmt.Errorf("putting object %q/%q: %w", bucket, key, err)
	}

	return int64(len(data)), etag, nil
}

// GetObject retrieves the object data and ETag from the object_data table.
// Returns an io.NopCloser wrapping a bytes.Reader for the data, the data
// size, the ETag, and any error. Returns an error if the object is not found.
func (b *SQLiteBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, int64, string, error) {
	var data []byte
	var etag string

	err := b.db.QueryRowContext(ctx,
		`SELECT data, etag FROM object_data WHERE bucket = ? AND key = ?`,
		bucket, key,
	).Scan(&data, &etag)
	if err == sql.ErrNoRows {
		return nil, 0, "", fmt.Errorf("object not found: %s/%s", bucket, key)
	}
	if err != nil {
		return nil, 0, "", fmt.Errorf("getting object %q/%q: %w", bucket, key, err)
	}

	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), etag, nil
}

// DeleteObject removes the object data from the object_data table.
// Idempotent: deleting a non-existent object is not an error.
func (b *SQLiteBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := b.db.ExecContext(ctx,
		`DELETE FROM object_data WHERE bucket = ? AND key = ?`,
		bucket, key,
	)
	if err != nil {
		return fmt.Errorf("deleting object %q/%q: %w", bucket, key, err)
	}
	return nil
}

// CopyObject copies an object from the source bucket/key to the destination
// bucket/key by reading the source data and inserting it at the destination.
// Returns the ETag of the copied object (which is the same as the source).
func (b *SQLiteBackend) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (string, error) {
	var data []byte
	var etag string

	err := b.db.QueryRowContext(ctx,
		`SELECT data, etag FROM object_data WHERE bucket = ? AND key = ?`,
		srcBucket, srcKey,
	).Scan(&data, &etag)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("source object not found: %s/%s", srcBucket, srcKey)
	}
	if err != nil {
		return "", fmt.Errorf("reading source object %q/%q: %w", srcBucket, srcKey, err)
	}

	_, err = b.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO object_data (bucket, key, data, etag) VALUES (?, ?, ?, ?)`,
		dstBucket, dstKey, data, etag,
	)
	if err != nil {
		return "", fmt.Errorf("writing destination object %q/%q: %w", dstBucket, dstKey, err)
	}

	return etag, nil
}

// PutPart writes a single part of a multipart upload into the part_data table.
// Uses INSERT OR REPLACE so re-uploading the same part number overwrites the
// previous data.
func (b *SQLiteBackend) PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, reader io.Reader, size int64) (string, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("reading part data: %w", err)
	}

	etag := computeETag(data)

	_, err = b.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO part_data (upload_id, part_number, data, etag) VALUES (?, ?, ?, ?)`,
		uploadID, partNumber, data, etag,
	)
	if err != nil {
		return "", fmt.Errorf("putting part %d for upload %q: %w", partNumber, uploadID, err)
	}

	return etag, nil
}

// AssembleParts concatenates the specified parts into a single object and stores
// the result in the object_data table. The composite ETag is computed by hashing
// the concatenation of the individual part MD5 digests, producing the standard
// S3 multipart ETag format: "md5-of-part-md5s-N".
func (b *SQLiteBackend) AssembleParts(ctx context.Context, bucket, key, uploadID string, partNumbers []int) (string, error) {
	// Sort part numbers to ensure correct assembly order.
	sorted := make([]int, len(partNumbers))
	copy(sorted, partNumbers)
	sort.Ints(sorted)

	var assembled bytes.Buffer
	compositeMD5 := md5.New()

	for _, pn := range sorted {
		var data []byte
		err := b.db.QueryRowContext(ctx,
			`SELECT data FROM part_data WHERE upload_id = ? AND part_number = ?`,
			uploadID, pn,
		).Scan(&data)
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("part %d not found for upload %q", pn, uploadID)
		}
		if err != nil {
			return "", fmt.Errorf("reading part %d for upload %q: %w", pn, uploadID, err)
		}

		assembled.Write(data)

		// Compute MD5 of this individual part and feed it into the composite hash.
		partHash := md5.Sum(data)
		compositeMD5.Write(partHash[:])
	}

	// Composite ETag format: "md5-of-concatenated-part-md5s-N"
	etag := fmt.Sprintf(`"%x-%d"`, compositeMD5.Sum(nil), len(sorted))

	// Store the assembled object.
	_, err := b.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO object_data (bucket, key, data, etag) VALUES (?, ?, ?, ?)`,
		bucket, key, assembled.Bytes(), etag,
	)
	if err != nil {
		return "", fmt.Errorf("storing assembled object %q/%q: %w", bucket, key, err)
	}

	// Clean up parts for this upload.
	_, err = b.db.ExecContext(ctx,
		`DELETE FROM part_data WHERE upload_id = ?`,
		uploadID,
	)
	if err != nil {
		return "", fmt.Errorf("cleaning up parts for upload %q: %w", uploadID, err)
	}

	return etag, nil
}

// DeleteParts removes all part data associated with the given multipart upload.
func (b *SQLiteBackend) DeleteParts(ctx context.Context, bucket, key, uploadID string) error {
	_, err := b.db.ExecContext(ctx,
		`DELETE FROM part_data WHERE upload_id = ?`,
		uploadID,
	)
	if err != nil {
		return fmt.Errorf("deleting parts for upload %q: %w", uploadID, err)
	}
	return nil
}

// DeleteUploadParts removes all part data for a specific upload ID. This is
// used during startup reaping of expired uploads to clean up orphaned part
// data in the database.
func (b *SQLiteBackend) DeleteUploadParts(uploadID string) error {
	_, err := b.db.Exec(
		`DELETE FROM part_data WHERE upload_id = ?`,
		uploadID,
	)
	if err != nil {
		return fmt.Errorf("deleting upload parts for %q: %w", uploadID, err)
	}
	return nil
}

// CreateBucket is a no-op for the SQLite backend because bucket organization
// is handled entirely through the (bucket, key) composite keys in the tables.
// The metadata layer tracks bucket existence.
func (b *SQLiteBackend) CreateBucket(ctx context.Context, bucket string) error {
	return nil
}

// DeleteBucket is a no-op for the SQLite backend. Object data rows are deleted
// individually via DeleteObject. The metadata layer manages bucket lifecycle.
func (b *SQLiteBackend) DeleteBucket(ctx context.Context, bucket string) error {
	return nil
}

// ObjectExists checks whether an object exists in the object_data table.
func (b *SQLiteBackend) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	var count int
	err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM object_data WHERE bucket = ? AND key = ?`,
		bucket, key,
	).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("checking object existence %q/%q: %w", bucket, key, err)
	}
	return count > 0, nil
}

// HealthCheck verifies that the SQLite storage database is operational by
// executing a simple query.
func (b *SQLiteBackend) HealthCheck(ctx context.Context) error {
	var n int
	return b.db.QueryRowContext(ctx, `SELECT 1`).Scan(&n)
}
