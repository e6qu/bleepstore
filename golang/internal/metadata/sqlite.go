package metadata

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite" // Pure-Go SQLite driver
)

const (
	// timeFormat is the ISO 8601 format used for all timestamps in SQLite.
	timeFormat = "2006-01-02T15:04:05.000Z"
)

// SQLiteStore implements the MetadataStore interface using SQLite as the
// backing database. It provides durable, ACID-compliant metadata storage
// suitable for single-node deployments.
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore creates a new SQLiteStore with the given DSN and initializes
// the database schema.
func NewSQLiteStore(dsn string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening SQLite database: %w", err)
	}

	s := &SQLiteStore{db: db}
	if err := s.initDB(); err != nil {
		db.Close()
		return nil, fmt.Errorf("initializing SQLite database: %w", err)
	}
	return s, nil
}

// initDB applies PRAGMAs and creates the required tables and indexes.
// This is safe to call multiple times (idempotent via IF NOT EXISTS).
func (s *SQLiteStore) initDB() error {
	// Apply PRAGMAs for performance and correctness.
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
	}
	for _, p := range pragmas {
		if _, err := s.db.Exec(p); err != nil {
			return fmt.Errorf("executing %q: %w", p, err)
		}
	}

	// Create all tables and indexes.
	schema := `
		CREATE TABLE IF NOT EXISTS schema_version (
			version    INTEGER PRIMARY KEY,
			applied_at TEXT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS buckets (
			name           TEXT PRIMARY KEY,
			region         TEXT NOT NULL DEFAULT 'us-east-1',
			owner_id       TEXT NOT NULL,
			owner_display  TEXT NOT NULL DEFAULT '',
			acl            TEXT NOT NULL DEFAULT '{}',
			created_at     TEXT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS objects (
			bucket              TEXT NOT NULL,
			key                 TEXT NOT NULL,
			size                INTEGER NOT NULL,
			etag                TEXT NOT NULL,
			content_type        TEXT NOT NULL DEFAULT 'application/octet-stream',
			content_encoding    TEXT,
			content_language    TEXT,
			content_disposition TEXT,
			cache_control       TEXT,
			expires             TEXT,
			storage_class       TEXT NOT NULL DEFAULT 'STANDARD',
			acl                 TEXT NOT NULL DEFAULT '{}',
			user_metadata       TEXT NOT NULL DEFAULT '{}',
			last_modified       TEXT NOT NULL,
			delete_marker       INTEGER NOT NULL DEFAULT 0,

			PRIMARY KEY (bucket, key),
			FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
		);

		CREATE INDEX IF NOT EXISTS idx_objects_bucket ON objects(bucket);
		CREATE INDEX IF NOT EXISTS idx_objects_bucket_prefix ON objects(bucket, key);

		CREATE TABLE IF NOT EXISTS multipart_uploads (
			upload_id           TEXT PRIMARY KEY,
			bucket              TEXT NOT NULL,
			key                 TEXT NOT NULL,
			content_type        TEXT NOT NULL DEFAULT 'application/octet-stream',
			content_encoding    TEXT,
			content_language    TEXT,
			content_disposition TEXT,
			cache_control       TEXT,
			expires             TEXT,
			storage_class       TEXT NOT NULL DEFAULT 'STANDARD',
			acl                 TEXT NOT NULL DEFAULT '{}',
			user_metadata       TEXT NOT NULL DEFAULT '{}',
			owner_id            TEXT NOT NULL,
			owner_display       TEXT NOT NULL DEFAULT '',
			initiated_at        TEXT NOT NULL,

			FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
		);

		CREATE INDEX IF NOT EXISTS idx_uploads_bucket ON multipart_uploads(bucket);
		CREATE INDEX IF NOT EXISTS idx_uploads_bucket_key ON multipart_uploads(bucket, key);

		CREATE TABLE IF NOT EXISTS multipart_parts (
			upload_id    TEXT NOT NULL,
			part_number  INTEGER NOT NULL,
			size         INTEGER NOT NULL,
			etag         TEXT NOT NULL,
			last_modified TEXT NOT NULL,

			PRIMARY KEY (upload_id, part_number),
			FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
		);

		CREATE TABLE IF NOT EXISTS credentials (
			access_key_id TEXT PRIMARY KEY,
			secret_key    TEXT NOT NULL,
			owner_id      TEXT NOT NULL,
			display_name  TEXT NOT NULL DEFAULT '',
			active        INTEGER NOT NULL DEFAULT 1,
			created_at    TEXT NOT NULL
		);
	`

	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}

	// Insert initial schema version if not present.
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO schema_version (version, applied_at) VALUES (1, ?)`,
		time.Now().UTC().Format(timeFormat),
	)
	if err != nil {
		return fmt.Errorf("inserting schema version: %w", err)
	}

	return nil
}

// Close closes the underlying SQLite database connection.
func (s *SQLiteStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// ---- Bucket operations ----

// CreateBucket creates a new bucket record in the SQLite database.
func (s *SQLiteStore) CreateBucket(ctx context.Context, bucket *BucketRecord) error {
	acl := "{}"
	if bucket.ACL != nil {
		acl = string(bucket.ACL)
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO buckets (name, region, owner_id, owner_display, acl, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		bucket.Name,
		bucket.Region,
		bucket.OwnerID,
		bucket.OwnerDisplay,
		acl,
		bucket.CreatedAt.UTC().Format(timeFormat),
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") ||
			strings.Contains(err.Error(), "PRIMARY KEY") {
			return fmt.Errorf("bucket already exists: %s", bucket.Name)
		}
		return fmt.Errorf("creating bucket %q: %w", bucket.Name, err)
	}
	return nil
}

// GetBucket retrieves bucket metadata by name.
func (s *SQLiteStore) GetBucket(ctx context.Context, name string) (*BucketRecord, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT name, region, owner_id, owner_display, acl, created_at
		 FROM buckets WHERE name = ?`,
		name,
	)

	var b BucketRecord
	var aclStr, createdAtStr string
	err := row.Scan(&b.Name, &b.Region, &b.OwnerID, &b.OwnerDisplay, &aclStr, &createdAtStr)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting bucket %q: %w", name, err)
	}
	b.ACL = json.RawMessage(aclStr)
	b.CreatedAt, _ = time.Parse(timeFormat, createdAtStr)
	return &b, nil
}

// DeleteBucket removes the named bucket. Returns an error if the bucket
// is not empty (contains objects).
func (s *SQLiteStore) DeleteBucket(ctx context.Context, name string) error {
	// Check if bucket exists.
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM buckets WHERE name = ?`, name,
	).Scan(&count)
	if err != nil {
		return fmt.Errorf("checking bucket %q: %w", name, err)
	}
	if count == 0 {
		return fmt.Errorf("bucket not found: %s", name)
	}

	// Check if bucket is empty.
	err = s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM objects WHERE bucket = ? LIMIT 1`, name,
	).Scan(&count)
	if err != nil {
		return fmt.Errorf("checking bucket contents %q: %w", name, err)
	}
	if count > 0 {
		return fmt.Errorf("bucket not empty: %s", name)
	}

	// Check for in-progress multipart uploads.
	err = s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM multipart_uploads WHERE bucket = ? LIMIT 1`, name,
	).Scan(&count)
	if err != nil {
		return fmt.Errorf("checking bucket uploads %q: %w", name, err)
	}
	if count > 0 {
		return fmt.Errorf("bucket not empty: %s", name)
	}

	_, err = s.db.ExecContext(ctx,
		`DELETE FROM buckets WHERE name = ?`, name,
	)
	if err != nil {
		return fmt.Errorf("deleting bucket %q: %w", name, err)
	}
	return nil
}

// ListBuckets returns all buckets owned by the given owner.
func (s *SQLiteStore) ListBuckets(ctx context.Context, owner string) ([]BucketRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, region, owner_id, owner_display, acl, created_at
		 FROM buckets WHERE owner_id = ?
		 ORDER BY name`,
		owner,
	)
	if err != nil {
		return nil, fmt.Errorf("listing buckets: %w", err)
	}
	defer rows.Close()

	var buckets []BucketRecord
	for rows.Next() {
		var b BucketRecord
		var aclStr, createdAtStr string
		if err := rows.Scan(&b.Name, &b.Region, &b.OwnerID, &b.OwnerDisplay, &aclStr, &createdAtStr); err != nil {
			return nil, fmt.Errorf("scanning bucket row: %w", err)
		}
		b.ACL = json.RawMessage(aclStr)
		b.CreatedAt, _ = time.Parse(timeFormat, createdAtStr)
		buckets = append(buckets, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating bucket rows: %w", err)
	}
	return buckets, nil
}

// BucketExists checks whether the named bucket exists.
func (s *SQLiteStore) BucketExists(ctx context.Context, name string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM buckets WHERE name = ?`, name,
	).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("checking bucket existence %q: %w", name, err)
	}
	return count > 0, nil
}

// UpdateBucketAcl updates the ACL for the named bucket.
func (s *SQLiteStore) UpdateBucketAcl(ctx context.Context, name string, acl json.RawMessage) error {
	result, err := s.db.ExecContext(ctx,
		`UPDATE buckets SET acl = ? WHERE name = ?`,
		string(acl), name,
	)
	if err != nil {
		return fmt.Errorf("updating bucket ACL %q: %w", name, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("bucket not found: %s", name)
	}
	return nil
}

// ---- Object operations ----

// PutObject creates or replaces the metadata for an object.
func (s *SQLiteStore) PutObject(ctx context.Context, obj *ObjectRecord) error {
	userMeta := "{}"
	if obj.UserMetadata != nil {
		b, err := json.Marshal(obj.UserMetadata)
		if err != nil {
			return fmt.Errorf("marshaling user metadata: %w", err)
		}
		userMeta = string(b)
	}

	acl := "{}"
	if obj.ACL != nil {
		acl = string(obj.ACL)
	}

	storageClass := obj.StorageClass
	if storageClass == "" {
		storageClass = "STANDARD"
	}

	contentType := obj.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	deleteMarker := 0
	if obj.DeleteMarker {
		deleteMarker = 1
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO objects
			(bucket, key, size, etag, content_type, content_encoding, content_language,
			 content_disposition, cache_control, expires, storage_class, acl,
			 user_metadata, last_modified, delete_marker)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		obj.Bucket,
		obj.Key,
		obj.Size,
		obj.ETag,
		contentType,
		nullString(obj.ContentEncoding),
		nullString(obj.ContentLanguage),
		nullString(obj.ContentDisposition),
		nullString(obj.CacheControl),
		nullString(obj.Expires),
		storageClass,
		acl,
		userMeta,
		obj.LastModified.UTC().Format(timeFormat),
		deleteMarker,
	)
	if err != nil {
		return fmt.Errorf("putting object %q/%q: %w", obj.Bucket, obj.Key, err)
	}
	return nil
}

// GetObject retrieves object metadata by bucket and key.
func (s *SQLiteStore) GetObject(ctx context.Context, bucket, key string) (*ObjectRecord, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT bucket, key, size, etag, content_type, content_encoding,
				content_language, content_disposition, cache_control, expires,
				storage_class, acl, user_metadata, last_modified, delete_marker
		 FROM objects WHERE bucket = ? AND key = ?`,
		bucket, key,
	)

	obj, err := scanObjectRow(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting object %q/%q: %w", bucket, key, err)
	}
	return obj, nil
}

// DeleteObject removes object metadata by bucket and key.
func (s *SQLiteStore) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM objects WHERE bucket = ? AND key = ?`,
		bucket, key,
	)
	if err != nil {
		return fmt.Errorf("deleting object %q/%q: %w", bucket, key, err)
	}
	return nil
}

// ObjectExists checks whether the named object exists.
func (s *SQLiteStore) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM objects WHERE bucket = ? AND key = ?`,
		bucket, key,
	).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("checking object existence %q/%q: %w", bucket, key, err)
	}
	return count > 0, nil
}

// DeleteObjectsMeta removes metadata for multiple objects. Returns the
// list of keys that were successfully deleted and any errors.
func (s *SQLiteStore) DeleteObjectsMeta(ctx context.Context, bucket string, keys []string) ([]string, []error) {
	var deleted []string
	var errs []error

	for _, key := range keys {
		result, err := s.db.ExecContext(ctx,
			`DELETE FROM objects WHERE bucket = ? AND key = ?`,
			bucket, key,
		)
		if err != nil {
			errs = append(errs, fmt.Errorf("deleting %q: %w", key, err))
			continue
		}
		rows, _ := result.RowsAffected()
		// S3 reports deletion even if the key didn't exist.
		_ = rows
		deleted = append(deleted, key)
	}
	return deleted, errs
}

// UpdateObjectAcl updates the ACL for the specified object.
func (s *SQLiteStore) UpdateObjectAcl(ctx context.Context, bucket, key string, acl json.RawMessage) error {
	result, err := s.db.ExecContext(ctx,
		`UPDATE objects SET acl = ? WHERE bucket = ? AND key = ?`,
		string(acl), bucket, key,
	)
	if err != nil {
		return fmt.Errorf("updating object ACL %q/%q: %w", bucket, key, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("object not found: %s/%s", bucket, key)
	}
	return nil
}

// ListObjects lists objects in the given bucket according to the provided options.
func (s *SQLiteStore) ListObjects(ctx context.Context, bucket string, opts ListObjectsOptions) (*ListObjectsResult, error) {
	maxKeys := opts.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}

	// Determine the start-after key for pagination.
	startAfter := opts.StartAfter
	if opts.ContinuationToken != "" {
		startAfter = opts.ContinuationToken
	}
	if opts.Marker != "" && startAfter == "" {
		startAfter = opts.Marker
	}

	// Build query: select all keys matching prefix, after the start key.
	var args []interface{}
	query := `SELECT bucket, key, size, etag, content_type, content_encoding,
					 content_language, content_disposition, cache_control, expires,
					 storage_class, acl, user_metadata, last_modified, delete_marker
			  FROM objects WHERE bucket = ?`
	args = append(args, bucket)

	if opts.Prefix != "" {
		query += ` AND key LIKE ? || '%' ESCAPE '\'`
		args = append(args, escapeLikePattern(opts.Prefix))
	}

	if startAfter != "" {
		query += ` AND key > ?`
		args = append(args, startAfter)
	}

	query += ` ORDER BY key`
	// Fetch one extra to determine truncation.
	query += fmt.Sprintf(` LIMIT %d`, maxKeys+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("listing objects in %q: %w", bucket, err)
	}
	defer rows.Close()

	var allObjects []ObjectRecord
	for rows.Next() {
		obj, err := scanObjectRows(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning object row: %w", err)
		}
		allObjects = append(allObjects, *obj)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating object rows: %w", err)
	}

	// If no delimiter, return directly.
	if opts.Delimiter == "" {
		isTruncated := len(allObjects) > maxKeys
		if isTruncated {
			allObjects = allObjects[:maxKeys]
		}
		result := &ListObjectsResult{
			Objects:     allObjects,
			IsTruncated: isTruncated,
		}
		if isTruncated && len(allObjects) > 0 {
			lastKey := allObjects[len(allObjects)-1].Key
			result.NextMarker = lastKey
			result.NextContinuationToken = lastKey
		}
		return result, nil
	}

	// With delimiter: group into objects and common prefixes.
	var objects []ObjectRecord
	prefixSet := make(map[string]bool)

	for _, obj := range allObjects {
		// Get the part of the key after the prefix.
		keyAfterPrefix := obj.Key
		if opts.Prefix != "" {
			keyAfterPrefix = obj.Key[len(opts.Prefix):]
		}

		// Find delimiter in the key after the prefix.
		delimIdx := strings.Index(keyAfterPrefix, opts.Delimiter)
		if delimIdx >= 0 {
			// This key has the delimiter: it becomes a common prefix.
			commonPrefix := opts.Prefix + keyAfterPrefix[:delimIdx+len(opts.Delimiter)]
			prefixSet[commonPrefix] = true
		} else {
			objects = append(objects, obj)
		}
	}

	// Sort common prefixes.
	var commonPrefixes []string
	for p := range prefixSet {
		commonPrefixes = append(commonPrefixes, p)
	}
	sort.Strings(commonPrefixes)

	// Count total entries (objects + prefixes) and truncate if needed.
	totalEntries := len(objects) + len(commonPrefixes)
	isTruncated := totalEntries > maxKeys

	// Merge objects and prefixes to determine the correct truncation point.
	// We need to interleave them by key order and take only maxKeys.
	if isTruncated {
		// Simple approach: merge all keys (object keys + prefix keys), sort, take maxKeys.
		type entry struct {
			key      string
			isPrefix bool
		}
		var entries []entry
		for _, obj := range objects {
			entries = append(entries, entry{key: obj.Key, isPrefix: false})
		}
		for _, p := range commonPrefixes {
			entries = append(entries, entry{key: p, isPrefix: true})
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].key < entries[j].key
		})

		// Take maxKeys entries.
		if len(entries) > maxKeys {
			entries = entries[:maxKeys]
		}

		// Separate back.
		objects = nil
		prefixSet = make(map[string]bool)
		for _, e := range entries {
			if e.isPrefix {
				prefixSet[e.key] = true
			} else {
				// Re-find the object.
				for _, obj := range allObjects {
					if obj.Key == e.key {
						objects = append(objects, obj)
						break
					}
				}
			}
		}
		commonPrefixes = nil
		for p := range prefixSet {
			commonPrefixes = append(commonPrefixes, p)
		}
		sort.Strings(commonPrefixes)
	}

	result := &ListObjectsResult{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    isTruncated,
	}
	if isTruncated {
		// NextMarker is the last key in the combined set.
		var lastKey string
		if len(objects) > 0 {
			lastKey = objects[len(objects)-1].Key
		}
		if len(commonPrefixes) > 0 {
			lastPrefix := commonPrefixes[len(commonPrefixes)-1]
			if lastPrefix > lastKey {
				lastKey = lastPrefix
			}
		}
		result.NextMarker = lastKey
		result.NextContinuationToken = lastKey
	}

	return result, nil
}

// ---- Multipart upload operations ----

// generateUploadID generates a unique upload ID using crypto/rand.
func generateUploadID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating upload ID: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// CreateMultipartUpload creates a new multipart upload record.
func (s *SQLiteStore) CreateMultipartUpload(ctx context.Context, upload *MultipartUploadRecord) (string, error) {
	uploadID := upload.UploadID
	if uploadID == "" {
		var err error
		uploadID, err = generateUploadID()
		if err != nil {
			return "", err
		}
	}

	acl := "{}"
	if upload.ACL != nil {
		acl = string(upload.ACL)
	}
	userMeta := "{}"
	if upload.UserMetadata != nil {
		b, err := json.Marshal(upload.UserMetadata)
		if err != nil {
			return "", fmt.Errorf("marshaling user metadata: %w", err)
		}
		userMeta = string(b)
	}

	contentType := upload.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	storageClass := upload.StorageClass
	if storageClass == "" {
		storageClass = "STANDARD"
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO multipart_uploads
			(upload_id, bucket, key, content_type, content_encoding, content_language,
			 content_disposition, cache_control, expires, storage_class, acl,
			 user_metadata, owner_id, owner_display, initiated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		uploadID,
		upload.Bucket,
		upload.Key,
		contentType,
		nullString(upload.ContentEncoding),
		nullString(upload.ContentLanguage),
		nullString(upload.ContentDisposition),
		nullString(upload.CacheControl),
		nullString(upload.Expires),
		storageClass,
		acl,
		userMeta,
		upload.OwnerID,
		upload.OwnerDisplay,
		upload.InitiatedAt.UTC().Format(timeFormat),
	)
	if err != nil {
		return "", fmt.Errorf("creating multipart upload: %w", err)
	}
	return uploadID, nil
}

// GetMultipartUpload retrieves multipart upload metadata.
func (s *SQLiteStore) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*MultipartUploadRecord, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT upload_id, bucket, key, content_type, content_encoding,
				content_language, content_disposition, cache_control, expires,
				storage_class, acl, user_metadata, owner_id, owner_display, initiated_at
		 FROM multipart_uploads
		 WHERE upload_id = ? AND bucket = ? AND key = ?`,
		uploadID, bucket, key,
	)

	var u MultipartUploadRecord
	var contentEncoding, contentLanguage, contentDisposition, cacheControl, expires sql.NullString
	var aclStr, userMetaStr, initiatedAtStr string

	err := row.Scan(
		&u.UploadID, &u.Bucket, &u.Key, &u.ContentType,
		&contentEncoding, &contentLanguage, &contentDisposition,
		&cacheControl, &expires,
		&u.StorageClass, &aclStr, &userMetaStr,
		&u.OwnerID, &u.OwnerDisplay, &initiatedAtStr,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting multipart upload %q: %w", uploadID, err)
	}

	u.ContentEncoding = contentEncoding.String
	u.ContentLanguage = contentLanguage.String
	u.ContentDisposition = contentDisposition.String
	u.CacheControl = cacheControl.String
	u.Expires = expires.String
	u.ACL = json.RawMessage(aclStr)
	u.InitiatedAt, _ = time.Parse(timeFormat, initiatedAtStr)

	if userMetaStr != "" && userMetaStr != "{}" {
		u.UserMetadata = make(map[string]string)
		json.Unmarshal([]byte(userMetaStr), &u.UserMetadata)
	}

	return &u, nil
}

// PutPart records metadata for an uploaded part.
func (s *SQLiteStore) PutPart(ctx context.Context, part *PartRecord) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO multipart_parts
			(upload_id, part_number, size, etag, last_modified)
		 VALUES (?, ?, ?, ?, ?)`,
		part.UploadID,
		part.PartNumber,
		part.Size,
		part.ETag,
		part.LastModified.UTC().Format(timeFormat),
	)
	if err != nil {
		return fmt.Errorf("putting part %d for upload %q: %w", part.PartNumber, part.UploadID, err)
	}
	return nil
}

// ListParts lists parts for the specified multipart upload.
func (s *SQLiteStore) ListParts(ctx context.Context, uploadID string, opts ListPartsOptions) (*ListPartsResult, error) {
	maxParts := opts.MaxParts
	if maxParts <= 0 {
		maxParts = 1000
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT upload_id, part_number, size, etag, last_modified
		 FROM multipart_parts
		 WHERE upload_id = ? AND part_number > ?
		 ORDER BY part_number
		 LIMIT ?`,
		uploadID, opts.PartNumberMarker, maxParts+1,
	)
	if err != nil {
		return nil, fmt.Errorf("listing parts for upload %q: %w", uploadID, err)
	}
	defer rows.Close()

	var parts []PartRecord
	for rows.Next() {
		var p PartRecord
		var lastModifiedStr string
		if err := rows.Scan(&p.UploadID, &p.PartNumber, &p.Size, &p.ETag, &lastModifiedStr); err != nil {
			return nil, fmt.Errorf("scanning part row: %w", err)
		}
		p.LastModified, _ = time.Parse(timeFormat, lastModifiedStr)
		parts = append(parts, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating part rows: %w", err)
	}

	isTruncated := len(parts) > maxParts
	if isTruncated {
		parts = parts[:maxParts]
	}

	result := &ListPartsResult{
		Parts:       parts,
		IsTruncated: isTruncated,
	}
	if isTruncated && len(parts) > 0 {
		result.NextPartNumberMarker = parts[len(parts)-1].PartNumber
	}
	return result, nil
}

// GetPartsForCompletion retrieves part records for the given part numbers.
func (s *SQLiteStore) GetPartsForCompletion(ctx context.Context, uploadID string, partNumbers []int) ([]PartRecord, error) {
	if len(partNumbers) == 0 {
		return nil, nil
	}

	// Build query with IN clause.
	placeholders := make([]string, len(partNumbers))
	args := make([]interface{}, 0, len(partNumbers)+1)
	args = append(args, uploadID)
	for i, pn := range partNumbers {
		placeholders[i] = "?"
		args = append(args, pn)
	}

	query := fmt.Sprintf(
		`SELECT upload_id, part_number, size, etag, last_modified
		 FROM multipart_parts
		 WHERE upload_id = ? AND part_number IN (%s)
		 ORDER BY part_number`,
		strings.Join(placeholders, ", "),
	)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getting parts for completion: %w", err)
	}
	defer rows.Close()

	var parts []PartRecord
	for rows.Next() {
		var p PartRecord
		var lastModifiedStr string
		if err := rows.Scan(&p.UploadID, &p.PartNumber, &p.Size, &p.ETag, &lastModifiedStr); err != nil {
			return nil, fmt.Errorf("scanning part row: %w", err)
		}
		p.LastModified, _ = time.Parse(timeFormat, lastModifiedStr)
		parts = append(parts, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating part rows: %w", err)
	}
	return parts, nil
}

// CompleteMultipartUpload finalizes a multipart upload: inserts the final
// object record and deletes the upload and part records, all in a transaction.
func (s *SQLiteStore) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, obj *ObjectRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert the final object record.
	userMeta := "{}"
	if obj.UserMetadata != nil {
		b, err := json.Marshal(obj.UserMetadata)
		if err != nil {
			return fmt.Errorf("marshaling user metadata: %w", err)
		}
		userMeta = string(b)
	}
	acl := "{}"
	if obj.ACL != nil {
		acl = string(obj.ACL)
	}
	storageClass := obj.StorageClass
	if storageClass == "" {
		storageClass = "STANDARD"
	}
	contentType := obj.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	deleteMarker := 0
	if obj.DeleteMarker {
		deleteMarker = 1
	}

	_, err = tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO objects
			(bucket, key, size, etag, content_type, content_encoding, content_language,
			 content_disposition, cache_control, expires, storage_class, acl,
			 user_metadata, last_modified, delete_marker)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		obj.Bucket, obj.Key, obj.Size, obj.ETag, contentType,
		nullString(obj.ContentEncoding), nullString(obj.ContentLanguage),
		nullString(obj.ContentDisposition), nullString(obj.CacheControl),
		nullString(obj.Expires), storageClass, acl, userMeta,
		obj.LastModified.UTC().Format(timeFormat), deleteMarker,
	)
	if err != nil {
		return fmt.Errorf("inserting object during completion: %w", err)
	}

	// Delete parts.
	_, err = tx.ExecContext(ctx,
		`DELETE FROM multipart_parts WHERE upload_id = ?`, uploadID,
	)
	if err != nil {
		return fmt.Errorf("deleting parts: %w", err)
	}

	// Delete upload record.
	_, err = tx.ExecContext(ctx,
		`DELETE FROM multipart_uploads WHERE upload_id = ?`, uploadID,
	)
	if err != nil {
		return fmt.Errorf("deleting upload record: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

// AbortMultipartUpload cancels a multipart upload and removes all part records.
func (s *SQLiteStore) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	// Delete parts first (foreign key constraint).
	_, err = tx.ExecContext(ctx,
		`DELETE FROM multipart_parts WHERE upload_id = ?`, uploadID,
	)
	if err != nil {
		return fmt.Errorf("deleting parts: %w", err)
	}

	// Delete upload record.
	result, err := tx.ExecContext(ctx,
		`DELETE FROM multipart_uploads WHERE upload_id = ? AND bucket = ? AND key = ?`,
		uploadID, bucket, key,
	)
	if err != nil {
		return fmt.Errorf("deleting upload record: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("upload not found: %s", uploadID)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

// ListMultipartUploads lists in-progress multipart uploads for the given bucket.
func (s *SQLiteStore) ListMultipartUploads(ctx context.Context, bucket string, opts ListUploadsOptions) (*ListUploadsResult, error) {
	maxUploads := opts.MaxUploads
	if maxUploads <= 0 {
		maxUploads = 1000
	}

	var args []interface{}
	query := `SELECT upload_id, bucket, key, content_type, content_encoding,
					 content_language, content_disposition, cache_control, expires,
					 storage_class, acl, user_metadata, owner_id, owner_display, initiated_at
			  FROM multipart_uploads WHERE bucket = ?`
	args = append(args, bucket)

	if opts.Prefix != "" {
		query += ` AND key LIKE ? || '%' ESCAPE '\'`
		args = append(args, escapeLikePattern(opts.Prefix))
	}

	if opts.KeyMarker != "" {
		if opts.UploadIDMarker != "" {
			query += ` AND (key > ? OR (key = ? AND upload_id > ?))`
			args = append(args, opts.KeyMarker, opts.KeyMarker, opts.UploadIDMarker)
		} else {
			query += ` AND key > ?`
			args = append(args, opts.KeyMarker)
		}
	}

	query += ` ORDER BY key, initiated_at`
	query += fmt.Sprintf(` LIMIT %d`, maxUploads+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("listing multipart uploads: %w", err)
	}
	defer rows.Close()

	var uploads []MultipartUploadRecord
	for rows.Next() {
		var u MultipartUploadRecord
		var contentEncoding, contentLanguage, contentDisposition, cacheControl, expires sql.NullString
		var aclStr, userMetaStr, initiatedAtStr string

		if err := rows.Scan(
			&u.UploadID, &u.Bucket, &u.Key, &u.ContentType,
			&contentEncoding, &contentLanguage, &contentDisposition,
			&cacheControl, &expires,
			&u.StorageClass, &aclStr, &userMetaStr,
			&u.OwnerID, &u.OwnerDisplay, &initiatedAtStr,
		); err != nil {
			return nil, fmt.Errorf("scanning upload row: %w", err)
		}

		u.ContentEncoding = contentEncoding.String
		u.ContentLanguage = contentLanguage.String
		u.ContentDisposition = contentDisposition.String
		u.CacheControl = cacheControl.String
		u.Expires = expires.String
		u.ACL = json.RawMessage(aclStr)
		u.InitiatedAt, _ = time.Parse(timeFormat, initiatedAtStr)

		if userMetaStr != "" && userMetaStr != "{}" {
			u.UserMetadata = make(map[string]string)
			json.Unmarshal([]byte(userMetaStr), &u.UserMetadata)
		}

		uploads = append(uploads, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating upload rows: %w", err)
	}

	isTruncated := len(uploads) > maxUploads
	if isTruncated {
		uploads = uploads[:maxUploads]
	}

	result := &ListUploadsResult{
		Uploads:     uploads,
		IsTruncated: isTruncated,
	}
	if isTruncated && len(uploads) > 0 {
		last := uploads[len(uploads)-1]
		result.NextKeyMarker = last.Key
		result.NextUploadIDMarker = last.UploadID
	}
	return result, nil
}

// ---- Credential operations ----

// GetCredential retrieves a credential record by access key ID.
func (s *SQLiteStore) GetCredential(ctx context.Context, accessKeyID string) (*CredentialRecord, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT access_key_id, secret_key, owner_id, display_name, active, created_at
		 FROM credentials WHERE access_key_id = ?`,
		accessKeyID,
	)

	var c CredentialRecord
	var active int
	var createdAtStr string
	err := row.Scan(&c.AccessKeyID, &c.SecretKey, &c.OwnerID, &c.DisplayName, &active, &createdAtStr)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting credential %q: %w", accessKeyID, err)
	}
	c.Active = active != 0
	c.CreatedAt, _ = time.Parse(timeFormat, createdAtStr)
	return &c, nil
}

// PutCredential creates or updates a credential record.
func (s *SQLiteStore) PutCredential(ctx context.Context, cred *CredentialRecord) error {
	active := 0
	if cred.Active {
		active = 1
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO credentials
			(access_key_id, secret_key, owner_id, display_name, active, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		cred.AccessKeyID,
		cred.SecretKey,
		cred.OwnerID,
		cred.DisplayName,
		active,
		cred.CreatedAt.UTC().Format(timeFormat),
	)
	if err != nil {
		return fmt.Errorf("putting credential %q: %w", cred.AccessKeyID, err)
	}
	return nil
}

// ---- Helper functions ----

// nullString converts a Go string to sql.NullString. Empty strings become NULL.
func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// escapeLikePattern escapes special LIKE characters (%, _) in a pattern
// using backslash as the escape character. The caller must append
// ESCAPE '\' to the LIKE clause.
func escapeLikePattern(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "%", `\%`)
	s = strings.ReplaceAll(s, "_", `\_`)
	return s
}

// scanObjectRow scans an object row from a *sql.Row.
func scanObjectRow(row *sql.Row) (*ObjectRecord, error) {
	var obj ObjectRecord
	var contentEncoding, contentLanguage, contentDisposition, cacheControl, expires sql.NullString
	var aclStr, userMetaStr, lastModifiedStr string
	var deleteMarker int

	err := row.Scan(
		&obj.Bucket, &obj.Key, &obj.Size, &obj.ETag, &obj.ContentType,
		&contentEncoding, &contentLanguage, &contentDisposition,
		&cacheControl, &expires,
		&obj.StorageClass, &aclStr, &userMetaStr, &lastModifiedStr, &deleteMarker,
	)
	if err != nil {
		return nil, err
	}

	obj.ContentEncoding = contentEncoding.String
	obj.ContentLanguage = contentLanguage.String
	obj.ContentDisposition = contentDisposition.String
	obj.CacheControl = cacheControl.String
	obj.Expires = expires.String
	obj.ACL = json.RawMessage(aclStr)
	obj.LastModified, _ = time.Parse(timeFormat, lastModifiedStr)
	obj.DeleteMarker = deleteMarker != 0

	if userMetaStr != "" && userMetaStr != "{}" {
		obj.UserMetadata = make(map[string]string)
		json.Unmarshal([]byte(userMetaStr), &obj.UserMetadata)
	}

	return &obj, nil
}

// scanObjectRows scans an object row from *sql.Rows.
func scanObjectRows(rows *sql.Rows) (*ObjectRecord, error) {
	var obj ObjectRecord
	var contentEncoding, contentLanguage, contentDisposition, cacheControl, expires sql.NullString
	var aclStr, userMetaStr, lastModifiedStr string
	var deleteMarker int

	err := rows.Scan(
		&obj.Bucket, &obj.Key, &obj.Size, &obj.ETag, &obj.ContentType,
		&contentEncoding, &contentLanguage, &contentDisposition,
		&cacheControl, &expires,
		&obj.StorageClass, &aclStr, &userMetaStr, &lastModifiedStr, &deleteMarker,
	)
	if err != nil {
		return nil, err
	}

	obj.ContentEncoding = contentEncoding.String
	obj.ContentLanguage = contentLanguage.String
	obj.ContentDisposition = contentDisposition.String
	obj.CacheControl = cacheControl.String
	obj.Expires = expires.String
	obj.ACL = json.RawMessage(aclStr)
	obj.LastModified, _ = time.Parse(timeFormat, lastModifiedStr)
	obj.DeleteMarker = deleteMarker != 0

	if userMetaStr != "" && userMetaStr != "{}" {
		obj.UserMetadata = make(map[string]string)
		json.Unmarshal([]byte(userMetaStr), &obj.UserMetadata)
	}

	return &obj, nil
}
