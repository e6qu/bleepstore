package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// newTestStore creates a SQLiteStore backed by a temporary database file.
// The database is automatically cleaned up when the test finishes.
func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore(%q) failed: %v", dbPath, err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// seedBucket creates a test bucket and returns the record.
func seedBucket(t *testing.T, store *SQLiteStore, name string) *BucketRecord {
	t.Helper()
	bucket := &BucketRecord{
		Name:         name,
		Region:       "us-east-1",
		OwnerID:      "test-owner",
		OwnerDisplay: "Test Owner",
		ACL:          json.RawMessage(`{}`),
		CreatedAt:    time.Now().UTC().Truncate(time.Millisecond),
	}
	if err := store.CreateBucket(context.Background(), bucket); err != nil {
		t.Fatalf("CreateBucket(%q) failed: %v", name, err)
	}
	return bucket
}

// ---- Bucket tests ----

func TestBucketCRUD(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Create bucket.
	bucket := &BucketRecord{
		Name:         "my-bucket",
		Region:       "us-west-2",
		OwnerID:      "owner1",
		OwnerDisplay: "Owner One",
		ACL:          json.RawMessage(`{"owner":{"id":"owner1"}}`),
		CreatedAt:    time.Date(2026, 2, 22, 12, 0, 0, 0, time.UTC),
	}
	if err := store.CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Get bucket.
	got, err := store.GetBucket(ctx, "my-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if got == nil {
		t.Fatal("GetBucket returned nil")
	}
	if got.Name != "my-bucket" {
		t.Errorf("Name = %q, want %q", got.Name, "my-bucket")
	}
	if got.Region != "us-west-2" {
		t.Errorf("Region = %q, want %q", got.Region, "us-west-2")
	}
	if got.OwnerID != "owner1" {
		t.Errorf("OwnerID = %q, want %q", got.OwnerID, "owner1")
	}
	if got.OwnerDisplay != "Owner One" {
		t.Errorf("OwnerDisplay = %q, want %q", got.OwnerDisplay, "Owner One")
	}

	// BucketExists.
	exists, err := store.BucketExists(ctx, "my-bucket")
	if err != nil {
		t.Fatalf("BucketExists: %v", err)
	}
	if !exists {
		t.Error("BucketExists returned false, want true")
	}

	// Non-existent bucket.
	exists, err = store.BucketExists(ctx, "no-such-bucket")
	if err != nil {
		t.Fatalf("BucketExists: %v", err)
	}
	if exists {
		t.Error("BucketExists returned true for non-existent bucket")
	}

	// GetBucket for non-existent.
	got, err = store.GetBucket(ctx, "no-such-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if got != nil {
		t.Errorf("GetBucket(non-existent) = %v, want nil", got)
	}

	// Delete bucket.
	if err := store.DeleteBucket(ctx, "my-bucket"); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	// Verify deleted.
	exists, err = store.BucketExists(ctx, "my-bucket")
	if err != nil {
		t.Fatalf("BucketExists: %v", err)
	}
	if exists {
		t.Error("BucketExists returned true after deletion")
	}
}

func TestBucketDuplicateCreate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	bucket := &BucketRecord{
		Name:      "dup-bucket",
		Region:    "us-east-1",
		OwnerID:   "owner1",
		CreatedAt: time.Now().UTC(),
	}
	if err := store.CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Second create should fail.
	err := store.CreateBucket(ctx, bucket)
	if err == nil {
		t.Error("Expected error on duplicate CreateBucket, got nil")
	}
}

func TestDeleteBucketNotEmpty(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "has-objects")

	// Add an object to the bucket.
	obj := &ObjectRecord{
		Bucket:       "has-objects",
		Key:          "file.txt",
		Size:         100,
		ETag:         `"abc123"`,
		ContentType:  "text/plain",
		StorageClass: "STANDARD",
		LastModified: time.Now().UTC(),
	}
	if err := store.PutObject(ctx, obj); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// Delete should fail because bucket is not empty.
	err := store.DeleteBucket(ctx, "has-objects")
	if err == nil {
		t.Error("Expected error deleting non-empty bucket, got nil")
	}
}

func TestDeleteBucketNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	err := store.DeleteBucket(ctx, "no-such-bucket")
	if err == nil {
		t.Error("Expected error deleting non-existent bucket, got nil")
	}
}

func TestListBuckets(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Create 3 buckets for owner1 and 1 for owner2.
	for _, name := range []string{"alpha", "charlie", "bravo"} {
		bucket := &BucketRecord{
			Name:      name,
			Region:    "us-east-1",
			OwnerID:   "owner1",
			CreatedAt: time.Now().UTC(),
		}
		if err := store.CreateBucket(ctx, bucket); err != nil {
			t.Fatalf("CreateBucket(%q): %v", name, err)
		}
	}
	other := &BucketRecord{
		Name:      "other-bucket",
		Region:    "eu-west-1",
		OwnerID:   "owner2",
		CreatedAt: time.Now().UTC(),
	}
	if err := store.CreateBucket(ctx, other); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// List for owner1 — should be sorted alphabetically.
	buckets, err := store.ListBuckets(ctx, "owner1")
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(buckets) != 3 {
		t.Fatalf("ListBuckets returned %d buckets, want 3", len(buckets))
	}
	if buckets[0].Name != "alpha" || buckets[1].Name != "bravo" || buckets[2].Name != "charlie" {
		t.Errorf("Buckets not sorted: %v", []string{buckets[0].Name, buckets[1].Name, buckets[2].Name})
	}

	// List for owner2.
	buckets, err = store.ListBuckets(ctx, "owner2")
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(buckets) != 1 {
		t.Fatalf("ListBuckets returned %d buckets, want 1", len(buckets))
	}
}

func TestUpdateBucketAcl(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "acl-bucket")

	newACL := json.RawMessage(`{"grants":[{"permission":"READ"}]}`)
	if err := store.UpdateBucketAcl(ctx, "acl-bucket", newACL); err != nil {
		t.Fatalf("UpdateBucketAcl: %v", err)
	}

	got, err := store.GetBucket(ctx, "acl-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if string(got.ACL) != string(newACL) {
		t.Errorf("ACL = %s, want %s", string(got.ACL), string(newACL))
	}

	// Update non-existent bucket.
	err = store.UpdateBucketAcl(ctx, "no-such-bucket", newACL)
	if err == nil {
		t.Error("Expected error updating ACL for non-existent bucket")
	}
}

// ---- Object tests ----

func TestObjectCRUD(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "obj-bucket")

	// Put object.
	now := time.Now().UTC().Truncate(time.Millisecond)
	obj := &ObjectRecord{
		Bucket:             "obj-bucket",
		Key:                "path/to/file.txt",
		Size:               1024,
		ETag:               `"d41d8cd98f00b204e9800998ecf8427e"`,
		ContentType:        "text/plain",
		ContentEncoding:    "gzip",
		ContentLanguage:    "en-US",
		ContentDisposition: "attachment",
		CacheControl:       "max-age=3600",
		Expires:            "Mon, 02 Jan 2026 15:04:05 GMT",
		StorageClass:       "STANDARD",
		ACL:                json.RawMessage(`{"owner":{"id":"owner1"}}`),
		UserMetadata:       map[string]string{"x-amz-meta-author": "tester"},
		LastModified:       now,
	}
	if err := store.PutObject(ctx, obj); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// Get object.
	got, err := store.GetObject(ctx, "obj-bucket", "path/to/file.txt")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if got == nil {
		t.Fatal("GetObject returned nil")
	}
	if got.Size != 1024 {
		t.Errorf("Size = %d, want %d", got.Size, 1024)
	}
	if got.ETag != `"d41d8cd98f00b204e9800998ecf8427e"` {
		t.Errorf("ETag = %q", got.ETag)
	}
	if got.ContentType != "text/plain" {
		t.Errorf("ContentType = %q", got.ContentType)
	}
	if got.ContentEncoding != "gzip" {
		t.Errorf("ContentEncoding = %q", got.ContentEncoding)
	}
	if got.ContentLanguage != "en-US" {
		t.Errorf("ContentLanguage = %q", got.ContentLanguage)
	}
	if got.ContentDisposition != "attachment" {
		t.Errorf("ContentDisposition = %q", got.ContentDisposition)
	}
	if got.CacheControl != "max-age=3600" {
		t.Errorf("CacheControl = %q", got.CacheControl)
	}
	if got.Expires != "Mon, 02 Jan 2026 15:04:05 GMT" {
		t.Errorf("Expires = %q", got.Expires)
	}
	if got.UserMetadata["x-amz-meta-author"] != "tester" {
		t.Errorf("UserMetadata = %v", got.UserMetadata)
	}

	// ObjectExists.
	exists, err := store.ObjectExists(ctx, "obj-bucket", "path/to/file.txt")
	if err != nil {
		t.Fatalf("ObjectExists: %v", err)
	}
	if !exists {
		t.Error("ObjectExists returned false, want true")
	}

	// Non-existent object.
	exists, err = store.ObjectExists(ctx, "obj-bucket", "no-such-key")
	if err != nil {
		t.Fatalf("ObjectExists: %v", err)
	}
	if exists {
		t.Error("ObjectExists returned true for non-existent object")
	}

	// GetObject for non-existent.
	got, err = store.GetObject(ctx, "obj-bucket", "no-such-key")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if got != nil {
		t.Errorf("GetObject(non-existent) = %v, want nil", got)
	}

	// Delete object.
	if err := store.DeleteObject(ctx, "obj-bucket", "path/to/file.txt"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	// Verify deleted.
	exists, err = store.ObjectExists(ctx, "obj-bucket", "path/to/file.txt")
	if err != nil {
		t.Fatalf("ObjectExists: %v", err)
	}
	if exists {
		t.Error("ObjectExists returned true after deletion")
	}
}

func TestPutObjectUpsert(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "upsert-bucket")

	// First put.
	obj := &ObjectRecord{
		Bucket:       "upsert-bucket",
		Key:          "key1",
		Size:         100,
		ETag:         `"aaa"`,
		ContentType:  "text/plain",
		LastModified: time.Now().UTC(),
	}
	if err := store.PutObject(ctx, obj); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// Second put (upsert) — should overwrite.
	obj.Size = 200
	obj.ETag = `"bbb"`
	if err := store.PutObject(ctx, obj); err != nil {
		t.Fatalf("PutObject (upsert): %v", err)
	}

	got, err := store.GetObject(ctx, "upsert-bucket", "key1")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if got.Size != 200 {
		t.Errorf("Size after upsert = %d, want 200", got.Size)
	}
	if got.ETag != `"bbb"` {
		t.Errorf("ETag after upsert = %q, want %q", got.ETag, `"bbb"`)
	}
}

func TestDeleteObjectIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "del-bucket")

	// Deleting a non-existent object should not error.
	if err := store.DeleteObject(ctx, "del-bucket", "no-such-key"); err != nil {
		t.Fatalf("DeleteObject(non-existent) returned error: %v", err)
	}
}

func TestDeleteObjectsMeta(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "batch-bucket")

	// Create 3 objects.
	for i := 0; i < 3; i++ {
		obj := &ObjectRecord{
			Bucket:       "batch-bucket",
			Key:          fmt.Sprintf("key%d", i),
			Size:         int64(i * 100),
			ETag:         fmt.Sprintf(`"etag%d"`, i),
			ContentType:  "text/plain",
			LastModified: time.Now().UTC(),
		}
		if err := store.PutObject(ctx, obj); err != nil {
			t.Fatalf("PutObject: %v", err)
		}
	}

	// Delete 2 of them plus a non-existent key.
	deleted, errs := store.DeleteObjectsMeta(ctx, "batch-bucket", []string{"key0", "key2", "non-existent"})
	if len(errs) != 0 {
		t.Errorf("DeleteObjectsMeta errs = %v, want empty", errs)
	}
	if len(deleted) != 3 {
		t.Errorf("DeleteObjectsMeta deleted %d keys, want 3", len(deleted))
	}

	// Verify key1 still exists.
	exists, _ := store.ObjectExists(ctx, "batch-bucket", "key1")
	if !exists {
		t.Error("key1 should still exist")
	}

	// Verify key0 and key2 are gone.
	exists, _ = store.ObjectExists(ctx, "batch-bucket", "key0")
	if exists {
		t.Error("key0 should be deleted")
	}
	exists, _ = store.ObjectExists(ctx, "batch-bucket", "key2")
	if exists {
		t.Error("key2 should be deleted")
	}
}

func TestUpdateObjectAcl(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "obj-acl-bucket")

	obj := &ObjectRecord{
		Bucket:       "obj-acl-bucket",
		Key:          "file.txt",
		Size:         10,
		ETag:         `"abc"`,
		ContentType:  "text/plain",
		LastModified: time.Now().UTC(),
	}
	if err := store.PutObject(ctx, obj); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	newACL := json.RawMessage(`{"grants":[{"permission":"READ"}]}`)
	if err := store.UpdateObjectAcl(ctx, "obj-acl-bucket", "file.txt", newACL); err != nil {
		t.Fatalf("UpdateObjectAcl: %v", err)
	}

	got, _ := store.GetObject(ctx, "obj-acl-bucket", "file.txt")
	if string(got.ACL) != string(newACL) {
		t.Errorf("ACL = %s, want %s", string(got.ACL), string(newACL))
	}

	// Non-existent object.
	err := store.UpdateObjectAcl(ctx, "obj-acl-bucket", "no-such-key", newACL)
	if err == nil {
		t.Error("Expected error updating ACL for non-existent object")
	}
}

// ---- ListObjects tests ----

func TestListObjectsBasic(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "list-bucket")

	// Create objects.
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		obj := &ObjectRecord{
			Bucket:       "list-bucket",
			Key:          k,
			Size:         10,
			ETag:         fmt.Sprintf(`"%s"`, k),
			ContentType:  "text/plain",
			LastModified: time.Now().UTC(),
		}
		store.PutObject(ctx, obj)
	}

	// List all.
	result, err := store.ListObjects(ctx, "list-bucket", ListObjectsOptions{MaxKeys: 100})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(result.Objects) != 5 {
		t.Fatalf("ListObjects returned %d objects, want 5", len(result.Objects))
	}
	if result.IsTruncated {
		t.Error("IsTruncated should be false")
	}
}

func TestListObjectsWithPrefix(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "prefix-bucket")

	keys := []string{"photos/2024/jan.jpg", "photos/2024/feb.jpg", "photos/2025/jan.jpg", "docs/readme.md"}
	for _, k := range keys {
		store.PutObject(ctx, &ObjectRecord{
			Bucket: "prefix-bucket", Key: k, Size: 10, ETag: `"x"`,
			ContentType: "text/plain", LastModified: time.Now().UTC(),
		})
	}

	// Prefix = "photos/"
	result, err := store.ListObjects(ctx, "prefix-bucket", ListObjectsOptions{
		Prefix:  "photos/",
		MaxKeys: 100,
	})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(result.Objects) != 3 {
		t.Errorf("ListObjects with prefix returned %d objects, want 3", len(result.Objects))
	}
}

func TestListObjectsWithDelimiter(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "delim-bucket")

	keys := []string{
		"photos/2024/jan.jpg",
		"photos/2024/feb.jpg",
		"photos/2025/jan.jpg",
		"docs/readme.md",
		"root-file.txt",
	}
	for _, k := range keys {
		store.PutObject(ctx, &ObjectRecord{
			Bucket: "delim-bucket", Key: k, Size: 10, ETag: `"x"`,
			ContentType: "text/plain", LastModified: time.Now().UTC(),
		})
	}

	// Delimiter = "/" at root level.
	result, err := store.ListObjects(ctx, "delim-bucket", ListObjectsOptions{
		Delimiter: "/",
		MaxKeys:   100,
	})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	// Should have 1 object (root-file.txt) and 2 common prefixes (docs/, photos/).
	if len(result.Objects) != 1 {
		t.Errorf("Objects count = %d, want 1", len(result.Objects))
	}
	if result.Objects[0].Key != "root-file.txt" {
		t.Errorf("Object key = %q, want %q", result.Objects[0].Key, "root-file.txt")
	}
	if len(result.CommonPrefixes) != 2 {
		t.Errorf("CommonPrefixes count = %d, want 2", len(result.CommonPrefixes))
	} else {
		if result.CommonPrefixes[0] != "docs/" {
			t.Errorf("CommonPrefixes[0] = %q, want %q", result.CommonPrefixes[0], "docs/")
		}
		if result.CommonPrefixes[1] != "photos/" {
			t.Errorf("CommonPrefixes[1] = %q, want %q", result.CommonPrefixes[1], "photos/")
		}
	}

	// Prefix = "photos/" + delimiter = "/".
	result, err = store.ListObjects(ctx, "delim-bucket", ListObjectsOptions{
		Prefix:    "photos/",
		Delimiter: "/",
		MaxKeys:   100,
	})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	// Should have 0 objects and 2 common prefixes (photos/2024/, photos/2025/).
	if len(result.Objects) != 0 {
		t.Errorf("Objects count = %d, want 0", len(result.Objects))
	}
	if len(result.CommonPrefixes) != 2 {
		t.Errorf("CommonPrefixes count = %d, want 2", len(result.CommonPrefixes))
	} else {
		if result.CommonPrefixes[0] != "photos/2024/" {
			t.Errorf("CommonPrefixes[0] = %q, want %q", result.CommonPrefixes[0], "photos/2024/")
		}
		if result.CommonPrefixes[1] != "photos/2025/" {
			t.Errorf("CommonPrefixes[1] = %q, want %q", result.CommonPrefixes[1], "photos/2025/")
		}
	}
}

func TestListObjectsPagination(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "page-bucket")

	// Create 5 objects.
	for i := 0; i < 5; i++ {
		store.PutObject(ctx, &ObjectRecord{
			Bucket: "page-bucket", Key: fmt.Sprintf("key%d", i), Size: 10, ETag: `"x"`,
			ContentType: "text/plain", LastModified: time.Now().UTC(),
		})
	}

	// Page 1: max 2 keys.
	result, err := store.ListObjects(ctx, "page-bucket", ListObjectsOptions{MaxKeys: 2})
	if err != nil {
		t.Fatalf("ListObjects page 1: %v", err)
	}
	if len(result.Objects) != 2 {
		t.Fatalf("Page 1 objects = %d, want 2", len(result.Objects))
	}
	if !result.IsTruncated {
		t.Error("Page 1 IsTruncated should be true")
	}
	if result.NextContinuationToken == "" {
		t.Error("Page 1 NextContinuationToken should not be empty")
	}

	// Page 2: continue from token.
	result2, err := store.ListObjects(ctx, "page-bucket", ListObjectsOptions{
		MaxKeys:           2,
		ContinuationToken: result.NextContinuationToken,
	})
	if err != nil {
		t.Fatalf("ListObjects page 2: %v", err)
	}
	if len(result2.Objects) != 2 {
		t.Fatalf("Page 2 objects = %d, want 2", len(result2.Objects))
	}
	if !result2.IsTruncated {
		t.Error("Page 2 IsTruncated should be true")
	}

	// Page 3: last page.
	result3, err := store.ListObjects(ctx, "page-bucket", ListObjectsOptions{
		MaxKeys:           2,
		ContinuationToken: result2.NextContinuationToken,
	})
	if err != nil {
		t.Fatalf("ListObjects page 3: %v", err)
	}
	if len(result3.Objects) != 1 {
		t.Fatalf("Page 3 objects = %d, want 1", len(result3.Objects))
	}
	if result3.IsTruncated {
		t.Error("Page 3 IsTruncated should be false")
	}
}

func TestListObjectsWithMarker(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "marker-bucket")

	for _, k := range []string{"a", "b", "c", "d"} {
		store.PutObject(ctx, &ObjectRecord{
			Bucket: "marker-bucket", Key: k, Size: 10, ETag: `"x"`,
			ContentType: "text/plain", LastModified: time.Now().UTC(),
		})
	}

	// Marker = "b" should return c, d.
	result, err := store.ListObjects(ctx, "marker-bucket", ListObjectsOptions{
		Marker:  "b",
		MaxKeys: 100,
	})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(result.Objects) != 2 {
		t.Fatalf("Objects = %d, want 2", len(result.Objects))
	}
	if result.Objects[0].Key != "c" {
		t.Errorf("First key = %q, want %q", result.Objects[0].Key, "c")
	}
}

func TestListObjectsEmptyBucket(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "empty-bucket")

	result, err := store.ListObjects(ctx, "empty-bucket", ListObjectsOptions{MaxKeys: 100})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(result.Objects) != 0 {
		t.Errorf("Objects = %d, want 0", len(result.Objects))
	}
	if result.IsTruncated {
		t.Error("IsTruncated should be false for empty bucket")
	}
}

// ---- Multipart upload tests ----

func TestMultipartLifecycle(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "mp-bucket")

	// Create multipart upload.
	upload := &MultipartUploadRecord{
		Bucket:       "mp-bucket",
		Key:          "large-file.bin",
		ContentType:  "application/octet-stream",
		OwnerID:      "test-owner",
		OwnerDisplay: "Test Owner",
		UserMetadata: map[string]string{"x-amz-meta-custom": "value"},
		InitiatedAt:  time.Now().UTC().Truncate(time.Millisecond),
	}
	uploadID, err := store.CreateMultipartUpload(ctx, upload)
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if uploadID == "" {
		t.Fatal("CreateMultipartUpload returned empty uploadID")
	}

	// Get multipart upload.
	got, err := store.GetMultipartUpload(ctx, "mp-bucket", "large-file.bin", uploadID)
	if err != nil {
		t.Fatalf("GetMultipartUpload: %v", err)
	}
	if got == nil {
		t.Fatal("GetMultipartUpload returned nil")
	}
	if got.UploadID != uploadID {
		t.Errorf("UploadID = %q, want %q", got.UploadID, uploadID)
	}
	if got.ContentType != "application/octet-stream" {
		t.Errorf("ContentType = %q", got.ContentType)
	}
	if got.UserMetadata["x-amz-meta-custom"] != "value" {
		t.Errorf("UserMetadata = %v", got.UserMetadata)
	}

	// Non-existent upload.
	got, err = store.GetMultipartUpload(ctx, "mp-bucket", "large-file.bin", "no-such-upload")
	if err != nil {
		t.Fatalf("GetMultipartUpload: %v", err)
	}
	if got != nil {
		t.Errorf("GetMultipartUpload(non-existent) = %v, want nil", got)
	}

	// Upload 3 parts.
	for i := 1; i <= 3; i++ {
		part := &PartRecord{
			UploadID:     uploadID,
			PartNumber:   i,
			Size:         int64(i * 1000),
			ETag:         fmt.Sprintf(`"part%d"`, i),
			LastModified: time.Now().UTC().Truncate(time.Millisecond),
		}
		if err := store.PutPart(ctx, part); err != nil {
			t.Fatalf("PutPart(%d): %v", i, err)
		}
	}

	// List parts.
	partsResult, err := store.ListParts(ctx, uploadID, ListPartsOptions{MaxParts: 100})
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(partsResult.Parts) != 3 {
		t.Fatalf("ListParts returned %d parts, want 3", len(partsResult.Parts))
	}
	if partsResult.Parts[0].PartNumber != 1 {
		t.Errorf("First part number = %d, want 1", partsResult.Parts[0].PartNumber)
	}
	if partsResult.Parts[2].Size != 3000 {
		t.Errorf("Third part size = %d, want 3000", partsResult.Parts[2].Size)
	}

	// GetPartsForCompletion.
	completionParts, err := store.GetPartsForCompletion(ctx, uploadID, []int{1, 2, 3})
	if err != nil {
		t.Fatalf("GetPartsForCompletion: %v", err)
	}
	if len(completionParts) != 3 {
		t.Fatalf("GetPartsForCompletion returned %d parts, want 3", len(completionParts))
	}

	// Complete multipart upload.
	finalObj := &ObjectRecord{
		Bucket:       "mp-bucket",
		Key:          "large-file.bin",
		Size:         6000, // 1000 + 2000 + 3000
		ETag:         `"composite-etag-3"`,
		ContentType:  "application/octet-stream",
		UserMetadata: map[string]string{"x-amz-meta-custom": "value"},
		LastModified: time.Now().UTC(),
	}
	if err := store.CompleteMultipartUpload(ctx, "mp-bucket", "large-file.bin", uploadID, finalObj); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	// Verify the object exists.
	obj, err := store.GetObject(ctx, "mp-bucket", "large-file.bin")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if obj == nil {
		t.Fatal("GetObject returned nil after completion")
	}
	if obj.Size != 6000 {
		t.Errorf("Object size = %d, want 6000", obj.Size)
	}
	if obj.ETag != `"composite-etag-3"` {
		t.Errorf("Object ETag = %q", obj.ETag)
	}

	// Verify upload and parts are cleaned up.
	gotUpload, _ := store.GetMultipartUpload(ctx, "mp-bucket", "large-file.bin", uploadID)
	if gotUpload != nil {
		t.Error("Upload record should be deleted after completion")
	}
	partsResult, _ = store.ListParts(ctx, uploadID, ListPartsOptions{MaxParts: 100})
	if len(partsResult.Parts) != 0 {
		t.Errorf("Parts should be deleted after completion, got %d", len(partsResult.Parts))
	}
}

func TestMultipartAbort(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "abort-bucket")

	upload := &MultipartUploadRecord{
		Bucket:      "abort-bucket",
		Key:         "aborted-file.bin",
		OwnerID:     "test-owner",
		InitiatedAt: time.Now().UTC(),
	}
	uploadID, err := store.CreateMultipartUpload(ctx, upload)
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	// Upload 2 parts.
	for i := 1; i <= 2; i++ {
		store.PutPart(ctx, &PartRecord{
			UploadID: uploadID, PartNumber: i, Size: 100,
			ETag: fmt.Sprintf(`"p%d"`, i), LastModified: time.Now().UTC(),
		})
	}

	// Abort.
	if err := store.AbortMultipartUpload(ctx, "abort-bucket", "aborted-file.bin", uploadID); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}

	// Verify cleaned up.
	gotUpload, _ := store.GetMultipartUpload(ctx, "abort-bucket", "aborted-file.bin", uploadID)
	if gotUpload != nil {
		t.Error("Upload record should be deleted after abort")
	}
	parts, _ := store.ListParts(ctx, uploadID, ListPartsOptions{MaxParts: 100})
	if len(parts.Parts) != 0 {
		t.Errorf("Parts should be deleted after abort, got %d", len(parts.Parts))
	}
}

func TestAbortMultipartUploadNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "abort-nf-bucket")

	err := store.AbortMultipartUpload(ctx, "abort-nf-bucket", "key", "no-such-upload")
	if err == nil {
		t.Error("Expected error aborting non-existent upload")
	}
}

func TestPartOverwrite(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "overwrite-bucket")

	upload := &MultipartUploadRecord{
		Bucket:      "overwrite-bucket",
		Key:         "file.bin",
		OwnerID:     "owner",
		InitiatedAt: time.Now().UTC(),
	}
	uploadID, _ := store.CreateMultipartUpload(ctx, upload)

	// Upload part 1.
	store.PutPart(ctx, &PartRecord{
		UploadID: uploadID, PartNumber: 1, Size: 100,
		ETag: `"first"`, LastModified: time.Now().UTC(),
	})

	// Overwrite part 1.
	store.PutPart(ctx, &PartRecord{
		UploadID: uploadID, PartNumber: 1, Size: 200,
		ETag: `"second"`, LastModified: time.Now().UTC(),
	})

	// Should have only 1 part with the updated values.
	parts, _ := store.ListParts(ctx, uploadID, ListPartsOptions{MaxParts: 100})
	if len(parts.Parts) != 1 {
		t.Fatalf("Parts count = %d, want 1", len(parts.Parts))
	}
	if parts.Parts[0].ETag != `"second"` {
		t.Errorf("Part ETag = %q, want %q", parts.Parts[0].ETag, `"second"`)
	}
	if parts.Parts[0].Size != 200 {
		t.Errorf("Part Size = %d, want 200", parts.Parts[0].Size)
	}
}

func TestListPartsPagination(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "parts-page-bucket")

	upload := &MultipartUploadRecord{
		Bucket:      "parts-page-bucket",
		Key:         "file.bin",
		OwnerID:     "owner",
		InitiatedAt: time.Now().UTC(),
	}
	uploadID, _ := store.CreateMultipartUpload(ctx, upload)

	// Upload 5 parts.
	for i := 1; i <= 5; i++ {
		store.PutPart(ctx, &PartRecord{
			UploadID: uploadID, PartNumber: i, Size: int64(i * 100),
			ETag: fmt.Sprintf(`"p%d"`, i), LastModified: time.Now().UTC(),
		})
	}

	// Page 1: max 2.
	result, _ := store.ListParts(ctx, uploadID, ListPartsOptions{MaxParts: 2})
	if len(result.Parts) != 2 {
		t.Fatalf("Page 1 parts = %d, want 2", len(result.Parts))
	}
	if !result.IsTruncated {
		t.Error("Page 1 IsTruncated should be true")
	}

	// Page 2.
	result2, _ := store.ListParts(ctx, uploadID, ListPartsOptions{
		MaxParts:         2,
		PartNumberMarker: result.NextPartNumberMarker,
	})
	if len(result2.Parts) != 2 {
		t.Fatalf("Page 2 parts = %d, want 2", len(result2.Parts))
	}

	// Page 3.
	result3, _ := store.ListParts(ctx, uploadID, ListPartsOptions{
		MaxParts:         2,
		PartNumberMarker: result2.NextPartNumberMarker,
	})
	if len(result3.Parts) != 1 {
		t.Fatalf("Page 3 parts = %d, want 1", len(result3.Parts))
	}
	if result3.IsTruncated {
		t.Error("Page 3 IsTruncated should be false")
	}
}

func TestListMultipartUploads(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "multi-list-bucket")

	// Create 3 uploads.
	var uploadIDs []string
	for i := 0; i < 3; i++ {
		upload := &MultipartUploadRecord{
			Bucket:      "multi-list-bucket",
			Key:         fmt.Sprintf("file%d.bin", i),
			OwnerID:     "owner",
			InitiatedAt: time.Now().UTC().Add(time.Duration(i) * time.Second),
		}
		id, err := store.CreateMultipartUpload(ctx, upload)
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
		uploadIDs = append(uploadIDs, id)
	}

	// List all uploads.
	result, err := store.ListMultipartUploads(ctx, "multi-list-bucket", ListUploadsOptions{MaxUploads: 100})
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if len(result.Uploads) != 3 {
		t.Fatalf("Uploads count = %d, want 3", len(result.Uploads))
	}

	// List with prefix.
	result, err = store.ListMultipartUploads(ctx, "multi-list-bucket", ListUploadsOptions{
		Prefix:     "file0",
		MaxUploads: 100,
	})
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if len(result.Uploads) != 1 {
		t.Fatalf("Uploads with prefix = %d, want 1", len(result.Uploads))
	}

	// Pagination.
	result, err = store.ListMultipartUploads(ctx, "multi-list-bucket", ListUploadsOptions{MaxUploads: 2})
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if len(result.Uploads) != 2 {
		t.Fatalf("Uploads page 1 = %d, want 2", len(result.Uploads))
	}
	if !result.IsTruncated {
		t.Error("IsTruncated should be true")
	}
}

// ---- Credential tests ----

func TestCredentialCRUD(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	cred := &CredentialRecord{
		AccessKeyID: "AKID123",
		SecretKey:   "secret123",
		OwnerID:     "owner1",
		DisplayName: "Test User",
		Active:      true,
		CreatedAt:   time.Now().UTC().Truncate(time.Millisecond),
	}
	if err := store.PutCredential(ctx, cred); err != nil {
		t.Fatalf("PutCredential: %v", err)
	}

	got, err := store.GetCredential(ctx, "AKID123")
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if got == nil {
		t.Fatal("GetCredential returned nil")
	}
	if got.SecretKey != "secret123" {
		t.Errorf("SecretKey = %q", got.SecretKey)
	}
	if got.OwnerID != "owner1" {
		t.Errorf("OwnerID = %q", got.OwnerID)
	}
	if !got.Active {
		t.Error("Active should be true")
	}

	// Non-existent credential.
	got, err = store.GetCredential(ctx, "NOEXIST")
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if got != nil {
		t.Errorf("GetCredential(non-existent) = %v, want nil", got)
	}

	// Update credential (upsert).
	cred.SecretKey = "new-secret"
	cred.Active = false
	if err := store.PutCredential(ctx, cred); err != nil {
		t.Fatalf("PutCredential (update): %v", err)
	}
	got, _ = store.GetCredential(ctx, "AKID123")
	if got.SecretKey != "new-secret" {
		t.Errorf("SecretKey after update = %q", got.SecretKey)
	}
	if got.Active {
		t.Error("Active should be false after update")
	}
}

// ---- Schema idempotency test ----

func TestIdempotentSchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "idempotent.db")

	// Create store (runs initDB).
	store1, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("First NewSQLiteStore: %v", err)
	}
	store1.Close()

	// Create another store on same DB (runs initDB again).
	store2, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("Second NewSQLiteStore: %v", err)
	}
	defer store2.Close()

	// Should work fine.
	ctx := context.Background()
	bucket := &BucketRecord{
		Name:      "test-bucket",
		Region:    "us-east-1",
		OwnerID:   "owner",
		CreatedAt: time.Now().UTC(),
	}
	if err := store2.CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("CreateBucket after idempotent schema: %v", err)
	}
}

// ---- Object with default fields test ----

func TestObjectDefaultFields(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	seedBucket(t, store, "defaults-bucket")

	// Put object with minimal fields.
	obj := &ObjectRecord{
		Bucket:       "defaults-bucket",
		Key:          "minimal.txt",
		Size:         0,
		ETag:         `"empty"`,
		LastModified: time.Now().UTC(),
	}
	if err := store.PutObject(ctx, obj); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	got, _ := store.GetObject(ctx, "defaults-bucket", "minimal.txt")
	if got.ContentType != "application/octet-stream" {
		t.Errorf("Default ContentType = %q, want application/octet-stream", got.ContentType)
	}
	if got.StorageClass != "STANDARD" {
		t.Errorf("Default StorageClass = %q, want STANDARD", got.StorageClass)
	}
	if got.DeleteMarker {
		t.Error("DeleteMarker should be false by default")
	}
}
