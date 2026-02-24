package handlers

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bleepstore/bleepstore/internal/metadata"
	"github.com/bleepstore/bleepstore/internal/storage"
	"github.com/bleepstore/bleepstore/internal/xmlutil"
)

// newTestMultipartHandler creates a MultipartHandler and ObjectHandler backed
// by real in-memory SQLite and local filesystem storage (temp dirs).
// Returns both handlers and the metadata store (for direct DB assertions).
func newTestMultipartHandler(t *testing.T) (*MultipartHandler, *ObjectHandler, metadata.MetadataStore, storage.StorageBackend) {
	t.Helper()

	dbPath := t.TempDir() + "/test.db"
	meta, err := metadata.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore failed: %v", err)
	}
	t.Cleanup(func() { meta.Close() })

	storageDir := t.TempDir()
	store, err := storage.NewLocalBackend(storageDir)
	if err != nil {
		t.Fatalf("NewLocalBackend failed: %v", err)
	}

	mh := NewMultipartHandler(meta, store, "bleepstore", "bleepstore", 5368709120)
	oh := NewObjectHandler(meta, store, "bleepstore", "bleepstore", 5368709120)

	return mh, oh, meta, store
}

// createTestBucketForMultipart creates a test bucket in the metadata store and
// storage backend.
func createTestBucketForMultipart(t *testing.T, meta metadata.MetadataStore, store storage.StorageBackend, bucketName string) {
	t.Helper()
	err := meta.CreateBucket(context.Background(), &metadata.BucketRecord{
		Name:         bucketName,
		Region:       "us-east-1",
		OwnerID:      "bleepstore",
		OwnerDisplay: "bleepstore",
		CreatedAt:    time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if err := store.CreateBucket(context.Background(), bucketName); err != nil {
		t.Fatalf("CreateBucket storage failed: %v", err)
	}
}

func TestCreateMultipartUpload(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	req := httptest.NewRequest("POST", "/"+bucketName+"/test-key?uploads", nil)
	rec := httptest.NewRecorder()
	mh.CreateMultipartUpload(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("CreateMultipartUpload status = %d, want %d", rec.Code, http.StatusOK)
	}

	var result xmlutil.InitiateMultipartUploadResult
	if err := xml.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("Decode XML: %v", err)
	}

	if result.Bucket != bucketName {
		t.Errorf("Bucket = %q, want %q", result.Bucket, bucketName)
	}
	if result.Key != "test-key" {
		t.Errorf("Key = %q, want %q", result.Key, "test-key")
	}
	if result.UploadID == "" {
		t.Error("UploadID is empty")
	}
	if len(result.UploadID) != 32 {
		t.Errorf("UploadID length = %d, want 32", len(result.UploadID))
	}
}

func TestCreateMultipartUploadNoSuchBucket(t *testing.T) {
	mh, _, _, _ := newTestMultipartHandler(t)

	req := httptest.NewRequest("POST", "/nonexistent/test-key?uploads", nil)
	rec := httptest.NewRecorder()
	mh.CreateMultipartUpload(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "NoSuchBucket") {
		t.Errorf("expected NoSuchBucket error, got: %s", body)
	}
}

func TestUploadPart(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	// Create upload.
	req := httptest.NewRequest("POST", "/"+bucketName+"/test-key?uploads", nil)
	rec := httptest.NewRecorder()
	mh.CreateMultipartUpload(rec, req)

	var initResult xmlutil.InitiateMultipartUploadResult
	xml.NewDecoder(rec.Body).Decode(&initResult)
	uploadID := initResult.UploadID

	// Upload a part.
	partData := []byte("hello world part 1")
	expectedMD5 := fmt.Sprintf(`"%x"`, md5.Sum(partData))

	req = httptest.NewRequest("PUT",
		fmt.Sprintf("/%s/test-key?partNumber=1&uploadId=%s", bucketName, uploadID),
		bytes.NewReader(partData))
	req.ContentLength = int64(len(partData))
	rec = httptest.NewRecorder()
	mh.UploadPart(rec, req)

	if rec.Code != http.StatusOK {
		body, _ := io.ReadAll(rec.Body)
		t.Fatalf("UploadPart status = %d, want %d, body: %s", rec.Code, http.StatusOK, body)
	}

	etag := rec.Header().Get("ETag")
	if etag != expectedMD5 {
		t.Errorf("ETag = %q, want %q", etag, expectedMD5)
	}
}

func TestUploadPartInvalidPartNumber(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	// Create upload.
	req := httptest.NewRequest("POST", "/"+bucketName+"/test-key?uploads", nil)
	rec := httptest.NewRecorder()
	mh.CreateMultipartUpload(rec, req)

	var initResult xmlutil.InitiateMultipartUploadResult
	xml.NewDecoder(rec.Body).Decode(&initResult)
	uploadID := initResult.UploadID

	tests := []struct {
		partNumber string
	}{
		{"0"},
		{"-1"},
		{"10001"},
		{"abc"},
		{""},
	}

	for _, tt := range tests {
		t.Run("partNumber="+tt.partNumber, func(t *testing.T) {
			req := httptest.NewRequest("PUT",
				fmt.Sprintf("/%s/test-key?partNumber=%s&uploadId=%s", bucketName, tt.partNumber, uploadID),
				bytes.NewReader([]byte("data")))
			rec := httptest.NewRecorder()
			mh.UploadPart(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestUploadPartNoSuchUpload(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	req := httptest.NewRequest("PUT",
		fmt.Sprintf("/%s/test-key?partNumber=1&uploadId=nonexistent", bucketName),
		bytes.NewReader([]byte("data")))
	rec := httptest.NewRecorder()
	mh.UploadPart(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "NoSuchUpload") {
		t.Errorf("expected NoSuchUpload error, got: %s", body)
	}
}

func TestUploadPartOverwrite(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	// Create upload.
	req := httptest.NewRequest("POST", "/"+bucketName+"/test-key?uploads", nil)
	rec := httptest.NewRecorder()
	mh.CreateMultipartUpload(rec, req)

	var initResult xmlutil.InitiateMultipartUploadResult
	xml.NewDecoder(rec.Body).Decode(&initResult)
	uploadID := initResult.UploadID

	// Upload part 1 (version 1).
	data1 := []byte("version 1 data")
	req = httptest.NewRequest("PUT",
		fmt.Sprintf("/%s/test-key?partNumber=1&uploadId=%s", bucketName, uploadID),
		bytes.NewReader(data1))
	req.ContentLength = int64(len(data1))
	rec = httptest.NewRecorder()
	mh.UploadPart(rec, req)
	etag1 := rec.Header().Get("ETag")

	// Upload part 1 (version 2).
	data2 := []byte("version 2 data different")
	req = httptest.NewRequest("PUT",
		fmt.Sprintf("/%s/test-key?partNumber=1&uploadId=%s", bucketName, uploadID),
		bytes.NewReader(data2))
	req.ContentLength = int64(len(data2))
	rec = httptest.NewRecorder()
	mh.UploadPart(rec, req)
	etag2 := rec.Header().Get("ETag")

	if etag1 == etag2 {
		t.Error("ETags should differ for different data")
	}

	// Verify part 1 was overwritten in metadata.
	parts, err := meta.ListParts(context.Background(), uploadID, metadata.ListPartsOptions{})
	if err != nil {
		t.Fatalf("ListParts error: %v", err)
	}
	if len(parts.Parts) != 1 {
		t.Fatalf("Parts count = %d, want 1", len(parts.Parts))
	}
	if parts.Parts[0].ETag != etag2 {
		t.Errorf("Part ETag = %q, want %q (overwritten)", parts.Parts[0].ETag, etag2)
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	// Create upload.
	req := httptest.NewRequest("POST", "/"+bucketName+"/test-key?uploads", nil)
	rec := httptest.NewRecorder()
	mh.CreateMultipartUpload(rec, req)

	var initResult xmlutil.InitiateMultipartUploadResult
	xml.NewDecoder(rec.Body).Decode(&initResult)
	uploadID := initResult.UploadID

	// Upload a part.
	req = httptest.NewRequest("PUT",
		fmt.Sprintf("/%s/test-key?partNumber=1&uploadId=%s", bucketName, uploadID),
		bytes.NewReader([]byte("part data")))
	req.ContentLength = 9
	rec = httptest.NewRecorder()
	mh.UploadPart(rec, req)

	// Abort.
	req = httptest.NewRequest("DELETE",
		fmt.Sprintf("/%s/test-key?uploadId=%s", bucketName, uploadID),
		nil)
	rec = httptest.NewRecorder()
	mh.AbortMultipartUpload(rec, req)

	if rec.Code != http.StatusNoContent {
		body, _ := io.ReadAll(rec.Body)
		t.Fatalf("AbortMultipartUpload status = %d, want %d, body: %s", rec.Code, http.StatusNoContent, body)
	}

	// Verify upload is gone from metadata.
	upload, err := meta.GetMultipartUpload(context.Background(), bucketName, "test-key", uploadID)
	if err != nil {
		t.Fatalf("GetMultipartUpload error: %v", err)
	}
	if upload != nil {
		t.Error("Upload should be deleted after abort")
	}
}

func TestAbortMultipartUploadNoSuchUpload(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	req := httptest.NewRequest("DELETE",
		fmt.Sprintf("/%s/test-key?uploadId=nonexistent", bucketName),
		nil)
	rec := httptest.NewRecorder()
	mh.AbortMultipartUpload(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "NoSuchUpload") {
		t.Errorf("expected NoSuchUpload error, got: %s", body)
	}
}

func TestListMultipartUploads(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	// Create two uploads.
	var uploadIDs []string
	keys := []string{"upload1.bin", "upload2.bin"}
	for _, key := range keys {
		req := httptest.NewRequest("POST", "/"+bucketName+"/"+key+"?uploads", nil)
		rec := httptest.NewRecorder()
		mh.CreateMultipartUpload(rec, req)

		var result xmlutil.InitiateMultipartUploadResult
		xml.NewDecoder(rec.Body).Decode(&result)
		uploadIDs = append(uploadIDs, result.UploadID)
	}

	// List uploads.
	req := httptest.NewRequest("GET", "/"+bucketName+"?uploads", nil)
	rec := httptest.NewRecorder()
	mh.ListMultipartUploads(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ListMultipartUploads status = %d, want %d", rec.Code, http.StatusOK)
	}

	var result xmlutil.ListMultipartUploadsResult
	if err := xml.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("Decode XML: %v", err)
	}

	if result.Bucket != bucketName {
		t.Errorf("Bucket = %q, want %q", result.Bucket, bucketName)
	}
	if len(result.Uploads) != 2 {
		t.Fatalf("Uploads count = %d, want 2", len(result.Uploads))
	}

	// Check that both upload IDs are present.
	foundIDs := make(map[string]bool)
	for _, u := range result.Uploads {
		foundIDs[u.UploadID] = true
	}
	for _, id := range uploadIDs {
		if !foundIDs[id] {
			t.Errorf("Upload ID %q not found in list", id)
		}
	}
}

func TestListMultipartUploadsWithPrefix(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	// Create uploads with different key prefixes.
	for _, key := range []string{"data/file1.bin", "logs/file2.bin"} {
		req := httptest.NewRequest("POST", "/"+bucketName+"/"+key+"?uploads", nil)
		rec := httptest.NewRecorder()
		mh.CreateMultipartUpload(rec, req)
	}

	// List with prefix.
	req := httptest.NewRequest("GET", "/"+bucketName+"?uploads&prefix=data/", nil)
	rec := httptest.NewRecorder()
	mh.ListMultipartUploads(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var result xmlutil.ListMultipartUploadsResult
	xml.NewDecoder(rec.Body).Decode(&result)

	if len(result.Uploads) != 1 {
		t.Fatalf("Uploads count = %d, want 1", len(result.Uploads))
	}
	if result.Uploads[0].Key != "data/file1.bin" {
		t.Errorf("Upload key = %q, want %q", result.Uploads[0].Key, "data/file1.bin")
	}
}

func TestListMultipartUploadsNoSuchBucket(t *testing.T) {
	mh, _, _, _ := newTestMultipartHandler(t)

	req := httptest.NewRequest("GET", "/nonexistent?uploads", nil)
	rec := httptest.NewRecorder()
	mh.ListMultipartUploads(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "NoSuchBucket") {
		t.Errorf("expected NoSuchBucket error, got: %s", body)
	}
}

func TestListParts(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	// Create upload.
	req := httptest.NewRequest("POST", "/"+bucketName+"/test-key?uploads", nil)
	rec := httptest.NewRecorder()
	mh.CreateMultipartUpload(rec, req)

	var initResult xmlutil.InitiateMultipartUploadResult
	xml.NewDecoder(rec.Body).Decode(&initResult)
	uploadID := initResult.UploadID

	// Upload 3 parts.
	for i := 1; i <= 3; i++ {
		data := bytes.Repeat([]byte{byte('A' + i)}, 100)
		req = httptest.NewRequest("PUT",
			fmt.Sprintf("/%s/test-key?partNumber=%d&uploadId=%s", bucketName, i, uploadID),
			bytes.NewReader(data))
		req.ContentLength = int64(len(data))
		rec = httptest.NewRecorder()
		mh.UploadPart(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("UploadPart %d failed: %d", i, rec.Code)
		}
	}

	// List parts.
	req = httptest.NewRequest("GET",
		fmt.Sprintf("/%s/test-key?uploadId=%s", bucketName, uploadID),
		nil)
	rec = httptest.NewRecorder()
	mh.ListParts(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ListParts status = %d, want %d", rec.Code, http.StatusOK)
	}

	var result xmlutil.ListPartsResult
	if err := xml.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("Decode XML: %v", err)
	}

	if result.Bucket != bucketName {
		t.Errorf("Bucket = %q, want %q", result.Bucket, bucketName)
	}
	if result.Key != "test-key" {
		t.Errorf("Key = %q, want %q", result.Key, "test-key")
	}
	if result.UploadID != uploadID {
		t.Errorf("UploadID = %q, want %q", result.UploadID, uploadID)
	}
	if len(result.Parts) != 3 {
		t.Fatalf("Parts count = %d, want 3", len(result.Parts))
	}

	for i, p := range result.Parts {
		if p.PartNumber != i+1 {
			t.Errorf("Part[%d].PartNumber = %d, want %d", i, p.PartNumber, i+1)
		}
		if p.ETag == "" {
			t.Errorf("Part[%d].ETag is empty", i)
		}
		if p.LastModified == "" {
			t.Errorf("Part[%d].LastModified is empty", i)
		}
	}
}

func TestListPartsNoSuchUpload(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	req := httptest.NewRequest("GET",
		fmt.Sprintf("/%s/test-key?uploadId=nonexistent", bucketName),
		nil)
	rec := httptest.NewRecorder()
	mh.ListParts(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "NoSuchUpload") {
		t.Errorf("expected NoSuchUpload error, got: %s", body)
	}
}

func TestMultipartLifecycleCreateUploadAbort(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	// Create upload.
	req := httptest.NewRequest("POST", "/"+bucketName+"/lifecycle-key?uploads", nil)
	rec := httptest.NewRecorder()
	mh.CreateMultipartUpload(rec, req)

	var initResult xmlutil.InitiateMultipartUploadResult
	xml.NewDecoder(rec.Body).Decode(&initResult)
	uploadID := initResult.UploadID

	// Upload 3 parts.
	for i := 1; i <= 3; i++ {
		data := bytes.Repeat([]byte{byte('0' + i)}, 100)
		req = httptest.NewRequest("PUT",
			fmt.Sprintf("/%s/lifecycle-key?partNumber=%d&uploadId=%s", bucketName, i, uploadID),
			bytes.NewReader(data))
		req.ContentLength = int64(len(data))
		rec = httptest.NewRecorder()
		mh.UploadPart(rec, req)
	}

	// Verify parts are listed.
	req = httptest.NewRequest("GET",
		fmt.Sprintf("/%s/lifecycle-key?uploadId=%s", bucketName, uploadID),
		nil)
	rec = httptest.NewRecorder()
	mh.ListParts(rec, req)

	var partsResult xmlutil.ListPartsResult
	xml.NewDecoder(rec.Body).Decode(&partsResult)
	if len(partsResult.Parts) != 3 {
		t.Fatalf("Parts before abort = %d, want 3", len(partsResult.Parts))
	}

	// Verify upload appears in list.
	req = httptest.NewRequest("GET", "/"+bucketName+"?uploads", nil)
	rec = httptest.NewRecorder()
	mh.ListMultipartUploads(rec, req)

	var uploadsResult xmlutil.ListMultipartUploadsResult
	xml.NewDecoder(rec.Body).Decode(&uploadsResult)
	found := false
	for _, u := range uploadsResult.Uploads {
		if u.UploadID == uploadID {
			found = true
			break
		}
	}
	if !found {
		t.Error("Upload should be in list before abort")
	}

	// Abort.
	req = httptest.NewRequest("DELETE",
		fmt.Sprintf("/%s/lifecycle-key?uploadId=%s", bucketName, uploadID),
		nil)
	rec = httptest.NewRecorder()
	mh.AbortMultipartUpload(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("Abort status = %d, want %d", rec.Code, http.StatusNoContent)
	}

	// Verify upload is gone from metadata.
	upload, err := meta.GetMultipartUpload(context.Background(), bucketName, "lifecycle-key", uploadID)
	if err != nil {
		t.Fatalf("GetMultipartUpload error: %v", err)
	}
	if upload != nil {
		t.Error("Upload should be gone after abort")
	}

	// Verify parts are cleaned up.
	parts, err := meta.ListParts(context.Background(), uploadID, metadata.ListPartsOptions{})
	if err != nil {
		t.Fatalf("ListParts error: %v", err)
	}
	if len(parts.Parts) != 0 {
		t.Errorf("Parts after abort = %d, want 0", len(parts.Parts))
	}
}

func TestUploadPartETag(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	// Create upload.
	req := httptest.NewRequest("POST", "/"+bucketName+"/etag-key?uploads", nil)
	rec := httptest.NewRecorder()
	mh.CreateMultipartUpload(rec, req)

	var initResult xmlutil.InitiateMultipartUploadResult
	xml.NewDecoder(rec.Body).Decode(&initResult)
	uploadID := initResult.UploadID

	// Upload part with known data.
	data := bytes.Repeat([]byte("X"), 1024)
	h := md5.New()
	h.Write(data)
	expectedETag := fmt.Sprintf(`"%x"`, h.Sum(nil))

	req = httptest.NewRequest("PUT",
		fmt.Sprintf("/%s/etag-key?partNumber=1&uploadId=%s", bucketName, uploadID),
		bytes.NewReader(data))
	req.ContentLength = int64(len(data))
	rec = httptest.NewRecorder()
	mh.UploadPart(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPart status = %d, want %d", rec.Code, http.StatusOK)
	}

	etag := rec.Header().Get("ETag")
	if etag != expectedETag {
		t.Errorf("ETag = %q, want %q", etag, expectedETag)
	}
}

func TestCreateMultipartUploadWithContentType(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	req := httptest.NewRequest("POST", "/"+bucketName+"/typed-key?uploads", nil)
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	mh.CreateMultipartUpload(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var result xmlutil.InitiateMultipartUploadResult
	xml.NewDecoder(rec.Body).Decode(&result)

	// Verify the content type is stored in metadata.
	upload, err := meta.GetMultipartUpload(context.Background(), bucketName, "typed-key", result.UploadID)
	if err != nil {
		t.Fatalf("GetMultipartUpload error: %v", err)
	}
	if upload.ContentType != "text/plain" {
		t.Errorf("ContentType = %q, want %q", upload.ContentType, "text/plain")
	}
}

func TestListPartsXMLStructure(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	// Create upload and upload a part.
	req := httptest.NewRequest("POST", "/"+bucketName+"/xml-key?uploads", nil)
	rec := httptest.NewRecorder()
	mh.CreateMultipartUpload(rec, req)

	var initResult xmlutil.InitiateMultipartUploadResult
	xml.NewDecoder(rec.Body).Decode(&initResult)
	uploadID := initResult.UploadID

	data := []byte("xml structure test data")
	req = httptest.NewRequest("PUT",
		fmt.Sprintf("/%s/xml-key?partNumber=1&uploadId=%s", bucketName, uploadID),
		bytes.NewReader(data))
	req.ContentLength = int64(len(data))
	rec = httptest.NewRecorder()
	mh.UploadPart(rec, req)

	// List parts and verify XML structure.
	req = httptest.NewRequest("GET",
		fmt.Sprintf("/%s/xml-key?uploadId=%s", bucketName, uploadID),
		nil)
	rec = httptest.NewRecorder()
	mh.ListParts(rec, req)

	body := rec.Body.String()

	// Check xmlns.
	if !strings.Contains(body, "http://s3.amazonaws.com/doc/2006-03-01/") {
		t.Error("ListParts response missing S3 xmlns")
	}

	// Check XML declaration.
	if !strings.HasPrefix(body, "<?xml") {
		t.Error("ListParts response missing XML declaration")
	}

	// Check required elements.
	for _, element := range []string{"<Bucket>", "<Key>", "<UploadId>", "<Part>", "<PartNumber>", "<ETag>", "<Size>", "<LastModified>"} {
		if !strings.Contains(body, element) {
			t.Errorf("ListParts response missing %s element", element)
		}
	}
}

// --- Stage 8: CompleteMultipartUpload tests ---

// completeMultipartUploadXML builds the XML body for CompleteMultipartUpload.
func completeMultipartUploadXML(parts []CompletePart) string {
	var b strings.Builder
	b.WriteString("<CompleteMultipartUpload>")
	for _, p := range parts {
		b.WriteString(fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>",
			p.PartNumber, p.ETag))
	}
	b.WriteString("</CompleteMultipartUpload>")
	return b.String()
}

// uploadTestParts creates an upload, uploads N parts of the given sizes, and
// returns the upload ID and the ETags of each part in order.
func uploadTestParts(t *testing.T, mh *MultipartHandler, meta metadata.MetadataStore, bucketName, key string, partSizes []int) (string, []string) {
	t.Helper()

	// Create upload.
	req := httptest.NewRequest("POST", "/"+bucketName+"/"+key+"?uploads", nil)
	rec := httptest.NewRecorder()
	mh.CreateMultipartUpload(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("CreateMultipartUpload status = %d, want %d", rec.Code, http.StatusOK)
	}

	var initResult xmlutil.InitiateMultipartUploadResult
	xml.NewDecoder(rec.Body).Decode(&initResult)
	uploadID := initResult.UploadID

	var etags []string
	for i, size := range partSizes {
		partNum := i + 1
		data := bytes.Repeat([]byte{byte('A' + i%26)}, size)
		req = httptest.NewRequest("PUT",
			fmt.Sprintf("/%s/%s?partNumber=%d&uploadId=%s", bucketName, key, partNum, uploadID),
			bytes.NewReader(data))
		req.ContentLength = int64(len(data))
		rec = httptest.NewRecorder()
		mh.UploadPart(rec, req)
		if rec.Code != http.StatusOK {
			body, _ := io.ReadAll(rec.Body)
			t.Fatalf("UploadPart %d status = %d, want %d, body: %s", partNum, rec.Code, http.StatusOK, body)
		}
		etags = append(etags, rec.Header().Get("ETag"))
	}

	return uploadID, etags
}

func TestCompleteMultipartUpload(t *testing.T) {
	mh, oh, meta, store := newTestMultipartHandler(t)
	_ = oh
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	// Use 3 parts where parts 1 and 2 are >= 5 MiB (to satisfy part size
	// validation) and part 3 (last) can be any size.
	const minPartSize = 5 * 1024 * 1024 // 5 MiB
	partData1 := bytes.Repeat([]byte("A"), minPartSize)
	partData2 := bytes.Repeat([]byte("B"), minPartSize)
	partData3 := []byte("part 3 last part data")

	// Create upload.
	req := httptest.NewRequest("POST", "/"+bucketName+"/complete-key?uploads", nil)
	rec := httptest.NewRecorder()
	mh.CreateMultipartUpload(rec, req)
	var initResult xmlutil.InitiateMultipartUploadResult
	xml.NewDecoder(rec.Body).Decode(&initResult)
	uploadID := initResult.UploadID

	// Upload 3 parts and collect ETags.
	var etags []string
	for i, data := range [][]byte{partData1, partData2, partData3} {
		partNum := i + 1
		req = httptest.NewRequest("PUT",
			fmt.Sprintf("/%s/complete-key?partNumber=%d&uploadId=%s", bucketName, partNum, uploadID),
			bytes.NewReader(data))
		req.ContentLength = int64(len(data))
		rec = httptest.NewRecorder()
		mh.UploadPart(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("UploadPart %d failed: %d", partNum, rec.Code)
		}
		etags = append(etags, rec.Header().Get("ETag"))
	}

	// Complete the upload.
	completeParts := []CompletePart{
		{PartNumber: 1, ETag: etags[0]},
		{PartNumber: 2, ETag: etags[1]},
		{PartNumber: 3, ETag: etags[2]},
	}
	xmlBody := completeMultipartUploadXML(completeParts)
	req = httptest.NewRequest("POST",
		fmt.Sprintf("/%s/complete-key?uploadId=%s", bucketName, uploadID),
		strings.NewReader(xmlBody))
	rec = httptest.NewRecorder()
	mh.CompleteMultipartUpload(rec, req)

	if rec.Code != http.StatusOK {
		body, _ := io.ReadAll(rec.Body)
		t.Fatalf("CompleteMultipartUpload status = %d, want %d, body: %s", rec.Code, http.StatusOK, body)
	}

	var result xmlutil.CompleteMultipartUploadResult
	if err := xml.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("Decode XML: %v", err)
	}

	if result.Bucket != bucketName {
		t.Errorf("Bucket = %q, want %q", result.Bucket, bucketName)
	}
	if result.Key != "complete-key" {
		t.Errorf("Key = %q, want %q", result.Key, "complete-key")
	}
	if result.ETag == "" {
		t.Error("ETag is empty")
	}
	// Composite ETag format: "hex-3"
	if !strings.HasSuffix(strings.Trim(result.ETag, `"`), "-3") {
		t.Errorf("ETag = %q, expected composite with -3 suffix", result.ETag)
	}
	if result.Location == "" {
		t.Error("Location is empty")
	}

	// Verify object exists in metadata.
	obj, err := meta.GetObject(context.Background(), bucketName, "complete-key")
	if err != nil {
		t.Fatalf("GetObject error: %v", err)
	}
	if obj == nil {
		t.Fatal("Object should exist after completion")
	}
	if obj.ETag != result.ETag {
		t.Errorf("Stored ETag = %q, want %q", obj.ETag, result.ETag)
	}
	// Verify total size.
	expectedSize := int64(len(partData1) + len(partData2) + len(partData3))
	if obj.Size != expectedSize {
		t.Errorf("Stored Size = %d, want %d", obj.Size, expectedSize)
	}

	// Verify the upload is cleaned up from metadata.
	upload, err := meta.GetMultipartUpload(context.Background(), bucketName, "complete-key", uploadID)
	if err != nil {
		t.Fatalf("GetMultipartUpload error: %v", err)
	}
	if upload != nil {
		t.Error("Upload should be deleted after completion")
	}

	// Verify assembled object content by reading from storage.
	reader, _, _, err := store.GetObject(context.Background(), bucketName, "complete-key")
	if err != nil {
		t.Fatalf("GetObject from storage error: %v", err)
	}
	defer reader.Close()
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	expectedContent := make([]byte, 0, len(partData1)+len(partData2)+len(partData3))
	expectedContent = append(expectedContent, partData1...)
	expectedContent = append(expectedContent, partData2...)
	expectedContent = append(expectedContent, partData3...)
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("Assembled content length = %d, want %d", len(content), len(expectedContent))
	}
}

func TestCompleteMultipartUploadInvalidPartOrder(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	uploadID, etags := uploadTestParts(t, mh, meta, bucketName, "order-key", []int{100, 100})

	// Send parts in descending order.
	completeParts := []CompletePart{
		{PartNumber: 2, ETag: etags[1]},
		{PartNumber: 1, ETag: etags[0]},
	}
	xmlBody := completeMultipartUploadXML(completeParts)
	req := httptest.NewRequest("POST",
		fmt.Sprintf("/%s/order-key?uploadId=%s", bucketName, uploadID),
		strings.NewReader(xmlBody))
	rec := httptest.NewRecorder()
	mh.CompleteMultipartUpload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "InvalidPartOrder") {
		t.Errorf("expected InvalidPartOrder error, got: %s", body)
	}
}

func TestCompleteMultipartUploadDuplicatePartNumber(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	uploadID, etags := uploadTestParts(t, mh, meta, bucketName, "dup-key", []int{100, 100})

	// Send same part number twice.
	completeParts := []CompletePart{
		{PartNumber: 1, ETag: etags[0]},
		{PartNumber: 1, ETag: etags[0]},
	}
	xmlBody := completeMultipartUploadXML(completeParts)
	req := httptest.NewRequest("POST",
		fmt.Sprintf("/%s/dup-key?uploadId=%s", bucketName, uploadID),
		strings.NewReader(xmlBody))
	rec := httptest.NewRecorder()
	mh.CompleteMultipartUpload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "InvalidPartOrder") {
		t.Errorf("expected InvalidPartOrder error, got: %s", body)
	}
}

func TestCompleteMultipartUploadWrongETag(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	uploadID, _ := uploadTestParts(t, mh, meta, bucketName, "etag-key", []int{100})

	// Use a wrong ETag.
	completeParts := []CompletePart{
		{PartNumber: 1, ETag: `"0000000000000000000000000000dead"`},
	}
	xmlBody := completeMultipartUploadXML(completeParts)
	req := httptest.NewRequest("POST",
		fmt.Sprintf("/%s/etag-key?uploadId=%s", bucketName, uploadID),
		strings.NewReader(xmlBody))
	rec := httptest.NewRecorder()
	mh.CompleteMultipartUpload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "InvalidPart") {
		t.Errorf("expected InvalidPart error, got: %s", body)
	}
}

func TestCompleteMultipartUploadMissingPart(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	// Upload 1 part but reference a non-existent part number (only).
	uploadID, _ := uploadTestParts(t, mh, meta, bucketName, "missing-key", []int{100})

	// Reference only a part that doesn't exist (single part = last, so no size check).
	completeParts := []CompletePart{
		{PartNumber: 99, ETag: `"deadbeef00000000000000000000dead"`},
	}
	xmlBody := completeMultipartUploadXML(completeParts)
	req := httptest.NewRequest("POST",
		fmt.Sprintf("/%s/missing-key?uploadId=%s", bucketName, uploadID),
		strings.NewReader(xmlBody))
	rec := httptest.NewRecorder()
	mh.CompleteMultipartUpload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "InvalidPart") {
		t.Errorf("expected InvalidPart error, got: %s", body)
	}
}

func TestCompleteMultipartUploadNoSuchUpload(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	completeParts := []CompletePart{
		{PartNumber: 1, ETag: `"abcd1234"`},
	}
	xmlBody := completeMultipartUploadXML(completeParts)
	req := httptest.NewRequest("POST",
		fmt.Sprintf("/%s/nosuch-key?uploadId=nonexistent", bucketName),
		strings.NewReader(xmlBody))
	rec := httptest.NewRecorder()
	mh.CompleteMultipartUpload(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "NoSuchUpload") {
		t.Errorf("expected NoSuchUpload error, got: %s", body)
	}
}

func TestCompleteMultipartUploadEmptyBody(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	uploadID, _ := uploadTestParts(t, mh, meta, bucketName, "empty-key", []int{100})

	// Send empty XML body.
	req := httptest.NewRequest("POST",
		fmt.Sprintf("/%s/empty-key?uploadId=%s", bucketName, uploadID),
		strings.NewReader(""))
	rec := httptest.NewRecorder()
	mh.CompleteMultipartUpload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "MalformedXML") {
		t.Errorf("expected MalformedXML error, got: %s", body)
	}
}

func TestCompleteMultipartUploadEntityTooSmall(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	// Upload 2 parts where the first is smaller than 5 MiB.
	// Part sizes are recorded from Content-Length. Part 1 = 100 bytes (too small
	// for non-last), Part 2 = 100 bytes (ok as last).
	uploadID, etags := uploadTestParts(t, mh, meta, bucketName, "small-key", []int{100, 100})

	completeParts := []CompletePart{
		{PartNumber: 1, ETag: etags[0]},
		{PartNumber: 2, ETag: etags[1]},
	}
	xmlBody := completeMultipartUploadXML(completeParts)
	req := httptest.NewRequest("POST",
		fmt.Sprintf("/%s/small-key?uploadId=%s", bucketName, uploadID),
		strings.NewReader(xmlBody))
	rec := httptest.NewRecorder()
	mh.CompleteMultipartUpload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "EntityTooSmall") {
		t.Errorf("expected EntityTooSmall error, got: %s", body)
	}
}

func TestCompleteMultipartUploadSinglePart(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	// Single part upload should work regardless of size (it's the last part).
	uploadID, etags := uploadTestParts(t, mh, meta, bucketName, "single-key", []int{50})

	completeParts := []CompletePart{
		{PartNumber: 1, ETag: etags[0]},
	}
	xmlBody := completeMultipartUploadXML(completeParts)
	req := httptest.NewRequest("POST",
		fmt.Sprintf("/%s/single-key?uploadId=%s", bucketName, uploadID),
		strings.NewReader(xmlBody))
	rec := httptest.NewRecorder()
	mh.CompleteMultipartUpload(rec, req)

	if rec.Code != http.StatusOK {
		body, _ := io.ReadAll(rec.Body)
		t.Fatalf("CompleteMultipartUpload status = %d, want %d, body: %s", rec.Code, http.StatusOK, body)
	}

	var result xmlutil.CompleteMultipartUploadResult
	xml.NewDecoder(rec.Body).Decode(&result)

	// Composite ETag for single part: "hex-1"
	if !strings.HasSuffix(strings.Trim(result.ETag, `"`), "-1") {
		t.Errorf("ETag = %q, expected composite with -1 suffix", result.ETag)
	}
}

func TestCompleteMultipartUploadCompositeETag(t *testing.T) {
	// Verify composite ETag computation matches expected formula.
	part1Data := bytes.Repeat([]byte("X"), 256)
	part2Data := bytes.Repeat([]byte("Y"), 256)

	part1MD5 := md5.Sum(part1Data)
	part2MD5 := md5.Sum(part2Data)

	etag1 := fmt.Sprintf(`"%x"`, part1MD5)
	etag2 := fmt.Sprintf(`"%x"`, part2MD5)

	// Compute expected composite: MD5(rawMD5_1 + rawMD5_2) + "-2"
	compositeHash := md5.New()
	compositeHash.Write(part1MD5[:])
	compositeHash.Write(part2MD5[:])
	expectedETag := fmt.Sprintf(`"%x-2"`, compositeHash.Sum(nil))

	result := computeCompositeETag([]string{etag1, etag2})
	if result != expectedETag {
		t.Errorf("computeCompositeETag = %q, want %q", result, expectedETag)
	}
}

func TestCompleteMultipartUploadXMLStructure(t *testing.T) {
	mh, _, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	uploadID, etags := uploadTestParts(t, mh, meta, bucketName, "xml-key", []int{50})

	completeParts := []CompletePart{
		{PartNumber: 1, ETag: etags[0]},
	}
	xmlBody := completeMultipartUploadXML(completeParts)
	req := httptest.NewRequest("POST",
		fmt.Sprintf("/%s/xml-key?uploadId=%s", bucketName, uploadID),
		strings.NewReader(xmlBody))
	rec := httptest.NewRecorder()
	mh.CompleteMultipartUpload(rec, req)

	if rec.Code != http.StatusOK {
		body, _ := io.ReadAll(rec.Body)
		t.Fatalf("status = %d, want %d, body: %s", rec.Code, http.StatusOK, body)
	}

	body := rec.Body.String()

	// Check xmlns.
	if !strings.Contains(body, "http://s3.amazonaws.com/doc/2006-03-01/") {
		t.Error("Response missing S3 xmlns")
	}

	// Check XML declaration.
	if !strings.HasPrefix(body, "<?xml") {
		t.Error("Response missing XML declaration")
	}

	// Check required elements.
	for _, element := range []string{"<Location>", "<Bucket>", "<Key>", "<ETag>"} {
		if !strings.Contains(body, element) {
			t.Errorf("Response missing %s element", element)
		}
	}
}

func TestParseCompleteMultipartXML(t *testing.T) {
	xmlStr := `<CompleteMultipartUpload>
		<Part>
			<PartNumber>1</PartNumber>
			<ETag>"abc123"</ETag>
		</Part>
		<Part>
			<PartNumber>2</PartNumber>
			<ETag>"def456"</ETag>
		</Part>
	</CompleteMultipartUpload>`

	parts, err := parseCompleteMultipartXML(strings.NewReader(xmlStr))
	if err != nil {
		t.Fatalf("parseCompleteMultipartXML error: %v", err)
	}

	if len(parts) != 2 {
		t.Fatalf("Parts count = %d, want 2", len(parts))
	}
	if parts[0].PartNumber != 1 {
		t.Errorf("Part[0].PartNumber = %d, want 1", parts[0].PartNumber)
	}
	if parts[0].ETag != `"abc123"` {
		t.Errorf("Part[0].ETag = %q, want %q", parts[0].ETag, `"abc123"`)
	}
	if parts[1].PartNumber != 2 {
		t.Errorf("Part[1].PartNumber = %d, want 2", parts[1].PartNumber)
	}
	if parts[1].ETag != `"def456"` {
		t.Errorf("Part[1].ETag = %q, want %q", parts[1].ETag, `"def456"`)
	}
}

func TestParseCompleteMultipartXMLInvalid(t *testing.T) {
	_, err := parseCompleteMultipartXML(strings.NewReader("not xml at all"))
	if err == nil {
		t.Error("Expected error for invalid XML, got nil")
	}
}

func TestCompleteMultipartUploadFullLifecycle(t *testing.T) {
	// Full lifecycle: create upload, upload parts, complete, then verify the
	// object can be retrieved via the object handler (GetObject).
	// Part 1 must be >= 5 MiB (non-last), part 2 can be any size (last).
	mh, oh, meta, store := newTestMultipartHandler(t)
	bucketName := "test-bucket"
	createTestBucketForMultipart(t, meta, store, bucketName)

	const minPartSize = 5 * 1024 * 1024 // 5 MiB
	// Create upload.
	req := httptest.NewRequest("POST", "/"+bucketName+"/lifecycle-obj?uploads", nil)
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	mh.CreateMultipartUpload(rec, req)
	var initResult xmlutil.InitiateMultipartUploadResult
	xml.NewDecoder(rec.Body).Decode(&initResult)
	uploadID := initResult.UploadID

	// Upload 2 parts: part 1 = 5 MiB + "Hello, ", part 2 = "World!"
	part1 := append(bytes.Repeat([]byte("X"), minPartSize), []byte("Hello, ")...)
	part2 := []byte("World!")

	var etags []string
	for i, data := range [][]byte{part1, part2} {
		partNum := i + 1
		req = httptest.NewRequest("PUT",
			fmt.Sprintf("/%s/lifecycle-obj?partNumber=%d&uploadId=%s", bucketName, partNum, uploadID),
			bytes.NewReader(data))
		req.ContentLength = int64(len(data))
		rec = httptest.NewRecorder()
		mh.UploadPart(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("UploadPart %d status = %d", partNum, rec.Code)
		}
		etags = append(etags, rec.Header().Get("ETag"))
	}

	// Complete upload.
	completeParts := []CompletePart{
		{PartNumber: 1, ETag: etags[0]},
		{PartNumber: 2, ETag: etags[1]},
	}
	xmlBody := completeMultipartUploadXML(completeParts)
	req = httptest.NewRequest("POST",
		fmt.Sprintf("/%s/lifecycle-obj?uploadId=%s", bucketName, uploadID),
		strings.NewReader(xmlBody))
	rec = httptest.NewRecorder()
	mh.CompleteMultipartUpload(rec, req)

	if rec.Code != http.StatusOK {
		body, _ := io.ReadAll(rec.Body)
		t.Fatalf("Complete status = %d, body: %s", rec.Code, body)
	}

	// Retrieve object via ObjectHandler.GetObject.
	req = httptest.NewRequest("GET", "/"+bucketName+"/lifecycle-obj", nil)
	rec = httptest.NewRecorder()
	oh.GetObject(rec, req)

	if rec.Code != http.StatusOK {
		body, _ := io.ReadAll(rec.Body)
		t.Fatalf("GetObject status = %d, body: %s", rec.Code, body)
	}

	content := rec.Body.Bytes()
	expectedContent := append(part1, part2...)
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("GetObject content length = %d, want %d", len(content), len(expectedContent))
	}

	// Verify content type was preserved from the upload.
	ct := rec.Header().Get("Content-Type")
	if ct != "text/plain" {
		t.Errorf("Content-Type = %q, want %q", ct, "text/plain")
	}
}
