package handlers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bleepstore/bleepstore/internal/metadata"
	"github.com/bleepstore/bleepstore/internal/storage"
)

// newTestObjectHandler creates an ObjectHandler backed by real in-memory
// SQLite metadata store and local filesystem storage (temp dirs).
// Also creates a test bucket for use in object tests.
func newTestObjectHandler(t *testing.T) *ObjectHandler {
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

	// Create a test bucket in both metadata and storage.
	bucket := &metadata.BucketRecord{
		Name:         "test-bucket",
		Region:       "us-east-1",
		OwnerID:      "bleepstore",
		OwnerDisplay: "bleepstore",
	}
	if err := meta.CreateBucket(context.Background(), bucket); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if err := store.CreateBucket(context.Background(), "test-bucket"); err != nil {
		t.Fatalf("CreateBucket storage failed: %v", err)
	}

	return NewObjectHandler(meta, store, "bleepstore", "bleepstore", 5368709120)
}

func TestPutAndGetObject(t *testing.T) {
	h := newTestObjectHandler(t)

	// PutObject
	body := "Hello, BleepStore!"
	req := httptest.NewRequest("PUT", "/test-bucket/hello.txt", strings.NewReader(body))
	req.Header.Set("Content-Type", "text/plain")
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)

	if rec.Code != http.StatusOK {
		respBody, _ := io.ReadAll(rec.Body)
		t.Fatalf("PutObject status = %d, want %d; body: %s", rec.Code, http.StatusOK, respBody)
	}

	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("PutObject: missing ETag header")
	}
	if !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) {
		t.Errorf("PutObject: ETag not quoted: %q", etag)
	}

	// GetObject
	req = httptest.NewRequest("GET", "/test-bucket/hello.txt", nil)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)

	if rec.Code != http.StatusOK {
		respBody, _ := io.ReadAll(rec.Body)
		t.Fatalf("GetObject status = %d, want %d; body: %s", rec.Code, http.StatusOK, respBody)
	}

	gotBody := rec.Body.String()
	if gotBody != body {
		t.Errorf("GetObject body = %q, want %q", gotBody, body)
	}

	gotETag := rec.Header().Get("ETag")
	if gotETag != etag {
		t.Errorf("GetObject ETag = %q, want %q", gotETag, etag)
	}

	gotCT := rec.Header().Get("Content-Type")
	if gotCT != "text/plain" {
		t.Errorf("GetObject Content-Type = %q, want %q", gotCT, "text/plain")
	}

	gotCL := rec.Header().Get("Content-Length")
	if gotCL != "18" {
		t.Errorf("GetObject Content-Length = %q, want %q", gotCL, "18")
	}

	if rec.Header().Get("Last-Modified") == "" {
		t.Error("GetObject: missing Last-Modified header")
	}

	if rec.Header().Get("Accept-Ranges") != "bytes" {
		t.Errorf("GetObject Accept-Ranges = %q, want %q", rec.Header().Get("Accept-Ranges"), "bytes")
	}
}

func TestHeadObject(t *testing.T) {
	h := newTestObjectHandler(t)

	// Put an object first.
	body := "Head test content"
	req := httptest.NewRequest("PUT", "/test-bucket/head-test.txt", strings.NewReader(body))
	req.Header.Set("Content-Type", "text/plain")
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}
	etag := rec.Header().Get("ETag")

	// HeadObject
	req = httptest.NewRequest("HEAD", "/test-bucket/head-test.txt", nil)
	rec = httptest.NewRecorder()
	h.HeadObject(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("HeadObject status = %d, want %d", rec.Code, http.StatusOK)
	}

	// HEAD should not have a body.
	if rec.Body.Len() != 0 {
		t.Errorf("HeadObject body should be empty, got %d bytes", rec.Body.Len())
	}

	if rec.Header().Get("ETag") != etag {
		t.Errorf("HeadObject ETag = %q, want %q", rec.Header().Get("ETag"), etag)
	}

	if rec.Header().Get("Content-Type") != "text/plain" {
		t.Errorf("HeadObject Content-Type = %q, want %q", rec.Header().Get("Content-Type"), "text/plain")
	}

	if rec.Header().Get("Content-Length") != "17" {
		t.Errorf("HeadObject Content-Length = %q, want %q", rec.Header().Get("Content-Length"), "17")
	}

	if rec.Header().Get("Last-Modified") == "" {
		t.Error("HeadObject: missing Last-Modified header")
	}

	if rec.Header().Get("Accept-Ranges") != "bytes" {
		t.Errorf("HeadObject Accept-Ranges = %q, want %q", rec.Header().Get("Accept-Ranges"), "bytes")
	}
}

func TestHeadObjectNotFound(t *testing.T) {
	h := newTestObjectHandler(t)

	req := httptest.NewRequest("HEAD", "/test-bucket/nonexistent.txt", nil)
	rec := httptest.NewRecorder()
	h.HeadObject(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("HeadObject status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestGetObjectNotFound(t *testing.T) {
	h := newTestObjectHandler(t)

	req := httptest.NewRequest("GET", "/test-bucket/nonexistent.txt", nil)
	rec := httptest.NewRecorder()
	h.GetObject(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("GetObject status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "NoSuchKey") {
		t.Errorf("GetObject body missing NoSuchKey: %s", body)
	}
}

func TestGetObjectNoSuchBucket(t *testing.T) {
	h := newTestObjectHandler(t)

	req := httptest.NewRequest("GET", "/nonexistent-bucket/key.txt", nil)
	rec := httptest.NewRecorder()
	h.GetObject(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("GetObject status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "NoSuchBucket") {
		t.Errorf("GetObject body missing NoSuchBucket: %s", body)
	}
}

func TestDeleteObject(t *testing.T) {
	h := newTestObjectHandler(t)

	// Put an object.
	body := "Delete me"
	req := httptest.NewRequest("PUT", "/test-bucket/delete-me.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	// Delete the object.
	req = httptest.NewRequest("DELETE", "/test-bucket/delete-me.txt", nil)
	rec = httptest.NewRecorder()
	h.DeleteObject(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("DeleteObject status = %d, want %d", rec.Code, http.StatusNoContent)
	}

	// Verify it's gone.
	req = httptest.NewRequest("GET", "/test-bucket/delete-me.txt", nil)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("GetObject after delete status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestDeleteObjectIdempotent(t *testing.T) {
	h := newTestObjectHandler(t)

	// Delete a non-existent object: S3 returns 204.
	req := httptest.NewRequest("DELETE", "/test-bucket/never-existed.txt", nil)
	rec := httptest.NewRecorder()
	h.DeleteObject(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("DeleteObject (non-existent) status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestPutObjectOverwrite(t *testing.T) {
	h := newTestObjectHandler(t)

	// Put first version.
	body1 := "version 1"
	req := httptest.NewRequest("PUT", "/test-bucket/overwrite.txt", strings.NewReader(body1))
	req.ContentLength = int64(len(body1))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("First PutObject status = %d", rec.Code)
	}
	etag1 := rec.Header().Get("ETag")

	// Put second version.
	body2 := "version 2 (different content)"
	req = httptest.NewRequest("PUT", "/test-bucket/overwrite.txt", strings.NewReader(body2))
	req.ContentLength = int64(len(body2))
	rec = httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Second PutObject status = %d", rec.Code)
	}
	etag2 := rec.Header().Get("ETag")

	// ETags should differ.
	if etag1 == etag2 {
		t.Errorf("ETags should differ: %q == %q", etag1, etag2)
	}

	// Get should return second version.
	req = httptest.NewRequest("GET", "/test-bucket/overwrite.txt", nil)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GetObject status = %d", rec.Code)
	}
	if rec.Body.String() != body2 {
		t.Errorf("GetObject body = %q, want %q", rec.Body.String(), body2)
	}
}

func TestPutObjectWithUserMetadata(t *testing.T) {
	h := newTestObjectHandler(t)

	body := "metadata test"
	req := httptest.NewRequest("PUT", "/test-bucket/with-meta.txt", strings.NewReader(body))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("x-amz-meta-author", "tester")
	req.Header.Set("x-amz-meta-version", "42")
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)

	if rec.Code != http.StatusOK {
		respBody, _ := io.ReadAll(rec.Body)
		t.Fatalf("PutObject status = %d; body: %s", rec.Code, respBody)
	}

	// HeadObject to check metadata headers.
	req = httptest.NewRequest("HEAD", "/test-bucket/with-meta.txt", nil)
	rec = httptest.NewRecorder()
	h.HeadObject(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("HeadObject status = %d", rec.Code)
	}

	if rec.Header().Get("x-amz-meta-author") != "tester" {
		t.Errorf("x-amz-meta-author = %q, want %q", rec.Header().Get("x-amz-meta-author"), "tester")
	}
	if rec.Header().Get("x-amz-meta-version") != "42" {
		t.Errorf("x-amz-meta-version = %q, want %q", rec.Header().Get("x-amz-meta-version"), "42")
	}
}

func TestPutObjectDefaultContentType(t *testing.T) {
	h := newTestObjectHandler(t)

	body := "no content type specified"
	req := httptest.NewRequest("PUT", "/test-bucket/no-ct.bin", strings.NewReader(body))
	// Do NOT set Content-Type header.
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	// HeadObject to check content type.
	req = httptest.NewRequest("HEAD", "/test-bucket/no-ct.bin", nil)
	rec = httptest.NewRecorder()
	h.HeadObject(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("HeadObject status = %d", rec.Code)
	}

	ct := rec.Header().Get("Content-Type")
	if ct != "application/octet-stream" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/octet-stream")
	}
}

func TestPutObjectNestedKey(t *testing.T) {
	h := newTestObjectHandler(t)

	body := "nested object"
	req := httptest.NewRequest("PUT", "/test-bucket/path/to/deep/object.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)

	if rec.Code != http.StatusOK {
		respBody, _ := io.ReadAll(rec.Body)
		t.Fatalf("PutObject status = %d; body: %s", rec.Code, respBody)
	}

	// Get the nested object.
	req = httptest.NewRequest("GET", "/test-bucket/path/to/deep/object.txt", nil)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GetObject status = %d", rec.Code)
	}
	if rec.Body.String() != body {
		t.Errorf("GetObject body = %q, want %q", rec.Body.String(), body)
	}
}

func TestPutObjectEmptyBody(t *testing.T) {
	h := newTestObjectHandler(t)

	req := httptest.NewRequest("PUT", "/test-bucket/empty.txt", strings.NewReader(""))
	req.ContentLength = 0
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)

	if rec.Code != http.StatusOK {
		respBody, _ := io.ReadAll(rec.Body)
		t.Fatalf("PutObject (empty) status = %d; body: %s", rec.Code, respBody)
	}

	// HeadObject should show size 0.
	req = httptest.NewRequest("HEAD", "/test-bucket/empty.txt", nil)
	rec = httptest.NewRecorder()
	h.HeadObject(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("HeadObject status = %d", rec.Code)
	}

	if rec.Header().Get("Content-Length") != "0" {
		t.Errorf("Content-Length = %q, want %q", rec.Header().Get("Content-Length"), "0")
	}
}

func TestExtractObjectKey(t *testing.T) {
	tests := []struct {
		path    string
		wantKey string
	}{
		{"/bucket/key", "key"},
		{"/bucket/path/to/key", "path/to/key"},
		{"/bucket/", ""},
		{"/bucket", ""},
		{"/", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			got := extractObjectKey(req)
			if got != tt.wantKey {
				t.Errorf("extractObjectKey(%q) = %q, want %q", tt.path, got, tt.wantKey)
			}
		})
	}
}

func TestExtractUserMetadata(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Amz-Meta-Author", "tester")
	req.Header.Set("X-Amz-Meta-Version", "42")
	req.Header.Set("Content-Type", "text/plain")

	meta := extractUserMetadata(req)

	if meta == nil {
		t.Fatal("extractUserMetadata returned nil")
	}
	if meta["author"] != "tester" {
		t.Errorf("meta[author] = %q, want %q", meta["author"], "tester")
	}
	if meta["version"] != "42" {
		t.Errorf("meta[version] = %q, want %q", meta["version"], "42")
	}
	if _, ok := meta["content-type"]; ok {
		t.Error("extractUserMetadata should not include non-meta headers")
	}
}

func TestExtractUserMetadataEmpty(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Content-Type", "text/plain")

	meta := extractUserMetadata(req)
	if meta != nil {
		t.Errorf("extractUserMetadata with no meta headers should return nil, got %v", meta)
	}
}

// --- Stage 5a: CopyObject Tests ---

func TestCopyObject(t *testing.T) {
	h := newTestObjectHandler(t)

	// Put a source object.
	body := "copy me"
	req := httptest.NewRequest("PUT", "/test-bucket/original.txt", strings.NewReader(body))
	req.Header.Set("Content-Type", "text/plain")
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	// CopyObject (COPY directive, default).
	req = httptest.NewRequest("PUT", "/test-bucket/copy.txt", nil)
	req.Header.Set("X-Amz-Copy-Source", "/test-bucket/original.txt")
	rec = httptest.NewRecorder()
	h.CopyObject(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("CopyObject status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "CopyObjectResult") {
		t.Errorf("CopyObject response missing CopyObjectResult: %s", respBody)
	}
	if !strings.Contains(respBody, "ETag") {
		t.Errorf("CopyObject response missing ETag: %s", respBody)
	}
	if !strings.Contains(respBody, "LastModified") {
		t.Errorf("CopyObject response missing LastModified: %s", respBody)
	}

	// Verify copy content.
	req = httptest.NewRequest("GET", "/test-bucket/copy.txt", nil)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GetObject (copy) status = %d", rec.Code)
	}
	if rec.Body.String() != body {
		t.Errorf("GetObject (copy) body = %q, want %q", rec.Body.String(), body)
	}

	// Verify copied metadata: Content-Type should match source.
	if rec.Header().Get("Content-Type") != "text/plain" {
		t.Errorf("GetObject (copy) Content-Type = %q, want %q", rec.Header().Get("Content-Type"), "text/plain")
	}
}

func TestCopyObjectWithReplaceDirective(t *testing.T) {
	h := newTestObjectHandler(t)

	// Put source with metadata.
	body := "data for replace"
	req := httptest.NewRequest("PUT", "/test-bucket/src.txt", strings.NewReader(body))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("x-amz-meta-original", "true")
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	// CopyObject with REPLACE directive.
	req = httptest.NewRequest("PUT", "/test-bucket/dst.txt", nil)
	req.Header.Set("X-Amz-Copy-Source", "/test-bucket/src.txt")
	req.Header.Set("x-amz-metadata-directive", "REPLACE")
	req.Header.Set("Content-Type", "text/csv")
	req.Header.Set("x-amz-meta-copied", "true")
	rec = httptest.NewRecorder()
	h.CopyObject(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("CopyObject (REPLACE) status = %d; body: %s", rec.Code, rec.Body.String())
	}

	// Verify destination metadata: should have new content type and new metadata.
	req = httptest.NewRequest("HEAD", "/test-bucket/dst.txt", nil)
	rec = httptest.NewRecorder()
	h.HeadObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("HeadObject status = %d", rec.Code)
	}

	if rec.Header().Get("Content-Type") != "text/csv" {
		t.Errorf("Content-Type = %q, want %q", rec.Header().Get("Content-Type"), "text/csv")
	}
	if rec.Header().Get("x-amz-meta-copied") != "true" {
		t.Errorf("x-amz-meta-copied = %q, want %q", rec.Header().Get("x-amz-meta-copied"), "true")
	}
	// Original metadata should NOT be present (replaced).
	if rec.Header().Get("x-amz-meta-original") != "" {
		t.Errorf("x-amz-meta-original should be empty, got %q", rec.Header().Get("x-amz-meta-original"))
	}
}

func TestCopyObjectNonexistentSource(t *testing.T) {
	h := newTestObjectHandler(t)

	req := httptest.NewRequest("PUT", "/test-bucket/dst.txt", nil)
	req.Header.Set("X-Amz-Copy-Source", "/test-bucket/nonexistent.txt")
	rec := httptest.NewRecorder()
	h.CopyObject(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("CopyObject (nonexistent source) status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if !strings.Contains(rec.Body.String(), "NoSuchKey") {
		t.Errorf("CopyObject body should contain NoSuchKey: %s", rec.Body.String())
	}
}

func TestCopyObjectInvalidSource(t *testing.T) {
	h := newTestObjectHandler(t)

	req := httptest.NewRequest("PUT", "/test-bucket/dst.txt", nil)
	req.Header.Set("X-Amz-Copy-Source", "")
	rec := httptest.NewRecorder()
	h.CopyObject(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("CopyObject (invalid source) status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

// --- Stage 5a: DeleteObjects Tests ---

func TestDeleteObjects(t *testing.T) {
	h := newTestObjectHandler(t)

	// Create 3 objects.
	for _, key := range []string{"a.txt", "b.txt", "c.txt"} {
		body := "data"
		req := httptest.NewRequest("PUT", "/test-bucket/"+key, strings.NewReader(body))
		req.ContentLength = int64(len(body))
		rec := httptest.NewRecorder()
		h.PutObject(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutObject %s status = %d", key, rec.Code)
		}
	}

	// DeleteObjects request.
	xmlBody := `<?xml version="1.0" encoding="UTF-8"?>
<Delete>
  <Object><Key>a.txt</Key></Object>
  <Object><Key>b.txt</Key></Object>
  <Object><Key>c.txt</Key></Object>
</Delete>`
	req := httptest.NewRequest("POST", "/test-bucket?delete", strings.NewReader(xmlBody))
	rec := httptest.NewRecorder()
	h.DeleteObjects(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("DeleteObjects status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "DeleteResult") {
		t.Errorf("DeleteObjects response missing DeleteResult: %s", respBody)
	}

	// Verify all keys are reported as deleted.
	for _, key := range []string{"a.txt", "b.txt", "c.txt"} {
		if !strings.Contains(respBody, "<Key>"+key+"</Key>") {
			t.Errorf("DeleteObjects response missing key %q: %s", key, respBody)
		}
	}

	// Verify objects are actually gone.
	for _, key := range []string{"a.txt", "b.txt", "c.txt"} {
		req := httptest.NewRequest("GET", "/test-bucket/"+key, nil)
		rec := httptest.NewRecorder()
		h.GetObject(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("GetObject %s after delete status = %d, want 404", key, rec.Code)
		}
	}
}

func TestDeleteObjectsQuietMode(t *testing.T) {
	h := newTestObjectHandler(t)

	// Create an object.
	body := "quiet test"
	req := httptest.NewRequest("PUT", "/test-bucket/quiet.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	// DeleteObjects in quiet mode.
	xmlBody := `<?xml version="1.0" encoding="UTF-8"?>
<Delete>
  <Quiet>true</Quiet>
  <Object><Key>quiet.txt</Key></Object>
</Delete>`
	req = httptest.NewRequest("POST", "/test-bucket?delete", strings.NewReader(xmlBody))
	rec = httptest.NewRecorder()
	h.DeleteObjects(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("DeleteObjects (quiet) status = %d; body: %s", rec.Code, rec.Body.String())
	}

	respBody := rec.Body.String()
	// In quiet mode, successful deletes should NOT be listed.
	if strings.Contains(respBody, "<Deleted>") {
		t.Errorf("DeleteObjects (quiet) should not contain <Deleted>: %s", respBody)
	}
	// Errors should also not be present (we expect success).
	if strings.Contains(respBody, "<Error>") {
		t.Errorf("DeleteObjects (quiet) should not contain <Error>: %s", respBody)
	}
}

func TestDeleteObjectsMalformedXML(t *testing.T) {
	h := newTestObjectHandler(t)

	req := httptest.NewRequest("POST", "/test-bucket?delete", strings.NewReader("not xml"))
	rec := httptest.NewRecorder()
	h.DeleteObjects(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("DeleteObjects (malformed XML) status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if !strings.Contains(rec.Body.String(), "MalformedXML") {
		t.Errorf("DeleteObjects body should contain MalformedXML: %s", rec.Body.String())
	}
}

// --- Stage 5a: ListObjectsV2 Tests ---

func putTestObjects(t *testing.T, h *ObjectHandler, keys []string) {
	t.Helper()
	for _, key := range keys {
		body := "data for " + key
		req := httptest.NewRequest("PUT", "/test-bucket/"+key, strings.NewReader(body))
		req.ContentLength = int64(len(body))
		rec := httptest.NewRecorder()
		h.PutObject(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutObject %s status = %d; body: %s", key, rec.Code, rec.Body.String())
		}
	}
}

func TestListObjectsV2(t *testing.T) {
	h := newTestObjectHandler(t)

	keys := []string{
		"file1.txt",
		"file2.txt",
		"photos/2024/jan/photo1.jpg",
		"photos/2024/jan/photo2.jpg",
		"photos/2024/feb/photo3.jpg",
		"photos/2025/mar/photo4.jpg",
		"docs/readme.md",
		"docs/guide.md",
	}
	putTestObjects(t, h, keys)

	req := httptest.NewRequest("GET", "/test-bucket?list-type=2", nil)
	rec := httptest.NewRecorder()
	h.ListObjectsV2(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ListObjectsV2 status = %d; body: %s", rec.Code, rec.Body.String())
	}

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "ListBucketResult") {
		t.Errorf("ListObjectsV2 response missing ListBucketResult: %s", respBody)
	}
	if !strings.Contains(respBody, "<KeyCount>8</KeyCount>") {
		t.Errorf("ListObjectsV2 KeyCount should be 8: %s", respBody)
	}

	// Verify all keys present.
	for _, key := range keys {
		if !strings.Contains(respBody, "<Key>"+key+"</Key>") {
			t.Errorf("ListObjectsV2 missing key %q: %s", key, respBody)
		}
	}
}

func TestListObjectsV2WithPrefix(t *testing.T) {
	h := newTestObjectHandler(t)

	keys := []string{
		"file1.txt",
		"file2.txt",
		"photos/2024/jan/photo1.jpg",
		"photos/2024/jan/photo2.jpg",
		"photos/2024/feb/photo3.jpg",
		"photos/2025/mar/photo4.jpg",
		"docs/readme.md",
		"docs/guide.md",
	}
	putTestObjects(t, h, keys)

	req := httptest.NewRequest("GET", "/test-bucket?list-type=2&prefix=photos/", nil)
	rec := httptest.NewRecorder()
	h.ListObjectsV2(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ListObjectsV2 (prefix) status = %d", rec.Code)
	}

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "<KeyCount>4</KeyCount>") {
		t.Errorf("ListObjectsV2 (prefix) KeyCount should be 4: %s", respBody)
	}
	if strings.Contains(respBody, "<Key>file1.txt</Key>") {
		t.Errorf("ListObjectsV2 (prefix) should not contain file1.txt: %s", respBody)
	}
}

func TestListObjectsV2WithDelimiter(t *testing.T) {
	h := newTestObjectHandler(t)

	keys := []string{
		"file1.txt",
		"file2.txt",
		"photos/2024/jan/photo1.jpg",
		"photos/2024/jan/photo2.jpg",
		"docs/readme.md",
	}
	putTestObjects(t, h, keys)

	req := httptest.NewRequest("GET", "/test-bucket?list-type=2&delimiter=/", nil)
	rec := httptest.NewRecorder()
	h.ListObjectsV2(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ListObjectsV2 (delimiter) status = %d", rec.Code)
	}

	respBody := rec.Body.String()
	// Should have file1.txt and file2.txt as Contents.
	if !strings.Contains(respBody, "<Key>file1.txt</Key>") {
		t.Errorf("ListObjectsV2 (delimiter) missing file1.txt: %s", respBody)
	}
	if !strings.Contains(respBody, "<Key>file2.txt</Key>") {
		t.Errorf("ListObjectsV2 (delimiter) missing file2.txt: %s", respBody)
	}

	// Should have CommonPrefixes for photos/ and docs/.
	if !strings.Contains(respBody, "<Prefix>photos/</Prefix>") {
		t.Errorf("ListObjectsV2 (delimiter) missing CommonPrefix photos/: %s", respBody)
	}
	if !strings.Contains(respBody, "<Prefix>docs/</Prefix>") {
		t.Errorf("ListObjectsV2 (delimiter) missing CommonPrefix docs/: %s", respBody)
	}
}

func TestListObjectsV2Pagination(t *testing.T) {
	h := newTestObjectHandler(t)

	// Create 5 objects.
	keys := []string{"page-000.txt", "page-001.txt", "page-002.txt", "page-003.txt", "page-004.txt"}
	putTestObjects(t, h, keys)

	// First page: max 2.
	req := httptest.NewRequest("GET", "/test-bucket?list-type=2&max-keys=2", nil)
	rec := httptest.NewRecorder()
	h.ListObjectsV2(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ListObjectsV2 (page 1) status = %d", rec.Code)
	}

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "<KeyCount>2</KeyCount>") {
		t.Errorf("ListObjectsV2 (page 1) KeyCount should be 2: %s", respBody)
	}
	if !strings.Contains(respBody, "<MaxKeys>2</MaxKeys>") {
		t.Errorf("ListObjectsV2 (page 1) MaxKeys should be 2: %s", respBody)
	}
	if !strings.Contains(respBody, "<IsTruncated>true</IsTruncated>") {
		t.Errorf("ListObjectsV2 (page 1) should be truncated: %s", respBody)
	}
	if !strings.Contains(respBody, "NextContinuationToken") {
		t.Errorf("ListObjectsV2 (page 1) missing NextContinuationToken: %s", respBody)
	}
}

func TestListObjectsV2EmptyBucket(t *testing.T) {
	h := newTestObjectHandler(t)

	req := httptest.NewRequest("GET", "/test-bucket?list-type=2", nil)
	rec := httptest.NewRecorder()
	h.ListObjectsV2(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ListObjectsV2 (empty) status = %d", rec.Code)
	}

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "<KeyCount>0</KeyCount>") {
		t.Errorf("ListObjectsV2 (empty) KeyCount should be 0: %s", respBody)
	}
}

func TestListObjectsV2StartAfter(t *testing.T) {
	h := newTestObjectHandler(t)

	keys := []string{"file1.txt", "file2.txt", "file3.txt", "file4.txt"}
	putTestObjects(t, h, keys)

	req := httptest.NewRequest("GET", "/test-bucket?list-type=2&start-after=file2.txt", nil)
	rec := httptest.NewRecorder()
	h.ListObjectsV2(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ListObjectsV2 (start-after) status = %d", rec.Code)
	}

	respBody := rec.Body.String()
	// file1.txt and file2.txt should NOT be included.
	if strings.Contains(respBody, "<Key>file1.txt</Key>") {
		t.Errorf("ListObjectsV2 (start-after) should not contain file1.txt: %s", respBody)
	}
	if strings.Contains(respBody, "<Key>file2.txt</Key>") {
		t.Errorf("ListObjectsV2 (start-after) should not contain file2.txt: %s", respBody)
	}
	// file3.txt and file4.txt should be included.
	if !strings.Contains(respBody, "<Key>file3.txt</Key>") {
		t.Errorf("ListObjectsV2 (start-after) should contain file3.txt: %s", respBody)
	}
	if !strings.Contains(respBody, "<Key>file4.txt</Key>") {
		t.Errorf("ListObjectsV2 (start-after) should contain file4.txt: %s", respBody)
	}
}

func TestListObjectsV2ContentFields(t *testing.T) {
	h := newTestObjectHandler(t)

	body := "test content"
	req := httptest.NewRequest("PUT", "/test-bucket/fields.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	req = httptest.NewRequest("GET", "/test-bucket?list-type=2", nil)
	rec = httptest.NewRecorder()
	h.ListObjectsV2(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ListObjectsV2 status = %d", rec.Code)
	}

	respBody := rec.Body.String()
	// Verify each object entry has required fields.
	if !strings.Contains(respBody, "<Key>fields.txt</Key>") {
		t.Errorf("missing Key: %s", respBody)
	}
	if !strings.Contains(respBody, "<LastModified>") {
		t.Errorf("missing LastModified: %s", respBody)
	}
	if !strings.Contains(respBody, "<ETag>") {
		t.Errorf("missing ETag: %s", respBody)
	}
	if !strings.Contains(respBody, "<Size>") {
		t.Errorf("missing Size: %s", respBody)
	}
	if !strings.Contains(respBody, "<StorageClass>") {
		t.Errorf("missing StorageClass: %s", respBody)
	}
}

// --- Stage 5a: ListObjects V1 Tests ---

func TestListObjectsV1(t *testing.T) {
	h := newTestObjectHandler(t)

	keys := []string{
		"file1.txt",
		"file2.txt",
		"photos/2024/jan/photo1.jpg",
		"docs/readme.md",
	}
	putTestObjects(t, h, keys)

	req := httptest.NewRequest("GET", "/test-bucket", nil)
	rec := httptest.NewRecorder()
	h.ListObjects(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ListObjects V1 status = %d; body: %s", rec.Code, rec.Body.String())
	}

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "ListBucketResult") {
		t.Errorf("ListObjects V1 response missing ListBucketResult: %s", respBody)
	}

	for _, key := range keys {
		if !strings.Contains(respBody, "<Key>"+key+"</Key>") {
			t.Errorf("ListObjects V1 missing key %q: %s", key, respBody)
		}
	}
}

func TestListObjectsV1WithMarker(t *testing.T) {
	h := newTestObjectHandler(t)

	keys := []string{
		"file1.txt",
		"file2.txt",
		"photos/2024/jan/photo1.jpg",
		"photos/2024/jan/photo2.jpg",
		"photos/2024/feb/photo3.jpg",
		"photos/2025/mar/photo4.jpg",
		"docs/readme.md",
		"docs/guide.md",
	}
	putTestObjects(t, h, keys)

	req := httptest.NewRequest("GET", "/test-bucket?max-keys=2", nil)
	rec := httptest.NewRecorder()
	h.ListObjects(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ListObjects V1 (marker) status = %d", rec.Code)
	}

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "<IsTruncated>true</IsTruncated>") {
		t.Errorf("ListObjects V1 should be truncated: %s", respBody)
	}
	if !strings.Contains(respBody, "<MaxKeys>2</MaxKeys>") {
		t.Errorf("ListObjects V1 MaxKeys should be 2: %s", respBody)
	}
}

func TestListObjectsV2NoSuchBucket(t *testing.T) {
	h := newTestObjectHandler(t)

	req := httptest.NewRequest("GET", "/nonexistent-bucket?list-type=2", nil)
	rec := httptest.NewRecorder()
	h.ListObjectsV2(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("ListObjectsV2 (no bucket) status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if !strings.Contains(rec.Body.String(), "NoSuchBucket") {
		t.Errorf("ListObjectsV2 body should contain NoSuchBucket: %s", rec.Body.String())
	}
}

// --- Stage 5a: parseCopySource Tests ---

func TestParseCopySource(t *testing.T) {
	tests := []struct {
		name       string
		header     string
		wantBucket string
		wantKey    string
		wantOK     bool
	}{
		{"standard", "/bucket/key", "bucket", "key", true},
		{"no leading slash", "bucket/key", "bucket", "key", true},
		{"nested key", "/bucket/path/to/key.txt", "bucket", "path/to/key.txt", true},
		{"url encoded", "/bucket/key%20with%20spaces.txt", "bucket", "key with spaces.txt", true},
		{"empty", "", "", "", false},
		{"bucket only", "/bucket/", "", "", false},
		{"no key", "/bucket", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, ok := parseCopySource(tt.header)
			if ok != tt.wantOK {
				t.Errorf("parseCopySource(%q) ok = %v, want %v", tt.header, ok, tt.wantOK)
			}
			if ok {
				if bucket != tt.wantBucket {
					t.Errorf("parseCopySource(%q) bucket = %q, want %q", tt.header, bucket, tt.wantBucket)
				}
				if key != tt.wantKey {
					t.Errorf("parseCopySource(%q) key = %q, want %q", tt.header, key, tt.wantKey)
				}
			}
		})
	}
}

// --- Stage 5b: parseRange Tests ---

func TestParseRange(t *testing.T) {
	tests := []struct {
		name      string
		header    string
		size      int64
		wantStart int64
		wantEnd   int64
		wantErr   bool
	}{
		{"first 5 bytes", "bytes=0-4", 100, 0, 4, false},
		{"from byte 5 to end", "bytes=5-", 100, 5, 99, false},
		{"last 10 bytes", "bytes=-10", 100, 90, 99, false},
		{"last 10 from small object", "bytes=-10", 5, 0, 4, false},
		{"single byte", "bytes=0-0", 100, 0, 0, false},
		{"last byte", "bytes=99-99", 100, 99, 99, false},
		{"end beyond size is clamped", "bytes=90-200", 100, 90, 99, false},
		{"entire object", "bytes=0-99", 100, 0, 99, false},
		{"start beyond size", "bytes=100-200", 100, 0, 0, true},
		{"empty object", "bytes=0-0", 0, 0, 0, true},
		{"no bytes prefix", "0-4", 100, 0, 0, true},
		{"multi range unsupported", "bytes=0-4,10-20", 100, 0, 0, true},
		{"negative suffix zero", "bytes=-0", 100, 0, 0, true},
		{"start > end", "bytes=10-5", 100, 0, 0, true},
		{"suffix larger than file", "bytes=-200", 100, 0, 99, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := parseRange(tt.header, tt.size)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseRange(%q, %d) expected error, got start=%d end=%d", tt.header, tt.size, start, end)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRange(%q, %d) unexpected error: %v", tt.header, tt.size, err)
			}
			if start != tt.wantStart {
				t.Errorf("parseRange(%q, %d) start = %d, want %d", tt.header, tt.size, start, tt.wantStart)
			}
			if end != tt.wantEnd {
				t.Errorf("parseRange(%q, %d) end = %d, want %d", tt.header, tt.size, end, tt.wantEnd)
			}
		})
	}
}

// --- Stage 5b: Range Request Handler Tests ---

func TestGetObjectRangeFirstBytes(t *testing.T) {
	h := newTestObjectHandler(t)

	// Put a 26-byte object.
	body := "abcdefghijklmnopqrstuvwxyz"
	req := httptest.NewRequest("PUT", "/test-bucket/range-test.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	// GET with Range: bytes=0-4.
	req = httptest.NewRequest("GET", "/test-bucket/range-test.txt", nil)
	req.Header.Set("Range", "bytes=0-4")
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("GetObject range status = %d, want %d; body: %s", rec.Code, http.StatusPartialContent, rec.Body.String())
	}

	if rec.Body.String() != "abcde" {
		t.Errorf("GetObject range body = %q, want %q", rec.Body.String(), "abcde")
	}

	cr := rec.Header().Get("Content-Range")
	if cr != "bytes 0-4/26" {
		t.Errorf("Content-Range = %q, want %q", cr, "bytes 0-4/26")
	}

	cl := rec.Header().Get("Content-Length")
	if cl != "5" {
		t.Errorf("Content-Length = %q, want %q", cl, "5")
	}
}

func TestGetObjectRangeOpenEnd(t *testing.T) {
	h := newTestObjectHandler(t)

	body := "abcdefghijklmnopqrstuvwxyz"
	req := httptest.NewRequest("PUT", "/test-bucket/range-open.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	// GET with Range: bytes=20-.
	req = httptest.NewRequest("GET", "/test-bucket/range-open.txt", nil)
	req.Header.Set("Range", "bytes=20-")
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("GetObject range status = %d, want %d", rec.Code, http.StatusPartialContent)
	}

	if rec.Body.String() != "uvwxyz" {
		t.Errorf("GetObject range body = %q, want %q", rec.Body.String(), "uvwxyz")
	}

	cr := rec.Header().Get("Content-Range")
	if cr != "bytes 20-25/26" {
		t.Errorf("Content-Range = %q, want %q", cr, "bytes 20-25/26")
	}
}

func TestGetObjectRangeSuffix(t *testing.T) {
	h := newTestObjectHandler(t)

	body := "abcdefghijklmnopqrstuvwxyz"
	req := httptest.NewRequest("PUT", "/test-bucket/range-suffix.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	// GET with Range: bytes=-5 (last 5 bytes).
	req = httptest.NewRequest("GET", "/test-bucket/range-suffix.txt", nil)
	req.Header.Set("Range", "bytes=-5")
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("GetObject range status = %d, want %d", rec.Code, http.StatusPartialContent)
	}

	if rec.Body.String() != "vwxyz" {
		t.Errorf("GetObject range body = %q, want %q", rec.Body.String(), "vwxyz")
	}

	cr := rec.Header().Get("Content-Range")
	if cr != "bytes 21-25/26" {
		t.Errorf("Content-Range = %q, want %q", cr, "bytes 21-25/26")
	}
}

func TestGetObjectRangeUnsatisfiable(t *testing.T) {
	h := newTestObjectHandler(t)

	body := "short"
	req := httptest.NewRequest("PUT", "/test-bucket/range-unsat.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	// GET with Range: bytes=100-200 (beyond object size).
	req = httptest.NewRequest("GET", "/test-bucket/range-unsat.txt", nil)
	req.Header.Set("Range", "bytes=100-200")
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)

	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("GetObject range status = %d, want %d", rec.Code, http.StatusRequestedRangeNotSatisfiable)
	}

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "InvalidRange") {
		t.Errorf("expected InvalidRange error, got: %s", respBody)
	}
}

// --- Stage 5b: Conditional Request Handler Tests ---

func TestGetObjectIfMatch(t *testing.T) {
	h := newTestObjectHandler(t)

	body := "conditional test"
	req := httptest.NewRequest("PUT", "/test-bucket/cond.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}
	etag := rec.Header().Get("ETag")

	// If-Match with correct ETag: should succeed.
	req = httptest.NewRequest("GET", "/test-bucket/cond.txt", nil)
	req.Header.Set("If-Match", etag)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GetObject If-Match (match) status = %d, want %d", rec.Code, http.StatusOK)
	}

	// If-Match with wrong ETag: should return 412.
	req = httptest.NewRequest("GET", "/test-bucket/cond.txt", nil)
	req.Header.Set("If-Match", `"wrong-etag"`)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Errorf("GetObject If-Match (mismatch) status = %d, want %d", rec.Code, http.StatusPreconditionFailed)
	}
}

func TestGetObjectIfNoneMatch(t *testing.T) {
	h := newTestObjectHandler(t)

	body := "none-match test"
	req := httptest.NewRequest("PUT", "/test-bucket/none-match.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}
	etag := rec.Header().Get("ETag")

	// If-None-Match with matching ETag: should return 304.
	req = httptest.NewRequest("GET", "/test-bucket/none-match.txt", nil)
	req.Header.Set("If-None-Match", etag)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Errorf("GetObject If-None-Match (match, GET) status = %d, want %d", rec.Code, http.StatusNotModified)
	}

	// If-None-Match with different ETag: should succeed.
	req = httptest.NewRequest("GET", "/test-bucket/none-match.txt", nil)
	req.Header.Set("If-None-Match", `"different-etag"`)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GetObject If-None-Match (no match) status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestHeadObjectIfNoneMatch(t *testing.T) {
	h := newTestObjectHandler(t)

	body := "head none-match test"
	req := httptest.NewRequest("PUT", "/test-bucket/head-nm.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}
	etag := rec.Header().Get("ETag")

	// HEAD with If-None-Match matching: 304.
	req = httptest.NewRequest("HEAD", "/test-bucket/head-nm.txt", nil)
	req.Header.Set("If-None-Match", etag)
	rec = httptest.NewRecorder()
	h.HeadObject(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Errorf("HeadObject If-None-Match (match) status = %d, want %d", rec.Code, http.StatusNotModified)
	}
}

func TestGetObjectIfModifiedSince(t *testing.T) {
	h := newTestObjectHandler(t)

	body := "modified-since test"
	req := httptest.NewRequest("PUT", "/test-bucket/mod-since.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	// If-Modified-Since with a future date: should return 304.
	futureDate := time.Now().Add(24 * time.Hour).UTC().Format(http.TimeFormat)
	req = httptest.NewRequest("GET", "/test-bucket/mod-since.txt", nil)
	req.Header.Set("If-Modified-Since", futureDate)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Errorf("GetObject If-Modified-Since (future) status = %d, want %d", rec.Code, http.StatusNotModified)
	}

	// If-Modified-Since with a past date: should succeed.
	pastDate := time.Now().Add(-24 * time.Hour).UTC().Format(http.TimeFormat)
	req = httptest.NewRequest("GET", "/test-bucket/mod-since.txt", nil)
	req.Header.Set("If-Modified-Since", pastDate)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GetObject If-Modified-Since (past) status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestGetObjectIfUnmodifiedSince(t *testing.T) {
	h := newTestObjectHandler(t)

	body := "unmodified-since test"
	req := httptest.NewRequest("PUT", "/test-bucket/unmod-since.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	// If-Unmodified-Since with a future date: should succeed.
	futureDate := time.Now().Add(24 * time.Hour).UTC().Format(http.TimeFormat)
	req = httptest.NewRequest("GET", "/test-bucket/unmod-since.txt", nil)
	req.Header.Set("If-Unmodified-Since", futureDate)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GetObject If-Unmodified-Since (future) status = %d, want %d", rec.Code, http.StatusOK)
	}

	// If-Unmodified-Since with a past date: should return 412.
	pastDate := time.Now().Add(-24 * time.Hour).UTC().Format(http.TimeFormat)
	req = httptest.NewRequest("GET", "/test-bucket/unmod-since.txt", nil)
	req.Header.Set("If-Unmodified-Since", pastDate)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Errorf("GetObject If-Unmodified-Since (past) status = %d, want %d", rec.Code, http.StatusPreconditionFailed)
	}
}

// --- Stage 5b: checkConditionalHeaders Unit Tests ---

func TestCheckConditionalHeaders(t *testing.T) {
	etag := `"abc123"`
	lastModified := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		method   string
		headers  map[string]string
		wantCode int
		wantSkip bool
	}{
		{
			name:     "no conditional headers",
			method:   "GET",
			wantCode: 0,
			wantSkip: false,
		},
		{
			name:     "If-Match match",
			method:   "GET",
			headers:  map[string]string{"If-Match": `"abc123"`},
			wantCode: 0,
			wantSkip: false,
		},
		{
			name:     "If-Match mismatch",
			method:   "GET",
			headers:  map[string]string{"If-Match": `"wrong"`},
			wantCode: 412,
			wantSkip: true,
		},
		{
			name:     "If-Match wildcard",
			method:   "GET",
			headers:  map[string]string{"If-Match": `*`},
			wantCode: 0,
			wantSkip: false,
		},
		{
			name:     "If-None-Match match GET",
			method:   "GET",
			headers:  map[string]string{"If-None-Match": `"abc123"`},
			wantCode: 304,
			wantSkip: true,
		},
		{
			name:     "If-None-Match match HEAD",
			method:   "HEAD",
			headers:  map[string]string{"If-None-Match": `"abc123"`},
			wantCode: 304,
			wantSkip: true,
		},
		{
			name:     "If-None-Match match PUT",
			method:   "PUT",
			headers:  map[string]string{"If-None-Match": `"abc123"`},
			wantCode: 412,
			wantSkip: true,
		},
		{
			name:     "If-None-Match no match",
			method:   "GET",
			headers:  map[string]string{"If-None-Match": `"different"`},
			wantCode: 0,
			wantSkip: false,
		},
		{
			name:     "If-Modified-Since not modified",
			method:   "GET",
			headers:  map[string]string{"If-Modified-Since": "Fri, 16 Jan 2026 12:00:00 GMT"},
			wantCode: 304,
			wantSkip: true,
		},
		{
			name:     "If-Modified-Since modified",
			method:   "GET",
			headers:  map[string]string{"If-Modified-Since": "Wed, 14 Jan 2026 12:00:00 GMT"},
			wantCode: 0,
			wantSkip: false,
		},
		{
			name:     "If-Unmodified-Since not modified",
			method:   "GET",
			headers:  map[string]string{"If-Unmodified-Since": "Fri, 16 Jan 2026 12:00:00 GMT"},
			wantCode: 0,
			wantSkip: false,
		},
		{
			name:     "If-Unmodified-Since modified",
			method:   "GET",
			headers:  map[string]string{"If-Unmodified-Since": "Wed, 14 Jan 2026 12:00:00 GMT"},
			wantCode: 412,
			wantSkip: true,
		},
		{
			name:   "If-Match takes priority over If-Unmodified-Since",
			method: "GET",
			headers: map[string]string{
				"If-Match":            `"abc123"`,
				"If-Unmodified-Since": "Wed, 14 Jan 2026 12:00:00 GMT",
			},
			wantCode: 0,
			wantSkip: false,
		},
		{
			name:   "If-None-Match takes priority over If-Modified-Since",
			method: "GET",
			headers: map[string]string{
				"If-None-Match":     `"different"`,
				"If-Modified-Since": "Fri, 16 Jan 2026 12:00:00 GMT",
			},
			wantCode: 0,
			wantSkip: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/test-bucket/test.txt", nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			code, skip := checkConditionalHeaders(req, etag, lastModified)
			if code != tt.wantCode {
				t.Errorf("checkConditionalHeaders code = %d, want %d", code, tt.wantCode)
			}
			if skip != tt.wantSkip {
				t.Errorf("checkConditionalHeaders skip = %v, want %v", skip, tt.wantSkip)
			}
		})
	}
}

// --- Stage 5b: Object ACL Tests ---

func TestGetObjectAcl(t *testing.T) {
	h := newTestObjectHandler(t)

	// Put an object.
	body := "acl test"
	req := httptest.NewRequest("PUT", "/test-bucket/acl-test.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	// Get object ACL.
	req = httptest.NewRequest("GET", "/test-bucket/acl-test.txt?acl", nil)
	rec = httptest.NewRecorder()
	h.GetObjectAcl(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GetObjectAcl status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "AccessControlPolicy") {
		t.Errorf("GetObjectAcl missing AccessControlPolicy: %s", respBody)
	}
	if !strings.Contains(respBody, "FULL_CONTROL") {
		t.Errorf("GetObjectAcl missing FULL_CONTROL: %s", respBody)
	}
	if !strings.Contains(respBody, "bleepstore") {
		t.Errorf("GetObjectAcl missing owner: %s", respBody)
	}
	if !strings.Contains(respBody, "xmlns:xsi") {
		t.Errorf("GetObjectAcl missing xmlns:xsi: %s", respBody)
	}
	if !strings.Contains(respBody, `xsi:type="CanonicalUser"`) {
		t.Errorf("GetObjectAcl missing xsi:type: %s", respBody)
	}
}

func TestGetObjectAclNoSuchKey(t *testing.T) {
	h := newTestObjectHandler(t)

	req := httptest.NewRequest("GET", "/test-bucket/nonexistent.txt?acl", nil)
	rec := httptest.NewRecorder()
	h.GetObjectAcl(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("GetObjectAcl (no key) status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if !strings.Contains(rec.Body.String(), "NoSuchKey") {
		t.Errorf("GetObjectAcl body should contain NoSuchKey: %s", rec.Body.String())
	}
}

func TestPutObjectAclCanned(t *testing.T) {
	h := newTestObjectHandler(t)

	// Put an object.
	body := "acl put test"
	req := httptest.NewRequest("PUT", "/test-bucket/acl-put.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	// Set canned ACL to public-read.
	req = httptest.NewRequest("PUT", "/test-bucket/acl-put.txt?acl", nil)
	req.Header.Set("x-amz-acl", "public-read")
	rec = httptest.NewRecorder()
	h.PutObjectAcl(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("PutObjectAcl status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	// Verify the ACL was updated by reading it back.
	req = httptest.NewRequest("GET", "/test-bucket/acl-put.txt?acl", nil)
	rec = httptest.NewRecorder()
	h.GetObjectAcl(rec, req)

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "FULL_CONTROL") {
		t.Errorf("After PutObjectAcl, missing FULL_CONTROL: %s", respBody)
	}
	if !strings.Contains(respBody, "READ") {
		t.Errorf("After PutObjectAcl(public-read), missing READ grant: %s", respBody)
	}
	if !strings.Contains(respBody, "AllUsers") {
		t.Errorf("After PutObjectAcl(public-read), missing AllUsers: %s", respBody)
	}
}

func TestPutObjectAclXMLBody(t *testing.T) {
	h := newTestObjectHandler(t)

	// Put an object.
	body := "acl xml test"
	req := httptest.NewRequest("PUT", "/test-bucket/acl-xml.txt", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()
	h.PutObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject status = %d", rec.Code)
	}

	// Set ACL via XML body.
	aclXML := `<?xml version="1.0" encoding="UTF-8"?>
<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>bleepstore</ID>
    <DisplayName>bleepstore</DisplayName>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
        <ID>bleepstore</ID>
        <DisplayName>bleepstore</DisplayName>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
        <URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
      </Grantee>
      <Permission>READ</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>`

	req = httptest.NewRequest("PUT", "/test-bucket/acl-xml.txt?acl", strings.NewReader(aclXML))
	req.ContentLength = int64(len(aclXML))
	rec = httptest.NewRecorder()
	h.PutObjectAcl(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("PutObjectAcl (XML) status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	// Verify the ACL was updated by reading it back.
	req = httptest.NewRequest("GET", "/test-bucket/acl-xml.txt?acl", nil)
	rec = httptest.NewRecorder()
	h.GetObjectAcl(rec, req)

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "FULL_CONTROL") {
		t.Errorf("After PutObjectAcl (XML), missing FULL_CONTROL: %s", respBody)
	}
	if !strings.Contains(respBody, "READ") {
		t.Errorf("After PutObjectAcl (XML), missing READ grant: %s", respBody)
	}
}

func TestPutObjectAclNoSuchKey(t *testing.T) {
	h := newTestObjectHandler(t)

	req := httptest.NewRequest("PUT", "/test-bucket/nonexistent.txt?acl", nil)
	req.Header.Set("x-amz-acl", "private")
	rec := httptest.NewRecorder()
	h.PutObjectAcl(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("PutObjectAcl (no key) status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if !strings.Contains(rec.Body.String(), "NoSuchKey") {
		t.Errorf("PutObjectAcl body should contain NoSuchKey: %s", rec.Body.String())
	}
}

func TestGetObjectAclNoSuchBucket(t *testing.T) {
	h := newTestObjectHandler(t)

	req := httptest.NewRequest("GET", "/nonexistent-bucket/key.txt?acl", nil)
	rec := httptest.NewRecorder()
	h.GetObjectAcl(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("GetObjectAcl (no bucket) status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if !strings.Contains(rec.Body.String(), "NoSuchBucket") {
		t.Errorf("GetObjectAcl body should contain NoSuchBucket: %s", rec.Body.String())
	}
}
