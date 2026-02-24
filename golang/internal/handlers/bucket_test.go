package handlers

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bleepstore/bleepstore/internal/metadata"
	"github.com/bleepstore/bleepstore/internal/storage"
	"github.com/bleepstore/bleepstore/internal/xmlutil"
)

// newTestBucketHandler creates a BucketHandler backed by real in-memory
// SQLite metadata store and local filesystem storage (temp dir).
func newTestBucketHandler(t *testing.T) *BucketHandler {
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

	return NewBucketHandler(meta, store, "bleepstore", "bleepstore", "us-east-1")
}

func TestValidateBucketName(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		// Valid names
		{"my-bucket", false},
		{"my.bucket", false},
		{"mybucket123", false},
		{"a-b", false},
		{"aaa", false},
		{"bucket-with-many-hyphens-and-dots.and.more", false},

		// Invalid names
		{"ab", true},                    // too short
		{"UPPERCASE", true},             // uppercase
		{"my_bucket", true},             // underscore
		{"-start-with-hyphen", true},    // starts with hyphen
		{"end-with-hyphen-", true},      // ends with hyphen
		{"192.168.0.1", true},           // IP address
		{"xn--test-bucket", true},       // starts with xn--
		{"my-bucket-s3alias", true},     // ends with -s3alias
		{"my-bucket--ol-s3", true},      // ends with --ol-s3
		{"my..bucket", true},            // consecutive periods
		{"", true},                      // empty
		{strings.Repeat("a", 64), true}, // too long (64 chars)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validateBucketName(tt.name)
			if tt.wantErr && result == "" {
				t.Errorf("validateBucketName(%q) = valid, want error", tt.name)
			}
			if !tt.wantErr && result != "" {
				t.Errorf("validateBucketName(%q) = %q, want valid", tt.name, result)
			}
		})
	}
}

func TestCreateBucket(t *testing.T) {
	h := newTestBucketHandler(t)

	req := httptest.NewRequest("PUT", "/my-test-bucket", nil)
	rec := httptest.NewRecorder()
	h.CreateBucket(rec, req)

	if rec.Code != http.StatusOK {
		body, _ := io.ReadAll(rec.Body)
		t.Fatalf("CreateBucket status = %d, want %d; body: %s", rec.Code, http.StatusOK, body)
	}

	location := rec.Header().Get("Location")
	if location != "/my-test-bucket" {
		t.Errorf("Location header = %q, want %q", location, "/my-test-bucket")
	}
}

func TestCreateBucketAlreadyOwnedByYou(t *testing.T) {
	h := newTestBucketHandler(t)

	// Create bucket first.
	req := httptest.NewRequest("PUT", "/my-test-bucket", nil)
	rec := httptest.NewRecorder()
	h.CreateBucket(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("First CreateBucket status = %d, want %d", rec.Code, http.StatusOK)
	}

	// Create again: should return 200 (us-east-1 behavior).
	req = httptest.NewRequest("PUT", "/my-test-bucket", nil)
	rec = httptest.NewRecorder()
	h.CreateBucket(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Second CreateBucket status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestCreateBucketInvalidName(t *testing.T) {
	h := newTestBucketHandler(t)

	tests := []string{"UPPERCASE", "ab", "my_bucket", "192.168.0.1"}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest("PUT", "/"+name, nil)
			rec := httptest.NewRecorder()
			h.CreateBucket(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("CreateBucket(%q) status = %d, want %d", name, rec.Code, http.StatusBadRequest)
			}

			body := rec.Body.String()
			if !strings.Contains(body, "InvalidBucketName") {
				t.Errorf("CreateBucket(%q) body missing InvalidBucketName: %s", name, body)
			}
		})
	}
}

func TestDeleteBucket(t *testing.T) {
	h := newTestBucketHandler(t)

	// Create bucket first.
	req := httptest.NewRequest("PUT", "/my-test-bucket", nil)
	rec := httptest.NewRecorder()
	h.CreateBucket(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("CreateBucket status = %d", rec.Code)
	}

	// Delete it.
	req = httptest.NewRequest("DELETE", "/my-test-bucket", nil)
	rec = httptest.NewRecorder()
	h.DeleteBucket(rec, req)

	if rec.Code != http.StatusNoContent {
		body, _ := io.ReadAll(rec.Body)
		t.Errorf("DeleteBucket status = %d, want %d; body: %s", rec.Code, http.StatusNoContent, body)
	}
}

func TestDeleteBucketNotFound(t *testing.T) {
	h := newTestBucketHandler(t)

	req := httptest.NewRequest("DELETE", "/nonexistent-bucket", nil)
	rec := httptest.NewRecorder()
	h.DeleteBucket(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("DeleteBucket status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "NoSuchBucket") {
		t.Errorf("expected NoSuchBucket error, got: %s", body)
	}
}

func TestHeadBucket(t *testing.T) {
	h := newTestBucketHandler(t)

	// Create bucket first.
	req := httptest.NewRequest("PUT", "/my-test-bucket", nil)
	rec := httptest.NewRecorder()
	h.CreateBucket(rec, req)

	// Head the bucket.
	req = httptest.NewRequest("HEAD", "/my-test-bucket", nil)
	rec = httptest.NewRecorder()
	h.HeadBucket(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("HeadBucket status = %d, want %d", rec.Code, http.StatusOK)
	}

	region := rec.Header().Get("x-amz-bucket-region")
	if region != "us-east-1" {
		t.Errorf("x-amz-bucket-region = %q, want %q", region, "us-east-1")
	}
}

func TestHeadBucketNotFound(t *testing.T) {
	h := newTestBucketHandler(t)

	req := httptest.NewRequest("HEAD", "/nonexistent-bucket", nil)
	rec := httptest.NewRecorder()
	h.HeadBucket(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("HeadBucket status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestListBuckets(t *testing.T) {
	h := newTestBucketHandler(t)

	// Create a couple of buckets.
	for _, name := range []string{"alpha-bucket", "beta-bucket"} {
		req := httptest.NewRequest("PUT", "/"+name, nil)
		rec := httptest.NewRecorder()
		h.CreateBucket(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("CreateBucket(%q) failed: %d", name, rec.Code)
		}
	}

	// List buckets.
	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	h.ListBuckets(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ListBuckets status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.Bytes()

	// Parse the XML response.
	var result xmlutil.ListAllMyBucketsResult
	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatalf("Failed to parse ListBuckets XML: %v\nBody: %s", err, body)
	}

	if result.Owner.ID != "bleepstore" {
		t.Errorf("Owner.ID = %q, want %q", result.Owner.ID, "bleepstore")
	}

	if len(result.Buckets) != 2 {
		t.Fatalf("len(Buckets) = %d, want 2", len(result.Buckets))
	}

	// Buckets should be sorted by name.
	if result.Buckets[0].Name != "alpha-bucket" {
		t.Errorf("Buckets[0].Name = %q, want %q", result.Buckets[0].Name, "alpha-bucket")
	}
	if result.Buckets[1].Name != "beta-bucket" {
		t.Errorf("Buckets[1].Name = %q, want %q", result.Buckets[1].Name, "beta-bucket")
	}

	// Each bucket should have a CreationDate.
	for i, b := range result.Buckets {
		if b.CreationDate == "" {
			t.Errorf("Buckets[%d].CreationDate is empty", i)
		}
	}

	// Verify xmlns in the XML body.
	bodyStr := string(body)
	if !strings.Contains(bodyStr, `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`) {
		t.Errorf("ListBuckets XML missing xmlns: %s", bodyStr)
	}
}

func TestGetBucketLocation(t *testing.T) {
	h := newTestBucketHandler(t)

	// Create a bucket.
	req := httptest.NewRequest("PUT", "/my-test-bucket", nil)
	rec := httptest.NewRecorder()
	h.CreateBucket(rec, req)

	// Get location (us-east-1 should return empty LocationConstraint).
	req = httptest.NewRequest("GET", "/my-test-bucket?location", nil)
	rec = httptest.NewRecorder()
	h.GetBucketLocation(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GetBucketLocation status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	// For us-east-1, the location value should be empty.
	var loc xmlutil.LocationConstraint
	if err := xml.Unmarshal([]byte(body), &loc); err != nil {
		t.Fatalf("Failed to parse LocationConstraint XML: %v", err)
	}
	if loc.Location != "" {
		t.Errorf("Location = %q, want empty string for us-east-1", loc.Location)
	}
}

func TestGetBucketLocationNotFound(t *testing.T) {
	h := newTestBucketHandler(t)

	req := httptest.NewRequest("GET", "/nonexistent-bucket?location", nil)
	rec := httptest.NewRecorder()
	h.GetBucketLocation(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("GetBucketLocation status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestGetBucketAcl(t *testing.T) {
	h := newTestBucketHandler(t)

	// Create a bucket.
	req := httptest.NewRequest("PUT", "/my-test-bucket", nil)
	rec := httptest.NewRecorder()
	h.CreateBucket(rec, req)

	// Get ACL.
	req = httptest.NewRequest("GET", "/my-test-bucket?acl", nil)
	rec = httptest.NewRecorder()
	h.GetBucketAcl(rec, req)

	if rec.Code != http.StatusOK {
		body, _ := io.ReadAll(rec.Body)
		t.Fatalf("GetBucketAcl status = %d, want %d; body: %s", rec.Code, http.StatusOK, body)
	}

	body := rec.Body.String()

	// Verify the response contains expected elements.
	if !strings.Contains(body, "FULL_CONTROL") {
		t.Errorf("GetBucketAcl missing FULL_CONTROL: %s", body)
	}
	if !strings.Contains(body, "bleepstore") {
		t.Errorf("GetBucketAcl missing owner ID: %s", body)
	}
	if !strings.Contains(body, "xmlns:xsi") {
		t.Errorf("GetBucketAcl missing xmlns:xsi: %s", body)
	}
	if !strings.Contains(body, `xsi:type="CanonicalUser"`) {
		t.Errorf("GetBucketAcl missing xsi:type: %s", body)
	}
}

func TestPutBucketAclCanned(t *testing.T) {
	h := newTestBucketHandler(t)

	// Create a bucket.
	req := httptest.NewRequest("PUT", "/my-test-bucket", nil)
	rec := httptest.NewRecorder()
	h.CreateBucket(rec, req)

	// Set canned ACL to public-read.
	req = httptest.NewRequest("PUT", "/my-test-bucket?acl", nil)
	req.Header.Set("x-amz-acl", "public-read")
	rec = httptest.NewRecorder()
	h.PutBucketAcl(rec, req)

	if rec.Code != http.StatusOK {
		body, _ := io.ReadAll(rec.Body)
		t.Fatalf("PutBucketAcl status = %d, want %d; body: %s", rec.Code, http.StatusOK, body)
	}

	// Verify the ACL was updated by reading it back.
	req = httptest.NewRequest("GET", "/my-test-bucket?acl", nil)
	rec = httptest.NewRecorder()
	h.GetBucketAcl(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "READ") {
		t.Errorf("After PutBucketAcl(public-read), missing READ grant: %s", body)
	}
}

func TestParseCannedACL(t *testing.T) {
	tests := []struct {
		cannedACL  string
		wantGrants int
		wantPerms  []string
	}{
		{"private", 1, []string{"FULL_CONTROL"}},
		{"public-read", 2, []string{"FULL_CONTROL", "READ"}},
		{"public-read-write", 3, []string{"FULL_CONTROL", "READ", "WRITE"}},
		{"authenticated-read", 2, []string{"FULL_CONTROL", "READ"}},
	}

	for _, tt := range tests {
		t.Run(tt.cannedACL, func(t *testing.T) {
			acp := parseCannedACL(tt.cannedACL, "owner-id", "owner-display")

			if len(acp.AccessControlList.Grants) != tt.wantGrants {
				t.Errorf("grants count = %d, want %d", len(acp.AccessControlList.Grants), tt.wantGrants)
			}

			var perms []string
			for _, g := range acp.AccessControlList.Grants {
				perms = append(perms, g.Permission)
			}

			for _, p := range tt.wantPerms {
				found := false
				for _, got := range perms {
					if got == p {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected permission %q not found in %v", p, perms)
				}
			}
		})
	}
}
