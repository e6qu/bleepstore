// Package handlers implements HTTP request handlers for S3-compatible API operations.
package handlers

import (
	"context"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	s3err "github.com/bleepstore/bleepstore/internal/errors"
	"github.com/bleepstore/bleepstore/internal/metadata"
	"github.com/bleepstore/bleepstore/internal/storage"
	"github.com/bleepstore/bleepstore/internal/xmlutil"
)

// BucketHandler contains handlers for S3 bucket-level operations.
type BucketHandler struct {
	meta         metadata.MetadataStore
	store        storage.StorageBackend
	ownerID      string
	ownerDisplay string
	region       string
}

// NewBucketHandler creates a new BucketHandler with the given dependencies.
func NewBucketHandler(meta metadata.MetadataStore, store storage.StorageBackend, ownerID, ownerDisplay, region string) *BucketHandler {
	return &BucketHandler{
		meta:         meta,
		store:        store,
		ownerID:      ownerID,
		ownerDisplay: ownerDisplay,
		region:       region,
	}
}

// ListBuckets handles GET / and returns a list of all buckets owned by the
// authenticated sender of the request.
func (h *BucketHandler) ListBuckets(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()

	buckets, err := h.meta.ListBuckets(ctx, h.ownerID)
	if err != nil {
		slog.Error("ListBuckets error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	var xmlBuckets []xmlutil.Bucket
	for _, b := range buckets {
		xmlBuckets = append(xmlBuckets, xmlutil.Bucket{
			Name:         b.Name,
			CreationDate: xmlutil.FormatTimeS3(b.CreatedAt),
		})
	}

	result := &xmlutil.ListAllMyBucketsResult{
		Owner: xmlutil.Owner{
			ID:          h.ownerID,
			DisplayName: h.ownerDisplay,
		},
		Buckets: xmlBuckets,
	}

	xmlutil.RenderListBuckets(w, result)
}

// CreateBucket handles PUT /{bucket} and creates a new bucket with the
// specified name.
func (h *BucketHandler) CreateBucket(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil || h.store == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)

	// Validate bucket name.
	if errMsg := validateBucketName(bucketName); errMsg != "" {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInvalidBucketName)
		return
	}

	// Parse optional canned ACL from header.
	cannedACL := r.Header.Get("x-amz-acl")

	// Build ACL: if canned ACL specified, use it; otherwise default to private.
	acp := parseCannedACL(cannedACL, h.ownerID, h.ownerDisplay)
	aclJSON := aclToJSON(acp)

	// Determine region from request body (CreateBucketConfiguration) or config.
	region := h.region
	if r.ContentLength > 0 || r.Header.Get("Content-Length") != "" {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MB max
		if err == nil && len(body) > 0 {
			region = parseCreateBucketRegion(body, h.region)
		}
	}

	// Check if bucket already exists.
	existing, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("CreateBucket GetBucket error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if existing != nil {
		// Bucket already exists.
		if existing.OwnerID == h.ownerID {
			// us-east-1 behavior: return 200 OK (BucketAlreadyOwnedByYou).
			w.Header().Set("Location", "/"+bucketName)
			w.WriteHeader(http.StatusOK)
			return
		}
		// Bucket owned by someone else.
		xmlutil.WriteErrorResponse(w, r, s3err.ErrBucketAlreadyExists)
		return
	}

	// Create bucket record in metadata store.
	record := &metadata.BucketRecord{
		Name:         bucketName,
		Region:       region,
		OwnerID:      h.ownerID,
		OwnerDisplay: h.ownerDisplay,
		ACL:          aclJSON,
		CreatedAt:    time.Now().UTC(),
	}

	if err := h.meta.CreateBucket(ctx, record); err != nil {
		// Handle race condition: bucket was created between our check and insert.
		if strings.Contains(err.Error(), "already exists") {
			w.Header().Set("Location", "/"+bucketName)
			w.WriteHeader(http.StatusOK)
			return
		}
		slog.Error("CreateBucket metadata error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Create the bucket directory in the storage backend.
	if err := h.store.CreateBucket(ctx, bucketName); err != nil {
		slog.Error("CreateBucket storage error", "error", err)
		// Best effort: metadata is created, storage directory failed.
		// Log but don't fail -- the directory will be created on first object write.
	}

	w.Header().Set("Location", "/"+bucketName)
	w.WriteHeader(http.StatusOK)
}

// DeleteBucket handles DELETE /{bucket} and removes the specified bucket.
// The bucket must be empty before it can be deleted.
func (h *BucketHandler) DeleteBucket(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil || h.store == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)

	// Delete from metadata store (validates existence and emptiness).
	if err := h.meta.DeleteBucket(ctx, bucketName); err != nil {
		if strings.Contains(err.Error(), "not found") {
			xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		if strings.Contains(err.Error(), "not empty") {
			xmlutil.WriteErrorResponse(w, r, s3err.ErrBucketNotEmpty)
			return
		}
		slog.Error("DeleteBucket error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Remove bucket directory from storage backend (best effort).
	if err := h.store.DeleteBucket(ctx, bucketName); err != nil {
		slog.Error("DeleteBucket storage cleanup error", "error", err)
	}

	w.WriteHeader(http.StatusNoContent)
}

// HeadBucket handles HEAD /{bucket} and checks whether the specified bucket
// exists and is accessible.
func (h *BucketHandler) HeadBucket(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)

	bucket, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("HeadBucket error", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if bucket == nil {
		// HEAD requests: no body, status code only.
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("x-amz-bucket-region", bucket.Region)
	w.WriteHeader(http.StatusOK)
}

// GetBucketLocation handles GET /{bucket}?location and returns the region
// constraint for the specified bucket.
func (h *BucketHandler) GetBucketLocation(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)

	bucket, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("GetBucketLocation error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if bucket == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	// us-east-1 quirk: return empty LocationConstraint (effectively null).
	location := bucket.Region
	if location == "us-east-1" {
		location = ""
	}

	xmlutil.RenderLocationConstraint(w, location)
}

// GetBucketAcl handles GET /{bucket}?acl and returns the access control list
// for the specified bucket.
func (h *BucketHandler) GetBucketAcl(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)

	bucket, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("GetBucketAcl error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if bucket == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	// Parse ACL from stored JSON.
	acp := aclFromJSON(bucket.ACL)
	if acp == nil {
		// No ACL stored: return default private ACL.
		acp = parseCannedACL("private", bucket.OwnerID, bucket.OwnerDisplay)
	}

	// Ensure Owner is set correctly.
	acp.Owner = xmlutil.Owner{
		ID:          bucket.OwnerID,
		DisplayName: bucket.OwnerDisplay,
	}

	xmlutil.RenderAccessControlPolicy(w, acp)
}

// PutBucketAcl handles PUT /{bucket}?acl and sets the access control list
// for the specified bucket.
func (h *BucketHandler) PutBucketAcl(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)

	// Verify bucket exists.
	bucket, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("PutBucketAcl error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if bucket == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	var acp *xmlutil.AccessControlPolicy

	// Three mutually exclusive modes:
	// 1. Canned ACL via x-amz-acl header
	// 2. Explicit grants via x-amz-grant-* headers
	// 3. XML body
	cannedACL := r.Header.Get("x-amz-acl")
	if cannedACL != "" {
		// Mode 1: Canned ACL.
		acp = parseCannedACL(cannedACL, bucket.OwnerID, bucket.OwnerDisplay)
	} else if r.ContentLength > 0 {
		// Mode 3: XML body.
		body, readErr := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MB max
		if readErr != nil {
			xmlutil.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
			return
		}
		acp = &xmlutil.AccessControlPolicy{}
		if xmlErr := xml.Unmarshal(body, acp); xmlErr != nil {
			xmlutil.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
			return
		}
	} else {
		// No canned ACL and no body: default to private.
		acp = parseCannedACL("private", bucket.OwnerID, bucket.OwnerDisplay)
	}

	// Store the ACL.
	aclJSON := aclToJSON(acp)
	if err := h.meta.UpdateBucketAcl(ctx, bucketName, aclJSON); err != nil {
		slog.Error("PutBucketAcl update error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// parseCreateBucketRegion parses a CreateBucketConfiguration XML body to
// extract the LocationConstraint value. Returns the default region if
// parsing fails or no LocationConstraint is specified.
func parseCreateBucketRegion(body []byte, defaultRegion string) string {
	type createBucketConfig struct {
		XMLName            xml.Name `xml:"CreateBucketConfiguration"`
		LocationConstraint string   `xml:"LocationConstraint"`
	}
	var config createBucketConfig
	if err := xml.Unmarshal(body, &config); err != nil {
		return defaultRegion
	}
	if config.LocationConstraint == "" {
		return defaultRegion
	}
	return config.LocationConstraint
}

// ensureBucketExists is a helper that checks for bucket existence and writes
// the appropriate error response if it does not exist. Returns the bucket
// record if found, nil otherwise.
func (h *BucketHandler) ensureBucketExists(w http.ResponseWriter, r *http.Request, ctx context.Context, bucketName string) *metadata.BucketRecord {
	bucket, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("ensureBucketExists error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return nil
	}
	if bucket == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return nil
	}
	return bucket
}
