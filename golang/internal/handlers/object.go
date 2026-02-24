// Package handlers implements HTTP request handlers for S3-compatible API operations.
package handlers

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	s3err "github.com/bleepstore/bleepstore/internal/errors"
	"github.com/bleepstore/bleepstore/internal/metadata"
	"github.com/bleepstore/bleepstore/internal/storage"
	"github.com/bleepstore/bleepstore/internal/xmlutil"
)

// ObjectHandler contains handlers for S3 object-level operations.
type ObjectHandler struct {
	meta          metadata.MetadataStore
	store         storage.StorageBackend
	ownerID       string
	ownerDisplay  string
	maxObjectSize int64
}

// NewObjectHandler creates a new ObjectHandler with the given dependencies.
func NewObjectHandler(meta metadata.MetadataStore, store storage.StorageBackend, ownerID, ownerDisplay string, maxObjectSize int64) *ObjectHandler {
	return &ObjectHandler{
		meta:          meta,
		store:         store,
		ownerID:       ownerID,
		ownerDisplay:  ownerDisplay,
		maxObjectSize: maxObjectSize,
	}
}

// PutObject handles PUT /{bucket}/{object} and stores an object in the
// specified bucket. Follows crash-only design: writes to temp file, fsyncs,
// renames atomically, then commits metadata. Never acknowledges before commit.
func (h *ObjectHandler) PutObject(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil || h.store == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)
	key := extractObjectKey(r)

	if key == "" {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInvalidArgument)
		return
	}

	// Validate key length (max 1024 bytes per S3 spec).
	if len(key) > 1024 {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrKeyTooLongError)
		return
	}

	// Enforce max object size.
	if h.maxObjectSize > 0 && r.ContentLength > 0 && r.ContentLength > h.maxObjectSize {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrEntityTooLarge)
		return
	}

	// Verify bucket exists.
	bucket, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("PutObject GetBucket error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if bucket == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	// Extract content type, defaulting to application/octet-stream.
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Extract user metadata (x-amz-meta-* headers).
	userMeta := extractUserMetadata(r)

	// Extract optional content headers.
	contentEncoding := r.Header.Get("Content-Encoding")
	contentLanguage := r.Header.Get("Content-Language")
	contentDisposition := r.Header.Get("Content-Disposition")
	cacheControl := r.Header.Get("Cache-Control")
	expires := r.Header.Get("Expires")

	// Extract optional canned ACL.
	cannedACL := r.Header.Get("x-amz-acl")
	var aclJSON json.RawMessage
	if cannedACL != "" {
		acp := parseCannedACL(cannedACL, h.ownerID, h.ownerDisplay)
		aclJSON = aclToJSON(acp)
	} else {
		aclJSON = defaultPrivateACL(h.ownerID, h.ownerDisplay)
	}

	// Write object data to storage backend (atomic: temp-fsync-rename).
	bytesWritten, etag, err := h.store.PutObject(ctx, bucketName, key, r.Body, r.ContentLength)
	if err != nil {
		slog.Error("PutObject storage error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Commit metadata to SQLite.
	now := time.Now().UTC()
	objRecord := &metadata.ObjectRecord{
		Bucket:             bucketName,
		Key:                key,
		Size:               bytesWritten,
		ETag:               etag,
		ContentType:        contentType,
		ContentEncoding:    contentEncoding,
		ContentLanguage:    contentLanguage,
		ContentDisposition: contentDisposition,
		CacheControl:       cacheControl,
		Expires:            expires,
		StorageClass:       "STANDARD",
		ACL:                aclJSON,
		UserMetadata:       userMeta,
		LastModified:       now,
	}

	if err := h.meta.PutObject(ctx, objRecord); err != nil {
		slog.Error("PutObject metadata error", "error", err)
		// Storage write succeeded but metadata failed. The orphan file on disk
		// is safe (crash-only: storage is the data, metadata is the index).
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Success: set response headers and return 200.
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

// GetObject handles GET /{bucket}/{object} and retrieves the object data
// and metadata from the specified bucket. Supports range requests (Range header)
// and conditional requests (If-Match, If-None-Match, If-Modified-Since,
// If-Unmodified-Since).
func (h *ObjectHandler) GetObject(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil || h.store == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)
	key := extractObjectKey(r)

	// Verify bucket exists.
	bucket, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("GetObject GetBucket error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if bucket == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	// Get object metadata.
	objMeta, err := h.meta.GetObject(ctx, bucketName, key)
	if err != nil {
		slog.Error("GetObject metadata error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if objMeta == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
		return
	}

	// Evaluate conditional request headers before opening data.
	if statusCode, skip := checkConditionalHeaders(r, objMeta.ETag, objMeta.LastModified); skip {
		// Set ETag and Last-Modified even on 304/412 responses.
		w.Header().Set("ETag", objMeta.ETag)
		w.Header().Set("Last-Modified", xmlutil.FormatTimeHTTP(objMeta.LastModified))
		if statusCode == http.StatusNotModified {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		xmlutil.WriteErrorResponse(w, r, s3err.ErrPreconditionFailed)
		return
	}

	// Open object data from storage.
	reader, _, _, err := h.store.GetObject(ctx, bucketName, key)
	if err != nil {
		slog.Error("GetObject storage error", "error", err)
		// Metadata exists but file is missing: log error, return 500.
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	defer reader.Close()

	// Check for range request.
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		start, end, rangeErr := parseRange(rangeHeader, objMeta.Size)
		if rangeErr != nil {
			// 416 Range Not Satisfiable.
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", objMeta.Size))
			xmlutil.WriteErrorResponse(w, r, s3err.ErrInvalidRange)
			return
		}

		// Seek to the start position.
		if seeker, ok := reader.(io.ReadSeeker); ok {
			if _, seekErr := seeker.Seek(start, io.SeekStart); seekErr != nil {
				slog.Error("GetObject seek error", "error", seekErr)
				xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
				return
			}
		} else {
			// Fall back to discarding bytes.
			if _, discardErr := io.CopyN(io.Discard, reader, start); discardErr != nil {
				slog.Error("GetObject discard error", "error", discardErr)
				xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
				return
			}
		}

		rangeLen := end - start + 1

		// Set response headers for partial content.
		setObjectResponseHeaders(w, objMeta)
		w.Header().Set("Content-Length", strconv.FormatInt(rangeLen, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, objMeta.Size))
		w.WriteHeader(http.StatusPartialContent)

		// Stream the requested range.
		io.CopyN(w, reader, rangeLen)
		return
	}

	// Full object response.
	setObjectResponseHeaders(w, objMeta)
	w.WriteHeader(http.StatusOK)

	// Stream object data to the client.
	io.Copy(w, reader)
}

// HeadObject handles HEAD /{bucket}/{object} and returns the object metadata
// without the object body. Supports conditional requests (If-Match,
// If-None-Match, If-Modified-Since, If-Unmodified-Since).
func (h *ObjectHandler) HeadObject(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil || h.store == nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)
	key := extractObjectKey(r)

	// Verify bucket exists.
	bucket, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("HeadObject GetBucket error", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if bucket == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Get object metadata.
	objMeta, err := h.meta.GetObject(ctx, bucketName, key)
	if err != nil {
		slog.Error("HeadObject metadata error", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if objMeta == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Evaluate conditional request headers.
	if statusCode, skip := checkConditionalHeaders(r, objMeta.ETag, objMeta.LastModified); skip {
		// Set ETag and Last-Modified even on 304/412 responses.
		w.Header().Set("ETag", objMeta.ETag)
		w.Header().Set("Last-Modified", xmlutil.FormatTimeHTTP(objMeta.LastModified))
		w.WriteHeader(statusCode)
		return
	}

	// Set response headers from metadata (includes Content-Length, ETag, etc.).
	setObjectResponseHeaders(w, objMeta)

	w.WriteHeader(http.StatusOK)
}

// DeleteObject handles DELETE /{bucket}/{object} and removes the specified
// object from the bucket. Idempotent: deleting a non-existent object returns 204.
func (h *ObjectHandler) DeleteObject(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil || h.store == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)
	key := extractObjectKey(r)

	// Verify bucket exists.
	bucket, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("DeleteObject GetBucket error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if bucket == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	// Delete metadata first (the authoritative record).
	if err := h.meta.DeleteObject(ctx, bucketName, key); err != nil {
		slog.Error("DeleteObject metadata error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Delete the file from storage (best-effort; orphan files are safe).
	if err := h.store.DeleteObject(ctx, bucketName, key); err != nil {
		slog.Error("DeleteObject storage error", "error", err)
		// Don't fail the request -- metadata is already deleted.
	}

	// S3 always returns 204 for DeleteObject, even if the key didn't exist.
	w.WriteHeader(http.StatusNoContent)
}

// DeleteObjects handles POST /{bucket}?delete and performs a multi-object
// delete operation. The request body contains an XML list of keys to delete.
func (h *ObjectHandler) DeleteObjects(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil || h.store == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)

	// Verify bucket exists.
	bucket, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("DeleteObjects GetBucket error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if bucket == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	// Parse the Delete XML request body.
	deleteReq, err := parseDeleteRequest(r.Body)
	if err != nil {
		slog.Error("DeleteObjects XML parse error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	result := &xmlutil.DeleteResult{}

	// Collect all keys for batch metadata delete.
	allKeys := make([]string, len(deleteReq.Objects))
	for i, obj := range deleteReq.Objects {
		allKeys[i] = obj.Key
	}

	// Batch delete metadata (authoritative record).
	deleted, errs := h.meta.DeleteObjectsMeta(ctx, bucketName, allKeys)
	if len(errs) > 0 {
		for _, e := range errs {
			slog.Error("DeleteObjects metadata batch error", "error", e)
		}
		// On batch error, report all keys as errors.
		for _, obj := range deleteReq.Objects {
			result.Errors = append(result.Errors, xmlutil.DeleteError{
				Key:     obj.Key,
				Code:    "InternalError",
				Message: "We encountered an internal error. Please try again.",
			})
		}
		xmlutil.RenderDeleteResult(w, result)
		return
	}

	// Delete files from storage (best-effort, per-key).
	for _, key := range deleted {
		if err := h.store.DeleteObject(ctx, bucketName, key); err != nil {
			slog.Error("DeleteObjects storage error", "key", key, "error", err)
		}
	}

	// Report successful deletes (unless quiet mode).
	if !deleteReq.Quiet {
		for _, key := range deleted {
			result.Deleted = append(result.Deleted, xmlutil.DeletedItem{Key: key})
		}
	}

	xmlutil.RenderDeleteResult(w, result)
}

// CopyObject handles PUT /{bucket}/{object} with an X-Amz-Copy-Source header,
// copying an object from one location to another. Supports x-amz-metadata-directive:
// COPY (default, copy source metadata) or REPLACE (use request headers).
func (h *ObjectHandler) CopyObject(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil || h.store == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()
	dstBucket := extractBucketName(r)
	dstKey := extractObjectKey(r)

	if dstKey == "" {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInvalidArgument)
		return
	}

	// Parse the X-Amz-Copy-Source header.
	copySource := r.Header.Get("X-Amz-Copy-Source")
	srcBucket, srcKey, ok := parseCopySource(copySource)
	if !ok {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInvalidArgument)
		return
	}

	// Verify destination bucket exists.
	dstBucketRec, err := h.meta.GetBucket(ctx, dstBucket)
	if err != nil {
		slog.Error("CopyObject GetBucket (dst) error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if dstBucketRec == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	// Verify source bucket exists.
	srcBucketRec, err := h.meta.GetBucket(ctx, srcBucket)
	if err != nil {
		slog.Error("CopyObject GetBucket (src) error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if srcBucketRec == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	// Get source object metadata.
	srcObj, err := h.meta.GetObject(ctx, srcBucket, srcKey)
	if err != nil {
		slog.Error("CopyObject GetObject (src) error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if srcObj == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
		return
	}

	// Copy file data via storage backend (atomic).
	newETag, err := h.store.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey)
	if err != nil {
		slog.Error("CopyObject storage error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Determine metadata directive: COPY (default) or REPLACE.
	directive := strings.ToUpper(r.Header.Get("x-amz-metadata-directive"))
	if directive == "" {
		directive = "COPY"
	}

	now := time.Now().UTC()
	var dstObj *metadata.ObjectRecord

	if directive == "REPLACE" {
		// Use request headers for metadata.
		contentType := r.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		userMeta := extractUserMetadata(r)

		cannedACL := r.Header.Get("x-amz-acl")
		var aclJSON json.RawMessage
		if cannedACL != "" {
			acp := parseCannedACL(cannedACL, h.ownerID, h.ownerDisplay)
			aclJSON = aclToJSON(acp)
		} else {
			aclJSON = defaultPrivateACL(h.ownerID, h.ownerDisplay)
		}

		dstObj = &metadata.ObjectRecord{
			Bucket:             dstBucket,
			Key:                dstKey,
			Size:               srcObj.Size,
			ETag:               newETag,
			ContentType:        contentType,
			ContentEncoding:    r.Header.Get("Content-Encoding"),
			ContentLanguage:    r.Header.Get("Content-Language"),
			ContentDisposition: r.Header.Get("Content-Disposition"),
			CacheControl:       r.Header.Get("Cache-Control"),
			Expires:            r.Header.Get("Expires"),
			StorageClass:       "STANDARD",
			ACL:                aclJSON,
			UserMetadata:       userMeta,
			LastModified:       now,
		}
	} else {
		// COPY: duplicate source metadata to destination.
		dstObj = &metadata.ObjectRecord{
			Bucket:             dstBucket,
			Key:                dstKey,
			Size:               srcObj.Size,
			ETag:               newETag,
			ContentType:        srcObj.ContentType,
			ContentEncoding:    srcObj.ContentEncoding,
			ContentLanguage:    srcObj.ContentLanguage,
			ContentDisposition: srcObj.ContentDisposition,
			CacheControl:       srcObj.CacheControl,
			Expires:            srcObj.Expires,
			StorageClass:       srcObj.StorageClass,
			ACL:                srcObj.ACL,
			UserMetadata:       srcObj.UserMetadata,
			LastModified:       now,
		}
	}

	// Commit metadata for the destination object.
	if err := h.meta.PutObject(ctx, dstObj); err != nil {
		slog.Error("CopyObject metadata error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Return CopyObjectResult XML.
	result := &xmlutil.CopyObjectResult{
		LastModified: xmlutil.FormatTimeS3(now),
		ETag:         newETag,
	}
	xmlutil.RenderCopyObject(w, result)
}

// ListObjectsV2 handles GET /{bucket}?list-type=2 and returns a listing of
// objects in the bucket using the V2 API format.
func (h *ObjectHandler) ListObjectsV2(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil || h.store == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)
	q := r.URL.Query()

	// Verify bucket exists.
	bucket, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("ListObjectsV2 GetBucket error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if bucket == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	// Parse query parameters.
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	startAfter := q.Get("start-after")
	continuationToken := q.Get("continuation-token")
	encodingType := q.Get("encoding-type")

	maxKeys := 1000 // Default
	if mk := q.Get("max-keys"); mk != "" {
		if parsed, err := strconv.Atoi(mk); err == nil && parsed >= 0 {
			maxKeys = parsed
		}
	}

	opts := metadata.ListObjectsOptions{
		Prefix:            prefix,
		Delimiter:         delimiter,
		StartAfter:        startAfter,
		ContinuationToken: continuationToken,
		MaxKeys:           maxKeys,
	}

	listResult, err := h.meta.ListObjects(ctx, bucketName, opts)
	if err != nil {
		slog.Error("ListObjectsV2 ListObjects error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Build XML response.
	result := &xmlutil.ListBucketV2Result{
		Name:         bucketName,
		Prefix:       prefix,
		MaxKeys:      maxKeys,
		KeyCount:     len(listResult.Objects),
		IsTruncated:  listResult.IsTruncated,
		EncodingType: encodingType,
	}

	if delimiter != "" {
		result.Delimiter = delimiter
	}

	if startAfter != "" {
		result.StartAfter = startAfter
	}

	if continuationToken != "" {
		result.ContinuationToken = continuationToken
	}

	if listResult.IsTruncated && listResult.NextContinuationToken != "" {
		result.NextContinuationToken = listResult.NextContinuationToken
	}

	// Convert objects to XML Objects.
	for _, obj := range listResult.Objects {
		result.Contents = append(result.Contents, xmlutil.Object{
			Key:          obj.Key,
			LastModified: xmlutil.FormatTimeS3(obj.LastModified),
			ETag:         obj.ETag,
			Size:         obj.Size,
			StorageClass: obj.StorageClass,
		})
	}

	// Convert common prefixes.
	for _, cp := range listResult.CommonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, xmlutil.CommonPrefix{
			Prefix: cp,
		})
	}

	xmlutil.RenderListObjectsV2(w, result)
}

// ListObjects handles GET /{bucket} and returns a listing of objects in the
// bucket using the V1 API format.
func (h *ObjectHandler) ListObjects(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil || h.store == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)
	q := r.URL.Query()

	// Verify bucket exists.
	bucket, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("ListObjects GetBucket error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if bucket == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	// Parse query parameters.
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	marker := q.Get("marker")

	maxKeys := 1000 // Default
	if mk := q.Get("max-keys"); mk != "" {
		if parsed, err := strconv.Atoi(mk); err == nil && parsed >= 0 {
			maxKeys = parsed
		}
	}

	opts := metadata.ListObjectsOptions{
		Prefix:    prefix,
		Delimiter: delimiter,
		Marker:    marker,
		MaxKeys:   maxKeys,
	}

	listResult, err := h.meta.ListObjects(ctx, bucketName, opts)
	if err != nil {
		slog.Error("ListObjects ListObjects error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Build XML response.
	result := &xmlutil.ListBucketResult{
		Name:        bucketName,
		Prefix:      prefix,
		Marker:      marker,
		MaxKeys:     maxKeys,
		IsTruncated: listResult.IsTruncated,
	}

	if delimiter != "" {
		result.Delimiter = delimiter
	}

	if listResult.IsTruncated && listResult.NextMarker != "" {
		result.NextMarker = listResult.NextMarker
	}

	// Convert objects to XML Objects.
	for _, obj := range listResult.Objects {
		result.Contents = append(result.Contents, xmlutil.Object{
			Key:          obj.Key,
			LastModified: xmlutil.FormatTimeS3(obj.LastModified),
			ETag:         obj.ETag,
			Size:         obj.Size,
			StorageClass: obj.StorageClass,
		})
	}

	// Convert common prefixes.
	for _, cp := range listResult.CommonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, xmlutil.CommonPrefix{
			Prefix: cp,
		})
	}

	xmlutil.RenderListObjects(w, result)
}

// GetObjectAcl handles GET /{bucket}/{object}?acl and returns the access
// control list for the specified object.
func (h *ObjectHandler) GetObjectAcl(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)
	key := extractObjectKey(r)

	// Verify bucket exists.
	bucket, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("GetObjectAcl GetBucket error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if bucket == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	// Get object metadata.
	objMeta, err := h.meta.GetObject(ctx, bucketName, key)
	if err != nil {
		slog.Error("GetObjectAcl GetObject error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if objMeta == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
		return
	}

	// Parse ACL from stored JSON.
	acp := aclFromJSON(objMeta.ACL)
	if acp == nil {
		// No ACL stored: return default private ACL.
		acp = parseCannedACL("private", h.ownerID, h.ownerDisplay)
	}

	// Ensure Owner is set correctly.
	acp.Owner = xmlutil.Owner{
		ID:          h.ownerID,
		DisplayName: h.ownerDisplay,
	}

	xmlutil.RenderAccessControlPolicy(w, acp)
}

// PutObjectAcl handles PUT /{bucket}/{object}?acl and sets the access
// control list for the specified object.
func (h *ObjectHandler) PutObjectAcl(w http.ResponseWriter, r *http.Request) {
	if h.meta == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	ctx := r.Context()
	bucketName := extractBucketName(r)
	key := extractObjectKey(r)

	// Verify bucket exists.
	bucket, err := h.meta.GetBucket(ctx, bucketName)
	if err != nil {
		slog.Error("PutObjectAcl GetBucket error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if bucket == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	// Verify object exists.
	objMeta, err := h.meta.GetObject(ctx, bucketName, key)
	if err != nil {
		slog.Error("PutObjectAcl GetObject error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if objMeta == nil {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
		return
	}

	var acp *xmlutil.AccessControlPolicy

	// Three mutually exclusive modes:
	// 1. Canned ACL via x-amz-acl header
	// 2. Explicit grants via x-amz-grant-* headers (not yet implemented)
	// 3. XML body
	cannedACL := r.Header.Get("x-amz-acl")
	if cannedACL != "" {
		// Mode 1: Canned ACL.
		acp = parseCannedACL(cannedACL, h.ownerID, h.ownerDisplay)
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
		acp = parseCannedACL("private", h.ownerID, h.ownerDisplay)
	}

	// Store the ACL.
	aclJSON := aclToJSON(acp)
	if err := h.meta.UpdateObjectAcl(ctx, bucketName, key, aclJSON); err != nil {
		slog.Error("PutObjectAcl update error", "error", err)
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// extractObjectKey extracts the object key from the request URL path.
// The key is everything after the bucket name in the path.
func extractObjectKey(r *http.Request) string {
	path := r.URL.Path
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	// Find the first slash to separate bucket from key.
	idx := strings.IndexByte(path, '/')
	if idx < 0 {
		return ""
	}
	return path[idx+1:]
}
