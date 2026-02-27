// Package handlers provides shared helper utilities for S3 operation handlers.
package handlers

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	s3err "github.com/bleepstore/bleepstore/internal/errors"
	"github.com/bleepstore/bleepstore/internal/metadata"
	"github.com/bleepstore/bleepstore/internal/xmlutil"
)

// bucketNameRegex validates bucket names per S3 naming rules:
// - 3-63 characters
// - Lowercase letters, numbers, hyphens, and periods only
// - Must begin and end with a letter or number
var bucketNameRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$`)

// ipAddressRegex detects IP address-formatted bucket names.
var ipAddressRegex = regexp.MustCompile(`^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$`)

// validateBucketName checks whether the given name is a valid S3 bucket name.
// Returns an error message string if invalid, or empty string if valid.
func validateBucketName(name string) string {
	if len(name) < 3 || len(name) > 63 {
		return "Bucket name must be between 3 and 63 characters long"
	}

	if !bucketNameRegex.MatchString(name) {
		return "Bucket name can only contain lowercase letters, numbers, hyphens, and periods"
	}

	// Cannot be formatted as an IP address.
	if ipAddressRegex.MatchString(name) {
		return "Bucket name must not be formatted as an IP address"
	}

	// Must not start with xn-- (internationalized domain label).
	if strings.HasPrefix(name, "xn--") {
		return "Bucket name must not start with xn--"
	}

	// Must not end with -s3alias or --ol-s3.
	if strings.HasSuffix(name, "-s3alias") || strings.HasSuffix(name, "--ol-s3") {
		return "Bucket name must not end with -s3alias or --ol-s3"
	}

	// Cannot have consecutive periods.
	if strings.Contains(name, "..") {
		return "Bucket name must not contain consecutive periods"
	}

	return ""
}

// defaultPrivateACL returns a JSON-serialized ACL granting FULL_CONTROL
// to the specified owner. This is the default ACL for new buckets and objects.
func defaultPrivateACL(ownerID, ownerDisplay string) json.RawMessage {
	acl := xmlutil.AccessControlPolicy{
		Owner: xmlutil.Owner{
			ID:          ownerID,
			DisplayName: ownerDisplay,
		},
		AccessControlList: xmlutil.ACL{
			Grants: []xmlutil.Grant{
				{
					Grantee: xmlutil.Grantee{
						Type:        "CanonicalUser",
						ID:          ownerID,
						DisplayName: ownerDisplay,
					},
					Permission: "FULL_CONTROL",
				},
			},
		},
	}

	data, _ := json.Marshal(acl)
	return data
}

// parseCannedACL converts a canned ACL name into an AccessControlPolicy
// with the appropriate grants for the given owner.
func parseCannedACL(cannedACL, ownerID, ownerDisplay string) *xmlutil.AccessControlPolicy {
	acp := &xmlutil.AccessControlPolicy{
		Owner: xmlutil.Owner{
			ID:          ownerID,
			DisplayName: ownerDisplay,
		},
	}

	ownerGrant := xmlutil.Grant{
		Grantee: xmlutil.Grantee{
			Type:        "CanonicalUser",
			ID:          ownerID,
			DisplayName: ownerDisplay,
		},
		Permission: "FULL_CONTROL",
	}

	switch cannedACL {
	case "private", "":
		acp.AccessControlList = xmlutil.ACL{
			Grants: []xmlutil.Grant{ownerGrant},
		}
	case "public-read":
		acp.AccessControlList = xmlutil.ACL{
			Grants: []xmlutil.Grant{
				ownerGrant,
				{
					Grantee: xmlutil.Grantee{
						Type: "Group",
						URI:  "http://acs.amazonaws.com/groups/global/AllUsers",
					},
					Permission: "READ",
				},
			},
		}
	case "public-read-write":
		acp.AccessControlList = xmlutil.ACL{
			Grants: []xmlutil.Grant{
				ownerGrant,
				{
					Grantee: xmlutil.Grantee{
						Type: "Group",
						URI:  "http://acs.amazonaws.com/groups/global/AllUsers",
					},
					Permission: "READ",
				},
				{
					Grantee: xmlutil.Grantee{
						Type: "Group",
						URI:  "http://acs.amazonaws.com/groups/global/AllUsers",
					},
					Permission: "WRITE",
				},
			},
		}
	case "authenticated-read":
		acp.AccessControlList = xmlutil.ACL{
			Grants: []xmlutil.Grant{
				ownerGrant,
				{
					Grantee: xmlutil.Grantee{
						Type: "Group",
						URI:  "http://acs.amazonaws.com/groups/global/AuthenticatedUsers",
					},
					Permission: "READ",
				},
			},
		}
	default:
		// Unknown canned ACL: fall back to private.
		acp.AccessControlList = xmlutil.ACL{
			Grants: []xmlutil.Grant{ownerGrant},
		}
	}

	return acp
}

// grantHeaderMap maps x-amz-grant-* header names to the corresponding S3
// permission string.
var grantHeaderMap = map[string]string{
	"X-Amz-Grant-Full-Control": "FULL_CONTROL",
	"X-Amz-Grant-Read":         "READ",
	"X-Amz-Grant-Read-Acp":     "READ_ACP",
	"X-Amz-Grant-Write":        "WRITE",
	"X-Amz-Grant-Write-Acp":    "WRITE_ACP",
}

// hasGrantHeaders returns true if any x-amz-grant-* header is present in the request.
func hasGrantHeaders(headers http.Header) bool {
	for headerName := range grantHeaderMap {
		if headers.Get(headerName) != "" {
			return true
		}
	}
	return false
}

// parseGrantHeaders parses x-amz-grant-* headers into an AccessControlPolicy.
// The header values use the format: id="canonical-user-id" or
// uri="http://acs.amazonaws.com/groups/...", comma-separated for multiple grantees.
// Returns nil if no grant headers are present.
func parseGrantHeaders(headers http.Header, ownerID, ownerDisplay string) *xmlutil.AccessControlPolicy {
	var grants []xmlutil.Grant

	for headerName, permission := range grantHeaderMap {
		headerVal := headers.Get(headerName)
		if headerVal == "" {
			continue
		}

		// Split by comma for multiple grantees.
		entries := strings.Split(headerVal, ",")
		for _, entry := range entries {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}

			// Parse key="value" pairs within each entry.
			// An entry may contain multiple key=value separated by commas,
			// but we already split by comma above, so each entry is a single grantee.
			grant := xmlutil.Grant{
				Permission: permission,
			}

			if strings.HasPrefix(entry, "id=") {
				// Canonical user grant: id="user-id"
				idVal := strings.TrimPrefix(entry, "id=")
				idVal = strings.Trim(idVal, `"`)
				grant.Grantee = xmlutil.Grantee{
					Type: "CanonicalUser",
					ID:   idVal,
				}
			} else if strings.HasPrefix(entry, "uri=") {
				// Group grant: uri="http://acs.amazonaws.com/groups/..."
				uriVal := strings.TrimPrefix(entry, "uri=")
				uriVal = strings.Trim(uriVal, `"`)
				grant.Grantee = xmlutil.Grantee{
					Type: "Group",
					URI:  uriVal,
				}
			} else if strings.HasPrefix(entry, "emailAddress=") {
				// Email grant: emailAddress="email@example.com" — treat as canonical user.
				emailVal := strings.TrimPrefix(entry, "emailAddress=")
				emailVal = strings.Trim(emailVal, `"`)
				grant.Grantee = xmlutil.Grantee{
					Type: "AmazonCustomerByEmail",
					ID:   emailVal,
				}
			} else {
				// Unknown format: skip.
				continue
			}

			grants = append(grants, grant)
		}
	}

	if len(grants) == 0 {
		return nil
	}

	return &xmlutil.AccessControlPolicy{
		Owner: xmlutil.Owner{
			ID:          ownerID,
			DisplayName: ownerDisplay,
		},
		AccessControlList: xmlutil.ACL{
			Grants: grants,
		},
	}
}

// aclToJSON converts an AccessControlPolicy to a JSON-encoded RawMessage.
func aclToJSON(acp *xmlutil.AccessControlPolicy) json.RawMessage {
	data, _ := json.Marshal(acp)
	return data
}

// aclFromJSON parses a JSON-encoded ACL into an AccessControlPolicy.
// Returns nil if the JSON is empty or unparseable.
func aclFromJSON(data json.RawMessage) *xmlutil.AccessControlPolicy {
	if len(data) == 0 || string(data) == "{}" {
		return nil
	}
	var acp xmlutil.AccessControlPolicy
	if err := json.Unmarshal(data, &acp); err != nil {
		return nil
	}
	return &acp
}

// extractBucketName extracts the bucket name from the URL path.
func extractBucketName(r *http.Request) string {
	path := r.URL.Path
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	// Find the first slash (if any) to separate bucket from key.
	idx := strings.IndexByte(path, '/')
	if idx >= 0 {
		return path[:idx]
	}
	return path
}

// extractUserMetadata scans request headers for x-amz-meta-* prefixed headers
// and returns them as a map. The prefix is stripped and the key is lowercased.
func extractUserMetadata(r *http.Request) map[string]string {
	meta := make(map[string]string)
	for key, values := range r.Header {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "x-amz-meta-") {
			metaKey := lower[len("x-amz-meta-"):]
			if len(values) > 0 && metaKey != "" {
				meta[metaKey] = values[0]
			}
		}
	}
	if len(meta) == 0 {
		return nil
	}
	return meta
}

// parseDeleteRequest parses a DeleteObjects XML request body into a DeleteRequest struct.
func parseDeleteRequest(body io.Reader) (*xmlutil.DeleteRequest, error) {
	var req xmlutil.DeleteRequest
	if err := xml.NewDecoder(body).Decode(&req); err != nil {
		return nil, err
	}
	return &req, nil
}

// parseCopySource parses the X-Amz-Copy-Source header and returns the source
// bucket and key. The header value is URL-decoded and expected in the format
// "/bucket/key" or "bucket/key".
func parseCopySource(header string) (bucket, key string, ok bool) {
	// URL-decode the header value.
	decoded, err := url.PathUnescape(header)
	if err != nil {
		return "", "", false
	}

	// Trim leading slash.
	decoded = strings.TrimPrefix(decoded, "/")
	if decoded == "" {
		return "", "", false
	}

	// Split into bucket/key at the first slash.
	idx := strings.IndexByte(decoded, '/')
	if idx < 0 || idx == len(decoded)-1 {
		return "", "", false
	}

	return decoded[:idx], decoded[idx+1:], true
}

// parseRange parses an HTTP Range header value and returns the byte range
// [start, end] inclusive. Supports three formats:
//   - bytes=0-4   (first 5 bytes)
//   - bytes=5-    (from byte 5 to end)
//   - bytes=-10   (last 10 bytes)
//
// Returns an error for unsatisfiable ranges or invalid syntax.
func parseRange(rangeHeader string, objectSize int64) (start, end int64, err error) {
	if objectSize == 0 {
		return 0, 0, fmt.Errorf("empty object")
	}

	// Must start with "bytes=".
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, 0, fmt.Errorf("invalid range header: missing bytes= prefix")
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")

	// We only support a single range (no multi-range).
	if strings.Contains(rangeSpec, ",") {
		return 0, 0, fmt.Errorf("multi-range not supported")
	}

	parts := strings.SplitN(rangeSpec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range spec: %q", rangeSpec)
	}

	startStr := strings.TrimSpace(parts[0])
	endStr := strings.TrimSpace(parts[1])

	if startStr == "" && endStr == "" {
		return 0, 0, fmt.Errorf("invalid range: both start and end are empty")
	}

	if startStr == "" {
		// Suffix range: bytes=-N (last N bytes).
		suffixLen, parseErr := strconv.ParseInt(endStr, 10, 64)
		if parseErr != nil || suffixLen <= 0 {
			return 0, 0, fmt.Errorf("invalid suffix length: %q", endStr)
		}
		if suffixLen >= objectSize {
			// Entire object.
			return 0, objectSize - 1, nil
		}
		return objectSize - suffixLen, objectSize - 1, nil
	}

	start, err = strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, fmt.Errorf("invalid range start: %q", startStr)
	}

	if start >= objectSize {
		return 0, 0, fmt.Errorf("range start %d beyond object size %d", start, objectSize)
	}

	if endStr == "" {
		// Open-ended range: bytes=N- (from byte N to end).
		return start, objectSize - 1, nil
	}

	end, err = strconv.ParseInt(endStr, 10, 64)
	if err != nil || end < 0 {
		return 0, 0, fmt.Errorf("invalid range end: %q", endStr)
	}

	// Clamp end to last byte.
	if end >= objectSize {
		end = objectSize - 1
	}

	if start > end {
		return 0, 0, fmt.Errorf("range start %d > end %d", start, end)
	}

	return start, end, nil
}

// checkCopySourceConditionals evaluates x-amz-copy-source-if-* headers against
// the source object's ETag and LastModified time. Used by CopyObject and UploadPartCopy.
// Returns true if the copy should proceed, false if a precondition failed.
// On failure, returns the appropriate S3Error.
func checkCopySourceConditionals(r *http.Request, etag string, lastModified time.Time) (proceed bool, err *s3err.S3Error) {
	normalizeETag := func(e string) string {
		return strings.Trim(e, `"`)
	}

	objectETag := normalizeETag(etag)

	ifMatch := r.Header.Get("x-amz-copy-source-if-match")
	if ifMatch != "" {
		matched := false
		if ifMatch == "*" {
			matched = true
		} else {
			tags := strings.Split(ifMatch, ",")
			for _, tag := range tags {
				if normalizeETag(strings.TrimSpace(tag)) == objectETag {
					matched = true
					break
				}
			}
		}
		if !matched {
			return false, s3err.ErrPreconditionFailed
		}
	}

	if ifMatch == "" {
		ifUnmodSince := r.Header.Get("x-amz-copy-source-if-unmodified-since")
		if ifUnmodSince != "" {
			t, parseErr := http.ParseTime(ifUnmodSince)
			if parseErr == nil {
				if lastModified.Truncate(time.Second).After(t.Truncate(time.Second)) {
					return false, s3err.ErrPreconditionFailed
				}
			}
		}
	}

	ifNoneMatch := r.Header.Get("x-amz-copy-source-if-none-match")
	if ifNoneMatch != "" {
		matched := false
		if ifNoneMatch == "*" {
			matched = true
		} else {
			tags := strings.Split(ifNoneMatch, ",")
			for _, tag := range tags {
				if normalizeETag(strings.TrimSpace(tag)) == objectETag {
					matched = true
					break
				}
			}
		}
		if matched {
			return false, s3err.ErrPreconditionFailed
		}
	}

	if ifNoneMatch == "" {
		ifModSince := r.Header.Get("x-amz-copy-source-if-modified-since")
		if ifModSince != "" {
			t, parseErr := http.ParseTime(ifModSince)
			if parseErr == nil {
				if !lastModified.Truncate(time.Second).After(t.Truncate(time.Second)) {
					return false, s3err.ErrPreconditionFailed
				}
			}
		}
	}

	return true, nil
}

// checkConditionalHeaders evaluates the conditional request headers against
// the object's ETag and LastModified time. Returns the appropriate HTTP status
// code and whether the response should be skipped (no body).
//
// Priority order per RFC 7232:
//  1. If-Match (412 on mismatch)
//  2. If-Unmodified-Since (412 if modified)
//  3. If-None-Match (304 for GET/HEAD, 412 for other methods)
//  4. If-Modified-Since (304 if not modified)
func checkConditionalHeaders(r *http.Request, etag string, lastModified time.Time) (statusCode int, skip bool) {
	// Normalize ETags: strip surrounding quotes for comparison.
	normalizeETag := func(e string) string {
		return strings.Trim(e, `"`)
	}

	objectETag := normalizeETag(etag)

	// Step 1: If-Match
	ifMatch := r.Header.Get("If-Match")
	if ifMatch != "" {
		matched := false
		if ifMatch == "*" {
			matched = true
		} else {
			tags := strings.Split(ifMatch, ",")
			for _, tag := range tags {
				if normalizeETag(strings.TrimSpace(tag)) == objectETag {
					matched = true
					break
				}
			}
		}
		if !matched {
			return http.StatusPreconditionFailed, true
		}
	}

	// Step 2: If-Unmodified-Since (only if If-Match was not present)
	if ifMatch == "" {
		ifUnmodSince := r.Header.Get("If-Unmodified-Since")
		if ifUnmodSince != "" {
			t, parseErr := http.ParseTime(ifUnmodSince)
			if parseErr == nil {
				if lastModified.Truncate(time.Second).After(t.Truncate(time.Second)) {
					return http.StatusPreconditionFailed, true
				}
			}
		}
	}

	// Step 3: If-None-Match
	ifNoneMatch := r.Header.Get("If-None-Match")
	if ifNoneMatch != "" {
		matched := false
		if ifNoneMatch == "*" {
			matched = true
		} else {
			tags := strings.Split(ifNoneMatch, ",")
			for _, tag := range tags {
				if normalizeETag(strings.TrimSpace(tag)) == objectETag {
					matched = true
					break
				}
			}
		}
		if matched {
			// For GET and HEAD: 304 Not Modified.
			// For other methods: 412 Precondition Failed.
			if r.Method == "GET" || r.Method == "HEAD" {
				return http.StatusNotModified, true
			}
			return http.StatusPreconditionFailed, true
		}
	}

	// Step 4: If-Modified-Since (only if If-None-Match was not present)
	if ifNoneMatch == "" {
		ifModSince := r.Header.Get("If-Modified-Since")
		if ifModSince != "" {
			t, parseErr := http.ParseTime(ifModSince)
			if parseErr == nil {
				// 304 if the object has NOT been modified since the given time.
				if !lastModified.Truncate(time.Second).After(t.Truncate(time.Second)) {
					if r.Method == "GET" || r.Method == "HEAD" {
						return http.StatusNotModified, true
					}
				}
			}
		}
	}

	return 0, false
}

// setObjectResponseHeaders sets standard S3 object response headers from the
// object metadata record. This is used by GetObject and HeadObject.
func setObjectResponseHeaders(w http.ResponseWriter, obj *metadata.ObjectRecord) {
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("ETag", obj.ETag)
	w.Header().Set("Last-Modified", xmlutil.FormatTimeHTTP(obj.LastModified))
	w.Header().Set("Accept-Ranges", "bytes")

	if obj.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", obj.ContentEncoding)
	}
	if obj.ContentLanguage != "" {
		w.Header().Set("Content-Language", obj.ContentLanguage)
	}
	if obj.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", obj.ContentDisposition)
	}
	if obj.CacheControl != "" {
		w.Header().Set("Cache-Control", obj.CacheControl)
	}
	if obj.Expires != "" {
		w.Header().Set("Expires", obj.Expires)
	}
	if obj.StorageClass != "" && obj.StorageClass != "STANDARD" {
		w.Header().Set("x-amz-storage-class", obj.StorageClass)
	}

	// Emit user metadata as x-amz-meta-* headers.
	for key, value := range obj.UserMetadata {
		w.Header().Set("x-amz-meta-"+strings.ToLower(key), value)
	}

	// Set Content-Length from metadata.
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
}

// applyResponseOverrides applies response-* query parameter overrides to the
// response headers. These are used for presigned URLs to override content headers.
func applyResponseOverrides(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if v := q.Get("response-content-type"); v != "" {
		w.Header().Set("Content-Type", v)
	}
	if v := q.Get("response-content-language"); v != "" {
		w.Header().Set("Content-Language", v)
	}
	if v := q.Get("response-expires"); v != "" {
		w.Header().Set("Expires", v)
	}
	if v := q.Get("response-cache-control"); v != "" {
		w.Header().Set("Cache-Control", v)
	}
	if v := q.Get("response-content-disposition"); v != "" {
		w.Header().Set("Content-Disposition", v)
	}
	if v := q.Get("response-content-encoding"); v != "" {
		w.Header().Set("Content-Encoding", v)
	}
}

// CompletePart represents a single part entry in a CompleteMultipartUpload
// XML request body.
type CompletePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// CompleteMultipartUploadRequest is the XML structure for the
// CompleteMultipartUpload request body.
type CompleteMultipartUploadRequest struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []CompletePart `xml:"Part"`
}

// parseCompleteMultipartXML parses the CompleteMultipartUpload XML request body
// and returns the list of parts. Returns an error if the XML is malformed.
func parseCompleteMultipartXML(body io.Reader) ([]CompletePart, error) {
	var req CompleteMultipartUploadRequest
	if err := xml.NewDecoder(body).Decode(&req); err != nil {
		return nil, fmt.Errorf("decoding CompleteMultipartUpload XML: %w", err)
	}
	return req.Parts, nil
}

// computeCompositeETag computes the S3-style composite ETag from a list of
// individual part ETags. The composite is formed by:
//  1. Stripping quotes from each part ETag
//  2. Decoding each hex string to raw bytes
//  3. Concatenating the raw MD5 bytes
//  4. Computing MD5 of the concatenation
//  5. Formatting as "hexdigest-N" where N is the part count
func computeCompositeETag(partETags []string) string {
	h := md5.New()
	for _, etag := range partETags {
		hexStr := strings.Trim(etag, `"`)
		raw, err := hex.DecodeString(hexStr)
		if err != nil {
			// If we can't decode, skip (should not happen for valid ETags).
			continue
		}
		h.Write(raw)
	}
	return fmt.Sprintf(`"%x-%d"`, h.Sum(nil), len(partETags))
}
