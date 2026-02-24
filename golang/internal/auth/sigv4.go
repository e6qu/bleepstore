// Package auth implements AWS Signature Version 4 request authentication.
package auth

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bleepstore/bleepstore/internal/metadata"
)

const (
	// signingKeyTTL is the TTL for cached signing keys (24 hours).
	signingKeyTTL = 24 * time.Hour
	// credCacheTTL is the TTL for cached credential lookups (60 seconds).
	credCacheTTL = 60 * time.Second
	// maxCacheEntries is the maximum number of entries in each cache map.
	maxCacheEntries = 1000
)

// signingKeyCacheEntry holds a cached signing key with its expiration.
type signingKeyCacheEntry struct {
	key       []byte
	expiresAt time.Time
}

// credCacheEntry holds a cached credential record with its expiration.
type credCacheEntry struct {
	cred      *metadata.CredentialRecord
	expiresAt time.Time
}

const (
	// algorithm is the signing algorithm identifier.
	algorithm = "AWS4-HMAC-SHA256"

	// scopeTerminator is the fixed suffix of the credential scope.
	scopeTerminator = "aws4_request"

	// service is the service name for S3.
	service = "s3"

	// unsignedPayload is the constant used when payload verification is skipped.
	unsignedPayload = "UNSIGNED-PAYLOAD"

	// streamingPayload indicates chunked upload with per-chunk signing.
	streamingPayload = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"

	// emptySHA256 is the SHA-256 hash of an empty string.
	emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	// maxPresignedExpiry is the maximum presigned URL expiration in seconds (7 days).
	maxPresignedExpiry = 604800

	// clockSkewTolerance is the maximum allowed clock skew for header-based auth.
	clockSkewTolerance = 15 * time.Minute

	// amzDateFormat is the format for x-amz-date values.
	amzDateFormat = "20060102T150405Z"

	// amzDateShort is the format for the date portion of credential scope.
	amzDateShort = "20060102"
)

// contextKey is an unexported type used for context keys to avoid collisions.
type contextKey int

const (
	// ownerIDKey is the context key for the authenticated owner ID.
	ownerIDKey contextKey = iota
	// ownerDisplayKey is the context key for the authenticated owner display name.
	ownerDisplayKey
)

// OwnerFromContext retrieves the authenticated owner ID from the request context.
func OwnerFromContext(ctx context.Context) (ownerID, displayName string) {
	if v, ok := ctx.Value(ownerIDKey).(string); ok {
		ownerID = v
	}
	if v, ok := ctx.Value(ownerDisplayKey).(string); ok {
		displayName = v
	}
	return
}

// contextWithOwner sets the owner identity on the given context.
func contextWithOwner(ctx context.Context, ownerID, displayName string) context.Context {
	ctx = context.WithValue(ctx, ownerIDKey, ownerID)
	ctx = context.WithValue(ctx, ownerDisplayKey, displayName)
	return ctx
}

// SigV4Verifier verifies AWS Signature Version 4 signed requests.
// It looks up credentials from the metadata store to support multiple access keys.
type SigV4Verifier struct {
	// Meta is the metadata store used to look up credentials.
	Meta metadata.MetadataStore
	// Region is the AWS region used in the credential scope.
	Region string

	// signingKeys caches derived signing keys. Key format: "secretKey\x00dateStr\x00region\x00service".
	signingKeyMu sync.RWMutex
	signingKeys  map[string]signingKeyCacheEntry

	// credCache caches credential lookups by access key ID.
	credCacheMu sync.RWMutex
	credCache   map[string]credCacheEntry
}

// NewSigV4Verifier creates a new SigV4Verifier with the given metadata store and region.
func NewSigV4Verifier(meta metadata.MetadataStore, region string) *SigV4Verifier {
	return &SigV4Verifier{
		Meta:        meta,
		Region:      region,
		signingKeys: make(map[string]signingKeyCacheEntry),
		credCache:   make(map[string]credCacheEntry),
	}
}

// cachedDeriveSigningKey returns a cached signing key or derives and caches a new one.
func (v *SigV4Verifier) cachedDeriveSigningKey(secretKey, dateStr, region, svc string) []byte {
	cacheKey := secretKey + "\x00" + dateStr + "\x00" + region + "\x00" + svc
	now := time.Now()

	v.signingKeyMu.RLock()
	if entry, ok := v.signingKeys[cacheKey]; ok && now.Before(entry.expiresAt) {
		v.signingKeyMu.RUnlock()
		return entry.key
	}
	v.signingKeyMu.RUnlock()

	key := deriveSigningKey(secretKey, dateStr, region, svc)

	v.signingKeyMu.Lock()
	if len(v.signingKeys) >= maxCacheEntries {
		// Clear entire map to avoid unbounded growth.
		v.signingKeys = make(map[string]signingKeyCacheEntry)
	}
	v.signingKeys[cacheKey] = signingKeyCacheEntry{
		key:       key,
		expiresAt: now.Add(signingKeyTTL),
	}
	v.signingKeyMu.Unlock()

	return key
}

// cachedGetCredential returns a cached credential or fetches and caches from the store.
func (v *SigV4Verifier) cachedGetCredential(ctx context.Context, accessKeyID string) (*metadata.CredentialRecord, error) {
	now := time.Now()

	v.credCacheMu.RLock()
	if entry, ok := v.credCache[accessKeyID]; ok && now.Before(entry.expiresAt) {
		v.credCacheMu.RUnlock()
		return entry.cred, nil
	}
	v.credCacheMu.RUnlock()

	cred, err := v.Meta.GetCredential(ctx, accessKeyID)
	if err != nil {
		return nil, err
	}

	v.credCacheMu.Lock()
	if len(v.credCache) >= maxCacheEntries {
		v.credCache = make(map[string]credCacheEntry)
	}
	v.credCache[accessKeyID] = credCacheEntry{
		cred:      cred,
		expiresAt: now.Add(credCacheTTL),
	}
	v.credCacheMu.Unlock()

	return cred, nil
}

// AuthError represents an authentication failure with an S3-compatible error code.
type AuthError struct {
	Code    string // S3 error code (AccessDenied, InvalidAccessKeyId, SignatureDoesNotMatch, etc.)
	Message string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// parsedAuth holds the parsed components of an Authorization header.
type parsedAuth struct {
	AccessKeyID   string
	DateStr       string // YYYYMMDD
	Region        string
	Service       string
	SignedHeaders []string
	Signature     string
}

// parseAuthorizationHeader parses the AWS SigV4 Authorization header.
// Format: AWS4-HMAC-SHA256 Credential=AKID/date/region/service/aws4_request, SignedHeaders=host;..., Signature=hex
func parseAuthorizationHeader(header string) (*parsedAuth, error) {
	if !strings.HasPrefix(header, algorithm+" ") {
		return nil, fmt.Errorf("unsupported algorithm")
	}

	// Remove the "AWS4-HMAC-SHA256 " prefix.
	rest := strings.TrimPrefix(header, algorithm+" ")

	parts := make(map[string]string)
	for _, part := range strings.Split(rest, ",") {
		part = strings.TrimSpace(part)
		idx := strings.IndexByte(part, '=')
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(part[:idx])
		value := strings.TrimSpace(part[idx+1:])
		parts[key] = value
	}

	credential, ok := parts["Credential"]
	if !ok || credential == "" {
		return nil, fmt.Errorf("missing Credential")
	}

	signedHeadersStr, ok := parts["SignedHeaders"]
	if !ok || signedHeadersStr == "" {
		return nil, fmt.Errorf("missing SignedHeaders")
	}

	signature, ok := parts["Signature"]
	if !ok || signature == "" {
		return nil, fmt.Errorf("missing Signature")
	}

	// Parse credential: accessKeyID/date/region/service/aws4_request
	credParts := strings.SplitN(credential, "/", 5)
	if len(credParts) != 5 {
		return nil, fmt.Errorf("invalid credential format")
	}
	if credParts[4] != scopeTerminator {
		return nil, fmt.Errorf("invalid credential scope terminator: %s", credParts[4])
	}

	return &parsedAuth{
		AccessKeyID:   credParts[0],
		DateStr:       credParts[1],
		Region:        credParts[2],
		Service:       credParts[3],
		SignedHeaders: strings.Split(signedHeadersStr, ";"),
		Signature:     signature,
	}, nil
}

// VerifyRequest validates the AWS SigV4 signature on the given HTTP request
// using the Authorization header. Returns the credential record on success.
func (v *SigV4Verifier) VerifyRequest(r *http.Request) (*metadata.CredentialRecord, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return nil, &AuthError{Code: "AccessDenied", Message: "Missing Authorization header"}
	}

	parsed, err := parseAuthorizationHeader(authHeader)
	if err != nil {
		return nil, &AuthError{Code: "AccessDenied", Message: fmt.Sprintf("Invalid Authorization header: %v", err)}
	}

	// Look up credential by access key ID (cached).
	cred, err := v.cachedGetCredential(r.Context(), parsed.AccessKeyID)
	if err != nil {
		return nil, &AuthError{Code: "InternalError", Message: "Failed to look up credentials"}
	}
	if cred == nil || !cred.Active {
		return nil, &AuthError{Code: "InvalidAccessKeyId", Message: "The AWS Access Key Id you provided does not exist in our records"}
	}

	// Get the timestamp from x-amz-date or Date header.
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		amzDate = r.Header.Get("Date")
	}
	if amzDate == "" {
		return nil, &AuthError{Code: "AccessDenied", Message: "Missing X-Amz-Date or Date header"}
	}

	// Parse the timestamp.
	requestTime, parseErr := time.Parse(amzDateFormat, amzDate)
	if parseErr != nil {
		// Try HTTP date format as fallback.
		requestTime, parseErr = time.Parse(time.RFC1123, amzDate)
		if parseErr != nil {
			return nil, &AuthError{Code: "AccessDenied", Message: "Invalid date format"}
		}
	}

	// Check clock skew.
	now := time.Now().UTC()
	diff := now.Sub(requestTime)
	if diff < 0 {
		diff = -diff
	}
	if diff > clockSkewTolerance {
		return nil, &AuthError{Code: "RequestTimeTooSkewed", Message: "The difference between the request time and the server's time is too large"}
	}

	// Verify credential date matches the timestamp date portion.
	dateStr := amzDate[:8] // First 8 chars = YYYYMMDD
	if parsed.DateStr != dateStr {
		return nil, &AuthError{Code: "SignatureDoesNotMatch", Message: "Credential date does not match X-Amz-Date"}
	}

	// When x-amz-content-sha256 header is absent (e.g., botocore SigV4Auth
	// without S3 SigV4), compute SHA256(body) for the canonical request
	// instead of using UNSIGNED-PAYLOAD. This matches what the client does
	// when computing the canonical request without sending the header.
	if r.Header.Get("X-Amz-Content-Sha256") == "" && r.Body != nil {
		bodyBytes, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			return nil, &AuthError{Code: "InternalError", Message: "Failed to read request body"}
		}
		// Replace the body so downstream handlers can still read it.
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		bodyHash := sha256.Sum256(bodyBytes)
		r.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(bodyHash[:]))
	} else if r.Header.Get("X-Amz-Content-Sha256") == "" {
		// No body: use the hash of empty string.
		r.Header.Set("X-Amz-Content-Sha256", emptySHA256)
	}

	// Build canonical request.
	canonicalRequest := buildCanonicalRequest(r, parsed.SignedHeaders)

	// Build string to sign.
	scope := fmt.Sprintf("%s/%s/%s/%s", parsed.DateStr, parsed.Region, parsed.Service, scopeTerminator)
	stringToSign := buildStringToSign(amzDate, scope, canonicalRequest)

	// Derive signing key (cached) and compute expected signature.
	signingKey := v.cachedDeriveSigningKey(cred.SecretKey, parsed.DateStr, parsed.Region, parsed.Service)
	expectedSignature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	// Constant-time comparison.
	if subtle.ConstantTimeCompare([]byte(expectedSignature), []byte(parsed.Signature)) != 1 {
		return nil, &AuthError{Code: "SignatureDoesNotMatch", Message: "The request signature we calculated does not match the signature you provided"}
	}

	return cred, nil
}

// VerifyPresigned validates a presigned URL by checking the X-Amz-* query parameters.
func (v *SigV4Verifier) VerifyPresigned(r *http.Request) (*metadata.CredentialRecord, error) {
	q := r.URL.Query()

	// Validate algorithm.
	algo := q.Get("X-Amz-Algorithm")
	if algo != algorithm {
		return nil, &AuthError{Code: "AccessDenied", Message: "Unsupported algorithm"}
	}

	// Parse credential.
	credStr := q.Get("X-Amz-Credential")
	if credStr == "" {
		return nil, &AuthError{Code: "AccessDenied", Message: "Missing X-Amz-Credential"}
	}
	credParts := strings.SplitN(credStr, "/", 5)
	if len(credParts) != 5 || credParts[4] != scopeTerminator {
		return nil, &AuthError{Code: "AccessDenied", Message: "Invalid credential format"}
	}

	accessKeyID := credParts[0]
	dateStr := credParts[1]
	region := credParts[2]
	svc := credParts[3]

	// Get other parameters.
	amzDate := q.Get("X-Amz-Date")
	if amzDate == "" {
		return nil, &AuthError{Code: "AccessDenied", Message: "Missing X-Amz-Date"}
	}

	expiresStr := q.Get("X-Amz-Expires")
	if expiresStr == "" {
		return nil, &AuthError{Code: "AccessDenied", Message: "Missing X-Amz-Expires"}
	}

	signedHeadersStr := q.Get("X-Amz-SignedHeaders")
	if signedHeadersStr == "" {
		return nil, &AuthError{Code: "AccessDenied", Message: "Missing X-Amz-SignedHeaders"}
	}

	signature := q.Get("X-Amz-Signature")
	if signature == "" {
		return nil, &AuthError{Code: "AccessDenied", Message: "Missing X-Amz-Signature"}
	}

	// Parse and validate expiration.
	var expires int
	_, scanErr := fmt.Sscanf(expiresStr, "%d", &expires)
	if scanErr != nil || expires < 1 || expires > maxPresignedExpiry {
		return nil, &AuthError{Code: "AccessDenied", Message: fmt.Sprintf("Invalid X-Amz-Expires value: %s", expiresStr)}
	}

	// Parse the timestamp.
	requestTime, parseErr := time.Parse(amzDateFormat, amzDate)
	if parseErr != nil {
		return nil, &AuthError{Code: "AccessDenied", Message: "Invalid X-Amz-Date format"}
	}

	// Check expiration.
	if time.Now().UTC().After(requestTime.Add(time.Duration(expires) * time.Second)) {
		return nil, &AuthError{Code: "AccessDenied", Message: "Request has expired"}
	}

	// Verify credential date matches X-Amz-Date date portion.
	if dateStr != amzDate[:8] {
		return nil, &AuthError{Code: "SignatureDoesNotMatch", Message: "Credential date does not match X-Amz-Date"}
	}

	// Look up credential (cached).
	cred, err := v.cachedGetCredential(r.Context(), accessKeyID)
	if err != nil {
		return nil, &AuthError{Code: "InternalError", Message: "Failed to look up credentials"}
	}
	if cred == nil || !cred.Active {
		return nil, &AuthError{Code: "InvalidAccessKeyId", Message: "The AWS Access Key Id you provided does not exist in our records"}
	}

	// Build canonical request for presigned URL.
	signedHeaders := strings.Split(signedHeadersStr, ";")
	canonicalRequest := buildPresignedCanonicalRequest(r, signedHeaders)

	// Build string to sign.
	scope := fmt.Sprintf("%s/%s/%s/%s", dateStr, region, svc, scopeTerminator)
	stringToSign := buildStringToSign(amzDate, scope, canonicalRequest)

	// Derive signing key (cached) and compute expected signature.
	signingKey := v.cachedDeriveSigningKey(cred.SecretKey, dateStr, region, svc)
	expectedSignature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	// Constant-time comparison.
	if subtle.ConstantTimeCompare([]byte(expectedSignature), []byte(signature)) != 1 {
		return nil, &AuthError{Code: "SignatureDoesNotMatch", Message: "The request signature we calculated does not match the signature you provided"}
	}

	return cred, nil
}

// buildCanonicalRequest builds the canonical request string for header-based auth.
func buildCanonicalRequest(r *http.Request, signedHeaders []string) string {
	var sb strings.Builder

	// HTTP method.
	sb.WriteString(r.Method)
	sb.WriteByte('\n')

	// Canonical URI.
	sb.WriteString(canonicalURI(r.URL.Path))
	sb.WriteByte('\n')

	// Canonical query string.
	sb.WriteString(canonicalQueryString(r.URL.Query()))
	sb.WriteByte('\n')

	// Canonical headers (each followed by \n).
	sb.WriteString(canonicalHeaders(r, signedHeaders))
	sb.WriteByte('\n')

	// Signed headers.
	sb.WriteString(strings.Join(signedHeaders, ";"))
	sb.WriteByte('\n')

	// Hashed payload.
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = unsignedPayload
	}
	sb.WriteString(payloadHash)

	return sb.String()
}

// buildPresignedCanonicalRequest builds the canonical request for presigned URL auth.
func buildPresignedCanonicalRequest(r *http.Request, signedHeaders []string) string {
	var sb strings.Builder

	// HTTP method.
	sb.WriteString(r.Method)
	sb.WriteByte('\n')

	// Canonical URI.
	sb.WriteString(canonicalURI(r.URL.Path))
	sb.WriteByte('\n')

	// Canonical query string (excludes X-Amz-Signature).
	q := r.URL.Query()
	q.Del("X-Amz-Signature")
	sb.WriteString(canonicalQueryString(q))
	sb.WriteByte('\n')

	// Canonical headers.
	sb.WriteString(canonicalHeaders(r, signedHeaders))
	sb.WriteByte('\n')

	// Signed headers.
	sb.WriteString(strings.Join(signedHeaders, ";"))
	sb.WriteByte('\n')

	// Presigned URLs always use UNSIGNED-PAYLOAD.
	sb.WriteString(unsignedPayload)

	return sb.String()
}

// buildStringToSign builds the string to sign for SigV4.
func buildStringToSign(amzDate, scope, canonicalRequest string) string {
	hash := sha256.Sum256([]byte(canonicalRequest))
	return algorithm + "\n" +
		amzDate + "\n" +
		scope + "\n" +
		hex.EncodeToString(hash[:])
}

// deriveSigningKey derives the SigV4 signing key using the HMAC chain.
func deriveSigningKey(secretKey, dateStr, region, svc string) []byte {
	dateKey := hmacSHA256([]byte("AWS4"+secretKey), dateStr)
	regionKey := hmacSHA256(dateKey, region)
	serviceKey := hmacSHA256(regionKey, svc)
	return hmacSHA256(serviceKey, scopeTerminator)
}

// canonicalURI returns the URI-encoded absolute path.
// Forward slashes are NOT encoded. Empty path becomes "/".
func canonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	// Split on slashes, URI-encode each segment, rejoin.
	segments := strings.Split(path, "/")
	for i, seg := range segments {
		segments[i] = URIEncode(seg, false)
	}
	return strings.Join(segments, "/")
}

// canonicalQueryString returns the sorted, URI-encoded query string.
// Parameters with no value use empty value: "acl=".
func canonicalQueryString(values url.Values) string {
	if len(values) == 0 {
		return ""
	}

	// Collect all key=value pairs.
	var pairs []string
	for key, vals := range values {
		encodedKey := URIEncode(key, true)
		if len(vals) == 0 {
			pairs = append(pairs, encodedKey+"=")
		}
		for _, val := range vals {
			pairs = append(pairs, encodedKey+"="+URIEncode(val, true))
		}
	}

	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

// canonicalHeaders builds the canonical headers string from the signed header list.
func canonicalHeaders(r *http.Request, signedHeaders []string) string {
	var sb strings.Builder
	for _, name := range signedHeaders {
		name = strings.ToLower(name)
		var values []string
		if name == "host" {
			// Host header is often not in r.Header but in r.Host.
			host := r.Host
			if host == "" {
				host = r.Header.Get("Host")
			}
			values = []string{host}
		} else {
			values = r.Header.Values(http.CanonicalHeaderKey(name))
		}
		// Join multiple values with comma, trim whitespace, collapse spaces.
		joined := strings.Join(values, ",")
		joined = strings.TrimSpace(joined)
		// Collapse sequential spaces to single space.
		for strings.Contains(joined, "  ") {
			joined = strings.ReplaceAll(joined, "  ", " ")
		}
		sb.WriteString(name)
		sb.WriteByte(':')
		sb.WriteString(joined)
		sb.WriteByte('\n')
	}
	return sb.String()
}

// URIEncode encodes a string per S3 URI encoding rules.
// Characters A-Z, a-z, 0-9, '-', '_', '.', '~' are NOT encoded.
// If encodeSlash is false, '/' is also NOT encoded.
// All other characters are percent-encoded with uppercase hex.
func URIEncode(s string, encodeSlash bool) string {
	var sb strings.Builder
	sb.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if isURIUnreserved(c) || (!encodeSlash && c == '/') {
			sb.WriteByte(c)
		} else {
			sb.WriteByte('%')
			sb.WriteByte(hexDigit(c >> 4))
			sb.WriteByte(hexDigit(c & 0x0f))
		}
	}
	return sb.String()
}

// isURIUnreserved returns true if the byte is an unreserved URI character.
func isURIUnreserved(c byte) bool {
	return (c >= 'A' && c <= 'Z') ||
		(c >= 'a' && c <= 'z') ||
		(c >= '0' && c <= '9') ||
		c == '-' || c == '_' || c == '.' || c == '~'
}

// hexDigit returns the uppercase hex digit for a 4-bit value.
func hexDigit(b byte) byte {
	if b < 10 {
		return '0' + b
	}
	return 'A' + b - 10
}

// hmacSHA256 computes HMAC-SHA256 of the data using the given key.
func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// DetectAuthMethod returns the authentication method based on the request:
// "header" for Authorization header, "presigned" for query parameters, or "none".
// Returns "ambiguous" if both are present.
func DetectAuthMethod(r *http.Request) string {
	hasHeader := strings.HasPrefix(r.Header.Get("Authorization"), algorithm)
	hasQuery := r.URL.Query().Get("X-Amz-Algorithm") != ""

	if hasHeader && hasQuery {
		return "ambiguous"
	}
	if hasHeader {
		return "header"
	}
	if hasQuery {
		return "presigned"
	}
	return "none"
}
