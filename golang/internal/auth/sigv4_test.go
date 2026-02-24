package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bleepstore/bleepstore/internal/metadata"
)

// --- Test helpers ---

// newTestStore creates a real SQLite-backed metadata store in a temp directory.
func newTestStore(t *testing.T) *metadata.SQLiteStore {
	t.Helper()
	dir := t.TempDir()
	store, err := metadata.NewSQLiteStore(dir + "/test.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// seedTestCredential creates a test credential in the store.
func seedTestCredential(t *testing.T, store *metadata.SQLiteStore, accessKey, secretKey string) {
	t.Helper()
	cred := &metadata.CredentialRecord{
		AccessKeyID: accessKey,
		SecretKey:   secretKey,
		OwnerID:     accessKey,
		DisplayName: accessKey,
		Active:      true,
		CreatedAt:   time.Now().UTC(),
	}
	if err := store.PutCredential(context.Background(), cred); err != nil {
		t.Fatalf("PutCredential: %v", err)
	}
}

// signRequest signs an HTTP request using SigV4 header-based auth.
func signRequest(r *http.Request, accessKey, secretKey, region string, signTime time.Time) {
	amzDate := signTime.UTC().Format(amzDateFormat)
	dateStr := signTime.UTC().Format(amzDateShort)

	r.Header.Set("X-Amz-Date", amzDate)

	// Determine payload hash.
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = unsignedPayload
		r.Header.Set("X-Amz-Content-Sha256", payloadHash)
	}

	// Determine signed headers: host + all x-amz-* headers + content-type if present.
	var signedHeaderNames []string
	signedHeaderNames = append(signedHeaderNames, "host")

	headerMap := make(map[string]bool)
	headerMap["host"] = true
	for key := range r.Header {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "x-amz-") || lower == "content-type" {
			if !headerMap[lower] {
				signedHeaderNames = append(signedHeaderNames, lower)
				headerMap[lower] = true
			}
		}
	}
	sortStrings(signedHeaderNames)

	// Build canonical request.
	canonReq := buildCanonicalRequest(r, signedHeaderNames)

	// Build string to sign.
	scope := fmt.Sprintf("%s/%s/%s/%s", dateStr, region, service, scopeTerminator)
	strToSign := buildStringToSign(amzDate, scope, canonReq)

	// Derive signing key and compute signature.
	signingKey := deriveSigningKey(secretKey, dateStr, region, service)
	signature := hex.EncodeToString(hmacSHA256(signingKey, strToSign))

	// Set Authorization header.
	authHeader := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algorithm,
		accessKey+"/"+dateStr+"/"+region+"/"+service+"/"+scopeTerminator,
		"", // credential is already complete
		strings.Join(signedHeaderNames, ";"),
		signature,
	)
	// Fix: build credential properly.
	credential := fmt.Sprintf("%s/%s/%s/%s/%s", accessKey, dateStr, region, service, scopeTerminator)
	authHeader = fmt.Sprintf("%s Credential=%s, SignedHeaders=%s, Signature=%s",
		algorithm,
		credential,
		strings.Join(signedHeaderNames, ";"),
		signature,
	)
	r.Header.Set("Authorization", authHeader)
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// --- URIEncode tests ---

func TestURIEncode(t *testing.T) {
	tests := []struct {
		input       string
		encodeSlash bool
		expected    string
	}{
		// Unreserved characters are NOT encoded.
		{"abc123", true, "abc123"},
		{"ABCxyz", true, "ABCxyz"},
		{"-_.~", true, "-_.~"},

		// Spaces are encoded as %20.
		{"hello world", true, "hello%20world"},

		// Slashes: encode when encodeSlash=true, keep when false.
		{"path/to/object", true, "path%2Fto%2Fobject"},
		{"path/to/object", false, "path/to/object"},

		// Special characters.
		{"key=value&foo", true, "key%3Dvalue%26foo"},
		{"test@email.com", true, "test%40email.com"},
		{"file#1", true, "file%231"},

		// Unicode (multi-byte).
		{"\xc3\xa9", true, "%C3%A9"}, // e-acute

		// Empty string.
		{"", true, ""},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("URIEncode(%q, %v)", tt.input, tt.encodeSlash)
		t.Run(name, func(t *testing.T) {
			got := URIEncode(tt.input, tt.encodeSlash)
			if got != tt.expected {
				t.Errorf("got %q, want %q", got, tt.expected)
			}
		})
	}
}

// --- HMAC and signing key tests ---

func TestHmacSHA256(t *testing.T) {
	// Known test vector.
	key := []byte("key")
	data := "message"
	expected := "6e9ef29b75fffc5b7abae527d58fdadb2fe42e7219011976917343065f58ed4a"

	result := hex.EncodeToString(hmacSHA256(key, data))
	if result != expected {
		t.Errorf("hmacSHA256 = %s, want %s", result, expected)
	}
}

func TestDeriveSigningKey(t *testing.T) {
	// AWS test vector from documentation.
	secretKey := "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	dateStr := "20120215"
	region := "us-east-1"
	svc := "iam"

	signingKey := deriveSigningKey(secretKey, dateStr, region, svc)

	// Known expected value from AWS docs.
	expected := "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"
	got := hex.EncodeToString(signingKey)
	if got != expected {
		t.Errorf("deriveSigningKey = %s, want %s", got, expected)
	}
}

// --- Canonical request tests ---

func TestCanonicalURI(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"", "/"},
		{"/", "/"},
		{"/bucket/key", "/bucket/key"},
		{"/bucket/path/to/object", "/bucket/path/to/object"},
		{"/bucket/key with spaces", "/bucket/key%20with%20spaces"},
		{"/bucket/special%chars", "/bucket/special%25chars"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := canonicalURI(tt.path)
			if got != tt.expected {
				t.Errorf("canonicalURI(%q) = %q, want %q", tt.path, got, tt.expected)
			}
		})
	}
}

func TestCanonicalQueryString(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"empty", "", ""},
		{"single param", "acl=", "acl="},
		{"two params sorted", "prefix=test&delimiter=/", "delimiter=%2F&prefix=test"},
		{"param with no value", "acl", "acl="},
		{"params with special chars", "key=hello%20world&foo=bar", "foo=bar&key=hello%20world"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values, _ := parseQuery(tt.query)
			got := canonicalQueryString(values)
			if got != tt.expected {
				t.Errorf("canonicalQueryString(%q) = %q, want %q", tt.query, got, tt.expected)
			}
		})
	}
}

// parseQuery is a helper that parses query strings including bare keys (e.g., "acl").
func parseQuery(query string) (map[string][]string, error) {
	values := make(map[string][]string)
	if query == "" {
		return values, nil
	}
	for _, part := range strings.Split(query, "&") {
		idx := strings.IndexByte(part, '=')
		if idx < 0 {
			// Bare key with no value.
			key, _ := decodeQueryComponent(part)
			values[key] = append(values[key], "")
		} else {
			key, _ := decodeQueryComponent(part[:idx])
			val, _ := decodeQueryComponent(part[idx+1:])
			values[key] = append(values[key], val)
		}
	}
	return values, nil
}

func decodeQueryComponent(s string) (string, error) {
	s = strings.ReplaceAll(s, "+", " ")
	return url.QueryUnescape(s)
}

// --- Parse Authorization header tests ---

func TestParseAuthorizationHeader(t *testing.T) {
	t.Run("valid header", func(t *testing.T) {
		header := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024"
		parsed, err := parseAuthorizationHeader(header)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if parsed.AccessKeyID != "AKIAIOSFODNN7EXAMPLE" {
			t.Errorf("AccessKeyID = %q, want AKIAIOSFODNN7EXAMPLE", parsed.AccessKeyID)
		}
		if parsed.DateStr != "20130524" {
			t.Errorf("DateStr = %q, want 20130524", parsed.DateStr)
		}
		if parsed.Region != "us-east-1" {
			t.Errorf("Region = %q, want us-east-1", parsed.Region)
		}
		if parsed.Service != "s3" {
			t.Errorf("Service = %q, want s3", parsed.Service)
		}
		if len(parsed.SignedHeaders) != 4 {
			t.Errorf("SignedHeaders count = %d, want 4", len(parsed.SignedHeaders))
		}
		if parsed.Signature != "fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024" {
			t.Errorf("Signature = %q, want fe5f80f...", parsed.Signature)
		}
	})

	t.Run("wrong algorithm", func(t *testing.T) {
		_, err := parseAuthorizationHeader("AWS4-HMAC-SHA512 Credential=test")
		if err == nil {
			t.Error("expected error for wrong algorithm")
		}
	})

	t.Run("missing credential", func(t *testing.T) {
		_, err := parseAuthorizationHeader("AWS4-HMAC-SHA256 SignedHeaders=host, Signature=abc")
		if err == nil {
			t.Error("expected error for missing credential")
		}
	})

	t.Run("invalid credential format", func(t *testing.T) {
		_, err := parseAuthorizationHeader("AWS4-HMAC-SHA256 Credential=AKID/date/region, SignedHeaders=host, Signature=abc")
		if err == nil {
			t.Error("expected error for invalid credential format")
		}
	})
}

// --- DetectAuthMethod tests ---

func TestDetectAuthMethod(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(r *http.Request)
		expected string
	}{
		{
			"no auth",
			func(r *http.Request) {},
			"none",
		},
		{
			"header auth",
			func(r *http.Request) {
				r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=...")
			},
			"header",
		},
		{
			"presigned",
			func(r *http.Request) {
				q := r.URL.Query()
				q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
				r.URL.RawQuery = q.Encode()
			},
			"presigned",
		},
		{
			"ambiguous",
			func(r *http.Request) {
				r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=...")
				q := r.URL.Query()
				q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
				r.URL.RawQuery = q.Encode()
			},
			"ambiguous",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/bucket/key", nil)
			tt.setup(req)
			got := DetectAuthMethod(req)
			if got != tt.expected {
				t.Errorf("DetectAuthMethod = %q, want %q", got, tt.expected)
			}
		})
	}
}

// --- Full VerifyRequest round-trip tests ---

func TestVerifyRequestValidSignature(t *testing.T) {
	store := newTestStore(t)
	seedTestCredential(t, store, "bleepstore", "bleepstore-secret")

	verifier := NewSigV4Verifier(store, "us-east-1")

	req := httptest.NewRequest("GET", "/test-bucket", nil)
	req.Host = "localhost:9011"

	now := time.Now().UTC()
	signRequest(req, "bleepstore", "bleepstore-secret", "us-east-1", now)

	cred, err := verifier.VerifyRequest(req)
	if err != nil {
		t.Fatalf("VerifyRequest failed: %v", err)
	}
	if cred.AccessKeyID != "bleepstore" {
		t.Errorf("AccessKeyID = %q, want bleepstore", cred.AccessKeyID)
	}
}

func TestVerifyRequestWrongSecretKey(t *testing.T) {
	store := newTestStore(t)
	seedTestCredential(t, store, "bleepstore", "the-real-secret")

	verifier := NewSigV4Verifier(store, "us-east-1")

	req := httptest.NewRequest("GET", "/test-bucket", nil)
	req.Host = "localhost:9011"

	now := time.Now().UTC()
	signRequest(req, "bleepstore", "wrong-secret", "us-east-1", now)

	_, err := verifier.VerifyRequest(req)
	if err == nil {
		t.Fatal("expected error for wrong secret key")
	}
	authErr, ok := err.(*AuthError)
	if !ok {
		t.Fatalf("expected *AuthError, got %T", err)
	}
	if authErr.Code != "SignatureDoesNotMatch" {
		t.Errorf("error code = %q, want SignatureDoesNotMatch", authErr.Code)
	}
}

func TestVerifyRequestInvalidAccessKey(t *testing.T) {
	store := newTestStore(t)
	seedTestCredential(t, store, "bleepstore", "bleepstore-secret")

	verifier := NewSigV4Verifier(store, "us-east-1")

	req := httptest.NewRequest("GET", "/test-bucket", nil)
	req.Host = "localhost:9011"

	now := time.Now().UTC()
	signRequest(req, "nonexistent-key", "some-secret", "us-east-1", now)

	_, err := verifier.VerifyRequest(req)
	if err == nil {
		t.Fatal("expected error for invalid access key")
	}
	authErr, ok := err.(*AuthError)
	if !ok {
		t.Fatalf("expected *AuthError, got %T", err)
	}
	if authErr.Code != "InvalidAccessKeyId" {
		t.Errorf("error code = %q, want InvalidAccessKeyId", authErr.Code)
	}
}

func TestVerifyRequestMissingAuthHeader(t *testing.T) {
	store := newTestStore(t)
	verifier := NewSigV4Verifier(store, "us-east-1")

	req := httptest.NewRequest("GET", "/test-bucket", nil)
	req.Host = "localhost:9011"

	_, err := verifier.VerifyRequest(req)
	if err == nil {
		t.Fatal("expected error for missing auth header")
	}
	authErr, ok := err.(*AuthError)
	if !ok {
		t.Fatalf("expected *AuthError, got %T", err)
	}
	if authErr.Code != "AccessDenied" {
		t.Errorf("error code = %q, want AccessDenied", authErr.Code)
	}
}

func TestVerifyRequestClockSkew(t *testing.T) {
	store := newTestStore(t)
	seedTestCredential(t, store, "bleepstore", "bleepstore-secret")

	verifier := NewSigV4Verifier(store, "us-east-1")

	req := httptest.NewRequest("GET", "/test-bucket", nil)
	req.Host = "localhost:9011"

	// Sign with a time 20 minutes in the past (exceeds 15 minute tolerance).
	pastTime := time.Now().UTC().Add(-20 * time.Minute)
	signRequest(req, "bleepstore", "bleepstore-secret", "us-east-1", pastTime)

	_, err := verifier.VerifyRequest(req)
	if err == nil {
		t.Fatal("expected error for clock skew")
	}
	authErr, ok := err.(*AuthError)
	if !ok {
		t.Fatalf("expected *AuthError, got %T", err)
	}
	if authErr.Code != "RequestTimeTooSkewed" {
		t.Errorf("error code = %q, want RequestTimeTooSkewed", authErr.Code)
	}
}

func TestVerifyRequestPutObject(t *testing.T) {
	store := newTestStore(t)
	seedTestCredential(t, store, "bleepstore", "bleepstore-secret")

	verifier := NewSigV4Verifier(store, "us-east-1")

	req := httptest.NewRequest("PUT", "/test-bucket/test-key", strings.NewReader("hello world"))
	req.Host = "localhost:9011"
	req.Header.Set("Content-Type", "text/plain")

	// Compute actual SHA-256 of body (this is what the SDK would normally do).
	bodyHash := sha256.Sum256([]byte("hello world"))
	req.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(bodyHash[:]))

	now := time.Now().UTC()
	signRequest(req, "bleepstore", "bleepstore-secret", "us-east-1", now)

	cred, err := verifier.VerifyRequest(req)
	if err != nil {
		t.Fatalf("VerifyRequest failed: %v", err)
	}
	if cred.AccessKeyID != "bleepstore" {
		t.Errorf("AccessKeyID = %q, want bleepstore", cred.AccessKeyID)
	}
}

func TestVerifyRequestWithQueryParams(t *testing.T) {
	store := newTestStore(t)
	seedTestCredential(t, store, "bleepstore", "bleepstore-secret")

	verifier := NewSigV4Verifier(store, "us-east-1")

	req := httptest.NewRequest("GET", "/test-bucket?list-type=2&prefix=photos/&delimiter=/", nil)
	req.Host = "localhost:9011"

	now := time.Now().UTC()
	signRequest(req, "bleepstore", "bleepstore-secret", "us-east-1", now)

	cred, err := verifier.VerifyRequest(req)
	if err != nil {
		t.Fatalf("VerifyRequest failed: %v", err)
	}
	if cred.AccessKeyID != "bleepstore" {
		t.Errorf("AccessKeyID = %q, want bleepstore", cred.AccessKeyID)
	}
}

// --- Presigned URL tests ---

func TestVerifyPresignedValid(t *testing.T) {
	store := newTestStore(t)
	seedTestCredential(t, store, "bleepstore", "bleepstore-secret")

	verifier := NewSigV4Verifier(store, "us-east-1")

	now := time.Now().UTC()
	amzDate := now.Format(amzDateFormat)
	dateStr := now.Format(amzDateShort)
	region := "us-east-1"
	expires := "3600"

	credential := fmt.Sprintf("%s/%s/%s/%s/%s", "bleepstore", dateStr, region, service, scopeTerminator)
	signedHeaders := "host"

	// Build the URL with all presigned params except signature.
	rawURL := fmt.Sprintf("/test-bucket/test-key?X-Amz-Algorithm=%s&X-Amz-Credential=%s&X-Amz-Date=%s&X-Amz-Expires=%s&X-Amz-SignedHeaders=%s",
		algorithm,
		strings.ReplaceAll(credential, "/", "%2F"),
		amzDate,
		expires,
		signedHeaders,
	)

	req := httptest.NewRequest("GET", rawURL, nil)
	req.Host = "localhost:9011"

	// Build canonical request for presigned.
	signedHeadersList := []string{"host"}
	canonReq := buildPresignedCanonicalRequest(req, signedHeadersList)
	scope := fmt.Sprintf("%s/%s/%s/%s", dateStr, region, service, scopeTerminator)
	strToSign := buildStringToSign(amzDate, scope, canonReq)
	signingKey := deriveSigningKey("bleepstore-secret", dateStr, region, service)
	signature := hex.EncodeToString(hmacSHA256(signingKey, strToSign))

	// Add the signature to the URL.
	q := req.URL.Query()
	q.Set("X-Amz-Signature", signature)
	req.URL.RawQuery = q.Encode()

	cred, err := verifier.VerifyPresigned(req)
	if err != nil {
		t.Fatalf("VerifyPresigned failed: %v", err)
	}
	if cred.AccessKeyID != "bleepstore" {
		t.Errorf("AccessKeyID = %q, want bleepstore", cred.AccessKeyID)
	}
}

func TestVerifyPresignedExpired(t *testing.T) {
	store := newTestStore(t)
	seedTestCredential(t, store, "bleepstore", "bleepstore-secret")

	verifier := NewSigV4Verifier(store, "us-east-1")

	// Use a time 2 hours in the past with 1-second expiry.
	pastTime := time.Now().UTC().Add(-2 * time.Hour)
	amzDate := pastTime.Format(amzDateFormat)
	dateStr := pastTime.Format(amzDateShort)
	region := "us-east-1"
	expires := "1"

	credential := fmt.Sprintf("%s/%s/%s/%s/%s", "bleepstore", dateStr, region, service, scopeTerminator)

	rawURL := fmt.Sprintf("/test-bucket/test-key?X-Amz-Algorithm=%s&X-Amz-Credential=%s&X-Amz-Date=%s&X-Amz-Expires=%s&X-Amz-SignedHeaders=host&X-Amz-Signature=dummysig",
		algorithm,
		strings.ReplaceAll(credential, "/", "%2F"),
		amzDate,
		expires,
	)

	req := httptest.NewRequest("GET", rawURL, nil)
	req.Host = "localhost:9011"

	_, err := verifier.VerifyPresigned(req)
	if err == nil {
		t.Fatal("expected error for expired presigned URL")
	}
	authErr, ok := err.(*AuthError)
	if !ok {
		t.Fatalf("expected *AuthError, got %T", err)
	}
	if authErr.Code != "AccessDenied" {
		t.Errorf("error code = %q, want AccessDenied", authErr.Code)
	}
}

func TestVerifyPresignedInvalidExpires(t *testing.T) {
	store := newTestStore(t)
	seedTestCredential(t, store, "bleepstore", "bleepstore-secret")

	verifier := NewSigV4Verifier(store, "us-east-1")

	now := time.Now().UTC()
	amzDate := now.Format(amzDateFormat)
	dateStr := now.Format(amzDateShort)
	credential := fmt.Sprintf("%s/%s/%s/%s/%s", "bleepstore", dateStr, "us-east-1", service, scopeTerminator)

	// Expires > 604800 (7 days).
	rawURL := fmt.Sprintf("/test-bucket/test-key?X-Amz-Algorithm=%s&X-Amz-Credential=%s&X-Amz-Date=%s&X-Amz-Expires=700000&X-Amz-SignedHeaders=host&X-Amz-Signature=dummy",
		algorithm,
		strings.ReplaceAll(credential, "/", "%2F"),
		amzDate,
	)

	req := httptest.NewRequest("GET", rawURL, nil)
	req.Host = "localhost:9011"

	_, err := verifier.VerifyPresigned(req)
	if err == nil {
		t.Fatal("expected error for invalid expires")
	}
}

// --- OwnerFromContext tests ---

func TestOwnerFromContext(t *testing.T) {
	ctx := context.Background()

	// Empty context.
	ownerID, display := OwnerFromContext(ctx)
	if ownerID != "" || display != "" {
		t.Errorf("empty context: ownerID=%q, display=%q", ownerID, display)
	}

	// With owner set.
	ctx = contextWithOwner(ctx, "testowner", "Test Owner")
	ownerID, display = OwnerFromContext(ctx)
	if ownerID != "testowner" {
		t.Errorf("ownerID = %q, want testowner", ownerID)
	}
	if display != "Test Owner" {
		t.Errorf("display = %q, want Test Owner", display)
	}
}

// --- buildStringToSign test ---

func TestBuildStringToSign(t *testing.T) {
	amzDate := "20130524T000000Z"
	scope := "20130524/us-east-1/s3/aws4_request"
	canonicalRequest := "GET\n/\n\nhost:examplebucket.s3.amazonaws.com\nrange:bytes=0-9\nx-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\nx-amz-date:20130524T000000Z\n\nhost;range;x-amz-content-sha256;x-amz-date\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	result := buildStringToSign(amzDate, scope, canonicalRequest)

	lines := strings.Split(result, "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines, got %d", len(lines))
	}
	if lines[0] != algorithm {
		t.Errorf("line 0 = %q, want %q", lines[0], algorithm)
	}
	if lines[1] != amzDate {
		t.Errorf("line 1 = %q, want %q", lines[1], amzDate)
	}
	if lines[2] != scope {
		t.Errorf("line 2 = %q, want %q", lines[2], scope)
	}
	// Line 3 should be the hex-encoded SHA-256 of the canonical request.
	expectedHash := sha256.Sum256([]byte(canonicalRequest))
	if lines[3] != hex.EncodeToString(expectedHash[:]) {
		t.Errorf("line 3 hash mismatch")
	}
}

// --- Multiple credential support ---

func TestVerifyRequestMultipleCredentials(t *testing.T) {
	store := newTestStore(t)
	seedTestCredential(t, store, "user1", "secret1")
	seedTestCredential(t, store, "user2", "secret2")

	verifier := NewSigV4Verifier(store, "us-east-1")

	// Request signed by user2.
	req := httptest.NewRequest("GET", "/my-bucket", nil)
	req.Host = "localhost:9011"
	now := time.Now().UTC()
	signRequest(req, "user2", "secret2", "us-east-1", now)

	cred, err := verifier.VerifyRequest(req)
	if err != nil {
		t.Fatalf("VerifyRequest failed: %v", err)
	}
	if cred.AccessKeyID != "user2" {
		t.Errorf("AccessKeyID = %q, want user2", cred.AccessKeyID)
	}
	if cred.OwnerID != "user2" {
		t.Errorf("OwnerID = %q, want user2", cred.OwnerID)
	}
}

// --- Canonical headers tests ---

func TestCanonicalHeaders(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "localhost:9011"
	req.Header.Set("X-Amz-Date", "20260223T120000Z")
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	req.Header.Set("Content-Type", "application/octet-stream")

	signedHeaders := []string{"content-type", "host", "x-amz-content-sha256", "x-amz-date"}
	result := canonicalHeaders(req, signedHeaders)

	// Each header followed by \n.
	lines := strings.Split(result, "\n")
	// Last element is empty string after trailing \n.
	if len(lines) < 5 {
		t.Fatalf("expected at least 5 lines (4 headers + empty), got %d", len(lines))
	}
	if !strings.HasPrefix(lines[0], "content-type:") {
		t.Errorf("line 0 = %q, expected content-type:", lines[0])
	}
	if !strings.HasPrefix(lines[1], "host:localhost:9011") {
		t.Errorf("line 1 = %q, expected host:localhost:9011", lines[1])
	}
}
