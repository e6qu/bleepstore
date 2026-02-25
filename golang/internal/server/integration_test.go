// Package server contains integration tests that start a full in-process BleepStore
// server and run HTTP requests against it.
package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bleepstore/bleepstore/internal/config"
	"github.com/bleepstore/bleepstore/internal/metadata"
	"github.com/bleepstore/bleepstore/internal/storage"
)

// integrationServer is a helper struct that holds a running test server instance.
type integrationServer struct {
	srv      *Server
	addr     string
	endpoint string
	tmpDir   string
	meta     *metadata.SQLiteStore
}

// newIntegrationServer creates and starts a full BleepStore server with temporary
// data directories for integration testing.
func newIntegrationServer(t *testing.T) *integrationServer {
	t.Helper()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "metadata.db")
	storageDir := filepath.Join(tmpDir, "objects")
	os.MkdirAll(storageDir, 0o755)

	cfg := &config.Config{
		Server: config.ServerConfig{
			Host:   "127.0.0.1",
			Port:   0,
			Region: "us-east-1",
		},
		Auth: config.AuthConfig{
			AccessKey: "bleepstore",
			SecretKey: "bleepstore-secret",
		},
		Metadata: config.MetadataConfig{
			Engine: "sqlite",
			SQLite: config.SQLiteConfig{Path: dbPath},
		},
		Storage: config.StorageConfig{
			Backend: "local",
			Local:   config.LocalConfig{RootDir: storageDir},
		},
		Observability: config.ObservabilityConfig{
			Metrics:     true,
			HealthCheck: true,
		},
	}

	metaStore, err := metadata.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("creating metadata store: %v", err)
	}

	// Seed credentials
	cred := &metadata.CredentialRecord{
		AccessKeyID: cfg.Auth.AccessKey,
		SecretKey:   cfg.Auth.SecretKey,
		OwnerID:     cfg.Auth.AccessKey,
		DisplayName: cfg.Auth.AccessKey,
		Active:      true,
		CreatedAt:   time.Now().UTC(),
	}
	if err := metaStore.PutCredential(context.Background(), cred); err != nil {
		t.Fatalf("seeding credentials: %v", err)
	}

	storageBackend, err := storage.NewLocalBackend(storageDir)
	if err != nil {
		t.Fatalf("creating storage backend: %v", err)
	}

	srv, err := New(cfg, metaStore, WithStorageBackend(storageBackend))
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}

	// Find a free port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding free port: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()

	// Start the server in a goroutine
	go func() {
		srv.ListenAndServe(addr)
	}()

	// Wait for the server to be ready
	endpoint := "http://" + addr
	for i := 0; i < 50; i++ {
		resp, err := http.Get(endpoint + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		metaStore.Close()
	})

	return &integrationServer{
		srv:      srv,
		addr:     addr,
		endpoint: endpoint,
		tmpDir:   tmpDir,
		meta:     metaStore,
	}
}

// intCanonicalQueryString builds a sorted, URI-encoded query string for signing.
func intCanonicalQueryString(values url.Values) string {
	if len(values) == 0 {
		return ""
	}
	var pairs []string
	for key, vals := range values {
		for _, val := range vals {
			pairs = append(pairs, url.QueryEscape(key)+"="+url.QueryEscape(val))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

// signedRequest creates a SigV4-signed HTTP request for the test server.
func (ts *integrationServer) signedRequest(method, path string, body []byte) (*http.Request, error) {
	reqURL := ts.endpoint + path
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, reqURL, bodyReader)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStr := now.Format("20060102")

	payloadHash := intSha256Hex(body)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("Host", ts.addr)
	if body != nil {
		req.Header.Set("Content-Length", fmt.Sprintf("%d", len(body)))
	}

	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	sort.Strings(signedHeaders)

	var canonReq strings.Builder
	canonReq.WriteString(method)
	canonReq.WriteByte('\n')
	canonReq.WriteString(intURIEncode(req.URL.Path))
	canonReq.WriteByte('\n')
	canonReq.WriteString(intCanonicalQueryString(req.URL.Query()))
	canonReq.WriteByte('\n')

	for _, h := range signedHeaders {
		canonReq.WriteString(h)
		canonReq.WriteByte(':')
		if h == "host" {
			canonReq.WriteString(ts.addr)
		} else {
			canonReq.WriteString(req.Header.Get(http.CanonicalHeaderKey(h)))
		}
		canonReq.WriteByte('\n')
	}
	canonReq.WriteByte('\n')
	canonReq.WriteString(strings.Join(signedHeaders, ";"))
	canonReq.WriteByte('\n')
	canonReq.WriteString(payloadHash)

	scope := fmt.Sprintf("%s/us-east-1/s3/aws4_request", dateStr)
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + intSha256Hex([]byte(canonReq.String()))

	signingKey := intHmacSHA256([]byte("AWS4bleepstore-secret"), dateStr)
	signingKey = intHmacSHA256(signingKey, "us-east-1")
	signingKey = intHmacSHA256(signingKey, "s3")
	signingKey = intHmacSHA256(signingKey, "aws4_request")

	signature := hex.EncodeToString(intHmacSHA256(signingKey, stringToSign))

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=bleepstore/%s/us-east-1/s3/aws4_request, SignedHeaders=%s, Signature=%s",
		dateStr, strings.Join(signedHeaders, ";"), signature)
	req.Header.Set("Authorization", authHeader)

	return req, nil
}

func intSha256Hex(data []byte) string {
	if data == nil {
		data = []byte{}
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func intHmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func intURIEncode(path string) string {
	segments := strings.Split(path, "/")
	for i, seg := range segments {
		var sb strings.Builder
		for j := 0; j < len(seg); j++ {
			c := seg[j]
			if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
				c == '-' || c == '_' || c == '.' || c == '~' {
				sb.WriteByte(c)
			} else {
				fmt.Fprintf(&sb, "%%%02X", c)
			}
		}
		segments[i] = sb.String()
	}
	return strings.Join(segments, "/")
}

// doSigned is a convenience that signs and executes the request.
func (ts *integrationServer) doSigned(t *testing.T, method, path string, body []byte) *http.Response {
	t.Helper()
	req, err := ts.signedRequest(method, path, body)
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("executing request %s %s: %v", method, path, err)
	}
	return resp
}

// doSignedWithHeaders signs and executes with extra headers.
func (ts *integrationServer) doSignedWithHeaders(t *testing.T, method, path string, body []byte, headers map[string]string) *http.Response {
	t.Helper()

	reqURL := ts.endpoint + path
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, reqURL, bodyReader)
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStr := now.Format("20060102")

	payloadHash := intSha256Hex(body)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("Host", ts.addr)

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	signedHeaderNames := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	for k := range headers {
		lower := strings.ToLower(k)
		signedHeaderNames = append(signedHeaderNames, lower)
	}
	sort.Strings(signedHeaderNames)

	unique := signedHeaderNames[:0]
	seen := map[string]bool{}
	for _, h := range signedHeaderNames {
		if !seen[h] {
			seen[h] = true
			unique = append(unique, h)
		}
	}
	signedHeaderNames = unique

	var canonReq strings.Builder
	canonReq.WriteString(method)
	canonReq.WriteByte('\n')
	canonReq.WriteString(intURIEncode(req.URL.Path))
	canonReq.WriteByte('\n')
	canonReq.WriteString(intCanonicalQueryString(req.URL.Query()))
	canonReq.WriteByte('\n')

	for _, h := range signedHeaderNames {
		canonReq.WriteString(h)
		canonReq.WriteByte(':')
		if h == "host" {
			canonReq.WriteString(ts.addr)
		} else {
			canonReq.WriteString(req.Header.Get(http.CanonicalHeaderKey(h)))
		}
		canonReq.WriteByte('\n')
	}
	canonReq.WriteByte('\n')
	canonReq.WriteString(strings.Join(signedHeaderNames, ";"))
	canonReq.WriteByte('\n')
	canonReq.WriteString(payloadHash)

	scope := fmt.Sprintf("%s/us-east-1/s3/aws4_request", dateStr)
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + intSha256Hex([]byte(canonReq.String()))

	signingKey := intHmacSHA256([]byte("AWS4bleepstore-secret"), dateStr)
	signingKey = intHmacSHA256(signingKey, "us-east-1")
	signingKey = intHmacSHA256(signingKey, "s3")
	signingKey = intHmacSHA256(signingKey, "aws4_request")

	signature := hex.EncodeToString(intHmacSHA256(signingKey, stringToSign))

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=bleepstore/%s/us-east-1/s3/aws4_request, SignedHeaders=%s, Signature=%s",
		dateStr, strings.Join(signedHeaderNames, ";"), signature)
	req.Header.Set("Authorization", authHeader)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("executing request %s %s: %v", method, path, err)
	}
	return resp
}

func intReadBody(resp *http.Response) string {
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return string(data)
}

func intReadBodyBytes(resp *http.Response) []byte {
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return data
}

// --- Integration Tests ---

func TestIntegrationHealth(t *testing.T) {
	ts := newIntegrationServer(t)
	resp, err := http.Get(ts.endpoint + "/health")
	if err != nil {
		t.Fatalf("health check: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("health status = %d, want 200", resp.StatusCode)
	}
}

func TestIntegrationBucketCRUD(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-bucket-crud"

	resp := ts.doSigned(t, "PUT", "/"+bucket, nil)
	if resp.StatusCode != 200 {
		t.Errorf("CreateBucket status = %d, want 200: %s", resp.StatusCode, intReadBody(resp))
	} else {
		resp.Body.Close()
	}

	resp = ts.doSigned(t, "HEAD", "/"+bucket, nil)
	if resp.StatusCode != 200 {
		t.Errorf("HeadBucket status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	resp = ts.doSigned(t, "GET", "/", nil)
	body := intReadBody(resp)
	if resp.StatusCode != 200 {
		t.Errorf("ListBuckets status = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(body, bucket) {
		t.Errorf("ListBuckets does not contain bucket %q", bucket)
	}

	resp = ts.doSigned(t, "GET", "/"+bucket+"?location", nil)
	if resp.StatusCode != 200 {
		t.Errorf("GetBucketLocation status = %d, want 200: %s", resp.StatusCode, intReadBody(resp))
	} else {
		resp.Body.Close()
	}

	resp = ts.doSigned(t, "DELETE", "/"+bucket, nil)
	if resp.StatusCode != 204 {
		t.Errorf("DeleteBucket status = %d, want 204: %s", resp.StatusCode, intReadBody(resp))
	} else {
		resp.Body.Close()
	}

	resp = ts.doSigned(t, "HEAD", "/"+bucket, nil)
	if resp.StatusCode != 404 {
		t.Errorf("HeadBucket after delete status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestIntegrationPutGetObject(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-object-crud"
	key := "hello.txt"
	body := []byte("Hello, BleepStore!")

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	resp := ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/"+key, body, map[string]string{
		"Content-Type": "text/plain",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("PutObject status = %d, want 200: %s", resp.StatusCode, intReadBody(resp))
	}
	etag := resp.Header.Get("ETag")
	resp.Body.Close()

	expectedMD5 := fmt.Sprintf(`"%x"`, md5.Sum(body))
	if etag != expectedMD5 {
		t.Errorf("PutObject ETag = %q, want %q", etag, expectedMD5)
	}

	resp = ts.doSigned(t, "GET", "/"+bucket+"/"+key, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("GetObject status = %d, want 200: %s", resp.StatusCode, intReadBody(resp))
	}
	gotBody := intReadBodyBytes(resp)
	if !bytes.Equal(gotBody, body) {
		t.Errorf("GetObject body = %q, want %q", gotBody, body)
	}

	resp = ts.doSigned(t, "HEAD", "/"+bucket+"/"+key, nil)
	if resp.StatusCode != 200 {
		t.Errorf("HeadObject status = %d, want 200", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "text/plain" {
		t.Errorf("HeadObject Content-Type = %q, want text/plain", resp.Header.Get("Content-Type"))
	}
	if resp.Header.Get("Accept-Ranges") != "bytes" {
		t.Errorf("HeadObject Accept-Ranges = %q, want bytes", resp.Header.Get("Accept-Ranges"))
	}
	resp.Body.Close()

	resp = ts.doSigned(t, "DELETE", "/"+bucket+"/"+key, nil)
	if resp.StatusCode != 204 {
		t.Errorf("DeleteObject status = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()

	resp = ts.doSigned(t, "GET", "/"+bucket+"/"+key, nil)
	if resp.StatusCode != 404 {
		t.Errorf("GetObject after delete status = %d, want 404: %s", resp.StatusCode, intReadBody(resp))
	} else {
		resp.Body.Close()
	}

	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationKeyTooLong(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-key-too-long"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	longKey := strings.Repeat("k", 1025)
	resp := ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/"+longKey, []byte("data"), map[string]string{
		"Content-Type": "application/octet-stream",
	})
	if resp.StatusCode != 400 {
		t.Errorf("PutObject with long key status = %d, want 400: %s", resp.StatusCode, intReadBody(resp))
	} else {
		body := intReadBody(resp)
		if !strings.Contains(body, "KeyTooLongError") {
			t.Errorf("Expected KeyTooLongError in response: %s", body)
		}
	}

	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationConditionalRequests(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-conditional"
	body := []byte("conditional test")

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()
	resp := ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/cond.txt", body, map[string]string{
		"Content-Type": "text/plain",
	})
	etag := resp.Header.Get("ETag")
	resp.Body.Close()

	resp = ts.doSignedWithHeaders(t, "GET", "/"+bucket+"/cond.txt", nil, map[string]string{
		"If-Match": etag,
	})
	if resp.StatusCode != 200 {
		t.Errorf("If-Match correct status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	resp = ts.doSignedWithHeaders(t, "GET", "/"+bucket+"/cond.txt", nil, map[string]string{
		"If-Match": `"wrong-etag"`,
	})
	if resp.StatusCode != 412 {
		t.Errorf("If-Match wrong status = %d, want 412", resp.StatusCode)
	}
	resp.Body.Close()

	resp = ts.doSignedWithHeaders(t, "GET", "/"+bucket+"/cond.txt", nil, map[string]string{
		"If-None-Match": etag,
	})
	if resp.StatusCode != 304 {
		t.Errorf("If-None-Match matching status = %d, want 304", resp.StatusCode)
	}
	resp.Body.Close()

	ts.doSigned(t, "DELETE", "/"+bucket+"/cond.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationRangeRequest(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-range"
	body := []byte("0123456789ABCDEF")

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()
	ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/range.txt", body, map[string]string{
		"Content-Type": "text/plain",
	}).Body.Close()

	resp := ts.doSignedWithHeaders(t, "GET", "/"+bucket+"/range.txt", nil, map[string]string{
		"Range": "bytes=0-4",
	})
	if resp.StatusCode != 206 {
		t.Errorf("Range request status = %d, want 206: %s", resp.StatusCode, intReadBody(resp))
	} else {
		gotBody := intReadBodyBytes(resp)
		if string(gotBody) != "01234" {
			t.Errorf("Range body = %q, want %q", gotBody, "01234")
		}
	}

	ts.doSigned(t, "DELETE", "/"+bucket+"/range.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationMultipartUpload(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-multipart"
	key := "multipart.bin"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	resp := ts.doSignedWithHeaders(t, "POST", "/"+bucket+"/"+key+"?uploads", nil, map[string]string{
		"Content-Type": "application/octet-stream",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("CreateMultipartUpload status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	initBody := intReadBody(resp)

	type InitResult struct {
		UploadID string `xml:"UploadId"`
	}
	var initResult InitResult
	xml.Unmarshal([]byte(initBody), &initResult)
	uploadID := initResult.UploadID
	if uploadID == "" {
		t.Fatalf("Empty upload ID: %s", initBody)
	}

	part1Data := bytes.Repeat([]byte("A"), 5*1024*1024)
	resp = ts.doSignedWithHeaders(t, "PUT",
		fmt.Sprintf("/%s/%s?partNumber=1&uploadId=%s", bucket, key, uploadID),
		part1Data, map[string]string{
			"Content-Type": "application/octet-stream",
		})
	if resp.StatusCode != 200 {
		t.Fatalf("UploadPart 1 status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	part1ETag := resp.Header.Get("ETag")
	resp.Body.Close()

	part2Data := bytes.Repeat([]byte("B"), 1024)
	resp = ts.doSignedWithHeaders(t, "PUT",
		fmt.Sprintf("/%s/%s?partNumber=2&uploadId=%s", bucket, key, uploadID),
		part2Data, map[string]string{
			"Content-Type": "application/octet-stream",
		})
	if resp.StatusCode != 200 {
		t.Fatalf("UploadPart 2 status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	part2ETag := resp.Header.Get("ETag")
	resp.Body.Close()

	completeXML := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, part1ETag, part2ETag)

	resp = ts.doSignedWithHeaders(t, "POST",
		fmt.Sprintf("/%s/%s?uploadId=%s", bucket, key, uploadID),
		[]byte(completeXML), map[string]string{
			"Content-Type": "application/xml",
		})
	if resp.StatusCode != 200 {
		t.Fatalf("CompleteMultipartUpload status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	completeBody := intReadBody(resp)
	if !strings.Contains(completeBody, "CompleteMultipartUploadResult") {
		t.Errorf("Response should contain result: %s", completeBody)
	}

	resp = ts.doSigned(t, "GET", "/"+bucket+"/"+key, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("GetObject assembled status = %d", resp.StatusCode)
	}
	assembled := intReadBodyBytes(resp)
	expected := append(part1Data, part2Data...)
	if !bytes.Equal(assembled, expected) {
		t.Errorf("Assembled object size = %d, want %d", len(assembled), len(expected))
	}

	ts.doSigned(t, "DELETE", "/"+bucket+"/"+key, nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationXMLNamespaces(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-xml-ns"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	resp := ts.doSigned(t, "GET", "/", nil)
	body := intReadBody(resp)
	if !strings.Contains(body, `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`) {
		t.Errorf("ListBuckets should have S3 xmlns: %s", body)
	}

	resp = ts.doSigned(t, "GET", "/nonexistent-bucket-xyz123?list-type=2", nil)
	body = intReadBody(resp)
	if strings.Contains(body, "xmlns") {
		t.Errorf("Error response should NOT have xmlns: %s", body)
	}

	if resp.Header.Get("Content-Type") != "application/xml" {
		t.Errorf("Error Content-Type = %q, want application/xml", resp.Header.Get("Content-Type"))
	}

	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationCommonHeaders(t *testing.T) {
	ts := newIntegrationServer(t)

	resp := ts.doSigned(t, "GET", "/", nil)
	resp.Body.Close()

	if resp.Header.Get("x-amz-request-id") == "" {
		t.Error("Missing x-amz-request-id header")
	}
	if resp.Header.Get("Server") != "BleepStore" {
		t.Errorf("Server header = %q, want BleepStore", resp.Header.Get("Server"))
	}
	if resp.Header.Get("Date") == "" {
		t.Error("Missing Date header")
	}
}

func TestIntegrationErrorResponses(t *testing.T) {
	ts := newIntegrationServer(t)

	resp := ts.doSigned(t, "GET", "/nonexistent-bucket-xyz123?list-type=2", nil)
	body := intReadBody(resp)
	if resp.StatusCode != 404 {
		t.Errorf("NoSuchBucket status = %d, want 404: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "NoSuchBucket") {
		t.Errorf("Expected NoSuchBucket in response: %s", body)
	}

	if resp.Header.Get("x-amz-request-id") == "" {
		t.Error("Error response missing x-amz-request-id header")
	}
}

func TestIntegrationCopyObject(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-copy"
	srcBody := []byte("copy me please")

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	// Put source object
	resp := ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/original.txt", srcBody, map[string]string{
		"Content-Type":   "text/plain",
		"x-amz-meta-foo": "bar",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("PutObject status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	resp.Body.Close()

	// Copy object (COPY metadata directive - default)
	resp = ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/copy.txt", nil, map[string]string{
		"X-Amz-Copy-Source": "/" + bucket + "/original.txt",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("CopyObject status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	copyBody := intReadBody(resp)
	if !strings.Contains(copyBody, "CopyObjectResult") {
		t.Errorf("CopyObject response missing CopyObjectResult: %s", copyBody)
	}

	// Verify copy content
	resp = ts.doSigned(t, "GET", "/"+bucket+"/copy.txt", nil)
	gotBody := intReadBodyBytes(resp)
	if !bytes.Equal(gotBody, srcBody) {
		t.Errorf("Copied body = %q, want %q", gotBody, srcBody)
	}

	// Copy with REPLACE metadata directive
	resp = ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/replaced.txt", nil, map[string]string{
		"X-Amz-Copy-Source":        "/" + bucket + "/original.txt",
		"x-amz-metadata-directive": "REPLACE",
		"Content-Type":             "text/csv",
		"x-amz-meta-replaced":      "true",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("CopyObject REPLACE status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	resp.Body.Close()

	// Verify replaced metadata
	resp = ts.doSigned(t, "HEAD", "/"+bucket+"/replaced.txt", nil)
	if resp.Header.Get("Content-Type") != "text/csv" {
		t.Errorf("Replaced Content-Type = %q, want text/csv", resp.Header.Get("Content-Type"))
	}
	if resp.Header.Get("x-amz-meta-replaced") != "true" {
		t.Errorf("Replaced metadata missing x-amz-meta-replaced")
	}
	resp.Body.Close()

	// Copy nonexistent source
	resp = ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/dst.txt", nil, map[string]string{
		"X-Amz-Copy-Source": "/" + bucket + "/nonexistent.txt",
	})
	if resp.StatusCode != 404 {
		t.Errorf("Copy nonexistent source status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	ts.doSigned(t, "DELETE", "/"+bucket+"/original.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket+"/copy.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket+"/replaced.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationDeleteObjects(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-delete-objects"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	// Create 3 objects
	for _, key := range []string{"a.txt", "b.txt", "c.txt"} {
		ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/"+key, []byte("data"), map[string]string{
			"Content-Type": "text/plain",
		}).Body.Close()
	}

	// Delete multiple objects
	deleteXML := `<Delete><Object><Key>a.txt</Key></Object><Object><Key>b.txt</Key></Object><Object><Key>c.txt</Key></Object></Delete>`
	resp := ts.doSignedWithHeaders(t, "POST", "/"+bucket+"?delete", []byte(deleteXML), map[string]string{
		"Content-Type": "application/xml",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("DeleteObjects status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	body := intReadBody(resp)
	if !strings.Contains(body, "a.txt") || !strings.Contains(body, "b.txt") || !strings.Contains(body, "c.txt") {
		t.Errorf("DeleteObjects response missing deleted keys: %s", body)
	}

	// Verify objects deleted
	for _, key := range []string{"a.txt", "b.txt", "c.txt"} {
		resp = ts.doSigned(t, "GET", "/"+bucket+"/"+key, nil)
		if resp.StatusCode != 404 {
			t.Errorf("Object %s should be deleted, got status %d", key, resp.StatusCode)
		}
		resp.Body.Close()
	}

	// Test quiet mode
	ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/quiet.txt", []byte("data"), map[string]string{
		"Content-Type": "text/plain",
	}).Body.Close()

	quietXML := `<Delete><Quiet>true</Quiet><Object><Key>quiet.txt</Key></Object></Delete>`
	resp = ts.doSignedWithHeaders(t, "POST", "/"+bucket+"?delete", []byte(quietXML), map[string]string{
		"Content-Type": "application/xml",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("DeleteObjects quiet status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	quietBody := intReadBody(resp)
	// In quiet mode, successful deletes should not be listed
	if strings.Contains(quietBody, "quiet.txt") {
		t.Errorf("Quiet mode should not list deleted keys: %s", quietBody)
	}

	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationListObjectsV2WithPrefixDelimiter(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-list-v2"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	// Create test objects
	objects := map[string][]byte{
		"file1.txt":         []byte("f1"),
		"file2.txt":         []byte("f2"),
		"photos/cat.jpg":    []byte("cat"),
		"photos/dog.jpg":    []byte("dog"),
		"photos/2024/a.jpg": []byte("a"),
		"photos/2024/b.jpg": []byte("b"),
		"docs/readme.md":    []byte("readme"),
		"docs/notes.md":     []byte("notes"),
	}
	for key, body := range objects {
		ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/"+key, body, map[string]string{
			"Content-Type": "application/octet-stream",
		}).Body.Close()
	}

	// List all objects
	resp := ts.doSigned(t, "GET", "/"+bucket+"?list-type=2", nil)
	body := intReadBody(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("ListV2 status = %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "<KeyCount>8</KeyCount>") {
		t.Errorf("Expected 8 keys: %s", body)
	}

	// List with prefix
	resp = ts.doSigned(t, "GET", "/"+bucket+"?list-type=2&prefix=photos/", nil)
	body = intReadBody(resp)
	if !strings.Contains(body, "<KeyCount>4</KeyCount>") {
		t.Errorf("Expected 4 keys with photos/ prefix: %s", body)
	}

	// List with delimiter
	resp = ts.doSigned(t, "GET", "/"+bucket+"?list-type=2&delimiter=/", nil)
	body = intReadBody(resp)
	if !strings.Contains(body, "file1.txt") || !strings.Contains(body, "file2.txt") {
		t.Errorf("Top-level files missing: %s", body)
	}
	if !strings.Contains(body, "<Prefix>photos/</Prefix>") {
		t.Errorf("Missing photos/ common prefix: %s", body)
	}
	if !strings.Contains(body, "<Prefix>docs/</Prefix>") {
		t.Errorf("Missing docs/ common prefix: %s", body)
	}

	// List with max-keys pagination
	resp = ts.doSigned(t, "GET", "/"+bucket+"?list-type=2&max-keys=2", nil)
	body = intReadBody(resp)
	if !strings.Contains(body, "<IsTruncated>true</IsTruncated>") {
		t.Errorf("Expected IsTruncated=true: %s", body)
	}
	if !strings.Contains(body, "<MaxKeys>2</MaxKeys>") {
		t.Errorf("Expected MaxKeys=2: %s", body)
	}
	if !strings.Contains(body, "<KeyCount>2</KeyCount>") {
		t.Errorf("Expected KeyCount=2: %s", body)
	}
	if !strings.Contains(body, "NextContinuationToken") {
		t.Errorf("Expected NextContinuationToken: %s", body)
	}

	// List empty result with start-after
	resp = ts.doSigned(t, "GET", "/"+bucket+"?list-type=2&start-after=zzz", nil)
	body = intReadBody(resp)
	if !strings.Contains(body, "<KeyCount>0</KeyCount>") {
		t.Errorf("Expected 0 keys after zzz: %s", body)
	}

	// StorageClass should be present in listings
	resp = ts.doSigned(t, "GET", "/"+bucket+"?list-type=2&prefix=file1.txt", nil)
	body = intReadBody(resp)
	if !strings.Contains(body, "<StorageClass>STANDARD</StorageClass>") {
		t.Errorf("Expected STANDARD StorageClass in listing: %s", body)
	}

	// Cleanup
	for key := range objects {
		ts.doSigned(t, "DELETE", "/"+bucket+"/"+key, nil).Body.Close()
	}
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationListObjectsV1(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-list-v1"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("obj-%03d.txt", i)
		ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/"+key, []byte("data"), map[string]string{
			"Content-Type": "text/plain",
		}).Body.Close()
	}

	// V1 list (no list-type parameter)
	resp := ts.doSigned(t, "GET", "/"+bucket, nil)
	body := intReadBody(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("ListV1 status = %d: %s", resp.StatusCode, body)
	}
	// V1 response should be ListBucketResult, not ListBucketV2Result
	if !strings.Contains(body, "ListBucketResult") {
		t.Errorf("Expected ListBucketResult in V1 response: %s", body)
	}

	// V1 pagination with MaxKeys
	resp = ts.doSigned(t, "GET", "/"+bucket+"?max-keys=2", nil)
	body = intReadBody(resp)
	if !strings.Contains(body, "<IsTruncated>true</IsTruncated>") {
		t.Errorf("V1 pagination should be truncated: %s", body)
	}

	// Cleanup
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("obj-%03d.txt", i)
		ts.doSigned(t, "DELETE", "/"+bucket+"/"+key, nil).Body.Close()
	}
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationObjectUserMetadata(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-metadata"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	// Put object with user metadata
	resp := ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/meta.txt", []byte("data"), map[string]string{
		"Content-Type":       "text/plain",
		"x-amz-meta-author":  "tester",
		"x-amz-meta-version": "1.0",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("PutObject status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	resp.Body.Close()

	// HEAD should return user metadata
	resp = ts.doSigned(t, "HEAD", "/"+bucket+"/meta.txt", nil)
	if resp.Header.Get("x-amz-meta-author") != "tester" {
		t.Errorf("Missing x-amz-meta-author: got %q", resp.Header.Get("x-amz-meta-author"))
	}
	if resp.Header.Get("x-amz-meta-version") != "1.0" {
		t.Errorf("Missing x-amz-meta-version: got %q", resp.Header.Get("x-amz-meta-version"))
	}
	resp.Body.Close()

	// GET should also return user metadata
	resp = ts.doSigned(t, "GET", "/"+bucket+"/meta.txt", nil)
	if resp.Header.Get("x-amz-meta-author") != "tester" {
		t.Errorf("GET missing x-amz-meta-author")
	}
	resp.Body.Close()

	ts.doSigned(t, "DELETE", "/"+bucket+"/meta.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationObjectOverwrite(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-overwrite"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	// Put v1
	ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/file.txt", []byte("version 1"), map[string]string{
		"Content-Type": "text/plain",
	}).Body.Close()

	// Put v2
	ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/file.txt", []byte("version 2"), map[string]string{
		"Content-Type": "text/plain",
	}).Body.Close()

	// GET should return v2
	resp := ts.doSigned(t, "GET", "/"+bucket+"/file.txt", nil)
	got := intReadBodyBytes(resp)
	if string(got) != "version 2" {
		t.Errorf("Expected 'version 2', got %q", got)
	}

	ts.doSigned(t, "DELETE", "/"+bucket+"/file.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationEmptyObject(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-empty"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	// Put zero-byte object
	resp := ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/empty.txt", []byte(""), map[string]string{
		"Content-Type": "text/plain",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("PutObject empty status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	resp.Body.Close()

	// HEAD should show 0 content length
	resp = ts.doSigned(t, "HEAD", "/"+bucket+"/empty.txt", nil)
	if resp.Header.Get("Content-Length") != "0" {
		t.Errorf("Empty object Content-Length = %q, want 0", resp.Header.Get("Content-Length"))
	}
	resp.Body.Close()

	ts.doSigned(t, "DELETE", "/"+bucket+"/empty.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationSlashInKey(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-slash-key"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	body := []byte("nested content")
	resp := ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/a/b/c/file.txt", body, map[string]string{
		"Content-Type": "text/plain",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("PutObject with slashes status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	resp.Body.Close()

	resp = ts.doSigned(t, "GET", "/"+bucket+"/a/b/c/file.txt", nil)
	got := intReadBodyBytes(resp)
	if !bytes.Equal(got, body) {
		t.Errorf("Body = %q, want %q", got, body)
	}

	ts.doSigned(t, "DELETE", "/"+bucket+"/a/b/c/file.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationDeleteNonexistentObject(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-delete-idempotent"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	// Delete of non-existent object should return 204 (idempotent)
	resp := ts.doSigned(t, "DELETE", "/"+bucket+"/never-existed.txt", nil)
	if resp.StatusCode != 204 {
		t.Errorf("Delete nonexistent status = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()

	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationBucketNotEmpty(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-notempty"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()
	ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/blocker.txt", []byte("data"), map[string]string{
		"Content-Type": "text/plain",
	}).Body.Close()

	// Delete non-empty bucket should fail
	resp := ts.doSigned(t, "DELETE", "/"+bucket, nil)
	body := intReadBody(resp)
	if resp.StatusCode != 409 {
		t.Errorf("Delete non-empty bucket status = %d, want 409: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "BucketNotEmpty") {
		t.Errorf("Expected BucketNotEmpty: %s", body)
	}

	// Cleanup
	ts.doSigned(t, "DELETE", "/"+bucket+"/blocker.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationBucketAlreadyExists(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-already-exists"

	// Create bucket
	resp := ts.doSigned(t, "PUT", "/"+bucket, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("CreateBucket status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	resp.Body.Close()

	// Create same bucket again - us-east-1 behavior should return 200
	resp = ts.doSigned(t, "PUT", "/"+bucket, nil)
	if resp.StatusCode != 200 {
		t.Errorf("CreateBucket again status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationInvalidBucketName(t *testing.T) {
	ts := newIntegrationServer(t)

	// Uppercase
	resp := ts.doSigned(t, "PUT", "/INVALID-UPPERCASE", nil)
	if resp.StatusCode != 400 {
		t.Errorf("Invalid bucket name (uppercase) status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()

	// Too short
	resp = ts.doSigned(t, "PUT", "/ab", nil)
	if resp.StatusCode != 400 {
		t.Errorf("Invalid bucket name (short) status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestIntegrationGetBucketLocation(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-location"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	resp := ts.doSigned(t, "GET", "/"+bucket+"?location", nil)
	body := intReadBody(resp)
	if resp.StatusCode != 200 {
		t.Errorf("GetBucketLocation status = %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "LocationConstraint") {
		t.Errorf("Expected LocationConstraint in response: %s", body)
	}

	// Nonexistent bucket
	resp = ts.doSigned(t, "GET", "/nonexistent-bucket-xyz123?location", nil)
	if resp.StatusCode != 404 {
		t.Errorf("GetBucketLocation nonexistent status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationObjectACL(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-acl"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	// Put object
	ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/acl.txt", []byte("data"), map[string]string{
		"Content-Type": "text/plain",
	}).Body.Close()

	// Get default ACL
	resp := ts.doSigned(t, "GET", "/"+bucket+"/acl.txt?acl", nil)
	body := intReadBody(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("GetObjectACL status = %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "FULL_CONTROL") {
		t.Errorf("Default ACL should have FULL_CONTROL: %s", body)
	}
	if !strings.Contains(body, "Owner") {
		t.Errorf("ACL response should have Owner: %s", body)
	}

	// Put canned ACL
	resp = ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/acl.txt?acl", nil, map[string]string{
		"x-amz-acl": "public-read",
	})
	if resp.StatusCode != 200 {
		t.Errorf("PutObjectACL status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	resp.Body.Close()

	// Verify ACL updated
	resp = ts.doSigned(t, "GET", "/"+bucket+"/acl.txt?acl", nil)
	body = intReadBody(resp)
	if !strings.Contains(body, "READ") {
		t.Errorf("Public-read ACL should have READ: %s", body)
	}

	// Get ACL of nonexistent object
	resp = ts.doSigned(t, "GET", "/"+bucket+"/nonexistent.txt?acl", nil)
	if resp.StatusCode != 404 {
		t.Errorf("ACL nonexistent object status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// Put object with canned ACL
	resp = ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/acl-on-put.txt", []byte("data"), map[string]string{
		"Content-Type": "text/plain",
		"x-amz-acl":    "public-read",
	})
	resp.Body.Close()

	resp = ts.doSigned(t, "GET", "/"+bucket+"/acl-on-put.txt?acl", nil)
	body = intReadBody(resp)
	if !strings.Contains(body, "READ") {
		t.Errorf("Object with public-read ACL should have READ: %s", body)
	}

	ts.doSigned(t, "DELETE", "/"+bucket+"/acl.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket+"/acl-on-put.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationBucketACL(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-bucket-acl"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	// Get default bucket ACL
	resp := ts.doSigned(t, "GET", "/"+bucket+"?acl", nil)
	body := intReadBody(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("GetBucketACL status = %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "FULL_CONTROL") {
		t.Errorf("Default bucket ACL should have FULL_CONTROL: %s", body)
	}

	// Put canned ACL
	resp = ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"?acl", nil, map[string]string{
		"x-amz-acl": "public-read",
	})
	if resp.StatusCode != 200 {
		t.Errorf("PutBucketACL status = %d: %s", resp.StatusCode, intReadBody(resp))
	}
	resp.Body.Close()

	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationListBucketsOwner(t *testing.T) {
	ts := newIntegrationServer(t)

	// ListBuckets should include Owner
	resp := ts.doSigned(t, "GET", "/", nil)
	body := intReadBody(resp)
	if !strings.Contains(body, "<Owner>") {
		t.Errorf("ListBuckets should have Owner element: %s", body)
	}
	if !strings.Contains(body, "<ID>") {
		t.Errorf("ListBuckets Owner should have ID: %s", body)
	}
}

func TestIntegrationRangeSuffix(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-range-suffix"
	body := []byte("0123456789ABCDEF")

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()
	ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/range.txt", body, map[string]string{
		"Content-Type": "text/plain",
	}).Body.Close()

	// Suffix range: last 5 bytes
	resp := ts.doSignedWithHeaders(t, "GET", "/"+bucket+"/range.txt", nil, map[string]string{
		"Range": "bytes=-5",
	})
	if resp.StatusCode != 206 {
		t.Errorf("Suffix range status = %d, want 206", resp.StatusCode)
	}
	got := intReadBodyBytes(resp)
	if string(got) != "BCDEF" {
		t.Errorf("Suffix range body = %q, want BCDEF", got)
	}

	// Invalid range (beyond object size)
	resp = ts.doSignedWithHeaders(t, "GET", "/"+bucket+"/range.txt", nil, map[string]string{
		"Range": "bytes=100-200",
	})
	if resp.StatusCode != 416 {
		t.Errorf("Invalid range status = %d, want 416", resp.StatusCode)
	}
	resp.Body.Close()

	ts.doSigned(t, "DELETE", "/"+bucket+"/range.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationMultipartAbort(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-mp-abort"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	// Create multipart upload
	resp := ts.doSignedWithHeaders(t, "POST", "/"+bucket+"/abort.bin?uploads", nil, map[string]string{
		"Content-Type": "application/octet-stream",
	})
	initBody := intReadBody(resp)
	type InitResult struct {
		UploadID string `xml:"UploadId"`
	}
	var initResult InitResult
	xml.Unmarshal([]byte(initBody), &initResult)
	uploadID := initResult.UploadID

	// Abort
	resp = ts.doSigned(t, "DELETE", fmt.Sprintf("/%s/abort.bin?uploadId=%s", bucket, uploadID), nil)
	if resp.StatusCode != 204 {
		t.Errorf("Abort status = %d, want 204: %s", resp.StatusCode, intReadBody(resp))
	} else {
		resp.Body.Close()
	}

	// Abort nonexistent upload
	resp = ts.doSigned(t, "DELETE", fmt.Sprintf("/%s/nope.bin?uploadId=fake-id", bucket), nil)
	body := intReadBody(resp)
	if resp.StatusCode != 404 {
		t.Errorf("Abort nonexistent status = %d, want 404: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "NoSuchUpload") {
		t.Errorf("Expected NoSuchUpload: %s", body)
	}

	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationMultipartListUploads(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-mp-list"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	// Create two uploads
	type InitResult struct {
		UploadID string `xml:"UploadId"`
	}

	resp := ts.doSignedWithHeaders(t, "POST", "/"+bucket+"/upload1.bin?uploads", nil, map[string]string{
		"Content-Type": "application/octet-stream",
	})
	initBody := intReadBody(resp)
	var init1 InitResult
	xml.Unmarshal([]byte(initBody), &init1)

	resp = ts.doSignedWithHeaders(t, "POST", "/"+bucket+"/upload2.bin?uploads", nil, map[string]string{
		"Content-Type": "application/octet-stream",
	})
	initBody = intReadBody(resp)
	var init2 InitResult
	xml.Unmarshal([]byte(initBody), &init2)

	// List uploads
	resp = ts.doSigned(t, "GET", "/"+bucket+"?uploads", nil)
	body := intReadBody(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("ListMultipartUploads status = %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, init1.UploadID) {
		t.Errorf("ListUploads should contain upload1 ID: %s", body)
	}
	if !strings.Contains(body, init2.UploadID) {
		t.Errorf("ListUploads should contain upload2 ID: %s", body)
	}

	// Cleanup
	ts.doSigned(t, "DELETE", fmt.Sprintf("/%s/upload1.bin?uploadId=%s", bucket, init1.UploadID), nil).Body.Close()
	ts.doSigned(t, "DELETE", fmt.Sprintf("/%s/upload2.bin?uploadId=%s", bucket, init2.UploadID), nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationMultipartInvalidPartOrder(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-mp-order"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	resp := ts.doSignedWithHeaders(t, "POST", "/"+bucket+"/order.bin?uploads", nil, map[string]string{
		"Content-Type": "application/octet-stream",
	})
	initBody := intReadBody(resp)
	type InitResult struct {
		UploadID string `xml:"UploadId"`
	}
	var initResult InitResult
	xml.Unmarshal([]byte(initBody), &initResult)
	uploadID := initResult.UploadID

	part1Data := bytes.Repeat([]byte("A"), 5*1024*1024)
	part2Data := bytes.Repeat([]byte("B"), 1024)

	resp = ts.doSignedWithHeaders(t, "PUT",
		fmt.Sprintf("/%s/order.bin?partNumber=1&uploadId=%s", bucket, uploadID),
		part1Data, map[string]string{"Content-Type": "application/octet-stream"})
	part1ETag := resp.Header.Get("ETag")
	resp.Body.Close()

	resp = ts.doSignedWithHeaders(t, "PUT",
		fmt.Sprintf("/%s/order.bin?partNumber=2&uploadId=%s", bucket, uploadID),
		part2Data, map[string]string{"Content-Type": "application/octet-stream"})
	part2ETag := resp.Header.Get("ETag")
	resp.Body.Close()

	// Complete with wrong order (2, 1 instead of 1, 2)
	completeXML := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`,
		part2ETag, part1ETag)

	resp = ts.doSignedWithHeaders(t, "POST",
		fmt.Sprintf("/%s/order.bin?uploadId=%s", bucket, uploadID),
		[]byte(completeXML), map[string]string{"Content-Type": "application/xml"})
	body := intReadBody(resp)
	if resp.StatusCode != 400 {
		t.Errorf("InvalidPartOrder status = %d, want 400: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "InvalidPartOrder") {
		t.Errorf("Expected InvalidPartOrder: %s", body)
	}

	// Cleanup
	ts.doSigned(t, "DELETE", fmt.Sprintf("/%s/order.bin?uploadId=%s", bucket, uploadID), nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationMultipartWrongETag(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-mp-etag"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	resp := ts.doSignedWithHeaders(t, "POST", "/"+bucket+"/etag.bin?uploads", nil, map[string]string{
		"Content-Type": "application/octet-stream",
	})
	initBody := intReadBody(resp)
	type InitResult struct {
		UploadID string `xml:"UploadId"`
	}
	var initResult InitResult
	xml.Unmarshal([]byte(initBody), &initResult)
	uploadID := initResult.UploadID

	partData := bytes.Repeat([]byte("A"), 5*1024*1024)
	resp = ts.doSignedWithHeaders(t, "PUT",
		fmt.Sprintf("/%s/etag.bin?partNumber=1&uploadId=%s", bucket, uploadID),
		partData, map[string]string{"Content-Type": "application/octet-stream"})
	resp.Body.Close()

	// Complete with wrong ETag
	completeXML := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"0000000000000000000000000000000"</ETag></Part></CompleteMultipartUpload>`
	resp = ts.doSignedWithHeaders(t, "POST",
		fmt.Sprintf("/%s/etag.bin?uploadId=%s", bucket, uploadID),
		[]byte(completeXML), map[string]string{"Content-Type": "application/xml"})
	body := intReadBody(resp)
	if resp.StatusCode != 400 {
		t.Errorf("InvalidPart status = %d, want 400: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "InvalidPart") {
		t.Errorf("Expected InvalidPart: %s", body)
	}

	// Cleanup
	ts.doSigned(t, "DELETE", fmt.Sprintf("/%s/etag.bin?uploadId=%s", bucket, uploadID), nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationNoSuchKeyError(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-nosuchkey"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	resp := ts.doSigned(t, "GET", "/"+bucket+"/does-not-exist.txt", nil)
	body := intReadBody(resp)
	if resp.StatusCode != 404 {
		t.Errorf("NoSuchKey status = %d, want 404: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "NoSuchKey") {
		t.Errorf("Expected NoSuchKey: %s", body)
	}

	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationMalformedXML(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-malformed"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	// Send malformed XML to DeleteObjects
	resp := ts.doSignedWithHeaders(t, "POST", "/"+bucket+"?delete", []byte("<Delete><this is not valid xml"), map[string]string{
		"Content-Type": "application/xml",
	})
	body := intReadBody(resp)
	if resp.StatusCode != 400 {
		t.Errorf("MalformedXML status = %d, want 400: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "MalformedXML") {
		t.Errorf("Expected MalformedXML: %s", body)
	}

	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationSignatureMismatch(t *testing.T) {
	ts := newIntegrationServer(t)

	// Create a request signed with the wrong secret key
	reqURL := ts.endpoint + "/"
	req, _ := http.NewRequest("GET", reqURL, nil)

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStr := now.Format("20060102")

	payloadHash := intSha256Hex(nil)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("Host", ts.addr)

	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	sort.Strings(signedHeaders)

	var canonReq strings.Builder
	canonReq.WriteString("GET\n")
	canonReq.WriteString("/\n")
	canonReq.WriteString("\n")
	for _, h := range signedHeaders {
		canonReq.WriteString(h)
		canonReq.WriteByte(':')
		if h == "host" {
			canonReq.WriteString(ts.addr)
		} else {
			canonReq.WriteString(req.Header.Get(http.CanonicalHeaderKey(h)))
		}
		canonReq.WriteByte('\n')
	}
	canonReq.WriteByte('\n')
	canonReq.WriteString(strings.Join(signedHeaders, ";"))
	canonReq.WriteByte('\n')
	canonReq.WriteString(payloadHash)

	scope := fmt.Sprintf("%s/us-east-1/s3/aws4_request", dateStr)
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + intSha256Hex([]byte(canonReq.String()))

	// Use WRONG secret key
	signingKey := intHmacSHA256([]byte("AWS4wrong-secret-key"), dateStr)
	signingKey = intHmacSHA256(signingKey, "us-east-1")
	signingKey = intHmacSHA256(signingKey, "s3")
	signingKey = intHmacSHA256(signingKey, "aws4_request")

	signature := hex.EncodeToString(intHmacSHA256(signingKey, stringToSign))

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=bleepstore/%s/us-east-1/s3/aws4_request, SignedHeaders=%s, Signature=%s",
		dateStr, strings.Join(signedHeaders, ";"), signature)
	req.Header.Set("Authorization", authHeader)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	body := intReadBody(resp)
	if resp.StatusCode != 403 {
		t.Errorf("Wrong signature status = %d, want 403: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "SignatureDoesNotMatch") && !strings.Contains(body, "AccessDenied") {
		t.Errorf("Expected SignatureDoesNotMatch or AccessDenied: %s", body)
	}
}

func TestIntegrationPresignedGetURL(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-presigned"
	body := []byte("presigned download content")

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()
	ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/presigned-get.txt", body, map[string]string{
		"Content-Type": "text/plain",
	}).Body.Close()

	// Build a presigned GET URL manually
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStr := now.Format("20060102")
	credential := fmt.Sprintf("bleepstore/%s/us-east-1/s3/aws4_request", dateStr)

	params := url.Values{}
	params.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	params.Set("X-Amz-Credential", credential)
	params.Set("X-Amz-Date", amzDate)
	params.Set("X-Amz-Expires", "300")
	params.Set("X-Amz-SignedHeaders", "host")

	// Build canonical request for presigned URL
	path := "/" + bucket + "/presigned-get.txt"
	canonQueryStr := intCanonicalQueryString(params)

	var canonReq strings.Builder
	canonReq.WriteString("GET\n")
	canonReq.WriteString(intURIEncode(path))
	canonReq.WriteByte('\n')
	canonReq.WriteString(canonQueryStr)
	canonReq.WriteByte('\n')
	canonReq.WriteString("host:" + ts.addr + "\n")
	canonReq.WriteByte('\n')
	canonReq.WriteString("host\n")
	canonReq.WriteString("UNSIGNED-PAYLOAD")

	scope := fmt.Sprintf("%s/us-east-1/s3/aws4_request", dateStr)
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + intSha256Hex([]byte(canonReq.String()))

	signingKey := intHmacSHA256([]byte("AWS4bleepstore-secret"), dateStr)
	signingKey = intHmacSHA256(signingKey, "us-east-1")
	signingKey = intHmacSHA256(signingKey, "s3")
	signingKey = intHmacSHA256(signingKey, "aws4_request")

	signature := hex.EncodeToString(intHmacSHA256(signingKey, stringToSign))
	params.Set("X-Amz-Signature", signature)

	presignedURL := ts.endpoint + path + "?" + params.Encode()

	// Fetch via plain HTTP GET (no authorization header)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(presignedURL)
	if err != nil {
		t.Fatalf("Presigned GET failed: %v", err)
	}
	got := intReadBodyBytes(resp)
	if resp.StatusCode != 200 {
		t.Errorf("Presigned GET status = %d, want 200: %s", resp.StatusCode, string(got))
	}
	if !bytes.Equal(got, body) {
		t.Errorf("Presigned GET body = %q, want %q", got, body)
	}

	ts.doSigned(t, "DELETE", "/"+bucket+"/presigned-get.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationListObjectsContentFields(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-list-fields"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()
	ts.doSignedWithHeaders(t, "PUT", "/"+bucket+"/fields.txt", []byte("test"), map[string]string{
		"Content-Type": "text/plain",
	}).Body.Close()

	resp := ts.doSigned(t, "GET", "/"+bucket+"?list-type=2", nil)
	body := intReadBody(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("ListV2 status = %d: %s", resp.StatusCode, body)
	}

	// Check required fields
	if !strings.Contains(body, "<Key>fields.txt</Key>") {
		t.Errorf("Missing Key in listing: %s", body)
	}
	if !strings.Contains(body, "<LastModified>") {
		t.Errorf("Missing LastModified in listing: %s", body)
	}
	if !strings.Contains(body, "<ETag>") {
		t.Errorf("Missing ETag in listing: %s", body)
	}
	if !strings.Contains(body, "<Size>") {
		t.Errorf("Missing Size in listing: %s", body)
	}
	if !strings.Contains(body, "<StorageClass>") {
		t.Errorf("Missing StorageClass in listing: %s", body)
	}

	ts.doSigned(t, "DELETE", "/"+bucket+"/fields.txt", nil).Body.Close()
	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}

func TestIntegrationListObjectsEmptyBucket(t *testing.T) {
	ts := newIntegrationServer(t)
	bucket := "test-list-empty"

	ts.doSigned(t, "PUT", "/"+bucket, nil).Body.Close()

	resp := ts.doSigned(t, "GET", "/"+bucket+"?list-type=2", nil)
	body := intReadBody(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("ListV2 empty status = %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "<KeyCount>0</KeyCount>") {
		t.Errorf("Empty bucket should have KeyCount=0: %s", body)
	}
	if strings.Contains(body, "<Contents>") {
		t.Errorf("Empty bucket should not have Contents: %s", body)
	}

	ts.doSigned(t, "DELETE", "/"+bucket, nil).Body.Close()
}
