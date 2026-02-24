package server

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	s3err "github.com/bleepstore/bleepstore/internal/errors"
	"github.com/bleepstore/bleepstore/internal/metrics"
	"github.com/bleepstore/bleepstore/internal/xmlutil"
)

// generateRequestID generates a 16-character uppercase hexadecimal request ID
// using crypto/rand for randomness.
func generateRequestID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// Fallback: should never happen with crypto/rand, but if it does,
		// use a timestamp-based value rather than panicking.
		return fmt.Sprintf("%016X", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// commonHeaders is HTTP middleware that injects common S3 response headers
// on every response: x-amz-request-id, x-amz-id-2, Date, and Server.
func commonHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := generateRequestID()
		w.Header().Set("x-amz-request-id", requestID)
		w.Header().Set("x-amz-id-2", requestID)
		w.Header().Set("Date", xmlutil.FormatTimeHTTP(time.Now()))
		w.Header().Set("Server", "BleepStore")
		next.ServeHTTP(w, r)
	})
}

// responseRecorder wraps http.ResponseWriter to capture the HTTP status code
// and the number of bytes written. This is used by the metrics middleware.
type responseRecorder struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
	wroteHeader  bool
}

// WriteHeader captures the status code and delegates to the wrapped ResponseWriter.
func (rr *responseRecorder) WriteHeader(code int) {
	if !rr.wroteHeader {
		rr.statusCode = code
		rr.wroteHeader = true
	}
	rr.ResponseWriter.WriteHeader(code)
}

// Write captures the number of bytes written and delegates to the wrapped ResponseWriter.
func (rr *responseRecorder) Write(b []byte) (int, error) {
	if !rr.wroteHeader {
		rr.statusCode = http.StatusOK
		rr.wroteHeader = true
	}
	n, err := rr.ResponseWriter.Write(b)
	rr.bytesWritten += n
	return n, err
}

// Flush implements the http.Flusher interface if the underlying ResponseWriter supports it.
func (rr *responseRecorder) Flush() {
	if f, ok := rr.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// metricsMiddleware records Prometheus metrics for each request:
// request count, duration, request size, and response size.
// The /metrics endpoint is excluded from self-instrumentation to avoid recursion.
func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Exclude /metrics from self-instrumentation.
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		rec := &responseRecorder{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(rec, r)

		duration := time.Since(start).Seconds()
		normalizedPath := metrics.NormalizePath(r.URL.Path)
		method := r.Method
		status := strconv.Itoa(rec.statusCode)

		// Record metrics — best-effort, never block.
		metrics.HTTPRequestsTotal.WithLabelValues(method, normalizedPath, status).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(method, normalizedPath).Observe(duration)

		if r.ContentLength > 0 {
			metrics.HTTPRequestSize.WithLabelValues(method, normalizedPath).Observe(float64(r.ContentLength))
			metrics.BytesReceivedTotal.Add(float64(r.ContentLength))
		}

		if rec.bytesWritten > 0 {
			metrics.HTTPResponseSize.WithLabelValues(method, normalizedPath).Observe(float64(rec.bytesWritten))
			metrics.BytesSentTotal.Add(float64(rec.bytesWritten))
		}
	})
}

// transferEncodingCheck rejects requests with non-chunked Transfer-Encoding
// (e.g., "identity") which S3 does not support. This must run early in the
// pipeline, before auth or handler processing.
func transferEncodingCheck(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check r.Header first (some non-standard values may survive Go's parsing)
		te := r.Header.Get("Transfer-Encoding")
		if te != "" {
			lower := strings.ToLower(strings.TrimSpace(te))
			if lower != "chunked" {
				xmlutil.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
				return
			}
		}
		// Also check r.TransferEncoding slice — Go's net/http strips the header
		// but populates this slice with non-chunked values like "identity"
		for _, enc := range r.TransferEncoding {
			if strings.ToLower(enc) != "chunked" {
				xmlutil.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// metaHeaderPrefix is the canonical form of "x-amz-meta-" as produced by
// Go's textproto.CanonicalMIMEHeaderKey.
const metaHeaderPrefix = "X-Amz-Meta-"

// metadataHeaderWriter wraps an http.ResponseWriter to rewrite X-Amz-Meta-*
// response header keys to fully lowercase before they are flushed to the wire.
//
// S3 requires user metadata header keys to be lowercase (e.g., x-amz-meta-author).
// Go's http.Header.Set() auto-canonicalizes keys to Title-Case (X-Amz-Meta-Author),
// which causes boto3 (and other S3 SDKs) to see the metadata key portion as title-cased
// (e.g., "Author" instead of "author"). This wrapper fixes that at write time.
type metadataHeaderWriter struct {
	http.ResponseWriter
	headerRewritten bool
}

// rewriteMetaHeaders moves any X-Amz-Meta-* canonical headers to their
// fully-lowercase equivalent in the raw header map.
func (mw *metadataHeaderWriter) rewriteMetaHeaders() {
	if mw.headerRewritten {
		return
	}
	mw.headerRewritten = true

	h := mw.ResponseWriter.Header()
	for key, values := range h {
		if strings.HasPrefix(key, metaHeaderPrefix) {
			lowerKey := strings.ToLower(key)
			if lowerKey != key {
				delete(h, key)
				h[lowerKey] = values
			}
		}
	}
}

func (mw *metadataHeaderWriter) WriteHeader(code int) {
	mw.rewriteMetaHeaders()
	mw.ResponseWriter.WriteHeader(code)
}

func (mw *metadataHeaderWriter) Write(b []byte) (int, error) {
	mw.rewriteMetaHeaders()
	return mw.ResponseWriter.Write(b)
}

func (mw *metadataHeaderWriter) Flush() {
	if f, ok := mw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// metadataHeaderMiddleware wraps the response writer to ensure x-amz-meta-*
// headers are written with lowercase keys on the wire.
func metadataHeaderMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mw := &metadataHeaderWriter{ResponseWriter: w}
		next.ServeHTTP(mw, r)
	})
}
