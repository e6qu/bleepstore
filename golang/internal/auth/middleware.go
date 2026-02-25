package auth

import (
	"net/http"
	"strings"

	s3err "github.com/bleepstore/bleepstore/internal/errors"
	"github.com/bleepstore/bleepstore/internal/xmlutil"
)

// skipPaths is the set of paths that do not require authentication.
var skipPaths = map[string]bool{
	"/health":       true,
	"/healthz":      true,
	"/readyz":       true,
	"/metrics":      true,
	"/docs":         true,
	"/docs/":        true,
	"/openapi":      true,
	"/openapi.json": true,
}

// Middleware returns HTTP middleware that enforces AWS SigV4 authentication
// on all requests except those to excluded paths (/health, /metrics, /docs, /openapi.json).
// On success, the authenticated owner identity is set on the request context.
func Middleware(verifier *SigV4Verifier) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip authentication for excluded paths.
			path := r.URL.Path
			if skipPaths[path] || strings.HasPrefix(path, "/docs") {
				next.ServeHTTP(w, r)
				return
			}

			// Detect authentication method.
			method := DetectAuthMethod(r)

			switch method {
			case "none":
				xmlutil.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
				return

			case "ambiguous":
				xmlutil.WriteErrorResponse(w, r, &s3err.S3Error{
					Code:       "InvalidArgument",
					Message:    "Only one auth mechanism allowed; found both Authorization header and query string parameters",
					HTTPStatus: 400,
				})
				return

			case "header":
				cred, err := verifier.VerifyRequest(r)
				if err != nil {
					writeAuthError(w, r, err)
					return
				}
				// Set owner identity on context.
				ctx := contextWithOwner(r.Context(), cred.OwnerID, cred.DisplayName)
				r = r.WithContext(ctx)

			case "presigned":
				cred, err := verifier.VerifyPresigned(r)
				if err != nil {
					writeAuthError(w, r, err)
					return
				}
				// Set owner identity on context.
				ctx := contextWithOwner(r.Context(), cred.OwnerID, cred.DisplayName)
				r = r.WithContext(ctx)
			}

			next.ServeHTTP(w, r)
		})
	}
}

// writeAuthError maps an AuthError to the appropriate S3 error XML response.
func writeAuthError(w http.ResponseWriter, r *http.Request, err error) {
	authErr, ok := err.(*AuthError)
	if !ok {
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	switch authErr.Code {
	case "InvalidAccessKeyId":
		xmlutil.WriteErrorResponse(w, r, s3err.ErrInvalidAccessKeyId)
	case "SignatureDoesNotMatch":
		xmlutil.WriteErrorResponse(w, r, s3err.ErrSignatureDoesNotMatch)
	case "RequestTimeTooSkewed":
		xmlutil.WriteErrorResponse(w, r, s3err.ErrRequestTimeTooSkewed)
	case "AccessDenied":
		xmlutil.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
	default:
		xmlutil.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
	}
}
