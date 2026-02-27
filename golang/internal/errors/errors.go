// Package errors defines S3-compatible error types used throughout BleepStore.
package errors

import "fmt"

// S3Error represents an S3 API error with a machine-readable code,
// human-readable message, HTTP status code, and optional extra fields.
type S3Error struct {
	// Code is the S3 error code (e.g., "NoSuchBucket", "AccessDenied").
	Code string
	// Message is a human-readable description of the error.
	Message string
	// HTTPStatus is the HTTP status code to return (e.g., 404, 403).
	HTTPStatus int
	// ExtraFields holds additional key-value pairs included in the XML error response.
	ExtraFields map[string]string
}

// Error implements the error interface for S3Error.
func (e *S3Error) Error() string {
	return fmt.Sprintf("S3Error %s (%d): %s", e.Code, e.HTTPStatus, e.Message)
}

// WithExtra returns a copy of the S3Error with the given extra field set.
func (e *S3Error) WithExtra(key, value string) *S3Error {
	cp := *e
	if cp.ExtraFields == nil {
		cp.ExtraFields = make(map[string]string)
	}
	cp.ExtraFields[key] = value
	return &cp
}

// Pre-defined S3 errors for common conditions.
var (
	// ErrAccessDenied is returned when the caller lacks permission.
	ErrAccessDenied = &S3Error{
		Code:       "AccessDenied",
		Message:    "Access Denied",
		HTTPStatus: 403,
	}

	// ErrNoSuchBucket is returned when the specified bucket does not exist.
	ErrNoSuchBucket = &S3Error{
		Code:       "NoSuchBucket",
		Message:    "The specified bucket does not exist",
		HTTPStatus: 404,
	}

	// ErrNoSuchKey is returned when the specified object key does not exist.
	ErrNoSuchKey = &S3Error{
		Code:       "NoSuchKey",
		Message:    "The specified key does not exist",
		HTTPStatus: 404,
	}

	// ErrBucketAlreadyExists is returned when creating a bucket that already exists.
	ErrBucketAlreadyExists = &S3Error{
		Code:       "BucketAlreadyExists",
		Message:    "The requested bucket name is not available",
		HTTPStatus: 409,
	}

	// ErrBucketAlreadyOwnedByYou is returned when creating a bucket you already own.
	ErrBucketAlreadyOwnedByYou = &S3Error{
		Code:       "BucketAlreadyOwnedByYou",
		Message:    "Your previous request to create the named bucket succeeded and you already own it",
		HTTPStatus: 409,
	}

	// ErrBucketNotEmpty is returned when deleting a non-empty bucket.
	ErrBucketNotEmpty = &S3Error{
		Code:       "BucketNotEmpty",
		Message:    "The bucket you tried to delete is not empty",
		HTTPStatus: 409,
	}

	// ErrInvalidBucketName is returned when the bucket name is invalid.
	ErrInvalidBucketName = &S3Error{
		Code:       "InvalidBucketName",
		Message:    "The specified bucket is not valid",
		HTTPStatus: 400,
	}

	// ErrNoSuchUpload is returned when the specified multipart upload does not exist.
	ErrNoSuchUpload = &S3Error{
		Code:       "NoSuchUpload",
		Message:    "The specified multipart upload does not exist",
		HTTPStatus: 404,
	}

	// ErrInvalidPart is returned when a part is invalid during multipart completion.
	ErrInvalidPart = &S3Error{
		Code:       "InvalidPart",
		Message:    "One or more of the specified parts could not be found",
		HTTPStatus: 400,
	}

	// ErrInvalidPartOrder is returned when parts are not in ascending order.
	ErrInvalidPartOrder = &S3Error{
		Code:       "InvalidPartOrder",
		Message:    "The list of parts was not in ascending order",
		HTTPStatus: 400,
	}

	// ErrEntityTooLarge is returned when the object is too large.
	ErrEntityTooLarge = &S3Error{
		Code:       "EntityTooLarge",
		Message:    "Your proposed upload exceeds the maximum allowed object size",
		HTTPStatus: 400,
	}

	// ErrEntityTooSmall is returned when a multipart part is too small.
	ErrEntityTooSmall = &S3Error{
		Code:       "EntityTooSmall",
		Message:    "Your proposed upload is smaller than the minimum allowed object size",
		HTTPStatus: 400,
	}

	// ErrInternalError is returned for unexpected internal failures.
	ErrInternalError = &S3Error{
		Code:       "InternalError",
		Message:    "We encountered an internal error. Please try again.",
		HTTPStatus: 500,
	}

	// ErrNotImplemented is returned when a feature is not yet implemented.
	ErrNotImplemented = &S3Error{
		Code:       "NotImplemented",
		Message:    "A header you provided implies functionality that is not implemented",
		HTTPStatus: 501,
	}

	// ErrMalformedXML is returned when the request body contains invalid XML.
	ErrMalformedXML = &S3Error{
		Code:       "MalformedXML",
		Message:    "The XML you provided was not well-formed or did not validate",
		HTTPStatus: 400,
	}

	// ErrSignatureDoesNotMatch is returned when SigV4 verification fails.
	ErrSignatureDoesNotMatch = &S3Error{
		Code:       "SignatureDoesNotMatch",
		Message:    "The request signature we calculated does not match the signature you provided",
		HTTPStatus: 403,
	}

	// ErrMethodNotAllowed is returned when the HTTP method is not supported.
	ErrMethodNotAllowed = &S3Error{
		Code:       "MethodNotAllowed",
		Message:    "The specified method is not allowed against this resource",
		HTTPStatus: 405,
	}

	// ErrInvalidAccessKeyId is returned when the access key is not found.
	ErrInvalidAccessKeyId = &S3Error{
		Code:       "InvalidAccessKeyId",
		Message:    "The AWS Access Key Id you provided does not exist in our records",
		HTTPStatus: 403,
	}

	// ErrInvalidArgument is returned when an argument value is invalid.
	ErrInvalidArgument = &S3Error{
		Code:       "InvalidArgument",
		Message:    "Invalid Argument",
		HTTPStatus: 400,
	}

	// ErrPreconditionFailed is returned when a conditional check fails.
	ErrPreconditionFailed = &S3Error{
		Code:       "PreconditionFailed",
		Message:    "At least one of the pre-conditions you specified did not hold",
		HTTPStatus: 412,
	}

	// ErrInvalidRange is returned when the range is not satisfiable.
	ErrInvalidRange = &S3Error{
		Code:       "InvalidRange",
		Message:    "The requested range is not satisfiable",
		HTTPStatus: 416,
	}

	// ErrMissingContentLength is returned when Content-Length is required but missing.
	ErrMissingContentLength = &S3Error{
		Code:       "MissingContentLength",
		Message:    "You must provide the Content-Length HTTP header",
		HTTPStatus: 411,
	}

	// ErrRequestTimeTooSkewed is returned when the clock skew is too large.
	ErrRequestTimeTooSkewed = &S3Error{
		Code:       "RequestTimeTooSkewed",
		Message:    "The difference between the request time and the server's time is too large",
		HTTPStatus: 403,
	}

	// ErrServiceUnavailable is returned when the service is temporarily unavailable.
	ErrServiceUnavailable = &S3Error{
		Code:       "ServiceUnavailable",
		Message:    "Service is not available. Please retry.",
		HTTPStatus: 503,
	}

	// ErrKeyTooLongError is returned when the object key exceeds the maximum length.
	ErrKeyTooLongError = &S3Error{
		Code:       "KeyTooLongError",
		Message:    "Your key is too long",
		HTTPStatus: 400,
	}

	// ErrInvalidRequest is returned for generally invalid requests (e.g., unsupported Transfer-Encoding).
	ErrInvalidRequest = &S3Error{
		Code:       "InvalidRequest",
		Message:    "Invalid Request",
		HTTPStatus: 400,
	}

	// ErrBadDigest is returned when the Content-MD5 does not match the body.
	ErrBadDigest = &S3Error{
		Code:       "BadDigest",
		Message:    "The Content-MD5 you specified did not match what we received",
		HTTPStatus: 400,
	}

	// ErrIncompleteBody is returned when the body is shorter than Content-Length.
	ErrIncompleteBody = &S3Error{
		Code:       "IncompleteBody",
		Message:    "You did not provide the number of bytes specified by the Content-Length HTTP header",
		HTTPStatus: 400,
	}

	// ErrInvalidDigest is returned when the Content-MD5 header is not valid base64 or wrong length.
	ErrInvalidDigest = &S3Error{
		Code:       "InvalidDigest",
		Message:    "The Content-MD5 you specified is not valid",
		HTTPStatus: 400,
	}

	// ErrMalformedACLError is returned when the ACL XML is not well-formed.
	ErrMalformedACLError = &S3Error{
		Code:       "MalformedACLError",
		Message:    "The XML you provided for the ACL is not well-formed or did not validate",
		HTTPStatus: 400,
	}

	// ErrMissingRequestBodyError is returned when the request body is empty but required.
	ErrMissingRequestBodyError = &S3Error{
		Code:       "MissingRequestBodyError",
		Message:    "Request body is empty",
		HTTPStatus: 400,
	}

	// ErrTooManyBuckets is returned when the maximum number of buckets is exceeded.
	ErrTooManyBuckets = &S3Error{
		Code:       "TooManyBuckets",
		Message:    "You have attempted to create more buckets than allowed",
		HTTPStatus: 400,
	}

	// ErrInvalidLocationConstraint is returned when an invalid location constraint is specified.
	ErrInvalidLocationConstraint = &S3Error{
		Code:       "InvalidLocationConstraint",
		Message:    "The specified location constraint is not valid",
		HTTPStatus: 400,
	}

	// ErrRequestTimeout is returned when a request times out.
	ErrRequestTimeout = &S3Error{
		Code:       "RequestTimeout",
		Message:    "Your socket connection to the server was not read from or written to within the timeout period",
		HTTPStatus: 400,
	}
)
