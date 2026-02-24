//! S3-compatible error types.
//!
//! Every variant maps to a well-known S3 error code.  The enum
//! implements [`axum::response::IntoResponse`] so handlers can simply
//! return `Err(S3Error::NoSuchBucket { .. })`.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

use crate::xml::render_error;

/// Generate a 16-character hex request ID.
pub fn generate_request_id() -> String {
    let bytes: [u8; 8] = rand::random();
    hex::encode(bytes).to_uppercase()
}

/// S3 error codes expressed as a Rust enum.
#[derive(Debug, Error)]
pub enum S3Error {
    /// The specified bucket does not exist.
    #[error("The specified bucket does not exist")]
    NoSuchBucket { bucket: String },

    /// The specified key does not exist.
    #[error("The resource you requested does not exist")]
    NoSuchKey { key: String },

    /// The specified multipart upload does not exist.
    #[error("The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.")]
    NoSuchUpload { upload_id: String },

    /// A bucket with the requested name already exists.
    #[error("The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.")]
    BucketAlreadyExists { bucket: String },

    /// You already own this bucket (us-east-1 returns 200, elsewhere 409).
    #[error("Your previous request to create the named bucket succeeded and you already own it.")]
    BucketAlreadyOwnedByYou { bucket: String },

    /// The bucket you tried to delete is not empty.
    #[error("The bucket you tried to delete is not empty")]
    BucketNotEmpty { bucket: String },

    /// Access denied.
    #[error("Access Denied")]
    AccessDenied { message: String },

    /// A request argument is invalid.
    #[error("{message}")]
    InvalidArgument { message: String },

    /// The request signature does not match.
    #[error("The request signature we calculated does not match the signature you provided.")]
    SignatureDoesNotMatch,

    /// An invalid bucket name was provided.
    #[error("The specified bucket is not valid.")]
    InvalidBucketName { name: String },

    /// The provided ETag does not match (conditional request).
    #[error("At least one of the pre-conditions you specified did not hold")]
    PreconditionFailed,

    /// The entity is too large.
    #[error("Your proposed upload exceeds the maximum allowed object size.")]
    EntityTooLarge,

    /// The entity is too small (multipart part).
    #[error("Your proposed upload is smaller than the minimum allowed size")]
    EntityTooSmall,

    /// The object key is too long (> 1024 bytes).
    #[error("Your key is too long")]
    KeyTooLongError,

    /// Invalid part in multipart upload.
    #[error("{message}")]
    InvalidPart { message: String },

    /// Invalid part order in CompleteMultipartUpload.
    #[error("The list of parts was not in ascending order. Parts must be ordered by part number.")]
    InvalidPartOrder,

    /// Malformed XML in request body.
    #[error("The XML you provided was not well-formed or did not validate against our published schema.")]
    MalformedXML,

    /// Invalid access key ID.
    #[error("The AWS Access Key Id you provided does not exist in our records.")]
    InvalidAccessKeyId,

    /// Feature not implemented.
    #[error("A header you provided implies functionality that is not implemented")]
    NotImplemented,

    /// HTTP method not allowed for this resource.
    #[error("The specified method is not allowed against this resource.")]
    MethodNotAllowed,

    /// Content-Length header is required but missing.
    #[error("You must provide the Content-Length HTTP header.")]
    MissingContentLength,

    /// Invalid range request.
    #[error("The requested range is not satisfiable")]
    InvalidRange,

    /// Not modified (304 response for conditional requests).
    #[error("Not Modified")]
    NotModified,

    /// Catch-all for unexpected internal errors.
    #[error("We encountered an internal error, please try again.")]
    InternalError(#[from] anyhow::Error),
}

impl S3Error {
    /// Return the S3 XML error code string.
    pub fn code(&self) -> &'static str {
        match self {
            S3Error::NoSuchBucket { .. } => "NoSuchBucket",
            S3Error::NoSuchKey { .. } => "NoSuchKey",
            S3Error::NoSuchUpload { .. } => "NoSuchUpload",
            S3Error::BucketAlreadyExists { .. } => "BucketAlreadyExists",
            S3Error::BucketAlreadyOwnedByYou { .. } => "BucketAlreadyOwnedByYou",
            S3Error::BucketNotEmpty { .. } => "BucketNotEmpty",
            S3Error::AccessDenied { .. } => "AccessDenied",
            S3Error::InvalidArgument { .. } => "InvalidArgument",
            S3Error::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            S3Error::InvalidBucketName { .. } => "InvalidBucketName",
            S3Error::PreconditionFailed => "PreconditionFailed",
            S3Error::EntityTooLarge => "EntityTooLarge",
            S3Error::EntityTooSmall => "EntityTooSmall",
            S3Error::KeyTooLongError => "KeyTooLongError",
            S3Error::InvalidPart { .. } => "InvalidPart",
            S3Error::InvalidPartOrder => "InvalidPartOrder",
            S3Error::MalformedXML => "MalformedXML",
            S3Error::InvalidAccessKeyId => "InvalidAccessKeyId",
            S3Error::NotImplemented => "NotImplemented",
            S3Error::MethodNotAllowed => "MethodNotAllowed",
            S3Error::MissingContentLength => "MissingContentLength",
            S3Error::InvalidRange => "InvalidRange",
            S3Error::NotModified => "NotModified",
            S3Error::InternalError(_) => "InternalError",
        }
    }

    /// Return the appropriate HTTP status code for this error.
    pub fn status_code(&self) -> StatusCode {
        match self {
            S3Error::NoSuchBucket { .. } => StatusCode::NOT_FOUND,
            S3Error::NoSuchKey { .. } => StatusCode::NOT_FOUND,
            S3Error::NoSuchUpload { .. } => StatusCode::NOT_FOUND,
            S3Error::BucketAlreadyExists { .. } => StatusCode::CONFLICT,
            S3Error::BucketAlreadyOwnedByYou { .. } => StatusCode::CONFLICT,
            S3Error::BucketNotEmpty { .. } => StatusCode::CONFLICT,
            S3Error::AccessDenied { .. } => StatusCode::FORBIDDEN,
            S3Error::InvalidArgument { .. } => StatusCode::BAD_REQUEST,
            S3Error::SignatureDoesNotMatch => StatusCode::FORBIDDEN,
            S3Error::InvalidBucketName { .. } => StatusCode::BAD_REQUEST,
            S3Error::PreconditionFailed => StatusCode::PRECONDITION_FAILED,
            S3Error::EntityTooLarge => StatusCode::BAD_REQUEST,
            S3Error::EntityTooSmall => StatusCode::BAD_REQUEST,
            S3Error::KeyTooLongError => StatusCode::BAD_REQUEST,
            S3Error::InvalidPart { .. } => StatusCode::BAD_REQUEST,
            S3Error::InvalidPartOrder => StatusCode::BAD_REQUEST,
            S3Error::MalformedXML => StatusCode::BAD_REQUEST,
            S3Error::InvalidAccessKeyId => StatusCode::FORBIDDEN,
            S3Error::NotImplemented => StatusCode::NOT_IMPLEMENTED,
            S3Error::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            S3Error::MissingContentLength => StatusCode::LENGTH_REQUIRED,
            S3Error::InvalidRange => StatusCode::RANGE_NOT_SATISFIABLE,
            S3Error::NotModified => StatusCode::NOT_MODIFIED,
            S3Error::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let request_id = generate_request_id();
        let status = self.status_code();
        let date = httpdate::fmt_http_date(std::time::SystemTime::now());

        // 304 Not Modified responses must not have a body.
        if matches!(self, S3Error::NotModified) {
            return (
                status,
                [
                    ("x-amz-request-id", request_id),
                    ("date", date),
                    ("server", "BleepStore".to_string()),
                ],
            )
                .into_response();
        }

        let body = render_error(self.code(), &self.to_string(), "", &request_id);

        (
            status,
            [
                ("content-type", "application/xml".to_string()),
                ("x-amz-request-id", request_id),
                ("date", date),
                ("server", "BleepStore".to_string()),
            ],
            body,
        )
            .into_response()
    }
}
