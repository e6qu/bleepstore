const std = @import("std");

/// S3-compatible error codes.
pub const S3Error = enum {
    // Client errors (4xx)
    AccessDenied,
    BadDigest,
    BucketAlreadyExists,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    EntityTooLarge,
    EntityTooSmall,
    IncompleteBody,
    InvalidAccessKeyId,
    InvalidArgument,
    InvalidBucketName,
    InvalidDigest,
    InvalidPart,
    InvalidPartOrder,
    InvalidRange,
    InvalidRequest,
    KeyTooLongError,
    MalformedACLError,
    MalformedXML,
    MethodNotAllowed,
    MissingContentLength,
    MissingRequestBodyError,
    NoSuchBucket,
    NoSuchKey,
    NoSuchUpload,
    PreconditionFailed,
    RequestTimeTooSkewed,
    SignatureDoesNotMatch,
    TooManyBuckets,

    // Server errors (5xx)
    InternalError,
    NotImplemented,
    ServiceUnavailable,

    /// Return the HTTP status code for this S3 error.
    pub fn httpStatus(self: S3Error) std.http.Status {
        return switch (self) {
            .AccessDenied => .forbidden,
            .BadDigest => .bad_request,
            .BucketAlreadyExists => .conflict,
            .BucketAlreadyOwnedByYou => .conflict,
            .BucketNotEmpty => .conflict,
            .EntityTooLarge => .payload_too_large,
            .EntityTooSmall => .bad_request,
            .IncompleteBody => .bad_request,
            .InvalidAccessKeyId => .forbidden,
            .InvalidArgument => .bad_request,
            .InvalidBucketName => .bad_request,
            .InvalidDigest => .bad_request,
            .InvalidPart => .bad_request,
            .InvalidPartOrder => .bad_request,
            .InvalidRange => .range_not_satisfiable,
            .InvalidRequest => .bad_request,
            .KeyTooLongError => .bad_request,
            .MalformedACLError => .bad_request,
            .MalformedXML => .bad_request,
            .MethodNotAllowed => .method_not_allowed,
            .MissingContentLength => .length_required,
            .MissingRequestBodyError => .bad_request,
            .NoSuchBucket => .not_found,
            .NoSuchKey => .not_found,
            .NoSuchUpload => .not_found,
            .PreconditionFailed => .precondition_failed,
            .RequestTimeTooSkewed => .forbidden,
            .SignatureDoesNotMatch => .forbidden,
            .TooManyBuckets => .bad_request,
            .InternalError => .internal_server_error,
            .NotImplemented => .not_implemented,
            .ServiceUnavailable => .service_unavailable,
        };
    }

    /// Return a human-readable message for this error.
    pub fn message(self: S3Error) []const u8 {
        return switch (self) {
            .AccessDenied => "Access Denied",
            .BadDigest => "The Content-MD5 you specified did not match what we received",
            .BucketAlreadyExists => "The requested bucket name is not available",
            .BucketAlreadyOwnedByYou => "The bucket you tried to create already exists, and you own it",
            .BucketNotEmpty => "The bucket you tried to delete is not empty",
            .EntityTooLarge => "Your proposed upload exceeds the maximum allowed size",
            .EntityTooSmall => "Your proposed upload is smaller than the minimum allowed size",
            .IncompleteBody => "You did not provide the number of bytes specified by the Content-Length HTTP header",
            .InvalidAccessKeyId => "The AWS access key ID you provided does not exist in our records",
            .InvalidArgument => "Invalid Argument",
            .InvalidBucketName => "The specified bucket is not valid",
            .InvalidDigest => "The Content-MD5 you specified is not valid",
            .InvalidPart => "One or more of the specified parts could not be found",
            .InvalidPartOrder => "The list of parts was not in ascending order",
            .InvalidRange => "The requested range is not satisfiable",
            .InvalidRequest => "Invalid Request",
            .KeyTooLongError => "Your key is too long",
            .MalformedACLError => "The XML you provided for the ACL is not well-formed or did not validate",
            .MalformedXML => "The XML you provided was not well-formed or did not validate against our published schema",
            .MethodNotAllowed => "The specified method is not allowed against this resource",
            .MissingContentLength => "You must provide the Content-Length HTTP header",
            .MissingRequestBodyError => "Request body is empty",
            .NoSuchBucket => "The specified bucket does not exist",
            .NoSuchKey => "The specified key does not exist",
            .NoSuchUpload => "The specified multipart upload does not exist",
            .PreconditionFailed => "At least one of the preconditions you specified did not hold",
            .RequestTimeTooSkewed => "The difference between the request time and the current time is too large",
            .SignatureDoesNotMatch => "The request signature we calculated does not match the signature you provided",
            .TooManyBuckets => "You have attempted to create more buckets than allowed",
            .InternalError => "We encountered an internal error. Please try again.",
            .NotImplemented => "A header you provided implies functionality that is not implemented",
            .ServiceUnavailable => "Reduce your request rate",
        };
    }

    /// Return the S3 error code string (matching AWS XML error codes).
    pub fn code(self: S3Error) []const u8 {
        return @tagName(self);
    }
};

test "S3Error httpStatus" {
    try std.testing.expectEqual(std.http.Status.not_found, S3Error.NoSuchBucket.httpStatus());
    try std.testing.expectEqual(std.http.Status.forbidden, S3Error.AccessDenied.httpStatus());
    try std.testing.expectEqual(std.http.Status.internal_server_error, S3Error.InternalError.httpStatus());
    try std.testing.expectEqual(std.http.Status.not_implemented, S3Error.NotImplemented.httpStatus());
    try std.testing.expectEqual(std.http.Status.precondition_failed, S3Error.PreconditionFailed.httpStatus());
    try std.testing.expectEqual(std.http.Status.forbidden, S3Error.InvalidAccessKeyId.httpStatus());
    try std.testing.expectEqual(std.http.Status.bad_request, S3Error.MalformedXML.httpStatus());
}

test "S3Error message" {
    try std.testing.expect(S3Error.NoSuchKey.message().len > 0);
    try std.testing.expectEqualStrings("Access Denied", S3Error.AccessDenied.message());
    try std.testing.expect(S3Error.PreconditionFailed.message().len > 0);
}

test "S3Error code" {
    try std.testing.expectEqualStrings("NoSuchBucket", S3Error.NoSuchBucket.code());
    try std.testing.expectEqualStrings("InvalidPartOrder", S3Error.InvalidPartOrder.code());
    try std.testing.expectEqualStrings("MalformedXML", S3Error.MalformedXML.code());
    try std.testing.expectEqualStrings("PreconditionFailed", S3Error.PreconditionFailed.code());
}
