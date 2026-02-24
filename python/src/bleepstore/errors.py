"""S3-compatible error definitions for BleepStore."""


class S3Error(Exception):
    """An S3-compatible error with code, message, and HTTP status.

    Attributes:
        code: The S3 error code string (e.g. "NoSuchBucket", "AccessDenied").
        message: Human-readable error description.
        http_status: The HTTP status code to return.
        extra_fields: Additional key-value pairs to include in the XML error response.
    """

    def __init__(
        self,
        code: str,
        message: str,
        http_status: int = 400,
        extra_fields: dict[str, str] | None = None,
    ) -> None:
        """Initialize the S3 error.

        Args:
            code: S3 error code.
            message: Error description.
            http_status: HTTP status code (default 400).
            extra_fields: Optional extra XML fields.
        """
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.extra_fields = extra_fields or {}


# -- Common pre-defined errors ------------------------------------------------


class AccessDenied(S3Error):
    """Access denied error."""

    def __init__(self, message: str = "Access Denied") -> None:
        super().__init__(code="AccessDenied", message=message, http_status=403)


class NoSuchBucket(S3Error):
    """The specified bucket does not exist."""

    def __init__(self, bucket: str = "") -> None:
        super().__init__(
            code="NoSuchBucket",
            message="The specified bucket does not exist.",
            http_status=404,
            extra_fields={"BucketName": bucket} if bucket else {},
        )


class NoSuchKey(S3Error):
    """The specified key does not exist."""

    def __init__(self, key: str = "") -> None:
        super().__init__(
            code="NoSuchKey",
            message="The specified key does not exist.",
            http_status=404,
            extra_fields={"Key": key} if key else {},
        )


class BucketAlreadyExists(S3Error):
    """The requested bucket name is already in use."""

    def __init__(self, bucket: str = "") -> None:
        super().__init__(
            code="BucketAlreadyExists",
            message="The requested bucket name is not available.",
            http_status=409,
            extra_fields={"BucketName": bucket} if bucket else {},
        )


class BucketNotEmpty(S3Error):
    """The bucket is not empty and cannot be deleted."""

    def __init__(self, bucket: str = "") -> None:
        super().__init__(
            code="BucketNotEmpty",
            message="The bucket you tried to delete is not empty.",
            http_status=409,
            extra_fields={"BucketName": bucket} if bucket else {},
        )


class InvalidArgument(S3Error):
    """An invalid argument was provided."""

    def __init__(self, message: str = "Invalid Argument") -> None:
        super().__init__(code="InvalidArgument", message=message, http_status=400)


class NoSuchUpload(S3Error):
    """The specified multipart upload does not exist."""

    def __init__(self, upload_id: str = "") -> None:
        super().__init__(
            code="NoSuchUpload",
            message="The specified multipart upload does not exist.",
            http_status=404,
            extra_fields={"UploadId": upload_id} if upload_id else {},
        )


class InternalError(S3Error):
    """An internal server error occurred."""

    def __init__(self, message: str = "Internal Error") -> None:
        super().__init__(code="InternalError", message=message, http_status=500)


class InvalidBucketName(S3Error):
    """The specified bucket name is not valid."""

    def __init__(self, bucket: str = "") -> None:
        super().__init__(
            code="InvalidBucketName",
            message="The specified bucket is not valid.",
            http_status=400,
            extra_fields={"BucketName": bucket} if bucket else {},
        )


class InvalidPart(S3Error):
    """One or more of the specified parts could not be found."""

    def __init__(
        self, message: str = "One or more of the specified parts could not be found."
    ) -> None:
        super().__init__(code="InvalidPart", message=message, http_status=400)


class InvalidPartOrder(S3Error):
    """The list of parts was not in ascending order."""

    def __init__(self, message: str = "The list of parts was not in ascending order.") -> None:
        super().__init__(code="InvalidPartOrder", message=message, http_status=400)


class InvalidRange(S3Error):
    """The requested range is not satisfiable."""

    def __init__(self, message: str = "The requested range is not satisfiable.") -> None:
        super().__init__(code="InvalidRange", message=message, http_status=416)


class EntityTooLarge(S3Error):
    """The proposed upload exceeds the maximum allowed object size."""

    def __init__(
        self, message: str = "Your proposed upload exceeds the maximum allowed object size."
    ) -> None:
        super().__init__(code="EntityTooLarge", message=message, http_status=400)


class EntityTooSmall(S3Error):
    """The proposed upload is smaller than the minimum allowed object size."""

    def __init__(
        self, message: str = "Your proposed upload is smaller than the minimum allowed object size."
    ) -> None:
        super().__init__(code="EntityTooSmall", message=message, http_status=400)


class SignatureDoesNotMatch(S3Error):
    """The request signature does not match."""

    def __init__(
        self,
        message: str = "The request signature we calculated does not match the signature you provided.",
    ) -> None:
        super().__init__(code="SignatureDoesNotMatch", message=message, http_status=403)


class MalformedXML(S3Error):
    """The XML provided was not well-formed or did not validate."""

    def __init__(
        self,
        message: str = "The XML you provided was not well-formed or did not validate against our published schema.",
    ) -> None:
        super().__init__(code="MalformedXML", message=message, http_status=400)


class MethodNotAllowed(S3Error):
    """The specified method is not allowed against this resource."""

    def __init__(
        self, message: str = "The specified method is not allowed against this resource."
    ) -> None:
        super().__init__(code="MethodNotAllowed", message=message, http_status=405)


class PreconditionFailed(S3Error):
    """At least one of the preconditions did not hold."""

    def __init__(
        self, message: str = "At least one of the pre-conditions you specified did not hold."
    ) -> None:
        super().__init__(code="PreconditionFailed", message=message, http_status=412)


class InvalidRequest(S3Error):
    """The request is not valid."""

    def __init__(self, message: str = "Invalid Request") -> None:
        super().__init__(code="InvalidRequest", message=message, http_status=400)


class MissingContentLength(S3Error):
    """The Content-Length HTTP header must be provided."""

    def __init__(self, message: str = "You must provide the Content-Length HTTP header.") -> None:
        super().__init__(code="MissingContentLength", message=message, http_status=411)


class KeyTooLongError(S3Error):
    """The specified key is too long."""

    def __init__(self, message: str = "Your key is too long.") -> None:
        super().__init__(code="KeyTooLongError", message=message, http_status=400)


class BucketAlreadyOwnedByYou(S3Error):
    """The bucket already exists and is owned by you."""

    def __init__(self, bucket: str = "") -> None:
        super().__init__(
            code="BucketAlreadyOwnedByYou",
            message="Your previous request to create the named bucket succeeded and you already own it.",
            http_status=409,
            extra_fields={"BucketName": bucket} if bucket else {},
        )


class InvalidAccessKeyId(S3Error):
    """The AWS access key Id you provided does not exist in our records."""

    def __init__(
        self, message: str = "The AWS access key Id you provided does not exist in our records."
    ) -> None:
        super().__init__(code="InvalidAccessKeyId", message=message, http_status=403)


class AuthorizationQueryParametersError(S3Error):
    """Error with authorization query parameters (presigned URLs)."""

    def __init__(
        self,
        message: str = "Query-string authentication requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.",
    ) -> None:
        super().__init__(code="AuthorizationQueryParametersError", message=message, http_status=400)


class RequestTimeTooSkewed(S3Error):
    """The difference between the request time and the server's time is too large."""

    def __init__(
        self,
        message: str = "The difference between the request time and the current time is too large.",
    ) -> None:
        super().__init__(code="RequestTimeTooSkewed", message=message, http_status=403)


class ExpiredPresignedUrl(S3Error):
    """The presigned URL has expired."""

    def __init__(self, message: str = "Request has expired.") -> None:
        super().__init__(code="AccessDenied", message=message, http_status=403)


class NotImplementedS3Error(S3Error):
    """The requested functionality is not implemented."""

    def __init__(
        self, message: str = "A header you provided implies functionality that is not implemented."
    ) -> None:
        super().__init__(code="NotImplemented", message=message, http_status=501)
