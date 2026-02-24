# S3 Error Responses — Complete Specification

## XML Error Format

**Content-Type:** `application/xml`
**Encoding:** UTF-8
**Note:** The `<Error>` root element has **no XML namespace** (unlike success responses).

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The resource you requested does not exist</Message>
  <Resource>/mybucket/myfoto.jpg</Resource>
  <RequestId>4442587FB7D0A2F9</RequestId>
  <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
</Error>
```

### Standard Fields

| Element | Type | Description |
|---|---|---|
| `Code` | String | Machine-readable error identifier |
| `Message` | String | Human-readable error description |
| `Resource` | String | Bucket or object involved |
| `RequestId` | String | Unique request ID (matches `x-amz-request-id` header) |
| `HostId` | String | Base64-encoded token (matches `x-amz-id-2` header) |

### Context-Specific Extra Fields

| Field | When Present |
|---|---|
| `BucketName` | Bucket-related errors |
| `Key` | Object key-related errors |
| `Region` | Region mismatch errors |
| `Endpoint` | Redirect errors |
| `ArgumentName` | `InvalidArgument` errors |
| `ArgumentValue` | `InvalidArgument` errors |
| `MaxSizeAllowed` | `EntityTooLarge` errors |
| `ProposedSize` | `EntityTooLarge` errors |
| `MinSizeAllowed` | `EntityTooSmall` errors |

---

## Special Cases

### HEAD Requests
HEAD responses have **no body**. Only HTTP status code and headers are returned. No XML error format.

### 200 OK with Embedded Errors
`CopyObject` and `CompleteMultipartUpload` can return `<Error>` inside a 200 OK response body. Clients must always parse the response body.

---

## Complete Error Code Reference

### Client Errors (4xx)

| Error Code | HTTP Status | Description |
|---|---|---|
| `AccessDenied` | 403 | Access denied |
| `BadDigest` | 400 | Content-MD5 mismatch |
| `BucketAlreadyExists` | 409 | Bucket name taken globally |
| `BucketAlreadyOwnedByYou` | 409 | You own this bucket (us-east-1: returns 200) |
| `BucketNotEmpty` | 409 | Bucket has objects |
| `EntityTooLarge` | 400 | Exceeds max object size |
| `EntityTooSmall` | 400 | Part smaller than 5 MiB |
| `ExpiredToken` | 400 | Security token expired |
| `IllegalLocationConstraintException` | 400 | Wrong region or illegal location |
| `IncompleteBody` | 400 | Body shorter than Content-Length |
| `InvalidAccessKeyId` | 403 | Access key not found |
| `InvalidArgument` | 400 | Invalid argument value |
| `InvalidBucketName` | 400 | Bucket name invalid |
| `InvalidDigest` | 400 | Content-MD5 not valid |
| `InvalidLocationConstraint` | 400 | Location constraint invalid |
| `InvalidObjectState` | 403 | Object archived (Glacier) |
| `InvalidPart` | 400 | Part not found or ETag mismatch |
| `InvalidPartOrder` | 400 | Parts not ascending |
| `InvalidRange` | 416 | Range not satisfiable |
| `InvalidRequest` | 400 | Generic invalid request |
| `KeyTooLongError` | 400 | Key exceeds 1024 bytes |
| `MalformedACLError` | 400 | ACL XML malformed |
| `MalformedXML` | 400 | XML not well-formed |
| `MethodNotAllowed` | 405 | HTTP method not allowed |
| `MissingContentLength` | 411 | Content-Length required |
| `MissingRequestBodyError` | 400 | Empty request body |
| `NoSuchBucket` | 404 | Bucket does not exist |
| `NoSuchKey` | 404 | Key does not exist |
| `NoSuchUpload` | 404 | Multipart upload not found |
| `NoSuchVersion` | 404 | Version not found |
| `PreconditionFailed` | 412 | Conditional check failed |
| `RequestTimeout` | 400 | Socket timeout |
| `RequestTimeTooSkewed` | 403 | Clock skew too large |
| `SignatureDoesNotMatch` | 403 | Signature mismatch |
| `TooManyBuckets` | 400 | Bucket limit exceeded |

### Server Errors (5xx)

| Error Code | HTTP Status | Description |
|---|---|---|
| `InternalError` | 500 | Internal server error |
| `NotImplemented` | 501 | Feature not implemented |
| `ServiceUnavailable` | 503 | Service unavailable |
| `SlowDown` | 503 | Rate limiting / throttle |

### Redirect Responses (3xx)

| Error Code | HTTP Status | Description |
|---|---|---|
| `PermanentRedirect` | 301 | Use specified endpoint |
| `TemporaryRedirect` | 307 | DNS propagation redirect |

---

## Common Response Headers

These headers should be on **all** responses (success and error):

| Header | Format | Description |
|---|---|---|
| `x-amz-request-id` | Hex string (e.g., `4442587FB7D0A2F9`) | Unique request identifier |
| `x-amz-id-2` | Base64 string | Extended request identifier |
| `Date` | RFC 1123 (e.g., `Wed, 01 Mar 2006 12:00:00 GMT`) | Response timestamp |
| `Server` | String | Server name (AWS uses `AmazonS3`) |
| `Content-Type` | MIME type | `application/xml` for errors |
| `Content-Length` | Integer | Response body size |
| `Connection` | `open` or `close` | Connection state |

---

## Implementation Notes

1. **Request ID generation**: Use random hex string (16 chars). `x-amz-id-2` should be random Base64 string.
2. **Error XML has no namespace**: Unlike success responses which use `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`.
3. **Content-Type for errors**: Always `application/xml` (never `text/xml` or `application/json`).
4. **HEAD requests**: No body, no XML — status code only.
5. **200+Error**: `CompleteMultipartUpload` and `CopyObject` only.
6. **BucketAlreadyOwnedByYou**: Returns 200 in us-east-1 for legacy compatibility.
7. **Error messages**: Always English. `encoding="UTF-8"` in XML declaration.
