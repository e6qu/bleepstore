# S3 Common Headers — Reference

## Common Request Headers

| Header | Required | Description |
|---|---|---|
| `Authorization` | Yes | SigV4 auth: `AWS4-HMAC-SHA256 Credential=.../.../.../s3/aws4_request, SignedHeaders=..., Signature=...` |
| `Host` | Yes | Endpoint hostname (include port if non-standard) |
| `x-amz-date` | Yes (or `Date`) | Request timestamp: `YYYYMMDDTHHMMSSZ` |
| `x-amz-content-sha256` | Yes (S3) | SHA-256 hex of body, `UNSIGNED-PAYLOAD`, or `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` |
| `x-amz-security-token` | Conditional | STS session token (temporary credentials only) |
| `Content-Length` | Conditional | Body size in bytes (required for PUT, POST with body) |
| `Content-Type` | No | MIME type of body |
| `Content-MD5` | Conditional | Base64 of binary MD5 (required for DeleteObjects, PutBucketAcl, PutObjectAcl) |
| `x-amz-expected-bucket-owner` | No | Account ID for ownership validation |
| `x-amz-request-payer` | No | `requester` for Requester Pays buckets |

## Common Response Headers

| Header | Description |
|---|---|
| `x-amz-request-id` | Unique request identifier (hex string, e.g., `4442587FB7D0A2F9`) |
| `x-amz-id-2` | Extended request ID (Base64 string, for troubleshooting) |
| `Date` | RFC 1123 date: `Wed, 01 Mar 2006 12:00:00 GMT` |
| `Server` | Server identifier (BleepStore should use `BleepStore`) |
| `Content-Type` | Response MIME type (`application/xml` for XML responses) |
| `Content-Length` | Response body size in bytes |
| `Connection` | `close` or `keep-alive` |

## Object-Specific Response Headers

| Header | Description |
|---|---|
| `ETag` | Entity tag, quoted: `"d41d8cd98f00b204e9800998ecf8427e"` |
| `Last-Modified` | RFC 7231 date: `Wed, 12 Oct 2009 17:50:00 GMT` |
| `Content-Range` | `bytes start-end/total` (206 responses only) |
| `Accept-Ranges` | `bytes` |
| `x-amz-version-id` | Object version ID |
| `x-amz-delete-marker` | `true` if object is a delete marker |
| `x-amz-storage-class` | Storage class |
| `x-amz-expiration` | Lifecycle expiration: `expiry-date="date", rule-id="id"` |
| `x-amz-server-side-encryption` | `AES256` or `aws:kms` |
| `x-amz-meta-*` | User-defined metadata |

## Conditional Request Headers

| Header | Applies To | Description |
|---|---|---|
| `If-Match` | GET, HEAD, PUT, DELETE | Succeed if ETag matches |
| `If-None-Match` | GET, HEAD, PUT | Succeed if ETag does NOT match |
| `If-Modified-Since` | GET, HEAD | Succeed if modified after date |
| `If-Unmodified-Since` | GET, HEAD | Succeed if NOT modified after date |
| `x-amz-copy-source-if-match` | CopyObject, UploadPartCopy | Conditional on source ETag |
| `x-amz-copy-source-if-none-match` | CopyObject, UploadPartCopy | Conditional on source ETag |
| `x-amz-copy-source-if-modified-since` | CopyObject, UploadPartCopy | Conditional on source date |
| `x-amz-copy-source-if-unmodified-since` | CopyObject, UploadPartCopy | Conditional on source date |

## ACL Headers

| Header | Description |
|---|---|
| `x-amz-acl` | Canned ACL: `private`, `public-read`, `public-read-write`, `authenticated-read`, `bucket-owner-read`, `bucket-owner-full-control` |
| `x-amz-grant-full-control` | `id="canonical-user-id"` or `uri="group-uri"` |
| `x-amz-grant-read` | Same format |
| `x-amz-grant-read-acp` | Same format |
| `x-amz-grant-write` | Same format |
| `x-amz-grant-write-acp` | Same format |

**Note:** `x-amz-acl` and `x-amz-grant-*` are mutually exclusive.

## Date Formats

| Context | Format | Example |
|---|---|---|
| `x-amz-date` header | ISO 8601 basic | `20260222T120000Z` |
| `Date` header | RFC 7231 | `Sun, 22 Feb 2026 12:00:00 GMT` |
| `Last-Modified` header | RFC 7231 | `Sun, 22 Feb 2026 12:00:00 GMT` |
| XML `LastModified` | ISO 8601 | `2026-02-22T12:00:00.000Z` |
| XML `CreationDate` | ISO 8601 | `2026-02-22T12:00:00.000Z` |
| Conditional headers | RFC 7231 | `Sun, 22 Feb 2026 12:00:00 GMT` |

## Content-MD5 Encoding

`Content-MD5` is the **Base64** encoding of the raw **binary** 128-bit MD5 digest (not the hex string).

Example:
- Body MD5 (hex): `d41d8cd98f00b204e9800998ecf8427e`
- Body MD5 (binary): 16 bytes
- Content-MD5: `1B2M2Y8AsgTpgAmY7PhCfg==`
