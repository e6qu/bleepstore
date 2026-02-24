# S3 Object Operations — Complete Specification

## Common Elements

**XML Namespace URI:** `http://s3.amazonaws.com/doc/2006-03-01/`

**URL Pattern:** `/{Bucket}/{Key+}` (Key+ indicates the key may contain `/`)

---

## 1. PutObject

**Request:**
```
PUT /{Bucket}/{Key+} HTTP/1.1
```

### Request Headers

| Header | Required | Description |
|---|---|---|
| `Content-Length` | Yes | Size of the object body in bytes |
| `Content-Type` | No | MIME type (default: `application/octet-stream`) |
| `Content-MD5` | No | Base64-encoded 128-bit MD5 digest |
| `Content-Encoding` | No | Content encoding (e.g., `gzip`) |
| `Content-Disposition` | No | Content disposition |
| `Content-Language` | No | Content language |
| `Cache-Control` | No | Cache directives |
| `Expires` | No | Object expiration date |
| `x-amz-acl` | No | Canned ACL |
| `x-amz-grant-*` | No | Explicit grant headers |
| `x-amz-storage-class` | No | `STANDARD`, `REDUCED_REDUNDANCY`, `STANDARD_IA`, etc. |
| `x-amz-tagging` | No | URL-encoded key=value pairs |
| `x-amz-meta-*` | No | User-defined metadata (total limit: 2 KB) |
| `x-amz-server-side-encryption` | No | `AES256`, `aws:kms` |
| `x-amz-website-redirect-location` | No | Redirect URL |
| `If-None-Match` | No | `*` — only write if object doesn't exist |
| `x-amz-content-sha256` | Yes | SHA-256 hex digest or `UNSIGNED-PAYLOAD` |

### Request Body
Raw binary object data.

### Success Response

```
HTTP/1.1 200 OK
ETag: "d41d8cd98f00b204e9800998ecf8427e"
```

### Response Headers

| Header | Description |
|---|---|
| `ETag` | Quoted MD5 hex digest (for non-SSE-KMS uploads) |
| `x-amz-version-id` | Version ID (if versioning enabled) |
| `x-amz-server-side-encryption` | Encryption algorithm used |
| `x-amz-expiration` | Lifecycle expiration info |

### Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `EntityTooLarge` | 400 | Object exceeds 5 TB max |
| `AccessDenied` | 403 | Insufficient permissions |
| `NoSuchBucket` | 404 | Bucket does not exist |

---

## 2. GetObject

**Request:**
```
GET /{Bucket}/{Key+} HTTP/1.1
```

### Query Parameters

| Parameter | Description |
|---|---|
| `versionId` | Retrieve specific version |
| `response-cache-control` | Override Cache-Control |
| `response-content-disposition` | Override Content-Disposition |
| `response-content-encoding` | Override Content-Encoding |
| `response-content-language` | Override Content-Language |
| `response-content-type` | Override Content-Type |
| `response-expires` | Override Expires |

### Request Headers

| Header | Required | Description |
|---|---|---|
| `Range` | No | `bytes=start-end` (single range only; S3 does not support multi-range) |
| `If-Match` | No | Return only if ETag matches |
| `If-None-Match` | No | Return only if ETag does NOT match |
| `If-Modified-Since` | No | Return only if modified after date |
| `If-Unmodified-Since` | No | Return only if NOT modified after date |

### Response — 200 OK (or 206 for Range)

| Header | Description |
|---|---|
| `Content-Length` | Object size in bytes |
| `Content-Type` | MIME type |
| `Content-Range` | `bytes start-end/total` (206 only) |
| `Accept-Ranges` | `bytes` |
| `ETag` | Entity tag |
| `Last-Modified` | RFC 7231 date |
| `x-amz-version-id` | Version ID |
| `x-amz-delete-marker` | `true`/`false` |
| `x-amz-storage-class` | Storage class |
| `x-amz-meta-*` | User-defined metadata |

### Conditional Request Behavior

| Condition | Match | No Match |
|---|---|---|
| `If-Match` | 200 + body | 412 Precondition Failed |
| `If-None-Match` | 304 Not Modified | 200 + body |
| `If-Modified-Since` | 200 + body | 304 Not Modified |
| `If-Unmodified-Since` | 200 + body | 412 Precondition Failed |

**Priority:** `If-Match` takes precedence over `If-Unmodified-Since`. `If-None-Match` takes precedence over `If-Modified-Since`.

### Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `NoSuchKey` | 404 | Object does not exist |
| `InvalidRange` | 416 | Range not satisfiable |
| `AccessDenied` | 403 | Insufficient permissions |
| `PreconditionFailed` | 412 | Conditional header not met |

---

## 3. HeadObject

**Request:**
```
HEAD /{Bucket}/{Key+} HTTP/1.1
```

All request headers and query parameters identical to GetObject.

### Response
All response headers identical to GetObject. **No response body.**

Error details conveyed entirely through HTTP status codes (no XML body for HEAD).

---

## 4. DeleteObject

**Request:**
```
DELETE /{Bucket}/{Key+} HTTP/1.1
```

### Query Parameters

| Parameter | Description |
|---|---|
| `versionId` | Delete specific version |

### Success Response

```
HTTP/1.1 204 No Content
```

### Response Headers

| Header | Description |
|---|---|
| `x-amz-delete-marker` | `true` if delete marker created |
| `x-amz-version-id` | Version ID of delete marker or deleted version |

**Note:** Returns 204 even if the key does not exist (idempotent).

---

## 5. DeleteObjects (Multi-Object Delete)

**Request:**
```
POST /{Bucket}?delete HTTP/1.1
Content-MD5: {base64-md5}
Content-Type: application/xml
```

### Request Body

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Quiet>false</Quiet>
    <Object>
        <Key>string</Key>
        <VersionId>string</VersionId>
    </Object>
    <!-- up to 1,000 Object elements -->
</Delete>
```

| Element | Required | Description |
|---|---|---|
| `Delete` | Yes | Root container |
| `Quiet` | No | `true` = only errors in response; `false` (default) = all results |
| `Object` | Yes | One per object, max **1,000** per request |
| `Key` | Yes | Object key |
| `VersionId` | No | Version to delete |

### Response — 200 OK

```xml
<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Deleted>
        <Key>string</Key>
        <VersionId>string</VersionId>
        <DeleteMarker>true</DeleteMarker>
        <DeleteMarkerVersionId>string</DeleteMarkerVersionId>
    </Deleted>
    <Error>
        <Key>string</Key>
        <Code>AccessDenied</Code>
        <Message>Access Denied</Message>
    </Error>
</DeleteResult>
```

**Important:** HTTP status is 200 even if individual deletions fail. Check for `<Error>` elements.

---

## 6. CopyObject

**Request:**
```
PUT /{Bucket}/{Key+} HTTP/1.1
x-amz-copy-source: /{source-bucket}/{source-key}
```

Distinguished from PutObject by the presence of `x-amz-copy-source`.

### Request Headers

| Header | Required | Description |
|---|---|---|
| `x-amz-copy-source` | **Yes** | `/{source-bucket}/{source-key}` (URL-encoded). Max source: 5 GB |
| `x-amz-metadata-directive` | No | `COPY` (default) or `REPLACE` |
| `x-amz-tagging-directive` | No | `COPY` (default) or `REPLACE` |
| `x-amz-copy-source-if-match` | No | Copy only if source ETag matches |
| `x-amz-copy-source-if-none-match` | No | Copy only if source ETag does NOT match |
| `x-amz-copy-source-if-modified-since` | No | Conditional on modification time |
| `x-amz-copy-source-if-unmodified-since` | No | Conditional on modification time |

### Response — 200 OK

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult>
    <ETag>string</ETag>
    <LastModified>2009-10-12T17:50:30.000Z</LastModified>
</CopyObjectResult>
```

**Critical:** CopyObject can return HTTP 200 with `<Error>` in body. Always parse the body.

### Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `NoSuchKey` | 404 | Source object not found |
| `EntityTooLarge` | 400 | Source exceeds 5 GB |
| `PreconditionFailed` | 412 | Conditional copy failed |

---

## 7. ListObjectsV2

**Request:**
```
GET /{Bucket}?list-type=2 HTTP/1.1
```

### Query Parameters

| Parameter | Required | Description |
|---|---|---|
| `list-type` | **Yes** | Must be `2` |
| `prefix` | No | Filter keys by prefix |
| `delimiter` | No | Grouping character (typically `/`) |
| `max-keys` | No | Max keys to return (default/max: 1000) |
| `continuation-token` | No | Token from `NextContinuationToken` for pagination |
| `start-after` | No | Key to start listing after |
| `fetch-owner` | No | `true` to include Owner |
| `encoding-type` | No | `url` to URL-encode keys |

### Response — 200 OK

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>bucket-name</Name>
    <Prefix>string</Prefix>
    <Delimiter>string</Delimiter>
    <MaxKeys>1000</MaxKeys>
    <KeyCount>250</KeyCount>
    <IsTruncated>false</IsTruncated>
    <NextContinuationToken>string</NextContinuationToken>
    <Contents>
        <Key>string</Key>
        <LastModified>2009-10-12T17:50:30.000Z</LastModified>
        <ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
        <Size>434234</Size>
        <StorageClass>STANDARD</StorageClass>
    </Contents>
    <CommonPrefixes>
        <Prefix>photos/</Prefix>
    </CommonPrefixes>
</ListBucketResult>
```

### Pagination
When `IsTruncated=true`, use `NextContinuationToken` as `continuation-token` in the next request.

---

## 8. ListObjects (Legacy V1)

**Request:**
```
GET /{Bucket} HTTP/1.1
```

No `list-type` parameter.

### Query Parameters

| Parameter | Description |
|---|---|
| `prefix` | Filter keys by prefix |
| `delimiter` | Grouping character |
| `marker` | Key to start listing after |
| `max-keys` | Max keys (default/max: 1000) |
| `encoding-type` | `url` |

### Key Differences from V2
- Uses `Marker`/`NextMarker` instead of `ContinuationToken`/`NextContinuationToken`
- No `KeyCount` element
- Owner always included (no `fetch-owner` parameter)
- `NextMarker` only returned when delimiter is used; otherwise use last `Key` as next marker

---

## 9. GetObjectAcl

**Request:**
```
GET /{Bucket}/{Key+}?acl HTTP/1.1
```

### Response — 200 OK

Same `<AccessControlPolicy>` XML format as GetBucketAcl (see bucket operations spec).

---

## 10. PutObjectAcl

**Request:**
```
PUT /{Bucket}/{Key+}?acl HTTP/1.1
```

Three mutually exclusive modes (same as PutBucketAcl):
1. Canned ACL via `x-amz-acl` header
2. Explicit grants via `x-amz-grant-*` headers
3. Full ACL XML body

### Object-Specific Canned ACLs

| Canned ACL | Description |
|---|---|
| `private` | Owner gets FULL_CONTROL |
| `public-read` | Owner FULL_CONTROL, AllUsers READ |
| `public-read-write` | Owner FULL_CONTROL, AllUsers READ + WRITE |
| `authenticated-read` | Owner FULL_CONTROL, AuthenticatedUsers READ |
| `bucket-owner-read` | Object owner FULL_CONTROL, bucket owner READ |
| `bucket-owner-full-control` | Both owners FULL_CONTROL |

---

## Implementation Notes

1. **ETag format**: Non-multipart uploads: quoted MD5 hex (`"d41d8cd98f00b204e9800998ecf8427e"`). Multipart uploads: `"{md5-of-md5s}-{part-count}"`.
2. **Date formats**: `LastModified` in XML: ISO 8601 (`2009-10-12T17:50:30.000Z`). HTTP headers: RFC 7231 (`Wed, 12 Oct 2009 17:50:00 GMT`).
3. **Range requests**: S3 supports only single byte ranges. Multi-range not supported. Range requests return 206; invalid ranges return 416.
4. **User-defined metadata**: Stored via `x-amz-meta-*` headers. Keys are case-insensitive. Total limit: 2 KB.
5. **Content-MD5**: Base64 of raw binary MD5 (not hex string). Required for DeleteObjects, recommended for PutObject.
6. **CopyObject 200+Error**: Always parse response body even on 200. Body may contain `<Error>` instead of `<CopyObjectResult>`.
7. **URL encoding**: Keys in URL paths must be URL-encoded. When `encoding-type=url` in list operations, response XML URL-encodes Key, Prefix, Delimiter, Marker, StartAfter.
8. **DeleteObject is idempotent**: Returns 204 whether or not the key exists.
