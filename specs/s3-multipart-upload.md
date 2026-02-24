# S3 Multipart Upload — Complete Specification

## Constants and Limits

| Constraint | Value |
|---|---|
| Minimum part size | 5 MiB (5,242,880 bytes) — all parts except the last |
| Maximum part size | 5 GiB (5,368,709,120 bytes) |
| Maximum number of parts | 10,000 |
| Maximum object size | 5 TiB |
| Part number range | 1 to 10,000 inclusive |
| Last part | No minimum size (can be 0 bytes) |

## ETag Handling

- **Per-part ETag**: MD5 hex digest of part data, quoted: `"b54357faf0632cce46e942fa68356b38"`
- **Composite ETag** (after completion): `MD5(concat(MD5_part1_binary, MD5_part2_binary, ..., MD5_partN_binary))` hex-encoded with `-N` suffix where N = number of parts. Example: `"3858f62230ac3c915f300c664312c11f-9"`
- The composite ETag is NOT the MD5 of the assembled object data

---

## 1. CreateMultipartUpload

**Request:**
```
POST /{Bucket}/{Key+}?uploads HTTP/1.1
```

### Request Headers

| Header | Required | Description |
|---|---|---|
| `Content-Type` | No | MIME type for the final object |
| `Cache-Control` | No | Cache directives |
| `Content-Disposition` | No | Disposition |
| `Content-Encoding` | No | Encoding |
| `Content-Language` | No | Language |
| `Expires` | No | Expiration date |
| `x-amz-acl` | No | Canned ACL |
| `x-amz-grant-*` | No | Grant headers |
| `x-amz-storage-class` | No | Storage class |
| `x-amz-tagging` | No | URL-encoded `key1=value1&key2=value2` |
| `x-amz-meta-*` | No | User-defined metadata |
| `x-amz-server-side-encryption` | No | `AES256` or `aws:kms` |

### Response — 200 OK

```xml
<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Bucket>string</Bucket>
   <Key>string</Key>
   <UploadId>string</UploadId>
</InitiateMultipartUploadResult>
```

### Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `NoSuchBucket` | 404 | Bucket does not exist |
| `AccessDenied` | 403 | Insufficient permissions |

---

## 2. UploadPart

**Request:**
```
PUT /{Bucket}/{Key+}?partNumber={N}&uploadId={ID} HTTP/1.1
```

### Query Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `partNumber` | Integer | Yes | 1 to 10,000 |
| `uploadId` | String | Yes | Upload ID from CreateMultipartUpload |

### Request Headers

| Header | Required | Description |
|---|---|---|
| `Content-Length` | Yes | Size of part body |
| `Content-MD5` | No | Base64-encoded MD5 |

### Request Body
Raw binary data of the part.

### Response — 200 OK

| Header | Description |
|---|---|
| `ETag` | Quoted MD5 hex digest of the part. **Must be saved for CompleteMultipartUpload.** |

### Key Behaviors
- Uploading with the same part number **overwrites** the previous part
- SSE-C headers must be identical to those used in CreateMultipartUpload

### Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `NoSuchUpload` | 404 | Upload ID invalid or already completed/aborted |
| `InvalidArgument` | 400 | Invalid part number |

---

## 3. UploadPartCopy

**Request:**
```
PUT /{Bucket}/{Key+}?partNumber={N}&uploadId={ID} HTTP/1.1
x-amz-copy-source: /{source-bucket}/{source-key}
```

Distinguished from UploadPart by the presence of `x-amz-copy-source`.

### Request Headers

| Header | Required | Description |
|---|---|---|
| `x-amz-copy-source` | **Yes** | `/{SourceBucket}/{SourceKey}` URL-encoded |
| `x-amz-copy-source-range` | No | `bytes={first}-{last}` (0-based, inclusive) |
| `x-amz-copy-source-if-match` | No | Conditional ETag |
| `x-amz-copy-source-if-none-match` | No | Conditional ETag |
| `x-amz-copy-source-if-modified-since` | No | Conditional date |
| `x-amz-copy-source-if-unmodified-since` | No | Conditional date |

### Response — 200 OK

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CopyPartResult>
   <ETag>string</ETag>
   <LastModified>2011-04-11T20:34:56.000Z</LastModified>
</CopyPartResult>
```

### Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `NoSuchUpload` | 404 | Upload ID invalid |
| `NoSuchKey` | 404 | Source key does not exist |
| `PreconditionFailed` | 412 | Conditional copy failed |

---

## 4. CompleteMultipartUpload

**Request:**
```
POST /{Bucket}/{Key+}?uploadId={ID} HTTP/1.1
```

### Request Body

```xml
<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Part>
      <PartNumber>1</PartNumber>
      <ETag>"a54357faf0632cce46e942fa68356b38"</ETag>
   </Part>
   <Part>
      <PartNumber>2</PartNumber>
      <ETag>"0c78aef83f66abc1fa1e8477f296d394"</ETag>
   </Part>
</CompleteMultipartUpload>
```

### Rules
- Parts **must** be in ascending `PartNumber` order
- Each `Part` requires `PartNumber` and `ETag`
- `ETag` values must match those returned by UploadPart/UploadPartCopy
- All parts except the last must be >= 5 MiB
- You can omit parts — only listed parts are assembled

### Response — 200 OK

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Location>http://bucket.s3.us-east-1.amazonaws.com/Key</Location>
   <Bucket>string</Bucket>
   <Key>string</Key>
   <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
</CompleteMultipartUploadResult>
```

**CRITICAL:** S3 may return HTTP 200 with `<Error>` body instead of success result. Implementations **must** parse the body even on 200.

### Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `NoSuchUpload` | 404 | Upload ID invalid |
| `InvalidPart` | 400 | Part not found or ETag mismatch |
| `InvalidPartOrder` | 400 | Parts not in ascending order |
| `EntityTooSmall` | 400 | Part (not last) smaller than 5 MiB |
| `InternalError` | 200 (embedded) or 500 | Server error — always retry |

---

## 5. AbortMultipartUpload

**Request:**
```
DELETE /{Bucket}/{Key+}?uploadId={ID} HTTP/1.1
```

### Response — 204 No Content

No response body.

### Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `NoSuchUpload` | 404 | Upload does not exist |
| `AccessDenied` | 403 | No permission |

---

## 6. ListMultipartUploads

**Request:**
```
GET /{Bucket}?uploads HTTP/1.1
```

### Query Parameters

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `uploads` | Flag | Yes | — | Literal flag |
| `delimiter` | String | No | — | Grouping character |
| `prefix` | String | No | — | Filter by prefix |
| `key-marker` | String | No | — | Start after this key |
| `upload-id-marker` | String | No | — | With key-marker, start position |
| `max-uploads` | Integer | No | 1000 | Max uploads (1-1000) |
| `encoding-type` | String | No | — | `url` |

### Response — 200 OK

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Bucket>string</Bucket>
   <KeyMarker>string</KeyMarker>
   <UploadIdMarker>string</UploadIdMarker>
   <NextKeyMarker>string</NextKeyMarker>
   <NextUploadIdMarker>string</NextUploadIdMarker>
   <MaxUploads>1000</MaxUploads>
   <IsTruncated>false</IsTruncated>
   <Upload>
      <Key>string</Key>
      <UploadId>string</UploadId>
      <Initiator>
         <ID>string</ID>
         <DisplayName>string</DisplayName>
      </Initiator>
      <Owner>
         <ID>string</ID>
         <DisplayName>string</DisplayName>
      </Owner>
      <StorageClass>STANDARD</StorageClass>
      <Initiated>2009-10-12T17:50:30.000Z</Initiated>
   </Upload>
   <CommonPrefixes>
      <Prefix>photos/</Prefix>
   </CommonPrefixes>
</ListMultipartUploadsResult>
```

**Pagination:** When `IsTruncated=true`, use `NextKeyMarker` as `key-marker` and `NextUploadIdMarker` as `upload-id-marker`.

**Sorting:** Lexicographic by key, then by initiation time for same key.

---

## 7. ListParts

**Request:**
```
GET /{Bucket}/{Key+}?uploadId={ID} HTTP/1.1
```

### Query Parameters

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `uploadId` | String | Yes | — | Upload ID |
| `max-parts` | Integer | No | 1000 | Max parts (1-1000) |
| `part-number-marker` | Integer | No | — | List parts after this number |

### Response — 200 OK

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Bucket>string</Bucket>
   <Key>string</Key>
   <UploadId>string</UploadId>
   <Initiator>
      <ID>string</ID>
      <DisplayName>string</DisplayName>
   </Initiator>
   <Owner>
      <ID>string</ID>
      <DisplayName>string</DisplayName>
   </Owner>
   <StorageClass>STANDARD</StorageClass>
   <PartNumberMarker>0</PartNumberMarker>
   <NextPartNumberMarker>3</NextPartNumberMarker>
   <MaxParts>1000</MaxParts>
   <IsTruncated>false</IsTruncated>
   <Part>
      <PartNumber>1</PartNumber>
      <LastModified>2009-10-12T17:50:30.000Z</LastModified>
      <ETag>"b54357faf0632cce46e942fa68356b38"</ETag>
      <Size>5242880</Size>
   </Part>
</ListPartsResult>
```

**Pagination:** When `IsTruncated=true`, use `NextPartNumberMarker` as `part-number-marker`.

### Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `NoSuchUpload` | 404 | Upload invalid or completed/aborted |
| `AccessDenied` | 403 | Insufficient permissions |

---

## Implementation Checklist

1. **XML namespace**: All response root elements use `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`
2. **Part size enforcement**: At CompleteMultipartUpload time (not at UploadPart time)
3. **CompleteMultipartUpload 200+Error**: Body must be checked for `<Error>` on 200
4. **Part ordering**: Return `InvalidPartOrder` (400) if not ascending
5. **Part overwrite**: Same part number replaces previous
6. **Upload ID lifetime**: Valid from Create until Complete or Abort; then `NoSuchUpload`
7. **AbortMultipartUpload returns 204**: Not 200
8. **Content-Type on Complete**: Must not be `application/x-www-form-urlencoded`
9. **Composite ETag**: `MD5(binary_MD5s_concatenated)` hex with `-N` suffix
