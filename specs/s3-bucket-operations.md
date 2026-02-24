# S3 Bucket Operations — Complete Specification

## Common Elements

**XML Namespace URI:** `http://s3.amazonaws.com/doc/2006-03-01/`

**URL Styles:**
- Virtual-hosted style: `https://{BucketName}.s3.{region}.amazonaws.com/`
- Path style: `https://s3.{region}.amazonaws.com/{BucketName}/`

---

## 1. CreateBucket

**Request:**
```
PUT /{Bucket} HTTP/1.1
Host: s3.{region}.amazonaws.com
```

### Request Headers

| Header | Required | Description |
|---|---|---|
| `Content-Type` | No | `application/xml` |
| `Content-Length` | If body | Length of request body |
| `x-amz-acl` | No | Canned ACL: `private` (default), `public-read`, `public-read-write`, `authenticated-read` |
| `x-amz-grant-full-control` | No | `id="canonical-user-id"` |
| `x-amz-grant-read` | No | `id="canonical-user-id"` |
| `x-amz-grant-read-acp` | No | `id="canonical-user-id"` |
| `x-amz-grant-write` | No | `id="canonical-user-id"` |
| `x-amz-grant-write-acp` | No | `id="canonical-user-id"` |

**Note:** `x-amz-acl` and `x-amz-grant-*` headers are mutually exclusive.

### Request Body (optional, required if not us-east-1)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <LocationConstraint>us-west-2</LocationConstraint>
</CreateBucketConfiguration>
```

### Success Response

```
HTTP/1.1 200 OK
Location: /{BucketName}
Content-Length: 0
```

### Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `BucketAlreadyExists` | 409 | Bucket name taken globally |
| `BucketAlreadyOwnedByYou` | 409 | You already own this bucket (us-east-1 returns 200 instead) |
| `TooManyBuckets` | 400 | Account bucket limit reached |
| `InvalidBucketName` | 400 | Name does not meet naming rules |
| `InvalidLocationConstraint` | 400 | Invalid region code |

### Bucket Naming Rules
- 3-63 characters long
- Lowercase letters, numbers, hyphens, and periods only
- Must begin and end with a letter or number
- Cannot be formatted as an IP address (e.g., 192.168.5.4)
- Must not start with `xn--` or end with `-s3alias` or `--ol-s3`

---

## 2. DeleteBucket

**Request:**
```
DELETE /{Bucket} HTTP/1.1
Host: s3.{region}.amazonaws.com
```

### Request Headers

| Header | Required | Description |
|---|---|---|
| `x-amz-expected-bucket-owner` | No | Account ID for ownership validation |

### Success Response

```
HTTP/1.1 204 No Content
Content-Length: 0
```

### Preconditions
- Bucket must be completely empty (no objects, no incomplete multipart uploads)

### Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `NoSuchBucket` | 404 | Bucket does not exist |
| `BucketNotEmpty` | 409 | Bucket still contains objects |
| `AccessDenied` | 403 | Not the bucket owner |

---

## 3. HeadBucket

**Request:**
```
HEAD /{Bucket} HTTP/1.1
Host: s3.{region}.amazonaws.com
```

### Success Response

```
HTTP/1.1 200 OK
x-amz-bucket-region: us-west-2
```

### Response Headers

| Header | Description |
|---|---|
| `x-amz-bucket-region` | Region where bucket resides |

**Note:** HEAD responses never have a body. Error codes are conveyed solely through HTTP status codes.

### Error Status Codes

| HTTP Status | Description |
|---|---|
| 301 | Bucket exists in a different region (`x-amz-bucket-region` header indicates correct region) |
| 403 | Access denied |
| 404 | Bucket does not exist |

---

## 4. ListBuckets

**Request:**
```
GET / HTTP/1.1
Host: s3.{region}.amazonaws.com
```

**Note:** Called on the S3 service endpoint, not a bucket-specific host.

### Query Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `continuation-token` | String | No | Pagination token from previous response |
| `max-buckets` | Integer | No | Max buckets to return (1-10000, default 10000) |
| `prefix` | String | No | Filter bucket names by prefix |

### Success Response

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Owner>
        <ID>bcaf1ffd86f41161ca5fb16fd081034f</ID>
        <DisplayName>webfile</DisplayName>
    </Owner>
    <Buckets>
        <Bucket>
            <Name>my-bucket</Name>
            <CreationDate>2006-02-03T16:45:09.000Z</CreationDate>
        </Bucket>
    </Buckets>
    <ContinuationToken>...</ContinuationToken>
</ListAllMyBucketsResult>
```

### Response Elements

| Element | Type | Description |
|---|---|---|
| `ListAllMyBucketsResult` | Container | Root element |
| `Owner/ID` | String | Canonical user ID |
| `Owner/DisplayName` | String | Display name |
| `Buckets/Bucket/Name` | String | Bucket name |
| `Buckets/Bucket/CreationDate` | ISO 8601 | When bucket was created |
| `ContinuationToken` | String | Token for next page (if more results) |

---

## 5. GetBucketLocation

**Request:**
```
GET /{Bucket}?location HTTP/1.1
Host: s3.{region}.amazonaws.com
```

### Success Response

```xml
<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-west-2</LocationConstraint>
```

**us-east-1 quirk:** Returns empty/self-closing `<LocationConstraint/>` (effectively null), not the string `us-east-1`.

### Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `NoSuchBucket` | 404 | Bucket does not exist |
| `AccessDenied` | 403 | Not authorized |

---

## 6. GetBucketAcl

**Request:**
```
GET /{Bucket}?acl HTTP/1.1
Host: s3.{region}.amazonaws.com
```

### Success Response

```xml
<?xml version="1.0" encoding="UTF-8"?>
<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Owner>
        <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
        <DisplayName>CustomersName@amazon.com</DisplayName>
    </Owner>
    <AccessControlList>
        <Grant>
            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                     xsi:type="CanonicalUser">
                <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                <DisplayName>CustomersName@amazon.com</DisplayName>
            </Grantee>
            <Permission>FULL_CONTROL</Permission>
        </Grant>
    </AccessControlList>
</AccessControlPolicy>
```

### Grantee Types (via `xsi:type`)

| xsi:type | Identifying Element | Description |
|---|---|---|
| `CanonicalUser` | `<ID>`, `<DisplayName>` | Specific account |
| `Group` | `<URI>` | Predefined group |

### Permission Values

| Permission | Bucket Meaning |
|---|---|
| `FULL_CONTROL` | READ, WRITE, READ_ACP, WRITE_ACP |
| `READ` | List objects |
| `WRITE` | Create/overwrite/delete objects |
| `READ_ACP` | Read bucket ACL |
| `WRITE_ACP` | Write bucket ACL |

### Predefined Group URIs

| URI | Description |
|---|---|
| `http://acs.amazonaws.com/groups/global/AllUsers` | Anyone (public) |
| `http://acs.amazonaws.com/groups/global/AuthenticatedUsers` | Any authenticated AWS account |

---

## 7. PutBucketAcl

**Request:**
```
PUT /{Bucket}?acl HTTP/1.1
Host: s3.{region}.amazonaws.com
```

### Three Mutually Exclusive Modes

1. **Canned ACL** via `x-amz-acl` header (no body)
2. **Explicit grants** via `x-amz-grant-*` headers (no body)
3. **XML body** with full ACL specification

### Request Headers

| Header | Required | Description |
|---|---|---|
| `Content-Type` | If body | `application/xml` |
| `Content-MD5` | No | Base64 of 128-bit MD5 of body |
| `x-amz-acl` | No | Canned ACL: `private`, `public-read`, `public-read-write`, `authenticated-read` |
| `x-amz-grant-*` | No | Grant headers (same as CreateBucket) |

### Request Body (when using XML mode)

Same `<AccessControlPolicy>` format as GetBucketAcl response.

### Success Response

```
HTTP/1.1 200 OK
Content-Length: 0
```

### Error Codes

| Code | HTTP Status | Description |
|---|---|---|
| `NoSuchBucket` | 404 | Bucket does not exist |
| `AccessDenied` | 403 | No WRITE_ACP permission |
| `MalformedACLError` | 400 | XML body not well-formed |

---

## Implementation Notes

1. **DeleteBucket returns 204**, not 200 — the only bucket operation using 204.
2. **HeadBucket has no body** — errors conveyed solely via HTTP status codes.
3. **GetBucketLocation us-east-1 quirk** — returns empty `<LocationConstraint/>`.
4. **BucketAlreadyOwnedByYou in us-east-1** — returns 200 OK instead of 409.
5. **ListBuckets uses service endpoint** — not a bucket-specific host.
6. **PutBucketAcl modes are mutually exclusive** — mixing should return an error.
7. **CreationDate format** — ISO 8601: `2006-02-03T16:45:09.000Z`
8. **XML responses use** `Content-Type: application/xml` (not `text/xml`).
