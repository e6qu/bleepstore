# S3 Authentication — AWS Signature Version 4 & Presigned URLs

## Overview

All authenticated S3 requests use Signature Version 4 (SigV4). The signature can be delivered via:

1. **Authorization header** — used by SDKs and direct API calls
2. **Query string parameters** — used by presigned URLs

Both use the same core signing algorithm.

---

## SigV4 Signing Process

### Step 1: Create Canonical Request

```
CanonicalRequest =
  HTTPMethod + '\n' +
  CanonicalURI + '\n' +
  CanonicalQueryString + '\n' +
  CanonicalHeaders + '\n' +
  SignedHeaders + '\n' +
  HashedPayload
```

**HTTPMethod**: Uppercase (`GET`, `PUT`, `HEAD`, `DELETE`, `POST`)

**CanonicalURI**: URI-encoded absolute path (RFC 3986). Forward slashes `/` are NOT encoded. Empty path becomes `/`. S3 does NOT double-encode or normalize `.`/`..`.

**CanonicalQueryString**: All query parameters (excluding `X-Amz-Signature` for presigned) sorted by name (byte-order, case-sensitive). Each name and value URI-encoded. Format: `name1=value1&name2=value2`. Empty query string = empty string. Parameters with no value use empty value: `acl=`.

**CanonicalHeaders**: Headers listed in SignedHeaders, lowercased, trimmed (leading/trailing whitespace), sequential spaces collapsed to single space, sorted by name, each as `name:value\n`. **Must** include `host`. Block ends with trailing `\n`.

```
content-type:application/octet-stream
host:mybucket.s3.us-east-1.amazonaws.com
x-amz-content-sha256:UNSIGNED-PAYLOAD
x-amz-date:20260222T120000Z
```

**SignedHeaders**: Semicolon-separated, lowercased, sorted:
```
content-type;host;x-amz-content-sha256;x-amz-date
```

**HashedPayload**: Hex-encoded SHA-256 of the request body, OR:
- `UNSIGNED-PAYLOAD` — skip payload verification
- `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` — chunked upload

### Step 2: Create String to Sign

```
StringToSign =
  "AWS4-HMAC-SHA256" + '\n' +
  TimeStamp + '\n' +
  Scope + '\n' +
  HexEncode(SHA256(CanonicalRequest))
```

**TimeStamp**: ISO 8601 basic: `YYYYMMDDTHHMMSSZ` (e.g., `20260222T120000Z`)

**Scope** (Credential Scope):
```
YYYYMMDD/region/s3/aws4_request
```

### Step 3: Derive Signing Key (HMAC Chain)

```
DateKey    = HMAC-SHA256("AWS4" + SecretAccessKey,  YYYYMMDD)
RegionKey  = HMAC-SHA256(DateKey,                    region)
ServiceKey = HMAC-SHA256(RegionKey,                  "s3")
SigningKey  = HMAC-SHA256(ServiceKey,                 "aws4_request")
```

**Critical details:**
- Initial key: literal `"AWS4"` prepended to secret key as UTF-8 bytes
- Date: 8-character `YYYYMMDD` string
- Intermediate values: raw binary (32 bytes each), never hex-encoded between steps
- Signing key can be **cached** for same day/region/service

### Step 4: Calculate Signature

```
Signature = HexEncode(HMAC-SHA256(SigningKey, StringToSign))
```

Result: 64-character lowercase hex string.

---

## Authorization Header Format

```
Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260222/us-east-1/s3/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=abcdef0123456789...
```

**Required headers for header-based auth:**
- `Host`
- `x-amz-date` (or `Date`, but `x-amz-date` takes precedence)
- `x-amz-content-sha256` (required for S3 specifically)

---

## Presigned URLs

### Query Parameters

| Parameter | Description |
|---|---|
| `X-Amz-Algorithm` | Always `AWS4-HMAC-SHA256` |
| `X-Amz-Credential` | `{AccessKeyId}/{YYYYMMDD}/{region}/s3/aws4_request` (slashes URL-encoded as `%2F`) |
| `X-Amz-Date` | Timestamp: `YYYYMMDDTHHMMSSZ` |
| `X-Amz-Expires` | Validity in seconds (1 to 604800 = 7 days) |
| `X-Amz-SignedHeaders` | Semicolon-separated header names (minimum: `host`) |
| `X-Amz-Signature` | 64-char hex signature (**excluded** from canonical query string during signing) |
| `X-Amz-Security-Token` | (Optional) STS session token — included in canonical query string |

### Generation Process

1. Construct base URL with required query parameters (except `X-Amz-Signature`)
2. Create canonical request with `UNSIGNED-PAYLOAD` as HashedPayload
3. Sign using standard SigV4 process
4. Append `X-Amz-Signature=<result>` to URL

### Presigned GET Example

```
https://bucket.s3.region.amazonaws.com/key?
  X-Amz-Algorithm=AWS4-HMAC-SHA256&
  X-Amz-Credential=AKID%2F20260222%2Fus-east-1%2Fs3%2Faws4_request&
  X-Amz-Date=20260222T120000Z&
  X-Amz-Expires=3600&
  X-Amz-SignedHeaders=host&
  X-Amz-Signature=<64-hex-chars>
```

### Presigned PUT
Same as GET but:
- HTTPMethod is `PUT`
- May include `Content-Type` in SignedHeaders (user must then send that exact header)

### Maximum Expiration
- **604800 seconds** (7 days)
- Values > 604800: HTTP 400 `AuthorizationQueryParametersError`

### Server-Side Validation

1. Extract presigned query parameters
2. Validate algorithm = `AWS4-HMAC-SHA256`
3. Parse credential: access key, date, region, service, terminator
4. Validate `X-Amz-Expires` in range [1, 604800]
5. Check: `now <= parse(X-Amz-Date) + X-Amz-Expires`
6. Verify credential date matches `X-Amz-Date` date portion
7. Look up secret key for access key ID
8. Reconstruct canonical request (exclude `X-Amz-Signature` from query string, use `UNSIGNED-PAYLOAD`)
9. Reconstruct string to sign
10. Derive signing key and compute signature
11. **Constant-time comparison** of computed vs. provided signature

---

## Server Detection Logic

- Query string contains `X-Amz-Algorithm` → presigned URL auth
- `Authorization` header starts with `AWS4-HMAC-SHA256` → header-based SigV4
- Both present → reject as ambiguous

---

## Header-Based vs. Query-String Auth Differences

| Aspect | Header-Based | Query-String (Presigned) |
|---|---|---|
| Signature location | `Authorization` header | `X-Amz-Signature` parameter |
| Timestamp | `x-amz-date` header | `X-Amz-Date` parameter |
| Expiration | Clock skew tolerance (~15 min) | `X-Amz-Expires` (1–604800 sec) |
| Payload hash | Actual SHA-256, `UNSIGNED-PAYLOAD`, or `STREAMING-...` | Always `UNSIGNED-PAYLOAD` |

---

## `x-amz-content-sha256` Values

| Value | Meaning |
|---|---|
| `{hex-sha256}` | Actual payload hash — S3 verifies |
| `UNSIGNED-PAYLOAD` | Skip payload verification |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` | Chunked transfer with per-chunk signing |

---

## Chunked Transfer Encoding (`aws-chunked`)

### Request Headers
```
Content-Encoding: aws-chunked
x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD
x-amz-decoded-content-length: <actual-body-length>
Content-Length: <total-chunked-encoded-length>
```

### Seed Signature
The `Authorization` header uses `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` as HashedPayload.

### Chunk Format
```
<hex-chunk-size>;chunk-signature=<signature>\r\n
<chunk-data>\r\n
```

Final (zero-length) chunk:
```
0;chunk-signature=<signature>\r\n
\r\n
```

### Per-Chunk Signature
```
StringToSign =
  "AWS4-HMAC-SHA256-PAYLOAD" + '\n' +
  TimeStamp + '\n' +
  CredentialScope + '\n' +
  PreviousSignature + '\n' +
  SHA256("") + '\n' +
  SHA256(chunk-data)
```

- First chunk's `PreviousSignature` = seed signature from Authorization header
- `SHA256("")` = `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`

---

## URI Encoding Rules

- Characters `A-Z`, `a-z`, `0-9`, `-`, `_`, `.`, `~` are NOT encoded
- All other characters: percent-encoded with **uppercase** hex: `%2F`, `%20`
- Spaces: `%20` (NOT `+`)
- `/` in URI path: NOT encoded
- `/` in query parameter names/values: encoded as `%2F`
- S3 uses **single encoding** (no double-encoding)

---

## Edge Cases

1. **Date matching**: Credential date must match `X-Amz-Date` date portion (first 8 chars)
2. **Clock skew**: ~15 minutes tolerance for header-based auth
3. **Empty query parameters**: `?acl` canonicalizes as `acl=`
4. **Duplicate query parameters**: Sort by name, then by value
5. **Port in Host**: Non-standard ports must be in Host header: `host:localhost:9000`
6. **Multiple same headers**: Values joined with `,` (comma) in canonical headers
7. **Signing key caching**: Cache per day/region/service to avoid 4 HMACs per request

---

## Constants Reference

| Constant | Value |
|---|---|
| Algorithm | `AWS4-HMAC-SHA256` |
| Chunk algorithm | `AWS4-HMAC-SHA256-PAYLOAD` |
| Key prefix | `AWS4` |
| Scope terminator | `aws4_request` |
| Service name | `s3` |
| Unsigned payload | `UNSIGNED-PAYLOAD` |
| Streaming payload | `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` |
| SHA-256 of empty string | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| Max presigned expiration | `604800` seconds (7 days) |
| Clock skew tolerance | ~900 seconds (15 minutes) |
