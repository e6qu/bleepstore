# Storage Backends — Specification

## Backend Interface

All storage backends implement a common interface. The metadata layer is separate —
backends only handle raw object data storage and retrieval.

### Interface Definition

```
StorageBackend:
    put_object(bucket: str, key: str, data: Stream, content_length: int) -> str (ETag)
    get_object(bucket: str, key: str, range: Optional[ByteRange]) -> Stream
    delete_object(bucket: str, key: str) -> void
    head_object(bucket: str, key: str) -> ObjectInfo (size, etag, last_modified)
    copy_object(src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> str (ETag)
    delete_objects(bucket: str, keys: List[str]) -> List[DeleteResult]
    create_bucket(bucket: str) -> void
    delete_bucket(bucket: str) -> void
    bucket_exists(bucket: str) -> bool
```

### ByteRange

```
ByteRange:
    start: Optional[int]   # None for suffix ranges
    end: Optional[int]     # None for open-ended ranges
```

Supports: `bytes=start-end`, `bytes=start-`, `bytes=-suffix`

### ObjectInfo

```
ObjectInfo:
    size: int              # Object size in bytes
    etag: str              # Quoted MD5 hex digest
    last_modified: datetime # UTC timestamp
```

### DeleteResult

```
DeleteResult:
    key: str
    success: bool
    error_code: Optional[str]
    error_message: Optional[str]
```

---

## Backend 1: Local Filesystem

### Overview
Stores objects as files on the local filesystem. Suitable for embedded mode,
development, and single-node deployments.

### Configuration

```yaml
storage:
  backend: "local"
  local:
    root_dir: "./data/objects"
```

### Data Layout

```
{root_dir}/
├── {bucket-name}/
│   ├── {key}                    # Object data files
│   └── nested/path/key          # Keys with / create directories
```

### Key-to-Path Mapping
- Bucket name → directory under root_dir
- Object key → file path under bucket directory
- Keys containing `/` → nested directory structure
- Keys are URL-decoded before mapping to filesystem paths
- Special filesystem characters must be escaped (platform-dependent)

### ETag Computation
- MD5 hash of the file content, hex-encoded, quoted
- Computed during write, stored alongside file or in metadata DB

### Atomicity
- Writes use temp file + atomic rename to prevent partial reads
- Pattern: write to `{key}.tmp.{uuid}`, then `rename()` to `{key}`

### Multipart Upload Storage
- Parts stored in temp directory: `{root_dir}/.multipart/{upload_id}/{part_number}`
- On complete: assemble parts into final object, delete temp directory
- On abort: delete temp directory

---

## Backend 2: AWS S3 Gateway

### Overview
Proxies/translates BleepStore S3 API calls to real AWS S3. Acts as a transparent
gateway with BleepStore handling authentication and the AWS SDK handling data
operations.

### Configuration

```yaml
storage:
  backend: "aws"
  aws:
    bucket: "my-backing-bucket"
    region: "us-east-1"
    prefix: "bleepstore/"          # Optional key prefix
    # Uses standard AWS credential chain:
    # env vars, ~/.aws/credentials, IAM role, etc.
```

### Mapping
- BleepStore bucket → prefix in backing bucket: `{prefix}{bleepstore-bucket}/{key}`
- OR: one backing bucket per BleepStore bucket (configurable)

### Implementation
- Use official AWS SDK (boto3, aws-sdk-go-v2, aws-sdk-rust, direct HTTP for Zig)
- Map BleepStore operations to equivalent AWS SDK calls
- Forward ETags, Content-Types, and metadata transparently
- Range requests forwarded to AWS S3

### Multipart Upload
- CreateMultipartUpload → AWS CreateMultipartUpload
- UploadPart → AWS UploadPart
- CompleteMultipartUpload → AWS CompleteMultipartUpload
- AbortMultipartUpload → AWS AbortMultipartUpload
- Direct passthrough; ETags from AWS used as-is

---

## Backend 3: GCP Cloud Storage Gateway

### Overview
Proxies BleepStore S3 API calls to Google Cloud Storage.

### Configuration

```yaml
storage:
  backend: "gcp"
  gcp:
    bucket: "my-backing-bucket"
    project: "my-project"
    prefix: "bleepstore/"          # Optional
    # Uses Application Default Credentials (ADC):
    # GOOGLE_APPLICATION_CREDENTIALS, gcloud auth, metadata server
```

### Mapping
- BleepStore operations → GCS JSON API / client library calls
- GCS uses `generation` for versioning (not version IDs)
- GCS ETags differ from S3 ETags — must compute MD5 ETags ourselves or use `md5Hash` field

### Key Differences from S3
- GCS multipart upload is "resumable upload" or "compose" — different semantics
- For multipart: upload parts as temp objects, then compose into final object
- GCS `compose` supports up to 32 source objects per call — need chaining for >32 parts
- ETags: GCS returns base64-encoded MD5 in `md5Hash` — convert to hex for S3 compatibility

---

## Backend 4: Azure Blob Storage Gateway

### Overview
Proxies BleepStore S3 API calls to Azure Blob Storage.

### Configuration

```yaml
storage:
  backend: "azure"
  azure:
    container: "my-container"
    account: "my-account"
    prefix: "bleepstore/"          # Optional
    # Uses DefaultAzureCredential chain:
    # env vars, managed identity, Azure CLI, etc.
```

### Mapping
- BleepStore bucket → prefix within Azure container
- BleepStore operations → Azure Blob REST API / SDK calls

### Key Differences from S3
- Azure uses "block blobs" with "block IDs" for multipart
- Upload parts as blocks (PutBlock), then commit (PutBlockList)
- Block IDs must be same length, base64-encoded
- Azure ETag includes quotes and may differ from MD5 — compute MD5 ETags ourselves
- Azure uses "containers" not "buckets" — BleepStore maps buckets to blob prefixes

### Multipart Upload Mapping
1. CreateMultipartUpload → generate upload ID (local), no Azure call needed
2. UploadPart → `Put Block` with block ID derived from part number
3. CompleteMultipartUpload → `Put Block List` with committed block ID list
4. AbortMultipartUpload → no explicit cleanup needed (uncommitted blocks auto-expire)

---

## Error Mapping

Each backend must translate provider-specific errors to S3 error codes:

| Provider Error | S3 Error Code |
|---|---|
| Not found / 404 | `NoSuchKey` or `NoSuchBucket` |
| Forbidden / 403 | `AccessDenied` |
| Conflict / 409 | `BucketAlreadyExists` |
| Rate limit / 429 | `SlowDown` |
| Server error / 500+ | `InternalError` |
| Timeout | `RequestTimeout` |

---

## Backend Selection at Runtime

Backend is selected via configuration at startup. All backends implement the same
interface, so the rest of the application is backend-agnostic.

```
config.storage.backend → BackendFactory → StorageBackend implementation
```

Only one backend is active per BleepStore instance (no per-bucket backend selection
in Phase 1).
