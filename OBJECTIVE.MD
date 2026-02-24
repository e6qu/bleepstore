# BleepStore — Production-Ready S3-Compatible Object Store

## Vision

BleepStore is a clean-room, production-ready object store that fully implements the
AWS S3 API. It is designed to run in two modes:

- **Embedded mode**: Single-node, minimal resources, SQLite-backed metadata — ideal for
  local development, testing, edge deployments, and embedded use cases.
- **Cluster mode**: Multi-node, Raft-based consensus for metadata replication, horizontally
  scalable — suitable for production workloads.

BleepStore is **not** a fork or derivative of MinIO, Ceph, or any existing implementation.
It is a clean-room design informed by the public AWS S3 API documentation.

---

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| **S3 API scope** | Core subset (Phase 1) | Covers ~90% of real-world usage: Buckets CRUD, Objects CRUD, multipart upload, presigned URLs, ACLs, listing. Versioning, lifecycle, replication deferred to later phases. |
| **Consistency model** | Eventual consistency | Writes go through Raft consensus; reads can be served from any replica. Matches original S3 semantics. Lower latency than strong consistency. |
| **Cloud backends** | Gateway/proxy mode | Translates S3 API calls to native cloud storage APIs (AWS S3, GCP Cloud Storage, Azure Blob). Simpler architecture, good for migration and hybrid scenarios. |
| **Authentication** | AWS Signature V4 only | Maximum compatibility with existing S3 tools, SDKs, and CLIs. No custom auth schemes. |
| **Metadata store** | SQLite (embedded) | Zero-config, battle-tested, universally available across all target languages. Sufficient for single-node; cluster mode uses Raft-replicated metadata. |
| **OpenAPI schemas** | Two separate schemas | One for S3-compatible API, one for Admin/management API. Clean separation of concerns. Both shared across all implementations. |
| **E2E tests** | Python + boto3 + pytest | Language-agnostic: tests run against any implementation's HTTP endpoint. boto3 is the canonical AWS SDK. |
| **Project name** | BleepStore | Across all four language implementations. |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      S3 API Layer                           │
│  (HTTP server implementing S3 REST API with SigV4 auth)     │
├─────────────────────────────────────────────────────────────┤
│                    Request Router                            │
│  (Path-style & virtual-hosted-style bucket addressing)       │
├──────────────────┬──────────────────────────────────────────┤
│  Metadata Engine │           Object Engine                   │
│  ┌─────────────┐ │  ┌────────────────────────────────────┐  │
│  │   SQLite     │ │  │      Storage Backend Interface      │  │
│  │  (embedded)  │ │  ├────────────────────────────────────┤  │
│  ├─────────────┤ │  │ Local FS │ AWS S3 │ GCP CS │ Azure │  │
│  │ Raft-backed │ │  └────────────────────────────────────┘  │
│  │ (cluster)   │ │                                          │
│  └─────────────┘ │                                          │
├──────────────────┴──────────────────────────────────────────┤
│                  Cluster Layer (optional)                     │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  Raft Consensus │ Node Discovery │ Health Checking   │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Components

#### S3 API Layer
- Full HTTP/1.1 server implementing the S3 REST API
- Path-style addressing: `http://host:port/bucket/key`
- Virtual-hosted-style addressing: `http://bucket.host:port/key`
- XML request/response serialization (namespace: `http://s3.amazonaws.com/doc/2006-03-01/`)
- AWS Signature V4 authentication (header-based and query-string-based)
- Presigned URL generation and validation
- Proper S3 error response XML formatting

#### Metadata Engine
- **Embedded mode**: SQLite database storing bucket metadata, object metadata, ACLs,
  multipart upload state
- **Cluster mode**: Raft-replicated state machine backed by SQLite (writes go through
  Raft log, reads served locally for eventual consistency)
- Schema supports: buckets, objects (key, size, etag, content-type, user metadata,
  last-modified), ACLs, multipart uploads, parts

#### Object Engine / Storage Backend Interface
Pluggable storage backends behind a common interface:

```
interface StorageBackend:
    put_object(bucket, key, data, metadata) -> ETag
    get_object(bucket, key, range?) -> Stream
    delete_object(bucket, key) -> void
    head_object(bucket, key) -> ObjectMetadata
    copy_object(src_bucket, src_key, dst_bucket, dst_key) -> ETag
```

**Backends:**
1. **Local filesystem** — Objects stored as files in a configurable directory.
   Content-addressable or key-path layout.
2. **AWS S3** — Gateway/proxy to real AWS S3. Translates BleepStore requests
   to AWS SDK calls.
3. **GCP Cloud Storage** — Gateway/proxy to Google Cloud Storage.
4. **Azure Blob Storage** — Gateway/proxy to Azure Blob Storage.

#### Cluster Layer (Raft)
- Raft consensus for metadata replication across nodes
- Leader handles all metadata writes; followers serve reads (eventual consistency)
- Node discovery via static config, DNS, or gossip protocol
- Automatic leader election and log compaction
- Object data is NOT replicated via Raft (too large) — data replication is handled
  by the storage backend or via async replication

---

## S3 API Scope — Phase 1 (Core)

### Bucket Operations
| Operation | Method | Path |
|---|---|---|
| ListBuckets | `GET /` | |
| CreateBucket | `PUT /{Bucket}` | |
| DeleteBucket | `DELETE /{Bucket}` | |
| HeadBucket | `HEAD /{Bucket}` | |
| GetBucketLocation | `GET /{Bucket}?location` | |
| GetBucketAcl | `GET /{Bucket}?acl` | |
| PutBucketAcl | `PUT /{Bucket}?acl` | |

### Object Operations
| Operation | Method | Path |
|---|---|---|
| PutObject | `PUT /{Bucket}/{Key+}` | |
| GetObject | `GET /{Bucket}/{Key+}` | |
| HeadObject | `HEAD /{Bucket}/{Key+}` | |
| DeleteObject | `DELETE /{Bucket}/{Key+}` | |
| DeleteObjects | `POST /{Bucket}?delete` | |
| CopyObject | `PUT /{Bucket}/{Key+}` | (with `x-amz-copy-source`) |
| ListObjectsV2 | `GET /{Bucket}?list-type=2` | |
| ListObjects | `GET /{Bucket}` | (legacy v1) |
| GetObjectAcl | `GET /{Bucket}/{Key+}?acl` | |
| PutObjectAcl | `PUT /{Bucket}/{Key+}?acl` | |

### Multipart Upload Operations
| Operation | Method | Path |
|---|---|---|
| CreateMultipartUpload | `POST /{Bucket}/{Key+}?uploads` | |
| UploadPart | `PUT /{Bucket}/{Key+}?partNumber=N&uploadId=ID` | |
| UploadPartCopy | `PUT /{Bucket}/{Key+}?partNumber=N&uploadId=ID` | (with `x-amz-copy-source`) |
| CompleteMultipartUpload | `POST /{Bucket}/{Key+}?uploadId=ID` | |
| AbortMultipartUpload | `DELETE /{Bucket}/{Key+}?uploadId=ID` | |
| ListMultipartUploads | `GET /{Bucket}?uploads` | |
| ListParts | `GET /{Bucket}/{Key+}?uploadId=ID` | |

### Presigned URLs
- Presigned GET (download)
- Presigned PUT (upload)
- Query-string authentication via `X-Amz-Algorithm`, `X-Amz-Credential`,
  `X-Amz-Date`, `X-Amz-Expires`, `X-Amz-SignedHeaders`, `X-Amz-Signature`
- Maximum expiration: 604800 seconds (7 days)

### Future Phases
- **Phase 2**: Object versioning, lifecycle rules, tagging
- **Phase 3**: Cross-region replication, inventory, analytics
- **Phase 4**: S3 Select, batch operations, event notifications

---

## Implementations

Four independent, clean-room implementations sharing the same API contract:

### 1. Python (`python/`)
- Framework: FastAPI or aiohttp
- SQLite: built-in `sqlite3` module
- Raft: custom implementation or `pysyncobj`
- Cloud SDKs: boto3 (AWS), google-cloud-storage (GCP), azure-storage-blob (Azure)
- Packaging: pyproject.toml, uv (package manager)

### 2. Go (`golang/`)
- Framework: net/http (stdlib) or chi router
- SQLite: `modernc.org/sqlite` (pure Go, no CGo)
- Raft: `hashicorp/raft`
- Cloud SDKs: aws-sdk-go-v2, cloud.google.com/go/storage, azure-sdk-for-go
- Packaging: go modules

### 3. Rust (`rust/`)
- Framework: axum or actix-web
- SQLite: rusqlite
- Raft: openraft or custom
- Cloud SDKs: aws-sdk-rust, google-cloud-rust, azure_storage
- Packaging: Cargo

### 4. Zig (`zig/`)
- Framework: std.http.Server or custom HTTP server
- SQLite: C SQLite via @cImport
- Raft: custom implementation
- Cloud SDKs: HTTP-based (direct REST API calls, no official SDKs)
- Packaging: build.zig

### Shared Across All Implementations
- Same OpenAPI schema (S3 API): `schemas/s3-api.openapi.yaml`
- Same OpenAPI schema (Admin API): `schemas/admin-api.openapi.yaml`
- Same E2E test suite: `tests/e2e/`
- Same performance test suite: `tests/performance/`
- Same configuration format (YAML/TOML)

---

## Project Structure

```
bleepstore/
├── OBJECTIVE.MD                    # This file
├── schemas/
│   ├── s3-api.openapi.yaml         # S3-compatible API schema (shared)
│   └── admin-api.openapi.yaml      # Admin/management API schema (shared)
├── specs/
│   ├── s3-bucket-operations.md     # Detailed bucket API spec
│   ├── s3-object-operations.md     # Detailed object API spec
│   ├── s3-multipart-upload.md      # Detailed multipart upload spec
│   ├── s3-authentication.md        # SigV4 and presigned URL spec
│   ├── s3-error-responses.md       # Error codes and XML format
│   ├── s3-common-headers.md        # Common request/response headers
│   ├── storage-backends.md         # Backend interface and implementations
│   ├── clustering.md               # Raft consensus and cluster architecture
│   └── metadata-schema.md          # SQLite schema for metadata
├── python/
│   ├── pyproject.toml
│   ├── src/bleepstore/
│   │   ├── __init__.py
│   │   ├── server.py               # HTTP server and routing
│   │   ├── auth.py                 # SigV4 authentication
│   │   ├── handlers/
│   │   │   ├── bucket.py           # Bucket operation handlers
│   │   │   ├── object.py           # Object operation handlers
│   │   │   └── multipart.py        # Multipart upload handlers
│   │   ├── metadata/
│   │   │   ├── store.py            # Metadata store interface
│   │   │   └── sqlite.py           # SQLite implementation
│   │   ├── storage/
│   │   │   ├── backend.py          # Storage backend interface
│   │   │   ├── local.py            # Local filesystem backend
│   │   │   ├── aws.py              # AWS S3 gateway backend
│   │   │   ├── gcp.py              # GCP Cloud Storage backend
│   │   │   └── azure.py            # Azure Blob Storage backend
│   │   ├── cluster/
│   │   │   └── raft.py             # Raft consensus
│   │   ├── xml_utils.py            # S3 XML serialization/deserialization
│   │   └── errors.py               # S3 error types and formatting
│   └── tests/                      # Unit tests
├── golang/
│   ├── go.mod
│   ├── cmd/bleepstore/main.go
│   ├── internal/
│   │   ├── server/                 # HTTP server and routing
│   │   ├── auth/                   # SigV4 authentication
│   │   ├── handlers/               # Request handlers
│   │   ├── metadata/               # Metadata store
│   │   ├── storage/                # Storage backends
│   │   ├── cluster/                # Raft consensus
│   │   └── xml/                    # XML serialization
│   └── tests/                      # Unit tests
├── rust/
│   ├── Cargo.toml
│   ├── src/
│   │   ├── main.rs
│   │   ├── server.rs
│   │   ├── auth.rs
│   │   ├── handlers/
│   │   ├── metadata/
│   │   ├── storage/
│   │   ├── cluster/
│   │   └── xml.rs
│   └── tests/                      # Unit tests
├── zig/
│   ├── build.zig
│   ├── src/
│   │   ├── main.zig
│   │   ├── server.zig
│   │   ├── auth.zig
│   │   ├── handlers/
│   │   ├── metadata/
│   │   ├── storage/
│   │   ├── cluster/
│   │   └── xml.zig
│   └── tests/                      # Unit tests
└── tests/
    ├── e2e/
    │   ├── conftest.py             # pytest fixtures, server lifecycle
    │   ├── test_buckets.py         # Bucket operation tests
    │   ├── test_objects.py         # Object operation tests
    │   ├── test_multipart.py       # Multipart upload tests
    │   ├── test_presigned.py       # Presigned URL tests
    │   ├── test_acl.py             # ACL tests
    │   ├── test_errors.py          # Error handling tests
    │   └── requirements.txt        # boto3, pytest, etc.
    ├── performance/
    │   ├── bench_throughput.py     # Throughput benchmarks
    │   ├── bench_latency.py        # Latency benchmarks
    │   ├── bench_multipart.py      # Large file upload benchmarks
    │   └── requirements.txt
    └── smoke/
        └── smoke_test.sh           # Quick AWS CLI-based smoke test
```

---

## Configuration

All implementations share the same configuration format:

```yaml
# bleepstore.yaml
server:
  host: "0.0.0.0"
  port: 9000
  region: "us-east-1"

auth:
  access_key: "bleepstore"
  secret_key: "bleepstore-secret"

metadata:
  engine: "sqlite"           # "sqlite" (embedded) or "raft" (cluster)
  sqlite:
    path: "./data/metadata.db"

storage:
  backend: "local"           # "local", "aws", "gcp", "azure"
  local:
    root_dir: "./data/objects"
  aws:
    bucket: "my-backing-bucket"
    region: "us-east-1"
    # uses standard AWS credential chain
  gcp:
    bucket: "my-backing-bucket"
    project: "my-project"
  azure:
    container: "my-container"
    account: "my-account"

cluster:                     # only used when metadata.engine = "raft"
  node_id: "node-1"
  bind_addr: "0.0.0.0:9001"
  peers:
    - "node-2:9001"
    - "node-3:9001"
```

---

## Testing Strategy

### E2E Tests (Python + boto3 + pytest)
- Tests target `http://localhost:9000` (configurable via `BLEEPSTORE_ENDPOINT`)
- Use boto3 S3 client configured with BleepStore credentials
- Each test function is independent; bucket/object cleanup in fixtures
- Cover all Phase 1 operations with positive and negative test cases
- Validate response XML, headers, status codes, ETags

### Performance Tests
- Throughput: objects/second for small (1KB), medium (1MB), large (100MB) objects
- Latency: p50, p95, p99 for GET/PUT/DELETE/LIST
- Multipart: upload speed for 1GB+ files
- Concurrency: behavior under 10, 100, 1000 concurrent clients

### Smoke Tests (AWS CLI)
- Shell script using `aws s3` and `aws s3api` commands
- Quick validation that basic operations work
- Useful for CI/CD pipelines

### Compatibility Testing
- All tests must pass against all 4 language implementations
- Tests should also pass against real AWS S3 (to validate test correctness)
- CI matrix: {python, golang, rust, zig} × {local, aws-backend} × {embedded, cluster}

---

## Non-Goals (Explicit)

- **Not a CDN**: No edge caching or content delivery features
- **Not a filesystem**: No POSIX filesystem interface (no FUSE mount)
- **No S3 Select in Phase 1**: Query-in-place deferred to Phase 4
- **No bucket versioning in Phase 1**: Deferred to Phase 2
- **No event notifications in Phase 1**: Deferred to Phase 4
- **No cross-implementation data sharing**: Each implementation manages its own data
- **No custom auth**: SigV4 only, no OIDC/LDAP/custom auth plugins

---

## Success Criteria

1. All 4 implementations pass the complete E2E test suite
2. `aws s3 cp`, `aws s3 ls`, `aws s3 sync` work out of the box
3. `aws s3api` commands for all Phase 1 operations succeed
4. Embedded mode starts in < 1 second with < 50MB memory
5. Cluster mode maintains consistency under node failures
6. Performance within 2x of MinIO for equivalent single-node configuration
7. All implementations export the identical OpenAPI schema
