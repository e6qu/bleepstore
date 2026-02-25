# BleepStore Go -- What We Did

## Session 19 -- 2026-02-25

### Pluggable Storage Backends (memory, sqlite, cloud enhancements)

**New storage backends:**
- **Memory backend** (`internal/storage/memory.go`): In-memory map-based storage with sync.RWMutex. Supports `max_size_bytes` limit, SQLite snapshot persistence, Close() for graceful shutdown.
- **SQLite backend** (`internal/storage/sqlite.go`): Object BLOBs stored in the same SQLite database as metadata. Tables: `object_data`, `part_data`. Uses modernc.org/sqlite with WAL mode.

**Cloud config enhancements:**
- AWS: `endpoint_url`, `use_path_style`, `access_key_id`, `secret_access_key`
- GCP: `credentials_file`
- Azure: `connection_string`, `use_managed_identity`

**Config + factory:**
- Restructured `config.go` to use nested structs (MemoryConfig, AWSConfig, GCPConfig, AzureConfig)
- Updated `main.go` switch with "memory" and "sqlite" cases
- Updated cloud backend constructors with new parameters

**E2E:**
- Updated `run_e2e.sh` with `--backend` flag (e.g., `./run_e2e.sh --backend memory`)

---

## Session 18 -- Stage 15: Performance Optimization & Production Readiness (2026-02-24)

### What was implemented:

**Phase 1: SigV4 Signing Key & Credential Cache** (`internal/auth/sigv4.go`)
- Added `sync.RWMutex`-protected cache maps to `SigV4Verifier`: `signingKeys` (24h TTL) and `credCache` (60s TTL)
- `cachedDeriveSigningKey()` avoids recomputing 4 HMAC-SHA256 ops per request (signing key only changes daily)
- `cachedGetCredential()` avoids DB query per request for credential lookup
- Both caches have max 1000 entries; full map clear on overflow
- Updated `VerifyRequest()` and `VerifyPresigned()` to use cached versions

**Phase 2: Batch DeleteObjects SQL** (`internal/metadata/sqlite.go`, `internal/handlers/object.go`)
- Rewrote `DeleteObjectsMeta()` to use `DELETE FROM objects WHERE bucket=? AND key IN (?,?,...)` instead of per-key DELETE loops
- Batch size 998 (SQLite's 999-variable limit minus 1 for bucket parameter)
- Rewrote `DeleteObjects()` handler to collect all keys, call batch metadata delete once, then loop only for storage file deletion

**Phase 3: Structured Logging with `log/slog`**
- Created `internal/logging/logging.go` package with `Setup(level, format, writer)` function
- Added `LoggingConfig{Level, Format}` to `config.Config`
- Added `--log-level` and `--log-format` CLI flags to main.go
- Converted all `log.Printf` calls to `slog.Info/Warn/Error/Debug` with structured key-value pairs across:
  - `cmd/bleepstore/main.go` (~10 calls)
  - `internal/handlers/object.go` (~31 calls)
  - `internal/handlers/multipart.go` (~25 calls)
  - `internal/handlers/bucket.go` (~12 calls)
  - `internal/storage/aws.go`, `gcp.go`, `azure.go` (~5 calls)
  - `internal/cluster/raft.go` (~3 calls)

**Phase 4: Production Config (Shutdown Timeout, Max Object Size)**
- Added `ShutdownTimeout int` (default 30) and `MaxObjectSize int64` (default 5 GiB) to `ServerConfig`
- Added `--shutdown-timeout` and `--max-object-size` CLI flags
- Server shutdown uses configurable timeout instead of hardcoded 30s
- PutObject enforces max object size via Content-Length check (returns `EntityTooLarge`)
- UploadPart also enforces max object size per part
- Handler constructors updated: `NewObjectHandler()` and `NewMultipartHandler()` accept `maxObjectSize int64`

### Test results:
- `go test ./... -v -race` -- 274 unit tests pass
- `./run_e2e.sh` -- **86/86 E2E tests pass**

### Files changed:
- `internal/auth/sigv4.go` -- signing key & credential caching
- `internal/metadata/sqlite.go` -- batch DELETE SQL
- `internal/handlers/object.go` -- batch delete handler, slog, max object size
- `internal/handlers/multipart.go` -- slog, max object size
- `internal/handlers/bucket.go` -- slog
- `internal/storage/aws.go` -- slog
- `internal/storage/gcp.go` -- slog
- `internal/storage/azure.go` -- slog
- `internal/cluster/raft.go` -- slog
- `internal/config/config.go` -- LoggingConfig, ShutdownTimeout, MaxObjectSize
- `internal/server/server.go` -- pass maxObjectSize to handlers
- `cmd/bleepstore/main.go` -- CLI flags, logging setup, slog
- NEW `internal/logging/logging.go` -- structured logging setup

---

## Session 17 -- Stage 11b: Azure Blob Storage Gateway Backend (2026-02-24)

### What was implemented:

- **internal/storage/azure.go** (fully implemented from stub): Complete Azure Blob Storage gateway backend:
  - `AzureBlobAPI` interface: Defines the subset of Azure Blob client operations used by the gateway, enabling mock-based unit testing. Includes: UploadBlob, DownloadBlob, DeleteBlob, BlobExists, GetBlobProperties, StartCopyFromURL, StageBlock, CommitBlockList.
  - `AzureGatewayBackend` struct: Holds upstream container name, account URL, prefix, and AzureBlobAPI client.
  - `NewAzureGatewayBackend(ctx, container, accountURL, prefix)`: Initializes Azure SDK client via DefaultAzureCredential, verifies upstream container is accessible. Production constructor.
  - `NewAzureGatewayBackendWithClient(container, accountURL, prefix, client)`: Test constructor that accepts a pre-built AzureBlobAPI client (mock).
  - `blobName(bucket, key)`: Maps BleepStore bucket/key to upstream Azure blob name: `{prefix}{bucket}/{key}`.
  - `blockID(uploadID, partNumber)`: Generates base64-encoded block ID: `base64("{uploadID}:{05d partNumber}")`. Includes uploadID to avoid collisions between concurrent multipart uploads.
  - `PutObject`: Reads all data, computes MD5 locally for consistent ETag, uploads to Azure via UploadBlob.
  - `GetObject`: Gets blob properties for size, downloads blob data. Maps Azure not-found errors.
  - `DeleteObject`: Deletes blob from Azure. Idempotent (catches not-found silently).
  - `CopyObject`: Uses StartCopyFromURL for server-side copy. Downloads result to compute MD5 for consistent ETag.
  - `PutPart`: **Stages a block directly on the final blob** using StageBlock -- no temporary objects created (unlike AWS/GCP which use temp part objects). Block ID encodes both uploadID and part number.
  - `AssembleParts`: Calls CommitBlockList with ordered block IDs to finalize the blob. Downloads result to compute MD5 ETag.
  - `DeleteParts`: **No-op** -- uncommitted Azure blocks auto-expire in 7 days. No cleanup needed.
  - `CreateBucket` / `DeleteBucket`: No-ops (BleepStore buckets map to key prefixes in the upstream container).
  - `ObjectExists`: Uses BlobExists via GetProperties, maps 404 to false.
  - `isAzureNotFound`: Checks error messages for BlobNotFound, ContainerNotFound, "not found", "404" patterns.
  - Compile-time interface compliance check: `var _ StorageBackend = (*AzureGatewayBackend)(nil)`.

- **internal/storage/azure_client.go** (new file): Real Azure SDK client wrapper implementing AzureBlobAPI:
  - `realAzureClient` struct wrapping `*azblob.Client`.
  - `newRealAzureClient(accountURL)`: Creates client using `azidentity.NewDefaultAzureCredential` + `azblob.NewClient`.
  - `UploadBlob`: Uses `client.UploadBuffer`.
  - `DownloadBlob`: Uses `client.DownloadStream` + `io.ReadAll`.
  - `DeleteBlob`: Uses `client.DeleteBlob`.
  - `BlobExists`: Uses `ServiceClient().NewContainerClient().NewBlobClient().GetProperties()`.
  - `GetBlobProperties`: Same as BlobExists but returns ContentLength.
  - `StartCopyFromURL`: Uses `ServiceClient().NewContainerClient().NewBlobClient().StartCopyFromURL()`.
  - `StageBlock`: Uses `ServiceClient().NewContainerClient().NewBlockBlobClient().StageBlock()` with `streaming.NopCloser` for ReadSeekCloser conversion.
  - `CommitBlockList`: Uses `ServiceClient().NewContainerClient().NewBlockBlobClient().CommitBlockList()`.

- **internal/storage/azure_test.go** (new file): 25 test functions with comprehensive mock-based coverage:
  - `mockAzureClient`: Full mock implementing AzureBlobAPI interface with in-memory blob storage, staged block tracking, call counters for upload/download/delete/copy/stageBlock/commitBlockList operations.
  - `TestAzurePutAndGetObject`: Full round-trip put/get with content and ETag verification.
  - `TestAzurePutObjectEmptyBody`: Zero-byte object works correctly.
  - `TestAzureGetObjectNotFound`: Returns "not found" error for missing objects.
  - `TestAzureDeleteObject`: Put, verify exists, delete, verify gone.
  - `TestAzureDeleteObjectIdempotent`: Delete non-existent returns no error.
  - `TestAzureCopyObject`: Copy object, verify ETags match and content correct.
  - `TestAzureCopyObjectNotFound`: Copy non-existent source returns "not found".
  - `TestAzureObjectExists`: False for missing, true for existing.
  - `TestAzureCreateDeleteBucketNoOp`: Both are no-ops, no errors.
  - `TestAzureKeyMapping`: Verifies `{prefix}{bucket}/{key}` mapping with prefix "bp/".
  - `TestAzureKeyMappingNoPrefix`: Verifies mapping works with empty prefix.
  - `TestAzurePutPartAndAssemble`: Upload 2 parts via StageBlock, verify staged (not committed), assemble via CommitBlockList, verify assembled data and ETag.
  - `TestAzureDeletePartsNoOp`: DeleteParts returns nil (no-op).
  - `TestAzureDeletePartsAfterUpload`: DeleteParts returns nil even with staged blocks.
  - `TestAzureAssemblePartsSinglePart`: Single part assembled correctly.
  - `TestAzureAssemblePartsThreeParts`: Three parts assembled with correct data and single CommitBlockList call.
  - `TestAzurePutObjectETagConsistency`: Local MD5 matches expected.
  - `TestAzurePutPartETagConsistency`: Part MD5 matches expected.
  - `TestAzurePutObjectOverwrite`: Overwrite changes ETag and content.
  - `TestAzureBlobKeyMapping`: Table-driven key mapping verification.
  - `TestAzureBlockIDFormat`: Verifies block IDs are valid base64 encoding of `{uploadID}:{05d partNumber}`.
  - `TestAzureBlockIDConsistentLength`: All block IDs for same upload have same length.
  - `TestAzureBlockIDNoCollision`: Different upload IDs produce different block IDs.
  - `TestAzureCopyObjectETag`: Copy returns same ETag as original.
  - `TestAzureIsAzureNotFound`: Table-driven tests for error classification.
  - `TestAzureInterfaceCompliance`: Compile-time interface check.

- **internal/config/config.go** (updated): Added `AzureAccountURL` and `AzurePrefix` fields to `StorageConfig`:
  - `AzureAccountURL string` with `yaml:"azure_account_url"` -- optional full account URL.
  - `AzurePrefix string` with `yaml:"azure_prefix"` -- optional key prefix.
  - If `AzureAccountURL` is empty, it is constructed from `AzureAccount` as `https://{account}.blob.core.windows.net`.

- **cmd/bleepstore/main.go** (updated): Backend factory logic -- added `"azure"` case:
  - Validates `azure_container` is set.
  - Validates either `azure_account` or `azure_account_url` is set.
  - Constructs account URL from account name if `azure_account_url` is empty.
  - Creates `AzureGatewayBackend` with container, accountURL, prefix.
  - Logs backend configuration.

- **go.mod** (updated): Added direct dependencies:
  - `github.com/Azure/azure-sdk-for-go/sdk/azcore v1.21.0`
  - `github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.9.0`
  - `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.1`
  - NOTE: User must run `go get` + `go mod tidy` to resolve transitive dependencies.

### Key decisions:
- **AzureBlobAPI interface for testability**: Defined a mock-friendly interface covering all Azure operations used by the gateway. The real `realAzureClient` wraps the official Azure SDK to satisfy this interface. Tests use a full mock implementation -- same pattern as AWS S3API and GCS GCSAPI.
- **No temporary part objects (unlike AWS/GCP)**: Azure Block Blob primitives allow staging blocks directly on the final blob. PutPart calls StageBlock, AssembleParts calls CommitBlockList. This eliminates the need for temporary part objects, part cleanup, and the `.parts/` prefix used by AWS and GCP backends.
- **DeleteParts is a no-op**: Uncommitted Azure blocks auto-expire in 7 days. No explicit cleanup is needed, simplifying the abort multipart flow.
- **Block ID includes uploadID**: Block IDs are `base64("{uploadID}:{05d partNumber}")` to avoid collisions between concurrent multipart uploads to the same key. The zero-padded part number ensures all block IDs have the same encoded length.
- **Local MD5 computation**: Always compute MD5 locally for consistent ETags, matching the AWS and GCP backends.
- **CopyObject downloads result for ETag**: Azure server-side copy via StartCopyFromURL, then download to compute MD5. Matches the Python reference implementation.
- **Idempotent DeleteObject**: Azure errors on deleting non-existent blobs. The backend catches not-found errors and returns nil for idempotent behavior.
- **Error mapping via message inspection**: Azure SDK errors checked by message content (BlobNotFound, ContainerNotFound, "not found", "404"). More robust detection would use `*azcore.ResponseError` but message-based approach works across all error types.
- **Separate azure_client.go file**: The real Azure SDK client wrapper is isolated in its own file to keep the main azure.go focused on business logic. The mock interface and tests have no dependency on the Azure SDK.
- **Config supports both account name and URL**: `azure_account` constructs URL automatically; `azure_account_url` allows explicit override for non-standard endpoints (e.g., Azurite emulator).

## Session 16 -- Stage 11a: GCP Cloud Storage Gateway Backend (2026-02-24)

### What was implemented:

- **internal/storage/gcp.go** (fully implemented from stub): Complete GCP Cloud Storage gateway backend using `cloud.google.com/go/storage`:
  - `GCSAPI` interface: Defines the subset of GCS client operations used by the gateway, enabling mock-based unit testing. Includes: NewWriter, NewReader, Delete, Attrs, Copy, Compose, ListObjects.
  - `GCSWriter` interface: Wraps `io.WriteCloser` for GCS object writers.
  - `GCSAttrs` struct: Holds object Size and raw MD5 bytes.
  - `realGCSClient` struct: Wraps the official `*gcs.Client` to satisfy GCSAPI interface. Implements all 7 methods using the real GCS SDK: `client.Bucket(name).Object(key).NewWriter(ctx)`, `.NewReader(ctx)`, `.Delete(ctx)`, `.Attrs(ctx)`, `.CopierFrom(src).Run(ctx)`, `.ComposerFrom(srcs...).Run(ctx)`, `.Objects(ctx, &query)`.
  - `GCPGatewayBackend` struct: Holds upstream bucket name, project, prefix, and GCSAPI client.
  - `NewGCPGatewayBackend(ctx, bucket, project, prefix)`: Initializes GCS client via `gcs.NewClient(ctx)` with Application Default Credentials, verifies upstream bucket is accessible. Production constructor.
  - `NewGCPGatewayBackendWithClient(bucket, project, prefix, client)`: Test constructor that accepts a pre-built GCSAPI client (mock).
  - `gcsKey(bucket, key)`: Maps BleepStore bucket/key to upstream GCS object name: `{prefix}{bucket}/{key}`.
  - `partKey(uploadID, partNumber)`: Maps parts to `{prefix}.parts/{upload_id}/{part_number}`.
  - `PutObject`: Reads all data, computes MD5 locally for consistent ETag, uploads to GCS via NewWriter + io.Copy + Close.
  - `GetObject`: Gets object attrs for size, opens NewReader for streaming. Maps GCS not-found errors.
  - `DeleteObject`: Deletes object from GCS. Idempotent (catches 404 silently, unlike raw GCS which errors on missing).
  - `CopyObject`: Uses GCS server-side copy via CopierFrom. Downloads result to compute MD5 for consistent ETag.
  - `PutPart`: Stores part as temporary GCS object. Computes MD5 locally.
  - `AssembleParts`: Uses GCS Compose (max 32 sources). For <=32 parts, single compose call. For >32, chains compose in batches via `chainCompose`. Downloads final object to compute MD5 ETag. Cleans up intermediate composite objects.
  - `chainCompose`: Recursive tree-based composition for >32 parts. Batches sources into groups of 32, composes each batch into an intermediate object, repeats until <=32 remain, then final compose.
  - `DeleteParts`: Lists objects under `.parts/{upload_id}/` prefix, deletes each individually. Catches 404 on individual deletes.
  - `CreateBucket` / `DeleteBucket`: No-ops (BleepStore buckets map to key prefixes in the upstream bucket).
  - `ObjectExists`: Uses Attrs API, maps 404 to false.
  - `isGCSNotFound`: Checks for `gcs.ErrObjectNotExist`, `gcs.ErrBucketNotExist`, and error message patterns ("not found", "404").
  - Compile-time interface compliance check: `var _ StorageBackend = (*GCPGatewayBackend)(nil)`.

- **internal/storage/gcp_test.go** (new file): 24 test functions with comprehensive mock-based coverage:
  - `mockGCSClient`: Full mock implementing GCSAPI interface with in-memory object storage, call counters for put/delete/copy/compose/attrs operations.
  - `mockGCSWriter`: Implements GCSWriter with buffer-based write + Close stores to mock objects.
  - `TestGCPPutAndGetObject`: Full round-trip put/get with content and ETag verification.
  - `TestGCPPutObjectEmptyBody`: Zero-byte object works correctly.
  - `TestGCPGetObjectNotFound`: Returns "not found" error for missing objects.
  - `TestGCPDeleteObject`: Put, verify exists, delete, verify gone.
  - `TestGCPDeleteObjectIdempotent`: Delete non-existent returns no error.
  - `TestGCPCopyObject`: Copy object, verify ETags match and content correct.
  - `TestGCPCopyObjectNotFound`: Copy non-existent source returns "not found".
  - `TestGCPObjectExists`: False for missing, true for existing.
  - `TestGCPCreateDeleteBucketNoOp`: Both are no-ops, no errors.
  - `TestGCPKeyMapping`: Verifies `{prefix}{bucket}/{key}` mapping with prefix "bp/".
  - `TestGCPKeyMappingNoPrefix`: Verifies mapping works with empty prefix.
  - `TestGCPPutPartAndDeleteParts`: Upload parts, verify GCS keys, delete, verify cleaned.
  - `TestGCPAssemblePartsSingleCompose`: 3 parts assembled with single compose call.
  - `TestGCPAssemblePartsChainCompose`: 35 parts assembled with recursive chain compose (>32 limit).
  - `TestGCPPutObjectETagConsistency`: Local MD5 matches expected.
  - `TestGCPPutPartETagConsistency`: Part MD5 matches expected.
  - `TestGCPPutObjectOverwrite`: Overwrite changes ETag and content.
  - `TestGCPGCSKeyMapping`: Table-driven key mapping verification.
  - `TestGCPPartKeyMapping`: Table-driven part key mapping verification.
  - `TestGCPInterfaceCompliance`: Compile-time interface check.
  - `TestGCPDeletePartsNoParts`: No error when deleting parts for non-existent upload.
  - `TestGCPAssembleSinglePart`: Single part uses single compose call.
  - `TestGCPCopyObjectETag`: Copy returns same ETag as original (same content).
  - `TestGCPIsGCSNotFound`: Table-driven tests for error classification.

- **internal/config/config.go** (updated): Added `GCPPrefix string` field to `StorageConfig` with `yaml:"gcp_prefix"` tag for optional key prefix configuration.

- **cmd/bleepstore/main.go** (updated): Backend factory logic — added `"gcp"` case:
  - Validates `gcp_bucket` is set.
  - Creates `GCPGatewayBackend` with bucket, project, prefix.
  - Logs backend configuration.

- **go.mod** (updated via `go mod tidy`): Added direct dependencies:
  - `cloud.google.com/go/storage v1.60.0`
  - `google.golang.org/api v0.268.0`
  - Plus transitive dependencies.

### Key decisions:
- **GCSAPI interface for testability**: Defined a mock-friendly interface covering all GCS operations used by the gateway: NewWriter, NewReader, Delete, Attrs, Copy, Compose, ListObjects. The real `realGCSClient` wraps the official GCS SDK to satisfy this interface. Tests use a full mock implementation.
- **Local MD5 computation**: Always compute MD5 locally rather than relying on GCS md5Hash attribute, which may not be present for composite objects. PutObject and PutPart read all data to memory to compute MD5 before uploading.
- **GCS Compose for multipart assembly**: Unlike AWS which has native multipart upload, GCS uses Compose to merge objects. Implemented recursive tree-based chaining for >32 parts: batch sources into groups of 32, compose each batch to an intermediate, repeat until <=32 remain. Intermediate objects cleaned up after final compose.
- **CopyObject downloads result for ETag**: GCS server-side copy is used, then the result is downloaded to compute MD5 for a consistent ETag. This matches the Python reference implementation.
- **Idempotent DeleteObject**: GCS errors on deleting non-existent objects (unlike S3). The backend catches 404 errors and returns nil for idempotent behavior.
- **CreateBucket/DeleteBucket as no-ops**: All BleepStore buckets share a single upstream GCS bucket with key prefixes. No actual GCS bucket creation/deletion needed.
- **Error mapping via errors.Is + message fallback**: Primary check uses `gcs.ErrObjectNotExist` and `gcs.ErrBucketNotExist`. Fallback checks error message for "not found" or "404" patterns.
- **No factory.go file**: The plan mentioned `storage/factory.go` but the existing pattern puts the factory in `cmd/bleepstore/main.go` (same as AWS). Followed the existing pattern for consistency.

## Session 15 — Stage 10: AWS Gateway Backend (2026-02-23)

### What was implemented:

- **internal/storage/aws.go** (fully implemented from stub): Complete AWS S3 gateway backend using AWS SDK for Go v2:
  - `S3API` interface: Defines the subset of AWS S3 client methods used by the gateway, enabling mock-based unit testing. Includes: PutObject, GetObject, DeleteObject, DeleteObjects, CopyObject, HeadObject, HeadBucket, CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload, AbortMultipartUpload, ListObjectsV2.
  - `AWSGatewayBackend` struct: Holds upstream bucket name, region, prefix, and S3API client.
  - `NewAWSGatewayBackend(ctx, bucket, region, prefix)`: Initializes AWS SDK client via `config.LoadDefaultConfig`, verifies upstream bucket exists via HeadBucket. Production constructor.
  - `NewAWSGatewayBackendWithClient(bucket, region, prefix, client)`: Test constructor that accepts a pre-built S3API client (mock).
  - `s3Key(bucket, key)`: Maps BleepStore bucket/key to upstream S3 key: `{prefix}{bucket}/{key}`.
  - `partKey(uploadID, partNumber)`: Maps parts to `{prefix}.parts/{upload_id}/{part_number}`.
  - `PutObject`: Reads all data, computes MD5 locally for consistent ETag, uploads to S3 via PutObject API.
  - `GetObject`: Retrieves object from S3, maps NoSuchKey/404 to "object not found" error.
  - `DeleteObject`: Deletes object from S3. Idempotent (S3 doesn't error on missing keys).
  - `CopyObject`: Uses AWS server-side copy via CopyObject API. Extracts ETag from CopyObjectResult.
  - `PutPart`: Stores part as temporary S3 object at `.parts/{upload_id}/{part_number}`. Computes MD5 locally.
  - `AssembleParts`: For single part, uses CopyObject. For multiple parts, creates native AWS multipart upload with UploadPartCopy for server-side assembly. Falls back to download + re-upload via UploadPart if EntityTooSmall error. Aborts multipart upload on any failure.
  - `DeleteParts`: Lists objects under `.parts/{upload_id}/` prefix, batch-deletes them via DeleteObjects. Handles pagination.
  - `CreateBucket` / `DeleteBucket`: No-ops (BleepStore buckets map to key prefixes in the upstream bucket).
  - `ObjectExists`: Uses HeadObject API, maps 404 to false.
  - `isAWSNotFound`: Checks for NoSuchKey, NotFound, 404 error codes via smithy.APIError, types.NoSuchKey, and HTTP status code.
  - `isAWSEntityTooSmall`: Checks for EntityTooSmall error code for multipart fallback logic.
  - Compile-time interface compliance check: `var _ StorageBackend = (*AWSGatewayBackend)(nil)`.

- **internal/storage/aws_test.go** (new file): 22 test functions with comprehensive mock-based coverage:
  - `mockS3Client`: Full mock implementing S3API interface with in-memory object storage, multipart upload tracking, call counters, and configurable EntityTooSmall forcing.
  - `mockAPIError`: Implements smithy.APIError for mock error responses.
  - `TestAWSPutAndGetObject`: Full round-trip put/get with content and ETag verification.
  - `TestAWSPutObjectEmptyBody`: Zero-byte object works correctly.
  - `TestAWSGetObjectNotFound`: Returns "not found" error for missing objects.
  - `TestAWSDeleteObject`: Put, verify exists, delete, verify gone.
  - `TestAWSDeleteObjectIdempotent`: Delete non-existent returns no error.
  - `TestAWSCopyObject`: Copy object, verify ETags match and content correct.
  - `TestAWSCopyObjectNotFound`: Copy non-existent source returns "not found".
  - `TestAWSObjectExists`: False for missing, true for existing.
  - `TestAWSCreateDeleteBucketNoOp`: Both are no-ops, no errors.
  - `TestAWSKeyMapping`: Verifies `{prefix}{bucket}/{key}` mapping with prefix "bp/".
  - `TestAWSKeyMappingNoPrefix`: Verifies mapping works with empty prefix.
  - `TestAWSPutPartAndDeleteParts`: Upload parts, verify S3 keys, delete, verify cleaned.
  - `TestAWSAssemblePartsSinglePart`: Single-part assembly uses CopyObject.
  - `TestAWSAssemblePartsMultiple`: Multi-part assembly uses native multipart upload.
  - `TestAWSAssemblePartsEntityTooSmallFallback`: EntityTooSmall triggers download+re-upload fallback.
  - `TestAWSPutObjectETagConsistency`: Local MD5 matches expected.
  - `TestAWSPutPartETagConsistency`: Part MD5 matches expected.
  - `TestAWSPutObjectOverwrite`: Overwrite changes ETag and content.
  - `TestAWSS3KeyMapping`: Table-driven key mapping verification.
  - `TestAWSPartKeyMapping`: Table-driven part key mapping verification.
  - `TestAWSInterfaceCompliance`: Compile-time interface check.
  - `TestAWSDeletePartsNoParts`: No error when deleting parts for non-existent upload.

- **internal/config/config.go** (updated): Added `AWSPrefix string` field to `StorageConfig` with `yaml:"aws_prefix"` tag for optional key prefix configuration.

- **cmd/bleepstore/main.go** (updated): Backend factory logic — switch on `cfg.Storage.Backend`:
  - `"aws"`: Validates `aws_bucket` is set, defaults `aws_region` to "us-east-1", creates `AWSGatewayBackend`.
  - `default` (including `"local"`): Creates `LocalBackend` with crash-only temp file cleanup (same as before).

- **go.mod** (updated): Added direct dependencies:
  - `github.com/aws/aws-sdk-go-v2 v1.32.7`
  - `github.com/aws/aws-sdk-go-v2/config v1.28.7`
  - `github.com/aws/aws-sdk-go-v2/service/s3 v1.71.1`
  - `github.com/aws/smithy-go v1.22.1`
  - NOTE: User must run `go mod tidy` to resolve transitive dependencies.

### Key decisions:
- **S3API interface for testability**: Defined a mock-friendly interface covering all S3 operations used by the gateway. The real `*s3.Client` satisfies this interface implicitly. Tests use a full mock implementation.
- **Local MD5 computation**: Always compute MD5 locally rather than relying on AWS ETags, which may differ with server-side encryption (SSE-S3, SSE-KMS).
- **Server-side copy for multipart assembly**: Uses UploadPartCopy to avoid downloading/re-uploading part data. EntityTooSmall fallback handles parts smaller than AWS's minimum part size for server-side copy.
- **CreateBucket/DeleteBucket as no-ops**: All BleepStore buckets share a single upstream S3 bucket with key prefixes. No actual AWS S3 bucket creation/deletion needed.
- **Batch delete for parts cleanup**: Uses ListObjectsV2 + DeleteObjects (up to 1000 per batch) for efficient cleanup of temporary part objects.
- **No streaming for PutObject**: Reads all data into memory to compute MD5 before uploading. This matches the Python reference implementation. For very large objects, the local backend's streaming approach is better.
- **Error mapping via smithy.APIError**: Checks error codes (NoSuchKey, NotFound, 404, EntityTooSmall) via Go's `errors.As` with the smithy error interface. Also checks `types.NoSuchKey` and HTTP status code for comprehensive coverage.

## Session 14 — Stage 9b: External Test Suite Code Review (2026-02-23)
- Performed thorough code review against Ceph s3-tests, MinIO Mint, and Snowflake s3compat expectations
- Verified XML namespaces, error formats, header values all match S3 spec
- Verified bucket naming validation covers all AWS rules
- Identified 3 minor compliance gaps (non-blocking):
  1. **encoding-type=url**: ListObjectsV2 should support `encoding-type=url` query param
  2. **V1 encoding-type**: ListObjectsV1 should also support encoding-type parameter
  3. **Content-MD5 validation**: PutObject should validate Content-MD5 header when present
- Updated STATUS.md, DO_NEXT.md, WHAT_WE_DID.md — Stage 9b complete

## Session 1 — Project Scaffolding (2026-02-22)
- Created project structure: 18 source files + go.mod
- All handler, metadata, storage, and cluster packages stubbed
- Config structs defined with YAML tags
- Error types created (14 pre-defined S3 errors)
- StorageBackend interface with all methods
- MetadataStore interface with all methods
- XML utility structs and render functions

## Session 2 — Stage 1: Server Bootstrap & Configuration (2026-02-22)

### What was implemented:
- **cmd/bleepstore/main.go**: Rewrote to add graceful shutdown via `os/signal` + `context.WithTimeout`. Wired SIGINT/SIGTERM to `http.Server.Shutdown()` with a 30-second deadline. Server runs in a goroutine; main blocks on signal or server error. Added crash-only design comments.
- **internal/config/config.go**: Restructured config to match YAML structure from `bleepstore.example.yaml`. Added `ServerConfig.Region`, nested `MetadataConfig.SQLite.Path`, nested `StorageConfig.Local.RootDir`. Added fallback path loading (tries parent directory for `bleepstore.example.yaml`). Added `defaultConfig()` and `applyDefaults()` functions.
- **internal/server/server.go**: Added `GET /health` handler returning `{"status": "ok"}`. Added `commonHeaders()` middleware that injects `x-amz-request-id` (16-char hex via crypto/rand), `x-amz-id-2`, `Date` (RFC 7231), `Server: BleepStore` on every response. Stored `*http.Server` for graceful shutdown. Added `Shutdown()` method. Added dispatch for AbortMultipartUpload (DELETE with `?uploadId`), ListParts (GET with `?uploadId`). Changed fallback handlers for unknown sub-resources from `http.NotFound` to proper S3 error XML.
- **internal/errors/errors.go**: Added 9 new error codes: `NotImplemented`, `BucketAlreadyOwnedByYou`, `EntityTooSmall`, `InvalidAccessKeyId`, `InvalidArgument`, `PreconditionFailed`, `InvalidRange`, `MissingContentLength`, `RequestTimeTooSkewed`, `ServiceUnavailable`.
- **internal/xmlutil/xmlutil.go**: Added `RequestID` field to `ErrorResponse` (from `x-amz-request-id` header). Added `RenderError()` that takes `r *http.Request` to pull request ID from response headers. Added `WriteErrorResponse()` convenience function. Fixed `FormatTimeS3()` to use millisecond-precision ISO 8601 (`2006-01-02T15:04:05.000Z`). Added `FormatTimeHTTP()` for RFC 7231 format.
- **internal/handlers/bucket.go**: All 7 handlers now return proper S3 error XML (501 NotImplemented) via `xmlutil.WriteErrorResponse()`. HeadBucket returns 501 status with no body.
- **internal/handlers/object.go**: All 10 handlers now return proper S3 error XML (501 NotImplemented). HeadObject returns 501 status with no body.
- **internal/handlers/multipart.go**: All 6 handlers now return proper S3 error XML (501 NotImplemented).

### Key decisions:
- Request ID uses `crypto/rand` for 8 random bytes -> 16-char hex, matching AWS format
- `x-amz-id-2` set to same value as `x-amz-request-id` (simplified; real AWS uses a longer base64 value)
- HEAD requests return status code only with no body (per S3 spec)
- Config structure restructured to match the YAML nesting in `bleepstore.example.yaml`
- Error XML has NO xmlns namespace (per spec); success responses will add xmlns in later stages
- Common headers middleware wraps the entire mux, including health check (health check also gets request ID and Date headers)

## Session 3 — Stage 1b: Framework Migration to Huma, OpenAPI & Observability (2026-02-23)

### What was implemented:
- **go.mod**: Added dependencies: `github.com/danielgtaylor/huma/v2`, `github.com/go-chi/chi/v5`, `github.com/prometheus/client_golang`
- **internal/metrics/metrics.go** (new): Defined all Prometheus metrics with `init()` registration:
  - HTTP RED metrics: `bleepstore_http_requests_total` (CounterVec: method, path, status), `bleepstore_http_request_duration_seconds` (HistogramVec), `bleepstore_http_request_size_bytes` (HistogramVec), `bleepstore_http_response_size_bytes` (HistogramVec)
  - S3 operation metrics: `bleepstore_s3_operations_total` (CounterVec: operation, status), `bleepstore_objects_total` (Gauge), `bleepstore_buckets_total` (Gauge), `bleepstore_bytes_received_total` (Counter), `bleepstore_bytes_sent_total` (Counter)
  - `NormalizePath()` helper for mapping actual paths to template labels (avoids high-cardinality Prometheus labels)
- **internal/server/middleware.go** (new): Moved `commonHeaders` and `generateRequestID` from server.go. Added `responseRecorder` struct wrapping `http.ResponseWriter` to capture status code and bytes written. Added `metricsMiddleware` that times requests, records all Prometheus metrics, and excludes `/metrics` from self-instrumentation.
- **internal/server/server.go** (rewritten): Replaced `*http.ServeMux` with `chi.Router`. Created Huma API with `humachi.New(router, humaConfig)` — auto-registers `/docs` (Stoplight Elements) and `/openapi.json`. Registered `/health` via `huma.Register()` with struct-based I/O (`HealthOutput`, `HealthBody`). Registered `/metrics` via `router.Handle("/metrics", promhttp.Handler())`. Registered S3 catch-all via `router.HandleFunc("/*", s.dispatch)`. Middleware chain: `metricsMiddleware(commonHeaders(router))`. Preserved all existing dispatch logic unchanged.
- **internal/server/server_test.go** (new): Comprehensive unit tests covering: `/health` GET and HEAD, `/docs` HTML content, `/openapi.json` validity, `/metrics` with all `bleepstore_*` metrics, common S3 headers, all S3 stub routes returning 501 NotImplemented, `parsePath()` function.
- **internal/metrics/metrics_test.go** (new): Unit tests for `NormalizePath()` function and metric registration verification.

### Architecture decision — Hybrid approach:
- Chi is the primary router for all routes
- Huma is used selectively for `/health` (JSON, benefits from OpenAPI docs auto-generation)
- S3 routes go through catch-all `/*` on Chi with the existing dispatch pattern preserved
- Huma auto-serves `/docs` (Stoplight Elements) and `/openapi.json`
- Prometheus `/metrics` via `promhttp.Handler()`
- Critical ordering: Huma routes and `/metrics` registered FIRST on Chi, then catch-all `/*` last (Chi matches more specific routes first)

### Key decisions:
- Handler files (bucket.go, object.go, multipart.go) were NOT modified — they continue to use `http.ResponseWriter` directly
- `cmd/bleepstore/main.go` was NOT modified — Server interface (New, ListenAndServe, Shutdown) preserved
- Metrics middleware wraps everything but excludes `/metrics` from self-instrumentation
- `responseRecorder` implements `http.Flusher` for streaming compatibility
- Path normalization maps bucket/key paths to `/{bucket}` and `/{bucket}/{key}` templates
- Histogram buckets: default for duration, exponential for sizes (256 to 64MB)

## Session 4 — Stage 2: Metadata Store & SQLite (2026-02-23)

### What was implemented:

- **go.mod**: Added dependency: `modernc.org/sqlite v1.34.5` (pure Go SQLite, no CGO required)
- **internal/metadata/store.go** (rewritten): Expanded `MetadataStore` interface with 7 new methods: `UpdateBucketAcl`, `UpdateObjectAcl`, `ObjectExists`, `DeleteObjectsMeta`, `GetPartsForCompletion`, `GetCredential`, `PutCredential`. Expanded record structs:
  - `BucketRecord`: added `OwnerID`/`OwnerDisplay` (replacing single `Owner`), `ACL` as `json.RawMessage`
  - `ObjectRecord`: added `ContentEncoding`, `ContentLanguage`, `ContentDisposition`, `CacheControl`, `Expires`, `ACL`, `DeleteMarker`; `Owner` field renamed to split fields in record but kept conceptually
  - `MultipartUploadRecord`: added all content headers, ACL, UserMetadata, `OwnerID`/`OwnerDisplay`, StorageClass
  - `PartRecord`: `UploadedAt` renamed to `LastModified` for consistency
  - Added `CredentialRecord` struct
  - Changed `CompleteMultipartUpload` signature to accept `*ObjectRecord` instead of `[]int` (handlers compute the final object externally)
- **internal/metadata/sqlite.go** (fully implemented): Complete SQLite implementation:
  - Opens `*sql.DB` via `modernc.org/sqlite` driver
  - Applies PRAGMAs: WAL, synchronous=NORMAL, foreign_keys=ON, busy_timeout=5000
  - Creates 6 tables with indexes: `buckets`, `objects`, `multipart_uploads`, `multipart_parts`, `credentials`, `schema_version`
  - Implements all 20 `MetadataStore` methods:
    - Buckets: Create (with duplicate detection), Get, Delete (with not-empty check for objects and uploads), List (sorted), BucketExists, UpdateBucketAcl
    - Objects: Put (INSERT OR REPLACE upsert), Get, Delete, ObjectExists, DeleteObjectsMeta (batch), UpdateObjectAcl, ListObjects (prefix, delimiter, marker, continuation token, pagination, CommonPrefixes grouping)
    - Multipart: CreateMultipartUpload (auto-generates 32-char hex upload ID via crypto/rand), GetMultipartUpload, PutPart (INSERT OR REPLACE for overwrite), ListParts (pagination), GetPartsForCompletion (IN clause query), CompleteMultipartUpload (transaction: insert object + delete parts + delete upload), AbortMultipartUpload (transaction: delete parts + delete upload with not-found check)
    - Credentials: GetCredential, PutCredential (INSERT OR REPLACE)
    - ListMultipartUploads with prefix and key/uploadID marker pagination
  - Helper functions: `nullString` for nullable columns, `escapeLikePattern`, `scanObjectRow`/`scanObjectRows`
  - All timestamp columns use ISO 8601 format with millisecond precision
  - JSON columns for ACL and user_metadata stored as TEXT, marshaled/unmarshaled with encoding/json
- **internal/metadata/sqlite_test.go** (new): 25+ test functions:
  - `TestBucketCRUD`: create, get, list, delete, verify not found, exists/not exists
  - `TestBucketDuplicateCreate`: duplicate bucket name returns error
  - `TestDeleteBucketNotEmpty`: cannot delete bucket with objects
  - `TestDeleteBucketNotFound`: deleting non-existent bucket returns error
  - `TestListBuckets`: 3 buckets for owner1, 1 for owner2, sorted by name
  - `TestUpdateBucketAcl`: update ACL, verify; non-existent bucket errors
  - `TestObjectCRUD`: full round-trip with all fields (content headers, ACL, user metadata)
  - `TestPutObjectUpsert`: second put overwrites
  - `TestDeleteObjectIdempotent`: deleting non-existent key succeeds
  - `TestDeleteObjectsMeta`: batch delete 2 of 3 + non-existent, verify
  - `TestUpdateObjectAcl`: update, verify; non-existent errors
  - `TestListObjectsBasic`: 5 objects, list all
  - `TestListObjectsWithPrefix`: prefix filtering
  - `TestListObjectsWithDelimiter`: delimiter grouping with CommonPrefixes
  - `TestListObjectsPagination`: 3 pages with continuation tokens
  - `TestListObjectsWithMarker`: v1 marker pagination
  - `TestListObjectsEmptyBucket`: empty result
  - `TestMultipartLifecycle`: full flow: create upload, upload 3 parts, list parts, get parts for completion, complete, verify object created, verify upload/parts cleaned up
  - `TestMultipartAbort`: create upload, upload parts, abort, verify cleanup
  - `TestAbortMultipartUploadNotFound`: aborting non-existent upload errors
  - `TestPartOverwrite`: uploading same part number twice overwrites
  - `TestListPartsPagination`: 3 pages of parts
  - `TestListMultipartUploads`: list with prefix and pagination
  - `TestCredentialCRUD`: create, get, update (upsert), verify
  - `TestIdempotentSchema`: NewSQLiteStore twice on same DB works
  - `TestObjectDefaultFields`: minimal put gets default content type and storage class
- **internal/server/server.go** (updated): `Server` struct gains `meta` field of type `metadata.MetadataStore`. `New()` function accepts optional variadic `metadata.MetadataStore` parameter (backward compatible — existing callers without metadata store still work). Added `metadata` import.
- **cmd/bleepstore/main.go** (updated): On startup, creates parent directory for SQLite DB, opens `metadata.NewSQLiteStore(dbPath)`, defers `Close()`. Seeds default credentials from config (idempotent). Passes metadata store to `server.New(cfg, metaStore)`.

### Key decisions:
- Used `modernc.org/sqlite` (pure Go) over `mattn/go-sqlite3` (CGO) for cross-compilation simplicity
- `CompleteMultipartUpload` interface changed to accept a pre-built `*ObjectRecord` rather than computing the final object inside the metadata layer — this keeps the metadata store as a pure data layer while handlers handle business logic (ETag computation, size calculation)
- Record structs expanded with all fields from the spec (ACL as json.RawMessage, all content headers, DeleteMarker)
- `BucketRecord.Owner` split into `OwnerID`+`OwnerDisplay` to match the spec's canonical user model
- Upload ID generated as 32-char hex string via `crypto/rand` (no external UUID dependency)
- `server.New()` made backward compatible via variadic parameter — existing tests don't need modification
- Delete bucket checks both objects and multipart_uploads tables
- ListObjects with delimiter does application-level CommonPrefixes grouping (not SQL-level) per plan guidance

## Session 5 — Stage 3: Bucket CRUD (2026-02-23)

### What was implemented:

- **internal/handlers/bucket.go** (fully implemented): Replaced all 7 stub handlers with full implementations:
  - `ListBuckets`: Queries MetadataStore for all buckets owned by the configured owner, returns ListAllMyBucketsResult XML with Owner, Buckets/Bucket (Name, CreationDate in ISO 8601)
  - `CreateBucket`: Validates bucket name (3-63 chars, lowercase, no IP format, no xn-- prefix, no -s3alias suffix, no consecutive periods), parses optional canned ACL from x-amz-acl header, parses optional CreateBucketConfiguration XML body for LocationConstraint, checks for duplicate (returns 200 for us-east-1 owned-by-you behavior), creates metadata record and storage directory
  - `DeleteBucket`: Deletes from metadata store (which validates existence and emptiness), then removes storage directory. Returns 204 on success, 404 NoSuchBucket, 409 BucketNotEmpty
  - `HeadBucket`: Returns 200 with x-amz-bucket-region header if exists, 404 if not found (no body for HEAD)
  - `GetBucketLocation`: Returns LocationConstraint XML with empty value for us-east-1, 404 NoSuchBucket
  - `GetBucketAcl`: Parses stored JSON ACL back to AccessControlPolicy, returns XML with proper Owner and Grants
  - `PutBucketAcl`: Supports 3 mutually exclusive modes (canned ACL header, XML body, default private), updates ACL in metadata store
  - Added `NewBucketHandler()` constructor with dependency injection (MetadataStore, StorageBackend, ownerID, ownerDisplay, region)
  - Added `ensureBucketExists()` helper for reuse in future stages

- **internal/handlers/helpers.go** (new): Shared handler utilities:
  - `validateBucketName()`: Regex-based validation with compiled patterns, checks length, character set, IP format, xn-- prefix, -s3alias suffix, consecutive periods
  - `defaultPrivateACL()`: Creates default private ACL JSON with owner FULL_CONTROL
  - `parseCannedACL()`: Converts canned ACL names (private, public-read, public-read-write, authenticated-read) to AccessControlPolicy structs with proper grants
  - `aclToJSON()` / `aclFromJSON()`: Conversion between AccessControlPolicy and json.RawMessage for database storage
  - `extractBucketName()`: Extracts bucket name from URL path

- **internal/handlers/bucket_test.go** (new): Comprehensive unit tests using real in-memory SQLite and temp directory storage:
  - `TestValidateBucketName`: Table-driven tests for valid and invalid bucket names (18 cases)
  - `TestCreateBucket`: Create and verify Location header
  - `TestCreateBucketAlreadyOwnedByYou`: Idempotent creation returns 200
  - `TestCreateBucketInvalidName`: Invalid names return 400 InvalidBucketName
  - `TestDeleteBucket`: Create then delete returns 204
  - `TestDeleteBucketNotFound`: Non-existent bucket returns 404 NoSuchBucket
  - `TestHeadBucket`: Existing bucket returns 200 with x-amz-bucket-region
  - `TestHeadBucketNotFound`: Non-existent returns 404
  - `TestListBuckets`: Two buckets listed in sorted order with Owner, CreationDate, xmlns
  - `TestGetBucketLocation`: us-east-1 returns empty LocationConstraint
  - `TestGetBucketLocationNotFound`: Non-existent returns 404
  - `TestGetBucketAcl`: Default ACL has FULL_CONTROL, proper xmlns:xsi and xsi:type
  - `TestPutBucketAclCanned`: Set public-read, verify READ grant
  - `TestParseCannedACL`: Table-driven tests for all 4 canned ACL types

- **internal/xmlutil/xmlutil.go** (updated):
  - Added `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"` to all success response struct XMLName tags: ListAllMyBucketsResult, ListBucketResult, ListBucketV2Result, CopyObjectResult, InitiateMultipartUploadResult, CompleteMultipartUploadResult, ListPartsResult, ListMultipartUploadsResult, DeleteResult, LocationConstraint, AccessControlPolicy
  - Added `s3NS` constant for the namespace URI
  - Grantee: Changed `Type` field to `xml:"-"` (skip default XML marshaling)
  - Added custom `MarshalXML` for Grantee to produce `xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"` and `xsi:type="CanonicalUser"` attributes (Go's encoding/xml cannot produce namespace-prefixed attributes natively)
  - Added custom `UnmarshalXML` for Grantee to extract `xsi:type` attribute from incoming XML (for PutBucketAcl XML body mode)

- **internal/storage/local.go** (partially implemented):
  - `NewLocalBackend()`: Now creates root directory if it doesn't exist
  - `CreateBucket()`: Creates bucket directory via os.MkdirAll
  - `DeleteBucket()`: Removes bucket directory via os.Remove (only removes empty dirs)
  - `ObjectExists()`: Checks file existence via os.Stat
  - Other methods (PutObject, GetObject, DeleteObject, etc.) remain stubs for Stage 4

- **internal/server/server.go** (updated):
  - Added `storage.StorageBackend` field to Server struct
  - Added `ServerOption` functional options pattern: `WithMetadataStore()`, `WithStorageBackend()`
  - `New()` function now accepts `...interface{}` to support both MetadataStore and ServerOption args
  - Creates `BucketHandler` via `handlers.NewBucketHandler()` with injected MetadataStore, StorageBackend, owner info, and region from config
  - Backward compatible: tests without metadata store still work (bucket handlers return 500 InternalError instead of 501)

- **internal/server/server_test.go** (updated):
  - `newTestServer()` now includes Auth config for owner info
  - `TestS3StubRoutes`: Updated expectations — bucket routes return 500 InternalError (no meta store) instead of 501 NotImplemented; object/multipart routes still return 501

- **cmd/bleepstore/main.go** (updated):
  - Initializes LocalBackend from `cfg.Storage.Local.RootDir` with directory creation
  - Passes storage backend to server via `server.WithStorageBackend(storageBackend)`
  - Logs storage backend initialization

### Key decisions:
- Constructor injection pattern for handlers: `NewBucketHandler(meta, store, ownerID, ownerDisplay, region)` — no global state
- `server.New()` uses `...interface{}` with type switch to support both old-style MetadataStore args and new-style ServerOption args — preserves backward compatibility
- Bucket handlers check for nil dependencies and return 500 InternalError rather than panic — graceful degradation
- ACL stored as JSON in SQLite, converted to/from AccessControlPolicy structs for XML rendering
- Custom MarshalXML/UnmarshalXML for Grantee to handle the xmlns:xsi and xsi:type attributes that Go's encoding/xml cannot produce natively
- All XML success responses now include the S3 namespace URI — this is a breaking change for the TestS3StubRoutes test expectations (500 vs 501 for bucket routes), which was updated
- Storage directory creation is best-effort in CreateBucket — if it fails, logging occurs but the response still succeeds (directory will be created on first object write)
- us-east-1 behavior: creating a bucket you already own returns 200 OK (not 409 BucketAlreadyOwnedByYou)

## Session 6 — Stage 4: Basic Object CRUD (2026-02-23)

### What was implemented:

- **internal/storage/local.go** (fully implemented): Complete `LocalBackend` implementation for all storage operations:
  - `PutObject`: Crash-only atomic write: write to `.tmp/` temp file, compute MD5 via `io.TeeReader`, `fsync`, `os.Rename` to final path. Returns bytes written and quoted ETag (`"hex-md5"`).
  - `GetObject`: Opens file, `os.Stat` for size, returns `io.ReadCloser` for streaming to client.
  - `DeleteObject`: Removes file via `os.Remove`, then cleans empty parent directories up to bucket root.
  - `CopyObject`: Opens source file, calls PutObject with source as reader for atomic copy with new ETag.
  - `PutPart`: Writes part to `.multipart/{uploadID}/{partNumber}` with atomic temp-fsync-rename.
  - `AssembleParts`: Concatenates parts into final object, computes composite ETag, removes part directory.
  - `DeleteParts`: Removes `.multipart/{uploadID}/` directory via `os.RemoveAll`.
  - `CleanTempFiles`: Removes all files in `.tmp/` directory (crash-only startup recovery).
  - `ObjectExists`: Enhanced to verify target is a file, not a directory.
  - `NewLocalBackend`: Now creates `.tmp/` directory alongside root directory.
  - Helper functions: `objectPath`, `tempPath`, `cleanEmptyParents`.

- **internal/uid/uid.go** (new package): Unique ID generation using `crypto/rand` for temp file names (32-char hex strings).

- **internal/handlers/object.go** (4 CRUD operations implemented):
  - `NewObjectHandler()`: Constructor with dependency injection (MetadataStore, StorageBackend, ownerID, ownerDisplay), following BucketHandler pattern.
  - `PutObject`: Validates bucket existence, extracts Content-Type (default: `application/octet-stream`), user metadata (`x-amz-meta-*` headers), content headers (Encoding, Language, Disposition, Cache-Control, Expires), canned ACL. Writes to storage backend (atomic), then commits metadata to SQLite. Returns ETag in response header. Never acknowledges before commit.
  - `GetObject`: Validates bucket and key existence via metadata, opens storage reader, sets all S3-required response headers (Content-Type, ETag, Last-Modified, Content-Length, Accept-Ranges, user metadata, content headers), streams data to client via `io.Copy`.
  - `HeadObject`: Same as GetObject but no body. Returns all metadata headers without streaming object data.
  - `DeleteObject`: Validates bucket, deletes metadata first (authoritative record), then deletes storage file (best-effort). Always returns 204 (idempotent, even for non-existent keys).
  - `extractObjectKey()`: Extracts key from URL path after bucket name.
  - Remaining stubs preserved for Stage 5a/5b: CopyObject, DeleteObjects, ListObjectsV2, ListObjects, GetObjectAcl, PutObjectAcl.

- **internal/handlers/helpers.go** (expanded with new helpers):
  - `extractUserMetadata()`: Scans `x-amz-meta-*` request headers, strips prefix, lowercases key, returns `map[string]string`.
  - `setObjectResponseHeaders()`: Sets all S3 object response headers from ObjectRecord: Content-Type, ETag, Last-Modified, Accept-Ranges, Content-Length, Content-Encoding, Content-Language, Content-Disposition, Cache-Control, Expires, x-amz-storage-class, x-amz-meta-* user metadata. Used by both GetObject and HeadObject.

- **internal/server/server.go** (updated): Changed ObjectHandler instantiation from `&handlers.ObjectHandler{}` to `handlers.NewObjectHandler(s.meta, s.store, ownerID, ownerDisplay)` for constructor injection with dependencies.

- **cmd/bleepstore/main.go** (updated): Added crash-only recovery step — `storageBackend.CleanTempFiles()` on every startup to remove orphan temp files from incomplete writes.

- **internal/handlers/object_test.go** (new): 16 test functions using real in-memory SQLite + temp dir storage:
  - `TestPutAndGetObject`: Put, get, verify content, ETag, Content-Type, Content-Length, Last-Modified, Accept-Ranges
  - `TestHeadObject`: Head returns metadata headers with no body
  - `TestHeadObjectNotFound`: 404 for non-existent key
  - `TestGetObjectNotFound`: 404 NoSuchKey error XML
  - `TestGetObjectNoSuchBucket`: 404 NoSuchBucket error XML
  - `TestDeleteObject`: Put, delete, verify gone
  - `TestDeleteObjectIdempotent`: Delete non-existent returns 204
  - `TestPutObjectOverwrite`: Two puts, verify second overwrites, ETags differ
  - `TestPutObjectWithUserMetadata`: x-amz-meta-* headers round-trip via HeadObject
  - `TestPutObjectDefaultContentType`: Default application/octet-stream when no Content-Type
  - `TestPutObjectNestedKey`: Keys with `/` create subdirectories
  - `TestPutObjectEmptyBody`: Zero-byte objects work correctly
  - `TestExtractObjectKey`: Table-driven tests for key extraction from URL path
  - `TestExtractUserMetadata`: Verify header scanning and prefix stripping
  - `TestExtractUserMetadataEmpty`: No meta headers returns nil

- **internal/storage/local_test.go** (new): 15 test functions for the storage backend:
  - `TestPutAndGetObject`, `TestPutObjectNestedKey`, `TestPutObjectAtomicWrite` (verify .tmp is clean)
  - `TestDeleteObject`, `TestDeleteObjectIdempotent`, `TestDeleteObjectCleansEmptyDirs`
  - `TestObjectExists`, `TestCleanTempFiles`, `TestCopyObject`
  - `TestGetObjectNotFound`, `TestPutObjectEmptyBody`
  - `TestCreateAndDeleteBucket`, `TestPutObjectOverwrite`

- **internal/server/server_test.go** (updated): Updated `TestS3StubRoutes` expected status codes — object PUT/GET/HEAD/DELETE now return 500 (InternalError, nil deps) instead of 501 (NotImplemented). Object ACL and list/copy/batch/multipart routes remain 501.

### Key decisions:
- Atomic writes via `.tmp/` directory with `crypto/rand`-generated unique names; `fsync` before `os.Rename`
- ETag = quoted MD5 hex digest (`"abc123..."`) computed during write via `io.TeeReader`
- Metadata is the authority: GetObject reads metadata first (for headers), then opens storage. If file missing but metadata exists, returns 500 (not 404) — crash-only design logs this as a database-as-index inconsistency
- Delete order: metadata first (authoritative), storage second (best-effort). Orphan files on disk are safe.
- Empty directory cleanup: After DeleteObject, walks up from the deleted file's parent to bucket root, removing empty directories
- Constructor injection for ObjectHandler: `NewObjectHandler(meta, store, ownerID, ownerDisplay)`
- `setObjectResponseHeaders` shared between GetObject and HeadObject to avoid duplication
- User metadata extracted from `x-amz-meta-*` headers, stored as `map[string]string` in JSON, returned as `x-amz-meta-*` response headers

## Session 7 — Stage 5a: List, Copy & Batch Delete (2026-02-23)

### What was implemented:

- **internal/handlers/object.go** (4 new handlers):
  - `CopyObject`: Parses `X-Amz-Copy-Source` header with `url.PathUnescape`, splits into source bucket/key. Verifies both source and destination buckets and source object exist. Copies file data via `storage.CopyObject` (atomic). Supports `x-amz-metadata-directive`: COPY (default, copies source metadata) or REPLACE (uses request headers). Commits destination metadata. Returns CopyObjectResult XML with ETag and LastModified.
  - `DeleteObjects`: Parses `<Delete>` XML body via `xml.NewDecoder`. Iterates keys, deleting metadata first (authoritative) then storage (best-effort). Supports `<Quiet>true</Quiet>` mode (only errors reported, no Deleted entries). Returns DeleteResult XML.
  - `ListObjectsV2`: Reads query params `prefix`, `delimiter`, `max-keys` (default 1000), `start-after`, `continuation-token`, `encoding-type`. Calls `meta.ListObjects`. Renders ListBucketV2Result XML with KeyCount, MaxKeys, IsTruncated, NextContinuationToken, Contents, CommonPrefixes, Delimiter.
  - `ListObjects` (v1): Reads query params `prefix`, `delimiter`, `max-keys` (default 1000), `marker`. Calls `meta.ListObjects`. Renders ListBucketResult XML with Marker, NextMarker, MaxKeys, IsTruncated, Contents, CommonPrefixes, Delimiter.

- **internal/handlers/helpers.go** (2 new helpers):
  - `parseDeleteRequest`: Parses a Delete XML request body into `DeleteRequest` struct using `xml.NewDecoder`.
  - `parseCopySource`: Parses `X-Amz-Copy-Source` header — URL-decodes, trims leading slash, splits at first `/` into bucket and key.
  - Added imports: `encoding/xml`, `io`, `net/url`.

- **internal/xmlutil/xmlutil.go** (4 additions):
  - `DeleteRequest` struct: Parses `<Delete>` XML input with `Quiet` bool and `Objects` slice.
  - `DeleteRequestObj` struct: Single object key in delete request.
  - `ListBucketResult`: Added `NextMarker` (omitempty), `Delimiter` (omitempty) fields.
  - `ListBucketV2Result`: Added `Delimiter` (omitempty), `EncodingType` (omitempty) fields.

- **internal/handlers/object_test.go** (18 new test functions):
  - CopyObject: TestCopyObject, TestCopyObjectWithReplaceDirective, TestCopyObjectNonexistentSource, TestCopyObjectInvalidSource
  - DeleteObjects: TestDeleteObjects, TestDeleteObjectsQuietMode, TestDeleteObjectsMalformedXML
  - ListObjectsV2: TestListObjectsV2, TestListObjectsV2WithPrefix, TestListObjectsV2WithDelimiter, TestListObjectsV2Pagination, TestListObjectsV2EmptyBucket, TestListObjectsV2StartAfter, TestListObjectsV2ContentFields, TestListObjectsV2NoSuchBucket
  - ListObjects V1: TestListObjectsV1, TestListObjectsV1WithMarker
  - Helpers: TestParseCopySource (7 table-driven cases)
  - Helper function: `putTestObjects` for creating multiple objects in tests.

### Key decisions:
- CopyObject defaults to COPY directive (same as S3). Source metadata is fully duplicated to destination.
- REPLACE directive uses request headers for Content-Type, user metadata, ACL — ignoring source metadata entirely.
- DeleteObjects processes keys sequentially. Metadata deletion is authoritative; storage deletion is best-effort. Errors for individual keys are reported in the DeleteResult XML.
- Quiet mode: in quiet mode, successful deletes are NOT reported (no `<Deleted>` entries), only errors.
- Both list operations default to max-keys=1000. `strconv.Atoi` parses the max-keys query param.
- No new dependencies — all stdlib (`encoding/xml`, `net/url`, `strconv`).
- The existing `storage.CopyObject` and `metadata.ListObjects` methods from Stages 2 and 4 are reused as-is.

## Session 8 — Stage 5b: Range, Conditional Requests & Object ACLs (2026-02-23)

### What was implemented:

- **internal/handlers/helpers.go** (2 new helper functions):
  - `parseRange(rangeHeader string, objectSize int64) (start, end int64, err error)`: Parses HTTP Range header. Supports three formats: `bytes=0-4` (start-end), `bytes=5-` (open-ended), `bytes=-10` (suffix/last N bytes). Clamps end to last byte. Returns error for unsatisfiable ranges, multi-range requests, empty objects, and invalid syntax.
  - `checkConditionalHeaders(r *http.Request, etag string, lastModified time.Time) (statusCode int, skip bool)`: Evaluates conditional request headers per RFC 7232 priority:
    1. `If-Match`: 412 on ETag mismatch (supports `*` wildcard and comma-separated ETags)
    2. `If-Unmodified-Since`: 412 if modified after given time (only if If-Match absent)
    3. `If-None-Match`: 304 for GET/HEAD on match, 412 for other methods (supports `*` wildcard)
    4. `If-Modified-Since`: 304 if not modified since given time (only if If-None-Match absent)
    - ETag comparison normalizes by stripping surrounding quotes
    - Time comparison truncates to second precision (HTTP dates have 1-second granularity)

- **internal/handlers/object.go** (GetObject enhanced, HeadObject enhanced, 2 handlers implemented):
  - `GetObject` (enhanced with range + conditional):
    - Evaluates conditional headers BEFORE opening storage data (early return for 304/412)
    - 304 Not Modified: sets ETag + Last-Modified headers, returns no body
    - 412 Precondition Failed: returns S3 error XML
    - Range request handling: parses Range header, seeks to start position using `io.ReadSeeker` (with discard fallback), sets `Content-Range` header, returns 206 Partial Content with only the requested byte range
    - 416 Range Not Satisfiable: sets `Content-Range: bytes */SIZE` header, returns InvalidRange S3 error
  - `HeadObject` (enhanced with conditional):
    - Evaluates conditional headers, returns 304 or 412 as appropriate
    - Sets ETag + Last-Modified even on conditional failure responses
  - `GetObjectAcl` (new implementation): Validates bucket and object existence, parses stored JSON ACL via `aclFromJSON()`, falls back to default private ACL if none stored, ensures Owner is set, renders AccessControlPolicy XML
  - `PutObjectAcl` (new implementation): Validates bucket and object existence. Three mutually exclusive modes: (1) canned ACL via `x-amz-acl` header, (2) XML body with `AccessControlPolicy`, (3) default to private. Stores ACL via `metadata.UpdateObjectAcl()`. Returns 200 on success.
  - Added imports: `encoding/xml`, `fmt`

- **internal/handlers/object_test.go** (26 new test functions):
  - Range parsing unit tests: TestParseRange (15 table-driven cases: first bytes, open-end, suffix, single byte, clamping, unsatisfiable, invalid syntax, multi-range, empty object)
  - Range handler tests: TestGetObjectRangeFirstBytes, TestGetObjectRangeOpenEnd, TestGetObjectRangeSuffix, TestGetObjectRangeUnsatisfiable (verify 206 status, Content-Range header, Content-Length, body content, 416 for unsatisfiable)
  - Conditional handler tests: TestGetObjectIfMatch (match succeeds, mismatch returns 412), TestGetObjectIfNoneMatch (match returns 304, no-match succeeds), TestHeadObjectIfNoneMatch (HEAD returns 304), TestGetObjectIfModifiedSince (future date returns 304, past date succeeds), TestGetObjectIfUnmodifiedSince (future date succeeds, past date returns 412)
  - checkConditionalHeaders unit tests: TestCheckConditionalHeaders (14 table-driven cases: no headers, If-Match match/mismatch/wildcard, If-None-Match match GET/HEAD/PUT/no-match, If-Modified-Since modified/not-modified, If-Unmodified-Since modified/not-modified, priority: If-Match > If-Unmodified-Since, If-None-Match > If-Modified-Since)
  - Object ACL tests: TestGetObjectAcl (default ACL has FULL_CONTROL, proper xsi attributes), TestGetObjectAclNoSuchKey (404), TestPutObjectAclCanned (public-read round-trip), TestPutObjectAclXMLBody (XML ACL body round-trip), TestPutObjectAclNoSuchKey (404), TestGetObjectAclNoSuchBucket (404)
  - Added `time` import to test file

- **internal/server/server_test.go** (updated):
  - Updated `TestS3StubRoutes` expectations: object ACL routes now return 500 InternalError (implemented, no metadata store) instead of 501 NotImplemented

### Key decisions:
- Conditional headers are evaluated BEFORE opening the storage file, avoiding unnecessary I/O for 304/412 responses
- Range requests use `io.ReadSeeker` (cast from the `os.File` returned by LocalBackend) for efficient seeking. A discard-based fallback handles non-seekable readers.
- 304 Not Modified responses include ETag and Last-Modified headers (per RFC 7232 Section 4.1)
- ETag comparison strips surrounding quotes for normalization — handles both `"abc"` and `abc` formats
- Time comparison truncates to second precision since HTTP dates (RFC 7231) only have 1-second resolution
- Object ACL reuses all existing ACL infrastructure from Stage 3: `aclFromJSON`, `aclToJSON`, `parseCannedACL`, `defaultPrivateACL`, and the `xmlutil.AccessControlPolicy` XML rendering with custom `Grantee.MarshalXML`
- Multi-range requests (e.g., `bytes=0-4,10-20`) are not supported — returns error. S3 only supports single ranges.
- No new dependencies — all stdlib (`fmt`, `encoding/xml`, `time`, `io`).

## Session 10 — Stage 7: Multipart Upload - Core (2026-02-23)

### What was implemented:

- **internal/handlers/multipart.go** (fully rewritten from stubs): Complete multipart handler implementation:
  - `NewMultipartHandler(meta, store, ownerID, ownerDisplay)`: Constructor with dependency injection, following the same pattern as BucketHandler and ObjectHandler.
  - `CreateMultipartUpload`: Validates bucket exists, extracts Content-Type (default: `application/octet-stream`), content headers, user metadata (`x-amz-meta-*`), canned ACL. Creates `MultipartUploadRecord` in metadata store. Returns `InitiateMultipartUploadResult` XML with generated upload ID.
  - `UploadPart`: Validates upload ID and part number (1-10000 via `strconv.Atoi`). Verifies upload exists in metadata. Writes part data via `store.PutPart` (atomic: temp-fsync-rename to `.multipart/{uploadID}/{partNumber}`). Records `PartRecord` in metadata. Returns ETag header. Detects `X-Amz-Copy-Source` header and delegates to `uploadPartCopy`.
  - `uploadPartCopy` (private): Handles UploadPartCopy — parses copy source header, verifies source bucket/object exist, opens source data from storage, handles optional `X-Amz-Copy-Source-Range` header (with seek or discard fallback), writes to part storage, records metadata, returns `CopyPartResult` XML.
  - `AbortMultipartUpload`: Verifies upload exists. Deletes part files from storage (best-effort via `store.DeleteParts`). Deletes upload and part metadata via `meta.AbortMultipartUpload`. Returns 204 No Content.
  - `ListMultipartUploads`: Validates bucket exists. Parses query params: `prefix`, `delimiter`, `key-marker`, `upload-id-marker`, `max-uploads` (default 1000). Queries metadata. Builds and renders `ListMultipartUploadsResult` XML with uploads, common prefixes, pagination markers.
  - `ListParts`: Validates upload exists. Parses `part-number-marker` and `max-parts` (default 1000). Queries metadata. Renders `ListPartsResult` XML.
  - `CompleteMultipartUpload`: Still returns 501 NotImplemented (deferred to Stage 8).
  - Added `getQueryValue` helper for extracting values from `url.Values` map.

- **internal/xmlutil/xmlutil.go** (expanded):
  - Added `CopyPartResult` struct with S3 namespace in XMLName tag.
  - Added `RenderCopyPartResult(w, result)` function.

- **internal/server/server.go** (updated):
  - Changed `s.multi` initialization from `&handlers.MultipartHandler{}` to `handlers.NewMultipartHandler(s.meta, s.store, ownerID, ownerDisplay)` for full dependency injection.

- **internal/server/server_test.go** (updated):
  - Updated `TestS3StubRoutes` expected status codes: multipart routes now return 500 InternalError (nil dependencies in test server) instead of 501 NotImplemented.
  - `ListMultipartUploads` (GET `?uploads`): 500 InternalError.
  - `CreateMultipartUpload` (POST `?uploads`): 500 InternalError.
  - `UploadPart` (PUT `?partNumber&uploadId`): 500 InternalError.
  - `AbortMultipartUpload` (DELETE `?uploadId`): 500 InternalError.
  - `ListParts` (GET `?uploadId`): 500 InternalError.
  - `CompleteMultipartUpload` (POST `?uploadId`): remains 501 NotImplemented (Stage 8).

- **internal/handlers/multipart_test.go** (new file): 14 test functions with comprehensive coverage:
  - `TestCreateMultipartUpload`: Create upload, verify XML response with Bucket, Key, UploadId.
  - `TestCreateMultipartUploadNoSuchBucket`: 404 NoSuchBucket error.
  - `TestUploadPart`: Upload part, verify 200 + ETag header.
  - `TestUploadPartInvalidPartNumber`: Table-driven (5 cases: 0, -1, 10001, abc, empty) → all InvalidArgument.
  - `TestUploadPartNoSuchUpload`: 404 NoSuchUpload error.
  - `TestUploadPartOverwrite`: Same part number twice overwrites, ETag may differ.
  - `TestUploadPartETag`: ETag is quoted MD5 hex string matching `"[0-9a-f]{32}"`.
  - `TestAbortMultipartUpload`: Create, upload parts, abort, verify 204 + parts cleaned.
  - `TestAbortMultipartUploadNoSuchUpload`: 404 NoSuchUpload error.
  - `TestListMultipartUploads`: Create 2 uploads, list, verify both present with Owner/Initiated.
  - `TestListMultipartUploadsWithPrefix`: 2 uploads with different key prefixes, filter by prefix.
  - `TestListMultipartUploadsNoSuchBucket`: 404 NoSuchBucket error.
  - `TestListParts`: Upload 3 parts, list, verify all present with PartNumber/ETag/Size/LastModified.
  - `TestListPartsNoSuchUpload`: 404 NoSuchUpload error.
  - `TestListPartsXMLStructure`: Verify full XML structure including Bucket, Key, UploadId, MaxParts.
  - `TestMultipartLifecycleCreateUploadAbort`: Full lifecycle test — create, upload 3 parts, abort, verify cleanup.
  - `TestCreateMultipartUploadWithContentType`: Content-Type captured in upload record.
  - Uses real in-memory SQLite + temp dir storage (same pattern as bucket_test.go and object_test.go).

### Key decisions:
- UploadPartCopy implemented as a private method `uploadPartCopy` called from `UploadPart` when `X-Amz-Copy-Source` header is detected — avoids dispatch changes.
- CopyPartResult XML type added to xmlutil package (not inline) for consistency with all other XML types.
- Part size tracking: uses `r.ContentLength` for direct uploads, computed from source object size for UploadPartCopy (with range adjustment).
- Storage delete on abort is best-effort — if storage delete fails, metadata deletion still proceeds (metadata is authoritative).
- Nil dependency checks return 500 InternalError (graceful degradation) — not panic.
- No new external dependencies — all stdlib.
- Owner identity extracted from auth context when available, falls back to handler defaults.

## Session 9 — Stage 6: AWS Signature V4 (2026-02-23)

### What was implemented:

- **internal/auth/sigv4.go** (fully rewritten from stub): Complete AWS SigV4 implementation:
  - `SigV4Verifier` struct: holds MetadataStore reference and region. Looks up credentials from SQLite by access key ID (supports multiple credentials).
  - `VerifyRequest(r *http.Request) (*CredentialRecord, error)`: Full header-based SigV4 verification:
    1. Parses `Authorization` header: extracts Credential (access key, date, region, service, terminator), SignedHeaders, Signature
    2. Looks up credential by access key ID from metadata store
    3. Validates clock skew (15-minute tolerance)
    4. Verifies credential date matches X-Amz-Date date portion
    5. Builds canonical request: Method + CanonicalURI + CanonicalQueryString + CanonicalHeaders + SignedHeaders + HashedPayload
    6. Builds string to sign: AWS4-HMAC-SHA256 + timestamp + scope + SHA256(canonical request)
    7. Derives signing key: HMAC-SHA256 chain (AWS4+secret -> date -> region -> service -> aws4_request)
    8. Computes expected signature and compares with `crypto/subtle.ConstantTimeCompare`
  - `VerifyPresigned(r *http.Request) (*CredentialRecord, error)`: Presigned URL validation:
    1. Extracts X-Amz-Algorithm, X-Amz-Credential, X-Amz-Date, X-Amz-Expires, X-Amz-SignedHeaders, X-Amz-Signature from query params
    2. Validates algorithm = AWS4-HMAC-SHA256
    3. Validates X-Amz-Expires in range [1, 604800] (max 7 days)
    4. Checks expiration: now <= parse(X-Amz-Date) + X-Amz-Expires
    5. Builds canonical request with X-Amz-Signature excluded from query string, UNSIGNED-PAYLOAD as payload hash
    6. Verifies signature with constant-time comparison
  - `URIEncode(s string, encodeSlash bool)`: S3-compatible URI encoding. Unreserved: A-Za-z0-9-_.~. Percent-encodes with uppercase hex. Spaces as %20. Slashes optionally preserved.
  - `DetectAuthMethod(r *http.Request)`: Returns "header", "presigned", "ambiguous", or "none"
  - `AuthError` type with S3 error code for structured error handling
  - `parseAuthorizationHeader`: Parses the Authorization header format
  - `buildCanonicalRequest`, `buildPresignedCanonicalRequest`: Canonical request construction
  - `buildStringToSign`: String-to-sign construction
  - `deriveSigningKey`: HMAC-SHA256 chain for signing key derivation
  - `canonicalURI`, `canonicalQueryString`, `canonicalHeaders`: Helper functions for canonical components
  - `hmacSHA256`: HMAC-SHA256 computation
  - Context key types for owner identity: `ownerIDKey`, `ownerDisplayKey`
  - `OwnerFromContext`, `contextWithOwner`: Context-based owner identity access

- **internal/auth/middleware.go** (new file): HTTP authentication middleware:
  - `Middleware(verifier)` returns `func(http.Handler) http.Handler`
  - Skips authentication for /health, /metrics, /docs, /openapi, /openapi.json paths
  - Detects auth method (header vs presigned vs ambiguous vs none)
  - Calls VerifyRequest or VerifyPresigned depending on method
  - Returns S3 error XML on failure: AccessDenied (no auth), InvalidAccessKeyId, SignatureDoesNotMatch, RequestTimeTooSkewed
  - Sets authenticated owner identity on request context via `contextWithOwner`
  - Maps AuthError codes to pre-defined S3 error types

- **internal/auth/sigv4_test.go** (new file): 23+ test functions:
  - `TestURIEncode`: Table-driven (11 cases) — unreserved chars, spaces, slashes, special chars, unicode, empty
  - `TestHmacSHA256`: Known HMAC test vector
  - `TestDeriveSigningKey`: AWS documentation test vector (wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY)
  - `TestCanonicalURI`: Table-driven (6 cases) — empty, root, paths, spaces, percent chars
  - `TestCanonicalQueryString`: Table-driven (5 cases) — empty, single, sorted, bare keys, special chars
  - `TestParseAuthorizationHeader`: Valid header parsing, wrong algorithm, missing credential, invalid format
  - `TestDetectAuthMethod`: No auth, header, presigned, ambiguous
  - `TestVerifyRequestValidSignature`: Full round-trip: sign request, verify passes
  - `TestVerifyRequestWrongSecretKey`: Wrong secret -> SignatureDoesNotMatch
  - `TestVerifyRequestInvalidAccessKey`: Unknown key -> InvalidAccessKeyId
  - `TestVerifyRequestMissingAuthHeader`: No auth -> AccessDenied
  - `TestVerifyRequestClockSkew`: 20-min skew -> RequestTimeTooSkewed
  - `TestVerifyRequestPutObject`: PUT with Content-Type and actual SHA-256 payload hash
  - `TestVerifyRequestWithQueryParams`: GET with list-type, prefix, delimiter query params
  - `TestVerifyPresignedValid`: Full presigned URL round-trip
  - `TestVerifyPresignedExpired`: Expired presigned -> AccessDenied
  - `TestVerifyPresignedInvalidExpires`: Expires > 604800 -> error
  - `TestOwnerFromContext`: Empty context and populated context
  - `TestBuildStringToSign`: Verify 4-line format with SHA-256
  - `TestVerifyRequestMultipleCredentials`: Two credentials, sign with user2, verify returns user2
  - `TestCanonicalHeaders`: Verify header formatting

- **internal/server/server.go** (updated):
  - Added `auth` import
  - Added `verifier *auth.SigV4Verifier` field to Server struct
  - `New()`: creates SigV4Verifier when metadata store is available (`s.meta != nil`)
  - `ListenAndServe()`: middleware chain updated to: metricsMiddleware -> commonHeaders -> authMiddleware -> router
  - Auth middleware only applied when verifier is non-nil (backward compatible — tests without metadata store still work)

### Key decisions:
- SigV4Verifier uses MetadataStore for credential lookup rather than hardcoded credentials — supports multiple access keys from the database
- Auth middleware wraps the router inside the commonHeaders middleware, so common headers (x-amz-request-id, Date, Server) are set even on auth failure responses
- Excluded paths: /health, /metrics, /docs, /openapi.json — these are infrastructure endpoints that should not require SigV4
- Context-based owner identity: `contextWithOwner` / `OwnerFromContext` use unexported context key types to avoid collisions. Handlers can retrieve the authenticated owner from the request context.
- AuthError type provides structured error codes that map directly to S3 error responses
- Constant-time comparison via `crypto/subtle.ConstantTimeCompare` prevents timing attacks
- Custom URI encoding matches S3 behavior exactly: unreserved chars A-Za-z0-9-_.~ are not encoded, spaces become %20 (not +), slashes optionally preserved for URI path vs query values
- Clock skew tolerance set to 15 minutes (same as AWS)
- Presigned URL max expiry: 604800 seconds (7 days)
- No new dependencies — all stdlib: `crypto/hmac`, `crypto/sha256`, `crypto/subtle`, `encoding/hex`
- Server test backward compatibility preserved: tests without metadata store create a nil verifier, auth middleware is not applied

## Session 11 — Stage 8: Multipart Upload - Completion (2026-02-23)

### What was implemented:

- **internal/handlers/multipart.go** (`CompleteMultipartUpload` fully implemented):
  - Validates bucket, key, and uploadId query parameter
  - Verifies upload exists in metadata store (404 NoSuchUpload if not)
  - Parses `<CompleteMultipartUpload>` XML request body via `parseCompleteMultipartXML`
  - Validates part order: ascending by PartNumber, no duplicates (400 InvalidPartOrder)
  - Fetches stored part records from metadata via `GetPartsForCompletion`
  - Validates each requested part exists in stored parts (400 InvalidPart if missing)
  - Validates ETag match between request and stored parts (400 InvalidPart on mismatch)
  - Validates part sizes: all non-last parts must be >= 5 MiB (400 EntityTooSmall)
  - Calls `storage.AssembleParts` to concatenate part files into final object (atomic temp-fsync-rename)
  - Computes total object size from stored part sizes
  - Builds final `ObjectRecord` from upload metadata (content type, headers, ACL, user metadata)
  - Calls `metadata.CompleteMultipartUpload` (transactional: insert object, delete parts, delete upload)
  - Returns `CompleteMultipartUploadResult` XML with Location, Bucket, Key, ETag
  - Added `fmt` to imports

- **internal/handlers/helpers.go** (3 new types/functions):
  - `CompletePart` struct: `PartNumber int` + `ETag string` with XML tags
  - `CompleteMultipartUploadRequest` struct: XML container with `[]CompletePart`
  - `parseCompleteMultipartXML(body io.Reader) ([]CompletePart, error)`: Parses request XML body
  - `computeCompositeETag(partETags []string) string`: Computes S3-style composite ETag — strips quotes, decodes hex to raw MD5, concatenates raw bytes, MD5 of concatenation, formatted as `"hexdigest-N"` where N = part count
  - Added `crypto/md5`, `encoding/hex` to imports

- **internal/server/server_test.go** (updated):
  - Updated `TestS3StubRoutes`: CompleteMultipartUpload (POST `?uploadId`) now returns 500 InternalError (nil dependencies) instead of 501 NotImplemented

- **internal/handlers/multipart_test.go** (14 new test functions):
  - `TestCompleteMultipartUpload`: Full completion with 3 parts (5 MiB + 5 MiB + small), verifies XML response (Bucket, Key, ETag with `-3` suffix, Location), verifies object in metadata, verifies upload cleaned up, verifies assembled content on disk
  - `TestCompleteMultipartUploadInvalidPartOrder`: Descending part order returns 400 InvalidPartOrder
  - `TestCompleteMultipartUploadDuplicatePartNumber`: Same part number twice returns 400 InvalidPartOrder
  - `TestCompleteMultipartUploadWrongETag`: ETag mismatch returns 400 InvalidPart
  - `TestCompleteMultipartUploadMissingPart`: Non-existent part number returns 400 InvalidPart
  - `TestCompleteMultipartUploadNoSuchUpload`: Non-existent upload returns 404 NoSuchUpload
  - `TestCompleteMultipartUploadEmptyBody`: Empty body returns 400 MalformedXML
  - `TestCompleteMultipartUploadEntityTooSmall`: Non-last part < 5 MiB returns 400 EntityTooSmall
  - `TestCompleteMultipartUploadSinglePart`: Single small part succeeds (last part exempt from size check)
  - `TestCompleteMultipartUploadCompositeETag`: Verifies `computeCompositeETag` against manually computed expected value
  - `TestCompleteMultipartUploadXMLStructure`: Verifies xmlns, XML declaration, required elements (Location, Bucket, Key, ETag)
  - `TestParseCompleteMultipartXML`: Parses valid XML, verifies 2 parts with correct PartNumber/ETag
  - `TestParseCompleteMultipartXMLInvalid`: Invalid XML returns error
  - `TestCompleteMultipartUploadFullLifecycle`: End-to-end: create upload with Content-Type, upload 2 parts, complete, retrieve via GetObject, verify content and Content-Type preserved
  - Helper functions: `completeMultipartUploadXML` (builds XML body), `uploadTestParts` (creates upload and uploads N parts)

### Test count: 165 unit tests passing (up from 151, +14 new tests)

## Session 13 — Stage 9a: E2E Verification & Fixes (2026-02-23)
- Ran full Python/boto3 E2E suite: 81/86 initially, 84/86 after fixes
- **internal/auth/sigv4.go**: Fixed SigV4 body hash fallback — compute SHA256(body) when `x-amz-content-sha256` absent (fixes test_malformed_xml: 403→400)
- **internal/server/middleware.go**: Added `metadataHeaderMiddleware` — rewrites `X-Amz-Meta-*` response headers to lowercase before wire output (fixes test_put_object_with_metadata and test_copy_object_with_replace_metadata)
- **internal/server/middleware.go**: Added `transferEncodingCheck` middleware — rejects non-chunked Transfer-Encoding (partially effective; Go's net/http strips `identity` at protocol level)
- **internal/errors/errors.go**: Added `ErrInvalidRequest` error (400)
- **internal/server/server.go**: Wired new middleware in chain
- Unit tests: 202/202 pass
- E2E tests: **84/86 pass** (1 known test bug, 1 Go runtime limitation with Transfer-Encoding: identity)
- Updated STATUS.md, DO_NEXT.md, WHAT_WE_DID.md — Stage 9a complete

## Session 12 — Stage 9a: Core Integration Testing (2026-02-23)

### What was implemented:

- **internal/errors/errors.go** (added ErrKeyTooLongError):
  - Added `ErrKeyTooLongError` pre-defined S3 error: Code `"KeyTooLongError"`, Message `"Your key is too long"`, HTTPStatus 400
  - Total: 23 pre-defined S3 error codes

- **internal/handlers/object.go** (key length validation):
  - Added key length check in `PutObject`: keys > 1024 bytes return 400 KeyTooLongError
  - Inserted after empty key check, before bucket existence verification

- **internal/server/integration_test.go** (new file — 37 integration tests):
  - In-process integration tests that start a full BleepStore server per test (random free port, temp dirs, SQLite DB, seeded credentials)
  - Custom SigV4 signing implementation for test requests (`intSha256Hex`, `intHmacSHA256`, `intURIEncode`, `intCanonicalQueryString`)
  - `integrationServer` struct with `signedRequest`, `doSigned`, `doSignedWithHeaders` helpers
  - Tests cover all E2E scenarios:
    - `TestIntegrationHealth`: Health endpoint returns 200
    - `TestIntegrationBucketCRUD`: Create, head, list, location, delete, verify deleted
    - `TestIntegrationPutGetObject`: Put with Content-Type, verify ETag (MD5), get, head (check headers), delete, verify gone
    - `TestIntegrationKeyTooLong`: 1025-byte key returns 400 KeyTooLongError
    - `TestIntegrationConditionalRequests`: If-Match (success/failure), If-None-Match (304)
    - `TestIntegrationRangeRequest`: bytes=0-4 returns 206 with correct slice
    - `TestIntegrationMultipartUpload`: Full lifecycle (create, 2 parts, complete, verify assembled, delete)
    - `TestIntegrationXMLNamespaces`: Success XML has xmlns, error XML does not, Content-Type is application/xml
    - `TestIntegrationCommonHeaders`: x-amz-request-id, Server: BleepStore, Date present
    - `TestIntegrationErrorResponses`: NoSuchBucket error with request ID
    - `TestIntegrationCopyObject`: Copy default (COPY directive), copy with REPLACE directive (verify Content-Type, metadata replaced), copy nonexistent source (404)
    - `TestIntegrationDeleteObjects`: Batch delete 3 objects, verify deletion, quiet mode test
    - `TestIntegrationListObjectsV2WithPrefixDelimiter`: 8 objects, prefix filter, delimiter grouping (CommonPrefixes), pagination (max-keys=2, IsTruncated, NextContinuationToken), start-after, StorageClass in listing
    - `TestIntegrationListObjectsV1`: V1 list, pagination with marker
    - `TestIntegrationObjectUserMetadata`: PUT with x-amz-meta-*, HEAD and GET return metadata
    - `TestIntegrationObjectOverwrite`: Put v1, put v2, get returns v2
    - `TestIntegrationEmptyObject`: Zero-byte object, Content-Length=0
    - `TestIntegrationSlashInKey`: Keys with / work correctly
    - `TestIntegrationDeleteNonexistentObject`: Returns 204 (idempotent)
    - `TestIntegrationBucketNotEmpty`: Delete non-empty bucket returns 409 BucketNotEmpty
    - `TestIntegrationBucketAlreadyExists`: Create same bucket twice returns 200 (us-east-1 behavior)
    - `TestIntegrationInvalidBucketName`: Uppercase and too-short names return 400
    - `TestIntegrationGetBucketLocation`: Returns LocationConstraint, 404 for nonexistent
    - `TestIntegrationObjectACL`: Default ACL (FULL_CONTROL), put public-read, verify READ, ACL on nonexistent object (404), put with canned ACL
    - `TestIntegrationBucketACL`: Default ACL (FULL_CONTROL), put public-read
    - `TestIntegrationListBucketsOwner`: ListBuckets has Owner with ID
    - `TestIntegrationRangeSuffix`: bytes=-5 returns last 5 bytes, bytes=100-200 on 16-byte object returns 416
    - `TestIntegrationMultipartAbort`: Create, abort (204), abort nonexistent (404 NoSuchUpload)
    - `TestIntegrationMultipartListUploads`: Create 2 uploads, list, verify both present
    - `TestIntegrationMultipartInvalidPartOrder`: Parts in wrong order return 400 InvalidPartOrder
    - `TestIntegrationMultipartWrongETag`: Wrong ETag returns 400 InvalidPart
    - `TestIntegrationNoSuchKeyError`: GET nonexistent key returns 404 NoSuchKey
    - `TestIntegrationMalformedXML`: Malformed XML to DeleteObjects returns 400 MalformedXML
    - `TestIntegrationSignatureMismatch`: Wrong secret key returns 403 SignatureDoesNotMatch
    - `TestIntegrationPresignedGetURL`: Build presigned URL manually, GET without auth header, verify 200 + correct body
    - `TestIntegrationListObjectsContentFields`: Verify Key, LastModified, ETag, Size, StorageClass in listing
    - `TestIntegrationListObjectsEmptyBucket`: KeyCount=0, no Contents element

### Key decisions:
- Used in-process integration testing (start server within `go test`) because the sandbox environment blocks server process startup. The server starts on a random free port per test.
- Named types `integrationServer` and `newIntegrationServer` to avoid conflicts with existing `testServer` in `server_test.go`.
- Custom SigV4 signing helpers prefixed with `int` (intCanonicalQueryString, intSha256Hex, etc.) to avoid conflicting with the unexported auth package functions.
- Presigned URL test constructs the URL manually with the same SigV4 signing process, then fetches via plain HTTP GET (no Authorization header).
- Each test creates its own isolated server instance with fresh temp dirs and SQLite database.

### Test count: 202 total tests passing (165 unit + 37 integration)

### Issues found and fixed:
- **Missing KeyTooLong validation**: E2E test `test_key_too_long` expects keys > 1024 bytes to be rejected. Added `ErrKeyTooLongError` and validation check in PutObject.
- **No other compliance issues found**: After thorough code review of all handlers against E2E test expectations, the existing implementation was compliant for: ETag quoting, Content-Type (application/xml), HEAD responses, 204 responses, Accept-Ranges header, StorageClass in listings, XML namespaces, conditional requests, range requests, presigned URLs, multipart uploads, user metadata, bucket validation, ACLs.

### Key decisions:
- ETag comparison normalizes by stripping surrounding quotes — handles both `"abc"` and `abc` formats from clients
- Part size validation: all non-last parts must be >= 5 MiB (5,242,880 bytes), matching AWS S3 behavior
- Composite ETag computed in `computeCompositeETag` helper (separate from storage `AssembleParts`) for testability, but the handler uses the storage-computed ETag for the final object record
- Location format: `/{bucket}/{key}` (relative path, not full URL — matches other BleepStore implementations)
- Upload metadata (Content-Type, ACL, user metadata, content headers) is propagated to the final object record
- Part files cleaned up by `storage.AssembleParts` after successful concatenation
- No new dependencies — all stdlib: `crypto/md5`, `encoding/hex`, `encoding/xml`, `fmt`
