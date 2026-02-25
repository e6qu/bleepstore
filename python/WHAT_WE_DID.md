# BleepStore Python -- What We Did

## Cross-Language Storage Identity Normalization (2026-02-25)

Normalized multipart temp directory from `.parts` to `.multipart` to match Go/Rust/Zig convention.

### Changes:
- **storage/local.py**: Changed all references from `.parts` to `.multipart` (put_part, put_part_stream, assemble_parts, delete_parts, delete_upload_parts, _clean_temp_files, docstrings)
- **tests/test_multipart.py**: Updated `.parts` → `.multipart` in disk path assertions
- **tests/test_multipart_complete.py**: Updated `.parts` → `.multipart` in disk path assertions

### Verification:
- 619/619 unit tests pass
- All E2E tests pass (86/86)

## Session 19 -- Stage 15: Performance Optimization & Production Readiness (2026-02-24)

Skipped Stages 12–14 (Raft clustering) — Stage 15 is independent of clustering, targets single-node performance.

### Changes by file:
- **server.py**: Fixed SigV4 authenticator cache bug (create once in lifespan, reuse via app.state); added per-request structured logging (method, path, status, duration_ms, request_id) with time import
- **storage/backend.py**: Added `put_stream()` and `put_part_stream()` to StorageBackend protocol
- **storage/local.py**: Implemented streaming `put_stream()`, `put_part_stream()`, streaming `copy_object()` with incremental MD5; added ENOSPC error logging; optimized `_clean_temp_files()` to skip hidden dirs
- **storage/aws.py**, **storage/gcp.py**, **storage/azure.py**: Added `put_stream()` and `put_part_stream()` (collect-and-delegate pattern for gateway backends)
- **handlers/object.py**: PutObject now uses streaming write when body not pre-consumed; added Content-Length size limit check against max_object_size
- **handlers/multipart.py**: UploadPart now uses streaming write when body not pre-consumed
- **metadata/sqlite.py**: Batch delete reduced from 2N to 2 queries using IN clause; list_objects truncation detection from over-fetch (no separate SELECT); schema check skips DDL on warm starts; added OperationalError logging
- **config.py**: Added log_level, log_format, shutdown_timeout, max_object_size to ServerConfig
- **cli.py**: Added --log-level, --log-format, --shutdown-timeout CLI args; uses configure_logging(); passes timeout_graceful_shutdown and timeout_keep_alive to uvicorn
- **logging_config.py**: NEW — JSONFormatter class and configure_logging() function

### Results: 582/582 unit tests, 86/86 E2E tests pass

## Session 18 -- Stage 11b: Azure Blob Storage Gateway Backend (2026-02-23)
- **pyproject.toml**: Added `azure-storage-blob>=12.19.0` and `azure-identity>=1.15.0` as optional `azure` dependency and to dev deps
- **src/bleepstore/storage/azure.py**: Complete rewrite from stub to full implementation (~260 lines):
  - `AzureGatewayBackend` class implementing all 12 `StorageBackend` protocol methods
  - Key mapping: `{prefix}{bucket}/{key}` for blobs (all data in one Azure container)
  - **No temporary part objects** — uses Azure Block Blob primitives directly:
    - `put_part()` → `stage_block()` on the final blob with base64-encoded block IDs
    - `assemble_parts()` → `commit_block_list()` to finalize, then download to compute MD5
    - `delete_parts()` → no-op (uncommitted blocks auto-expire in 7 days)
  - Block IDs: `base64(upload_id:part_number)` — includes upload_id to avoid collisions between concurrent multipart uploads
  - `init()`: creates `ContainerClient` with `DefaultAzureCredential`, verifies container exists
  - `close()`: closes both container client and credential
  - `put()`: `upload_blob(overwrite=True)` with locally-computed MD5
  - `get()`: `download_blob()` + `readall()`, `ResourceNotFoundError` → `FileNotFoundError`
  - `get_stream()`: `download_blob(offset, length)` + `chunks()` async iterator
  - `delete()`: idempotent — catches `ResourceNotFoundError` silently
  - `exists()`: native `blob_client.exists()` (efficient HEAD request)
  - `copy_object()`: `start_copy_from_url()` for server-side copy, download to compute MD5
- **src/bleepstore/server.py**: Updated `_create_storage_backend()`:
  - Added `elif backend == "azure":` branch with conditional import + `ImportError` guard
  - Validates `azure_container` is set; helpful error message for missing azure-storage-blob
  - Updated docstring to reflect all 4 backends (local, AWS, GCP, Azure)
- **tests/test_storage_azure.py** (new): 40 tests across 13 test classes:
  - `TestKeyMapping` (3): blob_name with/without prefix, nested key
  - `TestBlockId` (4): format, padding, base64 validity, upload_id collision avoidance
  - `TestInit` (5): container verification, missing container raises ValueError, access error, close, close noop
  - `TestPut` (3): returns MD5, correct blob name with prefix, empty data
  - `TestGet` (3): returns bytes, ResourceNotFoundError→FileNotFoundError, other errors propagate
  - `TestGetStream` (4): yields chunks, offset kwargs, offset+length kwargs, not found
  - `TestDelete` (3): calls delete_blob, idempotent on ResourceNotFoundError, other errors propagate
  - `TestExists` (3): true, false, correct blob name with prefix
  - `TestPutPart` (3): returns MD5, stages block with correct block_id, uses final blob name
  - `TestAssembleParts` (3): commits block list, single part, downloads to compute MD5
  - `TestDeleteParts` (1): confirms no-op (no blob operations called)
  - `TestCopyObject` (3): server-side copy, correct source URL, uses destination blob name
  - `TestServerFactory` (2): requires container, creates instance with correct config
- **Results**: 582 unit tests pass, 86/86 E2E tests pass

## Session 17 -- Stage 11a: GCP Cloud Storage Gateway Backend (2026-02-23)
- **pyproject.toml**: Added `gcloud-aio-storage>=9.0.0` as optional `gcp` dependency and to dev deps
- **src/bleepstore/storage/gcp.py**: Complete rewrite from stub to full implementation (~280 lines):
  - `GCPGatewayBackend` class implementing all 12 `StorageBackend` protocol methods
  - Key mapping: `{prefix}{bucket}/{key}` for objects, `{prefix}.parts/{upload_id}/{part_number}` for parts
  - `init()`: creates `Storage()` client, verifies upstream bucket via `list_objects(maxResults=1)`
  - `close()`: `await client.close()`
  - `put()`: `upload()` with locally-computed MD5 for consistent ETags
  - `get()`: `download()` with 404 → `FileNotFoundError` mapping via `_is_not_found()` helper
  - `get_stream()`: `download_stream()` with Range header support, 64KB chunks
  - `delete()`: idempotent — catches 404 silently (GCS errors on delete of non-existent unlike S3)
  - `exists()`: `download()` with `Range: bytes=0-0` header to avoid full download, 404 → False
  - `put_part()`: stores parts as temporary GCS objects at `.parts/` prefix
  - `assemble_parts()`: GCS `compose()` with chaining for >32 parts (batches of 32, compose intermediates, repeat). Downloads final object to compute MD5.
  - `_chain_compose()`: helper for recursive compose batching with intermediate cleanup
  - `delete_parts()`: `list_objects(prefix=...)` + individual `delete()` calls with 404 tolerance
  - `copy_object()`: GCS server-side `copy()` + download to compute MD5
  - `_is_not_found()`: module-level helper checking `exc.status == 404` or "404"/"not found" in message
- **src/bleepstore/server.py**: Updated `_create_storage_backend()`:
  - Added `elif backend == "gcp":` branch with conditional import + `ImportError` guard
  - Validates `gcp_bucket` is set; helpful error message for missing gcloud-aio-storage
  - Updated docstring to reflect GCP support
- **tests/test_storage_gcp.py** (new): 43 tests across 14 test classes:
  - `TestKeyMapping` (5): gcs_name and part_name with/without prefix
  - `TestInit` (4): bucket verification, missing bucket raises ValueError, close closes client, close noop
  - `TestPut` (3): returns MD5, with prefix, empty data
  - `TestGet` (3): returns bytes, 404→FileNotFoundError, other errors propagate
  - `TestGetStream` (4): yields chunks, offset range header, offset+length range header, not found
  - `TestDelete` (3): calls delete, idempotent on 404, other errors propagate
  - `TestExists` (4): true, false on 404, uses Range header, other errors propagate
  - `TestCopyObject` (2): server-side copy with MD5 computation, with prefix
  - `TestPutPart` (1): returns MD5, stores at .parts/ name
  - `TestAssembleParts` (5): single compose, single part, chain compose >32 parts, 64 parts, with prefix
  - `TestDeleteParts` (3): deletes all, empty no-op, ignores 404
  - `TestIsNotFound` (4): status 404, status 403, plain exception, message with 404
  - `TestServerFactory` (2): requires bucket, creates instance with correct config
- **Test results**: 542/542 unit tests pass, 86/86 E2E pass
- **State files updated**: STATUS.md, DO_NEXT.md, WHAT_WE_DID.md, PLAN.md

## Session 16 -- Stage 10: AWS S3 Gateway Backend (2026-02-23)
- **pyproject.toml**: Added `aiobotocore>=2.7.0` as optional `aws` dependency and to dev deps
- **src/bleepstore/storage/aws.py**: Complete rewrite from stub to full implementation (~280 lines):
  - `AWSGatewayBackend` class implementing all 12 `StorageBackend` protocol methods
  - Key mapping: `{prefix}{bucket}/{key}` for objects, `{prefix}.parts/{upload_id}/{part_number}` for parts
  - `init()`: creates aiobotocore session+client, verifies upstream bucket via `head_bucket()`
  - `close()`: exits client context manager
  - `put()`: `put_object()` with locally-computed MD5 for consistent ETags
  - `get()`: `get_object()` with `NoSuchKey`/404 → `FileNotFoundError` mapping
  - `get_stream()`: streaming `get_object()` with Range header support, 64KB chunks
  - `delete()`: idempotent `delete_object()`
  - `exists()`: `head_object()` with 404 → False mapping
  - `put_part()`: stores parts as temporary S3 objects at `.parts/` prefix
  - `assemble_parts()`: single-part uses `copy_object`; multi-part creates AWS native multipart upload with `upload_part_copy` (server-side copy). Falls back to download+re-upload on `EntityTooSmall`. Aborts on failure.
  - `delete_parts()`: paginated `list_objects_v2` + batch `delete_objects` (1000/call)
  - `copy_object()`: AWS server-side `copy_object()` with ETag quote stripping
- **src/bleepstore/server.py**: Updated `_create_storage_backend()`:
  - Changed return type from `LocalStorageBackend` to `StorageBackend` (Protocol)
  - Added `elif backend == "aws":` branch with conditional import + `ImportError` guard
  - Validates `aws_bucket` is set; helpful error message for missing aiobotocore
  - Added `StorageBackend` import from `bleepstore.storage.backend`
- **tests/test_storage_aws.py** (new): 35 tests across 11 test classes:
  - `TestKeyMapping` (5): s3_key and part_key with/without prefix
  - `TestInit` (4): bucket verification, missing bucket raises ValueError, close exits context, close noop
  - `TestPut` (3): returns MD5, with prefix, empty data
  - `TestGet` (4): returns bytes, NoSuchKey→FileNotFoundError, 404→FileNotFoundError, other errors propagate
  - `TestGetStream` (4): yields chunks, offset range header, offset+length range header, not found
  - `TestDelete` (1): calls delete_object
  - `TestExists` (4): true, false on 404, false on NoSuchKey, other errors propagate
  - `TestCopyObject` (1): server-side copy with ETag stripping
  - `TestPutPart` (1): returns MD5, stores at .parts/ key
  - `TestAssembleParts` (4): single-part copy, multi-part multipart upload, EntityTooSmall fallback, abort on failure
  - `TestDeleteParts` (2): batch deletes, empty no-op
  - `TestServerFactory` (2): requires bucket, creates instance with correct config
- **Test results**: 499/499 unit tests pass, 85/86 E2E pass (same known test bug)
- **State files updated**: STATUS.md, DO_NEXT.md, WHAT_WE_DID.md, PLAN.md

## Session 15 -- Stage 9b: External Test Suite Code Review (2026-02-23)
- Performed thorough code review against Ceph s3-tests, MinIO Mint, and Snowflake s3compat expectations
- Verified XML namespaces, error formats, header values all match S3 spec
- Verified bucket naming validation covers all AWS rules
- Verified SigV4 auth handles all signing variants
- **Result**: No critical compliance gaps found — Python/FastAPI implementation is solid
- Updated STATUS.md, DO_NEXT.md, WHAT_WE_DID.md — Stage 9b complete

## Session 14 -- Stage 9a: E2E Verification & Final Fix (2026-02-23)
- Ran full E2E test suite: 84/86 initially, 85/86 after fix
- **src/bleepstore/auth.py**: Fixed SigV4 body hash fallback — when `x-amz-content-sha256` header is absent (e.g. non-S3 SigV4 clients like `botocore.auth.SigV4Auth`), compute `SHA256(body)` instead of using `UNSIGNED_PAYLOAD`. This matches what the client uses to sign, fixing `test_malformed_xml` (was 403, now 400).
- Created `python/data/` directory (missing, caused server startup failure)
- Unit tests: 464/464 pass (no regressions)
- E2E tests: **85/86 pass** (only `test_invalid_access_key` fails — known test bug with hardcoded port 9000)
- Updated STATUS.md, DO_NEXT.md, WHAT_WE_DID.md — Stage 9a complete

## Session 13 -- Stage 9a: Core Integration Testing - Compliance Fixes (2026-02-23)
- **src/bleepstore/handlers/object.py**: Applied 2 proactive compliance fixes based on deep E2E test analysis:
  - **Fix 1: Last-Modified header format**: Added `_iso_to_http_date(iso_str)` function that converts ISO 8601 timestamps (e.g., `2024-01-01T00:00:00.000Z`) from the metadata store to RFC 1123 HTTP date format (e.g., `Wed, 01 Jan 2024 00:00:00 GMT`) for `Last-Modified` response headers. Applied in 3 locations:
    - `_build_object_headers()`: converts `last_modified` metadata field to HTTP date for GetObject/HeadObject responses
    - `get_object()` 304 Not Modified response: uses `_iso_to_http_date()` for the `Last-Modified` header
    - `head_object()` 304 Not Modified response: same conversion
    - Without this fix, boto3 would fail to parse the `LastModified` field since it expects HTTP date format in response headers, not ISO 8601
  - **Fix 2: Canned ACL on PutObject**: Added `x-amz-acl` header support in `put_object()` handler. When the `x-amz-acl` header is present (e.g., `public-read`), the handler:
    - Derives owner_id from config access key
    - Calls `parse_canned_acl()` to build the ACL structure
    - Serializes to JSON via `acl_to_json()`
    - Passes `acl=acl_json` to `metadata.put_object()` instead of the default `{}`
    - Required for `test_put_object_with_canned_acl` E2E test in `tests/e2e/test_acl.py`
- **Comprehensive E2E test analysis**: Reviewed all 86 E2E tests across 6 files against full source code to identify compliance issues:
  - `test_buckets.py` (16 tests): All bucket operations should pass
  - `test_objects.py` (34 tests): All object operations should pass with Last-Modified fix
  - `test_multipart.py` (14 tests): All multipart operations should pass
  - `test_presigned.py` (6 tests): All presigned URL operations should pass
  - `test_acl.py` (4 tests): All ACL operations should pass with canned ACL fix
  - `test_errors.py` (12 tests): 11/12 should pass; `test_invalid_access_key` has known test bug (hardcoded port 9000)
- **Known test bug documented**: `test_invalid_access_key` in `tests/e2e/test_errors.py` line 82 uses hardcoded `endpoint_url="http://localhost:9000"` instead of `BLEEPSTORE_ENDPOINT` env var. This test will fail when running against port 9010. This is a shared test suite issue; per AGENTS.md rules, do NOT modify the test file.
- **E2E test count correction**: The actual E2E test count is 86, not 75 as stated in PLAN.md. Breakdown: test_buckets(16) + test_objects(34) + test_multipart(14) + test_presigned(6) + test_acl(4) + test_errors(12) = 86
- **Blockers**: Could not run unit tests, E2E tests, or smoke test due to Bash execution restrictions. All fixes are proactive based on code review. Manual test execution required to complete Stage 9a verification.
- **State files updated**: STATUS.md, DO_NEXT.md, WHAT_WE_DID.md

## Session 12 -- Stage 8: Multipart Upload - Completion (2026-02-23)
- **src/bleepstore/handlers/multipart.py**: Added 3 new methods to MultipartHandler:
  - `complete_multipart_upload()`: Parses CompleteMultipartUpload XML body, validates ascending part order, validates ETags match stored parts, validates minimum part sizes (>= 5 MiB except last), assembles parts atomically via `storage.assemble_parts()`, computes composite ETag `MD5(concat(binary_md5s))-{part_count}`, commits metadata atomically (insert object, delete upload+parts via `metadata.complete_multipart_upload()`), cleans up part files from storage, returns CompleteMultipartUploadResult XML
  - `upload_part_copy()`: Reads source object via `x-amz-copy-source` header (with optional byte range via `x-amz-copy-source-range`), writes data as a multipart part using same atomic storage pattern, returns CopyPartResult XML with ETag and LastModified
  - `_compute_composite_etag()`: Static method implementing S3 composite ETag computation: concatenate binary MD5 digests of each part, MD5 the concatenation, append `-{part_count}`
- **src/bleepstore/xml_utils.py**: Implemented `render_complete_multipart_upload()` (was raising NotImplementedError), renders CompleteMultipartUploadResult with Location, Bucket, Key, ETag elements and S3 namespace
- **src/bleepstore/server.py**: Wired `POST /{bucket}/{key}?uploadId` to `multipart_handler.complete_multipart_upload()` (was raising NotImplementedS3Error). Added UploadPartCopy dispatch: when `PUT /{bucket}/{key}?partNumber&uploadId` has `x-amz-copy-source` header, dispatches to `multipart_handler.upload_part_copy()` instead of `upload_part()`
- **tests/test_multipart_complete.py**: New test file with ~31 tests across 8 test classes:
  - TestCompleteMultipartUpload: 5 tests (basic 200 XML, XML structure, namespace, NoSuchBucket, NoSuchUpload)
  - TestCompositeETag: 3 tests (single part format, multi-part computation match, part count suffix)
  - TestCompletedObject: 5 tests (GET assembled object, correct size, composite ETag on HEAD, content-type preserved, nested key)
  - TestPartCleanup: 2 tests (part files removed, upload metadata removed)
  - TestCompleteMultipartErrors: 10 tests (InvalidPartOrder, duplicate part numbers, ETag mismatch, non-existent part, EntityTooSmall, single small part OK, last part can be small, MalformedXML, empty parts list, subset of uploaded parts)
  - TestMultipartCompleteLifecycle: 2 tests (full lifecycle with disk verification, common headers)
  - TestUploadPartCopy: 4 tests (copy from existing object, copy with byte range, source not found, cross-bucket copy)
- **tests/test_server.py**: Updated 4 tests affected by route changes:
  - `test_post_object_upload_id_501` renamed to `test_post_object_upload_id_nosuchbucket` (route now goes through handler, returns 404 NoSuchBucket instead of 501)
  - 3 ErrorXMLFormat tests updated to use `POST /{bucket}` without query params (still returns 501 NotImplemented) since `POST /{bucket}/{key}?uploadId` is now wired
- **State files updated**: PLAN.md (Stage 8 checked), STATUS.md, DO_NEXT.md, WHAT_WE_DID.md
- **Test results**: 464 tests, all passing (31 new tests added)

## Session 11 -- Stage 7: Multipart Upload - Core (2026-02-23)
- **src/bleepstore/handlers/multipart.py**: Complete rewrite from stubs to full implementation of 5 multipart handlers:
  - `MultipartHandler` class with `metadata`, `storage`, `config` property shortcuts (same pattern as BucketHandler/ObjectHandler)
  - `_ensure_bucket_exists()`: shared helper that raises NoSuchBucket if bucket not found
  - `_extract_user_metadata()`: extracts `x-amz-meta-*` headers (same pattern as ObjectHandler)
  - `create_multipart_upload(request, bucket, key)`: Generates UUID upload_id via `str(uuid.uuid4())`, extracts Content-Type and other headers for the final object, extracts user metadata, derives owner info from request.state (auth middleware) or falls back to config, stores upload metadata via `metadata.create_multipart_upload()`, returns InitiateMultipartUploadResult XML
  - `upload_part(request, bucket, key)`: Extracts uploadId and partNumber from query params, validates part number is 1-10000, verifies bucket and upload exist, reads body, writes to storage via `storage.put_part()` (atomic temp-fsync-rename), computes MD5 ETag, records part metadata via `metadata.put_part()` (upsert), returns 200 with ETag header
  - `abort_multipart_upload(request, bucket, key)`: Extracts uploadId, verifies bucket and upload exist, deletes part files via `storage.delete_parts()`, deletes metadata via `metadata.abort_multipart_upload()`, returns 204
  - `list_uploads(request, bucket)`: Extracts prefix/delimiter/key-marker/upload-id-marker/max-uploads query params, queries `metadata.list_multipart_uploads()`, renders ListMultipartUploadsResult XML with Upload elements (Key, UploadId, Initiator, Owner, StorageClass, Initiated), CommonPrefixes, pagination markers
  - `list_parts(request, bucket, key)`: Extracts uploadId/part-number-marker/max-parts query params, verifies upload exists, queries `metadata.list_parts()`, renders ListPartsResult XML with Part elements (PartNumber, LastModified, ETag, Size), Initiator/Owner, pagination
- **src/bleepstore/xml_utils.py**: Implemented 3 XML rendering functions (replacing NotImplementedError stubs):
  - `render_initiate_multipart_upload(bucket, key, upload_id)`: InitiateMultipartUploadResult XML with S3 namespace, Bucket, Key, UploadId
  - `render_list_multipart_uploads(bucket, uploads, ...)`: ListMultipartUploadsResult XML with Bucket, KeyMarker, UploadIdMarker, NextKeyMarker/NextUploadIdMarker (when truncated), MaxUploads, IsTruncated, Prefix, Delimiter, Upload elements with Key/UploadId/Initiator/Owner/StorageClass/Initiated, CommonPrefixes. Added `common_prefixes` parameter.
  - `render_list_parts(bucket, key, upload_id, parts, ...)`: ListPartsResult XML with Bucket, Key, UploadId, Initiator, Owner, StorageClass, PartNumberMarker, NextPartNumberMarker (when truncated), MaxParts, IsTruncated, Part elements with PartNumber/LastModified/ETag/Size. Added `storage_class`, `owner_id`, `owner_display` parameters.
- **src/bleepstore/server.py**: Wired all multipart routes via `MultipartHandler`:
  - Imported `MultipartHandler` from `bleepstore.handlers.multipart`
  - Created `multipart_handler = MultipartHandler(app)` in `_setup_routes()`
  - `GET /{bucket}?uploads` -> `multipart_handler.list_uploads(request, bucket)` (was NotImplementedS3Error)
  - `POST /{bucket}/{key}?uploads` -> `multipart_handler.create_multipart_upload(request, bucket, key)` (was NotImplementedS3Error)
  - `PUT /{bucket}/{key}?partNumber&uploadId` -> `multipart_handler.upload_part(request, bucket, key)` (was NotImplementedS3Error)
  - `GET /{bucket}/{key}?uploadId` -> `multipart_handler.list_parts(request, bucket, key)` (was NotImplementedS3Error)
  - `DELETE /{bucket}/{key}?uploadId` -> `multipart_handler.abort_multipart_upload(request, bucket, key)` (was NotImplementedS3Error)
  - `POST /{bucket}/{key}?uploadId` -> still raises NotImplementedS3Error (CompleteMultipartUpload deferred to Stage 8)
- **tests/test_multipart.py** (new): ~48 tests across 6 test classes:
  - `mp_client` fixture: creates fresh in-memory SQLite metadata store + fresh temp storage per test (avoids state leaks between tests), swaps in on app.state, restores on teardown
  - `_create_bucket()` and `_initiate_upload()` helper functions
  - `TestCreateMultipartUpload` (7 tests): returns_200_with_xml, xml_has_bucket_and_key, upload_id_is_uuid, nosuchbucket, multiple_uploads_different_ids, has_namespace, nested_key
  - `TestUploadPart` (9 tests): returns_200_with_etag, etag_is_md5, nosuchupload, nosuchbucket, part_overwrite, multiple_parts, invalid_part_number_zero, invalid_part_number_too_large, part_stored_on_disk
  - `TestAbortMultipartUpload` (5 tests): returns_204, nosuchupload, cleans_up_parts, cleans_up_metadata, nosuchbucket
  - `TestListParts` (8 tests): returns_200_with_xml, lists_uploaded_parts, parts_have_etag_and_size, nosuchupload, has_bucket_key_upload_id, empty_parts, pagination_max_parts, has_namespace
  - `TestListMultipartUploads` (9 tests): returns_200_with_xml, empty_uploads, lists_active_uploads, upload_has_key_and_initiated, aborted_upload_not_listed, prefix_filter, nosuchbucket, has_bucket, has_namespace
  - `TestMultipartLifecycle` (3 tests): create_upload_parts_list_abort (full end-to-end lifecycle), parallel_uploads_for_same_key, common_headers_present
- **tests/test_server.py**: Updated 7 tests to reflect newly wired multipart routes:
  - `test_post_object_uploads_501` -> `test_post_object_uploads_nosuchbucket` (now returns 404 instead of 501)
  - `test_put_object_upload_part_501` -> `test_put_object_upload_part_nosuchbucket` (now returns 404 instead of 501)
  - `test_delete_object_abort_upload_501` -> `test_delete_object_abort_upload_nosuchbucket` (now returns 404 instead of 501)
  - `test_get_object_list_parts_501` -> `test_get_object_list_parts_nosuchbucket` (now returns 404 instead of 501)
  - `test_list_uploads_501` -> `test_list_uploads_returns_xml` (now returns 200 with ListMultipartUploadsResult XML)
  - `TestErrorXMLFormat` tests updated to use `POST /{bucket}/{key}?uploadId=abc` (CompleteMultipartUpload, still 501) instead of `GET /{bucket}/{key}?uploadId=abc` (ListParts, now wired)
- **Key decisions**:
  - Upload ID generated via `str(uuid.uuid4())` for guaranteed uniqueness across crashes
  - Part storage path: `{root}/.parts/{upload_id}/{part_number}` (already implemented in storage backend)
  - Part overwrite: same upload_id + part_number replaces previous via upsert in both storage and metadata
  - Part number validation: 1-10000 (S3 spec)
  - Owner info: first tries `request.state.owner_id` from auth middleware, falls back to deriving from config access key
  - Multipart handler follows same patterns as BucketHandler/ObjectHandler (property shortcuts, _ensure_bucket_exists helper)
  - All metadata store methods were already implemented in Stage 2; all storage methods were already implemented in Stage 4
  - Tests use per-test fresh metadata/storage to avoid state leaks (unlike the session-scoped client fixture used by other tests)

## Session 10 -- Stage 6: AWS Signature V4 (2026-02-23)
- **src/bleepstore/auth.py**: Complete rewrite from stub to full SigV4 implementation:
  - `SigV4Authenticator` class with `metadata` store reference and `region` config
  - `verify_request(request)`: Dispatches to header-based or presigned URL auth. Detects auth method from `Authorization` header (starts with `AWS4-HMAC-SHA256`) or `X-Amz-Algorithm` query param. Rejects ambiguous requests with both present.
  - `_verify_header_auth(request)`: Parses Authorization header, validates credential scope (date/region/service/terminator), checks clock skew (~15 min tolerance), looks up credential by access key, builds canonical request, computes expected signature, constant-time comparison via `hmac.compare_digest()`.
  - `_verify_presigned(request)`: Validates all 6 required query params (Algorithm, Credential, Date, Expires, SignedHeaders, Signature), validates algorithm, credential format, expiration (1-604800 seconds), checks URL not expired, looks up credential, reconstructs canonical request with `UNSIGNED-PAYLOAD`, verifies signature.
  - `_parse_authorization_header(header)`: Regex-based parsing of `AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=...` format. Returns dict with credential, signed_headers, signature.
  - `_build_canonical_request(method, uri, query_string, headers, signed_headers, payload_hash, is_presigned)`: Builds full canonical request string. URI-encodes path preserving slashes. Sorts query params. Lowercases/sorts/trims canonical headers. Handles presigned mode (pre-built query string, excludes X-Amz-Signature).
  - `_build_string_to_sign(timestamp, scope, canonical_request)`: Assembles `AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{sha256(canonical_request)}`.
  - `_derive_signing_key(secret_key, date, region, service, access_key)`: HMAC-SHA256 chain: `AWS4{secret}` -> date -> region -> service -> aws4_request. Caches signing keys by `(access_key, date, region, service)` to avoid 4 HMACs per request. Auto-evicts cache when >100 entries.
  - `_compute_signature(signing_key, string_to_sign)`: Final `HMAC-SHA256(signing_key, string_to_sign)` hex digest.
  - `_check_clock_skew(amz_date)`: Validates request timestamp within 900 seconds of server time.
  - Module-level utilities: `derive_signing_key()` (standalone), `_uri_encode(s, encode_slash)`, `_uri_encode_path(path)`, `_build_canonical_query_string(query_string)`, `_trim_header_value(value)`.
  - Constants: `ALGORITHM`, `KEY_PREFIX`, `SCOPE_TERMINATOR`, `SERVICE_NAME`, `UNSIGNED_PAYLOAD`, `STREAMING_PAYLOAD`, `EMPTY_SHA256`, `MAX_PRESIGNED_EXPIRES`, `CLOCK_SKEW_TOLERANCE`, `AUTH_HEADER_RE`.
- **src/bleepstore/errors.py**: Added 4 new error classes:
  - `InvalidAccessKeyId`: 403, code="InvalidAccessKeyId"
  - `AuthorizationQueryParametersError`: 400, code="AuthorizationQueryParametersError"
  - `RequestTimeTooSkewed`: 403, code="RequestTimeTooSkewed"
  - `ExpiredPresignedUrl`: 403, code="AccessDenied" with "Request has expired." message
- **src/bleepstore/server.py**: Added auth middleware:
  - Imported `SigV4Authenticator` from `bleepstore.auth`
  - Added `auth_middleware` in `_register_middleware()` -- registered after `common_headers_middleware` so it runs after common headers are set (FastAPI middleware reverse registration order)
  - Auth skip paths: `/health`, `/metrics`, `/docs`, `/docs/oauth2-redirect`, `/openapi.json`, `/redoc`
  - When `auth.enabled=False` in config, all requests pass through without auth
  - When metadata store not available (e.g. during testing), skips auth
  - On auth success: stores `access_key`, `owner_id`, `display_name` on `request.state`
  - On auth failure: catches `S3Error` in middleware (FastAPI exception handlers don't catch middleware exceptions) and returns S3 error XML directly
- **tests/conftest.py**: Updated session-scoped `config` fixture to set `auth.enabled=False` so existing tests that don't sign requests continue to pass. Auth-enabled tests use their own fixture.
- **tests/test_auth.py** (new): 58 tests across 12 test classes:
  - `TestDeriveSigningKey` (5 tests): known AWS vector, different dates/regions/services produce different keys, deterministic
  - `TestUriEncode` (7 tests): unreserved chars, space->%20, slash encoding, special chars, unicode, empty
  - `TestUriEncodePath` (5 tests): simple path, empty->/, preserves slashes, encodes special chars, root
  - `TestCanonicalQueryString` (7 tests): empty, single param, sorted, empty value, equals, multiple, uri-encoded values
  - `TestTrimHeaderValue` (3 tests): whitespace trim, space collapse, already trimmed
  - `TestParseAuthorizationHeader` (3 tests): valid parse, invalid raises, missing signature raises
  - `TestBuildCanonicalRequest` (4 tests): simple GET, PUT with body hash, query params sorted, headers sorted/lowercased
  - `TestBuildStringToSign` (2 tests): format verification, different inputs
  - `TestComputeSignature` (3 tests): 64-char hex, deterministic, different inputs
  - `TestHeaderAuthIntegration` (9 tests): signed request succeeds (ListBuckets), unsigned denied (403+AccessDenied), bad signature (403+SignatureDoesNotMatch), invalid access key (403+InvalidAccessKeyId), health/metrics/docs/openapi skip auth, PutObject with valid sig, error response is XML
  - `TestPresignedUrlIntegration` (6 tests): presigned GET succeeds (create bucket+object, then GET via presigned URL), bad signature denied, missing params (400+AuthorizationQueryParametersError), expired URL denied, invalid access key, presigned PUT succeeds
  - `TestAuthDisabled` (1 test): requests succeed without auth when disabled
  - `TestSigningKeyCache` (2 tests): cache returns same object, different dates produce different keys
  - `auth_client` fixture: temporarily enables auth on the session-scoped app, swaps in fresh metadata store with known credentials, restores original state after test
  - Helper functions: `_sign_request()` (reference SigV4 signer), `_generate_presigned_url()` (presigned URL generator), `_now_timestamp()` (current UTC in SigV4 format)
- **Key decisions**:
  - Auth middleware catches `S3Error` internally and returns XML response directly, because FastAPI exception handlers do not catch exceptions raised inside middleware (they only catch route handler exceptions)
  - Existing tests use `auth.enabled=False` in config to avoid needing to sign every test request. Only `test_auth.py` tests exercise the auth middleware with `auth.enabled=True`.
  - `auth_client` fixture temporarily mutates the session-scoped app's config to enable auth, then restores it -- avoids creating a second app (which would cause Prometheus duplicate metric registration errors)
  - Signing key cache uses a simple dict with (access_key, date, region, service) tuple key and auto-evicts when exceeding 100 entries
  - Presigned URL query string reconstruction excludes `X-Amz-Signature` during signing, per spec
  - Clock skew check uses 900 seconds (15 minutes) tolerance, matching AWS behavior
- **Test results**: 392 tests total (334 existing + 58 new), all passing, zero warnings

## Session 9 -- Stage 5b: Range, Conditional Requests & Object ACLs (2026-02-23)
- **src/bleepstore/handlers/object.py**: Major update with 3 new features:
  - **Range request support**: Added `parse_range_header(header, total)` function that parses `Range: bytes=start-end`, `bytes=-suffix`, `bytes=start-` forms. Returns `(start, end)` tuple of inclusive byte offsets. Raises `InvalidRange` for unsatisfiable ranges. Clamps end to file size - 1 when range extends past end.
  - **Conditional request support**: Added `evaluate_conditionals(request, etag, last_modified_str, is_get_or_head)` function that evaluates all 4 conditional headers in spec-compliant order:
    1. `If-Match` -> 412 on ETag mismatch (supports `*` and comma-separated lists)
    2. `If-Unmodified-Since` -> 412 if modified after date (only when If-Match absent)
    3. `If-None-Match` -> 304 on match for GET/HEAD, 412 for other methods
    4. `If-Modified-Since` -> 304 if not modified (GET/HEAD only, only when If-None-Match absent)
  - Helper functions: `_strip_etag_quotes()` handles quoted and W/ prefixed ETags, `_parse_http_date()` uses `email.utils.parsedate_to_datetime()`, `_parse_last_modified()` parses ISO 8601 timestamps
  - Updated `get_object()`: evaluates conditionals before range processing, returns 304/412 as appropriate. Handles Range header by calling `parse_range_header()`, returns 206 Partial Content with `Content-Range` header and streaming via `get_stream(offset, length)`.
  - Updated `head_object()`: evaluates conditionals, returns 304/412. No range processing (HEAD has no body).
  - Implemented `get_object_acl(request, bucket, key)`: validates bucket+key exist, deserializes stored ACL JSON, fills in default owner if ACL has no owner info, renders AccessControlPolicy XML.
  - Implemented `put_object_acl(request, bucket, key)`: validates bucket+key exist, supports canned ACL via `x-amz-acl` header, XML body with AccessControlPolicy, or defaults to private. Uses `metadata.update_object_acl()`.
  - Added `_parse_acl_xml()` and `_find_elem()` helper functions (same pattern as bucket handler) for parsing AccessControlPolicy XML bodies.
  - Imports: added `email.utils`, `hashlib`, `re`, `datetime`, `InvalidRange`, `PreconditionFailed`, and ACL helpers from `bleepstore.handlers.acl`.
- **src/bleepstore/server.py**: Wired 2 object ACL routes:
  - `handle_object_put`: Changed `?acl` dispatch from `raise NotImplementedS3Error()` to `object_handler.put_object_acl(request, bucket, key)`
  - `handle_object_get`: Changed `?acl` dispatch from `raise NotImplementedS3Error()` to `object_handler.get_object_acl(request, bucket, key)`
- **tests/test_range.py** (new): 20 unit tests for `parse_range_header()`:
  - Covers: basic range, first byte, suffix range, suffix larger than file, open-ended range, clamp end, single byte, zero-length suffix (raises), start past end (raises), start > end (raises), None input, malformed, multi-range (unsupported), small files
- **tests/test_handlers_object_advanced.py**: Added 25 integration tests across 3 new test classes:
  - `TestGetObjectRange` (9 tests): basic_206, suffix, open_ended, full_file, single_byte, invalid_416, clamps_end, has_etag, no_range_returns_200
  - `TestConditionalRequests` (9 tests): if_none_match_304, if_none_match_200, if_match_412, if_match_200, if_match_star, if_none_match_star_304, if_none_match_head_304, 304_includes_etag, if_none_match_multiple_etags
  - `TestObjectAcl` (7 tests): get_default, put_canned, get_nosuchkey, get_nosuchbucket, put_nosuchkey, put_private, has_namespace
- **tests/test_server.py**: Updated 5 tests to reflect newly wired ACL routes:
  - `test_get_object_acl_501` -> `test_get_object_acl_nosuchbucket` (now returns 404 instead of 501)
  - `test_put_object_acl_501` -> `test_put_object_acl_nosuchbucket` (now returns 404 instead of 501)
  - `test_error_xml_structure`, `test_error_xml_has_request_id`, `test_error_content_type_is_xml`: changed test route from `?acl` (now wired) to `?uploadId=abc` (still 501)
- **Key decisions**:
  - Range parsing uses a compiled regex for `bytes=(\d*)-(\d*)$` matching with explicit handling of the 3 forms
  - Conditional request evaluation follows HTTP/1.1 spec exactly: If-Match takes priority, If-Modified-Since is only evaluated when If-None-Match is absent
  - ETag comparison strips quotes and optional `W/` prefix before comparing
  - 304 Not Modified responses include only ETag and Last-Modified headers (not full object headers)
  - Object ACLs reuse the same ACL infrastructure (build_default_acl, parse_canned_acl, render_acl_xml, acl_to_json/acl_from_json) established in Stage 3 for bucket ACLs
  - Object ACL defaults to private FULL_CONTROL for the owner when no ACL is stored
  - Owner ID derived from SHA-256 of access key (same as bucket handler pattern)
- **Test results**: 334 tests total (289 existing + 45 new), all passing, zero warnings

## Session 8 -- Stage 5a: List, Copy & Batch Delete (2026-02-23)
- **src/bleepstore/handlers/object.py**: Implemented 4 new operations, replacing stubs:
  - `copy_object(request, bucket, key)`: Parses `x-amz-copy-source` header (URL-decodes, strips leading slash, splits into src_bucket/src_key). Validates source and destination buckets/keys exist. Copies data via `storage.copy_object()`. Supports `x-amz-metadata-directive` header: COPY (default) preserves source metadata, REPLACE uses metadata from the request. Commits new metadata to SQLite. Returns CopyObjectResult XML with ETag and LastModified.
  - `delete_objects(request, bucket)`: Parses `<Delete>` XML body using `xml.etree.ElementTree`. Handles XML namespace automatically. Supports quiet mode (`<Quiet>true</Quiet>` omits `<Deleted>` elements). Iterates over `<Object><Key>` elements, deletes each from storage (logging failures) and metadata. Returns `<DeleteResult>` XML with `<Deleted>` and `<Error>` elements.
  - `list_objects_v2(request, bucket)`: Extracts query params: prefix, delimiter, max-keys, continuation-token, start-after, encoding-type. Validates max-keys via `validate_max_keys()`. Delegates to `metadata.list_objects()` which handles prefix/delimiter/pagination. Returns ListBucketResult XML with Name, Prefix, Delimiter, MaxKeys, KeyCount, IsTruncated, ContinuationToken, NextContinuationToken, StartAfter, Contents (Key, LastModified, ETag, Size, StorageClass), CommonPrefixes.
  - `list_objects_v1(request, bucket)`: Extracts query params: prefix, delimiter, max-keys, marker. Returns ListBucketResult XML with Marker, NextMarker, and marker-based pagination.
  - `list_objects(request, bucket)`: Dispatch method that routes to v2 when `list-type=2` query param is present, otherwise v1.
  - Removed duplicate old `delete_objects` stub that was shadowing the new implementation (Python method resolution uses last definition in class body).
- **src/bleepstore/xml_utils.py**: Implemented 4 XML rendering functions (replacing `NotImplementedError` stubs):
  - `render_list_objects_v2()`: Full ListBucketResult XML with S3 namespace, all required elements. Added `start_after` parameter to signature.
  - `render_list_objects_v1()`: ListBucketResult XML with Marker/NextMarker.
  - `render_copy_object_result()`: CopyObjectResult XML with ETag and LastModified.
  - `render_delete_result()`: DeleteResult XML with Deleted (Key, optional VersionId) and Error (Key, Code, Message) elements.
- **src/bleepstore/server.py**: Wired 3 route dispatches:
  - `handle_bucket_get`: ListObjects dispatch from `raise NotImplementedS3Error()` to `object_handler.list_objects(request, bucket)`
  - `handle_bucket_post`: DeleteObjects dispatch from `raise NotImplementedS3Error()` to `object_handler.delete_objects(request, bucket)`
  - `handle_object_put`: Updated CopyObject call to pass `bucket` and `key` args: `object_handler.copy_object(request, bucket, key)`
- **tests/test_handlers_object_advanced.py** (new): 29 integration tests across 5 test classes:
  - `TestCopyObject` (7 tests): same_bucket, cross_bucket, preserves_metadata_by_default, replace_metadata, source_not_found, url_decoded_source, no_leading_slash
  - `TestDeleteObjects` (6 tests): basic, quiet_mode, nonexistent_keys, no_such_bucket, malformed_xml, with_namespace
  - `TestListObjectsV2` (9 tests): empty_bucket, with_objects, prefix, delimiter, max_keys, pagination, start_after, has_storage_class, nosuchbucket
  - `TestListObjectsV1` (5 tests): basic, marker, has_marker_in_xml, delimiter, pagination_with_max_keys
  - `TestListObjectsDispatch` (2 tests): default_dispatches_to_v1, list_type_2_dispatches_to_v2
- **tests/test_server.py**: Updated 2 tests to reflect newly wired routes:
  - `test_post_bucket_delete_501` -> `test_post_bucket_delete_nosuchbucket` (now returns 404 instead of 501)
  - `test_list_objects_501` -> `test_list_objects_returns_xml` (now returns 200 with ListBucketResult XML)
- **Key decisions**:
  - Used `xml.etree.ElementTree` for parsing DeleteObjects XML body (not xmltodict) for lighter dependency footprint
  - XML namespace handling: detect namespace from root element tag, prepend to all element lookups
  - CopyObject metadata directive defaults to COPY when header is absent
  - CopyObject source parsing: URL-decode first, then strip leading slash, split on first `/`
  - ListObjects v2 uses `continuation_token or start_after` as the start-after key for metadata.list_objects()
  - ListObjects v1 uses `marker` directly
  - All XML rendering uses manual string building (consistent with existing xml_utils.py pattern)
  - DeleteObjects in quiet mode: still deletes objects, just omits `<Deleted>` elements from response
- **Test results**: 289 tests total (260 existing + 29 new), all passing, zero warnings

## Session 7 -- Stage 4: Basic Object CRUD (2026-02-23)
- **src/bleepstore/storage/local.py**: Full implementation of `LocalStorageBackend`:
  - `init()`: creates root directory, cleans orphan `.tmp.*` files on startup (crash-only recovery)
  - `put()`: atomic temp-fsync-rename pattern using `os.open()`/`os.write()`/`os.fsync()`/`Path.rename()`. Computes MD5 hex digest. Cleans up temp file on failure.
  - `get()`: reads and returns file bytes via `Path.read_bytes()`
  - `get_stream()`: async generator yielding 64 KB chunks with offset/length support. Uses `f.seek()` for offset, tracks remaining bytes for length.
  - `delete()`: removes file (idempotent via `FileNotFoundError` catch), cleans empty parent directories up to bucket dir
  - `exists()`: checks `Path.is_file()`
  - `copy_object()`: read-then-write with atomic write
  - `put_part()`, `assemble_parts()`, `delete_parts()`: multipart storage operations (for future stages)
  - `_object_path()`: helper to build `{root}/{bucket}/{key}` path
  - `_clean_temp_files()`: walks root dir on startup, removes orphan temp files
- **src/bleepstore/handlers/object.py**: Full implementation of 4 basic object handlers:
  - `put_object(request, bucket, key)`: validates bucket exists + key length, reads body, extracts Content-Type (default `application/octet-stream`), extracts optional content headers (encoding, language, disposition, cache-control, expires), extracts `x-amz-meta-*` user metadata, writes to storage atomically, quotes ETag as `'"' + md5hex + '"'`, commits metadata to SQLite, returns 200 with ETag header
  - `get_object(request, bucket, key)`: validates bucket + key exist, builds response headers via `_build_object_headers()`, returns `StreamingResponse` streaming from `storage.get_stream()`
  - `head_object(request, bucket, key)`: same as GetObject minus body, returns 200 with metadata headers only
  - `delete_object(request, bucket, key)`: validates bucket exists, deletes from storage (logs warning on failure), deletes metadata, always returns 204 (idempotent)
  - `_ensure_bucket_exists()`: shared helper that raises `NoSuchBucket` if bucket not found
  - `_extract_user_metadata()`: extracts `x-amz-meta-*` headers, strips prefix, returns dict
  - `_build_object_headers()`: builds response headers dict from object metadata (ETag, Last-Modified, Content-Length, Content-Type, Accept-Ranges, optional content headers, user metadata as `x-amz-meta-*`)
  - Remaining methods (list, copy, batch delete, ACL) still raise `NotImplementedS3Error`
- **src/bleepstore/server.py**: Major update:
  - Added `_create_storage_backend()` factory function (currently only `local` backend)
  - Updated `lifespan` hook to initialize storage backend alongside metadata store, and close both on shutdown
  - Created `ObjectHandler` instance alongside `BucketHandler` in `_setup_routes()`
  - Wired object CRUD routes: PUT `/{bucket}/{key}` -> `object_handler.put_object()`, GET `/{bucket}/{key}` -> `object_handler.get_object()`, HEAD `/{bucket}/{key}` -> `object_handler.head_object()`, DELETE `/{bucket}/{key}` -> `object_handler.delete_object()`
  - Updated PUT `/{bucket}/{key}` to dispatch to `copy_object` when `x-amz-copy-source` header present (still returns 501)
  - Remaining multipart/list/ACL routes still raise `NotImplementedS3Error`
- **tests/conftest.py**: Updated `client` fixture to also initialize `LocalStorageBackend` on `app.state.storage` (using `tmp_path`), since lifespan doesn't auto-run with ASGITransport
- **tests/test_handlers_bucket.py**: Updated `bucket_client` fixture to also create and initialize a fresh `LocalStorageBackend` per test (prevents test failures from missing `app.state.storage`)
- **tests/test_server.py**: Updated to reflect Stage 4 changes:
  - Object CRUD routes (PUT/GET/HEAD/DELETE `/{bucket}/{key}`) no longer return 501 -- they return 404 NoSuchBucket since the test buckets don't exist. Updated 4 tests accordingly.
  - Added `TestObjectRoutesWired` class (4 tests): verifies PUT+GET round-trip, HEAD returns metadata with no body, DELETE returns 204, GET non-existent key returns 404
  - Updated `TestErrorXMLFormat` to use still-501 routes (e.g., `?acl`) instead of now-implemented object CRUD routes
- **tests/test_storage_local.py** (new): 23 unit tests across 5 test classes:
  - `TestInit` (3 tests): creates_root_directory, idempotent_init, cleans_temp_files
  - `TestPutAndGet` (8 tests): round_trip, returns_md5, creates_parent_directories, overwrites_existing, empty_bytes, large_data, get_nonexistent_raises, atomic_write_creates_final_file
  - `TestGetStream` (5 tests): stream_full_file, with_offset, with_length, with_offset_and_length, large_file (multi-chunk)
  - `TestDelete` (4 tests): delete_existing, nonexistent_is_idempotent, cleans_empty_parents, does_not_remove_nonempty_parents
  - `TestExists` (3 tests): exists_true, exists_false, exists_after_delete
- **tests/test_handlers_object.py** (new): 39 integration tests across 6 test classes:
  - `TestPutObject` (10 tests): success, etag_is_quoted_md5, empty_body, nosuchbucket, overwrite, nested_key, preserves_content_type, default_content_type, user_metadata, common_headers
  - `TestGetObject` (10 tests): returns_body, has_etag, has_content_length, has_last_modified, has_accept_ranges, nosuchkey, nosuchbucket, empty, with_content_type, user_metadata
  - `TestHeadObject` (9 tests): returns_200, no_body, has_etag, has_content_length, has_content_type, has_accept_ranges, nosuchkey, nosuchbucket, user_metadata
  - `TestDeleteObject` (5 tests): returns_204, removes_from_get, nonexistent_returns_204, nosuchbucket, twice_idempotent
  - `TestObjectRoundTrip` (4 tests): put_head_get_delete_round_trip, multiple_objects_in_bucket, objects_in_different_buckets, delete_one_does_not_affect_others
- **Key decisions**:
  - Storage backend uses low-level `os.open()`/`os.write()`/`os.fsync()` for atomic writes instead of higher-level Python file API, ensuring data is fsync'd to disk before rename
  - ETag is always quoted (`'"' + md5hex + '"'`) per S3 convention
  - Content-Type defaults to `application/octet-stream` when not provided
  - DeleteObject always returns 204 even for non-existent keys (idempotent)
  - Bucket existence is verified before all object operations (NoSuchBucket)
  - GetObject uses `StreamingResponse` for efficient large file handling
  - HeadObject returns same headers as GetObject but with empty body
  - User metadata round-trips through JSON serialization in SQLite
  - Storage backend factory pattern in server.py allows future backends (AWS, GCP, Azure)
  - Each test fixture creates a fresh metadata store and storage backend to avoid state leaks
- **Test results**: 260 tests total (195 existing + 65 new), all passing, zero warnings

## Session 6 -- Stage 3: Bucket CRUD (2026-02-23)
- **src/bleepstore/handlers/acl.py** (new): ACL helper module with 6 functions:
  - `build_default_acl(owner_id, owner_display)` -- creates private ACL with FULL_CONTROL grant
  - `parse_canned_acl(acl_name, owner_id, owner_display)` -- supports 4 canned ACLs: private, public-read, public-read-write, authenticated-read
  - `acl_to_json(acl)` / `acl_from_json(acl_json)` -- serialize/deserialize ACL dicts to/from JSON
  - `render_acl_xml(acl)` -- renders S3-compatible AccessControlPolicy XML with proper namespaces (xsi:type for Grantee, xmlns for root)
  - Supports CanonicalUser and Group grantee types, Group URIs for AllUsers and AuthenticatedUsers
- **src/bleepstore/xml_utils.py**: Implemented 2 new XML rendering functions:
  - `render_list_buckets(owner_id, owner_display_name, buckets)` -- renders ListAllMyBucketsResult XML with Owner, Buckets, per-Bucket Name+CreationDate, proper S3 namespace
  - `render_location_constraint(region)` -- renders LocationConstraint XML with us-east-1 quirk (empty/self-closing element for us-east-1)
- **src/bleepstore/handlers/bucket.py**: Rewrote from stubs to full implementation of all 7 bucket handlers:
  - `list_buckets()` -- queries metadata store, renders XML response
  - `create_bucket()` -- validates bucket name via `validate_bucket_name()`, parses LocationConstraint from XML body, supports x-amz-acl canned ACL header, idempotent (existing bucket returns 200 per us-east-1 behavior), derives owner_id from SHA-256 of access key
  - `delete_bucket()` -- checks existence (NoSuchBucket), checks empty via count_objects (BucketNotEmpty), returns 204
  - `head_bucket()` -- checks existence (NoSuchBucket on 404), returns x-amz-bucket-region header, no body
  - `get_bucket_location()` -- checks existence, returns LocationConstraint XML
  - `get_bucket_acl()` -- checks existence, deserializes stored ACL JSON, falls back to default ACL, returns AccessControlPolicy XML
  - `put_bucket_acl()` -- checks existence, supports 3 modes: canned ACL header, XML body with full AccessControlPolicy parsing, or default private. Uses `_parse_acl_xml()` helper for XML body parsing with namespace-aware element lookup via `_find_elem()` helper (avoids ElementTree deprecated truth-value warnings)
- **src/bleepstore/server.py**: Major update:
  - Added `lifespan` async context manager: initializes SQLiteMetadataStore on startup (`init_db()`), seeds default credentials from config (`put_credential()`), closes store on shutdown. Crash-only: every startup is recovery.
  - Wired `BucketHandler` into route dispatch: GET / -> list_buckets, PUT /{bucket} -> create_bucket (or put_bucket_acl if ?acl), DELETE /{bucket} -> delete_bucket, HEAD /{bucket} -> head_bucket, GET /{bucket}?location -> get_bucket_location, GET /{bucket}?acl -> get_bucket_acl
  - Remaining object/multipart routes still raise NotImplementedS3Error
- **tests/conftest.py**: Updated `client` fixture to manually initialize metadata store on app.state (since lifespan doesn't auto-run with httpx ASGITransport). Seeds credentials to match lifespan behavior.
- **tests/test_server.py**: Updated to reflect bucket routes being wired:
  - Removed bucket-level 501 tests (those routes now return real responses)
  - Added `TestBucketRoutesWired` class (7 tests): verifies ListBuckets returns 200 XML, CreateBucket returns 200, HeadBucket returns 200/404, GetBucketLocation returns XML, GetBucketAcl returns XML, DeleteBucket returns 204
  - Retained all object/multipart 501 stub tests (those are still not implemented)
  - Updated error XML tests to use object routes (still 501) instead of bucket routes
  - Added tests that create buckets before testing ?uploads and plain GET (ListObjects) to avoid NoSuchBucket
- **tests/test_handlers_bucket.py** (new): 38 integration tests across 7 test classes, each using a fresh in-memory metadata store:
  - `TestListBuckets` (5 tests): empty, has_owner, with_buckets, has_namespace, common_headers
  - `TestCreateBucket` (9 tests): success, exists_in_head, idempotent, invalid_uppercase, invalid_too_short, invalid_ip, with_location_constraint, with_canned_acl, malformed_xml
  - `TestDeleteBucket` (4 tests): success, nonexistent, not_empty, then_head_404
  - `TestHeadBucket` (4 tests): existing, has_region_header, nonexistent, no_body
  - `TestGetBucketLocation` (5 tests): default_region, custom_region, nonexistent, content_type, xml_declaration
  - `TestGetBucketAcl` (6 tests): default (FULL_CONTROL), has_owner, has_grantee, nonexistent, has_namespace, content_type
  - `TestPutBucketAcl` (5 tests): canned_private, canned_public_read (verifies READ + AllUsers), nonexistent, xml_body, replaces_existing
- **Key decisions**:
  - Owner ID derived from SHA-256 hash of access key, truncated to 32 chars (matching the plan)
  - ACL stored as JSON in metadata store, rendered as XML on output -- allows flexible ACL structures
  - CreateBucket idempotency follows us-east-1 behavior: if bucket exists, return 200 (not 409)
  - `_find_elem()` helper avoids ElementTree deprecated truth-value warnings when searching for elements with fallback
  - Each bucket test gets a fresh in-memory SQLite to avoid inter-test state pollution
  - Unused import `json` removed from bucket.py
- **Test results**: 195 tests total (157 existing + 38 new), all passing, zero warnings

## Session 5 -- Stage 2: Metadata Store & SQLite (2026-02-23)
- **pyproject.toml**: Added `aiosqlite` to dependencies.
- **src/bleepstore/metadata/models.py** (new): Defined 8 `@dataclass` types:
  - `BucketMeta` -- bucket metadata (name, region, owner_id, owner_display, acl, created_at)
  - `ObjectMeta` -- object metadata (15 fields including all optional content headers, ACL, user_metadata)
  - `UploadMeta` -- multipart upload metadata (15 fields including all headers that transfer to the final object)
  - `PartMeta` -- part metadata (upload_id, part_number, size, etag, last_modified)
  - `Credential` -- authentication credential (access_key_id, secret_key, owner_id, display_name, active, created_at)
  - `ListResult` -- result container for ListObjects (contents, common_prefixes, is_truncated, tokens)
  - `ListUploadsResult` -- result container for ListMultipartUploads
  - `ListPartsResult` -- result container for ListParts
- **src/bleepstore/metadata/store.py**: Expanded `MetadataStore` protocol to include all CRUD methods:
  - Bucket: `create_bucket()` with owner_id/owner_display/acl params, `bucket_exists()`, `delete_bucket()`, `get_bucket()`, `list_buckets()` with optional owner_id filter, `update_bucket_acl()`
  - Object: `put_object()` with 13 params (all content headers, ACL, user_metadata), `object_exists()`, `get_object()`, `delete_object()`, `delete_objects_meta()` (batch), `update_object_acl()`, `list_objects()` with prefix/delimiter/pagination, `count_objects()`
  - Multipart: `create_multipart_upload()` with all 14 params, `get_multipart_upload()`, `complete_multipart_upload()` (atomic: insert object + delete upload/parts), `abort_multipart_upload()`, `put_part()`, `get_parts_for_completion()`, `list_parts()` with pagination, `list_multipart_uploads()` with prefix/delimiter/pagination
  - Credential: `get_credential()`, `put_credential()`
- **src/bleepstore/metadata/sqlite.py**: Full `SQLiteMetadataStore` implementation:
  - `init_db()` sets pragmas (WAL, NORMAL sync, foreign keys, busy timeout 5000ms), creates 6 tables (buckets, objects, multipart_uploads, multipart_parts, credentials, schema_version) with indexes and foreign keys
  - Schema is idempotent (`CREATE TABLE IF NOT EXISTS`) -- safe to call on every startup (crash-only)
  - All CRUD methods implemented using `aiosqlite` with `await self._db.execute()` / cursor-based reads
  - `list_objects()` uses application-level CommonPrefixes grouping for delimiter support, over-fetches rows to handle prefix collapsing, and determines truncation by checking for additional rows in DB
  - `complete_multipart_upload()` wraps object insert + upload/parts delete in explicit `BEGIN`/`COMMIT`/`ROLLBACK` transaction
  - `put_object()` and `put_part()` use `INSERT OR REPLACE` for upsert behavior
  - `get_credential()` only returns active credentials (active=1)
  - ACL and user_metadata stored as JSON text via `json.dumps()`/`json.loads()`
  - max_keys=0 short-circuits with empty result
- **src/bleepstore/metadata/__init__.py**: Updated to export all new models and `SQLiteMetadataStore`
- **tests/test_metadata_sqlite.py** (new): 64 tests across 7 test classes:
  - `TestSchemaIdempotency` (3 tests): init_db twice, schema_version exists, reopen after close
  - `TestBucketCRUD` (12 tests): create/get, nonexistent, exists, delete, list (empty, multiple, by owner), ACL, default region, duplicate raises IntegrityError
  - `TestObjectCRUD` (12 tests): put/get, nonexistent, exists, upsert, delete, batch delete, all fields, ACL update, count, cascade delete
  - `TestListObjects` (11 tests): all objects, prefix, delimiter, prefix+delimiter (two levels), max_keys, pagination (continuation_token and marker), empty bucket, key_count, required fields
  - `TestMultipartLifecycle` (14 tests): create/get upload, nonexistent, put/list parts, part upsert, get_parts_for_completion, complete (object created, upload+parts removed), abort, list_parts pagination, list_multipart_uploads (basic, prefix, empty), all fields, cascade delete
  - `TestCredentials` (5 tests): put/get, nonexistent, upsert, inactive not returned, multiple
  - `TestEdgeCases` (7 tests): slashes in keys, special chars, empty prefix, nonexistent prefix, max_keys=0, user_metadata round-trip, ACL JSON round-trip
- **Key decisions**:
  - No HTTP handler changes in this stage (purely metadata layer, as specified)
  - ACL and user_metadata stored as JSON text for flexibility
  - Continuation tokens are just the last returned key (opaque to caller)
  - `complete_multipart_upload` accepts final object metadata so handlers can pass through upload metadata
  - `put_part` signature simplified to (upload_id, part_number, size, etag) since bucket/key are on the upload record
  - Foreign key CASCADE ensures bucket deletion cleans up objects and uploads
- **Test results**: 157 tests total (93 existing + 64 new), all passing

## Session 4 -- Stage 1b: OpenAPI, Observability & Validation (2026-02-23)
- **pyproject.toml**: Added `prometheus-client` and `prometheus-fastapi-instrumentator` to dependencies.
- **src/bleepstore/metrics.py** (new): Defined 6 custom Prometheus metrics:
  - `bleepstore_s3_operations_total` (Counter with `operation`, `status` labels)
  - `bleepstore_objects_total` (Gauge)
  - `bleepstore_buckets_total` (Gauge)
  - `bleepstore_bytes_received_total` (Counter)
  - `bleepstore_bytes_sent_total` (Counter)
  - `bleepstore_http_request_duration_seconds` (Histogram with custom buckets: 0.005-10s)
- **src/bleepstore/server.py**: Updated app title from "BleepStore" to "BleepStore S3 API". Added `Instrumentator` integration with `/metrics` excluded from instrumentation. Added `RequestValidationError` exception handler that maps Pydantic validation errors to S3 error XML (`InvalidArgument`, 400). Imported `bleepstore.metrics` to ensure custom metrics are registered.
- **src/bleepstore/validation.py** (new): Three validation functions:
  - `validate_bucket_name()` -- full S3 bucket naming rules (3-63 chars, lowercase, no IP, no `xn--`, no `-s3alias`, no `--ol-s3`, no consecutive dots)
  - `validate_object_key()` -- max 1024 bytes (UTF-8 encoded)
  - `validate_max_keys()` -- integer in [0, 1000] range
- **tests/test_metrics.py** (new): 10 tests -- /metrics returns 200, Prometheus format, all bleepstore_* metrics present.
- **tests/test_openapi.py** (new): 8 tests -- /docs returns HTML with "swagger", /openapi.json returns valid JSON with `openapi` key and correct title.
- **tests/test_validation.py** (new): 24 tests -- comprehensive unit tests for all three validation functions covering valid inputs, edge cases, and all rejection rules.
- **Key decisions**: Validation functions are defined independently of handlers. They are NOT wired into the 501-stub handlers yet (deferred to Stage 3 per instructions). The `RequestValidationError` handler is in place for when FastAPI's own Pydantic validation triggers (e.g., on typed path/query params).

## Session 1 -- Project Scaffolding (2026-02-22)
- Created project structure: 22 source files + pyproject.toml
- All handler, metadata, storage, and cluster modules stubbed
- Config dataclasses defined
- Error hierarchy created (22 S3Error subclasses)
- Storage backend Protocol with all methods
- Metadata store Protocol with all methods

## Session 2 -- Stage 1: Server Bootstrap & Configuration (2026-02-22)
- **config.py**: Rewrote config parsing to handle nested YAML structure (`metadata.sqlite.path`, `storage.local.root_dir`). Added `region` field to `ServerConfig`. Default port set to 9000 (matching example config).
- **server.py**: Full rewrite. Implemented `create_app()` with two middleware layers: `error_middleware` (catches S3Error, renders XML) and `common_headers_middleware` (x-amz-request-id, x-amz-id-2, Date, Server). All S3 routes registered with query-param dispatch. Health endpoint at `GET /health`. `run_server()` with `web.AppRunner`, signal handlers, crash-only lifecycle.
- **cli.py**: Full implementation. Loads config from YAML, applies `--host`/`--port` CLI overrides, runs `asyncio.run(run_server(config))`.
- **errors.py**: Added `NotImplementedS3Error` (code="NotImplemented", status=501).
- **xml_utils.py**: Implemented `render_error()` with proper XML escaping, no namespace on Error element.
- **pyproject.toml**: Added `pytest-aiohttp` to dev dependencies.
- **tests/**: Created `test_server.py` (21 route tests + health + common headers + error XML format), `test_xml_utils.py` (5 tests), `test_config.py` (7 tests), `conftest.py`.

## Session 3 -- Framework Migration: aiohttp -> FastAPI (2026-02-22)
- **pyproject.toml**: Replaced `aiohttp`, `aiosqlite`, `xmltodict` with `fastapi`, `uvicorn[standard]`, `pydantic`, `pydantic-settings`. Replaced `pytest-aiohttp` with `httpx` for testing. Kept `pyyaml`, `pytest-asyncio`.
- **config.py**: Rewrote from `dataclass` to Pydantic `BaseModel`. All config sections (`ServerConfig`, `AuthConfig`, `MetadataConfig`, `StorageConfig`, `ClusterConfig`, `BleepStoreConfig`) now use `BaseModel` with `Field(default_factory=...)`. Same `load_config()` function signature and YAML parsing logic preserved. Full backward compatibility with existing test expectations.
- **server.py**: Full rewrite from aiohttp to FastAPI.
  - `create_app()` returns a FastAPI instance instead of `web.Application`.
  - Middleware: single `@app.middleware("http")` for common headers (x-amz-request-id, x-amz-id-2, Date, Server). Request ID stored on `request.state.request_id`.
  - Exception handlers: `@app.exception_handler(S3Error)` for S3 error XML, `@app.exception_handler(Exception)` for unhandled errors returning InternalError. HEAD requests return no body.
  - Routes: All S3 routes registered as FastAPI path operations with `{key:path}` for object keys containing slashes. Query param dispatch via `request.query_params`.
  - Removed `run_server()` async function -- uvicorn handles the server lifecycle.
  - Config stored on `app.state.config`.
- **cli.py**: Replaced `asyncio.run(run_server(config))` with `uvicorn.run(app, ...)`. SIGTERM/SIGINT handled by uvicorn's built-in graceful shutdown. No custom signal handlers needed.
- **xml_utils.py**: Replaced `aiohttp.web.Response` import with `fastapi.responses.Response`. `xml_response()` now returns `Response(content=..., status_code=..., media_type="application/xml")`.
- **errors.py**: No changes needed -- the S3Error hierarchy is framework-agnostic.
- **tests/conftest.py**: Replaced `aiohttp_client` fixture with `httpx.AsyncClient` + `ASGITransport`.
- **tests/test_server.py**: Rewrote all tests from aiohttp test client to httpx AsyncClient. Changed `resp.status` to `resp.status_code`, `await resp.text()` to `resp.text`, `await resp.json()` to `resp.json()`, `resp.content_type` to `resp.headers["content-type"]`. Header names are lowercase in httpx (e.g., `"server"` not `"Server"`).
- **tests/test_xml_utils.py**: No changes needed -- tests only use `render_error()` which is framework-agnostic.
- **tests/test_config.py**: No changes needed -- Pydantic BaseModel is API-compatible with dataclass usage in tests.

## Session 20 — 2026-02-25

### Pluggable Storage Backends (memory, sqlite, cloud enhancements)

**New storage backends:**
- **Memory backend** (`storage/memory.py`): In-memory dict-based storage with optional SQLite snapshot persistence. Supports `max_size_bytes` limit, `persistence: "none"|"snapshot"`, background snapshot task via asyncio.
- **SQLite backend** (`storage/sqlite.py`): Object BLOBs stored in the same SQLite database as metadata. Tables: `object_data`, `part_data`. Uses aiosqlite with WAL mode.

**Cloud config enhancements:**
- AWS: `endpoint_url`, `use_path_style`, `access_key_id`, `secret_access_key`
- GCP: `credentials_file`
- Azure: `connection_string`, `use_managed_identity`

**Config + factory:**
- Updated `config.py` with memory_* fields and cloud enhancement fields
- Updated `server.py` `_create_storage_backend()` with "memory" and "sqlite" cases
- Updated `bleepstore.example.yaml` with new config sections

**E2E:**
- Updated `run_e2e.sh` with `--backend` flag (e.g., `./run_e2e.sh --backend memory`)
