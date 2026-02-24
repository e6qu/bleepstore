# Stage 1: Server Bootstrap & Configuration

**Date:** 2026-02-22
**Status:** Complete (pending verification)

## What Was Implemented

### 1. Configuration Loading (`src/bleepstore/config.py`)
- Added `region` field to `ServerConfig` (default: `"us-east-1"`)
- Replaced flat `_build_section()` with dedicated parsers for each section
- `_parse_metadata()`: handles nested `metadata.sqlite.path` from YAML
- `_parse_storage()`: handles nested `storage.local.root_dir`, `storage.aws.*`, `storage.gcp.*`, `storage.azure.*`
- All fields have sensible defaults matching `bleepstore.example.yaml`
- Default port changed from 8333 to 9000 (matching example config)

### 2. CLI Entry Point (`src/bleepstore/cli.py`)
- `parse_args()`: `--config`, `--host`, `--port` arguments
- `main()`: loads config, applies CLI overrides, calls `asyncio.run(run_server(config))`
- Logging configured to stderr with timestamps
- Handles `FileNotFoundError` for missing config gracefully

### 3. HTTP Server (`src/bleepstore/server.py`)
- **Middleware stack**: `error_middleware` (outer) -> `common_headers_middleware` (inner)
- **Common headers on every response**: `x-amz-request-id` (16-char uppercase hex via `secrets.token_hex(8).upper()`), `x-amz-id-2` (base64 of 24 random bytes), `Date` (RFC 1123 via `email.utils.formatdate`), `Server: BleepStore`
- **Health check**: `GET /health` -> `{"status": "ok"}` (200)
- **All S3 routes registered** with query-param-based dispatch:
  - `GET /` -> ListBuckets
  - `PUT /{bucket}` -> CreateBucket (or PutBucketAcl if `?acl`)
  - `DELETE /{bucket}` -> DeleteBucket
  - `HEAD /{bucket}` -> HeadBucket
  - `GET /{bucket}` -> ListObjects / GetBucketLocation / GetBucketAcl / ListUploads
  - `POST /{bucket}` -> DeleteObjects (batch)
  - `PUT /{bucket}/{key}` -> PutObject / UploadPart / PutObjectAcl / CopyObject
  - `GET /{bucket}/{key}` -> GetObject / GetObjectAcl / ListParts
  - `HEAD /{bucket}/{key}` -> HeadObject
  - `DELETE /{bucket}/{key}` -> DeleteObject / AbortMultipartUpload
  - `POST /{bucket}/{key}` -> CreateMultipartUpload / CompleteMultipartUpload
- **All stub handlers** raise `NotImplementedS3Error` (501)
- **Error middleware**: catches `S3Error` -> renders XML; catches exceptions -> InternalError XML
- **HEAD requests**: error responses have no body (just status code)
- **`run_server()`**: uses `web.AppRunner` + `web.TCPSite`, signal handlers for SIGTERM/SIGINT

### 4. Error XML (`src/bleepstore/xml_utils.py`)
- `render_error()`: produces well-formed S3 error XML with no namespace
- `_escape_xml()`: proper XML escaping using `xml.sax.saxutils.escape`
- Includes Code, Message, Resource, RequestId, and any extra_fields

### 5. NotImplementedS3Error (`src/bleepstore/errors.py`)
- Added `NotImplementedS3Error` class (code="NotImplemented", status=501)

### 6. Crash-Only Design
- Every startup is recovery (no `--recovery-mode`)
- SIGTERM handler: stop accepting, wait, exit. No cleanup.
- No persistent in-memory state

## Files Changed
- `src/bleepstore/config.py` -- Full rewrite of parsing logic
- `src/bleepstore/server.py` -- Full rewrite with middleware, routes, server lifecycle
- `src/bleepstore/cli.py` -- Full implementation
- `src/bleepstore/errors.py` -- Added NotImplementedS3Error
- `src/bleepstore/xml_utils.py` -- Implemented render_error()
- `pyproject.toml` -- Added pytest-aiohttp to dev dependencies
- `tests/conftest.py` -- Test fixtures
- `tests/test_server.py` -- Server and route tests
- `tests/test_xml_utils.py` -- XML rendering tests
- `tests/test_config.py` -- Config loading tests

## Key Decisions
1. **Middleware ordering**: error_middleware wraps common_headers_middleware, so even error responses get common headers
2. **Query-param dispatch**: Since aiohttp cannot route on query strings, each path has a single handler that dispatches internally based on query parameters
3. **Request ID propagation**: Stored on `request["request_id"]` by common_headers_middleware so error_middleware can include it in XML
4. **Object key pattern**: `{key:.+}` regex to match keys containing slashes
