# BleepStore Python Implementation Plan

Derived from the [global plan](../PLAN.md). Each stage below adds Python-specific setup, file paths, library patterns, and test commands. See the global plan for full specifications and definitions of done.

## Setup

- **Python 3.11+** (required by `pyproject.toml`)
- **Package manager:** `uv`
- **Create environment and install:**
  ```bash
  cd python/
  uv venv .venv && source .venv/bin/activate
  uv pip install -e ".[dev]"
  ```
- **Run the server:**
  ```bash
  bleepstore --config ../bleepstore.example.yaml
  ```
- **Unit tests:**
  ```bash
  uv run pytest tests/ -v
  ```
- **E2E tests (from repo root):**
  ```bash
  cd tests/
  BLEEPSTORE_ENDPOINT=http://localhost:9000 ./run_tests.sh
  ```
- **Type checking:** `uv run mypy src/bleepstore/`
- **Linting:** `uv run ruff check src/`

### Key dependencies

| Package | Purpose |
|---|---|
| `fastapi` | HTTP framework (ASGI) |
| `uvicorn[standard]` | ASGI server |
| `pydantic` | Data validation and config models |
| `pydantic-settings` | Settings management |
| `pyyaml` | Configuration parsing |
| `aiosqlite` | Async SQLite access (added in Stage 2) |
| `httpx` | HTTP client for testing |
| `pytest` + `pytest-asyncio` | Unit testing with `asyncio_mode = "auto"` |

---

## Stage 1: Server Bootstrap & Configuration ✅

**Goal:** Running HTTP server, S3 error XML on all routes, common response headers.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/config.py` | Add `region` field to `ServerConfig`; add nested `metadata.sqlite.path` / `storage.local.root_dir` config parsing to match `bleepstore.example.yaml` structure (the YAML uses `metadata.sqlite.path` but the dataclass uses flat `sqlite_path`) |
| `src/bleepstore/server.py` | Implement `create_app()` fully: store metadata/storage refs on `app`, add `@web.middleware` for common headers (`x-amz-request-id`, `Date`, `Server`, `Content-Type`), add `/health` endpoint, implement `run_server()` with `web.AppRunner` + graceful shutdown via `asyncio.Event` + signal handlers |
| `src/bleepstore/cli.py` | Wire `main()`: load config, call `asyncio.run(run_server(config))`, handle `--host`/`--port` overrides |
| `src/bleepstore/errors.py` | Already has all error subclasses; add `NotImplementedS3Error` (501) if not present |
| `src/bleepstore/xml_utils.py` | Implement `render_error()` -- build error XML without namespace; use `xmltodict.unparse()` or manual f-string |
| `src/bleepstore/server.py` | Complete route table: add missing routes for `POST /{bucket}?delete`, `POST /{bucket}/{key}?uploads`, `POST /{bucket}/{key}?uploadId=...`; add `PUT /{bucket}?acl` |

### Key patterns

- **aiohttp middleware** for common headers:
  ```python
  @web.middleware
  async def common_headers_middleware(request: web.Request, handler):
      response = await handler(request)
      response.headers["x-amz-request-id"] = secrets.token_hex(8).upper()
      response.headers["x-amz-id-2"] = base64.b64encode(secrets.token_bytes(24)).decode()
      response.headers["Date"] = email.utils.formatdate(usegmt=True)
      response.headers["Server"] = "BleepStore"
      return response
  ```
- **Error-handling middleware** that catches `S3Error` and calls `render_error()`:
  ```python
  @web.middleware
  async def error_middleware(request, handler):
      try:
          return await handler(request)
      except S3Error as exc:
          body = render_error(exc.code, exc.message, request.path, request_id)
          return web.Response(text=body, status=exc.http_status, content_type="application/xml")
  ```
- **Route dispatching by query params** -- aiohttp does not natively route on query strings. Use a single handler per path and dispatch internally:
  ```python
  async def handle_bucket_get(request):
      if "location" in request.query: return await bucket.get_bucket_location(request)
      if "acl" in request.query: return await bucket.get_bucket_acl(request)
      if "uploads" in request.query: return await multipart.list_uploads(request)
      return await obj.list_objects(request)
  ```
- **Stub handlers** return `S3Error("NotImplemented", "...", 501)` initially.
- **Request ID:** `secrets.token_hex(8).upper()` produces 16-char uppercase hex.

### Crash-only startup

- **Every startup is a recovery.** On startup: open SQLite (WAL auto-recovers), clean temp files (e.g. `data/.tmp/`), reap expired multipart uploads, seed credentials. There is no separate `--recovery-mode` flag.
- See `../specs/crash-only.md` for the full crash-only design specification.

### Unit test approach

- `tests/test_server.py`: Create app with `aiohttp.test_utils.TestClient`, verify `/health` returns 200, all stub routes return 501 with XML body, common headers present.
- `tests/test_xml_utils.py`: Verify `render_error()` output matches expected XML string.
- `tests/test_config.py`: Load example YAML and verify all fields populated.

---

## Stage 1b: Framework Upgrade, OpenAPI & Observability ✅

**Goal:** Add Prometheus metrics, verify OpenAPI/Swagger UI, and wire Pydantic validation for S3 inputs. FastAPI already serves /docs and /openapi.json — just verify they work.

### Files to modify

| File | Work |
|---|---|
| `pyproject.toml` | Add `prometheus-fastapi-instrumentator` and `prometheus_client` dependencies |
| `src/bleepstore/server.py` | Wire `Instrumentator().instrument(app)` in `create_app()`. Add custom S3 metrics (Counters, Histograms, Gauges). |
| `src/bleepstore/metrics.py` | **New file.** Define custom Prometheus metrics: `bleepstore_s3_operations_total`, `bleepstore_objects_total`, `bleepstore_buckets_total`, `bleepstore_bytes_received_total`, `bleepstore_bytes_sent_total`, `bleepstore_http_request_duration_seconds` |
| `src/bleepstore/handlers/bucket.py` | Add Pydantic models for bucket name validation (S3 bucket naming rules) |
| `src/bleepstore/handlers/object.py` | Add Pydantic models for object key validation, query param validation |

### Key patterns

- **prometheus-fastapi-instrumentator** auto-instruments all routes:
  ```python
  from prometheus_fastapi_instrumentator import Instrumentator
  Instrumentator().instrument(app).expose(app, endpoint="/metrics")
  ```
  This auto-tracks `http_requests_total`, `http_request_duration_seconds`, etc.
- **Custom S3 metrics** via `prometheus_client`:
  ```python
  from prometheus_client import Counter, Histogram, Gauge
  s3_ops_total = Counter("bleepstore_s3_operations_total", "S3 operations", ["operation", "status"])
  objects_gauge = Gauge("bleepstore_objects_total", "Total objects")
  buckets_gauge = Gauge("bleepstore_buckets_total", "Total buckets")
  ```
- **FastAPI built-in OpenAPI:** `/docs` (Swagger UI) and `/openapi.json` already work. Verify title is "BleepStore S3 API" and version is correct.
- **Pydantic validation:** FastAPI already uses Pydantic for request validation. Add `BucketNameValidator`, `MaxKeysParam`, etc. Map validation errors to S3 error XML using exception handlers.

### Crash-only design

- Metrics counters reset on restart (Prometheus handles gaps via `rate()`)
- Never block a request to update a counter
- Object/bucket gauges populated from metadata store on startup (when available)

### Unit test approach

- `tests/test_metrics.py`: Verify `/metrics` returns Prometheus text format with `bleepstore_` prefixed metrics
- `tests/test_openapi.py`: Verify `/docs` returns HTML, `/openapi.json` returns valid OpenAPI JSON
- Existing tests still pass

### Build/run

```bash
uv pip install -e ".[dev]"
uv run pytest tests/ -v
bleepstore --config ../bleepstore.example.yaml --port 9010 &
curl http://localhost:9010/docs     # Swagger UI
curl http://localhost:9010/metrics  # Prometheus metrics
```

### Dependencies to add

| Package | Purpose |
|---|---|
| `prometheus-fastapi-instrumentator` | Auto HTTP metrics for FastAPI |
| `prometheus_client` | Custom Prometheus metrics |

---

## Stage 2: Metadata Store & SQLite ✅

**Goal:** Full SQLite-backed metadata CRUD. No HTTP handler changes.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/metadata/store.py` | Expand `MetadataStore` protocol: add `bucket_exists()`, `object_exists()`, `update_bucket_acl()`, `update_object_acl()`, `delete_objects_meta()`, `get_multipart_upload()`, `get_parts_for_completion()`, `get_credential()`, `put_credential()`. Add broader field signatures matching global plan (owner_id, acl JSON, content_encoding, etc.) |
| `src/bleepstore/metadata/sqlite.py` | Implement all methods: `init_db()` creates tables + indexes + pragmas; all CRUD methods use `aiosqlite` with `await self._db.execute()` / `await self._db.fetchone()`. Implement `list_objects` with application-level `CommonPrefixes` grouping. |
| `src/bleepstore/metadata/models.py` | **New file.** Define `@dataclass` types: `BucketMeta`, `ObjectMeta`, `UploadMeta`, `PartMeta`, `ListResult`, `ListUploadsResult`, `ListPartsResult`, `Credential`. All typed with `datetime`, `dict`, `str | None` as appropriate. |

### Key patterns

- **aiosqlite async context:**
  ```python
  async def init_db(self):
      self._db = await aiosqlite.connect(self.db_path)
      self._db.row_factory = aiosqlite.Row
      await self._db.execute("PRAGMA journal_mode = WAL")
      await self._db.execute("PRAGMA synchronous = NORMAL")
      await self._db.execute("PRAGMA foreign_keys = ON")
      await self._db.execute("PRAGMA busy_timeout = 5000")
      await self._create_tables()
  ```
- **ACL and user_metadata** stored as JSON text via `json.dumps()` / `json.loads()`.
- **Continuation tokens** for ListObjectsV2: use the last returned key as the opaque token.
- **CommonPrefixes grouping:**
  ```python
  seen_prefixes: set[str] = set()
  for row in rows:
      if delimiter and delimiter in row["key"][len(prefix):]:
          cp = prefix + row["key"][len(prefix):].split(delimiter)[0] + delimiter
          seen_prefixes.add(cp)
      else:
          contents.append(row)
  ```
- **Owner ID derivation:** `hashlib.sha256(access_key.encode()).hexdigest()[:32]`
- **Upsert:** `INSERT OR REPLACE INTO objects (...) VALUES (...)`.
- **complete_multipart_upload** wraps insert-into-objects + delete-upload + delete-parts in `async with self._db.execute("BEGIN")`.

### Unit test approach

- `tests/test_metadata_sqlite.py` using `pytest-asyncio`:
  ```python
  @pytest.fixture
  async def store(tmp_path):
      s = SQLiteMetadataStore(str(tmp_path / "test.db"))
      await s.init_db()
      yield s
      await s.close()

  async def test_create_and_get_bucket(store):
      await store.create_bucket("my-bucket", "us-east-1")
      b = await store.get_bucket("my-bucket")
      assert b is not None
      assert b["name"] == "my-bucket"
  ```
- Cover: bucket CRUD, object CRUD, list with prefix/delimiter/pagination, multipart lifecycle, credential lookup, schema idempotency.

---

## Stage 3: Bucket CRUD ✅

**Goal:** All 7 bucket handlers wired to metadata store. 16 bucket E2E tests pass.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/handlers/bucket.py` | Implement all 7 methods: `list_buckets`, `create_bucket`, `delete_bucket`, `head_bucket`, `get_bucket_location`, `get_bucket_acl`, `put_bucket_acl` (add this method). Each accesses `request.app["metadata"]` and `request.app["config"]`. |
| `src/bleepstore/xml_utils.py` | Implement `render_list_buckets()`, `render_location_constraint()` (new), `render_acl()` (new). Use `xmltodict.unparse()` for structured XML with `xmlns`. |
| `src/bleepstore/server.py` | Ensure metadata store is initialized in `create_app()` / `on_startup` hook. Add `PUT /{bucket}?acl` route dispatch. Initialize metadata store and seed credentials on startup. |
| `src/bleepstore/handlers/acl.py` | **New file.** ACL helpers: `build_default_acl(owner_id, owner_display)`, `parse_canned_acl(acl_name, owner_id, owner_display)`, `acl_to_json()`, `acl_from_json()`, `render_acl_xml()`. |

### Key patterns

- **Bucket name validation** (regex + rules):
  ```python
  import re
  BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")
  IP_RE = re.compile(r"^\d+\.\d+\.\d+\.\d+$")
  def validate_bucket_name(name: str) -> None:
      if not BUCKET_RE.match(name) or IP_RE.match(name) or name.startswith("xn--") or ...:
          raise InvalidBucketName(name)
  ```
- **XML rendering with xmltodict:**
  ```python
  def render_list_buckets(owner_id, owner_display, buckets):
      doc = {"ListAllMyBucketsResult": {
          "@xmlns": "http://s3.amazonaws.com/doc/2006-03-01/",
          "Owner": {"ID": owner_id, "DisplayName": owner_display},
          "Buckets": {"Bucket": [{"Name": b["name"], "CreationDate": b["created_at"]} for b in buckets]}
      }}
      return xmltodict.unparse(doc)
  ```
- **LocationConstraint quirk:** us-east-1 returns `<LocationConstraint/>` (empty element), not `us-east-1` string.
- **CreateBucket** idempotency: if bucket exists and owned by caller, return 200 (us-east-1 behavior).

### Unit test approach

- `tests/test_handlers_bucket.py`: Use `aiohttp.test_utils.AioHTTPTestCase` or `aiohttp_client` fixture. Mock metadata store or use real SQLite with `tmp_path`. Test each handler for happy path and error paths.

### Build/run

```bash
uv run pytest tests/test_handlers_bucket.py -v
# Then E2E:
bleepstore --config ../bleepstore.example.yaml &
cd ../tests && python -m pytest e2e/test_buckets.py -v -m bucket_ops
```

---

## Stage 4: Basic Object CRUD ✅

**Goal:** PutObject, GetObject, HeadObject, DeleteObject with local filesystem backend. 12 basic object tests pass.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/storage/local.py` | Implement: `init()` (create root dir), `put()` (write to temp file, compute MD5, atomic rename), `get()` (read file bytes), `get_stream()` (async generator yielding 64KB chunks with offset/length), `delete()` (remove file, optionally clean empty parents), `exists()` (path check). |
| `src/bleepstore/handlers/object.py` | Implement `put_object()`, `get_object()`, `head_object()`, `delete_object()`. Dispatch within `put_object` if `x-amz-copy-source` header present (defer to Stage 5). Extract `x-amz-meta-*` headers. |
| `src/bleepstore/server.py` | Initialize storage backend in `on_startup`, store as `app["storage"]`. Factory pattern: read `storage.backend` from config to choose `LocalStorageBackend`. |

### Key patterns

- **Atomic write:**
  ```python
  import hashlib, uuid
  async def put(self, bucket, key, data):
      path = self.root / bucket / key
      path.parent.mkdir(parents=True, exist_ok=True)
      tmp = path.with_suffix(f".tmp.{uuid.uuid4().hex[:8]}")
      md5 = hashlib.md5(data).hexdigest()
      tmp.write_bytes(data)
      tmp.rename(path)
      return md5
  ```
- **Streaming GET with aiohttp.web.StreamResponse:**
  ```python
  async def get_object(self, request):
      resp = web.StreamResponse(status=200, headers={...})
      await resp.prepare(request)
      async for chunk in storage.get_stream(bucket, key, offset, length):
          await resp.write(chunk)
      await resp.write_eof()
      return resp
  ```
- **ETag quoting:** Always return `'"' + md5hex + '"'` in the `ETag` header.
- **Content-Type default:** `application/octet-stream` when not specified.
- **User metadata:** Extract headers matching `x-amz-meta-*`, strip prefix, store in metadata store as JSON dict.
- **HeadObject:** Same as GetObject logic minus writing the body. Return `web.Response(status=200, headers=...)` with empty body.
- **DeleteObject:** Always return 204, even if key does not exist (idempotent).

### Unit test approach

- `tests/test_storage_local.py`: Test `put` + `get` round-trip, `get_stream` with offset/length, `delete`, `exists`, atomic rename behavior.
- `tests/test_handlers_object.py`: Integration tests via `aiohttp.test_utils.TestClient` with real SQLite + local storage backend in `tmp_path`.

---

## Stage 5a: List, Copy & Batch Delete ✅

**Goal:** CopyObject, DeleteObjects (batch), ListObjectsV2, and ListObjects v1. 14 list/copy/delete object tests pass.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/handlers/object.py` | Implement: `copy_object()` (dispatch from `put_object` when `x-amz-copy-source` present), `delete_objects()` / `delete_multi()`, `list_objects_v2()`, `list_objects_v1()`. |
| `src/bleepstore/xml_utils.py` | Implement: `render_list_objects_v2()`, `render_list_objects_v1()`, `render_copy_object_result()`, `render_delete_result()`. |
| `src/bleepstore/storage/local.py` | Implement `copy_object()` -- read source bytes, write to destination atomically, return new ETag. |

### Key patterns

- **DeleteObjects XML parsing with xmltodict:**
  ```python
  body = await request.read()
  parsed = xmltodict.parse(body)
  objects = parsed["Delete"]["Object"]
  if isinstance(objects, dict): objects = [objects]  # xmltodict returns dict for single item
  quiet = parsed["Delete"].get("Quiet", "false") == "true"
  ```
- **CopyObject:** Parse `x-amz-copy-source`, URL-decode it, split into `(src_bucket, src_key)`. Handle `x-amz-metadata-directive` (COPY vs REPLACE).
- **ListObjects** delegation: `list_objects()` already dispatches to v1/v2 in the scaffold.

### Unit test approach

- `tests/test_handlers_object_advanced.py`: Copy, batch delete, list with pagination.

### Test targets

- 14 tests from `tests/e2e/test_objects.py`: `TestDeleteObjects` (2), `TestCopyObject` (3), `TestListObjectsV2` (7), `TestListObjectsV1` (2)

### Definition of done

- CopyObject supports COPY and REPLACE metadata directives
- DeleteObjects handles both quiet and verbose modes
- ListObjectsV2 with delimiter correctly returns CommonPrefixes
- ListObjectsV2 pagination works with MaxKeys and ContinuationToken
- ListObjects v1 works with Marker

---

## Stage 5b: Range, Conditional Requests & Object ACLs ✅

**Goal:** Range requests, conditional requests (If-Match, If-None-Match, etc.), and object ACL operations. All 32 object tests + 4 ACL tests pass (cumulative with 5a).

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/handlers/object.py` | Add range and conditional request handling to `get_object()` and `head_object()`. Implement `get_object_acl()`, `put_object_acl()`. |
| `src/bleepstore/handlers/acl.py` | Add `render_acl_xml()` for object ACLs (same format as bucket ACLs). |

### Key patterns

- **Range request parsing:**
  ```python
  def parse_range(header: str, total: int) -> tuple[int, int] | None:
      # "bytes=0-4" -> (0, 4), "bytes=-5" -> (total-5, total-1), "bytes=10-" -> (10, total-1)
  ```
  Return 206 with `Content-Range: bytes start-end/total`.
- **Conditional request evaluation** (in order):
  - `If-Match` / `If-Unmodified-Since` -> 412 on failure
  - `If-None-Match` / `If-Modified-Since` -> 304 on match
  - Use `email.utils.parsedate_to_datetime()` for date parsing.

### Unit test approach

- `tests/test_handlers_object_advanced.py`: Range requests (206, 416), conditional requests (304, 412).
- `tests/test_range.py`: Isolated range header parser tests.

### Test targets

- 6 tests from `tests/e2e/test_objects.py`: `TestGetObjectRange` (3), `TestConditionalRequests` (3)
- 4 tests from `tests/e2e/test_acl.py`: `TestObjectAcl` (4)

### Definition of done

- All 32 tests in `test_objects.py` pass (cumulative with 5a)
- All 4 tests in `test_acl.py` pass
- Range requests return 206 with correct Content-Range header
- Conditional requests return 304/412 as appropriate

---

## Stage 6: AWS Signature V4 ✅

**Goal:** SigV4 header auth + presigned URL validation. 4 presigned tests + 2 auth error tests pass. All previous tests still pass.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/auth.py` | Full implementation: `SigV4Authenticator.verify_request()` dispatches to header-auth or presigned-URL-auth. `_parse_authorization_header()`, `_build_canonical_request()`, `_build_string_to_sign()`, `_derive_signing_key()`, `_compute_signature()`. Add `verify_presigned()` method. Add `_uri_encode()` utility. |
| `src/bleepstore/server.py` | Add auth middleware that runs before handlers (but after common-headers). Skip `/health`. Catch auth errors -> return S3 error responses. |
| `src/bleepstore/metadata/sqlite.py` | Ensure `get_credential()` and `put_credential()` are implemented (from Stage 2). |

### Key patterns

- **HMAC-SHA256 chain:**
  ```python
  import hmac, hashlib
  def _derive_signing_key(secret_key, date, region, service):
      k_date = hmac.new(("AWS4" + secret_key).encode(), date.encode(), hashlib.sha256).digest()
      k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
      k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
      return hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
  ```
- **Constant-time comparison:** `hmac.compare_digest(expected, provided)`
- **URI encoding:**
  ```python
  import urllib.parse
  def _uri_encode(s: str, encode_slash: bool = True) -> str:
      safe = "-_.~" if encode_slash else "-_.~/"
      return urllib.parse.quote(s, safe=safe)
  ```
- **Presigned URL validation:** Extract `X-Amz-*` query params, validate expiration (`int(X-Amz-Expires)` seconds from `X-Amz-Date`), reconstruct canonical request with `UNSIGNED-PAYLOAD`, verify signature.
- **Auth middleware ordering:** `app = web.Application(middlewares=[error_middleware, auth_middleware, common_headers_middleware])`

### Unit test approach

- `tests/test_auth.py`: Test canonical request construction, string-to-sign, signing key derivation, full signature verification against known test vectors (AWS provides these). Test presigned URL validation including expiration.

---

## Stage 7: Multipart Upload - Core ✅

**Goal:** Create, upload parts, abort, list uploads, list parts. 8 multipart tests pass.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/handlers/multipart.py` | Implement: `create_multipart_upload()`, `upload_part()`, `abort_multipart_upload()`, `list_uploads()`, `list_parts()`. |
| `src/bleepstore/storage/local.py` | Implement: `put_part()` (write to `.parts/{upload_id}/{part_number}`), `delete_parts()` (remove upload directory). |
| `src/bleepstore/xml_utils.py` | Implement: `render_initiate_multipart_upload()`, `render_list_multipart_uploads()`, `render_list_parts()`. |
| `src/bleepstore/server.py` | Ensure POST routes are dispatched: `POST /{bucket}/{key}` dispatches to `create_multipart_upload` (if `?uploads`) or `complete_multipart_upload` (if `?uploadId`). `DELETE /{bucket}/{key}` dispatches to `abort_multipart_upload` if `?uploadId` present. `PUT /{bucket}/{key}` dispatches to `upload_part` if `?partNumber&uploadId` present. `GET /{bucket}/{key}` dispatches to `list_parts` if `?uploadId` present. |
| `src/bleepstore/metadata/sqlite.py` | Expand `create_multipart_upload` to accept and store all headers (content_type, acl, user_metadata, etc.). Add `get_multipart_upload()`. |

### Key patterns

- **Upload ID generation:** `str(uuid.uuid4())`
- **Query-based dispatch** within `put_object` handler:
  ```python
  async def put_object(self, request):
      if "uploadId" in request.query and "partNumber" in request.query:
          return await self._upload_part(request)
      if "x-amz-copy-source" in request.headers:
          return await self.copy_object(request)
      if "acl" in request.query:
          return await self.put_object_acl(request)
      # ... normal put
  ```
- **Part storage path:** `self.root / ".parts" / upload_id / str(part_number)`
- **Part overwrite:** Same upload_id + part_number replaces the previous file and metadata record (upsert).

### Unit test approach

- `tests/test_multipart.py`: Full lifecycle: create upload, put 3 parts, list parts, abort. Verify part files created and cleaned up. Test `NoSuchUpload` errors.

---

## Stage 8: Multipart Upload - Completion ✅

**Goal:** CompleteMultipartUpload with part assembly, composite ETag, UploadPartCopy. All 11 multipart tests pass.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/handlers/multipart.py` | Implement `complete_multipart_upload()`: parse XML body, validate part order and ETags, validate part sizes (>= 5MiB except last), call storage `assemble_parts()`, compute composite ETag, call metadata `complete_multipart_upload()`, return `CompleteMultipartUploadResult` XML. |
| `src/bleepstore/handlers/object.py` | Add `upload_part_copy()` dispatch: when `PUT /{bucket}/{key}?partNumber&uploadId` has `x-amz-copy-source`, read source data (optionally with range), write as part. |
| `src/bleepstore/storage/local.py` | Implement `assemble_parts()`: read each part file sequentially, write concatenated to final object path atomically, return ETag. Clean up part files after. |
| `src/bleepstore/xml_utils.py` | Implement `render_complete_multipart_upload()`. |

### Key patterns

- **Composite ETag computation:**
  ```python
  import hashlib, binascii
  def compute_composite_etag(part_etags: list[str]) -> str:
      binary_md5s = b""
      for etag in part_etags:
          clean = etag.strip('"')
          binary_md5s += binascii.unhexlify(clean)
      final_md5 = hashlib.md5(binary_md5s).hexdigest()
      return f'"{final_md5}-{len(part_etags)}"'
  ```
- **Part size validation:**
  ```python
  MIN_PART_SIZE = 5 * 1024 * 1024  # 5 MiB
  for i, part in enumerate(parts[:-1]):  # all except last
      if part["size"] < MIN_PART_SIZE:
          raise EntityTooSmall()
  ```
- **Streaming assembly** (avoid loading all parts into memory):
  ```python
  async def assemble_parts(self, bucket, key, upload_id, part_numbers):
      dest = self.root / bucket / key
      dest.parent.mkdir(parents=True, exist_ok=True)
      tmp = dest.with_suffix(f".tmp.{uuid.uuid4().hex[:8]}")
      with open(tmp, "wb") as f:
          for pn in part_numbers:
              part_path = self.root / ".parts" / upload_id / str(pn)
              f.write(part_path.read_bytes())
      tmp.rename(dest)
  ```

### Unit test approach

- `tests/test_multipart_complete.py`: Full lifecycle including completion. Verify composite ETag format. Test `InvalidPartOrder`, `InvalidPart`, `EntityTooSmall` errors. Verify part files cleaned up after completion.

---

## Stage 9a: Core Integration Testing

**Goal:** All 75 internal E2E tests pass. Smoke test passes (20/20). Fix compliance issues found by BleepStore's own test suite.

### Files to modify

| File | Work |
|---|---|
| Any/all files | Bug fixes based on E2E test failures. |

### Process

1. Run full E2E suite: `cd tests && python -m pytest e2e/ -v --tb=long`
2. Run smoke test: `BLEEPSTORE_ENDPOINT=http://localhost:9000 tests/smoke/smoke_test.sh`
3. Fix issues systematically. Common Python-specific issues:
   - **xmltodict output ordering:** Ensure element order matches S3 expectations. Use `OrderedDict` if needed.
   - **ETag quoting:** Must be `"abc123"` with literal quotes in both headers and XML.
   - **Content-Type:** Must be `application/xml` (not `text/xml`).
   - **Empty list handling with xmltodict:** When no `<Contents>`, omit the element entirely.
   - **Date formatting:** `email.utils.formatdate(usegmt=True)` for HTTP headers, `.strftime("%Y-%m-%dT%H:%M:%S.000Z")` for XML.
   - **aiohttp Content-Length:** Ensure it is set for all responses with a body.
   - **HEAD responses:** Must not have a body, even for errors.
   - **204 responses:** Must not have a body (use `web.Response(status=204)`).

### Key patterns

- **xmltodict single-item list bug:** When there is one `<Bucket>` or `<Contents>`, xmltodict parses it as a dict instead of a list. Always normalize:
  ```python
  items = parsed.get("Bucket", [])
  if isinstance(items, dict): items = [items]
  ```
- **Accept-Ranges header:** Must be `bytes` on GetObject and HeadObject responses.
- **StorageClass:** Always `STANDARD` in list responses.

### Test targets

- **BleepStore E2E: 75/75 tests pass**
  - `test_buckets.py`: 16/16
  - `test_objects.py`: 32/32
  - `test_multipart.py`: 11/11
  - `test_presigned.py`: 4/4
  - `test_acl.py`: 4/4
  - `test_errors.py`: 8/8
- **Smoke test: 20/20 pass**

### Build/run

```bash
# Full suite
bleepstore --config ../bleepstore.example.yaml &
cd tests && python -m pytest e2e/ -v
# Smoke
BLEEPSTORE_ENDPOINT=http://localhost:9000 tests/smoke/smoke_test.sh
```

### Definition of done

- All 75 BleepStore E2E tests pass
- Smoke test passes (20/20)
- `aws s3 cp`, `aws s3 ls`, `aws s3 sync` work out of the box
- `aws s3api` commands for all Phase 1 operations succeed
- No 500 Internal Server Error for valid requests
- XML responses are well-formed and namespace-correct
- All headers match S3 format expectations

---

## Stage 9b: External Test Suites & Compliance

**Goal:** Run external S3 conformance test suites (Ceph s3-tests, MinIO Mint, Snowflake s3compat) and fix compliance issues found.

### Files to modify

| File | Work |
|---|---|
| Any/all files | Bug fixes based on external test suite failures. |

### Process

1. Run Ceph s3-tests (filtered to Phase 1 operations):
   ```bash
   S3TEST_CONF=s3tests.conf python -m pytest s3tests_boto3/functional/ \
     -k "test_bucket or test_object or test_multipart" \
     --ignore=s3tests_boto3/functional/test_s3_versioning.py \
     --ignore=s3tests_boto3/functional/test_s3_lifecycle.py
   ```
2. Run MinIO Mint core mode:
   ```bash
   docker run --rm --network host \
     -e SERVER_ENDPOINT=localhost:9000 \
     -e ACCESS_KEY=bleepstore \
     -e SECRET_KEY=bleepstore-secret \
     minio/mint:latest
   ```
3. Run Snowflake s3compat (9 core operations)
4. Fix remaining compliance issues found by external suites

### Key patterns

- **Edge cases in bucket naming validation** found by Ceph s3-tests
- **Content-MD5 verification** required by some external tests
- **Chunked transfer encoding handling** for multi-SDK compatibility

### Test targets

- Ceph s3-tests: >80% of Phase 1-applicable tests pass
- Snowflake s3compat: 9/9 pass
- MinIO Mint: aws-cli tests pass
- BleepStore E2E: 75/75 still pass (no regressions)

### Definition of done

- Ceph s3-tests Phase 1 tests mostly pass (>80%)
- Snowflake s3compat 9/9 pass
- MinIO Mint aws-cli tests pass
- All 75 BleepStore E2E tests still pass (no regressions)
- Smoke test still passes (20/20)

---

## Stage 10: AWS S3 Gateway Backend ✅

**Goal:** AWS S3 storage backend passes all 75 E2E tests.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/storage/aws.py` | Full implementation using `aiobotocore` (async boto3). Implement all `StorageBackend` protocol methods. Key mapping: `{prefix}{bleepstore_bucket}/{key}`. Multipart passthrough via AWS native multipart API. |
| `src/bleepstore/server.py` | Backend factory: `"aws"` -> `AWSGatewayBackend(config)`. |
| `src/bleepstore/config.py` | Ensure `StorageConfig` parses `aws.bucket`, `aws.region`, `aws.prefix` from nested YAML. |
| `pyproject.toml` | Add optional dependency group: `aws = ["aiobotocore"]`. |

### Key patterns

- **aiobotocore session:**
  ```python
  from aiobotocore.session import get_session
  async def init(self):
      session = get_session()
      self._client_ctx = session.create_client("s3", region_name=self.region)
      self._client = await self._client_ctx.__aenter__()
  ```
- **Key mapping:** `f"{self.prefix}{bucket}/{key}"`
- **Error mapping:** Catch `botocore.exceptions.ClientError`, map `error["Code"]` to S3Error subclasses.
- **Multipart passthrough:** `create_multipart_upload` -> `self._client.create_multipart_upload(...)`, use AWS-returned upload ID directly.

### Unit test approach

- `tests/test_storage_aws.py`: Mock `aiobotocore` client with `unittest.mock.AsyncMock`. Verify key mapping, error mapping, multipart passthrough.

---

## Stage 11a: GCP Cloud Storage Backend ✅

**Goal:** GCP Cloud Storage backend passes all 75 E2E tests. Two cloud backends (AWS + GCP) working.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/storage/gcp.py` | Full implementation using `gcloud-aio-storage` (async GCS client). Handle ETag conversion (base64 md5Hash -> hex). Multipart via GCS compose (chain for >32 parts). |
| `src/bleepstore/server.py` | Backend factory: `"gcp"` -> `GCPGatewayBackend(config)`. |
| `pyproject.toml` | Add optional dependency group: `gcp = ["gcloud-aio-storage"]`. |

### Key patterns

- **GCS ETag conversion:**
  ```python
  import base64
  gcs_md5_b64 = blob.md5_hash  # e.g. "rL0Y20zC+Fzt72VPzMSk2A=="
  md5_hex = base64.b64decode(gcs_md5_b64).hex()
  ```
- **GCS compose chaining** for >32 parts:
  ```python
  # Compose in batches of 32, then compose the composites
  while len(sources) > 1:
      batches = [sources[i:i+32] for i in range(0, len(sources), 32)]
      sources = [await compose_batch(batch) for batch in batches]
  ```
- **Backend-agnostic error mapping utility**: common error mapping function shared across all cloud backends.

### Unit test approach

- `tests/test_storage_gcp.py`: Mock `gcloud-aio-storage` client, verify key mapping, ETag conversion (base64 -> hex), GCS compose chaining for >32 parts.

### Test targets

- All 75 E2E tests pass with `BLEEPSTORE_BACKEND=gcp`

### Definition of done

- GCP backend implements full `StorageBackend` interface
- All 75 E2E tests pass with `BLEEPSTORE_BACKEND=gcp`
- GCS compose-based multipart works for >32 parts
- Backend error mapping utility covers GCS error codes

---

## Stage 11b: Azure Blob Storage Backend ✅

**Goal:** Azure Blob Storage backend passes all 75 E2E tests. All three cloud backends (AWS, GCP, Azure) working.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/storage/azure.py` | Full implementation using `azure-storage-blob` with `aiohttp` transport. Multipart via Azure block blobs (`put_block` + `put_block_list`). Block IDs: base64 of zero-padded part number. |
| `src/bleepstore/server.py` | Backend factory: `"azure"` -> `AzureGatewayBackend(config)`. |
| `pyproject.toml` | Add optional dependency group: `azure = ["azure-storage-blob", "aiohttp"]`. |

### Key patterns

- **Azure block ID encoding:**
  ```python
  block_id = base64.b64encode(f"{part_number:05d}".encode()).decode()
  ```
- **Azure multipart:** `UploadPart` -> `put_block` with block ID, `CompleteMultipartUpload` -> `put_block_list`. `AbortMultipartUpload` is a no-op (uncommitted blocks auto-expire in 7 days).
- **ETag handling:** Azure ETags may differ from MD5 -- compute MD5 ourselves.
- Error mapping uses shared utility from Stage 11a.

### Unit test approach

- `tests/test_storage_azure.py`: Mock `azure-storage-blob` client, verify key mapping, block ID encoding, multipart assembly logic.

### Test targets

- All 75 E2E tests pass with `BLEEPSTORE_BACKEND=azure`

### Definition of done

- Azure backend implements full `StorageBackend` interface
- All 75 E2E tests pass with `BLEEPSTORE_BACKEND=azure`
- Azure block blob-based multipart works
- Backend error mapping covers Azure error codes

---

## Stage 12a: Raft State Machine & Storage

**Goal:** Implement the core Raft state machine, log entry types, and persistent storage. The state machine handles state transitions and log management in isolation (no networking yet).

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/cluster/raft.py` | Core state machine: state transitions (Follower/Candidate/Leader), `request_vote()` and `append_entries()` RPC handlers (accept/produce message structs, no network), `propose()` for log entry submission. |
| `src/bleepstore/cluster/log.py` | **New file.** Raft log storage: in-memory list backed by SQLite persistence. Entry types enum (`CreateBucket`, `DeleteBucket`, `PutObjectMeta`, etc.). `append()`, `get()`, `slice()`, `last_index()`, `last_term()`, truncation on conflict. |
| `src/bleepstore/cluster/state.py` | **New file.** Persistent state: `current_term`, `voted_for` stored in SQLite. `save()` and `load()` with fsync. |

### Key patterns

- **Log entry dataclass:**
  ```python
  @dataclass
  class LogEntry:
      term: int
      index: int
      entry_type: str
      data: dict[str, Any]
  ```
- **State machine is fully testable without networking** -- accept/produce message structs.

### Unit test approach

- `tests/test_raft.py`: Test state machine transitions (Follower -> Candidate -> Leader), vote granting (correct term/log checks), log append/truncation, term monotonicity, persistent state survives restart.

### Test targets

- State machine transitions: Follower -> Candidate -> Leader
- Vote granting: correct term/log checks
- Log append: entries persisted correctly
- Log truncation on conflict
- Term monotonicity: reject messages from old terms
- Persistent state survives restart (term, votedFor, log)

### Definition of done

- Raft state machine correctly transitions between Follower/Candidate/Leader
- Log entry types defined and serializable
- Persistent storage for term, votedFor, log entries
- RequestVote and AppendEntries handlers work (in-process, no networking)
- Unit tests cover state transitions, vote granting, log replication logic

---

## Stage 12b: Raft Networking & Elections

**Goal:** Add HTTP-based RPC transport to the Raft state machine. Leader election and log replication work across 3 nodes over the network.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/cluster/raft.py` | Add election timer (`asyncio.Task` with randomized timeout), `_send_heartbeats()` loop, multi-node integration. |
| `src/bleepstore/cluster/transport.py` | **New file.** HTTP transport using `aiohttp`: serve Raft RPCs on `POST /raft/request_vote`, `POST /raft/append_entries`, `POST /raft/install_snapshot`. Use `aiohttp.ClientSession` for outgoing RPCs. JSON serialization. Timeout handling for unresponsive peers. |

### Key patterns

- **Election timer with asyncio:**
  ```python
  async def _election_loop(self):
      while self._running:
          timeout = random.uniform(1.0, 2.0)  # seconds
          try:
              await asyncio.wait_for(self._heartbeat_event.wait(), timeout)
              self._heartbeat_event.clear()
          except asyncio.TimeoutError:
              await self._run_election()
  ```
- **Concurrent vote requests:**
  ```python
  tasks = [self._request_vote_from(peer) for peer in self.peers]
  results = await asyncio.gather(*tasks, return_exceptions=True)
  votes = sum(1 for r in results if isinstance(r, dict) and r.get("vote_granted"))
  ```
- **Heartbeat interval** < election timeout (by ~10x). Default heartbeat 150ms, election timeout 1000-2000ms.

### Unit test approach

- `tests/test_raft.py`: Simulate 3-node cluster in-process using mock transport. Test three-node election, log replication over network, heartbeat reset, split vote resolution.
- `tests/test_raft_transport.py`: Test HTTP RPC client/server, timeout handling, connection errors.

### Test targets

- Three-node election: one leader elected within timeout
- Log replication: leader's entries replicated to followers
- Heartbeats prevent election timeout
- Leader failure triggers new election
- Split vote resolves via randomized timeouts

### Definition of done

- Leader election works with 3 nodes over HTTP
- Log replication works over HTTP (entries committed to majority)
- Heartbeats prevent unnecessary elections
- RPCs work reliably over HTTP with timeout handling
- Integration tests cover multi-node Raft scenarios

---

## Stage 13a: Raft-Metadata Wiring

**Goal:** Wire the Raft consensus layer to the metadata store. Metadata writes go through the Raft log. Reads served from local SQLite replica. Write forwarding from followers to leader.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/cluster/state_machine.py` | **New file.** Apply function: deserialize Raft log entry -> execute corresponding `SQLiteMetadataStore` method. Deterministic application in log-index order. |
| `src/bleepstore/metadata/raft_store.py` | **New file.** `RaftMetadataStore` that wraps `SQLiteMetadataStore`: write methods submit log entries to Raft via `raft_node.propose()`, read methods delegate directly to SQLite. |
| `src/bleepstore/server.py` | When `metadata.engine == "raft"`: create `RaftNode`, wire `RaftMetadataStore` as the active metadata store. Start Raft node in `on_startup`. |

### Key patterns

- **Write forwarding:**
  ```python
  async def create_bucket(self, bucket, region, ...):
      if self.raft_node.state != NodeState.LEADER:
          # Forward to leader
          return await self._forward_to_leader("create_bucket", ...)
      entry = {"type": "CreateBucket", "bucket": bucket, "region": region, ...}
      success = await self.raft_node.propose(entry)
      if not success:
          raise InternalError("Failed to commit metadata write")
  ```
- **Read path:** Direct SQLite query, no Raft involvement.

### Unit test approach

- `tests/test_raft_integration.py`: 3-node in-process cluster. Write bucket on leader, verify read on follower (poll for eventual consistency). Test leader failover.

### Test targets

- Write on leader, read on follower (eventually consistent)
- Follower forwards writes to leader transparently
- Leader failure -> new leader -> writes continue
- Node restart -> catches up from Raft log

### Definition of done

- Metadata writes go through Raft consensus
- Reads served from local SQLite on any node
- Write forwarding from follower to leader works transparently
- Leader failover maintains metadata consistency

---

## Stage 13b: Snapshots & Node Management

**Goal:** Implement log compaction via snapshots, InstallSnapshot RPC, and dynamic node join/leave.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/cluster/raft.py` | Add snapshot support: `take_snapshot()` copies SQLite DB file, `install_snapshot()` replaces local DB. Log compaction: discard entries before snapshot index. |
| `src/bleepstore/cluster/transport.py` | Implement `POST /raft/install_snapshot` handler. Chunked transfer for large snapshots. |
| `src/bleepstore/server.py` | Configuration for `snapshot_interval` (default 10000 committed entries). |

### Key patterns

- **Snapshot:** `shutil.copy2(sqlite_path, snapshot_path)` after ensuring WAL checkpoint.
- **InstallSnapshot:** Leader sends full SQLite snapshot to followers that are too far behind.
- **Node join/leave:** Configuration changes go through Raft log (single-step, not joint consensus).

### Unit test approach

- `tests/test_raft_snapshots.py`: Test snapshot creation after configured entries, new node joins via snapshot transfer, log entries before snapshot are discarded.

### Test targets

- Snapshot created after configured number of entries
- New node joins via snapshot transfer
- Node offline for extended period catches up via snapshot
- Log entries before snapshot index are discarded
- Node leave: removed from configuration, cluster continues

### Definition of done

- Log compaction/snapshotting works
- InstallSnapshot RPC transfers full database to lagging nodes
- New node can join and sync via snapshot
- Node leave removes from cluster configuration
- Snapshot-based recovery for nodes that missed too many entries

---

## Stage 14: Cluster Operations & Admin API

**Goal:** Admin API for cluster management, multi-node E2E testing.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/cluster/admin.py` | **New file.** aiohttp handlers: `GET /admin/cluster/status`, `GET /admin/cluster/nodes`, `POST /admin/cluster/nodes`, `DELETE /admin/cluster/nodes/{id}`, `GET /admin/cluster/raft/stats`, `POST /admin/cluster/raft/snapshot`. Bearer token auth middleware. |
| `src/bleepstore/server.py` | Create separate `web.Application` for admin API on `admin_port`. Wire admin routes. Add `X-BleepStore-Leader` header to forwarded responses. |
| `src/bleepstore/config.py` | Add `admin_port` and `admin_token` to `ServerConfig`. |
| `scripts/cluster_test.py` | **New file.** Script to start 3 local BleepStore processes, run E2E suite, test failover. |

### Key patterns

- **Admin bearer token middleware:**
  ```python
  @web.middleware
  async def admin_auth(request, handler):
      token = request.headers.get("Authorization", "").removeprefix("Bearer ")
      if token != config.server.admin_token:
          raise web.HTTPForbidden()
      return await handler(request)
  ```
- **Cluster status response:**
  ```python
  return web.json_response({
      "node_id": raft.node_id,
      "state": raft.state.value,
      "term": raft.current_term,
      "leader_id": raft.leader_id,
      "commit_index": raft.commit_index,
      "peers": [{"id": p.id, "addr": p.addr, "state": p.state} for p in raft.peers],
  })
  ```

### Unit test approach

- `tests/test_admin_api.py`: Test admin endpoints with `aiohttp.test_utils.TestClient`. Mock Raft node for status responses.
- `tests/test_cluster_e2e.py`: Multi-process integration test (spawn 3 servers, run E2E suite, kill leader, verify recovery).

---

## Stage 15: Performance Optimization & Production Readiness

**Goal:** Streaming I/O, startup < 1s, memory < 50MB idle, benchmarks within 2x of MinIO.

### Files to modify

| File | Work |
|---|---|
| `src/bleepstore/handlers/object.py` | Ensure GetObject and PutObject use true streaming: read request body in chunks (`async for chunk in request.content`), write to storage incrementally. Compute MD5 during streaming. |
| `src/bleepstore/storage/local.py` | Use `aiofiles` or `asyncio.to_thread(open(...).write, ...)` for non-blocking file I/O. Stream PutObject to disk in chunks. |
| `src/bleepstore/auth.py` | Cache signing keys: `@functools.lru_cache` keyed on `(date, region, service)`. |
| `src/bleepstore/metadata/sqlite.py` | Use prepared statements via `aiosqlite` cursor caching. Ensure no N+1 queries in list operations. |
| `src/bleepstore/server.py` | Add configurable timeouts: `aiohttp.web.AppRunner` with `tcp_keepalive`, `client_max_size`. Add graceful shutdown: drain connections on SIGTERM. Add structured logging via `logging` module with JSON formatter. |
| `src/bleepstore/cli.py` | Add `--log-level` argument. Configure `logging.basicConfig()` at startup. |

### Key patterns

- **Streaming PutObject with incremental MD5:**
  ```python
  md5 = hashlib.md5()
  size = 0
  tmp = dest.with_suffix(f".tmp.{uuid.uuid4().hex[:8]}")
  with open(tmp, "wb") as f:
      async for chunk in request.content.iter_chunked(65536):
          f.write(chunk)
          md5.update(chunk)
          size += len(chunk)
  tmp.rename(dest)
  etag = md5.hexdigest()
  ```
- **Signing key cache:**
  ```python
  _signing_key_cache: dict[tuple[str, str, str], bytes] = {}
  def _derive_signing_key(self, secret_key, date, region, service):
      key = (date, region, service)
      if key not in self._signing_key_cache:
          self._signing_key_cache[key] = self._compute_signing_key(secret_key, date, region, service)
      return self._signing_key_cache[key]
  ```
- **Graceful shutdown:**
  ```python
  loop = asyncio.get_running_loop()
  stop_event = asyncio.Event()
  for sig in (signal.SIGINT, signal.SIGTERM):
      loop.add_signal_handler(sig, stop_event.set)
  await stop_event.wait()
  await runner.cleanup()
  ```
- **Structured logging:**
  ```python
  import logging, json
  class JSONFormatter(logging.Formatter):
      def format(self, record):
          return json.dumps({"level": record.levelname, "msg": record.getMessage(), "time": self.formatTime(record)})
  ```

### Build/run benchmarks

```bash
# Install warp
go install github.com/minio/warp@latest

# Start BleepStore
bleepstore --config ../bleepstore.example.yaml &

# Run benchmark
warp mixed --host=localhost:9000 --access-key=bleepstore --secret-key=bleepstore-secret --duration=60s --concurrent=10 --obj.size=1KiB

# Measure startup time
time bleepstore --config ../bleepstore.example.yaml &
# Should be < 1 second to first /health 200

# Memory check
ps -o rss= -p $(pgrep -f bleepstore) | awk '{print $1/1024 " MB"}'
```

### Unit test approach

- `tests/test_streaming.py`: Verify large objects (>10MB) stream without loading fully into memory (check peak RSS).
- `tests/performance/bench_throughput.py`: Measure objects/second for 1KB, 1MB, 100MB objects.

---

## Stage 16a: Queue Interface & Redis Backend

**Goal:** Define the QueueBackend interface, event types/envelope, and implement the Redis Streams backend with write-through mode.

### Files to create/modify

| File | Work |
|---|---|
| `src/bleepstore/queue/__init__.py` | Module init |
| `src/bleepstore/queue/backend.py` | `QueueBackend` Protocol with `connect()`, `close()`, `health_check()`, `publish()`, `publish_batch()`, `subscribe()`, `acknowledge()`, `enqueue_task()`, `dequeue_task()`, `complete_task()`, `fail_task()`, `retry_failed_tasks()` |
| `src/bleepstore/queue/events.py` | Event types enum (`bucket.created`, `bucket.deleted`, `object.created`, `object.deleted`, `objects.deleted`, `object.acl.updated`, `bucket.acl.updated`, `multipart.created`, `multipart.completed`, `multipart.aborted`, `part.uploaded`), Event dataclass (id, type, timestamp, source, request_id, data) |
| `src/bleepstore/queue/redis_backend.py` | Redis Streams implementation using `redis[hiredis]`: `XADD`, `XREADGROUP`, `XACK` for publish/subscribe/acknowledge. Dead letter stream for failed messages after max retries. |
| `src/bleepstore/config.py` | Add `queue` config section: `enabled`, `backend`, `consistency`, `redis.url`, `redis.stream_prefix`, `redis.consumer_group` |
| `src/bleepstore/server.py` | Initialize queue backend on startup, reconnect/reprocess pending on restart. Health check includes queue connectivity status. |
| All handler files | Publish events after successful writes (write-through mode). |

### Key patterns

- **redis** Streams:
  ```python
  r = redis.asyncio.from_url(url)
  await r.xadd("bleepstore:events", {"type": "object.created", "data": json.dumps(event_data)})
  ```
- **Write-through mode** (default): normal direct write path (storage + metadata), then publish event to queue (fire-and-forget). Queue failure does not block the write.
- **Crash-only**: startup reconnects to queue, reprocesses pending/unacknowledged tasks.
- Queue is entirely optional -- disabled by default, BleepStore works fine without it.

### Dependencies to add to pyproject.toml

| Package | Purpose |
|---|---|
| `redis[hiredis]` | Redis Streams (optional) |

### Unit test approach

- Event serialization/deserialization round-trip
- Redis backend: publish, subscribe, acknowledge, dead letter
- Write-through mode: event published after successful write

### Test targets

- All 75 E2E tests pass with Redis queue enabled (write-through mode)
- Events published for each write operation
- Queue unavailable at startup: BleepStore starts in degraded mode (logs warning)

### Definition of done

- QueueBackend interface defined
- Redis backend implemented (publish, subscribe, acknowledge, dead letter)
- Event types and envelope defined
- Write-through mode works: events published after successful writes
- All 75 E2E tests pass with Redis queue enabled (write-through mode)
- Configuration section for queue settings
- Health check reports queue status

---

## Stage 16b: RabbitMQ Backend

**Goal:** Implement the RabbitMQ/AMQP backend using the QueueBackend interface established in 16a.

### Files to create/modify

| File | Work |
|---|---|
| `src/bleepstore/queue/rabbitmq_backend.py` | RabbitMQ implementation using `aio-pika` (AMQP 0-9-1, ActiveMQ compatible). Topic exchange for event routing by type. Durable queues with manual ack. Dead letter exchange for failed messages. Automatic reconnection on connection loss. |

### Key patterns

- **aio-pika** for RabbitMQ:
  ```python
  connection = await aio_pika.connect_robust(url)
  channel = await connection.channel()
  exchange = await channel.declare_exchange("bleepstore", aio_pika.ExchangeType.TOPIC, durable=True)
  await exchange.publish(aio_pika.Message(body=json.dumps(event).encode()), routing_key="object.created")
  ```
- **Routing keys** based on event type (e.g., `bucket.created`, `object.deleted`).
- **Dead letter routing** for failed messages after max retries.
- Compatible with ActiveMQ via AMQP 0-9-1.
- Configuration: `queue.rabbitmq.url`, `queue.rabbitmq.exchange`, `queue.rabbitmq.queue_prefix`.

### Dependencies to add to pyproject.toml

| Package | Purpose |
|---|---|
| `aio-pika` | RabbitMQ/AMQP (optional) |

### Unit test approach

- RabbitMQ backend: publish, subscribe, acknowledge, dead letter
- Exchange and queue declaration

### Test targets

- All 75 E2E tests pass with RabbitMQ queue enabled (write-through mode)
- Events routed correctly by type

### Definition of done

- RabbitMQ backend implements full QueueBackend interface
- All 75 E2E tests pass with RabbitMQ queue enabled (write-through mode)
- Dead letter exchange handles failed messages
- Compatible with AMQP 0-9-1 (ActiveMQ compatible)

---

## Stage 16c: Kafka Backend & Consistency Modes

**Goal:** Implement the Kafka backend and the sync/async consistency modes. All three queue backends support all three consistency modes.

### Files to create/modify

| File | Work |
|---|---|
| `src/bleepstore/queue/kafka_backend.py` | Kafka implementation using `aiokafka`. Topics per event type. Consumer groups for parallel processing. `acks=all` for durability. Partitioned by bucket name for ordering within a bucket. |
| All handler files | Add sync/async consistency mode support (in addition to write-through from 16a). |

### Key patterns

- **aiokafka** for Kafka:
  ```python
  producer = AIOKafkaProducer(bootstrap_servers=brokers)
  await producer.start()
  await producer.send("bleepstore.object.created", json.dumps(event).encode())
  ```
- **Sync mode** (all backends): handler writes to temp file (fsync), enqueues WriteTask to queue, blocks waiting for consumer to complete task. Crash-safe: pending tasks survive in queue.
- **Async mode** (all backends): handler writes to temp file (fsync), enqueues WriteTask, responds 202 Accepted immediately. Consumer processes asynchronously. Clean up orphan temp files on startup.
- Configuration: `queue.kafka.brokers`, `queue.kafka.topic_prefix`, `queue.kafka.consumer_group`. `queue.consistency`: `write-through` (default), `sync`, `async`.

### Dependencies to add to pyproject.toml

| Package | Purpose |
|---|---|
| `aiokafka` | Apache Kafka (optional) |

### Unit test approach

- Kafka backend: publish, subscribe, acknowledge
- Sync mode: handler blocks until task completed
- Async mode: handler returns 202, task processed asynchronously

### Test targets

- All 75 E2E tests pass with Kafka queue enabled (write-through mode)
- Kill BleepStore mid-operation, restart, verify pending tasks reprocessed
- Sync mode: write completes only after consumer processes task
- Async mode: write returns 202, object eventually available

### Definition of done

- Kafka backend implements full QueueBackend interface
- Sync mode: writes blocked until queue consumer completes (all backends)
- Async mode: writes return 202, processed asynchronously (all backends)
- All 75 E2E tests pass with Kafka queue enabled (write-through mode)
- Crash-only: pending tasks survive restarts, orphan temp files cleaned
- All three backends support all three consistency modes

---

## File inventory

All source files in `python/src/bleepstore/` and which stages they are primarily modified in:

| File | Stages |
|---|---|
| `cli.py` | 1, 15 |
| `config.py` | 1, 10, 14, 16a |
| `server.py` | 1, 3, 4, 6, 7, 13a, 14, 15, 16a |
| `metrics.py` | 1b (new) |
| `errors.py` | 1 (mostly complete) |
| `xml_utils.py` | 1, 3, 5a, 7, 8 |
| `auth.py` | 6, 15 |
| `handlers/__init__.py` | -- |
| `handlers/bucket.py` | 3 |
| `handlers/object.py` | 4, 5a, 5b, 8, 15 |
| `handlers/multipart.py` | 7, 8 |
| `handlers/acl.py` | 3, 5b (new) |
| `metadata/store.py` | 2 |
| `metadata/sqlite.py` | 2, 7 |
| `metadata/models.py` | 2 (new) |
| `metadata/raft_store.py` | 13a (new) |
| `storage/backend.py` | -- (protocol, already complete) |
| `storage/local.py` | 4, 7, 8, 15 |
| `storage/aws.py` | 10 |
| `storage/gcp.py` | 11a |
| `storage/azure.py` | 11b |
| `cluster/raft.py` | 12a, 12b, 13b |
| `cluster/transport.py` | 12b, 13b (new) |
| `cluster/log.py` | 12a (new) |
| `cluster/state.py` | 12a (new) |
| `cluster/state_machine.py` | 13a (new) |
| `cluster/admin.py` | 14 (new) |
| `queue/__init__.py` | 16a (new) |
| `queue/backend.py` | 16a (new) |
| `queue/events.py` | 16a (new) |
| `queue/redis_backend.py` | 16a (new) |
| `queue/rabbitmq_backend.py` | 16b (new) |
| `queue/kafka_backend.py` | 16c (new) |
