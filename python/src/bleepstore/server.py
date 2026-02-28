"""FastAPI application factory and route setup for BleepStore."""

import base64
import email.utils
import hashlib
import json
import logging
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse

from bleepstore.auth import SigV4Authenticator
from bleepstore.config import BleepStoreConfig
from bleepstore.errors import NotImplementedS3Error, S3Error
from bleepstore.handlers.bucket import BucketHandler
from bleepstore.handlers.multipart import MultipartHandler
from bleepstore.handlers.object import ObjectHandler
from bleepstore.metadata import create_metadata_store
from bleepstore.storage.backend import StorageBackend
from bleepstore.storage.local import LocalStorageBackend
from bleepstore.xml_utils import render_error, xml_response

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Canonical OpenAPI spec loader
# ---------------------------------------------------------------------------

_CANONICAL_SPEC_PATH = Path(__file__).resolve().parents[3] / "schemas" / "s3-api.openapi.json"


def _load_openapi_spec(port: int = 9000) -> dict:
    """Load the canonical OpenAPI spec from schemas/s3-api.openapi.json.

    Patches the ``servers`` array to point at the local BleepStore instance.

    Args:
        port: The port the server is listening on (used for the servers URL).

    Returns:
        The parsed and patched OpenAPI spec as a dict.
    """
    with open(_CANONICAL_SPEC_PATH) as f:
        spec = json.load(f)
    spec["servers"] = [
        {
            "url": f"http://localhost:{port}",
            "description": "BleepStore Python",
        }
    ]
    return spec


# Module-level singleton so multiple create_app() calls (e.g. in tests)
# don't re-register the same Prometheus gauge in the global registry.
_instrumentator = None


def _get_instrumentator():
    global _instrumentator
    if _instrumentator is None:
        from prometheus_fastapi_instrumentator import Instrumentator

        _instrumentator = Instrumentator(
            should_instrument_requests_inprogress=True,
            excluded_handlers=["/metrics"],
        )
    return _instrumentator


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app(config: BleepStoreConfig) -> FastAPI:
    """Create and configure the BleepStore FastAPI application.

    Middleware and exception handlers ensure common headers are applied to
    ALL responses (including error responses), and S3Error exceptions are
    rendered as proper S3 error XML.

    The lifespan context manager initializes the metadata store and storage
    backend on startup and closes them on shutdown (crash-only: every startup
    is recovery).

    Args:
        config: The loaded BleepStore configuration.

    Returns:
        A configured FastAPI application ready to run.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Lifespan hook: initialize metadata store, storage backend,
        and seed credentials.

        Crash-only design: every startup is a recovery. Open metadata store,
        initialize storage (clean temp files), seed credentials.
        """
        metadata = create_metadata_store(config.metadata)
        await metadata.init_db()
        app.state.metadata = metadata

        # Initialize storage backend (factory pattern)
        storage = _create_storage_backend(config)
        await storage.init()
        app.state.storage = storage

        # Seed default credentials (crash-only: idempotent upsert)
        access_key = config.auth.access_key
        secret_key = config.auth.secret_key
        owner_id = hashlib.sha256(access_key.encode()).hexdigest()[:32]
        await metadata.put_credential(
            access_key_id=access_key,
            secret_key=secret_key,
            owner_id=owner_id,
            display_name=access_key,
        )
        # Create SigV4 authenticator once so signing-key cache persists
        app.state.authenticator = SigV4Authenticator(
            metadata=metadata,
            region=config.server.region,
        )

        # Reap expired multipart uploads (crash-only: clean up on startup)
        reaped_uploads = await metadata.reap_expired_uploads()
        if reaped_uploads:
            # Clean up storage files for reaped uploads
            for upload in reaped_uploads:
                try:
                    if hasattr(storage, "delete_upload_parts"):
                        await storage.delete_upload_parts(upload["upload_id"])
                    else:
                        await storage.delete_parts(
                            upload["bucket"], upload["key"], upload["upload_id"]
                        )
                except Exception:
                    logger.warning(
                        "Failed to clean storage for reaped upload %s",
                        upload["upload_id"],
                    )
            logger.info("Reaped %d expired multipart uploads", len(reaped_uploads))

        # Load canonical OpenAPI spec with patched server URL
        try:
            app.state.openapi_spec = _load_openapi_spec(port=config.server.port)
            logger.info("Loaded canonical OpenAPI spec from %s", _CANONICAL_SPEC_PATH)
        except FileNotFoundError:
            logger.warning("Canonical OpenAPI spec not found at %s", _CANONICAL_SPEC_PATH)
            app.state.openapi_spec = None

        logger.info("Metadata store initialized, credentials seeded")
        logger.info("Storage backend initialized: %s", config.storage.backend)

        yield

        # Shutdown: close storage and metadata
        await storage.close()
        await metadata.close()
        logger.info("Metadata store and storage backend closed")

    app = FastAPI(
        title="BleepStore S3 API",
        version="0.1.0",
        lifespan=lifespan,
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
    )
    app.state.config = config

    # Register exception handler for S3Error
    _register_exception_handlers(app)

    # Register middleware for common headers
    _register_middleware(app, config)

    # Wire Prometheus metrics instrumentation BEFORE S3 routes so /metrics
    # is registered first and not shadowed by the /{bucket} catch-all.
    if config.observability.metrics:
        import bleepstore.metrics as _metrics

        _metrics.init_metrics()
        _get_instrumentator().instrument(app, metric_namespace="bleepstore").expose(
            app, endpoint="/metrics"
        )

    # Register all routes (S3 catch-all routes like /{bucket} must come after
    # fixed routes like /health and /metrics)
    _setup_routes(app, config)

    return app


def _create_storage_backend(config: BleepStoreConfig) -> StorageBackend:
    """Create a storage backend instance based on configuration.

    Supports 'local', 'aws', 'gcp', and 'azure' backends.

    Args:
        config: The BleepStore configuration.

    Returns:
        A storage backend instance.
    """
    backend = config.storage.backend
    if backend == "local":
        return LocalStorageBackend(config.storage.local_root)
    elif backend == "memory":
        from bleepstore.storage.memory import MemoryStorageBackend

        return MemoryStorageBackend(
            max_size_bytes=config.storage.memory_max_size_bytes,
            persistence=config.storage.memory_persistence,
            snapshot_path=config.storage.memory_snapshot_path,
            snapshot_interval_seconds=config.storage.memory_snapshot_interval_seconds,
        )
    elif backend == "sqlite":
        from bleepstore.storage.sqlite import SQLiteStorageBackend

        return SQLiteStorageBackend(db_path=config.metadata.sqlite.path)
    elif backend == "aws":
        if not config.storage.aws_bucket:
            raise ValueError("storage.aws.bucket is required when backend is 'aws'")
        try:
            from bleepstore.storage.aws import AWSGatewayBackend
        except ImportError as exc:
            raise ImportError(
                "aiobotocore is required for the AWS backend. "
                "Install with: pip install bleepstore[aws]"
            ) from exc
        return AWSGatewayBackend(
            bucket_name=config.storage.aws_bucket,
            region=config.storage.aws_region,
            prefix=config.storage.aws_prefix,
            endpoint_url=config.storage.aws_endpoint_url,
            use_path_style=config.storage.aws_use_path_style,
            access_key_id=config.storage.aws_access_key_id,
            secret_access_key=config.storage.aws_secret_access_key,
        )
    elif backend == "gcp":
        if not config.storage.gcp_bucket:
            raise ValueError("storage.gcp.bucket is required when backend is 'gcp'")
        try:
            from bleepstore.storage.gcp import GCPGatewayBackend
        except ImportError as exc:
            raise ImportError(
                "gcloud-aio-storage is required for the GCP backend. "
                "Install with: pip install bleepstore[gcp]"
            ) from exc
        return GCPGatewayBackend(
            bucket_name=config.storage.gcp_bucket,
            project=config.storage.gcp_project,
            prefix=config.storage.gcp_prefix,
            credentials_file=config.storage.gcp_credentials_file,
        )
    elif backend == "azure":
        if not config.storage.azure_container:
            raise ValueError("storage.azure.container is required when backend is 'azure'")
        try:
            from bleepstore.storage.azure import AzureGatewayBackend
        except ImportError as exc:
            raise ImportError(
                "azure-storage-blob and azure-identity are required for the Azure backend. "
                "Install with: pip install bleepstore[azure]"
            ) from exc
        return AzureGatewayBackend(
            container_name=config.storage.azure_container,
            account_url=config.storage.azure_account,
            prefix=config.storage.azure_prefix,
            connection_string=config.storage.azure_connection_string,
            use_managed_identity=config.storage.azure_use_managed_identity,
        )
    else:
        raise ValueError(f"Unknown storage backend: {backend}")


# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------


def _register_exception_handlers(app: FastAPI) -> None:
    """Register exception handlers on the FastAPI app."""

    @app.exception_handler(S3Error)
    async def s3_error_handler(request: Request, exc: S3Error) -> Response:
        """Catch S3Error exceptions and render proper S3 error XML responses.

        HEAD requests must not have a body.
        """
        request_id = getattr(request.state, "request_id", "")

        # HEAD requests must not have a body
        if request.method == "HEAD":
            return Response(status_code=exc.http_status)

        body = render_error(
            code=exc.code,
            message=exc.message,
            resource=request.url.path,
            request_id=request_id,
            extra_fields=exc.extra_fields,
        )
        return xml_response(body, status=exc.http_status)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, exc: RequestValidationError) -> Response:
        """Map Pydantic / FastAPI validation errors to S3 error XML.

        Instead of returning the default JSON validation error, produce an
        S3-compatible XML error response with code ``InvalidArgument``.
        """
        request_id = getattr(request.state, "request_id", "")

        # Build a human-readable summary from Pydantic error details.
        messages = []
        for err in exc.errors():
            loc = " -> ".join(str(p) for p in err.get("loc", []))
            msg = err.get("msg", "Invalid value")
            messages.append(f"{loc}: {msg}" if loc else msg)
        combined = "; ".join(messages) or "Invalid request parameters"

        if request.method == "HEAD":
            return Response(status_code=400)

        body = render_error(
            code="InvalidArgument",
            message=combined,
            resource=request.url.path,
            request_id=request_id,
        )
        return xml_response(body, status=400)

    @app.exception_handler(Exception)
    async def generic_error_handler(request: Request, exc: Exception) -> Response:
        """Catch unexpected exceptions and return InternalError."""
        logger.exception("Unhandled exception in request handler")
        request_id = getattr(request.state, "request_id", "")

        if request.method == "HEAD":
            return Response(status_code=500)

        body = render_error(
            code="InternalError",
            message="We encountered an internal error. Please try again.",
            resource=request.url.path,
            request_id=request_id,
        )
        return xml_response(body, status=500)


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


def _register_middleware(app: FastAPI, config: BleepStoreConfig) -> None:
    """Register middleware on the FastAPI app.

    In FastAPI, middleware is registered in reverse order (last registered
    runs first). We register common_headers first, then auth, so the
    execution order is: common_headers -> auth -> handler.
    """

    # Paths that skip auth -- health, metrics, docs, openapi
    AUTH_SKIP_PATHS = {
        "/health",
        "/healthz",
        "/readyz",
        "/metrics",
        "/docs",
        "/openapi.json",
    }

    # Paths to suppress from per-request logging
    _QUIET_PATHS = {"/metrics", "/health", "/healthz", "/readyz"}

    metrics_enabled = config.observability.metrics

    @app.middleware("http")
    async def common_headers_middleware(request: Request, call_next) -> Response:
        """Add common S3 response headers to every response.

        Generates x-amz-request-id (16-char uppercase hex), x-amz-id-2 (base64),
        Date (RFC 1123), and Server header. Stores request_id on request.state
        so exception handlers can use it.

        When metrics are enabled, also observes request/response body sizes in
        the size histograms and increments byte counters.
        """
        request_id = secrets.token_hex(8).upper()
        request.state.request_id = request_id
        start = time.monotonic()

        response = await call_next(request)

        duration_ms = round((time.monotonic() - start) * 1000, 2)

        response.headers["x-amz-request-id"] = request_id
        response.headers["x-amz-id-2"] = base64.b64encode(secrets.token_bytes(24)).decode()
        response.headers["Date"] = email.utils.formatdate(usegmt=True)
        response.headers["Server"] = "BleepStore"

        # Track byte counters when enabled (best-effort, never block request).
        # Size histograms (http_request_size_bytes, http_response_size_bytes)
        # are handled by the prometheus-fastapi-instrumentator middleware.
        if metrics_enabled:
            try:
                import bleepstore.metrics as _m

                # Request bytes from Content-Length header
                req_size = 0
                cl = request.headers.get("content-length")
                if cl:
                    try:
                        req_size = int(cl)
                    except (ValueError, TypeError):
                        pass
                if req_size > 0 and _m.bytes_received_total is not None:
                    _m.bytes_received_total.inc(req_size)

                # Response bytes from Content-Length header
                resp_size = 0
                rcl = response.headers.get("content-length")
                if rcl:
                    try:
                        resp_size = int(rcl)
                    except (ValueError, TypeError):
                        pass
                if resp_size > 0 and _m.bytes_sent_total is not None:
                    _m.bytes_sent_total.inc(resp_size)
            except Exception:
                pass  # Best-effort: never block a request for metrics

        # Per-request structured log (skip noisy endpoints)
        if request.url.path not in _QUIET_PATHS:
            logger.info(
                "%s %s %d %.2fms",
                request.method,
                request.url.path,
                response.status_code,
                duration_ms,
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "status": response.status_code,
                    "duration_ms": duration_ms,
                    "request_id": request_id,
                },
            )

        return response

    @app.middleware("http")
    async def auth_middleware(request: Request, call_next) -> Response:
        """SigV4 authentication middleware.

        Runs before handlers but after common-headers middleware.
        Skips auth for non-S3 endpoints (/health, /metrics, /docs, /openapi.json).
        When auth is disabled in config, all requests pass through.

        On success, stores credential info on request.state for handlers to use.
        On failure, catches S3Error and returns S3 error XML directly (since
        FastAPI exception handlers do not catch exceptions from middleware).
        """
        cfg: BleepStoreConfig = app.state.config

        # Skip auth for non-S3 endpoints
        if request.url.path in AUTH_SKIP_PATHS:
            return await call_next(request)

        # Skip auth if disabled in config
        if not cfg.auth.enabled:
            return await call_next(request)

        # Skip auth if no metadata store is available yet (e.g. during testing
        # when lifespan hasn't run)
        metadata = getattr(app.state, "metadata", None)
        if metadata is None:
            return await call_next(request)

        # Perform SigV4 verification -- catch auth errors and render as S3 XML
        try:
            authenticator = getattr(app.state, "authenticator", None)
            if authenticator is None:
                # Fallback: create per-request (shouldn't happen in production)
                authenticator = SigV4Authenticator(
                    metadata=metadata,
                    region=cfg.server.region,
                )
            credential_info = await authenticator.verify_request(request)
        except S3Error as exc:
            request_id = getattr(request.state, "request_id", "")
            if request.method == "HEAD":
                return Response(status_code=exc.http_status)
            body = render_error(
                code=exc.code,
                message=exc.message,
                resource=request.url.path,
                request_id=request_id,
                extra_fields=exc.extra_fields,
            )
            return xml_response(body, status=exc.http_status)

        # Store credential info on request.state for handlers
        request.state.access_key = credential_info["access_key"]
        request.state.owner_id = credential_info["owner_id"]
        request.state.display_name = credential_info["display_name"]

        return await call_next(request)


# ---------------------------------------------------------------------------
# Health check helpers
# ---------------------------------------------------------------------------


async def _check_metadata(app: FastAPI) -> dict:
    """Probe the metadata store with ``SELECT 1``.

    Returns a dict with ``status`` and ``latency_ms`` keys.
    """
    metadata = getattr(app.state, "metadata", None)
    if metadata is None:
        return {"status": "error", "error": "metadata store not initialized", "latency_ms": 0}
    try:
        start = time.monotonic()
        db = metadata._db
        if db is None:
            return {"status": "error", "error": "database connection closed", "latency_ms": 0}
        async with db.execute("SELECT 1") as cursor:
            await cursor.fetchone()
        latency = round((time.monotonic() - start) * 1000, 1)
        return {"status": "ok", "latency_ms": latency}
    except Exception as exc:
        return {"status": "error", "error": str(exc), "latency_ms": 0}


async def _check_storage(app: FastAPI) -> dict:
    """Probe the storage backend (check root directory exists).

    Returns a dict with ``status`` and ``latency_ms`` keys.
    """
    storage = getattr(app.state, "storage", None)
    if storage is None:
        return {"status": "error", "error": "storage backend not initialized", "latency_ms": 0}
    try:
        start = time.monotonic()
        root = getattr(storage, "root", None)
        if root is not None:
            if not Path(root).is_dir():
                return {
                    "status": "error",
                    "error": "data directory not found",
                    "latency_ms": round((time.monotonic() - start) * 1000, 1),
                }
        latency = round((time.monotonic() - start) * 1000, 1)
        return {"status": "ok", "latency_ms": latency}
    except Exception as exc:
        return {"status": "error", "error": str(exc), "latency_ms": 0}


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------


def _setup_routes(app: FastAPI, config: BleepStoreConfig) -> None:
    """Register all S3-compatible routes on the application.

    Bucket operations are wired to BucketHandler. Object operations for
    PutObject, GetObject, HeadObject, and DeleteObject are wired to
    ObjectHandler. Remaining operations still return 501 NotImplemented.

    Args:
        app: The FastAPI application to attach routes to.
        config: The BleepStore configuration.
    """
    bucket_handler = BucketHandler(app)
    object_handler = ObjectHandler(app)
    multipart_handler = MultipartHandler(app)

    health_check_enabled = config.observability.health_check

    # Health check (non-S3) -- always served, behaviour depends on config
    @app.get("/health")
    async def health_check(request: Request) -> Response:
        """Return health status.

        When health_check is enabled: deep-probe metadata and storage,
        return JSON with component checks and latency_ms.
        When disabled: return static ``{"status": "ok"}``.
        """
        if not health_check_enabled:
            return Response(
                content='{"status":"ok"}',
                media_type="application/json",
            )

        meta_check = await _check_metadata(app)
        storage_check = await _check_storage(app)
        all_ok = meta_check["status"] == "ok" and storage_check["status"] == "ok"

        body = json.dumps(
            {
                "status": "ok" if all_ok else "degraded",
                "checks": {
                    "metadata": meta_check,
                    "storage": storage_check,
                },
            }
        )
        return Response(
            content=body,
            status_code=200 if all_ok else 503,
            media_type="application/json",
        )

    # Kubernetes liveness probe
    if health_check_enabled:

        @app.get("/healthz")
        async def healthz() -> Response:
            """Liveness probe. Returns 200 with empty body."""
            return Response(status_code=200)

        @app.get("/readyz")
        async def readyz() -> Response:
            """Readiness probe. Probes metadata and storage.

            Returns 200 (empty) if all pass, 503 (empty) if any fail.
            """
            meta_check = await _check_metadata(app)
            storage_check = await _check_storage(app)
            all_ok = meta_check["status"] == "ok" and storage_check["status"] == "ok"
            return Response(status_code=200 if all_ok else 503)

    # OpenAPI spec and Swagger UI (canonical spec from schemas/)
    @app.get("/openapi.json")
    async def openapi_json() -> Response:
        """Serve the canonical OpenAPI spec (patched with local server URL)."""
        spec = getattr(app.state, "openapi_spec", None)
        if spec is None:
            # Lazy-load if lifespan didn't run (e.g. tests without lifespan)
            spec = _load_openapi_spec(port=config.server.port)
            app.state.openapi_spec = spec
        return JSONResponse(content=spec)

    @app.get("/docs")
    async def swagger_ui() -> Response:
        """Serve Swagger UI pointing at the canonical OpenAPI spec."""
        html = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>BleepStore API - Swagger UI</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({ url: '/openapi.json', dom_id: '#swagger-ui',
      presets: [SwaggerUIBundle.presets.apis], layout: 'BaseLayout' });
  </script>
</body>
</html>"""
        return HTMLResponse(content=html)

    # Service-level
    @app.get("/")
    async def handle_service_get(request: Request) -> Response:
        """Handle GET / -- ListBuckets."""
        return await bucket_handler.list_buckets(request)

    # Bucket-level routes
    @app.put("/{bucket}")
    async def handle_bucket_put(bucket: str, request: Request) -> Response:
        """Handle PUT /{bucket} -- dispatches by query params."""
        if "acl" in request.query_params:
            return await bucket_handler.put_bucket_acl(request, bucket)
        # CreateBucket
        return await bucket_handler.create_bucket(request, bucket)

    @app.delete("/{bucket}")
    async def handle_bucket_delete(bucket: str, request: Request) -> Response:
        """Handle DELETE /{bucket} -- DeleteBucket."""
        return await bucket_handler.delete_bucket(request, bucket)

    @app.head("/{bucket}")
    async def handle_bucket_head(bucket: str, request: Request) -> Response:
        """Handle HEAD /{bucket} -- HeadBucket."""
        return await bucket_handler.head_bucket(request, bucket)

    @app.get("/{bucket}")
    async def handle_bucket_get(bucket: str, request: Request) -> Response:
        """Handle GET /{bucket} -- dispatches by query params.

        ?location -> GetBucketLocation
        ?acl -> GetBucketAcl
        ?uploads -> ListMultipartUploads
        otherwise -> ListObjects (v1 or v2 depending on list-type param)
        """
        if "location" in request.query_params:
            return await bucket_handler.get_bucket_location(request, bucket)
        if "acl" in request.query_params:
            return await bucket_handler.get_bucket_acl(request, bucket)
        if "uploads" in request.query_params:
            return await multipart_handler.list_uploads(request, bucket)
        # ListObjects (v1 or v2 based on list-type param)
        return await object_handler.list_objects(request, bucket)

    @app.post("/{bucket}")
    async def handle_bucket_post(bucket: str, request: Request) -> Response:
        """Handle POST /{bucket} -- dispatches by query params.

        ?delete -> DeleteObjects (batch)
        """
        if "delete" in request.query_params:
            return await object_handler.delete_objects(request, bucket)
        raise NotImplementedS3Error()

    # Object-level routes (key can contain slashes via {key:path})
    @app.put("/{bucket}/{key:path}")
    async def handle_object_put(bucket: str, key: str, request: Request) -> Response:
        """Handle PUT /{bucket}/{key} -- dispatches by query params.

        ?uploadId&partNumber -> UploadPart
        ?acl -> PutObjectAcl
        x-amz-copy-source header -> CopyObject
        otherwise -> PutObject
        """
        if "uploadId" in request.query_params and "partNumber" in request.query_params:
            if "x-amz-copy-source" in request.headers:
                return await multipart_handler.upload_part_copy(request, bucket, key)
            return await multipart_handler.upload_part(request, bucket, key)
        if "acl" in request.query_params:
            return await object_handler.put_object_acl(request, bucket, key)
        if "x-amz-copy-source" in request.headers:
            return await object_handler.copy_object(request, bucket, key)
        return await object_handler.put_object(request, bucket, key)

    @app.head("/{bucket}/{key:path}")
    async def handle_object_head(bucket: str, key: str, request: Request) -> Response:
        """Handle HEAD /{bucket}/{key} -- HeadObject."""
        return await object_handler.head_object(request, bucket, key)

    @app.get("/{bucket}/{key:path}")
    async def handle_object_get(bucket: str, key: str, request: Request) -> Response:
        """Handle GET /{bucket}/{key} -- dispatches by query params.

        ?acl -> GetObjectAcl
        ?uploadId -> ListParts
        otherwise -> GetObject
        """
        if "acl" in request.query_params:
            return await object_handler.get_object_acl(request, bucket, key)
        if "uploadId" in request.query_params:
            return await multipart_handler.list_parts(request, bucket, key)
        return await object_handler.get_object(request, bucket, key)

    @app.delete("/{bucket}/{key:path}")
    async def handle_object_delete(bucket: str, key: str, request: Request) -> Response:
        """Handle DELETE /{bucket}/{key} -- dispatches by query params.

        ?uploadId -> AbortMultipartUpload
        otherwise -> DeleteObject
        """
        if "uploadId" in request.query_params:
            return await multipart_handler.abort_multipart_upload(request, bucket, key)
        return await object_handler.delete_object(request, bucket, key)

    @app.post("/{bucket}/{key:path}")
    async def handle_object_post(bucket: str, key: str, request: Request) -> Response:
        """Handle POST /{bucket}/{key} -- dispatches by query params.

        ?uploads -> CreateMultipartUpload
        ?uploadId -> CompleteMultipartUpload
        """
        if "uploads" in request.query_params:
            return await multipart_handler.create_multipart_upload(request, bucket, key)
        if "uploadId" in request.query_params:
            return await multipart_handler.complete_multipart_upload(request, bucket, key)
        raise NotImplementedS3Error()
