"""FastAPI application factory and route setup for BleepStore."""

import base64
import email.utils
import hashlib
import logging
import secrets
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from prometheus_fastapi_instrumentator import Instrumentator

from bleepstore.auth import SigV4Authenticator
from bleepstore.config import BleepStoreConfig
from bleepstore.errors import NotImplementedS3Error, S3Error
from bleepstore.handlers.bucket import BucketHandler
from bleepstore.handlers.multipart import MultipartHandler
from bleepstore.handlers.object import ObjectHandler
from bleepstore.metadata.sqlite import SQLiteMetadataStore
from bleepstore.storage.backend import StorageBackend
from bleepstore.storage.local import LocalStorageBackend
from bleepstore.xml_utils import render_error, xml_response

# Ensure custom metrics are registered in the Prometheus default registry on
# import.  The metrics module defines module-level Counter/Gauge/Histogram
# objects that are created when first imported.
import bleepstore.metrics as _metrics  # noqa: F401

logger = logging.getLogger(__name__)

# Module-level singleton so multiple create_app() calls (e.g. in tests)
# don't re-register the same Prometheus gauge in the global registry.
_instrumentator: Instrumentator | None = None


def _get_instrumentator() -> Instrumentator:
    global _instrumentator
    if _instrumentator is None:
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

        Crash-only design: every startup is a recovery. Open SQLite (WAL
        auto-recovers), initialize storage (clean temp files), seed credentials.
        """
        # Initialize metadata store
        metadata = SQLiteMetadataStore(config.metadata.sqlite_path)
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
        logger.info("Metadata store initialized, credentials seeded")
        logger.info("Storage backend initialized: %s", config.storage.backend)

        yield

        # Shutdown: close storage and metadata
        await storage.close()
        await metadata.close()
        logger.info("Metadata store and storage backend closed")

    app = FastAPI(title="BleepStore S3 API", version="0.1.0", lifespan=lifespan)
    app.state.config = config

    # Register exception handler for S3Error
    _register_exception_handlers(app)

    # Register middleware for common headers
    _register_middleware(app)

    # Wire Prometheus metrics instrumentation BEFORE S3 routes so /metrics
    # is registered first and not shadowed by the /{bucket} catch-all.
    _get_instrumentator().instrument(app).expose(app, endpoint="/metrics")

    # Register all routes (S3 catch-all routes like /{bucket} must come after
    # fixed routes like /health and /metrics)
    _setup_routes(app)

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


def _register_middleware(app: FastAPI) -> None:
    """Register middleware on the FastAPI app.

    In FastAPI, middleware is registered in reverse order (last registered
    runs first). We register common_headers first, then auth, so the
    execution order is: common_headers -> auth -> handler.
    """

    # Paths that skip auth -- health, metrics, docs, openapi
    AUTH_SKIP_PATHS = {
        "/health",
        "/metrics",
        "/docs",
        "/docs/oauth2-redirect",
        "/openapi.json",
        "/redoc",
    }

    @app.middleware("http")
    async def common_headers_middleware(request: Request, call_next) -> Response:
        """Add common S3 response headers to every response.

        Generates x-amz-request-id (16-char uppercase hex), x-amz-id-2 (base64),
        Date (RFC 1123), and Server header. Stores request_id on request.state
        so exception handlers can use it.
        """
        request_id = secrets.token_hex(8).upper()
        request.state.request_id = request_id

        response = await call_next(request)

        response.headers["x-amz-request-id"] = request_id
        response.headers["x-amz-id-2"] = base64.b64encode(secrets.token_bytes(24)).decode()
        response.headers["Date"] = email.utils.formatdate(usegmt=True)
        response.headers["Server"] = "BleepStore"
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
# Route handlers
# ---------------------------------------------------------------------------


def _setup_routes(app: FastAPI) -> None:
    """Register all S3-compatible routes on the application.

    Bucket operations are wired to BucketHandler. Object operations for
    PutObject, GetObject, HeadObject, and DeleteObject are wired to
    ObjectHandler. Remaining operations still return 501 NotImplemented.

    Args:
        app: The FastAPI application to attach routes to.
    """
    bucket_handler = BucketHandler(app)
    object_handler = ObjectHandler(app)
    multipart_handler = MultipartHandler(app)

    # Health check (non-S3)
    @app.get("/health")
    async def health_check() -> dict:
        """Return health status.

        GET /health -> {"status": "ok"}
        """
        return {"status": "ok"}

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
