"""Shared pytest fixtures for BleepStore tests.

A single FastAPI app is created per test session to avoid duplicate
Prometheus metric registration errors (the instrumentator registers
gauges in the global prometheus_client registry).

The metadata store and storage backend are manually initialized on the
app to avoid needing to run the full lifespan (which requires asyncio
at session scope).
"""

import hashlib

import pytest
from httpx import ASGITransport, AsyncClient

from bleepstore.config import (
    AuthConfig,
    BleepStoreConfig,
    MetadataConfig,
    ServerConfig,
    StorageConfig,
)
from bleepstore.metadata.sqlite import SQLiteMetadataStore
from bleepstore.server import create_app
from bleepstore.storage.local import LocalStorageBackend


@pytest.fixture(scope="session")
def config() -> BleepStoreConfig:
    """Create a test BleepStoreConfig with auth disabled.

    Auth is disabled for existing tests that don't sign requests.
    The test_auth.py module creates its own config with auth enabled.
    """
    return BleepStoreConfig(
        server=ServerConfig(host="127.0.0.1", port=9010, region="us-east-1"),
        auth=AuthConfig(access_key="test", secret_key="test-secret", enabled=False),
        metadata=MetadataConfig(engine="sqlite", sqlite_path=":memory:"),
        storage=StorageConfig(backend="local", local_root="/tmp/bleepstore-test"),
    )


@pytest.fixture(scope="session")
def app(config: BleepStoreConfig):
    """Create a single test FastAPI application for the whole session."""
    return create_app(config)


@pytest.fixture
async def client(app, config, tmp_path) -> AsyncClient:
    """Create an async test client for the BleepStore app.

    Also ensures the metadata store and storage backend are initialized
    on app.state before each test (the lifespan context doesn't auto-run
    with ASGITransport).
    """
    # Initialize metadata store if not already done
    if not hasattr(app.state, "metadata") or app.state.metadata is None:
        metadata = SQLiteMetadataStore(":memory:")
        await metadata.init_db()
        app.state.metadata = metadata

        # Seed default credentials (mirrors lifespan behavior)
        access_key = config.auth.access_key
        secret_key = config.auth.secret_key
        owner_id = hashlib.sha256(access_key.encode()).hexdigest()[:32]
        await metadata.put_credential(
            access_key_id=access_key,
            secret_key=secret_key,
            owner_id=owner_id,
            display_name=access_key,
        )

    # Initialize storage backend if not already done
    if not hasattr(app.state, "storage") or app.state.storage is None:
        storage = LocalStorageBackend(str(tmp_path / "objects"))
        await storage.init()
        app.state.storage = storage

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
