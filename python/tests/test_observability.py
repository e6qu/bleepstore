"""Tests for observability spec conformance (metrics, health probes, config)."""

import hashlib
import tempfile
from pathlib import Path

import yaml
from httpx import ASGITransport, AsyncClient

from bleepstore.config import (
    AuthConfig,
    BleepStoreConfig,
    MetadataConfig,
    ObservabilityConfig,
    ServerConfig,
    StorageConfig,
    load_config,
)
from bleepstore.metadata.sqlite import SQLiteMetadataStore
from bleepstore.server import create_app
from bleepstore.storage.local import LocalStorageBackend


# ---------------------------------------------------------------------------
# Helpers: create a test client for a custom config
# ---------------------------------------------------------------------------


async def _make_client(config: BleepStoreConfig, tmp_path: Path):
    """Create an initialised AsyncClient for the given config."""
    app = create_app(config)

    # Initialise metadata + storage on app.state (mirrors conftest.py)
    metadata = SQLiteMetadataStore(":memory:")
    await metadata.init_db()
    app.state.metadata = metadata

    access_key = config.auth.access_key
    secret_key = config.auth.secret_key
    owner_id = hashlib.sha256(access_key.encode()).hexdigest()[:32]
    await metadata.put_credential(
        access_key_id=access_key,
        secret_key=secret_key,
        owner_id=owner_id,
        display_name=access_key,
    )

    storage = LocalStorageBackend(str(tmp_path / "objects"))
    await storage.init()
    app.state.storage = storage

    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://testserver")


def _base_config(**overrides) -> BleepStoreConfig:
    """Return a minimal test config with optional overrides."""
    kwargs = dict(
        server=ServerConfig(host="127.0.0.1", port=9010, region="us-east-1"),
        auth=AuthConfig(access_key="test", secret_key="test-secret", enabled=False),
        metadata=MetadataConfig(engine="sqlite", sqlite_path=":memory:"),
        storage=StorageConfig(backend="local", local_root="/tmp/bleepstore-test-obs"),
    )
    kwargs.update(overrides)
    return BleepStoreConfig(**kwargs)


# ===================================================================
# Metrics endpoint tests (default config has metrics=True)
# ===================================================================


class TestMetricsNamespace:
    """The instrumentator must emit bleepstore_http_requests_total."""

    async def test_bleepstore_http_requests_total_present(self, client):
        """bleepstore_http_requests_total is present in /metrics output."""
        # Make a request first so the counter is populated
        await client.get("/health")
        resp = await client.get("/metrics")
        assert resp.status_code == 200
        assert "bleepstore_http_requests_total" in resp.text


class TestMetricsDisabled:
    """When observability.metrics is false, /metrics returns 404."""

    async def test_metrics_returns_404_when_disabled(self, tmp_path):
        """With metrics disabled, GET /metrics returns 404."""
        config = _base_config(observability=ObservabilityConfig(metrics=False))
        async with await _make_client(config, tmp_path) as client:
            resp = await client.get("/metrics")
            assert resp.status_code == 404


# ===================================================================
# Liveness probe: /healthz
# ===================================================================


class TestHealthzEndpoint:
    """Tests for the /healthz liveness probe."""

    async def test_healthz_returns_200(self, client):
        """/healthz returns 200 with empty body."""
        resp = await client.get("/healthz")
        assert resp.status_code == 200
        assert resp.content == b""

    async def test_healthz_returns_404_when_disabled(self, tmp_path):
        """/healthz returns 404 when health_check is disabled."""
        config = _base_config(
            observability=ObservabilityConfig(metrics=False, health_check=False)
        )
        async with await _make_client(config, tmp_path) as client:
            resp = await client.get("/healthz")
            assert resp.status_code == 404


# ===================================================================
# Readiness probe: /readyz
# ===================================================================


class TestReadyzEndpoint:
    """Tests for the /readyz readiness probe."""

    async def test_readyz_returns_200(self, client):
        """/readyz returns 200 with empty body when all checks pass."""
        resp = await client.get("/readyz")
        assert resp.status_code == 200
        assert resp.content == b""

    async def test_readyz_returns_503_on_metadata_failure(self, client, app):
        """/readyz returns 503 when metadata store check fails."""
        original_db = app.state.metadata._db

        # Simulate a broken metadata connection
        app.state.metadata._db = None
        try:
            resp = await client.get("/readyz")
            assert resp.status_code == 503
            assert resp.content == b""
        finally:
            app.state.metadata._db = original_db

    async def test_readyz_returns_404_when_disabled(self, tmp_path):
        """/readyz returns 404 when health_check is disabled."""
        config = _base_config(
            observability=ObservabilityConfig(metrics=False, health_check=False)
        )
        async with await _make_client(config, tmp_path) as client:
            resp = await client.get("/readyz")
            assert resp.status_code == 404


# ===================================================================
# Enhanced /health endpoint
# ===================================================================


class TestHealthEndpoint:
    """Tests for the enhanced /health endpoint with component checks."""

    async def test_health_returns_json_with_checks(self, client):
        """/health returns JSON with checks key containing metadata and storage."""
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "checks" in data
        assert "metadata" in data["checks"]
        assert "storage" in data["checks"]
        assert data["checks"]["metadata"]["status"] == "ok"
        assert "latency_ms" in data["checks"]["metadata"]
        assert data["checks"]["storage"]["status"] == "ok"
        assert "latency_ms" in data["checks"]["storage"]

    async def test_health_returns_degraded_on_metadata_failure(self, client, app):
        """/health returns 503 with degraded status when metadata fails."""
        original_db = app.state.metadata._db

        app.state.metadata._db = None
        try:
            resp = await client.get("/health")
            assert resp.status_code == 503
            data = resp.json()
            assert data["status"] == "degraded"
            assert data["checks"]["metadata"]["status"] == "error"
        finally:
            app.state.metadata._db = original_db

    async def test_health_static_when_health_check_disabled(self, tmp_path):
        """/health returns static {"status":"ok"} when health_check is disabled."""
        config = _base_config(
            observability=ObservabilityConfig(metrics=False, health_check=False)
        )
        async with await _make_client(config, tmp_path) as client:
            resp = await client.get("/health")
            assert resp.status_code == 200
            data = resp.json()
            assert data == {"status": "ok"}


# ===================================================================
# Config parsing
# ===================================================================


class TestObservabilityConfig:
    """Tests for observability config section parsing."""

    def test_defaults(self):
        """ObservabilityConfig defaults to metrics=True, health_check=True."""
        config = BleepStoreConfig()
        assert config.observability.metrics is True
        assert config.observability.health_check is True

    def test_yaml_parsing_enabled(self):
        """observability section is parsed from YAML correctly (enabled)."""
        data = {
            "observability": {
                "metrics": True,
                "health_check": True,
            }
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(data, f)
            f.flush()
            config = load_config(Path(f.name))
        assert config.observability.metrics is True
        assert config.observability.health_check is True

    def test_yaml_parsing_disabled(self):
        """observability section is parsed from YAML correctly (disabled)."""
        data = {
            "observability": {
                "metrics": False,
                "health_check": False,
            }
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(data, f)
            f.flush()
            config = load_config(Path(f.name))
        assert config.observability.metrics is False
        assert config.observability.health_check is False

    def test_yaml_missing_observability_uses_defaults(self):
        """Missing observability section in YAML uses defaults."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({}, f)
            f.flush()
            config = load_config(Path(f.name))
        assert config.observability.metrics is True
        assert config.observability.health_check is True
