"""Tests for OpenAPI / Swagger UI endpoints (Stage 1b)."""


class TestSwaggerUI:
    """Tests for the /docs Swagger UI endpoint."""

    async def test_docs_returns_200(self, client):
        """GET /docs returns 200."""
        resp = await client.get("/docs")
        assert resp.status_code == 200

    async def test_docs_returns_html(self, client):
        """GET /docs returns HTML content type."""
        resp = await client.get("/docs")
        ct = resp.headers.get("content-type", "")
        assert "text/html" in ct

    async def test_docs_contains_swagger(self, client):
        """GET /docs HTML contains 'swagger' (case-insensitive)."""
        resp = await client.get("/docs")
        body = resp.text.lower()
        assert "swagger" in body


class TestOpenAPIJSON:
    """Tests for the /openapi.json endpoint."""

    async def test_openapi_returns_200(self, client):
        """GET /openapi.json returns 200."""
        resp = await client.get("/openapi.json")
        assert resp.status_code == 200

    async def test_openapi_returns_json(self, client):
        """GET /openapi.json returns valid JSON."""
        resp = await client.get("/openapi.json")
        ct = resp.headers.get("content-type", "")
        assert "application/json" in ct
        # Must be parseable as JSON
        data = resp.json()
        assert isinstance(data, dict)

    async def test_openapi_has_openapi_key(self, client):
        """GET /openapi.json JSON contains the 'openapi' version key."""
        resp = await client.get("/openapi.json")
        data = resp.json()
        assert "openapi" in data
        # Should be a 3.x version string
        assert data["openapi"].startswith("3.")

    async def test_openapi_has_correct_title(self, client):
        """The OpenAPI spec has the correct title."""
        resp = await client.get("/openapi.json")
        data = resp.json()
        assert data["info"]["title"] == "BleepStore S3 API"

    async def test_openapi_has_paths(self, client):
        """The OpenAPI spec includes paths."""
        resp = await client.get("/openapi.json")
        data = resp.json()
        assert "paths" in data
        # Should have at least the /health endpoint
        assert "/health" in data["paths"]
