"""Tests for Prometheus metrics endpoint (Stage 1b)."""


class TestMetricsEndpoint:
    """Tests for GET /metrics."""

    async def test_metrics_returns_200(self, client):
        """GET /metrics returns 200."""
        resp = await client.get("/metrics")
        assert resp.status_code == 200

    async def test_metrics_content_type(self, client):
        """GET /metrics returns Prometheus text format content type."""
        resp = await client.get("/metrics")
        ct = resp.headers.get("content-type", "")
        # Prometheus exposition format
        assert (
            "text/plain" in ct or "text/plain; version=0.0.4" in ct or "openmetrics" in ct.lower()
        )

    async def test_metrics_contains_bleepstore_prefix(self, client):
        """GET /metrics response includes bleepstore_ prefixed metrics."""
        # Make a request first so some metrics are populated
        await client.get("/health")
        resp = await client.get("/metrics")
        body = resp.text
        assert "bleepstore_" in body

    async def test_metrics_has_s3_operations_total(self, client):
        """The bleepstore_s3_operations_total metric is registered."""
        resp = await client.get("/metrics")
        body = resp.text
        assert "bleepstore_s3_operations_total" in body

    async def test_metrics_has_objects_total(self, client):
        """The bleepstore_objects_total metric is registered."""
        resp = await client.get("/metrics")
        body = resp.text
        assert "bleepstore_objects_total" in body

    async def test_metrics_has_buckets_total(self, client):
        """The bleepstore_buckets_total metric is registered."""
        resp = await client.get("/metrics")
        body = resp.text
        assert "bleepstore_buckets_total" in body

    async def test_metrics_has_bytes_received(self, client):
        """The bleepstore_bytes_received_total metric is registered."""
        resp = await client.get("/metrics")
        body = resp.text
        assert "bleepstore_bytes_received_total" in body

    async def test_metrics_has_bytes_sent(self, client):
        """The bleepstore_bytes_sent_total metric is registered."""
        resp = await client.get("/metrics")
        body = resp.text
        assert "bleepstore_bytes_sent_total" in body

    async def test_metrics_has_duration_histogram(self, client):
        """The bleepstore_http_request_duration_seconds histogram is registered."""
        resp = await client.get("/metrics")
        body = resp.text
        assert "bleepstore_http_request_duration_seconds" in body

    async def test_health_still_works_with_metrics(self, client):
        """/health continues to work after metrics are wired."""
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}
