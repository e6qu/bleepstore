"""
BleepStore E2E Observability Tests

Language-agnostic tests for /health, /healthz, /readyz, /docs, /openapi.json, /metrics.
Uses plain HTTP requests (not boto3) since these are non-S3 endpoints.
"""

import json
import os
from pathlib import Path

import pytest
import requests

ENDPOINT = os.environ.get("BLEEPSTORE_ENDPOINT", "http://localhost:9000")
CANONICAL_SPEC_PATH = Path(__file__).resolve().parents[2] / "schemas" / "s3-api.openapi.json"

EXPECTED_METRIC_NAMES = [
    "bleepstore_http_requests_total",
    "bleepstore_http_request_duration_seconds",
    "bleepstore_http_request_size_bytes",
    "bleepstore_http_response_size_bytes",
    "bleepstore_s3_operations_total",
    "bleepstore_objects_total",
    "bleepstore_buckets_total",
    "bleepstore_bytes_received_total",
    "bleepstore_bytes_sent_total",
]


@pytest.mark.observability
class TestHealth:
    def test_health_returns_json_with_checks(self):
        resp = requests.get(f"{ENDPOINT}/health")
        assert resp.status_code == 200
        body = resp.json()
        assert "status" in body
        assert "checks" in body
        assert "metadata" in body["checks"]
        assert "storage" in body["checks"]
        for component in ("metadata", "storage"):
            assert "status" in body["checks"][component]
            assert "latency_ms" in body["checks"][component]

    def test_healthz_returns_200_empty_body(self):
        resp = requests.get(f"{ENDPOINT}/healthz")
        assert resp.status_code == 200
        assert resp.text == "" or resp.content == b""

    def test_readyz_returns_200_empty_body(self):
        resp = requests.get(f"{ENDPOINT}/readyz")
        assert resp.status_code == 200
        assert resp.text == "" or resp.content == b""


@pytest.mark.observability
class TestDocs:
    def test_docs_returns_html_with_swagger(self):
        resp = requests.get(f"{ENDPOINT}/docs")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
        assert "swagger" in resp.text.lower()

    def test_openapi_json_returns_valid_spec(self):
        resp = requests.get(f"{ENDPOINT}/openapi.json")
        assert resp.status_code == 200
        body = resp.json()
        assert "openapi" in body


@pytest.mark.observability
class TestMetrics:
    def test_metrics_returns_text(self):
        resp = requests.get(f"{ENDPOINT}/metrics")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers.get("content-type", "")

    def test_metrics_contains_all_metric_names(self):
        resp = requests.get(f"{ENDPOINT}/metrics")
        assert resp.status_code == 200
        body = resp.text
        for name in EXPECTED_METRIC_NAMES:
            assert name in body, f"Missing metric: {name}"

    def test_metrics_does_not_instrument_itself(self):
        """Scraping /metrics should not appear in http_requests_total."""
        # Scrape twice to ensure any self-counting would show up
        requests.get(f"{ENDPOINT}/metrics")
        resp = requests.get(f"{ENDPOINT}/metrics")
        body = resp.text
        for line in body.splitlines():
            if line.startswith("bleepstore_http_requests_total") and "/metrics" in line:
                pytest.fail(f"/metrics is being self-instrumented: {line}")


@pytest.mark.observability
class TestOpenAPIConformance:
    """Verify the served OpenAPI spec matches the canonical schema exactly."""

    def test_full_spec_matches_canonical(self):
        """Fetched /openapi.json must match schemas/s3-api.openapi.json (except servers)."""
        # Load canonical spec from file
        with open(CANONICAL_SPEC_PATH) as f:
            canonical = json.load(f)

        # Fetch served spec from running server
        resp = requests.get(f"{ENDPOINT}/openapi.json")
        assert resp.status_code == 200
        served = resp.json()

        # Strip servers from both (each impl patches servers to its own port)
        canonical_cmp = {k: v for k, v in canonical.items() if k != "servers"}
        served_cmp = {k: v for k, v in served.items() if k != "servers"}

        assert canonical_cmp == served_cmp, (
            "Served OpenAPI spec does not match canonical schema. "
            f"Mismatched top-level keys: {set(canonical_cmp.keys()) ^ set(served_cmp.keys())}"
        )

    def test_served_spec_has_correct_metadata(self):
        """Verify key metadata fields in the served spec."""
        resp = requests.get(f"{ENDPOINT}/openapi.json")
        assert resp.status_code == 200
        spec = resp.json()

        assert spec["openapi"] == "3.1.0"
        assert spec["info"]["title"] == "BleepStore S3-Compatible API"
        assert spec["info"]["version"] == "1.0.0"
        assert spec["info"]["license"]["name"] == "MIT"
        assert len(spec["security"]) > 0
        assert "sigv4" in spec["security"][0]

        tag_names = [t["name"] for t in spec["tags"]]
        assert "Bucket" in tag_names
        assert "Object" in tag_names
        assert "MultipartUpload" in tag_names

    def test_served_spec_has_servers_array(self):
        """The served spec must have a servers array with at least one entry."""
        resp = requests.get(f"{ENDPOINT}/openapi.json")
        assert resp.status_code == 200
        spec = resp.json()

        assert "servers" in spec
        assert len(spec["servers"]) >= 1
        assert "url" in spec["servers"][0]
