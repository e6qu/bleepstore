"""Tests for AWS Signature V4 authentication (Stage 6).

Tests cover:
- Signing key derivation with known test vectors
- Canonical request construction
- String-to-sign computation
- Full signature verification (header-based)
- Presigned URL validation
- Auth middleware (skip paths, error responses)
- Edge cases (clock skew, expired URLs, bad keys)
"""

import hashlib
import hmac
import urllib.parse
from datetime import datetime, timezone

import pytest
from httpx import ASGITransport, AsyncClient

from bleepstore.auth import (
    ALGORITHM,
    EMPTY_SHA256,
    KEY_PREFIX,
    SCOPE_TERMINATOR,
    SERVICE_NAME,
    UNSIGNED_PAYLOAD,
    SigV4Authenticator,
    _build_canonical_query_string,
    _trim_header_value,
    _uri_encode,
    _uri_encode_path,
    derive_signing_key,
)
from bleepstore.config import AuthConfig
from bleepstore.metadata.sqlite import SQLiteMetadataStore
from bleepstore.storage.local import LocalStorageBackend


# ---- Test fixtures ---------------------------------------------------------


@pytest.fixture
async def metadata():
    """Create a fresh in-memory metadata store with seeded credentials."""
    store = SQLiteMetadataStore(":memory:")
    await store.init_db()
    # Seed test credentials
    await store.put_credential(
        access_key_id="AKIAIOSFODNN7EXAMPLE",
        secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        owner_id="testowner123",
        display_name="TestUser",
    )
    yield store
    await store.close()


@pytest.fixture
def authenticator(metadata):
    """Create a SigV4Authenticator with the test metadata store."""
    return SigV4Authenticator(metadata=metadata, region="us-east-1")


# ---- Helper functions for test signing -------------------------------------


def _sign_request(
    method: str,
    path: str,
    query_string: str,
    headers: dict[str, str],
    body: bytes,
    access_key: str,
    secret_key: str,
    region: str,
    service: str,
    timestamp: str,
    signed_header_names: list[str] | None = None,
) -> str:
    """Sign a request using SigV4 and return the Authorization header value.

    This is a reference implementation used to generate test vectors.
    """
    date_part = timestamp[:8]

    if signed_header_names is None:
        signed_header_names = sorted(headers.keys())
    else:
        signed_header_names = sorted(signed_header_names)

    # Payload hash
    payload_hash = headers.get("x-amz-content-sha256", hashlib.sha256(body).hexdigest())

    # Canonical URI
    canonical_uri = _uri_encode_path(path)
    if not canonical_uri:
        canonical_uri = "/"

    # Canonical query string
    canonical_query = _build_canonical_query_string(query_string)

    # Canonical headers
    lower_headers = {k.lower(): v.strip() for k, v in headers.items()}
    canonical_headers_lines = []
    for name in signed_header_names:
        value = lower_headers.get(name, "")
        canonical_headers_lines.append(f"{name}:{value}\n")
    canonical_headers = "".join(canonical_headers_lines)

    signed_headers_str = ";".join(signed_header_names)

    # Canonical request
    canonical_request = (
        f"{method}\n{canonical_uri}\n{canonical_query}\n"
        f"{canonical_headers}\n{signed_headers_str}\n{payload_hash}"
    )

    # String to sign
    scope = f"{date_part}/{region}/{service}/{SCOPE_TERMINATOR}"
    canonical_hash = hashlib.sha256(canonical_request.encode()).hexdigest()
    string_to_sign = f"{ALGORITHM}\n{timestamp}\n{scope}\n{canonical_hash}"

    # Signing key
    signing_key = derive_signing_key(secret_key, date_part, region, service)

    # Signature
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()

    # Authorization header
    credential = f"{access_key}/{date_part}/{region}/{service}/{SCOPE_TERMINATOR}"
    return f"{ALGORITHM} Credential={credential}, SignedHeaders={signed_headers_str}, Signature={signature}"


def _generate_presigned_url(
    method: str,
    path: str,
    access_key: str,
    secret_key: str,
    region: str,
    timestamp: str,
    expires: int = 3600,
    host: str = "testserver",
) -> str:
    """Generate a presigned URL for testing.

    Returns the path + query string portion of the URL.
    """
    date_part = timestamp[:8]
    credential = f"{access_key}/{date_part}/{region}/{SERVICE_NAME}/{SCOPE_TERMINATOR}"

    # Build query params (alphabetical order matters for signing)
    params = {
        "X-Amz-Algorithm": ALGORITHM,
        "X-Amz-Credential": credential,
        "X-Amz-Date": timestamp,
        "X-Amz-Expires": str(expires),
        "X-Amz-SignedHeaders": "host",
    }

    # Build canonical query string (sorted, excluding signature)
    sorted_params = sorted(params.items())
    canonical_query_parts = []
    for name, value in sorted_params:
        canonical_query_parts.append(f"{_uri_encode(name, True)}={_uri_encode(value, True)}")
    canonical_query = "&".join(canonical_query_parts)

    # Canonical headers
    canonical_headers = f"host:{host}\n"

    # Canonical request
    canonical_uri = _uri_encode_path(path)
    canonical_request = (
        f"{method}\n{canonical_uri}\n{canonical_query}\n"
        f"{canonical_headers}\nhost\n{UNSIGNED_PAYLOAD}"
    )

    # String to sign
    scope = f"{date_part}/{region}/{SERVICE_NAME}/{SCOPE_TERMINATOR}"
    canonical_hash = hashlib.sha256(canonical_request.encode()).hexdigest()
    string_to_sign = f"{ALGORITHM}\n{timestamp}\n{scope}\n{canonical_hash}"

    # Signing key and signature
    signing_key = derive_signing_key(secret_key, date_part, region, SERVICE_NAME)
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()

    # Full URL
    url_query = canonical_query + f"&X-Amz-Signature={signature}"
    return f"{path}?{url_query}"


def _now_timestamp() -> str:
    """Return the current UTC timestamp in SigV4 format."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ---- Auth-enabled client fixture -------------------------------------------
# Uses the session-scoped app but temporarily enables auth and swaps metadata.


@pytest.fixture
async def auth_client(app, tmp_path):
    """Create a client with auth enabled using the session-scoped app.

    Temporarily enables auth and swaps in a fresh metadata store with
    known credentials, then restores the original state after the test.
    """
    # Save original state
    old_config_auth_enabled = app.state.config.auth.enabled
    old_config_access_key = app.state.config.auth.access_key
    old_config_secret_key = app.state.config.auth.secret_key
    old_metadata = getattr(app.state, "metadata", None)
    old_storage = getattr(app.state, "storage", None)

    # Enable auth and set known credentials
    app.state.config.auth.enabled = True
    app.state.config.auth.access_key = "AKIAIOSFODNN7EXAMPLE"
    app.state.config.auth.secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

    # Create fresh metadata with known credentials
    metadata = SQLiteMetadataStore(":memory:")
    await metadata.init_db()
    access_key = "AKIAIOSFODNN7EXAMPLE"
    secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    owner_id = hashlib.sha256(access_key.encode()).hexdigest()[:32]
    await metadata.put_credential(
        access_key_id=access_key,
        secret_key=secret_key,
        owner_id=owner_id,
        display_name=access_key,
    )
    app.state.metadata = metadata

    # Fresh storage
    storage = LocalStorageBackend(str(tmp_path / "auth-objects"))
    await storage.init()
    app.state.storage = storage

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac

    # Restore original state
    app.state.config.auth.enabled = old_config_auth_enabled
    app.state.config.auth.access_key = old_config_access_key
    app.state.config.auth.secret_key = old_config_secret_key
    app.state.metadata = old_metadata
    app.state.storage = old_storage
    await metadata.close()


# ---- Signing key derivation tests ------------------------------------------


class TestDeriveSigningKey:
    """Test signing key derivation (HMAC-SHA256 chain)."""

    def test_known_vector(self):
        """Verify signing key derivation against a known test vector.

        These are the AWS example values from the SigV4 documentation.
        """
        secret_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
        date = "20120215"
        region = "us-east-1"
        service = "iam"

        signing_key = derive_signing_key(secret_key, date, region, service)

        # Verify intermediate steps
        k_date = hmac.new(
            (KEY_PREFIX + secret_key).encode(), date.encode(), hashlib.sha256
        ).digest()
        k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
        k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
        expected = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()

        assert signing_key == expected
        assert len(signing_key) == 32  # SHA-256 output is 32 bytes

    def test_different_dates_produce_different_keys(self):
        """Different dates produce different signing keys."""
        secret = "mysecret"
        key1 = derive_signing_key(secret, "20260222", "us-east-1", "s3")
        key2 = derive_signing_key(secret, "20260223", "us-east-1", "s3")
        assert key1 != key2

    def test_different_regions_produce_different_keys(self):
        """Different regions produce different signing keys."""
        secret = "mysecret"
        key1 = derive_signing_key(secret, "20260222", "us-east-1", "s3")
        key2 = derive_signing_key(secret, "20260222", "eu-west-1", "s3")
        assert key1 != key2

    def test_different_services_produce_different_keys(self):
        """Different services produce different signing keys."""
        secret = "mysecret"
        key1 = derive_signing_key(secret, "20260222", "us-east-1", "s3")
        key2 = derive_signing_key(secret, "20260222", "us-east-1", "iam")
        assert key1 != key2

    def test_s3_signing_key_deterministic(self):
        """Same inputs always produce the same signing key."""
        secret = "mysecret"
        key1 = derive_signing_key(secret, "20260222", "us-east-1", "s3")
        key2 = derive_signing_key(secret, "20260222", "us-east-1", "s3")
        assert key1 == key2


# ---- URI encoding tests ----------------------------------------------------


class TestUriEncode:
    """Test S3-compatible URI encoding."""

    def test_unreserved_characters(self):
        """Unreserved characters are not encoded."""
        assert _uri_encode("abcXYZ0123456789-_.~") == "abcXYZ0123456789-_.~"

    def test_space_becomes_percent_20(self):
        """Spaces are encoded as %20 (not +)."""
        assert _uri_encode("hello world") == "hello%20world"

    def test_slash_encoded_by_default(self):
        """Forward slash is encoded as %2F by default."""
        assert _uri_encode("a/b") == "a%2Fb"

    def test_slash_preserved_when_disabled(self):
        """Forward slash preserved when encode_slash=False."""
        assert _uri_encode("a/b", encode_slash=False) == "a/b"

    def test_special_characters(self):
        """Special characters are percent-encoded with uppercase hex."""
        result = _uri_encode("key=value&foo")
        assert "%3D" in result  # =
        assert "%26" in result  # &

    def test_unicode_encoded(self):
        """Unicode characters are percent-encoded."""
        result = _uri_encode("\u00e9")
        assert "%" in result

    def test_empty_string(self):
        """Empty string returns empty string."""
        assert _uri_encode("") == ""


class TestUriEncodePath:
    """Test URI path encoding."""

    def test_simple_path(self):
        """Simple path is preserved."""
        assert _uri_encode_path("/bucket/key") == "/bucket/key"

    def test_empty_path(self):
        """Empty path becomes /."""
        assert _uri_encode_path("") == "/"

    def test_preserves_slashes(self):
        """Slashes in path are preserved."""
        assert _uri_encode_path("/bucket/dir/file.txt") == "/bucket/dir/file.txt"

    def test_encodes_special_chars(self):
        """Special characters in path segments are encoded."""
        result = _uri_encode_path("/bucket/my key.txt")
        assert "my%20key.txt" in result

    def test_root_path(self):
        """Root path is preserved."""
        assert _uri_encode_path("/") == "/"


# ---- Canonical query string tests ------------------------------------------


class TestCanonicalQueryString:
    """Test canonical query string construction."""

    def test_empty(self):
        """Empty query string returns empty string."""
        assert _build_canonical_query_string("") == ""

    def test_single_param(self):
        """Single parameter is returned as-is."""
        assert _build_canonical_query_string("key=value") == "key=value"

    def test_params_sorted(self):
        """Parameters are sorted by name."""
        result = _build_canonical_query_string("z=1&a=2")
        assert result == "a=2&z=1"

    def test_empty_value(self):
        """Parameter with no value uses empty value."""
        result = _build_canonical_query_string("acl")
        assert result == "acl="

    def test_empty_value_with_equals(self):
        """Parameter with equals but no value."""
        result = _build_canonical_query_string("acl=")
        assert result == "acl="

    def test_multiple_params(self):
        """Multiple params sorted correctly."""
        result = _build_canonical_query_string("prefix=test&delimiter=/&max-keys=10")
        parts = result.split("&")
        names = [p.split("=")[0] for p in parts]
        assert names == sorted(names)

    def test_values_uri_encoded(self):
        """Values are URI-encoded."""
        result = _build_canonical_query_string("key=a b")
        assert "a%20b" in result


# ---- Header value trimming tests -------------------------------------------


class TestTrimHeaderValue:
    """Test header value trimming for canonical headers."""

    def test_trim_whitespace(self):
        """Leading/trailing whitespace is stripped."""
        assert _trim_header_value("  value  ") == "value"

    def test_collapse_spaces(self):
        """Sequential spaces are collapsed to single space."""
        assert _trim_header_value("a   b   c") == "a b c"

    def test_already_trimmed(self):
        """Already-trimmed value is returned as-is."""
        assert _trim_header_value("value") == "value"


# ---- Parse authorization header tests --------------------------------------


class TestParseAuthorizationHeader:
    """Test Authorization header parsing."""

    def test_valid_header(self, authenticator):
        """Parses a valid Authorization header."""
        header = (
            "AWS4-HMAC-SHA256 "
            "Credential=AKID/20260222/us-east-1/s3/aws4_request, "
            "SignedHeaders=host;x-amz-date, "
            "Signature=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        )
        result = authenticator._parse_authorization_header(header)
        assert result["credential"] == "AKID/20260222/us-east-1/s3/aws4_request"
        assert result["signed_headers"] == "host;x-amz-date"
        assert (
            result["signature"]
            == "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        )

    def test_invalid_header_raises(self, authenticator):
        """Invalid Authorization header raises AccessDenied."""
        from bleepstore.errors import AccessDenied

        with pytest.raises(AccessDenied):
            authenticator._parse_authorization_header("BadHeader")

    def test_missing_signature_raises(self, authenticator):
        """Missing signature in header raises AccessDenied."""
        from bleepstore.errors import AccessDenied

        header = "AWS4-HMAC-SHA256 Credential=AKID/20260222/us-east-1/s3/aws4_request, SignedHeaders=host"
        with pytest.raises(AccessDenied):
            authenticator._parse_authorization_header(header)


# ---- Build canonical request tests -----------------------------------------


class TestBuildCanonicalRequest:
    """Test canonical request construction."""

    def test_simple_get(self, authenticator):
        """Simple GET request produces correct canonical request."""
        result = authenticator._build_canonical_request(
            method="GET",
            uri="/test-bucket",
            query_string="",
            headers={"host": "localhost:9010", "x-amz-date": "20260222T120000Z"},
            signed_headers=["host", "x-amz-date"],
            payload_hash=UNSIGNED_PAYLOAD,
        )
        lines = result.split("\n")
        assert lines[0] == "GET"  # method
        assert lines[1] == "/test-bucket"  # canonical URI
        assert lines[2] == ""  # empty query string
        # Canonical headers: each header line has trailing \n, which becomes
        # two entries after split (the value line and an empty string)
        assert lines[3] == "host:localhost:9010"  # first header value
        assert lines[4] == "x-amz-date:20260222T120000Z"  # second header value
        assert lines[5] == ""  # trailing \n from headers
        assert lines[6] == "host;x-amz-date"  # signed headers
        assert lines[7] == UNSIGNED_PAYLOAD  # payload hash

    def test_put_with_body(self, authenticator):
        """PUT request with body hash."""
        body_hash = hashlib.sha256(b"hello").hexdigest()
        result = authenticator._build_canonical_request(
            method="PUT",
            uri="/bucket/key.txt",
            query_string="",
            headers={
                "host": "localhost:9010",
                "x-amz-date": "20260222T120000Z",
                "x-amz-content-sha256": body_hash,
            },
            signed_headers=["host", "x-amz-content-sha256", "x-amz-date"],
            payload_hash=body_hash,
        )
        assert result.endswith(body_hash)

    def test_query_params_sorted(self, authenticator):
        """Query params in canonical request are sorted."""
        result = authenticator._build_canonical_request(
            method="GET",
            uri="/bucket",
            query_string="prefix=test&delimiter=/&max-keys=10",
            headers={"host": "localhost:9010"},
            signed_headers=["host"],
            payload_hash=UNSIGNED_PAYLOAD,
        )
        lines = result.split("\n")
        query_line = lines[2]
        params = query_line.split("&")
        param_names = [p.split("=")[0] for p in params]
        assert param_names == sorted(param_names)

    def test_headers_sorted_and_lowercased(self, authenticator):
        """Canonical headers are sorted by lowercase name."""
        result = authenticator._build_canonical_request(
            method="GET",
            uri="/",
            query_string="",
            headers={
                "Host": "localhost:9010",
                "X-Amz-Date": "20260222T120000Z",
                "Content-Type": "text/plain",
            },
            signed_headers=["content-type", "host", "x-amz-date"],
            payload_hash=UNSIGNED_PAYLOAD,
        )
        lines = result.split("\n")
        # Headers block starts at line 3
        assert lines[3].startswith("content-type:")
        assert lines[4].startswith("host:")
        assert lines[5].startswith("x-amz-date:")


# ---- String to sign tests --------------------------------------------------


class TestBuildStringToSign:
    """Test string-to-sign construction."""

    def test_format(self, authenticator):
        """String to sign has correct format."""
        canonical_request = "GET\n/\n\nhost:localhost\n\nhost\nUNSIGNED-PAYLOAD"
        result = authenticator._build_string_to_sign(
            timestamp="20260222T120000Z",
            scope="20260222/us-east-1/s3/aws4_request",
            canonical_request=canonical_request,
        )
        lines = result.split("\n")
        assert lines[0] == ALGORITHM
        assert lines[1] == "20260222T120000Z"
        assert lines[2] == "20260222/us-east-1/s3/aws4_request"
        # Line 3 is SHA-256 of canonical request
        expected_hash = hashlib.sha256(canonical_request.encode()).hexdigest()
        assert lines[3] == expected_hash

    def test_different_canonical_requests_different_hashes(self, authenticator):
        """Different canonical requests produce different string-to-sign."""
        sts1 = authenticator._build_string_to_sign(
            "20260222T120000Z", "20260222/us-east-1/s3/aws4_request", "request1"
        )
        sts2 = authenticator._build_string_to_sign(
            "20260222T120000Z", "20260222/us-east-1/s3/aws4_request", "request2"
        )
        assert sts1 != sts2


# ---- Compute signature tests -----------------------------------------------


class TestComputeSignature:
    """Test final signature computation."""

    def test_returns_64_char_hex(self, authenticator):
        """Signature is a 64-character lowercase hex string."""
        signing_key = derive_signing_key("secret", "20260222", "us-east-1", "s3")
        sig = authenticator._compute_signature(signing_key, "string to sign")
        assert len(sig) == 64
        assert all(c in "0123456789abcdef" for c in sig)

    def test_deterministic(self, authenticator):
        """Same inputs produce same signature."""
        signing_key = derive_signing_key("secret", "20260222", "us-east-1", "s3")
        sig1 = authenticator._compute_signature(signing_key, "string to sign")
        sig2 = authenticator._compute_signature(signing_key, "string to sign")
        assert sig1 == sig2

    def test_different_inputs_different_sigs(self, authenticator):
        """Different inputs produce different signatures."""
        signing_key = derive_signing_key("secret", "20260222", "us-east-1", "s3")
        sig1 = authenticator._compute_signature(signing_key, "string1")
        sig2 = authenticator._compute_signature(signing_key, "string2")
        assert sig1 != sig2


# ---- Full header-based auth verification tests (integration) ---------------


class TestHeaderAuthIntegration:
    """Integration tests for header-based SigV4 authentication."""

    async def test_signed_request_succeeds(self, auth_client):
        """A properly signed GET / (ListBuckets) returns 200."""
        timestamp = _now_timestamp()
        access_key = "AKIAIOSFODNN7EXAMPLE"
        secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

        headers = {
            "host": "testserver",
            "x-amz-date": timestamp,
            "x-amz-content-sha256": UNSIGNED_PAYLOAD,
        }
        headers["authorization"] = _sign_request(
            method="GET",
            path="/",
            query_string="",
            headers=headers,
            body=b"",
            access_key=access_key,
            secret_key=secret_key,
            region="us-east-1",
            service="s3",
            timestamp=timestamp,
            signed_header_names=["host", "x-amz-content-sha256", "x-amz-date"],
        )

        resp = await auth_client.get("/", headers=headers)
        assert resp.status_code == 200

    async def test_unsigned_request_denied(self, auth_client):
        """Request without any auth returns 403 AccessDenied."""
        resp = await auth_client.get("/")
        assert resp.status_code == 403
        assert "AccessDenied" in resp.text

    async def test_bad_signature_denied(self, auth_client):
        """Request with wrong signature returns 403 SignatureDoesNotMatch."""
        timestamp = _now_timestamp()
        date_part = timestamp[:8]

        headers = {
            "host": "testserver",
            "x-amz-date": timestamp,
            "x-amz-content-sha256": UNSIGNED_PAYLOAD,
            "authorization": (
                f"{ALGORITHM} "
                f"Credential=AKIAIOSFODNN7EXAMPLE/{date_part}/us-east-1/s3/{SCOPE_TERMINATOR}, "
                f"SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
                f"Signature=0000000000000000000000000000000000000000000000000000000000000000"
            ),
        }

        resp = await auth_client.get("/", headers=headers)
        assert resp.status_code == 403
        assert "SignatureDoesNotMatch" in resp.text

    async def test_invalid_access_key_denied(self, auth_client):
        """Request with unknown access key returns 403 InvalidAccessKeyId."""
        timestamp = _now_timestamp()
        date_part = timestamp[:8]

        headers = {
            "host": "testserver",
            "x-amz-date": timestamp,
            "x-amz-content-sha256": UNSIGNED_PAYLOAD,
            "authorization": (
                f"{ALGORITHM} "
                f"Credential=BADACCESSKEY/{date_part}/us-east-1/s3/{SCOPE_TERMINATOR}, "
                f"SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
                f"Signature=0000000000000000000000000000000000000000000000000000000000000000"
            ),
        }

        resp = await auth_client.get("/", headers=headers)
        assert resp.status_code == 403
        assert "InvalidAccessKeyId" in resp.text

    async def test_health_skips_auth(self, auth_client):
        """GET /health does not require authentication."""
        resp = await auth_client.get("/health")
        assert resp.status_code == 200

    async def test_metrics_skips_auth(self, auth_client):
        """GET /metrics does not require authentication."""
        resp = await auth_client.get("/metrics")
        assert resp.status_code == 200

    async def test_docs_skips_auth(self, auth_client):
        """GET /docs does not require authentication."""
        resp = await auth_client.get("/docs")
        assert resp.status_code == 200

    async def test_openapi_skips_auth(self, auth_client):
        """GET /openapi.json does not require authentication."""
        resp = await auth_client.get("/openapi.json")
        assert resp.status_code == 200

    async def test_put_object_with_valid_sig(self, auth_client):
        """PutObject with valid signature succeeds."""
        timestamp = _now_timestamp()
        access_key = "AKIAIOSFODNN7EXAMPLE"
        secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

        # First create bucket
        bucket_headers = {
            "host": "testserver",
            "x-amz-date": timestamp,
            "x-amz-content-sha256": UNSIGNED_PAYLOAD,
        }
        bucket_headers["authorization"] = _sign_request(
            method="PUT",
            path="/auth-test-bucket",
            query_string="",
            headers=bucket_headers,
            body=b"",
            access_key=access_key,
            secret_key=secret_key,
            region="us-east-1",
            service="s3",
            timestamp=timestamp,
            signed_header_names=["host", "x-amz-content-sha256", "x-amz-date"],
        )
        resp = await auth_client.put("/auth-test-bucket", headers=bucket_headers)
        assert resp.status_code == 200

        # Now put object
        body = b"hello world"
        body_hash = hashlib.sha256(body).hexdigest()
        obj_headers = {
            "host": "testserver",
            "x-amz-date": timestamp,
            "x-amz-content-sha256": body_hash,
            "content-type": "text/plain",
        }
        obj_headers["authorization"] = _sign_request(
            method="PUT",
            path="/auth-test-bucket/test-key.txt",
            query_string="",
            headers=obj_headers,
            body=body,
            access_key=access_key,
            secret_key=secret_key,
            region="us-east-1",
            service="s3",
            timestamp=timestamp,
            signed_header_names=["content-type", "host", "x-amz-content-sha256", "x-amz-date"],
        )
        resp = await auth_client.put(
            "/auth-test-bucket/test-key.txt",
            content=body,
            headers=obj_headers,
        )
        assert resp.status_code == 200
        assert "etag" in resp.headers

    async def test_error_response_is_xml(self, auth_client):
        """Auth error response is S3-compatible XML."""
        resp = await auth_client.get("/")
        assert resp.status_code == 403
        assert "application/xml" in resp.headers.get("content-type", "")
        assert "<Error>" in resp.text
        assert "<Code>" in resp.text
        assert "<Message>" in resp.text


# ---- Presigned URL tests ---------------------------------------------------


class TestPresignedUrlIntegration:
    """Integration tests for presigned URL authentication."""

    async def test_presigned_get_succeeds(self, auth_client):
        """Presigned GET URL with valid signature succeeds."""
        timestamp = _now_timestamp()
        access_key = "AKIAIOSFODNN7EXAMPLE"
        secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

        # First create a bucket and put an object using header auth
        bucket_headers = {
            "host": "testserver",
            "x-amz-date": timestamp,
            "x-amz-content-sha256": UNSIGNED_PAYLOAD,
        }
        bucket_headers["authorization"] = _sign_request(
            method="PUT",
            path="/presigned-bucket",
            query_string="",
            headers=bucket_headers,
            body=b"",
            access_key=access_key,
            secret_key=secret_key,
            region="us-east-1",
            service="s3",
            timestamp=timestamp,
            signed_header_names=["host", "x-amz-content-sha256", "x-amz-date"],
        )
        await auth_client.put("/presigned-bucket", headers=bucket_headers)

        body = b"presigned content"
        body_hash = hashlib.sha256(body).hexdigest()
        obj_headers = {
            "host": "testserver",
            "x-amz-date": timestamp,
            "x-amz-content-sha256": body_hash,
        }
        obj_headers["authorization"] = _sign_request(
            method="PUT",
            path="/presigned-bucket/obj.txt",
            query_string="",
            headers=obj_headers,
            body=body,
            access_key=access_key,
            secret_key=secret_key,
            region="us-east-1",
            service="s3",
            timestamp=timestamp,
            signed_header_names=["host", "x-amz-content-sha256", "x-amz-date"],
        )
        await auth_client.put(
            "/presigned-bucket/obj.txt",
            content=body,
            headers=obj_headers,
        )

        # Now use a presigned URL to GET the object
        presigned_url = _generate_presigned_url(
            method="GET",
            path="/presigned-bucket/obj.txt",
            access_key=access_key,
            secret_key=secret_key,
            region="us-east-1",
            timestamp=timestamp,
        )
        resp = await auth_client.get(presigned_url, headers={"host": "testserver"})
        assert resp.status_code == 200
        assert resp.content == body

    async def test_presigned_bad_signature_denied(self, auth_client):
        """Presigned URL with wrong signature returns 403."""
        timestamp = _now_timestamp()
        date_part = timestamp[:8]
        credential = f"AKIAIOSFODNN7EXAMPLE/{date_part}/us-east-1/s3/{SCOPE_TERMINATOR}"

        url = (
            f"/some-bucket/key?"
            f"X-Amz-Algorithm={ALGORITHM}&"
            f"X-Amz-Credential={urllib.parse.quote(credential, safe='')}&"
            f"X-Amz-Date={timestamp}&"
            f"X-Amz-Expires=3600&"
            f"X-Amz-SignedHeaders=host&"
            f"X-Amz-Signature=0000000000000000000000000000000000000000000000000000000000000000"
        )
        resp = await auth_client.get(url, headers={"host": "testserver"})
        assert resp.status_code == 403

    async def test_presigned_missing_params_400(self, auth_client):
        """Presigned URL missing required params returns 400."""
        url = "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256"
        resp = await auth_client.get(url, headers={"host": "testserver"})
        assert resp.status_code == 400
        assert "AuthorizationQueryParametersError" in resp.text

    async def test_presigned_expired_denied(self, auth_client):
        """Expired presigned URL returns 403 AccessDenied."""
        old_timestamp = "20200101T000000Z"
        access_key = "AKIAIOSFODNN7EXAMPLE"
        secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

        presigned_url = _generate_presigned_url(
            method="GET",
            path="/bucket/key",
            access_key=access_key,
            secret_key=secret_key,
            region="us-east-1",
            timestamp=old_timestamp,
            expires=1,
        )
        resp = await auth_client.get(presigned_url, headers={"host": "testserver"})
        assert resp.status_code == 403

    async def test_presigned_invalid_access_key(self, auth_client):
        """Presigned URL with unknown access key returns 403."""
        timestamp = _now_timestamp()
        presigned_url = _generate_presigned_url(
            method="GET",
            path="/bucket/key",
            access_key="BADKEY",
            secret_key="badsecret",
            region="us-east-1",
            timestamp=timestamp,
        )
        resp = await auth_client.get(presigned_url, headers={"host": "testserver"})
        assert resp.status_code == 403
        assert "InvalidAccessKeyId" in resp.text

    async def test_presigned_put_succeeds(self, auth_client):
        """Presigned PUT URL succeeds."""
        timestamp = _now_timestamp()
        access_key = "AKIAIOSFODNN7EXAMPLE"
        secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

        # Create bucket first (with header auth)
        bucket_headers = {
            "host": "testserver",
            "x-amz-date": timestamp,
            "x-amz-content-sha256": UNSIGNED_PAYLOAD,
        }
        bucket_headers["authorization"] = _sign_request(
            method="PUT",
            path="/presigned-put-bucket",
            query_string="",
            headers=bucket_headers,
            body=b"",
            access_key=access_key,
            secret_key=secret_key,
            region="us-east-1",
            service="s3",
            timestamp=timestamp,
            signed_header_names=["host", "x-amz-content-sha256", "x-amz-date"],
        )
        await auth_client.put("/presigned-put-bucket", headers=bucket_headers)

        # Now use a presigned PUT URL
        presigned_url = _generate_presigned_url(
            method="PUT",
            path="/presigned-put-bucket/uploaded.txt",
            access_key=access_key,
            secret_key=secret_key,
            region="us-east-1",
            timestamp=timestamp,
        )
        resp = await auth_client.put(
            presigned_url,
            content=b"uploaded via presigned",
            headers={"host": "testserver"},
        )
        assert resp.status_code == 200


# ---- Auth disabled tests ---------------------------------------------------


class TestAuthDisabled:
    """Test that auth can be disabled via config."""

    async def test_no_auth_when_disabled(self, client):
        """Requests succeed without auth when auth.enabled=False."""
        # The `client` fixture uses auth disabled config
        resp = await client.get("/")
        assert resp.status_code == 200


# ---- Signing key cache tests -----------------------------------------------


class TestSigningKeyCache:
    """Test that signing keys are cached."""

    def test_cache_returns_same_key(self, authenticator):
        """Cached signing key is returned on subsequent calls."""
        key1 = authenticator._derive_signing_key("secret", "20260222", "us-east-1", "s3", "access1")
        key2 = authenticator._derive_signing_key("secret", "20260222", "us-east-1", "s3", "access1")
        assert key1 is key2  # Same object from cache

    def test_cache_different_date(self, authenticator):
        """Different dates produce different cached keys."""
        key1 = authenticator._derive_signing_key("secret", "20260222", "us-east-1", "s3", "access1")
        key2 = authenticator._derive_signing_key("secret", "20260223", "us-east-1", "s3", "access1")
        assert key1 != key2
