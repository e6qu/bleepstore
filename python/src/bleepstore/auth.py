"""AWS Signature Version 4 authentication for BleepStore.

Implements the full SigV4 signing and verification algorithm for both
header-based auth (Authorization header) and query-string auth (presigned URLs).

References:
    - ../specs/s3-authentication.md
    - https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
"""

import hashlib
import hmac
import logging
import re
import urllib.parse
from datetime import datetime, timezone
from typing import Any

from fastapi import Request

from bleepstore.errors import (
    AccessDenied,
    AuthorizationQueryParametersError,
    ExpiredPresignedUrl,
    InvalidAccessKeyId,
    RequestTimeTooSkewed,
    SignatureDoesNotMatch,
)

logger = logging.getLogger(__name__)

# Constants
ALGORITHM = "AWS4-HMAC-SHA256"
KEY_PREFIX = "AWS4"
SCOPE_TERMINATOR = "aws4_request"
SERVICE_NAME = "s3"
UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
STREAMING_PAYLOAD = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
MAX_PRESIGNED_EXPIRES = 604800  # 7 days in seconds
CLOCK_SKEW_TOLERANCE = 900  # 15 minutes in seconds

# Regex for the Authorization header
# Example: AWS4-HMAC-SHA256 Credential=AKID/20260222/us-east-1/s3/aws4_request,
#          SignedHeaders=host;x-amz-date, Signature=abcdef...
AUTH_HEADER_RE = re.compile(
    r"AWS4-HMAC-SHA256\s+"
    r"Credential=(?P<credential>[^,]+),\s*"
    r"SignedHeaders=(?P<signed_headers>[^,]+),\s*"
    r"Signature=(?P<signature>[0-9a-f]{64})"
)


class SigV4Authenticator:
    """Verifies AWS Signature Version 4 signed requests.

    Supports both header-based auth and presigned URL auth.

    The authenticator is initialized with a reference to the metadata store
    to look up credentials by access key ID.

    Attributes:
        metadata: The metadata store for credential lookups.
        region: The server's configured region.
    """

    def __init__(self, metadata: Any, region: str = "us-east-1") -> None:
        """Initialize the authenticator.

        Args:
            metadata: Metadata store with get_credential() method.
            region: The server's configured region.
        """
        self.metadata = metadata
        self.region = region
        # Signing key cache: (access_key, date, region, service) -> signing_key bytes
        self._signing_key_cache: dict[tuple[str, str, str, str], bytes] = {}

    async def verify_request(self, request: Request) -> dict[str, str]:
        """Verify that the incoming request has a valid SigV4 signature.

        Dispatches to header-based or presigned URL auth based on request content.

        Args:
            request: The incoming FastAPI request.

        Returns:
            A dict with 'access_key', 'owner_id', and 'display_name' on success.

        Raises:
            AccessDenied: If both auth methods are present (ambiguous).
            S3Error subclass: Various auth errors on failure.
        """
        has_auth_header = request.headers.get("authorization", "").startswith(ALGORITHM)
        has_presigned = "X-Amz-Algorithm" in request.query_params

        if has_auth_header and has_presigned:
            raise AccessDenied("Both Authorization header and presigned URL parameters present.")

        if has_presigned:
            return await self._verify_presigned(request)

        if has_auth_header:
            return await self._verify_header_auth(request)

        raise AccessDenied(
            "Missing authentication: no Authorization header or presigned URL parameters."
        )

    async def _verify_header_auth(self, request: Request) -> dict[str, str]:
        """Verify header-based SigV4 authentication.

        Args:
            request: The incoming FastAPI request.

        Returns:
            A dict with credential info on success.

        Raises:
            AccessDenied: On malformed header.
            InvalidAccessKeyId: If access key not found.
            SignatureDoesNotMatch: On signature mismatch.
            RequestTimeTooSkewed: If request is too old/new.
        """
        auth_header = request.headers.get("authorization", "")
        parsed = self._parse_authorization_header(auth_header)

        credential_parts = parsed["credential"].split("/")
        if len(credential_parts) != 5:
            raise AccessDenied("Invalid Credential format.")

        access_key = credential_parts[0]
        credential_date = credential_parts[1]
        credential_region = credential_parts[2]
        credential_service = credential_parts[3]
        credential_terminator = credential_parts[4]

        if credential_terminator != SCOPE_TERMINATOR:
            raise AccessDenied(f"Invalid credential scope terminator: {credential_terminator}")
        if credential_service != SERVICE_NAME:
            raise AccessDenied(f"Invalid credential service: {credential_service}")

        # Get the timestamp
        amz_date = request.headers.get("x-amz-date", "")
        if not amz_date:
            amz_date = request.headers.get("date", "")
        if not amz_date:
            raise AccessDenied("Missing date header.")

        # Validate date matches credential date
        date_part = amz_date[:8]
        if date_part != credential_date:
            raise AccessDenied(
                f"Date in Credential scope ({credential_date}) does not match "
                f"date in x-amz-date header ({date_part})."
            )

        # Clock skew check
        self._check_clock_skew(amz_date)

        # Look up credential
        cred = await self.metadata.get_credential(access_key)
        if cred is None:
            raise InvalidAccessKeyId()

        secret_key = cred["secret_key"]
        signed_headers_list = parsed["signed_headers"].split(";")

        # Get payload hash — if x-amz-content-sha256 is absent (e.g. non-S3 SigV4 clients),
        # compute SHA256(body) since that's what the client used to sign
        payload_hash = request.headers.get("x-amz-content-sha256")
        if payload_hash is None:
            body = await request.body()
            payload_hash = hashlib.sha256(body).hexdigest()

        # Build canonical request
        canonical_request = self._build_canonical_request(
            method=request.method,
            uri=request.url.path,
            query_string=str(request.url.query) if request.url.query else "",
            headers=dict(request.headers),
            signed_headers=signed_headers_list,
            payload_hash=payload_hash,
        )

        # Build scope
        scope = f"{credential_date}/{credential_region}/{credential_service}/{SCOPE_TERMINATOR}"

        # Build string to sign
        string_to_sign = self._build_string_to_sign(amz_date, scope, canonical_request)

        # Derive signing key
        signing_key = self._derive_signing_key(
            secret_key, credential_date, credential_region, credential_service, access_key
        )

        # Compute signature
        expected_signature = self._compute_signature(signing_key, string_to_sign)

        # Constant-time comparison
        if not hmac.compare_digest(expected_signature, parsed["signature"]):
            logger.debug(
                "Signature mismatch: expected=%s, got=%s",
                expected_signature,
                parsed["signature"],
            )
            raise SignatureDoesNotMatch()

        return {
            "access_key": access_key,
            "owner_id": cred["owner_id"],
            "display_name": cred.get("display_name", access_key),
        }

    async def _verify_presigned(self, request: Request) -> dict[str, str]:
        """Verify presigned URL authentication.

        Args:
            request: The incoming FastAPI request.

        Returns:
            A dict with credential info on success.

        Raises:
            AuthorizationQueryParametersError: On missing required params.
            AccessDenied: On expired URL or invalid params.
            InvalidAccessKeyId: If access key not found.
            SignatureDoesNotMatch: On signature mismatch.
        """
        params = request.query_params

        # Validate required parameters
        required = [
            "X-Amz-Algorithm",
            "X-Amz-Credential",
            "X-Amz-Date",
            "X-Amz-Expires",
            "X-Amz-SignedHeaders",
            "X-Amz-Signature",
        ]
        for param in required:
            if param not in params:
                raise AuthorizationQueryParametersError()

        # Validate algorithm
        algorithm = params["X-Amz-Algorithm"]
        if algorithm != ALGORITHM:
            raise AccessDenied(f"Unsupported algorithm: {algorithm}")

        # Parse credential
        credential = params["X-Amz-Credential"]
        credential_parts = credential.split("/")
        if len(credential_parts) != 5:
            raise AccessDenied("Invalid Credential format.")

        access_key = credential_parts[0]
        credential_date = credential_parts[1]
        credential_region = credential_parts[2]
        credential_service = credential_parts[3]
        credential_terminator = credential_parts[4]

        if credential_terminator != SCOPE_TERMINATOR:
            raise AccessDenied(f"Invalid credential scope terminator: {credential_terminator}")
        if credential_service != SERVICE_NAME:
            raise AccessDenied(f"Invalid credential service: {credential_service}")

        amz_date = params["X-Amz-Date"]
        amz_expires = params["X-Amz-Expires"]
        signed_headers_str = params["X-Amz-SignedHeaders"]
        provided_signature = params["X-Amz-Signature"]

        # Validate date matches credential date
        date_part = amz_date[:8]
        if date_part != credential_date:
            raise AccessDenied(
                f"Date in Credential scope ({credential_date}) does not match "
                f"X-Amz-Date ({date_part})."
            )

        # Validate expires
        try:
            expires_seconds = int(amz_expires)
        except ValueError:
            raise AuthorizationQueryParametersError("Invalid X-Amz-Expires value.")

        if expires_seconds < 1 or expires_seconds > MAX_PRESIGNED_EXPIRES:
            raise AuthorizationQueryParametersError(
                f"X-Amz-Expires must be between 1 and {MAX_PRESIGNED_EXPIRES} seconds."
            )

        # Clock skew check (fail fast before signature computation)
        self._check_clock_skew(amz_date)

        # Check expiration
        try:
            request_time = datetime.strptime(amz_date, "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            raise AccessDenied("Invalid X-Amz-Date format.")

        now = datetime.now(timezone.utc)
        expiry_time = request_time.timestamp() + expires_seconds
        if now.timestamp() > expiry_time:
            raise ExpiredPresignedUrl()

        # Look up credential
        cred = await self.metadata.get_credential(access_key)
        if cred is None:
            raise InvalidAccessKeyId()

        secret_key = cred["secret_key"]
        signed_headers_list = signed_headers_str.split(";")

        # Build canonical query string -- exclude X-Amz-Signature
        canonical_query = self._build_canonical_query_string_for_presigned(
            str(request.url.query) if request.url.query else ""
        )

        # Build canonical request with UNSIGNED-PAYLOAD
        canonical_request = self._build_canonical_request(
            method=request.method,
            uri=request.url.path,
            query_string=canonical_query,
            headers=dict(request.headers),
            signed_headers=signed_headers_list,
            payload_hash=UNSIGNED_PAYLOAD,
            is_presigned=True,
        )

        # Build scope
        scope = f"{credential_date}/{credential_region}/{credential_service}/{SCOPE_TERMINATOR}"

        # Build string to sign
        string_to_sign = self._build_string_to_sign(amz_date, scope, canonical_request)

        # Derive signing key
        signing_key = self._derive_signing_key(
            secret_key, credential_date, credential_region, credential_service, access_key
        )

        # Compute expected signature
        expected_signature = self._compute_signature(signing_key, string_to_sign)

        # Constant-time comparison
        if not hmac.compare_digest(expected_signature, provided_signature):
            logger.debug(
                "Presigned signature mismatch: expected=%s, got=%s",
                expected_signature,
                provided_signature,
            )
            raise SignatureDoesNotMatch()

        return {
            "access_key": access_key,
            "owner_id": cred["owner_id"],
            "display_name": cred.get("display_name", access_key),
        }

    # -- Parsing ---------------------------------------------------------------

    def _parse_authorization_header(self, header: str) -> dict[str, str]:
        """Parse the Authorization header into its SigV4 components.

        Args:
            header: The raw Authorization header value.

        Returns:
            A dict with keys: credential, signed_headers, signature.

        Raises:
            AccessDenied: If the header format is invalid.
        """
        match = AUTH_HEADER_RE.match(header)
        if not match:
            raise AccessDenied("Invalid Authorization header format.")

        return {
            "credential": match.group("credential"),
            "signed_headers": match.group("signed_headers"),
            "signature": match.group("signature"),
        }

    # -- Canonical request construction ----------------------------------------

    def _build_canonical_request(
        self,
        method: str,
        uri: str,
        query_string: str,
        headers: dict[str, str],
        signed_headers: list[str],
        payload_hash: str,
        is_presigned: bool = False,
    ) -> str:
        """Build the canonical request string.

        Args:
            method: HTTP method (uppercase).
            uri: The request URI path.
            query_string: The raw query string (or pre-built canonical query for presigned).
            headers: All request headers (names may be mixed case).
            signed_headers: List of signed header names (lowercase, sorted).
            payload_hash: SHA-256 hex digest or UNSIGNED-PAYLOAD.
            is_presigned: If True, query_string is already canonical (no re-processing).

        Returns:
            The canonical request string.
        """
        # Canonical URI: URI-encode the path, do not encode slashes
        canonical_uri = _uri_encode_path(uri)
        if not canonical_uri:
            canonical_uri = "/"

        # Canonical query string
        if is_presigned:
            canonical_query = query_string
        else:
            canonical_query = _build_canonical_query_string(query_string)

        # Canonical headers: lowercase names, trim values, sort by name
        # Build a map of lowercase header name -> value
        lower_headers: dict[str, str] = {}
        for name, value in headers.items():
            lower_name = name.lower()
            if lower_name in lower_headers:
                # Multiple same headers: join with comma
                lower_headers[lower_name] += "," + _trim_header_value(value)
            else:
                lower_headers[lower_name] = _trim_header_value(value)

        # Build canonical headers string (only signed headers, sorted)
        sorted_signed = sorted(signed_headers)
        canonical_headers_lines = []
        for name in sorted_signed:
            value = lower_headers.get(name, "")
            canonical_headers_lines.append(f"{name}:{value}\n")
        canonical_headers = "".join(canonical_headers_lines)

        # Signed headers string (semicolon-separated, sorted)
        signed_headers_str = ";".join(sorted_signed)

        # Assemble canonical request
        parts = [
            method,
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers_str,
            payload_hash,
        ]
        return "\n".join(parts)

    def _build_canonical_query_string_for_presigned(self, query_string: str) -> str:
        """Build canonical query string for presigned URLs, excluding X-Amz-Signature.

        Args:
            query_string: The raw query string from the URL.

        Returns:
            Canonical query string with X-Amz-Signature excluded.
        """
        if not query_string:
            return ""

        params: list[tuple[str, str]] = []
        for pair in query_string.split("&"):
            if "=" in pair:
                name, value = pair.split("=", 1)
            else:
                name = pair
                value = ""
            # URL-decode first, then we'll re-encode
            decoded_name = urllib.parse.unquote_plus(name)
            decoded_value = urllib.parse.unquote_plus(value)
            if decoded_name == "X-Amz-Signature":
                continue
            params.append((decoded_name, decoded_value))

        # Sort by name, then by value
        params.sort()

        # Re-encode
        encoded = []
        for name, value in params:
            encoded.append(
                f"{_uri_encode(name, encode_slash=True)}={_uri_encode(value, encode_slash=True)}"
            )
        return "&".join(encoded)

    # -- String to sign --------------------------------------------------------

    def _build_string_to_sign(self, timestamp: str, scope: str, canonical_request: str) -> str:
        """Build the string to sign.

        Args:
            timestamp: ISO 8601 timestamp (YYYYMMDDTHHMMSSZ).
            scope: Credential scope (YYYYMMDD/region/s3/aws4_request).
            canonical_request: The assembled canonical request string.

        Returns:
            The string to sign.
        """
        canonical_hash = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
        return f"{ALGORITHM}\n{timestamp}\n{scope}\n{canonical_hash}"

    # -- Signing key derivation ------------------------------------------------

    def _derive_signing_key(
        self,
        secret_key: str,
        date: str,
        region: str,
        service: str,
        access_key: str = "",
    ) -> bytes:
        """Derive the signing key via the HMAC-SHA256 chain.

        Caches signing keys by (access_key, date, region, service) to avoid
        recomputing the 4-step HMAC chain on every request.

        Args:
            secret_key: The secret access key.
            date: Date string (YYYYMMDD).
            region: AWS region.
            service: AWS service name.
            access_key: Access key ID for cache keying.

        Returns:
            The 32-byte signing key.
        """
        cache_key = (access_key, date, region, service)
        cached = self._signing_key_cache.get(cache_key)
        if cached is not None:
            return cached

        signing_key = derive_signing_key(secret_key, date, region, service)

        # Store in cache (evict old entries if cache gets too large)
        if len(self._signing_key_cache) > 100:
            self._signing_key_cache.clear()
        self._signing_key_cache[cache_key] = signing_key

        return signing_key

    # -- Signature computation -------------------------------------------------

    def _compute_signature(self, signing_key: bytes, string_to_sign: str) -> str:
        """Compute the final HMAC-SHA256 hex signature.

        Args:
            signing_key: The derived signing key bytes.
            string_to_sign: The assembled string to sign.

        Returns:
            64-character lowercase hex string.
        """
        return hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    # -- Clock skew check ------------------------------------------------------

    def _check_clock_skew(self, amz_date: str) -> None:
        """Check that the request timestamp is within the allowed clock skew.

        Args:
            amz_date: The x-amz-date timestamp (YYYYMMDDTHHMMSSZ format).

        Raises:
            RequestTimeTooSkewed: If the timestamp differs by more than 15 minutes.
        """
        try:
            request_time = datetime.strptime(amz_date, "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            raise AccessDenied("Invalid x-amz-date format.")

        now = datetime.now(timezone.utc)
        diff = abs((now - request_time).total_seconds())
        if diff > CLOCK_SKEW_TOLERANCE:
            raise RequestTimeTooSkewed()


# ---------------------------------------------------------------------------
# Module-level utility functions
# ---------------------------------------------------------------------------


def derive_signing_key(secret_key: str, date: str, region: str, service: str) -> bytes:
    """Derive the SigV4 signing key via the HMAC-SHA256 chain.

    This is a standalone function usable without a SigV4Authenticator instance.

    Args:
        secret_key: The secret access key.
        date: Date string (YYYYMMDD).
        region: AWS region.
        service: AWS service name.

    Returns:
        The 32-byte signing key.
    """
    k_date = hmac.new(
        (KEY_PREFIX + secret_key).encode("utf-8"),
        date.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    k_region = hmac.new(k_date, region.encode("utf-8"), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode("utf-8"), hashlib.sha256).digest()
    k_signing = hmac.new(k_service, SCOPE_TERMINATOR.encode("utf-8"), hashlib.sha256).digest()
    return k_signing


def _uri_encode(s: str, encode_slash: bool = True) -> str:
    """S3-compatible URI encoding.

    Characters A-Z, a-z, 0-9, '-', '_', '.', '~' are not encoded.
    All other characters are percent-encoded with uppercase hex.
    Spaces become %20 (not +).

    Args:
        s: The string to encode.
        encode_slash: If True (default), '/' is encoded as %2F.
                     If False, '/' is left as-is.

    Returns:
        The URI-encoded string.
    """
    safe = "-_.~" if encode_slash else "-_.~/"
    return urllib.parse.quote(s, safe=safe)


def _uri_encode_path(path: str) -> str:
    """URI-encode a path, preserving forward slashes.

    Each path segment is individually URI-encoded. Forward slashes
    are preserved (not encoded).

    Args:
        path: The URI path to encode.

    Returns:
        The URI-encoded path.
    """
    if not path:
        return "/"
    # Split by /, encode each segment, rejoin
    segments = path.split("/")
    encoded_segments = [_uri_encode(seg, encode_slash=False) for seg in segments]
    result = "/".join(encoded_segments)
    if not result.startswith("/"):
        result = "/" + result
    return result


def _build_canonical_query_string(query_string: str) -> str:
    """Build the canonical query string from a raw query string.

    Parameters are sorted by name (byte-order), then by value.
    Each name and value is URI-encoded. Parameters with no value
    use empty value (e.g., 'acl=').

    Args:
        query_string: The raw query string (without leading '?').

    Returns:
        The canonical query string.
    """
    if not query_string:
        return ""

    params: list[tuple[str, str]] = []
    for pair in query_string.split("&"):
        if not pair:
            continue
        if "=" in pair:
            name, value = pair.split("=", 1)
        else:
            name = pair
            value = ""
        # URL-decode first (query params may already be encoded)
        decoded_name = urllib.parse.unquote_plus(name)
        decoded_value = urllib.parse.unquote_plus(value)
        params.append((decoded_name, decoded_value))

    # Sort by name first, then by value (byte-order)
    params.sort()

    # Re-encode with S3-compatible encoding
    encoded = []
    for name, value in params:
        encoded.append(
            f"{_uri_encode(name, encode_slash=True)}={_uri_encode(value, encode_slash=True)}"
        )
    return "&".join(encoded)


def _trim_header_value(value: str) -> str:
    """Trim and normalize a header value for canonical headers.

    Strips leading/trailing whitespace and collapses sequential spaces
    to a single space.

    Args:
        value: The raw header value.

    Returns:
        The trimmed and normalized value.
    """
    # Strip leading/trailing whitespace
    value = value.strip()
    # Collapse sequential spaces to single space
    return re.sub(r" +", " ", value)
