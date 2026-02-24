"""S3 input validation helpers for BleepStore.

These functions enforce S3 naming and parameter rules *independently* of any
HTTP handler.  They are defined here so they can be unit-tested in isolation
and later wired into handlers (Stage 3+).

Each function raises an appropriate ``S3Error`` subclass on invalid input.
"""

import re

from bleepstore.errors import InvalidArgument, InvalidBucketName, KeyTooLongError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# S3 bucket naming rules:
#   - 3-63 characters
#   - lowercase letters, digits, hyphens, and periods
#   - must start and end with a letter or digit
#   - must not be formatted as an IP address
#   - must not start with "xn--" (internationalized domain prefix)
#   - must not end with "-s3alias" or "--ol-s3"
#   - no consecutive periods ("..") allowed

_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")
_IP_RE = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")

_MAX_KEY_BYTES = 1024
_MAX_MAX_KEYS = 1000


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def validate_bucket_name(name: str) -> None:
    """Validate an S3 bucket name against AWS naming rules.

    Args:
        name: The candidate bucket name.

    Raises:
        InvalidBucketName: If the name violates any S3 bucket naming rule.
    """
    if len(name) < 3 or len(name) > 63:
        raise InvalidBucketName(name)

    if not _BUCKET_RE.match(name):
        raise InvalidBucketName(name)

    if _IP_RE.match(name):
        raise InvalidBucketName(name)

    if name.startswith("xn--"):
        raise InvalidBucketName(name)

    if name.endswith("-s3alias"):
        raise InvalidBucketName(name)

    if name.endswith("--ol-s3"):
        raise InvalidBucketName(name)

    if ".." in name:
        raise InvalidBucketName(name)


def validate_object_key(key: str) -> None:
    """Validate an S3 object key.

    Args:
        key: The object key string.

    Raises:
        KeyTooLongError: If the key exceeds 1024 bytes when UTF-8 encoded.
    """
    if len(key.encode("utf-8")) > _MAX_KEY_BYTES:
        raise KeyTooLongError()


def validate_max_keys(value: str) -> int:
    """Validate and parse the ``max-keys`` query parameter.

    Args:
        value: The raw string value from the query string.

    Returns:
        An integer in the range [0, 1000].

    Raises:
        InvalidArgument: If the value is not a valid integer or is out of range.
    """
    try:
        n = int(value)
    except (ValueError, TypeError):
        raise InvalidArgument(f"Argument max-keys must be an integer between 0 and {_MAX_MAX_KEYS}")

    if n < 0 or n > _MAX_MAX_KEYS:
        raise InvalidArgument(f"Argument max-keys must be an integer between 0 and {_MAX_MAX_KEYS}")

    return n
