"""ACL helpers for BleepStore S3-compatible access control.

Provides functions to build, parse, serialize, and render S3-compatible
Access Control Lists (ACLs). ACLs are stored as JSON in the metadata store
and rendered as XML in S3 API responses.
"""

import json
from typing import Any
from xml.sax.saxutils import escape as _sax_escape

# S3 predefined group URIs
ALL_USERS_URI = "http://acs.amazonaws.com/groups/global/AllUsers"
AUTHENTICATED_USERS_URI = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"

# S3 XML namespace
S3_XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
XSI_XMLNS = "http://www.w3.org/2001/XMLSchema-instance"


def build_default_acl(owner_id: str, owner_display: str) -> dict[str, Any]:
    """Build a default private ACL granting FULL_CONTROL to the owner.

    Args:
        owner_id: The canonical user ID of the owner.
        owner_display: The display name of the owner.

    Returns:
        A dict representing the ACL with owner and grants.
    """
    return {
        "owner": {"id": owner_id, "display_name": owner_display},
        "grants": [
            {
                "grantee": {
                    "type": "CanonicalUser",
                    "id": owner_id,
                    "display_name": owner_display,
                },
                "permission": "FULL_CONTROL",
            }
        ],
    }


def parse_canned_acl(acl_name: str, owner_id: str, owner_display: str) -> dict[str, Any]:
    """Parse a canned ACL name into a full ACL dict.

    Supported canned ACLs:
        - private: owner gets FULL_CONTROL
        - public-read: owner gets FULL_CONTROL, AllUsers get READ
        - public-read-write: owner gets FULL_CONTROL, AllUsers get READ + WRITE
        - authenticated-read: owner gets FULL_CONTROL, AuthenticatedUsers get READ

    Args:
        acl_name: The canned ACL name string.
        owner_id: The canonical user ID of the owner.
        owner_display: The display name of the owner.

    Returns:
        A dict representing the full ACL.

    Raises:
        ValueError: If the canned ACL name is not recognized.
    """
    owner_grant = {
        "grantee": {
            "type": "CanonicalUser",
            "id": owner_id,
            "display_name": owner_display,
        },
        "permission": "FULL_CONTROL",
    }

    grants = [owner_grant]

    if acl_name == "private":
        pass  # owner FULL_CONTROL only
    elif acl_name == "public-read":
        grants.append(
            {
                "grantee": {"type": "Group", "uri": ALL_USERS_URI},
                "permission": "READ",
            }
        )
    elif acl_name == "public-read-write":
        grants.append(
            {
                "grantee": {"type": "Group", "uri": ALL_USERS_URI},
                "permission": "READ",
            }
        )
        grants.append(
            {
                "grantee": {"type": "Group", "uri": ALL_USERS_URI},
                "permission": "WRITE",
            }
        )
    elif acl_name == "authenticated-read":
        grants.append(
            {
                "grantee": {"type": "Group", "uri": AUTHENTICATED_USERS_URI},
                "permission": "READ",
            }
        )
    else:
        raise ValueError(f"Unknown canned ACL: {acl_name}")

    return {
        "owner": {"id": owner_id, "display_name": owner_display},
        "grants": grants,
    }


# Grant header names and their corresponding S3 permissions
_GRANT_HEADER_MAP = {
    "x-amz-grant-full-control": "FULL_CONTROL",
    "x-amz-grant-read": "READ",
    "x-amz-grant-read-acp": "READ_ACP",
    "x-amz-grant-write": "WRITE",
    "x-amz-grant-write-acp": "WRITE_ACP",
}


def _parse_grantee_value(value: str) -> dict[str, Any]:
    """Parse a single grantee specification from a grant header value.

    Supports formats:
        - ``id="canonical-user-id"``
        - ``uri="http://acs.amazonaws.com/groups/..."``
        - ``emailAddress="user@example.com"`` (treated as canonical user)

    Args:
        value: A single grantee specification string.

    Returns:
        A grantee dict with ``type`` and either ``id``/``display_name`` or ``uri``.
    """
    value = value.strip()
    if value.startswith('id="') and value.endswith('"'):
        canonical_id = value[4:-1]
        return {
            "type": "CanonicalUser",
            "id": canonical_id,
            "display_name": "",
        }
    elif value.startswith('uri="') and value.endswith('"'):
        uri = value[5:-1]
        return {"type": "Group", "uri": uri}
    elif value.startswith('emailAddress="') and value.endswith('"'):
        email_addr = value[14:-1]
        return {
            "type": "CanonicalUser",
            "id": email_addr,
            "display_name": "",
        }
    # Fallback: treat as canonical user ID
    return {
        "type": "CanonicalUser",
        "id": value,
        "display_name": "",
    }


def parse_grant_headers(
    headers: dict[str, str],
    owner_id: str,
    owner_display: str,
) -> dict[str, Any] | None:
    """Parse x-amz-grant-* headers into an ACL dict.

    Checks for ``x-amz-grant-full-control``, ``x-amz-grant-read``,
    ``x-amz-grant-read-acp``, ``x-amz-grant-write``, and
    ``x-amz-grant-write-acp`` headers. Each header value is a
    comma-separated list of grantee specifications.

    Args:
        headers: The request headers (case-insensitive mapping).
        owner_id: The canonical user ID of the resource owner.
        owner_display: The display name of the resource owner.

    Returns:
        An ACL dict with owner and grants if any grant headers are present,
        or None if no grant headers are found.
    """
    grants: list[dict[str, Any]] = []
    found_any = False

    for header_name, permission in _GRANT_HEADER_MAP.items():
        header_value = headers.get(header_name)
        if header_value is None:
            continue
        found_any = True
        # Parse comma-separated grantees
        for grantee_spec in header_value.split(","):
            grantee_spec = grantee_spec.strip()
            if not grantee_spec:
                continue
            grantee = _parse_grantee_value(grantee_spec)
            grants.append({"grantee": grantee, "permission": permission})

    if not found_any:
        return None

    return {
        "owner": {"id": owner_id, "display_name": owner_display},
        "grants": grants,
    }


def has_grant_headers(headers: dict[str, str]) -> bool:
    """Check whether any x-amz-grant-* headers are present.

    Args:
        headers: The request headers (case-insensitive mapping).

    Returns:
        True if at least one grant header is present.
    """
    return any(headers.get(h) is not None for h in _GRANT_HEADER_MAP)


def acl_to_json(acl: dict[str, Any]) -> str:
    """Serialize an ACL dict to a JSON string for storage.

    Args:
        acl: The ACL dict to serialize.

    Returns:
        A JSON string representation.
    """
    return json.dumps(acl)


def acl_from_json(acl_json: str) -> dict[str, Any]:
    """Deserialize a JSON string to an ACL dict.

    Args:
        acl_json: The JSON string to parse.

    Returns:
        The deserialized ACL dict. Returns a minimal empty ACL if the
        JSON is empty or invalid.
    """
    if not acl_json or acl_json == "{}":
        return {"owner": {"id": "", "display_name": ""}, "grants": []}
    try:
        return json.loads(acl_json)
    except (json.JSONDecodeError, TypeError):
        return {"owner": {"id": "", "display_name": ""}, "grants": []}


def _escape(value: str) -> str:
    """Escape special XML characters."""
    return _sax_escape(str(value))


def render_acl_xml(acl: dict[str, Any]) -> str:
    """Render an ACL dict as S3-compatible AccessControlPolicy XML.

    Args:
        acl: The ACL dict with 'owner' and 'grants' keys.

    Returns:
        An XML string conforming to the S3 AccessControlPolicy format.
    """
    owner = acl.get("owner", {})
    owner_id = _escape(owner.get("id", ""))
    owner_display = _escape(owner.get("display_name", ""))
    grants = acl.get("grants", [])

    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<AccessControlPolicy xmlns="{S3_XMLNS}">',
        "<Owner>",
        f"<ID>{owner_id}</ID>",
        f"<DisplayName>{owner_display}</DisplayName>",
        "</Owner>",
        "<AccessControlList>",
    ]

    for grant in grants:
        grantee = grant.get("grantee", {})
        grantee_type = grantee.get("type", "CanonicalUser")
        permission = _escape(grant.get("permission", ""))

        parts.append("<Grant>")

        if grantee_type == "CanonicalUser":
            grantee_id = _escape(grantee.get("id", ""))
            grantee_display = _escape(grantee.get("display_name", ""))
            parts.append(f'<Grantee xmlns:xsi="{XSI_XMLNS}" xsi:type="CanonicalUser">')
            parts.append(f"<ID>{grantee_id}</ID>")
            parts.append(f"<DisplayName>{grantee_display}</DisplayName>")
            parts.append("</Grantee>")
        elif grantee_type == "Group":
            uri = _escape(grantee.get("uri", ""))
            parts.append(f'<Grantee xmlns:xsi="{XSI_XMLNS}" xsi:type="Group">')
            parts.append(f"<URI>{uri}</URI>")
            parts.append("</Grantee>")

        parts.append(f"<Permission>{permission}</Permission>")
        parts.append("</Grant>")

    parts.append("</AccessControlList>")
    parts.append("</AccessControlPolicy>")

    return "\n".join(parts)
