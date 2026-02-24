"""Bucket-level S3 request handlers for BleepStore.

Implements the 7 bucket operations:
    - ListBuckets (GET /)
    - CreateBucket (PUT /{bucket})
    - DeleteBucket (DELETE /{bucket})
    - HeadBucket (HEAD /{bucket})
    - GetBucketLocation (GET /{bucket}?location)
    - GetBucketAcl (GET /{bucket}?acl)
    - PutBucketAcl (PUT /{bucket}?acl)
"""

import hashlib
import logging
from xml.etree import ElementTree

from fastapi import FastAPI, Request, Response

from bleepstore.errors import (
    BucketNotEmpty,
    InvalidBucketName,
    MalformedXML,
    NoSuchBucket,
)
from bleepstore.handlers.acl import (
    acl_from_json,
    acl_to_json,
    build_default_acl,
    parse_canned_acl,
    render_acl_xml,
)
from bleepstore.validation import validate_bucket_name
from bleepstore.xml_utils import (
    render_list_buckets,
    render_location_constraint,
    xml_response,
)

logger = logging.getLogger(__name__)


def _derive_owner_id(access_key: str) -> str:
    """Derive a canonical owner ID from an access key.

    Uses SHA-256 hash of the access key, truncated to 32 characters.

    Args:
        access_key: The AWS access key string.

    Returns:
        A 32-character hex string owner ID.
    """
    return hashlib.sha256(access_key.encode()).hexdigest()[:32]


class BucketHandler:
    """Handles S3 bucket operations.

    All handlers access metadata and config from ``app.state``.

    Attributes:
        app: The parent FastAPI application.
    """

    def __init__(self, app: FastAPI) -> None:
        """Initialize the bucket handler.

        Args:
            app: The FastAPI application instance.
        """
        self.app = app

    @property
    def metadata(self):
        """Shortcut to the metadata store on app.state."""
        return self.app.state.metadata

    @property
    def config(self):
        """Shortcut to the BleepStoreConfig on app.state."""
        return self.app.state.config

    async def list_buckets(self, request: Request) -> Response:
        """List all buckets owned by the authenticated user.

        Implements: GET /

        Args:
            request: The incoming HTTP request.

        Returns:
            XML response containing the bucket list.
        """
        access_key = self.config.auth.access_key
        owner_id = _derive_owner_id(access_key)

        buckets = await self.metadata.list_buckets()

        xml = render_list_buckets(
            owner_id=owner_id,
            owner_display_name=access_key,
            buckets=buckets,
        )
        return xml_response(xml, status=200)

    async def create_bucket(self, request: Request, bucket: str) -> Response:
        """Create a new bucket.

        Implements: PUT /{bucket}

        Idempotency: if the bucket already exists and is owned by the caller,
        returns 200 (us-east-1 behavior).

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.

        Returns:
            Empty 200 response on success with Location header.
        """
        # Validate bucket name
        try:
            validate_bucket_name(bucket)
        except InvalidBucketName:
            raise

        # Determine the region from the request body (CreateBucketConfiguration)
        region = self.config.server.region
        body = await request.body()
        if body:
            try:
                root = ElementTree.fromstring(body)
                # Handle namespace
                ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
                loc_elem = root.find(f"{ns}LocationConstraint")
                if loc_elem is None:
                    # Try without namespace
                    loc_elem = root.find("LocationConstraint")
                if loc_elem is not None and loc_elem.text:
                    region = loc_elem.text.strip()
            except ElementTree.ParseError:
                raise MalformedXML()

        # Derive owner from access key
        access_key = self.config.auth.access_key
        owner_id = _derive_owner_id(access_key)
        owner_display = access_key

        # Check if bucket already exists
        existing = await self.metadata.get_bucket(bucket)
        if existing is not None:
            # us-east-1 behavior: if owned by caller, return 200
            # Since we have a single-user system, any existing bucket
            # is "owned by you".
            return Response(
                status_code=200,
                headers={"Location": f"/{bucket}"},
            )

        # Build default ACL
        acl = build_default_acl(owner_id, owner_display)
        acl_json = acl_to_json(acl)

        # Determine canned ACL from header
        canned_acl = request.headers.get("x-amz-acl")
        if canned_acl:
            acl = parse_canned_acl(canned_acl, owner_id, owner_display)
            acl_json = acl_to_json(acl)

        # Create the bucket in metadata store
        await self.metadata.create_bucket(
            bucket=bucket,
            region=region,
            owner_id=owner_id,
            owner_display=owner_display,
            acl=acl_json,
        )

        return Response(
            status_code=200,
            headers={"Location": f"/{bucket}"},
        )

    async def delete_bucket(self, request: Request, bucket: str) -> Response:
        """Delete an existing empty bucket.

        Implements: DELETE /{bucket}

        Preconditions:
            - Bucket must exist (NoSuchBucket if not).
            - Bucket must be empty (BucketNotEmpty if objects exist).

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.

        Returns:
            204 No Content on success.
        """
        # Check if bucket exists
        exists = await self.metadata.bucket_exists(bucket)
        if not exists:
            raise NoSuchBucket(bucket)

        # Check if bucket has any objects
        obj_count = await self.metadata.count_objects(bucket)
        if obj_count > 0:
            raise BucketNotEmpty(bucket)

        # Delete the bucket
        await self.metadata.delete_bucket(bucket)

        return Response(status_code=204)

    async def head_bucket(self, request: Request, bucket: str) -> Response:
        """Check if a bucket exists and the caller has access.

        Implements: HEAD /{bucket}

        HEAD responses never have a body.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.

        Returns:
            200 OK if the bucket exists, with x-amz-bucket-region header.
        """
        bucket_meta = await self.metadata.get_bucket(bucket)
        if bucket_meta is None:
            raise NoSuchBucket(bucket)

        region = bucket_meta.get("region", "us-east-1")
        return Response(
            status_code=200,
            headers={"x-amz-bucket-region": region},
        )

    async def get_bucket_location(
        self, request: Request, bucket: str
    ) -> Response:
        """Return the region where the bucket resides.

        Implements: GET /{bucket}?location

        us-east-1 quirk: returns empty ``<LocationConstraint/>`` instead of
        the string ``us-east-1``.

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.

        Returns:
            XML response with the bucket location constraint.
        """
        bucket_meta = await self.metadata.get_bucket(bucket)
        if bucket_meta is None:
            raise NoSuchBucket(bucket)

        region = bucket_meta.get("region", "us-east-1")
        xml = render_location_constraint(region)
        return xml_response(xml, status=200)

    async def get_bucket_acl(
        self, request: Request, bucket: str
    ) -> Response:
        """Return the access control list for a bucket.

        Implements: GET /{bucket}?acl

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.

        Returns:
            XML response with the bucket ACL.
        """
        bucket_meta = await self.metadata.get_bucket(bucket)
        if bucket_meta is None:
            raise NoSuchBucket(bucket)

        acl_json_str = bucket_meta.get("acl", "{}")
        acl = acl_from_json(acl_json_str)

        # If the ACL has no owner info, fill it in from bucket metadata
        if not acl.get("owner", {}).get("id"):
            owner_id = bucket_meta.get("owner_id", "")
            owner_display = bucket_meta.get("owner_display", "")
            if not acl.get("grants"):
                # Build a default ACL
                acl = build_default_acl(owner_id, owner_display)
            else:
                acl["owner"] = {"id": owner_id, "display_name": owner_display}

        xml = render_acl_xml(acl)
        return xml_response(xml, status=200)

    async def put_bucket_acl(
        self, request: Request, bucket: str
    ) -> Response:
        """Set the access control list for a bucket.

        Implements: PUT /{bucket}?acl

        Supports three mutually exclusive modes:
            1. Canned ACL via x-amz-acl header
            2. XML body with full AccessControlPolicy
            3. (x-amz-grant-* headers - simplified support)

        Args:
            request: The incoming HTTP request.
            bucket: The bucket name from the URL path.

        Returns:
            Empty 200 response on success.
        """
        bucket_meta = await self.metadata.get_bucket(bucket)
        if bucket_meta is None:
            raise NoSuchBucket(bucket)

        owner_id = bucket_meta.get("owner_id", "")
        owner_display = bucket_meta.get("owner_display", "")

        # Check for canned ACL header
        canned_acl = request.headers.get("x-amz-acl")
        if canned_acl:
            try:
                acl = parse_canned_acl(canned_acl, owner_id, owner_display)
            except ValueError:
                raise InvalidBucketName()  # S3 returns InvalidArgument for bad ACL names
            acl_json = acl_to_json(acl)
            await self.metadata.update_bucket_acl(bucket, acl_json)
            return Response(status_code=200)

        # Check for XML body
        body = await request.body()
        if body:
            try:
                root = ElementTree.fromstring(body)
                # Parse the AccessControlPolicy XML into our ACL format
                acl = _parse_acl_xml(root, owner_id, owner_display)
                acl_json = acl_to_json(acl)
                await self.metadata.update_bucket_acl(bucket, acl_json)
                return Response(status_code=200)
            except ElementTree.ParseError:
                raise MalformedXML()

        # No ACL specified -- default to private
        acl = build_default_acl(owner_id, owner_display)
        acl_json = acl_to_json(acl)
        await self.metadata.update_bucket_acl(bucket, acl_json)
        return Response(status_code=200)


def _find_elem(
    parent: ElementTree.Element, ns_name: str, bare_name: str
) -> ElementTree.Element | None:
    """Find a child element, trying namespaced name first, then bare name.

    Uses explicit ``is not None`` checks to avoid ElementTree's deprecated
    truth-value testing of elements.

    Args:
        parent: The parent XML element to search.
        ns_name: The namespace-qualified element name.
        bare_name: The element name without namespace.

    Returns:
        The found element, or None.
    """
    elem = parent.find(ns_name)
    if elem is not None:
        return elem
    return parent.find(bare_name)


def _parse_acl_xml(
    root: ElementTree.Element,
    default_owner_id: str,
    default_owner_display: str,
) -> dict:
    """Parse an AccessControlPolicy XML element into an ACL dict.

    Args:
        root: The parsed XML root element.
        default_owner_id: Fallback owner ID if not in XML.
        default_owner_display: Fallback display name if not in XML.

    Returns:
        An ACL dict with 'owner' and 'grants' keys.
    """
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"

    # Parse owner
    owner_elem = _find_elem(root, f"{ns}Owner", "Owner")
    owner_id = default_owner_id
    owner_display = default_owner_display
    if owner_elem is not None:
        id_elem = _find_elem(owner_elem, f"{ns}ID", "ID")
        if id_elem is not None and id_elem.text:
            owner_id = id_elem.text
        dn_elem = _find_elem(owner_elem, f"{ns}DisplayName", "DisplayName")
        if dn_elem is not None and dn_elem.text:
            owner_display = dn_elem.text

    # Parse grants
    grants = []
    acl_elem = _find_elem(root, f"{ns}AccessControlList", "AccessControlList")
    if acl_elem is not None:
        for grant_elem in (
            acl_elem.findall(f"{ns}Grant") + acl_elem.findall("Grant")
        ):
            grantee_elem = _find_elem(
                grant_elem, f"{ns}Grantee", "Grantee"
            )
            perm_elem = _find_elem(
                grant_elem, f"{ns}Permission", "Permission"
            )
            if grantee_elem is None or perm_elem is None:
                continue

            permission = perm_elem.text or ""

            # Determine grantee type from xsi:type attribute
            xsi_type = grantee_elem.get(
                "{http://www.w3.org/2001/XMLSchema-instance}type", ""
            )

            if xsi_type == "Group" or grantee_elem.find(f"{ns}URI") is not None:
                uri_elem = _find_elem(grantee_elem, f"{ns}URI", "URI")
                uri = uri_elem.text if uri_elem is not None else ""
                grants.append({
                    "grantee": {"type": "Group", "uri": uri},
                    "permission": permission,
                })
            else:
                # CanonicalUser
                g_id_elem = _find_elem(grantee_elem, f"{ns}ID", "ID")
                g_dn_elem = _find_elem(
                    grantee_elem, f"{ns}DisplayName", "DisplayName"
                )
                g_id = g_id_elem.text if g_id_elem is not None else ""
                g_dn = g_dn_elem.text if g_dn_elem is not None else ""
                grants.append({
                    "grantee": {
                        "type": "CanonicalUser",
                        "id": g_id,
                        "display_name": g_dn,
                    },
                    "permission": permission,
                })

    return {
        "owner": {"id": owner_id, "display_name": owner_display},
        "grants": grants,
    }
