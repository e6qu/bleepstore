"""Multi-SDK S3 client discovery."""

from __future__ import annotations

import shutil

from .boto3_client import Boto3Client
from .boto3_resource import Boto3ResourceClient


def discover_clients() -> list:
    """Return all available S3 clients. Skip those whose CLI tool is not installed."""
    clients: list = [Boto3Client(), Boto3ResourceClient()]

    if shutil.which("aws"):
        from .awscli import AwsCliClient

        clients.append(AwsCliClient())

    if shutil.which("mc"):
        from .mc import McClient

        clients.append(McClient())

    if shutil.which("s3cmd"):
        from .s3cmd import S3CmdClient

        clients.append(S3CmdClient())

    if shutil.which("rclone"):
        from .rclone import RcloneClient

        clients.append(RcloneClient())

    return clients
