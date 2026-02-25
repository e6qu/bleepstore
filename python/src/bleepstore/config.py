"""Configuration loading and Pydantic models for BleepStore."""

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class ServerConfig(BaseModel):
    """Server binding and runtime configuration."""

    host: str = "0.0.0.0"
    port: int = 9000
    region: str = "us-east-1"
    log_level: str = "INFO"
    log_format: str = "text"
    shutdown_timeout: int = 30
    max_object_size: int = 5 * 1024 * 1024 * 1024 * 1024  # 5 TB


class AuthConfig(BaseModel):
    """Authentication and credential configuration."""

    access_key: str = "bleepstore"
    secret_key: str = "bleepstore-secret"
    enabled: bool = True


class MetadataConfig(BaseModel):
    """Metadata store configuration."""

    engine: str = "sqlite"
    sqlite_path: str = "./data/metadata.db"


class StorageConfig(BaseModel):
    """Object storage backend configuration."""

    backend: str = "local"
    local_root: str = "./data/objects"
    # Memory backend
    memory_max_size_bytes: int = 0
    memory_persistence: str = "none"
    memory_snapshot_path: str = "./data/memory.snap"
    memory_snapshot_interval_seconds: int = 300
    # AWS S3
    aws_bucket: str = ""
    aws_region: str = "us-east-1"
    aws_prefix: str = ""
    aws_endpoint_url: str = ""
    aws_use_path_style: bool = False
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    # GCP Cloud Storage
    gcp_bucket: str = ""
    gcp_project: str = ""
    gcp_prefix: str = ""
    gcp_credentials_file: str = ""
    # Azure Blob
    azure_container: str = ""
    azure_account: str = ""
    azure_prefix: str = ""
    azure_connection_string: str = ""
    azure_use_managed_identity: bool = False


class ClusterConfig(BaseModel):
    """Cluster and replication configuration."""

    enabled: bool = False
    node_id: str = ""
    peers: list[str] = Field(default_factory=list)
    raft_port: int = 8334


class ObservabilityConfig(BaseModel):
    """Observability feature toggles."""

    metrics: bool = True
    health_check: bool = True


class BleepStoreConfig(BaseModel):
    """Top-level BleepStore configuration."""

    server: ServerConfig = Field(default_factory=ServerConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    metadata: MetadataConfig = Field(default_factory=MetadataConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    cluster: ClusterConfig = Field(default_factory=ClusterConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)


def _parse_server(data: dict[str, Any] | None) -> dict[str, Any]:
    """Parse the server section from YAML data into a dict for Pydantic."""
    if data is None:
        return {}
    return {
        "host": data.get("host", "0.0.0.0"),
        "port": data.get("port", 9000),
        "region": data.get("region", "us-east-1"),
        "log_level": data.get("log_level", "INFO"),
        "log_format": data.get("log_format", "text"),
        "shutdown_timeout": data.get("shutdown_timeout", 30),
        "max_object_size": data.get("max_object_size", 5 * 1024 * 1024 * 1024 * 1024),
    }


def _parse_auth(data: dict[str, Any] | None) -> dict[str, Any]:
    """Parse the auth section from YAML data."""
    if data is None:
        return {}
    return {
        "access_key": data.get("access_key", "bleepstore"),
        "secret_key": data.get("secret_key", "bleepstore-secret"),
        "enabled": data.get("enabled", True),
    }


def _parse_metadata(data: dict[str, Any] | None) -> dict[str, Any]:
    """Parse the metadata section from YAML data.

    Handles nested structure: metadata.sqlite.path -> sqlite_path
    """
    if data is None:
        return {}
    result: dict[str, Any] = {"engine": data.get("engine", "sqlite")}
    sqlite_section = data.get("sqlite")
    if isinstance(sqlite_section, dict):
        result["sqlite_path"] = sqlite_section.get("path", "./data/metadata.db")
    return result


def _parse_storage(data: dict[str, Any] | None) -> dict[str, Any]:
    """Parse the storage section from YAML data.

    Handles nested structure: storage.local.root_dir -> local_root, etc.
    """
    if data is None:
        return {}

    result: dict[str, Any] = {"backend": data.get("backend", "local")}

    local_section = data.get("local")
    if isinstance(local_section, dict):
        result["local_root"] = local_section.get("root_dir", "./data/objects")

    memory_section = data.get("memory")
    if isinstance(memory_section, dict):
        result["memory_max_size_bytes"] = memory_section.get("max_size_bytes", 0)
        result["memory_persistence"] = memory_section.get("persistence", "none")
        result["memory_snapshot_path"] = memory_section.get("snapshot_path", "./data/memory.snap")
        result["memory_snapshot_interval_seconds"] = memory_section.get(
            "snapshot_interval_seconds", 300
        )

    aws_section = data.get("aws")
    if isinstance(aws_section, dict):
        result["aws_bucket"] = aws_section.get("bucket", "")
        result["aws_region"] = aws_section.get("region", "us-east-1")
        result["aws_prefix"] = aws_section.get("prefix", "")
        result["aws_endpoint_url"] = aws_section.get("endpoint_url", "")
        result["aws_use_path_style"] = aws_section.get("use_path_style", False)
        result["aws_access_key_id"] = aws_section.get("access_key_id", "")
        result["aws_secret_access_key"] = aws_section.get("secret_access_key", "")

    gcp_section = data.get("gcp")
    if isinstance(gcp_section, dict):
        result["gcp_bucket"] = gcp_section.get("bucket", "")
        result["gcp_project"] = gcp_section.get("project", "")
        result["gcp_prefix"] = gcp_section.get("prefix", "")
        result["gcp_credentials_file"] = gcp_section.get("credentials_file", "")

    azure_section = data.get("azure")
    if isinstance(azure_section, dict):
        result["azure_container"] = azure_section.get("container", "")
        result["azure_account"] = azure_section.get("account", "")
        result["azure_prefix"] = azure_section.get("prefix", "")
        result["azure_connection_string"] = azure_section.get("connection_string", "")
        result["azure_use_managed_identity"] = azure_section.get("use_managed_identity", False)

    return result


def _parse_cluster(data: dict[str, Any] | None) -> dict[str, Any]:
    """Parse the cluster section from YAML data."""
    if data is None:
        return {}
    return {
        "enabled": data.get("enabled", False),
        "node_id": data.get("node_id", ""),
        "peers": data.get("peers", []),
        "raft_port": data.get("raft_port", 8334),
    }


def _parse_observability(data: dict[str, Any] | None) -> dict[str, Any]:
    """Parse the observability section from YAML data."""
    if data is None:
        return {}
    return {
        "metrics": data.get("metrics", True),
        "health_check": data.get("health_check", True),
    }


def load_config(path: Path) -> BleepStoreConfig:
    """Load a BleepStoreConfig from a YAML file.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        A fully populated BleepStoreConfig validated by Pydantic.

    Raises:
        FileNotFoundError: If the config file does not exist.
        yaml.YAMLError: If the file is not valid YAML.
    """
    with open(path, "r") as fh:
        raw: dict[str, Any] = yaml.safe_load(fh) or {}

    return BleepStoreConfig(
        server=ServerConfig(**_parse_server(raw.get("server"))),
        auth=AuthConfig(**_parse_auth(raw.get("auth"))),
        metadata=MetadataConfig(**_parse_metadata(raw.get("metadata"))),
        storage=StorageConfig(**_parse_storage(raw.get("storage"))),
        cluster=ClusterConfig(**_parse_cluster(raw.get("cluster"))),
        observability=ObservabilityConfig(**_parse_observability(raw.get("observability"))),
    )
