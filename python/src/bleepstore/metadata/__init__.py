"""Metadata store backends for BleepStore."""

from typing import TYPE_CHECKING

from bleepstore.metadata.models import (
    BucketMeta,
    Credential,
    ListPartsResult,
    ListResult,
    ListUploadsResult,
    ObjectMeta,
    PartMeta,
    UploadMeta,
)
from bleepstore.metadata.store import MetadataStore

if TYPE_CHECKING:
    from bleepstore.config import MetadataConfig

__all__ = [
    "BucketMeta",
    "create_metadata_store",
    "Credential",
    "ListPartsResult",
    "ListResult",
    "ListUploadsResult",
    "MetadataStore",
    "ObjectMeta",
    "PartMeta",
    "UploadMeta",
]


def create_metadata_store(config: "MetadataConfig") -> MetadataStore:
    """Create a metadata store instance based on configuration.

    Args:
        config: The metadata configuration.

    Returns:
        A metadata store instance implementing the MetadataStore protocol.

    Raises:
        ValueError: If the engine is unknown or required config is missing.
    """
    engine = config.engine

    if engine == "sqlite":
        from bleepstore.metadata.sqlite import SQLiteMetadataStore

        return SQLiteMetadataStore(config.sqlite.path)

    elif engine == "memory":
        from bleepstore.metadata.memory import MemoryMetadataStore

        return MemoryMetadataStore()

    elif engine == "local":
        from bleepstore.metadata.local import LocalMetadataConfig, LocalMetadataStore

        if config.local is not None:
            local_config = LocalMetadataConfig(
                root_dir=config.local.root_dir,
                compact_on_startup=config.local.compact_on_startup,
            )
        else:
            local_config = LocalMetadataConfig()
        return LocalMetadataStore(local_config)

    elif engine == "dynamodb":
        from bleepstore.metadata.dynamodb import DynamoDBMetadataStore

        if config.dynamodb is None:
            raise ValueError("metadata.dynamodb config is required when engine is 'dynamodb'")
        return DynamoDBMetadataStore(config.dynamodb)

    elif engine == "firestore":
        from bleepstore.metadata.firestore import FirestoreMetadataStore

        if config.firestore is None:
            raise ValueError("metadata.firestore config is required when engine is 'firestore'")
        return FirestoreMetadataStore(config.firestore)

    elif engine == "cosmos":
        from bleepstore.metadata.cosmos import CosmosMetadataStore

        if config.cosmos is None:
            raise ValueError("metadata.cosmos config is required when engine is 'cosmos'")
        return CosmosMetadataStore(config.cosmos)

    else:
        raise ValueError(f"Unknown metadata engine: {engine}")
