"""Tests for BleepStore configuration loading."""

import tempfile
from pathlib import Path

import yaml

from bleepstore.config import load_config, BleepStoreConfig


class TestLoadConfig:
    """Tests for load_config()."""

    def test_load_example_config(self):
        """Loading the example config file populates all fields."""
        config = load_config(
            Path(__file__).resolve().parent.parent.parent / "bleepstore.example.yaml"
        )
        assert config.server.host == "0.0.0.0"
        assert config.server.port == 9000
        assert config.server.region == "us-east-1"
        assert config.auth.access_key == "bleepstore"
        assert config.auth.secret_key == "bleepstore-secret"
        assert config.metadata.engine == "sqlite"
        assert config.metadata.sqlite_path == "./data/metadata.db"
        assert config.storage.backend == "local"
        assert config.storage.local_root == "./data/objects"

    def test_load_minimal_config(self):
        """Loading a minimal YAML uses defaults for all fields."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({}, f)
            f.flush()
            config = load_config(Path(f.name))
        assert config.server.host == "0.0.0.0"
        assert config.server.port == 9000
        assert config.server.region == "us-east-1"
        assert config.metadata.engine == "sqlite"

    def test_load_custom_port(self):
        """CLI-style override: custom port in YAML."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"server": {"port": 9010, "host": "127.0.0.1"}}, f)
            f.flush()
            config = load_config(Path(f.name))
        assert config.server.port == 9010
        assert config.server.host == "127.0.0.1"

    def test_nested_metadata_sqlite_path(self):
        """metadata.sqlite.path is correctly parsed from nested YAML."""
        data = {
            "metadata": {
                "engine": "sqlite",
                "sqlite": {"path": "/custom/path.db"},
            }
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(data, f)
            f.flush()
            config = load_config(Path(f.name))
        assert config.metadata.sqlite_path == "/custom/path.db"

    def test_nested_storage_local_root(self):
        """storage.local.root_dir is correctly parsed from nested YAML."""
        data = {
            "storage": {
                "backend": "local",
                "local": {"root_dir": "/data/custom"},
            }
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(data, f)
            f.flush()
            config = load_config(Path(f.name))
        assert config.storage.local_root == "/data/custom"

    def test_defaults_instance(self):
        """BleepStoreConfig() with no arguments uses sane defaults."""
        config = BleepStoreConfig()
        assert config.server.port == 9000
        assert config.server.region == "us-east-1"
        assert config.auth.access_key == "bleepstore"
        assert config.metadata.engine == "sqlite"
        assert config.storage.backend == "local"
