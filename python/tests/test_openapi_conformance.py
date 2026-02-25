"""OpenAPI conformance test -- served spec must match canonical schema exactly."""
import json
from pathlib import Path


def test_served_spec_matches_canonical():
    """The spec served at /openapi.json must match schemas/s3-api.openapi.json (except servers)."""
    # Load canonical
    canonical_path = Path(__file__).resolve().parents[2] / "schemas" / "s3-api.openapi.json"
    with open(canonical_path) as f:
        canonical = json.load(f)

    # Load the served spec by importing the loading function
    from bleepstore.server import _load_openapi_spec

    served = _load_openapi_spec()

    # Strip servers from both for comparison
    canonical_cmp = {k: v for k, v in canonical.items() if k != "servers"}
    served_cmp = {k: v for k, v in served.items() if k != "servers"}

    assert canonical_cmp == served_cmp, (
        f"Spec mismatch! Diff keys: {set(canonical_cmp.keys()) ^ set(served_cmp.keys())}"
    )
