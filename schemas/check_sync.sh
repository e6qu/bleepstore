#!/usr/bin/env bash
# Verify s3-api.openapi.json stays in sync with s3-api.openapi.yaml.
# Usage: ./schemas/check_sync.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="${SCRIPT_DIR}/../python/.venv/bin/python"

"$VENV" -c "
import yaml, json, sys
y = yaml.safe_load(open('${SCRIPT_DIR}/s3-api.openapi.yaml'))
j = json.load(open('${SCRIPT_DIR}/s3-api.openapi.json'))
if y != j:
    print('ERROR: s3-api.openapi.json is out of sync with s3-api.openapi.yaml', file=sys.stderr)
    sys.exit(1)
print('OK: JSON and YAML specs are in sync')
"
