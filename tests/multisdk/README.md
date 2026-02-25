# Multi-SDK S3 Test Framework

Pluggable test framework that runs the same S3 operations through every available S3 client. Same pattern as BleepStore's `StorageBackend` — define an abstract `S3Client` protocol, each SDK implements it, tests are parameterized over all registered clients.

## Quick Start

```bash
# Start any BleepStore implementation
cd zig && zig build run  # port 9013

# Run multi-SDK tests
BLEEPSTORE_ENDPOINT=http://localhost:9013 ../tests/run_multisdk.sh
```

## Available Clients

| Client | Install | Always available? |
|--------|---------|-------------------|
| **boto3** | `pip install boto3` | Yes (Python dependency) |
| **boto3-resource** | same package | Yes |
| **AWS CLI v2** | `brew install awscli` | No — auto-skipped if missing |
| **MinIO mc** | `brew install minio/stable/mc` | No — auto-skipped if missing |
| **s3cmd** | `brew install s3cmd` | No — auto-skipped if missing |
| **rclone** | `brew install rclone` | No — auto-skipped if missing |

## Running

```bash
# All available clients
BLEEPSTORE_ENDPOINT=http://localhost:9013 ./tests/run_multisdk.sh

# Filter by client
./tests/run_multisdk.sh -k boto3
./tests/run_multisdk.sh -k awscli
./tests/run_multisdk.sh -k mc

# Filter by operation
./tests/run_multisdk.sh -m bucket_ops
./tests/run_multisdk.sh -m object_ops
./tests/run_multisdk.sh -m large_file

# List tests without running
./tests/run_multisdk.sh --co
```

## Test Output

Each test name shows which client it runs through:

```
test_put_and_get_roundtrip[boto3]        PASSED
test_put_and_get_roundtrip[boto3-resource] PASSED
test_put_and_get_roundtrip[awscli]       PASSED
test_put_and_get_roundtrip[mc]           PASSED
```

## Architecture

```
tests/multisdk/
├── conftest.py              # Fixtures: s3 (parameterized), bucket, s3_client_boto3
├── requirements.txt         # boto3, pytest, pytest-timeout
├── clients/
│   ├── __init__.py          # discover_clients() auto-discovery
│   ├── base.py              # S3Client protocol + CliS3Client base
│   ├── boto3_client.py      # boto3 low-level client
│   ├── boto3_resource.py    # boto3 high-level resource API
│   ├── awscli.py            # AWS CLI v2 subprocess
│   ├── mc.py                # MinIO mc subprocess
│   ├── s3cmd.py             # s3cmd subprocess
│   └── rclone.py            # rclone subprocess
├── test_s3_operations.py    # ~30 tests, each runs per-client
└── README.md
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BLEEPSTORE_ENDPOINT` | `http://localhost:9000` | Server endpoint URL |
| `BLEEPSTORE_ACCESS_KEY` | `bleepstore` | AWS access key |
| `BLEEPSTORE_SECRET_KEY` | `bleepstore-secret` | AWS secret key |
| `BLEEPSTORE_REGION` | `us-east-1` | AWS region |
