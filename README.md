# BleepStore

A production-ready, S3-compatible object store with four independent implementations in Python, Go, Rust, and Zig.

## Overview

BleepStore is a clean-room implementation of the AWS S3 API, designed to run in two modes:

- **Embedded mode**: Single-node, minimal resources, SQLite-backed metadata
- **Cluster mode**: Multi-node, Raft-based consensus (planned)

All four implementations share the same API contract and pass the same E2E test suite.

## Features

- Full S3 API compatibility (buckets, objects, multipart uploads, presigned URLs, ACLs)
- AWS Signature V4 authentication
- Multiple storage backends (local filesystem, AWS S3, GCP Cloud Storage, Azure Blob)
- Prometheus metrics and health endpoints
- OpenAPI/Swagger UI documentation
- Crash-only design for reliability

## Quick Start

Choose your preferred implementation:

| Implementation | Port | Language | README | Deployment Guide |
|---------------|------|----------|--------|------------------|
| Python | 9010 | Python 3.11+ | [python/README.md](python/README.md) | [python/docs/DEPLOYMENT.md](python/docs/DEPLOYMENT.md) |
| Go | 9011 | Go 1.21+ | [golang/README.md](golang/README.md) | [golang/docs/DEPLOYMENT.md](golang/docs/DEPLOYMENT.md) |
| Rust | 9012 | Rust 1.75+ | [rust/README.md](rust/README.md) | [rust/docs/DEPLOYMENT.md](rust/docs/DEPLOYMENT.md) |
| Zig | 9013 | Zig 0.15+ | [zig/README.md](zig/README.md) | [zig/docs/DEPLOYMENT.md](zig/docs/DEPLOYMENT.md) |

## Build & Run

### Python

```bash
cd python
uv venv .venv && source .venv/bin/activate
uv pip install -e ".[dev]"
bleepstore --config ../bleepstore.example.yaml --port 9010
```

### Go

```bash
cd golang
go build -o bleepstore ./cmd/bleepstore
./bleepstore --config ../bleepstore.example.yaml --port 9011
```

### Rust

```bash
cd rust
cargo build --release
./target/release/bleepstore --config ../bleepstore.example.yaml --bind 0.0.0.0:9012
```

### Zig

```bash
cd zig
zig build -Doptimize=ReleaseFast
./zig-out/bin/bleepstore --config ../bleepstore.example.yaml --port 9013
```

## Configuration

All implementations use the same YAML configuration format. See [bleepstore.example.yaml](bleepstore.example.yaml) for a complete example.

```yaml
server:
  host: "0.0.0.0"
  port: 9000
  region: "us-east-1"

auth:
  access_key: "bleepstore"
  secret_key: "bleepstore-secret"

metadata:
  engine: "sqlite"
  sqlite:
    path: "./data/metadata.db"

storage:
  backend: "local"
  local:
    root_dir: "./data/objects"
```

## Testing

Each implementation includes unit tests and shares a common E2E test suite:

```bash
# Run E2E tests against a running server
BLEEPSTORE_ENDPOINT=http://localhost:9010 tests/run_tests.sh
```

## API Documentation

When running, access the Swagger UI at `http://localhost:PORT/docs` and OpenAPI spec at `http://localhost:PORT/openapi.json`.

## Project Structure

```
bleepstore/
├── python/          # Python implementation
├── golang/          # Go implementation
├── rust/            # Rust implementation
├── zig/             # Zig implementation
├── specs/           # S3 API specifications
├── tests/           # Shared E2E tests
├── schemas/         # OpenAPI schemas
└── benchmarking/    # Performance benchmarks
```

## License

MIT
