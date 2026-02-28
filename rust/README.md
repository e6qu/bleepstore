# BleepStore — Rust Implementation

An S3-compatible object store implemented in Rust using axum.

## Prerequisites

- Rust 1.75+ (use [rustup](https://rustup.rs/))

## Quick Start

```bash
# Setup and build
cd rust/
cargo build --release

# Run
./target/release/bleepstore --config ../bleepstore.example.yaml --bind 0.0.0.0:9012
```

## Documentation

- **[Deployment Guide](docs/DEPLOYMENT.md)** - Full deployment instructions including Docker, systemd, and production considerations

## Testing

```bash
# Unit tests
cargo test

# E2E tests (requires running server)
./run_e2e.sh
```

## Development

```bash
# Format
cargo fmt

# Lint
cargo clippy

# Run with debug logging
RUST_LOG=debug cargo run -- --config ../bleepstore.example.yaml
```

## Configuration

See [bleepstore.example.yaml](../bleepstore.example.yaml) for configuration options.

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `/` | S3 API |
| `/docs` | Swagger UI |
| `/openapi.json` | OpenAPI spec |
| `/metrics` | Prometheus metrics |
| `/healthz` | Liveness probe |
| `/readyz` | Readiness probe |
