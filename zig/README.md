# BleepStore — Zig Implementation

An S3-compatible object store implemented in Zig using tokamak/httpz.

## Prerequisites

- Zig 0.15.0+
- SQLite 3.x (system library)

## Quick Start

```bash
# Setup and build
cd zig/
zig build -Doptimize=ReleaseFast

# Run
./zig-out/bin/bleepstore --config ../bleepstore.example.yaml --port 9013
```

## Documentation

- **[Deployment Guide](docs/DEPLOYMENT.md)** - Full deployment instructions including Docker, systemd, and production considerations

## Testing

```bash
# Unit tests
zig build test

# E2E tests (requires running server)
./run_e2e.sh
```

## Development

```bash
# Build (debug)
zig build

# Format
zig fmt src/

# Check formatting
zig fmt --check src/
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
