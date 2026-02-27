# BleepStore — Go Implementation

An S3-compatible object store implemented in Go using net/http and Huma.

## Prerequisites

- Go 1.21+

## Quick Start

```bash
# Setup and build
cd golang/
go mod download
go build -o bleepstore ./cmd/bleepstore

# Run
./bleepstore --config ../bleepstore.example.yaml --port 9011
```

## Documentation

- **[Deployment Guide](docs/DEPLOYMENT.md)** - Full deployment instructions including Docker, systemd, and production considerations

## Testing

```bash
# Unit tests
go test ./... -v

# With race detector
go test ./... -v -race

# E2E tests (requires running server)
./run_e2e.sh
```

## Development

```bash
# Format
go fmt ./...

# Vet
go vet ./...

# Build optimized
go build -ldflags="-s -w" -o bleepstore ./cmd/bleepstore
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
