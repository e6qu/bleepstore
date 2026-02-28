# BleepStore Go — Deployment Guide

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Running](#running)
5. [Docker Deployment](#docker-deployment)
6. [Production Considerations](#production-considerations)
7. [Monitoring](#monitoring)
8. [Troubleshooting](#troubleshooting)

## Prerequisites

- **Go**: 1.21 or higher
- **SQLite**: 3.x (built-in via modernc.org/sqlite - pure Go, no CGO)

## Installation

### From Source

```bash
cd golang/

# Download dependencies
go mod download

# Build binary
go build -o bleepstore ./cmd/bleepstore
```

### Verify Installation

```bash
./bleepstore --version
./bleepstore --help
```

## Configuration

### Configuration File

Copy the example configuration and customize:

```bash
cp ../bleepstore.example.yaml bleepstore.yaml
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `server.host` | `0.0.0.0` | Bind address |
| `server.port` | `9000` | HTTP port |
| `server.region` | `us-east-1` | AWS region identifier |
| `auth.access_key` | `bleepstore` | Access key for SigV4 |
| `auth.secret_key` | `bleepstore-secret` | Secret key for SigV4 |
| `metadata.engine` | `sqlite` | Metadata backend |
| `metadata.sqlite.path` | `./data/metadata.db` | SQLite database path |
| `storage.backend` | `local` | Storage backend type |
| `storage.local.root_dir` | `./data/objects` | Object storage directory |
| `observability.metrics` | `true` | Enable Prometheus metrics |
| `observability.health_check` | `true` | Enable health endpoints |

### Storage Backends

#### Local Filesystem (Default)

```yaml
storage:
  backend: "local"
  local:
    root_dir: "./data/objects"
```

#### In-Memory (Testing)

```yaml
storage:
  backend: "memory"
```

#### SQLite (Single-file storage)

```yaml
storage:
  backend: "sqlite"
```

#### AWS S3 Gateway

```yaml
storage:
  backend: "aws"
  aws:
    bucket: "my-backing-bucket"
    region: "us-east-1"
    prefix: "bleepstore/"
```

#### GCP Cloud Storage Gateway

```yaml
storage:
  backend: "gcp"
  gcp:
    bucket: "my-backing-bucket"
    project: "my-project"
    prefix: "bleepstore/"
```

#### Azure Blob Storage Gateway

```yaml
storage:
  backend: "azure"
  azure:
    container: "my-container"
    account: "my-account"
    prefix: "bleepstore/"
```

## Running

### Development

```bash
# Run directly with go run
go run ./cmd/bleepstore --config ../bleepstore.example.yaml

# Run with custom port
go run ./cmd/bleepstore --config ../bleepstore.example.yaml --port 9011

# Or run the compiled binary
./bleepstore --config ../bleepstore.example.yaml --port 9011
```

### Command Line Options

```
bleepstore [OPTIONS]

Options:
  --config PATH         Path to configuration file [required]
  --port INTEGER        Override port from config
  --host TEXT           Override bind address from config
  --version             Show version and exit
  --help                Show this message and exit
```

### As a Systemd Service

Create `/etc/systemd/system/bleepstore.service`:

```ini
[Unit]
Description=BleepStore S3-Compatible Object Store (Go)
After=network.target

[Service]
Type=simple
User=bleepstore
Group=bleepstore
WorkingDirectory=/opt/bleepstore
ExecStart=/opt/bleepstore/bleepstore --config /etc/bleepstore.yaml
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable bleepstore
sudo systemctl start bleepstore
sudo systemctl status bleepstore
```

## Docker Deployment

### Build Image

Create `Dockerfile`:

```dockerfile
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bleepstore ./cmd/bleepstore

FROM alpine:3.19

RUN apk --no-cache add ca-certificates
WORKDIR /app

COPY --from=builder /app/bleepstore /app/bleepstore

EXPOSE 9000

ENTRYPOINT ["./bleepstore"]
CMD ["--config", "/etc/bleepstore.yaml"]
```

Build:

```bash
docker build -t bleepstore-go:latest .
```

### Run Container

```bash
docker run -d \
  --name bleepstore \
  -p 9011:9000 \
  -v /path/to/bleepstore.yaml:/etc/bleepstore.yaml:ro \
  -v /path/to/data:/data \
  bleepstore-go:latest
```

### Docker Compose

```yaml
version: '3.8'

services:
  bleepstore:
    image: bleepstore-go:latest
    ports:
      - "9011:9000"
    volumes:
      - ./bleepstore.yaml:/etc/bleepstore.yaml:ro
      - bleepstore-data:/data
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:9000/healthz"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  bleepstore-data:
```

## Production Considerations

### Security

1. **Change default credentials** - Never use the default access/secret keys in production
2. **Use TLS** - Deploy behind a reverse proxy with TLS termination
3. **Network isolation** - Bind to private interfaces only
4. **File permissions** - Ensure data directories are only accessible by the service user

### Performance

1. **Use SSD storage** - Place SQLite database and object storage on SSD
2. **Build with optimizations** - `go build -ldflags="-s -w"` for smaller binary
3. **Tune file limits** - Increase `LimitNOFILE` for high connection counts

### Reliability

1. **Backup SQLite database** - Regular snapshots of `metadata.db`
2. **Monitor disk space** - Set alerts for storage capacity
3. **Graceful shutdown** - SIGTERM triggers graceful drain (30s default)

### Resource Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 1 core | 2+ cores |
| RAM | 256 MB | 512+ MB |
| Disk | 10 GB | SSD, sized for data |

## Monitoring

### Health Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/healthz` | Liveness probe |
| `/readyz` | Readiness probe |
| `/health` | Combined health check |

### Prometheus Metrics

Metrics available at `/metrics`:

| Metric | Description |
|--------|-------------|
| `bleepstore_http_requests_total` | Total HTTP requests |
| `bleepstore_http_request_duration_seconds` | Request latency histogram |
| `bleepstore_s3_operations_total` | S3 operation counts |
| `bleepstore_objects_total` | Total object count |
| `bleepstore_buckets_total` | Total bucket count |
| `bleepstore_bytes_received_total` | Bytes received |
| `bleepstore_bytes_sent_total` | Bytes sent |

## Troubleshooting

### Common Issues

**Port already in use**
```bash
# Check what's using the port
lsof -i :9011
# Kill the process or use a different port
./bleepstore --config bleepstore.yaml --port 9020
```

**Permission denied on data directory**
```bash
chown -R bleepstore:bleepstore ./data/
chmod 750 ./data/
```

**SQLite database locked**
- Ensure only one instance is running
- Check for stale lock files

### Debug Mode

Enable verbose logging via config:

```yaml
logging:
  level: debug
```

### Running Tests

```bash
# Unit tests
go test ./... -v

# With race detector
go test ./... -v -race

# E2E tests
./run_e2e.sh
```
