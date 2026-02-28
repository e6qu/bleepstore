# BleepStore Zig — Deployment Guide

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

- **Zig**: 0.15.0 or higher ([install guide](https://ziglang.org/learn/getting-started/))
- **SQLite**: 3.x (system library, linked at build time)
- **ZLS** (optional): Zig Language Server for IDE support

### Installing Zig

**macOS:**
```bash
brew install zig
```

**Linux:**
```bash
# Download from https://ziglang.org/download/
tar -xf zig-linux-x86_64-0.15.0.tar.xz
sudo mv zig-linux-x86_64-0.15.0 /opt/zig
sudo ln -s /opt/zig/zig /usr/local/bin/zig
```

**Windows:**
```powershell
# Using scoop
scoop install zig
```

## Installation

### From Source

```bash
cd zig/

# Build debug version
zig build

# Build optimized release version
zig build -Doptimize=ReleaseFast
```

The binary will be at `./zig-out/bin/bleepstore`.

### Verify Installation

```bash
./zig-out/bin/bleepstore --version
./zig-out/bin/bleepstore --help
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
# Run directly with zig build
zig build run -- --config ../bleepstore.example.yaml

# Run with custom port
zig build run -- --config ../bleepstore.example.yaml --port 9013

# Or run the compiled binary directly
./zig-out/bin/bleepstore --config ../bleepstore.example.yaml --port 9013
```

### Command Line Options

```
bleepstore [OPTIONS]

Options:
  --config <PATH>       Path to configuration file
  --port <INTEGER>      Override port from config
  --help                Show this message and exit
```

### As a Systemd Service

Create `/etc/systemd/system/bleepstore.service`:

```ini
[Unit]
Description=BleepStore S3-Compatible Object Store (Zig)
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
FROM ziglang/zig:0.15.0 AS builder

WORKDIR /app
COPY build.zig build.zig.zon ./
COPY src ./src

RUN zig build -Doptimize=ReleaseFast

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates libsqlite3-0 && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=builder /app/zig-out/bin/bleepstore /app/bleepstore

EXPOSE 9000

ENTRYPOINT ["./bleepstore"]
CMD ["--config", "/etc/bleepstore.yaml"]
```

Build:

```bash
docker build -t bleepstore-zig:latest .
```

### Run Container

```bash
docker run -d \
  --name bleepstore \
  -p 9013:9000 \
  -v /path/to/bleepstore.yaml:/etc/bleepstore.yaml:ro \
  -v /path/to/data:/data \
  bleepstore-zig:latest
```

### Docker Compose

```yaml
version: '3.8'

services:
  bleepstore:
    image: bleepstore-zig:latest
    ports:
      - "9013:9000"
    volumes:
      - ./bleepstore.yaml:/etc/bleepstore.yaml:ro
      - bleepstore-data:/data
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/healthz"]
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

1. **Use release builds** - `zig build -Doptimize=ReleaseFast` for optimized binary
2. **Use SSD storage** - Place SQLite database and object storage on SSD
3. **Memory management** - Zig uses a GeneralPurposeAllocator with leak detection in debug builds

### Reliability

1. **Backup SQLite database** - Regular snapshots of `metadata.db`
2. **Monitor disk space** - Set alerts for storage capacity
3. **Graceful shutdown** - SIGTERM triggers graceful drain

### Resource Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 1 core | 2+ cores |
| RAM | 64 MB | 128+ MB |
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
lsof -i :9013
# Kill the process or use a different port
./zig-out/bin/bleepstore --config bleepstore.yaml --port 9020
```

**Permission denied on data directory**
```bash
chown -R bleepstore:bleepstore ./data/
chmod 750 ./data/
```

**SQLite library not found**
```bash
# macOS
brew install sqlite

# Ubuntu/Debian
sudo apt-get install libsqlite3-dev

# Fedora
sudo dnf install sqlite-devel
```

**Memory leaks detected**
- Debug builds use GeneralPurposeAllocator with leak detection
- Run `zig build test` to check for leaks in tests
- Production ReleaseFast builds do not include leak detection overhead

### Debug Mode

Run in debug mode for detailed output:

```bash
zig build run -- --config ../bleepstore.example.yaml
```

### Running Tests

```bash
# Run all tests
zig build test

# Run tests for a specific file
zig test src/metadata/sqlite.zig

# Check for memory leaks (debug builds include leak detection)
zig build test
```

### Formatting Code

```bash
# Format all files
zig fmt src/

# Check formatting without modifying
zig fmt --check src/
```
