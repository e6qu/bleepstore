# BleepStore Python — Deployment Guide

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

- **Python**: 3.11 or higher
- **uv**: Package manager ([install guide](https://docs.astral.sh/uv/))
- **SQLite**: 3.x (usually pre-installed)

## Installation

### From Source

```bash
cd python/

# Create virtual environment
uv venv .venv
source .venv/bin/activate  # Linux/macOS
# or: .venv\Scripts\activate  # Windows

# Install package with dependencies
uv pip install -e ".[dev]"
```

### Verify Installation

```bash
bleepstore --version
# or
python -m bleepstore.cli --help
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

### Environment Variables

Credentials can be set via environment variables:

```bash
export BLEEPSTORE_ACCESS_KEY="your-access-key"
export BLEEPSTORE_SECRET_KEY="your-secret-key"
```

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
  memory:
    max_size_bytes: 1073741824  # 1GB limit
    persistence: "snapshot"
    snapshot_path: "./data/memory.snap"
    snapshot_interval_seconds: 300
```

#### SQLite (Single-file storage)

```yaml
storage:
  backend: "sqlite"
  # Uses the same database as metadata
```

#### AWS S3 Gateway

```yaml
storage:
  backend: "aws"
  aws:
    bucket: "my-backing-bucket"
    region: "us-east-1"
    prefix: "bleepstore/"
    # endpoint_url: "https://s3.amazonaws.com"
    # access_key_id: ""  # Falls back to AWS_ACCESS_KEY_ID env
    # secret_access_key: ""  # Falls back to AWS_SECRET_ACCESS_KEY env
```

#### GCP Cloud Storage Gateway

```yaml
storage:
  backend: "gcp"
  gcp:
    bucket: "my-backing-bucket"
    project: "my-project"
    prefix: "bleepstore/"
    # credentials_file: ""  # Falls back to GOOGLE_APPLICATION_CREDENTIALS env
```

#### Azure Blob Storage Gateway

```yaml
storage:
  backend: "azure"
  azure:
    container: "my-container"
    account: "my-account"
    prefix: "bleepstore/"
    # connection_string: ""  # Alternative to account-based auth
    # use_managed_identity: false
```

## Running

### Development

```bash
# Activate virtual environment first
source .venv/bin/activate

# Run with default config
bleepstore --config ../bleepstore.example.yaml

# Run with custom port
bleepstore --config ../bleepstore.example.yaml --port 9010

# Run with debug logging
bleepstore --config ../bleepstore.example.yaml --log-level debug
```

### Command Line Options

```
bleepstore [OPTIONS]

Options:
  --config PATH         Path to configuration file [required]
  --port INTEGER        Override port from config
  --host TEXT           Override bind address from config
  --log-level TEXT      Log level: debug, info, warning, error
  --version             Show version and exit
  --help                Show this message and exit
```

### As a Systemd Service

Create `/etc/systemd/system/bleepstore.service`:

```ini
[Unit]
Description=BleepStore S3-Compatible Object Store
After=network.target

[Service]
Type=simple
User=bleepstore
Group=bleepstore
WorkingDirectory=/opt/bleepstore/python
Environment="PATH=/opt/bleepstore/python/.venv/bin"
ExecStart=/opt/bleepstore/python/.venv/bin/bleepstore --config /etc/bleepstore.yaml
Restart=always
RestartSec=5

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
FROM python:3.12-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy package files
COPY pyproject.toml uv.lock* ./
COPY src/ ./src/

# Install dependencies
RUN uv pip install --system -e "."

# Create data directory
RUN mkdir -p /data

EXPOSE 9000

ENTRYPOINT ["bleepstore"]
CMD ["--config", "/etc/bleepstore.yaml"]
```

Build:

```bash
docker build -t bleepstore-python:latest .
```

### Run Container

```bash
docker run -d \
  --name bleepstore \
  -p 9000:9000 \
  -v /path/to/bleepstore.yaml:/etc/bleepstore.yaml:ro \
  -v /path/to/data:/data \
  bleepstore-python:latest
```

### Docker Compose

```yaml
version: '3.8'

services:
  bleepstore:
    image: bleepstore-python:latest
    ports:
      - "9000:9000"
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
2. **Use TLS** - Deploy behind a reverse proxy (nginx, Traefik, AWS ALB) with TLS termination
3. **Network isolation** - Bind to private interfaces only; use firewall rules
4. **File permissions** - Ensure data directories are only accessible by the service user

### Performance

1. **Use SSD storage** - Place SQLite database and object storage on SSD
2. **Tune SQLite** - The defaults are optimized; advanced tuning in `metadata/sqlite.py`
3. **Connection pooling** - uvicorn handles this automatically

### Reliability

1. **Backup SQLite database** - Regular snapshots of `metadata.db`
2. **Monitor disk space** - Set alerts for storage capacity
3. **Log rotation** - Configure log rotation for the logs directory

### Resource Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 1 core | 2+ cores |
| RAM | 512 MB | 1+ GB |
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

### Prometheus Scrape Config

```yaml
scrape_configs:
  - job_name: 'bleepstore'
    static_configs:
      - targets: ['localhost:9000']
    metrics_path: '/metrics'
```

## Troubleshooting

### Common Issues

**Port already in use**
```bash
# Check what's using the port
lsof -i :9000
# Kill the process or use a different port
bleepstore --config bleepstore.yaml --port 9001
```

**Permission denied on data directory**
```bash
# Ensure the user running bleepstore owns the data directory
chown -R bleepstore:bleepstore ./data/
chmod 750 ./data/
```

**SQLite database locked**
- Ensure only one instance is running
- Check for stale lock files in `./data/`

### Logs

Logs are written to `./logs/` directory (configurable). Check:

```bash
tail -f logs/bleepstore.log
```

### Debug Mode

Enable verbose logging:

```bash
bleepstore --config bleepstore.yaml --log-level debug
```
