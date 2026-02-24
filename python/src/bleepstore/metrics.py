"""Prometheus metrics definitions for BleepStore.

All custom BleepStore metrics use the ``bleepstore_`` prefix for namespace
isolation.  These are *application-level* S3 operation metrics; the
``prometheus-fastapi-instrumentator`` package provides automatic HTTP-level
metrics (request count, duration, sizes).

Crash-only design: counters reset to zero on restart.  Prometheus handles
gaps via ``rate()``.  Gauges (objects/buckets totals) are populated from the
metadata store on startup when available; otherwise they stay at zero.
"""

from prometheus_client import Counter, Gauge, Histogram

# ---------------------------------------------------------------------------
# S3 operation counter  (labels: operation, status)
# ---------------------------------------------------------------------------
s3_operations_total = Counter(
    "bleepstore_s3_operations_total",
    "Total S3 operations by type and outcome",
    ["operation", "status"],
)

# ---------------------------------------------------------------------------
# Object & bucket gauges
# ---------------------------------------------------------------------------
objects_total = Gauge(
    "bleepstore_objects_total",
    "Total number of objects across all buckets",
)

buckets_total = Gauge(
    "bleepstore_buckets_total",
    "Total number of buckets",
)

# ---------------------------------------------------------------------------
# Byte counters
# ---------------------------------------------------------------------------
bytes_received_total = Counter(
    "bleepstore_bytes_received_total",
    "Total bytes received in request bodies",
)

bytes_sent_total = Counter(
    "bleepstore_bytes_sent_total",
    "Total bytes sent in response bodies",
)

# ---------------------------------------------------------------------------
# Request duration histogram (custom buckets per spec)
# ---------------------------------------------------------------------------
http_request_duration_seconds = Histogram(
    "bleepstore_http_request_duration_seconds",
    "HTTP request latency in seconds",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
)
