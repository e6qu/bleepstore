"""Prometheus metrics definitions for BleepStore.

All custom BleepStore metrics use the ``bleepstore_`` prefix for namespace
isolation.  These are *application-level* S3 operation metrics; the
``prometheus-fastapi-instrumentator`` package provides automatic HTTP-level
metrics (request count, duration, sizes including request/response size
summaries).

Crash-only design: counters reset to zero on restart.  Prometheus handles
gaps via ``rate()``.  Gauges (objects/buckets totals) are populated from the
metadata store on startup when available; otherwise they stay at zero.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge

# Flag indicating whether metrics have been initialised via init_metrics().
_initialized: bool = False

# ---------------------------------------------------------------------------
# S3 operation counter  (labels: operation, status)
# ---------------------------------------------------------------------------
s3_operations_total: Counter | None = None

# ---------------------------------------------------------------------------
# Object & bucket gauges
# ---------------------------------------------------------------------------
objects_total: Gauge | None = None
buckets_total: Gauge | None = None

# ---------------------------------------------------------------------------
# Byte counters
# ---------------------------------------------------------------------------
bytes_received_total: Counter | None = None
bytes_sent_total: Counter | None = None


def init_metrics() -> None:
    """Create and register all Prometheus metrics.

    This must be called once when metrics are enabled.  When metrics are
    disabled in config the module-level references stay ``None`` and no
    collectors are registered in the global registry.

    Note: ``http_request_size_bytes``, ``http_response_size_bytes``, and
    ``http_request_duration_seconds`` are provided by the
    ``prometheus-fastapi-instrumentator`` library (registered with the
    ``bleepstore`` namespace) and are NOT duplicated here.
    """
    global _initialized
    global s3_operations_total, objects_total, buckets_total
    global bytes_received_total, bytes_sent_total

    if _initialized:
        return

    s3_operations_total = Counter(
        "bleepstore_s3_operations_total",
        "Total S3 operations by type and outcome",
        ["operation", "status"],
    )

    objects_total = Gauge(
        "bleepstore_objects_total",
        "Total number of objects across all buckets",
    )

    buckets_total = Gauge(
        "bleepstore_buckets_total",
        "Total number of buckets",
    )

    bytes_received_total = Counter(
        "bleepstore_bytes_received_total",
        "Total bytes received in request bodies",
    )

    bytes_sent_total = Counter(
        "bleepstore_bytes_sent_total",
        "Total bytes sent in response bodies",
    )

    _initialized = True
