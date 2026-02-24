# Persistent Event Queues

## Overview

BleepStore supports **optional persistent queues** for task execution and event
propagation. When enabled, write operations follow a **write-first,
read-from-queue** pattern that provides optional consistency guarantees and
enables event-driven architectures.

This is a pluggable system supporting three queue backends:
- **Redis** (Streams or Lists)
- **RabbitMQ** (AMQP 0-9-1, compatible with ActiveMQ via AMQP)
- **Kafka** (Apache Kafka)

When no queue is configured, BleepStore operates in **direct mode** (default):
writes go directly to storage + metadata with no queue involvement.

---

## Architecture

### Direct Mode (Default — No Queue)

```
Client → HTTP Handler → Storage Backend + Metadata Store → Response
```

### Queue-Enabled Mode (Write-First, Read-From-Queue)

```
Write Path:
  Client → HTTP Handler → Queue (persist intent) → Ack to client
                                    ↓
  Queue Consumer → Storage Backend + Metadata Store → Mark complete

Read Path:
  Client → HTTP Handler → Metadata Store → Storage Backend → Response
```

### Consistency Modes

When queues are enabled, the operator chooses a consistency mode:

| Mode | Behavior | Trade-off |
|------|----------|-----------|
| `sync` | Handler writes to queue, waits for consumer to complete, then responds | Consistent reads, higher latency |
| `async` | Handler writes to queue, responds immediately with 202 Accepted | Lower latency, eventually consistent reads |
| `write-through` | Handler writes to both queue AND storage, responds after storage commit. Queue used for events/replication only | Consistent reads, queue for side effects |

**Default:** `write-through` (when queues are enabled). This provides the
consistency of direct mode while enabling event propagation.

---

## Configuration

```yaml
# In bleepstore.yaml
queue:
  enabled: false                # Default: no queue
  backend: redis                # redis | rabbitmq | kafka
  consistency: write-through    # sync | async | write-through

  redis:
    url: redis://localhost:6379
    stream_prefix: bleepstore   # Stream/key prefix
    consumer_group: bleepstore-workers
    max_retries: 3
    retry_delay_ms: 1000

  rabbitmq:
    url: amqp://guest:guest@localhost:5672/
    exchange: bleepstore
    exchange_type: topic        # topic | direct | fanout
    queue_prefix: bleepstore
    durable: true
    prefetch_count: 10
    max_retries: 3
    retry_delay_ms: 1000

  kafka:
    brokers:
      - localhost:9092
    topic_prefix: bleepstore
    consumer_group: bleepstore-workers
    acks: all                   # all | 1 | 0
    max_retries: 3
    retry_delay_ms: 1000
    partitions: 6
    replication_factor: 1
```

---

## Event Types

All S3 operations that modify state produce events:

| Event Type | Trigger | Payload |
|-----------|---------|---------|
| `bucket.created` | CreateBucket | bucket name, region, owner |
| `bucket.deleted` | DeleteBucket | bucket name |
| `object.created` | PutObject, CopyObject, CompleteMultipartUpload | bucket, key, size, etag, content_type |
| `object.deleted` | DeleteObject | bucket, key |
| `objects.deleted` | DeleteObjects (multi) | bucket, keys[] |
| `object.acl.updated` | PutObjectAcl | bucket, key, acl |
| `bucket.acl.updated` | PutBucketAcl | bucket, acl |
| `multipart.created` | CreateMultipartUpload | bucket, key, upload_id |
| `multipart.completed` | CompleteMultipartUpload | bucket, key, upload_id, etag |
| `multipart.aborted` | AbortMultipartUpload | bucket, key, upload_id |
| `part.uploaded` | UploadPart | bucket, key, upload_id, part_number, etag |

### Event Envelope

```json
{
  "id": "evt_01HXYZ...",
  "type": "object.created",
  "timestamp": "2026-02-22T12:00:00.000Z",
  "source": "bleepstore-node-1",
  "request_id": "4442587FB7D0A2F9",
  "data": {
    "bucket": "my-bucket",
    "key": "photos/cat.jpg",
    "size": 1048576,
    "etag": "\"d41d8cd98f00b204e9800998ecf8427e\"",
    "content_type": "image/jpeg"
  }
}
```

- `id`: Unique event ID (ULID or UUID). Serves as idempotency key for consumers.
- `type`: Dot-separated event type.
- `timestamp`: ISO 8601 with milliseconds.
- `source`: Node identifier (for clustered deployments).
- `request_id`: The S3 request ID that triggered this event.

---

## Queue Backend Interface

Each implementation defines a `QueueBackend` interface/trait/protocol:

```
interface QueueBackend {
    // Lifecycle
    connect(config) -> void
    close() -> void
    health_check() -> bool

    // Publishing
    publish(event: Event) -> void
    publish_batch(events: []Event) -> void

    // Consuming (for sync/async modes)
    subscribe(event_types: []string, handler: fn(Event)) -> void
    acknowledge(event_id: string) -> void

    // Task queue (for write-first pattern)
    enqueue_task(task: WriteTask) -> task_id
    dequeue_task() -> WriteTask | None
    complete_task(task_id: string) -> void
    fail_task(task_id: string, error: string) -> void
    retry_failed_tasks() -> int
}
```

### WriteTask Structure

For `sync` and `async` consistency modes, write operations are enqueued as tasks:

```json
{
  "task_id": "task_01HXYZ...",
  "operation": "PutObject",
  "created_at": "2026-02-22T12:00:00.000Z",
  "attempts": 0,
  "max_retries": 3,
  "payload": {
    "bucket": "my-bucket",
    "key": "photos/cat.jpg",
    "content_type": "image/jpeg",
    "content_length": 1048576,
    "data_ref": "/tmp/bleepstore/pending/task_01HXYZ..."
  }
}
```

For `PutObject` in async/sync mode, the request body is written to a temporary
file first (following crash-only temp-fsync-rename), and the task payload
references this temp file. The consumer reads the temp file, writes to final
storage, commits metadata, then deletes the temp file.

---

## Backend-Specific Details

### Redis

- **Streams** for ordered, persistent event log with consumer groups
- `XADD bleepstore:events * type object.created data '...'`
- `XREADGROUP GROUP bleepstore-workers consumer-1 COUNT 10 BLOCK 5000 STREAMS bleepstore:events >`
- `XACK bleepstore:events bleepstore-workers <id>`
- **Lists** (RPUSH/BLPOP) as alternative for simple task queue
- Dead letter: failed messages moved to `bleepstore:dead-letter` after max retries

### RabbitMQ / ActiveMQ

- **Topic exchange** for event routing (e.g., `object.created` routed to subscribers)
- Durable queues with manual ack
- Dead letter exchange for failed messages
- Prefetch count controls concurrency
- Compatible with ActiveMQ via AMQP 0-9-1 protocol

### Kafka

- **Topics** per event type (e.g., `bleepstore.object.created`)
- Consumer groups for parallel processing
- `acks=all` for durability guarantee
- Compacted topics optional for latest-state queries
- Partitioned by bucket name for ordering within a bucket

---

## Write-First Pattern (Consistency Fix)

The write-first pattern ensures that no write is lost, even if the storage
backend or metadata store is temporarily unavailable:

### sync mode

```
1. Client sends PutObject
2. Handler validates request
3. Handler writes request body to temp file (fsync)
4. Handler enqueues WriteTask to queue (with temp file reference)
5. Handler waits (blocks) for task completion
6. Consumer dequeues task, writes to storage, commits metadata
7. Consumer marks task complete
8. Handler receives completion, responds 200 to client
```

If the server crashes between steps 4 and 7, the task remains in the queue
and is retried on next startup (crash-only: startup reprocesses pending tasks).

### async mode

```
1. Client sends PutObject
2. Handler validates request
3. Handler writes request body to temp file (fsync)
4. Handler enqueues WriteTask to queue
5. Handler responds 202 Accepted immediately (with task_id in response header)
6. Consumer processes task asynchronously
```

Client can poll for completion status via a status endpoint or rely on
eventual consistency.

### write-through mode

```
1. Client sends PutObject
2. Handler validates request
3. Handler writes to storage + metadata (normal direct path)
4. Handler publishes event to queue (fire-and-forget or best-effort)
5. Handler responds 200 to client
```

Queue failure does not block the write. Events are used for:
- Cross-region replication
- Webhook notifications
- Audit logging
- Cache invalidation
- External system integration

---

## Crash-Only Integration

The queue system follows crash-only principles:

1. **Queue state survives crashes** — Redis, RabbitMQ, and Kafka all persist
   messages. Unacknowledged messages are redelivered on reconnect.
2. **Idempotent consumers** — Consumer operations use the event/task ID as
   idempotency key. Reprocessing the same event is safe.
3. **Startup reprocesses pending** — On startup, the queue consumer reconnects
   and picks up any unacknowledged tasks from before the crash.
4. **No in-memory buffering** — Events are published directly to the queue,
   never buffered in memory.
5. **Temp files for payloads** — Large payloads (object data) are written to
   temp files before enqueueing. Crash-safe via temp-fsync-rename. Orphan temp
   files cleaned on startup.

---

## Implementation Stages

Queue support is implemented in **Stage 16** (new stage added to the plan):

1. **Queue backend interface** — Define the abstract interface
2. **Redis backend** — Implement using Redis Streams
3. **RabbitMQ backend** — Implement using AMQP
4. **Kafka backend** — Implement using native Kafka protocol
5. **Write-through mode** — Publish events after direct writes
6. **Sync mode** — Write-first with blocking wait
7. **Async mode** — Write-first with immediate response
8. **Configuration** — Queue section in config, backend selection
9. **Startup integration** — Reconnect, reprocess pending tasks
10. **Health check** — Queue connectivity in health endpoint

### Language-Specific Libraries

| Language | Redis | RabbitMQ | Kafka |
|----------|-------|----------|-------|
| Python | `redis[hiredis]` / `aioredis` | `aio-pika` | `aiokafka` |
| Go | `github.com/redis/go-redis/v9` | `github.com/rabbitmq/amqp091-go` | `github.com/segmentio/kafka-go` |
| Rust | `redis` (tokio) | `lapin` | `rdkafka` |
| Zig | Custom Redis protocol (std.net) | Custom AMQP (std.net) | Custom Kafka protocol (std.net) |

---

## Monitoring & Observability

When queues are enabled, expose metrics:

| Metric | Description |
|--------|-------------|
| `bleepstore_queue_published_total` | Events published |
| `bleepstore_queue_consumed_total` | Events consumed |
| `bleepstore_queue_failed_total` | Failed deliveries |
| `bleepstore_queue_pending` | Pending tasks in queue |
| `bleepstore_queue_dead_letter` | Messages in dead letter |
| `bleepstore_queue_latency_ms` | End-to-end queue latency |

Health check endpoint (`GET /health`) should include queue status:

```json
{
  "status": "ok",
  "queue": {
    "backend": "redis",
    "connected": true,
    "pending_tasks": 0,
    "dead_letter_count": 0
  }
}
```
