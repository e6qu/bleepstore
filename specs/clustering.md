# Clustering — Raft Consensus Architecture

## Overview

BleepStore supports two modes:

1. **Embedded mode** (single node): SQLite metadata, no Raft, minimal resources
2. **Cluster mode** (multi-node): Raft-replicated metadata, eventual consistency for reads

Cluster mode uses the Raft consensus protocol for metadata replication. Object data
is NOT replicated through Raft (too large) — data availability depends on the storage
backend.

---

## Consistency Model: Eventual Consistency

- **Writes** (create/update/delete bucket, object metadata): Go through Raft leader.
  The leader commits the write to the Raft log and replicates to a quorum of followers
  before acknowledging.
- **Reads** (list, get metadata, head): Can be served from any node (leader or follower).
  Followers may have slightly stale metadata until they apply the latest Raft log entries.
- **Object data reads/writes**: Handled by the storage backend independently of Raft.

### Guarantees
- Write-after-write consistency on the leader
- Read-your-writes consistency if the client always contacts the same node
- Eventually consistent reads across nodes (bounded by Raft log replication lag)

---

## Raft Protocol Summary

### Roles
- **Leader**: Handles all write requests, replicates log entries to followers
- **Follower**: Accepts log entries from leader, serves read requests
- **Candidate**: Temporary state during leader election

### State Machine
The Raft state machine is the SQLite metadata database. Each Raft log entry represents
a metadata operation (SQL statement). When a log entry is committed, it is applied to
the local SQLite database.

### Log Entry Types

```
enum LogEntryType:
    CreateBucket(name, region, owner, acl, created_at)
    DeleteBucket(name)
    PutObjectMeta(bucket, key, size, etag, content_type, metadata, acl, last_modified)
    DeleteObjectMeta(bucket, key)
    DeleteObjectsMeta(bucket, keys)
    PutBucketAcl(bucket, acl)
    PutObjectAcl(bucket, key, acl)
    CreateMultipartUpload(bucket, key, upload_id, metadata)
    RegisterPart(bucket, key, upload_id, part_number, etag, size)
    CompleteMultipartUpload(bucket, key, upload_id, parts, final_etag, final_size)
    AbortMultipartUpload(bucket, key, upload_id)
```

---

## Cluster Configuration

```yaml
cluster:
  enabled: true
  node_id: "node-1"
  bind_addr: "0.0.0.0:9001"     # Raft communication port
  advertise_addr: "10.0.1.1:9001" # Address visible to other nodes
  data_dir: "./data/raft"         # Raft log and snapshot storage
  peers:
    - id: "node-2"
      addr: "10.0.1.2:9001"
    - id: "node-3"
      addr: "10.0.1.3:9001"
  election_timeout_ms: 1000       # Follower election timeout
  heartbeat_interval_ms: 150      # Leader heartbeat interval
  snapshot_interval: 10000        # Log entries between snapshots
  snapshot_threshold: 8192        # Min entries before snapshot
  max_append_entries: 64          # Max entries per AppendEntries RPC
```

### Minimum Cluster Size
- 3 nodes for fault tolerance (tolerates 1 failure)
- 5 nodes for higher availability (tolerates 2 failures)
- Must be odd number for clear majority

---

## Node Discovery

### Static Configuration
Peers listed in config file. Simplest approach.

### DNS-Based Discovery
```yaml
cluster:
  discovery: "dns"
  dns:
    name: "bleepstore.service.consul"
    port: 9001
    refresh_interval_s: 30
```

---

## Raft RPCs

### AppendEntries
Leader → Followers. Replicates log entries and serves as heartbeat.

### RequestVote
Candidate → All nodes. Requests votes during leader election.

### InstallSnapshot
Leader → Follower. Transfers full state snapshot when follower is too far behind.

---

## Snapshots

The Raft log is compacted periodically by taking a snapshot of the SQLite database.

- Snapshot = copy of the SQLite database file at a point in time
- After snapshot, log entries before that point can be discarded
- New followers can bootstrap from the latest snapshot + subsequent log entries

---

## Write Flow (Cluster Mode)

```
Client → Any Node → Leader (redirect if not leader)
  1. Leader receives write request
  2. Leader creates Raft log entry
  3. Leader appends to local log
  4. Leader sends AppendEntries to followers
  5. Majority of nodes acknowledge → entry committed
  6. Leader applies entry to SQLite state machine
  7. Leader responds to client with success
  8. Followers apply entry to their SQLite on next replication
```

### Leader Redirect
If a follower receives a write request:
- Option A: Forward (proxy) to leader
- Option B: Return redirect with leader address in response header

BleepStore uses Option A (transparent proxying) so clients don't need cluster awareness.

---

## Read Flow (Cluster Mode — Eventual Consistency)

```
Client → Any Node
  1. Node queries its local SQLite database
  2. Returns result immediately (may be slightly stale)
```

No Raft involvement for reads. This provides eventual consistency with low latency.

---

## Failure Handling

### Leader Failure
1. Followers detect missing heartbeats after `election_timeout_ms`
2. A follower becomes candidate and starts election
3. Candidate with most up-to-date log wins
4. New leader begins accepting writes
5. In-flight writes to old leader may fail — clients should retry

### Follower Failure
- Cluster continues operating as long as a quorum exists
- Recovering follower catches up from leader's log or snapshot

### Network Partition
- Partition with majority continues operating
- Partition with minority becomes read-only (stale reads)
- On partition heal, minority nodes catch up

---

## Admin API — Cluster Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/admin/cluster/status` | Cluster health, leader info, node states |
| GET | `/admin/cluster/nodes` | List all nodes with addresses and roles |
| POST | `/admin/cluster/nodes` | Add a new node to the cluster |
| DELETE | `/admin/cluster/nodes/{id}` | Remove a node from the cluster |
| GET | `/admin/cluster/raft/stats` | Raft protocol statistics |
| POST | `/admin/cluster/raft/snapshot` | Trigger manual snapshot |

### Cluster Status Response

```json
{
  "cluster_id": "bleepstore-cluster-1",
  "node_id": "node-1",
  "state": "leader",
  "leader_id": "node-1",
  "leader_addr": "10.0.1.1:9001",
  "term": 42,
  "commit_index": 15234,
  "applied_index": 15234,
  "nodes": [
    {"id": "node-1", "addr": "10.0.1.1:9001", "state": "leader", "last_contact": "0s"},
    {"id": "node-2", "addr": "10.0.1.2:9001", "state": "follower", "last_contact": "50ms"},
    {"id": "node-3", "addr": "10.0.1.3:9001", "state": "follower", "last_contact": "45ms"}
  ]
}
```

---

## Implementation Per Language

### Go (`golang/`)
- Use `hashicorp/raft` — battle-tested, production-grade
- BoltDB or SQLite-based log store

### Rust (`rust/`)
- Use `openraft` crate or custom implementation
- Async-first design with tokio

### Python (`python/`)
- Use `pysyncobj` or custom implementation
- asyncio-based for consistency with the HTTP server

### Zig (`zig/`)
- Custom implementation (no established Raft library)
- Opportunity for a clean, minimal implementation
- Use std.net for RPC communication

---

## Transition: Embedded → Cluster

A single-node embedded deployment can be promoted to a cluster:

1. Stop the node
2. Update config to enable clustering with self as initial peer
3. Restart — node initializes Raft log from existing SQLite state
4. Add peer nodes one at a time via admin API
5. Leader replicates state to new followers via snapshot

This allows starting simple and scaling when needed.
