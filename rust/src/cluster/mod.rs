//! Cluster coordination and replication.
//!
//! When clustering is enabled, BleepStore uses Raft consensus to
//! replicate metadata across nodes.

pub mod raft;
