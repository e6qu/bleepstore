//! BleepStore library — S3-compatible object storage engine.
//!
//! This crate provides the core components for running an S3-compatible
//! storage server, including request handling, authentication, metadata
//! management, pluggable storage backends, and cluster coordination.

use std::sync::Arc;

pub mod auth;
pub mod cluster;
pub mod config;
pub mod errors;
pub mod handlers;
pub mod metadata;
pub mod metrics;
pub mod serialization;
pub mod server;
pub mod storage;
pub mod xml;

pub use metadata::cosmos;
pub use metadata::dynamodb;
pub use metadata::firestore;
pub use metadata::local;
pub use metadata::memory;
pub use metadata::sqlite;
use crate::config::Config;
use crate::metadata::store::MetadataStore;
use crate::storage::backend::StorageBackend;

/// Shared application state passed to all handlers via `axum::extract::State`.
pub struct AppState {
    /// Server configuration.
    pub config: Config,
    /// Metadata store (SQLite or future Raft-backed).
    pub metadata: Arc<dyn MetadataStore>,
    /// Object storage backend (local filesystem or cloud gateway).
    pub storage: Arc<dyn StorageBackend>,
    /// SigV4 signing key and credential cache.
    pub auth_cache: auth::AuthCache,
}
