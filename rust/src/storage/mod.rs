//! Object storage backends.
//!
//! The [`backend::StorageBackend`] trait abstracts over where bytes
//! physically live.  Implementations include local disk, and gateway
//! proxies to AWS S3, GCP Cloud Storage, and Azure Blob Storage.

pub mod aws;
pub mod azure;
pub mod backend;
pub mod gcp;
pub mod local;
pub mod memory;
pub mod sqlite;
