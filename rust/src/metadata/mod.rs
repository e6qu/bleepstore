//! Metadata storage layer.
//!
//! The metadata store keeps track of buckets, objects, and multipart
//! uploads.  The [`store::MetadataStore`] trait defines the interface;
//! [`sqlite::SqliteMetadataStore`] is the default implementation.

pub mod sqlite;
pub mod store;
