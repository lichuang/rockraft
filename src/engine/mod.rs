//! RocksDB storage engine.
//!
//! Provides a low-level interface to RocksDB, including:
//! - Database initialization with configurable options
//! - Column family management
//! - Shared access to the underlying DB instance

mod rocksdb;

pub use rocksdb::RocksDBConfig;
pub use rocksdb::RocksDBEngine;
