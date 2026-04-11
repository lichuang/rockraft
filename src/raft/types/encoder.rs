//! Serialization and deserialization utilities using postcard.
//!
//! # Why postcard (not protobuf)?
//!
//! This crate uses two serialization formats:
//!
//! - **Protobuf**: gRPC transport framing only. The `.proto` definitions use
//!   opaque `bytes` fields (`RaftRequest.data`, `VoteRequest.value`, etc.)
//!   — protobuf does not participate in actual data serialization.
//! - **Postcard** (this module): all substantive serialization — RocksDB
//!   storage, Raft log entries, forwarded requests, and internal metadata.
//!
//! Postcard is chosen for internal data because:
//! 1. OpenRaft's generic types (`VoteRequest<C>`, `Entry<C>`) require `serde`
//!    and have no protobuf mapping. A single postcard codec serves both
//!    storage and network paths.
//! 2. All nodes run the same Rust version — cross-language interop is not
//!    a requirement, so protobuf's schema evolution adds no value.
//! 3. Postcard produces compact, heap-efficient output for `#[derive(Serialize)]`
//!    types without schema boilerplate.

use crate::error::Result;
use serde::Serialize;
use serde::de::DeserializeOwned;

pub fn encode<T>(val: &T) -> Result<Vec<u8>>
where
  T: Serialize,
{
  Ok(postcard::to_allocvec(val)?)
}

pub fn decode<T>(bytes: &[u8]) -> Result<T>
where
  T: DeserializeOwned,
{
  Ok(postcard::from_bytes(bytes)?)
}
