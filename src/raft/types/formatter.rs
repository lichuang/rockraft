//! Serialization and deserialization utilities using bincode.
//!
//! This module provides thin wrappers around bincode's serialize and deserialize
//! functions to centralize encoding/decoding logic throughout the codebase.

use serde::de::DeserializeOwned;
use serde::Serialize;

/// Serialize a value into a byte vector using bincode.
///
/// # Arguments
///
/// * `val` - The value to serialize
///
/// # Returns
///
/// Returns a `Result` containing the serialized bytes on success,
/// or a `bincode::Error` on failure.
pub fn encode<T>(val: &T) -> bincode::Result<Vec<u8>>
where
  T: Serialize,
{
  bincode::serialize(val)
}

/// Deserialize a value from a byte slice using bincode.
///
/// # Arguments
///
/// * `bytes` - The bytes to deserialize from
///
/// # Returns
///
/// Returns a `Result` containing the deserialized value on success,
/// or a `bincode::Error` on failure.
pub fn decode<T>(bytes: &[u8]) -> bincode::Result<T>
where
  T: DeserializeOwned,
{
  bincode::deserialize(bytes)
}
