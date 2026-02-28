//! Serialization and deserialization utilities using postcard.
//!
//! This module provides thin wrappers around postcard's serialize and deserialize
//! functions to centralize encoding/decoding logic throughout the codebase.

use crate::error::Result;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Serialize a value into a byte vector using postcard.
///
/// # Arguments
///
/// * `val` - The value to serialize
///
/// # Returns
///
/// Returns a `Result` containing the serialized bytes on success,
/// or a serialization error on failure.
pub fn encode<T>(val: &T) -> Result<Vec<u8>>
where
  T: Serialize,
{
  Ok(postcard::to_allocvec(val)?)
}

/// Deserialize a value from a byte slice using postcard.
///
/// # Arguments
///
/// * `bytes` - The bytes to deserialize from
///
/// # Returns
///
/// Returns a `Result` containing the deserialized value on success,
/// or a deserialization error on failure.
pub fn decode<T>(bytes: &[u8]) -> Result<T>
where
  T: DeserializeOwned,
{
  Ok(postcard::from_bytes(bytes)?)
}
