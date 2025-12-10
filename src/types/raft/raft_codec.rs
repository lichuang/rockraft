use std::error::Error;
use std::io;

use bincode::deserialize;
use bincode::serialize;
use openraft::StoredMembership;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;

use crate::types::TypeConfig;

pub trait RaftCodec {
  fn decode_from(buf: &[u8]) -> Result<Self, io::Error>
  where Self: Sized;
  fn encode_to(&self) -> Result<Vec<u8>, io::Error>;
}

impl RaftCodec for LogIdOf<TypeConfig> {
  fn decode_from(buf: &[u8]) -> Result<Self, io::Error>
  where Self: Sized {
    Ok(deserialize(buf).map_err(read_logs_err)?)
  }

  fn encode_to(&self) -> Result<Vec<u8>, io::Error> {
    Ok(serialize(self).map_err(read_logs_err)?)
  }
}

impl RaftCodec for VoteOf<TypeConfig> {
  fn decode_from(buf: &[u8]) -> Result<Self, io::Error>
  where Self: Sized {
    Ok(deserialize(buf).map_err(read_logs_err)?)
  }

  fn encode_to(&self) -> Result<Vec<u8>, io::Error> {
    Ok(serialize(self).map_err(read_logs_err)?)
  }
}

impl RaftCodec for StoredMembership<TypeConfig> {
  fn decode_from(buf: &[u8]) -> Result<Self, io::Error>
  where Self: Sized {
    Ok(deserialize(buf).map_err(read_logs_err)?)
  }

  fn encode_to(&self) -> Result<Vec<u8>, io::Error> {
    Ok(serialize(self).map_err(read_logs_err)?)
  }
}

pub fn read_logs_err(e: impl Error + 'static) -> io::Error {
  io::Error::other(e.to_string())
}
