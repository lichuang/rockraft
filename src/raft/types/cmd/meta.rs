use std::{
  fmt::{self, Formatter},
  time::Duration,
};

use display_more::DisplayUnixTimeStampExt;

use super::time::flexible_timestamp_to_duration;

use super::time::Interval;

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct MetaSpec {
  pub(crate) expire_at: Option<u64>,

  #[serde(skip_serializing_if = "Option::is_none")]
  pub(crate) ttl: Option<Interval>,
}

impl fmt::Display for MetaSpec {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(f, "MetaSpec(",)?;

    if let Some(expires_at) = self.expire_at {
      write!(
        f,
        "expire_at: {} ",
        flexible_timestamp_to_duration(expires_at).display_unix_timestamp_short()
      )?;
    }
    if let Some(ttl) = &self.ttl {
      write!(f, "ttl: {:?} ", Duration::from_millis(ttl.millis()))?;
    }

    write!(f, ")")?;
    Ok(())
  }
}

impl MetaSpec {
  /// Create a new KVMeta
  ///
  /// `expires_at_sec_or_ms`: absolute expiration time in **seconds or milliseconds** since 1970-01-01.
  pub fn new(expires_at_sec_or_ms: Option<u64>, ttl: Option<Interval>) -> Self {
    Self {
      expire_at: expires_at_sec_or_ms,
      ttl,
    }
  }

  /// Create a KVMeta with an absolute expiration time in second since 1970-01-01.
  pub fn new_expire(expires_at_sec_or_ms: u64) -> Self {
    Self {
      expire_at: Some(expires_at_sec_or_ms),
      ttl: None,
    }
  }

  /// Create a KVMeta with relative expiration time(ttl).
  pub fn new_ttl(ttl: Duration) -> Self {
    Self {
      expire_at: None,
      ttl: Some(Interval::from_duration(ttl)),
    }
  }
}
