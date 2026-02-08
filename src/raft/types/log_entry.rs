use serde::{Deserialize, Serialize};
use std::fmt;

use super::Cmd;

/// The application data request type which the `metasrv` works with.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
  /// The time in millisecond when this log is proposed by the leader.
  ///
  /// State machine depends on clock time to expire values.
  /// The time to use has to be consistent on leader and followers.
  /// Otherwise an `apply` results in different state on leader and followers.
  pub time_ms: Option<u64>,

  /// The action a client want to take.
  pub cmd: Cmd,
}

impl LogEntry {
  pub fn new(cmd: Cmd) -> Self {
    Self { time_ms: None, cmd }
  }

  pub fn new_with_time(cmd: Cmd, time_ms: Option<u64>) -> Self {
    Self { time_ms, cmd }
  }
}

impl fmt::Display for LogEntry {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.cmd)
  }
}
