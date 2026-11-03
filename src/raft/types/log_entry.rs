use serde::{Deserialize, Serialize};
use std::fmt;

use super::Cmd;

/// A Raft log entry carrying a command and optional timestamp.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
  /// The time in milliseconds when this log was proposed by the leader.
  ///
  /// Reserved and currently **not consumed by anything**: the TTL support that
  /// needed it lives at the application layer. It stays in the type because
  /// existing log entries in RocksDB were written with it -- removing the field
  /// would make them undecodable -- and because a future feature may want a
  /// leader-stamped time.
  ///
  /// It must stay identical on leader and followers, and must never drive a
  /// decision in `apply()`: the value comes from the leader's clock, so using
  /// it for expiry would make replicas diverge. `LeaderHandler::do_write` no
  /// longer stamps it, so entries written now carry `None`.
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
