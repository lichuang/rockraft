use std::time::SystemTime;
use std::time::UNIX_EPOCH;

pub fn now_millis() -> u64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis() as u64
}
