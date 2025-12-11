use std::time::SystemTime;
use std::time::UNIX_EPOCH;

pub fn now_millis() -> u128 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .expect("System time before UNIX epoch")
    .as_millis()
}
