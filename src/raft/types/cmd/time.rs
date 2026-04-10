use std::ops::Add;
use std::ops::Sub;
use std::time::Duration;

/// A interval of time.
///
/// As a replacement of [`Duration`], which is not `serde`-able.
///
/// `Interval` implements: `Interval +- Interval`.
#[derive(
  serde::Serialize,
  serde::Deserialize,
  Debug,
  Default,
  Clone,
  Copy,
  Hash,
  Eq,
  PartialEq,
  PartialOrd,
  Ord,
)]
pub struct Interval {
  pub(crate) millis: u64,
}

impl Interval {
  pub fn from_duration(duration: Duration) -> Self {
    Self {
      millis: duration.as_millis() as u64,
    }
  }

  pub fn as_duration(&self) -> Duration {
    Duration::from_millis(self.millis)
  }

  pub fn from_millis(millis: u64) -> Self {
    Self::from_duration(Duration::from_millis(millis))
  }

  pub fn from_secs(secs: u64) -> Self {
    Self::from_duration(Duration::from_secs(secs))
  }

  pub fn millis(&self) -> u64 {
    self.millis
  }

  pub fn seconds(&self) -> u64 {
    self.millis / 1000
  }
}

impl Add for Interval {
  type Output = Self;

  fn add(self, rhs: Self) -> Self::Output {
    Self {
      millis: self.millis.saturating_add(rhs.millis),
    }
  }
}

impl Sub for Interval {
  type Output = Self;

  fn sub(self, rhs: Self) -> Self::Output {
    Self {
      millis: self.millis.saturating_sub(rhs.millis),
    }
  }
}

/// Timestamp in **seconds or milliseconds** since Unix epoch (1970-01-01).
///
/// The interpretation depends on the magnitude of the value:
/// - Values > `100_000_000_000`: treated as milliseconds since epoch
/// - Values ≤ `100_000_000_000`: treated as seconds since epoch
///
/// Examples:
/// - `100_000_000_001` → `1973-03-03 17:46:40` (milliseconds)
/// - `100_000_000_000` → `5138-11-16 17:46:40` (seconds)
///
/// Valid ranges:
/// - Seconds: `1970-01-01 00:00:00` to `5138-11-16 17:46:40`
/// - Milliseconds: `1973-03-03 17:46:40` onwards
///
/// To avoid overflow issues, use timestamps between `1973-03-03 17:46:40`
/// and `5138-11-16 17:46:40` for reliable behavior across both interpretations.
pub fn flexible_timestamp_to_duration(timestamp: u64) -> Duration {
  if timestamp > 100_000_000_000 {
    // Milliseconds since epoch
    Duration::from_millis(timestamp)
  } else {
    // Seconds since epoch
    Duration::from_secs(timestamp)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_interval() {
    let interval = Interval::from_millis(1000);
    assert_eq!(interval.millis(), 1000);
    assert_eq!(interval.seconds(), 1);

    let interval = Interval::from_secs(1);
    assert_eq!(interval.millis(), 1000);
    assert_eq!(interval.seconds(), 1);

    assert_eq!(interval + interval, Interval::from_millis(2000));
    assert_eq!(interval - interval, Interval::from_millis(0));
    assert_eq!(
      interval - Interval::from_millis(1500),
      Interval::from_millis(0)
    );
  }

  #[test]
  fn test_flexible_timestamp_to_duration() {
    assert_eq!(
      flexible_timestamp_to_duration(100_000_000_001),
      Duration::from_millis(100_000_000_001)
    );
    assert_eq!(
      flexible_timestamp_to_duration(100_000_000_000),
      Duration::from_secs(100_000_000_000)
    );
  }
}
