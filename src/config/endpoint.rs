use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// A network endpoint consisting of address and port
///
/// This type is used for raft node endpoints and is stored in the config module
/// to avoid circular dependencies between config and raft types.

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct Endpoint {
  address: String,
  port: u32,
}

impl Endpoint {
  pub fn new(addr: impl ToString, port: u32) -> Self {
    Self {
      address: addr.to_string(),
      port,
    }
  }

  #[allow(clippy::inherent_to_string_shadow_display)]
  pub fn to_string(&self) -> String {
    // Re-bracket IPv6 literals so the output round-trips through `parse`.
    if self.address.contains(':') {
      format!("[{}]:{}", self.address, self.port)
    } else {
      format!("{}:{}", self.address, self.port)
    }
  }

  pub fn addr(&self) -> &str {
    &self.address
  }

  pub fn port(&self) -> u32 {
    self.port
  }

  /// Parse `1.2.3.4:5555` or `[::1]:5555` into `Endpoint`.
  ///
  /// Handles the `[host]:port` bracket syntax required for IPv6 literals
  /// (a bare `:` split cannot work since IPv6 addresses contain `:`), and
  /// falls back to the plain `host:port` form for IPv4/hostnames.
  pub fn parse(address: &str) -> Result<Self, Error> {
    if let Some(rest) = address.strip_prefix('[') {
      // Bracketed form: only valid for IPv6 literals like `[::1]:8080`.
      let (host, port) = rest
        .split_once("]:")
        .ok_or_else(|| Error::config(format!("Failed to parse address: {}", address)))?;
      let port = port
        .parse::<u32>()
        .map_err(|e| Error::config(format!("Failed to parse port: {}; address: {}", e, address)))?;
      Ok(Self::new(host, port))
    } else {
      let x = address.splitn(2, ':').collect::<Vec<_>>();
      if x.len() != 2 {
        return Err(Error::config(format!(
          "Failed to parse address: {}",
          address
        )));
      }
      let port = x[1]
        .parse::<u32>()
        .map_err(|e| Error::config(format!("Failed to parse port: {}; address: {}", e, address)))?;
      Ok(Self::new(x[0], port))
    }
  }
}

impl fmt::Display for Endpoint {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}", self.to_string())
  }
}

#[cfg(test)]
mod tests {
  use super::Endpoint;

  #[test]
  fn test_endpoint_parse() -> anyhow::Result<()> {
    assert!(Endpoint::parse("1.2.3.4").is_err());

    assert_eq!("1.2.3.4", Endpoint::parse("1.2.3.4:1234")?.addr());
    assert_eq!(1234, Endpoint::parse("1.2.3.4:1234")?.port());

    Ok(())
  }

  #[test]
  fn test_endpoint_parse_ipv6() -> anyhow::Result<()> {
    assert!(
      Endpoint::parse("[::1]").is_err(),
      "bracketed form without a port is invalid"
    );

    let e = Endpoint::parse("[::1]:8080")?;
    assert_eq!("::1", e.addr());
    assert_eq!(8080, e.port());

    let e = Endpoint::parse("[2001:db8::1]:9092")?;
    assert_eq!("2001:db8::1", e.addr());
    assert_eq!(9092, e.port());

    Ok(())
  }
}
