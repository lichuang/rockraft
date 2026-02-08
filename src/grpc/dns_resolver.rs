use crate::error::Result;
use crate::error::RockRaftError;
use hickory_resolver::TokioResolver;
use hyper_util::client::legacy::connect::dns::Name;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::task;
use std::task::Context;
use std::task::Poll;
use tokio::spawn;
use tokio::task::JoinHandle;

pub struct DNSResolver {
  inner: TokioResolver,
}

static INSTANCE: LazyLock<Result<Arc<DNSResolver>>> =
  LazyLock::new(|| match TokioResolver::builder_tokio() {
    Err(error) => Err(RockRaftError::DnsParseError(format!(
      "DNS resolver create error: {}",
      error
    ))),
    Ok(resolver) => Ok(Arc::new(DNSResolver {
      inner: resolver.build(),
    })),
  });

impl DNSResolver {
  pub fn instance() -> Result<Arc<DNSResolver>> {
    match INSTANCE.as_ref() {
      Ok(resolver) => Ok(resolver.clone()),
      Err(error) => Err(RockRaftError::DnsParseError(error.to_string())),
    }
  }

  pub async fn resolve(&self, hostname: impl Into<String>) -> Result<Vec<IpAddr>> {
    let hostname = hostname.into();
    match self.inner.lookup_ip(hostname.clone()).await {
      Ok(lookup_ip) => Ok(lookup_ip.iter().collect::<Vec<_>>()),
      Err(error) => Err(RockRaftError::DnsParseError(format!(
        "Cannot lookup ip {} : {}",
        hostname, error
      ))),
    }
  }
}

#[derive(Clone)]
pub struct DNSService;

impl tower_service::Service<Name> for DNSService {
  type Response = DNSServiceAddrs;
  type Error = RockRaftError;
  type Future = DNSServiceFuture;

  fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
    Poll::Ready(Ok(()))
  }

  fn call(&mut self, name: Name) -> Self::Future {
    let blocking = spawn(async move {
      let resolver = DNSResolver::instance()?;
      match resolver.resolve(name.to_string()).await {
        Err(err) => Err(err),
        Ok(addrs) => Ok(DNSServiceAddrs {
          inner: addrs.into_iter(),
        }),
      }
    });

    DNSServiceFuture { inner: blocking }
  }
}

pub struct DNSServiceFuture {
  inner: JoinHandle<Result<DNSServiceAddrs>>,
}

pub struct DNSServiceAddrs {
  inner: std::vec::IntoIter<IpAddr>,
}

impl Iterator for DNSServiceAddrs {
  type Item = SocketAddr;

  fn next(&mut self) -> Option<Self::Item> {
    self.inner.next().map(|addr| SocketAddr::new(addr, 0))
  }
}

impl Future for DNSServiceFuture {
  type Output = Result<DNSServiceAddrs>;

  fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
    Pin::new(&mut self.inner).poll(cx).map(|res| match res {
      Ok(Err(err)) => Err(err),
      Ok(Ok(addrs)) => Ok(addrs),
      Err(join_err) => Err(RockRaftError::TokioError(format!(
        "Interrupted future: {}",
        join_err
      ))),
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn test_resolve_localhost() -> Result<()> {
    let resolver = DNSResolver::instance()?;
    let addrs = resolver.resolve("localhost").await?;

    assert!(
      !addrs.is_empty(),
      "localhost should resolve to at least one IP address"
    );

    let has_v4_loopback = addrs.iter().any(|addr| matches!(addr, IpAddr::V4(ip) if ip.is_loopback()));
    assert!(
      has_v4_loopback,
      "localhost should resolve to 127.0.0.1 (IPv4 loopback), got: {:?}",
      addrs
    );

    Ok(())
  }
}
