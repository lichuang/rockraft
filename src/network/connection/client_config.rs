#[derive(Clone, Debug, Default)]
pub struct RpcClientTlsConfig {
  pub rpc_tls_server_root_ca_cert: String,
  pub domain_name: String,
}

impl RpcClientTlsConfig {
  pub fn enabled(&self) -> bool {
    !self.rpc_tls_server_root_ca_cert.is_empty() && !self.domain_name.is_empty()
  }
}
