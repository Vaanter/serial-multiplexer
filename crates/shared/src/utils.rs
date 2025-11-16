use anyhow::{anyhow, bail};
use tokio::net::{TcpListener, TcpStream, lookup_host};
use tracing::{debug, warn};
use tracing_attributes::instrument;

pub async fn create_upstream_listener(upstream: &str) -> anyhow::Result<TcpListener> {
  let upstream_addresses = lookup_host(upstream).await?;
  let mut errors = Vec::new();
  for upstream_address in upstream_addresses {
    match TcpListener::bind(upstream_address).await {
      Ok(listener) => return Ok(listener),
      Err(e) => errors.push(e),
    }
  }
  let mut result_error = anyhow!(format!("Failed to start listener with address {}", upstream));
  for error in errors {
    result_error = result_error.context(error);
  }
  Err(result_error)
}

#[instrument]
pub async fn connect_downstream(downstream: &str) -> anyhow::Result<TcpStream> {
  let downstream_addresses = lookup_host(downstream).await?.collect::<Vec<_>>();
  for downstream_address in downstream_addresses.iter() {
    let downstream = match TcpStream::connect(downstream_address).await {
      Ok(downstream) => {
        debug!("Successfully connected to server");
        downstream
      }
      Err(e) => {
        warn!("Failed to connect to downstream: {}", e);
        continue;
      }
    };
    return Ok(downstream);
  }
  bail!("Failed to connect to downstream {}", downstream);
}
