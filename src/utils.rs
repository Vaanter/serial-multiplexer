use anyhow::bail;
use tokio::net::{TcpListener, TcpStream, lookup_host};
use tracing::{debug, warn};

pub async fn create_upstream_listener(upstream: &str) -> anyhow::Result<TcpListener> {
  let upstream_addresses = lookup_host(upstream).await?;
  for upstream_address in upstream_addresses {
    if let Ok(listener) = TcpListener::bind(upstream_address).await {
      return Ok(listener);
    }
  }
  bail!("Failed to start listener with address {}", upstream);
}

pub async fn connect_downstream(downstream: &str) -> anyhow::Result<TcpStream> {
  let downstream_addresses = lookup_host(downstream).await?.collect::<Vec<_>>();
  for downstream_address in downstream_addresses.iter() {
    let downstream = match TcpStream::connect(downstream_address).await {
      Ok(downstream) => {
        debug!("Successfully connected to server");
        downstream
      }
      Err(e) => {
        warn!("Failed to connect to downstream {}: {}", downstream_address, e);
        continue;
      }
    };
    return Ok(downstream);
  }
  bail!("Failed to connect to downstream {}", downstream);
}
