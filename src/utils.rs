use anyhow::bail;
use tokio::net::{TcpListener, TcpStream, lookup_host};
use tracing::{debug, error, warn};

pub async fn create_upstream_listener(upstream: &str) -> anyhow::Result<Option<TcpListener>> {
  let upstream_addresses = lookup_host(upstream).await?;
  for upstream_address in upstream_addresses {
    match TcpListener::bind(upstream_address).await {
      Ok(listener) => {
        return Ok(Some(listener));
      }
      Err(e) => {
        error!("Failed to start listener with address {}. {}", upstream_address, e);
      }
    }
  }
  Ok(None)
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
