use anyhow::bail;
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, lookup_host};
use tokio::sync::broadcast;
use tracing::{debug, error, trace, warn};

const DATAGRAM_START: u8 = 199;
const DATAGRAM_END: u8 = 85;

pub async fn create_upstream_listener(upstream: &str) -> anyhow::Result<Option<TcpListener>> {
  let upstream_addresses = lookup_host(upstream).await?;
  for upstream_address in upstream_addresses {
    match TcpListener::bind(upstream_address).await {
      Ok(listener) => {
        debug!("Listening on {}", upstream_address);
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

pub async fn handle_sink_read(
  n: usize,
  sink_buf: &mut BytesMut,
  start_index: &mut Option<usize>,
  sink_to_client_push: broadcast::Sender<Bytes>,
) -> anyhow::Result<usize> {
  debug!("Read {} bytes from sink", n);
  let read_data = &sink_buf[..n];
  let mut last_end: Option<usize> = None;
  for (idx, byte) in read_data
    .iter()
    .skip((*start_index).unwrap_or(0))
    .enumerate()
  {
    trace!("Read byte: {:x} at index {}", byte, idx);
    if *byte == DATAGRAM_START {
      trace!("Found START at index {}", idx);
      let _ = start_index.insert(idx);
    }
    if *byte == DATAGRAM_END && start_index.is_some_and(|start| start + 1 < idx) {
      trace!("Found END at index {}", idx);
      let data_start = start_index.unwrap() + 1;
      let data = Bytes::copy_from_slice(&read_data[data_start..idx]);
      trace!("Read data: {:?}", data);
      if let Err(e) = sink_to_client_push.send(data) {
        bail!("Failed to send data to clients. {}", e);
      }
      start_index.take();
      last_end = Some(idx);
    }
  }
  let mut unprocessed_bytes = n;
  if let Some(last_end) = last_end {
    let unprocessed_data_start = last_end + 1;
    let _ = sink_buf.split_to(unprocessed_data_start);
    unprocessed_bytes = n - last_end;
    if start_index.is_some_and(|start| start > last_end) {
      let old_start = start_index.take();
      let _ = start_index.insert(old_start.unwrap() - unprocessed_data_start);
    }
  }
  Ok(unprocessed_bytes)
}

pub async fn handle_sink_write<T: AsyncReadExt + AsyncWriteExt + Unpin + Sized>(
  sink: &mut T,
  data: Bytes,
) -> anyhow::Result<()> {
  if let Err(e) = sink.write(&[DATAGRAM_START]).await {
    bail!("Failed to write STX to sink: {}", e);
  }
  trace!("Writing {} bytes to sink: {:?}", data.len(), data);
  if let Err(e) = sink.write_all(&data).await {
    bail!("Failed to write to sink: {}", e);
  }
  if let Err(e) = sink.write(&[DATAGRAM_END]).await {
    bail!("Failed to write ETX to sink: {}", e);
  }
  if let Err(e) = sink.flush().await {
    bail!("Failed to flush sink after writing: {}", e);
  }
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn test_handle_sink_read() {
    let mut sink_buf = BytesMut::new();
    let mut start_index = None;
    let (sink_to_client_push, mut sink_to_client_pull) = broadcast::channel(10);
    sink_buf.extend_from_slice(&[
      1,
      2,
      DATAGRAM_START,
      4,
      5,
      6,
      DATAGRAM_END,
      DATAGRAM_START,
      9,
      10,
    ]);

    handle_sink_read(10, &mut sink_buf, &mut start_index, sink_to_client_push.clone())
      .await
      .unwrap();
    assert_eq!(sink_buf.len(), 3);
    assert_eq!(start_index, Some(0));
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), Bytes::from_static(&[4, 5, 6]));

    sink_buf.extend_from_slice(&[11, 12, 13, DATAGRAM_END, 14, 15, 16, 17, 18, 19]);
    handle_sink_read(10, &mut sink_buf, &mut start_index, sink_to_client_push.clone())
      .await
      .unwrap();
    assert_eq!(sink_buf.len(), 6);
    assert_eq!(start_index, None);
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), Bytes::from_static(&[9, 10, 11, 12, 13]));
  }
}
