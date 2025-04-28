use crate::protocol_utils::{create_close_datagram, create_data_datagram};
use crate::schema_generated::serial_proxy::{ControlCode, root_as_datagram};
use anyhow::bail;
use bytes::{Bytes, BytesMut};
use memchr::memmem::Finder;
use std::cmp::min;
use std::sync::LazyLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace};
use zeroize::Zeroize;

const SINK_BUFFER_SIZE: usize = 2usize.pow(17);
pub(crate) const CONNECTION_BUFFER_SIZE: usize = 2usize.pow(15);
const DATAGRAM_HEADER: [u8; 8] = [2, 200, 94, 2, 6, 9, 4, 20];
const HEADER_BYTES: usize = DATAGRAM_HEADER.len();
const LENGTH_BYTES: usize = 2;
static HEADER_FINDER: LazyLock<Finder<'static>> =
  LazyLock::new(|| Finder::new(&DATAGRAM_HEADER).into_owned());

pub async fn sink_loop(
  mut pipe: impl AsyncReadExt + AsyncWriteExt + Unpin,
  sink_to_client_push: broadcast::Sender<Bytes>,
  mut client_to_sink_pull: mpsc::Receiver<Bytes>,
  cancel: CancellationToken,
) {
  let mut pipe_buf = BytesMut::zeroed(SINK_BUFFER_SIZE);
  let mut unprocessed_data_start = 0;
  loop {
    pipe_buf.resize(SINK_BUFFER_SIZE, 0);
    tokio::select! {
      biased;
      _ = cancel.cancelled() => {
        break;
      }
      data = client_to_sink_pull.recv() => {
        if let Some(data) = data {
          if let Err(e) = handle_sink_write(&mut pipe, data).await {
            error!("Failed to handle sink write: {}", e);
            cancel.cancel();
            break;
          }
        }
      }
      n = pipe.read(&mut pipe_buf[unprocessed_data_start..]) => {
        match n {
          Ok(0) => {
            info!("Sink disconnected");
            cancel.cancel();
            break;
          }
          Ok(n) => {
            match handle_sink_read(n + unprocessed_data_start, &mut pipe_buf, sink_to_client_push.clone()) {
              Ok(unprocessed_bytes) => unprocessed_data_start = unprocessed_bytes,
              Err(e) => {
                error!("Failed to handle sink read: {}", e);
                cancel.cancel();
                break;
              }
            }
          }
          Err(e) => {
            error!("Failed to read from sink: {}", e);
            cancel.cancel();
            break;
          }
        }
      }
    }
  }
}

pub fn handle_sink_read(
  n: usize,
  sink_buf: &mut BytesMut,
  sink_to_client_push: broadcast::Sender<Bytes>,
) -> anyhow::Result<usize> {
  trace!("Read {} bytes from sink", n);
  let mut read_data = &sink_buf[..n];
  trace!("Data in working buffer: {:?}", &read_data);
  let mut unprocessed_data_start = 0;
  while let Some(header_idx) = HEADER_FINDER.find(read_data) {
    trace!("Found HEADER at index {}", header_idx);
    if header_idx + HEADER_BYTES + LENGTH_BYTES > read_data.len() {
      trace!("Buffer is too small to contain datagram size");
      break;
    }
    let size = join_u8_to_u16(
      read_data[header_idx + HEADER_BYTES],
      read_data[header_idx + HEADER_BYTES + 1],
    );
    trace!("Found size - {} bytes", size);
    if header_idx + size as usize + HEADER_BYTES + LENGTH_BYTES > read_data.len() {
      trace!("Buffer is too small to contain all datagram bytes");
      break;
    }
    let datagram_start = header_idx + HEADER_BYTES + LENGTH_BYTES;
    let datagram_end = datagram_start + size as usize - 1;
    let datagram_bytes = Bytes::copy_from_slice(&read_data[datagram_start..=datagram_end]);
    trace!("Read datagram: {:?}", datagram_bytes);
    if let Err(e) = sink_to_client_push.send(datagram_bytes) {
      bail!("Failed to send data to clients. {}", e);
    }
    unprocessed_data_start += datagram_end + 1;
    trace!("Shrinking buffer to size {} to remove processed data", min(n, unprocessed_data_start));
    read_data = &sink_buf[min(n, unprocessed_data_start)..n];
    trace!("Data in buffer after reading datagram: {:?}", &read_data);
  }

  let mut unprocessed_bytes = n;
  if unprocessed_data_start > 0 {
    unprocessed_bytes = n - unprocessed_data_start;
    if unprocessed_data_start >= n {
      trace!("Whole buffer was read, zeroing out processed data");
      sink_buf.zeroize();
    } else {
      trace!(
        "Copying unprocessed data (length: {}) to the start of buffer and zeroing out the rest",
        unprocessed_data_start
      );
      let buffer_end = sink_buf.len();
      trace!("Buffer before shrinking: {:?}", &sink_buf);
      sink_buf.copy_within(unprocessed_data_start..buffer_end, 0);
      sink_buf[unprocessed_bytes..buffer_end].zeroize();
      trace!("Buffer after shrinking: {:?}", &sink_buf);
    }
  }
  Ok(unprocessed_bytes)
}

pub async fn handle_sink_write<T: AsyncReadExt + AsyncWriteExt + Unpin + Sized>(
  sink: &mut T,
  data: Bytes,
) -> anyhow::Result<()> {
  trace!("Writing {} bytes to sink: {:?}", data.len() + HEADER_BYTES + LENGTH_BYTES, data);
  if let Err(e) = sink.write_all(&DATAGRAM_HEADER).await {
    bail!("Failed to write HEADER to sink: {}", e);
  }
  let size = split_u16_to_u8(data.len() as u16);
  if let Err(e) = sink.write(&size).await {
    bail!("Failed to write size to sink: {}", e);
  }
  if let Err(e) = sink.write_all(&data).await {
    bail!("Failed to write data to sink: {}", e);
  }
  if let Err(e) = sink.flush().await {
    bail!("Failed to flush sink after writing: {}", e);
  }
  Ok(())
}

pub async fn handle_client_read(
  identifier: u64,
  client_to_pipe_push: mpsc::Sender<Bytes>,
  bytes_read: std::io::Result<usize>,
  read_buf: &mut BytesMut,
) -> bool {
  match bytes_read {
    Ok(0) => {
      info!("Client {} disconnected", identifier);
      let datagram = create_close_datagram(identifier);
      if let Err(e) = client_to_pipe_push.send(datagram).await {
        error!("Failed to send CLOSE datagram for connection {}: {}", identifier, e);
      }
      true
    }
    Ok(n) => {
      debug!("Read {} bytes from client", n);
      let data = read_buf.split_to(n).freeze();
      let datagram = create_data_datagram(identifier, &data);
      trace!("Sending DATA datagram to sink {:?}", datagram);
      if let Err(e) = client_to_pipe_push.send(datagram).await {
        error!("Failed to send DATA datagram for connection {}: {}", identifier, e);
        return true;
      }
      false
    }
    Err(e) => {
      error!("Failed to read from client: {}", e);
      true
    }
  }
}

pub async fn process_sink_read(
  identifier: u64,
  data: Result<Bytes, broadcast::error::RecvError>,
  client: &mut TcpStream,
) -> bool {
  match data {
    Ok(data_buf) => {
      match root_as_datagram(&data_buf) {
        Ok(datagram) => {
          if datagram.identifier() != identifier {
            // Not our datagram, ignore it
            return false;
          }
          debug!(
            "Received {:?} datagram with {} bytes of data",
            datagram.code(),
            datagram.data().map(|d| d.len()).unwrap_or(0)
          );
          if datagram.code() == ControlCode::Close {
            if let Err(e) = client.shutdown().await {
              error!("Failed to shutdown client after receiving CLOSE: {}", e);
            }
            return true;
          }
          if let Some(data) = datagram.data() {
            trace!("Sending data to client, size {}", data.len());
            match client.write_all(data.bytes()).await {
              Ok(_) => {
                if let Err(e) = client.flush().await {
                  error!("Failed to flush client data after writing data: {}", e);
                }
              }
              Err(e) => {
                error!("Failed to write data to client: {}", e);
                return true;
              }
            }
          }
          false
        }
        Err(e) => {
          error!("Received malformed datagram: {:?}, ignoring", e);
          false
        }
      }
    }
    Err(broadcast::error::RecvError::Closed) => {
      if let Err(e) = client.shutdown().await {
        error!("Failed to shutdown client: {}", e);
      }
      true
    }
    Err(_) => false,
  }
}

pub const fn split_u16_to_u8(n: u16) -> [u8; LENGTH_BYTES] {
  let upper = (n >> 8) as u8;
  let lower = n as u8;
  [upper, lower]
}

pub const fn join_u8_to_u16(upper: u8, lower: u8) -> u16 {
  ((upper as u16) << 8) | lower as u16
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::protocol_utils::{create_ack_datagram, create_data_datagram, create_initial_datagram};
  use crate::schema_generated::serial_proxy::{ControlCode, root_as_datagram};
  use crate::test_utils::setup_tracing;
  use std::net::SocketAddr;
  use tokio::net::TcpListener;

  #[tokio::test]
  async fn test_handle_sink_read() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::new();
    let (sink_to_client_push, mut sink_to_client_pull) = broadcast::channel(20);
    sink_buf.extend_from_slice(&[1, 2]);
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    sink_buf.extend_from_slice(&split_u16_to_u8(3));
    sink_buf.extend_from_slice(&[4, 5, 6]);
    sink_buf.extend_from_slice(&[7, 8, 9]);
    let mut buffer_size = sink_buf.len();

    let unprocessed_bytes =
      handle_sink_read(sink_buf.len(), &mut sink_buf, sink_to_client_push.clone()).unwrap();
    assert_eq!(sink_buf.len(), buffer_size);
    assert_eq!(unprocessed_bytes, 3);
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), Bytes::from_static(&[4, 5, 6]));
    assert_eq!(sink_buf[3..], BytesMut::zeroed(buffer_size - unprocessed_bytes));
    sink_buf.resize(unprocessed_bytes, 0);

    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    sink_buf.extend_from_slice(&split_u16_to_u8(100));
    sink_buf.extend_from_slice(&[5; 100]);
    sink_buf.extend_from_slice(&[11, 12, 13, 14, 15]);
    buffer_size = sink_buf.len();
    let unprocessed_bytes =
      handle_sink_read(sink_buf.len(), &mut sink_buf, sink_to_client_push.clone()).unwrap();
    assert_eq!(sink_buf.len(), buffer_size);
    assert_eq!(unprocessed_bytes, 5);
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), Bytes::from_static(&[5; 100]));
    assert_eq!(sink_buf[5..], BytesMut::zeroed(buffer_size - unprocessed_bytes));
  }

  #[tokio::test]
  async fn test_handle_sink_read_multiple_datagrams() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::new();
    let (sink_to_client_push, mut sink_to_client_pull) = broadcast::channel(20);

    let datagram1 = create_data_datagram(0, "datagram1".as_bytes());
    let datagram2 = create_data_datagram(1, "datagram2".as_bytes());
    let datagram3 = create_data_datagram(2, "datagram3".as_bytes());
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    sink_buf.extend_from_slice(&split_u16_to_u8(datagram1.len() as u16));
    sink_buf.extend_from_slice(&datagram1);
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    sink_buf.extend_from_slice(&split_u16_to_u8(datagram2.len() as u16));
    sink_buf.extend_from_slice(&datagram2);
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    sink_buf.extend_from_slice(&split_u16_to_u8(datagram3.len() as u16));
    sink_buf.extend_from_slice(&datagram3);

    let buffer_size = sink_buf.len();
    let unprocessed_bytes =
      handle_sink_read(buffer_size, &mut sink_buf, sink_to_client_push.clone()).unwrap();
    assert_eq!(buffer_size, sink_buf.len());
    assert_eq!(unprocessed_bytes, 0);
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), datagram1);
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), datagram2);
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), datagram3);
    assert_eq!(sink_buf, BytesMut::zeroed(buffer_size));
  }

  #[tokio::test]
  async fn test_handle_sink_read_size_not_read() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::new();
    let (sink_to_client_push, mut sink_to_client_pull) = broadcast::channel(20);
    sink_buf.extend_from_slice(&[1, 2]); // invalid data to offset HEADER
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let buffer_size = sink_buf.len();

    let unprocessed_bytes =
      handle_sink_read(buffer_size, &mut sink_buf, sink_to_client_push.clone()).unwrap();
    assert_eq!(buffer_size, sink_buf.len());
    assert!(sink_to_client_pull.try_recv().is_err());
    assert_eq!(unprocessed_bytes, buffer_size);
  }

  #[tokio::test]
  async fn test_handle_sink_read_data_partially_read() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::new();
    let (sink_to_client_push, mut sink_to_client_pull) = broadcast::channel(20);

    sink_buf.extend_from_slice(&[1, 2]); // invalid data to offset HEADER
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    sink_buf.extend_from_slice(&split_u16_to_u8(5));
    sink_buf.extend_from_slice(&[1; 3]);
    let buffer_size = sink_buf.len();

    let unprocessed_bytes =
      handle_sink_read(buffer_size, &mut sink_buf, sink_to_client_push.clone()).unwrap();
    assert_eq!(buffer_size, sink_buf.len());
    assert!(sink_to_client_pull.try_recv().is_err());
    assert_eq!(unprocessed_bytes, buffer_size);
  }

  #[tokio::test]
  async fn test_pipe_loop() {
    setup_tracing().await;
    let (sink_a, mut sink_b) = tokio::io::duplex(4096);
    let (pipe_to_client_push, mut pipe_to_client_pull) = broadcast::channel(256);
    let (_client_to_pipe_push, client_to_pipe_pull) = mpsc::channel(256);
    let cancel = CancellationToken::new();
    let _pipe_loop_handle = tokio::spawn(sink_loop(
      sink_a,
      pipe_to_client_push.clone(),
      client_to_pipe_pull,
      cancel.clone(),
    ));

    info!("Sending initial datagram");
    let initial_data = "test data";
    let initial_datagram = create_initial_datagram(1, initial_data);
    handle_sink_write(&mut sink_b, initial_datagram)
      .await
      .unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let initial_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(initial_datagram.identifier(), 1);
    assert_eq!(initial_datagram.code(), ControlCode::Initial);
    assert_eq!(initial_datagram.data().unwrap().bytes(), initial_data.as_bytes());

    // write some rubbish between datagrams
    info!("Sending rubbish data");
    let data = BytesMut::from("rubbish");
    sink_b.write_all(&data).await.unwrap();
    sink_b.flush().await.unwrap();

    info!("Sending ACK datagram");
    let ack_datagram = create_ack_datagram(2);
    handle_sink_write(&mut sink_b, ack_datagram).await.unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let ack_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(ack_datagram.identifier(), 2);
    assert_eq!(ack_datagram.code(), ControlCode::Ack);
    assert_eq!(ack_datagram.data().unwrap().bytes(), &[]);

    // write more rubbish between datagrams
    info!("Sending rubbish data");
    let data = BytesMut::from("rubbish2");
    sink_b.write_all(&data).await.unwrap();
    sink_b.flush().await.unwrap();

    // write even more rubbish between datagrams
    info!("Sending rubbish data");
    let data = BytesMut::from("rubbish3");
    sink_b.write_all(&data).await.unwrap();
    sink_b.flush().await.unwrap();

    // Send a bunch of big ones
    info!("Sending large datagram 1");
    let mut data = BytesMut::new();
    data.resize(1000, 100u8);
    let data_datagram = create_data_datagram(3, &data);
    handle_sink_write(&mut sink_b, data_datagram).await.unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let data_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(data_datagram.identifier(), 3);
    assert_eq!(data_datagram.code(), ControlCode::Data);
    assert_eq!(data_datagram.data().unwrap().bytes(), data);

    info!("Sending large datagram 2");
    let mut data = BytesMut::new();
    data.resize(1000, 100u8);
    let data_datagram = create_data_datagram(3, &data);
    handle_sink_write(&mut sink_b, data_datagram).await.unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let data_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(data_datagram.identifier(), 3);
    assert_eq!(data_datagram.code(), ControlCode::Data);
    assert_eq!(data_datagram.data().unwrap().bytes(), data);

    info!("Sending large datagram 3");
    let mut data = BytesMut::new();
    data.resize(1000, 100u8);
    let data_datagram = create_data_datagram(3, &data);
    handle_sink_write(&mut sink_b, data_datagram).await.unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let data_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(data_datagram.identifier(), 3);
    assert_eq!(data_datagram.code(), ControlCode::Data);
    assert_eq!(data_datagram.data().unwrap().bytes(), data);

    info!("Sending large datagram 4");
    let mut data = BytesMut::new();
    data.resize(1000, 100u8);
    let data_datagram = create_data_datagram(3, &data);
    handle_sink_write(&mut sink_b, data_datagram).await.unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let data_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(data_datagram.identifier(), 3);
    assert_eq!(data_datagram.code(), ControlCode::Data);
    assert_eq!(data_datagram.data().unwrap().bytes(), data);

    info!("Sending large datagram 5");
    let mut data = BytesMut::new();
    data.resize(1000, 100u8);
    let data_datagram = create_data_datagram(3, &data);
    handle_sink_write(&mut sink_b, data_datagram).await.unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let data_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(data_datagram.identifier(), 3);
    assert_eq!(data_datagram.code(), ControlCode::Data);
    assert_eq!(data_datagram.data().unwrap().bytes(), data);

    // Write datagram in parts
    info!("Sending split datagram");
    let mut data = BytesMut::new();
    data.resize(1000, 100u8);
    let data_datagram = create_data_datagram(3, &data);
    sink_b.write_all(&DATAGRAM_HEADER).await.unwrap();
    sink_b.flush().await.unwrap();
    let size = split_u16_to_u8(data_datagram.len() as u16);
    sink_b.write_all(&size).await.unwrap();
    sink_b.flush().await.unwrap();
    sink_b.write_all(&data_datagram[0..100]).await.unwrap();
    sink_b.flush().await.unwrap();
    sink_b.write_all(&data_datagram[100..400]).await.unwrap();
    sink_b.flush().await.unwrap();
    sink_b.write_all(&data_datagram[400..405]).await.unwrap();
    sink_b.flush().await.unwrap();
    sink_b.write_all(&data_datagram[405..800]).await.unwrap();
    sink_b.flush().await.unwrap();
    sink_b.write_all(&data_datagram[800..801]).await.unwrap();
    sink_b.flush().await.unwrap();
    sink_b.write_all(&data_datagram[801..802]).await.unwrap();
    sink_b.flush().await.unwrap();
    sink_b.write_all(&data_datagram[802..]).await.unwrap();
    sink_b.flush().await.unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let data_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(data_datagram.identifier(), 3);
    assert_eq!(data_datagram.code(), ControlCode::Data);
    assert_eq!(data_datagram.data().unwrap().bytes(), data);
  }

  #[tokio::test]
  async fn test_handle_client_read_smoke() {
    setup_tracing().await;
    let contents = "test data";
    let mut bytes = BytesMut::from(contents);
    let (pipe_push, mut pipe_pull) = mpsc::channel(1);

    let identifier = 123;
    let bytes_read = Ok(bytes.len());

    assert!(!handle_client_read(identifier, pipe_push, bytes_read, &mut bytes).await);
    let pipe_read = pipe_pull.recv().await.unwrap();
    let datagram = root_as_datagram(&pipe_read).expect("Received datagram should be valid");
    assert_eq!(datagram.identifier(), identifier);
    assert_eq!(datagram.code(), ControlCode::Data);
    assert_eq!(datagram.data().unwrap().bytes(), contents.as_bytes());
  }

  #[tokio::test]
  async fn test_handle_client_read_client_disconnect() {
    setup_tracing().await;
    let (pipe_push, _pipe_pull) = mpsc::channel(1);
    let identifier = 123;
    let bytes_read = Ok(0);

    assert!(handle_client_read(identifier, pipe_push, bytes_read, &mut BytesMut::new()).await);
  }

  #[tokio::test]
  async fn test_handle_client_read_client_error() {
    setup_tracing().await;
    let (pipe_push, _pipe_pull) = mpsc::channel(1);
    let identifier = 123;
    let bytes_read = Err(std::io::Error::new(std::io::ErrorKind::Other, "test error"));

    assert!(handle_client_read(identifier, pipe_push, bytes_read, &mut BytesMut::new()).await);
  }

  #[tokio::test]
  async fn test_handle_sink_read_smoke() {
    setup_tracing().await;
    let identifier = 123;
    let data = BytesMut::from("test data");
    let datagram = create_data_datagram(identifier, &data);

    let (address_sender, mut address_receiver) = mpsc::channel::<SocketAddr>(1);
    let handle = tokio::spawn(async move {
      let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
      let address = listener.local_addr().unwrap();
      address_sender.send(address).await.unwrap();
      let (mut client, _) = listener.accept().await.unwrap();
      let mut client_buf = BytesMut::zeroed(2048);
      let n = client.read(&mut client_buf).await.unwrap();
      assert_eq!(n, data.len());
      assert_eq!(data, client_buf[..n]);
    });

    let server_address = address_receiver.recv().await.unwrap();
    let mut client = TcpStream::connect(server_address).await.unwrap();
    assert!(!process_sink_read(identifier, Ok(datagram), &mut client).await);
    handle.await.unwrap();
  }
}
