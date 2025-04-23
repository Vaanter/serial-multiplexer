use crate::protocol_utils::{create_data_datagram, create_initial_datagram, datagram_from_bytes};
use crate::schema_generated::serial_proxy::{ControlCode, root_as_datagram};
use crate::utils::{handle_sink_read, handle_sink_write};
use anyhow::Error;
use bytes::{Bytes, BytesMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(windows)]
use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

static IDENTIFIER_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub struct Connection {
  identifier: u64,
  target_address: String,
  client: TcpStream,
}

#[cfg(windows)]
pub async fn prepare_pipe(pipe_path: &str) -> Result<NamedPipeClient, Error> {
  ClientOptions::new()
    .write(true)
    .read(true)
    .open(pipe_path)
    .map_err(|e| e.into())
}

pub async fn pipe_loop(
  mut pipe: impl AsyncReadExt + AsyncWriteExt + Unpin,
  pipe_to_client_push: broadcast::Sender<Bytes>,
  mut client_to_pipe_pull: mpsc::Receiver<Bytes>,
  cancel: CancellationToken,
) {
  let mut pipe_buf = BytesMut::zeroed(3072);
  let mut stx_index: Option<usize> = None;
  let mut unprocessed_data_start = 0;
  loop {
    pipe_buf.resize(3072, 0);
    tokio::select! {
      biased;
      _ = cancel.cancelled() => {
        break;
      }
      data = client_to_pipe_pull.recv() => {
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
            info!("Pipe disconnected");
            break;
          }
          Ok(n) => {
            match handle_sink_read(n + unprocessed_data_start, &mut pipe_buf, &mut stx_index, pipe_to_client_push.clone()).await {
              Ok(unprocessed_bytes) => unprocessed_data_start = unprocessed_bytes,
              Err(e) => {
                error!("Failed to handle sink read: {}", e);
                cancel.cancel();
                break;
              }
            }
          }
          Err(e) => {
            error!("Failed to read from pipe: {}", e);
            cancel.cancel();
            break;
          }
        }
      }
    }
  }
}

pub async fn run_listener(
  listener: TcpListener,
  target_address: String,
  connection_sender: mpsc::Sender<Connection>,
  cancel: CancellationToken,
) {
  let listener_address = listener.local_addr().unwrap();
  loop {
    tokio::select! {
      biased;
      _ = cancel.cancelled() => {
        info!("Closing listener {}", listener_address);
        break;
      }
      new_client = listener.accept() => {
        match new_client {
          Ok((client, client_address)) => {
            info!("Client connected: {}", client_address);

            let identifier = IDENTIFIER_SEQUENCE.fetch_add(1, Ordering::SeqCst);
            let connection = Connection {
              identifier,
              target_address: target_address.clone(),
              client,
            };

            if let Err(e) = connection_sender.send(connection).await {
              error!("Failed to finish setup for upstream {}", e);
            }
          }
          Err(e) => {
            error!("Failed to establish client connection: {}", e);
          }
        }
      }
    }
  }
}

pub async fn connection_initiator(
  mut client_to_pipe_push: mpsc::Sender<Bytes>,
  mut pipe_to_client_pull: broadcast::Receiver<Bytes>,
  mut connection_receiver: mpsc::Receiver<Connection>,
  cancellation_token: CancellationToken,
) {
  loop {
    tokio::select! {
      biased;
      _ = cancellation_token.cancelled() => {
        break;
      }
      data = connection_receiver.recv() => {
        match data {
          Some(connection) => {
            info!("Starting connection to {}", connection.target_address);
            pipe_to_client_pull = pipe_to_client_pull.resubscribe(); // clean up previous datagrams
            if !initiate_connection(&connection, &mut client_to_pipe_push, &mut pipe_to_client_pull).await {
              info!("Failed to initialize connection, closing client");
              let mut client = connection.client;
              if let Err(e) = client.shutdown().await {
                error!("Failed to shutdown client after initialization failed: {}", e);
              }
              continue;
            }
            tokio::spawn(connection_loop(connection, pipe_to_client_pull.resubscribe(), client_to_pipe_push.clone(), cancellation_token.clone()));
          }
          None => {
            warn!("Connection receiver closed");
            break;
          }
        }
      }
    }
  }
}

async fn initiate_connection(
  connection: &Connection,
  client_to_pipe_push: &mut mpsc::Sender<Bytes>,
  pipe_to_client_pull: &mut broadcast::Receiver<Bytes>,
) -> bool {
  let initial_datagram = create_initial_datagram(connection.identifier, &connection.target_address);
  trace!("Sending initial datagram: {:?}", datagram_from_bytes(&initial_datagram));
  if let Err(e) = client_to_pipe_push.send(initial_datagram).await {
    info!("Failed to initialize connection: {}", e);
    return false;
  }
  // TODO rework
  match timeout(Duration::from_secs(5), async {
    loop {
      match pipe_to_client_pull.recv().await {
        Ok(data) => match root_as_datagram(&data) {
          Ok(datagram) => {
            trace!("Received datagram from server: {:?}", datagram);
            if datagram.identifier() == connection.identifier {
              break Some(data);
            }
          }
          Err(e) => {
            debug!("Received invalid datagram, will ignore: {:?}", e);
          }
        },
        Err(_) => break None,
      }
    }
  })
  .await
  {
    Ok(Some(datagram)) => {
      let datagram = root_as_datagram(&datagram)
        .expect("Datagram should have been validated by checking the identifier");
      if datagram.code() != ControlCode::Ack || datagram.identifier() != connection.identifier {
        debug!("Received invalid response from server: {:?}", datagram);
        return false;
      }
      true
    }
    Ok(None) | Err(_) => {
      debug!("Failed to initialize connection, pipe is closed or failed to receive response");
      false
    }
  }
}

pub async fn connection_loop(
  connection: Connection,
  mut pipe_to_client_pull: broadcast::Receiver<Bytes>,
  client_to_pipe_push: mpsc::Sender<Bytes>,
  cancel: CancellationToken,
) {
  let identifier = connection.identifier;
  let mut client = connection.client;
  let mut tcp_buf = BytesMut::zeroed(3072);
  loop {
    tcp_buf.resize(3072, 0);
    tokio::select! {
      biased;
      _ = cancel.cancelled() => {
        client.shutdown().await.unwrap(); // TODO error handling
        info!("Connection cancelled");
        break;
      }
      data = pipe_to_client_pull.recv() => {
        if process_sink_read(identifier, data, &mut client).await {
          break;
        }
      }
      n = client.read(&mut tcp_buf) => {
        if handle_client_read(identifier, client_to_pipe_push.clone(), n, &mut tcp_buf).await {
          break;
        }
      }
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
          if datagram.code() == ControlCode::Close {
            if let Err(e) = client.shutdown().await {
              error!("Failed to shutdown client after receiving CLOSE: {}", e);
              return true;
            }
          }
          if let Some(data) = datagram.data() {
            trace!("Sending data to client, size {}", data.len());
            client.write_all(data.bytes()).await.unwrap(); // TODO error handling
            client.flush().await.unwrap();
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

pub async fn handle_client_read(
  identifier: u64,
  client_to_pipe_push: mpsc::Sender<Bytes>,
  bytes_read: std::io::Result<usize>,
  read_buf: &mut BytesMut,
) -> bool {
  match bytes_read {
    Ok(0) => {
      info!("Client disconnected");
      true
    }
    Ok(n) => {
      debug!("Read {} bytes from client", n);
      let data = read_buf.split_to(n).freeze();
      let datagram = create_data_datagram(identifier, &data);
      debug!("Sending datagram to sink {:?}", datagram);
      client_to_pipe_push.send(datagram).await.unwrap(); // TODO error handling
      false
    }
    Err(e) => {
      error!("Failed to read from client: {}", e);
      true
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::protocol_utils::create_ack_datagram;
  use crate::schema_generated::serial_proxy::{ControlCode, root_as_datagram};
  use std::net::SocketAddr;
  use std::sync::atomic::{AtomicBool, Ordering};
  use tracing::Level;

  static TRACING_SETUP: AtomicBool = AtomicBool::new(false);

  fn setup_tracing() {
    let Ok(false) = TRACING_SETUP.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
    else {
      return;
    };
    let subscriber = tracing_subscriber::fmt()
      .with_env_filter(format!("serial_proxy={}", Level::TRACE))
      .with_file(true)
      .with_line_number(true)
      .with_thread_ids(true)
      .with_target(false)
      .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
  }

  #[tokio::test]
  async fn test_handle_client_read_smoke() {
    setup_tracing();
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
    setup_tracing();
    let (pipe_push, _pipe_pull) = mpsc::channel(1);
    let identifier = 123;
    let bytes_read = Ok(0);

    assert!(handle_client_read(identifier, pipe_push, bytes_read, &mut BytesMut::new()).await);
  }

  #[tokio::test]
  async fn test_handle_client_read_client_error() {
    setup_tracing();
    let (pipe_push, _pipe_pull) = mpsc::channel(1);
    let identifier = 123;
    let bytes_read = Err(std::io::Error::new(std::io::ErrorKind::Other, "test error"));

    assert!(handle_client_read(identifier, pipe_push, bytes_read, &mut BytesMut::new()).await);
  }

  #[tokio::test]
  async fn test_handle_sink_read_smoke() {
    setup_tracing();
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

  #[tokio::test]
  async fn test_pipe_loop() {
    let (sink_a, mut sink_b) = tokio::io::duplex(4096);
    let (pipe_to_client_push, mut pipe_to_client_pull) = broadcast::channel(256);
    let (_client_to_pipe_push, client_to_pipe_pull) = mpsc::channel(256);
    let cancel = CancellationToken::new();
    let _pipe_loop_handle = tokio::spawn(pipe_loop(
      sink_a,
      pipe_to_client_push.clone(),
      client_to_pipe_pull,
      cancel.clone(),
    ));

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
    let data = BytesMut::from("rubbish");
    sink_b.write_all(&data).await.unwrap();
    sink_b.flush().await.unwrap();

    let ack_datagram = create_ack_datagram(2);
    handle_sink_write(&mut sink_b, ack_datagram).await.unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let ack_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(ack_datagram.identifier(), 2);
    assert_eq!(ack_datagram.code(), ControlCode::Ack);
    assert_eq!(ack_datagram.data().unwrap().bytes(), &[]);

    // write more rubbish between datagrams
    let data = BytesMut::from("rubbish2");
    sink_b.write_all(&data).await.unwrap();
    sink_b.flush().await.unwrap();

    // write even more rubbish between datagrams
    let data = BytesMut::from("rubbish3");
    sink_b.write_all(&data).await.unwrap();
    sink_b.flush().await.unwrap();

    let mut data = BytesMut::new();
    data.resize(1000, 100u8);
    let data_datagram = create_data_datagram(3, &data);
    handle_sink_write(&mut sink_b, data_datagram).await.unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let data_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(data_datagram.identifier(), 3);
    assert_eq!(data_datagram.code(), ControlCode::Data);
    assert_eq!(data_datagram.data().unwrap().bytes(), data);
  }
}
