use crate::protocol_utils::{create_data_datagram, create_initial_datagram};
use crate::schema_generated::serial_proxy::{ControlCode, root_as_datagram};
use anyhow::Error;
use bytes::{Bytes, BytesMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
use tokio::net::{TcpListener, TcpStream, lookup_host};
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

pub async fn prepare_pipe(pipe_path: &str) -> Result<NamedPipeClient, Error> {
  ClientOptions::new()
    .write(true)
    .read(true)
    .open(pipe_path)
    .map_err(|e| e.into())
}

pub async fn pipe_loop(
  mut pipe: NamedPipeClient,
  pipe_to_client_push: broadcast::Sender<Bytes>,
  mut client_to_pipe_pull: mpsc::Receiver<Bytes>,
  cancel: CancellationToken,
) {
  let mut pipe_buf = BytesMut::zeroed(3072);
  loop {
    pipe_buf.resize(3072, 0);
    tokio::select! {
      biased;
      _ = cancel.cancelled() => {
        break;
      }
      data = client_to_pipe_pull.recv() => {
        if let Some(data) = data {
          if let Err(e) = pipe.write_all(&data).await {
            error!("Failed to write to pipe: {}", e);
            cancel.cancel();
            break;
          }
        }
      }
      n = pipe.read(&mut pipe_buf) => {
        match n {
          Ok(0) => {
            info!("Pipe disconnected");
            break;
          }
          Ok(n) => {
            debug!("Read {} bytes from pipe", n);
            let data = pipe_buf.split_to(n).freeze();
            if let Err(e) = pipe_to_client_push.send(data) {
              error!("Failed to send data to clients: {}", e);
              cancel.cancel();
              break;
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
  if let Err(e) = client_to_pipe_push.send(initial_datagram).await {
    info!("Failed to initialize connection: {}", e);
    return false;
  }
  // TODO rework
  match timeout(Duration::from_secs(2), async {
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
            debug!("Received invalid datagram, will ignore: {}", e);
            break None;
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
      let data = datagram.data().map(|d| d.bytes());
      if datagram.code() != ControlCode::Ack
        || data.is_some_and(|d| d != connection.target_address.as_bytes())
      {
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
  let mut tcp_buf = BytesMut::zeroed(2048);
  loop {
    tokio::select! {
      biased;
      _ = cancel.cancelled() => {
        client.shutdown().await.unwrap(); // TODO error handling
        info!("Connection cancelled");
        break;
      }
      data = pipe_to_client_pull.recv() => {
        if handle_pipe_read(identifier, data, &mut client).await {
          break;
        }
      }
      n = client.read(&mut tcp_buf) => {
        if handle_client_read(identifier, client_to_pipe_push.clone(), n, &tcp_buf).await {
          break;
        }
      }
    }
  }
}

async fn handle_pipe_read(
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
          if let Some(data) = datagram.data() {
            trace!("Sending data to client, size {}", data.len());
            client.write_all(data.bytes()).await.unwrap(); // TODO error handling
            client.flush().await.unwrap();
          }
          false
        }
        Err(e) => {
          error!("Received malformed datagram: {}, ignoring", e);
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

async fn handle_client_read(
  identifier: u64,
  client_to_pipe_push: mpsc::Sender<Bytes>,
  bytes_read: std::io::Result<usize>,
  read_buf: &BytesMut,
) -> bool {
  match bytes_read {
    Ok(0) => {
      info!("Client disconnected");
      true
    }
    Ok(n) => {
      debug!("Read {} bytes from client", n);
      let datagram = create_data_datagram(identifier, &read_buf[..n]);
      debug!("Sending datagram to pipe {:?}", datagram);
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
    let bytes = BytesMut::from("test data");
    let (pipe_push, mut pipe_pull) = tokio::sync::mpsc::channel(1);

    let identifier = 123;
    let bytes_read = Ok(bytes.len());

    assert!(!handle_client_read(identifier, pipe_push, bytes_read, &bytes).await);
    let pipe_read = pipe_pull.recv().await.unwrap();
    let datagram = root_as_datagram(&pipe_read).expect("Received datagram should be valid");
    assert_eq!(datagram.identifier(), identifier);
    assert_eq!(datagram.code(), ControlCode::Data);
    assert_eq!(datagram.data().unwrap().bytes(), bytes);
  }

  #[tokio::test]
  async fn test_handle_client_read_client_disconnect() {
    setup_tracing();
    let (pipe_push, pipe_pull) = tokio::sync::mpsc::channel(1);
    let identifier = 123;
    let bytes_read = Ok(0);

    assert!(handle_client_read(identifier, pipe_push, bytes_read, &BytesMut::new()).await);
  }

  #[tokio::test]
  async fn test_handle_client_read_client_error() {
    setup_tracing();
    let (pipe_push, pipe_pull) = tokio::sync::mpsc::channel(1);
    let identifier = 123;
    let bytes_read = Err(std::io::Error::new(std::io::ErrorKind::Other, "test error"));

    assert!(handle_client_read(identifier, pipe_push, bytes_read, &BytesMut::new()).await);
  }

  #[tokio::test]
  async fn test_handle_pipe_read_smoke() {
    setup_tracing();
    let identifier = 123;
    let data = BytesMut::from("test data");
    let datagram = create_data_datagram(identifier, &data);

    let (address_sender, mut address_receiver) = tokio::sync::mpsc::channel::<SocketAddr>(1);
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
    assert!(!handle_pipe_read(identifier, Ok(datagram), &mut client).await);
    handle.await.unwrap();
  }
}
