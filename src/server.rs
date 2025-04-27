use crate::common::{CONNECTION_BUFFER_SIZE, handle_client_read, process_sink_read};
use crate::protocol_utils::{create_initial_datagram, datagram_from_bytes};
use crate::schema_generated::serial_proxy::{ControlCode, root_as_datagram};
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
use tracing_attributes::instrument;

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

#[instrument(skip_all, fields(client_id = %connection.identifier))]
pub async fn connection_loop(
  connection: Connection,
  mut pipe_to_client_pull: broadcast::Receiver<Bytes>,
  client_to_pipe_push: mpsc::Sender<Bytes>,
  cancel: CancellationToken,
) {
  let identifier = connection.identifier;
  let mut client = connection.client;
  let mut tcp_buf = BytesMut::zeroed(CONNECTION_BUFFER_SIZE);
  loop {
    tcp_buf.resize(CONNECTION_BUFFER_SIZE, 0);
    tokio::select! {
      biased;
      _ = cancel.cancelled() => {
        if let Err(e) = client.shutdown().await {
          error!("Failed to shutdown client after server shutdown: {}", e);
        }
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
  debug!("Connection {} loop ending", identifier);
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::test_utils::setup_tracing;

  #[tokio::test]
  async fn test_run_listener_smoke() {
    setup_tracing().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_address = listener.local_addr().unwrap();
    let target_address = "tcpbin.com:4242".to_string();
    let (connection_sender, mut connection_receiver) = mpsc::channel::<Connection>(1);
    tokio::spawn({
      let target_address = target_address.clone();
      async move {
        run_listener(listener, target_address, connection_sender, CancellationToken::new()).await;
      }
    });
    let test_connection = TcpStream::connect(listener_address).await.unwrap();
    let connection = connection_receiver.recv().await.unwrap();
    assert_eq!(connection.target_address, target_address);
    assert_eq!(0, connection.identifier);
    assert_eq!(connection.client.peer_addr().unwrap(), test_connection.local_addr().unwrap());
  }
}
