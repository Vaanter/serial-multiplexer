use crate::common::ConnectionState;
use crate::common::{CONNECTION_BUFFER_SIZE, handle_client_read, process_sink_read};
use crate::protocol_utils::{create_initial_datagram, datagram_from_bytes};
use crate::schema_generated::serial_multiplexer::{ControlCode, root_as_datagram};
use anyhow::{Context, Error};
use bytes::{Bytes, BytesMut};
use fast_socks5::ReplyError;
use fast_socks5::server::Socks5ServerProtocol;
use futures::TryFutureExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
#[cfg(windows)]
use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
use tokio::sync::{broadcast, mpsc};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};
use tracing_attributes::instrument;

static IDENTIFIER_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ConnectionType {
  Direct { target_address: String },
  Socks5,
}

impl ConnectionType {
  pub fn new_direct(target_address: String) -> Self {
    Self::Direct { target_address }
  }

  pub fn new_socks5() -> Self {
    Self::Socks5
  }
}

#[cfg(windows)]
pub async fn prepare_pipe(pipe_path: &str) -> Result<NamedPipeClient, Error> {
  ClientOptions::new()
    .write(true)
    .read(true)
    .open(pipe_path)
    .map_err(|e| e.into())
}

#[instrument(skip_all, fields(listener_address = %listener.local_addr().unwrap()))]
pub async fn run_listener(
  listener: TcpListener,
  connection_type: ConnectionType,
  connection_sender: mpsc::Sender<(ConnectionState, ConnectionType)>,
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
            let connection = ConnectionState::new(identifier, client);

            if let Err(e) = connection_sender.send((connection, connection_type.clone())).await {
              error!("Failed to finish setup for connection {}: {}", identifier, e);
            }
          }
          Err(e) => {
            error!("Failed to establish client connection: {}", e);
            break;
          }
        }
      }
    }
  }
}

#[instrument(skip_all)]
pub async fn connection_initiator(
  client_to_pipe_push: async_channel::Sender<Bytes>,
  mut pipe_to_client_pull: broadcast::Receiver<Bytes>,
  mut connection_receiver: mpsc::Receiver<(ConnectionState, ConnectionType)>,
  cancel: CancellationToken,
) {
  debug!("Starting connection initiator");
  loop {
    tokio::select! {
      biased;
      _ = cancel.cancelled() => {
        break;
      }
      data = connection_receiver.recv() => {
        match data {
          Some((connection_state, connection_type)) => {
            handle_new_connection(
              connection_state,
              connection_type,
              client_to_pipe_push.clone(),
              pipe_to_client_pull.resubscribe(),
              cancel.clone()
            ).await;
          }
          None => {
            warn!("Connection receiver closed");
            cancel.cancel();
            break;
          }
        }
      }
      _ = pipe_to_client_pull.recv() => {}
    }
  }
}

async fn handle_new_connection(
  mut connection_state: ConnectionState,
  connection_type: ConnectionType,
  mut client_to_pipe_push: async_channel::Sender<Bytes>,
  mut pipe_to_client_pull: broadcast::Receiver<Bytes>,
  cancel: CancellationToken,
) {
  let connect_result = match connection_type {
    ConnectionType::Direct { target_address } => {
      initiate_direct_connection(
        &mut connection_state,
        target_address,
        &mut client_to_pipe_push,
        &mut pipe_to_client_pull,
      )
      .await
    }
    ConnectionType::Socks5 => {
      initiate_socks5_connection(
        &mut connection_state,
        &mut client_to_pipe_push,
        &mut pipe_to_client_pull,
      )
      .await
    }
  };
  if let Err(e) = connect_result {
    info!("Failed to initialize connection, closing client. {}", e);
    if let Err(e) = connection_state.client.shutdown().await {
      error!("Failed to shutdown client after initialization failed: {}", e);
    }
  } else {
    tokio::spawn(connection_loop(
      connection_state,
      pipe_to_client_pull.resubscribe(),
      client_to_pipe_push.clone(),
      cancel.clone(),
    ));
  }
}

async fn initiate_direct_connection(
  connection_state: &mut ConnectionState,
  target_address: String,
  client_to_pipe_push: &mut async_channel::Sender<Bytes>,
  pipe_to_client_pull: &mut broadcast::Receiver<Bytes>,
) -> anyhow::Result<()> {
  info!("Starting direct connection to {}", &target_address);
  match initiate_connection(
    connection_state.identifier,
    target_address,
    client_to_pipe_push,
    pipe_to_client_pull,
  )
  .await
  {
    true => Ok(()),
    false => Err(anyhow::anyhow!("Failed to initialize direct connection")),
  }
}

async fn initiate_socks5_connection(
  connection_state: &mut ConnectionState,
  client_to_pipe_push: &mut async_channel::Sender<Bytes>,
  pipe_to_client_pull: &mut broadcast::Receiver<Bytes>,
) -> anyhow::Result<()> {
  // Use the local address as the guest does not reply with the actual address of it's connection
  let local_address = connection_state.client.local_addr()?;
  debug!("Setting up socks5 connection for {}", &local_address);
  match Socks5ServerProtocol::accept_no_auth(&mut connection_state.client)
    .and_then(|prot| prot.read_command())
    .await
  {
    Ok((socks5_prot, _, addr)) => {
      info!("Starting socks5 connection to {}", &addr);
      if initiate_connection(
        connection_state.identifier,
        addr.to_string(),
        client_to_pipe_push,
        pipe_to_client_pull,
      )
      .await
      {
        debug!("Guest connection established, notifying socks5 client");
        socks5_prot
          .reply_success(local_address)
          .await
          .map(|_| ())
          .context("Failed to notify client of successful socks5 connection")
      } else {
        debug!("Guest connection failed, notifying socks5 client");
        if let Err(e) = socks5_prot.reply_error(&ReplyError::HostUnreachable).await {
          error!("Failed to notify client of failed socks5 connection: {}", e);
        }
        Err(anyhow::anyhow!("Failed to initialize guest connection"))
      }
    }
    Err(e) => Err(anyhow::Error::new(e)),
  }
}

async fn initiate_connection(
  connection_identifier: u64,
  target_address: String,
  client_to_pipe_push: &mut async_channel::Sender<Bytes>,
  pipe_to_client_pull: &mut broadcast::Receiver<Bytes>,
) -> bool {
  let initial_datagram = create_initial_datagram(connection_identifier, 0, &target_address);
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
            if datagram.identifier() == connection_identifier {
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
      if datagram.code() != ControlCode::Ack || datagram.identifier() != connection_identifier {
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

#[instrument(skip_all, fields(connection_id = %connection.identifier))]
pub async fn connection_loop(
  mut connection: ConnectionState,
  mut pipe_to_client_pull: broadcast::Receiver<Bytes>,
  client_to_pipe_push: async_channel::Sender<Bytes>,
  cancel: CancellationToken,
) {
  let mut tcp_buf = BytesMut::zeroed(CONNECTION_BUFFER_SIZE);
  loop {
    tcp_buf.resize(CONNECTION_BUFFER_SIZE, 0);
    tokio::select! {
      biased;
      _ = cancel.cancelled() => {
        if let Err(e) = connection.client.shutdown().await {
          error!("Failed to shutdown client after server shutdown: {}", e);
        }
        break;
      }
      data = pipe_to_client_pull.recv() => {
        if process_sink_read(&mut connection, data).await {
          break;
        }
      }
      bytes_read = connection.client.read(&mut tcp_buf) => {
        connection.sequence += 1;
        if handle_client_read(connection.identifier, connection.sequence, client_to_pipe_push.clone(), bytes_read, &mut tcp_buf).await {
          break;
        }
      }
    }
  }
  debug!("Connection {} loop ending", connection.identifier);
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::protocol_utils::create_ack_datagram;
  use crate::test_utils::{run_echo, setup_tracing};
  use fast_socks5::client::{Config, Socks5Stream};
  use tokio::net::TcpStream;

  #[tokio::test]
  async fn test_run_listener_smoke() {
    setup_tracing().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_address = listener.local_addr().unwrap();
    let (target_address, _) = run_echo().await;
    let connection_type = ConnectionType::new_direct(target_address.to_string());
    let (connection_sender, mut connection_receiver) = mpsc::channel(1);
    tokio::spawn({
      let connection_type = connection_type.clone();
      async move {
        run_listener(listener, connection_type, connection_sender, CancellationToken::new()).await;
      }
    });
    let test_connection = TcpStream::connect(listener_address).await.unwrap();
    let (connection, connection_type_received) = connection_receiver.recv().await.unwrap();
    assert_eq!(connection_type_received, connection_type);
    assert_eq!(0, connection.identifier);
    assert_eq!(connection.client.peer_addr().unwrap(), test_connection.local_addr().unwrap());
  }

  #[tokio::test]
  async fn test_initiate_connection_success() {
    setup_tracing().await;
    let (pipe_to_client_push, pipe_to_client_pull) = broadcast::channel(256);
    let (client_to_pipe_push, client_to_pipe_pull) = async_channel::bounded(256);
    let (address, _) = run_echo().await;
    let target_address = "test:1234";
    let identifier = 1;
    let connection = ConnectionState::new(identifier, TcpStream::connect(address).await.unwrap());

    let initiate_handle = tokio::spawn({
      let mut client_to_pipe_push = client_to_pipe_push.clone();
      let mut pipe_to_client_pull = pipe_to_client_pull.resubscribe();
      async move {
        initiate_connection(
          connection.identifier,
          target_address.to_string(),
          &mut client_to_pipe_push,
          &mut pipe_to_client_pull,
        )
        .await
      }
    });

    let initial_datagram_bytes = client_to_pipe_pull.recv().await.unwrap();
    let initial_datagram = root_as_datagram(&initial_datagram_bytes).unwrap();
    let initial_data = initial_datagram.data().unwrap().bytes();
    assert_eq!(target_address.as_bytes(), initial_data);
    assert_eq!(identifier, initial_datagram.identifier());
    pipe_to_client_push
      .send(create_ack_datagram(initial_datagram.identifier(), 0))
      .unwrap();

    assert!(initiate_handle.await.unwrap());
  }

  #[tokio::test]
  async fn initiate_socks5_connection_success() {
    setup_tracing().await;
    let (pipe_to_client_push, mut pipe_to_client_pull) = broadcast::channel(256);
    let (mut client_to_pipe_push, client_to_pipe_pull) = async_channel::bounded::<Bytes>(256);
    let (connection_sender, mut connection_receiver) = mpsc::channel(128);

    let local_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_address = local_listener.local_addr().unwrap();
    let listener_socks5_task = tokio::spawn(async move {
      let connection = local_listener.accept().await.unwrap().0;
      connection_sender
        .send((ConnectionState::new(0, connection), ConnectionType::Socks5))
        .await
        .unwrap();
    });
    let socks5_client_task = tokio::spawn(Socks5Stream::connect(
      local_address,
      "target.test".to_string(),
      1234,
      Config::default(),
    ));
    let (mut connection_state, connection_type) = connection_receiver.recv().await.unwrap();

    assert_eq!(connection_type, ConnectionType::Socks5);

    let guest_task = tokio::spawn(async move {
      let initial = client_to_pipe_pull.recv().await.unwrap();
      let initial_datagram = root_as_datagram(&initial).unwrap();
      let ack = create_ack_datagram(initial_datagram.identifier(), 0);
      pipe_to_client_push.send(ack).unwrap();
    });

    assert!(
      initiate_socks5_connection(
        &mut connection_state,
        &mut client_to_pipe_push,
        &mut pipe_to_client_pull
      )
      .await
      .is_ok()
    );
    timeout(Duration::from_secs(1), socks5_client_task)
      .await
      .unwrap()
      .unwrap()
      .unwrap();
    timeout(Duration::from_secs(1), guest_task)
      .await
      .unwrap()
      .unwrap();
    timeout(Duration::from_secs(1), listener_socks5_task)
      .await
      .unwrap()
      .unwrap();
  }
}
