use crate::common::{ConnectionState, HUGE_DATA_TARGET, connection_loop};
use crate::protocol_utils::{create_initial_datagram, datagram_from_bytes};
use crate::schema_generated::serial_multiplexer::{ControlCode, root_as_datagram};
use anyhow::{Context, Error};
use bytes::Bytes;
use fast_socks5::ReplyError;
use fast_socks5::server::Socks5ServerProtocol;
use futures::TryFutureExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};
use tracing_attributes::instrument;

#[cfg(not(test))]
pub(crate) static IDENTIFIER_SEQUENCE: AtomicU64 = AtomicU64::new(0);
// Since this is static, if the tests run in different order between multiple test runs, they
// could fail indeterministically if they use a constant connection identifier in assertions
#[cfg(test)]
pub(crate) static IDENTIFIER_SEQUENCE: AtomicU64 = AtomicU64::new(100);

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ConnectionType {
  Direct { target_address: String },
  Socks5,
  Http,
}

impl ConnectionType {
  pub const fn new_direct(target_address: String) -> Self {
    Self::Direct { target_address }
  }
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
      () = cancel.cancelled() => {
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
  client_to_sink_push: async_channel::Sender<Bytes>,
  sink_to_client_pull: async_broadcast::Receiver<Bytes>,
  mut connection_receiver: mpsc::Receiver<(ConnectionState, ConnectionType)>,
  cancel: CancellationToken,
) {
  debug!("Starting connection initiator");
  // Deactivate the receiver so we don't need to drain it
  let sink_to_client_pull_inactive = sink_to_client_pull.deactivate();
  loop {
    tokio::select! {
      biased;
      () = cancel.cancelled() => {
        break;
      }
      data = connection_receiver.recv() => {
        if let Some((connection_state, connection_type)) = data {
          tokio::spawn(handle_new_connection(
            connection_state,
            connection_type,
            client_to_sink_push.clone(),
            sink_to_client_pull_inactive.activate_cloned(),
            cancel.clone()
          ));
        } else {
          warn!("Connection receiver closed");
          cancel.cancel();
          break;
        }
      }
    }
  }
}

pub(crate) async fn handle_new_connection(
  mut connection_state: ConnectionState,
  connection_type: ConnectionType,
  client_to_sink_push: async_channel::Sender<Bytes>,
  mut sink_to_client_pull: async_broadcast::Receiver<Bytes>,
  cancel: CancellationToken,
) {
  let connect_result = match connection_type {
    ConnectionType::Direct { target_address } => {
      initiate_direct_connection(
        &connection_state,
        target_address,
        &client_to_sink_push,
        &mut sink_to_client_pull,
      )
      .await
    }
    ConnectionType::Socks5 => {
      initiate_socks5_connection(
        &mut connection_state,
        &client_to_sink_push,
        &mut sink_to_client_pull,
      )
      .await
    }
    // Http connections should have been initialized before this, so we just let them pass
    ConnectionType::Http => Ok(()),
  };
  if let Err(e) = connect_result {
    info!("Failed to initialize connection, closing client. {}", e);
    if let Err(e) = connection_state.client.shutdown().await {
      error!("Failed to shutdown client after initialization failed: {}", e);
    }
  } else {
    tokio::spawn(connection_loop(
      connection_state,
      sink_to_client_pull,
      client_to_sink_push,
      cancel,
    ));
  }
}

async fn initiate_direct_connection(
  connection_state: &ConnectionState,
  target_address: String,
  client_to_sink_push: &async_channel::Sender<Bytes>,
  sink_to_client_pull: &mut async_broadcast::Receiver<Bytes>,
) -> anyhow::Result<()> {
  info!("Starting direct connection to {}", &target_address);
  if initiate_connection(
    connection_state.identifier,
    target_address,
    client_to_sink_push,
    sink_to_client_pull,
  )
  .await
  {
    Ok(())
  } else {
    Err(anyhow::anyhow!("Failed to initialize direct connection"))
  }
}

async fn initiate_socks5_connection(
  connection_state: &mut ConnectionState,
  client_to_sink_push: &async_channel::Sender<Bytes>,
  sink_to_client_pull: &mut async_broadcast::Receiver<Bytes>,
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
        client_to_sink_push,
        sink_to_client_pull,
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
    Err(e) => Err(Error::new(e)),
  }
}

pub(crate) async fn initiate_connection(
  connection_identifier: u64,
  target_address: String,
  client_to_sink_push: &async_channel::Sender<Bytes>,
  sink_to_client_pull: &mut async_broadcast::Receiver<Bytes>,
) -> bool {
  let initial_datagram = create_initial_datagram(connection_identifier, 0, &target_address);
  trace!(target: HUGE_DATA_TARGET, "Sending initial datagram: {:?}", datagram_from_bytes(&initial_datagram));
  if let Err(e) = client_to_sink_push.send(initial_datagram).await {
    info!("Failed to initialize connection: {}", e);
    return false;
  }
  // TODO rework
  if let Ok(Some(datagram)) = timeout(Duration::from_secs(5), async {
    loop {
      match sink_to_client_pull.recv_direct().await {
        Ok(data) => match root_as_datagram(&data) {
          Ok(datagram) => {
            trace!(target: HUGE_DATA_TARGET, "Received datagram from server: {:?}", datagram);
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
    let datagram = root_as_datagram(&datagram)
      .expect("Datagram should have been validated by checking the identifier");
    if datagram.code() != ControlCode::Ack || datagram.identifier() != connection_identifier {
      debug!(target: HUGE_DATA_TARGET, "Received invalid response from server: {:?}", datagram);
      return false;
    }
    debug!("Successfully initiated connection to {}", target_address);
    true
  } else {
    debug!("Failed to initialize connection, pipe is closed or failed to receive response");
    false
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::protocol_utils::create_ack_datagram;
  use crate::test_utils::{receive_initial_ack_data, run_echo, setup_tracing};
  use crate::utils::create_upstream_listener;
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
    assert_eq!(connection.client.peer_addr().unwrap(), test_connection.local_addr().unwrap());
  }

  #[tokio::test]
  async fn test_initiate_connection_success() {
    setup_tracing().await;
    let (pipe_to_client_push, pipe_to_client_pull) = async_broadcast::broadcast(256);
    let (client_to_pipe_push, client_to_pipe_pull) = async_channel::bounded(256);
    let (address, _) = run_echo().await;
    let target_address = "test:1234";
    let identifier = 1;
    let connection = ConnectionState::new(identifier, TcpStream::connect(address).await.unwrap());

    let initiate_handle = tokio::spawn({
      let client_to_pipe_push = client_to_pipe_push.clone();
      let mut pipe_to_client_pull = pipe_to_client_pull.clone();
      async move {
        initiate_connection(
          connection.identifier,
          target_address.to_string(),
          &client_to_pipe_push,
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
      .broadcast_direct(create_ack_datagram(initial_datagram.identifier(), 0, 0))
      .await
      .unwrap();

    assert!(initiate_handle.await.unwrap());
  }

  #[tokio::test]
  async fn initiate_socks5_connection_success() {
    setup_tracing().await;
    let (pipe_to_client_push, mut pipe_to_client_pull) = async_broadcast::broadcast(256);
    let (client_to_pipe_push, client_to_pipe_pull) = async_channel::bounded::<Bytes>(256);
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
      let ack = create_ack_datagram(initial_datagram.identifier(), 0, 0);
      pipe_to_client_push.broadcast_direct(ack).await.unwrap();
    });

    assert!(
      initiate_socks5_connection(
        &mut connection_state,
        &client_to_pipe_push,
        &mut pipe_to_client_pull
      )
      .await
      .is_ok()
    );
    timeout(Duration::from_secs(1), socks5_client_task).await.unwrap().unwrap().unwrap();
    timeout(Duration::from_secs(1), guest_task).await.unwrap().unwrap();
    timeout(Duration::from_secs(1), listener_socks5_task).await.unwrap().unwrap();
  }

  #[tokio::test]
  async fn test_connection_initiator() {
    setup_tracing().await;
    let (pipe_to_client_push, sink_to_client_pull) = async_broadcast::broadcast::<Bytes>(256);
    let (client_to_sink_push, client_to_pipe_pull) = async_channel::bounded::<Bytes>(256);
    let (connection_sender, connection_receiver) =
      mpsc::channel::<(ConnectionState, ConnectionType)>(128);
    let cancel = CancellationToken::new();
    let target_addr = "test:1234";

    let initiator_task = tokio::spawn(connection_initiator(
      client_to_sink_push.clone(),
      sink_to_client_pull.clone(),
      connection_receiver,
      cancel.clone(),
    ));
    let connection_listener = create_upstream_listener("127.0.0.1:0").await.unwrap();
    let listener_address = connection_listener.local_addr().unwrap();
    let client_task = tokio::spawn({
      let cancel = cancel.clone();
      async move {
        let _server = TcpStream::connect(listener_address).await.unwrap();
        cancel.cancelled().await;
      }
    });
    let client = connection_listener.accept().await.unwrap().0;
    let connection_state = ConnectionState::new(0, client);
    let connection_type = ConnectionType::new_direct(target_addr.to_string());
    connection_sender.send((connection_state, connection_type)).await.unwrap();
    let datagram_bytes = client_to_pipe_pull.recv().await.unwrap();
    let initial_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(initial_datagram.identifier(), 0);
    assert_eq!(initial_datagram.code(), ControlCode::Initial);
    assert_eq!(initial_datagram.data().unwrap().bytes(), target_addr.as_bytes());
    let ack = create_ack_datagram(initial_datagram.identifier(), 0, 0);
    pipe_to_client_push.broadcast_direct(ack).await.unwrap();
    cancel.cancel();
    timeout(Duration::from_secs(1), initiator_task).await.unwrap().unwrap();
    timeout(Duration::from_secs(1), client_task).await.unwrap().unwrap();
  }

  #[tokio::test]
  async fn test_handle_new_connection_direct_success() {
    setup_tracing().await;
    let (pipe_to_client_push, pipe_to_client_pull) = async_broadcast::broadcast::<Bytes>(256);
    let (client_to_pipe_push, client_to_pipe_pull) = async_channel::bounded::<Bytes>(256);
    let cancel = CancellationToken::new();
    let (target_addr, _) = run_echo().await;
    let local_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_address = local_listener.local_addr().unwrap();
    let data_datagram_contents = Bytes::from_static(b"test");

    let client_task = tokio::spawn({
      let data_datagram_contents = data_datagram_contents.clone();
      async move {
        let mut client = TcpStream::connect(local_address).await.unwrap();
        client.write_all(&data_datagram_contents).await.unwrap();
      }
    });

    let connection_type = ConnectionType::new_direct(target_addr.to_string());
    let connection_state = ConnectionState::new(0, local_listener.accept().await.unwrap().0);

    let guest_task = receive_initial_ack_data(
      connection_state.identifier,
      Bytes::from(target_addr.to_string()),
      client_to_pipe_pull,
      pipe_to_client_push,
      data_datagram_contents,
    )
    .await;

    handle_new_connection(
      connection_state,
      connection_type,
      client_to_pipe_push,
      pipe_to_client_pull,
      cancel,
    )
    .await;

    timeout(Duration::from_secs(3), guest_task).await.unwrap().unwrap();
    timeout(Duration::from_secs(3), client_task).await.unwrap().unwrap();
  }

  #[tokio::test]
  async fn test_handle_new_connection_socks5_success() {
    setup_tracing().await;
    let (pipe_to_client_push, pipe_to_client_pull) = async_broadcast::broadcast::<Bytes>(256);
    let (client_to_pipe_push, client_to_pipe_pull) = async_channel::bounded::<Bytes>(256);
    let cancel = CancellationToken::new();
    let (target_addr, _) = run_echo().await;
    let local_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_address = local_listener.local_addr().unwrap();
    let data_datagram_contents = Bytes::from_static(b"test");

    let client_task = tokio::spawn({
      let data_datagram_contents = data_datagram_contents.clone();
      async move {
        let mut socks5_stream = Socks5Stream::connect(
          local_address,
          target_addr.ip().to_string(),
          target_addr.port(),
          Config::default(),
        )
        .await
        .unwrap();
        socks5_stream.write_all(&data_datagram_contents).await.unwrap();
      }
    });

    let connection_type = ConnectionType::Socks5;
    let connection_state = ConnectionState::new(0, local_listener.accept().await.unwrap().0);

    let guest_task = receive_initial_ack_data(
      connection_state.identifier,
      Bytes::from(target_addr.to_string()),
      client_to_pipe_pull,
      pipe_to_client_push,
      data_datagram_contents,
    )
    .await;

    handle_new_connection(
      connection_state,
      connection_type,
      client_to_pipe_push,
      pipe_to_client_pull,
      cancel,
    )
    .await;

    timeout(Duration::from_secs(3), guest_task).await.unwrap().unwrap();
    timeout(Duration::from_secs(3), client_task).await.unwrap().unwrap();
  }
}
