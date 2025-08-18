use crate::common::{ConnectionState, connection_loop};
use crate::protocol_utils::{create_ack_datagram, datagram_from_bytes};
use crate::schema_generated::serial_multiplexer::ControlCode;
use crate::utils::connect_downstream;
use anyhow::bail;
use async_broadcast::RecvError;
use bytes::Bytes;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const CLIENT_INITIATION_TIMEOUT: Duration = Duration::from_secs(3);

/// Reads datagrams from the sink looking for an [`Initial`] datagrams.
/// From these attempts to create a new client connection.
///
/// # Parameters
///
/// * `serial_to_client_pull`:
///   A [`async_broadcast::Receiver<Bytes>`] to receive data from a sink.
/// * `client_to_serial_push`: An [`async_channel::Sender<Bytes>`] to send data from clients back
///   to the sink(s).
/// * `cancel`: A [`CancellationToken`] to signal when the function should terminate.
///
/// # Behaviour
///
/// * Continuously waits for either incoming messages from the sink or a cancellation signal.
/// * When data is received from a sink,
///   it attempts to initiate a client connection within a [`CLIENT_INITIATION_TIMEOUT`] timeout
///   window using [`initiate_client_connection`].
///   * If successful and a connection is established,
///     it spawns a new task that runs [`connection_loop`],
///     handling communication between the client connection and the sink.
///   * If the connection initiation fails or exceeds the timeout, appropriate errors are logged.
/// * If no data can be received from a sink(s) (i.e. all ports are closed),
///   `cancel` will be triggered, and the loop ends.
/// * The loop terminates when the cancellation token (`cancel`) is triggered.
///
/// [`Initial`]: ControlCode::Initial
pub async fn client_initiator(
  mut serial_to_client_pull: async_broadcast::Receiver<Bytes>,
  client_to_serial_push: async_channel::Sender<Bytes>,
  cancel: CancellationToken,
) {
  debug!("Starting client initiator");
  loop {
    tokio::select! {
      biased;
      () = cancel.cancelled() => {
        break;
      }
      data = serial_to_client_pull.recv() => {
        match data {
          Ok(data) => {
            match timeout(CLIENT_INITIATION_TIMEOUT,
              initiate_client_connection(data, client_to_serial_push.clone())).await {
              Ok(Ok(Some(connection))) => {
                tokio::spawn(connection_loop(
                  connection,
                  serial_to_client_pull.clone(),
                  client_to_serial_push.clone(),
                  cancel.clone()
                ));
              },
              Ok(Err(e)) => {
                error!("Failed to initiate client connection: {}", e);
              },
              Err(_) => {
                error!("Failed to initiate client connection in time");
              }
              Ok(Ok(None)) => {}
            }
          }
          Err(RecvError::Overflowed(amount)) => {
            warn!("Missed {amount} datagrams due to lag, some INITIAL datagrams might have been skipped");
          }
          Err(RecvError::Closed) => {
            error!("Failed to receive data, {}", RecvError::Closed);
            cancel.cancel();
            break;
          }
        }
      }
    }
  }
}

/// Handles the initiation of a client connection by processing an incoming datagram
/// and establishing a downstream connection if the datagram is an [`Initial`] datagram.
///
/// # Parameters
///
/// * `data`: The raw [`Bytes`] representing the incoming datagram.
/// * `client_to_serial_push`:
///   An [`async_channel::Sender<Bytes>`] to send datagrams to the sink(s).
///
/// # Returns
/// An [`anyhow::Result`] that contains:
/// * [`Ok(Some(ConnectionState))`] if the connection was successfully established.
/// * [`Err(anyhow::Error)`] if the connection could not be established
/// * [`Ok(None)`] if the datagram was not an [`Initial`] datagram or if it was invalid.
///
/// # Errors
/// Returns an error if:
/// * The datagram is an initial connection request but does not contain a valid target address.
/// * Establishing a connection to the downstream target fails.
/// * Sending an acknowledgement ([`ACK`]) back to the client fails.
///
/// # Behaviour
/// * Parses the incoming datagram using [`datagram_from_bytes`].
/// * If it's an initial connection request ([`Initial`]), extracts the target
///   address from the datagram, establishes a connection to the downstream,
///   and sends an [`ACK`] response to the client.
/// * Performs clean-up by shutting down the downstream connection if sending the [`ACK`] fails.
///
/// [`Initial`]: ControlCode::Initial
/// [`ACK`]: ControlCode::Ack
async fn initiate_client_connection(
  data: Bytes,
  client_to_serial_push: async_channel::Sender<Bytes>,
) -> anyhow::Result<Option<ConnectionState>> {
  match datagram_from_bytes(&data) {
    Ok(datagram) => {
      // Not the first datagram for connection, ignore
      if datagram.code() != ControlCode::Initial {
        return Ok(None);
      }
      debug!("Connection {} received {:?} datagram", datagram.identifier(), datagram.code());
      let identifier = datagram.identifier();
      let Some(target_address) = datagram.data().map(|d| String::from_utf8_lossy(d.bytes())) else {
        bail!("Initial datagram did not contain target address");
      };
      info!("Connecting to downstream: {}", target_address);
      let mut downstream = connect_downstream(&target_address).await?;
      let ack = create_ack_datagram(identifier, 0);
      let ack_datagram = datagram_from_bytes(&ack);
      debug!("Sending ACK: {:?}", ack_datagram);
      if let Err(e) = client_to_serial_push.send(ack).await {
        if let Err(e) = downstream.shutdown().await {
          error!("Failed to shutdown downstream after failing to send ACK: {}", e);
        }
        bail!("Failed to send ACK : {}", e);
      }
      Ok(Some(ConnectionState::new(identifier, downstream)))
    }
    Err(e) => {
      debug!("Received invalid datagram, will ignore: {}", e);
      Ok(None)
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::protocol_utils::create_initial_datagram;
  use crate::schema_generated::serial_multiplexer::root_as_datagram;
  use crate::test_utils::{run_echo, setup_tracing};

  #[tokio::test]
  async fn test_initiate_client_connection_smoke() {
    setup_tracing().await;
    let (client_to_serial_push, client_to_serial_pull) = async_channel::bounded(10);
    let (target_address, _) = run_echo().await;
    let target_address = target_address.to_string();
    let datagram = create_initial_datagram(123, 0, &target_address);

    let connection =
      initiate_client_connection(datagram, client_to_serial_push.clone()).await.unwrap().unwrap();
    assert_eq!(connection.identifier, 123);
    assert_eq!(connection.sequence, 0);
    assert_eq!(connection.largest_processed, 0);
    assert_eq!(connection.client.peer_addr().unwrap().to_string(), target_address);
    let ack_data = client_to_serial_pull.recv().await.unwrap();
    let ack_datagram = root_as_datagram(&ack_data).unwrap();
    assert_eq!(ack_datagram.code(), ControlCode::Ack);
    assert_eq!(ack_datagram.identifier(), 123);
    assert_eq!(ack_datagram.data().unwrap().len(), 0);
  }

  #[tokio::test]
  async fn test_initiate_client_connection_invalid_datagram() {
    setup_tracing().await;
    let (client_to_serial_push, _) = async_channel::bounded(10);

    let connection = initiate_client_connection(Bytes::new(), client_to_serial_push.clone()).await;
    assert!(matches!(connection, Ok(None)));
  }

  #[tokio::test]
  async fn test_client_initiator_success() {
    setup_tracing().await;
    let (client_to_serial_push, client_to_serial_pull) = async_channel::bounded(10);
    let (serial_to_client_push, serial_to_client_pull) = async_broadcast::broadcast(10);
    let (target_address, _) = run_echo().await;
    let target_address = target_address.to_string();
    let initial = create_initial_datagram(123, 0, &target_address);

    tokio::spawn(client_initiator(
      serial_to_client_pull,
      client_to_serial_push,
      CancellationToken::new(),
    ));

    serial_to_client_push.broadcast_direct(initial).await.unwrap();
    let ack = client_to_serial_pull.recv().await.unwrap();
    let ack_datagram = root_as_datagram(&ack).unwrap();
    assert_eq!(ack_datagram.code(), ControlCode::Ack);
    assert_eq!(ack_datagram.identifier(), 123);
    assert_eq!(ack_datagram.data().unwrap().len(), 0);
  }

  #[tokio::test]
  async fn test_client_initiator_target_unreachable() {
    setup_tracing().await;
    let (client_to_serial_push, client_to_serial_pull) = async_channel::bounded(10);
    let (serial_to_client_push, serial_to_client_pull) = async_broadcast::broadcast(10);
    let target_address = "127.0.0.1:0".to_string();
    let initial = create_initial_datagram(123, 0, &target_address);

    tokio::spawn(client_initiator(
      serial_to_client_pull,
      client_to_serial_push,
      CancellationToken::new(),
    ));

    serial_to_client_push.broadcast_direct(initial).await.unwrap();
    assert!(timeout(Duration::from_secs(1), client_to_serial_pull.recv()).await.is_err());
  }

  #[tokio::test]
  async fn test_client_initiator_pipe_closed() {
    setup_tracing().await;
    let (client_to_serial_push, _) = async_channel::bounded(10);
    let (serial_to_client_push, serial_to_client_pull) = async_broadcast::broadcast(10);
    let cancel = CancellationToken::new();

    tokio::spawn(client_initiator(serial_to_client_pull, client_to_serial_push, cancel.clone()));

    drop(serial_to_client_push);
    assert!(timeout(Duration::from_secs(1), cancel.cancelled()).await.is_ok());
  }
}
