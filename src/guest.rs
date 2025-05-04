use crate::common::Connection;
use crate::common::{CONNECTION_BUFFER_SIZE, handle_client_read, process_sink_read};
use crate::protocol_utils::{create_ack_datagram, datagram_from_bytes};
use crate::schema_generated::serial_multiplexer::ControlCode;
use crate::utils::connect_downstream;
use anyhow::bail;
use bytes::{Bytes, BytesMut};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::broadcast;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};
use tracing_attributes::instrument;

pub async fn client_initiator(
  mut serial_to_client_pull: broadcast::Receiver<Bytes>,
  client_to_serial_push: async_channel::Sender<Bytes>,
  cancel: CancellationToken,
) {
  loop {
    tokio::select! {
      biased;
      _ = cancel.cancelled() => {
        break;
      }
      data = serial_to_client_pull.recv() => {
        match data {
          Ok(data) => {
            match timeout(Duration::from_secs(3),
              initiate_client_connection(data, client_to_serial_push.clone())).await {
              Ok(Ok(Some(connection))) => {
                tokio::spawn(guest_loop(connection, client_to_serial_push.clone(), serial_to_client_pull.resubscribe(), cancel.clone()));
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
          Err(e) => {
            error!("Failed to receive data from serial port: {}", e);
            cancel.cancel();
            break;
          }
        }
      }
    }
  }
}

async fn initiate_client_connection(
  data: Bytes,
  client_to_serial_push: async_channel::Sender<Bytes>,
) -> anyhow::Result<Option<Connection>> {
  match datagram_from_bytes(&data) {
    Ok(datagram) => {
      // Not the first datagram for connection, ignore
      if datagram.code() != ControlCode::Initial {
        return Ok(None);
      }
      debug!("Connection {} received {:?} datagram", datagram.identifier(), datagram.code());
      let identifier = datagram.identifier();
      let target_address = match datagram.data().map(|d| String::from_utf8_lossy(d.bytes())) {
        Some(data) => data,
        None => {
          bail!("Initial datagram did not contain target address");
        }
      };
      debug!("Connecting to downstream: {}", target_address);
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
      Ok(Some(Connection::new(identifier, downstream)))
    }
    Err(e) => {
      debug!("Received invalid datagram, will ignore: {}", e);
      Ok(None)
    }
  }
}

#[instrument(skip_all, fields(connection_id = %connection.identifier))]
async fn guest_loop(
  mut connection: Connection,
  client_to_serial_push: async_channel::Sender<Bytes>,
  mut serial_to_client_pull: broadcast::Receiver<Bytes>,
  cancel: CancellationToken,
) {
  let mut tcp_buf = BytesMut::zeroed(CONNECTION_BUFFER_SIZE);
  loop {
    tcp_buf.resize(CONNECTION_BUFFER_SIZE, 0);
    tokio::select! {
      biased;
      _ = cancel.cancelled() => {
        break;
      }
      data = serial_to_client_pull.recv() => {
        if process_sink_read(&mut connection, data).await {
          break;
        }
      }
      bytes_read = connection.client.read(&mut tcp_buf) => {
        connection.sequence += 1;
        if handle_client_read(connection.identifier, connection.sequence,  client_to_serial_push.clone(), bytes_read, &mut tcp_buf).await {
          break;
        }
      }
    }
  }
  debug!("Client {} disconnected", connection.identifier);
}
