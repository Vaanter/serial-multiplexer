use crate::protocol_utils::{create_ack_datagram, datagram_from_bytes};
use crate::schema_generated::serial_proxy::ControlCode;
use crate::server::{handle_client_read, process_sink_read};
use crate::utils::{connect_downstream, handle_sink_read, handle_sink_write};
use bytes::{Bytes, BytesMut};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio::time::timeout;
use tokio_serial::SerialStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

pub struct Client {
  pub identifier: u64,
  pub downstream: TcpStream,
}

pub async fn serial_loop(
  mut serial: SerialStream,
  mut client_to_serial_pull: mpsc::Receiver<Bytes>,
  serial_to_client_push: broadcast::Sender<Bytes>,
  cancel: CancellationToken,
) {
  let mut serial_buf = BytesMut::zeroed(3072);
  let mut stx_index: Option<usize> = None;
  let mut unprocessed_data_start = 0;
  loop {
    serial_buf.resize(3072, 0);
    tokio::select! {
      biased;
      _ = cancel.cancelled() => {
        break;
      }
      data = client_to_serial_pull.recv() => {
        if let Some(data) = data {
          if let Err(e) = handle_sink_write(&mut serial, data).await {
            error!("Failed to handle sink write: {}", e);
            cancel.cancel();
            break;
          }
        }
      }
      n = serial.read(&mut serial_buf) => {
        match n {
          Ok(0) => {
            info!("Serial disconnected");
            break;
          }
          Ok(n) => {
            match handle_sink_read(n + unprocessed_data_start, &mut serial_buf, &mut stx_index, serial_to_client_push.clone()).await {
              Ok(unprocessed_bytes) => unprocessed_data_start = unprocessed_bytes,
              Err(e) => {
                error!("Failed to handle sink read: {}", e);
                cancel.cancel();
                break;
              }
            }
          }
          Err(e) => {
            error!("Failed to read from serial: {}", e);
            cancel.cancel();
            break;
          }
        }
      }
    }
  }
}

pub async fn client_initiator(
  mut serial_to_client_pull: broadcast::Receiver<Bytes>,
  client_to_serial_push: mpsc::Sender<Bytes>,
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
            if let Ok(Some(client)) = timeout(Duration::from_secs(3),
              initiate_client_connection(data, client_to_serial_push.clone())).await {
              tokio::spawn(client_loop(client, client_to_serial_push.clone(), serial_to_client_pull.resubscribe(), cancel.clone()));
            } else {
              error!("Failed to initiate client connection");
            }
          }
          Err(e) => {
            error!("Failed to receive data from serial port: {}", e);
            break;
          }
        }
      }
    }
  }
}

async fn initiate_client_connection(
  data: Bytes,
  client_to_serial_push: mpsc::Sender<Bytes>,
) -> Option<Client> {
  match datagram_from_bytes(&data) {
    Ok(datagram) => {
      debug!("Received datagram: {:?}", datagram);
      // Not the first datagram for connection, ignore
      if datagram.code() != ControlCode::Initial {
        return None;
      }
      let identifier = datagram.identifier();
      let target_address = match datagram
        .data()
        .map(|d| String::from_utf8_lossy(d.bytes()).to_string())
      {
        Some(data) => data,
        None => {
          return None;
        }
      };
      debug!("Connecting to downstream: {}", target_address);
      let mut downstream = match connect_downstream(&target_address).await {
        Ok(downstream) => downstream,
        Err(e) => {
          error!("Failed to connect to downstream: {}", e);
          return None;
        }
      };
      let ack = create_ack_datagram(identifier);
      let ack_datagram = datagram_from_bytes(&ack);
      debug!("Sending ACK: {:?}", ack_datagram);
      if let Err(e) = client_to_serial_push.send(ack).await {
        error!("Failed to send ACK: {}", e);
        if let Err(e) = downstream.shutdown().await {
          error!("Failed to shutdown downstream after failing to send ACK: {}", e);
        }
        return None;
      }
      Some(Client {
        identifier,
        downstream,
      })
    }
    Err(e) => {
      debug!("Received invalid datagram, will ignore: {}", e);
      None
    }
  }
}

async fn client_loop(
  client: Client,
  client_to_serial_push: mpsc::Sender<Bytes>,
  mut serial_to_client_pull: broadcast::Receiver<Bytes>,
  cancel: CancellationToken,
) {
  let identifier = client.identifier;
  let mut downstream = client.downstream;
  let mut tcp_buf = BytesMut::zeroed(3072);
  loop {
    tcp_buf.resize(3072, 0);
    tokio::select! {
      biased;
      _ = cancel.cancelled() => {
        break;
      }
      data = serial_to_client_pull.recv() => {
        if process_sink_read(identifier, data, &mut downstream).await {
          break;
        }
      }
      n = downstream.read(&mut tcp_buf) => {
        if handle_client_read(identifier, client_to_serial_push.clone(), n, &mut tcp_buf).await {
          break;
        }
      }
    }
  }
  debug!("Client {} disconnected", identifier);
}
