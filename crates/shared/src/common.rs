use crate::channels::{ChannelMap, Identifier, channel_get_or_insert_with_guard};
use crate::protocol_utils::{create_ack_datagram, create_close_datagram, create_data_datagram};
use crate::schema_generated::serial_multiplexer::{ControlCode, root_as_datagram};
use crate::try_write::Sink;
use anyhow::{Context, anyhow, bail};
use bytes::{Bytes, BytesMut};
use memchr::memmem::Finder;
use std::cmp::{max, min};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug, error, info, trace};
use tracing_attributes::instrument;
use zeroize::Zeroize;

/// Specifies how many datagrams without ACK a connection can have
/// before the TCP connection gets plugged.
pub const ACK_THRESHOLD: u64 = 32;

const SINK_BUFFER_SIZE: usize = 2usize.pow(17);
/// The maximum size of a (de)compressed datagram.
pub const SINK_COMPRESSION_BUFFER_SIZE: usize = 2usize.pow(15);
pub const CONNECTION_BUFFER_SIZE: usize = SINK_COMPRESSION_BUFFER_SIZE - 256;
const _: () = {
  // Comptime check that the size of data after compression doesn't exceed the compression buffer
  // Formula for maximum size taken from zstd source code
  // https://github.com/facebook/zstd/blob/f9938c217da17ec3e9dcd2a2d99c5cf39536aeb9/lib/zstd.h#L249
  // https://facebook.github.io/zstd/zstd_manual.html
  let mut maximum_size = CONNECTION_BUFFER_SIZE;
  maximum_size += CONNECTION_BUFFER_SIZE >> 8;
  if (CONNECTION_BUFFER_SIZE) < (128 << 10) {
    maximum_size += ((128 << 10) - (CONNECTION_BUFFER_SIZE)) >> 11;
  }
  assert!(maximum_size < SINK_COMPRESSION_BUFFER_SIZE);
  // Blocked connections read 4 bytes at a time to check if the connection was closed
  assert!(CONNECTION_BUFFER_SIZE.is_multiple_of(4));
};

/// An array representing a header used to identify the start of a datagram in a byte stream
const DATAGRAM_HEADER: [u8; 8] = [2, 200, 94, 2, 6, 9, 4, 20];
const HEADER_BYTES: usize = DATAGRAM_HEADER.len();
pub const HUGE_DATA_TARGET: &str = "serial_multiplexer::huge_data";
pub const METRICS_TARGET: &str = "serial_multiplexer::metrics";
/// Number of bytes used to store the size of a datagram
const LENGTH_BYTES: usize = 2;
static HEADER_FINDER: LazyLock<Finder<'static>> =
  LazyLock::new(|| Finder::new(&DATAGRAM_HEADER).into_owned());
const SINK_WRITE_RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// Represents the state of a network connection, holding information about the
/// connection identifier, client communication stream, sequence tracking, and
/// a queue for storing datagram received out-of-order.
///
/// # Fields
///
/// * `identifier` (`u64`) - A unique identifier assigned to the connection.
/// * `client` (`TcpStream`) - The TCP communication stream associated with the client.
/// * `sequence` (`u64`) - A counter tracking the sequence of the last sent datagram for the connection.
/// * `largest_processed` (`u64`) - The largest sequence number that has been processed.
/// * `largest_acked` (`u64`) - The largest sequence number for which this connection received an ACK.
/// * `datagram_queue` (`BTreeMap<u64, Bytes>`) - A map (ordered by sequence number) storing datagrams
///   that are awaiting processing because they arrived out-of-order.
/// * `client_address` (`String`) - A display string of clients connection used in traces
#[derive(Debug)]
pub struct ConnectionState {
  pub identifier: u64,
  pub client: TcpStream,
  pub sequence: u64,
  pub largest_processed: u64,
  pub largest_acked: u64,
  pub datagram_queue: BTreeMap<u64, Bytes>,
  pub client_address: String,
}

impl ConnectionState {
  pub fn new(identifier: u64, client: TcpStream) -> Self {
    let client_address =
      client.peer_addr().map(|a| format!("{:?}", a)).unwrap_or("???".to_string());
    Self {
      identifier,
      client,
      sequence: 0,
      largest_processed: 0,
      largest_acked: 0,
      datagram_queue: BTreeMap::new(),
      client_address,
    }
  }
}

/// Asynchronous loop for handling communication between a `sink` and clients.
///
/// This function manages bidirectional communication between a `sink` and clients using channels.
/// It reads data from the sink and pushes it to the appropriate client via separate broadcast channels,
/// while also receiving data from clients and writing it to the sink.
/// If the sink buffer becomes full, write operations are retried at intervals to prevent blocking.
/// The loop terminates on a cancellation signal, sink disconnection, or an error during read/write operations.
///
/// # Parameters
///
/// * `sink`: A [`Sink`] data transfer medium through which two multiplexers communicate.
/// * `channel_map`: A [`ChannelMap`] for sending received datagrams to clients/client initiator.
/// * `client_to_sink_pull`: An [`async_channel::Receiver`] channel through which the
///   function receives data sent by clients to be written to the sink.
/// * `cancel`: A [`CancellationToken`] used to signal when this loop should terminate.
///
/// # Behaviour
///
/// * Continuously reads from the `sink` using a buffer of size [`SINK_BUFFER_SIZE`] and
///   sends the processed data to the appropriate client channel.
/// * Continuously listens for data from the `client_to_sink_pull` channel to write to the `sink`.
/// * Retries failed writes to the sink at intervals to prevent indefinite blocking when the sink is full.
/// * Exits the loop gracefully when the cancellation token is triggered, the sink is
///   disconnected, or an error occurs during read/write operations.
///
/// # Key Operations:
///
/// * `sink.read()`:
///   * Reads data from the `sink` into an internal buffer.
///   * Processes any unprocessed bytes in the buffer and sends them to the appropriate client channel.
///   * Handles errors or disconnects from the `sink`, breaking the loop if necessary.
///
/// * `client_to_sink_pull.recv()`:
///   * Receives data from a client to write into the `sink`.
///   * Only receives when there is no pending data to write.
///   * Uses the `handle_sink_write()` function to handle the actual write to the `sink`.
///
/// * `sleep(SINK_WRITE_RETRY_INTERVAL)`:
///   * Retries writing pending data to the sink at regular intervals.
///
/// * `cancel.cancelled()`:
///   * Terminates the loop immediately when cancellation is triggered.
///
/// [`SerialStream`]: tokio_serial::SerialStream
/// [`NamedPipeClient`]: tokio::net::windows::named_pipe::NamedPipeServer
/// [`UnixStream`]: tokio::net::unix::socket::UnixSocket
#[instrument(skip_all)]
pub async fn sink_loop(
  mut sink: impl Sink,
  channel_map: ChannelMap,
  client_to_sink_pull: async_channel::Receiver<Bytes>,
  cancel: CancellationToken,
) {
  let mut sink_buf_read = BytesMut::zeroed(SINK_BUFFER_SIZE);
  let mut sink_buf_write = BytesMut::zeroed(SINK_BUFFER_SIZE);
  let mut compression_buffer = BytesMut::zeroed(SINK_COMPRESSION_BUFFER_SIZE);
  let mut unprocessed_data_start = 0;
  let mut unwritten_data_end = 0;
  loop {
    let current_span = Span::current();
    sink_buf_read.resize(SINK_BUFFER_SIZE, 0);
    sink_buf_write.resize(SINK_BUFFER_SIZE, 0);
    tokio::select! {
      biased;
      () = cancel.cancelled() => {
        if let Err(e) = sink.shutdown().await {
          error!("Failed to shutdown sink: {}", e);
        }
        return;
      }
      n = sink.read(&mut sink_buf_read[unprocessed_data_start..]) => {
        match n {
          Ok(0) => {
            info!("Sink disconnected");
            break;
          }
          Ok(n) => {
            let sink_read_duration = Instant::now();
            let result = handle_sink_read(
              &channel_map,
              n + unprocessed_data_start,
              &mut sink_buf_read,
              &mut compression_buffer
            ).instrument(current_span).await;
            trace!(target: METRICS_TARGET, duration = ?sink_read_duration.elapsed(), "Finished reading data from sink");
            match result {
              Ok(unprocessed_bytes) => unprocessed_data_start = unprocessed_bytes,
              Err(e) => {
                error!("Failed to handle sink read: {}", e);
                break;
              }
            }
          }
          Err(e) => {
            error!("Failed to read from sink: {}", e);
            break;
          }
        }
      }
      data = client_to_sink_pull.recv(), if unwritten_data_end == 0 => {
        match data {
          Ok(data) => {
            let sink_write_duration = Instant::now();
            let write_result = handle_sink_write(&mut sink, data, &mut sink_buf_write)
              .instrument(current_span).await;
            trace!(target: METRICS_TARGET, duration = ?sink_write_duration.elapsed(), "Finished writing data to sink");
            match write_result {
              Ok(n) => unwritten_data_end = n,
              Err(e) => {
                error!("Failed to handle sink write: {}", e);
                break;
              }
            }
          },
          Err(e) => {
            info!("Sink receiver closed, {}", e);
            break;
          }
        }
      }
      () = sleep(SINK_WRITE_RETRY_INTERVAL), if unwritten_data_end > 0 => {
        let write_result = write_to_sink(&mut sink, &mut sink_buf_write[0..unwritten_data_end])
          .instrument(current_span).await;
        match write_result {
          Ok(n) => unwritten_data_end = n,
          Err(e) => {
            error!("Failed to handle sink write: {}", e);
            break;
          }
        }
      }
    }
  }
  cancel.cancel();
}

/// Handles reading data from a sink buffer, processes complete datagrams, and forwards them
/// to the appropriate client channels.
/// Any unprocessed data is preserved for subsequent reads.
///
/// # Parameters
///
/// * `channel_map`: A [`ChannelMap`] containing separate channels for each client.
/// * `bytes_read`: The number of bytes read into the sink buffer from the sink.
/// * `sink_buf`:
///   A mutable reference to a buffer ([`BytesMut`]) that stores the data read from the sink.
/// * `decompression_buffer`: A mutable reference to a buffer ([`BytesMut`]) for storing
///   the datagram after decompression
///
/// # Behaviour
///
/// 1. Reads data from the provided buffer `sink_buf` up to `bytes_read` bytes.
/// 2. Searches for a specific header ([`HEADER_FINDER`])
///    within the buffer to identify the start of datagrams.
/// 3. Extracts each datagram from the buffer if it is complete
///    (based on the size after the header).
/// 4. Decompresses the datagram
/// 5. Sends the decompressed datagram to the appropriate client channel based on the datagram identifier.
/// 6. Updates the sink buffer (`sink_buf`) to retain any unprocessed data for the next read.
///    Ensures already processed
///    data is removed or cleared appropriately.
///
/// # Returns
///
/// Returns `Ok(unprocessed_bytes)`
/// where `unprocessed_bytes` is the number of bytes in the `sink_buf`
/// that could not be processed (i.e. incomplete or insufficient data for a datagram).
///
/// # Errors
///
/// Returns an error if there is an issue with sending the datagram to the appropriate client channel.
/// The error is propagated using [`anyhow::Result`].
///
/// # Notes
///
/// * Datagram format assumptions:
///   * Datagram headers are identified using the [`HEADER_FINDER`].
///   * The size of the datagram is determined by bytes immediately following the header.
///     The number of bytes is specified by [`LENGTH_BYTES`].
///
/// * Buffer behaviour:
///   * If the entire buffer was processed, the buffer will be zeroed out.
///   * If some bytes remain unprocessed, they will be copied to the beginning of the buffer
///     and the rest of the buffer will be zeroed out.
pub async fn handle_sink_read(
  channel_map: &ChannelMap,
  bytes_read: usize,
  sink_buf: &mut BytesMut,
  decompression_buffer: &mut BytesMut,
) -> anyhow::Result<usize> {
  debug_assert!(decompression_buffer.len() <= SINK_COMPRESSION_BUFFER_SIZE);
  debug_assert!(sink_buf.len() <= SINK_BUFFER_SIZE);
  trace!("Read {} bytes from sink", bytes_read);
  let mut read_data = &sink_buf[..bytes_read];
  trace!(target: HUGE_DATA_TARGET, "Data in working buffer: {:?}", &read_data);
  let mut unprocessed_data_start = 0;
  let mut last_header_idx = None;
  while let Some(header_idx) = HEADER_FINDER.find(read_data) {
    trace!("Found HEADER at index {}", header_idx);
    unprocessed_data_start += header_idx;
    last_header_idx = Some(unprocessed_data_start);
    if header_idx + HEADER_BYTES + LENGTH_BYTES > read_data.len() {
      trace!("Buffer is too small to contain datagram size");
      break;
    }
    let size = u16::from_be_bytes([
      read_data[header_idx + HEADER_BYTES],
      read_data[header_idx + HEADER_BYTES + 1],
    ]);
    trace!("Found size - {} bytes", size);
    if header_idx + size as usize + HEADER_BYTES + LENGTH_BYTES > read_data.len() {
      trace!("Buffer is too small to contain all datagram bytes");
      break;
    }
    let datagram_start = header_idx + HEADER_BYTES + LENGTH_BYTES;
    let datagram_end = datagram_start + size as usize - 1;
    let decompressed_size = zstd_safe::decompress(
      decompression_buffer.as_mut(),
      &read_data[datagram_start..=datagram_end],
    )
    .inspect(|n| trace!("Decompressed {} to {} bytes", size, n))
    .map_err(|e| anyhow!("Failed to decompress datagram with error code = {}", e))?;
    let datagram_bytes = Bytes::copy_from_slice(&decompression_buffer[..decompressed_size]);
    decompression_buffer.zeroize();
    trace!(target: HUGE_DATA_TARGET, "Read datagram: {:?}", datagram_bytes.as_ref());
    unprocessed_data_start += datagram_end + 1 - header_idx;
    trace!(
      "Removing processed data in the working buffer at index {}",
      min(bytes_read, unprocessed_data_start)
    );
    read_data = &sink_buf[min(bytes_read, unprocessed_data_start)..bytes_read];
    trace!(target: HUGE_DATA_TARGET, "Data in buffer after reading datagram: {:?}", &read_data);
    let Ok(datagram) = root_as_datagram(&datagram_bytes) else {
      continue;
    };
    trace!("Read {:?} datagram for connection {}", datagram.code(), datagram.identifier());
    let channel_map_pin = channel_map.pin_owned();
    if datagram.code() == ControlCode::Initial {
      if let Some(initiator_channel) = channel_map_pin.get(&Identifier::ClientInitiator)
        && initiator_channel.0.broadcast_direct(datagram_bytes).await.is_err()
      {
        bail!("Failed to send Initial datagram to client initiator");
      }
    } else if let Some(client_channel) =
      channel_map_pin.get(&Identifier::Client(datagram.identifier()))
      && let Err(e) = client_channel.0.broadcast_direct(datagram_bytes).await
    {
      bail!("Failed to send data to clients. {}", e);
    }
  }

  if unprocessed_data_start > last_header_idx.unwrap_or(0) || last_header_idx.is_none() {
    unprocessed_data_start =
      max(unprocessed_data_start, bytes_read.saturating_sub(HEADER_BYTES - 1));
  }
  let unprocessed_bytes = bytes_read - unprocessed_data_start;
  if unprocessed_data_start >= bytes_read {
    trace!("Whole buffer was read, zeroing out processed data");
    sink_buf.zeroize();
  } else if unprocessed_data_start > 0 {
    trace!(
      "Copying unprocessed data (start index: {}) to the start of buffer and zeroing out the rest",
      unprocessed_data_start
    );
    let buffer_end = sink_buf.len();
    trace!(target: HUGE_DATA_TARGET, "Buffer before copying and zeroing: {:?}", sink_buf.as_ref());
    sink_buf.copy_within(unprocessed_data_start..buffer_end, 0);
    sink_buf[unprocessed_bytes..buffer_end].zeroize();
    trace!(target: HUGE_DATA_TARGET, "Buffer after copying and zeroing: {:?}", sink_buf.as_ref());
  }
  Ok(unprocessed_bytes)
}

/// Handles writing of the datagram header, length, and compressed datagram to the sink.
///
/// Returns Ok(n) where n is the number of unwritten bytes.
///
/// # Parameters
/// * `sink`: A [`Sink`] data transfer medium through which two multiplexers communicate.
/// * `data`:
///   A [`Bytes`] object representing the datagram that will be written to the sink.
/// * `compression_buffer`: A buffer to store all bytes (header, length, and datagram) that need
///   to be written to the sink.
///
/// # Behaviour
/// The function follows these steps:
/// 1. Compresses the datagram using [`zstd_safe`] into `compression_buffer`
/// 2. Prepares the header, length, and compressed datagram in the buffer.
/// 3. Attempts to write the data to the sink using `write_to_sink`.
/// 4. If the sink is full, returns the number of unwritten bytes.
/// 5. If successful, flushes the sink to ensure everything is written out.
///
/// # Errors
/// When compressing the datagram or writing to the sink fails.
///
/// [`SerialStream`]: tokio_serial::SerialStream
/// [`NamedPipeClient`]: tokio::net::windows::named_pipe::NamedPipeServer
/// [`UnixStream`]: tokio::net::unix::socket::UnixSocket
pub async fn handle_sink_write(
  sink: &mut impl Sink,
  data: Bytes,
  sink_buffer: &mut BytesMut,
) -> anyhow::Result<usize> {
  trace!("Maximum compressed size: {}", zstd_safe::compress_bound(data.len()));
  let compressed_size =
    zstd_safe::compress(sink_buffer[HEADER_BYTES + LENGTH_BYTES..].as_mut(), &data, 9)
      .inspect(|n| trace!("Compressed {} to {} bytes", data.len(), n))
      .map_err(|e| anyhow!("Failed to compress datagram with error code = {}", e))?;
  sink_buffer[0..HEADER_BYTES].copy_from_slice(&DATAGRAM_HEADER);
  sink_buffer[HEADER_BYTES..HEADER_BYTES + LENGTH_BYTES]
    .copy_from_slice(&(compressed_size as u16).to_be_bytes());
  let data_to_send = &mut sink_buffer[..HEADER_BYTES + LENGTH_BYTES + compressed_size];
  trace!(target: HUGE_DATA_TARGET, "Writing compressed datagram to sink: {:?}", data_to_send);
  write_to_sink(sink, data_to_send).await
}

async fn write_to_sink(sink: &mut impl Sink, data_to_send: &mut [u8]) -> anyhow::Result<usize> {
  let bytes_to_send = data_to_send.len();
  trace!("Writing {} bytes to sink", bytes_to_send);
  let mut bytes_sent = 0;
  while bytes_sent < bytes_to_send {
    match timeout(Duration::from_millis(10), sink.write(&data_to_send[bytes_sent..])).await {
      Ok(Ok(bytes_written)) => {
        trace!("Wrote {} bytes to sink", bytes_written);
        bytes_sent = bytes_sent.saturating_add(bytes_written);
      }
      Ok(Err(e)) => bail!(e),
      Err(_) => {
        let leftover_bytes = bytes_to_send.saturating_sub(bytes_sent);
        trace!("Unable to write {} bytes to sink", leftover_bytes);
        data_to_send.copy_within(bytes_sent..bytes_to_send, 0);
        data_to_send[bytes_to_send.saturating_sub(bytes_sent)..].zeroize();
        return Ok(leftover_bytes);
      }
    };
  }
  trace!("All bytes written to sink");
  sink.flush().await.context("Failed to flush data to sink")?;
  data_to_send.zeroize();
  Ok(0)
}

///
/// Handles reads from client connections.
///
/// This function processes the result of a read operation from a client and performs actions
/// based on whether the read was successful, encountered an error, or if the client disconnected.
/// It sends appropriate datagrams to be handled downstream via the provided channel.
///
/// # Parameters
///
/// * `identifier`: A unique identifier for the client connection.
/// * `sequence`: The current datagram sequence number for the connection, used for ordering data.
/// * `client_to_sink_push`: An async channel sender used to send datagrams to [`sink_loop`].
/// * `bytes_read`: The result of a read operation indicating the number of bytes read or an error.
/// * `read_buf`: A mutable buffer containing the bytes read from the client connection.
///
/// # Returns
///
/// A boolean indicating whether the connection should be closed:
/// * `true` if the connection should be terminated (e.g. client disconnected or read error).
/// * `false` if the connection remains active.
///
/// # Behaviour
///
/// * If `bytes_read` is `Ok(0)`, the client has disconnected. A "CLOSE" datagram is created
///   and sent via the channel. The function returns `true`.
///
/// * If `bytes_read` is `Ok(n)`, where `n > 0`, the bytes are processed, and a "DATA" datagram
///   is created and sent via the channel. The read buffer is cleared, and the function returns `false`.
///
/// * If `bytes_read` is `Err(e)`, an error occurred while reading from the client. The error
///   is logged, and the function returns `true`.
///
/// # Zeroization
///
/// The portion of the read buffer containing the bytes read is zeroed to clear
/// any sensitive data after processing.
pub async fn handle_client_read(
  identifier: u64,
  sequence: u64,
  client_to_sink_push: async_channel::Sender<Bytes>,
  bytes_read: std::io::Result<usize>,
  read_buf: &mut BytesMut,
) -> bool {
  let connection_ending = match bytes_read {
    Ok(0) => {
      info!("Client {} disconnected", identifier);
      true
    }
    Ok(n) => {
      assert!(
        n <= CONNECTION_BUFFER_SIZE,
        "Number of bytes read from client must not exceed the buffer"
      );
      debug!("Read {} bytes from client", n);
      let datagram = create_data_datagram(identifier, sequence, &read_buf[..n]);
      trace!(target: HUGE_DATA_TARGET, "Built datagram with client data: {:?}", datagram.as_ref());
      read_buf[..n].zeroize();
      debug!("Sending DATA datagram of {} bytes with seq: {}", datagram.len(), sequence);
      if let Err(e) = client_to_sink_push.send(datagram).await {
        error!("Failed to send DATA datagram for connection {}: {}", identifier, e);
        true
      } else {
        false
      }
    }
    Err(e) => {
      error!("Failed to read from client: {}", e);
      true
    }
  };
  if connection_ending {
    let datagram = create_close_datagram(identifier, sequence);
    if let Err(e) = client_to_sink_push.send(datagram).await {
      error!("Failed to send CLOSE datagram for connection {}: {}", identifier, e);
    }
  }
  connection_ending
}

/// Processes incoming datagrams for a connection and handles
/// their sequencing, processing, and appropriate action based on their control code.
///
/// This function accepts a [`ConnectionState`] object and a [`Result`] containing either
/// a [`Bytes`] buffer of the datagram payload or an error.
/// It processes the datagrams based on their sequence number
/// and performs operations such as forwarding data
/// to the client or closing the connection if a [`Close`] datagram is received.
///
/// # Parameters
///
/// * `connection`: A mutable reference to a [`ConnectionState`] object, which represents
///   the current connection state.
/// * `data`: A [`Result`] containing the incoming datagram payload encapsulated in [`Bytes`]
///   or an error of type [`broadcast::error::RecvError`].
///
/// # Returns
/// A `bool` indicating:
/// * `true`: if the connection should be terminated, either due to receiving a `CLOSE`
///   control code from the client or because the sink channel is closed.
/// * `false`: if the connection should remain active.
///
/// # Behaviour
///
/// 1. **Datagram Validation**:
///     * If the datagram's identifier does not match the connection's, it is ignored.
///     * If the received datagram is malformed, it is logged and ignored.
///
/// 2. **Sequencing and Queuing**:
///     * If the datagram's sequence is higher than the expected next sequence, it is
///       queued for future processing (out-of-order reception).
///     * If the datagram is in sequence, it and any subsequent queued datagrams are processed
///       in order until no more in-sequence datagrams remain.
///
/// 3. **Processing and Actions**:
///     * If valid payload data is present in a datagram:
///         * Attempts to forward it to the client.
///         * Any write or flush errors are logged and terminate the function with `true`.
///     * If a [`Close`] control code is received, the client connection is shut down, the
///       termination is logged, and the function returns `true`.
///
/// 4. **Error Handling**:
///     * If a [`broadcast::error::RecvError::Closed`] is encountered, it shuts down the client
///       connection and returns `true`.
///
/// # Example Workflow
/// 1. Receives incoming datagrams from a sink.
/// 2. Validates the datagrams' identifier and integrity.
/// 3. Orders incoming datagrams by their sequence number, queuing out-of-order ones.
/// 4. Forwards valid data to the client.
/// 5. Handles special control codes like [`Close`] by shutting down the connection.
///
/// # Errors
/// * Malformed datagrams are logged and ignored.
///
/// # Notes
/// * Connection's `datagram_queue` and `largest_processed` are used to ensure in-order
///   delivery of datagrams.
///
/// [`Close`]: ControlCode::Close
pub async fn process_sink_read(
  connection: &mut ConnectionState,
  data: Result<Bytes, async_broadcast::RecvError>,
  client_to_sink_push: &async_channel::Sender<Bytes>,
) -> bool {
  let processing_result = process_datagram(connection, data).await;
  if processing_result & 0b01 == 0b01 {
    send_ack(connection, client_to_sink_push).await;
  }
  if processing_result & 0b10 == 0b10 {
    if let Err(e) = connection.client.shutdown().await {
      error!("Failed to shutdown client: {}", e);
    }
    connection.sequence += 1;
    let datagram = create_close_datagram(connection.identifier, connection.sequence);
    if let Err(e) = client_to_sink_push.send(datagram).await {
      error!("Failed to send CLOSE datagram for connection: {}", e);
    }
    return true;
  }
  false
}

async fn process_datagram(
  connection: &mut ConnectionState,
  data: Result<Bytes, async_broadcast::RecvError>,
) -> u8 {
  let mut result = 0b00; // from right: should send ACK, should shut down client connection
  match data {
    Ok(data_buf) => {
      let datagram = match root_as_datagram(&data_buf) {
        Ok(datagram) => datagram,
        Err(e) => {
          error!("Received malformed datagram: {:?}, ignoring", e);
          return result;
        }
      };
      if datagram.identifier() != connection.identifier {
        // Not our datagram, ignore it
        return result;
      }
      let datagram_sequence = datagram.sequence();
      debug!(
        "Received {} datagram with seq: {} and {} bytes of data",
        format!("{:?}", datagram.code()).to_uppercase(),
        datagram_sequence,
        datagram.data().map_or(0, |d| d.len())
      );
      trace!(target: HUGE_DATA_TARGET, "Datagrams in queue: {:?}", connection.datagram_queue);
      if datagram_sequence >= (connection.largest_processed + 2) {
        trace!(
          "Received datagram out of order, seq: {}, largest sent: {}",
          datagram_sequence, connection.largest_processed
        );
        connection.datagram_queue.insert(datagram_sequence, data_buf);
        return result;
      } else if datagram_sequence == (connection.largest_processed + 1) {
        trace!(
          "Received datagram in order, seq: {}, largest sent: {}",
          datagram_sequence, connection.largest_processed
        );
        connection.datagram_queue.insert(datagram_sequence, data_buf);
        result |= evaluate_datagram_queue(connection).await;
      } else {
        debug!("Received a datagram that was already processed before, ignoring");
      }
      result
    }
    Err(async_broadcast::RecvError::Closed) => {
      if let Err(e) = connection.client.shutdown().await {
        error!("Failed to shutdown client: {}", e);
      }
      result | 0b10
    }
    Err(_) => result,
  }
}

async fn evaluate_datagram_queue(connection: &mut ConnectionState) -> u8 {
  let mut result = 0b00;
  while let Some(datagram_buf) =
    connection.datagram_queue.remove(&(connection.largest_processed + 1))
  {
    connection.largest_processed += 1;
    let Ok(datagram) = root_as_datagram(&datagram_buf) else {
      continue;
    };
    match datagram.code() {
      ControlCode::Close => {
        result |= 0b10;
        break;
      }
      ControlCode::Ack => {
        if let Some(data) = datagram.data() {
          let Ok(sequence_to_ack) = data.bytes().try_into() else {
            error!("Received ACK datagram with invalid sequence number!");
            result |= 0b10;
            break;
          };
          connection.largest_acked = connection.sequence.min(u64::from_be_bytes(sequence_to_ack));
        }
      }
      ControlCode::Data => {
        if let Some(data) = datagram.data() {
          trace!("Sending data to client, size {}", data.len());
          match connection.client.write_all(data.bytes()).await {
            Ok(()) => {
              trace!("Sent {} bytes to client", data.len());
              result |= 0b01;
            }
            Err(e) => {
              error!("Failed to write data to client: {}", e);
              result |= 0b10;
              break;
            }
          };
        }
      }
      _ => {} // We don't care about other types (only Initial at the time of writing)
    }
  }
  if let Err(e) = connection.client.flush().await {
    error!("Failed to flush client data after writing data: {}", e);
  }
  result
}

async fn send_ack(
  connection: &mut ConnectionState,
  client_to_sink_push: &async_channel::Sender<Bytes>,
) {
  connection.sequence += 1;
  connection.largest_acked += 1;
  let ack =
    create_ack_datagram(connection.identifier, connection.sequence, connection.largest_processed);
  debug!("Sending ACK: {:?}", root_as_datagram(&ack).unwrap());
  if client_to_sink_push.send(ack).await.is_err() {
    error!("Failed to send ACK datagram, ending connection");
  }
}

/// Handles reading and processing data to/from the client and sink(s).
///
/// # Parameters
///
/// * `connection`: The current [`ConnectionState`] that holds the client's state and identifier.
/// * `channel_map`: A [`ChannelMap`] containing the separate channel for this client.
/// * `client_to_sink_push`:
///   An [`async_channel::Sender`] for sending data received from the client to the sink(s).
/// * `cancel`:
///   A [`CancellationToken`] used to signal loop termination due to the app shutting down.
///
/// # Behaviour
///
/// The function runs in a loop that listens for three primary conditions:
/// 1. **Cancellation signal**: If `cancel.cancelled()` is triggered, the connection is closed
///    gracefully by shutting down the client, sending a [`Close`] datagram, and exiting the loop.
/// 2. **Incoming data from the sink**:
///    Receives data from the connection's dedicated channel via `recv_direct()`,
///    then uses [`process_sink_read()`] to handle and forward the data to the client.
///    The processing might signal connection termination, which breaks the loop.
/// 3. **Incoming data from the client**:
///    Uses [`peek()`](TcpStream::peek) to check for available data without blocking the STC
///    channel. If data is available, reads it with [`try_read()`](TcpStream::try_read)
///    and processes it with [`handle_client_read()`].
///    If the connection is blocked (too many unacknowledged datagrams), sleeps instead of reading.
///    The processing might signal connection termination, which breaks the loop.
///
/// [`Close`]: ControlCode::Close
#[instrument(skip_all, fields(connection_id = %connection.identifier, client_address = connection.client_address))]
pub async fn connection_loop(
  mut connection: ConnectionState,
  channel_map: ChannelMap,
  client_to_sink_push: async_channel::Sender<Bytes>,
  cancel: CancellationToken,
) {
  let current_span = Span::current();
  debug!("Connection loop starting");
  let (mut sink_to_client_pull, _guard) = channel_get_or_insert_with_guard(
    channel_map.clone(),
    Identifier::Client(connection.identifier),
  );
  let mut tcp_buf = BytesMut::zeroed(CONNECTION_BUFFER_SIZE);
  let mut unprocessed_bytes: usize = 0;
  loop {
    tcp_buf.resize(CONNECTION_BUFFER_SIZE, 0);
    let connection_blocked =
      connection.largest_acked < connection.sequence.saturating_sub(ACK_THRESHOLD);
    if connection_blocked {
      trace!(
        sequence = connection.sequence,
        largest_processed = connection.largest_processed,
        largest_acked = connection.largest_acked,
        buffer_fullness = unprocessed_bytes,
        "Connection blocked."
      );
    }
    tokio::select! {
      biased;
      () = cancel.cancelled() => {
        if let Err(e) = connection.client.shutdown().await {
          error!("Failed to shutdown client after server shutdown: {}", e);
        }
        send_close_datagram(&mut connection, &client_to_sink_push).await;
        break;
      }
      data = sink_to_client_pull.recv_direct() => {
        let sink_read_start = Instant::now();
        let connection_terminating =
          process_sink_read(&mut connection, data, &client_to_sink_push).instrument(current_span.clone()).await;
        trace!(target: METRICS_TARGET, duration = ?sink_read_start.elapsed(), "Finished processing sink read");
        if connection_terminating {
          break;
        }
      }
      bytes_read = async {
        if connection_blocked {
          sleep(Duration::from_secs(3)).await;
          connection.client.read(&mut tcp_buf[unprocessed_bytes..unprocessed_bytes.saturating_add(4)]).await
        } else {
          connection.client.read(&mut tcp_buf[unprocessed_bytes..]).await
        }
      } => {
        if matches!(bytes_read, Ok(0) | Err(_)) || !connection_blocked {
          connection.sequence += 1;
          let client_read_start = Instant::now();
          let connection_terminating = handle_client_read(
            connection.identifier,
            connection.sequence,
            client_to_sink_push.clone(),
            bytes_read.map(|b| b + unprocessed_bytes),
            &mut tcp_buf,
          ).instrument(current_span.clone()).await;
          trace!(target: METRICS_TARGET, duration = ?client_read_start.elapsed(), "Finished processing client read");
          if connection_terminating {
            break;
          }
          unprocessed_bytes = 0;
        } else {
          unprocessed_bytes = unprocessed_bytes.saturating_add(4);
          if unprocessed_bytes == tcp_buf.len() {
            debug!("Buffer overrun, closing connection");
            if let Err(e) = connection.client.shutdown().await {
              error!("Failed to shutdown connection. {e}");
            }
            send_close_datagram(&mut connection, &client_to_sink_push).await;
            break;
          }
        }
      }
    }
  }
  debug!("Connection {} loop ending", connection.identifier);
}

async fn send_close_datagram(
  connection: &mut ConnectionState,
  client_to_sink_push: &async_channel::Sender<Bytes>,
) {
  trace!("Sending CLOSE datagram");
  connection.sequence += 1;
  let datagram = create_close_datagram(connection.identifier, connection.sequence);
  if let Err(e) = client_to_sink_push.send(datagram).await {
    error!("Failed to send CLOSE datagram for connection: {}", e);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::protocol_utils::{create_ack_datagram, create_data_datagram, create_initial_datagram};
  use crate::schema_generated::serial_multiplexer::{ControlCode, root_as_datagram};
  use crate::test_utils::{run_echo, setup_tracing};
  use crate::try_write::Sink;
  use crate::utils::create_upstream_listener;
  use papaya::HashMap;
  use std::io::ErrorKind;
  use std::net::SocketAddr;
  use std::sync::Arc;
  use std::time::Duration;
  use tokio::io::AsyncReadExt;
  use tokio::net::{TcpListener, TcpStream};
  use tokio::sync::mpsc;
  use tokio::task::JoinHandle;
  use tokio::time::timeout;

  #[tokio::test]
  async fn test_handle_sink_read_double_with_rubbish() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::new();
    let mut compression_buf = BytesMut::zeroed(SINK_COMPRESSION_BUFFER_SIZE);
    let channel_map = Arc::new(HashMap::new());
    let (sink_to_client_push, mut sink_to_client_pull) = async_broadcast::broadcast(10);
    channel_map.pin().insert(
      Identifier::Client(5),
      (sink_to_client_push.clone(), sink_to_client_pull.clone().deactivate()),
    );
    sink_buf.extend_from_slice(&[1, 2]);
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let datagram1 = create_data_datagram(5, 1, &[4, 5, 6]);
    let n = zstd_safe::compress(compression_buf.as_mut(), &datagram1, 9).unwrap();
    sink_buf.extend_from_slice(&(n as u16).to_be_bytes());
    sink_buf.extend_from_slice(&compression_buf[..n]);
    sink_buf.extend_from_slice(&[7, 8, 9]);
    let mut buffer_size = sink_buf.len();

    let unprocessed_bytes =
      handle_sink_read(&channel_map, sink_buf.len(), &mut sink_buf, &mut compression_buf)
        .await
        .unwrap();
    assert_eq!(sink_buf.len(), buffer_size);
    assert_eq!(unprocessed_bytes, 3);
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), datagram1);
    assert_eq!(sink_buf[3..], BytesMut::zeroed(buffer_size - unprocessed_bytes));
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));
    sink_buf.resize(unprocessed_bytes, 0);

    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let datagram2 = create_close_datagram(5, 2);
    let n = zstd_safe::compress(compression_buf.as_mut(), &datagram2, 9).unwrap();
    sink_buf.extend_from_slice(&(n as u16).to_be_bytes());
    sink_buf.extend_from_slice(&compression_buf[..n]);
    sink_buf.extend_from_slice(&[11, 12, 13, 14, 15]);
    buffer_size = sink_buf.len();
    let unprocessed_bytes =
      handle_sink_read(&channel_map, sink_buf.len(), &mut sink_buf, &mut compression_buf)
        .await
        .unwrap();
    assert_eq!(sink_buf.len(), buffer_size);
    assert_eq!(unprocessed_bytes, 5);
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), datagram2);
    assert_eq!(sink_buf[5..], BytesMut::zeroed(buffer_size - unprocessed_bytes));
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));
  }

  #[tokio::test]
  async fn test_handle_sink_read_multiple_datagrams() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::new();
    let mut compression_buf = BytesMut::zeroed(SINK_COMPRESSION_BUFFER_SIZE);
    let channel_map = Arc::new(HashMap::new());
    let (sink_to_client_push, mut sink_to_client_pull) = async_broadcast::broadcast(10);
    channel_map.pin().insert(
      Identifier::Client(5),
      (sink_to_client_push.clone(), sink_to_client_pull.clone().deactivate()),
    );

    let datagram1 = create_data_datagram(5, 1, b"datagram1");
    let datagram2 = create_data_datagram(5, 2, b"datagram2");
    let datagram3 = create_data_datagram(5, 3, b"datagram3");
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let n = zstd_safe::compress(compression_buf.as_mut(), &datagram1, 9).unwrap();
    sink_buf.extend_from_slice(&(n as u16).to_be_bytes());
    sink_buf.extend_from_slice(&compression_buf[..n]);
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let n = zstd_safe::compress(compression_buf.as_mut(), &datagram2, 9).unwrap();
    sink_buf.extend_from_slice(&(n as u16).to_be_bytes());
    sink_buf.extend_from_slice(&compression_buf[..n]);
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let n = zstd_safe::compress(compression_buf.as_mut(), &datagram3, 9).unwrap();
    sink_buf.extend_from_slice(&(n as u16).to_be_bytes());
    sink_buf.extend_from_slice(&compression_buf[..n]);

    let buffer_size = sink_buf.len();
    let unprocessed_bytes =
      handle_sink_read(&channel_map, buffer_size, &mut sink_buf, &mut compression_buf)
        .await
        .unwrap();
    assert_eq!(buffer_size, sink_buf.len());
    assert_eq!(unprocessed_bytes, 0);
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), datagram1);
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), datagram2);
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), datagram3);
    assert_eq!(sink_buf, BytesMut::zeroed(buffer_size));
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));
  }

  #[tokio::test]
  async fn test_handle_sink_read_large() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::new();
    let mut compression_buf = BytesMut::zeroed(SINK_COMPRESSION_BUFFER_SIZE);
    let channel_map = Arc::new(HashMap::new());
    let (sink_to_client_push, mut sink_to_client_pull) = async_broadcast::broadcast(10);
    channel_map.pin().insert(
      Identifier::Client(0),
      (sink_to_client_push.clone(), sink_to_client_pull.clone().deactivate()),
    );

    let datagram_data = BytesMut::from(vec![65u8; CONNECTION_BUFFER_SIZE].as_slice());
    let datagram = create_data_datagram(0, 1, &datagram_data);
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let n = zstd_safe::compress(compression_buf.as_mut(), &datagram, 9).unwrap();
    sink_buf.extend_from_slice(&(n as u16).to_be_bytes());
    sink_buf.extend_from_slice(&compression_buf[..n]);

    let buffer_size = sink_buf.len();
    let unprocessed_bytes =
      handle_sink_read(&channel_map, buffer_size, &mut sink_buf, &mut compression_buf)
        .await
        .unwrap();
    assert_eq!(buffer_size, sink_buf.len());
    assert_eq!(unprocessed_bytes, 0);
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), datagram);
    assert_eq!(sink_buf, BytesMut::zeroed(buffer_size));
    assert_eq!(&compression_buf, &BytesMut::zeroed(compression_buf.len()));
  }

  #[tokio::test]
  async fn test_handle_sink_read_size_not_read() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::new();
    let mut compression_buf = BytesMut::zeroed(SINK_COMPRESSION_BUFFER_SIZE);
    let channel_map = Arc::new(HashMap::new());
    let (sink_to_client_push, mut sink_to_client_pull) = async_broadcast::broadcast(10);
    channel_map.pin().insert(
      Identifier::ClientInitiator,
      (sink_to_client_push.clone(), sink_to_client_pull.clone().deactivate()),
    );
    sink_buf.extend_from_slice(&[1, 2]); // invalid data to offset HEADER
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let buffer_size = sink_buf.len();

    let unprocessed_bytes =
      handle_sink_read(&channel_map, buffer_size, &mut sink_buf, &mut compression_buf)
        .await
        .unwrap();
    assert_eq!(buffer_size, sink_buf.len());
    assert!(sink_to_client_pull.try_recv().is_err());
    assert_eq!(unprocessed_bytes, buffer_size - 2); // -2 because garbage was taken out
    assert_ne!(sink_buf, BytesMut::zeroed(sink_buf.len()));
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));
  }

  #[tokio::test]
  async fn test_handle_sink_read_data_partially_read() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::new();
    let mut compression_buf = BytesMut::zeroed(SINK_COMPRESSION_BUFFER_SIZE);
    let channel_map = Arc::new(HashMap::new());
    let (sink_to_client_push, mut sink_to_client_pull) = async_broadcast::broadcast(10);
    channel_map.pin().insert(
      Identifier::ClientInitiator,
      (sink_to_client_push.clone(), sink_to_client_pull.clone().deactivate()),
    );

    sink_buf.extend_from_slice(&[1, 2]); // invalid data to offset HEADER
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    sink_buf.extend_from_slice(&5u16.to_be_bytes());
    sink_buf.extend_from_slice(&[1; 3]);
    let buffer_size = sink_buf.len();

    let unprocessed_bytes =
      handle_sink_read(&channel_map, buffer_size, &mut sink_buf, &mut compression_buf)
        .await
        .unwrap();
    assert_eq!(buffer_size, sink_buf.len());
    assert!(sink_to_client_pull.try_recv().is_err());
    assert_eq!(unprocessed_bytes, buffer_size - 2); // -2 because garbage was taken out
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));
  }

  #[tokio::test]
  async fn test_handle_sink_read_data_garbage_cleanup() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::new();
    let mut compression_buf = BytesMut::zeroed(200);
    let channel_map = Arc::new(HashMap::new());
    let (sink_to_client_push, mut sink_to_client_pull) = async_broadcast::broadcast(10);
    channel_map.pin().insert(
      Identifier::Client(0),
      (sink_to_client_push.clone(), sink_to_client_pull.clone().deactivate()),
    );

    sink_buf.extend_from_slice(&[70; 5]); // garbage
    let datagram1 = create_data_datagram(0, 1, b"datagram1");
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let n = zstd_safe::compress(compression_buf.as_mut(), &datagram1, 9).unwrap();
    sink_buf.extend_from_slice(&(n as u16).to_be_bytes());
    sink_buf.extend_from_slice(&compression_buf[..n]);
    sink_buf.extend_from_slice(&[65; 5]); // more garbage
    sink_buf.extend_from_slice(&DATAGRAM_HEADER[..7]);

    let buffer_size = sink_buf.len();
    let unprocessed_bytes =
      handle_sink_read(&channel_map, buffer_size, &mut sink_buf, &mut compression_buf)
        .await
        .unwrap();
    assert_eq!(buffer_size, sink_buf.len());
    assert!(sink_to_client_pull.try_recv().is_ok());
    assert_eq!(unprocessed_bytes, 7);
    assert_eq!(&sink_buf[..7], &DATAGRAM_HEADER[..7]);
    assert_eq!(sink_buf[7..], BytesMut::zeroed(buffer_size - 7));
  }

  #[tokio::test]
  async fn test_handle_sink_read_data_partial_header() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::new();
    let mut compression_buf = BytesMut::zeroed(200);
    let channel_map = Arc::new(HashMap::new());
    let (sink_to_client_push, mut sink_to_client_pull) = async_broadcast::broadcast(10);
    channel_map.pin().insert(
      Identifier::Client(0),
      (sink_to_client_push.clone(), sink_to_client_pull.clone().deactivate()),
    );

    sink_buf.extend_from_slice(&DATAGRAM_HEADER[..7]);

    let buffer_size = sink_buf.len();
    let unprocessed_bytes =
      handle_sink_read(&channel_map, buffer_size, &mut sink_buf, &mut compression_buf)
        .await
        .unwrap();
    assert_eq!(buffer_size, sink_buf.len());
    assert!(sink_to_client_pull.try_recv().is_err());
    assert_eq!(unprocessed_bytes, 7);
    assert_eq!(&sink_buf[..], &DATAGRAM_HEADER[..7]);
  }

  #[tokio::test]
  async fn test_handle_sink_read_data_garbage_filled() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::zeroed(SINK_BUFFER_SIZE);
    let mut compression_buf = BytesMut::zeroed(SINK_COMPRESSION_BUFFER_SIZE);
    let channel_map = Arc::new(HashMap::new());

    sink_buf.fill(80);
    let buffer_size = sink_buf.len();
    let unprocessed_bytes =
      handle_sink_read(&channel_map, buffer_size, &mut sink_buf, &mut compression_buf)
        .await
        .unwrap();
    assert_eq!(buffer_size, sink_buf.len());
    assert_eq!(unprocessed_bytes, 7);
    assert_eq!(sink_buf[..7], Bytes::from_static(&[80; 7]));
    assert_eq!(sink_buf[8..], BytesMut::zeroed(buffer_size - 8)[..]);
  }

  #[tokio::test]
  async fn test_sink_loop_read_sink() {
    setup_tracing().await;
    let mut sink_buf_write = BytesMut::zeroed(SINK_BUFFER_SIZE);
    let (sink_a, mut sink_b) = tokio::io::duplex(4096);
    let channel_map = Arc::new(HashMap::new());
    let (sink_to_client_push, mut sink_to_client_pull) = async_broadcast::broadcast(10);
    channel_map.pin().insert(
      Identifier::ClientInitiator,
      (sink_to_client_push.clone(), sink_to_client_pull.clone().deactivate()),
    );
    let (_client_to_sink_push, client_to_sink_pull) = async_channel::bounded(256);
    let cancel = CancellationToken::new();
    let _sink_loop_handle =
      tokio::spawn(sink_loop(sink_a, channel_map.clone(), client_to_sink_pull, cancel.clone()));

    info!("Sending initial datagram");
    let initial_data = "test data";
    let initial_datagram = create_initial_datagram(1, initial_data);
    handle_sink_write(&mut sink_b, initial_datagram, &mut sink_buf_write).await.unwrap();
    let datagram_bytes =
      timeout(Duration::from_secs(1), sink_to_client_pull.recv()).await.unwrap().unwrap();
    let initial_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(initial_datagram.identifier(), 1);
    assert_eq!(initial_datagram.code(), ControlCode::Initial);
    assert_eq!(initial_datagram.data().unwrap().bytes(), initial_data.as_bytes());
    assert_eq!(sink_buf_write, BytesMut::zeroed(sink_buf_write.len()));

    // write some rubbish between datagrams
    info!("Sending rubbish data");
    let data = BytesMut::from("rubbish");
    sink_b.write_all(&data).await.unwrap();
    sink_b.flush().await.unwrap();

    info!("Sending ACK datagram");
    let (sink_to_client_push, mut sink_to_client_pull) = async_broadcast::broadcast(10);
    channel_map.pin().insert(
      Identifier::Client(2),
      (sink_to_client_push.clone(), sink_to_client_pull.clone().deactivate()),
    );
    let ack_datagram = create_ack_datagram(2, 0, 0);
    handle_sink_write(&mut sink_b, ack_datagram, &mut sink_buf_write).await.unwrap();
    let datagram_bytes = sink_to_client_pull.recv().await.unwrap();
    let ack_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(ack_datagram.identifier(), 2);
    assert_eq!(ack_datagram.code(), ControlCode::Ack);
    assert_eq!(ack_datagram.data().unwrap().bytes(), 0u64.to_be_bytes());
    assert_eq!(sink_buf_write, BytesMut::zeroed(sink_buf_write.len()));

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

    let sizes = [1, 5, 100, 500, 1000, 5000, 10000, 15000, CONNECTION_BUFFER_SIZE];
    for (i, data_size) in (0..).zip(sizes.into_iter()) {
      let (sink_to_client_push, mut sink_to_client_pull) = async_broadcast::broadcast(10);
      channel_map.pin().insert(
        Identifier::Client(i),
        (sink_to_client_push.clone(), sink_to_client_pull.clone().deactivate()),
      );
      let mut data = BytesMut::new();
      data.resize(data_size, 100u8);
      let data_datagram = create_data_datagram(i, 1, &data);
      handle_sink_write(&mut sink_b, data_datagram, &mut sink_buf_write).await.unwrap();
      let datagram_bytes = sink_to_client_pull.recv().await.unwrap();
      let data_datagram = root_as_datagram(&datagram_bytes).unwrap();
      assert_eq!(data_datagram.identifier(), i);
      assert_eq!(data_datagram.code(), ControlCode::Data);
      assert_eq!(data_datagram.data().unwrap().bytes(), data);
      assert_eq!(sink_buf_write, BytesMut::zeroed(sink_buf_write.len()));
    }
  }

  #[tokio::test]
  async fn test_sink_loop_write() {
    setup_tracing().await;
    let (sink_a, sink_b) = tokio::io::duplex(4096);
    let channel_map_write = Arc::new(HashMap::new());
    let (sink_to_client_push_write, sink_to_client_pull_write) = async_broadcast::broadcast(256);
    channel_map_write.pin().insert(
      Identifier::Client(3),
      (sink_to_client_push_write.clone(), sink_to_client_pull_write.deactivate()),
    );
    let (sink_to_client_push_read, mut sink_to_client_pull_read) = async_broadcast::broadcast(256);
    let channel_map_read = Arc::new(HashMap::new());
    let (client_to_sink_push_write, client_to_sink_pull_write) = async_channel::bounded(256);
    channel_map_read.pin().insert(
      Identifier::Client(3),
      (sink_to_client_push_read.clone(), sink_to_client_pull_read.clone().deactivate()),
    );
    let (_client_to_sink_push_read, client_to_sink_pull_read) = async_channel::bounded(256);
    let cancel = CancellationToken::new();
    let _sink_loop_write_handle =
      tokio::spawn(sink_loop(sink_a, channel_map_write, client_to_sink_pull_write, cancel.clone()));
    let _sink_loop_read_handle =
      tokio::spawn(sink_loop(sink_b, channel_map_read, client_to_sink_pull_read, cancel.clone()));

    let sizes = [1, 5, 100, 500, 1000, 5000, 10000, 15000, CONNECTION_BUFFER_SIZE];
    for (i, data_size) in sizes.into_iter().enumerate() {
      let mut data = BytesMut::new();
      data.resize(data_size, 100u8);
      let data_copy = Bytes::copy_from_slice(&data);
      data.resize(CONNECTION_BUFFER_SIZE, 50u8);
      handle_client_read(3, i as u64, client_to_sink_push_write.clone(), Ok(data_size), &mut data)
        .await;
      let datagram_bytes =
        timeout(Duration::from_secs(1), sink_to_client_pull_read.recv()).await.unwrap().unwrap();
      let initial_datagram = root_as_datagram(&datagram_bytes).unwrap();
      assert_eq!(initial_datagram.identifier(), 3);
      assert_eq!(initial_datagram.code(), ControlCode::Data);
      assert_eq!(initial_datagram.sequence(), i as u64);
      assert_eq!(initial_datagram.data().unwrap().bytes(), &data_copy);
    }
  }

  #[cfg(windows)]
  #[tokio::test]
  async fn test_write_to_sink_full_sink_named_pipe() {
    use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};
    setup_tracing().await;
    let pipe_path = "\\\\.\\pipe\\test_pipe_multiplexer_sink_loop";
    let pipe_server = ServerOptions::new().first_pipe_instance(true).create(pipe_path).unwrap();
    let pipe_server_handle = tokio::spawn(async move {
      pipe_server.connect().await.unwrap();
      pipe_server
    });
    let sink_a = ClientOptions::new().write(true).read(true).open(pipe_path).unwrap();
    let _sink_b = pipe_server_handle.await.unwrap();

    test_write_to_sink_full_sink(sink_a).await;
  }

  #[cfg(unix)]
  #[tokio::test]
  async fn test_write_to_sink_full_sink_unix_socket() {
    use tokio::net::{UnixListener, UnixStream};
    setup_tracing().await;
    let uds_path = "test_write_to_sink_full_sink_unix_socket.sock";
    let uds_server = UnixListener::bind(uds_path).unwrap();
    let pipe_server_handle = tokio::spawn(async move { uds_server.accept().await.unwrap().0 });
    let sink_a = UnixStream::connect(uds_path).await.unwrap();
    let _sink_b = pipe_server_handle.await.unwrap();
    test_write_to_sink_full_sink(sink_a).await;
  }

  async fn test_write_to_sink_full_sink(mut sink_a: impl Sink) {
    loop {
      if let Err(e) = sink_a.try_write(&BytesMut::zeroed(2usize.pow(20))).await
        && e.kind() == ErrorKind::WouldBlock
      {
        break;
      }
    }
    let mut data = BytesMut::from_iter(0..255u8);
    let result = write_to_sink(&mut sink_a, &mut data).await.unwrap();
    assert_eq!(result, data.len());
  }

  #[tokio::test]
  async fn test_write_to_sink_closed() {
    let (mut sink_a, _sink_b) = tokio::io::duplex(128);
    sink_a.shutdown().await.unwrap();
    let mut data = BytesMut::from("test_data");
    assert!(write_to_sink(&mut sink_a, &mut data).await.is_err());
  }

  #[tokio::test]
  async fn test_connection_loop_sink_read_write() {
    setup_tracing().await;
    let channel_map = Arc::new(HashMap::new());
    let (sink_to_client_push, sink_to_client_pull) = async_broadcast::broadcast(10);
    channel_map.pin().insert(
      Identifier::Client(0),
      (sink_to_client_push.clone(), sink_to_client_pull.deactivate()),
    );
    let (client_to_sink_push, client_to_sink_pull) = async_channel::bounded(256);
    let cancel = CancellationToken::new();
    let data = Bytes::from_static(b"test data");

    let (address_sender, mut address_receiver) = mpsc::channel::<SocketAddr>(1);
    let _handle = tokio::spawn(async move {
      let listener = create_upstream_listener("127.0.0.1:0").await.unwrap();
      let address = listener.local_addr().unwrap();
      address_sender.send(address).await.unwrap();
      let (client, _) = listener.accept().await.unwrap();
      let identifier = 0;

      let connection = ConnectionState::new(identifier, client);
      tokio::spawn(connection_loop(
        connection,
        channel_map,
        client_to_sink_push.clone(),
        cancel.clone(),
      ));
    });

    let server_address = address_receiver.recv().await.unwrap();
    let mut client = TcpStream::connect(server_address).await.unwrap();
    let data_datagram = create_data_datagram(0, 1, &data);
    sink_to_client_push.broadcast_direct(data_datagram).await.unwrap();
    let mut client_buf = BytesMut::zeroed(2048);
    let n = client.read(&mut client_buf).await.unwrap();
    assert_eq!(n, data.len());
    assert_eq!(data, client_buf[..n]);
    let ack_bytes = client_to_sink_pull.recv().await.unwrap();
    let ack = root_as_datagram(&ack_bytes).unwrap();
    assert_eq!(ControlCode::Ack, ack.code());
    assert_eq!(1, u64::from_be_bytes(ack.data().unwrap().bytes().try_into().unwrap()));
    client.write_all(&data).await.unwrap();
    let mut data_start = 0;
    let mut seq = 2;
    while data_start < data.len() {
      let data_bytes = client_to_sink_pull.recv().await.unwrap();
      let data_datagram = root_as_datagram(&data_bytes).unwrap();
      assert_eq!(ControlCode::Data, data_datagram.code());
      assert_eq!(seq, data_datagram.sequence() as usize);
      let datagram_data = data_datagram.data().unwrap().bytes();
      assert_eq!(&data[data_start..data_start + datagram_data.len()], datagram_data);
      data_start += datagram_data.len();
      seq += 1;
    }
    let close_datagram = create_close_datagram(0, 2);
    sink_to_client_push.broadcast_direct(close_datagram).await.unwrap();
    let n = client.read(&mut client_buf).await.unwrap();
    assert_eq!(0, n);
  }

  #[tokio::test(start_paused = true)]
  async fn test_connection_loop_blocked_timeout() {
    setup_tracing().await;
    let identifier = 0;
    let channel_map = Arc::new(HashMap::new());
    let (sink_to_client_push, sink_to_client_pull) = async_broadcast::broadcast(10);
    channel_map.pin().insert(
      Identifier::ClientInitiator,
      (sink_to_client_push.clone(), sink_to_client_pull.deactivate()),
    );
    let (client_to_sink_push, client_to_sink_pull) = async_channel::bounded(256);
    let cancel = CancellationToken::new();

    let (address_sender, mut address_receiver) = mpsc::channel::<SocketAddr>(1);
    let (loop_task_sender, mut loop_task_receiver) = mpsc::channel::<JoinHandle<()>>(1);
    let _handle = tokio::spawn(async move {
      let listener = create_upstream_listener("127.0.0.1:0").await.unwrap();
      let address = listener.local_addr().unwrap();
      address_sender.send(address).await.unwrap();
      let (client, _) = listener.accept().await.unwrap();

      let mut connection = ConnectionState::new(identifier, client);
      connection.sequence += ACK_THRESHOLD + 1;
      loop_task_sender
        .send(tokio::spawn(connection_loop(
          connection,
          channel_map,
          client_to_sink_push.clone(),
          cancel.clone(),
        )))
        .await
        .unwrap();
    });

    let server_address = address_receiver.recv().await.unwrap();
    let mut client = TcpStream::connect(server_address).await.unwrap();
    let loop_task = loop_task_receiver.recv().await.unwrap();
    client.shutdown().await.unwrap();
    let close_data = client_to_sink_pull.recv().await.unwrap();
    let close_datagram = root_as_datagram(&close_data).unwrap();
    assert_eq!(close_datagram.code(), ControlCode::Close);
    assert_eq!(close_datagram.identifier(), identifier);
    timeout(Duration::from_secs(1), _handle).await.unwrap().unwrap();
    timeout(Duration::from_secs(1), loop_task).await.unwrap().unwrap();
  }

  #[tokio::test]
  async fn test_connection_loop_cancel() {
    setup_tracing().await;
    let channel_map = Arc::new(HashMap::new());
    let (client_to_sink_push, client_to_sink_pull) = async_channel::bounded(256);
    let cancel = CancellationToken::new();

    let (address_sender, mut address_receiver) = mpsc::channel::<SocketAddr>(1);
    let handle = tokio::spawn({
      let cancel = cancel.clone();
      async move {
        let listener = create_upstream_listener("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        address_sender.send(address).await.unwrap();
        let (client, _) = listener.accept().await.unwrap();
        let identifier = 0;

        let connection = ConnectionState::new(identifier, client);
        connection_loop(connection, channel_map, client_to_sink_push, cancel).await;
      }
    });

    let server_address = address_receiver.recv().await.unwrap();
    let mut client = TcpStream::connect(server_address).await.unwrap();
    cancel.cancel();
    let mut client_buf = BytesMut::zeroed(2048);
    let n = client.read(&mut client_buf).await.unwrap();
    assert_eq!(n, 0);
    let sink_read = client_to_sink_pull.recv().await.unwrap();
    let close_datagram = root_as_datagram(&sink_read).unwrap();
    assert_eq!(close_datagram.identifier(), 0);
    assert_eq!(close_datagram.code(), ControlCode::Close);
    assert_eq!(close_datagram.data().unwrap().bytes(), &[]);
    assert!(timeout(Duration::from_secs(1), handle).await.is_ok());
  }

  #[tokio::test(start_paused = true)]
  async fn test_connection_loop_blocked_buffer_overrun() {
    setup_tracing().await;
    let identifier = 0;
    let channel_map = Arc::new(HashMap::new());
    let (client_to_sink_push, client_to_sink_pull) = async_channel::bounded(256);
    let cancel = CancellationToken::new();

    let (address_sender, mut address_receiver) = mpsc::channel::<SocketAddr>(1);
    let (loop_task_sender, mut loop_task_receiver) = mpsc::channel::<JoinHandle<()>>(1);
    let _handle = tokio::spawn(async move {
      let listener = create_upstream_listener("127.0.0.1:0").await.unwrap();
      let address = listener.local_addr().unwrap();
      address_sender.send(address).await.unwrap();
      let (client, _) = listener.accept().await.unwrap();

      let mut connection = ConnectionState::new(identifier, client);
      connection.sequence += ACK_THRESHOLD + 1;
      loop_task_sender
        .send(tokio::spawn(connection_loop(
          connection,
          channel_map,
          client_to_sink_push.clone(),
          cancel.clone(),
        )))
        .await
        .unwrap();
    });

    let server_address = address_receiver.recv().await.unwrap();
    let mut client = TcpStream::connect(server_address).await.unwrap();
    let loop_task = loop_task_receiver.recv().await.unwrap();
    let client_data = BytesMut::from(vec![90u8; CONNECTION_BUFFER_SIZE - 4].as_slice());
    client.write_all(&client_data).await.unwrap();
    assert!(!loop_task.is_finished());
    client.write_all(&client_data).await.unwrap();
    let close_data = client_to_sink_pull.recv().await.unwrap();
    let close_datagram = root_as_datagram(&close_data).unwrap();
    assert_eq!(close_datagram.code(), ControlCode::Close);
    assert_eq!(close_datagram.identifier(), identifier);
    timeout(Duration::from_secs(1), _handle).await.unwrap().unwrap();
    timeout(Duration::from_secs(1), loop_task).await.unwrap().unwrap();
  }

  #[tokio::test]
  async fn test_handle_client_read_smoke() {
    setup_tracing().await;
    let contents = "test data";
    let mut bytes = BytesMut::from(contents);
    let (client_to_sink_push, client_to_sink_pull) = async_channel::bounded(1);

    let identifier = 123;
    let bytes_read = Ok(bytes.len());

    assert!(!handle_client_read(identifier, 1, client_to_sink_push, bytes_read, &mut bytes).await);
    let sink_read = client_to_sink_pull.recv().await.unwrap();
    let datagram = root_as_datagram(&sink_read).expect("Received datagram should be valid");
    assert_eq!(datagram.identifier(), identifier);
    assert_eq!(datagram.code(), ControlCode::Data);
    assert_eq!(datagram.data().unwrap().bytes(), contents.as_bytes());
    assert_eq!(bytes, BytesMut::zeroed(bytes.len()));
  }

  #[tokio::test]
  async fn test_handle_client_read_client_disconnect() {
    setup_tracing().await;
    let (client_to_sink_push, _client_to_sink_pull) = async_channel::bounded(1);
    let identifier = 123;
    let bytes_read = Ok(0);

    assert!(
      handle_client_read(identifier, 1, client_to_sink_push, bytes_read, &mut BytesMut::new())
        .await
    );
  }

  #[tokio::test]
  async fn test_handle_client_read_client_error() {
    setup_tracing().await;
    let (client_to_sink_push, _client_to_sink_pull) = async_channel::bounded(1);
    let identifier = 123;
    let bytes_read = Err(std::io::Error::other("test error"));

    assert!(
      handle_client_read(identifier, 2, client_to_sink_push, bytes_read, &mut BytesMut::new())
        .await
    );
  }

  #[tokio::test]
  async fn test_process_sink_read_smoke() {
    setup_tracing().await;
    let (client_to_sink_push, _client_to_sink_pull) = async_channel::bounded(256);
    let identifier = 123;
    let data = BytesMut::from("test data");
    let datagram = create_data_datagram(identifier, 1, &data);

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
    let client = TcpStream::connect(server_address).await.unwrap();
    let mut connection = ConnectionState::new(identifier, client);
    assert!(!process_sink_read(&mut connection, Ok(datagram), &client_to_sink_push).await);
    handle.await.unwrap();
  }

  #[tokio::test]
  async fn test_process_sink_read_out_of_order() {
    setup_tracing().await;
    let (client_to_sink_push, _client_to_sink_pull) = async_channel::bounded(256);
    let identifier = 123;
    let data1 = BytesMut::from("test data1");
    let data2 = BytesMut::from("test data2");
    let datagram1 = create_data_datagram(identifier, 1, &data1);
    let datagram2 = create_data_datagram(identifier, 2, &data2);

    let (address_sender, mut address_receiver) = mpsc::channel(1);
    let handle = tokio::spawn(async move {
      let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
      let address = listener.local_addr().unwrap();
      address_sender.send(address).await.unwrap();
      let (mut client, _) = listener.accept().await.unwrap();
      let mut client_buf = BytesMut::zeroed(24);
      let mut n = 0;
      while n < data1.len() + data2.len() {
        n += client.read(&mut client_buf[n..]).await.unwrap();
        trace!("Client read: {:?}", client_buf);
      }
      assert_eq!(data1, client_buf[..data1.len()]);
      assert_eq!(data2, client_buf[data1.len()..data1.len() + data2.len()]);
    });

    let server_address = address_receiver.recv().await.unwrap();
    let client = TcpStream::connect(server_address).await.unwrap();
    let mut connection = ConnectionState::new(identifier, client);
    assert!(!process_sink_read(&mut connection, Ok(datagram2), &client_to_sink_push).await);
    assert!(connection.datagram_queue.contains_key(&2));
    assert_eq!(1, connection.datagram_queue.len());
    assert!(!process_sink_read(&mut connection, Ok(datagram1), &client_to_sink_push).await);
    assert!(connection.datagram_queue.is_empty());
    handle.await.unwrap();
  }

  #[tokio::test]
  async fn test_process_sink_read_invalid_datagram() {
    setup_tracing().await;
    let (client_to_sink_push, _client_to_sink_pull) = async_channel::bounded(256);
    let (target_address, _) = run_echo().await;
    let identifier = 123;
    let data = Bytes::from("invalid test data");
    let client = TcpStream::connect(target_address).await.unwrap();
    let mut connection = ConnectionState::new(identifier, client);

    assert!(!process_sink_read(&mut connection, Ok(data), &client_to_sink_push).await);
  }

  #[tokio::test]
  async fn test_process_sink_read_different_identifier() {
    setup_tracing().await;
    let (client_to_sink_push, _client_to_sink_pull) = async_channel::bounded(256);
    let (target_address, _) = run_echo().await;
    let identifier = 123;
    let data = BytesMut::from("test data");
    let datagram = create_data_datagram(identifier, 1, &data);

    let client = TcpStream::connect(target_address).await.unwrap();
    let mut connection = ConnectionState::new(500, client);
    assert!(!process_sink_read(&mut connection, Ok(datagram), &client_to_sink_push).await);
    let mut client_buf = BytesMut::zeroed(2048);
    assert_eq!(
      ErrorKind::WouldBlock,
      connection.client.try_read(&mut client_buf).unwrap_err().kind()
    );
  }

  #[tokio::test]
  async fn test_process_sink_read_close_datagram() {
    setup_tracing().await;
    let (client_to_sink_push, _client_to_sink_pull) = async_channel::bounded(256);
    let (target_address, _) = run_echo().await;
    let identifier = 123;
    let datagram = create_close_datagram(identifier, 1);

    let client = TcpStream::connect(target_address).await.unwrap();
    let mut connection = ConnectionState::new(identifier, client);
    assert!(process_sink_read(&mut connection, Ok(datagram), &client_to_sink_push).await);
  }

  #[tokio::test]
  async fn test_process_sink_read_ack_datagram() {
    setup_tracing().await;
    let (client_to_sink_push, _client_to_sink_pull) = async_channel::bounded(256);
    let (target_address, _) = run_echo().await;
    let identifier = 123;
    let datagram = create_ack_datagram(identifier, 1, 10);

    let client = TcpStream::connect(target_address).await.unwrap();
    let mut connection = ConnectionState::new(identifier, client);
    connection.sequence = 5;
    assert_eq!(0, connection.largest_acked);
    assert!(!process_sink_read(&mut connection, Ok(datagram), &client_to_sink_push).await);
    assert_eq!(5, connection.largest_acked);
  }

  #[tokio::test]
  async fn test_process_sink_read_multi_ack_datagram() {
    setup_tracing().await;
    let (client_to_sink_push, client_to_sink_pull) = async_channel::bounded(256);
    let (target_address_a, _) = run_echo().await;
    let (target_address_b, _) = run_echo().await;
    let identifier = 123;

    let client_a = TcpStream::connect(target_address_a).await.unwrap();
    let client_b = TcpStream::connect(target_address_b).await.unwrap();
    let mut connection_a = ConnectionState::new(identifier, client_a);
    let mut connection_b = ConnectionState::new(identifier, client_b);

    for i in (2..21).rev() {
      connection_a.datagram_queue.insert(i, create_data_datagram(identifier, i, b"test"));
    }
    let data_datagram = create_data_datagram(identifier, 1, b"test");
    assert_eq!(0, connection_a.largest_acked);
    assert_eq!(0, connection_a.sequence);
    connection_b.sequence = 20;
    assert!(!process_sink_read(&mut connection_a, Ok(data_datagram), &client_to_sink_push).await);
    let ack_data = client_to_sink_pull.recv().await.unwrap();
    let ack_datagram = root_as_datagram(&ack_data).unwrap();
    assert_eq!(1, connection_a.largest_acked);
    assert_eq!(1, connection_a.sequence);
    assert_eq!(identifier, ack_datagram.identifier());
    assert_eq!(ControlCode::Ack, ack_datagram.code());
    assert_eq!(connection_a.sequence, ack_datagram.sequence());
    assert_eq!(20, u64::from_be_bytes(ack_datagram.data().unwrap().bytes().try_into().unwrap()));

    assert!(!process_sink_read(&mut connection_b, Ok(ack_data), &client_to_sink_push).await);
    assert_eq!(20, connection_b.largest_acked);
    assert!(client_to_sink_pull.try_recv().is_err());
  }

  #[tokio::test(start_paused = true)]
  async fn test_process_sink_old_datagram() {
    setup_tracing().await;
    let (client_to_sink_push, _client_to_sink_pull) = async_channel::bounded(256);
    let identifier = 123;
    let data = BytesMut::from("test data");
    let datagram = create_data_datagram(identifier, 1, &data);

    let (address_sender, mut address_receiver) = mpsc::channel::<SocketAddr>(1);
    let handle = tokio::spawn(async move {
      let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
      let address = listener.local_addr().unwrap();
      address_sender.send(address).await.unwrap();
      let (mut client, _) = listener.accept().await.unwrap();
      let mut client_buf = BytesMut::zeroed(2048);
      let read_result = timeout(Duration::from_secs(1), client.read(&mut client_buf)).await;
      assert!(read_result.is_err());
    });

    let server_address = address_receiver.recv().await.unwrap();
    let client = TcpStream::connect(server_address).await.unwrap();
    let mut connection = ConnectionState::new(identifier, client);
    connection.largest_processed += 1;
    assert!(!process_sink_read(&mut connection, Ok(datagram), &client_to_sink_push).await);
    assert!(connection.datagram_queue.is_empty());
    handle.await.unwrap();
  }

  #[tokio::test]
  async fn test_sink_loop_channel_closed() {
    setup_tracing().await;
    let (sink_a, _sink_b) = tokio::io::duplex(4096);
    let channel_map = Arc::new(HashMap::new());
    let (sink_to_client_push, sink_to_client_pull) = async_broadcast::broadcast(10);
    channel_map.pin().insert(
      Identifier::ClientInitiator,
      (sink_to_client_push.clone(), sink_to_client_pull.deactivate()),
    );
    let (_client_to_sink_push, client_to_sink_pull) = async_channel::bounded(256);
    let cancel = CancellationToken::new();
    client_to_sink_pull.close();
    let _sink_loop_handle =
      tokio::spawn(sink_loop(sink_a, channel_map, client_to_sink_pull, cancel.clone()));
    assert!(timeout(Duration::from_secs(1), cancel.cancelled()).await.is_ok());
  }

  #[tokio::test]
  async fn test_sink_read_sink_closed() {
    setup_tracing().await;
    let (sink_a, mut sink_b) = tokio::io::duplex(4096);
    let channel_map = Arc::new(HashMap::new());
    let (sink_to_client_push, sink_to_client_pull) = async_broadcast::broadcast(10);
    channel_map.pin().insert(
      Identifier::ClientInitiator,
      (sink_to_client_push.clone(), sink_to_client_pull.deactivate()),
    );
    let (_client_to_sink_push, client_to_sink_pull) = async_channel::bounded(256);
    let cancel = CancellationToken::new();
    sink_b.shutdown().await.unwrap();
    let _sink_loop_handle =
      tokio::spawn(sink_loop(sink_a, channel_map, client_to_sink_pull, cancel.clone()));
    assert!(timeout(Duration::from_secs(1), cancel.cancelled()).await.is_ok());
  }
}
