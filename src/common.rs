use crate::protocol_utils::{create_close_datagram, create_data_datagram};
use crate::schema_generated::serial_multiplexer::{ControlCode, root_as_datagram};
use anyhow::{anyhow, bail};
use bytes::{Bytes, BytesMut};
use memchr::memmem::Finder;
use std::cmp::min;
use std::collections::BTreeMap;
use std::sync::LazyLock;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug, error, info, trace};
use tracing_attributes::instrument;
use zeroize::Zeroize;

const SINK_BUFFER_SIZE: usize = 2usize.pow(17);
/// The maximum size of a (de)compressed datagram.
const SINK_COMPRESSION_BUFFER_SIZE: usize = 2usize.pow(15);
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
};

/// An array representing a header used to identify the start of a datagram in a byte stream
const DATAGRAM_HEADER: [u8; 8] = [2, 200, 94, 2, 6, 9, 4, 20];
const HEADER_BYTES: usize = DATAGRAM_HEADER.len();
pub const HUGE_DATA_TARGET: &str = concat!(env!("CARGO_CRATE_NAME"), "::huge_data");
/// Number of bytes used to store the size of a datagram
const LENGTH_BYTES: usize = 2;
static HEADER_FINDER: LazyLock<Finder<'static>> =
  LazyLock::new(|| Finder::new(&DATAGRAM_HEADER).into_owned());

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
/// * `datagram_queue` (`BTreeMap<u64, Bytes>`) - A map (ordered by sequence number) storing datagrams
///   that are awaiting processing because they arrived out-of-order.
pub struct ConnectionState {
  pub identifier: u64,
  pub client: TcpStream,
  pub sequence: u64,
  pub largest_processed: u64,
  pub datagram_queue: BTreeMap<u64, Bytes>,
}

impl ConnectionState {
  pub const fn new(identifier: u64, client: TcpStream) -> Self {
    Self {
      identifier,
      client,
      sequence: 0,
      largest_processed: 0,
      datagram_queue: BTreeMap::new(),
    }
  }
}

/// Asynchronous loop for handling communication between a `sink` and clients.
///
/// This function manages bidirectional communication between a `sink` (a stream
/// implementing both `AsyncRead` and `AsyncWrite`) and clients using channels.
/// It reads data from the sink and pushes it to the client via a broadcast channel,
/// while also receiving data from the client and writing it to the sink.
/// The loop terminates on a cancellation signal, sink disconnection, or an error during
/// read/write operations.
///
/// # Parameters
///
/// * `sink`: An object implementing [`AsyncReadExt`] and [`AsyncWriteExt`],
///   currently, this will either be [`SerialStream`] or [`NamedPipeClient`].
/// * `sink_to_client_push`: A [`broadcast::Sender`] used to send data from the sink to
///   clients.
/// * `client_to_sink_pull`: An [`async_channel::Receiver`] channel through which the
///   function receives data sent by the client to be written to the sink.
/// * `cancel`: A [`CancellationToken`] used to signal when this loop should terminate.
///
/// # Behaviour
///
/// - Continuously reads from the `sink` using a buffer of size [`SINK_BUFFER_SIZE`] and
///   sends the processed data to clients via the `sink_to_client_push` channel.
/// - Continuously listens for data from the `client_to_sink_pull` channel to write
///   to the `sink`.
/// - Exits the loop gracefully when the cancellation token is triggered, the sink is
///   disconnected, or an error occurs during read/write operations.
///
/// # Key Operations:
///
/// * `sink.read()`:
///   - Reads data from the `sink` into an internal buffer.
///   - Processes any unprocessed bytes in the buffer and sends them to the client
///     broadcast channel.
///   - Handles errors or disconnects from the `sink`, breaking the loop if necessary.
///
/// * `client_to_sink_pull.recv()`:
///   - Receives data from a client to write into the `sink`
///   - Uses the `handle_sink_write()` function to handle the actual write to the `sink`.
///
/// * `cancel.cancelled()`:
///   - Terminates the loop immediately when cancellation is triggered.
///
/// [`SerialStream`]: tokio_serial::SerialStream
/// [`NamedPipeClient`]: tokio::net::windows::named_pipe::NamedPipeServer
#[instrument(skip_all)]
pub async fn sink_loop(
  mut sink: impl AsyncReadExt + AsyncWriteExt + Unpin,
  sink_to_client_push: broadcast::Sender<Bytes>,
  client_to_sink_pull: async_channel::Receiver<Bytes>,
  cancel: CancellationToken,
) {
  let mut sink_buf = BytesMut::zeroed(SINK_BUFFER_SIZE);
  let mut compression_buffer = BytesMut::zeroed(SINK_COMPRESSION_BUFFER_SIZE);
  let mut unprocessed_data_start = 0;
  loop {
    let current_span = Span::current();
    sink_buf.resize(SINK_BUFFER_SIZE, 0);
    tokio::select! {
      biased;
      () = cancel.cancelled() => {
        if let Err(e) = sink.shutdown().await {
          error!("Failed to shutdown sink: {}", e);
        }
        return;
      }
      data = client_to_sink_pull.recv() => {
        match data {
          Ok(data) => {
            let sink_write_duration = Instant::now();
            let result = handle_sink_write(&mut sink, data, &mut compression_buffer).instrument(current_span).await;
            trace!(duration = ?sink_write_duration.elapsed(), "Finished writing data to sink");
            if let Err(e) = result {
              error!("Failed to handle sink write: {}", e);
              break;
            }
          },
          Err(e) => {
            info!("Sink receiver closed, {}", e);
            break;
          }
        }
      }
      n = sink.read(&mut sink_buf[unprocessed_data_start..]) => {
        match n {
          Ok(0) => {
            info!("Sink disconnected");
            break;
          }
          Ok(n) => {
            let sink_read_duration = Instant::now();
            let result = handle_sink_read(n + unprocessed_data_start, &mut sink_buf, sink_to_client_push.clone(), &mut compression_buffer);
            trace!(duration = ?sink_read_duration.elapsed(), "Finished reading data from sink");
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
    }
  }
  cancel.cancel();
}

/// Handles reading data from a sink buffer, processes complete datagrams, and forwards them
/// to clients via a broadcast channel.
/// Any unprocessed data is preserved for subsequent reads.
///
/// # Arguments
///
/// * `bytes_read` - The number of bytes read into the sink buffer from the sink.
/// * `sink_buf` -
///   A mutable reference to a buffer ([`BytesMut`]) that stores the data read from the sink.
/// * `sink_to_client_push` - A [`broadcast::Sender<Bytes>`]
///   used to send processed datagrams to clients.
/// * `decompression_buffer` - A mutable reference to a buffer ([`BytesMut`]) for storing
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
/// 5. Sends the decompressed datagram to clients via the `sink_to_client_push` channel.
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
/// Returns an error if there is an issue with sending the datagram to the `sink_to_client_push`
/// channel.
/// The error is propagated using [`anyhow::Result`].
///
/// # Notes
///
/// - Datagram format assumptions:
///   * Datagram headers are identified using the [`HEADER_FINDER`].
///   * The size of the datagram is determined by bytes immediately following the header.
///     The number of bytes is specified by [`LENGTH_BYTES`].
///
/// - Buffer behaviour:
///   * If the entire buffer was processed, the buffer will be zeroed out.
///   * If some bytes remain unprocessed, they will be copied to the beginning of the buffer
///     and the rest of the buffer will be zeroed out.
pub fn handle_sink_read(
  bytes_read: usize,
  sink_buf: &mut BytesMut,
  sink_to_client_push: broadcast::Sender<Bytes>,
  decompression_buffer: &mut BytesMut,
) -> anyhow::Result<usize> {
  debug_assert!(decompression_buffer.len() <= SINK_COMPRESSION_BUFFER_SIZE);
  debug_assert!(sink_buf.len() <= SINK_BUFFER_SIZE);
  trace!("Read {} bytes from sink", bytes_read);
  let mut read_data = &sink_buf[..bytes_read];
  trace!(target: HUGE_DATA_TARGET, "Data in working buffer: {:?}", &read_data);
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
    let decompressed_size = zstd_safe::decompress(
      decompression_buffer.as_mut(),
      &read_data[datagram_start..=datagram_end],
    )
    .inspect(|n| debug!("Decompressed {} to {} bytes", size, n))
    .map_err(|e| anyhow!("Failed to decompress datagram with error code = {}", e))?;
    let datagram_bytes = Bytes::copy_from_slice(&decompression_buffer[..decompressed_size]);
    decompression_buffer.zeroize();
    trace!(target: HUGE_DATA_TARGET, "Read datagram: {:?}", datagram_bytes);
    if let Err(e) = sink_to_client_push.send(datagram_bytes) {
      bail!("Failed to send data to clients. {}", e);
    }
    unprocessed_data_start += datagram_end + 1;
    trace!(
      "Removing processed data in the working buffer at index {}",
      min(bytes_read, unprocessed_data_start)
    );
    read_data = &sink_buf[min(bytes_read, unprocessed_data_start)..bytes_read];
    trace!(target: HUGE_DATA_TARGET, "Data in buffer after reading datagram: {:?}", &read_data);
  }

  let mut unprocessed_bytes = bytes_read;
  if unprocessed_data_start > 0 {
    unprocessed_bytes = bytes_read - unprocessed_data_start;
    if unprocessed_data_start >= bytes_read {
      trace!("Whole buffer was read, zeroing out processed data");
      sink_buf.zeroize();
    } else {
      trace!(
        "Copying unprocessed data (length: {}) to the start of buffer and zeroing out the rest",
        unprocessed_data_start
      );
      let buffer_end = sink_buf.len();
      trace!(target: HUGE_DATA_TARGET, "Buffer before copying and zeroing: {:?}", &sink_buf);
      sink_buf.copy_within(unprocessed_data_start..buffer_end, 0);
      sink_buf[unprocessed_bytes..buffer_end].zeroize();
      trace!(target: HUGE_DATA_TARGET, "Buffer after copying and zeroing: {:?}", &sink_buf);
    }
  }
  Ok(unprocessed_bytes)
}

/// Handles writing datagrams to the sink.
/// This function writes a predefined header,
/// the length of the datagram, and the actual datagram in order.
///
/// # Parameters
/// * `sink`:
///   A mutable reference to any type (`T`)
///   that is used as a sink and where the data will be written.
///   Currently, this will either be [`SerialStream`] or [`NamedPipeClient`].
/// * `data`:
///   A [`Bytes`] object representing the datagram that will be written to the sink.
/// * `compression_buffer`: A mutable reference to a buffer ([`BytesMut`]) for storing
///   the datagram after compression
///
/// # Behaviour
/// The function follows these steps:
/// 1. Compresses the datagram using [`zstd_safe`] into `compression_buffer`
/// 2. Writes a constant [`DATAGRAM_HEADER`] (the header) to the sink.
/// 3. Writes the size of the `data` as a 2-byte value to the sink.
/// 4. Writes the `datagram` itself to the sink.
/// 5. Flushes the sink to ensure everything is written out.
///
/// # Errors
/// This function returns an `anyhow::Result<()>`, which will contain an error
/// if any of the following operations fail:
/// - Writing the [`DATAGRAM_HEADER`] to the sink.
/// - Writing the length of the datagram to the sink.
/// - Writing the `datagram` itself to the sink.
/// - Flushing the sink after all writes.
///
/// The error message will include details about which operation failed.
///
/// # Notes
/// The size of the data is written as a 2-byte value using the [`split_u16_to_u8`]
/// function.
///
/// [`SerialStream`]: tokio_serial::SerialStream
/// [`NamedPipeClient`]: tokio::net::windows::named_pipe::NamedPipeServer
pub async fn handle_sink_write<T: AsyncReadExt + AsyncWriteExt + Unpin + Sized>(
  sink: &mut T,
  data: Bytes,
  compression_buffer: &mut BytesMut,
) -> anyhow::Result<()> {
  trace!("Maximum compressed size: {}", zstd_safe::compress_bound(data.len()));
  let compressed_size = zstd_safe::compress(compression_buffer.as_mut(), &data, 9)
    .inspect(|n| debug!("Compressed {} to {} bytes", data.len(), n))
    .map_err(|e| anyhow!("Failed to compress datagram with error code = {}", e))?;
  debug!("Writing {} bytes to sink", compressed_size + HEADER_BYTES + LENGTH_BYTES);
  trace!(target: HUGE_DATA_TARGET, "Writing compressed datagram to sink: {:?}", &compression_buffer[..compressed_size]);
  if let Err(e) = sink.write_all(&DATAGRAM_HEADER).await {
    bail!("Failed to write HEADER to sink: {}", e);
  }
  let size = split_u16_to_u8(compressed_size as u16);
  if let Err(e) = sink.write(&size).await {
    bail!("Failed to write size to sink: {}", e);
  }
  if let Err(e) = sink.write_all(&compression_buffer[..compressed_size]).await {
    bail!("Failed to write data to sink: {}", e);
  }
  if let Err(e) = sink.flush().await {
    bail!("Failed to flush sink after writing: {}", e);
  }
  compression_buffer[..compressed_size].zeroize();
  Ok(())
}

///
/// Handles reads from client connections.
///
/// This function processes the result of a read operation from a client and performs actions
/// based on whether the read was successful, encountered an error, or if the client disconnected.
/// It sends appropriate datagrams to be handled downstream via the provided channel.
///
/// # Arguments
///
/// * `identifier` - A unique identifier for the client connection.
/// * `sequence` - The current datagram sequence number for the connection, used for ordering data.
/// * `client_to_pipe_push` - An async channel sender used to send datagrams to [`sink_loop`].
/// * `bytes_read` - The result of a read operation indicating the number of bytes read or an error.
/// * `read_buf` - A mutable buffer containing the bytes read from the client connection.
///
/// # Returns
///
/// A boolean indicating whether the connection should be closed:
/// - `true` if the connection should be terminated (e.g. client disconnected or read error).
/// - `false` if the connection remains active.
///
/// # Behaviour
///
/// - If `bytes_read` is `Ok(0)`, the client has disconnected. A "CLOSE" datagram is created
///   and sent via the channel. The function returns `true`.
///
/// - If `bytes_read` is `Ok(n)`, where `n > 0`, the bytes are processed, and a "DATA" datagram
///   is created and sent via the channel. The read buffer is cleared, and the function returns `false`.
///
/// - If `bytes_read` is `Err(e)`, an error occurred while reading from the client. The error
///   is logged, and the function returns `true`.
///
/// # Zeroization
///
/// The portion of the read buffer containing the bytes read is zeroed to clear
/// any sensitive data after processing.
pub async fn handle_client_read(
  identifier: u64,
  sequence: u64,
  client_to_pipe_push: async_channel::Sender<Bytes>,
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
      trace!(target: HUGE_DATA_TARGET, "Built datagram with client data: {:?}", &datagram);
      read_buf[..n].zeroize();
      debug!("Sending DATA datagram of {} bytes with seq: {}", datagram.len(), sequence);
      if let Err(e) = client_to_pipe_push.send(datagram).await {
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
    if let Err(e) = client_to_pipe_push.send(datagram).await {
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
/// # Arguments
///
/// * `connection` - A mutable reference to a [`ConnectionState`] object, which represents
///   the current connection state.
/// * `data` - A [`Result`] containing the incoming datagram payload encapsulated in [`Bytes`]
///   or an error of type [`broadcast::error::RecvError`].
///
/// # Returns
/// A `bool` indicating:
/// * `true` - if the connection should be terminated, either due to receiving a `CLOSE`
///   control code from the client or because the sink channel is closed.
/// * `false` - if the connection should remain active.
///
/// # Behaviour
///
/// 1. **Datagram Validation**:
///     - If the datagram's identifier does not match the connection's, it is ignored.
///     - If the received datagram is malformed, it is logged and ignored.
///
/// 2. **Sequencing and Queuing**:
///     - If the datagram's sequence is higher than the expected next sequence, it is
///       queued for future processing (out-of-order reception).
///     - If the datagram is in sequence, it and any subsequent queued datagrams are processed
///       in order until no more in-sequence datagrams remain.
///
/// 3. **Processing and Actions**:
///     - If valid payload data is present in a datagram:
///         - Attempts to forward it to the client.
///         - Any write or flush errors are logged and terminate the function with `true`.
///     - If a [`Close`] control code is received, the client connection is shut down, the
///       termination is logged, and the function returns `true`.
///
/// 4. **Error Handling**:
///     - If a [`broadcast::error::RecvError::Closed`] is encountered, it shuts down the client
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
/// - Malformed datagrams are logged and ignored.
///
/// # Notes
/// - Connection's `datagram_queue` and `largest_processed` are used to ensure in-order
///   delivery of datagrams.
///
/// [`Close`]: ControlCode::Close
pub async fn process_sink_read(
  connection: &mut ConnectionState,
  data: Result<Bytes, broadcast::error::RecvError>,
) -> bool {
  match data {
    Ok(data_buf) => {
      let datagram = match root_as_datagram(&data_buf) {
        Ok(datagram) => datagram,
        Err(e) => {
          error!("Received malformed datagram: {:?}, ignoring", e);
          return false;
        }
      };
      if datagram.identifier() != connection.identifier {
        // Not our datagram, ignore it
        return false;
      }
      let datagram_sequence = datagram.sequence();
      debug!(
        "Received {:?} datagram with seq: {} and {} bytes of data",
        datagram.code(),
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
        return false;
      } else if datagram_sequence == (connection.largest_processed + 1) {
        trace!(
          "Received datagram in order, seq: {}, largest sent: {}",
          datagram_sequence, connection.largest_processed
        );
        connection.datagram_queue.insert(datagram_sequence, data_buf);
        while let Some(datagram_buf) =
          connection.datagram_queue.remove(&(connection.largest_processed + 1))
        {
          connection.largest_processed += 1;
          let Ok(datagram) = root_as_datagram(&datagram_buf) else {
            continue;
          };
          if let Some(data) = datagram.data() {
            trace!("Sending data to client, size {}", data.len());
            match connection.client.write_all(data.bytes()).await {
              Ok(()) => {
                trace!("Sent {} bytes to client", data.len());
              }
              Err(e) => {
                error!("Failed to write data to client: {}", e);
                return true;
              }
            }
          }
          if datagram.code() == ControlCode::Close {
            if let Err(e) = connection.client.shutdown().await {
              error!("Failed to shutdown client after receiving CLOSE: {}", e);
            }
            return true;
          }
        }
        if let Err(e) = connection.client.flush().await {
          error!("Failed to flush client data after writing data: {}", e);
        }
      }
      false
    }
    Err(broadcast::error::RecvError::Closed) => {
      if let Err(e) = connection.client.shutdown().await {
        error!("Failed to shutdown client: {}", e);
      }
      true
    }
    Err(_) => false,
  }
}

/// Handles reading and processing data to/from the client and serial port(s).
///
/// # Parameters
///
/// * `connection` - The current [`ConnectionState`] that holds the client's state and identifier.
/// * `sink_to_client_pull` -
///   A [`broadcast::Receiver<Bytes>`] to receive datagrams from the sink for processing.
/// * `client_to_sink_push` -
///   An [`async_channel::Sender`] for sending data received from the client to the sink(s).
/// * `cancel` -
///   A [`CancellationToken`] used to signal loop termination due to the app shutting down.
///
/// # Behaviour
///
/// The function runs in a loop that listens for three primary conditions:
/// 1. **Cancellation signal**: If `cancel.cancelled()` is triggered, the connection is closed
///    gracefully by shutting down the client, sending a [`Close`] datagram and exiting the loop.
/// 2. **Incoming data from the server**:
///    When [`pipe_to_client_pull.recv()`] receives data from sink,
///    it uses [`process_sink_read()`] to handle and forward the data to the client.
///    The processing might signal connection termination, which breaks the loop.
/// 3. **Incoming data from the client**:
///    When [`connection.client.read()`] receives data from the client,
///    it processes the data with [`handle_client_read()`].
///    The processing might signal connection termination, which breaks the loop.
///
/// [`Close`]: ControlCode::Close
#[instrument(skip_all, fields(connection_id = %connection.identifier))]
pub async fn connection_loop(
  mut connection: ConnectionState,
  mut sink_to_client_pull: broadcast::Receiver<Bytes>,
  client_to_sink_push: async_channel::Sender<Bytes>,
  cancel: CancellationToken,
) {
  let mut tcp_buf = BytesMut::zeroed(CONNECTION_BUFFER_SIZE);
  loop {
    let current_span = Span::current();
    tcp_buf.resize(CONNECTION_BUFFER_SIZE, 0);
    tokio::select! {
      biased;
      () = cancel.cancelled() => {
        if let Err(e) = connection.client.shutdown().await {
          error!("Failed to shutdown client after server shutdown: {}", e);
        }
        connection.sequence += 1;
        let datagram = create_close_datagram(connection.identifier, connection.sequence);
        if let Err(e) = client_to_sink_push.send(datagram).await {
          error!("Failed to send CLOSE datagram for connection {}: {}", connection.identifier, e);
        }
        break;
      }
      data = sink_to_client_pull.recv() => {
        let sink_read_start = Instant::now();
        let connection_terminating = process_sink_read(&mut connection, data).await;
        trace!(duration = ?sink_read_start.elapsed(), "Finished processing sink read");
        if connection_terminating {
          break;
        }
      }
      bytes_read = connection.client.read(&mut tcp_buf) => {
        connection.sequence += 1;
        let client_read_duration = Instant::now();
        let connection_terminating = handle_client_read(
          connection.identifier,
          connection.sequence,
          client_to_sink_push.clone(),
          bytes_read,
          &mut tcp_buf,
        ).instrument(current_span).await;
        trace!(duration = ?client_read_duration.elapsed(), "Finished processing client read");
        if connection_terminating {
          break;
        }
      }
    }
  }
  debug!("Connection {} loop ending", connection.identifier);
}

/// Splits a 16-bit unsigned integer (`u16`) into two 8-bit unsigned integers (`u8`)
/// and returns them as an array.
///
/// # Parameters
/// - `n`: A 16-bit unsigned integer (`u16`) to be split.
///
/// # Returns
/// - An array of two `u8` values:
///   - `[upper, lower]`, where:
///     - `upper` is the most significant byte of the input `u16`.
///     - `lower` is the least significant byte of the input `u16`.
///
/// # Note
/// The actual length of the returned array is specified by [`LENGTH_BYTES`].
pub const fn split_u16_to_u8(n: u16) -> [u8; LENGTH_BYTES] {
  let upper = (n >> 8) as u8;
  let lower = n as u8;
  [upper, lower]
}

/// Combines two 8-bit unsigned integers (`u8`) into a single 16-bit unsigned integer (`u16`).
///
/// # Arguments
///
/// * `upper` - An 8-bit unsigned integer (`u8`) representing the upper byte of the 16-bit result.
/// * `lower` - An 8-bit unsigned integer (`u8`) representing the lower byte of the 16-bit result.
///
/// # Returns
///
/// A 16-bit unsigned integer (`u16`) created by combining `upper` and `lower`.
/// The `upper` value is shifted left by 8 bits to occupy the high byte, and
/// the `lower` value occupies the low byte.
pub const fn join_u8_to_u16(upper: u8, lower: u8) -> u16 {
  ((upper as u16) << 8) | lower as u16
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::protocol_utils::{create_ack_datagram, create_data_datagram, create_initial_datagram};
  use crate::schema_generated::serial_multiplexer::{ControlCode, root_as_datagram};
  use crate::test_utils::{run_echo, setup_tracing};
  use std::net::SocketAddr;
  use std::time::Duration;
  use tokio::net::{TcpListener, TcpStream};
  use tokio::sync::mpsc;
  use tokio::time::timeout;

  #[tokio::test]
  async fn test_handle_sink_read_double_with_rubbish() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::new();
    let mut compression_buf = BytesMut::zeroed(SINK_COMPRESSION_BUFFER_SIZE);
    let (sink_to_client_push, mut sink_to_client_pull) = broadcast::channel(20);
    sink_buf.extend_from_slice(&[1, 2]);
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let n = zstd_safe::compress(compression_buf.as_mut(), &[4, 5, 6], 9).unwrap();
    sink_buf.extend_from_slice(&split_u16_to_u8(n as u16));
    sink_buf.extend_from_slice(&compression_buf[..n]);
    sink_buf.extend_from_slice(&[7, 8, 9]);
    let mut buffer_size = sink_buf.len();

    let unprocessed_bytes = handle_sink_read(
      sink_buf.len(),
      &mut sink_buf,
      sink_to_client_push.clone(),
      &mut compression_buf,
    )
    .unwrap();
    assert_eq!(sink_buf.len(), buffer_size);
    assert_eq!(unprocessed_bytes, 3);
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), Bytes::from_static(&[4, 5, 6]));
    assert_eq!(sink_buf[3..], BytesMut::zeroed(buffer_size - unprocessed_bytes));
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));
    sink_buf.resize(unprocessed_bytes, 0);

    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let n = zstd_safe::compress(compression_buf.as_mut(), &[5; 100], 9).unwrap();
    sink_buf.extend_from_slice(&split_u16_to_u8(n as u16));
    sink_buf.extend_from_slice(&compression_buf[..n]);
    sink_buf.extend_from_slice(&[11, 12, 13, 14, 15]);
    buffer_size = sink_buf.len();
    let unprocessed_bytes = handle_sink_read(
      sink_buf.len(),
      &mut sink_buf,
      sink_to_client_push.clone(),
      &mut compression_buf,
    )
    .unwrap();
    assert_eq!(sink_buf.len(), buffer_size);
    assert_eq!(unprocessed_bytes, 5);
    assert_eq!(sink_to_client_pull.recv().await.unwrap(), Bytes::from_static(&[5; 100]));
    assert_eq!(sink_buf[5..], BytesMut::zeroed(buffer_size - unprocessed_bytes));
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));
  }

  #[tokio::test]
  async fn test_handle_sink_read_multiple_datagrams() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::new();
    let mut compression_buf = BytesMut::zeroed(SINK_COMPRESSION_BUFFER_SIZE);
    let (sink_to_client_push, mut sink_to_client_pull) = broadcast::channel(20);

    let datagram1 = create_data_datagram(0, 1, b"datagram1");
    let datagram2 = create_data_datagram(1, 2, b"datagram2");
    let datagram3 = create_data_datagram(2, 3, b"datagram3");
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let n = zstd_safe::compress(compression_buf.as_mut(), &datagram1, 9).unwrap();
    sink_buf.extend_from_slice(&split_u16_to_u8(n as u16));
    sink_buf.extend_from_slice(&compression_buf[..n]);
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let n = zstd_safe::compress(compression_buf.as_mut(), &datagram2, 9).unwrap();
    sink_buf.extend_from_slice(&split_u16_to_u8(n as u16));
    sink_buf.extend_from_slice(&compression_buf[..n]);
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let n = zstd_safe::compress(compression_buf.as_mut(), &datagram3, 9).unwrap();
    sink_buf.extend_from_slice(&split_u16_to_u8(n as u16));
    sink_buf.extend_from_slice(&compression_buf[..n]);

    let buffer_size = sink_buf.len();
    let unprocessed_bytes = handle_sink_read(
      buffer_size,
      &mut sink_buf,
      sink_to_client_push.clone(),
      &mut compression_buf,
    )
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
    let (sink_to_client_push, mut sink_to_client_pull) = broadcast::channel(20);

    let datagram_data = BytesMut::from(vec![65u8; CONNECTION_BUFFER_SIZE].as_slice());
    let datagram = create_data_datagram(0, 1, &datagram_data);
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let n = zstd_safe::compress(compression_buf.as_mut(), &datagram, 9).unwrap();
    sink_buf.extend_from_slice(&split_u16_to_u8(n as u16));
    sink_buf.extend_from_slice(&compression_buf[..n]);

    let buffer_size = sink_buf.len();
    let unprocessed_bytes = handle_sink_read(
      buffer_size,
      &mut sink_buf,
      sink_to_client_push.clone(),
      &mut compression_buf,
    )
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
    let (sink_to_client_push, mut sink_to_client_pull) = broadcast::channel(20);
    sink_buf.extend_from_slice(&[1, 2]); // invalid data to offset HEADER
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    let buffer_size = sink_buf.len();

    let unprocessed_bytes = handle_sink_read(
      buffer_size,
      &mut sink_buf,
      sink_to_client_push.clone(),
      &mut compression_buf,
    )
    .unwrap();
    assert_eq!(buffer_size, sink_buf.len());
    assert!(sink_to_client_pull.try_recv().is_err());
    assert_eq!(unprocessed_bytes, buffer_size);
    assert_ne!(sink_buf, BytesMut::zeroed(sink_buf.len()));
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));
  }

  #[tokio::test]
  async fn test_handle_sink_read_data_partially_read() {
    setup_tracing().await;
    let mut sink_buf = BytesMut::new();
    let mut compression_buf = BytesMut::zeroed(SINK_COMPRESSION_BUFFER_SIZE);
    let (sink_to_client_push, mut sink_to_client_pull) = broadcast::channel(20);

    sink_buf.extend_from_slice(&[1, 2]); // invalid data to offset HEADER
    sink_buf.extend_from_slice(&DATAGRAM_HEADER);
    sink_buf.extend_from_slice(&split_u16_to_u8(5));
    sink_buf.extend_from_slice(&[1; 3]);
    let buffer_size = sink_buf.len();

    let unprocessed_bytes = handle_sink_read(
      buffer_size,
      &mut sink_buf,
      sink_to_client_push.clone(),
      &mut compression_buf,
    )
    .unwrap();
    assert_eq!(buffer_size, sink_buf.len());
    assert!(sink_to_client_pull.try_recv().is_err());
    assert_eq!(unprocessed_bytes, buffer_size);
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));
  }

  #[tokio::test]
  async fn test_sink_loop_read_sink() {
    setup_tracing().await;
    let mut compression_buf = BytesMut::zeroed(SINK_COMPRESSION_BUFFER_SIZE);
    let (sink_a, mut sink_b) = tokio::io::duplex(4096);
    let (pipe_to_client_push, mut pipe_to_client_pull) = broadcast::channel(256);
    let (_client_to_pipe_push, client_to_pipe_pull) = async_channel::bounded(256);
    let cancel = CancellationToken::new();
    let _pipe_loop_handle = tokio::spawn(sink_loop(
      sink_a,
      pipe_to_client_push.clone(),
      client_to_pipe_pull,
      cancel.clone(),
    ));

    info!("Sending initial datagram");
    let initial_data = "test data";
    let initial_datagram = create_initial_datagram(1, 0, initial_data);
    handle_sink_write(&mut sink_b, initial_datagram, &mut compression_buf).await.unwrap();
    let datagram_bytes =
      timeout(Duration::from_secs(1), pipe_to_client_pull.recv()).await.unwrap().unwrap();
    let initial_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(initial_datagram.identifier(), 1);
    assert_eq!(initial_datagram.code(), ControlCode::Initial);
    assert_eq!(initial_datagram.data().unwrap().bytes(), initial_data.as_bytes());
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));

    // write some rubbish between datagrams
    info!("Sending rubbish data");
    let data = BytesMut::from("rubbish");
    sink_b.write_all(&data).await.unwrap();
    sink_b.flush().await.unwrap();

    info!("Sending ACK datagram");
    let ack_datagram = create_ack_datagram(2, 0);
    handle_sink_write(&mut sink_b, ack_datagram, &mut compression_buf).await.unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let ack_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(ack_datagram.identifier(), 2);
    assert_eq!(ack_datagram.code(), ControlCode::Ack);
    assert_eq!(ack_datagram.data().unwrap().bytes(), &[]);
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));

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
    let data_datagram = create_data_datagram(3, 1, &data);
    handle_sink_write(&mut sink_b, data_datagram, &mut compression_buf)
      .await
      .unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let data_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(data_datagram.identifier(), 3);
    assert_eq!(data_datagram.code(), ControlCode::Data);
    assert_eq!(data_datagram.data().unwrap().bytes(), data);
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));

    info!("Sending large datagram 2");
    let mut data = BytesMut::new();
    data.resize(5000, 100u8);
    let data_datagram = create_data_datagram(3, 2, &data);
    handle_sink_write(&mut sink_b, data_datagram, &mut compression_buf)
      .await
      .unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let data_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(data_datagram.identifier(), 3);
    assert_eq!(data_datagram.code(), ControlCode::Data);
    assert_eq!(data_datagram.data().unwrap().bytes(), data);
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));

    info!("Sending large datagram 3");
    let mut data = BytesMut::new();
    data.resize(10000, 100u8);
    let data_datagram = create_data_datagram(3, 4, &data);
    handle_sink_write(&mut sink_b, data_datagram, &mut compression_buf)
      .await
      .unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let data_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(data_datagram.identifier(), 3);
    assert_eq!(data_datagram.code(), ControlCode::Data);
    assert_eq!(data_datagram.data().unwrap().bytes(), data);
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));

    info!("Sending large datagram 4");
    let mut data = BytesMut::new();
    data.resize(15000, 100u8);
    let data_datagram = create_data_datagram(3, 3, &data);
    handle_sink_write(&mut sink_b, data_datagram, &mut compression_buf)
      .await
      .unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let data_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(data_datagram.identifier(), 3);
    assert_eq!(data_datagram.code(), ControlCode::Data);
    assert_eq!(data_datagram.data().unwrap().bytes(), data);
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));

    info!("Sending large datagram 5");
    let mut data = BytesMut::new();
    data.resize(CONNECTION_BUFFER_SIZE, 100u8);
    let data_datagram = create_data_datagram(3, 5, &data);
    handle_sink_write(&mut sink_b, data_datagram, &mut compression_buf)
      .await
      .unwrap();
    let datagram_bytes = pipe_to_client_pull.recv().await.unwrap();
    let data_datagram = root_as_datagram(&datagram_bytes).unwrap();
    assert_eq!(data_datagram.identifier(), 3);
    assert_eq!(data_datagram.code(), ControlCode::Data);
    assert_eq!(data_datagram.data().unwrap().bytes(), data);
    assert_eq!(compression_buf, BytesMut::zeroed(compression_buf.len()));
  }

  #[tokio::test]
  async fn test_handle_client_read_smoke() {
    setup_tracing().await;
    let contents = "test data";
    let mut bytes = BytesMut::from(contents);
    let (pipe_push, pipe_pull) = async_channel::bounded(1);

    let identifier = 123;
    let bytes_read = Ok(bytes.len());

    assert!(!handle_client_read(identifier, 1, pipe_push, bytes_read, &mut bytes).await);
    let pipe_read = pipe_pull.recv().await.unwrap();
    let datagram = root_as_datagram(&pipe_read).expect("Received datagram should be valid");
    assert_eq!(datagram.identifier(), identifier);
    assert_eq!(datagram.code(), ControlCode::Data);
    assert_eq!(datagram.data().unwrap().bytes(), contents.as_bytes());
    assert_eq!(bytes, BytesMut::zeroed(bytes.len()));
  }

  #[tokio::test]
  async fn test_handle_client_read_client_disconnect() {
    setup_tracing().await;
    let (pipe_push, _pipe_pull) = async_channel::bounded(1);
    let identifier = 123;
    let bytes_read = Ok(0);

    assert!(handle_client_read(identifier, 1, pipe_push, bytes_read, &mut BytesMut::new()).await);
  }

  #[tokio::test]
  async fn test_handle_client_read_client_error() {
    setup_tracing().await;
    let (pipe_push, _pipe_pull) = async_channel::bounded(1);
    let identifier = 123;
    let bytes_read = Err(std::io::Error::other("test error"));

    assert!(handle_client_read(identifier, 2, pipe_push, bytes_read, &mut BytesMut::new()).await);
  }

  #[tokio::test]
  async fn test_process_sink_read_smoke() {
    setup_tracing().await;
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
    assert!(!process_sink_read(&mut connection, Ok(datagram),).await);
    handle.await.unwrap();
  }

  #[tokio::test]
  async fn test_process_sink_read_out_of_order() {
    setup_tracing().await;
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
    assert!(!process_sink_read(&mut connection, Ok(datagram2)).await);
    assert!(connection.datagram_queue.contains_key(&2));
    assert_eq!(1, connection.datagram_queue.len());
    assert!(!process_sink_read(&mut connection, Ok(datagram1)).await);
    assert!(connection.datagram_queue.is_empty());
    handle.await.unwrap();
  }

  #[tokio::test]
  async fn test_process_sink_read_invalid_datagram() {
    setup_tracing().await;
    let (target_address, _) = run_echo().await;
    let identifier = 123;
    let data = Bytes::from("invalid test data");
    let client = TcpStream::connect(target_address).await.unwrap();
    let mut connection = ConnectionState::new(identifier, client);

    assert!(!process_sink_read(&mut connection, Ok(data)).await);
  }

  #[tokio::test]
  async fn test_process_sink_read_different_identifier() {
    setup_tracing().await;
    let (target_address, _) = run_echo().await;
    let identifier = 123;
    let data = BytesMut::from("test data");
    let datagram = create_data_datagram(identifier, 1, &data);

    let client = TcpStream::connect(target_address).await.unwrap();
    let mut connection = ConnectionState::new(500, client);
    assert!(!process_sink_read(&mut connection, Ok(datagram)).await);
    let mut client_buf = BytesMut::zeroed(2048);
    assert_eq!(
      std::io::ErrorKind::WouldBlock,
      connection.client.try_read(&mut client_buf).unwrap_err().kind()
    );
  }

  #[tokio::test]
  async fn test_process_sink_read_close_datagram() {
    setup_tracing().await;
    let (target_address, _) = run_echo().await;
    let identifier = 123;
    let datagram = create_close_datagram(identifier, 1);

    let client = TcpStream::connect(target_address).await.unwrap();
    let mut connection = ConnectionState::new(identifier, client);
    assert!(process_sink_read(&mut connection, Ok(datagram)).await);
    let mut client_buf = BytesMut::zeroed(2048);
    assert_eq!(
      0,
      timeout(Duration::from_secs(1), connection.client.read(&mut client_buf))
        .await
        .unwrap()
        .unwrap()
    );
  }

  #[tokio::test]
  async fn test_sink_loop_channel_closed() {
    setup_tracing().await;
    let (sink_a, _sink_b) = tokio::io::duplex(4096);
    let (pipe_to_client_push, _pipe_to_client_pull) = broadcast::channel(256);
    let (_client_to_pipe_push, client_to_pipe_pull) = async_channel::bounded(256);
    let cancel = CancellationToken::new();
    client_to_pipe_pull.close();
    let _pipe_loop_handle = tokio::spawn(sink_loop(
      sink_a,
      pipe_to_client_push.clone(),
      client_to_pipe_pull,
      cancel.clone(),
    ));
    assert!(timeout(Duration::from_secs(1), cancel.cancelled()).await.is_ok());
  }

  #[tokio::test]
  async fn test_sink_read_sink_closed() {
    setup_tracing().await;
    let (sink_a, mut sink_b) = tokio::io::duplex(4096);
    let (pipe_to_client_push, _pipe_to_client_pull) = broadcast::channel(256);
    let (_client_to_pipe_push, client_to_pipe_pull) = async_channel::bounded(256);
    let cancel = CancellationToken::new();
    sink_b.shutdown().await.unwrap();
    let _pipe_loop_handle = tokio::spawn(sink_loop(
      sink_a,
      pipe_to_client_push.clone(),
      client_to_pipe_pull,
      cancel.clone(),
    ));
    assert!(timeout(Duration::from_secs(1), cancel.cancelled()).await.is_ok());
  }
}
