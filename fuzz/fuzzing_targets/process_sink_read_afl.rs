use afl::fuzz;
use bytes::{Bytes, BytesMut};
use serial_multiplexer_lib::common::{
  CONNECTION_BUFFER_SIZE, ConnectionState, SINK_COMPRESSION_BUFFER_SIZE, process_sink_read,
};
use serial_multiplexer_lib::test_utils::setup_tracing;
use tokio::io::{AsyncReadExt, duplex};

fn main() {
  let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

  fuzz!(|data: &[u8]| {
    if data.len() > SINK_COMPRESSION_BUFFER_SIZE {
      return;
    }

    let sink_buf = Bytes::copy_from_slice(data);

    runtime.block_on(async move {
      setup_tracing().await;
      let (mut target_stream, source_stream) = duplex(17usize.pow(2));
      tokio::spawn(async move {
        loop {
          let mut buf = BytesMut::zeroed(CONNECTION_BUFFER_SIZE);
          let _ = target_stream.read(&mut buf).await;
        }
      });
      let mut state = ConnectionState::new(0, source_stream);
      let _result = process_sink_read(&mut state, Ok(sink_buf)).await;
    });
  });
}
