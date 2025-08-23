use afl::fuzz;
use bytes::{Bytes, BytesMut};
use serial_multiplexer_lib::common::{SINK_COMPRESSION_BUFFER_SIZE, handle_sink_read};

fn main() {
  let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

  fuzz!(|data: &[u8]| {
    if data.len() > serial_multiplexer_lib::common::CONNECTION_BUFFER_SIZE {
      return;
    }

    let mut sink_buf = BytesMut::from(data);
    let mut compression_buffer = BytesMut::zeroed(SINK_COMPRESSION_BUFFER_SIZE);
    let (sink_to_client_push, mut sink_to_client_pull) = async_broadcast::broadcast::<Bytes>(512);

    runtime.block_on(async move {
      tokio::spawn(async move { sink_to_client_pull.recv_direct().await });
      let _result =
        handle_sink_read(data.len(), &mut sink_buf, sink_to_client_push, &mut compression_buffer)
          .await;
    });
  });
}
