use crate::protocol_utils::create_ack_datagram;
use crate::schema_generated::serial_multiplexer::{ControlCode, root_as_datagram};
use bytes::{Bytes, BytesMut};
use std::io::{stderr, stdout};
use std::net::SocketAddr;
use std::sync::LazyLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::Level;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

static TRACING_LOCK: LazyLock<()> = LazyLock::new(|| {
  let subscriber1 = tracing_subscriber::fmt::Layer::default()
    .with_writer(stdout)
    .with_file(false)
    .with_line_number(false)
    .with_thread_ids(true)
    .with_target(false)
    .with_filter(EnvFilter::new(format!("serial_multiplexer={}", Level::TRACE)));
  let subscriber2 = tracing_subscriber::fmt::Layer::default()
    .with_writer(stderr)
    .with_file(true)
    .with_line_number(true)
    .with_thread_ids(true)
    .with_target(true)
    .with_filter(EnvFilter::new(format!("serial_multiplexer={}", Level::TRACE)));
  Registry::default().with(subscriber1).with(subscriber2).init();
});

pub fn setup_tracing() {
  LazyLock::force(&TRACING_LOCK);
}

pub async fn run_echo() -> (SocketAddr, JoinHandle<()>) {
  let listener = TcpListener::bind("127.0.0.1:0").await.expect("Echo listener should run");
  let addr = listener.local_addr().expect("Echo listener local_addr");
  let listener_task = tokio::spawn(async move {
    loop {
      let (mut socket, _) = listener.accept().await.expect("Accept failed");
      tokio::spawn(async move {
        let mut buf = BytesMut::zeroed(4096);
        loop {
          match socket.read(&mut buf).await.expect("Read should succeed") {
            0 => break,
            n => {
              socket.write_all(&buf[..n]).await.expect("Write should succeed");
            }
          }
        }
      });
    }
  });
  (addr, listener_task)
}

pub async fn receive_initial_ack_data(
  identifier: u64,
  initial_data: Bytes,
  client_to_sink_pull: async_channel::Receiver<Bytes>,
  sink_to_client_push: async_broadcast::Sender<Bytes>,
  data_datagram_contents: Bytes,
) -> JoinHandle<()> {
  tokio::spawn(async move {
    let initial = client_to_sink_pull.recv().await.unwrap();
    let initial_datagram = root_as_datagram(&initial).unwrap();
    assert_eq!(initial_datagram.identifier(), identifier);
    assert_eq!(initial_datagram.code(), ControlCode::Initial);
    assert_eq!(initial_datagram.data().unwrap().bytes(), initial_data);
    let ack = create_ack_datagram(initial_datagram.identifier(), 0, 0);
    sink_to_client_push.broadcast_direct(ack).await.unwrap();
    let data = client_to_sink_pull.recv().await.unwrap();
    let data_datagram = root_as_datagram(&data).unwrap();
    assert_eq!(data_datagram.identifier(), identifier);
    assert_eq!(data_datagram.code(), ControlCode::Data);
    assert_eq!(data_datagram.data().unwrap().bytes(), data_datagram_contents);
  })
}
