use crate::protocol_utils::create_ack_datagram;
use crate::schema_generated::serial_multiplexer::{ControlCode, root_as_datagram};
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, broadcast};
use tokio::task::JoinHandle;
use tracing::Level;

static TRACING_SETUP: AtomicBool = AtomicBool::new(false);
static PERMIT: Semaphore = Semaphore::const_new(1);

pub async fn setup_tracing() {
  let _permit = PERMIT.acquire().await.unwrap();
  let Ok(false) = TRACING_SETUP.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
  else {
    return;
  };
  let subscriber = tracing_subscriber::fmt()
    .with_env_filter(format!("serial_multiplexer={}", Level::TRACE))
    .with_file(true)
    .with_line_number(true)
    .with_thread_ids(true)
    .with_target(false)
    .finish();
  let _ = tracing::subscriber::set_global_default(subscriber);
}

pub async fn run_echo() -> (SocketAddr, JoinHandle<()>) {
  let listener = TcpListener::bind("127.0.0.1:0")
    .await
    .expect("Echo listener should run");
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
              socket
                .write_all(&buf[..n])
                .await
                .expect("Write should succeed");
            }
          }
        }
      });
    }
  });
  (addr, listener_task)
}

pub fn receive_initial_ack_data(
  identifier: u64,
  initial_data: Bytes,
  client_to_pipe_pull: async_channel::Receiver<Bytes>,
  pipe_to_client_push: broadcast::Sender<Bytes>,
  data_datagram_contents: Bytes,
) -> JoinHandle<()> {
  tokio::spawn(async move {
    let initial = client_to_pipe_pull.recv().await.unwrap();
    let initial_datagram = root_as_datagram(&initial).unwrap();
    assert_eq!(initial_datagram.identifier(), identifier);
    assert_eq!(initial_datagram.code(), ControlCode::Initial);
    assert_eq!(initial_datagram.data().unwrap().bytes(), initial_data);
    let ack = create_ack_datagram(initial_datagram.identifier(), 0);
    pipe_to_client_push.send(ack).unwrap();
    let data = client_to_pipe_pull.recv().await.unwrap();
    let data_datagram = root_as_datagram(&data).unwrap();
    assert_eq!(data_datagram.identifier(), identifier);
    assert_eq!(data_datagram.code(), ControlCode::Data);
    assert_eq!(data_datagram.data().unwrap().bytes(), data_datagram_contents);
  })
}
