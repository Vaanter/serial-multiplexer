pub mod common {
  use crate::common::{ConnectionState, sink_loop};
  use crate::configuration::{Guest, GuestMode, Host};
  use crate::guest::client_initiator;
  use crate::host::{ConnectionType, connection_initiator, run_listener};
  use crate::utils::create_upstream_listener;
  use bytes::Bytes;
  use futures::future::{JoinAll, MaybeDone, join_all, maybe_done};
  use futures::stream::FuturesUnordered;
  use std::collections::HashSet;
  use tokio::sync::{broadcast, mpsc};
  use tokio::task;
  use tokio::task::JoinHandle;
  use tokio_serial::SerialPortBuilderExt;
  use tokio_util::sync::CancellationToken;
  use tracing::{debug, info};

  pub async fn create_host_tasks(
    mut properties: Host,
    cancel: CancellationToken,
  ) -> MaybeDone<JoinAll<JoinHandle<()>>> {
    let (client_to_socket_push, client_to_socket_pull) = async_channel::bounded::<Bytes>(128);
    let (socket_to_client_push, socket_to_client_pull) = broadcast::channel::<Bytes>(128);
    let (connection_sender, connection_receiver) = mpsc::channel(128);

    let mut sink_loops = FuturesUnordered::new();
    #[cfg(unix)]
    {
      use crate::runner::linux::accept_unix_connection;
      let socket_task = accept_unix_connection(
        &properties,
        socket_to_client_push,
        client_to_socket_pull,
        cancel.clone(),
      )
      .await;
      sink_loops.extend([socket_task]);
    }

    #[cfg(windows)]
    {
      use crate::runner::windows::create_windows_pipe_loops;
      let pipe_loop_tasks = create_windows_pipe_loops(
        &properties,
        socket_to_client_push,
        client_to_socket_pull,
        cancel.clone(),
      )
      .await;
      assert!(!pipe_loop_tasks.is_empty(), "All pipe loops failed to start");
      sink_loops.extend(pipe_loop_tasks);
    }

    let tasks = FuturesUnordered::new();

    let initiator_task = tokio::spawn(connection_initiator(
      client_to_socket_push,
      socket_to_client_pull,
      connection_receiver,
      cancel.clone(),
    ));
    tasks.push(initiator_task);

    let listener_tasks = initialize_listeners(&mut properties, connection_sender, cancel).await;
    assert!(!listener_tasks.is_empty(), "All listeners failed to start");

    maybe_done(join_all(tasks.into_iter().chain(listener_tasks).chain(sink_loops)))
  }

  pub async fn create_guest_tasks(
    properties: Guest,
    cancel: CancellationToken,
  ) -> MaybeDone<JoinAll<JoinHandle<()>>> {
    debug!("Creating guest tasks");
    let (client_to_sink_push, client_to_sink_pull) = async_channel::bounded::<Bytes>(128);
    let (sink_to_client_push, sink_to_client_pull) = broadcast::channel::<Bytes>(128);

    let mut sink_loops = FuturesUnordered::new();
    match properties.guest_mode {
      GuestMode::Serial => {
        sink_loops.extend(
          initialize_serials(&properties, sink_to_client_push, client_to_sink_pull, cancel.clone())
            .await,
        );
      }
      GuestMode::UnixSocket => {
        #[cfg(unix)]
        {
          use crate::runner::linux::connect_to_unix_socket;
          let socket_task = connect_to_unix_socket(
            &properties,
            sink_to_client_push,
            client_to_sink_pull,
            cancel.clone(),
          )
          .await;
          sink_loops.push(socket_task);
        }
        #[cfg(windows)]
        {
          panic!("Unix socket guest mode is not available on Windows!");
        }
      }
    }

    let tasks = FuturesUnordered::new();

    let initiator_task =
      task::spawn(client_initiator(sink_to_client_pull, client_to_sink_push, cancel));
    tasks.push(initiator_task);

    maybe_done(join_all(tasks.into_iter().chain(sink_loops)))
  }

  pub async fn initialize_serials(
    properties: &Guest,
    sink_to_client_push: broadcast::Sender<Bytes>,
    client_to_sink_pull: async_channel::Receiver<Bytes>,
    cancel: CancellationToken,
  ) -> FuturesUnordered<JoinHandle<()>> {
    debug!("Initializing serials");
    let mut serials = Vec::new();
    let distinct_paths: HashSet<&_> = properties.serial_paths.iter().collect();
    for serial_path in distinct_paths {
      let serial = tokio_serial::new(serial_path, properties.baud_rate)
        .open_native_async()
        .expect("Failed to open serial port");
      info!("Serial port {} is ready", &serial_path);
      serials.push(serial);
    }

    let serial_tasks = FuturesUnordered::new();

    for serial in serials {
      let serial_task = task::spawn(sink_loop(
        serial,
        sink_to_client_push.clone(),
        client_to_sink_pull.clone(),
        cancel.clone(),
      ));
      serial_tasks.push(serial_task);
    }
    serial_tasks
  }

  pub async fn initialize_listeners(
    properties: &mut Host,
    connection_sender: mpsc::Sender<(ConnectionState, ConnectionType)>,
    cancel: CancellationToken,
  ) -> FuturesUnordered<JoinHandle<()>> {
    debug!("Initializing listeners");
    let listener_tasks = FuturesUnordered::new();
    while let Some(pair_direct) = properties.address_pairs.pop() {
      let cancel = cancel.clone();
      let connection_sender = connection_sender.clone();
      let pair_task = setup_listener(
        &pair_direct.listener_address,
        ConnectionType::new_direct(pair_direct.target_address),
        connection_sender,
        cancel,
      )
      .await
      .unwrap_or_else(|e| {
        panic!("Failed to setup listener for {}: {}", pair_direct.listener_address, e)
      });
      listener_tasks.push(pair_task);
    }

    if let Some(ref socks5_proxy) = properties.socks5_proxy {
      let socks5_task =
        setup_listener(socks5_proxy, ConnectionType::Socks5, connection_sender, cancel)
          .await
          .unwrap_or_else(|e| panic!("Failed to setup socks5 listener at {socks5_proxy}: {e}"));
      listener_tasks.push(socks5_task);
    }

    listener_tasks
  }

  async fn setup_listener(
    listener_address: &str,
    connection_type: ConnectionType,
    connection_sender: mpsc::Sender<(ConnectionState, ConnectionType)>,
    cancel: CancellationToken,
  ) -> anyhow::Result<JoinHandle<()>> {
    let listener = create_upstream_listener(listener_address).await?;
    match connection_type {
      ConnectionType::Socks5 => {
        info!("Created Socks5 listener at {}", listener_address);
      }
      ConnectionType::Direct { ref target_address } => {
        info!("Created listener at {} for {}", listener_address, target_address);
      }
    }

    Ok(tokio::spawn(run_listener(listener, connection_type, connection_sender, cancel)))
  }
}

#[cfg(windows)]
mod windows {
  use crate::common::sink_loop;
  use crate::configuration::Host;
  use anyhow::Error;
  use bytes::Bytes;
  use futures::stream::FuturesUnordered;
  use std::collections::HashSet;
  use std::time::Duration;
  use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
  use tokio::sync::broadcast;
  use tokio::task::JoinHandle;
  use tokio::time::sleep;
  use tokio_util::sync::CancellationToken;
  use tracing::{debug, info};

  pub async fn create_windows_pipe_loops(
    properties: &Host,
    pipe_to_client_push: broadcast::Sender<Bytes>,
    client_to_pipe_pull: async_channel::Receiver<Bytes>,
    cancel: CancellationToken,
  ) -> FuturesUnordered<JoinHandle<()>> {
    let mut pipes = Vec::new();
    let mut retry_count = 0;
    let distinct_paths: HashSet<&String> = properties.pipe_paths.iter().collect();
    for pipe_path in distinct_paths {
      loop {
        match prepare_pipe(pipe_path) {
          Ok(pipe) => {
            info!("Pipe {} is ready", &pipe_path);
            pipes.push(pipe);
            break;
          }
          Err(err) => {
            debug!("Failed to connect to pipe: {}", err);
            assert!(retry_count < 10, "Failed to prepare pipe {} after 10 attempts", &pipe_path);
            sleep(Duration::from_millis(100)).await;
            retry_count += 1;
          }
        }
      }
    }

    let pipe_loops = FuturesUnordered::new();
    for pipe in pipes {
      let cancel = cancel.clone();
      let pipe_to_client_push = pipe_to_client_push.clone();
      let client_to_pipe_pull = client_to_pipe_pull.clone();
      let pipe_task =
        tokio::spawn(sink_loop(pipe, pipe_to_client_push, client_to_pipe_pull, cancel));
      pipe_loops.push(pipe_task);
    }
    pipe_loops
  }

  fn prepare_pipe(pipe_path: &str) -> Result<NamedPipeClient, Error> {
    ClientOptions::new().write(true).read(true).open(pipe_path).map_err(|e| e.into())
  }
}

#[cfg(unix)]
mod linux {
  use crate::common::sink_loop;
  use crate::configuration::{Guest, Host};
  use bytes::Bytes;
  use std::fs::remove_file;
  use std::io::ErrorKind;
  use tokio::net::{UnixListener, UnixStream};
  use tokio::sync::broadcast;
  use tokio::task::JoinHandle;
  use tokio_util::sync::CancellationToken;
  use tracing::debug;

  fn create_unix_socket_loop(
    unix_socket: UnixStream,
    socket_to_client_push: broadcast::Sender<Bytes>,
    client_to_socket_pull: async_channel::Receiver<Bytes>,
    cancel: CancellationToken,
  ) -> JoinHandle<()> {
    tokio::spawn(sink_loop(unix_socket, socket_to_client_push, client_to_socket_pull, cancel))
  }

  pub async fn connect_to_unix_socket(
    properties: &Guest,
    socket_to_client_push: broadcast::Sender<Bytes>,
    client_to_socket_pull: async_channel::Receiver<Bytes>,
    cancel: CancellationToken,
  ) -> JoinHandle<()> {
    debug!("Attempting to connect to a unix socket at {}", &properties.socket_path);
    let unix_socket =
      UnixStream::connect(&properties.socket_path).await.expect("Failed to connect to socket");
    debug!(peer_address = ?unix_socket.peer_addr().unwrap(), "Connected to unix socket");
    create_unix_socket_loop(unix_socket, socket_to_client_push, client_to_socket_pull, cancel)
  }

  pub async fn accept_unix_connection(
    properties: &Host,
    socket_to_client_push: broadcast::Sender<Bytes>,
    client_to_socket_pull: async_channel::Receiver<Bytes>,
    cancel: CancellationToken,
  ) -> JoinHandle<()> {
    if let Err(e) = remove_file(&properties.socket_path)
      && e.kind() != ErrorKind::NotFound
    {
      panic!("Failed to remove socket file: {}", e);
    }
    debug!("Listening for a unix socket connection on {}", &properties.socket_path);
    let socket_listener =
      UnixListener::bind(&properties.socket_path).expect("Failed to bind to socket");
    let (unix_socket, _) =
      socket_listener.accept().await.expect("Failed to accept socket connection");
    debug!(peer_address = ?unix_socket.peer_addr().unwrap(), "Accepted unix socket connection");
    create_unix_socket_loop(unix_socket, socket_to_client_push, client_to_socket_pull, cancel)
  }
}
