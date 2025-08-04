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
      use crate::runner::linux::listen_accept_unix_connection;
      let socket_task = listen_accept_unix_connection(
        &properties,
        socket_to_client_push,
        client_to_socket_pull,
        cancel.clone(),
      )
      .await
      .unwrap_or_else(|e| panic!("Failed to create unix socket based sink loop"));
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

  /// Initializes network listeners from the specified [`Host`] properties.
  ///
  /// This function creates asynchronous tasks for each network listener ([`Direct`] or [`Socks5`])
  /// specified in the [`Host`] configuration.
  /// These listeners are responsible for handling incoming connections.
  ///
  /// # Parameters
  ///
  /// - `properties`: A mutable reference to [`Host`] that provides the addresses for the listeners
  /// - `connection_sender`: An [`mpsc::Sender`] channel where the connection information will be
  ///   sent when a client connects
  /// - `cancel`: A [`CancellationToken`] to signal the listener loops to end due to application
  ///   shutdown
  ///
  /// # Returns
  ///
  /// A [`FuturesUnordered<JoinHandle<()>>`] containing the set of tasks created for listeners.
  /// Each `JoinHandle` represents an asynchronous task with the listener loop.
  ///
  /// # Panics
  ///
  /// Failing to create a listener will cause a panic.
  ///
  /// [`Direct`]: ConnectionType::Direct
  /// [`Socks5`]: ConnectionType::Socks5
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

  /// Sets up a network listener for a given connection type and address
  /// and spawns an asynchronous task to handle incoming connections.
  ///
  /// # Parameters
  ///
  /// * `listener_address` - A string slice representing the address to bind the listener to,
  ///   will be resolved by [`tokio::net::lookup_host`].
  /// * `connection_type` - [`ConnectionType`] that specifies how the connection should be created.
  /// * `connection_sender` -
  ///   a [`mpsc::Sender`] channel whereto will the new client connection be sent.
  /// * `cancel` - A [`CancellationToken`] used to signal cancellation of the listener operations.
  ///
  /// # Returns
  ///
  /// Returns an [`anyhow::Result`] with a [`JoinHandle`]
  /// of the spawned listener task if the listener is successfully created.
  ///
  /// * On success: A [`JoinHandle`] that represents the spawned task for managing the listener.
  /// * On failure: An [`anyhow::Error`] if the listener could not be created.
  ///
  /// # Behaviour
  ///
  /// * This function creates a TCP listener bound to the specified `listener_address`.
  /// * Spawns a [`task`] that runs the listener loop using [`run_listener`].
  ///
  /// # Errors
  ///
  /// This function returns an error if the upstream listener fails to initialize,
  /// such as in the case of an invalid address or lack of permissions.
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

  #[cfg(test)]
  mod tests {
    use crate::configuration::{AddressPair, Host};
    use crate::host::ConnectionType;
    use crate::runner::common::initialize_listeners;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_initialize_listeners() {
      let cancel = CancellationToken::new();
      let (connection_sender, mut connection_receiver) = mpsc::channel(128);
      let address_pairs = vec![
        AddressPair {
          listener_address: "127.0.0.1:2000".to_string(),
          target_address: "127.0.0.1:3000".to_string(),
        },
        AddressPair {
          listener_address: "127.0.0.1:2001".to_string(),
          target_address: "127.0.0.1:3001".to_string(),
        },
      ];
      let mut host = Host {
        socks5_proxy: Some("127.0.0.1:5000".to_string()),
        address_pairs: address_pairs.clone(),
        ..Default::default()
      };

      let listener_task = initialize_listeners(&mut host, connection_sender, cancel).await;
      assert_eq!(listener_task.len(), 3);

      let mut connection_id = 1;
      for address_pair in address_pairs.iter() {
        let client = TcpStream::connect(&address_pair.listener_address).await.unwrap();
        let (state, connection_type) = connection_receiver.recv().await.unwrap();
        assert_eq!(state.identifier, connection_id);
        assert_eq!(state.client.peer_addr().unwrap(), client.local_addr().unwrap());
        assert_eq!(
          connection_type,
          ConnectionType::Direct {
            target_address: address_pair.target_address.clone()
          }
        );
        connection_id += 1;
      }
      let socks5_client = TcpStream::connect(&host.socks5_proxy.unwrap()).await.unwrap();
      let (state, connection_type) = connection_receiver.recv().await.unwrap();
      assert_eq!(state.identifier, connection_id);
      assert_eq!(state.client.peer_addr().unwrap(), socks5_client.local_addr().unwrap());
      assert_eq!(connection_type, ConnectionType::Socks5);
    }
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
  use anyhow::{Context, bail};
  use bytes::Bytes;
  use std::fs::remove_file;
  use std::io::ErrorKind;
  use tokio::net::{UnixListener, UnixStream};
  use tokio::sync::broadcast;
  use tokio::task::JoinHandle;
  use tokio_util::sync::CancellationToken;
  use tracing::debug;

  /// A shorthand function to create a task running a sink loop with a Unix socket as the sink.
  ///
  /// Check [`sink_loop`] documentation.
  fn create_unix_socket_loop(
    unix_socket: UnixStream,
    socket_to_client_push: broadcast::Sender<Bytes>,
    client_to_socket_pull: async_channel::Receiver<Bytes>,
    cancel: CancellationToken,
  ) -> JoinHandle<()> {
    tokio::spawn(sink_loop(unix_socket, socket_to_client_push, client_to_socket_pull, cancel))
  }

  /// Attempts to connect to a Unix socket and sets up a sink loop.
  ///
  /// This function tries to connect to a Unix socket specified in the guest properties, panicking
  /// if it fails to do so. After the connection is successful, a sink loop task is spawned using
  /// [`create_unix_socket_loop`].
  ///
  /// # Parameters
  /// * `properties`: A reference to [`Guest`], a struct that contains the `socket_path`
  //     property, which specifies a path of the Unix socket to which this function will connect.
  /// * `socket_to_client_push`: A [`broadcast::Sender<Bytes>`] used to send received datagrams to
  //     client loops.
  /// - `client_to_socket_pull`: An [`async_channel::Receiver`], through which the sink loop
  ///    receives data sent by clients to be written to the sink.
  /// - `cancel`: [`CancellationToken`]: A [`CancellationToken`] passed to the sink loop
  ///    used to signal when the loop should terminate.
  ///
  /// # Returns:
  /// A [`JoinHandle<()>`] with the spawned sink loop task.
  ///
  /// # Panics
  /// This function panics will panic if it fails to connect to the Unix socket.
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

  /// Creates and listens on a Unix socket, then creates a sink loop when a client connects.
  ///
  /// This function deletes a file specified in the host properties (if it exists) and then
  /// binds a new [`UnixListener`] on the path.
  /// After a client connects, a sink loop task is spawned using [`create_unix_socket_loop`].
  ///
  /// # Parameters:
  /// - `properties`: A reference to [`Host`], a struct that contains the `socket_path`
  ///    property, which specifies a path where the Unix socket will be created.
  /// - `socket_to_client_push`: A [`broadcast::Sender<Bytes>`] used to send received datagrams to
  ///    client loops.
  /// - `client_to_socket_pull`: An [`async_channel::Receiver`], through which the sink loop
  ///    receives data sent by clients to be written to the sink.
  /// - `cancel`: A [`CancellationToken`] passed to the sink loop used to signal when the
  ///    loop should terminate.
  ///
  /// # Returns:
  /// An [`anyhow::Result<JoinHandle<()>>`] with the spawned sink loop task if successful,
  /// Otherwise an error, if:
  ///   - Removing an existing socket file fails
  ///   - The socket fails to bind to the provided `socket_path`
  ///   - The connected client does not have an address
  pub async fn listen_accept_unix_connection(
    properties: &Host,
    socket_to_client_push: broadcast::Sender<Bytes>,
    client_to_socket_pull: async_channel::Receiver<Bytes>,
    cancel: CancellationToken,
  ) -> anyhow::Result<JoinHandle<()>> {
    if let Err(e) = remove_file(&properties.socket_path)
      && e.kind() != ErrorKind::NotFound
    {
      bail!("Failed to remove socket file: {}", e);
    }
    debug!("Listening for a unix socket connection on {}", &properties.socket_path);
    let socket_listener =
      UnixListener::bind(&properties.socket_path).context("Failed to bind to socket")?;
    let (unix_socket, _) =
      socket_listener.accept().await.context("Failed to accept socket connection")?;
    debug!(peer_address = ?unix_socket.peer_addr()?, "Accepted unix socket connection");
    Ok(create_unix_socket_loop(unix_socket, socket_to_client_push, client_to_socket_pull, cancel))
  }

  #[cfg(test)]
  mod test {
    use super::*;
    use crate::configuration::GuestMode;
    use crate::test_utils::setup_tracing;
    use std::time::Duration;
    use tokio::time::timeout;

    async fn listen_connect_accept_send_test() {
      setup_tracing().await;
      let socket_path = "test_socket.sock";
      let host = Host {
        socket_path: socket_path.to_string(),
        address_pairs: vec![],
        socks5_proxy: None,
      };
      let guest = Guest {
        serial_paths: vec![],
        socket_path: socket_path.to_string(),
        guest_mode: GuestMode::Serial,
        baud_rate: 0,
      };

      let (sink_to_client_push, _sink_to_client_pull) = broadcast::channel(256);
      let (_client_to_sink_push, client_to_sink_pull) = async_channel::bounded(256);
      let cancel = CancellationToken::new();

      timeout(Duration::from_secs(3), async {
        let host_loop = tokio::spawn({
          let cancel = cancel.clone();
          async move {
            let sink_loop = listen_accept_unix_connection(
              &host,
              sink_to_client_push,
              client_to_sink_pull,
              cancel,
            )
            .await
            .unwrap();
            cancel.cancelled().await;
          }
        });

        let sink_loop =
          connect_to_unix_socket(&guest, sink_to_client_push, client_to_sink_pull, cancel.clone())
            .await;
        cancel.cancel();
      })
      .await
      .unwrap();
    }
  }
}
