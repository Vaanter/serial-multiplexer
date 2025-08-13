pub mod common {
  use crate::common::{ConnectionState, sink_loop};
  use crate::configuration::{Guest, GuestMode, Host};
  use crate::guest::client_initiator;
  use crate::host::{ConnectionType, connection_initiator, run_listener};
  use crate::utils::create_upstream_listener;
  use anyhow::{Context, ensure};
  use bytes::Bytes;
  use futures::future::{JoinAll, MaybeDone, join_all, maybe_done};
  use futures::stream::FuturesUnordered;
  use std::collections::HashSet;
  use tokio::io::AsyncWriteExt;
  use tokio::sync::mpsc;
  use tokio::task;
  use tokio::task::JoinHandle;
  use tokio_serial::SerialPortBuilderExt;
  use tokio_util::sync::CancellationToken;
  use tracing::{debug, error, info};

  pub async fn create_host_tasks(
    mut properties: Host,
    cancel: CancellationToken,
  ) -> anyhow::Result<MaybeDone<JoinAll<JoinHandle<()>>>> {
    let (client_to_socket_push, client_to_socket_pull) = async_channel::bounded::<Bytes>(512);
    let (socket_to_client_push, socket_to_client_pull) = async_broadcast::broadcast::<Bytes>(512);
    let (connection_sender, connection_receiver) = mpsc::channel(512);

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
      .context("Failed to create unix socket based sink loop.")?;
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
      .await
      .context("Failed to create named pipe based sink loops.")?;
      ensure!(!pipe_loop_tasks.is_empty(), "All pipe based sink loops failed to start");
      sink_loops.extend(pipe_loop_tasks);
    }

    #[cfg(not(any(unix, windows)))]
    {
      anyhow::bail!("Host mode is only available on Linux and Windows!");
    }

    let tasks = FuturesUnordered::new();

    let initiator_task = tokio::spawn(connection_initiator(
      client_to_socket_push,
      socket_to_client_pull,
      connection_receiver,
      cancel.clone(),
    ));
    tasks.push(initiator_task);

    let listener_tasks = initialize_listeners(&mut properties, connection_sender, cancel)
      .await
      .context("Failed to initialize listeners")?;
    ensure!(!listener_tasks.is_empty(), "All listeners failed to start");

    Ok(maybe_done(join_all(tasks.into_iter().chain(listener_tasks).chain(sink_loops))))
  }

  pub async fn create_guest_tasks(
    properties: Guest,
    cancel: CancellationToken,
  ) -> anyhow::Result<MaybeDone<JoinAll<JoinHandle<()>>>> {
    debug!("Creating guest tasks");
    let (client_to_sink_push, client_to_sink_pull) = async_channel::bounded::<Bytes>(512);
    let (sink_to_client_push, sink_to_client_pull) = async_broadcast::broadcast::<Bytes>(512);

    let mut sink_loops = FuturesUnordered::new();
    match properties.guest_mode {
      GuestMode::Serial => {
        let serial_port_sinks =
          initialize_serials(&properties, sink_to_client_push, client_to_sink_pull, cancel.clone())
            .await
            .context("Failed to create serial port based sink loops.")?;
        sink_loops.extend(serial_port_sinks);
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
          .await
          .context("Failed to create unix socket based sink loop.")?;
          sink_loops.push(socket_task);
        }
        #[cfg(not(unix))]
        {
          anyhow::bail!("Unix socket guest mode is only available on Linux!");
        }
      }
    }

    let tasks = FuturesUnordered::new();

    let initiator_task =
      task::spawn(client_initiator(sink_to_client_pull, client_to_sink_push, cancel));
    tasks.push(initiator_task);

    Ok(maybe_done(join_all(tasks.into_iter().chain(sink_loops))))
  }

  /// Attempts to connect to serial port(s) and create a sink loop for each.
  ///
  /// This function will go through all the serial ports in [`Guest`] properties connecting to them.
  /// If connecting to any serial port fails, all other serial ports will be closed and this
  /// function will return an Error. Otherwise, if all connections succeed, a sink loop is created
  /// for each of them.
  ///
  /// # Parameters
  /// * `properties`: A reference to [`Guest`], a struct that contains the `serial_paths`
  ///   property, which specifies paths of the erial ports to which this function will connect.
  /// * `socket_to_client_push`: A [`broadcast::Sender<Bytes>`] used to send received datagrams to
  ///   client loops.
  /// * `client_to_socket_pull`: An [`async_channel::Receiver`], through which the sink loop
  ///   receives data sent by clients to be written to the sink.
  /// * `cancel`: A [`CancellationToken`] passed to the sink loop(s) used to signal when the loop(s)
  ///   should terminate.
  ///
  /// # Returns
  /// An [`anyhow::Result<FuturesUnordered<JoinHandle<()>>>`] with the spawned sink loop task(s)
  /// if successful, otherwise an error if connecting to any of the serial ports fails.
  pub async fn initialize_serials(
    properties: &Guest,
    sink_to_client_push: async_broadcast::Sender<Bytes>,
    client_to_sink_pull: async_channel::Receiver<Bytes>,
    cancel: CancellationToken,
  ) -> anyhow::Result<FuturesUnordered<JoinHandle<()>>> {
    debug!("Initializing serial ports");
    let distinct_paths: HashSet<&_> = properties.serial_paths.iter().collect();
    let mut serials_ok = Vec::with_capacity(distinct_paths.len());
    let mut serials_err = Vec::with_capacity(distinct_paths.len());
    for serial_path in distinct_paths.into_iter() {
      match tokio_serial::new(serial_path, properties.baud_rate).open_native_async() {
        Ok(serial) => {
          info!("Serial port {} is ready", &serial_path);
          serials_ok.push(serial);
        }
        Err(e) => {
          error!("Failed to connect to serial port '{}'. {e}", serial_path);
          serials_err.push(e);
        }
      }
    }

    if !serials_err.is_empty() {
      let mut error = anyhow::anyhow!("Failed to connect to all serial ports!");
      for serial_error in serials_err {
        error = error.context(serial_error);
      }
      for mut serial in serials_ok {
        if let Err(e) = serial.shutdown().await {
          error = error.context(format!("Failed to close serial port. {e:?}"));
        }
      }
      return Err(error);
    }

    let serial_tasks = FuturesUnordered::new();

    for serial in serials_ok {
      let serial_task = task::spawn(sink_loop(
        serial,
        sink_to_client_push.clone(),
        client_to_sink_pull.clone(),
        cancel.clone(),
      ));
      serial_tasks.push(serial_task);
    }
    Ok(serial_tasks)
  }

  /// Initializes network listeners from the specified [`Host`] properties.
  ///
  /// This function creates tasks via [`tokio::spawn`] for each network listener
  /// ([`Direct`] or [`Socks5`]) specified in the [`Host`] configuration.
  /// These listeners are responsible for handling incoming connections.
  ///
  /// # Parameters
  ///
  /// * `properties`: A mutable reference to [`Host`] that provides the addresses for the listeners
  /// * `connection_sender`: An [`mpsc::Sender`] channel where the connection information will be
  ///   sent when a client connects.
  /// * `cancel`: A [`CancellationToken`] to signal the listener loops to end due to application
  ///   shutdown.
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
  ) -> anyhow::Result<FuturesUnordered<JoinHandle<()>>> {
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
      .context(format!("Failed to setup listener at address '{}'", pair_direct.listener_address))?;
      listener_tasks.push(pair_task);
    }

    if let Some(ref socks5_proxy) = properties.socks5_proxy {
      let socks5_task =
        setup_listener(socks5_proxy, ConnectionType::Socks5, connection_sender, cancel)
          .await
          .context(format!("Failed to setup socks5 listener at address '{socks5_proxy}'"))?;
      listener_tasks.push(socks5_task);
    }

    Ok(listener_tasks)
  }

  /// Sets up a network listener for a given connection type and address
  /// and spawns a task to run the listener loop.
  ///
  /// # Parameters
  ///
  /// * `listener_address`: A string slice representing the address to bind the listener to,
  ///   will be resolved by [`tokio::net::lookup_host`].
  /// * `connection_type`: [`ConnectionType`] that specifies how the incoming connections should
  ///   be handled.
  /// * `connection_sender`:
  ///   An [`mpsc::Sender`] channel whereto will the new client connection be sent.
  /// * `cancel`: A [`CancellationToken`] used to signal cancellation of the listener operations.
  ///
  /// # Returns
  ///
  /// Returns an [`anyhow::Result<JoinHandle<()>>`] with the spawned listener task
  /// if the listener is successfully created, otherwise an error if the upstream listener fails
  /// to initialize, such as in the case of an invalid address or lack of permissions.
  async fn setup_listener(
    listener_address: &str,
    connection_type: ConnectionType,
    connection_sender: mpsc::Sender<(ConnectionState, ConnectionType)>,
    cancel: CancellationToken,
  ) -> anyhow::Result<JoinHandle<()>> {
    let listener = create_upstream_listener(listener_address).await?;
    match connection_type {
      ConnectionType::Socks5 => {
        info!("Created Socks5 listener at address '{}'", listener_address);
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

      let listener_task = initialize_listeners(&mut host, connection_sender, cancel).await.unwrap();
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
  use anyhow::{Error, bail};
  use bytes::Bytes;
  use futures::stream::FuturesUnordered;
  use std::collections::HashSet;
  use std::time::Duration;
  use tokio::io::AsyncWriteExt;
  use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
  use tokio::task::JoinHandle;
  use tokio::time::sleep;
  use tokio_util::sync::CancellationToken;
  use tracing::{debug, error, info};

  /// Attempts to connect to Windows named pipe(s) and set up a sink loop for each pipe.
  ///
  /// This function will go through all the pipes in [`Host`] properties connecting to them.
  /// As apparently connecting to a pipe can sometimes just fail, each connection is attempted up to
  /// 10 times. If connecting to any pipe fails, all other pipes will be closed and this
  /// function will return an Error. Otherwise, if all connections succeed, a sink loop is created
  /// for each of them.
  ///
  /// # Parameters:
  /// * `properties`: A reference to [`Host`], a struct that contains the `pipe_paths`
  ///   property, which specifies paths to the named pipes.
  /// * `pipe_to_client_push`: A [`broadcast::Sender<Bytes>`] used to send received datagrams to
  ///   client loops.
  /// * `client_to_pipe_pull`: An [`async_channel::Receiver`], through which the sink loop
  ///   receives data sent by clients to be written to the sink.
  /// * `cancel`: A [`CancellationToken`] passed to the sink loop(s) used to signal when the
  ///   loop(s) should terminate.
  ///
  /// # Returns:
  /// An [`anyhow::Result<FuturesUnordered<JoinHandle<()>>>`] with the spawned sink loop task(s)
  /// if successful, otherwise an error if connecting to any of the pipes fails.
  pub async fn create_windows_pipe_loops(
    properties: &Host,
    pipe_to_client_push: async_broadcast::Sender<Bytes>,
    client_to_pipe_pull: async_channel::Receiver<Bytes>,
    cancel: CancellationToken,
  ) -> anyhow::Result<FuturesUnordered<JoinHandle<()>>> {
    let mut pipes = Vec::new();
    let distinct_paths: HashSet<&String> = properties.pipe_paths.iter().collect();
    for pipe_path in distinct_paths.iter() {
      for _ in 0..10 {
        match prepare_pipe(pipe_path) {
          Ok(pipe) => {
            info!("Pipe {} is ready", &pipe_path);
            pipes.push(pipe);
            break;
          }
          Err(err) => {
            debug!("Failed to connect to pipe: {}", err);
            sleep(Duration::from_millis(100)).await;
          }
        }
      }
    }

    if pipes.len() != distinct_paths.len() {
      for mut pipe in pipes {
        if let Err(e) = pipe.shutdown().await {
          error!("Failed to close a pipe! {}", e);
        }
      }
      bail!("Failed to connect to all pipes!");
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
    Ok(pipe_loops)
  }

  /// Attempts to open a named pipe at the specified path for reading and writing
  fn prepare_pipe(pipe_path: &str) -> Result<NamedPipeClient, Error> {
    ClientOptions::new().write(true).read(true).open(pipe_path).map_err(|e| e.into())
  }

  #[cfg(test)]
  mod tests {
    use super::*;
    use crate::test_utils::setup_tracing;
    use tokio::net::windows::named_pipe::ServerOptions;
    use tokio::time::timeout;

    #[tokio::test]
    async fn create_windows_pipe_loops_non_existent_test() {
      setup_tracing().await;
      let (sink_to_client_push, _sink_to_client_pull) = async_broadcast::broadcast(256);
      let (_client_to_sink_push, client_to_sink_pull) = async_channel::bounded(256);
      let cancel = CancellationToken::new();
      let host: Host = Host {
        pipe_paths: vec!["non_existent_pipe".to_string()],
        ..Default::default()
      };

      let result =
        create_windows_pipe_loops(&host, sink_to_client_push, client_to_sink_pull, cancel.clone())
          .await;
      assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_windows_pipe_loops_multiple_test() {
      setup_tracing().await;
      let pipe1_path = "\\\\.\\pipe\\test_pipe_multiplexer_1";
      let pipe2_path = "\\\\.\\pipe\\test_pipe_multiplexer_2";
      let pipe3_path = "\\\\.\\pipe\\test_pipe_multiplexer_3";
      let pipe1 = ServerOptions::new().create(pipe1_path).unwrap();
      let pipe2 = ServerOptions::new().create(pipe2_path).unwrap();
      let pipe3 = ServerOptions::new().create(pipe3_path).unwrap();
      let server_task = tokio::spawn(async move {
        pipe1.connect().await.unwrap();
        pipe2.connect().await.unwrap();
        pipe3.connect().await.unwrap();
        pipe1.disconnect().unwrap();
        pipe2.disconnect().unwrap();
        pipe3.disconnect().unwrap();
      });

      let (sink_to_client_push, _sink_to_client_pull) = async_broadcast::broadcast(256);
      let (_client_to_sink_push, client_to_sink_pull) = async_channel::bounded(256);
      let cancel = CancellationToken::new();
      let host: Host = Host {
        pipe_paths: vec![pipe1_path.to_string(), pipe2_path.to_string(), pipe3_path.to_string()],
        ..Default::default()
      };

      let result =
        create_windows_pipe_loops(&host, sink_to_client_push, client_to_sink_pull, cancel.clone())
          .await;
      assert!(result.is_ok());

      timeout(Duration::from_secs(3), server_task).await.unwrap().unwrap();
    }
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
  use tokio::task::JoinHandle;
  use tokio_util::sync::CancellationToken;
  use tracing::debug;

  /// A shorthand function to create a task running a sink loop with a Unix socket as the sink.
  ///
  /// Check [`sink_loop`] documentation.
  fn create_unix_socket_loop(
    unix_socket: UnixStream,
    socket_to_client_push: async_broadcast::Sender<Bytes>,
    client_to_socket_pull: async_channel::Receiver<Bytes>,
    cancel: CancellationToken,
  ) -> JoinHandle<()> {
    tokio::spawn(sink_loop(unix_socket, socket_to_client_push, client_to_socket_pull, cancel))
  }

  /// Attempts to connect to a Unix socket and set up a sink loop.
  ///
  /// This function tries to connect to a Unix socket specified in the guest properties, panicking
  /// if it fails to do so. After the connection is successful, a sink loop task is spawned using
  /// [`create_unix_socket_loop`].
  ///
  /// # Parameters
  /// * `properties`: A reference to [`Guest`], a struct that contains the `socket_path`
  ///    property, which specifies a path of the Unix socket to which this function will connect.
  /// * `socket_to_client_push`: A [`broadcast::Sender<Bytes>`] used to send received datagrams to
  ///    client loops.
  /// * `client_to_socket_pull`: An [`async_channel::Receiver`], through which the sink loop
  ///    receives data sent by clients to be written to the sink.
  /// * `cancel`: A [`CancellationToken`] passed to the sink loop used to signal when the loop
  ///    should terminate.
  ///
  /// # Returns:
  /// An [`anyhow::Result<JoinHandle<()>>`] with the spawned sink loop task if successful,
  /// otherwise an error if connecting to the Unix socket fails.
  pub async fn connect_to_unix_socket(
    properties: &Guest,
    socket_to_client_push: async_broadcast::Sender<Bytes>,
    client_to_socket_pull: async_channel::Receiver<Bytes>,
    cancel: CancellationToken,
  ) -> anyhow::Result<JoinHandle<()>> {
    debug!("Attempting to connect to a unix socket at {}", &properties.socket_path);
    let unix_socket =
      UnixStream::connect(&properties.socket_path).await.context("Failed to connect to socket")?;
    debug!(peer_address = ?unix_socket.peer_addr()?, "Connected to unix socket");
    Ok(create_unix_socket_loop(unix_socket, socket_to_client_push, client_to_socket_pull, cancel))
  }

  /// Creates and listens on a Unix socket, then creates a sink loop when a client connects.
  ///
  /// This function deletes a file specified in the host properties (if it exists) and then
  /// binds a new [`UnixListener`] on the path.
  /// After a client connects, a sink loop task is spawned using [`create_unix_socket_loop`].
  ///
  /// # Parameters:
  /// * `properties`: A reference to [`Host`], a struct that contains the `socket_path`
  ///    property, which specifies a path where the Unix socket will be created.
  /// * `socket_to_client_push`: A [`broadcast::Sender<Bytes>`] used to send received datagrams to
  ///    client loops.
  /// * `client_to_socket_pull`: An [`async_channel::Receiver`], through which the sink loop
  ///    receives data sent by clients to be written to the sink.
  /// * `cancel`: A [`CancellationToken`] passed to the sink loop used to signal when the
  ///    loop should terminate.
  ///
  /// # Returns:
  /// An [`anyhow::Result<JoinHandle<()>>`] with the spawned sink loop task if successful,
  /// otherwise an error if:
  ///   * Removing an existing socket file fails
  ///   * The socket fails to bind to the provided `socket_path`
  ///   * The connected client does not have an address
  pub async fn listen_accept_unix_connection(
    properties: &Host,
    socket_to_client_push: async_broadcast::Sender<Bytes>,
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
  mod tests {
    use super::*;
    use crate::configuration::GuestMode;
    use crate::test_utils::setup_tracing;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
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

      let (sink_to_client_push, _sink_to_client_pull) = async_broadcast::broadcast(256);
      let (_client_to_sink_push, client_to_sink_pull) = async_channel::bounded(256);
      let cancel = CancellationToken::new();

      timeout(Duration::from_secs(3), async {
        let host_loop = tokio::spawn({
          let cancel = cancel.clone();
          let sink_to_client_push = sink_to_client_push.clone();
          let client_to_sink_pull = client_to_sink_pull.clone();
          async move {
            let sink_loop = listen_accept_unix_connection(
              &host,
              sink_to_client_push,
              client_to_sink_pull,
              cancel.clone(),
            )
            .await
            .unwrap();
            cancel.cancelled().await;
            sink_loop.await.unwrap();
          }
        });

        // HACK should wait until the socket is actually bound
        sleep(Duration::from_secs(1)).await;
        let sink_loop =
          connect_to_unix_socket(&guest, sink_to_client_push, client_to_sink_pull, cancel.clone())
            .await;
        cancel.cancel();
        host_loop.await.unwrap();
        sink_loop.unwrap().await.unwrap();
      })
      .await
      .unwrap();
    }
  }
}
