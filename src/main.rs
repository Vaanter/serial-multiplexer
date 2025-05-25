use crate::common::ConnectionState;
use crate::guest::client_initiator;
use crate::host::{ConnectionType, connection_initiator, run_listener};
use crate::utils::create_upstream_listener;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use common::sink_loop;
use config::Config;
use futures::future::{JoinAll, MaybeDone, join_all, maybe_done};
use futures::stream::FuturesUnordered;
use std::fs::OpenOptions;
use std::time::Duration;
use tokio::signal::ctrl_c;
use tokio::sync::{broadcast, mpsc};
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_serial::SerialPortBuilderExt;
use tokio_util::sync::CancellationToken;
use tracing::{Level, debug, error, info, trace};
use tracing_attributes::instrument;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

mod common;
mod guest;
mod host;
mod protocol_utils;
#[allow(unsafe_op_in_unsafe_fn, unused)]
mod schema_generated;
#[cfg(test)]
mod test_utils;
mod utils;

#[derive(Parser, Debug)]
#[clap(version, about, author)]
struct Args {
  /// Mode in which the program will run. Either host or guest
  #[clap(subcommand)]
  command: Commands,
  /// Logging verbosity, default is WARN, each repetition increases the logging level.
  /// 1 = INFO, 2 = DEBUG, 3+ = TRACE
  #[clap(short, long, default_value = "0", action = clap::ArgAction::Count)]
  verbose: u8,
  /// Path to the config file
  #[clap(short, long, default_value = "config.toml")]
  config: String,
}

#[derive(Subcommand, Debug)]
enum Commands {
  /// Initializes the application in host mode to listen on configured network addresses.
  /// Requires a functional pipe connection from VirtualBox.
  /// Note: This mode is Windows-exclusive.
  Host {
    #[arg(short, long, required = true)]
    pipe_path: Vec<String>,
  },
  /// Initializes the application in guest mode awaiting data from serial port.
  /// Requires a serial port.
  Guest {
    /// Path to a serial port file. On linux this will likely be a /dev/ttyS0 - 3
    #[arg(short, long, required = true)]
    serial_path: Vec<String>,
    #[arg(short, long, default_value = "115200")]
    baud_rate: u32,
  },
}

struct AddressPair {
  listener_address: String,
  target_address: String,
}

fn main() {
  let args = Args::parse();
  let config = Config::builder()
    .add_source(config::File::with_name(&args.config))
    // Add in settings from the environment (with a prefix of SEMUL)
    .add_source(config::Environment::with_prefix("SEMUL"))
    .build()
    .expect("Failed to load config");

  let (writer, _guard) = if let Ok(log_file_name) = config.get_string("log_file") {
    let mut log_file_options = OpenOptions::new();
    log_file_options.write(true).truncate(true).create(true);
    let log_file = log_file_options
      .open(log_file_name)
      .expect("Log file should be accessible");
    tracing_appender::non_blocking(log_file)
  } else {
    tracing_appender::non_blocking(std::io::stdout())
  };

  let tracing_filter = config.get_string("tracing_filter").ok();

  let fmt_layer = tracing_subscriber::fmt::Layer::default()
    .with_writer(writer)
    .with_file(false)
    .with_ansi(config.get_string("log_file").is_err())
    .with_line_number(false)
    .with_thread_ids(true)
    .with_target(false)
    .with_filter(build_filter(tracing_filter, args.verbose));

  Registry::default().with(fmt_layer).init();
  debug!("args: {:?}", args);
  debug!("config: {:?}", config);

  tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()
    .expect("Setting up runtime must succeed")
    .block_on(async {
      match args.command {
        Commands::Guest {
          serial_path,
          baud_rate,
        } => {
          run_guest(serial_path, baud_rate).await;
        }
        Commands::Host { pipe_path } => {
          let direct_connections = load_direct_connections(&config);
          let socks5_proxy = config.get_string("socks5_proxy").ok();
          assert!(
            !(direct_connections.is_empty() && socks5_proxy.is_none()),
            "No address pairs or socks5 proxy configured"
          );
          run_host(pipe_path, direct_connections, socks5_proxy).await;
        }
      }
    });
}

fn build_filter(filter_string: Option<String>, verbosity: u8) -> EnvFilter {
  let level = match verbosity {
    1 => Level::INFO,
    2 => Level::DEBUG,
    n if n > 2 => Level::TRACE,
    _ => Level::WARN,
  };
  let filter_string =
    filter_string.unwrap_or_else(|| format!("{}={}", env!("CARGO_CRATE_NAME"), level));
  EnvFilter::new(filter_string)
}

fn load_direct_connections(config: &Config) -> Vec<AddressPair> {
  config
    .get_array("address_pairs")
    .expect("Config should contain address pairs")
    .into_iter()
    .filter_map(|row| row.into_table().ok())
    .enumerate()
    .map(|(idx, pair)| AddressPair {
      listener_address: pair
        .get("listener_address")
        .unwrap_or_else(|| panic!("Address pair {idx} doesn't contain the listener address"))
        .to_string(),
      target_address: pair
        .get("target_address")
        .unwrap_or_else(|| panic!("Address pair {idx} doesn't contain the target address"))
        .to_string(),
    })
    .collect::<Vec<AddressPair>>()
}

#[cfg(unix)]
#[instrument(skip_all)]
async fn run_host(
  pipe_path: Vec<String>,
  direct_connections: Vec<AddressPair>,
  socks5_proxy: Option<String>,
) {
  panic!("Running on linux is not supported.");
}

#[cfg(windows)]
#[instrument(skip_all)]
async fn run_host(
  pipe_path: Vec<String>,
  direct_connections: Vec<AddressPair>,
  socks5_proxy: Option<String>,
) {
  let cancel = CancellationToken::new();
  let (client_to_pipe_push, client_to_pipe_pull) = async_channel::bounded::<Bytes>(128);
  let (pipe_to_client_push, pipe_to_client_pull) = broadcast::channel::<Bytes>(128);

  let mut pipes = Vec::new();
  let mut retry_count = 0;
  for pipe_path in pipe_path {
    loop {
      match host::prepare_pipe(&pipe_path) {
        Ok(pipe) => {
          info!("Pipe {} is ready", &pipe_path);
          pipes.push(pipe);
          break;
        }
        Err(err) => {
          trace!("Failed to prepare pipe: {}", err);
          assert!(retry_count < 10, "Failed to prepare pipe {} after 10 attempts", &pipe_path);
          sleep(Duration::from_millis(100)).await;
          retry_count += 1;
        }
      }
    }
  }

  assert!(!pipes.is_empty(), "No pipes available");

  let tasks = FuturesUnordered::new();
  for pipe in pipes {
    let pipe_task = tokio::spawn({
      let cancel = cancel.clone();
      let pipe_to_client_push = pipe_to_client_push.clone();
      let client_to_pipe_pull = client_to_pipe_pull.clone();
      async move {
        sink_loop(pipe, pipe_to_client_push, client_to_pipe_pull, cancel).await;
      }
    });
    tasks.push(pipe_task);
  }

  let (connection_sender, connection_receiver) = mpsc::channel(128);

  let initiator_task = tokio::spawn({
    let cancel = cancel.clone();
    connection_initiator(client_to_pipe_push, pipe_to_client_pull, connection_receiver, cancel)
  });
  tasks.push(initiator_task);

  let listener_tasks = FuturesUnordered::new();
  for pair_direct in direct_connections.into_iter() {
    let cancel = cancel.clone();
    let connection_sender = connection_sender.clone();
    match setup_listener(
      &pair_direct.listener_address,
      ConnectionType::new_direct(pair_direct.target_address),
      connection_sender,
      cancel,
    )
    .await
    {
      Ok(pair_task) => {
        listener_tasks.push(pair_task);
      }
      Err(e) => {
        error!("Failed to setup listener for {}: {}", pair_direct.listener_address, e);
      }
    }
  }

  if let Some(socks5_proxy) = socks5_proxy {
    let cancel = cancel.clone();
    let connection_sender = connection_sender.clone();
    match setup_listener(&socks5_proxy, ConnectionType::Socks5, connection_sender, cancel).await {
      Ok(socks5_task) => {
        listener_tasks.push(socks5_task);
      }
      Err(e) => {
        error!("Failed to setup socks5 listener at {}: {}", socks5_proxy, e);
      }
    }
  }

  assert!(!listener_tasks.is_empty(), "All listeners failed to start");

  let joined_tasks = maybe_done(join_all(tasks.into_iter().chain(listener_tasks)));
  run_indefinitely(cancel, joined_tasks).await;
}

#[instrument(skip_all)]
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

#[instrument(skip_all)]
async fn run_guest(serial_paths: Vec<String>, baud_rate: u32) {
  let mut serials = Vec::new();
  for serial_path in serial_paths {
    let serial = tokio_serial::new(serial_path, baud_rate)
      .open_native_async()
      .expect("Failed to open serial port");
    serials.push(serial);
  }

  let cancel = CancellationToken::new();
  let (client_to_serial_push, client_to_serial_pull) = async_channel::bounded::<Bytes>(128);
  let (serial_to_client_push, serial_to_client_pull) = broadcast::channel::<Bytes>(128);
  let tasks = FuturesUnordered::new();

  for serial in serials {
    let serial_task = task::spawn(sink_loop(
      serial,
      serial_to_client_push.clone(),
      client_to_serial_pull.clone(),
      cancel.clone(),
    ));
    tasks.push(serial_task);
  }

  let initiator_task =
    task::spawn(client_initiator(serial_to_client_pull, client_to_serial_push, cancel.clone()));
  tasks.push(initiator_task);

  let join = maybe_done(join_all(tasks));
  run_indefinitely(cancel.clone(), join).await;
}

async fn run_indefinitely(
  cancel: CancellationToken,
  running_tasks: MaybeDone<JoinAll<JoinHandle<()>>>,
) {
  tokio::select! {
    () = cancel.cancelled() => {
      if timeout(Duration::from_secs(2), running_tasks).await.is_err() {
        info!("Failed to shutdown gracefully. Forcing shutdown.");
      }
    }
    c = ctrl_c() => {
      info!("Received Ctrl+C. Shutting down.");
      cancel.cancel();
      match c {
        Ok(()) => {
          if timeout(Duration::from_secs(2), running_tasks).await.is_err() {
            info!("Failed to shutdown gracefully. Forcing shutdown.");
          }
        },
        Err(e) => {
          error!("Failed to handle Ctrl+C: {}", e);
        }
      }
    }
  }
}
