use crate::client::client_initiator;
use crate::server::{Connection, connection_initiator, run_listener};
use crate::utils::create_upstream_listener;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use common::sink_loop;
use config::Config;
use futures::future::{JoinAll, MaybeDone, join_all, maybe_done};
use futures::join;
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

mod client;
mod common;
mod protocol_utils;
#[allow(unsafe_op_in_unsafe_fn, unused)]
mod schema_generated;
mod server;
#[cfg(test)]
mod test_utils;
mod utils;

#[derive(Parser, Debug)]
#[clap(version, about, author)]
struct Args {
  #[clap(subcommand)]
  command: Commands,
  #[clap(short, long, default_value = "4", action = clap::ArgAction::Count)]
  verbose: u8,
  #[clap(short, long, default_value = "config.toml")]
  config: String,
}

#[derive(Subcommand, Debug)]
enum Commands {
  Host {
    #[arg(short, long)]
    pipe_path: String,
  },
  Client {
    #[arg(short, long)]
    serial_path: String,
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

  let level = match args.verbose {
    1 => Level::INFO,
    2 => Level::DEBUG,
    3 => Level::TRACE,
    _ => Level::WARN,
  };

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

  let fmt_layer = tracing_subscriber::fmt::Layer::default()
    .with_writer(writer)
    .with_file(false)
    .with_ansi(true)
    .with_line_number(false)
    .with_thread_ids(true)
    .with_target(false)
    .with_filter(EnvFilter::new(format!("serial_multiplexer={}", level)));

  Registry::default().with(fmt_layer).init();
  debug!("args: {:?}", args);

  tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()
    .expect("Setting up runtime must succeed")
    .block_on(async {
      match args.command {
        Commands::Client { serial_path } => {
          run_client(&serial_path).await;
        }
        Commands::Host { pipe_path } => {
          let addresses = config
            .get_array("address_pairs")
            .expect("Config should contain address pairs")
            .into_iter()
            .filter_map(|row| row.into_table().ok())
            .enumerate()
            .map(|(idx, pair)| AddressPair {
              listener_address: pair
                .get("listener_address")
                .unwrap_or_else(|| {
                  panic!("Address pair {} doesn't contain the listener address", idx)
                })
                .to_string(),
              target_address: pair
                .get("target_address")
                .unwrap_or_else(|| {
                  panic!("Address pair {} doesn't contain the target address", idx)
                })
                .to_string(),
            })
            .collect::<Vec<AddressPair>>();
          run_server(&pipe_path, addresses).await;
        }
      }
    });
}

#[cfg(unix)]
#[instrument(skip_all)]
async fn run_server(_pipe_path: &str, _addresses: Vec<AddressPair>) {
  panic!("Running on linux is not supported.");
}

#[cfg(windows)]
#[instrument(skip_all)]
async fn run_server(pipe_path: &str, addresses: Vec<AddressPair>) {
  let cancel = CancellationToken::new();
  let (client_to_pipe_push, client_to_pipe_pull) = mpsc::channel::<Bytes>(128);
  let (pipe_to_client_push, pipe_to_client_pull) = broadcast::channel::<Bytes>(128);

  let mut retry_count = 0;
  let pipe = loop {
    match server::prepare_pipe(pipe_path).await {
      Ok(pipe) => {
        info!("Pipe is ready");
        break pipe;
      }
      Err(err) => {
        trace!("Failed to prepare pipe: {}", err);
        if retry_count >= 10 {
          return;
        }
        sleep(Duration::from_millis(100)).await;
        retry_count += 1;
        continue;
      }
    };
  };

  let pipe_task = tokio::spawn({
    let cancel = cancel.clone();
    let pipe_to_client_push = pipe_to_client_push.clone();
    let client_to_pipe_pull = client_to_pipe_pull;
    async move {
      sink_loop(pipe, pipe_to_client_push, client_to_pipe_pull, cancel).await;
    }
  });

  let tasks = FuturesUnordered::new();
  tasks.push(pipe_task);
  for pair in addresses.into_iter() {
    let client_to_pipe_push = client_to_pipe_push.clone();
    let pipe_to_client_pull = pipe_to_client_pull.resubscribe();
    let cancel = cancel.clone();
    let pair_task = tokio::spawn(async move {
      setup_address_pair(pair, client_to_pipe_push, pipe_to_client_pull, cancel).await;
    });
    tasks.push(pair_task);
  }

  let join = maybe_done(join_all(tasks));
  run_indefinitely(cancel.clone(), join).await;
}

async fn setup_address_pair(
  pair: AddressPair,
  client_to_pipe_push: mpsc::Sender<Bytes>,
  pipe_to_client_pull: broadcast::Receiver<Bytes>,
  cancel: CancellationToken,
) {
  let listener_address = pair.listener_address;
  let target_address = pair.target_address;
  let (connection_sender, connection_receiver) = mpsc::channel::<Connection>(128);

  let listener = match create_upstream_listener(&listener_address).await {
    Ok(Some(listener)) => listener,
    Ok(None) => {
      error!("Failed to create listener for address {}.", listener_address);
      return;
    }
    Err(e) => {
      error!("Failed to create listener for address {}: {}", listener_address, e);
      return;
    }
  };

  let listener_task = tokio::spawn({
    let cancel = cancel.clone();
    async move {
      run_listener(listener, target_address, connection_sender, cancel).await;
    }
  });

  let initiator_task = tokio::spawn({
    let cancel = cancel.clone();
    async move {
      connection_initiator(client_to_pipe_push, pipe_to_client_pull, connection_receiver, cancel)
        .await;
    }
  });

  let _ = join!(listener_task, initiator_task);
}

#[instrument(skip_all)]
async fn run_client(serial_path: &str) {
  let serial = tokio_serial::new(serial_path, 115200)
    .open_native_async()
    .expect("Failed to open serial port");

  let cancel = CancellationToken::new();
  let (client_to_serial_push, client_to_serial_pull) = mpsc::channel::<Bytes>(128);
  let (serial_to_client_push, serial_to_client_pull) = broadcast::channel::<Bytes>(128);

  let serial_task =
    task::spawn(sink_loop(serial, serial_to_client_push, client_to_serial_pull, cancel.clone()));

  let initiator_task =
    task::spawn(client_initiator(serial_to_client_pull, client_to_serial_push, cancel.clone()));

  let join = maybe_done(join_all(vec![serial_task, initiator_task]));
  run_indefinitely(cancel.clone(), join).await;
}

async fn run_indefinitely(
  cancel: CancellationToken,
  running_tasks: MaybeDone<JoinAll<JoinHandle<()>>>,
) {
  tokio::select! {
    _ = cancel.cancelled() => {
      if timeout(Duration::from_secs(2), running_tasks).await.is_err() {
        info!("Failed to shutdown gracefully. Forcing shutdown.");
      }
    }
    c = ctrl_c() => {
      info!("Received Ctrl+C. Shutting down.");
      cancel.cancel();
      match c {
        Ok(_) => {
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
