use crate::client::client_initiator;
use crate::server::{Connection, connection_initiator, run_listener};
use crate::utils::create_upstream_listener;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use common::sink_loop;
use futures::future::{JoinAll, MaybeDone, join_all, maybe_done};
use futures::join;
use futures::stream::FuturesUnordered;
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
  #[clap(short, long)]
  verbose: Option<u8>,
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

fn main() {
  let args = Args::parse();

  let level = match args.verbose {
    Some(1) => Level::INFO,
    Some(2) => Level::DEBUG,
    Some(3) => Level::TRACE,
    _ => Level::WARN,
  };

  let fmt_layer = tracing_subscriber::fmt::Layer::default()
    .with_writer(std::io::stdout)
    .with_file(false)
    .with_ansi(true)
    .with_line_number(false)
    .with_thread_ids(true)
    .with_target(false)
    .with_filter(EnvFilter::new(format!("serial_multiplexer={}", level)));

  Registry::default().with(fmt_layer).init();
  debug!("args: {:?}", args);

  let addresses = vec![
    AddressPair {
      listener_address: "127.0.0.1:1234".to_string(),
      target_address: "tcpbin.com:4242".to_string(),
    },
    AddressPair {
      listener_address: "127.0.0.1:1235".to_string(),
      target_address: "www.google.com:443".to_string(),
    },
  ];

  tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()
    .unwrap()
    .block_on(async {
      match args.command {
        Commands::Client { serial_path } => {
          run_client(&serial_path).await;
        }
        Commands::Host { pipe_path } => {
          run_server(&pipe_path, addresses).await;
        }
      }
    });
}

struct AddressPair {
  listener_address: String,
  target_address: String,
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
