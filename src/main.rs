use crate::server::{
  Connection, connection_initiator, create_upstream_listener, pipe_loop, run_listener,
};
use anyhow::{Context, Error, bail};
use bytes::Bytes;
use clap::Parser;
use futures::future::{join_all, maybe_done};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, join};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, lookup_host};
use tokio::signal::ctrl_c;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{sleep, timeout};
use tokio_serial::SerialPortBuilderExt;
use tokio_serial::SerialStream;
use tokio_util::sync::CancellationToken;
use tracing::{Level, debug, error, info, trace, warn};
use tracing_attributes::instrument;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

mod client;
mod protocol_utils;
#[allow(unsafe_op_in_unsafe_fn, unused)]
mod schema_generated;
mod server;

#[derive(Parser, Debug)]
#[clap(version, about, author)]
struct Args {
  #[clap(short, long)]
  target: String,
  #[clap(short, long)]
  serial: String,
  #[clap(short, long, default_value_t = 1)]
  verbose: u8,
}

fn main() {
  let args = Args::parse();
  debug!("args: {:?}", args);

  let level = match args.verbose {
    1 => Level::INFO,
    2 => Level::DEBUG,
    3 => Level::TRACE,
    _ => Level::WARN,
  };

  let fmt_layer = tracing_subscriber::fmt::Layer::default()
    .with_writer(std::io::stdout)
    .with_file(false)
    .with_ansi(true)
    .with_line_number(false)
    .with_thread_ids(true)
    .with_target(false)
    .with_filter(EnvFilter::new(format!("serial_proxy={}", level)));

  Registry::default().with(fmt_layer).init();

  let addresses = vec![
    AddressPair {
      listener_address: "127.0.0.1:1234".to_string(),
      target_address: "tcpbin.com:4242".to_string(),
    },
    AddressPair {
      listener_address: "127.0.0.1:1235".to_string(),
      target_address: "tcpbin.com:4242".to_string(),
    },
  ];

  tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()
    .unwrap()
    .block_on(async {
      run_server(addresses).await;
    });
}

struct AddressPair {
  listener_address: String,
  target_address: String,
}

async fn run_server(addresses: Vec<AddressPair>) {
  let cancel = CancellationToken::new();
  let (client_to_pipe_push, client_to_pipe_pull) = mpsc::channel::<Bytes>(128);
  let (pipe_to_client_push, pipe_to_client_pull) = broadcast::channel::<Bytes>(128);

  let mut retry_count = 0;
  let pipe = loop {
    match server::prepare_pipe(r"\\.\pipe\ubuntuvirtual").await {
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
      pipe_loop(pipe, pipe_to_client_push, client_to_pipe_pull, cancel).await;
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
  match ctrl_c().await {
    Ok(_) => {
      info!("Received Ctrl+C. Shutting down.");
      cancel.cancel();
      if timeout(Duration::from_secs(2), join).await.is_err() {
        info!("Failed to shutdown gracefully. Forcing shutdown.");
      }
    }
    Err(e) => {
      error!("Failed to handle Ctrl+C: {}", e);
    }
  }
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

#[instrument(skip(args))]
async fn run_serial(args: Args) {
  let mut serial = tokio_serial::new(&args.serial, 115200)
    .open_native_async()
    .expect("Failed to open serial port");

  loop {
    let duplex_result = duplex_loop(&args.target, &mut serial).await;
    match duplex_result {
      Ok(_) => {
        info!("Good bye :)");
        break;
      }
      Err(DuplexError::DownstreamClosed) => {
        info!("Downstream connection closed. Will try to reconnect.");
        continue;
      }
      Err(err) => {
        error!("Duplex connection error: {}", err);
        break;
      }
    }
  }
}

#[derive(Error, Debug)]
enum DuplexError {
  #[error("Connection to downstream closed")]
  DownstreamClosed,
  #[error("Failed to connect to target downstream server")]
  DownstreamConnectionFailed(anyhow::Error),
  #[error("Serial port is closed")]
  SerialClosed,
  #[error(transparent)]
  SerialError(anyhow::Error),
  #[error(transparent)]
  DownstreamError(anyhow::Error),
  #[error("System error occurred. {0:?}")]
  SystemError(anyhow::Error),
}

async fn duplex_loop(target: &str, serial: &mut SerialStream) -> Result<(), DuplexError> {
  let mut downstream = connect_downstream(target)
    .await
    .map_err(DuplexError::DownstreamConnectionFailed)?;

  loop {
    let mut buf_serial = vec![0u8; 2048];
    let mut buf_tcp = vec![0u8; 2048];

    let read_serial = serial.read(&mut buf_serial);
    let read_tcp = downstream.read(&mut buf_tcp);

    tokio::select! {
      n = read_serial => {
        match n {
          Ok(0) => return Err(DuplexError::SerialClosed),
          Ok(n) => {
            trace!("Read {} bytes from serial stream", n);
            downstream.write_all(&buf_serial[..n]).await.context("Failed to write to downstream")
              .map_err(DuplexError::DownstreamError)?;
          },
          Err(e) => return Err(DuplexError::SerialError(Error::from(e)))
        }
      },
      n = read_tcp => {
        match n {
          Ok(0) => return Err(DuplexError::DownstreamClosed),
          Ok(n) => {
            trace!("Read {} bytes from tcp stream", n);
            serial.write_all(&buf_tcp[..n]).await.context("Failed to write to serial stream")
              .map_err(DuplexError::SerialError)?;
          },
          Err(e) => return Err(DuplexError::DownstreamError(Error::from(e)))
        }
      }
    }
  }
}

async fn connect_downstream(downstream: &str) -> anyhow::Result<TcpStream> {
  let downstream_addresses = lookup_host(downstream).await?.collect::<Vec<_>>();
  for downstream_address in downstream_addresses.iter() {
    let downstream = match TcpStream::connect(downstream_address).await {
      Ok(downstream) => {
        debug!("Successfully connected to server");
        downstream
      }
      Err(e) => {
        warn!("Failed to connect to downstream {}: {}", downstream_address, e);
        continue;
      }
    };
    return Ok(downstream);
  }
  bail!("Failed to connect to downstream {}", downstream);
}
