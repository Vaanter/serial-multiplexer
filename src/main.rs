use anyhow::{Context, Error, bail};
use clap::Parser;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, lookup_host};
use tokio_serial::SerialPortBuilderExt;
use tokio_serial::SerialStream;
use tracing::{Level, debug, error, info, trace, warn};
use tracing_attributes::instrument;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

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
    .with_ansi(false)
    .with_line_number(false)
    .with_thread_ids(true)
    .with_target(false)
    .with_filter(EnvFilter::new(format!("serial_proxy={}", level)));

  Registry::default().with(fmt_layer).init();

  tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()
    .unwrap()
    .block_on(run_serial(args));
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
        warn!(
          "Failed to connect to downstream {}: {}",
          downstream_address, e
        );
        continue;
      }
    };
    return Ok(downstream);
  }
  bail!("Failed to connect to downstream {}", downstream);
}
