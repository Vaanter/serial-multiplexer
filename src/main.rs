use crate::configuration::{ConfigArgs, Guest, Host, Modes};
use crate::runner::common::{create_guest_tasks, create_host_tasks};
use futures::future::{JoinAll, MaybeDone};
use std::fs::OpenOptions;
use std::time::Duration;
use tokio::signal::ctrl_c;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{Level, debug, error, info};
use tracing_attributes::instrument;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

mod common;
mod configuration;
mod guest;
mod host;
mod protocol_utils;
mod runner;
#[allow(unsafe_op_in_unsafe_fn, unused)]
mod schema_generated;
#[cfg(test)]
mod test_utils;
mod utils;

fn main() {
  let config = ConfigArgs::build_config().unwrap_or_else(|e| panic!("Failed to parse config: {e}"));

  let (writer, _guard) = if let Some(ref log_file_name) = config.log_file {
    let mut log_file_options = OpenOptions::new();
    log_file_options.write(true).truncate(true).create(true);
    let log_file = log_file_options.open(log_file_name).expect("Log file should be accessible");
    tracing_appender::non_blocking(log_file)
  } else {
    tracing_appender::non_blocking(std::io::stdout())
  };

  let fmt_layer = tracing_subscriber::fmt::Layer::default()
    .with_writer(writer)
    .with_file(false)
    // ansi should be disabled when logging to a file because it would make it difficult to read
    .with_ansi(config.log_file.is_none())
    .with_line_number(false)
    .with_thread_ids(true)
    .with_target(false)
    .with_filter(build_filter(config.tracing_filter.clone(), config.verbose));

  Registry::default().with(fmt_layer).init();
  debug!("config: {:?}", config);

  tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()
    .expect("Setting up runtime should succeed")
    .block_on(async {
      match config.mode {
        Some(Modes::Guest(guest)) => {
          #[cfg(not(windows))]
          assert!(
            !guest.serial_paths.is_empty() || !guest.socket_path.is_empty(),
            "No serial ports and no unix socket configured"
          );
          #[cfg(windows)]
          assert!(!guest.serial_paths.is_empty(), "No serial ports configured");

          run_guest(guest).await;
        }
        Some(Modes::Host(host)) => {
          #[cfg(windows)]
          assert!(!host.pipe_paths.is_empty(), "No pipe paths configured");
          assert!(
            !(host.address_pairs.is_empty() && host.socks5_proxy.is_none()),
            "No address pairs or socks5 proxy configured"
          );
          run_host(host).await;
        }
        None => {}
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

#[instrument(skip_all)]
async fn run_host(properties: Host) {
  let cancel = CancellationToken::new();
  let joined_tasks = create_host_tasks(properties, cancel.clone()).await;
  run_indefinitely(cancel, joined_tasks).await;
}

#[instrument(skip_all)]
async fn run_guest(properties: Guest) {
  let cancel = CancellationToken::new();

  let joined_tasks = create_guest_tasks(properties, cancel.clone()).await;
  run_indefinitely(cancel.clone(), joined_tasks).await;
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
