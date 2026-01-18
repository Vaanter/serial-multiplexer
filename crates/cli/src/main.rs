use crate::runner::common::{create_guest_tasks, create_host_tasks};
use anyhow::{Context, bail, ensure};
use config::configuration::{ALLOWED_CONFIG_VERSIONS, ConfigArgs, GuestSink, Modes};
use futures::future::{JoinAll, MaybeDone};
use std::fs::OpenOptions;
use std::io::{IsTerminal, Write, stdin};
use std::num::NonZeroUsize;
use std::process::ExitCode;
use std::thread::available_parallelism;
use std::time::Duration;
use tokio::signal::ctrl_c;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{Level, Subscriber, debug, error, info};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_attributes::instrument;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};
use tracing_subscriber::{Layer, registry};

mod runner;

fn main() -> ExitCode {
  fn inner_main() -> anyhow::Result<()> {
    let config = ConfigArgs::build_config().context("Failed to parse config")?;
    ensure!(
      ALLOWED_CONFIG_VERSIONS.iter().any(|v| *v == config.version),
      "Unsupported config version"
    );

    // Must not be dropped, so the logs can actually be written out
    let _tracing_guards = initiate_tracing(&config)?;

    debug!("config: {:?}", config);
    match execute(config) {
      Ok(()) => Ok(()),
      Err(e) => {
        error!("{}", e);
        Err(e)
      }
    }
  }
  match inner_main() {
    Ok(()) => ExitCode::from(0),
    Err(e) => {
      if stdin().is_terminal() {
        eprintln!("\nFatal error:\n{}", e);
        let mut enter_buffer = String::new();
        println!("\nPress enter to exit");
        if let Err(e) = stdin().read_line(&mut enter_buffer) {
          eprintln!("Failed to read enter from the terminal. {}", e);
        }
      }
      ExitCode::from(1)
    }
  }
}

/// Initializes tracing with stdout and optional file logging.
///
/// # Parameters
/// * `config` - Configuration containing log file path, tracing filter, and verbosity level
///
/// # Returns
/// `WorkerGuard`s that must be kept alive for logging to function
fn initiate_tracing(config: &ConfigArgs) -> anyhow::Result<Vec<WorkerGuard>> {
  let mut tracing_guards = Vec::with_capacity(2);

  let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
  tracing_guards.push(stdout_guard);

  let stdout_layer = setup_tracing_layer(config, stdout_writer, true);

  let registry_setup = Registry::default().with(stdout_layer);

  if let Some(ref log_file_name) = config.log_file {
    let mut log_file_options = OpenOptions::new();
    log_file_options.write(true).truncate(true).create(true);
    let log_file = log_file_options.open(log_file_name).context("Failed to access log file")?;
    let (file_writer, file_guard) = tracing_appender::non_blocking(log_file);
    tracing_guards.push(file_guard);

    let file_layer = setup_tracing_layer(config, file_writer, false);

    registry_setup.with(file_layer).init();
  } else {
    registry_setup.init();
  }
  Ok(tracing_guards)
}

fn setup_tracing_layer<S, W>(config: &ConfigArgs, writer: W, ansi: bool) -> impl Layer<S>
where
  S: Subscriber + for<'span> registry::LookupSpan<'span>,
  W: Write + for<'a> MakeWriter<'a> + 'static,
{
  tracing_subscriber::fmt::Layer::default()
    .with_writer(writer)
    .with_file(false)
    // ansi should be disabled when logging to a file because it would make it difficult to read
    .with_ansi(ansi)
    .with_line_number(false)
    .with_thread_ids(true)
    .with_target(false)
    .with_filter(build_filter(config.tracing_filter.clone(), config.verbose))
}

fn execute(config: ConfigArgs) -> anyhow::Result<()> {
  let runtime =
    match config.threads.unwrap_or_else(|| available_parallelism().map_or(1, NonZeroUsize::get)) {
      0 => {
        bail!("At least one worker thread is required!");
      }
      1 => {
        debug!("Using current thread tokio runtime");
        tokio::runtime::Builder::new_current_thread().enable_all().build()
      }
      threads => {
        debug!("Using multithreaded tokio runtime with {} worker threads", threads);
        tokio::runtime::Builder::new_multi_thread().enable_all().worker_threads(threads).build()
      }
    }
    .context("Failed to setup tokio runtime")?;

  runtime.block_on(async {
    match config.mode {
      Some(Modes::Guest(ref guest)) => {
        match guest.sink_type {
          GuestSink::Serial(ref serial) => {
            ensure!(!serial.serial_paths.is_empty(), "No serial ports configured");
          }
          #[cfg(windows)]
          GuestSink::WindowsPipe(ref windows_pipe_properties) => {
            ensure!(!windows_pipe_properties.pipe_paths.is_empty(), "No pipe paths configured");
          }
          #[cfg(not(windows))]
          GuestSink::UnixSocket(ref unix_socket) => {
            use std::path::PathBuf;
            ensure!(
              unix_socket.socket_paths.iter().all(|path| path.parse::<PathBuf>().is_ok()),
              "No unix socket configured"
            )
          }
        }

        run_guest(config).await
      }
      Some(Modes::Host(ref host)) => {
        #[cfg(windows)]
        {
          use config::configuration::HostSink;
          let HostSink::WindowsPipe(ref windows_pipe_properties) = host.sink_type;
          ensure!(!windows_pipe_properties.pipe_paths.is_empty(), "No pipe paths configured");
        }
        ensure!(
          !(host.address_pairs.is_empty()
            && host.socks5_proxy.is_none()
            && host.http_proxy.is_none()),
          "No address pairs or socks5/HTTP proxy configured"
        );
        run_host(config).await
      }
      None => {
        bail!("No mode specified");
      }
    }
  })
}

fn build_filter(filter_string: Option<String>, verbosity: u8) -> EnvFilter {
  let level = match verbosity {
    1 => Level::INFO,
    2 => Level::DEBUG,
    n if n > 2 => Level::TRACE,
    _ => Level::WARN,
  };
  let filter_string = filter_string.unwrap_or_else(|| format!("serial_multiplexer={}", level));
  EnvFilter::new(filter_string)
}

#[instrument(skip_all)]
async fn run_host(properties: ConfigArgs) -> anyhow::Result<()> {
  let cancel = CancellationToken::new();
  let joined_tasks = match create_host_tasks(properties, cancel.clone()).await {
    Ok(joined_tasks) => joined_tasks,
    Err(e) => {
      bail!("Initialisation failed! {:?}", e);
    }
  };
  run_indefinitely(cancel, joined_tasks).await;
  Ok(())
}

#[instrument(skip_all)]
async fn run_guest(properties: ConfigArgs) -> anyhow::Result<()> {
  let cancel = CancellationToken::new();

  let joined_tasks = match create_guest_tasks(properties, cancel.clone()).await {
    Ok(joined_tasks) => joined_tasks,
    Err(e) => {
      bail!("Initialisation failed! {:?}", e);
    }
  };
  run_indefinitely(cancel, joined_tasks).await;
  Ok(())
}

async fn run_indefinitely(
  cancel: CancellationToken,
  running_tasks: MaybeDone<JoinAll<JoinHandle<()>>>,
) {
  info!("Setup successful. Waiting for Ctrl+C or shutdown signal.");
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
