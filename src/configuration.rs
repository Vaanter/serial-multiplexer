use clap::{Args, Parser, Subcommand};
use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Parser, Clone, Debug, Serialize, Deserialize)]
#[clap(version, about, author)]
#[serde(rename_all = "snake_case")]
pub struct ConfigArgs {
  /// Mode in which the program will run. Either host or guest
  #[command(subcommand)]
  pub mode: Option<Modes>,
  /// Logging verbosity, default is WARN, each repetition increases the logging level.
  /// 1 = INFO, 2 = DEBUG, 3+ = TRACE
  #[arg(short, long, default_value = "0", action = clap::ArgAction::Count)]
  #[serde(skip)]
  pub verbose: u8,
  /// Path to the config file
  #[arg(short, long, default_value_t = default_config_path())]
  #[serde(default = "default_config_path")]
  pub config: String,
  /// Path to a file where logs will be written. If not specified, logs will be written to stdout.
  #[arg(long)]
  pub log_file: Option<String>,
  /// A filter for the traces (logs). To set a global filter at a specific level, use "serial_multiplexer=<LEVEL>"
  #[arg(long)]
  pub tracing_filter: Option<String>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Subcommand, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Modes {
  /// Initializes the application in host mode to listen on configured network addresses.
  /// On Windows this requires a functional Windows pipe from VirtualBox,
  /// and on Linux this will create a Unix socket.
  Host(Host),
  /// Initializes the application in guest mode awaiting data from serial port or Unix socket.
  /// Requires a serial port or a Unix socket depending on the specified sink type.
  #[command(subcommand)]
  Guest(Guest),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Args, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Host {
  /// Specifies how the 2 multiplexer instances communicate
  #[command(subcommand)]
  pub sink_type: SinkType,

  /// Listener and target address pairs. When parsed from the command line, a pipe must separate the listener and client address.
  #[arg(long)]
  #[serde(default)]
  pub(crate) address_pairs: Vec<AddressPair>,
  /// Address at which the proxy will listen for incoming connections.
  #[arg(long)]
  pub(crate) socks5_proxy: Option<String>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Args, Serialize, Deserialize)]
#[group(required = true, multiple = true)]
#[serde(rename_all = "snake_case")]
pub struct AddressPair {
  /// The address at which the multiplexer will listen for incoming connections.
  #[arg(long, requires = "target_address")]
  pub(crate) listener_address: String,
  /// The address at which the multiplexer will attempt to connect to.
  #[arg(long, requires = "listener_address")]
  pub(crate) target_address: String,
}

impl FromStr for AddressPair {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    let mut address_pairs = s.split('|');
    let listener_address =
      address_pairs.next().ok_or_else(|| anyhow::anyhow!("No listener address"))?;
    let target_address =
      address_pairs.next().ok_or_else(|| anyhow::anyhow!("No target address"))?;
    Ok(Self {
      listener_address: listener_address.to_string(),
      target_address: target_address.to_string(),
    })
  }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Subcommand, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkType {
  /// Communicate with multiplexer in guest mode via Windows pipe(s) (VirtualBox)
  #[cfg(windows)]
  WindowsPipe(WindowsPipeSink),
  /// Communicate with multiplexer in guest mode via a Unix socket
  #[cfg(not(windows))]
  UnixSocket(UnixSocketSink),
}

#[cfg(windows)]
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Args, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WindowsPipeSink {
  /// Path(s) to the pipe(s) that will be used to communicate with VirtualBox VM.
  #[arg(short, long)]
  #[serde(default)]
  pub(crate) pipe_paths: Vec<String>,
}

#[cfg(not(windows))]
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Args, Serialize, Deserialize)]
pub struct UnixSocketSink {
  /// Path to a Unix socket for communication with a multiplexer in host mode
  #[arg(short, long, default_value_t = default_socket_path())]
  #[serde(default = "default_socket_path")]
  pub(crate) socket_path: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Subcommand, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Guest {
  /// Communicate with multiplexer in host mode via serial port(s)
  Serial(Serial),
  /// Communicate with multiplexer in host mode via a Unix socket
  #[cfg(not(windows))]
  UnixSocket(UnixSocket),
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Debug, Args, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Serial {
  /// Path to a serial port file. On Linux this will likely be a /dev/ttyS0 - 3, and COM1 - 4 on Windows.
  #[arg(short, long)]
  #[serde(default)]
  pub serial_paths: Vec<String>,

  #[arg(long, hide = true, default_value_t = default_baud_rate())]
  #[serde(default = "default_baud_rate")]
  pub baud_rate: u32,
}

#[cfg(not(windows))]
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Debug, Args, Serialize, Deserialize)]
pub struct UnixSocket {
  #[arg(short, long, default_value_t = default_socket_path())]
  #[serde(default = "default_socket_path")]
  pub socket_path: String,
}

const fn default_baud_rate() -> u32 {
  115200
}

fn default_config_path() -> String {
  "config.toml".to_string()
}

#[cfg(not(windows))]
fn default_socket_path() -> String {
  "serial_multiplexer.sock".to_string()
}

impl ConfigArgs {
  pub fn build_config() -> Result<Self, Box<figment::Error>> {
    let args = ConfigArgs::parse();

    let config_file_path = args.config.clone();
    Figment::new()
      .merge(Toml::file(&config_file_path))
      .merge(Env::prefixed("SEMUL"))
      .adjoin(Serialized::defaults(args.clone()))
      .extract::<Self>()
      .map(|mut c: Self| {
        // TODO mode should be replaced automatically, but for now we need to do this
        if let Some(args_mode) = args.mode {
          c.mode = Some(args_mode);
        }
        c
      })
      .map_err(Box::new)
  }
}
