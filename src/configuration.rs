use clap::{Args, Parser, Subcommand};
use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Parser, Clone, Debug, Serialize, Deserialize)]
#[clap(version, about, author)]
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
pub enum Modes {
  /// Initializes the application in host mode to listen on configured network addresses.
  /// Requires a functional pipe connection from VirtualBox.
  /// Note: This mode is Windows-exclusive.
  Host(Host),
  /// Initializes the application in guest mode awaiting data from serial port.
  /// Requires a serial port.
  Guest(Guest),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Args, Serialize, Deserialize)]
pub struct Host {
  /// Path(s) to the pipe(s) that will be used to communicate with VirtualBox VM.
  #[cfg(windows)]
  #[arg(short, long)]
  #[serde(default)]
  pub(crate) pipe_paths: Vec<String>,

  #[cfg(not(windows))]
  #[arg(short, long, default_value_t = default_socket_path())]
  #[serde(default = "default_socket_path")]
  pub(crate) socket_path: String,
  /// Listener and target address pairs. When parsed from the command line, a pipe must separate the listener and client address.
  #[arg(long)]
  #[serde(default)]
  pub(crate) address_pairs: Vec<AddressPair>,
  /// Address at which the proxy will listen for incoming connections.
  #[arg(long)]
  pub(crate) socks5_proxy: Option<String>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Args, Serialize, Deserialize)]
pub struct Guest {
  /// Path to a serial port file. On linux this will likely be a /dev/ttyS0 - 3
  #[arg(short, long)]
  #[serde(default)]
  pub(crate) serial_paths: Vec<String>,

  #[cfg(not(windows))]
  #[arg(short, long, default_value_t = default_socket_path())]
  #[serde(default = "default_socket_path")]
  pub(crate) socket_path: String,

  #[arg(short, long)]
  pub(crate) guest_mode: GuestMode,

  #[arg(long, hide = true, default_value_t = default_baud_rate())]
  #[serde(default = "default_baud_rate")]
  pub(crate) baud_rate: u32,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Args, Serialize, Deserialize)]
#[group(required = true, multiple = true)]
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

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub enum GuestMode {
  Serial,
  UnixSocket,
}

impl FromStr for GuestMode {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s.to_lowercase().as_str() {
      "serial" => Ok(Self::Serial),
      "unix" | "unixsocket" => Ok(Self::UnixSocket),
      _ => Err(anyhow::anyhow!("Invalid guest mode")),
    }
  }
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
  pub fn build_config() -> Result<Self, figment::Error> {
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
  }
}
