use crate::channels::ChannelMap;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A data transfer medium, through which two multiplexers can communicate
pub trait Sink: AsyncReadExt + AsyncWriteExt + Unpin + Sized + Send {}

impl<T> Sink for T where T: AsyncReadExt + AsyncWriteExt + Unpin + Sized + Send {}

#[derive(Clone, Debug)]
pub struct SinkLoopProperties {
  pub channel_map: ChannelMap,
  pub client_to_sink_pull: async_channel::Receiver<Bytes>,
  pub disable_compression: bool,
}

impl SinkLoopProperties {
  pub fn new(
    channel_map: ChannelMap,
    client_to_sink_pull: async_channel::Receiver<Bytes>,
    disable_compression: bool,
  ) -> Self {
    Self {
      channel_map,
      client_to_sink_pull,
      disable_compression,
    }
  }
}
