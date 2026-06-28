use crate::channels::ChannelMap;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A data transfer medium, through which two multiplexers can communicate
pub trait Sink: AsyncReadExt + AsyncWriteExt + Unpin + Sized + Send {}

impl<T> Sink for T where T: AsyncReadExt + AsyncWriteExt + Unpin + Sized + Send {}

/// Properties for a [`sink loop`].
///
/// * `channel_map`: A [`ChannelMap`] for sending received datagrams to clients/client initiator.
/// * `client_to_sink_pull`: An [`async_channel::Receiver`] channel through which the
///   sink loop receives data sent by clients to be written to the sink.
/// * `disable_compression`: A flag to disable the compression of datagrams sent through the sink.
///
/// [`sink loop`]: crate::common::sink_loop
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
