use crate::common::ACK_THRESHOLD;
use bytes::Bytes;
use papaya::HashMap;
use std::sync::Arc;
use tracing::debug;

/// Thread-safe map storing each client's channel used to send datagrams to/from the sink(s).
pub type ChannelMap = Arc<
  HashMap<Identifier, (async_broadcast::Sender<Bytes>, async_broadcast::InactiveReceiver<Bytes>)>,
>;

/// RAII guard that automatically removes a channel from the map when dropped.
pub struct ChannelMapGuard {
  channel_map: ChannelMap,
  identifier: Identifier,
}

impl Drop for ChannelMapGuard {
  fn drop(&mut self) {
    debug!("Removing {:?} channels", self.identifier);
    self.channel_map.pin().remove(&self.identifier);
  }
}

/// Identifies the owner of a communication channel.
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum Identifier {
  /// A connected client with a unique ID.
  Client(u64),
  /// The channel used during client connection initiation (Guest only).
  ClientInitiator,
}

/// Retrieves or creates a broadcast channel for the given identifier.
///
/// Creates a new channel if one doesn't exist, or activates an existing one.
/// The returned guard automatically removes the channel when dropped.
///
/// # Parameters
/// * `channel_map` - The shared map of channels
/// * `identifier` - The unique identifier for the channel
///
/// # Returns
/// A tuple containing:
/// * An active receiver for reading datagrams from the channel
/// * A guard that cleans up the channel when dropped
pub fn channel_get_or_insert_with_guard(
  channel_map: ChannelMap,
  identifier: Identifier,
) -> (async_broadcast::Receiver<Bytes>, ChannelMapGuard) {
  let guard = ChannelMapGuard {
    channel_map: channel_map.clone(),
    identifier,
  };
  let sink_to_client_pull = channel_map
    .pin()
    .get_or_insert_with(identifier, || {
      let (sink_to_client_push, sink_to_client_pull) =
        async_broadcast::broadcast::<Bytes>((ACK_THRESHOLD * 2) as usize);
      (sink_to_client_push, sink_to_client_pull.deactivate())
    })
    .1
    .activate_cloned();
  (sink_to_client_pull, guard)
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::test_utils::setup_tracing;
  use papaya::HashMap;
  use std::mem;
  use std::sync::Arc;

  #[tokio::test]
  async fn test_channel_get_or_insert_with_guard_created_and_dropped() {
    setup_tracing().await;
    let channel_map = Arc::new(HashMap::new());
    let id = Identifier::Client(50);
    let (_pull, guard) = channel_get_or_insert_with_guard(channel_map.clone(), id);
    assert!(channel_map.pin().contains_key(&id));
    drop(guard);
    assert!(!channel_map.pin().contains_key(&id));
  }

  #[tokio::test]
  async fn test_channel_get_or_insert_with_guard_forgotten() {
    setup_tracing().await;
    let channel_map = Arc::new(HashMap::new());
    let id = Identifier::Client(50);
    let (_pull, guard) = channel_get_or_insert_with_guard(channel_map.clone(), id);
    mem::forget(guard);
    assert!(channel_map.pin().contains_key(&id));
  }
}
