use crate::common::ACK_THRESHOLD;
use crate::protocol::DatagramOwned;
use papaya::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::debug;

/// Thread-safe map storing separate channels for each client and the client initiator.
/// Each entry contains a broadcast sender and inactive receiver for sending datagrams from the sink(s).
pub type ChannelMap = Arc<
  HashMap<
    Identifier,
    (async_broadcast::Sender<DatagramOwned>, async_broadcast::InactiveReceiver<DatagramOwned>),
  >,
>;

type ChannelMapGuard = MapGuard<(
  async_broadcast::Sender<DatagramOwned>,
  async_broadcast::InactiveReceiver<DatagramOwned>,
)>;

/// RAII guard that automatically removes an entry from the map when dropped.
pub struct MapGuard<V> {
  map: Arc<HashMap<Identifier, V>>,
  identifier: Identifier,
}

impl<V> MapGuard<V> {
  pub fn new(map: Arc<HashMap<Identifier, V>>, identifier: Identifier) -> Self {
    Self { map, identifier }
  }
}

impl<V> Drop for MapGuard<V> {
  fn drop(&mut self) {
    debug!("Removing entry {:?}", self.identifier);
    self.map.pin().remove(&self.identifier);
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
/// Each client and the client initiator have separate channels for receiving datagrams from the sink(s).
/// Creates a new channel with capacity `ACK_THRESHOLD * 2` if one doesn't exist, or activates an existing one.
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
) -> (async_broadcast::Receiver<DatagramOwned>, ChannelMapGuard) {
  let guard = ChannelMapGuard {
    map: channel_map.clone(),
    identifier,
  };
  let sink_to_client_pull = channel_map
    .pin()
    .get_or_insert_with(identifier, || {
      let (sink_to_client_push, sink_to_client_pull) =
        async_broadcast::broadcast::<DatagramOwned>((ACK_THRESHOLD * 2) as usize);
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
    setup_tracing();
    let channel_map = Arc::new(HashMap::new());
    let id = Identifier::Client(50);
    let (_pull, guard) = channel_get_or_insert_with_guard(channel_map.clone(), id);
    assert!(channel_map.pin().contains_key(&id));
    drop(guard);
    assert!(!channel_map.pin().contains_key(&id));
  }

  #[tokio::test]
  async fn test_channel_get_or_insert_with_guard_forgotten() {
    setup_tracing();
    let channel_map = Arc::new(HashMap::new());
    let id = Identifier::Client(50);
    let (_pull, guard) = channel_get_or_insert_with_guard(channel_map.clone(), id);
    mem::forget(guard);
    assert!(channel_map.pin().contains_key(&id));
  }
}
