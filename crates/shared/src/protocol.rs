use crate::schema_generated::serial_multiplexer::{ControlCode, Datagram, root_as_datagram};
use bytes::Bytes;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DatagramOwned {
  pub identifier: u64,
  pub sequence: u64,
  pub code: ControlCode,
  pub data: Option<Bytes>,
}

impl DatagramOwned {
  pub fn new(identifier: u64, sequence: u64, code: ControlCode, data: Option<Bytes>) -> Self {
    DatagramOwned {
      identifier,
      sequence,
      code,
      data,
    }
  }

  pub fn new_initial(identifier: u64, target: &str) -> Self {
    Self::new(identifier, 0, ControlCode::Initial, Some(Bytes::copy_from_slice(target.as_bytes())))
  }

  pub fn new_data(identifier: u64, sequence: u64, data: Bytes) -> Self {
    Self::new(identifier, sequence, ControlCode::Data, Some(data))
  }

  pub fn new_ack(identifier: u64, sequence: u64, acked: u64) -> Self {
    Self::new(
      identifier,
      sequence,
      ControlCode::Ack,
      Some(Bytes::copy_from_slice(&acked.to_be_bytes())),
    )
  }

  pub fn new_close(identifier: u64, sequence: u64) -> Self {
    Self::new(identifier, sequence, ControlCode::Close, None)
  }

  fn from_bytes(buf: &Bytes) -> Result<Self, flatbuffers::InvalidFlatbuffer> {
    let d = root_as_datagram(buf)?;
    Ok(DatagramOwned {
      identifier: d.identifier(),
      sequence: d.sequence(),
      code: d.code(),
      data: d.data().map(|v| buf.slice_ref(v.bytes())),
    })
  }
}

impl From<Datagram<'_>> for DatagramOwned {
  fn from(value: Datagram<'_>) -> Self {
    DatagramOwned {
      identifier: value.identifier(),
      sequence: value.sequence(),
      code: value.code(),
      data: value.data().map(|v| Bytes::copy_from_slice(v.bytes())),
    }
  }
}

impl TryFrom<Bytes> for DatagramOwned {
  type Error = flatbuffers::InvalidFlatbuffer;

  fn try_from(value: Bytes) -> Result<Self, Self::Error> {
    DatagramOwned::from_bytes(&value)
  }
}

impl TryFrom<&Bytes> for DatagramOwned {
  type Error = flatbuffers::InvalidFlatbuffer;

  fn try_from(value: &Bytes) -> Result<Self, Self::Error> {
    DatagramOwned::from_bytes(value)
  }
}

impl TryFrom<&[u8]> for DatagramOwned {
  type Error = flatbuffers::InvalidFlatbuffer;

  fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
    root_as_datagram(value).map(|d| d.into())
  }
}
