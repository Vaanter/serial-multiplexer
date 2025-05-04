use crate::schema_generated::serial_multiplexer::{
  ControlCode, Datagram, DatagramArgs, root_as_datagram,
};
use bytes::Bytes;
use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer};

pub fn create_data_datagram(identifier: u64, sequence: u64, data: &[u8]) -> Bytes {
  create_data_datagram_with_code(identifier, sequence, ControlCode::Data, data)
}

pub fn create_initial_datagram(identifier: u64, sequence: u64, target: &str) -> Bytes {
  create_data_datagram_with_code(identifier, sequence, ControlCode::Initial, target.as_bytes())
}

pub fn create_ack_datagram(identifier: u64, sequence: u64) -> Bytes {
  create_data_datagram_with_code(identifier, sequence, ControlCode::Ack, &[])
}

pub fn create_close_datagram(identifier: u64, sequence: u64) -> Bytes {
  create_data_datagram_with_code(identifier, sequence, ControlCode::Close, &[])
}

fn create_data_datagram_with_code(
  identifier: u64,
  sequence: u64,
  code: ControlCode,
  data: &[u8],
) -> Bytes {
  let mut builder = FlatBufferBuilder::with_capacity(3072);
  let data_vector = builder.create_vector(data);
  let datagram = Datagram::create(
    &mut builder,
    &DatagramArgs {
      identifier,
      code,
      sequence,
      data: Some(data_vector),
    },
  );
  builder.finish_minimal(datagram);
  Bytes::copy_from_slice(builder.finished_data())
}

pub fn datagram_from_bytes(data: &[u8]) -> Result<Datagram, InvalidFlatbuffer> {
  root_as_datagram(data)
}
