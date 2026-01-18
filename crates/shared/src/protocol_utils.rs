use crate::schema_generated::serial_multiplexer::{
  ControlCode, Datagram, DatagramArgs, root_as_datagram,
};
use bytes::Bytes;
use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer};

pub fn create_data_datagram(identifier: u64, sequence: u64, data: &[u8]) -> Bytes {
  create_datagram_with_code(identifier, sequence, ControlCode::Data, data)
}

pub fn create_initial_datagram(identifier: u64, target: &str) -> Bytes {
  create_datagram_with_code(identifier, 0, ControlCode::Initial, target.as_bytes())
}

pub fn create_ack_datagram(identifier: u64, sequence: u64, acked: u64) -> Bytes {
  create_datagram_with_code(identifier, sequence, ControlCode::Ack, &acked.to_be_bytes())
}

pub fn create_close_datagram(identifier: u64, sequence: u64) -> Bytes {
  create_datagram_with_code(identifier, sequence, ControlCode::Close, &[])
}

fn create_datagram_with_code(
  identifier: u64,
  sequence: u64,
  code: ControlCode,
  data: &[u8],
) -> Bytes {
  const ASSUMED_DATAGRAM_SIZE: usize = 64;
  let mut builder = FlatBufferBuilder::with_capacity(ASSUMED_DATAGRAM_SIZE + data.len());
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

pub fn datagram_from_bytes(data: &'_ [u8]) -> Result<Datagram<'_>, InvalidFlatbuffer> {
  root_as_datagram(data)
}

#[cfg(test)]
mod tests {
  use super::*;
  use bytes::BytesMut;

  #[test]
  fn test_datagram_size() {
    let initial_datagram_min = create_initial_datagram(0, "www.google.com:433");
    let initial_datagram_small = create_initial_datagram(150, "www.google.com:433");
    let initial_datagram_max = create_initial_datagram(u64::MAX, "www.google.com:433");
    let ack_datagram_min = create_ack_datagram(0, 0, 0);
    let ack_datagram_small = create_ack_datagram(150, 3000, 3000);
    let ack_datagram_max = create_ack_datagram(u64::MAX, u64::MAX, u64::MAX);
    let close_datagram_min = create_close_datagram(0, 0);
    let close_datagram_small = create_close_datagram(150, 3000);
    let close_datagram_max = create_close_datagram(u64::MAX, u64::MAX);
    let data_datagram_min = create_data_datagram(0, 0, &[]);
    let data_datagram_small = create_data_datagram(150, 3000, &[]);
    let data_datagram_max = create_data_datagram(
      u64::MAX,
      u64::MAX,
      &BytesMut::zeroed(crate::common::CONNECTION_BUFFER_SIZE),
    );

    let mut compression_buffer = BytesMut::zeroed(crate::common::SINK_COMPRESSION_BUFFER_SIZE);

    let initial_datagram_min_cs =
      zstd_safe::compress(compression_buffer.as_mut(), &initial_datagram_min, 9).unwrap();
    let initial_datagram_small_cs =
      zstd_safe::compress(compression_buffer.as_mut(), &initial_datagram_small, 9).unwrap();
    let initial_datagram_max_cs =
      zstd_safe::compress(compression_buffer.as_mut(), &initial_datagram_max, 9).unwrap();
    let ack_datagram_min_cs =
      zstd_safe::compress(compression_buffer.as_mut(), &ack_datagram_min, 9).unwrap();
    let ack_datagram_small_cs =
      zstd_safe::compress(compression_buffer.as_mut(), &ack_datagram_small, 9).unwrap();
    let ack_datagram_max_cs =
      zstd_safe::compress(compression_buffer.as_mut(), &ack_datagram_max, 9).unwrap();
    let close_datagram_min_cs =
      zstd_safe::compress(compression_buffer.as_mut(), &close_datagram_min, 9).unwrap();
    let close_datagram_small_cs =
      zstd_safe::compress(compression_buffer.as_mut(), &close_datagram_small, 9).unwrap();
    let close_datagram_max_cs =
      zstd_safe::compress(compression_buffer.as_mut(), &close_datagram_max, 9).unwrap();
    let data_datagram_min_cs =
      zstd_safe::compress(compression_buffer.as_mut(), &data_datagram_min, 9).unwrap();
    let data_datagram_small_cs =
      zstd_safe::compress(compression_buffer.as_mut(), &data_datagram_small, 9).unwrap();
    let data_datagram_max_cs =
      zstd_safe::compress(compression_buffer.as_mut(), &data_datagram_max, 9).unwrap();

    println!(
      "Initial: {} -> {} | {} -> {} | {} -> {}",
      initial_datagram_min.len(),
      initial_datagram_min_cs,
      initial_datagram_small.len(),
      initial_datagram_small_cs,
      initial_datagram_max.len(),
      initial_datagram_max_cs
    );
    println!(
      "Ack: {} -> {} | {} -> {} | {} -> {}",
      ack_datagram_min.len(),
      ack_datagram_min_cs,
      ack_datagram_small.len(),
      ack_datagram_small_cs,
      ack_datagram_max.len(),
      ack_datagram_max_cs
    );
    println!(
      "Close: {} -> {} | {} -> {} | {} -> {}",
      close_datagram_min.len(),
      close_datagram_min_cs,
      close_datagram_small.len(),
      close_datagram_small_cs,
      close_datagram_max.len(),
      close_datagram_max_cs
    );
    println!(
      "Data: {} -> {} | {} -> {} | {} -> {}",
      data_datagram_min.len(),
      data_datagram_min_cs,
      data_datagram_small.len(),
      data_datagram_small_cs,
      data_datagram_max.len(),
      data_datagram_max_cs
    );
  }
}
