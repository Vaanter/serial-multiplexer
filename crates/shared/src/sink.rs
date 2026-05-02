use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A data transfer medium, through which two multiplexers can communicate
pub trait Sink: AsyncReadExt + AsyncWriteExt + Unpin + Sized + Send {}

impl<T> Sink for T where T: AsyncReadExt + AsyncWriteExt + Unpin + Sized + Send {}
