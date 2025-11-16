use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub trait Sink: TryWrite + AsyncWriteExt + AsyncReadExt + Unpin + Sized {}

impl<T> Sink for T where T: TryWrite + AsyncReadExt + AsyncWriteExt + Unpin + Sized {}

pub trait TryWrite: AsyncWriteExt + AsyncReadExt + Send {
  fn try_write(&mut self, buf: &[u8]) -> impl Future<Output = std::io::Result<usize>> + Send;
}

mod common {
  use crate::try_write::TryWrite;
  use tokio::io::{AsyncWriteExt, DuplexStream};
  use tokio_serial::SerialStream;

  impl TryWrite for DuplexStream {
    async fn try_write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
      DuplexStream::write(self, buf).await
    }
  }

  impl TryWrite for SerialStream {
    async fn try_write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
      SerialStream::try_write(self, buf)
    }
  }
}

#[cfg(windows)]
mod windows {
  use crate::try_write::TryWrite;
  use tokio::net::windows::named_pipe::{NamedPipeClient, NamedPipeServer};

  impl TryWrite for NamedPipeClient {
    async fn try_write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
      NamedPipeClient::try_write(self, buf)
    }
  }

  impl TryWrite for NamedPipeServer {
    async fn try_write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
      NamedPipeServer::try_write(self, buf)
    }
  }
}

#[cfg(unix)]
mod unix {
  use crate::try_write::TryWrite;
  use tokio::net::UnixStream;

  impl TryWrite for UnixStream {
    async fn try_write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
      UnixStream::try_write(self, buf)
    }
  }
}
