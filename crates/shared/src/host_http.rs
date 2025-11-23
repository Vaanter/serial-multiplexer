use crate::channels::{ChannelMap, Identifier, channel_get_or_insert_with_guard};
use crate::common::ConnectionState;
use crate::host::{ConnectionType, IDENTIFIER_SEQUENCE};
use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::mem;
use std::sync::atomic::Ordering;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{Span, debug, error, info};
use tracing_attributes::instrument;

type ServerBuilder = hyper::server::conn::http1::Builder;

/// Runs an HTTP proxy listener that accepts CONNECT requests and tunnels TCP connections.
///
/// Accepts incoming TCP connections, serves them as HTTP/1.1, and handles HTTP CONNECT
/// method requests to establish TCP tunnels through the proxy.
/// The listener runs until the cancellation token is triggered.
///
/// # Parameters
///
/// * `listener`: A [`TcpListener`] whose connections will be accepted.
/// * `channel_map`: A [`ChannelMap`] to receive ACK datagram when initiating a connection.
/// * `client_to_sink_push`: An [`async_channel::Sender`] to send an Initial datagram to the
///   sink(s) when initiating a connection.
/// * `connection_sender`: An [`mpsc::Sender`] used to send a connection to
///   [`crate::host::connection_initiator`] after initiation succeeds.
/// * `cancel`:
///   A [`CancellationToken`] used to signal loop termination due to the app shutting down.
#[instrument(skip_all, fields(listener_address))]
pub async fn run_http_listener(
  listener: TcpListener,
  channel_map: ChannelMap,
  client_to_sink_push: async_channel::Sender<Bytes>,
  connection_sender: mpsc::Sender<(ConnectionState, ConnectionType)>,
  cancel: CancellationToken,
) {
  let listener_address =
    listener.local_addr().map(|a| format!("{:?}", a)).unwrap_or("???".to_string());
  Span::current().record("listener_address", &listener_address);
  loop {
    tokio::select! {
      biased;
      () = cancel.cancelled() => {
        info!("Closing HTTP listener {}", listener_address);
        break;
      }
      new_client = listener.accept() => {
        match new_client {
          Ok((client, client_address)) => {
            info!("Client connected: {}", client_address);
            handle_http_connection(
              client,
              channel_map.clone(),
              client_to_sink_push.clone(),
              connection_sender.clone(),
            );
          }
          Err(e) => {
            error!("Failed to establish client connection: {}", e);
            break;
          }
        }
      }
    }
  }
}

/// Spawns a tokio task to serve a connection with HTTP upgrade support via [`hyper`].
///
/// # Parameters
///
/// * `client`: The TCP stream for the connected client.
/// * `channel_map`: A [`ChannelMap`] to receive ACK datagram when initiating a connection.
/// * `client_to_sink_push`: An [`async_channel::Sender`] to send an Initial datagram to the
///   sink(s) when initiating a connection.
/// * `connection_sender`: An [`mpsc::Sender`] used to send a connection to
///   [`crate::host::connection_initiator`] after initiation succeeds.
fn handle_http_connection(
  client: TcpStream,
  channel_map: ChannelMap,
  client_to_sink_push: async_channel::Sender<Bytes>,
  connection_sender: mpsc::Sender<(ConnectionState, ConnectionType)>,
) {
  let io = TokioIo::new(client);
  let serving = service_fn(move |req| {
    serve_http(req, channel_map.clone(), client_to_sink_push.clone(), connection_sender.clone())
  });

  tokio::spawn(async move {
    if let Err(e) = ServerBuilder::new()
      .auto_date_header(true)
      .serve_connection(io, serving)
      .with_upgrades()
      .await
    {
      error!("Failed to serve connection. {e:?}");
    };
  });
}

/// Handles an HTTP request and initiates a connection after receiving a CONNECT method request.
///
/// Processes HTTP CONNECT requests by initiating a connection to a target address,
/// upgrading the HTTP connection to a TCP tunnel, and sending the connection state to the
/// [`crate::host::connection_initiator`].
/// Returns a 400 response for non-CONNECT requests, requests that do not have a valid authority
/// in their URI, and when initiating a connection fails.
///
/// # Parameters
///
/// * `req`: The incoming HTTP request.
/// * `channel_map`: A [`ChannelMap`] to receive ACK datagram when initiating a connection.
/// * `sink_to_client_pull`:
///   An [`async_broadcast::Receiver<Bytes>`] to receive ACK datagram when initiating a connection.
/// * `connection_sender`: An [`mpsc::Sender`] used to send a connection to
///   [`crate::host::connection_initiator`] after initiation succeeds.
async fn serve_http(
  req: Request<hyper::body::Incoming>,
  channel_map: ChannelMap,
  client_to_sink_push: async_channel::Sender<Bytes>,
  connection_sender: mpsc::Sender<(ConnectionState, ConnectionType)>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
  debug!("Received request: {:?}", req);

  if Method::CONNECT == req.method() {
    if let Some(target_address) = extract_authority(req.uri()) {
      let identifier = IDENTIFIER_SEQUENCE.fetch_add(1, Ordering::SeqCst);
      let (mut sink_to_client_pull, guard) =
        channel_get_or_insert_with_guard(channel_map.clone(), Identifier::Client(identifier));
      let connect_result = crate::host::initiate_connection(
        identifier,
        target_address.to_string(),
        &client_to_sink_push,
        &mut sink_to_client_pull,
      )
      .await;
      if connect_result {
        tokio::spawn(async move {
          if let Ok(upgraded) = hyper::upgrade::on(req).await {
            debug!("Taking TCP connection from upgrade");
            let client = upgraded
              .downcast::<TokioIo<TcpStream>>()
              .expect("Downcasting HTTP connection shouldn't fail. At least I guess.")
              .io
              .into_inner();
            let connection = ConnectionState::new(identifier, client);

            if let Err(e) = connection_sender.send((connection, ConnectionType::Http)).await {
              error!("Failed to finish setup for connection {}: {}", identifier, e);
            } else {
              // Forget the guard so the client's channel doesn't get removed
              mem::forget(guard);
            }
          }
        });
      } else {
        info!("Failed to initialize connection {}, sending 400 status.", identifier);
        return Ok(create_400_response(None::<Bytes>));
      }

      Ok(Response::new(empty()))
    } else {
      debug!("Target address invalid: {:?}", req.uri());
      Ok(create_400_response(Some("Target address is invalid!")))
    }
  } else {
    Ok(create_400_response(Some("Only the CONNECT method is supported")))
  }
}

/// Extracts the authority part of a URI
fn extract_authority(uri: &http::Uri) -> Option<String> {
  uri.authority().map(|auth| auth.to_string())
}

/// Creates a boxed, empty body of an HTTP request
fn empty() -> BoxBody<Bytes, hyper::Error> {
  Empty::<Bytes>::new().map_err(|never| match never {}).boxed()
}

/// Creates a boxed body of an HTTP request with a single chunk
fn full(chunk: impl Into<Bytes>) -> BoxBody<Bytes, hyper::Error> {
  Full::new(chunk.into()).map_err(|never| match never {}).boxed()
}

fn create_400_response(body: Option<impl Into<Bytes>>) -> Response<BoxBody<Bytes, hyper::Error>> {
  let mut response = match body {
    Some(body) => Response::new(full(body)),
    None => Response::new(empty()),
  };
  *response.status_mut() = StatusCode::BAD_REQUEST;
  response
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::host::ConnectionType;
  use crate::protocol_utils::create_ack_datagram;
  use crate::schema_generated::serial_multiplexer::{ControlCode, root_as_datagram};
  use crate::test_utils::{run_echo, setup_tracing};
  use bytes::Bytes;
  use http_body_util::BodyExt;
  use papaya::HashMap;
  use std::sync::Arc;
  use tokio::net::{TcpListener, TcpStream};
  use tokio::sync::mpsc;
  use tokio_util::sync::CancellationToken;

  #[tokio::test]
  async fn test_create_400_response_empty() {
    let response = create_400_response(None::<&str>);
    assert_eq!(400, response.status().as_u16());
    assert_eq!(Bytes::new(), response.into_body().collect().await.unwrap().to_bytes());
  }

  #[tokio::test]
  async fn test_create_400_response_with_str_content() {
    let content = "test content";
    let response = create_400_response(Some(content));
    assert_eq!(400, response.status().as_u16());
    assert_eq!(content.as_bytes(), response.into_body().collect().await.unwrap().to_bytes());
  }

  #[tokio::test]
  async fn test_create_400_response_with_bytes_content() {
    let content = Bytes::from_static("test content".as_bytes());
    let response = create_400_response(Some(content.clone()));
    assert_eq!(400, response.status().as_u16());
    assert_eq!(content, response.into_body().collect().await.unwrap().to_bytes());
  }

  #[tokio::test]
  async fn test_run_http_listener_smoke() {
    type ClientBuilder = hyper::client::conn::http1::Builder;
    setup_tracing().await;
    let channel_map = Arc::new(HashMap::new());
    let (client_to_sink_push, client_to_sink_pull) = async_channel::bounded(256);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_address = listener.local_addr().unwrap();
    let (target_address, _) = run_echo().await;
    let connection_type = ConnectionType::Http;
    let (connection_sender, mut connection_receiver) = mpsc::channel(4);
    tokio::spawn(run_http_listener(
      listener,
      channel_map.clone(),
      client_to_sink_push,
      connection_sender,
      CancellationToken::new(),
    ));
    let test_connection = TcpStream::connect(listener_address).await.unwrap();
    let (mut sender, conn) =
      ClientBuilder::new().handshake(TokioIo::new(test_connection)).await.unwrap();
    let request =
      Request::builder().method("CONNECT").uri(target_address.to_string()).body(empty()).unwrap();
    tokio::spawn(conn.with_upgrades());
    tokio::spawn(async move {
      let recv = client_to_sink_pull.recv().await.unwrap();
      let initial_datagram = root_as_datagram(&recv).unwrap();
      assert_eq!(0, initial_datagram.sequence());
      assert_eq!(ControlCode::Initial, initial_datagram.code());
      assert_eq!(target_address.to_string().as_bytes(), initial_datagram.data().unwrap().bytes());
      let sink_to_client_push = channel_map
        .pin()
        .get(&Identifier::Client(initial_datagram.identifier()))
        .unwrap()
        .0
        .clone();
      sink_to_client_push
        .broadcast_direct(create_ack_datagram(initial_datagram.identifier(), 0, 0))
        .await
        .unwrap();
    });
    let resp = sender.send_request(request).await.unwrap();
    assert_eq!(200, resp.status());
    let (connection, connection_type_received) = connection_receiver.recv().await.unwrap();
    assert_eq!(connection_type_received, connection_type);
    let upgraded = hyper::upgrade::on(resp).await.unwrap();
    let client = upgraded.downcast::<TokioIo<TcpStream>>().unwrap().io;
    assert_eq!(connection.client.peer_addr().unwrap(), client.inner().local_addr().unwrap());
  }
}
