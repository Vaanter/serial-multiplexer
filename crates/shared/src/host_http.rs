use crate::common::ConnectionState;
use crate::host::{ConnectionType, IDENTIFIER_SEQUENCE};
use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::sync::atomic::Ordering;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use tracing_attributes::instrument;

type ServerBuilder = hyper::server::conn::http1::Builder;

#[instrument(skip_all, fields(listener_address = %listener.local_addr().unwrap()))]
pub async fn run_http_listener(
  listener: TcpListener,
  client_to_sink_push: async_channel::Sender<Bytes>,
  sink_to_client_pull: async_broadcast::Receiver<Bytes>,
  connection_sender: mpsc::Sender<(ConnectionState, ConnectionType)>,
  cancel: CancellationToken,
) {
  let listener_address = listener.local_addr().unwrap();
  let sink_to_client_pull_inactive = sink_to_client_pull.deactivate();
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
              client_to_sink_push.clone(),
              sink_to_client_pull_inactive.clone(),
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

fn handle_http_connection(
  client: TcpStream,
  client_to_sink_push: async_channel::Sender<Bytes>,
  sink_to_client_pull: async_broadcast::InactiveReceiver<Bytes>,
  connection_sender: mpsc::Sender<(ConnectionState, ConnectionType)>,
) {
  let io = TokioIo::new(client);
  let serving = service_fn(move |req| {
    let identifier = IDENTIFIER_SEQUENCE.fetch_add(1, Ordering::SeqCst);
    serve_http(
      req,
      identifier,
      client_to_sink_push.clone(),
      sink_to_client_pull.clone(),
      connection_sender.clone(),
    )
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

async fn serve_http(
  req: Request<hyper::body::Incoming>,
  identifier: u64,
  client_to_sink_push: async_channel::Sender<Bytes>,
  sink_to_client_pull: async_broadcast::InactiveReceiver<Bytes>,
  connection_sender: mpsc::Sender<(ConnectionState, ConnectionType)>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
  debug!("Received request: {:?}", req);

  if Method::CONNECT == req.method() {
    // Received an HTTP request like:
    // ```
    // CONNECT www.domain.com:443 HTTP/1.1
    // Host: www.domain.com:443
    // Proxy-Connection: Keep-Alive
    // ```
    //
    // When HTTP method is CONNECT we should return an empty body,
    // then we can eventually upgrade the connection and talk a new protocol.
    //
    // Note: only after client received an empty body with STATUS_OK can the
    // connection be upgraded, so we can't return a response inside
    // `on_upgrade` future.
    if let Some(addr) = host_addr(req.uri()) {
      let mut sink_to_client_pull = sink_to_client_pull.activate();
      let connect_result = crate::host::initiate_connection(
        identifier,
        addr.to_string(),
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
            }
          }
        });
      } else {
        info!("Failed to initialize connection, sending 400 status.");
        let mut resp = Response::new(empty());
        *resp.status_mut() = StatusCode::BAD_REQUEST;
        return Ok(resp);
      }

      Ok(Response::new(empty()))
    } else {
      error!("CONNECT host is not socket addr: {:?}", req.uri());
      let mut resp = Response::new(full("CONNECT must be to a socket address"));
      *resp.status_mut() = StatusCode::BAD_REQUEST;

      Ok(resp)
    }
  } else {
    let response = Response::builder()
      .status(StatusCode::BAD_REQUEST)
      .body(empty())
      .expect("Empty 400 response shouldn't have a reason to fail");
    Ok(response.map(|b| b.boxed()))
  }
}

fn host_addr(uri: &http::Uri) -> Option<String> {
  uri.authority().map(|auth| auth.to_string())
}

fn empty() -> BoxBody<Bytes, hyper::Error> {
  Empty::<Bytes>::new().map_err(|never| match never {}).boxed()
}

fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, hyper::Error> {
  Full::new(chunk.into()).map_err(|never| match never {}).boxed()
}
