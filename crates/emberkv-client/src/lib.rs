//! # emberkv-client
//!
//! An async client for `emberkv-server`. One [`Client`] owns one TCP
//! connection and issues one request at a time; open several clients for
//! concurrency.
//!
//! ```no_run
//! # async fn demo() -> Result<(), emberkv_client::ClientError> {
//! let mut client = emberkv_client::Client::connect("127.0.0.1:7878").await?;
//! client.put("greeting", "hello").await?;
//! assert_eq!(client.get("greeting").await?.as_deref(), Some(&b"hello"[..]));
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

use std::io;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::codec::Framed;

use emberkv_protocol::ClientCodec;
pub use emberkv_protocol::{ErrorCode, ProtocolError, Request, Response, StatsSnapshot};

/// Default per-request deadline.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Errors returned by [`Client`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// Transport failure.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// The server sent bytes we could not parse.
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    /// The server processed the request and reported a failure.
    #[error("server error ({code:?}): {message}")]
    Server {
        /// Category of failure.
        code: ErrorCode,
        /// Detail from the server.
        message: String,
    },
    /// The server closed the connection.
    #[error("connection closed by server")]
    Closed,
    /// No response arrived within the deadline.
    #[error("request timed out after {0:?}")]
    Timeout(Duration),
    /// A previous failure left the connection in an unknown state.
    #[error("connection is unusable after an earlier failure; reconnect")]
    Broken,
    /// The server answered with a response of the wrong type.
    #[error("unexpected response to {op}: {response:?}")]
    UnexpectedResponse {
        /// Request that was sent.
        op: &'static str,
        /// What came back instead.
        response: Box<Response>,
    },
}

/// A connection to an emberkv server.
#[derive(Debug)]
pub struct Client {
    framed: Framed<TcpStream, ClientCodec>,
    timeout: Duration,
    broken: bool,
}

impl Client {
    /// Connects to `addr` with [`DEFAULT_TIMEOUT`] as the request deadline.
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            framed: Framed::new(stream, ClientCodec::default()),
            timeout: DEFAULT_TIMEOUT,
            broken: false,
        })
    }

    /// Sets the per-request deadline.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Round-trips a liveness check.
    pub async fn ping(&mut self) -> Result<(), ClientError> {
        match self.call(Request::Ping).await? {
            Response::Pong => Ok(()),
            other => Err(unexpected("ping", other)),
        }
    }

    /// Authenticates the connection. Required before any other request
    /// (except ping) when the server has authentication enabled.
    pub async fn authenticate(&mut self, token: impl Into<Bytes>) -> Result<(), ClientError> {
        match self.call(Request::Auth { token: token.into() }).await? {
            Response::Ok => Ok(()),
            other => Err(unexpected("auth", other)),
        }
    }

    /// Reads `key`.
    pub async fn get(&mut self, key: impl Into<Bytes>) -> Result<Option<Bytes>, ClientError> {
        match self.call(Request::Get { key: key.into() }).await? {
            Response::Value(value) => Ok(value),
            other => Err(unexpected("get", other)),
        }
    }

    /// Writes `key = value`.
    pub async fn put(&mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<(), ClientError> {
        match self.call(Request::Put { key: key.into(), value: value.into() }).await? {
            Response::Ok => Ok(()),
            other => Err(unexpected("put", other)),
        }
    }

    /// Deletes `key`, returning whether it existed.
    pub async fn delete(&mut self, key: impl Into<Bytes>) -> Result<bool, ClientError> {
        match self.call(Request::Delete { key: key.into() }).await? {
            Response::Deleted(existed) => Ok(existed),
            other => Err(unexpected("delete", other)),
        }
    }

    /// Lists up to `limit` entries whose keys start with `prefix`.
    pub async fn scan_prefix(
        &mut self,
        prefix: impl Into<Bytes>,
        limit: u32,
    ) -> Result<Vec<(Bytes, Bytes)>, ClientError> {
        match self.call(Request::Scan { prefix: prefix.into(), limit }).await? {
            Response::Entries(entries) => Ok(entries),
            other => Err(unexpected("scan", other)),
        }
    }

    /// Fetches engine statistics.
    pub async fn stats(&mut self) -> Result<StatsSnapshot, ClientError> {
        match self.call(Request::Stats).await? {
            Response::Stats(stats) => Ok(stats),
            other => Err(unexpected("stats", other)),
        }
    }

    /// Sends one request and waits for its response.
    ///
    /// Requests and responses are matched by order, so after a timeout or a
    /// transport error a late response could be mistaken for the answer to
    /// the *next* request. The client therefore refuses further use.
    pub async fn call(&mut self, request: Request) -> Result<Response, ClientError> {
        if self.broken {
            return Err(ClientError::Broken);
        }
        let deadline = self.timeout;
        let result = match tokio::time::timeout(deadline, self.round_trip(request)).await {
            Ok(result) => result,
            Err(_) => Err(ClientError::Timeout(deadline)),
        };
        match result {
            Ok(Response::Error { code, message }) => Err(ClientError::Server { code, message }),
            Ok(response) => Ok(response),
            Err(err) => {
                self.broken = true;
                Err(err)
            }
        }
    }

    async fn round_trip(&mut self, request: Request) -> Result<Response, ClientError> {
        self.framed.send(request).await?;
        match self.framed.next().await {
            Some(response) => Ok(response?),
            None => Err(ClientError::Closed),
        }
    }
}

fn unexpected(op: &'static str, response: Response) -> ClientError {
    ClientError::UnexpectedResponse { op, response: Box::new(response) }
}
