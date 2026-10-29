//! Per-connection request loop and request dispatch.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use emberkv_engine::{Engine, Error as EngineError};
use emberkv_protocol::{ErrorCode, Message, ProtocolError, Request, Response, ServerCodec, StatsSnapshot};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::auth::Authenticator;
use crate::config::ServerConfig;
use crate::executor::{ExecuteError, Executor};
use crate::state_machine::{Command, Outcome};
use crate::telemetry;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ConnectionSettings {
    pub(crate) idle_timeout: Duration,
    pub(crate) max_frame_bytes: usize,
    pub(crate) max_scan_limit: u32,
}

impl From<&ServerConfig> for ConnectionSettings {
    fn from(config: &ServerConfig) -> Self {
        Self {
            idle_timeout: config.idle_timeout,
            max_frame_bytes: config.max_frame_bytes,
            max_scan_limit: config.max_scan_limit,
        }
    }
}

/// Serves requests on one connection until the peer leaves, it idles out, it
/// sends garbage, or the server shuts down.
///
/// Shutdown is only observed *between* requests, so a request that has been
/// read is always answered: draining never drops acknowledged work.
///
/// With authentication enabled, a connection must send `AUTH` with an admin
/// token before anything but `PING`: the binary protocol addresses the raw
/// keyspace across all namespaces, so it is an admin-only interface. A
/// failed `AUTH` closes the connection, which slows down token guessing.
pub(crate) async fn serve(
    stream: TcpStream,
    engine: Engine,
    executor: Executor,
    auth: Option<Arc<Authenticator>>,
    shutdown: CancellationToken,
    settings: ConnectionSettings,
) -> Result<(), ProtocolError> {
    let mut framed = Framed::new(stream, ServerCodec::new(settings.max_frame_bytes));
    let mut authenticated = auth.is_none();
    loop {
        let request = tokio::select! {
            () = shutdown.cancelled() => {
                debug!("closing connection for shutdown");
                return Ok(());
            }
            next = tokio::time::timeout(settings.idle_timeout, framed.next()) => match next {
                Err(_) => {
                    debug!("closing idle connection");
                    return Ok(());
                }
                Ok(None) => return Ok(()),
                Ok(Some(Ok(request))) => request,
                Ok(Some(Err(err))) => {
                    // The byte stream can no longer be trusted to be aligned
                    // on frame boundaries, so explain and hang up.
                    let _ = framed.send(Response::error(ErrorCode::BadRequest, err.to_string())).await;
                    return Err(err);
                }
            },
        };
        let response = match request {
            Request::Auth { token } => {
                let principal = auth.as_ref().map(|auth| auth.authenticate(&token));
                match principal {
                    None => Response::Ok,
                    Some(Some(principal)) if principal.is_admin() => {
                        debug!(key = principal.name(), "connection authenticated");
                        authenticated = true;
                        Response::Ok
                    }
                    Some(_) => {
                        let denied =
                            Response::error(ErrorCode::Unauthorized, "invalid token or not an admin key");
                        framed.send(denied).await?;
                        return Ok(());
                    }
                }
            }
            Request::Ping => Response::Pong,
            _ if !authenticated => {
                Response::error(ErrorCode::Unauthorized, "send AUTH with an admin token first")
            }
            request => handle(&engine, &executor, request, settings).await,
        };
        framed.send(response).await?;
    }
}

async fn handle(
    engine: &Engine,
    executor: &Executor,
    request: Request,
    settings: ConnectionSettings,
) -> Response {
    let op = request.op();
    let started = Instant::now();
    let response = match request {
        Request::Ping => Response::Pong,
        // Writes go through the executor, like HTTP writes.
        Request::Put { key, value } => {
            let command =
                Command::Put { key: key.to_vec(), value: value.to_vec(), ttl_ms: None, condition: None };
            match executor.execute(command).await {
                Ok(Outcome::Written) => Response::Ok,
                other => write_error(other),
            }
        }
        Request::Delete { key } => {
            match executor.execute(Command::Delete { key: key.to_vec(), condition: None }).await {
                Ok(Outcome::Deleted { existed }) => Response::Deleted(existed),
                other => write_error(other),
            }
        }
        // Engine calls block on disk I/O (and possibly fsync), so they run on
        // the blocking pool instead of stalling an async worker thread.
        request => {
            let engine = engine.clone();
            let max_scan = settings.max_scan_limit;
            match tokio::task::spawn_blocking(move || execute(&engine, request, max_scan)).await {
                Ok(response) => response,
                Err(err) => {
                    error!(op, error = %err, "request handler panicked");
                    Response::error(ErrorCode::Internal, "internal error")
                }
            }
        }
    };
    let response = if response.encoded_len() > settings.max_frame_bytes {
        Response::error(
            ErrorCode::TooLarge,
            format!("response exceeds the {}-byte frame limit; narrow the request", settings.max_frame_bytes),
        )
    } else {
        response
    };
    telemetry::record_request("tcp", op, response.is_error(), started.elapsed());
    response
}

fn execute(engine: &Engine, request: Request, max_scan: u32) -> Response {
    let result = match request {
        Request::Ping => Ok(Response::Pong),
        // Handled by the connection loop before dispatch.
        Request::Auth { .. } => Ok(Response::error(ErrorCode::BadRequest, "unexpected AUTH")),
        Request::Get { key } => engine.get(&key).map(|value| Response::Value(value.map(Bytes::from))),
        Request::Put { .. } | Request::Delete { .. } => {
            Ok(Response::error(ErrorCode::Internal, "write on the read path"))
        }
        Request::Scan { prefix, limit } => {
            engine.scan_prefix(&prefix, limit.min(max_scan) as usize).map(|entries| {
                Response::Entries(
                    entries.into_iter().map(|(k, v)| (Bytes::from(k), Bytes::from(v))).collect(),
                )
            })
        }
        Request::Stats => {
            let stats = engine.stats();
            Ok(Response::Stats(StatsSnapshot {
                keys: stats.keys as u64,
                segments: stats.segments as u64,
                total_bytes: stats.total_bytes,
                live_bytes: stats.live_bytes,
            }))
        }
    };
    result.unwrap_or_else(storage_error)
}

fn write_error(result: Result<Outcome, ExecuteError>) -> Response {
    match result {
        Err(ExecuteError::Storage(err)) => storage_error(err),
        Err(ExecuteError::Panicked) => Response::error(ErrorCode::Internal, "internal error"),
        Ok(outcome) => {
            error!(?outcome, "unexpected outcome for a binary-protocol write");
            Response::error(ErrorCode::Internal, "internal error")
        }
    }
}

/// Client mistakes are reported verbatim; internal failures are logged with
/// full detail but surfaced to clients generically.
fn storage_error(err: EngineError) -> Response {
    match err {
        EngineError::EmptyKey => Response::error(ErrorCode::BadRequest, err.to_string()),
        EngineError::KeyTooLarge { .. }
        | EngineError::ValueTooLarge { .. }
        | EngineError::BatchTooLarge { .. } => Response::error(ErrorCode::TooLarge, err.to_string()),
        EngineError::Poisoned => {
            error!(error = %err, "storage engine is poisoned");
            Response::error(ErrorCode::Unavailable, "storage is read-only after an I/O failure")
        }
        other => {
            error!(error = %other, "storage error");
            Response::error(ErrorCode::Internal, "internal storage error")
        }
    }
}
