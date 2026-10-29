use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;

/// A type that can be carried in the body of a frame.
pub trait Message: Sized {
    /// Exact number of bytes [`encode`](Message::encode) will write.
    fn encoded_len(&self) -> usize;
    /// Appends the encoded body to `dst`.
    fn encode(&self, dst: &mut BytesMut);
    /// Parses a complete frame body. Zero-copy: byte fields share `src`.
    fn decode(src: Bytes) -> Result<Self, ProtocolError>;
}

/// A client → server request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// Liveness check.
    Ping,
    /// Read one key.
    Get {
        /// Key to read.
        key: Bytes,
    },
    /// Write one key.
    Put {
        /// Key to write.
        key: Bytes,
        /// Value to store.
        value: Bytes,
    },
    /// Remove one key.
    Delete {
        /// Key to remove.
        key: Bytes,
    },
    /// List keys sharing a prefix, in key order.
    Scan {
        /// Prefix to match; empty matches everything.
        prefix: Bytes,
        /// Maximum entries to return (the server may cap this further).
        limit: u32,
    },
    /// Engine statistics.
    Stats,
    /// Authenticate this connection with an API token.
    Auth {
        /// The token, as issued by `ekv token`.
        token: Bytes,
    },
}

/// Engine statistics as reported over the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StatsSnapshot {
    /// Number of live keys.
    pub keys: u64,
    /// Number of segment files.
    pub segments: u64,
    /// Bytes on disk.
    pub total_bytes: u64,
    /// Bytes still reachable.
    pub live_bytes: u64,
}

/// Machine-readable error categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// The request was malformed or invalid; retrying will not help.
    BadRequest,
    /// A key, value or response exceeded a size limit.
    TooLarge,
    /// The server failed internally.
    Internal,
    /// The server is temporarily unable to serve; retry later or elsewhere.
    Unavailable,
    /// Missing or invalid credentials, or insufficient access.
    Unauthorized,
    /// A code from a newer protocol version.
    Other(u16),
}

impl ErrorCode {
    fn to_u16(self) -> u16 {
        match self {
            Self::BadRequest => 1,
            Self::TooLarge => 2,
            Self::Internal => 3,
            Self::Unavailable => 4,
            Self::Unauthorized => 5,
            Self::Other(code) => code,
        }
    }

    fn from_u16(code: u16) -> Self {
        match code {
            1 => Self::BadRequest,
            2 => Self::TooLarge,
            3 => Self::Internal,
            4 => Self::Unavailable,
            5 => Self::Unauthorized,
            other => Self::Other(other),
        }
    }
}

/// A server → client response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// Reply to [`Request::Ping`].
    Pong,
    /// Reply to [`Request::Put`].
    Ok,
    /// Reply to [`Request::Get`].
    Value(Option<Bytes>),
    /// Reply to [`Request::Delete`]: whether the key existed.
    Deleted(bool),
    /// Reply to [`Request::Scan`].
    Entries(Vec<(Bytes, Bytes)>),
    /// Reply to [`Request::Stats`].
    Stats(StatsSnapshot),
    /// The request failed.
    Error {
        /// Category of failure.
        code: ErrorCode,
        /// Human-readable detail.
        message: String,
    },
}

impl Request {
    /// Stable, low-cardinality name for logs and metrics labels.
    pub fn op(&self) -> &'static str {
        match self {
            Self::Ping => "ping",
            Self::Get { .. } => "get",
            Self::Put { .. } => "put",
            Self::Delete { .. } => "delete",
            Self::Scan { .. } => "scan",
            Self::Stats => "stats",
            Self::Auth { .. } => "auth",
        }
    }
}

impl Response {
    /// Builds an error response.
    pub fn error(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::Error { code, message: message.into() }
    }

    /// Whether this is [`Response::Error`].
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error { .. })
    }
}

mod op {
    pub(super) const PING: u8 = 0x01;
    pub(super) const GET: u8 = 0x02;
    pub(super) const PUT: u8 = 0x03;
    pub(super) const DELETE: u8 = 0x04;
    pub(super) const SCAN: u8 = 0x05;
    pub(super) const STATS: u8 = 0x06;
    pub(super) const AUTH: u8 = 0x07;

    pub(super) const PONG: u8 = 0x81;
    pub(super) const OK: u8 = 0x82;
    pub(super) const VALUE: u8 = 0x83;
    pub(super) const DELETED: u8 = 0x84;
    pub(super) const ENTRIES: u8 = 0x85;
    pub(super) const STATS_REPLY: u8 = 0x86;
    pub(super) const ERROR: u8 = 0xFF;
}

const BLOB_PREFIX: usize = 4;

fn blob_len(bytes: &[u8]) -> usize {
    BLOB_PREFIX + bytes.len()
}

/// Lengths are bounded by the frame limit (checked from `encoded_len` before
/// encoding), so they always fit in a `u32`.
fn put_blob(dst: &mut BytesMut, bytes: &[u8]) {
    dst.put_u32(bytes.len() as u32);
    dst.put_slice(bytes);
}

/// Bounds-checked cursor over a frame body.
struct Reader(Bytes);

impl Reader {
    fn ensure(&self, n: usize) -> Result<(), ProtocolError> {
        if self.0.remaining() < n { Err(ProtocolError::Truncated) } else { Ok(()) }
    }

    fn u8(&mut self) -> Result<u8, ProtocolError> {
        self.ensure(1)?;
        Ok(self.0.get_u8())
    }

    fn u16(&mut self) -> Result<u16, ProtocolError> {
        self.ensure(2)?;
        Ok(self.0.get_u16())
    }

    fn u32(&mut self) -> Result<u32, ProtocolError> {
        self.ensure(4)?;
        Ok(self.0.get_u32())
    }

    fn u64(&mut self) -> Result<u64, ProtocolError> {
        self.ensure(8)?;
        Ok(self.0.get_u64())
    }

    fn bool(&mut self) -> Result<bool, ProtocolError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ProtocolError::InvalidField("boolean")),
        }
    }

    fn blob(&mut self) -> Result<Bytes, ProtocolError> {
        let len = self.u32()? as usize;
        self.ensure(len)?;
        Ok(self.0.split_to(len))
    }

    fn finish(self) -> Result<(), ProtocolError> {
        match self.0.remaining() {
            0 => Ok(()),
            n => Err(ProtocolError::TrailingBytes(n)),
        }
    }
}

impl Message for Request {
    fn encoded_len(&self) -> usize {
        1 + match self {
            Self::Ping | Self::Stats => 0,
            Self::Get { key } | Self::Delete { key } | Self::Auth { token: key } => blob_len(key),
            Self::Put { key, value } => blob_len(key) + blob_len(value),
            Self::Scan { prefix, .. } => blob_len(prefix) + 4,
        }
    }

    fn encode(&self, dst: &mut BytesMut) {
        match self {
            Self::Ping => dst.put_u8(op::PING),
            Self::Get { key } => {
                dst.put_u8(op::GET);
                put_blob(dst, key);
            }
            Self::Put { key, value } => {
                dst.put_u8(op::PUT);
                put_blob(dst, key);
                put_blob(dst, value);
            }
            Self::Delete { key } => {
                dst.put_u8(op::DELETE);
                put_blob(dst, key);
            }
            Self::Scan { prefix, limit } => {
                dst.put_u8(op::SCAN);
                put_blob(dst, prefix);
                dst.put_u32(*limit);
            }
            Self::Stats => dst.put_u8(op::STATS),
            Self::Auth { token } => {
                dst.put_u8(op::AUTH);
                put_blob(dst, token);
            }
        }
    }

    fn decode(src: Bytes) -> Result<Self, ProtocolError> {
        let mut r = Reader(src);
        let request = match r.u8()? {
            op::PING => Self::Ping,
            op::GET => Self::Get { key: r.blob()? },
            op::PUT => Self::Put { key: r.blob()?, value: r.blob()? },
            op::DELETE => Self::Delete { key: r.blob()? },
            op::SCAN => Self::Scan { prefix: r.blob()?, limit: r.u32()? },
            op::STATS => Self::Stats,
            op::AUTH => Self::Auth { token: r.blob()? },
            other => return Err(ProtocolError::UnknownOpcode(other)),
        };
        r.finish()?;
        Ok(request)
    }
}

impl Message for Response {
    fn encoded_len(&self) -> usize {
        1 + match self {
            Self::Pong | Self::Ok => 0,
            Self::Value(None) | Self::Deleted(_) => 1,
            Self::Value(Some(value)) => 1 + blob_len(value),
            Self::Entries(entries) => {
                4 + entries.iter().map(|(k, v)| blob_len(k) + blob_len(v)).sum::<usize>()
            }
            Self::Stats(_) => 4 * 8,
            Self::Error { message, .. } => 2 + blob_len(message.as_bytes()),
        }
    }

    fn encode(&self, dst: &mut BytesMut) {
        match self {
            Self::Pong => dst.put_u8(op::PONG),
            Self::Ok => dst.put_u8(op::OK),
            Self::Value(value) => {
                dst.put_u8(op::VALUE);
                dst.put_u8(u8::from(value.is_some()));
                if let Some(value) = value {
                    put_blob(dst, value);
                }
            }
            Self::Deleted(existed) => {
                dst.put_u8(op::DELETED);
                dst.put_u8(u8::from(*existed));
            }
            Self::Entries(entries) => {
                dst.put_u8(op::ENTRIES);
                dst.put_u32(entries.len() as u32);
                for (key, value) in entries {
                    put_blob(dst, key);
                    put_blob(dst, value);
                }
            }
            Self::Stats(stats) => {
                dst.put_u8(op::STATS_REPLY);
                dst.put_u64(stats.keys);
                dst.put_u64(stats.segments);
                dst.put_u64(stats.total_bytes);
                dst.put_u64(stats.live_bytes);
            }
            Self::Error { code, message } => {
                dst.put_u8(op::ERROR);
                dst.put_u16(code.to_u16());
                put_blob(dst, message.as_bytes());
            }
        }
    }

    fn decode(src: Bytes) -> Result<Self, ProtocolError> {
        let mut r = Reader(src);
        let response = match r.u8()? {
            op::PONG => Self::Pong,
            op::OK => Self::Ok,
            op::VALUE => Self::Value(if r.bool()? { Some(r.blob()?) } else { None }),
            op::DELETED => Self::Deleted(r.bool()?),
            op::ENTRIES => {
                let count = r.u32()? as usize;
                // Each entry needs at least two length prefixes. Checking this
                // first stops a tiny frame from forcing a huge allocation.
                if count > r.0.remaining() / (2 * BLOB_PREFIX) {
                    return Err(ProtocolError::InvalidField("entry count"));
                }
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    entries.push((r.blob()?, r.blob()?));
                }
                Self::Entries(entries)
            }
            op::STATS_REPLY => Self::Stats(StatsSnapshot {
                keys: r.u64()?,
                segments: r.u64()?,
                total_bytes: r.u64()?,
                live_bytes: r.u64()?,
            }),
            op::ERROR => {
                let code = ErrorCode::from_u16(r.u16()?);
                let message = String::from_utf8(r.blob()?.to_vec())
                    .map_err(|_| ProtocolError::InvalidField("error message (not UTF-8)"))?;
                Self::Error { code, message }
            }
            other => return Err(ProtocolError::UnknownOpcode(other)),
        };
        r.finish()?;
        Ok(response)
    }
}
