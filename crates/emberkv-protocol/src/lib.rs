//! # emberkv-protocol
//!
//! A small, length-prefixed binary protocol. Every frame is
//!
//! ```text
//! +----------------+-------------+------------------+
//! | body_len (u32) | opcode (u8) | fields ...       |
//! +----------------+-------------+------------------+
//! ```
//!
//! All integers are big-endian (network order). Byte strings are encoded as a
//! `u32` length followed by the bytes. The full specification lives in
//! `docs/PROTOCOL.md`.
//!
//! The decoder is written to be safe against hostile input: frame sizes are
//! bounded *before* buffering, collection counts are checked against the
//! bytes actually present before allocating, and malformed input yields a
//! [`ProtocolError`] — never a panic.

#![warn(missing_docs)]

mod codec;
mod error;
mod message;

pub use codec::{ClientCodec, DEFAULT_MAX_FRAME_BYTES, FrameCodec, ServerCodec};
pub use error::ProtocolError;
pub use message::{ErrorCode, Message, Request, Response, StatsSnapshot};
