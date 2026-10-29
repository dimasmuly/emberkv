use std::fmt;
use std::marker::PhantomData;

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::ProtocolError;
use crate::message::{Message, Request, Response};

/// Default upper bound on a frame body: 32 MiB.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;

const LENGTH_PREFIX: usize = 4;

/// Length-delimited codec that decodes `In` and encodes `Out`.
pub struct FrameCodec<In, Out> {
    max_frame_bytes: usize,
    _marker: PhantomData<fn() -> (In, Out)>,
}

/// Server side: reads [`Request`]s, writes [`Response`]s.
pub type ServerCodec = FrameCodec<Request, Response>;
/// Client side: reads [`Response`]s, writes [`Request`]s.
pub type ClientCodec = FrameCodec<Response, Request>;

impl<In, Out> FrameCodec<In, Out> {
    /// Creates a codec rejecting frames larger than `max_frame_bytes`.
    ///
    /// # Panics
    ///
    /// If `max_frame_bytes` does not fit the 32-bit length prefix.
    pub fn new(max_frame_bytes: usize) -> Self {
        assert!(u32::try_from(max_frame_bytes).is_ok(), "max_frame_bytes must fit in u32");
        Self { max_frame_bytes, _marker: PhantomData }
    }

    /// The configured frame limit.
    pub fn max_frame_bytes(&self) -> usize {
        self.max_frame_bytes
    }
}

impl<In, Out> Default for FrameCodec<In, Out> {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_FRAME_BYTES)
    }
}

impl<In, Out> fmt::Debug for FrameCodec<In, Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrameCodec").field("max_frame_bytes", &self.max_frame_bytes).finish()
    }
}

impl<In: Message, Out> Decoder for FrameCodec<In, Out> {
    type Item = In;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<In>, ProtocolError> {
        let Some(prefix) = src.get(..LENGTH_PREFIX) else {
            return Ok(None);
        };
        let len = u32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]) as usize;
        // Reject before buffering: a peer must not be able to make us hold
        // gigabytes of memory by announcing a huge frame.
        if len > self.max_frame_bytes {
            return Err(ProtocolError::FrameTooLarge { len, max: self.max_frame_bytes });
        }
        let needed = LENGTH_PREFIX + len;
        if src.len() < needed {
            src.reserve(needed - src.len());
            return Ok(None);
        }
        src.advance(LENGTH_PREFIX);
        In::decode(src.split_to(len).freeze()).map(Some)
    }
}

impl<In, Out: Message> Encoder<Out> for FrameCodec<In, Out> {
    type Error = ProtocolError;

    fn encode(&mut self, item: Out, dst: &mut BytesMut) -> Result<(), ProtocolError> {
        let len = item.encoded_len();
        if len > self.max_frame_bytes {
            return Err(ProtocolError::FrameTooLarge { len, max: self.max_frame_bytes });
        }
        dst.reserve(LENGTH_PREFIX + len);
        dst.put_u32(len as u32);
        let start = dst.len();
        item.encode(dst);
        debug_assert_eq!(dst.len() - start, len, "encoded_len disagrees with encode");
        Ok(())
    }
}
