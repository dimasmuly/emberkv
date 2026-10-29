use bytes::{Bytes, BytesMut};
use emberkv_protocol::{
    ClientCodec, ErrorCode, FrameCodec, Message, ProtocolError, Request, Response, ServerCodec, StatsSnapshot,
};
use proptest::collection::vec;
use proptest::prelude::*;
use tokio_util::codec::{Decoder, Encoder};

fn blob() -> impl Strategy<Value = Bytes> {
    vec(any::<u8>(), 0..64).prop_map(Bytes::from)
}

fn request() -> impl Strategy<Value = Request> {
    prop_oneof![
        Just(Request::Ping),
        Just(Request::Stats),
        blob().prop_map(|key| Request::Get { key }),
        blob().prop_map(|key| Request::Delete { key }),
        (blob(), blob()).prop_map(|(key, value)| Request::Put { key, value }),
        (blob(), any::<u32>()).prop_map(|(prefix, limit)| Request::Scan { prefix, limit }),
        blob().prop_map(|token| Request::Auth { token }),
    ]
}

fn response() -> impl Strategy<Value = Response> {
    prop_oneof![
        Just(Response::Pong),
        Just(Response::Ok),
        proptest::option::of(blob()).prop_map(Response::Value),
        any::<bool>().prop_map(Response::Deleted),
        vec((blob(), blob()), 0..8).prop_map(Response::Entries),
        any::<[u64; 4]>().prop_map(|[keys, segments, total_bytes, live_bytes]| {
            Response::Stats(StatsSnapshot { keys, segments, total_bytes, live_bytes })
        }),
        (any::<u16>(), ".{0,32}").prop_map(|(code, message)| Response::Error {
            code: match code % 6 {
                0 => ErrorCode::BadRequest,
                1 => ErrorCode::TooLarge,
                2 => ErrorCode::Internal,
                3 => ErrorCode::Unavailable,
                4 => ErrorCode::Unauthorized,
                _ => ErrorCode::Other(code.max(1000)),
            },
            message,
        }),
    ]
}

/// Feeds `wire` to the decoder in chunks of `chunk` bytes, as TCP might.
fn decode_chunked<In: Message, Out>(codec: &mut FrameCodec<In, Out>, wire: &[u8], chunk: usize) -> Vec<In> {
    let mut buf = BytesMut::new();
    let mut out = Vec::new();
    for piece in wire.chunks(chunk) {
        buf.extend_from_slice(piece);
        while let Some(msg) = codec.decode(&mut buf).unwrap() {
            out.push(msg);
        }
    }
    assert!(buf.is_empty(), "leftover bytes after decoding");
    out
}

proptest! {
    #[test]
    fn requests_roundtrip_across_arbitrary_chunking(
        requests in vec(request(), 1..16),
        chunk in 1usize..64,
    ) {
        let mut wire = BytesMut::new();
        let mut client = ClientCodec::default();
        for req in &requests {
            client.encode(req.clone(), &mut wire).unwrap();
        }
        let decoded = decode_chunked(&mut ServerCodec::default(), &wire, chunk);
        prop_assert_eq!(decoded, requests);
    }

    #[test]
    fn responses_roundtrip_across_arbitrary_chunking(
        responses in vec(response(), 1..16),
        chunk in 1usize..64,
    ) {
        let mut wire = BytesMut::new();
        let mut server = ServerCodec::default();
        for resp in &responses {
            server.encode(resp.clone(), &mut wire).unwrap();
        }
        let decoded = decode_chunked(&mut ClientCodec::default(), &wire, chunk);
        prop_assert_eq!(decoded, responses);
    }

    #[test]
    fn encoded_len_is_exact(req in request(), resp in response()) {
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        prop_assert_eq!(buf.len(), req.encoded_len());
        buf.clear();
        resp.encode(&mut buf);
        prop_assert_eq!(buf.len(), resp.encoded_len());
    }

    /// Fuzz-style: arbitrary bytes must produce `Ok` or `Err`, never a panic.
    #[test]
    fn arbitrary_input_never_panics(body in vec(any::<u8>(), 0..256)) {
        let _ = Request::decode(Bytes::from(body.clone()));
        let _ = Response::decode(Bytes::from(body.clone()));
        let mut buf = BytesMut::from(&body[..]);
        let _ = ServerCodec::new(128).decode(&mut buf);
    }
}

#[test]
fn oversized_frame_is_rejected_before_buffering() {
    let mut codec = ServerCodec::new(1024);
    let mut buf = BytesMut::from(&u32::MAX.to_be_bytes()[..]);
    let err = codec.decode(&mut buf).unwrap_err();
    assert!(matches!(err, ProtocolError::FrameTooLarge { len, max: 1024 } if len == u32::MAX as usize));
}

#[test]
fn oversized_message_is_not_encoded() {
    let mut codec = ClientCodec::new(16);
    let mut buf = BytesMut::new();
    let req = Request::Put { key: Bytes::from_static(b"k"), value: Bytes::from(vec![0; 64]) };
    assert!(matches!(codec.encode(req, &mut buf), Err(ProtocolError::FrameTooLarge { .. })));
    assert!(buf.is_empty());
}

#[test]
fn lying_entry_count_cannot_force_allocation() {
    // Opcode ENTRIES claiming 2^32-1 entries inside a 5-byte body.
    let body = Bytes::from_static(&[0x85, 0xFF, 0xFF, 0xFF, 0xFF]);
    assert!(matches!(Response::decode(body), Err(ProtocolError::InvalidField(_))));
}

#[test]
fn trailing_bytes_are_rejected() {
    let body = Bytes::from_static(&[0x01, 0x00]);
    assert!(matches!(Request::decode(body), Err(ProtocolError::TrailingBytes(1))));
}

#[test]
fn unknown_opcode_is_rejected() {
    assert!(matches!(Request::decode(Bytes::from_static(&[0x7F])), Err(ProtocolError::UnknownOpcode(0x7F))));
}
