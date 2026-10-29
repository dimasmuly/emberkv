//! On-disk record format.
//!
//! ```text
//! +-----------+----------+-------------+---------------+-------------------+---------+-----------+
//! | crc32 (4) | kind (1) | key_len (4) | value_len (4) | expires_at (0|8)  | key ... | value ... |
//! +-----------+----------+-------------+---------------+-------------------+---------+-----------+
//! ```
//!
//! `kind` is an operation in the low nibble plus flags in the high bits:
//!
//! | bits   | meaning                                                            |
//! |--------|--------------------------------------------------------------------|
//! | `0x0F` | op: `1` put, `2` delete (tombstone), `3` batch commit               |
//! | `0x40` | EXPIRES: an 8-byte expiry (unix ms) follows the header             |
//! | `0x80` | BATCH: member of an atomic batch, applied only once committed      |
//! | `0x30` | reserved, must be zero                                             |
//!
//! Integers are little-endian. The CRC covers every byte after the checksum
//! field. Plain put/delete records are byte-identical to the v0.1 format, so
//! old data directories remain readable.

use std::fmt;

/// Hard upper bound on key length imposed by the file format.
pub const MAX_KEY_LEN: usize = 64 * 1024;
/// Hard upper bound on value length imposed by the file format.
pub const MAX_VALUE_LEN: usize = 64 * 1024 * 1024;

pub(crate) const HEADER_LEN: usize = 4 + 1 + 4 + 4;
const EXPIRY_LEN: usize = 8;

const OP_MASK: u8 = 0x0F;
const RESERVED: u8 = 0x30;
const FLAG_EXPIRES: u8 = 0x40;
const FLAG_BATCH: u8 = 0x80;

const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;
const OP_COMMIT: u8 = 3;

/// Key used by commit records; never indexed.
const COMMIT_KEY: &[u8] = &[0];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    Put,
    Delete,
    Commit,
}

impl Op {
    fn as_byte(self) -> u8 {
        match self {
            Op::Put => OP_PUT,
            Op::Delete => OP_DELETE,
            Op::Commit => OP_COMMIT,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Header {
    pub(crate) crc: u32,
    pub(crate) op: Op,
    pub(crate) batch: bool,
    pub(crate) expires: bool,
    pub(crate) key_len: u32,
    pub(crate) value_len: u32,
}

impl Header {
    /// Parses and sanity-checks a header. Lengths are bounded by the format
    /// limits so a corrupted length can never trigger a huge allocation.
    pub(crate) fn parse(raw: &[u8; HEADER_LEN]) -> Result<Self, DecodeError> {
        let u32_at = |at: usize| u32::from_le_bytes([raw[at], raw[at + 1], raw[at + 2], raw[at + 3]]);
        let kind = raw[4];
        let op = match kind & OP_MASK {
            _ if kind & RESERVED != 0 => return Err(DecodeError::UnknownKind(kind)),
            OP_PUT => Op::Put,
            OP_DELETE => Op::Delete,
            OP_COMMIT => Op::Commit,
            _ => return Err(DecodeError::UnknownKind(kind)),
        };
        let header = Self {
            crc: u32_at(0),
            op,
            batch: kind & FLAG_BATCH != 0,
            expires: kind & FLAG_EXPIRES != 0,
            key_len: u32_at(5),
            value_len: u32_at(9),
        };
        let key_ok = header.key_len > 0 && header.key_len as usize <= MAX_KEY_LEN;
        let valid = match op {
            Op::Put => key_ok && header.value_len as usize <= MAX_VALUE_LEN,
            Op::Delete => key_ok && header.value_len == 0 && !header.expires,
            Op::Commit => header.key_len == 1 && header.value_len == 4 && !header.expires && !header.batch,
        };
        if !valid {
            return Err(DecodeError::Malformed {
                kind,
                key_len: header.key_len,
                value_len: header.value_len,
            });
        }
        Ok(header)
    }

    pub(crate) fn record_len(&self) -> u64 {
        HEADER_LEN as u64 + self.expiry_len() as u64 + u64::from(self.key_len) + u64::from(self.value_len)
    }

    pub(crate) fn expiry_len(&self) -> usize {
        if self.expires { EXPIRY_LEN } else { 0 }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DecodeError {
    Truncated,
    UnknownKind(u8),
    Malformed { kind: u8, key_len: u32, value_len: u32 },
    LengthMismatch { expected: u64, actual: u64 },
    ChecksumMismatch { stored: u32, computed: u32 },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => f.write_str("record is truncated"),
            Self::UnknownKind(kind) => write!(f, "unknown record kind {kind:#04x}"),
            Self::Malformed { kind, key_len, value_len } => {
                write!(f, "malformed record (kind={kind:#04x}, key={key_len}, value={value_len})")
            }
            Self::LengthMismatch { expected, actual } => {
                write!(f, "record length mismatch (expected {expected}, got {actual})")
            }
            Self::ChecksumMismatch { stored, computed } => {
                write!(f, "checksum mismatch (stored {stored:#010x}, computed {computed:#010x})")
            }
        }
    }
}

/// What to write: one record, before encoding.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Spec<'a> {
    pub(crate) op: Op,
    pub(crate) batch: bool,
    pub(crate) expires_at: Option<u64>,
    pub(crate) key: &'a [u8],
    pub(crate) value: &'a [u8],
}

impl<'a> Spec<'a> {
    pub(crate) fn put(key: &'a [u8], value: &'a [u8], expires_at: Option<u64>) -> Self {
        Self { op: Op::Put, batch: false, expires_at, key, value }
    }

    pub(crate) fn delete(key: &'a [u8]) -> Self {
        Self { op: Op::Delete, batch: false, expires_at: None, key, value: &[] }
    }

    pub(crate) fn in_batch(self) -> Self {
        Self { batch: true, ..self }
    }
}

pub(crate) fn encoded_len(key_len: usize, value_len: usize, expires: bool) -> usize {
    HEADER_LEN + if expires { EXPIRY_LEN } else { 0 } + key_len + value_len
}

/// Appends one encoded record to `buf` and returns its length.
///
/// Callers validate lengths against [`MAX_KEY_LEN`] / [`MAX_VALUE_LEN`]
/// first, so the `u32` conversions below cannot truncate.
pub(crate) fn append(buf: &mut Vec<u8>, spec: &Spec<'_>) -> usize {
    debug_assert!(spec.key.len() <= MAX_KEY_LEN && spec.value.len() <= MAX_VALUE_LEN);
    let start = buf.len();
    let mut kind = spec.op.as_byte();
    if spec.batch {
        kind |= FLAG_BATCH;
    }
    if spec.expires_at.is_some() {
        kind |= FLAG_EXPIRES;
    }
    buf.reserve(encoded_len(spec.key.len(), spec.value.len(), spec.expires_at.is_some()));
    buf.extend_from_slice(&[0; 4]);
    buf.push(kind);
    buf.extend_from_slice(&(spec.key.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(spec.value.len() as u32).to_le_bytes());
    if let Some(expires_at) = spec.expires_at {
        buf.extend_from_slice(&expires_at.to_le_bytes());
    }
    buf.extend_from_slice(spec.key);
    buf.extend_from_slice(spec.value);
    let crc = crc32fast::hash(&buf[start + 4..]);
    buf[start..start + 4].copy_from_slice(&crc.to_le_bytes());
    buf.len() - start
}

/// Appends the record that makes the preceding `count` batch records visible.
pub(crate) fn append_commit(buf: &mut Vec<u8>, count: u32) -> usize {
    let count = count.to_le_bytes();
    let spec = Spec { op: Op::Commit, batch: false, expires_at: None, key: COMMIT_KEY, value: &count };
    append(buf, &spec)
}

/// Number of batch records a commit record covers.
pub(crate) fn commit_count(value: &[u8]) -> u32 {
    value.try_into().map(u32::from_le_bytes).unwrap_or(u32::MAX)
}

/// Turns an encoded batch member into a standalone record of the same
/// length. Compaction uses this when it copies a committed batch member,
/// because the commit record itself is not copied.
pub(crate) fn clear_batch_flag(buf: &mut [u8]) {
    buf[4] &= !FLAG_BATCH;
    let crc = crc32fast::hash(&buf[4..]);
    buf[..4].copy_from_slice(&crc.to_le_bytes());
}

#[derive(Debug)]
pub(crate) struct RecordRef<'a> {
    pub(crate) op: Op,
    pub(crate) batch: bool,
    pub(crate) expires_at: Option<u64>,
    pub(crate) key: &'a [u8],
    pub(crate) value: &'a [u8],
}

/// Splits a validated payload (everything after the header) into its parts.
pub(crate) fn split_payload<'a>(header: &Header, payload: &'a [u8]) -> (Option<u64>, &'a [u8], &'a [u8]) {
    let (expiry, rest) = payload.split_at(header.expiry_len());
    let expires_at = header.expires.then(|| {
        u64::from_le_bytes(expiry.try_into().expect("expiry is exactly 8 bytes when the flag is set"))
    });
    let (key, value) = rest.split_at(header.key_len as usize);
    (expires_at, key, value)
}

/// Decodes exactly one complete record, verifying its checksum.
pub(crate) fn decode(buf: &[u8]) -> Result<RecordRef<'_>, DecodeError> {
    let raw: &[u8; HEADER_LEN] =
        buf.get(..HEADER_LEN).and_then(|bytes| bytes.try_into().ok()).ok_or(DecodeError::Truncated)?;
    let header = Header::parse(raw)?;
    if header.record_len() != buf.len() as u64 {
        return Err(DecodeError::LengthMismatch { expected: header.record_len(), actual: buf.len() as u64 });
    }
    let computed = crc32fast::hash(&buf[4..]);
    if computed != header.crc {
        return Err(DecodeError::ChecksumMismatch { stored: header.crc, computed });
    }
    let (expires_at, key, value) = split_payload(&header, &buf[HEADER_LEN..]);
    Ok(RecordRef { op: header.op, batch: header.batch, expires_at, key, value })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(spec: Spec<'_>) -> Vec<u8> {
        let mut buf = Vec::new();
        let len = append(&mut buf, &spec);
        assert_eq!(len, buf.len());
        buf
    }

    #[test]
    fn put_roundtrip() {
        let buf = encode(Spec::put(b"key", b"value", None));
        assert_eq!(buf.len(), encoded_len(3, 5, false));
        let record = decode(&buf).unwrap();
        assert_eq!((record.op, record.batch, record.expires_at), (Op::Put, false, None));
        assert_eq!((record.key, record.value), (&b"key"[..], &b"value"[..]));
    }

    #[test]
    fn expiring_batch_put_roundtrip() {
        let buf = encode(Spec::put(b"key", b"value", Some(1_700_000_000_000)).in_batch());
        assert_eq!(buf.len(), encoded_len(3, 5, true));
        let record = decode(&buf).unwrap();
        assert_eq!((record.op, record.batch, record.expires_at), (Op::Put, true, Some(1_700_000_000_000)));
        assert_eq!((record.key, record.value), (&b"key"[..], &b"value"[..]));
    }

    #[test]
    fn tombstone_and_commit_roundtrip() {
        let record = encode(Spec::delete(b"key"));
        assert_eq!(decode(&record).unwrap().op, Op::Delete);

        let mut buf = Vec::new();
        append_commit(&mut buf, 7);
        let commit = decode(&buf).unwrap();
        assert_eq!(commit.op, Op::Commit);
        assert_eq!(commit_count(commit.value), 7);
    }

    #[test]
    fn v0_1_records_are_still_readable() {
        // Hand-built v0.1 record: kind byte 1, no flags.
        let mut buf = vec![0, 0, 0, 0, 1];
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(b"kv");
        let crc = crc32fast::hash(&buf[4..]);
        buf[..4].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(buf, encode(Spec::put(b"k", b"v", None)));
    }

    #[test]
    fn clearing_the_batch_flag_keeps_the_record_valid() {
        let mut buf = encode(Spec::put(b"key", b"value", Some(42)).in_batch());
        clear_batch_flag(&mut buf);
        let record = decode(&buf).unwrap();
        assert!(!record.batch);
        assert_eq!(record.expires_at, Some(42));
    }

    #[test]
    fn every_single_bit_flip_is_detected() {
        let clean = encode(Spec::put(b"some-key", b"some-value", Some(99)));
        for byte in 0..clean.len() {
            for bit in 0..8 {
                let mut damaged = clean.clone();
                damaged[byte] ^= 1 << bit;
                assert!(decode(&damaged).is_err(), "flip at byte {byte} bit {bit} undetected");
            }
        }
    }

    #[test]
    fn truncation_is_detected() {
        let buf = encode(Spec::put(b"key", b"value", None));
        for len in 0..buf.len() {
            assert!(decode(&buf[..len]).is_err());
        }
    }

    #[test]
    fn malformed_headers_are_rejected_before_allocation() {
        let mut buf = encode(Spec::put(b"key", b"value", None));
        buf[9..13].copy_from_slice(&u32::MAX.to_le_bytes());
        let raw: &[u8; HEADER_LEN] = buf[..HEADER_LEN].try_into().unwrap();
        assert!(matches!(Header::parse(raw), Err(DecodeError::Malformed { .. })));

        let mut reserved = encode(Spec::put(b"key", b"value", None));
        reserved[4] |= 0x10;
        let raw: &[u8; HEADER_LEN] = reserved[..HEADER_LEN].try_into().unwrap();
        assert!(matches!(Header::parse(raw), Err(DecodeError::UnknownKind(_))));
    }
}
