//! Every write the server accepts, as a deterministic [`Command`].
//!
//! The HTTP and binary front-ends translate requests into commands, and
//! [`apply`] executes a command inside one engine transaction. A standalone
//! server applies commands directly. A replicated server first commits them
//! to the Raft log, then every replica applies the same commands in the same
//! order. Applying a command depends only on the engine's state and on the
//! transaction's instant ([`Transaction::now_ms`]), never on the wall clock,
//! so all replicas reach the same state.
//!
//! Commands have a versioned binary encoding because they are stored in the
//! Raft log: an upgraded server must still read entries written by an older one.

use emberkv_engine::{Entry, Expiry, Transaction};
use serde::Serialize;
use sha2::{Digest, Sha256};

/// Bumped whenever the encoding changes; decoders accept every older version.
const VERSION: u8 = 1;

/// A write, with everything needed to apply it deterministically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Command {
    Put { key: Vec<u8>, value: Vec<u8>, ttl_ms: Option<u64>, condition: Option<Condition> },
    Delete { key: Vec<u8>, condition: Option<Condition> },
    Batch { ops: Vec<BatchOp> },
    Increment { key: Vec<u8>, by: i64, ttl_ms: Option<u64> },
    RateLimit { key: Vec<u8>, limit: u64, window_ms: u64, cost: u64 },
}

/// One member of [`Command::Batch`]: a put when `value` is set, else a delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BatchOp {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) ttl_ms: Option<u64>,
}

/// Conditional-request semantics (RFC 9110 §13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Condition {
    /// The current ETag must be one of these (`*` = the key exists).
    IfMatch(Vec<String>),
    /// The key must not exist (create-only).
    IfNoneMatch,
}

/// What applying a command did. Domain refusals (a failed precondition, a
/// non-integer counter) are outcomes, not errors: they are deterministic,
/// so every replica reaches the same verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    Written,
    Deleted { existed: bool },
    PreconditionFailed,
    Counter { value: i64, expires_at: Option<u64> },
    NotACounter,
    CounterOverflow,
    RateLimit(RateLimitDecision),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct RateLimitDecision {
    pub(crate) allowed: bool,
    pub(crate) limit: u64,
    pub(crate) remaining: u64,
    pub(crate) reset_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) retry_after_ms: Option<u64>,
}

/// A strong validator derived from the value itself, so no version counter
/// has to be stored: equal values have equal ETags.
pub(crate) fn etag(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let hex: String = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
    format!("\"{hex}\"")
}

impl Condition {
    fn allows(&self, current: Option<&Entry>) -> bool {
        match self {
            Self::IfNoneMatch => current.is_none(),
            Self::IfMatch(tags) => {
                current.is_some_and(|entry| tags.iter().any(|tag| tag == "*" || *tag == etag(&entry.value)))
            }
        }
    }
}

fn ttl(ttl_ms: Option<u64>) -> Expiry {
    ttl_ms.map_or(Expiry::Never, |ms| Expiry::After(std::time::Duration::from_millis(ms)))
}

fn parse_counter<T: std::str::FromStr>(value: &[u8]) -> Option<T> {
    std::str::from_utf8(value).ok()?.trim().parse().ok()
}

/// Applies `command` to the transaction's view of the engine.
pub(crate) fn apply(tx: &mut Transaction<'_>, command: &Command) -> emberkv_engine::Result<Outcome> {
    let now = tx.now_ms();
    match command {
        Command::Put { key, value, ttl_ms, condition } => {
            if let Some(condition) = condition
                && !condition.allows(tx.get(key)?.as_ref())
            {
                return Ok(Outcome::PreconditionFailed);
            }
            tx.put(key, value, ttl(*ttl_ms))?;
            Ok(Outcome::Written)
        }
        Command::Delete { key, condition } => {
            if let Some(condition) = condition
                && !condition.allows(tx.get(key)?.as_ref())
            {
                return Ok(Outcome::PreconditionFailed);
            }
            Ok(Outcome::Deleted { existed: tx.delete(key)? })
        }
        Command::Batch { ops } => {
            for op in ops {
                match &op.value {
                    Some(value) => tx.put(&op.key, value, ttl(op.ttl_ms))?,
                    None => {
                        tx.delete(&op.key)?;
                    }
                }
            }
            Ok(Outcome::Written)
        }
        Command::Increment { key, by, ttl_ms } => {
            let current = tx.get(key)?;
            let (count, expires_at) = match &current {
                None => (0, ttl_ms.map(|ms| now.saturating_add(ms))),
                Some(entry) => match parse_counter::<i64>(&entry.value) {
                    Some(count) => (count, entry.expires_at_ms),
                    None => return Ok(Outcome::NotACounter),
                },
            };
            let Some(next) = count.checked_add(*by) else { return Ok(Outcome::CounterOverflow) };
            let expiry = match (&current, expires_at) {
                (Some(_), _) => Expiry::KeepExisting,
                (None, Some(at)) => Expiry::AtUnixMs(at),
                (None, None) => Expiry::Never,
            };
            tx.put(key, next.to_string().as_bytes(), expiry)?;
            Ok(Outcome::Counter { value: next, expires_at })
        }
        Command::RateLimit { key, limit, window_ms, cost } => {
            let (limit, cost) = (*limit, *cost);
            let current = tx.get(key)?;
            let (used, reset_at) = match &current {
                None => (0, Some(now.saturating_add(*window_ms))),
                Some(entry) => match parse_counter::<u64>(&entry.value) {
                    Some(used) => (used, entry.expires_at_ms),
                    None => return Ok(Outcome::NotACounter),
                },
            };
            let next = used.saturating_add(cost);
            if next > limit {
                return Ok(Outcome::RateLimit(RateLimitDecision {
                    allowed: false,
                    limit,
                    remaining: limit.saturating_sub(used),
                    reset_at,
                    retry_after_ms: reset_at.map(|at| at.saturating_sub(now)),
                }));
            }
            let expiry = match (&current, reset_at) {
                (None, Some(at)) => Expiry::AtUnixMs(at),
                _ => Expiry::KeepExisting,
            };
            tx.put(key, next.to_string().as_bytes(), expiry)?;
            Ok(Outcome::RateLimit(RateLimitDecision {
                allowed: true,
                limit,
                remaining: limit - next,
                reset_at,
                retry_after_ms: None,
            }))
        }
    }
}

// ---------------------------------------------------------------- encoding

/// The input could not be decoded as a command.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("malformed command: {0}")]
pub(crate) struct DecodeError(&'static str);

impl Command {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Writer(vec![VERSION]);
        match self {
            Command::Put { key, value, ttl_ms, condition } => {
                out.u8(1);
                out.bytes(key);
                out.bytes(value);
                out.opt_u64(*ttl_ms);
                out.condition(condition.as_ref());
            }
            Command::Delete { key, condition } => {
                out.u8(2);
                out.bytes(key);
                out.condition(condition.as_ref());
            }
            Command::Batch { ops } => {
                out.u8(3);
                out.u32(ops.len() as u32);
                for op in ops {
                    out.bytes(&op.key);
                    match &op.value {
                        Some(value) => {
                            out.u8(1);
                            out.bytes(value);
                        }
                        None => out.u8(0),
                    }
                    out.opt_u64(op.ttl_ms);
                }
            }
            Command::Increment { key, by, ttl_ms } => {
                out.u8(4);
                out.bytes(key);
                out.u64(*by as u64);
                out.opt_u64(*ttl_ms);
            }
            Command::RateLimit { key, limit, window_ms, cost } => {
                out.u8(5);
                out.bytes(key);
                out.u64(*limit);
                out.u64(*window_ms);
                out.u64(*cost);
            }
        }
        out.0
    }

    pub(crate) fn decode(input: &[u8]) -> Result<Self, DecodeError> {
        let mut r = Reader(input);
        let version = r.u8()?;
        if version == 0 || version > VERSION {
            return Err(DecodeError("unknown version"));
        }
        let command = match r.u8()? {
            1 => Command::Put {
                key: r.bytes()?,
                value: r.bytes()?,
                ttl_ms: r.opt_u64()?,
                condition: r.condition()?,
            },
            2 => Command::Delete { key: r.bytes()?, condition: r.condition()? },
            3 => {
                let count = r.u32()? as usize;
                // Each op takes at least 13 bytes; refuse counts the input cannot hold.
                if count > r.0.len() / 13 {
                    return Err(DecodeError("batch count exceeds input"));
                }
                let mut ops = Vec::with_capacity(count);
                for _ in 0..count {
                    let key = r.bytes()?;
                    let value = match r.u8()? {
                        0 => None,
                        1 => Some(r.bytes()?),
                        _ => return Err(DecodeError("bad batch op tag")),
                    };
                    ops.push(BatchOp { key, value, ttl_ms: r.opt_u64()? });
                }
                Command::Batch { ops }
            }
            4 => Command::Increment { key: r.bytes()?, by: r.u64()? as i64, ttl_ms: r.opt_u64()? },
            5 => Command::RateLimit { key: r.bytes()?, limit: r.u64()?, window_ms: r.u64()?, cost: r.u64()? },
            _ => return Err(DecodeError("unknown command tag")),
        };
        if !r.0.is_empty() {
            return Err(DecodeError("trailing bytes"));
        }
        Ok(command)
    }
}

struct Writer(Vec<u8>);

impl Writer {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn bytes(&mut self, v: &[u8]) {
        self.u32(v.len() as u32);
        self.0.extend_from_slice(v);
    }
    fn opt_u64(&mut self, v: Option<u64>) {
        match v {
            Some(v) => {
                self.u8(1);
                self.u64(v);
            }
            None => self.u8(0),
        }
    }
    fn condition(&mut self, v: Option<&Condition>) {
        match v {
            None => self.u8(0),
            Some(Condition::IfNoneMatch) => self.u8(1),
            Some(Condition::IfMatch(tags)) => {
                self.u8(2);
                self.u32(tags.len() as u32);
                for tag in tags {
                    self.bytes(tag.as_bytes());
                }
            }
        }
    }
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], DecodeError> {
        if self.0.len() < n {
            return Err(DecodeError("truncated"));
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4 bytes")))
    }
    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8 bytes")))
    }
    fn bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }
    fn opt_u64(&mut self) -> Result<Option<u64>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            _ => Err(DecodeError("bad option tag")),
        }
    }
    fn condition(&mut self) -> Result<Option<Condition>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(Condition::IfNoneMatch)),
            2 => {
                let count = self.u32()? as usize;
                if count > self.0.len() / 4 {
                    return Err(DecodeError("tag count exceeds input"));
                }
                let tags = (0..count)
                    .map(|_| String::from_utf8(self.bytes()?).map_err(|_| DecodeError("tag is not UTF-8")))
                    .collect::<Result<_, _>>()?;
                Ok(Some(Condition::IfMatch(tags)))
            }
            _ => Err(DecodeError("bad condition tag")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples() -> Vec<Command> {
        vec![
            Command::Put { key: b"ns/a".to_vec(), value: b"{}".to_vec(), ttl_ms: None, condition: None },
            Command::Put {
                key: b"ns/b".to_vec(),
                value: vec![0, 255, 7],
                ttl_ms: Some(60_000),
                condition: Some(Condition::IfMatch(vec!["\"ab\"".into(), "*".into()])),
            },
            Command::Delete { key: b"ns/c".to_vec(), condition: Some(Condition::IfNoneMatch) },
            Command::Batch {
                ops: vec![
                    BatchOp { key: b"ns/d".to_vec(), value: Some(b"1".to_vec()), ttl_ms: Some(5) },
                    BatchOp { key: b"ns/e".to_vec(), value: None, ttl_ms: None },
                ],
            },
            Command::Increment { key: b"ns/f".to_vec(), by: -3, ttl_ms: Some(1) },
            Command::RateLimit { key: b"ns/g".to_vec(), limit: 5, window_ms: 900_000, cost: 2 },
        ]
    }

    #[test]
    fn commands_roundtrip() {
        for command in samples() {
            assert_eq!(Command::decode(&command.encode()), Ok(command));
        }
    }

    #[test]
    fn malformed_input_is_rejected_not_panicked_on() {
        for command in samples() {
            let bytes = command.encode();
            for cut in 0..bytes.len() {
                assert!(Command::decode(&bytes[..cut]).is_err(), "{command:?} cut at {cut}");
            }
            let mut longer = bytes.clone();
            longer.push(0);
            assert!(Command::decode(&longer).is_err());
        }
        assert!(Command::decode(&[2, 1]).is_err(), "future version");
        // A huge declared batch must not allocate before failing.
        assert!(Command::decode(&[1, 3, 255, 255, 255, 255]).is_err());
    }
}
