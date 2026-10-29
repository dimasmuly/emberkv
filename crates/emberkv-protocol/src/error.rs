use std::io;

/// Errors produced while framing or parsing messages.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// Transport failure.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// A frame announced (or would need) more bytes than allowed.
    #[error("frame of {len} bytes exceeds the {max}-byte limit")]
    FrameTooLarge {
        /// Announced or required size.
        len: usize,
        /// Configured limit.
        max: usize,
    },

    /// The opcode is not part of this protocol version.
    #[error("unknown opcode {0:#04x}")]
    UnknownOpcode(u8),

    /// The frame ended in the middle of a field.
    #[error("frame ended unexpectedly")]
    Truncated,

    /// The frame carried bytes after the last field.
    #[error("{0} unexpected trailing bytes in frame")]
    TrailingBytes(usize),

    /// A field held a value outside its domain.
    #[error("invalid {0}")]
    InvalidField(&'static str),
}
