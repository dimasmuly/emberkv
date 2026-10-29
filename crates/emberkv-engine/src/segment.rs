//! Segment files: naming, discovery, positional reads and sequential scans.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};

use crate::record::{self, HEADER_LEN, Header, Op};

pub(crate) type SegmentId = u64;

const SEGMENT_SUFFIX: &str = ".log";
const TMP_SUFFIX: &str = ".log.tmp";

pub(crate) fn path(dir: &Path, id: SegmentId) -> PathBuf {
    dir.join(format!("{id:020}{SEGMENT_SUFFIX}"))
}

pub(crate) fn tmp_path(dir: &Path, id: SegmentId) -> PathBuf {
    dir.join(format!("{id:020}{TMP_SUFFIX}"))
}

#[derive(Debug, Default)]
pub(crate) struct Listing {
    /// Segment ids in ascending (= replay) order.
    pub(crate) segments: Vec<SegmentId>,
    /// Leftovers from a compaction that crashed before its atomic rename.
    pub(crate) stale_tmp: Vec<PathBuf>,
}

pub(crate) fn list(dir: &Path) -> io::Result<Listing> {
    let mut listing = Listing::default();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.ends_with(TMP_SUFFIX) {
            listing.stale_tmp.push(entry.path());
        } else if let Some(id) = name.strip_suffix(SEGMENT_SUFFIX).and_then(|s| s.parse().ok()) {
            listing.segments.push(id);
        }
    }
    listing.segments.sort_unstable();
    Ok(listing)
}

/// Creates a brand-new segment, returning `(append handle, read handle)`.
pub(crate) fn create(dir: &Path, id: SegmentId) -> io::Result<(File, File)> {
    let path = path(dir, id);
    let writer = OpenOptions::new().create_new(true).append(true).open(&path)?;
    let reader = File::open(&path)?;
    sync_dir(dir)?;
    Ok((writer, reader))
}

/// Reads exactly `buf.len()` bytes at `offset` without moving a shared cursor,
/// so many threads can read the same file concurrently without locking.
pub(crate) fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let (mut buf, mut offset) = (buf, offset);
        while !buf.is_empty() {
            match file.seek_read(buf, offset) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(n) => {
                    buf = &mut buf[n..];
                    offset += n as u64;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

/// Makes directory entry changes (create / rename / unlink) durable.
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct ScannedRecord<'a> {
    pub(crate) offset: u64,
    pub(crate) len: u32,
    pub(crate) op: Op,
    pub(crate) batch: bool,
    pub(crate) expires_at: Option<u64>,
    pub(crate) key: &'a [u8],
    pub(crate) value: &'a [u8],
}

#[derive(Debug)]
pub(crate) struct Damage {
    pub(crate) offset: u64,
    pub(crate) reason: String,
}

#[derive(Debug)]
pub(crate) struct ScanOutcome {
    pub(crate) file_len: u64,
    /// Set if the scan stopped before the end of the file.
    pub(crate) damage: Option<Damage>,
}

/// Sequentially validates every record in a segment, calling `visit` for each
/// valid one. Stops at the first invalid record, or the first record `visit`
/// rejects, and reports where.
pub(crate) fn scan(
    path: &Path,
    mut visit: impl FnMut(ScannedRecord<'_>) -> Result<(), String>,
) -> io::Result<ScanOutcome> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut header = [0u8; HEADER_LEN];
    let mut payload = Vec::new();
    let mut offset = 0u64;

    let damaged =
        |offset: u64, reason: String| ScanOutcome { file_len, damage: Some(Damage { offset, reason }) };

    while offset < file_len {
        if offset + HEADER_LEN as u64 > file_len {
            return Ok(damaged(offset, "truncated header".into()));
        }
        reader.read_exact(&mut header)?;
        let parsed = match Header::parse(&header) {
            Ok(parsed) => parsed,
            Err(err) => return Ok(damaged(offset, err.to_string())),
        };
        let record_len = parsed.record_len();
        if offset + record_len > file_len {
            return Ok(damaged(offset, "record extends past end of file".into()));
        }

        payload.resize(record_len as usize - HEADER_LEN, 0);
        reader.read_exact(&mut payload)?;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header[4..]);
        hasher.update(&payload);
        if hasher.finalize() != parsed.crc {
            return Ok(damaged(offset, "checksum mismatch".into()));
        }

        let (expires_at, key, value) = record::split_payload(&parsed, &payload);
        let scanned = ScannedRecord {
            offset,
            len: record_len as u32,
            op: parsed.op,
            batch: parsed.batch,
            expires_at,
            key,
            value,
        };
        if let Err(reason) = visit(scanned) {
            return Ok(damaged(offset, reason));
        }
        offset += record_len;
    }

    Ok(ScanOutcome { file_len, damage: None })
}
