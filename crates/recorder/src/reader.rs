use crate::{Kind, MAGIC, RECORD_HEADER_LEN, SegmentHeader, crc};
use anyhow::{Context, bail};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, ErrorKind, Read};
use std::path::{Path, PathBuf};

/// One record. `payload` borrows the reader's buffer until the next call.
#[derive(Debug)]
pub struct Record<'a> {
    pub kind: Kind,
    pub recv_ns: i64,
    pub mono_ns: i64,
    pub payload: &'a [u8],
    /// Index of the segment this record came from (0-based within the reader).
    pub segment: usize,
}

/// How a segment ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SegmentEnd {
    Clean,
    /// Torn record at `offset` (short read or CRC mismatch).
    Torn {
        offset: u64,
        why: &'static str,
    },
}

pub fn list_segments(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("read {}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "seg"))
        .collect();
    v.sort();
    Ok(v)
}

fn read_header(r: &mut impl Read) -> anyhow::Result<(SegmentHeader, u64)> {
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        bail!("bad segment magic");
    }
    let mut l = [0u8; 4];
    r.read_exact(&mut l)?;
    let n = u32::from_le_bytes(l) as usize;
    let mut h = vec![0u8; n];
    r.read_exact(&mut h)?;
    Ok((serde_json::from_slice(&h)?, (12 + n) as u64))
}

enum Step {
    Rec { kind: Kind, recv: i64, mono: i64, len: usize },
    Eof,
    Torn(&'static str),
}

/// Read one record into `buf`. Distinguishes clean EOF from a torn tail.
fn read_record(r: &mut impl Read, buf: &mut Vec<u8>) -> std::io::Result<Step> {
    let mut hdr = [0u8; RECORD_HEADER_LEN];
    let mut got = 0;
    while got < hdr.len() {
        match r.read(&mut hdr[got..]) {
            Ok(0) => {
                return Ok(if got == 0 { Step::Eof } else { Step::Torn("short header") });
            }
            Ok(n) => got += n,
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    let len = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
    let k = hdr[4];
    let c = u32::from_le_bytes(hdr[5..9].try_into().unwrap());
    let recv = i64::from_le_bytes(hdr[9..17].try_into().unwrap());
    let mono = i64::from_le_bytes(hdr[17..25].try_into().unwrap());
    if len > (1 << 30) {
        return Ok(Step::Torn("implausible length"));
    }
    buf.resize(len, 0);
    if let Err(e) = r.read_exact(buf) {
        return if e.kind() == ErrorKind::UnexpectedEof { Ok(Step::Torn("short payload")) } else { Err(e) };
    }
    if crc(k, recv, mono, buf) != c {
        return Ok(Step::Torn("crc mismatch"));
    }
    let Some(kind) = Kind::from_u8(k) else {
        return Ok(Step::Torn("unknown kind"));
    };
    Ok(Step::Rec { kind, recv, mono, len })
}

/// Scan a segment and truncate it at the first torn record. Returns how it ended.
pub fn recover_segment(path: &Path) -> anyhow::Result<SegmentEnd> {
    let f = File::open(path)?;
    let mut r = BufReader::new(f);
    let (_, mut offset) = match read_header(&mut r) {
        Ok(h) => h,
        Err(_) => {
            // Header itself torn: the segment holds nothing usable.
            OpenOptions::new().write(true).open(path)?.set_len(0)?;
            return Ok(SegmentEnd::Torn { offset: 0, why: "torn header" });
        }
    };
    let mut buf = Vec::new();
    loop {
        match read_record(&mut r, &mut buf)? {
            Step::Rec { len, .. } => offset += (RECORD_HEADER_LEN + len) as u64,
            Step::Eof => return Ok(SegmentEnd::Clean),
            Step::Torn(why) => {
                let f = OpenOptions::new().write(true).open(path)?;
                f.set_len(offset)?;
                f.sync_all()?;
                tracing::warn!(path = %path.display(), offset, why, "truncated torn segment tail");
                return Ok(SegmentEnd::Torn { offset, why });
            }
        }
    }
}

/// Sequential reader over all segments of a recording.
pub struct Reader {
    segments: Vec<PathBuf>,
    idx: usize,
    cur: Option<BufReader<File>>,
    buf: Vec<u8>,
    pub headers: Vec<SegmentHeader>,
    /// Segments that ended torn (reader stops that segment there and continues).
    pub torn: Vec<(PathBuf, &'static str)>,
}

impl Reader {
    pub fn open(dir: impl AsRef<Path>) -> anyhow::Result<Reader> {
        let segments = list_segments(dir.as_ref())?;
        if segments.is_empty() {
            bail!("no segments in {}", dir.as_ref().display());
        }
        Ok(Reader { segments, idx: 0, cur: None, buf: Vec::with_capacity(1 << 16), headers: vec![], torn: vec![] })
    }

    /// Next record, or `None` at the end of the recording.
    pub fn next_record(&mut self) -> anyhow::Result<Option<Record<'_>>> {
        loop {
            if self.cur.is_none() {
                let Some(p) = self.segments.get(self.idx) else {
                    return Ok(None);
                };
                let mut r = BufReader::with_capacity(1 << 20, File::open(p)?);
                match read_header(&mut r) {
                    Ok((h, _)) => {
                        self.headers.push(h);
                        self.cur = Some(r);
                    }
                    Err(_) => {
                        self.torn.push((p.clone(), "torn header"));
                        self.idx += 1;
                        continue;
                    }
                }
            }
            let r = self.cur.as_mut().unwrap();
            match read_record(r, &mut self.buf)? {
                Step::Rec { kind, recv, mono, .. } => {
                    return Ok(Some(Record {
                        kind,
                        recv_ns: recv,
                        mono_ns: mono,
                        payload: &self.buf,
                        segment: self.idx,
                    }));
                }
                Step::Eof => {}
                Step::Torn(why) => self.torn.push((self.segments[self.idx].clone(), why)),
            }
            self.cur = None;
            self.idx += 1;
        }
    }
}
