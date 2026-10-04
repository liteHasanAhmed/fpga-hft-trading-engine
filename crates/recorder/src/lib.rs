//! Raw market-data recording.
//!
//! We record the exact bytes received (WebSocket frames, REST snapshot bodies)
//! rather than normalized events, so a parser bug can be fixed and the recording
//! re-derived. Normalization happens at replay through the same parser.
//!
//! ## On-disk format (schema version 1)
//!
//! A recording is a directory of segment files `NNNNNN-<unix_ns>.seg`, read in name order.
//!
//! ```text
//! segment  := "HFTSEG01" header_len:u32 header_json record*
//! record   := len:u32 kind:u8 crc:u32 recv_ns:i64 mono_ns:i64 payload[len]
//! crc      := crc32(kind, recv_ns, mono_ns, payload)
//! ```
//! All integers little-endian. `recv_ns` is wall-clock receive time; `mono_ns` is a
//! monotonic clock (only meaningful within one recorder process; used for intervals).
//!
//! ## Crash safety
//! Records are appended through a buffer flushed every `flush_interval` and fsynced
//! on rotation. A crash can leave a torn final record; [`recover_segment`] truncates
//! at the first short or CRC-failing record, and the next session starts with a
//! `Gap` record so the hole is explicit.

mod reader;
mod writer;

pub use reader::{Reader, Record, SegmentEnd, list_segments, recover_segment};
pub use writer::{Recorder, RecorderConfig};

use serde::{Deserialize, Serialize};

pub const MAGIC: &[u8; 8] = b"HFTSEG01";
pub const SCHEMA_VERSION: u32 = 1;
pub const RECORD_HEADER_LEN: usize = 4 + 1 + 4 + 8 + 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Kind {
    /// Raw WebSocket text frame.
    WsFrame = 1,
    /// Raw REST L3 snapshot body used to (re)synchronize.
    Snapshot = 2,
    /// Connection opened. Payload: URL.
    ConnOpen = 3,
    /// Connection closed or lost. Payload: reason.
    ConnClose = 4,
    /// Explicit hole in the recording. Payload: reason.
    Gap = 5,
    /// Free-form JSON note (clock sync status, config).
    Note = 6,
    /// REST snapshot taken for verification only; never applied to the book.
    VerifySnapshot = 7,
    /// Live book checkpoint: JSON `{"seq":u64,"hash":u64}` for replay equality checks.
    Checkpoint = 8,
}

impl Kind {
    pub fn from_u8(v: u8) -> Option<Kind> {
        Some(match v {
            1 => Kind::WsFrame,
            2 => Kind::Snapshot,
            3 => Kind::ConnOpen,
            4 => Kind::ConnClose,
            5 => Kind::Gap,
            6 => Kind::Note,
            7 => Kind::VerifySnapshot,
            8 => Kind::Checkpoint,
            _ => return None,
        })
    }
}

/// Segment header (JSON).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SegmentHeader {
    pub schema_version: u32,
    pub venue: String,
    pub product: String,
    pub created_ns: i64,
    pub segment_index: u64,
    pub host: String,
    pub git_rev: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    pub seq: u64,
    pub hash: u64,
}

pub(crate) fn crc(kind: u8, recv_ns: i64, mono_ns: i64, payload: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(&[kind]);
    h.update(&recv_ns.to_le_bytes());
    h.update(&mono_ns.to_le_bytes());
    h.update(payload);
    h.finalize()
}

#[cfg(test)]
mod tests;
