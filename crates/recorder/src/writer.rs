use crate::{Kind, MAGIC, SCHEMA_VERSION, SegmentHeader, crc, list_segments, recover_segment};
use anyhow::Context;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct RecorderConfig {
    pub venue: String,
    pub product: String,
    /// Rotate after this many bytes.
    pub max_segment_bytes: u64,
    /// Rotate after this much wall time.
    pub max_segment_age: Duration,
    /// Flush the userspace buffer at least this often.
    pub flush_interval: Duration,
    /// fsync at least this often (and always on rotation). `None` = only on rotation.
    pub fsync_interval: Option<Duration>,
    pub git_rev: String,
}

impl RecorderConfig {
    pub fn new(venue: &str, product: &str) -> RecorderConfig {
        RecorderConfig {
            venue: venue.into(),
            product: product.into(),
            max_segment_bytes: 256 << 20,
            max_segment_age: Duration::from_secs(3600),
            flush_interval: Duration::from_millis(200),
            fsync_interval: Some(Duration::from_secs(5)),
            git_rev: option_env!("HFT_GIT_REV").unwrap_or("unknown").into(),
        }
    }
}

pub struct Recorder {
    dir: PathBuf,
    cfg: RecorderConfig,
    out: BufWriter<File>,
    path: PathBuf,
    seg_index: u64,
    seg_bytes: u64,
    seg_opened: Instant,
    last_flush: Instant,
    last_fsync: Instant,
    mono_origin: Instant,
    pub records_written: u64,
}

impl Recorder {
    /// Open a recording directory. If previous segments exist, the last one is
    /// recovered (torn tail truncated) and the new session begins with a `Gap`.
    pub fn open(dir: impl AsRef<Path>, cfg: RecorderConfig) -> anyhow::Result<Recorder> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        let existing = list_segments(&dir)?;
        let mut next_index = 0;
        let mut resumed = None;
        if let Some(last) = existing.last() {
            let end = recover_segment(last)?;
            next_index = existing.len() as u64;
            resumed = Some(format!("recorder restarted; previous segment end: {end:?}"));
        }
        let (out, path) = new_segment(&dir, &cfg, next_index)?;
        let now = Instant::now();
        let mut r = Recorder {
            dir,
            cfg,
            out,
            path,
            seg_index: next_index,
            seg_bytes: 0,
            seg_opened: now,
            last_flush: now,
            last_fsync: now,
            mono_origin: now,
            records_written: 0,
        };
        if let Some(reason) = resumed {
            r.write(Kind::Gap, hft_core::Ts::wall_now().0, reason.as_bytes())?;
        }
        Ok(r)
    }

    pub fn current_path(&self) -> &Path {
        &self.path
    }

    /// Monotonic nanoseconds since this recorder opened.
    pub fn mono_ns(&self) -> i64 {
        self.mono_origin.elapsed().as_nanos() as i64
    }

    pub fn write(&mut self, kind: Kind, recv_ns: i64, payload: &[u8]) -> anyhow::Result<()> {
        let mono = self.mono_ns();
        self.write_with_mono(kind, recv_ns, mono, payload)
    }

    pub fn write_with_mono(&mut self, kind: Kind, recv_ns: i64, mono_ns: i64, payload: &[u8]) -> anyhow::Result<()> {
        let k = kind as u8;
        let len = u32::try_from(payload.len()).context("payload too large")?;
        let c = crc(k, recv_ns, mono_ns, payload);
        self.out.write_all(&len.to_le_bytes())?;
        self.out.write_all(&[k])?;
        self.out.write_all(&c.to_le_bytes())?;
        self.out.write_all(&recv_ns.to_le_bytes())?;
        self.out.write_all(&mono_ns.to_le_bytes())?;
        self.out.write_all(payload)?;
        self.seg_bytes += (crate::RECORD_HEADER_LEN + payload.len()) as u64;
        self.records_written += 1;
        self.maintain()
    }

    /// Flush/fsync/rotate according to policy. Called after every write; may also
    /// be called on a timer when the feed is quiet.
    pub fn maintain(&mut self) -> anyhow::Result<()> {
        if self.seg_bytes >= self.cfg.max_segment_bytes || self.seg_opened.elapsed() >= self.cfg.max_segment_age {
            return self.rotate();
        }
        if self.last_flush.elapsed() >= self.cfg.flush_interval {
            self.out.flush()?;
            self.last_flush = Instant::now();
        }
        if let Some(iv) = self.cfg.fsync_interval
            && self.last_fsync.elapsed() >= iv
        {
            self.out.flush()?;
            self.out.get_ref().sync_data()?;
            self.last_fsync = Instant::now();
        }
        Ok(())
    }

    pub fn rotate(&mut self) -> anyhow::Result<()> {
        self.sync()?;
        self.seg_index += 1;
        let (out, path) = new_segment(&self.dir, &self.cfg, self.seg_index)?;
        self.out = out;
        self.path = path;
        self.seg_bytes = 0;
        self.seg_opened = Instant::now();
        Ok(())
    }

    pub fn sync(&mut self) -> anyhow::Result<()> {
        self.out.flush()?;
        self.out.get_ref().sync_all()?;
        let now = Instant::now();
        self.last_flush = now;
        self.last_fsync = now;
        Ok(())
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        let _ = self.sync();
    }
}

fn new_segment(dir: &Path, cfg: &RecorderConfig, index: u64) -> anyhow::Result<(BufWriter<File>, PathBuf)> {
    let created_ns = hft_core::Ts::wall_now().0;
    let path = dir.join(format!("{index:06}-{created_ns}.seg"));
    let f = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("create {}", path.display()))?;
    let mut out = BufWriter::with_capacity(1 << 20, f);
    let header = SegmentHeader {
        schema_version: SCHEMA_VERSION,
        venue: cfg.venue.clone(),
        product: cfg.product.clone(),
        created_ns,
        segment_index: index,
        host: hostname(),
        git_rev: cfg.git_rev.clone(),
    };
    let h = serde_json::to_vec(&header)?;
    out.write_all(MAGIC)?;
    out.write_all(&(h.len() as u32).to_le_bytes())?;
    out.write_all(&h)?;
    out.flush()?;
    // Make the new file's directory entry durable.
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    Ok((out, path))
}

/// Recordings may be shared, so the machine's network hostname is never stored.
/// Set `HFT_HOST_LABEL` to describe the capture host (e.g. "old-laptop-wifi").
fn hostname() -> String {
    std::env::var("HFT_HOST_LABEL").unwrap_or_else(|_| "unspecified".into())
}
