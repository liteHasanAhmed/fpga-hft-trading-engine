use super::*;
use std::io::Write;

fn cfg() -> RecorderConfig {
    let mut c = RecorderConfig::new("coinbase", "BTC-USD");
    c.git_rev = "test".into();
    c
}

fn collect(dir: &std::path::Path) -> Vec<(Kind, i64, Vec<u8>)> {
    let mut r = Reader::open(dir).unwrap();
    let mut v = vec![];
    while let Some(rec) = r.next_record().unwrap() {
        v.push((rec.kind, rec.recv_ns, rec.payload.to_vec()));
    }
    v
}

#[test]
fn roundtrip_with_rotation() {
    let d = tempfile::tempdir().unwrap();
    let mut c = cfg();
    c.max_segment_bytes = 40; // ~32-byte records: rotate every second record
    {
        let mut w = Recorder::open(d.path(), c).unwrap();
        for i in 0..10 {
            w.write(Kind::WsFrame, i, format!("frame-{i}").as_bytes()).unwrap();
        }
    }
    assert!(list_segments(d.path()).unwrap().len() > 3);
    let recs = collect(d.path());
    assert_eq!(recs.len(), 10);
    for (i, (k, t, p)) in recs.iter().enumerate() {
        assert_eq!(*k, Kind::WsFrame);
        assert_eq!(*t, i as i64);
        assert_eq!(p, format!("frame-{i}").as_bytes());
    }
}

#[test]
fn torn_tail_is_truncated_and_marked() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut w = Recorder::open(d.path(), cfg()).unwrap();
        w.write(Kind::WsFrame, 1, b"good-1").unwrap();
        w.write(Kind::WsFrame, 2, b"good-2").unwrap();
    }
    // Simulate a crash mid-write: append half a record.
    let seg = list_segments(d.path()).unwrap().pop().unwrap();
    let len_before = std::fs::metadata(&seg).unwrap().len();
    {
        let mut f = std::fs::OpenOptions::new().append(true).open(&seg).unwrap();
        f.write_all(&[50, 0, 0, 0, 1, 9, 9]).unwrap();
    }
    // Reader stops at the torn record and reports it.
    let mut r = Reader::open(d.path()).unwrap();
    let mut n = 0;
    while r.next_record().unwrap().is_some() {
        n += 1;
    }
    assert_eq!(n, 2);
    assert_eq!(r.torn.len(), 1);

    // Reopening the recorder truncates and writes an explicit gap.
    {
        let mut w = Recorder::open(d.path(), cfg()).unwrap();
        w.write(Kind::WsFrame, 3, b"good-3").unwrap();
    }
    assert_eq!(std::fs::metadata(&seg).unwrap().len(), len_before);
    let recs = collect(d.path());
    let kinds: Vec<Kind> = recs.iter().map(|r| r.0).collect();
    assert_eq!(kinds, vec![Kind::WsFrame, Kind::WsFrame, Kind::Gap, Kind::WsFrame]);
}

#[test]
fn corrupted_payload_detected_by_crc() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut w = Recorder::open(d.path(), cfg()).unwrap();
        w.write(Kind::WsFrame, 1, b"aaaaaaaa").unwrap();
        w.write(Kind::WsFrame, 2, b"bbbbbbbb").unwrap();
    }
    let seg = list_segments(d.path()).unwrap().pop().unwrap();
    let mut bytes = std::fs::read(&seg).unwrap();
    let n = bytes.len();
    bytes[n - 3] ^= 0xff; // flip a byte in the last payload
    std::fs::write(&seg, bytes).unwrap();
    assert_eq!(
        recover_segment(&seg).unwrap(),
        SegmentEnd::Torn { offset: (n - 8 - RECORD_HEADER_LEN) as u64, why: "crc mismatch" }
    );
    assert_eq!(collect(d.path()).len(), 1);
}
