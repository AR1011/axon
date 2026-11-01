use std::collections::BTreeMap;

use parking_lot::Mutex;

use super::*;
use crate::files::MemoryFiles;

/// Polls once: `MemoryFiles`, and so a log on it, is ready at once.
fn now<T>(future: FileFuture<T>) -> io::Result<T> {
    let mut future = future;
    match future.as_mut().poll(&mut std::task::Context::from_waker(std::task::Waker::noop())) {
        std::task::Poll::Ready(out) => out,
        std::task::Poll::Pending => panic!("a log on memory files waited"),
    }
}

fn hex(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex")).collect()
}

fn at(epoch: u64, seq: u64) -> Address {
    Address { epoch, seq }
}

/// The smallest segments a config allows, for a largest record of 1 KiB, zero-filled 1 KiB ahead of their frames.
fn small(preallocate: bool) -> LogConfig {
    let largest = FRAME + MAX_KEY + 1024;
    LogConfig {
        name: "log".into(),
        segment_bytes: HEADER + largest as u64,
        preallocate,
        fill_ahead: 1024,
        max_record: 1024,
        max_unwritten_bytes: 4 * largest,
        max_write_bytes: largest,
        max_bytes: 1 << 30,
        max_keys: 1 << 10,
    }
}

/// Memory files that fail the next writes, or land only part of the next and fail it, as a crash would leave it.
#[derive(Clone, Default)]
struct TestFiles {
    memory: MemoryFiles,
    state: Arc<Mutex<TestState>>,
}

#[derive(Default)]
struct TestState {
    fail: usize,
    /// Of the next write, the units that land (a byte of an append or write, a whole truncate or remove), and whether
    /// those land from the end of its last op, its start lost.
    crash: Option<(usize, bool)>,
    writes: Vec<Vec<FileOp>>,
}

impl Files for TestFiles {
    fn list(&self) -> FileFuture<Vec<(String, u64)>> {
        self.memory.list()
    }

    fn read(&self, name: &str) -> FileFuture<Vec<u8>> {
        self.memory.read(name)
    }

    fn read_range(&self, name: &str, offset: u64, len: usize) -> FileFuture<Vec<u8>> {
        self.memory.read_range(name, offset, len)
    }

    fn apply(&self, ops: Vec<FileOp>) -> FileFuture<()> {
        let mut state = self.state.lock();
        if std::env::var("SIM_DEBUG").is_ok() {
            let shown: Vec<String> = ops
                .iter()
                .map(|op| match op {
                    FileOp::Append(n, b) => format!("append {n} {}", b.len()),
                    FileOp::Write(n, o, b) => format!("write {n} @{o} {}", b.len()),
                    FileOp::Truncate(n, l) => format!("truncate {n} {l}"),
                    FileOp::Remove(n) => format!("remove {n}"),
                    FileOp::Replace(n, b) => format!("replace {n} {}", b.len()),
                })
                .collect();
            eprintln!("apply fail={} crash={:?}: {shown:?}", state.fail, state.crash);
        }
        state.writes.push(ops.clone());
        if let Some((units, tail_first)) = state.crash.take() {
            land(&self.memory, &ops, units, tail_first);
            return Box::pin(std::future::ready(Err(io::Error::other("crashed"))));
        }
        if state.fail > 0 {
            state.fail -= 1;
            return Box::pin(std::future::ready(Err(io::Error::other("failed"))));
        }
        self.memory.apply(ops)
    }
}

fn units(ops: &[FileOp]) -> usize {
    ops.iter()
        .map(|op| match op {
            FileOp::Append(_, b) | FileOp::Write(_, _, b) => b.len(),
            _ => 1,
        })
        .sum()
}

/// Lands the first `units` of `ops`; with `tail_first`, of the op they end in, its end lands and its start does not.
fn land(memory: &MemoryFiles, ops: &[FileOp], mut units: usize, tail_first: bool) {
    for op in ops {
        let len = units_of(op);
        if units >= len {
            now(memory.apply(vec![op.clone()])).expect("landed");
            units -= len;
            continue;
        }
        let part = match op {
            FileOp::Append(name, b) if tail_first => {
                let mut data = vec![0u8; units];
                data.extend_from_slice(&b[units..]);
                Some(FileOp::Append(name.clone(), data.into()))
            }
            FileOp::Write(name, offset, b) if tail_first => {
                Some(FileOp::Write(name.clone(), offset + units as u64, b.slice(units..)))
            }
            FileOp::Append(name, b) => Some(FileOp::Append(name.clone(), b.slice(..units))),
            FileOp::Write(name, offset, b) => Some(FileOp::Write(name.clone(), *offset, b.slice(..units))),
            _ => None,
        };
        if let Some(part) = part {
            now(memory.apply(vec![part])).expect("landed in part");
        }
        return;
    }
}

fn units_of(op: &FileOp) -> usize {
    units(std::slice::from_ref(op))
}

fn copy(memory: &MemoryFiles) -> MemoryFiles {
    let to = MemoryFiles::new();
    for (name, _) in now(memory.list()).expect("listed") {
        let data = now(memory.read(&name)).expect("read");
        now(to.apply(vec![FileOp::Replace(name, data.into())])).expect("copied");
    }
    to
}

fn open<F: Files + Clone + 'static>(files: &F, cfg: &LogConfig) -> io::Result<Log> {
    now(Log::open(Arc::new(files.clone()), cfg.clone()))
}

/// Writes until nothing is left to write, as an owner does at each turn's end, and returns how many writes it took.
fn flush(log: &mut Log) -> usize {
    let mut writes = 0;
    while let Some(write) = log.take_write(1_000) {
        let result = now(write);
        assert!(result.is_ok(), "{result:?}");
        log.written(result);
        writes += 1;
    }
    assert_eq!((log.stats().unwritten_bytes, log.durable()), (0, log.newest()), "all written");
    writes
}

fn entries(log: &Log, key: &[u8]) -> Vec<Entry> {
    now(log.read(key, Address::default(), usize::MAX)).expect("read")
}

fn record(epoch: u64, seq: u64, body: &str) -> Entry {
    Entry::Record { at: at(epoch, seq), body: Bytes::copy_from_slice(body.as_bytes()) }
}

/// The index, without what a reopen need not give back: a segment's written, cut, removal and creation marks.
fn snapshot(log: &Log) -> String {
    let segments: Vec<_> =
        log.segments.values().map(|s| (s.first, s.size, s.end, s.last, s.live, s.keys.clone())).collect();
    format!("{:?}\n{segments:?}\n{} {} {}", log.keys, log.live, log.next, log.durable)
}

/// Reopens the files and checks the index is the one the log built as it went.
fn reopen<F: Files + Clone + 'static>(files: &F, log: &Log) -> Log {
    let again = open(files, &log.cfg).expect("reopened");
    assert_eq!(snapshot(&again), snapshot(log), "the index after open");
    assert_eq!(again.stats().live_bytes, log.stats().live_bytes);
    again
}

#[test]
fn frames_and_headers_keep_their_bytes() {
    let frame = encode(1, RECORD, b"k", at(2, 1), b"hi");
    assert_eq!(frame.len(), FRAME + 3);
    assert_eq!(&frame[..], &hex("1e00000040ff64dc0100000000000000010100020000000000000001000000000000006b6869")[..]);
    let header = Header { id: 0x0102_0304_0506_0708, first: 1, created_ms: 1000, size: 4096 }.bytes();
    let pinned = "41584c470100010008070605040302010100000000000000e8030000000000000010000000000000f6818bd4";
    assert_eq!(&header[..44], &hex(pinned)[..]);
    assert_eq!(header[44..], [0; 20]);
    let parsed = Parsed::read(&frame).expect("reads back");
    assert_eq!(
        (parsed.len, parsed.lsn, parsed.kind, parsed.key, parsed.at, parsed.body),
        (38, 1, RECORD, &b"k"[..], at(2, 1), &b"hi"[..])
    );
    assert!(Header::parse(&header).is_some());
    for i in 0..frame.len() {
        let mut bad = frame.to_vec();
        bad[i] ^= 1;
        assert!(Parsed::read(&bad).is_none(), "a flipped bit at {i} read back");
    }
    assert_eq!(segment_name("log", 1), "log.00000000000000000001");
    assert_eq!(segment_first("log", "log.00000000000000000001"), Some(1));
    assert_eq!(segment_first("log", "logs.00000000000000000001"), None);
    assert_eq!(segment_first("log", "log.1"), None);
}

#[test]
fn records_of_many_keys_go_in_one_write_and_read_back_after_open() {
    for preallocate in [false, true] {
        let files = TestFiles::default();
        let mut log = now(Log::open(Arc::new(files.clone()), small(preallocate))).expect("opened");
        assert!(log.take_write(1).is_none(), "nothing to write");
        let a = log.append(b"a", 1, Bytes::from_static(b"a1")).expect("appended");
        let b = log.append(b"b", 1, Bytes::from_static(b"b1")).expect("appended");
        let a2 = log.append(b"a", 1, Bytes::from_static(b"a2")).expect("appended");
        assert_eq!((a.at, b.at, a2.at), (at(1, 1), at(1, 1), at(1, 2)));
        assert_eq!((a.lsn, b.lsn, a2.lsn), (Lsn(1), Lsn(2), Lsn(3)));
        assert_eq!(log.durable(), Lsn(0));
        assert!(entries(&log, b"a").is_empty(), "nothing durable to read");
        let write = log.take_write(7).expect("a write");
        assert!(log.take_write(8).is_none(), "one write at a time");
        assert_eq!(log.writing_since(), Some(7));
        let c = log.append(b"c", 4, Bytes::from_static(b"c1")).expect("appended while writing");
        let result = now(write);
        assert_eq!(log.written(result), Lsn(3));
        assert_eq!((log.writing_since(), log.failing()), (None, None));
        assert_eq!(files.state.lock().writes.len(), 1, "one write for both keys");
        assert_eq!(entries(&log, b"a"), [record(1, 1, "a1"), record(1, 2, "a2")]);
        assert!(entries(&log, b"c").is_empty());
        assert_eq!(flush(&mut log), 1);
        assert_eq!(log.durable(), c.lsn);
        let ops = files.state.lock().writes.clone();
        let file = segment_name("log", 1);
        if preallocate {
            assert!(
                matches!(&ops[0][..], [FileOp::Append(f, data)] if *f == file && data.len() as u64 == HEADER + 3 * (FRAME as u64 + 3) + 1024)
            );
            assert!(
                matches!(&ops[1][..], [FileOp::Write(f, offset, _)] if *f == file && *offset == HEADER + 3 * (FRAME as u64 + 3))
            );
        } else {
            assert!(matches!(&ops[1][..], [FileOp::Append(f, data)] if *f == file && data.len() == FRAME + 3));
        }
        let again = reopen(&files.memory, &log);
        assert_eq!(entries(&again, b"a"), [record(1, 1, "a1"), record(1, 2, "a2")]);
        assert_eq!(entries(&again, b"c"), [record(4, 1, "c1")]);
        assert_eq!(now(again.read(b"a", at(1, 2), 0)).expect("read"), [record(1, 2, "a2")], "one entry at least");
        let keys: Vec<Bytes> = again.keys(None, 10).into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, ["a", "b", "c"]);
        assert_eq!(again.keys(Some(b"b"), 1)[0].0, "b");
        assert_eq!(
            again.key(b"a"),
            Some(KeyInfo {
                fence: 1,
                epochs: vec![(1, 2)],
                checkpoint: None,
                trimmed: None,
                live_bytes: 2 * (FRAME as u64 + 3)
            })
        );
        assert_eq!(again.stats().durable, Lsn(4));
    }
}

/// A zero-filled segment is filled ahead of its frames a part at a time, so no write, the one that starts a segment
/// or the first after an open included, takes much more than its frames: a write that zero-filled a whole segment of
/// 64 MiB held every key's writes behind it for 107 to 194 ms in the kv example.
#[test]
fn no_write_zero_fills_much_more_than_its_frames() {
    let files = TestFiles::default();
    let cfg = LogConfig {
        segment_bytes: 4 << 20,
        max_record: 1024,
        max_unwritten_bytes: 1 << 20,
        max_write_bytes: 1 << 20,
        fill_ahead: 1 << 20,
        ..LogConfig::default()
    };
    let mut log = open(&files, &cfg).expect("opened");
    let body = Bytes::from(vec![3u8; 1000]);
    let per = 64u64;
    let frames = per * (FRAME as u64 + 4 + 1000);
    let mut reopened = false;
    while log.stats().segments < 4 {
        for i in 0..per {
            log.append(format!("k{i:03}").as_bytes(), 1, body.clone()).expect("appended");
        }
        flush(&mut log);
        // A segment found part filled on open is filled further as it goes, not at once.
        if log.stats().segments == 2 && !reopened {
            (log, reopened) = (reopen(&files, &log), true);
        }
    }
    let writes = files.state.lock().writes.clone();
    let most = writes.iter().map(|ops| units(ops) as u64).max().unwrap_or(0);
    assert!(most <= HEADER + frames + cfg.fill_ahead, "a write of {most} bytes for {frames} of frames");
    // Most writes go over zeros, and so need not sync a new length.
    let growing = writes.iter().filter(|ops| ops.iter().any(|op| matches!(op, FileOp::Append(..)))).count();
    assert!(growing * 8 < writes.len(), "{growing} of {} writes grew a file", writes.len());
    reopen(&files.memory, &log);
}

#[test]
fn a_fence_refuses_older_epochs_and_a_second_fence_at_its_epoch() {
    let files = MemoryFiles::new();
    let mut log = open(&files, &small(true)).expect("opened");
    log.append(b"k", 1, Bytes::from_static(b"x")).expect("appended");
    log.append(b"k", 1, Bytes::from_static(b"y")).expect("appended");
    let (lsn, epochs) = log.fence(b"k", 2).expect("fenced");
    assert_eq!((lsn, epochs), (Lsn(3), vec![(1, 2)]));
    assert_eq!(log.fence(b"k", 2), Err(LogError::Fenced { fence: 2 }), "a second owner at the same epoch");
    assert_eq!(log.fence(b"k", 1), Err(LogError::Fenced { fence: 2 }));
    assert_eq!(log.append(b"k", 1, Bytes::new()), Err(LogError::Fenced { fence: 2 }));
    assert_eq!(log.append_at(b"k", at(1, 3), Bytes::new()), Err(LogError::Fenced { fence: 2 }));
    assert_eq!(log.append(b"k", 2, Bytes::from_static(b"z")).expect("appended").at, at(2, 1));
    // A write from a newer epoch raises the fence too.
    assert_eq!(log.append(b"k", 5, Bytes::from_static(b"w")).expect("appended").at, at(5, 1));
    assert_eq!(log.fence(b"k", 5), Err(LogError::Fenced { fence: 5 }));
    flush(&mut log);
    let again = reopen(&files, &log);
    assert_eq!(again.key(b"k").map(|k| (k.fence, k.epochs)), Some((5, vec![(1, 2), (2, 1), (5, 1)])));
    let mut again = again;
    assert_eq!(again.fence(b"k", 5), Err(LogError::Fenced { fence: 5 }), "the fence is kept across an open");
    assert_eq!(again.fence(b"new", 0), Err(LogError::Fenced { fence: 0 }));
}

#[test]
fn append_at_refuses_a_gap_and_numbers_go_on_after_a_checkpoint_and_a_trim() {
    let files = MemoryFiles::new();
    let mut log = open(&files, &small(false)).expect("opened");
    assert_eq!(log.append_at(b"k", at(3, 2), Bytes::new()), Err(LogError::Gap { have: 0 }));
    for seq in 1..=4 {
        log.append_at(b"k", at(3, seq), Bytes::from(format!("{seq}"))).expect("appended");
    }
    assert_eq!(log.append_at(b"k", at(3, 4), Bytes::new()), Err(LogError::Gap { have: 4 }));
    assert_eq!(log.checkpoint(b"k", at(3, 5), Bytes::new()), Err(LogError::Gap { have: 4 }));
    assert_eq!(log.checkpoint(b"none", at(0, 0), Bytes::new()), Err(LogError::Gap { have: 0 }));
    assert_eq!(log.trim(b"k", at(4, 1)), Err(LogError::Gap { have: 0 }));
    log.checkpoint(b"k", at(3, 2), Bytes::from_static(b"s2")).expect("checkpointed");
    let newest = log.newest();
    assert_eq!(log.checkpoint(b"k", at(3, 1), Bytes::new()), Ok(newest), "below it, nothing");
    log.trim(b"k", at(3, 3)).expect("trimmed");
    assert_eq!(log.trim(b"k", at(3, 3)), Ok(log.newest()), "at it, nothing");
    flush(&mut log);
    let expect = [Entry::Checkpoint { through: at(3, 2), state: Bytes::from_static(b"s2") }, record(3, 4, "4")];
    assert_eq!(entries(&log, b"k"), expect);
    assert_eq!(now(log.read(b"k", at(3, 3), 1 << 20)).expect("read"), [record(3, 4, "4")]);
    let mut again = reopen(&files, &log);
    assert_eq!(entries(&again, b"k"), expect);
    // All its records trimmed, the key goes on from the trim's number.
    again.trim(b"k", at(3, 4)).expect("trimmed");
    assert_eq!(again.append(b"k", 3, Bytes::new()).expect("appended").at, at(3, 5));
    again.checkpoint(b"k", at(3, 5), Bytes::from_static(b"s5")).expect("checkpointed");
    let next = Lsn(again.newest().0 + 1);
    assert_eq!(again.append_at(b"k", at(3, 6), Bytes::new()), Ok(next));
    flush(&mut again);
    let info = reopen(&files, &again).key(b"k").expect("held");
    assert_eq!((info.checkpoint, info.trimmed, info.epochs), (Some(at(3, 5)), Some(at(3, 5)), vec![(3, 6)]));
}

/// Checkpoints, trims and removes let every segment but the last go, and the counters return to zero.
#[test]
fn checkpoint_trim_and_remove_let_segments_go() {
    for preallocate in [false, true] {
        let files = MemoryFiles::new();
        let mut log = open(&files, &small(preallocate)).expect("opened");
        let body = Bytes::from(vec![7u8; 1000]);
        for i in 0..300u64 {
            log.append(format!("k{}", i % 3).as_bytes(), 1, body.clone()).expect("appended");
            if i % 50 == 0 {
                flush(&mut log);
            }
        }
        flush(&mut log);
        let segments = log.stats().segments;
        assert!(segments >= 4, "{segments} segments");
        let pinning = log.pinning(2);
        assert_eq!(pinning.len(), 2);
        assert!(pinning.iter().all(|(_, last, _)| *last == at(1, 100)));
        log.checkpoint(b"k0", at(1, 100), Bytes::from_static(b"state")).expect("checkpointed");
        log.trim(b"k1", at(1, 100)).expect("trimmed");
        log.remove(b"k2").expect("removed");
        assert_eq!(log.remove(b"k2"), Ok(log.newest()), "removed already");
        assert_eq!(log.key(b"k2"), None);
        flush(&mut log);
        assert_eq!(log.stats().segments, 1, "{:?}", log.stats());
        assert_eq!(log.pinning(10), []);
        let again = reopen(&files, &log);
        assert_eq!(now(files.list()).expect("listed").len(), 1);
        assert_eq!(
            entries(&again, b"k0"),
            [Entry::Checkpoint { through: at(1, 100), state: Bytes::from_static(b"state") }]
        );
        assert_eq!(entries(&again, b"k1"), []);
        assert_eq!(again.key(b"k1").map(|k| k.epochs), Some(vec![(1, 100)]));
        let mut log = again;
        log.remove(b"k0").expect("removed");
        log.remove(b"k1").expect("removed");
        flush(&mut log);
        let stats = log.stats();
        assert_eq!((stats.live_bytes, stats.unwritten_bytes, log.keys(None, 10).len()), (0, 0, 0));
        let again = reopen(&files, &log);
        assert_eq!(again.stats().live_bytes, 0);
    }
}

/// A removed key keeps its fence and numbers while the segments holding its frames are there, and a key written again
/// after a remove goes on from them.
#[test]
fn a_key_written_again_after_a_remove_goes_on_from_its_numbers() {
    let files = MemoryFiles::new();
    let mut log = open(&files, &small(true)).expect("opened");
    log.append(b"k", 4, Bytes::from_static(b"a")).expect("appended");
    log.remove(b"k").expect("removed");
    assert_eq!(log.key(b"k"), None);
    assert_eq!(log.append(b"k", 3, Bytes::new()), Err(LogError::Fenced { fence: 4 }));
    assert_eq!(log.append(b"k", 4, Bytes::from_static(b"b")).expect("appended").at, at(4, 2));
    flush(&mut log);
    let again = reopen(&files, &log);
    assert_eq!(entries(&again, b"k"), [record(4, 2, "b")]);
}

/// A remove frame stays while an older segment holds frames of its key, though the key was written again and trimmed
/// since: its checkpoint and fence there do not come back on open.
#[test]
fn a_remove_keeps_older_frames_of_its_key_dead_after_the_key_is_trimmed_again() {
    let files = MemoryFiles::new();
    let mut log = open(&files, &small(false)).expect("opened");
    let body = Bytes::from(vec![0u8; 1000]);
    log.append(b"k", 1, Bytes::from_static(b"a")).expect("appended");
    log.checkpoint(b"k", at(1, 1), Bytes::from_static(b"old")).expect("checkpointed");
    log.fence(b"k", 2).expect("fenced");
    // `pins` keeps the first segment; the remove goes in the second, between records of `filler` that go.
    log.append(b"pins", 1, Bytes::from_static(b"p")).expect("appended");
    let mut filler = at(1, 0);
    while log.segments.len() < 2 {
        filler = log.append(b"filler", 1, body.clone()).expect("appended").at;
    }
    log.remove(b"k").expect("removed");
    while log.segments.len() < 3 {
        filler = log.append(b"filler", 1, body.clone()).expect("appended").at;
    }
    let again = log.append(b"k", 2, Bytes::from_static(b"b")).expect("appended").at;
    log.trim(b"k", again).expect("trimmed");
    log.trim(b"filler", filler).expect("trimmed");
    flush(&mut log);
    flush(&mut log);
    assert_eq!(log.key(b"k").map(|k| (k.checkpoint, k.trimmed)), Some((None, Some(again))));
    let reopened = reopen(&files, &log);
    assert_eq!(entries(&reopened, b"k"), []);
}

#[test]
fn a_mostly_dead_old_segment_is_copied_forward_and_removed() {
    for preallocate in [false, true] {
        let files = TestFiles::default();
        let mut cfg = small(preallocate);
        cfg.max_bytes = 3 * cfg.segment_bytes;
        let mut log = now(Log::open(Arc::new(files.clone()), cfg.clone())).expect("opened");
        let body = Bytes::from(vec![1u8; 1000]);
        // A key that stays, written first, and one whose records go.
        log.append(b"stays", 1, Bytes::from_static(b"kept")).expect("appended");
        log.fence(b"stays", 2).expect("fenced");
        let mut seq = 0;
        while log.stats().segments < 3 {
            seq = log.append(b"goes", 1, body.clone()).expect("appended").at.seq;
            flush(&mut log);
        }
        log.trim(b"goes", at(1, seq)).expect("trimmed");
        flush(&mut log);
        // The stays record and fence moved to the last segment, and the first went with the others.
        let stats = log.stats();
        assert_eq!((stats.segments, stats.copied_bytes), (1, (FRAME + 5 + 4 + FRAME + 5) as u64), "{stats:?}");
        assert_eq!(stats.live_bytes, (FRAME + 5 + 4 + FRAME + 5 + FRAME + 4) as u64, "stays and the trim");
        assert!(now(files.memory.list()).expect("listed").iter().all(|(name, _)| *name != segment_name("log", 1)));
        assert_eq!(entries(&log, b"stays"), [record(1, 1, "kept")]);
        let again = reopen(&files.memory, &log);
        assert_eq!(entries(&again, b"stays"), [record(1, 1, "kept")]);
        assert_eq!(again.key(b"stays").map(|k| k.fence), Some(2));
    }
}

/// A segment that copies forward still read from stays until they land, though its frames die meanwhile: a write that
/// removed it and then failed leaves the next able to read them again.
#[test]
fn a_segment_copies_read_from_stays_until_they_land() {
    for preallocate in [false, true] {
        let files = TestFiles::default();
        let mut cfg = small(preallocate);
        cfg.max_bytes = 3 * cfg.segment_bytes;
        let mut log = now(Log::open(Arc::new(files.clone()), cfg.clone())).expect("opened");
        log.append(b"stays", 1, Bytes::from_static(b"kept")).expect("appended");
        let mut seq = 0;
        while log.stats().segments < 3 {
            seq = log.append(b"goes", 1, Bytes::from(vec![1u8; 1000])).expect("appended").at.seq;
            flush(&mut log);
        }
        log.trim(b"goes", at(1, seq)).expect("trimmed");
        // The write that copies `stays` forward fails, then `stays` goes, so nothing in the first segment is live.
        files.state.lock().fail = 1;
        let write = log.take_write(1).expect("a write");
        let result = now(write);
        log.written(result);
        assert!(log.copies.len() == 1 && log.failing().is_some(), "{:?}", log.stats());
        log.remove(b"stays").expect("removed");
        // The next lands whole and fails: a removal it held is done.
        let write = log.take_write(2).expect("a write");
        files.state.lock().crash = Some((usize::MAX, false));
        let result = now(write);
        log.written(result);
        assert!(log.failing().is_some());
        flush(&mut log);
        let stats = log.stats();
        assert_eq!((log.failing(), stats.segments, stats.copied_bytes), (None, 1, 0), "nothing moved: {stats:?}");
        assert_eq!(log.key(b"stays"), None);
        reopen(&files.memory, &log);
    }
}

/// A segment whose last live record a queued remove ended stays until a write takes that remove: a crash after the
/// writes before it reopens to the record.
#[test]
fn a_segment_stays_until_the_frame_that_ended_its_records_is_written() {
    for preallocate in [false, true] {
        let files = MemoryFiles::new();
        let mut log = open(&files, &small(preallocate)).expect("opened");
        let body = Bytes::from(vec![2u8; 1000]);
        log.append(b"a", 1, Bytes::from_static(b"r")).expect("appended");
        let (mut first, mut last) = (at(1, 0), at(1, 0));
        while log.segments.len() < 2 {
            (first, last) = (last, log.append(b"filler", 1, body.clone()).expect("appended").at);
        }
        flush(&mut log);
        // The trim ends the first segment's filler; the remove comes after more than a write takes.
        log.trim(b"filler", first).expect("trimmed");
        while log.segments.len() < 4 {
            log.append(b"more", 1, body.clone()).expect("appended");
        }
        let removed = log.remove(b"a").expect("removed");
        let write = log.take_write(1).expect("a write");
        let result = now(write);
        assert!(log.written(result) < removed);
        assert_eq!(entries(&open(&copy(&files), &log.cfg).expect("reopened"), b"a"), [record(1, 1, "r")]);
        flush(&mut log);
        assert!(log.segments.values().all(|s| s.first != 1), "the first segment went");
        let again = reopen(&files, &log);
        assert_eq!(entries(&again, b"a"), []);
    }
}

#[test]
fn a_failed_write_goes_again_byte_for_byte_and_durable_waits() {
    for preallocate in [false, true] {
        let files = TestFiles::default();
        let mut log = now(Log::open(Arc::new(files.clone()), small(preallocate))).expect("opened");
        log.append(b"a", 1, Bytes::from_static(b"1")).expect("appended");
        flush(&mut log);
        let lsn = log.append(b"a", 1, Bytes::from_static(b"2")).expect("appended").lsn;
        files.state.lock().fail = 2;
        for failed in 1..=2 {
            let write = log.take_write(1).expect("a write");
            let result = now(write);
            assert_eq!(log.written(result), Lsn(1));
            assert_eq!((log.failing(), log.stats().failed_writes), (Some(io::ErrorKind::Other), failed));
            assert_eq!(entries(&log, b"a"), [record(1, 1, "1")], "not durable");
        }
        let more = log.append(b"a", 1, Bytes::from_static(b"3")).expect("appended while failing").lsn;
        let write = log.take_write(1).expect("a write");
        let result = now(write);
        assert_eq!(log.written(result), more);
        assert!(log.durable() >= lsn && log.failing().is_none());
        let writes = files.state.lock().writes.clone();
        let data = |ops: &[FileOp]| {
            ops.iter().find_map(|op| match op {
                FileOp::Append(_, data) | FileOp::Write(_, _, data) => Some(data.clone()),
                _ => None,
            })
        };
        assert_eq!(data(&writes[1]), data(&writes[2]), "the same bytes again");
        if preallocate {
            assert!(matches!(&writes[3][..], [FileOp::Write(..)]), "{:?}", writes[3]);
        } else {
            assert!(matches!(&writes[3][..], [FileOp::Truncate(_, _), FileOp::Append(..)]), "{:?}", writes[3]);
        }
        assert_eq!(entries(&log, b"a"), [record(1, 1, "1"), record(1, 2, "2"), record(1, 3, "3")]);
        reopen(&files.memory, &log);
    }
}

#[test]
fn refusals_change_nothing() {
    let files = MemoryFiles::new();
    let mut cfg = small(true);
    cfg.max_keys = 2;
    cfg.max_bytes = 3 * (FRAME as u64 + 1 + 1000) + FRAME as u64 + 1;
    let mut log = open(&files, &cfg).expect("opened");
    let body = Bytes::from(vec![0u8; 1000]);
    assert_eq!(log.append(b"a", 1, Bytes::from(vec![0u8; 1025])), Err(LogError::TooLarge));
    assert_eq!(log.append(&vec![0u8; MAX_KEY + 1], 1, Bytes::new()), Err(LogError::TooLarge));
    assert_eq!(log.checkpoint(b"a", at(0, 0), Bytes::from(vec![0u8; 1025])), Err(LogError::TooLarge));
    for _ in 0..3 {
        log.append(b"a", 1, body.clone()).expect("appended");
    }
    assert_eq!(log.append(b"a", 1, body.clone()), Err(LogError::Full));
    log.append(b"b", 1, Bytes::new()).expect("appended");
    assert_eq!(log.append(b"c", 1, Bytes::new()), Err(LogError::Keys));
    assert_eq!(log.fence(b"c", 1), Err(LogError::Keys));
    // Room for one more trim frame only.
    log.cfg.max_unwritten_bytes = log.stats().unwritten_bytes + FRAME + 1;
    log.trim(b"a", at(1, 1)).expect("a trim fits");
    let before = snapshot(&log);
    assert_eq!(log.trim(b"a", at(1, 2)), Err(LogError::Behind));
    assert_eq!(log.remove(b"b"), Err(LogError::Behind));
    assert_eq!(log.append(b"b", 1, Bytes::new()), Err(LogError::Behind));
    assert_eq!(snapshot(&log), before, "refused, nothing changed");
    log.cfg.max_unwritten_bytes = cfg.max_unwritten_bytes;
    // A record goes in once a trim makes room.
    log.trim(b"a", at(1, 2)).expect("trimmed");
    flush(&mut log);
    log.append(b"a", 1, body.clone()).expect("appended");
    flush(&mut log);
    reopen(&files, &log);
}

#[test]
fn open_refuses_a_bad_config_a_log_of_another_id_and_data_after_a_bad_frame() {
    let files = MemoryFiles::new();
    for bad in [
        LogConfig { name: "a.b".into(), ..small(true) },
        LogConfig { name: String::new(), ..small(true) },
        LogConfig { max_write_bytes: 100, ..small(true) },
        LogConfig { segment_bytes: 100, ..small(true) },
    ] {
        assert_eq!(open(&files, &bad).map(|_| ()).map_err(|e| e.kind()), Err(io::ErrorKind::InvalidInput));
    }
    let cfg = small(false);
    let mut log = open(&files, &cfg).expect("opened");
    while log.segments.len() < 2 {
        log.append(b"a", 1, Bytes::from(vec![1u8; 1000])).expect("appended");
        flush(&mut log);
    }
    let first = segment_name("log", 1);
    let second = segment_name("log", *log.segments.keys().next_back().expect("a segment"));
    let changed = |name: &str, change: &dyn Fn(&mut Vec<u8>)| {
        let files = copy(&files);
        let mut data = now(files.read(name)).expect("read");
        change(&mut data);
        now(files.apply(vec![FileOp::Replace(name.into(), data.into())])).expect("written");
        open(&files, &cfg).map(|_| ())
    };
    // A flipped bit in a frame of a segment that is not the last: not a torn tail.
    let err = changed(&first, &|data| data[HEADER as usize + 20] ^= 1).expect_err("refused");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(err.to_string().starts_with(&format!("{first}: ")) && err.to_string().ends_with("offset 64"), "{err}");
    // A segment of another log.
    let other_id = |data: &mut Vec<u8>| {
        let mut header = Header::parse(data).expect("a header");
        header.id += 1;
        data[..HEADER as usize].copy_from_slice(&header.bytes());
    };
    let err = changed(&second, &other_id).expect_err("refused");
    assert!(err.kind() == io::ErrorKind::InvalidData && err.to_string().contains("another log"), "{err}");
    // A segment whose name is not its header's.
    let err = changed(&second, &|data| {
        let mut header = Header::parse(data).expect("a header");
        header.first += 1;
        data[..HEADER as usize].copy_from_slice(&header.bytes());
    })
    .expect_err("refused");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    // Files of another name are not the log's.
    now(files.apply(vec![FileOp::Append("other.00000000000000000009".into(), Bytes::from_static(b"x"))]))
        .expect("written");
    assert!(open(&files, &cfg).is_ok());
}

/// Bytes after the tail of the last segment are a torn write when within twice `max_write_bytes` of it, and are cut;
/// further, they are corruption.
#[test]
fn a_torn_tail_is_cut_and_bytes_beyond_reach_are_refused() {
    for preallocate in [false, true] {
        let cfg = small(preallocate);
        let files = MemoryFiles::new();
        let mut log = open(&files, &cfg).expect("opened");
        log.append(b"a", 1, Bytes::from_static(b"1")).expect("appended");
        flush(&mut log);
        let name = segment_name("log", 1);
        let len = now(files.read(&name)).expect("read").len() as u64;
        let tail = HEADER + FRAME as u64 + 2;
        let reach = cfg.reach();
        for (offset, ok) in [(tail, true), (tail + reach - 1, true), (tail + reach, false)] {
            let torn = copy(&files);
            if offset + 1 > len {
                let grow = vec![0u8; (offset + 1 - len) as usize];
                now(torn.apply(vec![FileOp::Append(name.clone(), grow.into())])).expect("grown");
            }
            now(torn.apply(vec![FileOp::Write(name.clone(), offset, Bytes::from_static(b"x"))])).expect("written");
            match open(&torn, &cfg) {
                Ok(mut again) => {
                    assert!(ok, "{offset}: opened");
                    assert_eq!(again.stats().truncated_bytes, offset + 1 - tail);
                    again.append(b"a", 1, Bytes::from_static(b"2")).expect("appended");
                    flush(&mut again);
                    let reread = open(&torn, &cfg).expect("reopened after the cut");
                    assert_eq!(entries(&reread, b"a"), [record(1, 1, "1"), record(1, 2, "2")]);
                    assert_eq!(reread.stats().truncated_bytes, 0, "cut");
                }
                Err(e) => assert!(!ok && e.kind() == io::ErrorKind::InvalidData, "{offset}: {e}"),
            }
        }
    }
}

/// Of a write cut short at any byte, with its start or its end lost, open keeps a prefix of its frames, at least
/// what was durable before it, and the log goes on from there: a write after it reopens to the same index.
#[test]
fn a_crash_at_any_byte_of_a_write_reopens_to_a_prefix() {
    for preallocate in [false, true] {
        let cfg = small(preallocate);
        let files = TestFiles::default();
        let mut log = now(Log::open(Arc::new(files.clone()), cfg.clone())).expect("opened");
        let body = Bytes::from(vec![9u8; 700]);
        // The writes: one into the first segment, one that starts a segment, one that writes over zeros and fills
        // further, and one with removals.
        let mut cases: Vec<(MemoryFiles, Vec<FileOp>, Lsn, Lsn)> = Vec::new();
        for round in 0..4 {
            match round {
                0 => {
                    for i in 0..3u64 {
                        log.append(format!("k{i}").as_bytes(), 1, body.clone()).expect("appended");
                    }
                }
                1 => {
                    while log.segments.len() < 2 {
                        log.append(b"k0", 1, body.clone()).expect("appended");
                        if log.segments.len() < 2 {
                            flush(&mut log);
                        }
                    }
                }
                2 => loop {
                    log.append(b"k1", 1, body.clone()).expect("appended");
                    let head = log.segments.values().next_back().expect("a segment");
                    if !preallocate || head.end > head.filled {
                        break;
                    }
                    flush(&mut log);
                },
                _ => {
                    for i in 0..3 {
                        let key = format!("k{i}");
                        let last = log.key(key.as_bytes()).expect("held").epochs[0].1;
                        log.checkpoint(key.as_bytes(), at(1, last), Bytes::from_static(b"s")).expect("checkpointed");
                    }
                }
            }
            let (before, durable) = (copy(&files.memory), log.durable());
            let write = log.take_write(1).expect("a write");
            let result = now(write);
            log.written(result);
            let ops = files.state.lock().writes.last().expect("a write").clone();
            if round == 2 && preallocate {
                assert!(matches!(&ops[..], [FileOp::Write(..), FileOp::Append(..)]), "{ops:?}");
            }
            if round == 3 {
                assert!(ops.iter().any(|op| matches!(op, FileOp::Remove(_))), "{ops:?}");
            }
            flush(&mut log);
            cases.push((before, ops, durable, log.durable()));
        }
        // The files of a write sync in any order: of a Write and an Append, the Append may land and the Write not.
        let cases = cases.into_iter().flat_map(|(before, ops, durable, after)| {
            let lost = matches!(&ops[..], [FileOp::Write(..), FileOp::Append(..)]).then(|| ops[1..].to_vec());
            let lost = lost.map(|ops| (copy(&before), ops, durable, durable));
            [Some((before, ops, durable, after)), lost].into_iter().flatten()
        });
        for (case, (before, ops, durable, after)) in cases.enumerate() {
            let total = units(&ops);
            // Every byte of the header and the frames, then a sample of the zeros after them.
            let points = (0..=total).filter(|n| *n < 2400 || n % 997 == 0 || *n == total);
            for n in points {
                for tail_first in [false, true] {
                    let crashed = copy(&before);
                    land(&crashed, &ops, n, tail_first);
                    let mut log = open(&crashed, &cfg)
                        .unwrap_or_else(|e| panic!("case {case}, {n} of {total}, tail first {tail_first}: {e}"));
                    let at = log.durable();
                    assert!(durable <= at && at <= after, "case {case}, {n}: {durable:?} <= {at:?} <= {after:?}");
                    if n == total {
                        assert_eq!(at, after);
                    }
                    for key in [&b"k0"[..], b"k1", b"k2"] {
                        for entry in entries(&log, key) {
                            assert!(
                                matches!(entry, Entry::Record { ref body, .. } if body.len() == 700)
                                    || matches!(entry, Entry::Checkpoint { .. })
                            );
                        }
                    }
                    log.append(b"k9", 1, Bytes::from_static(b"after")).expect("appended");
                    flush(&mut log);
                    let again = reopen(&crashed, &log);
                    assert_eq!(entries(&again, b"k9"), [record(1, 1, "after")], "case {case}, {n}");
                }
            }
        }
    }
}

#[test]
fn pinning_names_the_keys_holding_the_oldest_segments() {
    let files = MemoryFiles::new();
    let mut log = open(&files, &small(false)).expect("opened");
    let body = Bytes::from(vec![0u8; 1000]);
    log.append(b"small", 1, Bytes::from_static(b"x")).expect("appended");
    log.append(b"big", 1, body.clone()).expect("appended");
    log.append(b"big", 1, body.clone()).expect("appended");
    while log.segments.len() < 3 {
        log.append(b"later", 1, body.clone()).expect("appended");
    }
    flush(&mut log);
    let later = log.key(b"later").map(|k| at(1, k.epochs[0].1)).expect("held");
    let frame = |body: u64| FRAME as u64 + body;
    // In the oldest segment, the most bytes first; a key once.
    assert_eq!(log.pinning(2)[1], (Bytes::from_static(b"big"), at(1, 2), 2 * frame(1003)));
    let pinning: Vec<(Bytes, Address)> = log.pinning(10).into_iter().map(|(k, at, _)| (k, at)).collect();
    assert_eq!(
        pinning,
        [
            (Bytes::from_static(b"later"), later),
            (Bytes::from_static(b"big"), at(1, 2)),
            (Bytes::from_static(b"small"), at(1, 1)),
        ]
    );
}

/// The keys the random changes use; one removed is not used again.
#[derive(Clone, Debug, Default, PartialEq)]
struct ModelKey {
    fence: u64,
    records: BTreeMap<Address, Bytes>,
    checkpoint: Option<(Address, Bytes)>,
    low: Option<Address>,
}

impl ModelKey {
    fn length(&self, epoch: u64) -> u64 {
        let in_records = self.records.keys().filter(|a| a.epoch == epoch).map(|a| a.seq).max();
        in_records.or(self.low.filter(|l| l.epoch == epoch).map(|l| l.seq)).unwrap_or(0)
    }

    fn last(&self) -> Address {
        self.records.keys().next_back().copied().or(self.low).unwrap_or_default()
    }

    fn cut(&mut self, through: Address) {
        self.records.retain(|a, _| *a > through);
        self.low = Some(through);
    }
}

#[derive(Clone, Debug)]
enum Op {
    Append(u64, Bytes),
    Fence(u64),
    Checkpoint(Address, Bytes),
    Trim(Address),
    Remove,
}

/// What the durable changes, in LSN order, leave.
fn model(ops: &[(u64, String, Op)]) -> BTreeMap<String, ModelKey> {
    let mut keys: BTreeMap<String, ModelKey> = BTreeMap::new();
    for (_, key, op) in ops {
        let k = keys.entry(key.clone()).or_default();
        match op {
            Op::Append(epoch, body) => {
                let at = Address { epoch: *epoch, seq: k.length(*epoch) + 1 };
                k.records.insert(at, body.clone());
                k.fence = k.fence.max(*epoch);
            }
            Op::Fence(epoch) => k.fence = *epoch,
            Op::Checkpoint(through, state) => {
                k.cut(*through);
                k.checkpoint = Some((*through, state.clone()));
            }
            Op::Trim(through) => k.cut(*through),
            Op::Remove => drop(keys.remove(key)),
        }
    }
    keys
}

fn check(log: &Log, ops: &[(u64, String, Op)]) {
    let expect = model(ops);
    let listed: Vec<String> =
        log.keys(None, usize::MAX).into_iter().map(|(k, _)| String::from_utf8_lossy(&k).into_owned()).collect();
    assert_eq!(listed, expect.keys().cloned().collect::<Vec<_>>());
    for (key, k) in &expect {
        let info = log.key(key.as_bytes()).expect("held");
        assert_eq!(
            (info.fence, info.checkpoint, info.trimmed),
            (k.fence, k.checkpoint.as_ref().map(|c| c.0), k.low),
            "{key}"
        );
        let mut want: Vec<Entry> = k
            .checkpoint
            .iter()
            .map(|(through, state)| Entry::Checkpoint { through: *through, state: state.clone() })
            .collect();
        want.extend(k.records.iter().map(|(at, body)| Entry::Record { at: *at, body: body.clone() }));
        assert_eq!(entries(log, key.as_bytes()), want, "{key}");
    }
    let live: u64 = log.keys.values().map(|k| k.live_bytes).sum();
    let in_segments: u64 = log.segments.values().map(|s| s.live).sum();
    assert_eq!((live, in_segments), (log.stats().live_bytes, log.stats().live_bytes));
}

/// Random changes to a few keys, written, failed, crashed at a random byte and reopened, against a model of the
/// changes that are durable. Removed keys are not written again: a removed key's fence lasts only as long as its
/// segments, which the model does not follow.
#[test]
fn random_changes_match_a_model_across_failures_crashes_and_opens() {
    // Bytes copied forward, torn bytes cut on open and segments removed, over the seeds: each path was taken.
    let mut seen = [0; 3];
    // SEEDS=n runs n seeds, SIM_SEED=n the one.
    let seeds = std::env::var("SEEDS").ok().and_then(|n| n.parse().ok()).unwrap_or(48u64);
    let seeds = match std::env::var("SIM_SEED").ok().and_then(|n| n.parse().ok()) {
        Some(seed) => seed..seed + 1,
        None => 0..seeds,
    };
    let all = seeds.end - seeds.start > 1;
    for seed in seeds {
        let one = std::panic::catch_unwind(|| random_seed(seed)).unwrap_or_else(|_| panic!("seed {seed} failed"));
        seen.iter_mut().zip(one).for_each(|(seen, one)| *seen += one);
    }
    assert!(!all || seen.iter().all(|n| *n > 0), "copied, cut and removed: {seen:?}");
}

fn random_seed(seed: u64) -> [u64; 3] {
    let (mut copied, mut cut, mut removed) = (0, 0, 0);
    let mut state = seed.wrapping_add(1);
    let mut pick = |n: u64| {
        state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) % n
    };
    let mut cfg = small(seed.is_multiple_of(2));
    cfg.max_bytes = 3 * cfg.segment_bytes;
    let files = TestFiles::default();
    let mut log = now(Log::open(Arc::new(files.clone()), cfg.clone())).expect("opened");
    // Each change with its LSN and key; the durable ones are those at or below the log's durable LSN.
    let mut ops: Vec<(u64, String, Op)> = Vec::new();
    let mut next_key = 0;
    let mut keys: Vec<String> = Vec::new();
    for step in 0..600 {
        if keys.len() < 4 {
            keys.push(format!("k{next_key}"));
            next_key += 1;
        }
        let key = keys[pick(keys.len() as u64) as usize].clone();
        let now_model = model(&ops);
        let k = now_model.get(&key).cloned().unwrap_or_default();
        let lsn_before = log.newest();
        let what = pick(20);
        if std::env::var("SIM_DEBUG").is_ok() {
            let segments: Vec<_> =
                log.segments.values().map(|s| (s.first, s.end, s.written, s.made, s.cut, s.live)).collect();
            eprintln!("step {step} {what} {key}: {segments:?} unwritten {}", log.unwritten.len());
        }
        match what {
            0..=8 => {
                let epoch = (k.fence + pick(3)).saturating_sub(1);
                let body = Bytes::from(vec![step as u8; pick(1024) as usize]);
                match log.append(key.as_bytes(), epoch, body.clone()) {
                    Ok(appended) => {
                        assert_eq!(appended.at, Address { epoch, seq: k.length(epoch) + 1 }, "seed {seed} step {step}");
                        ops.push((appended.lsn.0, key, Op::Append(epoch, body)));
                    }
                    Err(LogError::Fenced { fence }) => assert!(epoch < k.fence && fence == k.fence),
                    Err(LogError::Full) => {
                        assert!(log.stats().live_bytes + (FRAME + key.len() + body.len()) as u64 > cfg.max_bytes)
                    }
                    Err(LogError::Behind) => {}
                    Err(e) => panic!("seed {seed} step {step}: {e}"),
                }
            }
            9 => {
                let epoch = k.fence + pick(2);
                match log.fence(key.as_bytes(), epoch) {
                    Ok((lsn, _)) => ops.push((lsn.0, key, Op::Fence(epoch))),
                    Err(LogError::Fenced { fence }) => assert!(epoch <= k.fence && fence == k.fence, "seed {seed}"),
                    Err(e) => assert_eq!(e, LogError::Behind),
                }
            }
            10..=11 => {
                let last = k.last();
                let through = Address { epoch: last.epoch, seq: last.seq.saturating_sub(pick(3)) };
                let state = Bytes::from(vec![1; pick(64) as usize]);
                match log.checkpoint(key.as_bytes(), through, state.clone()) {
                    Ok(lsn) if lsn > lsn_before => ops.push((lsn.0, key, Op::Checkpoint(through, state))),
                    Ok(_) => assert!(k.low.is_some_and(|low| through < low), "seed {seed} step {step}"),
                    Err(LogError::Gap { .. }) => assert!(!now_model.contains_key(&key), "seed {seed} step {step}"),
                    Err(e) => assert_eq!(e, LogError::Behind),
                }
            }
            12..=13 => {
                let last = k.last();
                let through = Address { epoch: last.epoch, seq: last.seq.saturating_sub(pick(3)) };
                match log.trim(key.as_bytes(), through) {
                    Ok(lsn) if lsn > lsn_before => ops.push((lsn.0, key, Op::Trim(through))),
                    Ok(_) => assert!(
                        !now_model.contains_key(&key)
                            || k.low.is_some_and(|low| through <= low)
                            || through == Address::default(),
                        "seed {seed} step {step}"
                    ),
                    Err(e) => assert_eq!(e, LogError::Behind),
                }
            }
            14 => {
                if now_model.contains_key(&key) {
                    match log.remove(key.as_bytes()) {
                        Ok(lsn) => {
                            ops.push((lsn.0, key.clone(), Op::Remove));
                            keys.retain(|k| *k != key);
                        }
                        Err(e) => assert_eq!(e, LogError::Behind),
                    }
                }
            }
            15..=16 => {
                if let Some(write) = log.take_write(step) {
                    let result = now(write);
                    log.written(result);
                    // The index holds what is not durable too: compared once everything is.
                    if log.stats().unwritten_bytes == 0 {
                        check(&log, &ops);
                    }
                }
            }
            17 => {
                // A write that fails with nothing landed, or landed whole, as when an op after its appends fails.
                if pick(2) == 0 {
                    files.state.lock().crash = Some((usize::MAX, false));
                } else {
                    files.state.lock().fail = 1;
                }
                if let Some(write) = log.take_write(step) {
                    let result = now(write);
                    log.written(result);
                    assert!(log.failing().is_some());
                }
                let mut state = files.state.lock();
                (state.fail, state.crash) = (0, None);
            }
            18 => {
                // A crash: part of the next write lands, and the log is opened again.
                let durable = log.durable();
                if let Some(write) = log.take_write(step) {
                    // Run with nothing landing, then land a part of what it wrote.
                    files.state.lock().crash = Some((0, false));
                    assert!(now(write).is_err());
                    let ops = files.state.lock().writes.last().cloned().expect("the write");
                    let n = pick(units(&ops) as u64 + 1) as usize;
                    land(&files.memory, &ops, n, pick(2) == 0);
                }
                copied += log.stats().copied_bytes;
                log = now(Log::open(Arc::new(files.clone()), cfg.clone())).expect("reopened after a crash");
                cut += log.stats().truncated_bytes;
                assert!(log.durable() >= durable, "seed {seed} step {step}");
                let durable = log.durable().0;
                // A remove lost with the write leaves its key held, and not used again.
                ops.retain(|(l, ..)| *l <= durable);
                check(&log, &ops);
            }
            _ => {
                flush(&mut log);
                check(&log, &ops);
                copied += log.stats().copied_bytes;
                log = reopen(&files, &log);
                check(&log, &ops);
            }
        }
    }
    flush(&mut log);
    check(&log, &ops);
    reopen(&files.memory, &log);
    copied += log.stats().copied_bytes;
    let writes = files.state.lock().writes.clone();
    removed += writes.iter().flatten().filter(|op| matches!(op, FileOp::Remove(_))).count() as u64;
    [copied, cut, removed]
}
