#![no_main]
//! `cargo +nightly fuzz run log_frame`. A segment of a `files::Log` whose bytes after the header are changed by the
//! input: the first two bytes say where. Oracle: open never panics and gives the same result twice; an open log reads
//! every key it lists, and takes a write and opens again to the same keys.
use std::sync::Arc;

use axon::files::{Address, FileFuture, FileOp, Files, KeyInfo, Log, LogConfig, MemoryFiles};
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fn now<T>(mut future: FileFuture<T>) -> std::io::Result<T> {
    match future.as_mut().poll(&mut std::task::Context::from_waker(std::task::Waker::noop())) {
        std::task::Poll::Ready(out) => out,
        std::task::Poll::Pending => panic!("memory files waited"),
    }
}

fn cfg() -> LogConfig {
    LogConfig {
        segment_bytes: 64 + 35 + 65_535 + 64,
        preallocate: false,
        max_record: 64,
        max_write_bytes: 35 + 65_535 + 64,
        max_unwritten_bytes: 35 + 65_535 + 64,
        ..LogConfig::default()
    }
}

fn flush(log: &mut Log) {
    while let Some(write) = log.take_write(0) {
        let written = now(write);
        log.written(written);
    }
}

/// A segment holding each kind of frame.
fn segment() -> (String, Vec<u8>) {
    let files = MemoryFiles::new();
    let mut log = now(Log::open(Arc::new(files.clone()), cfg())).expect("opened");
    for (key, epoch) in [(&b"a"[..], 1), (b"b", 1), (b"a", 2), (b"c", 3)] {
        log.append(key, epoch, Bytes::from_static(b"body")).expect("appended");
    }
    log.fence(b"b", 4).expect("fenced");
    log.checkpoint(b"a", Address { epoch: 2, seq: 1 }, Bytes::from_static(b"state")).expect("checkpointed");
    log.trim(b"c", Address { epoch: 3, seq: 1 }).expect("trimmed");
    log.append(b"d", 1, Bytes::new()).expect("appended");
    log.remove(b"d").expect("removed");
    flush(&mut log);
    let (name, _) = now(files.list()).expect("listed").pop().expect("a segment");
    let data = now(files.read(&name)).expect("read");
    (name, data)
}

type Seen = Result<Vec<(Bytes, KeyInfo, Vec<axon::files::Entry>)>, std::io::ErrorKind>;

fn seen(files: &MemoryFiles) -> (Seen, Option<Log>) {
    let log = match now(Log::open(Arc::new(files.clone()), cfg())) {
        Ok(log) => log,
        Err(e) => return (Err(e.kind()), None),
    };
    let mut seen = Vec::new();
    for (key, info) in log.keys(None, usize::MAX) {
        let entries = now(log.read(&key, Address::default(), usize::MAX)).expect("an open log reads what it lists");
        seen.push((key, info, entries));
    }
    (Ok(seen), Some(log))
}

fuzz_target!(|input: &[u8]| {
    let Some((at, bytes)) = input.split_first_chunk::<2>() else { return };
    let (name, mut data) = segment();
    let at = 64 + usize::from(u16::from_le_bytes(*at)) % (data.len() - 63);
    data.truncate(at);
    data.extend_from_slice(bytes);
    let files = MemoryFiles::new();
    now(files.apply(vec![FileOp::Replace(name, data.into())])).expect("written");
    let (first, log) = seen(&files);
    let (again, _) = seen(&files);
    assert_eq!(first, again, "opened twice");
    if let Some(mut log) = log
        && log.append(b"z", u64::MAX, Bytes::from_static(b"after")).is_ok()
    {
        flush(&mut log);
        assert!(log.failing().is_none());
        let (after, _) = seen(&files);
        let after = after.expect("opened after a write");
        assert!(after.iter().any(|(key, ..)| key == "z"), "the write after it reads back");
    }
});
