use axon::files::MemoryFiles;
use axon_testkit::{crash_points, crashes};

use super::*;
use crate::keyed;

/// What a future over `MemoryFiles` gives: it is ready at once.
fn now<T>(future: impl Future<Output = T>) -> T {
    match std::pin::pin!(future).poll(&mut std::task::Context::from_waker(std::task::Waker::noop())) {
        std::task::Poll::Ready(out) => out,
        std::task::Poll::Pending => panic!("files in memory waited"),
    }
}

trait Edit {
    fn apply_now(&self, ops: Vec<FileOp>);
    fn apply_failing(&self, ops: Vec<FileOp>, n: usize);
    fn names(&self) -> Vec<String>;
    fn edit(&self, name: &str, f: impl FnOnce(&mut Vec<u8>));
}

impl Edit for MemoryFiles {
    fn apply_now(&self, ops: Vec<FileOp>) {
        drop(self.apply(ops));
    }

    /// Applies the first `n` ops and fails, as `OsFiles` does: each file appended to since the last op that changed a
    /// file is cut back to its length before, and what was truncated, removed or replaced stays so.
    fn apply_failing(&self, ops: Vec<FileOp>, n: usize) {
        let mut cut: Vec<FileOp> = Vec::new();
        for op in ops.into_iter().take(n) {
            let len = |name: &str| now(self.read(name)).ok().map(|d| d.len() as u64);
            let changes = match &op {
                FileOp::Append(name, _) => {
                    if !cut.iter().any(|c| matches!(c, FileOp::Truncate(n, _) if n == name)) {
                        cut.push(FileOp::Truncate(name.clone(), len(name).unwrap_or(0)));
                    }
                    false
                }
                FileOp::Truncate(name, to) => len(name).is_some_and(|l| l > *to),
                FileOp::Remove(name) => len(name).is_some(),
                FileOp::Replace(..) => true,
                FileOp::Write(..) => false,
            };
            if changes {
                cut.clear();
            }
            self.apply_now(vec![op]);
        }
        self.apply_now(cut);
    }

    fn names(&self) -> Vec<String> {
        now(self.list()).unwrap().into_iter().map(|(name, _)| name).collect()
    }

    fn edit(&self, name: &str, f: impl FnOnce(&mut Vec<u8>)) {
        let mut data = now(self.read(name)).unwrap();
        f(&mut data);
        self.apply_now(vec![FileOp::Replace(name.to_owned(), data.into())]);
    }
}

const ALL: (usize, usize) = (usize::MAX, usize::MAX);

fn ok(_: &[Payload], _: Option<usize>) -> Result<(), StreamError> {
    Ok(())
}

impl DiskStore {
    /// Reads what an open resumes from, as the core does.
    fn resume(
        &mut self,
        key: &StreamKey,
        generation: u64,
        policy: DeliveryPolicy,
        ring: (usize, usize),
    ) -> Result<Option<Resume>, StreamError> {
        let Some(reader) = self.reader(key, generation, policy, ring)? else { return Ok(None) };
        now(reader.read()).map(Some).map_err(|e| self.read_failed(e))
    }
}

fn key() -> StreamKey {
    StreamKey::new("md", "btc/usd")
}

fn open_store(files: &MemoryFiles, max_bytes: u64, max_age: Duration) -> DiskStore {
    let retention = Retention { namespace: "md".into(), max_bytes, max_age };
    let mut cfg = DiskConfig::new(Arc::new(files.clone()), vec![retention]);
    cfg.segment_bytes = 100;
    now(DiskStore::open(cfg)).unwrap()
}

fn records(from: u64, n: u64) -> Vec<Payload> {
    (from..from + n).map(|i| Bytes::from(format!("record {i}"))).collect()
}

/// Writes what is queued, and answers what became durable.
fn write(store: &mut DiskStore, files: &MemoryFiles, now_ms: u64) -> Vec<(StreamKey, SequencedBatch)> {
    let Some(ops) = store.take(now_ms) else { return Vec::new() };
    files.apply_now(ops);
    store.written(Ok(()))
}

/// Opens the key as `open_stream` does, and answers the position it resumed at and the records the disk held.
fn reopen(files: &MemoryFiles, policy: DeliveryPolicy) -> (DiskStore, Cursor, Vec<Payload>, Vec<Payload>) {
    let mut store = open_store(files, u64::MAX, Duration::MAX);
    let resume = store.resume(&key(), 1, policy, ALL).unwrap().unwrap();
    let held = resume.batches.iter().flat_map(|b| b.records().to_vec()).collect();
    store.open_stream(&key(), 1, policy, &resume.latest);
    (store, resume.through, held, resume.latest)
}

#[test]
fn names_round_trip_any_key() {
    let key = StreamKey::new("md.x", "a/b %c.é");
    let name = segment_name(&key, 7, 42, 1_767_225_600_000);
    assert_eq!(name, "md%2Ex.a%2Fb%20%25c%2E%C3%A9.7.00000000000000000042.1767225600000.seg");
    assert_eq!(parse_name(&name), Some((key, 7, 42, 1_767_225_600_000)));
    assert_eq!(parse_name("md.btc.1.1.1.tmp"), None);
}

#[test]
fn batches_round_trip_across_segments_and_resume_after_the_last() {
    let files = MemoryFiles::default();
    let mut store = open_store(&files, u64::MAX, Duration::MAX);
    assert!(store.resume(&key(), 1, DeliveryPolicy::ReplayRequired, ALL).unwrap().is_none());
    store.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired, &[]);
    let mut next = 1;
    for n in [1, 3, 2, 5, 1, 4] {
        assert_eq!(
            store.admit(&key(), records(next, n), None, ok).unwrap(),
            Cursor { generation: 1, sequence: next + n - 1 }
        );
        next += n;
    }
    let durable = write(&mut store, &files, 1_000);
    assert_eq!(durable.iter().map(|(_, b)| b.len() as u64).sum::<u64>(), next - 1);
    assert!(files.names().len() > 1, "{:?}", files.names());
    assert_eq!(store.stats().unwritten_bytes, 0);

    let (_, through, held, _) = reopen(&files, DeliveryPolicy::ReplayRequired);
    assert_eq!(through, Cursor { generation: 1, sequence: next - 1 });
    assert_eq!(held, records(1, next - 1));
    // Only the newest segments that fill the ring are read back.
    let mut store = open_store(&files, u64::MAX, Duration::MAX);
    let tail = store.resume(&key(), 1, DeliveryPolicy::ReplayRequired, (3, usize::MAX)).unwrap().unwrap();
    let tail: Vec<Payload> = tail.batches.iter().flat_map(|b| b.records().to_vec()).collect();
    assert!((3..next as usize - 1).contains(&tail.len()), "{}", tail.len());
    assert_eq!(tail, records(next - tail.len() as u64, tail.len() as u64));

    // A newer generation starts over and removes the old one's files with its first write; an older one is refused.
    assert_eq!(store.stored(&key()), Some(through));
    assert!(store.resume(&key(), 2, DeliveryPolicy::ReplayRequired, ALL).unwrap().is_none());
    store.open_stream(&key(), 2, DeliveryPolicy::ReplayRequired, &[]);
    assert_eq!(store.stored(&key()), None);
    store.admit(&key(), records(1, 1), None, ok).unwrap();
    write(&mut store, &files, 2_000);
    assert_eq!(files.names(), [segment_name(&key(), 2, 1, 2_000)]);
    let mut store = open_store(&files, u64::MAX, Duration::MAX);
    let older = store.resume(&key(), 1, DeliveryPolicy::ReplayRequired, ALL).err();
    assert_eq!(older, Some(RecoveryError::GenerationMismatch.into()));
}

#[test]
fn an_older_generation_left_by_a_crash_is_removed_at_the_first_write() {
    let files = MemoryFiles::default();
    let mut store = open_store(&files, u64::MAX, Duration::MAX);
    store.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired, &[]);
    store.admit(&key(), records(1, 2), None, ok).unwrap();
    write(&mut store, &files, 1_000);
    let newer = segment_name(&key(), 2, 1, 2_000);
    files.apply_now(vec![FileOp::Append(newer.clone(), frame(BATCH, 1, &records(5, 1)))]);

    let mut store = open_store(&files, u64::MAX, Duration::MAX);
    assert_eq!(store.stored(&key()), Some(Cursor { generation: 2, sequence: 1 }));
    write(&mut store, &files, 3_000);
    assert_eq!(files.names(), [newer]);
}

#[test]
fn a_key_whose_file_names_would_be_too_long_is_refused() {
    let files = MemoryFiles::default();
    let mut store = open_store(&files, u64::MAX, Duration::MAX);
    let long = StreamKey::new("md", "/".repeat(80));
    let refused = store.resume(&long, 1, DeliveryPolicy::ReplayRequired, ALL).err();
    assert_eq!(refused, Some(StreamError::InvalidOptions));
}

#[test]
fn a_torn_tail_is_cut_at_the_last_whole_record() {
    let files = MemoryFiles::default();
    let mut store = open_store(&files, u64::MAX, Duration::MAX);
    store.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired, &[]);
    store.admit(&key(), records(1, 2), None, ok).unwrap();
    write(&mut store, &files, 1_000);
    let name = files.names()[0].clone();
    let whole = frame(BATCH, 3, &records(3, 1));
    files.edit(&name, |d| d.extend_from_slice(&whole[..whole.len() - 1]));

    let (mut store, through, held, _) = reopen(&files, DeliveryPolicy::ReplayRequired);
    assert_eq!((through.sequence, held), (2, records(1, 2)));
    let stats = store.stats();
    assert_eq!((stats.truncated, stats.truncated_bytes), (1, whole.len() as u64 - 1));
    // The next record takes the torn one's place, after the cut.
    assert_eq!(store.admit(&key(), records(9, 1), None, ok).unwrap().sequence, 3);
    write(&mut store, &files, 2_000);
    let (_, through, held, _) = reopen(&files, DeliveryPolicy::ReplayRequired);
    assert_eq!((through.sequence, &held[2]), (3, &records(9, 1)[0]));
}

#[test]
fn a_crc_mismatch_cuts_there_and_drops_every_later_segment() {
    let files = MemoryFiles::default();
    let mut store = open_store(&files, u64::MAX, Duration::MAX);
    store.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired, &[]);
    for i in 1..=30 {
        store.admit(&key(), records(i, 1), None, ok).unwrap();
    }
    write(&mut store, &files, 1_000);
    let names = files.names();
    assert!(names.len() >= 3, "{names:?}");
    // A byte of the second record of the second segment: its frame is whole, and only its CRC is wrong.
    let second = parse_name(&names[1]).unwrap().2;
    let one = frame(BATCH, 1, &records(1, 1)).len();
    files.edit(&names[1], |d| d[one + HEADER + 4] ^= 1);

    let (mut store, through, held, _) = reopen(&files, DeliveryPolicy::ReplayRequired);
    assert_eq!((through.sequence, held), (second, records(1, second)));
    assert_eq!(store.stats().truncated, 1);
    write(&mut store, &files, 2_000);
    assert_eq!(files.names(), names[..2]);
}

/// Positions below a segment's name are not taken again even when all its records are gone.
#[test]
fn a_history_cut_whole_goes_on_after_what_the_names_prove() {
    let compacted = DeliveryPolicy::Compacted(CompactLimits { max_keys: 64, max_bytes: 1 << 12 });
    for policy in [DeliveryPolicy::ReplayRequired, compacted] {
        let files = MemoryFiles::default();
        let mut store = open_store(&files, u64::MAX, Duration::MAX);
        store.open_stream(&key(), 1, policy, &[]);
        for i in 1..=30 {
            store.admit(&key(), vec![keyed(format!("key {i}").as_bytes(), b"value")], None, ok).unwrap();
        }
        write(&mut store, &files, 1_000);
        let names = files.names();
        files.apply_now(vec![FileOp::Remove(names[0].clone())]);
        files.edit(&names[1], |d| d[HEADER + 4] ^= 1);

        let (mut store, through, held, _) = reopen(&files, policy);
        let first = parse_name(&names[1]).unwrap().2;
        assert_eq!((through.sequence, held.len()), (first - 1, 0));
        let last = vec![keyed(b"key 99", b"value")];
        assert_eq!(store.admit(&key(), last.clone(), None, ok).unwrap().sequence, first);
        write(&mut store, &files, 2_000);
        let (_, through, held, latest) = reopen(&files, policy);
        assert_eq!((through.sequence, &held), (first, &last));
        if policy == compacted {
            assert_eq!(latest, last);
        }
    }
}

/// A stream that wrote 1 to a full segment, then 2..=5 to the next in a write that also removed the first for
/// retention: what it appended was synced before the removal took effect, so a failure after it leaves them.
#[test]
fn a_write_failed_after_its_removal_keeps_what_it_appended() {
    let files = MemoryFiles::default();
    let mut store = open_store(&files, 100, Duration::MAX);
    store.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired, &[]);
    store.admit(&key(), vec![Bytes::from(vec![1; 100])], None, ok).unwrap();
    assert_eq!(write(&mut store, &files, 1_000).len(), 1);
    store.admit(&key(), records(2, 4), None, ok).unwrap();
    let ops = store.take(2_000).unwrap();
    assert_eq!(ops.last(), Some(&FileOp::Remove(segment_name(&key(), 1, 1, 1_000))), "{ops:?}");
    files.apply_failing(ops.clone(), ops.len());
    assert!(store.written(Err(io::ErrorKind::Other.into())).is_empty());
    // Taken again, the write appends them a second time, and they are read once.
    assert_eq!(write(&mut store, &files, 3_000).len(), 1);
    let (_, through, held, _) = reopen(&files, DeliveryPolicy::ReplayRequired);
    assert_eq!((through.sequence, held), (5, records(2, 4)));
}

/// A segment whose every record a boot cut, and the only one left, stays empty: its name proves the positions before
/// it through the next restarts and writes.
#[test]
fn a_history_cut_whole_still_proves_its_position_after_a_write() {
    let files = MemoryFiles::default();
    let mut store = open_store(&files, 100, Duration::MAX);
    store.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired, &[]);
    store.admit(&key(), vec![Bytes::from(vec![1; 100])], None, ok).unwrap();
    write(&mut store, &files, 1_000);
    store.admit(&key(), records(2, 4), None, ok).unwrap();
    write(&mut store, &files, 2_000);
    let [second] = &files.names()[..] else { panic!("{:?}", files.names()) };
    files.edit(second, |d| d[HEADER + 4] ^= 1);
    let mut store = open_store(&files, u64::MAX, Duration::MAX);
    assert_eq!(store.stored(&key()), Some(Cursor { generation: 1, sequence: 1 }));
    // The writer writes before the stream is opened again.
    write(&mut store, &files, 3_000);
    assert_eq!(open_store(&files, u64::MAX, Duration::MAX).stored(&key()), Some(Cursor { generation: 1, sequence: 1 }));
    let (mut store, ..) = reopen(&files, DeliveryPolicy::ReplayRequired);
    assert_eq!(store.admit(&key(), records(9, 1), None, ok).unwrap().sequence, 2);
    write(&mut store, &files, 4_000);
    let (_, through, held, _) = reopen(&files, DeliveryPolicy::ReplayRequired);
    assert_eq!((through.sequence, held), (2, records(9, 1)));
}

/// Tried again, a write does not create anew a segment a close removed, holding only what it had to append to it.
#[test]
fn a_write_tried_again_does_not_bring_back_a_segment_a_close_removed() {
    let files = MemoryFiles::default();
    let mut store = open_store(&files, u64::MAX, Duration::MAX);
    store.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired, &[]);
    store.admit(&key(), records(1, 1), None, ok).unwrap();
    write(&mut store, &files, 1_000);
    store.admit(&key(), records(2, 4), None, ok).unwrap();
    let ops = store.take(2_000).unwrap();
    files.apply_failing(ops, 0);
    assert!(store.written(Err(io::ErrorKind::Other.into())).is_empty());
    store.close(&key());
    assert_eq!(store.take(2_500), Some(vec![FileOp::Remove(segment_name(&key(), 1, 1, 1_000))]));
}

/// A segment begun after a restart in the millisecond an earlier incarnation began one at the same position, which the
/// boot found torn and removes, is named after it: that removal, taken again with a failed write, leaves the new one.
#[test]
fn a_new_segment_never_takes_the_name_of_one_a_boot_removes() {
    let (files, replay) = (MemoryFiles::default(), DeliveryPolicy::ReplayRequired);
    let mut store = open_store(&files, u64::MAX, Duration::MAX);
    store.open_stream(&key(), 1, replay, &[]);
    store.admit(&key(), records(1, 8), None, ok).unwrap();
    write(&mut store, &files, 1_000);
    let torn = segment_name(&key(), 1, 9, 2_000);
    files.apply_now(vec![FileOp::Append(torn.clone(), Bytes::from_static(b"torn"))]);

    // Retention keeps only the segment being written.
    let mut store = open_store(&files, 1, Duration::MAX);
    assert_eq!(store.resume(&key(), 1, replay, ALL).unwrap().unwrap().through.sequence, 8);
    store.open_stream(&key(), 1, replay, &[]);
    store.admit(&key(), records(9, 1), None, ok).unwrap();
    let ops = store.take(2_000).unwrap();
    files.apply_failing(ops.clone(), ops.len());
    assert!(store.written(Err(io::ErrorKind::Other.into())).is_empty());
    // The first try removed the first segment, so it kept record 9. Taken again, the write fails after its first op,
    // the boot's removal, and the process crashes: the new segment is not the one it removes.
    let ops = store.take(2_500).unwrap();
    assert_eq!(ops[0], FileOp::Remove(torn));
    files.apply_failing(ops, 1);
    let stored = open_store(&files, u64::MAX, Duration::MAX).stored(&key());
    assert_eq!(stored, Some(Cursor { generation: 1, sequence: 9 }), "{:?}", files.names());
}

/// Retention lets go of a segment a failed write still appends to: its Remove drops that append, so the failed write's
/// own Remove of the segment before runs ahead of every append left, and a crash there leaves no file to prove the
/// position a subscriber saw.
#[test]
fn retention_keeps_a_segment_the_write_appends_to() {
    let files = MemoryFiles::default();
    // Retention keeps only the segment being written, and each record begins a segment.
    let mut store = open_store(&files, 1, Duration::MAX);
    store.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired, &[]);
    let big = |i: u8| vec![Bytes::from(vec![i; 100])];
    store.admit(&key(), big(1), None, ok).unwrap();
    assert_eq!(write(&mut store, &files, 1_000).len(), 1);
    store.admit(&key(), big(2), None, ok).unwrap();
    let ops = store.take(2_000).unwrap();
    store.admit(&key(), big(3), None, ok).unwrap();
    files.apply_failing(ops, 0);
    assert!(store.written(Err(io::ErrorKind::Other.into())).is_empty());
    // Taken again, it fails after its first op, and the process crashes.
    let ops = store.take(3_000).unwrap();
    files.apply_failing(ops, 1);
    let stored = open_store(&files, u64::MAX, Duration::MAX).stored(&key());
    assert!(stored.is_some_and(|c| c.sequence >= 1), "{stored:?} {:?}", files.names());
}

#[test]
fn retention_removes_the_oldest_segments_by_bytes() {
    let files = MemoryFiles::default();
    let mut store = open_store(&files, 300, Duration::MAX);
    store.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired, &[]);
    for i in 1..=40 {
        store.admit(&key(), records(i, 1), None, ok).unwrap();
        write(&mut store, &files, 1_000 + i * 10);
    }
    // Retention ran at the first write, and waits a second before it runs again.
    assert!(store.stats().bytes > 300);
    store.admit(&key(), records(41, 1), None, ok).unwrap();
    write(&mut store, &files, 2_100);
    let bytes = store.stats().bytes;
    assert!((150..=300).contains(&bytes), "{bytes}");
    let (_, _, held, _) = reopen(&files, DeliveryPolicy::ReplayRequired);
    assert_eq!(held.last(), Some(&records(41, 1)[0]));
    assert!(held.len() < 20, "{}", held.len());
}

#[test]
fn retention_goes_on_after_the_clock_steps_forward_and_back() {
    let files = MemoryFiles::default();
    let mut store = open_store(&files, 300, Duration::MAX);
    store.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired, &[]);
    store.admit(&key(), records(1, 1), None, ok).unwrap();
    write(&mut store, &files, 301_000);
    for i in 2..=60 {
        store.admit(&key(), records(i, 1), None, ok).unwrap();
        write(&mut store, &files, 1_000 + i * 100);
    }
    store.admit(&key(), records(61, 1), None, ok).unwrap();
    write(&mut store, &files, 8_000);
    let bytes = store.stats().bytes;
    assert!((150..=300).contains(&bytes), "{bytes}");
}

#[test]
fn retention_removes_a_segment_once_the_next_began_max_age_ago() {
    let files = MemoryFiles::default();
    let mut store = open_store(&files, u64::MAX, Duration::from_secs(60));
    store.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired, &[]);
    // Each batch fills a segment, so the next begins one.
    for (i, now) in [(1, 1_000), (9, 10_000), (17, 20_000)] {
        store.admit(&key(), records(i, 8), None, ok).unwrap();
        write(&mut store, &files, now);
    }
    let names = files.names();
    assert_eq!(names.len(), 3);
    write(&mut store, &files, 70_000);
    assert_eq!(files.names(), names);
    write(&mut store, &files, 71_000);
    assert_eq!(files.names(), names[1..]);
    // The segment being written stays however old.
    write(&mut store, &files, 3_600_000);
    assert_eq!(files.names(), names[2..]);
}

#[test]
fn a_compacted_stream_keeps_its_state_through_rolls_and_retention() {
    let files = MemoryFiles::default();
    let policy = DeliveryPolicy::Compacted(CompactLimits { max_keys: 8, max_bytes: 1 << 10 });
    let mut store = open_store(&files, 200, Duration::MAX);
    store.open_stream(&key(), 1, policy, &[]);
    let set = |k: &str, v: &str| vec![keyed(k.as_bytes(), v.as_bytes())];
    store.admit(&key(), set("a", "1"), None, ok).unwrap();
    store.admit(&key(), set("b", "1"), None, ok).unwrap();
    for i in 0..30 {
        store.admit(&key(), set("c", &i.to_string()), None, ok).unwrap();
        write(&mut store, &files, 1_000 * (i + 1));
    }
    store.admit(&key(), set("a", ""), None, ok).unwrap();
    write(&mut store, &files, 100_000);
    let first = parse_name(&files.names()[0]).unwrap().2;
    assert!(first > 3, "the segments holding a and b were not removed");

    let (_, through, _, latest) = reopen(&files, policy);
    assert_eq!(through.sequence, 33);
    assert_eq!(latest, [set("b", "1"), set("c", "29")].concat());
    let limits = CompactLimits { max_keys: 1, max_bytes: 1 << 10 };
    let mut reopened = open_store(&files, u64::MAX, Duration::MAX);
    let small = reopened.resume(&key(), 1, DeliveryPolicy::Compacted(limits), ALL).err();
    assert_eq!(small, Some(StreamError::StateFull));
    let replay = reopened.resume(&key(), 1, DeliveryPolicy::ReplayRequired, ALL).err();
    assert_eq!(replay, Some(StreamError::InvalidOptions));
}

/// The state resumed is what the segments written after the restart start with, so it outlives the ones before.
#[test]
fn a_compacted_state_outlives_restarts_and_retention() {
    let files = MemoryFiles::default();
    let policy = DeliveryPolicy::Compacted(CompactLimits { max_keys: 8, max_bytes: 1 << 10 });
    let set = |k: &str, v: &str| vec![keyed(k.as_bytes(), v.as_bytes())];
    let mut store = open_store(&files, u64::MAX, Duration::MAX);
    store.open_stream(&key(), 1, policy, &[]);
    store.admit(&key(), set("a", "1"), None, ok).unwrap();
    write(&mut store, &files, 1_000);
    for restart in 0..2 {
        let resume = open_store(&files, u64::MAX, Duration::MAX).resume(&key(), 1, policy, ALL).unwrap().unwrap();
        let mut store = open_store(&files, 200, Duration::MAX);
        store.open_stream(&key(), 1, policy, &resume.latest);
        for i in 0..30 {
            store.admit(&key(), set("c", &format!("{restart}{i}")), None, ok).unwrap();
            write(&mut store, &files, 100_000 * (restart + 1) + 1_000 * i);
        }
        assert!(parse_name(&files.names()[0]).unwrap().2 > 2, "the segment holding a was not removed");
    }
    assert_eq!(reopen(&files, policy).3, [set("a", "1"), set("c", "129")].concat());
}

#[test]
fn a_failed_write_refuses_publishes_until_it_is_written_again() {
    let files = MemoryFiles::default();
    let mut store = open_store(&files, u64::MAX, Duration::MAX);
    store.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired, &[]);
    store.admit(&key(), records(1, 2), None, ok).unwrap();
    store.take(1_000).unwrap();
    assert!(store.written(Err(io::ErrorKind::StorageFull.into())).is_empty());
    let refused = store.admit(&key(), records(3, 1), None, ok).unwrap_err();
    assert_eq!((refused.error, refused.returned), (StreamError::Disk(DiskError::Full), records(3, 1)));
    assert_eq!((store.stats().failing, store.stats().refused), (Some(DiskError::Full), 1));

    let durable = write(&mut store, &files, 2_000);
    assert_eq!(durable.len(), 1);
    assert_eq!(store.admit(&key(), records(3, 1), None, ok).unwrap().sequence, 3);
    assert_eq!(store.stats().failing, None);
}

#[test]
fn too_much_unwritten_is_refused_behind() {
    let files = MemoryFiles::default();
    let mut cfg = DiskConfig::new(
        Arc::new(files.clone()),
        vec![Retention { namespace: "md".into(), max_bytes: u64::MAX, max_age: Duration::MAX }],
    );
    cfg.max_unwritten_bytes = 64;
    let mut store = now(DiskStore::open(cfg)).unwrap();
    store.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired, &[]);
    store.admit(&key(), records(1, 2), None, ok).unwrap();
    let refused = store.admit(&key(), records(3, 8), None, ok).unwrap_err();
    assert_eq!((refused.error, refused.returned), (StreamError::Disk(DiskError::Behind), records(3, 8)));
    write(&mut store, &files, 1_000);
    assert_eq!(store.admit(&key(), records(3, 1), None, ok).unwrap().sequence, 3);
}

/// A compacted stream's segment starts with its state, which is then on disk twice too.
#[test]
fn a_write_that_stayed_although_it_failed_is_read_once() {
    let compacted = DeliveryPolicy::Compacted(CompactLimits { max_keys: 8, max_bytes: 1 << 10 });
    let set = |from: u64, n: u64| (from..from + n).map(|i| keyed(b"k", format!("{i}").as_bytes())).collect::<Vec<_>>();
    for policy in [DeliveryPolicy::ReplayRequired, compacted] {
        let files = MemoryFiles::default();
        let mut store = open_store(&files, u64::MAX, Duration::MAX);
        store.open_stream(&key(), 1, policy, &[]);
        store.admit(&key(), set(1, 2), None, ok).unwrap();
        // The write lands, and still reports an error: taken again, its frames are on disk twice.
        files.apply_now(store.take(1_000).unwrap());
        assert!(store.written(Err(io::ErrorKind::Other.into())).is_empty());
        assert_eq!(store.stats().failing, Some(DiskError::Failed));
        assert_eq!(write(&mut store, &files, 2_000).len(), 1);
        store.admit(&key(), set(3, 1), None, ok).unwrap();
        write(&mut store, &files, 3_000);

        let (store, through, held, _) = reopen(&files, policy);
        assert_eq!((through.sequence, held), (3, set(1, 3)), "{policy:?}");
        assert_eq!(store.stats().truncated, 0, "{policy:?}");
    }
}

/// A crash at any point inside a write that fills a segment, begins the next and removes the oldest for retention,
/// tearing either append or both, leaves files that open and read back every record written before, once and in
/// order, then the first of the write's own: the oldest segment is gone only once the write's records are all there.
/// The next record takes the next position, and is read back after the last.
#[test]
fn a_crash_anywhere_inside_a_write_keeps_every_record_written_before() {
    let files = MemoryFiles::default();
    let mut store = open_store(&files, 200, Duration::MAX);
    store.open_stream(&key(), 1, DeliveryPolicy::ReplayRequired, &[]);
    let admit = |store: &mut DiskStore, from: u64, n: u64| store.admit(&key(), records(from, n), None, ok).unwrap();
    // 45, 45 and 33 bytes: the first segment is full.
    for (from, n) in [(1, 2), (3, 2), (5, 1)] {
        admit(&mut store, from, n);
    }
    write(&mut store, &files, 1_000);
    for (from, n) in [(6, 2), (8, 2), (10, 1), (11, 1)] {
        admit(&mut store, from, n);
    }
    let ops = store.take(2_000).expect("the second write");
    let [FileOp::Append(a, _), FileOp::Append(b, _), FileOp::Remove(old)] = &ops[..] else { panic!("{ops:?}") };
    assert!(a != b && files.names() == [old.clone()], "{ops:?}");
    // Frames of 45, 45 and 34 bytes to the second segment, which is then full, and 34 to the third; then the removal.
    assert_eq!(crash_points(&ops), (124 + 2) * (34 + 2) + 1);
    let mut torn = 0;
    for (n, copy) in crashes(&files, &ops) {
        let (mut store, through, held, _) = reopen(&copy, DeliveryPolicy::ReplayRequired);
        let first = through.sequence + 1 - held.len() as u64;
        assert_eq!(held, records(first, held.len() as u64), "crash {n}");
        assert!(through.sequence >= 5, "crash {n}: through {through:?}");
        assert!(first == 1 || (first, through.sequence) == (6, 11), "crash {n}: {first}..={through:?}");
        torn += u64::from(store.stats().truncated > 0);
        assert_eq!(admit(&mut store, 99, 1).sequence, through.sequence + 1, "crash {n}");
        write(&mut store, &copy, 3_000);
        let (_, after, held, _) = reopen(&copy, DeliveryPolicy::ReplayRequired);
        assert_eq!((after.sequence, held.last()), (through.sequence + 1, records(99, 1).first()), "crash {n}");
    }
    assert!(torn > 0, "no crash tore a frame");
}
