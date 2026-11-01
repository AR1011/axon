//! Durable history at the origin. Each stream of a listed namespace is appended to segment files, a frame per batch
//! with its length and CRC, and what was published while the last write was out is written and synced in the next, in
//! one go. The records reach the hub, the ring and every subscriber only once they are synced, so an origin that
//! restarts resumes its generation after the last synced record and nobody has seen one past it.
//!
//! Nothing here does I/O but [`DiskStore::open`], which awaits a check of every segment once, and [`DiskRead::read`],
//! which awaits a stream's newest segments without the store: the node's writer task hands each write's ops to
//! [`Files::apply`] and the result back to the core.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use axon::files::crc32;
use bytes::{BufMut, Bytes, BytesMut};

use crate::hub::{DeliveryPolicy, PublishError, StreamError};
use crate::recovery::{
    CompactLimits, Cursor, MemoryState, Payload, RecoveryError, SequencedBatch, StreamKey, compacted,
};

#[cfg(feature = "cluster")]
pub use axon::files::OsFiles;
pub use axon::files::{FileOp, Files};

/// Where an origin keeps its streams' history, and for which namespaces.
#[derive(Clone)]
pub struct DiskConfig {
    pub files: Arc<dyn Files>,
    /// The namespaces whose streams are kept, each with its retention. Other streams keep only their ring.
    pub namespaces: Vec<Retention>,
    /// A stream moves to a new segment once its last holds this many bytes. Retention removes whole segments.
    pub segment_bytes: u64,
    /// Bytes published and not synced yet past which a publish is refused with `Disk(Behind)`.
    pub max_unwritten_bytes: usize,
}

impl DiskConfig {
    pub fn new(files: Arc<dyn Files>, namespaces: Vec<Retention>) -> Self {
        Self { files, namespaces, segment_bytes: 8 << 20, max_unwritten_bytes: 64 << 20 }
    }
}

/// How much of each stream in a namespace stays on disk. The oldest segment goes while the stream holds more than
/// `max_bytes`, or while the next one began more than `max_age` ago; the segment being written always stays.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Retention {
    pub namespace: String,
    pub max_bytes: u64,
    pub max_age: Duration,
}

/// Why a publish to a stream kept on disk was refused, nothing of it admitted, or why an open that read the disk failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub enum DiskError {
    /// The last write found the disk full. It is tried again every tenth of a second, and publishes are taken again
    /// once one succeeds.
    Full,
    /// The last write failed for another reason, tried again as for `Full`, or an open's read failed: the error is
    /// in [`DiskStats::error`].
    Failed,
    /// More than `max_unwritten_bytes` wait for the disk.
    Behind,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct DiskStats {
    /// Streams this node keeps on disk.
    pub streams: usize,
    /// Bytes in their segments, what is not written yet included.
    pub bytes: u64,
    /// Admitted, and not synced yet.
    pub unwritten_bytes: usize,
    pub writes: u64,
    pub write_failures: u64,
    /// Publishes refused with a `DiskError`.
    pub refused: u64,
    /// Set from a failed write until one succeeds.
    pub failing: Option<DiskError>,
    /// The last failed write's or read's error.
    pub error: Option<String>,
    /// Segments found cut short, or corrupt, when read back: each cut at its last whole record, and what followed it
    /// removed.
    pub truncated: u64,
    pub truncated_bytes: u64,
}

/// One write: its file ops, and the batches that are durable once they succeed.
#[derive(Default)]
struct Flush {
    ops: Vec<FileOp>,
    batches: Vec<(StreamKey, u64, SequencedBatch)>,
    bytes: usize,
}

const BATCH: u8 = 0;
const STATE: u8 = 1;
/// A frame's length and CRC, then its kind, position and record count.
const HEADER: usize = 8 + 13;

fn frame_len(records: &[Payload]) -> usize {
    HEADER + records.iter().map(|r| 4 + r.len()).sum::<usize>()
}

/// `len u32 | crc u32 | kind u8 | sequence u64 | count u32 | (len u32 | record)*`, little-endian. The sequence is a
/// batch's first, or the position a state is through.
fn frame(kind: u8, sequence: u64, records: &[Payload]) -> Bytes {
    let len = frame_len(records);
    let mut out = BytesMut::with_capacity(len);
    out.put_u32_le((len - 8) as u32);
    out.put_u32_le(0);
    out.put_u8(kind);
    out.put_u64_le(sequence);
    out.put_u32_le(records.len() as u32);
    for record in records {
        out.put_u32_le(record.len() as u32);
        out.put_slice(record);
    }
    let crc = crc32(&out[8..]);
    out[4..8].copy_from_slice(&crc.to_le_bytes());
    out.freeze()
}

/// A frame's kind, sequence and records.
type Frame = (u8, u64, Vec<Payload>);

/// The frame at `at` and where the next begins, or `None` for a torn, corrupt or malformed one.
fn parse(data: &Bytes, at: usize) -> Option<(Frame, usize)> {
    let u32_at = |p: usize| Some(u32::from_le_bytes(data.get(p..p + 4)?.try_into().ok()?) as usize);
    let end = at.checked_add(8)?.checked_add(u32_at(at)?)?;
    let body = data.get(at + 8..end)?;
    if body.len() < HEADER - 8 || crc32(body) != u32_at(at + 4)? as u32 || body[0] > STATE {
        return None;
    }
    let sequence = u64::from_le_bytes(body[1..9].try_into().ok()?);
    let (count, mut p) = (u32_at(at + 17)?, at + HEADER);
    let mut records = Vec::with_capacity(count.min(body.len() / 4));
    for _ in 0..count {
        let len = u32_at(p)?;
        let next = (p + 4).checked_add(len).filter(|n| *n <= end)?;
        records.push(data.slice(p + 4..next));
        p = next;
    }
    (p == end).then_some(((body[0], sequence, records), end))
}

fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b == b'_' || b == b'-' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn unescape(s: &str) -> Option<String> {
    let (mut out, mut bytes) = (Vec::with_capacity(s.len()), s.bytes());
    while let Some(b) = bytes.next() {
        out.push(match b {
            b'%' => u8::from_str_radix(std::str::from_utf8(&[bytes.next()?, bytes.next()?]).ok()?, 16).ok()?,
            b => b,
        });
    }
    String::from_utf8(out).ok()
}

/// `namespace.name.generation.first.created_ms.seg`, the key escaped, `first` zero-padded so names sort by it.
fn segment_name(key: &StreamKey, generation: u64, first: u64, created_ms: u64) -> String {
    format!("{}.{}.{generation}.{first:020}.{created_ms}.seg", escape(key.namespace()), escape(key.name()))
}

fn parse_name(name: &str) -> Option<(StreamKey, u64, u64, u64)> {
    let mut parts = name.strip_suffix(".seg")?.split('.');
    let mut next = || parts.next();
    let (namespace, stream) = (unescape(next()?)?, unescape(next()?)?);
    let (generation, first, created) = (next()?.parse().ok()?, next()?.parse().ok()?, next()?.parse().ok()?);
    next().is_none().then(|| (StreamKey::new(namespace, stream), generation, first, created))
}

/// How long after a failed write it is tried again.
pub(crate) const RETRY_WRITE: Duration = Duration::from_millis(100);

/// How often, at most, retention looks for segments to remove; while nothing is published too, as long as a stream
/// has one it could.
pub(crate) const RETAIN_EVERY: Duration = Duration::from_secs(1);

/// The longest file name the store makes; a key whose name would be longer is refused.
const MAX_NAME: usize = 240;

struct Segment {
    first: u64,
    /// Set when its first write is taken.
    created_ms: Option<u64>,
    bytes: u64,
}

impl Segment {
    /// Its file's name, once it has one.
    fn name(&self, key: &StreamKey, generation: u64) -> Option<String> {
        self.created_ms.map(|created| segment_name(key, generation, self.first, created))
    }
}

/// A stream the disk holds that no open has taken yet: its segments, checked at `open`, and the last position they
/// hold.
struct Found {
    generation: u64,
    segments: Vec<Segment>,
    last: u64,
}

/// The frames of one segment that follow `last`, up to the first torn, corrupt or out-of-place one.
struct Scan {
    /// The bytes they fill.
    good: usize,
    last: u64,
    /// A compacted stream's state at the segment's start.
    state: Option<Vec<Payload>>,
    batches: Vec<SequencedBatch>,
}

/// A frame that repeats positions already read, as a write tried again may leave, is skipped: a batch, or the state a
/// segment starts with.
fn scan(data: &Bytes, generation: u64, first: u64, last: u64) -> Scan {
    let mut out = Scan { good: 0, last, state: None, batches: Vec::new() };
    while let Some(((kind, sequence, records), end)) = parse(data, out.good) {
        if kind == STATE {
            if sequence != first.wrapping_sub(1) || out.good == 0 && sequence != out.last {
                break;
            }
            if out.good == 0 {
                out.state = Some(records);
            }
        } else {
            let Ok(batch) = SequencedBatch::new(generation, sequence, records) else { break };
            if batch.first() > out.last + 1 || (out.good == 0 && batch.first() != first) {
                break;
            }
            if let Some(batch) = batch.after(out.last) {
                out.last = batch.last();
                out.batches.push(batch);
            }
        }
        out.good = end;
    }
    out
}

/// Queues the file's removal, and drops the appends to it before: a write that fails after the removal and is tried
/// again would create the file anew, holding only them.
fn remove(ops: &mut Vec<FileOp>, name: String) {
    ops.retain(|op| !matches!(op, FileOp::Append(appended, _) if *appended == name));
    ops.push(FileOp::Remove(name));
}

/// A stream kept on disk. A new one, with another id, replaces it at each open and restart, so what is still queued
/// for the old one is dropped.
struct Durable {
    id: u64,
    generation: u64,
    /// The position the next admitted record takes.
    next: u64,
    /// The state through the last admitted record, which a new segment starts with.
    state: Option<MemoryState>,
    segments: VecDeque<Segment>,
    retention: Retention,
}

enum Op {
    Append { key: StreamKey, id: u64, first: u64, frame: Bytes },
    File(FileOp),
}

/// What a stream resumes from: the last durable position, the newest records the disk holds and a compacted stream's
/// state.
pub(crate) struct Resume {
    pub through: Cursor,
    pub first: u64,
    pub batches: Vec<SequencedBatch>,
    pub latest: Vec<Payload>,
}

/// What an open reads of a stream the disk holds, from its newest segment back until they fill its ring.
pub(crate) struct DiskRead {
    files: Arc<dyn Files>,
    generation: u64,
    limits: Option<CompactLimits>,
    /// Each segment's name, first position and length, oldest first.
    segments: Vec<(String, u64, u64)>,
    last: u64,
    ring: (usize, usize),
}

pub(crate) enum ReadError {
    Io(io::Error),
    /// `InvalidOptions` for a stream the disk holds under another policy, or a state past the policy's limits.
    Refused(StreamError),
}

impl From<StreamError> for ReadError {
    fn from(error: StreamError) -> Self {
        Self::Refused(error)
    }
}

impl DiskRead {
    /// Reads the files, and nothing else, so the caller awaits it holding no lock another stream needs.
    pub(crate) async fn read(self) -> Result<Resume, ReadError> {
        let Self { files, generation, limits, segments, last, ring: (max_records, max_bytes) } = self;
        let (mut tail, mut records, mut bytes, mut latest) = (Vec::new(), 0, 0, Vec::new());
        for (name, first, len) in segments.iter().rev() {
            if records >= max_records || bytes >= max_bytes {
                break;
            }
            let data = Bytes::from(files.read_range(name, 0, *len as usize).await.map_err(ReadError::Io)?);
            let mut scan = scan(&data, generation, *first, first - 1);
            // Copied, so neither the ring nor the state keeps whole segments.
            let copy = |records: &[Payload]| records.iter().map(|r| Bytes::copy_from_slice(r)).collect::<Vec<_>>();
            for batch in &mut scan.batches {
                records += batch.len();
                bytes += batch.bytes();
                *batch = SequencedBatch::new(generation, batch.first(), copy(batch.records())).expect("read above");
            }
            if tail.is_empty() {
                // The newest segment: a compacted stream's state is at its start, the batches after it.
                if limits.is_some() != scan.state.is_some() {
                    return Err(StreamError::InvalidOptions.into());
                }
                if let (Some(limits), Some(state)) = (limits, &scan.state) {
                    let mut rebuilt = MemoryState::new(limits);
                    rebuilt.apply(&copy(state), &mut |_| Ok(()))?;
                    for batch in &scan.batches {
                        rebuilt.apply(batch.records(), &mut |_| Ok(()))?;
                    }
                    latest = rebuilt.latest();
                }
            }
            tail.push(scan.batches);
        }
        let batches: Vec<SequencedBatch> = tail.into_iter().rev().flatten().collect();
        Ok(Resume {
            through: Cursor { generation, sequence: last },
            first: batches.first().map_or(last + 1, SequencedBatch::first),
            batches,
            latest,
        })
    }
}

pub(crate) struct DiskStore {
    cfg: DiskConfig,
    found: BTreeMap<StreamKey, Found>,
    streams: BTreeMap<StreamKey, Durable>,
    next_id: u64,
    unwritten: Vec<Op>,
    pending: Vec<(StreamKey, u64, SequencedBatch)>,
    /// The write being made, or one that failed and is taken again first.
    flush: Option<Flush>,
    retained_ms: u64,
    /// One past the newest `created_ms` of the files found at open, the least a new segment is named with: a clock
    /// that repeats a millisecond across restarts would otherwise give it the name of a file the boot removes.
    created_min: u64,
    stats: DiskStats,
}

impl DiskStore {
    /// Checks every segment of the listed namespaces' streams, one at a time: for each stream its newest generation,
    /// cut at its first torn or corrupt frame. Older generations and whatever follows a cut are removed with the first
    /// write; other namespaces' files are left alone.
    pub(crate) async fn open(cfg: DiskConfig) -> io::Result<Self> {
        let mut store = Self {
            found: BTreeMap::new(),
            streams: BTreeMap::new(),
            next_id: 0,
            unwritten: Vec::new(),
            pending: Vec::new(),
            flush: None,
            retained_ms: 0,
            created_min: 0,
            stats: DiskStats::default(),
            cfg,
        };
        // Per key and generation: each segment's first position, creation, name and length.
        type Listed = BTreeMap<u64, Vec<(u64, u64, String, u64)>>;
        let mut by_key: BTreeMap<StreamKey, Listed> = BTreeMap::new();
        for (name, len) in store.cfg.files.list().await? {
            if let Some((key, generation, first, created)) = parse_name(&name)
                && store.holds(&key)
            {
                store.created_min = store.created_min.max(created.saturating_add(1));
                by_key.entry(key).or_default().entry(generation).or_default().push((first, created, name, len));
            }
        }
        for (key, mut generations) in by_key {
            let Some((generation, mut segments)) = generations.pop_last() else { continue };
            for (.., name, _) in generations.into_values().flatten() {
                store.unwritten.push(Op::File(FileOp::Remove(name)));
            }
            segments.sort();
            // The names prove the positions before the first segment, should every segment be cut.
            let last = segments.first().map_or(0, |s| s.0.saturating_sub(1));
            let mut found = Found { generation, segments: Vec::new(), last };
            let mut cut = false;
            for (first, created, name, len) in segments {
                if cut {
                    store.stats.truncated_bytes += len;
                    store.unwritten.push(Op::File(FileOp::Remove(name)));
                    continue;
                }
                let data = Bytes::from(store.cfg.files.read(&name).await?);
                let Scan { good, last, .. } = scan(&data, generation, first, found.last);
                found.last = last;
                if good < data.len() {
                    cut = true;
                    store.stats.truncated += 1;
                    store.stats.truncated_bytes += (data.len() - good) as u64;
                }
                // The first stays, emptied, while nothing follows it: its name is the only proof of `last`.
                if good == 0 && !found.segments.is_empty() {
                    store.unwritten.push(Op::File(FileOp::Remove(name)));
                } else {
                    if good < data.len() {
                        store.unwritten.push(Op::File(FileOp::Truncate(name, good as u64)));
                    }
                    found.segments.push(Segment { first, created_ms: Some(created), bytes: good as u64 });
                }
            }
            store.found.insert(key, found);
        }
        Ok(store)
    }

    /// The open of a stream kept here: its id, generation and the position its next record takes.
    pub(crate) fn open_of(&self, key: &StreamKey) -> Option<(u64, u64, u64)> {
        self.streams.get(key).map(|d| (d.id, d.generation, d.next))
    }

    pub(crate) fn holds(&self, key: &StreamKey) -> bool {
        self.cfg.namespaces.iter().any(|r| r.namespace == key.namespace())
    }

    /// The generation and last record the disk holds of a stream no open has taken yet.
    pub(crate) fn stored(&self, key: &StreamKey) -> Option<Cursor> {
        self.found.get(key).map(|f| Cursor { generation: f.generation, sequence: f.last })
    }

    /// What an open of `generation` must read to go on from what the disk holds, which [`DiskRead::read`] reads
    /// without the store: `None` for a stream the disk does not hold, or holds of an older generation,
    /// `GenerationMismatch` for a newer one.
    pub(crate) fn reader(
        &self,
        key: &StreamKey,
        generation: u64,
        policy: DeliveryPolicy,
        ring: (usize, usize),
    ) -> Result<Option<DiskRead>, StreamError> {
        if segment_name(key, generation, u64::MAX, u64::MAX).len() > MAX_NAME {
            return Err(StreamError::InvalidOptions);
        }
        let Some(found) = self.found.get(key).filter(|f| f.generation >= generation) else { return Ok(None) };
        if found.generation > generation {
            return Err(RecoveryError::GenerationMismatch.into());
        }
        let segments = found.segments.iter().filter(|s| s.bytes > 0);
        let segments = segments.map(|s| (s.name(key, generation).expect("read from disk"), s.first, s.bytes));
        Ok(Some(DiskRead {
            files: self.cfg.files.clone(),
            generation,
            limits: compacted(policy),
            segments: segments.collect(),
            last: found.last,
            ring,
        }))
    }

    pub(crate) fn files(&self) -> Arc<dyn Files> {
        self.cfg.files.clone()
    }

    /// A read that failed: `Disk(Failed)`, its error kept in the stats.
    pub(crate) fn read_failed(&mut self, error: ReadError) -> StreamError {
        match error {
            ReadError::Io(error) => {
                self.stats.error = Some(error.to_string());
                StreamError::Disk(DiskError::Failed)
            }
            ReadError::Refused(error) => error,
        }
    }

    /// Goes on from what the disk holds of `generation`, whose state is `latest`, or starts it anew and removes what
    /// the disk holds of an older one.
    pub(crate) fn open_stream(&mut self, key: &StreamKey, generation: u64, policy: DeliveryPolicy, latest: &[Payload]) {
        self.close(key);
        let Some(retention) = self.cfg.namespaces.iter().find(|r| r.namespace == key.namespace()) else { return };
        self.next_id += 1;
        let mut durable = Durable {
            id: self.next_id,
            generation,
            next: 1,
            state: compacted(policy).map(MemoryState::new),
            segments: VecDeque::new(),
            retention: retention.clone(),
        };
        match self.found.remove(key) {
            Some(found) if found.generation == generation => {
                durable.next = found.last + 1;
                durable.segments = found.segments.into();
                if let Some(state) = &mut durable.state {
                    let _ = state.apply(latest, &mut |_| Ok(()));
                }
            }
            Some(found) => {
                let names = found.segments.iter().filter_map(|s| s.name(key, found.generation));
                self.unwritten.extend(names.map(|name| Op::File(FileOp::Remove(name))));
            }
            None => {}
        }
        self.streams.insert(key.clone(), durable);
    }

    /// A new generation: what the disk holds of the old one goes.
    pub(crate) fn restart(&mut self, key: &StreamKey, generation: u64) {
        let Some(limits) = self.streams.get(key).map(|d| d.state.as_ref().map(MemoryState::limits)) else { return };
        let policy = limits.map_or(DeliveryPolicy::ReplayRequired, DeliveryPolicy::Compacted);
        self.open_stream(key, generation, policy, &[]);
    }

    /// Removes the stream's files.
    pub(crate) fn close(&mut self, key: &StreamKey) {
        let Some(durable) = self.streams.remove(key) else { return };
        let names = durable.segments.iter().filter_map(|s| s.name(key, durable.generation));
        self.unwritten.extend(names.map(|name| Op::File(FileOp::Remove(name))));
    }

    /// Assigns the records their positions and queues them for the next write, once every check has passed and then
    /// `admit`, which is given the bytes a compacted stream's state would hold after them (`None` without one). They
    /// reach the hub once the write succeeds.
    pub(crate) fn admit(
        &mut self,
        key: &StreamKey,
        records: Vec<Payload>,
        trace: Option<(u64, u64)>,
        admit: impl FnOnce(&[Payload], Option<usize>) -> Result<(), StreamError>,
    ) -> Result<Cursor, PublishError<Vec<Payload>>> {
        let bytes = frame_len(&records);
        let refused = self.stats.failing.or_else(|| {
            (self.stats.unwritten_bytes + bytes > self.cfg.max_unwritten_bytes).then_some(DiskError::Behind)
        });
        if let Some(error) = refused {
            self.stats.refused += 1;
            return Err(PublishError { error: StreamError::Disk(error), returned: records });
        }
        let Some(durable) = self.streams.get_mut(key) else {
            return Err(PublishError { error: StreamError::UnknownStream, returned: records });
        };
        if durable.next.checked_add(records.len() as u64).is_none() {
            return Err(PublishError { error: RecoveryError::PositionOverflow.into(), returned: records });
        }
        let back = durable.segments.back().map(|s| s.bytes);
        let rolls = back.is_none_or(|bytes| bytes >= self.cfg.segment_bytes);
        // A compacted stream's every segment starts with its state before the segment's first batch, so the oldest
        // can go; an emptied one kept at open too.
        let before = durable.state.as_ref().filter(|_| rolls || back == Some(0)).map(|state| state.latest());
        let applied = match &mut durable.state {
            Some(state) => {
                let mut admit = Some(admit);
                state.apply(&records, &mut |bytes| admit.take().expect("asked once")(&records, Some(bytes)))
            }
            None => admit(&records, None),
        };
        if let Err(error) = applied {
            return Err(PublishError { error, returned: records });
        }
        if rolls {
            durable.segments.push_back(Segment { first: durable.next, created_ms: None, bytes: 0 });
        }
        if let Some(latest) = before {
            let frame = frame(STATE, durable.next - 1, &latest);
            self.stats.unwritten_bytes += frame.len();
            durable.segments.back_mut().expect("rolled above").bytes += frame.len() as u64;
            self.unwritten.push(Op::Append { key: key.clone(), id: durable.id, first: durable.next, frame });
        }
        let frame = frame(BATCH, durable.next, &records);
        let batch =
            SequencedBatch::new(durable.generation, durable.next, records).expect("checked above").traced(trace);
        let segment = durable.segments.back_mut().expect("rolled above");
        segment.bytes += frame.len() as u64;
        self.stats.unwritten_bytes += frame.len();
        durable.next = batch.last() + 1;
        self.unwritten.push(Op::Append { key: key.clone(), id: durable.id, first: segment.first, frame });
        self.pending.push((key.clone(), durable.id, batch.clone()));
        Ok(batch.cursor())
    }

    /// The ops of the next write, `None` when there is nothing to write: one at a time, each followed by `written`. At
    /// most every [`RETAIN_EVERY`] it also removes the segments retention lets go.
    pub(crate) fn take(&mut self, now_ms: u64) -> Option<Vec<FileOp>> {
        let mut flush = self.flush.take().unwrap_or_default();
        let mut appending: Option<(String, BytesMut)> = None;
        let append = |(name, bytes): (String, BytesMut)| FileOp::Append(name, bytes.freeze());
        for op in std::mem::take(&mut self.unwritten) {
            let op = match op {
                Op::File(op) => op,
                Op::Append { key, id, first, frame } => {
                    let durable = self.streams.get_mut(&key).filter(|d| d.id == id);
                    let generation = durable.as_ref().map_or(0, |d| d.generation);
                    let Some(segment) = durable.and_then(|d| d.segments.iter_mut().find(|s| s.first == first)) else {
                        self.stats.unwritten_bytes -= frame.len();
                        continue;
                    };
                    let name = segment_name(
                        &key,
                        generation,
                        first,
                        *segment.created_ms.get_or_insert(now_ms.max(self.created_min)),
                    );
                    flush.bytes += frame.len();
                    match &mut appending {
                        Some((last, bytes)) if *last == name => bytes.extend_from_slice(&frame),
                        _ => flush.ops.extend(appending.replace((name, BytesMut::from(&frame[..]))).map(append)),
                    }
                    continue;
                }
            };
            flush.ops.extend(appending.take().map(append));
            match op {
                FileOp::Remove(name) => remove(&mut flush.ops, name),
                op => flush.ops.push(op),
            }
        }
        flush.ops.extend(appending.map(append));
        flush.batches.append(&mut self.pending);
        // A wall clock stepped back would otherwise hold retention off for as long as the step.
        if now_ms >= self.retained_ms.saturating_add(RETAIN_EVERY.as_millis() as u64) || now_ms < self.retained_ms {
            self.retained_ms = now_ms;
            self.retain(now_ms, &mut flush.ops);
        }
        if flush.ops.is_empty() && flush.batches.is_empty() {
            return None;
        }
        let ops = flush.ops.clone();
        self.flush = Some(flush);
        Some(ops)
    }

    /// Whether a write waits to be taken.
    pub(crate) fn queued(&self) -> bool {
        !self.unwritten.is_empty()
    }

    /// Whether retention may yet remove a segment: some stream has more than the one being written.
    pub(crate) fn may_retain(&self) -> bool {
        self.streams.values().any(|d| d.segments.len() > 1)
    }

    fn retain(&mut self, now_ms: u64, ops: &mut Vec<FileOp>) {
        for (key, durable) in &mut self.streams {
            let (max_bytes, max_age) = (durable.retention.max_bytes, durable.retention.max_age.as_millis() as u64);
            let mut bytes: u64 = durable.segments.iter().map(|s| s.bytes).sum();
            while durable.segments.len() > 1 {
                let old = durable.segments[1].created_ms.is_some_and(|c| c.saturating_add(max_age) < now_ms);
                let name = durable.segments[0].name(key, durable.generation);
                // A segment this write appends to stays: removing it drops the append, and a failed write's Remove of
                // the segment before would then run ahead of every append left, leaving no file to prove a position
                // the hub hands out if the process stops there.
                let busy =
                    name.as_ref().is_some_and(|n| ops.iter().any(|op| matches!(op, FileOp::Append(a, _) if a == n)));
                if bytes <= max_bytes && !old || busy {
                    break;
                }
                bytes -= durable.segments.pop_front().expect("more than one").bytes;
                if let Some(name) = name {
                    remove(ops, name);
                }
            }
        }
    }

    /// The batches the write taken last made durable, in order, of streams still open; none when it failed, and then
    /// it is taken again first.
    pub(crate) fn written(&mut self, result: io::Result<()>) -> Vec<(StreamKey, SequencedBatch)> {
        let Some(flush) = self.flush.take() else { return Vec::new() };
        if let Err(error) = result {
            self.stats.write_failures += 1;
            let full = error.kind() == io::ErrorKind::StorageFull;
            self.stats.failing = Some(if full { DiskError::Full } else { DiskError::Failed });
            self.stats.error = Some(error.to_string());
            self.flush = Some(flush);
            return Vec::new();
        }
        self.stats.writes += 1;
        self.stats.failing = None;
        self.stats.unwritten_bytes -= flush.bytes;
        let open = |key: &StreamKey, id: u64| self.streams.get(key).is_some_and(|d| d.id == id);
        flush.batches.into_iter().filter(|(key, id, _)| open(key, *id)).map(|(key, _, batch)| (key, batch)).collect()
    }

    pub(crate) fn failing(&self) -> Option<DiskError> {
        self.stats.failing
    }

    pub(crate) fn stats(&self) -> DiskStats {
        let bytes = self.streams.values().flat_map(|d| &d.segments).map(|s| s.bytes).sum();
        DiskStats { streams: self.streams.len(), bytes, ..self.stats.clone() }
    }
}

#[cfg(test)]
mod tests;
