//! Durable history at the origin. Each stream of a listed namespace is appended to segment files, a frame per batch
//! with its length and CRC, and what was published while the last write was out is written and synced in the next, in
//! one go. The records reach the hub, the ring and every subscriber only once they are synced, so an origin that
//! restarts resumes its generation after the last synced record and nobody has seen one past it.
//!
//! Nothing here does I/O but [`DiskStore::open`], which awaits a check of every segment once, and [`DiskRead::read`],
//! which awaits a stream's newest segments without the store: the node's writer task hands each write's ops to
//! [`Files::apply`] and the result back to the core, and reads the segments a start behind the ring asks the
//! [`Shelf`] for.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axon::files::crc32;
use bytes::{BufMut, Bytes, BytesMut};

use crate::hub::{DeliveryPolicy, PublishError, StreamError};
use crate::recovery::{
    CompactLimits, Cursor, MemoryState, Payload, ReadBounds, RecoveryError, RecoverySource, ReplayRange,
    SequencedBatch, StreamKey, compacted, lock, page,
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
    /// The segments read in for starts behind a ring that stay in memory, and the most one read of the writer task
    /// brings in: more starts at once take turns. `StreamNode::start` refuses 0.
    pub max_read_segments: usize,
    /// How long a segment read in keeps its place while no start reads from it. Past that it may go as one read from
    /// does, so a start that stalls or ends before it reads holds no place for good; polled later, it reads the
    /// segment in again. `StreamNode::start` refuses 0.
    pub unread_expiry: Duration,
}

impl DiskConfig {
    pub fn new(files: Arc<dyn Files>, namespaces: Vec<Retention>) -> Self {
        let unread_expiry = Duration::from_secs(10);
        Self {
            files,
            namespaces,
            segment_bytes: 8 << 20,
            max_unwritten_bytes: 64 << 20,
            max_read_segments: 8,
            unread_expiry,
        }
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

/// The names and first positions of the segments that have files, as the shelf indexes them.
fn named<'a>(key: &StreamKey, generation: u64, segments: impl IntoIterator<Item = &'a Segment>) -> Vec<(String, u64)> {
    segments.into_iter().filter_map(|s| Some((s.name(key, generation)?, s.first))).collect()
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

/// A segment the shelf asks the writer task to read.
#[derive(Clone)]
pub(crate) struct Asked {
    name: String,
    key: StreamKey,
    generation: u64,
    first: u64,
    /// The last position the read found, at least the last durable one when it was asked for. A start at or past it
    /// reads the file again.
    upto: u64,
    /// When a start last read a page from it, in the shelf's count of pages; 0 before one has.
    served: u64,
    /// Read in and not read from: when it was read in, `None` once it sat so for `unread_expiry` and may go.
    unread_since: Option<Instant>,
}

/// A segment read in: its batches, `None` when it could not be read.
pub(crate) type SegmentRead = (Asked, Option<Arc<[SequencedBatch]>>);

/// The segments of the streams kept here, as a history source behind their rings: a start behind a ring asks for the
/// segment that holds it, the node's writer task reads it, and the answer is `Pending` until it is in.
#[derive(Default)]
pub(crate) struct Shelf(Mutex<Shelved>);

/// A stream's generation, its last durable position, and its segments' names and first positions, oldest first.
struct Indexed {
    generation: u64,
    last: u64,
    segments: Vec<(String, u64)>,
}

#[derive(Default)]
struct Shelved {
    index: BTreeMap<StreamKey, Indexed>,
    /// In the order they were asked for, so each start waiting gets its turn.
    asked: VecDeque<Asked>,
    reading: BTreeSet<String>,
    read: VecDeque<SegmentRead>,
    pages: u64,
}

impl Shelf {
    /// The stream's generation and segments now, or `None` once it is closed. A segment gone, removed by retention or
    /// a close, is no longer served; the last durable position stays while the generation does.
    fn index(&self, key: &StreamKey, now: Option<(u64, Vec<(String, u64)>)>) {
        let mut s = lock(&self.0);
        let Some((generation, segments)) = now else {
            s.index.remove(key);
            s.read.retain(|(asked, _)| asked.key != *key);
            s.asked.retain(|asked| asked.key != *key);
            return;
        };
        s.read.retain(|(asked, _)| asked.key != *key || segments.iter().any(|(name, _)| *name == asked.name));
        let last = s.index.get(key).filter(|e| e.generation == generation).map_or(0, |e| e.last);
        s.index.insert(key.clone(), Indexed { generation, last, segments });
    }

    fn durable<'a>(&self, lasts: impl IntoIterator<Item = (&'a StreamKey, u64)>) {
        let mut s = lock(&self.0);
        for (key, last) in lasts {
            if let Some(entry) = s.index.get_mut(key) {
                entry.last = last;
            }
        }
    }

    /// Whether a read is due: segments asked for, and room for them.
    fn asked(&self, max: usize) -> bool {
        let s = lock(&self.0);
        !s.asked.is_empty() && s.room(max) > 0
    }

    /// At most as many as there is room for, once the segments unread for `expiry` give theirs up; the rest wait for
    /// the next read.
    fn take_asked(&self, max: usize, expiry: Duration, now: Instant) -> Vec<Asked> {
        let mut s = lock(&self.0);
        for (asked, _) in &mut s.read {
            if asked.unread_since.and_then(|at| at.checked_add(expiry)).is_some_and(|at| at <= now) {
                asked.unread_since = None;
            }
        }
        let n = s.room(max).min(s.asked.len());
        let asked: Vec<Asked> = s.asked.drain(..n).collect();
        s.reading.extend(asked.iter().map(|a| a.name.clone()));
        asked
    }

    /// When the first segment read in and not read from has sat so for `expiry`, and may give up its place.
    fn unread_ages(&self, expiry: Duration) -> Option<Instant> {
        let s = lock(&self.0);
        s.read.iter().filter_map(|(a, _)| a.unread_since?.checked_add(expiry)).min()
    }

    /// Keeps what a read brought in at `now`, at most `max` segments, and returns the streams it was for. Past `max`,
    /// a segment that sat unread too long goes, else the one a start read from longest ago.
    fn read_in(&self, read: Vec<SegmentRead>, max: usize, now: Instant) -> BTreeSet<StreamKey> {
        let mut s = lock(&self.0);
        let mut keys = BTreeSet::new();
        for (mut asked, batches) in read {
            s.reading.remove(&asked.name);
            keys.insert(asked.key.clone());
            if s.index.get(&asked.key).is_some_and(|e| e.segments.iter().any(|(name, _)| *name == asked.name)) {
                asked.unread_since = Some(now);
                s.read.push_back((asked, batches));
            }
        }
        while s.read.len() > max {
            // A read takes only the room the unread segments leave, so one may go; else the newest goes.
            let may_go = s.read.iter().enumerate().filter(|(_, (a, _))| a.unread_since.is_none());
            let at = may_go.min_by_key(|(_, (a, _))| a.served).map_or(s.read.len() - 1, |(at, _)| at);
            s.read.remove(at);
        }
        keys
    }
}

impl Shelved {
    /// The segments a read may bring in: `max` less those read in that no start has read from yet nor sat so too
    /// long, and those being read. A read never brings in one it would evict unread, so the starts that asked first
    /// keep theirs.
    fn room(&self, max: usize) -> usize {
        let unread = self.read.iter().filter(|(a, _)| a.unread_since.is_some()).count();
        max.saturating_sub(unread + self.reading.len())
    }
}

impl RecoverySource for Shelf {
    fn available_range(&self, key: &StreamKey) -> Result<ReplayRange, RecoveryError> {
        let s = lock(&self.0);
        let Indexed { generation, last, segments } = s.index.get(key).ok_or(RecoveryError::UnknownStream)?;
        let first = segments.first().map_or(last + 1, |(_, first)| *first);
        Ok(ReplayRange { generation: *generation, first, last: *last })
    }

    fn read_after(
        &self,
        key: &StreamKey,
        after: Cursor,
        through: u64,
        bounds: ReadBounds,
    ) -> Result<Vec<SequencedBatch>, RecoveryError> {
        let mut guard = lock(&self.0);
        let s = &mut *guard;
        let Indexed { generation, last, segments } = s.index.get(key).ok_or(RecoveryError::UnknownStream)?;
        if after.generation != *generation {
            return Err(RecoveryError::GenerationMismatch);
        }
        if after.sequence >= through {
            return Ok(Vec::new());
        }
        let Some((name, first)) = segments.iter().rev().find(|(_, first)| *first <= after.sequence + 1) else {
            return Err(RecoveryError::HistoryUnavailable);
        };
        // A segment read before the start was durable is read again.
        s.read.retain(|(a, _)| a.name != *name || a.upto > after.sequence);
        let Some(at) = s.read.iter().position(|(a, _)| a.name == *name) else {
            if !s.reading.contains(name) && !s.asked.iter().any(|a| a.name == *name) {
                let (name, key, generation, first, upto) = (name.clone(), key.clone(), *generation, *first, *last);
                s.asked.push_back(Asked { name, key, generation, first, upto, served: 0, unread_since: None });
            }
            return Err(RecoveryError::Pending);
        };
        // A read that failed resets the start it served; the next start reads the file again.
        let Some(batches) = s.read[at].1.clone() else {
            s.read.remove(at);
            return Err(RecoveryError::HistoryUnavailable);
        };
        let start = batches.partition_point(|b| b.last() <= after.sequence);
        let mut held = batches[start..]
            .iter()
            .flat_map(|b| (b.first()..).zip(b.records()))
            .skip_while(|(sequence, _)| *sequence <= after.sequence)
            .map(|(_, record)| record)
            .peekable();
        if held.peek().is_none() {
            // The file lost it.
            return Err(RecoveryError::HistoryUnavailable);
        }
        let page = page(after, through, bounds, held);
        s.pages += 1;
        (s.read[at].0.served, s.read[at].0.unread_since) = (s.pages, None);
        page
    }
}

/// Reads the segments the shelf asked for, one at a time, without the store.
pub(crate) async fn read_asked(files: Arc<dyn Files>, asked: Vec<Asked>) -> Vec<SegmentRead> {
    let mut read = Vec::with_capacity(asked.len());
    for mut asked in asked {
        let data = files.read(&asked.name).await.ok().map(Bytes::from);
        let batches = data.map(|data| {
            let scan = scan(&data, asked.generation, asked.first, asked.first - 1);
            asked.upto = asked.upto.max(scan.last);
            // Copied, as `DiskRead::read` does, so a replayed record does not keep its whole segment.
            let copy = |b: &SequencedBatch| {
                let records: Vec<_> = b.records().iter().map(|r| Bytes::copy_from_slice(r)).collect();
                SequencedBatch::new(asked.generation, b.first(), records).expect("read above")
            };
            scan.batches.iter().map(copy).collect()
        });
        read.push((asked, batches));
    }
    read
}

pub(crate) struct DiskStore {
    cfg: DiskConfig,
    shelf: Arc<Shelf>,
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
            shelf: Arc::default(),
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
            // Shelved before any open takes it: followers behind the open replay from it as the open is heard.
            store.shelf.index(&key, Some((generation, named(&key, generation, &found.segments))));
            store.shelf.durable([(&key, found.last)]);
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
        let last = durable.next - 1;
        self.streams.insert(key.clone(), durable);
        self.shelve(key);
        self.shelf.durable([(key, last)]);
    }

    /// Tells the shelf the stream's segments that have files.
    fn shelve(&self, key: &StreamKey) {
        let now = self.streams.get(key).map(|d| (d.generation, named(key, d.generation, &d.segments)));
        self.shelf.index(key, now);
    }

    /// The history of a stream kept here, behind its ring.
    pub(crate) fn history(&self, key: &StreamKey) -> Option<Arc<dyn RecoverySource>> {
        self.holds(key).then(|| self.shelf.clone() as Arc<dyn RecoverySource>)
    }

    /// The segments starts behind a ring asked for, to read with [`read_asked`] and hand to [`read_in`](Self::read_in).
    pub(crate) fn take_reads(&self, now: Instant) -> Option<(Arc<dyn Files>, Vec<Asked>)> {
        let asked = self.shelf.take_asked(self.cfg.max_read_segments, self.cfg.unread_expiry, now);
        (!asked.is_empty()).then(|| (self.cfg.files.clone(), asked))
    }

    /// Keeps what a read brought in; returns the streams whose starts wait on it.
    pub(crate) fn read_in(&self, read: Vec<SegmentRead>, now: Instant) -> BTreeSet<StreamKey> {
        self.shelf.read_in(read, self.cfg.max_read_segments, now)
    }

    /// When a segment read in may give up its place unread, for a read due then.
    pub(crate) fn unread_ages(&self) -> Option<Instant> {
        self.shelf.unread_ages(self.cfg.unread_expiry)
    }

    #[cfg(test)]
    pub(crate) fn read_segments(&self) -> usize {
        lock(&self.shelf.0).read.len()
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
        self.shelve(key);
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
        let mut shelve = BTreeSet::new();
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
                    if segment.created_ms.is_none() {
                        shelve.insert(key.clone());
                    }
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
            self.retain(now_ms, &mut flush.ops, &mut shelve);
        }
        for key in shelve {
            self.shelve(&key);
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
        !self.unwritten.is_empty() || self.shelf.asked(self.cfg.max_read_segments)
    }

    /// Whether retention may yet remove a segment: some stream has more than the one being written.
    pub(crate) fn may_retain(&self) -> bool {
        self.streams.values().any(|d| d.segments.len() > 1)
    }

    fn retain(&mut self, now_ms: u64, ops: &mut Vec<FileOp>, shelve: &mut BTreeSet<StreamKey>) {
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
                shelve.insert(key.clone());
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
        let durable: Vec<_> = flush
            .batches
            .into_iter()
            .filter(|(key, id, _)| open(key, *id))
            .map(|(key, _, batch)| (key, batch))
            .collect();
        self.shelf.durable(durable.iter().map(|(key, batch)| (key, batch.last())));
        durable
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
