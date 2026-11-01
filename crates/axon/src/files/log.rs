//! [`Log`]: the records of many keys in one directory, written by one owner. One write and one sync cover every key
//! appended since the last write. The design and the measurements behind it: spec/durable-log.md.

use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;
use std::sync::Arc;
use std::{fmt, io};

use bytes::{BufMut, Bytes, BytesMut};

use super::{FileFuture, FileOp, Files, crc32};

#[cfg(test)]
mod tests;

const MAGIC: &[u8; 4] = b"AXLG";
const VERSION: u16 = 1;
const ZERO_FILLED: u16 = 1;
/// A segment's header: magic, version, flags, log id, first LSN, created ms, zero-filled length, CRC, zeros.
const HEADER: u64 = 64;
/// A frame's bytes besides its key and body: length, CRC, LSN, kind, key length, epoch, seq.
const FRAME: usize = 35;
/// What a frame's length counts and its CRC covers, besides its key and body.
const AFTER_CRC: usize = FRAME - 8;
/// The longest key.
const MAX_KEY: usize = u16::MAX as usize;
/// Frames of one segment closer than this are read in one read.
const READ_GAP: u64 = 64 << 10;

const RECORD: u8 = 1;
const CHECKPOINT: u8 = 2;
const FENCE: u8 = 3;
const TRIM: u8 = 4;
const REMOVE: u8 = 5;

/// How a [`Log`] lays out its files, and what it refuses.
#[derive(Clone, Debug)]
pub struct LogConfig {
    /// Segments are named `<name>.<first LSN in 20 digits>`: letters, digits, `-` and `_`.
    pub name: String,
    /// A segment's length: a frame that does not fit in the last segment starts a new one.
    pub segment_bytes: u64,
    /// Zero-fill each segment ahead of its frames, up to `segment_bytes`, so that a write over the zeros does not grow
    /// the file, and its sync need not sync a new length: 3.3 times faster on build-1 (spec/durable-log.md §2).
    pub preallocate: bool,
    /// With `preallocate`, how far past its frames a write zero-fills once they pass the zeros: about one write in
    /// this many bytes of frames grows the file, and none fills more.
    pub fill_ahead: u64,
    /// The largest record body or checkpoint state.
    pub max_record: usize,
    /// Past this many bytes not yet durable, a change is refused `Behind`.
    pub max_unwritten_bytes: usize,
    /// The most frame bytes one write takes; the rest go in the next. Torn bytes further than twice this after the
    /// tail are read as corruption on open.
    pub max_write_bytes: usize,
    /// Past this many live bytes, a record is refused `Full`.
    pub max_bytes: u64,
    /// Past this many keys, a new key is refused `Keys`.
    pub max_keys: usize,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            name: "log".into(),
            segment_bytes: 64 << 20,
            preallocate: true,
            fill_ahead: 256 << 10,
            max_record: 4 << 20,
            max_unwritten_bytes: 64 << 20,
            max_write_bytes: 8 << 20,
            max_bytes: 4 << 30,
            max_keys: 1 << 20,
        }
    }
}

impl LogConfig {
    fn largest_frame(&self) -> usize {
        FRAME + MAX_KEY + self.max_record
    }

    fn check(&self) -> io::Result<()> {
        let refuse = |what: &str| Err(io::Error::new(io::ErrorKind::InvalidInput, format!("LogConfig: {what}")));
        let largest = self.largest_frame();
        if self.name.is_empty() || !self.name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
            refuse("name must be letters, digits, - and _")
        } else if self.max_write_bytes < largest || self.max_unwritten_bytes < largest {
            refuse("max_write_bytes and max_unwritten_bytes must hold the largest frame, 35 + 65 535 + max_record")
        } else if self.segment_bytes < HEADER + largest as u64 {
            refuse("segment_bytes must hold a header of 64 and the largest frame")
        } else {
            Ok(())
        }
    }

    /// How far after the tail of the last segment the bytes of one torn write may lie.
    fn reach(&self) -> u64 {
        2 * self.max_write_bytes as u64
    }
}

/// A record's place among its key's: the epoch it was written in, and its number in that epoch, from 1. Ordered by
/// epoch, then number.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Address {
    pub epoch: u64,
    pub seq: u64,
}

/// A frame's number in its log, one more for each frame: what [`Log::durable`] counts in.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

/// Where [`Log::append`] put a record: durable once [`Log::durable`] reaches `lsn`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Appended {
    pub lsn: Lsn,
    pub at: Address,
}

/// Why a change was refused. A refusal changes nothing. A failing disk is not one: see [`Log::failing`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogError {
    /// A body or state past `max_record`, or a key past 65 535 bytes.
    TooLarge,
    /// Bytes not yet durable past `max_unwritten_bytes`.
    Behind,
    /// Live bytes past `max_bytes`: checkpoint, trim or remove keys (see [`Log::pinning`]).
    Full,
    /// Keys past `max_keys`.
    Keys,
    /// The key is fenced at this epoch: a write from an older one, or a fence at or below it.
    Fenced { fence: u64 },
    /// The position is not the next of its epoch, or past the key's last: the epoch holds `have` records.
    Gap { have: u64 },
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge => f.write_str("the record or key is too large"),
            Self::Behind => f.write_str("too many bytes are not yet durable"),
            Self::Full => f.write_str("the log is full"),
            Self::Keys => f.write_str("the log holds too many keys"),
            Self::Fenced { fence } => write!(f, "the key is fenced at epoch {fence}"),
            Self::Gap { have } => write!(f, "the epoch holds {have} records"),
        }
    }
}

impl std::error::Error for LogError {}

/// What the log holds of a key.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyInfo {
    /// The highest epoch written or fenced.
    pub fence: u64,
    /// Each epoch it holds records of, or a checkpoint or trim in, with the number of its last.
    pub epochs: Vec<(u64, u64)>,
    pub checkpoint: Option<Address>,
    /// Records at or before this position are gone: the checkpoint's or the last trim's.
    pub trimmed: Option<Address>,
    pub live_bytes: u64,
}

/// What [`Log::read`] returns, in address order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    /// Stands for every record at or before `through`.
    Checkpoint {
        through: Address,
        state: Bytes,
    },
    Record {
        at: Address,
        body: Bytes,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogStats {
    pub durable: Lsn,
    /// Frames not yet durable, the write out included.
    pub unwritten_bytes: usize,
    /// The segments' length on disk.
    pub bytes: u64,
    pub live_bytes: u64,
    pub segments: usize,
    /// Keys in the index, a removed one until the segments holding its frames are gone.
    pub keys: usize,
    pub writes: u64,
    pub failed_writes: u64,
    /// Bytes copied forward out of mostly dead segments.
    pub copied_bytes: u64,
    /// Bytes of a torn tail cut on open.
    pub truncated_bytes: u64,
}

/// A frame's place: its LSN, and its offset and length in the segment that holds that LSN.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Loc {
    lsn: u64,
    offset: u64,
    len: u32,
}

/// A checkpoint, trim, remove or fence frame of a key, and whether it is counted live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Meta {
    at: Address,
    loc: Loc,
    live: bool,
}

/// A key in the index. Every field follows from its frames on disk, read in LSN order, so a reopened log holds the
/// same.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Key {
    /// The highest epoch of any of its frames.
    fence: u64,
    /// Above the checkpoint and the trim, in address order.
    records: VecDeque<(Address, Loc)>,
    checkpoint: Option<Meta>,
    /// The newest trim or remove frame above the checkpoint. A remove frame is never live: it stays while an older
    /// segment holds frames of the key it ends, and goes with its segment.
    trim: Option<Meta>,
    /// `trim` is a remove frame.
    removed: bool,
    /// The newest remove frame, until its segment goes. A later trim may take its place in `trim`, but it must stay
    /// while an older segment holds frames of the key that it ended, or open would read them again.
    tomb: Option<Loc>,
    fence_frame: Option<Meta>,
    /// Segments holding any frame of it: at none, it is gone from the index.
    present: u32,
    live_bytes: u64,
}

impl Key {
    fn low(&self) -> Option<Address> {
        match (self.trim, self.checkpoint) {
            (Some(t), Some(c)) => Some(t.at.max(c.at)),
            (t, c) => t.or(c).map(|m| m.at),
        }
    }

    /// Removed and not written to since: not listed.
    fn gone(&self) -> bool {
        self.removed && self.records.is_empty() && self.checkpoint.is_none()
    }

    fn last(&self) -> Address {
        self.records.back().map(|(at, _)| *at).or(self.low()).unwrap_or_default()
    }

    /// The number of the last record of `epoch`.
    fn length(&self, epoch: u64) -> u64 {
        let i = self.records.partition_point(|(at, _)| at.epoch <= epoch);
        match i.checked_sub(1).map(|i| self.records[i].0) {
            Some(at) if at.epoch == epoch => at.seq,
            _ => self.low().filter(|low| low.epoch == epoch).map_or(0, |low| low.seq),
        }
    }

    fn epochs(&self) -> Vec<(u64, u64)> {
        let mut epochs: Vec<(u64, u64)> =
            self.low().filter(|low| low.seq > 0).map(|low| (low.epoch, low.seq)).into_iter().collect();
        for (at, _) in &self.records {
            match epochs.last_mut() {
                Some((epoch, seq)) if *epoch == at.epoch => *seq = at.seq,
                _ => epochs.push((at.epoch, at.seq)),
            }
        }
        epochs
    }

    fn info(&self) -> KeyInfo {
        KeyInfo {
            fence: self.fence,
            epochs: self.epochs(),
            checkpoint: self.checkpoint.map(|c| c.at),
            trimmed: self.low(),
            live_bytes: self.live_bytes,
        }
    }
}

/// One segment file.
#[derive(Clone, Debug)]
struct Segment {
    first: u64,
    /// The length it is zero-filled up to, ahead of its frames; 0 for a segment that grows.
    size: u64,
    /// Its length on disk, durable.
    filled: u64,
    /// Where its next frame goes.
    end: u64,
    /// The LSN of its last frame, `first - 1` before one.
    last: u64,
    live: u64,
    /// The newest LSN that ended one of its frames: it is removed only by a write that holds that frame, or a later one.
    killed: u64,
    /// The live bytes of each key with a frame in it, live or not.
    keys: BTreeMap<Bytes, u64>,
    /// On disk.
    made: bool,
    /// Its bytes on disk up to here are durable.
    written: u64,
    /// Cut back to `written` by the next write to it: a torn tail, or a failed write to a file that grows.
    cut: bool,
    created_ms: u64,
}

impl Segment {
    fn holds(&self, loc: &Loc) -> bool {
        (self.first..=self.last).contains(&loc.lsn)
    }
}

/// A frame placed but not yet durable.
#[derive(Clone, Debug)]
struct Unwritten {
    lsn: u64,
    segment: u64,
    offset: u64,
    frame: Queued,
}

#[derive(Clone, Debug)]
enum Queued {
    Frame(Bytes),
    /// A live frame of an old segment copied forward, read by the write that takes it.
    Copy {
        from: u64,
        offset: u64,
        len: u32,
    },
}

impl Unwritten {
    fn len(&self) -> usize {
        match &self.frame {
            Queued::Frame(frame) => frame.len(),
            Queued::Copy { len, .. } => *len as usize,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Slot {
    Record(Address),
    Checkpoint,
    Trim,
    Fence,
}

/// A frame being copied forward: the index moves to `to` once it is durable, if `from` is still live.
#[derive(Clone, Debug)]
struct Copied {
    key: Bytes,
    slot: Slot,
    from: Loc,
    to: Loc,
}

/// The write out.
struct Pending {
    since_ms: u64,
    /// The last frame it holds, and its segment, end and length on disk after it.
    frames: Option<(u64, u64, u64, u64)>,
    removes: Vec<u64>,
    /// Segments it cuts back to what was written.
    cuts: Vec<u64>,
}

/// A frame as the index takes it.
struct Applied {
    kind: u8,
    at: Address,
    /// A remove frame's: the key's fence.
    fence: u64,
    loc: Loc,
}

/// The records of many keys in one directory, written by one owner: a store's write-ahead log, a replica's journal.
///
/// Each record has an [`Address`] in its key, and the log keeps a fence per key: a write from an epoch below it is
/// refused. [`append`](Self::append) never waits; the owner takes the next write with [`take_write`](Self::take_write),
/// in `on_turn_end`, runs it with `cx.submit_future` under `catch_panic`, and hands its result to
/// [`written`](Self::written), which moves [`durable`](Self::durable). A caller is answered once `durable()` reaches
/// the LSN of its change. One write is out at a time and takes everything queued since the last, so one sync covers
/// every key; there is no timer. The log has no task, no lock and reads no clock.
///
/// `checkpoint`, `trim` and `remove` let a key's records go, and a write removes segments with nothing live left, or
/// copies the live frames of a mostly dead old segment forward. On open the last segment's torn tail is cut; anything
/// else that does not read back is `InvalidData`, naming the file and offset.
pub struct Log {
    files: Arc<dyn Files>,
    cfg: LogConfig,
    /// Drawn when its first segment is made, in every segment's header.
    id: u64,
    keys: BTreeMap<Bytes, Key>,
    segments: BTreeMap<u64, Segment>,
    live: u64,
    next: u64,
    durable: u64,
    unwritten: VecDeque<Unwritten>,
    unwritten_bytes: usize,
    copies: Vec<Copied>,
    pending: Option<Pending>,
    /// A last segment whose header was torn, removed by the next write.
    discard: Option<String>,
    failing: Option<io::ErrorKind>,
    stats: LogStats,
}

impl Log {
    /// Reads every segment once and rebuilds the index. `InvalidInput` for a bad config, `InvalidData` for a log that
    /// does not read back, naming the file and offset.
    pub fn open(files: Arc<dyn Files>, cfg: LogConfig) -> FileFuture<Log> {
        Box::pin(async move {
            cfg.check()?;
            let mut names: Vec<(u64, String)> = files
                .list()
                .await?
                .into_iter()
                .filter_map(|(name, _)| Some((segment_first(&cfg.name, &name)?, name)))
                .collect();
            names.sort();
            let mut log = Log {
                files: files.clone(),
                cfg,
                id: 0,
                keys: BTreeMap::new(),
                segments: BTreeMap::new(),
                live: 0,
                next: 1,
                durable: 0,
                unwritten: VecDeque::new(),
                unwritten_bytes: 0,
                copies: Vec::new(),
                pending: None,
                discard: None,
                failing: None,
                stats: LogStats::default(),
            };
            // The last segment whose header reads is the one whose tail may be torn. After it, at most one file whose
            // header was torn as it was made: removed by the next write.
            let mut last = None;
            while let Some((first, name)) = names.pop() {
                let data = files.read(&name).await?;
                if Header::parse(&data).is_some() {
                    last = Some((first, name, data));
                    break;
                }
                let end = data_end(&data, 0).unwrap_or(0) as u64;
                if log.discard.is_some() || end > log.cfg.reach() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{name}: a bad segment header at offset 0"),
                    ));
                }
                log.stats.truncated_bytes += end;
                log.discard = Some(name);
            }
            for (first, name) in names {
                let data = files.read(&name).await?;
                log.load(first, name, &data, false)?;
            }
            if let Some((first, name, data)) = last {
                log.load(first, name, &data, true)?;
            }
            if log.id == 0 {
                // 0 is no log's: a header holding it does not read back.
                log.id = crate::trace::random_u64().max(1);
            }
            log.durable = log.next - 1;
            Ok(log)
        })
    }

    fn load(&mut self, first: u64, name: String, data: &[u8], last: bool) -> io::Result<()> {
        let corrupt = |offset: usize, what: &str| {
            io::Error::new(io::ErrorKind::InvalidData, format!("{name}: {what} at offset {offset}"))
        };
        let data_end = |from: usize| data_end(data, from);
        let Some(header) = Header::parse(data) else { return Err(corrupt(0, "a bad segment header")) };
        if header.first != first {
            return Err(corrupt(16, "the header of another segment"));
        }
        if self.id != 0 && header.id != self.id {
            return Err(corrupt(8, "a segment of another log"));
        }
        if first < self.next {
            return Err(corrupt(16, "a first LSN at or below the last segment's last"));
        }
        self.id = header.id;
        self.next = first;
        let segment = Segment {
            first,
            size: header.size,
            filled: data.len() as u64,
            end: HEADER,
            last: first - 1,
            live: 0,
            killed: 0,
            keys: BTreeMap::new(),
            made: true,
            written: HEADER,
            cut: false,
            created_ms: header.created_ms,
        };
        self.segments.insert(first, segment);
        let mut at = HEADER as usize;
        while let Some(frame) = data.get(at..).and_then(Parsed::read).filter(|f| f.lsn == self.next) {
            let loc = Loc { lsn: self.next, offset: at as u64, len: frame.len as u32 };
            let key = self.key_bytes(frame.key);
            if let Some(s) = self.segments.get_mut(&first) {
                s.end = (at + frame.len) as u64;
                s.last = loc.lsn;
            }
            self.next += 1;
            self.apply(key, Applied { kind: frame.kind, at: frame.at, fence: frame.fence(), loc });
            at += frame.len;
        }
        let torn = data_end(at);
        if let Some(end) = torn {
            if !last || (end - at) as u64 > self.cfg.reach() {
                return Err(corrupt(at, "a frame that does not read back, with data after it"));
            }
            self.stats.truncated_bytes += (end - at) as u64;
        }
        if let Some(s) = self.segments.get_mut(&first) {
            debug_assert_eq!(s.end, at as u64);
            s.written = s.end;
            // Zeros after the tail of a zero-filled segment are written over; a segment that grows is appended to.
            s.cut = last && if s.size > 0 { torn.is_some() } else { data.len() > at };
        }
        Ok(())
    }

    /// Appends a record at the next number of `epoch` in `key`, raising its fence to `epoch`. Never waits: the record
    /// is durable once [`durable`](Self::durable) reaches the LSN returned. `Fenced` below the key's fence.
    pub fn append(&mut self, key: &[u8], epoch: u64, body: Bytes) -> Result<Appended, LogError> {
        self.admit(key, body.len(), true)?;
        let k = self.keys.get(key);
        let fence = k.map_or(0, |k| k.fence);
        if epoch < fence {
            return Err(LogError::Fenced { fence });
        }
        let at = Address { epoch, seq: k.map_or(0, |k| k.length(epoch)) + 1 };
        Ok(Appended { lsn: Lsn(self.push(key, RECORD, at, &body, 0)), at })
    }

    /// A replica's write of a record another log appended: refused `Gap { have }` unless `at` is the next of its epoch,
    /// and `Fenced` below the key's fence.
    pub fn append_at(&mut self, key: &[u8], at: Address, body: Bytes) -> Result<Lsn, LogError> {
        self.admit(key, body.len(), true)?;
        let k = self.keys.get(key);
        let fence = k.map_or(0, |k| k.fence);
        if at.epoch < fence {
            return Err(LogError::Fenced { fence });
        }
        let have = k.map_or(0, |k| k.length(at.epoch));
        if at.seq != have + 1 {
            return Err(LogError::Gap { have });
        }
        Ok(Lsn(self.push(key, RECORD, at, &body, 0)))
    }

    /// Raises the key's fence to `epoch` at once: every later write from an older epoch is refused, and so is a fence
    /// at or below the key's fence, so two owners given one epoch cannot both fence. Returns each of the key's epochs
    /// with the number of its last record; the fence is durable, and a new owner may read, once `durable()` reaches
    /// the LSN.
    pub fn fence(&mut self, key: &[u8], epoch: u64) -> Result<(Lsn, Vec<(u64, u64)>), LogError> {
        self.admit(key, 0, false)?;
        let k = self.keys.get(key);
        let fence = k.map_or(0, |k| k.fence);
        if epoch <= fence {
            return Err(LogError::Fenced { fence });
        }
        let epochs = k.map(Key::epochs).unwrap_or_default();
        Ok((Lsn(self.push(key, FENCE, Address { epoch, seq: 0 }, &[], 0)), epochs))
    }

    /// `state` stands for every record of the key at or before `through`, and those records go. `Gap` past the key's
    /// last record; a position below its checkpoint or trim changes nothing, and returns the newest LSN.
    pub fn checkpoint(&mut self, key: &[u8], through: Address, state: Bytes) -> Result<Lsn, LogError> {
        self.admit(key, state.len(), false)?;
        let k = self.keys.get(key).filter(|k| !k.gone());
        if through > k.map(Key::last).unwrap_or_default() || k.is_none() {
            return Err(LogError::Gap { have: k.map_or(0, |k| k.length(through.epoch)) });
        }
        if k.and_then(Key::low).is_some_and(|low| through < low) {
            return Ok(self.newest());
        }
        Ok(Lsn(self.push(key, CHECKPOINT, through, &state, 0)))
    }

    /// The key's records at or before `through` go. `Gap` past its last record; a position at or below its
    /// checkpoint or trim changes nothing, and returns the newest LSN.
    pub fn trim(&mut self, key: &[u8], through: Address) -> Result<Lsn, LogError> {
        self.admit(key, 0, false)?;
        let k = self.keys.get(key).filter(|k| !k.gone());
        if through > k.map(Key::last).unwrap_or_default() {
            return Err(LogError::Gap { have: k.map_or(0, |k| k.length(through.epoch)) });
        }
        if k.is_none() || k.and_then(Key::low).is_some_and(|low| through <= low) {
            return Ok(self.newest());
        }
        Ok(Lsn(self.push(key, TRIM, through, &[], 0)))
    }

    /// The key's records, checkpoint and fence go. The log keeps its fence and numbers until the segments holding its
    /// frames are gone, so a key written again goes on from them. An unknown key changes nothing, and returns the
    /// newest LSN.
    pub fn remove(&mut self, key: &[u8]) -> Result<Lsn, LogError> {
        let Some(k) = self.keys.get(key).filter(|k| !k.gone()) else { return Ok(self.newest()) };
        let (at, fence) = (k.last(), k.fence);
        self.admit(key, 8, false)?;
        Ok(Lsn(self.push(key, REMOVE, at, &fence.to_le_bytes(), fence)))
    }

    fn newest(&self) -> Lsn {
        Lsn(self.next - 1)
    }

    fn admit(&self, key: &[u8], body: usize, record: bool) -> Result<(), LogError> {
        let len = FRAME + key.len() + body;
        if key.len() > MAX_KEY || body > self.cfg.max_record {
            Err(LogError::TooLarge)
        } else if self.unwritten_bytes + len > self.cfg.max_unwritten_bytes {
            Err(LogError::Behind)
        } else if !self.keys.contains_key(key) && self.keys.len() >= self.cfg.max_keys {
            Err(LogError::Keys)
        } else if record && self.live + len as u64 > self.cfg.max_bytes {
            Err(LogError::Full)
        } else {
            Ok(())
        }
    }

    fn key_bytes(&self, key: &[u8]) -> Bytes {
        self.keys.get_key_value(key).map_or_else(|| Bytes::copy_from_slice(key), |(k, _)| k.clone())
    }

    /// Frames, places and indexes a change.
    fn push(&mut self, key: &[u8], kind: u8, at: Address, body: &[u8], fence: u64) -> u64 {
        let frame = encode(self.next, kind, key, at, body);
        let (lsn, len) = (self.next, frame.len() as u64);
        self.next += 1;
        let (segment, offset) = match self.segments.iter_mut().next_back() {
            Some((&first, s)) if s.end + len <= if s.size > 0 { s.size } else { self.cfg.segment_bytes } => {
                s.end += len;
                s.last = lsn;
                (first, s.end - len)
            }
            _ => {
                let size = if self.cfg.preallocate { self.cfg.segment_bytes } else { 0 };
                let s = Segment {
                    first: lsn,
                    size,
                    filled: 0,
                    end: HEADER + len,
                    last: lsn,
                    live: 0,
                    killed: 0,
                    keys: BTreeMap::new(),
                    made: false,
                    written: 0,
                    cut: false,
                    created_ms: 0,
                };
                self.segments.insert(lsn, s);
                (lsn, HEADER)
            }
        };
        self.unwritten_bytes += frame.len();
        self.unwritten.push_back(Unwritten { lsn, segment, offset, frame: Queued::Frame(frame) });
        let key = self.key_bytes(key);
        self.apply(key, Applied { kind, at, fence, loc: Loc { lsn, offset, len: len as u32 } });
        lsn
    }

    /// Counts a frame of `key` live, or no longer live: ended by the frame at LSN `by`.
    fn mark(&mut self, key: &Bytes, k: &mut Key, loc: Loc, live: bool, by: u64) {
        let len = u64::from(loc.len);
        let Some((_, s)) = self.segments.range_mut(..=loc.lsn).next_back() else { return };
        let Some(bytes) = s.keys.get_mut(key) else { return };
        if live {
            (s.live, *bytes, self.live, k.live_bytes) =
                (s.live + len, *bytes + len, self.live + len, k.live_bytes + len);
        } else {
            debug_assert!(*bytes >= len && s.live >= len && k.live_bytes >= len, "a frame counted dead twice");
            s.live = s.live.saturating_sub(len);
            *bytes = bytes.saturating_sub(len);
            self.live = self.live.saturating_sub(len);
            k.live_bytes = k.live_bytes.saturating_sub(len);
            s.killed = s.killed.max(by);
        }
    }

    /// Takes a frame into the index, in LSN order: as it is appended, and on open.
    fn apply(&mut self, key: Bytes, f: Applied) {
        let mut k = self.keys.remove(&key).unwrap_or_default();
        let lsn = f.loc.lsn;
        if let Some((_, s)) = self.segments.range_mut(..=lsn).next_back()
            && !s.keys.contains_key(&key)
        {
            s.keys.insert(key.clone(), 0);
            k.present += 1;
        }
        k.fence = k.fence.max(f.at.epoch).max(f.fence);
        let fresh = Meta { at: f.at, loc: f.loc, live: true };
        match f.kind {
            RECORD if k.low().is_none_or(|low| f.at > low) => {
                match k.records.binary_search_by(|(at, _)| at.cmp(&f.at)) {
                    // A copy forward of a record the log still holds.
                    Ok(i) => {
                        let old = std::mem::replace(&mut k.records[i].1, f.loc);
                        self.mark(&key, &mut k, old, false, lsn);
                        self.mark(&key, &mut k, f.loc, true, 0);
                    }
                    // An append at the end; on open, before it, a copy whose segment went, after the key's later
                    // records.
                    Err(i) => {
                        k.records.insert(i, (f.at, f.loc));
                        self.mark(&key, &mut k, f.loc, true, 0);
                    }
                }
            }
            // At or above the trim as appended; on open, a copy may come after a later trim.
            CHECKPOINT if k.checkpoint.is_none_or(|c| f.at >= c.at) => {
                if let Some(old) = k.checkpoint.replace(fresh)
                    && old.live
                {
                    self.mark(&key, &mut k, old.loc, false, lsn);
                }
                self.mark(&key, &mut k, f.loc, true, 0);
                self.cut_below(&key, &mut k, f.at, lsn);
            }
            TRIM | REMOVE => {
                let remove = f.kind == REMOVE;
                let copy = k.trim.is_some_and(|t| t.at == f.at && !remove && !k.removed);
                let low = k.low();
                if copy || if remove { low.is_none_or(|low| f.at >= low) } else { low.is_none_or(|low| f.at > low) } {
                    if let Some(old) = k.trim.replace(Meta { live: !remove, ..fresh })
                        && old.live
                    {
                        self.mark(&key, &mut k, old.loc, false, lsn);
                    }
                    if !remove {
                        self.mark(&key, &mut k, f.loc, true, 0);
                    }
                    k.removed = remove;
                    if remove {
                        k.tomb = Some(f.loc);
                        for meta in [k.checkpoint.take(), k.fence_frame.take()].into_iter().flatten() {
                            if meta.live {
                                self.mark(&key, &mut k, meta.loc, false, lsn);
                            }
                        }
                    }
                    self.cut_below(&key, &mut k, f.at, lsn);
                }
            }
            FENCE if k.fence_frame.is_none_or(|m| f.at.epoch >= m.at.epoch) => {
                if let Some(old) = k.fence_frame.replace(fresh)
                    && old.live
                {
                    self.mark(&key, &mut k, old.loc, false, lsn);
                }
                self.mark(&key, &mut k, f.loc, true, 0);
            }
            _ => {}
        }
        self.keys.insert(key, k);
    }

    /// The records at or before `at` go, and a trim the checkpoint covers.
    fn cut_below(&mut self, key: &Bytes, k: &mut Key, at: Address, by: u64) {
        while let Some(&(record, loc)) = k.records.front()
            && record <= at
        {
            k.records.pop_front();
            self.mark(key, k, loc, false, by);
        }
        if let (Some(trim), Some(checkpoint)) = (k.trim, k.checkpoint)
            && checkpoint.at >= trim.at
        {
            if trim.live {
                self.mark(key, k, trim.loc, false, by);
            }
            (k.trim, k.removed) = (None, false);
        }
    }

    /// The next write, `None` while one is out or when there is nothing to write: every frame appended since the last
    /// write, up to `max_write_bytes` and within one segment, then the removal of the segments with nothing live left.
    /// Run it once, and hand its result to [`written`](Self::written). After a failure the same bytes go first again.
    /// `now_ms` is when it starts, as [`writing_since`](Self::writing_since) gives it back, and a new segment's
    /// created time.
    pub fn take_write(&mut self, now_ms: u64) -> Option<FileFuture<()>> {
        if self.pending.is_some() {
            return None;
        }
        self.plan_copies();
        let mut before: Vec<FileOp> = self.discard.iter().map(|name| FileOp::Remove(name.clone())).collect();
        let target = self.unwritten.front().map(|u| u.segment);
        // A torn tail cut on open in a segment that takes no more frames, as one after it does: cut on disk too, or the
        // next open reads it as data after a bad frame in a segment that is not the last.
        let last = self.segments.keys().next_back().copied();
        let mut cuts = Vec::new();
        for s in self.segments.values() {
            if s.cut && s.made && s.written == s.end && Some(s.first) != target && Some(s.first) != last {
                cuts.push(s.first);
                before.push(FileOp::Truncate(segment_name(&self.cfg.name, s.first), s.written));
            }
        }
        let (mut frames, mut taken, mut reads) = (None, None, Vec::new());
        if let Some(segment) = target
            && let Some(s) = self.segments.get_mut(&segment)
        {
            let name = segment_name(&self.cfg.name, segment);
            if s.created_ms == 0 {
                s.created_ms = now_ms.max(1);
            }
            let start = if s.made { s.written } else { 0 };
            let mut buf = BytesMut::new();
            if !s.made {
                buf.put_slice(&Header { id: self.id, first: segment, created_ms: s.created_ms, size: s.size }.bytes());
            }
            let (mut end, mut lsn) = (start, None::<u64>);
            for u in self.unwritten.iter().take_while(|u| u.segment == segment) {
                if end > start && buf.len() + u.len() > self.cfg.max_write_bytes + HEADER as usize {
                    break;
                }
                match &u.frame {
                    Queued::Frame(frame) => buf.put_slice(frame),
                    Queued::Copy { from, offset, len } => {
                        reads.push(CopyRead { at: buf.len(), from: *from, offset: *offset, len: *len, lsn: u.lsn });
                        buf.put_bytes(0, *len as usize);
                    }
                }
                (end, lsn) = (u.offset + u.len() as u64, Some(u.lsn));
            }
            // Of the bytes from `start`, those before `filled` are on disk and written over; the rest are appended, and
            // in a zero-filled segment they go on as zeros `fill_ahead` past the frames, so no write fills it whole.
            let filled = if s.cut { start } else { s.filled.max(start) };
            let to = if s.size > 0 && end > filled { s.size.min(end.saturating_add(self.cfg.fill_ahead)) } else { end };
            buf.resize((to - start) as usize, 0);
            if s.cut {
                before.push(FileOp::Truncate(name.clone(), start));
            }
            taken = lsn.map(|lsn| (lsn, segment, end, filled.max(to)));
            frames = Some((name, start, (filled.min(to) - start) as usize, buf));
        }
        let upto = taken.map_or(self.durable, |(lsn, ..)| lsn);
        let removes: Vec<u64> = self
            .segments
            .values()
            .filter(|s| Some(s.first) != last && self.removable(s, upto))
            .map(|s| s.first)
            .collect();
        let after: Vec<FileOp> = removes.iter().map(|r| FileOp::Remove(segment_name(&self.cfg.name, *r))).collect();
        if before.is_empty() && frames.is_none() && after.is_empty() {
            return None;
        }
        self.pending = Some(Pending { since_ms: now_ms, frames: taken, removes, cuts });
        let (files, log) = (self.files.clone(), self.cfg.name.clone());
        Some(Box::pin(async move {
            let mut ops = before;
            if let Some((name, start, over, mut buf)) = frames {
                copy_forward(&*files, &log, &mut buf, &reads).await?;
                let data = buf.freeze();
                if over > 0 {
                    ops.push(FileOp::Write(name.clone(), start, data.slice(..over)));
                }
                if over < data.len() {
                    ops.push(FileOp::Append(name, data.slice(over..)));
                }
            }
            ops.extend(after);
            files.apply(ops).await
        }))
    }

    /// Nothing live, every frame durable, no copy forward still to read from it, and nothing on disk that its remove
    /// frames still end.
    fn removable(&self, s: &Segment, upto: u64) -> bool {
        s.live == 0
            && s.made
            && s.written == s.end
            && s.killed <= upto
            && !self.copies.iter().any(|c| s.holds(&c.from))
            && s.keys.keys().all(|key| {
                let ends = self.keys.get(key).is_some_and(|k| k.tomb.is_some_and(|t| s.holds(&t)));
                !ends || !self.segments.range(..s.first).any(|(_, older)| older.keys.contains_key(key))
            })
    }

    /// Plans copying the live frames of the oldest segment to the head, when it is under half live and the log above
    /// half of `max_bytes`, up to half of `max_write_bytes`: only when what is queued goes in this write, so the copies
    /// follow it, and they fit in the last segment.
    fn plan_copies(&mut self) {
        if !self.copies.is_empty() || self.bytes() * 2 <= self.cfg.max_bytes {
            return;
        }
        let budget = (self.cfg.max_write_bytes / 2) as u64;
        let (Some((&oldest, o)), Some((&head, h))) = (self.segments.iter().next(), self.segments.iter().next_back())
        else {
            return;
        };
        let room = if h.size > 0 { h.size } else { self.cfg.segment_bytes }.saturating_sub(h.end);
        if oldest == head
            || !o.made
            || o.written != o.end
            || o.live * 2 >= o.end
            || self.unwritten_bytes as u64 > budget
            || self.unwritten.iter().any(|u| u.segment != head)
        {
            return;
        }
        let mut live = Vec::new();
        for (key, _) in o.keys.iter().filter(|(_, bytes)| **bytes > 0) {
            let Some(k) = self.keys.get(key) else { continue };
            for (at, loc) in k.records.iter().filter(|(_, loc)| o.holds(loc)) {
                live.push((*loc, key.clone(), Slot::Record(*at)));
            }
            for (slot, meta) in [(Slot::Checkpoint, k.checkpoint), (Slot::Trim, k.trim), (Slot::Fence, k.fence_frame)] {
                if let Some(meta) = meta.filter(|m| m.live && o.holds(&m.loc)) {
                    live.push((meta.loc, key.clone(), slot));
                }
            }
        }
        live.sort_by_key(|(loc, ..)| loc.lsn);
        let mut used = 0;
        for (from, key, slot) in live {
            let len = u64::from(from.len);
            if used + len > budget.min(room) {
                break;
            }
            used += len;
            let lsn = self.next;
            self.next += 1;
            let Some(h) = self.segments.get_mut(&head) else { return };
            let offset = h.end;
            (h.end, h.last) = (h.end + len, lsn);
            if !h.keys.contains_key(&key) {
                h.keys.insert(key.clone(), 0);
                if let Some(k) = self.keys.get_mut(&key) {
                    k.present += 1;
                }
            }
            let frame = Queued::Copy { from: oldest, offset: from.offset, len: from.len };
            self.unwritten.push_back(Unwritten { lsn, segment: head, offset, frame });
            self.unwritten_bytes += len as usize;
            self.copies.push(Copied { key, slot, from, to: Loc { lsn, offset, len: from.len } });
        }
    }

    /// The result of the write [`take_write`](Self::take_write) gave, and the new [`durable`](Self::durable). On an
    /// error it sets [`failing`](Self::failing), and the same frames go again in the next write.
    pub fn written(&mut self, result: std::io::Result<()>) -> Lsn {
        let Some(pending) = self.pending.take() else { return Lsn(self.durable) };
        match result {
            Ok(()) => {
                (self.failing, self.stats.writes) = (None, self.stats.writes + 1);
                self.discard = None;
                for first in pending.cuts {
                    if let Some(s) = self.segments.get_mut(&first) {
                        (s.cut, s.filled) = (false, s.written);
                    }
                }
                if let Some((last, segment, end, filled)) = pending.frames {
                    if let Some(s) = self.segments.get_mut(&segment) {
                        (s.made, s.cut, s.written, s.filled) = (true, false, end, filled);
                    }
                    while let Some(u) = self.unwritten.front()
                        && u.lsn <= last
                    {
                        self.unwritten_bytes -= u.len();
                        self.unwritten.pop_front();
                    }
                    self.durable = last;
                }
                let durable = self.durable;
                let (done, copies): (Vec<_>, Vec<_>) = self.copies.drain(..).partition(|c| c.to.lsn <= durable);
                self.copies = copies;
                for copy in done {
                    self.copied(copy);
                }
                for first in pending.removes {
                    self.drop_segment(first);
                }
            }
            Err(e) => {
                (self.failing, self.stats.failed_writes) = (Some(e.kind()), self.stats.failed_writes + 1);
                // A file the write appended to is cut back to where it began, so the next does not append it twice.
                if let Some((_, segment, _, filled)) = pending.frames
                    && let Some(s) = self.segments.get_mut(&segment)
                    && filled > s.filled
                {
                    s.cut = true;
                }
            }
        }
        Lsn(self.durable)
    }

    fn copied(&mut self, copy: Copied) {
        let Some(mut k) = self.keys.remove(&copy.key) else { return };
        let moved = |meta: &mut Option<Meta>| match meta {
            Some(m) if m.live && m.loc == copy.from => {
                m.loc = copy.to;
                true
            }
            _ => false,
        };
        let moved = match copy.slot {
            Slot::Record(at) => match k.records.binary_search_by(|(a, _)| a.cmp(&at)) {
                Ok(i) if k.records[i].1 == copy.from => {
                    k.records[i].1 = copy.to;
                    true
                }
                _ => false,
            },
            Slot::Checkpoint => moved(&mut k.checkpoint),
            Slot::Trim => moved(&mut k.trim),
            Slot::Fence => moved(&mut k.fence_frame),
        };
        if moved {
            self.stats.copied_bytes += u64::from(copy.to.len);
            self.mark(&copy.key, &mut k, copy.from, false, copy.to.lsn);
            self.mark(&copy.key, &mut k, copy.to, true, 0);
        }
        self.keys.insert(copy.key, k);
    }

    fn drop_segment(&mut self, first: u64) {
        let Some(mut s) = self.segments.remove(&first) else { return };
        for key in std::mem::take(&mut s.keys).into_keys() {
            let Some(mut k) = self.keys.remove(&key) else { continue };
            k.present = k.present.saturating_sub(1);
            // Only a remove frame, never live, can be in a segment that goes.
            if k.trim.is_some_and(|t| s.holds(&t.loc)) {
                (k.trim, k.removed) = (None, false);
            }
            if k.tomb.is_some_and(|t| s.holds(&t)) {
                k.tomb = None;
            }
            if k.present > 0 {
                self.keys.insert(key, k);
            }
        }
    }

    /// Every frame at or below it is on disk and synced. It never goes back.
    pub fn durable(&self) -> Lsn {
        Lsn(self.durable)
    }

    /// The last write failed, and how: the next goes again, and clears it once one lands.
    pub fn failing(&self) -> Option<io::ErrorKind> {
        self.failing
    }

    /// When the write out began, by the `now_ms` given to [`take_write`](Self::take_write): the owner's measure of a
    /// write that hangs, which it may then answer its callers without, or refuse them. The log answers no one.
    pub fn writing_since(&self) -> Option<u64> {
        self.pending.as_ref().map(|p| p.since_ms)
    }

    /// What the log holds of `key`, `None` for an unknown or removed one.
    pub fn key(&self, key: &[u8]) -> Option<KeyInfo> {
        self.keys.get(key).filter(|k| !k.gone()).map(Key::info)
    }

    /// Up to `limit` keys from `from` on, in key order.
    pub fn keys(&self, from: Option<&[u8]>, limit: usize) -> Vec<(Bytes, KeyInfo)> {
        let from = from.map_or(Bound::Unbounded, Bound::Included);
        self.keys
            .range::<[u8], _>((from, Bound::Unbounded))
            .filter(|(_, k)| !k.gone())
            .take(limit)
            .map(|(key, k)| (key.clone(), k.info()))
            .collect()
    }

    /// The key's durable entries from `from` on, in address order: its checkpoint first when `from` is at or before
    /// it, then records, until `max_bytes` (one entry at least) or the first not durable. Reads a segment's bytes
    /// once, and only the key's frames and the gaps under 64 KiB between them. `NotFound` when a write removed a
    /// segment meanwhile: read again. `InvalidData` for a frame that does not read back.
    pub fn read(&self, key: &[u8], from: Address, max_bytes: usize) -> FileFuture<Vec<Entry>> {
        // Each entry's segment, place, kind and address.
        let mut plan: Vec<(u64, Loc, u8, Address)> = Vec::new();
        if let Some(k) = self.keys.get(key) {
            let mut used = 0usize;
            let segment = |loc: &Loc| self.segments.range(..=loc.lsn).next_back().map_or(0, |(first, _)| *first);
            if let Some(c) = k.checkpoint.filter(|c| from <= c.at) {
                if c.loc.lsn > self.durable {
                    return Box::pin(std::future::ready(Ok(Vec::new())));
                }
                plan.push((segment(&c.loc), c.loc, CHECKPOINT, c.at));
                used += c.loc.len as usize;
            }
            let start = k.records.partition_point(|(at, _)| *at < from);
            for (at, loc) in k.records.range(start..) {
                if loc.lsn > self.durable || !plan.is_empty() && used + loc.len as usize > max_bytes {
                    break;
                }
                plan.push((segment(loc), *loc, RECORD, *at));
                used += loc.len as usize;
            }
        }
        let (files, key, log) = (self.files.clone(), Bytes::copy_from_slice(key), self.cfg.name.clone());
        Box::pin(async move {
            let mut entries = Vec::with_capacity(plan.len());
            let mut i = 0;
            while i < plan.len() {
                let (segment, start, end, j) = read_run(&plan, i, |(segment, loc, ..)| (*segment, loc.offset, loc.len));
                let name = segment_name(&log, segment);
                let data = Bytes::from(files.read_range(&name, start, (end - start) as usize).await?);
                for (_, loc, kind, at) in &plan[i..j] {
                    let offset = (loc.offset - start) as usize;
                    let frame = data
                        .get(offset..)
                        .and_then(Parsed::read)
                        .filter(|f| f.len == loc.len as usize && f.lsn == loc.lsn && f.kind == *kind)
                        .filter(|f| f.key == key && f.at == *at)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("{name}: a frame that does not read back at offset {}", loc.offset),
                            )
                        })?;
                    let body = data.slice_ref(frame.body);
                    entries.push(match *kind {
                        CHECKPOINT => Entry::Checkpoint { through: *at, state: body },
                        _ => Entry::Record { at: *at, body },
                    });
                }
                i = j;
            }
            Ok(entries)
        })
    }

    /// Up to `limit` keys whose live frames hold the oldest segments, oldest segment first and the most bytes first in
    /// each, with their last position and their live bytes there: checkpoint them, so those segments can go.
    pub fn pinning(&self, limit: usize) -> Vec<(Bytes, Address, u64)> {
        let mut pinning: Vec<(Bytes, Address, u64)> = Vec::new();
        let head = self.segments.keys().next_back().copied();
        for s in self.segments.values().filter(|s| Some(s.first) != head) {
            let mut here: Vec<(&Bytes, u64)> = s
                .keys
                .iter()
                .filter(|(key, bytes)| **bytes > 0 && !pinning.iter().any(|(k, ..)| k == *key))
                .map(|(key, bytes)| (key, *bytes))
                .collect();
            here.sort_by_key(|(_, bytes)| std::cmp::Reverse(*bytes));
            for (key, bytes) in here {
                if pinning.len() >= limit {
                    return pinning;
                }
                pinning.push((key.clone(), self.keys.get(key).map(Key::last).unwrap_or_default(), bytes));
            }
        }
        pinning
    }

    fn bytes(&self) -> u64 {
        self.segments.values().map(|s| s.filled.max(s.end)).sum()
    }

    pub fn stats(&self) -> LogStats {
        LogStats {
            durable: Lsn(self.durable),
            unwritten_bytes: self.unwritten_bytes,
            bytes: self.bytes(),
            live_bytes: self.live,
            segments: self.segments.len(),
            keys: self.keys.len(),
            ..self.stats
        }
    }
}

struct CopyRead {
    /// Where the copy goes in the write's bytes.
    at: usize,
    /// The segment it is read from.
    from: u64,
    offset: u64,
    len: u32,
    lsn: u64,
}

/// The frames from `items[i]` on that one read covers: of one segment, in offset order, each less than `READ_GAP`
/// after the last. Their segment, the read's start and end offsets, and the index after them.
fn read_run<T>(items: &[T], i: usize, place: impl Fn(&T) -> (u64, u64, u32)) -> (u64, u64, u64, usize) {
    let (segment, start, len) = place(&items[i]);
    let (mut end, mut j) = (start + u64::from(len), i + 1);
    while let Some((next, offset, len)) = items.get(j).map(&place)
        && next == segment
        && offset >= end
        && offset - end <= READ_GAP
    {
        (end, j) = (offset + u64::from(len), j + 1);
    }
    (segment, start, end, j)
}

/// Reads the frames a write copies forward, each run of a segment in one read, and gives each its new LSN.
async fn copy_forward(files: &dyn Files, log: &str, buf: &mut BytesMut, reads: &[CopyRead]) -> io::Result<()> {
    let mut i = 0;
    while i < reads.len() {
        let (segment, start, end, j) = read_run(reads, i, |r| (r.from, r.offset, r.len));
        let source = segment_name(log, segment);
        let data = files.read_range(&source, start, (end - start) as usize).await?;
        for read in &reads[i..j] {
            let from = (read.offset - start) as usize;
            let len = read.len as usize;
            let frame = data.get(from..).and_then(Parsed::read).filter(|f| f.len == len).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{source}: a frame to copy that does not read back at offset {}", read.offset),
                )
            })?;
            let to = &mut buf[read.at..read.at + frame.len];
            to.copy_from_slice(&data[from..from + len]);
            to[8..16].copy_from_slice(&read.lsn.to_le_bytes());
            let crc = crc32(&to[8..]);
            to[4..8].copy_from_slice(&crc.to_le_bytes());
        }
        i = j;
    }
    Ok(())
}

/// Where the last non-zero byte after `from` ends.
fn data_end(data: &[u8], from: usize) -> Option<usize> {
    data.get(from..).and_then(|d| d.iter().rposition(|b| *b != 0)).map(|p| from + p + 1)
}

fn segment_name(name: &str, first: u64) -> String {
    format!("{name}.{first:020}")
}

fn segment_first(name: &str, file: &str) -> Option<u64> {
    let digits = file.strip_prefix(name)?.strip_prefix('.')?;
    (digits.len() == 20 && digits.bytes().all(|b| b.is_ascii_digit())).then(|| digits.parse().ok()).flatten()
}

struct Header {
    id: u64,
    first: u64,
    created_ms: u64,
    size: u64,
}

impl Header {
    fn bytes(&self) -> [u8; HEADER as usize] {
        let mut h = [0u8; HEADER as usize];
        h[0..4].copy_from_slice(MAGIC);
        h[4..6].copy_from_slice(&VERSION.to_le_bytes());
        h[6..8].copy_from_slice(&if self.size > 0 { ZERO_FILLED } else { 0 }.to_le_bytes());
        h[8..16].copy_from_slice(&self.id.to_le_bytes());
        h[16..24].copy_from_slice(&self.first.to_le_bytes());
        h[24..32].copy_from_slice(&self.created_ms.to_le_bytes());
        h[32..40].copy_from_slice(&self.size.to_le_bytes());
        let crc = crc32(&h[..40]);
        h[40..44].copy_from_slice(&crc.to_le_bytes());
        h
    }

    fn parse(data: &[u8]) -> Option<Header> {
        let h = data.get(..HEADER as usize)?;
        let flags = u16::from_le_bytes(h[6..8].try_into().ok()?);
        let header = Header { id: le64(h, 8)?, first: le64(h, 16)?, created_ms: le64(h, 24)?, size: le64(h, 32)? };
        let ok = &h[0..4] == MAGIC
            && u16::from_le_bytes(h[4..6].try_into().ok()?) == VERSION
            && u32::from_le_bytes(h[40..44].try_into().ok()?) == crc32(&h[..40])
            && flags == if header.size > 0 { ZERO_FILLED } else { 0 }
            && (header.size == 0 || header.size >= HEADER)
            && header.id != 0;
        ok.then_some(header)
    }
}

fn le64(data: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(data.get(at..at + 8)?.try_into().ok()?))
}

fn encode(lsn: u64, kind: u8, key: &[u8], at: Address, body: &[u8]) -> Bytes {
    let mut f = BytesMut::with_capacity(FRAME + key.len() + body.len());
    f.put_u32_le((AFTER_CRC + key.len() + body.len()) as u32);
    f.put_u32_le(0);
    f.put_u64_le(lsn);
    f.put_u8(kind);
    f.put_u16_le(key.len() as u16);
    f.put_u64_le(at.epoch);
    f.put_u64_le(at.seq);
    f.put_slice(key);
    f.put_slice(body);
    let crc = crc32(&f[8..]);
    f[4..8].copy_from_slice(&crc.to_le_bytes());
    f.freeze()
}

/// A frame that reads back: its length in range, its CRC, a known kind and the body that kind has.
struct Parsed<'a> {
    len: usize,
    lsn: u64,
    kind: u8,
    key: &'a [u8],
    at: Address,
    body: &'a [u8],
}

impl<'a> Parsed<'a> {
    fn read(data: &'a [u8]) -> Option<Self> {
        let n = u32::from_le_bytes(data.get(0..4)?.try_into().ok()?) as usize;
        let rest = data.get(8..8usize.checked_add(n)?).filter(|_| n >= AFTER_CRC)?;
        if u32::from_le_bytes(data[4..8].try_into().ok()?) != crc32(rest) {
            return None;
        }
        let (kind, key_len) = (rest[8], usize::from(u16::from_le_bytes(rest[9..11].try_into().ok()?)));
        let at = Address { epoch: le64(rest, 11)?, seq: le64(rest, 19)? };
        let key = rest.get(AFTER_CRC..AFTER_CRC + key_len)?;
        let body = &rest[AFTER_CRC + key_len..];
        let known = match kind {
            RECORD | CHECKPOINT => true,
            FENCE => at.seq == 0 && body.is_empty(),
            TRIM => body.is_empty(),
            REMOVE => body.len() == 8,
            _ => false,
        };
        known.then_some(Parsed { len: 8 + n, lsn: le64(rest, 0)?, kind, key, at, body })
    }

    fn fence(&self) -> u64 {
        if self.kind == REMOVE { le64(self.body, 0).unwrap_or(0) } else { 0 }
    }
}
