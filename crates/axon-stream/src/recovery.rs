//! Positions, batches and the storage contract behind replay. Nothing here is durable: the
//! one adapter, [`MemoryStore`], is a bounded ring.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::{BuildHasher, Hash, Hasher, RandomState};
use std::sync::{Arc, Mutex, OnceLock};

use axon::IdHash;
use bytes::Bytes;

use crate::hub::{DeliveryPolicy, NamespaceLimits, StreamError};

pub type Payload = Bytes;

/// Hashed once, when made, with a key drawn per process: a record looks its stream up in several maps on every hop,
/// and those take this hash as it is, while a peer naming streams still cannot choose their collisions.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamKey {
    namespace: Arc<str>,
    name: Arc<str>,
    /// Last, so the order is by name: equal names hash alike, and ordered maps iterate the same in every process.
    hash: u64,
}

impl StreamKey {
    pub fn new(namespace: impl Into<Arc<str>>, name: impl Into<Arc<str>>) -> Self {
        static KEY: OnceLock<RandomState> = OnceLock::new();
        let (namespace, name) = (namespace.into(), name.into());
        #[expect(clippy::disallowed_methods, reason = "never iterated; only equal keys must hash alike")]
        let hash = KEY.get_or_init(RandomState::new).hash_one((&namespace, &name));
        Self { namespace, name, hash }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn valid(&self, max_bytes: usize) -> bool {
        !self.namespace.is_empty() && !self.name.is_empty() && self.namespace.len() + self.name.len() <= max_bytes
    }
}

impl Hash for StreamKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl std::fmt::Debug for StreamKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamKey").field("namespace", &self.namespace).field("name", &self.name).finish()
    }
}

impl std::fmt::Display for StreamKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

/// The inclusive last record seen. `generation` names a sequence domain, not an authority;
/// sequence 0 is the empty prefix.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Cursor {
    pub generation: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryError {
    UnknownStream,
    GenerationMismatch,
    HistoryUnavailable,
    InvalidBatch,
    Gap { expected: u64, actual: u64 },
    ConflictingDuplicate,
    PositionOverflow,
    LimitExceeded,
    InvalidSnapshot,
    Adapter(String),
}

/// A non-empty run of records with their origin positions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequencedBatch {
    generation: u64,
    first: u64,
    records: Arc<[Payload]>,
    bytes: usize,
    trace: Option<(u64, u64)>,
}

impl SequencedBatch {
    pub fn new(generation: u64, first: u64, records: impl Into<Arc<[Payload]>>) -> Result<Self, RecoveryError> {
        let records = records.into();
        if first == 0 || records.is_empty() {
            return Err(RecoveryError::InvalidBatch);
        }
        // The last position there is stays unused, so the one after any record always exists.
        first.checked_add(records.len() as u64).ok_or(RecoveryError::PositionOverflow)?;
        let bytes = records.iter().map(Bytes::len).sum();
        Ok(Self { generation, first, records, bytes, trace: None })
    }

    /// The trace this batch was published in and the span that caused it. Live delivery carries
    /// it; a replay from history does not.
    pub fn trace(&self) -> Option<(u64, u64)> {
        self.trace
    }

    /// Enter it to continue the trace from what a subscriber does with the batch.
    #[cfg(feature = "cluster")]
    pub fn trace_context(&self) -> Option<axon::TraceContext> {
        let (trace, span) = self.trace?;
        Some(axon::TraceContext { trace: trace.try_into().ok()?, span })
    }

    pub fn traced(self, trace: Option<(u64, u64)>) -> Self {
        Self { trace, ..self }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn first(&self) -> u64 {
        self.first
    }

    pub fn last(&self) -> u64 {
        self.first + self.records.len() as u64 - 1
    }

    pub fn cursor(&self) -> Cursor {
        Cursor { generation: self.generation, sequence: self.last() }
    }

    pub fn records(&self) -> &[Payload] {
        &self.records
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Drops the prefix at or before `sequence` without renumbering the rest: the one dedup primitive.
    pub fn after(self, sequence: u64) -> Option<Self> {
        if sequence < self.first {
            return Some(self);
        }
        if sequence >= self.last() {
            return None;
        }
        let records: Arc<[Payload]> = self.records[(sequence + 1 - self.first) as usize..].into();
        Some(Self { bytes: records.iter().map(Bytes::len).sum(), records, first: sequence + 1, ..self })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadBounds {
    pub max_records: usize,
    pub max_bytes: usize,
}

/// What a source still holds. Empty history has `first == last + 1`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayRange {
    pub generation: u64,
    pub first: u64,
    pub last: u64,
}

impl ReplayRange {
    /// Whether every record in `(after, through]` is held.
    pub fn covers(&self, after: u64, through: u64) -> bool {
        after == through || (after < through && after + 1 >= self.first && through <= self.last)
    }
}

/// The state of a compacted stream: the latest record of each key, in key order, through a position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    through: Cursor,
    records: Arc<[Payload]>,
    bytes: usize,
}

impl Snapshot {
    pub fn new(through: Cursor, records: impl Into<Arc<[Payload]>>) -> Self {
        let records = records.into();
        let bytes = records.iter().map(|r| 4 + r.len()).sum();
        Self { through, records, bytes }
    }

    /// The stream's own records follow it, from `through.sequence + 1`.
    pub fn through(&self) -> Cursor {
        self.through
    }

    pub fn records(&self) -> &[Payload] {
        &self.records
    }

    /// As it travels: each record and its 4-byte length, so a push sizes it as a batch of no records.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

/// A record of a compacted stream: its key, then its value. An empty value removes the key.
///
/// # Panics
///
/// On a key longer than 65535 bytes.
pub fn keyed(key: &[u8], value: &[u8]) -> Payload {
    let len = u16::try_from(key.len()).expect("a key of at most 65535 bytes");
    [&len.to_be_bytes()[..], key, value].concat().into()
}

/// The key and value of a [`keyed`] record, or `None` when it is not one.
pub fn split_keyed(record: &Payload) -> Option<(Payload, Payload)> {
    let len = u16::from_be_bytes(record.get(..2)?.try_into().ok()?) as usize;
    (record.len() >= 2 + len).then(|| (record.slice(2..2 + len), record.slice(2 + len..)))
}

/// A compacted stream's bounds, in its [`DeliveryPolicy::Compacted`](crate::DeliveryPolicy::Compacted).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactLimits {
    pub max_keys: usize,
    /// Summed over the latest records, each counted whole: key, value and the 2-byte key length.
    pub max_bytes: usize,
}

/// The limits of a [`DeliveryPolicy::Compacted`](crate::DeliveryPolicy::Compacted), `None` for any other policy.
pub(crate) fn compacted(policy: DeliveryPolicy) -> Option<CompactLimits> {
    match policy {
        DeliveryPolicy::Compacted(limits) => Some(limits),
        _ => None,
    }
}

/// A compacted stream's state at its origin: the latest record of each key.
pub(crate) struct MemoryState {
    limits: CompactLimits,
    latest: BTreeMap<Payload, Payload>,
    bytes: usize,
}

impl MemoryState {
    pub(crate) fn new(limits: CompactLimits) -> Self {
        Self { limits, latest: BTreeMap::new(), bytes: 0 }
    }

    pub(crate) fn limits(&self) -> CompactLimits {
        self.limits
    }

    /// All of a batch or none of it: a malformed record is `InvalidBatch`, and a batch that would leave the state past
    /// its limits is `StateFull`. Then `admit` is asked, with the bytes the state would hold, and a refusal applies none.
    ///
    /// Out of line and not generic: inlined into `publish_admitted`, in the caller's crate, `BTreeMap::insert` stopped
    /// being inlined and a compacted publish cost 9 to 15 % more (fanout's `publish compacted` row), twice.
    #[inline(never)]
    pub(crate) fn apply(
        &mut self,
        records: &[Payload],
        admit: &mut dyn FnMut(usize) -> Result<(), StreamError>,
    ) -> Result<(), StreamError> {
        let mut changes = BTreeMap::new();
        for record in records {
            let (key, value) = split_keyed(record).ok_or(RecoveryError::InvalidBatch)?;
            changes.insert(key, (!value.is_empty()).then_some(record));
        }
        let (mut keys, mut bytes) = (self.latest.len(), self.bytes);
        for (key, new) in &changes {
            let old = self.latest.get(key);
            keys = keys + usize::from(new.is_some()) - usize::from(old.is_some());
            bytes = bytes + new.map_or(0, |r| r.len()) - old.map_or(0, Payload::len);
        }
        if keys > self.limits.max_keys || bytes > self.limits.max_bytes {
            return Err(StreamError::StateFull);
        }
        admit(bytes)?;
        for (key, new) in changes {
            match new {
                Some(record) => self.latest.insert(key, record.clone()),
                None => self.latest.remove(&key),
            };
        }
        self.bytes = bytes;
        Ok(())
    }

    /// The latest record of each key, in key order.
    pub(crate) fn latest(&self) -> Vec<Payload> {
        self.latest.values().cloned().collect()
    }

    /// The bytes of those records.
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}

/// History an application already keeps. Calls are synchronous and bounded; slow storage
/// belongs behind a cache.
pub trait RecoverySource: Send + Sync {
    fn available_range(&self, key: &StreamKey) -> Result<ReplayRange, RecoveryError>;

    /// Contiguous records strictly after `after`, none later than `through`, within `bounds`.
    /// Empty output short of `through` means the history is gone.
    fn read_after(
        &self,
        key: &StreamKey,
        after: Cursor,
        through: u64,
        bounds: ReadBounds,
    ) -> Result<Vec<SequencedBatch>, RecoveryError>;
}

#[derive(Clone, Copy, Debug)]
pub struct MemoryLimits {
    pub max_streams: usize,
    pub max_records_per_stream: usize,
    pub max_bytes_per_stream: usize,
    pub max_key_bytes: usize,
}

impl Default for MemoryLimits {
    fn default() -> Self {
        Self { max_streams: 1024, max_records_per_stream: 16_384, max_bytes_per_stream: 16 << 20, max_key_bytes: 4096 }
    }
}

/// The ring of every stream in one namespace, in place of the store-wide bounds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RingLimits {
    pub namespace: String,
    pub max_records: usize,
    pub max_bytes: usize,
}

struct Log {
    max_records: usize,
    max_bytes: usize,
    generation: u64,
    /// Sequence the next appended record gets.
    next: u64,
    records: VecDeque<Payload>,
    bytes: usize,
    /// Opened with `open`, an origin's: held to its namespace's `ring_bytes`, with its compacted state.
    budgeted: bool,
    /// The bytes of the stream's compacted state at this origin.
    state: usize,
}

impl Log {
    fn first(&self) -> u64 {
        self.next - self.records.len() as u64
    }
}

/// A volatile bounded ring per stream, evicting from the front.
pub struct MemoryStore {
    limits: MemoryLimits,
    rings: Vec<RingLimits>,
    /// Per namespace with `ring_bytes`: the most, and the bytes its budgeted rings hold.
    budgets: BTreeMap<String, (usize, usize)>,
    logs: HashMap<StreamKey, Log, IdHash>,
    total: usize,
}

impl MemoryStore {
    pub fn new(limits: MemoryLimits) -> Result<Self, RecoveryError> {
        let l = &limits;
        let all = [l.max_streams, l.max_records_per_stream, l.max_bytes_per_stream, l.max_key_bytes];
        if all.contains(&0) {
            return Err(RecoveryError::LimitExceeded);
        }
        Ok(Self { limits, rings: Vec::new(), budgets: BTreeMap::new(), logs: HashMap::default(), total: 0 })
    }

    /// Bounds the rings of some namespaces on their own. A batch larger than its ring keeps its tail.
    pub fn with_rings(mut self, rings: Vec<RingLimits>) -> Self {
        self.rings = rings;
        self
    }

    /// Holds the rings opened with `open` of each namespace with `ring_bytes` to it together: a ring that grows past it
    /// gives up its own oldest records, never another's.
    pub(crate) fn with_namespaces(mut self, limits: &BTreeMap<String, NamespaceLimits>) -> Self {
        self.budgets = limits.iter().filter_map(|(ns, l)| Some((ns.clone(), (l.ring_bytes?, 0)))).collect();
        self
    }

    /// The most bytes the key's ring and compacted state may hold together without another of its namespace giving
    /// any up; `None` without a budget.
    pub(crate) fn ring_room(&self, key: &StreamKey) -> Option<usize> {
        let (max, used) = self.budgets.get(key.namespace())?;
        let log = self.logs.get(key).filter(|log| log.budgeted)?;
        Some(max - (used - log.bytes - log.state))
    }

    /// Counts the bytes of the key's compacted state toward its namespace's `ring_bytes`, beside its ring, which gives
    /// up its oldest records to make room.
    pub(crate) fn hold_state(&mut self, key: &StreamKey, bytes: usize) {
        let room = self.ring_room(key).unwrap_or(usize::MAX);
        let Some(log) = self.logs.get_mut(key) else { return };
        let (held, state) = (log.bytes, std::mem::replace(&mut log.state, bytes));
        while log.bytes > room.saturating_sub(bytes) {
            log.bytes -= log.records.pop_front().map_or(0, |r| r.len());
        }
        let (after, budgeted) = (log.bytes, log.budgeted);
        if budgeted && let Some((_, used)) = self.budgets.get_mut(key.namespace()) {
            *used = *used + bytes - state;
        }
        self.charge(key, budgeted, held, after);
    }

    fn charge(&mut self, key: &StreamKey, budgeted: bool, before: usize, after: usize) {
        self.total = self.total + after - before;
        if budgeted && let Some((_, used)) = self.budgets.get_mut(key.namespace()) {
            *used = *used + after - before;
        }
    }

    /// Record payload held across all rings.
    pub fn total_bytes(&self) -> usize {
        self.total
    }

    pub fn open(&mut self, key: &StreamKey, generation: u64) -> Result<(), RecoveryError> {
        self.open_origin(key, Cursor { generation, sequence: 0 })
    }

    /// An origin's history, held to its namespace's budget as [`open`](Self::open)'s, that goes on after `after`: one
    /// resumed from disk.
    pub(crate) fn open_origin(&mut self, key: &StreamKey, after: Cursor) -> Result<(), RecoveryError> {
        self.open_at(key, after)?;
        self.logs.get_mut(key).expect("opened above").budgeted = true;
        Ok(())
    }

    /// Starts a history whose first record will be `after.sequence + 1`: a relay joining mid-stream.
    pub fn open_at(&mut self, key: &StreamKey, after: Cursor) -> Result<(), RecoveryError> {
        if !key.valid(self.limits.max_key_bytes) {
            return Err(RecoveryError::InvalidBatch);
        }
        if self.logs.contains_key(key) {
            return Err(RecoveryError::ConflictingDuplicate);
        }
        if self.logs.len() >= self.limits.max_streams {
            return Err(RecoveryError::LimitExceeded);
        }
        let next = after.sequence.checked_add(1).ok_or(RecoveryError::PositionOverflow)?;
        let (max_records, max_bytes) = self.ring_limits(key);
        let log = Log {
            max_records,
            max_bytes,
            generation: after.generation,
            next,
            records: VecDeque::new(),
            bytes: 0,
            budgeted: false,
            state: 0,
        };
        self.logs.insert(key.clone(), log);
        Ok(())
    }

    /// The records and bytes the key's ring holds at most.
    pub(crate) fn ring_limits(&self, key: &StreamKey) -> (usize, usize) {
        let ring = self.rings.iter().find(|r| r.namespace == key.namespace());
        ring.map_or((self.limits.max_records_per_stream, self.limits.max_bytes_per_stream), |r| {
            (r.max_records, r.max_bytes)
        })
    }

    /// Discards the whole prior sequence domain.
    pub fn restart(&mut self, key: &StreamKey, generation: u64) -> Result<(), RecoveryError> {
        let log = self.logs.get_mut(key).ok_or(RecoveryError::UnknownStream)?;
        if log.generation == generation {
            return Err(RecoveryError::GenerationMismatch);
        }
        let (before, budgeted) = (log.bytes, log.budgeted);
        *log = Log { generation, next: 1, records: VecDeque::new(), bytes: 0, ..*log };
        self.charge(key, budgeted, before, 0);
        self.hold_state(key, 0);
        Ok(())
    }

    pub fn remove(&mut self, key: &StreamKey) -> bool {
        self.hold_state(key, 0);
        let Some(log) = self.logs.remove(key) else { return false };
        self.charge(key, log.budgeted, log.bytes, 0);
        true
    }

    /// Validates before mutating. Overlap is checked byte-for-byte where it is still held;
    /// an evicted overlap cannot be checked and is skipped. A batch larger than the ring keeps its tail.
    pub fn append(&mut self, key: &StreamKey, batch: &SequencedBatch) -> Result<(), RecoveryError> {
        let room = self.ring_room(key).unwrap_or(usize::MAX);
        let log = self.logs.get_mut(key).ok_or(RecoveryError::UnknownStream)?;
        if batch.generation != log.generation {
            return Err(RecoveryError::GenerationMismatch);
        }
        if batch.first > log.next {
            return Err(RecoveryError::Gap { expected: log.next, actual: batch.first });
        }
        let first = log.first();
        for (sequence, record) in (batch.first..log.next).zip(batch.records.iter()) {
            if sequence >= first && log.records[(sequence - first) as usize] != *record {
                return Err(RecoveryError::ConflictingDuplicate);
            }
        }
        let before = log.bytes;
        for record in batch.records.iter().skip((log.next - batch.first) as usize) {
            log.bytes += record.len();
            log.records.push_back(record.clone());
            log.next += 1;
        }
        while log.records.len() > log.max_records || log.bytes > log.max_bytes.min(room.saturating_sub(log.state)) {
            log.bytes -= log.records.pop_front().map_or(0, |r| r.len());
        }
        let (after, budgeted) = (log.bytes, log.budgeted);
        self.charge(key, budgeted, before, after);
        Ok(())
    }

    pub fn retained_bytes(&self, key: &StreamKey) -> usize {
        self.logs.get(key).map_or(0, |log| log.bytes)
    }
}

impl RecoverySource for MemoryStore {
    fn available_range(&self, key: &StreamKey) -> Result<ReplayRange, RecoveryError> {
        let log = self.logs.get(key).ok_or(RecoveryError::UnknownStream)?;
        Ok(ReplayRange { generation: log.generation, first: log.first(), last: log.next - 1 })
    }

    fn read_after(
        &self,
        key: &StreamKey,
        after: Cursor,
        through: u64,
        bounds: ReadBounds,
    ) -> Result<Vec<SequencedBatch>, RecoveryError> {
        let range = self.available_range(key)?;
        if after.generation != range.generation {
            return Err(RecoveryError::GenerationMismatch);
        }
        if !range.covers(after.sequence, through) {
            return Err(RecoveryError::HistoryUnavailable);
        }
        if after.sequence == through {
            return Ok(Vec::new());
        }
        if bounds.max_records == 0 || bounds.max_bytes == 0 {
            return Err(RecoveryError::LimitExceeded);
        }
        let log = &self.logs[key];
        let (mut page, mut bytes) = (Vec::new(), 0);
        for record in log.records.iter().skip((after.sequence + 1 - range.first) as usize) {
            if page.len() as u64 >= through - after.sequence
                || page.len() >= bounds.max_records
                || bytes + record.len() > bounds.max_bytes
            {
                break;
            }
            bytes += record.len();
            page.push(record.clone());
        }
        if page.is_empty() {
            return Err(RecoveryError::LimitExceeded);
        }
        Ok(vec![SequencedBatch::new(range.generation, after.sequence + 1, page)?])
    }
}

/// A store the publisher appends to while subscriptions replay from it.
impl<S: RecoverySource> RecoverySource for Mutex<S> {
    fn available_range(&self, key: &StreamKey) -> Result<ReplayRange, RecoveryError> {
        lock(self).available_range(key)
    }

    fn read_after(
        &self,
        key: &StreamKey,
        after: Cursor,
        through: u64,
        bounds: ReadBounds,
    ) -> Result<Vec<SequencedBatch>, RecoveryError> {
        lock(self).read_after(key, after, through, bounds)
    }
}

pub(crate) fn lock<S>(store: &Mutex<S>) -> std::sync::MutexGuard<'_, S> {
    store.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}
