//! The fanout hub: a synchronous, single-owner state machine. It owns no threads, clock or
//! sockets; the host authenticates peers, publishes, drives recovery and drains events.
//!
//! One mutable owner makes "capture the cut and start retaining live batches after it"
//! atomic, which is what makes the replay-to-live join gap-free.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use axon::IdHash;

use crate::disk::DiskError;
use crate::recovery::{
    CompactLimits, Cursor, MemoryState, Payload, ReadBounds, RecoveryError, RecoverySource, SequencedBatch, Snapshot,
    StreamKey, compacted,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    Publish,
    Subscribe,
    Replay,
    Snapshot,
}

/// Exact-key permission checks. Asked when a session acts and again for every subscription
/// in [`StreamHub::update_authorizer`], which is the revocation boundary.
pub trait Authorizer {
    fn authorize(&self, peer: &PeerId, key: &StreamKey, access: Access) -> bool;
}

/// A trusted-process policy. It is not authentication.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAll;

impl Authorizer for AllowAll {
    fn authorize(&self, _: &PeerId, _: &StreamKey, _: Access) -> bool {
        true
    }
}

impl<T: Authorizer + ?Sized> Authorizer for Arc<T> {
    fn authorize(&self, peer: &PeerId, key: &StreamKey, access: Access) -> bool {
        (**self).authorize(peer, key, access)
    }
}

/// A node's boot. One node has at most one live session with a hub; a different incarnation replaces it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId {
    pub node: Arc<str>,
    pub incarnation: u128,
}

impl PeerId {
    pub fn new(node: &str, incarnation: u128) -> Self {
        Self { node: node.into(), incarnation }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HubLimits {
    pub max_sessions: usize,
    pub max_streams: usize,
    pub max_subscriptions: usize,
    pub max_interests_per_session: usize,
    pub max_interest_keys: usize,
    pub max_key_bytes: usize,
    pub max_batch_records: usize,
    pub max_batch_bytes: usize,
    /// Summed over branches: a payload queued on N branches is charged N times.
    pub max_queued_bytes: usize,
    pub max_queued_records: usize,
}

impl Default for HubLimits {
    fn default() -> Self {
        Self {
            max_sessions: 1024,
            max_streams: 4096,
            max_subscriptions: 16_384,
            max_interests_per_session: 4096,
            max_interest_keys: 16_384,
            max_key_bytes: 4096,
            max_batch_records: 4096,
            max_batch_bytes: 1 << 20,
            max_queued_bytes: 256 << 20,
            max_queued_records: 1_000_000,
        }
    }
}

/// One namespace's limits. Subscriptions and ring bytes hold on each node, exactly; the rates and open streams hold
/// across the cluster, through the cluster's quotas at each stream's origin. `None` is no limit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NamespaceLimits {
    /// Subscriptions to the namespace on this node, until their end is read. A downstream node's branch is not one:
    /// its subscribers are held on their own node.
    pub subscriptions: Option<usize>,
    /// Record bytes of the rings, and the compacted states, of the namespace's streams this node originates. A publish
    /// that would not fit once its own ring gives up its oldest records is refused. A relayed stream's ring is held by
    /// `CoreConfig::rings`.
    pub ring_bytes: Option<usize>,
    pub records_per_sec: Option<u64>,
    pub bytes_per_sec: Option<u64>,
    pub open_streams: Option<u64>,
}

/// The names of [`NamespaceLimit`], in its order.
pub const NAMESPACE_LIMITS: [&str; 5] = ["subscriptions", "ring_bytes", "records", "bytes", "open_streams"];

/// The limit of [`NamespaceLimits`] a refusal names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum NamespaceLimit {
    Subscriptions,
    RingBytes,
    Records,
    Bytes,
    OpenStreams,
}

/// Per subscription. Control events are never counted; the terminal event has its own slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BranchLimits {
    pub max_bytes: usize,
    pub max_records: usize,
    pub max_events: usize,
}

impl BranchLimits {
    /// `CoreConfig::branch`'s default: a stream may have half of it in flight to a child, so 32 MiB carries about
    /// 70 MB/s at a 219 ms round trip.
    pub const BRANCH: Self = Self { max_bytes: 32 << 20, max_records: 65_536, max_events: 4096 };
}

impl Default for BranchLimits {
    /// A subscription's: holds what a parent with the default `CoreConfig` has in flight of one stream, applied back to
    /// back. A new push takes a stream's items while under half its branch is in flight ([`BranchLimits::BRANCH`]),
    /// and one push holds at most 1 MiB, `hub.max_batch_records` (4096) records and `max_push_items` (1024) batches, so
    /// at most 17 MiB, 36864 records and 3072 events. A node hands each push to its subscribers at once, so a smaller
    /// queue would reset a reader that reads once a round trip whenever a far parent's window opens.
    fn default() -> Self {
        Self { max_bytes: Self::BRANCH.max_bytes / 2 + (1 << 20), ..Self::BRANCH }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryPolicy {
    /// Every record matters; a lagging branch is stopped or reset.
    ReplayRequired,
    /// Each batch is one record that replaces the state, so a lagging branch may skip to the latest.
    ReplaceableState,
    /// Every record is [`keyed`](crate::keyed), and the origin keeps the latest record of each key, within the
    /// limits: a `SnapshotThenLive` subscriber gets them, then every record after them. Relays pass records on as
    /// they are.
    Compacted(CompactLimits),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SlowConsumerPolicy {
    Detach,
    #[default]
    Reset,
    /// Only on a `ReplaceableState` stream.
    CoalesceLatest,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StartPosition {
    #[default]
    Live,
    /// Resume strictly after an applied position.
    After(Cursor),
    /// From record 1 of the current generation.
    First,
    /// The state of a [`Compacted`](DeliveryPolicy::Compacted) stream, then every record after it, on any node. A
    /// stream without compaction has no state: an empty snapshot at record 0, then its whole history. Ends
    /// `End(Revoked)` where `Access::Snapshot` is denied on the way, and `End(SlowConsumer)` on a relay whose
    /// subscriber, branches or pushes cannot hold the state.
    SnapshotThenLive,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SubscribeOptions {
    pub start: StartPosition,
    pub slow_consumer: SlowConsumerPolicy,
    pub limits: BranchLimits,
}

pub const RESET_REASONS: [&str; 6] =
    ["generation_changed", "slow_consumer", "history_unavailable", "gap", "invalid_recovery", "global_capacity"];
pub const END_REASONS: [&str; 7] = [
    "unsubscribed",
    "session_replaced",
    "session_expired",
    "revoked",
    "slow_consumer",
    "stream_closed",
    "upstream_lost",
];

/// Continuity is broken: the consumer must start over. Indexes [`RESET_REASONS`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ResetReason {
    GenerationChanged,
    SlowConsumer,
    HistoryUnavailable,
    Gap,
    InvalidRecovery,
    GlobalCapacity,
}

/// Indexes [`END_REASONS`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum EndReason {
    Unsubscribed,
    SessionReplaced,
    SessionExpired,
    Revoked,
    SlowConsumer,
    StreamClosed,
    UpstreamLost,
}

/// Order per subscription: `Ready`, then `Snapshot` if asked for, replayed batches,
/// `CaughtUp`, live batches. `CaughtUp` says the prefix was emitted, not applied or durable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamEvent {
    /// `None` for a live start on a relay stream whose position is not known yet; a second `Ready`
    /// follows once it is. A cursor or `First` start on such a stream sees nothing until then.
    Ready {
        start: Option<Cursor>,
    },
    Snapshot(Snapshot),
    Batch(SequencedBatch),
    CaughtUp {
        through: Cursor,
    },
    /// Replaces the consumer's state; nothing may be inferred about the skipped records.
    Replacement {
        batch: SequencedBatch,
    },
    ResetRequired(ResetReason),
    End(EndReason),
    /// To a `Live` follower of a pulled key: no parent reaches the origin any more. Every parent says it lacks the
    /// key or has lost its own upstream, or none has fed the key for its `upstream_timeout`. Told once per loss, and to a
    /// follower that subscribes before a parent feeds the key again. Not an end: the follower keeps its position, the
    /// same generation goes on if its origin comes back, and a new origin is a new `Ready`.
    OriginLost,
}

impl StreamEvent {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::ResetRequired(_) | Self::End(_))
    }

    fn charge(&self) -> Option<(usize, usize)> {
        match self {
            Self::Batch(b) | Self::Replacement { batch: b } => Some((b.bytes(), b.len())),
            Self::Snapshot(s) => Some((s.bytes(), 0)),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamError {
    Forbidden,
    StaleSession,
    UnknownStream,
    UnknownSubscription,
    AlreadyExists,
    LimitExceeded,
    InvalidOptions,
    StaleRevision,
    RevisionConflict,
    /// A change to a node's wanted set was built on a revision its parent does not hold.
    OutOfSync,
    NotRecovering,
    /// A publish would leave a compacted stream past its `CompactLimits`. Nothing of it was applied; removing keys
    /// makes room.
    StateFull,
    /// The origin keeps the stream on disk, and the disk refused it; nothing of the publish was admitted.
    Disk(DiskError),
    Recovery(RecoveryError),
    /// The namespace is at one of its [`NamespaceLimits`]; a cluster-wide one says when to try again.
    QuotaExceeded {
        namespace: Arc<str>,
        limit: NamespaceLimit,
        retry_after: Option<Duration>,
    },
}

impl From<RecoveryError> for StreamError {
    fn from(e: RecoveryError) -> Self {
        Self::Recovery(e)
    }
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for StreamError {}

/// A definite rejection: nothing was admitted and the caller gets its records back.
#[derive(Debug)]
pub struct PublishError<T> {
    pub error: StreamError,
    pub returned: T,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryProgress {
    Advanced(Cursor),
    CaughtUp(Cursor),
    /// Backpressure: poll, then drive again.
    Blocked,
    Live,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InterestAck {
    pub revision: u64,
    pub changed: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueUsage {
    pub bytes: usize,
    pub records: usize,
    pub events: usize,
}

impl QueueUsage {
    /// What an event takes of its branch: its bytes, its records and one event; nothing for a control event.
    fn of(event: &StreamEvent) -> Self {
        event.charge().map_or_else(Self::default, |(bytes, records)| Self { bytes, records, events: 1 })
    }

    pub(crate) fn add(&mut self, other: Self) {
        (self.bytes, self.records, self.events) =
            (self.bytes + other.bytes, self.records + other.records, self.events + other.events);
    }

    /// A charge is given back once; never below zero in a release build if one is not.
    fn sub(&mut self, other: Self) {
        debug_assert!(
            self.bytes >= other.bytes && self.records >= other.records && self.events >= other.events,
            "{other:?} given back from {self:?}"
        );
        self.bytes = self.bytes.saturating_sub(other.bytes);
        self.records = self.records.saturating_sub(other.records);
        self.events = self.events.saturating_sub(other.events);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct HubStats {
    pub streams: usize,
    pub sessions: usize,
    pub subscriptions: usize,
    pub recovering: usize,
    pub demand_keys: usize,
    pub queued_bytes: usize,
    pub queued_records: usize,
    pub published_batches: u64,
    pub published_records: u64,
    pub published_bytes: u64,
    /// Records dropped because their positions were already published.
    pub duplicate_records: u64,
    pub publish_rejected: u64,
    /// Counted at `poll`, where queue capacity is released.
    pub delivered_records: u64,
    pub delivered_bytes: u64,
    pub replayed_records: u64,
    pub recovery_blocked: u64,
    pub coalesced: u64,
    /// By [`RESET_REASONS`].
    pub resets: [u64; 6],
    /// By [`END_REASONS`].
    pub ends: [u64; 7],
    /// Refusals at a namespace's limit, by [`NAMESPACE_LIMITS`]; a refused publish is in `publish_rejected` too.
    pub quota_refused: [u64; 5],
}

impl HubStats {
    /// Counts a subscription's terminal event under its reason.
    fn count(&mut self, terminal: &StreamEvent) {
        match terminal {
            StreamEvent::ResetRequired(reason) => self.resets[*reason as usize] += 1,
            StreamEvent::End(reason) => self.ends[*reason as usize] += 1,
            _ => {}
        }
    }
}

struct Session {
    incarnation: u128,
    revision: u64,
    interests: BTreeSet<StreamKey>,
}

struct Stream {
    /// `None` until a relay stream sees its first batch and adopts the origin's position.
    cursor: Option<Cursor>,
    policy: DeliveryPolicy,
    subscribers: Vec<u64>,
    /// Fed by a parent, so a cursor past its position is one it has not received yet.
    relay: bool,
    /// A compacted stream's, at its origin.
    state: Option<MemoryState>,
}

impl Stream {
    /// `None` before a relay stream has a position.
    fn snapshot(&self) -> Option<Snapshot> {
        let cursor = self.cursor?;
        Some(match &self.state {
            Some(state) => Snapshot::new(cursor, state.latest()),
            None => Snapshot::new(Cursor { sequence: 0, ..cursor }, Vec::new()),
        })
    }
}

enum Phase {
    Recovering {
        next_after: u64,
        cut: Cursor,
        source: Arc<dyn RecoverySource>,
        with_state: bool,
    },
    Live,
    /// A cursor or `First` (`None` until the generation is known) start on a relay stream that had no position,
    /// or a cursor ahead of a relay's position in its generation: nothing is queued until the stream reaches it.
    Waiting(Option<Cursor>),
    /// A `SnapshotThenLive` start on a relay stream: nothing is queued until its parent sends a snapshot.
    Snapshot(Option<Arc<dyn RecoverySource>>),
    Done,
}

struct Sub {
    peer: PeerId,
    key: StreamKey,
    policy: SlowConsumerPolicy,
    limits: BranchLimits,
    queue: VecDeque<StreamEvent>,
    /// Live batches at the back of the queue, held until recovery reaches the cut.
    tail: usize,
    usage: QueueUsage,
    /// Polled with [`StreamHub::poll_charged`] and not yet given back: a downstream branch's records in pushes the
    /// child has not applied. Counted against the branch's limits with `usage`, not against the hub's total.
    inflight: QueueUsage,
    phase: Phase,
    terminal: Option<StreamEvent>,
    /// Survives restarts and closes of the key: a `Ready` marks each one instead of a terminal event.
    follow: bool,
    /// The position the relay gave up under this follower: another one in its generation would skip what was lost.
    lost: Option<Cursor>,
    /// A branch waiting for the snapshot its relay asked its own parent for.
    awaits_state: bool,
    /// The state for a branch, queued once it has stopped replaying and has room for it, so it never holds back what
    /// it follows.
    held: Option<Box<Snapshot>>,
}

impl Sub {
    /// What the branch holds: queued, and in flight to its child.
    fn held(&self) -> QueueUsage {
        let mut held = self.usage;
        held.add(self.inflight);
        held
    }

    fn fits(&self, bytes: usize, records: usize, total: &QueueUsage, hub: &HubLimits) -> (bool, bool) {
        let held = self.held();
        let branch = held.events < self.limits.max_events
            && held.bytes + bytes <= self.limits.max_bytes
            && held.records + records <= self.limits.max_records;
        let global = total.bytes + bytes <= hub.max_queued_bytes && total.records + records <= hub.max_queued_records;
        (branch, global)
    }

    /// The only place queue capacity is taken.
    fn enqueue(&mut self, at: usize, event: StreamEvent, total: &mut QueueUsage) {
        let charge = QueueUsage::of(&event);
        self.usage.add(charge);
        (total.bytes, total.records) = (total.bytes + charge.bytes, total.records + charge.records);
        self.queue.insert(at, event);
    }

    /// A new position. An unpolled one it replaces carried nothing the new one does not.
    fn ready(&mut self, start: Option<Cursor>) {
        if start.is_some() {
            self.lost = None;
        }
        if self.tail == 0 && matches!(self.queue.back(), Some(StreamEvent::Ready { .. })) {
            self.queue.pop_back();
        }
        self.queue.push_back(StreamEvent::Ready { start });
    }

    /// Whether a relay's new position `at` would move this follower past records its node lost.
    fn would_skip(&self, at: Cursor) -> bool {
        self.lost.is_some_and(|lost| at.generation <= lost.generation && at != lost)
    }

    /// The only place queue capacity is given back.
    fn release(&mut self, event: &StreamEvent, total: &mut QueueUsage) {
        if let Some((bytes, records)) = event.charge() {
            self.usage.bytes -= bytes;
            self.usage.records -= records;
            self.usage.events -= 1;
            total.bytes -= bytes;
            total.records -= records;
        }
    }

    /// Goes live once the stream is at or past a waiting start.
    fn reach(&mut self, at: u64) {
        if let Phase::Waiting(Some(after)) = self.phase
            && at >= after.sequence
        {
            self.queue.push_back(StreamEvent::Ready { start: Some(after) });
            self.queue.push_back(StreamEvent::CaughtUp { through: after });
            self.phase = Phase::Live;
        }
    }

    fn pollable(&self) -> bool {
        self.queue.len() > self.tail || self.terminal.is_some()
    }

    /// Its first events: `Ready`, the state if it has one, then a replay to `cut` or `CaughtUp` at once.
    fn begin(
        &mut self,
        start: Option<Cursor>,
        snapshot: Option<Snapshot>,
        replay: Option<(u64, Cursor, Arc<dyn RecoverySource>)>,
        total: &mut QueueUsage,
    ) {
        self.queue.push_back(StreamEvent::Ready { start });
        let with_state = snapshot.is_some();
        if let Some(snapshot) = snapshot {
            self.enqueue(1, StreamEvent::Snapshot(snapshot), total);
        }
        self.phase = match replay {
            Some((next_after, cut, source)) => Phase::Recovering { next_after, cut, source, with_state },
            None => {
                self.queue.push_back(StreamEvent::CaughtUp { through: start.unwrap_or_default() });
                Phase::Live
            }
        };
    }

    /// `true` when the state still waits with nothing to poll ahead of it and nothing in flight: only the hub's
    /// global room is short, and no poll or discharge of this branch would come to try again.
    fn release_held(&mut self, total: &mut QueueUsage, hub: &HubLimits) -> bool {
        let Some(s) = self.held.as_ref().filter(|_| !matches!(self.phase, Phase::Recovering { .. })) else {
            return false;
        };
        if self.fits(s.bytes(), 0, total, hub) == (true, true) {
            let (at, snapshot) = (self.queue.len(), self.held.take().expect("looked at above"));
            self.enqueue(at, StreamEvent::Snapshot(*snapshot), total);
            return false;
        }
        !self.pollable() && self.inflight == QueueUsage::default()
    }

    /// Admits a live batch or says how the branch ends.
    fn offer(
        &mut self,
        batch: &SequencedBatch,
        total: &mut QueueUsage,
        hub: &HubLimits,
        coalesced: &mut u64,
    ) -> Result<(), StreamEvent> {
        if matches!(self.phase, Phase::Snapshot(_)) {
            return Ok(());
        }
        if let Phase::Waiting(Some(after)) = self.phase {
            self.reach(batch.last());
            return match batch.clone().after(after.sequence) {
                Some(rest) => self.offer(&rest, total, hub, coalesced),
                None => Ok(()),
            };
        }
        let (branch, global) = self.fits(batch.bytes(), batch.len(), total, hub);
        if branch && global {
            let at = self.queue.len();
            self.enqueue(at, StreamEvent::Batch(batch.clone()), total);
            if matches!(self.phase, Phase::Recovering { .. }) {
                self.tail += 1;
            }
            return Ok(());
        }
        match self.policy {
            SlowConsumerPolicy::Detach => Err(StreamEvent::End(EndReason::SlowConsumer)),
            SlowConsumerPolicy::Reset => Err(StreamEvent::ResetRequired(ResetReason::SlowConsumer)),
            SlowConsumerPolicy::CoalesceLatest => {
                for event in std::mem::take(&mut self.queue) {
                    if event.charge().is_some() {
                        self.release(&event, total);
                    } else {
                        self.queue.push_back(event);
                    }
                }
                (self.tail, self.phase) = (0, Phase::Live);
                if !self.fits(batch.bytes(), batch.len(), total, hub).1 {
                    return Err(StreamEvent::ResetRequired(ResetReason::GlobalCapacity));
                }
                *coalesced += 1;
                let at = self.queue.len();
                self.enqueue(at, StreamEvent::Replacement { batch: batch.clone() }, total);
                Ok(())
            }
        }
    }

    fn end(&mut self, terminal: StreamEvent, total: &mut QueueUsage) {
        for event in std::mem::take(&mut self.queue) {
            self.release(&event, total);
        }
        (self.tail, self.phase, self.terminal) = (0, Phase::Done, Some(terminal));
    }
}

fn add_demand(demand: &mut BTreeMap<StreamKey, usize>, key: &StreamKey) {
    match demand.get_mut(key) {
        Some(count) => *count += 1,
        None => {
            demand.insert(key.clone(), 1);
        }
    }
}

fn release_demand(demand: &mut BTreeMap<StreamKey, usize>, key: &StreamKey) {
    if let Some(count) = demand.get_mut(key) {
        *count -= 1;
        if *count == 0 {
            demand.remove(key);
        }
    }
}

/// What a publish is checked for once it is allowed, before it takes positions: its stream, and the cursor it follows.
fn publishable<'s>(
    limits: &HubLimits,
    streams: &'s mut HashMap<StreamKey, Stream, IdHash>,
    key: &StreamKey,
    records: &[Payload],
) -> Result<(&'s mut Stream, Cursor), StreamError> {
    let bytes = records.iter().try_fold(0usize, |sum, r| sum.checked_add(r.len()));
    if records.len() > limits.max_batch_records || bytes.is_none_or(|b| b > limits.max_batch_bytes) {
        return Err(StreamError::LimitExceeded);
    }
    let stream = streams.get_mut(key).ok_or(StreamError::UnknownStream)?;
    let cursor = stream.cursor.ok_or(StreamError::InvalidOptions)?;
    if records.is_empty() || (stream.policy == DeliveryPolicy::ReplaceableState && records.len() != 1) {
        return Err(RecoveryError::InvalidBatch.into());
    }
    cursor.sequence.checked_add(records.len() as u64).ok_or(RecoveryError::PositionOverflow)?;
    Ok((stream, cursor))
}

/// Moves the stream's cursor to the batch and offers it to each subscriber.
#[expect(clippy::too_many_arguments, reason = "the hub's fields apart from the stream, which the caller has looked up")]
fn deliver(
    stream: &mut Stream,
    subs: &mut HashMap<u64, Sub, IdHash>,
    total: &mut QueueUsage,
    limits: &HubLimits,
    stats: &mut HubStats,
    demand: &mut BTreeMap<StreamKey, usize>,
    key: &StreamKey,
    batch: &SequencedBatch,
) {
    stream.cursor = Some(batch.cursor());
    stats.published_batches += 1;
    stats.published_records += batch.len() as u64;
    stats.published_bytes += batch.bytes() as u64;
    stream.subscribers.retain(|id| {
        let sub = subs.get_mut(id).expect("subscribers are live subscriptions");
        let Err(terminal) = sub.offer(batch, total, limits, &mut stats.coalesced) else { return true };
        stats.count(&terminal);
        sub.end(terminal, total);
        release_demand(demand, key);
        false
    });
}

pub struct StreamHub<A = AllowAll> {
    authorizer: A,
    limits: HubLimits,
    // Ordered: iterated while it changes demand, so a run stays reproducible.
    sessions: BTreeMap<Arc<str>, Session>,
    streams: HashMap<StreamKey, Stream, IdHash>,
    subs: HashMap<u64, Sub, IdHash>,
    demand: BTreeMap<StreamKey, usize>,
    /// Followers of a closed key, waiting for it to be opened again.
    parked: BTreeMap<StreamKey, Vec<u64>>,
    /// Per limited namespace: its most subscriptions, and those `counted`'s session holds.
    namespace_subs: BTreeMap<String, (usize, usize)>,
    counted: Arc<str>,
    next_subscription: u64,
    total: QueueUsage,
    stats: HubStats,
}

impl<A: Authorizer> StreamHub<A> {
    pub fn new(authorizer: A, limits: HubLimits) -> Result<Self, StreamError> {
        let l = &limits;
        let all = [
            l.max_sessions,
            l.max_streams,
            l.max_subscriptions,
            l.max_interests_per_session,
            l.max_interest_keys,
            l.max_key_bytes,
            l.max_batch_records,
            l.max_batch_bytes,
            l.max_queued_bytes,
            l.max_queued_records,
        ];
        if all.contains(&0) {
            return Err(StreamError::InvalidOptions);
        }
        Ok(Self {
            authorizer,
            limits,
            sessions: BTreeMap::new(),
            streams: HashMap::default(),
            subs: HashMap::default(),
            demand: BTreeMap::new(),
            parked: BTreeMap::new(),
            namespace_subs: BTreeMap::new(),
            counted: "".into(),
            next_subscription: 1,
            total: QueueUsage::default(),
            stats: HubStats::default(),
        })
    }

    /// Holds the subscriptions of `local`'s session to each namespace's `subscriptions`.
    pub(crate) fn with_namespaces(mut self, local: &PeerId, limits: &BTreeMap<String, NamespaceLimits>) -> Self {
        self.namespace_subs = limits.iter().filter_map(|(ns, l)| Some((ns.clone(), (l.subscriptions?, 0)))).collect();
        self.counted = local.node.clone();
        self
    }

    /// Counts a refusal at the namespace's `limit`, and returns it.
    pub(crate) fn refuse(
        &mut self,
        key: &StreamKey,
        limit: NamespaceLimit,
        retry_after: Option<Duration>,
    ) -> StreamError {
        self.stats.quota_refused[limit as usize] += 1;
        StreamError::QuotaExceeded { namespace: key.namespace().into(), limit, retry_after }
    }

    fn session(&self, peer: &PeerId) -> Result<&Session, StreamError> {
        self.sessions.get(&peer.node).filter(|s| s.incarnation == peer.incarnation).ok_or(StreamError::StaleSession)
    }

    pub(crate) fn allowed(&self, peer: &PeerId, key: &StreamKey, access: Access) -> Result<(), StreamError> {
        self.session(peer)?;
        if self.authorizer.authorize(peer, key, access) { Ok(()) } else { Err(StreamError::Forbidden) }
    }

    fn terminate(&mut self, id: u64, terminal: StreamEvent) {
        let Some(sub) = self.subs.get_mut(&id).filter(|s| s.terminal.is_none() && !matches!(s.phase, Phase::Done))
        else {
            return;
        };
        self.stats.count(&terminal);
        sub.end(terminal, &mut self.total);
        release_demand(&mut self.demand, &sub.key);
        if let Some(stream) = self.streams.get_mut(&sub.key) {
            stream.subscribers.retain(|s| *s != id);
        } else if let Some(parked) = self.parked.get_mut(&sub.key) {
            parked.retain(|s| *s != id);
            if parked.is_empty() {
                self.parked.remove(&sub.key);
            }
        }
    }

    /// Ends the key's subscriptions. Followers instead see the key's new position, or wait
    /// parked for it to be opened again when `terminal` is an `End`.
    fn terminate_key(&mut self, key: &StreamKey, terminal: StreamEvent) {
        let Some(stream) = self.streams.get_mut(key) else { return };
        let (ids, start) = (std::mem::take(&mut stream.subscribers), stream.cursor);
        for id in ids {
            match self.subs.get_mut(&id) {
                Some(sub) if sub.follow && !matches!(terminal, StreamEvent::End(_)) => {
                    sub.ready(start);
                    self.stats.count(&terminal);
                    self.streams.get_mut(key).expect("looked up above").subscribers.push(id);
                }
                Some(sub) if sub.follow => self.parked.entry(key.clone()).or_default().push(id),
                _ => self.terminate(id, terminal.clone()),
            }
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "IdHash hashes an id the same in every process, so the same calls give the same order"
    )]
    fn ids_where(&self, pick: impl Fn(&Sub) -> bool) -> Vec<u64> {
        self.subs.iter().filter(|(_, s)| pick(s)).map(|(id, _)| *id).collect()
    }

    fn terminate_where(&mut self, terminal: StreamEvent, pick: impl Fn(&Sub) -> bool) {
        for id in self.ids_where(pick) {
            self.terminate(id, terminal.clone());
        }
    }

    fn close_session(&mut self, peer: &str, reason: EndReason) {
        let Some(session) = self.sessions.remove(peer) else { return };
        self.terminate_where(StreamEvent::End(reason), |s| *s.peer.node == *peer);
        for key in &session.interests {
            release_demand(&mut self.demand, key);
        }
    }

    /// Call only after authenticating the peer.
    pub fn open_session(&mut self, peer: &PeerId) -> Result<(), StreamError> {
        if peer.node.is_empty() || peer.node.len() > self.limits.max_key_bytes {
            return Err(StreamError::InvalidOptions);
        }
        match self.sessions.get(&peer.node) {
            Some(s) if s.incarnation == peer.incarnation => return Ok(()),
            Some(_) => self.close_session(&peer.node, EndReason::SessionReplaced),
            None => {}
        }
        if self.sessions.len() >= self.limits.max_sessions {
            return Err(StreamError::LimitExceeded);
        }
        let session = Session { incarnation: peer.incarnation, revision: 0, interests: BTreeSet::new() };
        self.sessions.insert(peer.node.clone(), session);
        Ok(())
    }

    pub fn expire_session(&mut self, peer: &PeerId) -> Result<(), StreamError> {
        self.session(peer)?;
        self.close_session(&peer.node, EndReason::SessionExpired);
        Ok(())
    }

    /// The revocation boundary: changes the policy, then ends every subscription and drops
    /// every interest it no longer permits. Events already polled cannot be recalled.
    pub fn update_authorizer(&mut self, change: impl FnOnce(&mut A)) {
        change(&mut self.authorizer);
        let revoked = self.ids_where(|s| {
            let ok = |access| self.authorizer.authorize(&s.peer, &s.key, access);
            let recovering = match s.phase {
                Phase::Recovering { with_state, .. } => ok(Access::Replay) && (!with_state || ok(Access::Snapshot)),
                Phase::Waiting(_) => ok(Access::Replay),
                Phase::Snapshot(_) => ok(Access::Replay) && ok(Access::Snapshot),
                _ => true,
            };
            !(ok(Access::Subscribe) && recovering)
        });
        for id in revoked {
            self.terminate(id, StreamEvent::End(EndReason::Revoked));
        }
        let Self { sessions, demand, authorizer, .. } = self;
        for (node, session) in sessions.iter_mut() {
            let peer = PeerId { node: node.clone(), incarnation: session.incarnation };
            session.interests.retain(|key| {
                let keep = authorizer.authorize(&peer, key, Access::Subscribe);
                if !keep {
                    release_demand(demand, key);
                }
                keep
            });
        }
    }

    pub fn open_stream(
        &mut self,
        publisher: &PeerId,
        key: &StreamKey,
        generation: u64,
        policy: DeliveryPolicy,
    ) -> Result<(), StreamError> {
        self.insert_stream(publisher, key, Some(Cursor { generation, sequence: 0 }), policy)
    }

    /// An origin stream that goes on after `cursor`, from history kept elsewhere, with `latest` as a compacted stream's
    /// state.
    pub(crate) fn resume_stream(
        &mut self,
        publisher: &PeerId,
        key: &StreamKey,
        cursor: Cursor,
        policy: DeliveryPolicy,
        latest: &[Payload],
    ) -> Result<(), StreamError> {
        self.insert_stream(publisher, key, Some(cursor), policy)?;
        if let Some(state) = self.streams.get_mut(key).and_then(|s| s.state.as_mut()) {
            // Rebuilt under the same limits, so it fits.
            let _ = state.apply(latest, &mut |_| Ok(()));
        }
        Ok(())
    }

    /// A stream fed by [`accept_batch`](Self::accept_batch) that takes its generation and
    /// position from the first batch it sees.
    pub fn open_relay_stream(
        &mut self,
        publisher: &PeerId,
        key: &StreamKey,
        policy: DeliveryPolicy,
    ) -> Result<(), StreamError> {
        self.insert_stream(publisher, key, None, policy)
    }

    fn insert_stream(
        &mut self,
        publisher: &PeerId,
        key: &StreamKey,
        cursor: Option<Cursor>,
        policy: DeliveryPolicy,
    ) -> Result<(), StreamError> {
        self.allowed(publisher, key, Access::Publish)?;
        let empty = |l: CompactLimits| l.max_keys == 0 || l.max_bytes == 0;
        if !key.valid(self.limits.max_key_bytes) || matches!(policy, DeliveryPolicy::Compacted(l) if empty(l)) {
            return Err(StreamError::InvalidOptions);
        }
        if self.streams.contains_key(key) {
            return Err(StreamError::AlreadyExists);
        }
        if self.streams.len() >= self.limits.max_streams {
            return Err(StreamError::LimitExceeded);
        }
        let parked = self.parked.remove(key).unwrap_or_default();
        let mut subscribers = Vec::with_capacity(parked.len());
        for id in parked {
            let Some(sub) = self.subs.get_mut(&id) else { continue };
            if sub.policy == SlowConsumerPolicy::CoalesceLatest && policy != DeliveryPolicy::ReplaceableState {
                self.terminate(id, StreamEvent::End(EndReason::Unsubscribed));
            } else {
                sub.ready(cursor);
                subscribers.push(id);
            }
        }
        let state = cursor.and(compacted(policy).map(MemoryState::new));
        self.streams.insert(key.clone(), Stream { cursor, policy, subscribers, relay: cursor.is_none(), state });
        Ok(())
    }

    /// Starts a new sequence domain; every subscriber is reset.
    pub fn restart_stream(&mut self, publisher: &PeerId, key: &StreamKey, generation: u64) -> Result<(), StreamError> {
        self.allowed(publisher, key, Access::Publish)?;
        let stream = self.streams.get_mut(key).ok_or(StreamError::UnknownStream)?;
        if stream.cursor.is_some_and(|c| c.generation >= generation) {
            return Err(RecoveryError::GenerationMismatch.into());
        }
        stream.cursor = Some(Cursor { generation, sequence: 0 });
        stream.state = compacted(stream.policy).map(MemoryState::new);
        self.terminate_key(key, StreamEvent::ResetRequired(ResetReason::GenerationChanged));
        Ok(())
    }

    /// Gives a relay stream that has no position yet the one its parent reported.
    pub fn adopt_relay_stream(
        &mut self,
        publisher: &PeerId,
        key: &StreamKey,
        cursor: Cursor,
    ) -> Result<bool, StreamError> {
        self.allowed(publisher, key, Access::Publish)?;
        let stream = self.streams.get_mut(key).ok_or(StreamError::UnknownStream)?;
        if stream.cursor.is_some() {
            return Ok(false);
        }
        stream.cursor = Some(cursor);
        for id in &stream.subscribers {
            if let Some(sub) = self
                .subs
                .get_mut(id)
                .filter(|s| !matches!(s.phase, Phase::Waiting(_) | Phase::Snapshot(_)) && !s.would_skip(cursor))
            {
                sub.queue.push_back(StreamEvent::Ready { start: Some(cursor) });
            }
        }
        self.position(key, cursor);
        Ok(true)
    }

    /// A relay stream took its first position: each start waiting on it goes live or is reset, and so is a follower
    /// it would move past records lost to this node.
    fn position(&mut self, key: &StreamKey, at: Cursor) {
        let Some(stream) = self.streams.get_mut(key) else { return };
        // Taken out while the reset are ended, so each end finds no list to search.
        let mut ids = std::mem::take(&mut stream.subscribers);
        let mut reset = Vec::new();
        ids.retain(|&id| {
            let Some(sub) = self.subs.get_mut(&id) else { return true };
            let skips = sub.would_skip(at);
            sub.lost = None;
            let reason = if skips {
                ResetReason::HistoryUnavailable
            } else {
                let Phase::Waiting(after) = sub.phase else { return true };
                let after = after.unwrap_or(Cursor { sequence: 0, ..at });
                if after.generation != at.generation {
                    ResetReason::GenerationChanged
                } else if after.sequence < at.sequence {
                    ResetReason::HistoryUnavailable
                } else {
                    sub.phase = Phase::Waiting(Some(after));
                    sub.reach(at.sequence);
                    return true;
                }
            };
            reset.push((id, reason));
            false
        });
        for (id, reason) in reset {
            self.terminate(id, StreamEvent::ResetRequired(reason));
        }
        self.streams.get_mut(key).expect("looked up above").subscribers = ids;
    }

    /// Tells a follower [`StreamEvent::OriginLost`]; it keeps its position.
    pub(crate) fn origin_lost(&mut self, id: u64) {
        if let Some(sub) = self.subs.get_mut(&id).filter(|s| s.follow) {
            sub.queue.push_back(StreamEvent::OriginLost);
        }
    }

    /// Resets every subscriber and forgets a relay stream's position, so the next batch is adopted. A
    /// follower sees the next position, unless the position was lost (`HistoryUnavailable`) and the next
    /// is in its generation, past records it never had: then it is reset too.
    pub fn reset_relay_stream(
        &mut self,
        publisher: &PeerId,
        key: &StreamKey,
        reason: ResetReason,
    ) -> Result<(), StreamError> {
        self.allowed(publisher, key, Access::Publish)?;
        let stream = self.streams.get_mut(key).ok_or(StreamError::UnknownStream)?;
        let lost = stream.cursor.take().filter(|_| reason == ResetReason::HistoryUnavailable);
        self.terminate_key(key, StreamEvent::ResetRequired(reason));
        // A reset with no position left keeps the mark of the one before it.
        if let Some(lost) = lost {
            for id in self.streams.get(key).map(|s| s.subscribers.as_slice()).unwrap_or_default() {
                if let Some(sub) = self.subs.get_mut(id) {
                    sub.lost = Some(lost);
                }
            }
        }
        Ok(())
    }

    pub fn close_stream(&mut self, publisher: &PeerId, key: &StreamKey, reason: EndReason) -> Result<(), StreamError> {
        self.allowed(publisher, key, Access::Publish)?;
        if !self.streams.contains_key(key) {
            return Err(StreamError::UnknownStream);
        }
        self.terminate_key(key, StreamEvent::End(reason));
        self.streams.remove(key);
        Ok(())
    }

    /// `None` for a relay stream that has not seen a batch yet.
    pub fn cursor(&self, key: &StreamKey) -> Result<Option<Cursor>, StreamError> {
        self.streams.get(key).map(|s| s.cursor).ok_or(StreamError::UnknownStream)
    }

    pub fn has_stream(&self, key: &StreamKey) -> bool {
        self.streams.contains_key(key)
    }

    /// Assigns the next positions. Admission is neither delivery nor durability.
    pub fn publish(
        &mut self,
        publisher: &PeerId,
        key: &StreamKey,
        records: Vec<Payload>,
    ) -> Result<SequencedBatch, PublishError<Vec<Payload>>> {
        self.publish_admitted(publisher, key, records, None, |_, _| Ok(()))
    }

    /// [`publish`](Self::publish), with the trace the batch carries to its subscribers, once the batch has passed
    /// every check and then `admit`, which is given the bytes a compacted stream's state would hold after it (`None`
    /// without one).
    pub(crate) fn publish_admitted(
        &mut self,
        publisher: &PeerId,
        key: &StreamKey,
        records: Vec<Payload>,
        trace: Option<(u64, u64)>,
        admit: impl FnOnce(&[Payload], Option<usize>) -> Result<(), StreamError>,
    ) -> Result<SequencedBatch, PublishError<Vec<Payload>>> {
        let allowed = self.allowed(publisher, key, Access::Publish);
        let Self { streams, subs, total, limits, stats, demand, .. } = self;
        // The stream is looked up once for all of it: a second lookup on every publish cost 6 % of it.
        let checked = match allowed {
            Ok(()) => publishable(limits, streams, key, &records),
            Err(error) => Err(error),
        };
        // A compacted state takes the batch only once `admit` has, which the rate is taken in, last.
        let admitted = checked.and_then(|(stream, cursor)| {
            match &mut stream.state {
                Some(state) => {
                    let mut admit = Some(admit);
                    state.apply(&records, &mut |bytes| admit.take().expect("asked once")(&records, Some(bytes)))?;
                }
                None => admit(&records, None)?,
            }
            Ok((stream, cursor))
        });
        let (stream, cursor) = match admitted {
            Ok(admitted) => admitted,
            Err(error) => {
                self.count_rejected(&error);
                return Err(PublishError { error, returned: records });
            }
        };
        let batch = SequencedBatch::new(cursor.generation, cursor.sequence + 1, records)
            .expect("validated above")
            .traced(trace);
        deliver(stream, subs, total, limits, stats, demand, key, &batch);
        self.check();
        Ok(batch)
    }

    /// What a publish is checked for before it takes positions. A failure counts as a rejected publish.
    pub(crate) fn check_publish(
        &mut self,
        publisher: &PeerId,
        key: &StreamKey,
        records: &[Payload],
    ) -> Result<(), StreamError> {
        let checked = self
            .allowed(publisher, key, Access::Publish)
            .and_then(|()| publishable(&self.limits, &mut self.streams, key, records).map(drop));
        self.stats.publish_rejected += u64::from(checked.is_err());
        checked
    }

    /// Counts a publish refused after [`check_publish`](Self::check_publish) passed it.
    pub(crate) fn count_rejected(&mut self, error: &StreamError) {
        self.stats.publish_rejected += 1;
        if let StreamError::QuotaExceeded { limit, .. } = error {
            self.stats.quota_refused[*limit as usize] += 1;
        }
    }

    /// Hands on a batch that took its positions before, as [`DiskStore`](crate::disk::DiskStore) admits them: the one right
    /// after the stream's cursor. `false`, and nothing done, for any other.
    pub(crate) fn commit_admitted(&mut self, key: &StreamKey, batch: &SequencedBatch) -> bool {
        let Self { streams, subs, total, limits, stats, demand, .. } = self;
        let Some(stream) = streams.get_mut(key) else { return false };
        if stream.cursor != Some(Cursor { generation: batch.generation(), sequence: batch.first() - 1 }) {
            return false;
        }
        if let Some(state) = &mut stream.state {
            // Admitted by the same state under the same limits, so it fits.
            let _ = state.apply(batch.records(), &mut |_| Ok(()));
        }
        deliver(stream, subs, total, limits, stats, demand, key, batch);
        self.check();
        true
    }

    /// The relay entry point: admits records that already carry origin positions. Overlap with
    /// published positions is dropped by position, not content; a gap fails without mutation.
    /// Returns what was new, if anything.
    pub fn accept_batch(
        &mut self,
        publisher: &PeerId,
        key: &StreamKey,
        batch: SequencedBatch,
    ) -> Result<Option<SequencedBatch>, PublishError<SequencedBatch>> {
        let check = || {
            self.allowed(publisher, key, Access::Publish)?;
            if batch.len() > self.limits.max_batch_records || batch.bytes() > self.limits.max_batch_bytes {
                return Err(StreamError::LimitExceeded);
            }
            let stream = self.streams.get(key).ok_or(StreamError::UnknownStream)?;
            if stream.policy == DeliveryPolicy::ReplaceableState && batch.len() != 1 {
                return Err(RecoveryError::InvalidBatch.into());
            }
            let Some(cursor) = stream.cursor else { return Ok(batch.first() - 1) };
            if batch.generation() != cursor.generation {
                return Err(RecoveryError::GenerationMismatch.into());
            }
            if batch.first() > cursor.sequence + 1 {
                return Err(RecoveryError::Gap { expected: cursor.sequence + 1, actual: batch.first() }.into());
            }
            Ok(cursor.sequence)
        };
        let published = match check() {
            Ok(published) => published,
            Err(error) => {
                self.stats.publish_rejected += 1;
                return Err(PublishError { error, returned: batch });
            }
        };
        if self.cursor(key) == Ok(None) {
            self.position(key, Cursor { generation: batch.generation(), sequence: published });
        }
        let offered = batch.len();
        let fresh = batch.after(published);
        self.stats.duplicate_records += (offered - fresh.as_ref().map_or(0, SequencedBatch::len)) as u64;
        if let Some(batch) = &fresh {
            let Self { streams, subs, total, limits, stats, demand, .. } = self;
            let stream = streams.get_mut(key).expect("checked above");
            deliver(stream, subs, total, limits, stats, demand, key, batch);
            self.check();
        }
        Ok(fresh)
    }

    /// The cut is the stream cursor now; live batches after it are retained from this call on.
    pub fn subscribe(
        &mut self,
        peer: &PeerId,
        key: &StreamKey,
        options: SubscribeOptions,
        source: Option<Arc<dyn RecoverySource>>,
    ) -> Result<u64, StreamError> {
        self.admit(peer, key, options, source, false)
    }

    /// A `Live` subscription that follows the key: a restart or a close of the stream is a new
    /// `Ready { start }` (`None` while the position is unknown), never a terminal event. Between a
    /// close and the next open it holds the key's demand and receives nothing.
    pub fn follow(&mut self, peer: &PeerId, key: &StreamKey, options: SubscribeOptions) -> Result<u64, StreamError> {
        if options.start != StartPosition::Live {
            return Err(StreamError::InvalidOptions);
        }
        self.admit(peer, key, options, None, true)
    }

    fn admit(
        &mut self,
        peer: &PeerId,
        key: &StreamKey,
        options: SubscribeOptions,
        source: Option<Arc<dyn RecoverySource>>,
        follow: bool,
    ) -> Result<u64, StreamError> {
        self.allowed(peer, key, Access::Subscribe)?;
        if options.start != StartPosition::Live {
            self.allowed(peer, key, Access::Replay)?;
        }
        if options.start == StartPosition::SnapshotThenLive {
            self.allowed(peer, key, Access::Snapshot)?;
        }
        let l = options.limits;
        if l.max_bytes == 0 || l.max_records == 0 || l.max_events == 0 {
            return Err(StreamError::InvalidOptions);
        }
        let stream = self.streams.get(key).ok_or(StreamError::UnknownStream)?;
        if options.slow_consumer == SlowConsumerPolicy::CoalesceLatest
            && stream.policy != DeliveryPolicy::ReplaceableState
        {
            return Err(StreamError::InvalidOptions);
        }
        if self.subs.len() >= self.limits.max_subscriptions
            || (!self.demand.contains_key(key) && self.demand.len() >= self.limits.max_interest_keys)
        {
            return Err(StreamError::LimitExceeded);
        }
        let counted = peer.node == self.counted;
        if counted && self.namespace_subs.get(key.namespace()).is_some_and(|(max, held)| held >= max) {
            return Err(self.refuse(key, NamespaceLimit::Subscriptions, None));
        }
        let cut = stream.cursor;
        // A relay holds no state: the subscriber waits for the one its parent sends.
        let pending = (options.start == StartPosition::SnapshotThenLive && stream.relay).then(|| source.clone());
        let waiting = match (options.start, cut) {
            (StartPosition::After(_) | StartPosition::First, None) => true,
            (StartPosition::After(after), Some(cut)) => {
                stream.relay && after.generation == cut.generation && after.sequence > cut.sequence
            }
            _ => false,
        };
        let mut sub = Sub {
            peer: peer.clone(),
            key: key.clone(),
            policy: options.slow_consumer,
            limits: l,
            queue: VecDeque::new(),
            tail: 0,
            usage: QueueUsage::default(),
            inflight: QueueUsage::default(),
            phase: Phase::Live,
            terminal: None,
            follow,
            lost: None,
            awaits_state: false,
            held: None,
        };
        let mut snapshot = None;
        let from = match options.start {
            StartPosition::Live => None,
            _ if waiting || pending.is_some() => None,
            StartPosition::After(cursor) => {
                let cut = cut.ok_or(RecoveryError::HistoryUnavailable)?;
                if cursor.generation != cut.generation {
                    return Err(RecoveryError::GenerationMismatch.into());
                }
                Some(cursor.sequence)
            }
            StartPosition::First => Some(0),
            StartPosition::SnapshotThenLive => {
                let s = stream.snapshot().ok_or(RecoveryError::HistoryUnavailable)?;
                if s.bytes() > l.max_bytes || self.total.bytes + s.bytes() > self.limits.max_queued_bytes {
                    return Err(StreamError::LimitExceeded);
                }
                let through = s.through().sequence;
                snapshot = Some(s);
                Some(through)
            }
        };
        let start = cut.map(|c| Cursor { sequence: from.unwrap_or(c.sequence), ..c });
        let recovering = match (from, cut) {
            (Some(from), Some(cut)) if from != cut.sequence => {
                let source = source.ok_or(StreamError::InvalidOptions)?;
                if from > cut.sequence || !source.available_range(key)?.covers(from, cut.sequence) {
                    return Err(RecoveryError::HistoryUnavailable.into());
                }
                Some((from, cut, source))
            }
            _ => None,
        };
        let id = self.next_subscription;
        self.next_subscription = id.checked_add(1).ok_or(StreamError::LimitExceeded)?;
        // Nothing can fail from here on, so nothing charged below can leak.
        if let Some(source) = pending {
            sub.phase = Phase::Snapshot(source);
        } else if waiting {
            sub.phase = Phase::Waiting(match options.start {
                StartPosition::After(after) => Some(after),
                _ => None,
            });
        } else {
            sub.begin(start, snapshot, recovering, &mut self.total);
        }
        add_demand(&mut self.demand, key);
        self.streams.get_mut(key).expect("looked up above").subscribers.push(id);
        self.subs.insert(id, sub);
        if let Some((_, held)) = self.namespace_subs.get_mut(key.namespace()).filter(|_| counted) {
            *held += 1;
        }
        self.check();
        Ok(id)
    }

    /// The relay entry point for state: a snapshot its parent sent after every record it covers, so at or behind this
    /// stream's position. Each subscriber waiting for one starts from it and replays the rest from its source, and
    /// each branch that asked for one gets it next.
    pub fn accept_snapshot(
        &mut self,
        publisher: &PeerId,
        key: &StreamKey,
        snapshot: Snapshot,
    ) -> Result<(), StreamError> {
        self.allowed(publisher, key, Access::Publish)?;
        let stream = self.streams.get(key).ok_or(StreamError::UnknownStream)?;
        let (cut, through) = (stream.cursor.ok_or(RecoveryError::InvalidSnapshot)?, snapshot.through());
        if through.generation != cut.generation {
            return Err(RecoveryError::GenerationMismatch.into());
        }
        if through.sequence > cut.sequence {
            return Err(RecoveryError::InvalidSnapshot.into());
        }
        for id in stream.subscribers.clone() {
            let Some(sub) = self.subs.get_mut(&id) else { continue };
            let Phase::Snapshot(source) = &sub.phase else {
                if sub.awaits_state {
                    self.send_snapshot(id, &snapshot);
                }
                continue;
            };
            let replay = if through == cut {
                Some(None)
            } else {
                let covers = |s: &Arc<dyn RecoverySource>| {
                    s.available_range(key).is_ok_and(|r| r.covers(through.sequence, cut.sequence))
                };
                source.clone().filter(covers).map(Some)
            };
            let Some(replay) = replay else {
                self.terminate(id, StreamEvent::ResetRequired(ResetReason::HistoryUnavailable));
                continue;
            };
            // Nothing else is queued yet: a subscriber without room for it never has any.
            let terminal = match sub.fits(snapshot.bytes(), 0, &self.total, &self.limits) {
                (true, true) => None,
                (false, _) => Some(StreamEvent::End(EndReason::SlowConsumer)),
                (true, false) => Some(StreamEvent::ResetRequired(ResetReason::GlobalCapacity)),
            };
            if let Some(terminal) = terminal {
                self.terminate(id, terminal);
                continue;
            }
            let replay = replay.map(|source| (through.sequence, cut, source));
            sub.begin(Some(through), Some(snapshot.clone()), replay, &mut self.total);
        }
        self.check();
        Ok(())
    }

    /// A branch asks for the state. An origin sends it now, after every record it covers; a relay has none and
    /// answers `true`: the branch gets the next one [`accept_snapshot`](Self::accept_snapshot) takes.
    pub(crate) fn want_snapshot(&mut self, id: u64) -> bool {
        let Some(sub) = self.subs.get_mut(&id) else { return false };
        let Some(stream) = self.streams.get(&sub.key) else { return false };
        if stream.relay {
            sub.awaits_state = true;
            return true;
        }
        if let Some(snapshot) = stream.snapshot() {
            self.send_snapshot(id, &snapshot);
            self.check();
        }
        false
    }

    /// Behind every record the branch holds or replays: the child continues it from its own ring.
    fn send_snapshot(&mut self, id: u64, snapshot: &Snapshot) {
        let Some(sub) = self.subs.get_mut(&id) else { return };
        sub.awaits_state = false;
        if snapshot.bytes() > sub.limits.max_bytes || snapshot.bytes() > self.limits.max_queued_bytes {
            return self.terminate(id, StreamEvent::End(EndReason::SlowConsumer));
        }
        sub.held = Some(Box::new(snapshot.clone()));
        if sub.release_held(&mut self.total, &self.limits) {
            self.terminate(id, StreamEvent::ResetRequired(ResetReason::GlobalCapacity));
        }
    }

    /// Ends every subscription on this node waiting for the key's state, branches included: the state cannot reach it.
    pub(crate) fn end_state_waits(&mut self, key: &StreamKey, reason: EndReason) {
        let waits = |s: &Sub| s.key == *key && (s.awaits_state || matches!(s.phase, Phase::Snapshot(_)));
        self.terminate_where(StreamEvent::End(reason), waits);
    }

    /// One bounded read from the subscription's source. Loop it, polling in between.
    pub fn drive_recovery(&mut self, id: u64, bounds: ReadBounds) -> Result<RecoveryProgress, StreamError> {
        let limits = self.limits;
        let sub = self.subs.get_mut(&id).ok_or(StreamError::UnknownSubscription)?;
        let (next_after, cut, source) = match &sub.phase {
            Phase::Recovering { next_after, cut, source, .. } => (*next_after, *cut, source.clone()),
            Phase::Live => return Ok(RecoveryProgress::Live),
            Phase::Waiting(_) | Phase::Snapshot(_) | Phase::Done => return Err(StreamError::NotRecovering),
        };
        let (room, held) = (|used: usize, max: usize| max.saturating_sub(used), sub.held());
        let branch = ReadBounds {
            max_records: room(held.records, sub.limits.max_records)
                .min(bounds.max_records)
                .min(limits.max_batch_records),
            max_bytes: room(held.bytes, sub.limits.max_bytes).min(bounds.max_bytes).min(limits.max_batch_bytes),
        };
        if branch.max_records == 0 || branch.max_bytes == 0 || held.events >= sub.limits.max_events {
            self.stats.recovery_blocked += 1;
            return Ok(RecoveryProgress::Blocked);
        }
        let bounds = ReadBounds {
            max_records: branch.max_records.min(room(self.total.records, limits.max_queued_records)),
            max_bytes: branch.max_bytes.min(room(self.total.bytes, limits.max_queued_bytes)),
        };
        if bounds.max_records == 0 || bounds.max_bytes == 0 {
            self.stats.recovery_blocked += 1;
            // Waiting would deadlock when retained live batches hold every credit and this
            // branch has nothing to poll that would free one.
            if sub.queue.len() <= sub.tail {
                self.terminate(id, StreamEvent::ResetRequired(ResetReason::GlobalCapacity));
            }
            return Ok(RecoveryProgress::Blocked);
        }
        let after = Cursor { generation: cut.generation, sequence: next_after };
        let pages = match source.read_after(&sub.key, after, cut.sequence, bounds) {
            Ok(pages) => pages,
            Err(RecoveryError::LimitExceeded) => {
                // An empty branch bounded only by its own size can never hold the next record.
                if held.events == 0 && bounds.max_bytes == sub.limits.max_bytes {
                    self.terminate(id, StreamEvent::ResetRequired(ResetReason::SlowConsumer));
                }
                return Err(RecoveryError::LimitExceeded.into());
            }
            Err(error) => {
                let reason = match error {
                    RecoveryError::HistoryUnavailable => ResetReason::HistoryUnavailable,
                    RecoveryError::GenerationMismatch => ResetReason::GenerationChanged,
                    _ => ResetReason::InvalidRecovery,
                };
                self.terminate(id, StreamEvent::ResetRequired(reason));
                return Err(error.into());
            }
        };
        let (mut records, mut reached, mut fault) = (Vec::new(), next_after, None);
        for page in pages {
            let Some(page) = page.after(reached) else { continue };
            if page.generation() != cut.generation || page.last() > cut.sequence {
                fault = Some(ResetReason::InvalidRecovery);
            } else if page.first() != reached + 1 {
                fault = Some(ResetReason::Gap);
            }
            if fault.is_some() {
                break;
            }
            reached = page.last();
            records.extend_from_slice(page.records());
        }
        let bytes: usize = records.iter().map(Payload::len).sum();
        if fault.is_none() && (records.len() > bounds.max_records || bytes > bounds.max_bytes) {
            fault = Some(ResetReason::InvalidRecovery);
        }
        if fault.is_none() && records.is_empty() {
            fault = Some(ResetReason::HistoryUnavailable);
        }
        if let Some(reason) = fault {
            self.terminate(id, StreamEvent::ResetRequired(reason));
            return Ok(RecoveryProgress::Blocked);
        }
        self.stats.replayed_records += records.len() as u64;
        let batch = SequencedBatch::new(cut.generation, next_after + 1, records).expect("non-empty and within the cut");
        let at = sub.queue.len() - sub.tail;
        sub.enqueue(at, StreamEvent::Batch(batch), &mut self.total);
        let progress = if reached == cut.sequence {
            sub.queue.insert(at + 1, StreamEvent::CaughtUp { through: cut });
            (sub.tail, sub.phase) = (0, Phase::Live);
            // CaughtUp is queued ahead of it, so it is never stuck here.
            sub.release_held(&mut self.total, &limits);
            RecoveryProgress::CaughtUp(cut)
        } else {
            if let Phase::Recovering { next_after, .. } = &mut sub.phase {
                *next_after = reached;
            }
            RecoveryProgress::Advanced(Cursor { generation: cut.generation, sequence: reached })
        };
        self.check();
        Ok(progress)
    }

    /// Hands the next event to the host and releases its queue capacity. Polling the terminal
    /// event removes the subscription; after that, and for unknown ids, this is `None`.
    pub fn poll(&mut self, id: u64) -> Option<StreamEvent> {
        self.take(id, false).map(|(event, _)| event)
    }

    /// As [`poll`](Self::poll), but the event's charge stays on the branch, as in flight, until
    /// [`discharge`](Self::discharge) gives it back: the branch's limits count what its child has not applied. The
    /// hub's total does not.
    pub fn poll_charged(&mut self, id: u64) -> Option<(StreamEvent, QueueUsage)> {
        self.take(id, true)
    }

    /// Gives back a charge [`poll_charged`](Self::poll_charged) left in flight, and says whether the subscription has
    /// more to give: an event to poll (a state held for room may have been queued) or history to read. Nothing for a
    /// subscription gone: its charge went with it.
    pub fn discharge(&mut self, id: u64, charge: QueueUsage) -> bool {
        let Some(sub) = self.subs.get_mut(&id) else { return false };
        sub.inflight.sub(charge);
        if sub.release_held(&mut self.total, &self.limits) {
            self.terminate(id, StreamEvent::ResetRequired(ResetReason::GlobalCapacity));
        }
        self.check();
        self.subs.get(&id).is_some_and(|s| s.pollable() || matches!(s.phase, Phase::Recovering { .. }))
    }

    /// What the subscription has in flight, by [`poll_charged`](Self::poll_charged).
    pub fn inflight(&self, id: u64) -> Option<QueueUsage> {
        self.subs.get(&id).map(|s| s.inflight)
    }

    fn take(&mut self, id: u64, charged: bool) -> Option<(StreamEvent, QueueUsage)> {
        let sub = self.subs.get_mut(&id)?;
        if sub.queue.len() > sub.tail {
            let event = sub.queue.pop_front()?;
            sub.release(&event, &mut self.total);
            let charge = if charged { QueueUsage::of(&event) } else { QueueUsage::default() };
            sub.inflight.add(charge);
            if sub.release_held(&mut self.total, &self.limits) {
                self.terminate(id, StreamEvent::ResetRequired(ResetReason::GlobalCapacity));
            }
            if let Some((bytes, records)) = event.charge() {
                self.stats.delivered_bytes += bytes as u64;
                self.stats.delivered_records += records as u64;
            }
            return Some((event, charge));
        }
        let terminal = sub.terminal.take()?;
        self.remove(id);
        Some((terminal, QueueUsage::default()))
    }

    /// `(bytes, records)` of the event `poll` would return; zeros for a control event.
    pub fn next_size(&self, id: u64) -> Option<(usize, usize)> {
        let sub = self.subs.get(&id)?;
        if sub.queue.len() > sub.tail {
            return Some(sub.queue.front()?.charge().unwrap_or_default());
        }
        sub.terminal.as_ref().map(|_| (0, 0))
    }

    /// The subscription's stream, and whether only its terminal event is left to poll.
    pub fn subscription(&self, id: u64) -> Option<(&StreamKey, bool)> {
        self.subs.get(&id).map(|s| (&s.key, matches!(s.phase, Phase::Done)))
    }

    /// Whether `poll` would return an event.
    pub fn pollable(&self, id: u64) -> bool {
        self.subs.get(&id).is_some_and(Sub::pollable)
    }

    pub fn is_recovering(&self, id: u64) -> bool {
        self.subs.get(&id).is_some_and(|s| matches!(s.phase, Phase::Recovering { .. }))
    }

    /// Ends the subscription, leaving `End(Unsubscribed)` to poll.
    pub fn unsubscribe(&mut self, id: u64) {
        self.end(id, EndReason::Unsubscribed);
    }

    pub fn end(&mut self, id: u64, reason: EndReason) {
        self.terminate(id, StreamEvent::End(reason));
    }

    pub(crate) fn reset(&mut self, id: u64, reason: ResetReason) {
        self.terminate(id, StreamEvent::ResetRequired(reason));
    }

    pub fn contains(&self, id: u64) -> bool {
        self.subs.contains_key(&id)
    }

    /// Ends the subscription and frees its slot without the host reading the final event.
    pub fn forget(&mut self, id: u64) {
        self.unsubscribe(id);
        self.remove(id);
    }

    fn remove(&mut self, id: u64) {
        if let Some(sub) = self.subs.remove(&id)
            && sub.peer.node == self.counted
            && let Some((_, held)) = self.namespace_subs.get_mut(sub.key.namespace())
        {
            *held -= 1;
        }
    }

    /// Replaces the session's whole desired set. A retry of the same revision is idempotent;
    /// the same revision with other contents is a conflict.
    pub fn replace_interests(
        &mut self,
        peer: &PeerId,
        revision: u64,
        keys: BTreeSet<StreamKey>,
    ) -> Result<InterestAck, StreamError> {
        let session = self.session(peer)?;
        if revision < session.revision {
            return Err(StreamError::StaleRevision);
        }
        if revision == session.revision && revision != 0 {
            return if session.interests == keys {
                Ok(InterestAck { revision, changed: false })
            } else {
                Err(StreamError::RevisionConflict)
            };
        }
        if keys.len() > self.limits.max_interests_per_session {
            return Err(StreamError::LimitExceeded);
        }
        for key in &keys {
            if !key.valid(self.limits.max_key_bytes) {
                return Err(StreamError::InvalidOptions);
            }
            if !self.authorizer.authorize(peer, key, Access::Subscribe) {
                return Err(StreamError::Forbidden);
            }
        }
        let freed = session.interests.iter().filter(|k| !keys.contains(*k) && self.demand.get(*k) == Some(&1)).count();
        let added = keys.iter().filter(|k| !self.demand.contains_key(*k)).count();
        if self.demand.len() - freed + added > self.limits.max_interest_keys {
            return Err(StreamError::LimitExceeded);
        }
        let Self { sessions, demand, .. } = self;
        let session = sessions.get_mut(&peer.node).expect("checked above");
        let changed = session.interests != keys;
        for key in keys.difference(&session.interests) {
            add_demand(demand, key);
        }
        for key in session.interests.difference(&keys) {
            release_demand(demand, key);
        }
        (session.revision, session.interests) = (revision, keys);
        Ok(InterestAck { revision, changed })
    }

    /// Local subscriptions plus session interests for the key.
    pub fn demand_count(&self, key: &StreamKey) -> usize {
        self.demand.get(key).copied().unwrap_or(0)
    }

    pub fn demanded(&self) -> impl Iterator<Item = &StreamKey> {
        self.demand.keys()
    }

    pub fn queue_usage(&self, id: u64) -> Option<QueueUsage> {
        self.subs.get(&id).map(|s| s.usage)
    }

    /// `(bytes, records)` queued across all branches.
    pub fn total_queued(&self) -> (usize, usize) {
        (self.total.bytes, self.total.records)
    }

    /// Bytes of latest records held in compacted streams' state.
    pub(crate) fn state_bytes(&self) -> usize {
        #[expect(clippy::disallowed_methods, reason = "a sum, the same in any order")]
        self.streams.values().filter_map(|s| s.state.as_ref()).map(|s| s.bytes()).sum()
    }

    /// Resets and ends so far: it grows each time a subscription has one to deliver.
    pub(crate) fn terminals(&self) -> u64 {
        self.stats.resets.iter().chain(&self.stats.ends).sum()
    }

    pub fn stats(&self) -> HubStats {
        #[expect(clippy::disallowed_methods, reason = "a count, the same in any order")]
        let recovering = self.subs.values().filter(|s| matches!(s.phase, Phase::Recovering { .. })).count();
        HubStats {
            streams: self.streams.len(),
            sessions: self.sessions.len(),
            subscriptions: self.subs.len(),
            recovering,
            demand_keys: self.demand.len(),
            queued_bytes: self.total.bytes,
            queued_records: self.total.records,
            ..self.stats.clone()
        }
    }

    // On the function: an attribute on the debug_assert_eq! statement does not reach its expansion.
    #[expect(clippy::disallowed_methods, reason = "sums, the same in any order")]
    fn check(&self) {
        debug_assert_eq!(
            (self.total.bytes, self.total.records),
            self.subs.values().fold((0, 0), |(b, r), s| (b + s.usage.bytes, r + s.usage.records)),
            "global queue accounting drifted from the branches"
        );
    }
}
