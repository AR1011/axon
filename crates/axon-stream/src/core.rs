//! One node of a stream tree, as a state machine: a hub, a history ring, the branches it
//! feeds downstream and the parents it pulls from. No sockets, no clock, no tasks; the
//! driver moves the messages and says what time it is.
//!
//! Every hop keeps the origin's generation and sequence numbers, so any parent can continue
//! any child: a switch subscribes to the new parent after the child's position while the old
//! one still feeds it, and the hub drops the overlap by position. A child never sees a
//! duplicate or a gap, only an explicit reset when nobody holds the history it needs.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axon::{IdHash, Lane};
use bytes::Bytes;

use crate::disk::{DiskRead, DiskStats, DiskStore, FileOp, Files, ReadError, Resume};
use crate::hub::*;
use crate::recovery::*;
use crate::wire;

pub const STATUSES: [&str; 8] = [
    "ok",
    "forbidden",
    "unknown_stream",
    "history_unavailable",
    "generation_mismatch",
    "unavailable",
    "limit_exceeded",
    "loop",
];

/// Where a node pulls from, most preferred first: one list, or one per namespace with `default` for the rest.
#[derive(Clone, Copy)]
pub struct Parents<'a> {
    pub default: &'a [PeerId],
    pub by_namespace: &'a BTreeMap<String, Vec<PeerId>>,
}

static NO_NAMESPACES: BTreeMap<String, Vec<PeerId>> = BTreeMap::new();

impl<'a, T: AsRef<[PeerId]> + ?Sized> From<&'a T> for Parents<'a> {
    fn from(default: &'a T) -> Self {
        Self { default: default.as_ref(), by_namespace: &NO_NAMESPACES }
    }
}

impl Parents<'_> {
    fn for_key(&self, key: &StreamKey) -> &[PeerId] {
        self.by_namespace.get(key.namespace()).map_or(self.default, Vec::as_slice)
    }

    /// Every parent once, in order of first mention.
    fn all(&self) -> Vec<PeerId> {
        let mut all: Vec<PeerId> = Vec::new();
        for peer in self.default.iter().chain(self.by_namespace.values().flatten()) {
            if !all.contains(peer) {
                all.push(peer.clone());
            }
        }
        all
    }
}

/// Whether a parent serves a wanted key, or why not. Indexes [`STATUSES`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Ok,
    Forbidden,
    UnknownStream,
    HistoryUnavailable,
    GenerationMismatch,
    /// The parent has lost its own upstream.
    Unavailable,
    LimitExceeded,
    /// Never sent: what a child makes of an answer whose path names the child, or is [`MAX_PATH`] long.
    Loop,
}

/// The most nodes between an origin and a child, which bounds the depth of a tree.
pub const MAX_PATH: usize = 32;

/// A parent's answer for one wanted key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answer {
    pub status: Status,
    /// The nodes the stream crosses, from its origin to the parent. Empty unless `status` is `Ok`.
    pub path: Vec<Arc<str>>,
}

impl From<Status> for Answer {
    fn from(status: Status) -> Self {
        Self { status, path: Vec::new() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Want {
    pub key: StreamKey,
    /// Names one branch: a new epoch asks the parent to start over from `start`.
    pub epoch: u32,
    /// `Live`, `After` or `First`. Only read when the parent creates the branch.
    pub start: StartPosition,
    /// The branch also carries the stream's state: an [`Item::Snapshot`] behind every record it covers.
    pub snapshot: bool,
}

/// A change to what a child wants from one parent. With neither wants nor drops it only checks `base`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Interest {
    pub revision: u64,
    /// The last revision the child knows this parent applied, 0 when the parent holds nothing for it.
    /// A parent at another revision answers `OutOfSync` and the child sends the whole set again.
    pub base: u64,
    /// `wants` is everything the child wants among the keys in `(from, to]`, `None` being open:
    /// the parent drops what else it holds there. `base` is not checked when `from` is `None`.
    pub whole: Option<(Option<StreamKey>, Option<StreamKey>)>,
    pub wants: Vec<Want>,
    pub drops: Vec<StreamKey>,
}

/// What came back for an [`Interest`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// One per want, in order.
    Answers(Vec<Answer>),
    OutOfSync,
    Failed,
}

impl From<Result<Vec<Answer>, StreamError>> for Reply {
    fn from(result: Result<Vec<Answer>, StreamError>) -> Self {
        match result {
            Ok(answers) => Self::Answers(answers),
            Err(StreamError::OutOfSync) => Self::OutOfSync,
            Err(_) => Self::Failed,
        }
    }
}

impl From<Option<Vec<Answer>>> for Reply {
    fn from(answers: Option<Vec<Answer>>) -> Self {
        answers.map_or(Self::Failed, Self::Answers)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Item {
    /// Where the branch starts, once the parent knows its own position.
    Ready(Cursor),
    Batch(SequencedBatch),
    CaughtUp,
    Reset(ResetReason),
    End(EndReason),
    /// The nodes the stream now crosses down to the parent, as in an [`Answer`].
    Path(Vec<Arc<str>>),
    Snapshot(Snapshot),
}

/// Safe to deliver more than once: batches are deduplicated by position.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Push {
    pub items: Vec<(StreamKey, u32, Item)>,
}

/// One push to a downstream node: its channel, its number on the channel from 1, and the go-backs of the channel
/// before it went out, so an answer to a push sent before a go-back does not go back again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushId {
    pub channel: u64,
    pub seq: u64,
    pub(crate) round: u64,
}

/// What a child answers a push.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushAnswer {
    /// The push was the next of its channel and was applied: `applied` is its seq. `unwanted` indexes its items whose
    /// branch the child no longer wants.
    Applied { applied: u64, unwanted: Vec<u16> },
    /// Not applied: the child had it already, or lacks one before it. `applied` is the last it applied.
    Skipped { applied: u64 },
    /// The push is of a parent boot or channel the child no longer follows, or of a channel it never followed.
    Retired,
}

/// The reasons a child does not apply a push, by [`RelayStats::push_skipped`].
pub const PUSH_SKIPPED: [&str; 3] = ["duplicate", "gap", "retired"];
const DUPLICATE: usize = 0;
const GAP: usize = 1;
const RETIRED: usize = 2;

/// The most pushes in flight on one channel.
pub const MAX_PUSHES_IN_FLIGHT: usize = 64;

/// Calls every channel's pushes past its first may hold at once: a quarter of a cluster's default `max_pending`.
pub const PUSH_SLOTS: usize = 1024;

/// The earlier boots of one parent a child keeps, so their late pushes are refused.
const MAX_RETIRED: usize = 4;

/// Delivery-rate samples one channel keeps.
const MAX_RATES: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Wake {
    Local(u64),
    /// The downstream node has events to push on the lane.
    Peer(Arc<str>, Lane),
    Reconcile,
}

/// How long a pulled key waits on its parents. Per key, so per namespace: a namespace whose origins are far away waits
/// longer than one beside them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RelayTimings {
    /// After this long without a parent, the key's downstream branches end with `UpstreamLost` and its followers are
    /// told `OriginLost`. A key whose every parent refuses its position waits this long after the newest appeared.
    pub upstream_timeout: Duration,
    /// How long a better parent must stay available before a key moves back to it.
    pub hold_down: Duration,
}

impl Default for RelayTimings {
    fn default() -> Self {
        Self { upstream_timeout: Duration::from_secs(3), hold_down: Duration::from_secs(10) }
    }
}

#[derive(Clone, Debug)]
pub struct CoreConfig {
    pub hub: HubLimits,
    pub store: MemoryLimits,
    /// Smaller or larger history rings for some namespaces: with many streams, the rings are the memory.
    /// A ring smaller than a batch keeps the batch's tail.
    pub rings: Vec<RingLimits>,
    /// Limits of some namespaces. This core holds their subscriptions and ring bytes; a `StreamNode` holds the rest
    /// through its `quotas`.
    pub namespaces: BTreeMap<String, NamespaceLimits>,
    /// Queue bounds of each downstream branch.
    pub branch: BranchLimits,
    /// Whether this node pulls streams it does not originate from a parent. A `StreamNode` sets it from its tiers.
    pub relay: bool,
    /// The timings of every namespace without its own in `timings_by_namespace`.
    pub timings: RelayTimings,
    pub timings_by_namespace: BTreeMap<String, RelayTimings>,
    /// How long an upstream interest outlives its last listener.
    pub linger: Duration,
    /// How long a parent that refused a stream is left alone; doubles per refusal, up to 32 times this.
    pub retry_refused: Duration,
    pub retry_failed: Duration,
    /// How often a parent with nothing to be told is asked whether it still holds this node's set.
    pub refresh: Duration,
    /// How often the whole wanted set is sent again, which repairs whatever the two sides disagree on.
    pub resync: Duration,
    /// The most wants and drops in one interest; a larger change goes out in several.
    pub max_keys_per_interest: usize,
    /// The largest encoded push. It must fit the smallest `max_frame` of the sessions it crosses
    /// and hold the largest batch the hub admits.
    pub max_push_bytes: usize,
    /// The most batches in one push; a branch must hold this many events. A push also carries at most
    /// `hub.max_batch_records` records of each stream. The child hands each push to its subscribers at once: the
    /// default `BranchLimits` hold what the default config has in flight of one stream.
    pub max_push_items: usize,
    /// The most bytes in flight to one child: its window grows from `2 × max_push_bytes` to three times its delivery
    /// rate times the round trip, up to this. At least `max_push_bytes`.
    pub max_push_window: usize,
    /// The most bytes in flight to every child together, past each one's first push, shared fairly between the busy
    /// ones. At least `max_push_window`.
    pub max_push_inflight: usize,
    /// The lane each namespace's pushes go on, `Live` for a namespace not named: a channel, its window and its pushes
    /// are per child and lane, and a push carries the namespaces of one lane only.
    pub lanes: BTreeMap<String, Lane>,
    /// The largest encoded interest, where under `max_push_bytes`: `StreamNode::start` makes it Control's `max_frame`.
    /// It must hold three of the largest wants.
    pub max_interest_bytes: usize,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            hub: HubLimits { max_batch_bytes: 512 << 10, ..Default::default() },
            store: MemoryLimits { max_streams: 4096, ..Default::default() },
            rings: Vec::new(),
            namespaces: BTreeMap::new(),
            branch: BranchLimits::BRANCH,
            relay: true,
            timings: RelayTimings::default(),
            timings_by_namespace: BTreeMap::new(),
            linger: Duration::from_secs(5),
            retry_refused: Duration::from_secs(2),
            retry_failed: Duration::from_millis(500),
            refresh: Duration::from_secs(2),
            resync: Duration::from_secs(60),
            max_keys_per_interest: 4096,
            max_push_bytes: (1 << 20) - 64,
            max_push_items: 1024,
            max_push_window: 64 << 20,
            max_push_inflight: 256 << 20,
            lanes: BTreeMap::new(),
            max_interest_bytes: usize::MAX,
        }
    }
}

impl CoreConfig {
    fn timings(&self, key: &StreamKey) -> RelayTimings {
        self.timings_by_namespace.get(key.namespace()).copied().unwrap_or(self.timings)
    }

    /// The lane of `key`'s pushes.
    pub fn lane(&self, key: &StreamKey) -> Lane {
        self.lanes.get(key.namespace()).copied().unwrap_or(Lane::Live)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ParentStats {
    pub node: String,
    pub keys: usize,
    pub joining: usize,
}

/// One child's channel on one lane.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct DownstreamStats {
    pub node: String,
    pub lane: Lane,
    /// Of this lane's namespaces.
    pub branches: usize,
    /// Queued for this node across its branches of this lane, past what is in flight.
    pub queued_bytes: usize,
    /// Record payload pushed to this node since it last joined.
    pub pushed_bytes: u64,
    pub pushed_records: u64,
    /// The channel its pushes go out on, from 1 for each boot of this node.
    pub channel: u64,
    /// The bytes it may have in flight, as last worked out.
    pub window_bytes: usize,
    pub inflight_bytes: usize,
    pub inflight_pushes: usize,
    /// The round trip to it the driver last gave.
    pub round_trip_us: Option<u64>,
    /// Bytes a second, the largest recent sample.
    pub delivery_rate: u64,
    /// Pushes sent to it again after a failure or a gap.
    pub resent: u64,
}

/// One stream on one node, for tooling. Never sampled: ask for it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct StreamInfo {
    pub namespace: String,
    pub name: String,
    /// `None` until a pulled stream learns its position.
    pub generation: Option<u64>,
    pub sequence: Option<u64>,
    pub origin: bool,
    pub parent: Option<String>,
    pub joining: Option<String>,
    /// The nodes the stream crosses to reach this one, origin first. Empty until the parent has answered.
    pub path: Vec<String>,
    /// Parents left alone for now, each with its answer: `relay-1 answered loop`.
    pub refused: Vec<String>,
    pub local_subscribers: usize,
    pub downstream_branches: usize,
}

/// Something that happened to a pulled stream or a downstream node.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct RelayEvent {
    pub time_ms: u64,
    /// One of parent_chosen, parent_lost, parent_restored, switched, refused, reset, orphaned, downstream_expired.
    pub kind: &'static str,
    /// Empty for an event about a node rather than a stream.
    pub stream: String,
    pub detail: String,
}

const MAX_EVENTS: usize = 256;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct RelayStats {
    pub hub: HubStats,
    /// Streams this node publishes.
    pub originated: usize,
    /// Streams pulled from a parent.
    pub feeds: usize,
    /// Pulled streams with no parent right now.
    pub orphaned: usize,
    pub parents: Vec<ParentStats>,
    pub downstream: Vec<DownstreamStats>,
    /// A parent went away under a stream.
    pub parents_lost: u64,
    /// A stream finished moving to a better parent.
    pub switches: u64,
    pub interests_sent: u64,
    pub interests_failed: u64,
    /// Wants and drops sent in those interests.
    pub interest_keys: u64,
    /// A parent did not hold the revision a change was built on, so the whole set went out again.
    pub resyncs: u64,
    /// Record payload held in this node's history rings and compacted streams' state.
    pub retained_bytes: usize,
    /// By [`STATUSES`].
    pub refusals: [u64; 8],
    pub pushes_in: u64,
    pub pushes_out: u64,
    pub push_bytes_in: u64,
    pub push_bytes_out: u64,
    pub unwanted_items: u64,
    /// Pushes sent again after a failure or a gap.
    pub push_resent: u64,
    /// Pushes this node, as a child, did not apply, by [`PUSH_SKIPPED`].
    pub push_skipped: [u64; 3],
    /// Times a channel had to wait for the node-wide cap, `max_push_inflight` or [`PUSH_SLOTS`].
    pub push_capped: u64,
    /// A pulled stream broke continuity for its local subscribers.
    pub local_resets: u64,
    /// The history this node keeps on disk for the streams it originates, with a `StreamNodeConfig::disk`.
    pub disk: Option<DiskStats>,
    pub downstream_expired: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Link {
    peer: PeerId,
    epoch: u32,
}

struct Refusal {
    until: Instant,
    status: Status,
    /// The doublings of `retry_refused` the next refusal waits.
    strikes: u32,
    /// The position refused.
    at: Option<Cursor>,
}

#[derive(Default)]
struct Feed {
    current: Option<Link>,
    /// A better parent catching up while `current` still feeds the stream.
    joining: Option<Link>,
    refused: BTreeMap<PeerId, Refusal>,
    idle_since: Option<Instant>,
    orphaned_since: Option<Instant>,
    orphan_told: bool,
    /// The followers were told the origin is lost, and no parent has fed the key since.
    lost_told: bool,
    /// When the membership last changed, until a parent refuses the key as unavailable or unknown.
    view_changed: Option<Instant>,
    /// The earliest of `wants`, which the parents are asked from while this node has no position for the stream.
    wanted_from: StartPosition,
    /// The start each subscription and branch on this node asked for, by id, while it had no position.
    wants: BTreeMap<u64, StartPosition>,
    /// A new generation is asked from its record 1 on top of `wanted_from`, until a parent cannot give that.
    fresh: bool,
    /// This node gave up the key's history after taking a generation: a newer one is taken from its record 1 too.
    gave_up: bool,
    /// The newest generation this node took up. A parent that shows an older one is behind the origin.
    generation: u64,
    /// The path each live link's parent last gave, by the link's epoch.
    paths: BTreeMap<u32, Vec<Arc<str>>>,
    /// The upstream path the children were last told.
    told: Vec<Arc<str>>,
    /// Something on this node waits for the state: each new branch asks for it until one arrives.
    wants_snapshot: bool,
}

impl Feed {
    /// The path the current parent last gave, origin first.
    fn upstream(&self) -> &[Arc<str>] {
        self.current.as_ref().and_then(|l| self.paths.get(&l.epoch)).map_or(&[], |path| path)
    }

    fn links(&self) -> impl Iterator<Item = &Link> {
        self.current.iter().chain(&self.joining)
    }
}

/// The path a child of this node is told: the one this node pulls over, then this node.
fn path_through(feed: Option<&Feed>, me: &Arc<str>) -> Vec<Arc<str>> {
    feed.map_or(&[][..], Feed::upstream).iter().chain([me]).cloned().collect()
}

struct Sent {
    revision: u64,
    wants: Vec<(StreamKey, u32)>,
    drops: Vec<StreamKey>,
    whole: Option<(Option<StreamKey>, Option<StreamKey>)>,
}

#[derive(Default)]
struct Parent {
    /// The branches this parent is known to hold, by epoch.
    acked: BTreeMap<StreamKey, u32>,
    base: u64,
    /// Keys whose link to this parent may differ from `acked`.
    pending: BTreeSet<StreamKey>,
    /// While the whole set is going out again: the last key sent so far.
    sync: Option<Option<StreamKey>>,
    inflight: Option<Sent>,
    retry_at: Option<Instant>,
    refresh_at: Option<Instant>,
    resync_at: Option<Instant>,
}

struct Downstream {
    peer: PeerId,
    revision: u64,
    branches: BTreeMap<StreamKey, (u32, u64)>,
    /// When its last interest arrived.
    heard: Instant,
    /// By lane, a channel for each lane it has had a branch on, kept while the child is.
    lanes: [Option<Channel>; 3],
}

impl Downstream {
    fn lane(&mut self, lane: Lane) -> Option<&mut Channel> {
        self.lanes[lane as usize].as_mut()
    }

    fn each(&self) -> impl Iterator<Item = (Lane, &Channel)> {
        [Lane::Control, Lane::Live, Lane::Bulk].into_iter().zip(&self.lanes).filter_map(|(l, c)| Some((l, c.as_ref()?)))
    }

    /// Its pumps, one per lane.
    fn wakes(&self) -> impl Iterator<Item = Wake> {
        self.each().map(|(lane, _)| Wake::Peer(self.peer.node.clone(), lane))
    }
}

/// The pushes to one child on one lane.
#[derive(Default)]
struct Channel {
    /// Of the child's branches, those of this lane's namespaces, by subscription id.
    branches: BTreeSet<u64>,
    /// Of `branches`, those that may have something to push.
    ready: BTreeSet<u64>,
    /// Where the next push starts, so a push that fills up does not starve the branches after it.
    resume: u64,
    /// Streams whose path changed since this child was told.
    paths: BTreeSet<StreamKey>,
    pushed_bytes: u64,
    pushed_records: u64,
    channel: u64,
    acked: u64,
    /// The pushes the child has not acknowledged, from seq `acked + 1` on.
    unacked: VecDeque<Unacked>,
    /// Of `unacked`, how many went out since the last go-back: the rest go out again first.
    sent: usize,
    /// Go-backs so far.
    round: u64,
    /// Encoded bytes in `unacked`.
    inflight: usize,
    /// Encoded bytes of the pushes the child acknowledged.
    delivered: u64,
    /// Delivery-rate samples in bytes a second, each with when it was taken, the largest first: each one is larger than
    /// every later one.
    rates: VecDeque<(Instant, u64)>,
    rtt: Option<Duration>,
    window: usize,
    /// Since when the oldest unacknowledged push has failed.
    failing: Option<Instant>,
    resent: u64,
    /// The hub's resets and ends when the last push went, under half full with all there was: what is published while
    /// it is in flight waits for an answer and goes in one push, not a call for each publish, unless a reset or an end
    /// comes.
    drained: Option<u64>,
}

impl Channel {
    /// Every unacknowledged push goes out again, in order, and the window starts again from its floor.
    fn go_back(&mut self) {
        (self.sent, self.round) = (0, self.round + 1);
        self.rates.clear();
    }

    /// Bytes and pushes in flight past the first, which the node-wide cap counts.
    fn past_first(&self) -> (usize, usize) {
        let first = self.unacked.front().map_or(0, |push| push.bytes.len());
        (self.inflight - first, self.unacked.len().saturating_sub(1))
    }

    /// Has a push in flight or perhaps one to send.
    fn busy(&self) -> bool {
        !self.unacked.is_empty() || !self.ready.is_empty() || !self.paths.is_empty()
    }

    /// A sample on the answer to a push sent at `at`, when `delivered` bytes had been acknowledged.
    fn sample(&mut self, now: Instant, at: Instant, delivered: u64) {
        let Some(elapsed) = now.checked_duration_since(at).filter(|e| !e.is_zero()) else { return };
        let rate = (u128::from(self.delivered - delivered) * 1_000_000_000 / elapsed.as_nanos()) as u64;
        while self.rates.back().is_some_and(|(_, r)| *r <= rate) {
            self.rates.pop_back();
        }
        if self.rates.len() == MAX_RATES {
            self.rates.pop_front();
        }
        self.rates.push_back((now, rate));
    }

    /// Three times the delivery rate times the round trip, from twice the largest push up to `max_push_window`. The
    /// rate is the largest sample of the last ten round trips, or the last second if that is longer. A lost packet
    /// holds a push's answer about one round trip more, so at twice the rate a window under loss never grows
    /// (spec/push-window.md §2.4).
    fn window(&mut self, cfg: &CoreConfig, now: Instant) -> usize {
        let floor = (2 * cfg.max_push_bytes).min(cfg.max_push_window);
        let Some(rtt) = self.rtt.filter(|rtt| !rtt.is_zero()) else { return floor };
        let horizon = rtt.saturating_mul(10).max(Duration::from_secs(1));
        while self.rates.front().is_some_and(|(at, _)| now.saturating_duration_since(*at) > horizon) {
            self.rates.pop_front();
        }
        let rate = self.rates.front().map_or(0, |(_, rate)| *rate);
        let want = 3 * u128::from(rate) * rtt.as_nanos() / 1_000_000_000;
        usize::try_from(want).unwrap_or(usize::MAX).clamp(floor, cfg.max_push_window)
    }
}

/// A push sent and not yet acknowledged: sent again, byte for byte, until it is.
struct Unacked {
    bytes: Bytes,
    /// Each item's key and epoch, for an `unwanted` answer.
    items: Vec<(StreamKey, u32)>,
    /// What it took of each branch, by subscription id.
    charges: Vec<(u64, QueueUsage)>,
    sent_at: Instant,
    /// The channel's `delivered` when it was sent.
    delivered: u64,
}

/// The channels a child follows of one parent's pushes in its latest boot.
struct Followed {
    boot: u128,
    /// By lane, the channel and the last push applied of it.
    lanes: [Option<(u64, u64)>; 3],
    /// Earlier boots of the parent, oldest first: their pushes are refused on every lane.
    retired: VecDeque<u128>,
}

/// Whether `start` asks for records that `than` does not.
fn earlier(start: StartPosition, than: StartPosition) -> bool {
    match (start, than) {
        (StartPosition::First, StartPosition::First) => false,
        (StartPosition::First, _) | (StartPosition::After(_), StartPosition::Live) => true,
        (StartPosition::After(a), StartPosition::After(b)) => a.generation == b.generation && a.sequence < b.sequence,
        _ => false,
    }
}

pub struct RelayCore<A: Authorizer = AllowAll> {
    cfg: CoreConfig,
    me: Arc<str>,
    local: PeerId,
    hub: StreamHub<A>,
    store: Arc<Mutex<MemoryStore>>,
    /// Keeps the history of the streams it originates in the store's namespaces on disk.
    pub(crate) disk: Option<DiskStore>,
    originated: HashSet<StreamKey, IdHash>,
    // Ordered maps keep a run reproducible from its inputs.
    feeds: BTreeMap<StreamKey, Feed>,
    parents: BTreeMap<PeerId, Parent>,
    downstream: BTreeMap<Arc<str>, Downstream>,
    /// The next downstream's channel: from 1 for each boot of this node, never the same twice.
    next_channel: u64,
    /// Downstream channels that waited for the node-wide cap, woken when it frees.
    capped: BTreeSet<(Arc<str>, Lane)>,
    /// The channels this node follows of each parent's pushes, until the parent leaves the view.
    channels: BTreeMap<Arc<str>, Followed>,
    /// The downstream nodes holding a branch of each stream, with the branch's subscription id.
    fed: HashMap<StreamKey, Vec<(Arc<str>, u64)>, IdHash>,
    watchers: HashMap<StreamKey, Vec<u64>, IdHash>,
    available_since: BTreeMap<PeerId, Instant>,
    candidates: Vec<PeerId>,
    by_namespace: BTreeMap<String, Vec<PeerId>>,
    /// Pulled streams to look at in the next `reconcile`, and when to look at one again.
    dirty: BTreeSet<StreamKey>,
    timers: BTreeSet<(Instant, StreamKey)>,
    next_epoch: u32,
    revision: u64,
    wakes: Vec<Wake>,
    stats: RelayStats,
    events: VecDeque<(Instant, &'static str, String, String)>,
    /// What a cluster's timeline keeps, until [`take_timeline`](Self::take_timeline): per kind, the first stream, its
    /// detail and how many streams. One entry per kind, so a relay crash under a thousand streams is one event.
    timeline: BTreeMap<&'static str, (String, String, u64)>,
}

impl<A: Authorizer> RelayCore<A> {
    /// The authorizer sees downstream nodes by node id and this process as `local:<node>`.
    pub fn new(me: &PeerId, authorizer: A, cfg: CoreConfig) -> Result<Self, StreamError> {
        let h = &cfg.hub;
        // Any one batch the hub admits must fit a ring, a branch and an otherwise empty push.
        let largest =
            h.max_batch_bytes + 4 * h.max_batch_records + h.max_key_bytes + wire::PUSH_HEADER + wire::ITEM_FIXED;
        if cfg.rings.iter().any(|r| r.max_records == 0 || r.max_bytes == 0)
            || cfg.store.max_records_per_stream < h.max_batch_records
            || cfg.store.max_bytes_per_stream < h.max_batch_bytes
            || cfg.store.max_streams < h.max_streams
            || cfg.branch.max_records < h.max_batch_records
            || cfg.branch.max_bytes < h.max_batch_bytes
            || cfg.max_push_bytes < largest
            || cfg.max_push_window < cfg.max_push_bytes
            || cfg.max_push_inflight < cfg.max_push_window
            || !(1..=1 << 16).contains(&cfg.max_push_items)
            || cfg.branch.max_events < cfg.max_push_items
            || cfg.max_keys_per_interest == 0
            || cfg.max_push_bytes.min(cfg.max_interest_bytes)
                < wire::INTEREST_HEADER + 3 * (wire::WANT_HEADER + h.max_key_bytes)
            || h.max_key_bytes > u16::MAX as usize
        {
            return Err(StreamError::InvalidOptions);
        }
        let local = PeerId::new(&format!("local:{}", me.node), me.incarnation);
        let mut hub = StreamHub::new(authorizer, cfg.hub)?.with_namespaces(&local, &cfg.namespaces);
        hub.open_session(&local)?;
        let store = MemoryStore::new(cfg.store)
            .map_err(|_| StreamError::InvalidOptions)?
            .with_rings(cfg.rings.clone())
            .with_namespaces(&cfg.namespaces);
        Ok(Self {
            cfg,
            me: me.node.clone(),
            local,
            hub,
            store: Arc::new(Mutex::new(store)),
            disk: None,
            originated: HashSet::default(),
            feeds: BTreeMap::new(),
            parents: BTreeMap::new(),
            downstream: BTreeMap::new(),
            next_channel: 1,
            capped: BTreeSet::new(),
            channels: BTreeMap::new(),
            fed: HashMap::default(),
            watchers: HashMap::default(),
            available_since: BTreeMap::new(),
            candidates: Vec::new(),
            by_namespace: BTreeMap::new(),
            dirty: BTreeSet::new(),
            timers: BTreeSet::new(),
            next_epoch: 1,
            revision: 0,
            wakes: Vec::new(),
            stats: RelayStats::default(),
            events: VecDeque::new(),
            timeline: BTreeMap::new(),
        })
    }

    pub fn take_wakes(&mut self) -> Vec<Wake> {
        std::mem::take(&mut self.wakes)
    }

    fn wake_key(&mut self, key: &StreamKey) {
        self.wakes.extend(self.watchers.get(key).into_iter().flatten().map(|id| Wake::Local(*id)));
        let Some(fed) = self.fed.get(key) else { return };
        let lane = self.cfg.lane(key);
        for (node, id) in fed {
            if let Some(channel) = self.downstream.get_mut(node).and_then(|d| d.lane(lane)) {
                channel.ready.insert(*id);
            }
            self.wakes.push(Wake::Peer(node.clone(), lane));
        }
    }

    /// Has `reconcile` look at a pulled stream whose demand or links changed.
    fn touch(&mut self, key: &StreamKey) {
        if self.feeds.contains_key(key) && self.dirty.insert(key.clone()) {
            self.wakes.push(Wake::Reconcile);
        }
    }

    /// Each node this one feeds the key asks again from its own position.
    fn rebranch_children(&mut self, key: &StreamKey) {
        for (_, id) in self.fed.get(key).into_iter().flatten() {
            self.hub.reset(*id, ResetReason::Gap);
        }
    }

    fn unfeed(&mut self, key: &StreamKey) {
        self.feeds.remove(key);
        for parent in self.parents.values_mut().filter(|p| p.acked.contains_key(key)) {
            parent.pending.insert(key.clone());
        }
        self.wakes.push(Wake::Reconcile);
    }

    fn drop_branch(&mut self, node: &str, key: &StreamKey) {
        let Some((_, id)) = self.downstream.get_mut(node).and_then(|d| d.branches.remove(key)) else { return };
        self.forget_branch(node, key, id);
    }

    fn forget_branch(&mut self, node: &str, key: &StreamKey, id: u64) {
        if let Some(channel) = self.downstream.get_mut(node).and_then(|d| d.lane(self.cfg.lane(key))) {
            channel.ready.remove(&id);
            channel.branches.remove(&id);
        }
        if let Some(nodes) = self.fed.get_mut(key) {
            nodes.retain(|(_, i)| *i != id);
            if nodes.is_empty() {
                self.fed.remove(key);
            }
        }
        self.hub.forget(id);
        self.want_from(key, None);
        self.touch(key);
    }

    /// Its pumps are woken: parked, they find no channel and end.
    fn drop_downstream(&mut self, node: &str) {
        let Some(entry) = self.downstream.remove(node) else { return };
        for (key, (_, id)) in &entry.branches {
            self.forget_branch(node, key, *id);
        }
        if entry.each().any(|(_, c)| !c.unacked.is_empty()) {
            self.wake_capped();
        }
        self.wakes.extend(entry.wakes());
    }

    /// The channels that waited for the node-wide cap try again.
    fn wake_capped(&mut self) {
        self.wakes.extend(std::mem::take(&mut self.capped).into_iter().map(|(node, lane)| Wake::Peer(node, lane)));
    }

    fn note(&mut self, now: Instant, kind: &'static str, key: Option<&StreamKey>, detail: String) {
        if self.events.len() == MAX_EVENTS {
            self.events.pop_front();
        }
        let stream = key.map(ToString::to_string).unwrap_or_default();
        if matches!(kind, "parent_lost" | "reset" | "orphaned" | "parent_restored") {
            self.timeline.entry(kind).or_insert_with(|| (stream.clone(), detail.clone(), 0)).2 += 1;
        }
        self.events.push_back((now, kind, stream, detail));
    }

    // ---- origin ----

    pub fn open_stream(&mut self, key: &StreamKey, generation: u64, policy: DeliveryPolicy) -> Result<(), StreamError> {
        self.open_read(key, generation, policy, None)
    }

    /// What an open of the key reads from disk first, without the core: `None` when it reads nothing.
    pub(crate) fn disk_read(
        &self,
        key: &StreamKey,
        generation: u64,
        policy: DeliveryPolicy,
    ) -> Result<Option<DiskRead>, StreamError> {
        if self.originated.contains(key) {
            return Err(StreamError::AlreadyExists);
        }
        match self.disk.as_ref().filter(|d| d.holds(key)) {
            Some(disk) => disk.reader(key, generation, policy, lock(&self.store).ring_limits(key)),
            None => Ok(None),
        }
    }

    /// [`open_stream`](Self::open_stream) with what [`disk_read`](Self::disk_read) asked for, read. A read is dropped
    /// when the disk no longer holds the key unopened: another open took it meanwhile.
    pub(crate) fn open_read(
        &mut self,
        key: &StreamKey,
        generation: u64,
        policy: DeliveryPolicy,
        read: Option<Result<Resume, ReadError>>,
    ) -> Result<(), StreamError> {
        // Opening a key this node pulls makes it the origin: the producer moved here, after a crash its old node
        // never closed the key, or followers of a key this node closed, its own or its children's, made it pull the
        // key. A node that may not publish the key keeps relaying it.
        self.hub.allowed(&self.local, key, Access::Publish)?;
        // The whole state goes to a child in one push item and waits in one branch. Refused before a key this node
        // pulls is let go, as the hub would refuse it after.
        if let DeliveryPolicy::Compacted(l) = policy {
            let max = self.cfg.max_push_bytes.min(self.cfg.branch.max_bytes);
            // Each bound alone first, so the sum cannot overflow.
            if l.max_keys == 0
                || l.max_bytes == 0
                || l.max_keys > max
                || l.max_bytes > max
                || wire::item_len(key, l.max_bytes, l.max_keys) + wire::PUSH_HEADER > max
            {
                return Err(StreamError::InvalidOptions);
            }
        }
        let pulled = self.feeds.contains_key(key);
        if pulled {
            self.wake_key(key);
            // Not a close to the children: the same generation resumed from disk goes on where each was, and another
            // is answered as one.
            self.rebranch_children(key);
            let _ = self.hub.close_stream(&self.local, key, EndReason::StreamClosed);
            lock(&self.store).remove(key);
            self.unfeed(key);
        }
        let opened = self.originate(key, generation, policy, read);
        if opened.is_err() && pulled {
            // A refused open leaves the key pulled for its followers, as a close does.
            self.repull(key);
        }
        opened
    }

    fn originate(
        &mut self,
        key: &StreamKey,
        generation: u64,
        policy: DeliveryPolicy,
        read: Option<Result<Resume, ReadError>>,
    ) -> Result<(), StreamError> {
        if self.hub.has_stream(key) {
            return Err(StreamError::AlreadyExists);
        }
        // The disk holds what it held when `read` was asked for until an open takes it.
        let read = self.disk_read(key, generation, policy)?.map(|_| read.expect("the disk only gives up keys it held"));
        let resume =
            read.map(|r| r.map_err(|e| self.disk.as_mut().expect("read from it").read_failed(e))).transpose()?;
        let mut store = lock(&self.store);
        store.remove(key);
        store.open_origin(key, Cursor { generation, sequence: resume.as_ref().map_or(0, |r| r.first - 1) })?;
        if let Some(r) = &resume {
            // A resumed state is held to the namespace's budget as a published one: past it, the open is refused.
            let state = r.latest.iter().map(Payload::len).sum();
            if store.ring_room(key).is_some_and(|room| state > room) {
                store.remove(key);
                drop(store);
                return Err(self.hub.refuse(key, NamespaceLimit::RingBytes, None));
            }
            store.hold_state(key, state);
            for batch in &r.batches {
                let _ = store.append(key, batch);
            }
        }
        drop(store);
        // Followers hear of the open last, so a refusal shows them nothing.
        let opened = match &resume {
            Some(r) => self.hub.resume_stream(&self.local, key, r.through, policy, &r.latest),
            None => self.hub.open_stream(&self.local, key, generation, policy),
        };
        if let Err(error) = opened {
            lock(&self.store).remove(key);
            return Err(error);
        }
        if let Some(disk) = self.disk.as_mut().filter(|d| d.holds(key)) {
            disk.open_stream(key, generation, policy, resume.as_ref().map_or(&[], |r| &r.latest));
        }
        self.originated.insert(key.clone());
        Ok(())
    }

    pub fn publish(&mut self, key: &StreamKey, records: Vec<Payload>) -> Result<Cursor, PublishError<Vec<Payload>>> {
        self.publish_admitted(key, records, None, |_| Ok(()))
    }

    /// Publishes once the batch has passed every check of the hub and the ring, and then `admit`.
    pub(crate) fn publish_admitted(
        &mut self,
        key: &StreamKey,
        records: Vec<Payload>,
        trace: Option<(u64, u64)>,
        admit: impl FnOnce(&[Payload]) -> Result<(), StreamError>,
    ) -> Result<Cursor, PublishError<Vec<Payload>>> {
        if !self.originated.contains(key) {
            return Err(PublishError { error: StreamError::UnknownStream, returned: records });
        }
        // The hub never reads the store while it publishes.
        let mut store = lock(&self.store);
        let room = store.ring_room(key);
        let mut held = None;
        let admit = |records: &[Payload], state: Option<usize>| {
            // The records and the compacted state they leave fit what the namespace's ring budget leaves: the state
            // is memory this node holds for the namespace too, and cannot give up its oldest.
            if room.is_some_and(|room| records.iter().map(Payload::len).sum::<usize>() + state.unwrap_or(0) > room) {
                let (namespace, limit) = (key.namespace().into(), NamespaceLimit::RingBytes);
                return Err(StreamError::QuotaExceeded { namespace, limit, retry_after: None });
            }
            admit(records)?;
            held = state;
            Ok(())
        };
        // Held until it is synced: `disk_written` hands it on. The state it would leave is charged at once, as it is
        // held from now.
        let published = match self.disk.as_mut().filter(|d| d.holds(key)) {
            Some(disk) => match self.hub.check_publish(&self.local, key, &records) {
                Ok(()) => disk.admit(key, records, trace, admit).inspect_err(|e| self.hub.count_rejected(&e.error)),
                Err(error) => Err(PublishError { error, returned: records }),
            }
            .map(|cursor| (cursor, None)),
            None => self.hub.publish_admitted(&self.local, key, records, trace, admit).map(|b| (b.cursor(), Some(b))),
        };
        // Set only by an admit that passed, the last check of either.
        if let Some(state) = held {
            store.hold_state(key, state);
        }
        let (cursor, batch) = published?;
        if let Some(batch) = batch {
            let _ = store.append(key, &batch);
            drop(store);
            self.wake_key(key);
        }
        Ok(cursor)
    }

    pub(crate) fn originates(&self, key: &StreamKey) -> bool {
        self.originated.contains(key)
    }

    pub(crate) fn limits(&self, key: &StreamKey) -> Option<NamespaceLimits> {
        self.cfg.namespaces.get(key.namespace()).copied()
    }

    /// Counts a refusal at a limit this core does not hold itself.
    pub(crate) fn over_quota(&mut self, key: &StreamKey, limit: NamespaceLimit, retry_after: Duration) -> StreamError {
        self.hub.refuse(key, limit, Some(retry_after))
    }

    pub fn restart_stream(&mut self, key: &StreamKey, generation: u64) -> Result<(), StreamError> {
        if !self.originated.contains(key) {
            return Err(StreamError::UnknownStream);
        }
        self.hub.restart_stream(&self.local, key, generation)?;
        lock(&self.store).restart(key, generation)?;
        if let Some(disk) = &mut self.disk {
            disk.restart(key, generation);
        }
        self.wake_key(key);
        Ok(())
    }

    pub fn close_stream(&mut self, key: &StreamKey) -> Result<(), StreamError> {
        if !self.originated.remove(key) {
            return Err(StreamError::UnknownStream);
        }
        self.end_stream(key)?;
        if let Some(disk) = &mut self.disk {
            disk.close(key);
        }
        Ok(())
    }

    /// Closes the key's stream here, whether this node originated or pulled it.
    fn end_stream(&mut self, key: &StreamKey) -> Result<(), StreamError> {
        self.wake_key(key);
        self.hub.close_stream(&self.local, key, EndReason::StreamClosed)?;
        lock(&self.store).remove(key);
        self.repull(key);
        Ok(())
    }

    /// For a key this node has no stream of: local followers that still want it wait on an empty relay stream for it
    /// to be opened again, here or on another node. It is asked of the parents at the refusal backoff, and from its
    /// record 1, as the sequence domain died with the close and the next generation may be any number.
    fn repull(&mut self, key: &StreamKey) {
        if self.hub.demand_count(key) == 0 || !self.cfg.relay {
            if self.feeds.contains_key(key) {
                self.unfeed(key);
            }
            return;
        }
        let _ = self.hub.open_relay_stream(&self.local, key, DeliveryPolicy::ReplayRequired);
        let feed = self.feeds.entry(key.clone()).or_default();
        (feed.current, feed.wanted_from, feed.fresh, feed.gave_up, feed.generation) =
            (None, StartPosition::Live, true, false, 0);
        feed.wants.clear();
        self.touch(key);
    }

    /// The generation and last record the disk holds of a stream not opened since the node started.
    pub(crate) fn stored(&self, key: &StreamKey) -> Option<Cursor> {
        self.disk.as_ref()?.stored(key)
    }

    /// The ops of the next write of what was published to streams kept on disk, with the store's `Files`: hand them
    /// to it and the result to [`disk_written`](Self::disk_written), one write at a time. `now_ms` is the wall clock,
    /// which names new segments and ages old ones.
    pub(crate) fn take_disk_writes(&mut self, now_ms: u64) -> Option<(Arc<dyn Files>, Vec<FileOp>)> {
        let disk = self.disk.as_mut()?;
        Some((disk.files(), disk.take(now_ms)?))
    }

    /// Whether a write waits to be taken.
    pub(crate) fn disk_queued(&self) -> bool {
        self.disk.as_ref().is_some_and(DiskStore::queued)
    }

    /// Whether retention may yet remove a segment, and a write be due with nothing published.
    pub(crate) fn disk_may_retain(&self) -> bool {
        self.disk.as_ref().is_some_and(DiskStore::may_retain)
    }

    /// Hands on what the write made durable. A failed write is taken again, and publishes are refused until one
    /// succeeds.
    ///
    /// Returns the event for the timeline when the write started or ended a run of failures.
    pub(crate) fn disk_written(&mut self, result: std::io::Result<()>) -> Option<(&'static str, String)> {
        let disk = self.disk.as_mut()?;
        let failing = disk.failing();
        let mut woken = BTreeSet::new();
        for (key, batch) in disk.written(result) {
            if self.hub.commit_admitted(&key, &batch) {
                let _ = lock(&self.store).append(&key, &batch);
                woken.insert(key);
            }
        }
        let told = match (failing, disk.failing()) {
            (None, Some(_)) => Some(("disk_failing", disk.stats().error.unwrap_or_default())),
            (Some(_), None) => Some(("disk_recovered", "a write succeeded".to_owned())),
            _ => None,
        };
        for key in woken {
            self.wake_key(&key);
        }
        told
    }

    pub fn cursor(&self, key: &StreamKey) -> Option<Cursor> {
        self.hub.cursor(key).ok().flatten()
    }

    /// What a wait for `at` to be visible waits on: `Ok(id)` of the key's open on disk, or `Err(cursor)`, the cursor
    /// now, of a stream not kept on disk, whose admitted records are visible at once.
    pub(crate) fn wait_open(&self, key: &StreamKey, at: Cursor) -> Result<Result<u64, Cursor>, StreamError> {
        if !self.originated.contains(key) {
            return Err(StreamError::UnknownStream);
        }
        let (open, generation, next) = match self.disk.as_ref().and_then(|d| d.open_of(key)) {
            Some((id, generation, next)) => (Ok(id), generation, next),
            None => {
                let cursor = self.cursor(key).ok_or(StreamError::UnknownStream)?;
                (Err(cursor), cursor.generation, cursor.sequence + 1)
            }
        };
        if at.generation != generation {
            return Err(RecoveryError::GenerationMismatch.into());
        }
        if at.sequence >= next {
            return Err(StreamError::InvalidOptions);
        }
        Ok(open)
    }

    /// The cursor once it is at or past `at` in the open on disk [`wait_open`](Self::wait_open) gave,
    /// `UnknownStream` once that open has ended, and `None` while its write is still to come.
    pub(crate) fn reached(&self, key: &StreamKey, at: Cursor, open: u64) -> Option<Result<Cursor, StreamError>> {
        if !self.disk.as_ref().and_then(|d| d.open_of(key)).is_some_and(|(id, ..)| id == open) {
            return Some(Err(StreamError::UnknownStream));
        }
        self.cursor(key).filter(|c| c.generation == at.generation && c.sequence >= at.sequence).map(Ok)
    }

    // ---- local subscribers ----

    /// On a relay, subscribing to a stream this node does not have yet starts pulling it.
    pub fn subscribe(&mut self, key: &StreamKey, options: SubscribeOptions) -> Result<u64, StreamError> {
        self.ensure_stream(key)?;
        let id = match options.start {
            StartPosition::Live => {
                let id = self.hub.follow(&self.local, key, options)?;
                // A follower that comes after the loss is told it too.
                if self.feeds.get(key).is_some_and(|f| f.lost_told) {
                    self.hub.origin_lost(id);
                }
                id
            }
            StartPosition::SnapshotThenLive => {
                let id = self.hub.subscribe(&self.local, key, options, Some(self.store.clone()))?;
                self.ask_snapshot(key);
                id
            }
            start => {
                let id = self.hub.subscribe(&self.local, key, options, Some(self.store.clone()))?;
                self.want_from(key, Some((id, start)));
                id
            }
        };
        self.watchers.entry(key.clone()).or_default().push(id);
        self.wakes.push(Wake::Reconcile);
        Ok(id)
    }

    /// `None` means nothing yet; check [`is_subscribed`](Self::is_subscribed) to tell that from the end.
    pub fn poll(&mut self, id: u64) -> Option<StreamEvent> {
        if self.hub.is_recovering(id) && !self.hub.pollable(id) {
            let _ = self.hub.drive_recovery(id, ReadBounds { max_records: usize::MAX, max_bytes: usize::MAX });
        }
        let ended = self.hub.subscription(id).filter(|(_, ended)| *ended).map(|(key, _)| key.clone());
        let event = self.hub.poll(id)?;
        if let Some(key) = ended {
            self.unwatch(&key, id);
        }
        Some(event)
    }

    pub fn is_subscribed(&self, id: u64) -> bool {
        self.hub.contains(id)
    }

    pub fn unsubscribe(&mut self, id: u64) {
        let key = self.hub.subscription(id).map(|(key, _)| key.clone());
        self.hub.forget(id);
        if let Some(key) = key {
            self.unwatch(&key, id);
        }
    }

    fn unwatch(&mut self, key: &StreamKey, id: u64) {
        if let Some(ids) = self.watchers.get_mut(key) {
            ids.retain(|i| *i != id);
            if ids.is_empty() {
                self.watchers.remove(key);
            }
        }
        self.want_from(key, None);
        self.touch(key);
    }

    /// Asks for a stream this node has no position for from the earliest start a subscription or branch still open
    /// on it wants, and asks again when that moves: one that ended or moved on holds the others back no more.
    fn want_from(&mut self, key: &StreamKey, want: Option<(u64, StartPosition)>) {
        let Some(feed) = self.feeds.get_mut(key) else { return };
        if self.hub.cursor(key) != Ok(None) {
            return;
        }
        let hub = &self.hub;
        feed.wants.retain(|id, _| hub.subscription(*id).is_some_and(|(_, done)| !done));
        feed.wants.extend(want);
        let from = feed.wants.values().fold(StartPosition::Live, |from, &s| if earlier(s, from) { s } else { from });
        if from != feed.wanted_from {
            feed.wanted_from = from;
            self.rebranch_all(key);
        }
    }

    /// Only an origin holds a stream's state: a relay asks its parents for it.
    fn ask_snapshot(&mut self, key: &StreamKey) {
        let Some(feed) = self.feeds.get_mut(key).filter(|f| !f.wants_snapshot) else { return };
        feed.wants_snapshot = true;
        self.rebranch_all(key);
    }

    fn rebranch_all(&mut self, key: &StreamKey) {
        let Some(feed) = self.feeds.get(key) else { return };
        for link in feed.links().cloned().collect::<Vec<_>>() {
            self.rebranch(key, &link);
        }
    }

    fn ensure_stream(&mut self, key: &StreamKey) -> Result<(), StreamError> {
        if self.hub.has_stream(key) {
            self.touch(key);
            return Ok(());
        }
        if !self.cfg.relay {
            return Err(StreamError::UnknownStream);
        }
        self.hub.open_relay_stream(&self.local, key, DeliveryPolicy::ReplayRequired)?;
        self.feeds.insert(key.clone(), Feed::default());
        self.touch(key);
        Ok(())
    }

    // ---- downstream: this node as a parent ----

    /// `peer` is the authenticated peer, never something from the message.
    pub fn handle_interest(
        &mut self,
        peer: &PeerId,
        interest: Interest,
        now: Instant,
    ) -> Result<Vec<Answer>, StreamError> {
        if peer.node == self.local.node {
            return Err(StreamError::Forbidden);
        }
        if self.downstream.get(&peer.node).is_some_and(|d| d.peer != *peer) {
            self.drop_downstream(&peer.node);
        }
        let held = self.downstream.get_mut(&peer.node).map_or(0, |d| {
            d.heard = now;
            d.revision
        });
        if interest.revision < held {
            return Err(StreamError::StaleRevision);
        }
        if interest.base != held && !matches!(interest.whole, Some((None, _))) {
            return Err(StreamError::OutOfSync);
        }
        let Interest { revision, whole, wants, mut drops, .. } = interest;
        if whole.is_none() && wants.is_empty() && drops.is_empty() {
            return Ok(Vec::new());
        }
        if let Some((Some(from), Some(to))) = &whole
            && from >= to
        {
            return Err(StreamError::InvalidOptions);
        }
        self.hub.open_session(peer)?;
        let entry = self.downstream.entry(peer.node.clone()).or_insert_with(|| Downstream {
            peer: peer.clone(),
            heard: now,
            revision: 0,
            branches: BTreeMap::new(),
            lanes: Default::default(),
        });
        entry.revision = revision;
        if let Some((from, to)) = &whole {
            let wanted: HashSet<&StreamKey, IdHash> = wants.iter().map(|w| &w.key).collect();
            let bounds = (from.as_ref().map_or(Unbounded, Excluded), to.as_ref().map_or(Unbounded, Included));
            drops.extend(
                entry.branches.range::<StreamKey, _>(bounds).map(|(k, _)| k).filter(|k| !wanted.contains(k)).cloned(),
            );
        }
        for key in &drops {
            self.drop_branch(&peer.node, key);
        }
        let mut statuses = Vec::with_capacity(wants.len());
        for want in &wants {
            let existing = self.downstream[&peer.node].branches.get(&want.key).copied();
            if let Some((epoch, id)) = existing {
                if epoch == want.epoch && self.hub.contains(id) {
                    statuses.push(self.answer(&want.key));
                    continue;
                }
                self.drop_branch(&peer.node, &want.key);
            }
            match self.open_branch(peer, want, now) {
                Ok(id) => {
                    let entry = self.downstream.get_mut(&peer.node).expect("inserted above");
                    entry.branches.insert(want.key.clone(), (want.epoch, id));
                    // A channel's number is new to the child on every lane: from 1 for each boot of this node.
                    let channel = entry.lanes[self.cfg.lane(&want.key) as usize].get_or_insert_with(|| {
                        self.next_channel += 1;
                        Channel { channel: self.next_channel - 1, ..Default::default() }
                    });
                    channel.ready.insert(id);
                    channel.branches.insert(id);
                    self.fed.entry(want.key.clone()).or_default().push((peer.node.clone(), id));
                    statuses.push(self.answer(&want.key));
                }
                Err(status) => statuses.push(status.into()),
            }
        }
        let entry = &self.downstream[&peer.node];
        if entry.branches.is_empty() {
            // A child that wants nothing holds nothing here: no entry, no session.
            self.drop_downstream(&peer.node);
            let _ = self.hub.expire_session(peer);
        } else {
            self.wakes.extend(entry.wakes());
        }
        self.wakes.push(Wake::Reconcile);
        Ok(statuses)
    }

    fn upstream(&self, key: &StreamKey) -> &[Arc<str>] {
        self.feeds.get(key).map_or(&[], Feed::upstream)
    }

    /// Tells the children of a stream whose upstream path is no longer the one they were told.
    fn announce(&mut self, key: &StreamKey) {
        let path = self.upstream(key);
        if self.feeds.get(key).is_none_or(|f| f.told == path) {
            return;
        }
        self.feeds.get_mut(key).expect("checked above").told = path.to_vec();
        let lane = self.cfg.lane(key);
        for (node, _) in self.fed.get(key).into_iter().flatten() {
            let channel = self.downstream.get_mut(node).and_then(|d| d.lane(lane));
            if channel.is_some_and(|channel| channel.paths.insert(key.clone())) {
                self.wakes.push(Wake::Peer(node.clone(), lane));
            }
        }
    }

    /// A parent's path for one link: a loop refuses the parent, anything else is kept and passed on.
    fn learn_path(&mut self, key: &StreamKey, parent: &PeerId, epoch: u32, path: Vec<Arc<str>>, now: Instant) {
        if path.len() >= MAX_PATH || path.contains(&self.me) {
            return self.refuse(key, parent, epoch, Status::Loop, now);
        }
        let Some(feed) = self.feeds.get_mut(key) else { return };
        feed.refused.remove(parent);
        let live = |e: &u32| feed.current.iter().chain(&feed.joining).any(|l| l.epoch == *e);
        if live(&epoch) {
            feed.paths.insert(epoch, path);
        }
        feed.paths.retain(|e, _| live(e));
        self.announce(key);
    }

    fn answer(&self, key: &StreamKey) -> Answer {
        Answer { status: Status::Ok, path: path_through(self.feeds.get(key), &self.me) }
    }

    fn open_branch(&mut self, peer: &PeerId, want: &Want, now: Instant) -> Result<u64, Status> {
        self.hub.allowed(peer, &want.key, Access::Subscribe).map_err(|_| Status::Forbidden)?;
        if want.snapshot {
            self.hub.allowed(peer, &want.key, Access::Snapshot).map_err(|_| Status::Forbidden)?;
        }
        self.ensure_stream(&want.key).map_err(|e| match e {
            StreamError::UnknownStream => Status::UnknownStream,
            _ => Status::LimitExceeded,
        })?;
        let cut = self.hub.cursor(&want.key).ok().flatten();
        if let Some(feed) = self.feeds.get(&want.key)
            && feed
                .orphaned_since
                .is_some_and(|since| now.duration_since(since) >= self.cfg.timings(&want.key).upstream_timeout)
        {
            return Err(Status::Unavailable);
        }
        // A child ahead of this node starts live: what it already has is dropped by position, and one on a newer
        // generation refuses this node's Ready as behind the origin. So does a child asking before this node has a
        // position: the parents are asked from the child's start.
        let start = match (want.start, cut) {
            (StartPosition::After(after), Some(cut)) if after.generation < cut.generation => {
                return Err(Status::GenerationMismatch);
            }
            (StartPosition::After(after), Some(cut))
                if after.generation == cut.generation && after.sequence < cut.sequence =>
            {
                want.start
            }
            (StartPosition::First, Some(_)) => want.start,
            _ => StartPosition::Live,
        };
        let options = SubscribeOptions { start, slow_consumer: SlowConsumerPolicy::Reset, limits: self.cfg.branch };
        let id = self.hub.subscribe(peer, &want.key, options, Some(self.store.clone())).map_err(|e| match e {
            StreamError::Forbidden => Status::Forbidden,
            StreamError::Recovery(RecoveryError::HistoryUnavailable) => Status::HistoryUnavailable,
            StreamError::Recovery(RecoveryError::GenerationMismatch) => Status::GenerationMismatch,
            StreamError::LimitExceeded | StreamError::QuotaExceeded { .. } => Status::LimitExceeded,
            _ => Status::Unavailable,
        })?;
        // Only a child that may replay can make this node pull history for it.
        if self.hub.allowed(peer, &want.key, Access::Replay).is_ok() {
            self.want_from(&want.key, Some((id, want.start)));
        }
        if want.snapshot && self.hub.want_snapshot(id) {
            self.ask_snapshot(&want.key);
        }
        Ok(id)
    }

    /// The boot of a downstream node with a channel on `lane`.
    pub fn downstream_peer(&self, node: &str, lane: Lane) -> Option<&PeerId> {
        self.downstream.get(node).filter(|d| d.lanes[lane as usize].is_some()).map(|d| &d.peer)
    }

    pub fn downstream_peers(&self) -> Vec<PeerId> {
        self.downstream.values().map(|d| d.peer.clone()).collect()
    }

    /// The next push for one downstream node on one lane, with its id on that channel: first a push to send again
    /// after a go-back, byte for byte, in seq order; then a new one while the channel's window and the node-wide cap
    /// leave room, taking a stream's events only while what it has in flight is under half its branch. `rtt` is the
    /// round trip to the node, `None` before one is known: the window is then its floor. A push carries the
    /// namespaces of `lane` only.
    pub fn next_push(
        &mut self,
        node: &str,
        lane: Lane,
        now: Instant,
        rtt: Option<Duration>,
    ) -> Option<(PushId, Bytes)> {
        let entry = self.downstream.get_mut(node)?.lane(lane)?;
        entry.rtt = rtt;
        if let Some(push) = entry.unacked.get_mut(entry.sent) {
            (push.sent_at, push.delivered) = (now, entry.delivered);
            (entry.sent, entry.resent) = (entry.sent + 1, entry.resent + 1);
            self.stats.push_resent += 1;
            let id = PushId { channel: entry.channel, seq: entry.acked + entry.sent as u64, round: entry.round };
            return Some((id, push.bytes.clone()));
        }
        // The first push in flight always goes, so a channel is never slower than one push per round trip. Another
        // goes while at least half a push fits the window and the node-wide cap, and the last one was not `drained`: a
        // smaller one would cost a call for little.
        let (mut budget, least) = (self.cfg.max_push_bytes, self.cfg.max_push_bytes / 2);
        if !entry.unacked.is_empty() {
            entry.window = entry.window(&self.cfg, now);
            budget = budget.min(entry.window.saturating_sub(entry.inflight));
            if entry.drained == Some(self.hub.terminals())
                || entry.unacked.len() >= MAX_PUSHES_IN_FLIGHT
                || budget < least
            {
                return None;
            }
            let waits = !entry.ready.is_empty() || !entry.paths.is_empty();
            let (room, slots) = self.cap_room(node, lane);
            if slots == 0 || room < least {
                // Once a wait: a channel asked again before the cap frees room still waits the same wait.
                if waits && self.capped.insert((self.downstream[node].peer.node.clone(), lane)) {
                    self.stats.push_capped += 1;
                }
                return None;
            }
            budget = budget.min(room);
        }
        let Self { hub, downstream, cfg, stats, .. } = self;
        let Downstream { branches, lanes, .. } = downstream.get_mut(node).expect("looked up above");
        let entry = lanes[lane as usize].as_mut().expect("looked up above");
        let half = |f: QueueUsage| {
            let b = cfg.branch;
            f.bytes >= b.max_bytes / 2 || f.records >= b.max_records / 2 || f.events >= b.max_events / 2
        };
        let (mut push, mut encoded, mut bytes) = (Push::default(), wire::PUSH_HEADER, 0);
        while let Some(key) = entry.paths.pop_first() {
            let Some((epoch, _)) = branches.get(&key) else { continue };
            let path = path_through(self.feeds.get(&key), &self.me);
            let len = wire::item_len(&key, wire::path_len(&path), 0);
            if push.items.len() >= cfg.max_push_items || encoded + len > budget {
                entry.paths.insert(key);
                break;
            }
            encoded += len;
            push.items.push((key, *epoch, Item::Path(path)));
        }
        // One item from each ready branch per pass, from where the last push stopped.
        // It ends full, or once every ready branch in a row had nothing that could go: the ones with
        // something may lie anywhere past the ones without.
        let (mut records_out, mut done, mut stopped, mut idle) = (0, Vec::new(), None, 0);
        // What each stream's events take of its branch, one charge each: its records are what the push holds of it.
        let mut charges = BTreeMap::<u64, QueueUsage>::new();
        while push.items.len() < cfg.max_push_items && idle < entry.ready.len() {
            let room = cfg.max_push_items - push.items.len();
            let pass: Vec<u64> = entry
                .ready
                .range(entry.resume..)
                .chain(entry.ready.range(..entry.resume))
                .take(room)
                .copied()
                .collect();
            for id in pass {
                (entry.resume, idle) = (id + 1, idle + 1);
                let Some((bare, ended)) = hub.subscription(id).map(|(key, ended)| (wire::item_len(key, 0, 0), ended))
                else {
                    entry.ready.remove(&id);
                    continue;
                };
                // What a batch may still add: the push never outgrows its budget encoded.
                let Some(room) = budget.checked_sub(encoded + bare) else {
                    stopped.get_or_insert(id);
                    continue;
                };
                // Half its branch in flight: the rest waits in the branch, where a child that falls behind fills it. The
                // answer that gives a charge back makes it ready again.
                if !ended && !entry.unacked.is_empty() && hub.inflight(id).is_some_and(half) {
                    entry.ready.remove(&id);
                    continue;
                }
                let took = charges.get(&id).map_or(0, |c| c.records);
                let recovering = hub.is_recovering(id);
                if recovering && !hub.pollable(id) {
                    let max_records = (cfg.hub.max_batch_records - took).min(room / 8);
                    if max_records == 0 {
                        stopped.get_or_insert(id);
                        continue;
                    }
                    let _ = hub.drive_recovery(id, ReadBounds { max_records, max_bytes: room - 4 * max_records });
                }
                let Some((mut size, records)) = hub.next_size(id) else {
                    if !recovering {
                        entry.ready.remove(&id);
                    }
                    continue;
                };
                // A state too large for this node's pushes, whose parent's were larger: the child cannot have it here.
                if records == 0 && wire::PUSH_HEADER + bare + size > cfg.max_push_bytes {
                    hub.end(id, EndReason::SlowConsumer);
                    size = 0;
                }
                // Nor one full batch of records of a stream, which every branch holds, so a whole push fits an empty
                // one.
                if size + 4 * records > room || took + records > cfg.hub.max_batch_records {
                    stopped.get_or_insert(id);
                    continue;
                }
                let key = hub.subscription(id).expect("sized above").0.clone();
                let Some((event, charge)) = hub.poll_charged(id) else { continue };
                let item = match event {
                    StreamEvent::Ready { start: Some(start) } => Item::Ready(start),
                    StreamEvent::Batch(batch) => Item::Batch(batch),
                    StreamEvent::CaughtUp { .. } => Item::CaughtUp,
                    StreamEvent::ResetRequired(reason) => Item::Reset(reason),
                    StreamEvent::End(reason) => Item::End(reason),
                    StreamEvent::Snapshot(snapshot) => Item::Snapshot(snapshot),
                    _ => {
                        if charge != QueueUsage::default() {
                            hub.discharge(id, charge);
                        }
                        idle = 0;
                        continue;
                    }
                };
                if charge != QueueUsage::default() {
                    charges.entry(id).or_default().add(charge);
                }
                idle = 0;
                encoded += bare + size + 4 * records;
                bytes += size;
                records_out += records;
                if matches!(item, Item::Reset(_) | Item::End(_)) {
                    done.push(key.clone());
                }
                let epoch = branches.get(&key).map_or(0, |b| b.0);
                push.items.push((key, epoch, item));
            }
        }
        // A branch whose next batch did not fit goes first next time: an empty push holds any batch.
        if let Some(id) = stopped {
            entry.resume = id;
        }
        if push.items.is_empty() {
            return None;
        }
        // Under half a push, and nothing left out for room: a branch reading back from its ring has more each pass.
        let drained =
            encoded < least && stopped.is_none() && entry.paths.is_empty() && push.items.len() < cfg.max_push_items;
        entry.drained = drained.then(|| hub.terminals());
        stats.pushes_out += 1;
        stats.push_bytes_out += bytes as u64;
        entry.pushed_bytes += bytes as u64;
        entry.pushed_records += records_out as u64;
        let seq = entry.acked + entry.unacked.len() as u64 + 1;
        let body = wire::encode_push(entry.channel, seq, &push);
        let items = push.items.into_iter().map(|(key, epoch, _)| (key, epoch)).collect();
        let (sent_at, delivered) = (now, entry.delivered);
        let charges = charges.into_iter().collect();
        entry.unacked.push_back(Unacked { bytes: body.clone(), items, charges, sent_at, delivered });
        (entry.inflight, entry.sent) = (entry.inflight + body.len(), entry.sent + 1);
        let id = PushId { channel: entry.channel, seq, round: entry.round };
        for key in done {
            self.drop_branch(node, &key);
        }
        Some((id, body))
    }

    /// Every channel, with its child and lane.
    fn channels(&self) -> impl Iterator<Item = (&Downstream, Lane, &Channel)> {
        self.downstream.values().flat_map(|d| d.each().map(move |(lane, c)| (d, lane, c)))
    }

    /// Room under the node-wide cap for one more push to `node` on `lane`: bytes, and call slots. Of the cap, each busy
    /// channel may hold its share past its first push, the cap over the number of busy channels; more only while what is
    /// left keeps every other busy channel its share.
    fn cap_room(&self, node: &str, lane: Lane) -> (usize, usize) {
        let (mut held, mut channels) = ((0, 0), 0);
        // The asking channel has a push in flight, so it is among the busy.
        for (_, _, c) in self.channels() {
            let (bytes, pushes) = c.past_first();
            (held, channels) = ((held.0 + bytes, held.1 + pushes), channels + c.busy() as usize);
        }
        let share = (self.cfg.max_push_inflight / channels.max(1), PUSH_SLOTS / channels.max(1));
        let (mut owed, mut mine) = ((0, 0), (0, 0));
        for (d, l, c) in self.channels().filter(|(_, _, c)| c.busy()) {
            if (&*d.peer.node, l) == (node, lane) {
                mine = c.past_first();
            } else {
                let (bytes, pushes) = c.past_first();
                owed = (owed.0 + share.0.saturating_sub(bytes), owed.1 + share.1.saturating_sub(pushes));
            }
        }
        let free = (self.cfg.max_push_inflight.saturating_sub(held.0), PUSH_SLOTS.saturating_sub(held.1));
        let room = |free: usize, share: usize, mine: usize, owed: usize| {
            free.min(share.saturating_sub(mine).max(free.saturating_sub(owed)))
        };
        (room(free.0, share.0, mine.0, owed.0), room(free.1, share.1, mine.1, owed.1))
    }

    /// The channel of a downstream node on a lane, while `peer` is its boot.
    fn channel(&mut self, peer: &PeerId, lane: Lane) -> Option<&mut Channel> {
        self.downstream.get_mut(&peer.node).filter(|d| d.peer == *peer)?.lane(lane)
    }

    /// The child's answer to push `id`, sent to `peer` on `lane`: the pushes it applied free what they held, a gap
    /// sends again every push from the first it lacks, and `Retired`, or an `applied` past the last push sent, drops
    /// the node, whose next interest makes new channels. An answer for another channel, or one behind what the child
    /// acknowledged before, changes nothing.
    pub fn push_answered(&mut self, peer: &PeerId, lane: Lane, id: PushId, answer: PushAnswer, now: Instant) {
        if !self.channel(peer, lane).is_some_and(|c| c.channel == id.channel) {
            return;
        }
        let (applied, unwanted) = match answer {
            PushAnswer::Retired => return self.expire(peer, now, "retired its channel"),
            PushAnswer::Applied { applied, unwanted } => (applied, Some(unwanted)),
            PushAnswer::Skipped { applied } => (applied, None),
        };
        let entry = self.channel(peer, lane).expect("looked up above");
        // A child cannot have applied what it was not sent: one that says so is wrong about the channel, and freeing
        // what it says it applied could lose pushes it never did.
        if applied > entry.acked + entry.unacked.len() as u64 {
            return self.expire(peer, now, "answered past its pushes");
        }
        if applied < entry.acked {
            // An answer to a push not yet answered, sent since the last go-back, below what the child answered before:
            // the child went back, and nothing more on this channel would be applied. An older answer that came late
            // changes nothing.
            if id.seq > entry.acked && id.round == entry.round {
                self.expire(peer, now, "went back on its pushes");
            }
            return;
        }
        let answered = id.seq.checked_sub(entry.acked + 1).and_then(|i| entry.unacked.get(i as usize));
        let sample = answered.map(|push| (push.sent_at, push.delivered)).filter(|_| id.round == entry.round);
        let dropped: Vec<(StreamKey, u32)> = match (answered, &unwanted) {
            (Some(push), Some(unwanted)) => {
                unwanted.iter().filter_map(|i| push.items.get(*i as usize).cloned()).collect()
            }
            _ => Vec::new(),
        };
        let (mut freed, popped) = (Vec::new(), entry.acked < applied);
        // Only progress counts as heard: a child that answers each push `Skipped` without taking it is failing. What
        // was published since may go now, one push for each answer.
        if popped {
            (entry.failing, entry.drained) = (None, None);
        }
        while entry.acked < applied
            && let Some(push) = entry.unacked.pop_front()
        {
            (entry.acked, entry.sent) = (entry.acked + 1, entry.sent.saturating_sub(1));
            entry.inflight -= push.bytes.len();
            entry.delivered += push.bytes.len() as u64;
            freed.extend(push.charges);
        }
        match (unwanted, sample) {
            (Some(_), Some((at, delivered))) => entry.sample(now, at, delivered),
            // A push came before one it follows: they go out again, in order.
            (None, _) if applied < id.seq && id.round == entry.round => {
                if !popped {
                    entry.failing.get_or_insert(now);
                }
                entry.go_back();
            }
            _ => {}
        }
        // Bytes freed, with or without a charge (a push of paths or control events has none): this channel's window
        // and the capped channels may go. What the child applied leaves its branches: a branch held at half in flight,
        // or a state held for room, can go now.
        if popped {
            self.wake_capped();
            let ready: Vec<u64> =
                freed.into_iter().filter(|(sub, charge)| self.hub.discharge(*sub, *charge)).map(|f| f.0).collect();
            let entry = self.channel(peer, lane).expect("looked up above");
            entry.ready.extend(ready);
            if !entry.ready.is_empty() || !entry.paths.is_empty() {
                self.wakes.push(Wake::Peer(peer.node.clone(), lane));
            }
        }
        for (key, epoch) in dropped {
            if self.downstream.get(&peer.node).is_some_and(|d| d.branches.get(&key).is_some_and(|(e, _)| *e == epoch)) {
                self.drop_branch(&peer.node, &key);
            }
        }
    }

    /// A call carrying push `id` to `peer` on `lane` failed: every unacknowledged push of the channel goes out again
    /// from the oldest, on the next [`next_push`](Self::next_push), which a driver calls a tick later. A node whose
    /// oldest push on a lane has failed that long is given up on by [`expire_unheard`](Self::expire_unheard). A failure
    /// of a push sent before the last go-back changes nothing.
    pub fn push_failed(&mut self, peer: &PeerId, lane: Lane, id: PushId, now: Instant) {
        let Some(entry) = self.channel(peer, lane).filter(|c| c.channel == id.channel) else { return };
        if id.round == entry.round && id.seq > entry.acked {
            entry.failing.get_or_insert(now);
            entry.go_back();
        }
    }

    /// Gives up on a downstream node the driver could not reach for too long.
    pub fn expire_downstream(&mut self, peer: &PeerId, now: Instant) {
        if self.downstream.get(&peer.node).is_some_and(|d| d.peer == *peer) {
            self.expire(peer, now, "could not be reached");
        }
    }

    /// Gives up on the downstream nodes whose last interest arrived `after` ago or earlier, or whose oldest push on
    /// any lane has failed for `after`. A child asks at least every `refresh`, so one this long silent is gone, or no
    /// longer let ask: records are pushed, not asked for, and nothing else would stop them. Its interests cover every
    /// lane, so a lane its pushes cannot reach drops all of it, and it asks again for the rest.
    pub fn expire_unheard(&mut self, now: Instant, after: Duration) {
        let past = |at: Instant| now.saturating_duration_since(at) >= after;
        let gone: Vec<_> = self
            .downstream
            .values()
            .filter_map(|d| match (past(d.heard), d.each().any(|(_, c)| c.failing.is_some_and(past))) {
                (true, _) => Some((d.peer.clone(), "was not heard from")),
                (false, true) => Some((d.peer.clone(), "could not be reached")),
                _ => None,
            })
            .collect();
        for (peer, why) in gone {
            self.expire(&peer, now, why);
        }
    }

    fn expire(&mut self, peer: &PeerId, now: Instant, why: &str) {
        self.drop_downstream(&peer.node);
        let _ = self.hub.expire_session(peer);
        self.stats.downstream_expired += 1;
        self.note(now, "downstream_expired", None, format!("{} {why}", peer.node));
        self.wakes.push(Wake::Reconcile);
    }

    // ---- upstream: this node as a child ----

    fn local_reset(&mut self, key: &StreamKey, reason: ResetReason, now: Instant) {
        if self.hub.cursor(key).ok().flatten().is_some() {
            self.stats.local_resets += 1;
            self.note(now, "reset", Some(key), RESET_REASONS[reason as usize].replace('_', " "));
            if reason == ResetReason::HistoryUnavailable {
                // What this node lost may be behind a child, or held by the child's other parents: a child is
                // refused only if no parent holds its own position either.
                self.rebranch_children(key);
            }
        }
        let _ = self.hub.reset_relay_stream(&self.local, key, reason);
        if let Some(feed) = self.feeds.get_mut(key) {
            // A new generation is fetched from its record 1, as the origin's own followers see it.
            (feed.wanted_from, feed.fresh) = (StartPosition::Live, reason == ResetReason::GenerationChanged);
            feed.wants.clear();
            feed.gave_up = reason == ResetReason::HistoryUnavailable && feed.generation > 0;
        }
        lock(&self.store).remove(key);
        self.wake_key(key);
    }

    fn refuse(&mut self, key: &StreamKey, peer: &PeerId, epoch: u32, status: Status, now: Instant) {
        self.stats.refusals[status as usize] += 1;
        let at = self.cursor(key);
        let Some(feed) = self.feeds.get_mut(key) else { return };
        let link = Link { peer: peer.clone(), epoch };
        let followed = feed.links().any(|l| *l == link);
        // Record 1 of a new generation is a best effort: a parent that cannot give it is asked for what is wanted.
        if feed.fresh
            && matches!(status, Status::Forbidden | Status::HistoryUnavailable)
            && followed
            && self.hub.cursor(key) == Ok(None)
        {
            feed.fresh = false;
            return self.rebranch(key, &link);
        }
        // Perhaps only the state was refused: its waits end, and the records are asked for without it.
        if status == Status::Forbidden && feed.wants_snapshot && followed {
            return self.give_up_state(key, &link, EndReason::Revoked);
        }
        if feed.joining.as_ref() == Some(&link) {
            feed.joining = None;
        } else if feed.current.as_ref() == Some(&link) {
            feed.current = None;
        } else {
            return;
        }
        let strikes = feed.refused.get(peer).map_or(0, |r| r.strikes).min(5);
        // The parent answers from its own view, which may not have the change this node saw yet: its first refusal
        // within a backoff of the change is asked again at once, as a refusal before the change was, without a strike.
        let mended = matches!(status, Status::Unavailable | Status::UnknownStream);
        let lagging = mended && feed.view_changed.take().is_some_and(|at| now < at + self.cfg.retry_refused);
        let entry = if lagging {
            Refusal { until: now, status, strikes, at }
        } else {
            Refusal { until: now + self.cfg.retry_refused * (1 << strikes), status, strikes: strikes + 1, at }
        };
        let before = feed.refused.insert(peer.clone(), entry);
        // Retries of a parent that keeps giving the same answer stay out of the feed.
        if before.is_none_or(|r| r.status != status) {
            let detail = format!("{} answered {}", peer.node, STATUSES[status as usize].replace('_', " "));
            self.note(now, "refused", Some(key), detail);
        }
        self.announce(key);
        self.touch(key);
    }

    /// The state cannot reach this node from `link`: what waits for it here ends with `reason`, and the link is asked
    /// again for the records alone.
    fn give_up_state(&mut self, key: &StreamKey, link: &Link, reason: EndReason) {
        if let Some(feed) = self.feeds.get_mut(key) {
            feed.wants_snapshot = false;
        }
        self.hub.end_state_waits(key, reason);
        self.wake_key(key);
        self.rebranch(key, link);
    }

    /// Asks the same parent for a fresh branch from the current position.
    fn rebranch(&mut self, key: &StreamKey, link: &Link) {
        self.next_epoch = self.next_epoch.wrapping_add(1);
        let epoch = self.next_epoch;
        let Some(feed) = self.feeds.get_mut(key) else { return };
        for slot in [&mut feed.current, &mut feed.joining] {
            if slot.as_ref() == Some(link) {
                *slot = Some(Link { peer: link.peer.clone(), epoch });
                feed.paths.remove(&link.epoch).map(|path| feed.paths.insert(epoch, path));
            }
        }
        self.touch(key);
    }

    /// Push `seq` of `channel` from parent `from` on `lane` in its boot `boot`, the session's incarnation, which `from`
    /// may not carry: applied only when it is the next of the channel this node follows of that parent on that lane. A
    /// new boot of the parent retires the one before on every lane, whose late pushes are refused, as are those of an
    /// older channel and those this node cannot place, of a channel it never followed past its first push. The
    /// parent's lane for a namespace wins: the push is not checked against this node's.
    #[expect(clippy::too_many_arguments, reason = "the push's place: who sent it on which lane and channel")]
    pub fn handle_push(
        &mut self,
        from: &PeerId,
        lane: Lane,
        boot: u128,
        channel: u64,
        seq: u64,
        push: Push,
        now: Instant,
    ) -> PushAnswer {
        if seq == 1 && !self.channels.contains_key(&*from.node) {
            let lanes = [None; 3];
            self.channels.insert(from.node.clone(), Followed { boot, lanes, retired: VecDeque::new() });
        }
        let Some(f) = self.channels.get_mut(&*from.node).filter(|f| !f.retired.contains(&boot)) else {
            self.stats.push_skipped[RETIRED] += 1;
            return PushAnswer::Retired;
        };
        let slot = lane as usize;
        if f.boot != boot {
            if f.retired.len() == MAX_RETIRED {
                f.retired.pop_front();
            }
            f.retired.push_back(f.boot);
            let followed = f.lanes[slot].is_some();
            (f.boot, f.lanes) = (boot, [None; 3]);
            f.lanes[slot] = followed.then_some((channel, 0));
        }
        let slot = &mut f.lanes[slot];
        match *slot {
            Some((c, _)) if channel == c => {}
            Some((c, _)) if channel > c => *slot = Some((channel, 0)),
            None if seq == 1 => *slot = Some((channel, 0)),
            _ => {
                self.stats.push_skipped[RETIRED] += 1;
                return PushAnswer::Retired;
            }
        }
        let (_, applied) = slot.as_mut().expect("set above");
        if seq.checked_sub(1) != Some(*applied) {
            let applied = *applied;
            self.stats.push_skipped[if seq <= applied { DUPLICATE } else { GAP }] += 1;
            return PushAnswer::Skipped { applied };
        }
        *applied = seq;
        PushAnswer::Applied { applied: seq, unwanted: self.apply_push(from, push, now) }
    }

    /// Returns the indexes of items this node does not want from `from`.
    fn apply_push(&mut self, from: &PeerId, push: Push, now: Instant) -> Vec<u16> {
        self.stats.pushes_in += 1;
        let mut unwanted = Vec::new();
        for (index, (key, epoch, item)) in push.items.into_iter().enumerate() {
            let link = Link { peer: from.clone(), epoch };
            let feed = self.feeds.get(&key);
            let (current, joining) =
                feed.map_or((false, false), |f| (f.current.as_ref() == Some(&link), f.joining.as_ref() == Some(&link)));
            if !current && !joining {
                unwanted.push(index as u16);
                self.stats.unwanted_items += 1;
                continue;
            }
            let shown = match &item {
                Item::Ready(start) => Some(start.generation),
                Item::Batch(batch) => Some(batch.generation()),
                Item::Snapshot(snapshot) => Some(snapshot.through().generation),
                _ => None,
            };
            if shown.is_some_and(|g| g < self.feeds[&key].generation) {
                self.refuse(&key, from, epoch, Status::GenerationMismatch, now);
                continue;
            }
            match item {
                Item::Ready(start) => {
                    let at = self.cursor(&key);
                    let feed = self.feeds.get_mut(&key).expect("looked up above");
                    if current && at.is_some_and(|c| c.generation < start.generation) {
                        // The parent that fed the old generation moved on: take the new one from its record 1.
                        self.local_reset(&key, ResetReason::GenerationChanged, now);
                        self.rebranch(&key, &link);
                    } else if current && feed.gave_up && start.generation > feed.generation && start.sequence > 0 {
                        // Given up, the position is not continued: a newer generation is, from its record 1.
                        (feed.fresh, feed.gave_up, feed.generation) = (true, false, start.generation);
                        self.rebranch(&key, &link);
                    } else if self.hub.adopt_relay_stream(&self.local, &key, start) == Ok(true) {
                        self.adopt_ring(&key, start);
                        self.wake_key(&key);
                    } else if at.is_some_and(|c| c.generation != start.generation || c.sequence < start.sequence) {
                        // The branch starts past this node's position: the parent cannot continue it.
                        self.refuse(&key, from, epoch, Status::HistoryUnavailable, now);
                    }
                }
                Item::Batch(batch) => {
                    self.stats.push_bytes_in += batch.bytes() as u64;
                    self.accept(&key, &link, current, batch, now);
                }
                Item::CaughtUp if joining => {
                    let feed = self.feeds.get_mut(&key).expect("looked up above");
                    let old = std::mem::replace(&mut feed.current, feed.joining.take());
                    self.stats.switches += 1;
                    let old = old.map_or_else(String::new, |l| format!(" from {}", l.peer.node));
                    self.note(now, "switched", Some(&key), format!("moved to {}{old}", from.node));
                    self.announce(&key);
                    self.touch(&key);
                }
                Item::CaughtUp => {}
                Item::Reset(ResetReason::GenerationChanged) if current => {
                    self.local_reset(&key, ResetReason::GenerationChanged, now);
                    self.rebranch(&key, &link);
                }
                Item::Reset(ResetReason::HistoryUnavailable | ResetReason::GenerationChanged) => {
                    self.refuse(&key, from, epoch, Status::HistoryUnavailable, now);
                }
                Item::Reset(_) => self.rebranch(&key, &link),
                Item::End(EndReason::StreamClosed) if current => {
                    let _ = self.end_stream(&key);
                }
                Item::Path(path) => self.learn_path(&key, from, epoch, path, now),
                Item::Snapshot(snapshot) => {
                    let accepted = self.hub.accept_snapshot(&self.local, &key, snapshot).is_ok();
                    let feed = self.feeds.get_mut(&key).expect("looked up above");
                    if accepted {
                        feed.wants_snapshot = false;
                        self.wake_key(&key);
                    } else if feed.wants_snapshot {
                        // One behind the records it covers, from a parent this node could not follow: ask again.
                        self.rebranch(&key, &link);
                    }
                }
                Item::End(EndReason::Revoked) => self.refuse(&key, from, epoch, Status::Forbidden, now),
                // A branch always resets a slow child: this one could not carry the state.
                Item::End(EndReason::SlowConsumer) if self.feeds.get(&key).is_some_and(|f| f.wants_snapshot) => {
                    self.give_up_state(&key, &link, EndReason::SlowConsumer);
                }
                Item::End(_) => self.refuse(&key, from, epoch, Status::Unavailable, now),
            }
            let Some(feed) = self.feeds.get_mut(&key) else { continue };
            if let Some(taken) = self.hub.cursor(&key).ok().flatten().map(|c| c.generation) {
                feed.generation = feed.generation.max(taken);
            }
            // A position from a link still in use: the origin is reachable again.
            if feed.lost_told && shown.is_some() && feed.links().any(|l| *l == link) {
                feed.lost_told = false;
            }
        }
        unwanted
    }

    /// The ring of a pulled stream starts over after `start`.
    fn adopt_ring(&self, key: &StreamKey, start: Cursor) {
        let mut store = lock(&self.store);
        store.remove(key);
        let _ = store.open_at(key, start);
    }

    fn accept(&mut self, key: &StreamKey, link: &Link, current: bool, batch: SequencedBatch, now: Instant) {
        let before = |batch: &SequencedBatch| Cursor { generation: batch.generation(), sequence: batch.first() - 1 };
        if self.hub.cursor(key) == Ok(None) {
            self.adopt_ring(key, before(&batch));
        }
        let batch = match self.hub.accept_batch(&self.local, key, batch) {
            Ok(fresh) => fresh,
            Err(PublishError { error: StreamError::Recovery(RecoveryError::GenerationMismatch), returned })
                if current =>
            {
                self.local_reset(key, ResetReason::GenerationChanged, now);
                self.adopt_ring(key, before(&returned));
                self.hub.accept_batch(&self.local, key, returned).ok().flatten()
            }
            Err(PublishError {
                error: StreamError::Recovery(RecoveryError::Gap { .. } | RecoveryError::GenerationMismatch),
                ..
            }) => {
                return self.refuse(key, &link.peer, link.epoch, Status::HistoryUnavailable, now);
            }
            Err(_) => return,
        };
        if let Some(batch) = batch {
            let _ = lock(&self.store).append(key, &batch);
            self.wake_key(key);
        }
    }

    /// Decides which parent feeds each pulled stream and returns the interests to send. It looks
    /// only at the streams something happened to, or all of them when the parents changed.
    /// `upstream` are the reachable parents, most preferred first, never this node.
    pub fn reconcile<'a>(&mut self, upstream: impl Into<Parents<'a>>, now: Instant) -> Vec<(PeerId, Interest)> {
        let upstream = upstream.into();
        let all = upstream.all();
        if self.candidates != all || self.by_namespace != *upstream.by_namespace {
            (self.candidates, self.by_namespace) = (all, upstream.by_namespace.clone());
            self.available_since.retain(|p, _| self.candidates.contains(p));
            for peer in &self.candidates {
                self.available_since.entry(peer.clone()).or_insert(now);
            }
            self.dirty.extend(self.feeds.keys().cloned());
        }
        let mut keys = std::mem::take(&mut self.dirty);
        while self.timers.first().is_some_and(|(at, _)| *at <= now) {
            keys.extend(self.timers.pop_first().map(|(_, key)| key));
        }
        let (mut expired, mut broken, mut orphans, mut unreached, mut notes) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let mut epoch = self.next_epoch;
        for key in &keys {
            let Some(feed) = self.feeds.get_mut(key) else { continue };
            let (candidates, timings) = (upstream.for_key(key), self.cfg.timings(key));
            let mut again: Option<Instant> = None;
            let mut at = |when: Instant| again = Some(again.map_or(when, |a| a.min(when)));
            if self.hub.demand_count(key) == 0 {
                let since = *feed.idle_since.get_or_insert(now);
                if now.duration_since(since) >= self.cfg.linger {
                    expired.push(key.clone());
                    continue;
                }
                at(since + self.cfg.linger);
            } else {
                feed.idle_since = None;
            }
            if let Some(lost) = feed.current.take_if(|l| !candidates.contains(&l.peer)) {
                self.stats.parents_lost += 1;
                notes.push(("parent_lost", key.clone(), format!("{} is gone", lost.peer.node)));
            }
            if feed.joining.as_ref().is_some_and(|l| !candidates.contains(&l.peer)) {
                feed.joining = None;
            }
            feed.refused.retain(|peer, _| candidates.contains(peer));
            // A parent that refused a position this node has since left may hold the one it is at: with no parent,
            // it is asked again before the position is given up.
            if feed.current.is_none() {
                let position = self.hub.cursor(key).ok().flatten();
                let stale = |r: &&mut Refusal| {
                    r.at != position && matches!(r.status, Status::HistoryUnavailable | Status::GenerationMismatch)
                };
                feed.refused.values_mut().filter(stale).for_each(|r| r.until = now);
            }
            feed.refused.values().filter(|r| r.until > now).for_each(|r| at(r.until));
            let refused = |peer: &PeerId| feed.refused.get(peer).is_some_and(|r| r.until > now);
            let rank = |peer: &PeerId| candidates.iter().position(|c| c == peer);
            // Every parent refused the key, for a reason `ok` accepts.
            let every = |ok: &dyn Fn(&Refusal) -> bool| {
                !candidates.is_empty() && candidates.iter().all(|p| feed.refused.get(p).is_some_and(ok))
            };
            let desired = candidates.iter().find(|p| !refused(p));
            let mut link = |peer: &PeerId| {
                epoch = epoch.wrapping_add(1);
                Some(Link { peer: peer.clone(), epoch })
            };
            let mut stranded = false;
            match (&feed.current, desired) {
                (None, Some(peer)) => {
                    (feed.current, feed.joining) = (link(peer), None);
                    notes.push(("parent_chosen", key.clone(), format!("pulling from {}", peer.node)));
                    if feed.orphan_told {
                        notes.push(("parent_restored", key.clone(), format!("pulling from {}", peer.node)));
                    }
                }
                (Some(current), Some(peer)) if rank(peer) < rank(&current.peer) => {
                    let stable = self.available_since[peer] + timings.hold_down;
                    if now < stable {
                        at(stable);
                    } else if feed.joining.as_ref().is_none_or(|j| j.peer != *peer) {
                        feed.joining = link(peer);
                    }
                }
                (Some(_), _) => feed.joining = None,
                (None, None) => {
                    if every(&|r| {
                        r.until > now && matches!(r.status, Status::HistoryUnavailable | Status::GenerationMismatch)
                    }) {
                        // Candidates show up one by one: after a heal, a relay that restarted and took the key up live
                        // can answer before the origin, which holds the history, is back in view.
                        let newest = candidates.iter().map(|p| self.available_since[p]).max();
                        match newest.map(|since| since + timings.upstream_timeout) {
                            Some(settled) if now < settled => at(settled),
                            _ => broken.push(key.clone()),
                        }
                        stranded = true;
                    }
                }
            }
            // A key every parent reports as unknown is closed, not unreachable: the followers are told the origin is
            // lost, as when every parent has lost its own upstream, and wait for it.
            let closed = every(&|r| r.status == Status::UnknownStream);
            if every(&|r| matches!(r.status, Status::UnknownStream | Status::Unavailable)) {
                unreached.push(key.clone());
            }
            // Parents that refuse only the position are reachable: the key is not orphaned while they are waited for.
            if feed.current.is_some() || closed || stranded {
                (feed.orphaned_since, feed.orphan_told) = (None, false);
            } else if !feed.orphan_told {
                let since = *feed.orphaned_since.get_or_insert(now);
                if now.duration_since(since) >= timings.upstream_timeout {
                    feed.orphan_told = true;
                    orphans.push(key.clone());
                } else {
                    at(since + timings.upstream_timeout);
                }
            }
            if let Some(when) = again {
                self.timers.insert((when, key.clone()));
            }
        }
        self.next_epoch = epoch;
        for (kind, key, detail) in notes {
            self.note(now, kind, Some(&key), detail);
            self.announce(&key);
        }
        for key in expired {
            self.unfeed(&key);
            let _ = self.hub.close_stream(&self.local, &key, EndReason::StreamClosed);
            lock(&self.store).remove(&key);
        }
        for key in broken {
            self.local_reset(&key, ResetReason::HistoryUnavailable, now);
            let feed = self.feeds.get_mut(&key).expect("still pulled");
            // Retry now, but keep the strikes: a parent that stays behind is asked at the backoff, not every round trip.
            feed.refused.values_mut().for_each(|r| r.until = now);
            self.touch(&key);
        }
        for key in orphans {
            self.note(now, "orphaned", Some(&key), "no parent is reachable; downstream branches ended".into());
            for (_, id) in self.fed.get(&key).into_iter().flatten() {
                self.hub.end(*id, EndReason::UpstreamLost);
            }
            self.wake_key(&key);
            unreached.push(key);
        }
        for key in unreached {
            let Some(feed) = self.feeds.get_mut(&key) else { continue };
            // Only a position can be lost: a key never fed here has nothing to tell.
            if !feed.lost_told && self.hub.cursor(&key).ok().flatten().is_some() {
                feed.lost_told = true;
                for id in self.watchers.get(&key).into_iter().flatten() {
                    self.hub.origin_lost(*id);
                }
                self.wake_key(&key);
            }
        }
        // Whatever was looked at may now differ from what its parents hold.
        for key in keys {
            for link in self.feeds.get(&key).into_iter().flat_map(Feed::links) {
                self.parents.entry(link.peer.clone()).or_default().pending.insert(key.clone());
            }
            for parent in self.parents.values_mut().filter(|p| p.acked.contains_key(&key)) {
                parent.pending.insert(key.clone());
            }
        }

        let Self { parents, feeds, hub, cfg, stats, revision, candidates, .. } = self;
        let mut out = Vec::new();
        parents.retain(|peer, parent| {
            if parent.inflight.is_some() {
                return true;
            }
            if !candidates.contains(peer) {
                return false;
            }
            if parent.retry_at.is_some_and(|at| now < at) {
                return true;
            }
            let linked = |feed: &Feed| feed.links().find(|l| l.peer == *peer).map(|l| l.epoch);
            let want = |key: &StreamKey, feed: &Feed, epoch| {
                let from = if feed.fresh { StartPosition::First } else { feed.wanted_from };
                let start = hub.cursor(key).ok().flatten().map_or(from, StartPosition::After);
                Want { key: key.clone(), epoch, start, snapshot: feed.wants_snapshot }
            };
            if parent.sync.is_none() && !parent.acked.is_empty() && parent.resync_at.is_some_and(|at| now >= at) {
                parent.sync = Some(None);
            }
            let mut interest = Interest { base: parent.base, ..Default::default() };
            let mut room = cfg.max_push_bytes.min(cfg.max_interest_bytes)
                - wire::INTEREST_HEADER
                - 2 * (wire::WANT_HEADER + cfg.hub.max_key_bytes);
            let mut fits = |interest: &Interest, key: &StreamKey| {
                let len = wire::want_len(key);
                let fits = interest.wants.len() + interest.drops.len() < cfg.max_keys_per_interest && len <= room;
                room -= if fits { len } else { 0 };
                fits
            };
            if let Some(from) = parent.sync.clone() {
                let mut to = None;
                let bounds = (from.as_ref().map_or(Unbounded, Excluded), Unbounded);
                for (key, feed) in feeds.range::<StreamKey, _>(bounds) {
                    let Some(epoch) = linked(feed) else { continue };
                    if !fits(&interest, key) {
                        to = interest.wants.last().map(|w| w.key.clone());
                        break;
                    }
                    interest.wants.push(want(key, feed, epoch));
                }
                interest.whole = Some((from, to));
            } else {
                let mut sent = Vec::new();
                while let Some(key) = parent.pending.pop_first() {
                    let feed = feeds.get(&key);
                    let epoch = feed.and_then(linked);
                    if epoch == parent.acked.get(&key).copied() {
                        continue;
                    }
                    if !fits(&interest, &key) {
                        parent.pending.insert(key);
                        break;
                    }
                    match (feed, epoch) {
                        (Some(feed), Some(epoch)) => interest.wants.push(want(&key, feed, epoch)),
                        _ => interest.drops.push(key.clone()),
                    }
                    sent.push(key);
                }
                // Checked again once the answer is in: what was wanted may have changed meanwhile.
                parent.pending.extend(sent);
                let due = !parent.acked.is_empty() && parent.refresh_at.is_none_or(|at| now >= at);
                if interest.wants.is_empty() && interest.drops.is_empty() && !due {
                    return !(parent.acked.is_empty() && parent.pending.is_empty());
                }
            }
            *revision += 1;
            interest.revision = *revision;
            stats.interests_sent += 1;
            stats.interest_keys += (interest.wants.len() + interest.drops.len()) as u64;
            parent.inflight = Some(Sent {
                revision: *revision,
                wants: interest.wants.iter().map(|w| (w.key.clone(), w.epoch)).collect(),
                drops: interest.drops.clone(),
                whole: interest.whole.clone(),
            });
            out.push((peer.clone(), interest));
            true
        });
        out
    }

    /// The membership changed, which may have mended a parent's refusal for a lost upstream or an unknown key: such a
    /// parent is asked again at the next `reconcile`, its backoff started over, and so is one that refuses so within
    /// `retry_refused`.
    #[cfg(feature = "cluster")]
    pub(crate) fn membership_changed(&mut self, now: Instant) {
        for (key, feed) in &mut self.feeds {
            feed.view_changed = Some(now);
            let before = feed.refused.len();
            feed.refused.retain(|_, r| !matches!(r.status, Status::Unavailable | Status::UnknownStream));
            if feed.refused.len() != before {
                self.dirty.insert(key.clone());
            }
        }
    }

    /// Nodes that left the view since the last [`reconcile`](Self::reconcile), and may be back in it already, which
    /// the next one would not see: a parent on one is taken as new, its time of appearing renewed and its refusals
    /// dropped.
    pub fn parents_left<'a>(&mut self, nodes: impl IntoIterator<Item = &'a str>, now: Instant) {
        for node in nodes {
            self.channels.remove(node);
            let on = |peer: &PeerId| *peer.node == *node;
            self.available_since.iter_mut().filter(|(p, _)| on(p)).for_each(|(_, since)| *since = now);
            for (key, feed) in &mut self.feeds {
                let before = feed.refused.len();
                feed.refused.retain(|p, _| !on(p));
                if feed.refused.len() != before {
                    self.dirty.insert(key.clone());
                }
            }
        }
    }

    /// What came back for an interest returned by [`reconcile`](Self::reconcile).
    pub fn interest_result(&mut self, parent: &PeerId, revision: u64, reply: impl Into<Reply>, now: Instant) {
        let Some(entry) = self.parents.get_mut(parent) else { return };
        let Some(sent) = entry.inflight.take_if(|sent| sent.revision == revision) else { return };
        self.wakes.push(Wake::Reconcile);
        let answers = match reply.into() {
            Reply::Answers(answers) if answers.len() == sent.wants.len() => answers,
            Reply::OutOfSync => {
                self.stats.resyncs += 1;
                entry.acked.clear();
                (entry.base, entry.sync) = (0, Some(None));
                // The parent holds nothing of this child, or another revision of it: what the child followed of its
                // pushes may be a boot it retired wrongly, and the parent's next channel starts from 1 anyway.
                self.channels.remove(&*parent.node);
                return;
            }
            _ => {
                self.stats.interests_failed += 1;
                entry.retry_at = Some(now + self.cfg.retry_failed);
                return;
            }
        };
        (entry.retry_at, entry.refresh_at) = (None, Some(now + self.cfg.refresh));
        if sent.whole.is_some() || !sent.wants.is_empty() || !sent.drops.is_empty() {
            // A check changes nothing on the parent, so a lost answer to one costs nothing.
            entry.base = revision;
        }
        entry.resync_at.get_or_insert(now + self.cfg.resync);
        if let Some((from, to)) = &sent.whole {
            let bounds = (from.as_ref().map_or(Unbounded, Excluded), to.as_ref().map_or(Unbounded, Included));
            let stale: Vec<StreamKey> = entry.acked.range::<StreamKey, _>(bounds).map(|(k, _)| k.clone()).collect();
            for key in stale {
                entry.acked.remove(&key);
            }
            entry.sync = to.clone().map(Some);
            entry.pending.extend(sent.wants.iter().map(|(key, _)| key.clone()));
            if to.is_none() {
                entry.resync_at = Some(now + self.cfg.resync);
            }
        }
        for key in &sent.drops {
            entry.acked.remove(key);
        }
        for ((key, epoch), answer) in sent.wants.iter().zip(&answers) {
            match answer.status {
                Status::Ok => entry.acked.insert(key.clone(), *epoch),
                _ => entry.acked.remove(key),
            };
        }
        if entry.acked.is_empty() {
            // The parent drops a child it holds nothing for, and with it the revision.
            entry.base = 0;
        }
        for ((key, epoch), Answer { status, path }) in sent.wants.into_iter().zip(answers) {
            let link = Link { peer: parent.clone(), epoch };
            let current = self.feeds.get(&key).is_some_and(|f| f.current.as_ref() == Some(&link));
            match status {
                Status::Ok => self.learn_path(&key, parent, epoch, path, now),
                Status::GenerationMismatch if current => {
                    self.local_reset(&key, ResetReason::GenerationChanged, now);
                    self.rebranch(&key, &link);
                }
                status => self.refuse(&key, parent, epoch, status, now),
            }
        }
    }

    /// The parent of each pulled stream, for tests and tooling.
    pub fn parent_of(&self, key: &StreamKey) -> Option<&PeerId> {
        self.feeds.get(key)?.current.as_ref().map(|l| &l.peer)
    }

    /// The first `limit` streams in key order, and how many there are.
    pub fn streams(&self, limit: usize, now: Instant) -> (Vec<StreamInfo>, usize) {
        #[expect(clippy::disallowed_methods, reason = "the hash order never shows: sorted on the next line")]
        let mut keys: Vec<&StreamKey> = self.originated.iter().chain(self.feeds.keys()).collect();
        keys.sort();
        let list = keys.iter().take(limit).map(|key| {
            let (cursor, feed) = (self.cursor(key), self.feeds.get(*key));
            let local = self.watchers.get(*key).map_or(0, Vec::len);
            let downstream = self.fed.get(*key).map_or(0, Vec::len);
            let node = |link: &Option<Link>| link.as_ref().map(|l| l.peer.node.to_string());
            let mut refused: Vec<String> = (feed.iter().flat_map(|f| &f.refused))
                .filter(|(_, r)| r.until > now)
                .map(|(peer, r)| format!("{} answered {}", peer.node, STATUSES[r.status as usize].replace('_', " ")))
                .collect();
            refused.sort();
            StreamInfo {
                namespace: key.namespace().to_string(),
                name: key.name().to_string(),
                generation: cursor.map(|c| c.generation),
                sequence: cursor.map(|c| c.sequence),
                origin: feed.is_none(),
                parent: feed.and_then(|f| node(&f.current)),
                joining: feed.and_then(|f| node(&f.joining)),
                path: self.upstream(key).iter().map(|n| n.to_string()).collect(),
                refused,
                local_subscribers: local,
                downstream_branches: downstream,
            }
        });
        (list.collect(), keys.len())
    }

    /// The `parent_lost`, `reset`, `orphaned` and `parent_restored` noted since this was last called: per kind, the first stream, its
    /// detail, and how many streams it stands for.
    pub fn take_timeline(&mut self) -> BTreeMap<&'static str, (String, String, u64)> {
        std::mem::take(&mut self.timeline)
    }

    /// Recent events, oldest first. `wall_ms` is the wall clock at `now`.
    pub fn events(&self, now: Instant, wall_ms: u64) -> Vec<RelayEvent> {
        let at = |then: &Instant| wall_ms.saturating_sub(now.saturating_duration_since(*then).as_millis() as u64);
        (self.events.iter())
            .map(|(then, kind, stream, detail)| RelayEvent {
                time_ms: at(then),
                kind,
                stream: stream.clone(),
                detail: detail.clone(),
            })
            .collect()
    }

    pub fn stats(&self) -> RelayStats {
        let (mut parents, mut orphaned) = (BTreeMap::<&str, ParentStats>::new(), 0);
        for feed in self.feeds.values() {
            orphaned += feed.current.is_none() as usize;
            for (link, joining) in [(&feed.current, false), (&feed.joining, true)] {
                let Some(link) = link else { continue };
                let entry = parents
                    .entry(&link.peer.node)
                    .or_insert_with(|| ParentStats { node: link.peer.node.to_string(), ..ParentStats::default() });
                *(if joining { &mut entry.joining } else { &mut entry.keys }) += 1;
            }
        }
        // In node and lane order, as the maps hold them.
        let downstream = self
            .channels()
            .map(|(d, lane, c)| DownstreamStats {
                node: d.peer.node.to_string(),
                lane,
                branches: c.branches.len(),
                // Not only the ready branches: one held at half its bound in flight is the child's backlog.
                queued_bytes: c.branches.iter().filter_map(|id| self.hub.queue_usage(*id)).map(|u| u.bytes).sum(),
                pushed_bytes: c.pushed_bytes,
                pushed_records: c.pushed_records,
                channel: c.channel,
                window_bytes: c.window,
                inflight_bytes: c.inflight,
                inflight_pushes: c.unacked.len(),
                round_trip_us: c.rtt.map(|rtt| rtt.as_micros() as u64),
                delivery_rate: c.rates.front().map_or(0, |(_, rate)| *rate),
                resent: c.resent,
            })
            .collect();
        RelayStats {
            hub: self.hub.stats(),
            originated: self.originated.len(),
            feeds: self.feeds.len(),
            orphaned,
            retained_bytes: lock(&self.store).total_bytes() + self.hub.state_bytes(),
            parents: parents.into_values().collect(),
            downstream,
            disk: self.disk.as_ref().map(DiskStore::stats),
            ..self.stats.clone()
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
