//! The two state machines of Paxos leases (spec/paxos-leases.md), and the acceptor's log. No clock: every call is
//! handed `now`, monotonic, and where an epoch is picked or a floor set, `unix_us`. The log is read at the start and
//! its writes handed out as `FileOp`s, their results handed back.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use axon::files::{FileOp, Files, crc32};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::wire::{get_bool, get_str, put_str, take};
use crate::{Holder, OwnershipError};

/// The route's schema: `Cluster::serve` refuses any other, so bodies carry none.
pub(crate) const SCHEMA: u16 = 3;
/// The schema of `held_by`'s route, its own: 4 asks for lapsed leases with a tag, where 3 sent an empty node, which an
/// acceptor of an older build reads as a node of that name and answers with nothing.
pub(crate) const HELD_SCHEMA: u16 = 4;
pub(crate) const MAX_ITEMS: usize = 512;
/// Requests one proposer has in flight to one acceptor in each lane. What it asks meanwhile waits and then
/// leaves as one request, so a burst of keys costs a few requests, not one per key. One, not
/// more: a node placing keys as fast as it can then sends fewer, fuller requests.
const IN_FLIGHT: usize = 1;
/// The lanes of requests to one acceptor, so that one whose answers wait for its log never holds up one whose answers
/// do not, and a renewal waits for no other key's write. Prepares, and renewals of a ballot the acceptor accepted
/// (logged before it answered), go quick; grants, releases and renewals it may grant now are logged.
const QUICK: usize = 0;
const LOGGED: usize = 1;
const BATCH_BYTES: usize = 64 * 1024;
pub(crate) const MAX_LEASE: Duration = Duration::from_secs(300);

const PREPARE: u8 = 1;
const PROPOSE: u8 = 2;
const RELEASE: u8 = 3;

const PROMISED: u8 = 1;
const ACCEPTED: u8 = 2;
const REFUSED: u8 = 3;
const WARMING: u8 = 4;
const FULL: u8 = 5;

/// `node` names one process: the node id and its incarnation, so a restarted node does not take
/// the leases of the process before it for its own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Ballot {
    pub epoch: u64,
    pub node: Arc<str>,
}

impl Ballot {
    /// Only a larger epoch, or the very ballot promised: two nodes never win with one epoch, so
    /// the epoch alone fences. The model found the usual `(epoch, node)` order breaks that.
    fn admitted_by(&self, promise: &Ballot) -> bool {
        self.epoch > promise.epoch || self == promise
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Item {
    pub key: Arc<str>,
    pub ballot: Ballot,
    pub kind: Kind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Prepare,
    /// For how long.
    Propose(Duration),
    Release,
}

impl Item {
    fn size(&self) -> usize {
        self.key.len() + self.ballot.node.len() + 24
    }
}

/// The node of a ballot's process, `node/incarnation`.
fn node_of(process: &str) -> &str {
    process.rsplit_once('/').map_or(process, |(node, _)| node)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Answer {
    /// The process of another node whose live lease the acceptor holds for the key, if any.
    Promised(Option<Arc<str>>),
    /// Also the answer to a release.
    Accepted,
    /// With the epoch it promised.
    Refused(u64),
    /// Restarted less than a lease time ago.
    Warming,
    Full,
}

fn count(body: &mut Bytes) -> Option<usize> {
    Some(take(body, 2)?.get_u16() as usize).filter(|n| *n <= MAX_ITEMS)
}

/// One proposer sends the whole request, so its name goes once, ahead of the items.
pub(crate) fn encode_items(items: &[Item]) -> Bytes {
    let node = items.first().map_or("", |i| &i.ballot.node);
    debug_assert!(items.iter().all(|i| *i.ballot.node == *node), "one proposer per request");
    let mut out = BytesMut::new();
    out.put_u16(items.len() as u16);
    put_str(&mut out, node);
    for Item { key, ballot, kind } in items {
        out.put_u8(match kind {
            Kind::Prepare => PREPARE,
            Kind::Propose(_) => PROPOSE,
            Kind::Release => RELEASE,
        });
        put_str(&mut out, key);
        out.put_u64(ballot.epoch);
        if let Kind::Propose(lease) = kind {
            out.put_u32(lease.as_millis().min(u32::MAX as u128) as u32);
        }
    }
    out.freeze()
}

pub(crate) fn decode_items(mut body: Bytes) -> Option<Vec<Item>> {
    let n = count(&mut body)?;
    let node: Arc<str> = get_str(&mut body)?.into();
    let mut items = Vec::with_capacity(n);
    for _ in 0..n {
        let code = take(&mut body, 1)?.get_u8();
        let (key, epoch) = (get_str(&mut body)?.into(), take(&mut body, 8)?.get_u64());
        let ballot = Ballot { epoch, node: node.clone() };
        let kind = match code {
            PREPARE => Kind::Prepare,
            PROPOSE => Kind::Propose(Duration::from_millis(take(&mut body, 4)?.get_u32().into())),
            RELEASE => Kind::Release,
            _ => return None,
        };
        items.push(Item { key, ballot, kind });
    }
    body.is_empty().then_some(items)
}

pub(crate) fn encode_answers(answers: &[Answer]) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u16(answers.len() as u16);
    for answer in answers {
        match answer {
            Answer::Promised(other) => {
                out.put_u8(PROMISED);
                out.put_u8(other.is_some().into());
                if let Some(other) = other {
                    put_str(&mut out, other);
                }
            }
            Answer::Accepted => out.put_u8(ACCEPTED),
            Answer::Refused(epoch) => {
                out.put_u8(REFUSED);
                out.put_u64(*epoch);
            }
            Answer::Warming => out.put_u8(WARMING),
            Answer::Full => out.put_u8(FULL),
        }
    }
    out.freeze()
}

pub(crate) fn decode_answers(mut body: Bytes) -> Option<Vec<Answer>> {
    let n = count(&mut body)?;
    let mut answers = Vec::with_capacity(n);
    for _ in 0..n {
        answers.push(match take(&mut body, 1)?.get_u8() {
            PROMISED => Answer::Promised(match get_bool(&mut body)? {
                true => Some(get_str(&mut body)?.into()),
                false => None,
            }),
            ACCEPTED => Answer::Accepted,
            REFUSED => Answer::Refused(take(&mut body, 8)?.get_u64()),
            WARMING => Answer::Warming,
            FULL => Answer::Full,
            _ => return None,
        });
    }
    body.is_empty().then_some(answers)
}

/// Most bytes of keys in one answer to `held_by`, well inside a frame: a longer list is asked for page by page.
const HELD_PAGE: usize = 64 << 10;
/// How long a lease that ran out unreleased stays listed by `held_by`. A node is seen gone well within it, however
/// short the lease: the transport's idle timeout, a keepalive and `rebalance_settle`.
const LISTED: Duration = Duration::from_secs(60);

fn put_strs<'a>(out: &mut BytesMut, strs: impl IntoIterator<Item = &'a str>) {
    strs.into_iter().for_each(|s| put_str(out, s));
}

fn get_strs(mut body: Bytes) -> Option<Vec<String>> {
    let mut strs = Vec::new();
    while !body.is_empty() {
        strs.push(get_str(&mut body)?);
    }
    Some(strs)
}

/// Asks `held_by`: `0 | node` or `1` for lapsed leases, the key after which the page starts (empty for the first), then
/// each kind.
pub(crate) fn encode_held_ask(holder: Holder<'_>, after: &str, kinds: &[String]) -> Bytes {
    let mut out = BytesMut::new();
    match holder {
        Holder::Node(node) => {
            out.put_u8(0);
            put_str(&mut out, node);
        }
        Holder::Lapsed => out.put_u8(1),
    }
    put_strs(&mut out, [after].into_iter().chain(kinds.iter().map(String::as_str)));
    out.freeze()
}

/// The node asked about, `None` for lapsed leases, the key after which the page starts, and the kinds.
pub(crate) fn decode_held_ask(mut body: Bytes) -> Option<(Option<String>, String, Vec<String>)> {
    let node = match take(&mut body, 1)?.get_u8() {
        0 => Some(get_str(&mut body)?),
        1 => None,
        _ => return None,
    };
    let mut strs = get_strs(body)?.into_iter();
    Some((node, strs.next()?, strs.collect()))
}

/// The answer to `held_by`: whether another page follows, then each key.
pub(crate) fn encode_held(keys: &[Arc<str>], more: bool) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u8(more.into());
    put_strs(&mut out, keys.iter().map(|k| &**k));
    out.freeze()
}

pub(crate) fn decode_held(mut body: Bytes) -> Option<(Vec<String>, bool)> {
    let more = get_bool(&mut body)?;
    Some((get_strs(body)?, more))
}

struct Slot {
    promise: Ballot,
    lease: Option<(Ballot, Duration)>,
    touched: Duration,
    /// The log's frame of its last grant or release: an answer about the key waits until it is synced.
    logged: u64,
}

impl Slot {
    fn live(&self, now: Duration) -> Option<&Ballot> {
        self.lease.as_ref().filter(|(_, until)| now < *until).map(|(b, _)| b)
    }

    /// Whether its lease, live or run out, was granted under `ballot`.
    fn held_under(&self, ballot: &Ballot) -> bool {
        self.lease.as_ref().is_some_and(|(b, _)| b == ballot)
    }
}

/// The acceptor's log: who holds each key, so that a restart of every acceptor at once still lists the keys a node
/// held. One file of frames `len u32 | crc u32 | body`, big-endian: a header (`AXLS`, the version, the configuration's
/// protocol id), then a frame per change of a key's holder (`GRANT` key, epoch, holder), per release (`RELEASE` key,
/// epoch) and per lapsed lease the sweep forgets (`FORGET` key). Renewals write nothing. A rewrite replaces the file
/// whole with the header, one `GRANT` per lease and `TOP` (the highest epoch granted), so the epochs of leases released
/// or forgotten still raise the floor of the next start. A build that does not know `TOP` reads up to it.
const LOG_FILE: &str = "leases.log";
const LOG_MAGIC: &[u8; 4] = b"AXLS";
/// Version 1 kept numbered files, `leases-<n>.log`: they are not read, and left where they are.
const LOG_VERSION: u16 = 2;
const LOG_HEADER: u8 = 0;
const LOG_GRANT: u8 = 1;
const LOG_RELEASE: u8 = 2;
const LOG_FORGET: u8 = 3;
const LOG_TOP: u8 = 4;
/// The log is rewritten once it holds this much and four times what its leases would take rewritten: three quarters
/// of it dead, released, forgotten or granted again, never because it holds more leases.
const COMPACT_AT: u64 = 1 << 20;
/// Bytes of frames not handed to a write yet past which a request that would grant a key is not answered.
const MAX_UNWRITTEN: usize = 4 << 20;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Record {
    Grant(Arc<str>, Ballot),
    Release(Arc<str>, u64),
    Forget(Arc<str>),
    Top(u64),
}

fn put_frame(out: &mut BytesMut, body: impl FnOnce(&mut BytesMut)) {
    let at = out.len();
    out.put_u64(0);
    body(out);
    let len = (out.len() - at - 8) as u32;
    let crc = crc32(&out[at + 8..]);
    out[at..at + 4].copy_from_slice(&len.to_be_bytes());
    out[at + 4..at + 8].copy_from_slice(&crc.to_be_bytes());
}

fn put_log_header(out: &mut BytesMut, protocol: u64) {
    put_frame(out, |out| {
        out.put_u8(LOG_HEADER);
        out.put_slice(LOG_MAGIC);
        out.put_u16(LOG_VERSION);
        out.put_u64(protocol);
    });
}

fn put_record(out: &mut BytesMut, record: &Record) {
    put_frame(out, |out| match record {
        Record::Grant(key, ballot) => {
            out.put_u8(LOG_GRANT);
            put_str(out, key);
            out.put_u64(ballot.epoch);
            put_str(out, &ballot.node);
        }
        Record::Release(key, epoch) => {
            out.put_u8(LOG_RELEASE);
            put_str(out, key);
            out.put_u64(*epoch);
        }
        Record::Forget(key) => {
            out.put_u8(LOG_FORGET);
            put_str(out, key);
        }
        Record::Top(epoch) => {
            out.put_u8(LOG_TOP);
            out.put_u64(*epoch);
        }
    });
}

/// The protocol id the log was written for and its records, up to its first frame that is torn or fails its CRC, or
/// `None` when it does not begin with a header of this version.
fn decode_log(mut data: Bytes) -> Option<(u64, Vec<Record>)> {
    let mut frames = std::iter::from_fn(|| {
        let (len, crc) = (u32::from_be_bytes(data.get(..4)?.try_into().ok()?), data.get(4..8)?);
        let body = data.get(8..8 + len as usize)?;
        (crc32(body).to_be_bytes() == crc).then(|| {
            let mut frame = data.split_to(8 + len as usize);
            frame.split_off(8)
        })
    });
    let mut header = frames.next()?;
    let expected = [&[LOG_HEADER][..], LOG_MAGIC, &LOG_VERSION.to_be_bytes()].concat();
    if header.len() != expected.len() + 8 || !header.starts_with(&expected) {
        return None;
    }
    let protocol = header.split_off(expected.len()).get_u64();
    let decode = |mut body: Bytes| -> Option<Record> {
        let code = take(&mut body, 1)?.get_u8();
        if code == LOG_TOP {
            let epoch = take(&mut body, 8)?.get_u64();
            return body.is_empty().then_some(Record::Top(epoch));
        }
        let key: Arc<str> = get_str(&mut body)?.into();
        let record = match code {
            LOG_GRANT => {
                let epoch = take(&mut body, 8)?.get_u64();
                Record::Grant(key, Ballot { epoch, node: get_str(&mut body)?.into() })
            }
            LOG_RELEASE => Record::Release(key, take(&mut body, 8)?.get_u64()),
            LOG_FORGET => Record::Forget(key),
            _ => return None,
        };
        body.is_empty().then_some(record)
    };
    Some((protocol, frames.map_while(decode).collect()))
}

/// What an acceptor's log held when it started: the leases it accepted and that were not released, and the highest
/// epoch it granted.
#[derive(Default)]
pub(crate) struct Stored {
    pub leases: BTreeMap<Arc<str>, Ballot>,
    pub top: u64,
}

impl Stored {
    /// Replays the log written for `protocol`. One of another version holds nothing, and one of another configuration
    /// only its highest epoch: raising the floor only refuses more, so epochs grow across a change of `lease`, `drift`
    /// or acceptors too. A `leases.log` that cannot be read, or is a directory, fails the start, naming it: the
    /// rewrite could not replace it.
    pub async fn load(files: &dyn Files, protocol: u64) -> std::io::Result<Self> {
        let mut stored = Stored::default();
        if !files.list().await?.iter().any(|(name, _)| name == LOG_FILE) {
            return Ok(stored);
        }
        let data = files.read(LOG_FILE).await.map_err(|e| std::io::Error::new(e.kind(), format!("{LOG_FILE}: {e}")))?;
        let (written_for, records) = decode_log(data.into()).unwrap_or_default();
        for record in records {
            if let Record::Grant(_, Ballot { epoch, .. }) | Record::Top(epoch) = &record {
                stored.top = stored.top.max(*epoch);
            }
            if written_for != protocol {
                continue;
            }
            match record {
                Record::Grant(key, ballot) => drop(stored.leases.insert(key, ballot)),
                Record::Release(key, epoch) => {
                    if stored.leases.get(&key).is_some_and(|b| b.epoch == epoch) {
                        stored.leases.remove(&key);
                    }
                }
                Record::Forget(key) => drop(stored.leases.remove(&key)),
                Record::Top(_) => {}
            }
        }
        Ok(stored)
    }
}

/// A write handed out and not yet back: the frames it goes through and the bytes the log holds once it lands, and for
/// a rewrite the leases it holds.
struct Writing {
    through: u64,
    bytes: u64,
    rewrite: Option<usize>,
}

struct Log {
    protocol: u64,
    bytes: u64,
    /// Bytes a lease takes in a rewrite: the last rewrite's per lease, or the first grant's frame before one held any.
    per_lease: u64,
    unwritten: BytesMut,
    /// Frames appended, and through which the last write that landed went.
    appended: u64,
    synced: u64,
    /// The next write rewrites the whole log: after the start, a failed write, or a log grown large.
    compact: bool,
    /// The last write failed, or the one out has not landed within the round's patience.
    failing: bool,
    writing: Option<Writing>,
}

impl Log {
    /// Appends `record`, and returns its frame.
    fn record(&mut self, record: Record) -> u64 {
        let at = self.unwritten.len();
        put_record(&mut self.unwritten, &record);
        if self.per_lease == 0 && matches!(record, Record::Grant(..)) {
            self.per_lease = (self.unwritten.len() - at) as u64;
        }
        self.appended += 1;
        self.appended
    }
}

pub(crate) struct Acceptor {
    ready_at: Duration,
    /// The epoch every key without a slot has promised: the clock when the wait ends, then
    /// raised by each dropped slot's promise.
    floor: u64,
    /// The highest epoch granted, here or by the log this start read: each rewrite keeps it.
    top: u64,
    slots: BTreeMap<Arc<str>, Slot>,
    /// Slots with a lease, live or not: what the log holds.
    stored: usize,
    max: usize,
    /// The restart wait; a slot without a live lease is kept this long after its last use.
    wait: Duration,
    log: Option<Log>,
    pub refused: u64,
    /// Prepares and proposes whose epoch was further ahead of this clock than the restart wait.
    pub clock_behind: u64,
    /// Proposes answered `Full` because they would grant a key while the log is `MAX_UNWRITTEN` behind.
    pub store_behind: u64,
}

impl Acceptor {
    /// Refuses everything for `wait`: whatever it accepted before this start has run out by then.
    /// The floor is its clock when the wait ends, so an epoch granted before the restart cannot
    /// win again however late its messages arrive, as long as clocks are less than `wait` apart.
    pub fn new(now: Duration, unix_us: u64, wait: Duration, max: usize) -> Self {
        Self {
            ready_at: now + wait,
            floor: unix_us.saturating_add(wait.as_micros() as u64),
            top: 0,
            slots: BTreeMap::new(),
            stored: 0,
            max,
            wait,
            log: None,
            refused: 0,
            clock_behind: 0,
            store_behind: 0,
        }
    }

    /// Keeps a log, starting from what the files held. The floor is at least the highest epoch they hold granted, so
    /// a key's epoch never goes back, however far the clock stepped back while the acceptor was down. Each lease they
    /// hold comes back for `held_by` and a release to match, already run out when the wait ends, and the key promises
    /// only the floor: the epoch it was granted under is not above the floor, and taken back as a promise it would
    /// admit that very ballot's late propose.
    pub fn with_log(mut self, stored: Stored, protocol: u64, now: Duration) -> Self {
        (self.top, self.floor) = (stored.top, self.floor.max(stored.top));
        let (until, floor) = (self.ready_at, Ballot { epoch: self.floor, node: "".into() });
        for (key, ballot) in stored.leases {
            let slot = Slot { promise: floor.clone(), lease: Some((ballot, until)), touched: now, logged: 0 };
            self.slots.insert(key, slot);
        }
        self.stored = self.slots.values().filter(|s| s.lease.is_some()).count();
        self.log = Some(Log {
            protocol,
            bytes: 0,
            per_lease: 0,
            unwritten: BytesMut::new(),
            appended: 0,
            synced: 0,
            compact: true,
            failing: false,
            writing: None,
        });
        self
    }

    pub fn warming(&self, now: Duration) -> bool {
        now < self.ready_at
    }

    pub fn floor(&self) -> u64 {
        self.floor
    }

    pub fn leases(&self, now: Duration) -> usize {
        self.slots.values().filter(|s| s.live(now).is_some()).count()
    }

    /// Leases the log holds: accepted, not released and not forgotten.
    pub fn stored(&self) -> usize {
        self.stored
    }

    /// `stored`, counted again: what the simulation holds it to.
    #[cfg(test)]
    pub fn stored_counted(&self) -> usize {
        self.slots.values().filter(|s| s.lease.is_some()).count()
    }

    /// The frame the answers to `items` wait for, while it is not synced: the last grant or release of the keys they
    /// propose or release. A promise tells nothing the log holds. None without a log, or while it is failing.
    #[cfg(test)]
    pub fn waits_for(&self, items: &[Item]) -> Option<u64> {
        let log = self.log.as_ref().filter(|l| !l.failing && l.appended > l.synced)?;
        let proposed = items.iter().filter(|i| i.kind != Kind::Prepare);
        let logged = proposed.filter_map(|i| self.slots.get(&i.key)).map(|s| s.logged).max()?;
        (logged > log.synced).then_some(logged)
    }

    pub fn synced(&self) -> u64 {
        self.log.as_ref().map_or(0, |l| l.synced)
    }

    pub fn store_failing(&self) -> bool {
        self.log.as_ref().is_some_and(|l| l.failing)
    }

    #[cfg(test)]
    pub fn answer(&mut self, item: &Item, now: Duration, unix_us: u64) -> Answer {
        self.answer_logged(item, now, unix_us).0
    }

    /// Answers each of `items`, and the frame the answers wait for while it is not synced: the last grant or release of
    /// the keys they propose or release, read as each is answered (`waits_for`). None without a log, or while it is
    /// failing.
    pub fn answer_all(&mut self, items: &[Item], now: Duration, unix_us: u64) -> (Vec<Answer>, Option<u64>) {
        let mut logged = 0;
        let mut answer = |item: &Item| {
            let (answer, frame) = self.answer_logged(item, now, unix_us);
            if item.kind != Kind::Prepare {
                logged = logged.max(frame);
            }
            answer
        };
        let answers = items.iter().map(&mut answer).collect();
        let log = self.log.as_ref().filter(|l| !l.failing);
        (answers, log.and_then(|l| (logged > l.synced).then_some(logged)))
    }

    /// The answer, and the last frame logged of the key, 0 for none.
    fn answer_logged(&mut self, item: &Item, now: Duration, unix_us: u64) -> (Answer, u64) {
        if self.warming(now) {
            return (Answer::Warming, 0);
        }
        let Item { key, ballot, kind } = item;
        if *kind == Kind::Release {
            let slot = self.slots.get_mut(key);
            let logged = match slot {
                Some(slot) if slot.held_under(ballot) => {
                    slot.lease = None;
                    self.stored -= 1;
                    if let Some(log) = &mut self.log {
                        slot.logged = log.record(Record::Release(key.clone(), ballot.epoch));
                    }
                    slot.logged
                }
                Some(slot) => slot.logged,
                None => 0,
            };
            return (Answer::Accepted, logged);
        }
        if ballot.epoch > unix_us.saturating_add(self.wait.as_micros() as u64) {
            self.clock_behind += 1;
        }
        // The log stays a bounded distance behind: a propose that would grant waits for it, the others go through.
        let grants = || !self.slots.get(key).is_some_and(|s| s.held_under(ballot));
        let behind = self.log.as_ref().is_some_and(|l| l.unwritten.len() >= MAX_UNWRITTEN);
        if behind && matches!(kind, Kind::Propose(_)) && grants() {
            self.store_behind += 1;
            return (Answer::Full, 0);
        }
        let len = self.slots.len();
        let slot = match self.slots.entry(key.clone()) {
            Entry::Occupied(slot) => slot.into_mut(),
            Entry::Vacant(_) if len >= self.max => return (Answer::Full, 0),
            Entry::Vacant(_) if ballot.epoch <= self.floor => {
                self.refused += 1;
                return (Answer::Refused(self.floor), 0);
            }
            Entry::Vacant(slot) => slot.insert(Slot { promise: ballot.clone(), lease: None, touched: now, logged: 0 }),
        };
        if !ballot.admitted_by(&slot.promise) {
            self.refused += 1;
            return (Answer::Refused(slot.promise.epoch), slot.logged);
        }
        // The proposer gives up on a promise that reports another node's lease, so recording it
        // would only refuse the holder's renewals.
        if *kind == Kind::Prepare
            && let Some(holder) = slot.live(now).filter(|b| b.node != ballot.node)
        {
            return (Answer::Promised(Some(holder.node.clone())), slot.logged);
        }
        slot.promise = ballot.clone();
        slot.touched = now;
        let answer = match kind {
            Kind::Propose(lease) => {
                let granted = !slot.held_under(ballot);
                self.top = self.top.max(ballot.epoch);
                self.stored += slot.lease.is_none() as usize;
                slot.lease = Some((ballot.clone(), now + (*lease).min(MAX_LEASE)));
                if let Some(log) = self.log.as_mut().filter(|_| granted) {
                    slot.logged = log.record(Record::Grant(key.clone(), ballot.clone()));
                }
                Answer::Accepted
            }
            _ => Answer::Promised(None),
        };
        (answer, slot.logged)
    }

    /// The keys of `kinds` after `after` whose last lease went to a process of `Holder::Node` and was not released, or
    /// for `Holder::Lapsed` every such key whose lease ran out, whoever held it: what a node that starts restarts of a
    /// node none of its peers saw go. Up to `HELD_PAGE` bytes of them and whether more follow, or `None` while warming.
    pub fn held_by(
        &self,
        holder: Holder<'_>,
        kinds: &[String],
        after: &str,
        now: Duration,
    ) -> Option<(Vec<Arc<str>>, bool)> {
        if self.warming(now) {
            return None;
        }
        let of = |slot: &Slot| match (&slot.lease, holder) {
            (Some(_), Holder::Lapsed) => slot.live(now).is_none(),
            (Some((b, _)), Holder::Node(node)) => node_of(&b.node) == node,
            (None, _) => false,
        };
        let wanted = |key: &str| key.split_once('/').is_some_and(|(kind, _)| kinds.iter().any(|k| k == kind));
        let (mut keys, mut bytes) = (Vec::new(), 0);
        for (key, slot) in self.slots.range::<str, _>((Bound::Excluded(after), Bound::Unbounded)) {
            if bytes >= HELD_PAGE {
                return Some((keys, true));
            }
            if of(slot) && wanted(key) {
                bytes += 2 + key.len();
                keys.push(key.clone());
            }
        }
        Some((keys, false))
    }

    /// Drops slots with neither a live lease nor recent use; their promises raise the floor,
    /// which only ever refuses more. A lease that ran out unreleased is kept `LISTED` longer, so that its holder's
    /// keys are still listed by `held_by` once the holder is seen gone, and then forgotten in the log too.
    pub fn sweep(&mut self, now: Duration) {
        let Self { floor, slots, stored, log, .. } = self;
        let wait = self.wait;
        slots.retain(|key, slot| {
            let leased = slot.lease.as_ref().is_some_and(|(_, until)| now < *until + wait.max(LISTED));
            let alive = leased || now < slot.touched + wait;
            if !alive {
                *floor = (*floor).max(slot.promise.epoch);
                *stored -= slot.lease.is_some() as usize;
                if let Some(log) = log.as_mut().filter(|_| slot.lease.is_some()) {
                    log.record(Record::Forget(key.clone()));
                }
            }
            alive
        });
    }

    /// The next write of the log, unless one is out or there is nothing to write: the whole log as one `GRANT` per
    /// lease and `TOP`, replacing the file, or the frames logged since the last.
    pub fn take_write(&mut self) -> Option<Vec<FileOp>> {
        let leases = self.stored;
        let log = self.log.as_mut().filter(|l| l.writing.is_none())?;
        let live = leases as u64 * log.per_lease;
        let (ops, writing) = if log.compact || log.bytes + log.unwritten.len() as u64 > COMPACT_AT.max(4 * live) {
            let mut out = BytesMut::with_capacity(live as usize + 64);
            put_log_header(&mut out, log.protocol);
            for (key, slot) in &self.slots {
                if let Some((ballot, _)) = &slot.lease {
                    put_record(&mut out, &Record::Grant(key.clone(), ballot.clone()));
                }
            }
            put_record(&mut out, &Record::Top(self.top));
            log.unwritten.clear();
            let bytes = out.len() as u64;
            let ops = vec![FileOp::Replace(LOG_FILE.into(), out.freeze())];
            (ops, Writing { through: log.appended, bytes, rewrite: Some(leases) })
        } else if !log.unwritten.is_empty() {
            let bytes = log.bytes + log.unwritten.len() as u64;
            let ops = vec![FileOp::Append(LOG_FILE.into(), log.unwritten.split().freeze())];
            (ops, Writing { through: log.appended, bytes, rewrite: None })
        } else {
            return None;
        };
        log.writing = Some(writing);
        Some(ops)
    }

    /// The write `take_write` handed out landed, or failed. After a failure answers no longer wait, and the next write
    /// rewrites the whole log, which also holds what this one did not write.
    pub fn written(&mut self, ok: bool) {
        let Some(log) = self.log.as_mut() else { return };
        let Some(Writing { through, bytes, rewrite }) = log.writing.take() else { return };
        log.failing = !ok;
        if !ok {
            log.compact = true;
            log.unwritten.clear();
            return;
        }
        (log.synced, log.bytes, log.compact) = (through, bytes, false);
        if let Some(leases @ 1..) = rewrite {
            log.per_lease = bytes / leases as u64;
        }
    }

    /// The write out has not landed within the round's patience: until it does, answers no longer wait, as after a
    /// failed write, and grants stop once `MAX_UNWRITTEN` is logged behind it.
    pub fn stalled(&mut self) {
        if let Some(log) = self.log.as_mut().filter(|l| l.writing.is_some()) {
            log.failing = true;
        }
    }
}

#[derive(Debug)]
pub(crate) enum Out {
    Send {
        request: u64,
        acceptor: usize,
        items: Vec<Item>,
    },
    /// The end of an `acquire`, or of a key's renewal: the fencing epoch, or why not.
    Done {
        key: Arc<str>,
        renew: bool,
        result: Result<u64, OwnershipError>,
    },
}

struct Held {
    ballot: Ballot,
    epoch: u64,
    until: Duration,
    /// The acceptors heard accepting `ballot`, each once it had logged it: renewals go to them quick.
    accepted: Set,
    /// The round that granted or last renewed it, whose acceptances after the majority's still count in `accepted`.
    round: u64,
}

pub(crate) const MAX_ACCEPTORS: usize = 7;

/// Acceptors by index, as bits: `MAX_ACCEPTORS` fits a byte.
#[derive(Clone, Copy, Default)]
struct Set(u8);

impl Set {
    const ALL: Set = Set(u8::MAX);

    fn insert(&mut self, acceptor: usize) {
        self.0 |= 1 << acceptor;
    }

    fn contains(self, acceptor: usize) -> bool {
        self.0 & 1 << acceptor != 0
    }

    fn len(self) -> usize {
        self.0.count_ones() as usize
    }
}

enum Stage {
    Prepare { promised: Set, holder: Option<Arc<str>> },
    Propose { sent: Duration, accepted: Set },
}

struct Round {
    id: u64,
    renew: bool,
    ballot: Ballot,
    stage: Stage,
    answered: Set,
    refused: bool,
    /// What tells the caller more than "no majority": what acceptors answered, or that some did not.
    why: Option<&'static str>,
    retried: bool,
    deadline: Duration,
}

#[derive(Default)]
struct Key {
    held: Option<Held>,
    round: Option<Round>,
}

fn end(renewals: &mut usize, key: &Arc<str>, k: &mut Key, result: Result<u64, OwnershipError>, out: &mut Vec<Out>) {
    let renew = set_round(renewals, k, None).is_some_and(|r| r.renew);
    if matches!(result, Err(OwnershipError::Lost)) {
        k.held = None;
    }
    // Renewals are many and the caller only needs to hear of those that failed.
    if !(renew && result.is_ok()) {
        out.push(Out::Done { key: key.clone(), renew, result });
    }
}

const NO_ANSWER: &str = "acceptors did not answer";

fn no_majority(what: &str, why: Option<&str>) -> OwnershipError {
    OwnershipError::Unavailable(why.map_or(what.into(), |why| format!("{what}: {why}")))
}

/// Every change of a key's round goes through here, so `renewals` counts the renewals out.
fn set_round(renewals: &mut usize, k: &mut Key, round: Option<Round>) -> Option<Round> {
    let renew = |r: &Option<Round>| usize::from(r.as_ref().is_some_and(|r| r.renew));
    *renewals = *renewals + renew(&round) - renew(&k.round);
    std::mem::replace(&mut k.round, round)
}

/// An item's key and the id of its round.
type Tag = (Arc<str>, u64);

pub(crate) struct Proposer {
    node: Arc<str>,
    /// Sent with every propose: how long acceptors keep the lease.
    lease: Duration,
    /// How long this node believes after sending a propose: less than `lease` by the drift
    /// allowance and a margin, so it stops first.
    hold: Duration,
    /// How long a round waits for its majority.
    round: Duration,
    seen: u64,
    keys: BTreeMap<Arc<str>, Key>,
    renewals: usize,
    next: u64,
    /// Request id -> its acceptor and lane, and (key, round id) per item. Round 0 is no round's.
    requests: BTreeMap<u64, (usize, usize, Vec<Tag>)>,
    /// Round id and item, per acceptor and lane.
    outbox: Vec<[VecDeque<(u64, Item)>; 2]>,
    in_flight: Vec<[usize; 2]>,
}

impl Proposer {
    pub fn new(node: Arc<str>, acceptors: usize, lease: Duration, hold: Duration, round: Duration) -> Self {
        Self {
            node,
            lease,
            hold,
            round,
            seen: 0,
            keys: BTreeMap::new(),
            renewals: 0,
            next: 0,
            requests: BTreeMap::new(),
            outbox: (0..acceptors).map(|_| Default::default()).collect(),
            in_flight: vec![[0; 2]; acceptors],
        }
    }

    fn majority(&self) -> usize {
        self.outbox.len() / 2 + 1
    }

    /// The fencing epoch of `key` while this node believes it holds it.
    pub fn holds(&self, key: &str, now: Duration) -> Option<u64> {
        self.keys.get(key)?.held.as_ref().filter(|h| now < h.until).map(|h| h.epoch)
    }

    /// Whether a renewal is still waiting for its majority.
    pub fn renewing(&self) -> bool {
        debug_assert_eq!(
            self.renewals,
            self.keys.values().filter(|k| k.round.as_ref().is_some_and(|r| r.renew)).count()
        );
        self.renewals > 0
    }

    pub fn held(&self, now: Duration) -> usize {
        self.keys.values().filter(|k| k.held.as_ref().is_some_and(|h| now < h.until)).count()
    }

    pub fn acquire(&mut self, key: Arc<str>, now: Duration, unix_us: u64, out: &mut Vec<Out>) {
        if let Some(epoch) = self.holds(&key, now) {
            out.push(Out::Done { key, renew: false, result: Ok(epoch) });
            return;
        }
        self.cancel(&key, out);
        self.prepare(key, false, false, now, unix_us);
        self.flush(out);
    }

    /// Extends every lease this node holds, batched per acceptor. A lease that ran out ends in
    /// `Done` with `Lost`, and so does a renewal that fails with it.
    pub fn renew(&mut self, now: Duration, out: &mut Vec<Out>) {
        self.tick(now, out);
        let due: Vec<(Item, Set)> = (self.keys.iter())
            .filter(|(_, k)| k.round.is_none())
            .filter_map(|(key, k)| {
                let held = k.held.as_ref()?;
                Some((
                    Item { key: key.clone(), ballot: held.ballot.clone(), kind: Kind::Propose(self.lease) },
                    held.accepted,
                ))
            })
            .collect();
        for (item, quick) in due {
            self.start_round(true, false, Stage::Propose { sent: now, accepted: Set::default() }, item, quick, now);
        }
        self.flush(out);
    }

    /// Stops believing first, then tells the acceptors. A lost release costs a lease time.
    pub fn release(&mut self, key: &str, out: &mut Vec<Out>) {
        let Some(key) = self.keys.get_key_value(key).map(|(key, _)| key.clone()) else { return };
        self.cancel(&key, out);
        if let Some(held) = self.keys.remove(&key).and_then(|k| k.held) {
            self.send_all(0, Item { key, ballot: held.ballot, kind: Kind::Release }, Set::default());
        }
        self.flush(out);
    }

    /// Releases `key` only while this node holds it at `epoch`: otherwise it may be on its way to another caller.
    pub fn release_at(&mut self, key: &str, epoch: u64, now: Duration, out: &mut Vec<Out>) {
        if self.holds(key, now) == Some(epoch) {
            self.release(key, out);
        }
    }

    pub fn release_all(&mut self, out: &mut Vec<Out>) {
        let keys: Vec<Arc<str>> = self.keys.keys().cloned().collect();
        keys.iter().for_each(|key| self.release(key, out));
    }

    /// The answers to `request`, or `None` when it failed or timed out.
    pub fn reply(
        &mut self,
        request: u64,
        answers: Option<Vec<Answer>>,
        now: Duration,
        unix_us: u64,
        out: &mut Vec<Out>,
    ) {
        let Some((acceptor, lane, tags)) = self.requests.remove(&request) else { return };
        self.in_flight[acceptor][lane] -= 1;
        let answers = answers.filter(|a| a.len() == tags.len());
        for (i, (key, round)) in tags.into_iter().enumerate() {
            let answer = answers.as_ref().map(|a| a[i].clone());
            self.answer(&key, round, acceptor, answer, now, unix_us, out);
        }
        self.flush(out);
    }

    /// Ends rounds that waited past their deadline and drops leases that ran out.
    pub fn tick(&mut self, now: Duration, out: &mut Vec<Out>) {
        let Self { keys, renewals, .. } = self;
        keys.retain(|key, k| {
            if let Some(r) = k.round.as_ref().filter(|r| now >= r.deadline) {
                let why = no_majority("no majority answered in time", r.why);
                end(renewals, key, k, Err(why), out);
            }
            // A lease that ran out unrenewed is lost, and said so: the renewal is how its actors learn.
            if k.held.as_ref().is_some_and(|h| now >= h.until) {
                k.held = None;
                out.push(Out::Done { key: key.clone(), renew: true, result: Err(OwnershipError::Lost) });
            }
            k.held.is_some() || k.round.is_some()
        });
    }

    fn next_id(&mut self) -> u64 {
        self.next += 1;
        self.next
    }

    /// The caller has taken the key over. Its renewal just ends: the others still decide it.
    fn cancel(&mut self, key: &Arc<str>, out: &mut Vec<Out>) {
        let Some(k) = self.keys.get_mut(key) else { return };
        match &k.round {
            Some(r) if r.renew => drop(set_round(&mut self.renewals, k, None)),
            Some(_) => end(&mut self.renewals, key, k, Err(OwnershipError::Unavailable("superseded".into())), out),
            None => {}
        }
    }

    fn prepare(&mut self, key: Arc<str>, renew: bool, retried: bool, now: Duration, unix_us: u64) {
        let epoch = unix_us.max(self.seen + 1);
        self.seen = epoch;
        let ballot = Ballot { epoch, node: self.node.clone() };
        let stage = Stage::Prepare { promised: Set::default(), holder: None };
        self.start_round(renew, retried, stage, Item { key, ballot, kind: Kind::Prepare }, Set::ALL, now);
    }

    fn start_round(&mut self, renew: bool, retried: bool, stage: Stage, item: Item, quick: Set, now: Duration) {
        let id = self.next_id();
        let round = Round {
            id,
            renew,
            ballot: item.ballot.clone(),
            stage,
            answered: Set::default(),
            refused: false,
            why: None,
            retried,
            deadline: now + self.round,
        };
        set_round(&mut self.renewals, self.keys.entry(item.key.clone()).or_default(), Some(round));
        self.send_all(id, item, quick);
    }

    /// To every acceptor, in the quick lane to those in `quick`.
    fn send_all(&mut self, round: u64, item: Item, quick: Set) {
        for (acceptor, lanes) in self.outbox.iter_mut().enumerate() {
            let lane = if quick.contains(acceptor) { QUICK } else { LOGGED };
            lanes[lane].push_back((round, item.clone()));
        }
    }

    fn flush(&mut self, out: &mut Vec<Out>) {
        for acceptor in 0..self.outbox.len() {
            for lane in [QUICK, LOGGED] {
                while self.in_flight[acceptor][lane] < IN_FLIGHT {
                    let (mut tags, mut items, mut bytes) = (Vec::new(), Vec::new(), 0);
                    while let Some((_, item)) = self.outbox[acceptor][lane].front()
                        && items.len() < MAX_ITEMS
                        && (items.is_empty() || bytes + item.size() <= BATCH_BYTES)
                        // A release leaves after a renewal of its key out in the quick lane, or could land first and
                        // the renewal grant the key again.
                        && !(item.kind == Kind::Release && self.quick_carries(acceptor, &item.key))
                    {
                        let (round, item) = self.outbox[acceptor][lane].pop_front().expect("front is Some");
                        // Of a round the key has moved on from: its answer counts for nothing, and a renewal of a key
                        // released since would grant it again.
                        let current = |k: &Key| k.round.as_ref().is_some_and(|r| r.id == round);
                        if item.kind != Kind::Release && !self.keys.get(&item.key).is_some_and(current) {
                            continue;
                        }
                        bytes += item.size();
                        tags.push((item.key.clone(), round));
                        items.push(item);
                    }
                    if items.is_empty() {
                        break;
                    }
                    let request = self.next_id();
                    self.requests.insert(request, (acceptor, lane, tags));
                    self.in_flight[acceptor][lane] += 1;
                    out.push(Out::Send { request, acceptor, items });
                }
            }
        }
    }

    /// Whether the request out to `acceptor` in the quick lane carries `key`.
    fn quick_carries(&self, acceptor: usize, key: &str) -> bool {
        let carries = |tags: &Vec<Tag>| tags.iter().any(|(k, _)| **k == *key);
        self.in_flight[acceptor][QUICK] > 0
            && self.requests.values().any(|(to, lane, tags)| *to == acceptor && *lane == QUICK && carries(tags))
    }

    #[allow(clippy::too_many_arguments)]
    fn answer(
        &mut self,
        key: &Arc<str>,
        round: u64,
        acceptor: usize,
        answer: Option<Answer>,
        now: Duration,
        unix_us: u64,
        out: &mut Vec<Out>,
    ) {
        let (majority, acceptors, hold, lease) = (self.majority(), self.outbox.len(), self.hold, self.lease);
        let Some(k) = self.keys.get_mut(key) else { return };
        if let (Some(held), Some(Answer::Accepted)) = (k.held.as_mut().filter(|h| h.round == round), &answer) {
            held.accepted.insert(acceptor);
        }
        let Some(r) = k.round.as_mut().filter(|r| r.id == round) else { return };
        r.answered.insert(acceptor);
        match answer {
            Some(Answer::Refused(epoch)) => {
                self.seen = self.seen.max(epoch);
                r.refused = true;
            }
            Some(Answer::Full) => r.why = Some("acceptors hold `max_leases` keys, or their lease log is behind"),
            // No answer says least: timed out, stopped or another list alike (the provider card's link says which).
            Some(Answer::Warming) if r.why.is_none_or(|w| w == NO_ANSWER) => {
                r.why = Some("acceptors are waiting out a restart")
            }
            None => r.why = r.why.or(Some(NO_ANSWER)),
            _ => {}
        }
        enum Next {
            Wait,
            Propose,
            Granted(Duration, Set),
            Failed,
        }
        let next = match (&mut r.stage, answer) {
            (Stage::Prepare { promised, holder }, Some(Answer::Promised(held))) => {
                if held.is_some() {
                    *holder = held;
                }
                promised.insert(acceptor);
                if promised.len() >= majority { Next::Propose } else { Next::Wait }
            }
            (Stage::Propose { sent, accepted }, Some(Answer::Accepted)) => {
                accepted.insert(acceptor);
                if accepted.len() >= majority { Next::Granted(*sent + hold, *accepted) } else { Next::Wait }
            }
            _ => Next::Wait,
        };
        let next = match next {
            Next::Wait if r.answered.len() == acceptors => Next::Failed,
            next => next,
        };
        match next {
            Next::Wait => {}
            Next::Propose => {
                let Stage::Prepare { holder, .. } = &r.stage else { return };
                if let Some(holder) = holder {
                    // The node, without the process: the caller asks the node.
                    let node = node_of(holder).to_owned();
                    let result = if r.renew { OwnershipError::Lost } else { OwnershipError::Held(Some(node)) };
                    end(&mut self.renewals, key, k, Err(result), out);
                    return;
                }
                let ballot = r.ballot.clone();
                r.stage = Stage::Propose { sent: now, accepted: Set::default() };
                r.answered = Set::default();
                r.refused = false;
                r.why = None;
                // A new id, so promises that arrive late are not counted against the proposes.
                self.next += 1;
                r.id = self.next;
                let id = r.id;
                self.send_all(id, Item { key: key.clone(), ballot, kind: Kind::Propose(lease) }, Set::default());
            }
            Next::Granted(until, accepted) => {
                let (renew, ballot, round) = (r.renew, r.ballot.clone(), r.id);
                let result = match k.held.as_mut() {
                    Some(held) if renew && now < held.until => {
                        if held.ballot != ballot {
                            held.accepted = Set::default();
                        }
                        held.accepted.0 |= accepted.0;
                        (held.until, held.ballot, held.round) = (until, ballot, round);
                        Ok(held.epoch)
                    }
                    _ if renew => Err(OwnershipError::Lost),
                    _ if now < until => {
                        let epoch = ballot.epoch;
                        k.held = Some(Held { epoch, ballot, until, accepted, round });
                        Ok(epoch)
                    }
                    _ => Err(OwnershipError::Unavailable("granted too late to use".into())),
                };
                end(&mut self.renewals, key, k, result, out);
            }
            Next::Failed => {
                let (renew, refused, retried, why) = (r.renew, r.refused, r.retried, r.why);
                let still_held = k.held.as_ref().is_some_and(|h| now < h.until);
                if refused && !retried && (!renew || still_held) {
                    // Someone promised a larger epoch: once more above it, keeping a lease this node holds.
                    self.prepare(key.clone(), renew, true, now, unix_us);
                } else {
                    end(&mut self.renewals, key, k, Err(no_majority("no majority", why)), out);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
