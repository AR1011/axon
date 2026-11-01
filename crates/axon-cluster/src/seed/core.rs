//! The two state machines of seed discovery. No I/O and no clock: every call is handed the
//! time, and answers with what to send. `now` is monotonic, `unix_ms` is the wall clock.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axon_net::frame::pack;
use axon_net::{Compression, Frame};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::SeedTimings;
use crate::wire::{get_map, get_str, put_map, put_str, take};
use crate::{DiscoveryError, NodeRecord};

/// 2: `SYNCED` carries the seed's timings.
pub(crate) const VERSION: u16 = 2;

pub(crate) const REGISTER: u16 = 1;
pub(crate) const LEAVE: u16 = 2;
pub(crate) const CHANGES: u16 = 3;
pub(crate) const SYNCED: u16 = 4;
pub(crate) const REFUSED: u16 = 5;

const PUT: u8 = 1;
const REMOVE: u8 = 2;

const STALE: u8 = 1;
const FULL: u8 = 2;
const INVALID: u8 = 3;

pub(crate) const MAX_FRAME: usize = 64 << 10;
pub(crate) const MAX_LIST: usize = 4 << 20;
pub(crate) const MAX_MEMBERS: usize = 4096;
pub(crate) const MAX_OBSERVERS: usize = 64;
const MAX_SEEDS: usize = 16;
const BATCH: Duration = Duration::from_millis(20);
const OBSERVER_AFTER: Duration = Duration::from_secs(10);
const WITHDRAW_AFTER: Duration = Duration::from_secs(4);
const RETRY: Duration = Duration::from_secs(2);
const AHEAD_MS: u64 = 60_000;
const PACK_FROM: usize = 512;

fn put_record(out: &mut BytesMut, r: &NodeRecord) {
    put_str(out, &r.id);
    out.put_u128(r.incarnation);
    match r.address.ip() {
        IpAddr::V4(ip) => {
            out.put_u8(4);
            out.put_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            out.put_u8(6);
            out.put_slice(&ip.octets());
        }
    }
    out.put_u16(r.address.port());
    out.put_u64(r.version);
    put_map(out, &r.metadata);
}

fn get_record(body: &mut Bytes) -> Option<NodeRecord> {
    let id = get_str(body)?;
    let incarnation = take(body, 16)?.get_u128();
    let ip = match take(body, 1)?.get_u8() {
        4 => IpAddr::V4(Ipv4Addr::from(take(body, 4)?.get_u32())),
        6 => IpAddr::V6(Ipv6Addr::from(take(body, 16)?.get_u128())),
        _ => return None,
    };
    let address = SocketAddr::new(ip, take(body, 2)?.get_u16());
    let version = take(body, 8)?.get_u64();
    let metadata = get_map(body)?;
    Some(NodeRecord { id, incarnation, address, metadata, version })
}

fn put_entry(r: &NodeRecord) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u8(PUT);
    put_record(&mut out, r);
    out.freeze()
}

fn remove_entry(id: &str, incarnation: u128) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u8(REMOVE);
    put_str(&mut out, id);
    out.put_u128(incarnation);
    out.freeze()
}

pub(crate) fn register_frame(r: &NodeRecord) -> Frame {
    let mut body = BytesMut::new();
    put_record(&mut body, r);
    frame(REGISTER, body.freeze())
}

pub(crate) fn frame(kind: u16, payload: Bytes) -> Frame {
    Frame { kind, schema: VERSION, payload, ..Frame::default() }
}

/// The body of `SYNCED`: each timing in ms, a u32, saturating.
pub(crate) fn put_timings(t: &SeedTimings) -> Bytes {
    let mut out = BytesMut::with_capacity(16);
    for d in [t.idle, t.idle_hold, t.closed_grace, t.warm_up] {
        out.put_u32(u32::try_from(d.as_millis()).unwrap_or(u32::MAX));
    }
    out.freeze()
}

fn get_timings(mut body: Bytes) -> Option<SeedTimings> {
    let mut ms = || take(&mut body, 4).map(|mut b| Duration::from_millis(b.get_u32().into()));
    let timings = SeedTimings { idle: ms()?, idle_hold: ms()?, closed_grace: ms()?, warm_up: ms()? };
    body.is_empty().then_some(timings)
}

/// `CHANGES` frames of at most `MAX_FRAME`, each compressed once for every session it goes to.
fn changes<'a>(entries: impl IntoIterator<Item = &'a Bytes>) -> Vec<Frame> {
    let mut frames = Vec::new();
    let mut chunk = BytesMut::new();
    let mut close = |chunk: &mut BytesMut| {
        let body = chunk.split().freeze();
        // Only when it pays: a block that grew would not fit the frame limit.
        let packed = (body.len() >= PACK_FROM).then(|| pack(&body, 1).ok()).flatten().filter(|p| p.len() < body.len());
        frames.push(match packed {
            Some(packed) => Frame { packed: Compression::Zstd, ..frame(CHANGES, packed) },
            None => frame(CHANGES, body),
        });
    };
    for entry in entries {
        if !chunk.is_empty() && chunk.len() + entry.len() > MAX_FRAME {
            close(&mut chunk);
        }
        chunk.extend_from_slice(entry);
    }
    if !chunk.is_empty() {
        close(&mut chunk);
    }
    frames
}

fn ahead(incarnation: u128, unix_ms: u64) -> bool {
    (incarnation >> 64) as u64 > unix_ms.saturating_add(AHEAD_MS)
}

pub(crate) type SessionId = u64;

#[derive(Debug)]
pub(crate) enum Out {
    To(SessionId, Frame),
    All(Frame),
    Close(SessionId),
}

struct Member {
    incarnation: u128,
    version: u64,
    entry: Bytes,
    /// The session that last registered it; only that one ending starts a removal.
    session: SessionId,
    remove_at: Option<Duration>,
}

struct Session {
    id: String,
    incarnation: u128,
}

pub(crate) struct Server {
    timings: SeedTimings,
    synced: Bytes,
    started: Duration,
    warm: bool,
    members: BTreeMap<String, Member>,
    bytes: usize,
    sessions: BTreeMap<SessionId, Session>,
    /// Sessions without a registration, oldest first.
    observers: VecDeque<(SessionId, Duration)>,
    list: Option<Vec<Frame>>,
    /// What changed since the last send, per id, without the `Put`s a later change made redundant.
    batch: BTreeMap<String, Vec<Bytes>>,
    sent_at: Option<Duration>,
    /// By reason: stale, full, invalid.
    pub refusals: [u64; 3],
}

impl Server {
    pub fn new(now: Duration, timings: SeedTimings) -> Self {
        Self {
            timings,
            synced: put_timings(&timings),
            started: now,
            warm: false,
            members: BTreeMap::new(),
            bytes: 0,
            sessions: BTreeMap::new(),
            observers: VecDeque::new(),
            list: None,
            batch: BTreeMap::new(),
            sent_at: None,
            refusals: [0; 3],
        }
    }

    pub fn members(&self) -> usize {
        self.members.len()
    }

    pub fn sessions(&self) -> usize {
        self.sessions.len()
    }

    pub fn observers(&self) -> usize {
        self.observers.len()
    }

    pub fn list_bytes(&self) -> usize {
        self.bytes
    }

    pub fn warm_up_left(&self, now: Duration) -> Duration {
        if self.warm { Duration::ZERO } else { (self.started + self.timings.warm_up).saturating_sub(now) }
    }

    pub fn open(&mut self, session: SessionId, id: String, incarnation: u128, now: Duration, out: &mut Vec<Out>) {
        let list = self.list.get_or_insert_with(|| changes(self.members.values().map(|m| &m.entry)));
        out.extend(list.iter().map(|f| Out::To(session, f.clone())));
        if self.warm {
            out.push(Out::To(session, frame(SYNCED, self.synced.clone())));
        }
        self.sessions.insert(session, Session { id, incarnation });
        self.observers.push_back((session, now));
    }

    pub fn frame(&mut self, session: SessionId, frame: Frame, now: Duration, unix_ms: u64, out: &mut Vec<Out>) {
        match frame.kind {
            _ if frame.schema != VERSION => out.push(Out::Close(session)),
            REGISTER => self.register(session, frame.payload, now, unix_ms, out),
            LEAVE => self.leave(session, now, out),
            _ => out.push(Out::Close(session)),
        }
    }

    fn register(&mut self, session: SessionId, mut body: Bytes, now: Duration, unix_ms: u64, out: &mut Vec<Out>) {
        let Some(sess) = self.sessions.get(&session) else { return };
        let Some(record) = get_record(&mut body).filter(|_| body.is_empty()) else {
            return out.push(Out::Close(session));
        };
        let own = record.id == sess.id && record.incarnation == sess.incarnation;
        let old = self.members.get(&record.id);
        let entry = put_entry(&record);
        let bytes = self.bytes - old.map_or(0, |m| m.entry.len()) + entry.len();
        let refusal = if !own || record.validate().is_err() || ahead(record.incarnation, unix_ms) {
            Some(INVALID)
        } else if old.is_some_and(|m| m.incarnation > record.incarnation) {
            Some(STALE)
        } else if bytes > MAX_LIST || old.is_none() && self.members.len() >= MAX_MEMBERS {
            Some(FULL)
        } else {
            None
        };
        if let Some(reason) = refusal {
            self.refusals[reason as usize - 1] += 1;
            return out.push(Out::To(session, frame(REFUSED, Bytes::copy_from_slice(&[reason]))));
        }
        let same_boot = old.filter(|m| m.incarnation == record.incarnation);
        if same_boot.is_some_and(|m| record.version < m.version) {
            return;
        }
        if let Some(m) = old.filter(|m| m.incarnation < record.incarnation && m.session != session) {
            out.push(Out::Close(m.session));
        }
        let known = same_boot.is_some_and(|m| record.version == m.version);
        self.observers.retain(|(s, _)| *s != session);
        if known {
            // The version it already has, on a new session: the record is that session's now.
            let m = self.members.get_mut(&record.id).expect("known says so");
            (m.session, m.remove_at) = (session, None);
            return;
        }
        self.bytes = bytes;
        self.change(&record.id, entry.clone(), now, out);
        let member =
            Member { incarnation: record.incarnation, version: record.version, entry, session, remove_at: None };
        self.members.insert(record.id, member);
    }

    fn leave(&mut self, session: SessionId, now: Duration, out: &mut Vec<Out>) {
        let Some(sess) = self.sessions.get(&session) else { return };
        if self.members.get(&sess.id).is_some_and(|m| m.session == session) {
            let id = sess.id.clone();
            self.remove(&id, now, out);
            self.observers.push_back((session, now));
        }
    }

    /// `idle`: the session timed out, so the node may still be running behind a dead link.
    pub fn closed(&mut self, session: SessionId, idle: bool, now: Duration) {
        let Some(sess) = self.sessions.remove(&session) else { return };
        self.observers.retain(|(s, _)| *s != session);
        if let Some(m) = self.members.get_mut(&sess.id).filter(|m| m.session == session) {
            m.remove_at = Some(now + if idle { self.timings.idle_hold } else { self.timings.closed_grace });
        }
    }

    pub fn tick(&mut self, now: Duration, out: &mut Vec<Out>) {
        if !self.warm && now >= self.started + self.timings.warm_up {
            self.warm = true;
            out.extend(self.sessions.keys().map(|id| Out::To(*id, frame(SYNCED, self.synced.clone()))));
        }
        let due = |m: &Member| m.remove_at.is_some_and(|at| at <= now);
        let gone: Vec<String> = self.members.iter().filter(|(_, m)| due(m)).map(|(id, _)| id.clone()).collect();
        for id in gone {
            self.remove(&id, now, out);
        }
        while self.observers.get(MAX_OBSERVERS).is_some_and(|(_, at)| *at + OBSERVER_AFTER <= now) {
            let (oldest, _) = self.observers.pop_front().expect("get above");
            out.push(Out::Close(oldest));
        }
        self.flush(now, out);
    }

    fn remove(&mut self, id: &str, now: Duration, out: &mut Vec<Out>) {
        let Some(m) = self.members.remove(id) else { return };
        self.bytes -= m.entry.len();
        self.change(id, remove_entry(id, m.incarnation), now, out);
    }

    fn change(&mut self, id: &str, entry: Bytes, now: Duration, out: &mut Vec<Out>) {
        self.list = None;
        // A later change makes a `Put` redundant. A `Remove` stays: what follows may be an older boot.
        let changes = self.batch.entry(id.to_owned()).or_default();
        if changes.last().is_some_and(|last| last[0] == PUT) {
            changes.pop();
        }
        changes.push(entry);
        self.flush(now, out);
    }

    /// At once when quiet, otherwise one frame per `BATCH`.
    fn flush(&mut self, now: Duration, out: &mut Vec<Out>) {
        if !self.batch.is_empty() && self.sent_at.is_none_or(|at| now >= at + BATCH) {
            let batch = std::mem::take(&mut self.batch);
            out.extend(changes(batch.values().flatten()).into_iter().map(Out::All));
            self.sent_at = Some(now);
        }
    }
}

/// `host:port` or `id@host:port`: the expected identity, and what to dial.
pub(crate) fn parse_seed(entry: &str) -> Option<(Option<&str>, &str)> {
    let (id, target) = match entry.rsplit_once('@') {
        Some((id, target)) => (Some(id), target),
        None => (None, entry),
    };
    let (host, port) = target.rsplit_once(':')?;
    let ok = id != Some("") && !host.is_empty() && port.parse::<u16>().is_ok_and(|p| p != 0);
    ok.then_some((id, target))
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NodeOut {
    Send(String, Frame),
    Close(String),
    /// Stop dialing this seed.
    Retire(String),
    /// The seed runs these timings, not the node's: it does not count.
    Timings(String, SeedTimings),
}

#[derive(Default)]
struct Slot {
    expect: Option<String>,
    /// Dropped from the seed list, kept until the node is live without it.
    retired: bool,
    /// The seed's handshake id, while a session is open.
    peer: Option<String>,
    synced: bool,
    /// A `SYNCED` came in this session: a second one closes it.
    answered: bool,
    /// The timings of a seed whose last `SYNCED` showed others: it is not counted, and not told the record, in later
    /// sessions too until one shows ours.
    odd: Option<SeedTimings>,
    list: BTreeMap<String, Arc<NodeRecord>>,
    /// The record version this session was last sent.
    sent: Option<u64>,
    refused: Option<DiscoveryError>,
    retry_at: Duration,
}

/// A node attached again stays frozen while the others cut off with it register again: until `warm_up` has passed, or
/// every member of its last view is listed again.
#[derive(Clone, Copy)]
enum Thaw {
    Done,
    /// Not attached at the last tick: not live either, though `set_seeds` attached it since.
    Owed,
    /// The deadline, and the first member of the view not yet seen listed: the view stays as it is while frozen.
    Until(Duration, usize),
}

pub(crate) struct Node {
    id: String,
    incarnation: u128,
    timings: SeedTimings,
    synced: Bytes,
    slots: BTreeMap<String, Slot>,
    record: Option<NodeRecord>,
    version: u64,
    minority_since: Option<Duration>,
    thaw: Thaw,
    dirty: bool,
    revision: u64,
    view: Vec<Arc<NodeRecord>>,
    pub refusals: u64,
}

impl Node {
    pub fn new(id: String, incarnation: u128, seeds: &[String], timings: SeedTimings) -> Result<Self, DiscoveryError> {
        let mut node = Self {
            id,
            incarnation,
            timings,
            synced: put_timings(&timings),
            slots: BTreeMap::new(),
            record: None,
            version: 0,
            minority_since: None,
            thaw: Thaw::Done,
            dirty: true,
            revision: 1,
            view: Vec::new(),
            refusals: 0,
        };
        node.set_seeds(seeds)?;
        Ok(node)
    }

    /// The majority is counted over the new list at once. Seeds that were dropped stay in the
    /// union until the node is live without them.
    pub fn set_seeds(&mut self, seeds: &[String]) -> Result<(), DiscoveryError> {
        if seeds.is_empty() || seeds.len() > MAX_SEEDS {
            return Err(DiscoveryError::Invalid("1 to 16 seeds"));
        }
        if seeds.iter().any(|s| parse_seed(s).is_none()) {
            return Err(DiscoveryError::Invalid("a seed is host:port or id@host:port"));
        }
        for (entry, slot) in &mut self.slots {
            slot.retired = !seeds.contains(entry);
        }
        for entry in seeds {
            let expect = parse_seed(entry).and_then(|(id, _)| id).map(str::to_owned);
            self.slots.entry(entry.clone()).or_insert_with(|| Slot { expect, ..Slot::default() });
        }
        Ok(())
    }

    /// Every seed to keep a session to.
    pub fn seeds(&self) -> impl Iterator<Item = &str> {
        self.slots.keys().map(String::as_str)
    }

    /// Per seed: the entry, the identity it answered with, and its state.
    pub fn states(&self) -> impl Iterator<Item = (&str, Option<&str>, &'static str)> {
        self.slots.iter().map(|(entry, s)| {
            let state = match s {
                _ if s.peer.is_none() => "connecting",
                _ if s.retired => "retired",
                _ if s.odd.is_some() => "timings",
                _ if !s.synced => "open",
                _ if s.refused.is_some() => "refused",
                _ => "synced",
            };
            (entry.as_str(), s.peer.as_deref(), state)
        })
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    /// Whether the seed counts: its session is open and its `SYNCED` showed this node's timings.
    pub fn synced(&self, entry: &str) -> bool {
        self.slots.get(entry).is_some_and(|s| s.synced)
    }

    /// The seeds that run other timings, and theirs.
    pub fn odd(&self) -> impl Iterator<Item = (&str, SeedTimings)> {
        self.slots.iter().filter_map(|(entry, s)| s.odd.map(|t| (entry.as_str(), t)))
    }

    /// Members of the last view built.
    pub fn members(&self) -> usize {
        self.view.len()
    }

    fn count(&self, holds: impl Fn(&Slot) -> bool) -> (usize, usize) {
        let current = || self.slots.values().filter(|s| !s.retired);
        let peers: BTreeSet<_> = current().filter(|s| s.synced && holds(s)).filter_map(|s| s.peer.as_deref()).collect();
        (peers.len(), current().count())
    }

    fn lists(&self, slot: &Slot, version: u64) -> bool {
        slot.list.get(&self.id).is_some_and(|r| r.incarnation == self.incarnation && r.version >= version)
    }

    fn attached(&self) -> bool {
        let (attached, seeds) = self.count(|_| true);
        attached * 2 > seeds
    }

    /// Whether more than half of the seeds list this boot at `version` or later.
    pub fn listed(&self, version: u64) -> bool {
        let (listing, seeds) = self.count(|s| self.lists(s, version));
        listing * 2 > seeds
    }

    /// A seed that still lists a greater boot of this id keeps that boot in the view, so a boot under a
    /// lower incarnation is not live until it is gone. Nor is one attached again while it thaws.
    pub fn live(&self) -> bool {
        let superseded = || {
            self.slots
                .values()
                .any(|s| s.synced && s.list.get(&self.id).is_some_and(|r| r.incarnation > self.incarnation))
        };
        matches!(self.thaw, Thaw::Done) && self.attached() && (self.record.is_none() || self.listed(0) && !superseded())
    }

    /// Sessions that reach a seed another entry already reached; they count once.
    pub fn duplicates(&self) -> usize {
        let open = self.slots.values().filter(|s| !s.retired && s.peer.is_some()).count();
        open - self
            .slots
            .values()
            .filter(|s| !s.retired)
            .filter_map(|s| s.peer.as_deref())
            .collect::<BTreeSet<_>>()
            .len()
    }

    pub fn refusal(&self) -> Option<DiscoveryError> {
        let mut refusals = self.slots.values().filter_map(|s| s.refused.clone());
        refusals.clone().find(|r| *r == DiscoveryError::Stale).or_else(|| refusals.next())
    }

    #[cfg(test)]
    pub fn debug(&self) -> String {
        let slot = |(entry, s): (&String, &Slot)| {
            let own = s.list.get(&self.id).map(|r| (r.incarnation as u64, r.version));
            format!(
                "  {entry}: peer={:?} synced={} sent={:?} own={own:?} retry={:?} members={}\n",
                s.peer,
                s.synced,
                s.sent,
                s.retry_at,
                s.list.len()
            )
        };
        format!(
            "  version={} minority_since={:?}\n{}",
            self.version,
            self.minority_since,
            self.slots.iter().map(slot).collect::<String>()
        )
    }

    /// `None` withdraws. Returns the record's version, which moves only when the record does.
    pub fn set_record(&mut self, record: Option<NodeRecord>, now: Duration, out: &mut Vec<NodeOut>) -> u64 {
        if record != self.record {
            self.version += 1;
            self.record = record.map(|r| NodeRecord { version: self.version, ..r });
        }
        self.tick(now, out);
        self.version
    }

    pub fn opened(&mut self, entry: &str, peer: String, now: Duration, out: &mut Vec<NodeOut>) {
        let Some(slot) = self.slots.get_mut(entry) else { return };
        if slot.expect.as_ref().is_some_and(|id| *id != peer) {
            return out.push(NodeOut::Close(entry.to_owned()));
        }
        slot.peer = Some(peer);
        self.tick(now, out);
    }

    pub fn lost(&mut self, entry: &str, now: Duration, out: &mut Vec<NodeOut>) {
        let Some(slot) = self.slots.get_mut(entry) else { return };
        *slot = Slot { expect: slot.expect.take(), retired: slot.retired, odd: slot.odd, ..Slot::default() };
        self.dirty = true;
        self.tick(now, out);
    }

    pub fn frame(&mut self, entry: &str, frame: Frame, now: Duration, unix_ms: u64, out: &mut Vec<NodeOut>) {
        let (id, incarnation) = (&self.id, self.incarnation);
        if self.slots.get(entry).is_none_or(|s| s.peer.is_none()) {
            return;
        }
        // Out of the map while the frame is read, so that a record another seed already sent is shared.
        let (entry, mut slot) = self.slots.remove_entry(entry).expect("entry comes from slots");
        let (others, view, dirty, ours) = (&self.slots, &self.view, &mut self.dirty, &self.synced);
        // A change the view already shows, as when a second seed repeats it, asks for no rebuild.
        let shown = |id: &str, incarnation: u128, version: Option<u64>| {
            let at = view.binary_search_by(|r| r.id.as_str().cmp(id));
            at.is_ok_and(|i| view[i].incarnation == incarnation && version.is_none_or(|v| view[i].version == v))
        };
        let mut body = frame.payload;
        let ok = match frame.kind {
            _ if frame.schema != VERSION => false,
            // Nothing reads an odd seed's list.
            CHANGES if slot.odd.is_some() && slot.answered => true,
            CHANGES => {
                let mut apply = || {
                    while body.has_remaining() {
                        match body.get_u8() {
                            PUT => {
                                let r = get_record(&mut body)?;
                                let newer =
                                    |old: &Arc<NodeRecord>| (old.incarnation, old.version) > (r.incarnation, r.version);
                                let usable = r.validate().is_ok() && !ahead(r.incarnation, unix_ms);
                                if !usable || slot.list.get(&r.id).is_some_and(newer) {
                                    continue;
                                }
                                if r.id == *id && r.incarnation == incarnation {
                                    slot.refused = None;
                                }
                                *dirty |= slot.synced && !shown(&r.id, r.incarnation, Some(r.version));
                                let shared = others.values().find_map(|s| s.list.get(&r.id).filter(|o| ***o == r));
                                let r = shared.cloned().unwrap_or_else(|| Arc::new(r));
                                slot.list.insert(r.id.clone(), r);
                            }
                            REMOVE => {
                                let gone = (get_str(&mut body)?, take(&mut body, 16)?.get_u128());
                                if slot.list.get(&gone.0).is_some_and(|r| r.incarnation == gone.1) {
                                    slot.list.remove(&gone.0);
                                    *dirty |= slot.synced && shown(&gone.0, gone.1, None);
                                }
                                // Dropped by a seed this session still reaches: tell it again.
                                if gone == (id.clone(), incarnation) {
                                    slot.sent = None;
                                }
                            }
                            _ => return None,
                        }
                    }
                    (slot.list.len() <= MAX_MEMBERS).then_some(())
                };
                apply().is_some()
            }
            SYNCED if slot.answered => false,
            SYNCED if body == *ours => {
                (slot.synced, slot.answered, slot.odd) = (true, true, None);
                *dirty = true;
                // The wait before withdrawing counts from the last seed gained.
                self.minority_since = None;
                true
            }
            SYNCED => match get_timings(body) {
                Some(theirs) => {
                    if slot.odd.replace(theirs) != Some(theirs) {
                        out.push(NodeOut::Timings(entry.clone(), theirs));
                    }
                    slot.answered = true;
                    slot.list.clear();
                    true
                }
                _ => false,
            },
            REFUSED => {
                self.refusals += 1;
                slot.refused = Some(match body.first() {
                    Some(&STALE) => DiscoveryError::Stale,
                    Some(&FULL) => DiscoveryError::Capacity,
                    _ => DiscoveryError::Invalid("refused by a seed"),
                });
                slot.sent = None;
                slot.retry_at = now + RETRY;
                true
            }
            _ => false,
        };
        let close = (!ok).then(|| NodeOut::Close(entry.clone()));
        self.slots.insert(entry, slot);
        match close {
            Some(close) => out.push(close),
            None => self.tick(now, out),
        }
    }

    /// Brings what each seed was told in line with what it should know.
    pub fn tick(&mut self, now: Duration, out: &mut Vec<NodeOut>) {
        // A node that reaches too few seeds for too long leaves the ones it reaches, so that
        // it is in nobody's view, not in some.
        let attached = self.attached();
        if attached {
            self.minority_since = None;
            if let Thaw::Owed = self.thaw {
                self.thaw = Thaw::Until(now + self.timings.warm_up, 0);
            }
        } else {
            self.thaw = Thaw::Owed;
        }
        if let Thaw::Until(until, mut from) = self.thaw {
            let known = |r: &Arc<NodeRecord>| self.slots.values().any(|s| s.synced && s.list.contains_key(&r.id));
            while self.view.get(from).is_some_and(known) {
                from += 1;
            }
            self.thaw = if now >= until || from == self.view.len() { Thaw::Done } else { Thaw::Until(until, from) };
        }
        let withdrawn = !attached && now >= *self.minority_since.get_or_insert(now) + WITHDRAW_AFTER;
        let live = self.live();
        let record = self.record.as_ref();
        let dirty = &mut self.dirty;
        self.slots.retain(|entry, slot| {
            let retire = slot.retired && (live || slot.peer.is_none());
            let wanted = record.filter(|_| !(retire || withdrawn && slot.synced || slot.odd.is_some()));
            match wanted {
                _ if slot.peer.is_none() => {}
                Some(r) if slot.sent != Some(r.version) && now >= slot.retry_at => {
                    out.push(NodeOut::Send(entry.clone(), register_frame(r)));
                    slot.sent = Some(r.version);
                }
                None => {
                    slot.refused = None;
                    if slot.sent.take().is_some() {
                        out.push(NodeOut::Send(entry.clone(), frame(LEAVE, Bytes::new())));
                    }
                }
                _ => {}
            }
            if retire {
                *dirty = true;
                out.push(NodeOut::Retire(entry.clone()));
            }
            !retire
        });
    }

    /// The union of the attached seeds' lists, or `None` while frozen. The revision moves only
    /// when a member, its boot or its version does.
    pub fn view(&mut self) -> Option<(u64, &[Arc<NodeRecord>])> {
        if !self.live() {
            return None;
        }
        if std::mem::take(&mut self.dirty) {
            let mut union: BTreeMap<&str, &Arc<NodeRecord>> = BTreeMap::new();
            for r in self.slots.values().filter(|s| s.synced).flat_map(|s| s.list.values()) {
                let seen = union.entry(&r.id).or_insert(r);
                if (r.incarnation, r.version) > (seen.incarnation, seen.version) {
                    *seen = r;
                }
            }
            let key = |r: &Arc<NodeRecord>| (r.incarnation, r.version);
            let same = union.len() == self.view.len()
                && union.values().zip(&self.view).all(|(a, b)| a.id == b.id && key(a) == key(b));
            if !same {
                self.view = union.into_values().cloned().collect();
                self.revision += 1;
            }
        }
        Some((self.revision, &self.view))
    }
}
