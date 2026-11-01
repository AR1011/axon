//! The streams: each node originates `per_node` streams in the namespace of its id, `b1/s0`.., one record every 50 ms
//! each, and follows every other node's. The next node in the list relays a node's streams to the rest, which fall
//! back to the origin. A follower checks each record against the last it handled and logs to `stream.jsonl` only what
//! breaks the guarantee, a record after a silence, and one aggregate line per 10 s. Half the followers drop their
//! subscription every 10 to 30 s and subscribe again after their cursor, so ring recovery runs under every fault.
//! With the disk workload the node keeps its disk streams (src/disk.rs) and follows every other node's the same way,
//! logging `rewritten` where an origin's restart went back over records a follower had.

use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use axon::trace::random_u64;
use axon_stream::{
    Cursor, DeliveryPolicy, EndReason, RecoveryError, ResetReason, StartPosition, StreamError, StreamEvent, StreamKey,
    StreamNode, StreamNodeConfig, SubscribeOptions,
};
use bytes::Bytes;
use serde_json::{Map, json};
use tokio::time::MissedTickBehavior;

use crate::Tick;
use crate::account::Log;
use crate::store::now_us;

const EVERY: Duration = Duration::from_millis(50);
/// A record after this long without one is logged as a `resume`, and a follower this silent is listed as `quiet`.
const QUIET_MS: u64 = 2000;

/// What one follower has handled, within one subscription.
struct Seen {
    /// The generation and sequence of the last record; `None` while the next may start anywhere.
    last: Option<Cursor>,
    /// The last gap within `last`'s generation, first and last missing, so a late record from it is a reorder.
    hole: (u64, u64),
}

impl Seen {
    /// `After(cursor)` continues from the cursor; `Live` and `First` start where their first `Ready` or record says.
    fn new(after: Option<Cursor>) -> Self {
        Seen { last: after, hole: (1, 0) }
    }

    fn ready(&mut self, start: Option<Cursor>) -> Option<&'static str> {
        let start = start?;
        // Within a generation a Ready that moved on would hide a gap; one behind shows as the dups that follow.
        let gap = match self.last {
            Some(last) if start.generation < last.generation => return Some("reorder"),
            Some(last) if last.generation == start.generation => {
                if start.sequence <= last.sequence {
                    return None;
                }
                Some("gap")
            }
            _ => None,
        };
        self.moved(start);
        gap
    }

    fn record(&mut self, generation: u64, sequence: u64) -> Option<&'static str> {
        let at = Cursor { generation, sequence };
        let Some(last) = self.last else {
            self.moved(at);
            return None;
        };
        if generation < last.generation {
            // A stale relay feeding the generation before a restart.
            return Some("reorder");
        }
        if last.generation != generation {
            // A new generation shows as a Ready first, unless it continues from record 1.
            self.moved(at);
            return (sequence != 1).then_some("gap");
        }
        if sequence == last.sequence + 1 {
            self.last = Some(at);
            None
        } else if sequence > last.sequence {
            self.hole = (last.sequence + 1, sequence - 1);
            self.last = Some(at);
            Some("gap")
        } else if (self.hole.0..=self.hole.1).contains(&sequence) {
            Some("reorder")
        } else {
            Some("dup")
        }
    }

    fn moved(&mut self, to: Cursor) {
        (self.last, self.hole) = (Some(to), (1, 0));
    }
}

/// Per 10 s window, but `max_lag_ms` is the largest in it.
#[derive(Default)]
struct Counts {
    recv: AtomicU64,
    gaps: AtomicU64,
    dups: AtomicU64,
    reorders: AtomicU64,
    resets: AtomicU64,
    origin_lost: AtomicU64,
    /// Subscriptions started `After` a cursor.
    afters: AtomicU64,
    max_lag_ms: AtomicU64,
}

struct Follower {
    key: StreamKey,
    name: String,
    live: bool,
    /// ms since the workload's start of the last record, or of the follower's start.
    last_ms: AtomicU64,
}

struct Shared {
    node: StreamNode,
    log: Log,
    counts: Counts,
    started: Instant,
}

impl Shared {
    fn ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }
}

/// This node's hub, for the other workloads on streams: a cluster serves one (src/compacted.rs).
pub(crate) static NODE: OnceLock<StreamNode> = OnceLock::new();

/// The `streams` workload among nodes `b1..b{nodes}`, `STREAMS` of them from this node. The generation is one above
/// this node's last, counted in the log directory: see [`next_generation`]. Its tick counts publish rounds.
pub async fn start(cx: &crate::Ctx<'_>) -> Result<Tick, Box<dyn Error>> {
    let (cluster, me, dir) = (cx.cluster, cx.sh.node.as_str(), cx.log_dir);
    let per_node: usize = crate::env("STREAMS", "4").parse()?;
    let ids = cx.ids();
    let label = |n: &str| BTreeMap::from([("node".to_owned(), n.to_owned())]);
    let disk = cx.workloads.iter().any(|w| w == "disk");
    let mut cfg = StreamNodeConfig::new(vec![]);
    for (i, origin) in ids.iter().enumerate().filter(|(_, n)| *n != me) {
        let relay = &ids[(i + 1) % ids.len()];
        let tiers = if relay == me { vec![label(origin)] } else { vec![label(relay), label(origin)] };
        if disk {
            cfg = cfg.upstream_for(crate::disk::namespace(origin), tiers.clone());
        }
        cfg = cfg.upstream_for(origin.clone(), tiers);
    }
    if disk {
        cfg.disk = Some(crate::disk::config(me)?);
    }
    if crate::wan() {
        // spec/wan.md § 9: a parent lost after one redial across regions, 6.4 s; one back for under 30 s not worth a
        // switch; a child's refresh through an idle-out and a redial
        cfg.core.timings.upstream_timeout = Duration::from_secs(7);
        cfg.core.timings.hold_down = Duration::from_secs(30);
        cfg.downstream_expiry = Duration::from_secs(20);
    }
    if cx.workloads.iter().any(|w| w == "firehose") {
        cfg = crate::firehose::config(cfg, me);
    }
    let node = StreamNode::start(cluster, crate::quota::limits(cfg)).await?;
    let _ = NODE.set(node.clone());
    let sh = Arc::new(Shared {
        node,
        log: Log::create(&dir.join("stream.jsonl"))?,
        counts: Counts::default(),
        started: Instant::now(),
    });
    let generation = next_generation(dir)?;
    if disk {
        crate::disk::NODE.set((sh.node.clone(), generation)).map_err(|_| "the stream node started twice")?;
    }
    let names: Vec<String> = (0..per_node).map(|j| format!("s{j}")).collect();
    let mine: Vec<StreamKey> = names.iter().map(|s| StreamKey::new(me, s.as_str())).collect();
    for key in &mine {
        sh.node.open_stream(key, generation, DeliveryPolicy::ReplayRequired).await?;
        sh.log.line("open", json!({"s": key.to_string(), "g": generation}));
    }
    crate::quota::lim(&sh.node, me);
    let tick = Tick::default();
    tokio::spawn(publish(sh.clone(), mine, generation, tick.clone()));
    let mut followers = Vec::new();
    for origin in &cx.others() {
        let mut keys: Vec<StreamKey> = names.iter().map(|s| StreamKey::new(origin.as_str(), s.as_str())).collect();
        if disk {
            keys.extend(crate::disk::NAMES.map(|s| StreamKey::new(crate::disk::namespace(origin), s)));
        }
        for (j, key) in keys.into_iter().enumerate() {
            let f = Arc::new(Follower { name: key.to_string(), key, live: j % 2 == 0, last_ms: AtomicU64::new(0) });
            tokio::spawn(follow(sh.clone(), f.clone()));
            followers.push(f);
        }
    }
    tokio::spawn(report(sh, followers));
    Ok(tick)
}

/// Counted in a file of the log directory, which no fault touches: not the wall clock, which `skew-restart` steps
/// back across a restart, nor the journal's disk, which a reset can format. A generation must only go up.
pub(crate) fn next_generation(dir: &Path) -> std::io::Result<u64> {
    let path = dir.join("stream-generation");
    let last = match std::fs::read_to_string(&path) {
        Ok(s) => s.trim().parse().map_err(std::io::Error::other)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => return Err(e),
    };
    // A rename, so a kill mid-write leaves the old count, never an empty file.
    let next = dir.join("stream-generation.next");
    std::fs::write(&next, format!("{}\n", last + 1))?;
    std::fs::rename(&next, &path)?;
    Ok(last + 1)
}

async fn publish(sh: Arc<Shared>, keys: Vec<StreamKey>, generation: u64, round: Tick) {
    let mut next = vec![1u64; keys.len()];
    let mut failing = vec![crate::Failing::default(); keys.len()];
    let mut tick = tokio::time::interval(EVERY);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        round.fetch_add(1, Relaxed);
        for (i, key) in keys.iter().enumerate() {
            let record = format!("{key} {generation} {} {}", next[i], now_us());
            match sh.node.publish(key, vec![Bytes::from(record)]) {
                Ok(_) => {
                    next[i] += 1;
                    failing[i].ok();
                }
                // Once per run of failures: none clears by waiting, so this is a finding, not load.
                Err(e) => failing[i].err(&sh.log, key, &e.error),
            }
        }
    }
}

/// Follows one stream for good, as the streams guide's § Many origins does: a `Live` follower subscribes again live
/// after an end; an `After` one after the last record it handled, from `First` once its cursor means nothing, and
/// live once when the history after it is gone. An `After` follower also drops its subscription every 10 to 30 s.
async fn follow(sh: Arc<Shared>, f: Arc<Follower>) {
    f.last_ms.store(sh.ms(), Relaxed);
    let (mut cursor, mut live_once, mut run) = (None::<Cursor>, false, None);
    'subscribe: loop {
        let (start, tag) = match cursor {
            _ if f.live || live_once => (StartPosition::Live, "live"),
            Some(c) => (StartPosition::After(c), "after"),
            None => (StartPosition::First, "first"),
        };
        let mut seen = Seen::new(cursor.filter(|_| tag == "after"));
        live_once = false;
        // Live for good; the others for 10 to 30 s, out of step with each other.
        let hold = Duration::from_secs(if f.live { 1 << 30 } else { 10 + random_u64() % 21 });
        let until = tokio::time::Instant::now() + hold;
        let sub = sh.node.subscribe(&f.key, SubscribeOptions { start, ..Default::default() });
        if tag == "after" && sub.is_ok() {
            sh.counts.afters.fetch_add(1, Relaxed);
        }
        match sub {
            Err(e) => {
                sh.log.line("subscribe_err", json!({"s": f.name, "err": format!("{e:?}")}));
                match e {
                    StreamError::Recovery(RecoveryError::GenerationMismatch) => cursor = None,
                    StreamError::Recovery(RecoveryError::HistoryUnavailable) => live_once = true,
                    _ => {}
                }
            }
            Ok(mut sub) => loop {
                let event = match tokio::time::timeout_at(until, sub.next()).await {
                    Ok(Some(event)) => event,
                    Ok(None) => break,
                    Err(_) => continue 'subscribe,
                };
                match event {
                    StreamEvent::Ready { start } => {
                        if start.is_none() {
                            // no position: what the next batch says of a gap is read beside this
                            sh.log.line("ready", json!({"s": f.name, "start": tag}));
                        }
                        let before = seen.last;
                        if let (Some(kind), Some(c)) = (seen.ready(start), start) {
                            anomaly(&sh, &f, kind, c, before, tag);
                        }
                    }
                    StreamEvent::Batch(batch) => {
                        received(&sh, &f, &mut seen, &mut run, batch.records(), tag);
                        cursor = Some(batch.cursor());
                    }
                    StreamEvent::OriginLost => {
                        sh.counts.origin_lost.fetch_add(1, Relaxed);
                        sh.log.line("origin_lost", json!({"s": f.name}));
                    }
                    StreamEvent::ResetRequired(why) => {
                        reset(&sh, &f, format!("{why:?}"), tag);
                        match why {
                            ResetReason::GenerationChanged => cursor = None,
                            ResetReason::HistoryUnavailable => live_once = true,
                            _ => {}
                        }
                    }
                    StreamEvent::End(why) => {
                        reset(&sh, &f, format!("{why:?}"), tag);
                        if why == EndReason::StreamClosed {
                            cursor = None;
                        }
                    }
                    StreamEvent::Snapshot(_) | StreamEvent::CaughtUp { .. } | StreamEvent::Replacement { .. } => {}
                }
            },
        }
        // Bounds the log should a subscription fail or end as soon as it starts.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn reset(sh: &Shared, f: &Follower, why: String, tag: &str) {
    sh.counts.resets.fetch_add(1, Relaxed);
    sh.log.line("reset", json!({"s": f.name, "why": why, "start": tag}));
}

fn received(sh: &Shared, f: &Follower, seen: &mut Seen, run: &mut Option<[u64; 4]>, records: &[Bytes], tag: &str) {
    let (now, ms) = (now_us(), sh.ms());
    let quiet = ms.saturating_sub(f.last_ms.swap(ms, Relaxed));
    if quiet >= QUIET_MS {
        sh.log.line("resume", json!({"s": f.name, "quiet_ms": quiet}));
    }
    sh.counts.recv.fetch_add(records.len() as u64, Relaxed);
    for r in records {
        // `<origin>/<name> <generation> <sequence> <published µs>`, and on a disk stream its incarnation, the position
        // it was opened after and padding (src/disk.rs)
        let w: Result<Vec<u64>, _> =
            std::str::from_utf8(r).unwrap_or("").split(' ').skip(1).take(5).map(str::parse).collect();
        let (Ok(&[generation, sequence, at]) | Ok(&[generation, sequence, at, _, _])) = w.as_deref() else {
            let record = String::from_utf8_lossy(r);
            sh.counts.gaps.fetch_add(1, Relaxed);
            sh.log.line("gap", json!({"s": f.name, "bad": record, "start": tag}));
            continue;
        };
        // A start from `First` replays the history by design; any other record's age is what delivery took.
        if tag != "first" {
            sh.counts.max_lag_ms.fetch_max(now.saturating_sub(at) / 1000, Relaxed);
        }
        let before = seen.last;
        if let Some(kind) = seen.record(generation, sequence) {
            anomaly(sh, f, kind, Cursor { generation, sequence }, before, tag);
        }
        if let Ok(&[_, _, _, inc, open]) = w.as_deref()
            && let Some([max, was]) = rewritten(run, generation, sequence, inc, open)
        {
            let line = json!({"s": f.name, "g": generation, "seq": sequence, "inc": inc, "open": open, "max": max, "was": was});
            sh.log.line("rewritten", line);
        }
    }
}

/// A disk stream's record against the generation, incarnation and highest record a follower has had of it, across
/// its subscriptions: a later incarnation opened behind that record went back over records the follower had, which
/// the disk lost. Returns the highest record then and the incarnation it came from, so the loss lies between that
/// one's open and this one's. From there the highest is the new incarnation's own, so a later start that resumes
/// after what that one published is no loss. An earlier incarnation's record is history replayed, and changes nothing.
fn rewritten(run: &mut Option<[u64; 4]>, generation: u64, sequence: u64, inc: u64, open: u64) -> Option<[u64; 2]> {
    let Some([g, i, max, was]) = run else {
        *run = Some([generation, inc, sequence, inc]);
        return None;
    };
    if generation != *g {
        if generation > *g {
            *run = Some([generation, inc, sequence, inc]);
        }
        return None;
    }
    if inc < *i {
        return None;
    }
    let lost = (inc > *i && open < *max).then_some([*max, *was]);
    if lost.is_some() || sequence > *max {
        (*max, *was) = (sequence, inc);
    }
    *i = inc;
    lost
}

/// `at` is the record or the Ready's start, `last` the record handled before it.
fn anomaly(sh: &Shared, f: &Follower, kind: &str, at: Cursor, last: Option<Cursor>, tag: &str) {
    let counter = match kind {
        "dup" => &sh.counts.dups,
        "reorder" => &sh.counts.reorders,
        _ => &sh.counts.gaps,
    };
    counter.fetch_add(1, Relaxed);
    let last = last.map(|c| [c.generation, c.sequence]);
    sh.log.line(kind, json!({"s": f.name, "g": at.generation, "seq": at.sequence, "last": last, "start": tag}));
}

async fn report(sh: Arc<Shared>, followers: Vec<Arc<Follower>>) {
    let mut tick = crate::reports();
    loop {
        tick.tick().await;
        let ms = sh.ms();
        let quiet: Map<_, _> = followers
            .iter()
            .map(|f| (f.name.clone(), ms.saturating_sub(f.last_ms.load(Relaxed))))
            .filter(|(_, q)| *q >= QUIET_MS)
            .map(|(s, q)| (s, q.into()))
            .collect();
        let c = &sh.counts;
        sh.log.line(
            "streams",
            json!({"recv": c.recv.swap(0, Relaxed), "gaps": c.gaps.swap(0, Relaxed), "dups": c.dups.swap(0, Relaxed),
                   "reorders": c.reorders.swap(0, Relaxed), "resets": c.resets.swap(0, Relaxed),
                   "origin_lost": c.origin_lost.swap(0, Relaxed), "afters": c.afters.swap(0, Relaxed),
                   "max_lag_ms": c.max_lag_ms.swap(0, Relaxed),
                   "quiet": quiet}),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(generation: u64, sequence: u64) -> Cursor {
        Cursor { generation, sequence }
    }

    #[test]
    fn a_follower_names_each_dup_reorder_and_gap_and_moves_only_where_a_ready_says() {
        let mut live = Seen::new(None);
        assert_eq!(live.ready(None), None, "a relay that has no position yet");
        assert_eq!(live.ready(Some(at(1, 5))), None, "a live start begins where its Ready says");
        assert_eq!(live.record(1, 6), None);
        assert_eq!(live.record(1, 6), Some("dup"));
        assert_eq!(live.record(1, 9), Some("gap"));
        assert_eq!(live.record(1, 8), Some("reorder"), "a record the gap skipped, late");
        assert_eq!(live.record(1, 5), Some("dup"));
        assert_eq!(live.record(1, 10), None);
        assert_eq!(live.ready(Some(at(1, 12))), Some("gap"), "a Ready that moves within a generation hides a gap");
        assert_eq!(live.ready(Some(at(2, 30))), None, "a new generation may start at the live edge");
        assert_eq!(live.record(2, 31), None);
        assert_eq!(live.record(3, 1), None, "a new generation from its record 1");
        assert_eq!(live.record(4, 7), Some("gap"), "a new generation without a Ready, not from record 1");
        assert_eq!(live.record(3, 2), Some("reorder"), "the generation before, from a stale relay");
        assert_eq!(live.ready(Some(at(2, 1))), Some("reorder"));
        assert_eq!(live.record(4, 8), None, "neither moved it back");

        let mut after = Seen::new(Some(at(2, 7)));
        assert_eq!(after.ready(Some(at(2, 7))), None);
        assert_eq!(after.record(2, 7), Some("dup"), "strictly after the cursor");
        assert_eq!(after.record(2, 9), Some("gap"));
    }

    #[test]
    fn a_follower_names_an_incarnation_that_opened_behind_what_it_had() {
        let mut run = None;
        assert_eq!(rewritten(&mut run, 3, 1, 10, 0), None);
        assert_eq!(rewritten(&mut run, 3, 40, 10, 0), None);
        assert_eq!(rewritten(&mut run, 3, 41, 11, 40), None, "a restart that resumed after the last record");
        assert_eq!(rewritten(&mut run, 3, 5, 10, 0), None, "the incarnation before, replayed from the disk");
        assert_eq!(rewritten(&mut run, 3, 38, 12, 37), Some([41, 11]), "a restart that went back over 38 to 41");
        assert_eq!(rewritten(&mut run, 3, 39, 12, 37), None);
        assert_eq!(rewritten(&mut run, 3, 40, 13, 39), None, "resumed after that one's last, though below the lost 41");
        assert_eq!(rewritten(&mut run, 3, 60, 14, 20), Some([40, 13]), "and past them, unseen: its open says so");
        assert_eq!(rewritten(&mut run, 2, 99, 15, 0), None, "a stale relay's generation changes nothing");
        assert_eq!(rewritten(&mut run, 4, 1, 15, 0), None, "a new generation, the disk formatted");
        assert_eq!(rewritten(&mut run, 4, 2, 16, 1), None);
    }
}
