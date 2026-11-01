//! Dead letters: messages that were accepted and then never handled, with nobody told.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;

use crate::clock::Clock;
use crate::trace::{Span, SpanKind, TraceContext, Tracer, unix_micros};
use crate::types::{ActorIncarnation, Exit, RejectReason, StopReason};

const CAPACITY: usize = 1024;
const COALESCE: Duration = Duration::from_secs(1);
const COALESCE_BACK: usize = 16;
/// A group stops taking repeats at this age, so a steady flow still settles.
const COALESCE_SPAN: Duration = Duration::from_secs(10);

#[derive(Clone, Debug)]
pub struct DeadLetter {
    /// Position in the recording system's ring, from 1.
    pub seq: u64,
    /// When it was last seen, on the wall clock: for display only, as the ring groups and settles on the system's clock.
    pub at_us: u64,
    pub reason: &'static str,
    /// The message label, the call kind of a message another node sent, or `publish` for one a topic's subscriber missed.
    pub label: &'static str,
    pub actor: Option<(ActorIncarnation, &'static str)>,
    /// The actor's registered name, the endpoint a remote message was for, or the topic.
    pub target: Option<Arc<str>>,
    /// The node that sent it, when it came from another node.
    pub peer: Option<Arc<str>>,
    /// The first trace among the coalesced messages; 0 for none.
    pub trace: u64,
    pub count: u64,
}

impl DeadLetter {
    fn same(&self, other: &Self) -> bool {
        (self.reason, self.label, self.actor.map(|a| a.0), &self.target, &self.peer)
            == (other.reason, other.label, other.actor.map(|a| a.0), &other.target, &other.peer)
    }
}

/// One lost on this node, now.
fn letter(
    reason: &'static str,
    label: &'static str,
    actor: Option<(ActorIncarnation, &'static str)>,
    target: Option<Arc<str>>,
    count: u64,
) -> DeadLetter {
    DeadLetter { seq: 0, at_us: unix_micros(), reason, label, actor, target, peer: None, trace: 0, count }
}

#[derive(Default)]
struct Ring {
    /// The last position given out.
    next: u64,
    /// The last position `settled` handed out: nothing up to it changes again.
    collected: u64,
    /// Each letter with its first and last sighting on the clock.
    letters: VecDeque<(Duration, Duration, DeadLetter)>,
}

/// The most recent dead letters of one system.
pub struct DeadLetters {
    ring: Mutex<Ring>,
    discarded: AtomicU64,
    /// Monotonic, so a wall step neither merges letters nor holds them back.
    clock: Clock,
    tracer: Arc<Tracer>,
}

impl DeadLetters {
    #[cfg_attr(not(any(feature = "system", test)), expect(dead_code, reason = "only an ActorSystem makes one"))]
    pub(crate) fn new(clock: Clock, tracer: Arc<Tracer>) -> Self {
        Self { ring: Mutex::default(), discarded: AtomicU64::new(0), clock, tracer }
    }

    /// What an actor still had queued when it ended, by label.
    pub(crate) fn record_exit(&self, exit: &Exit, actor_type: &'static str, name: Option<&Arc<str>>) {
        let reason = match exit.reason {
            StopReason::Failed(_) | StopReason::StartFailed(_) | StopReason::Linked(_) => "actor_failed",
            StopReason::Panicked => "actor_panicked",
            // A drain handles all that was queued: what a clean one leaves was still stashed.
            StopReason::Drained => "drained",
            StopReason::DrainDeadline => "drain_deadline",
            StopReason::RuntimeShutdown | StopReason::DriverDropped => "shutdown",
            StopReason::Stopped => "actor_stopped",
            StopReason::Fenced => "fenced",
        };
        for lost in &exit.discarded {
            self.discarded.fetch_add(lost.count as u64, Ordering::Relaxed);
            let actor = Some((exit.incarnation, actor_type));
            self.record(letter(reason, lost.label, actor, name.cloned(), lost.count as u64), lost.trace);
        }
    }

    /// A message a topic's subscriber refused with `reason`, labelled `publish` under the topic's name.
    #[cfg_attr(not(feature = "system"), expect(dead_code, reason = "only a topic records one"))]
    pub(crate) fn record_missed(&self, reason: RejectReason, topic: &Arc<str>) {
        self.discarded.fetch_add(1, Ordering::Relaxed);
        let letter = letter(RejectReason::NAMES[reason as usize], "publish", None, Some(topic.clone()), 1);
        self.record(letter, TraceContext::current());
    }

    /// Messages actors on this system accepted and never handled, and topic messages a subscriber missed, since it
    /// was built.
    pub fn discarded(&self) -> u64 {
        self.discarded.load(Ordering::Relaxed)
    }

    /// A repeat of a recent letter within a second of its last sighting takes over its count. `cause` is what the first
    /// traced message in it carried: no handler ran that message, so its drop becomes its span, failed and of zero
    /// length, or its trace would show nothing where it ended. `cause`, when given, also sets `trace`; with `None` the
    /// letter keeps the trace it carries.
    pub fn record(&self, mut letter: DeadLetter, cause: Option<TraceContext>) {
        if let Some(cause) = cause {
            let mut span = Span::begin(cause, cause.child().span, SpanKind::Handle, letter.label);
            (span.actor, span.peer, span.failed) = (letter.actor, letter.peer.clone(), true);
            self.tracer.record(span);
            letter.trace = cause.trace.get();
        }
        let now = self.clock.now();
        let mut ring = self.ring.lock();
        let Ring { next, collected, letters } = &mut *ring;
        let recent = letters.iter().rev().take(COALESCE_BACK);
        let found = recent.into_iter().position(|(first, last, l)| {
            l.seq > *collected
                && l.same(&letter)
                && now.saturating_sub(*last) < COALESCE
                && now.saturating_sub(*first) < COALESCE_SPAN
        });
        let mut first = now;
        if let Some((was, _, old)) = found.and_then(|back| letters.remove(letters.len() - 1 - back)) {
            first = was;
            letter.count += old.count;
            letter.trace = if old.trace != 0 { old.trace } else { letter.trace };
        }
        *next += 1;
        letter.seq = *next;
        if letters.len() == CAPACITY {
            letters.pop_front();
        }
        letters.push_back((first, now, letter));
    }

    /// Up to `limit` letters, newest first.
    pub fn recent(&self, limit: usize) -> Vec<DeadLetter> {
        self.ring.lock().letters.iter().rev().take(limit).map(|(_, _, l)| l.clone()).collect()
    }

    /// Up to `limit` letters after position `after` that can no longer take a repeat, oldest first.
    pub fn settled(&self, after: u64, limit: usize) -> Vec<DeadLetter> {
        let now = self.clock.now();
        let mut ring = self.ring.lock();
        let skip = ring.letters.partition_point(|(_, _, l)| l.seq <= after);
        let ready = ring.letters.iter().skip(skip).take_while(|(_, last, _)| now.saturating_sub(*last) >= COALESCE);
        let ready: Vec<_> = ready.take(limit).map(|(_, _, l)| l.clone()).collect();
        ring.collected = ring.collected.max(ready.last().map_or(0, |l| l.seq));
        ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeats_within_a_second_coalesce_and_the_ring_is_bounded() {
        let letter = |at_us, reason| DeadLetter {
            seq: 0,
            at_us,
            reason,
            label: "tell",
            actor: None,
            target: None,
            peer: None,
            trace: 0,
            count: 1,
        };
        let manual = Clock::manual();
        let at = |ring: &DeadLetters, after: Duration, reason| {
            manual.advance(after);
            ring.record(letter(manual.clock().now().as_micros() as u64, reason), None);
        };
        let (us, second) = (Duration::from_micros(10), COALESCE.as_micros() as u64);
        let ring = DeadLetters::new(manual.clock(), Tracer::new(0, 0));
        at(&ring, us, "full");
        at(&ring, us, "stopped");
        at(&ring, us, "full");
        at(&ring, COALESCE, "full");
        let seen: Vec<_> = ring.recent(10).iter().map(|l| (l.seq, l.at_us, l.reason, l.count)).collect();
        assert_eq!(seen, [(4, 30 + second, "full", 1), (3, 30, "full", 2), (2, 20, "stopped", 1)]);

        manual.advance(COALESCE);
        assert_eq!(ring.settled(2, 10).iter().map(|l| l.seq).collect::<Vec<_>>(), [3, 4]);
        at(&ring, Duration::ZERO, "full");
        assert_eq!(ring.recent(1)[0].count, 1, "a collected letter takes no repeat");
        at(&ring, COALESCE, "full");
        assert_eq!(ring.settled(4, 10).len(), 1, "a letter that may still take a repeat is held back");

        let late = DeadLetters::new(manual.clock(), Tracer::new(0, 0));
        at(&late, us, "full");
        at(&late, COALESCE * 9 / 10, "full");
        manual.advance(COALESCE * 6 / 10);
        assert!(late.settled(0, 10).is_empty(), "held back by its last sighting, not its first");

        // The wall stepped an hour back after the first: 2 s apart on the clock, they neither merge nor wait.
        let stepped = DeadLetters::new(manual.clock(), Tracer::new(0, 0));
        let hour = 3_600_000_000;
        stepped.record(letter(2 * hour, "full"), None);
        for n in 1..3 {
            manual.advance(2 * COALESCE);
            stepped.record(letter(hour + n, "full"), None);
        }
        manual.advance(COALESCE);
        assert_eq!(stepped.recent(10).iter().map(|l| l.count).collect::<Vec<_>>(), [1, 1, 1]);
        assert_eq!(stepped.settled(0, 10).iter().map(|l| l.seq).collect::<Vec<_>>(), [1, 2, 3]);

        let steady = DeadLetters::new(manual.clock(), Tracer::new(0, 0));
        (0..=20).for_each(|_| at(&steady, COALESCE / 2, "full"));
        assert_eq!(steady.recent(10).iter().map(|l| l.count).collect::<Vec<_>>(), [1, 20]);

        for _ in 0..2 * CAPACITY {
            at(&ring, 2 * COALESCE, "full");
        }
        assert_eq!(ring.recent(usize::MAX).len(), CAPACITY);
    }
}
