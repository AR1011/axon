//! What happened on this node that belongs on a cluster's timeline: keyed actors moving, supervised families
//! restarting, streams losing their parent. A bounded ring that a collector pages through; recording never waits on it.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use parking_lot::Mutex;

use crate::clock::Clock;
use crate::trace::unix_micros;

/// Events one log holds.
pub const EVENTS: usize = 256;
/// Bytes a subject or a detail keeps; the rest is cut when it is recorded.
const TEXT: usize = 1024;
/// A family restarting in a loop is told at most once in this long.
const RESTART_WINDOW: Duration = Duration::from_secs(60);
/// Families that restarted within their window and are remembered; past it each restart is told at once.
const FAMILIES: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    /// Position in the recording system's log, from 1.
    pub seq: u64,
    /// When it was recorded, on the wall clock.
    pub at_us: u64,
    pub kind: &'static str,
    /// What it happened to: a keyed actor's `kind/key`, a supervised family's name, a stream.
    pub subject: String,
    pub detail: String,
    /// The occurrences it stands for: above 1 only for a family's restarts told after their window.
    pub count: u64,
}

struct Window {
    opened: Duration,
    detail: String,
    /// Restarts since the window opened and not yet told.
    held: u64,
}

#[derive(Default)]
struct Log {
    next: u64,
    events: VecDeque<Event>,
    windows: BTreeMap<String, Window>,
}

impl Log {
    fn push(&mut self, kind: &'static str, mut subject: String, mut detail: String, count: u64) {
        subject.truncate(subject.floor_char_boundary(TEXT));
        detail.truncate(detail.floor_char_boundary(TEXT));
        self.next += 1;
        if self.events.len() == EVENTS {
            self.events.pop_front();
        }
        let event = Event { seq: self.next, at_us: unix_micros(), kind, subject, detail, count };
        self.events.push_back(event);
    }

    /// Tells what each window that is over held, and opens the next one where it held anything.
    fn close(&mut self, now: Duration) {
        let mut due = Vec::new();
        self.windows.retain(|subject, w| {
            if now.saturating_sub(w.opened) < RESTART_WINDOW {
                return true;
            }
            if w.held == 0 {
                return false;
            }
            due.push((subject.clone(), std::mem::take(&mut w.detail), w.held));
            (w.opened, w.held) = (now, 0);
            true
        });
        due.into_iter().for_each(|(subject, detail, count)| self.push("actor_restarted", subject, detail, count));
    }
}

/// The newest [`EVENTS`] events of one system.
pub struct EventLog {
    log: Mutex<Log>,
    /// Monotonic, so a wall step neither shortens nor stretches a window.
    clock: Clock,
}

impl EventLog {
    pub fn new(clock: Clock) -> Self {
        Self { log: Mutex::default(), clock }
    }

    pub fn record(&self, kind: &'static str, subject: impl Into<String>, detail: impl Into<String>) {
        self.log.lock().push(kind, subject.into(), detail.into(), 1);
    }

    /// An `actor_restarted` of `family`, at most one a minute: the first at once, and those after it in its window as
    /// one event, with their count and the last detail, once the window is over.
    pub(crate) fn record_restart(&self, family: String, detail: String) {
        let now = self.clock.now();
        let mut log = self.log.lock();
        log.close(now);
        if let Some(window) = log.windows.get_mut(&family) {
            (window.held, window.detail) = (window.held + 1, detail);
            return;
        }
        if log.windows.len() < FAMILIES {
            log.windows.insert(family.clone(), Window { opened: now, detail: String::new(), held: 0 });
        }
        log.push("actor_restarted", family, detail, 1);
    }

    /// Up to `limit` events after position `after`, oldest first.
    pub fn since(&self, after: u64, limit: usize) -> Vec<Event> {
        let mut log = self.log.lock();
        log.close(self.clock.now());
        let skip = log.events.partition_point(|e| e.seq <= after);
        log.events.iter().skip(skip).take(limit).cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn families_past_the_bound_are_each_told_at_once_and_text_is_cut() {
        let clock = Clock::manual();
        let log = EventLog::new(clock.clock());
        for n in 0..FAMILIES + 10 {
            log.record_restart(format!("f{n}"), "boom".repeat(TEXT));
            log.record_restart(format!("f{n}"), "again".into());
        }
        assert_eq!(log.log.lock().windows.len(), FAMILIES);
        let told = log.since(0, usize::MAX);
        assert!(told.iter().all(|e| e.detail.len() <= TEXT && e.count == 1));
        // A remembered family's second restart waits for its window; one past the bound tells it at once.
        let last: Vec<_> = told[told.len() - 3..].iter().map(|e| (e.subject.as_str(), e.detail.len())).collect();
        assert_eq!(last, [("f264", 5), ("f265", TEXT), ("f265", 5)]);
        clock.advance(RESTART_WINDOW);
        let held = log.since(told.last().expect("told").seq, usize::MAX);
        assert_eq!((held.len(), held[0].count, held[0].detail.as_str()), (FAMILIES, 1, "again"));
    }
}
