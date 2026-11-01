use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Waker;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

/// Monotonic time plus a deadline wake registry. All actor timers and deadlines use this.
#[derive(Clone)]
pub struct Clock(Arc<Handle>);

struct Handle {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Clock").field(&self.now()).finish()
    }
}

struct Shared {
    origin: Instant,
    source: Source,
    state: Mutex<State>,
    changed: Condvar,
}

enum Source {
    System,
    Manual(AtomicU64),
    /// A threadless runtime's: its tokio runtime's time, with the wakes fired by its pump.
    Driven(Box<dyn Fn() -> Duration + Send + Sync>),
}

#[derive(Default)]
struct State {
    wakes: BTreeMap<WakeId, Waker>,
    /// The system clock's deadlines, by the grain each ends in. Most are cancelled soon after they are registered,
    /// and an ask's place among a thousand others in `wakes` cost a third of the asking thread.
    grains: BTreeMap<u64, Grain>,
    next: u64,
    thread_running: bool,
    /// The system clock's thread: the wake it sleeps until, `MAX` with none, `ZERO` while it is awake and looks at
    /// `wakes` before it sleeps again. Only a wake earlier than this one notifies it.
    sleeps_until: Duration,
    closed: bool,
    /// A parked pump, woken by a wake earlier than the one it sleeps until.
    pump: Option<Waker>,
}

/// A system clock deadline fires at the end of its grain, at most this much late: 2^20 ns, about a millisecond.
const GRAIN_SHIFT: u32 = 20;

/// The deadlines of one grain. `serial` tells this grain from one registered later under the same number, once this
/// one has fired. A cancelled deadline's slot goes to `free` for the next, so a grain holds no more slots than it had
/// deadlines at once. An empty grain is removed, but for the last: a thread that asks one at a time then allocates
/// nothing, and the clock thread sleeping until it is not woken by each next ask, as it would be with none left.
struct Grain {
    serial: u64,
    wakers: Vec<Option<Waker>>,
    free: Vec<usize>,
}

/// Where a deadline's wake is kept.
pub(crate) enum Deadline {
    Exact(WakeId),
    Grain { grain: u64, serial: u64, index: usize },
}

impl Grain {
    fn is_empty(&self) -> bool {
        self.free.len() == self.wakers.len()
    }
}

fn grain_end(grain: u64) -> Duration {
    Duration::from_nanos(grain.saturating_mul(1 << GRAIN_SHIFT))
}

/// When, then the order registered in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct WakeId(Duration, u64);

impl WakeId {
    pub(crate) fn at(self) -> Duration {
        self.0
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.shared.state.lock().closed = true;
        self.shared.changed.notify_all();
    }
}

impl Clock {
    /// Real time. A background thread is started on the first registration and exits when
    /// the last clone of this clock is dropped.
    pub fn system() -> Self {
        Self::new(Source::System)
    }

    pub fn manual() -> ManualClock {
        ManualClock(Self::new(Source::Manual(AtomicU64::new(0))))
    }

    pub(crate) fn driven(now: impl Fn() -> Duration + Send + Sync + 'static) -> Self {
        Self::new(Source::Driven(Box::new(now)))
    }

    #[expect(clippy::disallowed_methods, reason = "the system clock's origin; the other sources never read it")]
    fn new(source: Source) -> Self {
        Self(Arc::new(Handle {
            shared: Arc::new(Shared {
                origin: Instant::now(),
                source,
                state: Mutex::default(),
                changed: Condvar::new(),
            }),
        }))
    }

    pub(crate) fn is_system(&self) -> bool {
        matches!(self.0.shared.source, Source::System)
    }

    pub(crate) fn is_driven(&self) -> bool {
        matches!(self.0.shared.source, Source::Driven(_))
    }

    pub fn now(&self) -> Duration {
        let s = &self.0.shared;
        match &s.source {
            #[expect(clippy::disallowed_methods, reason = "the system clock is real time")]
            Source::System => s.origin.elapsed(),
            Source::Manual(ns) => Duration::from_nanos(ns.load(Ordering::Acquire)),
            Source::Driven(now) => now(),
        }
    }

    pub(crate) fn register(&self, at: Duration, waker: Waker) -> WakeId {
        let s = &self.0.shared;
        let mut state = s.state.lock();
        let id = WakeId(at, state.next);
        state.next += 1;
        state.wakes.insert(id, waker);
        match &s.source {
            Source::System => Shared::rouse(s, &mut state, at),
            // First once inserted: earlier than every other, as a tie goes to the one registered first.
            Source::Driven(_) if state.wakes.first_key_value().is_some_and(|(k, _)| *k == id) => {
                if let Some(pump) = state.pump.take() {
                    drop(state);
                    pump.wake();
                }
            }
            _ => {}
        }
        id
    }

    pub(crate) fn cancel(&self, id: WakeId) {
        let waker = self.0.shared.state.lock().wakes.remove(&id);
        drop(waker);
    }

    /// A wake for a deadline, which is almost always cancelled first: on the system clock it fires at the end of its
    /// grain, elsewhere at `at` exactly.
    pub(crate) fn register_deadline(&self, at: Duration, waker: Waker) -> Deadline {
        let s = &self.0.shared;
        if !self.is_system() {
            return Deadline::Exact(self.register(at, waker));
        }
        let grain = nanos(at).div_ceil(1 << GRAIN_SHIFT);
        let mut state = s.state.lock();
        state.next += 1;
        let serial = state.next;
        let last = state.grains.last_key_value().filter(|(last, kept)| **last < grain && kept.is_empty());
        if let Some((&last, _)) = last {
            state.grains.remove(&last);
        }
        let kept = state.grains.entry(grain).or_insert_with(|| Grain { serial, wakers: Vec::new(), free: Vec::new() });
        let index = match kept.free.pop() {
            Some(index) => {
                kept.wakers[index] = Some(waker);
                index
            }
            None => {
                kept.wakers.push(Some(waker));
                kept.wakers.len() - 1
            }
        };
        let deadline = Deadline::Grain { grain, serial: kept.serial, index };
        Shared::rouse(s, &mut state, grain_end(grain));
        deadline
    }

    pub(crate) fn cancel_deadline(&self, deadline: Deadline) {
        let (grain, serial, index) = match deadline {
            Deadline::Exact(id) => return self.cancel(id),
            Deadline::Grain { grain, serial, index } => (grain, serial, index),
        };
        let mut state = self.0.shared.state.lock();
        let Some(kept) = state.grains.get_mut(&grain).filter(|g| g.serial == serial) else { return };
        let waker = kept.wakers.get_mut(index).and_then(Option::take);
        if waker.is_some() {
            kept.free.push(index);
            if kept.is_empty() && state.grains.last_key_value().is_some_and(|(&last, _)| last != grain) {
                state.grains.remove(&grain);
            }
        }
        drop(state);
        drop(waker);
    }

    pub fn pending_wakes(&self) -> usize {
        let state = self.0.shared.state.lock();
        state.wakes.len() + state.grains.values().map(|g| g.wakers.len() - g.free.len()).sum::<usize>()
    }

    /// Wakes what is due by `now`.
    pub(crate) fn fire(&self, now: Duration) {
        let due = Shared::take_due(&mut self.0.shared.state.lock(), now);
        crate::lifecycle::wake_all(due);
    }

    /// The first wake, and `pump` is woken if an earlier one is registered.
    pub(crate) fn park(&self, pump: &Waker) -> Option<Duration> {
        let mut state = self.0.shared.state.lock();
        state.pump = Some(pump.clone());
        state.wakes.first_key_value().map(|(k, _)| k.0)
    }

    /// Drops every wake, and what it would have woken.
    pub(crate) fn clear(&self) {
        let mut state = self.0.shared.state.lock();
        let taken = (std::mem::take(&mut state.wakes), std::mem::take(&mut state.grains));
        drop(state);
        drop(taken);
    }
}

impl Shared {
    /// Starts the system clock's thread, or wakes it for a wake earlier than the one it sleeps until.
    fn rouse(self: &Arc<Self>, state: &mut State, at: Duration) {
        if !state.thread_running {
            let shared = self.clone();
            // Latched only once the thread exists: a failed spawn is retried by the next registration.
            #[expect(clippy::disallowed_methods, reason = "the system clock's timer thread")]
            let spawned = std::thread::Builder::new().name("axon-clock".into()).spawn(move || shared.run());
            state.thread_running = spawned.is_ok();
            spawned.expect("spawn clock thread");
        } else if at < state.sleeps_until {
            state.sleeps_until = Duration::ZERO;
            self.changed.notify_all();
        }
    }

    fn take_due(state: &mut State, now: Duration) -> Vec<Waker> {
        let mut due = Vec::new();
        while let Some(entry) = state.wakes.first_entry() {
            if entry.key().0 > now {
                break;
            }
            due.push(entry.remove());
        }
        while let Some(entry) = state.grains.first_entry() {
            if grain_end(*entry.key()) > now {
                break;
            }
            due.extend(entry.remove().wakers.into_iter().flatten());
        }
        due
    }

    fn first(state: &State) -> Option<Duration> {
        let grain = state.grains.first_key_value().map(|(&g, _)| grain_end(g));
        state.wakes.first_key_value().map(|(k, _)| k.0).into_iter().chain(grain).min()
    }

    #[expect(clippy::disallowed_methods, reason = "the system clock's timer thread waits in real time")]
    fn run(&self) {
        let mut state = self.state.lock();
        while !state.closed {
            let due = Self::take_due(&mut state, self.origin.elapsed());
            if !due.is_empty() {
                drop(state);
                crate::lifecycle::wake_all(due);
                state = self.state.lock();
                continue;
            }
            match Self::first(&state) {
                Some(at) => {
                    state.sleeps_until = at;
                    let wait = at.saturating_sub(self.origin.elapsed());
                    self.changed.wait_for(&mut state, wait);
                }
                None => {
                    state.sleeps_until = Duration::MAX;
                    self.changed.wait(&mut state);
                }
            }
            state.sleeps_until = Duration::ZERO;
        }
    }
}

/// `d` in nanoseconds, `u64::MAX` past what that holds (about 584 years).
pub(crate) fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// A deadline's wake, registered again only when the task's waker changes.
#[derive(Default)]
pub(crate) struct DeadlineWake(Option<(Deadline, Waker)>);

impl DeadlineWake {
    pub(crate) fn arm(&mut self, clock: &Clock, at: Option<Duration>, waker: &Waker) {
        let Some(at) = at else { return };
        if self.0.as_ref().is_some_and(|(_, w)| w.will_wake(waker)) {
            return;
        }
        self.disarm(clock);
        self.0 = Some((clock.register_deadline(at, waker.clone()), waker.clone()));
    }

    pub(crate) fn armed(&self) -> bool {
        self.0.is_some()
    }

    pub(crate) fn disarm(&mut self, clock: &Clock) {
        if let Some((deadline, _)) = self.0.take() {
            clock.cancel_deadline(deadline);
        }
    }
}

pub struct ManualClock(Clock);

impl ManualClock {
    pub fn clock(&self) -> Clock {
        self.0.clone()
    }

    pub fn advance(&self, by: Duration) {
        let Source::Manual(ns) = &self.0.0.shared.source else { unreachable!("a manual clock") };
        ns.store(ns.load(Ordering::Acquire).saturating_add(nanos(by)), Ordering::Release);
        self.0.fire(self.0.now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Asks one at a time, each a grain later than the last, leave one empty grain, and a deadline still due keeps its own.
    #[test]
    fn only_the_last_empty_grain_is_kept() {
        let clock = Clock::system();
        let grains = || clock.0.shared.state.lock().grains.len();
        let ms = |n: u64| Duration::from_secs(3600) + Duration::from_millis(n);
        let held = clock.register_deadline(ms(0), Waker::noop().clone());
        for n in 1..1000 {
            let deadline = clock.register_deadline(ms(n), Waker::noop().clone());
            clock.cancel_deadline(deadline);
        }
        assert_eq!((grains(), clock.pending_wakes()), (2, 1));
        let earlier = clock.register_deadline(ms(500), Waker::noop().clone());
        clock.cancel_deadline(earlier);
        assert_eq!(grains(), 2);
        clock.cancel_deadline(held);
        assert_eq!((grains(), clock.pending_wakes()), (1, 0));
    }
}
