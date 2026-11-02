use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crate::clock::{Clock, DeadlineWake, nanos};
use crate::context::Watch;
use crate::dead::DeadLetters;
use crate::events::EventLog;
use crate::histogram::{Timings, sampled};
use crate::lifecycle::{ExitSignal, Lifecycle, LifecycleHandle, wake_all};
use crate::sync::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Mutex};
use crate::trace::{TraceContext, Tracer};
use crate::types::*;

pub type Weight<M> = Arc<dyn Fn(&M) -> usize + Send + Sync>;

pub struct MailboxConfig<M> {
    pub limits: MailboxLimits,
    /// Declared retained bytes per message; defaults to `size_of::<M>()`.
    pub weight: Option<Weight<M>>,
    /// Queue slots reserved in one step when a message is admitted into a queue with none; an actor never sent
    /// anything holds none. A burst no longer than this never regrows the queue. Raise it for actors whose first burst
    /// is long.
    pub first_block: usize,
    /// `0` disables timing histograms and message age tracking.
    pub timing_sample_every: u32,
    /// Where the actor's spans go. Without one, trace context is still carried on.
    pub tracer: Option<Arc<Tracer>>,
    /// Where a supervised family records what a generation drops before it is replaced, and the family's name.
    pub dead: Option<(Arc<DeadLetters>, Option<Arc<str>>)>,
    /// Where a supervised family records its restarts, at most one event a minute.
    pub events: Option<Arc<EventLog>>,
    pub fence: Option<Fence>,
    /// Drains the actor once it has handled no message for this long, with as long again as the drain's deadline;
    /// timers and job results do not count.
    pub idle: Option<Duration>,
}

/// A deadline past which an actor handles nothing, held open by its owner: a lease whose actor must not act once it
/// lapses, however late a paused process wakes. It is read on the actor clock, which runs on through a pause, before
/// every message, timer and completion; once lapsed, the actor stops with [`StopReason::Fenced`].
#[derive(Clone, Debug)]
pub struct Fence(Arc<(Clock, std::sync::atomic::AtomicU64)>);

impl Fence {
    pub fn new(clock: &Clock, lapses_in: Duration) -> Self {
        let fence = Self(Arc::new((clock.clone(), Default::default())));
        fence.hold_for(lapses_in);
        fence
    }

    /// Moves the deadline later or earlier; `hold_for(Duration::ZERO)` lapses it.
    pub fn hold_for(&self, d: Duration) {
        self.0.1.store(nanos(self.0.0.now().saturating_add(d)), Ordering::Relaxed);
    }

    pub fn is_open(&self) -> bool {
        nanos(self.0.0.now()) < self.0.1.load(Ordering::Relaxed)
    }
}

/// Whoever asks, an actor past its fence stops because of it: its owner knows why it ends.
pub(crate) fn past_fence(fence: Option<&Fence>, reason: StopReason) -> StopReason {
    if reason == StopReason::Stopped && fence.is_some_and(|f| !f.is_open()) { StopReason::Fenced } else { reason }
}

impl<M> Clone for MailboxConfig<M> {
    fn clone(&self) -> Self {
        Self {
            limits: self.limits,
            weight: self.weight.clone(),
            first_block: self.first_block,
            timing_sample_every: self.timing_sample_every,
            tracer: self.tracer.clone(),
            dead: self.dead.clone(),
            events: self.events.clone(),
            fence: self.fence.clone(),
            idle: self.idle,
        }
    }
}

impl<M> Default for MailboxConfig<M> {
    fn default() -> Self {
        MailboxLimits::default().into()
    }
}

impl<M> From<MailboxLimits> for MailboxConfig<M> {
    fn from(limits: MailboxLimits) -> Self {
        Self {
            limits,
            weight: None,
            first_block: 4,
            timing_sample_every: 0,
            tracer: None,
            dead: None,
            events: None,
            fence: None,
            idle: None,
        }
    }
}

impl<M> MailboxConfig<M> {
    pub fn with_weight(mut self, weight: impl Fn(&M) -> usize + Send + Sync + 'static) -> Self {
        self.weight = Some(Arc::new(weight));
        self
    }

    pub fn with_first_block(mut self, slots: usize) -> Self {
        self.first_block = slots;
        self
    }

    pub fn with_timing(mut self, sample_every: u32) -> Self {
        self.timing_sample_every = sample_every;
        self
    }

    pub fn with_tracer(mut self, tracer: Arc<Tracer>) -> Self {
        self.tracer = Some(tracer);
        self
    }
}

pub(crate) struct Entry<M> {
    pub msg: M,
    pub bytes: usize,
    /// Admission time, set on sampled and traced admissions only.
    pub at: Option<Duration>,
    pub trace: Option<TraceContext>,
}

struct Waiter {
    id: u64,
    waker: Waker,
}

struct Hot<M> {
    queue: VecDeque<Entry<M>>,
    waiters: VecDeque<Waiter>,
    next_waiter: u64,
    admitted: u64,
    slots: usize,
    bytes: usize,
}

pub(crate) enum JobKind {
    /// Waited for by a drain.
    Job,
    /// Dropped by a drain.
    Source,
    /// Closed by a drain once the actor has nothing left to write.
    Sink(Arc<dyn crate::sink::Drainable>),
}

pub(crate) struct LiveJob {
    pub cancel: Arc<crate::jobs::Cancel>,
    pub kind: JobKind,
}

/// A job's result, or a sink's `Ready` (operation 0), waiting to be handled.
pub(crate) type Settled<M> = (u64, M, Option<TraceContext>);

pub(crate) struct JobState<M> {
    pub live: BTreeMap<u64, LiveJob>,
    /// How many of `live` are sources or sinks.
    pub sources: usize,
    /// In the order they settled. A job's result holds its slot of `MailboxLimits::jobs` until it is taken to be
    /// handled.
    pub done: VecDeque<Settled<M>>,
    /// The jobs the actor is paused on: live, or with a result in `done`. A few at most.
    pub pausing: Vec<u64>,
    /// How the first job to fail ends the actor.
    pub failure: Option<StopReason>,
    /// The actors this one watches and has not been told of. The watched actor holds each weakly, so it lives as long as
    /// the watch and no longer.
    pub watches: Vec<(LifecycleHandle, Arc<Watch<M>>)>,
    /// Exits told and not yet handled: mapped only when handled, so a cancel in between still holds.
    pub fired: VecDeque<(Arc<Watch<M>>, Exit)>,
}

/// What a drain does once the mailbox is empty.
pub(crate) enum DrainEnd {
    /// Nothing is left: the actor ends.
    Drained,
    /// Only sinks are left, the actor owes none of them and some are still open: it has written all it will.
    CloseSinks,
}

impl<M> Default for JobState<M> {
    fn default() -> Self {
        Self {
            live: BTreeMap::new(),
            sources: 0,
            done: VecDeque::new(),
            pausing: Vec::new(),
            failure: None,
            watches: Vec::new(),
            fired: VecDeque::new(),
        }
    }
}

impl<M> JobState<M> {
    /// What counts against `MailboxLimits::sources`: attached sources and sinks, and watches until handled.
    pub(crate) fn held_sources(&self) -> usize {
        self.sources + self.watches.len() + self.fired.len()
    }

    /// What counts against `MailboxLimits::jobs`: jobs in flight, and job results not yet taken to be handled.
    pub(crate) fn held_jobs(&self) -> usize {
        self.live.len() - self.sources + self.done.iter().filter(|(operation, ..)| *operation != 0).count()
    }

    /// Takes the next result to handle: while paused, the first of a job paused on, which lifts that pause; else the
    /// first.
    pub(crate) fn next_done(&mut self, paused: bool) -> Option<Settled<M>> {
        if !paused {
            return self.done.pop_front();
        }
        let at = self.done.iter().position(|(operation, ..)| self.pausing.contains(operation))?;
        let settled = self.done.remove(at)?;
        self.pausing.retain(|&operation| operation != settled.0);
        Some(settled)
    }

    /// Takes out `watch` if it has not been told.
    pub(crate) fn take_watch(&mut self, watch: &Watch<M>) -> Option<(LifecycleHandle, Arc<Watch<M>>)> {
        let at = self.watches.iter().position(|(_, w)| std::ptr::eq(&**w, watch))?;
        Some(self.watches.swap_remove(at))
    }

    /// `None` while the drain waits on a job, a job's result, or a sink the actor owes or that is closing.
    pub(crate) fn drain_end(jobs: Option<&Self>) -> Option<DrainEnd> {
        let Some(jobs) = jobs else { return Some(DrainEnd::Drained) };
        if !jobs.done.is_empty() || !jobs.fired.is_empty() {
            return None;
        }
        if jobs.live.is_empty() {
            return Some(DrainEnd::Drained);
        }
        let idle = |j: &LiveJob| matches!(&j.kind, JobKind::Sink(s) if !s.owes_ready());
        let open = |j: &LiveJob| matches!(&j.kind, JobKind::Sink(s) if !s.closing());
        (jobs.live.values().all(idle) && jobs.live.values().any(open)).then_some(DrainEnd::CloseSinks)
    }

    /// Whether `operation` was live.
    pub(crate) fn remove(&mut self, operation: u64) -> bool {
        let Some(job) = self.live.remove(&operation) else { return false };
        if !matches!(job.kind, JobKind::Job) {
            self.sources -= 1;
        }
        true
    }
}

#[derive(Default)]
struct Control {
    stop_reason: Option<StopReason>,
    drain_deadline: Option<Option<Duration>>,
}

/// Counters for cross-thread diagnostics: `processed`, `turns` and `budget_overruns` the driver's alone, `timers`
/// published by `Core::publish_timer_count`, and the ask counters bumped by the askers.
#[derive(Default)]
pub(crate) struct OwnerStats {
    pub processed: AtomicU64,
    pub turns: AtomicU64,
    pub budget_overruns: AtomicU64,
    pub timers: AtomicUsize,
    pub asks: AtomicU64,
    pub ask_timeouts: AtomicU64,
}

pub(crate) struct Mailbox<M> {
    incarnation: ActorIncarnation,
    pub(crate) limits: MailboxLimits,
    weight: Option<Weight<M>>,
    // A `u32` beside `sample_every`, so `queued` costs no size.
    first_block: u32,
    pub(crate) sample_every: u32,
    pub(crate) clock: Clock,
    /// Wakes the actor's turn; none for an `EmbeddedDriver`, whose caller runs it.
    pub(crate) notify: Option<Waker>,
    phase: AtomicU8,
    opened: AtomicBool,
    hot: Mutex<Hot<M>>,
    /// `hot.queue.len()`, set under `hot`; read without it by a least-loaded router, whose lock of every member for
    /// every message met the workers taking turns there. Declared after `hot`, so it shares a line with the tail of
    /// `Hot`, which a sender and a turn write anyway, and not one that either only reads.
    queued: AtomicUsize,
    reservations: AtomicUsize,
    rejected: [AtomicU64; 6],
    pub(crate) requests: AtomicUsize,
    /// Allocated by the first job and kept until the actor ends: most actors never start one.
    pub(crate) jobs: Mutex<Option<Box<JobState<M>>>>,
    pub(crate) jobs_live: AtomicUsize,
    // Set under `jobs` when a job leaves a result or a failure; lets a turn skip the lock. Raised and read only through
    // `flag_settled` and `settled`, SeqCst as the stop and drain flags are (see `runtime::Scheduled`), so the loom
    // model through `settle` holds the sink and watch paths too.
    pub(crate) jobs_settled: AtomicBool,
    pub(crate) stop_requested: AtomicBool,
    pub(crate) drain_requested: AtomicBool,
    // Written and read by the driver only: a flag, so a batch handed to `on_batch` can see it.
    pub(crate) paused: AtomicBool,
    // Set while stashed messages `unstash_all` released wait to run, as `paused` is: the batch stops for them.
    pub(crate) unstashed: AtomicBool,
    control: Mutex<Control>,
    pub(crate) signal: ExitSignal,
    pub(crate) stats: OwnerStats,
    // Boxed: over a kilobyte inline, and off by default.
    pub(crate) timings: Option<Box<Mutex<Timings>>>,
    pub(crate) tracer: Option<Arc<Tracer>>,
    fence: Option<Fence>,
}

impl<M: Send + 'static> Mailbox<M> {
    /// Asks the actor to stop once its fence has lapsed, and says whether it has.
    pub(crate) fn fenced(&self) -> bool {
        let lapsed = self.fence.as_ref().is_some_and(|f| !f.is_open());
        if lapsed {
            self.request_stop(StopReason::Fenced);
        }
        lapsed
    }

    /// Called under `jobs`, after what the turn is to find there, and before `notify`.
    pub(crate) fn flag_settled(&self) {
        self.jobs_settled.store(true, Ordering::SeqCst);
    }

    pub(crate) fn settled(&self) -> bool {
        self.jobs_settled.load(Ordering::SeqCst)
    }

    /// What a turn takes once `settled` said something did. The flag is cleared under `jobs` before anything there is
    /// read, so what settles after the read raises it again. Lifts a pause no job holds any more, and returns the
    /// failure a job left, how many results and downs there are to handle, and whether the pause lifted.
    pub(crate) fn take_settled(&self) -> (Option<StopReason>, usize, bool) {
        let mut jobs = self.jobs.lock();
        let jobs = jobs.get_or_insert_default();
        self.jobs_settled.store(false, Ordering::Release);
        let lifted = self.paused.load(Ordering::Relaxed) && jobs.pausing.is_empty();
        if lifted {
            self.paused.store(false, Ordering::Relaxed);
        }
        (jobs.failure.take(), jobs.done.len() + jobs.fired.len(), lifted)
    }

    pub(crate) fn new(
        config: MailboxConfig<M>,
        clock: Clock,
        incarnation: ActorIncarnation,
        notify: Option<Waker>,
    ) -> Arc<Self> {
        let every = config.timing_sample_every;
        Arc::new(Self {
            incarnation,
            limits: config.limits,
            weight: config.weight,
            first_block: u32::try_from(config.first_block.min(config.limits.messages)).unwrap_or(u32::MAX),
            sample_every: every,
            clock,
            notify,
            phase: AtomicU8::new(Phase::Starting as u8),
            opened: AtomicBool::new(false),
            hot: Mutex::new(Hot {
                queue: VecDeque::new(),
                waiters: VecDeque::new(),
                next_waiter: 0,
                admitted: 0,
                slots: 0,
                bytes: 0,
            }),
            queued: AtomicUsize::new(0),
            reservations: AtomicUsize::new(0),
            rejected: Default::default(),
            requests: AtomicUsize::new(0),
            jobs: Mutex::new(None),
            jobs_live: AtomicUsize::new(0),
            jobs_settled: AtomicBool::new(false),
            stop_requested: AtomicBool::new(false),
            drain_requested: AtomicBool::new(false),
            paused: AtomicBool::new(false),
            unstashed: AtomicBool::new(false),
            control: Mutex::default(),
            signal: ExitSignal::default(),
            stats: OwnerStats::default(),
            timings: (every != 0).then(|| Box::new(Mutex::new(Timings { sample_every: every, ..Default::default() }))),
            tracer: config.tracer,
            fence: config.fence,
        })
    }

    pub(crate) fn notify(&self) {
        if let Some(n) = &self.notify {
            n.wake_by_ref();
        }
    }

    pub(crate) fn phase(&self) -> Phase {
        Phase::from_u8(self.phase.load(Ordering::Acquire))
    }

    pub(crate) fn weigh(&self, msg: &M) -> usize {
        match &self.weight {
            Some(w) => w(msg),
            None => size_of::<M>(),
        }
    }

    // Caller holds `hot`; `ordinal` is the admission count including this message.
    fn stamp(&self, ordinal: u64, ambient: Option<TraceContext>) -> (Option<Duration>, Option<TraceContext>) {
        let trace = ambient.or_else(|| self.tracer.as_ref()?.sample(ordinal));
        ((trace.is_some() || sampled(self.sample_every, ordinal)).then(|| self.clock.now()), trace)
    }

    fn reject<T>(&self, reason: RejectReason, message: T) -> Reject<T> {
        self.count(reason);
        Reject { reason, message }
    }

    fn count(&self, reason: RejectReason) {
        self.rejected[reason as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn admission_phase(&self) -> Result<(), RejectReason> {
        match self.phase() {
            Phase::Starting | Phase::Running => Ok(()),
            _ => Err(RejectReason::Closed),
        }
    }

    fn try_take(&self, hot: &mut Hot<M>, bytes: usize) -> bool {
        let fits = hot.slots < self.limits.messages && hot.bytes.saturating_add(bytes) <= self.limits.bytes;
        if fits {
            hot.slots += 1;
            hot.bytes += bytes;
        }
        fits
    }

    fn try_tell(&self, msg: M) -> Result<Enqueued, Reject<M>> {
        let bytes = self.weigh(&msg);
        if bytes > self.limits.max_message_bytes {
            return Err(self.reject(RejectReason::TooLarge, msg));
        }
        self.admit(msg, bytes).inspect_err(|reject| self.count(reject.reason))
    }

    /// Admission at once, in one locked section: the actor takes messages, no waiter is queued and there is room. A
    /// refusal is the caller's to count.
    fn admit(&self, msg: M, bytes: usize) -> Result<Enqueued, Reject<M>> {
        let ambient = TraceContext::current();
        let mut hot = self.hot.lock();
        if let Err(reason) = self.admission_phase() {
            drop(hot);
            return Err(Reject { reason, message: msg });
        }
        if !hot.waiters.is_empty() || !self.try_take(&mut hot, bytes) {
            drop(hot);
            return Err(Reject { reason: RejectReason::Full, message: msg });
        }
        let was_empty = self.enqueue(&mut hot, msg, bytes, ambient);
        drop(hot);
        if was_empty {
            self.notify();
        }
        Ok(Enqueued)
    }

    /// Queues a message whose room is taken, under `hot`; says whether the queue was empty. A queue with no slots
    /// reserves `first_block` of them.
    fn enqueue(&self, hot: &mut Hot<M>, msg: M, bytes: usize, ambient: Option<TraceContext>) -> bool {
        let was_empty = hot.queue.is_empty();
        hot.admitted += 1;
        let (at, trace) = self.stamp(hot.admitted, ambient);
        if hot.queue.capacity() == 0 {
            // Only a hint: a block too large to allocate leaves the queue to grow as it fills.
            let _ = hot.queue.try_reserve(self.first_block as usize);
        }
        hot.queue.push_back(Entry { msg, bytes, at, trace });
        self.queued.store(hot.queue.len(), Ordering::Relaxed);
        was_empty
    }

    /// Moves up to `max` messages out; returns how many stay queued.
    pub(crate) fn pull(&self, max: usize, out: &mut VecDeque<Entry<M>>) -> usize {
        let mut hot = self.hot.lock();
        if out.is_empty() && !hot.queue.is_empty() && hot.queue.len() <= max {
            std::mem::swap(&mut hot.queue, out);
            self.queued.store(0, Ordering::Relaxed);
            return 0;
        }
        let n = max.min(hot.queue.len());
        out.extend(hot.queue.drain(..n));
        self.queued.store(hot.queue.len(), Ordering::Relaxed);
        hot.queue.len()
    }

    /// Gives back the capacity of what a turn took, or a permit held.
    pub(crate) fn release(&self, slots: usize, bytes: usize) {
        if slots == 0 && bytes == 0 {
            return;
        }
        let waker = freed(&mut self.hot.lock(), slots, bytes);
        wake_all(waker);
    }

    /// Releases consumed capacity and returns any unconsumed suffix to the queue front.
    pub(crate) fn finish_batch(&self, slots: usize, bytes: usize, leftover: &mut VecDeque<Entry<M>>) {
        if leftover.is_empty() && slots == 0 {
            return;
        }
        let waker = {
            let mut hot = self.hot.lock();
            while let Some(e) = leftover.pop_back() {
                hot.queue.push_front(e);
            }
            self.queued.store(hot.queue.len(), Ordering::Relaxed);
            freed(&mut hot, slots, bytes)
        };
        wake_all(waker);
    }

    pub(crate) fn open(&self) {
        {
            let _hot = self.hot.lock();
            let (from, to) = (Phase::Starting as u8, Phase::Running as u8);
            let _ = self.phase.compare_exchange(from, to, Ordering::SeqCst, Ordering::SeqCst);
        }
        self.opened.store(true, Ordering::Release);
        self.signal.touch();
    }

    pub(crate) fn opened(&self) -> bool {
        self.opened.load(Ordering::Acquire)
    }

    /// Moves to `to` unless already further along, waking every admission waiter.
    pub(crate) fn close(&self, to: Phase) {
        let waiters = {
            let mut hot = self.hot.lock();
            if (self.phase.load(Ordering::SeqCst)) < to as u8 {
                self.phase.store(to as u8, Ordering::SeqCst);
            }
            std::mem::take(&mut hot.waiters)
        };
        wake_all(waiters.into_iter().map(|w| w.waker));
    }

    pub(crate) fn discard(&self) -> VecDeque<Entry<M>> {
        let mut hot = self.hot.lock();
        self.queued.store(0, Ordering::Relaxed);
        std::mem::take(&mut hot.queue)
    }

    pub(crate) fn stop_reason(&self) -> Option<StopReason> {
        self.control.lock().stop_reason.clone()
    }

    pub(crate) fn drain_deadline(&self) -> Option<Duration> {
        self.control.lock().drain_deadline.flatten()
    }

    pub(crate) fn finish(&self, exit: Exit) {
        self.close(Phase::Stopped);
        self.signal.publish(exit);
    }

    /// `diagnostics().mailbox_depth` alone, for a driver's look for what is queued.
    pub(crate) fn depth(&self) -> usize {
        self.hot.lock().queue.len()
    }

    /// `depth()` without the lock, as of the last change another thread made visible.
    pub(crate) fn queued(&self) -> usize {
        self.queued.load(Ordering::Relaxed)
    }

    pub(crate) fn diagnostics(&self) -> Diagnostics {
        let (mailbox_depth, mailbox_bytes, waiters, admitted, oldest) = {
            let hot = self.hot.lock();
            // One entry in `sample_every` is stamped, so the walk under the lock is bounded by it; unsampled, by zero.
            let oldest = hot.queue.iter().take(self.sample_every as usize).find_map(|e| e.at);
            (hot.queue.len(), hot.bytes, hot.waiters.len(), hot.admitted, oldest)
        };
        Diagnostics {
            mailbox_depth,
            mailbox_bytes,
            reservations: self.reservations.load(Ordering::Relaxed),
            waiters,
            requests: self.requests.load(Ordering::Relaxed),
            jobs: self.jobs_live.load(Ordering::Relaxed),
            timers: self.stats.timers.load(Ordering::Relaxed),
            admitted,
            rejected: std::array::from_fn(|i| self.rejected[i].load(Ordering::Relaxed)),
            processed: self.stats.processed.load(Ordering::Relaxed),
            turns: self.stats.turns.load(Ordering::Relaxed),
            budget_overruns: self.stats.budget_overruns.load(Ordering::Relaxed),
            asks: self.stats.asks.load(Ordering::Relaxed),
            ask_timeouts: self.stats.ask_timeouts.load(Ordering::Relaxed),
            // A live actor has lost nothing yet: what it holds becomes dead letters when it ends.
            dead_letters: 0,
            oldest_message_age: oldest.map(|at| self.clock.now().saturating_sub(at)),
            timings: self.timings.as_ref().map(|t| Box::new(t.lock().clone())),
        }
    }
}

/// Gives capacity back; the front waiter's waker, to wake once `hot` is released, when any was freed.
fn freed<M>(hot: &mut Hot<M>, slots: usize, bytes: usize) -> Option<Waker> {
    if slots == 0 && bytes == 0 {
        return None;
    }
    hot.slots = hot.slots.saturating_sub(slots);
    hot.bytes = hot.bytes.saturating_sub(bytes);
    take_front(hot)
}

// The woken waiter re-registers its waker when polled.
fn take_front<M>(hot: &mut Hot<M>) -> Option<Waker> {
    hot.waiters.front_mut().map(|w| std::mem::replace(&mut w.waker, Waker::noop().clone()))
}

impl<M: Send + 'static> Lifecycle for Mailbox<M> {
    fn incarnation(&self) -> ActorIncarnation {
        self.incarnation
    }

    fn phase(&self) -> Phase {
        Mailbox::phase(self)
    }

    fn request_stop(&self, reason: StopReason) {
        self.control.lock().stop_reason.get_or_insert(past_fence(self.fence.as_ref(), reason));
        self.stop_requested.store(true, Ordering::SeqCst);
        self.close(Phase::Stopping);
        self.notify();
    }

    fn request_drain(&self, deadline: Option<Duration>) {
        {
            let mut control = self.control.lock();
            control.drain_deadline = Some(match (control.drain_deadline.flatten(), deadline) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            });
        }
        self.close(Phase::Draining);
        self.drain_requested.store(true, Ordering::SeqCst);
        self.notify();
    }

    fn signal(&self) -> &ExitSignal {
        &self.signal
    }

    fn clock(&self) -> &Clock {
        &self.clock
    }
}

/// Address of exactly one actor incarnation. Cloning is cheap; dropping never stops the actor.
pub struct ActorRef<M>(pub(crate) Arc<Mailbox<M>>);

impl<M> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M> std::fmt::Debug for ActorRef<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ActorRef({})", self.0.incarnation)
    }
}

impl<M: Send + 'static> ActorRef<M> {
    pub fn incarnation(&self) -> ActorIncarnation {
        self.0.incarnation
    }

    pub fn phase(&self) -> Phase {
        self.0.phase()
    }

    pub fn clock(&self) -> &Clock {
        &self.0.clock
    }

    pub fn lifecycle(&self) -> LifecycleHandle {
        LifecycleHandle(self.0.clone())
    }

    pub fn diagnostics(&self) -> Diagnostics {
        self.0.diagnostics()
    }

    /// Immediate admission. Never jumps a queued waiter.
    pub fn try_tell(&self, msg: M) -> Result<Enqueued, Reject<M>> {
        self.0.try_tell(msg)
    }

    /// Waits (FIFO) for one slot and `bytes` of capacity. `deadline` is absolute on the actor's clock.
    pub fn reserve(&self, bytes: usize, deadline: Option<Duration>) -> Reserve<M> {
        Reserve(Admission::new(self.0.clone(), bytes, deadline))
    }

    /// Admitted at its first poll as `try_tell` would be, else waits as `reserve` does and commits. Nothing is admitted
    /// before it is polled; a dropped pending send drops its message.
    pub fn send(&self, msg: M, deadline: Option<Duration>) -> SendMessage<M> {
        let bytes = self.0.weigh(&msg);
        SendMessage { admission: Admission::new(self.0.clone(), bytes, deadline), msg: Some(msg) }
    }

    /// A send from an attached source, which `MailboxLimits::sources` bounds instead of `waiters`.
    pub(crate) fn send_from_source(&self, msg: M) -> SendMessage<M> {
        let mut send = self.send(msg, None);
        send.admission.waiter_limit = false;
        send
    }
}

struct Admission<M: Send + 'static> {
    mailbox: Arc<Mailbox<M>>,
    bytes: usize,
    deadline: Option<Duration>,
    waiter: Option<u64>,
    wake: DeadlineWake,
    waiter_limit: bool,
}

impl<M: Send + 'static> Admission<M> {
    fn new(mailbox: Arc<Mailbox<M>>, bytes: usize, deadline: Option<Duration>) -> Self {
        Self { mailbox, bytes, deadline, waiter: None, wake: DeadlineWake::default(), waiter_limit: true }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), RejectReason>> {
        let mb = self.mailbox.clone();
        let expired = self.deadline.is_some_and(|d| mb.clock.now() >= d);
        let mut waker = Some(cx.waker().clone());
        let mut next = None;
        let mut hot = mb.hot.lock();
        let outcome = 'done: {
            if self.bytes > mb.limits.max_message_bytes {
                break 'done Some(Err(RejectReason::TooLarge));
            }
            if let Err(reason) = mb.admission_phase() {
                break 'done Some(Err(reason));
            }
            match self.waiter {
                None => {
                    if hot.waiters.is_empty() && mb.try_take(&mut hot, self.bytes) {
                        break 'done Some(Ok(()));
                    }
                    if expired {
                        break 'done Some(Err(RejectReason::Deadline));
                    }
                    if self.waiter_limit && hot.waiters.len() >= mb.limits.waiters {
                        break 'done Some(Err(RejectReason::WaiterLimit));
                    }
                    let id = hot.next_waiter;
                    hot.next_waiter += 1;
                    hot.waiters.push_back(Waiter { id, waker: waker.take().expect("handed over once per poll") });
                    self.waiter = Some(id);
                }
                Some(id) => {
                    if let Some(w) = hot.waiters.iter_mut().find(|w| w.id == id) {
                        waker = Some(std::mem::replace(&mut w.waker, waker.take().expect("handed over once per poll")));
                    }
                }
            }
            let front = hot.waiters.front().is_some_and(|w| Some(w.id) == self.waiter);
            if front && mb.try_take(&mut hot, self.bytes) {
                hot.waiters.pop_front();
                self.waiter = None;
                next = take_front(&mut hot);
                break 'done Some(Ok(()));
            }
            expired.then_some(Err(RejectReason::Deadline))
        };
        drop(hot);
        drop(waker);
        wake_all(next);
        match outcome {
            Some(Ok(())) => {
                mb.reservations.fetch_add(1, Ordering::Relaxed);
                self.wake.disarm(&mb.clock);
                Poll::Ready(Ok(()))
            }
            Some(Err(reason)) => {
                self.cancel();
                mb.count(reason);
                Poll::Ready(Err(reason))
            }
            None => {
                self.wake.arm(&mb.clock, self.deadline, cx.waker());
                Poll::Pending
            }
        }
    }

    fn cancel(&mut self) {
        self.wake.disarm(&self.mailbox.clock);
        let Some(id) = self.waiter.take() else { return };
        let (removed, next) = {
            let mut hot = self.mailbox.hot.lock();
            let was_front = hot.waiters.front().is_some_and(|w| w.id == id);
            let removed = hot.waiters.iter().position(|w| w.id == id).and_then(|i| hot.waiters.remove(i));
            (removed, if was_front { take_front(&mut hot) } else { None })
        };
        drop(removed);
        wake_all(next);
    }
}

impl<M: Send + 'static> Drop for Admission<M> {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct Reserve<M: Send + 'static>(Admission<M>);

impl<M: Send + 'static> Future for Reserve<M> {
    type Output = Result<Permit<M>, RejectReason>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut self.get_mut().0;
        this.poll(cx).map_ok(|()| Permit { mailbox: this.mailbox.clone(), bytes: this.bytes, armed: true })
    }
}

/// One reserved slot plus bytes. Dropping it uncommitted releases the capacity.
#[must_use]
pub struct Permit<M: Send + 'static> {
    mailbox: Arc<Mailbox<M>>,
    bytes: usize,
    armed: bool,
}

impl<M: Send + 'static> Permit<M> {
    pub fn commit(self, msg: M) -> Result<Enqueued, Reject<M>> {
        let bytes = self.mailbox.weigh(&msg);
        self.commit_weighed(msg, bytes)
    }

    fn commit_weighed(mut self, msg: M, bytes: usize) -> Result<Enqueued, Reject<M>> {
        let mb = self.mailbox.clone();
        if bytes > self.bytes {
            return Err(mb.reject(RejectReason::TooLarge, msg));
        }
        let ambient = TraceContext::current();
        let mut hot = mb.hot.lock();
        if let Err(reason) = mb.admission_phase() {
            drop(hot);
            return Err(mb.reject(reason, msg));
        }
        let was_empty = mb.enqueue(&mut hot, msg, bytes, ambient);
        let waker = freed(&mut hot, 0, self.bytes - bytes);
        drop(hot);
        self.armed = false;
        mb.reservations.fetch_sub(1, Ordering::Relaxed);
        if was_empty {
            mb.notify();
        }
        wake_all(waker);
        Ok(Enqueued)
    }
}

impl<M: Send + 'static> Drop for Permit<M> {
    fn drop(&mut self) {
        if self.armed {
            self.mailbox.reservations.fetch_sub(1, Ordering::Relaxed);
            self.mailbox.release(1, self.bytes);
        }
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct SendMessage<M: Send + 'static> {
    admission: Admission<M>,
    msg: Option<M>,
}

impl<M: Send + 'static> Unpin for SendMessage<M> {}

impl<M: Send + 'static> Future for SendMessage<M> {
    type Output = Result<Enqueued, Reject<M>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let admission = &this.admission;
        // Not yet waiting: one locked section admits it when there is room. Any refusal goes on to `Admission::poll`,
        // which looks again and counts it.
        if admission.waiter.is_none() && admission.bytes <= admission.mailbox.limits.max_message_bytes {
            let msg = this.msg.take().expect("SendMessage polled after completion");
            match admission.mailbox.admit(msg, admission.bytes) {
                Ok(enqueued) => return Poll::Ready(Ok(enqueued)),
                Err(reject) => this.msg = Some(reject.message),
            }
        }
        let outcome = std::task::ready!(this.admission.poll(cx));
        let msg = this.msg.take().expect("SendMessage polled after completion");
        Poll::Ready(match outcome {
            Ok(()) => {
                let bytes = this.admission.bytes;
                Permit { mailbox: this.admission.mailbox.clone(), bytes, armed: true }.commit_weighed(msg, bytes)
            }
            Err(reason) => Err(Reject { reason, message: msg }),
        })
    }
}

// Every byte here is a megabyte of a million idle actors, and spawning them is mostly page faults.
#[cfg(not(axon_loom))]
const _: () = assert!(size_of::<Mailbox<[u64; 8]>>() <= 552);

#[cfg(all(test, not(axon_loom)))]
fn poll_once<F: Future + Unpin>(f: &mut F) -> Poll<F::Output> {
    Pin::new(f).poll(&mut Context::from_waker(Waker::noop()))
}

#[cfg(all(test, not(axon_loom)))]
#[test]
fn the_first_message_reserves_the_first_block_however_large_the_message_and_however_sent() {
    let incarnation = ActorIncarnation { runtime: 0, slot: 0, generation: 0 };
    let address = || {
        let mailbox = Mailbox::new(MailboxConfig::<[u8; 2048]>::default(), Clock::manual().clock(), incarnation, None);
        mailbox.open();
        ActorRef(mailbox)
    };
    let capacity = |a: &ActorRef<[u8; 2048]>| a.0.hot.lock().queue.capacity();
    let told = address();
    told.try_tell([0; 2048]).expect("room");
    assert_eq!(capacity(&told), 4);
    let sent = address();
    assert!(matches!(poll_once(&mut sent.send([0; 2048], None)), Poll::Ready(Ok(_))));
    assert_eq!(capacity(&sent), 4);
    let committed = address();
    let Poll::Ready(Ok(permit)) = poll_once(&mut committed.reserve(2048, None)) else { panic!("room") };
    permit.commit([0; 2048]).expect("room");
    assert_eq!(capacity(&committed), 4);
}

#[cfg(all(test, not(axon_loom)))]
#[test]
fn a_send_that_waits_weighs_its_message_once() {
    let weighed = Arc::new(AtomicUsize::new(0));
    let counter = weighed.clone();
    let config =
        MailboxConfig::<u64>::from(MailboxLimits { messages: 1, ..MailboxLimits::default() }).with_weight(move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
            8
        });
    let incarnation = ActorIncarnation { runtime: 0, slot: 0, generation: 0 };
    let mailbox = Mailbox::new(config, Clock::manual().clock(), incarnation, None);
    mailbox.open();
    let address = ActorRef(mailbox);
    address.try_tell(1u64).expect("room");
    let mut send = address.send(2u64, None);
    assert!(poll_once(&mut send).is_pending());
    let mut taken = VecDeque::new();
    address.0.pull(1, &mut taken);
    address.0.release(1, taken[0].bytes);
    assert!(matches!(poll_once(&mut send), Poll::Ready(Ok(_))));
    assert_eq!(weighed.load(Ordering::Relaxed), 2);
    assert_eq!(address.0.queued(), address.0.depth());
}

#[cfg(all(test, not(axon_loom)))]
#[test]
fn the_length_a_router_reads_without_the_lock_follows_every_change_to_the_queue() {
    let incarnation = ActorIncarnation { runtime: 0, slot: 0, generation: 0 };
    let mailbox = Mailbox::new(MailboxConfig::<u64>::default(), Clock::manual().clock(), incarnation, None);
    mailbox.open();
    let address = ActorRef(mailbox);
    let mb = &address.0;
    let same = || assert_eq!(mb.queued(), mb.depth());
    for i in 0..5u64 {
        address.try_tell(i).expect("room");
        same();
    }
    let mut batch = VecDeque::new();
    mb.pull(2, &mut batch);
    same();
    mb.finish_batch(1, 0, &mut batch.split_off(1));
    same();
    let mut all = VecDeque::new();
    mb.pull(usize::MAX, &mut all);
    assert_eq!(mb.queued(), 0);
    address.try_tell(9u64).expect("room");
    same();
    mb.discard();
    assert_eq!((mb.queued(), mb.depth()), (0, 0));
}

/// Every interleaving of the admission handshake. A lost wakeup shows as a deadlock.
/// `RUSTFLAGS="--cfg axon_loom" cargo test -p axon-actors --lib --release --target-dir target/loom mailbox::model`
#[cfg(test)] // apart from `axon_loom`, so clippy's allow-unwrap-in-tests sees a test module
#[cfg(axon_loom)]
mod model;
