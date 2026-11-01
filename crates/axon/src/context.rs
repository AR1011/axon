use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use futures_core::Stream;
use futures_sink::Sink;

use crate::driver::MailboxBatch;
use crate::jobs::{Cancel, CatchPanic, Job, JobExecutor, JobHandle, Panic};
use crate::lifecycle::{ExitWatcher, Lifecycle, LifecycleHandle, LinkHandle};
use crate::mailbox::{ActorRef, JobKind, LiveJob, Mailbox, SendMessage};
use crate::sink::{Drainable, Pump, Shared, SinkEvent, SinkHandle};
use crate::sync::Mutex;
use crate::trace::{self, Scoped, TraceContext};
use crate::types::*;

pub trait Actor: Sized + 'static {
    type Message: Send + 'static;
    type Error: Debug + Send + 'static;

    fn on_start(&mut self, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn on_message(&mut self, msg: Self::Message, cx: &mut Context<'_, Self>) -> Result<(), Self::Error>;

    fn on_batch(
        &mut self,
        batch: &mut MailboxBatch<'_, Self::Message>,
        cx: &mut Context<'_, Self>,
    ) -> Result<(), Self::Error> {
        for msg in batch.by_ref() {
            self.on_message(msg, cx)?;
        }
        Ok(())
    }

    /// Once after every turn that handled a message, a job result or a timer: the place to flush what the turn
    /// queued. Not after a turn that failed or was stopped.
    fn on_turn_end(&mut self, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn on_stop(&mut self, _reason: &StopReason) {}

    /// Groups sampled handler timings by message kind. Empty means ungrouped.
    fn label(_msg: &Self::Message) -> &'static str {
        ""
    }
}

pub(crate) enum TimerKind<M> {
    Once(M, Option<TraceContext>),
    Every(Duration, Box<dyn FnMut() -> M + Send>),
}

pub(crate) struct Core<M: Send + 'static> {
    pub(crate) mailbox: Arc<Mailbox<M>>,
    pub(crate) timers: BTreeMap<(Duration, u64), TimerKind<M>>,
    pub(crate) timer_seq: u64,
    pub(crate) children: Vec<LifecycleHandle>,
    pub(crate) jobs: Option<JobExecutor>,
    pub(crate) job_seq: u64,
    /// Allocated by the first stash and kept until the actor ends: most actors never stash.
    pub(crate) stash: Option<Box<Stash<M>>>,
}

pub(crate) struct Stash<M> {
    /// Each with its weight, as the mailbox weighed it.
    pub(crate) held: VecDeque<(M, Option<TraceContext>, usize)>,
    pub(crate) bytes: usize,
    /// How many at the front `unstash_all` released to run before the mailbox.
    pub(crate) released: usize,
}

/// What a handler may do: its own address, time, timers, jobs, children, stop/drain.
pub struct Context<'a, A: Actor> {
    pub(crate) core: &'a mut Core<A::Message>,
    pub(crate) _actor: PhantomData<A>,
}

impl<A: Actor> Context<'_, A> {
    pub fn incarnation(&self) -> ActorIncarnation {
        self.core.mailbox.incarnation()
    }

    /// Time on the system's clock, which timers are set on: since the clock was made, or wherever a `Clock::manual()`
    /// was advanced to.
    pub fn now(&self) -> Duration {
        self.core.mailbox.clock.now()
    }

    pub fn address(&self) -> ActorRef<A::Message> {
        ActorRef(self.core.mailbox.clone())
    }

    pub fn stop(&self) {
        self.core.mailbox.request_stop(StopReason::Stopped);
    }

    pub fn drain(&self, deadline: Option<Duration>) {
        self.core.mailbox.request_drain(deadline);
    }

    /// Keeps `msg`, outside the mailbox, until [`unstash_all`](Self::unstash_all). Holds as many messages and bytes as
    /// the mailbox may (`MailboxLimits::messages` and `bytes`), and hands the next one back. What is still stashed when
    /// the actor ends is discarded and counted with what was queued; a drain does not wait for it.
    pub fn stash(&mut self, msg: A::Message) -> Result<(), A::Message> {
        let mb = &self.core.mailbox;
        let bytes = mb.weigh(&msg);
        let stash =
            self.core.stash.get_or_insert_with(|| Box::new(Stash { held: VecDeque::new(), bytes: 0, released: 0 }));
        if stash.held.len() >= mb.limits.messages || stash.bytes.saturating_add(bytes) > mb.limits.bytes {
            return Err(msg);
        }
        stash.held.push_back((msg, TraceContext::current(), bytes));
        stash.bytes += bytes;
        Ok(())
    }

    /// Hands every stashed message to the actor again, in the order stashed, before anything still in the mailbox. One
    /// stashed again as they replay goes to the back and waits for the next `unstash_all`.
    pub fn unstash_all(&mut self) {
        if let Some(stash) = self.core.stash.as_deref_mut().filter(|s| !s.held.is_empty()) {
            stash.released = stash.held.len();
            self.core.mailbox.unstashed.store(true, Ordering::Relaxed);
        }
    }

    /// The child is drained with a drained parent and stopped otherwise.
    pub fn adopt_child(&mut self, child: LifecycleHandle) -> Result<(), LifecycleHandle> {
        self.core.children.retain(|c| c.exit().is_none());
        if self.core.children.len() >= self.core.mailbox.limits.children {
            return Err(child);
        }
        self.core.children.push(child);
        Ok(())
    }

    /// Sends `msg` once [`now`](Self::now) reaches `at`; an `at` already past is due at once.
    pub fn schedule_at(&mut self, at: Duration, msg: A::Message) -> Result<TimerId, TimerError<A::Message>> {
        match self.timer_admission() {
            Err(reason) => Err(TimerError { reason, message: Some(msg) }),
            Ok(()) => Ok(self.insert_timer(at, TimerKind::Once(msg, TraceContext::current()))),
        }
    }

    pub fn schedule_after(&mut self, after: Duration, msg: A::Message) -> Result<TimerId, TimerError<A::Message>> {
        self.schedule_at(self.now().saturating_add(after), msg)
    }

    /// Repeats every `period` until cancelled. Missed periods are skipped, not replayed.
    pub fn schedule_every(
        &mut self,
        period: Duration,
        make: impl FnMut() -> A::Message + Send + 'static,
    ) -> Result<TimerId, TimerError<A::Message>> {
        self.timer_admission().map_err(|reason| TimerError { reason, message: None })?;
        let at = self.now().saturating_add(period);
        Ok(self.insert_timer(at, TimerKind::Every(period.max(Duration::from_nanos(1)), Box::new(make))))
    }

    pub fn cancel_timer(&mut self, id: TimerId) -> TimerCancel {
        if id.incarnation != self.incarnation() {
            return TimerCancel::StaleIncarnation;
        }
        let key = self.core.timers.keys().find(|k| k.1 == id.seq).copied();
        match key.and_then(|k| self.core.timers.remove(&k)) {
            Some(_) => {
                self.core.publish_timer_count();
                TimerCancel::Cancelled
            }
            None => TimerCancel::AlreadyDispatched,
        }
    }

    /// Runs `work` on the job executor; its result arrives as an ordinary `on_message` call, in the
    /// trace of the message that submitted it. A drain waits for it. A blocking job cannot be
    /// interrupted: cancelling it suppresses its result. A refusal hands `work` back unrun.
    pub fn submit_io<W>(&mut self, work: W) -> Result<JobHandle, SubmitError<W>>
    where
        W: FnOnce() -> A::Message + Send + 'static,
    {
        self.io(None, work)
    }

    /// Like `submit_io`, on `executor`: under the same `MailboxLimits::jobs`, cancel, pause and drain.
    pub fn submit_io_on<W>(&mut self, executor: &JobExecutor, work: W) -> Result<JobHandle, SubmitError<W>>
    where
        W: FnOnce() -> A::Message + Send + 'static,
    {
        self.io(Some(executor), work)
    }

    /// Like `submit_io` for a future. Cancelling drops the future at its next poll.
    pub fn submit_future<W>(&mut self, work: W) -> Result<JobHandle, SubmitError<W>>
    where
        W: Future<Output = A::Message> + Send + 'static,
    {
        self.future(None, work)
    }

    /// Like `submit_future`, on `executor`: under the same `MailboxLimits::jobs`, cancel, pause and drain.
    pub fn submit_future_on<W>(&mut self, executor: &JobExecutor, work: W) -> Result<JobHandle, SubmitError<W>>
    where
        W: Future<Output = A::Message> + Send + 'static,
    {
        self.future(Some(executor), work)
    }

    fn io<W>(&mut self, executor: Option<&JobExecutor>, work: W) -> Result<JobHandle, SubmitError<W>>
    where
        W: FnOnce() -> A::Message + Send + 'static,
    {
        self.core.submit(JobKind::Job, executor, work, |work, trace, _, settler| {
            Job::Blocking(Box::new(move || {
                let work = take_work(work);
                settler.finish(match catch_unwind(AssertUnwindSafe(|| trace::enter(trace, work))) {
                    Ok(msg) => Settle::Done(msg),
                    Err(_) => Settle::Panicked,
                })
            }))
        })
    }

    fn future<W>(&mut self, executor: Option<&JobExecutor>, work: W) -> Result<JobHandle, SubmitError<W>>
    where
        W: Future<Output = A::Message> + Send + 'static,
    {
        self.core.submit(JobKind::Job, executor, work, |work, trace, cancel, settler| {
            Job::Async(Box::pin(async move {
                let work = CatchPanic::boxed(Box::pin(Scoped::new(trace, take_work(work))));
                settler.finish(Abortable { cancel, inner: Some(work) }.await)
            }))
        })
    }

    /// Until `job`'s result is handled, or it ends with none, the actor handles only that result, its timers, a stop
    /// and a drain's deadline. Messages, the stash's replay, other jobs' results, sinks' events and watch downs wait,
    /// then come in the order they arrived: held results, downs, the replay, the mailbox, unless a handler among them
    /// pauses again. Pausing on several jobs waits for all of them, handling each result as it comes. Pausing on a job
    /// whose result is still held handles it next; on one whose result was handled, or that left none, does nothing.
    /// A pause `on_start` takes is part of the start: the actor opens once it lifts.
    pub fn pause_until(&mut self, job: &JobHandle) -> Result<(), PauseError> {
        let mb = &self.core.mailbox;
        let held = {
            let mut jobs = mb.jobs.lock();
            let jobs = jobs.get_or_insert_default();
            let operation = job.operation;
            let held = match jobs.live.get(&operation) {
                Some(live) if !Arc::ptr_eq(&live.cancel, &job.cancel) => return Ok(()),
                Some(live) if !matches!(live.kind, JobKind::Job) => return Err(PauseError::Source),
                Some(_) => false,
                None if jobs.done.iter().any(|(o, ..)| *o == operation) => true,
                None => return Ok(()),
            };
            if !jobs.pausing.contains(&operation) {
                jobs.pausing.push(operation);
            }
            if held {
                // Taken by the next turn, which nothing else may wake.
                mb.flag_settled();
            }
            held
        };
        mb.paused.store(true, Ordering::Relaxed);
        if held {
            mb.notify();
        }
        Ok(())
    }

    /// Owns `source` for the rest of the actor's life: every item arrives as the message `map` makes of it,
    /// `map(None)` follows the last one through the mailbox, and a stop or a drain drops the source. A cancel takes back
    /// nothing already sent, the end included. Delivery waits for mailbox room, so a full mailbox pauses the source.
    /// Items start no trace of their own. An item over `max_message_bytes` fails the actor.
    pub fn attach<S>(
        &mut self,
        source: S,
        map: impl FnMut(Option<S::Item>) -> A::Message + Send + 'static,
    ) -> Result<JobHandle, TaskAdmissionError>
    where
        S: Stream + Send + 'static,
        S::Item: Send,
    {
        let address = self.address();
        Ok(self.core.submit(JobKind::Source, None, (), |_, _, cancel, settler| {
            let map: Box<dyn FnMut(Option<S::Item>) -> A::Message + Send> = Box::new(map);
            let pump = CatchPanic::boxed(Box::pin(Attached { source: Box::pin(source), map, address, pending: None }));
            Job::Async(Box::pin(async move {
                settler.finish(match (Abortable { cancel, inner: Some(pump) }).await {
                    Settle::Done(Err(reason)) => Settle::Failed(format!("attached source: item refused ({reason:?})")),
                    Settle::Panicked => Settle::Panicked,
                    _ => Settle::Finished,
                })
            }))
        })?)
    }

    /// Owns `sink` for the rest of the actor's life. `try_send` on the handle queues up to `capacity` items and
    /// never waits; a job feeds them to the sink and flushes whenever the queue runs dry. A refused `try_send` is
    /// answered by `map(SinkEvent::Ready)` once the queue is half empty, and `map(SinkEvent::Closed(..))` ends the
    /// sink. A drain closes the sink once the actor has handled everything and owes it nothing; a stop drops it.
    /// Events arrive like job results, during a drain too, and start no trace.
    pub fn attach_sink<S, I>(
        &mut self,
        sink: S,
        capacity: usize,
        map: impl FnMut(SinkEvent<S::Error>) -> A::Message + Send + 'static,
    ) -> Result<SinkHandle<I>, TaskAdmissionError>
    where
        S: Sink<I> + Send + 'static,
        S::Error: Send,
        I: Send + 'static,
    {
        let shared = Shared::new(capacity.max(1));
        let mailbox = self.core.mailbox.clone();
        let job = self.core.submit(JobKind::Sink(shared.clone()), None, (), |_, _, cancel, settler| {
            let (stopped, owed) = (cancel.clone(), shared.clone());
            let pump = Pump {
                sink: Box::pin(sink),
                shared: shared.clone(),
                map: Box::new(map),
                deliver: Box::new(move |msg| deliver(&mailbox, &stopped, &*owed, msg)),
                held: None,
                unflushed: false,
            };
            let pump = CatchPanic::boxed(Box::pin(pump));
            Job::Async(Box::pin(async move { settler.finish(Abortable { cancel, inner: Some(pump) }.await) }))
        })?;
        Ok(SinkHandle { shared, job })
    }

    /// Tells this actor when the one `lifecycle` is of ends: `map(exit)` arrives once, like a job result, so a full
    /// mailbox does not refuse it, and starts no trace. A supervised family is told of when it ends, not of a restart,
    /// and one that has ended already at once. By then the system has freed its name, though `lifecycle.exit()` may
    /// still be `None`: the `Exit` is in the message. A watch counts against `MailboxLimits::sources` until its message
    /// is handled or it is cancelled, and ends with this actor: nothing arrives once it is stopping.
    pub fn watch(
        &mut self,
        lifecycle: &LifecycleHandle,
        map: impl FnOnce(Exit) -> A::Message + Send + 'static,
    ) -> Result<WatchHandle, TaskAdmissionError> {
        self.core.admission()?;
        let mb = &self.core.mailbox;
        let watch = Arc::new(Watch { watcher: Arc::downgrade(mb), map: Mutex::new(Some(Box::new(map))) });
        {
            let mut jobs = mb.jobs.lock();
            let jobs = jobs.get_or_insert_default();
            if jobs.held_sources() >= mb.limits.sources {
                return Err(TaskAdmissionError::Full);
            }
            jobs.watches.push((lifecycle.clone(), watch.clone()));
        }
        lifecycle.watch(Arc::downgrade(&watch) as Weak<dyn ExitWatcher>);
        Ok(WatchHandle(Arc::downgrade(&watch) as Weak<dyn Unwatch>))
    }

    /// Links this actor and the one `lifecycle` is of, both ways: when either ends by a failure
    /// ([`StopReason::is_failure`]), the other is stopped with [`StopReason::Linked`]; a stop or drain ends the link and
    /// nothing else. One already failed stops this one at once. Linking a pair again, from either end, keeps the one
    /// link. It is this incarnation's: a supervised actor restarted after a failure starts unlinked.
    pub fn link(&mut self, lifecycle: &LifecycleHandle) -> LinkHandle {
        self.address().lifecycle().link(lifecycle)
    }

    /// Takes back the link between this actor and the one `lifecycle` is of, if there is one.
    pub fn unlink(&mut self, lifecycle: &LifecycleHandle) {
        self.address().lifecycle().unlink(lifecycle);
    }

    fn timer_admission(&self) -> Result<(), TimerReject> {
        self.core.admission().map_err(|e| match e {
            TaskAdmissionError::Draining => TimerReject::Draining,
            _ => TimerReject::Closed,
        })?;
        if self.core.timers.len() >= self.core.mailbox.limits.timers {
            return Err(TimerReject::Full);
        }
        Ok(())
    }

    fn insert_timer(&mut self, at: Duration, kind: TimerKind<A::Message>) -> TimerId {
        let seq = self.core.timer_seq;
        self.core.timer_seq += 1;
        self.core.timers.insert((at, seq), kind);
        self.core.publish_timer_count();
        TimerId { incarnation: self.incarnation(), seq }
    }
}

/// How a job ended. `Finished` left nothing to deliver: a cancelled future, or a source that ran dry.
pub(crate) enum Settle<M> {
    Done(M),
    Finished,
    Panicked,
    Failed(String),
}

/// Drops `inner` at the first poll after a cancel, and at completion: the job is over only once
/// what it owned is gone.
struct Abortable<F> {
    cancel: Arc<Cancel>,
    inner: Option<F>,
}

impl<T, F: Future<Output = Result<T, Panic>> + Unpin> Future for Abortable<F> {
    type Output = Settle<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let Some(inner) = this.inner.as_mut() else { return Poll::Ready(Settle::Finished) };
        let out = if this.cancel.poll(cx).is_ready() {
            Settle::Finished
        } else {
            match Pin::new(inner).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(value)) => Settle::Done(value),
                Poll::Ready(Err(_)) => Settle::Panicked,
            }
        };
        this.cancel.clear();
        let inner = this.inner.take();
        Poll::Ready(match catch_unwind(AssertUnwindSafe(|| drop(inner))) {
            Ok(()) => out,
            Err(_) => Settle::Panicked,
        })
    }
}

/// A source feeding a mailbox: one item in flight at a time, each waiting for room.
struct Attached<S: Stream, M: Send + 'static> {
    source: Pin<Box<S>>,
    map: Box<dyn FnMut(Option<S::Item>) -> M + Send>,
    address: ActorRef<M>,
    /// The send in flight and whether it carries the end message.
    pending: Option<(SendMessage<M>, bool)>,
}

impl<S: Stream, M: Send + 'static> Future for Attached<S, M> {
    type Output = Result<(), RejectReason>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        loop {
            if let Some((send, last)) = this.pending.as_mut() {
                let last = *last;
                match std::task::ready!(Pin::new(send).poll(cx)) {
                    Ok(_) if !last => {}
                    // The mailbox is closing: the actor will not read anything more.
                    Ok(_) | Err(Reject { reason: RejectReason::Closed, .. }) => return Poll::Ready(Ok(())),
                    Err(reject) => return Poll::Ready(Err(reject.reason)),
                }
                this.pending = None;
            }
            let item = std::task::ready!(this.source.as_mut().poll_next(cx));
            let last = item.is_none();
            this.pending = Some((this.address.send_from_source((this.map)(item)), last));
        }
    }
}

impl<M: Send + 'static> Core<M> {
    pub(crate) fn released(&self) -> bool {
        self.stash.as_ref().is_some_and(|s| s.released > 0)
    }

    pub(crate) fn timer_due(&self) -> bool {
        self.timers.first_key_value().is_some_and(|(k, _)| k.0 <= self.mailbox.clock.now())
    }

    pub(crate) fn publish_timer_count(&self) {
        self.mailbox.stats.timers.store(self.timers.len(), Ordering::Relaxed);
    }

    fn admission(&self) -> Result<(), TaskAdmissionError> {
        match self.mailbox.phase() {
            Phase::Starting | Phase::Running => Ok(()),
            // A drain asked for before on_start ran begins after it, so the actor's own setup is still admitted.
            Phase::Draining if !self.mailbox.opened() => Ok(()),
            Phase::Draining => Err(TaskAdmissionError::Draining),
            _ => Err(TaskAdmissionError::Closed),
        }
    }

    /// Counts a job in and hands it to `executor`, or the actor's. `build` gets the work in a slot the job takes it
    /// from as it starts, the trace it runs in and what settles it; a refusal hands the work back unrun.
    fn submit<W>(
        &mut self,
        kind: JobKind,
        executor: Option<&JobExecutor>,
        work: W,
        build: impl FnOnce(Slot<W>, Option<TraceContext>, Arc<Cancel>, Settler<M>) -> Job,
    ) -> Result<JobHandle, SubmitError<W>> {
        if let Err(reason) = self.admission() {
            return Err(SubmitError { reason, work });
        }
        let Some(executor) = executor.or(self.jobs.as_ref()) else {
            return Err(SubmitError { reason: TaskAdmissionError::NoExecutor, work });
        };
        let cancel = Arc::new(Cancel::default());
        let operation = self.job_seq;
        let long = !matches!(kind, JobKind::Job);
        {
            let mut jobs = self.mailbox.jobs.lock();
            let jobs = jobs.get_or_insert_default();
            let full = match long {
                true => jobs.held_sources() >= self.mailbox.limits.sources,
                false => jobs.held_jobs() >= self.mailbox.limits.jobs,
            };
            if full {
                return Err(SubmitError { reason: TaskAdmissionError::Full, work });
            }
            jobs.sources += usize::from(long);
            jobs.live.insert(operation, LiveJob { cancel: cancel.clone(), kind });
        }
        self.job_seq += 1;
        self.mailbox.jobs_live.fetch_add(1, Ordering::Relaxed);
        // Sources and sinks outlive the message that attached them; what they deliver is not its work.
        let trace = if long { None } else { TraceContext::current() };
        let settler =
            Settler { mailbox: self.mailbox.clone(), operation, cancel: cancel.clone(), trace, finished: false };
        let work = Arc::new(Mutex::new(Some(work)));
        if let Err(job) = executor.submit(build(work.clone(), trace, cancel.clone(), settler)) {
            // Refused, it has not run: counted out again, so its settler, dropped with it, finds nothing to settle.
            self.mailbox.jobs.lock().get_or_insert_default().remove(operation);
            self.mailbox.jobs_live.fetch_sub(1, Ordering::Relaxed);
            drop(job);
            return Err(SubmitError { reason: TaskAdmissionError::Full, work: take_work(work) });
        }
        Ok(JobHandle { operation, cancel })
    }
}

/// Settles its job once: by `finish`, or, dropped by an executor that never finished the job, as cancelled when it
/// was, and otherwise as the failure that ends the actor. Nothing waits on a job its executor lost.
pub(crate) struct Settler<M: Send + 'static> {
    mailbox: Arc<Mailbox<M>>,
    operation: u64,
    cancel: Arc<Cancel>,
    trace: Option<TraceContext>,
    finished: bool,
}

impl<M: Send + 'static> Settler<M> {
    fn finish(mut self, result: Settle<M>) {
        self.finished = true;
        settle(&self.mailbox, self.operation, &self.cancel, result, self.trace);
    }
}

impl<M: Send + 'static> Drop for Settler<M> {
    fn drop(&mut self) {
        if !self.finished {
            let result = match self.cancel.is_cancelled() {
                true => Settle::Finished,
                false => Settle::Failed("a job was dropped by its executor".into()),
            };
            settle(&self.mailbox, self.operation, &self.cancel, result, self.trace);
        }
    }
}

/// A job's work until the job starts.
type Slot<W> = Arc<Mutex<Option<W>>>;

fn take_work<W>(work: Slot<W>) -> W {
    let taken = work.lock().take();
    taken.expect("a job starts once, and never after its executor refused it")
}

/// A sink's `Ready`, queued with job results: handled during a drain, counted as discarded by a stop.
/// A stop cancels under the same lock, so nothing lands after it took the results, and the drain check reads the sink
/// under it too, so it sees the `Ready` owed or queued, never neither.
fn deliver<M: Send + 'static>(mailbox: &Mailbox<M>, cancel: &Cancel, sink: &dyn Drainable, msg: M) {
    let mut guard = mailbox.jobs.lock();
    let jobs = guard.get_or_insert_default();
    sink.delivered();
    if cancel.is_cancelled() {
        drop(guard);
        let _ = catch_unwind(AssertUnwindSafe(|| drop(msg)));
        return;
    }
    jobs.done.push_back((0, msg, None));
    mailbox.flag_settled();
    drop(guard);
    mailbox.notify();
}

pub(crate) fn settle<M: Send + 'static>(
    mailbox: &Mailbox<M>,
    operation: u64,
    cancel: &Cancel,
    result: Settle<M>,
    trace: Option<TraceContext>,
) {
    let suppressed = {
        let mut jobs = mailbox.jobs.lock();
        let jobs = jobs.get_or_insert_default();
        // Not live: its executor refused it, and it was counted out then.
        if !jobs.remove(operation) {
            return;
        }
        let (held, suppressed) = match result {
            Settle::Panicked => {
                jobs.failure.get_or_insert(StopReason::Panicked);
                (false, None)
            }
            Settle::Failed(text) => {
                jobs.failure.get_or_insert(StopReason::Failed(text));
                (false, None)
            }
            Settle::Finished => (false, None),
            Settle::Done(msg) if cancel.is_cancelled() => (false, Some(msg)),
            Settle::Done(msg) => {
                jobs.done.push_back((operation, msg, trace));
                (true, None)
            }
        };
        if !held {
            // Ended with nothing to handle: a pause on it lifts at the next turn.
            jobs.pausing.retain(|&o| o != operation);
        }
        mailbox.flag_settled();
        suppressed
    };
    mailbox.jobs_live.fetch_sub(1, Ordering::Relaxed);
    let _ = catch_unwind(AssertUnwindSafe(|| drop(suppressed)));
    mailbox.notify();
}

/// A `cx.watch`, owned by its watcher's job state until it is told, cancelled or its watcher ends.
pub(crate) struct Watch<M> {
    watcher: Weak<Mailbox<M>>,
    /// Taken by whichever comes first: the down being handled, a cancel, the watcher's end.
    pub(crate) map: Mutex<Option<Down<M>>>,
}

type Down<M> = Box<dyn FnOnce(Exit) -> M + Send>;

impl<M: Send + 'static> ExitWatcher for Watch<M> {
    fn exited(&self, exit: &Exit) {
        let Some(mb) = self.watcher.upgrade() else { return };
        let target = {
            let mut jobs = mb.jobs.lock();
            let Some(jobs) = jobs.as_deref_mut() else { return };
            let Some((target, watch)) = jobs.take_watch(self) else { return };
            jobs.fired.push_back((watch, exit.clone()));
            mb.flag_settled();
            target
        };
        drop(target);
        mb.notify();
    }
}

trait Unwatch: Send + Sync {
    fn cancel(&self);
}

impl<M: Send + 'static> Unwatch for Watch<M> {
    fn cancel(&self) {
        let map = self.map.lock().take();
        let registered = self.watcher.upgrade().and_then(|mb| {
            let mut jobs = mb.jobs.lock();
            let jobs = jobs.as_deref_mut()?;
            // Told and not yet handled: its budget is back now.
            jobs.fired.retain(|(w, _)| !std::ptr::eq(&**w, self));
            jobs.take_watch(self)
        });
        if let Some((target, watch)) = registered {
            target.unwatch(&watch);
        }
        drop(map);
    }
}

/// A watch `cx.watch` made. Dropping it does nothing.
#[derive(Clone, Debug)]
pub struct WatchHandle(Weak<dyn Unwatch>);

impl WatchHandle {
    /// Takes the watch back. Called by the watcher, nothing arrives after this, even if the watched actor has ended
    /// meanwhile; called from another thread, a message the watcher is handling at that moment still arrives.
    pub fn cancel(&self) {
        if let Some(watch) = self.0.upgrade() {
            watch.cancel();
        }
    }
}
