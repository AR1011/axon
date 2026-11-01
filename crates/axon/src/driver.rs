use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

use crate::clock::{Clock, WakeId};
use crate::context::{Actor, Context, Core, TimerKind};
use crate::histogram::sampled;
use crate::jobs::{JobExecutor, ManualJobs};
use crate::lifecycle::{Lifecycle, LifecycleHandle};
use crate::mailbox::{ActorRef, DrainEnd, Entry, JobKind, JobState, Mailbox, MailboxConfig};
use crate::runtime::Drive;
use crate::trace::{self, Span, SpanKind, TraceContext};
use crate::types::*;

/// A traced message being handled: the thread's current context until it is finished.
struct OpenSpan {
    span: Span,
    began: Duration,
    outer: Option<TraceContext>,
}

impl OpenSpan {
    fn begin<M: Send + 'static>(
        mailbox: &Mailbox<M>,
        parent: TraceContext,
        label: &'static str,
        admitted: Option<Duration>,
    ) -> Self {
        let (began, here) = (mailbox.clock.now(), parent.child());
        let outer = trace::replace_current(Some(here));
        let mut span = Span::begin(parent, here.span, SpanKind::Handle, label);
        span.wait = admitted.map_or(Duration::ZERO, |at| began.saturating_sub(at));
        Self { span, began, outer }
    }

    fn finish<M: Send + 'static>(mut self, mailbox: &Mailbox<M>, actor_type: &'static str, failed: bool) {
        trace::replace_current(self.outer);
        if let Some(tracer) = &mailbox.tracer {
            self.span.actor = Some((mailbox.incarnation(), actor_type));
            self.span.busy = mailbox.clock.now().saturating_sub(self.began);
            self.span.failed = failed;
            tracer.record(self.span);
        }
    }
}

/// Owned-message iterator over one turn's share of the mailbox.
pub struct MailboxBatch<'a, M: Send + 'static> {
    entries: &'a mut VecDeque<Entry<M>>,
    mailbox: &'a Mailbox<M>,
    until: Option<Duration>,
    label: fn(&M) -> &'static str,
    actor_type: &'static str,
    open: Option<(&'static str, Duration)>,
    span: Option<OpenSpan>,
    slots: usize,
    bytes: usize,
    overrun: bool,
}

impl<M: Send + 'static> MailboxBatch<'_, M> {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Ends the message handed out last: its timing sample and its span.
    #[inline]
    fn close(&mut self, failed: bool) {
        if self.open.is_some() {
            self.close_sample();
        }
        if self.span.is_some() {
            self.close_span(failed);
        }
    }

    #[cold]
    fn close_sample(&mut self) {
        if let (Some((label, at)), Some(t)) = (self.open.take(), &self.mailbox.timings) {
            t.lock().record_message(label, self.mailbox.clock.now().saturating_sub(at));
        }
    }

    #[cold]
    fn close_span(&mut self, failed: bool) {
        if let Some(span) = self.span.take() {
            span.finish(self.mailbox, self.actor_type, failed);
        }
    }

    #[cold]
    fn open_span(&mut self, parent: TraceContext, at: Option<Duration>, msg: &M) {
        self.span = Some(OpenSpan::begin(self.mailbox, parent, (self.label)(msg), at));
    }

    #[cold]
    fn sample(&mut self, at: Duration, msg: &M) {
        let Some(t) = &self.mailbox.timings else { return };
        let now = self.mailbox.clock.now();
        t.lock().mailbox_wait.record(now.saturating_sub(at));
        let label = (self.label)(msg);
        self.open = (!label.is_empty()).then_some((label, now));
    }
}

impl<M: Send + 'static> Iterator for MailboxBatch<'_, M> {
    type Item = M;

    fn next(&mut self) -> Option<M> {
        self.close(false);
        if self.mailbox.stop_requested.load(Ordering::Relaxed)
            || self.mailbox.paused.load(Ordering::Relaxed)
            || self.mailbox.unstashed.load(Ordering::Relaxed)
            || self.mailbox.fenced()
        {
            return None;
        }
        if self.slots > 0 && self.until.is_some_and(|u| self.mailbox.clock.now() >= u) {
            self.overrun = !self.entries.is_empty();
            return None;
        }
        let entry = self.entries.pop_front()?;
        self.slots += 1;
        self.bytes += entry.bytes;
        if let Some(at) = entry.at {
            self.sample(at, &entry.msg);
        }
        if let Some(parent) = entry.trace {
            self.open_span(parent, entry.at, &entry.msg);
        }
        Some(entry.msg)
    }
}

struct Ending {
    reason: StopReason,
    deadline: Option<Duration>,
    discarded: Vec<Discarded>,
    pending_jobs: usize,
    pending_timers: usize,
}

pub(crate) struct Driver<A: Actor> {
    actor: Option<A>,
    core: Core<A::Message>,
    batch: VecDeque<Entry<A::Message>>,
    wake: Option<WakeId>,
    // Boxed: only an actor that is ending needs it.
    ending: Option<Box<Ending>>,
    /// `on_start` returned `Ok`. The actor opens once it has and no pause holds it.
    started: bool,
    stopped: bool,
    dropped_timers: usize,
    handler_observed: u64,
    /// The idle time, and when the actor last handled a message. Boxed: most actors have none.
    idle: Option<Box<(Duration, Duration)>>,
}

static NEXT_RUNTIME: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_runtime_id() -> u64 {
    NEXT_RUNTIME.fetch_add(1, Ordering::Relaxed)
}

impl<A: Actor> Driver<A> {
    pub(crate) fn new(
        actor: A,
        config: MailboxConfig<A::Message>,
        clock: Clock,
        incarnation: ActorIncarnation,
        jobs: Option<JobExecutor>,
        notify: Option<Waker>,
    ) -> Result<Self, SpawnError> {
        config.limits.validate().map_err(SpawnError::InvalidLimits)?;
        let idle = config.idle.map(|after| Box::new((after, Duration::ZERO)));
        let mailbox = Mailbox::new(config, clock, incarnation, notify);
        Ok(Self {
            actor: Some(actor),
            core: Core {
                mailbox,
                timers: BTreeMap::new(),
                timer_seq: 0,
                children: Vec::new(),
                jobs,
                job_seq: 1,
                stash: None,
            },
            batch: VecDeque::new(),
            wake: None,
            ending: None,
            started: false,
            stopped: false,
            dropped_timers: 0,
            handler_observed: 0,
            idle,
        })
    }

    pub(crate) fn address(&self) -> ActorRef<A::Message> {
        ActorRef(self.core.mailbox.clone())
    }

    fn call(&mut self, f: impl FnOnce(&mut A, &mut Context<'_, A>) -> Result<(), A::Error>) -> Result<(), StopReason> {
        self.handler_observed += 1;
        let mb = &self.core.mailbox;
        let started = sampled(mb.sample_every, self.handler_observed).then(|| mb.clock.now());
        let result = self.invoke(f);
        let mb = &self.core.mailbox;
        if let (Some(at), Some(t)) = (started, &mb.timings) {
            t.lock().handler.record(mb.clock.now().saturating_sub(at));
        }
        result
    }

    /// Runs a callback without counting it as a handled message.
    fn invoke(
        &mut self,
        f: impl FnOnce(&mut A, &mut Context<'_, A>) -> Result<(), A::Error>,
    ) -> Result<(), StopReason> {
        let actor = self.actor.as_mut().expect("actor present while running");
        let mut cx = Context { core: &mut self.core, _actor: PhantomData };
        let result = catch_unwind(AssertUnwindSafe(|| f(actor, &mut cx)));
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                let text = catch_unwind(AssertUnwindSafe(|| format!("{error:?}")));
                let dropped = catch_unwind(AssertUnwindSafe(|| drop(error)));
                Err(match (text, dropped) {
                    (Ok(text), Ok(())) => StopReason::Failed(text),
                    _ => StopReason::Panicked,
                })
            }
            Err(_) => Err(StopReason::Panicked),
        }
    }

    /// One message outside the mailbox batch: a timer, a finished job or a replayed stash message. Its handler time is
    /// sampled as any is, but it has no mailbox wait and no timing by label.
    fn handle(&mut self, msg: A::Message, trace: Option<TraceContext>) -> Result<(), StopReason> {
        let Some(parent) = trace else { return self.call(|a, cx| a.on_message(msg, cx)) };
        let mb = self.core.mailbox.clone();
        let label = catch_unwind(AssertUnwindSafe(|| A::label(&msg))).unwrap_or("");
        let open = OpenSpan::begin(&mb, parent, label, None);
        let outcome = self.call(|a, cx| a.on_message(msg, cx));
        open.finish(&mb, std::any::type_name::<A>(), outcome.is_err());
        outcome
    }

    pub(crate) fn run_turn(&mut self, budget: TurnBudget) -> TurnReport {
        let mut report = TurnReport::default();
        if self.stopped {
            report.stopped = true;
            return report;
        }
        let mb = self.core.mailbox.clone();
        // The driver is the only writer: the count lives there alone, and the driver stays a word smaller.
        let turns = mb.stats.turns.load(Ordering::Relaxed) + 1;
        mb.stats.turns.store(turns, Ordering::Relaxed);
        let timed = sampled(mb.sample_every, turns);
        let began = (timed || budget.max_elapsed.is_some()).then(|| mb.clock.now());
        let until = budget.max_elapsed.zip(began).map(|(max, at)| at.saturating_add(max));

        let advanced = match self.ending {
            None => self.advance(&mb, budget, until, &mut report),
            Some(_) => Ok(()),
        };
        // Counted before the actor can end: its exit retires these counters into the per-type totals.
        if report.budget_overrun {
            mb.stats.budget_overruns.fetch_add(1, Ordering::Relaxed);
        }
        let processed = (report.ordinary + report.completions + report.timers) as u64;
        // The driver is the only writer.
        mb.stats.processed.store(mb.stats.processed.load(Ordering::Relaxed) + processed, Ordering::Relaxed);
        if let Err(reason) = advanced {
            self.terminate(reason);
        }
        self.poll_ending();

        if let (true, Some(at), Some(t)) = (timed, began, &mb.timings) {
            t.lock().turn.record(mb.clock.now().saturating_sub(at));
        }
        report.stopped = self.stopped;
        report.next_timer = self.core.timers.first_key_value().map(|(k, _)| k.0);
        report.runnable &= !self.stopped;
        self.rearm();
        report
    }

    fn advance(
        &mut self,
        mb: &Arc<Mailbox<A::Message>>,
        budget: TurnBudget,
        until: Option<Duration>,
        report: &mut TurnReport,
    ) -> Result<(), StopReason> {
        let stop_check = |mb: &Mailbox<A::Message>| {
            if mb.fenced() || mb.stop_requested.load(Ordering::SeqCst) {
                return Err(mb.stop_reason().unwrap_or(StopReason::Stopped));
            }
            Ok(())
        };
        stop_check(mb)?;
        if !self.started {
            self.call(|a, cx| a.on_start(cx)).map_err(|reason| match reason {
                StopReason::Failed(text) => StopReason::StartFailed(text),
                other => other,
            })?;
            self.started = true;
        }

        if mb.settled() {
            let (failure, mut left, lifted) = mb.take_settled();
            if let Some(reason) = failure {
                return Err(reason);
            }
            if lifted {
                self.active();
            }
            // Only what was there: what settles meanwhile raised the flag again, and waits for the next turn.
            while left > 0 {
                stop_check(mb)?;
                let paused = mb.paused.load(Ordering::Relaxed);
                let (next, lifted) = {
                    let mut jobs = mb.jobs.lock();
                    let jobs = jobs.get_or_insert_default();
                    let next = match jobs.next_done(paused) {
                        Some((_, msg, trace)) => Some(Ok((msg, trace))),
                        // Downs wait for a pause as other results do.
                        None if paused => None,
                        None => jobs.fired.pop_front().map(Err),
                    };
                    (next, paused && jobs.pausing.is_empty())
                };
                let Some(next) = next else { break };
                left -= 1;
                if lifted {
                    // Its handler may pause again, which holds the rest.
                    mb.paused.store(false, Ordering::Relaxed);
                    self.active();
                }
                match next {
                    Ok((msg, trace)) => {
                        report.completions += 1;
                        self.handle(msg, trace)?;
                    }
                    Err((watch, exit)) => {
                        // None: cancelled since it was told.
                        let Some(map) = watch.map.lock().take() else { continue };
                        report.completions += 1;
                        let msg = catch_unwind(AssertUnwindSafe(|| map(exit))).map_err(|_| StopReason::Panicked)?;
                        self.handle(msg, None)?;
                    }
                }
            }
        }
        if !mb.opened() && !mb.paused.load(Ordering::Relaxed) {
            self.active();
            mb.open();
        }

        if mb.drain_requested.load(Ordering::SeqCst) {
            if !self.core.timers.is_empty() {
                let timers = std::mem::take(&mut self.core.timers);
                self.dropped_timers += timers.len();
                let _ = catch_unwind(AssertUnwindSafe(|| drop(timers)));
            }
            if let Some(jobs) = mb.jobs.lock().as_deref() {
                jobs.live.values().filter(|j| matches!(j.kind, JobKind::Source)).for_each(|j| j.cancel.cancel());
            }
        }
        let now = if self.core.timers.is_empty() { Duration::ZERO } else { mb.clock.now() };
        for _ in 0..self.core.timers.len() {
            let Some(entry) = self.core.timers.first_entry().filter(|e| e.key().0 <= now) else { break };
            let ((at, seq), kind) = entry.remove_entry();
            stop_check(mb)?;
            if let Some(t) = mb.timings.as_ref().filter(|_| sampled(mb.sample_every, report.timers as u64 + 1)) {
                t.lock().timer_lag.record(now.saturating_sub(at));
            }
            let (msg, trace) = match kind {
                TimerKind::Once(msg, trace) => (msg, trace),
                TimerKind::Every(period, mut make) => {
                    let msg = catch_unwind(AssertUnwindSafe(&mut make)).map_err(|_| StopReason::Panicked)?;
                    let next = at.saturating_add(period).max(now.saturating_add(Duration::from_nanos(1)));
                    self.core.timers.insert((next, seq), TimerKind::Every(period, make));
                    (msg, None)
                }
            };
            report.timers += 1;
            self.handle(msg, trace)?;
        }
        self.core.publish_timer_count();

        // The replay and the batch after it share the turn's budget, and as a batch it hands out one message before
        // time can end it.
        let max = budget.max_messages.get();
        let spent = |n: usize| n >= max || (n > 0 && until.is_some_and(|u| mb.clock.now() >= u));
        while self.core.released() && !spent(report.ordinary) && !mb.paused.load(Ordering::Relaxed) {
            stop_check(mb)?;
            let stash = self.core.stash.as_deref_mut().expect("released");
            let (msg, trace, bytes) = stash.held.pop_front().expect("released from what is held");
            stash.released -= 1;
            stash.bytes -= bytes;
            mb.unstashed.store(stash.released > 0, Ordering::Relaxed);
            report.ordinary += 1;
            self.handle(msg, trace)?;
            self.active();
        }

        stop_check(mb)?;
        if let Some(after) = self.idle_due() {
            // Closes admission first, as any drain: a message that raced it is still handled. The deadline keeps a job
            // that never ends from holding an actor nobody talks to.
            mb.request_drain(Some(mb.clock.now().saturating_add(after)));
        }
        // Read before the pull: a drain seen here closed admission first, so what the pull leaves is final.
        let draining = mb.drain_requested.load(Ordering::SeqCst);
        let replaying = self.core.released() || spent(report.ordinary);
        let mut batch = std::mem::take(&mut self.batch);
        let remaining = match mb.paused.load(Ordering::Relaxed) || replaying {
            true => 0,
            false => mb.pull(max - report.ordinary, &mut batch),
        };
        let mut view = MailboxBatch {
            entries: &mut batch,
            mailbox: mb,
            until,
            label: A::label,
            actor_type: std::any::type_name::<A>(),
            open: None,
            span: None,
            slots: 0,
            bytes: 0,
            overrun: false,
        };
        let outcome = if view.is_empty() { Ok(()) } else { self.call(|a, cx| a.on_batch(&mut view, cx)) };
        view.close(outcome.is_err());
        let (slots, bytes, overrun) = (view.slots, view.bytes, view.overrun);
        // The batch hands out at least one message unless a stop, a pause or a replay is pending, so an `on_batch` that
        // took none would be handed the same batch for ever.
        let paused = mb.paused.load(Ordering::Relaxed);
        // An `on_batch` may have released the stash before it took anything.
        let replaying = replaying || self.core.released();
        let stalled =
            slots == 0 && !batch.is_empty() && !paused && !replaying && !mb.stop_requested.load(Ordering::SeqCst);
        let outcome =
            if stalled { outcome.and(Err(StopReason::Failed("on_batch took no message".into()))) } else { outcome };
        report.ordinary += slots;
        if slots > 0 {
            self.active();
        }
        report.budget_overrun = overrun || until.is_some_and(|u| mb.clock.now() > u);
        if outcome.is_err() || mb.stop_requested.load(Ordering::SeqCst) {
            // Closed before the slots are freed, or a sender waiting for room is admitted to a mailbox about to be
            // discarded. A stop request has closed it already.
            mb.close(Phase::Stopping);
            self.batch = batch;
            mb.release(slots, bytes);
            outcome?;
            return stop_check(mb);
        }
        report.runnable = !paused && (remaining > 0 || !batch.is_empty() || replaying);
        mb.finish_batch(slots, bytes, &mut batch);
        self.batch = batch;

        if report.ordinary + report.completions + report.timers > 0 {
            self.invoke(|a, cx| a.on_turn_end(cx))?;
            stop_check(mb)?;
            report.runnable |= !mb.paused.load(Ordering::Relaxed) && self.core.released();
        }

        if draining {
            if !report.runnable && !paused {
                let jobs = mb.jobs.lock();
                match JobState::drain_end(jobs.as_deref()) {
                    Some(DrainEnd::Drained) => return Err(StopReason::Drained),
                    Some(DrainEnd::CloseSinks) => jobs.iter().flat_map(|jobs| jobs.live.values()).for_each(|j| {
                        if let JobKind::Sink(s) = &j.kind {
                            s.close()
                        }
                    }),
                    None => {}
                }
            }
            if mb.drain_deadline().is_some_and(|d| mb.clock.now() >= d) {
                return Err(StopReason::DrainDeadline);
            }
        }
        report.runnable |= self.core.timer_due();
        Ok(())
    }

    /// As `Drive::owed`, which `EmbeddedDriver` asks too, of an actor that need not be `Send`.
    pub(crate) fn owed(&self) -> Option<&'static str> {
        if self.stopped || self.ending.is_some() {
            return None;
        }
        let mb = &self.core.mailbox;
        if !self.started {
            return Some("on_start");
        }
        if mb.stop_requested.load(Ordering::SeqCst) {
            return Some("a stop");
        }
        if mb.settled() {
            return Some("a job result");
        }
        if !mb.paused.load(Ordering::Relaxed) && (!self.batch.is_empty() || mb.depth() > 0 || self.core.released()) {
            return Some("a message");
        }
        if self.core.timer_due() {
            return Some("a timer");
        }
        if self.idle_due().is_some() {
            return Some("an idle time");
        }
        if !mb.drain_requested.load(Ordering::SeqCst) {
            return None;
        }
        if mb.drain_deadline().is_some_and(|d| mb.clock.now() >= d) {
            return Some("a drain deadline");
        }
        JobState::drain_end(mb.jobs.lock().as_deref()).map(|_| "the end of a drain")
    }

    fn terminate(&mut self, mut reason: StopReason) {
        let mb = self.core.mailbox.clone();
        mb.close(Phase::Stopping);
        let queued = mb.discard();
        let batch = std::mem::take(&mut self.batch);
        let held = queued.iter().chain(&batch).fold((0, 0), |(slots, bytes), e| (slots + 1, bytes + e.bytes));
        mb.release(held.0, held.1);
        let (done, pending_jobs, watches, fired) = {
            match mb.jobs.lock().as_deref_mut() {
                Some(jobs) => {
                    jobs.live.values().for_each(|j| j.cancel.cancel());
                    let watches = std::mem::take(&mut jobs.watches);
                    (std::mem::take(&mut jobs.done), jobs.live.len(), watches, std::mem::take(&mut jobs.fired))
                }
                None => Default::default(),
            }
        };
        for (target, watch) in &watches {
            target.unwatch(watch);
        }
        // Taken, not left in the Arcs: a publisher or a cancel may hold one for a moment, and what a map captured
        // must drop here, inside the catch_unwind.
        let maps = watches.iter().map(|(_, w)| w).chain(fired.iter().map(|(w, _)| w));
        let maps: Vec<_> = maps.filter_map(|w| w.map.lock().take()).collect();
        let stash = self.core.stash.take().map(|s| s.held).unwrap_or_default();
        let lost = queued.iter().chain(&batch).map(|e| (&e.msg, e.trace));
        let lost = lost.chain(done.iter().map(|(_, m, t)| (m, *t))).chain(stash.iter().map(|(m, t, _)| (m, *t)));
        let mut discarded: Vec<Discarded> = Vec::new();
        for (msg, trace) in lost {
            // A label that panics costs its own message its label, not the others theirs.
            let label = catch_unwind(AssertUnwindSafe(|| A::label(msg))).unwrap_or("");
            match discarded.iter_mut().find(|d| d.label == label) {
                Some(d) => (d.count, d.trace) = (d.count + 1, d.trace.or(trace)),
                None => discarded.push(Discarded { label, count: 1, trace }),
            }
        }
        let timers = std::mem::take(&mut self.core.timers);
        let pending_timers = self.dropped_timers + timers.len();
        self.core.publish_timer_count();
        let mut actor = self.actor.take();
        let started = self.started;
        let clean = catch_unwind(AssertUnwindSafe(|| drop((queued, batch, done, stash, timers, maps, watches, fired))))
            .is_ok()
            & catch_unwind(AssertUnwindSafe(|| {
                if let (true, Some(a)) = (started, actor.as_mut()) {
                    a.on_stop(&reason);
                }
            }))
            .is_ok()
            & catch_unwind(AssertUnwindSafe(|| drop(actor))).is_ok();
        if !clean {
            reason = StopReason::Panicked;
        }
        let drained = reason == StopReason::Drained;
        let deadline = mb.drain_deadline();
        for child in &self.core.children {
            if drained { child.drain(deadline) } else { child.stop() }
        }
        let deadline = drained.then_some(deadline).flatten();
        self.ending = Some(Box::new(Ending { reason, deadline, discarded, pending_jobs, pending_timers }));
    }

    fn poll_ending(&mut self) {
        let Some(ending) = &mut self.ending else { return };
        let mb = &self.core.mailbox;
        let waker = mb.notify.as_ref();
        let wait = ending.reason == StopReason::Drained;
        self.core.children.retain(|c| match (wait, waker) {
            (true, Some(w)) => c.0.signal().poll(w).is_none(),
            _ => c.exit().is_none(),
        });
        let expired = ending.deadline.is_some_and(|d| mb.clock.now() >= d);
        if wait && !self.core.children.is_empty() && !expired {
            return;
        }
        let ending = self.ending.take().expect("checked above");
        let children_pending = self.core.children.len();
        let reason = match ending.reason {
            StopReason::Drained if children_pending > 0 => StopReason::DrainDeadline,
            other => other,
        };
        self.core.children.clear();
        self.stopped = true;
        mb.finish(Exit {
            incarnation: mb.incarnation(),
            reason,
            discarded: ending.discarded,
            pending_jobs: ending.pending_jobs,
            pending_timers: ending.pending_timers,
            children_pending,
        });
    }

    fn active(&mut self) {
        if let Some(idle) = &mut self.idle {
            idle.1 = self.core.mailbox.clock.now();
        }
    }

    /// The idle time, once it has passed.
    fn idle_due(&self) -> Option<Duration> {
        let mb = &self.core.mailbox;
        let &(after, since) = self.idle.as_deref()?;
        let due = self.batch.is_empty()
            && !mb.paused.load(Ordering::Relaxed)
            && mb.clock.now() >= since.saturating_add(after)
            && !mb.drain_requested.load(Ordering::SeqCst)
            && mb.depth() == 0;
        due.then_some(after)
    }

    // One clock registration covers the earliest of: next timer, drain deadline, idle time, ending deadline.
    fn rearm(&mut self) {
        let mb = &self.core.mailbox;
        let target = if self.stopped {
            None
        } else if let Some(ending) = &self.ending {
            ending.deadline
        } else {
            let timer = self.core.timers.first_key_value().map(|(k, _)| k.0);
            let draining = mb.drain_requested.load(Ordering::SeqCst);
            let drain = draining.then(|| mb.drain_deadline()).flatten();
            // A wake still to come before the idle time serves it: that turn looks again, so an actor kept busy
            // does not register a wake every turn.
            let idle = self.idle.as_deref().filter(|_| !draining).map(|&(after, since)| {
                let at = since.saturating_add(after);
                self.wake.map(WakeId::at).filter(|&w| w <= at && w > mb.clock.now()).unwrap_or(at)
            });
            [timer, drain, idle].into_iter().flatten().min()
        };
        if self.wake.map(WakeId::at) == target {
            return;
        }
        if let Some(id) = self.wake.take() {
            mb.clock.cancel(id);
        }
        if let (Some(at), Some(waker)) = (target, &mb.notify) {
            self.wake = Some(mb.clock.register(at, waker.clone()));
        }
    }
}

impl<A: Actor + Send> Drive for Driver<A> {
    fn turn(&mut self, budget: TurnBudget) -> (bool, bool) {
        let r = self.run_turn(budget);
        (r.runnable, r.stopped)
    }

    fn abandon(&mut self) {
        if !self.stopped && self.ending.is_none() {
            self.terminate(StopReason::Panicked);
        }
    }

    fn owed(&self) -> Option<&'static str> {
        Driver::owed(self)
    }

    fn forsake(&mut self) {
        self.stopped = true;
    }
}

impl<A: Actor> Drop for Driver<A> {
    fn drop(&mut self) {
        if !self.stopped {
            if self.ending.is_none() {
                self.terminate(StopReason::DriverDropped);
            }
            if let Some(ending) = &mut self.ending {
                ending.deadline = Some(Duration::ZERO);
                if ending.reason == StopReason::Drained {
                    ending.reason = StopReason::DrainDeadline;
                }
            }
            self.poll_ending();
        }
        self.rearm();
    }
}

/// Result of spawning: the address exists immediately, readiness is separate.
pub struct SpawnHandle<M: Send + 'static> {
    pub address: ActorRef<M>,
    pub lifecycle: LifecycleHandle,
}

impl<M: Send + 'static> SpawnHandle<M> {
    pub(crate) fn new(address: ActorRef<M>) -> Self {
        Self { lifecycle: address.lifecycle(), address }
    }

    fn readiness(&self, exit: Option<&Exit>) -> Option<Result<SpawnHandle<M>, StartError>> {
        if self.address.0.opened() {
            return Some(Ok(SpawnHandle::new(self.address.clone())));
        }
        exit.map(|exit| Err(StartError::Failed(exit.reason.clone())))
    }

    /// Timing out does not cancel the actor.
    pub fn wait_ready(&self, timeout: Duration) -> Result<SpawnHandle<M>, StartError> {
        let mb = &self.address.0;
        mb.signal.wait_until(&mb.clock, timeout, |exit| self.readiness(exit)).unwrap_or(Err(StartError::TimedOut))
    }

    pub fn ready(self) -> Ready<M> {
        Ready { handle: self, spun: false }
    }
}

pub struct Ready<M: Send + 'static> {
    handle: SpawnHandle<M>,
    spun: bool,
}

// A start usually finishes within microseconds; waiting that long costs less than being woken.
const READY_SPIN: Duration = Duration::from_micros(20);

impl<M: Send + 'static> Future for Ready<M> {
    type Output = Result<SpawnHandle<M>, StartError>;

    #[expect(clippy::disallowed_methods, reason = "a 20 µs spin; it falls through to the same result")]
    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        if !std::mem::replace(&mut self.spun, true) {
            let (since, mut spins) = (std::time::Instant::now(), 0u32);
            while !self.handle.address.0.opened()
                && matches!(self.handle.address.phase(), Phase::Starting | Phase::Running)
            {
                std::hint::spin_loop();
                spins += 1;
                if spins % 64 == 0 && since.elapsed() >= READY_SPIN {
                    break;
                }
            }
        }
        let exit = self.handle.address.0.signal.poll(cx.waker());
        match self.handle.readiness(exit.as_ref()) {
            Some(result) => Poll::Ready(result),
            None => Poll::Pending,
        }
    }
}

/// An actor driven by the caller's own thread. Nothing runs outside `run_turn`, so with a manual clock and manual
/// jobs the actor is a deterministic step function.
pub struct EmbeddedDriver<A: Actor>(Driver<A>);

impl<A: Actor> EmbeddedDriver<A> {
    pub fn new(
        actor: A,
        config: impl Into<MailboxConfig<A::Message>>,
        clock: Clock,
    ) -> Result<(Self, SpawnHandle<A::Message>), SpawnError> {
        Self::build(actor, config, clock, None)
    }

    pub fn with_jobs(
        actor: A,
        config: impl Into<MailboxConfig<A::Message>>,
        clock: Clock,
        executor: JobExecutor,
    ) -> Result<(Self, SpawnHandle<A::Message>), SpawnError> {
        Self::build(actor, config, clock, Some(executor))
    }

    fn build(
        actor: A,
        config: impl Into<MailboxConfig<A::Message>>,
        clock: Clock,
        jobs: Option<JobExecutor>,
    ) -> Result<(Self, SpawnHandle<A::Message>), SpawnError> {
        let incarnation = ActorIncarnation { runtime: next_runtime_id(), slot: 0, generation: 1 };
        let driver = Driver::new(actor, config.into(), clock, incarnation, jobs, None)?;
        let handle = SpawnHandle::new(driver.address());
        Ok((Self(driver), handle))
    }

    pub fn run_turn(&mut self, budget: TurnBudget) -> TurnReport {
        self.0.run_turn(budget)
    }

    /// Runs the jobs a manual executor holds, and turns, until nothing is due now: no job queued, message, timer due
    /// or job result. Returns how many turns handled something, or `NotIdle` if work is still due after `max_rounds`.
    pub fn run_until_idle(&mut self, max_rounds: usize) -> Result<usize, NotIdle> {
        let manual = self.0.core.jobs.as_ref().and_then(|jobs| jobs.manual.clone());
        let mut busy = 0;
        for round in 0..=max_rounds {
            let queued = manual.as_ref().map_or(0, ManualJobs::pending);
            if queued == 0 && self.0.owed().is_none() {
                return Ok(busy);
            }
            if round == max_rounds {
                break;
            }
            // Only what is queued now: a job that wakes itself runs again next round, not forever in this one.
            if let Some(manual) = &manual {
                for _ in 0..queued {
                    manual.run_next();
                }
            }
            let turn = self.0.run_turn(TurnBudget::default());
            busy += usize::from(turn.ordinary + turn.completions + turn.timers > 0);
        }
        Err(NotIdle)
    }
}

#[cfg(test)]
mod tests;
