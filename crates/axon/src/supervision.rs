use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Poll, Wake, Waker};
use std::time::Duration;

use parking_lot::Mutex;

use crate::backoff::Backoff;
use crate::clock::{Clock, WakeId};
use crate::context::Actor;
use crate::driver::Driver;
use crate::events::EventLog;
use crate::jobs::JobExecutor;
use crate::lifecycle::{ExitSignal, Lifecycle, LifecycleHandle};
use crate::mailbox::{ActorRef, Fence, MailboxConfig, past_fence};
use crate::runtime::Drive;
use crate::trace::unix_micros;
use crate::types::*;

#[derive(Clone)]
pub struct RestartPolicy {
    pub max_restarts: usize,
    pub within: Duration,
    /// The wait before each restart, by the failures within `within`: [`RestartPolicy::BACKOFF`] by default.
    pub backoff: Backoff,
    pub should_restart: Arc<dyn Fn(&StopReason) -> bool + Send + Sync>,
}

impl RestartPolicy {
    /// The default policy's: 10 ms, doubling up to 1 s, with up to a fifth of each wait cut away at random.
    pub const BACKOFF: Backoff =
        Backoff { initial: Duration::from_millis(10), max: Duration::from_secs(1), jitter: 0.2 };
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            max_restarts: 3,
            within: Duration::from_secs(60),
            backoff: Self::BACKOFF,
            should_restart: Arc::new(StopReason::is_failure),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RestartDecision {
    RestartAt(Duration),
    Stop,
    Escalate,
}

pub(crate) struct RestartBudget {
    policy: RestartPolicy,
    failures: VecDeque<Duration>,
}

impl RestartBudget {
    pub(crate) fn new(policy: RestartPolicy) -> Self {
        Self { policy, failures: VecDeque::new() }
    }

    pub(crate) fn on_exit(&mut self, reason: &StopReason, now: Duration) -> RestartDecision {
        match catch_unwind(AssertUnwindSafe(|| (self.policy.should_restart)(reason))) {
            Ok(true) => {}
            Ok(false) => return RestartDecision::Stop,
            Err(_) => return RestartDecision::Escalate,
        }
        while self.failures.front().is_some_and(|at| now.saturating_sub(*at) > self.policy.within) {
            self.failures.pop_front();
        }
        if self.failures.len() >= self.policy.max_restarts {
            return RestartDecision::Escalate;
        }
        self.failures.push_back(now);
        let failures = u32::try_from(self.failures.len()).unwrap_or(u32::MAX);
        RestartDecision::RestartAt(now.saturating_add(self.policy.backoff.delay(failures)))
    }
}

struct FamilyState<M> {
    current: Option<ActorRef<M>>,
    generation: u64,
    stop: Option<StopReason>,
    draining: bool,
    /// The counters and timings of the generations that ended, so the family's never go back at a restart.
    ended: Diagnostics,
}

impl<M: Send + 'static> FamilyState<M> {
    /// Takes the current generation's address, keeping its counters in `ended` under the same lock.
    fn retire(&mut self) {
        if let Some(current) = self.current.take() {
            add_counters!(&mut self.ended, &current.diagnostics());
        }
    }
}

/// Shared view of a supervised family: exactly one current address, and the counters of the generations before it.
pub(crate) struct Family<M> {
    base: ActorIncarnation,
    state: Mutex<FamilyState<M>>,
    signal: ExitSignal,
    notify: Option<Waker>,
    restarts: AtomicU64,
    /// `unix_micros()` at the latest restart, 0 before one; stored before `restarts` is released.
    restarted_us: AtomicU64,
    fence: Option<Fence>,
    clock: Clock,
}

impl<M: Send + 'static> Family<M> {
    /// Wakes whoever waits on the family, and its turn.
    fn touch(&self) {
        self.signal.touch();
        if let Some(n) = &self.notify {
            n.wake_by_ref();
        }
    }
}

/// A restart's wake.
impl<M: Send + 'static> Wake for Family<M> {
    fn wake(self: Arc<Self>) {
        self.touch();
    }
}

impl<M: Send + 'static> Lifecycle for Family<M> {
    fn incarnation(&self) -> ActorIncarnation {
        ActorIncarnation { generation: self.state.lock().generation, ..self.base }
    }

    fn phase(&self) -> Phase {
        if self.signal.ended() {
            return Phase::Stopped;
        }
        self.state.lock().current.as_ref().map_or(Phase::Starting, |c| c.phase())
    }

    fn request_stop(&self, reason: StopReason) {
        let reason = past_fence(self.fence.as_ref(), reason);
        let current = {
            let mut s = self.state.lock();
            s.stop.get_or_insert(reason.clone());
            s.current.clone()
        };
        if let Some(c) = current {
            c.lifecycle().stop_with(reason);
        }
        self.touch();
    }

    fn fail(&self, reason: StopReason) {
        // Between generations there is nothing to fail: the restart already under way goes ahead.
        let current = self.state.lock().current.clone();
        if let Some(c) = current {
            c.lifecycle().stop_with(reason);
        }
    }

    fn request_drain(&self, deadline: Option<Duration>) {
        let current = {
            let mut s = self.state.lock();
            s.draining = true;
            s.current.clone()
        };
        match current {
            Some(c) => c.lifecycle().drain(deadline),
            None => self.request_stop(StopReason::Stopped),
        }
    }

    fn signal(&self) -> &ExitSignal {
        &self.signal
    }

    fn clock(&self) -> &Clock {
        &self.clock
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SupervisionWaitError {
    TimedOut,
    Stopped,
}

pub struct SupervisionHandle<M>(Arc<Family<M>>);

impl<M> Clone for SupervisionHandle<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M: Send + 'static> SupervisionHandle<M> {
    /// The live generation's address, from its spawn until it closes; none while a restart waits out its backoff.
    pub fn current(&self) -> Option<ActorRef<M>> {
        self.0.state.lock().current.clone().filter(|c| matches!(c.phase(), Phase::Starting | Phase::Running))
    }

    /// The first generation after `after` that takes messages; `Closed` once the family is asked to stop or has ended.
    pub(crate) async fn next(&self, after: u64) -> Result<ActorRef<M>, RejectReason> {
        let found = || {
            let s = self.0.state.lock();
            let closing = s.stop.is_some() || s.draining;
            drop(s);
            if closing {
                return Some(Err(RejectReason::Closed));
            }
            self.current().filter(|c| c.incarnation().generation > after).map(Ok)
        };
        std::future::poll_fn(|cx| {
            if let Some(found) = found() {
                return Poll::Ready(found);
            }
            if self.0.signal.poll(cx.waker()).is_some() {
                return Poll::Ready(Err(RejectReason::Closed));
            }
            found().map_or(Poll::Pending, Poll::Ready)
        })
        .await
    }

    pub fn generation(&self) -> u64 {
        self.0.state.lock().generation
    }

    /// The counters and timings of every generation so far, and the gauges of the current one.
    pub(crate) fn diagnostics(&self) -> Diagnostics {
        let s = self.0.state.lock();
        let mut d = s.current.as_ref().map(ActorRef::diagnostics).unwrap_or_default();
        // The gauges stay the live generation's.
        add_counters!(&mut d, &s.ended);
        d
    }

    pub fn restarts(&self) -> u64 {
        self.0.restarts.load(Ordering::Acquire)
    }

    /// Read after `restarts`, it is at least as recent as the restart that count saw.
    pub(crate) fn restarted_us(&self) -> Option<u64> {
        Some(self.0.restarted_us.load(Ordering::Relaxed)).filter(|&us| us != 0)
    }

    pub fn lifecycle(&self) -> LifecycleHandle {
        LifecycleHandle(self.0.clone())
    }

    /// Blocks until a generation newer than `after` is running.
    pub fn wait_for_generation(&self, after: u64, timeout: Duration) -> Result<ActorRef<M>, SupervisionWaitError> {
        self.0
            .signal
            .wait_until(&self.0.clock, timeout, |exit| {
                let s = self.0.state.lock();
                match &s.current {
                    Some(c) if s.generation > after && c.phase() == Phase::Running => Some(Ok(c.clone())),
                    _ if exit.is_some() || s.stop.is_some() => Some(Err(SupervisionWaitError::Stopped)),
                    _ => None,
                }
            })
            .unwrap_or(Err(SupervisionWaitError::TimedOut))
    }
}

#[derive(Clone, Debug, Default)]
pub struct SupervisionReport {
    pub turn: TurnReport,
    pub replaced: bool,
    pub next_restart: Option<Duration>,
    pub terminal: Option<Exit>,
    pub escalated: bool,
}

/// Restarting wrapper around a driver. A restart is a new incarnation: fresh state, a new
/// mailbox, nothing replayed; old addresses stay `Closed`.
pub struct Supervised<A: Actor, F> {
    factory: F,
    driver: Option<Driver<A>>,
    budget: RestartBudget,
    family: Arc<Family<A::Message>>,
    config: MailboxConfig<A::Message>,
    jobs: Option<JobExecutor>,
    restart: Option<(Duration, Option<WakeId>)>,
    last_exit: Option<Exit>,
    announced: bool,
    done: bool,
}

impl<A: Actor, F: FnMut() -> A> Supervised<A, F> {
    pub fn new(
        factory: F,
        config: impl Into<MailboxConfig<A::Message>>,
        clock: Clock,
        policy: RestartPolicy,
    ) -> Result<(Self, SupervisionHandle<A::Message>), SpawnError> {
        let base = ActorIncarnation { runtime: crate::driver::next_runtime_id(), slot: 0, generation: 0 };
        Self::build(factory, config.into(), clock, policy, base, None, None)
    }

    pub(crate) fn build(
        factory: F,
        config: MailboxConfig<A::Message>,
        clock: Clock,
        policy: RestartPolicy,
        base: ActorIncarnation,
        jobs: Option<JobExecutor>,
        notify: Option<Waker>,
    ) -> Result<(Self, SupervisionHandle<A::Message>), SpawnError> {
        config.limits.validate().map_err(SpawnError::InvalidLimits)?;
        let family = Arc::new(Family {
            base,
            state: Mutex::new(FamilyState {
                current: None,
                generation: 0,
                stop: None,
                draining: false,
                ended: Diagnostics::default(),
            }),
            signal: ExitSignal::default(),
            notify,
            restarts: AtomicU64::new(0),
            restarted_us: AtomicU64::new(0),
            fence: config.fence.clone(),
            clock,
        });
        let mut this = Self {
            factory,
            driver: None,
            budget: RestartBudget::new(policy),
            family: family.clone(),
            config,
            jobs,
            restart: None,
            last_exit: None,
            announced: true,
            done: false,
        };
        if !this.replace() {
            return Err(SpawnError::FactoryPanicked);
        }
        Ok((this, SupervisionHandle(family)))
    }

    fn replace(&mut self) -> bool {
        let Ok(actor) = catch_unwind(AssertUnwindSafe(&mut self.factory)) else { return false };
        let mut state = self.family.state.lock();
        let Some(generation) = state.generation.checked_add(1) else { return false };
        let incarnation = ActorIncarnation { generation, ..self.family.base };
        let notify = self.family.notify.clone();
        let Ok(driver) =
            Driver::new(actor, self.config.clone(), self.family.clock.clone(), incarnation, self.jobs.clone(), notify)
        else {
            return false;
        };
        state.generation = generation;
        state.current = Some(driver.address());
        self.driver = Some(driver);
        self.announced = false;
        true
    }

    fn finish(&mut self, report: &mut SupervisionReport, escalated: bool) {
        if escalated && let Some((events, subject, reason)) = self.note() {
            events.record("gave_up", subject, reason);
        }
        self.done = true;
        self.family.state.lock().retire();
        let exit = self.last_exit.clone().unwrap_or_else(|| Exit::new(self.family.incarnation(), StopReason::Stopped));
        self.family.signal.publish(exit.clone());
        report.terminal = Some(exit);
        report.escalated = escalated;
        report.turn.stopped = true;
        report.turn.runnable = false;
    }

    /// For a generation that was to restart: the family's exit reports the last generation alone, or the stop that
    /// came in its place, so this one's losses go now, under its own reason, wherever that stop lands.
    fn bury_losses(&mut self) {
        let Some(exit) = &mut self.last_exit else { return };
        self.family.state.lock().ended.dead_letters += exit.discarded_messages() as u64;
        if let Some((dead, name)) = &self.config.dead {
            dead.record_exit(exit, std::any::type_name::<A>(), name.as_ref());
            exit.discarded.clear();
        }
    }

    /// Ends the family as it was asked to, not as its last generation did.
    fn end_as_asked(&mut self, report: &mut SupervisionReport, reason: StopReason) {
        if let Some(exit) = &mut self.last_exit {
            exit.reason = reason;
        }
        self.finish(report, false);
    }

    /// Where an `actor_restarted` or a `gave_up` goes, and the family's name and last failure it goes under.
    fn note(&self) -> Option<(&EventLog, String, String)> {
        let events = self.config.events.as_deref()?;
        let subject = match self.config.dead.as_ref().and_then(|(_, name)| name.as_deref()) {
            Some(name) => name.to_owned(),
            None => format!("{} {}", std::any::type_name::<A>(), self.family.base),
        };
        let reason = match self.last_exit.as_ref().map_or(&StopReason::Stopped, |e| &e.reason) {
            StopReason::Failed(error) => error.clone(),
            reason => format!("{reason:?}"),
        };
        Some((events, subject, reason))
    }

    pub fn run_turn(&mut self, budget: TurnBudget) -> SupervisionReport {
        let mut report = SupervisionReport::default();
        if self.done {
            report.turn.stopped = true;
            return report;
        }
        if let Some(driver) = &mut self.driver {
            report.turn = driver.run_turn(budget);
            if !self.announced && driver.address().phase() != Phase::Starting {
                self.announced = true;
                self.family.signal.touch();
            }
            if !report.turn.stopped {
                return report;
            }
            self.last_exit = driver.address().lifecycle().exit();
            self.driver = None;
            report.turn.stopped = false;
            let reason = self.last_exit.as_ref().map_or(StopReason::Stopped, |e| e.reason.clone());
            let asked = {
                let s = self.family.state.lock();
                s.stop.clone().or(s.draining.then(|| past_fence(self.family.fence.as_ref(), StopReason::Stopped)))
            };
            match (self.budget.on_exit(&reason, self.family.clock.now()), asked) {
                // Asked to stop after this generation failed but before its restart was set: as in the backoff below.
                (RestartDecision::RestartAt(_), Some(asked))
                    if !matches!(reason, StopReason::Drained | StopReason::DrainDeadline) =>
                {
                    self.bury_losses();
                    self.end_as_asked(&mut report, asked);
                }
                (_, Some(_)) => self.finish(&mut report, false),
                (RestartDecision::RestartAt(at), None) => {
                    self.bury_losses();
                    self.family.state.lock().retire();
                    let waker = Waker::from(self.family.clone());
                    let id = self.family.notify.is_some().then(|| self.family.clock.register(at, waker));
                    self.restart = Some((at, id));
                }
                (RestartDecision::Stop, None) => self.finish(&mut report, false),
                (RestartDecision::Escalate, None) => self.finish(&mut report, true),
            }
        }
        if let Some((at, id)) = self.restart {
            report.next_restart = Some(at);
            let stop = self.family.state.lock().stop.clone();
            if stop.is_some() || self.family.clock.now() >= at {
                self.restart = None;
                if let Some(id) = id {
                    self.family.clock.cancel(id);
                }
                if let Some(reason) = stop {
                    // Stopped while waiting to restart.
                    self.end_as_asked(&mut report, reason);
                } else if self.replace() {
                    if let Some((events, subject, reason)) = self.note() {
                        events.record_restart(subject, reason);
                    }
                    self.family.restarted_us.store(unix_micros().max(1), Ordering::Relaxed);
                    self.family.restarts.fetch_add(1, Ordering::Release);
                    report.replaced = true;
                    report.next_restart = None;
                    report.turn.runnable = true;
                } else {
                    self.finish(&mut report, true);
                }
            }
        }
        report
    }
}

impl<A: Actor + Send, F: FnMut() -> A + Send> Drive for Supervised<A, F> {
    fn turn(&mut self, budget: TurnBudget) -> (bool, bool) {
        let r = self.run_turn(budget).turn;
        (r.runnable, r.stopped)
    }

    fn abandon(&mut self) {
        if let Some(driver) = &mut self.driver {
            driver.abandon();
        }
    }

    /// The driver's, and a restart whose backoff is over or that a stop cancels.
    fn owed(&self) -> Option<&'static str> {
        if self.done {
            return None;
        }
        if let Some(driver) = &self.driver {
            return driver.owed();
        }
        let (at, _) = self.restart?;
        (self.family.state.lock().stop.is_some() || self.family.clock.now() >= at).then_some("a restart")
    }

    fn forsake(&mut self) {
        self.done = true;
        if let Some(driver) = &mut self.driver {
            driver.forsake();
        }
    }
}

impl<A: Actor, F> Drop for Supervised<A, F> {
    fn drop(&mut self) {
        if let Some((_, Some(id))) = self.restart.take() {
            self.family.clock.cancel(id);
        }
        if !self.done {
            self.driver = None;
            self.family.state.lock().retire();
            self.family.signal.publish(Exit::new(self.family.incarnation(), StopReason::DriverDropped));
        }
    }
}
