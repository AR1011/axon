use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::task::Poll;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::watch;

use crate::clock::Clock;
use crate::context::Actor;
use crate::dead::DeadLetters;
use crate::driver::SpawnHandle;
use crate::events::EventLog;
use crate::histogram::Timings;
use crate::jobs::{CatchPanic, Job, JobExecutor};
use crate::lifecycle::{ExitWatcher, LifecycleHandle};
use crate::mailbox::{ActorRef, Fence, Mailbox, MailboxConfig};
use crate::reply::ReplyTo;
use crate::runtime::{Execution, Parked, Pump, Runtime, RuntimeConfig, RuntimeStats};
use crate::supervision::{RestartPolicy, SupervisionHandle};
use crate::topic::{Registry, Topic};
use crate::trace::{Tracer, unix_micros};
use crate::types::*;

const MAX_EXTENSIONS: usize = 32;

#[derive(Debug)]
pub enum SystemError {
    NoExecutor,
    Closed,
    Configuration(&'static str),
    NameTaken(String),
    ExtensionLimit,
    SubscriberLimit,
    Spawn(SpawnError),
    Start(StartError),
}

display_as_debug!(SystemError);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaveReason {
    Requested,
    Signal(String),
    Maintenance,
    RollingRestart,
}

impl LeaveReason {
    fn validate(&self) -> Result<(), SystemError> {
        let valid = match self {
            LeaveReason::Signal(s) => s.is_empty() || crate::valid_text(s, 32),
            _ => true,
        };
        if valid { Ok(()) } else { Err(SystemError::Configuration("invalid leave reason")) }
    }
}

pub type ExtensionFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

/// A service attached to a system and torn down with it. `leaving` hooks run in attachment
/// order before the drain; `shutdown` then `close` run in reverse order after it. A hook's `budget` is an eighth
/// less than it is waited for; past that its future is dropped.
pub trait SystemExtension: Send + Sync + 'static {
    fn name(&self) -> &str;

    fn leaving(&self, _reason: &LeaveReason, _budget: Duration) -> ExtensionFuture {
        Box::pin(std::future::ready(Ok(())))
    }

    fn shutdown(&self, _budget: Duration) -> ExtensionFuture {
        Box::pin(std::future::ready(Ok(())))
    }

    /// Non-blocking and idempotent.
    fn close(&self) {}
}

#[derive(Clone, Debug)]
pub struct SystemShutdown {
    pub leave: LeaveReason,
    pub actors: Vec<Exit>,
    pub still_running: Vec<ActorIncarnation>,
    /// Best-effort pre-drain steps that failed; they do not make the shutdown unclean.
    pub warnings: Vec<String>,
    pub failures: Vec<String>,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ActorDiagnostics {
    pub incarnation: ActorIncarnation,
    pub name: Option<Arc<str>>,
    pub actor_type: &'static str,
    pub phase: Phase,
    pub restarts: u64,
    /// `trace::unix_micros()` when the actor, or its supervised family, was spawned.
    pub started_us: u64,
    /// `trace::unix_micros()` at the family's latest restart; `None` before one, and always for an unsupervised actor.
    pub restarted_us: Option<u64>,
    pub diagnostics: Diagnostics,
}

#[derive(Clone, Debug)]
pub struct ActorDiagnosticsPage {
    pub actors: Vec<ActorDiagnostics>,
    pub total: usize,
    pub next: Option<ActorIncarnation>,
}

/// Counters summed over actors of one type.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct TypeTotals {
    pub actors: u64,
    pub restarts: u64,
    pub admitted: u64,
    pub rejected: [u64; 6],
    pub processed: u64,
    pub turns: u64,
    pub budget_overruns: u64,
    pub asks: u64,
    pub ask_timeouts: u64,
    pub dead_letters: u64,
    pub timings: Option<Box<Timings>>,
}

impl TypeTotals {
    pub fn add(&mut self, restarts: u64, d: &Diagnostics) {
        self.actors += 1;
        self.restarts += restarts;
        add_counters!(self, d);
    }
}

/// Lifecycle counters since the system was built. `retired` holds the final counters of
/// actors that have exited, so per-type sums stay monotonic under churn.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SystemStats {
    pub started: u64,
    pub stopped: u64,
    pub failed: u64,
    /// `DeadLetters::discarded`: messages accepted and never handled, and topic messages a subscriber missed.
    pub dead_letters: u64,
    pub retired: HashMap<&'static str, TypeTotals>,
}

struct Registered {
    name: Option<Arc<str>>,
    actor_type: &'static str,
    lifecycle: LifecycleHandle,
    address: Arc<dyn Registrant>,
    started_us: u64,
}

/// What the system keeps of an actor, to find it by name and to read its phase, restarts, when it last restarted and
/// diagnostics. An unsupervised actor's is its mailbox, so registering one allocates nothing.
trait Registrant: Any + Send + Sync {
    fn probe(&self) -> (Phase, u64, Option<u64>, Diagnostics);
}

impl<M: Send + 'static> Registrant for Mailbox<M> {
    fn probe(&self) -> (Phase, u64, Option<u64>, Diagnostics) {
        (self.phase(), 0, None, self.diagnostics())
    }
}

impl<M: Send + 'static> Registrant for SupervisionHandle<M> {
    fn probe(&self) -> (Phase, u64, Option<u64>, Diagnostics) {
        (self.lifecycle().phase(), self.restarts(), self.restarted_us(), self.diagnostics())
    }
}

#[derive(Default)]
struct State {
    leave: Option<LeaveReason>,
    actors: BTreeMap<ActorIncarnation, Registered>,
    /// `None` while a start holds the name before its actor exists, so a refused start runs nothing.
    names: HashMap<Arc<str>, Option<ActorIncarnation>>,
    extensions: Vec<Arc<dyn SystemExtension>>,
    started: u64,
    report: Option<watch::Receiver<Option<SystemShutdown>>>,
}

struct Inner {
    runtime: Runtime,
    tokio: tokio::runtime::Handle,
    mailbox: MailboxLimits,
    timing_sample_every: u32,
    tracer: Arc<Tracer>,
    dead: Arc<DeadLetters>,
    events: Arc<EventLog>,
    topics: Arc<Registry>,
    start_timeout: Duration,
    request_timeout: Duration,
    shutdown_timeout: Duration,
    state: Mutex<State>,
    /// Written under `state`; read without it only to refuse a spawn early.
    closed: AtomicBool,
    /// Apart from `state` so that an exit takes that only once.
    retired: Mutex<Retired>,
}

#[derive(Default)]
struct Retired {
    stopped: u64,
    failed: u64,
    types: HashMap<&'static str, TypeTotals>,
}

impl ExitWatcher for Inner {
    fn exited(&self, exit: &Exit) {
        // A family is kept under generation 0 and ends at whichever generation it reached.
        let family = ActorIncarnation { generation: 0, ..exit.incarnation };
        let entry = {
            let mut state = self.state.lock();
            let Some(entry) = state.actors.remove(&exit.incarnation).or_else(|| state.actors.remove(&family)) else {
                return;
            };
            if let Some(name) = &entry.name {
                state.names.remove(name);
            }
            entry
        };
        let (_, restarts, _, mut diagnostics) = entry.address.probe();
        diagnostics.dead_letters += exit.discarded_messages() as u64;
        self.dead.record_exit(exit, entry.actor_type, entry.name.as_ref());
        // Its own fault: one failure across a linked group counts once, as `keyed_failed` counts it.
        let failed = exit.reason.is_own_failure();
        let mut retired = self.retired.lock();
        retired.stopped += 1;
        retired.failed += failed as u64;
        retired.types.entry(entry.actor_type).or_default().add(restarts, &diagnostics);
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        for ext in self.state.get_mut().extensions.iter().rev() {
            let _ = catch_unwind(AssertUnwindSafe(|| ext.close()));
        }
    }
}

pub struct ActorSystemBuilder {
    runtime: RuntimeConfig,
    mailbox: MailboxLimits,
    timing_sample_every: u32,
    trace: (usize, u32),
    start_timeout: Duration,
    request_timeout: Duration,
    shutdown_timeout: Duration,
}

impl ActorSystemBuilder {
    pub fn runtime(mut self, config: RuntimeConfig) -> Self {
        self.runtime = config;
        self
    }

    /// Default mailbox limits for actors that do not set their own.
    pub fn mailbox(mut self, limits: MailboxLimits) -> Self {
        self.mailbox = limits;
        self
    }

    /// Default timing sample interval for actors that do not set their own; `0` disables.
    pub fn timing_sample_every(mut self, every: u32) -> Self {
        self.timing_sample_every = every;
        self
    }

    /// Keeps the last `spans` spans. One in `sample_every` messages arriving outside any trace
    /// starts one; 0 (the default) traces only what [`TraceContext::root`](crate::trace::TraceContext::root) started.
    pub fn tracing(mut self, spans: usize, sample_every: u32) -> Self {
        self.trace = (spans, sample_every);
        self
    }

    pub fn start_timeout(mut self, timeout: Duration) -> Self {
        self.start_timeout = timeout;
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    pub fn build(mut self) -> Result<ActorSystem, SystemError> {
        let tokio = tokio::runtime::Handle::try_current().map_err(|_| SystemError::NoExecutor)?;
        if [self.start_timeout, self.request_timeout, self.shutdown_timeout].contains(&Duration::ZERO) {
            return Err(SystemError::Configuration("timeouts must be non-zero"));
        }
        self.mailbox.validate().map_err(SystemError::Configuration)?;
        let (threadless, origin) = (self.runtime.threadless.is_some(), tokio::time::Instant::now());
        if let Some(controls) = self.runtime.threadless.clone() {
            if !self.runtime.clock.is_system() {
                return Err(SystemError::Configuration("a threadless runtime runs on its tokio runtime's clock"));
            }
            let handle = tokio.clone();
            self.runtime.clock = Clock::driven(move || {
                let _entered = handle.enter();
                controls.now(origin.elapsed())
            });
        }
        if self.runtime.job_executor.is_none() {
            let handle = tokio.clone();
            self.runtime.job_executor = Some(match threadless {
                // Every job a task on the one thread, so a simulation stays one trace.
                true => JobExecutor::custom(move |job| {
                    drop(match job {
                        Job::Blocking(f) => handle.spawn(async move { f() }),
                        Job::Async(fut) => handle.spawn(fut),
                    });
                    Ok(())
                }),
                false => JobExecutor::tokio(handle),
            });
        }
        let runtime = Runtime::start(self.runtime).map_err(SystemError::Spawn)?;
        let tracer = Tracer::new(self.trace.0, self.trace.1);
        if let Some(pump) = runtime.pump() {
            tokio.spawn(run(pump, origin));
        }
        let dead = Arc::new(DeadLetters::new(runtime.clock().clone(), tracer.clone()));
        let events = Arc::new(EventLog::new(runtime.clock().clone()));
        Ok(ActorSystem(Arc::new(Inner {
            runtime,
            tokio,
            mailbox: self.mailbox,
            timing_sample_every: self.timing_sample_every,
            tracer,
            dead,
            events,
            topics: Arc::default(),
            start_timeout: self.start_timeout,
            request_timeout: self.request_timeout,
            shutdown_timeout: self.shutdown_timeout,
            state: Mutex::default(),
            closed: AtomicBool::new(false),
            retired: Mutex::default(),
        })))
    }
}

/// Runs a threadless runtime's actors on this task, and sleeps until its clock's next wake when none is ready.
async fn run(pump: Pump, origin: tokio::time::Instant) {
    let mut sleep = std::pin::pin!(tokio::time::sleep_until(origin));
    loop {
        if pump.run(|| origin.elapsed()) > 0 {
            tokio::task::yield_now().await;
            continue;
        }
        let end = std::future::poll_fn(|cx| match pump.park(cx.waker(), origin.elapsed()) {
            Parked::Run => Poll::Ready(false),
            Parked::End => Poll::Ready(true),
            // A wake past what tokio's `Instant` holds, as `schedule_after(Duration::MAX)` asks for, never comes.
            Parked::Until(at) => match at.and_then(|at| origin.checked_add(at)) {
                Some(at) => {
                    sleep.as_mut().reset(at);
                    sleep.as_mut().poll(cx).map(|()| false)
                }
                None => Poll::Pending,
            },
        });
        if end.await {
            return;
        }
    }
}

/// Tokio-hosted orchestration over the runtime: registration, deadlines, shutdown, extensions.
/// Actor handles do not keep the system alive; dropping the last clone stops everything
/// without draining.
#[derive(Clone)]
pub struct ActorSystem(Arc<Inner>);

#[derive(Clone)]
pub struct WeakActorSystem(Weak<Inner>);

impl WeakActorSystem {
    pub fn upgrade(&self) -> Option<ActorSystem> {
        self.0.upgrade().map(ActorSystem)
    }
}

impl ActorSystem {
    pub fn builder() -> ActorSystemBuilder {
        let timeout = Duration::from_secs(5);
        ActorSystemBuilder {
            runtime: RuntimeConfig::default(),
            mailbox: MailboxLimits::default(),
            timing_sample_every: 0,
            trace: (4096, 0),
            start_timeout: timeout,
            request_timeout: timeout,
            shutdown_timeout: timeout,
        }
    }

    pub fn downgrade(&self) -> WeakActorSystem {
        WeakActorSystem(Arc::downgrade(&self.0))
    }

    /// The name a running actor was registered under.
    pub fn name_of(&self, actor: ActorIncarnation) -> Option<Arc<str>> {
        self.0.state.lock().actors.get(&actor)?.name.clone()
    }

    /// Where the actors' jobs run unless a job names another executor: `RuntimeConfig::job_executor`, or the system's
    /// tokio runtime.
    pub fn job_executor(&self) -> JobExecutor {
        self.0.runtime.job_executor().expect("a system's runtime always has a job executor").clone()
    }

    /// The clock of every actor of this system: real time, or the tokio runtime's in a threadless one.
    pub fn clock(&self) -> &Clock {
        self.0.runtime.clock()
    }

    pub fn tracer(&self) -> &Arc<Tracer> {
        &self.0.tracer
    }

    pub fn dead_letters(&self) -> &Arc<DeadLetters> {
        &self.0.dead
    }

    pub fn events(&self) -> &Arc<EventLog> {
        &self.0.events
    }

    pub fn runtime_stats(&self) -> RuntimeStats {
        self.0.runtime.stats()
    }

    pub fn stats(&self) -> SystemStats {
        // Retired first: an actor starts before it stops, so `stopped` never reads above `started`.
        let (stopped, failed, retired) = {
            let r = self.0.retired.lock();
            (r.stopped, r.failed, r.types.clone())
        };
        let started = self.0.state.lock().started;
        SystemStats { started, stopped, failed, dead_letters: self.0.dead.discarded(), retired }
    }

    pub fn leave_reason(&self) -> Option<LeaveReason> {
        self.0.state.lock().leave.clone()
    }

    pub fn spawn<A: Actor + Send>(&self, actor: A) -> ActorBuilder<'_, A, A> {
        self.builder_for(actor)
    }

    /// Spawns a restarting family; `factory` builds each generation's fresh state.
    pub fn supervise<A: Actor + Send, F: FnMut() -> A + Send + 'static>(&self, factory: F) -> ActorBuilder<'_, A, F> {
        self.builder_for(factory)
    }

    fn builder_for<A: Actor, S>(&self, source: S) -> ActorBuilder<'_, A, S> {
        ActorBuilder {
            system: self,
            source,
            name: None,
            execution: Execution::Shared,
            mailbox: MailboxConfig::from(self.0.mailbox)
                .with_timing(self.0.timing_sample_every)
                .with_tracer(self.0.tracer.clone()),
            policy: None,
        }
    }

    /// The running actor registered under `name`, if its message type is `M`.
    pub fn lookup<M: Send + 'static>(&self, name: &str) -> Option<ActorHandle<M>> {
        let (lifecycle, address) = self.registered(name)?;
        let address: Arc<dyn Any + Send + Sync> = address;
        Some(self.handle(ActorRef(address.downcast::<Mailbox<M>>().ok()?), lifecycle))
    }

    /// The supervised family registered under `name`, if its message type is `M`.
    pub fn lookup_supervised<M: Send + 'static>(&self, name: &str) -> Option<SupervisedHandle<M>> {
        let (lifecycle, address) = self.registered(name)?;
        let family = (&*address as &dyn Any).downcast_ref::<SupervisionHandle<M>>()?.clone();
        Some(SupervisedHandle { lifecycle, family, request_timeout: self.0.request_timeout })
    }

    /// What is registered under `name`, taken out of the lock.
    fn registered(&self, name: &str) -> Option<(LifecycleHandle, Arc<dyn Registrant>)> {
        let state = self.0.state.lock();
        let entry = state.actors.get(state.names.get(name)?.as_ref()?)?;
        Some((entry.lifecycle.clone(), entry.address.clone()))
    }

    fn handle<M: Send + 'static>(&self, address: ActorRef<M>, lifecycle: LifecycleHandle) -> ActorHandle<M> {
        ActorHandle { address, lifecycle, request_timeout: self.0.request_timeout, start_timeout: self.0.start_timeout }
    }

    /// The topic named `name` on this node, made on first use; `NameTaken` if it carries another message type. Names
    /// follow the rule for actor names, apart from them.
    pub fn topic<M: Send + 'static>(&self, name: &str) -> Result<Topic<M>, SystemError> {
        if !crate::valid_text(name, crate::MAX_NAME) {
            return Err(SystemError::Configuration("invalid topic name"));
        }
        Topic::open(&self.0.topics, &self.0.dead, name)
    }

    /// A live view ordered by incarnation, not a frozen snapshot.
    pub fn diagnostics_page(
        &self,
        after: Option<ActorIncarnation>,
        limit: usize,
    ) -> Result<ActorDiagnosticsPage, SystemError> {
        if !(1..=1000).contains(&limit) {
            return Err(SystemError::Configuration("limit must be 1..=1000"));
        }
        let (mut probes, total) = {
            let state = self.0.state.lock();
            let lower = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
            let range = state.actors.range((lower, std::ops::Bound::Unbounded));
            let probes: Vec<_> = range
                .take(limit + 1)
                .map(|(id, r)| (*id, r.name.clone(), r.actor_type, r.started_us, r.address.clone()))
                .collect();
            (probes, state.actors.len())
        };
        let more = probes.len() > limit;
        probes.truncate(limit);
        let actors: Vec<_> = probes
            .into_iter()
            .map(|(incarnation, name, actor_type, started_us, probe)| {
                let (phase, restarts, restarted_us, diagnostics) = probe.probe();
                ActorDiagnostics {
                    incarnation,
                    name,
                    actor_type,
                    phase,
                    restarts,
                    started_us,
                    restarted_us,
                    diagnostics,
                }
            })
            .collect();
        let next = if more { actors.last().map(|a| a.incarnation) } else { None };
        Ok(ActorDiagnosticsPage { actors, total, next })
    }

    pub fn attach_extension(&self, extension: Arc<dyn SystemExtension>) -> Result<(), SystemError> {
        let mut state = self.0.state.lock();
        if self.0.closed.load(Ordering::Relaxed) {
            return Err(SystemError::Closed);
        }
        if state.extensions.len() >= MAX_EXTENSIONS {
            return Err(SystemError::ExtensionLimit);
        }
        state.extensions.push(extension);
        Ok(())
    }

    /// Registers what a start spawned, or stops it if the system closed meanwhile.
    fn register(&self, key: ActorIncarnation, entry: Registered) -> Result<(), SystemError> {
        let mut state = self.0.state.lock();
        // The name was claimed by `claim_name`: this start fills it or gives it back.
        if self.0.closed.load(Ordering::Relaxed) {
            if let Some(name) = &entry.name {
                state.names.remove(name);
            }
            drop(state);
            entry.lifecycle.stop();
            return Err(SystemError::Closed);
        }
        if let Some(name) = &entry.name {
            state.names.insert(name.clone(), Some(key));
        }
        let lifecycle = entry.lifecycle.clone();
        state.actors.insert(key, entry);
        state.started += 1;
        drop(state);
        let watcher: Weak<dyn ExitWatcher> = Arc::downgrade(&self.0) as _;
        lifecycle.watch(watcher);
        Ok(())
    }

    /// Gives back a name `claim_name` held for a start that spawned nothing.
    fn unclaim(&self, name: &Option<Arc<str>>, e: SpawnError) -> SystemError {
        if let Some(name) = name {
            self.0.state.lock().names.remove(name);
        }
        SystemError::Spawn(e)
    }

    pub async fn shutdown(&self) -> Result<SystemShutdown, SystemError> {
        self.shutdown_with_reason(LeaveReason::Requested).await
    }

    /// The first accepted caller fixes the reason. Shutdown runs detached: every caller gets
    /// the same report, and cancelling a waiter does not cancel the shutdown.
    pub async fn shutdown_with_reason(&self, reason: LeaveReason) -> Result<SystemShutdown, SystemError> {
        reason.validate()?;
        let mut rx = {
            let mut state = self.0.state.lock();
            match &state.report {
                Some(rx) => rx.clone(),
                None => {
                    self.0.closed.store(true, Ordering::Relaxed);
                    state.leave = Some(reason.clone());
                    let (tx, rx) = watch::channel(None);
                    state.report = Some(rx.clone());
                    let inner = self.0.clone();
                    self.0.tokio.spawn(async move {
                        let report = coordinate(&inner, reason).await;
                        tx.send_replace(Some(report));
                    });
                    rx
                }
            }
        };
        let report = rx.wait_for(Option::is_some).await.map_err(|_| SystemError::Closed)?;
        Ok(report.clone().expect("report present"))
    }
}

async fn coordinate(inner: &Arc<Inner>, leave: LeaveReason) -> SystemShutdown {
    let deadline = tokio::time::Instant::now() + inner.shutdown_timeout;
    // The extensions hand keys off, give leases up and leave discovery once the actors are gone, so actors that drain
    // slowly must not take all the time: they drain until a quarter is left, and are waited for until an eighth is,
    // so one that ends at its drain deadline is not reported still running.
    let (drained, joined) = (deadline - inner.shutdown_timeout / 4, deadline - inner.shutdown_timeout / 8);
    let until = |at: tokio::time::Instant| at.saturating_duration_since(tokio::time::Instant::now());
    let extensions = inner.state.lock().extensions.clone();
    let mut report = SystemShutdown {
        leave,
        actors: Vec::new(),
        still_running: Vec::new(),
        warnings: Vec::new(),
        failures: Vec::new(),
    };

    let leaving_budget = (inner.shutdown_timeout / 4).min(Duration::from_millis(250));
    let leaving_deadline = tokio::time::Instant::now() + leaving_budget;
    for ext in &extensions {
        if let Err(e) = guarded(|given| ext.leaving(&report.leave, given), until(leaving_deadline)).await {
            report.warnings.push(format!("{}: leaving: {e}", ext.name()));
        }
    }

    let actors: Vec<_> = inner.state.lock().actors.values().map(|r| r.lifecycle.clone()).collect();
    let clock = inner.runtime.clock().clone();
    for actor in &actors {
        actor.drain(Some(clock.now().saturating_add(until(drained))));
    }
    for actor in &actors {
        match tokio::time::timeout(until(joined), actor.join()).await {
            Ok(exit) => report.actors.push(exit),
            Err(_) => report.still_running.push(actor.incarnation()),
        }
    }
    inner.runtime.shutdown(Duration::ZERO);

    for (i, ext) in extensions.iter().enumerate().rev() {
        // An equal share of what is left, what one does not use passing on: one that hangs cannot take the others'.
        let budget = (until(deadline) / (i as u32 + 1)).max(Duration::from_millis(1));
        if let Err(e) = guarded(|given| ext.shutdown(given), budget).await {
            report.failures.push(format!("{}: shutdown: {e}", ext.name()));
        }
        if catch_unwind(AssertUnwindSafe(|| ext.close())).is_err() {
            report.failures.push(format!("{}: close panicked", ext.name()));
        }
    }
    report
}

// A panicking extension cannot skip the rest of the teardown: the hook call, every poll and
// the future's drop are each isolated (a drop cut short by cancelling the shutdown task, by tokio). The hook is given
// an eighth less than it is waited for, so one that gives up at its budget in a task of its own is not reported timed
// out.
async fn guarded(hook: impl FnOnce(Duration) -> ExtensionFuture, budget: Duration) -> Result<(), String> {
    let fut = catch_unwind(AssertUnwindSafe(|| hook(budget - budget / 8))).map_err(|_| "panicked".to_string())?;
    let mut fut = CatchPanic::boxed(fut);
    let outcome = tokio::time::timeout(budget, &mut fut).await;
    let _ = catch_unwind(AssertUnwindSafe(|| drop(fut)));
    match outcome {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err("panicked".into()),
        Err(_) => Err("timed out".into()),
    }
}

pub struct ActorBuilder<'s, A: Actor, S> {
    system: &'s ActorSystem,
    source: S,
    name: Option<String>,
    execution: Execution,
    mailbox: MailboxConfig<A::Message>,
    policy: Option<RestartPolicy>,
}

impl<A: Actor, S> ActorBuilder<'_, A, S> {
    /// 1..=256 bytes, no control characters, unique among running named actors.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn execution(mut self, execution: Execution) -> Self {
        self.execution = execution;
        self
    }

    pub fn mailbox(mut self, config: impl Into<MailboxConfig<A::Message>>) -> Self {
        let (tracer, fence, idle) = (self.mailbox.tracer.take(), self.mailbox.fence.take(), self.mailbox.idle);
        self.mailbox = config.into();
        self.mailbox.tracer = self.mailbox.tracer.take().or(tracer);
        self.mailbox.fence = self.mailbox.fence.take().or(fence);
        self.mailbox.idle = self.mailbox.idle.or(idle);
        self
    }

    /// Drains the actor, or each generation of a supervised one, once it has handled no message for `idle`, waiting
    /// as long again for its jobs. Timers and job results do not count. A drained generation is not restarted by the
    /// default policy.
    pub fn idle(mut self, idle: impl Into<Option<Duration>>) -> Self {
        self.mailbox.idle = idle.into();
        self
    }

    /// Stops the actor, or each generation of a supervised one, before it handles anything once `fence` lapses.
    pub fn fence(mut self, fence: impl Into<Option<Fence>>) -> Self {
        self.mailbox.fence = fence.into();
        self
    }

    /// Holds the name before anything is spawned; `register` fills it, `unclaim` gives it back.
    fn claim_name(&self) -> Result<Option<Arc<str>>, SystemError> {
        if self.system.0.closed.load(Ordering::Relaxed) {
            return Err(SystemError::Closed);
        }
        let Some(name) = &self.name else { return Ok(None) };
        if !crate::valid_text(name, crate::MAX_NAME) {
            return Err(SystemError::Configuration("invalid actor name"));
        }
        let name: Arc<str> = name.as_str().into();
        let mut state = self.system.0.state.lock();
        if state.names.contains_key(&name) {
            return Err(SystemError::NameTaken(name.to_string()));
        }
        state.names.insert(name.clone(), None);
        Ok(Some(name))
    }
}

impl<A: Actor + Send> ActorBuilder<'_, A, A> {
    /// Returns at once. `on_start` runs on the actor's worker and messages sent meanwhile wait for it;
    /// `ActorHandle::ready` waits for it, and a failed one ends the actor with `StartFailed`.
    pub fn start(self) -> Result<ActorHandle<A::Message>, SystemError> {
        let name = self.claim_name()?;
        let started_us = unix_micros();
        let spawned = self.system.0.runtime.spawn(self.source, self.execution, self.mailbox);
        let spawned = spawned.map_err(|e| self.system.unclaim(&name, e))?;
        self.system.register(
            spawned.address.incarnation(),
            Registered {
                name,
                actor_type: std::any::type_name::<A>(),
                lifecycle: spawned.lifecycle.clone(),
                address: spawned.address.0.clone(),
                started_us,
            },
        )?;
        Ok(self.system.handle(spawned.address, spawned.lifecycle))
    }
}

impl<A: Actor + Send, F: FnMut() -> A + Send + 'static> ActorBuilder<'_, A, F> {
    pub fn policy(mut self, policy: RestartPolicy) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Returns once the family is placed; the first generation may still be starting.
    pub fn start_supervised(self) -> Result<SupervisedHandle<A::Message>, SystemError> {
        let name = self.claim_name()?;
        let inner = &self.system.0;
        let mut mailbox = self.mailbox;
        mailbox.dead = Some((inner.dead.clone(), name.clone()));
        mailbox.events = Some(inner.events.clone());
        let policy = self.policy.unwrap_or_default();
        // Before the first generation runs, so a restart without backoff cannot come before it.
        let started_us = unix_micros();
        let family = inner.runtime.spawn_supervised(self.source, self.execution, mailbox, policy);
        let family = family.map_err(|e| self.system.unclaim(&name, e))?;
        let lifecycle = family.lifecycle();
        self.system.register(
            ActorIncarnation { generation: 0, ..lifecycle.incarnation() },
            Registered {
                name,
                actor_type: std::any::type_name::<A>(),
                lifecycle: lifecycle.clone(),
                address: Arc::new(family.clone()),
                started_us,
            },
        )?;
        Ok(SupervisedHandle { family, lifecycle, request_timeout: inner.request_timeout })
    }
}

pub struct ActorHandle<M: Send + 'static> {
    pub address: ActorRef<M>,
    pub lifecycle: LifecycleHandle,
    request_timeout: Duration,
    start_timeout: Duration,
}

impl<M: Send + 'static> Clone for ActorHandle<M> {
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            lifecycle: self.lifecycle.clone(),
            request_timeout: self.request_timeout,
            start_timeout: self.start_timeout,
        }
    }
}

impl<M: Send + 'static> std::fmt::Debug for ActorHandle<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ActorHandle({})", self.address.incarnation())
    }
}

impl<M: Send + 'static> ActorHandle<M> {
    /// Waits for `on_start`, up to the system start timeout. Timing out does not stop the actor.
    pub async fn ready(&self) -> Result<(), SystemError> {
        let ready = SpawnHandle::new(self.address.clone()).ready();
        tokio::time::timeout(self.start_timeout, ready)
            .await
            .map_err(|_| SystemError::Start(StartError::TimedOut))?
            .map_err(SystemError::Start)?;
        Ok(())
    }

    pub fn tell(&self, msg: impl Into<M>) -> Result<Enqueued, Reject<M>> {
        self.address.try_tell(msg.into())
    }

    /// Waits for capacity up to the system request timeout.
    pub async fn send(&self, msg: impl Into<M>) -> Result<Enqueued, Reject<M>> {
        let deadline = self.address.clock().now().saturating_add(self.request_timeout);
        self.address.send(msg.into(), Some(deadline)).await
    }

    pub async fn ask<R, T: Into<M>>(&self, build: impl FnOnce(ReplyTo<R>) -> T) -> Result<R, RequestError> {
        let deadline = self.address.clock().now().saturating_add(self.request_timeout);
        self.address.request(Some(deadline), |reply| build(reply).into()).await
    }

    pub async fn join(&self) -> Exit {
        self.lifecycle.join().await
    }
}

/// Routes to whichever generation is currently running. While a restart waits out its backoff, `tell` is refused with
/// `NotReady` and `send` and `ask` wait for the next generation, up to the request timeout.
pub struct SupervisedHandle<M: Send + 'static> {
    pub family: SupervisionHandle<M>,
    pub lifecycle: LifecycleHandle,
    request_timeout: Duration,
}

impl<M: Send + 'static> Clone for SupervisedHandle<M> {
    fn clone(&self) -> Self {
        Self { family: self.family.clone(), lifecycle: self.lifecycle.clone(), request_timeout: self.request_timeout }
    }
}

impl<M: Send + 'static> SupervisedHandle<M> {
    pub fn tell(&self, msg: impl Into<M>) -> Result<Enqueued, Reject<M>> {
        match self.family.current() {
            Some(address) => address.try_tell(msg.into()),
            None => Err(Reject { reason: RejectReason::NotReady, message: msg.into() }),
        }
    }

    pub async fn send(&self, msg: impl Into<M>) -> Result<Enqueued, Reject<M>> {
        let deadline = tokio::time::Instant::now() + self.request_timeout;
        let (mut message, mut after) = (msg.into(), 0);
        loop {
            let (address, until) = match self.reach(after, deadline).await {
                Ok(reached) => reached,
                Err(reason) => return Err(Reject { reason, message }),
            };
            match address.send(message, Some(until)).await {
                // That generation ended as it was reached: the next one takes it.
                Err(Reject { reason: RejectReason::Closed, message: back }) => {
                    (message, after) = (back, address.incarnation().generation)
                }
                sent => return sent,
            }
        }
    }

    /// As `send`, a generation that closes before it takes the request passes it to the next, so `build` may run more
    /// than once. Once taken, the request is not sent again.
    pub async fn ask<R, T: Into<M>>(&self, build: impl Fn(ReplyTo<R>) -> T) -> Result<R, RequestError> {
        let deadline = tokio::time::Instant::now() + self.request_timeout;
        let mut after = 0;
        loop {
            let (address, until) = self.reach(after, deadline).await.map_err(RequestError::NotAdmitted)?;
            match address.request(Some(until), |reply| build(reply).into()).await {
                Err(RequestError::NotAdmitted(RejectReason::Closed)) => after = address.incarnation().generation,
                answer => return answer,
            }
        }
    }

    /// The first generation after `after` that takes messages, and `deadline` in that actor's clock.
    async fn reach(&self, after: u64, deadline: tokio::time::Instant) -> Result<(ActorRef<M>, Duration), RejectReason> {
        let address = match self.family.current().filter(|a| a.incarnation().generation > after) {
            Some(running) => running,
            None => tokio::time::timeout_at(deadline, self.family.next(after))
                .await
                .map_err(|_| RejectReason::NotReady)??,
        };
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let until = address.clock().now().saturating_add(left);
        Ok((address, until))
    }

    /// Resolves when the family stops for good.
    pub async fn join(&self) -> Exit {
        self.lifecycle.join().await
    }
}
