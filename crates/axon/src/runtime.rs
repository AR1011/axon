use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::Waker;
use std::time::{Duration, Instant};

use crate::clock::{Clock, nanos};
use crate::context::Actor;
use crate::driver::{Driver, SpawnHandle, next_runtime_id};
use crate::jobs::JobExecutor;
use crate::lifecycle::LifecycleHandle;
use crate::mailbox::MailboxConfig;
use crate::supervision::{RestartPolicy, Supervised, SupervisionHandle};
use crate::sync::{self, Condvar, Mutex, fence};
#[cfg(axon_loom)]
use crate::sync::{LocalCell as _, LocalRefCell as _};
use crate::types::*;

/// `workers`, `cores`, `spin` and `turn_budget` are the shared pool's, and a dedicated actor's `spin` and
/// `turn_budget`.
#[derive(Clone)]
pub struct RuntimeConfig {
    pub workers: usize,
    /// As `PoolConfig::cores`.
    pub cores: Vec<usize>,
    pub max_actors: usize,
    pub turn_budget: TurnBudget,
    pub clock: Clock,
    /// None: a Runtime built directly runs no jobs; an ActorSystem runs them on its tokio runtime.
    pub job_executor: Option<JobExecutor>,
    /// How long an idle worker spins for new work before it sleeps. Waking a sleeping thread costs about this much.
    /// `Duration::MAX` never sleeps.
    pub spin: Duration,
    /// More pools, for `Execution::Pool`.
    pub pools: Vec<PoolConfig>,
    #[doc(hidden)]
    pub threadless: Option<Threadless>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            workers: std::thread::available_parallelism().map_or(1, |n| n.get()).min(8),
            cores: Vec::new(),
            max_actors: 65_536,
            turn_budget: TurnBudget::default(),
            clock: Clock::system(),
            job_executor: None,
            spin: SPIN,
            pools: Vec::new(),
            threadless: None,
        }
    }
}

impl RuntimeConfig {
    /// The cores the calling thread may run on, which `cores` and `PoolConfig::cores` must come from: under
    /// `taskset` or a container's cpuset they need not start at 0. Empty where the OS does not say.
    pub fn allowed_cores() -> Vec<usize> {
        core_affinity::get_core_ids().map_or_else(Vec::new, |ids| ids.iter().map(|c| c.id).collect())
    }
}

const SPIN: Duration = Duration::from_micros(20);

/// Worker threads of their own, for the actors spawned with `Execution::Pool(name)`.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Also names the threads `{name}-{i}`, of which Linux keeps 15 bytes.
    pub name: String,
    pub workers: usize,
    /// Worker `i` runs only on core `cores[i % cores.len()]` on Linux and Windows; macOS takes it as a hint at most.
    /// Empty leaves where to the OS. Each must be one the thread that builds the runtime may run on, one of
    /// `RuntimeConfig::allowed_cores()`.
    pub cores: Vec<usize>,
    /// As `RuntimeConfig::spin`.
    pub spin: Duration,
    /// Whether actors move between the pool's workers, as they do on the shared pool. Off, an actor runs only on
    /// the worker it was placed on, which keeps its cache warm on a pinned core, and a turn that blocks holds up the
    /// actors placed with it.
    pub balance: bool,
    pub turn_budget: TurnBudget,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            workers: 1,
            cores: Vec::new(),
            spin: SPIN,
            balance: true,
            turn_budget: TurnBudget::default(),
        }
    }
}

/// Runs every actor of an `ActorSystem` on one task of the tokio runtime it is built in, with that runtime's clock.
/// For simulators. Applications leave it `None`.
///
/// Every `Execution` shares the one queue, so pools are not isolated, and no worker, dedicated, watchdog, clock or job
/// thread is started.
#[doc(hidden)]
#[derive(Clone, Default)]
pub struct Threadless(Arc<Controls>);

#[derive(Default)]
struct Controls {
    runtime: OnceLock<std::sync::Weak<Inner>>,
    held: AtomicBool,
    /// Nanoseconds each turn takes.
    cost: AtomicU64,
    /// Nanoseconds after the clock's origin until which the turns charged so far keep the actors busy.
    busy: AtomicU64,
    /// A turn runs whose cost is not charged yet.
    owed: AtomicBool,
}

impl Threadless {
    /// While held, the pump runs no turns: a handler that never returns, or a stalled worker. Timers and I/O go on.
    pub fn hold(&self, held: bool) {
        self.0.held.store(held, Ordering::Relaxed);
        if let Some(rt) = self.runtime() {
            rt.pools[0].pool.queues[0].rouse();
        }
    }

    /// Virtual time each actor turn takes. It is charged at the turn's first clock read, so the reads after it see
    /// the turn done and a shorter `TurnBudget::max_elapsed` trips. The pump runs no turn until tokio's time is within
    /// a millisecond of the clock's, so what is below a millisecond carries to the next turn.
    pub fn turn_cost(&self, cost: Duration) {
        self.0.cost.store(nanos(cost), Ordering::Relaxed);
    }

    /// The clock when tokio's time is `elapsed` after its origin. The first read in a turn charges the turn's cost.
    pub(crate) fn now(&self, elapsed: Duration) -> Duration {
        let now = elapsed.max(Duration::from_nanos(self.0.busy.load(Ordering::Relaxed)));
        if self.0.owed.swap(false, Ordering::Relaxed) {
            let busy = now.saturating_add(Duration::from_nanos(self.0.cost.load(Ordering::Relaxed)));
            self.0.busy.store(nanos(busy), Ordering::Relaxed);
        }
        now
    }

    /// A crash: every actor is dropped as if it had stopped, so no `on_stop`, exit or child stop runs, and the queue,
    /// the registry and the clock's wakes are cleared, which frees what they held of each other. The pump ends.
    pub fn forsake(&self) {
        let Some(rt) = self.runtime() else { return };
        rt.stopped.store(true, Ordering::Release);
        let registry = std::mem::take(&mut *rt.registry.lock());
        let cells: Vec<_> = registry.into_values().filter_map(|(_, cell)| cell.upgrade()).collect();
        for cell in cells {
            let drive = cell.drive.lock().take();
            if let Some(mut drive) = drive {
                drive.forsake();
            }
            // Holds its place for good, so a later wake cannot queue it where no pump will take it.
            cell.scheduled.claim();
        }
        let queue = &rt.pools[0].pool.queues[0];
        queue.ready.lock().cells.clear();
        queue.shutdown();
        rt.config.clock.clear();
    }

    fn runtime(&self) -> Option<Arc<Inner>> {
        self.0.runtime.get()?.upgrade()
    }
}

/// What the pump task of a threadless runtime holds. It outlives the runtime until the queue is shut down and empty,
/// so a dropped system still runs its actors' stops, as the workers would.
pub(crate) struct Pump {
    threadless: Threadless,
    pool: Arc<Pool>,
    clock: Clock,
}

pub(crate) enum Parked {
    Run,
    /// Woken by a push, a release, the end, or a clock wake earlier than this one, which is when it must run again.
    /// While charged turns keep the actors busy, it is no later than their end, and no turn runs before it.
    Until(Option<Duration>),
    End,
}

impl Pump {
    /// Wakes what is due on the clock and runs up to 64 turns, unless held or `busy`. Tokio's time is `elapsed` after
    /// the clock's origin. Returns how many it ran.
    pub(crate) fn run(&self, elapsed: impl Fn() -> Duration) -> usize {
        self.clock.fire(self.due(elapsed()));
        let queue = &self.pool.queues[0];
        let charged = self.threadless.0.cost.load(Ordering::Relaxed) > 0;
        let mut ran = 0;
        while ran < 64
            && !self.threadless.0.held.load(Ordering::Relaxed)
            && (!charged || self.busy(elapsed()).is_none())
        {
            let Some(cell) = queue.pop() else { break };
            ran += 1;
            queue.owner.turns.fetch_add(1, Ordering::Relaxed);
            self.threadless.0.owed.store(charged, Ordering::Relaxed);
            if catch_unwind(AssertUnwindSafe(|| cell.run())).unwrap_or(false) {
                queue.push(cell);
            }
            if charged {
                // Charges a turn that read no clock.
                self.threadless.now(elapsed());
            }
        }
        ran
    }

    /// Until when after the clock's origin charged turns keep the actors busy, if that is a whole millisecond or more
    /// past tokio's time at `elapsed`. They run again once tokio's time is within a millisecond of it: the rest carries.
    fn busy(&self, elapsed: Duration) -> Option<Duration> {
        let ahead = Duration::from_nanos(self.threadless.0.busy.load(Ordering::Relaxed)).saturating_sub(elapsed);
        let whole = ahead - Duration::from_nanos(u64::from(ahead.subsec_nanos() % 1_000_000));
        (!whole.is_zero()).then(|| elapsed + whole)
    }

    /// How far the clock's wakes are due. While the actors are busy the clock reads ahead of tokio's time, and a
    /// caller's deadline still fires on tokio's time, as the clock thread fires it while a worker is busy.
    fn due(&self, elapsed: Duration) -> Duration {
        if self.busy(elapsed).is_some() { elapsed } else { self.clock.now() }
    }

    pub(crate) fn park(&self, pump: &Waker, elapsed: Duration) -> Parked {
        let busy = self.busy(elapsed);
        {
            let mut ready = self.pool.queues[0].ready.lock();
            if ready.shutdown && ready.cells.is_empty() {
                return Parked::End;
            }
            if !ready.cells.is_empty() && !self.threadless.0.held.load(Ordering::Relaxed) && busy.is_none() {
                return Parked::Run;
            }
            (ready.sleeping, ready.notified, ready.pump) = (true, false, Some(pump.clone()));
        }
        // A due wake runs now: a timer for it would wait for tokio's next millisecond tick.
        match (self.clock.park(pump), busy) {
            (Some(at), _) if at <= self.due(elapsed) => Parked::Run,
            (at, Some(busy)) => Parked::Until(Some(at.map_or(busy, |at| at.min(busy)))),
            (at, None) => Parked::Until(at),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Execution {
    /// On the shared workers, which pass actors between them.
    #[default]
    Shared,
    /// Its own OS thread.
    Dedicated,
    /// On the workers of the `RuntimeConfig::pools` entry of this name.
    Pool(&'static str),
}

/// What a cell runs: a `Driver`, or a `Supervised` family of them.
pub(crate) trait Drive: Send {
    /// Returns `(runnable, stopped)`.
    fn turn(&mut self, budget: TurnBudget) -> (bool, bool);
    /// A turn unwound outside a handler guard: end the actor as panicked before it is dropped.
    fn abandon(&mut self);
    /// What a turn is owed for right now, if anything. Asked only of an actor neither scheduled nor running, where
    /// any answer is a lost wakeup.
    fn owed(&self) -> Option<&'static str>;
    /// A crash: dropped as if it had stopped, so nothing of its ending runs and no exit is published.
    fn forsake(&mut self);
}

#[derive(Default)]
struct Ready {
    cells: VecDeque<Arc<Cell>>,
    sleeping: bool,
    /// Set by whoever wakes the sleeping owner, so a wake that lands before the wait is not lost.
    notified: bool,
    shutdown: bool,
    /// A threadless runtime's parked pump, which a wake of the sleeping owner wakes.
    pump: Option<Waker>,
}

impl Ready {
    /// True for the one caller that wakes the sleeping owner; the rest find it notified.
    fn claim_wake(&mut self) -> bool {
        let woke = self.sleeping && !self.notified;
        self.notified |= woke;
        woke
    }
}

/// One worker's queue. The owner takes from the front; a worker with nothing to do takes the older half.
/// Aligned so that neighbouring queues in the pool never share a cache line.
#[repr(align(128))]
struct Queue {
    ready: Mutex<Ready>,
    wake: Condvar,
    queued: Queued,
    owner: Owner,
    parks: AtomicU64,
    // Busy time is the time since `origin` not spent asleep, so a working or spinning worker never reads the clock.
    origin: Instant,
    idle_ns: AtomicU64,
    /// Nanoseconds after `origin` at which the current sleep began; `0` while awake.
    idle_since: AtomicU64,
}

/// `ready.cells.len()` and the thefts from it, set under `ready`; read lock-free by the owner and idle workers. Off the
/// lock's cache line, so a spinning owner does not pull it away from the sender about to take the lock.
#[repr(align(64))]
struct Queued {
    len: sync::AtomicUsize,
    thefts: sync::AtomicU64,
}

/// Written by the owner every turn, so kept off the cache line that pushers lock.
#[repr(align(128))]
struct Owner {
    /// `BUSY`, `LOOKING` or `ASLEEP`. Idle workers never take from a queue whose owner is looking.
    state: sync::AtomicU8,
    turns: sync::AtomicU64,
}

const BUSY: u8 = 0;
const LOOKING: u8 = 1;
const ASLEEP: u8 = 2;

impl Queue {
    #[expect(clippy::disallowed_methods, reason = "busy and idle stats, never in a simulation's trace")]
    fn new() -> Self {
        Self {
            ready: Mutex::default(),
            wake: Condvar::new(),
            queued: Queued { len: sync::AtomicUsize::new(0), thefts: sync::AtomicU64::new(0) },
            owner: Owner { state: sync::AtomicU8::new(LOOKING), turns: sync::AtomicU64::new(0) },
            parks: AtomicU64::new(0),
            origin: Instant::now(),
            idle_ns: AtomicU64::new(0),
            idle_since: AtomicU64::new(0),
        }
    }

    /// Caller holds `ready`, which the worker also holds when it moves the idle figures, so the two agree.
    #[expect(clippy::disallowed_methods, reason = "busy and idle stats, never in a simulation's trace")]
    fn busy(&self) -> Duration {
        let (now, since) = (self.origin.elapsed().as_nanos() as u64, self.idle_since.load(Ordering::Relaxed));
        let idle = self.idle_ns.load(Ordering::Relaxed) + if since == 0 { 0 } else { now.saturating_sub(since) };
        Duration::from_nanos(now.saturating_sub(idle))
    }

    fn push(&self, cell: Arc<Cell>) {
        let woke = {
            let mut ready = self.ready.lock();
            ready.cells.push_back(cell);
            self.queued.len.store(ready.cells.len(), Ordering::Relaxed);
            ready.claim_wake().then(|| ready.pump.take())
        };
        if let Some(pump) = woke {
            self.wake.notify_one();
            if let Some(pump) = pump {
                pump.wake();
            }
        }
    }

    fn queued(&self) -> bool {
        self.queued.len.load(Ordering::Relaxed) > 0
    }

    /// Whether an idle worker should take from this queue, given its owner's turns and its thefts at the worker's last
    /// look. Moving an actor costs its cache, so only when that clearly pays: the owner is asleep, and waking it takes
    /// longer than a spinning worker needs to find the work; or it is stuck in one turn, or has more waiting than
    /// it will get through soon.
    ///
    /// A theft is a move too, so the other workers look once more before they take what queues after it, however deep:
    /// behind a turn that sends to many idle actors, each of them then gets its messages in a few turns, not one turn
    /// each.
    fn worth_stealing(&self, seen: &mut (u64, u64)) -> bool {
        let queued = self.queued.len.load(Ordering::Relaxed);
        // Not read while there is nothing to take: a busy owner writes `turns` every turn. The look before stays in
        // `seen`, and turns and thefts that have not moved since mean the owner is still in that turn.
        if queued == 0 {
            return false;
        }
        let now = (self.owner.turns.load(Ordering::Relaxed), self.queued.thefts.load(Ordering::Relaxed));
        let before = std::mem::replace(seen, now);
        match self.owner.state.load(Ordering::Relaxed) {
            ASLEEP => true,
            BUSY => before == now || (before.1 == now.1 && queued >= STEAL_DEPTH),
            _ => false,
        }
    }

    fn pop(&self) -> Option<Arc<Cell>> {
        if !self.queued() {
            return None;
        }
        let mut ready = self.ready.lock();
        let cell = ready.cells.pop_front();
        self.queued.len.store(ready.cells.len(), Ordering::Relaxed);
        cell
    }

    /// Moves the older half into `into`. The queue keeps its buffer: one left the remainder in a new one, which the
    /// busy owner grew again by doubling after every theft.
    fn take_half(&self, into: &mut VecDeque<Arc<Cell>>) {
        // Thieves never queue on the lock: one that finds it held leaves the queue to whoever holds it.
        let Some(mut ready) = self.ready.try_lock() else { return };
        let half = ready.cells.len().div_ceil(2);
        if half == 0 {
            return;
        }
        into.extend(ready.cells.drain(..half));
        self.queued.len.store(ready.cells.len(), Ordering::Relaxed);
        self.queued.thefts.store(self.queued.thefts.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
    }

    fn wake_if_asleep(&self) {
        // A sleeper's lock is held only briefly, and a wake lost here could leave no worker watching.
        let woke = self.ready.lock().claim_wake();
        if woke {
            self.wake.notify_one();
        }
    }

    fn shutdown(&self) {
        self.ready.lock().shutdown = true;
        self.wake.notify_all();
        self.rouse();
    }

    fn rouse(&self) {
        let pump = self.ready.lock().pump.take();
        if let Some(pump) = pump {
            pump.wake();
        }
    }

    /// Asleep with cells queued: a wake that was never sent, or never arrived. Which sleep, as a worker woken and not
    /// yet run by a loaded machine looks the same, and may be in a new sleep at every look.
    fn stalled(&self) -> Option<u64> {
        let ready = self.ready.lock();
        (ready.sleeping && !ready.shutdown && !ready.cells.is_empty()).then(|| self.parks.load(Ordering::Relaxed))
    }
}

/// Workers that share actors. A cell woken by a turn runs next on that turn's worker, where what it was sent is
/// still in cache, unless something already waits there, when it queues behind it; one woken from outside goes to
/// its home worker's queue; a worker with nothing to do takes half of another's queue.
///
/// Every queue's owner is awake whenever something is added to it, except by a push from another thread, which
/// wakes it. So `nudge` only buys parallelism, and one it misses costs no progress, only a wait for what queues
/// behind a turn that does not end.
///
/// Without `balance` none of that: a cell runs only on its home worker.
struct Pool {
    queues: Vec<Queue>,
    sleepers: Sleepers,
    balance: bool,
    /// The shared pool of a threaded runtime with no pinned cores, whose actors may take a turn on an asking thread
    /// (`run_here`). Not a named pool, whose actors are kept on its own workers, nor workers pinned to cores.
    here: bool,
}

/// Change only when a worker sleeps or wakes, so every push can afford to read them. Apart from `queues`, which
/// every push reads.
///
/// The handshake's atomics (these, `Owner`, `Queued`) are `sync::` so the loom model in `runtime/model.rs` sees them;
/// the stats stay std to keep it small.
#[repr(align(128))]
struct Sleepers {
    asleep: sync::AtomicUsize,
    /// Of those, the ones that wake now and then to look for a turn that does not end: one is enough.
    watching: sync::AtomicUsize,
}

/// What a worker that looks for work keeps between looks: each queue's turns and thefts at its last look, and a buffer
/// for what it takes, kept so that a theft allocates nothing.
struct Thief {
    seen: Vec<(u64, u64)>,
    taken: VecDeque<Arc<Cell>>,
}

/// A busy worker's queue at least this deep is shared with idle workers even while its turns keep moving.
const STEAL_DEPTH: usize = 32;

/// How often a watching sleeper looks for actors queued behind a turn that does not end, while any worker is busy.
const LOOK_AGAIN: Duration = Duration::from_millis(1);

sync::thread_local! {
    /// The pool and index of the worker on this thread, `(0, 0)` elsewhere, `(CATCHING, 0)` in `run_here`'s send. Plain
    /// data, so a sender from outside the pool pays one load to find out.
    static WORKER: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
    /// A cell woken by a turn on this worker, to run as soon as that turn ends. Off the workers, the shared pool's cell
    /// woken by `run_here`'s send, for its turn to run here, not queued.
    static NEXT: RefCell<Option<Arc<Cell>>> = const { RefCell::new(None) };
}

/// Two turns in a row on an asking thread that take longer than `HERE_MAX` keep their actor on the workers for the
/// next 250 asks that would run it there, `HERE_SKIP` down to `HERE_FRESH`. `HERE_MAX` is about what an ask handed to
/// a worker costs the asking thread: a longer turn run there would keep its other tasks waiting longer, where many
/// askers share it, and their actors would run one after another on it, not side by side on the workers. Two, so that
/// one turn the machine preempted does not send a quick actor away.
///
/// An actor's first such turn, the turn after a slow one, and its first after the skip are timed, and then one in
/// `HERE_TIMED` at random: a clock read costs about a sixth of a quick ask, and a thread that asks 16 actors in turn
/// would time the same ones every time. `Cell::skip_here` says which: 0 runs
/// here untimed, `HERE_FRESH` and `HERE_STRUCK` timed, the second after one slow turn, and above them it counts down
/// the asks left to skip.
const HERE_TIMED: u64 = 16;
const HERE_MAX: Duration = Duration::from_nanos(500);
const HERE_SKIP: u8 = HERE_FRESH + 250;
const HERE_STRUCK: u8 = 1;
const HERE_FRESH: u8 = 2;

/// `WORKER`'s pool while an ask from outside the pools that would wait on this thread sends. Not a flag of its own,
/// which every wake from outside a pool would read: one such read made a least-loaded router's tells 10 % slower.
const CATCHING: usize = 1;

/// Whether this thread is a pool's worker, whose time is its actors'.
pub(crate) fn on_worker() -> bool {
    WORKER.get().0 > CATCHING
}

/// Runs `send`, then here, on this thread, the turn of the actor it woke, if that was an idle actor of the shared pool.
/// For an ask from outside the pools whose thread would wait for the answer anyway: the turn then crosses no thread
/// either way, where on a worker it crosses two, each a few cache lines from one core to another. On a tokio runtime's
/// thread its other tasks wait for the turn, as for each turn of a threadless runtime: one quick turn.
pub(crate) fn run_here<T>(send: impl FnOnce() -> T) -> T {
    struct Catching;
    impl Drop for Catching {
        fn drop(&mut self) {
            WORKER.set((0, 0));
            // A send that unwound: what it woke still gets its turn.
            if let Some(cell) = NEXT.take() {
                cell.requeue();
            }
        }
    }
    // A request polled on another thread than the one that made it.
    if on_worker() {
        return send();
    }
    WORKER.set((CATCHING, 0));
    let catching = Catching;
    let sent = send();
    let caught = NEXT.take();
    drop(catching);
    if let Some(cell) = caught {
        cell.run_here();
    }
    sent
}

impl Pool {
    fn new(workers: usize, balance: bool, here: bool) -> Arc<Self> {
        Arc::new(Self {
            queues: (0..workers.max(1)).map(|_| Queue::new()).collect(),
            sleepers: Sleepers { asleep: sync::AtomicUsize::new(0), watching: sync::AtomicUsize::new(0) },
            balance,
            here,
        })
    }

    fn id(&self) -> usize {
        self as *const Self as usize
    }

    fn wake(&self, cell: Arc<Cell>, home: usize) {
        let (pool, me) = WORKER.get();
        if pool != self.id() || (!self.balance && home != me) {
            if pool == CATCHING && self.here && cell.may_run_here() {
                WORKER.set((0, 0));
                return NEXT.set(Some(cell));
            }
            // The home worker is woken if asleep. Reading how busy it is would cost the sender a cache miss on
            // every wake; with balance, a worker looking for work takes it if the home worker stays busy or asleep.
            return self.queues[home].push(cell);
        }
        // The slot only while nothing waits here, so a woken cell never jumps the queue, and a sender's messages
        // still pile up for one turn of their receiver.
        let queue = &self.queues[me];
        if !queue.queued() && NEXT.with_borrow(Option::is_none) {
            return NEXT.set(Some(cell));
        }
        queue.push(cell);
        self.nudge();
    }

    /// Wakes a sleeping worker to take what a busy worker just queued, unless one is already looking.
    fn nudge(&self) {
        if !self.balance {
            return;
        }
        // Pairs with the fence in `idle`: a worker going to sleep either is counted here or sees what was queued.
        fence(Ordering::SeqCst);
        if self.sleepers.asleep.load(Ordering::Relaxed) == 0 {
            return;
        }
        let mut asleep = None;
        for q in &self.queues {
            match q.owner.state.load(Ordering::Relaxed) {
                LOOKING => return,
                ASLEEP => _ = asleep.get_or_insert(q),
                _ => {}
            }
        }
        if let Some(queue) = asleep {
            queue.wake_if_asleep();
        }
    }

    fn shutdown(&self) {
        self.queues.iter().for_each(Queue::shutdown);
    }

    fn work(self: Arc<Self>, me: usize, spin: Duration) {
        WORKER.set((self.id(), me));
        let (queue, mut hit, mut busy) = (&self.queues[me], false, false);
        let mut thief = Thief { seen: vec![(0, 0); self.queues.len()], taken: VecDeque::new() };
        loop {
            let Some(cell) = self.next(me) else {
                busy = false;
                if self.idle(me, spin, &mut hit, &mut thief) {
                    continue;
                }
                break;
            };
            if !busy {
                busy = true;
                queue.owner.state.store(BUSY, Ordering::Relaxed);
                // This turn may not end, and no sender wakes a sleeper for what queues behind it, so one must be
                // watching. Pairs with the fence in `idle`: a sleeper either is counted here or sees this worker busy.
                if self.balance {
                    fence(Ordering::SeqCst);
                    if self.sleepers.watching.load(Ordering::Relaxed) == 0 {
                        self.nudge();
                    }
                }
            }
            // A cell that is still runnable runs again here unless something else waits, so it does not move.
            loop {
                queue.owner.turns.store(queue.owner.turns.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
                if !catch_unwind(AssertUnwindSafe(|| cell.run())).unwrap_or(false) {
                    break;
                }
                if queue.queued() || NEXT.with_borrow(Option::is_some) {
                    queue.push(cell);
                    self.nudge();
                    break;
                }
            }
        }
        WORKER.set((0, 0));
        NEXT.take();
    }

    fn next(&self, me: usize) -> Option<Arc<Cell>> {
        NEXT.take().or_else(|| self.queues[me].pop())
    }

    /// Moves half of a queue worth taking into this worker's own. True when it took any.
    fn steal(&self, me: usize, thief: &mut Thief) -> bool {
        if !self.balance {
            return false;
        }
        let n = self.queues.len();
        (1..n).map(|k| (me + k) % n).any(|v| {
            let victim = &self.queues[v];
            if !victim.worth_stealing(&mut thief.seen[v]) {
                return false;
            }
            victim.take_half(&mut thief.taken);
            !thief.taken.is_empty() && {
                let mut ready = self.queues[me].ready.lock();
                ready.cells.append(&mut thief.taken);
                self.queues[me].queued.len.store(ready.cells.len(), Ordering::Relaxed);
                true
            }
        })
    }

    /// Spins, then sleeps, until there may be work. False once the pool is shut down and this queue is empty.
    /// With a `spin` of `Duration::MAX`, never sleeps, and looks for a shutdown every `LOOK_AGAIN`.
    #[expect(clippy::disallowed_methods, reason = "a worker's spin limit and idle stats")]
    fn idle(&self, me: usize, spin: Duration, hit: &mut bool, thief: &mut Thief) -> bool {
        let state = &self.queues[me].owner.state;
        if state.load(Ordering::Relaxed) != LOOKING {
            state.store(LOOKING, Ordering::Relaxed);
        }
        if !spin.is_zero() {
            // Work that arrived during the last spin is likely to again, so that spin may run longer.
            let limit = if spin == Duration::MAX {
                LOOK_AGAIN
            } else if *hit {
                spin.saturating_mul(5)
            } else {
                spin
            };
            let (mut spins, mut since) = (0u32, None);
            *hit = loop {
                // Other queues less often: every read of one competes with its pushers for the line.
                if self.queues[me].queued() || (spins % 256 == 0 && self.steal(me, thief)) {
                    break true;
                }
                std::hint::spin_loop();
                spins += 1;
                // The clock is first read here, so work that arrives at once costs no clock read.
                if spins % 256 == 0 && since.get_or_insert_with(Instant::now).elapsed() >= limit {
                    break false;
                }
            };
            if *hit {
                return true;
            }
        }
        let queue = &self.queues[me];
        {
            let mut ready = queue.ready.lock();
            if !ready.cells.is_empty() {
                return true;
            }
            if ready.shutdown {
                return false;
            }
            if spin == Duration::MAX {
                return true;
            }
            (ready.sleeping, ready.notified) = (true, false);
            queue.parks.fetch_add(1, Ordering::Relaxed);
        }
        state.store(ASLEEP, Ordering::Relaxed);
        self.sleepers.asleep.fetch_add(1, Ordering::Relaxed);
        let mut ready = loop {
            // A sender from outside wakes only the home worker, which may be stuck in one long turn with actors
            // queued behind it. So while any worker is busy, one sleeper looks again now and then.
            fence(Ordering::SeqCst);
            let busy = self.balance && self.queues.iter().any(|q| q.owner.state.load(Ordering::Relaxed) == BUSY);
            let found = self.steal(me, thief);
            let mut ready = queue.ready.lock();
            if found || !ready.cells.is_empty() || ready.notified || ready.shutdown {
                break ready;
            }
            let since = queue.origin.elapsed().as_nanos() as u64;
            queue.idle_since.store(since.max(1), Ordering::Relaxed);
            if busy && self.sleepers.watching.load(Ordering::Relaxed) == 0 {
                self.sleepers.watching.fetch_add(1, Ordering::Relaxed);
                queue.wake.wait_for(&mut ready, LOOK_AGAIN);
                self.sleepers.watching.fetch_sub(1, Ordering::Relaxed);
            } else {
                queue.wake.wait(&mut ready);
            }
            let slept = (queue.origin.elapsed().as_nanos() as u64).saturating_sub(since);
            queue.idle_ns.fetch_add(slept, Ordering::Relaxed);
            queue.idle_since.store(0, Ordering::Relaxed);
        };
        ready.sleeping = false;
        drop(ready);
        self.sleepers.asleep.fetch_sub(1, Ordering::Relaxed);
        state.store(LOOKING, Ordering::Relaxed);
        true
    }
}

/// A cell's place in its run queue: at most one at a time.
///
/// SeqCst pairs with the flag stores in the mailbox: a stop, drain or job result flagged while a
/// turn runs is either seen by that turn or gets a turn of its own, never neither. Release/Acquire
/// is a store-buffering race: the flag's thread reads the place still taken and the turn that just
/// freed it reads the flag unset. The loom models in `mailbox/model.rs` hold that for a drain and a
/// job result; the swap is SeqCst too because loom cannot model the AcqRel form, and on x86 and
/// AArch64 the two compile the same.
pub(crate) struct Scheduled(sync::AtomicBool);

impl Scheduled {
    pub(crate) fn new() -> Self {
        Self(sync::AtomicBool::new(false))
    }

    /// True when the caller took the place and must push the cell.
    pub(crate) fn claim(&self) -> bool {
        !self.0.load(Ordering::SeqCst) && !self.0.swap(true, Ordering::SeqCst)
    }

    /// The turn starts: the place is free for the next wake, which the turn may or may not see.
    pub(crate) fn begin(&self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

struct Cell {
    scheduled: Scheduled,
    /// Whether an asking thread may run its turn, and whether timed, see `HERE_SKIP`.
    skip_here: AtomicU8,
    drive: Mutex<Option<Box<dyn Drive>>>,
    pool: Arc<Pool>,
    /// The worker whose queue takes this cell's wakes from outside the pool.
    home: usize,
    budget: TurnBudget,
    slot: u64,
    dedicated: bool,
    runtime: std::sync::Weak<Inner>,
}

impl std::task::Wake for Cell {
    fn wake(self: Arc<Self>) {
        self.notify();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}

impl Cell {
    fn notify(self: &Arc<Self>) {
        if self.scheduled.claim() {
            self.pool.wake(self.clone(), self.home);
        }
    }

    /// One turn. True when the cell is still runnable and the caller holds its place and must run it again.
    fn run(self: &Arc<Self>) -> bool {
        self.scheduled.begin();
        self.turn(self.drive.lock(), self.budget)
    }

    /// One turn of a single message on this thread, off the workers, for a cell whose place it holds. A cell whose last
    /// turn still runs on a worker is queued instead, as is one with more to do after the turn.
    #[expect(clippy::disallowed_methods, reason = "a turn's length on an asking thread, which is on the system clock")]
    fn run_here(self: Arc<Self>) {
        let again = match self.drive.try_lock() {
            Some(drive) => {
                let skip = self.skip_here.load(Ordering::Relaxed);
                let probe = skip != 0;
                let began = (probe || crate::trace::random_u64().is_multiple_of(HERE_TIMED)).then(Instant::now);
                self.scheduled.begin();
                // The asking thread's trace is not the actor's: on a worker none is current between messages.
                let budget = TurnBudget { max_messages: std::num::NonZeroUsize::MIN, ..self.budget };
                let again = crate::trace::enter(None, || self.turn(drive, budget));
                match began.map(|at| at.elapsed() > HERE_MAX) {
                    Some(true) => {
                        let next = if skip == HERE_STRUCK { HERE_SKIP } else { HERE_STRUCK };
                        self.skip_here.store(next, Ordering::Relaxed);
                    }
                    Some(false) if probe => self.skip_here.store(0, Ordering::Relaxed),
                    _ => {}
                }
                again
            }
            None => true,
        };
        if again {
            self.requeue();
        }
    }

    /// False for the asks that come while a long turn on an asking thread keeps it on the workers.
    fn may_run_here(&self) -> bool {
        let skip = self.skip_here.load(Ordering::Relaxed);
        if skip > HERE_FRESH {
            self.skip_here.store(skip - 1, Ordering::Relaxed);
        }
        skip <= HERE_FRESH
    }

    /// To its home worker's queue, for a cell whose place this thread holds.
    fn requeue(self: Arc<Self>) {
        let pool = self.pool.clone();
        pool.queues[self.home].push(self);
    }

    fn turn(&self, mut drive: sync::MutexGuard<'_, Option<Box<dyn Drive>>>, budget: TurnBudget) -> bool {
        let Some(d) = drive.as_mut() else { return false };
        // A turn that unwinds leaves the driver in an unknown state. It is never run again: the actor
        // ends, its Exit is published, and a dedicated thread is released.
        let (runnable, stopped) = catch_unwind(AssertUnwindSafe(|| d.turn(budget))).unwrap_or_else(|_| {
            let _ = catch_unwind(AssertUnwindSafe(|| d.abandon()));
            (false, true)
        });
        if stopped {
            let finished = drive.take();
            drop(drive);
            let _ = catch_unwind(AssertUnwindSafe(|| drop(finished)));
            if let Some(rt) = self.runtime.upgrade() {
                rt.registry.lock().remove(&self.slot);
                rt.actors.fetch_sub(1, Ordering::AcqRel);
            }
            if self.dedicated {
                self.pool.shutdown();
            }
            return false;
        }
        drop(drive);
        runnable && self.scheduled.claim()
    }

    /// Why nothing will run this cell's next turn, if it is owed one: it is not scheduled, or it is and every worker
    /// that could run it sleeps. `None` while a turn runs.
    fn stalled(&self) -> Option<String> {
        let drive = self.drive.try_lock()?;
        let drive = drive.as_ref()?;
        if !self.scheduled.0.load(Ordering::SeqCst) {
            return drive.owed().map(|what| format!("has {what} to handle and is not scheduled"));
        }
        let asleep = self.pool.queues.iter().all(|q| q.owner.state.load(Ordering::Relaxed) == ASLEEP);
        let sleeps: u64 = self.pool.queues.iter().map(|q| q.parks.load(Ordering::Relaxed)).sum();
        asleep.then(|| format!("is scheduled and every worker of its pool sleeps (sleeps {sleeps})"))
    }
}

/// A pool and what placing on it needs.
struct Placed {
    name: String,
    pool: Arc<Pool>,
    budget: TurnBudget,
    next_worker: AtomicUsize,
}

impl Placed {
    /// `here`: the shared pool, with no pinned cores, see `Pool::here`.
    fn start(config: PoolConfig, available: &[usize], here: bool) -> Result<Self, SpawnError> {
        let cores = &config.cores;
        if !cores.iter().all(|core| available.contains(core)) {
            return Err(SpawnError::InvalidPool("a core this process may not run on"));
        }
        let pool = Pool::new(config.workers, config.balance, here);
        for i in 0..pool.queues.len() {
            let (worker, spin) = (pool.clone(), config.spin);
            let core = (!cores.is_empty()).then(|| cores[i % cores.len()]);
            #[expect(clippy::disallowed_methods, reason = "the worker threads; a threadless runtime starts none")]
            let started = std::thread::Builder::new().name(format!("{}-{i}", config.name)).spawn(move || {
                // Checked above against the cores this process may use.
                if let Some(id) = core {
                    core_affinity::set_for_current(core_affinity::CoreId { id });
                }
                worker.work(i, spin)
            });
            if let Err(e) = started {
                pool.shutdown();
                return Err(SpawnError::Thread(e));
            }
        }
        Ok(Self { name: config.name, pool, budget: config.turn_budget, next_worker: AtomicUsize::new(0) })
    }
}

struct Inner {
    id: u64,
    config: RuntimeConfig,
    /// The shared pool first, then `config.pools`.
    pools: Vec<Placed>,
    next_slot: AtomicU64,
    actors: AtomicUsize,
    stopped: AtomicBool,
    registry: Mutex<BTreeMap<u64, (LifecycleHandle, std::sync::Weak<Cell>)>>,
}

impl Inner {
    fn stop_all(&self) -> Vec<LifecycleHandle> {
        self.stopped.store(true, Ordering::Release);
        let handles: Vec<_> = self.registry.lock().values().map(|(h, _)| h.clone()).collect();
        handles.iter().for_each(|h| h.stop_with(StopReason::RuntimeShutdown));
        handles
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.stop_all();
        self.pools.iter().for_each(|p| p.pool.shutdown());
    }
}

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct WorkerStats {
    pub turns: u64,
    #[cfg_attr(feature = "serde", serde(rename = "busy_ns", serialize_with = "crate::ser::ns"))]
    pub busy: Duration,
    pub parks: u64,
    pub queued: usize,
}

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct RuntimeStats {
    pub actors: usize,
    /// The shared pool's.
    pub workers: Vec<WorkerStats>,
    pub pools: Vec<PoolStats>,
}

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct PoolStats {
    pub name: String,
    pub workers: Vec<WorkerStats>,
}

#[derive(Debug)]
pub struct ShutdownReport {
    pub exits: Vec<Exit>,
    pub still_running: Vec<ActorIncarnation>,
}

/// Thread-per-worker scheduler for synchronous actors. Dropping the last clone stops every
/// actor without waiting; use `shutdown` to wait.
#[derive(Clone)]
pub struct Runtime(Arc<Inner>);

impl Runtime {
    pub fn new(config: RuntimeConfig) -> Result<Self, SpawnError> {
        if config.threadless.is_some() {
            return Err(SpawnError::InvalidPool("only an ActorSystem drives a threadless runtime"));
        }
        Self::start(config)
    }

    pub(crate) fn start(config: RuntimeConfig) -> Result<Self, SpawnError> {
        let names: Vec<_> = config.pools.iter().map(|p| p.name.as_str()).collect();
        if names.iter().enumerate().any(|(i, n)| n.is_empty() || n.contains('\0') || names[..i].contains(n)) {
            return Err(SpawnError::InvalidPool("pool names must be distinct, not empty and without NUL"));
        }
        let shared = PoolConfig {
            name: "axon-worker".into(),
            workers: config.workers,
            cores: config.cores.clone(),
            spin: config.spin,
            balance: true,
            turn_budget: config.turn_budget,
        };
        let available = RuntimeConfig::allowed_cores();
        let threadless = config.threadless.is_some().then(|| Pool::new(1, true, false));
        let mut pools: Vec<Placed> = Vec::new();
        for pool in std::iter::once(shared).chain(config.pools.iter().cloned()) {
            // Turns on asking threads would leave cores pinned to the workers.
            let here = pools.is_empty() && pool.cores.is_empty();
            let placed = match &threadless {
                Some(one) => {
                    let next_worker = AtomicUsize::new(0);
                    Ok(Placed { name: pool.name, pool: one.clone(), budget: pool.turn_budget, next_worker })
                }
                None => Placed::start(pool, &available, here),
            };
            match placed {
                Ok(placed) => pools.push(placed),
                Err(e) => {
                    pools.iter().for_each(|p| p.pool.shutdown());
                    return Err(e);
                }
            }
        }
        let inner = Arc::new(Inner {
            id: next_runtime_id(),
            config,
            pools,
            next_slot: AtomicU64::new(1),
            actors: AtomicUsize::new(0),
            stopped: AtomicBool::new(false),
            registry: Mutex::default(),
        });
        if let Some(threadless) = &inner.config.threadless {
            let bound = threadless.0.runtime.set(Arc::downgrade(&inner));
            bound.map_err(|_| SpawnError::InvalidPool("a Threadless drives one runtime"))?;
        } else if cfg!(debug_assertions) {
            let watched = Arc::downgrade(&inner);
            // A diagnostic: a runtime without it still runs.
            #[expect(clippy::disallowed_methods, reason = "a debug diagnostic of the threaded runtime")]
            let _ = std::thread::Builder::new().name("axon-watchdog".into()).spawn(move || watch(watched));
        }
        Ok(Self(inner))
    }

    pub fn clock(&self) -> &Clock {
        &self.0.config.clock
    }

    pub(crate) fn job_executor(&self) -> Option<&JobExecutor> {
        self.0.config.job_executor.as_ref()
    }

    /// A threadless runtime's pump, which the caller runs.
    pub(crate) fn pump(&self) -> Option<Pump> {
        let threadless = self.0.config.threadless.clone()?;
        Some(Pump { threadless, pool: self.0.pools[0].pool.clone(), clock: self.0.config.clock.clone() })
    }

    pub fn actor_count(&self) -> usize {
        self.0.actors.load(Ordering::Acquire)
    }

    pub fn stats(&self) -> RuntimeStats {
        let workers = |p: &Placed| {
            (p.pool.queues.iter())
                .map(|q| {
                    let ready = q.ready.lock();
                    WorkerStats {
                        turns: q.owner.turns.load(Ordering::Relaxed),
                        busy: q.busy(),
                        parks: q.parks.load(Ordering::Relaxed),
                        queued: ready.cells.len(),
                    }
                })
                .collect()
        };
        RuntimeStats {
            actors: self.actor_count(),
            workers: workers(&self.0.pools[0]),
            pools: (self.0.pools[1..].iter())
                .map(|p| PoolStats { name: p.name.clone(), workers: workers(p) })
                .collect(),
        }
    }

    fn place<D: Drive + 'static, H>(
        &self,
        execution: Execution,
        build: impl FnOnce(ActorIncarnation, Option<JobExecutor>, Waker) -> Result<(D, H, LifecycleHandle), SpawnError>,
    ) -> Result<H, SpawnError> {
        let inner = &self.0;
        if inner.stopped.load(Ordering::Acquire) {
            return Err(SpawnError::RuntimeStopped);
        }
        let placed = match execution {
            Execution::Pool(name) => (inner.pools[1..].iter().find(|p| p.name == name))
                .ok_or(SpawnError::InvalidPool("no pool of that name"))?,
            _ => &inner.pools[0],
        };
        let admitted = inner
            .actors
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| (n < inner.config.max_actors).then_some(n + 1));
        if admitted.is_err() {
            return Err(SpawnError::ActorLimit);
        }
        let slot = inner.next_slot.fetch_add(1, Ordering::Relaxed);
        let dedicated = execution == Execution::Dedicated && inner.config.threadless.is_none();
        let (pool, home) = if dedicated {
            (Pool::new(1, true, false), 0)
        } else {
            let n = placed.pool.queues.len();
            (placed.pool.clone(), placed.next_worker.fetch_add(1, Ordering::Relaxed) % n)
        };
        let cell = Arc::new(Cell {
            scheduled: Scheduled::new(),
            skip_here: AtomicU8::new(HERE_FRESH),
            drive: Mutex::new(None),
            pool: pool.clone(),
            home,
            budget: placed.budget,
            slot,
            dedicated,
            runtime: Arc::downgrade(inner),
        });
        let notify = Waker::from(cell.clone());
        let incarnation = ActorIncarnation { runtime: inner.id, slot, generation: 1 };
        let spin = inner.config.spin;
        let built = build(incarnation, inner.config.job_executor.clone(), notify).and_then(|built| {
            if dedicated {
                #[expect(clippy::disallowed_methods, reason = "a Dedicated actor's thread")]
                std::thread::Builder::new()
                    .name(format!("axon-dedicated-{slot}"))
                    .spawn(move || pool.work(0, spin))
                    .map_err(SpawnError::Thread)?;
            }
            Ok(built)
        });
        let (drive, handle, lifecycle) = match built {
            Ok(built) => built,
            Err(e) => {
                inner.actors.fetch_sub(1, Ordering::AcqRel);
                return Err(e);
            }
        };
        inner.registry.lock().insert(slot, (lifecycle.clone(), Arc::downgrade(&cell)));
        *cell.drive.lock() = Some(Box::new(drive));
        if inner.stopped.load(Ordering::Acquire) {
            lifecycle.stop_with(StopReason::RuntimeShutdown);
        }
        cell.notify();
        Ok(handle)
    }

    pub fn spawn<A: Actor + Send>(
        &self,
        actor: A,
        execution: Execution,
        config: impl Into<MailboxConfig<A::Message>>,
    ) -> Result<SpawnHandle<A::Message>, SpawnError> {
        let (config, clock) = (config.into(), self.0.config.clock.clone());
        self.place(execution, move |incarnation, jobs, notify| {
            let driver = Driver::new(actor, config, clock, incarnation, jobs, Some(notify))?;
            let handle = SpawnHandle::new(driver.address());
            let lifecycle = handle.lifecycle.clone();
            Ok((driver, handle, lifecycle))
        })
    }

    pub fn spawn_supervised<A: Actor + Send, F: FnMut() -> A + Send + 'static>(
        &self,
        factory: F,
        execution: Execution,
        config: impl Into<MailboxConfig<A::Message>>,
        policy: RestartPolicy,
    ) -> Result<SupervisionHandle<A::Message>, SpawnError> {
        let (config, clock) = (config.into(), self.0.config.clock.clone());
        self.place(execution, move |incarnation, jobs, notify| {
            let base = ActorIncarnation { generation: 0, ..incarnation };
            let (supervised, handle) = Supervised::build(factory, config, clock, policy, base, jobs, Some(notify))?;
            let lifecycle = handle.lifecycle();
            Ok((supervised, handle, lifecycle))
        })
    }

    /// Stops every actor and waits up to `wait` in total for them to exit.
    #[expect(clippy::disallowed_methods, reason = "a blocking wait for threads, zero on a threadless runtime")]
    pub fn shutdown(&self, wait: Duration) -> ShutdownReport {
        let deadline = Instant::now() + wait;
        let mut report = ShutdownReport { exits: Vec::new(), still_running: Vec::new() };
        for handle in self.0.stop_all() {
            match handle.wait(deadline.saturating_duration_since(Instant::now())) {
                Some(exit) => report.exits.push(exit),
                None => report.still_running.push(handle.incarnation()),
            }
        }
        self.0.pools.iter().for_each(|p| p.pool.shutdown());
        report
    }
}

/// How often the watchdog looks, and how many looks in a row must find the same stall.
const STALL_LOOK: Duration = Duration::from_millis(250);
const STALL_LOOKS: u32 = 8;

/// Debug builds: aborts when an actor owes a turn and is neither scheduled nor running, or a worker sleeps with actors
/// queued that nothing woke it for, at every look for two seconds. Either is a lost wakeup, which would otherwise show
/// only as a hang with no one to blame. A sleep counts only while it is the same sleep: a starved machine runs woken
/// workers late, and a busy worker can be caught woken and not yet run at every look, each time in a new sleep.
/// Ends with the runtime.
fn watch(runtime: std::sync::Weak<Inner>) {
    let mut streaks: BTreeMap<String, u32> = BTreeMap::new();
    loop {
        std::thread::sleep(STALL_LOOK);
        let Some(inner) = runtime.upgrade().filter(|i| !i.stopped.load(Ordering::Acquire)) else { return };
        // Copied out, so spawns and exits do not wait on the look.
        let cells: Vec<_> = inner.registry.lock().values().cloned().collect();
        let mut stalls: Vec<String> = (cells.iter())
            .filter_map(|(h, c)| {
                let stall = c.upgrade()?.stalled()?;
                Some(format!("actor {} {stall}", h.incarnation()))
            })
            .collect();
        // A dedicated actor's worker is its cell's alone, which `Cell::stalled` already covers.
        for p in &inner.pools {
            for (i, sleep) in p.pool.queues.iter().enumerate().filter_map(|(i, q)| Some((i, q.stalled()?))) {
                stalls.push(format!("worker {}-{i} sleeps with actors queued (sleep {sleep})", p.name));
            }
        }
        streaks = (stalls.into_iter())
            .map(|stall| {
                let n = streaks.get(&stall).map_or(1, |n| n + 1);
                (stall, n)
            })
            .collect();
        if let Some((stall, _)) = streaks.iter().find(|(_, n)| **n >= STALL_LOOKS) {
            // Not `eprintln!`, which a test's output capture would take down with the process.
            let _ = writeln!(std::io::stderr(), "axon: lost wakeup: {stall}");
            std::process::abort();
        }
    }
}

#[cfg(test)]
impl Cell {
    /// A cell homed on worker 0 of `pool`, in no runtime.
    fn bare(pool: Arc<Pool>, drive: Box<dyn Drive>, dedicated: bool) -> Arc<Self> {
        Arc::new(Self {
            scheduled: Scheduled::new(),
            skip_here: AtomicU8::new(HERE_FRESH),
            drive: Mutex::new(Some(drive)),
            pool,
            home: 0,
            budget: TurnBudget::default(),
            slot: 0,
            dedicated,
            runtime: std::sync::Weak::new(),
        })
    }
}

#[cfg(test)]
mod tests;

/// Every interleaving of the idle workers' handshake. A lost wakeup shows as a deadlock.
/// `RUSTFLAGS="--cfg axon_loom" cargo test -p axon-actors --lib --release --target-dir target/loom runtime::model`
#[cfg(test)] // apart from `axon_loom`, so clippy's allow-unwrap-in-tests sees a test module
#[cfg(axon_loom)]
mod model;
