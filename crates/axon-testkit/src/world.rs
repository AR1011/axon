//! Real nodes on virtual time: each node is a paused `current_thread` tokio runtime with no IO driver, one thread
//! steps them all in quanta in an order drawn from the seed, and their sessions run over byte pipes the World owns.
//! Design and limits: `spec/simulation.md`.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Display;
use std::hash::{DefaultHasher, Hasher};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::ops::{Bound, Range};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll, Waker};
use std::thread::Thread;
use std::time::{Duration, Instant};

use axon::files::{FileFuture, FileOp, Files};
use axon::{RuntimeConfig, Threadless};
use axon_net::{Authenticator, Dial, Incoming, Io, Lane, Network};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::task::LocalSet;

use crate::{jitter, pick, shuffle, splitmix};

/// Trace lines kept for `trace_tail` and the divergence report; the digest covers every line.
const TRACE: usize = 1 << 16;
const BACKLOG: usize = 64;
const WATCHDOG: Duration = Duration::from_secs(10);
const MS: Duration = Duration::from_millis(1);
const EMFILE: i32 = 24;

thread_local! {
    /// A World on this thread started a node whose tokio takes no seed: built without `--cfg tokio_unstable`.
    static UNSEEDED: Cell<bool> = const { Cell::new(false) };
}

/// Whether a World on this thread started an unseeded node since the last take, and clears it.
pub(crate) fn take_unseeded() -> bool {
    UNSEEDED.take()
}

/// What a failed seed's report adds where its World did not seed tokio.
pub(crate) fn not_replayed(seed: u64, test: &str) -> String {
    format!(
        "SIM_SEED={seed} does not replay in this build, which leaves tokio unseeded: build with \
         RUSTFLAGS=\"--cfg tokio_unstable\" to replay it (in the axon repository, SIM_SEED={seed} scripts/check.sh sim \
         {test})"
    )
}

#[derive(Clone, Debug)]
pub struct WorldConfig {
    /// Take it from `seeds()`, which reads `SIM_SEED` and `SEEDS`.
    pub seed: u64,
    /// How far every node moves per step: a whole number of ms, because a node steps with a sleep, which tokio
    /// rounds up to its 1 ms tick.
    pub quantum: Duration,
    /// Every directed link's.
    pub wire: Wire,
    /// Bytes a sender may have unacknowledged per connection.
    pub window: usize,
    /// Every node's unix time at the World's start; each node's wall clock then runs with its own.
    pub wall_start_ms: u64,
    /// Virtual time a `join` may take before it panics with the trace.
    pub limit: Duration,
}

impl Default for WorldConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            quantum: Duration::from_millis(1),
            wire: Wire::default(),
            window: 64 << 10,
            // 2026-01-01.
            wall_start_ms: 1_767_225_600_000,
            limit: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Wire {
    pub latency: Duration,
    /// Each chunk waits up to this much more, drawn from the seed. A connection's chunks never overtake each other.
    pub jitter: Duration,
    /// About one chunk in this many also waits `stall`: loss as a TCP session sees it, never a hole. 0: none.
    pub stall_every: u32,
    /// A SYN, or its answer, waits `syn_stall` instead.
    pub stall: Duration,
    /// The kernel's first resend of a lost SYN.
    pub syn_stall: Duration,
    /// Bytes a second, 0 for no limit. The link is one first-in, first-out bottleneck that every connection on it
    /// shares: a chunk of data waits until the link is free and holds it for its length, before its latency.
    /// Acknowledgements, SYNs and their answers, FIN and RST take no time on it.
    pub bandwidth: u64,
}

/// Names `bandwidth` only when it is set, so the trace of a run without it is the one before it existed.
impl std::fmt::Debug for Wire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut wire = f.debug_struct("Wire");
        wire.field("latency", &self.latency)
            .field("jitter", &self.jitter)
            .field("stall_every", &self.stall_every)
            .field("stall", &self.stall)
            .field("syn_stall", &self.syn_stall);
        if self.bandwidth > 0 {
            wire.field("bandwidth", &self.bandwidth);
        }
        wire.finish()
    }
}

impl Default for Wire {
    fn default() -> Self {
        Self {
            latency: Duration::from_millis(1),
            jitter: Duration::from_millis(1),
            stall_every: 0,
            stall: Duration::from_millis(200),
            syn_stall: Duration::from_secs(1),
            bandwidth: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Crash {
    /// The process dies and its kernel stays: peers read ECONNRESET, and dials are refused.
    Process,
    /// The machine goes silent: what is sent to it vanishes, and dials hang until the caller's deadline.
    Machine,
}

/// The node crashed before the task finished.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Crashed;

/// A future on a node, for `World::join`.
pub struct Task<T> {
    node: usize,
    boot: u32,
    slot: Slot<T>,
}

type Slot<T> = Rc<RefCell<Option<std::thread::Result<T>>>>;

/// What a node's boot is given.
pub struct Host {
    name: String,
    nic: Arc<Nic>,
    threadless: Threadless,
    disk: Disk,
}

impl Host {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 1, then 2 after a restart, and so on.
    pub fn boot(&self) -> u32 {
        self.nic.boot
    }

    /// 10.0.0.n, fixed per name for the run.
    pub fn ip(&self) -> IpAddr {
        lock(&self.nic.net).hosts[self.nic.node].ip
    }

    pub fn ip_of(&self, name: &str) -> IpAddr {
        let net = lock(&self.nic.net);
        net.hosts.iter().find(|h| h.name == name).map(|h| h.ip).unwrap_or_else(|| panic!("no node {name}"))
    }

    /// `ip():7000`.
    pub fn addr(&self) -> SocketAddr {
        SocketAddr::new(self.ip(), 7000)
    }

    /// Sessions over the World's wires, node id = name, plaintext allowed, compression as `Config::new` sets it. The
    /// incarnation's high bits are the node's wall ms, so each boot of a node is later than the one before.
    pub fn transport(&self, auth: impl Authenticator) -> axon_net::Config {
        let mut cfg = axon_net::Config::new(self.name.clone(), auth);
        cfg.allow_plaintext = true;
        cfg.network = Some(self.nic.clone());
        cfg
    }

    /// Threadless: every actor runs on one task of this node, on its clock.
    pub fn runtime(&self) -> RuntimeConfig {
        RuntimeConfig { workers: 1, threadless: Some(self.threadless.clone()), ..RuntimeConfig::default() }
    }

    /// The machine's disk, the same one on every boot.
    pub fn disk(&self) -> Disk {
        self.disk.clone()
    }
}

/// A machine's files, kept across its restarts. What was appended since a file's last `sync` is kept by
/// `Crash::Process`, as a page cache would. `Crash::Machine` keeps the first bytes of it, a seeded number of them per
/// file: none, all of it (writeback got there first) or a torn part, a third of the time each. A truncation or a
/// `write` over synced bytes, not synced since, is undone by it whole. Creating, replacing and removing a file are
/// durable at once. Nothing here waits: whoever uses it sleeps for the latency it wants.
///
/// `dir(name)` is a directory in it, as `OsFiles::new(dir.join(name))` is in production: its own names, on the same
/// machine, with the same capacity, crashes and panicking writes. Each lists only its own files.
#[derive(Clone, Default)]
pub struct Disk {
    files: Arc<Mutex<DiskFiles>>,
    /// Empty, or the directory's path and a `/`.
    dir: String,
}

#[derive(Default)]
struct DiskFiles {
    files: BTreeMap<String, DiskFile>,
    capacity: Option<u64>,
    /// Writes through `Files::apply` still to panic after their appends, before their sync.
    panics: u32,
}

#[derive(Default)]
struct DiskFile {
    data: Vec<u8>,
    synced: usize,
    /// The synced bytes, where a truncation or a write over them not synced since changed them.
    cut: Option<Vec<u8>>,
}

impl DiskFiles {
    fn used(&self) -> u64 {
        self.files.values().map(|f| f.data.len() as u64).sum()
    }
}

impl Disk {
    /// A directory in this one, made by its first file.
    pub fn dir(&self, name: &str) -> Disk {
        Disk { files: self.files.clone(), dir: format!("{}{name}/", self.dir) }
    }

    fn path(&self, name: &str) -> String {
        format!("{}{name}", self.dir)
    }

    /// Each file's name and length, by name: the files of this directory, not of those in it.
    pub fn list(&self) -> Vec<(String, u64)> {
        let disk = lock(&self.files);
        disk.files
            .range::<str, _>((Bound::Included(self.dir.as_str()), Bound::Unbounded))
            .map_while(|(path, f)| Some((path.strip_prefix(&self.dir)?, f)))
            .filter(|(name, _)| !name.contains('/'))
            .map(|(name, f)| (name.to_owned(), f.data.len() as u64))
            .collect()
    }

    pub fn read(&self, name: &str) -> Option<Vec<u8>> {
        lock(&self.files).files.get(&self.path(name)).map(|f| f.data.clone())
    }

    /// Creates the file if it is missing. `StorageFull`, and nothing written, past the capacity.
    pub fn append(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        let mut disk = lock(&self.files);
        if disk.capacity.is_some_and(|c| disk.used() + bytes.len() as u64 > c) {
            return Err(io::ErrorKind::StorageFull.into());
        }
        disk.files.entry(self.path(name)).or_default().data.extend_from_slice(bytes);
        Ok(())
    }

    /// Over the file's bytes from `offset` on, never past its end: `InvalidInput` there, `NotFound` for a missing file,
    /// and nothing written.
    pub fn write(&self, name: &str, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let mut disk = lock(&self.files);
        let f = disk.files.get_mut(&self.path(name)).ok_or(io::ErrorKind::NotFound)?;
        let range = axon::files::written_range(f.data.len() as u64, offset, bytes.len())?;
        if range.start < f.synced && f.cut.is_none() {
            f.cut = Some(f.data[..f.synced].to_vec());
        }
        f.data[range].copy_from_slice(bytes);
        Ok(())
    }

    pub fn truncate(&self, name: &str, len: u64) {
        if let Some(f) = lock(&self.files).files.get_mut(&self.path(name)) {
            if (len as usize) < f.synced && f.cut.is_none() {
                f.cut = Some(f.data[..f.synced].to_vec());
            }
            f.data.truncate(len as usize);
            f.synced = f.synced.min(f.data.len());
        }
    }

    pub fn remove(&self, name: &str) {
        lock(&self.files).files.remove(&self.path(name));
    }

    fn replace(&self, name: &str, bytes: &[u8]) {
        let file = DiskFile { data: bytes.to_vec(), synced: bytes.len(), cut: None };
        lock(&self.files).files.insert(self.path(name), file);
    }

    pub fn sync(&self, name: &str) {
        if let Some(f) = lock(&self.files).files.get_mut(&self.path(name)) {
            f.synced = f.data.len();
            f.cut = None;
        }
    }

    /// Bytes the files may hold in all, `None` for no bound.
    pub fn set_capacity(&self, bytes: Option<u64>) {
        lock(&self.files).capacity = bytes;
    }

    /// Bytes still free, `u64::MAX` with no capacity.
    pub fn free(&self) -> u64 {
        let disk = lock(&self.files);
        disk.capacity.map_or(u64::MAX, |c| c.saturating_sub(disk.used()))
    }

    /// The next `n` writes through [`Files::apply`] panic after their appends, before their sync.
    pub fn panic_writes(&self, n: u32) {
        lock(&self.files).panics = n;
    }

    /// Writes through [`Files::apply`] still to panic.
    pub fn panics_left(&self) -> u32 {
        lock(&self.files).panics
    }

    /// What a machine crash leaves, drawn from `rng`: each torn file's line in the trace.
    fn lose_unsynced(&self, rng: &mut u64) -> Vec<String> {
        let mut torn = Vec::new();
        for (path, f) in &mut lock(&self.files).files {
            match f.cut.take() {
                Some(before) => f.data = before,
                None => {
                    let unsynced = (f.data.len() - f.synced) as u64;
                    if unsynced > 0 {
                        let kept = match pick(rng, 0..3) {
                            0 => 0,
                            1 => unsynced,
                            _ => pick(rng, 0..unsynced + 1),
                        };
                        f.data.truncate(f.synced + kept as usize);
                        torn.push(format!("disk {path} kept {kept} of {unsynced} unsynced bytes"));
                    }
                }
            }
            f.synced = f.data.len();
        }
        torn
    }
}

/// How long a sync of one file takes, in a write through [`Files::apply`].
pub const DISK_SYNC: Duration = Duration::from_millis(2);

/// The disk as a store's files, in the order `OsFiles` keeps: the ops land in turn, StorageFull and nothing written
/// past the capacity. Each file a write appended to or wrote over is synced before a `Remove`, `Truncate` or `Replace`
/// after it lands, unless that op removes or replaces it, and at its end; a file it cuts is synced at once, and one it
/// replaces is written and synced before it lands. Each of those syncs takes `DISK_SYNC` on the node's clock, one file
/// after another as `OsFiles` syncs them, so a write to K files takes K of them; a write that syncs no file takes one,
/// the directory's.
impl Files for Disk {
    /// Read when called: the disk is memory, and the rule `Files` gives holds.
    fn list(&self) -> FileFuture<Vec<(String, u64)>> {
        Box::pin(std::future::ready(Ok(Disk::list(self))))
    }

    /// Synced first, as `Files` asks: a crashed process may have left bytes that were never synced.
    fn read(&self, name: &str) -> FileFuture<Vec<u8>> {
        self.sync(name);
        Box::pin(std::future::ready(Disk::read(self, name).ok_or_else(|| io::ErrorKind::NotFound.into())))
    }

    fn apply(&self, ops: Vec<FileOp>) -> FileFuture<()> {
        let disk = self.clone();
        Box::pin(async move {
            let written: usize = ops
                .iter()
                .map(|op| match op {
                    FileOp::Append(_, b) | FileOp::Replace(_, b) => b.len(),
                    _ => 0,
                })
                .sum();
            if written as u64 > disk.free() {
                return Err(io::ErrorKind::StorageFull.into());
            }
            // Each file appended to, with its length before: another write on the machine may fill it while this one
            // syncs, and an append it refuses cuts them back, as `OsFiles` does.
            let (mut unsynced, mut before) = (Vec::<&str>::new(), Vec::<(&str, u64)>::new());
            let mut syncs = 0;
            for op in &ops {
                if !matches!(op, FileOp::Append(..) | FileOp::Write(..)) {
                    // Not the file the op removes or replaces, as `OsFiles` does not.
                    if let FileOp::Remove(name) | FileOp::Replace(name, _) = op {
                        unsynced.retain(|file| file != name);
                    }
                    syncs += disk.sync_each(std::mem::take(&mut unsynced)).await;
                }
                match op {
                    FileOp::Append(name, bytes) => {
                        if !before.iter().any(|(n, _)| *n == name.as_str()) {
                            let len = lock(&disk.files).files.get(&disk.path(name)).map_or(0, |f| f.data.len());
                            before.push((name.as_str(), len as u64));
                        }
                        if let Err(e) = disk.append(name, bytes) {
                            before.iter().for_each(|(name, len)| disk.truncate(name, *len));
                            return Err(e);
                        }
                        if !unsynced.contains(&name.as_str()) {
                            unsynced.push(name);
                        }
                    }
                    // An op that changes a file keeps the appends before it, synced above, as `OsFiles` does.
                    FileOp::Truncate(name, len) => {
                        let cuts =
                            lock(&disk.files).files.get(&disk.path(name)).is_some_and(|f| f.data.len() as u64 > *len);
                        disk.truncate(name, *len);
                        if cuts {
                            before.clear();
                            syncs += disk.sync_each([name.as_str()]).await;
                        }
                    }
                    FileOp::Remove(name) => {
                        if lock(&disk.files).files.contains_key(&disk.path(name)) {
                            before.clear();
                        }
                        disk.remove(name);
                    }
                    FileOp::Replace(name, bytes) => {
                        tokio::time::sleep(DISK_SYNC).await;
                        syncs += 1;
                        disk.replace(name, bytes);
                        before.clear();
                    }
                    FileOp::Write(name, offset, bytes) => {
                        if let Err(e) = disk.write(name, *offset, bytes) {
                            before.iter().for_each(|(name, len)| disk.truncate(name, *len));
                            return Err(e);
                        }
                        if !unsynced.contains(&name.as_str()) {
                            unsynced.push(name);
                        }
                    }
                }
            }
            let panics = {
                let mut files = lock(&disk.files);
                let panics = files.panics > 0;
                files.panics = files.panics.saturating_sub(1);
                panics
            };
            if panics {
                panic!("the disk panics mid-write");
            }
            if disk.sync_each(unsynced).await + syncs == 0 {
                tokio::time::sleep(DISK_SYNC).await;
            }
            Ok(())
        })
    }
}

impl Disk {
    /// Syncs the files in turn, `DISK_SYNC` each, and answers how many.
    async fn sync_each(&self, names: impl IntoIterator<Item = &str>) -> u32 {
        let mut synced = 0;
        for name in names {
            tokio::time::sleep(DISK_SYNC).await;
            self.sync(name);
            synced += 1;
        }
        synced
    }
}

pub struct World<N> {
    cfg: WorldConfig,
    net: Arc<Mutex<Net>>,
    nodes: Vec<Node<N>>,
    watch: Arc<Watch>,
    dog: Thread,
}

type Boot<N> = Box<dyn Fn(Host) -> Pin<Box<dyn Future<Output = N>>>>;

/// A node, and what its machine keeps across restarts.
struct Node<N> {
    name: String,
    boot: u32,
    start: Boot<N>,
    run: Option<Run<N>>,
    cost: Duration,
    /// Stepped one quantum in this many.
    starve: u32,
    /// Its monotonic clock's rate error, in parts per million.
    drift: i32,
    /// How far its wall clock has been stepped, in µs.
    wall_step: Rc<Cell<i64>>,
    disk: Disk,
}

struct Run<N> {
    rt: Runtime,
    local: LocalSet,
    booting: Slot<N>,
    node: Option<N>,
    threadless: Threadless,
    wall: Wall,
    /// Each entry into the node seeds `axon::trace::random_u64` from this, so its draws do not depend on other nodes'.
    rng: u64,
    /// Its runtime's time at boot.
    origin: tokio::time::Instant,
    /// Where its clock should be after this quantum.
    due: Duration,
    /// Drift not yet a whole ms, in ppm times ms.
    skew: i64,
    paused_until: Option<Duration>,
    suspended_until: Option<Duration>,
    held_until: Option<Duration>,
}

/// What `axon::trace::unix_micros` reads while the World steps or boots a node.
type Wall = Rc<dyn Fn() -> u64>;

impl<N: Clone + 'static> World<N> {
    pub fn new(cfg: WorldConfig) -> Self {
        let (quantum, seed) = (cfg.quantum, cfg.seed);
        assert!(
            !quantum.is_zero() && quantum.subsec_nanos() % 1_000_000 == 0,
            "a quantum of {quantum:?}: a node's sleep rounds up to tokio's 1 ms tick, so it must be whole ms"
        );
        let net = Arc::new(Mutex::new(Net::new(&cfg)));
        let test = std::thread::current().name().unwrap_or("main").to_owned();
        let watch = Arc::new(Watch { seed, test, net: net.clone(), armed: Mutex::new(None) });
        let weak = Arc::downgrade(&watch);
        #[expect(clippy::disallowed_methods, reason = "the watchdog bounds the wall time a step takes")]
        let dog = std::thread::Builder::new().name("world-watchdog".into()).spawn(move || watchdog(weak));
        let dog = dog.expect("a watchdog thread").thread().clone();
        Self { cfg, net, nodes: Vec::new(), watch, dog }
    }

    /// Boots on the first step, and again on each restart with the next `Host::boot`.
    pub fn add<F, Fut>(&mut self, name: &str, boot: F)
    where
        F: Fn(Host) -> Fut + 'static,
        Fut: Future<Output = N> + 'static,
    {
        assert!(self.nodes.iter().all(|n| n.name != name), "{name} is added twice");
        lock(&self.net).add(name);
        let start: Boot<N> = Box::new(move |host| Box::pin(boot(host)));
        self.nodes.push(Node {
            name: name.to_owned(),
            boot: 0,
            start,
            run: None,
            cost: Duration::ZERO,
            starve: 1,
            drift: 0,
            wall_step: Rc::default(),
            disk: Disk::default(),
        });
    }

    /// Runs `f` on the node once it has booted. On a node that is down, the task has crashed already.
    pub fn spawn_on<T: 'static, Fut: Future<Output = T> + 'static>(
        &mut self,
        name: &str,
        f: impl FnOnce(N) -> Fut + 'static,
    ) -> Task<T> {
        let i = self.index(name);
        let limit = self.cfg.limit;
        self.run_until(&format!("{name} booted"), limit, |w| {
            w.nodes[i].boot > 0 && w.nodes[i].run.as_ref().is_none_or(|run| run.node.is_some())
        });
        let node = &self.nodes[i];
        let slot = Slot::default();
        if let Some(run) = &node.run {
            let n = run.node.clone().expect("booted above");
            run.local.spawn_local(caught(slot.clone(), async move { f(n).await }));
        }
        Task { node: i, boot: node.boot, slot }
    }

    /// Steps until the task ends, and panics with the trace if that takes longer than `WorldConfig::limit`.
    /// A panic in the task panics here.
    pub fn join<T>(&mut self, task: Task<T>) -> Result<T, Crashed> {
        let what = format!("a task on {} done", self.nodes[task.node].name);
        self.run_until(&what, self.cfg.limit, |w| {
            let n = &w.nodes[task.node];
            task.slot.borrow().is_some() || n.boot != task.boot || n.run.is_none()
        });
        match task.slot.borrow_mut().take() {
            Some(done) => Ok(done.unwrap_or_else(|panic| resume_unwind(panic))),
            None => Err(Crashed),
        }
    }

    pub fn block_on<T: 'static, Fut: Future<Output = T> + 'static>(
        &mut self,
        name: &str,
        f: impl FnOnce(N) -> Fut + 'static,
    ) -> Result<T, Crashed> {
        let task = self.spawn_on(name, f);
        self.join(task)
    }

    pub fn run_for(&mut self, d: Duration) {
        let end = self.now() + d;
        while self.now() < end {
            self.step();
        }
    }

    /// Steps until `done`, and panics with the trace if that takes longer than `within`.
    pub fn run_until(&mut self, what: &str, within: Duration, mut done: impl FnMut(&Self) -> bool) {
        no_thread_started();
        let end = self.now() + within;
        while !done(self) {
            if self.now() >= end {
                panic!("seed {}: not {what} within {within:?}\n{}", self.cfg.seed, self.trace_tail(40));
            }
            self.step();
        }
    }

    /// The nodes that are up and have booted, in the order they were added: none before the first step.
    pub fn nodes(&self) -> impl Iterator<Item = (&str, &N)> {
        self.nodes.iter().filter_map(|n| Some((n.name.as_str(), n.run.as_ref()?.node.as_ref()?)))
    }

    /// Runs `f` now, as the node, without stepping: tokio's clock and `unix_micros` read the node's clocks where its
    /// last step left them (behind the World's time while it is paused or starved), and `random_u64` draws from a copy
    /// of its stream, so a read changes nothing in the run. For reads: what `f` spawns runs at the node's next step,
    /// and a future it polls draws from tokio's unseeded generator. None while the node is down or still booting.
    pub fn read<T>(&self, name: &str, f: impl FnOnce(&N) -> T) -> Option<T> {
        let run = self.nodes[self.index(name)].run.as_ref()?;
        let node = run.node.as_ref()?;
        let mut rng = run.rng;
        // Entered inside `as_node`: the wall reads tokio's clock, which is the node's only while its runtime is.
        Some(as_node(Some(&run.wall), &mut rng, || {
            let _entered = run.rt.enter();
            f(node)
        }))
    }

    /// In this order: its wires go inert, its actors are forsaken, everything it held is dropped inside its runtime,
    /// and then the runtime is. Nothing reaches a peer after the first.
    pub fn crash(&mut self, name: &str, how: Crash) {
        let i = self.index(name);
        let run = self.nodes[i].run.take().unwrap_or_else(|| panic!("{name} is not running"));
        run.crash(&self.net, i, how);
        if how == Crash::Machine {
            let mut net = lock(&self.net);
            for line in self.nodes[i].disk.lose_unsynced(&mut net.rng) {
                net.record(line);
            }
        }
    }

    /// The node's disk, up or down.
    pub fn disk(&self, name: &str) -> Disk {
        self.nodes[self.index(name)].disk.clone()
    }

    /// Boots the node again at the same address, as the next `Host::boot`.
    pub fn restart(&mut self, name: &str) {
        let i = self.index(name);
        assert!(self.nodes[i].run.is_none(), "{name} is running: crash it first");
        self.start(i);
    }

    /// SIGSTOP now, SIGCONT after `d`: the node is not stepped, so it reads nothing and its peers' windows fill, and
    /// dials to it wait in its backlog. Its clock runs on, and what fell due meanwhile fires at once when it resumes.
    pub fn pause(&mut self, name: &str, d: Duration) {
        let until = self.now() + d;
        self.running(name).paused_until = Some(until);
        lock(&self.net).record(format!("pause {name} {}", stamp(d)));
    }

    /// A suspended VM: the node is not stepped for `d`, as under `pause`, and its clocks stand still meanwhile, tokio's
    /// and `unix_micros` both, so it wakes as if no time had passed. Its leases and fences still look valid to it.
    pub fn suspend(&mut self, name: &str, d: Duration) {
        let until = self.now() + d;
        self.running(name).suspended_until = Some(until);
        lock(&self.net).record(format!("suspend {name} {}", stamp(d)));
    }

    /// Its actors run no turn for `d`, as behind a handler that never returns; its tasks, timers and I/O go on.
    pub fn hold_actors(&mut self, name: &str, d: Duration) {
        let until = self.now() + d;
        let run = self.running(name);
        run.held_until = Some(until);
        run.threadless.hold(true);
        lock(&self.net).record(format!("hold {name} {}", stamp(d)));
    }

    /// Each actor turn takes `d` of the node's time, in this boot and the next.
    pub fn cost(&mut self, name: &str, d: Duration) {
        let i = self.index(name);
        self.nodes[i].cost = d;
        if let Some(run) = &self.nodes[i].run {
            run.threadless.turn_cost(d);
        }
        lock(&self.net).record(format!("cost {name} {}", stamp(d)));
    }

    /// The node is stepped one quantum in `one_in`, and its clock runs on, so its timers fire late and together.
    pub fn starve(&mut self, name: &str, one_in: u32) {
        assert!(one_in > 0, "a node is stepped one quantum in at least one");
        let i = self.index(name);
        self.nodes[i].starve = one_in;
        lock(&self.net).record(format!("starve {name} {one_in}"));
    }

    /// The node's monotonic clock runs `ppm` parts per million fast, or slow when negative, a whole ms at a time. A
    /// slow node is not stepped in a quantum its clock stands still, so its tasks and I/O wait one quantum too.
    pub fn drift(&mut self, name: &str, ppm: i32) {
        assert!(ppm > -1_000_000, "a clock at {ppm} ppm would stand still or run backwards");
        let i = self.index(name);
        self.nodes[i].drift = ppm;
        lock(&self.net).record(format!("drift {name} {ppm}"));
    }

    /// The node's wall clock jumps `by_ms`, now and in its later boots; its monotonic clock does not. A step back
    /// before a restart gives an older incarnation, as in production.
    pub fn step_wall(&mut self, name: &str, by_ms: i64) {
        let step = &self.nodes[self.index(name)].wall_step;
        step.set(step.get() + by_ms * 1000);
        lock(&self.net).record(format!("step_wall {name} {by_ms}"));
    }

    /// Holds what `from` sends to `to`, acknowledgements included, and every dial between them, until `heal`.
    pub fn cut(&mut self, from: &[&str], to: &[&str]) {
        self.cut_on(from, to, None);
    }

    /// As `cut`, for the connections of one lane and its dials either way. Called both ways, it partitions that lane.
    pub fn cut_lane(&mut self, from: &[&str], to: &[&str], lane: Lane) {
        self.cut_on(from, to, Some(lane));
    }

    fn cut_on(&mut self, from: &[&str], to: &[&str], lane: Option<Lane>) {
        let mut net = lock(&self.net);
        for f in from {
            for t in to {
                net.cuts.insert((self.index(f), self.index(t), lane));
                net.record(format!("cut {f}>{t}{}", named(lane)));
            }
        }
    }

    /// Both ways.
    pub fn partition(&mut self, a: &[&str], b: &[&str]) {
        self.cut(a, b);
        self.cut(b, a);
    }

    /// Ends every cut. What they held flows, in the order it was sent, except from a crashed end: a cut drops packets
    /// and the sender's kernel sends them again, which a dead one never does. A process's RST is its kernel's.
    pub fn heal(&mut self) {
        let mut net = lock(&self.net);
        net.cuts.clear();
        for (key, chunk) in std::mem::take(&mut net.held) {
            let dead = net.conns.get(&chunk.conn).is_some_and(|c| c.ends[chunk.from].state == State::Dead);
            if !dead || matches!(chunk.what, What::Rst) {
                net.pending.insert(key, chunk);
            }
        }
        net.record("heal".to_owned());
    }

    /// The wire from `from` to `to`, in place of `WorldConfig::wire`.
    pub fn wire(&mut self, from: &str, to: &str, wire: Wire) {
        let link = (self.index(from), self.index(to));
        let mut net = lock(&self.net);
        net.links.insert(link, wire);
        net.record(format!("wire {from}>{to} {wire:?}"));
    }

    /// RST at both ends of every connection between them, now.
    pub fn reset(&mut self, a: &str, b: &str) {
        self.reset_on(a, b, None);
    }

    /// As `reset`, for the connections of one lane.
    pub fn reset_lane(&mut self, a: &str, b: &str, lane: Lane) {
        self.reset_on(a, b, Some(lane));
    }

    fn reset_on(&mut self, a: &str, b: &str, lane: Option<Lane>) {
        let (a, b) = (self.index(a), self.index(b));
        let wake = lock(&self.net).reset(a, b, lane);
        wake.into_iter().for_each(Waker::wake);
    }

    /// While on, what reaches the node's listeners is an accept error (EMFILE) and dials to it wait. A restart ends it.
    pub fn refuse_accepts(&mut self, name: &str, on: bool) {
        let i = self.index(name);
        let mut net = lock(&self.net);
        net.hosts[i].refusing = on;
        net.record(format!("refuse {name} {on}"));
    }

    /// The World's time since it began. A node's own clocks: `read`.
    pub fn now(&self) -> Duration {
        lock(&self.net).now
    }

    /// The World's seeded choice, for fault timing.
    pub fn pick(&mut self, range: Range<u64>) -> u64 {
        pick(&mut lock(&self.net).rng, range)
    }

    /// A line in the trace, and so in the digest.
    pub fn log(&mut self, line: impl Display) {
        lock(&self.net).record(format!("log {line}"));
    }

    pub fn trace_tail(&self, n: usize) -> String {
        lock(&self.net).tail(n)
    }

    /// A hash of every chunk delivered, every fault and every `log` line so far.
    pub fn digest(&self) -> u64 {
        lock(&self.net).hasher.finish()
    }

    /// Where another run parts from this one: the first quantum whose digest differs, and both trace windows up to it.
    pub fn divergence(&self, other: &Self) -> Option<String> {
        if Arc::ptr_eq(&self.net, &other.net) {
            return None;
        }
        let (a, b) = (lock(&self.net), lock(&other.net));
        let first = a.digests.iter().zip(&b.digests).position(|(x, y)| x != y).or_else(|| {
            let differ = a.digests.len() != b.digests.len() || a.hasher.finish() != b.hasher.finish();
            differ.then(|| a.digests.len().min(b.digests.len()))
        })?;
        let window = |net: &Net| {
            let lines = net.trace.iter().filter(|(q, _)| (first.saturating_sub(2)..=first).contains(q));
            lines.map(|(_, line)| line.as_str()).collect::<Vec<_>>().join("\n")
        };
        let at = stamp(self.cfg.quantum * first as u32);
        Some(format!(
            "seed {}: the runs part at quantum {first} ({at})\n--- this run\n{}\n--- the other run\n{}",
            self.cfg.seed,
            window(&a),
            window(&b)
        ))
    }

    /// One quantum: boot what was added, end the pauses, suspensions and holds that are over, move on where each
    /// node's clock should be unless it is suspended, deliver what is due, then step every live node in seeded order:
    /// move its clock to where it should be a quantum before the end of this one, which fires what fell due while it
    /// was paused or starved at once, and run it through the last quantum. Then record the quantum's digest.
    fn step(&mut self) {
        for i in 0..self.nodes.len() {
            if self.nodes[i].boot == 0 {
                self.start(i);
            }
        }
        let (q, now) = (self.cfg.quantum, self.now());
        for node in &mut self.nodes {
            let Some(run) = node.run.as_mut() else { continue };
            if run.held_until.is_some_and(|until| until <= now) {
                run.held_until = None;
                run.threadless.hold(false);
            }
            run.paused_until = run.paused_until.filter(|until| *until > now);
            run.suspended_until = run.suspended_until.filter(|until| *until > now);
            if run.suspended_until.is_some() {
                continue;
            }
            run.due += q;
            run.skew += i64::from(node.drift) * q.as_millis() as i64;
            while run.skew.abs() >= 1_000_000 {
                let ms = run.skew.signum();
                run.skew -= ms * 1_000_000;
                run.due = if ms > 0 { run.due + MS } else { run.due - MS };
            }
        }
        let (effects, order, quantum) = {
            let mut net = lock(&self.net);
            let effects = net.deliver(&self.net);
            let mut order: Vec<_> = (0..self.nodes.len()).filter(|i| self.nodes[*i].run.is_some()).collect();
            shuffle(&mut net.rng, &mut order);
            (effects, order, net.digests.len())
        };
        effects.run();
        for i in order {
            let node = &mut self.nodes[i];
            let Some(run) = node.run.as_mut() else { continue };
            if run.paused_until.or(run.suspended_until).is_some() || !quantum.is_multiple_of(node.starve as usize) {
                continue;
            }
            let behind = run.due.saturating_sub({
                let _entered = run.rt.enter();
                run.origin.elapsed()
            });
            // A slow clock stands still this quantum.
            if behind.is_zero() {
                continue;
            }
            let last = behind.min(q);
            #[expect(clippy::disallowed_methods, reason = "the watchdog bounds the wall time a step takes")]
            let armed = Some((Instant::now(), i));
            *lock(&self.watch.armed) = armed;
            as_node(Some(&run.wall), &mut run.rng, || {
                run.local.block_on(&run.rt, async {
                    if behind > last {
                        tokio::time::advance(behind - last).await;
                    }
                    tokio::time::sleep(last).await;
                })
            });
            *lock(&self.watch.armed) = None;
            if run.node.is_none()
                && let Some(booted) = run.booting.borrow_mut().take()
            {
                run.node = Some(booted.unwrap_or_else(|panic| resume_unwind(panic)));
            }
        }
        lock(&self.net).end_quantum();
    }

    fn start(&mut self, i: usize) {
        let node = &mut self.nodes[i];
        node.boot += 1;
        let mut builder = tokio::runtime::Builder::new_current_thread();
        builder.enable_time().start_paused(true);
        let mut net = lock(&self.net);
        let booted_at_us = self.cfg.wall_start_ms * 1000 + net.now.as_micros() as u64;
        // Drawn in every build, so a seed makes the same World choices in the plain and the sim build.
        let (tokio_seed, mut rng) = (net.random().to_le_bytes(), net.random());
        #[cfg(tokio_unstable)]
        builder.rng_seed(tokio::runtime::RngSeed::from_bytes(&tokio_seed));
        #[cfg(not(tokio_unstable))]
        {
            let _ = tokio_seed;
            UNSEEDED.set(true);
        }
        net.boot(i, node.boot);
        drop(net);
        let rt = builder.build().expect("a node's runtime");
        let origin = {
            let _entered = rt.enter();
            tokio::time::Instant::now()
        };
        let step = node.wall_step.clone();
        let wall: Wall =
            Rc::new(move || (booted_at_us + origin.elapsed().as_micros() as u64).saturating_add_signed(step.get()));
        let threadless = Threadless::default();
        threadless.turn_cost(node.cost);
        let nic = Arc::new(Nic { net: self.net.clone(), node: i, boot: node.boot });
        let host = Host { name: node.name.clone(), nic, threadless: threadless.clone(), disk: node.disk.clone() };
        let boot = as_node(Some(&wall), &mut rng, || {
            let _entered = rt.enter();
            (node.start)(host)
        });
        let (local, booting) = (LocalSet::new(), Slot::default());
        local.spawn_local(caught(booting.clone(), boot));
        node.run = Some(Run {
            rt,
            local,
            booting,
            node: None,
            threadless,
            wall,
            rng,
            origin,
            due: Duration::ZERO,
            skew: 0,
            paused_until: None,
            suspended_until: None,
            held_until: None,
        });
    }

    fn running(&mut self, name: &str) -> &mut Run<N> {
        let i = self.index(name);
        self.nodes[i].run.as_mut().unwrap_or_else(|| panic!("{name} is not running"))
    }

    fn index(&self, name: &str) -> usize {
        self.nodes.iter().position(|n| n.name == name).unwrap_or_else(|| panic!("no node {name}"))
    }
}

impl<N> Run<N> {
    /// Tokio drops the tasks in an order set by process-global ids, which cannot reach a peer over dead wires.
    fn crash(self, net: &Mutex<Net>, i: usize, how: Crash) {
        let Self { rt, local, booting, node, threadless, mut rng, .. } = self;
        lock(net).crash(i, how);
        threadless.forsake();
        as_node(None, &mut rng, || {
            let _entered = rt.enter();
            drop((node, booting, local));
        });
        drop(rt);
    }
}

impl<N> Drop for World<N> {
    fn drop(&mut self) {
        for (i, node) in self.nodes.iter_mut().enumerate() {
            if let Some(run) = node.run.take() {
                run.crash(&self.net, i, Crash::Machine);
            }
        }
        self.dog.unpark();
    }
}

/// Runs `f` as the node: `unix_micros` reads `wall`, and `random_u64` and axon-net's randomness are a stream drawn
/// from `rng`. Both are the thread's own again afterwards, even when `f` panics.
fn as_node<T>(wall: Option<&Wall>, rng: &mut u64, f: impl FnOnce() -> T) -> T {
    struct Off;
    impl Drop for Off {
        fn drop(&mut self) {
            axon::trace::simulate_wall(None);
            axon::trace::simulate_seed(None);
        }
    }
    axon::trace::simulate_wall(wall.cloned());
    axon::trace::simulate_seed(Some(splitmix(rng)));
    let _off = Off;
    f()
}

async fn caught<T>(slot: Slot<T>, future: impl Future<Output = T>) {
    let mut future = std::pin::pin!(future);
    let done = std::future::poll_fn(|cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
        Ok(Poll::Pending) => Poll::Pending,
        Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
        Err(panic) => Poll::Ready(Err(panic)),
    });
    let done = done.await;
    *slot.borrow_mut() = Some(done);
}

/// On Linux: a node that starts a thread (a threaded runtime, a blocking pool) runs outside virtual time. It sees the
/// whole process, so World tests live in test files of their own, never next to tests that start real threads.
fn no_thread_started() {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else { return };
    let names = tasks.flatten().filter_map(|task| std::fs::read_to_string(task.path().join("comm")).ok());
    let started: Vec<_> = names.filter(|name| name.starts_with("axon-") || name.starts_with("tokio-")).collect();
    assert!(started.is_empty(), "a node started threads {started:?}: build it from Host::runtime, and block nowhere");
}

struct Watch {
    seed: u64,
    /// The thread that made the World, which libtest names after the test.
    test: String,
    net: Arc<Mutex<Net>>,
    /// Since when which node has been stepping.
    armed: Mutex<Option<(Instant, usize)>>,
}

#[expect(clippy::disallowed_methods, reason = "the watchdog bounds the wall time a step takes")]
fn watchdog(watch: Weak<Watch>) {
    loop {
        std::thread::park_timeout(Duration::from_secs(1));
        let Some(watch) = watch.upgrade() else { return };
        let armed = *lock(&watch.armed);
        let Some((_, node)) = armed.filter(|(since, _)| since.elapsed() > WATCHDOG) else { continue };
        let (name, tail) = match watch.net.try_lock() {
            Ok(net) => (net.hosts[node].name.clone(), net.tail(60)),
            Err(_) => (node.to_string(), "(the network is locked)".to_owned()),
        };
        eprintln!(
            "seed {seed}: node {name} did not finish a step within {WATCHDOG:?} of wall time: a node that never goes \
             idle stops virtual time\n{tail}\nseeds {test}: aborted, then SIM_SEED={seed} failed",
            seed = watch.seed,
            test = watch.test
        );
        if cfg!(not(tokio_unstable)) {
            eprintln!("{}", not_replayed(watch.seed, &watch.test));
        }
        std::process::abort();
    }
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn stamp(t: Duration) -> String {
    format!("{}.{:06}", t.as_secs(), t.subsec_micros())
}

/// A lane in a trace line, nothing for every lane.
fn named(lane: Option<Lane>) -> String {
    lane.map_or_else(String::new, |lane| format!(" {lane:?}"))
}

/// The wires between the nodes, and the trace. Shared with every node's pipes, which run on the World's thread.
struct Net {
    rng: u64,
    now: Duration,
    quantum: Duration,
    wire: Wire,
    window: usize,
    hosts: Vec<Place>,
    conns: BTreeMap<u64, Conn>,
    dials: BTreeMap<u64, Dialing>,
    pending: BTreeMap<(Duration, u64), Chunk>,
    /// What came due across a cut, until the heal.
    held: BTreeMap<(Duration, u64), Chunk>,
    /// Directed (from, to) and a lane, or every lane: what `from` sends on it is held.
    cuts: BTreeSet<(usize, usize, Option<Lane>)>,
    /// Directed links whose wire is not the default.
    links: BTreeMap<(usize, usize), Wire>,
    /// Directed links with a bandwidth: until when the chunks already on it hold it.
    busy: BTreeMap<(usize, usize), Duration>,
    next: u64,
    trace: VecDeque<(usize, String)>,
    hasher: DefaultHasher,
    /// The hash after each quantum.
    digests: Vec<u64>,
}

type Accepted = io::Result<(Box<dyn Io>, SocketAddr)>;

struct Place {
    name: String,
    ip: IpAddr,
    boot: u32,
    down: Option<Crash>,
    /// Accepts fail with EMFILE.
    refusing: bool,
    listeners: BTreeMap<u16, mpsc::Sender<Accepted>>,
}

/// A connection. End 0 dialled, end 1 accepted.
struct Conn {
    ends: [End; 2],
    lane: Lane,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Open,
    /// Its pipe was dropped: what arrives is answered with RST.
    Closed,
    /// Its node crashed.
    Dead,
}

struct End {
    node: usize,
    state: State,
    /// Bytes delivered here and not yet read.
    buf: VecDeque<u8>,
    fin: bool,
    reset: bool,
    shut: bool,
    /// Bytes sent from here that the peer has not read yet, as far as this end has heard.
    unacked: usize,
    reader: Option<Waker>,
    writer: Option<Waker>,
    /// When the last chunk sent from here is due, so none overtakes it.
    last_due: Duration,
}

impl End {
    fn new(node: usize) -> Self {
        Self {
            node,
            state: State::Open,
            buf: VecDeque::new(),
            fin: false,
            reset: false,
            shut: false,
            unacked: 0,
            reader: None,
            writer: None,
            last_due: Duration::ZERO,
        }
    }
}

struct Dialing {
    node: usize,
    /// The node the address is, if any.
    far: Option<usize>,
    lane: Lane,
    waker: Option<Waker>,
    /// The connection, or why there is none.
    result: Option<io::Result<u64>>,
}

enum What {
    Syn(SocketAddr),
    /// The answers to a SYN, from the far end: the dial completes or fails when one arrives.
    SynAck,
    Refused,
    Data(Vec<u8>),
    Ack(usize),
    Fin,
    Rst,
}

struct Chunk {
    conn: u64,
    /// The end it was sent from.
    from: usize,
    what: What,
}

/// What a delivery does once the network is unlocked: a woken task or a dropped pipe may lock it again.
#[derive(Default)]
struct Effects {
    wake: Vec<Waker>,
    accept: Vec<(mpsc::OwnedPermit<Accepted>, Accepted)>,
}

impl Effects {
    fn run(self) {
        for (backlog, accepted) in self.accept {
            backlog.send(accepted);
        }
        self.wake.into_iter().for_each(Waker::wake);
    }
}

impl Net {
    fn new(cfg: &WorldConfig) -> Self {
        Self {
            rng: cfg.seed,
            now: Duration::ZERO,
            quantum: cfg.quantum,
            wire: cfg.wire,
            window: cfg.window,
            hosts: Vec::new(),
            conns: BTreeMap::new(),
            dials: BTreeMap::new(),
            pending: BTreeMap::new(),
            held: BTreeMap::new(),
            cuts: BTreeSet::new(),
            links: BTreeMap::new(),
            busy: BTreeMap::new(),
            next: 0,
            trace: VecDeque::new(),
            hasher: DefaultHasher::new(),
            digests: Vec::new(),
        }
    }

    fn random(&mut self) -> u64 {
        splitmix(&mut self.rng)
    }

    fn id(&mut self) -> u64 {
        self.next += 1;
        self.next
    }

    fn add(&mut self, name: &str) {
        let n = self.hosts.len() + 1;
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, (n >> 8) as u8, n as u8));
        self.hosts.push(Place {
            name: name.to_owned(),
            ip,
            boot: 0,
            down: None,
            refusing: false,
            listeners: BTreeMap::new(),
        });
    }

    fn live(&self, node: usize, boot: u32) -> bool {
        let host = &self.hosts[node];
        host.boot == boot && host.down.is_none()
    }

    fn record(&mut self, line: String) {
        let line = format!("{} {line}", stamp(self.now));
        self.hasher.write(line.as_bytes());
        if self.trace.len() == TRACE {
            self.trace.pop_front();
        }
        self.trace.push_back((self.digests.len(), line));
    }

    fn tail(&self, n: usize) -> String {
        let skip = self.trace.len().saturating_sub(n);
        self.trace.iter().skip(skip).map(|(_, line)| line.as_str()).collect::<Vec<_>>().join("\n")
    }

    fn end_quantum(&mut self) {
        self.digests.push(self.hasher.finish());
        self.now += self.quantum;
    }

    /// The nodes a chunk goes from and to, while both are known, and its lane.
    fn route(&self, chunk: &Chunk) -> Option<(usize, usize, Lane)> {
        let (ends, lane) = match &chunk.what {
            // A SYN goes from end 0 and a refusal from end 1, as on a connection.
            What::Syn(_) | What::Refused => {
                let dial = self.dials.get(&chunk.conn)?;
                ([dial.node, dial.far?], dial.lane)
            }
            _ => self.conns.get(&chunk.conn).map(|c| ([c.ends[0].node, c.ends[1].node], c.lane))?,
        };
        Some((ends[chunk.from], ends[1 - chunk.from], lane))
    }

    /// A cut holds what goes its way, and a SYN either way, as a connect across it hangs.
    fn cut(&self, chunk: &Chunk) -> bool {
        !self.cuts.is_empty()
            && self.route(chunk).is_some_and(|(from, to, lane)| {
                let cut = |f, t| self.cuts.contains(&(f, t, None)) || self.cuts.contains(&(f, t, Some(lane)));
                cut(from, to) || matches!(chunk.what, What::Syn(_)) && cut(to, from)
            })
    }

    /// Queues a chunk due a latency, seeded jitter and maybe a stall after `at`, or after its link's bandwidth lets
    /// its last byte go, and after the last one from its end.
    fn post(&mut self, mut at: Duration, conn: u64, from: usize, what: What) {
        let chunk = Chunk { conn, from, what };
        let link = self.route(&chunk).map(|(from, to, _)| (from, to));
        let wire = link.and_then(|link| self.links.get(&link)).copied().unwrap_or(self.wire);
        if let (What::Data(bytes), Some(link), 1..) = (&chunk.what, link, wire.bandwidth) {
            let busy = self.busy.entry(link).or_default();
            let hold = (bytes.len() as u64).saturating_mul(1_000_000_000) / wire.bandwidth;
            *busy = at.max(*busy) + Duration::from_nanos(hold);
            at = *busy;
        }
        let mut due = at + wire.latency + jitter(&mut self.rng, wire.jitter);
        if wire.stall_every > 0 && self.random().is_multiple_of(u64::from(wire.stall_every)) {
            let syn = matches!(chunk.what, What::Syn(_) | What::SynAck | What::Refused);
            due += if syn { wire.syn_stall } else { wire.stall };
        }
        if let Some(end) = self.conns.get_mut(&conn).map(|c| &mut c.ends[from]) {
            due = due.max(end.last_due);
            end.last_due = due;
        }
        let seq = self.id();
        self.pending.insert((due, seq), chunk);
    }

    /// Sent while a node steps: due after the end of this quantum.
    fn send(&mut self, conn: u64, from: usize, what: What) {
        self.post(self.now + self.quantum, conn, from, what);
    }

    /// Delivers what is due. The connections go in a seeded order, each one's chunks in the order sent.
    fn deliver(&mut self, net: &Arc<Mutex<Net>>) -> Effects {
        let later = self.pending.split_off(&(self.now, u64::MAX));
        let mut due: BTreeMap<(u64, usize), Vec<What>> = BTreeMap::new();
        for (key, chunk) in std::mem::replace(&mut self.pending, later) {
            if self.cut(&chunk) {
                self.held.insert(key, chunk);
            } else {
                due.entry((chunk.conn, chunk.from)).or_default().push(chunk.what);
            }
        }
        let mut due: Vec<_> = due.into_iter().collect();
        shuffle(&mut self.rng, &mut due);
        let mut effects = Effects::default();
        for ((conn, from), whats) in due {
            for what in whats {
                self.arrive(conn, from, what, &mut effects, net);
            }
        }
        effects
    }

    fn arrive(&mut self, conn: u64, from: usize, what: What, effects: &mut Effects, net: &Arc<Mutex<Net>>) {
        match what {
            What::Syn(to) => return self.syn(conn, to, effects, net),
            What::SynAck => return self.answer(conn, true, effects),
            What::Refused => return self.answer(conn, false, effects),
            _ => {}
        }
        let Some(c) = self.conns.get_mut(&conn) else { return };
        let [src, dst] = [from, 1 - from].map(|e| c.ends[e].node);
        // Only a crash makes an end Dead, so a Dead end's node that is up again is a later boot.
        let restarted = self.hosts[dst].down.is_none();
        let end = &mut c.ends[1 - from];
        let mut refuse = false;
        // A data line carries a hash of its bytes, so a divergence report shows which chunk differs.
        let data = |bytes: &[u8]| {
            let mut hasher = DefaultHasher::new();
            hasher.write(bytes);
            format!("{} {:016x}", bytes.len(), hasher.finish())
        };
        let what = match (what, end.state) {
            (What::Ack(n), State::Open) => {
                end.unacked -= n;
                effects.wake.extend(end.writer.take());
                format!("ack {n}")
            }
            (What::Ack(_), _) => return,
            (What::Data(bytes), State::Open) if !end.reset => {
                end.buf.extend(&bytes);
                effects.wake.extend(end.reader.take());
                data(&bytes)
            }
            (What::Fin, State::Open) => {
                end.fin = true;
                effects.wake.extend(end.reader.take());
                "fin".to_owned()
            }
            (What::Rst, State::Open) => {
                end.reset = true;
                effects.wake.extend(end.reader.take().into_iter().chain(end.writer.take()));
                "rst".to_owned()
            }
            (What::Data(bytes), state) => {
                refuse = state == State::Closed || (state == State::Dead && restarted);
                format!("{} lost", data(&bytes))
            }
            (What::Fin | What::Rst | What::Syn(_) | What::SynAck | What::Refused, _) => "lost".to_owned(),
        };
        let line = format!("{}>{} {conn} {what}", self.hosts[src].name, self.hosts[dst].name);
        self.record(line);
        if refuse {
            self.post(self.now, conn, 1 - from, What::Rst);
        }
    }

    fn syn(&mut self, conn: u64, to: SocketAddr, effects: &mut Effects, net: &Arc<Mutex<Net>>) {
        let Some(dial) = self.dials.get(&conn) else { return };
        let (from, target, lane) = (dial.node, dial.far, dial.lane);
        let listener = target.and_then(|t| self.hosts[t].listeners.get(&to.port()));
        let outcome = match target.map(|t| self.hosts[t].down) {
            None | Some(Some(Crash::Machine)) => None,
            // A full backlog drops the SYN, and the dial waits.
            Some(None) => match listener.map(|l| l.clone().try_reserve_owned()) {
                Some(Ok(backlog)) => Some(Ok(backlog)),
                Some(Err(mpsc::error::TrySendError::Full(_))) => None,
                Some(Err(mpsc::error::TrySendError::Closed(_))) | None => Some(Err(())),
            },
            Some(Some(Crash::Process)) => Some(Err(())),
        };
        let dst = target.map_or_else(|| to.to_string(), |t| self.hosts[t].name.clone());
        let line =
            format!("{}>{dst} {conn} syn{}", self.hosts[from].name, named((lane != Lane::Control).then_some(lane)));
        let answer = match outcome {
            None => return self.record(format!("{line} lost")),
            Some(Err(())) => {
                self.record(format!("{line} refused"));
                What::Refused
            }
            Some(Ok(backlog)) => {
                let t = target.expect("a listener has a node");
                if self.hosts[t].refusing {
                    // Out of descriptors, the listener cannot take it, and the SYN goes unanswered.
                    effects.accept.push((backlog, Err(io::Error::from_raw_os_error(EMFILE))));
                    return self.record(format!("{line} emfile"));
                }
                self.record(line);
                self.conns.insert(conn, Conn { ends: [End::new(from), End::new(t)], lane });
                let port = 32_768 + (conn % 28_000) as u16;
                let pipe = Box::new(Pipe { net: net.clone(), conn, end: 1 });
                effects.accept.push((backlog, Ok((pipe, SocketAddr::new(self.hosts[from].ip, port)))));
                What::SynAck
            }
        };
        // From the far end: the reverse wire's latency, and its first bytes queue behind it.
        self.post(self.now, conn, 1, answer);
    }

    /// A dial completes or fails one round trip after it started. Where its caller gave up meanwhile, its kernel
    /// closes what the far end accepted.
    fn answer(&mut self, conn: u64, accepted: bool, effects: &mut Effects) {
        let what = if accepted { "syn-ack" } else { "refusal" };
        let Some(dial) = self.dials.get_mut(&conn) else {
            self.close(conn, 0);
            return self.record(format!("{conn} {what} lost"));
        };
        dial.result = Some(if accepted { Ok(conn) } else { Err(io::ErrorKind::ConnectionRefused.into()) });
        effects.wake.extend(dial.waker.take());
        self.record(format!("{conn} {what}"));
    }

    /// Its wires go inert: a process's kernel resets its connections after a latency, a machine goes silent.
    /// What it drops (wakers, listener senders, connections with no pipe left) never locks the network.
    fn crash(&mut self, node: usize, how: Crash) {
        let host = &mut self.hosts[node];
        host.down = Some(how);
        host.listeners.clear();
        let line = format!("crash {} {how:?}", host.name);
        self.record(line);
        let (mut resets, mut gone) = (Vec::new(), Vec::new());
        for (id, conn) in &mut self.conns {
            for (e, end) in conn.ends.iter_mut().enumerate() {
                if end.node == node && end.state != State::Dead {
                    end.state = State::Dead;
                    (end.reader, end.writer) = (None, None);
                    if how == Crash::Process {
                        resets.push((*id, e));
                    }
                }
            }
            if conn.ends.iter().all(|end| end.state != State::Open) {
                gone.push(*id);
            }
        }
        for (conn, e) in resets {
            self.post(self.now, conn, e, What::Rst);
        }
        for id in gone {
            self.conns.remove(&id);
        }
    }

    /// Up again at the same address. A peer still sending to the old incarnation gets RST.
    fn boot(&mut self, node: usize, boot: u32) {
        let host = &mut self.hosts[node];
        (host.boot, host.down, host.refusing) = (boot, None, false);
        let line = format!("boot {} {boot}", host.name);
        self.record(line);
        let stale: Vec<_> = self
            .conns
            .iter()
            .flat_map(|(id, c)| (0..2).map(move |e| (*id, e, c)))
            .filter(|(_, e, c)| {
                c.ends[*e].node == node && c.ends[1 - e].state == State::Open && c.ends[1 - e].unacked > 0
            })
            .map(|(id, e, _)| (id, e))
            .collect();
        for (conn, e) in stale {
            self.post(self.now, conn, e, What::Rst);
        }
    }

    /// Every open end of every connection between them, on `lane` or on any, is reset now. Returns whom to wake once
    /// unlocked.
    fn reset(&mut self, a: usize, b: usize, lane: Option<Lane>) -> Vec<Waker> {
        let (mut wake, mut reset) = (Vec::new(), Vec::new());
        for (id, conn) in &mut self.conns {
            let nodes = [conn.ends[0].node, conn.ends[1].node];
            if nodes != [a, b] && nodes != [b, a] || lane.is_some_and(|lane| lane != conn.lane) {
                continue;
            }
            for end in conn.ends.iter_mut().filter(|end| end.state == State::Open && !end.reset) {
                end.reset = true;
                wake.extend(end.reader.take().into_iter().chain(end.writer.take()));
            }
            reset.push(*id);
        }
        let line = format!("reset {}:{}{} {reset:?}", self.hosts[a].name, self.hosts[b].name, named(lane));
        self.record(line);
        wake
    }

    /// The pipe of this end was dropped.
    fn close(&mut self, conn: u64, e: usize) {
        let Some(end) = self.conns.get_mut(&conn).map(|c| &mut c.ends[e]) else { return };
        if end.state != State::Open {
            return;
        }
        end.state = State::Closed;
        let fin = !end.shut && !end.reset;
        if fin {
            self.send(conn, e, What::Fin);
        }
        if self.conns[&conn].ends.iter().all(|end| end.state != State::Open) {
            self.conns.remove(&conn);
        }
    }
}

/// A node's network card: dials and listens as that boot of that node.
struct Nic {
    net: Arc<Mutex<Net>>,
    node: usize,
    boot: u32,
}

impl Network for Nic {
    /// One connection for each dial, as TCP's.
    fn connect(&self, to: SocketAddr, lane: Lane) -> Dial {
        let mut net = lock(&self.net);
        if !net.live(self.node, self.boot) {
            return Box::pin(std::future::pending());
        }
        let id = net.id();
        let far = net.hosts.iter().position(|h| h.ip == to.ip());
        net.dials.insert(id, Dialing { node: self.node, far, lane, waker: None, result: None });
        net.send(id, 0, What::Syn(to));
        Box::pin(Dialed { net: self.net.clone(), id })
    }

    fn bind(&self, at: SocketAddr) -> io::Result<(SocketAddr, Incoming)> {
        let mut net = lock(&self.net);
        if !net.live(self.node, self.boot) {
            return Err(io::ErrorKind::AddrNotAvailable.into());
        }
        let from = if at.port() == 0 { net.id() } else { 0 };
        let host = &mut net.hosts[self.node];
        if !at.ip().is_unspecified() && at.ip() != host.ip {
            return Err(io::ErrorKind::AddrNotAvailable.into());
        }
        let free = |port: &u16| host.listeners.get(port).is_none_or(|l| l.is_closed());
        let port = match at.port() {
            // An ephemeral port is one no listener holds, as the kernel picks it.
            0 => (0..10_000).map(|k| 20_000 + ((from + k) % 10_000) as u16).find(free),
            port => Some(port).filter(free),
        };
        let port = port.ok_or(io::ErrorKind::AddrInUse)?;
        let (tx, rx) = mpsc::channel(BACKLOG);
        let old = host.listeners.insert(port, tx);
        let local = SocketAddr::new(host.ip, port);
        drop(net);
        drop(old);
        Ok((local, rx))
    }
}

struct Dialed {
    net: Arc<Mutex<Net>>,
    id: u64,
}

impl Future for Dialed {
    type Output = io::Result<Box<dyn Io>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut net = lock(&self.net);
        let dial = net.dials.get_mut(&self.id).expect("a dial is known until it is dropped");
        match dial.result.take() {
            Some(Ok(conn)) => Poll::Ready(Ok(Box::new(Pipe { net: self.net.clone(), conn, end: 0 }))),
            Some(Err(e)) => Poll::Ready(Err(e)),
            None => {
                dial.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl Drop for Dialed {
    fn drop(&mut self) {
        let mut net = lock(&self.net);
        // A connection made after the caller gave up is closed at once.
        if let Some(Dialing { result: Some(Ok(conn)), .. }) = net.dials.remove(&self.id) {
            net.close(conn, 0);
        }
    }
}

/// One end of a connection.
struct Pipe {
    net: Arc<Mutex<Net>>,
    conn: u64,
    end: usize,
}

impl Pipe {
    /// This end while it is open. A crashed node's pipes, dropped with it, see `None`.
    fn end<'a>(&self, net: &'a mut Net) -> Option<&'a mut End> {
        let end = &mut net.conns.get_mut(&self.conn)?.ends[self.end];
        (end.state == State::Open).then_some(end)
    }
}

impl AsyncRead for Pipe {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let mut net = lock(&self.net);
        let Some(end) = self.end(&mut net) else { return Poll::Pending };
        if end.reset {
            return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
        }
        if end.buf.is_empty() && !end.fin {
            end.reader = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let (front, _) = end.buf.as_slices();
        let n = buf.remaining().min(front.len());
        buf.put_slice(&front[..n]);
        end.buf.drain(..n);
        // A read frees the window, as the kernel's buffer does: a node that stops reading stops its senders.
        if n > 0 {
            net.send(self.conn, self.end, What::Ack(n));
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Pipe {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, data: &[u8]) -> Poll<io::Result<usize>> {
        let mut net = lock(&self.net);
        let window = net.window;
        // What a crashed node writes goes nowhere.
        let Some(end) = self.end(&mut net) else { return Poll::Ready(Ok(data.len())) };
        if end.reset {
            return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
        }
        if end.shut {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        let n = data.len().min(window - end.unacked);
        if n == 0 && !data.is_empty() {
            end.writer = Some(cx.waker().clone());
            return Poll::Pending;
        }
        end.unacked += n;
        if n > 0 {
            net.send(self.conn, self.end, What::Data(data[..n].to_vec()));
        }
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut net = lock(&self.net);
        if let Some(end) = self.end(&mut net).filter(|end| !end.shut && !end.reset) {
            end.shut = true;
            net.send(self.conn, self.end, What::Fin);
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for Pipe {
    fn drop(&mut self) {
        lock(&self.net).close(self.conn, self.end);
    }
}
