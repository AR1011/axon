//! The durable keyed actor of guide/cluster.md § A key's state on disk, on the World: accounts kept in their node's
//! `files::Log`, deposited to from another node while the store's process or machine crashes, at any moment, its
//! writes panic, it crashes again while it opens the log, its disk fills, and a crowd of keys starts at once. The
//! program is the guide's, with smaller segments and checkpoints, and a question for the log's stats.
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use axon::files::{Address, Entry, Files, Log, LogConfig, LogError, LogStats, Lsn};
use axon::{Actor, ActorHandle, ActorSystem, Context, ReplyTo, RequestError};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, MemoryDiscovery, Placement, Rejection, Reply, SpawnContext,
    WireProtocol,
};
use axon_net::SharedSecret;
use axon_testkit::world::{Crash, Host, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::Bytes;

const SECRET: &str = "at-least-16-bytes-long";
/// Keys taking deposits at once.
const KEYS: usize = 6;
/// More keys than the log actor's `MailboxLimits::jobs` (16), and few enough that their records fit `max_bytes`.
const CROWD: usize = 24;
/// A header, the largest frame and a little more.
const SEGMENT: u64 = 68_000;
/// A record's length: its amount, then zeros.
const PAD: usize = 1000;
const REST: Duration = Duration::from_millis(100);

type Loaded = io::Result<(u64, Vec<Entry>)>;

enum ToLog {
    Opened(io::Result<Box<Log>>),
    Load(Bytes, ReplyTo<Loaded>),
    Read(Bytes, u64, io::Result<Vec<Entry>>, ReplyTo<Loaded>),
    Append(Bytes, u64, Bytes, ReplyTo<Result<Address, LogError>>),
    Checkpoint(Bytes, Address, Bytes),
    Written(io::Result<()>),
    Rested,
    /// The test's: the log's stats, how many wait for a write or a read, and the most live bytes of a key.
    Stats(ReplyTo<(LogStats, usize, u64)>),
}

enum Waiter {
    Load(Bytes, u64, ReplyTo<Loaded>),
    Append(Address, ReplyTo<Result<Address, LogError>>),
}

struct NodeLog {
    files: Arc<dyn Files>,
    log: Option<Log>,
    waiting: VecDeque<(Lsn, Waiter)>,
    to_read: VecDeque<(Bytes, u64, ReplyTo<Loaded>)>,
    reads: usize,
    resting: bool,
}

const READS: usize = 8;

impl NodeLog {
    fn new(files: Arc<dyn Files>) -> Self {
        let (waiting, to_read) = (VecDeque::new(), VecDeque::new());
        Self { files, log: None, waiting, to_read, reads: 0, resting: false }
    }
}

impl Actor for NodeLog {
    type Message = ToLog;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        // About the smallest segments the config allows, and records padded to PAD, so a run rolls through several
        // segments, removes them and copies live frames forward from them; each zero-filled a few records at a time, so
        // crashes land in writes that fill further too.
        let cfg = LogConfig {
            segment_bytes: SEGMENT,
            fill_ahead: 4 << 10,
            max_record: 1024,
            max_unwritten_bytes: 1 << 20,
            max_bytes: 256 << 10,
            ..LogConfig::default()
        };
        let open = Log::open(self.files.clone(), cfg);
        let job = cx.submit_future(async move { ToLog::Opened(open.await.map(Box::new)) });
        cx.pause_until(&job.map_err(|e| format!("{:?}", e.reason))?).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: ToLog, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(log) = self.log.as_mut() else {
            let ToLog::Opened(opened) = msg else { return Err("not open".into()) };
            self.log = Some(*opened.map_err(|e| e.to_string())?);
            return Ok(());
        };
        match msg {
            ToLog::Opened(_) => return Err("opened twice".into()),
            ToLog::Load(key, reply) => {
                let epoch = log.key(&key).map_or(1, |k| k.fence + 1);
                match log.fence(&key, epoch) {
                    Ok((lsn, _)) => self.waiting.push_back((lsn, Waiter::Load(key, epoch, reply))),
                    Err(refused) => drop(reply.reply(Err(io::Error::other(refused)))),
                }
            }
            ToLog::Read(key, epoch, Err(e), reply) if e.kind() == io::ErrorKind::NotFound => {
                self.reads -= 1;
                self.to_read.push_front((key, epoch, reply));
            }
            ToLog::Read(_, epoch, entries, reply) => {
                self.reads -= 1;
                drop(reply.reply(entries.map(|e| (epoch, e))));
            }
            ToLog::Append(key, epoch, body, reply) => match log.append(&key, epoch, body) {
                Ok(appended) => self.waiting.push_back((appended.lsn, Waiter::Append(appended.at, reply))),
                Err(refused) => drop(reply.reply(Err(refused))),
            },
            ToLog::Checkpoint(key, through, state) => drop(log.checkpoint(&key, through, state)),
            ToLog::Rested => self.resting = false,
            ToLog::Written(result) => {
                self.resting = result.is_err() && cx.schedule_after(REST, ToLog::Rested).is_ok();
                let durable = log.written(result);
                let done = self.waiting.partition_point(|(lsn, _)| *lsn <= durable);
                for (_, waiter) in self.waiting.drain(..done) {
                    match waiter {
                        Waiter::Load(key, epoch, reply) => self.to_read.push_back((key, epoch, reply)),
                        Waiter::Append(at, reply) => drop(reply.reply(Ok(at))),
                    }
                }
            }
            ToLog::Stats(reply) => {
                let waiting = self.waiting.len() + self.to_read.len() + self.reads;
                let most = log.keys(None, usize::MAX).iter().map(|(_, k)| k.live_bytes).max().unwrap_or(0);
                let _ = reply.reply((log.stats(), waiting, most));
            }
        }
        Ok(())
    }

    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(log) = self.log.as_mut() else { return Ok(()) };
        while self.reads < READS
            && let Some((key, epoch, reply)) = self.to_read.pop_front()
        {
            let read = log.read(&key, Address::default(), usize::MAX);
            let job = cx.submit_future(async move { ToLog::Read(key, epoch, read.await, reply) });
            job.map_err(|e| format!("{:?}", e.reason))?;
            self.reads += 1;
        }
        if !self.resting
            && let Some(write) = log.take_write(axon::trace::unix_micros() / 1000)
        {
            let job = cx.submit_future(async move {
                let written = axon::catch_panic(write).await;
                ToLog::Written(written.unwrap_or_else(|panic| Err(io::Error::other(panic.to_string()))))
            });
            job.map_err(|e| format!("{:?}", e.reason))?;
        }
        Ok(())
    }
}

struct AccountApi;

fn amount(bytes: &[u8]) -> Option<u64> {
    bytes.try_into().ok().map(u64::from_le_bytes)
}

impl WireProtocol for AccountApi {
    type Message = u64;
    type Reply = u64;
    const ID: u64 = 0xAD;
    const SCHEMA: u16 = 1;
    fn encode(n: u64) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&n.to_le_bytes()))
    }
    fn decode_reply(body: Bytes) -> Result<u64, CallError> {
        amount(&body).ok_or_else(|| CallError::Codec("not a u64".into()))
    }
}

impl ActorProtocol for AccountApi {
    type Mailbox = Msg;
    fn encode_reply(balance: u64) -> Result<Bytes, CallError> {
        Self::encode(balance)
    }
    fn decode(body: Bytes, reply: Option<Reply<u64>>) -> Result<Msg, Rejection> {
        amount(&body).map(|n| Msg::Call(n, reply)).ok_or(Rejection::InvalidMessage)
    }
}

enum Msg {
    Call(u64, Option<Reply<u64>>),
    Loaded(Result<Loaded, RequestError>),
    Written(Result<Result<Address, LogError>, RequestError>),
}

/// Often, so a run checkpoints each key many times.
const CHECKPOINT_EVERY: u64 = 8;

struct Account {
    key: Bytes,
    log: ActorHandle<ToLog>,
    epoch: u64,
    balance: u64,
    records: u64,
    queued: Vec<(u64, Option<Reply<u64>>)>,
    out: Vec<(u64, Option<Reply<u64>>)>,
}

impl Account {
    fn new(key: Bytes, log: ActorHandle<ToLog>) -> Self {
        Self { key, log, epoch: 0, balance: 0, records: 0, queued: Vec::new(), out: Vec::new() }
    }

    fn write(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if !self.out.is_empty() || self.queued.is_empty() {
            return Ok(());
        }
        self.out = std::mem::take(&mut self.queued);
        let sum: u64 = self.out.iter().map(|(n, _)| n).sum();
        let mut body = sum.to_le_bytes().to_vec();
        body.resize(PAD, 0);
        let body = Bytes::from(body);
        let (log, key, epoch) = (self.log.clone(), self.key.clone(), self.epoch);
        let job = cx.submit_future(async move { Msg::Written(log.ask(|r| ToLog::Append(key, epoch, body, r)).await) });
        job.map(drop).map_err(|e| format!("{:?}", e.reason))
    }
}

impl Actor for Account {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (log, key) = (self.log.clone(), self.key.clone());
        let job = cx.submit_future(async move { Msg::Loaded(log.ask(|r| ToLog::Load(key, r)).await) });
        cx.pause_until(&job.map_err(|e| format!("{:?}", e.reason))?).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Loaded(loaded) => {
                let (epoch, entries) = loaded.map_err(|e| e.to_string())?.map_err(|e| e.to_string())?;
                self.epoch = epoch;
                for entry in entries {
                    match entry {
                        Entry::Checkpoint { state, .. } => self.balance = amount(&state).ok_or("not a u64")?,
                        Entry::Record { body, .. } => {
                            self.balance += amount(body.get(..8).unwrap_or_default()).ok_or("not a u64")?;
                            self.records += 1;
                        }
                    }
                }
            }
            Msg::Call(0, reply) => drop(reply.map(|r| r.send(self.balance))),
            Msg::Call(n, reply) => {
                self.queued.push((n, reply));
                self.write(cx)?;
            }
            Msg::Written(Ok(Ok(at))) => {
                for (n, reply) in std::mem::take(&mut self.out) {
                    self.balance += n;
                    drop(reply.map(|r| r.send(self.balance)));
                }
                self.records += 1;
                if self.records >= CHECKPOINT_EVERY {
                    let state = Bytes::copy_from_slice(&self.balance.to_le_bytes());
                    if self.log.tell(ToLog::Checkpoint(self.key.clone(), at, state)).is_ok() {
                        self.records = 0;
                    }
                }
                self.write(cx)?;
            }
            Msg::Written(Ok(Err(refused))) if !matches!(refused, LogError::Fenced { .. }) => {
                self.out.clear();
                self.write(cx)?;
            }
            Msg::Written(other) => return Err(format!("{other:?}")),
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Node {
    cluster: Cluster,
    log: ActorHandle<ToLog>,
    _system: ActorSystem,
}

fn store() -> Placement {
    Placement::default().require("role", "store")
}

/// The accounts live on `store`, which keeps their log on its disk; `client` deposits.
async fn boot(host: Host, discovery: MemoryDiscovery) -> Node {
    let role = host.name().to_owned();
    let mut cfg = ClusterConfig::new(host.addr(), host.transport(SharedSecret::new(SECRET).unwrap()), discovery);
    cfg.labels.insert("role".into(), role.clone());
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let files: Arc<dyn Files> = Arc::new(host.disk().dir("accounts"));
    let log = system.spawn(NodeLog::new(files)).start().unwrap();
    log.ready().await.expect("the log opens");
    if role == "store" {
        let accounts = log.clone();
        cluster
            .kind::<AccountApi>("account")
            .register(move |cx: &SpawnContext, _args| {
                Ok(Account::new(cx.key.clone().unwrap_or_default().into(), accounts.clone()))
            })
            .await
            .unwrap();
    }
    Node { cluster, log, _system: system }
}

fn up(w: &World<Node>, name: &str) -> bool {
    w.nodes().any(|(n, node)| n == name && node.cluster.view().members.iter().any(|m| m.record.has_kind("account")))
}

/// Per key: the deposits tried, and each balance answered, in order.
#[derive(Clone, Default)]
struct Calls {
    tried: u64,
    answered: Vec<u64>,
}

type Ledger = Rc<RefCell<BTreeMap<String, Calls>>>;

/// One depositor a key of the ledger, each a deposit of 1 at a time, until `stop`.
fn depositors(w: &mut World<Node>, ledger: &Ledger, stop: &Rc<Cell<bool>>) {
    let keys: Vec<String> = ledger.borrow().keys().cloned().collect();
    for key in keys {
        let (ledger, stop) = (ledger.clone(), stop.clone());
        drop(w.spawn_on("client", move |n| async move {
            let account = n.cluster.keyed::<AccountApi>("account", key.as_str(), Bytes::new(), &store());
            while !stop.get() {
                ledger.borrow_mut().get_mut(&key).expect("a key of the ledger").tried += 1;
                if let Ok(balance) = account.request(1).await {
                    ledger.borrow_mut().get_mut(&key).expect("a key of the ledger").answered.push(balance);
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }));
    }
}

fn answered(ledger: &Ledger) -> Vec<usize> {
    ledger.borrow().values().map(|c| c.answered.len()).collect()
}

fn world(seed: u64, keys: usize) -> (World<Node>, Ledger, Rc<Cell<bool>>) {
    let discovery = MemoryDiscovery::new();
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    for name in ["store", "client"] {
        let discovery = discovery.clone();
        w.add(name, move |host| boot(host, discovery.clone()));
    }
    w.run_until("the client sees the store", Duration::from_secs(30), |w| up(w, "client"));
    let ledger: Ledger = Rc::new(RefCell::new((0..keys).map(|i| (format!("k{i}"), Calls::default())).collect()));
    let stop = Rc::new(Cell::new(false));
    depositors(&mut w, &ledger, &stop);
    let before = Duration::from_millis(w.pick(20..400));
    w.run_for(before);
    (w, ledger, stop)
}

/// Each key is answered again after `what`, and deposits go on a while. Then: each balance answered is higher than
/// the last answered for its key, as a deposit answered was durable; each balance is at least the last answered and
/// at most the deposits tried; the log has nothing left to write or read; and no key holds a checkpoint's worth of
/// records.
fn settle(mut w: World<Node>, ledger: &Ledger, stop: &Rc<Cell<bool>>, seed: u64, what: &str) -> World<Node> {
    let before = answered(ledger);
    w.run_until(&format!("every key answered after {what}"), Duration::from_secs(30), |_| {
        answered(ledger).iter().zip(&before).all(|(now, before)| now > before)
    });
    let more = Duration::from_millis(w.pick(0..300));
    w.run_for(more);
    stop.set(true);
    w.run_for(Duration::from_secs(3));

    let ledger = ledger.borrow().clone();
    for (key, calls) in ledger {
        let lost = calls.answered.windows(2).find(|w| w[1] <= w[0]);
        assert!(lost.is_none(), "seed {seed}: {key} answered {lost:?}, a deposit answered was lost to {what}");
        let account = key.clone();
        let balance = w
            .block_on("client", move |n| async move {
                n.cluster.keyed::<AccountApi>("account", &account, Bytes::new(), &store()).request(0).await
            })
            .unwrap()
            .unwrap_or_else(|e| panic!("seed {seed}: {key} read: {e:?}"));
        let last = calls.answered.last().copied().unwrap_or(0);
        assert!(
            (last..=calls.tried).contains(&balance),
            "seed {seed}: {key} holds {balance}, {last} was answered last and {} tried",
            calls.tried
        );
    }
    let (stats, waiting, most) = log_stats(&mut w);
    // Fewer than CHECKPOINT_EVERY records past its checkpoint, however often the key started.
    let bound = CHECKPOINT_EVERY * (PAD as u64 + 64);
    assert!(most < bound, "seed {seed}: a key holds {most} live bytes after {what}: {stats:?}");
    assert_eq!((stats.unwritten_bytes, waiting, w.disk("store").panics_left()), (0, 0, 0), "seed {seed}");
    println!("log stats {what} seed {seed}: {stats:?}");
    println!("world digest log {what} seed {seed}: {:016x}", w.digest());
    w
}

fn log_stats(w: &mut World<Node>) -> (LogStats, usize, u64) {
    w.block_on("store", |n| async move { n.log.ask(ToLog::Stats).await }).unwrap().expect("the log answers")
}

/// The store crashes `how` at a random moment while every key takes deposits, and in half the seeds again while it
/// opens the log; in some, writes panic before.
fn crash_at_any_moment(seed: u64, how: Crash) -> World<Node> {
    let (mut w, ledger, stop) = world(seed, KEYS);
    let panics = w.pick(0..4) as u32;
    w.disk("store").panic_writes(panics);
    let after = Duration::from_millis(w.pick(0..20));
    w.run_for(after);

    w.log(format_args!("store {how:?}"));
    w.crash("store", how);
    let down = Duration::from_millis(w.pick(0..300));
    w.run_for(down);
    w.restart("store");
    if w.pick(0..2) == 0 {
        // Booting, opening the log or loading a key: the second crash lands somewhere in there.
        let opening = Duration::from_millis(w.pick(0..30));
        w.run_for(opening);
        w.log(format_args!("store {how:?} again"));
        w.crash("store", how);
        w.restart("store");
    }
    settle(w, &ledger, &stop, seed, &format!("a {how:?} crash"))
}

/// What the page cache held is gone, and a seeded part of the last unsynced write with it: a deposit answered was
/// synced, so it is not.
#[test]
fn a_deposit_answered_survives_a_machine_crash_at_any_moment() {
    check_seeds(seeds(0..64), |seed| drop(crash_at_any_moment(seed, Crash::Machine))).unwrap();
}

#[test]
fn a_deposit_answered_survives_a_process_crash_at_any_moment() {
    check_seeds(seeds(0..64), |seed| drop(crash_at_any_moment(seed, Crash::Process))).unwrap();
}

/// The disk fills while every key takes deposits, the next segment cannot be made, and writes fail until the disk
/// has room again; in half the seeds the machine crashes meanwhile. Deposits wait, none answered is lost, and the
/// log writes again, resting between tries: without the rest it tried again at once, and the World never got past
/// the first failure.
#[test]
fn a_full_disk_holds_deposits_until_it_has_room_and_loses_none() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, ledger, stop) = world(seed, KEYS);
        let disk = w.disk("store");
        let room = w.pick(0..2000);
        disk.set_capacity(Some(disk.list().iter().map(|(_, len)| len).sum::<u64>() + room));
        let full = Duration::from_millis(w.pick(300..1500));
        w.run_for(full);
        let (stats, _, _) = log_stats(&mut w);
        assert!(stats.failed_writes > 0 && stats.unwritten_bytes > 0, "seed {seed}: the disk never filled: {stats:?}");
        if w.pick(0..2) == 0 {
            w.crash("store", Crash::Machine);
            w.restart("store");
        }
        let still = Duration::from_millis(w.pick(0..500));
        w.run_for(still);
        disk.set_capacity(None);
        drop(settle(w, &ledger, &stop, seed, "a full disk"));
    })
    .unwrap();
}

/// After a restart more keys start at once than the log actor may run jobs: each loads, and the log goes on writing.
#[test]
fn a_crowd_of_keys_starting_at_once_each_load() {
    check_seeds(seeds(0..64), |seed| {
        let (mut w, ledger, stop) = world(seed, CROWD);
        w.crash("store", Crash::Process);
        let down = Duration::from_millis(w.pick(0..300));
        w.run_for(down);
        w.restart("store");
        drop(settle(w, &ledger, &stop, seed, "a crowd of starts"));
    })
    .unwrap();
}

#[test]
#[cfg_attr(not(tokio_unstable), ignore = "reproducible only in the sim build: scripts/check.sh sim")]
fn the_same_seed_gives_the_same_trace() {
    check_seeds(seeds(0..2), |seed| {
        let (one, two) = (crash_at_any_moment(seed, Crash::Machine), crash_at_any_moment(seed, Crash::Machine));
        if let Some(report) = one.divergence(&two) {
            panic!("{report}");
        }
    })
    .unwrap();
}
