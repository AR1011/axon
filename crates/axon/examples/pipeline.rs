//! A durable request pipeline: clients → keyed owner shards → one log writer per shard → ack to the client.
//!
//! cargo run --release -p axon-actors --example pipeline -- [shards=4] [clients=16] [window=64] [keys=100000] [items=500]
//!     [holds=4] [feed=100000] [feed_batch=64] [secs=10] [warmup=2] [turn=1024] [sync=1] [dir=/tmp] [workers=8]
//!     [pin=0] [hot=0]
//!
//! Each client keeps `window` requests in flight and sends the next when one is acknowledged. A request names a key;
//! the shard that owns the key checks it against the key's holdings, priced from a table of `items` reference values,
//! and passes it to its log writer. The writer appends the turn's entries to a preallocated file, `fdatasync`s it
//! (`sync=0` skips that) and acknowledges each client once per sync. A feed thread sends `feed` reference updates a
//! second to every shard in batches of `feed_batch`, dropping them (`feed_dropped`) while a shard is `FEED_SLOTS`
//! behind; a shard re-checks every key holding an updated item. Shards and writers are `Execution::Dedicated`, clients
//! shared on `workers` workers. With `pin=1` the shared workers are pinned to the first cores this process may run on,
//! the shards to a pool of one worker each on the next, and the writers likewise after them, none passing actors
//! between workers; `hot=1` also keeps those two pools from sleeping. Latency is from the client's send to its ack.
#![expect(clippy::disallowed_methods, reason = "a benchmark measures real time")]

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axon::*;

const ENTRY: usize = 32;
const LOG_BYTES: u64 = 64 << 20;
/// Feed messages a shard may have queued; the feed drops past them. The shard's mailbox has room for these, every
/// client's window and one turn (a turn's slots are freed only when it ends), so an open-loop feed a shard cannot keep
/// up with never takes a client's slot.
const FEED_SLOTS: usize = 1024;

struct Args {
    shards: usize,
    clients: usize,
    window: usize,
    keys: usize,
    items: usize,
    holds: usize,
    feed: u64,
    feed_batch: usize,
    secs: u64,
    warmup: u64,
    turn: usize,
    sync: bool,
    dir: String,
    workers: usize,
    pin: bool,
    hot: bool,
}

fn args() -> Args {
    let mut a = Args {
        shards: 4,
        clients: 16,
        window: 64,
        keys: 100_000,
        items: 500,
        holds: 4,
        feed: 100_000,
        feed_batch: 64,
        secs: 10,
        warmup: 2,
        turn: 1024,
        sync: true,
        dir: std::env::temp_dir().display().to_string(),
        workers: RuntimeConfig::default().workers,
        pin: false,
        hot: false,
    };
    for arg in std::env::args().skip(1) {
        let (k, v) = arg.split_once('=').unwrap_or_else(|| panic!("expected key=value, got {arg}"));
        let n = || v.parse::<u64>().unwrap_or_else(|_| panic!("{k}={v} is not a number"));
        match k {
            "shards" => a.shards = n() as usize,
            "clients" => a.clients = n() as usize,
            "window" => a.window = n() as usize,
            "keys" => a.keys = n() as usize,
            "items" => a.items = n() as usize,
            "holds" => a.holds = n() as usize,
            "feed" => a.feed = n(),
            "feed_batch" => a.feed_batch = n() as usize,
            "secs" => a.secs = n(),
            "warmup" => a.warmup = n(),
            "turn" => a.turn = n() as usize,
            "sync" => a.sync = n() != 0,
            "workers" => a.workers = n() as usize,
            "pin" => a.pin = n() != 0,
            "hot" => a.hot = n() != 0,
            "dir" => a.dir = v.to_string(),
            _ => panic!("unknown argument {k}"),
        }
    }
    a
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }
}

#[derive(Clone, Copy)]
struct Request {
    client: u16,
    key: u32,
    item: u16,
    qty: i32,
    sent: Instant,
}

enum ClientMsg {
    Start(Vec<ActorRef<ShardMsg>>),
    Acked(Vec<Instant>),
    Report(ReplyTo<Vec<u32>>),
}

struct Client {
    id: u16,
    shards: Vec<ActorRef<ShardMsg>>,
    rng: Rng,
    keys: usize,
    items: usize,
    window: usize,
    measure: Instant,
    until: Instant,
    latencies: Vec<u32>,
}

impl Client {
    fn send(&mut self) {
        let key = self.rng.below(self.keys);
        let request = Request {
            client: self.id,
            key: key as u32,
            item: self.rng.below(self.items) as u16,
            qty: self.rng.below(200) as i32 - 100,
            sent: Instant::now(),
        };
        let shards = self.shards.len();
        self.shards[key % shards].try_tell(ShardMsg::Request(request)).expect("shard mailboxes hold every window");
    }
}

impl Actor for Client {
    type Message = ClientMsg;
    type Error = ();

    fn on_message(&mut self, msg: ClientMsg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            ClientMsg::Start(shards) => {
                self.shards = shards;
                (0..self.window).for_each(|_| self.send());
            }
            ClientMsg::Acked(sent) => {
                let now = Instant::now();
                for sent in sent {
                    if sent >= self.measure && now < self.until {
                        self.latencies.push((now - sent).as_nanos().min(u32::MAX as u128) as u32);
                    }
                    if now < self.until {
                        self.send();
                    }
                }
            }
            ClientMsg::Report(reply) => {
                let _ = reply.reply(std::mem::take(&mut self.latencies));
            }
        }
        Ok(())
    }
}

enum ShardMsg {
    Request(Request),
    Feed(Arc<[(u16, i64)]>),
}

struct Key {
    holds: Vec<(u16, i32)>,
    limit: i64,
}

struct Shard {
    shards: usize,
    log: ActorRef<Vec<Request>>,
    keys: Vec<Key>,
    values: Vec<i64>,
    holders: Vec<Vec<u32>>,
    pending: Vec<Request>,
    breaches: Arc<AtomicU64>,
    feed_queued: Arc<AtomicUsize>,
}

impl Shard {
    fn exposure(&self, key: usize) -> i64 {
        self.keys[key].holds.iter().map(|&(item, qty)| self.values[item as usize] * qty as i64).sum()
    }
}

impl Actor for Shard {
    type Message = ShardMsg;
    type Error = ();

    fn on_message(&mut self, msg: ShardMsg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            ShardMsg::Request(request) => {
                let key = request.key as usize / self.shards;
                let value = self.values[request.item as usize];
                if (self.exposure(key) + value * request.qty as i64).abs() <= self.keys[key].limit
                    && let Some((_, qty)) = self.keys[key].holds.iter_mut().find(|(item, _)| *item == request.item)
                {
                    *qty += request.qty;
                }
                self.pending.push(request);
            }
            ShardMsg::Feed(updates) => {
                let mut breaches = 0;
                for &(item, value) in updates.iter() {
                    self.values[item as usize] = value;
                    for &key in &self.holders[item as usize] {
                        breaches += u64::from(self.exposure(key as usize).abs() > self.keys[key as usize].limit);
                    }
                }
                self.breaches.fetch_add(breaches, Relaxed);
                self.feed_queued.fetch_sub(1, Relaxed);
            }
        }
        Ok(())
    }

    fn on_turn_end(&mut self, _: &mut Context<'_, Self>) -> Result<(), ()> {
        if !self.pending.is_empty() {
            let entries = std::mem::replace(&mut self.pending, Vec::with_capacity(256));
            self.log.try_tell(entries).expect("log mailboxes hold every window");
        }
        Ok(())
    }
}

struct Log {
    file: File,
    at: u64,
    buf: Vec<u8>,
    sync: bool,
    clients: Arc<Mutex<Vec<ActorRef<ClientMsg>>>>,
    acks: Vec<Vec<Instant>>,
    syncs: Arc<AtomicU64>,
    entries: Arc<AtomicU64>,
}

impl Actor for Log {
    type Message = Vec<Request>;
    type Error = ();

    fn on_message(&mut self, entries: Vec<Request>, _: &mut Context<'_, Self>) -> Result<(), ()> {
        for r in &entries {
            self.buf.extend_from_slice(&r.key.to_le_bytes());
            self.buf.extend_from_slice(&r.item.to_le_bytes());
            self.buf.extend_from_slice(&r.client.to_le_bytes());
            self.buf.extend_from_slice(&r.qty.to_le_bytes());
            self.buf.extend_from_slice(&[0; ENTRY - 12]);
            self.acks[r.client as usize].push(r.sent);
        }
        Ok(())
    }

    fn on_turn_end(&mut self, _: &mut Context<'_, Self>) -> Result<(), ()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        if self.at + self.buf.len() as u64 > LOG_BYTES {
            self.at = 0;
        }
        self.file.write_all_at(&self.buf, self.at).expect("log write");
        if self.sync {
            self.file.sync_data().expect("fdatasync");
        }
        self.at += self.buf.len() as u64;
        self.entries.fetch_add((self.buf.len() / ENTRY) as u64, Relaxed);
        self.syncs.fetch_add(1, Relaxed);
        self.buf.clear();
        let clients = self.clients.lock().expect("clients lock");
        for (client, acks) in clients.iter().zip(&mut self.acks) {
            if !acks.is_empty() {
                let acks = std::mem::replace(acks, Vec::with_capacity(64));
                client.try_tell(ClientMsg::Acked(acks)).expect("client mailboxes hold every window");
            }
        }
        Ok(())
    }
}

fn runtime(a: &Args) -> RuntimeConfig {
    let turn_budget = TurnBudget::messages(a.turn);
    let mut config = RuntimeConfig { workers: a.workers, turn_budget, ..Default::default() };
    if a.pin {
        let allowed = RuntimeConfig::allowed_cores();
        let cores = |first: usize, n: usize| {
            allowed.get(first..first + n).expect("pin=1 takes workers + 2 * shards cores").to_vec()
        };
        let pool = |name: &str, first: usize| PoolConfig {
            name: name.into(),
            workers: a.shards,
            cores: cores(first, a.shards),
            spin: if a.hot { Duration::MAX } else { config.spin }, // hot=1: the shards' workers never sleep
            balance: false,
            turn_budget,
        };
        config.cores = cores(0, a.workers);
        config.pools = vec![pool("shards", a.workers), pool("logs", a.workers + a.shards)];
    }
    config
}

fn percentile(sorted: &[u32], p: f64) -> f64 {
    sorted
        .get(((sorted.len() as f64 * p) as usize).min(sorted.len().saturating_sub(1)))
        .map_or(0.0, |&ns| ns as f64 / 1e3)
}

#[tokio::main]
async fn main() {
    let a = args();
    let in_flight = a.clients * a.window;
    let system = ActorSystem::builder().runtime(runtime(&a)).build().expect("system");
    let limits = |messages: usize| MailboxLimits { messages, bytes: 1 << 30, ..Default::default() };
    let (syncs, entries, breaches) =
        (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let clients_of_logs = Arc::new(Mutex::new(Vec::new()));

    let (mut shards, mut feeds) = (Vec::new(), Vec::new());
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for s in 0..a.shards {
        let path = std::path::Path::new(&a.dir).join(format!("axon-pipeline-{s}.log"));
        let file = File::options().read(true).write(true).create(true).truncate(true).open(&path).expect("log file");
        file.write_all_at(&vec![0; LOG_BYTES as usize], 0).expect("preallocate log");
        file.sync_all().expect("sync preallocated log");
        let log = system
            .spawn(Log {
                file,
                at: 0,
                buf: Vec::with_capacity(1 << 20),
                sync: a.sync,
                clients: clients_of_logs.clone(),
                acks: vec![Vec::new(); a.clients],
                syncs: syncs.clone(),
                entries: entries.clone(),
            })
            .name(format!("log-{s}"))
            .execution(if a.pin { Execution::Pool("logs") } else { Execution::Dedicated })
            .mailbox(limits(in_flight + 16))
            .start()
            .expect("log");

        let feed_queued = Arc::new(AtomicUsize::new(0));
        let owned = a.keys / a.shards;
        let mut holders = vec![Vec::new(); a.items];
        let keys = (0..owned)
            .map(|k| {
                let holds: Vec<_> = (0..a.holds)
                    .map(|_| {
                        let item = rng.below(a.items);
                        holders[item].push(k as u32);
                        (item as u16, rng.below(20) as i32)
                    })
                    .collect();
                Key { holds, limit: 1_000_000 }
            })
            .collect();
        let shard = system
            .spawn(Shard {
                shards: a.shards,
                log: log.address.clone(),
                keys,
                values: vec![1_000; a.items],
                holders,
                pending: Vec::with_capacity(256),
                breaches: breaches.clone(),
                feed_queued: feed_queued.clone(),
            })
            .name(format!("shard-{s}"))
            .execution(if a.pin { Execution::Pool("shards") } else { Execution::Dedicated })
            .mailbox(limits(in_flight + FEED_SLOTS + a.turn.max(1)))
            .start()
            .expect("shard");
        shards.push(shard.address.clone());
        feeds.push((shard.address.clone(), feed_queued));
    }

    let start = Instant::now();
    let (measure, until) = (start + Duration::from_secs(a.warmup), start + Duration::from_secs(a.warmup + a.secs));
    let clients: Vec<_> = (0..a.clients)
        .map(|c| {
            system
                .spawn(Client {
                    id: c as u16,
                    shards: Vec::new(),
                    rng: Rng(c as u64 * 0x2545_f491_4f6c_dd1d + 1),
                    keys: a.keys / a.shards * a.shards,
                    items: a.items,
                    window: a.window,
                    measure,
                    until,
                    latencies: Vec::with_capacity(1 << 20),
                })
                .name(format!("client-{c}"))
                .mailbox(limits(a.window + a.shards + 16))
                .start()
                .expect("client")
        })
        .collect();
    *clients_of_logs.lock().expect("clients lock") = clients.iter().map(|c| c.address.clone()).collect();

    let stop = Arc::new(AtomicBool::new(false));
    let fed = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));
    let feed = (a.feed > 0).then(|| {
        let (stop, fed, dropped) = (stop.clone(), fed.clone(), dropped.clone());
        let (items, batch, rate) = (a.items, a.feed_batch, a.feed);
        std::thread::spawn(move || {
            let mut rng = Rng(7);
            let began = Instant::now();
            let mut sent = 0u64;
            while !stop.load(Relaxed) {
                let due = began + Duration::from_nanos(sent * 1_000_000_000 / rate);
                if let Some(wait) = due.checked_duration_since(Instant::now()) {
                    std::thread::sleep(wait);
                }
                let updates: Arc<[(u16, i64)]> =
                    (0..batch).map(|_| (rng.below(items) as u16, 900 + rng.below(200) as i64)).collect();
                for (shard, queued) in &feeds {
                    // Counted before the send, so the shard's decrement cannot come first. This is the only thread that
                    // adds, so the check and the add cannot race past FEED_SLOTS.
                    if queued.load(Relaxed) < FEED_SLOTS {
                        queued.fetch_add(1, Relaxed);
                        if shard.try_tell(ShardMsg::Feed(updates.clone())).is_ok() {
                            continue;
                        }
                        queued.fetch_sub(1, Relaxed);
                    }
                    dropped.fetch_add(1, Relaxed);
                }
                sent += batch as u64;
                fed.store(sent, Relaxed);
            }
        })
    });

    for client in &clients {
        client.tell(ClientMsg::Start(shards.clone())).expect("start client");
    }
    tokio::time::sleep(Duration::from_secs(a.warmup)).await;
    let (syncs0, entries0, fed0, dropped0) =
        (syncs.load(Relaxed), entries.load(Relaxed), fed.load(Relaxed), dropped.load(Relaxed));
    tokio::time::sleep(until.saturating_duration_since(Instant::now())).await;
    let (syncs1, entries1, fed1, dropped1) =
        (syncs.load(Relaxed), entries.load(Relaxed), fed.load(Relaxed), dropped.load(Relaxed));
    stop.store(true, Relaxed);
    if let Some(feed) = feed {
        feed.join().expect("feed thread");
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut latencies = Vec::new();
    for client in &clients {
        latencies.extend(client.ask(ClientMsg::Report).await.expect("client report"));
    }
    latencies.sort_unstable();
    let secs = a.secs as f64;
    let (syncs, entries) = ((syncs1 - syncs0) as f64, (entries1 - entries0) as f64);
    println!(
        "pipeline: shards={} clients={} window={} sync={} workers={} pin={} hot={} req/s={:.0} p50={:.1}us p99={:.1}us p99.9={:.1}us max={:.1}us \
         syncs/s={:.0} entries/sync={:.1} feed/s={:.0} feed_dropped={} breaches={}",
        a.shards,
        a.clients,
        a.window,
        a.sync,
        a.workers,
        a.pin,
        a.hot,
        latencies.len() as f64 / secs,
        percentile(&latencies, 0.5),
        percentile(&latencies, 0.99),
        percentile(&latencies, 0.999),
        latencies.last().map_or(0.0, |&ns| ns as f64 / 1e3),
        syncs / secs,
        entries / syncs.max(1.0),
        (fed1 - fed0) as f64 / secs,
        dropped1 - dropped0,
        breaches.load(Relaxed),
    );
    system.shutdown().await.expect("shutdown");
    for s in 0..a.shards {
        let _ = std::fs::remove_file(std::path::Path::new(&a.dir).join(format!("axon-pipeline-{s}.log")));
    }
}
