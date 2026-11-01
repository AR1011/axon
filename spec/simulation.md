# Whole-cluster simulation

`spec/faults.md` points here.

## Goal

Run the real `Cluster`, `StreamNode`, keyed actors, `PaxosLeases`, `MemoryLeases`, `MemoryDiscovery`, seed discovery and user actors on N simulated nodes. Everything runs in one process, on one thread, and a seed drives it. The same seed gives the same trace.

The simulation controls:

- time: each node has its own virtual clock, with drift and wall-clock steps;
- the network: latency, jitter, stalls, resets, partitions and one-way cuts;
- process crashes and restarts: memory is lost and the incarnation changes;
- pauses, as with SIGSTOP, and suspensions, as of a VM, whose clocks stand still meanwhile;
- CPU starvation, and handler cost per turn.

A simulation test reads like an ordinary cluster test and asserts properties. Features that live on faults are proven
here: a crashed node's leased keys restarting elsewhere, per-key owner events, a stream follower told its origin
crashed, call deadlines on the wire.

This is also where thousands of seeds run across machines to find races.

The constraints come from the user:

- The framework must not get harder to use.
- Production code gets no sim-only branches, no cfg and no per-message cost.
- No code is duplicated or left dead.
- A new dependency is added only if it clearly beats a few hundred lines.
- Runs are seeded (`SIM_SEED=n`, `SEEDS=a..b`), and a same-seed-twice check guards them.

**Scope.** The seed varies:

- the order in which nodes are stepped;
- the order in which chunks are delivered;
- jitter;
- fault timing;
- handler cost.

It does not vary the order of tasks inside one node. Those run first in, first out, so the simulation finds races between nodes and races against time, but not a different ordering of two ready tasks on one node. loom covers the mailbox and a turn's start, the idle workers' sleep and wake, a remote reply's state and a session's write order (`scripts/check.sh loom`). Exploring orderings inside a node would need a seeded scheduler, which is out of scope here (Risks, item 4).

## Decision

**We build our own harness, the `World`, in `axon-testkit`.** It uses turmoil's technique without taking turmoil as a dependency:

- Each node gets its own paused `current_thread` tokio runtime.
- One thread steps every node in 1 ms quanta, in an order drawn from the seed.
- Actors run as a task on their node's runtime. Tokio's auto-advance therefore sees actor work, and actor time is the node's tokio time. Each node has one clock.

This needs two seams in production code:

1. A threadless mode for the actor runtime.
2. `axon_net::Config::network`, so that sessions take their bytes from a trait object instead of from `TcpStream` and `TcpListener`.

Everything above the byte stream runs unchanged: handshake, TLS, framing, credit, keepalive, idle detection and the accept limits. The rest is small fixes at existing chokepoints:

- `Fence` reads the actor clock.
- The std `Instant` sites below the actors are tokio `Instant`s.
- The wall clock goes through one thread-local hook in `axon::trace`.
- Randomness goes through two chokepoints the same hook seeds: `axon::trace::random_u64` and axon-net's `config.rs` `random<N>`.
- Iterated maps have a fixed order.

**Tokio's own randomness is seeded in a separate sim build rather than removed from production code.** tokio 1.53.1 draws on a per-runtime RNG in two places:

- the branch order of `select!`;
- the `watch` channel's choice of one of 8 `Notify` shards for each waiter (`sync/watch.rs:420-423`). When several tasks wait on the same watch, they wake in random order. That happens in the engine (every call to a peer that is still connecting waits on one), the session and seed discovery.

`biased;` fixes `select!` but not `watch`. It also changes how production detects failures. A tick-first `select!` in the session's read loop turns every pause longer than `idle_timeout` into certain session loss, even when the peer's bytes are already buffered.

So `scripts/check.sh sim` builds with `RUSTFLAGS="--cfg tokio_unstable"` in `target/sim`, and the World seeds each node runtime with `Builder::rng_seed`. That call is the only cfg, and it sits in test code. The 12 unbiased `select!` sites stay as they are.

A plain `cargo test` still runs every World test and checks every property, but tokio's choices are not reproducible there. The same-seed tests run only in the sim build. That `rng_seed` fixes both `select!` order and the order in which watch shards wake was tested before the World was built; § What holds it says where it holds.

**turmoil was rejected**, for these reasons (checked in 0.7.2, `top.rs` and `host.rs:400-417`):

- Its TCP drops segments on a partition and never retransmits them. Any cut that loses a byte wedges the connection until axon's idle timeout, which is not what TCP does.
- It has no pause, no rate skew and no slow node.
- Its per-host `step` freezes the clock of any host that is not stepped.
- `crash` cannot reach axon's worker threads.
- Production code would need turmoil's net types behind a type alias.
- We would need the same runtime seam anyway, and soon a fork.

turmoil-net (0.1) is a TCP state machine with no clock of its own. It could later replace our byte pipe if the pipe proves too simple.

**madsim was rejected.** It swaps tokio out for the whole build with `--cfg madsim`, needs a package rename in every crate, and forbids OS threads. That means cfg branches through the whole code base, and it has no per-node clock skew.

**Shadow, Hermit and Antithesis were rejected as the proving ground.** They run processes, not in-process tests. Antithesis can later check black-box runs of the real binaries.

**`EmbeddedDriver` was rejected as the vehicle.** It drives one actor, and using it would duplicate `ActorSystem`'s wiring for names, supervision and shutdown.

## Architecture and seams

### How a world runs

```
one OS thread (the #[test])
└─ World: time T, quantum q = 1 ms
   ├─ wires: directed links, pending BTreeMap<(deliver_at, seq), Chunk>
   ├─ node "a": current_thread runtime, start_paused, enable_time only (no IO driver), LocalSet
   │    tasks: engine, sessions, paxos, seed, stream node, test tasks, and the actor pump
   ├─ node "b": the same, with its own clock
   └─ thread-locals: the seeded generator, and the wall clock of the node being stepped
```

Each quantum does five things, in order:

1. Apply queued faults. This happens between `block_on`s, never inside one.
2. Deliver the chunks that are due. Chunks due at the same boundary go out in an order drawn from the seed, which gives reordering across links within a quantum.
3. For each live node, in seeded order:
   - skip the node if it is paused, or starved this quantum;
   - otherwise set the wall hook to that node;
   - `advance` its clock to its local time minus one quantum. For a node that was starved, this jump fires its overdue timers together, and the node never handles bytes before they arrived;
   - then `block_on(sleep(q))`, which runs every ready task and auto-advances through the node's own timers.
4. Record the quantum's digest.
5. `T += q`.

A node runtime has no IO driver, so a real socket used inside a node panics instead of leaking real time into the run. Nodes boot on the first step, in the order they were added, so `Host::ip_of` works for every name.

Test code is plain synchronous code that drives the World. There is no tester runtime. Node work goes through `spawn_on`, `block_on` and `join`, whose futures run on the node's `LocalSet`, and a read of a node's clocks or state through `read`, which runs under `rt.enter()` and `as_node` and steps nothing.

Anything that holds a tokio `Instant` or a `Clock` (`PaxosLeases`, deadlines) is built inside `boot` or `spawn_on`, never in the test body. The reason: outside a runtime, tokio's `Instant::now` falls back to real time (`time/clock.rs:284-295`). A node reads time from its system's clock (`system.clock()`, `cluster.clock()`): the actor `Clock` of a threadless runtime enters the node's runtime to read it, so the test body, a `run_until` predicate, reads the node's time through any of the node's public reads. A seed server and seed discovery start without a system, so their public reads enter the runtime they started on.

### Seam 1: the threadless runtime (`crates/axon`)

```rust
// runtime.rs
pub struct RuntimeConfig { /* unchanged fields */ pub threadless: Option<Threadless> }

/// Runs every actor of an ActorSystem on one task of the tokio runtime it is built in, with that runtime's clock.
/// For simulators. Applications leave it `None`.
#[doc(hidden)]
#[derive(Clone, Default)]
pub struct Threadless(/* Arc<OnceLock<Weak<runtime::Inner>>>, held: AtomicBool */);
impl Threadless {
    /// While held, the pump runs no turns: a handler that never returns, or a stalled worker. Timers and I/O go on.
    pub fn hold(&self, held: bool);
    /// Virtual time each actor turn takes (`World::cost`); see the pump below.
    pub fn turn_cost(&self, cost: Duration);
    /// A crash: marks every driver stopped, so no on_stop, Exit or child stop runs, and clears queues,
    /// registry and clock wakes. That breaks the Pool->Queue->Cell->Pool and Clock->Cell cycles.
    pub fn forsake(&self);
}

// system.rs (already behind feature "system")
impl ActorSystem { pub fn clock(&self) -> &Clock; }
```

How it behaves:

- **Building.** `ActorSystemBuilder::build` with `threadless: Some(..)` starts no worker, dedicated or watchdog thread. It builds the runtime's clock as a driven clock that reads the node's tokio time through the captured `Handle`, and it spawns one pump task.
- **Refused settings.** A non-default `clock` is refused with `SystemError::Configuration`. `Runtime::new` refuses a threadless config, because only an `ActorSystem` drives one, and a second runtime on one `Threadless` is refused; both with `SpawnError::InvalidPool`.
- **`workers: 0`** keeps its current meaning: it clamps to 1.
- **Placement.** In threadless mode every `Execution` (Shared, Pool, Dedicated) goes to one queue. Dedicated's `dedicated` flag is cleared so that `Cell::run` cannot shut the shared pool down. Pool isolation is therefore not modelled.
- **Jobs.** With no `job_executor`, blocking jobs run as tasks on the node. `spawn_blocking` is never used.
- **The pump.**
  - It runs up to 64 turns, then calls `yield_now`.
  - The turn cost. `tokio::time::advance` needs tokio's `test-util` in a published crate, and a `sleep(cost)` after each turn is rounded up to tokio's 1 ms tick (100 turns of 10 µs took 100 ms) and moves no time inside a turn. So the driven clock reads `max(tokio's time, busy)`, and a turn's first clock read moves `busy` to that reading plus the cost: the reads after it in the same turn see the turn done, and a `TurnBudget.max_elapsed` shorter than the cost trips. A turn that reads no clock is charged when it ends. The pump runs no turn while the clock is a whole millisecond or more ahead of tokio's time, and sleeps those whole milliseconds; what is below a millisecond carries to the next turn, so 100 turns of 10 µs take 1 ms. While the actors are busy the pump still fires the clock's wakes on tokio's time, not the clock's, as the clock thread fires a caller's deadline while a worker is busy.
  - A clock wake past what tokio's `Instant` holds, as `schedule_after(Duration::MAX)` asks for, is never slept for.
  - When nothing is ready, it parks its waker. `Queue::push` wakes it only inside the existing `woke` branch. That is the cold path that does a futex wake on a threaded runtime, so the hot path gains nothing.
  - It sleeps until the driven clock's next wake. A wake registered from a tokio task that is earlier than the pump's sleep wakes the pump, which fixes missed send, ask and reply deadlines.
- **Blocking APIs.** `wait_ready`, `LifecycleHandle::wait` and `wait_for_generation` get a `debug_assert!` that the runtime is not threadless, unless the timeout is zero, as the system's own `Runtime::shutdown(ZERO)` is.
- **`forsake`** also claims each cell's place in the queue for good, so a `tell` to a crashed actor queues nothing.

In `clock.rs`, `Clock` gains a private third source, `Driven(Box<dyn Fn() -> Duration + Send + Sync>)`:

- It has no thread, and the pump fires its wakes.
- Only `system.rs` builds it, so `clock.rs` needs no tokio dependency and no cfg.
- The system arm stays first in `Clock::now`.

In `mailbox.rs`, `Fence` moves onto the actor clock:

```rust
impl Fence {
    pub fn new(clock: &Clock, lapses_in: Duration) -> Self;
    /// Moves the deadline later or earlier; hold_for(Duration::ZERO) lapses it.
    pub fn hold_for(&self, d: Duration);
    pub fn is_open(&self) -> bool;
}
```

`is_open` reads the fence's clock, which costs what an `origin.elapsed()` on the system clock did. Cluster callers convert their tokio `until` with `until.saturating_duration_since(tokio::time::Instant::now())` when they spawn, and lapse a fence with `hold_for(ZERO)`.

In `trace.rs`:

```rust
/// Non-cryptographic, per thread. Placement, `Backoff` jitter and trace ids draw from it.
pub fn random_u64() -> u64;
/// Test only: makes unix_micros on this thread read `wall`, and the system clock again after `None`.
/// Production never calls it.
#[doc(hidden)]
pub fn simulate_wall(wall: Option<Rc<dyn Fn() -> u64>>);
/// Test only: makes this thread's random_u64 and axon-net's `random<N>` a seeded stream.
#[doc(hidden)]
pub fn simulate_seed(seed: Option<u64>);
```

The World sets the wall around every step of a node and clears it after, so the seed has a setter of its own that the per-step wall cannot reset.

`axon::trace` is not behind the `system` feature (lib.rs gates only `mod system`). axon-net depends on `axon` with `default-features = false` and can still call `random_u64`, `simulate_seed` and `unix_micros` without pulling in tokio through a feature.

`unix_micros` becomes the only wall clock:

- Seed discovery, the stream node, telemetry and the metrics service read it, not `SystemTime`.
- `axon_net::Identity::new` reads `axon::trace::unix_micros` instead of `SystemTime::UNIX_EPOCH.elapsed()`.

### Seam 2: the network (`crates/axon-net`)

```rust
pub trait Io: AsyncRead + AsyncWrite + Send + Unpin + 'static {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Io for T {}
pub type Incoming = tokio::sync::mpsc::Receiver<io::Result<(Box<dyn Io>, SocketAddr)>>;
/// Named because clippy's type_complexity refuses the type spelled out, in the trait and in every impl.
pub type Dial = Pin<Box<dyn Future<Output = io::Result<Box<dyn Io>>> + Send>>;

/// Where sessions get their bytes. `Config::network: None` is TCP.
pub trait Network: Send + Sync + 'static {
    /// A stream to `to`, or what a TCP dial gives: refused, reset, or no answer before the caller's deadline.
    fn connect(&self, to: SocketAddr, lane: Lane) -> Dial;
    /// Accepts on `at` until the receiver is dropped, which frees the address. An Err is an accept error (EMFILE).
    fn bind(&self, at: SocketAddr) -> io::Result<(SocketAddr, Incoming)>;
}
pub struct Config { /* unchanged */ pub network: Option<Arc<dyn Network>> }
```

- `accept_loop` in net.rs takes its `(stream, addr)` pairs from either `TcpListener` or `Incoming`. Everything after that is shared: the per-IP slots, both semaphores, the handshake deadline, the counters, TLS accept with peer certificates, and `establish_with`. There is no second accept loop.
- `set_nodelay`, `fd`/`TCP_INFO` and `tcp_halves` stay on the TCP arm. The other arm goes through `shared_halves` with `fd: None`, a path that already exists (`establish` uses it).
- Engine, `Redial`, `SeedServer` and `SeedDiscovery` get the network through the `Config` they already take. None of their signatures change.
- `random<N>` in `config.rs` is axon-net's only source of randomness. It supplies the handshake nonce and the low bits of the incarnation. It draws from the seeded generator only when `simulate_seed` has seeded the thread, and from ring otherwise. A unit test checks that it is ring when nothing seeded the thread.

### The World (`crates/axon-testkit/src/world.rs`)

`axon-testkit` depends on `axon`, `axon-net`, `tokio` (rt, time, sync, io-util, test-util) and `bytes`.

- It must not depend on `axon-cluster`, whose `src` simulations dev-depend on testkit.
- `Simulator<T>` stays as the message-level vehicle for the core simulations, and the World reuses its splitmix.
- The World's link type is called `Wire`, so it does not clash with `Simulator`'s `Link`.

Signatures are on the [API page](../crates/axon/README.md) and in
[cluster § Simulating a cluster](../crates/axon/guide/cluster.md#simulating-a-cluster). The design point of each part:

- `WorldConfig`: `quantum` is 1 ms, and whole milliseconds only, because a node's sleep rounds up to tokio's 1 ms
  tick; `window` (64 KiB) is what a sender may have unacknowledged per connection; `wall_start_ms` is the virtual unix
  time at T = 0; `limit` is the virtual time a `join` or `run_until` may take before it panics with the trace.
- `Host`: `boot()` counts from 1 and goes up at each restart; `ip()` is fixed per name for the run, so `ip_of` works
  for every name; `runtime()` is threadless; `transport(auth)` sets the network, node id = name and
  `allow_plaintext`, and runs `Identity::new` under the wall hook: the incarnation's high bits are the node's wall ms,
  so a node whose wall clock stepped back reboots as older, as in production.
- `Disk`: files in memory, each with the length synced so far. Creating, replacing and removing are durable at once; a
  truncation waits for the file's next sync, as appends do. `dir(name)` is a directory of its own names on the same
  machine, capacity and panics, as `OsFiles::new(dir.join(name))`. As `Files` it keeps `OsFiles`' order and cost: a
  write's appends are synced before a `Remove`, `Truncate` or `Replace` after them lands (but not those to the file it
  removes or replaces), and at its end, one file at a time, `DISK_SYNC` (2 ms) of virtual time each; a `Truncate`
  that cuts is synced at once and a `Replace` takes one sync before it lands; a write that syncs no file takes one,
  the directory's. A machine crash keeps a seeded first part of each file's unsynced bytes: none, all, or a length
  drawn from between, a third each, and the trace records it (`disk <path> kept <k> of <n> unsynced bytes`). It never
  writes bytes that were not appended.
- `crashes(&files, &ops)`, `crashed(&ops, n)`, `crash_points(&ops)`: every end a crash inside one write may leave, for
  a store's unit tests over `MemoryFiles`. For each op that is not an append, it and the ops before it land whole;
  of the appends that follow up to the next such op, each file is left as it was or keeps its first 0 to all bytes,
  whatever the other files keep. A `Replace` is whole or not. The World's `Disk` crashes into one of these ends
  (`crates/axon-testkit/tests/world.rs::a_machine_crash_inside_a_write_leaves_one_of_the_ends_crashes_goes_through`).
- Faults, each a call on the World between steps: `crash` as a `Process` (peers read a reset, dials are refused) or a
  `Machine` (silence, dials hang until the caller's deadline), `restart`, `pause` (SIGSTOP, its clock runs on),
  `suspend` (a suspended VM, its clocks stand still), `hold_actors` (actor turns stop, tokio tasks and I/O go on),
  `cost` (virtual time per actor turn), `starve` (stepped one quantum in `one_in`), `cut` (one way), `partition`,
  `heal`, `wire`, `reset`, `refuse_accepts` (accept fails with EMFILE), `drift` (a monotonic rate, whole-ms
  Bresenham) and `step_wall` (the wall jumps, the monotonic clock does not). § Fault model says what each looks like
  to the node.
- `pick` is the World's seeded choice, for fault timing; `log` lines go into the digest; `trace_tail` and `divergence`
  are what a failing seed prints.
- `now()` is the World's time; `read(name, f)` runs `f` as the node between steps, under its runtime and wall hook
  with a copy of its seed, so the test body can compare its own moments with the node's stamps exactly. It moves no
  clock, draw, turn cost or byte: `the_same_seed_gives_the_same_trace` reads every node between each two steps in one
  of its runs.

Frame compression is off in the World by default, because `Config::default()` has `Compression::None`. One World test turns it on. The two-process same-seed check covers that test.

**Wires.** There is one byte pipe per connection direction.

- A chunk is due at the end of the quantum it was written in, plus latency and seeded jitter. One chunk in every `stall_every` also waits `stall`. That is loss as a TCP session sees it: a stall, never a hole. A SYN or its answer that draws one waits `syn_stall` instead (1 s, the kernel's first resend of a SYN).
- A dial completes one round trip after it starts. The far node accepts when the SYN arrives, and its answer, a SYN-ACK or a refusal, goes back as a chunk of its own on the reverse wire, ahead of the far node's first bytes.
- A sender may have up to `window` bytes unacknowledged. Acknowledgements travel in the reverse direction.
- A directed link with a `bandwidth` is one first-in, first-out bottleneck that every connection on it shares: a chunk of data waits until the link is free and holds it for its length over the bandwidth, then travels its latency and jitter. Acknowledgements, SYNs and their answers, FIN and RST take no time on it, and it draws nothing from the seed.
- A one-way cut from a to b holds everything a sends to b: a's bytes, and a's acknowledgements of b's bytes, so b's own writes to a block once b's window fills. A connect across a cut, in either direction, hangs.
- Held bytes flow on `heal`, except those a crashed end sent: a cut drops packets and the sender's kernel sends them again, which a dead one never does. A process's RST is its kernel's and flows.
- A SYN lost to a `Machine`-crashed node or a full backlog is never sent again, even after the node restarts, so the dial waits until its deadline (5 s `handshake_timeout` for a cluster dial). A kernel would retry and reach the restarted node (Risks, item 7).
- A paused node's buffers fill until the window stops its senders, as a stopped process's kernel does. Dials to it complete into a bounded backlog and then wait for the axon handshake.
- After a restart at the same address, segments on connections to the old incarnation get RST.

**Crash**, in this order:

1. The node's wires go inert. `Machine` swallows writes and emits no EOF. `Process` delivers RST after one latency.
2. `threadless.forsake()`.
3. `N` and every node-held value are dropped inside `rt.enter()`, with the wall hook set to that node. Queued `ReplyTo`s answer into dead wires, and `Cluster::drop`'s `engine.close()` closes dead sessions. Nothing reaches a peer.
4. The runtime is dropped outside any `block_on`.
5. `Crashed` results go out in node-name order.

Tokio drops a node's tasks in an order set by its process-global task ids (`task/id.rs:78`, 4 shards). The wires are already dead, so that order cannot reach another node. Test oracles record at acknowledgement time, never in `on_stop`.

**Digest.** The digest is a hash of every delivered chunk as `(T, from, to, conn, bytes)`, plus every fault and every `log` line. It is kept per quantum. When two runs of one seed differ, the report names the first quantum that differs and prints both trace windows. Tokio task ids and `ActorIncarnation.runtime` (driver.rs:627, a process-global counter) are never logged.

**Guards.**

- A step that does not return within 10 s of wall time aborts with the seed and the trace tail. A node that never goes idle stops virtual time.
- On Linux, `World::run_until` and `join` assert that no thread was started.
- Seed entries are `IP:port`. Tokio parses those without `spawn_blocking` (`net/addr.rs:172-177`).

## How each nondeterminism source is controlled

| Source | Where | Control |
|---|---|---|
| Worker, dedicated and watchdog threads | `runtime.rs` | threadless mode starts none |
| Idle spin, steal, nudge | `runtime.rs`, the worker loop | unreachable without workers |
| `WORKER`/`NEXT` run-next affinity | `runtime.rs` | the pump never sets `WORKER`, so every push goes FIFO to home |
| Registry stop order | `runtime.rs`, the registry | `BTreeMap<u64,_>` (slots are sequential) |
| Busy/idle `Instant` stats | `runtime.rs` | stats only; not in the digest |
| `axon-clock` thread, `Clock::system` | `clock.rs` | driven clock that reads the node's tokio time and is fired by the pump |
| Two clocks (tokio and axon) | `system.rs` | one clock per node; the conversions are identities |
| `Fence` | `mailbox.rs`; its callers in `spawn.rs` and `cluster.rs` | `Fence` on the actor `Clock` |
| Handler cost is zero | the pump | `Threadless::turn_cost`, set by `World::cost` |
| Job threads, `spawn_blocking` | `jobs.rs`, `system.rs` | threadless mode runs blocking jobs as tasks on the node |
| Crash runs `Drop` (on_stop, Exit, ReplyTo) | `lifecycle.rs`, `driver.rs`, `engine.rs`, `Cluster`'s drop | dead wires first, then `forsake`, then drop under `rt.enter()` |
| Blocking waits on the sim thread | `lifecycle.rs`, `driver.rs` | `debug_assert!` when threadless; use the async forms |
| `ready()` 20 µs real spin | `driver.rs` | harmless: it falls through with the same result |
| Wake handshakes and atomics | `runtime.rs`, `mailbox.rs`, `ReplyState` in `engine.rs`, the session writer | program order on one thread. Races inside them are loom's job, and each has a model (see Fault model) |
| Trace and span ids | `trace.rs`; the stream node | seeded generator |
| Wall clock: dead letters, spans | `trace.rs`, `dead.rs`, `engine.rs` | wall hook |
| Paxos epochs and acceptor floor | `paxos/` | wall hook |
| Seed `unix_ms` and the `ahead` check | `seed/` | `unix_micros` |
| Identity incarnation: wall time | axon-net `Identity::new` | reads `unix_micros`, which the World's wall hook sets per node |
| Identity incarnation: low bits | axon-net `Identity::new` | `random<N>`, seeded when `simulate_seed` seeded the thread |
| Paxos monotonic `base`, link state | `paxos/` | `tokio::time::Instant` |
| Stream node `Instant::now` | axon-stream `node.rs` | `tokio::time::Instant::now().into_std()` at the core boundary |
| `Writer.written`: 5 µs gate on real time | axon-net `session.rs` | `tokio::time::Instant` (otherwise segment counts depend on CPU speed). The other session and net.rs deadlines, `heard`, `opened`, pings and sampled timings use tokio's `Instant` too |
| `select!` branch order (unbiased sites) | `session.rs`, `cluster.rs`, `engine.rs`, `spawn.rs`, `seed/`, the stream node | tokio RNG seeded per node (`rng_seed`, sim build) |
| `watch` shard choice (waiters wake in random order) | tokio `sync/watch.rs`; the waits in `engine.rs`, `session.rs`, `seed/`, `system.rs` | the same seeded tokio RNG |
| Tokio task drop order at crash | tokio's process-global task ids, owned-task shards | dead wires; oracles record at acknowledgement time |
| Tokio `Instant` outside a node context | tokio `time/clock.rs` | the driven clock reads through the node's `Handle`; drops under `rt.enter()`; holders built on the node |
| `unique` iteration (handoff, sweep, rebalance order) | `cluster.rs`, `spawn.rs` | `BTreeMap` |
| `leases` `extract_if` in `lose()` | `cluster.rs`, `spawn.rs` | `BTreeMap` |
| `peers`/`incoming` iteration | `engine.rs` | `incoming` is a `BTreeMap`; `peers` stays a `HashMap` (looked up per call) and `links()` sorts by peer |
| `Link.pending` cleared in hash order | `engine.rs` | drain, sort by call id, drop (the per-call insert and remove are unchanged) |
| Seed `senders`, `links` | `seed/` | `BTreeMap` |
| Placement and isolation choice | `spawn.rs` | `random_u64` |
| Backoff jitter: restarts, engine and seed dial | axon's `Backoff` | `random_u64`, seeded when `simulate_seed` seeded the thread |
| Handshake nonce | axon-net `handshake.rs` | `random<N>`: ring, or the seeded generator when `simulate_seed` seeded the thread |
| Process-global ids in wire text | `JoinError` and `ActorIncarnation` Display | a fixed refusal text: none reaches wire bytes |
| Frame compression bytes | axon-net frame compression | off by default in the World; one test turns it on, and the two-process check compares its bytes |
| DNS | the seed dial | `IP:port` entries |
| Traces merge order | axon-obs `traces.rs`, `telemetry.rs` | tie-break on trace id |
| `StreamKey` per-process hasher | `recovery.rs` | never iterated; `clippy::iter_over_hash_type` and the two-process check catch a slip |
| Telemetry's `spawn_blocking` and real threads | axon-obs `telemetry.rs`, `metrics.rs` | not started in the World (see Decisions) |
| Consul | `consul.rs` | outside the World; `check.sh live` |
| Node order, delivery order, jitter, fault timing | the World | splitmix from the seed |

Leaks are also guarded by lints that resolve paths, not by grep:

- `clippy.toml` `disallowed-methods` holds the real clocks (`Instant::now` and `elapsed`, `SystemTime::now` and
  `elapsed`), thread starts (`std::thread::spawn`, `Builder::spawn`, both `spawn_blocking`s), `RandomState::new`, and
  the iteration methods of `HashMap` and `HashSet`. Legitimate `src` sites carry
  `#[expect(clippy::disallowed_methods, reason = "..")]`: the threaded runtime, `Clock::system`, stats, and hash
  iterations whose order cannot show (a sort right after, a count, a `retain` whose keep depends only on the entry).
- `clippy.toml` is one root file, and a per-crate file would replace it rather than merge with it. So these entries apply to every test, example and bench. Test and bench crates that measure real time carry a crate-level `#![expect(clippy::disallowed_methods)]`, and the World tests do not. The `examples/*` apps have their own `examples/clippy.toml` with only the channel rules, which replaces the root file for them.
- `clippy::iter_over_hash_type` is denied in the `src` of `axon`, `axon-net`, `axon-cluster` and `axon-stream`. It also flags the `IdHash` maps (hub.rs `subs`) and iterations whose order does not matter, and those carry `#[expect]`.

## What a test looks like

### Keyed accounts on PaxosLeases through a crash, a pause and a one-way cut

`crates/axon-cluster/tests/world.rs`, on the accounts of `tests/accounts/mod.rs` (the ledger, the account actor and
its protocol, `boot` and the client helpers), which `world_leases.rs` shares. Three nodes on seed discovery and `PaxosLeases`, with 5 ms
of jitter:

1. b crashes, as a process or a machine as the seed picks, while deposits to its keys are on the wire; past the lease a
   and c take its keys. b returns while a client deposits and a's actors are held (`hold_actors`), so a key a took
   moves back only once its actor has ended on a, queue and all.
2. c is paused for 7 to 14 s while a long-lived client keeps depositing through routes that still point at it, so its
   buffers are full at the resume; its fence must be closed by then.
3. a stops hearing c for less than a lease, while c still hears a.

`check` then waits until the views agree and every key runs where each view places it, and asserts that every node
finds each key on one node with one epoch, and that no key was written by an older epoch after a newer one or started
on two nodes at one epoch; it prints the digest. The wait matters: a move that an isolated owner refused comes again
only `operation_timeout` later, and a move between the lookups shows a key on two nodes, one after the other. A lost
acknowledged deposit is not checked: the account writes the ledger before it replies and nothing removes a write, so
that could not fail.
`the_same_seed_gives_the_same_trace` runs the scenario twice per seed and compares the two Worlds.

### A stream origin crashes and returns as a new generation, then loses a link one way

The test is `crates/axon-stream/tests/world.rs`: an origin, a relay and a leaf on seed discovery. The origin crashes silently and returns as generation 2, then loses its link to the relay one way. The leaf's `Live` subscription must never end, each batch must continue from the last `Ready` or batch, a `Ready` may move only to a newer generation (one that jumped ahead within a generation would hide lost records), and every record of generation 2 arrives. The crash window opens only once the leaf has a position in generation 1, and the wait also ends on a terminal event, so that a reset is reported as the reset rather than as a timeout.

## Fault model

### In the simulation

| Fault (`spec/faults.md` name) | How | What the node sees |
|---|---|---|
| crash | `crash(n, Crash::Machine)` | peers see silence: writes fill the window and dials hang; keepalive and the idle timeout find it |
| crash, process only | `crash(n, Crash::Process)` | peers get RST after one latency, and dials are refused |
| restart | `restart(n)`: same address, fresh runtime, next `boot`, later incarnation | stale connections get RST; a wall step back before a restart gives an older incarnation |
| pause | `pause(n, d)`: not stepped, then one `advance(d)` | every overdue timer fires together; fences read a clock that ran through the pause |
| pause, a suspended VM | `suspend(n, d)`: not stepped, and its clock does not move | it wakes as if no time had passed: its fences are open and its leases look held, so it writes at an epoch others have passed |
| pause, handler never returns | `hold_actors(n, d)` | actor turns stop, while sessions and timers go on; calls wait in mailboxes (D9) |
| handler cost | `cost(n, d)` | each actor turn advances the node's clock by d; `TurnBudget.max_elapsed` and mailbox deadlines can trip; slow drains race handoffs |
| partition | `partition(a, b)`; one lane: `cut_lane` both ways | bytes held both ways, dials hang, held bytes flow on heal; a lane cut holds only that lane's connections and dials |
| one-way cut | `cut(from, to)`; one lane: `cut_lane(from, to, lane)` | from's bytes are held, and so are the acknowledgements of to's bytes, so to's writes stall once its window fills |
| loss | `Wire { stall_every, stall, syn_stall }`; `reset(a, b)`, one lane: `reset_lane(a, b, lane)` | a stall, a dial that waits a SYN resend, or a connection that ends with frames in flight |
| duplication | the application retries after a reset or a timeout | as in production |
| reordering | across connections: seeded jitter and the delivery order within a quantum | as in production; never inside one connection |
| latency | `Wire { latency, jitter, bandwidth }` per directed link | 1 ms resolution; a link with a bandwidth holds each chunk of data for its length |
| clock skew or step | `drift(n, ppm)`, `step_wall(n, ms)` | rate skew on every timer and fence; wall steps reach paxos epochs, the seed `ahead` check and incarnations |
| CPU starvation | `starve(n, k)` | stepped every k-th quantum after an `advance`, so timers are late in bursts |
| fd exhaustion | `refuse_accepts(n, true)` | `accept_loop`'s error path |
| disk full | `w.disk(n).set_capacity(Some(bytes))` | an append past it fails `StorageFull` and writes nothing |
| disk error, a write partly done | `crash(n, Crash::Machine)` | every file of the node's `Disk` keeps its synced length and a seeded first part of the rest, a torn tail, and a truncation not synced since is undone, while a removal or replacement stays; `Process` keeps what was written. Inside one write in a store's unit tests: `crashes` |
| memory limit | real bounded queues, a small `window`, slow nodes | refusals as in production |
| panic | in the test's actors and build closures | as today |

### Covered elsewhere

| Not in the simulation | Where it is covered |
|---|---|
| Real handler time (the World charges a declared or seeded `cost`, not measured CPU time) | `crates/axon/tests/embedded.rs` and the benchmarks |
| Tokio stalled while actor workers run on (`tests/cluster/leases.rs::a_holder_stalled_past_the_grace_handles_nothing_more`) | stays a real-runtime test, because here actors and tokio share one queue per node |
| Multi-worker scheduler, stealing, Dedicated threads, pool isolation, task order inside a node | `scripts/check.sh loom` (the mailbox, a drain or job result racing a turn's start, and in `runtime/model.rs` two workers sleeping and waking around a turn that does not end; with two workers only one sleeps at a time, so the one-watcher rule among several sleepers, a spin above zero and stealing past `STEAL_DEPTH` are not modelled), `hosted.rs`, `faults.rs`, `check.sh repeat` nightly on 2 cores |
| ReplyCore CAS race against actor threads (`ReplyState` in engine.rs) | `scripts/check.sh loom`: `crates/axon-cluster/src/engine/model.rs`, the endpoint's return against a reply sent or dropped on another thread, told once, a rejection over `NoReply` |
| `write_now` from actor threads against the writer task (`session.rs`) | `crates/axon-net/src/queued/model.rs` under `scripts/check.sh loom`: two senders and the writer task over `Queued`, the admission and count the session uses, for per-sender order, no frame torn by two writers, and no frame left unwritten |
| Kernel TCP: Nagle, buffers, `TCP_INFO`, half-open detection | `crates/axon-net/tests/{session,faults}.rs`, `scripts/check.sh smoke`, soak |
| TLS bytes, `CertificateIdentity` authorizers and certificate rotation | axon-net session tests; rotation in axon-cluster `tests/cluster/tls.rs` and `tests/seed.rs`. World runs are plaintext: rustls takes its randoms and key shares from ring's unseeded `SystemRandom`, so the bytes, and with them the digest, differ from run to run |
| Hostile bytes | fuzz `frame_decode`, `handshake`, `stream_wire`, `stream_core` |
| Message-level loss, duplication and reordering inside the paxos, seed and stream protocols | the core sims: `paxos/sim.rs`, `seed/sim.rs`, `axon-stream/tests/{sim,interests}.rs` |
| Faults in `MemoryDiscovery` and `MemoryLeases` (shared in-process, no TTL, heartbeat always Ok) | none: fault tests use `SeedServer` nodes and `PaxosLeases` |
| Consul discovery and leases | `scripts/check.sh live` |
| Paxos, exhaustively | TLC, `spec/tla` |
| Disk | the World's `Disk` holds what a node writes through it, as the stream store does in `axon-stream/tests/world_disk.rs` and an acceptor's lease log in every World test on `PaxosLeases`; the store's segment faults in `axon-stream/src/disk.rs`; `OsFiles` on a real file system in the guide program, `os_files_cut_a_failed_write_back_and_take_it_again`, and against `MemoryFiles` and `Disk` in `axon-testkit/tests/files.rs`; ClickHouse through the metrics provider tests and `live` |
| Performance | the benchmark harnesses; virtual time says nothing about speed |

## TLA

**Giving up a position.** [tla/RelayGiveUp.tla](tla/RelayGiveUp.tla) checks when a node gives up a stream position no candidate holds (`RelayCore::reconcile`, the wait from 8049630). A leaf with a Live follower pulls one key from the relays, then the origin, under link cuts, a heal of every link, a candidate that leaves the view and comes back between two turns of the node loop, a relay crash, relays orphaned while cut from the origin, new generations, and a view that follows the links within less than upstream_timeout, in discrete ticks. Its invariants: the leaf never resets while a node it has reached since its newest candidate appeared holds its position or could fetch it from the origin, unless that node refused that position while in view (`NoNeedlessReset`); a relay without a position neither skips a start the origin holds nor resets the branch that waits for it (`NoStartSkipped`); a parent on a newer generation is followed, not refused (`GenAheadNeverRefused`); a new generation is taken from its record 1 while a ring on the path holds it (`NewGenFromOne`). `NoSilentGap` and `GenNeverBack` only check that the model's own gap and generation guards are wired in: records carry no content and a branch sends in order, so the leaf's own gap check is never reached. `Recovers` says that once every link is up for good the follower gets every record again; it holds only because time stops while an answer is owed, where the code relies on its backoff doubling, and it cannot tell a delay from a livelock. The no-reset invariant needs the view to follow the links within upstream_timeout: `RelayGiveUp-SlowView.cfg` (ViewLag = UT) fails. Not modelled: OriginLost, the leaf's own orphaning and the `stranded` half of 8049630, hold_down and joining links, and a stale answer computed before a view change (a40f2b0). A behaviour has at most two cuts in all. `RelayGiveUp.cfg` (one relay, two generations, rings of two records, three records a generation, refusals of up to three ticks against an upstream_timeout of two) passes with 4.06 M distinct states in 5 min 13 s at `-workers 4`; `RelayGiveUp-TwoRelays.cfg` (two relays, one generation, rings of one record, two records, refusals of one tick) passes with 3.89 M in 4 min 37 s, and checks no generation rule. Both have two cuts and one crash, run one after the other at `-workers 4` on a shared box at load 30. Run them as the Paxos model is run, with `-config <cfg> RelayGiveUp`. Each `mutations/RelayGiveUp-*.cfg` fails in under 1 min 40 s. GiveUpAtOnce (before 8049630), OldestCandidate, StaleSince, ReadyStrict, BehindRefused, KeepRefusal, SpuriousGC and StaleRefusal (both before 5f19599), Coalesced (before 7f297b0: a candidate that leaves and returns between two turns of the node loop keeps its available_since and its refusal) and StaleReset (before 5e7ae55: a relay that fell behind resets a branch that is ahead of what it lost) break the no-reset invariant; NoWantFromAfter, RelayAfterLive and StaleWant (before 5e7ae55: a relay keeps the earliest start a child asked after that child moved on, and resets the branch of a child whose start the origin holds) the relay's start; GMRefuse and GCAsRefusal the newer generation; NewGenLive (before 7143010) and NotFresh (before 5f19599) record 1; NoGapChecks and RelayNoGap the no-gap one; OlderGen the generation one; NeverGiveUp `Recovers`. `RelayGiveUp-BestEffort.cfg` fails by design, switched on by `Gaps`: after a relay refuses record 1, the leaf takes the new generation live while the origin's ring behind it holds record 1. The guide promises record 1 only while the parent's ring holds it; `NewGenFromOne` checks the whole path. In the StaleReset and StaleRefusal mutations a relay falls behind the origin's ring only while cut from it (`KeepUp`), since the model's branches hold no more than the ring.

**The disk store.** [tla/DiskResume.tla](tla/DiskResume.tla) is one stream kept on disk at its origin, as `disk.rs`
writes and resumes it, and one follower fed only by what the hub delivers. The disk is the World's `Disk` (a machine
crash cuts each file to its synced length, a process crash keeps the page cache), and a machine crash may also keep part
of the unsynced tail and tear its last frame. A write runs its ops one at a time as `OsFiles` does: it may fail at any
op, at its `sync_data` or at the directory's sync, cut back to the last op that changed a file (whose appends before it
were synced first and stay) or left as a panicking store leaves it, and is taken again;
a process or machine crash may land between two ops or inside an append. Retention removes old segments at any write.
Invariants: the origin never serves a position the follower saw with another value (`NoRewrite`); a booted origin is at
or after the last record the follower saw (`ResumeAfterSeen`) and after the last record of every write that returned Ok
(`DurableKept`); the next position is one past the cursor and what waits for the disk (`NextAfterDurable`); the cursor,
and what the follower saw, are no later than where a boot would resume if a machine crash kept only what is synced now
(`CursorDurable`, `SeenDurable`); the ring holds every position from its oldest to the cursor (`RingContiguous`); and in
the end nothing waits for the disk (`WritesGoThrough`). Names hold the incarnation in place of `created_ms`: the store
names a new segment after every file a boot found, whatever the clock does. Run `java -cp tla2tools.jar tlc2.TLC
-workers 4 -config DiskResume.cfg DiskResume` in `spec/tla`. `DiskResume.cfg` (5 publishes, 2 frames a segment, 2
crashes, 2 failed writes) passes in 2 minutes on 4 workers (1.46 M distinct states), `DiskResume-Seg1.cfg` (a segment a
publish, 3 crashes) in 8 minutes (3.50 M), and `DiskResume-Live.cfg` (4 publishes, 1 failed write, with
`WritesGoThrough`) in 1 minute (61 k), on a shared box at load 30.

`DiskResume.cfg` is the store as it is, a dropped `StreamNode` whose write still lands included: `OsFiles` locks its
directory until that write has landed, so no node boots before it. `DiskResume-MetaReorder.cfg` beside it fails, and
shows what the store assumes of the file system: where a file's creation can be lost while a later removal is kept,
retention can leave no file of the stream; and `OsFiles` syncs the directory only for files this write creates, so a
segment a failed write created gets no directory sync from its retry.

Each `tla/mutations/DiskResume-*.cfg` must fail, in seconds (`TruncateMissing`, a liveness property, in about a minute).
Past bugs: a boot that serves the unsynced tail, a history cut whole starting over at 1, and a Truncate of a missing
file failing every retry (all e5bd1cb), a panicking store ending the writer (18d182f), `ProofRemoved`, a boot removing
the emptied first segment whose name was the only proof of its position, so a failed sync and two process crashes
started the generation over at 1 (294e305), `RetainUnsynced`, retention removing a segment a failed write still appended
to, so the retry's Remove of the segment before ran ahead of every append left and a crash there left no file (aadb655),
`TruncLost`, a truncated file synced only when the same write appended to it, so a machine crash brought back a torn
tail a boot cut and the boot after it removed a later segment whose records were delivered, and `ClockBack`, a new
segment named by a clock that repeated a millisecond across a restart, taking the name of a file the boot queued for
removal (both 39577f5), and `DropNode`, a node started on the directory while the write of one dropped in the process
still ran, so the two wrote files at the same first position and the older came back at the next boot (7769068).
Plausible ones: records delivered before they are synced, or after a failed directory sync, a
failed write dropped, a repeated frame not skipped, a frame past a gap taken, the first segment scanned from 0 (checked
with `NoRewrite` alone), a torn tail not truncated, retention taking the segment being written or removing before the
appends, the ring refilled with a hole, and only the last file of a write synced. Not checked: compacted streams, other
generations, ring eviction, and the directory sync in `OsFiles::list`.

**SnapshotThenLive.** [tla/SnapshotThenLive.tla](tla/SnapshotThenLive.tla) checks a compacted stream read
`SnapshotThenLive`, at an origin that keeps it on disk and through one relay: publishes, each write and its sync, the
origin's process and machine crashes and its resume (the relay's `Want` may reach the restarted origin first, which
resets the relay), pushes resent, rebranches, the origin refusing a branch its ring no longer covers and the relay's
reset after it, a relay ring that evicts, and a subscriber on each node, the relay's replaying one record a read and
resubscribing. Every subscriber gets each record once, in order, and one Snapshot (`Contiguous`), and a caught-up one
is at its node's position (`AtTip`); after every event its state is the origin's at its position (`StateMatches`);
what any subscriber sees is durable (`SeenIsDurable`), and what the relay and the subscribers hold stays the origin's
history through any crash (`SeenIsKept`). `SnapshotThenLive.cfg` (2 keys, 1 value and removal, 4 records, a ring of 2,
2 crashes, 1 resend, 1 relay subscriber) passes: 17.4 M distinct states, depth 31, 8 min 24 s on 4 workers of a
shared box at load about 100. `SnapshotThenLive-Live.cfg` (1 key, 3 records, 1 crash, 2 relay subscribers, no
symmetry) checks under fairness that a relay subscriber waiting for the state goes live or is reset (`StateArrives`),
in 5 min. Each `tla/mutations/SnapshotThenLive-*.cfg` fails within three minutes: a replay that starts at the snapshot's
position instead of the record after it, a snapshot whose state is read before or after its position or labelled at
its release, a restart that rebuilds the state behind its position, a relay that takes a batch it already holds, a
relay replay past what its ring evicted or past its cut, a replay only to the cursor at subscribe, live batches
delivered ahead of the replay, a relay that stops feeding its subscriber, a Snapshot taken again by a subscriber past
it, a relay reset that keeps its subscribers, the bug e5bd1cb fixed (a restarted origin serving bytes a process crash
left unsynced), and two that only `StateArrives` catches: a branch that queues the state ahead of its replay, and a
relay that stops at the first subscriber it resets. From `spec/tla`: `java -cp tla2tools.jar tlc2.TLC -workers 4
-config SnapshotThenLive.cfg SnapshotThenLive`. Not checked, as the module's header lists: batches of more than one
record, a second parent or a chain of relays, queue capacity, limits and access, and the state across generations.

**Deep runs (2026-10-01).** Each base config with its bounds raised, 3 h on 2 workers each, six at once on a 24-thread
EPYC 4484PX, on the models before c077ca6 and the hand-over and relay fixes after it. None found a violation.
Finished: `RelayGiveUp` (`MaxSeq=4`, `MaxCuts=3`, `MaxCrashes=2`) 36.6 M distinct states, depth 55, 1 h 18 min;
`SeedMembership-Seeds3x2` with three members, 4.9 M, depth 74, 59 min. Cut off at 3 h: `SnapshotThenLive`
(`MaxRecords=6`, `MaxCrashes=3`) 511 M to depth 22; `Handover` (`MaxE=4`, `MaxCrashes=2`, `MaxLeaves=2`) 168 M to depth
31; `DiskResume` (`MaxPub=7`, `MaxCrashes=3`) 15.6 M to depth 18; `QuotaGrant` (`MaxTakes=5`, `MaxReports=3`,
`MaxRestarts=1`) 14.8 M to depth 10.

## What holds it

- `rng_seed` fixes both tokio draws, the `select!` branch and the `watch` shard, but only where tokio reseeds: inside a
  runtime's `block_on`. `rt.enter()` does not reseed, so a `select!` or the first poll of `watch::changed` outside
  `block_on` draws from an unseeded generator. The World polls node futures only inside the node's `block_on`, and a
  drop under `rt.enter()` draws nothing; a `read` whose closure polls a future would draw unseeded. In a plain build
  tokio's draws are unseeded, so the same-seed tests are ignored there.
- The World reseeds `random_u64` from the node's own splitmix stream each time it steps, boots or drops the node, and
  unseeds the thread afterwards, so a stray draw moves only its own node's later draws in that step.
- `scripts/check.sh sim` runs every `world*.rs` in two processes and compares the `world digest` line each test prints
  per seed; `the_same_seed_gives_the_same_trace` (in axon-cluster's `world.rs` and in testkit's) runs one scenario
  twice in one process and names the first quantum that differs.
- The clippy rules above keep real time, threads, process entropy and hash order out of `src`; the `#[expect]` at each
  legitimate site says why.
- A fence planted to hold for twice the lease, and a lease planted to be released before its actor stopped, each
  failed `owners_crash_pause_and_lose_a_link_but_no_key_has_two_owners` at seed 0, and a seed that failed on one box
  failed byte for byte on another with the printed `SIM_SEED`.

## What the World found

Porting the real fault tests (the S8 step of the build) found three things:

1. **A catch-up burst reset a local subscriber that kept up.** A new parent's catch-up after a silent relay landed in
   the leaf's hub in one push, past the 256 events a default branch held, and ended every `Live` subscription with
   `ResetRequired(SlowConsumer)`. A default branch now holds 4096 events and a push carries at most `hub.max_batch_records` records of a stream;
   `axon-stream/tests/sim.rs::a_default_subscriber_takes_a_full_push` checks it.
2. **The ring is shorter than two silent relays in a row.** The leaf is refused with `HistoryUnavailable` and resets,
   which the spec allows; `world_relay.rs` raises `max_records_per_stream` so that it checks exactly-once, not the
   ring's size.
3. **A crashed owner's key moved only at the handshake timeout.** A dial to a silent machine hung for the 5 s
   handshake. The TCP connect now gets a keepalive's time within the handshake budget, and a SYN unanswered by then
   fails the dial with `Timeout(Connect)`. Under Paxos leases the survivor already held a session to the silent holder
   and learned only at its idle timeout: where the authority's `hold` is shorter than the idle timeout, the session's
   idle timeout becomes the hold and its keepalive a quarter of it (`Cluster::start`). The default 6 s lease holds
   5.74 s, past the 4 s idle timeout, so the defaults are unchanged.

## Risks

1. **Tokio upgrades** can reorder same-deadline timers or tasks. Seeds reproduce against one `Cargo.lock` and toolchain, not forever, and the failing-seed report records both. A failing seed becomes a named test with assertions, never a pinned digest.
2. **Hidden sources will reappear.** The clippy lints and the two-process check catch most of them. The per-quantum divergence report finds the rest quickly.
3. **One uncontrolled draw shifts the draws after it**, but only those of its node in the same step: the World reseeds `random_u64` from the node's own stream each time it enters the node. The divergence report finds the first quantum that differs.
4. **Tasks inside a node run FIFO.** The seed varies node order, delivery order, jitter, fault timing and handler cost. It does not vary which of two ready tasks on one node runs first. If a class of bugs needs that, a seeded pick in the pump can be added, and only then.
5. **A node that never goes idle stops virtual time.** The watchdog aborts with the seed. Sometimes that is a real livelock, and sometimes it is a workload the World cannot model, because busy work takes no time unless `cost` is set. The report names the node.
6. **The 1 ms quantum hides cross-node races shorter than a millisecond.** A node steps with a sleep, which tokio rounds up to its 1 ms tick, so a shorter quantum would run node clocks ahead of the World's, and `World::new` refuses one. Going below 1 ms needs another way to step, such as the quantum's `advance` with a check that tokio fires timers at that resolution.
7. **The byte pipe is not the kernel.** The rules above (window, acknowledgement stall, backlog, RST after reboot) are what axon can observe. If they prove too simple, turmoil-net (0.1) is the drop-in replacement to evaluate.
8. **Cost per seed** was estimated at 100 to 500 ms for example 1 in debug, from the prototypes, which took 85 to 125 ms per 8 s of virtual time for 3 nodes. Stepping 3 idle nodes, which send no byte, costs 12 ms per virtual second in debug and 0.81 ms in release ([benchmarks § Whole-cluster simulation](../crates/axon/guide/benchmarks.md#whole-cluster-simulation-axon-testkit)). That is a floor: traffic raises it, so each scenario's seed count comes from its own run time.

## Decisions

1. **Tokio is seeded in a separate sim build.** `scripts/check.sh sim` builds with `--cfg tokio_unstable` in `target/sim`, and production code keeps its `select!` sites and `watch` waits (§ What holds it).
2. **Telemetry stays out of the World** until a simulation needs it: it uses `spawn_blocking` and real time, and no World test starts it.
3. **No seed range runs on a schedule** (2026-09-25). The boxes are temporary, spun up when needed, and `scripts/seeds.sh` runs wider ranges by hand while they are up, at about 2 core-seconds a seed on two cores. Every box and CI runner is x86-64, so seeds are reproducible on x86-64 only; an arm64 check waits until something runs axon on arm64.
