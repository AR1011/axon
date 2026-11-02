# Actors

Crate: `axon-actors`, imported as `axon`. Every snippet on this page is a complete program and is compiled and run by `cargo test --doc -p axon-obs`.

An actor is a value that owns its state and handles one message at a time. Nothing else touches that state, so there
are no locks in your code. You talk to an actor through a handle; the handle puts messages in a bounded mailbox.

```toml
[dependencies]
axon-actors = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## Define, spawn, talk

```rust
use axon::{Actor, ActorSystem, Context, ReplyTo};

enum Command {
    Add(u64),
    Get(ReplyTo<u64>),
}

struct Counter(u64);

impl Actor for Counter {
    type Message = Command;
    type Error = &'static str;

    fn on_message(&mut self, msg: Command, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Command::Add(n) => self.0 += n,
            Command::Get(reply) => drop(reply.reply(self.0)),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let counter = system.spawn(Counter(0)).name("orders").start()?;

    counter.tell(Command::Add(10))?;          // never waits
    counter.send(Command::Add(32)).await?;    // waits for mailbox room
    let total = counter.ask(Command::Get).await?; // waits for the reply
    assert_eq!(total, 42);

    system.shutdown().await?;
    Ok(())
}
```

- One actor has one `Message` type, usually an enum. A request is a variant that carries a `ReplyTo<R>`.
- Handlers are plain synchronous functions. They must not block; see [Slow work](#slow-work).
- `ActorSystem::builder().build()` must be called inside a tokio runtime.
- A named actor can be found again with `system.lookup::<Command>("orders")`. Names are unique among running actors:
  a start whose name is taken fails with `NameTaken` before its actor runs anything.

## The three ways to send

| Call | Waits for | When the mailbox is full |
|---|---|---|
| `tell(msg)` | nothing | fails at once with `Reject { reason: Full, message }` |
| `send(msg).await` | room in the mailbox | waits in line, up to `request_timeout` (5 s) |
| `ask(build).await` | the reply | waits in line, then for the reply, same deadline |

- Messages from one sender arrive in the order they were sent. There is no order between different senders.
- A refused message is handed back in `Reject::message`, so nothing is lost and you can retry.
- `tell` is also refused while `send` callers are waiting, so it cannot jump the queue.
- `ask` fails with `RequestError`; `Deadline { admitted }` says whether the message got into the mailbox first.
- An `ask` that is the only one outstanding on its actor and on its thread, polled from a thread outside the workers on
  the system clock, runs the actor's turn there when the actor was idle, and otherwise waits there up to 2 µs for the
  answer before the thread sleeps. So a quick handler's answer needs no thread wake, and often no other thread. On a
  tokio runtime's thread its other tasks wait for that turn, as for each turn of a threadless runtime, and the turn
  counts in the actor's diagnostics, not in a worker's. Do not hold a lock the handler takes while you ask. An actor of
  a named pool or of workers pinned to cores, or one whose turns on asking threads take over 0.5 µs two times in a row,
  runs on the workers, and is tried on an asking thread again only after 250 more asks. An ask with others outstanding
  on its actor or its thread, and every ask on a simulated or manual clock, goes straight to sleep.
- On the system clock the deadline of a `send` or an `ask` ends it up to about 1 ms after it passes: their wakes are
  kept by the millisecond, as most are cancelled long before. A poll after the deadline sees it at once.
- An accepted message is either handled or reported as a [dead letter](observability.md#dead-letters).

## Lifecycle

```rust
use axon::{Actor, ActorSystem, Context, StopReason};

struct Sink(u64);

impl Actor for Sink {
    type Message = u64;
    type Error = std::convert::Infallible;

    fn on_start(&mut self, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        Ok(()) // open files, schedule timers
    }
    fn on_message(&mut self, v: u64, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        self.0 += v;
        Ok(())
    }
    fn on_stop(&mut self, _reason: &StopReason) {}
    fn label(v: &u64) -> &'static str {
        if *v == 0 { "zero" } else { "value" } // groups timings, traces and dead letters
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let sink = system.spawn(Sink(0)).start()?;
    for i in 0..100u64 {
        sink.tell(i)?;
    }
    sink.lifecycle.drain(None);              // finish what was accepted, then stop
    let exit = sink.join().await;
    assert_eq!(exit.reason, StopReason::Drained);
    assert_eq!(exit.discarded_messages(), 0);
    system.shutdown().await?;
    Ok(())
}
```

| How it ends | `StopReason` | Queued messages |
|---|---|---|
| `lifecycle.stop()` or `cx.stop()` | `Stopped` | dropped, reported as dead letters |
| `lifecycle.drain(deadline)` or `cx.drain(..)` | `Drained` / `DrainDeadline` | handled first; still stashed, dropped |
| no message for its idle time (`spawn(..).idle(d)`) | `Drained` / `DrainDeadline` | handled first; still stashed, dropped |
| handler returns `Err(e)` | `Failed("{e:?}")` | dropped |
| handler, job, source or sink panics | `Panicked` | dropped |
| `on_start` returns `Err` | `StartFailed(..)` | dropped |
| a job its executor drops, not cancelled | `Failed("a job was dropped by its executor")` | dropped |
| an actor linked to it by `cx.link` fails | `Linked(incarnation)`, the one that failed | dropped |
| its `Fence` lapses (`spawn(..).fence(Fence::new(system.clock(), d))`, moved by `hold_for`; keyed actors under a lease get one), or it is stopped once past it | `Fenced` | dropped |

Timers that had not fired are dropped by every ending, a drain included, and so are attached sources. A drain waits
for jobs, and closes attached sinks once the actor has written what it holds; a stop cancels both. The `Exit` counts
them: `pending_timers` and `pending_jobs`. While draining, new timers and jobs are refused with `Draining`, except in
`on_start`: a drain asked for before the actor started begins once `on_start` has set it up.

An idle time passivates an actor that nobody talks to: `spawn(actor).idle(Duration::from_secs(60))`, or
`MailboxConfig::idle`, drains it once `d` has passed since `on_start` or the last message it handled. Its own
timers and job results do not keep it alive, so an actor that ticks every second still ends. It is a drain with as
long again as its deadline: it waits that long for the actor's jobs, then ends it `DrainDeadline`.

A panic never takes down a worker thread or another actor, and one in a job, source or sink ends only the actor that
started it.

Once `join` returns, the actor is fully gone: its dead letters are recorded, its counters are in `system.stats()` and
its name is free for a new actor.

`system.shutdown().await` drains every actor and returns within `shutdown_timeout` (5 s) with a report: every `Exit`,
and anything still running. Actors drain until a quarter of the timeout is left, and one not ended when an eighth is
left is reported still running. The rest goes to the extensions, such as the cluster, which hands keys off and leaves
discovery once the actors are gone: each gets an equal share of what is left, and passes on what it does not use.
`shutdown_with_reason(LeaveReason::RollingRestart)` also tells the cluster why the node left.
Dropping the last `ActorSystem` clone stops everything without draining, so call `shutdown`. A drop is a crash, but
not an instant one: a job mid-poll on another thread finishes that poll after `drop` returns. A test that crashes a
node this way must wait for what it checks to settle.

## Supervision

Give the system a factory instead of a value and it restarts the actor when it fails.

```rust
use std::time::Duration;
use axon::{Actor, ActorSystem, Backoff, Context, RestartPolicy};

struct Flaky;

impl Actor for Flaky {
    type Message = bool;
    type Error = &'static str;
    fn on_message(&mut self, fail: bool, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        if fail { Err("flaky") } else { Ok(()) }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let backoff = Backoff { initial: Duration::from_millis(5), ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..Default::default() };
    let flaky = system.supervise(|| Flaky).name("flaky").policy(policy).start_supervised()?;

    flaky.send(true).await?;            // waits for the first start; fails, restarts
    while flaky.family.restarts() < 1 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    flaky.send(false).await?;           // the same handle reaches the new actor
    assert_eq!(flaky.family.restarts(), 1);
    let listed = &system.diagnostics_page(None, 10)?.actors[0];
    assert!(listed.restarted_us > Some(listed.started_us));

    system.shutdown().await?;
    Ok(())
}
```

- Default policy: 3 restarts within 60 s, backoff from 10 ms doubling to 1 s with 20 % jitter. Past that the family
  ends. `Backoff` is the cluster's dial backoff too (`ClusterConfig::backoff`); its `jitter` is the fraction of each
  delay cut away at random, so no wait is longer than `max`.
- Only failures restart: `Failed`, `StartFailed`, `Panicked` and `Linked` (`StopReason::is_failure`). A clean stop
  or drain is final. Change that with `RestartPolicy::should_restart`.
- A restart is a fresh actor from the factory with a new, empty mailbox. Nothing is replayed, and the failed actor's
  timers, jobs and sinks end with it: none of them reaches the new one.
- From the failure until the restart, `tell` is refused with `NotReady`, while `send` and `ask` wait for the new actor
  up to the request timeout. A stop or drain in that time ends the family at once, and its exit reads `Stopped`, not
  the failure it was about to restart from. What the failed actor still held is a dead letter of that failure
  (`actor_failed`), never counted again in the family's exit, however early the stop lands. The family's
  `Diagnostics::dead_letters` counts them, and every generation's after it.
- `system.diagnostics_page` lists each actor with `started_us`, when it or its family first started, and
  `restarted_us`, when the family last restarted (`None` before a restart, and still the last one while the next
  waits), both from `trace::unix_micros()`. The console shows them as Up and Since restart. The clock is read once per
  spawn and restart, never per message. A family's counters and timings there add up all its generations, so they
  never go back at a restart; its mailbox depth and other gauges are the live generation's.
- A `send` or `ask` waiting for room when its actor fails goes to the next one, so `ask`'s builder may run more than
  once. A message the failed actor already took is not sent again: it is a dead letter, and an `ask` gets
  `ReplyDropped`.

## Watching an actor end

`cx.watch(&lifecycle, map)` tells an actor when another one ends: `map(exit)` arrives once, as a message, with the
`Exit`. It takes the `lifecycle` of an `ActorHandle`, a `SupervisedHandle` or a `SpawnHandle`; a watch on a
supervised family is told when the whole family ends, not on a restart. Watching an actor that has ended already
tells at once. Any number of actors can watch one, and one can watch any number.

```rust
use axon::{Actor, ActorSystem, Context, Exit, LifecycleHandle, ReplyTo, StopReason, WatchHandle};

struct Worker;

impl Actor for Worker {
    type Message = ();
    type Error = &'static str;
    fn on_message(&mut self, _: (), _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        Err("gave up")
    }
}

enum Msg {
    Watch(LifecycleHandle),
    Down(Exit),
    Ended(ReplyTo<Vec<StopReason>>),
}

#[derive(Default)]
struct Pool {
    watches: Vec<WatchHandle>, // each one's cancel takes the watch back
    ended: Vec<StopReason>,
}

impl Actor for Pool {
    type Message = Msg;
    type Error = String;

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Msg::Watch(worker) => self.watches.push(cx.watch(&worker, Msg::Down).map_err(|e| format!("{e:?}"))?),
            Msg::Down(exit) => self.ended.push(exit.reason),
            Msg::Ended(reply) => drop(reply.reply(self.ended.clone())),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let pool = system.spawn(Pool::default()).start()?;
    let worker = system.spawn(Worker).start()?;
    pool.send(Msg::Watch(worker.lifecycle.clone())).await?;
    assert!(pool.ask(Msg::Ended).await?.is_empty()); // the watch is in place
    worker.tell(())?;                                // the worker fails
    worker.join().await;                             // the pool was told before this returns
    assert!(matches!(pool.ask(Msg::Ended).await?[..], [StopReason::Failed(_)]));
    system.shutdown().await?;
    Ok(())
}
```

The message comes in like a job result: a full mailbox does not refuse it, it is handled during a drain, and it
starts no trace. When it is handled, the system has freed the ended actor's name, so a pool can start another under
it; the watched handle's `exit()` may still be `None`, so take the `Exit` from the message. Watches share
`MailboxLimits::sources` (1024) with `attach` and `attach_sink`, each until its message is handled or it is
cancelled; past that, `cx.watch` is refused with `TaskAdmissionError::Full`, and while the actor drains with
`Draining`. `WatchHandle::cancel` from the watcher takes a watch back, and nothing arrives after it, even if the
actor ended meanwhile; from another thread it can race a message being handled. Dropping the handle does nothing. A
drain does not wait for a watch, and nothing arrives once the watcher is stopping: its watches end with it.

Watches are for actors on this node. A keyed actor anywhere in the cluster is watched with `cluster.watch_key`
([cluster](cluster.md#spawn-actors-on-other-nodes-one-per-key)); another node's named actor has no exit to watch,
so call it and act on `err.retry()`.

## Linking actors

`cx.link(&lifecycle)` ties this actor and another local one both ways: when either ends by a failure (`Failed`,
`StartFailed`, `Panicked`, or `Linked` itself: `StopReason::is_failure`), the other is stopped with
`StopReason::Linked(incarnation)`, naming the one that failed. A failure so runs along a chain or round a cycle of
links and stops each member once. A stop, a drain or a shutdown ends the link and stops nobody else.

```rust
use axon::{Actor, ActorSystem, Context, LifecycleHandle, StopReason};

enum Msg {
    Link(LifecycleHandle),
    Fail,
}

struct Half;

impl Actor for Half {
    type Message = Msg;
    type Error = &'static str;

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Msg::Link(other) => {
                cx.link(&other); // its LinkHandle's cancel, or cx.unlink(&other), takes it back
            }
            Msg::Fail => return Err("gave up"),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let reader = system.spawn(Half).start()?;
    let writer = system.spawn(Half).start()?;
    reader.send(Msg::Link(writer.lifecycle.clone())).await?;
    writer.tell(Msg::Fail)?; // one half fails
    assert!(matches!(writer.join().await.reason, StopReason::Failed(_)));
    let exit = reader.join().await; // and takes the other with it
    assert_eq!(exit.reason, StopReason::Linked(writer.lifecycle.incarnation()));
    system.shutdown().await?;
    Ok(())
}
```

Linking a pair again, from either end, keeps the one link. `cx.unlink(&lifecycle)` or the returned
`LinkHandle::cancel` takes it back; dropping the handle does nothing. Linking to an actor that has failed already
stops this one at once. A link holds no message, so it takes nothing from `MailboxLimits`. It belongs to this
incarnation: a supervised actor stopped by a link is restarted by its policy like any other failure, and the new
incarnation starts with no links. Linked to a `SupervisedHandle`'s lifecycle, the link is the family's: a failure on
the other side fails its current generation, for its policy to restart, and only the family's end, not a restart,
reaches the other side.

Which to use: a supervisor when an actor should come back after a failure, with fresh state; a watch when another
actor should hear of an end and decide what to do, such as start a replacement or fail the requests it was serving;
a link when two actors are no use without each other, such as the reader and the writer of one connection, so that one
failing ends both instead of leaving half of it running. To have such a group come back, link its parts to the actor
that starts them in `on_start`, supervise that one, and have it stop them in `on_stop`, since its own clean stop
passes nothing on.

## Routing over a pool

A `Router` spreads messages over local actors of one message type. It is a plain value, not an actor: clones share
its members, and `tell`, `send` and `ask` go straight to the chosen member's mailbox, with no hop through another
actor. `Routing::RoundRobin` takes each member in turn; `Routing::LeastLoaded` takes the one with the fewest queued
messages. `router.member(&key)` gives the member for a key on any router: rendezvous hashing, as keyed actors use for
nodes, so a member leaving moves only its own keys, and one joining takes only the keys it wins.

```rust
use axon::{Actor, ActorSystem, Context, ReplyTo, Router, Routing};

enum Job {
    Resize(u64),
    Done(ReplyTo<u64>),
}

struct Worker(u64);

impl Actor for Worker {
    type Message = Job;
    type Error = &'static str;

    fn on_message(&mut self, msg: Job, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Job::Resize(_) => self.0 += 1,
            Job::Done(reply) => drop(reply.reply(self.0)),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let router = Router::new(Routing::RoundRobin);
    let mut workers = Vec::new();
    for _ in 0..3 {
        let worker = system.spawn(Worker(0)).start()?;
        router.add(worker.clone());
        workers.push(worker);
    }
    for image in 0..6u64 {
        router.tell(Job::Resize(image))?; // one each, in turn
    }
    for worker in &workers {
        assert_eq!(worker.ask(Job::Done).await?, 2);
    }

    let owner = router.member(&"user-17").expect("a member"); // the same one for this key while it runs
    owner.send(Job::Resize(7)).await?;

    workers[0].lifecycle.stop();
    workers[0].join().await; // dropped from the router before this returns
    assert_eq!(router.members().len(), 2);
    router.send(Job::Resize(8)).await?; // never to the one that ended
    system.shutdown().await?;
    Ok(())
}
```

A routed call fails as the member's own would: `tell` finds the chosen member full and fails with `Full`, handing the
message back, without trying another; `send` and `ask` wait for that member's room, up to the request timeout. With
no member, `tell` and `send` fail with `NotReady` and `ask` with `NotAdmitted(NotReady)`, at once. `LeastLoaded`
reads each member's queue length when the call is made, so it costs a lock per member per message, and callers on
several threads at once can pick the same member; ties go in turn. It counts queued messages, not work: a member
busy on one long message looks idle.

A member that ends, however it ends, is dropped before its `join` returns, so nothing is routed to it after that.
One that is stopping or draining is passed over until then, and `Closed` comes back only when every member is;
`router.member(&key)` still gives it, so no key moves before the member has ended. `router.add` takes more members;
adding one already there changes nothing, and one added after it ended is dropped at once. Members are
`ActorHandle`s and the router starts nothing: to keep a pool at size, `cx.watch` the members from the actor that
starts them and `add` a replacement when one ends.

## Topics

A `Topic` sends each message it is given to every actor subscribed to it. `system.topic::<M>(name)` finds the topic
of that name on this node, or makes it, so a publisher and its subscribers need share only the name. Like a router it
is a plain value, not an actor: `publish` tells each subscriber straight into its mailbox, so it never waits, and it
can be called from a handler, even one whose own actor is subscribed.

```rust
use axon::{Actor, ActorSystem, Context, ReplyTo};

enum Feed {
    Price(u64),
    Last(ReplyTo<u64>),
}

// What is published is cloned for each subscriber, then made into its message: `Feed` itself needs no `Clone`.
#[derive(Clone)]
struct Price(u64);

impl From<Price> for Feed {
    fn from(price: Price) -> Self {
        Feed::Price(price.0)
    }
}

struct Ticker(u64);

impl Actor for Ticker {
    type Message = Feed;
    type Error = &'static str;

    fn on_message(&mut self, msg: Feed, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Feed::Price(price) => self.0 = price,
            Feed::Last(reply) => drop(reply.reply(self.0)),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let prices = system.topic::<Feed>("prices")?;
    let plain = system.spawn(Ticker(0)).start()?;
    let family = system.supervise(|| Ticker(0)).start_supervised()?;
    family.ask(Feed::Last).await?; // its first generation is up
    prices.subscribe(plain.clone())?;
    prices.subscribe(plain.clone())?; // still one subscription
    prices.subscribe(family.clone())?; // the family: each generation it restarts is subscribed too

    assert_eq!(prices.publish(Price(101)), 2); // how many took it
    assert_eq!(plain.ask(Feed::Last).await?, 101);
    assert_eq!(family.ask(Feed::Last).await?, 101);

    system.topic::<Feed>("prices")?.publish(Price(102)); // the same topic, by name
    assert!(system.topic::<u64>("prices").is_err()); // a name holds one message type

    plain.lifecycle.stop();
    plain.join().await; // dropped from the topic before this returns
    prices.unsubscribe(&family.lifecycle);
    assert_eq!(prices.publish(Price(103)), 0);
    system.shutdown().await?;
    Ok(())
}
```

A subscriber whose mailbox is full, or that is stopping, or a family between two generations, misses that message:
`publish` goes on to the next, and the miss is a dead letter with the label `publish`, the topic's name as its target,
and the refusal as its reason (`full`, `too_large`, `not_ready`, `closed`), counted in `runtime.dead_letters`. It never waits for room: an actor that
must see every message should be sent to on its own, with `send`. Each publisher's messages reach each subscriber in
the order it published them; two publishers' messages interleave. `publish` clones the message for each subscriber,
so a large payload goes in an `Arc`.

A subscriber that ends, however it ends, is dropped before its `join` returns, without anyone unsubscribing it; one
subscribed after it ended is dropped at once. A supervised family stays subscribed across its restarts and is
dropped when it ends for good. `unsubscribe` takes the subscriber's `lifecycle`, as `cx.unlink` does. A topic holds at
most `Topic::<M>::MAX_SUBSCRIBERS` (4096) subscribers, as `publish` tells each in turn, and `subscribe` past that fails
with `SubscriberLimit`. A topic lasts while any handle to it or any subscriber does, then its name is free again, for
any type; asking for a name with another message type than its topic's fails with `NameTaken`. Topic names are apart
from actor names.

Topics are local to their node: a subscriber is an actor on this system. To fan out across nodes, publish to an
`axon-stream` stream ([streams](streams.md)) and have an actor on each node follow it and publish to that node's topic.

## Timers

```rust
use std::time::Duration;
use axon::{Actor, ActorSystem, Context, ReplyTo};

enum Msg {
    Tick,
    Count(ReplyTo<u32>),
}

struct Ticker(u32);

impl Actor for Ticker {
    type Message = Msg;
    type Error = &'static str;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        cx.schedule_every(Duration::from_millis(10), || Msg::Tick).map_err(|_| "no timer")?;
        Ok(())
    }
    fn on_message(&mut self, msg: Msg, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Msg::Tick => self.0 += 1,
            Msg::Count(reply) => drop(reply.reply(self.0)),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let ticker = system.spawn(Ticker(0)).start()?;
    // Ask until it has ticked twice: on a loaded machine a timer can come late.
    while ticker.ask(Msg::Count).await? < 2 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    system.shutdown().await?;
    Ok(())
}
```

`schedule_after(delay, msg)`, `schedule_at(time, msg)` and `schedule_every(period, make)` return a `TimerId` for
`cx.cancel_timer(id)`. `time` is on the clock `cx.now()` reads, so `cx.now() + delay` is `schedule_after(delay, ..)`.
A timer is a message to yourself, so it is handled in turn like any other. Timers end with the actor.

That clock is monotonic. For a time of day, as a cron line gives, compute the next wall time, schedule the
difference, and check the wall clock again when the timer fires, since it can be set back or forward meanwhile. Read
it with `axon::trace::unix_micros()`, which a [simulated cluster](cluster.md#simulating-a-cluster) steps per node.

```rust
use std::time::Duration;
use axon::trace::unix_micros;
use axon::{Actor, ActorSystem, Context, ReplyTo, TimerReject};

enum Msg {
    Due(u64),
    Runs(ReplyTo<u32>),
}

/// Runs at every whole `period` of wall time, in microseconds since the epoch.
struct Every {
    period: u64,
    runs: u32,
}

impl Every {
    fn arm(&self, cx: &mut Context<'_, Self>, due: u64) -> Result<(), String> {
        // At most a minute at a time, so a clock set forward is seen within a minute.
        let wait = Duration::from_micros(due.saturating_sub(unix_micros())).min(Duration::from_secs(60));
        match cx.schedule_after(wait, Msg::Due(due)) {
            Err(e) if e.reason == TimerReject::Full => Err("no timer".into()),
            _ => Ok(()), // Draining or Closed: the actor is ending
        }
    }

    fn next(&self) -> u64 {
        (unix_micros() / self.period + 1) * self.period // missed slots are skipped
    }
}

impl Actor for Every {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        self.arm(cx, self.next())
    }
    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Msg::Due(due) if unix_micros() < due => self.arm(cx, due)?, // early: wait the rest
            Msg::Due(_) => {
                self.runs += 1;
                self.arm(cx, self.next())?;
            }
            Msg::Runs(reply) => drop(reply.reply(self.runs)),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let every = system.spawn(Every { period: 10_000, runs: 0 }).start()?;
    while every.ask(Msg::Runs).await? < 2 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    system.shutdown().await?;
    Ok(())
}
```

## Slow work

A handler runs on a shared worker thread. Blocking in it holds that thread: actors queued on it are taken by an idle
worker within about a millisecond, but the one it last sent to waits for it to return, and a pool with every worker
blocked stops. Hand slow work to a job; its result comes back as a message.

```rust
use axon::{Actor, ActorSystem, Context, ReplyTo};

enum Msg {
    Fetch(u64),
    Fetched(u64),
    Total(ReplyTo<u64>),
}

struct Worker(u64);

impl Actor for Worker {
    type Message = Msg;
    type Error = String;

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Msg::Fetch(id) => {
                // blocking work: submit_io. async work: submit_future.
                cx.submit_io(move || Msg::Fetched(id * 2)).map_err(|e| format!("{e:?}"))?;
            }
            Msg::Fetched(v) => self.0 += v,
            Msg::Total(reply) => drop(reply.reply(self.0)),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let worker = system.spawn(Worker(0)).start()?;
    worker.send(Msg::Fetch(21)).await?;
    // The job's result comes back as a message, after this send returns.
    while worker.ask(Msg::Total).await? == 0 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(worker.ask(Msg::Total).await?, 42);
    system.shutdown().await?;
    Ok(())
}
```

A job's result is handled in the trace of the message that submitted it. The `JobHandle` cancels it: a future is dropped
at its next poll, a blocking closure runs on but its result is discarded. An actor may have 16 jobs in flight by default
(`MailboxLimits::jobs`); a job holds its slot from `submit_*` until its result is taken to be handled, including the
time it spends waiting inside, on a shared semaphore for example. But the jobs of every actor share tokio's pools:
blocking closures run on its blocking threads (512 by default, `max_blocking_threads` on the tokio runtime), so a
resource each job opens, such as a database connection, can run out long before any actor reaches its limit. Share a
bounded pool of such resources between the jobs, or bound the jobs themselves with `RuntimeConfig::job_executor`: a
`JobExecutor::custom` that hands `Job::Async` to tokio and `Job::Blocking` to a bounded pool, and refuses a job when the
pool's queue is full. A refused job has not run: `SubmitError` holds the reason and the work. Keep the work to submit
again when a job ends, or a `ReplyTo` it holds answers its caller `ReplyDropped` when it is dropped. `?` turns the error
into its `TaskAdmissionError`. (`JobExecutor::thread_pool(threads, queued)` is such a pool, but it runs a future too by
blocking one of its threads on it, outside tokio.) An actor that must block can instead run on its own thread:
`.execution(Execution::Dedicated)`.

Jobs run on the system's executor unless one is named: `cx.submit_future_on(&executor, fut)` and
`cx.submit_io_on(&executor, f)` run that job elsewhere, under the actor's same job limit, cancel, pause and drain.
Work that would hold a thread for long, such as an interpreter running a guest's script, goes on a runtime of the
application's own, `JobExecutor::tokio(runtime.handle().clone())`, so the threads that run the cluster's sessions
and timers stay free; in a test, `system.job_executor()` is the system's own. Keep that runtime until
`system.shutdown()` has returned: a job its executor drops, other than a cancelled one, ends its actor
`Failed("a job was dropped by its executor")`. A job that panics ends its actor `Panicked`, as a handler that panics
does. For work that may panic, make the panic the job's result: `axon::catch_panic(work).await` is `Err(Panic)` with
the panic's message, and what the job holds outside `work`, a reply for example, is still there to answer with. Use it
on work whose state the job owns: what the work shared with others may be left half changed. A blocking job has
`std::panic::catch_unwind`, and `Panic::from(payload)` gives its message the same way.

```rust
use axon::{Actor, ActorSystem, Context, JobExecutor, ReplyTo};

enum Msg {
    Run(u64, ReplyTo<Result<u64, String>>),
    Ran(ReplyTo<Result<u64, String>>, Result<u64, String>),
}

/// Runs guests' scripts, which may take long and may panic, on threads of their own.
struct Engine(JobExecutor);

impl Actor for Engine {
    type Message = Msg;
    type Error = String;

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Msg::Run(input, reply) => {
                let script = async move {
                    assert!(input != 0, "division by zero"); // a guest's bug
                    100 / input
                };
                let job = async move {
                    let ran = axon::catch_panic(script).await.map_err(|panic| panic.to_string());
                    Msg::Ran(reply, ran)
                };
                cx.submit_future_on(&self.0, job).map_err(|e| format!("{e:?}"))?;
            }
            Msg::Ran(reply, ran) => drop(reply.reply(ran)),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let guests = tokio::runtime::Builder::new_multi_thread().worker_threads(2).thread_name("guest").build()?;
    let system = ActorSystem::builder().build()?;
    let engine = system.spawn(Engine(JobExecutor::tokio(guests.handle().clone()))).start()?;
    assert_eq!(engine.ask(|r| Msg::Run(0, r)).await?, Err("division by zero".into()));
    assert_eq!(engine.ask(|r| Msg::Run(4, r)).await?, Ok(25));
    system.shutdown().await?;
    guests.shutdown_background(); // after the system: its actors' jobs are done
    Ok(())
}
```

### Pausing until a job ends

An actor that must not do anything else before a job is done, such as one that loads its state when it starts or
applies one change at a time, pauses on the job: `cx.pause_until(&job)`. Until that job's result is handled, the
actor handles only it, its timers, a stop and a drain's deadline. Messages, other jobs' results, sinks' events and
watch downs wait, and come after it in the order they arrived: held results, then downs, then the stash's replay, then
the mailbox. A handler among them that pauses again holds the rest the same way. The idle time does not run, and the
mailbox's bound holds senders back: `tell` is refused `Full`, and `send` waits for room. A held result keeps its job's
slot of `MailboxLimits::jobs` until it is handled, so what a pause holds stays bounded and a timer's submit may be
refused `Full` meanwhile. Pausing on several jobs waits for all of them, handling each result as it comes. To bound
the wait, time out inside the job, or cancel it from a timer; a cancelled job lifts the pause with no result. A
source lives as long as the actor, so pausing on one is refused with `PauseError::Source`. A pause taken in
`on_start`, or by the handler of the result `on_start` paused for, is part of the start: until it lifts the actor is
`Starting`, `ready()` waits for it, and a failure in it fails `ready()` with its reason (`on_stop` runs, as
`on_start` returned `Ok`).

```rust
use std::time::Duration;
use axon::{Actor, ActorSystem, Context, ReplyTo};

enum Msg {
    Loaded(u64),
    Get(ReplyTo<Option<u64>>),
}

struct Account(Option<u64>);

impl Actor for Account {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        let load = cx
            .submit_future(async {
                tokio::time::sleep(Duration::from_millis(20)).await; // a read from storage
                Msg::Loaded(100)
            })
            .map_err(|e| format!("{e:?}"))?;
        cx.pause_until(&load).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Msg::Loaded(balance) => self.0 = Some(balance),
            Msg::Get(reply) => drop(reply.reply(self.0)),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let account = system.spawn(Account(None)).start()?;
    // Asked before the load is done, answered after it.
    assert_eq!(account.ask(Msg::Get).await?, Some(100));
    system.shutdown().await?;
    Ok(())
}
```

The same pieces make a write-ahead actor, one that answers only once a change is durable. Each message changes the
state in memory and keeps its reply. `on_turn_end` writes the whole turn's changes in one `submit_io` job, and the
job's result sends the replies it covers. While a write is out, the next turns' changes wait for it and go in the
next write; once too many are unwritten, `cx.pause_until(&write)` leaves the rest in the mailbox, whose bound then
holds the senders back.

### A store's files

Every `Files` call is a future that starts nothing until it is polled, so one job reads a store and writes it: call
them in the handler, await them in a `cx.submit_future` job, and `pause_until` it if the next messages must see what it
loaded. `OsFiles` works on tokio's blocking pool, or in place on a `JobExecutor::thread_pool` thread.

```rust
use std::sync::Arc;
use axon::files::{FileOp, Files, MemoryFiles, OsFiles};
use axon::{Actor, ActorSystem, Context, ReplyTo};
use bytes::Bytes;

enum Msg {
    Loaded(std::io::Result<usize>),
    Get(ReplyTo<usize>),
}

/// Appends a line to its log at each start, and answers how long the log is.
struct Log {
    files: Arc<dyn Files>,
    len: usize,
}

impl Actor for Log {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let files = self.files.clone();
        let job = cx.submit_future(async move {
            let had = match files.read("log").await {
                Ok(data) => data.len(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
                Err(e) => return Msg::Loaded(Err(e)),
            };
            let start = FileOp::Append("log".into(), Bytes::from_static(b"start\n"));
            Msg::Loaded(files.apply(vec![start]).await.map(|()| had + 6))
        });
        cx.pause_until(&job.map_err(|e| format!("{:?}", e.reason))?).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Loaded(len) => self.len = len.map_err(|e| e.to_string())?,
            Msg::Get(reply) => drop(reply.reply(self.len)),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("axon-store-files-{}", std::process::id()));
    let stores: [Arc<dyn Files>; 2] = [Arc::new(MemoryFiles::new()), Arc::new(OsFiles::new(&dir))];
    for files in stores {
        let system = ActorSystem::builder().build()?;
        for starts in 1..=2 {
            let log = system.spawn(Log { files: files.clone(), len: 0 }).start()?;
            // Asked before the load is done, answered after it.
            assert_eq!(log.ask(Msg::Get).await?, 6 * starts);
            log.lifecycle.stop();
            log.join().await;
        }
        system.shutdown().await?;
    }
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
```

### A log of keyed records

`files::Log` keeps the records of many keys in one directory, for one owner: a store's write-ahead log, a replica's
journal. `append` puts a record at the next position of its key and never waits. In `on_turn_end` the owner takes the
next write with `take_write`, runs it in a `cx.submit_future` job, and hands the result to `written`, which returns the
durable LSN. A change is durable, and its caller may be answered, once that LSN reaches the change's LSN. One write is
out at a time and takes every change queued since the last, so one sync covers every key.

Each key has a fence. `fence(key, epoch)` refuses an epoch at or below it, so two owners given the same epoch cannot
both take the key, and `append` refuses an epoch below it. `checkpoint` stands for a key's records up to a position,
and `trim` and `remove` let records go. Writes remove the files that nothing live is left in. `open` reads the
directory again after a crash, and cuts what a crash left torn at the end.

```rust
use std::sync::Arc;
use axon::files::{Address, Entry, Files, Log, LogConfig, LogError, Lsn, MemoryFiles};
use axon::{Actor, ActorSystem, Context, ReplyTo};
use bytes::Bytes;

type Answer = Result<Address, LogError>;

enum Msg {
    Put(&'static str, Bytes, ReplyTo<Answer>),
    Written(std::io::Result<()>),
}

/// Answers each put once it is durable.
struct Store {
    log: Log,
    waiting: Vec<(Lsn, Address, ReplyTo<Answer>)>,
}

impl Actor for Store {
    type Message = Msg;
    type Error = String;

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Put(key, body, reply) => match self.log.append(key.as_bytes(), 1, body) {
                Ok(appended) => self.waiting.push((appended.lsn, appended.at, reply)),
                Err(refused) => drop(reply.reply(Err(refused))),
            },
            Msg::Written(result) => {
                let durable = self.log.written(result);
                let (done, waiting) = std::mem::take(&mut self.waiting).into_iter().partition(|w| w.0 <= durable);
                self.waiting = waiting;
                for (_, at, reply) in done {
                    drop(reply.reply(Ok(at)));
                }
            }
        }
        Ok(())
    }

    /// Every put of the turn goes in one write; those of the turns while it is out go in the next.
    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if let Some(write) = self.log.take_write(axon::trace::unix_micros() / 1000) {
            let job = cx.submit_future(async move {
                let written = axon::catch_panic(write).await;
                Msg::Written(written.unwrap_or_else(|panic| Err(std::io::Error::other(panic.to_string()))))
            });
            job.map_err(|e| format!("{:?}", e.reason))?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files: Arc<dyn Files> = Arc::new(MemoryFiles::new());
    let system = ActorSystem::builder().build()?;
    // Segments of 1 MiB, not 64, as this one is in memory.
    let cfg = LogConfig { segment_bytes: 1 << 20, max_record: 64 << 10, ..LogConfig::default() };
    let log = Log::open(files.clone(), cfg.clone()).await?;
    let store = system.spawn(Store { log, waiting: Vec::new() }).start()?;
    let a = store.ask(|reply| Msg::Put("a", Bytes::from_static(b"one"), reply)).await?;
    let b = store.ask(|reply| Msg::Put("b", Bytes::from_static(b"two"), reply)).await?;
    assert_eq!((a?, b?), (Address { epoch: 1, seq: 1 }, Address { epoch: 1, seq: 1 }));
    store.lifecycle.stop();
    store.join().await;

    // A new owner opens the directory and fences the key: the old owner's epoch is refused from now on.
    let mut log = Log::open(files, cfg).await?;
    let (_, epochs) = log.fence(b"a", 2)?;
    assert_eq!(epochs, [(1, 1)]);
    assert_eq!(log.fence(b"a", 2), Err(LogError::Fenced { fence: 2 }));
    assert_eq!(log.append(b"a", 1, Bytes::new()), Err(LogError::Fenced { fence: 2 }));
    log.checkpoint(b"a", Address { epoch: 1, seq: 1 }, Bytes::from_static(b"state"))?;
    if let Some(write) = log.take_write(0) {
        let written = write.await;
        log.written(written);
    }
    let through = Address { epoch: 1, seq: 1 };
    let read = log.read(b"a", Address::default(), 1 << 20).await?;
    assert_eq!(read, [Entry::Checkpoint { through, state: Bytes::from_static(b"state") }]);
    system.shutdown().await?;
    Ok(())
}
```

`take_write` gives `None` while a write is out, so a turn that ends meanwhile starts nothing. A failed write sets
`failing()`, and the next write takes the same bytes again. The log has no timer, so this owner tries again at the
end of the turn that got the error. On a disk that keeps failing, wait before the next try (`cx.schedule_after`),
because each try writes all those bytes again. Past `max_unwritten_bytes` not yet durable, a change is
refused `Behind`; past `max_bytes` live, a record is refused `Full`, and `pinning(n)` names the keys that hold the
oldest files, to checkpoint first. `writing_since()` says when the write out began, so an owner can see one that hangs.
Zero-filled segments (`preallocate`, the default) make each sync about 3.3 times faster on build-1 than a file that
grows ([benchmarks](benchmarks.md#durable-log-axon)).

Keyed actors whose state must survive a crash share their node's log through one actor like this one: the program
in [cluster § A key's state on disk](cluster.md#a-keys-state-on-disk) opens the log in `on_start`, fences each key
as it starts, and reads the key back.

### Stashing until ready

A pause holds every message back. An actor that must answer some at once and hold others until it is ready, such as
one that loads its state and still answers a health check meanwhile, stashes the ones that must wait:
`cx.stash(msg)` keeps the message it was handed, outside the mailbox, and `cx.unstash_all()` hands every stashed
message to the handler again, in the order stashed, before anything still in the mailbox, within the same turn budget;
one stashed again as they replay goes to the back. The stash holds as many messages and bytes as the mailbox may
(`MailboxLimits::messages` and `bytes`, weighed as the mailbox weighs them), and past that `stash` hands the message
back for the handler to refuse or fail on. `Diagnostics` does not show the stash, so an actor waiting to be ready
shows an empty mailbox, and its `processed` counts a replayed message again. The stash belongs to one incarnation: what is still stashed when the actor ends, however it
ends, is discarded and counted with what was queued, and becomes a dead letter (`drained` after a clean drain); an ask
among it ends with `ReplyDropped`, and a restart starts with an empty stash. A drain handles what `unstash_all`
released, but does not wait for what is still stashed. While a pause holds, nothing replays.

```rust
use std::time::Duration;
use axon::{Actor, ActorSystem, Context, ReplyTo};

enum Msg {
    Loaded(u64),
    Get(ReplyTo<u64>),
    Health(ReplyTo<&'static str>),
}

struct Account(Option<u64>);

impl Actor for Account {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        let load = async {
            tokio::time::sleep(Duration::from_millis(200)).await; // a read from storage
            Msg::Loaded(100)
        };
        cx.submit_future(load).map(drop).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match (msg, self.0) {
            (Msg::Loaded(balance), _) => {
                self.0 = Some(balance);
                cx.unstash_all();
            }
            (Msg::Health(reply), None) => drop(reply.reply("loading")),
            (Msg::Health(reply), Some(_)) => drop(reply.reply("ready")),
            (Msg::Get(reply), Some(balance)) => drop(reply.reply(balance)),
            // A full stash hands it back; dropped, its asker gets ReplyDropped.
            (get, None) => drop(cx.stash(get)),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let account = system.spawn(Account(None)).start()?;
    // The get waits in the stash for the load; the health check behind it is answered at once.
    let (balance, health) = tokio::join!(account.ask(Msg::Get), account.ask(Msg::Health));
    assert_eq!((balance?, health?), (100, "loading"));
    assert_eq!(account.ask(Msg::Health).await?, "ready");
    system.shutdown().await?;
    Ok(())
}
```

### Sources

A socket, a channel, a stream subscription: anything that yields many items is a source, not a job. `cx.attach` hands it
to the actor for the rest of the actor's life. Every item arrives as a message, `None` follows the last one, and the
actor stopping or draining drops the source. A cancelled or dropped source sends nothing more, but what it had already
sent still arrives: the end, `map(None)`, comes through the mailbox behind the items, so a cancel does not take back an
end already sent. An actor that replaces a source tells their ends apart by what each map captures, such as a generation
number of its own; dropping a handle cancels nothing. Delivery waits for mailbox room, so a full mailbox pauses the
source instead of buffering. Items start no trace of their own; the message that attached the source does not parent
them. Sources and sinks live as long as the actor, so they have their own budget, `MailboxLimits::sources` (1024), and a
source waiting for room does not count against `waiters`. An item heavier than `max_message_bytes` fails the actor with
`Failed("attached source: item refused (TooLarge)")`.

```rust
use axon::*;
use tokio::sync::{mpsc, oneshot};

enum Msg {
    Attach(mpsc::Receiver<u64>, oneshot::Sender<u64>),
    Item(Option<u64>),
}

struct Recv(mpsc::Receiver<u64>);

impl Stream for Recv {
    type Item = u64;
    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<u64>> {
        self.0.poll_recv(cx)
    }
}

struct Summer(u64, Option<oneshot::Sender<u64>>);

impl Actor for Summer {
    type Message = Msg;
    type Error = String;

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Msg::Attach(rx, total) => {
                self.1 = Some(total);
                cx.attach(Recv(rx), Msg::Item).map_err(|e| format!("{e:?}"))?;
            }
            Msg::Item(Some(n)) => self.0 += n,
            Msg::Item(None) => {
                self.1.take().map(|total| total.send(self.0));
                cx.stop();
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let summer = system.spawn(Summer(0, None)).start()?;
    let (tx, rx) = mpsc::channel(8);
    let (total, sum) = oneshot::channel();
    summer.send(Msg::Attach(rx, total)).await?;
    for n in 1..=4 {
        tx.send(n).await?;
    }
    drop(tx); // the source ends, the actor answers and stops
    assert_eq!(sum.await?, 10);
    summer.join().await;
    system.shutdown().await?;
    Ok(())
}
```

Anything that implements `axon::Stream` attaches; `axon_stream::Subscription` does. The handle detaches
the source, which drops it: an unsubscribe is one call.

### Sinks

The other direction: `cx.attach_sink(sink, capacity, map)` hands the actor anything that implements `axon::Sink`,
such as the write half of a socket. A job owns the sink, on the tokio runtime like every job, and the handler only
ever calls `try_send`, which queues the item and never waits. The job feeds the queue to the sink and flushes when
the queue runs dry, so a burst goes out in one write.

`try_send` refuses with `Full` when `capacity` items are queued. The refusal hands the item back, and once the queue
is half empty `map(SinkEvent::Ready)` arrives: keep what did not fit in your own outbox and try again then. That
outbox is where priority lanes, coalescing and a slow-client limit belong, since they are rules of your protocol.
`Actor::on_turn_end` runs once after every turn that handled anything, so one flush covers every message of the
turn, including job results and timers.

```rust
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};

use axon::*;

/// Stands in for the write half of a socket.
struct Wire(Arc<Mutex<Vec<String>>>);

impl Sink<String> for Wire {
    type Error = std::io::Error;
    fn poll_ready(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, line: String) -> Result<(), Self::Error> {
        self.0.lock().unwrap().push(line);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

enum Msg {
    Say(String),
    Wire(SinkEvent<std::io::Error>),
}

struct Echo {
    wire: Option<Wire>,
    tx: Option<SinkHandle<String>>,
    outbox: VecDeque<String>,
}

impl Actor for Echo {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let wire = self.wire.take().ok_or("started twice")?;
        self.tx = Some(cx.attach_sink(wire, 2, Msg::Wire).map_err(|e| format!("{e:?}"))?);
        Ok(())
    }

    fn on_message(&mut self, msg: Msg, _cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Say(line) => self.outbox.push_back(line),
            Msg::Wire(SinkEvent::Ready) => {} // on_turn_end hands over the rest
            Msg::Wire(SinkEvent::Closed(result)) => result.map_err(|e| e.to_string())?,
        }
        Ok(())
    }

    fn on_turn_end(&mut self, _cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(tx) = &self.tx else { return Ok(()) };
        while let Some(line) = self.outbox.pop_front() {
            if let Err(refused) = tx.try_send(line) {
                if refused.reason == RejectReason::Full {
                    self.outbox.push_front(refused.message); // until Ready
                }
                break;
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().build()?;
    let written = Arc::new(Mutex::new(Vec::new()));
    let echo = Echo { wire: Some(Wire(written.clone())), tx: None, outbox: VecDeque::new() };
    let echo = system.spawn(echo).start()?;
    for n in 0..10 {
        echo.send(Msg::Say(format!("line {n}"))).await?;
    }
    echo.lifecycle.drain(None); // writes the outbox, closes the sink, then ends
    assert_eq!(echo.join().await.reason, StopReason::Drained);
    assert_eq!(written.lock().unwrap().len(), 10);
    system.shutdown().await?;
    Ok(())
}
```

`Ready` and `Closed` arrive like job results, so they are handled during a drain too. A drain closes a sink once
the actor has handled everything and is not waiting on a `Ready` from it: the sink writes what is queued, flushes,
closes, and `Closed(Ok(()))` arrives. `SinkHandle::close` does the same at any time, and dropping the handle closes
the sink. A sink error ends it with `Closed(Err(e))` and later `try_send`s are refused with `Closed`. A stop, or
`SinkHandle::cancel`, drops the sink with what it had queued and sends nothing. A drain waits for a sink the actor
still owes, so a peer that never reads holds a drain without a deadline for ever: give it one.

The job feeds at most 64 items per poll before it flushes and yields, so a busy sink never holds an executor thread.
The socket's syscalls happen there; the handler only moved an item into a queue.

A socket is both at once. With tokio-tungstenite the session actor takes the two halves in `on_start`, and the
socket's I/O never runs on the actor's thread:

```rust,ignore
let (write, read) = self.ws.take().ok_or("started twice")?.split();
cx.attach(read, Msg::Client).map_err(|e| format!("{e:?}"))?;            // frames arrive as messages
self.tx = Some(cx.attach_sink(write, 64, Msg::Socket).map_err(|e| format!("{e:?}"))?);
```

The market-data example in the axon-examples repository is a whole session: one actor per client, its subscriptions
attached as sources, an outbox with a slow-client limit, flushed in `on_turn_end`. An actor can own several sockets
the same way, each pair of halves tagged by its own message variant.

## Mailbox limits

Every mailbox is bounded. The defaults:

| Limit | Default | Refusal |
|---|---|---|
| `messages` | 1024 | `Full` |
| `bytes` | 16 MiB | `Full` |
| `max_message_bytes` | 1 MiB | `TooLarge` |
| `waiters` (pending `send`s) | 256 | `WaiterLimit` |
| `requests` (pending local `ask`s; a remote request is a message) | 128 | `RequestLimit` |
| `jobs` (`submit_io`, `submit_future`) | 16 | `TaskAdmissionError::Full`, with the work (`SubmitError`) |
| `sources` (`attach`, `attach_sink`, `watch`) | 1024 | `TaskAdmissionError::Full` |
| `timers` | 128 | `TimerReject::Full` |

```rust
use axon::{Actor, ActorSystem, Context, MailboxConfig, MailboxLimits, RejectReason};

struct Slow;

impl Actor for Slow {
    type Message = Vec<u8>;
    type Error = ();
    fn on_message(&mut self, _msg: Vec<u8>, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
        std::thread::sleep(std::time::Duration::from_millis(200));
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let limits = MailboxLimits { messages: 4, ..Default::default() };
    let system = ActorSystem::builder().mailbox(limits).build()?; // for every actor

    let config = MailboxConfig::from(limits).with_weight(|m: &Vec<u8>| m.len()); // or for one actor
    let slow = system.spawn(Slow).mailbox(config).start()?;

    let refused = (0..16).filter_map(|_| slow.tell(vec![0u8; 64]).err()).next().unwrap();
    assert_eq!(refused.reason, RejectReason::Full);
    assert_eq!(refused.message.len(), 64); // the message comes back
    Ok(())
}
```

A message weighs `size_of::<Message>()` unless you give a `weight` function. Heap data is not counted by default.

## Tests without threads or time

`EmbeddedDriver` runs one actor by hand and `Clock::manual()` moves time by hand, so a test is exact and instant.

```rust
use std::time::Duration;
use axon::{Actor, Clock, Context, EmbeddedDriver, MailboxLimits, TurnBudget};

enum Tick {
    Start,
    Fired,
}

struct Ticker(u32);

impl Actor for Ticker {
    type Message = Tick;
    type Error = &'static str;
    fn on_message(&mut self, msg: Tick, cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Tick::Start => drop(cx.schedule_after(Duration::from_secs(3600), Tick::Fired)),
            Tick::Fired => self.0 += 1,
        }
        Ok(())
    }
}

fn main() {
    let clock = Clock::manual();
    let (mut driver, actor) = EmbeddedDriver::new(Ticker(0), MailboxLimits::default(), clock.clock()).unwrap();
    driver.run_turn(TurnBudget::default()); // on_start
    actor.address.try_tell(Tick::Start).unwrap();
    assert_eq!(driver.run_turn(TurnBudget::default()).ordinary, 1);
    clock.advance(Duration::from_secs(3600));
    assert_eq!(driver.run_turn(TurnBudget::default()).timers, 1);
    actor.address.try_tell(Tick::Start).unwrap();
    assert_eq!(driver.run_until_idle(100), Ok(1)); // Start; its timer is an hour away
}
```

Give `EmbeddedDriver::with_jobs(actor, config, clock, executor)` the executor half of `JobExecutor::manual(n)`, and
`run_until_idle(max_rounds)` runs the jobs it holds, and turns, until nothing is due now: no job queued, message,
timer due or job result. It returns how many turns handled something, or `Err(NotIdle)` if work is still due after
`max_rounds` rounds, as with an actor that keeps telling itself more.

To test a protocol between several nodes, `axon-testkit` has a seeded network simulator with loss, duplicates,
partitions and crashes on the same kind of virtual time; its crate docs hold a runnable example. To run whole
nodes, your actors among them, through crashes, pauses and partitions, see
[Simulating a cluster](cluster.md#simulating-a-cluster).
The same World holds one node's actors (`hold_actors`), pauses the node (`pause`) or starves it (`starve`). Through
each, every actor feature delivers what it was sent or owed once, late: a message, a watch or link's notice, a job's
result, a sink's `Ready`, a drain's end. A periodic timer skips the periods it missed instead of firing them all.

## Without tokio

`ActorSystem` needs tokio. The runtime under it does not: turn off the default `system` feature and use `Runtime`.
It runs no jobs until it is given an executor: without `RuntimeConfig::job_executor`, `cx.submit_io` and
`submit_future` refuse with `TaskAdmissionError::NoExecutor`. `Some(JobExecutor::thread_pool(threads, queued)?)` runs
them on threads of its own.

```rust
use std::time::Duration;
use axon::{Actor, Context, Execution, MailboxLimits, Runtime, RuntimeConfig};

struct Adder(u64);

impl Actor for Adder {
    type Message = u64;
    type Error = ();
    fn on_message(&mut self, v: u64, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0 += v;
        Ok(())
    }
}

fn main() {
    let runtime = Runtime::new(RuntimeConfig { workers: 2, ..Default::default() }).unwrap();
    let actor = runtime.spawn(Adder(0), Execution::Shared, MailboxLimits::default()).unwrap();
    let actor = actor.wait_ready(Duration::from_secs(1)).unwrap();
    actor.address.try_tell(7).unwrap();
    actor.lifecycle.drain(None);
    assert_eq!(actor.lifecycle.wait(Duration::from_secs(1)).unwrap().discarded_messages(), 0);
    runtime.shutdown(Duration::from_secs(1));
}
```

## How it runs

- `workers` threads (default: cores, at most 8). Each actor has a home worker, where a message from outside the pool
  queues it. A message sent from a handler runs its receiver next on the sender's worker when nothing else waits
  there, while what it was sent is still in cache; otherwise the receiver queues there behind the rest. A worker with
  nothing to do takes half of another's queue when that worker is asleep, stuck in one turn, or has eight or more
  waiting.
  So a shared actor may run on any worker; one that must stay on one thread uses `Execution::Dedicated` or a pool
  without `balance` (below).
- A worker gives an actor up to 64 messages per turn, then moves on to the next actor. `Actor::on_batch` sees the
  whole turn at once if you want to batch writes. It must take at least one message each time it is called: what it
  leaves comes back next turn, and taking none fails the actor rather than spinning on the same batch.
- A worker with nothing to do spins for `RuntimeConfig::spin` (20 µs, up to five times that while work keeps arriving
  in time) before it sleeps, because waking a sleeping thread costs a sender several microseconds. Spinning counts as
  busy time in the worker stats. Set it to zero to sleep at once.
- An idle actor costs about 1.2 KB. Its mailbox takes memory only for what is queued; `messages` is a limit, not a
  reservation. The first message reserves `MailboxConfig::first_block` slots (4) in one step, and the queue doubles
  from there; with 64-byte messages that is under 1 KB for an actor that has received anything. Raise it with
  `with_first_block` for actors whose first burst is long. Raise `RuntimeConfig::max_actors` (65 536)
  on nodes that hold more; keyed actors have [two more caps](cluster.md#spawn-actors-on-other-nodes-one-per-key).
- A spawn allocates on the spawning thread, and an actor's end frees on its worker. A program that spawns on one
  thread and ends actors on others spends much of that in glibc's arena lock; a thread-caching allocator such as
  mimalloc took Savina fork-join-create on 1 worker from 82–99 ms to 24–35 ([benchmarks](benchmarks.md#scale-axon-obs)).
- `start()` returns at once; `on_start` runs on the actor's worker. Messages sent before it returns queue behind it,
  under the mailbox limits, and are handled after it. `handle.ready().await` waits for `on_start` when something depends
  on it having run, and for a pause `on_start` took, such as the load of its state; it fails with the start's error, and
  after `ActorSystemBuilder::start_timeout` with `Start(StartError::TimedOut)`, which does not stop the actor. A failed
  `on_start` ends the actor with `StartFailed` and what was queued becomes dead letters.
- Set `RuntimeConfig::turn_budget` to `TurnBudget { max_elapsed: Some(..), .. }` to also cap a turn by time.
- Debug builds run a watchdog thread per runtime. It aborts the process, naming the actor, when an actor has
  something to handle (a message, a job result, a due timer, a stop, the end of a drain, an idle time that has passed)
  and has not been scheduled for two seconds, or is scheduled while every worker of its pool stays in the same sleep
  for two seconds. Either is a bug in axon, a lost wakeup, that would otherwise show only as a hang. A turn that runs
  long is not one: nothing is lost while a worker runs it. Nor is a machine too loaded to run woken workers at once.

### Pools

Actors that need their own cores, turns or wake latency go on a pool of their own. `RuntimeConfig::pools` lists them
and `Execution::Pool(name)` places an actor on one. Each `PoolConfig` sets:

- `workers`, threads named `{name}-{i}` so that `ps`, `perf` and a pinning runbook can find them;
- `cores`, which pins worker `i` to `cores[i % cores.len()]`. `RuntimeConfig::cores` pins the shared pool the same
  way, so the two can be kept off each other's cores. A core the thread calling `build` may not run on fails it
  with `InvalidPool`, so build before narrowing that thread's affinity. `RuntimeConfig::allowed_cores()` lists the
  cores the calling thread may run on, which in a container or under `taskset` need not start at 0;
- `turn_budget`, so that a pool of actors that drain large batches can take thousands of messages a turn while the
  shared pool keeps 64;
- `spin`. `Duration::MAX` never sleeps: a message is picked up within a spin, at the price of a whole core per
  worker, all of it counted busy;
- `balance`. On, the pool passes actors between its workers as the shared pool does. Off, an actor runs only on the
  worker it was placed on, round robin, so its state stays in that core's cache; a turn that blocks then holds up
  the actors placed with it.

A dedicated actor is a pool of one worker with one actor. A pool of one worker holds several.

```rust
use axon::{Actor, ActorSystem, Context, Execution, PoolConfig, ReplyTo, RuntimeConfig, TurnBudget};

struct Shard;

impl Actor for Shard {
    type Message = ReplyTo<String>;
    type Error = ();
    fn on_message(&mut self, reply: ReplyTo<String>, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
        let _ = reply.reply(std::thread::current().name().unwrap_or_default().to_string());
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A cpuset (taskset, docker --cpuset-cpus, Kubernetes' CPU manager) need not hold cores 0 and 1.
    let cores = RuntimeConfig::allowed_cores();
    let shards = PoolConfig {
        name: "shards".into(),
        workers: 2,
        cores: cores.into_iter().take(2).collect(),
        balance: false,
        turn_budget: TurnBudget::messages(4096),
        ..Default::default()
    };
    let config = RuntimeConfig { workers: 2, pools: vec![shards], ..Default::default() };
    let system = ActorSystem::builder().runtime(config).build()?;
    let shard = system.spawn(Shard).execution(Execution::Pool("shards")).start()?;
    assert!(shard.ask(|reply| reply).await?.starts_with("shards-"));
    system.shutdown().await?;
    Ok(())
}
```

## Things that trip people up

- `sink.tell(0)` does not compile for a `u64` message because `tell` takes `impl Into<M>`. Write `0u64`.
- `system.lookup(name)` finds actors started with `spawn`. For supervised ones use `system.lookup_supervised(name)`.
- `.mailbox(config)` on one actor replaces the system's mailbox settings for that actor, including timing samples.
- Handler timings are off until `ActorSystemBuilder::timing_sample_every(n)`.
