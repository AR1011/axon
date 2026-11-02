# Streams

`axon-stream` carries numbered records from one producer to many consumers, across nodes, without duplicates or
silent gaps.

## Quick start

One node publishes, another subscribes. This program is compiled and run by `cargo test --doc -p axon-obs`.

```toml
[dependencies]
axon-stream = { version = "0.1", features = ["cluster"] }
```

```rust
use std::collections::BTreeMap;
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::SharedSecret;
use axon_stream::{DeliveryPolicy, StreamEvent, StreamKey, StreamNode, StreamNodeConfig, SubscribeOptions};
use bytes::Bytes;

async fn node(id: &str, role: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    let mut transport = axon_net::Config::new(id, SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    cfg.labels.insert("role".into(), role.into());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    (system, cluster)
}

#[tokio::main]
async fn main() {
    let discovery = MemoryDiscovery::new();
    let (_system_a, a) = node("a", "origin", &discovery).await;
    let (_system_b, b) = node("b", "edge", &discovery).await;
    b.wait_view(|view| view.members.len() == 2).await.unwrap();

    // The origin has no upstream. The edge pulls from any node labelled role=origin.
    let origin = StreamNode::start(&a, StreamNodeConfig::new(vec![])).await.unwrap();
    let from_origin = BTreeMap::from([("role".to_string(), "origin".to_string())]);
    let edge = StreamNode::start(&b, StreamNodeConfig::new(vec![from_origin])).await.unwrap();

    let key = StreamKey::new("prices", "EURUSD");
    origin.open_stream(&key, 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    let mut prices = edge.subscribe(&key, SubscribeOptions::default()).unwrap(); // live from now on

    tokio::spawn(async move {
        for tick in 0u64.. {
            origin.publish(&key, vec![Bytes::from(tick.to_string())]).unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    let mut seen = 0;
    while seen < 3 {
        match prices.next().await.expect("subscription open") {
            StreamEvent::Batch(batch) => {
                for (sequence, record) in (batch.first()..).zip(batch.records()) {
                    println!("{sequence}: {record:?}");
                    seen += 1;
                }
            }
            terminal if terminal.is_terminal() => panic!("stream ended: {terminal:?}"),
            _ => {} // Ready, CaughtUp
        }
    }
}
```

- A subscriber gets every record once and in order, or one explicit `ResetRequired(reason)` telling it to start over.
  Never a duplicate, never a silent gap.
- The default start is live, and its `Ready { start }` names the last record before it. On a node that does not carry
  the stream yet, the first `Ready` is `None` and a second names where the node joined: its parent's position when the
  pull reached it. Records published on another node after `subscribe` returned here can come before that, since nothing
  orders the two; to miss nothing from a known point, start from a cursor or `First`. `SubscribeOptions { start:
  StartPosition::After(cursor), .. }` resumes after the last record you handled, and `StartPosition::First` starts at
  record 1 of the current generation. Both work on any node. History is a bounded in-memory ring on each node (by
  default 16 384 records or 16 MiB per stream, whichever comes first: `CoreConfig::store`, and `CoreConfig::rings` per
  namespace), holding what has passed through that node. It bounds how long a slow or disconnected subscriber can be
  away and still resume. A node that carries the stream serves the position from its ring, and `subscribe` refuses one
  the ring no longer covers with `HistoryUnavailable`, and a cursor of another generation (from before a restart) with
  `GenerationMismatch`: start that one again from `First` or `Live`. A cursor ahead of a relay's position, from a client
  that comes from a node further along, waits for the relay to reach it; an origin refuses one ahead of it with
  `HistoryUnavailable`. A node that does not carry the stream asks its parents from the earliest position its
  subscribers and children still want, and the first ring on the path that covers the position serves it (the origin
  holds the most); a position no ring covers ends with `ResetRequired(HistoryUnavailable)`. A relay that falls behind
  its parent's ring has each node it feeds ask again from its own position, so one already past what it lost goes on.
- A live subscription follows the key. The publisher restarting the stream, or closing it and opening it again later,
  shows as a new `Ready { start }` (`None` until a relay learns the position), never as a terminal event; between a
  close and the next open it receives nothing and keeps the node pulling the key; a relay notices the new stream when it
  next asks its parents, at the refusal backoff (2 s doubling to 64 s per tier; a membership change, such as the
  origin's node coming back, asks again at once a parent that had lost its own upstream or lacked the key, and one
  that first refuses so within 2 s of it, from a view that has not caught up yet), and the generation it then shows
  may be lower than the last one when the parent is itself behind. Every node takes a new
  generation from its record 1 while its parent's ring still holds it, so a follower anywhere sees the whole of it, as
  one on the origin's node does; otherwise, or when the node may not replay from its parent, it takes the live edge, and
  the `Ready` shows a start past record 1. An origin that has `upstream` tiers of its own pulls the key it closed like
  any relay while a live follower on its node, or a node it fed, still wants it: they follow the key to the node that
  opens it next. Without tiers they wait for it to open there again. Its next `open_stream` of the key takes it back.
  Any node that relays a key becomes its origin by opening it, as a producer does that moved after its node crashed:
  its followers see the new `Ready`, it stops pulling, and the nodes it fed each ask it again from their own position,
  so the same generation resumed from disk goes on for their followers where they were, with no record twice and none
  skipped. Give it a generation above the last one: a cursor names a position in one generation, and a new origin that
  reuses the number gives the same positions to other records, which a cursor from the old one then takes as its own.
  Keeping one origin at a time is the application's part, for example a keyed actor under a lease. When no parent
  reaches the origin any more (every parent lacks the key or has lost its own upstream, or none has fed it for
  `upstream_timeout`), a follower on another node gets `StreamEvent::OriginLost`, once per loss, and one that
  subscribes there before a parent feeds the key again gets it after its `Ready`. It is not an end: the follower keeps
  its position, the same generation goes on if the origin comes back, and a new origin shows as the next `Ready`. A
  close is not a loss and is not told. Only what the subscriber itself does ends it: falling behind, unsubscribing, or
  losing its authorization; and records lost to its node, which a `Live` follower is never moved past in silence: when
  its node gives up a position no parent holds and takes the same generation up further on, it ends with
  `ResetRequired(HistoryUnavailable)`. A subscription that starts from a cursor, `First` or `SnapshotThenLive` holds a
  position in one generation: it goes on through an origin that resumes that generation [from disk](#history-on-disk),
  and ends with `ResetRequired(GenerationChanged)` at a new generation or `End(StreamClosed)` at a close, because its
  position, or the state it built, is meaningless afterwards. Subscribe again to start over.
- Put a middle tier in by giving nodes more `upstream` label sets, tried in order:
  `vec![{role: relay, region: eu}, {role: origin}]`. Relays fetch one copy of a stream however many subscribers they
  have, and children move to another parent when theirs fails.
- Every queue is bounded. A subscriber that falls too far behind is reset with `SlowConsumer` rather than slowing
  the others. A parent sends a backlog in several pushes per round trip, up to half of a stream's branch
  (`CoreConfig::branch`, `BranchLimits::BRANCH`: 32 MiB) in flight, and a node hands each push to its subscribers as
  it arrives. A subscription's default `BranchLimits` hold what the default config has in flight of one stream
  (17 MiB, 36864 records, 3072 events); smaller custom limits are reset by such a burst unless the subscriber reads as
  it comes, and a larger custom branch needs larger ones. The hub's own
  caps (`HubLimits::max_queued_bytes`, 256 MiB, and `max_queued_records`, a million) are shared by every subscriber:
  15 default subscribers that do not read fill the bytes (16 of small records, the records), so a node that serves
  many untrusted ones (a gateway) gives them smaller `limits`.
- `StreamNode::start` must be called inside the cluster's tokio runtime. Dropping a `Subscription` unsubscribes.

The rest of this page is the reference.

## Layers

Three layers, each usable alone:

| Layer | What it is |
|---|---|
| `StreamHub` | Synchronous, single-owner fanout state machine. No threads, clock or sockets. |
| `RelayCore` | One node of a stream tree as a state machine: a hub, a history ring, downstream branches, upstream parents. Still no I/O; the fault simulation drives it directly. |
| `StreamNode` (feature `cluster`) | Runs a `RelayCore` on an `axon-cluster` node: picks parents from membership labels and moves the frames. |

## Model

A stream is a `StreamKey` (namespace + name) and a run of records numbered from 1 within a `generation`. A `Cursor`
is `(generation, sequence)` and names the last record seen. Every hop keeps the origin's numbers, which is what lets
any parent continue any child.

A generation only goes up: `restart_stream` refuses one that is not above the current one, and a relay refuses a
parent that shows a generation older than the newest it has seen, since that parent is behind the origin. The
start time in milliseconds is a generation that survives a restart of the publisher; a stream kept
[on disk](#history-on-disk) resumes only when it is opened with the generation its files hold instead.

A subscription sees `Ready`, then `Snapshot` if it started `SnapshotThenLive`, replayed batches, `CaughtUp`, live batches, and
another `Ready` at each restart of the key it follows. It ends with exactly one `ResetRequired(reason)` (continuity
broke: start over) or `End(reason)`. `CaughtUp` means the prefix
was emitted, not applied. Admission is not delivery. History is a bounded in-memory ring (`MemoryStore`), or whatever
`RecoverySource` the application supplies; an origin can also keep it [on disk](#history-on-disk) to resume after a
crash.

A batch published inside a trace carries it to every live subscriber (`SequencedBatch::trace_context`), and each
node it crosses records a span ([tracing](tracing.md)).

Guarantee: within a generation a subscriber gets every record once, in order, or an explicit reset. Never a
duplicate, never a silent gap.

## Hub

```rust,ignore
let mut hub = StreamHub::new(AllowAll, HubLimits::default())?;
hub.open_session(&me)?;                                   // only after authenticating the peer
hub.open_stream(&me, &key, generation, DeliveryPolicy::ReplayRequired)?;
let batch = hub.publish(&me, &key, records)?;             // a rejection hands `records` back
let id = hub.subscribe(&me, &key, SubscribeOptions { start: StartPosition::After(cursor), ..Default::default() }, Some(store))?;
hub.drive_recovery(id, bounds)?;                          // one bounded read; loop it, polling in between
while let Some(event) = hub.poll(id) { /* capacity is released here */ }
```

- Start positions: `Live`, `After(cursor)`, `First` (after record 0 of the stream's generation), `SnapshotThenLive`
  (the state of a [compacted](#compacted-streams) stream, then every record after it; an empty state at record 0 on
  any other stream). The recovery source is bound at subscribe. The cut is the cursor at subscribe time, and live batches after it queue
  behind the replay from that same call. On a relay stream with no position yet, `After` and `First` queue nothing
  until it has one, nor does a cursor ahead of a relay's position in the same generation: they go live at the cursor
  once the stream reaches it, and end with `ResetRequired` if it starts past the cursor (`HistoryUnavailable`) or in
  another generation (`GenerationChanged`).
- `hub.follow(&me, &key, options)` is the `Live` subscription that survives `restart_stream` and `close_stream`
  (a `Ready` at each, parked while the key is closed); `subscribe` ends at both. A `StreamNode` follows for its
  local live subscribers and subscribes for its peers.
- One queue per subscription with byte, record and event bounds, plus hub-wide bounds charged per branch. A branch
  that cannot take a batch ends by its `SlowConsumerPolicy`: `Detach`, `Reset`, or `CoalesceLatest` (only on a
  `ReplaceableState` stream, one record per batch). A slow branch never holds back another.
- Recovery never waits on a full global pool when the branch has nothing to poll: it resets with `GlobalCapacity`,
  because retained live batches could hold every credit forever.
- Authorization is exact-key, checked when a session acts. `update_authorizer` is the revocation boundary: it ends
  every subscription and drops every interest the new policy refuses. Nothing is re-checked per event.
- `accept_batch` is the relay entry: positions already published are dropped by position, a gap fails without
  mutation and returns the batch. A relay stream (`open_relay_stream`) has no position until its parent reports one.
- `replace_interests` keeps a versioned desired set per session; a stale session or revision cannot lower demand.
- `close_stream` frees the slot and ends subscribers with `StreamClosed`.

## Tree

```text
producer (usa, role=origin)
 ├── relay-a (eu, role=relay) ── ws-1 … ws-5
 └── relay-b (eu, role=relay)
```

```rust,ignore
// leaf: prefer a relay in my region, else go direct
let tiers = vec![labels(&[("role", "relay"), ("region", "eu")]), labels(&[("role", "origin")])];
let node = StreamNode::start(&cluster, StreamNodeConfig::new(tiers)).await?;
let mut subscription = node.subscribe(&key, SubscribeOptions::default())?;
while let Some(event) = subscription.next().await { /* … */ }

// relay: StreamNodeConfig::new(vec![labels(&[("role", "origin")])])
// origin: StreamNodeConfig::new(vec![]), then node.open_stream(..).await and node.publish(..)
```

A `Subscription` is an `axon::Stream`, so an actor owns one with `cx.attach(subscription, map)` and gets every
event as a message; see the sources section of the actors guide.

- **Parents come from labels, not node ids.** Each tier is a label set matched against cluster membership; the first
  tier with an available member wins, and inside a tier the order is a rendezvous hash of the child's node id, so
  children spread over the relays and each keeps a stable choice. When one namespace originates elsewhere, give it
  its own tiers: `StreamNodeConfig::new(tiers).upstream_for("candles", vec![labels(&[("role", "processor")])])`
  asks those for `candles/*` and the default tiers for everything else, instead of trying tiers that will refuse.
  A namespace's tiers never fall back to the default: with no member matching them its streams wait.
- **Any depth, no loops.** A relay pulls from a relay the same way it pulls from an origin. A parent answers each
  wanted stream with the path it crosses, origin first (`[origin, relay-1]`). A child that finds its own id in the
  path, or a path of `MAX_PATH` (32) nodes, refuses that parent with `loop` and takes the next candidate, so tiers that
  make two relays each other's parent cost one refresh and show as a refusal, not a silent stall.
  A relay whose own path changes (a new parent, a lost one, a change further up) sends the new path to its children
  as a `Path` push item, so a change reaches the whole subtree in one pass instead of one `refresh` per level.
- **One copy per relay.** A leaf's first local subscriber makes the leaf ask its parent, whose first interest makes it
  ask the origin. Interest outlives the last listener by `linger` (5 s), so a reconnecting client does not rebuild the
  cross-region branch.
- **Failover.** A parent that leaves the membership view, or stops being reachable, is replaced at once by the next
  candidate: the other relay, then the origin. The child asks for `After(its cursor)`, so the new parent replays
  what was missed from its ring.
- **Failback.** When a better parent has been available for `hold_down` (10 s) the child subscribes to it while the
  old one still feeds the stream; overlap is dropped by position, and the old branch is released when the new one
  reports `CaughtUp`. A parent that refuses a stream is retried with exponential backoff from `retry_refused`.
- **Per-stream fallthrough.** A parent that lacks a stream, or the history a child needs, refuses that stream only;
  the child asks the next candidate for it. If every candidate lacks the history, and none has appeared for
  `upstream_timeout` (3 s), counting one that left the view and came back however briefly, the stream rejoins live:
  local subscribers get `ResetRequired(HistoryUnavailable)`, a `Live` one a new `Ready` when the stream comes back in a
  newer generation and `ResetRequired(HistoryUnavailable)` when it comes back in the same one, past what the node
  lost. The wait is for a heal, after which a relay that restarted and took the stream up live can answer before the
  origin, which holds the history, is back in view.
- **A relay that loses its own upstream** for `upstream_timeout` (3 s) ends its downstream branches with
  `UpstreamLost`, so children fail over instead of waiting on a silent link.
- **Timings per namespace.** `upstream_timeout` and `hold_down` are a key's, in `core.timings` (`RelayTimings`).
  `StreamNodeConfig::relay_timings(namespace, timings)` sets them for the keys of one namespace: a namespace whose
  origins are in another region can wait out a redial there (7 s and 30 s) while the rest keep 3 s and 10 s.
- **Self-repair.** A child sends its parent only what changed, against the revision the parent last applied. An empty
  interest every `refresh` (2 s) checks that revision; a parent that holds another one (it restarted, or dropped the
  child) answers out of sync and the child sends its set again, whole, in key-range slices. The whole set also goes
  out every `resync` (60 s). Pushes are numbered, sent again until answered, and applied once (below). A child whose
  pushes on one lane have failed, or none of whose interests has arrived, for `downstream_expiry` (10 s) is dropped,
  on every lane: its interests cover them all. One lost interest leaves
  a healthy child silent for `refresh`, the cluster's `request_timeout`, `retry_failed` and two ticks, so
  `StreamNode::start` refuses an expiry that does not outlast that.
- **Revocation.** Records are pushed, not asked for, so the silence rule above is what stops them. A child the parent's
  cluster no longer lets call `axon.stream` (`Cluster::set_authorizer`) has its refreshes refused `Forbidden` and is
  dropped within `downstream_expiry` and a `tick` of the revoke: nothing published after that reaches it, and its
  followers hear nothing more, no end and no reset. Granted access again, its next refresh is answered out of sync,
  and it sends its set again and reads on from its cursor, out of the parent's ring, or is reset
  (`HistoryUnavailable`) when the ring no longer holds it. The stream authorizer of a `StreamNodeConfig` is fixed at
  start and checked when a child asks for a stream: a running stream is revoked per peer, through the cluster's.

The wire is three cluster endpoints: `axon.stream` takes interests on `Lane::Control`, `axon.stream.live` and
`axon.stream.bulk` take pushes on `Live` and `Bulk`, and each refuses the other request `InvalidMessage`. A namespace's
pushes go on `Live` unless `StreamNodeConfig::lane(namespace, Lane::Bulk)` names another: put a namespace that moves
much more than its node's calls on `Bulk`, so its backlog holds up neither the live namespaces' records nor the calls.
The parent's lane wins, and `start` refuses `Control`. `Interest` is a change to what the child wants from that
parent: streams wanted (`key`, branch epoch, position), streams dropped, the revision it builds on, and for a resync
the key range it replaces. A node that has no position for a stream asks for it from the earliest start anything on
it wants, live, after a cursor or `First`, and asks again when an earlier one comes after the first request went out. It never carries more than `max_keys_per_interest` (4096) streams, nor more bytes than the smaller of `max_push_bytes` and Control's `max_frame`. The
reply is a status and a path per wanted stream, each distinct path sent once. `Push` is a bounded run of events for
many streams, numbered from 1 on a channel the parent opens for each child. The child applies only the next push of
the channel it follows, and answers `Applied` with the items it does not want, `Skipped` with the last push it
applied, or `Retired` for a channel or a boot of the parent it no longer follows: the parent then drops the child,
whose next interest opens a new channel from its position. A failed call or a `Skipped` push goes again, and every one
after it, in order, as the same bytes. A push goes to the boot of the child that asked for it: once another boot of the
child is up, it is refused unsent (`CallError::StaleNode`) and the parent gives that boot up at once. The relay core
keeps a window of pushes unanswered per child and lane, each a channel of its own, from twice `max_push_bytes` up to `core.max_push_window` (64 MiB):
three times the child's delivery rate times the round trip. A stream's records in flight count against its branch until the
child applies them, and the stream takes a new push only while they are under half of it, so a child that falls
behind is still reset `SlowConsumer`. All channels together have at most `core.max_push_inflight` (256 MiB) in flight
past each one's first push, shared fairly between the busy ones. After a push under half of `max_push_bytes` that
took all there was, what is published waits for an answer and goes in one push, unless a reset or an end comes: a
trickle costs a push per round trip, not one per publish. A stream node sends a child each push the window
lets go on a lane without waiting for the answers before it, one task per child and lane, and hands the answers to the relay core in push order; after a
failed call, or a push the child skipped for a gap, it sends again a tick later. An encoded push
never exceeds `max_push_bytes` (just under 1 MiB). That size must fit the `max_frame` of each lane the namespaces
use (`cluster.max_frame(lane)`), which `StreamNode::start` checks on the local side, and must hold the largest batch the hub admits (`hub.max_batch_bytes`,
512 KiB on a stream node). Raise all three together for bigger records. A received batch is copied out of the frame
in one block, so a ring holds its records' bytes, never the transport's read buffer they arrived in. A child that
stops wanting anything leaves no session behind.

One stream per topic is the intended use: `candles.binancef.btc/usd.1m` is a stream, not a filter on one. A node's work follows what changes, not how many streams it holds: a quiet stream costs memory only, a
subscription change costs one small interest, a push visits only branches with something to send. History is sized per
namespace with `CoreConfig::rings` (`RingLimits { namespace, max_records, max_bytes }`; the store limits are the
default), which is what bounds memory with tens of thousands of streams; a batch larger than its ring keeps its tail.
A node holds 4096 streams by default (`hub.max_streams` and `store.max_streams`), and `open_stream` past that is
refused `Recovery(LimitExceeded)` (the store is full) or `LimitExceeded` (the hub is, relayed keys included). Raise those, `hub.max_subscriptions` and `hub.max_interest_keys` to fit. Measured
figures are under Scale in [benchmarks](benchmarks.md).

Limits worth knowing: a relay stream is always `ReplayRequired` downstream, so `CoalesceLatest` applies only on the
origin node; a node that already carries a stream answers a cursor or `First` from its own ring, never from its
parents; parent choice is per child, not per stream.

## Many origins

A stream has one origin, the node that opened it. For many producers, give each a stream of its own, such as
`alerts/<node id>`. The consumer follows every producer in the set: it attaches
`cluster.nodes(&producers).changes()` ([cluster § What each call promises](cluster.md#what-each-call-promises)),
subscribes to each producer that joins, from `First` so it misses nothing the ring still holds, and cancels the job of
each one that leaves. It keeps, per stream,
the cursor of the last record it handled, and when a subscription ends it subscribes again from that cursor, in the
handler itself: a message to itself could be refused by its own full mailbox.

```rust,ignore
// in on_start: the nodes labelled producer=yes, each now and then each that joins or leaves
let producers = Placement::default().require("producer", "yes");
cx.attach(self.cluster.nodes(&producers).changes(), Listen::Producer)?;

fn follow(&mut self, cx: &mut Context<Self>, origin: String) -> Result<(), StreamError> {
    let start = self.cursors.get(&origin).map_or(StartPosition::First, |&c| StartPosition::After(c));
    let subscription = self.streams.subscribe(&alerts(&origin), SubscribeOptions { start, ..Default::default() })?;
    let name = origin.clone();
    let job = cx.attach(subscription, move |event| Listen::Event(name.clone(), event))?;
    // An end from a job this replaced, after the producer left and came back: dropping a job does not cancel it.
    if let Some(old) = self.following.insert(origin, job) {
        old.cancel();
    }
    Ok(())
}

// in on_message
Listen::Event(origin, Some(StreamEvent::Batch(batch))) => {
    // .. handle the records, then
    self.cursors.insert(origin, batch.cursor());
}
// The producer restarted or closed the stream: its cursor means nothing now.
Listen::Event(origin, Some(StreamEvent::ResetRequired(ResetReason::GenerationChanged) | StreamEvent::End(EndReason::StreamClosed))) => {
    self.cursors.remove(&origin);
}
// The job ended; follow again unless the producer left.
Listen::Event(origin, None) if self.following.contains_key(&origin) => self.follow(cx, origin)?,
Listen::Producer(Some(NodeChange::Joined(origin))) => self.follow(cx, origin.to_string())?,
Listen::Producer(Some(NodeChange::Left(origin))) => {
    self.following.remove(&*origin).map(|job| job.cancel());
    self.cursors.remove(&*origin);
}
// The cluster shut down.
Listen::Producer(None) => cx.stop(),
```

A consumer that needs only what is published while it follows can subscribe `Live` instead, once per producer: the
subscription follows the stream through the producer's crashes and restarts, `StreamEvent::OriginLost` says the producer
is gone, and each new generation arrives from its record 1 while the rings hold it, so there is no cursor to keep and
nothing to subscribe again.

## Compacted streams

A stream opened `DeliveryPolicy::Compacted(CompactLimits { max_keys, max_bytes })` is a table: each record is
`keyed(key, value)`, and a record with an empty value removes its key. The origin keeps the latest record of each key,
and a subscriber that starts `SnapshotThenLive`, on any node, gets them as one `Snapshot`, then every record after it:
`Ready { start }`, `Snapshot` (its `through()` is that start), the replay, `CaughtUp`, live, with nothing between the
state and the records that follow it missed or seen twice. That subscriber holds the state of one generation, unlike
a `Live` one: a new generation ends it with `ResetRequired(GenerationChanged)`, and a new subscribe gets the new state.
The state is bounded where it is kept: a publish that would leave more than `max_keys` keys or `max_bytes` bytes of
latest records (each counted whole) is refused `StateFull` and changes nothing, and one with a record that is not keyed
is refused `Recovery(InvalidBatch)`. Removing keys makes room.
Only the origin holds the state. A relay passes the records on as they are and asks its own parent for the state when a
subscriber of its needs one; the snapshot travels in one push item and waits in one branch queue, so `open_stream`
refuses limits that would not fit `max_push_bytes` or `BranchLimits::max_bytes` with `InvalidOptions`. A subscribe
whose own `limits.max_bytes` cannot hold the state now is refused `LimitExceeded` at the origin, and ends with
`End(SlowConsumer)` on a relay once the state arrives, as it does where a relay's smaller pushes or branches cannot carry
it on. A node that `Access::Snapshot` denies the state ends its waiting subscribers with `End(Revoked)`; either way the
key's records keep flowing to everyone else. A relay that crashes loses nothing here: its children ask the next parent,
and a subscriber waiting for the state gets it from there.

```rust
use std::collections::BTreeMap;

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::SharedSecret;
use axon_stream::{
    CompactLimits, DeliveryPolicy, StartPosition, StreamEvent, StreamKey, StreamNode, StreamNodeConfig,
    SubscribeOptions, keyed, split_keyed,
};
use bytes::Bytes;

async fn node(id: &str, role: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    let mut transport = axon_net::Config::new(id, SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    cfg.labels.insert("role".into(), role.into());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    (system, cluster)
}

fn apply(book: &mut BTreeMap<Bytes, Bytes>, records: &[Bytes]) {
    for record in records {
        let (key, value) = split_keyed(record).expect("a compacted stream carries keyed records");
        if value.is_empty() {
            book.remove(&key);
        } else {
            book.insert(key, value);
        }
    }
}

#[tokio::main]
async fn main() {
    let discovery = MemoryDiscovery::new();
    let (_system_a, a) = node("a", "origin", &discovery).await;
    let (_system_b, b) = node("b", "edge", &discovery).await;
    b.wait_view(|view| view.members.len() == 2).await.unwrap();
    let origin = StreamNode::start(&a, StreamNodeConfig::new(vec![])).await.unwrap();
    let from_origin = BTreeMap::from([("role".to_string(), "origin".to_string())]);
    let edge = StreamNode::start(&b, StreamNodeConfig::new(vec![from_origin])).await.unwrap();

    let key = StreamKey::new("positions", "desk-1");
    let limits = CompactLimits { max_keys: 1024, max_bytes: 256 << 10 };
    origin.open_stream(&key, 1, DeliveryPolicy::Compacted(limits)).await.unwrap();
    origin.publish(&key, vec![keyed(b"EURUSD", b"100"), keyed(b"USDJPY", b"-40")]).unwrap();
    origin.publish(&key, vec![keyed(b"EURUSD", b"250"), keyed(b"USDJPY", b"")]).unwrap(); // USDJPY is gone

    // A late subscriber on another node: the state as of its start, then every change after it.
    let options = SubscribeOptions { start: StartPosition::SnapshotThenLive, ..Default::default() };
    let mut positions = edge.subscribe(&key, options).unwrap();
    let mut book = BTreeMap::new();
    let mut live = false;
    while book.len() < 2 {
        match positions.next().await.expect("subscription open") {
            StreamEvent::Snapshot(state) => apply(&mut book, state.records()),
            StreamEvent::Batch(batch) => apply(&mut book, batch.records()),
            StreamEvent::CaughtUp { through } => {
                assert_eq!(book, BTreeMap::from([(Bytes::from("EURUSD"), Bytes::from("250"))]));
                println!("state as of record {}: {book:?}", through.sequence);
                origin.publish(&key, vec![keyed(b"GBPUSD", b"75")]).unwrap();
                live = true;
            }
            terminal if terminal.is_terminal() => panic!("stream ended: {terminal:?}"),
            _ => {} // Ready
        }
    }
    assert!(live, "the change came after the state");
    println!("live: {book:?}");
}
```

## History on disk

An origin can keep the history of a namespace's streams in files: set `StreamNodeConfig::disk` to a `DiskConfig` naming
the files (`OsFiles::new(dir)`) and, per namespace, a `Retention` by bytes and by age. Other namespaces keep only
their ring. Only the node that opens a stream writes it; relays keep their rings, and nothing is replicated.

Two words, two calls. A record is *published* when `publish` returns `Ok(cursor)`: it is admitted, numbered, and
waits for the next write. It is *durable* once that write is synced, and only then does it reach the ring, the
subscribers and the children: what any subscriber sees is durable. `node.cursor(&key)` is the last durable record,
and `node.wait_cursor(&key, cursor).await` waits until it is that far: an answer sent after it survives any crash. The
wait ends with `UnknownStream` if the stream is closed, restarted or the node dropped first, and goes on while the disk
fails (each write is tried again), so bound it with the caller's own deadline. One write is out at a time, off the
actor turns and off the publishing task. A publish to an idle disk starts a write at once; what is published while a
write is out goes in the next, which starts as soon as that one lands, all of it in one write. So a record waits for at
most one write in progress and its own, never for a timer, and the busier the disk the more each write holds. A
process crash loses nothing durable; a machine crash or power loss also loses nothing durable, which leaves at most
what was published during the last two writes. Dropping the `StreamNode` loses what was published and not durable yet,
as a crash does.

One node keeps a directory at a time. `OsFiles` locks it from the start until the node is dropped and a write it had
begun has landed; a `StreamNode::start` on it meanwhile, in this process or another, fails with an error saying it is in
use, and can be tried again. The program below drops its node with nothing left to write, so it starts again at once.

After a restart, opening the stream again with the same generation resumes it: the next record follows the last
durable one, the ring is filled from the files, and a compacted stream has its state again. `node.stored(&key)` names
the generation and last record the files hold, until the stream is opened. A subscriber that holds a cursor continues
`After(cursor)` on any node with nothing missed or seen twice. Replay still comes from the rings: the files serve the
restart, and a start behind the ring is `HistoryUnavailable` as without them. Opening a newer generation,
`restart_stream` or `close_stream` removes the old files; opening an older one is refused with `GenerationMismatch`.

`StreamNode::start` reads every segment of the listed namespaces once to check it, one at a time, keeping only their
positions, and `open_stream` reads back the newest segments until they fill the ring. Both await the files. An open is
a future that starts nothing until polled: from an actor's handler, hand it to `cx.submit_future`. Dropped before it
finishes, it leaves the stream unopened. The node's other streams go on publishing and serving while an open reads: it
takes the node's lock only to install what it read. What a crashed process wrote and never synced is synced as it is
read, before anyone sees it. Files of namespaces no longer listed stay as they are. Of a stream not opened again,
`start` keeps the newest generation up to its last whole record and removes the rest.

A compacted stream kept on disk is a keyed store of its own. Publish the change, `wait_cursor`, then answer. After a
restart, `open_stream` of the same generation resumes it, and a `SnapshotThenLive` subscription at the origin reads the
state back. A store kept in two places, such as files of its own and a stream, cannot write both in one step. Write the
files first, then publish and wait, and answer after both. On a start, publish again what the files hold past the
stream's state.

With [namespace limits](#namespace-limits), a stream kept on disk counts toward `ring_bytes` as any other: its ring and
compacted state from the moment it is opened, resumed or not, and a resumed state past what the namespace has room for
refuses the open with `QuotaExceeded` (`RingBytes`), leaving the files for a later open. A publish refused by a limit
is not written.

Each stream is a run of segment files, `namespace.name.generation.first.created.seg`, each record batch framed with its
length and a CRC-32. A write syncs every file it appended to, `OsFiles` up to 32 of them at once: on a disk where one
sync takes 4 ms, a write to 256 streams took 53 ms, against 347 ms one file after another
([benchmarks](benchmarks.md#history-on-disk)). A stream moves to a new segment past `segment_bytes`, and retention
removes the oldest whole segments; the one being written, and any a write still owes frames to, stay. A tail torn by a
crash, or a corrupt frame, is cut at the last whole record when the node starts, and every segment after it is removed:
counted in `RelayStats::disk.truncated` and `truncated_bytes`, never a panic. The generation goes on after the last
whole record, or, with every segment cut, after the position the first one's name proves, and that segment stays,
emptied, to prove it across later restarts: a subscriber that had read past the cut should start over, and opening a
newer generation makes every one of them do so. A write that fails, a full disk or a store that panics included, is
tried again every tenth of a second; until one succeeds each publish is refused with
`StreamError::Disk(DiskError::Full)` or `Disk(Failed)`, and past `max_unwritten_bytes` waiting for the disk with
`Disk(Behind)`. Nothing of a refused publish is admitted, and the stream keeps serving what is durable.
`RelayStats::disk` and the `stream.disk_*` [metrics](telemetry.md) show all of it.

```rust
use std::sync::Arc;
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_net::SharedSecret;
use axon_stream::{
    DeliveryPolicy, DiskConfig, OsFiles, Retention, StartPosition, StreamEvent, StreamKey, StreamNode,
    StreamNodeConfig, SubscribeOptions,
};
use bytes::Bytes;

fn config(dir: &std::path::Path) -> StreamNodeConfig {
    let mut cfg = StreamNodeConfig::new(vec![]);
    let orders = Retention { namespace: "orders".into(), max_bytes: 1 << 30, max_age: Duration::from_secs(7 * 86_400) };
    cfg.disk = Some(DiskConfig::new(Arc::new(OsFiles::new(dir)), vec![orders]));
    cfg
}

#[tokio::main]
async fn main() {
    let dir = std::env::temp_dir().join(format!("axon-history-{}", std::process::id()));
    let mut transport = axon_net::Config::new("a", SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true;
    let system = ActorSystem::builder().build().unwrap();
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, MemoryDiscovery::new());
    let cluster = Cluster::start(&system, cfg).await.unwrap();

    let key = StreamKey::new("orders", "desk-1");
    let origin = StreamNode::start(&cluster, config(&dir)).await.unwrap();
    origin.open_stream(&key, 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    let mut published = None;
    for i in 0..100 {
        published = Some(origin.publish(&key, vec![Bytes::from(format!("order {i}"))]).unwrap());
    }
    // Published is not durable: wait for the write that syncs the last record, and so every one before it.
    let seen = origin.wait_cursor(&key, published.unwrap()).await.unwrap();
    drop(origin); // as a crash would: what is durable stays

    let origin = StreamNode::start(&cluster, config(&dir)).await.unwrap();
    origin.open_stream(&key, 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    assert_eq!(origin.cursor(&key), Some(seen), "the generation goes on after its last durable record");
    let options = SubscribeOptions { start: StartPosition::After(seen), ..Default::default() };
    let mut orders = origin.subscribe(&key, options).unwrap();
    let next = origin.publish(&key, vec![Bytes::from("order 100")]).unwrap();
    assert_eq!(next.sequence, seen.sequence + 1);
    loop {
        match orders.next().await.expect("subscription open") {
            StreamEvent::Batch(batch) => {
                assert_eq!(batch.first(), 101, "nothing missed, nothing twice");
                break;
            }
            terminal if terminal.is_terminal() => panic!("stream ended: {terminal:?}"),
            _ => {} // Ready, CaughtUp
        }
    }
    println!("{:?}", origin.stats().disk.expect("kept on disk"));
    drop(origin);
    system.shutdown().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}
```

## Standby

A standby keeps what a consumer builds from a stream and takes over when the consumer is gone: snapshot the state
with the cursor of its last record, and on taking over load the snapshot and subscribe `After(cursor)`. Snapshot at
least once per ring turnover (`RingLimits`), or the rings no longer hold what follows it
(`HistoryUnavailable`, [above](#quick-start)). What the origin published but no node relayed before it restarted is gone.
A standby that follows `Live` is told `OriginLost` when the origin goes, and gets the next generation from its record 1
while the rings still hold it.

## Namespace limits

A namespace shared by tenants, or by one noisy producer and everyone else, gets limits of its own:
`StreamNodeConfig::limit(namespace, NamespaceLimits { .. })`, with the same limits on every node. Subscriptions and
ring bytes hold on each node, exactly. The publish rates and the open streams hold across the cluster, through the
[`Quotas`](cluster.md#quotas) passed as `StreamNodeConfig::quotas`; `start` refuses such a limit without it.

```rust
use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery, QuotaConfig, Quotas};
use axon_net::SharedSecret;
use axon_stream::{DeliveryPolicy, NamespaceLimit, NamespaceLimits, StreamError, StreamKey, StreamNode, StreamNodeConfig};
use bytes::Bytes;

#[tokio::main]
async fn main() {
    let mut transport = axon_net::Config::new("a", SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true;
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, MemoryDiscovery::new());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let quotas = Quotas::start(&cluster, QuotaConfig::default()).await.unwrap();
    let tenant = NamespaceLimits {
        open_streams: Some(2),       // across the cluster
        records_per_sec: Some(100),  // across the cluster
        subscriptions: Some(1000),   // on each node
        ring_bytes: Some(64 << 20),  // on each node
        ..NamespaceLimits::default()
    };
    let cfg = StreamNodeConfig::new(vec![]).limit("tenant-a", tenant);
    let node = StreamNode::start(&cluster, StreamNodeConfig { quotas: Some(quotas), ..cfg }).await.unwrap();

    let key = |name| StreamKey::new("tenant-a", name);
    let replay = DeliveryPolicy::ReplayRequired;
    node.open_stream(&key("orders"), 1, replay).await.unwrap();
    node.open_stream(&key("fills"), 1, replay).await.unwrap();
    let Err(StreamError::QuotaExceeded { limit: NamespaceLimit::OpenStreams, retry_after, .. }) =
        node.open_stream(&key("quotes"), 1, replay).await
    else {
        panic!("a third stream of tenant-a was opened")
    };
    println!("tenant-a has its two streams open; try again after {retry_after:?}");

    let mut admitted = 0;
    let retry_after = loop {
        match node.publish(&key("orders"), vec![Bytes::from_static(b"order")]) {
            Ok(_) => admitted += 1,
            Err(refused) => match refused.error {
                StreamError::QuotaExceeded { limit: NamespaceLimit::Records, retry_after, .. } => break retry_after,
                other => panic!("{other:?}"),
            },
        }
    };
    println!("{admitted} records admitted, then refused; try again after {retry_after:?}");

    // A closed stream gives its permit back.
    node.close_stream(&key("fills")).unwrap();
    node.open_stream(&key("quotes"), 1, replay).await.unwrap();
    println!("refusals by limit: {:?}", node.stats().hub.quota_refused);
    drop(node);
    system.shutdown().await.unwrap();
}
```

- **A refusal names its limit.** `open_stream`, `publish` and `subscribe` at a limit return
  `StreamError::QuotaExceeded { namespace, limit, retry_after }`; a refused publish returns its records, as every
  refusal does. `retry_after` is Quotas' answer for a rate or open streams, and `None` for a node's own limits, which
  clear only as something closes. Each refusal is counted in `HubStats::quota_refused` by limit, a publish in
  `publish_rejected` too, and in telemetry as `stream.quota_refused` by `reason`.
- **Rates at the origin.** `records_per_sec` and `bytes_per_sec` are taken where the stream is published, keyed by the
  namespace: every origin of it shares the one rate, relays take nothing. They are taken last, once the batch passed
  every other check, so a refused publish takes none of the rate; a batch past a rate a second is `LimitExceeded`, to
  split, as one past `hub.max_batch_bytes` is. The rate
  holds within [spec/quotas.md §5](../../../spec/quotas.md): in steady state a saturated rate admits about 8/9 of it,
  a namespace new to the nodes can admit about `M + N` at first, and each side of a partition up to `M`. Under a
  fault a node's share can also run short of a rate the namespace barely uses, for a report cycle or so.
- **Open streams.** A stream holds one of the namespace's `open_streams` permits from `open_stream` until
  `close_stream`, or until its node goes. A node that crashes keeps its permits counted until its restart shows or the
  view shows it left, else for 15 to 16 s, so a producer moving to another node can be refused until then; its retry
  loop waits each `retry_after`. A restarted node new to the namespace may open one on the fallback before its
  first grant.
- **Subscriptions on each node.** `subscriptions` counts the namespace's subscriptions on the node, each until its end
  is read or the subscription is dropped. A downstream node's branch is not one: its subscribers are counted, and
  refused, on their own node.
- **Ring bytes on each node.** `ring_bytes` bounds the history the rings of the namespace's streams the node
  originates hold together, with the state of those that are compacted (the bytes `stream.retained` counts), on top of
  the per-stream ring bounds (`CoreConfig::rings`, which alone bound the rings it relays). A publish that would not fit
  once its own ring gives up its oldest records is refused: one stream never evicts another's history, a state never
  gives up a key to make room, and closing another of the namespace's streams on the node makes room.
- **Fail open, as Quotas does.** A node that cannot hear the replicas admits on its last grant or the fallback, never
  pauses. A node with no limits pays a lookup in an empty map per subscribe and per publish, which the origin row of
  `fanout` does not tell apart (Streams in [benchmarks](benchmarks.md)).

## A websocket gateway

axon has no websocket gateway of its own: the socket, its protocol and its requests belong to the application. What
axon gives it is one actor per socket, a stream subscription per thing the client follows, and `Quotas` for what an
account may hold and ask. The program below is the whole shape, with an in-memory pipe standing in for the socket.

```rust
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll, ready};
use std::time::Duration;

use axon::{
    Actor, ActorHandle, ActorSystem, Context, JobHandle, RejectReason, Sink, SinkEvent, SinkHandle, Stream,
    SystemError, TimerId,
};
use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery, Permit, Permits, QuotaConfig, Quotas, Rate};
use axon_net::SharedSecret;
use axon_stream::{
    Cursor, DeliveryPolicy, SequencedBatch, StartPosition, StreamEvent, StreamKey, StreamNode, StreamNodeConfig,
    SubscribeOptions,
};
use bytes::Bytes;
use tokio::sync::mpsc;

/// Sockets an account holds open at once, on every node together.
const SOCKETS: Permits = Permits::new("ws/sockets", 2);
/// Requests an account sends a second, over all its sockets.
const REQUESTS: Rate = Rate::new("ws/requests", 20, Duration::from_secs(1));
/// Frames the socket's writer holds before `try_send` says `Full`.
const IN_FLIGHT: usize = 64;
/// Frames waiting behind a full socket, past which the session takes no more records until the client reads.
const OUTBOX: usize = 256;
/// How long a client that stopped reading keeps its socket.
const PATIENCE: Duration = Duration::from_secs(5);

fn prices() -> StreamKey {
    StreamKey::new("prices", "fx")
}

/// An account and the symbols its plan may see.
struct Account {
    name: &'static str,
    symbols: &'static [&'static str],
}

/// Stands in for checking a signed token, or asking the service that issued it.
fn authenticate(token: &str) -> Option<Account> {
    match token {
        "alice-token" => Some(Account { name: "alice", symbols: &["EURUSD", "GBPUSD"] }),
        "bob-token" => Some(Account { name: "bob", symbols: &["EURUSD", "GBPUSD", "USDJPY"] }),
        _ => None,
    }
}

/// The socket's read half: frames from the client.
struct Frames(mpsc::Receiver<String>);

impl Stream for Frames {
    type Item = String;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<String>> {
        self.0.poll_recv(cx)
    }
}

type Room = Pin<Box<dyn Future<Output = Option<mpsc::OwnedPermit<String>>> + Send>>;

/// The socket's write half: it waits for the client to read, as a socket waits for its peer's window.
struct Wire {
    tx: mpsc::Sender<String>,
    room: Option<Room>,
    permit: Option<mpsc::OwnedPermit<String>>,
}

impl Sink<String> for Wire {
    type Error = std::io::Error;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        if self.permit.is_none() {
            let tx = self.tx.clone();
            let room = self.room.get_or_insert_with(|| Box::pin(async move { tx.reserve_owned().await.ok() }));
            let permit = ready!(room.as_mut().poll(cx));
            self.room = None;
            self.permit = Some(permit.ok_or(std::io::ErrorKind::BrokenPipe)?);
        }
        Poll::Ready(Ok(()))
    }
    fn start_send(mut self: Pin<&mut Self>, frame: String) -> Result<(), Self::Error> {
        self.permit.take().expect("poll_ready first").send(frame);
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
    Client(Option<String>),
    Socket(SinkEvent<std::io::Error>),
    /// Tagged with its subscription: a cancelled source's items still arrive, and a replaced one's are ignored.
    Prices(u32, Option<StreamEvent>),
    /// A pause's deadline, tagged the same way.
    Stalled(u32),
}

/// One per socket, and only for an authenticated one.
struct Session {
    account: Account,
    /// One of the account's `SOCKETS`, given back when the session ends, however it ends.
    _socket: Permit,
    quotas: Quotas,
    streams: StreamNode,
    halves: Option<(Frames, Wire)>,
    tx: Option<SinkHandle<String>>,
    outbox: VecDeque<String>,
    following: Option<JobHandle>,
    follows: u32,
    /// Where a subscription paused for a slow client resumes, and the deadline for it to read.
    paused: Option<(Cursor, TimerId)>,
}

struct Gateway {
    system: ActorSystem,
    quotas: Quotas,
    streams: StreamNode,
}

impl Gateway {
    /// The upgrade request: the token names the account before anything is counted, then one of the account's
    /// `SOCKETS` admits it. A refusal is the HTTP answer.
    fn admit(&self, token: &str) -> Result<(Account, Permit), (u16, String)> {
        let account = authenticate(token).ok_or((401, "unknown token".to_string()))?;
        let permit = self.quotas.acquire(account.name, &SOCKETS).map_err(|denied| (429, denied.to_string()))?;
        Ok((account, permit))
    }

    /// The upgraded socket: its session holds the permit until it ends.
    fn open(&self, admitted: (Account, Permit), halves: (Frames, Wire)) -> Result<ActorHandle<Msg>, SystemError> {
        let (account, _socket) = admitted;
        let session = Session {
            account,
            _socket,
            quotas: self.quotas.clone(),
            streams: self.streams.clone(),
            halves: Some(halves),
            tx: None,
            outbox: VecDeque::new(),
            following: None,
            follows: 0,
            paused: None,
        };
        self.system.spawn(session).start()
    }
}

impl Session {
    fn say(&mut self, frame: String, cx: &mut Context<'_, Self>) {
        if self.outbox.len() < 2 * OUTBOX {
            self.outbox.push_back(frame);
        } else {
            cx.stop(); // records stop at OUTBOX: this client asks without reading
        }
    }

    /// The application's own protocol: `follow`, `follow <generation>:<sequence>` and `echo <text>`.
    fn request(&mut self, request: &str, cx: &mut Context<'_, Self>) {
        if let Err(denied) = self.quotas.take(self.account.name, &[(REQUESTS, 1)]) {
            return self.say(format!("429 {denied}"), cx);
        }
        let reply = match request.split_once(' ').unwrap_or((request, "")) {
            ("echo", text) => text.to_string(),
            ("follow", after) => self.follow(after, cx).map_or_else(|e| format!("error {e}"), |()| "following".into()),
            _ => format!("error unknown request {request:?}"),
        };
        self.say(reply, cx);
    }

    fn follow(&mut self, after: &str, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let start = match after.split_once(':') {
            None => StartPosition::Live,
            Some((generation, sequence)) => {
                let (Ok(generation), Ok(sequence)) = (generation.parse(), sequence.parse()) else {
                    return Err(format!("bad cursor {after:?}"));
                };
                StartPosition::After(Cursor { generation, sequence })
            }
        };
        self.subscribe(start, cx)
    }

    /// Replaces the subscription, if any.
    fn subscribe(&mut self, start: StartPosition, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let options = SubscribeOptions { start, ..Default::default() };
        let subscription = self.streams.subscribe(&prices(), options).map_err(|e| format!("{e:?}"))?;
        self.stop_following(cx);
        let follows = self.follows;
        let job = cx.attach(subscription, move |event| Msg::Prices(follows, event)).map_err(|e| format!("{e:?}"))?;
        self.following = Some(job);
        Ok(())
    }

    fn stop_following(&mut self, cx: &mut Context<'_, Self>) {
        if let Some(job) = self.following.take() {
            job.cancel();
        }
        if let Some((_, stalled)) = self.paused.take() {
            cx.cancel_timer(stalled);
        }
        self.follows = self.follows.wrapping_add(1);
    }

    fn flush(&mut self) {
        let Some(tx) = &self.tx else { return };
        while let Some(frame) = self.outbox.pop_front() {
            if let Err(refused) = tx.try_send(frame) {
                if refused.reason == RejectReason::Full {
                    self.outbox.push_front(refused.message); // until SinkEvent::Ready
                }
                break;
            }
        }
    }

    fn deliver(&mut self, batch: &SequencedBatch, cx: &mut Context<'_, Self>) {
        let generation = batch.generation();
        let mut unseen = None; // the last record handled, when the filter dropped it
        for (sequence, record) in (batch.first()..).zip(batch.records()) {
            if self.outbox.len() >= OUTBOX {
                // The client is OUTBOX frames behind: keep its place, not its records, and give it PATIENCE to read.
                self.stop_following(cx);
                match cx.schedule_after(PATIENCE, Msg::Stalled(self.follows)) {
                    Ok(stalled) => self.paused = Some((Cursor { generation, sequence: sequence - 1 }, stalled)),
                    Err(_) => cx.stop(),
                }
                break;
            }
            // The client's filter: what its plan may see. Each frame carries its cursor.
            let symbol = record.split(|&b| b == b' ').next().unwrap_or_default();
            if self.account.symbols.iter().any(|s| s.as_bytes() == symbol) {
                self.outbox.push_back(format!("{generation}:{sequence} {}", String::from_utf8_lossy(record)));
                unseen = None;
            } else {
                unseen = Some(sequence);
            }
        }
        // What the filter drops moves the client's cursor too, or a narrow plan's cursor falls out of the rings.
        if let Some(sequence) = unseen {
            self.say(format!("at {generation}:{sequence}"), cx);
        }
    }
}

impl Actor for Session {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (frames, wire) = self.halves.take().ok_or("started twice")?;
        cx.attach(frames, Msg::Client).map_err(|e| format!("{e:?}"))?;
        self.tx = Some(cx.attach_sink(wire, IN_FLIGHT, Msg::Socket).map_err(|e| format!("{e:?}"))?);
        Ok(())
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Client(Some(request)) => self.request(&request, cx),
            Msg::Client(None) | Msg::Socket(SinkEvent::Closed(_)) => cx.stop(),
            Msg::Socket(SinkEvent::Ready) => {} // on_turn_end writes the outbox
            Msg::Prices(follows, _) | Msg::Stalled(follows) if follows != self.follows => {} // replaced since
            Msg::Prices(_, Some(StreamEvent::Batch(batch))) => self.deliver(&batch, cx),
            // Continuity broke (a restart, or no ring holds the cursor): the client follows again.
            Msg::Prices(_, Some(event)) if event.is_terminal() => self.say(format!("reset {event:?}"), cx),
            // Still paused after PATIENCE: dropped, it reconnects and resumes after its cursor.
            Msg::Stalled(_) => cx.stop(),
            Msg::Prices(..) => {} // Ready, CaughtUp, OriginLost, the end after a terminal event
        }
        Ok(())
    }

    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        self.flush();
        // Written after the flush: an outbox it empties brings no SinkEvent::Ready to resume on.
        if let Some((at, _)) = self.paused.filter(|_| self.outbox.len() <= OUTBOX / 2)
            && let Err(e) = self.subscribe(StartPosition::After(at), cx)
        {
            self.stop_following(cx);
            self.say(format!("reset {e}"), cx); // the rings turned over while it read
            self.flush();
        }
        Ok(())
    }
}

/// The client's end of the pipe.
struct Client(mpsc::Sender<String>, mpsc::Receiver<String>);

impl Client {
    async fn send(&self, frame: &str) {
        self.0.send(frame.into()).await.expect("socket open");
    }
    async fn next(&mut self) -> String {
        let frame = tokio::time::timeout(Duration::from_secs(10), self.1.recv()).await;
        frame.expect("a frame within 10 s").expect("socket open")
    }
}

fn socket() -> ((Frames, Wire), Client) {
    let (to_server, from_client) = mpsc::channel(16);
    let (to_client, from_server) = mpsc::channel(16);
    let wire = Wire { tx: to_client, room: None, permit: None };
    ((Frames(from_client), wire), Client(to_server, from_server))
}

#[tokio::main]
async fn main() {
    let mut transport = axon_net::Config::new("a", SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true;
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, MemoryDiscovery::new());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let quotas = Quotas::start(&cluster, QuotaConfig::default()).await.unwrap();
    let streams = StreamNode::start(&cluster, StreamNodeConfig::new(vec![])).await.unwrap();
    streams.open_stream(&prices(), 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    let publish = |records: &[&'static str]| {
        streams.publish(&prices(), records.iter().map(|r| Bytes::from(*r)).collect()).unwrap();
    };
    let gateway = Gateway { system: system.clone(), quotas, streams: streams.clone() };
    let connect = |token: &str| {
        let (halves, client) = socket();
        gateway.admit(token).map(|admitted| (gateway.open(admitted, halves).unwrap(), client))
    };

    assert_eq!(connect("forged").err().map(|(status, _)| status), Some(401));
    let (session, mut alice) = connect("alice-token").unwrap();
    alice.send("follow").await;
    assert_eq!(alice.next().await, "following");
    publish(&["EURUSD 1.0842", "GBPUSD 1.2710", "USDJPY 149.10"]);
    assert_eq!(alice.next().await, "1:1 EURUSD 1.0842");
    assert_eq!(alice.next().await, "1:2 GBPUSD 1.2710");
    assert_eq!(alice.next().await, "at 1:3"); // her plan has no USDJPY, and her cursor passes it

    let _second = connect("alice-token").unwrap();
    assert_eq!(connect("alice-token").err().map(|(status, _)| status), Some(429)); // a third socket of alice's

    // Alice's connection drops; her session ends and gives its permit back.
    drop(alice);
    session.join().await;
    publish(&["EURUSD 1.0845", "GBPUSD 1.2702"]);
    let (_session, mut alice) = connect("alice-token").unwrap();
    alice.send("follow 1:3").await; // the last cursor she was told
    assert_eq!(alice.next().await, "following");
    assert_eq!(alice.next().await, "1:4 EURUSD 1.0845");
    assert_eq!(alice.next().await, "1:5 GBPUSD 1.2702");
    system.shutdown().await.unwrap();
}
```

- **Authenticate first.** A session exists only for a token that named an account, so nothing is subscribed, counted
  or held for a client that is not one. Call `take` and `acquire` after it: every key new to a node is admitted its
  fallback share once ([rule 12](../README.md#rules-that-prevent-most-mistakes)), so unauthenticated keys would each
  be admitted.
- **Sockets per account.** `admit` takes one of the account's `SOCKETS` before the upgrade, so a refusal is an HTTP
  401 or 429, and `open` moves the `Permit` into the session: it goes back when the actor ends, whether the client
  closed, the socket failed or the client stopped reading. `Denied::Share` means this node's share is used though the
  account has room: answer 429 with its `retry_after`, as for `Denied::Limit`; the retry is admitted once the grant
  grows, about a report cycle later. The limit holds across every node of the gateway within
  [spec/quotas.md §5](../../../spec/quotas.md), and nothing admitted is taken back, so size it for two cases there. An
  account opening sockets on every node at once, before the replicas have heard of it, can hold up to about `M + N` (a
  limit `M`, `N` nodes), and each side of a partition admits up to `M`; those sockets stay open until they close. A
  gateway node that crashes keeps its sockets counted until its restart shows, or for 15 to 16 s, so its clients
  reconnecting elsewhere can be told 429 until then.
- **Requests per account.** Each request of the application's own protocol takes one of `REQUESTS` before it is
  handled, so an account's clients share one rate wherever they connect. A weighted request takes its weight, as in
  [cluster § Quotas](cluster.md#quotas).
- **The filter** runs in the session, per record, on what the account may see. It suits records that most clients see
  some of. Where each client sees a slice of its own, such as its account's orders, give each slice its own stream:
  one stream per topic, [above](#tree). Every frame carries its cursor, and a batch whose last records the filter
  dropped ends with `at <cursor>`: a client whose plan sees little of the stream would otherwise hold a cursor the
  rings have long dropped, and its resume would be refused.
- **A bounded outbox** ([rule 11](../README.md#rules-that-prevent-most-mistakes)). The socket's writer holds
  `IN_FLIGHT` frames and what it refuses waits in the outbox. At `OUTBOX` frames the session cancels the subscription
  and keeps only its place; once the client has read half, it subscribes `After` that place. A slow client then costs
  its node a cursor rather than records, and a catch-up of any length goes at the client's pace. One that has not read
  half its outbox within `PATIENCE` has stopped reading, or reads too slowly to keep up: it is dropped, and resumes
  after its cursor when it reconnects. Past `OUTBOX` only replies are added, at the request rate, so `2 * OUTBOX`
  frames is a client that asks without reading, and it is dropped too. A cancelled source's items already in the
  mailbox still arrive ([actors § Sources](actors.md#sources)), so each subscription's events carry its tag and a
  replaced one's are ignored.
  The chat example in the AR1011/axon-examples repository pauses its rooms the same way.
- **Resume.** `follow <generation>:<sequence>` subscribes `After` the last cursor the client was told, a frame's or an
  `at`, on whichever node it reaches; `follow` on the same socket replaces what it followed. A cursor the rings no
  longer hold is refused (`error` with `HistoryUnavailable`), the socket keeps what it followed, and the client sends
  `follow` to follow live. A `reset` says continuity broke: the stream restarted as a new generation, or the rings
  turned over while the client was paused; it follows again.

With a real socket, only the halves change. Under axum the upgrade handler calls `admit` before it upgrades, so a
refused client gets its 401 or 429 as an HTTP answer, and `open` takes the two halves of the `WebSocket` in place of
`Frames` and `Wire` (in the AR1011/axon-examples repository, the chat example's `node.rs` for the handler, and
the market-data example's `ws.rs` for tokio-tungstenite):

```rust,ignore
async fn upgrade(ws: WebSocketUpgrade, Query(q): Query<Token>, State(gateway): State<Arc<Gateway>>) -> Response {
    let admitted = match gateway.admit(&q.token) {
        Ok(admitted) => admitted,
        Err((status, why)) => return (StatusCode::from_u16(status).unwrap(), why).into_response(),
    };
    ws.on_upgrade(move |socket| async move {
        let (write, read) = socket.split(); // Session's halves: (read, write) in place of (Frames, Wire)
        let _ = gateway.open(admitted, (read, write)); // refused only at the actor limit; the socket closes with it
    })
}
```

## Tests and telemetry

The test files of each area are listed in the feature map of [AGENTS.md](../../../AGENTS.md#feature-map), and what each
proves against which fault in [spec/faults.md](../../../spec/faults.md). `crates/axon-stream/examples/fanout.rs`
measures throughput; the figures are in [benchmarks](benchmarks.md).

Telemetry is in the [catalog](telemetry.md) (`stream.*`): per node, never per stream. A parent also reports, per child,
the branches it feeds, what is queued for it and the payload pushed to it, which costs one entry per node pair. Anything
that grows with the number of streams is asked for, never sampled: `StreamNode::detail` (telemetry's `Streams` request,
the dashboard's `api/streams`) lists each stream's position, parent, path from the origin, refused parents and listeners, and the last 256 stream events of the
node: parent chosen, parent lost, parent restored, switched, refused, reset, orphaned, downstream expired.

The dashboard's Streams page draws the tree: origins on top, each node under the parents it pulls from, one edge per
parent and child labelled with the payload rate, dashed while a child is catching up on a better parent, red and dotted when
more than 1 MiB is queued for the child. A node without a parent is outlined and a node that reset subscriptions is marked.
The side panel has the node's trend lines, its streams with how far each is behind the furthest node, and the event
feed. With more than one leaf the header compares what the origins send with what direct pulls would cost them.
