# Design — `PaxosLeases`

An `OwnershipAuthority` that needs no external service. A few ordinary nodes are acceptors, each
keeping a small log of who holds which key; every node can take a lease on a key by asking them. The protocol is PaxosLease
(Trencseni, Gazsó, Reinhardt, 2008): single-decree Paxos per key, where the accepted value is a
lease that expires. Status: built (`crates/axon-cluster/src/paxos/`), model-checked (`spec/tla/`).

## 1. Scope

In scope:

- Strict single ownership of keyed actors (CL-10, CL-12): at most one node holds a key at any
  moment, and each grant of a key carries a larger fencing epoch than the one before.
- 1, 3 or 5 acceptors, any number of other nodes, up to about 100 k live leases.
- Acceptors restarting, one at a time in a rolling deploy or all at once, with only who holds each
  key kept on disk (§4.1): promises are not, so a restart needs no disk to stay safe, only to say
  which node held which key.

Out of scope:

- A replicated log, configuration store or anything but leases.
- Changing the acceptor list while running (§10).
- Discovery. Any provider works beside this one.

No new dependency, so no feature flag. Changes to the `OwnershipAuthority` trait (§5) and two
fixes to the cluster's renew loop (§5.1, §5.2).

## 2. How it works

| Term | Meaning |
|---|---|
| Acceptor | A node listed in `PaxosConfig::acceptors`. It answers lease requests on an endpoint of the cluster. Same process, no extra port. |
| Majority | More than half of the listed acceptors. |
| `T` | The lease time, `PaxosConfig::lease` (6 s). An acceptor frees a key `T` after it last accepted it. |
| Ballot | `(epoch, node)`. An acceptor takes a ballot only with a larger epoch than it promised, or the very ballot it promised. So no two nodes win with one epoch, and `epoch`, a `u64`, is the fencing epoch the actor gets. |
| Holder | The node whose proposal a majority accepted, until its own timer runs out. |

Acquire, for a key this node does not hold:

1. **Prepare.** Pick `epoch = max(now in µs since UNIX_EPOCH, highest epoch seen + 1)`. Send
   `Prepare(key, ballot)` to every acceptor. An acceptor that promised the key an epoch at or
   above this one, to any other ballot, refuses and says its promise. Otherwise it promises and
   answers with the lease it has accepted for the key, if that lease has not run out on its own
   clock. When that lease is another node's, it answers without recording the promise: the
   proposer gives up on it (step 4), and a recorded promise would refuse the holder's renewals.
2. If a majority promised and none of them reports a live lease of another node: **Propose.**
   Note the local time `sent`, then send `Propose(key, ballot, holder, T)`. An acceptor accepts
   it by the same rule as a prepare, and starts its own timer `T`.
3. On a majority of accepts, the node holds the key until `sent + T·(1 − drift) − margin` by its
   own clock. `epoch` is the fencing epoch.
4. A live lease of another node in any promise: `Held`. No majority: `Unavailable`, with why
   when the acceptors said (full, warming) or did not answer within the patience, alive or not. A
   refusal with a higher promise: try again once with a larger epoch, then `Unavailable`.

The holder's timer starts before any acceptor's, so it runs out first. That is the whole safety
argument, and it needs only a bound on how fast clocks drift apart, not synchronised clocks.

Renew is step 2 again with the same ballot, for every key this node holds, as one batched
request per acceptor. One round trip. A key whose ballot was overtaken (someone prepared it,
saw the live lease and gave up) is renewed through steps 1–3, where the node's own live lease
does not count as held.

Release sends `Release(key, ballot)`; an acceptor drops the accepted lease if the ballot matches.
A lost release costs `T`.

### 2.1 What follows from "more than half"

| Event | Result |
|---|---|
| One of 3 acceptors crashes or restarts | Nothing visible. Acquire and renew get their majority from the other two. |
| 2 of 3 acceptors are down | No acquire succeeds and no renew succeeds. Every strictly owned actor stops `lease_grace` after its last renew. Nothing runs twice. |
| A partition | The side with a majority of acceptors can take keys once the other side's leases ran out. The other side stops its keyed actors. |
| A holder crashes | Its keys are free `T` after its last renew. No lock delay is needed on top. |
| 1 acceptor | It is the arbiter. Fine for development and 2-node clusters; if it is down nothing can be owned. |
| 2 or 4 acceptors | Allowed, never better, as with seeds. |

### 2.2 Restarts

An acceptor that starts refuses every request as `Warming` for `T·(1+drift) + margin`, long
enough on its own clock however fast that clock runs. Whatever it had accepted before has run out
by then, so forgetting its promises is safe. Its promise floor is its clock in µs when the wait
ends, or the highest epoch its log holds granted if that is higher (§2.3), so a ballot from before
the restart cannot win, however late its messages arrive.

What it had accepted comes back from its log (§4.1), run out when the wait ends: not to decide
anything, but so that `held_by` still lists the keys each node held and did not release, and a
release by that ballot still clears one. Each such key promises only the floor: its own ballot,
taken back as a promise, would admit that ballot's late propose.

A rolling deploy restarts acceptors one at a time and waits until each answers again (the
provider card shows `warming` per acceptor, §8, and `PaxosLeases::warming` answers it on the
acceptor's node) before stopping the next. A replaced machine that
keeps the node id, and its log's directory, is the same as a restart. When every acceptor starts at
once, as on a cold start of the whole cluster, nothing can be owned until the wait is over; then the
eager keys every node held run again (§4.1). A node's questions ask until the acceptors first answer,
however long their wait, less often up to once a second. A key listed waits for its restart once per
node, however many questions listed it, for at most `EAGER_PATIENCE` (a minute) and `T` from its first
try, kept at while a holder this node cannot reach still holds it; the sweep of lapsed leases leaves a
key refused `Held` to its holder. The sweep asks again every 20 s for a minute and `T` past its first
answer. A question about a gone node ends unanswered after 30 s and arms the sweep instead, and one about
the node itself after a minute and `T`, past the acceptors' wait, since the sweep starts only the keys a
node owns; the sweep asks until the acceptors answer, one request at a time, and the cluster's close ends
it.
The acceptors list what they restored for a minute after their wait, or for `T` if that is longer, so a
node that first comes up after that window finds its keys listed no more: they wait for a caller.

### 2.3 Epochs

Exclusivity never depends on clock values, only on drift. Epochs grow strictly per key, whatever
the clocks do. An acceptor that kept running carries the promise and needs no clock at all. One
that restarts starts its floor at the highest epoch it granted, read from its log (§4.1), or at
its clock when the wait ends if that is higher. A grant needs a majority, which shares an
acceptor with the majority of the grant before it; that acceptor logged the earlier epoch before
it answered, so it refuses every epoch up to it, from any clock.

One gap is left: while an acceptor's log writes fail or hang it answers without waiting for them
(§4.1), so a grant can reach no disk. If every acceptor of that grant's majority then restarts
with its clock more than `T` behind the epoch, the floor is the clock and a later epoch of the key
can be smaller. The provider reports a clock behind (§8: `clock_behind`) when a prepare carries
an epoch far ahead of the acceptor's clock, and the log failing (`store failing`).

Epochs order grants, not starts. A node that still holds a key's lease when it starts the key's
actor again starts it under the same epoch, and a kind's restart policy rebuilds the actor under
the lease it holds. `SpawnContext::start` numbers each start in a node's process, so `(epoch, start)`
grows with each start of a key: a restarted node counts from 1 again, under a new lease of a larger
epoch, since its process proposes under a name of its own, the node and its incarnation (§4).

### 2.4 Timing against the renew loop

| Moment after a holder is cut off from every acceptor | What happens |
|---|---|
| 0–2 s | The next renew is due. |
| 5 s | `lease_grace` after the last renew was *sent*: the holder stops its keyed actors (§5.1), each on its own deadline (§5.2). |
| 6 s | `T` after the last renew reached the acceptors: they free its keys. |
| 6 s + one acquire | The new owner starts the key. |

`Cluster::start` refuses a `lease_grace` above what the holder believes, `T·(1 − drift) − margin`,
so the actors stop before the lease can run out anywhere (`a_grace_the_lease_does_not_cover_is_refused`).
Unset, `lease_grace` is 7/8 of that and `lease_renew_interval` 2/5 of the grace, so the timings follow `T`
and a short lease needs no other setting (`a_short_lease_needs_no_other_setting`). Defaults: `T = 6 s`,
`drift = 1 %`, `margin = 200 ms`, hence `lease_grace = 5.02 s` and `lease_renew_interval = 2.01 s`.

## 3. API

```rust
pub struct PaxosConfig {
    /// Node ids of the acceptors, the same list on every node. 1 to 7 entries.
    pub acceptors: Vec<String>,
    /// How long an acceptor keeps a lease it accepted. 6 s.
    pub lease: Duration,
    /// Upper bound on how fast two clocks drift apart. 1 %.
    pub drift: f64,
    /// Most keys one acceptor tracks: live leases, those its log restored, lapsed ones it still lists
    /// and recent promises. A request about another key is answered `Full`. 100 k.
    pub max_leases: usize,
    /// Where an acceptor logs who holds each key (§4.1). Required on a node on `acceptors`, a
    /// directory of its own kept across restarts; not read on the others.
    pub store: Option<Arc<dyn Files>>,
}

PaxosConfig::new(acceptors)
PaxosLeases::new(cfg) -> Result<PaxosLeases, OwnershipError>   // 1–7 acceptors, drift < 10 %
```

`Cluster::start` refuses an acceptor without a store (`ClusterError::Config`; `PaxosLeases::new`
does not know the node's id), and one whose log it cannot read (`ClusterError::Ownership`).
`Files` is `axon::files::Files`, as a stream origin's history uses. One store serves one
`PaxosLeases`. An `OsFiles` locks its directory from its first list until it is dropped, and a failed
start leaves it in its `PaxosLeases`, whose writer holds it too until its last write lands: a retry
with a new `PaxosLeases` and `OsFiles` is refused as in use until both have let go, and every retry is
while the caller keeps the failed one. The lock is per object: two `PaxosLeases` given one `Arc<OsFiles>`
are not refused, and write over each other's log. A write that hangs keeps the writer task and the lock
for as long as it hangs (§4.1).

What it counts is in `describe()` (§8), like every provider. `margin` is a constant, 200 ms, and a
round waits `T/6` for answers.

Wiring, the same on every node:

```rust
let mut cfg = ClusterConfig::new(bind, transport, discovery);
let mut paxos = PaxosConfig::new(["seed-a", "seed-b", "seed-c"]);
paxos.store = Some(Arc::new(OsFiles::new(data_dir.join("leases"))));
cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos)?));
let cluster = Cluster::start(&system, cfg).await?;
```

A node whose id is in the list serves as an acceptor. Putting the acceptors on the seed nodes
gives one rule for deploys.

## 4. Protocol

An endpoint of its own, `Cluster::serve("axon.lease", …)` with schema `3` and a protocol id hashed
from the sorted acceptor list, `lease` and `drift`. `Cluster::request` refuses a route with another
id (`ProtocolMismatch`) and sends nothing, so a node configured differently gets no votes: with
another list its majority need not share an acceptor with the others', and with a longer lease it
believes past a restarted acceptor's wait. A list naming one acceptor twice is refused at start. A
node whose own list is a majority without the others', such as one listing only itself, needs no
votes from them and is not caught (§10). The endpoint is answered on the session's read path: an
acceptor's answer is a few map lookups, so it needs neither the control RPC's task per request nor
its queue. Only cluster members hold a session, so only they reach it. The proposer asks with
`Cluster::request`, each call bounded by the round's patience, and answers its own node's acceptor
in place. Requests reach nodes in the view, which the acceptors are once discovery lists them;
leases are needed only after `Cluster::start`, so the ordering holds.

One request carries the proposer's name once and up to 512 items, each `Prepare`, `Propose` or
`Release`; the reply carries one answer per item: `Promised(the process holding a live lease, if another
node's)`, `Accepted`, `Refused(promise)`, `Warming`, `Full`. An acquire that finds another node's lease is
refused `Held(Some(node))`, naming that node without its incarnation, and a renewal that finds one is
`Lost`; `get_or_spawn` asks the named node instead of waiting at the owner (§5.3). `Full` answers one
item, not the request: the acceptor tracks `max_leases` keys, or the propose would grant while its log
is behind (§4.1).

A proposer keeps one request in flight to one acceptor in each of two lanes; what it asks meanwhile
queues in its lane and leaves as one request of up to 512 items or 64 KiB. Prepares, and renewals of a
ballot the acceptor was heard accepting, go in the quick lane: that acceptor logged the grant before it
accepted, so their answers wait for no write. Proposes that may grant, releases, and renewals to the
other acceptors go in the logged lane. So a renewal waits for no other key's write, and a slow fsync
holds up grants only. A key's release and renewal can be in different lanes, and the two must land
in the order they were asked: a renewal that landed after the release would grant the key again,
held by nobody until it runs out. So a queued item of a round the key has moved on from is dropped
rather than sent, and a release waits in its lane, with what is queued behind it there, while a quick
request carrying an item of its key is out (`a_release_never_overtakes_its_keys_renewal`). A quick request that timed out can still land
after the release; that lease then runs out unrenewed, as before there were lanes. Four in flight per acceptor
placed a quarter fewer keys a second on the same cores
([benchmarks](../crates/axon/guide/benchmarks.md#scale-axon-obs)). So a burst of 2000 acquires from one node is a
few dozen requests, all granted (`a_burst_of_paxos_acquires_is_granted`; one request per key had
1982 of them time out or find the engine `Busy`). A round's own deadline allows for that wait: two
phases, each at most one request behind another. An answer later than the patience counts as none,
however alive the acceptor: on two starved cores a request of 512 keys took up to 400 ms against the
167 ms patience of a 1 s lease, and its keys are refused `acceptors did not answer`, `Retry::Later`,
and granted when asked again. Encoded by hand, pinned bytes in `paxos/core/tests.rs`; the route's
schema versions them, so bodies carry none.

Acceptor state, per key with a live promise or lease: `promise: Ballot`,
`accepted: Option<(Ballot, holder, expires)>`. An entry whose lease and promise are both older
than `T` is dropped, and its promise raises a single per-acceptor floor; a lease that ran out without
a release is kept a minute past its end first. Memory is bounded by live leases and those lapsed in
the last minute, not by keys ever seen.

`held_by(Holder::Node(node), kinds)` asks every acceptor, on a second endpoint `"axon.lease.held"` with
the same id and a schema of its own (4; the lease route's is 3), for the keys of those kinds whose
accepted lease went to a process of `node` and was not
released, 64 KiB of keys a page, and takes the union once a majority answered; a warming acceptor
does not count. A lease a majority accepted is at one of any majority, so no key the node held is
missed; a key listed that it never held is only an acquire that fails. That is what an eager kind
restarts once the node is gone, which is why a lapsed lease stays listed a minute after its end:
survivors see a silent node gone after the transport's idle timeout, however short the lease.
`Holder::Lapsed` lists every lease that has run out and that nobody released, whoever held it; the
ask is tagged, `0 | node` or `1`, then the page's first key and the kinds. A node asks it each time
its eager kinds change, and when a question about a gone node goes unanswered for 30 s, for the keys
of a node none of its peers saw go, as when the whole cluster restarted at once: until the acceptors
first answer (§2.2), then every 20 s for a minute and `T`, so an unseen holder that ends within that
window, its lease not yet run out at the first answer, is listed by a later one. A key it lists that
another node took meanwhile is refused `Held` and left: if that node goes later, unseen by any peer,
the key waits for a caller. An acceptor of an older build serves the question only at schema 3 and
refuses this one, which counts as no answer: a majority of older acceptors is no answer, not an empty
list. Iterated maps are `BTreeMap`s, so the simulation replays.

### 4.1 The lease log

A lease a majority accepted is at one of any majority only while those acceptors remember it; a
restart of every acceptor at once forgot every holder, so eager keys whose node had also gone never
ran again until a caller asked. Each acceptor therefore logs who holds each key in its store, and a
restart reads it back (§2.2). Promises are not logged: the floor keeps a restart safe without them.
The highest epoch granted is: a restart starts its floor there, so epochs grow across restarts
whatever the clocks did (§2.3).

One file, `leases.log`, of frames `len u32 | crc u32 | body`, big-endian: a header (`AXLS`, version
2, the protocol id of §4), then a frame per change of a key's holder (`GRANT`, key, epoch and holder),
per release (`RELEASE`, key and epoch) and per lapsed lease the sweep forgets a minute after its end
(`FORGET`, key). Renewals write nothing, so the log grows with grants and releases, not with time.
Reading stops at a torn frame or a bad CRC. A rewrite replaces the file whole (`FileOp::Replace`)
with the header, one `GRANT` per lease and `TOP` (the highest epoch the acceptor granted, here or in
the log it read at its start), so the epochs of leases released or forgotten still count; the
highest epoch a log holds is the highest of its `GRANT`s and `TOP`s. `TOP` came after version 2 and
kept it: a build without it reads a rewrite's leases and stops at `TOP`, as at a torn frame, so it
misses what was appended after. A log of another acceptor list, `lease` or `drift` (another
protocol id), as in a directory reused from another configuration, lists nothing and is replaced by
the start's rewrite: changing any of them starts the log afresh, and an acceptor restarted with the
change lists nothing it held before. Its highest epoch still raises the floor, which only refuses
more, so epochs grow across the change too. A log of another version is read as empty. Version 1 kept numbered files,
`leases-<n>.log`: they are not read, so an acceptor upgraded from it lists nothing it held before
either, and they are left where they are. A `leases.log` that cannot be read, or is a directory,
fails `Cluster::start`, naming it.

| Rule | Why |
|---|---|
| An answer about a key waits until the key's last grant or release is synced, and the request's other answers with it; a prepare's does not | A grant answered and then lost in a crash is a lease a majority accepted that no restarted majority lists. A release is synced too, so a restart does not list a key its node gave up. A prepare changes nothing logged. Renewals to an acceptor that accepted their ballot go in a lane of their own (§4), so they share no request with an unsynced key and wait for nothing |
| One write at a time, every frame since the last in it | Grants answered while a write is out share the next fsync. The holder's timer starts before it sends the propose, so a slow fsync only shortens what it holds, never lengthens it past the acceptors' |
| Rewritten on start, after a failed write, and once it holds 1 MiB and four times what its leases take rewritten (the last rewrite's bytes per lease, or the first grant's frame before one held any) | Bounded by the live and recently lapsed leases: a rewrite comes when three quarters of the file is dead, released, forgotten or granted again, so a log of grants alone is appended to and never rewritten. A rewrite is one `Replace`, so an append never follows bytes a failed write left |
| While a write fails, or the store panics in it, answers do not wait, the stat `store failing` is true, and it is tried again every second | Grants go on as without a log; what a restart would list, and the floor it would start from, is what the last write that landed held |
| A write not landed within a round's patience (`T/6`) counts as failing until it lands, and the answers waiting for it are sent | Held answers are bounded: past the patience the proposer has given up on them, and each held one keeps a reply permit of the peer's session. The writer task, and an `OsFiles`'s directory lock, stay held while the write hangs, and the next write waits for it |
| A propose that would grant a key is answered `Full` while 4 MiB of frames wait for a write (`store behind`) | The log stays a bounded distance behind; renewals, prepares, releases and the request's other items still go through |

**What a crash leaves.** Number the frames an acceptor logs, in order. A frame is logged at each
change of a slot's lease and only then, so the leases after frame n are those its slots held when it
logged it; before the first, those it read back at its start. A write that lands syncs every frame
through the last it holds. The claim: after a crash at any point, the file reads back the leases after
the last frame synced, or after a later one. An answer waits until its key's frame is synced, so every
grant and release answered reads back, unless a later change of the same key took its place.

- Between writes, the file is the header and the frames through the last synced, whole and synced.
  It holds after a rewrite that landed (the header and the leases at its frame) and after an append
  that landed to such a file. An append is handed out only after a write that landed: after a start,
  a failed write or one that panicked, the next write is a rewrite.
- Inside an append, the file is that and some of the appended bytes, in part or out of order where a
  machine crash keeps some unsynced pages and not others. Reading stops at the first frame torn or
  failing its CRC, so it reads the synced frames and then the appended ones up to that frame: the
  synced leases, or those after a later frame.
- Inside a rewrite, a `Replace` leaves the old file or the new one, each whole. The old one reads as
  between writes; the new one is the leases when the rewrite was handed out, after a frame no earlier
  than the last synced, as answers wait only for frames synced.
- After a write that failed, the store may have kept any part of it. Answers no longer wait while
  the log fails, and the next write is a rewrite. Until it lands, the file reads as inside that write.

A rewrite written as a removal and an append, or into the file in place, would leave a log cut inside
the leases after a crash between the two or inside the append: answered grants that no restart lists.
The simulation crashes acceptors inside their rewrites and fails on such a log (§9). `PaxosLeases.tla`
models the log as one value per acceptor that a step replaces whole, and that is what the `Replace`
keeps true.

A restarted acceptor lists what it restored for a minute after its wait, even a key another node
took while it was down: it refuses that node's renewals, below its floor. So a node that is an
acceptor too keeps at the eager keys it held before its restart, refused `Held`, for up to the
minute and `T` every eager restart takes, at most `max_pending / 4` (up to 256) at once. A restart of
one acceptor costs the other nodes' lapsed sweeps nothing, since only that node's is armed; but the
restarted node's own lapsed sweep lists every lease the acceptor restored, run out though their holders
renew them at the others, and tries each it owns at each of its asks, every 20 s for a minute and `T`:
one acquire per key and ask, each refused `Held` at its prepare, paced as above.

The rule in the first row is what `AnswerBeforeDisk` in the model weakens, and the log is what
`Durable` adds (§9).

Code layout, following `seed/`:

```text
crates/axon-cluster/src/paxos/
  core.rs   Acceptor and Proposer state machines and the log's records: every call takes `now`, writes are handed out as FileOps; only `Stored::load` reads, through `Files::list` and `read`
  mod.rs    PaxosLeases: the endpoint, requests, the log's writer, OwnershipAuthority
  sim.rs    seeded simulation over the cores (#[cfg(test)])
```

`scripts/check.sh` adds `paxos/core.rs` to the files that may not name `Instant::now`,
`SystemTime::now` or `tokio`.

## 5. Changes outside the provider

```rust
pub trait OwnershipAuthority {
    // new, default no-op: awaited once by Cluster::start, before any acquire; the acceptor reads its log here
    fn attach<'a>(&'a self, _cluster: &'a Cluster) -> BoxFuture<'a, Result<(), ClusterError>> { Box::pin(async { Ok(()) }) }
    // new, default None: how long a holder acts after a renew that succeeded was sent
    fn hold(&self) -> Option<Duration> { None }
    // changed: the keys not extended, each with why (§5.2)
    fn renew(&self) -> BoxFuture<'_, Result<Vec<(String, OwnershipError)>, OwnershipError>>;
    // new, default true: whether a grant could reach this node now, without asking (§5.3)
    fn can_grant(&self) -> bool { true }
    ...
}
```

`PaxosLeases` keeps a `WeakCluster` (`Cluster::downgrade`), so the cluster and the authority do not
keep each other alive, and registers the acceptor endpoint there. Its `hold` is what a holder
believes after a renew; `Cluster::start` refuses a `lease_grace` beyond it and derives both timings
from it when they are unset (§2.4).

### 5.1 The renew loop measured from the reply

`renew_loop` (`crates/axon-cluster/src/spawn.rs`) recorded `renewed` when a renew *returned*, and
decided a lease was lost only when a later renew returned or timed out. After the last renew the
authority saw, the holder could keep running for the reply's delay (up to `operation_timeout`,
5 s), plus `lease_grace` (5 s), plus up to one `lease_renew_interval` (2 s): about 12 s. Consul
hides this, because its TTL is at least 10 s and up to twice that before `lock_delay` (5 s) even
starts. A lease without a lock delay does not.

Fixed for every authority: the loop takes the time before sending a renew, and stops the actors at
`sent + lease_grace` on a timer, whether or not the renew has returned.
`a_renewal_that_hangs_stops_strictly_owned_actors_when_the_grace_is_spent` in `tests/cluster/leases.rs`
failed before the fix (the actor ran 5.03 s past a 400 ms grace) and passes after it.

### 5.2 One key fails alone

`renew` answers `Ok` with the keys it could not extend, each with why; an error is the answer for
every key. The cluster keeps a fence and a deadline per lease, from its acquire to its release: a key
answered `Lost` stops at once, and any other key stops `lease_grace` after the last renew that
extended it was sent. Paxos decides every key on its own, so this is exact for it; Consul holds
every key under one session and answers for all of them. A key answered `Lost` is named by every
renewal until it is released, so a second caller of `renew` cannot take the news from the cluster.
The cluster counts a key `renew` did not name as extended from when it called, so a proposer refuses
a `renew` while an earlier one is out: rounds sent before the call cannot vouch for it.
A proposer answers `acquire` of a key it still holds from that hold, without a round; the cluster
therefore keeps the deadline it has for that epoch, and a key it gave up is not started again under
the epoch it lost until the actor that had it released it. A grant whose `acquire` was dropped before
it answered, its start's budget spent, is released as it arrives: the cluster keeps no deadline for
it and nothing renews it, so an `acquire` answered from it would open a fence of `lease_grace` from
that call, past the acceptors' lease (`a_grant_that_arrives_after_its_start_gave_up_leaves_nothing_held`
in `tests/world_leases.rs` failed on every seed before the fix). Before, one key taken over, or one whose
renewal found no majority during a burst, stopped every strictly owned actor on the node
(`a_key_taken_over_is_the_only_one_a_renewal_reports` in `paxos/mod.rs` failed before the fix;
`one_key_whose_renewal_fails_stops_alone` in `tests/cluster/leases.rs`).

### 5.3 Placement under partial cuts

A cut that is not a clean split leaves nodes that reach their callers but not a majority of the
acceptors, and callers that reach the acceptors but not the key's owner. A caller that asked only
the owner would wait out `operation_timeout` on acquires that cannot succeed there, or be refused
`Held` with no way to find the holder, for as long as the cut lasts. Three rules serve the key from
a node that can, each with a test that fails without it (`tests/world_reach.rs`):

- `OwnershipAuthority::can_grant`, answered from what the node knows without asking: for `PaxosLeases`,
  more than half the acceptors are this node, or reachable in its view and not failing. A failure
  counts until a request is answered: one older than the round's patience is asked again, so the link
  recovers on a node with nothing to renew. Counting it as up instead let a cut-off owner say yes to
  a move and lose the key (`reach_minority`). A node that cannot grant refuses a keyed start at once,
  `SpawnRefusal::Isolated`, as it does while it cannot confirm it is a member.
- The caller walks the rendezvous order of its own view (`View::owner_past`): an owner that is
  unreachable, leaving or `Isolated` is passed and the next asked, three nodes in all, the request naming the
  nodes passed (`SpawnRequest::passed`, wire version 3) so the node asked takes a key it does not own. A
  refusal any node would give (acceptors warming up, `Ownership`) ends the walk with the owner's answer.
  A `Held` refusal names the holder, and the caller asks it if it reaches it; otherwise it walks again
  every `HELD_RETRY` until `operation_timeout`, which also ends a walk past nodes that timed out. A
  round is at most four requests.
- A key moves to its owner only once the owner says it takes keys (`axon.owns`: open, not isolated and
  `can_grant`, looked at again `HELD_RETRY` after a no, by when a healthy link asked again has
  answered). Each rebalance pass asks every owner it would move keys to once, moves the keys of those
  that answer yes, and asks the others again `operation_timeout` later. Without the probe the key went
  to an owner that reached no majority, stopped there and started past it again at the next call.

A holder that can grant keeps the key whoever cannot reach it: with only the link from the callers to
the owner cut, the node that took the key past the owner reaches it and moves the key back, and those
callers are refused `Held` until the cut heals (outside `KeyServedReachable`, §9). The lease, not any of
these checks, keeps a key on one node: two nodes that each think they may take a key still race for one
lease. The `RemoteRef` a call returned never retargets (rule 7); a key moved back shows
`Retry::Resolve` to it, as any move does.

## 6. Security

The acceptor service admits cluster members only, over the cluster's authenticated sessions.
Any member can take any key; that is the same trust `get_or_spawn` already gives members. A
request is capped at 512 items and the session's frame size; an acceptor over `max_leases`
refuses new keys and keeps serving the ones it has.

## 7. Choosing an authority

For `crates/axon/guide/cluster.md`, next to the discovery table.

| | `MemoryLeases` | `ConsulLeases` | `PaxosLeases` |
|---|---|---|---|
| Needs | one process | a Consul cluster | 1, 3 or 5 acceptors among your nodes |
| Disk | none | Consul's | a small log per acceptor, synced per grant and release |
| Key free after its holder crashes | at once | TTL to 2×TTL, then `lock_delay`: 15–25 s by default | `T`: 6 s |
| Keeps working with | n/a | Consul's quorum | a majority of acceptors |
| Acquire | a lock | 2 HTTP round trips through the agent | 2 round trips to the acceptors and an fsync shared by every grant written with it |
| Use when | tests, one process | you already run Consul | you do not want to run Consul, or 15–25 s takeover is too slow |

## 8. Console

`describe()` gives kind `paxos`; settings `acceptors`, `lease`, `drift`; stats `held`,
`acquired`, `held by other`, `lost`, and on an acceptor `leases`, `stored` (leases its log holds,
lapsed ones included), `floor`, `warming`, `refused`, `clock behind`, `store failing`, `store
behind`; one link per acceptor with state `idle`, `answering` or `failing` and the last error. A warming acceptor answers, so its link is `answering`; its own card says `warming`. The
provider card already renders `ProviderInfo`, so no UI change is expected; this is checked on a
`scripts/ctl.sh` run.

## 9. Tests

**Model.** [tla/PaxosLeases.tla](tla/PaxosLeases.tla), checked with TLC: 3 acceptors, 2 proposers,
one key (keys are independent), message loss, duplication and reordering, an acceptor crash and
restart, time in ticks with drift at its worst case. Invariants: at no instant do two nodes
believe they hold the key (`MutualExclusion`); every grant carries a larger epoch than every one
before it (`EpochsGrow`). `PaxosLeases.cfg` and `Skew.cfg` must pass; their state spaces are
too large to finish on one machine, and they are checked as far as time allows (the last run's
depth and state count are in `AGENTS.md`). Each unprefixed file in `tla/mutations/` weakens one rule and must fail: the holder's timer started at the reply, a
restarted acceptor answering at once, its floor taken at the crash instead of after the wait, a
holder believing longer than acceptors keep the lease, clocks further apart than the restart wait
without the log's floor, the log's floor from an accept answered before it was logged, and equal
epochs ordered by node. Two of them are how the model found flaws in the first design.
Ordering ballots by `(epoch, node)`, the usual Paxos rule, lets two nodes win the same key with the
same epoch one after the other. And a floor taken at the crash lets a prepare sent before the
crash, arriving after the wait, win an epoch already granted. `SkewAtWait.cfg` is the control for
`mutations/SkewBeyondWait.cfg`: the same bounds with clocks as far apart as the wait, and it passes.
Both allow one prepare per proposer, because in the model an epoch and a tick share a unit: a prepare
retried one above a refusal runs its epoch a tick ahead of its clock, the whole wait at these
bounds. In the code a retry adds a microsecond, well inside the margin.

`LogFloor` starts a restarted acceptor's floor at the highest epoch its log granted (§2.3), kept as
`top`, which a release does not clear. Every config sets it but `mutations/FloorAtCrash.cfg` and
`mutations/SkewBeyondWait.cfg`, which show what the clock's floor alone allows (`FloorAtCrash.cfg`
with `LogFloor` fails too, at depth 23 in 5 min, so the log's floor does not replace the wait). `SkewLogFloor.cfg`
is the bounds of `SkewBeyondWait.cfg` with it, and passes (4.59 M distinct states to depth 24, 7 min
with 4 workers on a shared 24-thread box): epochs grow with clocks further apart than the wait.
`mutations/FloorAfterAnswer.cfg` answers the accept before logging it and fails `EpochsGrow`: the
gap §2.3 leaves while a log fails.

`Durable` gives each acceptor the log of §4.1: an accept is logged before it is answered, a release
clears it, and a restart takes the logged lease back run out, below a promise of the floor alone.
Every config models it but `mutations/ListForgotten.cfg`, which sets `Durable = FALSE`. `Listed` says that the newest ballot a node was granted and did not give up
is held, as that ballot or a newer one, by an acceptor of every majority whose acceptors are all
warm, which is what `held_by` needs. `PaxosLeasesListed.cfg` checks it with the other two
invariants under two acceptor restarts, a majority or one twice, with lease, hold and wait of one
tick to `MaxTime` 4 and one prepare per proposer: it passes, 2.69 M distinct states to depth 28, in
4.5 min with 4 workers on a shared 24-thread box. `mutations/ListForgotten.cfg` (no log) and
`mutations/ListAfterAnswer.cfg` (the accept answered before it is logged, a later step logging it)
each fail it in seconds, after one crash. The model keeps a logged lease until a release clears
it: the sweep forgetting it a minute after its end, and the log's rewrite, are left to the code's
tests.

**Hand-over model.** [tla/Handover.tla](tla/Handover.tla) checks the cluster's side of one key, above
the lease as `PaxosLeases.tla` checks it: `get_or_spawn` on two nodes, acquires answered late, a leave
(drain, stop every actor, ask the owner to start it once the actor has ended, then release), a crash and an
eager restart, the fence before each message and timer, and `watch_key`. Invariants: never two nodes
running the actor with its fence open (`OneUnfenced`), nor one running it while another's actor has not
ended under an open fence (`MoveInOrder`, the guide's move order), store writes never going back in epoch
(`WritesInOrder`), a watch never showing an older epoch after a newer one or one epoch on two nodes
(`WatchInOrder`) nor an actor that is stopping or past its lease (`WatchShowsLive`, which guards only
`watch_slot`'s filter). `Handover.cfg` lets time run free, with 2 nodes, grace 2 ticks, hold and
acceptors' lease 3, renewals and acquires answered within 2, 3 grants, a crash, a leave and a start on a
stale view; it passes, 14.4 M distinct states to depth 45, in 6 min with 4 workers on a shared
24-thread box (15 min while that box ran at load 100). A one-tick timing error is below its resolution, so `HandoverWide.cfg` (grace 3,
hold and lease 4, no crash or leave) passes too (0.46 M states, 11 s). Run them in `spec/tla` with
`java -cp tla2tools.jar tlc2.TLC -workers 4 -config Handover.cfg Handover`. `HandoverLive.cfg` (callers,
one authority fault, one late tick) and `HandoverEager.cfg` (an eager kind, no callers) check that the
owner ends up serving the key for good (0.63 M states in 54 s, and 2,450). `HandoverReach.cfg` adds cuts that are
not a clean split (§5.3): two changes of which nodes the callers reach, which reach a majority of the acceptors and
whether the nodes reach each other, and checks that the callers are answered for good, by the node their walk lands
on or the holder it names, once they reach every node that can take the lease (`KeyServedReachable`; 77 k states in
21 s). `mutations/Handover-MoveBlind.cfg` (a key moved to its owner without asking, which stops and starts it again for
ever while the owner cannot grant) and `mutations/Handover-NoRedirect.cfg` (a caller refused
`Held` not asking the holder) fail it. The header of `Handover.tla` lists what is abstracted. Each `mutations/Handover-*.cfg` fails in seconds: a release before the actor
ends, on a leave or a rebalance; a timer without the fence check; a new lease counted from the acquire's
answer; a renewal holding the fence from its reply (on `HandoverWide.cfg`'s bounds); a start from the hold
building a new entry over the open one; a watch showing a draining actor; and thirteen flaws the code had
(no fence, before 82711c3; an acquire from a lapsed hold, and a leave closing with fences open while a
stopped actor had not ended, before f9c1546; the renewal measured from its reply, §5.1; a vacate
releasing under a newer actor, before 788c12d; a watch answering for any actor, before 2fddb01; a start
that does not wake the rebalance pass, before f8acecd; and `OrphanHold` and `OrphanHoldLive`, an
acquire that `get_or_spawn` gave up on at `operation_timeout` still granted and kept by the proposer, so
the next start there built a full `lease_grace` fence on it past the acceptors' lease, and on another
node the other keys' renewals kept it, refusing the owner `Held` for good, before 6e8a979; `LoseRace`,
`spawn_local` checking `lost`, then the lease entry under another lock, while `lose()` took the entry out
between the two and noted it lost after, so a start from the hold built a fresh fence on the epoch just
lost, before 234840d (`a_start_from_the_hold_never_fences_anew_an_epoch_lose_takes` races the two on
threads; hundreds of its 20,000 rounds fenced anew before the fix); and `WatchTrustsDue`, `watch_slot` counting a lease
past its deadline until `lose()` ran, so a starved renew loop showed epoch 1 after epoch 2, before
234840d; `HandOffOnce`, a hand-off asking the owner once, which refused `NotOwner` if the view had moved,
so the released key of an eager kind waited for a caller, and `CrashLosesRequest`, `respawn` releasing
before it asked, so a crash in between lost the request however it was retried, both before 71fe559, which
holds the lease until the owner has admitted the request, asking the owner the view names again after each
refusal for up to `operation_timeout`; the model's request follows the key with no such deadline, and outlives
a crash of the owner that took it on, between its answer and its acquire, which the code's does not
(`a_leave_cut_short_by_a_crash_loses_no_eager_key` in `tests/world_eager.rs` failed
before it). Each is opened by `Open` in its config.

**Simulation.** `paxos/sim.rs`, seeded like `seed/sim.rs`: the real cores under virtual time with
per-node drift and clock offsets, drops, duplicates, slow links, partitions, crashes and restarts.
The oracle knows real time and checks the same two invariants, plus liveness: once faults stop,
every key can be taken again within a few tries, and no round waits for its deadline: every
request ends by the sim's patience and a tick, answered or not, and waits behind at most one
request ahead of it, while the core gets five times that, so a round the core ends by its deadline
had every answer in and stalled. That check came from a rolling restart
in the conformance suite, where a renewal refused by one acceptor, refused by a warming one and
accepted by the last never moved on (`a_round_every_acceptor_answered_moves_on`). A burst of a
thousand keys shares `3 × 1` requests before the first answers (`a_burst_of_keys_shares_requests`). Only the acceptors fail
a renewal: the oracle also flags one the node ended itself by releasing a key while it was out, which
a rebalance during a rolling restart of 200 nodes hit (`a_release_during_a_renewal_does_not_fail_it`).
A same-seed-twice
test. The two epoch flaws need coincidences random faults seldom reach, so `paxos/core/tests.rs` replays
each counterexample against the real cores (`two_nodes_never_win_one_epoch`,
`a_restarted_acceptor_refuses_epochs_from_before_it_restarted`). Mutating the code the same way as
the model fails these tests or the simulation (40 seeds each); clocks further apart than the wait is
an assumption about the deployment, not code, and only the model checks it.

The simulation's acceptors keep their log on a disk that a crash cuts at any point of a write: the
ops before it land, an append in part, a `Replace` whole or not at all. Three times a run every
acceptor restarts at once, then crashes again inside the rewrite its start hands out, and starts
again: then every key granted and not released must be listed as lapsed at one acceptor of every
majority. Answering before the write is synced fails it (seed 117 of 1000), as does a log that
forgets, and a rewrite written as a removal and an append (seed 0).
`a_crash_inside_a_write_reads_back_the_log_before_it_or_after` in `paxos/core/tests.rs` crashes an
append and a rewrite at every byte and fails on that rewrite too, as does
`every_eager_key_runs_again_after_every_acceptor_crashes_inside_its_start_rewrite` in
`tests/world_eager.rs` (on each of the 8 seeds run), whose machines crash inside the rewrite once its bytes are
down and before an append's would be synced. The World tests of every acceptor or the whole cluster
restarting at once are in `tests/world_eager.rs` (spec/faults.md lists them): without the log each
failed on its first seed.

At half of those restarts, picked by the seed, each clock first steps back an hour and every
proposer restarts with them, so no process remembers an epoch: the oracle's check that each grant is
above the last fails without the log's floor (seed 0), and passes 200 seeds with it.
`a_keys_epoch_never_goes_back_after_a_clock_steps_back_across_restarts` in `paxos/core/tests.rs` is
the shortest case: a key taken and released with the clock an hour ahead, two restarts, the second
with the clock set right.

**Conformance.** `crates/axon-cluster/tests/ownership.rs`: one function every authority passes
(exclusive while held, keys independent, release then a larger epoch, a stale release ignored), run
over `MemoryLeases`, `PaxosLeases` with 3 acceptors, and `ConsulLeases` under `scripts/check.sh
live`. Then what Paxos adds: a lease nobody renews runs out within `T` and its renew says `Lost`;
nothing is granted without a majority; a node with another list or lease gets no votes
(`a_node_with_other_acceptors_or_lease_gets_no_votes`, which a stray list won before); a rolling
restart of every acceptor, each waited out, keeps the holder's lease and epoch; an acceptor without
a store is refused at start (`an_acceptor_without_a_store_is_refused`); two thousand
acquires at once are all granted (before batching, 1982 of them failed), on asking again where a
request was not answered in time; a grace longer than the hold is refused at start.

**Cluster.** In `tests/cluster/leases.rs`: the renew-loop overrun of §5.1 (fails before the fix);
`keyed_actors_move_and_stop_with_their_paxos_leases`, acceptors on nodes of their own: a holder
crashed, its key running on the other host within `3T`; then 2 of 3 acceptors gone, the actor
stopped within `T` and none started. `each_start_of_a_key_has_its_own_number_though_starts_share_an_epoch`:
a restart under the kind's policy builds under the epoch of the start before it, and `SpawnContext::start` grows.

**Partial cuts.** `tests/world_reach.rs`, World tests over 16 seeds, five nodes, three acceptors, the
callers' calls checked against a ledger: callers whose owner is cut from the key's holder are sent to
the holder (`reach_holder`, which waited 21 s refused `Held` before); an owner cut from the acceptor
majority is passed and the next node serves the key (`reach_minority`, 14 s refused `Ownership` and
`NotOwner` before); and a key started past its owner stays where it is until the owner can take it, then
moves back once (`reach_back`, five starts and four moves of the key during the cut without the probe).
`tests/world_eager.rs` and `world_faults.rs` hold the walk to the owner's answer: a node's own refusal
(`Ownership` while acceptors warm up) is not walked past.

**Wire.** Pinned bytes (`wire_bytes_are_pinned_to_the_schema`), and every truncation of a valid
message refused (`wire_round_trips_and_refuses_every_truncation`). The decoders stay crate-private,
so there is no fuzz target; they take bounds-checked reads only and cap the item count before
allocating.

## 10. Left out

| Item | Why |
|---|---|
| Changing the acceptor list while running | Restart every node with the new list; no grant can happen for `T` around the switch. Add a joint phase if someone needs it. |
| Persisting every promise | The highest epoch granted is logged (§4.1), which keeps epochs growing; a promise not followed by a grant fences nothing, and logging it would sync every prepare. |
| Waiting for the log while it fails | Grants go on without it (§4.1), so the floor of a restart then falls back on the clock (§2.3). |
| Reading who holds a key without acquiring it | A refused acquire names the holder (§4), which is all `get_or_spawn` needs. |
| Agreeing on the acceptor list | Each node takes it from its config; nodes whose lists differ get no votes from each other (§4), but a list that is a majority on its own, or two lists running side by side during a change, each grant. Needs the list decided like a lease, with the joint phase above. |
