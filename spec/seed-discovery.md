# Design — `SeedDiscovery`

A `Discovery` provider that needs no external service. A few ordinary nodes also run a seed
server. Every node keeps a connection to every seed, receives the member list once, and then
receives changes as they happen. Status: built.

## 1. Scope

In scope:

- Membership: who is in the cluster, at which address, with which metadata.
- Failure detection, graceful leave, restart of a node under the same id.
- 2 to 1000 nodes on one network, with 1, 3 or 5 seeds.

Out of scope:

- Ownership. `OwnershipAuthority` stays separate, and `ConsulLeases` works unchanged beside
  this provider. Like `ConsulDiscovery`, this provider never confers ownership.
- Consensus and leader election. Seeds do not talk to each other as seeds.
- More than about 1000 nodes. Past that the answer is a gossip protocol, not this design.

No change to the `Discovery` trait, to `ClusterConfig`, or to the cluster core. One small
addition to `axon-net` (§4.5). No new dependency, so no feature flag.

Why a second port and not the cluster's own links: the engine only dials nodes that are
already in the view, its endpoints can answer a request but cannot push or see a session
close, and `Discovery` is built before the cluster exists. Removing those limits costs more
core code than the listener here, and gives slower failure detection.

## 2. How it works

| Term | Meaning |
|---|---|
| Seed | A node that also runs a `SeedServer`. Same process, one more port. |
| Attached | A node has an open, authenticated session to a seed and has received `SYNCED` from it. |
| Live | A node is attached to more than half of the seeds, and, once it has a record, more than half of the seeds list that boot of it and none it is attached to lists a greater one. |

- Every node dials every seed and stays connected. With 3 seeds a node holds 3 connections and
  a seed holds one per node. A seed dials itself like anyone else; nothing about it is special
  on the client side.
- A seed keeps one list: the records of the nodes registered over its own sessions. It sends a
  new session the whole list, then every change.
- A node's view is the union of the lists of the seeds it is attached to. A member is present
  while any of those seeds lists it. For one id, the entry with the greatest
  `(incarnation, version)` wins. The view includes the node itself.
- A node that is not live freezes: it keeps its last view, `snapshot` and `watch` answer
  `Unavailable`, and `heartbeat` fails. The cluster core already handles that state.
- Two live nodes always share a seed that lists both, because two majorities overlap. Live
  nodes therefore agree about each other.

### 2.1 What follows from "more than half"

| Event | Result |
|---|---|
| One of 3 seeds crashes or restarts | Nothing visible. The other two still list everyone. |
| A node's link to one seed blips | Nothing visible. The other seeds still list it. |
| 2 of 3 seeds are down | Every node freezes. Actor traffic continues on the last view; nobody joins or leaves until a seed returns. The same as Consul losing quorum. |
| A partition | The side with most seeds carries on and drops the other side's nodes. The other side freezes on its last view. |
| A cut from most seeds heals | Each node thaws once every member of its last view is listed again, or 5 s after it is attached again (§2.2). |
| Cluster start | No view exists until more than half of the seeds are up and past their warm-up (§4.2). |
| 1 seed | The seed is the arbiter. If it dies the rest freeze. Fine for 2 to 5 nodes and for development. The seed itself is live alone. |
| 2 or 4 seeds | Allowed, never better. 2 tolerates no seed failure, like 1, with twice the chance of one. 4 tolerates one, like 3, and a 2–2 partition freezes both sides. |

Without an ownership authority, keys owned by nodes on the frozen side of a partition run on
both sides until it heals. That is today's behaviour with `ConsulDiscovery`. Strict ownership
needs leases.

### 2.2 Timing against the cluster's isolation check

A node that is cut off must stop its keyed actors before the others take them over. Without an ownership
authority the isolation check is what stops them; with one, the lease's fence does ([wan.md § 7](wan.md)).

| Moment after a node is cut off | What happens |
|---|---|
| 3–5 s | Its seed sessions go idle on both ends. The node is no longer live and its `heartbeat` fails. |
| 5–7 s | `heartbeat_grace` has passed. With no peer reachable it isolates; without an authority it stops its keyed actors. |
| 8–10 s | The seeds remove it (idle, then the 5 s hold). The others take its keys. |

The times are at the default `SeedTimings` (§3): idle 4 s, `idle_hold` 5 s.

- **Use `heartbeat_interval = 500 ms` and `heartbeat_grace = 1 s` with this provider.** The last good heartbeat
  is before the session loss, so the stop comes at most the grace, one interval of tick rounding and the peer
  check (half the grace) after it: 2 s, so by 7 s. The two ends of a session see the loss
  at most two ticks apart, so the seed removes the node at least 1 s after the stop, and 2 s in all but the
  case of a cut between a ping and its pong. Measured in the cluster suite: isolated at 6.4 s, removed at 9.9 s. A grace of 2 s with
  the default 1 s interval can take 4 s, which leaves no margin; the default of 5 s, sized for Consul's 10 s
  TTL, puts the stop after the takeover. A short grace is safe here: a node whose peers still list it does
  not isolate, and `heartbeat` is a local check that costs nothing. Without an ownership authority
  `Cluster::start` refuses `heartbeat_interval + 3/2 heartbeat_grace` at or past `Discovery::drops_after()`,
  which seeds answer as `idle_hold` less the two keepalive ticks (3 s by default), with `ClusterError::Config`.
  With an authority the lease fences the keys, so nothing is refused.
- A node cut off from its seeds only, with its peers still reachable, keeps its keys while the
  peers still list it, and stops within about a second of being removed. That second of overlap
  is the same as with `ConsulDiscovery`.
- A one-way cut of what reaches the node is tighter than the table. The node's sessions idle out
  and it closes them; its close still reaches the seeds, which see a clean close and remove it
  `closed_grace` (2 s) later, not `idle_hold`. From its last good heartbeat the stop takes up to
  2 s at the guide's timings, as long as `closed_grace`: the removal must still reach a peer's
  view and a caller there, but there is no margin. `drops_after` keeps `idle_hold` less 2 s, as
  `min(idle_hold - 2 s, closed_grace)` would refuse the guide's timings at start, and raising
  `closed_grace` would delay every crashed process's removal. Where this matters, use an ownership
  authority: its lease fences the keys, and a node isolated with nobody answering keeps them to that
  fence.
- A node attached to a minority of seeds for 4 s, counted from the last `SYNCED` it gained,
  sends `LEAVE` to the seeds that have synced, and registers again once it is attached again. Seeds
  still warming up keep its registration. A node that reaches a minority of seeds is then in nobody's
  view, not in some.
- A node attached again stays frozen on its last view until every member of that view is listed
  again, or for the warm-up (5 s, longer than a redial's 3 s). The others cut off with it withdrew
  or were dropped, and register again as each is attached, up to a redial apart: live sooner, the
  first back showed a view without them while they kept theirs, and quota nodes whose views differed
  chose different replicas and starved one of grants for over 2 s, in the World. A member that left
  meanwhile makes it wait the 5 s.

## 3. API

```rust
pub struct SeedConfig {
    /// "host:port" or "id@host:port". 1 to 16 entries.
    pub seeds: Vec<String>,
    pub transport: axon_net::Config,
    pub tls: Option<Tls>,
    pub timings: SeedTimings,
}

pub struct SeedServerConfig {
    pub addr: SocketAddr,
    pub transport: axon_net::Config,
    pub tls: Option<Tls>,
    pub accept: AcceptLimits,
    pub timings: SeedTimings,
}

pub struct SeedTimings {
    pub idle: Duration,         // a session with nothing received this long closes; 4 s
    pub idle_hold: Duration,    // a seed keeps a member whose session idled out this long more; 5 s
    pub closed_grace: Duration, // ... and one whose session closed; 2 s
    pub warm_up: Duration,      // a new seed withholds SYNCED, and a node attached again stays frozen; 5 s
}

SeedConfig::new(seeds, &transport)                       // clones the transport
SeedDiscovery::start(cfg).await -> Result<SeedDiscovery, _>
SeedDiscovery::set_seeds(&self, seeds) -> Result<(), _>
SeedDiscovery::stats(&self) -> SeedStats

SeedServerConfig::new(addr, &transport)
SeedServer::bind(cfg).await -> Result<SeedServer, _>
SeedServer::local_addr(&self) -> SocketAddr
SeedServer::stats(&self) -> SeedServerStats
```

Wiring:

```rust
let transport = axon_net::Config::new("orders-1", SharedSecret::new(secret)?);
let seeds = ["seed-a:7001", "seed-b:7001", "seed-c:7001"];

// On the seed machines only:
let _server = SeedServer::bind(SeedServerConfig::new("0.0.0.0:7001".parse()?, &transport)).await?;

let discovery = SeedDiscovery::start(SeedConfig::new(seeds, &transport)).await?;
let mut cfg = ClusterConfig::new(bind, transport, discovery.clone());
cfg.heartbeat_interval = Duration::from_millis(500);
cfg.heartbeat_grace = Duration::from_secs(1);
let cluster = Cluster::start(&system, cfg).await?;
```

- The seed role is its own type. A seed machine runs a `SeedServer` and a `SeedDiscovery` that
  dials it over loopback like any other seed. A test binds servers on port 0, reads
  `local_addr()`, and then builds the seed list.
- Both take `&transport` and clone it, so the identity is the cluster's. `register` answers
  `Invalid` if the record's id or incarnation differs from it.
- `SeedTimings` are settings, the same on every seed and node, checked where they are set:
  `idle` over 2 s (a session lives through one lost ping), `idle_hold` at least `closed_grace` and over
  2 s (`drops_after` above zero), `warm_up` at least 3 s, and at least 2 s more than the node's seed
  `connect_timeout` (a redial lands within it), `closed_grace` above
  zero, and none over 1 h. `SeedServer::bind`
  refuses others with `Error::Config`, `SeedDiscovery::start` with `DiscoveryError::Invalid`. Changing
  them means stopping every node and seed at once (§4.1).
- The seed links' connect is `SeedConfig::transport.connect_timeout`, 1 s from `SeedConfig::new`: a node whose
  redial does not connect within `closed_grace` is dropped by that seed, so across regions it is the cluster's, 3.5 s.
  A transport put in its place after `new` keeps its own (5 s from `Config::new`), which the default `warm_up`
  refuses; the error names both fields and the fix.
- Constants, not settings: keepalive 1 s, redial backoff 100 ms to 2 s, change
  batching 20 ms, view floor 50 ms, 64 KB per frame, 4 MiB per list, 4096 members, 64 observers.
  None of them changes with the round trip.
- `bind` raises `accept.max_connections` to at least 2 × 4096: a dead session holds its slot
  for some seconds, and the default of 1024 would block reconnects at 1000 nodes.

### 3.1 The provider's copy of the transport

Both types change their clone before use:

| Setting | Value | Why |
|---|---|---|
| `namespace` | `"{namespace}/seed"` | A seed address pointed at a cluster port, or the reverse, fails at the handshake. |
| `keepalive_interval`, `idle_timeout` | 1 s, 4 s | Failure detection. |
| `compression` | none, accepting all | Sessions never compress; blocks arrive compressed (§4.5). No per-session zstd state. |
| `limits.max_frame` | 64 KB | The largest valid record is about 35 KB. Bounds the read buffer an authenticated peer can force. |
| inbound limits on the server | 8 frames, 128 KB | A node sends one small frame at a time. |

### 3.2 The seed list

- One entry is one seed. A host name is resolved on every dial.
- Seeds are counted by handshake node id. A second entry that reaches an identity already
  counted does not count and is flagged in `stats()`. A DNS mistake cannot make one seed look
  like a majority.
- TLS server name: for `host:port` it is the host, so the seed's certificate must carry that
  name. For `id@host:port` it is the node id, as on cluster links, and the handshake identity
  must equal it.
- To replace a seed machine, keep its name.
- `set_seeds(list)` changes the list on a running node. The list is refused if it is empty,
  longer than 16, or does not parse. "More than half" counts over the new list at once. Seeds
  that were removed stay connected and in the union until the node is live on the new list,
  so the view never collapses on the way.
- Where the list comes from is the application's business. An application that serves its seed
  list from its own API fetches it at start, keeps the last good answer in a file for when the
  API is down, polls, and calls `set_seeds` on a change. No node restarts. While nodes hold
  different lists, a partition can leave both sides live; keep that window short and stop
  retired seed servers.

## 4. Protocol

Sessions are `axon_net` sessions: the same handshake, authenticator, namespace check and TLS as
cluster links.

### 4.1 Frames

Encoding in the style of `wire.rs`: fixed big-endian integers, strings as a `u16` length and
bytes, no field names. `Frame.schema` is the protocol version, `Frame.kind` the message:

| Direction | Kind | Body |
|---|---|---|
| node → seed | `REGISTER` | One record. |
| node → seed | `LEAVE` | — |
| seed → node | `CHANGES` | A batch of `Put(record)` and `Remove(id, incarnation)`. |
| seed → node | `SYNCED` | The seed's `SeedTimings`: `idle`, `idle_hold`, `closed_grace`, `warm_up`, in ms, a `u32` each (16 bytes). Sent once per session. |
| seed → node | `REFUSED` | `Stale`, `Full` or `Invalid`. |

A record is id, incarnation, address, metadata and `version`. `version` is a counter the node
bumps whenever its record changes. It is `NodeRecord.version`, so it is the same for every
observer.

The protocol version is 2, and a frame of another closes the session: a hard cutover, so every seed
and node stops at once to change it. A node compares the timings of each `SYNCED` with its own. When
they differ it does not count that seed: the seed stays in the denominator, so the node needs a
majority of the seeds that agree; the seed's list stays out of the view; the node sends it `LEAVE`,
since it registered during the warm-up. `SeedLinkStats` shows the seed as `timings`, and the node
records one event, `seed_timings`, naming the seed and both sets. The node keeps the seed's timings
across sessions: it does not register with it again, its redial keeps backing off, and the event
comes again only when the timings change, until a `SYNCED` shows the node's own. A second `SYNCED`
in a session closes it. A seed that holds a silent member for less than the node's design assumes
would otherwise drop it before it isolates.

### 4.2 The seed server

- **New session.** Send the list as `CHANGES` frames of at most 64 KB, then `SYNCED`, then
  changes. The list is capped at 4 MiB encoded, so it always fits a fresh session's queue in one
  burst and no per-session streaming state exists.
- **Warm-up.** For 5 s after it starts, a server accepts sessions and registrations but
  withholds `SYNCED`. A list that is still filling never counts as a seed's answer. Nodes redial
  within about 3 s (a backoff capped at 2 s, which its jitter only shortens, and a connect of up
  to the 1 s keepalive), so by the end of the warm-up the list is whole. Without this, a restarted seed
  would make the first nodes back see a nearly empty cluster and take every key.
- **`REGISTER`.** Refuse as `Invalid` unless the record passes `NodeRecord::validate`, its id and
  incarnation equal the session's handshake identity, and the incarnation's time half is not
  more than 60 s ahead of the server's clock. Refuse as `Stale` if a greater incarnation of
  that id is listed. Refuse as `Full` at 4096 members or 4 MiB. Otherwise store it and send
  `Put` to every session, the registrant included: the echo is the acceptance.
  - A greater incarnation replaces the old boot at once and closes its session.
  - The same incarnation with a lower `version` is ignored.
  - The server keeps each record as its encoded bytes, so lists and changes are concatenation.
- **A record belongs to the session that last registered it.** Only that session ending starts
  a removal. A lingering older session of the same node ending removes nothing.
- **Batching.** A change is sent at once when the server has been quiet; further changes are
  held until 20 ms after the last send and go as one frame. An isolated change is not delayed.
- **Observers.** A session that has not registered within 10 s is an observer. At most 64; past
  that the oldest is closed.
- **Slow sessions.** A change frame that does not fit a session's queue closes the session. The
  node reconnects and gets the list again. A session never holds more than its queue.

### 4.3 When a node is gone

| What happened | The server removes it after |
|---|---|
| `LEAVE` | 0 s |
| The session closed (process crash: the kernel resets the connection) | `closed_grace`, 2 s, unless the same boot has registered again |
| The session went idle (machine or network death; seen 3–5 s after the fact) | `idle_hold`, 5 s, more (§2.2) |

The rest of the cluster sees the member leave when the last seed that listed it removes it.

### 4.4 The node

- Per seed: the list as received, and whether `SYNCED` has arrived, with the node's timings or
  others (§4.1). A `Remove` applies to that seed's list only. Losing the session clears all three; redial with a backoff from 100 ms doubling
  to 2 s, 20 % jitter, which resets only after a session that got `SYNCED` and then held 2 s, so
  a session that keeps failing, or is closed as too slow again and again, cannot spin.
- `REGISTER` is sent as soon as a session opens, and again whenever the record changes.
- `Put`s whose incarnation is more than 60 s ahead of the local clock are dropped.
- The view is built when `watch` or `snapshot` asks, not on every frame. `revision` moves only
  if a fingerprint over the view's `(id, incarnation, version)` differs, and `watch` answers at
  most once per 50 ms: at once when quiet, deferred when busy. **This is required**: the same
  change arrives from every seed, and a `watch` answer that carries no change makes the
  cluster's poll loop pause for `poll_interval`.
- `Stale` is not kept. After a `REFUSED(Stale)` the node registers again every 2 s and reports
  `Stale` only while a seed still refuses it. A clock that was briefly behind heals by itself;
  a duplicate id keeps failing visibly.

### 4.5 Compress once (addition to `axon-net`)

Today a session compresses each frame in its own write loop, so a list sent to 1000 sessions
would be compressed 1000 times and leave zstd state on each.

- New: a frame whose payload is already a compressed block, with its codec. The encoder writes
  it as is and sets the codec in the header. The receiver needs no change. About 20 lines in
  `frame.rs`, with a helper that compresses a payload into such a block.
- The server compresses each list chunk and each change batch of 512 bytes or more once, with
  zstd, and shares the bytes between sessions. List chunks are rebuilt when the list has
  changed, at most once per batch.
- Both ends are this provider and accept zstd, so there is no fallback path.

## 5. The `Discovery` trait

| Method | Behaviour |
|---|---|
| `register(record)` | `Invalid` on an identity mismatch (§3). A first or changed record: send it, then wait up to 2 s until more than half of the seeds list it. `Stale`, or `Invalid` with the reason, if a seed refused it. If the node is not live at the deadline: `Ok`, because a cluster must be able to start before its seeds. If it is live and still not listed: `Unavailable`. An unchanged record (by `NodeRecord` equality) gets the same verdict: on a live node after the same wait, so two calls racing to send one record agree; on one that is not live at once, since the call that sent it waited already. `cluster.rs` re-registers after a failed heartbeat and counts itself heard only when a heartbeat then passes. |
| `heartbeat` | `Ok` when live. `Stale` while refused. Otherwise `Unavailable`. |
| `snapshot` | The view, if live. Otherwise `Unavailable`. After `register(changed)` returned `Ok` on a live node, it shows the change. |
| `watch(after)` | Waits until `revision != after`, then the view. When not live, waits up to 1 s for a thaw before answering `Unavailable`. Never answers with an unchanged revision. |
| `unregister` | Sends `LEAVE`, waits up to 1 s for the flush, stays connected as an observer. A later `register` works; `Cluster::start` calls `unregister` on its error paths. |

`revision` starts at 1. Dropping the last clone of `SeedDiscovery` closes its sessions; dropping
the `SeedServer` closes the listener and its sessions.

`SeedStats`: per seed the entry, identity, state, time in that state and `SessionStats`; live
or frozen; members; duplicates and refusals seen. `SeedServerStats`: sessions, registered,
observers, warm-up remaining, refusals by reason, handshakes rejected, list bytes.

## 6. Security

- Only a peer that passes the `axon_net` handshake can open a session, in a namespace separate
  from the cluster's.
- A node can register only itself: the record must match the handshake identity.
  - With `CertificateIdentity` that identity is bound to the certificate.
  - With `SharedSecret` any holder can claim any id. Here that means it can replace a member
    for as long as it stays connected, by registering that id with a greater incarnation. The
    60 s bound stops it from locking the real node out for good. Use certificates where that
    matters.
- A node dials only its configured seeds, and with TLS checks the name (§3.2). Standing up a
  false seed needs the credential and control of the name or address.
- A seed is trusted for its own list. A hostile seed can add members and can shadow one with a
  greater incarnation, within the 60 s bound.
- A record's address is not verified, as with Consul.
- Bounds: 4096 members and 4 MiB per list, 64 observers, 64 KB frames, small inbound limits,
  `accept` limits on the listener, slow sessions closed. Nothing grows with time.

## 7. Measured behaviour by size

`scale nodes` with `AXON_SEEDS=3` on the devbox (Ryzen 7 3800X, 16 threads): every node and all three
seed servers in one process over loopback, 20 links per node, 20,000 keyed actors, records of about 60 bytes
on the wire. The same run on `MemoryDiscovery`, which is polled once a second, is the baseline.

| | 200 nodes, memory | 200 nodes, 3 seeds | 1000 nodes, memory | 1000 nodes, 3 seeds |
|---|---|---|---|---|
| Started and agreed | 1.0 s | 6.0 s | 1.3 s | 6.4 s |
| Resident per node, no links | 0.4 MB | 0.8 MB | 0.9 MB | 2.35 MB |
| Idle, no links, whole process | 0.14 cores | 0.19 cores | 2.0 cores | 1.6 cores |
| A join seen by every node | up to 1 s (the poll) | 31 ms | up to 1 s | 0.39 s |
| A leave seen by every node | 1.0 s | 87 ms | 1.0 s | 0.61 s |
| One restart, until every node sees the new boot | 1.0 s | 0.14 s | 1.1 s | 1.4 s |

- A start is the 5 s warm-up plus 1 to 1.5 s. At 1000 nodes the lists, 1000 × 1000 records from each of three
  seeds, arrive in that last second.
- A seed re-encodes and compresses its list for the first session that opens after a change. In a cold start
  that is up to once per node, `n²` record bytes in all: 30 MB of zstd input at 1000 nodes of 60 bytes, which
  does not show. At 4096 nodes of 300 bytes it would be 2.5 GB, seconds on the one serve task. Left as it is
  for the sizes this is built for.
- Memory: a 60-byte record is about 650 bytes once decoded, most of it the metadata map. A node keeps one
  copy of a record however many seeds sent it (`Arc`), plus the three maps that index it. The seeds add
  0.4 MB per node at 200 and 1.4 MB at 1000, both ends of three sessions included, and it grows with the
  member count. 1000 nodes and three seeds in one process need about 1.5 GB and 6000 descriptors more than
  the baseline.
- No registration was refused (`refusals`) and no handshake failed in any run. A seed takes 64 handshakes
  at a time and the other first dials wait their turn within `handshake_timeout`. When it turned them away
  (`rejected_busy`), about 350 of the 1000 to each seed were, and one start in three on another machine had a
  node give up with `attached to too few seeds`. The per-address cap of 8 was lifted for
  every run, because all nodes share one address here; its effect was not measured.
- CPU per change was not measured per node. From wall time × cores with all 1000 nodes on one saturated
  16-thread machine, a restart (draining, leave, join, kinds, and the telemetry metadata) costs about 20 ms
  of CPU on each node, a few ms per change. A profile at 300 nodes puts most of it in rebuilding the view,
  the deep copy of the member list into the `Snapshot`, and `Membership::apply`; placement and validation are
  a few percent. A change that a second or third seed repeats does not rebuild the view.
- A process crash is reported in about 2 s, a leave at once, a dead machine in 8–10 s (§2.2).
- Idle traffic is the keepalive, one small frame a second each way per session.

## 8. Tests

**Core.** The server's list and the node's view are two state machines with no I/O and the time
passed in. Unit tests, plus a seeded simulation on `axon_testkit::Simulator` with
`check_seeds`: 1, 3 and 5 seeds, up to 8 nodes, ordered links, session loss synthesised by the
harness, crashes, restarts, partitions. Invariants:

1. A live node's view contains itself, and at rest all live views are equal to the set of
   running, registered nodes.
2. A view loses a member only after that member left, crashed or was partitioned away. Checked
   across: one seed restarting (1, 3, 5 seeds), all seeds restarting in quick succession, a
   thaw, a partition with no majority side healing, and `set_seeds`.
3. A session loss to a single seed changes no view (3 or 5 seeds).
4. A superseded session ending removes nothing.
5. A crashed node leaves every live view within 3 s; a dead one within 11 s. A viewer whose own
   link to a seed went silent before that seed's `Remove` counts the seed's last list until the
   session idles, 3 to 5 s after the silence began (§2.2): it cannot hear the change sooner, so
   a crashed process can stay in that view about 7 s.
6. A node that is not live answers no snapshot, and its first view after a thaw is built from
   lists synced at that time.
7. No node is live until more than half of the seeds are past warm-up (3 or 5 seeds).
8. A restarted node replaces its old boot everywhere. A boot with a lower incarnation is
   refused, and recovers once every seed it reaches has removed the old boot: a view shows the
   greater boot while one seed lists it.
9. A node attached to a minority of seeds is in no live view after 10 s.
10. After `register(changed)` answers `Ok` on a live node, its `snapshot` shows the change.
11. `watch` never answers with an unchanged revision, however many seeds repeat a change.
12. A duplicate seed identity counts once.
13. `set_seeds`: one seed replaced, 3 to 5, 5 to 3; an empty or oversized list is refused.
14. Lists, queues and observers stay within their bounds.

**Model.** `spec/tla/SeedMembership.tla` checks the server and node rules of §4 in TLA+, with the
shipped timings in ticks of 1 s. `SeedMembership.cfg` has 2 seeds, 2 members and up to 2 faults
between 6 and 8 s: link cuts healing after 3, 5 or 10 s, a seed restart, a process or machine crash,
a boot under another incarnation, `set_seeds` from both seeds to one. `-Seeds3.cfg` has 3 seeds and
one fault, `-Seeds3x2.cfg` 3 seeds and two faults up to 3 s apart (10 s cuts, no boot),
`-Replace.cfg` a `set_seeds` that replaces one of 2 seeds by a third, `-LowerBoot.cfg` 3 seeds and
a crash then a boot under a lower incarnation, `-Odd.cfg` 3 seeds of which one holds a silent member
3 s and is not counted (`Odd`), `-Wan.cfg` 2 seeds at the WAN timings (idle 8, `idle_hold` 7,
`closed_grace` 4, `warm_up` 8, a redial within 6) and one fault: 149,167 and 49,818 distinct
states, 33 s and 15 s. The promises are formulas of the
timings that give the numbers below at the shipped ones. Invariants: a live view loses
a member only after it crashed or its list lacked a usable majority for 2 s (2), and one attached
again lacks no running member of its last view for 5 s (§2.2); a seed removes a
record only once the session that owns it has ended (4), and a silent member no sooner than 8 s
after it went silent (§2.2); a warming seed holds every member with a session to it (§4.2); a live
node holds itself (1); live nodes see each other; a crashed boot leaves the views within 3 s, a dead
one within 11 s, and a view with a session on a silent link once it idles, as tight as the model's
idle allows (5); a member in a minority within 10 s (9); 19 s after the last fault (a 10 s cut, the
redial and the thaw's hold, §2.2) every running node is live with the same view (1). In `spec/tla`,
`java -cp tla2tools.jar tlc2.TLC -workers 4 -config <cfg> SeedMembership` finds no violation: the
base in 8.90 M distinct states, depth 61, 14 to 33 min on a shared 24-thread box, the others in
less. Each `mutations/SeedMembership-*.cfg`
breaks one rule and fails within 10 min, its first line naming the invariant; `OldWithdraw` is the
withdraw rule as first designed (78e22da); `NoThaw` drops §2.2's thaw hold; `HoldBelowValidated` counts the odd seed; `LowerBoot` is
`Node::live` before 13e1216, when a
boot under a lower incarnation listed by 2 of 3 seeds was live while the third still listed its
old boot, and its view showed the old boot as itself until that seed removed it (1 and 8);
`-LowerBoot.cfg` is the same run against the rule as built. The file's header lists what the
model leaves out, and the code lines whose deletion it does not notice.

**Integration** (real sockets, in-process): attach, register, leave, crash, restart under the
same id, observer only, wrong credential refused, a record for another id refused, an invalid
record and a far-future incarnation refused, the member cap, a slow session closed without a
redial loop, a cluster port used as a seed refused at the handshake, TLS with both seed forms,
a pre-compressed frame round trip in `axon-net`.

**Cluster suite.** `tests/cluster/` gets `transport(id)` and `node_from(transport, discovery,
tweak)` split out of `node_with`, because the provider needs the node's identity. The join,
leave, crash, restart, `kind(..).register`-then-spawn, drain handoff and keyed rebalance tests run
once more on seeds. A crash there is a dropped node, so each such test waits out the seed's 2 s hold.
New: a node cut off from its seeds and peers isolates before it is removed.

**Scale.** `crates/axon-obs/examples/scale.rs` takes `AXON_SEEDS=3`: bind the servers, read
their addresses, start the nodes. Raise `max_handshakes_per_ip`; plan about 3 GB more memory
and 6000 more file descriptors for 1000 nodes in one process. Run the `nodes` mode at 200 and
1000 on the devbox and replace §7 with the measurements (done).

`crates/axon/guide/cluster.md` snippets are compiled and run, so the docs step includes a working localhost
example.

## 9. Left out

| Left out | Add it when |
|---|---|
| The seed list pushed by the seeds themselves | Never, as designed: it can flip back, split the cluster and fail silently on spelling. `set_seeds` covers the need. |
| Full records from one seed only | Cold start measures too slow even compressed. |
| A node re-checking a seed whose process is wedged but whose sessions still answer pings | It is seen in practice. |
| A DNS name that expands to several seeds | Someone needs it. The seed count must be known for "more than half". |
