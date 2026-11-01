# Design — `Quotas`

Cluster-wide rate limits and concurrency limits per key (an API key, an account, a domain), checked on the node that
serves the request with no network round trip. Status: built: the cores, the driver, the World tests, the playground run,
the benchmark (whose two numbers far from §6 are accepted for now, §9), the websocket gateway guide program and the rig
mode. It came out of two design rounds with judges and a red team (2026-09-26) and the decisions in §9; it replaces the
planned distributed semaphores.

## 1. Scope

In scope:

- Rate limits: at most `n` units per window, for windows from 1 s to 1 h (`req/s`, `req/min`, `weight/h`).
- Weighted costs: a request takes any number of units, a different number per limit, and a cost known only after
  the work can be charged afterwards (§4).
- Concurrency limits: at most `n` permits held at once per key (5 websocket sessions per account, 2000 heavy queries
  cluster-wide).
- One API key used from many regions at once, each request served in the region nearest the client.
- Millions of keys, about 50 nodes, up to 5 regions.

Out of scope:

- Exact limits. Every bound here is approximate, and §5 says by how much. An exact limit is a keyed actor per key.
- Billing and metering. Counting each admitted request for an invoice needs a durable, idempotent log; a guide program
  shows it, fed by each `Ok`.
- Daily or calendar windows, and anything that must survive a restart of the whole cluster.
- Node-local limits: `tokio::sync::Semaphore` already does them.

The requirements it is built to: no request waits on the network; a key is never routed to a far region; one node,
replica or machine failing never pauses admission; when it fails, it fails open, and only after an outage in every
region or the node's own isolation.

## 2. How it works

| Term | Meaning |
|---|---|
| Limit | A named `Rate` (`n` units per window) or `Permits` (`n` held at once). |
| Key | What a limit is counted per: an API key, an account, `b""` for cluster-wide. |
| Usage `u` | What this node has admitted for a (limit, key): units in the window, or permits held. |
| Replica | A node that adds up every node's usage of a key and hands out grants. A key has 3 to 5. |
| Grant | The part of the limit a replica gives one node. The node admits while `u + cost ≤ grant`. |
| Hold | How long a grant stays valid without news: 5 s, counted as for leases (§2.4). |

### 2.1 The check

Every call is local: hash the (limit, key) id, lock one of 64 table shards, compute `u`, compare with the node's
grant, add the cost. A rate's `u` is a ring of 9 buckets of `W/8` on `system.clock()`, so it can read up to one bucket
(12.5 %) high of a true sliding window, never low. A permit's `u` is a count; dropping the `Permit` decrements it.

The entry is marked for the next report when the check is refused, when the key is new, or when `u` has moved by at
least `max(1, grant/8)` since the last report. Its demand is the cost refused over the last one to two heartbeats (one
100 ms report cycle often sees no refusal of a steady demand, which lost 5 % of a permit limit in the simulation) and,
once the entry has been refused, how far the most it used at once in those heartbeats (its permits held or its window's
units, taken where a cost is added) exceeds its usage now. A replica caps a grant at usage and demand and lowers a row
that stops asking to its usage (§2.3), zero for a permit dropped at once, so with refusals alone a node whose grant
sufficed was refused again although the key had room. Demand never enters the budget rule, only how the budget is
divided, so it moves no bound of §5. An entry that reported demand is reported again each heartbeat until it reports
none, so the replicas hear the demand end. When a replica may lack the node's rows, because the answer to a report of
the node's did not come within a heartbeat while a later one's did (the report or the answer lost), the view shows it
restarted, or it stopped being young, having answered young or pulled rows that may have missed one, the node reports
again every entry that replica granted and every entry that lacks a grant of one of its replicas. Without it a lost
report that lowered a permit count is never sent again, and the replicas keep the higher count; and a key a replica
answered young is not granted there until its usage moves by an eighth, which put a node that also lost another replica
on the fallback for as long, in the simulation. An answer that comes after a later one is no such loss: the replica had
the report, and the grants of the answer go again until acknowledged (§2.3). Taken for one, as when the tasks that take
answers in run out of order on a loaded node, it had every entry reported again, which kept a node behind after a
processor spike (§6). Nor is a report to a replica that answers nothing, until it answers again: counted as lost each
heartbeat, it had every node report every entry that replica had granted each cycle, to every replica, until the view
dropped the replica or, across a change of the quota `SCHEMA`, until every node was upgraded.

### 2.2 Replicas

- Nodes that run `Quotas` advertise it under the metadata key `axon.quota`, added to the keys `set_metadata` refuses
  (`crates/axon-cluster/src/cluster.rs:444`). A node's region is `QuotaConfig::region`; nodes without one form region
  `""`.
- A node chooses from the quota nodes its view shows not draining, reachable from it or not. Reachability is each
  node's own: chosen among the reachable ones, nodes that each lost a different peer counted different `N` and chose
  replicas whose majorities did not meet, and a node cut off from all became its keys' only replica, so the rig's
  five nodes admitted up to 2.7 times a rate's limit for 8 s. A replica it cannot reach keeps its keys until discovery
  drops it, the node admitting on the other replicas meanwhile, or on the fallback where they are not a majority; one
  leaving hands them over at once. Views that differ choose different replicas, so seed discovery holds a node
  back from a cut frozen until the others cut off with it are back
  ([seed-discovery.md §2.2](seed-discovery.md#22-timing-against-the-clusters-isolation-check)).
- Each region's candidates are its 8 best-scored quota nodes (`replicas_per_region`), so a node talks to at most 8 per
  region.
- A key has one replica per region, chosen by rendezvous among that region's candidates, up to 5 regions. With fewer
  than 3 regions, the regions with the most candidates give a second or third, so every key has at least 3 replicas;
  a one-region cluster has 3 in that region. Regions rank by candidates, not by members, then by name, so a node
  leaving a region of more than 8 moves no key between regions.
- Every node reports to every replica of the keys it has used, every 100 ms, batched into one message per replica node
  over `Cluster::serve`, whose session identity names the node and incarnation (the body names neither):
  `seq`, the last grant `seq` it has with the replica incarnation that sent it, and per entry
  `{id, max, buckets | held, demand}`. A 1 s heartbeat keeps its rows alive where nothing changed. The batching is
  not the paxos proposer's (`paxos/core.rs` `flush`): that one queues tagged items per acceptor under in-flight and
  byte caps, while a report is a snapshot of what changed since the last one, numbered by its link's seq and ack, with
  no queue to share.

### 2.3 Grants

A replica keeps rows per (node, incarnation, entry). Its budget rule: the sum over nodes of `max(u, grant sent or
acknowledged)` stays at or under the limit. The limit is divided max-min fair: every row that asks gets one level, never
more than its usage and demand; what no row needs is spread evenly over every row, so that a node that uses the key
only now and then is not refused on its first use while that spread covers its cost; remainders go to the rows first in
node order. A row using more than the level, as a node back from the fallback may, gets the level and admits again once
its usage drains below it: floored at its usage, such a row kept its excess, refilling as its window slid, and while
the key's usage stood at its limit a row whose window had just emptied was lowered to nothing, so that a node that was
not faulted admitted nothing for 2.7 s after another's cut healed, in the World. It is a pure function of the rows, so
replicas that have the same rows give the same grants, and the level does not follow a row's usage or demand while they
stay on either side of it, so replicas whose rows differ by a report or two still give the same grants. A node admits
on the smallest of them (below): a division in proportion to demand, which moves with every report, lost 15–25 % of the
limit to that minimum in the simulation. Demand counts for two heartbeats after the report that carried it, since a
node that asks reports every heartbeat; older demand was lost with a report, or pulled.

Counting is one-sided: a raise counts from the moment the replica sends it, a lower only once the node acknowledges it.
Lost, duplicated or reordered messages can then only under-admit. The replica counts what a node may hold in two
generations, grants set before its mark and since, and drops the older once the node acknowledges an answer sent after
the mark. A grant that moves again before each acknowledgement, as a saturated rate's sliding usage moves it, then
counts the most it took for two acknowledgements rather than for as long as it moves: counted that way, it held back 9 %
of a hot rate's limit for 10 s after demand moved, in the simulation. A report that acknowledges a lower gives the room
it frees to the rows that wait for it at once. A raise grows from what the node may already hold, or from its usage
while the replica's sum is within the limit: usage over it, taken on the fallback or charged past a grant, would
otherwise become a grant that outlives it. Room short of every raise, as while a node that stopped reporting here still
holds its grant, is split max-min fair as the limit is: handed out in node order, the last nodes got none from that
replica, and so admitted nothing on the smallest grant, for over a second after a partition healed, in the World. A
replica answers every entry a node reports with its grant, zero or more, so that a node that dropped the entry, or
missed an answer, gets it again. Each grant carries the usage the replica counts in all, which tells `Denied::Limit`
from `Denied::Share`.

A node that no longer uses a key reports it only with its heartbeat, so the room it held came back only after its next
heartbeat or two: in the World, a node that a key was new to, the room held by the node that used it last, was refused
for its share 44 times over 0.86 s, and a node whose use followed another's that had taken all of the limit admitted
nothing for 1.1–1.9 s. So the replica **pushes**: where a row that wants more, by its demand or by usage over its grant,
is left short of its plan, it sends the grants set since its last message unasked to every node that may hold more than
its grant now, and a raise to a row that wants it, at most once a report cycle per node. A push is a grants message as
an answer is, answering the last report the replica took from the node, so it holds until a hold after that report left.
A node takes a push in only where it answers one of its last two reports to that replica. Its reply acknowledges the
push, which ends the holds of the grants it carried and gives the room at once, but only where the push answers its last
report and none of the push's entries changed since: the replica then knows the usage the lowers leave, as from a
report. Acknowledged either way, a permit taken on the old grant and not yet reported did not count, and the room it
held went to another node, over the limit, in the TLA+ model (§7). A replica that does not hear the node, as when only
that direction is cut, plans from a row whose usage ages out of the window, and pushes a lower the node's usage does not
call for: taken in, it left a node cut that way admitting nothing for 2.1 s in the World, until its grants lapsed and it
fell back to `a`.

A replica numbers its grants messages across all nodes, so a node it forgot and hears from again never sees a number
it has taken in already. A report acknowledges the last answer the node took in. The replica keeps the grants each of
its last 16 messages to a node carried, and an acknowledgement ends the holds of those alone: a message that left a
grant out does not show that the node has it, and a node that took in one message may not have taken in those before
it. An answer carries the grants of the entries its report named, then at most 256 set since the last message to the
node, then at most 256 more that the node has not acknowledged, from where the last answer stopped and around; a push
carries only grants set since the last message. Each grant goes again until it is acknowledged, and a message carries
at most 4096. An acknowledgement names the replica incarnation it is for: a restarted replica's
numbering starts over, and its predecessor's acknowledgements acknowledge nothing of its own.

A node admits against the **smallest fresh grant** among its key's replicas, once a **majority** of them gave one,
ignoring replicas still young (§2.6). Any two nodes then share a replica whose budget counts both; with only one
replica's grant, two nodes each told of the other by a different replica could both hold it all. With every replica
agreeing, the smallest is the grant; with one replica lagging, dead or cut off, the others still bound it. A node
takes in grants only from the incarnation its view shows for a replica, whether or not that incarnation's clock read
later, and only for keys that replica serves in its view; when the view moves a key, the grants of replicas that no
longer serve it are dropped.

### 2.4 Hold

A grant is valid for the node until `sent + 5 s·(1 − ρ) − 200 ms`, measured from when the node sent the report it
answers, with `ρ` the clock drift bound (1 %). The replica frees it at `last heard + 5 s`. These are the lease formulas
of `PaxosLeases`, in `ownership::lease_hold` and `lease_wait`, which both call. A replica that frees a node's grants
marks them for the node, but a grants message carries a bounded share of them (§2.3). The node therefore drops every
grant of a replica whose grants lapsed before it takes in the next message from that replica: an entry the message
leaves out has no grant from that replica until one carries it, and is reported again. Otherwise the message would
make the old grant valid again, after the replica gave its room to another node (found by the TLA+ model, mutation
`LapsedKept`).

Rows of a node that went silent: a rate's buckets age out with its window; a permit row is dropped once the node has
been silent for 15 s at every replica of the key that can hear the others, or at once when the view shows the node
left or restarted as a later incarnation. That goes for every earlier incarnation the replica has rows of, including one
it heard from late that no view of its own showed, as a replica paused across a crash and restart does. A node that
still reports re-reports every permit row it holds every 30 s, and a replica zeroes, then drops, a permit row its node
has not refreshed for 120 s: a node whose view moved the key to other replicas no longer reports it here, and without
this the old replica would count those permits against the key for as long as the node lives. (Found by the World
drain check; the 15 s rule alone covers only silent nodes.) A replica checks these timeouts once a second, so each
may run up to a second over.

### 2.5 Cold keys and the fallback

A node with no fresh grant for a key admits up to `a = ceil(M / N)`, where `M` is the limit and `N` the quota nodes in
its view. That covers a node's first request for a key, and its first after the entry was swept, before any grant
arrives (one report cycle); a node back from a pause, whose grants lapsed; a node whose view just moved the key to
replicas it has not heard from; and a node that hears fewer than a majority of the key's replicas. The simulation fails
a run in which a node admits on the fallback outside these for longer than 1.5 s. An entry new to a node admits up to
`a` on its usage for a heartbeat (1 s) after it is made where its grants refuse it for a share, and that counts as the
fallback: the room of a key another node used last is held there until that node hands it back (§2.3), and the node new
to it was refused meanwhile although the limit had room. Its refusals are reported as demand all the same. An entry made
again after an eviction does not have it. An entry, with its last grant, is kept for 60 s after its last request (the
sweep reaches every entry every 6.4 s, so it goes within 6.4 s and a report cycle more), and for as long as it holds
permits or units in the window, which the replicas would otherwise forget with it. Under memory pressure the least
recently used entries not in use go, and their ids are set in a 1 MiB two-generation bitmap: a key that hits it is
refused until it is admitted on a majority's grants, so eviction cannot hand out `a` again and again.

Nothing admitted is ever taken back (§9): usage admitted over a grant drains as permits are dropped and windows slide.

### 2.6 Handover

A node that becomes a replica of keys, because one joined, left or failed, is young for those keys until it has pulled
the rows of **every** other candidate; nodes ignore a young replica's grants. A new process is young for every key.
Otherwise only keys handed to it make it young: it was no candidate before, or a candidate of its own region left, whose
keys go to the region's others. With one replica per region, a candidate leaving hands its keys to its region-mates
alone, so the other regions go on granting and no node falls back to `a`; were a replica young for every key whenever
any candidate left, the nodes using its keys would admit on the fallback for the length of each pull.

A pulled row carries what the node may hold on the replica it came from, which stands in for the grants of the
incarnation being replaced: a node admits on a majority, so one of the others gave it at least as much. The hold ends
at the first acknowledgement after the node reported the key to this replica, whose view then shows this one serving
it, or 120 s after the pull, when the node dropped the key or its view never moved it here; a node the replica frees
for silence frees it too. An acknowledgement alone does not end it: a node whose view lags takes none of this
replica's grants for the key, yet acknowledges its answers, and may still admit on the grant of the replica this one
replaces. One replica is not
enough: a restarted replica and one that missed a report are a majority that knows nothing of a grant. A pulled row of
an incarnation the view no longer shows is folded into the departed row as it arrives. Departed incarnations are
compacted into one row per entry, so a pull grows with keys, not with rows. `/readyz` waits for a new process's pull
(`quotas: starting`, `Cluster::quotas_starting`), so a rolling restart paces itself; a handover's leaves the node
ready, since it is young only for the keys handed to it.
A candidate that refuses a pull for good (another protocol, schema or `replicas_per_region`, which nodes add to the
endpoint's protocol id so that they never mix) has nothing to pull and is dropped from it, so readiness never waits on
it; one that does not answer is asked again every 500 ms until the view drops it or shows it unreachable. An
unreachable candidate is not pulled: a dead node that static discovery keeps listing would keep the puller young. A pull carries the puller's
candidates, and a candidate serves the keys the puller replicates among them: its own view may not show the puller
yet, or still show a node that left, and a page chosen by it would leave out keys the puller took over, which would
then turn ready, or stop being young, with nothing pulled for them.
A replica paused for more than 2.5 s ignores its own grants for one heartbeat round: the driver
ticks it before it answers the first report after the pause. Once a
replica is no longer young it grants every key again, since the zeros it answered with while young count at the nodes
from then on.

## 3. API

```rust,ignore
const REQ_SEC: Rate = Rate::new("req/s", 10, Duration::from_secs(1));
const WEIGHT_MIN: Rate = Rate::new("weight/min", 1200, Duration::from_secs(60));
const WS: Permits = Permits::new("ws", 5);
const HEAVY: Permits = Permits::new("heavy", 2000);                     // key b"": cluster-wide

let quotas = Quotas::start(&cluster, QuotaConfig::default()).await?;    // every serving node
quotas.take(key, &[(REQ_SEC, 1), (WEIGHT_MIN, weight)])?;               // Result<(), Denied>, all or nothing
quotas.take(key, &[(WEIGHT_MIN.with_max(plan.weight_min), weight)])?;   // a per-plan limit
quotas.charge(key, &[(WEIGHT_MIN, extra)]);                             // never refuses (§4)
let permit = quotas.acquire(account, &WS)?;                             // Result<Permit, Denied>
quotas.stats(); quotas.describe();                                      // for telemetry and the console
```

- `Denied::Limit { limit, retry_after }`: the key is at its limit. Answer 429 with `retry_after`; nothing was taken.
- `Denied::Share { limit, retry_after }`: the limit has room but this node's grant is too small for the cost. The
  refusal is reported as demand, and `retry_after` is when this node expects the room: once its next report went and
  a node that holds the room handed it back (§2.3), counted as the time to the next report and three round trips, or
  once enough of its own usage left the window, if sooner. It is at most a heartbeat, and a heartbeat for permits.
- `Permit` is `#[must_use]`, not `Clone`, and releases on drop. Move it into the actor or task it admits, never
  `let _ =`: an actor that holds it releases it when it ends.
- Nothing waits: there is no blocking acquire.

README rules: limits are approximate and never wait; call `take` after authentication (every new key gets `a` once,
so unauthenticated keys would be a flood of `a`s); use the same limit names and maxima everywhere, and where they
differ, the maximum in the newest report wins and `stats().mismatched` counts it.

## 4. Weighted costs

A request takes as many units as it costs, per limit, in one all-or-nothing call: `(REQ_SEC, 1)` and
`(WEIGHT_MIN, 20)` together, or neither. The shards of a multi-limit call are locked in index order.

- **A heavy request larger than this node's grant.** With 1200 per minute over 50 nodes, a node that has seen little
  of a key may hold 24, and a request of weight 100 is refused with `Denied::Share` although the cluster has room. The
  refused cost is reported as demand, so the replicas enlarge the node's grant within about one report cycle, and the
  retry is admitted.
- **A cost larger than the whole limit** is always `Denied::Limit`.
- **A cost known only afterwards** (rows returned, bytes streamed): `take` an estimate first, then `charge` the
  difference. `charge` never refuses, so the key can go over by one request's excess; later `take`s are refused until
  the window slides past it.
- **No refunds.** A cost given back is a lower, which only counts once acknowledged (§2.3); charge on success instead.

## 5. Guarantees

`M` the limit, `N` quota nodes, `a = ceil(M/N)`, `λ` = 100 ms + round trip, `ρ` drift. Every figure is an estimate
until the tests and the rig measure it.

| Fault | Most over the limit | Most under | How long | Pauses? |
|---|---|---|---|---|
| steady state | rates `ρ·M` per window | a saturated rate `M/9`, the ring's bucket (§2.1); grants in small fragments, up to 25 % | continuous | no |
| a new key, or a key new to a node | `a` per node new to it, up to `N·a ≈ M + N` if every node sees it at once | 0 | first `λ`, or first heartbeat (1 s) where another node holds the room | no |
| demand moves between nodes | 0 | the gainer waits for the loser's acknowledgement | a report cycle + 3 round trips: 50–100 ms in the World at 1 ms (1.1–1.9 s before pushes, §2.3) | no |
| a node crashes | its unreported usage, ≤ grant/8 | its permits stay counted | until its restart shows, else 15–16 s | no |
| a replica crashes or is cut off | 0 | growth capped at the last grant | ≤ hold (5 s) | no |
| a replica hands over | 0 | 0 | until the pull ends: 64 s at 10⁶ keys (§6) | no |
| partition into `k` sides | each side admits up to `M`: total ≤ `k·M`; while the views still list the other sides, a side without a majority of a key's replicas admits its nodes' `a` each | 0 | partition + a hold (5 s) + one window | no |
| message loss, duplication, reorder | 0 | one report cycle | ≤ 1 s | no |
| reports late by seconds, as from links whose queues fill | `(R/q − 1)·M`, `M/2` with 3 replicas: replicas whose rows differ divide `M` each among the nodes they heard from, and a node admits on at most the mean of a majority `q` of its `R` | 0 | until the queues drain, + one window | no |
| clock drift | `ρ·M` per window; wall-clock steps change nothing | 0 | continuous | no |
| process pause, CPU starvation | < 4.75 s: 0; longer: what it took that the replicas had not heard of, at most its rate grant or the permits it holds (all of them past 15–16 s, when its permit rows are dropped) | `a` on resume | a lease after it was last heard until it reports on resume | that node only |
| memory pressure, eviction | 0 | evicted keys refused | ≤ `λ` | no |
| a node hears fewer than a majority of a key's replicas | `Σ max(u, a)` ≤ `M + N·a` | 0 | its isolation | no |
| every replica of a key lost at once | that key's departed usage: ≤ `M` | 0 | until a replica is back and has pulled, + one window | no |
| `charge` | one request's excess | 0 | until the window slides | no |

Measured by `quota/sim.rs` over seeds 0..2000, where faults overlap and an excess counts for the set in effect: no key
went over its limit, beyond the fallback and charges, outside faults. The most over was 0.62·M with a partition in
effect (its row allows `(k − 1)·M`), and 0.1·M under crashes and pauses, with or without a cut replica, without a
partition or a lost key. Outside faults the hot key admitted at least 0.848·M of a rate (the ring allows 8/9·M ≈
0.889·M), held at least 0.954·M of permits, and 0.845·M in grants of 1 or 2. About 30 % of all units went on the
fallback, nearly all of them a node's first use of a Zipf key it takes once or twice a run, or while fewer than a
majority of the key's replicas could answer it; of the hot key's units outside faults, 3.7–4.0 %, counting the first
seconds, while every replica pulls. `quota/sim.rs`'s rig episode (§7) over seeds 0..512: at most 0.25·M over with reports late by seconds.

## 6. Cost

For scale: axon's measured local tell costs 18–35 ns, and a remote call 16–17 µs p50 over loopback. Measured by
`--example quota` on a 24-thread EPYC, every node in one process
([benchmarks § Quotas](../crates/axon/guide/benchmarks.md#quotas-axon-cluster)):

- **Check:** 97–102 ns on one thread at 10³ keys (estimated 80–120), 240–420 ns at 3 M entries and 290–480 ns at
  10 M (estimated 200–400). Uniform keys reach 54 M/s over 24 threads at 10³ keys and 34 M/s at 3 M entries: a check
  takes its shard's lock alone, each shard holding a copy of what it reads of the links. A permit drop locks its shard.
  A hot key serialises on its shard lock: 11 M/s on one thread, 1.1 M/s over 24.
- **Background:** each node talks to at most 8 replica nodes per region, one report per 100 ms while something changed
  and one heartbeat per second each. At 50 nodes, each take costs about 165–180 B sent, reports and the replicas'
  answers together (estimated 45 B per replica per changed entry, one way). A resync (§2.1) re-reports every entry the
  replica granted and every entry short of a grant, to all of their replicas: after a lost report or a replica's pull,
  one report of up to every entry, 4096 per message. A node reports at most 4096 entries per cycle, about 41 k a second,
  and an entry in use every 30 s, while a row unreported for 120 s loses its grant: a node keeps grants for at most
  about 4.9 M rate entries in use. An entry refused once can report every heartbeat for as long as it is used every
  heartbeat or two (§2.1), so each such entry takes one of the 41 k a second: with `A` of them a node keeps grants for
  about 120·(41 k − `A`) entries in use, 3.7 M with 10 k. 3 M were all granted within 75 s; 10 M were not in 10 minutes.
  The processor cost is the walks: a replica walks its rows once a second and a node sweeps its entries over 6.4 s,
  about 0.26 cores for 3 M entries and 3 M rows at rest. The sweep holds a shard's lock for 32 entries at a time, the
  shards taking turns, so a check waits for it a few µs at most. A handover pull asks one page (4096 rows) per candidate
  per cycle: 10⁶ keys took 64–66 s, though reports had brought every key's row back in 25–26 s. Pacing accounts for
  about 17 s of the pull, and the rest is not established. A replica pushes a node at most one grants message per 100
  ms, and only while a row of one of its keys that wants more is short of its plan (§2.3); the reply is 17 to 26 bytes.
  Where nodes contend for keys that is most cycles: at 1 000 takes/s on a 16-thread box that other work shared, 200 kB/s
  a node with pushes, 164 kB/s with them turned off, 150 kB/s before (indicative, [benchmarks §
  Quotas](../crates/axon/guide/benchmarks.md#quotas-axon-cluster)). Counted in a build with counters, each node sent
  236 reports a second of 271 B, as many answers of 261 B (10 entries reported, 12 set since the last message, 2 more
  not acknowledged), and 233 pushes of 150 B (13 entries), 207 of them acknowledged: the pushes, their replies and the
  framing of both are the 36 kB/s more than with pushes turned off, where the answers carry 340 B. After 16 busy loops
  took the box for 5 s, the nodes came back to 236–288 kB/s and 225–235 reports a second each, against 240, in 5 runs
  of 5. They did not before (1.7–1.95 MB/s and 63–74 reports a second, for the rest
  of every run): a node took an answer that came after a later one for a lost report (§2.1), and reported again every
  entry the replica had granted. On a loaded node the tasks that take answers in run out of order: a third of the
  answers came after a later one, the reports carried 350 entries each against 10, and the replicas' work on them kept
  the nodes loaded, with report cycles of 290 ms against 100. Pushes made that state last, not start: with pushes turned
  off, the nodes came back where grants messages carried every grant not acknowledged, and did not where they carried
  the bounded share below. Tried first, and none of them brought the nodes back: a bound on the grants a message carries
  (64 to 4096), acknowledgements that end the holds of every message up to them, a push only to a node whose last report
  acknowledged the answer before, a push declined while an answer is on its way, raises pushed only, no push after a
  late report cycle, and replicas chosen without a sort. Each grants message carries at most 256 grants set since the
  last message and, in an answer, at most 256 more the node has not acknowledged (§2.3), so the work a message makes is
  bounded however far the node fell behind: in an answer, about 10 reported, 12 set since and 2 more go at 1 000
  takes/s, below any bound tried; 64, 256 and 1 024 sent the same bytes, and the World's refusals of
  `a_key_new_to_a_node_or_moving_between_nodes_is_admitted_where_the_limit_has_room` are the same at 256 as before.
  256 sends a node 4 096 grants it has not acknowledged in 8 answers, under a second at one report per 100 ms.
- **Memory:** about 470 B per entry per node (470 MB per 10⁶ keys; estimated 130 B) and 320 B per key with one row at
  a replica, 200 B per further row (estimated 100 B per row). A node's table holds `QuotaConfig::capacity` entries and
  evicts the least recently used idle ones past it; entries in use (permits held, units in the window) stay, so it can
  grow past it. A replica has no cap of its own. A one-node region holds every row of its region.
- **The table:** std `HashMap` with a hand-written 64-bit hasher, so no new dependency: the ids it keys are
  `placement::hash` seeded with the quota wire's `PROTOCOL` already, and it only spreads them into the bits the map
  probes by; its eviction iterates under an `#[expect]` that says why, and the sweep
  walks a vector of each shard's ids. A replica's keys are a `BTreeMap`, since the simulation iterates them, and each
  key's rows a vector sorted by node: a `BTreeMap` there cost 1.7 kB for a key with one row.

## 7. Tests

- **The cores** (`quota/core.rs`: the node's table and the replica) have no I/O and take `now` (like `seed/core.rs` and
  `paxos/core.rs`). `quota/model.rs` enumerates every schedule, within a budget of acts and faults, of 2 nodes, 3
  replicas and `M = 2` on integer ticks, with drift, pauses, loss, duplication, late messages and a replica restart, and
  replays schedules longer than the budgets reach. It checks that what the nodes hold or could still take on their
  grants exceeds `M` by no more than their fallback permits, and that replicas with the same rows plan the same grants.
  Each mutation fails it: a free at the node's hold rather than the lease (drift), the largest grant obeyed (loss,
  lateness, drift, restart), a lower counted when sent (no faults, loss, a replayed restart), a raise grown from usage
  over the limit (no faults, loss), an acknowledgement of a restarted replica's predecessor counted (a replayed
  restart). A young replica's grant obeyed fails only the unit tests: a young replica raises nothing, so obeying it
  admits no more. A view that shows a replica restarted not retargeting its link fails only
  `a_view_showing_a_replica_restarted_points_its_link_at_the_new_incarnation`; a silent candidate asked once only fails
  `a_pull_ends_once_every_candidate_answered_refused_or_left` and the sim, whose lost messages leave a pull waiting.
  Demand of refusals alone fails
  `an_entry_that_asked_asks_while_in_use_so_that_a_key_with_room_does_not_refuse_it_again` and the World's watch of
  `ONE`; demand that adds up what was admitted rather than the most used at once fails
  `a_node_asks_for_the_most_it_holds_at_once_not_for_what_it_took`; demand counted where `check` admits rather than
  where the cost is added fails `a_take_refused_by_a_sibling_limit_adds_no_demand_to_the_limit_that_admitted_it`; and
  what no row needs spread over the rows that ask alone fails
  `a_node_that_uses_a_key_now_and_then_is_not_refused_while_another_uses_it_steadily_and_it_has_room`; room short of the
  raises handed out in node order fails `room_short_of_the_raises_is_split_fairly_not_handed_out_in_node_order` and, in
  about one plain-build run of seed 38 in twenty, the World's
  `partitions_cuts_resets_and_refused_accepts_never_pause_admission`; a row's grant floored at its usage fails
  `a_row_over_its_share_is_planned_the_level_and_the_others_keep_theirs` and that World test at `SIM_SEED=42168`. A
  candidate that picks a pull's keys by its own view, not the puller's, fails
  `a_candidate_serves_the_keys_the_puller_replicates_in_the_pullers_view_not_in_its_own` and, through the driver,
  `axon-obs/tests/health.rs::a_new_quota_replica_pulls_its_keys_from_a_candidate_whose_view_does_not_show_it`. An
  acknowledgement that does not give the room it frees at once fails
  `a_lower_another_row_waits_for_is_pushed_and_its_acknowledgement_gives_the_room_at_once`; a push taken in whatever
  report it answers fails `a_push_is_taken_in_only_where_it_answers_one_of_the_last_two_reports` and, at seeds 0 to 3,
  the World's `partitions_cuts_resets_and_refused_accepts_never_pause_admission` (§2.3); a push acknowledged where the
  replica has not heard the node's usage fails
  `a_push_is_acknowledged_only_where_the_replica_heard_every_change_of_its_entries` and the TLA+ mutation `AckUnheard`,
  but no simulation. An answer that comes after a later one taken for a lost report fails
  `an_answer_that_comes_after_a_later_one_has_nothing_reported_again` (§2.1); an answer that carries every grant the
  node has not acknowledged fails
  `a_node_behind_gets_at_most_resend_grants_it_has_not_acknowledged_a_message_and_every_one_in_the_end`; an
  acknowledgement that ends the holds of grants its message left out fails it,
  `a_lower_left_out_of_a_message_still_counts_the_grant_before_it_however_many_are_acknowledged` and the TLA+ mutation
  `AckLeftOut`; grants that lapsed and count again after a message that leaves them out fail
  `a_grant_that_lapsed_counts_no_more_after_a_message_that_leaves_it_out` and the mutation `LapsedKept`; an answer from
  another incarnation that closes its report fails `an_answer_from_another_incarnation_leaves_its_report_to_be_lost`; a
  report to a silent replica counted as lost fails
  `a_replica_that_stops_answering_has_nothing_reported_again_until_it_answers`. `quota/model.rs` has the floor of §2.5
  but no pushes; the TLA+ model has both.
- **TLA+ (`spec/tla/QuotaGrant.tla`):** one permit key, 2 nodes, 3 replicas, `M = 2`, costs of 1, ticks 0 and 1, `Hold =
  Lease` (the tightest §2.4 allows); `targets`, `regrant`, `Replica::report`, `absorb`, `Link::accept`, `commit` and
  `retarget` and `Entry::bound` are written out as in `core.rs`, and each node's view shows a restarted replica at a
  point of its own. A replica pushes the grant set since its last message where `regrant` flagged the node, at any point
  after (`MaxPushes` of them; the pacing to one grants message a report cycle is not modelled), and a node takes a push
  in or declines it as `Table::pushed` does, and acknowledges it where it took it in, it answers the node's last report
  and nothing changed since. A message may leave a pending entry out where other grants fill it (`MaxLeft` of them, the
  order of `RESEND`'s walk chosen freely), but an answer never leaves out the entry its report named; an acknowledgement
  ends the hold only where its message carried the entry. A node's takes within a tick of the one that made its entry
  may use the floor of §2.5. `WithinLimit` is §5's 0 over the limit for loss, duplication, reorder and a replica
  restart, as `quota/model.rs` states it: what the nodes hold or could still take on the smallest fresh grant of a
  majority exceeds `M` by no more than their fallback permits; `FallbackBound` is `a` per node (§2.5); `MaxMinFair` is
  §2.3's level on the model's own plan, not where remainders go. A node reports each change before the next tick (one
  silent with usage no replica heard of is §5's pause row), and deliveries not faulted happen in one global order.
  Passing, on 4 workers of a shared 24-thread box before pushes and the floor: `QuotaGrant.cfg`, one report or answer
  lost or late and a duplicate, 3 takes (147,722 distinct states, depth 10, 1 min 14 s); `QuotaGrantFaults.cfg`, three
  copies, several in one report, so a node may have one fresh grant (106,711, 1 min 3 s); `QuotaGrantReports.cfg`, three
  reports per node (12,499, 8 s); `QuotaGrantRestart.cfg`, a restart (522,309, depth 14, 2 min 34 s);
  `QuotaGrantPull.cfg`, a restart and two reports lost, with the gap below closed in the model (451,419, 2 min 2 s).
  Under load these took up to three times as long; two faults with takes, three reports with faults, or a restart with a
  fault do not finish in 10 minutes. With them and the bounded re-send, each with one push but `QuotaGrantPush.cfg`,
  three at a time on 4 workers each of a 16-thread build VM shared with builds and tests: `QuotaGrant.cfg` 551,596
  states, depth 12, 10 min; `QuotaGrantFaults.cfg` 118,913, 4 min 24 s; `QuotaGrantReports.cfg` 83,573, 2 min 31 s;
  `QuotaGrantPull.cfg` 505,943, 10 min 54 s, where no push is due; `QuotaGrantPush.cfg`, four pushes, two takes and one
  report, answer or push lost or late, 1,514,879, depth 13, 21 min; `QuotaGrantRestart.cfg` 3,807,685, depth 15, 59 min;
  `QuotaGrantBound.cfg`, three answers or pushes that leave the entry out where other keys fill them (`MaxLeft`),
  381,213, depth 12, 6 min 37 s. `QuotaGrantWitness.cfg`, run with `-continue` and stopped once each of its seven
  witnesses (one fresh grant, a pulled hold, a replaced incarnation's grant still counted, ...) was reported violated,
  within a minute, stands in for `-coverage`, which runs out of heap on this spec. Run `java -cp tla2tools.jar tlc2.TLC
  -workers 4 -config <cfg> QuotaGrant` in `spec/tla`; each `mutations/QuotaGrant-<Bug>.cfg` ends with an invariant
  violated within about two and a half minutes: a row floored at its usage (b78e21b, `MaxMinFair`), and for
  `WithinLimit` a lower counted when sent, a free before the node's hold, the largest grant obeyed, a raise grown from
  usage over the limit (the `model.rs` mutation; before f1997e9 a raise grew from held), one fresh grant enough, a grant
  held from its arrival or still at its until, the older hold generation dropped, an acknowledgement one message early,
  a pulled row with no hold, an acknowledgement that ends the hold of an entry its message left out (`AckLeftOut`), a
  grant that lapsed counted again after a message that leaves it out (`LapsedKept`), a push counted as acknowledged when
  it is sent, a push acknowledged where the replica has not heard the node's usage, and the floor not counted as the
  fallback. That last but one is how the model found the gap §2.3 closes: `QuotaGrantPush.cfg` ended with `WithinLimit`
  violated at depth 12 (887,515 states) where a node acknowledged a push whatever it had taken since its last report.
  These three fail only where the bounds reach a push and the floor, which the witnesses do not cover; `AckLeftOut` and
  `LapsedKept` only where a message leaves an entry out. Not caught within 15 minutes, so without a config: a stale
  grants message accepted, an acknowledgement counted for a predecessor incarnation (f1997e9), and a pulled row carrying
  the grant instead of the hold. Nor, since the lapse `LapsedKept` checks, a free not marked pending: the lapse drops
  the old grant too (83,927 states, depth 12, no violation; with the lapse also off, `WithinLimit` violated at depth 10,
  5,055 states). `QuotaGrantPulledRaise.cfg`, the protocol as built with a restart and two reports lost, ends with
  `WithinLimit` violated (86,788 states, depth 9, 96 s). One trace, in 8 states, found before pushes: r1 and r2 each
  missed one node's report and granted the other all of `M`; r3, which heard both, had lowered node 1 and given node 2
  nothing, then restarts and pulls holds of `M` for both; over the limit, `regrant` grows each grant from that hold, to
  1; node 2, whose view shows the restart, admits 1 on r1 and the new r3, while node 1, whose view lags, still admits 2
  on r2 and the old r3. Not fixed yet: `QuotaGrantPull.cfg` closes it in the model only (`PulledRaise = FALSE`).
- **`quota/sim.rs`:** the cores of 50 nodes in 3 or 4 regions on the testkit `Simulator`, replicas chosen as §2.2
  says, each process's view lagging the truth by up to 1.5 s, messages lost, duplicated, reordered and late; crashes
  (of any node, of a replica of the hot key, of all its replicas at once), restarts as new incarnations, pauses of
  0.3–20 s, 1 % drift, partitions into 2 to 4 sides and a replica cut from the other regions. Every node draws Zipf
  keys over a rate, a weighted rate with `charge` and two permit limits, while a hot key's demand, above its limits,
  moves between regions. The oracle knows every unit admitted and checks at each admission, over sliding windows,
  that a key admits no more than its limit, its fallback and charges and the rows of the faults in effect. Outside
  faults it requires the hot key to hold 95 % of what its limit allows: `8/9 · M` for a rate (§2.1's ring), `M` for
  permits, and 75 % for permits in grants of 1 or 2. It also bounds the fallback: a node admits at most `a` on it
  over a window or held, and only while fewer than a majority of the key's replicas could answer it, or within 1.5 s
  of what §2.5 lists. A second test replays the rig's episode of 2026-10-01 on five nodes over ordered links: the
  links' queues fill for 12 s, one node is killed in it and replaced, and each node finds peers unreachable on its
  own; it held 0.25·M over seeds 0..512 and failed at once with replicas chosen among reachable nodes (§2.2).
  `SIM_SEED=n`, `SEEDS=a..b`, `SIM_DEBUG=1`; a same-seed-twice test.
- **World (`tests/world_quota.rs`)**, each asserting no pause in admission, by `take` and by `acquire` of a second
  limit's permit on the key, dropped at once (a limit with room), `stats().permits` back to 0 and replica rows empty at
  the end: `Crash::Process` and `Crash::Machine`, a rolling restart at WAN latency, blue/green under load, pauses of 3 s
  and 20 s, `starve`, `drift`, `step_wall` then a restart, a `partition` into regions and back, `cut`, one replica cut
  from the other regions, `reset`, `refuse_accepts`, and eviction with more keys than fit. On three nodes that each
  replicate every key, `a_key_new_to_a_node_or_moving_between_nodes_is_admitted_where_the_limit_has_room` counts the
  `Denied::Share` refusals of a key new to the cluster, of a key new to a node that another node used last, and where
  use moves from one node to another and back after it took all of the limit, the longest run of them, and those whose
  `retry_after` came before the share. Before pushes and the floor (seeds 0 to 3): 0, 44 over 0.86 s, 3796 over 1.9 s
  and 56 to 61 over 1.1 to 1.2 s, and 3594 and 50 to 55 of them said to retry too soon. Since (seeds 0 to 15): 0, 0,
  114 over 50 ms and 5 over 100 ms, none too soon.
- **[spec/faults.md](faults.md):** a `cluster: quotas` section citing every row; disk rows are n/a (nothing is written).
- **The rig ([fault-rig.md](fault-rig.md)):** a mode `quota` with three regions (b1-2, b3-4, b5) and an inter-region
  delay added to each episode's netem. `check.py` checks each episode against its row in §5, no pause in admission,
  and outside episodes the hot key holding 95 % of what its limit allows (`8/9 · M` for a rate, `M` for permits, 75 %
  in grants of 1 or 2), with fallback only as §2.5 and the rows of §5 allow.
- **Benchmark:** `--example quota`, every node in one process: the check on 1, 4, 12 and 24 threads at 10³, 10⁶ and
  10⁷ keys, uniform, Zipf and one hot key, `take` with one and two limits and `acquire` with its drop; memory per entry
  and per row; bytes a second at 50 nodes; and a handover's pull, then a new process's, at 10⁶ keys. Recorded in
  `benchmarks.md`.

## 8. Left out

- Taking back what was admitted over a grant (revoking permits, trimming).
- Scaling a partitioned side down to its share of the nodes: each side gets the full limit.
- Day and calendar windows, durable windows, waiting acquires, refunds, token-bucket smoothing.
- A console view: `stats()` and `describe()` feed telemetry; a page comes with the console views item if wanted.

## 9. Decisions

| Question | Decision (user, 2026-09-26) |
|---|---|
| Route each key to one place? | No: an API key is used from many regions, each request served nearby. |
| Exact? | No: approximate, and no request waits on the network. Billing is separate. |
| One failure? | Must not pause admission. |
| When it fails | Fail open, and only after an outage in every region or the node's own isolation. |
| Take back over-admitted permits? | No. |
| A partitioned side | Gets the full limit. |
| Windows | 1 s to 1 h; no daily limits. |
| Weighted costs | Yes: per-limit costs, `Denied::Share`, `charge`. |
| One primitive or two | One: rates and permits differ only in how a unit comes back. |
| Ship with two benchmark numbers far from §6? | Yes, for now (2026-09-28): about 470 B per entry per node, and a node reports at most about 41 k entries a second, so keeps grants for about 4.9 M rate entries in use (10⁷ did not converge). Shrinking the entry and raising what a node reports a cycle is a stage-4 performance item. |
