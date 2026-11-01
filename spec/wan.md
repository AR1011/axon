# Design — running across regions

How one axon cluster runs across regions that are 90 to 220 ms apart. Of the shipped defaults only `ClusterConfig::new`'s
`connect_timeout` changes, to 1.5 s (§4). Status: built.

The values here are worked out for an example cluster with four sites: eu-a and eu-b in one data centre, sin and us. Three of the nodes are Paxos
acceptors: eu-a, sin and us. The peak stream feed is about 13 MB/s.

| Link | Round trip |
|---|---|
| eu-a ↔ eu-b | 0.24 ms |
| EU ↔ us | 92 ms (91–93) |
| EU ↔ sin | about 190 ms (185–190) |
| sin ↔ us | 219 ms |

Two other specs cover the rest, and this file does not repeat them:
[push-window.md](push-window.md) (several stream pushes in flight per parent and child, and the window's ceiling) and
[lanes.md](lanes.md) (one connection per lane per peer, and the limits of each lane).

## Terms

| Term | Meaning here |
|---|---|
| RTT | round trip time between two nodes |
| SYN | the first packet of a TCP connect. Linux sends it again at 1 s and 3 s after the first, then 7 s |
| RTO | TCP's retransmit timeout. Linux keeps it at least 200 ms, so it is about RTT + 200 ms, and it doubles with each loss in a row |
| World | the seeded whole-cluster simulator in `axon-testkit` ([simulation.md](simulation.md)) |
| digest | the hash of a World run's trace. Two processes on one seed must give the same digest |
| lease, hold | `PaxosConfig::lease` (default 6 s). The hold is what the holder may assume: lease × (1 − drift) − 200 ms (`ownership.rs:11,15-17`) |
| lease_grace | how long after the last renew it sent a holder keeps its key. Default 7/8 of the hold (`cluster.rs:380`) |
| patience | how long a Paxos round waits for one acceptor: lease / 6 (`paxos/mod.rs:73-75`). A round is 4 × patience (`paxos/mod.rs:358`) |
| heartbeat_grace | how long discovery may fail to hear a node before it checks whether it is isolated (`cluster.rs:115`, default 5 s) |
| isolated | a node that discovery cannot hear and that no peer vouches for. It refuses starts and `/readyz` answers 503 |
| sweep | `spawn.rs:418-423`: stop every keyed actor on this node at once |
| drops_after | new: how soon after a node's discovery link goes quiet a peer can drop it from its view |
| work item | one of the steps this was built in: A1, A2, ... Not a transport lane |

## 1. Scope

In scope:

- The World dials at the full round trip and can lose a SYN (§3).
- `connect_timeout`, a setting apart from the keepalive (§4).
- Seed timings as settings, checked between seeds and nodes (§5).
- Stream relay timings per namespace (§6).
- Isolation that does not flap, and leaves leased keys to their fences (§7).
- Host TCP settings, and one start-up event when a host's limits are below the push window (§8).
- The values across regions, derived (§9), the guide section that carries them (§10), and the tests that hold them (§11, §12).

Out of scope: the push window and lanes (their own specs), QUIC, one timing set per link or per peer, and timings
that adapt to the measured RTT (§16).

Order: A1 first. It changes every digest once, and must land before [lanes.md](lanes.md)'s bandwidth model, which
must keep them. Then A2, then A6. C1 comes after A6, and after B2 (reachability from the Control lane alone,
[lanes.md §2.2](lanes.md#22-reachability-from-control-alone)). A4's `RelayTimings` is that item's first commit, before
the push core. D1 and D3 come last.

## 2. What breaks today at these round trips

Each line is checked against main at 269b67cf.

1. **The World cannot show a slow or lost connect.** A dial's result is set when the SYN arrives, one way
   (`world.rs:1358-1400`). Nothing sends a SYN again. A test at 219 ms RTT connects in 110 ms.
2. **A lost SYN marks a peer unreachable.** A dial waits for its TCP connect one keepalive at most
   (`net.rs:57-59`). `ClusterConfig::new` sets the keepalive to 1 s (`cluster.rs:95`), and Linux sends a lost SYN
   again at exactly 1 s. So one lost SYN fails the dial, and a failed dial marks the peer unreachable at once
   (`engine.rs:1185-1190`). After a session that went silent, the redial also gives up after one keepalive
   (`engine.rs:1135-1142`). This is true on a LAN too.
3. **Seed timings are constants.** Keepalive 1 s and idle 4 s (`seed/mod.rs:27-28`), warm-up 5 s, closed grace
   2 s, idle hold 5 s (`seed/core.rs:38-40`). At 219 ms, three losses in a row of one segment take
   0.42 + 0.84 + 1.68 = 2.9 s, so idle 4 s is close. A seed and a node cannot tell that their timings differ.
4. **Paxos patience covers one retransmit, not two.** At the 6 s lease, patience is 1 s. An ask across sin ↔ us with
   one retransmit takes 0.22 + 0.42 + 0.22 ≈ 0.86 s.
5. **Relay timings are per node.** `upstream_timeout` 3 s and `hold_down` 10 s (`core.rs:228,221`) hold for every
   namespace. 3 s is shorter than one cross-region redial.
6. **Isolation sweeps and flaps.** When no peer answers, `check_isolation` sweeps every keyed actor
   (`spawn.rs:1440-1445`), leased or not. One heard heartbeat ends it (`cluster.rs:1202-1206`, `spawn.rs:463-466`).
   A node that is heard now and then goes in and out of isolation, and each time in it stops every key.
7. **The host caps one connection.** Linux's default `tcp_wmem` max is 4 MiB. build-1 has
   `tcp_wmem 4096 16384 4194304` and cubic. One connection then moves at most 4 MiB / 219 ms ≈ 19 MB/s, whatever
   axon's window.

## 3. The World dials at the round trip (work item A1)

**Change.** In `crates/axon-testkit/src/world.rs`:

- `syn` (line 1358) still accepts on the far node when the SYN arrives. It no longer sets the dial's result. It posts
  the answer back as a chunk of its own: a SYN-ACK, or the refusal. The dial completes, or fails, when that chunk
  arrives, one round trip after it started.
- The answer goes from the far end of the new connection, so it takes the reverse wire's latency and jitter, and the
  far node's first bytes queue behind it (`post`, line 1265, keeps each end's order).
- A cut either way still holds a SYN (`cut`, line 1258). A cut back towards the dialler also holds the answer.
- New field `Wire::syn_stall`, default 1 s: the kernel's first SYN resend. A SYN, or its answer, that draws a stall
  (`stall_every`) waits `syn_stall`, not `stall`. Every other chunk still waits `stall`.
- A test that wants every SYN lost once sets `stall_every: 1, stall: Duration::ZERO, syn_stall: 1 s`. No new World
  method.
- The five `Wire` values that set `stall_every` today (`world_faults.rs:1456,1571,1907,2103` in axon-cluster and
  `world_relay.rs:470`) set `syn_stall` to their `stall`. Their tests then mean what they meant. So does the
  axon-testkit one (`tests/world.rs:535`), which tests stalls on bytes: with the 1 s default, a SYN-ACK that draws a
  stall lets b's 100 ms sleep end before a writes, and b reads before a's window fills (seed 10 of `SEEDS=0..256`).
- `scripts/check.sh timing` (`check.sh:96-125`) scans `pub struct ([A-Za-z]*Config|Backoff)`. It widens to
  `*Timings` too, so `SeedTimings` and `RelayTimings` (§5, §6) need a `//` reason where a test shortens them.

Every digest changes once here. Two later items change them again where they run: push-window's keepalive at session
start (A5, [push-window.md §4](push-window.md#4-the-call-split-in-axon-cluster-a5)) and the Live and Bulk dials of B2.
The bandwidth model (A1, [lanes.md §7.1](lanes.md#71-bandwidth)) must not. No test pins a digest value (checked: no
`digest()` is compared to a literal), so only the two-process match of `scripts/check.sh sim` holds them, and it still
does.

**Proof.**

Tests that fail on main and pass after:

- `crates/axon-testkit/tests/world.rs::a_dial_completes_one_round_trip_after_it_starts`: a → b 20 ms, b → a 30 ms,
  no jitter. The far node accepts at 20 ms, the dial completes at 50 ms, and a refusal from a crashed process comes
  back at 50 ms too. On main the dial completes at 20 ms.
- `crates/axon-testkit/tests/world.rs::a_syn_that_draws_a_stall_waits_syn_stall_and_the_bytes_after_it_do_not`:
  `stall_every: 1, stall: 0, syn_stall: 1 s` at 10 ms each way. `stall_every: 1` stalls every chunk, so the SYN and its
  answer wait 1 s each and the dial completes at 2.02 s, and a 1 MiB write after it takes no stall. On main it does not
  compile: the field is new.
- `crates/axon-testkit/tests/world_faults.rs::a_partition_holds_bytes_and_dials_both_ways_and_they_flow_on_the_heal`
  changes: its refusal on a b > a 1 ms wire with up to 1 ms of jitter, where a > b is 20 ms, comes back after 22 to
  23 ms (a quantum for the SYN to leave, then each way), not `back < 10 ms`
  (lines 55-100). It asserts `20 ms <= back < 25 ms`. On main `back` is about 1 ms and fails the lower bound.

spec/faults.md, the "In the World" column:

- loss: add `crates/axon-testkit/tests/world.rs::a_syn_that_draws_a_stall_waits_syn_stall_and_the_bytes_after_it_do_not`.
- latency: add `crates/axon-testkit/tests/world.rs::a_dial_completes_one_round_trip_after_it_starts`.

Deletes: nothing. The `Wire` doc comment for `stall` says "a SYN waits `syn_stall`".

## 4. `connect_timeout` apart from the keepalive (work item A2)

**Change.**

- New field `axon_net::Config::connect_timeout`. It bounds the TCP connect of a dial, within `handshake_timeout`.
  Default 5 s, which is today's `min(handshake_timeout, keepalive_interval)` for a plain axon-net config (5 s and
  10 s, `config.rs:182,185`).
- `validate` (`config.rs:193-210`) adds: non-zero, and `connect_timeout <= handshake_timeout`.
- `net.rs:59` reads `deadline.min(now + cfg.connect_timeout)`.
- `ClusterConfig::new` sets it to 1.5 s beside the keepalive (`cluster.rs:95-96`), and its doc comment says so.
  Linux sends a lost SYN again at exactly 1 s, so today's 1 s fails a dial on one lost SYN, on a LAN too (§2, item 2).
  1.5 s holds one lost SYN on any link under 500 ms. What it costs: a dead machine is marked unreachable 0.5 s later,
  4.5 to 5.5 s after it died, not 4 to 5 s (idle 4 s from its last frame, which is up to a keepalive before it died,
  then the redial's 1.5 s). It is the one shipped default this design changes.
- The test helpers that shorten the keepalive set `connect_timeout` to it, with a `//` reason, so their bounds mean
  what they meant: `quick` in `crates/axon-cluster/tests/world_faults.rs:1330` and
  `crates/axon-cluster/tests/cluster/membership.rs:131` (500 ms), and `crates/axon-obs/tests/faults.rs:147` (300 ms).
- The redial after a silent session gives up after `connect_timeout`, not `keepalive_interval`
  (`engine.rs:1137`). The give-up is "this peer has not answered a connect": it belongs to the connect. After B2 only
  the Control lane's slot runs it ([lanes.md §2.2](lanes.md#22-reachability-from-control-alone)); every lane's dial
  waits `connect_timeout`, and a Live or Bulk dial past it fails only that lane's calls.
- `Cluster::start` lowers the keepalive to a quarter of a short hold (`cluster.rs:393-396`). It lowers
  `connect_timeout` to the same bound, `min(connect_timeout, hold / 4)`, so a dead holder is noticed as fast as
  today.
- Seed links take the `connect_timeout` of `SeedConfig::transport`, which `SeedConfig::new` sets to 1 s (or the
  handshake timeout where shorter), so the shipped seed links are unchanged. A failed seed dial costs no peer's
  reachability, but it is not free: a seed drops a node whose redial has not connected within `closed_grace`. Across
  regions set it as the cluster's, 3.5 s (§9). Until D4 `seed::transport` set `connect_timeout = KEEPALIVE` whatever
  the transport said, and a node whose seed link's SYN and its answer were each lost once never reconnected to that
  seed while the loss lasted (`world_wan.rs::a_seed_link_redials_across_a_lost_syn_within_the_connect_timeout_and_keeps_the_node_listed`).
- The keepalive's doc line "Also how long a dial waits for its TCP connect" (`config.rs:157`) goes.

**Proof.**

Tests that fail on main and pass after:

- `crates/axon-net/tests/session.rs::a_dial_waits_connect_timeout_for_its_connect_and_handshake_timeout_for_the_rest`:
  a `Network` whose connect hangs; keepalive 1 s, `connect_timeout` 2 s. The dial fails with `Timeout::Connect` at
  2 s. A connect that succeeds at 1.5 s and a handshake that hangs fail with `Timeout::Handshake` at
  `handshake_timeout`. `connect_timeout` above `handshake_timeout` is refused, `Error::Config`. On main the first
  dial fails at 1 s.
- `crates/axon-cluster/tests/world_faults.rs::a_peer_whose_syn_is_lost_once_after_a_silent_session_stays_reachable`:
  two nodes 46 ms apart (the EU ↔ us link), the shipped keepalive 1 s and idle 4 s, `connect_timeout` 3.5 s (`//`
  the WAN value of §9). A partition of idle + 3 s makes the session idle out; the redial starts in the cut, waits 2
  to 3 s for its connect, as through lost SYNs, and connects after the heal. The view keeps b reachable throughout
  (sampled every 50 ms), `became_unreachable` stays 0, and a request after the heal answers. On main the redial gives
  up after 1 s and b goes unreachable at 6.3 s. It holds the dial with a cut, not A1's `syn_stall`, so it runs
  without A1.
- `crates/axon-cluster/tests/world_faults.rs::a_lost_syn_costs_no_reachability_under_cluster_config_new`: two nodes
  on the World's default wire, on `ClusterConfig::new`'s timings, none changed. A cut a → b of 1.2 s holds a's first
  dial as a SYN lost once and sent again at 1 s would. The call connects on the heal, b stays reachable (sampled every
  50 ms), `became_unreachable` and `dial_failures` stay 0, and the call answers. On main the dial fails at 1 s and b
  is unreachable at 1.09 s. It holds the shipped default, as the test above holds the setting.
- `crates/axon-cluster/tests/world_faults.rs::a_distant_peer_whose_handshake_outlasts_a_keepalive_stays_reachable`
  (line 444) stays, now at a full round trip. It is the guard that the handshake keeps the rest of its budget.

Guards that pass before and after: `crates/axon-cluster/tests/world_faults.rs::a_peer_that_stops_answering_is_unreachable_without_waiting_for_a_handshake`
(line 417: idle 1.5 s and the redial's give-up, about 2 s, under its 3.5 s bound, since `quick` sets `connect_timeout`
to its 500 ms keepalive) and
`crates/axon-net/tests/session.rs::a_silent_network_times_out_dials_and_ends_listeners` (line 651).

spec/faults.md:

- transport: sessions and keepalive, loss: add the session.rs test,
  `crates/axon-cluster/tests/world_faults.rs::a_peer_whose_syn_is_lost_once_after_a_silent_session_stays_reachable`
  and `crates/axon-cluster/tests/world_faults.rs::a_lost_syn_costs_no_reachability_under_cluster_config_new`.
- transport: sessions and keepalive, pause (a silent session): add the same World test.
- cluster: membership, loss and latency: add the same World test.

Docs that change with it:

- The guide's sentence "A dial's TCP connect gets one keepalive too: ... raise it for such links"
  (`cluster.md:2086-2088`) becomes: "`ClusterConfig::new` sets `connect_timeout` to 1.5 s, so one lost SYN costs a
  dial nothing; raise it for links over 500 ms or that lose more (see Across regions)".
- `cluster.md` § Membership (`:1410-1414`): a paused or cut-off node is marked unreachable "in about 5.5 s", and a
  node that never called it counts it unreachable when the connection is refused or not accepted "within
  `connect_timeout`, 1.5 s", not "within the keepalive".
- `cluster.md:185`: a `NamedRef`'s silent node fails it "until that node is marked unreachable, about 5.5 s".
- The README's `Config::connect_timeout` line (§13).
- The comment at `net.rs:57-58` says `connect_timeout`.

## 5. Seed timings as settings (work item A6)

**Change.**

```rust
pub struct SeedTimings {
    pub idle: Duration,         // a seed session with nothing received for this long closes (today IDLE, 4 s)
    pub idle_hold: Duration,    // a seed keeps a member whose session idled out this long more (IDLE_HOLD, 5 s)
    pub closed_grace: Duration, // ... and one whose session closed cleanly (CLOSED_GRACE, 2 s)
    pub warm_up: Duration,      // a new seed withholds SYNCED, and a node that attached again stays frozen (WARM_UP, 5 s)
}
```

- Defaults are today's values, so nothing changes for a user who does not set them.
- The rest stay constants, each for a reason: keepalive 1 s (a ping at 219 ms costs nothing), `WITHDRAW_AFTER`,
  `RETRY`, `BATCH`, `OBSERVER_AFTER`, `REGISTER_WAIT`, `THAW_WAIT`, `AHEAD_MS` (`seed/core.rs:41-45`,
  `seed/mod.rs:32-33`). None of them changes with the RTT.
- Checked where they are set, `Error::Config` from `SeedServer::bind` and `DiscoveryError::Invalid` from
  `SeedDiscovery::start` otherwise:
  - `idle > 2 × keepalive`: a session lives through one lost ping.
  - `idle_hold >= closed_grace`.
  - `idle_hold > 2 × keepalive`: `drops_after` (below) is above zero.
  - `warm_up >= 3 s`: a node's redial lands within it (the seed dial's backoff cap 2 s and its 1 s connect). The
    comment at `seed/core.rs:36-37` already says "at most about 3 s". `SeedDiscovery::start` also refuses a `warm_up`
    under 2 s more than its transport's `connect_timeout` (D4).
  - `closed_grace > 0`, and each at most 1 h (an instant past it overflows; the `SYNCED` ms are a u32).
- `SeedServer::bind(addr, &transport, tls, accept)` (`seed/mod.rs:89-110`) becomes
  `SeedServer::bind(SeedServerConfig::new(addr, &transport))`, with fields `tls`, `accept` and `timings`.
  `SeedConfig` (`seed/mod.rs:215-226`) gains `timings`.
- The seed protocol goes to `VERSION = 2` (`seed/core.rs:16`). A hard cutover: a v1 frame closes the session, as any
  other version does today (`seed/core.rs:221,570`). Stop every seed and node at once.
- `SYNCED` (`seed/core.rs:21`), empty today (`seed/core.rs:213,293`), carries the seed's four timings in ms, u32
  big-endian each, as every seed frame is ([seed-discovery.md § 4.1](seed-discovery.md#41-frames)).
- A node compares them with its own on each `SYNCED` (`seed/core.rs:609-615`). When they differ it does not count
  that seed. The seed stays in the denominator, so the node needs a majority of the seeds that agree. Its list stays
  out of the view. The node sends it a `LEAVE`, since it registered during the warm-up. `SeedLinkStats` shows the seed
  in a new state, `timings`, and the node records one event, `seed_timings`, naming the seed and both sets. It keeps
  the seed's timings across sessions, so it does not register again, keeps the redial's backoff, and records the
  event again only when they change; a `SYNCED` with its own clears them. A second `SYNCED` in a session closes it.
- Changing the timings means stopping every node and seed at once, as changing a Paxos lease does.

**`drops_after`.** New `Discovery::drops_after() -> Option<Duration>`, default `None`. Seeds answer
`idle_hold − 2 × keepalive`: the two ends of a session see its loss at most two keepalive ticks apart
([seed-discovery.md § 2.2](seed-discovery.md#22-timing-against-the-clusters-isolation-check)), and a seed then holds
the member `idle_hold`. Memory, Static and Consul answer `None`: Consul's removal is its TTL, outside axon.

`Cluster::start` refuses `heartbeat_interval + heartbeat_grace × 3/2 >= drops_after` when the cluster has no
ownership authority: that node would isolate after its peers dropped it, and its keyed actors would run twice. The
error is `ClusterError::Config("heartbeat_interval + 3/2 heartbeat_grace must be under the discovery's drops_after
without an ownership authority")`, beside the other start checks (`cluster.rs:374-388`). The sum is § 2.2's bound on
the stop (one interval of tick rounding, the grace, half the grace for the peer check). At the guide's 500 ms / 1 s
it is 2 s, under the default 3 s. With an authority the lease fences the key, so nothing is refused (§7).

Corrected in C1: a one-way cut of what reaches the node bounds the drop tighter than `drops_after`. The node's
sessions idle out and it closes them, its close reaches the seeds, and they remove it `closed_grace` (2 s) later.
So the bound is `min(idle_hold − 2 s, closed_grace)`, 2 s at the defaults, equal to the guide's sum rather than above
it: the removal must still reach a peer's view and a caller, but there is no margin. `drops_after` stays as it is,
because the true bound would refuse the guide's 500 ms / 1 s at start, and a longer default `closed_grace` delays
every crashed process's removal (and moves the timings `SeedMembership.tla` checks). Without an authority this is
the same class as a cut from the seeds alone ([seed-discovery.md § 2.2](seed-discovery.md)); with one the lease
fences the key.

The tree already passes it: every test helper and example that starts a cluster on seeds without an authority sets
500 ms / 1 s (`crates/axon-cluster/tests/cluster/main.rs:195-196`, `tests/accounts/mod.rs:207`,
`tests/world_faults.rs:128`, `tests/world_quota.rs:73`, `crates/axon-stream/tests/world.rs:64` and
`tests/common/mod.rs:100`, `crates/axon-obs/examples/scale.rs:365`); `relay_wan.rs` and the rig run `PaxosLeases`.

Docs that change with it: the guide's bullet "Set `heartbeat_interval = 500 ms` and `heartbeat_grace = 1 s` with this
provider" (`cluster.md:1514-1517`) says that without an authority `Cluster::start` refuses heartbeats that would stop
the keyed actors after the seeds drop the node, and names the sum; the README's `with seeds:` line (§13); and the
same bullet in [seed-discovery.md § 2.2](seed-discovery.md#22-timing-against-the-clusters-isolation-check)
(line 78).

**TLA.** `spec/tla/SeedMembership.tla` writes six design numbers as literals today. They become formulas of the
constants, and at today's values they give the same numbers:

| Name | Today | Formula |
|---|---|---|
| IdleSeen | 3 | Idle − 1 |
| LeaveGrace | 2 | CloseGrace |
| ProcessGone | 3 | CloseGrace + 1 |
| SeedsRemove | 8 | Idle + IdleHold − 1 |
| DeadGone | 11 | Idle + IdleHold + 2 |
| MinorityGone | 10 | Idle + WithdrawAfter + 2 |

- A new constant `Odd`, a set of seeds with a short `IdleHold` that nodes do not count. `IdleHeld` (line 597) then
  ranges over the seeds nodes count. The timing mutations (`HoldZero`, `ShortHold`, `DropAtInterval`,
  `ProcessAsIdle`, `CloseZero`, `NoWithdraw`) become `Bug` values: as constants they would move the promises with them
  and pass (`NoWithdraw`'s `WithdrawAfter = 99` passed in 7.69 M states before it became one).
- `spec/tla/SeedMembership-Odd.cfg`: 3 seeds, s3 odd. It passes.
- `spec/tla/mutations/SeedMembership-HoldBelowValidated.cfg`: nodes count the odd seed. `IdleHeld` fails.
- `spec/tla/SeedMembership-Wan.cfg`: Idle 8, IdleHold 7, CloseGrace 4, WarmUp 8, RedialMax 6, CutFor {7, 12, 20},
  MaxFaults 1, Settle at least 35. The base config takes 26–33 min, so this one drops to one fault. It passes in
  49,818 distinct states, 15 s, and `-Odd.cfg` in 149,167, 33 s (in the model's spec section). `check.sh tla`'s count, 129 configs, goes to 132 here, and
  to 152 with push-window's `PushWindow` (3 configs, 17 mutations, A3).

**Proof.**

Tests that fail on main and pass after:

- `crates/axon-cluster/tests/seed.rs::a_node_does_not_count_a_seed_with_other_timings`: three seeds, s3 with
  `idle_hold` 3 s. A node attaches to all three and is live through s1 and s2. Stop s2: the node is not live (one of
  three agree), `SeedLinkStats` shows s3 as `timings`, and s3 lists no node. On main there are no timings to differ.
- `crates/axon-cluster/tests/seed.rs::seed_timings_that_cannot_work_are_refused`: each of the checks above
  gives `Error::Config` from `SeedServer::bind` and `DiscoveryError::Invalid` from `SeedDiscovery::start`.
- `crates/axon-cluster/tests/seed.rs::a_cluster_without_an_authority_that_would_isolate_after_the_seeds_drop_it_is_refused`:
  seed discovery, no authority, heartbeats 1 s / 5 s: `Cluster::start` is `Err(ClusterError::Config(_))`, its message
  naming `drops_after`. With 500 ms / 1 s it starts; with an authority and 1 s / 5 s it starts. On main all three
  start.
- `crates/axon-cluster/tests/world_faults.rs::a_node_cut_off_from_its_seeds_and_peers_isolates_before_it_is_removed`
  (line 647) gets a twin at the WAN seed timings and heartbeats (§9),
  `a_node_cut_off_from_its_seeds_and_peers_isolates_before_it_is_removed_at_the_wan_timings`: isolated by 12 s,
  removed after 14 s, at least 1.5 s apart.
- `crates/axon-cluster/src/seed/sim.rs`: the seeded seed simulation draws timings per run from the default and the
  WAN set, and one seed in five with an odd `idle_hold`. Its same-seed-twice test holds.
- `crates/axon-cluster/src/seed/sim.rs::synced_bytes_are_pinned`, beside `record_bytes_are_pinned`: the pinned bytes
  of a v2 `SYNCED`.

spec/faults.md, discovery: Seed:

- crash (a seed of the three that agree stops): add `tests/seed.rs::a_node_does_not_count_a_seed_with_other_timings`.
  It restarts no seed, so it is not cited for restart.
- hostile input (a seed that speaks v1, other timings, a `SYNCED` of the wrong length): add
  `seed/sim.rs::a_node_does_not_count_a_seed_with_other_timings`. The pinned-bytes test injects nothing.
- partition: add `a_node_cut_off_from_its_seeds_and_peers_isolates_before_it_is_removed_at_the_wan_timings`. Its
  wire has no latency, so it is not cited for latency; `world_wan.rs` is (§11).
- clock skew or step: add `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions`
  (§11), which steps and drifts a seed's clock.

Deletes: "Constants, not settings: keepalive 1 s, idle timeout 4 s, ..." in [seed-discovery.md § 3](seed-discovery.md)
becomes the list of what stays constant, with `SeedTimings` beside it. `IDLE`, `CLOSED_GRACE`, `IDLE_HOLD` and
`WARM_UP` go as constants (`seed/mod.rs:28`, `seed/core.rs:38-40`). The six TLA literals go.

## 6. Stream relay timings per namespace (work item A4)

**Change.** Of the relay core's timings, two are per key, and so can be per namespace: `upstream_timeout`
(`core.rs:1160,1667,1686-1690`: how long a key is orphaned before its followers are told `OriginLost`, and how long a
stranded key waits for its candidates to settle) and `hold_down` (`core.rs:1652`: how long a better parent must stay
available before a key switches back to it). The others are per parent, not per key: `retry_failed`, `refresh` and
`resync` (`core.rs:1872-1891`), and `retry_refused` follows a parent's refusal (`core.rs:1401,1405`). They stay per
node.

```rust
pub struct RelayTimings {
    pub upstream_timeout: Duration, // default 3 s
    pub hold_down: Duration,        // default 10 s
}
```

- The two fields move out of `CoreConfig` into `CoreConfig::timings: RelayTimings`, the default for every namespace.
- `StreamNodeConfig::relay_timings(namespace, RelayTimings)` sets them for one namespace, as `.limit(namespace, ..)`
  sets its limits.
- `downstream_expiry` (`node.rs`, default 10 s) stays per node: it is per parent and child, not per key. Its start
  check (`node.rs:411-414`) is unchanged.
- One timing set per namespace, not per upstream tier: settled.

**Proof.**

Tests that fail on main and pass after:

- `crates/axon-stream/tests/relay.rs::a_namespace_with_a_longer_upstream_timeout_waits_longer_before_origin_lost`:
  two namespaces, `upstream_timeout` 3 s and 7 s; cut the origin's parent. The first key's follower sees `OriginLost`
  at 3 s, the second's at 7 s, and a heal at 5 s spares the second. On main both are told at 3 s.
- `crates/axon-stream/tests/sim.rs`: the relay simulation draws `RelayTimings` per namespace from the seed, over the
  two namespaces push-window's A4 gives its 8 keys ([push-window.md §10](push-window.md#10-tests)). Its same-seed-twice
  test holds.

Guard: `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`
(line 476) passes unchanged at the defaults.

spec/faults.md, streams: relay core and node: partition and latency: add the relay.rs test and the world_wan.rs tests
of §11.

Deletes: `CoreConfig::upstream_timeout` and `CoreConfig::hold_down` as fields.

## 7. Isolation that does not flap (work item C1)

The item [spec/00](00-overview-and-requirements.md) deferred as "Letting isolation leave leased keys to their fences".

**Change.**

1. In `check_isolation` (`spawn.rs:1417-1446`), when no peer answered (`!listed && !dropped`) and the cluster has an
   ownership authority: set isolated, and do not sweep. Each leased key stays until its fence stops it, within
   `lease_grace` of its last renew, if the cut lasts. It then ends as `lease_lost`, not `isolated`. When a peer
   answered that it dropped this node, or the cluster has no authority, sweep as today.

   ```rust
   if listed && !dropped {
       return self.rejoined();
   }
   self.heard_since.lock().take();
   self.set_isolated(true);
   if dropped || self.ownership.is_none() {
       let swept = self.sweep();
       self.restart_eager(swept);
   }
   ```

2. A start is refused on an isolated node, a key it still runs is not. Today the `Isolated` refusal
   (`spawn.rs:595-597`) comes before the look for a live actor (`spawn.rs:603`). It moves after the loop, beside the
   `can_grant` refusal (`spawn.rs:621`). A caller then reaches a key that is still running. A key whose actor is
   ending, swept or fenced, is refused at once too, not after the wait for its end (up to `operation_timeout`).
3. A node rejoins only once it has been heard for a whole `heartbeat_grace`. New `heard_since: Mutex<Option<Instant>>`
   in the spawner. `rejoined` (`spawn.rs:463-466`) is called on a passing heartbeat (`cluster.rs:1205`) and on a
   peer's vouch, as today. It now sets `heard_since` if it is empty, and ends isolation only when `heard_since` is at
   least `heartbeat_grace` old. A failed check clears it, that is a gap as long as the grace with no vouch. A
   failed heartbeat inside the grace does not: a node heard more often than the grace would not isolate again,
   and clearing on any miss kept it isolated for as long as discovery dropped one heartbeat a grace (found in the
   C1 security review). While not isolated it changes nothing, as today. So `/readyz` stays 503 for one grace
   after the heal, and a node heard less often than the grace stays isolated, once, instead of flapping.
4. The `isolated` event's text (`spawn.rs:1455`) stays. `isolation_ended` (`spawn.rs:1457`) says "heard for the grace".
5. Found while building it: every node started with its seeds recorded an `isolated` and `isolation_ended` pair, as
   its heartbeats are not heard until the seeds are past their warm-up. A node that has not yet been heard, nor seen
   its own boot in its view, is only starting: it is isolated as before (starts refused), but records nothing,
   counts nothing, and its first hearing ends it at once. New `joined: AtomicBool` in the spawner, set at the first
   hearing, or as the node isolates if its view lists its own boot then: a node cut before its first heartbeat
   with keys running, as in the lone node's discovery test, still records its isolation.

Why this is safe: the lease, not isolation, keeps a key on one node. `PaxosLeases.tla` and `Handover.tla` check that
no two holders overlap whatever the timing. Isolation only decides how soon a node gives up keys it may still hold.
Under an authority the soonest safe moment is the fence. Without one, the sweep is the only fence, and it stays.

**What it costs.** A node cut from its peers but not from the acceptors keeps its keys while it can renew. Its callers
on the far side cannot reach it, and the key's other nodes get `Held(node)` from the acceptors, so its callers wait
until the cut heals or the lease lapses. That is the same wait as for a paused holder, and the price of one key on one
node.

**No Isolation.tla.** The first plan asked for one if it earns its place. It does not: safety is already the lease's,
checked by the two models above, and the flap is a timing property. The World and discovery tests below show it on the real
code, at real timings. A model would check a copy of four lines.

**Proof.** At the shipped 6 s lease, seed discovery gives no window to test in: a node's seed heartbeat keeps passing
until its seed sessions idle out (`SeedDiscovery::heartbeat` reads local state, `seed/mod.rs:572-578`), so it isolates
6 to 6.5 s after a cut, after `lease_grace` (5.02 s) has ended. The World tests use a lease of 20 s (`//` reason: a
window between isolation and the fence), so `lease_grace` is about 17.1 s.

Tests that fail on main and pass after:

- `crates/axon-cluster/tests/world_faults.rs::a_leased_key_outlives_a_cut_shorter_than_its_lease_while_its_node_is_isolated`:
  seeds on s0, s1, s2, three Paxos acceptors a, b, c, the holder d, heartbeats 500 ms / 1 s. Cut d from every node
  for 7 s: over its isolation at about 5.2 s, under its removal at 8 to 9 s and under `lease_grace`. d reports
  isolated.
  Its key keeps running, same epoch and state, and no `KEYED_ENDS` moves. A local `get_or_spawn` on d of that key
  answers; of a new key it is `Spawn(Isolated)`. After the heal d stays isolated until it has been heard for 1 s.
  On main `spawned` drops to 0 at the sweep and `KEYED_ENDS{isolated}` is 1.
- `crates/axon-cluster/tests/world_faults.rs::a_leased_key_cut_off_past_its_lease_stops_by_its_fence_and_runs_elsewhere`:
  the same, cut for 30 s. The key ends `lease_lost` within `lease_grace` of its last renew, not `isolated`, and runs
  on another node after the acceptors' wait, at a higher epoch. No two epochs run at once. On main it ends
  `isolated`.
- `crates/axon-cluster/tests/cluster/discovery.rs::a_node_heard_now_and_then_stays_isolated_until_heard_for_a_whole_grace`:
  a lone node on `Seldom`, a memory discovery that hears one heartbeat in N, with `quick_heartbeats` (30 ms /
  150 ms). The node isolates. Then discovery answers one heartbeat in ten for 2 s, 300 ms apart, each gap past the
  grace. `isolations` stays 1, and the event log shows one `isolated` and no `isolation_ended`, until discovery
  answers one heartbeat in three, 90 ms apart and inside the grace, for 150 ms. On main `isolations` is 7 and the
  node is not isolated.
- `crates/axon-cluster/tests/world_faults.rs::nodes_started_with_their_seeds_record_no_isolation`: seeds and two
  nodes started at once. On main each node records `isolated`, `isolation_ended` and `isolations` 1 (change 5).

Guards that pass before and after:

- `crates/axon-cluster/tests/world_faults.rs::keyed_actors_move_and_stop_with_their_paxos_leases` (line 758).
- `crates/axon-cluster/tests/cluster/discovery.rs::a_node_cut_off_from_discovery_stops_its_keyed_actors_once_a_peer_has_dropped_it`
  (line 126) and new beside it, its leased twin,
  `a_leased_node_cut_off_from_discovery_stops_its_keyed_actors_once_a_peer_has_dropped_it`: the same with
  `MemoryLeases`. A "dropped" answer still sweeps at once.
- `crates/axon-cluster/tests/cluster/membership.rs::a_node_cut_off_from_its_seeds_and_peers_isolates_before_it_is_removed`
  (line 245, no authority: it still sweeps) and
  `crates/axon-cluster/tests/cluster/discovery.rs::a_lone_node_cut_off_from_discovery_stops_its_keyed_actors_and_hosts_them_again`
  (line 233).
- A starved holder, the original repro (pilot bug 5, commit 69bdffea), folded into the first World test as a second
  phase: cut from its seeds alone, d's LISTED asks time out while its renewals land, and it goes isolated and keeps
  its keys. Starving it alone does not do it: at `w.starve(d, 4)` an answer is only a few ms late, and at one
  quantum in 600 to 900 (tried) a starved node's clock catches up when it is stepped, and the answers delivered
  meanwhile win over the 500 ms timeout, so the peers vouch. The phase also gives d's links to a, b and c 300 to
  400 ms each way, a round trip past the check's 500 ms and well under a Paxos round's patience (3.3 s at the 20 s
  lease), and starves it one quantum in 2 to 4.

spec/faults.md:

- cluster: membership, partition: add the new World and discovery tests.
- cluster: keyed actors, partition: add both World tests (the cut shorter and longer than the lease).
- cluster: keyed actors, CPU starvation: add the first World test (its starved phase).
- leases: Paxos, partition: add both World tests.
- the partition rows that say "stops its keyed actors" for every node are rewritten to "without an authority, or
  once a peer dropped it".

Deletes and rewrites:

- guide `cluster.md:894-903`: "stops its keyed actors" becomes two cases, with and without an authority, and the
  rejoin after a whole grace.
- guide `cluster.md:1514-1516`: "stops its keyed actors within 7 s" applies without an authority. With one, the
  fence stops them.
- [seed-discovery.md § 2.2](seed-discovery.md#22-timing-against-the-clusters-isolation-check): the 5–7 s row says
  "isolates; without an authority it stops its keyed actors".
- README API line for `cluster.isolated()` and the stats line: isolated refuses starts, not calls.
- `spec/00-overview-and-requirements.md`: CL-10 (line 112) says the same, and the deferred item at line 219 goes.

## 8. Host TCP settings

What axon does, and does not:

- **No `SO_SNDBUF` or `SO_RCVBUF`.** Should axon set them where it helps? On Linux it never
  helps: setting either turns off the kernel's autotuning for that socket and caps it at `net.core.wmem_max` or
  `rmem_max` (about 208 KiB by default). axon sets neither today (checked: no `SO_SNDBUF`, `SO_RCVBUF`,
  `TCP_CONGESTION` or `TCP_NOTSENT_LOWAT` in the tree; [lanes.md §4.4](lanes.md#44-tcp_notsent_lowat) owns it).
- **No `TCP_CONGESTION`.** BBR is the host's choice, with the `fq` qdisc it needs.
- **One event at start.** New `axon_net::host_tcp() -> Option<HostTcp>` in `crates/axon-net/src/tcp.rs`, beside
  `TcpHealth`. It reads `/proc/sys/net/ipv4/tcp_wmem`, `tcp_rmem`, `tcp_congestion_control` and
  `tcp_slow_start_after_idle` on Linux, and is `None` elsewhere. `StreamNode::start` records one event,
  `host_tcp_limits`, when `tcp_wmem`'s max is below the push window's ceiling (`max_push_window`,
  [push-window.md §3](push-window.md#3-api)), or `tcp_rmem`'s max below twice it (the window a receiver advertises is
  about half of its buffer). The event names the values and the ceiling. It refuses nothing: a host that never needs
  the full window is fine. `host_tcp` lands in A2; the event and its test land with B1, which owns `StreamNode::start`
  and comes after A4's `max_push_window`.
- `scripts/ctl.sh doctor` stays as it is. It checks a playground on this Mac, where there is no `/proc`.
  `scripts/rig.sh doctor` prints the four values per node.

The guide's host settings, for every node on a cross-region link:

```text
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr
net.ipv4.tcp_slow_start_after_idle = 0
net.ipv4.tcp_wmem = 4096 16384 67108864      # max ≥ the push window's ceiling (64 MiB by default)
net.ipv4.tcp_rmem = 4096 131072 134217728    # max ≥ twice it
```

For a 13 MB/s peak at 219 ms, about 2.8 MB must be in flight. The default 4 MiB `tcp_wmem` max gives about
19 MB/s, and the default 6 MiB `tcp_rmem` max, of which a receiver advertises about half, about 14 MB/s: at most
1.1 × headroom over the peak, and no catch-up after a cut. Hence the settings above.

**Proof.**

- `crates/axon-net/src/tcp.rs` unit test: `parse_host_tcp` reads the four files from a fixture directory, and gives
  `None` for a missing or malformed one. It fails on main: the function is new.
- `crates/axon-stream/src/node/tests.rs::a_host_below_the_push_window_records_one_event_at_start`: a fixture with
  build-1's `tcp_wmem 4096 16384 4194304` gives one `host_tcp_limits` event; one at 64 MiB / 128 MiB gives none.

spec/faults.md: none. A host limit is not a fault class, and the fixture tests are not fault injections.

Deletes: nothing.

## 9. The values across regions

The shipped defaults do not change. These go in the guide (§10), in `world_wan.rs` (§11) and in the rig's WAN profile
(§12). Each value comes from the slowest link, sin ↔ us at 219 ms, whose RTO is about 420 ms.

| Setting | Shipped | WAN | Why |
|---|---|---|---|
| `transport.keepalive_interval` | 1 s (ClusterConfig) | 1 s | a ping per second costs nothing at 219 ms. One for every lane: [lanes.md](lanes.md) leaves a keepalive per lane out |
| `transport.idle_timeout` | 4 s | 8 s | four losses in a row of one segment, 0.42 + 0.84 + 1.68 + 3.36 = 6.3 s, then a ping |
| `transport.connect_timeout` | 1.5 s (ClusterConfig) | 3.5 s | two lost SYNs: the third leaves at 3 s and its answer is back at 3.22 s |
| `SeedConfig::transport.connect_timeout` | 1 s (SeedConfig) | 3.5 s | as the cluster's: a seed drops a node whose redial has not connected within `closed_grace` |
| `transport.handshake_timeout` | 5 s | 5 s | connect 3.5 s, then TLS and the challenge, about 3 round trips, 0.66 s |
| `PaxosConfig::lease` | 6 s | 10 s | patience 1.67 s holds an ask and two retransmits, 0.22 + 0.42 + 0.84 = 1.48 s |
| `PaxosConfig::drift` | 0.01 | 0.01 | no reason to change |
| hold, `lease_grace`, renew | 5.74, 5.02, 2.0 s | 9.7, 8.49, 3.4 s | derived: two renews start inside one grace |
| round (4 × patience) | 4 s | 6.67 s | derived |
| acceptor warming | 6.26 s | 10.3 s | derived: lease × (1 + drift) + 200 ms |
| `heartbeat_interval` / `heartbeat_grace` | 1 s / 5 s (500 ms / 1 s with seeds) | 500 ms / 2 s | the peer check waits half the grace: 1 s holds an ask and one retransmit, 0.64 s; 1 s grace would give 0.5 s |
| `request_timeout` | 5 s | 5 s | a call over a live session is one RTT plus retransmits |
| `operation_timeout` | 5 s | 7 s | it must hold a full Paxos round (6.67 s) plus a round trip. At the shipped lease the round is 4 s |
| `SeedTimings::idle` | 4 s | 8 s | as the transport's idle |
| `SeedTimings::idle_hold` | 5 s | 7 s | `drops_after` = 7 − 2 = 5 s, above 0.5 + 2 × 3/2 = 3.5 s (§5) |
| `SeedTimings::closed_grace` | 2 s | 4 s | a clean close's LEAVE may need retransmits too |
| `SeedTimings::warm_up` | 5 s | 8 s | a node's redial: backoff 2 s, connect up to 3.5 s (3.22 s after two lost SYNs), handshake 0.66 s, and a register, at WAN loss |
| `RelayTimings::upstream_timeout` (cross-region namespaces) | 3 s | 7 s | one redial: backoff 2 s, connect 3.5 s, handshake 0.9 s ≈ 6.4 s |
| `RelayTimings::hold_down` (cross-region namespaces) | 10 s | 30 s | a choice: a parent back for less than twice a dead machine's removal (2 × 15 s) is not worth a switch across regions |
| `StreamNodeConfig::downstream_expiry` | 10 s | 20 s | a child cannot refresh through an idle-out and a redial, 8 + 6.4 s, plus one refresh 2 s ≈ 16.4 s |

The lease and drift are part of the Paxos protocol id (`paxos/mod.rs:77-89`), and the seed timings are checked in
`SYNCED`. Going to these values on a running cluster is one stop of every node.

What the values give:

- A dead machine is unreachable to its peers 10.5 to 11.5 s after it died: idle 8 s from its last frame, then
  `connect_timeout` 3.5 s. At the shipped values 4.5 to 5.5 s (today 4 to 5 s, §4).
- A node cut from everything isolates at about 11.5 s (idle 8 s, interval 0.5 s, grace 2 s, half the grace 1 s), and
  the seeds remove it at 14 to 15 s. Its leased keys stop at 8.49 s, by their fence, before it isolates.
- A dead holder's keys run elsewhere about 14 s after it died: unreachable at 11.5 s (the lease is gone at the
  acceptors by 10.3 s), then a redial cap and a round trip.
- Every renew crosses at least the 92 ms EU ↔ us link. Only eu-a of the three acceptors is in Europe, so a majority
  always holds a non-European one. eu-a and eu-b renew through us at 92 ms; sin and us renew through eu-a at 190 and
  92 ms.
- One timing set per cluster (settled): eu-a ↔ eu-b, 0.24 ms apart, also waits
  11.5 s for a dead machine.
- The first call on a Live or Bulk lane to a peer waits that lane's dial, about 0.9 s on sin ↔ us
  ([lanes.md §2.1](lanes.md#21-one-connection-per-lane)).

## 10. The guide section "Across regions" (work item D1)

In `crates/axon/guide/cluster.md`, after "Backpressure and limits" and before "Simulating a cluster". It is short and
holds one program that the test suite builds.

1. **When you need it.** Round trips over about 50 ms, or links that lose packets. One line on what goes wrong with
   the shipped timings (§2, items 2 to 4).
2. **The program.** One function that sets every value of §9 on `axon_net::Config`, `ClusterConfig`, `PaxosConfig`,
   `SeedConfig` and `SeedServerConfig`, and `StreamNodeConfig::relay_timings` for a cross-region namespace. It is the
   application's function, not axon's.
3. **How the values follow from the slowest round trip.** The table of §9, short: each value as a sum of RTTs, RTOs
   and SYN resends, so a reader with other links can redo it.
4. **What changes for operators.** Dead-machine and isolation times (§9), one stop of every node to change the lease
   or the seed timings, and the `seed_timings` and `host_tcp_limits` events.
5. **Host settings.** The sysctl block of §8, and why axon does not set socket buffers.
6. **Where the acceptors go.** Three regions, one acceptor each. Every renew then crosses at least the shortest link
   between two acceptor regions; no placement makes renewals local unless a majority of acceptors share a region.
7. **Lanes and the push window.** One line each, linking their guide text: `cluster.md` § Lanes and the push window paragraph of
   `streams.md`.

**Proof.** The guide program is a doctest of `axon-obs` (`crates/axon-obs/src/lib.rs`), so a renamed field fails
`check`. `world_wan.rs` uses the same values; a comment in each names the other. Deletes: the guide line of §4.

## 11. `world_wan.rs` (work item D1)

`crates/axon-stream/tests/world_wan.rs`, on 32 seeds (`seeds(0..32)`), shaped like
`crates/axon-stream/tests/world.rs:397-478`. `WorldConfig::window` is 64 MiB with a `//` reason, as in
[push-window.md §10](push-window.md#10-tests): at the default 64 KiB one connection moves 337 KB/s at 190 ms, and the
World, not axon, would set the stream's speed and every call's wait behind it.

The World: five nodes, eu-a, eu-b, sin, us and us-b. Seeds and acceptors on eu-a, sin and us. One-way latency 46 ms
EU ↔ us, 95 ms EU ↔ sin, 109.5 ms sin ↔ us, 0.12 ms inside a site; jitter 1 ms; on cross-region links
`stall_every: 200`, `stall` of the link's RTO and `syn_stall` 1 s. Every timing is §9's WAN value (`//` reason: the
values the guide gives for these links). An eu-a origin streams to followers on sin and us-b; keyed accounts live
everywhere.

Tests:

- `a_cluster_across_three_regions_runs_quiet_on_the_wan_timings`: 120 s with no fault. No `isolated` event, no
  `lease_lost`, no `became_unreachable`, no stream reset. Every record reaches each follower once, in order. Each
  call's p99 is under its RTT + 500 ms.
- `every_call_and_a_live_follower_recover_from_each_fault_across_regions`: one fault per seed, by `seed % 7`:
  sin partitioned from the rest for 5 s (under `lease_grace`) and for 20 s (over the lease); a machine crash of us; a
  process crash of eu-b; a 2 s pause of the sin acceptor; every SYN lost once through a 10 s cut; a node drifting
  ±5000 ppm with a wall step under a minute. After each: every call answers again and the follower moves again within
  `RECOVER` = 15 s (the dead-holder time of §9 plus a margin); no record lost or duplicated; no key on two nodes at
  one epoch. For the 5 s partition: no `lease_lost` on a node that kept a majority of acceptors, and no isolation.
- `the_shipped_timings_mark_a_peer_unreachable_on_a_lost_syn_and_the_wan_timings_do_not`: the 5 s sin partition, with
  every SYN and every answer to one lost once on sin's links (`stall_every: 1, stall: 0, syn_stall: 1 s` both ways),
  once at the shipped values and once at §9's. A redial's answer then lands about 2.2 s after its SYN left: the
  shipped run shows a `became_unreachable` from it, past the 1.5 s `connect_timeout`; the WAN run's 3.5 s holds it and
  shows none. One lost SYN alone (1.2 s) no longer flips the shipped run, as A2's LAN test holds (§4). It keeps the
  reason for §9 in the suite.

Fails on main: it does not compile (`syn_stall`, `connect_timeout`, `SeedTimings`, `RelayTimings`). With A1 alone,
the second test fails on the lost-SYN seeds at the redial, and the third shows the flip in both runs.

Run time is a risk (§15): five nodes, 120 s of virtual time, 32 seeds. A1 measures it in the sim build. If it is over
a minute, the long tests run `seeds(0..8)` in `check` and 32 in `check.sh sim`.

spec/faults.md: the second test goes in the latency, loss, partition, crash, restart, pause and clock skew or step
rows of transport: sessions and keepalive, cluster: membership, cluster: keyed actors, leases: Paxos, discovery: Seed
and streams: relay core and node, where it injects that fault. Under leases: Paxos it is the "slow acceptor" case,
in the pause and latency rows.

## 12. The rig (work item D3)

D3 runs one geo night on build-1 in `RIG_BOX` mode: five nodes b1..b5 in network namespaces on bridge
`rig0` ([fault-rig.md](fault-rig.md)). This section holds the class, the profile, the WAN scenarios and
`isolation-quiet`. The lanes scenarios (`wan-bulk`, `rate-bulk`) and the `live-under-bulk` invariant are in
[lanes.md § The rig (D3)](lanes.md#the-rig-d3); the `firehose` workload and the `stream-throughput` and
`stream-window` invariants in [push-window.md § The rig (D3)](push-window.md#the-rig-d3). All of them run on this
section's `geo` class and WAN profile, in the same night.

**The `geo` class.** `fault.sh geo on|off` installs on each namespace's veth a `prio` qdisc with one `netem` delay per
destination site, filtered by destination address. Map: b1 eu-a, b2 sin, b3 us (the rig's acceptors are b1..b3,
`examples/rig/src/main.rs:213`), b4 eu-b, b5 us-b. One-way delays 46, 95, 109.5 and 0.12 ms as in §11. It claims
the `qdisc` resource as `SOLE`, so `netem`, `asym`, `graylink` and `rate` do not stack with it. `geo rate=<Mbit/s>`
adds a `netem rate` to each of the node's destination bands, inside the same qdisc, for lanes' `rate-bulk`. Tagged
`$(tag)` like the others; `heal` removes it.

**Geo is the backdrop.** At `TIMING=wan`, `run.sh` puts the delays on every node before the nodes start
(`nemesis.py backdrop on`, `fault.sh backdrop on`, a `backdrop` line in `nemesis.jsonl`) and takes them off after they
stop. Put on under running sessions, the delays stalled each stream 10 to 15 s: BBR kept the loopback's round trip,
the push window stayed at its floor and a push was resent (scenarios-20261027-0626-s1). Under the backdrop a
scenario's `geo` marks its span only; one with `rate=` or `loss=` changes those bands in place (`tc qdisc change`),
never their delays, and its off, or `heal`, sets the backdrop's bands back.

**Host settings.** `rig.sh setup` with `RIG_BOX` writes §8's `tcp_wmem`, `tcp_rmem`, `tcp_congestion_control` (bbr)
and `tcp_slow_start_after_idle` in each namespace, where Linux keeps them per namespace. `geo`'s `prio` qdisc takes
the veth's place for `fq`; BBR paces without it. `rig.sh doctor` prints the four values per node, so a night that
ran below the window says so in its summary.

**The rig's WAN profile.** `TIMINGS=wan` in `examples/rig/src/main.rs` sets §9's values. It lives in the example, not
in axon. `T` in `library(T)` is the lease, 10 s.

**Scenarios** in `library(T)`, each a `geo` span on the backdrop:

| Scenario | Faults inside `geo` |
|---|---|
| `geo-quiet` | none, for 20 min |
| `geo-partition-short` | `partition` b2 (sin) for `G − 1 s`, under `lease_grace` |
| `geo-partition-long` | `partition` b2 for `3T` |
| `geo-syndrop` | `syndrop` on b3 through a 10 s `isolate` of b5 |
| `geo-loss` | `geo` with 2 % netem loss on b2's links for 5 min (`graylink` cannot stack with `geo`) |
| `wan-bulk`, `rate-bulk`, `rate-bulk-uncapped` | the `bulk` workload under `geo`, and under `geo rate=150` and `rate=100` ([lanes.md](lanes.md#the-rig-d3)) |

**The `isolation-quiet` invariant** in `check.py`:

| Invariant | Holds |
|---|---|
| `isolation-quiet` | in a quiet window, and through a fault that does not cut a node from every peer for longer than idle − keepalive − `heartbeat_interval` + `heartbeat_grace` (8.5 s, the soonest it can isolate), no node's `isolations` moves; through a fault shorter than `lease_grace`, no `leases_lost` moves on a node that kept a majority of acceptors |

Its plant: `@plant(want=["isolation-quiet"])` on `p_isolation_quiet`, a synthetic run with an isolation in a quiet
window. A real-binary plant, `examples/rig/plants/isolate-early.sh`, closes a session idle after two keepalives, not
after `idle_timeout` (`read_loop` in `crates/axon-net/src/session.rs`). In `geo-partition-short` sin's sessions,
its seeds' too, then close about 2 s into the 7.5 s partition, it isolates under the 8.5 s bound, and the run fails
`isolation-quiet`. Calling `check_isolation` on the first failed heartbeat instead of after `heartbeat_grace` is no
plant: with every node a seed, sin's heartbeat fails only once its seed sessions close idle, about 7 s in, and the
check's asks then take the rest of the partition. The flap itself is held by the discovery test of §7, not by the
rig.

**Proof.** `nemesis.py selftest` checks each new scenario at T = 2, 6 and 10; `doctor faults` runs `geo` on and off, and
`geo rate=50`; `check.py --selftest 0` holds every plant of this section, lanes' and push-window's. A geo night on
build-1 with `WORKLOADS=accounts,streams,eager,echo,bulk,firehose` passes with every invariant, including
`lease-lost-quiet`, `readyz-isolated`, `live-under-bulk`, `stream-throughput` and `stream-window`; each real-binary
plant (`isolate-early.sh`, `bulk-on-live.sh`, `window-floor.sh`, `no-window.sh`) fails its scenario. spec/fault-rig.md
gains the class row, the scenario rows, the workload rows and the invariant rows.

## 13. API

In the README's style, for `crates/axon/README.md`:

```text
axon-net
  Config::connect_timeout   a dial's TCP connect, within handshake_timeout ; default 5 s, 1.5 s from ClusterConfig::new (one lost SYN, resent at 1 s, still connects) ; a connect past it fails the dial ; on the Control lane it marks the peer unreachable, and so does a redial after a silent session that has not connected by then ; on Live or Bulk it fails that lane's calls
  axon_net::host_tcp() -> Option<HostTcp { wmem_max, rmem_max, congestion, slow_start_after_idle }>   this host's TCP limits, from /proc ; None off Linux

axon-cluster
  SeedTimings { idle, idle_hold, closed_grace, warm_up }   default 4, 5, 2, 5 s ; the same on every seed and node: a node does not count a seed with others (SeedLinkStats state "timings", event seed_timings) ; changing them stops every node and seed at once
    SeedServer::bind(SeedServerConfig::new(addr, &transport) { tls, accept, timings }).await? ; SeedConfig::new(seeds, &transport) .timings
  Discovery::drops_after() -> Option<Duration>   how soon a peer can drop a quiet node: seeds idle_hold − 2 s, others None ; without an authority, Cluster::start refuses heartbeat_interval + 3/2 heartbeat_grace at or past it, ClusterError::Config ; Discovery::attach(&events) hands a provider the node's EventLog at start (built in A6: the seed provider records seed_timings there)
  isolated: starts refused, keys already running answered ; without an authority, or once a peer has dropped it, its keyed actors stop at once ; with one, each stops by its lease's fence ; it rejoins once heard for a whole heartbeat_grace
```

The README's existing `with seeds:` line (`README.md:88`) becomes "with seeds: cfg.heartbeat_interval = 500 ms,
cfg.heartbeat_grace = 1 s (without an authority Cluster::start refuses more, Discovery::drops_after)".

```text
axon-stream (feature cluster)
  StreamNodeConfig .relay_timings(namespace, RelayTimings { upstream_timeout, hold_down })   that namespace's keys ; cfg.core.timings the rest's, default 3 s and 10 s
```

## 15. Risks

- **Run time.** `SeedMembership-Wan.cfg` may not finish in an hour even at one fault; then it drops `CutFor` to
  {7, 20}. `world_wan.rs` at 32 seeds may be slow (§11).
- **Tests that set `stall_every`.** With `syn_stall` 1 s and the 1.5 s cluster `connect_timeout`, a test that stalls
  both ways and forgot `syn_stall` sees a dial fail where its SYN and the answer both stall (2 s). §3 lists the five
  sites.
- **Tests that shorten the keepalive.** Their dials and the redial's give-up waited the shortened keepalive; after A2
  they wait `connect_timeout`. The three helpers of §4 set it to their keepalive. A test that shortens the keepalive
  elsewhere waits up to 1 s longer for a silent peer; A2 runs every crate's tests on 64 seeds to find one.
- **A cluster with seeds and no authority** that runs the 1 s / 5 s heartbeat defaults is refused at start after A6.
  In the tree, `tests/cluster/main.rs:195-196` already sets 500 ms / 1 s; a user who copied the defaults gets a
  clear `Config` error naming the sum.
- **Isolated but holding.** A node cut from its peers, not from the acceptors, keeps keys its callers cannot reach
  until the lease lapses or the cut heals (§7, what it costs).

## 16. Not added, and left out

Chosen not to add:

- `ClusterConfig::wan()` or any preset across config structs: rejected. The guide's
  function is the application's.
- Timings per peer or per link: one set per cluster (settled). Per lane: [lanes.md §14](lanes.md#14-left-out) leaves
  it out.
- Timings that adapt to the measured RTT: every bound here is a sum the operator can read; an adaptive one is not.
- A seed keepalive or seed connect setting: neither changes with the RTT (§4, §5).
- `SO_SNDBUF`, `SO_RCVBUF`, `TCP_CONGESTION`: §8.
- A doctor check in `scripts/ctl.sh`: it runs on a Mac.
- `Isolation.tla`: §7.
- `drops_after` for Consul: its removal is its TTL, outside axon (settled).
- Per-namespace `retry_failed`, `refresh`, `resync`, `retry_refused`: they are per parent (§6).
- A two-step rollout of seed v2: hard cutover, pre-1.0.

Left out: QUIC ([lanes.md](lanes.md#46-the-transport-seam)'s `Network::connect(to, lane)` is the seam), a geo run on five
separate boxes, and the clock-fault classes in the rig, which `RIG_BOX` lacks (`world_wan.rs`
covers drift and steps).
