# Design — traffic lanes

Today a node has one session to each peer. A session is one TCP connection, so a 10 MB upload, a lease renewal and a
small request all wait in one queue. On a slow link the queue is the 8 MiB outbox, and a lease round waits behind it.
A lane is a traffic class with its own connection to each peer. There are three: `Control` for axon's own messages,
`Live` for calls, `Bulk` for large transfers. A protocol names its lane. Each lane is dialled the first time something
uses it. Status: built.

Two other specs hold what is not here: [push-window.md](push-window.md) (how many stream pushes go
at once to one child) and [wan.md](wan.md) (the connect timeout, seeds, isolation, and the host's TCP settings). A1, A2, B2, C2, C3 and D2 name
the steps this was built in, not lanes.

## 1. Scope

In scope:

- `Lane { Control, Live, Bulk }`, one session per lane per peer and direction, dialled on first use.
- Wire version 7, whose hello names the lane. Only version 7; no fallback to one lane.
- Reachability decided by `Control` alone.
- Limits, compression, an optional rate cap and `TCP_NOTSENT_LOWAT` per lane.
- `WireProtocol::LANE`, the lane of typed endpoints, raw endpoints, routes and the directory's answer.
- Streams: a lane per namespace.
- Accept limits for three sessions a peer.
- `Network::connect(to, lane)`, the seam a later QUIC transport takes.
- The World: a bandwidth per link, and faults on one lane.
- The console and telemetry per lane.

Out of scope:

- The push window ([push-window.md](push-window.md)).
- Connect timeouts, seeds, isolation and socket buffer sizes ([wan.md](wan.md)).
- QUIC. Only the seam is here (§4.6).
- Priority inside one lane. A lane is first in, first out.

No new dependency. A wire change: version 7 (§4.1), a new route format (§5.3).

## 2. How it works

| Term | Meaning |
|---|---|
| lane | a traffic class; `axon::Lane`, re-exported as `axon_net::Lane` |
| `Control` | axon's own traffic: the directory, spawn, watches, handoff, leases, quota rows, stream interests |
| `Live` | calls: every protocol that names no lane, telemetry, control services |
| `Bulk` | what a protocol or a stream namespace puts there: uploads, large replies, bulk streams |
| session | one authenticated TCP connection, as today; now it also has a lane |
| link | the engine's handle on one session; now one per (node, lane, direction) |

### 2.1 One connection per lane

Each lane to a peer is its own session: its own TCP connection, outbox, inbox and limits, with pings at the cluster's
keepalive (§14). A frame on Bulk never waits in Live's queue, in the kernel's send buffer of Live's socket, or behind
Live's flow control. The lanes share only the path between the hosts.

A lane is dialled the first time a call uses it. `Control` is dialled first. `Live` and `Bulk` are dialled once
`Control` is up to that peer. So the first call on `Live` or `Bulk` to a peer, and the first after that lane's
`idle_link_timeout`, waits for its dial after the directory's answer: a TCP round trip and the handshake's three, about
0.9 s at 219 ms. Later calls do not. An application that never declares `Bulk` never dials it.

**Proof.** `crates/axon-net/tests/session.rs::the_hello_names_the_lane_and_the_responder_echoes_it`: three sessions
between two nodes, one per lane, each `Peer::lane` as dialled. `crates/axon-cluster/tests/world_lanes.rs::bulk_saturating_the_links_costs_no_lease_and_live_calls_stay_fast`
(§5.6), which also asserts that a cluster with no Bulk protocol has no Bulk session in `cluster.stats()`.

### 2.2 Reachability from Control alone

Only the `Control` link marks a node reachable or not (`set_reachable`, today at `engine.rs:1138`, `:1163`, `:1188`).
Only `Control` runs the quiet-redial give-up (`engine.rs:1134-1141`), after `connect_timeout` once
[wan.md §4](wan.md#4-connect_timeout-apart-from-the-keepalive-work-item-a2) lands. Every lane's dial waits
`connect_timeout` for its connect; a `Live` or `Bulk` dial that runs out of it fails only that lane's calls.

- `Control` goes down: the node is unreachable, as today. Its `Live` and `Bulk` sessions to that boot close at once.
  Their calls end as calls on a closed link do today.
- `Live` or `Bulk` goes down: only that lane closes. Its calls end. The node stays reachable, and the lane redials
  with the cluster's backoff.

A lane-only fault is real: a flow hashed onto a dead ECMP path, a middlebox that drops one connection. A node up on
`Control` with a dead `Live` lane keeps its keys: placement reads reachability, which is `Control`'s, and its lease
renews on `Control`. Its `Live` calls fail with `Retry::Later` until the lane redials. The caller sees the dead lane
three ways: `PeerStats` keeps a row for the lane with `down_ms`, how long it has had no session (§8); the console's
peer table shows that row as down; and the node records one `lane_down` event naming the peer and the lane, then one
`lane_up` when it has a session again. Nothing moves the keys by itself: an operator who sees a lane stay down drains
the node, and the keys move as on any drain.

**Proof.** `world_lanes.rs::a_dead_bulk_lane_fails_only_its_calls_and_the_peer_stays_reachable`,
`world_lanes.rs::a_node_with_a_dead_live_lane_keeps_its_keys_and_its_live_calls_retry_later`: `cut_lane(a, b, Live)`
both ways for 10 s while b holds a key under `PaxosLeases`; the key stays on b at its epoch, a's calls to it fail with
`Retry::Later`, a's `PeerStats` row for (b, Live) has `down_ms` above zero, `cluster.events()` holds one `lane_down`
and, after the heal, one `lane_up`, and the next call answers. It fails on main: there is one lane.
`world_lanes.rs::a_process_crash_resets_every_lane_and_a_machine_crash_or_pause_is_seen_on_control_alone` (§5.6).

### 2.3 Order

The promise changes from "calls to one peer arrive in the order they were made" (`guide/cluster.md:139`) to:

> Calls to one peer on one lane arrive in the order they were made.

A protocol has one lane, so the calls of one protocol keep their order: a tell, an upload and a request on one
protocol arrive in that order. Calls on two lanes can pass each other. Every protocol that names no lane is on `Live`,
so an application that never says `Bulk` sees no change.

**Proof.** `crates/axon-cluster/tests/cluster/parts.rs::a_tell_an_upload_and_a_request_on_one_protocol_arrive_in_order`,
a new guard. It passes on main and must keep passing: it fails if a later change moves a call to a lane by its size.

### 2.4 What goes where

| Traffic | Lane | Where it is chosen |
|---|---|---|
| directory, spawn, load, listing, watches, handoff, owns (ids 1 to 7, `wire.rs`) | Control | the engine's system endpoints |
| `axon.lease`, `axon.lease.held` (`paxos/mod.rs:545`, `:551`) | Control | `cluster.endpoint(..).lane(Control)` |
| `axon.quota` (`quota/mod.rs:197`) | Control | the same |
| `axon.stream`: stream interests (`node.rs:449`) | Control | the same |
| seed discovery sessions (`seed/mod.rs:448`) | Control | `connect(addr, Lane::Control, ..)` |
| a typed protocol | `P::LANE`, Live by default | the protocol |
| `cluster.serve(..)` | Live | unchanged signature |
| `serve_control` (telemetry, metrics, management) | Live | the engine |
| stream pushes | `axon.stream.live` or `axon.stream.bulk` | `StreamNodeConfig::lane(namespace, lane)` (§6) |
| a reply, its parts, an upload's body and its credit | the request's own session | §5.4 |

`Control` is axon's own. `publish::<P>` and `kind::<P>` refuse `P::LANE == Lane::Control`, and the raw builder takes
`Control` only for names under `axon.`. A flood of application calls cannot delay a lease round.

## 3. API

In the style of the README's API page (`crates/axon/README.md`), which gets these lines in the same commits:

```text
axon
  Lane::{Control, Live, Bulk}   a traffic class: its own connection to each peer, dialled on first use ; on the wire 0, 1, 2 ; also axon_net::Lane

axon-net
  Config::lanes: Lanes { control, live, bulk } ; cfg.lanes.get(lane) -> &LaneConfig
    LaneConfig { limits: Limits, compression, max_bytes_per_sec: Option<u64>, notsent_lowat: Option<u32> }
      Control: max_frame 1 MiB, 2 MiB out and in ; Live: 1 MiB, 8 MiB ; Bulk: 8 MiB, 32 MiB ; no compression ; no cap ; lowat 128 KiB
      a peer with every lane up both ways holds at most 168 MiB of frames at these limits (32 MiB on one session each way before)
      max_bytes_per_sec: a cap on what one session of the lane writes, pings and pongs apart ; None: none
  connect(addr, lane, tls, cfg) ; establish(io, Role::Initiator(lane) | Role::Responder, cfg) ; Peer::lane
  trait Network { connect(to, lane) -> Dial, bind(at) }   the lane lets a transport map lanes to its own streams
  VERSIONS = 7..=7   the hello names the lane ; a version-6 peer: Negotiation::Version
  AcceptLimits { max_connections 3072, max_handshakes 64, max_handshakes_per_ip 16 }
  SessionStats::paced   time a capped session waited for its rate

axon-cluster
  trait WireProtocol { .., const LANE: Lane = Lane::Live }   publish::<P> and kind::<P> refuse Lane::Control ; .also::<Q>() serves Q on Q::LANE
  cluster.endpoint(name, protocol, schema) .lane(lane) .serve(handler)? -> id   a raw endpoint on a lane ; Control only for names under axon. ; cluster.serve(..) is .lane(Live)
  cluster.max_frame(lane)   the largest body on that lane
  PeerStats { .., lane, down_ms } ; a row per (node, direction, lane) ; down_ms: Some(how long an outgoing Live or Bulk lane has had no session while Control is up), else None
  cluster.events() also: lane_down, lane_up (node, lane)   a Live or Bulk lane lost and got back while Control stays up ; its keys stay, its calls fail with Retry::Later

axon-stream (feature cluster)
  StreamNodeConfig::lane(namespace, lane)   the lane its pushes take, Live by default ; start refuses Control
```

Rule 3 of the README ("`tell` never waits ...") stands. The order promise in §2.3 replaces the guide's.

## 4. Transport (`axon-net`)

### 4.1 The lane and the hello v7

`Lane` is in `axon`, beside `Backoff`, which `axon-net` re-exports the same way (`axon-net/src/lib.rs:18`). The
stream core is I/O-free and builds without `axon-net` (`axon-stream/Cargo.toml`: `axon-net` comes only with the
`cluster` feature), and it needs the type (§6). Encoded `Control = 0`, `Live = 1`, `Bulk = 2`.

`VERSIONS` goes from `6..=6` to `7..=7` (`handshake.rs:22`). The hello appends one byte, the lane, after the
namespace:

```text
"AXON" | lowest u16 | highest u16 | body_len u16 | nonce [32] | incarnation u128 | max_frame u32 | accept u8
       | node_id (u16 len) | namespace (u16 len) | lane u8
```

- **The initiator names the lane.** `Role::Initiator(lane)`. Its `max_frame` is that lane's.
- **The responder echoes it.** The responder already reads the initiator's hello before it sends its own
  (`handshake.rs:152-157`). Today it encodes its hello before that (`let mine = encode(cfg)`, `:143`); it now encodes
  it after, with the initiator's lane and that lane's `max_frame`. This adds no round trip.
- **Checked after the versions.** `parse` reads the lane as optional, so a version-6 hello still parses, both hellos
  are sent, and `common` refuses it as `Negotiation::Version { ours, theirs }`, as today. Once version 7 is agreed, a
  hello without a lane, with a lane above 2, or with an echo that differs is `Negotiation::Malformed`.
- **Authenticated.** The lane is in both hellos, so it is in the HMAC transcript: no one in the middle can move a
  session to another lane.
- **Bytes after the lane are allowed**, as after the namespace today, for a later version.

The hello of node `a`, namespace `default`, `max_frame` 1 MiB, `accept` `Codecs::ALL`, on `Control`:

```text
41 58 4F 4E  00 07  00 07  00 42                 "AXON", versions 7..7, body 66 bytes
<nonce 32 bytes> <incarnation 16 bytes>
00 10 00 00  07  00 01 61  00 07 64 65 66 61 75 6C 74  00
max_frame    accept  "a"     "default"                  lane
```

66 = `FIXED` (53, `handshake.rs:24`) + 2 + 1 + 2 + 7 + 1.

**Proof.** `crates/axon-net/src/handshake.rs::the_v7_hello_is_these_bytes` (nonce and incarnation masked),
`handshake.rs::a_hello_with_an_unknown_lane_is_malformed`, and the existing
`handshake.rs::a_hello_parses_with_what_a_later_version_appends_to_its_body`, which must append after the lane: its
bytes `7, 7, 7` would now be read as lane 7. `session.rs::a_session_runs_the_highest_version_both_speak_and_ranges_that_do_not_meet_are_refused_naming_both`
(`tests/session.rs:146`) is rewritten for 6 against 7: the version-6 end is refused as `Version`, not `Malformed`.
`session.rs::the_hello_names_the_lane_and_the_responder_echoes_it` fails on main (there is no lane).

### 4.2 Limits per lane

`Config::limits` and `Config::compression` (`config.rs:141`, `:147`) move into `LaneConfig`, one per lane in
`Config::lanes`. The codec settings that are not per lane (`accept`, `zstd_level`, `compression_threshold`,
`compression_min_savings`) stay where they are. A session reads its lane's config, on both ends: the responder learns
the lane from the hello.

| Lane | `max_frame` | out and in, bytes | frames | why |
|---|---|---|---|---|
| Control | 1 MiB | 2 MiB | 256 | answers are small; a directory or held-keys page fits in 1 MiB |
| Live | 1 MiB | 8 MiB | 256 | today's frame and outbox; a reply over 1 MiB goes in parts (`request_parts`), as today; a larger frame would hold the calls behind it (§13) |
| Bulk | 8 MiB | 32 MiB | 256 | holds 256 ms at 1 Gbit/s, so a long fat link stays busy |

A peer with every lane up both ways can hold 2 × (4 + 16 + 64) MiB = 168 MiB of frames at the limits, against 32 MiB
today (8 MiB each way, out and in, on two sessions). It is the worst case: the limits are bounds, not buffers. Bulk's
32 MiB each way is most of it, and is what keeps a long fat link busy; the README line says the 168 MiB.

`Live` keeps today's 1 MiB frame. With windowed pushes ([push-window.md](push-window.md)) the push size no longer sets
a stream's speed, so nothing on `Live` needs a larger frame, and a larger one would make every call behind it wait
while it is written.

`cluster.max_frame()` (`cluster.rs:769`) becomes `max_frame(lane)`. Its callers: telemetry's budget
(`telemetry.rs:667`, Live) and the stream node's check of `max_push_bytes` (`node.rs:408`, against every lane its
namespaces use). B2 makes `max_frame()` return the smaller of `Control`'s and `Live`'s, as a stream's pushes go on `Live` and
its interests on `Control` until C2, and C2 makes it `max_frame(lane)`: the stream node is where a second lane is first
used, and B2 changes only the `serve` call in `node.rs`.

**Proof.** `session.rs::compression_is_negotiated_and_limits_only_shrink` (`tests/session.rs:194`), extended: each lane
negotiates its own codec and frame size, and one lane's full outbox refuses only its own submits.

### 4.3 The Bulk rate cap

`LaneConfig::max_bytes_per_sec` caps what one session of the lane writes. It is meant for `Bulk` and is off by default.
It keeps the queue at the path's bottleneck short, which separate connections alone do not (§13).

- A token bucket in `write_loop` (`session.rs:699`), filled at the rate, as deep as one write batch
  (`write_batch_bytes`, 256 KiB). A batch takes frames while the bucket holds a byte, and its last frame may take
  the bucket below empty; the next batch waits until that debt is paid back. A frame's sequence number is given when
  it is encoded, so a frame cannot be held back after it: the bucket decides before the frame is taken from the
  outbox, and one frame larger than the bucket still goes, alone, at the rate.
- A capped session never takes the direct write (`write_now`, `session.rs:340`): every frame goes through the bucket.
- Pings and pongs go first, as today (`session.rs:713-718`), and skip the bucket, so a capped lane is not taken for
  dead.
- A wait for the bucket is not a write without progress, so it does not count against `write_timeout`.
- The wait is counted: `SessionStats::paced`, and the metric `connection.paced` (C3).

**Proof.** `session.rs::a_capped_session_writes_no_faster_than_its_rate_and_keeps_order`,
`session.rs::a_capped_session_still_answers_pings_on_time`,
`crates/axon-net/tests/faults.rs::the_fault_loop_on_a_capped_session_ends_cleanly_and_returns_its_credit`. The loom
model `crates/axon-net/src/queued/model.rs` gains a paced writer: two senders, the writer task and a thread that
refills the bucket. `queued/model.rs::a_paced_writer_keeps_each_senders_order_and_never_writes_past_its_budget`
asserts that each sender's frames reach the stream in order, that `Queued` returns to 0 and that the bytes written
never pass what was refilled. Mutation check: let a sender write directly on a capped session, and the budget
assertion fails. A close during a wait gives every frame's credit back: the faults.rs loop above checks it on the
real session, which a model of its own close would not.

### 4.4 `TCP_NOTSENT_LOWAT`

`LaneConfig::notsent_lowat`, 128 KiB on every lane: the kernel holds at most that much unsent data on the socket.
Bytes wait in axon's bounded outbox, where they are counted, instead of in a send buffer that autotuning can grow to
megabytes. The rate cap then measures bytes close to when they leave, and `write_timeout` sees a stalled link sooner.
It is not what keeps `Live` fast: separate connections are.

It is set with `setsockopt` in `crates/axon-net/src/tcp.rs`, the one file allowed `unsafe`, on a dialled and an
accepted socket alike, once the handshake has named the lane (`establish_with`): the responder learns its lane only
from the initiator's hello, so it cannot be set beside `set_nodelay` at accept. The handshake's few hundred bytes go
before it. `TcpHealth::notsent_lowat` reads it back at each sample. The `libc` crate defines `TCP_NOTSENT_LOWAT` for
Linux only, so elsewhere it is a no-op. `SO_SNDBUF` and `SO_RCVBUF` are [wan.md](wan.md#8-host-tcp-settings)'s.

**Proof.** `session.rs::tcp_notsent_lowat_is_set_on_dialled_and_accepted_sockets` (Linux; reads the option back).

### 4.5 Accept limits

Each peer now opens up to three sessions to a node, so the defaults of `AcceptLimits` (`net.rs:97`) grow:
`max_connections` 1024 to 3072, `max_handshakes_per_ip` 8 to 16. `max_handshakes` stays 64. Three lanes dialled
together from one address take three handshake slots.

An authenticated peer could still open many sessions. The engine keeps at most two accepted sessions per (peer node,
lane): the newest and the one it replaced, and a third closes the older. Two, not one: a peer that let a session go
idle forgets it and still waits on it for up to a second for the replies it is owed, and a call in that second dials
again; closing the old session at once would cut those replies (`OutcomeUnknown`). A session from another boot of
the peer closes both, so a restart frees the half-open sessions of the boot before before their idle timeout. Under
`SharedSecret` the bound is per claimed node id: a holder of the secret can claim many, as §5.2 accepts; with
certificate identities it is per certificate.

**Proof.** `session.rs::handshake_slots_are_limited_per_ip` (`tests/session.rs:381`), at the new default.
`crates/axon-cluster/tests/cluster/lanes.rs::a_bulk_dial_that_waits_at_the_accept_cap_fails_only_bulk_calls`: a node
with `max_connections` 2 takes a peer's `Control` and `Live`; the `Bulk` dial waits, its calls end with `Retry::Later`
after the handshake timeout, `Live` calls succeed and the peer stays reachable.
`lanes.rs::a_third_session_of_one_peer_on_one_lane_closes_the_first`. Both fail on main.

### 4.6 The transport seam

`Network::connect(&self, to)` (`net.rs:31`) becomes `connect(&self, to, lane)`. TCP ignores the lane: each call is one
connection. The World records it (§7). A later QUIC transport maps the three lanes to three streams of one connection;
nothing above `Network` changes. QUIC is not built here.

**Proof.** The World's lane faults (§7) go through it.

## 5. The engine and routing (`axon-cluster`)

### 5.1 Lanes per peer

`peers: HashMap<node, Arc<PeerSlot>>` (`engine.rs:685`) becomes a map from node to `PeerLanes`, a slot per lane.
`ClusterConfig::max_peers` still counts nodes. `link(node, deadline)` (`engine.rs:1272`) becomes
`link(node, lane, deadline)`. Each slot runs its own `maintain` task (`engine.rs:1119`), with its own `Redial` on that
lane.

- Only the `Control` slot calls `set_reachable` and runs the give-up.
- A `Live` or `Bulk` slot dials once `Control` is `Up`, and takes a session only from the boot its `Control` link is to.
  A session from another boot is turned away as today's `named` check turns away a stale one (`LinkState::Down(Auth)`,
  `engine.rs:1179-1183`), and its callers get `StaleNode` when they named that boot.
- When `Control` goes `Down`, it closes the node's other lanes to that boot.
- `reset_peer` (`engine.rs:1249`) resets every lane of the node.
- `idle_link_timeout` (300 s) applies per lane. `Control` counts traffic on any of the node's lanes, so it closes for
  idleness last. A lane that has failed to dial, or answered as another boot, for that long is forgotten, so `Control`
  can idle; the next call on it dials again.
- The engine's own asks (`ask`, `engine.rs:1654`) and the fixed system endpoints go on `Control`.

Accepted sessions (`incoming`, `engine.rs:686`) are keyed by (peer node, lane) for §4.5's rule.

**Proof.** `world_lanes.rs::a_peer_that_restarts_between_its_lanes_dials_is_called_as_its_new_boot_only`: `b`
restarts after `a`'s `Control` is up and before its `Live` dial; no `Live` call reaches the new boot through the old
boot's route, and calls after a new resolve do. `world_lanes.rs::a_half_open_live_lane_closes_alone_after_its_idle_timeout`:
`cut_lane(a, b, Live)` and back; `Live` closes after `idle_timeout`, `Control` stays up, the node stays reachable.

### 5.2 Endpoints and their lane

The registry's `Entry` (`engine.rs:126`) and a call's `Target` (`engine.rs:143`) gain `lane`. `prepare`
(`engine.rs:1360`) links on `target.lane`.

- **Typed.** `WireProtocol` (`wire.rs:73`) gains `const LANE: Lane = Lane::Live`. `publish::<P>` (`cluster.rs:590`)
  and `kind::<P>` (`spawn.rs:1771`) register on `P::LANE`, and refuse `Control` with `ClusterError::Config`.
  `.also::<Q>()` (`spawn.rs:1571`) registers `Q` on `Q::LANE`: the lane is per schema endpoint.
- **Raw.** `cluster.serve(name, protocol, schema, handler)` (`cluster.rs:741`) keeps its signature and means `Live`.
  `cluster.endpoint(name, protocol, schema).lane(lane).serve(handler)` serves on another lane. `Control` is refused
  unless the name starts with `axon.`.
- **Control services.** `serve_control` (`cluster.rs:687`) runs on `Live`: telemetry and metrics pulls are calls,
  not cluster upkeep.

A request that arrives on another lane than its endpoint's is served anyway. The lane is where the caller sends, not
a check: a caller with an old route still gets an answer, and a hostile peer that knows the secret can do worse.

A protocol that wants small calls on `Live` and uploads on `Bulk` declares two protocols and publishes the same actor
under both names.

**Proof.** `crates/axon-cluster/tests/cluster/lanes.rs::publish_and_kind_refuse_control_and_the_raw_builder_takes_it_only_under_axon`,
`lanes.rs::a_kind_also_serving_a_bulk_schema_answers_each_schema_on_its_own_lane`.

### 5.3 Routes and the directory

A route (`cluster.rs:151-191`) is today `protocol u64 | (endpoint u64 | schema u16)+`, checked as `len % 10 == 8`.
It becomes:

```text
protocol u64 | (endpoint u64 | schema u16 | lane u8)+        len % 11 == 8, at least 19 bytes
```

The directory's answer (`engine.rs:845`), a spawn's and a handoff's answer (`spawn.rs:180`, `:186`) and `route_on`
(`cluster.rs:972`) all use it. The serving node's lane wins: the caller does not compare it with its own `P::LANE`. A
lane byte above 2 refuses the route as malformed.

`SYSTEM_SCHEMA` stays 1. The system endpoints' formats go with the session's wire version, and version 7 is the only
one ([rolling-upgrades.md § 2](rolling-upgrades.md#2-wire-versions)). The rule in `AGENTS.md` to bump a `SCHEMA` on
a wire change is for protocols, which are versioned by schema; this one is versioned by `VERSIONS`. A2 takes
`VERSIONS` to 7 with the hello, and B2 adds the lane byte to routes under it, so version 7 is whole only once B2
merges; nothing is released, tagged or run beside another build between the two.

**Proof.** `crates/axon-cluster/src/cluster.rs::a_route_with_an_unknown_lane_byte_is_refused` (unit),
`lanes.rs::a_kind_also_serving_a_bulk_schema_answers_each_schema_on_its_own_lane`.

### 5.4 Replies, parts and uploads

A `Reply` keeps the `Link` its request came on (`Sink::Remote { link, .. }`, `engine.rs:178`; `Reply`, `:359`). So a reply, its parts, an
upload's body and the credit for both travel on the request's session and lane, with no change. A request on `Bulk`
gets its reply on `Bulk`. Parts and uploads stay on `Live` unless the protocol says `Bulk`.

**Proof.** `cluster/parts.rs::a_tell_an_upload_and_a_request_on_one_protocol_arrive_in_order` (§2.3), and
`lanes.rs::a_bulk_upload_and_its_reply_parts_stay_on_the_bulk_session` (by `PeerStats` bytes per lane).

### 5.5 Crashes and restarts

- **Process crash.** The peer's kernel resets every connection: every lane ends at once, `Control` with it.
- **Machine crash or pause.** All lanes go quiet. `Control`'s keepalive notices (1 s pings, 4 s idle) and its `Down`
  closes the others. They do not each wait out their own idle timeout.
- **Restart between dials.** §5.1: a lane takes only its `Control` link's boot.

**Proof.** `world_lanes.rs::a_process_crash_resets_every_lane_and_a_machine_crash_or_pause_is_seen_on_control_alone`:
after `Crash::Machine` or `pause` of `b`, `a`'s `Live` and `Bulk` links to `b` end within a tick of `Control`'s.

### 5.6 The proof test

`crates/axon-cluster/tests/world_lanes.rs::bulk_saturating_the_links_costs_no_lease_and_live_calls_stay_fast`. Three
acceptors `a`, `b`, `c` under `PaxosLeases`; `a` holds a key. Every link: latency 10 ms (a 20 ms round trip),
bandwidth 2 MiB/s, the World's default window of 64 KiB. `a` runs two uploads of 10 MB to `b` and two to `c` in a loop,
which fills an 8 MiB outbox (one upload holds at most 4 MiB unread, `engine/parts.rs:30`). Meanwhile `a` times a
256 B `Live` request to `b` every 50 ms for 60 s.

It asserts: the key's epoch never changes and `cluster.events()` holds no lost lease; the `Live` p99 is at most the
round trip plus 50 ms; every body arrives whole.

On main with §7's bandwidth model, and the uploads on a protocol without `LANE`, the request waits behind up to 8 MiB:
about 4 s at 2 MiB/s. A lease round waits `lease / 6` = 1 s for answers (`paxos/mod.rs:73`), so renewals should fail
and the lease lapse. That the lease lapses on main is not yet run; the test is written first, run on main, and its
result recorded. With lanes, a `Live` frame waits at the bottleneck only behind what `Bulk`'s window leaves there:
64 KiB less the 40 KiB in flight, about 24 KiB, 12 ms.

`world_lanes.rs::a_slow_bulk_lane_never_delays_control_rounds`: a `Bulk` handler that does not read its uploads fills
`Bulk`'s outbox and inbox; the lease's rounds take no longer than without it. It fails on main.

`world_lanes.rs::lost_and_late_chunks_on_one_lane_keep_its_order_and_spare_the_others`: short `cut_lane`s and
`reset_lane`s on `Bulk`; `Bulk`'s calls end or arrive in order, `Live`'s round trips do not move.

## 6. Streams on lanes (`axon-stream`)

`StreamNodeConfig::lane(namespace, lane)` puts a namespace's pushes on a lane, `Live` by default. `start` refuses
`Control` and a `max_push_bytes` larger than `max_frame` of a lane in use (today one check, `node.rs:408`;
`max_push_bytes` counts the push header, `core.rs:1210`). `Live` is always in use, as the lane of every namespace not
named; `Bulk` once a namespace names it. `max_push_bytes` stays one setting, default `(1 << 20) - 64`, which fits
every lane's frame at the defaults. Interests are built within `max_push_bytes` too (`core.rs:1765`) and go on
`Control`: the node gives the core the smaller of `max_push_bytes` and `Control`'s `max_frame` as the interests'
bound, so an application that raises `Live`'s frame and `max_push_bytes` together does not make interests too large
for `Control`.

A namespace that moves much more than its node's calls goes on `Bulk`: on `Live` its pushes queue in the outbox the
calls use. Measured on today's one connection, 8 MiB pushes made the origin's requests take two round trips, 438 ms at
219 ms ([push-window.md §9](push-window.md#9-lanes)). The guide says so beside `StreamNodeConfig::lane`.

- **Interests** go on `Control`, on the endpoint `axon.stream` (`node.rs:33`) as today. A child's interest is upkeep:
  it must not wait behind records.
- **Pushes** go to `axon.stream.live` or `axon.stream.bulk`. One closure serves all three names (`node.rs:449`). The
  push names refuse an interest, and `axon.stream` refuses a push, with `InvalidMessage`. A child always serves both
  push names; serving costs no connection. The parent's lane for a namespace wins. The names come with push-window's
  SCHEMA 7 (A4, [push-window.md §6](push-window.md#6-wire)), both served on `Live`; C2 serves `axon.stream.bulk` on
  `Bulk` with the endpoint builder (§5.2) and sends each namespace's pushes to its lane's name.
- **The core.** `CoreConfig` (`core.rs:176`) maps a namespace to its lane. `Wake::Peer(node)` becomes
  `Wake::Peer(node, lane)`, and push-window's `next_push(node, now, rtt)`, `push_answered`, `push_failed` and
  `handle_push` ([push-window.md §3](push-window.md#3-api)) gain a `lane`: a push carries only that lane's namespaces,
  and a channel is per (node, lane) at the parent and per (parent node, lane) at the child. `DownstreamStats` gains
  `lane`. The I/O-free core needs `Lane` without `axon-net`, which is why it lives in `axon`.
- **The driver** (`pumps`, `node.rs:261`; `drain`, `node.rs:290-318`, which push-window's B1 replaces with one task per
  channel) is keyed by (node, lane), so a `Bulk` backlog never holds a `Live` push. How many may go at once is
  [push-window.md](push-window.md)'s; its window is per (node, lane).
- C2 changes no stream bytes: the push encoding and the stream `SCHEMA` stay at push-window's 7 (`node.rs:35`). The
  push carries no lane byte: the endpoint says it. A child built before C2 follows one channel per parent, so a
  parent's `Bulk` channel and its `Live` one would retire each other there: name a namespace's lane `Bulk` once every
  node runs C2.
- **Expiry.** A child whose pushes on one lane have failed for `downstream_expiry` is dropped on every lane, though
  Control and its other lane are up: its interests are one set for all its lanes, and the child forgets every lane's
  channel when it is answered out of sync, so its next refresh asks again for the rest at once. Kept per lane by
  `node::tests::a_child_whose_pushes_fail_while_its_interests_arrive_is_dropped` (Live) and
  `world_lanes.rs::a_dead_bulk_lane_stalls_only_its_namespaces_and_they_catch_up_from_the_ring` (Bulk).

**Proof.** `crates/axon-stream/tests/world_lanes.rs::a_bulk_namespace_saturating_the_links_keeps_a_live_namespaces_records_fast`
(fails on main: every namespace shares one push queue), `world_lanes.rs::a_dead_bulk_lane_stalls_only_its_namespaces_and_they_catch_up_from_the_ring`,
`world_lanes.rs::a_child_restart_ends_both_lanes_pumps_and_each_namespace_resumes`,
`crates/axon-stream/tests/relay.rs::start_refuses_a_namespace_on_control_and_a_push_larger_than_its_lanes_frame`. The
fuzz target `stream_core` maps namespaces to lanes from its input, and its oracle checks that a push for (node, lane)
carries records of that lane's namespaces only.

## 7. The World (`axon-testkit`)

### 7.1 Bandwidth

`Wire` (`world.rs:88`) gains `bandwidth: u64`, bytes a second, 0 for no limit, the default. Each directed link
(from, to) is one first-in, first-out bottleneck that every connection on it shares, whatever its lane. A data chunk
of `n` bytes starts when the link is free, `max(at, busy_until)`, and holds it for `n / bandwidth`; then it travels the
latency and jitter as today. Acknowledgements, SYN, FIN and RST take no time on it.

The model adds no random draw, so with bandwidth 0 `post` (`world.rs:1265`) makes the same draws as today. The `wire`
trace line (`world.rs:754`, `{wire:?}`) prints the bandwidth only when it is set. Every digest of today stays the same.
[wan.md §3](wan.md#3-the-world-dials-at-the-round-trip-work-item-a1) moves dials to take a round trip in the World, a
SYN's answer travelling back as a chunk; that change goes first, the answer takes no time on the bottleneck either, and
this model must keep the digests it leaves.

### 7.2 Faults on one lane

`Nic::connect` (`world.rs:1496`) records the lane of each dial on its connection. The SYN trace line names the lane
when it is not `Control`, so today's lines stay the same.

- `w.cut_lane(from, to, lane)` holds that lane's chunks from `from` to `to`, and its dials either way, as `cut` does
  (`world.rs:719`). `heal()` clears it. Both ways, it is one lane partitioned; one way, a half-open lane.
- `w.reset_lane(a, b, lane)` ends that lane's connections between two nodes, at both ends, as `reset` does
  (`world.rs:758`).
- Loss on one lane is a short `cut_lane` then `heal`. Reorder across lanes is already a property of the seeded order in
  which connections deliver.

Until the engine has lanes (§5), every connection is `Control`.

**Proof.** `crates/axon-testkit/tests/world.rs::a_link_with_a_bandwidth_sends_one_chunk_at_a_time_in_the_order_they_came`
(a 1 MiB transfer at 1 MiB/s takes 1 s plus latency, and a second connection's chunk waits behind it),
`crates/axon-testkit/tests/world_faults.rs::a_lane_cut_or_reset_holds_or_ends_that_lanes_connections_and_no_other`. The
digests: `scripts/check.sh sim` on the base and on the branch, then `diff` of both `target/check/sim-1.digests`
(written at `scripts/check.sh:297`) is empty. §5's `Live` dials change digests where they run; that is expected and
said in the commit.

## 8. Console and telemetry (`axon-obs`)

- `PeerStats` (`engine.rs:618`) gains `lane`. The engine's stats have a row per (node, direction, lane).
- Telemetry: `Request::Connection { peer, outgoing, lane }` (`telemetry.rs:389`, `model.rs:116`), and a `lane` label
  on the `connection.*` series (`telemetry.rs:992`, `guide/telemetry.md:75-78`). `connection.paced` is new. A sample
  still holds the busiest `sample_connections` connections, so its size does not grow with lanes.
- Console: the peer table (`features/connections/PeerTable.tsx`) gets a column "Lane", and a row's key adds the lane.
  The route `connections/:nodeId/:peerId/:direction` (`App.tsx:50`) gains `/:lane`. The detail page shows the lane and
  the rate wait. A lane with `down_ms` set is a row of its own with the state "Down" and how long; its tooltip:
  "This connection is down. Calls on it fail and try again later. The peer stays in the cluster." Text in ASD-STE100: the column "Lane"; its tooltip "The traffic class of this connection." The help
  dialog gets: "Each peer has up to three connections. Control sends cluster messages. Live sends calls. Bulk sends
  large transfers. A slow Bulk connection does not stop Live or Control."
- A down lane has no session: `Request::Connection` for it answers none, and it has no `connection.*` series. The
  topology draws one edge per pair of nodes and direction of dial, the sum of its lanes, its round trip the Control
  lane's.

**Proof.** `crates/axon-obs/tests/telemetry.rs::each_lane_of_a_peer_is_a_row_and_a_series_of_its_own`,
`crates/axon-obs/ui/tests/dashboard.spec.ts` (a connection's page by lane), and a `scripts/ctl.sh` run with the
verify-axon skill: a screenshot of the peer table with three lanes, the detail page clicked open, no layout shift.

## 9. Benchmarks

The harness `crates/axon-cluster/examples/uploads.rs`
([benchmarks § Requests in parts](../crates/axon/guide/benchmarks.md#requests-in-parts)) gets two rows:

| Row | What it times | Today (one session) |
|---|---|---|
| `lanes-live-p99-bulk` | 256 B `Live` requests, p50 and p99, while 10 MB bodies go on `Bulk` | the "in 256 KiB parts" row: p50 267–269 µs, p99 496–504 µs (b0bfa15e, build-1) |
| `lanes-control-round-bulk` | a round trip on a raw `axon.bench.round` endpoint on `Control`, with the same load | not measured |

The expected result is the "no bodies" row, p50 8.8–8.9 µs and p99 10.4–10.5 µs, plus what the two connections cost in
processor time. The rows go in the gate (`scripts/bench.sh`) if two runs agree within its tolerance; else they stay
indicative, like the harness now. `loopback` and `remote_calls` must not regress: a `Live` call takes one more field
and one more map lookup.

What it does not establish: a real network. On loopback there is no shared bottleneck. The World test of §5.6 and
the rig below cover that.

### The rig (D3)

D3's geo night ([wan.md §12](wan.md#12-the-rig-work-item-d3)) runs lanes on real TCP, on build-1 in `RIG_BOX` mode,
where `netem`'s queue stands in for a router's at the bottleneck, which the World's 64 KiB window understates (§13).

**The `bulk` workload.** A protocol `rig.bulk` with `LANE = Lane::Bulk`: b1 uploads 10 MB bodies to every other node,
two at once per peer, in a loop, and each answers with the body's CRC, checked as `blob` checks its echoes. It runs
with `accounts` (leases on `Control`) and `echo` (calls on `Live`). With `TIMINGS=wan`, `examples/rig/src/main.rs`
sets `lanes.bulk.max_bytes_per_sec` to 16 MB/s (128 Mbit/s): the `firehose` workload's 13 MB/s shares b1's Bulk
sessions to b2 and b3 ([push-window.md § The rig (D3)](push-window.md#the-rig-d3)), and a 5 MB/s cap would hold it
under its own invariant.

**Scenarios** in `library(T)`:

| Scenario | Faults | Judged by |
|---|---|---|
| `wan-bulk` | `geo` on and `bulk` running, 10 min | `live-under-bulk` and every other invariant |
| `rate-bulk` | `geo rate=150`: a `netem rate` of 150 Mbit/s on each of b1's destination bands, inside `geo`'s qdisc (the `rate` class cannot stack with `geo`), `bulk` running, 10 min | `live-under-bulk`: one Bulk session per peer, capped at 128 Mbit/s, keeps the bottleneck's queue short |
| `rate-bulk-uncapped` | the same at `rate=100`, under the cap, so the cap does not hold Bulk below the link | a probe: `live-under-bulk` is reported apart and never fails the run; it measures what the cap is worth |

**The invariant** in `check.py`:

| Invariant | Holds |
|---|---|
| `live-under-bulk` | through a `wan-bulk` or `rate-bulk` episode, every node's `echo` calls to each peer keep their p99 over the episode (not per minute, whose 30 calls' p99 is the slowest) under that pair's round trip plus 250 ms, and no node's `leases_lost`, `isolations` or `became_unreachable` moves |

The first plan named it `live_under_bulk`; every rig invariant has hyphens.

Plants: `@plant(want=["live-under-bulk"])` on `p_live_under_bulk`, a synthetic `wan-bulk` episode whose echo p99 is
the round trip plus 400 ms. The real-binary plant `examples/rig/plants/bulk-on-live.sh` sets `rig.bulk`'s `LANE` to
`Live`: in `rate-bulk` the echo calls then wait behind up to `Live`'s 8 MiB outbox of upload parts at 150 Mbit/s,
about 0.45 s, and the run fails `live-under-bulk`.

## 10. Fault rows for `spec/faults.md`

Three new tables, each with all eighteen rows. The `In the World` column gets: `Wire::bandwidth` (latency, overload,
memory limit), `cut_lane` (partition and one-way cut of one lane), `reset_lane` (loss of one lane), with §7's
self-tests.

### transport: lanes

| Fault | Covered by |
|---|---|
| crash | `crates/axon-net/tests/faults.rs::the_fault_loop_on_a_capped_session_ends_cleanly_and_returns_its_credit` |
| restart | n/a: a lane is a session; the engine checks each lane's boot (cluster: lanes) |
| pause | `crates/axon-net/tests/session.rs::a_capped_session_still_answers_pings_on_time` |
| partition | n/a: one session sees one path; one lane cut is in cluster: lanes |
| one-way cut | n/a: as partition |
| loss | `crates/axon-net/tests/faults.rs::the_fault_loop_on_a_capped_session_ends_cleanly_and_returns_its_credit` |
| duplication | n/a: a session resends nothing |
| reordering | `crates/axon-net/src/queued/model.rs::a_paced_writer_keeps_each_senders_order_and_never_writes_past_its_budget`, `crates/axon-net/tests/session.rs::a_capped_session_writes_no_faster_than_its_rate_and_keeps_order` |
| latency | `crates/axon-net/tests/session.rs::a_capped_session_writes_no_faster_than_its_rate_and_keeps_order` |
| clock skew or step | n/a: the bucket uses the monotonic clock |
| CPU starvation | gap |
| memory limit | `crates/axon-net/tests/session.rs::compression_is_negotiated_and_limits_only_shrink` |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | `crates/axon-net/tests/session.rs::handshake_slots_are_limited_per_ip` |
| panic | n/a: no user code runs in a session |
| overload | `crates/axon-net/tests/faults.rs::the_fault_loop_on_a_capped_session_ends_cleanly_and_returns_its_credit` |
| hostile input | `fuzz/fuzz_targets/handshake.rs::fuzz_target`, `crates/axon-net/src/handshake.rs::a_hello_with_an_unknown_lane_is_malformed` |

### cluster: lanes

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/tests/world_lanes.rs::a_process_crash_resets_every_lane_and_a_machine_crash_or_pause_is_seen_on_control_alone` |
| restart | `crates/axon-cluster/tests/world_lanes.rs::a_peer_that_restarts_between_its_lanes_dials_is_called_as_its_new_boot_only` |
| pause | `crates/axon-cluster/tests/world_lanes.rs::a_process_crash_resets_every_lane_and_a_machine_crash_or_pause_is_seen_on_control_alone` |
| partition | `crates/axon-cluster/tests/world_lanes.rs::a_dead_bulk_lane_fails_only_its_calls_and_the_peer_stays_reachable`, `crates/axon-cluster/tests/world_lanes.rs::a_node_with_a_dead_live_lane_keeps_its_keys_and_its_live_calls_retry_later` |
| one-way cut | `crates/axon-cluster/tests/world_lanes.rs::a_half_open_live_lane_closes_alone_after_its_idle_timeout` |
| loss | `crates/axon-cluster/tests/world_lanes.rs::lost_and_late_chunks_on_one_lane_keep_its_order_and_spare_the_others` |
| duplication | n/a: a call is sent once, on one lane |
| reordering | `crates/axon-cluster/tests/world_lanes.rs::lost_and_late_chunks_on_one_lane_keep_its_order_and_spare_the_others`, `crates/axon-cluster/tests/cluster/parts.rs::a_tell_an_upload_and_a_request_on_one_protocol_arrive_in_order` |
| latency | `crates/axon-cluster/tests/world_lanes.rs::bulk_saturating_the_links_costs_no_lease_and_live_calls_stay_fast` |
| clock skew or step | n/a: lanes read no clock; the lease rows stand |
| CPU starvation | gap |
| memory limit | `crates/axon-cluster/tests/world_lanes.rs::bulk_saturating_the_links_costs_no_lease_and_live_calls_stay_fast` |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | `crates/axon-cluster/tests/cluster/lanes.rs::a_bulk_dial_that_waits_at_the_accept_cap_fails_only_bulk_calls`, `crates/axon-cluster/tests/cluster/lanes.rs::a_third_session_of_one_peer_on_one_lane_closes_the_first` |
| panic | n/a: handlers run as on one session; the remote-call rows stand |
| overload | `crates/axon-cluster/tests/world_lanes.rs::a_slow_bulk_lane_never_delays_control_rounds` |
| hostile input | `crates/axon-cluster/src/cluster.rs::a_route_with_an_unknown_lane_byte_is_refused`, `fuzz/fuzz_targets/handshake.rs::fuzz_target` |

### streams: lanes

| Fault | Covered by |
|---|---|
| crash | `crates/axon-stream/tests/world_lanes.rs::a_child_restart_ends_both_lanes_pumps_and_each_namespace_resumes` |
| restart | `crates/axon-stream/tests/world_lanes.rs::a_child_restart_ends_both_lanes_pumps_and_each_namespace_resumes` |
| pause | gap |
| partition | `crates/axon-stream/tests/world_lanes.rs::a_dead_bulk_lane_stalls_only_its_namespaces_and_they_catch_up_from_the_ring` |
| one-way cut | gap |
| loss | `crates/axon-stream/tests/world_lanes.rs::a_dead_bulk_lane_stalls_only_its_namespaces_and_they_catch_up_from_the_ring` |
| duplication | n/a: the relay core drops records it already has (streams rows) |
| reordering | `fuzz/fuzz_targets/stream_core.rs::fuzz_target` |
| latency | `crates/axon-stream/tests/world_lanes.rs::a_bulk_namespace_saturating_the_links_keeps_a_live_namespaces_records_fast` |
| clock skew or step | n/a: lanes read no clock |
| CPU starvation | gap |
| memory limit | `crates/axon-stream/tests/world_lanes.rs::a_bulk_namespace_saturating_the_links_keeps_a_live_namespaces_records_fast`, `crates/axon-stream/src/core/tests.rs::each_lane_has_a_channel_and_a_window_of_its_own` |
| disk full | n/a: the disk rows of streams stand; a lane changes no write |
| disk error | n/a: as disk full |
| fd exhaustion | gap |
| panic | n/a: no user code in the pump |
| overload | `crates/axon-stream/tests/relay.rs::start_refuses_a_namespace_on_control_and_a_push_larger_than_its_lanes_frame`, `crates/axon-stream/src/core/tests.rs::interests_keep_within_max_interest_bytes` |
| hostile input | `fuzz/fuzz_targets/stream_core.rs::fuzz_target` |

The `frame_decode` and `stream_wire` fuzz targets do not change: the frame header and the push encoding stay the same.
`handshake` reads its role byte as `Responder` or `Initiator` of one of three lanes. `stream_core` is as in §6.

## 13. Risks

- **The path's queue is not axon's.** Lanes remove the queues axon owns: the outbox, the inbox, the socket's send
  buffer. A router's queue at the bottleneck is shared. Under Cubic, a busy `Bulk` connection can keep a bandwidth-delay
  product or more queued there, and `Live` waits behind it. The World's model, with a 64 KiB window, understates this.
  The rate cap (§4.3) is the remedy: cap `Bulk` below the link's rate. D3's `rate-bulk` holds it and
  `rate-bulk-uncapped` measures what it is worth (§9).
- **A lane-only fault keeps a node placed.** By design (§2.2) a node whose `Control` works and whose `Live` does not
  still owns its keys, and calls to them fail with `Retry::Later` until `Live` returns. `PeerStats`, the console and a
  `lane_down` event show it; the operator drains the node.
- **Three times the idle cost.** Each session end costs about 65 µs a second and 23 KB (`benchmarks.md:1891`). A peer
  with every lane up costs three times that, until `idle_link_timeout` closes the idle ones.
- **Memory bounds grow.** §4.2: 168 MiB per peer at the limits against 32 MiB.
- **`axon.` is a convention.** An application can name a raw endpoint `axon.mine` and put it on `Control`.
- **A large frame holds its lane while it is written.** On `Live` a frame is at most 1 MiB: 0.5 s at 2 MiB/s. On
  `Bulk` one of 8 MiB takes 4 s, which only `Bulk`'s own calls wait for.

## 14. Left out

- **A keepalive per lane.** Every lane's session pings at the cluster's `keepalive_interval` (1 s) and closes at its
  `idle_timeout` (4 s, 8 s at [wan.md §9](wan.md#9-the-values-across-regions)'s values). `LaneConfig` has no keepalive of its
  own: only `Control`'s decides reachability, and the others' only end their own lane.
- **A lane per reference, per message size or per call.** A protocol has one lane, so its calls keep their order.
- **A lane byte in the push header.** The endpoint name says it.
- **`Frame::packed` for pushes.** Nothing here needs it.
- **A bandwidth per lane in the World.** Real lanes share the path; the model does too.
- **Compression by default on `Bulk`.** The default stays `None` on every lane.
- **Fragmenting a record across frames.** A record still fits one frame of its lane.
- **A second `Cluster` as a bulk network**, the workaround the research note gave. Lanes replace it.
- **Taking a node out of placement when `Live` stays down.** The node keeps its keys and shows the dead lane (§2.2);
  the operator drains it.
- **QUIC.** The seam is `Network::connect(to, lane)`.
