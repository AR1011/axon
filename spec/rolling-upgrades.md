# Rolling upgrades

A cluster moves from one build to the next one node at a time. Each node drains with
`LeaveReason::Maintenance`, stops, and starts on the next build, while the other nodes keep serving. For the length of
the roll, old and new nodes run side by side and must keep talking. This page says what they promise each other and
how the code keeps that promise. The user's view is [guide/cluster.md § Rolling upgrades](../crates/axon/guide/cluster.md#rolling-upgrades).

## 1. Policy

- **From 1.0, N and N-1.** A release speaks its own wire version and the version of the release before it. A cluster
  can roll from any release to the next one. Skipping a release is not a roll: stop the cluster and start it again.
  The promise covers the transport and every internal protocol: membership, the directory, remote calls, remote
  spawn, watches, leases, streams, quotas and telemetry.
- **Before 1.0, no promise.** Every release may break the wire, and a pre-1.0 release speaks only its own version.
  This release speaks version 7 only (`axon_net::VERSIONS`, `handshake.rs:23`); the next speaks 8 only (§6).
  Version 7 named the lane in the hello ([lanes.md §4.1](lanes.md#41-the-lane-and-the-hello-v7)); version 6 added
  requests in parts (the `BODY` bit on `REQUEST`, `PART`, `END` and `CREDIT`); version 5 says in a spawn's answer whether that request started the actor, so that one node records
  an eager restart; version 4 added replies in parts (`PART`, `CREDIT`, the `PARTS` bit); version 3 put the nodes a
  keyed start passed in the spawn request and added `axon.owns`
  ([paxos-leases.md §5.3](paxos-leases.md#53-placement-under-partial-cuts)). A node of version 6 is refused at the
  handshake. A node on version 1, whose hello named a single version where the lowest is now, is
  refused as `Negotiation::Version` with `theirs` = `(1, 1)` and counted under `version`, so a range starts at 2.
- **User protocols are the user's.** A protocol's `ID` stays fixed, and a breaking change takes a larger `SCHEMA`.
  During a roll, the new build serves the old schema beside the new one. It stops serving the old one once no caller
  of it is left.

## 2. Wire versions

Each build has a range, `axon_net::VERSIONS`, and puts it in its hello:

```text
"AXON" | min u16 | max u16 | body_len u16 | body
```

Both ends send their hello before either end checks anything. A session then runs the highest version in both
ranges: `min(max_a, max_b)`, as long as that is at least `max(min_a, min_b)`.

- **Recorded.** The version is kept in `Peer::version` and shown in `PeerStats::version`.
- **Refused only when the ranges do not meet.** The refusal is `Negotiation::Version { ours, theirs }`, which names
  both ranges. The listener counts it under the handshake failure reason `version`, which appears in
  `cluster.stats().engine.listener` and in telemetry.
- **No downgrade.** The HMAC transcript covers both ranges, so a party in the middle that narrows a range fails
  authentication.
- **Trailing body bytes are allowed.** A later version may add fields to the hello.

Anything that is not a user protocol goes through the session version. That covers the frame format, the system
endpoints (membership, the directory, spawn, watches, listing) and the formats of their answers. A release that changes
one of these raises `VERSIONS`'s end. It keeps its start at the version before, and the code picks the format by
`Peer::version`.

The one test-only knob is `axon_net::Config::versions`, which is `#[doc(hidden)]`. It lets a test run a "next build"
without a real one.

## 3. Schemas side by side

The internal protocols that are served by name use the same mechanism as user protocols. These are streams
(`SCHEMA` 7, `axon-stream/src/node.rs:42`; 8 in wire v8, §6), Paxos leases, quotas and telemetry, all served with
`Cluster::serve`.

- **A name maps to one protocol, and to one endpoint per schema.** Publishing a name again is allowed only for the
  same protocol at a new schema. Anything else is `NameTaken`. Each endpoint has its own lifecycle, so withdrawing
  one schema leaves the others.
- **The directory answers with every schema.** The answer to a name lookup is
  `1 | protocol u64 | (endpoint u64 | schema u16)+`, and a spawn reply `epoch u64 | started u8` then the same
  route. A caller takes the endpoint for its own `SCHEMA`. A cached route that lacks it is looked up again, since the
  node may have published that schema after the route was cached.
- **A node that does not serve the schema.** When the node does not serve the caller's schema, the caller gets
  `ClusterError::SchemaMismatch { wanted, served }`. Its `retry()` is `Resolve`, because another node may serve that
  schema, or this node may serve it after its own upgrade. `resolve_all` puts such nodes in its failures, and a
  `NamedRef` or `KeyedRef` asks again on its next call. A quota replica's pull drops such a node as it does one of
  another protocol, so it never holds a new replica's readiness ([quotas.md §2.6](quotas.md)).
- **Dispatch did not change.** A call still names one endpoint and carries its schema. No per-call path does more work
  than before: a cache hit scans the couple of endpoints the route lists.

From 1.0, "negotiation" for an internal wire therefore means this: a release that changes its schema serves the old
schema too, and the caller asks for the newest schema the peer's lookup lists. Before 1.0 each is served at one
schema: streams went from 5 to 6 in the release that brought wire version 2, so a node at stream schema 5
speaks wire version 1 and is refused at the handshake before any stream call. Streams went from 6 to 7 with the push
window and the wire version stayed: a node at 6 and one at 7 connect, but each finds the other's `axon.stream` at
another schema and the 7 node's pushes go to `axon.stream.live`, which the 6 node does not serve. Each call is refused
before its body is read, so neither build relays a stream to the other during the roll: a child takes such a parent
as one that does not answer.

## 4. Keyed actors

A kind can serve more than one schema: `cluster.kind::<New>(kind).also::<Old>()`. Each key has one actor, which
serves every listed schema, one endpoint each. The node advertises the kind as `kind/2+1` in `axon.kinds`: first
its own schema, the `P` of `kind::<P>`, which its own callers use, then each it serves `also`.
`NodeRecord::kind_schemas` reads that back in that order.

**Placement.** Among the nodes that are eligible for a key, `Placement::candidates` takes each one's own schema, and
keeps the nodes that serve the most of them. The rule does not depend on the caller, so every node computes the
same owner and a key still runs once.

**What happens during a roll.** Rolled in one step (the new build calls 2 and serves 1 beside it), the first
upgraded node is the only one that serves both schemas in use, so it becomes the only candidate, and the keys move
to it by the usual rebalance: drain here, start there. They spread out again as more nodes come back upgraded. Rolled
in two steps, first a build that serves 2 `also` but still calls 1, then one that calls 2, every node serves every
schema in use at each step, and the keys stay spread.

**Ceiling.** In a one-step roll, the few upgraded nodes carry every key of the kind whose schema changed, and a key
whose only candidate refuses for capacity stays refused (`Spawn(Capacity)`, `Later`) until the next node comes back
upgraded. A kind whose schema did not change keeps its spread. The two-step roll avoids this. Without `also`,
neither build answers the other's callers: every node stays a candidate, and a caller whose key sits on a node of
the other build gets `SchemaMismatch` until that node upgrades.

**A caller no node can serve.** A `get_or_spawn` whose owner does not serve its schema places nothing. It returns
`SchemaMismatch` before it asks the owner to start anything.

## 5. Proof

**The roll itself.** `crates/axon-stream/tests/world_upgrade.rs` rolls five nodes, all of which are seeds and Paxos
acceptors, one at a time and in a seeded order. Each node drains with `Maintenance`, crashes, and restarts as the
next build. The next build has a test-only range that is one version higher, and it serves the user protocol at
schemas 1 and 2 instead of 1. Throughout the roll, the nodes run three kinds of traffic:

- keyed calls on 12 keys under `PaxosLeases`,
- published-name calls,
- a stream relayed from the origin through a relay to three leaves.

The test asserts the following for every seed in `SEEDS=0..512`:

- no call is lost when it is retried per `err.retry()`;
- no call waits longer than one lease plus a tick;
- no follower sees a reset, a gap or a repeat;
- sessions run the highest common version;
- nothing is in flight at the end;
- each key runs once;
- no session was refused for its version.

**Negotiation and routing.** These tests cover negotiation and schema routing:

- `crates/axon-net/tests/session.rs::a_session_runs_the_highest_version_both_speak_and_ranges_that_do_not_meet_are_refused_naming_both`
- `crates/axon-cluster/tests/upgrade.rs`
- `crates/axon-cluster/src/placement.rs::only_nodes_serving_every_callers_schema_of_a_kind_are_candidates`

## 6. Wire v8

Status: designed, not built. Wire v8 is one release and a clean break: `VERSIONS = 8..=8`. A v8 node and a v7 node
refuse each other at the handshake (`Negotiation::Version`), and no code picks a format by `Peer::version`. The
pre-1.0 policy (§1) allows it, and a window would be code deleted in the release after.

| Change | Where | Spec |
|---|---|---|
| A call carries its contract's id, and the server checks it | call body, `call_frame_bytes_are_pinned` | §6.1 |
| A new rejection, `ProtocolMismatch` | `END` answer byte, `REJECTION_NAMES` | §6.1 |
| A trace context is 25 bytes, on calls and stream batches | `TRACED` body, stream item kind 5; stream `SCHEMA` 8 | [tracing.md §1](tracing.md#1-context) |
| `axon.kinds` marks unleased kinds | node record metadata | [paxos-leases.md §11](paxos-leases.md#11-leases-per-kind-wire-v8) |

Nothing on disk changes: the lease log, a node's durable log and a stream's segments keep their formats, and a batch's
trace is not written to its segment (`disk.rs:545-592`). A node starts on v8 with its directories as they are.

### 6.1 Wire contracts

**Problem.** A contract is checked at one end. The caller compares the route's protocol id when it makes a target
(`Route::target`, `cluster.rs:196-199`). The call frame then carries the endpoint and the schema, not the contract
(`wire.rs:3-5`, the header's `protocol` field is the endpoint), and the server compares the schema alone
(`engine.rs:981-985`). A call whose target was made wrong, from a hand-built `Target`, a fixed endpoint id or a fault in
route caching, is admitted by another contract's endpoint at the same schema, and its body goes to that contract's
`decode`. And every serde contract repeats the same glue by hand (guide `cluster.md:600-630`, `json()`).

**Design.**

- A call's body starts with its contract's id, `u64`, before the trace context. The server compares `(ID, SCHEMA)`
  with the endpoint's and refuses a mismatch `Rejection::ProtocolMismatch`, which the caller sees as
  `ClusterError::ProtocolMismatch` (`retry()` `Resolve`). Eight bytes a call; `scripts/bench.sh` `route` and `remote`
  gate it.
- `WireProtocol::NAME: &'static str`, default `""`. A protocol's label, in telemetry and the console, is its `NAME`, or
  else its `ID` as 16 hex digits. axon's own: `axon.control` (the system endpoints), `axon.stream`, `axon.lease`,
  `axon.quota`, `axon.telemetry`.
- In axon-cluster, behind its `serde` feature:

  ```rust,ignore
  pub trait Codec: 'static {
      fn encode<T: Serialize>(value: &T) -> Result<Bytes, String>;
      fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, String>;
  }
  pub trait Contract: 'static {
      type Message: Serialize + DeserializeOwned + Send + 'static;
      type Reply: Serialize + DeserializeOwned + Send + 'static;
      type Codec: Codec;
      const ID: u64;
      const SCHEMA: u16;
      const LANE: Lane = Lane::Live;
      const NAME: &'static str = "";
  }
  impl<C: Contract> WireProtocol for C { .. }
  impl<C: Contract> ActorProtocol for C { type Mailbox = (C::Message, Option<Reply<C::Reply>>); .. }
  ```

  A codec error on encode is `CallError::Codec`, on decode at the server `Rejection::InvalidMessage`. A type with a
  hand-written `WireProtocol` and no `Contract` still compiles, as `ToString` beside `Display` does. axon names no
  codec: its tests and the guide use a JSON one; an application brings its own.

**Tests.**

- `crates/axon-cluster/src/wire.rs::call_frame_bytes_are_pinned`, with the id in front.
- `crates/axon-cluster/tests/cluster/remote.rs::a_call_for_one_contract_at_another_contracts_endpoint_is_refused_protocol_mismatch`
  (fails today: admitted and decoded)
- `crates/axon-cluster/tests/cluster/remote.rs::a_contract_from_serde_types_round_trips_tell_send_and_request`
- `crates/axon-cluster/tests/cluster/remote.rs::a_body_its_codec_cannot_read_is_rejected_invalid_message_and_a_bad_reply_is_a_codec_error`
- `crates/axon-obs/tests/telemetry.rs::a_protocol_is_labelled_by_its_name_or_else_its_hex_id`
- A guide program in `guide/cluster.md` (an axon-obs doctest) in place of the `json()` glue.

### 6.2 Upgrading a cluster to v8

The whole cluster at once:

1. Build every deployable at v8.
2. Stop every node and every seed server. A drain with `LeaveReason::Maintenance` hands keys to nobody: no peer on v8 is
   up yet.
3. Start them, in any order. The view forms once more than half of the seeds are past their warm-up
   ([seed-discovery.md §4.2](seed-discovery.md#42-the-seed-server)); acceptors refuse grants for their restart wait,
   `T·(1 + drift) + 200 ms` ([paxos-leases.md §2.2](paxos-leases.md#22-restarts)); eager keys start again after it. It
   costs what a cold restart costs, which the rig runs (`cold-restart`, [fault-rig.md](fault-rig.md)).
4. Check that no v7 node is left: `listener.handshake_failures{reason="version"}` stays 0 on every node.

A v7 node left running splits the cluster by version: each half sees only its own. Leased keys run only on the half
with a majority of the acceptors; unleased keys may run on both. So do not roll v8.

**Tests.** `crates/axon-net/tests/session.rs::a_v8_node_and_a_v7_node_refuse_each_other_naming_both_ranges`, through
the hidden `Config::versions`. `world_upgrade.rs` keeps rolling a test-only next version, which holds the N and N-1
mechanism for 1.0.

## Open

- §6.1: a build-time check that an application's contract ids are unique (`(ID, SCHEMA)` pairs), and a reserved range
  for axon's own ids (they spell `AXON`, `wire.rs:69`). Neither is chosen here; the server's check refuses a call that
  reaches another id, not two contracts that share one.
- §6.1: the contract id as 8 body bytes against a 16-bit tag in the frame header's reserved `u16` (`frame.rs:4`). The
  tag costs nothing and catches all but 1 in 65 536 mismatches; the id is exact. Default: the id.
