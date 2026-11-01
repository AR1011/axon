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
  This release speaks version 6 only. Version 6 added requests in parts (the `BODY` bit on `REQUEST`, `PART`, `END`
  and `CREDIT`); version 5 says in a spawn's answer whether that request started the actor, so that one node records
  an eager restart; version 4 added replies in parts (`PART`, `CREDIT`, the `PARTS` bit); version 3 put the nodes a
  keyed start passed in the spawn request and added `axon.owns`
  ([paxos-leases.md §5.3](paxos-leases.md#53-placement-under-partial-cuts)). A node of version 5 is refused at the
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
(`SCHEMA` 6), Paxos leases, quotas and telemetry, all served with `Cluster::serve`.

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
