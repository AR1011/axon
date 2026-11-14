# Design — who may do what, and the ops program

Status: designed, not built. Two pieces an operator of a cluster with several deployables needs: rules for which
deployable may call, start, watch, publish or follow what (§1), and a program that lists a running cluster's nodes,
leases, streams and links (§2). Neither changes the wire; both ship with wire v8
([rolling-upgrades.md §6](rolling-upgrades.md#6-wire-v8)).

## 1. Principals and rules

**Problem.** The only principal is the node id. `CertificateIdentity` binds the claimed id to a name the peer's
verified leaf certificate is valid for (`crates/axon-net/src/config.rs:93-103`, rustls `verify_server_name`). The
cluster's `Authorizer` judges `Action::Call { name } | Spawn { kind } | Watch { kind }` per `Peer`
(`crates/axon-cluster/src/engine.rs:46-59`), checked on each call (`engine.rs:987-991`), spawn (`spawn.rs:321`) and
watch (`spawn.rs:367`); streams have their own `Authorizer(PeerId, key, Access)` (`crates/axon-stream/src/hub.rs:29-31`).
So a service that runs many nodes of one deployable either issues a certificate per node, or writes both traits itself
and keeps a list of node ids, and must remember to allow the cluster's own `axon.*` endpoints (guide
[cluster § Security](../crates/axon/guide/cluster.md#security)). And no rule covers who may be a stream's origin:
`Access::Publish` is checked for the local publisher only (`hub.rs:936`), while a child picks parents by labels they
assert themselves (`node.rs:191-199`).

**Design.**

- **The principal is the node id after its first DNS label.** Node `n3.svc-a.example` is principal `svc-a.example`; an id
  with no dot is its own principal. One certificate per deployable, with a wildcard name (`*.svc-a.example`), then binds
  every node of that deployable to its principal: a rustls wildcard matches one left-most label, so `n3.svc-a.example`
  passes and `x.n3.svc-a.example` does not. `axon_cluster::principal(node_id) -> &str`. Under `SharedSecret` the id is
  a claim (`config.rs:66-67`), so a principal is too; the guide says so.
- **`axon_cluster::Rules`**, a table built in code:

  ```rust,ignore
  let rules = Rules::new()
      .allow("svc-a.example", Allow::call("jobs.").spawn("session").watch("session"))
      .allow("svc-b.example", Allow::stream("events", [Access::Publish]))
      .allow("ops.example", Allow::call("axon.").stream("*", [Access::Subscribe, Access::Replay]));
  cfg.authorizer = Some(Arc::new(rules.clone()));                       // calls, spawns, watches
  stream_cfg.authorizer = Arc::new(rules);                              // streams
  ```

  `Allow`'s starters each return a `Grants`, whose methods of the same names add the rest: Rust has no associated
  function and method of one name on one type. A call matches by name prefix, a kind and a stream namespace by name
  (`*` for any). Every authenticated member may
  call `axon.*` unless a rule for its principal names `axon.` itself, so the cluster keeps working under any table. A
  principal with no rule may do nothing else. `Rules` implements the cluster's `Authorizer` and, in axon-stream with
  its `cluster` feature, the stream one. `Cluster::set_authorizer` swaps it at run time, as today.
- **Who may be an origin: not built.** The design asked a relay or leaf to check `Access::Publish` for the path's
  first entry and refuse that parent `Forbidden` when it is denied. The first entry is not always the origin: a relay
  with no parent yet, at its start or after losing one, tells its children the path `[itself]`
  (`path_through`, `crates/axon-stream/src/core.rs:490-492`), the same as an origin does, so under `Rules` every
  such relay would be refused, with a backoff that grows on each refusal. The check also runs when the path arrives, and a
  parent's first pushes on another lane can arrive before it. See Open.

**Wire.** None.

**API.** `axon_cluster::{principal, Rules, Allow, Grants}`; `impl axon_stream::Authorizer for Rules`, under which a
`local:` peer, the node's own process, may do anything.

**Tests.**

- `crates/axon-cluster/tests/cluster/tls.rs::two_nodes_under_one_wildcard_certificate_share_a_principal_and_a_third_deployable_is_forbidden`
- `crates/axon-cluster/tests/cluster/tls.rs::a_name_two_labels_under_the_wildcard_fails_the_handshake`
- `crates/axon-cluster/tests/cluster/remote.rs::rules_let_every_member_call_axon_endpoints_unless_a_rule_names_them`
- The rig's `authz` fault (`examples/rig`) builds its policy from `Rules`.

## 2. The ops program

**Problem.** There is no tool that lists a running cluster. `scripts/ctl.sh` drives a playground's dashboard over HTTP
(`crates/axon-obs/src/dashboard/mod.rs:345-359`). The guide's answer for a process that only calls is to join as a
member and shut down before it exits ([cluster § Membership](../crates/axon/guide/cluster.md#membership)); every
node already answers `axon.telemetry` with its `NodeSnapshot` (`crates/axon-obs/src/model.rs:258-284`: its members,
its `ClusterStats` with the authority's `describe()` and the engine's links, its stream stats) and its streams
(`Request::Streams`), through `axon_obs::fetch` (`crates/axon-obs/src/telemetry.rs:283-286`).

**Design.** A cargo example, `crates/axon-obs/examples/ops.rs`, no new crate: axon-obs already depends on axon-cluster
and axon-stream.

- `ops <nodes|leases|streams|links> [--json]`, the discovery and the shared secret from the environment, as the
  playground takes them (`AXON_CONSUL`, `AXON_SERVICE`, `AXON_SECRET`), and `AXON_BIND` for its own address. Node id
  `<host>-<pid>.<AXON_PRINCIPAL>`, so §1's rules can let it in.
- It starts a `Cluster` that registers no kind, reads the view, and asks each member for its `NodeSnapshot`
  (`Request::Snapshot`) and, for `streams`, `Request::Streams`, at most 16 at once, each within `request_timeout`.
  A member that does not answer is a row that says so.
- `nodes`: id, incarnation, labels, reachable, isolated, wire version. `leases`: each node's authority `describe()`
  and the leased keys it holds per kind. `streams`: per node, the keys it originates and relays, with cursor and
  parent. `links`: per node, each peer and lane, up or down, round trip and wire version.
- It calls `system.shutdown()` before it exits, on SIGINT too, so it leaves the view at once instead of after idle
  plus `idle_hold`.

What a short-lived member costs: one join and one leave on every node. Each clears its route cache
(`crates/axon-cluster/src/cluster.rs:1216`) and rehashes its hosted keys after `rebalance_settle`
(`crates/axon-cluster/src/spawn.rs:1349-1400`). A member that registers no kind owns no key, so the rehash moves
nothing; the test below holds that with counts.

**Tests.**

- `scripts/check.sh` builds the example; `scripts/check.sh smoke` with `AXON_CONSUL` set runs `ops nodes --json`
  against a playground and checks every playground node answered.
- `crates/axon-cluster/tests/world.rs::a_member_that_joins_and_leaves_each_second_moves_no_key_and_drops_no_call`,
  with literal counts of keys moved and calls failed (0 and 0), and of leaves seen and nodes that went unreachable
  first (10 and 0).

## Open

- §1, who may be an origin: a path that says whether its first entry originates the stream (an origin flag in the
  answer and the `Path` item, a stream wire change), and pushes from a link held until its path is checked. Its test,
  `crates/axon-stream/tests/relay.rs::a_parent_whose_origin_may_not_publish_the_namespace_feeds_no_follower`, fails
  today (its records reach the follower), and would need a relay that loses its parent to keep its followers.
- §1: a principal from the verified certificate itself, carried as `Peer::principal` (an `Authenticator` API change),
  for certificates with URI names rather than wildcards. Not chosen: the node-id suffix needs no new plumbing.
- §2: TLS for `ops`: it takes a shared secret only, not a certificate and key from the environment.
- §2: `links` with the link meters of [lanes.md §8.1](lanes.md#81-link-meters-wire-v8) by region pair and protocol,
  once those meters land.
- §2: a join that clears no route cache when the member registers no name and no kind. Add it if the churn test shows
  a cost.
