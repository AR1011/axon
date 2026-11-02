# Cluster

Crates: `axon-cluster` for the cluster, `axon-net` for its transport settings. Every Rust snippet here is compiled,
and the complete programs are run, by `cargo test --doc -p axon-obs`.

A cluster is a set of nodes that find each other through a discovery provider, keep one authenticated TCP session per
peer, and call each other's actors by name.

```toml
[dependencies]
axon-actors = "0.1"
axon-cluster = "0.1"   # features = ["consul"] for Consul discovery and leases
axon-net = "0.1"
bytes = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## Call an actor on another node

Three parts: the actor, a wire contract that both sides compile, and the wiring.

```rust
use axon::{Actor, ActorSystem, Context};
use axon_cluster::{ActorProtocol, CallError, Cluster, ClusterConfig, MemoryDiscovery, Rejection, Reply, WireProtocol};
use axon_net::SharedSecret;
use bytes::{Buf, BufMut, Bytes, BytesMut};

// The actor. A remote request carries a `Reply` instead of a `ReplyTo`.
enum Msg {
    Add(u64),
    Get(Option<Reply<u64>>),
}

struct Counter(u64);

impl Actor for Counter {
    type Message = Msg;
    type Error = ();
    fn on_message(&mut self, msg: Msg, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            Msg::Add(n) => self.0 += n,
            Msg::Get(reply) => drop(reply.map(|r| r.send(self.0))),
        }
        Ok(())
    }
}

// The wire contract. You choose the encoding; axon moves bytes.
#[derive(Clone)] // for request_until, which may send it again
enum Op {
    Add(u64),
    Get,
}

struct CounterApi;

impl WireProtocol for CounterApi {
    type Message = Op;
    type Reply = u64;
    const ID: u64 = 0xC0;   // names the contract
    const SCHEMA: u16 = 1;  // bump on a breaking change

    fn encode(op: Op) -> Result<Bytes, CallError> {
        let mut out = BytesMut::new();
        match op {
            Op::Add(n) => {
                out.put_u8(0);
                out.put_u64(n);
            }
            Op::Get => out.put_u8(1),
        }
        Ok(out.freeze())
    }
    fn decode_reply(body: Bytes) -> Result<u64, CallError> {
        body[..].try_into().map(u64::from_be_bytes).map_err(|_| CallError::Codec("short reply".into()))
    }
}

// The serving side: what a message decodes into, and how its reply is encoded.
impl ActorProtocol for CounterApi {
    type Mailbox = Msg;
    fn encode_reply(n: u64) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&n.to_be_bytes()))
    }
    fn decode(mut body: Bytes, reply: Option<Reply<u64>>) -> Result<Msg, Rejection> {
        match body.try_get_u8() {
            Ok(0) => body.try_get_u64().map(Msg::Add).map_err(|_| Rejection::InvalidMessage),
            Ok(1) => Ok(Msg::Get(reply)),
            _ => Err(Rejection::InvalidMessage),
        }
    }
}

async fn node(id: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    let mut transport = axon_net::Config::new(id, SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true; // no TLS: fine on loopback, not across machines
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    (system, cluster)
}

#[tokio::main]
async fn main() {
    let discovery = MemoryDiscovery::new(); // two nodes in one process
    let (system_a, a) = node("a", &discovery).await;
    let (_system_b, b) = node("b", &discovery).await;
    b.wait_view(|view| view.members.len() == 2).await.unwrap();

    let counter = system_a.spawn(Counter(0)).start().unwrap();
    a.publish::<CounterApi>(&counter, "counter").unwrap();

    let remote = b.resolve::<CounterApi>("counter").await.unwrap();
    remote.tell(Op::Add(2)).await.unwrap();
    remote.send(Op::Add(5)).await.unwrap();
    assert_eq!(remote.request(Op::Get).await.unwrap(), 7);
    // The deadline goes with the request: past it, node a drops it unrun.
    assert_eq!(remote.request_within(Op::Get, std::time::Duration::from_millis(500)).await.unwrap(), 7);
    assert_eq!(remote.node(), "a");

    // Resolves on its first call, and again once the actor or its node is gone; retries what was not delivered.
    let counter = b.named::<CounterApi>("counter");
    let until = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    assert_eq!(counter.request_until(Op::Get, until).await.unwrap(), 7);
}
```

### What each call promises

| Call | Returns when | If the far side refuses |
|---|---|---|
| `tell(msg).await` | the frame is in this node's outbox | you are not told; the far node records a [dead letter](observability.md#dead-letters) |
| `send(msg).await` | the far mailbox accepted the message | `CallError::Rejected(reason)` |
| `request(msg).await` | the reply arrived | `CallError::Rejected(reason)`, or `NoReply` if the actor dropped the `Reply` or its reply did not encode |
| `request_within(msg, d).await` | the reply arrived, or `d` passed: `Timeout` | as `request`; past `d` when it comes in, the far node drops it unrun and unanswered |
| `request_parts(msg).await` | the first part, or the end, arrived ([below](#a-reply-larger-than-a-frame)) | as `request` |
| `upload(msg, parts).await` | the reply arrived; the body went in parts ([below](#a-request-larger-than-a-frame)) | as `request` |

- Calls to one peer on one [lane](#lanes) arrive in the order they were made. Nothing is retried or replayed for you, but by `request_until`
  (below). A call is made when its future first runs: two calls in two `cx.submit_future` jobs run on tokio's threads
  and can be made in either order, whatever order the handler submitted them in. Make ordered calls from one future, one
  after the other.
- A remote `send` or `request` does not wait for room in the far mailbox: a full one refuses it with `Rejected(Full)`
  at once. Waiting there would hold back every later call on the session, since they arrive in order. The exception
  is a supervised kind while it restarts: calls wait for its next generation
  ([below](#spawn-actors-on-other-nodes-one-per-key)).
- `request_timeout` is 5 s. `CallError::OutcomeUnknown` means the session died after the message left: it may or may
  not have been handled. A peer that restarts before this node notices it was gone gives it too, within a round trip:
  the next request goes out on the old session, and the new boot resets it. Nothing on this side can tell whether the
  old boot ran the request before it went.
- `request_within(msg, d)` gives the request `d` in place of `request_timeout` and sends it along: what is left of `d`
  as the frame is made, a duration and never a time of day, so clocks that disagree cannot expire or extend it. The far
  node takes off half the shortest of its session's last four keepalive round trips for the way there (a pause or a
  backlog lengthens one sample, not the shortest), and drops a request with nothing left unrun and unanswered, counted in
  `cluster.inbound_expired`; so too one still waiting for a supervised kind's next generation at the deadline. A
  request it runs carries a `Reply` whose `is_abandoned()` turns true at that deadline on the far node's clock, or when
  the caller's session goes, so a handler that holds the reply across a job can skip work nobody waits for, and whose
  `time_left()` says how much of it is left, zero once it has passed. A plain `request` sends no deadline: `time_left()`
  is `None`, on the caller's own node too, and on another node its `Reply` turns abandoned only when the session goes,
  although the caller stopped waiting at its `request_timeout`; on the caller's own node, when the caller stops
  waiting. The far
  node's deadline is off by the difference between the way there and half that round trip (a queue in either outbox, a
  slower way back, a way that grew in the last four keepalives) and by its clock's drift over `d` (5000 ppm is 0.5 %). Until the answer to a
  session's first keepalive, sent as it comes up, nothing is taken off, so a request in that first round trip may
  run up to one way after its caller gave up. Its
  `Timeout` still means it may have been handled: one run near its deadline can answer too late. A `KeyedRef` or
  `NamedRef` has it too, with `d` covering the lookup.
- A request must fit one frame (`max_frame`, 1 MiB by default), a `request_within` body 8 bytes less, for its
  deadline, and a `request_parts` or `upload_parts` message 4 bytes less, for its window. A larger request sends its
  body in parts: [below](#a-request-larger-than-a-frame). A reply must fit one frame too, unless it comes in parts:
  [below](#a-reply-larger-than-a-frame). `Reply::send` returns the
  `encode_reply` error, or `NotSent(Oversized)` for a reply over `Reply::max_payload()`; the caller then gets `NoReply`.
  A caller that has gone is not an error. So the handler owes nothing more, and the examples here drop the result.
- A call to an actor on the same node skips the network.
- A `RemoteRef` points at one actor on one boot of one node and never moves. `err.retry()` on a `CallError` or a
  `ClusterError` says what to do next, as below.
- `resolve(name)` looks once and wants exactly one hit in the cluster. None is `NotFound`, and a node that could not
  be asked fails it with that node's error. Two are `ClusterError::Ambiguous`: use `resolve_on(node, name)`, or
  `resolve_all(name)`, which answers the references it found and, apart, each node it could not ask with the error.
  It asks the nodes side by side, so a silent one costs one `request_timeout`, not the whole lookup. A lookup sends
  nothing to the actor, so a node that does not answer it fails with `NotSent`, whose `retry()` is `Later`.
  `resolve_all_among(name, &placement)` asks only the nodes that carry every label `placement` requires, so a silent
  node without them costs nothing.
- `cluster.named(name)` gives a `NamedRef`: it resolves on its first call, keeps the reference, and drops it after an
  error whose `retry()` is `Resolve`, so the next call resolves again, on whichever node publishes the name by then. Its
  lookup is `resolve`, so a silent node fails it with `NotSent` until that node is marked unreachable, about 5.5 s.
  `.among(&placement)` narrows the lookups to the nodes that carry the labels `placement` requires, as
  `resolve_all_among` does, so a silent node without them holds up no call. A `KeyedRef` does the
  same for a key ([below](#spawn-actors-on-other-nodes-one-per-key)). Neither resends a call: the error is yours, except
  through `request_until(msg, deadline)`. It requests again after an error whose `retry()` is `Later` or `Resolve`, and
  looks again after any lookup error but a `Never`; 10 ms apart, doubling to 100 ms, until
  the deadline, and then it returns the last error. Each try is a `request_within` of what is left, up to
  `request_timeout`, so its deadline travels too. A request still out at the deadline returns `Timeout`, and one not yet
  sent `NotSent(Deadline)`. To wait for a
  name to be published, call `request_until`. `RemoteRef::request_until` does the same for one reference, and requests
  again only after `Later`: after `Resolve` that reference cannot work.
- `cluster.nodes(&placement)` is the live set of the available nodes that carry every label `placement` requires: the
  runners of a platform, the shards of a store. It reads the view at each call and sends nothing. `ids()` lists them;
  `order(key)` ranks them for a key by rendezvous hashing, as keyed placement does, so every node with the same view
  tries the same node first for a key, a node leaving moves only the keys it led, and the rest of the order is where
  to go when the first refuses. Call one with `resolve_on(node, name)`, which the route cache answers without a round
  trip while it holds the route, and all of them side by side with `resolve_all_among(name, &placement)`. A node in
  the set that does not publish the name answers `NotFound`, whose `retry()` is `Resolve`: go on to the next.
  `changes()` yields `NodeChange::Joined` for each node now, then `Joined` and `Left` as the set changes, and ends
  when the cluster shuts down, for an actor to `cx.attach`, as a consumer of a stream per producer does
  ([streams § Many origins](streams.md#many-origins)). A node that turns unreachable or drains leaves the set; one
  that restarts before anyone misses it stays, and its old references fail with `StaleNode`.
- `SCHEMA` is checked on every call. To change it, serve the old and the new side by side: [§ Rolling upgrades](#rolling-upgrades).

| `Retry` | Means | For example | Do |
|---|---|---|---|
| `Later` | not delivered; the same reference may work after a pause | `Busy` (this node has `max_pending` calls out, or `max_peers` links for a new peer), `NotSent(Full \| Deadline \| Closed)`, `Rejected(Full)`, `Spawn(Held \| Capacity \| Ownership \| Isolated)`, `Discovery(Unavailable \| Stale)` | wait, then call again; `request_until` does that |
| `Resolve` | not delivered; the reference or this node's view is out of date | `UnknownActor`, `Stopped`, `StaleNode` (the view names another boot of the node), `NoRoute` (the node left the view), each also when its session closed under the call, `Unreachable`, `NotFound`, `NoCandidate`, `SchemaMismatch` (a node does not serve the caller's schema), `Spawn(NotOwner \| Leaving \| UnknownKind)` | resolve again, then call; a `KeyedRef` or `NamedRef` resolves on its next call, and `request_until` also makes that call |
| `IfIdempotent` | it may have been handled | `Timeout`, `OutcomeUnknown`, `NoReply` | call again only if repeating is safe |
| `Never` | retrying fails the same way | `Codec`, `Closed`, `Rejected(SchemaMismatch)`, `Forbidden`, `Spawn(Failed \| Forbidden)` | stop and report it |

`Never` is about this call from this node. Another target may still work, and `Closed` says this node's own cluster
is shut down or shutting down: work that must still happen, such as a keyed actor's next step, is the next owner's
once this node is gone, not a reason to undo it.

Sending one message at a time costs a round trip each. To keep a far actor busy, keep a window of sends in flight
and send again, backing off, what a full mailbox refused. That gives up order: a message sent again lands after later
ones. It also waits as long as the far actor stays full, so bound the call with `tokio::time::timeout`. Dropping a
call's future, as the timeout does, gives back its `max_pending` slot at once and counts it `abandoned`; the far side
is not told, and may still handle it. `request_within` tells it.

```rust,no_run
use std::time::Duration;

use axon_cluster::{CallError, RemoteRef, Retry, WireProtocol};
use tokio::task::JoinSet;

async fn send_all<P>(remote: &RemoteRef<P>, messages: Vec<P::Message>, window: usize) -> Result<(), CallError>
where
    P: WireProtocol + 'static,
    P::Message: Clone + Send + 'static,
{
    let mut flight = JoinSet::new();
    for message in messages {
        if flight.len() == window {
            flight.join_next().await.expect("a send in flight").expect("a send task")?;
        }
        let remote = remote.clone();
        flight.spawn(async move {
            let mut pause = Duration::from_millis(1);
            while let Err(e) = remote.send(message.clone()).await {
                if e.retry() != Retry::Later {
                    return Err(e);
                }
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(Duration::from_millis(100));
            }
            Ok(())
        });
    }
    flight.join_all().await.into_iter().collect()
}
```

Where order matters, batch instead: many items in one message cost one round trip and arrive together.

### Lanes

A node holds up to three sessions to each peer, one per `Lane`. `Control` carries axon's own traffic: membership,
heartbeats, lease rounds, lookups, spawns and `round_trip`. `Live` carries calls, and is where a protocol's calls go
unless it says otherwise. `Bulk` carries large transfers, with 8 MiB frames and 32 MiB outboxes. Each session has its
own socket, outbox and frame size, so an upload that fills `Bulk` does not delay a `Live` call or a lease.

A protocol picks its lane with `WireProtocol::LANE`; its replies, parts and upload bodies stay on that session. A raw
endpoint picks it with `cluster.endpoint(name, protocol, schema).lane(lane).serve(handler)`. The serving node's
choice travels in the route, so callers send where it says. `publish` and `kind` refuse `Control`, and the builder
gives it only to names under `axon.`.

```rust,no_run
use axon::Lane;
use axon_cluster::{CallError, Cluster, ClusterError, Rejection, WireProtocol};
use bytes::Bytes;

// Backups: large bodies that must not hold up the calls on `Live`.
struct Backups;

impl WireProtocol for Backups {
    type Message = Bytes;
    type Reply = ();
    const ID: u64 = 0xB4;
    const SCHEMA: u16 = 1;
    const LANE: Lane = Lane::Bulk;
    fn encode(body: Bytes) -> Result<Bytes, CallError> {
        Ok(body)
    }
    fn decode_reply(_: Bytes) -> Result<(), CallError> {
        Ok(())
    }
}

// The same on a raw endpoint.
fn serve_raw(cluster: &Cluster) -> Result<u64, ClusterError> {
    cluster.endpoint("backups", Backups::ID, Backups::SCHEMA).lane(Lane::Bulk).serve(|_peer, body, reply| {
        let reply = reply.ok_or(Rejection::InvalidMessage)?;
        drop(body);
        reply.send(Bytes::new()).map_err(|_| Rejection::Stopped)
    })
}
```

A `Live` or `Bulk` session is dialled on first use, to the boot `Control` reaches, and closes when `Control` goes down
or idles out like any other. Only `Control` decides whether a peer is reachable. A `Live` or `Bulk` session that fails
while `Control` stays up fails only the calls on it, with `NotSent(Closed)`, `Retry::Later`: the peer stays reachable
and keeps its keyed actors and leases, and the session is dialled again with backoff. `cluster.stats().engine.peers`
has a row for that lane with `down_ms`, how long it has had no session, and `cluster.events()` records one `lane_down`
naming the peer and the lane, then one `lane_up` once it is back.

### A reply larger than a frame

A value larger than a frame (a file, a generated response) comes back in parts. The caller asks with
`request_parts(msg)` and reads `parts.next().await` until `None`. The handler answers with
`reply.send_parts(stream)`, a future that pulls its stream only as fast as the caller reads: the caller grants
4 MiB of credit and grants more as it reads, so neither side holds more than that and one part. `None` means the
handler's stream ended. Any other ending, such as an error in the stream, the actor stopping, or a panic, gives the
caller `Err(NoReply)` after the parts it has, and never a clean end. Run the future as a job: a drain waits for it,
and a stop cuts the reply. Dropping `Parts` stops the far stream at once, even one waiting for its next part. Each
part is due within `request_timeout`, or the whole reply within `d` with `request_parts_within(msg, d)`, and past
that the reader gets `Timeout` and the far stream is stopped. Keep parts at 64 to 256 KiB, so other calls on the
session pass between them. A reply cut part way is not resent. Put the position in the message, as here, and ask
again from what you have.

- `request_parts` returns at the first part, and its errors are `request`'s, with the same `retry()`: a `NamedRef` or
  `KeyedRef` resolves again after `Resolve`, as for `request`. After it, a part fails only with `Timeout`,
  `OutcomeUnknown`, `NoReply` or `Codec`, and that error ends the parts.
- A reply in parts holds a `max_pending` slot on the caller and a `max_inbound_replies` slot on the handler's node
  until it ends, and a session carries at most 64 at once: the next is refused `Rejected(Full)`, `Later`. A `Parts`
  nobody reads holds them, so drop what you stop reading. An actor reads one with `cx.attach(parts, map)`: a full
  mailbox pauses it, and the credit with it.
- `Reply::send(r)` to a `request_parts` is one part, and `send_parts` to a plain `request` refuses it as
  `Rejected(InvalidMessage)` for both. What `send_parts` returns is about the reply: `Ok` when it ended or the caller
  went away, else the stream's error, the encoder's, or `NotSent(Oversized)` for a part over `max_payload()`; the
  caller has been told `NoReply` by then.

```rust
use axon::{Actor, ActorSystem, Context};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, MemoryDiscovery, Rejection, Reply, Retry, WireProtocol,
};
use axon_net::SharedSecret;
use bytes::{Bytes, BytesMut};

/// Asks for the blob from byte `from` on, so a cut fetch goes on where it stopped.
struct BlobApi;

impl WireProtocol for BlobApi {
    type Message = u64;
    type Reply = Bytes;
    const ID: u64 = 0xB1;
    const SCHEMA: u16 = 1;
    fn encode(from: u64) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&from.to_be_bytes()))
    }
    fn decode_reply(part: Bytes) -> Result<Bytes, CallError> {
        Ok(part)
    }
}

impl ActorProtocol for BlobApi {
    type Mailbox = Msg;
    fn encode_reply(part: Bytes) -> Result<Bytes, CallError> {
        Ok(part)
    }
    fn decode(body: Bytes, reply: Option<Reply<Bytes>>) -> Result<Msg, Rejection> {
        let from = body[..].try_into().map(u64::from_be_bytes).map_err(|_| Rejection::InvalidMessage)?;
        Ok(Msg::Fetch(from, reply))
    }
}

enum Msg {
    Fetch(u64, Option<Reply<Bytes>>),
    Sent(Result<(), CallError>),
}

struct Blob(Bytes);

impl Actor for Blob {
    type Message = Msg;
    type Error = String;
    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Msg::Fetch(from, Some(reply)) = msg else { return Ok(()) }; // Sent: the caller has been told
        let size = reply.max_payload().min(256 << 10);
        let rest = self.0.slice((from as usize).min(self.0.len())..);
        let parts = (0..rest.len()).step_by(size).map(move |at| Ok(rest.slice(at..(at + size).min(rest.len()))));
        // Pulled as the caller reads; a drain waits for it, a stop cuts it.
        let sending = reply.send_parts(futures_util::stream::iter(parts));
        cx.submit_future(async move { Msg::Sent(sending.await) }).map_err(|e| format!("{e:?}"))?;
        Ok(())
    }
}

async fn node(id: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    let mut transport = axon_net::Config::new(id, SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true;
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    (system, cluster)
}

#[tokio::main]
async fn main() {
    let discovery = MemoryDiscovery::new();
    let (system_a, a) = node("a", &discovery).await;
    let (_system_b, b) = node("b", &discovery).await;
    b.wait_view(|view| view.members.len() == 2).await.unwrap();
    let value = Bytes::from((0..12u32 << 20).map(|i| (i % 251) as u8).collect::<Vec<u8>>());
    let actor = system_a.spawn(Blob(value.clone())).start().unwrap();
    a.publish::<BlobApi>(&actor, "blob").unwrap();

    // On another node: 12 MiB with one call, asked again from where it stopped if it was cut.
    let blob = b.named::<BlobApi>("blob");
    let mut have = BytesMut::new();
    'fetch: loop {
        let mut parts = blob.request_parts(have.len() as u64).await.unwrap();
        while let Some(part) = parts.next().await {
            match part {
                Ok(bytes) => have.extend_from_slice(&bytes),
                Err(e) if e.retry() == Retry::IfIdempotent => continue 'fetch, // reading is idempotent
                Err(e) => panic!("{e:?}"),
            }
        }
        break;
    }
    assert_eq!(have.freeze(), value);

    // A stop cuts the reply: the parts that came, then NoReply, never the clean end.
    let mut parts = blob.request_parts(0).await.unwrap();
    actor.lifecycle.stop();
    let cut = loop {
        match parts.next().await {
            Some(Ok(_)) => {}
            other => break other,
        }
    };
    assert!(matches!(cut, Some(Err(CallError::NoReply))), "{cut:?}");
}
```

### A request larger than a frame

A value larger than a frame (a file to store, a binary to deploy) goes to the handler in parts. The caller calls
`upload(msg, parts)`: `msg` is an ordinary message, and `parts` is a stream of `Result<Bytes, CallError>`, each part at
most `max_frame`. The handler takes the parts with `reply.upload()`, an `Upload` the first time it asks and `None` for
a plain request, and reads `upload.next().await` until `None`. The caller starts with 4 MiB of credit and gets more as
the handler reads, so neither side holds more than that and one part, and other calls on the session go between the
parts. Keep parts at 64 to 256 KiB. `upload` returns the reply.

- The handler can answer at any time. The answer, or a dropped `Upload`, stops the body: the caller sends nothing
  more. A handler that does not take the `Upload` stops the body when it answers.
- Any other end of the body gives the handler `Err(NoReply)` after the parts it has: an error in the caller's stream,
  which `upload` returns, the caller dropping the call or a panic in its stream, a part over `max_frame`
  (`NotSent(Oversized)` to the caller), or no credit for `request_timeout` (`Timeout` to the caller).
  `Err(OutcomeUnknown)` says the session failed, and `Err(Timeout)` that no part came for the handler node's
  `request_timeout`. The reply is due within `request_timeout` of the handler's last read: the tail of the body may
  still be on the wire when the stream ends.
- The errors of an upload before its message goes are `request`'s, with the same `retry()`, and a `NamedRef` or
  `KeyedRef` looks again after `Resolve`, as for `request`. Nothing sends it again: once the message has gone the
  stream is spent, even when the handler then refuses it. Make the stream again to send again.
- An upload holds a `max_pending` slot on the caller until it ends, and a `max_inbound_replies` slot on the handler's
  node until the answer. A session carries at most 64 bodies at once, each counted until its `Upload` ends or is
  dropped, after the answer too: the next is refused `Rejected(Full)`, `Later`.
- `upload_parts(msg, parts)` gets the reply in parts too ([above](#a-reply-larger-than-a-frame)): a stream each way. It
  returns at the first part of the reply and sends the body from a task of its own, so `parts` is `Send + 'static`. The
  end of the reply, or a dropped `Parts`, cuts a body still under way. `reply.send_parts(upload)` sends the body back
  as it comes.

**Or put it on `Bulk`, or raise `max_frame`.** A body of a few MiB can go as one frame on a protocol whose `LANE` is
`Bulk` ([above](#lanes)): 8 MiB frames, on a session of its own, so `Live` calls do not wait behind it. Or set
`transport.lanes.live.limits.max_frame` on every node, and `outbound_bytes` and `inbound_bytes` above it. That costs memory and
time: the whole body is in memory at both ends, a frame is written whole, so every other call on that session waits
behind it, and every session can hold that much. With 10 MB in one frame on loopback, small requests on the same
session wait for it: [benchmarks § Requests in parts](benchmarks.md#requests-in-parts).

```rust
use std::collections::BTreeMap;

use axon::{Actor, ActorSystem, Context};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, ClusterError, MemoryDiscovery, Rejection, Reply, WireProtocol,
};
use axon_net::SharedSecret;
use bytes::{Bytes, BytesMut};

/// Stores a file under the name the message carries; the file comes in parts.
struct FilesApi;

impl WireProtocol for FilesApi {
    type Message = String;
    type Reply = u64;
    const ID: u64 = 0xF1;
    const SCHEMA: u16 = 1;
    fn encode(name: String) -> Result<Bytes, CallError> {
        Ok(name.into())
    }
    fn decode_reply(body: Bytes) -> Result<u64, CallError> {
        body[..].try_into().map(u64::from_be_bytes).map_err(|_| CallError::Codec("a u64".into()))
    }
}

impl ActorProtocol for FilesApi {
    type Mailbox = Msg;
    fn encode_reply(stored: u64) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&stored.to_be_bytes()))
    }
    fn decode(body: Bytes, reply: Option<Reply<u64>>) -> Result<Msg, Rejection> {
        let name = String::from_utf8(body.into()).map_err(|_| Rejection::InvalidMessage)?;
        Ok(Msg::Put(name, reply.ok_or(Rejection::InvalidMessage)?))
    }
}

enum Msg {
    Put(String, Reply<u64>),
    Stored(String, Bytes),
    Cut,
}

#[derive(Default)]
struct Files(BTreeMap<String, Bytes>);

impl Actor for Files {
    type Message = Msg;
    type Error = String;
    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Put(name, reply) => {
                let Some(mut upload) = reply.upload() else { return Ok(()) }; // a plain request: NoReply
                // Read in a job: each part read gives the caller more credit.
                let read = async move {
                    let mut file = BytesMut::new();
                    while let Some(part) = upload.next().await {
                        let Ok(part) = part else { return Msg::Cut }; // the reply drops: NoReply
                        file.extend_from_slice(&part);
                    }
                    let _ = reply.send(file.len() as u64);
                    Msg::Stored(name, file.freeze())
                };
                cx.submit_future(read).map_err(|e| format!("{e:?}"))?;
            }
            Msg::Stored(name, file) => drop(self.0.insert(name, file)),
            Msg::Cut => {}
        }
        Ok(())
    }
}

async fn node(id: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    let mut transport = axon_net::Config::new(id, SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true;
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    (system, cluster)
}

#[tokio::main]
async fn main() {
    let discovery = MemoryDiscovery::new();
    let (system_a, a) = node("a", &discovery).await;
    let (_system_b, b) = node("b", &discovery).await;
    b.wait_view(|view| view.members.len() == 2).await.unwrap();
    let actor = system_a.spawn(Files::default()).start().unwrap();
    a.publish::<FilesApi>(&actor, "files").unwrap();

    // 10 MiB from another node, in parts of 256 KiB that share one Bytes.
    let file = Bytes::from(vec![7u8; 10 << 20]);
    let size = 256 << 10;
    let parts = (0..file.len()).step_by(size).map(|at| Ok(file.slice(at..(at + size).min(file.len()))));
    let files = b.named::<FilesApi>("files");
    assert_eq!(files.upload("app.wasm".into(), futures_util::stream::iter(parts)).await.unwrap(), 10 << 20);

    // A stream that fails cuts the body: upload returns its error, and the handler reads Err(NoReply).
    let failing = futures_util::stream::iter([Ok(file.slice(..size)), Err(CallError::Codec("disk".into()))]);
    let cut = files.upload("other.wasm".into(), failing).await;
    assert!(matches!(cut, Err(ClusterError::Call(CallError::Codec(_)))), "{cut:?}");
}
```

### A contract with several calls

One contract can carry every call an actor answers: an enum of calls, an enum of answers, and a typed client with a
method per call, so callers never match on the answer. You pick the encoding; here it is `serde_json`.

```rust
# use axon::{Actor, ActorSystem, Context};
# use axon_cluster::{Cluster, ClusterConfig, MemoryDiscovery};
use axon_cluster::{ActorProtocol, CallError, RemoteRef, Rejection, Reply, WireProtocol};
# use axon_net::SharedSecret;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
enum Call { Deposit(u64), Balance }

#[derive(Serialize, Deserialize)]
enum Answer { Done, Balance(u64) }

struct AccountApi;

fn json<T: Serialize>(value: &T) -> Result<Bytes, CallError> {
    serde_json::to_vec(value).map(Bytes::from).map_err(|e| CallError::Codec(e.to_string()))
}

impl WireProtocol for AccountApi {
    type Message = Call;
    type Reply = Answer;
    const ID: u64 = 0xAC;
    const SCHEMA: u16 = 1;
    fn encode(call: Call) -> Result<Bytes, CallError> {
        json(&call)
    }
    fn decode_reply(body: Bytes) -> Result<Answer, CallError> {
        serde_json::from_slice(&body).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for AccountApi {
    type Mailbox = (Call, Option<Reply<Answer>>);
    fn encode_reply(answer: Answer) -> Result<Bytes, CallError> {
        json(&answer)
    }
    fn decode(body: Bytes, reply: Option<Reply<Answer>>) -> Result<Self::Mailbox, Rejection> {
        serde_json::from_slice(&body).map(|call| (call, reply)).map_err(|_| Rejection::InvalidMessage)
    }
}

struct Account(u64);

impl Actor for Account {
    type Message = (Call, Option<Reply<Answer>>);
    type Error = ();
    fn on_message(&mut self, (call, reply): Self::Message, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
        let answer = match call {
            Call::Deposit(n) => {
                self.0 += n;
                Answer::Done
            }
            Call::Balance => Answer::Balance(self.0),
        };
        drop(reply.map(|r| r.send(answer)));
        Ok(())
    }
}

struct AccountClient(RemoteRef<AccountApi>);

impl AccountClient {
    async fn deposit(&self, n: u64) -> Result<(), CallError> {
        match self.0.request(Call::Deposit(n)).await? {
            Answer::Done => Ok(()),
            _ => Err(CallError::Codec("unexpected answer".into())),
        }
    }
    async fn balance(&self) -> Result<u64, CallError> {
        match self.0.request(Call::Balance).await? {
            Answer::Balance(n) => Ok(n),
            _ => Err(CallError::Codec("unexpected answer".into())),
        }
    }
}

# async fn node(id: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
#     let mut transport = axon_net::Config::new(id, SharedSecret::new("at-least-16-bytes-long").unwrap());
#     transport.allow_plaintext = true;
#     let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
#     let system = ActorSystem::builder().build().unwrap();
#     let cluster = Cluster::start(&system, cfg).await.unwrap();
#     (system, cluster)
# }
#
#[tokio::main]
async fn main() {
#     let discovery = MemoryDiscovery::new();
#     let (system_a, a) = node("a", &discovery).await;
#     let (_system_b, b) = node("b", &discovery).await;
#     b.wait_view(|view| view.members.len() == 2).await.unwrap();
    // Two nodes as in the first program; the account lives on `a`, the client on `b`.
    a.publish::<AccountApi>(&system_a.spawn(Account(0)).start().unwrap(), "account").unwrap();

    let account = AccountClient(b.resolve::<AccountApi>("account").await.unwrap());
    account.deposit(30).await.unwrap();
    assert_eq!(account.balance().await.unwrap(), 30);
}
```

The other way is one contract per call, each with its own `ID`, `Message` and `Reply`, published under its own name
on the same actor: no answer enum to match, but a resolve per call and a mailbox type that holds them all.

## Spawn actors on other nodes, one per key

A node that registers a kind can be asked to create actors of that kind. `get_or_spawn` gives you the one actor for a
key, wherever it lives, creating it on first use. A caller that only needs it running, not its protocol, asks with
`get_or_spawn::<Unaddressed>`: the reference it gets carries the node and epoch and sends nothing.

```rust
use axon::{Actor, ActorSystem, Context};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, MemoryDiscovery, Owner, Placement, Rejection, Reply,
    SpawnContext, WireProtocol,
};
use axon_net::SharedSecret;
use bytes::Bytes;

struct Session(String); // answers every request with its key

impl Actor for Session {
    type Message = Option<Reply<String>>;
    type Error = ();
    fn on_message(&mut self, reply: Option<Reply<String>>, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
        drop(reply.map(|r| r.send(self.0.clone())));
        Ok(())
    }
}

struct SessionApi;

impl WireProtocol for SessionApi {
    type Message = ();
    type Reply = String;
    const ID: u64 = 0x5E;
    const SCHEMA: u16 = 1;
    fn encode(_: ()) -> Result<Bytes, CallError> {
        Ok(Bytes::new())
    }
    fn decode_reply(body: Bytes) -> Result<String, CallError> {
        String::from_utf8(body.to_vec()).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for SessionApi {
    type Mailbox = Option<Reply<String>>;
    fn encode_reply(key: String) -> Result<Bytes, CallError> {
        Ok(Bytes::from(key))
    }
    fn decode(_body: Bytes, reply: Option<Reply<String>>) -> Result<Self::Mailbox, Rejection> {
        Ok(reply)
    }
}

async fn node(id: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    let mut transport = axon_net::Config::new(id, SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    cfg.labels.insert("region".into(), "eu".into());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    cluster
        .kind::<SessionApi>("session")
        .register(|cx: &SpawnContext, _args: Bytes| Ok(Session(cx.key.clone().unwrap_or_default())))
        .await
        .unwrap();
    (system, cluster)
}

#[tokio::main]
async fn main() {
    let discovery = MemoryDiscovery::new();
    let (_system_a, a) = node("a", &discovery).await;
    let (_system_b, b) = node("b", &discovery).await;
    for cluster in [&a, &b] {
        cluster.wait_view(|view| view.members.iter().filter(|m| m.record.has_kind("session")).count() == 2).await.unwrap();
    }

    let eu = Placement::default().require("region", "eu");
    let from_a = a.get_or_spawn::<SessionApi>("session", "alice", Bytes::new(), &eu).await.unwrap();
    let from_b = b.get_or_spawn::<SessionApi>("session", "alice", Bytes::new(), &eu).await.unwrap();
    assert_eq!(from_a.node(), from_b.node()); // one actor, whoever asks
    assert_eq!(from_b.request(()).await.unwrap(), "alice");
    let alice = b.keyed::<SessionApi>("session", "alice", Bytes::new(), &eu); // asks on its first call
    assert_eq!(alice.request(()).await.unwrap(), "alice");
    assert_eq!(alice.node().as_deref(), Some(from_a.node())); // the node it called
    assert_eq!(a.owner("session", "alice", &eu).as_deref(), Some(from_a.node())); // asks no one
    let sessions = b.keyed_refs::<SessionApi>("session", &eu, 1024); // kept: each key's owner asked once
    assert_eq!(sessions.get("alice", Bytes::new()).request(()).await.unwrap(), "alice");
    assert_eq!(a.nodes(&eu).ids().len(), 2); // the nodes labelled region=eu
    let mut alice_runs = b.watch_key("session", "alice", &eu); // where alice runs, as it changes
    assert_eq!(alice_runs.next().await, Some(Owner::Running { node: from_a.node().into(), epoch: None }));

    let fresh = a.spawn::<SessionApi>("session", Bytes::new(), &eu).await.unwrap(); // a new actor each call
    let pinned = a.spawn_on::<SessionApi>("b", "session", Bytes::new()).await.unwrap();
    assert_eq!(pinned.node(), "b");
    drop(fresh);
}
```

- Labels come from `ClusterConfig::labels`, and `NodeRecord::label(key)` reads one. `require` filters nodes; `prefer`
  ranks them. A label holds one value, matched whole: `role=edge,origin` matches neither `require("role", "edge")` nor
  `"origin"`. Give a node with two roles a label for each, such as `edge=yes` and `origin=yes`.
- Three labels also mean something to the console: `role` names what a node is for, `country` (an ISO 3166-1
  alpha-2 code such as `JP`) draws its flag, and `region` (free text, such as `Tokyo`) is shown beside it.
- The owner of a key is picked by rendezvous hashing over the matching nodes, so every node picks the same owner
  without talking. When nodes join or leave, keyed actors that are now misplaced drain and are handed to the new
  owner, and then give up their lease; so does a key a node finishes starting after its view moved the key elsewhere,
  as when the owner restarts while another node waits out its lease. That starts `rebalance_settle` (500 ms) after a
  change, and changes within that window move keys together, so a rolling deploy moves keys a few times, not once per
  node. Drains run side by side; at most a quarter of `max_pending` (up to 256) keys are asked of their new owners at
  once.
- Different keys start side by side on a node. Two starts of the same key run one after the other, and the second
  gets the actor the first one started. A start that finds the key's actor draining or stopping waits for it to end
  and starts the next one; after `operation_timeout` it is refused `Held`.
- Use the same `args` and `placement` for a key everywhere. They are part of what the key means.
- A factory that needs the cluster takes the `WeakCluster` in `SpawnContext::cluster`, and captures none: the cluster
  holds the factory, so a `Cluster` captured there keeps it alive after `shutdown`. `upgrade()` gives the `Cluster`
  while it runs. The actor may keep that, as it ends with the shutdown.
- Each `get_or_spawn` is a request to the key's owner, even when the actor is running. Keep the reference, or call
  through `cluster.keyed(kind, key, args, &placement)`: a `KeyedRef` asks on its first call, keeps the reference, and
  drops it after an error whose `retry()` is `Resolve`, so the next call asks again. It resends nothing; the error is
  yours to retry, or `request_until` retries a request ([above](#what-each-call-promises)). A tell to another node
  is never refused, but that node says when the actor a tell went to is gone, having moved or ended: the tells sent
  before that news is back go to dead letters, and the first after it fails with `Resolve`. Use `send` where losing
  those matters. A `Timeout` is not `Resolve`: an owner that is reachable but answers too late, or never, keeps the key,
  and each call times out again. Nothing moves the key while its actor may still run, because a second one elsewhere
  would act beside it, which only a store that fences writes by epoch makes safe. The key moves when its owner is
  unreachable or leaves, or under an authority loses its lease. Until then, keep the actor's handler fast (slow work
  through `cx.submit_io`), call with `request_within` so the far node drops what it could not start in time, or have
  the actor stop itself (`cx.stop()`) when it sees it is stuck: a call then fails with `Resolve`, and the next starts
  the key again.
- To call many keys of a kind, keep their references in `cluster.keyed_refs(kind, &placement, max)`: `get(key, args)`
  gives the `KeyedRef` kept for a key, or makes and keeps one, so a key's owner is asked once, not before every call.
  It keeps at most `max` (at least 2), always those of the last `max / 2` keys got, and a clone shares them. What it
  keeps behaves as any kept `KeyedRef`: it calls where its key ran when it last asked, and asks again after `Resolve`.
  A key got again with other `args` keeps the first. A fresh `cluster.keyed(..)` asks the owner on its first call,
  which costs a round trip and finds a silent owner before the request is sent.
- `cluster.owner(kind, key, &placement)` names the node this node's view places a key on now, the one
  `get_or_spawn` asks, without asking it; `None` when no member matches. A `KeyedRef`'s `node()` and `epoch()` are
  those of the reference it keeps: `None` before its first call and after a call failed with `Resolve`.
- `cluster.hosted(limit)` is what this node hosts, read from memory: the kinds registered here (`eager` or not),
  the first `limit` keys here in kind and key order with the lease epoch each started under (`None` without an
  ownership authority), and `total`, every key here. A key is here from its start until its actor has ended, so one
  whose lease is lost, or that moves, leaves once its actor has stopped, and a moved key shows on its new owner under
  the next epoch. Each kind also has its `count` of keys here, kept as keys start and end, and the keys `started`
  here by how (`KEYED_STARTS`: a call, or a hand-off from a node that moved the key or left) and `ended` here by why
  (`KEYED_ENDS`: stopped, failed, lease_lost, isolated, moved); telemetry stores them as `cluster.keyed`,
  `cluster.keyed_started` and `cluster.keyed_ended` by kind. Each snapshot carries the kinds, and
  `TelemetryConfig::snapshot_keys` (none by default) keys, at most a quarter of the transport's `max_frame` of them.
- `cluster.hosted_keys(kind, prefix, from, limit)` pages the keys instead: those of `kind` here that start with
  `prefix`, in key order from `from` on, at most `limit`, and whether more follow. It reads a range of the node's
  map, so it costs the same at a million keys as at ten; the console's placement page asks it through telemetry
  (`api/keys`).
- `cluster.watch_key(kind, key, &placement)` is a stream of `Owner`: first what the key's owner runs now, then
  `Running { node, epoch }` each time the key starts and `Stopped` each time it ends. It asks the node this node's
  view places the key on, the one `get_or_spawn` asks first, which answers as its actor for the key starts or ends,
  and it asks the next owner when the view changes. Nothing queues: a watcher that reads late gets the state at that
  moment and skips what it missed. `Stopped` also means the owner cannot be asked: a crashed process at once, a
  dead machine or a cut-off owner once the call to it times out or its session idles out. The key's next start then
  shows as `Running` on its new node, which happens when something calls it, as below, with the same delay. A key
  moving to a node that joined shows `Stopped` from when the watcher's view names that node, while the old node
  still drains it. A key started past its owner ([below](#one-owner-per-key)) shows `Stopped` until it moves back.
  An actor that has lost its lease, or is stopping, shows as `Stopped` although it has not yet
  ended. Under a lease authority the epochs shown never go back, so no epoch is ever shown on two nodes; the same
  one can show again after a `Stopped` for an owner that could not be asked. Without one, a watcher on the other
  side of a split or with a different view can see another node. Each watch holds one request to the owner,
  counted in `max_pending` here and `max_inbound_replies` there, which it shares with every other call: enough
  watches on one owner leave its other callers refused `Full`. The watch ends when the cluster shuts down, or when
  the owner refuses it for good, as its `Authorizer` does a peer it does not allow `Action::Watch { kind }`.
- A keyed actor is named `kind/key` on its node, so the console lists it by its key and `system.lookup`
  (`lookup_supervised` for a kind with a policy) finds it. A key that cannot make a name (over 256 bytes with its
  kind, or holding a control character) runs unnamed. A start that finds its name taken is refused `Held` before
  it runs anything, as when the key's last actor is still ending. Leave names of that form to keyed actors: an
  actor you name `kind/key` keeps that key from starting on its node for as long as it runs.
- Keyed actors use the shared pool and the system mailbox unless the kind says otherwise:
  `cluster.kind::<P>(kind).execution(Execution::Pool("keys")).mailbox(limits)` gives each key's actor those
  instead. `Pool` is the useful choice; `Dedicated` means one thread per key.
- `cluster.kind::<P>(kind).idle(d)` ends a key whose actor has handled no message for `d`: it drains, gives up its
  lease and frees its slot, and the next `get_or_spawn` starts it again, fresh, from its args. Its timers and job
  results do not keep it alive, and a job still out after another `d` does not hold it. A paused actor is not idle:
  bound the job it waits for yourself. A `get_or_spawn` that lands while it drains waits for it to end, as above.
- A clean leave hands each key to its new owner at once. After a crash a key starts again when it is next called, on
  its new owner, or by itself if its kind is eager: `cluster.kind::<P>(kind).eager(args, placement)`. Once a node is
  seen gone (it left or is unreachable), each survivor asks the lease authority which keys of its eager kinds the node
  held, and starts those it owns, with the `args` and `placement` given to `eager`. That is paced as a rebalance is:
  `rebalance_settle` after the change, and at most `max_pending / 4` keys (up to 256) asked at once. The lease still
  decides, so no key runs twice: a key refused `Held` is asked again while the node still holds it, which is until its
  lease runs out (the "crashed holder" row of the table below: 6 s under `PaxosLeases`) or another node takes it, for
  at most a minute and a lease after its first try. Under `ConsulLeases` keep twice its `ttl` plus `lock_delay` inside
  that minute, or such keys wait for a caller. A restarted node that registers an eager kind starts the keys it held
  before, too. From the authority's first answer, and for a minute and a lease after it, the node also asks every
  20 s which leases have run out without a release, and starts those it owns: what a node that went while no peer was
  up to see it held, as when the whole cluster restarts and one node does not come back, or when a node ends just
  after this one starts. Under `PaxosLeases` that holds even when every acceptor restarted, since each logs its leases
  (below): such a node asks until the acceptors answer, after their wait of a little over one lease. The acceptors
  list a lapsed lease for a minute after its end (after their wait, if they restarted), so a key whose lease lapsed
  longer ago than that, while no node of its kind asked, waits for a caller. A question about a gone node that the
  authority does not answer within 30 s ends there, and the node asks for lapsed leases instead, as above. A key
  another node still holds is refused `Held` and left to it, so if that node goes later, unseen by any peer of the
  kind, the key waits for a caller. Each key waits for its restart once per node, however many questions listed it,
  and starts at most once per epoch: `cluster.stats().eager_waiting` counts the keys still waiting, and
  `eager_asking` the questions about a node not answered yet. Every key of an eager kind starts from the same `args`,
  so callers must pass those too, and what differs per key comes from a store the actor reads by
  `SpawnContext::key`. An eager kind needs
  `ClusterConfig::ownership` set to `PaxosLeases`, or `ConsulLeases` created with the node's id: `register` refuses it
  with `ClusterError::Config` otherwise, as with `MemoryLeases`, which cannot list a node's keys.
- Without `eager`, a key that works by itself, on timers or jobs, needs a caller that knows the keys and calls each
  one when `MemberChange::Unreachable` or `MemberChange::Left` names a node. Such a call succeeds once the node is seen
  gone (a crashed process in 2 s, a dead machine in 8 to 10 s) and the lease authority frees the key; until then it is
  refused with an error whose `retry()` is `Later` or `Resolve`, so a caller's deadline for it must cover both.
- A key keeps nothing: after a move, a crash, a restart or an idle stop, its actor starts fresh from its args. Put
  what must survive in a store the actor writes before it acknowledges, fenced by its epoch (below), or have callers
  send again with ids the actor can recognise twice, or keep it in the node's log ([below](#a-keys-state-on-disk)).
  To load that state in `on_start` before handling any message,
  use [`cx.pause_until`](actors.md#pausing-until-a-job-ends): a kind whose actor pauses in `on_start` is offered to
  callers once the pause lifts, within the system's `start_timeout`.
- Under a lease authority a move runs in order: the old actor's `on_stop`, the new owner taking the key on (it answers
  once the start has a place there, then waits for the lease), the release of the lease, then the new actor's start.
  The old node keeps the lease until the new owner has answered, asking the owner its view names again after a
  refusal, for at most `operation_timeout`, so a move cut short by a crash leaves the key listed as the crashed
  node's, and an eager kind's survivors start it as after any crash. A key of an eager kind that no owner took on
  starts again from the old node, and one whose new owner answered and then could not start it (its lease did not
  come within `operation_timeout`) starts again from the new owner, as after a lost lease (below). Without a lease
  authority, a caller whose view already names the new owner can start the key there while the old actor still drains.
- Three caps bound a node's keyed actors: `RuntimeConfig::max_actors` (65 536, every actor the node runs) and
  `ClusterConfig::max_spawned` (65 536, the actors other nodes asked it for), past which a start is refused
  `SpawnRefusal::Capacity`, and `PaxosConfig::max_leases` (100 000 keys for the whole cluster, since every acceptor
  sees every key), past which it is refused `Ownership`. Raise all three together.
- A kind is 1 to 64 bytes without `,` or `/`, and is refused with `ClusterError::Config` otherwise: its keys' leases
  are named `kind/key`.
- By default a key is unique per membership view: during a network split both sides can run it. For strict
  single ownership set `ClusterConfig::ownership` to a lease authority ([below](#one-owner-per-key)). The actor
  then gets a fencing `epoch` in `SpawnContext::epoch` and `RemoteRef::epoch()`; pass it to whatever you write to,
  and have that store refuse older epochs, and a write whose epoch it cannot check as well. A restart under the
  kind's policy keeps the epoch: where the store must tell its starts apart, add `SpawnContext::start`. Ids kept
  to recognise a request twice must be kept as long as any sender may still send it again.
- A keyed actor that fails (a handler error, a panic, a start that fails) stops and is counted in
  `ClusterStats::keyed_failed`; the next `get_or_spawn` builds it again from the same args. Register the kind with
  `.policy(RestartPolicy)` on `cluster.kind::<P>(kind)` and it is rebuilt in place under that policy, with fresh
  state, keeping its key, endpoint, lease and every `RemoteRef` to it; only when the policy gives up does the key end.
  Nothing else starts it again: a key that works by itself, on timers or jobs, and fails with no policy, or past it,
  stays down until it is called, even if its kind is eager, which restarts the keys of a node that is gone and those
  its own node stopped for a lost lease or isolation (below), not one that failed. Its `get_or_spawn` does not wait
  for `on_start`: a start that keeps failing shows as `keyed_failed` and `UnknownActor` on the reference, not as a
  refusal. Between two generations a `send` or `request` waits for the next, as a local `send` does, up to the hosting
  node's request timeout even if the caller gave up sooner; a `tell` is dropped as a `stopped` dead letter. A message
  the failed generation already took is not sent again, and calls that did not wait for each other may reach the new
  generation out of order. Each call waiting holds one of the host's `max_inbound_replies` (4096), and past that it is
  refused `Full`. A call gives its one back before its answer leaves; a `request_within` waiting there gives it back
  at the host's reading of its deadline, on the host's timer, which can trail the caller's: a call made just as one
  times out may still be refused `Full`, a `Later`.
- A lease another node still holds (it died without leaving, and the authority's lock delay has not passed) is
  retried every `HELD_RETRY` (200 ms) until `operation_timeout`, then refused with `SpawnRefusal::Held`;
  `SpawnRefusal::Ownership` is the authority being unavailable. Under `PaxosLeases` the owner names the holder
  (`OwnershipError::Held(Some(node))`, the refusal's text) and refuses at once, and `get_or_spawn` asks the holder
  instead when it reaches it, or asks the owner again every `HELD_RETRY` while it does not.
- A node that discovery has not heard for `heartbeat_grace` (5 s) asks up to 8 peers whether they still list it,
  waiting for each at most `operation_timeout` or half the grace, whichever is shorter. One that does vouches for it.
  Otherwise the node is isolated (`ClusterStats::isolations` counts it, and `/readyz` answers 503): `get_or_spawn` of
  a key it does not run is refused `SpawnRefusal::Isolated` ("this node cannot confirm it is still a member"), whose
  `retry()` is `Later`; under a lease authority the caller asks the key's next node instead
  ([below](#one-owner-per-key)). A key it runs still answers. Without an ownership authority, or once a peer answers
  that it dropped the node, the node stops its keyed actors at once. With an authority and no peer answering, each
  runs on until its lease's fence stops it (`lease_grace`, below), so a cut shorter than that costs no key, and one
  longer moves it only once the lease has lapsed. A node too starved to heartbeat and answer in time isolates
  itself like a cut-off one: keep blocking and busy work off the cluster's runtime. It rejoins once it has been
  heard, or vouched for, for a whole `heartbeat_grace` with no gap as long as the grace: a node heard less often
  stays isolated instead of going in and out, and one heard more often, which would not isolate again, rejoins; the
  keys of an eager kind it stopped then start again by themselves, as a lost lease's do (below). Before the node
  is first heard, its view not yet formed, nothing is recorded: with seed discovery its heartbeats are not heard
  until the seeds are past their warm-up (5 s after they start), so it may refuse starts until then, and its first
  hearing ends that at once.
- A strictly owned actor stops once the last renew that extended its lease was sent `lease_grace` (about 5 s) ago,
  even while a renew is still waiting for its answer: the authority last heard from it then. It stops at once when the
  authority says another node holds its key now. Keys are renewed together but fail alone: `PaxosLeases` decides each
  key on its own, so one key taken over or short of a majority costs only its own actor. The actor checks its deadline
  itself before every message and timer, so a process paused or starved past it wakes to actors that stop before
  handling what queued meanwhile (`StopReason::Fenced`). A pause in the middle of a handler still finishes that
  handler late, which only the store's epoch check catches. Either way the next `get_or_spawn` starts the key again,
  under a new epoch, once the actor that lost it has released it, and a key of an eager kind starts again without one,
  whether the renew loop or the actor's own check saw the lease go first: the node that lost it asks the owner, as
  survivors do a gone node's keys, for at most a minute. No other node would, since its node stays a member and the
  acceptors may list the key as nobody's. A node that leaves meanwhile hands such keys to their owners, which keep at
  them; its leave waits for that as for its other hand-offs. `ConsulLeases` holds every key of a
  node under one session, and replaces that session when it lapsed while there was nothing to renew. A key it may have
  left locked, by a release or an acquire whose answer was lost, is unlocked then or on a later renewal, and only
  while no acquire of the node holds it or is taking it: a release that reaches Consul late, after the node took the
  key again, leaves the key held. An authority that panics is answered as unavailable, and the grace decides.

### A key's state on disk

A keyed actor that must keep its state across a crash writes it to its node's disk before it answers. Give the node
one [`files::Log`](actors.md#a-log-of-keyed-records) for every key of the kind, kept by one actor, so that one write
and one sync cover all the keys that changed meanwhile. A file per key costs a sync per key: on build-1, 16 keys
took 24 ms that way against 1.3 ms in one zero-filled file, and 256 keys 343 to 379 ms
([spec/durable-log.md](../../../spec/durable-log.md#2-why-a-file-per-owner-not-per-key-or-per-node)). The node's
log actor opens the log in `on_start`, appends as it is told, and answers each append once it is durable. Each
account sends one record at a time, which holds every deposit that came while the last one was out, and answers those
deposits once the log has answered it.

```rust
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use axon::files::{Address, Entry, Files, Log, LogConfig, LogError, Lsn, MemoryFiles};
use axon::{Actor, ActorHandle, ActorSystem, Context, ReplyTo, RequestError};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, MemoryDiscovery, Placement, Rejection, Reply, SpawnContext,
    WireProtocol,
};
use axon_net::SharedSecret;
use bytes::Bytes;

/// How long the log waits after a failed write before it writes the same bytes again.
const REST: Duration = Duration::from_millis(100);

/// A key's epoch and what the log holds of it.
type Loaded = io::Result<(u64, Vec<Entry>)>;

enum ToLog {
    Opened(io::Result<Box<Log>>),
    /// A key's actor starts: the key is fenced at a new epoch, and read once the fence is durable.
    Load(Bytes, ReplyTo<Loaded>),
    Read(Bytes, u64, io::Result<Vec<Entry>>, ReplyTo<Loaded>),
    /// Answered once the record is durable.
    Append(Bytes, u64, Bytes, ReplyTo<Result<Address, LogError>>),
    Checkpoint(Bytes, Address, Bytes),
    Written(io::Result<()>),
    /// The rest after a failed write is over.
    Rested,
}

enum Waiter {
    Load(Bytes, u64, ReplyTo<Loaded>),
    Append(Address, ReplyTo<Result<Address, LogError>>),
}

/// The node's log: one for every key of the kind here.
struct NodeLog {
    files: Arc<dyn Files>,
    log: Option<Log>,
    /// In LSN order. Bounded by `max_unwritten_bytes`, past which an append is refused `Behind`.
    waiting: VecDeque<(Lsn, Waiter)>,
    /// Keys whose fence is durable, to read once fewer than `READS` reads are out.
    to_read: VecDeque<(Bytes, u64, ReplyTo<Loaded>)>,
    reads: usize,
    /// After a failed write, until `REST` has passed.
    resting: bool,
}

/// Reads out at once. With the write, fewer jobs than the actor's `MailboxLimits::jobs` (16), so neither is refused
/// however many keys start together.
const READS: usize = 8;

impl NodeLog {
    fn new(files: Arc<dyn Files>) -> Self {
        let (waiting, to_read) = (VecDeque::new(), VecDeque::new());
        Self { files, log: None, waiting, to_read, reads: 0, resting: false }
    }
}

impl Actor for NodeLog {
    type Message = ToLog;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let open = Log::open(self.files.clone(), LogConfig::default());
        let job = cx.submit_future(async move { ToLog::Opened(open.await.map(Box::new)) });
        cx.pause_until(&job.map_err(|e| format!("{:?}", e.reason))?).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: ToLog, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(log) = self.log.as_mut() else {
            // The pause in on_start holds every other message until this one.
            let ToLog::Opened(opened) = msg else { return Err("not open".into()) };
            self.log = Some(*opened.map_err(|e| e.to_string())?);
            return Ok(());
        };
        match msg {
            ToLog::Opened(_) => return Err("opened twice".into()),
            ToLog::Load(key, reply) => {
                // A new epoch at each start: what an older start of the key still sends is refused `Fenced`.
                let epoch = log.key(&key).map_or(1, |k| k.fence + 1);
                match log.fence(&key, epoch) {
                    Ok((lsn, _)) => self.waiting.push_back((lsn, Waiter::Load(key, epoch, reply))),
                    Err(refused) => drop(reply.reply(Err(io::Error::other(refused)))),
                }
            }
            // A write removed a segment before the read got to it.
            ToLog::Read(key, epoch, Err(e), reply) if e.kind() == io::ErrorKind::NotFound => {
                self.reads -= 1;
                self.to_read.push_front((key, epoch, reply));
            }
            ToLog::Read(_, epoch, entries, reply) => {
                self.reads -= 1;
                drop(reply.reply(entries.map(|e| (epoch, e))));
            }
            ToLog::Append(key, epoch, body, reply) => match log.append(&key, epoch, body) {
                Ok(appended) => self.waiting.push_back((appended.lsn, Waiter::Append(appended.at, reply))),
                Err(refused) => drop(reply.reply(Err(refused))),
            },
            ToLog::Checkpoint(key, through, state) => drop(log.checkpoint(&key, through, state)),
            ToLog::Rested => self.resting = false,
            ToLog::Written(result) => {
                // A disk that fails at once would otherwise be tried again at once, and again.
                self.resting = result.is_err() && cx.schedule_after(REST, ToLog::Rested).is_ok();
                let durable = log.written(result);
                let done = self.waiting.partition_point(|(lsn, _)| *lsn <= durable);
                for (_, waiter) in self.waiting.drain(..done) {
                    match waiter {
                        // Every record before the fence is durable too: the read sees them all.
                        Waiter::Load(key, epoch, reply) => self.to_read.push_back((key, epoch, reply)),
                        Waiter::Append(at, reply) => drop(reply.reply(Ok(at))),
                    }
                }
            }
        }
        Ok(())
    }

    /// Every change of the turn goes in one write; those of the turns while it is out go in the next.
    fn on_turn_end(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(log) = self.log.as_mut() else { return Ok(()) };
        while self.reads < READS
            && let Some((key, epoch, reply)) = self.to_read.pop_front()
        {
            // The whole key: a checkpoint every few records keeps that short.
            let read = log.read(&key, Address::default(), usize::MAX);
            let job = cx.submit_future(async move { ToLog::Read(key, epoch, read.await, reply) });
            job.map_err(|e| format!("{:?}", e.reason))?;
            self.reads += 1;
        }
        if !self.resting
            && let Some(write) = log.take_write(axon::trace::unix_micros() / 1000)
        {
            let job = cx.submit_future(async move {
                let written = axon::catch_panic(write).await;
                ToLog::Written(written.unwrap_or_else(|panic| Err(io::Error::other(panic.to_string()))))
            });
            job.map_err(|e| format!("{:?}", e.reason))?;
        }
        Ok(())
    }
}

/// A deposit of n answers the balance after it; 0 reads the balance.
struct AccountApi;

fn amount(bytes: &[u8]) -> Option<u64> {
    bytes.try_into().ok().map(u64::from_le_bytes)
}

impl WireProtocol for AccountApi {
    type Message = u64;
    type Reply = u64;
    const ID: u64 = 0xAD;
    const SCHEMA: u16 = 1;
    fn encode(n: u64) -> Result<Bytes, CallError> {
        Ok(Bytes::copy_from_slice(&n.to_le_bytes()))
    }
    fn decode_reply(body: Bytes) -> Result<u64, CallError> {
        amount(&body).ok_or_else(|| CallError::Codec("not a u64".into()))
    }
}

impl ActorProtocol for AccountApi {
    type Mailbox = Msg;
    fn encode_reply(balance: u64) -> Result<Bytes, CallError> {
        Self::encode(balance)
    }
    fn decode(body: Bytes, reply: Option<Reply<u64>>) -> Result<Msg, Rejection> {
        amount(&body).map(|n| Msg::Call(n, reply)).ok_or(Rejection::InvalidMessage)
    }
}

enum Msg {
    Call(u64, Option<Reply<u64>>),
    Loaded(Result<Loaded, RequestError>),
    Written(Result<Result<Address, LogError>, RequestError>),
}

/// The records after a checkpoint that make the next one.
const CHECKPOINT_EVERY: u64 = 64;

/// A balance kept in its node's log.
struct Account {
    key: Bytes,
    log: ActorHandle<ToLog>,
    epoch: u64,
    balance: u64,
    /// Since the key's checkpoint, the records read at the start too: a key that starts often still checkpoints.
    records: u64,
    /// The deposits of the next record, and of the record out.
    queued: Vec<(u64, Option<Reply<u64>>)>,
    out: Vec<(u64, Option<Reply<u64>>)>,
}

impl Account {
    fn new(key: Bytes, log: ActorHandle<ToLog>) -> Self {
        Self { key, log, epoch: 0, balance: 0, records: 0, queued: Vec::new(), out: Vec::new() }
    }

    /// Sends the queued deposits as one record, unless one is out.
    fn write(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        if !self.out.is_empty() || self.queued.is_empty() {
            return Ok(());
        }
        self.out = std::mem::take(&mut self.queued);
        let sum: u64 = self.out.iter().map(|(n, _)| n).sum();
        let body = Bytes::copy_from_slice(&sum.to_le_bytes());
        let (log, key, epoch) = (self.log.clone(), self.key.clone(), self.epoch);
        let job = cx.submit_future(async move { Msg::Written(log.ask(|r| ToLog::Append(key, epoch, body, r)).await) });
        job.map(drop).map_err(|e| format!("{:?}", e.reason))
    }
}

impl Actor for Account {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let (log, key) = (self.log.clone(), self.key.clone());
        let job = cx.submit_future(async move { Msg::Loaded(log.ask(|r| ToLog::Load(key, r)).await) });
        cx.pause_until(&job.map_err(|e| format!("{:?}", e.reason))?).map_err(|e| format!("{e:?}"))
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        match msg {
            Msg::Loaded(loaded) => {
                let (epoch, entries) = loaded.map_err(|e| e.to_string())?.map_err(|e| e.to_string())?;
                self.epoch = epoch;
                for entry in entries {
                    match entry {
                        Entry::Checkpoint { state, .. } => self.balance = amount(&state).ok_or("not a u64")?,
                        Entry::Record { body, .. } => {
                            self.balance += amount(&body).ok_or("not a u64")?;
                            self.records += 1;
                        }
                    }
                }
            }
            Msg::Call(0, reply) => drop(reply.map(|r| r.send(self.balance))),
            Msg::Call(n, reply) => {
                self.queued.push((n, reply));
                self.write(cx)?;
            }
            Msg::Written(Ok(Ok(at))) => {
                for (n, reply) in std::mem::take(&mut self.out) {
                    self.balance += n;
                    drop(reply.map(|r| r.send(self.balance)));
                }
                self.records += 1;
                if self.records >= CHECKPOINT_EVERY {
                    // The balance stands for every record through `at`, which go. Refused, the next record tries.
                    let state = Bytes::copy_from_slice(&self.balance.to_le_bytes());
                    if self.log.tell(ToLog::Checkpoint(self.key.clone(), at, state)).is_ok() {
                        self.records = 0;
                    }
                }
                self.write(cx)?;
            }
            // Refused (`Behind`, `Full`): those deposits did not happen, and their callers get `NoReply`.
            Msg::Written(Ok(Err(refused))) if !matches!(refused, LogError::Fenced { .. }) => {
                self.out.clear();
                self.write(cx)?;
            }
            // Fenced by a newer start of the key, or no answer from the log, so the record may yet be written: end
            // here, and the next call starts the key again from what the log holds.
            Msg::Written(other) => return Err(format!("{other:?}")),
        }
        Ok(())
    }
}

async fn node(files: Arc<dyn Files>) -> (ActorSystem, Cluster) {
    let mut transport = axon_net::Config::new("a", SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true;
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, MemoryDiscovery::new());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let log = system.spawn(NodeLog::new(files)).start().unwrap();
    log.ready().await.unwrap(); // the log is open: a log that does not open fails here, not at each key
    cluster
        .kind::<AccountApi>("account")
        .register(move |cx: &SpawnContext, _args| {
            Ok(Account::new(cx.key.clone().unwrap_or_default().into(), log.clone()))
        })
        .await
        .unwrap();
    cluster.wait_view(|view| view.members.iter().any(|m| m.record.has_kind("account"))).await.unwrap();
    (system, cluster)
}

#[tokio::main]
async fn main() {
    // On a server, a directory of its own: Arc::new(OsFiles::new("/var/lib/my-app/accounts")).
    let files: Arc<dyn Files> = Arc::new(MemoryFiles::new());
    let (system, cluster) = node(files.clone()).await;
    let alice = cluster.keyed::<AccountApi>("account", "alice", Bytes::new(), &Placement::default());
    assert_eq!(alice.request(30).await.unwrap(), 30); // answered once it is durable
    assert_eq!(alice.request(12).await.unwrap(), 42);
    system.shutdown().await.unwrap();

    // The node again, on the same files: alice starts from what the log holds.
    let (system, cluster) = node(files).await;
    let alice = cluster.keyed::<AccountApi>("account", "alice", Bytes::new(), &Placement::default());
    assert_eq!(alice.request(0).await.unwrap(), 42);
    system.shutdown().await.unwrap();
}
```

- The log is the node's, not the key's. Without replication a key's state stays on its node: keep the kind on nodes
  that keep their disks, and each key on one node, with a `Placement` that requires a label only that node carries.
  A key that moves to another node starts there from what that node's log holds of it.
- Each start of a key fences it at the next epoch of this log, so a record an older start of the key still sends is
  refused `Fenced`, and the read after the fence sees every record before it. Do not use the lease epoch of
  `SpawnContext::epoch` here: a key restarted in place by its `RestartPolicy` keeps its lease and epoch, and the log
  refuses a fence at or below the key's fence.
- An answer means the record is on disk. A call that got no answer (`NoReply`, `Timeout`) may still have been written:
  give deposits ids the actor can recognise twice if callers send again.
- A failed write keeps its bytes, and the log actor writes them again after `REST`. Appends go on until
  `max_unwritten_bytes` and then are refused `Behind`; refuse callers sooner if `writing_since()` is too long ago.
  [`world_log.rs`](../../axon-cluster/tests/world_log.rs) runs this program on the World: a deposit answered survives
  a crash of the process or the machine at any moment, a write that panics, a full disk, and more keys starting at
  once than the log actor may run jobs.

## One owner per key

A lease authority makes a keyed actor run on at most one node, even across a network split. Pick one:

| | `MemoryLeases` | `ConsulLeases` (feature `consul`) | `PaxosLeases` |
|---|---|---|---|
| Needs | one process | a Consul cluster | 1, 3 or 5 of your own nodes as acceptors |
| A crashed holder's key is free after | never by itself: a test calls `MemoryLeases::expire` | the session TTL up to twice over, then `lock_delay`: 15 to 25 s by default | the lease: 6 s |
| Keeps granting while | the process runs | Consul has its quorum | a majority of the acceptors answer |
| Use when | tests and examples | you already run Consul | you do not want to run Consul, or 25 s is too long to wait for a key |

```rust,no_run
use std::net::SocketAddr;
use std::sync::Arc;

use axon::ActorSystem;
use axon::files::OsFiles;
use axon_cluster::{Cluster, ClusterConfig, ClusterError, Discovery, PaxosConfig, PaxosLeases};

async fn start(
    system: &ActorSystem,
    bind: SocketAddr,
    transport: axon_net::Config,
    discovery: impl Discovery,
) -> Result<Cluster, ClusterError> {
    let mut cfg = ClusterConfig::new(bind, transport, discovery);
    // The same list on every node. A node on the list also answers as an acceptor, and logs its leases in a
    // directory of its own that outlives the process.
    let mut paxos = PaxosConfig::new(["seed-a", "seed-b", "seed-c"]);
    paxos.store = Some(Arc::new(OsFiles::new("/var/lib/my-app/leases")));
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).map_err(ClusterError::Ownership)?));
    Cluster::start(system, cfg).await
}
```

- The epoch fences only a store that compares it. For one that cannot, such as files, give each owner a log of its
  own named by its epoch. A new owner seals the older logs where it read them, before it writes, and readers ignore
  anything past a seal. A lock is not a fence: a holder paused past its lease wakes still holding it, and writes.
  In a World test, `w.suspend(node, d)` makes such a holder: its clocks stand still while it sleeps, so it wakes inside
  its fence and writes at its old epoch, which only the store's check refuses. Under `w.pause` its clock runs on, so
  it wakes past its fence and stops the key first.
  The ledger example in the AR1011/axon-examples repository works this way.
- `PaxosLeases` needs no leader. A key is granted once a majority of the acceptors accept it, and an
  acceptor keeps a lease for `PaxosConfig::lease` (6 s, at most 5 min: `PaxosLeases::new` refuses a longer one). The
  holder believes it for less, allowing for `drift` (1 %) and a 200 ms margin. `lease_grace` defaults to 7/8 of that and `lease_renew_interval` to 2/5 of the grace, so a
  shorter lease needs no other setting; one the holder believes for less than the idle timeout also shortens the
  session timeouts ([membership](#membership)). `Cluster::start` refuses a `lease_grace` set longer than what the
  holder believes.
- An acceptor logs which node holds each key in `PaxosConfig::store`, and answers a grant or release only once it is
  synced there: `Cluster::start` refuses a node on the acceptor list without one (`ClusterError::Config`), and reads it
  before the node announces itself, failing when it cannot (`Unavailable`). Give it a
  directory of its own that outlives the process, `axon::files::OsFiles`, and a new `OsFiles` for each
  `PaxosLeases`. An `OsFiles` locks its directory until it is dropped, and a failed `Cluster::start` leaves it in
  its `PaxosLeases`: drop that, and retry with a new `PaxosLeases` and `OsFiles`, which may get `Unavailable` (in
  use) until the failed attempt's last write lands; keep the old one and every retry does. The lock is per object, so
  two `PaxosLeases` given one `OsFiles` are not refused, and write over each other's log. A renewal writes nothing.
  When every acceptor restarts at once, the log is what lets the eager keys run again (above). A write that fails is
  tried again every second, and one that has not landed within a round's patience (a sixth of the lease) counts as
  failing until it does; meanwhile the acceptor answers as if it had no log, and its provider card shows `store
  failing`. Past 4 MiB not yet written it answers new grants `Full` (`store behind`). The log names the
  configuration it was written for (the acceptors, `lease` and `drift`) and its format: an acceptor lists nothing
  from a log of another, or of an older format, and replaces it, so changing any of them starts the log afresh. It
  still takes the highest epoch from a log of another configuration, so epochs grow across the change. The
  numbered `leases-<n>.log` files of the earlier format are neither read nor removed: an acceptor upgraded from it
  lists nothing it held before, and you may delete them.
- A started acceptor answers nothing for a little over one lease. It promised more than its log holds, which records
  only who holds each key, so it waits until whatever it promised has run out. When every acceptor starts at once,
  nothing can be owned for those 6 s. In a rolling deploy, restart the acceptors one at a time and wait until each has
  finished warming up (`PaxosLeases::warming` or `cluster.warming()`, also on its provider card and
  [`/readyz`](observability.md#health-probes)) before the next. The holders keep their leases and epochs throughout.
- The acceptors belong on machines that stay: the seed servers, if you run them. A replaced machine that keeps the
  acceptor's node id and its log's directory is a restart; never run two processes with one id. Changing the list
  means stopping every node, then starting them with the new one, not one at a time: two lists running side by side
  can each find a majority.
- Every node needs the same acceptors, `lease` and `drift`, in any order. A node whose settings differ gets no answer
  from the acceptors, only `Unavailable`, and its provider card says why on each link: its majority could otherwise
  share no acceptor with the others', or its lease outlast a restarted acceptor's wait. It cannot catch a node whose
  own list is a majority without the others, such as one that lists only itself. A refused node stays a member and
  keys are still placed on it. Once its requests to the acceptors have failed it refuses their starts
  `SpawnRefusal::Isolated` and callers pass it, as below, but a start that reaches it first is refused `Ownership`
  and retried, and its keys run away from their owner until it is fixed. Change these settings on every node at once,
  restarting them together, not one node at a time.
- An acceptor that does not answer within a sixth of the lease counts as absent for that request, alive or not. A
  round left without a majority so is refused `Unavailable`, its reason ending "acceptors did not answer", which is
  `SpawnRefusal::Ownership` and `Retry::Later` to `get_or_spawn`; the provider card says on each link why (timed out,
  stopped, another list). On a machine too starved to answer in time, the key is granted when asked again.
- A cut need not split the cluster in two: a node can reach its callers but no majority of the acceptors, or the
  callers can reach the acceptors but not the key's owner. A node that cannot grant (`OwnershipAuthority::can_grant`:
  more than half the acceptors reachable in its view and not failing) refuses a start at once with
  `SpawnRefusal::Isolated`. `get_or_spawn` passes an owner that is unreachable, leaving or `Isolated` and asks the next
  node of the rendezvous order, three nodes in all; a node that names another as the holder sends it there. The key then
  runs away from its owner, where `cluster.owner` and `watch_key` still look (`watch_key` shows `Stopped`), until the
  owner says it takes keys again (open, a member, able to grant): the node running the key asks it every
  `operation_timeout` and moves the key only on a yes. Each node asked past the owner, and each holder asked, is one
  more request, counted in `ClusterStats::spawn_requests`. A holder the callers cannot reach keeps the key while it keeps its lease.
  [spec/paxos-leases.md § 5.3](../../../spec/paxos-leases.md#53-placement-under-partial-cuts) has the rules and their
  tests.
- Every node that owns keys holds a connection to each acceptor, and after an acceptor starts they all dial it at once.
  It takes `accept.max_handshakes` (64) of them at a time and the rest wait their turn, within the transport's
  `handshake_timeout`.
- Epochs are the acceptors' clocks in microseconds, and a key's epoch never goes below one granted before, whatever
  the clocks do: each acceptor's log keeps the highest epoch it granted, and a restarted acceptor promises nothing
  at or below it. One gap is left: an acceptor whose log writes fail or hang answers without waiting for them, so if
  the acceptors that granted an epoch all restart before their writes land, with clocks more than a lease behind it,
  a lower epoch can follow. The provider card counts `clock behind` when it sees a clock behind an epoch asked for.
- Two starts of a key on one node can share an epoch: a restart under the kind's `.policy`, and a start while the
  node still holds the lease of the actor before it. `SpawnContext::start` numbers each start in the node's
  process, so `(epoch, start)` tells them apart and grows with each start of the key: it counts from 1 again after
  the node restarts, and the new process takes a new lease, of a larger epoch.
- Exclusivity does not depend on clocks agreeing, only on none running more than `drift` faster than another. The
  protocol is model-checked in [spec/tla](../../../spec/tla/PaxosLeases.tla); the design is in
  [spec/paxos-leases.md](../../../spec/paxos-leases.md).
- The paxos example in the AR1011/axon-examples repository runs the acceptors as processes of their own, which
  also serve as the seed servers, and shows a crashed holder's keys moving and a lost majority stopping them.

## Membership

```rust,no_run
use axon_cluster::{Cluster, MemberChange, MembershipEvent};

async fn watch(cluster: &Cluster) {
    let mut events = cluster.subscribe(); // the first event is always a Snapshot
    while let Some(event) = events.next().await { // ends when the cluster shuts down
        match event {
            MembershipEvent::Snapshot(view) | MembershipEvent::Resync(view) => println!("{} members", view.members.len()),
            MembershipEvent::Changes { changes, .. } => {
                for change in changes.iter() {
                    match change {
                        MemberChange::Joined(node) => println!("joined {}", node.id),
                        MemberChange::Left(node) => println!("left {}", node.id),
                        MemberChange::Replaced { current, .. } => println!("restarted {}", current.id),
                        MemberChange::Updated { current, .. } => println!("metadata changed on {}", current.id),
                        MemberChange::Unreachable(id) => println!("unreachable {id}"),
                        MemberChange::Reachable(id) => println!("reachable again {id}"),
                    }
                }
            }
            MembershipEvent::Unavailable(why) => println!("discovery is failing: {why}"),
        }
    }
}
```

The subscription is also an `axon::Stream`, so an actor can take one with `cx.attach(cluster.subscribe(), map)`. It
holds up to `ClusterConfig::event_capacity` events; a subscriber further behind gets `Resync` with the current view.

`cluster.view()` is the current list, and `cluster.wait_view(|view| ..)` returns the first view, current or later,
that satisfies the condition, or `Closed` if the cluster shuts down before one does. It has no deadline of its own;
wrap it in `tokio::time::timeout`. A harness that runs nodes as processes can have each print a line once `wait_view`
has seen the peers it needs, and wait for that line instead of polling; `scripts/ctl.sh up` prints its JSON only once
the console answers. Members are what discovery lists; `reachable` is what this node can connect to.
A node holds a `Control` session to each peer it has called, with a 1 s keepalive by default, so a dead node is marked
unreachable in about 4 s, and one that is paused or cut off, whose address still takes connections that are never
answered, in about 5.5 s: the idle timeout, then a redial that has not connected within `connect_timeout`, 1.5 s. A
node that never called it learns from discovery or on its next call, whose dial counts it unreachable when the
connection is refused or not accepted within `connect_timeout`. One asked to start a key it would place on that node tries
it first, so a crashed owner's keys move as soon as a caller has seen it go, about a second later when its machine went
silent. One that already holds a session to a silent machine waits for the idle timeout, as above. When the lease
authority's `hold`, how long a holder believes its lease, is shorter than the idle timeout, `Cluster::start` makes the
idle timeout the hold and the keepalive and `connect_timeout` at most a quarter of it, for every session: a holder silent that long holds
nothing, so a silent holder's key moves with its lease. A 1 s `PaxosLeases` lease holds 0.79 s, so every peer is marked
unreachable after 0.79 s of silence, with a keepalive of about 0.2 s.
Only keyed actors move by themselves, and after a crash only once called again, or at once if their kind is eager
([keyed actors](#spawn-actors-on-other-nodes-one-per-key)). Other work a node held, such as jobs it took from a queue,
is yours to hand back when `MemberChange::Left` or `MemberChange::Unreachable` names it.
`system.shutdown()` leaves cleanly: the node marks itself draining, hands each keyed actor's key to its new owner as
soon as that actor has stopped, side by side and while slower actors still drain, and releases its lease once that owner
has taken the key on (as a move does). Then it closes its session with the lease authority and unregisters, in at least
its share, with the other extensions, of the eighth of `shutdown_timeout` that actors slow to drain leave. A hand-off
not done three eighths into that share is cut short: its lease is released anyway, one already under way included, and
its key waits for a caller unless the new owner had taken it on, which counts it in `handoffs`. It
unregisters no sooner than a `poll_interval` after marking itself draining, within the hand-off's part of that share,
so a peer that polls discovery sees the drain and its reason (`MemberChange::Left` carries them) rather than a node
gone as after a crash: a leave with nothing to drain takes about a `poll_interval`. A key asked of it meanwhile is
refused `SpawnRefusal::Leaving`, whose `retry()` is `Resolve`: ask again and the new owner starts it.

## Discovery

| Provider | Use |
|---|---|
| `MemoryDiscovery::new()` | several nodes in one process: tests, examples |
| `StaticDiscovery::new(records)` | a fixed seed list; use `incarnation: 0` for "learn it on connect" |
| `ConsulDiscovery::new(ConsulConfig::new(url, service))` | production; feature `consul` |
| `SeedDiscovery::start(SeedConfig::new(seeds, &transport))` | production with no outside service; see below |
| your own | implement `Discovery` (`snapshot`, `register`, `unregister`, `heartbeat`, `watch` if it can, and `describe` for the dashboard's cluster page) |

A provider that implements `watch` tells the node when membership changes, so the node polls it only every
`resync_interval` (30 s) rather than every `poll_interval` (1 s). `ConsulDiscovery` watches with a blocking query:
while nothing changes a node costs Consul one open request, and a change is seen as soon as Consul has it. A view
that went stale, discovery having failed, asks for a snapshot on each `heartbeat_interval`, beside the heartbeat and
one at a time, until one lands: a watch answers only when the revision moves, and a node hands keys back only from a
view that is not stale.

A node asks for one snapshot at a time and applies it before it asks for the next, and drops a watched answer that
a snapshot asked for later has overtaken; callers that come while one is out wait for it and share the next, so none
waits for more than two. So once `set_metadata` or a kind's `register` returns, the node's own view holds the change,
and no answer that was already on its way puts the old record back.

A provider that fails, hangs, lists a record twice or more nodes than `ClusterConfig::max_nodes`, or panics, leaves the
view as it was, marked `stale` with the reason, and calls to the members listed keep working. The node asks again on
its next poll, and the mark goes with the next good answer.

There is no client mode. A process that only calls, such as a command-line tool, joins as a member with a node id of
its own, and is listed, dialled and kept alive like any other. It runs no keyed actor unless it registers a kind. Call
`system.shutdown()` before it exits, or it stays listed until discovery drops it.

### Seed servers

`SeedDiscovery` needs nothing but axon. A few machines, three in production, each run a `SeedServer`: it keeps
the member list in memory and pushes every change to every node over an authenticated session. Every node
dials every seed. A node trusts its view only while it is attached to more than half of the seeds; otherwise it
freezes, and `snapshot` and `heartbeat` answer `Unavailable` until it is attached again.

```rust
use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::*;
use axon_net::{Config, SharedSecret};

fn transport(id: &str) -> Config {
    let mut transport = Config::new(id, SharedSecret::new(*b"at least sixteen bytes").unwrap());
    transport.allow_plaintext = true; // loopback only; in production pass TLS to both types
    transport
}

async fn node(id: &str, seeds: &[String]) -> (ActorSystem, Cluster) {
    let transport = transport(id);
    // The same `Config` value goes to both: it holds this boot's identity.
    let discovery = SeedDiscovery::start(SeedConfig::new(seeds, &transport)).await.unwrap();
    // `SeedDiscovery` is a handle: an application keeps a clone to call `set_seeds` and `stats` on.
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    cfg.heartbeat_interval = Duration::from_millis(500);
    cfg.heartbeat_grace = Duration::from_secs(1);
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    (system, cluster)
}

#[tokio::main]
async fn main() {
    // On the seed machines only. In production: a fixed port and one server on each of three machines.
    let bind = "127.0.0.1:0".parse().unwrap();
    let server = SeedServer::bind(SeedServerConfig::new(bind, &transport("seed-1"))).await.unwrap();
    let seeds = [server.local_addr().to_string()]; // in production: ["seed-1:7001", "seed-2:7001", "seed-3:7001"]

    let (_system_a, a) = node("a", &seeds).await;
    let (_system_b, b) = node("b", &seeds).await;
    // A server that has just started collects registrations for 5 s before it answers with its list.
    for cluster in [&a, &b] {
        cluster.wait_view(|view| view.members.len() == 2).await.unwrap();
    }
    assert_eq!(server.stats().registered, 2);
}
```

- **Set `heartbeat_interval = 500 ms` and `heartbeat_grace = 1 s` with this provider.** A node that loses its
  network isolates within 7 s, and without an ownership authority stops its keyed actors then; the seeds remove it
  after 8 to 10 s, at least 1 s after the stop. With the defaults, which are sized for Consul's TTL, the stop would
  come after the others have taken its keys over: without an ownership authority `Cluster::start` refuses them with
  `ClusterError::Config`. It refuses any `heartbeat_interval + 3/2 heartbeat_grace` at or past
  `discovery.drops_after()`, the seeds' `idle_hold` less 2 s (3 s by default). With an authority the lease fences
  the keys, and nothing is refused. A cut of only what reaches the node is tighter: the node's seed sessions idle
  out and close, the seeds see a clean close and remove it `closed_grace` (2 s) later, as early as the stop. Where
  that matters, use an authority.
- `SeedTimings` (`idle`, `idle_hold`, `closed_grace`, `warm_up`: 4, 5, 2 and 5 s) go in `SeedServerConfig::timings`
  and `SeedConfig::timings`, the same on every seed and node. A node does not count a seed with other timings: its
  link shows the state `timings`, and the event log `seed_timings`. Change them with every node and seed stopped.
- A node dials its seeds with `SeedConfig::transport.connect_timeout`, which `SeedConfig::new` sets to 1 s, and
  `SeedDiscovery::start` refuses one within 2 s of `warm_up`. A transport you put in its place after `new` keeps its
  own, 5 s from `Config::new`: set its `connect_timeout` again, or raise `warm_up`.
- Keep clocks within a minute of each other. A node drops a record more than 60 s ahead of its own clock, so one
  further behind sees no other member, silently. A node restarted after its clock stepped back has an older incarnation
  than the boot the seeds still list: `Cluster::start` fails with `Discovery(Stale)`, `Retry::Later`, until they drop
  that boot.
- A seed entry is `host:port` or `id@host:port`. Give every seed server its own node id: seeds are counted by id.
- TLS is the cluster's `Tls` value: `SeedServerConfig { tls: Some(tls.clone()), ..SeedServerConfig::new(addr, &transport) }` and
  `SeedConfig { tls: Some(tls.clone()), ..SeedConfig::new(seeds, &transport) }`. The certificate is checked
  against the host, or against the id when one is given and can be a server name.
- The `AcceptLimits` of a seed (`SeedServerConfig::accept`) allow 16 handshakes at a time from one address. Raise `max_handshakes_per_ip`
  when many nodes reach the seed from one address, as behind a NAT.
- A seed machine that is also a cluster node runs both types and lists itself as a seed like the others.
- How fast things are seen: a join or a clean leave at once, a crashed process in 2 s, a dead machine in 8 to 10 s.
  A node whose own link to a seed has gone silent sees what that seed reports only once the session idles, 3 to 5 s
  after the silence began, so a crashed process can stay in its view about 7 s.
- One seed of three may be down or restarting without any effect. With two down, nodes freeze: the view and
  the running actors stay, nothing joins or leaves, and nodes thaw when a second seed is back. One seed alone
  is fine for development; if it restarts, the cluster freezes for 5 to 7 s. After a cut from most of the seeds, a
  node thaws once the others in its view are back, or 5 s after it reaches the seeds again.
- `discovery.set_seeds(list)` replaces the seed list of a running node. To replace a seed machine, keep its name
  and no list changes.
- `discovery.stats()` and `server.stats()` show each link's state and last error, refusals, and the list size.
- A cluster of up to 4096 nodes. Strict one-owner-per-key still needs an `OwnershipAuthority` such as
  `PaxosLeases` on the seed machines; without one, a partition can run a key on both sides until it heals, as with
  any provider.

## Security

Every session starts with a mutual challenge-response handshake over the whole hello, so a peer without the
credential gets nothing, and the negotiated limits cannot be tampered with.

| Setting | Proves | Encrypts |
|---|---|---|
| `SharedSecret` + `allow_plaintext` | the peer holds the secret | nothing. Loopback and tests only |
| `SharedSecret` + TLS | the peer holds the secret | yes |
| `CertificateIdentity` + mutual TLS | the peer's certificate is valid for its node id | yes |

A production node:

```rust,no_run
use std::sync::Arc;

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, ClusterError, ConsulConfig, ConsulDiscovery, ConsulLeases, Tls};
use axon_net::rustls::{ClientConfig, ServerConfig};
use axon_net::{CertificateIdentity, Compression, Lane};

async fn start(system: &ActorSystem, server: Arc<ServerConfig>, client: Arc<ClientConfig>) -> Result<Cluster, ClusterError> {
    let mut transport = axon_net::Config::new("orders-1", CertificateIdentity);
    transport.namespace = "prod".into();             // nodes only talk within one namespace
    for lane in Lane::ALL {
        transport.lanes.get_mut(lane).compression = Compression::Zstd;
    }

    let consul = ConsulConfig::new("http://127.0.0.1:8500", "orders");
    let discovery = ConsulDiscovery::new(consul.clone()).expect("consul config");
    let mut cfg = ClusterConfig::new("0.0.0.0:7000".parse().unwrap(), transport, discovery);
    cfg.advertise = Some("10.0.0.5:0".parse().unwrap()); // required with a wildcard bind; port 0 is the bound one
    cfg.tls = Some(Tls { server, client });
    cfg.labels.insert("region".into(), "eu".into());
    cfg.ownership = Some(Arc::new(ConsulLeases::new(consul, "orders-1").expect("consul leases")));
    Cluster::start(system, cfg).await
}
```

- With `SharedSecret`, any holder of the secret can claim any node id. Use `CertificateIdentity` when node identity
  matters. Certificates must be valid for the node id.
- To limit what an authenticated peer may do, set `ClusterConfig::authorizer`: it sees every `Action::Call { name }`,
  `Action::Spawn { kind }` and `Action::Watch { kind }`. Calls include the cluster's own endpoints, named `axon.*`:
  allow them to members, or spawn, watches and keys moving to their owner stop. Without one, every authenticated peer
  may do anything.
  `cluster.set_authorizer(authorizer)` swaps it on a running node, to revoke or grant access; `None` allows everything.
  Nothing is checked once and remembered: each call, spawn and watch question is checked as it arrives, so from the
  moment `set_authorizer` returns the new one decides, whatever sessions, resolved references or published names came
  before. What was admitted runs to its end: a request answers, and a watch's held question answers when the key
  changes or its hold passes (half the watcher's `request_timeout`, at most the owner's). The watch then asks again
  and, refused, ends, so a revoked peer's `KeyWatch` returns `None` within a hold, or its `request_timeout` when the
  answer is lost. Access granted again does not bring an ended watch back: the peer calls `watch_key` again. A stream
  a revoked peer follows stops too: its interest refreshes to `axon.stream` are refused (an authorizer that names
  the stream endpoints matches `axon.stream` and every name under `axon.stream.`: the pushes go to
  `axon.stream.live` and `axon.stream.bulk`), and a stream node drops a child it has not heard from for `StreamNodeConfig::downstream_expiry` (10 s), so nothing published after that
  reaches it; granted access again, it reads on from its cursor while its parent still holds what it missed, and its
  followers are reset otherwise ([streams § Tree](streams.md#tree)).
- Compression is per frame and only kept when it saves bytes (`compression_threshold` 512, `compression_min_savings`
  32).
- Inbound connections are capped: 3072 connections, 64 handshakes in flight, 16 per IP (`ClusterConfig::accept`), as a
  peer may open a session on each of the three lanes. Handshakes over 64 wait their turn within `handshake_timeout`;
  over 16 from one address they are turned away. A dial's TCP connect (within `connect_timeout`), TLS and
  authentication share one `handshake_timeout` (5 s) too.
- A session closes when a write makes no progress for `write_timeout` (5 s), not when a batch takes longer: a slow
  link that keeps moving stays up. Over TLS the flush that ends a batch must also drain rustls's 64 KiB buffer in that
  time, about 13 KB/s.
- A handshake that fails is one the transport counts and forgets: a certificate outside its validity dates, an
  `Authenticator` whose `secret` or `authorize` panics (counted as an authentication failure, the peer refused), a
  stream that stalls or goes one way (the handshake timeout). The listener serves the next, and a dialler backs off and
  dials again.

### Rotating certificates

`cluster.set_tls(tls)` swaps the certificates a running node serves and dials with. Sessions that start after it use
the new ones, redials included; sessions already up keep theirs until they end, so no call fails for it. It is refused,
and changes nothing, on a node started without TLS, and when the certificates would make it another node: they must
pass a TLS handshake with themselves under the name peers dial the node by (its id when that is a server name, else its
address), and the node's authenticator, asked with the node's own identity and no remote address, must take them as
that id (one that panics does not). `SeedServer::set_tls` swaps what a seed serves, and `SeedDiscovery::set_tls` what a node dials its seeds with;
those two refuse only a seed or node started without TLS, since a node's certificates for its seeds need not name it.

```rust,no_run
use axon_cluster::{Cluster, ClusterError, SeedDiscovery, Tls};

/// One node's part of a pass.
fn rotate(cluster: &Cluster, seeds: &SeedDiscovery, tls: Tls) -> Result<(), ClusterError> {
    cluster.set_tls(tls.clone())?;
    seeds.set_tls(tls).map_err(ClusterError::Discovery)
}
```

Moving a cluster to a new CA takes three passes. Each pass reaches every node, seeds included, before the next starts:

1. Trust both CAs: every node keeps its leaf and adds the new CA to its roots and to its client verifier.
2. Switch every leaf to one the new CA signed, still trusting both.
3. Drop the old CA.

A node that switches its leaf before the others trust the new CA keeps the sessions it has and gets no new ones, in
either direction, until they do. Dropping the old CA before every leaf has switched does the same to the nodes still on
it.

## Rolling upgrades

A cluster can run two builds side by side while you roll it one node at a time: drain a node with
`system.shutdown_with_reason(LeaveReason::Maintenance)`, start it again on the next build, wait for
`cluster.warming()` to be false, and go on to the next. Two things keep old and new nodes talking.

- **The wire.** Each build speaks a range of axon wire versions, `axon_net::VERSIONS`, and each session runs the
  highest version both ends speak, `Peer::version` (`PeerStats::version` in `cluster.stats()`). Only ranges that do
  not meet are refused: the error, `Negotiation::Version { ours, theirs }`, names both, and a listener counts it under
  the handshake failure reason `version`. From 1.0, a release speaks its own version and the one before it, so a
  cluster rolls from one release to the next; skipping a release means a stop, not a roll. Before 1.0 there is no
  such promise: this release speaks only version 7, and a node of an earlier build is refused.
- **Your protocols.** A node serves several `SCHEMA`s of one protocol `ID` side by side. `publish` the same name once
  per schema, the new build publishing both; a kind takes the old schema with `.also::<Old>()`, and each key's one
  actor answers either. A call goes to a node that serves its caller's schema: `resolve_all` returns those, and
  lists the others among its failures as `ClusterError::SchemaMismatch { wanted, served }`, whose `retry()` is
  `Resolve`, since the node may serve it after its upgrade. A node advertises a kind at its own schema, then the
  ones it serves `also` (`NodeRecord::kind_schemas`), and a key is placed only on the nodes that serve the most of
  the schemas the kind's nodes call with, so that every caller, old or new, reaches it. Rolled in one step, as
  below, the keys gather on the upgraded nodes. To keep them spread, roll twice: first a build that serves the new
  schema `also` but still calls the old, then the one that calls the new. A `get_or_spawn` of a schema its owner
  does not serve places nothing and returns `SchemaMismatch`. A new schema is a larger number, and serves the old one
  until no caller of it is left.

```rust
use axon::{Actor, ActorSystem, Context};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, ClusterError, MemoryDiscovery, Placement, Rejection, Reply,
    Retry, SpawnContext, WireProtocol,
};
use axon_net::SharedSecret;
use bytes::Bytes;

struct Greeter;

impl Actor for Greeter {
    type Message = (String, Option<Reply<String>>);
    type Error = ();
    fn on_message(&mut self, (name, reply): Self::Message, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
        drop(reply.map(|r| r.send(format!("hello {name}"))));
        Ok(())
    }
}

/// The old build's contract: the name as UTF-8.
struct GreetV1;
/// The new build's: the same `ID`, a length in front.
struct GreetV2;

impl WireProtocol for GreetV1 {
    type Message = String;
    type Reply = String;
    const ID: u64 = 0x6E;
    const SCHEMA: u16 = 1;
    fn encode(name: String) -> Result<Bytes, CallError> {
        Ok(Bytes::from(name))
    }
    fn decode_reply(body: Bytes) -> Result<String, CallError> {
        String::from_utf8(body.to_vec()).map_err(|e| CallError::Codec(e.to_string()))
    }
}

impl ActorProtocol for GreetV1 {
    type Mailbox = (String, Option<Reply<String>>);
    fn encode_reply(greeting: String) -> Result<Bytes, CallError> {
        Self::encode(greeting)
    }
    fn decode(body: Bytes, reply: Option<Reply<String>>) -> Result<Self::Mailbox, Rejection> {
        let name = String::from_utf8(body.to_vec()).map_err(|_| Rejection::InvalidMessage)?;
        Ok((name, reply))
    }
}

impl WireProtocol for GreetV2 {
    type Message = String;
    type Reply = String;
    const ID: u64 = 0x6E;
    const SCHEMA: u16 = 2;
    fn encode(name: String) -> Result<Bytes, CallError> {
        Ok(Bytes::from([&[name.len() as u8][..], name.as_bytes()].concat()))
    }
    fn decode_reply(body: Bytes) -> Result<String, CallError> {
        GreetV1::decode_reply(body)
    }
}

impl ActorProtocol for GreetV2 {
    type Mailbox = (String, Option<Reply<String>>);
    fn encode_reply(greeting: String) -> Result<Bytes, CallError> {
        GreetV1::encode_reply(greeting)
    }
    fn decode(body: Bytes, reply: Option<Reply<String>>) -> Result<Self::Mailbox, Rejection> {
        match body.split_first() {
            Some((&len, name)) if name.len() == len as usize => GreetV1::decode(body.slice(1..), reply),
            _ => Err(Rejection::InvalidMessage),
        }
    }
}

/// `upgraded` is the new build: it serves schema 2, and schema 1 for the callers still on the old one.
async fn node(id: &str, discovery: &MemoryDiscovery, upgraded: bool) -> (ActorSystem, Cluster) {
    let mut transport = axon_net::Config::new(id, SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true;
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let greeter = system.spawn(Greeter).start().unwrap();
    cluster.publish::<GreetV1>(&greeter, "greeter").unwrap();
    let build = |_: &SpawnContext, _: Bytes| Ok(Greeter);
    if upgraded {
        cluster.publish::<GreetV2>(&greeter, "greeter").unwrap();
        cluster.kind::<GreetV2>("greeter").also::<GreetV1>().register(build).await.unwrap();
    } else {
        cluster.kind::<GreetV1>("greeter").register(build).await.unwrap();
    }
    (system, cluster)
}

#[tokio::main]
async fn main() {
    let discovery = MemoryDiscovery::new();
    let (_old_system, old) = node("old", &discovery, false).await;
    let (_new_system, new) = node("new", &discovery, true).await;
    for cluster in [&old, &new] {
        cluster.wait_view(|view| view.members.iter().filter(|m| m.record.has_kind("greeter")).count() == 2).await.unwrap();
    }
    let view = old.view();
    assert_eq!(view.get("new").and_then(|m| m.record.kind_schemas("greeter")), Some(vec![2, 1]));

    // The old build reaches both nodes; the new build reaches the node that speaks its schema.
    assert_eq!(old.resolve_all::<GreetV1>("greeter").await.0.len(), 2);
    let (found, failed) = new.resolve_all::<GreetV2>("greeter").await;
    assert_eq!((found.len(), found[0].node()), (1, "new"));
    assert!(matches!(&failed[0].1, ClusterError::SchemaMismatch { wanted: 2, served } if *served == [1]));
    assert_eq!(failed[0].1.retry(), Retry::Resolve);
    assert_eq!(found[0].request("ada".into()).await.unwrap(), "hello ada");

    // Keys go to the node that speaks both builds' schemas, and callers of either build share each key's actor.
    let any = Placement::default();
    let from_old = old.get_or_spawn::<GreetV1>("greeter", "k", Bytes::new(), &any).await.unwrap();
    let from_new = new.get_or_spawn::<GreetV2>("greeter", "k", Bytes::new(), &any).await.unwrap();
    assert_eq!((from_old.node(), from_new.node()), ("new", "new"));
    assert_eq!(from_old.request("bob".into()).await.unwrap(), "hello bob");
    assert_eq!(new.stats().spawned, 1);
    for peer in new.stats().engine.peers {
        assert_eq!(peer.version, *axon_net::VERSIONS.end()); // the highest both speak
    }
}
```

## Quotas

`Quotas` limits a key (an API key, an account, `b""` for the whole cluster) across the cluster: a `Rate` of `n` units
per window of 1 s to 1 h, or `Permits`, `n` held at once. Every check is local, with no network round trip, and
nothing waits: a node admits against a grant its key's replicas gave it, and reports what it used every 100 ms.

```rust
use std::time::Duration;

use axon::{Actor, ActorSystem, Context};
use axon_cluster::{Cluster, ClusterConfig, Denied, MemoryDiscovery, Permit, Permits, QuotaConfig, Quotas, Rate};
use axon_net::SharedSecret;

const REQ_SEC: Rate = Rate::new("req/s", 10, Duration::from_secs(1));
const WEIGHT_MIN: Rate = Rate::new("weight/min", 1200, Duration::from_secs(60));
const WS: Permits = Permits::new("ws", 5);

/// An HTTP-like handler: a request costs one of `req/s` and its weight of `weight/min`, both or neither. What it
/// streams back is known only afterwards, and charged then.
fn handle(quotas: &Quotas, api_key: &str, weight: u64) -> Result<u64, (u16, Duration)> {
    match quotas.take(api_key, &[(REQ_SEC, 1), (WEIGHT_MIN, weight)]) {
        Ok(()) => {
            let rows = weight * 3;
            quotas.charge(api_key, &[(WEIGHT_MIN, rows / 10)]);
            Ok(rows)
        }
        Err(denied) => Err((429, denied.retry_after())),
    }
}

/// A websocket session. It holds one of its account's permits for as long as it runs.
struct Session {
    _permit: Permit,
}

impl Actor for Session {
    type Message = String;
    type Error = ();
    fn on_message(&mut self, _frame: String, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let mut transport = axon_net::Config::new("a", SharedSecret::new("at-least-16-bytes-long").unwrap());
    transport.allow_plaintext = true;
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, MemoryDiscovery::new());
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let quotas = Quotas::start(&cluster, QuotaConfig { region: "eu".into(), ..QuotaConfig::default() }).await.unwrap();

    for _ in 0..10 {
        assert!(handle(&quotas, "key-1", 20).is_ok());
    }
    assert_eq!(handle(&quotas, "key-1", 20).map_err(|(status, _)| status), Err(429)); // the 11th in a second

    let mut sessions = Vec::new();
    for _ in 0..5 {
        let permit = quotas.acquire("acct-7", &WS).unwrap();
        sessions.push(system.spawn(Session { _permit: permit }).start().unwrap());
    }
    assert!(quotas.acquire("acct-7", &WS).is_err()); // a sixth session of acct-7
    let first = sessions.remove(0);
    first.lifecycle.stop();
    first.join().await; // its permit goes with it
    sessions.push(system.spawn(Session { _permit: quotas.acquire("acct-7", &WS).unwrap() }).start().unwrap());
    system.shutdown().await.unwrap();
}
```

- Start `Quotas` on every node that serves requests, each with its `region`. Nodes advertise it under the metadata key
  `axon.quota`. A key has one replica per region, among each region's `replicas_per_region` (8) best-scored quota
  nodes, and at least three in all; a node admits against the smallest grant of a majority of them. Nodes with another
  `replicas_per_region` refuse each other, so change it only with every quota node stopped.
- Limits are approximate: [spec/quotas.md §5](../../../spec/quotas.md) says by how much, per fault. A key admits no
  more than its limit while nothing fails, the fallback `ceil(limit / quota nodes)` per node where a key is new here or
  too few of its replicas answer, and up to the limit on each side of a partition. Nothing admitted is taken back.
  A key new to a node is admitted up to the fallback for its first second where its grants are smaller, while a node
  that used it last hands back the room it holds.
- `Denied::Limit`: the key is at its limit; answer 429 with `retry_after`. `Denied::Share`: the limit has room but this
  node's grant is too small for the cost. The refusal is reported as demand, and the replicas ask the nodes that hold
  the room to hand it back at once, not at their next heartbeat. `retry_after` is when this node expects the room, about
  a report cycle and a few round trips, or sooner where its own usage leaves the window first; a retry then is admitted
  once the grant grows, or refused with `Denied::Limit` where the others use the room. From then on the most the node
  uses at once counts as demand too, so it keeps its share for as long as it keeps using the key; room no node asks for
  is spread over every node, so one that uses a key only now and then is admitted while that spread covers its cost.
- `charge` never refuses: a request that turns out dearer takes the difference afterwards, and later `take`s are
  refused until the window slides past it. There are no refunds.
- A `Permit` releases on drop. Move it into the actor or task it admits, never `let _ =`, which drops it at once.
- Call `take` after authentication: a key a node has not seen gets up to the fallback in its first second, so
  unauthenticated keys would each be admitted. Use the same limit names and maxima everywhere; where they differ, the maximum in a node's newest
  report wins and `stats().mismatched` counts it.
- A new or restarted node pulls its keys' rows from the other replicas before its grants count, and `/readyz` answers
  503 until then ([health probes](observability.md#health-probes), `cluster.quotas_starting()`): in a rolling restart,
  wait for it before the next. A node that gains keys when another leaves pulls them too, but stays ready.
  `stats()` and `describe()` feed telemetry (`quota.*` in [telemetry](telemetry.md)).
- A check costs about 100 ns under one shard's lock, and one hot key serialises there: 11 M checks a second from one
  thread, about 1 M from 24. The sweep that drops entries unused for a minute takes that lock for 32 entries at a time,
  so a check waits a few µs for it at most. An entry a node keeps costs about 470 B, so `QuotaConfig::capacity`'s
  default of 2²⁰ entries holds up to about 500 MB, and a key a replica holds 320 B. A node reports 41 k entries a second
  at most, so it keeps grants for about 4.9 M rate entries in use; past that, the rest admit on the fallback
  ([benchmarks](benchmarks.md#quotas-axon-cluster)).

## Backpressure and limits

- Each session has a bounded outbox (256 frames, 8 MiB). A full outbox makes callers wait, then fail with
  `CallError::NotSent(..)`; the message is not sent.
- A frame of up to 4 KiB to an idle session (nothing queued, nothing written for 5 µs) is written to the socket by
  the thread that sends it, an actor's worker for a reply; larger or more frequent frames go through the session's
  writer task in batches.
- A node has at most `max_pending` (4096) calls in flight; beyond that `CallError::Busy`.
- A node dials another one on the first call to it and keeps at most `max_peers` (4096) such links. A link that has
  carried no call for `idle_link_timeout` (5 min) is closed, and the next call dials again. A peer that cannot be
  reached is redialled with backoff capped at 2 s (`ClusterConfig::backoff`), so calls get through within about that
  once a cut heals, however long it lasted. A peer that discovery shows back as a new boot is dialled at once, whatever
  backoff the old boot earned, and a call made meanwhile waits for that dial rather than failing with the old boot's
  error. Each open link costs a keepalive a second on both ends. The keepalive is also what notices a crashed peer
  within seconds: a node with no link to it learns of the crash from discovery, or from its next call failing.
- A slow receiver holds inbound credit, which slows the sender's socket rather than growing memory.
- A received body handed to `ActorProtocol::decode` or `WireProtocol::decode_reply` can be a slice of the session's
  64 KiB read buffer, which it keeps alive. Keep a small part of it long (in actor state, say) with
  `Bytes::copy_from_slice`, or decode it into your own types.
- A Consul client keeps at most `ConsulConfig::max_in_flight` (64) requests in flight; the rest wait. An agent refuses
  a client that holds more than its `http_max_conns_per_client` (200) connections, and a burst of keyed spawns would
  otherwise open one connection per lease.

## Across regions

The shipped timings are for round trips under about 50 ms that seldom lose a packet. Across regions, say 92 ms
between Europe and the US, 190 ms to Singapore and 219 ms from there to the US, with one packet in 200 lost, a
session idles out after a few losses in a row, a dial whose SYN and answer are each lost once counts the peer
unreachable, and a lease round gives up before its retransmits land. Set these values on every node instead: one
const and one function of the application's, and the relay timings of each namespace whose origins are far away.

```rust,no_run
use std::sync::Arc;
use std::time::Duration;

use axon::files::OsFiles;
use axon::{ActorSystem, Lane};
use axon_cluster::{
    Cluster, ClusterConfig, PaxosConfig, PaxosLeases, SeedConfig, SeedDiscovery, SeedServer, SeedServerConfig,
    SeedTimings,
};
use axon_net::{Config, SharedSecret};
use axon_stream::{RelayTimings, StreamNode, StreamNodeConfig};

// crates/axon-stream/tests/world_wan.rs runs five nodes in three regions on SEED_TIMINGS and across_regions, and
// fails where its copy and this one differ.
const SEED_TIMINGS: SeedTimings = SeedTimings {
    // For a slowest round trip of about 220 ms, with some loss.
    idle: Duration::from_secs(8),
    idle_hold: Duration::from_secs(7),
    closed_grace: Duration::from_secs(4),
    warm_up: Duration::from_secs(8),
};

/// Every node: the cluster, its leases and its streams.
fn across_regions(cluster: &mut ClusterConfig, paxos: &mut PaxosConfig, streams: &mut StreamNodeConfig) {
    // For a slowest round trip of about 220 ms, with some loss.
    cluster.transport.keepalive_interval = Duration::from_secs(1);
    cluster.transport.idle_timeout = Duration::from_secs(8);
    cluster.transport.connect_timeout = Duration::from_millis(3500);
    cluster.transport.handshake_timeout = Duration::from_secs(5);
    cluster.heartbeat_interval = Duration::from_millis(500);
    cluster.heartbeat_grace = Duration::from_secs(2);
    cluster.request_timeout = Duration::from_secs(5);
    cluster.operation_timeout = Duration::from_secs(7);
    paxos.lease = Duration::from_secs(10);
    streams.downstream_expiry = Duration::from_secs(20);
    paxos.drift = 0.01;
}

#[tokio::main]
async fn main() {
    let mut transport = Config::new("eu-a", SharedSecret::new(*b"at least sixteen bytes").unwrap());
    // Here for brevity only: between regions pass TLS to the cluster and both seed types ([Security](#security)).
    transport.allow_plaintext = true;
    let seeds = ["10.0.1.1:7001", "10.0.2.1:7001", "10.0.3.1:7001"];
    // On the three seed machines, one in each region.
    let server = SeedServerConfig::new(seeds[0].parse().unwrap(), &transport);
    let _seed = SeedServer::bind(SeedServerConfig { timings: SEED_TIMINGS, ..server }).await.unwrap();
    let mut discovery = SeedConfig { timings: SEED_TIMINGS, ..SeedConfig::new(seeds, &transport) };
    // A seed dial's connect, as the cluster's in across_regions.
    discovery.transport.connect_timeout = Duration::from_millis(3500);
    let discovery = SeedDiscovery::start(discovery).await.unwrap();

    let mut cfg = ClusterConfig::new("10.0.1.1:7000".parse().unwrap(), transport, discovery);
    // The acceptors, one in each region.
    let mut paxos = PaxosConfig::new(["eu-a", "sin", "us"]);
    let mut streams = StreamNodeConfig::new(vec![]);
    across_regions(&mut cfg, &mut paxos, &mut streams);
    let relay = RelayTimings { upstream_timeout: Duration::from_secs(7), hold_down: Duration::from_secs(30) };
    // A namespace that moves much more than the calls goes on the Bulk lane.
    let streams = streams.relay_timings("md", relay).relay_timings("candles", relay).lane("candles", Lane::Bulk);
    paxos.store = Some(Arc::new(OsFiles::new("/var/lib/app/leases")));
    cfg.ownership = Some(Arc::new(PaxosLeases::new(paxos).unwrap()));

    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let _streams = StreamNode::start(&cluster, streams).await.unwrap();
    system.shutdown().await.unwrap();
}
```

Each value is a sum of round trips, retransmits and SYN resends. With other links, do it again with your slowest
round trip (RTT) and its retransmit timeout (RTO, about RTT + 200 ms on Linux); a lost SYN is sent again after 1 s,
then 3 s.

| Setting | Shipped | Here | Why, at RTT 219 ms and RTO 0.42 s |
|---|---|---|---|
| `transport.keepalive_interval` | 1 s | 1 s | a ping a second costs nothing |
| `transport.idle_timeout` | 4 s | 8 s | four losses in a row of one segment, 0.42 + 0.84 + 1.68 + 3.36 = 6.3 s, then a ping |
| `transport.connect_timeout` | 1.5 s | 3.5 s | two lost SYNs: the third leaves at 3 s, its answer is back at 3.22 s |
| `SeedConfig::transport.connect_timeout` | 1 s | 3.5 s | as the cluster's: a seed drops a node whose redial has not connected within `closed_grace` |
| `transport.handshake_timeout` | 5 s | 5 s | the connect, then about three round trips |
| `PaxosConfig::lease` | 6 s | 10 s | a round waits lease / 6 = 1.67 s, an ask and two retransmits take 0.22 + 0.42 + 0.84 = 1.48 s |
| `heartbeat_interval`, `heartbeat_grace` | 1 s, 5 s (500 ms, 1 s with seeds) | 500 ms, 2 s | the peer check waits half the grace: an ask and one retransmit, 0.64 s |
| `request_timeout` | 5 s | 5 s | a call on a live session is a round trip and its retransmits |
| `operation_timeout` | 5 s | 7 s | a whole lease round, 4 × 1.67 s, and a round trip |
| `SeedTimings::idle` | 4 s | 8 s | as the transport's idle timeout |
| `SeedTimings::idle_hold` | 5 s | 7 s | a seed drops a quiet member 2 s before it, at 5 s, which must be past `heartbeat_interval` + 3/2 `heartbeat_grace`, 3.5 s ([Seed servers](#seed-servers)) |
| `SeedTimings::closed_grace`, `warm_up` | 2 s, 5 s | 4 s, 8 s | a LEAVE may need retransmits; a redial is a 2 s backoff, the seed connect, a handshake and a register: about 4 s shipped, 6.4 s across regions |
| `RelayTimings::upstream_timeout` | 3 s | 7 s | one redial: backoff 2 s, connect 3.5 s, handshake 0.9 s, about 6.4 s |
| `RelayTimings::hold_down` | 10 s | 30 s | a choice: a parent back for less than twice a dead machine's removal is not worth a switch |
| `StreamNodeConfig::downstream_expiry` | 10 s | 20 s | an idle timeout and a redial, 8 + 6.4 s, and a refresh, 2 s |

What changes for operators:

- A dead machine is unreachable to its peers 10.5 to 11.5 s after it died (4.5 to 5.5 s shipped), and its keys run
  elsewhere about 14 s after. A node cut off from all others stops its leased keys at 8.5 s, by their fence, isolates
  at about 11.5 s, and the seeds remove it at 14 to 15 s. An acceptor grants nothing for 10.3 s after it starts.
- To change the lease, the drift or the seed timings, stop every node, then start them all on the new values: the
  acceptors refuse a node with another lease or drift ([One owner per key](#one-owner-per-key)), and a node does not
  count a seed with other timings ([Seed servers](#seed-servers)) and records the event `seed_timings`
  ([telemetry](telemetry.md)).

Host settings, on every node with a link across regions:

```text
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr
net.ipv4.tcp_slow_start_after_idle = 0
net.ipv4.tcp_wmem = 4096 16384 67108864      # max at least the push window's ceiling (64 MiB by default)
net.ipv4.tcp_rmem = 4096 131072 134217728    # max at least twice it
```

At 219 ms, 13 MB/s needs about 2.8 MB in flight on one connection. Linux's defaults, a `tcp_wmem` maximum of 4 MiB
and a `tcp_rmem` maximum of 6 MiB of which a receiver advertises about half, give 14 to 19 MB/s, with little room to
catch up after a cut. The maxima hold for every socket on the host, so a node that also serves the public may let one
connection hold that much kernel memory; `net.ipv4.tcp_mem` bounds the total. A stream node records the event `host_tcp_limits` at start
when the maxima are smaller than its push window needs ([telemetry](telemetry.md)). axon sets no `SO_SNDBUF` or
`SO_RCVBUF`, as on Linux either one turns off the kernel's autotuning for that socket and caps it at
`net.core.wmem_max` or `rmem_max`, and no `TCP_CONGESTION`: that is the host's choice, with the `fq` qdisc BBR needs.

Put the acceptors in three regions, one in each. Every renew then crosses at least the shortest link between two
acceptor regions: with eu-a, sin and us, every majority holds one outside Europe, so the Europe nodes renew through us
at 92 ms. Only a majority of acceptors in one region makes renewals local, and the loss of that region then stops
every key.

Calls and stream pushes share the `Live` lane unless a namespace goes on `Bulk` ([Lanes](#lanes)); cap
`transport.lanes.bulk.max_bytes_per_sec` below the link's rate, or a busy `Bulk` keeps a round trip of bytes queued in
the network ahead of `Live`. A parent keeps up to `core.max_push_window` of pushes unanswered per child
([streams § Tree](streams.md#tree)), which the host settings above let one connection carry.

## Simulating a cluster

`axon-testkit`'s `World` runs real nodes (cluster, keyed actors, leases, seed servers, streams, your actors) in one
process on one thread, on virtual time, over byte pipes it owns. A seed picks the order the nodes step in, the
delivery order, jitter and fault timing, so a test that crashes, pauses, partitions or skews nodes finds a race by
seed and replays it. Add it as a dev-dependency: `axon-testkit = "0.1"`.

```rust,standalone_crate
use std::time::Duration;

use axon::{Actor, ActorSystem, Context};
use axon_cluster::{
    ActorProtocol, CallError, Cluster, ClusterConfig, MemoryDiscovery, Placement, Rejection, Reply, Retry,
    SpawnContext, WireProtocol,
};
use axon_net::SharedSecret;
use axon_testkit::world::{Crash, Host, World, WorldConfig};
use axon_testkit::{check_seeds, seeds};
use bytes::Bytes;
#
# struct Session(String); // answers every request with its key
#
# impl Actor for Session {
#     type Message = Option<Reply<String>>;
#     type Error = ();
#     fn on_message(&mut self, reply: Option<Reply<String>>, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
#         drop(reply.map(|r| r.send(self.0.clone())));
#         Ok(())
#     }
# }
#
# struct SessionApi;
#
# impl WireProtocol for SessionApi {
#     type Message = ();
#     type Reply = String;
#     const ID: u64 = 0x5E;
#     const SCHEMA: u16 = 1;
#     fn encode(_: ()) -> Result<Bytes, CallError> {
#         Ok(Bytes::new())
#     }
#     fn decode_reply(body: Bytes) -> Result<String, CallError> {
#         String::from_utf8(body.to_vec()).map_err(|e| CallError::Codec(e.to_string()))
#     }
# }
#
# impl ActorProtocol for SessionApi {
#     type Mailbox = Option<Reply<String>>;
#     fn encode_reply(key: String) -> Result<Bytes, CallError> {
#         Ok(Bytes::from(key))
#     }
#     fn decode(_body: Bytes, reply: Option<Reply<String>>) -> Result<Self::Mailbox, Rejection> {
#         Ok(reply)
#     }
# }

#[derive(Clone)]
struct Node {
    cluster: Cluster,
    _system: ActorSystem,
}

async fn boot(host: Host, discovery: MemoryDiscovery) -> Node {
    let transport = host.transport(SharedSecret::new("at-least-16-bytes-long").unwrap());
    let system = ActorSystem::builder().runtime(host.runtime()).build().unwrap();
    let cluster = Cluster::start(&system, ClusterConfig::new(host.addr(), transport, discovery)).await.unwrap();
    let build = |cx: &SpawnContext, _: Bytes| Ok(Session(cx.key.clone().unwrap_or_default()));
    cluster.kind::<SessionApi>("session").register(build).await.unwrap();
    Node { cluster, _system: system }
}

fn a_crashed_owner_key_answers_from_another_node(seed: u64) {
    let discovery = MemoryDiscovery::new();
    let mut w = World::new(WorldConfig { seed, ..WorldConfig::default() });
    for name in ["a", "b", "c"] {
        let discovery = discovery.clone();
        w.add(name, move |host| boot(host, discovery.clone()));
    }
    w.run_until("every node lists three with the kind", Duration::from_secs(30), |w| {
        let with_kind = |n: &Node| n.cluster.view().members.iter().filter(|m| m.record.has_kind("session")).count();
        w.nodes().count() == 3 && w.nodes().all(|(_, n)| with_kind(n) == 3)
    });
    let ask_alice = |n: Node| async move {
        loop {
            match n.cluster.get_or_spawn::<SessionApi>("session", "alice", Bytes::new(), &Placement::default()).await {
                Ok(found) => return (found.node().to_owned(), found.request(()).await.unwrap()),
                Err(e) => assert_ne!(e.retry(), Retry::Never, "{e:?}"),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    let (owner, _) = w.block_on("a", ask_alice).unwrap();

    // The seed picks when and how the owner dies.
    let after = Duration::from_millis(w.pick(0..500));
    w.run_for(after);
    let how = if w.pick(0..2) == 0 { Crash::Process } else { Crash::Machine };
    // The moment of the crash on the asker's own clock: a read takes no time.
    let asker = if owner == "a" { "b" } else { "a" };
    let crashed = w.read(asker, |_| tokio::time::Instant::now()).unwrap();
    w.crash(&owner, how);

    let (on, answer) = w.block_on(asker, ask_alice).unwrap();
    assert!(on != owner && answer == "alice", "seed {seed}, {how:?}: {on} said {answer}\n{}", w.trace_tail(40));
    let took = w.read(asker, |_| crashed.elapsed()).unwrap();
    println!("seed {seed}, {how:?}: alice answered from {on} {took:?} after the crash");
}

fn main() {
    check_seeds(seeds(0..4), a_crashed_owner_key_answers_from_another_node).unwrap();
}
```

- Boot from the `Host`: `host.runtime()` runs the node's actors on its virtual clock, `host.transport(auth)` puts its
  sessions on the World's wires, and `host.addr()` is where it listens. Build anything that holds a clock (a lease
  authority, a deadline) in `boot` or on the node, never in the test body.
- Give each store a directory of the machine's disk, as in production: `host.disk().dir("leases")` as an acceptor's
  `PaxosConfig::store`, `host.disk().dir("streams")` in a `DiskConfig`. A directory lists only its own files, and
  shares the machine's capacity, crashes and panicking writes. `Crash::Machine` keeps a seeded first part of what was
  not synced, so a file may end in a torn record, and keeps what was removed or replaced; the `Disk` syncs a write's
  appends before a removal after them, one file at a time, as `OsFiles` does. In a store's unit tests,
  `axon_testkit::crashes(&files, &ops)` gives every end of a crash inside one write, each a `MemoryFiles` to open the
  store on.
- `tokio::time::sleep` and `tokio::time::Instant` in your code follow the node's virtual clock; `std::time::Instant`
  does not. A node has three clocks. Its system's clock (`cluster.clock()`, and through it `cluster.stats()` and
  `warming()`) is the node's own on any thread, so a `run_until` predicate can call it. Its tokio clock and its wall
  clock (`axon::trace::unix_micros()`) are the node's only inside the node: in the test body they are the machine's
  real clocks, and nothing fails.
- Do node work through `w.spawn_on`, `w.block_on` and `w.join`. Read a node with `w.read(name, |node| ..)`: it runs
  the closure at once, inside the node, and no time passes. Use it to compare a moment of the test with the node's own
  stamps: `let crashed = w.read("gateway", |_| Instant::now()).unwrap()` on the line before `w.crash`, then compare the
  gateway's request stamps with `crashed`. A read sees the node's clocks where its last step left them, which for a
  paused or starved node is behind the World until its next step. An `Instant` is one boot's of one node, and
  `w.now()` is the World's time, which a node's clock follows only without drift or suspension. Nothing may block or
  start a thread: on Linux the World fails a node that does, so a World test lives in a test file of its own, named
  `world*.rs`.
- Faults: `w.crash(name, Crash::Process | Crash::Machine)` then `w.restart(name)`, `pause`, `suspend`, `hold_actors`,
  `cost`, `starve`, `drift`, `step_wall`, `cut` (one way), `partition`, `heal`, `reset`, `cut_lane` and `reset_lane`
  (one lane), `refuse_accepts`. Draw their timing from `w.pick`, so each seed tries another moment. `pause(name, d)` is
  SIGSTOP: the node's clock runs on, so at its wake its leases and timers have run out. `suspend(name, d)` is a
  suspended VM: its clocks stand still too, so it wakes as if no time had passed, still sure of its leases.
- Shorten a timing only to make a test quick, with a `//` reason on that line or right above it (`scripts/check.sh`
  checks it); [a World test](../../axon-stream/tests/world.rs) runs the shipped timings.
- Keep what the test checks outside the nodes (an `Arc<Mutex<..>>`), recorded when a write is acknowledged, never in
  `on_stop`: a crash runs no `on_stop`.
- Assert properties (every key answers, no write from an older epoch lands), never a digest. `check_seeds` names the
  lowest seed that fails.
- `cargo test` runs it and checks the properties, but tokio's own draws are not seeded there, so a seed does not
  replay, and a failed seed's report says so. `RUSTFLAGS="--cfg tokio_unstable"` seeds them. `scripts/check.sh sim`
  builds so and runs every `world*.rs` in two processes whose digests must match; a test prints
  `world digest <name> seed <n>: {w.digest()}` to join that check. `SIM_SEED=n scripts/check.sh sim <test>` replays a
  seed, `SEEDS=a..b` runs a range, and `scripts/seeds.sh a..b <box>..` splits a range across machines.

The rules behind it are in [spec/simulation.md](../../../spec/simulation.md), and
[the World tests](../../axon-cluster/tests/world_faults.rs) hold longer scenarios.

## Lower level

- `cluster.serve(name, protocol, schema, handler)` and `cluster.request(node, name, ..)`: a raw, ordered bytes endpoint
  without an actor, on `Live` (`cluster.endpoint(..).lane(lane).serve(handler)` for another [lane](#lanes)). The handler runs on the session's read path and must not block. `cluster.request_to(node,
  incarnation, name, ..)` reaches only that boot of the node, and is refused with `CallError::StaleNode` once another
  boot replaced it: for a request sent again that was built for the boot that asked.
  `cluster.start_request_to(..)` returns a `PendingReply` once the session takes the request, so several can be in
  flight on one session, sent in the order they started; each answer is due `request_timeout` after its admission,
  not after its start, so one that waited behind others for room is not late. It holds a `max_pending` slot until
  then, up to twice `request_timeout`: a peer that reads slowly and answers late holds slots that the node's other
  calls share. `request_to` is
  `start_request_to(..).await?.await`. `cluster.round_trip(node)` is the shortest of the last four keepalive round
  trips to the node.
- `cluster.serve_control(service, AccessPolicy, handler)` and `cluster.control(node, service, body)`: async
  request/response for tooling. The observability crates are built on it. `AccessPolicy` defaults to `Deny`.
- `cluster.stats()`: calls, rejections by reason, round-trip histogram, per-peer session stats.

## Things that trip people up

- `allow_plaintext` is `false`. Without TLS, `Cluster::start` fails until you set it.
- `ClusterConfig::new` sets the transport keepalive to 1 s / 4 s. To change it, set `cfg.transport.keepalive_interval`
  after `new`. It sets `connect_timeout` to 1.5 s, so one lost SYN, which the kernel sends again at 1 s, costs a dial
  nothing; for links between regions, see [Across regions](#across-regions). A dial that has not connected by then
  counts the peer as unreachable until a redial connects. A lease authority whose hold is shorter than the idle
  timeout caps the keepalive and `connect_timeout` at a quarter of the hold, whatever you set
  ([membership](#membership)), so such a link needs a longer lease.
- `tell` returning `Ok` says nothing about the far side. Use `send` when you need to know it was accepted.
- Dropping the `ActorSystem` without `shutdown()` looks like a crash to the others: no handoff, and the discovery
  record stays until it expires.
- With one node and no reachable peers, a discovery outage longer than `heartbeat_grace` stops keyed actors. Raise
  `heartbeat_grace` for single-node setups.
- Static seeds need fixed ports.
