# Design — windowed stream pushes

Status: built. The steps A3 (the TLA model), A4 (stream core and wire), A5 (the call split in
axon-cluster), B1 (the push driver), D2 (the WAN bench), the pushes part of C3 (metrics and console) and the window's
part of D3 (the rig) built it. A *lane* here is
only a traffic class of [lanes.md](lanes.md).

Words used here:

- A *parent* is a node that sends a stream's records to a *child* node that asks for them (an interest). The parent
  keeps one `Downstream` per child: the child's branches, its position in each stream, and what it has been sent.
- A *push* is one call from parent to child that carries items of one or more streams: `Push { items: Vec<(StreamKey,
  u32, Item)> }` (`crates/axon-stream/src/core.rs:162-165`). It is at most `max_push_bytes` (`(1 << 20) - 64`,
  `core.rs:229`) and `max_push_items` (1024, `core.rs:230`).
- The *round trip* (RTT) is the time from a frame's send to its answer. On the routes this plan is for it is 92, 190
  and 219 ms.
- A *boot* is one process life of a node. Its *incarnation* (`unix millis << 64 | random`,
  `crates/axon-net/src/config.rs:35-39`) names it. A clock step can make a later boot's incarnation compare lower, so
  this design never orders boots by it.

## 1. Scope

Today a parent sends one push to each child and waits for the answer before it sends the next
(`crates/axon-stream/src/node.rs:290-318`, `drain`). That is stop-and-wait: one push of at most 1 MiB per round trip.
At 92, 190 and 219 ms that is 11.4, 5.5 and 4.8 MB/s per child, whatever the link can carry. The relay measured about
5 MB/s at 219 ms.

This design lets a parent have several pushes in flight to one child:

1. Pushes to a child go out on a *channel*: a numbered sequence that belongs to one `Downstream`. Each push carries its
   channel id and its sequence number. The child applies only the next number. On a failure the parent sends again
   every push the child has not acknowledged, in order (go-back-N).
2. One stream may have several pushes in flight. Its bytes in unacked pushes count against its branch's bound
   (`CoreConfig::branch`) with what still waits in the branch, until the child has applied them, so `SlowConsumer`
   still ends a child that cannot keep up and the backlog per stream stays bounded (§2.5).
3. The bytes in flight on a channel are bounded by a window sized from the measured round trip and delivery rate,
   from `min(2 × max_push_bytes, max_push_window)` up to a ceiling setting (default 64 MiB). A node-wide cap (default 256 MiB) bounds all
   channels together.
4. axon-cluster gets a call that returns once the request is admitted, so the driver can have several calls open in
   seq order, and a way to read a peer's round trip.

Out of scope, owned by other specs:

- Lanes (Control, Live, Bulk sessions per peer): [lanes.md](lanes.md). This design only says how pushes use them (§9).
- Kernel socket buffers. Linux's default `tcp_wmem` maximum is 4 MiB, which caps one connection at about
  4 MiB / RTT (19 MB/s at 219 ms) whatever this window says. The host settings that lift it are in [wan.md](wan.md).

## 2. How it works

### 2.1 The channel

The parent gives each `Downstream` a channel id when it creates it (`handle_interest`, `core.rs:1059-1069`). Ids come
from a counter that starts at 1 for each boot of the parent and never repeats within the boot. A `Downstream` that is
dropped (`drop_downstream`, `core.rs:581`) and made again from the next interest gets a new id. That is the gap the
research note found: today a re-created `Downstream` cannot be told from the old one.

Pushes on a channel are numbered from 1. The parent keeps, per channel:

- `next`: the seq of the next new push;
- `acked`: the highest seq the child has acknowledged;
- `unacked`: for each seq above `acked`, the encoded push (`Bytes`), the keys and epochs of its items, what it charged
  each branch (§2.5), its size, when it was sent, and the channel's delivered byte count at that time.

The child keeps channel state per parent node, in a map of its own (not in `parents`, whose entries go when no
interest is in flight, `core.rs:1745`):

- `boot`: the parent's incarnation, read from the session's handshake identity. It is the real boot also for a static
  seed, whose `PeerId` the node zeroes (`node.rs:215-218`);
- per lane: `channel`, the channel id it follows, and `applied`, the highest seq it applied;
- `retired`: up to 4 earlier boots of that parent, refused on every lane. One boot for all lanes: the lanes are
  sessions of their own, so a new boot's push on one may come before the old boot's last on another.

The child forgets this state when the parent node leaves its view, when the parent answers one of its interests out of
sync (it holds nothing of the child, or another revision: its next channel starts from 1 anyway), or when the child
restarts. The out-of-sync case is what ends a wrong retire (§16, risk 1).

### 2.2 The child's rules

For a push `(boot, channel, seq)` from parent node `n` on lane `l`, where `l` is the lane its endpoint names
(`axon.stream.live` or `axon.stream.bulk`, §6), not the session it came on: a request on another lane than its
endpoint's is served anyway ([lanes.md §5.2](lanes.md#52-endpoints-and-their-lane)).

| Case | Action | Answer |
|---|---|---|
| `boot` is in `retired` | nothing | `Retired` |
| no state for `(n, l)`, `seq == 1` | follow `channel` with `applied = 0`; then as below | |
| no state for `(n, l)`, `seq > 1` | nothing | `Retired` |
| a new `boot` | retire the old boot (drop the oldest past 4) and forget every lane's channel; follow `channel` on `l` with `applied = 0`; then as below | |
| same boot, `channel` below the one followed | nothing | `Retired` |
| same boot, `channel` above | follow it with `applied = 0`; then as below | |
| current channel, `seq == applied + 1` | apply the items (`apply_push`, the body of today's `handle_push`); `applied = seq` | `Applied { applied, unwanted }` |
| current channel, any other seq | nothing | `Skipped { applied }` |

The third line covers a child that forgot the channel (the parent left its view and came back without a restart). The
parent then drops the `Downstream`, and the child's next interest makes a new one. That costs one interest round trip,
and it is safe: a new `Downstream` starts from the position the child's interest carries. A new boot or a newer channel
is followed from any seq: its seq 2 before its seq 1 is `Skipped { applied: 0 }`, and the parent goes back to 1. Each
answer other than `Applied` counts `push_skipped` by reason: `duplicate` (`seq <= applied`), `gap` (above
`applied + 1`) or `retired`.

Applying stays as it is today, with its position check per branch: a record at or below the branch's position is
dropped. The seq check orders pushes; the position check is the second guard when a channel's life ends with pushes in
the air (§2.5).

An `unwanted` list says which items the child no longer wants (`core.rs:1311`, `push_acked`). If the reply that
carried it is lost, the next push of that branch is answered unwanted again. Nothing is lost.

### 2.3 The parent's rules

On an answer to seq `k` on channel `c`:

| Answer | Action |
|---|---|
| `Applied { applied, unwanted }` | free every unacked push at or below `applied` (a cumulative ack) and release what each charged its branches (§2.5), take a rate sample (§2.4), apply `unwanted` to `k`'s items |
| `Skipped { applied }`, `applied >= k` | the same as `Applied` without a rate sample: `k` arrived twice |
| `Skipped { applied }`, `applied < k` | free and release up to `applied`; go back: send again every push above `applied`, in order, still charged; not for a `k` sent before the channel's last go-back, which only frees |
| `Retired` | drop the `Downstream` (`expire_downstream`, `core.rs:1322`) with its branches and their charges; its next interest makes a new channel |
| an answer for a channel no longer kept | ignore it |
| an `applied` below `acked` | ignore it: an older answer that came late. But for a `k` above `acked` sent since the last go-back, the child went back on what it answered (it forgot the channel and took a seq 1 again): drop the `Downstream`, as for `Retired` |
| an `applied` above the last seq sent (`acked` + the unacked count) | drop the `Downstream`, as for `Retired` (`downstream_expired`, "answered past its pushes"). Only a wrong or hostile child says it: clamping it to the last seq sent would free pushes the child never applied, and their records would be lost on that channel. Its next interest makes a new channel from the position it carries. Test: `core::an_answer_past_the_last_push_sent_drops_the_child` |

On a call error:

| Error | Action |
|---|---|
| `StaleNode` (the child's boot changed) | drop the `Downstream` at once, as today (`node.rs:310`) |
| any other | go back to `acked + 1` (`push_failed`); the driver calls `next_push` again one `tick` (200 ms) later. A failure of a push sent before the last go-back changes nothing. The same for a `Skipped { applied }` with `applied < k`: the driver waits a tick before it sends again |
| the oldest unacked push has failed for `downstream_expiry` (10 s) | drop the `Downstream`: `expire_unheard(now, downstream_expiry)`, which already drops a child whose interests stopped, drops it too. Only progress clears the failure, so a child whose interests arrive on Control while every push fails, or is answered `Skipped` without progress, is dropped all the same. Tests: `core::a_child_that_skips_every_push_is_given_up_though_its_interests_arrive`, `crates/axon-stream/src/node/tests.rs::a_child_whose_pushes_fail_while_its_interests_arrive_is_dropped` |

A push's id carries the channel's count of go-backs when it went out (`PushId::round`, kept in the parent only), so
the answers and failures of the pushes a go-back already resends do not go back again.

A resend is the same `Bytes`, byte for byte. The parent never re-encodes a sent push, so its items, their `done`
effects (`core.rs:1270-1308`, a Reset or End drops the branch once encoded) and its size do not change.

After any go-back the window starts again from the floor and the rate samples are cleared.

### 2.4 The window

```text
floor   = min(2 × max_push_bytes, max_push_window)   (2 MiB at the default)
rate    = the largest delivery-rate sample over the last max(10 × rtt, 1 s)
window  = clamp(2 × rate × rtt, floor, max_push_window)
```

- `rtt` is `cluster.round_trip(node)`: the shortest of the last four keepalive round trips
  (`crates/axon-net/src/session.rs:396`). A keepalive is not queued behind pushes the way a push answer is, so the
  window does not grow with its own queue: with lanes it is Control's, which carries no pushes (§9); before them a ping
  goes ahead of the session's outbox (`session.rs:713-718`) and waits only behind the socket's unsent bytes, at most
  `notsent_lowat` (128 KiB, [lanes.md §4.4](lanes.md#44-tcp_notsent_lowat)). Before the first answer to a keepalive
  there is no `rtt`, and the window is the floor.
- A sample, on an `Applied` answer for seq `k`: (bytes delivered now − bytes delivered when `k` was sent) / (now − the
  time `k` was sent). The same sample as BBR's delivery rate.
- With `W` bytes in flight the sample is about `W / rtt`, so the next window is `2W`: the window doubles each round
  trip while the child keeps up. At 219 ms it goes from 2 MiB to 64 MiB in 5 round trips, about 1.1 s.
- A push past a channel's first goes while at least half of `max_push_bytes` fits under `window` and under the node
  cap below, and while fewer than 64 pushes are in flight (a constant); it is cut to what fits. A smaller one would
  cost a call for little. The first push of a channel always goes, so a channel is never slower than stop-and-wait. A
  push takes a stream's items only while that stream's in-flight charge is under half its branch (§2.5); the other
  streams of the channel fill the rest of the window.
- Nagle's rule: a push under half of `max_push_bytes` that took all its branches had ready leaves the channel
  drained, and while a push is in flight a drained channel sends nothing more until an answer frees one, or a
  subscription is reset or ended. What is published meanwhile goes in one push: at a steady rate below what one push
  holds, a channel sends one push per answer, as stop-and-wait did, and a backlog still fills the window with full
  pushes. Without the rule, every publish woke a push of its own: `scale-streams` at 200 k records/s over 50 k streams
  went from 4.16 to 4.78 cores, and with it 4.04. A branch reading back from its ring that has put a whole batch in
  the push has more, so it does not leave the channel drained. Tests (`src/core/tests.rs`):
  `records_published_while_a_push_that_took_all_there_was_is_in_flight_wait_for_its_answer`,
  `a_backlog_of_small_records_read_back_from_the_ring_fills_the_window`.

Where a channel's bytes in flight are: in its lane's outbox (8 MiB on Live, 32 MiB on Bulk,
[lanes.md §4.2](lanes.md#42-limits-per-lane); `start_request_to` waits while it is full), in the socket (unsent at most
`notsent_lowat`, sent and not yet acknowledged at most `tcp_wmem`'s maximum), on the path, and in the child's socket
and inbox. The outbox does not cap the window; the kernel's send buffer does. A full 64 MiB on the path needs a
`tcp_wmem` maximum of 64 MiB, as [wan.md §8](wan.md#8-host-tcp-settings) sets it. At Linux's default 4 MiB a channel
keeps about 4 MiB on the path and the rest of its window queued in the outbox.

The node-wide cap: every push beyond its channel's first takes its size from `max_push_inflight` (256 MiB) and one of
1024 call slots (`PUSH_SLOTS`, a constant: a quarter of the cluster's default `max_pending`, 4096,
`crates/axon-cluster/src/cluster.rs:121`, which the core does not see). It stays a constant: `Cluster` has no
`max_pending` accessor for `StreamNode::start` to scale it by. On a cluster set to fewer slots no record goes wrong:
a push refused `Busy` is a failed call, sent again a tick later with those after it (§2.3), and a node whose pushes
keep failing for `downstream_expiry` drops the child. But the pushes, up to `PUSH_SLOTS` and each child's first, then
take a larger share of the node's calls, and its other calls can be refused `Busy` too. Such a node sets
`max_push_inflight` lower, or keeps the default `max_pending`. Making it
follow the config needs `Cluster::max_pending()` in axon-cluster, outside B1; §16 risk 2. A channel that cannot get them waits and counts
`stream.push_capped`; an answer that frees bytes, or a `Downstream` dropped with pushes in flight, wakes it. In-flight pushes hold encoded bytes outside every hub limit; this
cap is what bounds them on a parent with many children.

The cap is shared fairly. A channel is busy while it has a push ready to send or in flight; the one asking counts as
busy. With `b` busy channels a channel's share is `max_push_inflight / b` bytes and `1024 / b` slots past its first
push. It may take `min(free, max(share − mine, free − owed))`, where `mine` is what it holds past its first push,
`free` what the cap has left, and `owed` what the other busy channels still lack of their shares: up to its share
always, more only while what is left keeps every other busy channel its share. So one fast far child cannot hold the
others to one push per round trip. Test: `the_node_cap_is_shared_fairly_between_busy_channels` (`src/core/tests.rs`):
one fast and three slow children at a 3 MiB cap; each slow one reaches its 0.75 MiB share, less at most one push, while
the fast one is busy.

Ceiling: 64 MiB at 219 ms is 306 MB/s (292 MiB/s) per channel. The kernel limit of §1 is lower until the host is set
up as [wan.md §8](wan.md#8-host-tcp-settings) says.

### 2.5 A stream's pushes count against its branch

A branch, one child's subscription to one stream at the parent (`core.rs:1179`), is a hub subscription under
`CoreConfig::branch` (`BranchLimits::BRANCH`: `max_bytes` 32 MiB, `max_records` 65 536, `max_events` 4096) with
`SlowConsumerPolicy::Reset`. Today an event's charge is taken when it is queued (`Sub::enqueue`, `hub.rs:448-457`) and
given back when `next_push` polls it (`hub.rs:1476-1480`), so only what waits counts. With several pushes in flight
that would move a stream's backlog out of its branch into unacked pushes, where `SlowConsumer` does not see it. So the
charge stays until the child has applied the records:

- **Taken.** When `next_push` polls an event of a downstream branch, its charge (its bytes, its records and one event,
  as `StreamEvent::charge` counts them) moves from the branch's queued usage to its in-flight usage; it is not given
  back. `Sub::fits` (`hub.rs:439-445`) checks queued plus in flight against `max_bytes`, `max_records` and
  `max_events`. The hub's total (`max_queued_bytes`) counts only what is queued: bytes in flight are the node cap's
  (§2.4).
- **Bounded.** A push takes a stream's items only while that branch's in-flight bytes, records and events are each
  under half its limits; past that the stream's next items wait in the branch. A child that keeps up never fills its
  branch with its own pushes. One that falls behind fills the other half with what waits, and the branch ends with
  `ResetRequired(SlowConsumer)`, as today. At the defaults one stream has up to 16 MiB in flight: sixteen pushes of
  1 MiB per round trip, 77 MB/s at 219 ms (measured 69, **Default** below). A larger `core.branch.max_bytes` lets
  one stream fill more of the window (128 MiB fills the 64 MiB ceiling), and is also what the parent may hold for
  that stream and child. A push carries at most `hub.max_batch_records` records of each stream, not in all: a push of
  many small streams is not cut at 4096 records, and one stream's backlog goes in several pushes in flight.
- **Held at the child.** The child hands each push to its local subscribers as it applies it, and the pushes of one
  round trip arrive back to back. So a child's subscriber must hold what its parent has in flight of one stream: under
  half the branch, plus one push, at most 17 MiB, 36864 records and 3072 events at the defaults. `BranchLimits`'
  default, a subscription's, is therefore 17 MiB with the branch's 65536 records and 4096 events: it holds that and
  no more bytes, so a stalled subscriber costs half what a branch may. Before the window it was 1 MiB and 4096
  records, one push: a default subscriber that read once a round trip was reset `SlowConsumer` by the second push.
  Test: `core::a_default_subscriber_holds_the_pushes_of_a_round_trip_applied_back_to_back`, which fails at 16 MiB.
  The cost is on the hub's shared caps (256 MiB and a million records): default subscribers that never read fill them
  at 16 (records) or 15 (bytes), where about 245 did before, and then the others are reset. A node that serves many
  untrusted subscribers, a gateway, sets smaller `limits` on them. The other way, a child that paces what it hands
  its subscribers, is a larger change and not made here.
- **Released**, each to the branch and epoch the push charged (the item's epoch, checked as `push_acked` checks it
  today, `core.rs:1311-1320`), so a late answer never frees a newer branch of the same key:
  - on `Applied { applied }`, or `Skipped { applied }` with `applied >= k`: every push at or below `applied`;
  - when the branch ends (a `Reset` or `End` item encoded, `core.rs:1288`; an `unwanted` answer; a `SlowConsumer`
    reset; an interest that drops the key): its charge goes with its subscription;
  - when the `Downstream` goes (`Retired`, `StaleNode`, `downstream_expiry`): every branch of it and every charge.

  A release marks the branch ready again and wakes its channel: a stream held at half its branch, or a state held for
  room (`Sub::release_held`, `hub.rs`), goes in the next push. Without it the state waited for the next record.
- **Kept through a go-back.** A failed call, a `Skipped { applied }` with `applied < k` or a lost session frees
  nothing: the same bytes are sent again and stay charged until they are answered.

A branch made again for the same key starts at zero while the old one's pushes may still be in flight; those bytes are
the channel's, under its window and the node cap.

Order and no duplicates come from the seq per channel (§2.2, §2.3), not from the stream: a stream's pushes are applied
in seq order like every other, and a go-back resends every unacked push of the channel, several of one stream among
them. The position check per branch stays the second guard.

Why go-back-N and not selective repeat: one session is one TCP connection. A push on it is lost only when the session
fails, and then every later push on it is lost too. So go-back-N resends what selective repeat would. The seq check is
for the other case, pushes of two sessions (an old one and its redial) that arrive out of order.

**Default.** The branch, and `BranchLimits::default()` with it, was 8 MiB until 8, 16 and 32 MiB were measured on
build-1 (2026-10-27, shared with others' jobs). Each row is the branch and its subscribers at the same value:

| Branch | relay-wan, 1 stream, MB/s at 190 / 219 ms | Rig geo-quiet, `stream-throughput` | Held: 16 slow children / 16 relay children / 64 slow children (MiB) | LAN, stream rows |
|---|---|---|---|---|
| 8 MiB | 18.7 / 16.5 | FAIL 138 of 318 windows | 78 / 109 / 310 | the reference |
| 16 MiB | 41.0 / 35.0 | FAIL 12 of 318, the first 2 min | 138 / 173 / 375 | 21 of 21 within noise |
| 32 MiB | 79.5 / 68.8 | PASS | 508 / 453 / 526 | 21 of 21 within noise |

- relay-wan: `scripts/relay-wan.sh` (§12) with §8's settings, the median of three runs at a 1-minute load of 16
  to 55; on `Bulk` and the `s` cell at
  219 ms the same within 2 MB/s. No reset or gap in any cell. A stream carries about 90 % of half its branch per round
  trip at each value.
- Rig: `scripts/rig.sh` geo-quiet on `RIG_BOX`, seed 1, about 28 min each: the firehose's far follower b2 read 0.79 of
  what b1 sent through the whole 8 MiB run; at 16 MiB it lagged 3.3 to 5.2 s while the windows opened. `stream-window`,
  `firehose-gap` and `live-under-bulk` passed at all three, with no gap and no reset.
- Held: a scratch harness of two or three `RelayCore`s at 219 ms and 4 KiB records, children that answer for 20 s and
  then stop while the stream runs on; the process's peak RSS above its start. Every child ended `SlowConsumer`. The
  ceiling at any value is about `max_push_inflight` (256 MiB) plus the hub's `max_queued_bytes` (256 MiB, charged per
  branch over shared payloads) plus the ring, and a larger branch reaches it with fewer slow children. 16 or 64
  local subscribers that never read hold 17 to 33 MiB of RSS at each value, as the payloads are shared; 64 of 64 B
  records reach the hub's million records at about 61 MiB.
- LAN: the stream rows of `scripts/bench.sh` (`fanout`, `scale streams`, `scale subscribers`), built at each value,
  the three run in turn each round so the box's load falls on all alike. `bench.py compare` against 8 MiB found no
  regression (load 2 to 15, others' jobs). The widest row, `scale subscribers` processor time per record,
  was 740 ns at 32 MiB against 649 (rounds 509 to 746 against 561 to 655), inside its ±14.5 % band.

32 MiB is the only value the rig passes, and it gives one stream 69 MB/s at 219 ms. Its cost is memory under slow
readers: 16 slow children reach the node's half-GiB ceiling, where 64 at 8 MiB held 310 MiB, and the hub's caps fill
at fewer subscribers (Held at the child, above). So the branch is `BranchLimits::BRANCH`, 32 MiB. `max_records` stays
65536, so a stream of small records is bound by records first. With a subscription's default then cut to 17 MiB,
relay-wan's `s` cells (2026-10-28, load 2 to 3, three runs) read 69.0 MB/s at 219 ms and 78.5 at 190, against 71.2
for subscribers of four pushes, with no reset or gap.

### 2.6 A lost or reset session

The calls on the session end with `OutcomeUnknown` or `NotSent`. The parent waits one tick, then sends every unacked
push again from `acked + 1`, in order, on whatever session the cluster routes to (a redial). The child applies the ones
above `applied` and answers `Skipped` to the ones it already has. A push sent on the new session before the old
session's last push arrived is answered `Skipped { applied }` with `applied < seq`, and the parent goes back to it.

A half-open session (one side still thinks it is up) ends at `idle_timeout` at the latest: 4 s under
`ClusterConfig::new`, 8 s at [wan.md §9](wan.md#9-the-values-across-regions)'s values. Its calls end at
`request_timeout`, counted from admission (§4), and the go-back starts then. If no push is answered for
`downstream_expiry`, the `Downstream` goes, as today.

### 2.7 A child restart

The child's new boot has no channel state. The parent's calls to the old boot are refused `StaleNode` (the call checks
the route's incarnation, `cluster.rs:793-807`), and the parent drops that `Downstream` at once
(`node/tests.rs::a_push_for_a_child_that_restarted_is_not_sent_to_its_next_boot` holds this today). The new boot's
interests make a new `Downstream` with a new channel from the position they carry.

### 2.8 A parent restart

The parent's new boot starts its channel counter at 1. The child sees a new `boot` on the handshake identity, retires
the old one and follows the new channel from `applied = 0`. A push of the old boot still queued at the child (it was
read before the old session closed) is answered `Retired` and applied nowhere. Up to 4 retired boots are kept per
parent node, so this holds through 4 restarts inside one push's flight; a 5th older boot is §16's first risk.

The child's out-of-sync answer does not name the parent's boot, so a child cannot tell which boot it answers, and a
late push of a boot it does not know is followed as new (§16, risk 1). That stays as it is, because the cost is
bounded: each such push costs the live boot one channel re-create, one interest round trip, and no record (the
position check stays, and the next channel starts from the child's position). The late pushes are finite: only those
of a dead boot still queued at the child, at most that boot's window and 64 per channel, and a dead boot makes no new
ones. Naming the boot would change the interest wire and the PushWindow model for a case that already ends.

## 3. API

The stream config gains two fields. Nothing else in the public API of axon-stream changes.

```text
axon-stream
  StreamNodeConfig .core.max_push_window   bytes in flight to one child on one lane, at most (default 64 MiB) ; at least max_push_bytes
  StreamNodeConfig .core.max_push_inflight bytes in flight to every child together (default 256 MiB), past each one's first push ; at least max_push_window
    a child gets a push window from min(2 × max_push_bytes, max_push_window) up to max_push_window: twice its delivery rate times the round trip ; a stream's records in flight count against its branch (core.branch) until applied, and it takes a new push while they are under half of it ; a failed push and those after it are sent again, in order
  node.stats().relay.downstream[i] .channel .window_bytes .inflight_bytes .inflight_pushes .round_trip_us .delivery_rate .resent

axon-cluster
  cluster.start_request_to(node, incarnation, name, protocol, schema, body).await? -> PendingReply   returns once the request is admitted on a session (within request_timeout) ; PendingReply.await -> Result<Bytes, ClusterError>, due request_timeout after admission ; one call: counted, traced, a max_pending slot until it ends ; request_to is start_request_to(..).await?.await
  cluster.round_trip(node) -> Option<Duration>   the shortest of the last four keepalive round trips on the session to node ; None before the first answer
```

`RelayCore` (public in axon-stream, `lib.rs` `pub use core::*`, but not on the README's API page) changes in A4:

```text
  next_push(node, now, rtt) -> Option<(PushId, Bytes)>   resends first, in seq order ; None when the window or the node cap holds it, or every ready stream is at half its branch
  PushId { channel, seq, .. }   and the channel's go-backs when it went out (§2.3)
  push_answered(peer, PushId, PushAnswer, now)   replaces push_acked
  push_failed(peer, PushId, now)               go back at once ; the driver waits a tick before the next next_push ; expire_unheard drops a child failing past downstream_expiry
  handle_push(from, boot, channel, seq, push, now) -> PushAnswer   the child's rules of §2.2
  enum PushAnswer { Applied { applied: u64, unwanted: Vec<u16> }, Skipped { applied: u64 }, Retired }
  RelayStats .push_resent .push_skipped [duplicate, gap, retired] .push_capped
  StreamHub::poll_charged(id) -> Option<(StreamEvent, QueueUsage)>   as poll, its charge left on the branch as in flight ; hub.discharge(id, charge) gives it back (§2.5) ; hub.inflight(id)
```

C2 adds a `lane` after the node, peer or sender of each ([lanes.md §6](lanes.md#6-streams-on-lanes-axon-stream)), so
A4 does not wait for A2's `axon::Lane`.

`CoreConfig::new` checks `max_push_window >= max_push_bytes` and `max_push_inflight >= max_push_window`, next to its
other checks (`core.rs:471-490`).

## 4. The call split in axon-cluster (A5)

Today `call_remote` (`crates/axon-cluster/src/engine.rs:1404-1440`) takes a pending slot, submits the frame and waits
for the answer, all in one future. The deadline is set at the start of the call (`counted`, `engine.rs:1602-1642`), so
it includes routing, a dial and the wait for outbound credit. With several pushes queued behind each other on a
session, that wait is the window itself; a deadline counted from the start would end pushes that were never late.

The split:

1. `start_request_to` routes, checks the incarnation (`prepare`, `engine.rs:1360-1388`), takes a `max_pending` slot
   (`Busy` if none), and awaits `submit` (`session.rs:303-316`) for outbound credit. All of it within
   `request_timeout`, after the route: a route not cached is looked up first, by a request of its own with its own
   `request_timeout`, as `request_to` does today. It returns a `PendingReply` once the frame is queued on the session.
2. `PendingReply` is a future of the answer. Its deadline is `request_timeout` from admission. It holds the pending
   slot and the call's trace span and counters until it ends or is dropped.
3. `request_to` (`cluster.rs:793-807`) becomes `start_request_to(..).await?.await`. Its other callers
   (`crates/axon-cluster/src/quota/mod.rs:398`, `crates/axon-cluster/tests/cluster/remote.rs:366-380`) do not change.

Pushes on one channel stay in seq order: the driver awaits each `start_request_to` before it starts the next, and a
session's frames leave in submit order.

`start_request_to` goes on the lane its endpoint is served on, which the directory's answer names
([lanes.md §5.3](lanes.md#53-routes-and-the-directory)); it takes no lane of its own.

`round_trip(node)` reads `Sender::min_rtt` (`session.rs:396`, already read for routing at `engine.rs:911`) of the one
session to the node; B2 moves it to the session of the node's Control lane
([lanes.md §5.1](lanes.md#51-lanes-per-peer)). A session sends its first ping one `keepalive_interval` after it starts
(`session.rs:603`): 1 s under `ClusterConfig::new`, 10 s for a bare axon-net `Config`. So A5 also sends one keepalive
as soon as a session is up, in `crates/axon-net/src/session.rs`. Without it the window stays at the floor for about a
second after each dial in a cluster. With lanes, Control is up before a push's lane is dialled, so the early keepalive
matters after a Control redial only.

## 5. The driver (B1)

One task per channel, as today's pump per child (`Inner::pump` in `node.rs`). `Inner::drain` loops while the core
holds a `Downstream` for the node:

1. Unless a retry tick is pending: while `next_push(node, now, cluster.round_trip(node))` gives a push,
   `start_request_to` it to the child's boot, and keep the `PendingReply` with its peer and `PushId` in a queue in seq
   order (a `VecDeque` whose front is awaited: a `FuturesOrdered` without the dependency). A `StaleNode` expires the
   `Downstream` at once; any other error is `push_failed`, and the next send waits a tick.
2. Return when nothing is open and no tick is pending. Else await the first of: the front's answer, a wake from the
   core (new items, a freed window, the `Downstream` gone), or the pending tick.
3. Hand the answer to the core: `push_answered`, or `push_failed` for an error or an answer that does not decode (an
   admitted call never ends `StaleNode`). A failure, or a `Skipped { applied }` below its seq, of a push of the last
   round sent sets the tick (the first one set stands, so the failures of a window that went back do not push it
   later, and those of an older round, which the core does not go back for, do not hold the resent pushes).

The pushes it has open when the `Downstream` goes are dropped with the task's queue, which abandons their calls. A push
of a new round, boot or channel first lets go of the calls before it of another: their pushes went again or are not
wanted, and their calls would hold call slots past `PUSH_SLOTS` (a child that answers each round's first push at once
and holds the rest could keep two windows of calls) and hold the answers behind them. The loop that retried one push
with `failing_since` went; the core counts how long the oldest push has failed.

The start checks (`node.rs:408-414`) stay as they are: `max_push_bytes` counts the push header already (`next_push`
starts its count at `PUSH_HEADER`, `core.rs:1210`), so `max_push_bytes <= cluster.max_frame(lane)` of each lane in use
holds a push in one frame. C2 made it per lane ([lanes.md §6](lanes.md#6-streams-on-lanes-axon-stream)).

## 6. Wire

`SCHEMA` of `axon.stream` goes from 6 to 7 (`node.rs:35`), this design's only bump of it. With it pushes move off
`axon.stream` to the push endpoints `axon.stream.live` and `axon.stream.bulk`
([lanes.md §6](lanes.md#6-streams-on-lanes-axon-stream)): A4 serves both names from the one closure (`node.rs:449`),
both on Live, and the parent sends to `axon.stream.live`; C2 moves `axon.stream.bulk` to the Bulk lane and picks the
name by namespace, with no byte or schema change. The endpoint names the lane, so the push header has no lane byte.
Interests stay on `axon.stream` with their bytes unchanged; from SCHEMA 7 it refuses a push, and the push names refuse
an interest, with `InvalidMessage`. A4 changes `node.rs` only so far as these names and the new core calls need, with
one push in flight; B1 writes the windowed driver.

### Push

```text
before (SCHEMA 6):  02 | count u32 | items…
after  (SCHEMA 7):  02 | channel u64 | seq u64 | count u32 | items…
```

All integers big-endian, as today. The items do not change (`crates/axon-stream/src/wire.rs:223-293`). `PUSH_HEADER`
(`wire.rs:216`) goes from 5 to 21 bytes.

The pinned fixture in `crates/axon-stream/src/wire/tests.rs` (key `md/btc`, 8 items, epoch 3) gets channel `0x0102`
and seq 7. Its first bytes become:

```text
02  00 00 00 00 00 00 01 02  00 00 00 00 00 00 00 07  00 00 00 08  00 02 6d 64 …
```

and the rest of `PUSH_BYTES` stays as it is.

### Push answer

`encode_unwanted` / `decode_unwanted` (`wire.rs:329-335`, a list of `u16`) are replaced by one answer codec:

```text
status u8 | applied u64 | unwanted u16…     status 0 Applied, 1 Skipped, 2 Retired ; unwanted only after 0
```

| Answer | Bytes |
|---|---|
| `Applied { applied: 7, unwanted: [1, 300] }` | `00  00 00 00 00 00 00 00 07  00 01 01 2c` |
| `Skipped { applied: 5 }` | `01  00 00 00 00 00 00 00 05` |
| `Retired` | `02  00 00 00 00 00 00 00 00` |

A decoder refuses a status above 2, a length that is not `9 + 2n`, and unwanted after status 1 or 2.

### A size check that moves

`RelayCore::new` checks that the largest batch fits a push with `+ 64` bytes for the push header and one item's fields
(`core.rs:473-475`, which says 50 = 5 + 45). With a 21-byte header that is 66 and the check is wrong by 2. A4 writes it
as `PUSH_HEADER + ITEM_FIXED` from `wire.rs`, so it cannot drift again.

## 7. The TLA model PushWindow (A3)

`spec/tla/PushWindow.tla`, in the style of `RelayGiveUp.tla`: one parent, one child, streams `{s1, s2}`. Its header
lists what it abstracts, what that could hide, and where the code is wider than the model. The main gaps: the round
trip, the rate and the window's growth (`Window` is a constant, in pushes), and the node cap with its floor and fair
share (one parent has one child); a charge is counted in items, not bytes. The sim and the core tests (§10) cover
those.

State:

- `pboot`, `cboot`: the boots of the parent and the child (1..`MaxBoots`).
- `ds`: the parent's `Downstream`, or none: `[cb, ch, next, acked, un: seq → items, open, pos: stream → record, q:
  stream → items queued, charge: stream → items, ep: stream → branch epoch, forgot]` (`forgot`, history: a `Forget`
  since it was made).
- `nextCh`: the parent's channel counter for its boot.
- `cs`: the child's channel state `[boot, ch, applied]`; `retired`: the set of parent boots it retired (two boots
  fill no list: `MAX_RETIRED`'s cap is `a_parent_boot_retired_stays_retired`'s);
  `synced`: its next interest may be a delta (false after an out-of-sync answer or a restart: it is whole).
- `have[s]`: the records the leaf below the child has of `s`; `got[s]`: the sequence it received (a history).
- `net`: a set of messages: `push(pboot, cboot, ch, seq, cur, items)`, `answer(pboot, ch, seq, cur, status, applied)`,
  `interest(cboot, have, whole)`. Rounds are only compared with the channel's current one, so a message carries `cur`,
  whether it is of the current round, and a go-back turns the channel's messages in flight old.

Actions: `Publish` (the origin adds a record to each branch at its edge; a branch whose `charge` would pass
`MaxBranch` is reset instead: `SlowConsumer`, its queue becomes a reset item and its epoch goes up; a branch behind
reads from the store up to its bound whenever its charge goes down), `Send` (the parent issues the next push within
`Window`, taking a stream's items while its items in flight stay at most `MaxInflight`, the model's half of the
branch), `DeliverPush` (§2.2), `DeliverAnswer` (§2.3: releasing the charge of the freed pushes to the epoch they
charged; an answer of a push sent before the last go-back only frees; one below `acked` for a push above it of the
current round drops `ds`), `Fail` (a call of the current round fails: go back to `acked + 1` in the next round,
releasing nothing), `Timeout` (nothing of the channel is in flight: the same go-back), `Expire` (the parent drops
`ds`), `Stale` (the child's boot changed: the parent drops `ds`), `Interest` (the child asks the parent node with its
`have`, which can be stale), `DeliverInterest` (§2.1: the parent makes `ds` from a whole interest, with `pos := have`,
or opens the branches it lacks for the child boot it holds; it answers any other delta out of sync and drops a `ds` of
another child boot; the child takes the answer in the same step, and out of sync forgets its channel state for the
parent, `retired` with it), `Forget` (the parent leaves the child's view and comes back while the child follows its
live channel: the child forgets the same, the parent keeps `ds`), `ParentRestart`, `ChildRestart`, `Lose(m)`.
Answers and interests arrive in any order, and pushes across rounds, channels and boots; a push overtakes an earlier
one of its own round only as a fault (one session keeps their order), and a push arrives twice only as a resend.

Invariants:

| Name | Says |
|---|---|
| `InOrder` | `got[s]` is `1..Len(got[s])` for every stream: no duplicate, no gap |
| `ChannelExact` | the child applied the seqs `1..applied` of its channel, each once |
| `AckSound` | the parent's `acked` is at most the child's `applied` on that channel, unless the child forgot the channel since `ds` was made (`Forget`: it may follow it again from seq 1) |
| `FewChannels` | a parent boot makes at most `MaxChan` channels: a `Downstream` is made again only after a fault, a restart or a late push of a boot the child does not hold retired, so a child that answers every new channel `Retired` breaks it |
| `WindowBound` | the parent has at most `Window` pushes unacked |
| `CountExact` | a stream's `charge` is its `queued` items plus its items of the current epoch in unacked pushes |
| `BranchBound` | what the parent holds for a stream, its `queued` items and its items of the current epoch in unacked pushes, is at most `MaxBranch` |
| `StreamInflight` | a stream's items in unacked pushes of its current epoch are at most `MaxInflight` |

Property: `Done`, every published record reaches `got`, under weak fairness on `Send`, `Timeout`, `Stale`,
`Interest`, `DeliverPush`, `DeliverAnswer` and `DeliverInterest`, with faults (loss, a reorder in a round, `Fail`,
`Expire`, `Forget`, a restart) at most `MaxFaults` in a behaviour. A reset leaves the child to ask again from its
`have`; the origin keeps every record, so the parent sends it again.

Configs:

| File | What |
|---|---|
| `PushWindow.cfg` | streams `{s1, s2}`, `MaxRec = 2`, `Window = 2`, `MaxInflight = 2`, `MaxBranch = 2`, `MaxBoots = 2`, `MaxChan = 5`, `MaxFaults = 2`; every invariant, `Done`: several pushes of one stream in flight, and no slow-consumer reset (`MaxRec = MaxBranch`) |
| `PushWindowOneStream.cfg` | streams `{s1}`, `MaxRec = 5`, `Window = 3`, `MaxInflight = 2`, `MaxBranch = 4`, the rest as above: one stream fills the window, and a child that falls behind is reset with pushes in flight; every invariant, `Done` |
| `PushWindowThreeFaults.cfg` | streams `{s1}`, `MaxRec = 3`, `Window = 2`, `MaxInflight = 2`, `MaxBranch = 3`, `MaxBoots = 1`, `MaxChan = 4`, `MaxFaults = 3`: a `Fail`, an `Expire` and a `Forget` let the child go back on what it answered, where the drop for an answer below `acked` (§2.3) is needed; every invariant, `Done` |

Mutations, each a `mutations/PushWindow-<Name>.cfg` that must end with a violation (`scripts/check.sh tla` runs them):

| Mutation | The change | Must break |
|---|---|---|
| `AcceptAhead` | the child applies any `seq > applied` | `ChannelExact`, `InOrder` |
| `ApplyDuplicate` | the child applies a `seq <= applied` again | `ChannelExact` |
| `AckOnSend` | the parent counts a push acknowledged when it sends it | `AckSound` |
| `AckAboveApplied` | the child answers `applied + 1` | `AckSound` |
| `ResendFromNext` | a go-back starts at `acked + 2` | `Done` |
| `KeepAppliedOnNewBoot` | the child keeps `applied` when the parent's boot changes | `ChannelExact`, `InOrder` (and `Done`) |
| `NoForgetOnOutOfSync` | the child keeps its channel state when an interest is answered out of sync (§2.1): a late push of a boot it never followed retires the live boot for good (§16, risk 1) | `FewChannels` (a stall: the live boot's every channel is `Retired`) |
| `IgnoreWentBack` | an answer below `acked` is ignored also for a push above `acked` of the current round (§2.3): the child forgot the channel and took a resent seq 1 | `Done` (the parent sends seqs the child never applies) |
| `ReuseChannel` | a new `Downstream` takes the last channel id again | `InOrder` |
| `CreateFromEdge` | a new `Downstream` starts at the origin's edge, not the interest's `have` | `InOrder` |
| `NoPositionCheck` | the child applies records at or below the branch's position | `InOrder` |
| `RetiredAsApplied` | the parent treats `Retired` as an acknowledgement and goes on | `Done` (not `InOrder`: the position check holds; the freed pushes' records never come) |
| `NoWindow` | `Send` ignores `Window` | `WindowBound` |
| `UncountedInflight` | `Send` takes a stream's items out of its `charge` (today's poll) | `CountExact`, `BranchBound` |
| `ReleaseOnGoBack` | a go-back releases the charge of the pushes it resends | `CountExact` |
| `ReleaseAcrossEpochs` | an answer releases its pushes' charge from the stream's current epoch, whichever epoch they charged | `CountExact` |
| `NoStreamLimit` | `Send` ignores `MaxInflight` | `StreamInflight` |

The four count mutations run on `PushWindowOneStream.cfg`'s constants, where a reset with pushes in flight happens;
`IgnoreWentBack` on `PushWindowThreeFaults.cfg`'s (TLC's shortest trace: a `Fail`, an `Expire` and a `Forget`; the
child then follows a late seq 1 of the expired channel, and the new channel's seq 2 from `applied = 0`, which it
answers `Skipped { 0 }` below `acked`); the others on `PushWindow.cfg`'s. Each mutation's config checks only what it
must break, so `check.sh tla` shows that one is violated; where the table names two, each was seen violated alone.
`StreamInflight` checks `Send`'s rule: the code takes whole batches while under half, so it can pass half by one batch.

Results (TLC 2026.10.06 on build-1, in `scripts/check.sh tla`: 4 workers a config, 6 configs at once, the box shared
with other jobs): `PushWindow.cfg` passes with 820 K distinct states, depth 45, in 2 min; `PushWindowOneStream.cfg`
with 3.67 M, depth 56, in 18 min (2 min 42 s alone on 8 workers), under `SeedMembership.cfg`'s 30;
`PushWindowThreeFaults.cfg` with 131 K, depth 36, in 20 s. Every mutation fails, in 1 to 23 s.

What the model changed in this design:

- A list of retired boots alone stalls a stream: with two boots, a late push of the one the child never followed
  retires the live one, whose every channel is then `Retired` (`NoForgetOnOutOfSync`). A4 added the out-of-sync forget
  (§2.1) that ends it; the model checks it, and the round in a push's id (§2.3) and the drop for an answer below
  `acked` (`IgnoreWentBack`) with it.
- The forget empties `retired` too, so the old boot is not retired after it: each late push of that boot retires the
  live one again, and each costs a new channel (§16, risk 1 says so). In TLC's trace on `PushWindowOneStream.cfg` at
  `MaxChan = 3`, boot 1 sends seq 1 and 2 and restarts; the child follows boot 2, boot 1's seq 1 retires it, the
  forget and channel 2 follow, then boot 1's seq 2 retires boot 2 again, and a child restart makes channel 4. Safe,
  and bounded by the old boot's pushes in flight; the configs allow 5 channels. An out-of-sync answer that named the
  parent's boot would let the child keep the old boot retired; a model of it still needs a channel per late push after
  a `Forget`.
- `BranchBound` bounds what the parent holds for a branch, queued and in flight, not its `charge` alone: with today's
  poll (`UncountedInflight`) the charge stays under the bound while what is held passes it.
- `KeepAppliedOnNewBoot` breaks safety, not only `Done`; `RetiredAsApplied` breaks `Done`, not `InOrder`. A push
  overtakes an earlier one of its round only as a fault: without that bound, a child with no channel state that
  always sees seq 2 first retires every new channel, and no design keeps `Done` (one session keeps the order).

## 8. Metrics and the console (C3, pushes part)

The existing stream metrics stay (`crates/axon/guide/telemetry.md:58-69`: `stream.pushes` and `stream.push_bytes` by
`direction`, `stream.downstream_expired`, `stream.unwanted_items`). New ones, per node, emitted next to them in
`crates/axon-obs/src/telemetry.rs`:

| Metric | Kind | Says |
|---|---|---|
| `stream.push_inflight_bytes` | gauge | bytes in unacked pushes to every child |
| `stream.push_resent` | counter | pushes sent again after a failure or a gap |
| `stream.push_skipped` `reason=duplicate\|gap\|retired` | counter | pushes this node, as a child, did not apply |
| `stream.push_capped` | counter | times a channel waited for the node-wide cap |

`DownstreamStats` (`core.rs:245-253`) gains `channel`, `window_bytes`, `inflight_bytes`, `inflight_pushes`,
`round_trip_us`, `delivery_rate` (bytes/s) and `resent`. The dashboard's `downstream` type (`ui/src/api.ts:228`)
gains the same fields.

The Streams page (`ui/src/features/streams/StreamsPage.tsx`) shows them on each child's edge: "In flight", "Window",
"Round trip", "Rate", "Resent". Short labels in ASD-STE100; what a window is goes in the page's help dialog. The red,
dotted edge for more than 1 MiB queued stays as it is. C3 sees it on a `scripts/ctl.sh` run with the verify-axon skill.

## 9. Lanes

[lanes.md](lanes.md) owns lanes. What this design takes from it:

- Pushes go to `axon.stream.live` or `axon.stream.bulk`, by the stream's namespace's lane. Interests stay on Control,
  under `axon.stream`.
- A `Downstream`, its channel, window and driver task are per (child node, lane). The child's channel state is per
  (parent node, lane). A Bulk backlog never holds a Live push: they are different channels on different sessions.
- The node-wide cap is shared by both lanes. Each channel's first push is outside it, so Live always has one push in
  flight however full Bulk is.
- `round_trip` is read from the Control lane's session, which carries no pushes.
- The start check uses each lane's `max_frame`.
- A Live channel shares Live's session, and its outbox, with the application's calls. A window of twice the delivery
  rate times the round trip keeps about one round trip of pushes queued there while the path is the bottleneck, so a
  Live call to that child can wait up to one round trip more, and never more than the 8 MiB outbox takes to drain.
  Measured on today's one connection (before this design, build-1):
  with 8 MiB pushes the origin's requests to a leaf took two round trips, 438 ms at 219 ms. So a namespace that moves
  much more than its node's calls goes on Bulk, and the guide says so beside `StreamNodeConfig::lane`
  ([lanes.md §6](lanes.md#6-streams-on-lanes-axon-stream)). Live pushes have no outbox or window ceiling of their own.

Until lanes land (B2, C2), the channel is per child node and the one session to it, and everything above holds with
one lane.

## 10. Tests

Each fails on main and passes after. The last column says why it fails on main.

The core tests go in a new `crates/axon-stream/src/core/tests.rs`, as `src/node/tests.rs` and `src/wire/tests.rs` sit
beside theirs; `core.rs` has none today. They run on a config scaled by 1/16 (64 KiB pushes, a 2 MiB window, 16 KiB
batches), so each takes milliseconds; the figures in their rows are the scaled ones.

| Test | File | Asserts | On main |
|---|---|---|---|
| `wire_bytes_are_pinned_to_the_schema` (changed) | `crates/axon-stream/src/wire/tests.rs` | `SCHEMA == 7`; the push and answer bytes of §6 | SCHEMA 6, old bytes |
| `frames_round_trip_and_every_truncation_is_refused` (changed) | same | every truncation of the new push and answer is refused; status 3 is refused | no answer codec |
| `a_child_applies_only_the_next_push_of_its_channel` | `crates/axon-stream/src/core/tests.rs` | seq 2 on a channel never followed is `Retired`; on a newer channel, seq 2 before 1 is `Skipped{0}`; 1 then 2 applied; 2 again `Skipped{2}`, nothing applied twice | no seq |
| `a_downstream_made_again_gets_a_new_channel_and_its_old_pushes_are_retired` | same | drop and re-create a `Downstream`: a new id; the child answers an old-channel push `Retired` and applies nothing of it | no channel id (`core.rs:1059-1069`) |
| `a_parent_boot_retired_stays_retired` | same | after boot B follows A, a late push of A is `Retired`; 4 retired kept, a 5th drops the oldest | no boot check |
| `a_late_push_of_a_boot_never_followed_costs_the_live_boot_one_new_channel` | same | a push of a boot the child never followed retires the live one; the parent drops the `Downstream`, the out-of-sync refresh clears the child's channel state, and the next channel delivers every record once | no channel |
| `a_child_that_answers_below_what_it_applied_makes_the_parent_start_a_new_channel` | same | an answer `Skipped { 0 }` to an unanswered push after seq 1 was acknowledged drops the `Downstream`; the next channel delivers every record once | no answer codec |
| `a_child_that_forgot_its_channel_makes_the_parent_start_a_new_one` | same | a push with `seq > 1` on an unknown channel is `Retired`; the parent drops the `Downstream` and the next interest starts channel 2 from its position | no channel |
| `the_window_grows_with_the_rate_and_stops_at_its_ceiling` | same | 32 streams with backlogs, `rtt` 200 ms and every push answered after it: bytes in flight double each round trip from the floor (128 KiB) and stop at `max_push_window` (2 MiB) | one push in flight |
| `one_stream_reaches_several_pushes_per_round_trip_up_to_half_its_branch` | same | one stream with a backlog, the same answers: with `branch.max_bytes` 512 KiB its pushes in flight grow past one and stop at its half, less than a push above 256 KiB; with 8 MiB they reach `max_push_window` | one push in flight |
| `a_slow_reader_is_reset_with_pushes_in_flight` | same | one stream at half its 512 KiB branch in flight whose answers stop: what waits grows to the other half, the branch ends `ResetRequired(SlowConsumer)`, the parent never holds more than `branch.max_bytes` for it, and the reset goes out in the next push | no push window: on main the reset only ever sees what waits |
| `a_streams_charge_is_released_on_applied_on_its_end_and_on_a_retire_but_not_on_a_go_back` | same | the branch's in-flight charge after each of §2.5's cases: an `Applied`, a `Skipped` below `k` (kept), a failed call (kept), a `Reset` item, an `unwanted`, a `Retired`; a late answer for an old epoch frees nothing of the new branch | no charge in flight |
| `the_node_cap_holds_every_channel_past_its_first_push` | same | 4 children, `max_push_inflight` = 3 MiB: at most 3 MiB past the first push each; `push_capped` counts | no cap |
| `the_node_cap_is_shared_fairly_between_busy_channels` | same | one fast and three slow children at a 3 MiB cap: each slow one reaches its share less a push (§2.4) | no cap |
| `every_leaf_sees_every_record_once_while_relays_fail_and_return` (changed) | `crates/axon-stream/tests/sim.rs` | the sim gets 8 keys in two namespaces (with [wan.md §6](wan.md#6-stream-relay-timings-per-namespace-work-item-a4)'s `RelayTimings` drawn per namespace) and several pushes in flight per pair (its `pump`, lines 187-200, stops skipping a pair with one in flight); loss, duplication, reorder and restarts; every record once and in order | the sim has one key and one push per pair |
| `the_same_seed_gives_the_same_run` (unchanged) | same | the windowed sim replays | |
| `pushes_resent_after_a_lost_answer_are_applied_once` | same | lose answers and calls, and deliver pushes twice and out of order, at random; `push_resent > 0`, `push_skipped{duplicate} > 0`, `push_skipped{gap} > 0`, every record once | no resend count |
| `a_request_admitted_late_counts_its_timeout_from_admission` | `crates/axon-cluster/tests/cluster/remote.rs` | a full outbound queue holds a request 2 s with `request_timeout` 3 s; its answer after 2.5 s more is `Ok` | the deadline counts from the start: `Timeout` |
| `several_started_requests_are_answered_in_their_order` | same | 16 `start_request_to` calls without awaiting answers; all answered; the far side saw them in order | no such call |
| `the_round_trip_is_known_one_round_trip_after_a_dial` | `crates/axon-net/tests/session.rs` | over a 100 ms link, `min_rtt` is about 200 ms within 300 ms of the session's start | first pong after `keepalive_interval` |
| `a_leaf_200_ms_away_reads_several_pushes_per_round_trip` | `crates/axon-stream/tests/world_relay.rs` | a parent and a child 100 ms apart each way, 32 streams with backlogs: the child gets 30 MiB in under 2 s of World time after its first record; every record once | 1 MiB per 200 ms: about 10 MiB |
| `one_stream_200_ms_away_reads_several_pushes_per_round_trip` | same | the same with one stream: the child gets 20 MiB in under 2 s after its first record, at least four pushes in flight once the window passes 4 MiB; every record once | 1 MiB per 200 ms: about 10 MiB |
| `a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides` | same | the test above with `reset`, a crash of each node and a restart, under `seeds()`: every leaf sees every record once and in order | one push in flight |

The World test must set `WorldConfig::window` (bytes unacknowledged per connection, default 64 KiB,
`crates/axon-testkit/src/world.rs:66`, 79) to 64 MiB with a `//` reason: at 64 KiB the World's own connection caps it
at 320 KiB per 200 ms, below today's push rate.

Fuzz (A4): `fuzz/fuzz_targets/stream_wire.rs` decodes the new header and answer; `stream_core.rs` drives
`handle_push` with random `(boot, channel, seq)` and checks that no record is applied twice, and that every branch's
charge in flight is zero once every push is answered or its `Downstream` gone.

## 11. Fault rows

A new table `### streams: push window` goes into [faults.md](faults.md) in B1's commit, when the tests exist
(`scripts/check.sh faults` fails on a test that does not). Short names here, full paths in faults.md: `core` is
`crates/axon-stream/src/core/tests.rs`, `sim` is `crates/axon-stream/tests/sim.rs`, `wr` is
`crates/axon-stream/tests/world_relay.rs`, `remote` is `crates/axon-cluster/tests/cluster/remote.rs`.

| Fault | Covered by |
|---|---|
| crash | `wr::a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides` (parent and child, each seed `Crash::Process` or `Crash::Machine`) |
| restart | `core::a_parent_boot_retired_stays_retired`, `core::a_downstream_made_again_gets_a_new_channel_and_its_old_pushes_are_retired`, `wr::a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides`, `crates/axon-stream/src/node/tests.rs::a_push_for_a_child_that_restarted_is_not_sent_to_its_next_boot` |
| pause | `wr::a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides` (a `pause` of the child past `request_timeout`, and a `cut` of its answers past it: the half-open case) |
| partition | `sim::every_leaf_sees_every_record_once_while_relays_fail_and_return` |
| one-way cut | `wr::a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides` (`cut` of the answers only: pushes arrive, answers do not, then go-back), `crates/axon-stream/src/node/tests.rs::a_child_whose_pushes_fail_while_its_interests_arrive_is_dropped` (pushes refused while interests arrive) |
| loss | `sim::pushes_resent_after_a_lost_answer_are_applied_once`, `wr::a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides` (`reset`), `crates/axon-stream/src/node/tests.rs::a_child_whose_pushes_fail_while_its_interests_arrive_is_dropped` |
| duplication | `sim::pushes_resent_after_a_lost_answer_are_applied_once`, `core::a_child_applies_only_the_next_push_of_its_channel` |
| reordering | `sim::every_leaf_sees_every_record_once_while_relays_fail_and_return`, `core::a_child_applies_only_the_next_push_of_its_channel` |
| latency | `wr::a_leaf_200_ms_away_reads_several_pushes_per_round_trip`, `wr::one_stream_200_ms_away_reads_several_pushes_per_round_trip`, `core::the_window_grows_with_the_rate_and_stops_at_its_ceiling` |
| clock skew or step | `wr::a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides` (a `step_wall` back on the parent before its restart: an older incarnation the child must not order by) |
| CPU starvation | `wr::a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides` (`starve` of the child: a slow child) |
| memory limit | `core::the_node_cap_holds_every_channel_past_its_first_push`, `core::a_slow_reader_is_reset_with_pushes_in_flight`, `core::a_streams_charge_is_released_on_applied_on_its_end_and_on_a_retire_but_not_on_a_go_back`, `core::a_default_subscriber_holds_the_pushes_of_a_round_trip_applied_back_to_back` |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | n/a: no descriptors of its own; a redial that cannot open one is a failed call (loss) |
| panic | n/a: no user code |
| overload | `remote::a_request_admitted_late_counts_its_timeout_from_admission`, `core::the_node_cap_holds_every_channel_past_its_first_push`, `core::a_slow_reader_is_reset_with_pushes_in_flight` |
| hostile input | `fuzz/fuzz_targets/stream_wire.rs::fuzz_target`, `fuzz/fuzz_targets/stream_core.rs::fuzz_target`, `crates/axon-stream/src/wire/tests.rs::frames_round_trip_and_every_truncation_is_refused`, `core::an_answer_past_the_last_push_sent_drops_the_child` |

## 12. Bench (D2)

`relay_wan`: an origin and a child in two Linux network namespaces joined by a veth pair, with `netem` delay on it.
One stream and 64 streams, each with a backlog, on a veth held to 1 Gbit/s, which caps every cell at 125 MB/s. Rows
go in `crates/axon/guide/benchmarks.md` and the baseline for build-1 in `scripts/bench/<cpu>.csv`.

| RTT | Today, any streams (1 MiB / RTT) | After, 1 stream (4 MiB / RTT) | After, 64 streams, Linux defaults | After, 64 streams, [wan.md](wan.md) host settings |
|---|---|---|---|---|
| 0 ms | measure | measure | measure | measure |
| 92 ms | 11.4 MB/s | about 45 MB/s | about 45 MB/s (4 MiB wmem) | measure; ceiling 729 MB/s |
| 190 ms | 5.5 MB/s | about 22 MB/s | about 22 MB/s | measure; ceiling 353 MB/s |
| 219 ms | 4.8 MB/s (measured about 5) | about 19 MB/s; target 13 MB/s with the host settings | about 19 MB/s | measure; target 60 MiB/s, ceiling 306 MB/s |

The numbers in the table are what the limits gave, not measurements. One stream was held to half its 8 MiB branch per
round trip (§2.5; 32 MiB since, **Default** there), which is also what Linux's default 4 MiB `tcp_wmem` gives one
connection, so its column is the same under both host settings; D2 measures it under both. D2 measures every cell
before and after, on the same box. The merge gate is two cells at 219 ms with [wan.md](wan.md#8-host-tcp-settings)'s settings: 64 streams, target 60 MiB/s,
and 1 stream, target 13 MB/s, [wan.md](wan.md)'s peak on one stream across sin ↔ us. Measured today, one stream reached 4.2 MB/s
there, and 12.3 MB/s with 8 MiB pushes.

### The rig (D3)

D3's geo night ([wan.md §12](wan.md#12-the-rig-work-item-d3)) holds the window on real TCP under `netem`, where the
World's model of a connection is not the kernel's.

**The `firehose` workload.** One stream from b1 (eu-a) in namespace `firehose`, `FIREHOSE_MBS` (13) MB/s in 4 KiB
records, followed `Live` on b2 (sin, 190 ms) and b3 (us, 92 ms). The namespace is on Bulk, as the guide says for a
heavy one (§9). It logs, per follower and second, the records and bytes it received and its lag behind the origin's
publish time; every node logs its `DownstreamStats` (§8) each second in a `window` line of `firehose.jsonl`. A
follower also logs its last cursor each second, a `gap` line for a batch that does not follow the last record of its
generation within one subscription, and a line for each `Ready`, `OriginLost` and `Replacement`. `FIREHOSE_BRANCH_MB`
sets every node's `core.branch.max_bytes` (32 MiB), of which a push holds half for one stream.

**Invariants** in `check.py`:

| Invariant | Holds |
|---|---|
| `firehose-gap` | no `gap` line: a `Live` follower skips no record of its generation short of a reset, fault or none |
| `stream-throughput` | in a quiet window under `geo`, each `firehose` follower receives in every 10 s at least 90 % of the bytes the origin published in those 10 s, and its lag stays under 2 s |
| `stream-window` | in every `window` line, each downstream's `inflight_bytes` is at most the largest `window_bytes` it had in the last 10 s plus one `max_push_bytes` (a window shrinks under bytes already sent), and that window at most `max_push_window`; `inflight_pushes` at most 64; the node's `stream.push_inflight_bytes` at most `max_push_inflight` plus one `max_push_bytes` per channel; and `resent` moves only within `B` of a fault on either node of the pair |

Plants: `@plant(want=["stream-throughput"])` on `p_stream_throughput`, a synthetic run whose follower falls to 60 %
in a quiet window; `firehose-gap`, a synthetic run whose b2 skips 300 records after a `Ready`; `@plant(want=["stream-window"])` on `p_stream_window`, a `window` line with `inflight_bytes`
above `window_bytes`. Real-binary plants: `examples/rig/plants/window-floor.sh` holds every window at the floor
(2 MiB per round trip is 11 MB/s at 190 ms, under 13), and a `geo-quiet` run fails `stream-throughput`;
`examples/rig/plants/no-window.sh` makes `next_push` ignore the window, as `PushWindow-NoWindow` does (§7), and the
same run fails `stream-window`.

## 15. What it does not do

- It makes one stream faster only up to half its branch per round trip: 16 MiB, 77 MB/s at 219 ms at the defaults
  (§2.5).
- It does not set kernel buffers: [wan.md](wan.md).
- It does not add sessions or lanes: [lanes.md](lanes.md).
- It is not congestion control. TCP's (cubic on build-1) still runs under it. The window only bounds what the
  application has in flight, so the parent's memory and the resend size stay bounded.
- It does not keep channel state across a restart. A restart makes a new channel from the next interest.
- It does not change interests, their timing or their bytes.

## 16. Risks

1. **A late push of a boot the child does not know.** The child keeps 4 retired boots per parent node. A push from a
   boot it never followed (two restarts of the parent, seen newest first) or a 5th older one is followed as new, and
   the live boot's pushes are then answered `Retired`. The parent drops the `Downstream`; the child's next interest is
   answered out of sync, and the child forgets the parent's channel state then (§2.1), so the next channel is followed.
   Each such late push costs a re-create, not a lost record (the position check stays). Test:
   `core::a_late_push_of_a_boot_never_followed_costs_the_live_boot_one_new_channel`.
2. **Call slots.** Pushes take up to 1024 of the cluster's 4096 `max_pending` slots. A node that relays to many
   children and makes many other calls shares them. The quarter is a constant (§2.4 says why): on a cluster with fewer
   slots a push refused `Busy` goes again a tick later, so the cost is throughput, not records. If it proves wrong,
   `Cluster::max_pending()` lets `StreamNode::start` scale it.
3. **Parent memory.** Up to 256 MiB of encoded pushes per node by default, outside the hub's `max_queued_bytes`
   (256 MiB, `crates/axon-stream/src/hub.rs:88`). A node sized for the hub limit alone can use twice that.
4. **Resend bursts.** A go-back sends a whole window again, up to 64 MiB on one channel, at once, and several pushes
   of one stream among them: up to half its branch, 16 MiB at the defaults. The child answers what it already applied
   `Skipped`, so nothing is applied twice, but the bytes cross again. The window starts from the floor after it; the
   resend itself is not paced.
5. **A rate sample on a quiet channel.** A channel that had little to send has a low rate and a small window. When a
   backlog arrives it doubles per round trip: about 1.1 s at 219 ms to reach 64 MiB.

## Left out

| Item | Why |
|---|---|
| Selective repeat | one session is one TCP connection: what it would save, go-back-N resends anyway (§2.5) |
| One push per stream in flight | it held one stream to `max_push_bytes` per round trip, 4.8 MB/s at 219 ms; a stream's charge in its branch bounds a slow reader without it (§2.5) |
| A window setting per stream or namespace | a stream's branch already bounds it; one ceiling per node is enough |
| An outbox or a lower window ceiling of their own for Live pushes | a heavy namespace goes on Bulk (§9) |
| A floor setting | the floor is `2 × max_push_bytes`; a setting would only let it go below stop-and-wait's pair |
| Acknowledgement frames of their own | the call's answer is the acknowledgement |
| An RTT probe of its own | the session's keepalives already measure it; one early keepalive fills the gap |
| Pacing | TCP paces under it; a paced resend is risk 4 if it shows in the bench |
| A lane byte in the push header | the endpoint names the lane |
| Ordering boots by incarnation | a clock step reverses it; a set of retired boots does not care |
| Channel state on disk | a restart is a new boot; the next interest re-creates the channel from the child's position |
