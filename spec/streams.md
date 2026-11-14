# Design — stream continuity

Status: §3, §1 and §2 built. Three gaps in what a stream promises a subscriber that holds a cursor: replay stops at
the in-memory ring (§1), a moved origin starts a new generation and ends every cursor (§2), and a `Live` follower has
no position of its own, so a relay that forgets its position moves the follower past records or back over them (§3).
The order to build them is §3, §1, §2: §3 guards the others, and §2 rests on both. All ship in wire v8
([rolling-upgrades.md](rolling-upgrades.md)), which none of them needs: no stream frame changes here.

Words used here:

- A *position* is `(generation, sequence)`, numbered by the origin and kept on every hop (guide
  [streams § Model](../crates/axon/guide/streams.md#model)). Within a generation a subscriber gets every record once,
  in order, or an explicit reset.
- A *history source* is a `RecoverySource` (`crates/axon-stream/src/recovery.rs:319-331`) that the application
  supplies for a namespace: its own log, a table, files.
- A *follower* is a subscription that survives closes and reopens of its key (`StreamHub::follow`, `hub.rs:1243`). On a
  `StreamNode` every `Live` subscription is one (`core.rs:1165-1172`).

## 1. History beyond the ring

**Problem.** `RelayCore` hands the hub its ring and nothing else: `store: Arc<Mutex<MemoryStore>>` (`core.rs:664`) is
the source of every local subscription (`core.rs:1175,1180`) and every child's branch (`core.rs:1435`). The hub takes
any `RecoverySource` (`hub.rs:1230-1237`), and ST-6 and the guide say "or whatever `RecoverySource` the application
supplies", but no `StreamNodeConfig` field reaches it. Disk history serves only the restart: a start behind the ring is
`HistoryUnavailable` with or without it (guide [streams § History on disk](../crates/axon/guide/streams.md#history-on-disk)).

**Design.**

- `StreamNodeConfig::history(namespace, Arc<dyn RecoverySource>)`. It is read only for keys this node originates
  (`originated`, `core.rs:667`). Relays keep their rings and pass a start behind them on as today.
- For such a key the core gives the hub a `History` source: the ring when it holds `after + 1`, otherwise the
  namespace's source for the whole span up to the cut. The two never meet inside one read: a replay that started on
  the source moves to the ring once the ring holds its next record. The contract on the
  application: the source holds every record the stream published, at the same position, with the same bytes. An
  application that writes its log before it publishes meets it.
- One generation for good. An application with its own sequence opens the stream with that sequence as the stream's
  (§2, `open_stream_after`), so a cursor it stored is a position in its own log, and a subscriber that comes back after
  a day resumes `After(cursor)`.
- `RecoveryError::Pending`. A source that must do I/O starts the read and returns `Pending`. The core holds its lock
  while it drives recovery (`core.rs:1193,1566`), so a source never blocks. The hub keeps the subscription recovering,
  as it does for `LimitExceeded` (`hub.rs:1492-1498`), and drives it again on `StreamNode::history_ready(&key)`, which
  the source calls when the read is in. Nothing in the core does I/O, and the trait stays one trait (ST-6).
- `DiskStore` is a history source from day one. For a namespace on `disk`, a start behind the ring reads the segments
  that hold it, in the node's writer task, and answers `Pending` until they are in. Retention bounds it: a start older
  than the oldest segment is `HistoryUnavailable`. `DiskConfig::max_read_segments` (8) bounds the segments read in
  that stay in memory and each read; more starts at once take turns, in the order they asked. A read brings in only
  what the segments no start has read from yet, and those being read, leave room for: none is read in to be evicted
  unread, and the start that asked first keeps its segment until it reads it. Past the limit the segment read from
  longest ago goes. Replay beyond the ring is then axon's own, and the World tests it.
- A start behind every relay's ring. A relay refuses it `HistoryUnavailable` (`core.rs:1435-1437`); the child asks its
  other candidates and waits for one that holds it while any is still appearing (`reconcile`, `core.rs:2176-2190`). The
  origin must be among the child's candidates (`upstream_for`): the guide says so, and a test holds it. A relay that
  forwards such a start upstream is left open (§ Open).
- A replay from a source goes through the push window like any branch (`core.rs:1561-1566`): one read is at most what
  the window has room for.

**Wire.** None. `Pending` never leaves the node: a branch that waits on it sends nothing.

**API** (the API page in `crates/axon/README.md`): `StreamNodeConfig::history`, `StreamNode::history_ready`,
`RecoveryError::Pending`; on a bare hub, `StreamHub::history_ready` and `StreamHub::reads_history`.

**Tests.**

- `crates/axon-stream/tests/hub.rs::a_source_that_answers_pending_keeps_the_subscription_recovering_until_woken`
- `crates/axon-stream/tests/hub.rs::a_replay_from_the_source_runs_to_the_cut_and_each_live_record_follows_once`
- `crates/axon-stream/tests/world_relay.rs::a_subscriber_behind_every_ring_replays_from_the_origins_history_then_goes_live`:
  rings of 4 records, a leaf `After` 1000 records back, the source an in-memory log. Seeded variants: the source
  answers `Pending` for 0 to 500 ms; a relay crashes during the replay. Fails today with `HistoryUnavailable`.
- `crates/axon-stream/tests/world_disk.rs::a_subscriber_behind_the_ring_replays_from_the_files`
- `crates/axon-stream/src/node/tests.rs::more_replays_from_the_disk_than_it_keeps_segments_for_take_turns`: five
  replays share two segments, read in once to three times between polls; the shelf holds at most two, and every row
  reads in as many segments. Before, the starts were read by segment name, and the last key waited for the others'
  whole replays; then a read past the limit evicted the first read's segments unread, and the first key got its first
  record in round 38; then reads twice a round brought in 168 segments against 90, evicted unread.
- `crates/axon-stream/src/core/tests.rs::a_source_replay_larger_than_the_branch_limit_goes_through_the_push_window_without_a_slow_consumer_reset`
- `crates/axon-stream/src/node/tests.rs::a_history_source_is_not_asked_for_a_key_the_node_relays`

## 2. Moving an origin

**Problem.** A stream's origin is whichever node opened it; a relay that opens a key it pulls becomes its origin
(`open_read`, `core.rs:870-910`). The guide asks the new origin for a higher generation (guide
[streams § Quick start](../crates/axon/guide/streams.md#quick-start)), because positions mean something only within
one numbering. A subscriber with a cursor then ends `ResetRequired(GenerationChanged)`, and at the old origin's close
`End(StreamClosed)` (`terminate_key`, `hub.rs:798-812`). An application whose origin moves on failover, a writer under
a lease with a standby that takes over, loses every stored cursor. `StreamHub::resume_stream` (`hub.rs:902-917`) can
open "generation g after position p", but it is `pub(crate)` and only disk resume uses it (`core.rs:942-944`).

**Design.**

- `StreamNode::open_stream_after(key, after: Cursor, policy)`, a future like `open_stream`. The key goes on in
  `after.generation` from `after.sequence + 1`, over `resume_stream`, with an empty ring and the namespace's history
  source behind it (§1). Without a source it is refused `InvalidOptions`: positions at or before `after` would be
  served by nobody. So is a namespace kept on disk (its files number the key from the disk's own last record), a
  `Compacted` policy (its state is not in the history source), and an `after` behind the position the node already
  relays in that generation (it would number those records again, against rule 2). An `after` in a generation older
  than the node relays, or than a follower on it holds, parked ones too, is refused `GenerationMismatch`, as is an
  `open_stream` of one: its followers would move back a generation.
- The rule the guide gives becomes: a new origin keeps the generation only when it gives every position the same
  record. The application holds it with two rules, which [tla/OriginMove.tla](tla/OriginMove.tla) checks:
  1. publish a record only once the node that may take over holds it (publish after commit);
  2. the new origin opens after the last record it holds, and never writes a position again.
- The old origin calls `close_stream` once it knows it lost the right to publish (its lease lapsed). Relays see a
  close, and repull from their own position (§3), so the same generation goes on from the new origin with no reset.
  A paused old origin cannot close: its children move once the cluster calls it gone and drops it from their
  candidates (`parent_lost`), as for a crash; `upstream_timeout` only orphans them in the meantime. At the WAN timings
  that is about 11.5 s after the pause. Until it closes it publishes nothing new, by rule 1.
- `StartPosition::FollowAfter(Cursor)`: strictly after the cursor, then the key is followed as a `Live` follower is.
  At a close it parks with its cursor and goes on at the next open of its generation; a new generation ends it
  `ResetRequired(GenerationChanged)`. A plain `After` keeps today's end at a close. `FollowAfter` is local to the
  subscriber's node: what goes upstream is the relay's `After(position)`, as now.
- Built in the hub as a follower that holds one generation (`Sub::one_generation`, checked in `moves_to`), admitted
  as an `After`. Three cases the sim found, and a fourth a review found:
  - a follower still replaying its start when the key closes loses its replay's source with the stream: it waits
    instead for the last position it was given, and the live batches held behind the replay are given back
    (`Sub::stop_replay`);
  - at a relay's repull every parked follower's position is a want (`StreamHub::waiting`), not only the best-effort
    `After` the relay closed at: a parent that cannot give it is then left for one that can, instead of the relay
    falling back to the live edge past the follower;
  - a follower told `Ready { start: None }` on a relay stream with no position is told its position before the first
    batch it gets (`Sub::unplaced`);
  - a follower on the standby itself is behind the open whenever the log is ahead of what reached the standby as a
    relay, which rule 1 allows and a crash of the old origin leaves: the parked follower, `Live` or a `FollowAfter`
    still replaying its start, replays the rest from the open's source and goes on live (`resume_stream` takes the
    source, `Sub::catch_up`), where it was reset `HistoryUnavailable`. Only a follower that may replay
    (`Access::Replay`), and only when the source holds every record between; otherwise the reset stands. The replay
    runs under the branch and hub limits, as a subscribe `After` it would.
- Topology the guide recommends: the standby relays the key from the origin (its tiers name the origin's label), so
  taking over is the tested "a relay opens a key it pulls" path (`world_relay.rs:278`), and no `UnknownStream` backoff
  (`core.rs:1859-1868`) delays its children.

**Model.** [tla/OriginMove.tla](tla/OriginMove.tla): two nodes that can be origin, a committed log written by one
writer at a time, one relay, a `Live` follower and a `FollowAfter(0)` subscriber on the relay's node. The writer moves
up to `MaxMoves` times (a lease lapsing, the old writer still running or not); a node crashes or pauses and comes back;
a ring gives up its oldest record. Invariants: no position twice (`NoRepeat`), none skipped (`NoGap`), each the record
the log holds there (`SameRecord`), no subscriber ended (`NoEnd`); under fairness every subscriber reaches the last
committed record (`CatchesUp`). `OriginMove.cfg` (3 records, 2 moves, 2 faults) passes: 685 441 distinct states,
depth 33, 14 s on 8 workers (40 s at `-workers 4` in `check.sh tla` on 2026-11-14, the seven mutations failing as
listed). `OriginMove-Wide.cfg` (3 moves, 3 faults) passes: 3 397 959, depth 37, 1 min 8 s; it is
not in `check.sh tla`. Each `mutations/OriginMove-*.cfg` fails within 3 s: `RepullForgets` (a relay that asks `First`
after a close takes the live edge past its followers), `NoSource` (an origin that serves only its ring) and `CloseEnds`
(a plain `After` at a close) break `NoEnd`; `TrustRelay` (a follower that takes its relay's position as its own, today's
hub) breaks `NoGap`; `PublishBeforeCommit` and `StaleOpen` (the two application rules) break `SameRecord`;
`NeverClose` (an old origin that stays open) breaks `CatchesUp`. Abstractions are listed in the model's header: the
relay's ring holds what it took, a read from the source is one step, and the standby is not itself a relay, so it
has no followers of its own behind the open (the fourth case above is tested in the World and the core instead).

**Wire.** None: positions, `Status` and the interest are unchanged.

**API.** `StreamNode::open_stream_after`, `StartPosition::FollowAfter`.

**Tests.**

- `crates/axon-stream/tests/world_relay.rs::the_origin_moves_to_its_standby_and_every_cursor_goes_on`: the origin
  crashes or pauses (seeded), the standby opens after the last record it holds, the old origin closes later or never
  comes back. The oracle of `tests/world.rs:159` (`check`), held to one generation. It failed before: there was no
  `open_stream_after`, and the moved generation had no history behind it (run against stubs that open the next
  generation: the leaf read "1 after 200"). On odd seeds the standby opens before the records reach it as a relay,
  with a `FollowAfter(0)` follower that came after them; those seeds failed before the fourth case was fixed.
- `crates/axon-stream/src/core/tests.rs::a_follower_on_the_standby_behind_the_open_replays_the_rest_from_the_history`
  and `::a_follower_on_the_standby_behind_the_open_that_may_not_replay_is_reset`
- `crates/axon-stream/src/node/tests.rs::a_follower_behind_an_open_from_disk_replays_the_rest_from_the_disk`: the same
  for an open from the disk of a key the node relayed after its restart, with a ring of 4. The disk's source knew the
  key only once the open had taken it, after the hub heard of the open, so the follower was reset
  `HistoryUnavailable`; the disk now shelves every key it holds at its boot.
- `crates/axon-stream/src/core/tests.rs::a_follower_whose_catch_up_stops_at_a_new_generation_follows_it_from_its_first_record`:
  a replay a close or a reset stops leaves a `Live` follower at the last position it was given, as any `Live`
  follower, and a `FollowAfter` waiting for it. Before, a `Live` one waited too: a restart ended it
  `GenerationChanged`, and a close and an open of the next generation dropped that generation's first records.
- `crates/axon-stream/src/core/tests.rs::a_follower_whose_catch_up_stops_is_told_it_caught_up_once_where_it_goes_on`:
  a `Live` follower gets one `CaughtUp` for each catch-up, at the end of the replay or at the last position it was
  given when the replay stops (`Sub::stop_replay`). Before, one whose replay stopped got none.
- `crates/axon-stream/src/core/tests.rs::origin_lost_told_to_a_follower_still_replaying_comes_after_the_records_it_holds`:
  `OriginLost` waits behind the live records held behind a replay. Before, they came ahead of the replay.
- `crates/axon-stream/src/core/tests.rs::a_subscriber_in_the_new_generation_goes_on_beside_a_follower_parked_in_the_old`:
  a parent's `GenerationMismatch` to the position a relay closed at moves it on to `First` (§3), on the parent it
  follows too. Before, the relay reset the stream `GenerationChanged`, ending a subscriber of the new generation.
- `crates/axon-stream/tests/world_wan.rs::an_origin_moved_to_another_region_keeps_every_cursor`: the same across three
  regions at the WAN timings. Every follower recovers within 15 s of the fault (the cluster calls a silent origin gone
  about 11.5 s after it), and within `upstream_timeout` plus one round trip of an old origin's close.
- `crates/axon-stream/tests/hub.rs::a_follow_after_subscription_parks_at_a_close_and_goes_on_at_a_reopen_of_its_generation`
- `crates/axon-stream/tests/hub.rs::a_follow_after_subscription_ends_generation_changed_at_a_new_generation`
- `crates/axon-stream/src/node/tests.rs::open_stream_after_without_a_history_source_is_refused`
- `crates/axon-stream/src/core/tests.rs::an_open_of_a_generation_older_than_the_node_holds_is_refused`: while the
  relay holds a position in the newer generation, and after its parent closed the key, where only its follower holds
  one. Before, the open took the key and the follower back a generation.
- `crates/axon-stream/src/core/tests.rs::a_parked_follower_in_a_newer_generation_refuses_an_open_of_an_older_one`:
  a node that closed a key it originated holds the generation of the follower parked at the close.
- `crates/axon-stream/src/core/tests.rs::an_open_of_a_key_the_node_originates_is_already_exists_before_its_generation_is_checked`:
  an open of a key the node originates is `AlreadyExists` in an older generation too, not `GenerationMismatch`.
- The rig: `move/m0` and `move/m1` in `examples/rig/src/streams.rs` (`Mover`). The owner of the eager key `mov`, which
  only b1 and b2 serve, opens both with `open_stream_after` after the record due last, or after the position its node
  relays where that is later, and closes them when it stops (drained or fenced). A record is a function of its position
  (`Due`, also the namespace's history source), so both origins publish the same bytes at a position (rule 1), and the
  new origin holds every record before the one it opens after (rule 2). Every node follows both, `m0` `Live` and `m1`
  `FollowAfter` its cursor; b3..b5 pull through b2 then b1, b1 and b2 through each other. `check.py` judges them by
  `stream-dup`, `stream-reorder`, `stream-gap`, `stream-liveness` while b1 or b2 is up, and `stream-reset-kind`, which
  on a `move/` stream also fails `GenerationChanged` and `StreamClosed`: every cursor goes on. Its selftest plants a
  `GenerationChanged` end and a silence on a move stream, and with those two rules taken out it misses both.
  Run on one box ([fault-rig.md § Layout](fault-rig.md#layout)):
  `ONLY=kill,pause,leave,freeze smoke 71 10` moved the origin 16 times (14 closes, 9 drained and 5 fenced) and
  `ONLY=kill,pause,leave,freeze,isolate,partition smoke 72 10` 23 times (20 closes, 11 drained and 9 fenced, and 38
  `OriginLost` on the move streams). In both every stream invariant held and no follower of a move stream was reset;
  72 passed, 71 failed only `latency-quiet` and `peer-leak`, in one window where every node stalled at once.
- TLC: `OriginMove.cfg` and every `mutations/OriginMove-*.cfg` in `scripts/check.sh tla`.

## 3. Follower positions

**Problem.** The hub trusts the position its relay stream adopts, and a follower carries none of its own. Its only
guard is `Sub::lost` (`hub.rs:466`), set only by `reset_relay_stream(HistoryUnavailable)` (`hub.rs:1047-1066`) and read
by `would_skip` (`hub.rs:511-513`). A close, a `GenerationChanged` reset or a repull leaves the relay stream with no
position and `lost = None`; `repull` also asks the parent for record 1 (`fresh`, `core.rs:1062-1075`). The next position
then comes from `adopt_relay_stream`, which queues `Ready { start }` for every follower (`hub.rs:977-1001`), or from
`accept_batch`, which queues none (`hub.rs:1206-1208`). The rig shows all three outcomes
([fault-rig.md § Findings fixed](fault-rig.md#findings-fixed)):

| Seen | Rig invariant | Reproduce |
|---|---|---|
| a follower is sent its generation again from record 1 after the origin resumes it from disk | `stream-dup` | `SMOKE_MIX=0 scripts/rig.sh smoke 51`, episode 1 |
| a follower skips the last durable record when the origin resumes from disk | `stream-gap` | `scripts/rig.sh chaos 62 5`, episode 4 |
| a follower skips records after `OriginLost`, with no `Ready` or reset | `stream-gap` | `scripts/rig.sh chaos 52 8` |

The World never reached it: `world_disk.rs` keeps the origin down for less than `upstream_timeout`
(`world_disk.rs:159-224`), so nothing is orphaned, and its ring holds the whole run. Which core event drops a relay's
position first in each seed is not pinned by reading; the fix below does not depend on it.

**Design.** Fix the class at the last hop, then stop the relay from forgetting.

- Hub: `Sub::lost` becomes `Sub::at: Option<Cursor>`, the follower's last delivered position, set at each delivered
  batch and each positioned `Ready`. Against it:
  - a batch at or below `at` in its generation is trimmed (`batch.after`, as `accept_batch` trims for the stream);
  - a `Ready` behind `at` in its generation is not queued;
  - a position past `at + 1` in its generation ends the follower `ResetRequired(HistoryUnavailable)`, the reset the
    guide promises;
  - a new generation not announced by a `Ready` gets one.
  This deletes `lost`, `would_skip`'s mark and the branch that keeps it across resets (`hub.rs:1056-1064`).
  `FollowAfter` (§2) starts with `at = Some(cursor)`.
- Core: `repull` keeps the stream's last position and asks `After(position)` first; on `GenerationMismatch` it falls
  back to `First`, then to what is wanted: `fresh` and the position become one `Feed::first`, the start asked on
  top of `wanted_from`, which each such refusal moves on. A reopen of the same generation, from disk or after a
  move, then goes on with no reset.
- The class, not the instance: the `stream_core` fuzz oracle (`fuzz/fuzz_targets/stream_core.rs`) checks that each
  follower's delivered positions are contiguous within a generation and that a generation change follows a `Ready`.

**Wire.** None. **API.** None.

**Tests.**

- `crates/axon-stream/tests/hub.rs::a_follower_never_sees_a_position_twice_or_moves_past_one_whatever_its_relay_adopts`,
  table-driven: follow, take 1..5, then (a) close, reopen, adopt `(g, 0)`, take 1..8: only 6..8 are delivered; (b) reset
  `GenerationChanged`, adopt `(g, 8)`: `ResetRequired(HistoryUnavailable)`; (c) reset `GenerationChanged`, a batch at 9
  with no adopt: the same reset. Each failed before the fix.
- `crates/axon-stream/src/core/tests.rs::a_relay_asks_again_after_the_position_its_stream_closed_at`: a relay
  follows, its parent closes the key, and the relay asks again `After` the position it closed at, not `First`. It
  failed before the fix.
- `crates/axon-stream/tests/sim.rs`: "the origin reopens the same generation" and "a relay sees a close" join the fault
  mix, judged by the oracle of `tests/world.rs:159`. They landed with §2: without a disk an origin that opens the same
  generation again numbers it from record 1, and with one `close_stream` removes the files, so the only reopen of a
  generation that goes on is an `open_stream_after`. The sim closes the origin's keys and reopens them after the last
  record, and late leaves start `FollowAfter` record 0 (`First` in the runs where it never closes them).
- `crates/axon-stream/tests/world_disk.rs::a_follower_two_relays_away_sees_each_record_once_when_the_origin_resumes_after_origin_lost`:
  the origin down 3.5 to 8 s, two relays. No seed of 0..512 fails before the fix either: the relays keep their
  positions through `OriginLost`, and nothing closes their streams. It stays as the guard of that path. A ring smaller
  than the run is left out: a child refused at the backoff while its parent was orphaned came back more than 64
  records behind and was rightly reset (`ResetRequired(HistoryUnavailable)`, seed 171). A close and a reopen of the
  same generation at the origin is not the case either: `close_stream` removes the files, so the reopen numbers from
  record 1 again, the reuse the guide forbids. A relay sees a close of a generation that goes on only when the origin
  moves, so that case is §2's World tests and the sim above.
- The three rows move from Open findings to Findings fixed in [fault-rig.md](fault-rig.md), citing these tests.

## 4. Elsewhere

- ST-6 in [00-overview-and-requirements.md](00-overview-and-requirements.md) gains "a history source per namespace,
  `Pending` for one that reads".
- [simulation.md § TLA](simulation.md#tla) gains an `OriginMove` paragraph when the model lands with the code.
- The guide: [streams § Model](../crates/axon/guide/streams.md#model), § History on disk, § Many origins and § Standby,
  and a new § "History the application keeps".
- [faults.md](faults.md) streams rows (recovery and cursors, disk store) cite the World tests above under crash, pause
  and latency, once they exist (`scripts/check.sh faults` refuses a row whose test is missing).

## Open

- A relay that forwards a start behind its ring to its own parent, so a child need not list the origin among its
  candidates. Today's answer is the child's tiers (§1). Add it if a deep tree needs it.
- `StartPosition::FollowAfter` against a `follow: bool` in `SubscribeOptions`. The variant keeps `SubscribeOptions`
  and the hub's `follow` check (`hub.rs:1243-1248`) as they are; a flag would let `First` follow too. Default: the
  variant.
