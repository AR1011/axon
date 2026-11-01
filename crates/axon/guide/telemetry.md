# Telemetry

`Telemetry::start(&system, &cluster, config)` attaches an extension that samples the node every
`sample_interval` (1 s) on a blocking thread and serves these requests, among others, on the control service
`axon.telemetry` (JSON, access `Deny` unless configured):

| Request | Reply |
|---|---|
| `Snapshot` | `NodeSnapshot`, serialized once per tick and served from cache |
| `Actors(ActorQuery)` | a page of actors; type, phase and search filters run at the source; `timings: true` adds per-actor histograms |
| `Connection { peer, outgoing, lane }` | one session's `{ timings, request_rtt }`, read when asked; `lane` is `"control"`, `"live"` or `"bulk"`; null when the node has no such session, or the lane is down |
| `Samples { after }` | `SamplePage`: batches after the cursor, assembled from cached bytes. `gap` = batches were evicted or dropped, `reset` = the cursor was from another incarnation. A page starts with `SAMPLES_SCHEMA`; a collector refuses a page of another schema |

Counters and histograms are cumulative per node incarnation; `connection.*` series restart with
each session. Histograms are one sample: `BUCKETS` (304) log-linear buckets of nanoseconds, eight to each power of
two, exact below 16 ns and with the last one open above about 1030 s (`Histogram::upper_bound_ns`). A histogram travels
sparse, in postcard and JSON alike: `first` and the `counts` of buckets `first..` up to the last nonzero one, and
`Value::Histogram` holds it so in memory too, as a `HistogramSnapshot`. Missing is not zero.
Telemetry omits reason-labelled counters and empty histograms until they have a value.
`connection.*` series are stored for the `sample_connections` (32) connections that moved the most bytes since the
last sample, so their number does not grow with the cluster; `cluster.connections` counts them all. A connection is
one lane of one peer, so a peer has up to three; a lane that is down is a row of the snapshot (`down_ms`) but has no
series.

A batch carries a series when its value changed, and again `rewrite_unchanged` (30 s) after it was last carried, so a
live series is stored at least once a minute (`HELD_MS`) however still it stands. The wait is timed on the batches'
own stamps, which stores judge gaps on: two batches that carry an unchanged series are stamped at least
`rewrite_unchanged` apart, whatever the monotonic clock says, and a wall clock stepped back makes every series due at
once, so a step either way does not make a live series look ended.
`rewrite_unchanged: Duration::ZERO` carries every series in every batch, and so does the next batch after one is dropped
or a reader is answered with `gap`. `telemetry.samples_skipped` counts the samples left out and is in every batch, so a
store knows how far each node has reached even when nothing else moved.

## Persisted samples

| Name | Kind | Labels |
|---|---|---|
| `runtime.actors` | gauge | |
| `runtime.actors_started` · `_stopped` · `_failed` (its own fault: an actor stopped by a link counts in `_stopped` only) · `runtime.dead_letters` | counter | |
| `runtime.worker.turns` · `.parks` · `.busy` | counter | `worker`, and `pool` on a named pool's |
| `runtime.worker.queued` · `.utilisation` | gauge | `worker`, and `pool` on a named pool's |
| `actor.count` · `.mailbox_depth` · `.mailbox_depth_max` · `.mailbox_bytes` · `.waiters` · `.requests` · `.oldest_message_age` | gauge | `actor_type` |
| `actor.admitted` · `.processed` · `.turns` · `.budget_overruns` · `.restarts` · `.asks` · `.ask_timeouts` | counter | `actor_type` |
| `actor.rejected` | counter | `actor_type`, `reason` |
| `actor.mailbox_wait` · `.handler` · `.turn` · `.timer_lag` | histogram | `actor_type` |
| `actor.message.handler` | histogram | `actor_type`, `message` (from `Actor::label`, 16 labels then `other`) |
| the `actor.*` depth, bytes, admitted, processed, restarts, rejected and histograms | | `actor`, only names in `persist_actors` |
| `cluster.members` · `.unreachable` · `.view_stale` · `.isolated` · `.connections` · `.pending_requests` · `.endpoints` · `.spawned` (starts other nodes asked for, draining included) | gauge | |
| `cluster.calls_started` · `.inbound_calls` · `.inbound_expired` (requests dropped unrun, their caller's deadline past when they came in or while they waited for a restart) · `.connects` · `.dial_failures` · `.discovery_errors` · `.heartbeat_failures` · `.route_hits` · `.route_misses` · `.spawn_requests` · `.spawn_refused` · `.handoffs` · `.handoffs_failed` · `.keyed_failed` · `.leases_lost` (one per key) · `.isolations` | counter | |
| `cluster.keyed` (keyed actors here now, `HostedKind::count`) | gauge | `kind` |
| `cluster.keyed_started` | counter | `kind`, `how` = call (a call asked this node: `get_or_spawn`, an eager restart, a move back), handoff (taken on from a node that moved the key or left) |
| `cluster.keyed_ended` | counter | `kind`, `reason` = stopped (by itself, idle, drained, a shutdown), failed (its own failure, as `keyed_failed`), lease_lost, isolated (stopped while no peer vouched for this node), moved (handed to its new owner on a rebalance or a leave) |
| `cluster.calls` | counter | `outcome` = succeeded, rejected, timed_out (sent, no answer in time), unknown, failed (a deadline before the send counts here), abandoned (the caller dropped the call first, as a `tokio::time::timeout` does) |
| `cluster.inbound_rejected` | counter | `reason` |
| `cluster.membership_changes` | counter | `kind` = joined, left, replaced, unreachable |
| `cluster.request_rtt` | histogram | |
| `quota.entries` · `.permits` · `.keys` · `.rows` · `.pulling` (after `Quotas::start`: entries this node used lately, permits held on it, keys and rows it replicates, 1 while its replica pulls) | gauge | |
| `quota.fallback` (units admitted with no fresh grant of a majority) · `.mismatched` (reports naming another maximum for a key) · `.reports` · `.report_failures` | counter | |
| `quota.denied` | counter | `reason` = limit, share |
| `stream.streams` · `.originated` · `.feeds` · `.orphaned` · `.sessions` · `.subscriptions` · `.recovering` · `.demand_keys` · `.queued_bytes` · `.queued_records` · `.retained` (bytes of history and compacted state held) · `.push_inflight_bytes` (bytes in pushes to children not answered yet) | gauge | after `Telemetry::watch_streams`; per node, never per stream key |
| `stream.published_batches` · `.published_records` · `.published_bytes` · `.duplicate_records` · `.publish_rejected` · `.delivered_records` · `.delivered_bytes` · `.replayed_records` · `.recovery_blocked` · `.coalesced` · `.parents_lost` · `.switches` · `.local_resets` · `.downstream_expired` · `.unwanted_items` · `.push_resent` (pushes sent again after a failed call or a gap) · `.push_capped` (times a child waited for the node's cap on pushes in flight) | counter | |
| `stream.resets` | counter | `reason` = generation_changed, slow_consumer, history_unavailable, gap, invalid_recovery, global_capacity |
| `stream.ends` | counter | `reason` = unsubscribed, session_replaced, session_expired, revoked, slow_consumer, stream_closed, upstream_lost |
| `stream.quota_refused` | counter | `reason` = the namespace limit a subscribe, publish or open met: subscriptions, ring_bytes, records, bytes, open_streams ([streams § Namespace limits](streams.md#namespace-limits)) |
| `stream.refusals` | counter | `reason` = a parent's answer: forbidden, unknown_stream, history_unavailable, generation_mismatch, unavailable, limit_exceeded, loop |
| `stream.interests` | counter | `outcome` = sent, failed, resync (a parent was out of sync and got the whole set again) |
| `stream.interest_keys`, streams named in the interests sent | counter | |
| `stream.pushes` · `.push_bytes` | counter | `direction` = in, out |
| `stream.push_skipped`, pushes this node, as a child, did not apply | counter | `reason` = duplicate, gap, retired |
| `stream.disk_streams` · `.disk_bytes` · `.disk_unwritten` (bytes admitted and not synced yet) · `.disk_failing` (1 from a failed write until one succeeds, publishes refused meanwhile) | gauge | with a `StreamNodeConfig::disk` ([streams § History on disk](streams.md#history-on-disk)) |
| `stream.disk_writes` | counter | `outcome` = ok, failed |
| `stream.disk_refused` (publishes refused with a `DiskError`) · `.disk_truncated` (segments found cut short or corrupt when read back, each cut at its last whole record) | counter | |
| `trace.spans`, spans this node recorded ([tracing](tracing.md)) | counter | |
| `telemetry.samples_skipped`, unchanged samples left out of a batch | counter | |
| `listener.accepted` · `.accept_errors` | counter | |
| `listener.rejected` | counter | `reason` = busy, per_ip |
| `listener.handshake_failures` | counter | `reason` = auth, negotiation, timeout, other, version (no wire version both ends speak) |
| `connection.sent_bytes` · `.received_bytes` · `.sent_payload_bytes` · `.received_payload_bytes` · `.sent_frames` · `.received_frames` · `.sent_compressed_frames` · `.received_compressed_frames` · `.write_batches` · `.calls` · `.tcp.retransmitted_segments` (Linux) · `.tcp.retransmitted_bytes` (macOS) | counter | `peer`, `direction`, `lane` (`control`, `live`, `bulk`) |
| `connection.paced`, nanoseconds the writer waited for its lane's `LaneConfig::max_bytes_per_sec` | counter | `peer`, `direction`, `lane` |
| `connection.rejected_frames`, sends refused locally; admitted frames are never dropped | counter | `peer`, `direction`, `lane`, `reason` (`full`, `closed`, `deadline`, `oversized`) |
| `connection.queued_bytes` · `.queued_frames` · `.inbound_queued_bytes` · `.inbound_queued_frames` · `.pending_requests` · `.rtt` · `.tcp.rtt` · `.tcp.cwnd` (kernel `TCP_INFO`, sampled at each keepalive) | gauge | `peer`, `direction`, `lane` |
| `connection.queue` · `.encode` · `.write` · `.decode` · `.rtt_distribution` · `.request_rtt` | histogram | `peer`, `direction`, `lane` |
| `process.rss_bytes` · `.virtual_bytes` · `.cpu_percent` | gauge | |
| `process.read_bytes` · `.written_bytes` | counter | |
| `host.total_memory_bytes` · `.available_memory_bytes` · `.cpu_percent` | gauge | |
| `host.received_bytes` · `.transmitted_bytes` | counter | |

`runtime.worker.busy` is the time a worker was not asleep, so it includes the short spin for new work
(`RuntimeConfig::spin`) a worker does before it sleeps. A worker that handles a message a millisecond shows a few percent
from the spin alone, and a pool that never sleeps (`spin: Duration::MAX`) shows all of it.

Compression ratio is `sent_payload_bytes / sent_bytes`. `process.*` and `host.*` need the
`system-metrics` feature; CPU is absent on the first sample.

## Sampling

| Knob | Default | Covers |
|---|---|---|
| `ActorSystemBuilder::timing_sample_every` / `MailboxConfig` | 0 (off) | actor histograms, `oldest_message_age` (a lower bound: the oldest *sampled* message) |
| `axon_net::Config::timing_sample_every` | 128 | session queue/encode/write/decode, `cluster.request_rtt` |

Keepalive RTT is recorded on every pong. Measured cost: [benchmarks](benchmarks.md#telemetry-overhead-axon-obs).

## Live only

`NodeSnapshot` carries the typed stats structs of the lower crates (`RuntimeStats`,
`ClusterStats` with per-peer `SessionStats` but not their timings or request round trips, which `Connection` serves),
membership, per-type aggregates, `hosted` (`Cluster::hosted`: the kinds registered, each with its keys here now and
those started and ended here, the first `snapshot_keys` keys here (none by default), at most a quarter of `max_frame` of
them, with their lease epochs, and the total), lifecycle phase, and `notices`, the coded
reasons for anything truncated, dropped or stale (`actor_scan_truncated`, `connections_truncated`,
`members_truncated`, `keys_truncated`, `sample_batch_dropped`, `membership_stale`). A snapshot keeps the busiest
`snapshot_connections` sessions; `connections_truncated`, `members_truncated` and `keys_truncated` name the
`TelemetryConfig` field that keeps more.

`Request::Keys{kind, prefix, from, limit}` pages a node's keys instead, a `KeyPage` of `{key, epoch}` and `more`: the
keys of one kind that start with `prefix`, in key order from `from` on (`from` included), at most `limit` (1024) and
as many as fit one reply. It reads a range of the node's map of keyed actors (`Cluster::hosted_keys`), so a page costs
the same at a million keys as at ten. A node from before it refuses it as `bad request: unknown variant`.

## Collection and history

`MetricsService` runs on a collector node. Every `collection_interval` (5 s) it pulls
`Samples{after}` from each reachable member (8 at a time, one deadline for the cycle) and writes
the page to every `MetricsSink` it was given (`with_providers(.., vec![clickhouse, s3], query, ..)`).
Each sink has its own cursor per node, and a cursor moves only after that sink stored the page: a
failed write is pulled again, and sinks standing at the same cursor share one pull. A sink whose
write fails or times out skips the rest of that cycle, so it falls behind alone
(`MetricsHealth.sink_errors`); a hung one costs the cycle one `write_timeout`. The collector takes
only batches that carry the identity of the node it asked, and passes over a batch no sink could
store (`rejected_batches`) so that it cannot block that node's cursor. A range the source evicted meanwhile comes
back as `gap`. The collector must be allowed by
every node's telemetry `access`, its own included. A node that does not answer is listed in
`MetricsHealth.node_errors` and does not hold up the others.
A sink that can say what it holds answers `MetricsSink::usage`, which `MetricsHealth.sink_usage` shows per sink and
Settings shows on the store's card: `held_bytes` against `max_bytes` (0 where the sink sets no bound of its own),
`held_batches`, the `evicted_batches` let go to stay under it, `free_bytes`, the room left under the bound or on the
database's disks, and per table its `rows`, `held_bytes` and `uncompressed_bytes` (their ratio is the compression),
`parts`, the `first_ms` and `last_ms` point it holds and its `retention_days` (TTL). The collector adds
`growth_bytes_per_day`, what the tables holding an hour or more but less than their TTL keeps gained per day of the
span they hold, and `full_in_ms`, `free_bytes` at that rate: `None` when retention levels every table off within the
room left. `MemoryMetrics` reports its one `samples` table, timed by when it stored each batch, and is full where it
starts to let the oldest go. `ClickHouseProvider` reads `system.tables`, `system.parts`, `system.disks` and
`system.storage_policies` at most once a minute, and no console request waits on it.

Writes are idempotent on `(node_id, incarnation, sequence)` for batches and `id` for events.
`MetricsService::with_providers(.., vec![], query, ..)` with `collection_interval: None` is a
query-only host over history another node wrote.

| Request on `axon.metrics` | Answer |
|---|---|
| `Capabilities` | `numeric_history`, `histograms`, `events`, `traces`, `max_points`, `storage` |
| `Health` | `MetricsHealth` |
| `Queries([HistoryQuery; ..32])` | one round trip for a page of charts, through `MetricsQuery::queries`: `MemoryMetrics` answers them in one pass over its samples, `ClickHouseProvider` in one SQL per shared range, step and filter (`metric IN`); a result or an error per query, in order |
| `Query(HistoryQuery)` | a `HistoryResult` in columns: `from` (`from_ms` down to a whole step), `step`, `count` steps through `to_ms`, then per series in `(node, incarnation, labels)` its header once and `values`, one per step, `None` where unknown: a gauge's value under `last` / `mean` / `min` / `max`, a counter's gain over the step, a histogram's gain as a `HistogramSnapshot`. A gain is unknown where the step before has no value or the series went back, as across a restart. `sum: Some(Sum::Node)` adds up each node boot's series per step, `Sum::Cluster` all of them, keeping apart the values of the labels in `by`; a sum has `summed`, how many series it added, adds the values known and ends at the last step every node still reporting reached. `settled`: the steps before it no longer change, but for a node more than `HELD_MS` behind the newest stored; `truncated` when `max_points` or `max_series` cut it |
| `Series(HistoryQuery)` | the series of the metric stored in the range, their headers without values, at most `max_series`, `truncated` past it |
| `Events(EventQuery{from_ms,to_ms,limit,node,kind})` | stored events plus those still waiting for storage, newest first; `node` and `kind`, when set, keep only that node's or that kind |

A step with no stored row holds the series' previous value, for less than `HELD_MS` after that row and never past the
newest batch stored from that node incarnation, so a still series draws a flat line and a node that stopped draws none.
`mean` is over the rows stored in a step, so a value that held still for most of it counts less than it lasted; use
`last` for what a series stood at.

A histogram value is what the cumulative histogram gained over its step, the last inside it against the last inside the
step before, whatever the aggregation; merge the steps of a window for its distribution. Percentiles are bucket upper
bounds, never below the true value and at most 12.5 % above it up to about 17 min (above that, the open last bucket),
and per-node percentiles must not be averaged into a cluster percentile: merge the histograms instead.

The collector stores its own health as `collector.cycles`, `.stored_batches`,
`.rejected_batches`, `.gaps`, `.source_resets`, `.write_errors`, `.dropped_events` (counters),
`.cycle_duration`, `.node_errors` and `.failing_sinks` (gauges).

`max_points` bounds the known values of the series read, in `(node, incarnation, labels)` order, before any sum: a
truncated answer is missing its last series, not thinned. Ask with a larger step or fewer series. In `Queries` each
answer is sized on its own, a sum after it is summed; one that does not fit becomes an error and the rest still
arrive. The answer's shape travels between a console and its collector: during a rolling upgrade, a console on a build
before the columns reads a collector on a build after them as failing, and the reverse, until both are on one build.

Events are the cluster's timeline, from two sources. The collector's membership subscription gives `joined`,
`left`, `leaving` (the node announced a drain; detail is its leave reason), `updated`, `restarted` (a node came back
under a new incarnation: discovery replaced its record, or the collector saw it join, or found it after a `resync`,
under another boot than the one it last saw; a console's live feed says the same), `unreachable`, `reachable`,
`discovery_unavailable`, `resync`. What discovery says (the first five) gets the id
`{node}:{kind}:v{version}`, with the kind discovery gave the change (a join seen as `restarted` keeps `joined`), where the version is the record's own in discovery (`left` uses the
last one this collector saw, so collectors that polled either side of a node's final update can
disagree about a `left`). Otherwise it is identical in every collector, so collectors sharing a store record a change
once, and a node that comes and goes keeps one event per occurrence however fast it flaps. Consul
supplies the larger of the service's and its checks' `ModifyIndex` (heartbeats and identical
re-registrations leave it alone; a health flip or a new registration moves it); `MemoryDiscovery`
counts registrations. Static seeds have no version and fall back to a fingerprint of the record
plus the minute. What one collector observed itself keeps `{collector}:{incarnation}:{n}`.

Each node also keeps its own: `ActorSystem::events()`, an `EventLog` of the newest `EVENTS` (256), shared by its
`Cluster` (`cluster.events()`) and its stream node. Recording takes a short lock and never waits, so no actor turn
does. The collector pages it with `Request::Events{incarnation, after, limit}` behind a cursor, as it does spans and
dead letters, and stores each once under the id `{node}:{incarnation}:e{seq}`. Events the ring let go of before the
collector asked are added to `dropped_events` once the rest of that page is stored; a node's new boot is read from
its first. A subject or a detail longer than 1024 bytes is cut when it is recorded.

| Kind | Subject | Detail | When |
|---|---|---|---|
| `acquired` | keyed actor `kind/key` | `epoch N` | this node started the keyed actor under a lease from its ownership authority |
| `lease_lost` | `kind/key` | `epoch N` | its lease lapsed or was taken, and the actor stops |
| `handed_off`, `handoff_failed` | `kind/key` | `to {node}, epoch N`, the epoch it held here (none without an ownership authority) | a leave or a placement change moved it (or could not) |
| `eager_restart` | `kind/key` | `on {node} for {gone}, epoch N` | this node restarted an eager key that `{gone}` held (this node's own id for a key it lost or held before a restart), or `a lapsed lease` nobody released; once per key and epoch in the cluster, on the node whose request started it (one that found it running records nothing) |
| `actor_restarted` | the supervised family's name, or its type | the failure | a family restarted; one event at once, then at most one a minute with the count since, detail prefixed `N times · ` |
| `gave_up` | the family's name, or its type | the last failure | a supervised family ran past its `RestartPolicy` and stopped for good |
| `parent_lost`, `reset`, `orphaned`, `parent_restored` | stream `namespace/name`, `… and N more` for several | the first one's reason; `pulling from {node}` for a restore | a relay lost its parent, reset a subscriber, has no parent left, or found one again after having none ([streams](streams.md)); at most one of each on a node's tick, however many streams it covers |
| `host_tcp_limits` | | each of the host's `tcp_wmem` and `tcp_rmem` maxima that is too small, and the value to set | the stream node started on a host whose TCP buffers hold less than `max_push_window` sent, or twice it received, so a far child gets less than its window each round trip ([wan.md](../../../spec/wan.md)); once a start |
| `disk_failing`, `disk_recovered` | | the write error; `a write succeeded` | the stream node's history writes started failing (publishes are refused), or one succeeded again |
| `lane_down`, `lane_up` | the peer | the lane, `Live` or `Bulk` | a Live or Bulk lane to the peer had no session while Control stayed up, or got one again; its calls fail with `Retry::Later` meanwhile ([cluster](cluster.md)) |
| `isolated`, `isolation_ended` | | why | discovery could not hear this node and no peer vouched for it, or it was heard again for a whole `heartbeat_grace`; neither while the node is only starting, not yet heard |
| `acceptor_warming`, `acceptor_warm` | | | the ownership authority started refusing grants until warm, or grants again |
| `seed_timings` | the seed entry | the seed's `SeedTimings` and this node's | a seed's `SYNCED` carried other timings than this node's, so the node does not count that seed ([cluster § Seed servers](cluster.md#seed-servers)); again only when the seed's timings change |
| `quota_mismatch` | the reporting node | | the first report this replica took that named another maximum for a limit than the one before; `QuotaStats::mismatched` counts the rest |
| `tls_rotated`, `authorizer_changed` | | `set` or `removed: …` for the authorizer | `Cluster::set_tls` succeeded, `Cluster::set_authorizer` was called |
| `login`, `login_failed`, `login_limited`, `logout` | | `{user} from {client}` | a console sign-in, failed sign-in, lockout or sign-out ([dashboard](dashboard.md#signing-in)) |
| `signed_out_everyone`, `console_tls_rotated` | | | `Dashboard::sign_out_everyone`, `Dashboard::set_tls` |

A keyed actor's subject is redacted with `DashboardConfig::redact` like its other names.

A node that answers history says so in its discovery metadata, `axon.metrics = <storage>`
(`Cluster::set_metadata`), so `history_sources(&cluster)` lists them from the local membership
view with no probing; a dashboard picks among them.

Several collectors may run. Each pulls everything independently (cursors live in the collector);
writing to the same store is harmless, writing to different stores is the point.

### Encoding

| Path | Encoding | Measured (178 samples, 4 nodes) |
|---|---|---|
| `Samples` reply, and the node's ring | postcard | 10.0 KB per batch; JSON was 21.5 KB |
| every control request and reply | zstd on request (`ClusterConfig::compress_control`, on by default, whatever the session codec) | a 7-batch page: 63 KB payload, 2.0 KB on the wire |
| `Snapshot`, `Actors`, everything on `axon.metrics` | JSON, because a browser reads it | |
| ClickHouse insert | `RowBinary`, zstd body | 46 KB as `JSONEachRow`, 30 KB as `RowBinary`, about 2 KB compressed |

A control reply is one frame: its cap is the session's `max_frame` (1 MiB by default, raise it in
the transport config) and `ClusterConfig::max_control_response` (16 MiB). A node serves
`max_control_handlers` (8) control requests at once; more wait up to `control_queue_wait` (1 s)
before they are refused.

### ClickHouse

`ClickHouseProvider::connect` creates `samples` and `events` (`ReplacingMergeTree`, TTL
`retention_days`) in `database` (`axon_metrics`); events (`id`, `time_ms`, `node_id`, `kind`, `subject`, `detail`) are keyed by `id` within a day partition, and an older `events` table gains `subject` on connect. One sample is one row; a histogram is one row
with `value` = count, `sum_ns` and `buckets Array(UInt64)`, all `BUCKETS` of them (a row from the older 32-bucket layout reads back without its histogram). `labels` is a canonical JSON string
and part of the sort key `(metric, node_id, incarnation, labels, kind, unit, timestamp_ms,
sequence)`, which is also what makes a retried insert harmless. Every value in a query is a typed
ClickHouse parameter; the database name is restricted to `[A-Za-z0-9_]`. Only `mean` reads with
`FINAL`. Queries run under `max_execution_time=2` and bounded rows, bytes and memory, so a long range at a
fine step is refused by those bounds, not answered slowly. Over
plain `http` the password travels in clear: use `https` off-host.

Rollups: materialized views keep `samples_1m` and `samples_1h` (`AggregatingMergeTree`: last value, last histogram,
min, max, sum and count per series and bucket). A query whose step is a whole number of hours reads the hour table,
whole minutes the minute table, anything else the raw samples; the dashboard asks for whole minutes from a
one-minute step up and whole hours from a one-hour step up. `samples_1m` lives as long as the raw samples, `samples_1h` for `rollup_retention_days` (365).
What differs from a raw answer: the first bucket may reach back before `from_ms` and the last one past `to_ms`,
each by less than one bucket, and a
rollup mean counts a sample twice if its insert was retried in a different grouping (an identical retry is dropped
by block deduplication; last, min and max are exact either way). Rollups start when the tables are created:
samples stored before that are not backfilled.

Live test: `AXON_CLICKHOUSE=http://127.0.0.1:8123 AXON_CLICKHOUSE_USER=.. AXON_CLICKHOUSE_PASSWORD=..
cargo test -p axon-obs --features clickhouse --test metrics`.

## Dead letters

A dead letter is a message a node accepted and never handled, with nobody told. A refused `try_tell`, `send` or
`request` is not one: the sender gets the refusal. Three things are:

- messages still queued when an actor exits, or when a supervised generation fails and is replaced
  (`Exit::discarded`, by `Actor::label`): `actor_stopped`, `actor_failed` (a stop by a link too),
  `actor_panicked`, `drain_deadline`, `drained` (still stashed when a drain ended), `shutdown`, `fenced`;
- a `tell` from another node that this node refuses: `unknown_actor`, `schema_mismatch`, `full`, `forbidden`,
  `invalid_message`, `stopped`;
- a topic's publish that a subscriber refuses ([actors § Topics](actors.md#topics)), labelled `publish` with the
  topic as its target: `full`, `too_large`, `not_ready` (a family between generations), `closed`.

Each system keeps the newest 1024 in `ActorSystem::dead_letters()`. The same letter again within a second of
its last sighting joins it: one row, the latest time, the summed count; a row stops taking repeats ten seconds after its first. Those windows run on the system's clock, not the wall's, so a wall step neither merges later letters into a row nor holds rows back from the collector; `at_us` is the wall time, for display. A letter names its actor or endpoint, the sending node, and the first trace among its messages.
`Request::DeadLetters{limit}` reads a node's ring. The collector pages `Request::SettledDeadLetters` (rows that can no longer
take a repeat) into `MetricsSink::write_dead_letters`, one row per `(node_id, at_us, seq)`; the memory provider and the
ClickHouse `dead_letters` table keep them, and `MetricsRequest::DeadLetters{limit}` reads them back. The persisted counts are
`runtime.dead_letters` for the first and third kinds and `cluster.inbound_rejected` for the second.
