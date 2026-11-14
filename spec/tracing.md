# Design — tracing in wire v8

Status: built, but for `axon_net::VERSIONS`, which moves to 8 with the rest of wire v8.

Five changes to trace context and spans (OB-9 in
[00-overview-and-requirements.md](00-overview-and-requirements.md); the user's view is the
[tracing guide](../crates/axon/guide/tracing.md)): a W3C-sized context (§1), spans an application records itself
(§2), a ring read in pages (§3), the error a span ended with (§4), and a count of spans lost before collection (§5).
Only §1 changes the wire, and it is one reason for wire v8 ([rolling-upgrades.md §6](rolling-upgrades.md#6-wire-v8)).

## 1. Context

**Problem.** `TraceContext { trace: NonZeroU64, span: u64 }`, 16 bytes on the wire (`crates/axon/src/trace.rs:95-157`,
`WIRE_LEN` at `:109`), ids from `random_u64` (`:32`). A W3C `traceparent` carries a 128-bit trace id, a 64-bit span id
and a flags byte, so a trace begun outside the cluster (a front end, a proxy) cannot be continued inside it, and an
exporter cannot hand axon's spans to a W3C collector without inventing the high half. Calls carry the 16 bytes first
in the body under `TRACED` (`crates/axon-cluster/src/wire.rs:45,117-145`, parsed at `engine.rs:1008-1009`, room at
`engine.rs:1736`); a stream batch carries `u64 trace, u64 span` as item kind 5 (`crates/axon-stream/src/wire.rs:241-249`,
decoded at `:312-319`), with `ITEM_FIXED = 45` exact for a traced batch (`:219`).

**Design.**

- `TraceContext { trace: TraceId, span: u64, flags: u8 }`, `WIRE_LEN = 25`: trace id 16 bytes, span id 8, flags 1,
  big-endian, in `traceparent` order. `root()` draws 128 bits and sets flags `0x01` (sampled); `child()` keeps the
  trace and the flags; `TraceContext::new(trace, span, flags)` takes one from outside, and `traceparent()` /
  `from_traceparent(&str)` convert the W3C text form.
- `TraceId` is `NonZeroU128` by default. The alternative, `[u64; 2]` with its own niche, keeps an
  `Option<TraceContext>` at 8-byte alignment: with `NonZeroU128` it is 16-byte aligned, so the context in each mailbox
  entry goes from 16 to 32 bytes. `perf hosted` and `perf traced` decide
  ([benchmarks](../crates/axon/guide/benchmarks.md)): `[u64; 2]` only if `NonZeroU128` regresses either beyond its
  noise. The public type is `TraceId` either way, with `get() -> u128`.
- `Span.trace`, `Span.parent`'s trace, `Tracer::trace(_)` and `DeadLetter.trace` (`crates/axon/src/dead.rs:35`) become
  `TraceId`; `tracing` events print `{:032x}`.
- Calls: `TRACED` with 25 bytes. Streams: item kind 5 with 25 bytes, `ITEM_FIXED` 54; `SequencedBatch::trace()`
  (`crates/axon-stream/src/recovery.rs:94,111-124`) returns `Option<TraceContext>`.
- axon-obs: ids are 32 hex digits (`hex`, `parse_id`, `crates/axon-obs/src/traces.rs:82-88`); `parse_id` also takes 16
  as a high half of 0, so traces stored before v8 still open. `MetricsQuery::trace` takes a `TraceId`
  (`provider.rs:134,951`). The ClickHouse ids are strings already: no DDL.

**Wire.** The call frame's traced prefix grows to 25 bytes; stream batch item kind 5 grows to 25 bytes and streams go
to `SCHEMA` 8 (`crates/axon-stream/src/node.rs:42`). Pinned bytes: `call_frame_bytes_are_pinned`
(`crates/axon-cluster/src/wire.rs:184`) and the traced batch in `crates/axon-stream/src/wire/tests.rs`. World digests
change, since a root draws twice; `check.sh sim` compares two runs of one build, so it still holds.

**Tests.**

- The pinned bytes above, and fuzz `stream_wire` and `frame_decode` on the new layouts.
- `crates/axon-cluster/tests/cluster/trace.rs::a_trace_crosses_nodes_and_untraced_calls_carry_nothing`: the 128-bit id
  and the flags read back on the far node.
- `crates/axon-cluster/tests/cluster/trace.rs::a_call_with_a_short_or_zero_trace_context_is_refused_unrun`, at 25 bytes.
- `crates/axon/tests/trace.rs::a_context_from_outside_keeps_its_id_and_flags_through_tells_timers_and_jobs`
- `crates/axon/tests/trace.rs::traceparent_text_round_trips_and_a_malformed_one_is_refused`
- `crates/axon-stream/tests/world_wan.rs::a_traced_publish_keeps_its_128_bit_trace_on_every_hop_across_regions`
- `crates/axon-obs/tests/metrics.rs::a_trace_stored_with_a_64_bit_id_still_opens`

## 2. App spans

**Problem.** `SpanKind` is `Handle`, `Call` or `Stream` (`trace.rs:189-197`): axon records its own work only. An
application that wants a span for a step inside a handler, with a few attributes, must keep a second tracer beside the
ring and lose the parent links.

**Design.**

- `SpanKind::App`, and `Span.attrs: Option<Arc<[(&'static str, Attr)]>>`, `enum Attr { U64(u64), I64(i64),
  Str(Cow<'static, str>) }`; `None` on axon's own spans. Not a `Vec`, as first designed: an empty `Vec` allocates
  nothing, but cloning a span that holds one cost 2.2 µs more a page of 256 (`perf trace_drain`,
  [benchmarks](../crates/axon/guide/benchmarks.md)); an `Option<Arc<_>>` that is `None` costs 0.2 µs. `AppSpan` keeps
  a `Vec` while it runs and shares it when it records.
- `Tracer::app(name) -> Option<AppSpan>`: `None` outside a trace. `AppSpan::attr(key, value)`, `error(kind)` (§4) and
  `enter(f)`, under which what `f` sends is its child; dropped, it records itself with its `busy` time.
- `SpanRecord.attrs: BTreeMap<String, String>`; ClickHouse `attrs Map(LowCardinality(String), String)` by
  `ALTER TABLE .. ADD COLUMN IF NOT EXISTS`, as columns were added before (`crates/axon-obs/src/clickhouse.rs:145`).
- Console: kind `"app"` beside `"handle" | "call" | "stream"` (`crates/axon-obs/ui/src/api.ts:529`), and the span
  detail lists the attributes.

**Wire.** None: spans stay on their node until collected.

**Tests.**

- `crates/axon/tests/trace.rs::an_app_span_records_its_attributes_and_parents_what_it_sends`
- `crates/axon/tests/trace.rs::an_app_span_outside_a_trace_is_none_and_records_nothing`
- `crates/axon-obs/tests/metrics.rs::an_app_spans_attributes_round_trip_through_the_store`
- `crates/axon-obs/ui/tests/dashboard.spec.ts`: an app span's attributes in the detail, with a `scripts/ctl.sh shot`
  of it.

## 3. Draining the ring

**Problem.** The ring is `Mutex<Ring { next, VecDeque<Span> }>` (`trace.rs:239-243`). `since(after, limit)` copies up
to `limit` spans under the lock (`:290-294`), and `trace(id)` scans the whole ring under it (`:296-298`).
`Request::Traces` asks `since(0, usize::MAX)` (`crates/axon-obs/src/telemetry.rs:353-354`): one hold that copies every
span the ring has, while every traced handler on the node waits to record.

**Design.**

- `since` copies at most `PAGE = 256` spans a hold. A caller that wants more takes the lock again from the last `seq`
  it got. `Request::Traces` and `Tracer::trace` read the ring through it.
- The writer is unchanged: one push under the lock.
- If `perf trace_drain` shows a hold of a page above 2 µs, the ring becomes segments of `Arc<[Span]>` that a reader
  clones under the lock and reads outside it. Not before. It showed 2.3 µs idle and 2.5 µs with 8 threads recording
  (Ryzen 7 5800X), so the ring is segments of 32 spans, `Arc<Vec<Span>>`, the newest one open. A writer pushes onto
  the open segment, copying it first if a reader still holds it, and a full one is sealed by swapping in an empty one.
  A hold is 80 ns idle and 220 ns with 8 threads recording. Sealing by copying the open spans into an `Arc<[Span]>`
  cost a traced message 23 ns; the swap costs nothing measurable. The oldest segment goes once the newer ones hold
  `capacity` spans, and readers skip the spans before the newest `capacity`, so the ring keeps exactly `capacity`
  and holds up to 2 segments more in memory.

**Wire.** None.

**Tests.**

- `crates/axon/src/trace.rs::reading_the_whole_ring_holds_its_lock_for_one_page_at_most`: a test-only count of the
  most spans copied in one hold is 256 after `Request::Traces` on a 65 536-span ring (65 536 today).
- A bench row `perf trace_drain` in `crates/axon/guide/benchmarks.md`: the hold for one page, recording on 8 threads.

## 4. Error kinds

**Problem.** A span knows only `failed: bool` (`trace.rs:215`), set from `result.is_err()` on calls
(`crates/axon-cluster/src/engine.rs:660`), from handlers, from dead letters (always) and from streams. A slow trace
with a failed call does not say whether it timed out, was refused as full or lost its session.

**Design.**

- `Span.error: Option<&'static str>` in place of `failed`. `CallError::kind(&self) -> &'static str` names it: `timeout`,
  `outcome_unknown`, `no_route`, `unreachable`, `stale_node`, `closed`, `busy`, `no_reply`, `codec`, `rejected.<name>`
  from `REJECTION_NAMES` (`engine.rs:713-714`) and `not_sent.<reason>` from `SUBMIT_REASONS`
  (`crates/axon-net/src/session.rs:90`). A handler's span gives `error` or `panic`, a dead letter its reason, a stream
  span its `StreamError`'s name.
- `SpanRecord` keeps `failed`, now derived (`error.is_some()`), so stored rows and the console's filter keep working,
  and gains `error`; ClickHouse `error LowCardinality(String)` by `ALTER`. The console's "The call failed" names the
  kind.

**Wire.** None.

**Tests.**

- `crates/axon-cluster/tests/cluster/trace.rs::a_failed_call_span_names_its_error_kind`: a timeout, `rejected.full`, and
  `outcome_unknown` after a session cut (the partition row of [faults.md § actors: tracing](faults.md#actors-tracing)).
- `crates/axon-obs/tests/metrics.rs::a_stored_span_keeps_failed_beside_its_error`

## 5. Dropped spans

**Problem.** A span the ring lets go before the collector reads it is lost without a count. Events have one
(`EventPage.missed`, `crates/axon-obs/src/model.rs:189`, summed into `MetricsHealth.dropped_events`,
`crates/axon-obs/src/metrics.rs:149`); spans have `recorded()` (`trace.rs:285`) and `seq`, but nothing reads them.

**Design.** `SpanPage.missed`, as `EventPage` has it; `MetricsHealth.dropped_spans`; the telemetry counter
`trace.spans_evicted`, rendered `axon_trace_spans_evicted_total`. An exporter that drains with `Tracer::since` counts
its own gap as `first.seq - after - 1`; the guide says so.

**Wire.** None.

**Tests.** `crates/axon-obs/tests/metrics.rs::spans_the_ring_let_go_before_the_collector_reached_them_are_counted`.

## 6. Elsewhere

- OB-9 links here. The [faults.md § actors: tracing](faults.md#actors-tracing) rows gain the 25-byte context and the
  error kind, once their tests exist.
- The guide: [tracing](../crates/axon/guide/tracing.md) § Context ("16 bytes", "16 hex digits" become 25 and 32),
  § Spans and § Collection, and [observability § Tracing](../crates/axon/guide/observability.md).
- §2 and §4 both change `Span`, `SpanRecord` and the ClickHouse columns: they ship together.

## Open

- §1: `NonZeroU128` against `[u64; 2]`, left to the bench, with `NonZeroU128` the default. Decided: `NonZeroU128`. On
  `perf traced`, `[u64; 2]` cost 10 ns a message more and `(u64, NonZeroU64)` 11 ns more, and the second cannot hold
  an id whose low half is zero; on `perf hosted` none of the three is cheaper than the others
  ([benchmarks](../crates/axon/guide/benchmarks.md)).
- §3: `Arc` segments, left to `perf trace_drain`, paging the default. Decided: segments (§3).
