# Tracing

A trace is one id that follows a piece of work through every actor, node and stream hop it touches. Each hop leaves a
span; the dashboard draws them as a waterfall. Nothing is traced unless a trace was started, and an untraced
message costs one thread-local read at admission and 32 bytes in its mailbox entry.

## Context

`TraceContext { trace, span, flags }` is ambient. While an actor handles a traced message the context is the worker thread's
current one, so everything the handler does is caused by that span without the handler saying so:

| From a traced handler | What carries the trace |
|---|---|
| `tell` / `send` / `ask`, any mailbox admission | the mailbox entry |
| `schedule_at` / `schedule_after` | the timer's message (a repeating timer starts clean) |
| `submit_io` / `submit_future` | the job while it runs, and its completion message |
| `RemoteRef::tell` / `send` / `request`, `Cluster::request` | the frame: kind bit `0x8000` and 25 bytes ahead of the body; an untraced call is byte for byte what it was, and a body within 25 bytes of the peer's `max_frame` is sent untraced rather than refused |
| `StreamNode::publish` | the batch, on every live hop to every subscriber; a batch replayed from history has none |

Outside an actor the application says where a trace starts and what belongs to it:

```rust,ignore
let trace = TraceContext::root();
trace.scope(async { uploads.ask(Upload::Store).await }).await;   // across awaits, on any thread
trace.enter(|| streams.publish(&key, records));                // synchronous code
if let Some(trace) = batch.trace() { trace.enter(|| handle(batch)) }           // a stream subscriber
```

`ActorSystem::builder().tracing(spans, sample_every)`: the system keeps the newest `spans` spans (4096) in a ring.
With `sample_every = N`, one in N messages that arrive with no trace starts one, per mailbox; 0, the default, leaves
starting traces to `TraceContext::root`.

The context is a W3C `traceparent`'s: a 128-bit `TraceId`, a 64-bit span id and a flags byte, 25 bytes on the wire in
that order, big-endian. `root()` draws a random trace id and sets the sampled flag (`TraceContext::SAMPLED`); a child
keeps its parent's trace and flags. A trace id reads as 32 hex digits, a span id as 16. To join a trace started
outside axon, `TraceContext::from_traceparent(header)` reads the header an HTTP request came with, refusing one that is
malformed or has a zero id, and `traceparent()` writes it back for the next service. It is `None` at a root, whose
span is 0, which W3C forbids: write it from inside a handler, where `TraceContext::current()` names the span that runs,
and send no header when there is none.

```rust,ignore
let trace = TraceContext::from_traceparent(header).unwrap_or_else(TraceContext::root);
trace.scope(async { jobs.ask(Job::run).await }).await;   // the spans keep the caller's trace id and flags
```

A trace started by hand, then read back from the node's ring:

```rust
use std::time::Duration;

use axon::{Actor, ActorSystem, Context, TraceContext};

struct Worker;

impl Actor for Worker {
    type Message = &'static str;
    type Error = ();
    fn on_message(&mut self, _msg: &'static str, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
        Ok(())
    }
    fn label(msg: &&'static str) -> &'static str {
        msg // the span's name
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::builder().tracing(4096, 0).build()?; // 0: only traces you start
    let worker = system.spawn(Worker).start()?;

    let root = TraceContext::root();
    root.scope(async { worker.send("checkout").await }).await?;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let spans = system.tracer().trace(root.trace);
    assert_eq!(spans[0].name, "checkout");
    println!("waited {:?}, ran {:?}", spans[0].wait, spans[0].busy);

    system.shutdown().await?;
    Ok(())
}
```

## Spans

| Kind | Recorded by | Name | Times |
|---|---|---|---|
| `handle` | the node whose actor handled the message, or that dropped it unrun ([dead letters](observability.md#dead-letters)) | `Actor::label`; `tell` for a tell the node refused | `wait` in the mailbox, `busy` in the handler; `failed` when the handler returned an error or panicked, and for a drop, which has zero length |
| `call` | the calling node | `tell`, `send` or `request`, with the peer | `busy` until the outbox took it (`tell`) or the answer came; `failed` on any `CallError` |
| `stream` | the origin when it publishes, then every node the batch reaches over the wire | the stream key; the parent it came from, none at the origin | `busy` publishing, or handling the push |
| `app` | the application, with `Tracer::app` | the name it gave | `busy` from `app` until it is dropped |

A failed span says why in `error`, a short kind, `None` when it succeeded:

| Span | `error` |
|---|---|
| a handler | `error` when it returned an error, `panic` when it panicked |
| a dropped message | `dead_letter`, with the dead letter's reason, such as `actor_stopped`, in its `reason` attribute |
| a call | `CallError::kind`: `timeout`, `outcome_unknown`, `no_route`, `unreachable`, `stale_node`, `closed`, `busy`, `no_reply`, `codec`, `rejected.<name>` for the endpoint's `Rejection`, `not_sent.<reason>` when the session refused to send it |
| a publish | the `StreamError`'s name, such as `quota_exceeded` |
| an app span | what the application passed to `AppSpan::error` |

An app span is a step inside a handler, or inside a `scope`, that the application wants on the waterfall with a few
attributes. `Tracer::app(name)` is `None` outside a trace, so an untraced message pays nothing for it. What the
closure given to `AppSpan::enter` sends is the step's child, and the span records itself when it is dropped:

```rust,ignore
// `self.tracer` is `system.tracer().clone()`, given to the actor when it was spawned.
fn on_message(&mut self, batch: Batch, _cx: &mut Context<'_, Self>) -> Result<(), Error> {
    let mut step = self.tracer.app("resize");
    if let Some(step) = &mut step {
        step.attr("items", batch.len() as u64).attr("format", batch.format());
    }
    let resized = self.resize(&batch);
    match &mut step {
        Some(step) => step.enter(|| self.store.tell(resized)),   // the store's span is the step's child
        None => self.store.tell(resized),
    }?;
    Ok(())
}
```

An attribute is a `u64`, an `i64` or a string; a span without any allocates nothing for them.
`start_us` is the recording node's wall clock, so spans from different nodes line up only as well as their clocks do.

## Collection

`Telemetry` serves the ring: `Request::Spans` (a page after a position, for the collector), `Request::Traces` (the
node's own per-trace summaries) and `Request::Trace`. `MetricsService` pulls every node's new spans each cycle and
hands them to every sink (`MetricsSink::write_spans`); the position moves once all of them stored the page, and a
span may be written twice, so a sink keeps one per `(trace, span)`. `MemoryMetrics` keeps the newest 65 536 spans.
ClickHouse keeps `spans` (`ReplacingMergeTree`, ordered by `(trace, span)`, partitioned by day, TTL
`retention_days`). `Capabilities::traces` says whether a provider answers `MetricsQuery::traces` and `trace`: where
it is false, the collector refuses `Traces` and `Trace` without asking it, and the console asks the nodes alone.
A reader takes at most `Tracer::PAGE` (256) spans a hold of the ring's lock, and one that wants more takes it again
from the last `seq` it got. The ring keeps its spans in segments of 32 that a reader clones under the lock and copies
after it, so a hold is a few `Arc` clones, under a microsecond, however big the ring.

A span the ring let go of before a collection cycle reached it is lost: size the ring for the traced rate. The
collector counts the loss in `MetricsHealth::dropped_spans` (`collector.dropped_spans`), from `SpanPage::missed`, the
spans between its cursor and the oldest one the node still held. `trace.spans` counts what a node recorded and
`trace.spans_evicted` what its ring let go of, collected or not. An exporter that drains with `Tracer::since(after, n)`
counts its own gap as `first.seq - after - 1`.

## Dashboard

`api/traces` lists summaries (start, duration, spans, nodes, failed, and the root span's name, kind, actor and peer,
so the list titles a trace the way the waterfall titles its first row) filtered by `from_ms`, `to_ms`, `q`,
`min_us`, `failed`, `limit` ≤ 1000: from the store when the history source keeps traces, otherwise merged from what
the nodes still hold (`source` says which). `api/trace?id=` is every span of one trace that a node or the store has.
The Traces page lists and filters them; a trace opens as a waterfall, indented by cause and coloured by node, with
the mailbox wait drawn as a pale segment at the start of each bar, and a span opens to its actor, peer and timings. A handled message is titled by its label and the actor's registered
name (looked up when the span is read, so the type stands in once the actor is gone); node colours never use red,
which is kept for failed spans. A span whose parent
is no longer held anywhere is shown as detached rather than dropped.

## The `tracing` crate

With `axon = { .., features = ["tracing"] }` an application that already uses [`tracing`](https://docs.rs/tracing)
sees axon's work there. Two things are emitted, both at `DEBUG` with target `axon::trace`, so a subscriber filtered at
the usual `INFO` needs a directive such as `RUST_LOG=axon::trace=debug`, and a `tracing` `max_level_*` feature above
`debug` compiles them out:

- Every span a tracer records, handle, call, stream or app, is an event with the span's `trace` (32 hex digits),
  `span` and `parent` ids (16), as the dashboard shows them, `name`, `actor`, `peer`, `start_us`, `wait_us`, `busy_us`
  and, for a failed span, `error`; its message is the kind. The event goes out when the span is recorded, before the ring, so a system
  built with `ActorSystem::builder().tracing(0, n)` keeps no ring and still emits them. Its `tracing` parent is the
  span current where it is recorded: the caller's for a call or a publish, none for a handler or a stream batch
  taken from a peer. A line-per-event layer such as `fmt` or JSON shows every event; a span-based exporter such as
  OpenTelemetry drops the ones without a parent.
- `TraceContext::root()` logs the trace it starts, `trace` alone, in the `tracing` span current where it is called.
  That is the link from the application's span, say an HTTP request exported to OpenTelemetry, to the axon trace:
  the request carries the `trace` id to look up on the dashboard, and every span event of that trace carries it too.

A trace started by sampling logs nothing: the sample is taken at admission under the mailbox's lock, and it starts on
the receiving side, not in whatever `tracing` span the sender is in. Nothing else crosses from `tracing` to axon: a
`tracing` span id means something only to the subscriber that issued it, is reused once the span closes, and cannot be
sent to another node, so it cannot parent a span that another node records; and reading the current `tracing` span at
every untraced admission would cost every message a dispatcher lookup. Inside a handler, `TraceContext::current()`
names the running span, to put on the application's own events.

Without the feature nothing is compiled in. With it, an untraced message costs nothing more, since only traced work
records spans, and a recorded span costs one atomic load while no subscriber wants `DEBUG`
([benchmarks](benchmarks.md)). The subscriber runs on the thread that records the span: a worker
after the handler returns, the calling task when a call ends, or the node that took a stream batch, so a slow
subscriber slows that thread. A subscriber that panics there loses that event and nothing else.

```rust
use std::sync::Mutex;

use axon::{Actor, ActorHandle, ActorSystem, Context, ReplyTo, TraceContext};
use tracing::field::Field;
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

static LINES: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Keeps each event as a line of fields. `tracing_subscriber::fmt()` with `axon::trace=debug` is the usual choice;
/// this needs no crate.
struct Lines;

impl Subscriber for Lines {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut line = event.metadata().target().to_string();
        event.record(&mut |field: &Field, value: &dyn std::fmt::Debug| line += &format!(" {field}={value:?}"));
        LINES.lock().expect("not poisoned").push(line);
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

enum Upload {
    Store(ReplyTo<()>),
}

struct Uploads(ActorHandle<u64>);

impl Actor for Uploads {
    type Message = Upload;
    type Error = &'static str;

    fn on_message(&mut self, Upload::Store(reply): Upload, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        self.0.tell(42u64).map_err(|_| "thumbnails is full")?;
        let _ = reply.reply(());
        Ok(())
    }

    fn label(_: &Upload) -> &'static str {
        "store"
    }
}

struct Thumbnails;

impl Actor for Thumbnails {
    type Message = u64;
    type Error = ();

    fn on_message(&mut self, _size: u64, _cx: &mut Context<'_, Self>) -> Result<(), ()> {
        Ok(())
    }

    fn label(_: &u64) -> &'static str {
        "resize"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing::subscriber::set_global_default(Lines)?;
    let system = ActorSystem::builder().build()?;
    let thumbnails = system.spawn(Thumbnails).start()?;
    let uploads = system.spawn(Uploads(thumbnails)).start()?;

    let request = tracing::info_span!("request");
    let trace = request.in_scope(TraceContext::root);
    trace.scope(uploads.ask(Upload::Store)).await?;
    system.shutdown().await?;

    let lines = LINES.lock().expect("not poisoned");
    for line in lines.iter() {
        println!("{line}");
    }
    assert_eq!(lines.len(), 3, "the trace started, then one span per actor");
    Ok(())
}
```

It prints the start, then the two handle spans; `resize`'s `parent` is `store`'s `span`:

```text
axon::trace message=trace started trace=46e0d1b1578c25a4c1f0e5a2b7d3986e
axon::trace message=Handle trace=46e0d1b1578c25a4c1f0e5a2b7d3986e span=9db34d60fdcf3e8e parent=0000000000000000 name=store actor="tracing::Uploads" start_us=1790413949126765 wait_us=5 busy_us=6
axon::trace message=Handle trace=46e0d1b1578c25a4c1f0e5a2b7d3986e span=2d0e3c292eccc373 parent=9db34d60fdcf3e8e name=resize actor="tracing::Thumbnails" start_us=1790413949126784 wait_us=17 busy_us=0
```
