# axon — system overview and capability requirements

The requirements axon was built against, as a checkable summary. "Old" below is the
earlier actor runtime axon replaced. IDs here (`RT-1`, `TR-3`…) are the
requirement handles axon was built and tested against.

Legend: **KEEP** = same behaviour as old · **FIX** = same capability, better
design/implementation · **NEW** = not in old · **DROP** = deliberately not carried over.

---

## 2. Requirements

### 2.1 Runtime

| ID | Requirement | |
|---|---|---|
| RT-1 | Typed actors: one `Message` type, `on_start` / `on_message` / `on_batch` / `on_stop`, sync `&mut self`, `Context` for self-ref, timers, jobs, stop | KEEP |
| RT-2 | Execution modes: shared worker pool, dedicated thread, embedded (caller drives turns) — one driver implementation | KEEP |
| RT-3 | Bounded mailbox with byte + slot accounting; `tell` (never waits, `Full`), `send` (bounded wait), and inside the `Mailbox` `try_tell` and `reserve`→`commit` permits; typed rejection **returns the owned payload**; `#[must_use]`; strict FIFO waiters, no bypass; permits release on drop | KEEP (port contract exactly) |
| RT-4 | Ask/reply with absolute deadlines; reply dropped ⇒ caller sees a distinct error | KEEP |
| RT-5 | Lifecycle: starting→running→draining→stopped; `drain`/`stop`; `Exit` with 10-variant `StopReason` and discarded/pending counts; `cx.watch(&lifecycle, map)` tells an actor once, as its own message admitted like a job result, when a local actor or supervised family ends (not on a restart), after the system freed its name; cancellable; counts against `sources`; `cx.link(&lifecycle)` ties two local actors so that either failing (`StopReason::is_failure`) stops the other with `Linked`, a stop or drain passing nothing on, symmetric, idempotent, removable, and held by the incarnation, not across a restart | KEEP |
| RT-6 | Timers (one-shot/interval, cancel), virtual clock | FIX: binary heap, not O(n) scan |
| RT-7 | Jobs: offload async/blocking work, completion delivered back as a message; manual job runner for tests | KEEP |
| RT-8 | Sources: zero-copy batch ingestion lane driven by the owner | DROP (2026-09-28: no caller; a hosted actor ingests with `cx.attach(stream, map)`) |
| RT-9 | Supervision: restart policy, backoff, generation-tracked address | FIX: one cell implementation (old duplicates ~180 lines), closure predicate, **jittered backoff**, **reachable from the System facade** |
| RT-10 | Panic boundaries: a panic in handler, waker, message `Drop`, or error `Debug` never corrupts the mailbox or wedges shutdown | KEEP (port the six adversarial tests) |
| RT-11 | Turn budgets + fairness between messages / job completions / timers | FIX: simpler than three-lane round-robin if tests still hold |
| RT-12 | Mailbox state split by lock discipline: hot producer path vs owner-only state vs atomic phase | FIX (main perf win) |
| RT-13 | Mailbox config: a handful of knobs (capacity, bytes, batch) instead of 12; memory accounting that means something | FIX |
| RT-14 | Runtime shutdown with deadline; reports still-running actors | KEEP |
| RT-15 | `Router`: a plain value over local `ActorHandle`s of one message type, clones sharing the members; round-robin or least-loaded (fewest queued messages), passing over a member that is stopping or draining; `member(&key)` by rendezvous, so a member leaving moves only its keys; a member that ends is dropped before its exit is visible; `add` is idempotent; the member's own errors and backpressure, no extra queue, no retry | NEW |
| — | `Scope`, `IdlePolicy::Spin`, `affinity` feature, `TimingHistogram::merge`, `ManualJobs::run_last` | DROP (dead) |

### 2.2 System API

| ID | Requirement | |
|---|---|---|
| SY-1 | `ActorSystem::builder()` → runtime, mailbox defaults, start/request/shutdown timeouts → `build()?`; a cluster starts on the built system (`Cluster::start(&system, cfg)`) | KEEP |
| SY-2 | `system.spawn(a).name(..).execution(..).mailbox(..).start()` returns at once; messages queue behind `on_start`; `handle.ready().await` waits for it (decided 2026-09-22, was: wait for `on_start`, cancel on timeout); `.idle(d)` drains an actor that has handled no message for `d` | FIX: no 5 ms polling cleanup; one mailbox setter |
| SY-3 | `ActorHandle`: `tell` / `send` / `ask` with `Into<M>`, cloneable repeatable `join()` | KEEP |
| SY-4 | **Supervision on the facade**: `system.supervise(factory).policy(..).start_supervised()`; lookup by name (`lookup`, `lookup_supervised`) | NEW |
| SY-5 | Paged live actor diagnostics (cursor by incarnation) | KEEP |
| SY-6 | Reasoned shutdown: `LeaveReason`, first caller fixes intent, detached coordinator, shared report `{leave, actors, still_running, warnings, failures}` consistent in one pass | KEEP / FIX consistency |
| SY-7 | `SystemExtension` (`leaving` / `shutdown` / `close`), attachment-order leaving, reverse-order shutdown, per-hook unwind guards, weak system handle | KEEP — this is the extension seam |

### 2.3 Transport

| ID | Requirement | |
|---|---|---|
| TR-1 | One authenticated, bounded, ordered, framed session over TCP; split reader/writer tasks; batched writes | KEEP; FIX: **buffered reads** (old = 3 syscalls/frame), buffer reuse, reused Zstd contexts |
| TR-2 | Wire format: versioned binary header, sequence gap = fatal | FIX: compact header (old 78 B, ~2.5× needed), length-prefixed; drop per-frame session id / unused fields |
| TR-3 | **Auth**: mutual challenge–response (HMAC over nonces + node ids + negotiated params) for shared-secret mode; credential never sent; responder proves first to nothing unauthenticated; binary pre-auth hello with tight size cap (no pre-auth JSON) | FIX (security) |
| TR-4 | TLS via rustls, optional mTLS with **cert→node identity binding as a shipped authenticator**; plaintext only by explicit opt-in | KEEP / FIX |
| TR-5 | Pluggable `Authenticator`, default deny, namespace isolation, min-only limit negotiation | KEEP |
| TR-6 | Compression: none / LZ4 / Zstd, per frame, size threshold + min-savings fallback | KEEP; FIX: **real negotiation** (receiver can refuse codecs), configurable Zstd level, decompressed-size cap enforced before work, bomb-safe |
| TR-7 | Budgets: max frame, outbox bytes/frames, handshake/read/write timeouts; slow consumer ⇒ bounded, explicit outcome | KEEP; FIX: reply-outbox-full fails that request, not the session |
| TR-8 | **Keepalive ping/pong + idle policy; reconnect with jittered backoff + circuit breaker** | NEW |
| TR-9 | Handshake concurrency + per-IP limits; accept loop survives transient errors | NEW / FIX |
| TR-10 | Typed error taxonomy with bounded reason enums (double as telemetry labels) | KEEP |

### 2.4 Cluster

| ID | Requirement | |
|---|---|---|
| CL-1 | Engine: listener, per-node connection cache, endpoint registry with authorizer, request timeout, caps on peers/endpoints/in-flight | KEEP; FIX: split telemetry out of the request path, no tombstone leak |
| CL-2 | Remote contract: user-defined `WireProtocol` (const id + schema, encode/decode), no reflection/registry; the serving side's `ActorProtocol` encodes the reply, so the actor answers `Reply<P::Reply>` with a value | KEEP |
| CL-3 | `publish` / `resolve` (unique) / `resolve_all` / `resolve_on(node)` → `RemoteRef`; `request` and `send` with admission result; `Reply<P::Reply>` in the actor's message, drop ⇒ NO_REPLY | KEEP; FIX: 2 frames per request (fold ACK into REPLY), resolve caching |
| CL-4 | Discovery providers: static, in-memory, Consul (register, TTL heartbeat, deregister) behind one trait | KEEP |
| CL-5 | Membership subscription: first event is a snapshot, then diffs; `wait_view` waits for a condition on the view; `running`/`draining` lifecycle metadata | KEEP |
| CL-6 | Graceful leave: announce → drain → withdraw; peers stop routing to a draining node | KEEP |
| CL-7 | Authenticated control RPC over the cluster transport (caller identity from session, per-service access policy default-deny, size + concurrency + deadline caps) — what extensions use to find each other | KEEP |
| CL-8 | Direct peer failure detection (heartbeat-based, faster than provider TTL) | NEW — *decision D4* |
| CL-9 | **Actor kinds + remote spawn**: nodes register named kinds (`cluster.kind::<P>(name)` with optional execution, mailbox and restart policy, then `.register(constructor from encoded args)`); `spawn_on(node, kind, args)` and placement-chosen `spawn(kind, args)` return a `RemoteRef`; authorizer-gated, capped | NEW |
| CL-10 | **Unique by key**: `get_or_spawn(kind, key)` — rendezvous placement over running members picks the owner node, owner enforces one instance per key locally, refs fenced by node incarnation; `keyed(kind, key)` is a reference that follows the key, asking the owner again after a call that says to resolve; on membership change misplaced instances drain and respawn on the new owner; `watch_key(kind, key)` streams where the key runs (`Running { node, epoch }` or `Stopped`), asking the owner only when polled so the latest state wins, and under an `OwnershipAuthority` never shows one epoch on two nodes; a kind registered with `.idle(d)` ends a key quiet for `d`, and the next `get_or_spawn` starts it fresh. Default is membership-consistent (brief duplicates possible during a partition); a node whose discovery heartbeats fail for `heartbeat_grace` asks its peers whether they still list it, and unless one does and none has dropped it, it is isolated: it refuses new keys, still answers for those it runs, and stops them at once without an authority or once a peer has dropped it, or else leaves each to its lease's fence; it rejoins once discovery has heard it, or a peer vouched for it, for a whole `heartbeat_grace` (an outage that hits every node alike stops nothing); optional strict mode via a pluggable `OwnershipAuthority` (Consul session lock first) | NEW |
| CL-11 | **Constrained placement**: nodes advertise labels (`region=eu`, `role=pricing`, `gpu`…) in discovery metadata; a `Placement` = required labels + optional preferred labels + strategy (rendezvous-by-key, least-loaded, spread); usable in `spawn`, `get_or_spawn` and as a per-kind default. For unique-by-key the constraint is part of the key's identity so every node computes the same owner; no eligible node ⇒ typed `NoCandidate` error | NEW |
| CL-12 | **Consensus seam**: `OwnershipAuthority` = lease with fencing epoch (`acquire` / `renew` / `release`); a key's actor self-stops when its lease cannot be renewed, and the node's other keys run on; refs carry the epoch. Implementations: Consul session lock (first); `PaxosLeases`, a majority of acceptor nodes with no leader, each logging who holds each key ([paxos-leases.md](paxos-leases.md)) | NEW (Consul, Paxos built) |
| CL-13 | **Rolling upgrades**: sessions negotiate the highest wire version both ends speak, and only ranges that do not meet are refused, with both ranges named and the refusal counted; from 1.0 a release speaks N and N-1. A name or a kind is served at several `SCHEMA`s of one protocol at once, a call goes to a node that serves its caller's schema (`SchemaMismatch`, retry `Resolve`, otherwise), and keys are placed only on the nodes that serve the most of the schemas the kind's callers use, so that every caller, old or new, reaches them ([rolling-upgrades.md](rolling-upgrades.md)) | NEW |
| — | `ActorDirectory`, `ExplicitRouting`, `LocalAuthority`, multi-lane header fields (no callers, not needed by CL-9/10) | DROP |

### 2.5 Streams + recovery

| ID | Requirement | |
|---|---|---|
| ST-1 | `StreamHub`: sync, single-owner, host-driven; open stream, publish sequenced batches, subscribe from `Live` / `After(seq)` / `First` / snapshot-then-live, on any node of a relay tree | KEEP |
| ST-2 | Exact-key authorization on 4 access classes; revocation takes effect synchronously | KEEP; FIX: subscription epoch instead of per-event authz calls. On a `StreamNode` the stream authorizer is fixed at start: a running stream is revoked per peer by `Cluster::set_authorizer`, within the parent's `downstream_expiry` and a tick |
| ST-3 | Positional dedup via one primitive; ordering contract `Ready → [Snapshot] → replay → CaughtUp → live` with atomic cut | KEEP |
| ST-4 | 4 delivery policies, 3 slow-consumer policies, per-subscriber and global byte/record limits; rejected publish returns the caller's allocation | KEEP; FIX: checked/central accounting, one queue |
| ST-5 | Session-scoped versioned interest sets (reconnect replaces; stale sessions can't decrement) | KEEP (full-set only, drop deltas) |
| ST-6 | Recovery contract: one `RecoverySource`, bounded reads; the in-memory ring `MemoryStore` or the application's own; source bound at subscribe; an origin keeps listed namespaces' history on disk and resumes after a crash ([streams § History on disk](../crates/axon/guide/streams.md#history-on-disk)); a snapshot is a compacted stream's state (`DeliveryPolicy::Compacted`, `SnapshotThenLive`) | KEEP / FIX |
| ST-7 | `close_stream`; stream telemetry (see OB-6); benchmarked | NEW |
| ST-8 | One module/crate; drop the 4 never-implemented trait families | FIX / DROP |
| ST-9 | First-class stream↔transport binding (old: only a hand-rolled example) | NEW — *decision D6* |

### 2.6 Observability backend

| ID | Requirement | |
|---|---|---|
| OB-1 | Three independently attachable extensions — `Telemetry`, `Metrics` (collector + query), `Dashboard` — same pattern as old: system owns `Arc<Inner>`, public handle is a clone, tasks hold `Weak`, discover each other over the control RPC | KEEP |
| OB-2 | Provider seam: `MetricsSink` (write/events) + `MetricsQuery` (capabilities/query/events/traces); query-only hosts; capabilities enforced by dashboard and collector | KEEP |
| OB-3 | ClickHouse provider: samples + events tables, TTL, idempotent via sort key, typed params (injection-safe), cursor advances only after a successful write | KEEP; FIX: native histogram rows instead of 34 series per histogram, single serialization |
| OB-4 | Dashboard host: overview, SSE events, actors, inventory, history, timeline; a login page for listed users (argon2 hashes), a signed session cookie with idle and absolute limits and no server store, sign-ins limited per client and user, Origin checked, HTTPS built in or through trusted proxies, optional redaction of actor keys; CSP/nosniff/no-store/no-referrer, HSTS over TLS; SPA embedded in binary, mountable under a base path; stale node snapshots kept with explicit error | KEEP; FIX: serialize SSE once per tick, one shared fan-out loop (old has three) |
| OB-5 | System metrics (CPU, memory, network, disk) per node | KEEP |
| OB-6 | **More telemetry** — persist what is today live-only (mailbox-wait / handler / turn histograms, session encode/decode/write timings, per-peer request counters, compression in/out bytes) **plus new**: per-message-type counts & handler latency (bounded top-N), dead letters & rejection reasons, ask timeout rate, restarts/lifecycle counters, worker utilisation & run-queue depth, timer lag, transport RTT (from keepalive), reconnects/backoff state, handshake failures by reason, membership churn, stream fanout/lag/resets by reason, collector health as metrics | NEW |
| OB-7 | Cardinality rule: persisted series are per-node / per-actor-**type** / per-peer; per-actor detail is live + opt-in allowlist | NEW — *decision D7* |
| OB-8 | Low overhead: feature-gated, relaxed atomics / thread-sharded counters, 1-in-N sampled timings, fixed-bucket histograms (one shared type), **a trustworthy overhead benchmark** with budget ≤2 % counters / ≤5 % timings | KEEP / NEW harness |
| OB-9 | Trace-context propagation in the message envelope: ambient context across mailboxes, timers, jobs, cluster calls and stream batches; spans collected, stored and drawn, and with the `tracing` feature emitted as `tracing` events ([tracing](../crates/axon/guide/tracing.md)) | NEW — *decision D8* |
| OB-10 | Health probes: `/healthz` (answers while the system lives) and `/readyz` (503 while shutting down, isolated, the ownership authority warms or a new quota replica starts) on every node, on the `Dashboard` host at the root outside `base_path` and without credentials, or on `Health`'s own port | NEW |
| OB-11 | A cluster timeline: each node keeps its newest 256 events (keyed actors acquired, lost, handed off and restarted eagerly; supervised restarts, at most one a minute per family with the count; streams losing their parent, reset or orphaned, at most one per kind per tick), the collector pages them by cursor with membership changes (a node back under a new boot is `restarted`), stores each once, and counts what a ring let go of in `dropped_events`; `api/timeline` filters by node and kind | NEW |

### 2.7 Dashboard UI

| ID | Requirement | |
|---|---|---|
| UI-1 | Stack: React + TS strict + Vite, **React Router** (URL is the source of truth for view state), **Zustand** for the live telemetry store (selector subscriptions, so the 1 s tick only re-renders what changed), **TanStack Query** for history/timeline fetches (dedup, cache, abort), **Tailwind v4** with the design tokens as `@theme` CSS variables + plain `.css` only where utilities don't fit (charts, topology SVG), ECharts in a lazy chunk, **self-hosted variable webfonts** (Inter + JetBrains Mono for numerics; bundled, CSP `font-src 'self'`), ESLint + Vitest + Playwright. Still embedded in the binary, still no runtime fetches to third parties | FIX |
| UI-1b | Icons: keep the existing hand-authored icon set (single `Icon` component, 24-unit paths) as is | KEEP |
| UI-2 | Same 8 routes: overview, topology, nodes, node detail, actors, actor detail (incarnation-aware), connections, events | KEEP |
| UI-3 | Same design language: both themes with the recorded tokens, 12 px root, tabular numerics (now via webfont features), 1 px borders, 3/4/5 px radii, shadows only on floating layers, no animation; resizable/collapsible persisted sidebar with no first-paint flash | KEEP (tokens copied as values from the spec) |
| UI-4 | Topology: 5 deterministic collision-free layouts, 4 edge measures, linear/log, 2 colour modes, pan/zoom/fit/focus, camera stable across refreshes, session dedup | KEEP |
| UI-5 | Telemetry correctness rules everywhere: counter resets, incarnation/session fencing, gaps stay gaps, no rate across retained↔live seam, unknown ≠ zero (`—`) | KEEP (port the model unit specs as acceptance tests) |
| UI-6 | History panels with time ranges backed by the metrics provider; live SSE with staleness/disconnected states | KEEP |
| UI-7 | Playwright UX contract re-implemented as the acceptance suite | KEEP |
| UI-8 | Structure: component-per-file, feature folders per route, one tokens file (old: 5 stacked global CSS files with order-dependent overrides) | FIX |
| UI-9 | All UI state (selection, filters, sort, tabs, range) in the URL | NEW |
| UI-10 | Events page backed by `api/timeline` (old caps at ~256 in-memory even with ClickHouse), with a swimlane per node above the table whose marks filter it to their node and kind (OB-11) | NEW |
| UI-11 | Views for OB-6: latency histograms/heatmaps, per-message-type table, dead letters, worker utilisation, RTT/compression per connection, streams page | NEW |

### 2.8 Cross-cutting

| ID | Requirement |
|---|---|
| X-1 | Minimal comments; normal line layout (old has minified hot paths) |
| X-2 | Fewer crates: `axon` (runtime+system), `axon-net` (transport), `axon-cluster` (membership, remote calls, keyed actors, leases, quotas), `axon-stream`, `axon-obs` (telemetry+metrics+clickhouse+dashboard, feature-gated, the UI in `crates/axon-obs/ui`), `axon-testkit` — *decision D9* |
| X-3 | Tests: deterministic runtime tests, adversarial/panic tests, transport fuzz targets, cluster multi-process smoke, observability HTTP contract, Playwright |
| X-4 | Perf gates: `scripts/bench.sh` gates every harness row (runtime tell/ask, transport frames/s, stream fanout, telemetry overhead, quotas…) against a per-CPU baseline ([benchmarks § The gate](../crates/axon/guide/benchmarks.md#the-gate-scriptsbenchsh)) |

---

## 3. Decisions

| # | Decision | Status |
|---|---|---|
| D1 | Third-party deps allowed where they earn it | decided |
| D2 | Auth: shared-secret challenge–response **and** mTLS; plaintext only by explicit opt-in | decided |
| D3 | No wire compatibility with old nodes | decided |
| D4 | Add heartbeat-based peer failure detection on the keepalive | decided |
| D5 | Keep placement + ownership ideas, but wired in as remote spawn (CL-9) and unique-by-key (CL-10); drop directory/explicit routing/lanes | decided |
| D6 | First-class stream↔transport binding (this is the pub/sub extension's foundation) | decided: yes |
| D7 | Persist per-actor-type series; per-actor live-only + allowlist | decided |
| D8 | Trace context: implemented after parity, as an optional field of the mailbox entry and a flagged prefix on the wire | decided, built |
| D9 | Crate layout per X-2; project name **axon** | decided |
| D10 | Dashboard read-only | decided |
| D11 | Keep `testkit` as its own crate | decided: keep |
| D12 | Prometheus | decided: after ClickHouse, each node's `Health` port serves `/metrics` in the text format once `serve_metrics` is called; a stock Prometheus scrapes every node, no collector between |
| D13 | UI: URL-shareable state, React Router, state management, Tailwind, webfonts, keep icons | decided (UI-1, UI-1b, UI-9) |

---

## 4. Not built, and why

Asked for by the example apps (2026-09-25), weighed, and left out. Each answers "why is there no X".

- **An automatic resend-once in `KeyedRef`**: a second retry policy. The opt-in form is `request_until(m, deadline)`;
  otherwise the caller acts on `err.retry()`.
- **`on_handoff` / a handoff hook**: helps planned moves only, and is racy without a handoff store. Deferred.
- **A `KeyStore` seam or an async kind factory**: they lean toward a journal, and would hold the key's gate across I/O.
- **`SerdeProtocol`, a `Format` trait, a derive**: no derive macros;
  the guide's serde program is the answer. A per-message reply type only moves the boilerplate.
- **`resolve_within`**: the same poll, hiding the per-try fan-out; `NamedRef` and `request_until` cover the need.
- **A route cache in `get_or_spawn`**: it cannot be invalidated without the call reporting back, and it is unbounded.
- **A `max_moves` knob**: the bound is derived from `max_pending`.
- **Refusing `max_leases < max_spawned`**: it misses the real case of N nodes × `max_spawned`.
- **`OwnershipAuthority::ready()`**: `warming()` covers it; a readiness future would be one more method on every
  authority.
- **`Fence::until`**: the per-key deadline lives in `Unique`.
- **Letting a dev seed skip the warm-up**: a production footgun.
- **A heartbeat thread, or grace counted in missed beats**: both break stop-before-removal.
- **`subscribe_namespace`** (wildcards are a non-goal) and **`wait_downstream`** (`First` covers it).
- **Remote admission that waits for room**: it would break per-peer order.
- **A client or observer mode**: a process that only calls joins as a member. Deferred until client churn shows up.
- **`ClusterStats::moving` and a running-keyed gauge**: deferred until the console or an operator asks.
- **A journal for a whole node, or a `RecoverySource` on `StreamNode`**: still out. What changed: the origin keeps the
  history of the namespaces given a `Retention` on disk and resumes after a crash
  ([streams § History on disk](../crates/axon/guide/streams.md#history-on-disk)); that is a bounded ring on disk, not
  an event store. A local log of one owner is built (2026-10-10): `axon::files::Log` ([durable-log](durable-log.md)).
  Five stores in axon and the examples had written the same CRC frames, torn-tail cut and grouped sync by hand, and
  on build-1 a sync of 256 keys in one file took 4.2 ms against 343–379 ms for a file per key. It is still not an
  event store: it has no queries, and each key's records go with its checkpoints, trims and the log's `max_bytes`. A
  replicated log stays out ([paxos-leases](paxos-leases.md) §1).
- **Refusing `NotOwner` by dialling a full mesh**: n² links. A node probes the owner it would pick before it refuses.
- **A start-up allowance for isolation**: it would weaken isolation at start. The root was fixed instead: a successful
  `register` is not taken as proof that the node was heard.

Not built yet; add when an app needs it:

- **A per-key relay parent**: a node picks one parent per tier for all its keys (a rendezvous of its own node id), so
  relays can form a ring. Add when that costs more than a hop.
- **`Placement::sticky()`**: rendezvous moves a key back as soon as its preferred node returns. Add when a move costs
  more than the imbalance.
- **A warning when an `ActorSystem` is dropped without `shutdown`**: the tests drop systems on purpose to crash a
  node, so it needs an explicit crash call or an opt-out first.
