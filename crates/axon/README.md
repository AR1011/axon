# axon

Synchronous typed actors on a thread-per-worker runtime, clustered over authenticated sessions, with ordered streams
across nodes and a console to watch it all.

Read this page first, then the guide for the part you need. Guides hold complete programs that the test suite runs;
copy from them rather than guessing signatures.

| Need | Read |
|---|---|
| actors, supervision, timers, mailboxes | [actors] |
| remote calls, remote spawn, keyed actors, TLS | [cluster] |
| rate and concurrency limits per key, cluster-wide | [cluster § Quotas][quotas] |
| a keyed actor whose state survives a crash, in its node's log | [cluster § A key's state on disk][keyed-disk] |
| ordered streams across nodes | [streams] |
| a websocket gateway: a session per socket, authentication, per-account limits, filters, resume | [streams § A websocket gateway][gateway] |
| console, metrics, traces, dead letters, health probes | [observability] |
| metric names, HTTP API | [telemetry], [dashboard] |
| trace context across actors, nodes, streams | [tracing] |
| fault tests: crashes, pauses, partitions, clock skew, by seed | [cluster § Simulating a cluster][simulating] |
| what the numbers are and how they were measured | [benchmarks] |

| Crate | Holds | Features |
|---|---|---|
| `axon` | actors, the runtime, the system, the guides | `serde`, `tracing` |
| `axon-net` | authenticated framed sessions over TCP or TLS | `serde` |
| `axon-cluster` | membership, discovery, remote calls, remote spawn, keyed actors, leases, quotas | `consul`, `serde` |
| `axon-stream` | sequenced streams, and relays between nodes | `cluster`, `serde` |
| `axon-obs` | telemetry, metrics collection, the console | `dashboard`, `clickhouse`, `system-metrics` |
| `axon-testkit` | a seeded network simulator, and real clusters on virtual time, for tests | |

## The API on one page

```text
axon
  trait Actor { type Message; type Error; on_start, on_message (required), on_batch, on_turn_end, on_stop, label }
  ActorSystem::builder() .mailbox(MailboxLimits) .timing_sample_every(n) .tracing(spans, every) .build()?
    .runtime(RuntimeConfig { workers, cores, spin, turn_budget, pools: vec![PoolConfig { name, workers, cores, spin, balance, turn_budget }], .. })
    RuntimeConfig::allowed_cores() -> Vec<usize>   the cores this thread may run on (taskset, a container's cpuset), the only ones cores may name ; empty where the OS does not say
    RuntimeConfig::job_executor = Some(JobExecutor::custom(|job| ..) | JobExecutor::thread_pool(threads, queued)? | JobExecutor::tokio(handle))   where the actors' jobs and attached sources run ; None: on the system's tokio runtime ; a job it refuses is Err(TaskAdmissionError::Full) ; one it drops unfinished, not cancelled, ends its actor Failed
  system.spawn(actor) .name(s) .mailbox(cfg) .fence(Fence) .idle(d) .execution(Execution::Dedicated | Execution::Pool(name)) .start()? -> ActorHandle<M> ; handle.ready().await? waits for on_start
  system.supervise(|| actor) .policy(RestartPolicy) .start_supervised()?              -> SupervisedHandle<M>
    RestartPolicy { max_restarts, within, backoff: Backoff { initial, max, jitter }, should_restart }   default 3 within 60 s, 10 ms doubling to 1 s ; jitter: the fraction of each delay cut away at random, so none is longer than max ; Backoff is ClusterConfig::backoff's too (axon_net::Backoff), backoff.delay(failures)
  handle.tell(m)? | handle.send(m).await? | handle.ask(Msg::Variant).await?   (variant holds ReplyTo<R>) ; an ask alone on its actor and its thread, polled off the workers on the system clock, runs an idle actor's quick turn there (not a named pool's, nor one on pinned cores), else waits up to 2 µs there for the answer before it sleeps ; on the system clock a send's or an ask's deadline ends it up to about 1 ms late
  handle.lifecycle.stop() | .drain(deadline) ; handle.join().await -> Exit
  cx.schedule_after / schedule_every / cancel_timer ; cx.submit_io(f) / submit_future(fut) / attach(stream, map) -> JobHandle (.cancel) ; a refused submit: SubmitError { reason, work } (the work back, not run) ; cx.stop()
    cx.submit_io_on(&executor, f) / submit_future_on(&executor, fut)   that job on an executor of the application's own, under the same MailboxLimits::jobs, cancel, pause and drain ; system.job_executor() the system's own
    cx.pause_until(&job)?   until its result is handled the actor handles only that, timers and a stop: messages, other jobs' results and watch downs wait, then come in order ; one taken in on_start is part of the start: ready() waits for it
    axon::catch_panic(fut).await -> Result<T, Panic> (.message())   a job's panic as its result ; a job left to unwind ends its actor Panicked
    map(None) of an attached source comes behind its items, through the mailbox: a cancel takes back nothing already sent
  cx.attach_sink(sink, capacity, map) -> SinkHandle (.try_send never waits, Full → SinkEvent::Ready later ; .close ; .cancel)
  cx.watch(&handle.lifecycle, |exit| Msg::Down(exit))? -> WatchHandle (.cancel)   told once when a local actor or supervised family ends
  cx.link(&handle.lifecycle) -> LinkHandle (.cancel) ; cx.unlink(&lifecycle)   either failing stops the other, StopReason::Linked(incarnation) ; reason.is_failure() (reason.is_own_failure(): not Linked)
  cx.stash(msg) -> Result<(), M>   held outside the mailbox, up to MailboxLimits::messages and bytes ; cx.unstash_all()   they run again in stash order before the mailbox ; still stashed at the end: discarded and counted
  Router::new(Routing::RoundRobin | Routing::LeastLoaded) .add(handle) .members() ; router.tell(m)? | .send(m).await? | .ask(..).await?   a clone shares it; a member that ends is dropped
    router.member(&key) -> Option<ActorHandle<M>>   rendezvous: a member leaving moves only its keys
  system.topic::<M>(name)? -> Topic<M>   this node's, made on first use ; NameTaken for another M ; lasts while a handle or a subscriber does
    topic.subscribe(handle | supervised_handle)? (SubscriberLimit past Topic::<M>::MAX_SUBSCRIBERS) .unsubscribe(&lifecycle) .subscribers()   one that ends is dropped ; a family stays across restarts
    topic.publish(msg) -> usize   msg: Clone + Into<M> ; tells each, never waits ; one that refuses misses it, a dead letter labelled publish, counted in SystemStats.dead_letters
  system.lookup::<M>(name) | lookup_supervised::<M>(name) ; system.dead_letters().recent(n) ; system.events().since(after, limit) ; system.tracer() ; system.shutdown().await? (actors drain until a quarter of shutdown_timeout is left, the extensions share the rest)
  system.diagnostics_page(after, limit)? -> ActorDiagnosticsPage   each actor's phase, restarts, diagnostics (a family's counters add up its generations; dead_letters counts what each ended with), started_us and restarted_us: Option (trace::unix_micros at its spawn and its latest restart)
  EventLog::new(clock) .record(kind, subject, detail) .since(after, limit) -> Vec<Event>   the newest EVENTS (256) of this node's timeline, never waits ; a supervised family's restarts are one event at once, then at most one a minute with its count
  system.clock() ; Fence::new(system.clock(), lapses_in) .hold_for(d) .is_open()   (hold_for(Duration::ZERO) lapses it)
  TraceContext::root().scope(future).await | .enter(|| ..) ; TraceContext::current() -> Option<TraceContext>   the trace the running handler or scope is in
  feature tracing: every recorded span is a `tracing` event (target axon::trace, its ids in hex) ; TraceContext::root() logs its trace in the current `tracing` span
  Lane::{Control, Live, Bulk}   a traffic class: its own session to each peer, dialled on first use ; on the wire 0, 1, 2 ; Lane::ALL ; lane.name() "control", "live", "bulk", as in JSON (feature serde) and the console ; also axon_net::Lane
  trace::random_u64()   placement, Backoff jitter, trace ids: per thread, and seeded where a simulation runs
  trace::unix_micros()   the only wall clock axon reads: stepped per node where a simulation runs
  files::{Files, FileOp, FileFuture, OsFiles, MemoryFiles, crc32}   a directory a store keeps across restarts: trait Files { list, read (synced first), read_range(name, offset, len), apply(Vec<FileOp>) }, each a FileFuture<T> that starts nothing until polled: call them in a handler and await them in one cx.submit_future job ; FileOp::{Append, Truncate, Remove, Replace (all or nothing, after any crash), Write(name, offset, bytes) (over bytes inside the file, in part after a crash)} ; apply runs the ops in order and syncs appends and writes before a later Remove, Truncate or Replace takes effect, and keeps them though an error follows ; OsFiles syncs the files of one apply together, up to 32 at once ; OsFiles::new(dir) locks it while in use, works on tokio's blocking pool, or in place where no tokio runtime is entered (a JobExecutor::thread_pool job) ; MemoryFiles for tests
  files::Log::open(files, LogConfig { segment_bytes, preallocate, fill_ahead, max_record, max_unwritten_bytes, max_write_bytes, max_bytes, max_keys, .. }).await? -> Log   the records of many keys in one directory, one owner ; preallocate (the default) zero-fills a segment fill_ahead (256 KiB) ahead of its frames ; reads every segment once, cuts a torn tail, InvalidData naming file and offset for the rest
    log.append(key, epoch, body)? -> Appended { lsn, at: Address { epoch, seq } } | append_at(key, at, body)? (Gap { have } unless the next) ; never waits, durable once log.durable() reaches lsn
    log.fence(key, epoch)? -> (Lsn, epochs)   refuses Fenced { fence } at or below the key's fence, so one epoch fences once ; append refuses an epoch below it
    log.checkpoint(key, through, state)? | trim(key, through)? | remove(key)?   records at or before through go ; segments with nothing live are removed, a mostly dead old one copied forward
    log.take_write(now_ms) -> Option<FileFuture<()>>   in on_turn_end: one write out at a time, of everything queued, run in cx.submit_future under catch_panic ; log.written(result) -> Lsn the new durable ; a failed one sets failing() and goes again
    log.read(key, from, max_bytes) -> FileFuture<Vec<Entry::{Checkpoint, Record}>> durable only ; key(k) keys(from, limit) -> KeyInfo ; pinning(n) the keys holding the oldest segments ; writing_since() ; stats() -> LogStats
    LogError::{TooLarge, Behind (max_unwritten_bytes), Full (max_bytes), Keys, Fenced { fence }, Gap { have }}   a refusal changes nothing
    one log for every key of a kind on a node, kept by one actor that keyed actors ask: the recipe in guide/cluster.md § A key's state on disk, not a shipped actor
  EmbeddedDriver::new(actor, cfg, clock) | with_jobs(actor, cfg, clock, executor) + Clock::manual()   deterministic single-actor tests
  Histogram::default() .record(d) .merge(&h) .since(&earlier) -> Option<Histogram> (None across a restart) .percentile(p)   (at most 12.5 % high, up to about 17 min) ; AtomicHistogram .record(d) .snapshot() -> HistogramSnapshot (.first() .counts() .count() .sum_ns(): the non-zero run ; Histogram::from(&s) and back, both serialise the same) ; driver.run_turn(budget) | run_until_idle(max_rounds) -> Result<usize, NotIdle> (runs JobExecutor::manual jobs too)

axon-cluster
  axon_net::Config::new(node_id, SharedSecret::new(secret)? | CertificateIdentity)   .allow_plaintext, .namespace, .network (None is TCP; a simulator sets a Network)
    Config::connect_timeout   a dial's TCP connect, within handshake_timeout ; default 5 s, 1.5 s from ClusterConfig::new (one lost SYN, resent at 1 s, still connects) ; a connect past it fails the dial and marks the peer unreachable, and so does a redial after a silent session that has not connected by then
    axon_net::host_tcp() -> Option<HostTcp { wmem_max, rmem_max, congestion, slow_start_after_idle }>   this host's TCP limits, from /proc ; None off Linux and in a simulation
    .lanes: Lanes { control, live, bulk } ; .lanes.get(lane) | get_mut(lane) -> LaneConfig { limits: Limits, compression, max_bytes_per_sec: Option<u64>, notsent_lowat: Option<u32> }
      Control: max_frame 1 MiB, 2 MiB out and in ; Live: 1 MiB, 8 MiB ; Bulk: 8 MiB, 32 MiB ; no compression ; no cap ; lowat 128 KiB (TCP_NOTSENT_LOWAT, Linux; TcpHealth::notsent_lowat reads it back)
      max_bytes_per_sec: what one session of the lane writes, pings and pongs apart, from a bucket of write_batch_bytes ; SessionStats::paced the time it waited
    axon_net::connect(addr, lane, tls, cfg) ; establish(io, Role::Initiator(lane) | Role::Responder, cfg) ; Peer::lane   the initiator names the lane in its hello, the responder echoes it
    trait Network { connect(to, lane) -> Dial, bind(at) } ; AcceptLimits { max_connections 3072, max_handshakes 64, max_handshakes_per_ip 16 }
    axon_net::VERSIONS (7..=7) the wire versions this build speaks ; a session runs the highest both ends do, Peer::version, PeerStats::version ; ranges that do not meet: Negotiation::Version { ours, theirs }, handshake failure reason version ; from 1.0 a release speaks its own and the one before
  ClusterConfig::new(bind, transport, discovery)   .labels .tls .advertise .ownership .authorizer ; record.label(key) -> Option<&str> ; record.has_labels(&labels) every one, each value whole ; a label has one value: a node with two roles carries a label for each (runner=yes, control=yes)
  cluster.set_tls(Tls)?   new sessions and redials take the new certificates, sessions up keep theirs ; refused without TLS or when they would not pass as the node id
  cluster.set_authorizer(Option<Arc<dyn Authorizer>>)   each call, spawn and watch question that arrives after it is checked by the new one ; what was admitted finishes ; a stream the peer follows stops within its parent's StreamNodeConfig::downstream_expiry and a tick
  discovery: MemoryDiscovery::new() | StaticDiscovery::new(records)? | ConsulDiscovery::new(cfg)? (feature consul)
           | SeedDiscovery::start(SeedConfig::new(seeds, &transport) .timings).await?   seeds run SeedServer::bind(SeedServerConfig::new(addr, &transport) { tls, accept, timings }).await?
             SeedConfig::transport.connect_timeout   a seed dial's connect, 1 s from new ; across regions as the cluster's ; SeedDiscovery::start refuses a warm_up under it + 2 s
             SeedTimings { idle, idle_hold, closed_grace, warm_up }   default 4, 5, 2, 5 s ; the same on every seed and node: a node does not count a seed with others (SeedLinkStats state "timings", event seed_timings) ; change them with every node and seed stopped
             with seeds: cfg.heartbeat_interval = 500 ms, cfg.heartbeat_grace = 1 s (without an authority Cluster::start refuses more, Discovery::drops_after) ; discovery.set_seeds(list)? ; discovery.set_tls(tls)? ; server.set_tls(tls)? (refused only without TLS) ; .stats()
           Discovery::drops_after() -> Option<Duration>   how soon a peer can drop a quiet node: seeds idle_hold − 2 s, others None ; without an authority, Cluster::start refuses heartbeat_interval + 3/2 heartbeat_grace at or past it, ClusterError::Config ; Discovery::attach(&events) hands a provider the node's EventLog at start
           every provider and ownership authority answers .describe() -> ProviderInfo ; cluster.stats() carries both, and isolated, warming, quotas_starting (what /readyz checks), eager_waiting (keys of eager kinds due to start again here) and eager_asking (questions about a node the authority has not answered), and engine.versions (the wire versions this node speaks, lowest and highest) ; engine.peers: PeerStats { node, outgoing, lane, down_ms, .. } a row per session, and one per outgoing Live or Bulk lane with no session while Control is up, down_ms how long
  ownership: one MemoryLeases::new() shared by the nodes of one process, .authority() for each | ConsulLeases::new(cfg, node_id)? (feature consul)
           | PaxosLeases::new(PaxosConfig::new(acceptor_ids))?   a node on the list serves as an acceptor ; lease 6 s, at most 5 min
             .store = Some(Arc::new(OsFiles::new(dir)))   required on an acceptor (Cluster::start refuses it without, and reads it before the node announces itself), one directory each: who holds each key, synced before the acceptor answers a grant or release, unless its writes fail or hang past a round's patience ; a log of another acceptor list, lease or drift is skipped and removed, its highest epoch kept as the floor
             .warming() for a little over a lease after an acceptor starts, the first start too: a cluster started at once owns nothing until then ; in a rolling restart, restart the next acceptor after it
           trait OwnershipAuthority { acquire, renew, release, close, hold, attach(&Cluster) -> BoxFuture, describe, held_by(Holder::Node(id) | Holder::Lapsed, kinds), warming, can_grant } ; PaxosLeases refuses an acquire OwnershipError::Held(Some(node)), naming the holder ; it holds cluster.downgrade() -> WeakCluster, not a Cluster
  Cluster::start(&system, cfg).await?
  trait WireProtocol { Message, Reply, ID, SCHEMA, encode, decode_reply, const LANE: Lane = Lane::Live } ; trait ActorProtocol { Mailbox, encode_reply, decode }
    LANE: the session its calls, replies, parts and uploads take ; calls to one peer on one lane arrive in order ; publish::<P> and kind::<P> refuse Lane::Control, axon's own (membership, heartbeats, leases, round trips) ; .also::<Q>() serves Q on Q::LANE
    a Live or Bulk session dials the boot Control reaches, once it does ; one that fails fails only its calls, NotSent(Closed), Retry::Later: the peer stays reachable and keeps its keys
  reply.send(r) on the Reply<P::Reply> a request carries   (Err only for the reply, Codec or NotSent(Oversized): the caller gets NoReply; nothing else to do) ; reply.is_abandoned() ; reply.time_left() -> Option<Duration>, None without a deadline
    reply.max_payload()   the largest reply body the caller's session accepts
  reply.send_parts(stream) -> impl Future<Output = Result<(), E>>   stream: Stream<Item = Result<P::Reply, E>>, E: From<CallError> ; pulled only as the caller reads, each part at most max_payload() ; the stream's end ends the reply, an Err, a drop, a stop or a panic cuts it (the caller: NoReply) ; Ok when the caller has gone ; run it with cx.submit_future (a drain waits for it, a stop cuts it) or await it in a job ; to a plain request: Rejected(InvalidMessage) for both ; reply.send(r) to request_parts is one part
  reply.upload() -> Option<Upload>   the body of an upload, the first time ; None for a plain request ; upload.next().await -> Option<Result<Bytes, CallError>>, None once the caller's stream ended, Err(NoReply) where the caller cut it, OutcomeUnknown where the session failed, Timeout where no part came for request_timeout ; reading grants the caller credit (at most 4 MiB and one part unread) ; the answer may go at any point, and it, or a dropped Upload, stops the body ; an axon::Stream
  cluster.publish::<P>(&handle, name)? ; cluster.resolve::<P>(name).await? -> RemoteRef<P>
    publish the name again at another SCHEMA of P::ID to serve both side by side ; a node that serves the name but not the caller's schema: ClusterError::SchemaMismatch { wanted, served }, retry() Resolve
    cluster.unpublish(&remote)   nothing more is admitted through it once it returns
  cluster.resolve_all::<P>(name).await -> (Vec<RemoteRef<P>>, Vec<(String, ClusterError)>)   the nodes it could not ask, apart ; resolve_all_among(name, &placement) asks only the nodes with its required labels ; a node that does not answer a lookup: NotSent, Retry::Later (a NamedRef's and a KeyedRef's lookups too)
  cluster.resolve_on::<P>(node, name).await? -> RemoteRef<P>   the one that node publishes, where resolve finds several (Ambiguous)
  cluster.named::<P>(name) -> NamedRef<P>   .among(&placement) .tell .send .request .request_within(m, d) .request_until(m, deadline) .request_parts(m) .request_parts_within(m, d) .upload(m, parts) .upload_parts(m, parts), resolves again after Retry::Resolve
  cluster.nodes(&placement) -> Nodes   the available nodes that carry every label placement requires (this node too), read from the view at each call: no I/O, no task, the cluster held weakly
    nodes.ids() -> Vec<Arc<str>> in id order ; nodes.order(key) -> Vec<Arc<str>>   rendezvous: the same order on every node with the same view, and one leaving moves only the keys it led ; call one with cluster.resolve_on(node, name), all with resolve_all_among(name, &placement)
    nodes.changes() -> NodeChanges   an axon::Stream (cx.attach) of NodeChange::Joined(id) | Left(id): each member now, then each change ; None once the cluster shuts down
  remote.tell(m).await? | remote.send(m).await? | remote.request(m).await? | remote.request_within(m, d).await? | remote.request_until(m, deadline).await? (again after Retry::Later only)   (d travels: the far node drops it unrun past it)
  remote.request_parts(m).await? -> Parts<P> | remote.request_parts_within(m, d).await?   a reply larger than a frame, in parts: returns at the first part, its error as request's ; parts.next().await -> Option<Result<P::Reply, CallError>>, None once the far stream ended, Err(Timeout | OutcomeUnknown | NoReply | Codec) where it was cut ; at most 4 MiB and one part unread ; each part due within request_timeout (within d: the whole reply) ; dropping it stops the far side ; an axon::Stream, for cx.attach ; 64 at once on a session, then Rejected(Full)
  remote.upload(m, parts).await? -> P::Reply | remote.upload_parts(m, parts).await? -> Parts<P>   a request larger than a frame: parts a Stream<Item = Result<Bytes, CallError>>, each at most the far max_frame, pulled while the handler grants credit ; returns at the reply, which may come before the body ends ; each wait for credit or for room in the outbox, and the reply after the handler's last read, bounded by request_timeout (Timeout) ; a stream Err cuts it (the handler: NoReply) ; nothing resent ; upload_parts: the body from a task of its own (parts: Send + 'static), cut when the reply ends ; 64 bodies at once on a session ; wire version 6
  err.retry() -> Retry { Later, Resolve, IfIdempotent, Never }   on CallError and ClusterError
  raw endpoints, under the protocols: cluster.serve(name, protocol, schema, |peer, body, reply: Option<Reply>| -> Result<(), Rejection>)? -> id   on the session's read path, must not block ; cluster.unserve(id)
    cluster.endpoint(name, protocol, schema) .lane(lane) .serve(handler)? -> id   the lane callers send on ; Control only for names under axon. ; cluster.serve(..) is .lane(Live)
    cluster.request(node, name, protocol, schema, body).await? -> Bytes   on the lane the endpoint's node names, bounded by request_timeout ; request_to(node, incarnation, ..) refuses another boot of node unsent, StaleNode ; cluster.max_frame(lane) the largest body on that lane
    cluster.start_request_to(node, incarnation, ..).await? -> PendingReply   once the session admits it (within request_timeout ; Busy without a max_pending slot), so the next can follow at once, sent in that order ; PendingReply.await -> Result<Bytes, ClusterError>, due request_timeout after admission ; one call, counted and traced, holding its slot until it ends or is dropped ; request_to is start_request_to(..).await?.await
    cluster.round_trip(node) -> Option<Duration>   the shortest of the last four keepalive round trips on the Control session to node ; a session pings as it comes up, so None only until its first answer
    cluster.serve_control(service, AccessPolicy::{Deny | Nodes(set) | Cluster}, |ControlRequest { peer, body, max_response }| async { Ok(bytes) })? ; cluster.control(node, service, body).await? -> Bytes   management traffic, ClusterConfig::max_control_handlers at once
  cluster.set_metadata(key, Option<value>).await?   one entry of this node's discovery metadata, announced ; the cluster's own keys refused
  cluster.kind::<P>(kind) .execution(e) .mailbox(cfg) .idle(d) .policy(RestartPolicy) .eager(args, placement) .register(|cx, args| Ok(actor)).await?
    the factory: Fn(&SpawnContext, Bytes) -> Result<A, String> ; an Err refuses the start, Spawn(Failed) ; SpawnContext { kind, key: Option<String> (None for spawn), epoch: Option<u64> (under an authority ; a restart under .policy, or a start while the node still holds the lease, has the one before it), start: u64 (this start's number in the node's process, larger than any before it, from 1 after a restart: (epoch, start) grows with each start of a key), caller: Identity, cluster: WeakCluster (weak, as the cluster holds the factory) }
    .also::<Q>()   the same actors serve Q, another SCHEMA of P::ID ; advertised as kind/2+1 (P's own first), NodeRecord::kind_schemas(kind) ; keys go only to nodes serving the most of the kind's callers' schemas
    (eager: a gone node's keys restart on survivors without a caller, and a node restarts those it stopped itself for a lost lease or isolation ; needs PaxosLeases, or ConsulLeases given the node id)
  cluster.get_or_spawn::<P>(kind, key, args, &Placement::default().require(k, v)).await?   (P = Unaddressed: existence only ; a lease still held elsewhere is tried every HELD_RETRY, 200 ms, until operation_timeout)
    under an authority: an owner that is unreachable, leaving or SpawnRefusal::Isolated (no majority of the acceptors, can_grant) is passed and the next of the order asked, 3 nodes in all ; Held names the holder and the caller asks it ; the reference is to the node that answered ; the owner takes such a key back once it can
  cluster.watch_key(kind, key, &placement) -> KeyWatch   a Stream of Owner::Running { node, epoch } | Owner::Stopped ; .next().await
  cluster.keyed::<P>(kind, key, args, &placement) -> KeyedRef<P>   .tell .send .request .request_within(m, d) .request_until(m, deadline) .request_parts(m) .request_parts_within(m, d) .upload(m, parts) .upload_parts(m, parts), asks again after Retry::Resolve (a tell sees one once the far node says the actor is gone) ; .node() .epoch() of the kept reference, None before it
    .request_until(m, deadline: tokio::time::Instant) needs P::Message: Clone ; each try a request_within of what is left, up to request_timeout ; tries again after Later or Resolve, not after Timeout (it may have run) ; NamedRef's too
    Timeout is not Resolve: an owner reachable but too slow keeps the key and the reference ; the key moves once the owner is unreachable, leaves or loses its lease, or the actor stops
  cluster.keyed_refs::<P>(kind, &placement, max) -> KeyedRefs<P>   .get(key, args) -> KeyedRef<P> the one kept for key, made on its first get ; at most max (at least 2) kept, those of the max/2 keys got last always ; a clone shares them
  cluster.owner(kind, key, &placement) -> Option<Arc<str>>   the node get_or_spawn asks first, from this node's view ; a keyed actor is named kind/key on its node
  cluster.hosted(limit) -> Hosted { kinds: Vec<HostedKind { kind, eager, count, started, ended }>, keys: Vec<HostedKey { kind, key, epoch: Option<u64> }>, total }   what this node hosts, from memory ; the first limit keys in kind and key order ; started by KEYED_STARTS (call, handoff), ended by KEYED_ENDS (stopped, failed, lease_lost, isolated, moved)
  cluster.hosted_keys(kind, prefix, from: Option<&str>, limit) -> (Vec<HostedKey>, more: bool)   the keys of kind here that start with prefix, in key order from `from` on, O(log n + limit)
  cluster.view() ; cluster.wait_view(|view| ..).await? -> Arc<View> ; cluster.subscribe().next().await -> MembershipEvent (an axon::Stream too, for cx.attach) ; cluster.local_addr() ; cluster.record() ; cluster.events() (the system's EventLog: keyed actors acquired, lost, handed off, restarted eagerly ; lane_down and lane_up (node, lane): a Live or Bulk session lost and back while Control stays up, once each) ; cluster.clock() and cluster.tracer() (the system's) ; cluster.isolated() (starts refused, keys already running answered ; without an authority, or once a peer has dropped it, its keyed actors stop at once ; with one, each stops by its lease's fence ; it rejoins once heard, or vouched for by a peer, for a whole heartbeat_grace) ; cluster.warming() ; cluster.request_timeout()
  Quotas::start(&cluster, QuotaConfig { region, replicas_per_region: 8, capacity }).await? -> Quotas   every serving node ; advertised under metadata axon.quota
    Rate::new(name, max, window 1 s..=1 h) | Permits::new(name, max)   consts ; .with_max(m) a plan's maximum
    quotas.take(key, &[(rate, cost), ..]) -> Result<(), Denied>   all or nothing, never waits ; quotas.charge(key, &[(rate, cost)]) never refuses
    quotas.acquire(key, &permits) -> Result<Permit, Denied>   #[must_use], released on drop ; Denied::Limit { limit, retry_after } | Denied::Share { .. } (.limit() .retry_after() of either)
    quotas.stats() -> QuotaStats { entries, permits, rows, pulling, mismatched, fallback, .. } (and in ClusterStats.quotas) ; quotas.describe()
    cluster.quotas_starting()   a new or restarted replica still pulls the others' rows: /readyz waits for it

axon-stream (feature cluster)
  StreamNode::start(&cluster, StreamNodeConfig::new(upstream_label_tiers).upstream_for(namespace, tiers)).await?
    .relay_timings(namespace, RelayTimings { upstream_timeout, hold_down })   that namespace's keys ; cfg.core.timings the rest's, default 3 s and 10 s
    .lane(namespace, Lane::Bulk)   its pushes' lane, Live by default: Bulk for a namespace that moves much more than the node's calls ; interests go on Control, within .core.max_interest_bytes, which start lowers to its max_frame ; start refuses Control, and a max_push_bytes past the max_frame of a lane in use
    .limit(namespace, NamespaceLimits { subscriptions, ring_bytes, records_per_sec, bytes_per_sec, open_streams }) ; .quotas = Some(quotas)   on each node: subscriptions, ring_bytes ; across the cluster, and refused at start without quotas: the rest
      at a limit: Err(StreamError::QuotaExceeded { namespace, limit: NamespaceLimit, retry_after }) ; node.stats().hub.quota_refused by NAMESPACE_LIMITS
    .core.rings = vec![RingLimits { namespace, max_records, max_bytes }]   each stream's ring in that namespace, on every node carrying it, in place of .core.store's ; not ring_bytes, the namespace's total at its origins
    .core.max_push_window (64 MiB) bytes in pushes to one child unanswered, at least max_push_bytes ; .core.max_push_inflight (256 MiB) to every child together past each one's first push, at least max_push_window
      start records event host_tcp_limits where host_tcp() gives a wmem_max below max_push_window or an rmem_max below twice it ; .core.branch BranchLimits::BRANCH (32 MiB, 65536 records, 4096 events), half of it a stream's in flight to a child ; BranchLimits::default() (17 MiB, 65536, 4096), a subscription's limits, holds that, a push more
    node.stats().downstream[i]   a row per child and lane: .lane .channel .window_bytes .inflight_bytes .inflight_pushes .round_trip_us .delivery_rate .resent ; node.stats() .push_resent .push_skipped [duplicate, gap, retired] .push_capped
  key = StreamKey::new(namespace, name) (.namespace() .name())
  node.open_stream(&key, generation, DeliveryPolicy::ReplayRequired).await? ; node.publish(&key, Vec<Bytes>)?
    DeliveryPolicy::Compacted(CompactLimits { max_keys, max_bytes })   records keyed(key, value), an empty value removes the key ; split_keyed(&record) ; the origin keeps the latest per key, a publish past the limits is Err(StateFull) and changes nothing
  node.subscribe(&key, SubscribeOptions { start: Live | After(cursor) | First | SnapshotThenLive, .. })? ; subscription.next().await -> StreamEvent   on any node
    (`SnapshotThenLive`: Ready, StreamEvent::Snapshot(s) (s.through() s.records()), replay, CaughtUp, live ; End(Revoked) where Access::Snapshot is denied, End(SlowConsumer) where the state cannot fit)
    (`Live` follows the key: a restart or re-open is a new `Ready`, not an end, and each new generation arrives from its record 1 while its parent's ring holds it ; StreamEvent::OriginLost when no parent reaches the origin, the position kept ; a node carrying the key refuses one behind its ring with Err(HistoryUnavailable) and one of another generation with Err(GenerationMismatch), a relay waits for one ahead of it, one no ring on the path holds ends with ResetRequired(HistoryUnavailable))
    (`After`, `First` and `SnapshotThenLive` hold a position in one generation: an origin that resumes it from disk goes on with the next record ; a new generation ends them with ResetRequired(GenerationChanged), a close with End(StreamClosed) ; subscribe again)
  cfg.disk = Some(DiskConfig::new(Arc::new(OsFiles::new(dir)), vec![Retention { namespace, max_bytes, max_age }]))   .segment_bytes .max_unwritten_bytes ; the origin keeps those namespaces' streams on disk, one write out at a time, the next as soon as it lands with all that waits (no timer) ; one node a directory: a start while another keeps it, or a dropped one's write still lands, fails as in use
    publish returns the admitted cursor ; node.cursor(&key) the last durable one, all subscribers see ; node.wait_cursor(&key, cursor).await? -> Cursor   once cursor(&key) reaches it (synced on disk, at once without one) ; Err(UnknownStream) where this node does not originate the key, or once the open that admitted it ends first (close, restart, the node dropped) ; it waits through Disk(Full | Failed): bound it with reply.time_left() ; open_stream of the same generation after a crash resumes after it, ring and compacted state refilled ; an older one GenerationMismatch
    node.stored(&key) -> Option<Cursor>   the generation and last record the disk holds of a key not opened yet ; start and open_stream await their reads (in an actor, hand open_stream's future to cx.submit_future), and an open holds the node's other streams up only to install what it read
    Err(Disk(DiskError::Full | Failed)) while the last write failed, Err(Disk(Behind)) past max_unwritten_bytes ; node.stats().disk -> Option<DiskStats> { failing, refused, truncated, .. } ; axon::files::Files for another store

axon-testkit   (dev-dependency; virtual time, same seed = same trace)
  Simulator::new(SimConfig { seed, jitter, drop_every, duplicate_every, ordered, .. }) ; net.add_node(name)?
  net.send(from, to, payload)? ; net.step() -> Option<Delivery<T>> ; net.run(max, |net, delivery| ..)
  net.partition(&left, &right)? / heal() / set_link / set_node_down / crash_node ; net.advance_idle_to(t)? ; net.pick(range) the model's own seeded choice
  check_seeds(0..200, |seed| ..) -> Err(FailedSeed) names the lowest seed that panics ; seeds run on every core, at most available_parallelism at once in the process
  crashes(&files, &ops) -> each (n, MemoryFiles) a crash inside one write may leave ; crashed(&ops, n) -> Vec<FileOp> the ops of the n-th end ; crash_points(&ops) how many   for a store's unit tests: the ops before one that is not an append land whole, each file keeps its first bytes of the appends after it, a Replace lands whole or not
  eventually(what, async || ..).await   polls every 20 ms, panics after 10 s ; eventually_within(what, d, check).await -> Duration   for threaded tests on real time
  world::World::new(WorldConfig { seed, wire: Wire { latency, jitter, stall_every, stall, syn_stall, bandwidth }, .. }) ; w.add(name, |host| boot(host))   boot from host.runtime(), host.transport(auth), host.addr()
    w.spawn_on(name, |node| fut) -> Task ; w.join(task) | w.block_on(name, f) -> Result<T, Crashed> ; w.run_for(d) ; w.run_until(what, within, |w| ..) ; w.now() the World's time since it began
    w.read(name, |node| ..) -> Option<T>   runs f now as the node, no time passing: tokio::time::Instant::now() and unix_micros() read its clocks where its last step left them, random_u64 a copy of its stream ; None while it is down or booting
    w.crash(name, Crash::Process | Crash::Machine) ; w.restart(name) ; w.pick(range) ; w.log(line) ; w.trace_tail(n) ; w.digest() ; seeds(0..64) (SIM_SEED=n, SEEDS=a..b ; a seed replays only with RUSTFLAGS="--cfg tokio_unstable", as scripts/check.sh sim builds: elsewhere a failed seed that ran a World says so)
    w.pause(name, d) (its clock runs on) ; w.suspend(name, d) (its clocks stand still: it wakes inside its leases and fences) ; w.hold_actors(name, d) ; w.cost(name, per_turn) ; w.starve(name, one_in) ; w.drift(name, ppm) ; w.step_wall(name, ms)
    w.cut(&from, &to) ; w.partition(&a, &b) ; w.heal() ; w.wire(from, to, Wire) ; w.reset(a, b) ; w.refuse_accepts(name, on)
    w.cut_lane(&from, &to, lane) ; w.reset_lane(a, b, lane)   as cut and reset, for the connections of one Lane (the one Network::connect named) and its dials ; heal() ends them too
    a dial takes a round trip ; a SYN or its answer that draws a stall waits syn_stall (1 s) ; bandwidth (bytes a second, 0 none): each directed link one first-in, first-out bottleneck for data, shared by its connections
    host.disk() | w.disk(name) -> Disk   files kept across restarts: .append .write .sync .truncate .remove .read .list .set_capacity(Some(bytes)) (StorageFull past it) .free ; Crash::Machine keeps a seeded first part of each file's unsynced bytes (none, all or a torn tail, a trace line each), undoes a truncation or a write not synced since, and keeps a removal or Replace ; impl Files in OsFiles' order, a write taking DISK_SYNC for each file it syncs (a store's: host.disk().dir("leases") as an acceptor's, .dir("streams") as a stream origin's) ; .dir(name) -> Disk a directory of its own names on the same machine, capacity, crashes and panics ; .panic_writes(n) the next n writes panic, .panics_left()

axon-obs
  Telemetry::start(&system, &cluster, TelemetryConfig { access, .. }).await?          every node
  MetricsService::start(&system, &cluster, Arc::new(store), MetricsConfig { access, .. }).await?   one node ; pulls each node's events too (Request::Events -> EventPage)
    MetricsRequest::Events(EventQuery { from_ms, to_ms, limit, node, kind }) -> newest first ; MetricsQuery::events(&EventQuery)
    store: MemoryMetrics::new(max_bytes) (oldest batches go first) | ClickHouseProvider::connect(cfg).await? ; sink.usage() -> Option<SinkUsage> { tables: Vec<TableUsage>, free_bytes, .. }, per sink in MetricsHealth.sink_usage with growth_bytes_per_day and full_in_ms, and on Settings' store cards (ClickHouse asked at most once a minute) ; MetricsHealth.collected_ago_ms on the collector's own clock
  Dashboard::start(&system, &cluster, DashboardConfig { bind, users: vec![User { name, hash }], .. }).await?   feature dashboard
    hash_password(p)? an argon2id PHC string ; .session SessionConfig { idle 30 min, max 12 h, key } ; .trusted_proxies ; .tls Some(Arc<ServerConfig>) ; .redact
    dashboard.sign_out_everyone(key)? refuses every cookie signed before ; dashboard.set_tls(config)? new handshakes only
  Health::start(&system, &cluster, bind).await? .address()   /healthz and /readyz, 200 or 503, no credentials ; a Dashboard serves them too ; feature dashboard
    health.serve_metrics(&telemetry)   /metrics: the last sample in the Prometheus text format, for a stock Prometheus to scrape ; 404 until called
```

## Rules that prevent most mistakes

1. Handlers are synchronous and must not block. Slow or async work goes through `cx.submit_io` / `cx.submit_future`
   and comes back as a message.
2. Build the `ActorSystem` inside a tokio runtime and end with `system.shutdown().await`. Dropping it is a crash, not
   a leave.
3. `tell` never waits and fails with `Full`. Use `send` for backpressure, `ask` / `request` for an answer. Remote
   `tell` returning `Ok` says nothing about the far side.
4. Integer literals need a suffix: `handle.tell(1u64)`, because `tell` takes `impl Into<M>`.
5. A supervised actor restarts with fresh state and an empty mailbox. Nothing is replayed.
6. Remote messages are bytes you encode yourself. Keep `ID` fixed per contract and bump `SCHEMA` on breaking changes.
7. A `RemoteRef` never retargets. `err.retry()` says what to do; on `Retry::Resolve`, resolve again (a `KeyedRef`
   or `NamedRef` does it for you on its next call).
8. Without TLS you must set `transport.allow_plaintext = true`. Use that on loopback only.
9. `TelemetryConfig::access` and `MetricsConfig::access` default to `Deny`. Set them or the console is empty.
10. Use the same `args` and `placement` for a keyed actor everywhere, and under `PaxosLeases` the same acceptors,
    `lease` and `drift` on every node: a node that differs is refused by the acceptors, and the keys placed on it run
    elsewhere, or wait, until it is fixed.
11. Every queue is bounded. Do not look for an unbounded option; handle the refusal.
12. Quotas are approximate and never wait. Call `take` after authentication (a key new to a node gets up to the
    fallback in its first second), and use the same limits and `replicas_per_region` on every node, a name for one
    limit only: where maxima differ, the newest report's wins and `stats().mismatched` counts it. Move a `Permit` into what it admits; `let _ =` drops it at once.

[actors]: guide/actors.md
[cluster]: guide/cluster.md
[streams]: guide/streams.md
[observability]: guide/observability.md
[telemetry]: guide/telemetry.md
[dashboard]: guide/dashboard.md
[tracing]: guide/tracing.md
[simulating]: guide/cluster.md#simulating-a-cluster
[quotas]: guide/cluster.md#quotas
[keyed-disk]: guide/cluster.md#a-keys-state-on-disk
[gateway]: guide/streams.md#a-websocket-gateway
[benchmarks]: guide/benchmarks.md
