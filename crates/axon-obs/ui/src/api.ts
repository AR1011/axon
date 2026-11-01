export interface Histogram {
  buckets: number[];
  count: number;
  sum_ns: number;
}

/** A histogram as axon sends it: the `counts` of buckets `first..`; `expand` makes a `Histogram` where one is drawn. */
export interface SparseHistogram {
  first: number;
  counts: number[];
  count: number;
  sum_ns: number;
}

export interface TcpHealth {
  rtt_ns: number;
  rtt_var_ns: number;
  rto_ns: number;
  cwnd_bytes: number;
  mss: number;
  retransmitted_segments: number | null;
  retransmitted_bytes: number | null;
  received_out_of_order_bytes: number | null;
  lost_segments: number | null;
  unacked_segments: number | null;
  send_buffer_bytes: number | null;
}

export interface SessionStats {
  tx_frames: number;
  tx_wire_bytes: number;
  tx_payload_bytes: number;
  tx_compressed_frames: number;
  tx_batches: number;
  rx_frames: number;
  rx_wire_bytes: number;
  rx_payload_bytes: number;
  rx_compressed_frames: number;
  pings: number;
  pongs: number;
  rtt_ns: number;
  outbound_queued_frames: number;
  outbound_reserved_bytes: number;
  inbound_queued_frames: number;
  inbound_reserved_bytes: number;
  age_ns: number;
  /** How long the writer waited for the lane's rate cap. */
  paced_ns: number;
  closed: string | null;
  tx_rejected?: number[];
  tcp?: TcpHealth | null;
}

/** `api/connection`: what the live overview leaves out of a session. */
export interface ConnectionTimings {
  timings: { sample_every: number; queue: SparseHistogram; encode: SparseHistogram; write: SparseHistogram; decode: SparseHistogram; rtt: SparseHistogram };
  request_rtt: SparseHistogram;
}

export interface Peer {
  node: string;
  incarnation: string;
  outgoing: boolean;
  pending: number;
  calls: number;
  session: SessionStats;
  /** The wire version the session runs. */
  version: number;
  lane: Lane;
  /** Set on a Live or Bulk lane with no session while Control is up: how long it has been down. */
  down_ms: number | null;
}

/** `axon::Lane`: each a connection of its own to a peer. */
export const LANES = ["control", "live", "bulk"] as const;
export type Lane = (typeof LANES)[number];

/** The names of `inbound_rejected`, by index (axon_cluster::REJECTION_NAMES). */
export const INBOUND_REJECTIONS = ["unknown_actor", "schema_mismatch", "full", "forbidden", "invalid_message", "stopped"];
/** The names of `listener.handshake_failures`, by index (axon_net::HANDSHAKE_FAILURE_REASONS). */
export const HANDSHAKE_FAILURES = ["auth", "negotiation", "timeout", "other", "version"];

/** `ClusterStats.quotas`, null on a node without `Quotas`. */
export interface QuotaStats {
  entries: number;
  permits: number;
  keys: number;
  rows: number;
  pulling: boolean;
  mismatched: number;
  denied_limit: number;
  denied_share: number;
  fallback: number;
  reports: number;
  report_failures: number;
}

/** `RelayStats.disk`, null on a node that keeps no stream on disk. */
export interface DiskStats {
  streams: number;
  bytes: number;
  unwritten_bytes: number;
  writes: number;
  write_failures: number;
  refused: number;
  failing: "Full" | "Failed" | "Behind" | null;
  error: string | null;
  truncated: number;
  truncated_bytes: number;
}

export interface Timings {
  sample_every: number;
  mailbox_wait: SparseHistogram;
  handler: SparseHistogram;
  turn: SparseHistogram;
  timer_lag: SparseHistogram;
  messages: { label: string; handler: SparseHistogram }[];
}

export interface Diagnostics {
  mailbox_depth: number;
  mailbox_bytes: number;
  waiters: number;
  requests: number;
  jobs: number;
  timers: number;
  admitted: number;
  rejected: number[];
  processed: number;
  turns: number;
  budget_overruns: number;
  asks: number;
  ask_timeouts: number;
  /** A family's messages that became dead letters as its generations ended; 0 for a live actor of its own. */
  dead_letters: number;
  oldest_message_age_ns: number | null;
  timings: Timings | null;
}

export interface ActorId {
  runtime: number;
  slot: number;
  generation: number;
}

export interface Actor {
  incarnation: ActorId;
  name: string | null;
  actor_type: string;
  phase: string;
  restarts: number;
  /** `unix_micros` when the actor, or its supervised family, was spawned. */
  started_us: number;
  /** `unix_micros` at the family's latest restart; null before one. */
  restarted_us: number | null;
  diagnostics: Diagnostics;
}

export interface InventoryActor extends Actor {
  node_id: string;
  node_incarnation: string;
}

export interface ActorType {
  actor_type: string;
  live: number;
  mailbox_depth: number;
  mailbox_depth_max: number;
  mailbox_bytes: number;
  oldest_message_age_ns: number;
  waiters: number;
  requests: number;
  totals: {
    actors: number;
    restarts: number;
    admitted: number;
    rejected: number[];
    processed: number;
    asks: number;
    ask_timeouts: number;
    budget_overruns: number;
    timings: Timings | null;
  };
}

export interface NodeRecord {
  id: string;
  incarnation: string;
  address: string;
  metadata: Record<string, string>;
}

/** What a discovery provider or an ownership authority reports about itself. */
export interface ProviderInfo {
  kind: string;
  settings: [string, string][];
  stats: [string, string][];
  links: ProviderLink[];
}

export interface ProviderLink {
  target: string;
  identity: string | null;
  state: string;
  since_ns: number;
  error: string | null;
}

/** A child this node pushes to (axon_stream::DownstreamStats). */
export interface Downstream {
  node: string;
  lane: Lane;
  branches: number;
  queued_bytes: number;
  pushed_bytes: number;
  pushed_records: number;
  /** From 1 for each boot of the child. */
  channel: number;
  window_bytes: number;
  inflight_bytes: number;
  inflight_pushes: number;
  round_trip_us: number | null;
  /** Bytes a second, the largest recent sample. */
  delivery_rate: number;
  resent: number;
}

export interface StreamStats {
  hub: {
    streams: number;
    sessions: number;
    subscriptions: number;
    recovering: number;
    demand_keys: number;
    queued_bytes: number;
    queued_records: number;
    published_batches: number;
    published_records: number;
    published_bytes: number;
    duplicate_records: number;
    publish_rejected: number;
    delivered_records: number;
    delivered_bytes: number;
    replayed_records: number;
    recovery_blocked: number;
    coalesced: number;
    resets: number[];
    ends: number[];
    quota_refused: number[];
  };
  originated: number;
  feeds: number;
  orphaned: number;
  parents: { node: string; keys: number; joining: number }[];
  downstream: Downstream[];
  parents_lost: number;
  switches: number;
  refusals: number[];
  push_bytes_in: number;
  push_bytes_out: number;
  push_resent: number;
  /** Pushes this node, as a child, did not apply, by `PUSH_SKIPPED`. */
  push_skipped: number[];
  push_capped: number;
  local_resets: number;
  interests_sent: number;
  interests_failed: number;
  interest_keys: number;
  resyncs: number;
  retained_bytes: number;
  disk: DiskStats | null;
}

export interface StreamInfo {
  namespace: string;
  name: string;
  generation: number | null;
  sequence: number | null;
  origin: boolean;
  parent: string | null;
  joining: string | null;
  /** The nodes the stream crosses to reach this one, origin first. */
  path: string[];
  /** Parents left alone for now, each with its answer. */
  refused: string[];
  local_subscribers: number;
  downstream_branches: number;
}

export interface StreamEventInfo {
  time_ms: number;
  kind: string;
  stream: string;
  detail: string;
}

/** `GET api/streams`: fetched on demand, never part of the live overview. */
export interface StreamDetails {
  nodes: Record<string, { streams: StreamInfo[]; total: number; events: StreamEventInfo[] }>;
  errors: Record<string, string>;
}

export interface WorkerStats {
  turns: number;
  busy_ns: number;
  parks: number;
  queued: number;
}

export interface Snapshot {
  node_id: string;
  incarnation: string;
  timestamp_ms: number;
  uptime_ms: number;
  lifecycle: { phase: string; reason: string | null };
  system: { started: number; stopped: number; failed: number; dead_letters: number };
  runtime: { actors: number; workers: WorkerStats[]; pools?: { name: string; workers: WorkerStats[] }[] };
  worker_utilisation: number[];
  /** Absent from a node that predates pools. */
  pool_utilisation?: number[][];
  actor_types: ActorType[];
  actor_total: number;
  cluster: {
    engine: {
      calls: number;
      succeeded: number;
      rejected: number;
      timed_out: number;
      unknown_outcomes: number;
      failed: number;
      abandoned: number;
      inbound_calls: number;
      inbound_rejected: number[];
      inbound_expired: number;
      request_rtt: SparseHistogram;
      connects: number;
      dial_failures: number;
      pending: number;
      endpoints: number;
      listener: { accepted: number; accept_errors: number; rejected_busy: number; rejected_per_ip: number; handshake_failures: number[] };
      peers: Peer[];
      /** The lowest and highest wire version the node speaks; absent from a node that predates it. */
      versions?: [number, number];
    };
    members: number;
    unreachable: number;
    joined: number;
    left: number;
    replaced: number;
    became_unreachable: number;
    discovery_errors: number;
    heartbeat_failures: number;
    last_heartbeat_error: string | null;
    spawned: number;
    spawn_requests: number;
    spawn_refused: number;
    handoffs: number;
    handoffs_failed: number;
    keyed_failed: number;
    leases_lost: number;
    isolations: number;
    isolated: boolean;
    /** A restarted Paxos acceptor here still refuses grants: `/readyz` says `warming`. Absent from a node that predates it. */
    warming?: boolean;
    /** Its quota replica still pulls the others' rows after its start: `/readyz` says `starting`. */
    quotas_starting?: boolean;
    discovery: ProviderInfo;
    ownership: ProviderInfo | null;
    quotas: QuotaStats | null;
  };
  /** This node's own view of the membership; the console's is `Overview.nodes`. */
  members: { record: NodeRecord; reachable: boolean }[];
  /** The kinds registered on the node, each with the keys it runs and its starts (`call`, `handoff`) and ends (`stopped`, `failed`, `lease_lost`, `isolated`, `moved`); absent from a node that predates it. A count or a key list is absent from an older node. `keys` holds the first `TelemetryConfig::snapshot_keys` keys, none by default: api/keys pages them. */
  hosted?: { kinds: { kind: string; eager: boolean; count?: number; started?: number[]; ended?: number[] }[]; keys: { kind: string; key: string; epoch: number | null }[]; total: number };
  streams: StreamStats | null;
  process: { pid: number; cpu_percent: number | null; rss_bytes: number; virtual_bytes: number; read_bytes: number; written_bytes: number } | null;
  host: {
    cpu_percent: number | null;
    cpus: number;
    total_memory_bytes: number;
    available_memory_bytes: number;
    received_bytes: number;
    transmitted_bytes: number;
  } | null;
  notices: { code: string; message: string }[];
}

export interface NodeView {
  node: NodeRecord;
  reachable: boolean;
  snapshot: Snapshot | null;
  error: string | null;
  /** Set by this console, never by the dashboard: when the node dropped out of the membership. */
  leftAt?: number;
  /** Set with `leftAt`: why it left, or null when it went without draining, as after a crash or a cut. */
  leaveReason?: string | null;
}

export interface ClusterEvent {
  id: string;
  time_ms: number;
  node_id: string;
  kind: string;
  /** A keyed actor's `kind/key`, a supervised family, a stream; empty for a node's own. */
  subject: string;
  detail: string;
}

export interface HistorySource {
  node: string;
  storage: string;
  capabilities: { numeric_history: boolean; histograms: boolean; events: boolean; max_points: number } | null;
  collector: {
    cycles: number;
    /** On the collector's own clock, so a browser or node whose wall clock is off cannot skew it. */
    collected_ago_ms: number | null;
    last_cycle_duration_ms: number;
    stored_batches: number;
    stored_spans: number;
    rejected_batches: number;
    collection_gaps: number;
    source_resets: number;
    write_errors: number;
    dropped_events: number;
    pending_events: number;
    sink_errors: Record<string, string>;
    node_errors: Record<string, string>;
    /** By `index:name`, for each sink that can say what it holds. */
    sink_usage?: Record<string, SinkUsage>;
  } | null;
  error: string | null;
}

export interface TableUsage {
  name: string;
  rows: number;
  held_bytes: number;
  /** Over `held_bytes`, the compression ratio. */
  uncompressed_bytes: number;
  parts: number;
  first_ms: number | null;
  last_ms: number | null;
  retention_days: number | null;
}

export interface SinkUsage {
  /** 0 where the sink sets no bound of its own. */
  max_bytes: number;
  held_bytes: number;
  held_batches: number;
  evicted_batches: number;
  tables: TableUsage[];
  free_bytes: number | null;
  growth_bytes_per_day: number | null;
  /** Null when retention levels the store off first, or it does not grow. */
  full_in_ms: number | null;
}

export interface Overview {
  timestamp_ms: number;
  build: string;
  observer: string;
  nodes: NodeView[];
  history: { available: boolean; node: string | null; pinned: boolean; sources: HistorySource[]; error: string | null };
  events: ClusterEvent[];
  discovery_error: string | null;
}

/** One stored series, or a sum of them (`summed` says how many), over its result's steps. */
export interface Series {
  node_id: string;
  incarnation: string;
  labels: Record<string, string>;
  kind: string;
  unit: string;
  summed?: number;
  /** Per step: a gauge's value, a counter's gain, a histogram's count gained; null where unknown. */
  values: (number | null)[];
  /** A histogram's gain per step. */
  gains?: (SparseHistogram | null)[];
}

/** Step `i` of every series starts at `from + i * step`. */
export interface HistoryResult {
  from: number;
  step: number;
  count: number;
  series: Series[];
  truncated: boolean;
  /** The steps before it no longer change: a refresh asks again from here. */
  settled: number;
}

export interface HistoryAnswer {
  source: string;
  results: { metric: string; result?: HistoryResult; error?: string }[];
}

/** `api/series`: the headers of a metric's series, without their values. */
export interface SeriesList {
  source: string;
  series: Pick<Series, "node_id" | "incarnation" | "labels" | "kind" | "unit">[];
  truncated: boolean;
}

export interface InventoryPage {
  actors: InventoryActor[];
  total: number;
  offset: number;
  limit: number;
  truncated: boolean;
  errors: Record<string, string>;
  /** When the server gathered these rows: answers from its short cache repeat it. */
  timestamp_ms: number;
}

export interface ActorPage {
  actors: Actor[];
  total: number;
  truncated: boolean;
}

export interface SpanRecord {
  trace: string;
  span: string;
  /** Empty on the first span of a trace. */
  parent: string;
  kind: "handle" | "call" | "stream";
  name: string;
  node_id: string;
  /** `runtime:slot:generation`, or empty. */
  actor: string;
  /** Empty when the actor had no name, or had stopped by the time the span was read. */
  actor_name: string;
  actor_type: string;
  peer: string;
  start_us: number;
  wait_ns: number;
  busy_ns: number;
  failed: boolean;
}

export interface TraceSummary {
  trace: string;
  start_us: number;
  end_us: number;
  spans: number;
  failed: boolean;
  nodes: string[];
  root: string;
  root_kind: string;
  root_actor_name: string;
  root_peer: string;
  root_actor: string;
  root_node: string;
}

export interface TraceList {
  /** `live`: no store keeps traces, so this is what the nodes still hold. */
  source: "store" | "live";
  store_error?: string;
  traces: TraceSummary[];
  errors: Record<string, string>;
}

export interface DeadLetterRecord {
  node_id: string;
  seq: number;
  at_us: number;
  reason: string;
  /** The message label, or `tell` for one another node sent. */
  label: string;
  actor: string;
  actor_type: string;
  /** The actor's registered name, or the endpoint a remote message was for. */
  target: string;
  peer: string;
  trace: string;
  count: number;
}

export interface DeadLetterList {
  letters: DeadLetterRecord[];
  errors: Record<string, string>;
}

export interface TraceDetail {
  spans: SpanRecord[];
  errors: Record<string, string>;
}

/** `api/key`: the node the console's view places a key on, null when none matches, and what that node runs for it. */
export interface KeyLookup {
  owner: string | null;
  running: boolean;
  epoch: number | null;
}

export const base = typeof document === "undefined" ? "/" : new URL(".", document.baseURI).pathname;

export const actorKey = (id: ActorId) => `${id.runtime}:${id.slot}:${id.generation}`;

type Params = Record<string, string | number | boolean | null | undefined>;

export function apiUrl(path: string, params: Params = {}): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) if (value != null && value !== "") query.set(key, String(value));
  const text = query.toString();
  return `${base}api/${path}${text ? `?${text}` : ""}`;
}

let leaving = false;
/** Aborted on sign-out, so that no answer still on its way sets the session cookie again. */
const session = new AbortController();
export const signedOut = session.signal;

export async function signOut() {
  session.abort();
  await fetch(`${base}logout`, { method: "POST", credentials: "same-origin", referrerPolicy: "same-origin" }).catch(() => null);
  location.assign(`${base}login`);
}

/** Goes to the login page once, and back to this page after the sign-in. */
export function signIn() {
  if (leaving) return;
  leaving = true;
  const here = location.pathname.startsWith(base) ? location.pathname.slice(base.length) : "";
  location.assign(`${base}login?next=${encodeURIComponent(here + location.search)}`);
}

/** A page of api/keys: rows in key, then node, order; the next page starts after the last row. */
export type KeysPage = { keys: { key: string; node: string; epoch: number | null }[]; more: boolean; errors: Record<string, string> };

export async function getJson<T>(path: string, params: Params = {}, signal?: AbortSignal): Promise<T> {
  const timeout = AbortSignal.timeout(10_000);
  const response = await fetch(apiUrl(path, params), {
    credentials: "same-origin",
    signal: AbortSignal.any(signal ? [signal, timeout, signedOut] : [timeout, signedOut]),
  });
  if (response.status === 401) {
    signIn();
    throw new Error("Signed out");
  }
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    const message = body?.error?.message ?? body?.error ?? body?.message;
    throw new Error(typeof message === "string" ? message : `Request failed (${response.status})`);
  }
  return response.json();
}

export function validOverview(value: unknown): value is Overview {
  const o = value as Overview;
  return (
    !!o &&
    Array.isArray(o.nodes) &&
    Array.isArray(o.events) &&
    !!o.history &&
    Number.isFinite(o.timestamp_ms) &&
    o.nodes.every(
      (n) =>
        typeof n?.node?.id === "string" &&
        (n.snapshot == null ||
          (Array.isArray(n.snapshot.cluster?.engine?.peers) &&
            typeof n.snapshot.lifecycle?.phase === "string" &&
            Number.isFinite(n.snapshot.timestamp_ms))),
    )
  );
}
