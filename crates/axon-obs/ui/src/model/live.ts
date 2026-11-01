import type { ClusterEvent, InventoryActor, Lane, NodeView, Overview, Peer } from "../api";
import { actorKey, HANDSHAKE_FAILURES } from "../api";
import { sum } from "./format";

export const STALE_MS = 7000;
export const GAP_MS = 5000;

export const direction = (peer: Peer) => (peer.outgoing ? "out" : "in");
export const connectionKey = (node: string, incarnation: string, peer: Peer) =>
  `${node}|${incarnation}|${peer.node}|${peer.incarnation}|${direction(peer)}|${peer.lane}`;
/** A lane's own row while it has no session; `down_ms` says how long. */
export const isDown = (peer: Peer) => peer.down_ms != null;
export const DOWN = "This connection is down. Calls on it fail and try again later. The peer stays in the cluster.";
export const LANE_NAMES: Record<Lane, string> = { control: "Control", live: "Live", bulk: "Bulk" };
export const LANE_TIP = "The traffic class of this connection.";

export interface ConnSample {
  t: number;
  tx: number | null;
  rx: number | null;
  frames: number | null;
  frameBytes: number | null;
  rtt: number | null;
}

interface ConnState {
  samples: ConnSample[];
  last: { t: number; tx: number; rx: number; frames: number; payload: number };
}

const delta = (now: number, before: number, seconds: number) => (now >= before ? (now - before) / seconds : null);

/** Per-connection rates derived from consecutive snapshots, by the source clock. */
export class ConnSeries {
  static readonly POINTS = 180;
  static readonly CONNECTIONS = 200;
  private byKey = new Map<string, ConnState>();

  append(overview: Overview) {
    const seen = new Set<string>();
    for (const { node, snapshot } of overview.nodes) {
      if (!snapshot) continue;
      for (const peer of snapshot.cluster.engine.peers) {
        if (isDown(peer)) continue;
        const key = connectionKey(node.id, snapshot.incarnation, peer);
        seen.add(key);
        this.observe(key, snapshot.timestamp_ms, peer);
      }
    }
    for (const key of this.byKey.keys()) if (!seen.has(key)) this.byKey.delete(key);
  }

  private observe(key: string, t: number, peer: Peer) {
    const s = peer.session;
    const now = { t, tx: s.tx_wire_bytes, rx: s.rx_wire_bytes, frames: s.tx_frames + s.rx_frames, payload: s.tx_payload_bytes + s.rx_payload_bytes };
    const state = this.byKey.get(key);
    if (!state) {
      if (this.byKey.size < ConnSeries.CONNECTIONS) this.byKey.set(key, { samples: [], last: now });
      return;
    }
    const before = state.last;
    if (t === before.t) return;
    state.last = now;
    if (t < before.t) {
      state.samples = [];
      return;
    }
    const seconds = (t - before.t) / 1000;
    const fresh = t - before.t <= GAP_MS;
    const frames = fresh ? delta(now.frames, before.frames, seconds) : null;
    const payload = fresh ? delta(now.payload, before.payload, seconds) : null;
    if (!fresh && state.samples.length) state.samples.push({ t: before.t + 1, tx: null, rx: null, frames: null, frameBytes: null, rtt: null });
    state.samples.push({
      t,
      tx: fresh ? delta(now.tx, before.tx, seconds) : null,
      rx: fresh ? delta(now.rx, before.rx, seconds) : null,
      frames,
      frameBytes: frames && payload != null ? payload / frames : null,
      rtt: s.rtt_ns > 0 ? s.rtt_ns : null,
    });
    if (state.samples.length > ConnSeries.POINTS) state.samples.splice(0, state.samples.length - ConnSeries.POINTS);
  }

  samples(key: string): ConnSample[] {
    return this.byKey.get(key)?.samples ?? [];
  }

  /** The newest sample, unless it is older than the staleness threshold. */
  latest(key: string, now: number): ConnSample | null {
    const last = this.byKey.get(key)?.samples.at(-1);
    return last && now - last.t <= STALE_MS ? last : null;
  }
}

export interface NodeRow {
  t: number;
  cpu: number | null;
  rss: number | null;
  actors: number;
  sent: number;
  received: number;
  read: number | null;
  written: number | null;
  queued: number;
  hostCpu: number | null;
  hostAvailable: number | null;
  utilisation: number | null;
  mailbox: number;
  processed: number;
  deadLetters: number;
  calls: number;
  /** Peers the listener turned away, busy, per IP or at the handshake. */
  refused: number;
  restarts: number;
  mailboxRejected: number;
  leasesLost: number;
  handoffsFailed: number;
  shareDenied: number;
  limitDenied: number;
  /** Handshakes refused because the peer spoke no wire version this node does. */
  versionRefused: number;
  /** Stream requests refused at a namespace's limit. */
  quotaRefused: number;
  /** The stream disk's error while its writes fail, else null. */
  diskFailing: string | null;
}

/** The fields of a row a chart or a rate can read. */
export type Counter = { [K in keyof NodeRow]: NodeRow[K] extends number | null ? K : never }[keyof NodeRow];

export function nodeRow({ snapshot: s }: NodeView): NodeRow | null {
  if (!s) return null;
  const peers = s.cluster.engine.peers;
  const onPeers = (pick: (p: Peer) => number) => sum(peers.map(pick));
  const u = s.worker_utilisation;
  const listener = s.cluster.engine.listener;
  return {
    t: s.timestamp_ms,
    cpu: s.process?.cpu_percent ?? null,
    rss: s.process?.rss_bytes ?? null,
    actors: s.actor_total,
    sent: onPeers((p) => p.session.tx_wire_bytes),
    received: onPeers((p) => p.session.rx_wire_bytes),
    read: s.process?.read_bytes ?? null,
    written: s.process?.written_bytes ?? null,
    queued: onPeers((p) => p.session.outbound_reserved_bytes),
    hostCpu: s.host?.cpu_percent ?? null,
    hostAvailable: s.host?.available_memory_bytes ?? null,
    utilisation: u.length ? sum(u) / u.length : null,
    mailbox: sum(s.actor_types.map((t) => t.mailbox_depth)),
    processed: sum(s.actor_types.map((t) => t.totals.processed)),
    deadLetters: s.system.dead_letters,
    calls: s.cluster.engine.calls,
    refused: listener.rejected_busy + listener.rejected_per_ip + sum(listener.handshake_failures),
    restarts: sum(s.actor_types.map((t) => t.totals.restarts)),
    mailboxRejected: sum(s.actor_types.flatMap((t) => t.totals.rejected)),
    leasesLost: s.cluster.leases_lost,
    handoffsFailed: s.cluster.handoffs_failed,
    shareDenied: s.cluster.quotas?.denied_share ?? 0,
    limitDenied: s.cluster.quotas?.denied_limit ?? 0,
    versionRefused: listener.handshake_failures[HANDSHAKE_FAILURES.indexOf("version")] ?? 0,
    quotaRefused: sum(s.streams?.hub.quota_refused),
    diskFailing: s.streams?.disk?.failing ? [s.streams.disk.failing, s.streams.disk.error].filter(Boolean).join(": ") : null,
  };
}

/**
 * How much a counter grew over the last `windowMs` of rows (`span`, shorter while the rows are), and since when it has
 * kept growing with no quiet window between; `open` when the rows begin less than a quiet window before that, so the
 * run may have started earlier. Null when it did not grow.
 */
export function growth(rows: NodeRow[], pick: (r: NodeRow) => number, windowMs = 60_000): { n: number; total: number; since: number; open: boolean; span: number } | null {
  const last = rows.at(-1);
  if (!last) return null;
  let [since, grew] = [last.t, -Infinity];
  for (let i = 1; i < rows.length; i++) {
    if (!(pick(rows[i]) > pick(rows[i - 1]))) continue;
    if (rows[i].t - grew > windowMs) since = rows[i].t;
    grew = rows[i].t;
  }
  const from = rows[Math.max(0, rows.findLastIndex((r) => r.t <= last.t - windowMs))];
  const n = pick(last) - pick(from);
  return n > 0 ? { n, total: pick(last), since, open: since - rows[0].t <= windowMs, span: last.t - from.t } : null;
}

/** Since when the newest rows have all passed `test`, or null when the newest does not. */
export function holding(rows: NodeRow[], test: (r: NodeRow) => boolean): number | null {
  const i = rows.findLastIndex((r) => !test(r));
  return i === rows.length - 1 ? null : rows[i + 1].t;
}

interface Boot {
  incarnation: string;
  rows: NodeRow[];
}

/** Live rows per node, fenced by incarnation: a restart keeps the boot before it apart, a clock rollback clears them. A node that left keeps its rows a while, for its page. */
export class NodeBuffer {
  static readonly POINTS = 600;
  private byNode = new Map<string, { current: Boot; earlier?: Boot }>();

  append(overview: Overview) {
    for (const [id, state] of this.byNode) if ((state.current.rows.at(-1)?.t ?? 0) < overview.timestamp_ms - DEPARTED_MS) this.byNode.delete(id);
    for (const view of overview.nodes) {
      const row = nodeRow(view);
      if (!row || !view.snapshot) continue;
      const incarnation = view.snapshot.incarnation;
      let state = this.byNode.get(view.node.id);
      const last = state?.current.rows.at(-1);
      if (!state || (state.current.incarnation === incarnation && last && row.t < last.t)) {
        state = { current: { incarnation, rows: [] } };
        this.byNode.set(view.node.id, state);
      } else if (state.current.incarnation !== incarnation) {
        state.earlier = state.current.rows.length ? state.current : state.earlier;
        state.current = { incarnation, rows: [] };
      }
      const rows = state.current.rows;
      if (rows.at(-1)?.t === row.t) continue;
      rows.push(row);
      if (rows.length > NodeBuffer.POINTS) rows.splice(0, rows.length - NodeBuffer.POINTS);
    }
  }

  rows(node: string, incarnation?: string): NodeRow[] {
    const state = this.byNode.get(node);
    const boot = !incarnation || state?.current.incarnation === incarnation ? state?.current : state?.earlier?.incarnation === incarnation ? state.earlier : undefined;
    return boot?.rows ?? [];
  }

  /** The boot before the current one, while its rows are kept. */
  earlier(node: string): Boot | undefined {
    return this.byNode.get(node)?.earlier;
  }
}

export const DEPARTED_MS = 900_000;

/** A `left` event's detail: the leave reason, if one was given, then the boot it ended. */
const LEFT = /^(?:(.*) · )?incarnation ([0-9a-f]+)$/;

/**
 * Nodes that left in the last 15 minutes, newest first. One seen leaving keeps its last report; one known only from a
 * `left` event, as on a fresh load, is a bare record. `leaveReason` is null for a node that went without draining:
 * a crash, a kill or a cut, not a leave.
 */
export function departures(kept: NodeView[], previous: NodeView[], now: Overview): NodeView[] {
  const present = new Set(now.nodes.map((n) => n.node.id));
  const left = new Map<string, ClusterEvent>();
  for (const e of now.events) if (e.kind === "left" && !present.has(e.node_id) && e.time_ms > (left.get(e.node_id)?.time_ms ?? -1)) left.set(e.node_id, e);
  const at = now.timestamp_ms;
  // Only this boot's `left` counts: a node that comes and goes has one per boot, and this one's may come a refresh later.
  const seen = [...previous.filter((n) => !present.has(n.node.id)), ...kept].map((n): NodeView => {
    const m = LEFT.exec(left.get(n.node.id)?.detail ?? "");
    const e = m?.[2] === n.node.incarnation ? left.get(n.node.id) : undefined;
    const reason = n.node.metadata["axon.leave.reason"] || (e && m?.[1]) || n.leaveReason;
    const drained = n.node.metadata["axon.lifecycle"] === "draining" || !!reason;
    return { ...n, reachable: false, leftAt: e?.time_ms ?? n.leftAt ?? at, leaveReason: drained ? reason || "drained" : null };
  });
  const told = [...left.values()].map((e): NodeView => {
    const m = LEFT.exec(e.detail);
    return { node: { id: e.node_id, incarnation: m?.[2] ?? "", address: "", metadata: {} }, reachable: false, snapshot: null, error: null, leftAt: e.time_ms, leaveReason: m?.[1] || null };
  });
  const out = new Map<string, NodeView>();
  for (const n of [...seen, ...told]) if (!present.has(n.node.id) && !out.has(n.node.id) && at - n.leftAt! < DEPARTED_MS) out.set(n.node.id, n);
  return [...out.values()].sort((a, b) => b.leftAt! - a.leftAt!).slice(0, 50);
}

export interface Trend {
  t: number;
  depth: number | null;
  processed: number | null;
}

/** Inline sparklines for the inventory: a bounded, least-recently-seen-first cache. */
export class ActorTrends {
  static readonly ACTORS = 128;
  static readonly POINTS = 60;
  static readonly GAP_MS = 15_000;
  private byActor = new Map<string, Trend[]>();

  static key = (a: InventoryActor) => `${a.node_id}|${a.node_incarnation}|${actorKey(a.incarnation)}`;

  append(actors: InventoryActor[], t: number) {
    for (const actor of actors) {
      const key = ActorTrends.key(actor);
      let points = this.byActor.get(key) ?? [];
      this.byActor.delete(key);
      const last = points.at(-1);
      if (last && t < last.t) points = [];
      else if (last && (t - last.t > ActorTrends.GAP_MS || (last.processed ?? 0) > actor.diagnostics.processed))
        points.push({ t: last.t + 1, depth: null, processed: null });
      if (last?.t !== t) points.push({ t, depth: actor.diagnostics.mailbox_depth, processed: actor.diagnostics.processed });
      this.byActor.set(key, points.slice(-ActorTrends.POINTS));
    }
    while (this.byActor.size > ActorTrends.ACTORS) this.byActor.delete(this.byActor.keys().next().value!);
  }

  points(actor: InventoryActor): Trend[] {
    return this.byActor.get(ActorTrends.key(actor)) ?? [];
  }
}

export interface Totals {
  nodes: number;
  actors: number | null;
  connections: number;
  throughput: number | null;
  queued: number;
}

/** Throughput is TX only: both ends report a session, so TX counts each byte once. */
export function clusterTotals(overview: Overview, series: ConnSeries): Totals {
  const observed = overview.nodes.filter((n) => n.snapshot);
  const peers = observed.flatMap((n) => n.snapshot!.cluster.engine.peers.filter((p) => !isDown(p)).map((p) => ({ n, p })));
  const rates = peers.map(({ n, p }) => series.latest(connectionKey(n.node.id, n.snapshot!.incarnation, p), overview.timestamp_ms)?.tx);
  const known = rates.filter((r): r is number => r != null);
  return {
    nodes: overview.nodes.length,
    actors: observed.length ? sum(observed.map((n) => n.snapshot!.actor_total)) : null,
    connections: peers.length,
    throughput: known.length ? sum(known) : null,
    queued: sum(peers.map(({ p }) => p.session.outbound_reserved_bytes)),
  };
}
