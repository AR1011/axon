import { LANES, type NodeView, type Peer } from "../api";
import type { ConnSample, ConnSeries } from "./live";
import { connectionKey, isDown, STALE_MS } from "./live";

export interface Link {
  id: string;
  source: string;
  target: string;
  /** The node the measurements come from: the lexically first endpoint that reports the pair. */
  node: string;
  /** Each lane of the pair that is up, as `node` reports it. */
  lanes: Peer[];
  stale: boolean;
  rate: number | null;
  frames: number | null;
  frameBytes: number | null;
  rtt: number | null;
  queued: number;
  carried: number;
  width: number;
}

export const MAX_LINKS = 200;

/**
 * One link per dialling and accepting node boot, the sum of its lanes. Both ends report each lane (one as `out`, one as `in`);
 * the pair is recognised by dialer, acceptor and both incarnations. Peers outside `views` are never invented.
 */
export function graphLinks(views: NodeView[], series: ConnSeries, now: number): Link[] {
  const visible = new Set(views.map((v) => v.node.id));
  const pairs = new Map<string, { view: NodeView; lanes: Peer[] }>();
  for (const view of [...views].sort((a, b) => a.node.id.localeCompare(b.node.id))) {
    const snapshot = view.snapshot;
    if (!snapshot) continue;
    for (const peer of snapshot.cluster.engine.peers) {
      if (!visible.has(peer.node) || isDown(peer)) continue;
      const [dialer, acceptor] = peer.outgoing
        ? [`${view.node.id}@${snapshot.incarnation}`, `${peer.node}@${peer.incarnation}`]
        : [`${peer.node}@${peer.incarnation}`, `${view.node.id}@${snapshot.incarnation}`];
      const id = `${dialer}>${acceptor}`;
      let pair = pairs.get(id);
      if (!pair) pairs.set(id, (pair = { view, lanes: [] }));
      // The other end reports the same lanes: the first endpoint speaks for the pair.
      if (pair.view === view) pair.lanes.push(peer);
    }
  }
  return [...pairs].map(([id, { view, lanes: reported }]) => {
    const snapshot = view.snapshot!;
    // In lane order, whatever order the snapshot ranks them in: Control first, the one an edge opens and times.
    const lanes = [...reported].sort((a, b) => LANES.indexOf(a.lane) - LANES.indexOf(b.lane));
    const stale = !!view.error || now - snapshot.timestamp_ms > STALE_MS;
    const samples = stale ? [] : lanes.map((peer) => series.latest(connectionKey(view.node.id, snapshot.incarnation, peer), now));
    const total = (pick: (s: ConnSample) => number | null) => samples.reduce<number | null>((n, s) => (s && pick(s) != null ? (n ?? 0) + pick(s)! : n), null);
    const rate = total((s) => (s.tx != null && s.rx != null ? s.tx + s.rx : null));
    const frames = total((s) => s.frames);
    const payload = total((s) => (s.frames != null && s.frameBytes != null ? s.frames * s.frameBytes : null));
    // Control carries the keepalive that a new lane has not timed yet.
    const timed = lanes.find((p) => p.session.rtt_ns > 0);
    return {
      id,
      source: view.node.id,
      target: lanes[0].node,
      node: view.node.id,
      lanes,
      stale,
      rate,
      frames,
      frameBytes: frames ? (payload ?? 0) / frames : null,
      rtt: timed?.session.rtt_ns ?? null,
      queued: lanes.reduce((n, p) => n + p.session.outbound_reserved_bytes, 0),
      carried: lanes.reduce((n, p) => n + p.session.tx_wire_bytes + p.session.rx_wire_bytes, 0),
      width: 1 + Math.min(7, Math.log2(1 + (rate ?? 0) / 1024)),
    };
  });
}

/** Over `max`, the sessions that carried the most bytes: a lifetime count, so the set holds still between refreshes. */
export function busiest(links: Link[], max = MAX_LINKS): Link[] {
  if (links.length <= max) return links;
  return [...links].sort((a, b) => b.carried - a.carried).slice(0, max);
}
