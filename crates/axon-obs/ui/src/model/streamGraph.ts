import type { Downstream, StreamStats } from "../api";
import { bytes, bytesPerSecond, count, duration } from "./format";
import { CARD, type Box } from "./topology";

export interface FlowNode {
  id: string;
  stats: StreamStats;
}

export interface FlowEdge {
  id: string;
  /** The parent: data flows from `source` to `target`. */
  source: string;
  target: string;
  streams: number;
  /** The child is still catching up on this parent and has not moved to it yet. */
  joining: boolean;
  queued: number;
  pushedBytes: number | null;
  pushedRecords: number | null;
  /** The parent's window to the child, from the parent's report. */
  push: Downstream | null;
}

export interface FlowLayout {
  positions: Map<string, Box>;
  layers: string[][];
  width: number;
  height: number;
}

const GAP = { x: 46, y: 96 };

/** The parent's window to a child, as its edge and the node's detail show it. */
export const pushLine = (d: Downstream) =>
  `In flight ${bytes(d.inflight_bytes)} · Window ${bytes(d.window_bytes)} · Round trip ${duration(d.round_trip_us == null ? null : d.round_trip_us * 1000)} · Rate ${bytesPerSecond(d.delivery_rate)} · Resent ${count(d.resent)}`;

/** Two lanes' windows to one child as one: the sums, and the longer round trip. */
const addLane = (a: Downstream, b: Downstream): Downstream => ({
  ...b,
  window_bytes: a.window_bytes + b.window_bytes,
  inflight_bytes: a.inflight_bytes + b.inflight_bytes,
  inflight_pushes: a.inflight_pushes + b.inflight_pushes,
  delivery_rate: a.delivery_rate + b.delivery_rate,
  resent: a.resent + b.resent,
  round_trip_us: a.round_trip_us == null || b.round_trip_us == null ? (a.round_trip_us ?? b.round_trip_us) : Math.max(a.round_trip_us, b.round_trip_us),
});

/** One edge per parent and child, from what either end reports: the child names its parent, the parent counts what it pushed. */
export function flowEdges(nodes: FlowNode[]): FlowEdge[] {
  const edges = new Map<string, FlowEdge>();
  const edge = (source: string, target: string) => {
    const id = `${source}>${target}`;
    if (!edges.has(id)) edges.set(id, { id, source, target, streams: 0, joining: false, queued: 0, pushedBytes: null, pushedRecords: null, push: null });
    return edges.get(id)!;
  };
  // The parent reports per lane: the edge adds them up.
  for (const { id, stats } of nodes) {
    for (const child of stats.downstream) {
      if (!child.branches) continue;
      const e = edge(id, child.node);
      e.streams += child.branches;
      e.queued += child.queued_bytes;
      e.pushedBytes = (e.pushedBytes ?? 0) + child.pushed_bytes;
      e.pushedRecords = (e.pushedRecords ?? 0) + child.pushed_records;
      e.push = e.push ? addLane(e.push, child) : child;
    }
  }
  // The child's count of the keys it takes from the parent, where it has one.
  for (const { id, stats } of nodes) {
    for (const parent of stats.parents) {
      const e = edge(parent.node, id);
      e.streams = parent.keys || e.streams;
      e.joining = parent.keys === 0 && parent.joining > 0;
    }
  }
  const known = new Set(nodes.map((n) => n.id));
  return [...edges.values()].filter((e) => known.has(e.source) && known.has(e.target)).sort((a, b) => a.id.localeCompare(b.id));
}

/** Rows by distance from an origin, top to bottom; within a row a node sits under its parents. Nodes with no edge are left out. */
export function flowLayout(nodes: FlowNode[], edges: FlowEdge[]): FlowLayout {
  const flowing = edges.filter((e) => !e.joining);
  const linked = new Set(edges.flatMap((e) => [e.source, e.target]));
  const ids = nodes.filter((n) => linked.has(n.id) || n.stats.originated > 0).map((n) => n.id).sort();
  const depth = new Map<string, number>();
  // Longest path from a node nobody feeds; the pass count bounds a cycle a misconfigured tier could make.
  for (let pass = 0, changed = true; changed && pass <= ids.length; pass++) {
    changed = false;
    for (const id of ids) {
      const before = depth.get(id);
      const parents = flowing.filter((e) => e.target === id && depth.has(e.source));
      const fed = flowing.some((e) => e.target === id);
      if (!fed) depth.set(id, 0);
      else if (parents.length) depth.set(id, Math.min(ids.length, Math.max(...parents.map((e) => depth.get(e.source)! + 1))));
      changed ||= depth.get(id) !== before;
    }
  }
  const layers: string[][] = [];
  for (const id of ids) (layers[depth.get(id) ?? 0] ??= []).push(id);
  const rows = layers.filter(Boolean);
  const column = new Map<string, number>();
  for (const row of rows) {
    const under = (id: string) => {
      const at = flowing.filter((e) => e.target === id && column.has(e.source)).map((e) => column.get(e.source)!);
      return at.length ? at.reduce((a, b) => a + b, 0) / at.length : Infinity;
    };
    row.sort((a, b) => under(a) - under(b) || a.localeCompare(b));
    row.forEach((id, i) => column.set(id, (i + 0.5) / row.length));
  }
  const widest = Math.max(1, ...rows.map((r) => r.length));
  const width = widest * (CARD.w + GAP.x) - GAP.x + 48;
  const positions = new Map<string, Box>();
  rows.forEach((row, r) => {
    const span = row.length * (CARD.w + GAP.x) - GAP.x;
    row.forEach((id, i) => positions.set(id, { x: (width - span) / 2 + i * (CARD.w + GAP.x), y: 24 + r * (CARD.h + GAP.y) }));
  });
  return { positions, layers: rows, width, height: 48 + rows.length * (CARD.h + GAP.y) - (rows.length ? GAP.y : 0) };
}
