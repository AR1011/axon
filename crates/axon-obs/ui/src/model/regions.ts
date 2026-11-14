import type { Overview } from "../api";
import { rates } from "./series";

/** The bytes per second each way between two regions, `a` before `b` in name order. */
export interface RegionEdge {
  id: string;
  a: string;
  b: string;
  ab: number;
  ba: number;
}

/**
 * The session bytes per second between each pair of regions, from each node's link meters over its last two snapshots
 * (`rates`). A byte counts once, at the node that sent it; a region to itself is left out.
 */
export function regionEdges(now: Overview): RegionEdge[] {
  const edges = new Map<string, RegionEdge>();
  for (const view of now.nodes) {
    const snapshot = view.snapshot;
    if (!snapshot?.cluster.links) continue;
    const sent = snapshot.cluster.links.filter((l) => l.protocol == null && l.direction === "sent" && l.src_region !== l.dst_region);
    const rate = rates(`links:${view.node.id}`, snapshot.incarnation, snapshot.timestamp_ms, new Map(sent.map((l) => [`${l.src_region}\n${l.dst_region}\n${l.lane}`, l.wire_bytes])));
    for (const l of sent) {
      const [a, b] = l.src_region < l.dst_region ? [l.src_region, l.dst_region] : [l.dst_region, l.src_region];
      const id = `${a}\n${b}`;
      let edge = edges.get(id);
      if (!edge) edges.set(id, (edge = { id, a, b, ab: 0, ba: 0 }));
      const n = Math.max(0, rate.get(`${l.src_region}\n${l.dst_region}\n${l.lane}`) ?? 0);
      if (l.src_region === a) edge.ab += n;
      else edge.ba += n;
    }
  }
  return [...edges.values()].sort((x, y) => x.id.localeCompare(y.id));
}
