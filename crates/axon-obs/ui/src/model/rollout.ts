import type { NodeView, Snapshot } from "../api";

/** What `/readyz` would answer, from the node's snapshot: each failing check by its `/readyz` value; null when the snapshot is not live. */
export function readiness(view: NodeView): string[] | null {
  const s = view.snapshot;
  if (!s || view.leftAt || view.error) return null;
  const c = s.cluster;
  return [s.lifecycle.phase !== "running" && "shutting down", c.isolated && "isolated", c.warming && "warming", c.quotas_starting && "quotas starting"].filter((v): v is string => !!v);
}

/** `2` or `2..3`; empty from a node that predates it. */
export function wireRange(s: Snapshot | null): string {
  const v = s?.cluster.engine.versions;
  return !v ? "" : v[0] === v[1] ? `${v[0]}` : `${v[0]}..${v[1]}`;
}

export interface AcceptorRow {
  id: string;
  /** The acceptor's own figures, null when it has not reported them or its last report is not live. */
  stats: Record<string, string> | null;
  state: "active" | "warming" | "no report";
}

/** One row per Paxos acceptor, as the nodes' ownership settings name them, each with what it reports of itself. */
export function acceptorRows(nodes: NodeView[]): AcceptorRow[] {
  const named = nodes.map((n) => n.snapshot?.cluster.ownership).find((o) => o?.kind === "paxos")?.settings.find(([k]) => k === "acceptors")?.[1];
  return (named ? named.split(", ") : []).map((id) => {
    const node = nodes.find((n) => n.node.id === id);
    const own = !node?.error && node?.snapshot?.cluster.ownership?.stats;
    const all = own && Object.fromEntries(own);
    const stats = all && "leases" in all ? all : null;
    return { id, stats, state: !stats ? "no report" : stats.warming === "true" ? "warming" : "active" };
  });
}
