import { useMemo } from "react";
import type { NodeView, Overview, Peer } from "../api";
import { useUrlState } from "../hooks";
import { connectionKey, direction } from "../model/live";
import { useLive, type Live } from "../store";

export const roleOf = (view: NodeView) => view.node.metadata.role ?? "";
/** The node's `country` label, as an ISO 3166-1 alpha-2 code, and its `region` label. */
export const placeOf = (view: NodeView) => {
  const country = (view.node.metadata.country ?? "").toUpperCase();
  const region = view.node.metadata.region ?? "";
  return { country, region, text: [region, country].filter(Boolean).join(", ") };
};
export const nodePath = (id: string) => `/nodes/${encodeURIComponent(id)}`;

let index: { overview: Overview | null; departed: NodeView[]; byId: Map<string, NodeView> } | undefined;
/** The member with this id, else the departed node; one map per overview, so a table of flags costs a lookup each. */
export function nodeById(s: Live, id: string): NodeView | undefined {
  if (index?.overview !== s.overview || index.departed !== s.departed) {
    index = { overview: s.overview, departed: s.departed, byId: new Map([...s.departed, ...(s.overview?.nodes ?? [])].map((n) => [n.node.id, n])) };
  }
  return index.byId.get(id);
}

/** The node whose snapshot speaks for the cluster: the observer, else any node that answered. */
export const reporter = (overview: Overview | null) =>
  overview?.nodes.find((n) => n.node.id === overview.observer && n.snapshot) ?? overview?.nodes.find((n) => n.snapshot);

/** Node, peer, direction and lane: what names a connection in its route and in its `ref`. */
const parts = (node: string, peer: Peer) => [node, peer.node, direction(peer), peer.lane];
export const refOf = (parts: string[]) => parts.join("|");
export const connectionPath = (c: Connection) => `/connections/${parts(c.view.node.id, c.peer).map(encodeURIComponent).join("/")}`;

/** Explicit selection, else the first node with actors, else the first node. */
export function useSelectedNode(): [NodeView | undefined, (id: string | null) => void] {
  const nodes = useLive((s) => s.overview?.nodes);
  const departed = useLive((s) => s.departed);
  const [id, setId] = useUrlState("node", "");
  const selected = useMemo(
    // A named node that is gone for good selects nothing rather than quietly standing in another.
    () => (id ? (nodes?.find((n) => n.node.id === id) ?? departed.find((n) => n.node.id === id)) : (nodes?.find((n) => (n.snapshot?.actor_total ?? 0) > 0) ?? nodes?.[0])),
    [nodes, departed, id],
  );
  return [selected, setId];
}

export interface Connection {
  view: NodeView;
  peer: Peer;
  /** Identifies the series in the live buffer. */
  key: string;
  /** Identifies the selection in the URL. */
  ref: string;
}

export const connectionRef = (node: string, peer: Peer) => refOf(parts(node, peer));

export function listConnections(overview: Overview | null, scope?: string): Connection[] {
  return (overview?.nodes ?? [])
    .filter((view) => view.snapshot && (!scope || view.node.id === scope))
    .flatMap((view) =>
      view.snapshot!.cluster.engine.peers.map((peer) => ({
        view,
        peer,
        key: connectionKey(view.node.id, view.snapshot!.incarnation, peer),
        ref: connectionRef(view.node.id, peer),
      })),
    );
}

/** `fallback` off: only a connection the URL names, for views where nothing selected must look like nothing selected. */
export function useSelectedConnection(scope?: string, fallback = true): [Connection | undefined, (ref: string | null) => void, Connection[]] {
  const overview = useLive((s) => s.overview);
  const [ref, setRef] = useUrlState("conn", "");
  const all = useMemo(() => listConnections(overview, scope), [overview, scope]);
  return [all.find((c) => c.ref === ref) ?? (fallback ? all[0] : undefined), setRef, all];
}
