import { useMemo, useState } from "react";
import { Empty } from "../../components/atoms";
import { Info } from "../../components/Info";
import { graphNav } from "../../components/rowLink";
import { useGlide } from "../../hooks";
import { bytes, bytesPerSecond, count, perSecond } from "../../model/format";
import { flowLayout, pushLine, type FlowEdge } from "../../model/streamGraph";
import { CARD } from "../../model/topology";
import { routeEdges } from "../topology/edgePath";
import { GraphCard } from "../topology/GraphCard";
import { GraphControls } from "../topology/GraphControls";
import { useCamera } from "../topology/useCamera";
import { useNodeDrag } from "../topology/useNodeDrag";
import type { Row } from "./StreamsPage";
import { CardTitle } from "../../components/Flag";

export interface RatedEdge extends FlowEdge {
  bytesRate: number | null;
  recordsRate: number | null;
}

/** A child's queue this full is on its way to a slow-consumer reset. */
const BACKED_UP = 1 << 20;

function RateLabel({ x, y, value }: { x: number; y: number; value: number }) {
  return <text className="edge-label" x={x} y={y} textAnchor="middle">{bytesPerSecond(useGlide(value) ?? value)}</text>;
}

function Card({ row, ...card }: { row: Row; x: number; y: number; selected: boolean; onSelect: () => void; onHover: (over: boolean) => void; onDrag: (e: React.PointerEvent<SVGGElement>) => void }) {
  const { stats, kind } = row;
  const id = row.view.node.id;
  const trouble = stats.orphaned > 0;
  const rate = kind === "origin" ? row.publishedRate : row.deliveredRate;
  return (
    <GraphCard
      {...card}
      label={`${kind} ${id}${trouble ? ", without a parent" : ""}${row.recentResets ? `, ${row.recentResets} recent resets` : ""}`}
      data={{ "data-stream-node": id, "data-kind": kind }}
      stroke={trouble ? "var(--warning)" : undefined}
      strokeWidth={trouble ? 1.5 : 1}
    >
      <circle cx="12" cy="17" r="3" fill={trouble ? "var(--warning)" : kind === "origin" ? "var(--success)" : "var(--muted)"} />
      <CardTitle view={row.view} room={17} />
      {row.recentResets > 0 && (
        <g>
          <title>{`${count(row.recentResets)} subscriptions reset on this node in the last 15 min (${count(row.resets)} in total)`}</title>
          <circle cx={CARD.w - 13} cy="17" r="6" fill="var(--danger)" />
          <text x={CARD.w - 13} y="20.5" fill="#fff" fontSize="9" fontWeight="700" textAnchor="middle">!</text>
        </g>
      )}
      <text x="12" y="40" fill="var(--muted)" fontSize="10">{kind}</text>
      <text x={CARD.w - 12} y="40" fill="var(--muted)" fontSize="10" textAnchor="end">{trouble ? `${stats.orphaned} without a parent` : `${count(stats.hub.streams)} streams`}</text>
      <text x="12" y="56" fill="var(--muted)" fontSize="10" fontFamily="var(--font-mono)">{`${perSecond(rate)} ${kind === "origin" ? "published" : "delivered"}`}</text>
      <text x={CARD.w - 12} y="56" fill="var(--muted)" fontSize="10" fontFamily="var(--font-mono)" textAnchor="end">{count(stats.hub.subscriptions)} subs</text>
    </GraphCard>
  );
}

export function StreamGraph({ rows, edges, selectedId, onSelect }: { rows: Row[]; edges: RatedEdge[]; selectedId?: string; onSelect: (id: string) => void }) {
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);
  const [hoveredEdge, setHoveredEdge] = useState<string | null>(null);
  const structure = useMemo(() => [...rows.map((r) => `${r.view.node.id}:${r.kind}`), "|", ...edges.map((e) => `${e.id}:${e.joining}`)].join(","), [rows, edges]);
  const graph = useMemo(
    () => flowLayout(rows.map((r) => ({ id: r.view.node.id, stats: r.stats })), edges),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the layout follows the tree's shape, never a telemetry refresh
    [structure],
  );
  const { host, camera, fit, zoom, onPointerDown } = useCamera(structure, graph.width, graph.height);
  const drag = useNodeDrag(graph.positions, camera.k);
  const routes = useMemo(() => routeEdges(edges, drag.positions), [edges, drag.positions]);
  const max = Math.max(0, ...edges.map((e) => e.bytesRate ?? 0));
  const idle = rows.filter((r) => !graph.positions.has(r.view.node.id)).map((r) => r.view.node.id);
  // Pointing at a node keeps its whole path up to the origin lit, not just the last hop.
  const chain = useMemo(() => {
    const up = new Set(hoveredNode ? [hoveredNode] : []);
    for (let grew = true; grew; ) {
      grew = false;
      for (const e of edges) {
        if (up.has(e.target) && !up.has(e.source)) {
          up.add(e.source);
          grew = true;
        }
      }
    }
    return up;
  }, [edges, hoveredNode]);

  if (!graph.positions.size) return <Empty title="No stream flows" detail="Nodes show here when a node publishes a stream or pulls a stream from a parent." />;
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div ref={host} onPointerDown={onPointerDown} className="dotgrid relative min-h-0 flex-1 cursor-grab touch-none overflow-hidden">
        <svg {...graphNav} className="graph size-full" role="group" aria-label="Stream flows between nodes, parents above their children. Select a node to inspect. Arrows move between them.">
          <defs>
            <marker id="flow-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="9" markerHeight="9" markerUnits="userSpaceOnUse" orient="auto-start-reverse">
              <path d="M0 0L8 4L0 8Z" fill="context-stroke" />
            </marker>
          </defs>
          <g data-scene transform={`translate(${camera.x} ${camera.y}) scale(${camera.k})`}>
            {edges.map((edge, index) => {
              const path = routes[index];
              if (!path) return null;
              // Flows cross in the middle, so a label sits where its edge arrives, stacked above the others arriving at that card.
              const end = path.d.split(/[ ,]+/).slice(-2).map(Number);
              const f = edge.bytesRate == null || max <= 0 ? null : edge.bytesRate / max;
              const backedUp = edge.queued >= BACKED_UP;
              // Pointing at a node lifts its flows and its path to the origin out of the rest; the selection alone dims nothing.
              const lit = hoveredEdge === edge.id || !hoveredNode || edge.source === hoveredNode || chain.has(edge.target);
              const state = edge.joining ? "gets the missing records before the move" : `${edge.streams} streams · ${bytesPerSecond(edge.bytesRate)} · ${perSecond(edge.recordsRate)} records · ${bytes(edge.queued)} queued`;
              const label = [`${edge.source} → ${edge.target} · ${state}`, edge.push && pushLine(edge.push)].filter(Boolean).join("\n");
              return (
                <g key={edge.id} className="edge-group" data-flow={edge.id} data-backed-up={backedUp} data-joining={edge.joining} data-rate={edge.bytesRate ?? "unknown"} onPointerEnter={() => setHoveredEdge(edge.id)} onPointerLeave={() => setHoveredEdge(null)}>
                  <title>{label}</title>
                  <path className="hit" d={path.d} />
                  <path
                    className="edge"
                    d={path.d}
                    markerEnd="url(#flow-arrow)"
                    stroke={backedUp ? "var(--danger)" : edge.joining ? "var(--muted)" : "var(--success)"}
                    strokeWidth={edge.joining || f == null ? 1 : 1.2 + f * 4.5}
                    strokeDasharray={edge.joining ? "5 5" : backedUp ? "2 3" : undefined}
                    opacity={lit ? 0.85 : 0.18}
                  />
                  {!edge.joining && edge.bytesRate != null && lit && <RateLabel x={end[0]} y={end[1] - 16 - 12 * edges.slice(0, index).filter((e) => e.target === edge.target && !e.joining).length} value={edge.bytesRate} />}
                </g>
              );
            })}
            {rows.map((row) => {
              const id = row.view.node.id;
              const at = drag.positions.get(id);
              return at && <Card key={id} row={row} x={at.x} y={at.y} selected={id === selectedId} onSelect={() => !drag.wasDrag() && onSelect(id)} onDrag={(e) => drag.onPointerDown(id, e)} onHover={(over) => setHoveredNode(over ? row.view.node.id : null)} />;
            })}
          </g>
        </svg>
        <GraphControls k={camera.k} zoom={zoom} fit={fit} drag={drag} />
      </div>
      <footer className="flex shrink-0 flex-wrap items-center gap-x-4 gap-y-1 border-t border-border px-[11px] py-1.5 text-[10px] text-muted">
        <span>{edges.filter((e) => !e.joining).length} flows</span>
        <span className="flex items-center gap-1.5"><svg width="22" height="6" aria-hidden="true"><path d="M0 3H22" stroke="var(--success)" strokeWidth="2" /></svg>Active flow</span>
        <span className="flex items-center gap-1.5"><svg width="22" height="6" aria-hidden="true"><path d="M0 3H22" stroke="var(--muted)" strokeDasharray="5 5" /></svg>Gets missing records before a move</span>
        <span className="flex items-center gap-1.5"><svg width="22" height="6" aria-hidden="true"><path d="M0 3H22" stroke="var(--danger)" strokeWidth="2" strokeDasharray="2 3" /></svg>Child queue over {bytes(BACKED_UP)}</span>
        {idle.length > 0 && <span title={idle.join(", ")}>{idle.length} idle {idle.length === 1 ? "node" : "nodes"} not drawn</span>}
        <span className="ml-auto"><Info label="About the flow graph">Parents are above the nodes that pull from them. The width of an edge shows its share of the busiest flow. Its label shows the record payload that the parent pushes to that child. Rates are for each node pair, never for each stream.</Info></span>
      </footer>
    </div>
  );
}
