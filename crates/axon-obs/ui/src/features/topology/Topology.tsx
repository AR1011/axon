import { useEffect, useMemo, useState } from "react";
import { Link as RouterLink, useLocation } from "react-router";
import { Icon } from "../../components/Icon";
import type { NodeView } from "../../api";
import { Empty } from "../../components/atoms";
import { CardTitle } from "../../components/Flag";
import { Info } from "../../components/Info";
import { graphNav } from "../../components/rowLink";
import { IconToggle, ToggleGroup } from "../../components/ToggleGroup";
import { Select } from "../../components/Select";
import { nodeStatus, useGlide, useNow, useTheme, useUrlState } from "../../hooks";
import { bytes, percent } from "../../model/format";
import { busiest, graphLinks } from "../../model/links";
import { isDown } from "../../model/live";
import { CARD, fraction, layout, LAYOUTS, MAX_NODES, MEASURES, ramp, rampCss, RAMPS, type LayoutMode, type Measure, type Ramp } from "../../model/topology";
import { connections, useLive } from "../../store";
import { connectionRef, nodePath, roleOf, useSelectedConnection, useSelectedNode } from "../select";
import { routeEdges } from "./edgePath";
import { GraphCard } from "./GraphCard";
import { GRAPH_BUTTON, GraphControls } from "./GraphControls";
import { useCamera } from "./useCamera";
import { useNodeDrag } from "./useNodeDrag";

const LAYOUT_LABELS: Record<LayoutMode, string> = { connectivity: "Connectivity", roles: "Grouped by role", circular: "Circular", grid: "Grid", force: "Force directed" };

const swatch = (name: Ramp) => <span aria-hidden="true" className="h-3 w-12 shrink-0 rounded-sm" style={{ background: rampCss(name) }} />;
const RAMP_OPTIONS = (Object.keys(RAMPS) as Ramp[]).map((value) => ({ value, label: RAMPS[value].label, preview: swatch(value) }));

function EdgeLabel({ x, y, value, format }: { x: number; y: number; value: number; format: (n: number) => string }) {
  return <text className="edge-label" x={x} y={y} textAnchor="middle">{format(useGlide(value) ?? value)}</text>;
}

function NodeCard({ view, ...card }: { view: NodeView; x: number; y: number; selected: boolean; onSelect: () => void; onHover: (over: boolean) => void; onDrag: (e: React.PointerEvent<SVGGElement>) => void }) {
  const now = useNow();
  const status = nodeStatus(view, now);
  const s = view.snapshot;
  const id = view.node.id;
  return (
    <GraphCard {...card} label={`Node ${id}, ${status.label}`} data={{ "data-node": id }} dash={status.stale ? "4 3" : undefined}>
      <circle cx="12" cy="17" r="3" fill={`var(--${status.tone})`} />
      <CardTitle view={view} room={20} />
      <text x="12" y="40" fill="var(--muted)" fontSize="10">{roleOf(view) || "no role"}</text>
      <text x={CARD.w - 12} y="40" fill="var(--muted)" fontSize="10" textAnchor="end">{s ? `${s.actor_total} actors` : "No snapshot"}</text>
      <text x="12" y="56" fill="var(--muted)" fontSize="10" fontFamily="var(--font-mono)">{percent(s?.process?.cpu_percent)}</text>
      <text x={CARD.w - 12} y="56" fill="var(--muted)" fontSize="10" fontFamily="var(--font-mono)" textAnchor="end">{bytes(s?.process?.rss_bytes)}</text>
    </GraphCard>
  );
}

export function Topology({ fullPage = false }: { fullPage?: boolean }) {
  const now = useNow();
  const overview = useLive((s) => s.overview);
  const version = useLive((s) => s.version);
  const [selectedNode, selectNode] = useSelectedNode();
  const [selectedConnection, selectConnection] = useSelectedConnection(undefined, false);
  const [mode, setMode] = useUrlState<LayoutMode>("layout", "connectivity");
  const [measure, setMeasure] = useUrlState<Measure>("measure", "traffic");
  const [colour, setColour] = useUrlState<"measure" | "queue">("colour", "measure");
  const [scale, setScale] = useUrlState<"linear" | "log">("scale", "linear");
  const [palette, setPalette] = useUrlState<Ramp>("ramp", "ocean");
  const rampName = palette in RAMPS ? palette : "ocean";
  const [scope, setScope] = useUrlState<"all" | "peers">("scope", "all");
  const [labels, setLabels] = useUrlState<"on" | "off">("labels", "off");
  const [theme] = useTheme();
  const faint = theme === "light" ? 0.6 : 0.35;
  const [hovered, setHovered] = useState<string | null>(null);
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);

  const selectedId = selectedNode?.node.id;
  // The panel and its full size share the view settings in the URL, and the selection, pinned so that both show the same node.
  const search = new URLSearchParams(useLocation().search);
  if (selectedId) search.set("node", selectedId);
  const toggle = fullPage ? "Back to the overview" : "Open full topology";
  const { views, total } = useMemo(() => {
    let nodes = overview?.nodes ?? [];
    if (scope === "peers" && selectedNode?.snapshot) {
      const near = new Set([selectedNode.node.id, ...selectedNode.snapshot.cluster.engine.peers.map((p) => p.node)]);
      nodes = nodes.filter((n) => near.has(n.node.id) || n.snapshot?.cluster.engine.peers.some((p) => p.node === selectedNode.node.id));
    }
    // The selection goes first so it is inside the window however large the cluster is.
    const ordered = [...nodes.filter((n) => n.node.id === selectedId), ...nodes.filter((n) => n.node.id !== selectedId)];
    return { views: ordered.slice(0, MAX_NODES), total: nodes.length };
  }, [overview, scope, selectedNode, selectedId]);

  // eslint-disable-next-line react-hooks/exhaustive-deps -- `version` stands in for the mutable live buffer
  const all = useMemo(() => graphLinks(views, connections, overview?.timestamp_ms ?? now), [views, version]);
  const links = useMemo(() => busiest(all), [all]);
  const ends = useMemo(() => {
    const visible = new Set(views.map((v) => v.node.id));
    return views.reduce((n, v) => n + (v.snapshot?.cluster.engine.peers.filter((p) => visible.has(p.node) && !isDown(p)).length ?? 0), 0);
  }, [views]);
  const structure = useMemo(
    () => [mode, ...views.map((v) => `${v.node.id}:${roleOf(v)}`).sort(), "|", ...links.map((l) => `${l.source}>${l.target}`).sort()].join(","),
    [mode, views, links],
  );
  const graph = useMemo(
    () => layout(views.map((v) => v.node.id), links.map((l) => [l.source, l.target]), mode, Object.fromEntries(views.map((v) => [v.node.id, roleOf(v)]))),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the layout depends on structure alone, never on a telemetry refresh
    [structure],
  );
  // Tab order follows the picture, top to bottom then left to right, not the order the data arrived in.
  const reading = useMemo(() => {
    const rank = (id: string) => { const at = graph.positions.get(id); return at ? at.y * 1e5 + at.x : Infinity; };
    return {
      views: [...views].sort((a, b) => rank(a.node.id) - rank(b.node.id)),
      links: links.map((link, index) => ({ link, index })).sort((a, b) => rank(a.link.source) - rank(b.link.source) || rank(a.link.target) - rank(b.link.target)),
    };
  }, [graph, views, links]);
  const { host, camera, fit, focus, zoom, onPointerDown } = useCamera(structure, graph.width, graph.height);

  const spec = MEASURES[measure];
  const max = Math.max(0, ...links.map((l) => spec.value(l) ?? 0));
  const maxQueue = Math.max(0, ...links.map((l) => l.queued));
  const chosen = links.find((l) => selectedConnection && l.lanes.some((p) => connectionRef(l.node, p) === selectedConnection.ref));
  const drag = useNodeDrag(graph.positions, camera.k);
  const { reset } = drag;
  // A hand-made arrangement belongs to the layout it was made in.
  useEffect(reset, [reset, mode]);
  const routes = useMemo(() => routeEdges(links, drag.positions), [links, drag.positions]);

  if (!overview?.nodes.length) return <Empty title="No nodes discovered" detail="No membership snapshot yet." />;

  return (
    <section
      aria-label="Topology"
      className={`flex min-h-0 min-w-0 flex-col overflow-hidden bg-panel ${fullPage ? "flex-1" : "rounded-md border border-border"} relative`}
    >
      <header data-no-pan className="flex min-h-[37px] shrink-0 flex-wrap items-center gap-2 border-b border-border px-3 py-1">
        {fullPage ? (
          <div className="flex min-w-0 flex-wrap items-center gap-x-4 gap-y-1 text-[10px] text-muted">
            <span className="flex items-center gap-2">
              {spec.label} · {spec.unit}
              <span data-ramp-legend aria-hidden="true" className="h-2.5 w-[46px]" style={{ background: rampCss(rampName), clipPath: "polygon(0 50%, 100% 0, 100% 100%)" }} />
              <span className="mono">0 → {spec.format(max)}</span>
            </span>
            <span className={links.some((l) => spec.value(l) == null) ? "" : "invisible"} title="A dashed edge has no fresh reading for this measurement: it is unknown, not zero.">┄ No reading</span>
          </div>
        ) : (
          <h2>Topology</h2>
        )}
        <div className="ml-auto flex flex-wrap items-center gap-2.5">
          {fullPage && (
            <>
              <ToggleGroup label="Layout" caption="Layout" value={mode} onChange={setMode} options={LAYOUTS.map((value) => ({ value, label: LAYOUT_LABELS[value], icon: value }))} />
              <ToggleGroup label="Edge measurement" caption="Measure" value={measure} onChange={setMeasure} options={(Object.keys(MEASURES) as Measure[]).map((value) => ({ value, label: MEASURES[value].label, icon: MEASURES[value].icon }))} />
              <ToggleGroup label="Colour source" caption="Colour" value={colour} onChange={setColour} options={[{ value: "measure", label: "Colour by measurement", icon: "palette" }, { value: "queue", label: "Colour by queued bytes", icon: "queue" }]} />
              <Select label="Colour map" value={rampName} onChange={setPalette} options={RAMP_OPTIONS} className="w-[132px]" />
              <ToggleGroup label="Scale" caption="Scale" value={scale} onChange={setScale} options={[{ value: "linear", label: "Linear", icon: "linear" }, { value: "log", label: "Log", icon: "log" }]} />
            </>
          )}
          <IconToggle label="Selected + peers" icon="focus" pressed={scope === "peers"} onChange={(on) => setScope(on ? "peers" : "all")} />
          <IconToggle label="Edge labels" icon="labels" pressed={labels === "on"} onChange={(on) => setLabels(on ? "on" : "off")} />
          <RouterLink
            to={`/overview${fullPage ? "" : "/topology"}${search.size ? `?${search}` : ""}`}
            aria-label={toggle}
            title={toggle}
            className={GRAPH_BUTTON}
          >
            <Icon name={fullPage ? "collapse" : "expand"} size={14} />
          </RouterLink>
        </div>
      </header>

      <div ref={host} onPointerDown={onPointerDown} data-full={fullPage} className="dotgrid relative min-h-0 flex-1 cursor-grab touch-none overflow-hidden">
        <svg {...graphNav} className="graph size-full" role="group" aria-label="Observed node connections. Select a node or connection to inspect. Arrows move between them.">
          <g data-scene transform={`translate(${camera.x} ${camera.y}) scale(${camera.k})`}>
            {graph.groups.map((g) => (
              <g key={g.role}>
                <rect x={g.x} y={g.y} width={g.w} height={g.h} rx="6" fill="color-mix(in srgb, var(--panel-raised) 35%, transparent)" stroke="var(--border)" strokeDasharray="5 4" />
                <text x={g.x + 12} y={g.y + 20} fill="var(--muted)" fontSize="10">{g.role}</text>
              </g>
            ))}
            {reading.links.map(({ link, index }) => {
              const path = routes[index];
              if (!path) return null;
              const value = spec.value(link);
              const f = fraction(value, max, scale === "log");
              const picked = chosen?.id === link.id;
              const active = hovered === link.id || picked;
              const related = selectedId === link.source || selectedId === link.target;
              // Pointing at a node or an edge lifts it out of the bundle.
              const spotlight = hoveredNode ?? (hovered ? "" : null);
              const lit = spotlight == null ? !fullPage || related || active || !selectedId : active || link.source === spotlight || link.target === spotlight;
              const stroke = fullPage
                ? f == null ? "var(--edge)" : ramp(colour === "queue" ? (fraction(link.queued, maxQueue, scale === "log") ?? 0) : f, rampName)
                : link.stale ? "var(--muted)" : related ? "var(--accent)" : "var(--edge)";
              const label = `${link.source} ↔ ${link.target} · ${spec.format(value)} · click to examine`;
              const choose = () => {
                selectConnection(connectionRef(link.node, link.lanes[0]));
              };
              return (
                <g
                  key={link.id}
                  className="edge-group"
                  role="button"
                  tabIndex={-1}
                  aria-label={label}
                  data-no-pan
                  data-edge={link.id}
                  data-value={value ?? "unknown"}
                  onClick={choose}
                  onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), choose())}
                  onPointerEnter={() => setHovered(link.id)}
                  onPointerLeave={() => setHovered(null)}
                  onFocus={() => setHovered(link.id)}
                  onBlur={() => setHovered(null)}
                >
                  <title>{label}</title>
                  <path className="hit" d={path.d} />
                  {picked && <path className="edge" d={path.d} stroke="var(--text)" strokeWidth={(f == null ? 1 : 1.2 + f * 5.5) + 4} opacity={0.9} />}
                  <path
                    className="edge"
                    d={path.d}
                    stroke={stroke}
                    strokeWidth={f == null ? 1 : fullPage ? 1.2 + f * 5.5 : 0.7 + (link.width - 1) * 0.3}
                    strokeDasharray={f == null ? "4 5" : undefined}
                    opacity={!lit ? (spotlight == null ? 0.3 : 0.1) : fullPage ? (active || theme === "light" ? 1 : 0.78) : related || active ? 0.9 : faint}
                  />
                  {labels === "on" && value != null && (lit || spotlight == null) && <EdgeLabel x={path.label[0]} y={path.label[1]} value={value} format={spec.format} />}
                </g>
              );
            })}
            {reading.views.map((view) => {
              const at = drag.positions.get(view.node.id);
              return at && <NodeCard key={view.node.id} view={view} x={at.x} y={at.y} selected={view.node.id === selectedId} onSelect={() => !drag.wasDrag() && selectNode(view.node.id)} onDrag={(e) => drag.onPointerDown(view.node.id, e)} onHover={(over) => setHoveredNode(over ? view.node.id : null)} />;
            })}
          </g>
        </svg>

        <GraphControls k={camera.k} zoom={zoom} fit={fit} drag={drag}>
          <button type="button" disabled={!selectedId || !drag.positions.has(selectedId)} onClick={() => focus(drag.positions.get(selectedId!))} className={GRAPH_BUTTON}>Focus</button>
        </GraphControls>
      </div>

      <footer data-no-pan className="flex shrink-0 items-center gap-2 border-t border-border px-[11px] py-1.5 text-[10px] text-muted">
        <span className="size-1 rounded-full bg-accent" />
        <span>{links.length < all.length ? `${links.length} of ${all.length} links · limit` : `${links.length} links · ${ends} ends`}</span>
        {total > MAX_NODES && <span>{MAX_NODES} node limit ·</span>}
        <Info label="About the graph">One edge per pair of nodes and direction: the sum of the connections that one node dials to the other, one for each lane, measured at the lexically first node that reports them. A connection has two ends, one on each node. The Connections tile and tab count ends. The graph does not show edges to nodes outside the view.</Info>
        {fullPage && selectedId && <RouterLink to={nodePath(selectedId)} className="ml-auto hover:text-text">Open {selectedId} ↗</RouterLink>}
      </footer>
    </section>
  );
}
