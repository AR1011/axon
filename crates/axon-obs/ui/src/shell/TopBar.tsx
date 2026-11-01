import { Suspense, type ComponentType } from "react";
import { Link, useLocation } from "react-router";
import { Icon } from "../components/Icon";
import { Segmented } from "../components/Segmented";
import { useUrlState } from "../hooks";
import { useLive } from "../store";
import { useTip } from "../components/tip";
import { ExportButton } from "../components/Export";
import { ActorHelp, ActorsHelp, ConnectionsHelp, EventsHelp, NodeHelp, NodesHelp, OverviewHelp, StreamsHelp, TopologyHelp, TracesHelp } from "../features/PageHelp";
import { TraceHelp } from "../features/traces/TraceHelp";
import { lazyNamed } from "../lazy";
import { TrendsCaption } from "./Kpis";
import { Status } from "./Status";

/** Lives in the bar's spare room so that a notice coming or going never moves the page. */
function Notices() {
  const error = useLive((s) => s.error);
  const historyError = useLive((s) => s.overview?.history.error);
  const discoveryError = useLive((s) => s.overview?.discovery_error);
  const notices = useLive((s) => s.overview?.nodes.flatMap((n) => (n.snapshot?.notices ?? []).map((x) => `${n.node.id}: ${x.message}`)).join("  ·  "));
  // The server's own sentence, which says what to set (`refusal` in control.rs).
  const refusals = useLive((s) => s.overview?.nodes.flatMap((n) => (n.error?.includes(" refuses to answer: ") ? [n.error] : [])).join("  ·  "));
  const text = [
    error && `${error} · The console tries again automatically. The last view that it received can be stale.`,
    refusals,
    discoveryError && `Discovery: ${discoveryError} · Membership is the last good snapshot.`,
    historyError && `History: ${historyError} · Live telemetry remains available.`,
    notices,
  ]
    .filter(Boolean)
    .join("  ·  ");
  return (
    <span role={error || discoveryError || refusals ? "alert" : "status"} title={text} className="min-w-0 flex-1 truncate text-[10px] text-warning">
      {text}
    </span>
  );
}

// From the page's own chunk, which its route loads anyway.
const ClusterHelp = lazyNamed(() => import("../features/cluster/ClusterPage"), "ClusterHelp");
const DeadLetterHelp = lazyNamed(() => import("../features/dead/DeadLettersPage"), "DeadLetterHelp");
const ExploreHelp = lazyNamed(() => import("../features/explore/ExplorePage"), "ExploreHelp");
const PlacementHelp = lazyNamed(() => import("../features/placement/PlacementPage"), "PlacementHelp");
const WorkloadHelp = lazyNamed(() => import("../features/workload/WorkloadPage"), "WorkloadHelp");
const HELP: Record<string, ComponentType> = { overview: OverviewHelp, cluster: ClusterHelp, placement: PlacementHelp, workload: WorkloadHelp, explore: ExploreHelp, nodes: NodesHelp, actors: ActorsHelp, connections: ConnectionsHelp, streams: StreamsHelp, "dead-letters": DeadLetterHelp, traces: TracesHelp, events: EventsHelp };
/** Detail routes explain the one thing, not the list; the overview's one is its topology at full size. */
const DETAIL: Record<string, ComponentType> = { overview: TopologyHelp, nodes: NodeHelp, actors: ActorHelp, traces: TraceHelp };

export function TopBar({ onRefresh, kpis }: { onRefresh: () => void; kpis: boolean }) {
  const parts = useLocation().pathname.split("/").filter(Boolean).map(decodeURIComponent);
  const section = parts[0] ?? "overview";
  const Help = parts.length > 1 ? DETAIL[section] : HELP[section];
  const [period, setPeriod] = useUrlState<"1h" | "24h">("period", "1h");
  const tip = useTip();
  const overview = useLive((s) => s.overview);
  const button = "flex size-[25px] items-center justify-center rounded-sm text-muted hover:bg-raised hover:text-text";
  return (
    <header className="flex h-10 shrink-0 items-center gap-2 border-b border-border px-5 max-[900px]:px-2.5">
      <nav aria-label="Breadcrumb" className="flex min-w-0 items-center gap-1.5 text-[11px] text-muted">
        <Link to="/overview" className="hover:text-text max-[580px]:hidden">Actor cluster</Link>
        <span className="max-[580px]:hidden">/</span>
        <Link to={`/${section}`} className="text-text capitalize hover:underline">
          {section.replace("-", " ")}
        </Link>
        {parts.length > 1 && (
          <>
            <span>/</span>
            <span className="mono truncate text-[10px]">
              {parts.slice(1).map((part, i) => {
                // In every detail route, a segment that names a node links to it.
                const node = (section === "actors" || section === "connections") && (i === 0 || (section === "connections" && i === 1));
                return <span key={i}>{i > 0 && " / "}{node ? <Link to={`/nodes/${encodeURIComponent(part)}`} className="hover:underline">{part}</Link> : part}</span>;
              })}
            </span>
          </>
        )}
      </nav>
      <Notices />
      {kpis && (
        <div className="flex shrink-0 items-center gap-2">
          <span className="text-[10px] text-muted max-[1100px]:hidden">Trends</span>
          <Segmented small label="Trend period" value={period} onChange={setPeriod} options={[{ value: "1h", label: "1h" }, { value: "24h", label: "24h" }]} />
          <span className="max-[1100px]:hidden"><TrendsCaption /></span>
        </div>
      )}
      <div className="flex shrink-0 items-center gap-2">
        {Help && <Suspense><Help /></Suspense>}
        <Status />
        <span className="h-4 w-px bg-border max-[580px]:hidden" />
        <ExportButton dataset={{ name: "Raw overview", count: overview ? 1 : 0, filters: [], raw: "All fields of all nodes, as the dashboard sent them. JSON only.", rows: () => [overview!] }} />
        <button type="button" aria-label="Refresh" onClick={onRefresh} className={button} {...tip.bind("Refresh")}>
          <Icon name="refresh" size={14} />
        </button>
      </div>
      {tip.node}
    </header>
  );
}
