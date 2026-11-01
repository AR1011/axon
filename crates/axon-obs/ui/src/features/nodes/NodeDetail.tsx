import { useQueryClient } from "@tanstack/react-query";
import { Link, useParams } from "react-router";
import { Badge, Empty, Facts } from "../../components/atoms";
import { Icon } from "../../components/Icon";
import { Flag } from "../../components/Flag";
import { Identity } from "../../components/Identity";
import { Select } from "../../components/Select";
import { Tabs } from "../../components/Tabs";
import { nodeStatus, RANGE_OPTIONS, RANGES, useNow, useUrlState, type Range } from "../../hooks";
import { ago, count } from "../../model/format";
import { isDown } from "../../model/live";
import { useLive } from "../../store";
import { Inventory } from "../actors/Inventory";
import { ConnectionsLayout } from "../connections/ConnectionsPage";
import { RecentEvents, Timeline } from "../events/Timeline";
import { nodeById, placeOf, roleOf } from "../select";
import { NodeCharts } from "./NodeCharts";
import { NodeDistributions } from "./NodeDistributions";
import { NodeMetrics } from "./NodeMetrics";

/** Labels the page shows above the tabs, so a node with only these has no metadata tab. */
const SHOWN = ["role", "country", "region"];
const TABS = ["resources", "actors", "distributions", "connections", "metrics", "events", "metadata"] as const;
type Tab = (typeof TABS)[number];

export function NodeDetail() {
  const { nodeId = "" } = useParams();
  const now = useNow();
  const queries = useQueryClient();
  const view = useLive((s) => nodeById(s, nodeId));
  const [tab, setTab] = useUrlState<Tab>("view", "resources");
  const [range, setRange] = useUrlState<Range>("range", "15m");
  const events = useLive((s) => s.overview?.events.filter((e) => e.node_id === nodeId && e.time_ms >= now - RANGES[range]).length ?? 0);
  if (!view)
    return (
      <Empty title="Node not in the current membership" detail="Possibly the node left, or discovery is not available.">
        <Link to="/nodes" className="text-accent hover:underline">Back to nodes</Link> · <Link to={`/events?eq=${encodeURIComponent(nodeId)}`} className="text-accent hover:underline">Events for {nodeId}</Link>
      </Empty>
    );
  const status = nodeStatus(view, now);
  const s = view.snapshot;
  const charted = tab === "resources" || tab === "distributions";
  const ranged = charted || tab === "connections" || tab === "events";
  const strip = [
    ["Incarnation", <Identity key="i" value={view.node.incarnation} label="Process incarnation" />],
    ["Address", <code key="a" className="mono">{view.node.address}</code>],
    ["Role", roleOf(view) || "—"],
    ["Location", placeOf(view).text || "—"],
    ["PID", s?.process?.pid ?? "—"],
    ["Actors", count(s?.actor_total)],
    ["Sessions", count(s?.cluster.engine.peers.filter((p) => !isDown(p)).length)],
  ] as const;
  return (
    <>
      <header className="flex shrink-0 items-center gap-2">
        <h1 className="mono flex min-w-0 items-center gap-2 text-[14px] font-semibold tracking-[-0.25px]">
          <Flag node={view.node.id} /><span className="truncate">{nodeId}</span>
        </h1>
        <span className="ml-auto"><Badge tone={status.tone}>{status.label}</Badge></span>
      </header>
      {view.leftAt && <p role="status" className="shrink-0 rounded-md border border-border bg-panel px-3 py-1.5 text-[11px] text-warning">This node left the membership {ago(now - view.leftAt)}. The data below is the last data that it reported. Retained history and events still have data for it.</p>}
      <Facts className="shrink-0 rounded-md border border-border bg-panel" items={strip} />
      <div className="shrink-0 rounded-md border border-border bg-panel">
        <Tabs
          tall
          label="Node views"
          value={tab}
          onChange={setTab}
          tabs={TABS.filter((value) => value !== "metadata" || Object.keys(view.node.metadata).some((key) => !SHOWN.includes(key))).map((value) => ({ value, label: value, count: value === "events" ? events : undefined }))}
          trailing={
            ranged && (
              <>
                <Select label="Time range" value={range} onChange={setRange} options={RANGE_OPTIONS} />
                {charted && <button type="button" aria-label="Refresh history" title="Refresh history" onClick={() => queries.invalidateQueries({ queryKey: ["history"] })} className="flex size-[25px] items-center justify-center rounded-sm text-muted hover:bg-raised hover:text-text">
                  <Icon name="refresh" size={14} />
                </button>}
              </>
            )
          }
        />
      </div>
      {tab === "resources" && <NodeCharts view={view} range={range} recent={<RecentEvents node={nodeId} onMore={() => setTab("events")} />} />}
      {tab === "distributions" && <NodeDistributions view={view} span={RANGES[range]} />}
      {tab === "actors" && <Inventory scope={nodeId} />}
      {tab === "connections" && <ConnectionsLayout scope={nodeId} pageRange={range} />}
      {tab === "metrics" && <NodeMetrics view={view} />}
      {tab === "events" && <Timeline node={nodeId} pageRange={range} />}
      {tab === "metadata" &&
        (Object.keys(view.node.metadata).length ? (
          <dl className="scroll-y grid flex-1 grid-cols-[minmax(120px,240px)_1fr] content-start rounded-md border border-border bg-panel px-3 py-2 text-[11px]">
            {Object.entries(view.node.metadata).map(([key, value]) => (
              <div key={key} className="contents"><dt className="mono border-b border-border py-1.5 text-muted">{key}</dt><dd className="mono border-b border-border py-1.5 break-all">{value}</dd></div>
            ))}
          </dl>
        ) : (
          <Empty title="No metadata" detail="This node advertises no labels." />
        ))}
    </>
  );
}
