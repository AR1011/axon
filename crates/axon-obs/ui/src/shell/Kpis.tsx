import { useMemo } from "react";
import { Link } from "react-router";
import { ProviderLogo } from "../components/Icon";
import { Sparkline } from "../components/Sparkline";
import { reporter } from "../features/select";
import { useHistory, useNow, useStaleSince, useUrlState } from "../hooks";
import { ago, bytes, bytesPerSecond, count } from "../model/format";
import { seriesLine } from "../model/history";
import { aggregate } from "../model/kpi";
import type { P } from "../model/series";
import { availableNodes, pending, session, totals, useLive } from "../store";

const METRICS = ["runtime.actors", "cluster.connections", "connection.sent_bytes", "connection.queued_bytes"];

/** The tiles' totals, added up by the server, and the nodes reporting per step; and the trends bar's caption. */
function useTrends() {
  const [period] = useUrlState<"1h" | "24h">("period", "1h");
  const span = period === "1h" ? 3_600_000 : 86_400_000;
  const history = useHistory({ metrics: METRICS, span, points: 144, sum: "cluster", refetchMs: 60_000 });
  const nodes = useHistory({ metrics: ["runtime.actors"], span, points: 144, sum: "node", refetchMs: 60_000 });
  const partial = history.data?.truncated || nodes.data?.truncated;
  const caption = history.data ? `${period} · ${partial ? "partial" : "observed totals"}` : history.available ? "Live session" : "Live · history unavailable";
  return { history, nodes, caption };
}

export function TrendsCaption() {
  return <span className="text-[10px] text-muted max-[580px]:hidden">{useTrends().caption}</span>;
}

export function Kpis() {
  useLive((s) => s.version);
  const live = totals;
  const { history, nodes, caption } = useTrends();

  const retained = useMemo(() => {
    const data = history.data;
    if (!data) return null;
    const total = (metric: string, rate = false) => seriesLine(data, metric, () => true, rate);
    const boots = nodes.data?.byMetric["runtime.actors"];
    return {
      nodes: boots ? aggregate(boots, boots.series, "count") : [],
      actors: total("runtime.actors"),
      connections: total("cluster.connections"),
      throughput: total("connection.sent_bytes", true),
      queued: total("connection.queued_bytes"),
    } as Record<string, P[]>;
  }, [history.data, nodes.data]);

  const healthy = useLive(availableNodes);
  const cluster = useLive((s) => reporter(s.overview)?.snapshot?.cluster);
  const now = useNow();
  const stale = useStaleSince();
  const waiting = useLive(pending);
  // Departures come newest first; one that went without draining is a loss, not a leave.
  const lost = useLive((s) => s.departed).filter((n) => n.leaveReason === null);
  const missing = live.nodes === healthy ? (lost.length ? `${lost.length} lost ${ago(now - lost[0].leftAt!)}` : "all report") : `${live.nodes - healthy} do not report`;
  const health = waiting ?? (stale != null ? `last seen ${ago(now - stale)}` : missing);
  const measured: [string, string, string][] = [
    ["nodes", "Nodes", count(live.nodes)],
    ["actors", "Actors", count(live.actors)],
    ["connections", "Connections", count(live.connections)],
    ["throughput", "Throughput", bytesPerSecond(live.throughput)],
    ["queued", "Queued", bytes(live.queued)],
  ];
  const tiles = waiting ? measured.map(([key, label]): [string, string, string] => [key, label, "—"]) : measured;
  return (
    <section aria-label="Cluster summary" className="grid shrink-0 grid-cols-6 overflow-hidden rounded-md border border-border bg-panel max-[1050px]:grid-cols-3 max-[600px]:grid-cols-2">
      {tiles.map(([key, label, value]) => (
        <div key={key} className={`flex h-[82px] flex-col border-r border-border px-3 py-2 max-[1050px]:h-[72px] max-[1050px]:border-b max-[600px]:h-[52px] ${key === "queued" ? "max-[600px]:col-span-2" : ""}`}>
          <div className="flex items-baseline justify-between gap-2">
            <span className="flex flex-col text-[10px] text-muted">
              {label}
              {key === "nodes" && <span className={stale != null || live.nodes !== healthy || lost.length ? "text-warning" : ""}>{health}</span>}
            </span>
            <span className={`mono text-right text-[15px] tracking-[-0.4px] whitespace-nowrap ${stale != null ? "text-muted opacity-60" : ""}`}>{value}</span>
          </div>
          <div className="mt-auto max-[600px]:hidden">
            <Sparkline points={retained?.[key] ?? session[key as keyof typeof session]} label={`${label} · ${caption}`} height={22} />
          </div>
        </div>
      ))}
      <Link to="/cluster" className="relative h-[82px] px-3 py-2 hover:bg-raised max-[1050px]:h-[72px] max-[1050px]:border-b max-[600px]:col-span-2 max-[600px]:h-[52px]">
        <div className="flex items-baseline justify-between gap-2">
          <span className="text-[10px] text-muted">Cluster</span>
          <span className="text-[10px] text-muted">Open ↗</span>
        </div>
        <dl className="mt-1.5 flex flex-col gap-1 max-[600px]:mt-0.5 max-[600px]:flex-row max-[600px]:gap-3">
          {(
            [
              ["Discovery", cluster?.discovery],
              ["Ownership", cluster ? (cluster.ownership ?? null) : undefined],
            ] as const
          ).map(([label, info]) => (
            <div key={label} className="flex items-center gap-1.5 text-[11px]">
              <dt className="w-[58px] text-[10px] text-muted max-[600px]:w-auto">{label}</dt>
              <dd className="flex min-w-0 items-center gap-1.5 truncate capitalize">
                {info === undefined ? "—" : info === null ? <span className="text-muted normal-case">not configured</span> : <><ProviderLogo kind={info.kind} size={12} />{info.kind}</>}
              </dd>
            </div>
          ))}
        </dl>
      </Link>
    </section>
  );
}
