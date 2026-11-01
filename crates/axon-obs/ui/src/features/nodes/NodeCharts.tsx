import { useMemo, useState, type ReactNode } from "react";
import type { NodeView, Snapshot, WorkerStats } from "../../api";
import { COLORS, type Line } from "../../charts/options";
import { flatZero, LinePlot, PlotFrame } from "../../charts/Plots";
import { coverage, RANGES, useDomain, useHistory, useNow, type Range } from "../../hooks";
import { byUnit, type Unit, count } from "../../model/format";
import { lastKnown, line as historyLine, type HistoryData } from "../../model/history";
import { GAP_MS, STALE_MS, type Counter, type NodeRow } from "../../model/live";
import { gaugePoints, joinPoints, latest, ratePoints, seamGap, type P } from "../../model/series";
import { nodeBuffer, useLive } from "../../store";

interface LineSpec {
  name: string;
  metric: string;
  live: Counter;
  rate?: boolean;
  mean?: boolean;
  color?: string;
}
interface ChartSpec {
  title: string;
  unit: Unit;
  lines: LineSpec[];
  host?: boolean;
  scale?: number;
}

const CHARTS: ChartSpec[] = [
  { title: "Process CPU", unit: "%", lines: [{ name: "CPU", metric: "process.cpu_percent", live: "cpu" }] },
  { title: "Resident memory", unit: "bytes", lines: [{ name: "RSS", metric: "process.rss_bytes", live: "rss" }] },
  { title: "Actor count", unit: "count", lines: [{ name: "Actors", metric: "runtime.actors", live: "actors" }] },
  {
    title: "Network throughput",
    unit: "bytes/s",
    lines: [
      { name: "Sent", metric: "connection.sent_bytes", live: "sent", rate: true, color: COLORS.sent },
      { name: "Received", metric: "connection.received_bytes", live: "received", rate: true, color: COLORS.received },
    ],
  },
  {
    title: "Process disk I/O",
    unit: "bytes/s",
    lines: [
      { name: "Read", metric: "process.read_bytes", live: "read", rate: true },
      { name: "Written", metric: "process.written_bytes", live: "written", rate: true },
    ],
  },
  { title: "Outbound queue", unit: "bytes", lines: [{ name: "Reserved bytes", metric: "connection.queued_bytes", live: "queued" }] },
  { title: "Worker utilisation", unit: "ratio", lines: [{ name: "Mean of shared workers", metric: "runtime.worker.utilisation", live: "utilisation", mean: true }] },
  { title: "Messages processed", unit: "/s", lines: [{ name: "Processed", metric: "actor.processed", live: "processed", rate: true }] },
  { title: "Mailbox depth", unit: "count", lines: [{ name: "Queued, all types", metric: "actor.mailbox_depth", live: "mailbox" }] },
  {
    title: "Calls and dead letters",
    unit: "/s",
    lines: [
      { name: "Outbound calls", metric: "cluster.calls_started", live: "calls", rate: true },
      { name: "Dead letters", metric: "runtime.dead_letters", live: "deadLetters", rate: true, color: COLORS.rejected },
    ],
  },
  { title: "Host CPU", unit: "%", host: true, lines: [{ name: "Host CPU", metric: "host.cpu_percent", live: "hostCpu" }] },
  { title: "Host available memory", unit: "bytes", host: true, lines: [{ name: "Available", metric: "host.available_memory_bytes", live: "hostAvailable" }] },
];
export const DISTRIBUTIONS = [
  { metric: "cluster.request_rtt", title: "Request round trip" },
  { metric: "actor.handler", title: "Handler duration" },
] as const;
const CHART_METRICS = [...new Set(CHARTS.flatMap((c) => c.lines.map((l) => l.metric)))];

/** The node page's history of `metrics`, each node boot's series added up but a named pool's: a tab asks for its own. */
export const useNodeHistory = (view: NodeView, span: number, metrics: string[]) => useHistory({ metrics, node: view.node.id, span, points: 144, sum: "node", by: ["pool"] });

function retained(data: HistoryData | undefined, line: LineSpec, incarnation: string): P[] {
  // Another boot of the same node is another process: its series never reach the plot.
  // Named pools have their own strips; one that never sleeps would pin a mean over every worker at its top.
  const r = data?.byMetric[line.metric];
  const series = r?.series.find((s) => s.incarnation === incarnation && !s.labels.pool);
  const points = historyLine(r, series, !!line.rate);
  return line.mean && series?.summed ? points.map(([t, v]) => [t, v == null ? null : v / series.summed!]) : points;
}

/** The boot before `incarnation`: the one the live buffer kept, else the newest other one history holds. */
function earlierBoot(data: HistoryData | undefined, incarnation: string, kept?: string): string | undefined {
  if (kept) return kept;
  let [boot, at] = [undefined as string | undefined, -Infinity];
  for (const r of Object.values(data?.byMetric ?? {}))
    for (const s of r.series) {
      const last = r.from + lastKnown(s) * r.step;
      if (s.incarnation !== incarnation && lastKnown(s) >= 0 && last > at) [boot, at] = [s.incarnation, last];
    }
  return boot;
}

export function NodeCharts({ view, range, recent }: { view: NodeView; range: Range; recent?: ReactNode }) {
  const span = RANGES[range];
  const now = useNow();
  const version = useLive((s) => s.version);
  const incarnation = view.snapshot?.incarnation ?? view.node.incarnation;
  const history = useNodeHistory(view, span, CHART_METRICS);
  const hasHost = !!view.snapshot?.host;
  const stale = !view.snapshot || now - view.snapshot.timestamp_ms > STALE_MS;

  const charts = useMemo(() => {
    const kept = nodeBuffer.earlier(view.node.id);
    const before = earlierBoot(history.data, incarnation, kept?.incarnation);
    const gap = seamGap(history.data?.step);
    const points = (line: LineSpec, boot: string, rows: NodeRow[]) => {
      const raw = rows.map((r) => ({ t: r.t, v: r[line.live] }));
      return joinPoints(retained(history.data, line, boot), line.rate ? ratePoints(raw, GAP_MS) : gaugePoints(raw, GAP_MS), gap);
    };
    return CHARTS.filter((c) => !c.host || hasHost).map((chart) => ({
      chart,
      lines: chart.lines.flatMap((line): Line[] => {
        const current = points(line, incarnation, nodeBuffer.rows(view.node.id, incarnation));
        const earlier = before ? points(line, before, nodeBuffer.rows(view.node.id, before)) : [];
        if (!earlier.some((p) => p[1] != null)) return [{ name: line.name, color: line.color, points: current }];
        // The restart: where this boot's line begins.
        const restart = current.find((p) => p[1] != null)?.[0];
        return [
          { name: line.name, color: line.color, points: current, markAt: restart, markLabel: "restart" },
          { name: `${line.name} · earlier boot`, color: line.color, points: earlier, faded: true },
        ];
      }),
    }));
    // eslint-disable-next-line react-hooks/exhaustive-deps -- `version` stands in for the mutable live buffer
  }, [view.node.id, incarnation, hasHost, history.data, version]);

  const domain = useDomain(span);
  const note = !history.available
    ? "History unavailable · live observations continue"
    : history.error || history.data?.errors.length
      ? (history.error ?? history.data!.errors.join(" · "))
      : history.data?.truncated
        ? "Partial observations · some series were outside the collection limit"
        : coverage(history.data, span);
  // Without history, two live samples: one zero sample would hide a chart for good.
  const settled = !!history.data || !!history.error || (!history.available && charts.some((c) => c.lines.some((l) => l.points.filter((p) => p[1] !== null).length > 1)));
  return (
    <>
      {note && <p className="shrink-0 text-[10px] text-warning">{note}</p>}
      {recent}
      {view.snapshot && <Workers snapshot={view.snapshot} />}
      {settled && <Tiles key={`${view.node.id}/${range}`} charts={charts} range={range} domain={domain} stale={stale} retained={!!history.data} />}
    </>
  );
}

type Tile = { chart: (typeof CHARTS)[number]; lines: Line[] };

/**
 * The charts, those flat at zero hidden. Which ones is decided when it mounts, at the first history answer for a node and
 * range, so tiles never come and go under the reader.
 */
function Tiles({ charts, range, domain, stale, retained }: { charts: Tile[]; range: Range; domain: ReturnType<typeof useDomain>; stale: boolean; retained: boolean }) {
  const [flat] = useState(() => new Set(charts.filter((c) => flatZero(c.lines)).map((c) => c.chart.title)));
  return (
    <>
      {flat.size > 0 && <p className="shrink-0 px-3 text-[10px] text-muted">Hidden, flat at zero over {range}: {[...flat].map((t) => t[0].toLowerCase() + t.slice(1)).join(", ")}</p>}
      <div className="scroll-y grid flex-1 auto-rows-[minmax(220px,1fr)] grid-cols-3 tiles max-[1200px]:grid-cols-2 max-[700px]:grid-cols-1">
        {charts.filter((c) => !flat.has(c.chart.title)).map(({ chart, lines }) => (
          <PlotFrame
            key={chart.title}
            title={chart.title}
            value={byUnit(chart.unit, latest(lines[0].points))}
            info={`The chart shows the selected range up to now. It shows the current incarnation of this node, and the boot before it in a lighter colour behind a restart mark${stale ? " · the source is stale" : ""}.`}
          >
            <LinePlot lines={lines} unit={chart.unit} domain={domain} label={`${chart.title} ${retained ? "from retained history and live observations" : "from live observations"}`} />
          </PlotFrame>
        ))}
      </div>
    </>
  );
}

/** One strip per pool, a bar per worker. Pool workers are named as their threads are, `{pool}-{i}`. */
function Workers({ snapshot: s }: { snapshot: Snapshot }) {
  const pools = [
    { label: "Workers", name: (i: number) => `worker ${i}`, util: s.worker_utilisation, workers: s.runtime.workers },
    ...(s.runtime.pools ?? []).map((p, k) => ({ label: p.name, name: (i: number) => `${p.name}-${i}`, util: s.pool_utilisation?.[k] ?? [], workers: p.workers })),
  ].filter((p) => p.util.length);
  return pools.map((p, i) => <Strip key={i} {...p} />);
}

function Strip({ label, name, util, workers }: { label: string; name: (i: number) => string; util: number[]; workers: WorkerStats[] }) {
  return (
    <div className="flex shrink-0 items-end gap-2 rounded-md border border-border bg-panel px-3 py-2 text-[10px]">
      <span className="w-[92px] truncate text-muted" title={`${label} · ${util.length}`}>{label} · {util.length}</span>
      <div className="flex h-[26px] flex-1 items-end gap-[3px]" role="img" aria-label={util.map((u, i) => `${name(i)} ${Math.round(u * 100)}%`).join(", ")}>
        {util.map((u, i) => (
          <span key={i} title={`${name(i)} · ${Math.round(u * 100)}% busy · ${count(workers[i]?.queued)} queued`} className={`max-w-[18px] flex-1 rounded-[2px] ${u > 0.85 ? "bg-warning" : "bg-accent"}`} style={{ height: `${Math.max(6, u * 100)}%`, opacity: 0.35 + u * 0.65 }} />
        ))}
      </div>
      <span className="mono w-[72px] text-right text-muted">max {Math.round(Math.max(...util) * 100)}%</span>
    </div>
  );
}
