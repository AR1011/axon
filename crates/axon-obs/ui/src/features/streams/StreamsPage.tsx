import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import { getJson, type DiskStats, type NodeView, type StreamDetails, type StreamStats } from "../../api";
import { Badge, Count, Empty, Field } from "../../components/atoms";
import { ExportButton } from "../../components/Export";
import { Panel } from "../../components/Panel";
import { rowNav } from "../../components/rowLink";
import { Search } from "../../components/Search";
import { Segmented } from "../../components/Segmented";
import { Sparkline } from "../../components/Sparkline";
import { SortHead, useSort, type Accessors } from "../../components/sort";
import { useHistory, useNow, useUrlState } from "../../hooks";
import { seriesLine } from "../../model/history";
import { ago, bytes, bytesPerSecond, count, perSecond, sum } from "../../model/format";
import { joinPoints, latest, MAX_PLOTTED, ratePoints, rates, seamGap, type P } from "../../model/series";
import { flowEdges, pushLine } from "../../model/streamGraph";
import { useLive } from "../../store";
import { StreamGraph, type RatedEdge } from "./StreamGraph";
import { NodeName } from "../../components/Flag";

const RESET_REASONS = ["Generation changed", "Slow consumer", "History unavailable", "Gap", "Invalid recovery", "Global capacity"];
const END_REASONS = ["Unsubscribed", "Session replaced", "Session expired", "Revoked", "Slow consumer", "Stream closed", "Upstream lost"];
const PUSH_SKIPPED = ["Duplicate", "Gap", "Retired"];
const NAMESPACE_LIMITS = ["Subscriptions", "Ring bytes", "Records per second", "Bytes per second", "Open streams"];
const REFUSALS = ["", "Forbidden", "Unknown stream", "History unavailable", "Generation mismatch", "Unavailable", "Limit exceeded", "Loop"];

export interface Row {
  view: NodeView;
  stats: StreamStats;
  kind: "origin" | "relay" | "leaf" | "idle";
  parents: string;
  publishedRate: number | null;
  deliveredRate: number | null;
  inRate: number | null;
  outRate: number | null;
  resets: number;
  /** Resets this browser has seen in the last `RESET_WINDOW` ms. */
  recentResets: number;
}

const rate = (n: number | undefined) => (n == null ? null : Math.max(0, n));

/** What this browser session has seen of each node incarnation, for the trend lines when no history store answers. */
const session = new Map<string, { records: P[]; resets: P[] }>();
const who = (view: NodeView) => `${view.node.id}:${view.snapshot!.incarnation}`;

function remember(node: string, t: number, records: number | null, resets: number) {
  const trend = session.get(node) ?? { records: [], resets: [] };
  session.set(node, trend);
  if (trend.records.at(-1)?.[0] === t) return;
  trend.records = [...trend.records, [t, records] as P].slice(-MAX_PLOTTED);
  trend.resets = [...trend.resets, [t, resets] as P].slice(-MAX_PLOTTED);
}

const RESET_WINDOW = 15 * 60_000;

/** Lifetime counter minus what it was a window ago, or when this browser first saw the node. */
function recentResets(node: string, t: number, resets: number): number {
  const trend = session.get(node)?.resets ?? [];
  const base = trend.find(([at]) => at >= t - RESET_WINDOW) ?? trend[0];
  return base ? Math.max(0, resets - (base[1] ?? 0)) : 0;
}

function toRow(view: NodeView, stats: StreamStats): Row {
  const h = stats.hub;
  const r = rates(
    `streams:${view.node.id}`,
    view.snapshot!.incarnation,
    view.snapshot!.timestamp_ms,
    new Map([
      ["published", h.published_records],
      ["delivered", h.delivered_records],
      ["in", stats.push_bytes_in],
      ["out", stats.push_bytes_out],
    ]),
  );
  const kind = stats.originated ? "origin" : stats.downstream.some((d) => d.branches > 0) ? "relay" : stats.feeds ? "leaf" : "idle";
  remember(who(view), view.snapshot!.timestamp_ms, rate(r.get(kind === "origin" ? "published" : "delivered")), sum(h.resets));
  return {
    view,
    stats,
    kind,
    parents: stats.parents.map((p) => p.node).join(", "),
    publishedRate: rate(r.get("published")),
    deliveredRate: rate(r.get("delivered")),
    inRate: rate(r.get("in")),
    outRate: rate(r.get("out")),
    resets: sum(h.resets),
    recentResets: recentResets(who(view), view.snapshot!.timestamp_ms, sum(h.resets)),
  };
}

/** A child that rejoins restarts its counters, so a falling counter reads as no rate rather than a negative one. */
function ratedEdges(rows: Row[]): RatedEdge[] {
  const at = new Map(rows.map((r) => [r.view.node.id, r.view.snapshot!]));
  return flowEdges(rows.map((r) => ({ id: r.view.node.id, stats: r.stats }))).map((edge) => {
    const parent = at.get(edge.source)!;
    const r = edge.pushedBytes == null ? null : rates(`stream-edge:${edge.id}`, parent.incarnation, parent.timestamp_ms, new Map([["bytes", edge.pushedBytes], ["records", edge.pushedRecords ?? 0]]));
    const positive = (n: number | undefined) => (n == null || n < 0 ? null : n);
    return { ...edge, bytesRate: positive(r?.get("bytes")), recordsRate: positive(r?.get("records")) };
  });
}

const FIELDS: Accessors<Row> = {
  node: (r) => r.view.node.id,
  kind: (r) => r.kind,
  parents: (r) => r.parents,
  streams: (r) => r.stats.hub.streams,
  published: (r) => r.publishedRate,
  delivered: (r) => r.deliveredRate,
  in: (r) => r.inRate,
  out: (r) => r.outRate,
  resets: (r) => r.resets,
};

function Reasons({ title, labels, counts }: { title: string; labels: string[]; counts: number[] }) {
  const seen = labels.map((label, i) => [label, counts[i]] as const).filter(([label, n]) => label && n > 0);
  return (
    <div className="border-b border-border px-3 py-2">
      <h3 className="pb-1 text-[10px] text-muted">{title}</h3>
      {seen.length ? <dl>{seen.map(([label, n]) => <Field key={label} label={label}>{count(n)}</Field>)}</dl> : <p className="text-[10px] text-muted">None</p>}
    </div>
  );
}

function Disk({ disk: d }: { disk: DiskStats }) {
  return (
    <div className="border-b border-border px-3 py-2" data-disk>
      <h3 className="flex items-center justify-between pb-1 text-[10px] text-muted">History on disk{d.failing ? <Badge tone="danger">{d.failing === "Behind" ? "behind" : `writes fail · ${d.failing.toLowerCase()}`}</Badge> : <Badge tone="success">writes OK</Badge>}</h3>
      <dl>
        <Field label="Streams">{count(d.streams)}</Field>
        <Field label="Held">{bytes(d.bytes)}{d.unwritten_bytes ? ` · ${bytes(d.unwritten_bytes)} unwritten` : ""}</Field>
        <Field label="Writes">{count(d.writes)}{d.write_failures ? <span className="text-warning"> · {count(d.write_failures)} failed</span> : ""}</Field>
        <Field label="Publishes refused"><span className={d.refused ? "text-warning" : ""}>{count(d.refused)}</span></Field>
        <Field label="Segments truncated">{count(d.truncated)}{d.truncated ? ` · ${bytes(d.truncated_bytes)}` : ""}</Field>
      </dl>
      {d.error && <p className="truncate pt-1 text-[10px] text-warning" title={d.error}>{d.error}</p>}
    </div>
  );
}

function Trends({ row }: { row: Row }) {
  const id = row.view.node.id;
  const incarnation = row.view.snapshot!.incarnation;
  const metric = row.kind === "origin" ? "stream.published_records" : "stream.delivered_records";
  const history = useHistory({ metrics: [metric, "stream.resets"], node: id, span: 900_000, points: 90, maxSeries: 16, sum: "node" });
  // Reset reasons are separate series of one counter, added up by the server.
  const retained = (name: string): P[] => seriesLine(history.data, name, (s) => s.incarnation === incarnation, true);
  const live = session.get(who(row.view)) ?? { records: [], resets: [] };
  const resetRate = ratePoints(live.resets.map(([t, v]) => ({ t, v })), 15_000);
  const gap = seamGap(history.data?.step, 15_000);
  const records = joinPoints(retained(metric), live.records, gap);
  const resets = joinPoints(retained("stream.resets"), resetRate, gap);
  const caption = history.data ? "last 15 minutes" : "this session";
  return (
    <div className="grid grid-cols-2 gap-3 border-b border-border px-3 py-2">
      {([[row.kind === "origin" ? "Published" : "Delivered", records, perSecond(latest(records))], ["Resets", resets, perSecond(latest(resets))]] as const).map(([label, points, now]) => (
        <div key={label}>
          <div className="flex items-baseline justify-between text-[10px] text-muted"><span>{label}</span><span className="mono text-text">{now}</span></div>
          <Sparkline points={points} label={`${label} per second · ${caption}`} height={26} />
        </div>
      ))}
    </div>
  );
}

function useDetails(enabled: boolean) {
  return useQuery({
    queryKey: ["streams"],
    enabled,
    refetchInterval: 3000,
    placeholderData: keepPreviousData,
    queryFn: ({ signal }) => getJson<StreamDetails>("streams", { limit: 200 }, signal),
  });
}

/** How far a node is behind the furthest position any node reports for the same stream and generation. */
function StreamList({ node, details }: { node: string; details?: StreamDetails }) {
  const mine = details?.nodes[node];
  const furthest = useMemo(() => {
    const best = new Map<string, number>();
    for (const other of Object.values(details?.nodes ?? {})) for (const s of other.streams) if (s.sequence != null) best.set(`${s.namespace}/${s.name}@${s.generation}`, Math.max(best.get(`${s.namespace}/${s.name}@${s.generation}`) ?? 0, s.sequence));
    return best;
  }, [details]);
  if (!details) return <Empty title="Wait for the streams" detail="The console asks each node for its streams." />;
  if (!mine?.streams.length) return <Empty title="No streams on this node" detail={details.errors[node] ?? "The node does not publish or pull a stream now."} />;
  return (
    <div className="scroll-y flex-1">
      <table>
        <thead><tr><th style={{ width: "42%" }}>Stream</th><th style={{ width: "24%" }}>From</th><th className="text-right" style={{ width: "18%" }}>Position</th><th className="text-right" style={{ width: "16%" }}>Behind</th></tr></thead>
        <tbody>
          {mine.streams.map((s) => {
            const key = `${s.namespace}/${s.name}`;
            const behind = s.sequence == null ? null : s.origin ? 0 : (furthest.get(`${key}@${s.generation}`) ?? s.sequence) - s.sequence;
            const from = s.origin ? "published here" : s.path.length ? s.path.join(" → ") : (s.parent ?? "no parent");
            const note = [s.joining && `moves to ${s.joining}`, ...s.refused].filter(Boolean).join(" · ");
            return (
              <tr key={key} data-stream={key}>
                <td title={`${key} · generation ${s.generation ?? "unknown"} · ${s.local_subscribers} local subscribers · feeds ${s.downstream_branches} nodes`}>{s.name}<small className="block truncate font-sans text-[10px] text-muted">{s.namespace}</small></td>
                <td className={`font-sans leading-tight whitespace-normal ${!s.origin && !s.parent ? "text-warning" : "text-muted"}`} title={[from, note].filter(Boolean).join(" · ")}>{from}{s.joining ? ` ⇢ ${s.joining}` : ""}{s.refused.length > 0 && <small className="block truncate text-[10px] text-warning">{s.refused.join(" · ")}</small>}</td>
                <td className="text-right">{s.sequence == null ? "—" : count(s.sequence)}</td>
                <td className={`text-right ${behind ? "text-warning" : "text-muted"}`}>{behind == null ? "—" : behind === 0 ? "0" : count(behind)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {mine.total > mine.streams.length && <p className="px-3 py-2 text-[10px] text-muted">The list shows {mine.streams.length} of {count(mine.total)} streams.</p>}
    </div>
  );
}

function EventList({ node, details }: { node: string; details?: StreamDetails }) {
  const now = useNow();
  const [scope, setScope] = useUrlState<"node" | "all">("sev", "node");
  const events = useMemo(
    () => Object.entries(details?.nodes ?? {}).filter(([id]) => scope === "all" || id === node).flatMap(([id, d]) => d.events.map((e) => ({ ...e, node: id }))).sort((a, b) => b.time_ms - a.time_ms).slice(0, 200),
    [details, scope, node],
  );
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="flex shrink-0 items-center justify-between border-b border-border px-3 py-1.5 text-[10px] text-muted">
        <span>{events.length} recent events</span>
        <Segmented small label="Events of" value={scope} onChange={setScope} options={[{ value: "all", label: "All nodes" }, { value: "node", label: "This node" }]} />
      </div>
      {!details ? <Empty title="Wait for the events" /> : !events.length ? <Empty title="No stream events" detail="Parent changes, refusals and resets show here when they occur." /> : (
        <ul className="scroll-y flex-1">
          {events.map((e, i) => (
            <li key={`${e.node}:${e.time_ms}:${i}`} data-stream-event={e.kind} className="border-b border-border px-3 py-1.5 text-[11px]">
              <div className="flex items-baseline justify-between gap-2">
                <span className={/lost|refused|reset|orphaned|expired/.test(e.kind) ? "text-warning" : ""}>{e.kind[0].toUpperCase() + e.kind.slice(1).replaceAll("_", " ")}</span>
                <span className="shrink-0 text-[10px] text-muted">{ago(now - e.time_ms)}</span>
              </div>
              <div className="truncate text-[10px] text-muted" title={`${e.node} · ${e.stream} · ${e.detail}`}><NodeName id={e.node} link={false} className="mono" />{e.stream ? ` · ${e.stream}` : ""} · {e.detail}</div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function Detail({ row, edges }: { row: Row; edges: RatedEdge[] }) {
  const { stats } = row;
  const h = stats.hub;
  const feeds = edges.filter((e) => e.source === row.view.node.id && !e.joining);
  return (
    <div className="scroll-y flex-1">
      <Trends row={row} />
      <dl className="border-b border-border px-3 py-2">
        <Field label="Streams">{count(h.streams)}{stats.originated ? ` · ${stats.originated} published here` : ""}</Field>
        <Field label="Pulled from a parent">{count(stats.feeds)}{stats.orphaned ? ` · ${stats.orphaned} without a parent` : ""}</Field>
        <Field label="Subscriptions">{count(h.subscriptions)}{h.recovering ? ` · ${h.recovering} in replay` : ""}</Field>
        <Field label="Downstream sessions">{count(h.sessions - 1)}</Field>
        <Field label="Queued">{bytes(h.queued_bytes)} · {count(h.queued_records)} records</Field>
        <Field label="Published">{count(h.published_records)} records · {bytes(h.published_bytes)}</Field>
        <Field label="Delivered">{count(h.delivered_records)} records · {perSecond(row.deliveredRate)}</Field>
        <Field label="Replayed">{count(h.replayed_records)} records</Field>
        <Field label="Duplicates dropped">{count(h.duplicate_records)} records</Field>
        <Field label="Pushes resent"><span className={stats.push_resent ? "text-warning" : ""}>{count(stats.push_resent)}</span></Field>
        <Field label="Waits for the node cap">{count(stats.push_capped)}</Field>
        <Field label="Parents lost">{count(stats.parents_lost)}</Field>
        <Field label="Moved to a better parent">{count(stats.switches)}</Field>
        <Field label="Local continuity breaks">{count(stats.local_resets)}</Field>
        <Field label="Publishes rejected"><span className={h.publish_rejected ? "text-warning" : ""}>{count(h.publish_rejected)}</span></Field>
        <Field label="Recovery blocked"><span className={h.recovery_blocked ? "text-warning" : ""}>{count(h.recovery_blocked)}</span></Field>
        <Field label="Batches coalesced">{count(h.coalesced)} of {count(h.published_batches)}</Field>
        <Field label="Demand keys">{count(h.demand_keys)}</Field>
        <Field label="Retained in rings and state">{bytes(stats.retained_bytes)}</Field>
        <Field label="Interests sent">{count(stats.interests_sent)} · {count(stats.interest_keys)} keys</Field>
        <Field label="Interests failed"><span className={stats.interests_failed ? "text-warning" : ""}>{count(stats.interests_failed)}</span>{stats.resyncs ? ` · ${count(stats.resyncs)} resent whole` : ""}</Field>
      </dl>
      {stats.disk && <Disk disk={stats.disk} />}
      <div className="border-b border-border px-3 py-2">
        <h3 className="pb-1 text-[10px] text-muted">Parents</h3>
        {stats.parents.length ? (
          <dl>{stats.parents.map((p) => <Field key={p.node} label={p.node}>{p.keys} streams{p.joining ? ` · ${p.joining} joining` : ""}</Field>)}</dl>
        ) : (
          <p className="text-[10px] text-muted">{stats.feeds ? "No parent is reachable" : "This node pulls nothing"}</p>
        )}
      </div>
      <div className="border-b border-border px-3 py-2">
        <h3 className="pb-1 text-[10px] text-muted">Downstream</h3>
        {feeds.length ? (
          <dl>
            {feeds.map((e) => (
              <Field key={e.target} label={e.target}>
                {e.streams} streams · {bytesPerSecond(e.bytesRate)}{e.queued ? ` · ${bytes(e.queued)} queued` : ""}
                {e.push && <span className="block text-[10px] text-muted">{pushLine(e.push)}</span>}
              </Field>
            ))}
          </dl>
        ) : (
          <p className="text-[10px] text-muted">No node pulls from this one</p>
        )}
      </div>
      <Reasons title="Subscriptions reset" labels={RESET_REASONS} counts={h.resets} />
      <Reasons title="Subscriptions ended" labels={END_REASONS} counts={h.ends} />
      <Reasons title="Refused by a parent" labels={REFUSALS} counts={stats.refusals} />
      <Reasons title="Pushes not applied" labels={PUSH_SKIPPED} counts={stats.push_skipped} />
      <Reasons title="Refused at a namespace limit" labels={NAMESPACE_LIMITS} counts={h.quota_refused} />
    </div>
  );
}

export function StreamsPage() {
  const nodes = useLive((s) => s.overview?.nodes);
  const rows = useMemo(() => {
    const rows = (nodes ?? []).flatMap((view) => (view.snapshot?.streams ? [toRow(view, view.snapshot.streams)] : []));
    for (const id of session.keys()) if (!nodes?.some((n) => n.node.id === id)) session.delete(id);
    return rows;
  }, [nodes]);
  const [q, setQ] = useUrlState("sq", "");
  const [selectedId, select] = useUrlState("snode", "");
  const matching = useMemo(() => rows.filter((r) => `${r.view.node.id} ${r.kind} ${r.parents}`.toLowerCase().includes(q.trim().toLowerCase())), [rows, q]);
  const sort = useSort(matching, FIELDS, "s", "node");
  const selected = rows.find((r) => r.view.node.id === selectedId) ?? sort.sorted[0];
  const orphaned = sum(rows.map((r) => r.stats.orphaned));
  const [view, setView] = useUrlState<"graph" | "table">("sview", "graph");
  const [tab, setTab] = useUrlState<"summary" | "streams" | "events">("stab", "summary");
  const edges = useMemo(() => ratedEdges(rows), [rows]);
  const details = useDetails(tab !== "summary").data;
  // What the origins send against what they would send if every leaf pulled from them itself.
  const sent = sum(rows.filter((r) => r.kind === "origin").map((r) => r.outRate ?? 0));
  const leaves = rows.filter((r) => r.kind === "leaf").length;
  const direct = sum(rows.filter((r) => r.kind === "leaf").map((r) => r.inRate ?? 0));
  return (
    <>
      <h1 className="sr-only-h1">Streams</h1>
      <div className="grid min-h-0 flex-1 grid-cols-[minmax(0,1fr)_minmax(300px,380px)] max-[1100px]:grid-cols-1 max-[1100px]:overflow-y-auto">
        <Panel
          flat
          className="flex-1 max-[1100px]:min-h-[440px]"
          title={<>Stream nodes <Count>{rows.length}</Count>{orphaned > 0 && <Badge tone="warning">{orphaned} without a parent</Badge>}</>}
          controls={
            <>
              {view === "graph" && leaves > 1 && sent > 0 && direct / sent >= 1.25 && (
                <span data-saving className="text-[10px] text-muted max-[900px]:hidden" title="Origins send this quantity to their children. If each leaf pulls directly from an origin, the origins must send all that the leaves receive.">
                  Origins send <span className="mono text-text">{bytesPerSecond(sent)}</span> · direct pulls need <span className="mono text-text">{bytesPerSecond(direct)}</span> (saves {Math.round((1 - sent / direct) * 100)}%)
                </span>
              )}
              {view === "table" && <Search label="Search stream nodes" value={q} onChange={setQ} className="w-[170px] max-[700px]:w-[110px]" />}
              <Segmented small label="View" value={view} onChange={setView} options={[{ value: "graph", label: "Graph" }, { value: "table", label: "Table" }]} />
              <ExportButton dataset={{ name: "Stream nodes", count: matching.length, filters: [q && ["search", q]], rows: () => sort.sorted.map((r) => ({ ...r.stats, node: r.view.node.id, kind: r.kind, parents: r.parents, published_rate: r.publishedRate, delivered_rate: r.deliveredRate, in_rate: r.inRate, out_rate: r.outRate })) }} />
            </>
          }
          footer={view === "graph" ? undefined : (
            <>
              <span>{sort.sorted.length} of {rows.length} nodes</span>
            </>
          )}
        >
          {view === "graph" ? (
            <StreamGraph rows={rows} edges={edges} selectedId={selected?.view.node.id} onSelect={select} />
          ) : !sort.sorted.length ? (
            <Empty title={rows.length ? "No nodes match the filter" : "No stream nodes"} detail={rows.length ? "Use a different name." : "Nodes show here when they run a stream node and report it to telemetry."} />
          ) : (
            <div className="scroll-y flex-1 overflow-x-auto">
              <table className="min-w-[860px]">
                <thead>
                  <tr>
                    <SortHead sort={sort} field="node" label="Node" width="17%" />
                    <SortHead sort={sort} field="kind" label="Role" width="9%" />
                    <SortHead sort={sort} field="parents" label="Pulls from" />
                    <SortHead sort={sort} field="streams" label="Streams" width="76px" align="right" />
                    <SortHead sort={sort} field="published" label="Admitted/s" width="100px" align="right" />
                    <SortHead sort={sort} field="delivered" label="Delivered/s" width="100px" align="right" />
                    <SortHead sort={sort} field="in" label="In" width="96px" align="right" />
                    <SortHead sort={sort} field="out" label="Out" width="96px" align="right" />
                    <SortHead sort={sort} field="resets" label="Resets" width="72px" align="right" />
                  </tr>
                </thead>
                <tbody {...rowNav}>
                  {sort.sorted.map((row) => (
                    <tr key={row.view.node.id} aria-selected={selected === row} tabIndex={-1} onClick={() => select(row.view.node.id)} onKeyDown={(e) => e.target === e.currentTarget && (e.key === "Enter" || e.key === " ") && (e.preventDefault(), select(row.view.node.id))} className="cursor-pointer">
                      <td><NodeName id={row.view.node.id} link={false} /></td>
                      <td className="font-sans text-muted">{row.kind}</td>
                      <td className={row.stats.orphaned ? "text-warning" : ""} title={row.parents}>{row.parents || (row.stats.feeds ? "No parent" : "—")}</td>
                      <td className="text-right">{count(row.stats.hub.streams)}</td>
                      <td className="text-right" title={`${count(row.stats.hub.published_records)} records admitted`}>{perSecond(row.publishedRate)}</td>
                      <td className="text-right" title={`${count(row.stats.hub.delivered_records)} records delivered, with fan-out`}>{perSecond(row.deliveredRate)}</td>
                      <td className="text-right">{bytesPerSecond(row.inRate)}</td>
                      <td className="text-right">{bytesPerSecond(row.outRate)}</td>
                      <td className={`text-right ${row.resets ? "text-warning" : ""}`}>{count(row.resets)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Panel>
        <Panel
          flat
          className="border-l border-border max-[1100px]:border-t max-[1100px]:border-l-0"
          title={selected ? selected.view.node.id : "Stream node"}
          controls={selected && <Segmented small label="Detail" value={tab} onChange={setTab} options={[{ value: "summary", label: "Summary" }, { value: "streams", label: "Streams" }, { value: "events", label: "Events" }]} />}
        >
          {selected && tab === "streams" ? <StreamList node={selected.view.node.id} details={details} /> : selected && tab === "events" ? <EventList node={selected.view.node.id} details={details} /> : selected ? <Detail row={selected} edges={edges} /> : <Empty title="No node selected" detail="Select a node to see its parents, downstream nodes and reset reasons." />}
        </Panel>
      </div>
    </>
  );
}
