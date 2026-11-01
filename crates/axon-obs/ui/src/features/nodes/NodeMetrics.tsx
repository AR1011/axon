import { useMemo } from "react";
import { Info } from "../../components/Info";
import type { NodeView } from "../../api";
import { Empty } from "../../components/atoms";
import { ExportButton } from "../../components/Export";
import { Panel } from "../../components/Panel";
import { SortHead, useSort, type Accessors } from "../../components/sort";
import { useUrlState } from "../../hooks";
import { count, duration, perSecond, shortType, sum } from "../../model/format";
import { boundText, mean, quantile } from "../../model/histogram";
import { flatten, rates } from "../../model/series";

interface TypeRow {
  name: string;
  message: string;
  live: number | null;
  depth: number | null;
  processed: number | null;
  rejected: number | null;
  restarts: number | null;
  timeouts: number | null;
  samples: number;
  mean: number | null;
  p95: number | null;
}

const FIELDS: Accessors<TypeRow> = {
  name: (r) => `${r.name} ${r.message}`,
  live: (r) => r.live,
  depth: (r) => r.depth,
  processed: (r) => r.processed,
  rejected: (r) => r.rejected,
  restarts: (r) => r.restarts,
  timeouts: (r) => r.timeouts,
  mean: (r) => r.mean,
  p95: (r) => r.p95,
};

interface CounterRow {
  name: string;
  value: number;
  rate: number | null;
}
const COUNTER_FIELDS: Accessors<CounterRow> = { name: (r) => r.name, value: (r) => r.value, rate: (r) => r.rate };

export function NodeMetrics({ view }: { view: NodeView }) {
  const s = view.snapshot;
  const types = useMemo<TypeRow[]>(
    () =>
      (s?.actor_types ?? []).flatMap((t) => {
        const handler = t.totals.timings?.handler;
        const total: TypeRow = {
          name: shortType(t.actor_type),
          message: "",
          live: t.live,
          depth: t.mailbox_depth,
          processed: t.totals.processed,
          rejected: sum(t.totals.rejected),
          restarts: t.totals.restarts,
          timeouts: t.totals.ask_timeouts,
          samples: handler?.count ?? 0,
          mean: handler ? mean(handler) : null,
          p95: handler ? quantile(handler, 0.95) : null,
        };
        const messages = (t.totals.timings?.messages ?? []).map((m) => ({
          ...total,
          message: m.label,
          live: null,
          depth: null,
          processed: null,
          rejected: null,
          restarts: null,
          timeouts: null,
          samples: m.handler.count,
          mean: mean(m.handler),
          p95: quantile(m.handler, 0.95),
        }));
        return [total, ...messages];
      }),
    [s],
  );
  const counters = useMemo<CounterRow[]>(() => {
    if (!s) return [];
    const flat = (prefix: string, value: unknown): CounterRow[] => flatten(prefix, value).map(([name, value]) => ({ name, value, rate: null }));
    const { peers, request_rtt: rtt, ...rest } = s.cluster.engine;
    const engine = { ...rest, request_rtt: { count: rtt.count, sum_ns: rtt.sum_ns } };
    const rows = [...flat("system", s.system), ...flat("cluster", { ...s.cluster, engine }), ...flat("runtime", { actors: s.runtime.actors, workers: s.runtime.workers.length, pools: Object.fromEntries((s.runtime.pools ?? []).map((p) => [p.name, { workers: p.workers.length }])) })];
    const perSec = rates(view.node.id, s.incarnation, s.timestamp_ms, new Map(rows.map((r) => [r.name, r.value])));
    return rows.map((r) => ({ ...r, rate: perSec.get(r.name) ?? null }));
  }, [s, view.node.id]);
  const typeSort = useSort(types, FIELDS, "m", "processed", "desc");
  const [zeros, setZeros] = useUrlState<"show" | "hide">("kzero", "hide");
  const visible = useMemo(() => (zeros === "hide" ? counters.filter((c) => c.value !== 0) : counters), [counters, zeros]);
  const counterSort = useSort(visible, COUNTER_FIELDS, "k", "name");
  if (!s) return <Empty title="No snapshot" detail="The console has no observation of this node." />;
  return (
    <div className="grid min-h-0 flex-1 grid-cols-[minmax(0,2fr)_minmax(0,1fr)] tiles max-[1100px]:grid-cols-1 max-[1100px]:overflow-y-auto">
      <Panel title="Actor types and messages" controls={<><Info label="About these timings">The node samples handler timings. The rows for each message use the same population rules as their type.</Info><ExportButton dataset={{ name: "Actor types", count: s?.actor_types.length ?? 0, filters: [["node", view.node.id]], rows: () => s?.actor_types ?? [] }} /></>}>
        <div className="scroll-y flex-1 overflow-x-auto">
          <table className="min-w-[760px]">
            <thead>
              <tr>
                <SortHead sort={typeSort} field="name" label="Type / message" width="18%" />
                <SortHead sort={typeSort} field="live" label="Live" width="8%" align="right" />
                <SortHead sort={typeSort} field="depth" label="Mailbox" width="9%" align="right" />
                <SortHead sort={typeSort} field="processed" label="Processed" width="12%" align="right" />
                <SortHead sort={typeSort} field="rejected" label="Rejected" width="10%" align="right" />
                <SortHead sort={typeSort} field="restarts" label="Restarts" width="9%" align="right" />
                <SortHead sort={typeSort} field="timeouts" label="Ask timeouts" width="12%" align="right" />
                <SortHead sort={typeSort} field="mean" label="Mean handler" width="12%" align="right" />
                <SortHead sort={typeSort} field="p95" label="p95 ≤" width="10%" align="right" />
              </tr>
            </thead>
            <tbody>
              {typeSort.sorted.map((r) => (
                <tr key={`${r.name}|${r.message}`} title={`${count(r.samples)} timing samples`}>
                  <td>{r.message ? <span className="pl-3 text-muted">↳ {r.message}</span> : r.name}</td>
                  <td className="text-right">{r.message ? "" : count(r.live)}</td>
                  <td className="text-right">{r.message ? "" : count(r.depth)}</td>
                  <td className="text-right">{r.message ? "" : count(r.processed)}</td>
                  <td className={`text-right ${r.rejected ? "text-warning" : ""}`}>{r.message ? "" : count(r.rejected)}</td>
                  <td className={`text-right ${r.restarts ? "text-warning" : ""}`}>{r.message ? "" : count(r.restarts)}</td>
                  <td className={`text-right ${r.timeouts ? "text-warning" : ""}`}>{r.message ? "" : count(r.timeouts)}</td>
                  <td className="text-right">{duration(r.mean)}</td>
                  <td className="text-right">{boundText(r.p95)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
      <Panel
        title="Counters"
        controls={<button type="button" aria-pressed={zeros === "hide"} onClick={() => setZeros(zeros === "hide" ? "show" : "hide")} className={`h-6 rounded-md border border-border px-2 text-[10px] ${zeros === "hide" ? "bg-raised text-text" : "text-muted"}`}>Hide zeros</button>}
        footer={<><span>{visible.length} of {counters.length} reported values</span><span className="ml-auto"><Info label="About these values">The total after the node started. Change is the change per second between the last two snapshots.</Info></span></>}
      >
        <div className="scroll-y flex-1">
          <table>
            <thead>
              <tr>
                <SortHead sort={counterSort} field="name" label="Name" width="56%" />
                <SortHead sort={counterSort} field="value" label="Value" width="22%" align="right" />
                <SortHead sort={counterSort} field="rate" label="Change" width="22%" align="right" />
              </tr>
            </thead>
            <tbody>
              {counterSort.sorted.map((r) => (
                <tr key={r.name}><td title={r.name}>{r.name}</td><td className="text-right">{r.name.endsWith("_ns") ? duration(r.value) : count(r.value)}</td><td className={`text-right ${r.rate ? "" : "text-muted"}`}>{r.rate === null ? "…" : r.rate === 0 ? "0" : r.name.endsWith("_ns") ? `${duration(r.rate)}/s` : perSecond(r.rate)}</td></tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  );
}
