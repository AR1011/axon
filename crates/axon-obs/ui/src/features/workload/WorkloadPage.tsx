import { useMemo } from "react";
import { HeatPlot } from "../../charts/Plots";
import { Empty } from "../../components/atoms";
import { HelpDialog, Part } from "../../components/HelpDialog";
import { Panel } from "../../components/Panel";
import { Select } from "../../components/Select";
import { useHistory, useUrlState } from "../../hooks";
import { duration, natural, shortType } from "../../model/format";
import { handlerTime, rowHeat } from "../../model/histogram";
import { line } from "../../model/history";
import { useLive } from "../../store";

const SPAN = 900_000;
const POINTS = 90;

export function WorkloadHelp() {
  return (
    <HelpDialog button="What is this" title="What the workload page shows">
      <Part title="Backlog">
        One row per node and actor type, one column per step of the last 15 minutes: the deepest mailbox of that type on that node in the step, from stored history. A row that becomes warm and stays warm shows a type that is behind its load. At most 128 rows, the series limit of the history.
      </Part>
      <Part title="Handler time by message">
        For one actor type, the sampled time inside on_message, per message label, added over the nodes since the start of each node. It is not a stack profile. It shows which messages use the handler time, not where the handler uses that time. The system times one message in timing_sample_every. A type without timing shows nothing.
      </Part>
    </HelpDialog>
  );
}

function Backlog() {
  const history = useHistory({ metrics: ["actor.mailbox_depth_max"], span: SPAN, points: POINTS, maxSeries: 128, aggregation: "max" });
  const result = history.data?.byMetric["actor.mailbox_depth_max"];
  const grid = useMemo(() => {
    const rows = (result?.series ?? [])
      .map((s) => ({ name: `${s.node_id} · ${shortType(s.labels.actor_type ?? "")}`, points: line(result, s, false).filter((p): p is [number, number] => p[1] !== null) }))
      .sort((a, b) => natural(a.name, b.name));
    const to = history.data?.to ?? 0;
    return rowHeat(rows, to - SPAN, to, POINTS);
  }, [result, history.data?.to]);
  const footer = [history.error ?? history.data?.errors.join(" · "), result?.truncated && "Only the first 128 rows", "Deepest mailbox per step · stored history"].filter(Boolean).join(" · ");
  return (
    <Panel label="Backlog" title="Backlog by node and actor type" footer={footer} className="shrink-0">
      {/* One height whatever the rows, so the page does not move when history comes: more rows scroll. */}
      <div className="scroll-y h-[320px]">
        <div style={{ height: (grid.rows?.length ?? 0) * 16 + 90 }} className="flex min-h-full flex-col">
          {!history.available ? (
            <Empty title="No stored history" detail="The backlog comes from the metrics store. Start a MetricsService." />
          ) : (
            <HeatPlot grid={grid} label="Mailbox depth heatmap, node and actor type by time" empty={{ title: "No backlog samples yet", detail: "Rows show when the collector stores two samples." }} />
          )}
        </div>
      </div>
    </Panel>
  );
}

function HandlerTime() {
  const nodes = useLive((s) => s.overview?.nodes);
  /** Busiest type first, so the page opens on the one that costs the most. */
  const byType = useMemo(() => handlerTime(nodes ?? []), [nodes]);
  const types = [...byType.keys()];
  const [picked, setPicked] = useUrlState("type", "");
  const type = byType.has(picked) ? picked : (types[0] ?? "");
  const parts = byType.get(type) ?? [];
  const total = parts.reduce((sum, p) => sum + p.ns, 0);
  const color = (i: number) => `hsl(${(200 + i * 67) % 360} 50% 55%)`;
  return (
    <Panel
      label="Handler time by message"
      title="Handler time by message"
      controls={types.length > 0 && <Select label="Actor type" value={type} onChange={setPicked} options={types.map((t) => ({ value: t, label: shortType(t) }))} />}
      footer="Sampled on_message time · all nodes · since each node started"
      className="min-h-[240px] flex-1"
    >
      {!total ? (
        <Empty title="No handler timings" detail="An actor type shows here when the system times its messages (timing_sample_every) and the messages have labels." />
      ) : (
        <div className="scroll-y flex-1 px-3 py-2.5">
          <div role="img" aria-label={`Handler time of ${shortType(type)} by message label`} className="flex h-5 w-full overflow-hidden rounded-sm">
            {parts.map((p, i) => (
              <div key={p.label} title={`${p.label}: ${duration(p.ns)}`} style={{ width: `${(p.ns / total) * 100}%`, background: color(i) }} />
            ))}
          </div>
          <table className="mt-2.5 w-full text-[10px]">
            <thead className="text-left text-muted">
              <tr><th className="py-1 font-normal">Message</th><th className="py-1 text-right font-normal">Handler time</th><th className="py-1 text-right font-normal">Share</th></tr>
            </thead>
            <tbody className="mono">
              {parts.map((p, i) => (
                <tr key={p.label}>
                  <td className="py-[3px]"><span className="mr-1.5 inline-block size-2 rounded-[2px] align-middle" style={{ background: color(i) }} />{p.label}</td>
                  <td className="py-[3px] text-right">{duration(p.ns)}</td>
                  <td className="py-[3px] text-right">{((p.ns / total) * 100).toFixed(1)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}

export function WorkloadPage() {
  return (
    <>
      <h1 className="sr-only-h1">Workload</h1>
      <div className="flex min-h-0 flex-1 flex-col gap-2.5 overflow-y-auto">
        <Backlog />
        <HandlerTime />
      </div>
    </>
  );
}
