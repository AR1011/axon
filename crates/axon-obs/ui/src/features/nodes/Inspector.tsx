import { Link } from "react-router";
import type { NodeView } from "../../api";
import { Badge, Empty, Field } from "../../components/atoms";
import { Flag } from "../../components/Flag";
import { Identity } from "../../components/Identity";
import { nodeStatus, useNow, useUrlState } from "../../hooks";
import { ago, bytes, count, percent, uptime } from "../../model/format";
import { nodePath, placeOf, roleOf } from "../select";

export function NodeSummary({ view }: { view: NodeView }) {
  const now = useNow();
  const status = nodeStatus(view, now);
  const s = view.snapshot;
  const reported = (block: unknown, value: string) => (block ? value : "Not reported");
  const cpu = (n?: number | null) => (n == null ? "Not measured yet" : percent(n));
  return (
    <div className="px-3 py-2">
      <p className="text-[10px] text-muted">Selected node</p>
      <div className="mb-2 flex items-center justify-between gap-2">
        <h3 className="mono flex min-w-0 items-center gap-1.5 text-[13px] font-[550]"><Flag node={view.node.id} /><span className="truncate">{view.node.id}</span></h3>
        <Badge tone={status.tone}>{status.label}</Badge>
      </div>
      <dl>
        <Field label="Role">{roleOf(view) || "—"}</Field>
        <Field label="Location">{placeOf(view).text || "—"}</Field>
        <Field label="Actors">{count(s?.actor_total)}</Field>
        <Field label="Address">{view.node.address}</Field>
        <Field label="Incarnation"><Identity value={view.node.incarnation} label="Process incarnation" /></Field>
        <Field label="Uptime">{uptime(s?.uptime_ms)}</Field>
        <Field label="Last observed">{s ? ago(now - s.timestamp_ms) : "Never"}</Field>
        <Field label="Process CPU">{reported(s?.process, cpu(s?.process?.cpu_percent))}</Field>
        <Field label="Process RSS">{reported(s?.process, bytes(s?.process?.rss_bytes))}{s?.process && s.host?.total_memory_bytes ? <span className="text-muted"> · {percent((s.process.rss_bytes / s.host.total_memory_bytes) * 100)} of host</span> : null}</Field>
        <Field label="Host CPU">{reported(s?.host, cpu(s?.host?.cpu_percent))}</Field>
        <Field label="Host memory available">{reported(s?.host?.available_memory_bytes, bytes(s?.host?.available_memory_bytes))}</Field>
        <Field label="Dead letters">{count(s?.system.dead_letters)}</Field>
      </dl>
      {view.error && <p className="mt-2 text-[10px] leading-[1.6] text-warning">{view.error}{view.leftAt ? " · Departure reason unknown if the node does not report it below." : ""}</p>}
      {!!s?.notices.length && <p className="mt-2 text-[10px] leading-[1.6] text-warning">Collection gaps: {s.notices.slice(0, 8).map((n) => n.message).join(" · ")}</p>}
      {s?.lifecycle.reason && <p className="mt-2 text-[10px] leading-[1.6] text-muted">Reported lifecycle reason: {s.lifecycle.reason}</p>}
    </div>
  );
}

export function Inspector({ view, children }: { view?: NodeView; children?: React.ReactNode }) {
  const [id] = useUrlState("node", "");
  return (
    <aside aria-label="Node inspector" className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-md border border-border bg-panel max-[900px]:min-h-[420px] max-[900px]:shrink-0">
      <header className="flex min-h-[37px] shrink-0 items-center justify-between border-b border-border px-3">
        <h2>Inspector</h2>
        {view && <Link to={nodePath(view.node.id)} className="text-[10px] text-muted hover:text-text">Open node ↗</Link>}
      </header>
      {view ? (
        <div className="scroll-y flex-1">
          <NodeSummary view={view} />
          <div className="border-t border-border">{children}</div>
        </div>
      ) : (
        <Empty title={id ? `${id} is not in this cluster` : "Select a node"} detail="Select a node in the list or the graph." />
      )}
    </aside>
  );
}
