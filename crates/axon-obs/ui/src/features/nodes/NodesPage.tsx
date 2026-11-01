import { useNavigate } from "react-router";
import type { NodeView } from "../../api";
import { Badge, Count, Empty } from "../../components/atoms";
import { ExportButton } from "../../components/Export";
import { NodeName } from "../../components/Flag";
import { Panel } from "../../components/Panel";
import { rowLink, rowNav } from "../../components/rowLink";
import { SortHead, useSort, type Accessors } from "../../components/sort";
import { nodeStatus, useNow } from "../../hooks";
import { bytes, count, percent, uptime } from "../../model/format";
import { readiness, wireRange } from "../../model/rollout";
import { nodePath, roleOf } from "../select";
import { useNodeFilter } from "./NodeList";

const FIELDS: Accessors<NodeView> = {
  name: (n) => n.node.id,
  role: roleOf,
  status: (n) => n.snapshot?.lifecycle.phase,
  ready: (n) => readiness(n)?.join(", "),
  wire: (n) => wireRange(n.snapshot),
  actors: (n) => n.snapshot?.actor_total,
  cpu: (n) => n.snapshot?.process?.cpu_percent,
  rss: (n) => n.snapshot?.process?.rss_bytes,
  address: (n) => n.node.address,
  uptime: (n) => n.snapshot?.uptime_ms,
};

export function NodesPage() {
  const now = useNow();
  const navigate = useNavigate();
  const { all, filtered, controls, q, role, tally } = useNodeFilter();
  const sort = useSort(filtered, FIELDS, "", "name");
  const rows = sort.sorted.slice(0, 300);
  const checked = all.map(readiness).filter((r) => r != null);
  const unready = checked.filter((r) => r.length).length;
  return (
    <>
      <h1 className="sr-only-h1">Nodes</h1>
      <Panel
        className="flex-1"
        title={<>Nodes {!!checked.length && (unready ? <Badge tone="warning">{unready} not ready</Badge> : <Count>{checked.length} ready</Count>)}</>}
        controls={<div className="flex w-[390px] max-w-[60vw] items-center gap-1.5">{controls}<ExportButton dataset={{ name: "Nodes", count: filtered.length, filters: [q && ["search", q], role && ["role", role]], rows: () => sort.sorted.map((view) => { const { node, reachable, error, leftAt, snapshot: s } = view; return { ...node, reachable, error, left_at: leftAt, phase: s?.lifecycle.phase, not_ready: readiness(view), wire: wireRange(s), uptime_ms: s?.uptime_ms, actors: s?.actor_total, process: s?.process, host: s?.host, system: s?.system, cluster: s && { members: s.cluster.members, unreachable: s.cluster.unreachable, isolated: s.cluster.isolated, discovery: s.cluster.discovery.kind, ownership: s.cluster.ownership?.kind } }; }) }} /></div>}
        footer={<><span>{tally(rows.length)}</span><span className="ml-auto">{filtered.length > 300 ? "List capped" : "Live inventory"}</span></>}
      >
        {!all.length ? (
          <Empty title="No nodes discovered yet" detail="No membership snapshot yet." />
        ) : !rows.length ? (
          <Empty title="No nodes match the filters" detail="Use a different name or role." />
        ) : (
          <div className="scroll-y flex-1 overflow-x-auto">
            <table className="min-w-[960px]">
              <thead>
                <tr>
                  <SortHead sort={sort} field="name" label="Name" width="20%" />
                  <SortHead sort={sort} field="role" label="Role" width="12%" />
                  <SortHead sort={sort} field="status" label="Status" width="11%" />
                  <SortHead sort={sort} field="ready" label="Ready" width="10%" />
                  <SortHead sort={sort} field="actors" label="Actors" width="7%" align="right" />
                  <SortHead sort={sort} field="cpu" label="CPU" width="7%" align="right" />
                  <SortHead sort={sort} field="rss" label="RSS" width="8%" align="right" />
                  <SortHead sort={sort} field="address" label="Address" width="14%" />
                  <SortHead sort={sort} field="wire" label="Wire" width="6%" align="right" />
                  <SortHead sort={sort} field="uptime" label="Uptime" width="9%" align="right" />
                </tr>
              </thead>
              <tbody {...rowNav}>
                {rows.map((view) => {
                  const status = nodeStatus(view, now);
                  const ready = readiness(view);
                  return (
                    <tr key={view.node.id} {...rowLink(navigate, nodePath(view.node.id))} className={view.leftAt ? "opacity-55" : ""}>
                      <td><NodeName id={view.node.id} tabIndex={-1} className="hover:underline" /></td>
                      <td className="font-sans">{roleOf(view) || "—"}</td>
                      <td><Badge tone={status.tone}>{status.label}</Badge></td>
                      <td title={ready?.length ? `/readyz answers 503 for: ${ready.join(", ")}` : undefined}>{!ready ? "—" : ready.length ? <Badge tone="warning">{ready.join(" · ")}</Badge> : <Badge tone="success">ready</Badge>}</td>
                      <td className="text-right">{count(view.snapshot?.actor_total)}</td>
                      <td className="text-right">{percent(view.snapshot?.process?.cpu_percent)}</td>
                      <td className="text-right">{bytes(view.snapshot?.process?.rss_bytes)}</td>
                      <td>{view.node.address}</td>
                      <td className="text-right">{wireRange(view.snapshot) || "—"}</td>
                      <td className="text-right">{uptime(view.snapshot?.uptime_ms)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
    </>
  );
}
