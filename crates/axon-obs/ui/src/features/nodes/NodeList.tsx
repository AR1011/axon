import { useMemo } from "react";
import { Count, Dot, Empty } from "../../components/atoms";
import { Flag } from "../../components/Flag";
import { Panel } from "../../components/Panel";
import { Search } from "../../components/Search";
import { Select } from "../../components/Select";
import { nodeStatus, useNow, useUrlState } from "../../hooks";
import { bytes, percent } from "../../model/format";
import { useLive } from "../../store";
import { placeOf, roleOf, useSelectedNode } from "../select";

export function useNodeFilter() {
  const members = useLive((s) => s.overview?.nodes);
  const departed = useLive((s) => s.departed);
  const nodes = useMemo(() => (members ? [...members, ...departed] : undefined), [members, departed]);
  const [q, setQ] = useUrlState("q", "");
  const [role, setRole] = useUrlState("role", "");
  const roles = useMemo(() => [...new Set((nodes ?? []).map(roleOf).filter(Boolean))].sort(), [nodes]);
  const filtered = useMemo(() => {
    const needle = q.trim().toLowerCase();
    return (nodes ?? []).filter(
      (n) => (!role || roleOf(n) === role) && (!needle || [n.node.id, n.node.address, ...Object.values(n.node.metadata)].some((text) => text.toLowerCase().includes(needle))),
    );
  }, [nodes, q, role]);
  const controls = (
    <>
      <Search label="Search nodes" value={q} onChange={setQ} className="min-w-0 flex-1" />
      <Select label="Role" value={role} onChange={setRole} options={[{ value: "", label: "All roles" }, ...roles.map((r) => ({ value: r, label: r }))]} />
    </>
  );
  // Departed nodes stay listed, but the count is the membership's, as on the tiles and the sidebar.
  const tally = (shown: number) => `${shown < (nodes?.length ?? 0) ? `${shown} shown · ` : ""}${members?.length ?? 0} members${departed.length ? ` · ${departed.length} left` : ""}`;
  return { all: nodes ?? [], members: members?.length ?? 0, filtered, controls, q, role, tally };
}

export function NodeList() {
  const now = useNow();
  const { all, members, filtered, controls, tally } = useNodeFilter();
  const [selected, select] = useSelectedNode();
  return (
    <Panel title={<>Nodes <Count>{members}</Count></>} footer={tally(filtered.length)}>
      <div className="flex shrink-0 gap-1.5 border-b border-border p-2">{controls}</div>
      {!all.length ? (
        <Empty title="No nodes discovered yet" detail="No membership snapshot yet." />
      ) : !filtered.length ? (
        <Empty title="No nodes match the filters" detail="Use a different name or role." />
      ) : (
        <ul className="scroll-y flex-1 p-1">
          {filtered.slice(0, 300).map((view) => {
            const status = nodeStatus(view, now);
            return (
              <li key={view.node.id}>
                <button
                  type="button"
                  aria-pressed={selected?.node.id === view.node.id}
                  onClick={() => select(view.node.id)}
                  className={`flex min-h-12 w-full items-center gap-2 rounded-sm px-2 text-left ${view.leftAt ? "opacity-55 " : ""}${selected?.node.id === view.node.id ? "bg-accent-soft" : "hover:bg-raised"}`}
                >
                  <Dot tone={status.tone} title={status.label} />
                  <span className="min-w-0 flex-1">
                    <span className="mono flex items-center gap-1.5 text-[11px]"><Flag node={view.node.id} /><span className="truncate">{view.node.id}</span></span>
                    <span className="block truncate text-[10px] text-muted">{view.leftAt ? status.label : [roleOf(view) || "No role", placeOf(view).region].filter(Boolean).join(" · ")}</span>
                  </span>
                  <span className="mono shrink-0 text-right text-[10px]">
                    <span className="block">{percent(view.snapshot?.process?.cpu_percent)}</span>
                    <span className="block text-[10px] text-muted">{bytes(view.snapshot?.process?.rss_bytes)}</span>
                  </span>
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </Panel>
  );
}
