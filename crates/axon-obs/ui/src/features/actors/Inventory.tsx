import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { Fragment, useEffect, useMemo, useState } from "react";
import { Link, useNavigate } from "react-router";
import type { Actor, InventoryActor, InventoryPage } from "../../api";
import { actorKey, getJson } from "../../api";
import { Badge, Dot, Empty, Since } from "../../components/atoms";
import { ExportButton } from "../../components/Export";
import { Icon } from "../../components/Icon";
import { Panel } from "../../components/Panel";
import { rowLink, rowNav } from "../../components/rowLink";
import { Search } from "../../components/Search";
import { Select } from "../../components/Select";
import { Head, SortHead } from "../../components/sort";
import { Sparkline } from "../../components/Sparkline";
import { ToggleGroup } from "../../components/ToggleGroup";
import { phaseTone, useUrlState } from "../../hooks";
import { bytes, count, duration, perSecond, shortType, sum } from "../../model/format";
import { ActorTrends } from "../../model/live";
import { latest, ratePoints, type P } from "../../model/series";
import { useLive } from "../../store";
import { NodeName } from "../../components/Flag";

const PAGE = 25;
const trends = new ActorTrends();
const PHASES = [
  { value: "", label: "All states", icon: "all" },
  { value: "running", label: "Running", icon: "running" },
  { value: "draining", label: "Draining", icon: "draining" },
  { value: "stopping", label: "Stopping", icon: "failed" },
  { value: "stopped", label: "Stopped", icon: "stopped" },
];

export const actorPath = (a: InventoryActor) => `/actors/${encodeURIComponent(a.node_id)}/${actorKey(a.incarnation)}?incarnation=${a.node_incarnation}`;
const rejected = (a: InventoryActor) => sum(a.diagnostics.rejected);

export const lettersPath = (node: string, actor: Actor) => `/dead-letters?q=${encodeURIComponent(`${node} ${actor.incarnation.runtime}:${actor.incarnation.slot}:`)}`;

export function Inventory({ scope, flat }: { scope?: string; flat?: boolean }) {
  const navigate = useNavigate();
  const nodes = useLive((s) => s.overview?.nodes);
  const [q, setQ] = useUrlState("aq", "");
  const [typed, setTyped] = useState(q);
  const [node, setNode] = useUrlState("anode", "");
  const [actorType, setType] = useUrlState("atype", "");
  const [phase, setPhase] = useUrlState("aphase", "");
  const [grouped, setGrouped] = useUrlState<"on" | "off">("agroup", "on");
  const [sort, setSort] = useUrlState("asort", "node_id");
  const [order, setOrder] = useUrlState<"asc" | "desc">("aorder", "asc");
  const [offsetText, setOffset] = useUrlState("aoff", "0");
  const offset = Number(offsetText) || 0;

  useEffect(() => {
    const timer = setTimeout(() => {
      if (typed !== q) {
        setQ(typed);
        setOffset(null);
      }
    }, 250);
    return () => clearTimeout(timer);
  }, [typed, q, setQ, setOffset]);

  const identities = useMemo(() => (nodes ?? []).map((n) => `${n.node.id}@${n.node.incarnation}`).join(","), [nodes]);
  const types = useMemo(() => [...new Set((nodes ?? []).flatMap((n) => n.snapshot?.actor_types.map((t) => t.actor_type) ?? []))].sort(), [nodes]);
  const params = { node_id: scope ?? node, q, actor_type: actorType, phase, sort, order, offset, limit: PAGE };
  const page = useQuery({
    queryKey: ["inventory", params, identities],
    refetchInterval: 5000,
    placeholderData: keepPreviousData,
    queryFn: async ({ signal }) => {
      const result = await getJson<InventoryPage>("inventory", params, signal);
      // The server answers from a short cache; a rate over two answers of one gathering is no rate.
      trends.append(result.actors, result.timestamp_ms);
      return result;
    },
  });

  const change = <T,>(set: (value: T) => void) => (value: T) => {
    set(value);
    setOffset(null);
  };
  const sorter = {
    field: sort,
    order,
    toggle: (field: string) => {
      if (field === sort) setOrder(order === "asc" ? "desc" : "asc");
      else {
        setSort(field);
        setOrder(field === "node_id" || field === "name" ? "asc" : "desc");
      }
      if (field !== "node_id") setGrouped("off");
      setOffset(null);
    },
  };
  const data = page.data;
  const rows = data?.actors ?? [];
  const showGroups = grouped === "on" && sort === "node_id" && !scope;
  const errors = Object.entries(data?.errors ?? {});
  const step = "h-6 rounded-md border border-border px-2 text-[11px]";

  return (
    <Panel
      flat={flat}
      label="Actor inventory"
      className="flex-1"
      footer={
        <>
          <span>{count(data?.total)} actors {scope ? `on ${scope}` : "across the cluster"}</span>
          <span className="mono ml-auto">{rows.length ? `${offset + 1}–${offset + rows.length} of ${data!.total}` : "0 of 0"}</span>
          <button type="button" aria-label="Previous page" disabled={offset === 0 || page.isFetching} onClick={() => setOffset(String(Math.max(0, offset - PAGE)))} className={step}>‹</button>
          <button type="button" aria-label="Next page" disabled={!data || offset + PAGE >= data.total || page.isFetching} onClick={() => setOffset(String(offset + PAGE))} className={step}>›</button>
        </>
      }
    >
      <div className="flex shrink-0 flex-wrap items-center gap-1.5 border-b border-border p-2">
        <Search label="Search actors" value={typed} onChange={setTyped} className="w-[200px]" />
        {!scope && <Select label="Node" value={node} onChange={change(setNode)} options={[{ value: "", label: "All nodes" }, ...(nodes ?? []).map((n) => ({ value: n.node.id, label: n.node.id }))]} />}
        <Select label="Actor type" value={actorType} onChange={change(setType)} options={[{ value: "", label: "All types" }, ...types.map((t) => ({ value: t, label: shortType(t) }))]} />
        <ToggleGroup label="State" caption="State" value={phase} onChange={change(setPhase)} options={PHASES} />
        {!scope && (
          <button type="button" aria-pressed={showGroups} onClick={() => (showGroups ? setGrouped("off") : (setGrouped("on"), setSort("node_id"), setOrder("asc")))} className={`h-7 rounded-md border border-border px-2 text-[10px] ${showGroups ? "bg-raised text-text" : "text-muted"}`}>
            Group by node
          </button>
        )}
        <span className="ml-auto" />
        <ExportButton
          dataset={{
            name: "Actor inventory",
            count: data?.total,
            capped: data?.truncated,
            filters: [q && ["search", q], (scope ?? node) && ["node", scope ?? node], actorType && ["type", actorType], phase && ["state", phase], sort !== "node_id" && ["sort", `${sort} ${order}`]],
            rows: async () => {
              const rows: InventoryActor[] = [];
              for (let offset = 0; ; offset += 1000) {
                const p = await getJson<InventoryPage>("inventory", { ...params, offset, limit: 1000 });
                rows.push(...p.actors);
                if (!p.actors.length || offset + 1000 >= p.total) return rows;
              }
            },
          }}
        />
        <button type="button" aria-label="Refresh inventory" title="Refresh inventory" onClick={() => page.refetch()} className="flex size-[25px] items-center justify-center rounded-sm text-muted hover:bg-raised hover:text-text">
          <Icon name="refresh" size={14} />
        </button>
      </div>
      {page.error && <p className="shrink-0 border-b border-border px-3 py-1.5 text-[10px] text-warning">Actor inventory unavailable: {page.error.message} · the table shows the previous data</p>}
      <p className={`shrink-0 border-b border-border px-3 py-1.5 text-[10px] ${data?.truncated ? "text-warning" : "text-muted"}`}>
        {data?.truncated ? `Partial inventory: the scan stopped at its limit, or some nodes did not answer.${errors.slice(0, 3).map(([id, message]) => ` ${id}: ${message}`).join(" ·")}` : data ? "Complete inventory" : "Inventory not complete yet"}
      </p>
      {!rows.length ? (
        <Empty title={page.isLoading ? "Wait for the actor inventory" : q || actorType || phase || node ? "No actors match the filters" : "No actors reported"} />
      ) : (
        <div className="scroll-y flex-1 overflow-x-auto">
          <table className="min-w-[1100px] [&_td]:h-[42px]">
            <thead>
              <tr>
                {!scope && <SortHead sort={sorter} field="node_id" label="Node" width="10%" />}
                <SortHead sort={sorter} field="name" label="Actor / type" width="15%" />
                <SortHead sort={sorter} field="phase" label="State" width="8%" />
                <SortHead sort={sorter} field="up" label="Up" width="7%" align="right" />
                <SortHead sort={sorter} field="since_restart" label="Since restart" width="9%" align="right" />
                <SortHead sort={sorter} field="mailbox_depth" label="Mailbox" width="10%" align="right" />
                <SortHead sort={sorter} field="mailbox_bytes" label="Bytes" width="7%" align="right" />
                <SortHead sort={sorter} field="processed" label="Processed" width="10%" align="right" />
                <SortHead sort={sorter} field="rejected" label="Rejected" width="7%" align="right" />
                <SortHead sort={sorter} field="oldest_message_age_ns" label="Oldest" width="7%" align="right" />
                <SortHead sort={sorter} field="jobs" label="Jobs" width="5%" align="right" />
                <Head label="Timers" width="5%" align="right" />
              </tr>
            </thead>
            <tbody {...rowNav}>
              {rows.map((actor, i) => {
                const points = trends.points(actor);
                const depth: P[] = points.map((p) => [p.t, p.depth]);
                const rate = ratePoints(points.map((p) => ({ t: p.t, v: p.processed })), ActorTrends.GAP_MS);
                const d = actor.diagnostics;
                const letters = d.dead_letters;
                return (
                  <Fragment key={`${actor.node_id}|${actorKey(actor.incarnation)}`}>
                    {showGroups && actor.node_id !== rows[i - 1]?.node_id && (
                      <tr className="bg-header hover:bg-header">
                        <td colSpan={12} className="!h-[29px] font-sans">
                          <NodeName id={actor.node_id} className="mono hover:underline" />
                          <span className="ml-2 text-[10px] text-muted">{rows.filter((r) => r.node_id === actor.node_id).length} on this page</span>
                        </td>
                      </tr>
                    )}
                    <tr {...rowLink(navigate, actorPath(actor))}>
                      {!scope && <td><NodeName id={actor.node_id} tabIndex={-1} className="hover:underline" /></td>}
                      <td>
                        <span className="flex min-w-0 items-center">
                          <Link to={actorPath(actor)} tabIndex={-1} className="truncate hover:underline">{actor.name ?? actorKey(actor.incarnation)}</Link>
                          {letters > 0 && <Link to={lettersPath(actor.node_id, actor)} onClick={(e) => e.stopPropagation()} aria-label={`${letters} dead letters`} className="-my-1 ml-0.5 p-1 leading-none"><Dot tone="warning" title={`${count(letters)} of its messages became dead letters`} /></Link>}
                        </span>
                        <small className="block truncate font-sans text-[10px] text-muted">{shortType(actor.actor_type)}</small>
                      </td>
                      <td><Badge tone={phaseTone(actor.phase)}>{actor.phase}</Badge></td>
                      <td className="text-right"><Since us={actor.started_us} label="Started" /></td>
                      <td className="text-right">{actor.restarted_us != null && <Since us={actor.restarted_us} label="Restarted" />}</td>
                      <td className="text-right"><span className="inline-flex w-full items-center justify-end gap-2"><span className="w-14"><Sparkline points={depth} label="Mailbox depth" height={16} /></span><span className="min-w-[6ch]">{count(d.mailbox_depth)}</span></span></td>
                      <td className="text-right">{bytes(d.mailbox_bytes)}</td>
                      <td className="text-right"><span className="inline-flex w-full items-center justify-end gap-2"><span className="w-14"><Sparkline points={rate} label="Processed per second" height={16} /></span><span className="min-w-[7ch]">{count(d.processed)}<small className="block text-[10px] text-muted">{perSecond(latest(rate))}</small></span></span></td>
                      <td className={`text-right ${rejected(actor) ? "text-warning" : ""}`}>{count(rejected(actor))}</td>
                      <td className="text-right">{duration(d.oldest_message_age_ns)}</td>
                      <td className="text-right">{count(d.jobs)}</td>
                      <td className="text-right">{count(d.timers)}</td>
                    </tr>
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}
