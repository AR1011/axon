import { useMemo } from "react";
import { Link, useNavigate } from "react-router";
import { Count, Empty } from "../../components/atoms";
import { ExportButton } from "../../components/Export";
import { Flag, NodeName } from "../../components/Flag";
import { Identity } from "../../components/Identity";
import { Panel } from "../../components/Panel";
import { rowNav } from "../../components/rowLink";
import { Search } from "../../components/Search";
import { Select } from "../../components/Select";
import { SortHead, useSort, type Accessors } from "../../components/sort";
import { useNow, useUrlState } from "../../hooks";
import { HANDSHAKE_FAILURES, LANES } from "../../api";
import { isDown, LANE_NAMES, LANE_TIP } from "../../model/live";
import { bytes, bytesPerSecond, count, duration, reasons, sum, uptime } from "../../model/format";
import { connections, useLive } from "../../store";
import { connectionPath, useSelectedConnection, type Connection } from "../select";
import { DownBadge } from "./TransportDiagnostics";

interface Row extends Connection {
  tx: number | null;
  rx: number | null;
}

const FIELDS: Accessors<Row> = {
  source: (r) => `${r.view.node.id} ${r.peer.node} ${r.peer.outgoing ? "out" : "in"} ${LANES.indexOf(r.peer.lane)}`,
  direction: (r) => (r.peer.outgoing ? "dialled" : "accepted"),
  peer: (r) => r.peer.node,
  lane: (r) => LANES.indexOf(r.peer.lane),
  session: (r) => r.peer.incarnation,
  wire: (r) => r.peer.version,
  sent: (r) => r.peer.session.tx_wire_bytes,
  received: (r) => r.peer.session.rx_wire_bytes,
  tx: (r) => r.tx,
  rx: (r) => r.rx,
  queue: (r) => r.peer.session.outbound_reserved_bytes,
  rtt: (r) => r.peer.session.rtt_ns || null,
};

/** Sessions that moved at least this many bytes, both ways together, since they opened. */
const TRAFFIC = [
  { value: "", label: "Any traffic" },
  { value: String(2 ** 20), label: "≥ 1 MiB" },
  { value: String(100 * 2 ** 20), label: "≥ 100 MiB" },
  { value: String(2 ** 30), label: "≥ 1 GiB" },
];

export function PeerTable({ scope, flat, limit = 200 }: { scope?: string; flat?: boolean; limit?: number }) {
  const navigate = useNavigate();
  const now = useNow();
  const version = useLive((s) => s.version);
  const [selected, select, all] = useSelectedConnection(scope);
  const rows = useMemo<Row[]>(
    () => all.map((c) => ({ ...c, tx: connections.latest(c.key, now)?.tx ?? null, rx: connections.latest(c.key, now)?.rx ?? null })),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- `version` stands in for the mutable live buffer
    [all, now, version],
  );
  const [q, setQ] = useUrlState("cq", "");
  const [node, setNode] = useUrlState("cnode", "");
  const [least, setLeast] = useUrlState("cmin", "");
  const matching = useMemo(() => {
    const needle = q.trim().toLowerCase();
    const min = Number(least) || 0;
    return rows.filter(
      (r) =>
        (!node || r.view.node.id === node || r.peer.node === node) &&
        (!needle || `${r.view.node.id} ${r.peer.node}`.toLowerCase().includes(needle)) &&
        r.peer.session.tx_wire_bytes + r.peer.session.rx_wire_bytes >= min,
    );
  }, [rows, q, node, least]);
  const names = useMemo(() => [...new Set(all.map((c) => c.view.node.id))], [all]);
  const sort = useSort(matching, FIELDS, "c", "source");
  const shown = sort.sorted.slice(0, limit);
  const overview = useLive((s) => s.overview);
  const listener = useMemo(() => {
    const l = { accepted: 0, accept_errors: 0, rejected_busy: 0, rejected_per_ip: 0, handshake: HANDSHAKE_FAILURES.map(() => 0) };
    for (const n of overview?.nodes ?? []) {
      const x = n.snapshot?.cluster.engine.listener;
      if (!x || (scope && n.node.id !== scope)) continue;
      l.accepted += x.accepted; l.accept_errors += x.accept_errors; l.rejected_busy += x.rejected_busy; l.rejected_per_ip += x.rejected_per_ip; x.handshake_failures.forEach((n, i) => (l.handshake[i] += n));
    }
    return l;
  }, [overview, scope]);
  const refused = listener.accept_errors + listener.rejected_busy + listener.rejected_per_ip + sum(listener.handshake);
  return (
    <Panel
      flat={flat}
      className="flex-1"
      title={flat ? undefined : <>Peer sessions <Count>{matching.length}</Count></>}
      controls={
        <>
          <Search label="Search sessions" value={q} onChange={setQ} className="w-[170px] max-[700px]:w-[110px]" />
          <Select label="Session traffic" value={least} onChange={setLeast} options={TRAFFIC} />
          {!scope && <Select label="Session node" value={node} onChange={setNode} options={[{ value: "", label: "All nodes" }, ...names.map((n) => ({ value: n, label: n }))]} />}
          <ExportButton dataset={{ name: "Connections", count: matching.length, filters: [q && ["search", q], (scope ?? node) && ["node", scope ?? node], least && ["traffic", TRAFFIC.find((t) => t.value === least)?.label ?? least]], rows: () => sort.sorted.map((r) => ({ ...r.peer, from: r.view.node.id, tx_rate: r.tx, rx_rate: r.rx })) }} />
        </>
      }
      footer={
        <>
          <span>{shown.length} of {all.length} sessions · Enter or ↗ opens one{all.length > limit ? ` · limited to ${limit}` : ""}</span>
          {!flat && (
            <span className={`ml-auto ${refused ? "text-warning" : ""}`} title="Inbound listener, the sum of the nodes shown">
              Listener · accepted {count(listener.accepted)} · errors {count(listener.accept_errors)} · busy {count(listener.rejected_busy)} · per-IP {count(listener.rejected_per_ip)} · handshake failures {reasons(HANDSHAKE_FAILURES, listener.handshake) || "0"}
            </span>
          )}
        </>
      }
    >
      {!shown.length ? (
        <Empty title={all.length ? "No sessions match the filters" : "No peer sessions"} detail={all.length ? "Use a different name, node or traffic." : "Sessions show when nodes send traffic to each other."} />
      ) : (
        <div className="scroll-y flex-1 overflow-x-auto">
          {/* Numbers get the width their longest value needs; the names share what is left, and the endpoint stays in view when it scrolls. */}
          <table className="sticky-first min-w-[1232px]">
            <thead>
              <tr>
                <SortHead sort={sort} field="source" label="Endpoint" width="150px" />
                <SortHead sort={sort} field="direction" label="Direction" width="100px" />
                <SortHead sort={sort} field="peer" label="Peer" />
                <SortHead sort={sort} field="lane" label="Lane" title={LANE_TIP} width="72px" />
                <SortHead sort={sort} field="session" label="Peer incarnation" width="136px" />
                <SortHead sort={sort} field="wire" label="Wire" width="60px" align="right" />
                <SortHead sort={sort} field="sent" label="Sent" width="88px" align="right" />
                <SortHead sort={sort} field="received" label="Received" width="92px" align="right" />
                <SortHead sort={sort} field="tx" label="TX/s" width="96px" align="right" />
                <SortHead sort={sort} field="rx" label="RX/s" width="96px" align="right" />
                <SortHead sort={sort} field="queue" label="Queue" width="84px" align="right" />
                <SortHead sort={sort} field="rtt" label="RTT" width="88px" align="right" />
              </tr>
            </thead>
            <tbody {...rowNav}>
              {shown.map((row) => (
                <tr key={row.ref} aria-selected={selected?.ref === row.ref} onClick={() => select(row.ref)} onDoubleClick={() => navigate(connectionPath(row))} tabIndex={-1} onKeyDown={(e) => e.target === e.currentTarget && (e.key === "Enter" ? navigate(connectionPath(row)) : e.key === " " && (e.preventDefault(), select(row.ref)))} className="group cursor-pointer">
                  <td><NodeName id={row.view.node.id} link={false} /></td>
                  <td className="font-sans text-muted"><span className="mono mr-1.5 text-text">{row.peer.outgoing ? "→" : "←"}</span>{row.peer.outgoing ? "dialled" : "accepted"}</td>
                  <td title={row.peer.node}>
                    <span className="flex items-center gap-2">
                      <Flag node={row.peer.node} />
                      <span className="truncate">{row.peer.node}</span>
                      <Link to={connectionPath(row)} tabIndex={-1} aria-label={`Open ${row.view.node.id} to ${row.peer.node}, ${LANE_NAMES[row.peer.lane]}`} title="Open connection" onClick={(e) => e.stopPropagation()} className="ml-auto text-muted opacity-0 group-hover:opacity-100 group-focus:opacity-100 hover:text-text [tr[aria-selected=true]_&]:opacity-100">↗</Link>
                    </span>
                  </td>
                  <td className="font-sans">{LANE_NAMES[row.peer.lane]}</td>
                  <td><Identity value={row.peer.incarnation} label="Peer incarnation" /></td>
                  <td className="text-right" title="The wire version that the session uses">{row.peer.version}</td>
                  {isDown(row.peer) ? (
                    <td colSpan={6}><DownBadge peer={row.peer} /></td>
                  ) : (
                    <>
                      <td className="text-right" title={`Sent since the session opened ${uptime(row.peer.session.age_ns / 1e6)} ago`}>{bytes(row.peer.session.tx_wire_bytes)}</td>
                      <td className="text-right" title={`Received since the session opened ${uptime(row.peer.session.age_ns / 1e6)} ago`}>{bytes(row.peer.session.rx_wire_bytes)}</td>
                      <td className="text-right">{bytesPerSecond(row.tx)}</td>
                      <td className="text-right">{bytesPerSecond(row.rx)}</td>
                      <td className="text-right">{bytes(row.peer.session.outbound_reserved_bytes)}</td>
                      <td className="text-right">{row.peer.session.rtt_ns ? duration(row.peer.session.rtt_ns) : <span className="font-sans text-muted">Unavailable</span>}</td>
                    </>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}
