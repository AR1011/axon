import { useMemo } from "react";
import { useParams } from "react-router";
import type { SparseHistogram } from "../../api";
import { COLORS } from "../../charts/options";
import { HistogramPlot, LinePlot, PlotFrame } from "../../charts/Plots";
import { Badge, Empty, Facts } from "../../components/atoms";
import { Identity } from "../../components/Identity";
import { Select } from "../../components/Select";
import { coverage, RANGE_OPTIONS, RANGES, useDomain, useHistory, useNow, useUrlState, type Range } from "../../hooks";
import { bytes, bytesPerSecond, count, duration, uptime, type Unit } from "../../model/format";
import { expand } from "../../model/histogram";
import { isDown, LANE_NAMES, LANE_TIP, STALE_MS } from "../../model/live";
import { seriesLine } from "../../model/history";
import { joinPoints, seamGap, type P } from "../../model/series";
import { connections, useLive } from "../../store";
import { listConnections, refOf } from "../select";
import { DownBadge, oneIn, TransportDiagnostics, useConnectionTimings } from "./TransportDiagnostics";
import { NodeName } from "../../components/Flag";

type Live = "tx" | "rx" | "rtt";
interface Spec {
  title: string;
  unit: Unit;
  info: string;
  lines: { name: string; metric: string; rate: boolean; color: string; live?: Live }[];
}

const KERNEL = "The node reads these values from the kernel at each keepalive. Linux counts retransmitted segments, and macOS counts retransmitted bytes. The chart shows only the series that the platform of this node reports.";
const CHARTS: Spec[] = [
  {
    title: "Framed traffic",
    unit: "bytes/s",
    info: "Bytes that this endpoint wrote and read, with the frame headers, below compression and above TLS.",
    lines: [
      { name: "Sent", metric: "connection.sent_bytes", rate: true, color: COLORS.sent, live: "tx" },
      { name: "Received", metric: "connection.received_bytes", rate: true, color: COLORS.received, live: "rx" },
    ],
  },
  {
    title: "Round trip",
    unit: "ns",
    info: "Keepalive is a ping that the session task of the peer answers, so it includes the two processes. TCP is the smoothed estimate of the kernel from acknowledgements: only the network.",
    lines: [
      { name: "Keepalive", metric: "connection.rtt", rate: false, color: COLORS.latency, live: "rtt" },
      { name: "TCP", metric: "connection.tcp.rtt", rate: false, color: COLORS.received },
    ],
  },
  {
    title: "TCP retransmissions",
    unit: "/s",
    info: `Data that the kernel sent again because of loss or a change of order on the path. ${KERNEL}`,
    lines: [
      { name: "Segments", metric: "connection.tcp.retransmitted_segments", rate: true, color: COLORS.rejected },
      { name: "Bytes", metric: "connection.tcp.retransmitted_bytes", rate: true, color: COLORS.latency },
    ],
  },
  {
    title: "TCP congestion window",
    unit: "bytes",
    info: "The quantity of data that the kernel lets this side have in flight. A window that decreases quickly and then increases again shows loss. A window that stays small limits throughput.",
    lines: [{ name: "Window", metric: "connection.tcp.cwnd", rate: false, color: COLORS.sent }],
  },
  {
    title: "Frames",
    unit: "/s",
    info: "The outbox of this endpoint refused the rejected frames (full, closed, deadline or oversized) and gave them back to the caller. The endpoint never drops admitted frames.",
    lines: [
      { name: "Sent", metric: "connection.sent_frames", rate: true, color: COLORS.sent },
      { name: "Received", metric: "connection.received_frames", rate: true, color: COLORS.received },
      { name: "Rejected", metric: "connection.rejected_frames", rate: true, color: COLORS.rejected },
    ],
  },
  {
    title: "Queued bytes",
    unit: "bytes",
    info: "Outbound is the bytes that the endpoint admitted but did not write yet. Inbound is the bytes that it read but that the engine did not take yet.",
    lines: [
      { name: "Outbound", metric: "connection.queued_bytes", rate: false, color: COLORS.sent },
      { name: "Inbound", metric: "connection.inbound_queued_bytes", rate: false, color: COLORS.received },
    ],
  },
];
const METRICS = [...new Set(CHARTS.flatMap((c) => c.lines.map((l) => l.metric)))];

export function ConnectionDetail() {
  const { nodeId = "", peerId = "", direction = "", lane = "" } = useParams();
  const now = useNow();
  const overview = useLive((s) => s.overview);
  const version = useLive((s) => s.version);
  const [range, setRange] = useUrlState<Range>("range", "15m");
  const span = RANGES[range];
  const connection = useMemo(() => listConnections(overview).find((c) => c.ref === refOf([nodeId, peerId, direction, lane])), [overview, nodeId, peerId, direction, lane]);
  const labels = useMemo(() => ({ peer: peerId, direction, lane }), [peerId, direction, lane]);
  const history = useHistory({ metrics: METRICS, node: nodeId, labels, span, points: 180, maxPoints: 4000, maxSeries: 32, sum: "node", enabled: !!connection });
  const timings = useConnectionTimings(nodeId, connection?.peer).data;
  const incarnation = connection?.view.snapshot?.incarnation;

  const charts = useMemo(() => {
    const samples = connection ? connections.samples(connection.key) : [];
    const step = seamGap(history.data?.step);
    return CHARTS.map((chart) => ({
      ...chart,
      lines: chart.lines.map((line) => {
        // Series differing only in `reason` are added up by the server; another boot of the node contributes nothing.
        const retained = seriesLine(history.data, line.metric, (s) => s.incarnation === incarnation, !!line.rate);
        const live = line.live;
        return { name: line.name, color: line.color, points: live ? joinPoints(retained, samples.map((s): P => [s.t, s[live]]), step) : retained };
      }),
    }));
    // eslint-disable-next-line react-hooks/exhaustive-deps -- `version` stands in for the mutable live buffer
  }, [connection, incarnation, history.data, version]);
  const domain = useDomain(span);

  if (!connection) return <Empty title="Connection not in the current view" detail="Possibly the session closed, or the node left." />;
  const { view, peer } = connection;
  const s = peer.session;
  const stale = now - view.snapshot!.timestamp_ms > STALE_MS;
  const short = coverage(history.data, span);
  const rates = connections.latest(connection.key, now);
  const down = isDown(peer);
  // A down lane has no session: its figures would read as zeros measured.
  const session: [string, React.ReactNode][] = down
    ? []
    : [
        // First of the live facts, so a burst on Queued does not move it.
        ["Rate wait", <span key="p" title="The time that the writer waited for the rate cap of this lane, since the session opened.">{duration(s.paced_ns)}</span>],
        ["Session age", uptime(s.age_ns / 1e6)],
        ["Sent", `${bytes(s.tx_wire_bytes)} · ${bytesPerSecond(rates?.tx)}`],
        ["Received", `${bytes(s.rx_wire_bytes)} · ${bytesPerSecond(rates?.rx)}`],
        ["Keepalive RTT", s.rtt_ns ? duration(s.rtt_ns) : "—"],
        ["TCP RTT", s.tcp ? duration(s.tcp.rtt_ns) : "—"],
        ["Retransmitted", s.tcp ? (s.tcp.retransmitted_segments != null ? `${count(s.tcp.retransmitted_segments)} segments` : bytes(s.tcp.retransmitted_bytes)) : "—"],
        ["Queued", bytes(s.outbound_reserved_bytes)],
      ];
  const strip: [string, React.ReactNode][] = [
    ["Lane", <span key="l" title={LANE_TIP}>{LANE_NAMES[peer.lane]}</span>],
    ["Peer incarnation", <Identity key="i" value={peer.incarnation} label="Peer incarnation" />],
    ["Dialled by", peer.outgoing ? view.node.id : peer.node],
    ["Wire version", peer.version],
    ...session,
  ];
  const t = timings?.timings;
  const frames = `The session times ${oneIn(t, "frames")}.`;
  const requests = t?.sample_every === 0 ? "no requests" : `approximately ${oneIn(t, "requests")}`;
  const stages: [string, SparseHistogram | undefined, string][] = [
    ["Keepalive round trip", t?.rtt, "The session times each keepalive ping."],
    ["Request round trip", timings?.request_rtt, `The engine times ${requests}. It does not time watches or requests in parts.`],
    ["Queue wait", t?.queue, frames],
    ["Encode", t?.encode, frames],
    ["Write + flush", t?.write, `The session times ${oneIn(t, "write batches")}.`],
    ["Decode", t?.decode, frames],
  ];
  return (
    <>
      <header className="flex shrink-0 items-center gap-2">
        <h1 className="mono truncate text-[14px] font-semibold tracking-[-0.25px]">
          <NodeName id={view.node.id} className="hover:underline" /> {peer.outgoing ? "→" : "←"} <NodeName id={peer.node} className="hover:underline" /> <span className="font-normal text-muted">{LANE_NAMES[peer.lane]}</span>
        </h1>
        <span className="ml-auto flex items-center gap-3">
          {!history.available ? <Badge tone="warning">Live only</Badge> : history.data?.truncated ? <Badge tone="warning">Partial history</Badge> : short && <Badge tone="muted">{short}</Badge>}
          {down ? (
            <DownBadge peer={peer} />
          ) : (
            <Badge tone={s.closed ? "danger" : stale ? "warning" : "success"}>{s.closed ? `Closed · ${s.closed}` : stale ? "Stale" : "Open"}</Badge>
          )}
          <Select label="Time range" value={range} onChange={setRange} options={RANGE_OPTIONS} />
        </span>
      </header>
      <div className="scroll-y flex flex-1 flex-col tiles">
        <section className="shrink-0 rounded-md border border-border bg-panel">
          <Facts className="border-b border-border" items={strip} />
          <TransportDiagnostics node={view.node.id} peer={peer} wide />
        </section>
        <div className="grid shrink-0 auto-rows-[240px] grid-cols-3 tiles max-[1250px]:grid-cols-2 max-[900px]:grid-cols-1">
          {charts.map((chart) => (
            <PlotFrame key={chart.title} title={chart.title} info={chart.info}>
              <LinePlot
                lines={chart.lines}
                unit={chart.unit}
                domain={domain}
                label={`${chart.title} for this connection`}
                empty={{ title: "No samples in this window", detail: history.available ? "The store has no records for this session yet." : "This chart needs a history source." }}
              />
            </PlotFrame>
          ))}
          {stages.map(([title, h, sampled]) => (
            <PlotFrame key={title} title={title} info={`All samples after the session opened. ${sampled}`}>
              <HistogramPlot h={h ? expand(h) : null} label={`${title} distribution since the session opened`} empty={down ? "No session" : timings ? "No timing samples" : "Wait…"} />
            </PlotFrame>
          ))}
        </div>
      </div>
    </>
  );
}
