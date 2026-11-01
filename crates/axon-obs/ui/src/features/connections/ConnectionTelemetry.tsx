import { useMemo } from "react";
import { Link } from "react-router";
import { COLORS } from "../../charts/options";
import { LinePlot } from "../../charts/Plots";
import { Empty } from "../../components/atoms";
import { Identity } from "../../components/Identity";
import { Info } from "../../components/Info";
import { Select } from "../../components/Select";
import { ToggleGroup } from "../../components/ToggleGroup";
import { coverage, RANGE_OPTIONS, RANGES, useDomain, useHistory, useNow, useUrlState, type Range } from "../../hooks";
import { ago, bytes, bytesPerSecond, count, duration } from "../../model/format";
import { direction, LANE_NAMES, LANE_TIP, STALE_MS } from "../../model/live";
import { seriesLine } from "../../model/history";
import { joinPoints, seamGap, type P } from "../../model/series";
import { connections, useLive } from "../../store";
import { connectionPath, type Connection } from "../select";
import { TransportDiagnostics } from "./TransportDiagnostics";
import { NodeName } from "../../components/Flag";

type Measure = "traffic" | "rtt";
const METRICS: Record<Measure, string[]> = { traffic: ["connection.sent_bytes", "connection.received_bytes"], rtt: ["connection.rtt"] };

/** `compact` is the inspector's version: the chart, with the diagnostics a link away. */
export function ConnectionTelemetry({ connection, compact = false, pageRange }: { connection?: Connection; compact?: boolean; pageRange?: Range }) {
  const now = useNow();
  const version = useLive((s) => s.version);
  const [measure, setMeasure] = useUrlState<Measure>("cm", "traffic");
  const [own, setRange] = useUrlState<Range>("range", "15m");
  const range = pageRange ?? own;
  const span = RANGES[range];
  const labels = useMemo(() => (connection ? { peer: connection.peer.node, direction: direction(connection.peer), lane: connection.peer.lane } : undefined), [connection]);
  const history = useHistory({ metrics: METRICS[measure], node: connection?.view.node.id, labels, span, points: 180, maxPoints: 2000, maxSeries: 8, enabled: !!connection });

  const lines = useMemo(() => {
    if (!connection) return [];
    const samples = connections.samples(connection.key);
    const incarnation = connection.view.snapshot!.incarnation;
    const step = seamGap(history.data?.step);
    // Only this boot of the node: a series from another incarnation yields nothing.
    const retained = (metric: string, rate: boolean): P[] => seriesLine(history.data, metric, (s) => s.incarnation === incarnation, rate);
    const live = (pick: (s: (typeof samples)[number]) => number | null): P[] => samples.map((s) => [s.t, pick(s)]);
    if (measure === "rtt") return [{ name: "Keepalive round trip", color: COLORS.latency, points: joinPoints(retained("connection.rtt", false), live((s) => s.rtt), step) }];
    return [
      { name: "Sent", color: COLORS.sent, points: joinPoints(retained("connection.sent_bytes", true), live((s) => s.tx), step) },
      { name: "Received", color: COLORS.received, points: joinPoints(retained("connection.received_bytes", true), live((s) => s.rx), step) },
    ];
    // eslint-disable-next-line react-hooks/exhaustive-deps -- `version` stands in for the mutable live buffer
  }, [connection, measure, history.data, version]);

  const domain = useDomain(span);
  if (!connection) return <Empty title="No connection selected" detail="Select a peer session to see its traffic." />;
  const { view, peer } = connection;
  const age = now - view.snapshot!.timestamp_ms;
  const provenance = history.data ? "from retained history and live observations" : "from live observations";
  const latest = connections.samples(connection.key).at(-1);
  const trouble = history.error ?? history.data?.errors.join(" · ");
  const short = coverage(history.data, span);
  const stats: [label: string, value: string, title?: string][] = [
    ["Sent", bytesPerSecond(latest?.tx)],
    ["Received", bytesPerSecond(latest?.rx)],
    ["RTT", peer.session.rtt_ns ? duration(peer.session.rtt_ns) : "—", "Keepalive round trip"],
    ["Queued", count(peer.session.outbound_queued_frames), `${bytes(peer.session.outbound_reserved_bytes)} that wait for their write to the peer`],
  ];
  return (
    <div className="flex min-h-0 flex-col">
      <header className="flex flex-wrap items-center gap-x-2 gap-y-1.5 border-b border-border px-3 py-2">
        <h3 className="mono basis-full text-[12px] font-[550] break-words">
          <NodeName id={view.node.id} link={false} /> <span className="text-muted">{peer.outgoing ? "→" : "←"}</span> <NodeName id={peer.node} link={false} /> <span className="font-normal text-muted" title={LANE_TIP}>{LANE_NAMES[peer.lane]}</span>
        </h3>
        <Identity value={peer.incarnation} label="Peer incarnation" />
        <span className="ml-auto flex shrink-0 items-center gap-1.5">
          <ToggleGroup label="Measurement" value={measure} onChange={setMeasure} options={[{ value: "traffic", label: "Framed traffic per second", icon: "traffic" }, { value: "rtt", label: "Keepalive round trip", icon: "latency" }]} />
          {!pageRange && <Select label="Time range" value={range} onChange={setRange} options={RANGE_OPTIONS} />}
          <Link to={connectionPath(connection)} aria-label="Open connection" title="Open connection" className="flex size-[25px] items-center justify-center rounded-sm text-muted hover:bg-raised hover:text-text">
            ↗
          </Link>
        </span>
      </header>
      <dl className="grid shrink-0 grid-cols-[repeat(auto-fit,minmax(96px,1fr))] overflow-hidden border-b border-border">
        {stats.map(([label, value, title]) => (
          <div key={label} title={title} className="-mb-px min-w-0 border-r border-b border-border px-3 py-1.5">
            <dt className="text-[10px] text-muted">{label}</dt>
            <dd className="mono truncate text-[12px]">{value}</dd>
          </div>
        ))}
      </dl>
      {(age > STALE_MS || !history.available || history.data?.truncated || trouble || short) && (
        <p className="flex flex-wrap items-center gap-x-3 px-3 pt-1.5 text-[10px] text-warning">
          {age > STALE_MS && <span>Stale · observed {ago(age)}</span>}
          {!history.available && <span className="text-muted">Live only · no metrics store</span>}
          {history.data?.truncated && <span>Partial history</span>}
          {short && <span className="text-muted">{short}</span>}
          {trouble && <Info label="History error">{trouble}</Info>}
        </p>
      )}
      <div className="h-[210px] shrink-0">
        <LinePlot
          lines={lines}
          unit={measure === "traffic" ? "bytes/s" : "ns"}
          domain={domain}
          label={`${measure === "traffic" ? "Sent and received bytes per second" : "Keepalive round trip"} ${provenance}`}
          empty={{ title: "No samples yet", detail: measure === "traffic" ? "A rate needs two observations of this session." : "Keepalive pings measure the round trip." }}
        />
      </div>
      {!compact && (
        <div className="border-t border-border">
          <TransportDiagnostics node={view.node.id} peer={peer} />
        </div>
      )}
    </div>
  );
}
