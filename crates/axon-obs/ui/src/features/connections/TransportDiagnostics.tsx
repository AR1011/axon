import { useQuery } from "@tanstack/react-query";
import { getJson, type ConnectionTimings, type Peer, type SparseHistogram, type TcpHealth } from "../../api";
import { Badge, Meter } from "../../components/atoms";
import { Info } from "../../components/Info";
import { bytes, count, duration, spread, UNKNOWN, uptime } from "../../model/format";
import { mean } from "../../model/histogram";
import { direction, DOWN, isDown } from "../../model/live";

/** A lane with no session, and for how long. */
export const DownBadge = ({ peer }: { peer: Peer }) => (
  <span title={DOWN}>
    <Badge tone="warning">Down · {uptime(peer.down_ms)}</Badge>
  </span>
);

/** Payload over wire bytes, framing included. Without a compressed frame there is no ratio to speak of. */
function ratio(payload: number, wire: number, compressed: number) {
  if (!compressed) return "not compressed";
  return payload > 0 && wire > 0 ? `${(payload / wire).toFixed(2)}×` : UNKNOWN;
}

/** What framing adds to each frame; hidden once compression makes the wire smaller than the payload. */
const overhead = (payload: number, wire: number, frames: number) => (frames > 0 && wire >= payload ? bytes((wire - payload) / frames) : UNKNOWN);

/** How many of `what` the session times: "one in N", or "no" with sampling off. */
export const oneIn = (t: ConnectionTimings["timings"] | undefined, what: string) => (t?.sample_every === 0 ? `no ${what}` : `one in ${t?.sample_every ?? UNKNOWN} ${what}`);

const SUBMIT_REASONS = ["full", "closed", "deadline", "oversized"];

function tcp(t: TcpHealth): [string, string, boolean][] {
  const cells: [string, string, boolean][] = [
    ["Round trip", spread(t.rtt_ns, t.rtt_var_ns), false],
    ["Retransmit timeout", t.rto_ns ? duration(t.rto_ns) : UNKNOWN, t.rto_ns > 1e9],
    ["Congestion window", bytes(t.cwnd_bytes), t.cwnd_bytes > 0 && t.cwnd_bytes <= 2 * t.mss],
    ["Segment size", bytes(t.mss), false],
  ];
  if (t.retransmitted_segments != null) cells.push(["Retransmitted", `${count(t.retransmitted_segments)} seg`, t.retransmitted_segments > 0]);
  if (t.retransmitted_bytes != null) cells.push(["Retransmitted", bytes(t.retransmitted_bytes), t.retransmitted_bytes > 0]);
  if (t.lost_segments != null) cells.push(["Presumed lost", `${count(t.lost_segments)} seg`, t.lost_segments > 0]);
  if (t.unacked_segments != null) cells.push(["Unacknowledged", `${count(t.unacked_segments)} seg`, false]);
  if (t.received_out_of_order_bytes != null) cells.push(["Out of order in", bytes(t.received_out_of_order_bytes), t.received_out_of_order_bytes > 0]);
  if (t.send_buffer_bytes != null) cells.push(["Send buffer", bytes(t.send_buffer_bytes), false]);
  while (cells.length % 3) cells.push(["", "", false]);
  return cells;
}

/** What the live overview leaves out of a session, polled while it is on screen; the views of one share the query. */
export function useConnectionTimings(node: string, peer?: Peer) {
  return useQuery({
    queryKey: ["connection", node, peer?.node, peer?.incarnation, peer && direction(peer), peer?.lane],
    enabled: !!peer && !isDown(peer),
    refetchInterval: 2000,
    queryFn: ({ signal }) => getJson<ConnectionTimings>("connection", { node_id: node, peer: peer?.node, direction: peer && direction(peer), lane: peer?.lane }, signal),
  });
}

/** `wide` lays the blocks out side by side, for the connection page. */
export function TransportDiagnostics({ node, peer, wide = false }: { node: string; peer?: Peer; wide?: boolean }) {
  const t = useConnectionTimings(node, peer).data?.timings;
  if (!peer) return <section aria-label="Transport diagnostics" className="p-3 text-[10px] text-muted">Not reported by this endpoint</section>;
  if (isDown(peer)) return <section aria-label="Transport diagnostics" className="p-3 text-[10px] text-muted">{DOWN}</section>;
  const s = peer.session;
  const matrix: [string, string, string][] = [
    ["Payload", bytes(s.tx_payload_bytes), bytes(s.rx_payload_bytes)],
    ["Framed bytes", bytes(s.tx_wire_bytes), bytes(s.rx_wire_bytes)],
    ["Frames", count(s.tx_frames), count(s.rx_frames)],
    ["Compressed frames", count(s.tx_compressed_frames), count(s.rx_compressed_frames)],
    ["Compression ratio", ratio(s.tx_payload_bytes, s.tx_wire_bytes, s.tx_compressed_frames), ratio(s.rx_payload_bytes, s.rx_wire_bytes, s.rx_compressed_frames)],
    ["Framing per frame", overhead(s.tx_payload_bytes, s.tx_wire_bytes, s.tx_frames), overhead(s.rx_payload_bytes, s.rx_wire_bytes, s.rx_frames)],
  ];
  const outcomes: [string, number, boolean][] = [
    ["Calls", peer.calls, false],
    ["Pending", peer.pending, false],
    ["Write batches", s.tx_batches, false],
    ["Pings", s.pings, false],
    ["Pongs", s.pongs, false],
    ["Unanswered pings", Math.max(0, s.pings - s.pongs - 1), true],
  ];
  const stages: [string, SparseHistogram | undefined][] = [["Queue wait", t?.queue], ["Encode", t?.encode], ["Write + flush", t?.write], ["Decode", t?.decode]];
  const means = stages.map(([, h]) => (h && mean(h)) ?? 0);
  const slowest = Math.max(...means, 1);
  // Under the connection page's title it is the next level; in the inspector it sits under the session's name.
  const Heading = wide ? "h2" : "h3";
  return (
    <section aria-label="Transport diagnostics" className={`p-3 text-[10px] ${wide ? "grid grid-cols-4 content-start gap-x-8 gap-y-3 max-[1250px]:grid-cols-2 max-[700px]:grid-cols-1" : "flex flex-col gap-3"}`}>
      <header className="col-span-full flex items-center gap-2">
        <Heading className="text-[11px] font-[550]">Transport diagnostics</Heading>
        <Info label="About transport diagnostics">Framed bytes are what the session wrote and read, with the headers. They do not include TLS, TCP and network overhead. Payload is the message bytes before compression.</Info>
      </header>
      <table className="[&_td]:h-6 [&_td]:px-0 [&_td]:leading-tight [&_td]:whitespace-normal [&_th]:static [&_th]:bg-transparent [&_th]:shadow-none">
        <thead>
          <tr className="text-muted"><th className="w-[40%]"><span className="sr-only">Metric</span></th><th className="text-right text-[10px]">↑ Sent</th><th className="text-right text-[10px]">↓ Received</th></tr>
        </thead>
        <tbody>
          {matrix.map(([name, tx, rx]) => (
            <tr key={name} className="hover:bg-transparent"><td className="font-sans text-muted">{name}</td><td className="text-right">{tx}</td><td className="text-right">{rx}</td></tr>
          ))}
        </tbody>
      </table>
      <div className="flex flex-col gap-3">
      <div className="grid grid-cols-2 gap-3">
        {([["Outbound", s.outbound_reserved_bytes, s.outbound_queued_frames], ["Inbound", s.inbound_reserved_bytes, s.inbound_queued_frames]] as const).map(([name, reserved, frames]) => (
          <div key={name}>
            <div className="mb-1 flex justify-between gap-2"><span className="truncate text-muted" title={`${name} queue`}>{name} queue</span><span className="mono shrink-0">{bytes(reserved)}</span></div>
            <Meter value={reserved} max={Math.max(reserved, 1 << 20)} label={`${name} queue ${bytes(reserved)}`} />
            <p className="mt-1 text-[10px] text-muted">{count(frames)} frames queued</p>
          </div>
        ))}
      </div>
      <Cells cells={outcomes.map(([name, value, bad]) => [name, count(value), bad && value > 0])} />
      </div>
      <div>
        <div className="mb-1 flex items-center gap-2">
          <span className="text-muted">Mean stage duration</span>
          <Info label="About stage durations">Each stage has its own sampled population, so the sum of the durations is not the end-to-end latency. The session times {oneIn(t, "frames")} and {oneIn(t, "write batches")}.</Info>
        </div>
        {stages.map(([name, h], i) => (
          <div key={name} title={`${count(h?.count ?? 0)} samples`} className="grid grid-cols-[82px_1fr_65px] items-center gap-2 py-[3px]">
            <span className="text-muted">{name}</span>
            <Meter thin value={means[i]} max={slowest} label={`${name} ${duration(means[i])}`} />
            <span className="mono text-right">{h?.count ? duration(means[i]) : UNKNOWN}</span>
          </div>
        ))}
      </div>
      <div>
        <div className="mb-1 flex items-center gap-2">
          <span className="text-muted">TCP link</span>
          <Info label="About the TCP link">The data that the kernel has about this connection, read at each keepalive. Retransmissions, a timeout that increases or a very small window show a problem on the network path. They do not cause data loss, because TCP repairs the data.</Info>
        </div>
        {s.tcp ? (
          <Cells cells={tcp(s.tcp)} />
        ) : (
          <p className="text-muted">Not reported by this endpoint's platform, or not sampled yet.</p>
        )}
      </div>
      <div className={wide ? "col-span-full" : ""}>
        <p className="mb-1 text-muted">Frames refused at the outbox</p>
        <p className="mono">
          {SUBMIT_REASONS.map((reason, i) => (
            <span key={reason} className={`mr-3 ${s.tx_rejected?.[i] ? "text-warning" : ""}`}>{reason} {count(s.tx_rejected?.[i] ?? 0)}</span>
          ))}
        </p>
      </div>
      {s.closed && <p className="col-span-full text-warning">Closed: {s.closed}</p>}
    </section>
  );
}

function Cells({ cells }: { cells: [string, string, boolean][] }) {
  return (
    <dl className="grid grid-cols-3 gap-px overflow-hidden rounded-md border border-border bg-border">
      {cells.map(([name, value, bad], i) => (
        <div key={i} className="bg-panel px-2 py-1.5">
          <dt className="text-[10px] text-muted">{name}</dt>
          <dd className={`mono ${bad ? "text-warning" : ""}`}>{value}</dd>
        </div>
      ))}
    </dl>
  );
}
