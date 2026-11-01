import type { HistoryResult, Series } from "../api";
import { natural, type Unit } from "./format";
import { aggregate } from "./kpi";
import { latest, type P } from "./series";

/** Every stored metric, as telemetry.md § Persisted samples names and kinds them, with a line on what it counts; a test holds the two equal. */
export const METRICS: Record<string, [kind: "counter" | "gauge" | "histogram", about: string]> = {
  "runtime.actors": ["gauge", "Actors alive on the node"],
  "runtime.actors_started": ["counter", "Actors started"],
  "runtime.actors_stopped": ["counter", "Actors that stopped, linked stops included"],
  "runtime.actors_failed": ["counter", "Actors that stopped on their own fault"],
  "runtime.dead_letters": ["counter", "Messages no actor took"],
  "runtime.worker.turns": ["counter", "Actor turns a worker ran"],
  "runtime.worker.parks": ["counter", "Times a worker went to sleep because it had no work"],
  "runtime.worker.busy": ["counter", "Time a worker was awake, its spin included"],
  "runtime.worker.queued": ["gauge", "Actors that are ready and wait for a worker"],
  "runtime.worker.utilisation": ["gauge", "Share of time a worker was busy"],
  "actor.count": ["gauge", "Actors of a type alive"],
  "actor.mailbox_depth": ["gauge", "Messages queued in mailboxes"],
  "actor.mailbox_depth_max": ["gauge", "Deepest mailbox of the type"],
  "actor.mailbox_bytes": ["gauge", "Bytes queued in mailboxes"],
  "actor.waiters": ["gauge", "Senders that wait for room in a mailbox"],
  "actor.requests": ["gauge", "Asks that wait for a reply"],
  "actor.oldest_message_age": ["gauge", "Age of the oldest sampled message in a mailbox"],
  "actor.admitted": ["counter", "Messages admitted into a mailbox"],
  "actor.processed": ["counter", "Messages handled"],
  "actor.turns": ["counter", "Turns run: a batch of messages each"],
  "actor.budget_overruns": ["counter", "Turns longer than their time budget"],
  "actor.restarts": ["counter", "Supervised restarts"],
  "actor.asks": ["counter", "Asks sent to the type"],
  "actor.ask_timeouts": ["counter", "Asks that got no reply in time"],
  "actor.rejected": ["counter", "Messages refused, by reason"],
  "actor.mailbox_wait": ["histogram", "Time a message waited before its turn"],
  "actor.handler": ["histogram", "Time in on_message"],
  "actor.turn": ["histogram", "Time a whole turn took"],
  "actor.timer_lag": ["histogram", "Delay of timers after their due time"],
  "actor.message.handler": ["histogram", "Handler time per message label"],
  "cluster.members": ["gauge", "Nodes in the view of this node"],
  "cluster.unreachable": ["gauge", "Members this node cannot reach"],
  "cluster.view_stale": ["gauge", "1 while discovery gives no recent answer"],
  "cluster.isolated": ["gauge", "1 while this node has no connection to a majority"],
  "cluster.connections": ["gauge", "Sessions open to peers"],
  "cluster.pending_requests": ["gauge", "Remote calls that wait for a reply"],
  "cluster.endpoints": ["gauge", "Names this node publishes"],
  "cluster.spawned": ["gauge", "Actors started for other nodes"],
  "cluster.calls_started": ["counter", "Remote calls sent"],
  "cluster.inbound_calls": ["counter", "Remote calls received"],
  "cluster.inbound_expired": ["counter", "Received calls dropped before they ran, after their deadline"],
  "cluster.connects": ["counter", "Sessions opened"],
  "cluster.dial_failures": ["counter", "Dials that failed"],
  "cluster.discovery_errors": ["counter", "Discovery lookups that failed"],
  "cluster.heartbeat_failures": ["counter", "Heartbeats with no answer"],
  "cluster.route_hits": ["counter", "Route lookups found in the cache"],
  "cluster.route_misses": ["counter", "Route lookups not found in the cache"],
  "cluster.spawn_requests": ["counter", "Remote spawn requests to this node"],
  "cluster.spawn_refused": ["counter", "Remote spawns this node refused"],
  "cluster.handoffs": ["counter", "Keyed actors moved to another node"],
  "cluster.handoffs_failed": ["counter", "Handoffs that failed"],
  "cluster.keyed": ["gauge", "Keyed actors on this node, by kind"],
  "cluster.keyed_started": ["counter", "Keyed actors started, by kind and how"],
  "cluster.keyed_ended": ["counter", "Keyed actors ended, by kind and reason"],
  "cluster.keyed_failed": ["counter", "Keyed actors that failed to start"],
  "cluster.leases_lost": ["counter", "Key leases lost, one per key"],
  "cluster.isolations": ["counter", "Times this node became isolated"],
  "cluster.calls": ["counter", "Remote calls finished, by outcome"],
  "cluster.inbound_rejected": ["counter", "Received calls refused, by reason"],
  "cluster.membership_changes": ["counter", "Joins, leaves, replacements, unreachables"],
  "cluster.request_rtt": ["histogram", "Remote request round trip"],
  "quota.entries": ["gauge", "Limits and keys this node used recently"],
  "quota.permits": ["gauge", "Permits held on this node"],
  "quota.keys": ["gauge", "Keys this node replicates"],
  "quota.rows": ["gauge", "Rows of those keys, one for each node that uses a key"],
  "quota.pulling": ["gauge", "1 while this replica pulls the rows of the other replicas"],
  "quota.fallback": ["counter", "Units admitted without a fresh grant from a majority"],
  "quota.mismatched": ["counter", "Reports with a different maximum for a key"],
  "quota.reports": ["counter", "Usage reports sent"],
  "quota.report_failures": ["counter", "Usage reports that failed"],
  "quota.denied": ["counter", "Takes and acquires refused, by reason"],
  "stream.streams": ["gauge", "Streams in the hub of this node"],
  "stream.originated": ["gauge", "Streams this node publishes"],
  "stream.feeds": ["gauge", "Streams pulled from a parent"],
  "stream.orphaned": ["gauge", "Pulled streams with no parent now"],
  "stream.sessions": ["gauge", "Subscriber sessions"],
  "stream.subscriptions": ["gauge", "Subscriptions open"],
  "stream.recovering": ["gauge", "Subscriptions in replay from history"],
  "stream.demand_keys": ["gauge", "Streams with demand on this node"],
  "stream.queued_bytes": ["gauge", "Bytes queued for subscribers"],
  "stream.queued_records": ["gauge", "Records queued for subscribers"],
  "stream.retained": ["gauge", "Bytes of history and compacted state that the node keeps"],
  "stream.push_inflight_bytes": ["gauge", "Bytes in pushes to children with no answer yet"],
  "stream.published_batches": ["counter", "Batches published here"],
  "stream.published_records": ["counter", "Records published here"],
  "stream.published_bytes": ["counter", "Bytes published here"],
  "stream.duplicate_records": ["counter", "Records received before, dropped"],
  "stream.publish_rejected": ["counter", "Publishes refused"],
  "stream.delivered_records": ["counter", "Records delivered to subscribers"],
  "stream.delivered_bytes": ["counter", "Bytes delivered to subscribers"],
  "stream.replayed_records": ["counter", "Records replayed from history"],
  "stream.recovery_blocked": ["counter", "History reads that waited"],
  "stream.coalesced": ["counter", "Batches merged before delivery"],
  "stream.parents_lost": ["counter", "Parents that a stream lost"],
  "stream.switches": ["counter", "Streams moved to a better parent"],
  "stream.local_resets": ["counter", "Pulled streams with a continuity break on this node"],
  "stream.downstream_expired": ["counter", "Children dropped because they became quiet"],
  "stream.unwanted_items": ["counter", "Records from a parent that no stream on this node follows"],
  "stream.push_resent": ["counter", "Pushes sent again after a failed call or a gap"],
  "stream.push_capped": ["counter", "Times a child waited for the node's cap on pushes in flight"],
  "stream.resets": ["counter", "Subscriptions reset, by reason"],
  "stream.ends": ["counter", "Subscriptions ended, by reason"],
  "stream.quota_refused": ["counter", "Refused at a namespace limit"],
  "stream.refusals": ["counter", "Refusals from parents, by reason"],
  "stream.interests": ["counter", "Interest sets sent to parents, by outcome"],
  "stream.interest_keys": ["counter", "Streams named in those interests"],
  "stream.pushes": ["counter", "Pushes between nodes, by direction"],
  "stream.push_bytes": ["counter", "Bytes pushed between nodes, by direction"],
  "stream.push_skipped": ["counter", "Pushes this node did not apply, by reason"],
  "stream.disk_streams": ["gauge", "Streams kept on disk"],
  "stream.disk_bytes": ["gauge", "Bytes of history on disk"],
  "stream.disk_unwritten": ["gauge", "Bytes admitted, not synced yet"],
  "stream.disk_failing": ["gauge", "1 while disk writes fail"],
  "stream.disk_writes": ["counter", "Disk writes, by outcome"],
  "stream.disk_refused": ["counter", "Publishes refused for the disk"],
  "stream.disk_truncated": ["counter", "Segments found truncated or corrupt"],
  "trace.spans": ["counter", "Spans recorded"],
  "telemetry.samples_skipped": ["counter", "Unchanged samples omitted from a batch"],
  "listener.accepted": ["counter", "Connections accepted"],
  "listener.accept_errors": ["counter", "Accepts that failed"],
  "listener.rejected": ["counter", "Connections refused, by reason"],
  "listener.handshake_failures": ["counter", "Handshakes that failed, by reason"],
  "connection.sent_bytes": ["counter", "Bytes on the wire, out"],
  "connection.received_bytes": ["counter", "Bytes on the wire, in"],
  "connection.sent_payload_bytes": ["counter", "Payload bytes before compression, out"],
  "connection.received_payload_bytes": ["counter", "Payload bytes after decompression, in"],
  "connection.sent_frames": ["counter", "Frames sent"],
  "connection.received_frames": ["counter", "Frames received"],
  "connection.sent_compressed_frames": ["counter", "Frames sent compressed"],
  "connection.received_compressed_frames": ["counter", "Frames received compressed"],
  "connection.write_batches": ["counter", "Socket writes, frames batched"],
  "connection.calls": ["counter", "Calls over the session"],
  "connection.paced": ["counter", "Time the writer waited for the rate cap of its lane"],
  "connection.tcp.retransmitted_segments": ["counter", "TCP segments resent (Linux)"],
  "connection.tcp.retransmitted_bytes": ["counter", "TCP bytes resent (macOS)"],
  "connection.rejected_frames": ["counter", "Sends refused locally, by reason"],
  "connection.queued_bytes": ["gauge", "Bytes queued for the socket"],
  "connection.queued_frames": ["gauge", "Frames queued for the socket"],
  "connection.inbound_queued_bytes": ["gauge", "Bytes read, not handled yet"],
  "connection.inbound_queued_frames": ["gauge", "Frames read, not handled yet"],
  "connection.pending_requests": ["gauge", "Requests that wait for a reply"],
  "connection.rtt": ["gauge", "Keepalive round trip"],
  "connection.tcp.rtt": ["gauge", "Smoothed TCP round trip of the kernel"],
  "connection.tcp.cwnd": ["gauge", "TCP congestion window"],
  "connection.queue": ["histogram", "Time a frame waited before the write"],
  "connection.encode": ["histogram", "Time to encode a frame"],
  "connection.write": ["histogram", "Time to write a batch"],
  "connection.decode": ["histogram", "Time to decode a frame"],
  "connection.rtt_distribution": ["histogram", "Keepalive round trips"],
  "connection.request_rtt": ["histogram", "Request round trip over the session"],
  "process.rss_bytes": ["gauge", "Resident memory"],
  "process.virtual_bytes": ["gauge", "Virtual memory"],
  "process.cpu_percent": ["gauge", "CPU the process used"],
  "process.read_bytes": ["counter", "Bytes the process read from disk"],
  "process.written_bytes": ["counter", "Bytes the process wrote to disk"],
  "host.total_memory_bytes": ["gauge", "Memory on the host"],
  "host.available_memory_bytes": ["gauge", "Memory free for use"],
  "host.cpu_percent": ["gauge", "CPU used across the host"],
  "host.received_bytes": ["counter", "Bytes the network of the host received"],
  "host.transmitted_bytes": ["counter", "Bytes the network of the host sent"],
};
const METRIC_NAMES = Object.keys(METRICS);

/** The metrics whose name or line holds every word of `query`: the name itself first, then names that start with it. */
export function findMetrics(query: string): string[] {
  const q = query.trim().toLowerCase();
  const rank = (name: string) => (name === q ? 0 : name.startsWith(q) ? 1 : 2);
  const words = q.split(/\s+/).filter(Boolean);
  return METRIC_NAMES.filter((name) => words.every((w) => `${name} ${METRICS[name][1]}`.toLowerCase().includes(w))).toSorted((a, b) => rank(a) - rank(b));
}

/** Lines drawn at once; the rest are counted beside the chart. */
export const MAX_LINES = 8;

const GAUGE_UNITS: Record<string, Unit> = { bytes: "bytes", nanoseconds: "ns", percent: "%", ratio: "ratio" };

/** Gauges that mean nothing added up: a ratio, a percentage, a duration. Their lines take the highest series. */
export const highest = (kind: string, unit: string) => kind === "gauge" && ["ratio", "percent", "nanoseconds"].includes(unit);

/** A gauge is charted as its value; a counter, and a histogram's count, as its rate. */
export const exploreUnit = (kind: string, unit: string): Unit => (kind === "gauge" ? (GAUGE_UNITS[unit] ?? "count") : unit === "bytes" ? "bytes/s" : "/s");

/** The label names the series carry, each one a way to split the chart. */
export const labelNames = (series: Pick<Series, "labels">[]) => [...new Set(series.flatMap((s) => Object.keys(s.labels)))].sort(natural);

/** The k=v pairs the series carry, but those already kept, each with how many series carry it. */
export function labelPairs(series: Pick<Series, "labels">[], keep: Record<string, string>): { key: string; pair: string; series: number }[] {
  const counts = new Map<string, { key: string; pair: string; series: number }>();
  for (const s of series)
    for (const [key, value] of Object.entries(s.labels)) {
      if (keep[key] === value) continue;
      const pair = `${key}=${value}`;
      const seen = counts.get(pair);
      if (seen) seen.series++;
      else counts.set(pair, { key, pair, series: 1 });
    }
  return [...counts.values()].sort((a, b) => natural(a.pair, b.pair));
}

/** The part of `k=v, k=v` text being typed: after the last comma. */
export const typing = (text: string) => text.split(",").at(-1)!.trim();

/** The labels text with `pair` in it, in place of its key's: completing, in place of the part being typed too. */
export function withLabel(text: string, pair: string, completing: boolean): string {
  const parts = text.split(",").map((part) => part.trim());
  if (completing) parts.pop();
  const key = (part: string) => part.split("=")[0].trim();
  return [...parts.filter((part) => part && key(part) !== key(pair)), pair].join(", ");
}

/** `k=v, k=v` as the labels `api/history` keeps; a part without `=` is left out. */
export function parseLabels(text: string): Record<string, string> {
  const pairs = text.split(",").map((part) => {
    const at = part.indexOf("=");
    return at < 0 ? [] : [part.slice(0, at).trim(), part.slice(at + 1).trim()];
  });
  return Object.fromEntries(pairs.filter(([k, v]) => k && v));
}

/**
 * One line per value of `by`: "" adds every series into one, "node" splits by node, any other is a label name (series
 * without it share `—`). A `highest` gauge takes its highest series instead of adding them. The highest now first, at
 * most MAX_LINES; `hidden` counts the rest.
 */
export function exploreLines(r: HistoryResult | undefined, by: string, total = "All series"): { lines: { name: string; points: P[] }[]; hidden: number } {
  const groups = new Map<string, Series[]>();
  for (const s of r?.series ?? []) {
    const name = by === "" ? total : by === "node" ? s.node_id : (s.labels[by] ?? "—");
    const group = groups.get(name);
    if (group) group.push(s);
    else groups.set(name, [s]);
  }
  const mode = ({ kind, unit }: Series) => (highest(kind, unit) ? "max" : kind === "gauge" ? "sum" : "rate");
  const lines = [...groups].map(([name, part]) => ({ name, points: aggregate(r!, part, mode(part[0])) }));
  lines.sort((a, b) => (latest(b.points) ?? -Infinity) - (latest(a.points) ?? -Infinity) || natural(a.name, b.name));
  return { lines: lines.slice(0, MAX_LINES), hidden: Math.max(0, lines.length - MAX_LINES) };
}
