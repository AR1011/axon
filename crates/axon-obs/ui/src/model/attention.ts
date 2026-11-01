import { HANDSHAKE_FAILURES, type HistorySource, type NodeView, type Overview, type Snapshot } from "../api";
import { nodePath } from "../features/select";
import type { Tone } from "../hooks";
import { ago, count, duration, reasons, shortType } from "./format";
import { DEPARTED_MS, growth, holding, STALE_MS, type Counter, type NodeBuffer } from "./live";

export interface Concern {
  tone: Tone;
  node: string;
  title: string;
  detail: string;
  to?: string;
}

// ponytail: fixed thresholds; make them settings if anyone asks.
const OLD_MESSAGE_NS = 5e9;
const WINDOW_MS = 60_000;

/**
 * Counters that run since boot: only their growth over the last minute says something is wrong now. A `floor` (per
 * second, averaged over the minute) leaves out what is normal in small amounts.
 */
const RATES: [field: Counter, tone: Tone, title: string, what: string | ((s: Snapshot) => string), to: string, floor?: number][] = [
  ["leasesLost", "danger", "Leases lost", "leases lost, and their keyed actors stopped", "/placement"],
  ["handoffsFailed", "warning", "Failed handoffs", "handoffs of keyed actors that failed", "/placement"],
  ["restarts", "warning", "Actor restarts", "restarts", "/actors"],
  ["mailboxRejected", "warning", "Messages refused at mailboxes", "refused, full or closed", "/actors"],
  ["deadLetters", "warning", "Dead letters", "dead letters", "/dead-letters"],
  ["limitDenied", "warning", "Quota limit denials", "denied at a limit", "/cluster"],
  // A share denial is a node's slice running out before the replicas rebalance, which a busy key does now and then.
  ["shareDenied", "warning", "Quota share denials", "denied at the share of a limit for this node", "/cluster", 1],
  ["quotaRefused", "warning", "Stream namespace limits", "stream requests refused at a namespace limit", "/streams"],
  ["versionRefused", "warning", "Peers on another wire version", "handshakes refused for the wire version", "/connections"],
  ["refused", "warning", "Peers refused at the listener", ({ cluster: { engine: { listener: l } } }) => reasons(["busy", "per IP", ...HANDSHAKE_FAILURES.map((r) => `handshake ${r}`)], [l.rejected_busy, l.rejected_per_ip, ...l.handshake_failures]), "/connections"],
];

/** Only what is wrong with a collector, or "" when nothing is. */
export function collectorProblem(c: HistorySource["collector"]): string {
  if (!c) return "";
  const sinks = Object.keys(c.sink_errors).length;
  return [c.dropped_events && `${count(c.dropped_events)} dropped events`, c.write_errors && `${count(c.write_errors)} write errors`, c.rejected_batches && `${count(c.rejected_batches)} rejected batches`, sinks && `${sinks} ${sinks === 1 ? "sink" : "sinks"} with errors`]
    .filter(Boolean)
    .join(" · ");
}

/** On the collector's own clock: subtracting its wall stamp from the browser's would show any skew between them. */
export const collected = (c: HistorySource["collector"]) => (c?.collected_ago_ms != null ? `collected ${ago(c.collected_ago_ms)}` : "no collection yet");

/** What deserves a look right now, most severe first. `stale` is when the console's view last moved and why it stopped, null while it moves. */
export function attention(overview: Overview | null, departed: NodeView[], now: number, stale: { since: number; why: string } | null, buffer: NodeBuffer): Concern[] {
  if (!overview) return [];
  const out: Concern[] = [];
  const add = (tone: Tone, node: string, title: string, detail: string, to?: string) => out.push({ tone, node, title, detail, to });
  // Every node's snapshot ages with the console's view, so one row says why instead of one per node.
  if (stale) add("danger", overview.observer, stale.why, `last seen ${ago(now - stale.since)}`);
  if (overview.discovery_error) add("warning", overview.observer, "Discovery unavailable", overview.discovery_error, "/cluster");
  if (overview.history.error) add("warning", overview.history.node ?? "", "History unavailable", overview.history.error);
  for (const src of overview.history.sources) {
    const problem = collectorProblem(src.collector);
    if (src.error) add("warning", src.node, "Metrics store unreachable", src.error);
    else if (problem) add("warning", src.node, "Collector data loss", problem);
  }
  for (const v of departed) {
    if (v.leaveReason === null) add("danger", v.node.id, "Lost", `${ago(now - v.leftAt!)} · stopped without a drain or a reason: a crash, a kill or a cut`, nodePath(v.node.id));
    else add("muted", v.node.id, "Left the membership", `${ago(now - v.leftAt!)} · ${v.leaveReason ?? "reason not reported"}`, nodePath(v.node.id));
  }
  for (const e of overview.events) {
    if (e.kind === "gave_up" && now - e.time_ms < DEPARTED_MS) add("warning", e.node_id, `Supervisor stopped ${e.subject}`, `${ago(now - e.time_ms)} · no more restarts after ${e.detail || "a failure"}`, `/actors?aq=${encodeURIComponent(e.subject)}`);
  }
  // The newest of a node's stream events, to name what a count leaves out.
  const latest = (node: string, kind: string) => overview.events.filter((e) => e.node_id === node && e.kind === kind).sort((a, b) => b.time_ms - a.time_ms).at(0);
  for (const v of overview.nodes) {
    const id = v.node.id;
    const s = v.snapshot;
    const to = nodePath(id);
    if (!v.reachable) add("danger", id, "Unreachable", v.error ?? "The dashboard lost its session to this node.", to);
    else if (!s) add("warning", id, "No snapshot", v.error ?? "The node did not answer yet.", to);
    if (!s) continue;
    if (!stale && now - s.timestamp_ms > STALE_MS) add("warning", id, "Stale snapshot", `Last reported ${ago(now - s.timestamp_ms)}`, to);
    if (s.lifecycle.phase !== "running") add("warning", id, `Lifecycle · ${s.lifecycle.phase}`, s.lifecycle.reason ?? "", to);
    if (s.cluster.isolated) add("danger", id, "Isolated", "The node lost discovery and its peers. It stopped its keyed actors.", "/cluster");
    if (s.cluster.last_heartbeat_error) add("warning", id, "Heartbeat errors", s.cluster.last_heartbeat_error, "/cluster");
    const rows = buffer.rows(id, s.incarnation);
    for (const [field, tone, title, what, link, floor = 0] of RATES) {
      const g = growth(rows, (r) => r[field] ?? 0, WINDOW_MS);
      if (g && g.n <= (floor * WINDOW_MS) / 1000) continue;
      const last = g && (g.span >= WINDOW_MS ? "minute" : `${Math.round(g.span / 1000)}s`);
      if (g) add(tone, id, title, `${count(g.n)} in the last ${last} · since ${ago(now - g.since)}${g.open ? " or earlier" : ""} · ${count(g.total)} since start, ${typeof what === "string" ? what : what(s)}`, link);
    }
    const failing = holding(rows, (r) => r.diskFailing != null);
    if (failing != null) add("danger", id, "Stream disk failure", `since ${ago(now - failing)} · ${rows.at(-1)!.diskFailing} · the node refuses publishes to the streams it keeps on disk`, "/streams");
    for (const t of s.actor_types) if (t.oldest_message_age_ns > OLD_MESSAGE_NS) add("warning", id, `Backlog in ${shortType(t.actor_type)}`, `Oldest message: ${duration(t.oldest_message_age_ns)} in the mailbox · ${count(t.mailbox_depth)} queued`, `/actors?atype=${encodeURIComponent(t.actor_type)}`);
    if (s.streams?.orphaned) {
      const [orphaned, lost] = [latest(id, "orphaned"), latest(id, "parent_lost")];
      const named = [orphaned?.subject, lost && `last parent: ${lost.detail}`].filter(Boolean).join(" · ");
      add("warning", id, "Streams without a parent", `${count(s.streams.orphaned)} pulled streams have no source now${named ? ` · ${named}` : ""}`, "/streams");
    }
    if (s.streams?.hub.recovering) add("muted", id, "Stream replay", `${count(s.streams.hub.recovering)} subscriptions in replay`, "/streams");
    for (const n of s.notices) add("muted", id, "Collection gap", n.message, to);
  }
  const rank: Record<Tone, number> = { danger: 0, warning: 1, muted: 2, success: 3 };
  return out.sort((a, b) => rank[a.tone] - rank[b.tone]);
}
