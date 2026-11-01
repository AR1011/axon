import type { SpanRecord, TraceSummary } from "../api";
import { shortType } from "./format";

export interface TraceRow {
  span: SpanRecord;
  depth: number;
  /** The parent named by the span is not among the spans: its ring dropped it, or it was never sampled. */
  orphan: boolean;
  /** Time no span accounts for, from the end of the parent to this span's arrival: the wire between nodes, or a timer's delay. */
  gapUs: number;
  /** The span this one names as its parent, where it is held. */
  cause?: SpanRecord;
  /** One per level of indent: whether the tree's line runs on below this row there. */
  guides: boolean[];
}

type Titled = Pick<SpanRecord, "kind" | "name" | "node_id" | "peer" | "actor_name" | "actor_type">;

/** What the span's node did, in a few words; the row names the node. */
export function spanTitle(span: Titled): string {
  if (span.kind === "call") return `${span.name} → ${span.peer}`;
  if (span.kind === "stream") return span.peer ? `batch from ${span.peer}` : `publish ${span.name}`;
  return [span.name, span.actor_name || shortType(span.actor_type)].filter(Boolean).join(" · ") || "message";
}

/** The root span as the list's columns: what was done, and to what. The node it started on is `root_node`. */
export function traceParts(t: TraceSummary): { verb: string; target: string } {
  if (t.root_kind === "call") return { verb: t.root, target: t.root_peer };
  if (t.root_kind === "stream") return { verb: t.root_peer ? "stream" : "publish", target: t.root };
  return { verb: t.root || "message", target: t.root_actor_name || shortType(t.root_actor) };
}

/** Yellow-green round to violet, by the golden angle: neighbours differ, and red is left to mean failed. */
export const nodeHue = (index: number) => 60 + Math.round((index * 137.5) % 240);

export interface TraceLayout {
  rows: TraceRow[];
  startUs: number;
  endUs: number;
}

const beganUs = (s: SpanRecord) => s.start_us - s.wait_ns / 1000;
export const endedUs = (s: SpanRecord) => s.start_us + s.busy_ns / 1000;

/** Parents before children, siblings by start. Time in the mailbox counts as part of a span. */
export function layoutTrace(spans: SpanRecord[]): TraceLayout {
  const ids = new Set(spans.map((s) => s.span));
  const children = new Map<string, SpanRecord[]>();
  for (const span of [...spans].sort((a, b) => a.start_us - b.start_us || a.span.localeCompare(b.span))) {
    const under = ids.has(span.parent) && span.parent !== span.span ? span.parent : "";
    children.set(under, [...(children.get(under) ?? []), span]);
  }
  const rows: TraceRow[] = [];
  const seen = new Set<string>();
  const walk = (parent: string, trail: boolean[], cause?: SpanRecord) => {
    const kids = (children.get(parent) ?? []).filter((s) => !seen.has(s.span));
    kids.forEach((span, i) => {
      if (seen.has(span.span)) return;
      seen.add(span.span);
      const guides = parent ? [...trail, i < kids.length - 1] : [];
      rows.push({ span, depth: guides.length, orphan: !parent && span.parent !== "", gapUs: cause ? Math.max(0, beganUs(span) - endedUs(cause)) : 0, cause, guides });
      walk(span.span, guides, span);
    });
  };
  walk("", []);
  // A cycle has no root to be reached from; its spans still belong on the page.
  for (const span of spans) if (!seen.has(span.span)) rows.push({ span, depth: 0, orphan: true, gapUs: 0, guides: [] });
  return {
    rows,
    startUs: Math.min(...spans.map(beganUs)),
    endUs: Math.max(...spans.map(endedUs)),
  };
}

/** Where a span sits on the trace's time axis, as fractions of its width. Clock skew between nodes can push a child before its parent; it is clamped, not hidden. */
export function place(span: SpanRecord, layout: TraceLayout, gapUs = 0): { gap: number; wait: number; left: number; width: number } {
  const total = Math.max(1, layout.endUs - layout.startUs);
  const at = (us: number) => Math.min(1, Math.max(0, (us - layout.startUs) / total));
  return {
    gap: at(beganUs(span) - gapUs),
    wait: at(beganUs(span)),
    left: at(span.start_us),
    width: Math.max(0, at(endedUs(span)) - at(span.start_us)),
  };
}
