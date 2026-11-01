import { HANDSHAKE_FAILURES, INBOUND_REJECTIONS } from "../api";
import { GAP_MS, STALE_MS } from "./live";

/** `[time_ms, value]`; a null value is a visible break in the line. */
export type P = [number, number | null];
export interface Raw {
  t: number;
  v: number | null | undefined;
}

export const MAX_RAW = 2000;
export const MAX_PLOTTED = 180;

/** Sorted ascending, identical timestamps collapsed to the last one seen. */
export function clean(raw: Raw[]): Raw[] {
  const byTime = new Map<number, Raw>();
  for (const point of raw.slice(-MAX_RAW)) if (Number.isFinite(point.t)) byTime.set(point.t, point);
  return [...byTime.values()].sort((a, b) => a.t - b.t);
}

/** Keeps one point before the window so the first visible rate has a real baseline. */
function bound(points: Raw[]): Raw[] {
  return points.length > MAX_PLOTTED + 1 ? points.slice(-(MAX_PLOTTED + 1)) : points;
}

/**
 * Cumulative counter to per-second rate. A reset, a missing or non-finite value, or an interval
 * longer than `gapMs` gives null and an explicit break, never zero.
 */
export function ratePoints(raw: Raw[], gapMs: number): P[] {
  const points = bound(clean(raw));
  const out: P[] = [];
  for (let i = 1; i < points.length; i++) {
    const [before, now] = [points[i - 1], points[i]];
    const ok =
      Number.isFinite(before.v) && Number.isFinite(now.v) && now.v! >= before.v! && now.t - before.t <= gapMs;
    if (!ok && out.length && out.at(-1)![1] !== null) out.push([before.t + 1, null]);
    out.push([now.t, ok ? ((now.v! - before.v!) * 1000) / (now.t - before.t) : null]);
  }
  return out.slice(-MAX_PLOTTED);
}

export function gaugePoints(raw: Raw[], gapMs: number): P[] {
  const points = bound(clean(raw));
  const out: P[] = [];
  points.forEach((point, i) => {
    if (i > 0 && point.t - points[i - 1].t > gapMs && out.at(-1)?.[1] !== null) out.push([points[i - 1].t + 1, null]);
    out.push([point.t, Number.isFinite(point.v) ? point.v! : null]);
  });
  return out.slice(-MAX_PLOTTED);
}

/** The gap at which `joinPoints` breaks a line: two and a half retained steps, so one step missing or late still joins. */
export const seamGap = (step = 0, floor = GAP_MS) => Math.max(floor, step * 2.5);

/** Retained then live, never fused: retained points inside the live window go, and the seam breaks only across a gap. */
export function joinPoints(retained: P[], live: P[], gapMs: number): P[] {
  if (!live.length) return retained;
  const start = live[0][0];
  const kept = retained.filter(([t]) => t < start);
  const last = kept.at(-1);
  const seam: P[] = last && start - last[0] > gapMs ? [[last[0] + 1, null]] : [];
  return [...kept, ...seam, ...live].slice(-MAX_PLOTTED * 2);
}

export const latest = (points: P[]): number | null => points.findLast(([, v]) => v !== null)?.[1] ?? null;

/** The arrays telemetry indexes by a reason, with their names. */
const BY_REASON: Record<string, string[]> = { "cluster.engine.inbound_rejected": INBOUND_REJECTIONS, "cluster.engine.listener.handshake_failures": HANDSHAKE_FAILURES };

/** Every number under `value` by its dotted path; an array by reason under each reason's name, any other array left out. */
export function flatten(prefix: string, value: unknown): [string, number][] {
  if (typeof value === "number") return [[prefix, value]];
  if (Array.isArray(value)) return BY_REASON[prefix] ? value.map((n, i): [string, number] => [`${prefix}.${BY_REASON[prefix][i] ?? i}`, n]) : [];
  return value && typeof value === "object" ? Object.entries(value).flatMap(([k, v]) => flatten(`${prefix}.${k}`, v)) : [];
}

interface Sample {
  epoch: string;
  t: number;
  values: Map<string, number>;
}
/** The last two snapshots seen per node, so a cumulative value can also be read as a rate. A new `epoch` (a restart) starts over. */
const seen = new Map<string, [Sample | undefined, Sample]>();

export function rates(node: string, epoch: string, t: number, values: Map<string, number>): Map<string, number> {
  let [before, last] = (seen.get(node) ?? []).map((s) => (s?.epoch === epoch ? s : undefined));
  // Further back than a snapshot can be and still be fresh is the node's clock stepping back, not a late poll: start over.
  if (last && last.t - t > STALE_MS) before = last = undefined;
  if (!last || t > last.t) seen.set(node, [last, { epoch, t, values }]);
  // One no newer than the last (a re-render, or a poll the stream overtook) answers with the newest rate.
  const [from, to] = last && t <= last.t ? [before, last] : [last, { t, values }];
  const out = new Map<string, number>();
  if (from && to.t > from.t) for (const [name, value] of to.values) if (from.values.has(name)) out.set(name, ((value - from.values.get(name)!) * 1000) / (to.t - from.t));
  return out;
}

/**
 * A chart's step. From a minute up it is whole minutes, from an hour up whole hours, which a store answers from its rollups instead of raw samples.
 * Under a minute it is whole seconds, so every step holds as many 1 Hz samples as the next and a rate does not saw.
 */
export function historyStep(span: number, points: number): number {
  const step = Math.max(1000, Math.ceil(span / points));
  const unit = step >= 3_600_000 ? 3_600_000 : step >= 60_000 ? 60_000 : 1000;
  return Math.ceil(step / unit) * unit;
}
