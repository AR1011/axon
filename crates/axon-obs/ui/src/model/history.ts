import type { HistoryAnswer, HistoryResult, Series, SparseHistogram } from "../api";
import { fits } from "./histogram";
import type { P } from "./series";

const validGain = (g: SparseHistogram | null) => (g && fits(g) && Number.isFinite(g.count) && Number.isFinite(g.sum_ns) ? g : null);

/** As sent, a histogram's `values` are its gains: they move to `gains`, and `values` keeps the count each gained. */
export function decode(result: HistoryResult): HistoryResult {
  for (const s of result.series) {
    if (s.kind !== "histogram") continue;
    s.gains = (s.values as unknown as (SparseHistogram | null)[]).map(validGain);
    s.values = s.gains.map((g) => g?.count ?? null);
  }
  return result;
}

/** What `v` gained over a step of `step` ms, per second. */
export const stepRate = (v: number, step: number) => (v * 1000) / step;

/** A series' line: a gauge's value per step, a counter's and a histogram's gain per second, at each step's start. */
export const line = (r: HistoryResult | undefined, s: Series | undefined, rate: boolean): P[] =>
  !r || !s ? [] : s.values.map((v, i): P => [r.from + i * r.step, v === null ? null : rate ? stepRate(v, r.step) : v]);

/** The line of `metric`'s first series that `pick` takes. */
export function seriesLine(data: HistoryData | undefined, metric: string, pick: (s: Series) => boolean, rate: boolean): P[] {
  const r = data?.byMetric[metric];
  return line(r, r?.series.find(pick), rate);
}

/** The step of a series' last known value, -1 for none. */
export const lastKnown = (s: Series) => s.values.findLastIndex((v) => v !== null);

const identity = (s: Series) => JSON.stringify([s.node_id, s.incarnation, s.labels, s.kind, s.unit]);

/**
 * `count` steps from `from`: `newer`'s from its own start on, `older`'s before it. A series `newer` lacks is unknown
 * over its steps, and one left with nothing known goes.
 */
export function merge(older: HistoryResult, newer: HistoryResult, from: number, count: number): HistoryResult {
  const { step } = newer;
  const pick = <T>(o: Series | undefined, n: Series | undefined, of: (s: Series) => T[] | undefined) =>
    Array.from({ length: count }, (_, i): T | null => {
      const t = from + i * step;
      const [s, r] = t >= newer.from ? [n, newer] : [o, older];
      const k = (t - r.from) / step;
      return (s && k < r.count ? of(s)?.[k] : null) ?? null;
    });
  const both = new Map<string, [Series | undefined, Series | undefined]>();
  for (const s of older.series) both.set(identity(s), [s, undefined]);
  for (const s of newer.series) both.set(identity(s), [both.get(identity(s))?.[0], s]);
  const series = [...both.values()].flatMap(([o, n]) => {
    const { node_id, incarnation, labels, kind, unit, summed } = (n ?? o)!;
    const merged: Series = { node_id, incarnation, labels, kind, unit, summed, values: pick(o, n, (s) => s.values) };
    if (kind === "histogram") merged.gains = pick(o, n, (s) => s.gains);
    return lastKnown(merged) < 0 ? [] : [merged];
  });
  return { from, step, count, series, truncated: older.truncated || newer.truncated, settled: newer.settled };
}

export interface HistoryData {
  byMetric: Record<string, HistoryResult>;
  errors: string[];
  truncated: boolean;
  from: number;
  to: number;
  step: number;
  /** How far back the oldest known step reaches; null when there is none. */
  covers: number | null;
  /** When the whole range was last asked for. */
  whole: number;
  /** The steps before it no longer change, in every answer: a refresh asks again from here. */
  settled: number;
}

/** How long a merged answer is kept before the whole range is asked again, for the rows a store took late, as from a backlog after a failover. */
export const WHOLE_EVERY_MS = 300_000;

/**
 * The steps of `span` that ended by `now`, asked of `ask` in full, or after `previous` only from where its answers
 * settled, the steps a store may still answer otherwise, merged into it. An answer cut short is asked again in full:
 * what it left out cannot be told from what ended; so is one `WHOLE_EVERY_MS` old.
 */
export async function loadHistory(ask: (from: number, to: number) => Promise<HistoryAnswer>, span: number, step: number, now: number, previous?: HistoryData): Promise<HistoryData> {
  // The step still filling would be served as a whole one, and every rate would fall off at the right edge.
  const to = Math.floor(now / step) * step - 1;
  const from = Math.floor((to + 1 - span) / step) * step;
  const count = (to + 1 - from) / step;
  const resumable = previous && previous.step === step && !previous.truncated && !previous.errors.length && now - previous.whole < WHOLE_EVERY_MS;
  const since = resumable ? Math.max(from, previous.settled) : from;
  const answer = await ask(since, to);
  const data: HistoryData = { byMetric: {}, errors: [], truncated: false, from, to, step, covers: null, whole: since > from ? previous!.whole : now, settled: Infinity };
  let oldest = Infinity;
  for (const { metric, result, error } of answer.results) {
    if (error) data.errors.push(`${metric}: ${error}`);
    if (!result) continue;
    const kept = since > from ? previous?.byMetric[metric] : undefined;
    const merged = kept ? merge(kept, decode(result), from, count) : decode(result);
    data.byMetric[metric] = merged;
    data.truncated ||= merged.truncated;
    data.settled = Math.min(data.settled, result.settled);
    for (const s of merged.series) if (lastKnown(s) >= 0) oldest = Math.min(oldest, merged.from + s.values.findIndex((v) => v !== null) * step);
  }
  if (since > from && data.truncated) return loadHistory(ask, span, step, now);
  if (oldest < Infinity) data.covers = to - oldest;
  if (data.settled === Infinity) data.settled = from;
  return data;
}
