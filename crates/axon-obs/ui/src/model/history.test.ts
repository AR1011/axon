import { describe, expect, it } from "vitest";
import type { HistoryAnswer, HistoryResult, Series } from "../api";
import { BUCKETS } from "./histogram";
import { decode, line, loadHistory, merge, WHOLE_EVERY_MS } from "./history";

const series = (node_id: string, values: (number | null)[], kind = "counter"): Series => ({ node_id, incarnation: "1", labels: {}, kind, unit: "", values });
const result = (from: number, count: number, all: Series[], truncated = false, settled = 0): HistoryResult => ({ from, step: 1000, count, series: all, truncated, settled });

describe("history", () => {
  it("moves a histogram's sparse gains apart and keeps the count each gained, refusing one past the buckets", () => {
    const gain = { first: 2, counts: [1, 0, 3], count: 4, sum_ns: 9 };
    const sent = series("a", [null, gain, { first: BUCKETS - 1, counts: [1, 1], count: 2, sum_ns: 1 }] as never, "histogram");
    const [s] = decode(result(0, 3, [sent])).series;
    expect([s.values, s.gains]).toEqual([[null, 4, null], [null, gain, null]]);
    expect(decode(result(0, 1, [series("b", [7])])).series[0]).toEqual(series("b", [7]));
  });

  it("draws a gauge as its value and a gain per second, at each step's start", () => {
    const r = { ...result(5000, 2, []), step: 2000 };
    expect(line(r, series("a", [4, null]), true)).toEqual([[5000, 2], [7000, null]]);
    expect(line(r, series("a", [4, null], "gauge"), false)).toEqual([[5000, 4], [7000, null]]);
    expect(line(undefined, series("a", [4]), false)).toEqual([]);
  });

  it("merges newer steps over older ones by series, and drops a series left with nothing", () => {
    const g = (count: number) => ({ first: 0, counts: [count], count, sum_ns: 0 });
    const older = result(0, 5, [series("a", [1, 2, 3, 4, 5]), series("b", [9, 9, 9, 9, null]), series("gone", [1, 1, null, null, null]), { ...series("h", [1, 1, 1, 1, 1], "histogram"), gains: [g(1), g(1), g(1), g(1), g(1)] }]);
    const newer = result(3000, 4, [series("a", [40, 50, 60, 70]), series("new", [null, null, 5, 6]), { ...series("h", [2, null, 2, 2], "histogram"), gains: [g(2), null, g(2), g(2)] }], true);
    const merged = merge(older, newer, 2000, 5);
    expect([merged.from, merged.count, merged.truncated]).toEqual([2000, 5, true]);
    expect(Object.fromEntries(merged.series.map((s) => [s.node_id, s.values]))).toEqual({
      a: [3, 40, 50, 60, 70],
      b: [9, null, null, null, null],
      h: [1, 2, null, 2, 2],
      new: [null, null, null, 5, 6],
    });
    expect(merged.series.find((s) => s.node_id === "h")!.gains).toEqual([g(1), g(2), null, g(2), g(2)]);
  });

  it("fails, not draws, an answer in the shape a collector from before the columns sends", async () => {
    const old = { source: "s", results: [{ metric: "m", result: { series: [{ node_id: "a", incarnation: "1", metric: "m", labels: {}, kind: "counter", unit: "", points: [{ time_ms: 0, value: 1 }] }], truncated: false } }] };
    await expect(loadHistory(async () => old as unknown as HistoryAnswer, 60_000, 1000, 100_000)).rejects.toThrow();
  });

  it("asks again only from where every answer settled, and in full after a cut answer or an error", async () => {
    const asked: [number, number][] = [];
    const answers: HistoryAnswer[] = [];
    const ask = async (from: number, to: number) => {
      asked.push([from, to]);
      return answers.shift()!;
    };
    const span = 300_000;
    const step = 1000;
    const answer = (from: number, count: number, values: number[], truncated = false, settled = 0): HistoryAnswer => ({ source: "s", results: [{ metric: "m", result: result(from, count, [series("a", values)], truncated, settled) }] });

    const whole = answer(700_000, 300, Array<number>(300).fill(1), false, 950_000);
    whole.results.push({ metric: "k", result: result(700_000, 300, [series("a", [1])], false, 940_000) });
    answers.push(whole);
    const first = await loadHistory(ask, span, step, 1_000_500);
    expect(asked.pop()).toEqual([700_000, 999_999]);
    expect([first.from, first.to, first.covers, first.byMetric.m.count, first.settled]).toEqual([700_000, 999_999, 299_999, 300, 940_000]);

    // Ten seconds on: from the earlier of the two edges the store said its answers settled at.
    answers.push(answer(940_000, 70, Array<number>(70).fill(2), false, 950_000));
    const next = await loadHistory(ask, span, step, 1_010_500, first);
    expect(asked.pop()).toEqual([940_000, 1_009_999]);
    const values = next.byMetric.m.series[0].values;
    expect([next.byMetric.m.from, values.length, values[0], values.at(-71), values.at(-70), values.at(-1)]).toEqual([710_000, 300, 1, 1, 2, 2]);

    answers.push(answer(950_000, 70, [3], true), answer(720_000, 300, [4]));
    const cut = await loadHistory(ask, span, step, 1_020_500, next);
    expect(asked.splice(0)).toEqual([[950_000, 1_019_999], [720_000, 1_019_999]]);
    expect(cut.byMetric.m.series[0].values).toEqual([4]);

    answers.push(answer(720_000, 300, [5]));
    await loadHistory(ask, span, step, 1_020_500, { ...next, errors: ["m: down"] });
    expect(asked.pop()).toEqual([720_000, 1_019_999]);

    answers.push(answer(700_000 + WHOLE_EVERY_MS, 300, [6]));
    await loadHistory(ask, span, step, first.whole + WHOLE_EVERY_MS, next);
    expect(asked.pop()![0], "a merged answer is asked for whole again in time").toBe(700_000 + WHOLE_EVERY_MS);

    answers.push(answer(900_000, 300, [6]));
    await loadHistory(ask, span, step, 1_500_000, next);
    expect(asked.pop()![0], "a previous answer older than the span is no help").toBe(1_200_000);
  });
});
