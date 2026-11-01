import { describe, expect, it } from "vitest";
import guide from "../../../../axon/guide/telemetry.md?raw";
import type { HistoryResult, Series } from "../api";
import { exploreLines, exploreUnit, findMetrics, labelNames, labelPairs, MAX_LINES, METRICS, parseLabels, typing, withLabel } from "./explore";

/** The names of the guide's table and their kinds, `a.b` · `.c` · `_d` spelled out as `a.b`, `a.c`, `a_d`. */
function persisted(text: string): Record<string, string> {
  const table = text.split("## Persisted samples")[1].split("\n## ")[0];
  const names: Record<string, string> = {};
  for (const row of table.split("\n").filter((l) => l.startsWith("| ") && !l.startsWith("| Name"))) {
    let base = "";
    const kind = row.split("|")[2].trim();
    for (const [, name] of row.split("|")[1].matchAll(/`([^`]+)`/g)) {
      if (/^[a-z]+\.[a-z_.]+$/.test(name)) names[(base = name)] = kind;
      else if (base && /^[._][a-z_.]+$/.test(name)) names[base.slice(0, base.lastIndexOf(name[0])) + name] = kind;
    }
  }
  return names;
}

const series = (node: string, kind: string, labels: Record<string, string>, values: (number | null)[]): Series => ({ node_id: node, incarnation: `i-${node}`, labels, kind, unit: "calls", values });
const result = (all: Series[]): HistoryResult => ({ from: 0, step: 1000, count: 3, truncated: false, settled: 0, series: all });

describe("explore", () => {
  it("offers every name the telemetry guide lists, of its kind, and no other", () => {
    expect(Object.fromEntries(Object.entries(METRICS).map(([name, [kind]]) => [name, kind]))).toEqual(persisted(guide));
  });

  it("finds a metric by words of its name or its line, names that start with the query first", () => {
    expect(findMetrics("")).toHaveLength(Object.keys(METRICS).length);
    expect(findMetrics("actor.turn")[0]).toBe("actor.turn");
    expect(findMetrics("actor.turn")).toContain("actor.turns");
    expect(findMetrics("mailbox room")).toEqual(["actor.waiters"]);
    expect(findMetrics("disk WRITES")).toContain("stream.disk_writes");
    expect(findMetrics("no such thing")).toEqual([]);
  });

  it("charts a counter as its rate and a gauge as its value, added, or split by node or label", () => {
    const calls = [series("a", "counter", { outcome: "ok" }, [null, 10, 10]), series("b", "counter", { outcome: "ok" }, [null, 5, 5]), series("b", "counter", { outcome: "failed" }, [null, 1, 1])];
    expect(exploreLines(result(calls), "").lines).toEqual([{ name: "All series", points: [[0, null], [1000, 16], [2000, 16]] }]);
    expect(exploreLines(result(calls), "node").lines.map((l) => [l.name, l.points.at(-1)![1]])).toEqual([["a", 10], ["b", 6]]);
    expect(exploreLines(result(calls), "outcome").lines.map((l) => l.name)).toEqual(["ok", "failed"]);
    expect(exploreLines(result([series("a", "gauge", {}, [3, 4])]), "").lines[0].points).toEqual([[0, 3], [1000, 4]]);
    expect(exploreLines(result([series("a", "gauge", {}, [1]), series("b", "gauge", { pool: "io" }, [2])]), "pool").lines.map((l) => l.name)).toEqual(["io", "—"]);
    expect(labelNames(calls)).toEqual(["outcome"]);
    expect(labelPairs(calls, {}).map((p) => [p.pair, p.series])).toEqual([["outcome=failed", 1], ["outcome=ok", 2]]);
    expect(labelPairs(calls, { outcome: "ok" }).map((p) => p.pair)).toEqual(["outcome=failed"]);
    const rtt = (node: string, values: number[]) => ({ ...series(node, "gauge", {}, values), unit: "nanoseconds" });
    expect(exploreLines(result([rtt("a", [5, 9]), rtt("b", [7, 2])]), "").lines[0].points).toEqual([[0, 7], [1000, 9]]);
  });

  it("draws the highest lines and counts the rest", () => {
    const many = Array.from({ length: MAX_LINES + 3 }, (_, i) => series(`n${i}`, "gauge", {}, [i, i]));
    const { lines, hidden } = exploreLines(result(many), "node");
    expect([lines.length, hidden, lines[0].name]).toEqual([MAX_LINES, 3, `n${MAX_LINES + 2}`]);
  });

  it("names units and reads labels", () => {
    expect([exploreUnit("counter", "bytes"), exploreUnit("histogram", "nanoseconds"), exploreUnit("gauge", "nanoseconds"), exploreUnit("gauge", "actors")]).toEqual(["bytes/s", "/s", "ns", "count"]);
    expect(parseLabels(" reason = limit, junk, a=b=c ,peer=b")).toEqual({ reason: "limit", a: "b=c", peer: "b" });
    expect(typing("a=b, pe")).toBe("pe");
    expect(withLabel("a=b, pe", "peer=x", true)).toBe("a=b, peer=x");
    expect(withLabel("a=b", "peer=x", false)).toBe("a=b, peer=x");
    expect(withLabel("a=b, peer=x", "a=c", false)).toBe("peer=x, a=c");
  });
});
