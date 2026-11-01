import { describe, expect, it } from "vitest";
import { HANDSHAKE_FAILURES, INBOUND_REJECTIONS, type ClusterEvent, type Histogram, type NodeView, type Overview, type HistoryResult, type Lane, type Peer, type Series, type SpanRecord, type StreamStats } from "../api";
import { heatOption, histogramOption, lanesOption, lineOption } from "../charts/options";
import { BUCKETS, boundNs, boundText, gained, handlerTime, heat, intervals, quantile, quantileLines, expand, rowHeat, validHistogram, windowHistogram } from "./histogram";
import { deadLetters, pending } from "../store";
import { attention } from "./attention";
import { aggregate } from "./kpi";
import { busiest, graphLinks } from "./links";
import { ActorTrends, ConnSeries, connectionKey, departures, growth, NodeBuffer, nodeRow, type NodeRow } from "./live";
import { reasons } from "./format";
import { flatten, historyStep, joinPoints, ratePoints, rates } from "./series";
import { flowEdges, flowLayout, pushLine } from "./streamGraph";
import { CARD, fraction, layout, LAYOUTS } from "./topology";
import { layoutTrace, nodeHue, place, spanTitle, traceParts } from "./trace";

function peer(node: string, outgoing: boolean, tx: number, rx = 0, lane: Peer["lane"] = "control"): Peer {
  const session = { tx_wire_bytes: tx, rx_wire_bytes: rx, tx_frames: tx / 100, rx_frames: 0, tx_payload_bytes: tx, rx_payload_bytes: 0, rtt_ns: 4e6, outbound_reserved_bytes: 0 };
  return { node, incarnation: `i-${node}`, outgoing, lane, down_ms: null, pending: 0, calls: 0, session } as unknown as Peer;
}

function view(id: string, t: number, peers: Peer[], incarnation = `i-${id}`): NodeView {
  const snapshot = { node_id: id, incarnation, timestamp_ms: t, actor_total: 1, actor_types: [], worker_utilisation: [], system: { dead_letters: 0 }, cluster: { engine: { peers, calls: 0, listener: { accepted: 0, accept_errors: 0, rejected_busy: 0, rejected_per_ip: 0, handshake_failures: [] } } } };
  return { node: { id, incarnation, address: "", metadata: {} }, reachable: true, error: null, snapshot } as unknown as NodeView;
}

const overview = (nodes: NodeView[]) => ({ nodes }) as Overview;

describe("rates", () => {
  it("deduplicate, sort, and break on gaps and resets", () => {
    const raw = [
      { t: 3000, v: 300 },
      { t: 1000, v: 100 },
      { t: 2000, v: 200 },
      { t: 2000, v: 250 },
      { t: 4000, v: 50 },
      { t: 5000, v: 150 },
      { t: 30000, v: 900 },
      { t: 31000, v: NaN },
    ];
    expect(ratePoints(raw, 5000)).toEqual([
      [2000, 150],
      [3000, 50],
      [3001, null],
      [4000, null],
      [5000, 100],
      [5001, null],
      [30000, null],
      [31000, null],
    ]);
  });

  it("bound output while keeping a real baseline", () => {
    const raw = Array.from({ length: 3000 }, (_, i) => ({ t: i * 1000, v: i * 10 }));
    const points = ratePoints(raw, 5000);
    expect(points).toHaveLength(180);
    expect(points[0][1]).toBe(10);
  });

  it("join retained and live without fusing them or a gap at the seam", () => {
    const joined = joinPoints([[1000, 1], [2000, 2], [3000, 3]], [[2500, 9], [3500, 8]], 5000);
    expect(joined).toEqual([[1000, 1], [2000, 2], [2500, 9], [3500, 8]]);
    expect(joinPoints([[0, 1]], [[600_000, 2]], 5000), "a stretch no one observed stays a break").toEqual([[0, 1], [1, null], [600_000, 2]]);
  });
});

describe("connection samples", () => {
  it("stay bounded, ignore duplicates, and detect tx and rx resets independently", () => {
    const series = new ConnSeries();
    const key = connectionKey("a", "i-a", peer("b", true, 0));
    for (let i = 0; i < 400; i++) series.append(overview([view("a", i * 1000, [peer("b", true, i * 1000, i * 10)])]));
    series.append(overview([view("a", 399_000, [peer("b", true, 9e9)])]));
    expect(series.samples(key)).toHaveLength(180);
    expect(series.samples(key).at(-1)).toMatchObject({ tx: 1000, rx: 10 });
    series.append(overview([view("a", 400_000, [peer("b", true, 500_000, 1)])]));
    expect(series.samples(key).at(-1)).toMatchObject({ tx: 101_000, rx: null });
    series.append(overview([view("a", 430_000, [peer("b", true, 600_000, 2)])]));
    expect(series.samples(key).at(-1)).toMatchObject({ tx: null, rx: null });
    series.append(overview([view("a", 1000, [peer("b", true, 0)])]));
    expect(series.samples(key)).toHaveLength(0);
    series.append(overview([view("a", 2000, [])]));
    expect(series.samples(key)).toHaveLength(0);
  });

  it("refuse connections past the cap", () => {
    const series = new ConnSeries();
    const peers = Array.from({ length: 250 }, (_, i) => peer(`p${i}`, true, 0));
    series.append(overview([view("a", 1000, peers)]));
    series.append(overview([view("a", 2000, peers)]));
    expect(peers.filter((p) => series.samples(connectionKey("a", "i-a", p)).length).length).toBe(200);
  });
});

describe("node and actor buffers", () => {
  it("reset on a new incarnation or a clock rollback", () => {
    const buffer = new NodeBuffer();
    for (let i = 0; i < 700; i++) buffer.append(overview([view("a", i * 1000, [])]));
    expect(buffer.rows("a")).toHaveLength(600);
    expect(buffer.rows("a", "other")).toHaveLength(0);
    buffer.append(overview([view("a", 800_000, [], "i-2")]));
    expect(buffer.rows("a")).toHaveLength(1);
    expect([buffer.earlier("a")?.incarnation, buffer.rows("a", "i-a").length]).toEqual(["i-a", 600]);
    buffer.append(overview([view("a", 1000, [], "i-2")]));
    expect(buffer.rows("a")).toHaveLength(1);
  });

  it("cannot bridge a restart, gap or rollback", () => {
    const trends = new ActorTrends();
    const actor = (slot: number, processed: number) =>
      ({ node_id: "a", node_incarnation: "i", incarnation: { runtime: 1, slot, generation: 1 }, diagnostics: { mailbox_depth: 1, processed } }) as never;
    for (let i = 0; i < 100; i++) trends.append([actor(1, i)], i * 1000);
    expect(trends.points(actor(1, 0))).toHaveLength(60);
    trends.append([actor(1, 5)], 101_000);
    expect(trends.points(actor(1, 0)).at(-2)).toMatchObject({ processed: null });
    trends.append([actor(1, 6)], 200_000);
    expect(trends.points(actor(1, 0)).at(-2)).toMatchObject({ depth: null });
    trends.append([actor(1, 7)], 1000);
    expect(trends.points(actor(1, 0))).toHaveLength(1);
    trends.append(Array.from({ length: 200 }, (_, slot) => actor(slot + 10, 1)), 2000);
    expect(trends.points(actor(1, 0))).toHaveLength(0);
  });
});

describe("graph links", () => {
  const nodes = (t: number) => [
    view("a", t, [peer("b", true, t), peer("c", true, t), peer("ghost", true, t)]),
    view("b", t, [peer("a", false, t * 2)]),
    view("c", t, [peer("a", false, t), peer("a", true, t)]),
  ];

  it("deduplicate both ends of one session, in either order, and never invent peers", () => {
    const series = new ConnSeries();
    for (const t of [1000, 2000]) series.append(overview(nodes(t)));
    const forward = graphLinks(nodes(2000), series, 2500);
    const backward = graphLinks([...nodes(2000)].reverse(), series, 2500);
    expect(forward).toEqual(backward);
    expect(forward.map((l) => `${l.source}>${l.target}`)).toEqual(["a>b", "a>c", "c>a"]);
    expect(forward[0].rate).toBe(1000);
    expect(forward.every((l) => l.width >= 1 && l.width <= 8)).toBe(true);
    expect(graphLinks(nodes(2000), series, 60_000).every((l) => l.stale && l.rate === null)).toBe(true);
  });

  it("keep the sessions that carried the most over a limit, whatever the names", () => {
    const views = [view("a", 2000, [peer("b", true, 10), peer("c", true, 500)]), view("b", 2000, []), view("c", 2000, [peer("b", true, 90)])];
    const links = graphLinks(views, new ConnSeries(), 2500);
    const names = (kept: typeof links) => kept.map((l) => `${l.source}>${l.target}`);
    expect(names(links)).toEqual(["a>b", "a>c", "c>b"]);
    expect(names(busiest(links, 2))).toEqual(["a>c", "c>b"]);
    expect(busiest(links, 3)).toBe(links);
  });

  it("sum the lanes of one pair into one link, and leave a down lane out", () => {
    const down = { ...peer("b", true, 0, 0, "bulk"), down_ms: 5000 };
    const at = (t: number) => [view("a", t, [peer("b", true, t, 0, "control"), peer("b", true, 4 * t, 0, "live"), down]), view("b", t, [peer("a", false, t, 0, "control"), peer("a", false, 4 * t, 0, "live")])];
    const series = new ConnSeries();
    for (const t of [1000, 2000]) series.append(overview(at(t)));
    expect(series.latest(connectionKey("a", "i-a", down), 2500), "a down lane has no series").toBeNull();
    const links = graphLinks(at(2000), series, 2500);
    expect(links.map((l) => [l.id, l.lanes.map((p) => p.lane)])).toEqual([["a@i-a>b@i-b", ["control", "live"]]]);
    expect(links[0]).toMatchObject({ rate: 5000, frames: 50, frameBytes: 100, rtt: 4e6, carried: 10_000 });
  });
});

describe("layouts", () => {
  const ids = Array.from({ length: 23 }, (_, i) => `node-${i}`);
  const edges = ids.slice(1).map((id, i) => [ids[Math.floor(i / 3)], id] as [string, string]);
  const roles = Object.fromEntries(ids.map((id, i) => [id, ["relay", "publisher", ""][i % 3]]));

  it.each(LAYOUTS)("%s is deterministic, order independent, bounded and free of overlaps", (mode) => {
    const a = layout(ids, edges, mode, roles);
    const b = layout([...ids].reverse(), [...edges].reverse().map(([x, y]) => [y, x]), mode, roles);
    expect([...a.positions]).toEqual([...b.positions]);
    const boxes = [...a.positions.values()];
    expect(boxes).toHaveLength(ids.length);
    for (const box of boxes) {
      expect(box.x >= 0 && box.y >= 0 && box.x + CARD.w <= a.width && box.y + CARD.h <= a.height).toBe(true);
    }
    for (let i = 0; i < boxes.length; i++)
      for (let j = i + 1; j < boxes.length; j++)
        expect(Math.abs(boxes[i].x - boxes[j].x) >= CARD.w || Math.abs(boxes[i].y - boxes[j].y) >= CARD.h).toBe(true);
  });

  it("scales keep unknown apart from zero", () => {
    expect(fraction(null, 10, false)).toBeNull();
    expect(fraction(0, 0, false)).toBe(0);
    expect(fraction(5, 100, true)!).toBeGreaterThan(fraction(5, 100, false)!);
    expect(fraction(500, 100, false)).toBe(1);
  });
});

describe("histograms", () => {
  const pad = (buckets: number[]) => [...buckets, ...Array<number>(BUCKETS - buckets.length).fill(0)];
  const h = (buckets: number[], sum_ns = 0): Histogram => ({ buckets: pad(buckets), count: buckets.reduce((a, b) => a + b, 0), sum_ns });

  it("share axon's bucket layout", () => {
    // Pinned the same in crates/axon/src/histogram.rs.
    expect([0, 7, 15, 16, 17, 100, 302, 303].map(boundNs)).toEqual([0, 7, 15, 17, 19, 26_623, 1_030_792_151_039, Infinity]);
  });

  it("report quantiles as bucket upper bounds", () => {
    const tail = h([...Array<number>(100).fill(0), 90, 9, ...Array<number>(201).fill(0), 1]);
    expect([0.5, 0.95, 1].map((p) => quantile(tail, p))).toEqual([26_623, 28_671, Infinity]);
    expect(quantile(h([0, 0, 0]), 0.5)).toBeNull();
  });

  it("take only complete intervals inside the window and exclude resets", () => {
    const points = [
      { t: 0, h: h([100, 0], 5) },
      { t: 1000, h: h([110, 0], 10) },
      { t: 2000, h: h([130, 5], 20) },
      { t: 3000, h: h([2, 0], 1) },
      { t: 4000, h: h([6, 0], 3) },
    ];
    expect(windowHistogram(intervals(points), 500, 5000)).toEqual({ buckets: pad([24, 5]), count: 29, sum_ns: 12 });
    expect(windowHistogram(intervals(points), 3500, 3900)).toBeNull();
    const unmatched = [{ t: 0, h: { buckets: pad([1]), count: 1, sum_ns: 1 } }, { t: 1000, h: { buckets: pad([3]), count: 9, sum_ns: 9 } }];
    expect(windowHistogram(intervals(unmatched), 0, 1000)!.sum_ns).toBeNaN();
  });

  it("add up runs: each series resets on its own, and their rates and quantiles combine", () => {
    const a = intervals([{ t: 0, h: h([0, 0]) }, { t: 1000, h: h([4, 0]) }, { t: 2000, h: h([1, 0]) }]);
    const b = intervals([{ t: 0, h: h([0, 0]) }, { t: 1000, h: h([0, 6]) }, { t: 2000, h: h([0, 8]) }]);
    expect(windowHistogram([...a, ...b], 0, 2000)).toEqual({ buckets: pad([4, 8]), count: 12, sum_ns: 0 });
    const grid = heat([a, b], 0, 1000, 1);
    expect([grid.cells.filter(([, , rate]) => rate > 0), grid.max]).toEqual([[[0, 0, 4], [0, 1, 6]], 6]);
    expect(heat([a, b], -60_000, 2000).columns[0]).toBe(0);
    expect(quantileLines([...a, ...b], [0.5, 1], 0, 2000)).toEqual([
      { name: "p50", points: [[1000, boundNs(1)], [2000, boundNs(1)]] },
      { name: "p100", points: [[1000, boundNs(1)], [2000, boundNs(1)]] },
    ]);
  });

  it("leave unobserved heat cells blank", () => {
    const grid = heat([intervals([{ t: 0, h: h([0, 0]) }, { t: 2000, h: h([4, 0]) }, { t: 8000, h: h([1, 0]) }, { t: 10_000, h: h([3, 0]) }])], 0, 10_000);
    const observed = new Set(grid.cells.map(([column]) => column));
    expect([...observed].sort((a, b) => a - b)).toEqual([0, 1, 8, 9]);
    expect(grid.max).toBe(2);
  });

  it("grid named rows over time, each cell the highest value in its column", () => {
    const rows = [
      { name: "a · Relay", points: [[0, 1], [400, 5], [1500, 2], [1600, 1], [9000, 3]] as [number, number][] },
      { name: "b · Relay", points: [[-1, 9], [2500, 4], [10_000, 7]] as [number, number][] },
    ];
    const grid = rowHeat(rows, 0, 10_000);
    expect([grid.columns.length, grid.buckets, grid.rows]).toEqual([10, 2, ["a · Relay", "b · Relay"]]);
    expect(grid.cells.sort((x, y) => x[0] - y[0] || x[1] - y[1])).toEqual([[0, 0, 5], [1, 0, 2], [2, 1, 4], [9, 0, 3]]);
    expect(grid.max).toBe(5);
    const option = heatOption(grid, "dark");
    expect([option.yAxis.data, option.tooltip.formatter({ value: [2, 1, 4] }).split("\n").at(0)]).toEqual([["a · Relay", "b · Relay"], "b · Relay"]);
    expect(rowHeat([], 0, 1000).cells).toEqual([]);
  });

  it("escape what the cluster names in a tooltip, which is HTML", () => {
    const name = `<img src=x onerror="alert(1)">`;
    const lanes = lanesOption([{ time_ms: 0, lane: name, text: name, tone: "muted", kind: "k" }], [name], [0, 1000], "dark");
    const lines = lineOption([{ name, points: [[0, 1], [1000, 2]] }], "count", [0, 1000], "dark");
    const shown = [lanes.tooltip.formatter({ data: { value: [0, name], text: name } }), lines.tooltip.formatter([{ axisValue: 0, marker: "", seriesName: name, value: [0, 1] }])];
    for (const text of shown) expect(text).not.toContain("<img");
    expect(shown[0]).toContain("&#60;img");
  });

  it("add each message label's handler time over the nodes, largest first, busiest type first", () => {
    const at = (sum_ns: number) => ({ buckets: [], count: 1, sum_ns });
    const type = (actor_type: string, messages: { label: string; handler: Histogram }[]) => ({ actor_type, totals: { timings: { messages } } });
    const views = [
      { snapshot: { actor_types: [type("app::Idle", [{ label: "Ping", handler: at(1) }]), type("app::Relay", [{ label: "Tick", handler: at(20) }, { label: "Call", handler: at(5) }])] } },
      { snapshot: { actor_types: [type("app::Relay", [{ label: "Call", handler: at(30) }]), { actor_type: "app::Other", totals: { timings: null } }] } },
      { snapshot: null },
    ] as unknown as NodeView[];
    const times = handlerTime(views);
    expect([...times.keys()]).toEqual(["app::Relay", "app::Idle"]);
    expect(times.get("app::Relay")).toEqual([{ label: "Call", ns: 35 }, { label: "Tick", ns: 20 }]);
  });

  it("draw only the buckets around what was observed", () => {
    const at = (bucket: number, n: number) => h([...Array<number>(bucket).fill(0), n]);
    const grid = heat([intervals([{ t: 0, h: at(150, 0) }, { t: 1000, h: at(150, 5) }])], 0, 1000);
    expect([grid.first, grid.buckets, grid.cells.find(([, , rate]) => rate > 0)]).toEqual([148, 8, [0, 2, 5]]);
    const label = (i: number) => (Number.isFinite(boundNs(i)) ? boundText(boundNs(i)) : "∞");
    expect(heatOption(grid, "dark").yAxis.data.slice(0, 3)).toEqual([148, 149, 150].map(label));
    const bars = histogramOption(h([...Array<number>(100).fill(0), 7, ...Array<number>(9).fill(0), 1]), "dark");
    expect([bars.xAxis.data[0], bars.xAxis.data[2], bars.series[0].data[2], bars.series[0].data.length]).toEqual([label(98), label(100), 7, 15]);
    expect(boundText(quantile(at(BUCKETS - 1, 1), 1))).toBe("> 1031 s");
  });

  it("read history's sparse gains in place, each over the step before its own", () => {
    const r: HistoryResult = { from: 10_000, step: 1000, count: 3, truncated: false, settled: 0, series: [] };
    const s = { gains: [null, { first: 1, counts: [4, 0, 2], count: 6, sum_ns: 60 }, { first: 0, counts: [1], count: 1, sum_ns: 5 }] } as Series;
    const spans = gained(r, s);
    expect(spans.map((i) => [i.from, i.to])).toEqual([[10_000, 11_000], [11_000, 12_000]]);
    expect(windowHistogram(spans, 10_000, 12_000)).toEqual({ buckets: pad([1, 4, 0, 2]), count: 7, sum_ns: 65 });
    expect(windowHistogram(spans, 11_000, 12_000)).toEqual({ buckets: pad([1]), count: 1, sum_ns: 5 });
    expect(quantileLines(spans, [1], 0, 20_000)[0].points).toEqual([[11_000, boundNs(3)], [12_000, boundNs(0)]]);
    expect(heat([spans], 10_000, 12_000, 2).cells.filter(([, , rate]) => rate > 0)).toEqual([[0, 1, 4], [0, 3, 2], [1, 0, 1]]);
  });

  it("expand the sparse histograms axon sends, and refuse any other length", () => {
    const sparse = { first: 2, counts: [1, 0, 3], count: 4, sum_ns: 9 };
    const a = expand(sparse);
    expect([validHistogram(a), a.buckets.slice(0, 6), a.buckets.length, a.count, a.sum_ns]).toEqual([true, [0, 0, 1, 0, 3, 0], BUCKETS, 4, 9]);
    expect(validHistogram(expand({ first: 303, counts: [1, 1], count: 2, sum_ns: 1 }))).toBe(false);
    expect(validHistogram({ buckets: Array<number>(32).fill(0), count: 0, sum_ns: 0 })).toBe(false);
    // Read sparse, a quantile lands in the same bucket as read expanded.
    for (const p of [0.25, 0.5, 0.95, 1]) expect(quantile(sparse, p)).toBe(quantile(a, p));
    expect(quantile(sparse, 0.25)).toBe(boundNs(2));
    expect(quantile({ first: 9, counts: [], count: 0, sum_ns: 0 }, 0.5)).toBeNull();
  });
});

describe("cluster aggregation", () => {
  const series = (node_id: string, incarnation: string, values: (number | null)[]): Series => ({ node_id, incarnation, labels: {}, kind: "counter", unit: "", values });
  const result = (all: Series[]): HistoryResult => ({ from: 1000, step: 1000, count: 9, truncated: false, settled: 0, series: all });

  it("counts nodes, adds what is known per step, and rates a gain over its step", () => {
    const all = [series("a", "1", [10, 10, 10, null, null]), series("a", "2", [null, null, null, 5, 10]), series("b", "1", [1, 1, null, null, null, null, null, null, 3])];
    expect(aggregate(result(all), all, "count").map(([, v]) => v)).toEqual([2, 2, 1, 1, 1, null, null, null, 1]);
    expect(aggregate(result(all), all, "sum").slice(0, 2)).toEqual([[1000, 11], [2000, 11]]);
    expect(aggregate({ ...result(all), step: 2000 }, all, "rate").slice(3, 6)).toEqual([[7000, 2.5], [9000, 5], [11_000, null]]);
    expect(aggregate(result(all), all, "max").at(-1)).toEqual([9000, 3]);
  });

  it("ends where the slowest current node has reported, so a step still filling is no drop", () => {
    const all = [series("a", "1", [10, 20, 30, 40]), series("b", "1", [1, 2, 3]), series("gone", "1", [7]), series("b", "0", [3])];
    expect(aggregate(result(all), all, "count").at(-1)).toEqual([3000, 2]);
    expect(aggregate(result(all), all, "sum").at(-1)).toEqual([3000, 33]);
    expect(aggregate(result(all), all, "rate").at(-1)).toEqual([3000, 33]);
    expect(aggregate(result([]), [], "sum")).toEqual([]);
  });

  it("reads a counter as a rate between the last two snapshots, and survives a re-render", () => {
    const at = (n: number) => new Map([["sent", n]]);
    expect(rates("n", "1", 1000, at(10)).size).toBe(0);
    expect(rates("n", "1", 3000, at(50)).get("sent")).toBe(20);
    expect(rates("n", "1", 3000, at(50)).get("sent")).toBe(20);
    expect(rates("n", "1", 4000, at(50)).get("sent")).toBe(0);
    expect(rates("n", "2", 5000, at(5)).size, "a restart is not a negative rate").toBe(0);
  });

  it("gives an observation that arrives late a rate, and keeps the newer pair", () => {
    const at = (n: number) => new Map([["sent", n]]);
    rates("late", "1", 1000, at(10));
    rates("late", "1", 2000, at(20));
    rates("late", "1", 3000, at(40));
    expect(rates("late", "1", 2000, at(20)).get("sent"), "a poll older than the stream's last frame answers with the newest rate").toBe(20);
    expect(rates("late", "1", 4000, at(70)).get("sent")).toBe(30);
  });

  it("starts over when a node's clock steps back further than a late poll", () => {
    const at = (n: number) => new Map([["sent", n]]);
    rates("step", "1", 60_000, at(0));
    rates("step", "1", 61_000, at(100));
    expect(rates("step", "1", 1000, at(100)).size, "no rate from before the step").toBe(0);
    expect(rates("step", "1", 2000, at(100)).get("sent")).toBe(0);
  });
});

describe("stream flow graph", () => {
  const stats = (over: Partial<StreamStats>): StreamStats => ({ originated: 0, feeds: 0, orphaned: 0, parents: [], downstream: [], ...over }) as StreamStats;
  const child = (node: string, pushed_bytes: number, lane: Lane = "live") => ({ node, lane, branches: 1, queued_bytes: 7, pushed_bytes, pushed_records: 1, channel: 1, window_bytes: 2 << 20, inflight_bytes: 1 << 20, inflight_pushes: 2, round_trip_us: 12_000, delivery_rate: 3 << 20, resent: 1 });
  const nodes = [
    { id: "o", stats: stats({ originated: 1, downstream: [child("r1", 100), child("r2", 200)] }) },
    { id: "r1", stats: stats({ feeds: 1, parents: [{ node: "o", keys: 1, joining: 0 }], downstream: [child("l1", 50)] }) },
    { id: "r2", stats: stats({ feeds: 1, parents: [{ node: "o", keys: 1, joining: 0 }] }) },
    { id: "l1", stats: stats({ feeds: 1, parents: [{ node: "r1", keys: 1, joining: 0 }, { node: "r2", keys: 0, joining: 1 }] }) },
    { id: "idle", stats: stats({}) },
  ];

  it("joins what the child and the parent each report into one edge", () => {
    const edges = flowEdges(nodes);
    expect(edges.map((e) => e.id)).toEqual(["o>r1", "o>r2", "r1>l1", "r2>l1"]);
    expect(edges.find((e) => e.id === "r1>l1")).toMatchObject({ streams: 1, joining: false, pushedBytes: 50, queued: 7 });
    expect(edges.find((e) => e.id === "r2>l1")).toMatchObject({ joining: true, pushedBytes: null, push: null });
    expect(pushLine(edges.find((e) => e.id === "o>r1")!.push!)).toBe("In flight 1.0 MiB · Window 2.0 MiB · Round trip 12.0 ms · Rate 3.0 MiB/s · Resent 1");
  });

  it("adds up a child's lanes on its edge", () => {
    const lanes = [{ ...child("c", 100), queued_bytes: 3 << 20 }, { ...child("c", 50, "bulk"), queued_bytes: 0 }];
    const edges = flowEdges([{ id: "o", stats: stats({ originated: 2, downstream: lanes }) }, { id: "c", stats: stats({}) }]);
    expect(edges[0]).toMatchObject({ id: "o>c", streams: 2, queued: 3 << 20, pushedBytes: 150, pushedRecords: 2 });
    expect(pushLine(edges[0].push!)).toBe("In flight 2.0 MiB · Window 4.0 MiB · Round trip 12.0 ms · Rate 6.0 MiB/s · Resent 2");
  });

  it("puts origins on top, a child under its parent, and leaves idle nodes out", () => {
    const edges = flowEdges(nodes);
    const graph = flowLayout(nodes, edges);
    expect(graph.layers).toEqual([["o"], ["r1", "r2"], ["l1"]]);
    expect(graph.positions.has("idle")).toBe(false);
    expect(graph.positions.get("l1")!.y).toBeGreaterThan(graph.positions.get("r1")!.y);
  });
});

it("a history step is whole minutes from a minute up and whole hours from an hour up", () => {
  expect(historyStep(900_000, 90)).toBe(10_000);
  expect(historyStep(3_600_000, 144)).toBe(25_000);
  expect(historyStep(21_600_000, 144)).toBe(180_000);
  expect(historyStep(7 * 86_400_000, 144)).toBe(7_200_000);
  expect(historyStep(45 * 86_400_000, 144)).toBe(8 * 3_600_000);
  expect(historyStep(10_000_000, 144)).toBe(120_000);
});

describe("shared live readings", () => {
  it("say Connecting or the live error until the first overview, never an empty cluster", () => {
    expect(pending({ overview: null, error: null })).toBe("Connecting");
    expect(pending({ overview: null, error: "Console server unreachable" })).toBe("Console server unreachable");
    expect(pending({ overview: overview([]), error: "Console server unreachable" })).toBeNull();
  });

  it("count dead letters one way for the sidebar and the Dead Letters page", () => {
    const [a, b] = [view("a", 0, []), view("b", 0, [])];
    a.snapshot!.system.dead_letters = 9104;
    b.snapshot!.system.dead_letters = 6;
    expect(deadLetters({ overview: overview([a, b, { ...b, snapshot: null }]) })).toBe(9110);
    expect(deadLetters({ overview: null })).toBe(0);
  });
});

describe("attention", () => {
  const listening = (id: string, t: number, refused: number): NodeView => {
    const v = view(id, t, []);
    Object.assign(v.snapshot!, { lifecycle: { phase: "running" }, notices: [] });
    Object.assign(v.snapshot!.cluster, { isolated: false });
    v.snapshot!.cluster.engine.listener.rejected_busy = refused;
    return v;
  };
  const watched = (nodes: NodeView[], events: ClusterEvent[] = []) => ({ observer: "a", nodes, events, history: { sources: [], error: null }, discovery_error: null }) as unknown as Overview;
  const titles = (concerns: { title: string }[]) => concerns.map((c) => c.title);

  it("names a listener refusing peers while the refusals grow, and forgets it a minute after they stop", () => {
    const buffer = new NodeBuffer();
    let last = watched([]);
    for (let s = 0; s <= 100; s++) buffer.append((last = watched([listening("a", s * 1000, Math.min(s, 10))])));
    expect(titles(attention(last, [], 100_000, null, buffer))).toEqual([]);
    const busy = new NodeBuffer();
    for (let s = 0; s <= 10; s++) busy.append((last = watched([listening("a", s * 1000, s)])));
    expect(attention(last, [], 10_000, null, busy)).toMatchObject([{ title: "Peers refused at the listener", detail: expect.stringMatching(/^10 in the last 10s · since 9s ago or earlier ·/) }]);
  });

  it("names each counter that grew in the last minute with since when, and a failing disk", () => {
    const buffer = new NodeBuffer();
    let last = watched([]);
    for (let s = 0; s <= 120; s++) {
      const v = listening("a", s * 1000, 0);
      // Dead letters grew from 70 s on, after a quiet minute, through a 15 s lull that does not end the burst.
      v.snapshot!.system.dead_letters = s < 70 ? 0 : s < 85 ? s - 69 : s < 100 ? 15 : s - 84;
      Object.assign(v.snapshot!, { streams: { orphaned: 0, hub: { recovering: 0 }, disk: s >= 100 ? { failing: "Full", error: "no space" } : null } });
      buffer.append((last = watched([v])));
    }
    expect(attention(last, [], 120_000, null, buffer)).toEqual([
      { tone: "danger", node: "a", title: "Stream disk failure", detail: "since 20s ago · Full: no space · the node refuses publishes to the streams it keeps on disk", to: "/streams" },
      { tone: "warning", node: "a", title: "Dead letters", detail: "36 in the last minute · since 50s ago · 36 since start, dead letters", to: "/dead-letters" },
    ]);
  });

  it("calls a node that went without draining lost, and a supervisor that gave up a warning", () => {
    const now = watched([], [{ id: "1", time_ms: 50_000, node_id: "a", kind: "gave_up", subject: "fragile", detail: '"poked to fail"' }]);
    const lost = { ...listening("b", 0, 0), leftAt: 40_000, leaveReason: null };
    const left = { ...listening("c", 0, 0), leftAt: 30_000, leaveReason: "rolling upgrade" };
    expect(attention(now, [lost, left], 100_000, null, new NodeBuffer())).toMatchObject([
      { tone: "danger", node: "b", title: "Lost", detail: expect.stringMatching(/^1m ago · stopped without a drain/) },
      { tone: "warning", node: "a", title: "Supervisor stopped fragile", detail: '50s ago · no more restarts after "poked to fail"', to: "/actors?aq=fragile" },
      { tone: "muted", node: "c", title: "Left the membership", detail: "1m ago · rolling upgrade" },
    ]);
  });

  it("names the handshake failures behind a refusing listener by reason", () => {
    const buffer = new NodeBuffer();
    let last = watched([]);
    for (let s = 0; s <= 10; s++) {
      const v = listening("a", s * 1000, 0);
      v.snapshot!.cluster.engine.listener.handshake_failures = [s, 0, 0, 0, 0];
      buffer.append((last = watched([v])));
    }
    expect(attention(last, [], 10_000, null, buffer)).toMatchObject([{ title: "Peers refused at the listener", detail: "10 in the last 10s · since 9s ago or earlier · 10 since start, handshake auth 10" }]);
  });

  it("flags quota share denials only above 1/s over the minute, and every limit denial", () => {
    const run = (share: (s: number) => number, limit: (s: number) => number = () => 0) => {
      const buffer = new NodeBuffer();
      let last = watched([]);
      for (let s = 0; s <= 120; s++) {
        const v = listening("a", s * 1000, 0);
        Object.assign(v.snapshot!.cluster, { quotas: { denied_share: share(s), denied_limit: limit(s) } });
        buffer.append((last = watched([v])));
      }
      return titles(attention(last, [], 120_000, null, buffer));
    };
    expect(run((s) => (s < 100 ? 0 : 50)), "a burst of 50 is under 1/s over the minute").toEqual([]);
    expect(run((s) => s), "exactly 1/s is not above it").toEqual([]);
    expect(run((s) => 2 * s), "2/s held for the minute").toEqual(["Quota share denials"]);
    expect(run(() => 0, (s) => (s < 110 ? 0 : 1)), "one limit denial").toEqual(["Quota limit denials"]);
  });

  it("reads version refusals at their reason's index, and an older node without stream quota counts as none refused", () => {
    const v = view("a", 1000, []);
    v.snapshot!.cluster.engine.listener.handshake_failures = [1, 0, 0, 0, 3];
    Object.assign(v.snapshot!, { streams: { orphaned: 0, hub: { recovering: 0, quota_refused: [2, 0, 5, 0, 0] }, disk: null } });
    expect(nodeRow(v)).toMatchObject({ refused: 4, versionRefused: 3, quotaRefused: 7, diskFailing: null });
    Object.assign(v.snapshot!, { streams: { orphaned: 0, hub: { recovering: 0 }, disk: null } });
    expect(nodeRow(v)!.quotaRefused).toBe(0);
  });

  it("says once that the console server is gone instead of calling every node stale", () => {
    const last = watched(["a", "b", "c"].map((id) => listening(id, 1000, 0)));
    expect(attention(last, [], 61_000, { since: 1000, why: "Console server unreachable" }, new NodeBuffer())).toEqual([{ tone: "danger", node: "a", title: "Console server unreachable", detail: "last seen 1m ago" }]);
    expect(titles(attention(last, [], 61_000, null, new NodeBuffer()))).toEqual(["Stale snapshot", "Stale snapshot", "Stale snapshot"]);
  });
});

it("a counter sampled every second reads as a flat rate at every range under a minute's step", () => {
  for (const span of [300_000, 900_000]) {
    const step = historyStep(span, 144);
    // As a store answers: each step holds the last sample in it, stamped with the step's start.
    const buckets = new Map<number, number>();
    for (let k = 0; k < span / 1000; k++) buckets.set(Math.floor((1_700_000_000_337 + k * 1000) / step) * step, k);
    // The server sends each step's gain over the step before it.
    const held = [...buckets.values()];
    const series: Series = { node_id: "a", incarnation: "1", labels: {}, kind: "counter", unit: "", values: held.slice(1).map((v, i) => v - held[i]) };
    const rates = aggregate({ from: 0, step, count: held.length - 1, truncated: false, settled: 0, series: [series] }, [series], "rate").map(([, v]) => v);
    expect(new Set(rates.slice(1, -1)), `${span / 60_000}m at a ${step} ms step`).toEqual(new Set([1]));
  }
});

it("lays a trace out parents first, keeps spans whose parent is gone, and clamps clock skew", () => {
  const span = (id: string, parent: string, start_us: number, busy_ns: number, wait_ns = 0) => ({ span: id, parent, start_us, busy_ns, wait_ns }) as SpanRecord;
  const spans = [span("c", "a", 1_300, 100_000), span("lost", "gone", 1_100, 0), span("b", "a", 1_200, 600_000, 100_000), span("a", "", 1_000, 1_000_000), span("d", "b", 900, 50_000)];
  const trace = layoutTrace(spans);
  expect(trace.rows.map((r) => [r.span.span, r.depth, r.orphan])).toEqual([["a", 0, false], ["b", 1, false], ["d", 2, false], ["c", 1, false], ["lost", 0, true]]);
  expect(trace.rows.map((r) => r.guides)).toEqual([[], [true], [true, false], [false], []]);
  expect([trace.startUs, trace.endUs]).toEqual([900, 2_000]);
  const b = place(spans[2], trace);
  expect([b.wait, b.left, b.width].map((n) => +n.toFixed(3))).toEqual([0.182, 0.273, 0.545]);
  expect(place(span("early", "", 0, 5_000_000), trace)).toEqual({ gap: 0, wait: 0, left: 0, width: 1 });
  const late = layoutTrace([span("tell", "", 1_000, 50_000), span("far", "tell", 2_000, 0, 300_000)]);
  expect(late.rows.map((r) => r.gapUs)).toEqual([0, 650]);
  const titled = { name: "work", node_id: "a", peer: "", actor_name: "", actor_type: "app::Worker" };
  expect(spanTitle({ ...titled, kind: "handle" })).toBe("work · Worker");
  expect(spanTitle({ ...titled, kind: "handle", actor_name: "orders" })).toBe("work · orders");
  expect(spanTitle({ ...titled, kind: "call", name: "request", peer: "b" })).toBe("request → b");
  expect(spanTitle({ ...titled, kind: "stream", name: "md/btc", peer: "b" })).toBe("batch from b");
  expect(traceParts({ root: "md/btc", root_kind: "stream", root_peer: "" } as never)).toEqual({ verb: "publish", target: "md/btc" });
  expect(traceParts({ root: "request", root_kind: "call", root_peer: "b" } as never)).toEqual({ verb: "request", target: "b" });
  expect(traceParts({ root: "work", root_kind: "handle", root_actor: "app::Worker", root_actor_name: "" } as never)).toEqual({ verb: "work", target: "Worker" });
  const hues = Array.from({ length: 12 }, (_, i) => nodeHue(i));
  expect(hues.every((h) => h >= 60 && h <= 300) && new Set(hues).size === 12).toBe(true);
  const loop = layoutTrace([span("x", "y", 1, 0), span("y", "x", 2, 0)]);
  expect(loop.rows.map((r) => r.span.span).sort()).toEqual(["x", "y"]);
});

describe("counters by reason", () => {
  it("expand each reason array under its names, and leave other arrays out", () => {
    const cluster = { engine: { inbound_rejected: [0, 0, 0, 2, 0, 0], listener: { handshake_failures: [1, 0, 0, 0, 3] }, request_rtt: { buckets: [5], count: 1 } }, isolated: false, quotas: null };
    const rows = new Map(flatten("cluster", cluster));
    expect(rows.get("cluster.engine.inbound_rejected.forbidden")).toBe(2);
    expect(rows.get("cluster.engine.listener.handshake_failures.version")).toBe(3);
    expect(rows.get("cluster.engine.request_rtt.count")).toBe(1);
    expect(rows.size, "six rejections, five handshake reasons, one count").toBe(12);
  });

  it("name only the reasons that counted, as words", () => {
    expect(reasons(HANDSHAKE_FAILURES, [1, 0, 0, 0, 3])).toBe("auth 1 · version 3");
    expect(reasons(INBOUND_REJECTIONS, [0, 4])).toBe("schema mismatch 4");
    expect(reasons(INBOUND_REJECTIONS, [])).toBe("");
  });
});

describe("departures", () => {
  const left = (node: string, time_ms: number, detail: string): ClusterEvent => ({ id: `${node}${time_ms}`, time_ms, node_id: node, kind: "left", subject: "", detail });
  const at = (time: number, nodes: NodeView[], events: ClusterEvent[]) => ({ timestamp_ms: time, nodes, events }) as unknown as Overview;
  const inc = "0".repeat(31) + "7";

  it("are rebuilt from the left events on a fresh load: a reason is a leave, none is a loss", () => {
    const events = [left("b", 1_000_000, `rolling upgrade · incarnation ${inc}`), left("c", 1_200_000, `incarnation ${inc}`), left("a", 1_100_000, "incarnation 1"), left("old", 100_000, "incarnation 2")];
    const gone = departures([], [], at(1_300_000, [view("a", 1_300_000, [])], events));
    expect(gone.map((n) => [n.node.id, n.leftAt, n.leaveReason, n.node.incarnation, n.snapshot])).toEqual([
      ["c", 1_200_000, null, inc, null],
      ["b", 1_000_000, "rolling upgrade", inc, null],
    ]);
  });

  it("keep the last report of a node seen leaving, and its drain from the record it left with", () => {
    const [drained, crashed] = [view("d", 1000, []), view("x", 1000, [], "1")];
    drained.node.metadata = { "axon.lifecycle": "draining", "axon.leave.reason": "scale in" };
    const gone = departures([], [drained, crashed], at(2000, [], [left("x", 1500, "incarnation 1")]));
    expect(gone.map((n) => [n.node.id, n.leftAt, n.leaveReason, n.snapshot?.timestamp_ms])).toEqual([
      ["d", 2000, "scale in", 1000],
      ["x", 1500, null, 1000],
    ]);
    expect(departures(gone, [], at(2000 + 900_000, [], []))).toEqual([]);
    expect(departures(gone, [], at(3000, [view("x", 3000, [])], [])).map((n) => n.node.id)).toEqual(["d"]);
  });

  it("take only their own boot's left, which may come a refresh later", () => {
    const again = { ...view("v", 50_000, [], "2"), node: { id: "v", incarnation: "2", address: "", metadata: { "axon.lifecycle": "draining" } } };
    const earlier = left("v", 30_000, "crash test · incarnation 1");
    const gone = departures([], [again], at(60_000, [], [earlier]));
    expect(gone.map((n) => [n.leftAt, n.leaveReason])).toEqual([[60_000, "drained"]]);
    const told = departures(gone, [], at(61_000, [], [earlier, left("v", 59_500, "scale in · incarnation 2")]));
    expect(told.map((n) => [n.leftAt, n.leaveReason, n.snapshot?.timestamp_ms])).toEqual([[59_500, "scale in", 50_000]]);
  });
});

it("a burst's since spans lulls shorter than the window, and a quiet window ends it", () => {
  const rows = [0, 1, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 4].map((v, i) => ({ t: i * 10_000, deadLetters: v }) as NodeRow);
  const pick = (r: NodeRow) => r.deadLetters;
  expect(growth(rows.slice(0, 6), pick)).toEqual({ n: 3, total: 3, since: 10_000, open: true, span: 50_000 });
  expect(growth(rows, pick)).toEqual({ n: 1, total: 4, since: 120_000, open: false, span: 60_000 });
  expect(growth(rows.slice(0, 12), pick)).toBeNull();
});
