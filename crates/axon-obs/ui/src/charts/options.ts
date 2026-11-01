import type { Histogram } from "../api";
import { accentColor } from "../appearance";
import { byUnit, clock, count, duration, niceStep, perSecond, type Unit } from "../model/format";
import { boundNs, drawn, type Heat } from "../model/histogram";
import type { P } from "../model/series";

const light = () => typeof document !== "undefined" && document.documentElement.dataset.theme === "light";
const SERIES = {
  dark: { received: "#79a7de", latency: "#d7b56d", rejected: "#d9808a", more: ["#b59ce0", "#5fc2bf", "#e0955f", "#a3a8b0"] },
  light: { received: "#2f6fbf", latency: "#a9781c", rejected: "#c04b57", more: ["#7a52b3", "#1d8582", "#bd5f22", "#686d75"] },
};
const series = () => SERIES[light() ? "light" : "dark"];
/** Eight colours, one per line the explore page draws at most. */
export const palette = () => [accentColor(), series().received, series().latency, series().rejected, ...series().more];
/** Resolved when a chart is built: `sent` is the accent, the rest are tuned per theme for contrast. */
export const COLORS = {
  get sent() {
    return accentColor();
  },
  get received() {
    return series().received;
  },
  get latency() {
    return series().latency;
  },
  get rejected() {
    return series().rejected;
  },
};

export interface Line {
  name: string;
  points: P[];
  color?: string;
  /** An earlier boot's line: drawn faint, in the colour of the line before it. */
  faded?: boolean;
  /** Where a dashed marker crosses the plot, as at a restart. */
  markAt?: number;
  markLabel?: string;
}

const THEME = {
  dark: { text: "#909ba9", grid: "#292e36", ink: "#dce3ed", tip: "#1c2026", heat: ["#1c242b", "#244b57", "#428b94", "#6ebca2", "#deb66c"] },
  light: { text: "#667080", grid: "#e7e9ee", ink: "#263140", tip: "#ffffff", heat: ["#f4f6f8", "#cfe3c4", "#8cc9a8", "#3f9a99", "#2c5f86"] },
};
const MONO = '"JetBrains Mono Variable", "SFMono-Regular", Consolas, monospace';

/** HTML, which glides after the pointer where a canvas tooltip jumps; formatters escape what they did not write with `plain`. */
function tooltip(theme: "dark" | "light") {
  const t = THEME[theme];
  return { renderMode: "html", confine: true, backgroundColor: t.tip, borderColor: t.grid, padding: [6, 8], extraCssText: "white-space:pre;box-shadow:none", textStyle: { color: t.ink, fontSize: 10, fontFamily: MONO } };
}

const plain = (text: string) => text.replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);

/** One event on a swimlane: its lane, what the tooltip says, and its tone's colour. */
export interface Mark {
  time_ms: number;
  lane: string;
  text: string;
  tone: "success" | "warning" | "muted";
  kind: string;
}

const TONES = { dark: { success: "#6ebca2", warning: "#e8975a", muted: "#7d8794" }, light: { success: "#2f8f6f", warning: "#a8561a", muted: "#8a939f" } };

export function lanesOption(marks: Mark[], lanes: string[], domain: [number, number], theme: "dark" | "light") {
  const t = THEME[theme];
  const span = domain[1] - domain[0];
  return {
    animation: false,
    grid: { left: 110, right: 20, top: 6, bottom: 22 },
    textStyle: { fontFamily: MONO },
    tooltip: { ...tooltip(theme), trigger: "item", formatter: (item: { data: { value: [number, string]; text: string } }) => `${clock(item.data.value[0], span, true)}  ${plain(item.data.value[1])}\n${plain(item.data.text)}` },
    xAxis: { type: "time", min: domain[0], max: domain[1], splitNumber: 6, axisLabel: { color: t.text, fontSize: 10, hideOverlap: true, formatter: (v: number) => clock(v, span, span < 300_000) }, axisLine: { lineStyle: { color: t.grid } }, axisTick: { show: false }, splitLine: { show: false } },
    yAxis: { type: "category", data: lanes, inverse: true, axisLabel: { color: t.text, fontSize: 10, width: 100, overflow: "truncate" }, axisLine: { show: false }, axisTick: { show: false }, splitLine: { show: true, lineStyle: { color: t.grid } } },
    series: [{ type: "scatter", symbolSize: 9, cursor: "pointer", data: marks.map((m) => ({ value: [m.time_ms, m.lane], text: m.text, kind: m.kind, itemStyle: { color: TONES[theme][m.tone], opacity: 0.85 } })) }],
  };
}

export function lineOption(lines: Line[], unit: Unit, domain: [number, number], theme: "dark" | "light") {
  const t = THEME[theme];
  const first = Math.min(...lines.flatMap((l) => l.points.filter((p) => p[1] !== null).slice(0, 1).map((p) => p[0])));
  // A young series fills the plot; an old one keeps the window the operator asked for.
  const from = first - domain[0] > (domain[1] - domain[0]) * 0.5 ? first - (domain[1] - first) * 0.08 : domain[0];
  const span = domain[1] - from;
  const top = Math.max(0, ...lines.flatMap((l) => l.points.map((p) => p[1] ?? 0)));
  // Floors keep a flat-zero series from getting an axis of repeated labels.
  const floor = { bytes: 1024, "bytes/s": 1024, "/s": 1, count: 3, "%": 1, ns: 1000, ratio: 0.01 }[unit];
  const step = niceStep(Math.max(top, floor), unit === "bytes" || unit === "bytes/s");
  const colors = palette();
  let shown = -1;
  return {
    animation: false,
    color: colors,
    grid: { left: 64, right: 20, top: 18, bottom: 46 },
    textStyle: { fontFamily: MONO },
    tooltip: {
      ...tooltip(theme),
      trigger: "axis",
      axisPointer: { type: "cross", label: { show: false } },
      formatter: (items: { axisValue: number; marker: string; seriesName: string; value: P }[]) =>
        [clock(items[0]?.axisValue, span, true), ...items.map((i) => `${i.marker} ${plain(i.seriesName)}  ${byUnit(unit, i.value[1])}`)].join("\n"),
    },
    // An earlier boot's line takes no legend entry: the faded line beside its own says what it is.
    legend: { data: lines.filter((l) => !l.faded).map((l) => l.name), bottom: 0, type: "scroll", itemWidth: 10, itemHeight: 2, icon: "rect", textStyle: { color: t.text, fontSize: 10 } },
    xAxis: { type: "time", min: from, max: domain[1], splitNumber: 4, axisLabel: { color: t.text, fontSize: 10, hideOverlap: true, formatter: (v: number) => clock(v, span, span < 300_000) }, axisLine: { lineStyle: { color: t.grid } }, axisTick: { show: false }, splitLine: { show: false } },
    yAxis: {
      type: "value",
      min: 0,
      max: Math.ceil(Math.max(top, floor) / step) * step,
      interval: step,
      axisLabel: { color: t.text, fontSize: 10, formatter: (v: number) => byUnit(unit, v) },
      axisLine: { show: false },
      axisTick: { show: false },
      splitLine: { lineStyle: { color: t.grid, type: "dashed" } },
    },
    series: lines.map((line) => ({
      type: "line",
      name: line.name,
      data: line.points,
      color: line.color ?? colors[(line.faded ? Math.max(shown, 0) : ++shown) % colors.length],
      smooth: false,
      connectNulls: false,
      showSymbol: line.points.filter((p) => p[1] !== null).length === 1,
      symbolSize: 4,
      lineStyle: { width: theme === "light" ? 1.8 : 1.5, opacity: line.faded ? 0.35 : 1 },
      areaStyle: { opacity: line.faded ? 0 : theme === "light" ? 0.07 : 0.035 },
      emphasis: { focus: "series" },
      ...(line.markAt != null && {
        markLine: { silent: true, symbol: "none", animation: false, lineStyle: { color: t.text, type: "dashed", width: 1 }, label: { formatter: line.markLabel ?? "", color: t.text, fontSize: 9 }, data: [{ xAxis: line.markAt }] },
      }),
    })),
  };
}

const bound = (i: number) => (Number.isFinite(boundNs(i)) ? duration(boundNs(i)) : "∞");
/** What bucket `i` holds: above the bound before it, up to its own. */
const bucketRange = (i: number) => `${i ? bound(i - 1) : "0"} – ${bound(i)}`;

export function histogramOption(h: Histogram, theme: "dark" | "light") {
  const t = THEME[theme];
  const [first, end] = drawn(h.buckets);
  const at = (i: number) => first + i;
  return {
    animation: false,
    grid: { left: 49, right: 18, top: 18, bottom: 42 },
    textStyle: { fontFamily: MONO },
    tooltip: {
      ...tooltip(theme),
      trigger: "item",
      formatter: (item: { dataIndex: number; value: number }) =>
        `${bucketRange(at(item.dataIndex))}\n${count(item.value)} samples`,
    },
    xAxis: { type: "category", name: "Duration ≤", nameLocation: "middle", nameGap: 26, nameTextStyle: { color: t.text, fontSize: 10 }, data: Array.from({ length: end - first }, (_, i) => bound(at(i))), axisLabel: { color: t.text, fontSize: 10, hideOverlap: true }, axisLine: { lineStyle: { color: t.grid } }, axisTick: { show: false } },
    yAxis: { type: "value", name: "Samples", nameTextStyle: { color: t.text, fontSize: 10 }, splitNumber: 3, minInterval: 1, axisLabel: { color: t.text, fontSize: 10, formatter: count }, splitLine: { lineStyle: { color: t.grid, type: "dashed" } } },
    series: [{ type: "bar", data: h.buckets.slice(first, end), color: accentColor(), barCategoryGap: "18%", itemStyle: { borderRadius: [2, 2, 0, 0] } }],
  };
}

/** A heat cell's tooltip: its time, rate, bucket and column width, over its column's buckets with the cell's marked, as Grafana draws one. */
function heatTip(grid: Heat, [column, row, rate]: [number, number, number], t: (typeof THEME)["dark"]): string {
  const rates = Array<number>(grid.buckets).fill(0);
  for (const [c, r, v] of grid.cells) if (c === column) rates[r] = v;
  const top = Math.max(...rates, 1e-9);
  const accent = accentColor();
  const bars = rates.map((v, r) => `<i style="flex:1;height:${Math.max(v > 0 ? 2 : 0, (v / top) * 44)}px;background:${r === row ? accent : t.text};opacity:${r === row ? 1 : 0.35}"></i>`).join("");
  const width = (grid.columns[1] ?? grid.columns[0]) - grid.columns[0];
  const line = (name: string, value: string) => `<span style="color:${t.text}">${name}</span><span style="text-align:right">${value}</span>`;
  return `<div style="min-width:180px;padding:2px;white-space:normal"><div style="padding-bottom:6px;margin-bottom:6px;border-bottom:1px solid ${t.grid}">${clock(grid.columns[column], 0, true)}</div><div style="display:grid;grid-template-columns:auto auto;gap:3px 16px">${line("Rate", rate < 1 ? `${rate.toFixed(2)}/s` : perSecond(rate))}${line("Bucket", bucketRange(grid.first + row))}${line("Column", duration(Math.round(width) * 1e6))}</div><div style="display:flex;align-items:flex-end;gap:1px;height:44px;margin-top:8px">${bars}</div></div>`;
}

export function heatOption(grid: Heat, theme: "dark" | "light") {
  const t = THEME[theme];
  const span = (grid.columns.at(-1) ?? 0) - (grid.columns[0] ?? 0);
  const at = (ms: number) => clock(ms, span, span < 300_000);
  const rows = grid.rows;
  return {
    animation: false,
    grid: { left: rows ? 170 : 62, right: 20, top: 12, bottom: 57 },
    textStyle: { fontFamily: MONO },
    tooltip: {
      ...tooltip(theme),
      trigger: "item",
      formatter: (item: { value: [number, number, number] }) =>
        rows ? `${plain(rows[item.value[1]])}\n${at(grid.columns[item.value[0]])}\nhighest ${count(item.value[2])}` : heatTip(grid, item.value, t),
    },
    xAxis: { type: "category", data: grid.columns.map(at), axisLabel: { color: t.text, fontSize: 10, hideOverlap: true }, axisLine: { lineStyle: { color: t.grid } }, axisTick: { show: false }, splitArea: { show: false } },
    yAxis: { type: "category", inverse: !!rows, data: rows ?? Array.from({ length: grid.buckets }, (_, i) => bound(grid.first + i)), axisLabel: { color: t.text, fontSize: 10, width: 160, overflow: "truncate" }, axisLine: { show: false }, axisTick: { show: false } },
    visualMap: { min: 0, max: Math.max(grid.max, 1e-9), calculable: false, orient: "horizontal", left: "center", bottom: 0, itemWidth: 8, itemHeight: 90, text: [rows ? count(grid.max) : `${grid.max.toFixed(1)}/s`, "0"], textStyle: { color: t.text, fontSize: 10 }, inRange: { color: t.heat } },
    series: [{ type: "heatmap", data: grid.cells, emphasis: { itemStyle: { borderColor: t.ink, borderWidth: 1 } } }],
  };
}
