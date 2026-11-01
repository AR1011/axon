import { bytes, bytesPerSecond, duration, perSecond } from "./format";
import type { Link } from "./links";

export const CARD = { w: 164, h: 66 };
export const MAX_NODES = 80;
const GAP = { x: 56, y: 64 };

export type LayoutMode = "connectivity" | "roles" | "circular" | "grid" | "force";
export const LAYOUTS: LayoutMode[] = ["connectivity", "roles", "circular", "grid", "force"];

export interface Box {
  x: number;
  y: number;
}
export interface Group {
  role: string;
  x: number;
  y: number;
  w: number;
  h: number;
}
export interface Layout {
  positions: Map<string, Box>;
  groups: Group[];
  width: number;
  height: number;
}

type Edge = [string, string];

function rows(ids: string[], perRow: number, top = 0): [string, Box][] {
  return ids.map((id, i) => [id, { x: (i % perRow) * (CARD.w + GAP.x), y: top + Math.floor(i / perRow) * (CARD.h + GAP.y) }]);
}

function connectivity(ids: string[], edges: Edge[]): [string, Box][] {
  const neighbours = new Map<string, Set<string>>(ids.map((id) => [id, new Set()]));
  for (const [a, b] of edges) {
    if (a === b || !neighbours.has(a) || !neighbours.has(b)) continue;
    neighbours.get(a)!.add(b);
    neighbours.get(b)!.add(a);
  }
  const byDegree = [...ids].sort((a, b) => neighbours.get(b)!.size - neighbours.get(a)!.size || a.localeCompare(b));
  const placed = new Set<string>();
  const layers: string[][] = [];
  const isolated: string[] = [];
  for (const root of byDegree) {
    if (placed.has(root)) continue;
    if (!neighbours.get(root)!.size) {
      isolated.push(root);
      placed.add(root);
      continue;
    }
    let frontier = [root];
    placed.add(root);
    while (frontier.length) {
      layers.push(frontier);
      const next = new Set<string>();
      for (const id of frontier) for (const n of neighbours.get(id)!) if (!placed.has(n)) next.add(n);
      frontier = [...next].sort();
      frontier.forEach((id) => placed.add(id));
    }
  }
  const out: [string, Box][] = [];
  let top = 0;
  for (const layer of [...layers, ...(isolated.length ? [isolated.sort()] : [])]) {
    out.push(...rows(layer, 4, top));
    top += Math.ceil(layer.length / 4) * (CARD.h + GAP.y);
  }
  return out;
}

function force(ids: string[], edges: Edge[]): [string, Box][] {
  const n = ids.length;
  const radius = 120 + n * 28;
  const at = ids.map((_, i) => ({ x: Math.cos((2 * Math.PI * i) / n) * radius, y: Math.sin((2 * Math.PI * i) / n) * radius }));
  const index = new Map(ids.map((id, i) => [id, i]));
  const springs = edges.map(([a, b]) => [index.get(a)!, index.get(b)!]).filter(([a, b]) => a !== b && a != null && b != null);
  for (let step = 0; step < 140; step++) {
    const cool = 1 - step / 140;
    const push = at.map((p) => ({ x: -p.x * 0.01, y: -p.y * 0.01 }));
    for (let i = 0; i < n; i++)
      for (let j = i + 1; j < n; j++) {
        const [dx, dy] = [at[i].x - at[j].x || 0.01 * (i - j), at[i].y - at[j].y || 0.01];
        const d2 = Math.max(dx * dx + dy * dy, 100);
        const f = 60000 / d2 / Math.sqrt(d2);
        push[i].x += dx * f;
        push[i].y += dy * f;
        push[j].x -= dx * f;
        push[j].y -= dy * f;
      }
    for (const [a, b] of springs) {
      const [dx, dy] = [at[b].x - at[a].x, at[b].y - at[a].y];
      const d = Math.hypot(dx, dy) || 1;
      const f = ((d - 240) / d) * 0.05;
      push[a].x += dx * f;
      push[a].y += dy * f;
      push[b].x -= dx * f;
      push[b].y -= dy * f;
    }
    at.forEach((p, i) => {
      p.x += Math.max(-40, Math.min(40, push[i].x)) * cool;
      p.y += Math.max(-40, Math.min(40, push[i].y)) * cool;
    });
  }
  // A centre-distance force leaves wide cards overlapping; separate the rectangles themselves.
  const [needX, needY] = [CARD.w + 24, CARD.h + 24];
  for (let pass = 0; pass < 200; pass++) {
    let moved = false;
    for (let i = 0; i < n; i++)
      for (let j = i + 1; j < n; j++) {
        const [dx, dy] = [at[j].x - at[i].x, at[j].y - at[i].y];
        const [ox, oy] = [needX - Math.abs(dx), needY - Math.abs(dy)];
        if (ox <= 0 || oy <= 0) continue;
        moved = true;
        if (ox / needX < oy / needY) {
          const shift = (ox / 2 + 0.5) * (dx < 0 ? -1 : 1);
          at[i].x -= shift;
          at[j].x += shift;
        } else {
          const shift = (oy / 2 + 0.5) * (dy < 0 ? -1 : 1);
          at[i].y -= shift;
          at[j].y += shift;
        }
      }
    if (!moved) break;
  }
  return ids.map((id, i) => [id, at[i]]);
}

/** Pure and order-independent: the same nodes and edges give the same picture. */
export function layout(nodeIds: string[], links: Edge[], mode: LayoutMode, roles: Record<string, string>): Layout {
  const ids = [...new Set(nodeIds)].sort();
  const edges = links.map(([a, b]) => (a < b ? [a, b] : [b, a]) as Edge).sort((x, y) => `${x}`.localeCompare(`${y}`));
  const groups: Group[] = [];
  let placed: [string, Box][];
  if (mode === "grid") placed = rows(ids, Math.max(1, Math.min(5, Math.ceil(Math.sqrt(ids.length)))));
  else if (mode === "circular") {
    const rx = Math.max(260, (ids.length * (CARD.w + 64)) / (2 * Math.PI));
    placed = ids.map((id, i) => {
      const angle = (2 * Math.PI * i) / ids.length - Math.PI / 2;
      return [id, { x: Math.cos(angle) * rx, y: Math.sin(angle) * rx * 0.75 }];
    });
  } else if (mode === "force") placed = force(ids, edges);
  else if (mode === "roles") {
    placed = [];
    let top = 0;
    const byRole = new Map<string, string[]>();
    for (const id of ids) byRole.set(roles[id] || "unassigned", [...(byRole.get(roles[id] || "unassigned") ?? []), id]);
    for (const role of [...byRole.keys()].sort()) {
      const members = byRole.get(role)!;
      const inner = rows(members, 4, top + 34);
      placed.push(...inner.map(([id, box]) => [id, { x: box.x + 18, y: box.y }] as [string, Box]));
      const columns = Math.min(4, members.length);
      const height = 34 + Math.ceil(members.length / 4) * (CARD.h + GAP.y) - GAP.y + 18;
      groups.push({ role, x: 0, y: top, w: columns * (CARD.w + GAP.x) - GAP.x + 36, h: height });
      top += height + 28;
    }
  } else placed = connectivity(ids, edges);

  const pad = 40;
  const minX = Math.min(0, ...placed.map(([, b]) => b.x), ...groups.map((g) => g.x));
  const minY = Math.min(0, ...placed.map(([, b]) => b.y), ...groups.map((g) => g.y));
  const positions = new Map(placed.map(([id, b]) => [id, { x: b.x - minX + pad, y: b.y - minY + pad }]));
  groups.forEach((g) => ((g.x += pad - minX), (g.y += pad - minY)));
  const width = Math.max(...[...positions.values()].map((b) => b.x + CARD.w), ...groups.map((g) => g.x + g.w), 0) + pad;
  const height = Math.max(...[...positions.values()].map((b) => b.y + CARD.h), ...groups.map((g) => g.y + g.h), 0) + pad;
  return { positions, groups, width, height };
}

export type Measure = "traffic" | "messages" | "size" | "latency";
export const MEASURES: Record<Measure, { label: string; unit: string; icon: string; value: (l: Link) => number | null; format: (n: number | null) => string }> = {
  traffic: { label: "Traffic", unit: "bytes/s", icon: "traffic", value: (l) => l.rate, format: (n) => (n == null ? "Unknown" : bytesPerSecond(n)) },
  messages: { label: "Frames/second", unit: "frames/s", icon: "messages", value: (l) => l.frames, format: (n) => (n == null ? "Unknown" : perSecond(n)) },
  size: { label: "Average frame size", unit: "bytes", icon: "size", value: (l) => l.frameBytes, format: (n) => (n == null ? "Unknown" : bytes(n)) },
  latency: { label: "Round trip", unit: "keepalive RTT", icon: "latency", value: (l) => l.rtt, format: (n) => (n == null ? "Unknown" : duration(n)) },
};

/** Unknown stays unknown; zero against a zero maximum is zero. */
export function fraction(value: number | null, max: number, log: boolean): number | null {
  if (value == null || !Number.isFinite(value)) return null;
  if (max <= 0) return 0;
  const f = log ? Math.log1p(value) / Math.log1p(max) : value / max;
  return Math.max(0, Math.min(1, f));
}

export const RAMPS = {
  spectrum: { label: "Spectrum", stops: ["#608ed6", "#48aab0", "#65cda0", "#dbb976", "#de7078"] },
  viridis: { label: "Viridis", stops: ["#46317e", "#365b8c", "#277e8e", "#1fa187", "#49c16d", "#a0da39"] },
  magma: { label: "Magma", stops: ["#51127c", "#8c2981", "#c73e73", "#f1605d", "#fe9f6d", "#fde3a5"] },
  heat: { label: "Heat", stops: ["#5b6b7b", "#c9a227", "#e8743b", "#d9363e"] },
  ocean: { label: "Ocean", stops: ["#2b4a6f", "#2f6f9f", "#3f9fc4", "#7fd0d8"] },
  mono: { label: "Mono", stops: ["#5d646c", "#8d949c", "#c4c9cf"] },
} as const;
export type Ramp = keyof typeof RAMPS;

const rgb = (hex: string) => [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));

export function ramp(f: number, name: Ramp = "spectrum"): string {
  const stops = RAMPS[name].stops.map(rgb);
  const at = Math.max(0, Math.min(1, f)) * (stops.length - 1);
  const i = Math.min(stops.length - 2, Math.floor(at));
  const mix = stops[i].map((c, k) => Math.round(c + (stops[i + 1][k] - c) * (at - i)));
  return `rgb(${mix.join(",")})`;
}

export const rampCss = (name: Ramp) => `linear-gradient(90deg, ${RAMPS[name].stops.join(", ")})`;
