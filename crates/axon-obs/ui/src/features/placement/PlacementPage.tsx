import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { memo, useCallback, useEffect, useMemo, useReducer, useRef, useState, type PointerEvent as ReactPointerEvent, type ReactNode } from "react";
import { Link } from "react-router";
import type { ClusterEvent, KeyLookup, KeysPage, NodeView, Overview } from "../../api";
import { getJson } from "../../api";
import { COLORS, palette } from "../../charts/options";
import { Count, Dot, Empty } from "../../components/atoms";
import { Flag, NodeName } from "../../components/Flag";
import { HelpDialog, Part } from "../../components/HelpDialog";
import { Icon } from "../../components/Icon";
import { Search } from "../../components/Search";
import { Select } from "../../components/Select";
import { useAccent, useEscape, useNow, useTheme, useUrlState } from "../../hooks";
import { ago, count, natural, sum } from "../../model/format";
import { acceptorRows, readiness } from "../../model/rollout";
import { useLive } from "../../store";
import { clampZoom, zoomAt, type Camera } from "../topology/useCamera";

// Indexed as HostedKind.started and .ended (axon_cluster::KEYED_STARTS, KEYED_ENDS).
const HANDOFF = 1;
const MOVED = 4;
const DANGER: Record<number, string> = { 1: "failed", 2: "lost their lease", 3: "stopped in isolation" };
const GHOST_MS = 120_000;
const NO_REGION = "no region";
/** The source of keys whose node left before its loss was counted. */
const LOST = "?";

interface Box {
  id: string;
  region: string;
  role: string;
  byKind: Map<string, number>;
  keys: number;
  isolated: boolean;
  acceptor: boolean;
  ready: boolean;
  /** When it left, for a node that is gone. */
  gone: number | null;
  drained: boolean;
}

interface Kind {
  kind: string;
  count: number;
  eager: boolean;
}

/** The cluster as boxes: a box per node, and nodes that left in the last two minutes as ghosts. */
function model(overview: Overview, departed: NodeView[]) {
  const kinds = new Map<string, Kind>();
  const acceptors = new Set(acceptorRows(overview.nodes).map((a) => a.id));
  const box = (view: NodeView, gone: number | null): Box => {
    const s = view.snapshot;
    const byKind = new Map<string, number>();
    for (const k of s?.hosted?.kinds ?? []) {
      byKind.set(k.kind, k.count ?? 0);
      if (gone === null) {
        const kind = kinds.get(k.kind) ?? { kind: k.kind, count: 0, eager: k.eager };
        kind.count += k.count ?? 0;
        kinds.set(k.kind, kind);
      }
    }
    const c = s?.cluster;
    return {
      id: view.node.id,
      region: view.node.metadata.region || NO_REGION,
      role: view.node.metadata.role ?? "",
      byKind,
      keys: s?.hosted?.total ?? 0,
      isolated: !!c?.isolated,
      acceptor: acceptors.has(view.node.id),
      ready: readiness(view)?.length === 0,
      gone,
      drained: view.leaveReason !== null,
    };
  };
  const members = overview.nodes.map((v) => box(v, null));
  const back = new Set(members.map((b) => b.id));
  const ghosts = departed.filter((v) => !back.has(v.node.id) && (v.leftAt ?? 0) > overview.timestamp_ms - GHOST_MS).map((v) => box(v, v.leftAt ?? overview.timestamp_ms));
  const sorted = [...kinds.values()].sort((a, b) => natural(a.kind, b.kind));
  // The kind a key list or a key search shows when none is picked: the one with the most keys.
  const top = sorted.reduce<Kind | undefined>((t, k) => (!t || k.count > t.count ? k : t), undefined)?.kind ?? "";
  return { kinds: sorted, top, boxes: [...members, ...ghosts], members: overview.nodes.length };
}
type Model = ReturnType<typeof model>;


// The movement between two overviews, from each node's counters, kept in this tab: the dashboard stores no pairs.
interface Seen {
  incarnation: string;
  kinds: Map<string, { keys: number; started: number[]; ended: number[] }>;
}
interface Flow {
  kind: string;
  from: string;
  to: string;
  n: number;
}
interface Side {
  node: string;
  n: number;
}
interface Row {
  /** When its last and its first move happened. */
  t: number;
  since: number;
  kind: string;
  from: Side[];
  to: Side[];
  n: number;
  danger?: string;
  names: string[];
  estimated: boolean;
}

const ZERO = { keys: 0, started: [], ended: [] };
const seen = new Map<string, Seen>();
const rows: Row[] = [];
let lastT = 0;
// Outflow and inflow not matched yet: the two ends of a hand-off are counted a tick apart.
let pendingOut = new Map<string, Map<string, number>>();
let pendingIn = new Map<string, Map<string, number>>();
let pendingAt = 0;

const add = (m: Map<string, Map<string, number>>, kind: string, node: string, n: number) => {
  if (n <= 0) return;
  const per = m.get(kind) ?? new Map<string, number>();
  per.set(node, (per.get(node) ?? 0) + n);
  m.set(kind, per);
};
const names = (events: ClusterEvent[], kind: string, node: string, since: number) => events.filter((e) => e.kind === kind && e.node_id === node && e.time_ms > since).map((e) => e.subject.split("/").at(-1) ?? e.subject);

const sides = (list: Flow[], side: "from" | "to"): Side[] => {
  const m = new Map<string, number>();
  for (const f of list) m.set(f[side], (m.get(f[side]) ?? 0) + f.n);
  return [...m].map(([node, n]) => ({ node, n }));
};
/** A row is one node and what it gave to, or got from, the others, over the ticks a leave or a join takes. */
const FOLD_MS = 15_000;
const rowKey = (r: Row) => `${r.kind}|${r.danger ?? ""}|${r.from.length === 1 ? `f:${r.from[0].node}` : `t:${r.to[0]?.node}`}`;
function push(row: Row) {
  const i = rows.findIndex((r) => rowKey(r) === rowKey(row) && row.t - r.since < FOLD_MS);
  const last = i >= 0 ? rows.splice(i, 1)[0] : null;
  if (last) {
    last.n += row.n;
    last.t = row.t;
    last.estimated ||= row.estimated;
    for (const side of ["from", "to"] as const)
      for (const d of row[side]) {
        const had = last[side].find((x) => x.node === d.node);
        if (had) had.n += d.n;
        else last[side].push(d);
      }
    last.names = [...new Set([...last.names, ...row.names])].slice(0, 6);
  }
  rows.unshift(last ?? row);
  if (rows.length > 60) rows.pop();
}

function observe(o: Overview) {
  if (o.timestamp_ms <= lastT) return;
  const sources = pendingOut;
  const sinks = pendingIn;
  const held = pendingAt > 0 && o.timestamp_ms - pendingAt > 8_000;
  const present = new Set<string>();
  for (const { node, snapshot } of o.nodes) {
    if (!snapshot?.hosted) continue;
    present.add(node.id);
    const now: Seen = { incarnation: snapshot.incarnation, kinds: new Map() };
    const before = seen.get(node.id);
    for (const k of snapshot.hosted.kinds) {
      const started = k.started ?? [];
      const ended = k.ended ?? [];
      now.kinds.set(k.kind, { keys: k.count ?? 0, started, ended });
      // A node seen for the first time after the first tick is a joiner, a new incarnation a restart: both count from zero.
      const was = before?.incarnation === now.incarnation ? before.kinds.get(k.kind) : lastT ? ZERO : undefined;
      if (!was) continue;
      add(sources, k.kind, node.id, (ended[MOVED] ?? 0) - (was.ended[MOVED] ?? 0));
      add(sinks, k.kind, node.id, (started[HANDOFF] ?? 0) - (was.started[HANDOFF] ?? 0));
      for (const [i, why] of Object.entries(DANGER)) {
        const n = (ended[+i] ?? 0) - (was.ended[+i] ?? 0);
        if (n <= 0) continue;
        push({ t: o.timestamp_ms, since: o.timestamp_ms, kind: k.kind, from: [{ node: node.id, n }], to: [], n, danger: why, names: names(o.events, "lease_lost", node.id, lastT).slice(0, 6), estimated: false });
      }
    }
    seen.set(node.id, now);
  }
  // A node that went takes its keys with it: they are the inflow's most likely source.
  for (const [id, s] of seen) {
    if (present.has(id)) continue;
    for (const [kind, k] of s.kinds) add(sources, kind, id, k.keys);
    seen.delete(id);
  }
  // What arrived is matched to what left, as much as both sides have; the rest waits for the other end's counter,
  // and inflow still without a source after a few ticks is drawn from nowhere.
  for (const [kind, inflow] of sinks) {
    const out = sources.get(kind) ?? new Map<string, number>();
    const inTotal = sum(inflow.values());
    const outTotal = sum(out.values());
    if (outTotal <= 0 && !held) continue;
    const from: [string, number][] = outTotal > 0 ? [...out] : [[LOST, 1]];
    const q = outTotal > 0 ? Math.min(inTotal, outTotal) : inTotal;
    const share = from.reduce((a, [, w]) => a + w, 0);
    const bySource: Flow[][] = [];
    for (const [s, w] of from) {
      const g: Flow[] = [];
      for (const [d, n] of inflow) {
        const k = Math.round((q * n * w) / (inTotal * share));
        if (k > 0) g.push({ kind, from: s, to: d, n: k });
      }
      if (g.length) bySource.push(g);
    }
    const here = bySource.flat();
    // One row per node that gave keys; one row when a single node got them all, as a joiner does.
    const groups = new Set(here.map((f) => f.to)).size === 1 ? [here] : bySource;
    for (const g of groups) {
      const handed = [...new Set(g.map((f) => f.from))].flatMap((s) => names(o.events, "handed_off", s, lastT));
      const shown = handed.length ? handed : [...new Set(g.map((f) => f.to))].flatMap((d) => names(o.events, "acquired", d, lastT));
      push({ t: o.timestamp_ms, since: o.timestamp_ms, kind, from: sides(g, "from"), to: sides(g, "to"), n: g.reduce((a, f) => a + f.n, 0), names: shown.slice(0, 6), estimated: from.length > 1 || from[0][0] === LOST });
    }
    for (const [d, n] of inflow) inflow.set(d, n - (q * n) / inTotal);
    for (const [s, w] of out) out.set(s, w - (q * w) / share);
    for (const m of [inflow, out]) for (const [k, n] of m) if (n < 1) m.delete(k);
    if (!inflow.size) sinks.delete(kind);
    if (!out.size) sources.delete(kind);
  }
  pendingOut = held ? new Map() : sources;
  pendingIn = sinks;
  pendingAt = pendingOut.size || pendingIn.size ? pendingAt || o.timestamp_ms : 0;
  lastT = o.timestamp_ms;
}

// The board: a card per node at a position kept in this browser, in a frame per region.
interface Pt {
  x: number;
  y: number;
}
const CW = 176;
const CH = 64;
const GX = 28;
const GY = 26;
const FRAME = 16;
const LABEL = 22;
const DRAWER = 400;
const POS_KEY = "axon.placement.pos";
const WINDOWS = { "2m": 120_000, "15m": 900_000, "1h": 3_600_000 } as const;
type Window = keyof typeof WINDOWS;

function loadPos(): Map<string, Pt> {
  try {
    return new Map(Object.entries(JSON.parse(localStorage.getItem(POS_KEY) ?? "{}") as Record<string, Pt>));
  } catch {
    return new Map();
  }
}
function savePos(pos: Map<string, Pt>) {
  try {
    localStorage.setItem(POS_KEY, JSON.stringify(Object.fromEntries(pos)));
  } catch {
    // A private window keeps nothing: the layout lasts for the tab.
  }
}

/** Gives a position to each node that has none: the next slot of its region's grid, a new region right of all. */
function place(boxes: Box[], pos: Map<string, Pt>) {
  if (boxes.every((b) => pos.has(b.id))) return;
  const groups = new Map<string, Box[]>();
  for (const b of boxes) {
    const list = groups.get(b.region);
    if (list) list.push(b);
    else groups.set(b.region, [b]);
  }
  const placedAll = boxes.filter((b) => pos.has(b.id)).map((b) => pos.get(b.id)!);
  let right = Math.max(0, ...placedAll.map((p) => p.x + CW));
  let bottom = Math.max(0, ...placedAll.map((p) => p.y + CH));
  let rowY = 0;
  let changed = false;
  for (const [, list] of [...groups].sort(([a], [b]) => natural(a, b))) {
    const placed = list.filter((b) => pos.has(b.id));
    const fresh = list.filter((b) => !pos.has(b.id)).sort((a, b) => natural(a.id, b.id));
    if (!fresh.length) continue;
    const cols = Math.max(2, Math.ceil(Math.sqrt(list.length)));
    // A new region goes right of the others, or on a new row once the board is about three cards wide.
    const wrap = right > 0 && right + 3 * GX + cols * (CW + GX) > 5 * (CW + GX);
    if (wrap && !placed.length) rowY = bottom + 3 * GY;
    const ox = placed.length ? Math.min(...placed.map((b) => pos.get(b.id)!.x)) : wrap ? 0 : right + (right ? 3 * GX : 0);
    const oy = placed.length ? Math.min(...placed.map((b) => pos.get(b.id)!.y)) : rowY;
    fresh.forEach((b, j) => {
      const i = placed.length + j;
      pos.set(b.id, { x: ox + (i % cols) * (CW + GX), y: oy + Math.floor(i / cols) * (CH + GY) });
    });
    right = wrap ? ox + cols * (CW + GX) : Math.max(right, ox + cols * (CW + GX));
    bottom = Math.max(bottom, oy + Math.ceil(list.length / cols) * (CH + GY));
    changed = true;
  }
  if (changed) savePos(pos);
}

interface Frame {
  name: string;
  x: number;
  y: number;
  w: number;
  h: number;
}
function frames(boxes: Box[], pos: Map<string, Pt>): Frame[] {
  const by = new Map<string, Frame>();
  for (const b of boxes) {
    const p = pos.get(b.id);
    if (!p) continue;
    const f = by.get(b.region);
    if (!f) by.set(b.region, { name: b.region, x: p.x, y: p.y, w: p.x + CW, h: p.y + CH });
    else {
      f.x = Math.min(f.x, p.x);
      f.y = Math.min(f.y, p.y);
      f.w = Math.max(f.w, p.x + CW);
      f.h = Math.max(f.h, p.y + CH);
    }
  }
  return [...by.values()].map((f) => ({ name: f.name, x: f.x - FRAME, y: f.y - FRAME - LABEL, w: f.w - f.x + 2 * FRAME, h: f.h - f.y + 2 * FRAME + LABEL }));
}

interface Edge {
  /** a < b; LOST stands for a node that left. */
  id: string;
  a: string;
  b: string;
  /** Keys per kind from a to b, and from b to a. */
  ab: Map<string, number>;
  ba: Map<string, number>;
  n: number;
  t: number;
}
/** The moves of the window as edges, one per node pair, both directions and every kind on it. */
function edgesOf(list: Row[]): Edge[] {
  const m = new Map<string, Edge>();
  for (const r of list) {
    if (r.danger) continue;
    const pairs: [string, string, number][] = r.from.length === 1 ? r.to.map((d) => [r.from[0].node, d.node, d.n]) : r.from.map((s) => [s.node, r.to[0].node, s.n]);
    for (const [from, to, n] of pairs) {
      const [a, b] = from < to ? [from, to] : [to, from];
      const id = `${a}|${b}`;
      let e = m.get(id);
      if (!e) m.set(id, (e = { id, a, b, ab: new Map(), ba: new Map(), n: 0, t: 0 }));
      const dir = from === a ? e.ab : e.ba;
      dir.set(r.kind, (dir.get(r.kind) ?? 0) + n);
      e.n += n;
      e.t = Math.max(e.t, r.t);
    }
  }
  return [...m.values()];
}

/** Keys that came to and left a node in the window. */
interface Traffic {
  in: number;
  out: number;
}

interface Rect {
  x: number;
  y: number;
  w: number;
  h: number;
}
type Port = "l" | "r" | "t" | "b";
const NORMAL: Record<Port, Pt> = { l: { x: -1, y: 0 }, r: { x: 1, y: 0 }, t: { x: 0, y: -1 }, b: { x: 0, y: 1 } };
const mid = (r: Rect): Pt => ({ x: r.x + r.w / 2, y: r.y + r.h / 2 });
const sideTo = (r: Rect, q: Pt): Port => {
  const c = mid(r);
  const dx = q.x - c.x;
  const dy = q.y - c.y;
  return Math.abs(dx) > Math.abs(dy) ? (dx > 0 ? "r" : "l") : dy > 0 ? "b" : "t";
};
const portAt = (r: Rect, side: Port, off: number): Pt =>
  side === "l" ? { x: r.x, y: r.y + r.h / 2 + off } : side === "r" ? { x: r.x + r.w, y: r.y + r.h / 2 + off } : side === "t" ? { x: r.x + r.w / 2 + off, y: r.y } : { x: r.x + r.w / 2 + off, y: r.y + r.h };

interface Wire {
  edge: Edge;
  d: string;
  pa: Pt;
  pb: Pt;
  mid: Pt;
  len: number;
}
/** Every edge from a port on one card's side to a port on the other's, the side that faces it; edges that share a side spread along it. */
function wires(edges: Edge[], pos: Map<string, Pt>): Wire[] {
  const rect = (id: string, other: Pt | null): Rect | null => {
    if (id === LOST) return other ? { x: other.x - 120, y: other.y - 80, w: 16, h: 16 } : null;
    const p = pos.get(id);
    return p ? { x: p.x, y: p.y, w: CW, h: CH } : null;
  };
  const ends: { edge: Edge; ra: Rect; rb: Rect; sa: Port; sb: Port }[] = [];
  for (const edge of edges) {
    const rb = rect(edge.b, null) ?? rect(edge.b, mid(rect(edge.a, null) ?? { x: 0, y: 0, w: 0, h: 0 }));
    const ra = rect(edge.a, rb && mid(rb));
    if (!ra || !rb) continue;
    ends.push({ edge, ra, rb, sa: sideTo(ra, mid(rb)), sb: sideTo(rb, mid(ra)) });
  }
  // The slot of each end along its side, by the angle toward the far end, so edges fan in and never share a pixel.
  const slots = new Map<string, number>();
  const bySide = new Map<string, { key: string; angle: number }[]>();
  for (const e of ends) {
    for (const [id, side, r, far] of [[e.edge.a, e.sa, e.ra, mid(e.rb)], [e.edge.b, e.sb, e.rb, mid(e.ra)]] as [string, Port, Rect, Pt][]) {
      const c = mid(r);
      const list = bySide.get(`${id}|${side}`) ?? [];
      list.push({ key: `${e.edge.id}|${id}`, angle: Math.atan2(far.y - c.y, far.x - c.x) });
      bySide.set(`${id}|${side}`, list);
    }
  }
  for (const [key, list] of bySide) {
    const side = key.slice(-1) as Port;
    list.sort((p, q) => (side === "l" || side === "t" ? q.angle - p.angle : p.angle - q.angle));
    const step = Math.min(10, (side === "l" || side === "r" ? CH - 12 : CW - 12) / Math.max(1, list.length));
    list.forEach((l, i) => slots.set(l.key, (i - (list.length - 1) / 2) * step));
  }
  return ends.map(({ edge, ra, rb, sa, sb }) => {
    const pa = portAt(ra, sa, slots.get(`${edge.id}|${edge.a}`) ?? 0);
    const pb = portAt(rb, sb, slots.get(`${edge.id}|${edge.b}`) ?? 0);
    const len = Math.hypot(pb.x - pa.x, pb.y - pa.y);
    const reach = Math.min(120, Math.max(24, len * 0.4));
    const c1 = { x: pa.x + NORMAL[sa].x * reach, y: pa.y + NORMAL[sa].y * reach };
    const c2 = { x: pb.x + NORMAL[sb].x * reach, y: pb.y + NORMAL[sb].y * reach };
    const at = (p: number, q: number, r: number, t: number) => 0.125 * p + 0.375 * q + 0.375 * r + 0.125 * t;
    return { edge, d: `M${pa.x} ${pa.y} C${c1.x} ${c1.y} ${c2.x} ${c2.y} ${pb.x} ${pb.y}`, pa, pb, mid: { x: at(pa.x, c1.x, c2.x, pb.x), y: at(pa.y, c1.y, c2.y, pb.y) }, len: len * 1.1 };
  });
}

function fitView(fs: Frame[], w: number, h: number): Camera {
  if (!fs.length || !w || !h) return { x: 0, y: 0, k: 1 };
  const x0 = Math.min(...fs.map((f) => f.x)) - 24;
  const y0 = Math.min(...fs.map((f) => f.y)) - 24;
  const x1 = Math.max(...fs.map((f) => f.x + f.w)) + 24;
  const y1 = Math.max(...fs.map((f) => f.y + f.h)) + 24;
  const k = clampZoom(Math.min(1.25, w / (x1 - x0), h / (y1 - y0)));
  return { k, x: (w - (x1 - x0) * k) / 2 - x0 * k, y: (h - (y1 - y0) * k) / 2 - y0 * k };
}

const STYLE = `
@keyframes axon-run { from { offset-distance: 0%; } to { offset-distance: 100%; } }
.axon-dot { animation: axon-run linear infinite; }
@media (prefers-reduced-motion: reduce) { .axon-dot { display: none; } }
`;

const Card = memo(function Card({ box, p, colors, kinds, selected, lod, alarm, maxKeys, age, flowIn, flowOut }: { box: Box; p: Pt; colors: Map<string, string>; kinds: string[]; selected: boolean; lod: boolean; alarm: string | undefined; maxKeys: number; age: number | null; flowIn: number; flowOut: number }) {
  const keys = kinds.reduce((a, k) => a + (box.byKind.get(k) ?? 0), 0);
  const opacity = age === null ? 1 : Math.max(0.15, 1 - age / GHOST_MS) * 0.7;
  const tone = box.isolated || alarm ? "border-danger" : selected ? "border-accent" : box.gone !== null ? "border-muted border-dashed" : !box.ready ? "border-warning" : "border-border";
  if (lod) {
    const r = keys ? 7 + 15 * Math.sqrt(keys / maxKeys) : 5;
    return <div data-node={box.id} title={`${box.id} · ${count(keys)} keys`} style={{ left: p.x + CW / 2 - r, top: p.y + CH / 2 - r, width: 2 * r, height: 2 * r, opacity, background: keys ? "var(--accent)" : "var(--panel-raised)" }} className={`absolute cursor-pointer rounded-full border-2 ${tone}`} />;
  }
  return (
    <div data-node={box.id} style={{ left: p.x, top: p.y, width: CW, height: CH, opacity, boxShadow: alarm ? "0 0 0 3px color-mix(in srgb, var(--danger) 45%, transparent)" : undefined }} className={`absolute cursor-grab select-none rounded-md border bg-panel shadow-float ${tone} ${selected ? "ring-1 ring-accent" : ""}`}>
      <div className="flex items-center gap-1.5 px-2 pt-1.5 text-[11px]">
        <Flag node={box.id} />
        <span className="mono truncate">{box.id}</span>
        <span className="mono ml-auto shrink-0">{count(keys)}</span>
      </div>
      <div className="mono flex h-[14px] items-center justify-end gap-1.5 px-2 text-[10px]">
        {alarm ? (
          <span className="truncate text-danger">{alarm}</span>
        ) : (
          <>
            {flowIn > 0 && <span className="rounded-sm bg-raised px-1 text-success">+{count(flowIn)} in</span>}
            {flowOut > 0 && <span className="rounded-sm bg-raised px-1 text-muted">−{count(flowOut)} out</span>}
          </>
        )}
      </div>
      <div className="flex items-center gap-2 px-2 text-[10px] text-muted">
        <span className="truncate">{box.role || "node"}</span>
        {box.acceptor && <span title="Paxos acceptor" className="rounded-sm border border-border px-1 leading-[14px]">acceptor</span>}
        {box.gone !== null ? <span className={box.drained ? "" : "text-danger"}>{box.drained ? "left" : "lost"} {ago(age ?? 0)}</span> : box.isolated ? <span className="text-danger">isolated</span> : !box.ready ? <span className="text-warning">not ready</span> : null}
      </div>
      <div className="mx-2 mt-1 flex h-[3px] overflow-hidden rounded-full bg-raised">{keys > 0 && kinds.map((k) => <span key={k} style={{ width: `${((box.byKind.get(k) ?? 0) / keys) * 100}%`, background: colors.get(k) }} />)}</div>
    </div>
  );
});

interface Gesture {
  kind: "pan" | "drag" | "pinch";
  id?: string;
  start: Pt;
  view0: Camera;
  pos0?: Pt;
  moved: boolean;
  dist0?: number;
  mid0?: Pt;
}

interface BoardProps {
  m: Model;
  kinds: string[];
  colors: Map<string, string>;
  edges: Edge[];
  alarms: Map<string, string>;
  flows: Map<string, Traffic>;
  /** The window of moves shown, in ms. */
  span: number;
  node: string;
  setNode: (id: string | null) => void;
  /** A node to bring to the middle, and a count that changes with each request. */
  focus: { id: string; n: number } | null;
  /** The overlays, kept clear of the drawer while it is open. */
  children?: React.ReactNode;
  drawer?: React.ReactNode;
}

function Board({ m, kinds, colors, edges, alarms, flows, span, node, setNode, focus, children, drawer }: BoardProps) {
  const el = useRef<HTMLDivElement>(null);
  const posRef = useRef<Map<string, Pt> | null>(null);
  const pos = (posRef.current ??= loadPos());
  const [tick, bump] = useReducer((x: number) => x + 1, 0);
  const [size, setSize] = useState({ w: 0, h: 0 });
  // Null: fitted to the nodes, until the first pan or zoom.
  const [view, setView] = useState<Camera | null>(null);
  const pointers = useRef(new Map<number, Pt>());
  const gesture = useRef<Gesture | null>(null);
  const focused = useRef(0);
  const now = useNow();

  // ponytail: positions are placed in render for the nodes that have none; the map is this browser's and never React state.
  place(m.boxes, pos);
  // eslint-disable-next-line react-hooks/exhaustive-deps -- tick: pos is mutated in place by a drag
  const fs = useMemo(() => frames(m.boxes, pos), [m.boxes, pos, tick]);
  // eslint-disable-next-line react-hooks/exhaustive-deps -- tick, as above
  const ws = useMemo(() => wires(edges, pos), [edges, pos, tick]);
  const [hover, setHover] = useState<{ edge?: string; node?: string } | null>(null);
  const hovered = (w: Wire) => (hover?.edge ? hover.edge === w.edge.id : hover?.node ? w.edge.a === hover.node || w.edge.b === hover.node : null);
  const tip = hover?.edge ? ws.find((w) => w.edge.id === hover.edge) : undefined;
  const shown = view ?? fitView(fs, size.w, size.h);
  // The wheel handler, bound once, fits from the frames of the latest render.
  const framesNow = useRef(fs);
  useEffect(() => {
    framesNow.current = fs;
  }, [fs]);
  const lod = shown.k < 0.4;
  const maxKeys = Math.max(1, ...m.boxes.map((b) => kinds.reduce((a, k) => a + (b.byKind.get(k) ?? 0), 0)));

  useEffect(() => {
    const e = el.current;
    if (!e) return;
    const ro = new ResizeObserver(([entry]) => {
      const next = { w: Math.round(entry.contentRect.width), h: Math.round(entry.contentRect.height) };
      setSize((was) => (was.w === next.w && was.h === next.h ? was : next));
    });
    ro.observe(e);
    const onWheel = (ev: WheelEvent) => {
      ev.preventDefault();
      const r = e.getBoundingClientRect();
      const factor = Math.exp(-ev.deltaY * (ev.ctrlKey ? 0.01 : 0.0015));
      setView((v) => zoomAt(v ?? fitView(framesNow.current, r.width, r.height), ev.clientX - r.left, ev.clientY - r.top, factor));
    };
    e.addEventListener("wheel", onWheel, { passive: false });
    return () => {
      ro.disconnect();
      e.removeEventListener("wheel", onWheel);
    };
  }, [pos]);

  useEffect(() => {
    if (!focus || focus.n === focused.current) return;
    focused.current = focus.n;
    const p = pos.get(focus.id);
    if (!p) return;
    const c = { x: p.x + CW / 2, y: p.y + CH / 2 };
    // A look at one node, from the user's own search: a render after the answer.
    const frame = requestAnimationFrame(() => setView((v) => {
      const k = Math.max(v?.k ?? 1, 0.8);
      return { k, x: size.w / 2 - c.x * k, y: size.h / 2 - c.y * k };
    }));
    return () => cancelAnimationFrame(frame);
  }, [focus, pos, size]);

  const client = (e: ReactPointerEvent) => ({ x: e.clientX, y: e.clientY });
  const onDown = (e: ReactPointerEvent<HTMLDivElement>) => {
    if (e.button !== 0 && e.pointerType === "mouse") return;
    // A captured pointer sends its click to the canvas, so a control on top of it would never see one.
    if ((e.target as Element).closest("button, a, input, select, textarea, [role=listbox]")) return;
    e.currentTarget.setPointerCapture(e.pointerId);
    pointers.current.set(e.pointerId, client(e));
    if (pointers.current.size === 2) {
      const [a, b] = [...pointers.current.values()];
      gesture.current = { kind: "pinch", start: a, view0: shown, moved: true, dist0: Math.hypot(b.x - a.x, b.y - a.y), mid0: { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 } };
      return;
    }
    const id = (e.target as HTMLElement).closest<HTMLElement>("[data-node]")?.dataset.node;
    gesture.current = id ? { kind: "drag", id, start: client(e), view0: shown, pos0: pos.get(id), moved: false } : { kind: "pan", start: client(e), view0: shown, moved: false };
  };
  const onMove = (e: ReactPointerEvent<HTMLDivElement>) => {
    if (!pointers.current.has(e.pointerId)) return;
    pointers.current.set(e.pointerId, client(e));
    const g = gesture.current;
    if (!g) return;
    const r = e.currentTarget.getBoundingClientRect();
    if (g.kind === "pinch" && pointers.current.size === 2 && g.dist0 && g.mid0) {
      const [a, b] = [...pointers.current.values()];
      const k = clampZoom((g.view0.k * Math.hypot(b.x - a.x, b.y - a.y)) / g.dist0);
      const mid = { x: (a.x + b.x) / 2 - r.left, y: (a.y + b.y) / 2 - r.top };
      const w = { x: (g.mid0.x - r.left - g.view0.x) / g.view0.k, y: (g.mid0.y - r.top - g.view0.y) / g.view0.k };
      setView({ k, x: mid.x - w.x * k, y: mid.y - w.y * k });
      return;
    }
    const dx = e.clientX - g.start.x;
    const dy = e.clientY - g.start.y;
    if (g.kind === "pan") setView({ ...g.view0, x: g.view0.x + dx, y: g.view0.y + dy });
    else if (g.kind === "drag" && g.id && g.pos0) {
      if (Math.hypot(dx, dy) > 3) g.moved = true;
      if (g.moved) {
        pos.set(g.id, { x: g.pos0.x + dx / g.view0.k, y: g.pos0.y + dy / g.view0.k });
        bump();
      }
    }
  };
  const onUp = (e: ReactPointerEvent<HTMLDivElement>) => {
    pointers.current.delete(e.pointerId);
    const g = gesture.current;
    if (g?.kind === "drag" && g.id) {
      if (g.moved) savePos(pos);
      else setNode(node === g.id ? null : g.id);
    }
    gesture.current = null;
    if (pointers.current.size === 1) {
      const [p] = [...pointers.current.values()];
      gesture.current = { kind: "pan", start: p, view0: shown, moved: true };
    }
  };

  const fit = () => setView(null);
  const zoom = (factor: number) => setView(zoomAt(shown, size.w / 2, size.h / 2, factor));
  const maxN = Math.max(1, ...edges.map((e) => e.n));
  const over = (e: ReactPointerEvent<HTMLDivElement>) => {
    const id = (e.target as HTMLElement).closest<HTMLElement>("[data-node]")?.dataset.node;
    setHover((h) => (h?.edge ? h : id ? (h?.node === id ? h : { node: id }) : h ? null : h));
  };

  // The minimap: every frame and node in a 160 by 100 box, the view a rectangle on it.
  const mm = useMemo(() => {
    if (!fs.length) return null;
    const x0 = Math.min(...fs.map((f) => f.x));
    const y0 = Math.min(...fs.map((f) => f.y));
    const x1 = Math.max(...fs.map((f) => f.x + f.w));
    const y1 = Math.max(...fs.map((f) => f.y + f.h));
    const s = Math.min(150 / Math.max(1, x1 - x0), 90 / Math.max(1, y1 - y0));
    return { x0, y0, s, w: (x1 - x0) * s + 10, h: (y1 - y0) * s + 10 };
  }, [fs]);
  const look = (e: ReactPointerEvent<SVGSVGElement>) => {
    if (!mm) return;
    const r = e.currentTarget.getBoundingClientRect();
    const w = { x: mm.x0 + (e.clientX - r.left - 5) / mm.s, y: mm.y0 + (e.clientY - r.top - 5) / mm.s };
    setView({ k: shown.k, x: size.w / 2 - w.x * shown.k, y: size.h / 2 - w.y * shown.k });
  };

  return (
    <div
      ref={el}
      role="application"
      aria-label="Placement board: drag to pan, wheel to zoom, drag a node to move it, click a node for its keys"
      className={`relative min-h-0 flex-1 touch-none overflow-hidden rounded-md border border-border bg-panel`}
      style={{ backgroundImage: "radial-gradient(var(--border) 1px, transparent 1px)", backgroundSize: `${24 * shown.k}px ${24 * shown.k}px`, backgroundPosition: `${shown.x}px ${shown.y}px` }}
      onPointerDown={onDown}
      onPointerMove={onMove}
      onPointerUp={onUp}
      onPointerCancel={onUp}
    >
      <style>{STYLE}</style>
      <div className="absolute top-0 left-0" style={{ transform: `translate(${shown.x}px, ${shown.y}px) scale(${shown.k})`, transformOrigin: "0 0" }} onPointerOver={over} onPointerLeave={() => setHover(null)}>
        {fs.map((f) => (
          <div key={f.name} className="absolute rounded-lg border border-dashed border-muted/60 bg-raised/30" style={{ left: f.x, top: f.y, width: f.w, height: f.h }}>
            <span className="mono absolute top-1 left-2.5 text-[11px] text-muted">{f.name}</span>
          </div>
        ))}
        <svg className="absolute top-0 left-0 overflow-visible" width={1} height={1} aria-hidden>
          {ws.map((w) => {
            const e = w.edge;
            const width = 1.5 + 2.5 * Math.sqrt(e.n / maxN);
            const age = Math.max(0.3, 1 - (now - e.t) / span);
            const h = hovered(w);
            const color = colors.get([...e.ab.entries(), ...e.ba.entries()].sort((p, q) => q[1] - p[1])[0]?.[0] ?? "") ?? "var(--muted)";
            const dur = w.len / 90;
            const dots = (dir: Map<string, number>, reverse: boolean) => {
              const n = sum(dir.values());
              if (!n) return null;
              const c = 1 + Math.round(3 * Math.sqrt(n / maxN));
              return Array.from({ length: c }, (_, i) => <circle key={`${reverse}${i}`} r={2.2} fill={color} className="axon-dot" style={{ offsetPath: `path("${w.d}")`, animationDuration: `${dur}s`, animationDelay: `${(-i * dur) / c}s`, animationDirection: reverse ? "reverse" : "normal" }} />);
            };
            return (
              <g key={e.id} opacity={h === false ? 0.12 : age} style={{ transition: "opacity 150ms" }}>
                {e.a === LOST && <circle cx={w.pa.x} cy={w.pa.y} r={8} fill="none" stroke="var(--muted)" strokeDasharray="3 3" />}
                <path d={w.d} fill="none" stroke={color} strokeWidth={width} strokeOpacity={h ? 0.9 : 0.35} strokeLinecap="round" />
                <path d={w.d} fill="none" stroke="transparent" strokeWidth={14} style={{ pointerEvents: "stroke" }} onPointerEnter={() => setHover({ edge: e.id })} onPointerLeave={() => setHover(null)} />
                {dots(e.ab, false)}
                {dots(e.ba, true)}
                {[w.pa, w.pb].map((p, i) => <circle key={i} cx={p.x} cy={p.y} r={2.5} fill="var(--panel)" stroke={color} strokeWidth={1.2} />)}
              </g>
            );
          })}
        </svg>
        {m.boxes.map((b) => {
          const p = pos.get(b.id);
          return p ? <Card key={b.id} box={b} p={p} colors={colors} kinds={kinds} selected={node === b.id} lod={lod} alarm={alarms.get(b.id)} maxKeys={maxKeys} age={b.gone === null ? null : Math.max(0, now - b.gone)} flowIn={flows.get(b.id)?.in ?? 0} flowOut={flows.get(b.id)?.out ?? 0} /> : null;
        })}
        {tip && (
          <div className="pointer-events-none absolute rounded-md border border-border bg-panel px-2 py-1 text-[11px] whitespace-nowrap shadow-float" style={{ left: tip.mid.x, top: tip.mid.y - 8, transform: `translate(-50%, -100%) scale(${1 / shown.k})`, transformOrigin: "50% 100%" }}>
            {[
              [tip.edge.a, tip.edge.b, tip.edge.ab],
              [tip.edge.b, tip.edge.a, tip.edge.ba],
            ].map(([from, to, per]) =>
              [...(per as Map<string, number>).entries()].map(([k, n]) => (
                <div key={`${from as string}${k}`} className="flex items-center gap-1.5">
                  <span className="size-2 rounded-sm" style={{ background: colors.get(k) }} />
                  <span className="mono">{from === LOST ? "a node that left" : (from as string)} → {to as string}</span>
                  <span className="mono ml-auto pl-3">{count(n)} {k}</span>
                </div>
              )),
            )}
          </div>
        )}
      </div>
      <div className="pointer-events-none absolute inset-y-0 left-0 transition-[right] duration-300 ease-out" style={{ right: node ? Math.min(DRAWER, size.w) : 0 }}>
        {children}
        <div className="pointer-events-none absolute right-3 bottom-3 flex items-end gap-2">
          <div className="pointer-events-auto flex flex-col gap-1">
            {(
              [
                ["Zoom in", "+", () => zoom(1.25)],
                ["Zoom out", "−", () => zoom(0.8)],
                ["Fit all nodes", "fit", fit],
              ] as [string, string, () => void][]
            ).map(([label, text, run]) => (
              <button key={label} type="button" aria-label={label} title={label} onClick={run} onPointerDown={(e) => e.stopPropagation()} className="mono flex h-7 min-w-7 items-center justify-center rounded-md border border-border bg-panel px-1.5 text-[11px] text-muted hover:text-text">
                {text}
              </button>
            ))}
          </div>
          {mm && (
            <svg role="img" aria-label="Minimap" width={mm.w} height={mm.h} onPointerDown={(e) => { e.stopPropagation(); look(e); }} className="pointer-events-auto cursor-pointer rounded-md border border-border bg-panel max-[700px]:hidden">
              <g transform={`translate(5 5) scale(${mm.s}) translate(${-mm.x0} ${-mm.y0})`}>
                {fs.map((f) => <rect key={f.name} x={f.x} y={f.y} width={f.w} height={f.h} fill="var(--panel-raised)" stroke="var(--border)" strokeWidth={1 / mm.s} />)}
                {m.boxes.map((b) => {
                  const p = pos.get(b.id);
                  return p ? <rect key={b.id} x={p.x} y={p.y} width={CW} height={CH} fill={b.gone === null ? (b.keys ? "var(--accent)" : "var(--muted)") : "var(--border)"} /> : null;
                })}
                <rect x={-shown.x / shown.k} y={-shown.y / shown.k} width={size.w / shown.k} height={size.h / shown.k} fill="none" stroke="var(--text)" strokeWidth={1.5 / mm.s} />
              </g>
            </svg>
          )}
        </div>
      </div>
      {drawer}
    </div>
  );
}

function Kinds({ kinds, colors, kind, setKind }: { kinds: Kind[]; colors: Map<string, string>; kind: string; setKind: (k: string | null) => void }) {
  return (
    <div role="group" aria-label="Kind" className="flex flex-wrap gap-1.5">
      {[null, ...kinds].map((k) => {
        const value = k?.kind ?? "";
        const on = kind === value;
        return (
          <button key={value} type="button" aria-pressed={on} onClick={() => setKind(on ? null : value)} className={`flex h-[26px] items-center gap-1.5 rounded-md border bg-panel px-2 text-[11px] ${on ? "border-accent text-text" : "border-border text-muted hover:text-text"}`}>
            {k && <span className="size-2 rounded-sm" style={{ background: colors.get(k.kind) }} />}
            <span className={k ? "mono" : ""}>{k ? k.kind : "All kinds"}</span>
            <Count>{count(k ? k.count : kinds.reduce((a, x) => a + x.count, 0))}</Count>
            {k?.eager && <span className="text-[10px] text-muted">eager</span>}
          </button>
        );
      })}
    </div>
  );
}

/** One side of a move; the count per node once the side has more than one. */
function Nodes({ list }: { list: Side[] }) {
  return list.map((d, i) => (
    <span key={d.node} className="flex items-baseline gap-1">
      {d.node === LOST ? <span className="text-muted">a node that left</span> : <NodeName id={d.node} />}
      {list.length > 1 && <span className="text-muted">{count(d.n)}</span>}
      {i < list.length - 1 && <span className="text-muted">·</span>}
    </span>
  ));
}

function MoveList({ list, colors }: { list: Row[]; colors: Map<string, string> }) {
  const now = useNow();
  return !list.length ? (
    <Empty title="No key moved yet" detail="A node that joins or leaves moves keys. They show here as they do." />
  ) : (
    <ul className="scroll-y flex-1">
      {list.map((r) => (
        <li key={`${rowKey(r)}|${r.since}`} className="border-b border-border px-3 py-1.5 text-[11px] last:border-b-0">
          <div className="flex items-baseline gap-2">
            <span className="mono flex items-center gap-1.5">
              {r.danger ? <Dot tone="danger" /> : <span className="size-2 shrink-0 rounded-sm" style={{ background: colors.get(r.kind) ?? "var(--edge)" }} />}
              {count(r.n)} {r.kind}
            </span>
            <span className="ml-auto shrink-0 text-[10px] text-muted">{ago(now - r.t)}</span>
          </div>
          <div className={`mono flex flex-wrap items-baseline gap-x-1.5 text-[10px] ${r.danger ? "text-danger" : ""}`}>
            {r.danger ? (
              <>
                {r.danger} on <NodeName id={r.from[0].node} />
              </>
            ) : (
              <>
                <Nodes list={r.from} />
                <span className="text-muted">→</span>
                <Nodes list={r.to} />
              </>
            )}
          </div>
          {r.names.length > 0 && (
            <div className="mono truncate text-[10px] text-muted" title={r.names.join(", ")}>
              {r.names.join(", ")}
              {r.n > r.names.length && " …"}
            </div>
          )}
        </li>
      ))}
    </ul>
  );
}

function useDebounced<T>(value: T, ms: number): T {
  const [slow, setSlow] = useState(value);
  useEffect(() => {
    const timer = setTimeout(() => setSlow(value), ms);
    return () => clearTimeout(timer);
  }, [value, ms]);
  return slow;
}

const PAGE = 100;

/** The keys of one kind on one node, a page at a time, from `prefix` on. */
function Keys({ m, kind, setKind, node }: { m: Model; kind: string; setKind: (k: string | null) => void; node: string }) {
  const [q, setQ] = useState("");
  const prefix = useDebounced(q.trim(), 300);
  const picked = kind || m.top;
  const scope = `${picked}|${prefix}|${node}`;
  const [stack, setStack] = useState<{ scope: string; pages: { after: string; after_node: string }[] }>({ scope, pages: [] });
  const cursors = stack.scope === scope ? stack.pages : [];
  const setCursors = (pages: { after: string; after_node: string }[]) => setStack({ scope, pages });
  const cursor = cursors.at(-1);
  const page = useQuery({
    queryKey: ["keys", picked, prefix, node, cursor],
    enabled: !!picked,
    refetchInterval: 10_000,
    placeholderData: keepPreviousData,
    queryFn: ({ signal }) => getJson<KeysPage>("keys", { kind: picked, prefix, node_id: node, limit: PAGE, ...cursor }, signal),
  });
  const list = page.data?.keys ?? [];
  const last = list.at(-1);
  const error = page.error?.message ?? Object.entries(page.data?.errors ?? {}).map(([n, e]) => `${n}: ${e}`)[0];
  return (
    <section aria-label="Keys" className="flex min-h-0 flex-1 flex-col">
      <div className="flex flex-wrap items-center gap-2 border-b border-border px-3 py-2">
        <h3 className="text-[11px]">Keys</h3>
        <div className="ml-auto flex items-center gap-2">
          <Select label="Kind" value={picked} onChange={setKind} options={m.kinds.map((k) => ({ value: k.kind, label: k.kind }))} />
          <Search label="Key starts with" value={q} onChange={setQ} className="w-[150px]" />
        </div>
      </div>
      {!picked ? (
        <Empty title="No kind registered" detail="A node registers a kind with cluster.kind(..).register." />
      ) : !list.length ? (
        <Empty title={page.isPending ? "Wait for the node" : error ? "Could not list the keys" : `No ${picked} key${prefix ? ` starts with ${prefix}` : " runs"} here`} detail={error} />
      ) : (
        <div className="scroll-y min-h-0 flex-1">
          <table>
            <thead>
              <tr>
                <th><div className="flex h-[28px] items-center px-3 text-[10px] text-muted">Key</div></th>
                <th style={{ width: "40%" }}><div className="flex h-[28px] items-center px-3 text-[10px] text-muted">Epoch</div></th>
              </tr>
            </thead>
            <tbody>
              {list.map((k) => (
                <tr key={k.key}>
                  <td className="truncate" title={k.key}>
                    <Link to={`/actors?${new URLSearchParams({ aq: `${picked}/${k.key}`, anode: k.node })}`} className="underline decoration-border underline-offset-2 hover:decoration-current">{k.key}</Link>
                  </td>
                  <td className="truncate text-muted">{k.epoch ?? "none"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <footer className="flex shrink-0 items-center gap-2 border-t border-border px-3 py-1.5 text-[10px] text-muted">
        <span className="truncate">{page.isPending && picked ? "Wait for the node" : `${list.length} keys${cursors.length ? ` · page ${cursors.length + 1}` : ""}${page.data?.more ? " · more follow" : ""}`}</span>
        <span className="ml-auto flex gap-1.5">
          <button type="button" disabled={!cursors.length} onClick={() => setCursors(cursors.slice(0, -1))} className="rounded-md border border-border px-2 py-0.5 hover:text-text">Back</button>
          <button type="button" disabled={!page.data?.more || !last} onClick={() => last && setCursors([...cursors, { after: last.key, after_node: last.node }])} className="rounded-md border border-border px-2 py-0.5 hover:text-text">Next {PAGE}</button>
        </span>
      </footer>
    </section>
  );
}

/** The side drawer of one node: its keys by kind, its moves, its key list. It keeps the last node while it slides out. */
function Drawer({ m, rows, colors, kind, setKind, node, setNode }: { m: Model; rows: Row[]; colors: Map<string, string>; kind: string; setKind: (k: string | null) => void; node: string; setNode: (id: string | null) => void }) {
  const [last, setLast] = useState(node);
  if (node && node !== last) setLast(node);
  useEscape(!!node, () => setNode(null));
  const id = node || last;
  const box = m.boxes.find((b) => b.id === id);
  const moves = rows.filter((r) => r.from.some((s) => s.node === id) || r.to.some((d) => d.node === id)).slice(0, 12);
  return (
    <aside aria-label="Node" aria-hidden={!node} style={{ width: DRAWER }} className={`absolute inset-y-0 right-0 z-20 flex max-w-full flex-col border-l border-border bg-panel shadow-float transition-transform duration-300 ease-out ${node ? "translate-x-0" : "translate-x-full"}`} onPointerDown={(e) => e.stopPropagation()}>
      {box && (
        <>
          <header className="flex items-center gap-2 border-b border-border px-3 py-2">
            <Flag node={box.id} />
            <h2 className="mono truncate text-[13px]">{box.id}</h2>
            <span className="text-[10px] text-muted">
              {box.role || "node"} · {box.region}
              {box.acceptor && " · acceptor"}
            </span>
            <button type="button" aria-label="Close" onClick={() => setNode(null)} className="ml-auto flex size-[22px] items-center justify-center rounded-md border border-border text-muted hover:text-text">×</button>
          </header>
          <div className="border-b border-border px-3 py-2 text-[11px]">
            <div className="flex items-baseline gap-2">
              <span className="mono text-[20px] leading-none">{count(box.keys)}</span>
              <span className="text-muted">keys</span>
              {box.gone !== null ? <span className={`ml-auto ${box.drained ? "text-muted" : "text-danger"}`}>{box.drained ? "left" : "lost"} <Ago at={box.gone} /></span> : box.isolated ? <span className="ml-auto text-danger">isolated</span> : !box.ready ? <span className="ml-auto text-warning">not ready</span> : <span className="ml-auto text-success">ready</span>}
            </div>
            <ul className="mt-2 flex flex-col gap-1">
              {[...box.byKind].map(([k, n]) => (
                <li key={k} className="mono flex items-center gap-2 text-[10px]">
                  <span className="size-2 rounded-sm" style={{ background: colors.get(k) }} />
                  <span className="w-[90px] truncate">{k}</span>
                  <span className="h-[3px] flex-1 overflow-hidden rounded-full bg-raised">
                    <span className="block h-full" style={{ width: `${box.keys ? (n / box.keys) * 100 : 0}%`, background: colors.get(k) }} />
                  </span>
                  <span className="w-[52px] text-right">{count(n)}</span>
                </li>
              ))}
              {!box.byKind.size && <li className="text-[10px] text-muted">No kind registered on this node</li>}
            </ul>
          </div>
          <section aria-label="Moves" className="flex max-h-[220px] min-h-0 flex-col border-b border-border">
            <h3 className="shrink-0 px-3 pt-2 pb-1 text-[11px]">Moves in and out</h3>
            {moves.length ? <MoveList list={moves} colors={colors} /> : <p className="px-3 pb-2 text-[10px] text-muted">No key moved to or from this node since this tab opened.</p>}
          </section>
          {box.gone === null ? <Keys m={m} kind={kind} setKind={setKind} node={box.id} /> : <p className="px-3 py-2 text-[10px] text-muted">The node is gone: its keys run elsewhere now.</p>}
        </>
      )}
    </aside>
  );
}

function Ago({ at }: { at: number }) {
  return <>{ago(useNow() - at)}</>;
}

/** Finds where one key runs: the first running key with the text, else api/key's answer for the exact name. */
function KeySearch({ m, kind, onFound }: { m: Model; kind: string; onFound: (node: string) => void }) {
  const [q, setQ] = useState("");
  const text = useDebounced(q.trim(), 400);
  const picked = kind || m.top;
  const [found, setFound] = useState<{ text: string; key: string; owner: string | null; running: boolean } | null>(null);
  // The url setter changes with the url, so the lookup must not depend on it.
  const notify = useRef(onFound);
  useEffect(() => {
    notify.current = onFound;
  }, [onFound]);
  useEffect(() => {
    if (!text || !picked) return;
    const ctl = new AbortController();
    getJson<KeysPage>("keys", { kind: picked, prefix: text, limit: 1 }, ctl.signal)
      .then((p) => (p.keys[0] ? { text, key: p.keys[0].key, owner: p.keys[0].node, running: true } : getJson<KeyLookup>("key", { kind: picked, key: text }, ctl.signal).then((k) => ({ text, key: text, owner: k.owner, running: k.running }))))
      .then((r) => {
        setFound(r);
        if (r.owner) notify.current(r.owner);
      })
      .catch(() => {
        if (!ctl.signal.aborted) setFound(null);
      });
    return () => ctl.abort();
  }, [text, picked]);
  const hit = text && found?.text === text ? found : null;
  return (
    <div className="flex flex-col items-end gap-1">
      <Search label={picked ? `Find a key of ${picked}` : "Find a key"} value={q} onChange={setQ} className="w-[220px] bg-panel max-[700px]:w-[170px]" />
      {hit && (
        <p className="mono max-w-[260px] truncate rounded-md border border-border bg-panel px-2 py-1 text-[10px]">
          {hit.running ? <Dot tone="success" /> : <Dot tone="muted" />}
          {hit.key} {hit.running ? "runs on" : hit.owner ? "would start on" : "has no node"} {hit.owner}
        </p>
      )}
    </div>
  );
}

const MiniCard = ({ name, keys, chip, tone = "border-border" }: { name: string; keys: string; chip?: ReactNode; tone?: string }) => (
  <span className={`inline-flex w-[120px] flex-col gap-1 rounded-md border bg-panel px-2 py-1.5 ${tone}`}>
    <span className="mono flex justify-between text-[10px]"><span>{name}</span><span>{keys}</span></span>
    {chip}
    <span className="block h-[3px] rounded-full bg-accent" />
  </span>
);

export function PlacementHelp() {
  return (
    <HelpDialog button="What is this" title="What the placement page shows">
      <Part title="A card is a node" picture={<span className="flex flex-col gap-1.5"><span className="text-[10px] text-muted">Reykjavík</span><MiniCard name="relay-0" keys="5968" /></span>}>
        A card shows a node, its role, the keys that it runs and a bar of these keys by kind. The cards stand in a frame for each region. Drag a card to move it. This browser keeps your layout. A node that left stays as a faded card for two minutes.
      </Part>
      <Part
        title="A line is a move"
        picture={
          <svg viewBox="0 0 200 40" className="h-10 w-full" aria-hidden>
            <circle cx="6" cy="20" r="3" fill="var(--edge)" />
            <path d="M6 20 C 70 20, 130 20, 194 20" stroke="var(--accent)" strokeOpacity="0.4" strokeWidth="2" fill="none" />
            {[50, 100, 150].map((x) => <circle key={x} cx={x} cy="20" r="2.5" fill="var(--accent)" />)}
            <circle cx="194" cy="20" r="3" fill="var(--edge)" />
          </svg>
        }
      >
        A line joins two nodes when keys went from one to the other. The dots move in the direction of the keys. A wide line is a large move. The line fades over the window. Put the pointer on a line to see the counts for each kind and each direction.
      </Part>
      <Part title="The counts are on the cards" picture={<MiniCard name="visitor-0" keys="1444" chip={<span className="mono self-end text-[9px]"><span className="text-success">+4332 in</span> <span className="text-muted">−2888 out</span></span>} />}>
        A card shows the keys that came in and went out in the window. Each node reports the keys that it lost and the keys that it got, but not the other node. When more than one node lost keys at the same time, the page calculates the source. The list of moves at the bottom shows the same moves with some key names.
      </Part>
      <Part title="Red is a loss" picture={<MiniCard name="relay-2" keys="6163" tone="border-danger shadow-[0_0_0_3px_color-mix(in_srgb,var(--danger)_45%,transparent)]" />}>
        A red ring on a card shows a key that stopped there: it failed, it lost its lease, or its node was isolated. An amber border shows a node that is not ready. Click a card to see its keys. Type the start of a key to find the node that runs it.
      </Part>
    </HelpDialog>
  );
}

export function PlacementPage() {
  const overview = useLive((s) => s.overview);
  const departed = useLive((s) => s.departed);
  const [kind, setKind] = useUrlState("kind", "");
  const [node, setNode] = useUrlState("node", "");
  const [win, setWin] = useUrlState<Window>("win", "2m");
  const [open, setOpen] = useState(false);
  const [focus, setFocus] = useState<{ id: string; n: number } | null>(null);
  const [theme] = useTheme();
  const accent = useAccent();
  const now = useNow();
  const m = useMemo(() => {
    if (overview) observe(overview);
    return overview ? { ...model(overview, departed), rows: [...rows] } : null;
  }, [overview, departed]);
  const colors = useMemo(() => {
    // Red stays for losses. theme and accent: palette() reads both from the document.
    const p = palette().filter((c) => c !== COLORS.rejected);
    return new Map((m?.kinds ?? []).map((k, i) => [k.kind, p[i % p.length]]));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [m?.kinds, theme, accent]);
  const kinds = useMemo(() => (m?.kinds ?? []).map((k) => k.kind).filter((k) => !kind || k === kind), [m?.kinds, kind]);
  // Rows are newest first, so the window is a prefix: the list changes when a row enters or leaves it, not each second.
  const since = now - WINDOWS[win];
  const inWindow = (m?.rows ?? []).filter((r) => r.t >= since).length;
  const list = useMemo(() => (m?.rows ?? []).slice(0, inWindow).filter((r) => !kind || r.kind === kind), [m?.rows, inWindow, kind]);
  const edges = useMemo(() => edgesOf(list), [list]);
  const alarms = useMemo(() => new Map(list.filter((r) => r.danger).map((r) => [r.from[0].node, r.danger!])), [list]);
  const flows = useMemo(() => {
    const f = new Map<string, Traffic>();
    const at = (id: string) => f.get(id) ?? f.set(id, { in: 0, out: 0 }).get(id)!;
    for (const r of list) {
      if (r.danger) continue;
      for (const s of r.from) at(s.node).out += s.n;
      for (const d of r.to) at(d.node).in += d.n;
    }
    return f;
  }, [list]);
  const onFound = useCallback(
    (id: string) => {
      setNode(id);
      setFocus((f) => ({ id, n: (f?.n ?? 0) + 1 }));
    },
    [setNode],
  );
  if (!m) return null;
  const total = m.kinds.filter((k) => kinds.includes(k.kind)).reduce((a, k) => a + k.count, 0);
  const serving = m.boxes.filter((b) => b.gone === null && kinds.some((k) => (b.byKind.get(k) ?? 0) > 0)).length;
  const minute = list.filter((r) => !r.danger && r.t > now - 60_000).reduce((a, r) => a + r.n, 0);
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <h1 className="sr-only-h1">Placement</h1>
      <Board m={m} kinds={kinds} colors={colors} edges={edges} alarms={alarms} flows={flows} span={WINDOWS[win]} node={node} setNode={setNode} focus={focus} drawer={<Drawer m={m} rows={m.rows} colors={colors} kind={kind} setKind={setKind} node={node} setNode={setNode} />}>
        <div className="pointer-events-none absolute inset-x-3 top-3 flex flex-wrap items-start justify-between gap-2">
          <div className="pointer-events-auto flex flex-col gap-2">
            <div className="flex items-baseline gap-2 rounded-md border border-border bg-panel px-2.5 py-1">
              <span className="mono text-[20px] leading-none">{count(total)}</span>
              <span className="text-[11px] text-muted">keys on {serving} of {m.members} nodes</span>
            </div>
            <Kinds kinds={m.kinds} colors={colors} kind={kind} setKind={setKind} />
          </div>
          <div className="pointer-events-auto">
            <KeySearch m={m} kind={kind} onFound={onFound} />
          </div>
        </div>
        <div className="pointer-events-none absolute bottom-3 left-3 flex max-h-[60%] w-[340px] max-w-[calc(100%-24px)] flex-col max-[700px]:max-w-[calc(100%-64px)]">
          <div className={`pointer-events-auto flex min-h-0 flex-col rounded-md border border-border bg-panel shadow-float ${open ? "" : "h-auto"}`}>
            <div className="flex shrink-0 items-center gap-2 py-1 pr-1.5">
              <button type="button" aria-expanded={open} onClick={() => setOpen(!open)} className="flex min-w-0 flex-1 items-center gap-2 px-3 py-0.5 text-left text-[11px]">
                <span className={`transition-transform ${open ? "" : "-rotate-90"}`}>
                  <Icon name="chevron" size={12} />
                </span>
                Keys that moved
                {minute > 0 && <Count>{count(minute)} in the last minute</Count>}
              </button>
              <Select label="Window" value={win} onChange={(v) => setWin(v)} options={(Object.keys(WINDOWS) as Window[]).map((w) => ({ value: w, label: w }))} />
            </div>
            {open && (
              <div className="flex min-h-0 flex-col border-t border-border">
                <MoveList list={list.slice(0, 40)} colors={colors} />
                <p className="shrink-0 border-t border-border px-3 py-1 text-[10px] text-muted">Since this tab opened · the source of a move is an estimate</p>
              </div>
            )}
          </div>
        </div>
      </Board>
    </div>
  );
}
