import { CARD, type Box } from "../../model/topology";

export interface Routed {
  d: string;
  label: [number, number];
}

type Side = "top" | "bottom" | "left" | "right";
interface End {
  link: number;
  first: boolean;
  /** Where the other endpoint lies along this side, which orders the ports so that edges fan out without crossing. */
  toward: number;
}

/**
 * Every edge gets its own port on the card side it leaves from, so edges sharing a node separate at
 * the card instead of stacking on its centre. Neighbours in a row join side to side; the rest of a
 * row arcs underneath, deeper the farther it reaches, so arcs nest.
 */
export function routeEdges(links: { source: string; target: string }[], positions: Map<string, Box>): (Routed | null)[] {
  const ports = new Map<string, End[]>();
  const sides: ([Side, Side] | null)[] = links.map((link, index) => {
    const [a, b] = [positions.get(link.source), positions.get(link.target)];
    if (!a || !b || link.source === link.target) return null;
    let pair: [Side, Side];
    if (Math.abs(a.y - b.y) >= 1) pair = a.y < b.y ? ["bottom", "top"] : ["top", "bottom"];
    else {
      const [lo, hi] = [Math.min(a.x, b.x), Math.max(a.x, b.x)];
      const between = [...positions.values()].some((p) => Math.abs(p.y - a.y) < 1 && p.x > lo && p.x < hi);
      pair = between ? ["bottom", "bottom"] : a.x < b.x ? ["right", "left"] : ["left", "right"];
    }
    const vertical = pair[0] === "left" || pair[0] === "right";
    const add = (node: string, side: Side, first: boolean, other: Box) => {
      const key = `${node}|${side}`;
      ports.set(key, [...(ports.get(key) ?? []), { link: index, first, toward: vertical ? other.y : other.x }]);
    };
    add(link.source, pair[0], true, b);
    add(link.target, pair[1], false, a);
    return pair;
  });

  const at = new Map<string, [number, number]>();
  for (const [key, ends] of ports) {
    const [node, side] = key.split("|") as [string, Side];
    const box = positions.get(node)!;
    ends.sort((p, q) => p.toward - q.toward || p.link - q.link);
    ends.forEach((end, i) => {
      const along = (i + 1) / (ends.length + 1);
      const point: [number, number] =
        side === "top" || side === "bottom" ? [box.x + CARD.w * (0.1 + 0.8 * along), box.y + (side === "bottom" ? CARD.h : 0)] : [box.x + (side === "right" ? CARD.w : 0), box.y + CARD.h * (0.15 + 0.7 * along)];
      at.set(`${end.link}|${end.first}`, point);
    });
  }

  const loops = new Map<string, number>();
  return links.map((link, index) => {
    const a = positions.get(link.source);
    if (!a || !positions.get(link.target)) return null;
    if (link.source === link.target) {
      const n = loops.get(link.source) ?? 0;
      loops.set(link.source, n + 1);
      const [x, y, r] = [a.x + CARD.w, a.y + CARD.h / 2, 26 + n * 14];
      return { d: `M${x} ${y - 10} C${x + r * 1.6} ${y - r}, ${x + r * 1.6} ${y + r}, ${x} ${y + 10}`, label: [x + r * 1.25, y] };
    }
    const [[x1, y1], [x2, y2]] = [at.get(`${index}|true`)!, at.get(`${index}|false`)!];
    const side = sides[index]![0];
    if (side === "left" || side === "right") {
      const mid = (x1 + x2) / 2;
      return { d: `M${x1} ${y1} C${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`, label: [mid, (y1 + y2) / 2 - 5] };
    }
    if (sides[index]![1] === side) {
      const bend = 22 + Math.abs(x2 - x1) * 0.2;
      return { d: `M${x1} ${y1} C${x1} ${y1 + bend}, ${x2} ${y2 + bend}, ${x2} ${y2}`, label: [(x1 + x2) / 2, y1 + bend * 0.75] };
    }
    const mid = (y1 + y2) / 2;
    return { d: `M${x1} ${y1} C${x1} ${mid}, ${x2} ${mid}, ${x2} ${y2}`, label: [(x1 + x2) / 2, mid] };
  });
}
