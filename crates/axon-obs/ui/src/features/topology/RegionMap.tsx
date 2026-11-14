import { bytesPerSecond, NO_REGION } from "../../model/format";
import type { RegionEdge } from "../../model/regions";

const [W, H, R] = [240, 150, 52];

const name = (region: string) => region || NO_REGION;

/** The regions on a ring, and an edge between two of them with the bytes per second across it, both ways added. */
export function RegionMap({ edges }: { edges: RegionEdge[] }) {
  const regions = [...new Set(edges.flatMap((e) => [e.a, e.b]))].sort();
  const at = new Map(regions.map((r, i) => {
    const turn = (2 * Math.PI * i) / regions.length - Math.PI / 2;
    return [r, [W / 2 + R * 1.6 * Math.cos(turn), H / 2 + R * Math.sin(turn)]] as const;
  }));
  return (
    <svg data-regions width={W} height={H} role="group" aria-label="Bytes per second between regions" className="absolute left-2 top-2 rounded-md border border-border bg-panel">
      {edges.map((e) => {
        const [[x1, y1], [x2, y2]] = [at.get(e.a)!, at.get(e.b)!];
        const label = `${name(e.a)} → ${name(e.b)} ${bytesPerSecond(e.ab)} · ${name(e.b)} → ${name(e.a)} ${bytesPerSecond(e.ba)}`;
        return (
          <g key={e.id} data-region-edge={e.id.replace("\n", ">")} aria-label={label}>
            <title>{label}</title>
            <line x1={x1} y1={y1} x2={x2} y2={y2} stroke="var(--edge)" strokeWidth="1.5" />
            <text x={(x1 + x2) / 2} y={(y1 + y2) / 2 - 3} textAnchor="middle" fill="var(--text)" fontSize="9" fontFamily="var(--font-mono)">{bytesPerSecond(e.ab + e.ba)}</text>
          </g>
        );
      })}
      {regions.map((r) => {
        const [x, y] = at.get(r)!;
        return (
          <g key={r}>
            <circle cx={x} cy={y} r="4" fill="var(--accent)" />
            <text x={x} y={y < H / 2 ? y - 8 : y + 15} textAnchor="middle" fill="var(--muted)" fontSize="10">{name(r)}</text>
          </g>
        );
      })}
    </svg>
  );
}
