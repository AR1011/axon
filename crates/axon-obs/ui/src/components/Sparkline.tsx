import type { P } from "../model/series";

export function Sparkline({ points, label, height = 32 }: { points: P[]; label: string; height?: number }) {
  const known = points.filter((p): p is [number, number] => p[1] !== null);
  const [t0, t1] = [points[0]?.[0] ?? 0, points.at(-1)?.[0] ?? 1];
  const top = Math.max(...known.map((p) => p[1]), 1e-9);
  const x = (t: number) => (t1 > t0 ? ((t - t0) / (t1 - t0)) * 128 : 64);
  const y = (v: number) => 30 - (v / top) * 27;
  let path = "";
  let pen = false;
  for (const [t, v] of points) {
    if (v === null) pen = false;
    else {
      path += `${pen ? "L" : "M"}${x(t).toFixed(1)} ${y(v).toFixed(1)}`;
      pen = true;
    }
  }
  return (
    <svg role="img" viewBox="0 0 128 32" preserveAspectRatio="none" width="100%" height={height} className="block">
      <title>{known.length ? `${label} · ${Math.min(...known.map((p) => p[1])).toLocaleString(undefined, { maximumFractionDigits: 1 })} to ${top.toLocaleString(undefined, { maximumFractionDigits: 1 })}` : label}</title>
      <line x1="0" x2="128" y1="31" y2="31" stroke="var(--border)" strokeWidth="1" vectorEffect="non-scaling-stroke" />
      {known.length === 1 ? (
        <circle cx={x(known[0][0])} cy={y(known[0][1])} r="1.8" fill="var(--accent)" />
      ) : (
        <path d={path} fill="none" stroke="var(--accent)" strokeWidth="1.25" vectorEffect="non-scaling-stroke" />
      )}
    </svg>
  );
}
