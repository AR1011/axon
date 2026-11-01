export const UNKNOWN = "—";

export function sum(values: Iterable<number> = []): number {
  let total = 0;
  for (const v of values) total += v;
  return total;
}

const known = (n: number | null | undefined): n is number => n != null && Number.isFinite(n);

function scaled(n: number, units: string[], step: number): string {
  let i = 0;
  while (Math.abs(n) >= step && i < units.length - 1) {
    n /= step;
    i++;
  }
  return `${i === 0 || Math.abs(n) >= 100 ? Math.round(n) : n.toFixed(1)} ${units[i]}`;
}

export const bytes = (n?: number | null) => (known(n) ? scaled(n, ["B", "KiB", "MiB", "GiB", "TiB"], 1024) : UNKNOWN);

export function count(n?: number | null): string {
  if (!known(n)) return UNKNOWN;
  if (Math.abs(n) < 10_000) return Number.isInteger(n) ? String(n) : n.toFixed(1);
  return scaled(n, ["", "k", "M", "B"], 1000).replace(" ", "");
}

/** Always one decimal below 100, so a live rate does not flip between "7/s" and "7.0/s". */
export const perSecond = (n?: number | null) => (known(n) ? `${Math.abs(n) < 100 ? n.toFixed(1) : count(Math.round(n))}/s` : UNKNOWN);
export const bytesPerSecond = (n?: number | null) => (known(n) ? `${bytes(n)}/s` : UNKNOWN);
/** Small shares keep a second decimal, so an axis of tiny values does not read 0.0% on every tick. */
export const percent = (n?: number | null) => (known(n) ? `${n.toFixed(n !== 0 && Math.abs(n) < 1 ? 2 : 1)}%` : UNKNOWN);
export const duration = (ns?: number | null) => (known(ns) ? scaled(ns, ["ns", "µs", "ms", "s"], 1000) : UNKNOWN);

export function ago(ms: number): string {
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return `${Math.floor(s / 3600)}h ago`;
}

export function uptime(ms?: number | null): string {
  if (!known(ms)) return UNKNOWN;
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`;
}

export type Unit = "bytes" | "bytes/s" | "/s" | "count" | "%" | "ns" | "ratio";

export function byUnit(unit: Unit, n?: number | null): string {
  switch (unit) {
    case "bytes":
      return bytes(n);
    case "bytes/s":
      return bytesPerSecond(n);
    case "/s":
      return perSecond(n);
    case "%":
      return percent(n);
    case "ns":
      return duration(n);
    case "ratio":
      return known(n) ? percent(n * 100) : UNKNOWN;
    default:
      return count(n);
  }
}

/** Each reason that counted, by name: "auth 1 · version 3". `counts` is indexed like `names`. */
export const reasons = (names: string[], counts: number[]) => names.flatMap((name, i) => (counts[i] ? [`${name.replaceAll("_", " ")} ${count(counts[i])}`] : [])).join(" · ");

export const shortType = (actorType: string) => actorType.split("::").at(-1) ?? actorType;

/** Names as people count them: publisher-2 before publisher-10. */
export const natural = new Intl.Collator(undefined, { numeric: true }).compare;

/** A spread in the unit of the value it qualifies: "1.0 ms ± 0.2 ms", never "± 0 ns". */
export function spread(ns: number, plusMinus: number): string {
  const value = duration(ns);
  const unit = value.split(" ")[1];
  const scale = { ns: 1, "µs": 1e3, ms: 1e6, s: 1e9 }[unit] ?? 1;
  return `${value} ± ${(plusMinus / scale).toFixed(1)} ${unit}`;
}

const pad = (n: number) => String(n).padStart(2, "0");
/** One clock for the whole console: 24-hour, with the date only when the window needs it. */
export function clock(ms: number, span = 0, seconds = false): string {
  const d = new Date(ms);
  const time = `${pad(d.getHours())}:${pad(d.getMinutes())}${seconds ? `:${pad(d.getSeconds())}` : ""}`;
  return span > 43_200_000 ? `${d.toLocaleDateString(undefined, { month: "short", day: "numeric" })} ${time}` : time;
}

/** A round axis step in the unit's own base: powers of two for bytes. */
export function niceStep(max: number, binary: boolean, ticks = 3): number {
  const raw = max / ticks;
  if (!(raw > 0)) return 1;
  const base = binary ? 2 : 10;
  const magnitude = base ** Math.floor(Math.log(raw) / Math.log(base));
  const steps = binary ? [1, 2] : [1, 2, 5, 10];
  return magnitude * (steps.find((m) => m * magnitude >= raw) ?? base);
}
