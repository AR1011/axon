import { useRef, useState } from "react";
import { Icon } from "./Icon";
import { Segmented } from "./Segmented";
import { useTip } from "./tip";

type Format = "json" | "csv";
type Cell = string | number | boolean | null | undefined;

/** One table's export: its rows as the table has them, the filters that narrowed it, and the server's cap if one bit. */
export interface Dataset {
  name: string;
  /** Rows the download will hold; undefined while the table is still loading. */
  count?: number;
  /** The server stopped short: its bound, or a node that did not answer. */
  capped?: boolean;
  filters: (readonly [string, string] | false | "" | null | undefined)[];
  rows: () => object[] | Promise<object[]>;
  /** One document, written as it is: JSON only, no row wrapper. */
  raw?: string;
}

const KEY = "axon.export";
const remembered = (): Format => { try { return localStorage.getItem(KEY) === "csv" ? "csv" : "json"; } catch { return "json"; } };

/** Nested objects become dotted columns, arrays of numbers join with `|`, anything else is JSON. */
function flatten(row: object, prefix = "", out: Record<string, Cell> = {}): Record<string, Cell> {
  for (const [k, v] of Object.entries(row)) {
    const key = prefix ? `${prefix}.${k}` : k;
    if (v && typeof v === "object" && !Array.isArray(v)) flatten(v, key, out);
    else if (Array.isArray(v)) out[key] = v.every((x) => typeof x !== "object") ? v.join("|") : JSON.stringify(v);
    else out[key] = v as Cell;
  }
  return out;
}

export function csv(rows: object[]): string {
  const flat = rows.map((r) => flatten(r));
  const cols = [...new Set(flat.flatMap(Object.keys))];
  // A leading = + - @ in text would run as a formula in a spreadsheet; numbers keep their sign.
  const cell = (v: Cell) => {
    if (v == null) return "";
    const s = typeof v === "string" && /^[=+\-@]/.test(v) ? `'${v}` : String(v);
    return /[",\n\r]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
  };
  return [cols.join(","), ...flat.map((r) => cols.map((c) => cell(r[c])).join(","))].join("\n");
}

export function download(name: string, body: string, type: string) {
  const url = URL.createObjectURL(new Blob([body], { type }));
  Object.assign(document.createElement("a"), { href: url, download: name }).click();
  setTimeout(() => URL.revokeObjectURL(url), 60_000);
}

export const stamp = () => new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");

/** The icon button a table puts among its controls, and the dialog behind it. */
export function ExportButton({ dataset: d }: { dataset: Dataset }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const tip = useTip();
  const [format, setFormat] = useState<Format>(remembered);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const pick = (f: Format) => { setFormat(f); try { localStorage.setItem(KEY, f); } catch { /* fine */ } };
  const filters = d.filters.filter((f): f is readonly [string, string] => !!f);
  const file = `axon-${d.name.toLowerCase().replaceAll(" ", "-")}-${stamp()}.${d.raw ? "json" : format}`;
  const run = async () => {
    setBusy(true);
    setError("");
    try {
      const rows = await d.rows();
      if (d.raw) download(file, JSON.stringify(rows[0], null, 2), "application/json");
      else if (format === "csv") download(file, csv(rows) + (d.capped ? `\n# partial: ${rows.length} rows; the server stopped at its limit or a node did not answer` : ""), "text/csv");
      else download(file, JSON.stringify({ exported: new Date().toISOString(), dataset: d.name, filters: Object.fromEntries(filters), capped: !!d.capped, rows }, null, 2), "application/json");
      dialog.current?.close();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };
  const label = `Export ${d.name.toLowerCase()}`;
  return (
    <>
      <button type="button" aria-label="Export" {...tip.bind(label)} onClick={() => dialog.current?.showModal()} className="flex size-[25px] shrink-0 items-center justify-center rounded-sm text-muted hover:bg-raised hover:text-text">
        <Icon name="download" size={14} />
      </button>
      {tip.node}
      <dialog ref={dialog} aria-label="Export" onClick={(e) => e.target === dialog.current && dialog.current.close()} className="m-auto w-[420px] max-w-[calc(100vw-32px)] rounded-lg border border-border bg-panel p-0 font-sans text-[11px] text-text shadow-float backdrop:bg-black/60">
        <header className="flex items-center border-b border-border px-4 py-3">
          <div>
            <h2 className="text-[13px] font-medium">Export {d.name.toLowerCase()}</h2>
            <p className={`mt-0.5 mono text-[10px] ${d.capped ? "text-warning" : "text-muted"}`}>{d.raw ?? (d.count == null ? "…" : `${d.count}${d.capped ? "+" : ""} rows`)}{d.capped ? " · partial: the server stopped at its limit or a node did not answer" : ""}</p>
          </div>
          <form method="dialog" className="ml-auto"><button aria-label="Close" className="px-1 text-[14px] text-muted hover:text-text">×</button></form>
        </header>
        <div className="flex flex-col gap-3 px-4 py-3">
          {!d.raw && <div>
            <div className="mb-1.5 text-[10px] text-muted">{filters.length ? "With the current filters of the table" : "No filters: the whole table"}</div>
            <div className="flex min-h-[22px] flex-wrap gap-1.5">
              {filters.map(([k, v]) => (
                <span key={k} className="inline-flex h-[22px] items-center gap-1 rounded-md border border-border bg-raised px-2 text-[10px]"><span className="text-muted">{k}</span><span className="mono">{v}</span></span>
              ))}
              {!filters.length && <span className="text-[10px] text-muted">To export a subset, filter the table first.</span>}
            </div>
          </div>}
          {!d.raw && <div className="flex items-center gap-3">
            <span className="text-[10px] text-muted">Format</span>
            <Segmented small label="Export format" value={format} onChange={pick} options={[{ value: "json", label: "JSON" }, { value: "csv", label: "CSV" }]} />
            <span className="text-[10px] text-muted">{format === "csv" ? "one row per line, nested fields as dotted columns" : "an array of rows with every field"}</span>
          </div>}
          {error && <p className="text-[10px] text-warning">{error}</p>}
        </div>
        <footer className="flex items-center gap-3 border-t border-border px-4 py-3">
          <span className="mono truncate text-[10px] text-muted">{file}</span>
          <button type="button" disabled={busy || d.count === 0} onClick={run} className="ml-auto shrink-0 rounded-md bg-accent px-3.5 py-1.5 text-[11px] font-[550] text-bg hover:opacity-90 disabled:opacity-40">{busy ? "Wait…" : "Download"}</button>
        </footer>
      </dialog>
    </>
  );
}
