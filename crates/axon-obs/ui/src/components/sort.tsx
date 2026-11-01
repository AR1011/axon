import { useMemo } from "react";
import { useUrlState } from "../hooks";
import { natural } from "../model/format";

type Key = string | number | null | undefined;
export type Accessors<T> = Record<string, (row: T) => Key>;

const compare = <T,>(read: (row: T) => Key, sign: number) => (a: T, b: T) => {
  const [x, y] = [read(a), read(b)];
  if (x == null || y == null) return x == null ? (y == null ? 0 : 1) : -1;
  return sign * (typeof x === "number" && typeof y === "number" ? x - y : natural(String(x), String(y)));
};

/**
 * Numeric-aware, nulls last in both directions; ties go by the initial field in the initial order, so rows that a live
 * frame reorders hold still where that field tells them apart. `fields` must be a stable object.
 */
export function useSort<T>(rows: T[], fields: Accessors<T>, prefix: string, initial: string, initialOrder: "asc" | "desc" = "asc") {
  const [field, setField] = useUrlState(`${prefix}sort`, initial);
  const [order, setOrder] = useUrlState<"asc" | "desc">(`${prefix}order`, initialOrder);
  const sorted = useMemo(() => {
    const read = fields[field] ?? fields[initial];
    const first = compare(read, order === "desc" ? -1 : 1);
    const tie = read === fields[initial] ? () => 0 : compare(fields[initial], initialOrder === "desc" ? -1 : 1);
    return [...rows].sort((a, b) => first(a, b) || tie(a, b));
  }, [rows, fields, field, order, initial, initialOrder]);
  const toggle = (next: string) => {
    if (next === field) setOrder(order === "asc" ? "desc" : "asc");
    else {
      setField(next);
      setOrder("asc");
    }
  };
  return { sorted, field, order, toggle };
}

export function SortHead({ field, label, sort, width, align, title = label }: { field: string; label: string; sort: { field: string; order: "asc" | "desc"; toggle: (field: string) => void }; width?: string; align?: "right"; title?: string }) {
  const active = sort.field === field;
  return (
    <th style={{ width }} aria-sort={active ? (sort.order === "asc" ? "ascending" : "descending") : "none"}>
      <button type="button" title={title} onClick={() => sort.toggle(field)} className={`flex h-[30px] w-full items-center gap-1 px-3 text-[10px] text-muted hover:text-text ${align === "right" ? "justify-end" : ""}`}>
        <span className="truncate">{label}</span>
        <span className={active ? "text-text" : "opacity-40"}>{active ? (sort.order === "asc" ? "↑" : "↓") : "↕"}</span>
      </button>
    </th>
  );
}

export function Head({ label, width, align }: { label: string; width?: string; align?: "right" }) {
  return (
    <th style={{ width }}>
      <div className={`flex h-[30px] items-center px-3 text-[10px] text-muted ${align === "right" ? "justify-end" : ""}`}>{label}</div>
    </th>
  );
}
