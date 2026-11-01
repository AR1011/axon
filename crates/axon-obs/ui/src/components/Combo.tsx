import { useEffect, useId, useState } from "react";
import { Count } from "./atoms";
import { Search } from "./Search";

export type Item = { value: string; about?: string; tag?: string; group?: string };

/** A search with a list under it, picked with the arrows and Enter or a click. Just focused, it lists everything under group headers; typed, what `items` finds for the text. Enter on text nothing matches submits the form. */
export function Combo({ label, value, onChange, items, onPick, current, select, empty, hint, className }: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  items: (typed: string) => Item[];
  onPick: (value: string, typed: boolean) => void;
  current?: string;
  select?: boolean;
  empty: string;
  hint: string;
  className: string;
}) {
  const id = useId();
  const [open, setOpen] = useState(false);
  const [typed, setTyped] = useState(false);
  const [active, setActive] = useState(0);
  const found = items(typed ? value : "");
  const at = Math.min(active, found.length - 1);
  useEffect(() => {
    // Scrolls the list alone: scrollIntoView would also scroll the page and its panels, sideways too.
    const item = open ? document.getElementById(`${id}-${at}`) : null;
    const list = item?.closest<HTMLElement>("[role=listbox]");
    if (!item || !list) return;
    if (item.offsetTop < list.scrollTop) list.scrollTop = item.offsetTop;
    else if (item.offsetTop + item.offsetHeight > list.scrollTop + list.clientHeight) list.scrollTop = item.offsetTop + item.offsetHeight - list.clientHeight;
  }, [open, at, id]);
  const pick = (picked: string) => {
    setOpen(false);
    setTyped(false);
    onPick(picked, typed);
  };
  return (
    <div className="relative max-[700px]:w-full">
      <Search
        label={label}
        value={value}
        onChange={(v) => {
          onChange(v);
          setTyped(true);
          setActive(0);
          setOpen(true);
        }}
        className={`${className} max-[700px]:w-full`}
        input={{
          role: "combobox",
          type: "text",
          "aria-expanded": open,
          "aria-controls": `${id}-list`,
          "aria-activedescendant": open && at >= 0 ? `${id}-${at}` : undefined,
          "aria-autocomplete": "list",
          autoComplete: "off",
          spellCheck: false,
          onFocus: (e) => {
            if (select) e.currentTarget.select();
            setTyped(false);
            setActive(Math.max(0, items("").findIndex((item) => item.value === current)));
            setOpen(true);
          },
          onClick: () => setOpen(true),
          onBlur: () => setOpen(false),
          onKeyDown: (e) => {
            if (e.key === "ArrowDown" || e.key === "ArrowUp") {
              e.preventDefault();
              if (!open) return setOpen(true);
              setActive(Math.max(0, Math.min(found.length - 1, at + (e.key === "ArrowDown" ? 1 : -1))));
            } else if (e.key === "Enter" && open && found[at]) {
              e.preventDefault();
              pick(found[at].value);
            } else if (e.key === "Escape" && open) {
              e.preventDefault();
              setOpen(false);
            }
          },
        }}
      >
        <Count>{found.length}</Count>
      </Search>
      {open && (
        <div onMouseDown={(e) => e.preventDefault()} className="absolute top-full left-0 z-30 mt-1 flex w-[440px] max-w-[calc(100vw-24px)] flex-col overflow-hidden rounded-lg border border-border bg-raised shadow-float">
          <ul id={`${id}-list`} role="listbox" aria-label={label} className="relative max-h-[min(340px,55vh)] overflow-y-auto p-1">
            {found.map((item, i) => (
              <li key={item.value} role="presentation">
                {!typed && item.group !== found[i - 1]?.group && <div className="px-2 pt-2 pb-1 text-[9px] font-[550] text-muted">{item.group}</div>}
                <div
                  id={`${id}-${i}`}
                  role="option"
                  aria-selected={i === at}
                  onMouseMove={() => i !== at && setActive(i)}
                  onClick={() => pick(item.value)}
                  className={`flex cursor-pointer items-baseline gap-2 rounded-sm px-2 py-1.5 ${i === at ? "bg-border" : ""}`}
                >
                  <span className="min-w-0 flex-1">
                    <span className={`mono block truncate text-[11px] ${item.value === current ? "text-accent" : "text-text"}`}>{item.value}</span>
                    {item.about && <span className="block truncate text-[10.5px] text-muted">{item.about}</span>}
                  </span>
                  <span className="shrink-0 text-[9px] text-muted">{item.tag}</span>
                </div>
              </li>
            ))}
          </ul>
          {found.length === 0 && <p className="px-3 py-3 text-[11px] text-muted">{empty}</p>}
          <div className="flex gap-3 border-t border-border px-3 py-1.5 text-[10px] text-muted">
            <span>↑↓ move</span>
            <span>↵ select</span>
            <span>esc close</span>
            <span className="ml-auto">{hint}</span>
          </div>
        </div>
      )}
    </div>
  );
}
