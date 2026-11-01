import { useEffect, useId, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { Icon } from "./Icon";

export interface Option<T extends string> {
  value: T;
  label: string;
  preview?: React.ReactNode;
}

interface Props<T extends string> {
  label: string;
  value: T;
  options: Option<T>[];
  onChange: (value: T) => void;
  className?: string;
}

export function Select<T extends string>({ label, value, options, onChange, className = "" }: Props<T>) {
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const [place, setPlace] = useState<React.CSSProperties>({});
  const trigger = useRef<HTMLButtonElement>(null);
  const menu = useRef<HTMLUListElement>(null);
  const typed = useRef({ text: "", at: -Infinity });
  const id = useId();
  const selected = Math.max(0, options.findIndex((o) => o.value === value));

  const show = (index = selected) => {
    setActive(index);
    setOpen(true);
  };
  const close = (focus = true) => {
    setOpen(false);
    if (focus) trigger.current?.focus();
  };
  const commit = (index: number) => {
    const option = options[index];
    if (option) onChange(option.value);
    close();
  };

  useLayoutEffect(() => {
    if (!open) return;
    const position = () => {
      const box = trigger.current!.getBoundingClientRect();
      const height = Math.min(320, options.length * 30 + 8);
      const above = box.bottom + height + 8 > window.innerHeight && box.top > height;
      setPlace({ left: box.left, minWidth: Math.max(180, box.width), top: above ? box.top - height - 4 : box.bottom + 4, maxHeight: height });
    };
    position();
    const outside = (e: PointerEvent) => {
      if (!menu.current?.contains(e.target as Node) && !trigger.current?.contains(e.target as Node)) close(false);
    };
    window.addEventListener("scroll", position, true);
    window.addEventListener("resize", position);
    window.addEventListener("pointerdown", outside);
    return () => {
      window.removeEventListener("scroll", position, true);
      window.removeEventListener("resize", position);
      window.removeEventListener("pointerdown", outside);
    };
  }, [open, options.length]);

  useEffect(() => {
    if (open) menu.current?.querySelector(`[data-index="${active}"]`)?.scrollIntoView({ block: "nearest" });
  }, [open, active]);

  const onKey = (e: React.KeyboardEvent) => {
    const last = options.length - 1;
    const move = (index: number) => {
      e.preventDefault();
      if (open) setActive(index);
      else show(index);
    };
    if (e.key === "ArrowDown") move(open ? Math.min(last, active + 1) : selected);
    else if (e.key === "ArrowUp") move(open ? Math.max(0, active - 1) : selected);
    else if (e.key === "Home") move(0);
    else if (e.key === "End") move(last);
    else if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      if (open) commit(active);
      else show();
    } else if (e.key === "Escape" || e.key === "Tab") {
      if (open) {
        if (e.key === "Escape") e.preventDefault();
        close();
      }
    } else if (e.key.length === 1) {
      // Keys typed within half a second spell one prefix, as in a native select.
      typed.current = { text: (e.timeStamp - typed.current.at < 500 ? typed.current.text : "") + e.key.toLowerCase(), at: e.timeStamp };
      const found = options.findIndex((o) => o.label.toLowerCase().startsWith(typed.current.text));
      if (found >= 0) move(found);
    }
  };

  return (
    <>
      <button
        ref={trigger}
        type="button"
        role="combobox"
        aria-label={label}
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-controls={id}
        aria-activedescendant={open ? `${id}-${active}` : undefined}
        onClick={() => (open ? close() : show())}
        onKeyDown={onKey}
        className={`flex h-7 min-w-0 items-center gap-2 rounded-md border bg-panel px-2 text-[10px] ${open ? "border-muted" : "border-border"} ${className}`}
      >
        {options[selected]?.preview}
        <span className="truncate">{options[selected]?.label ?? label}</span>
        <span className="ml-auto text-muted">
          <Icon name="chevron" size={12} />
        </span>
      </button>
      {open &&
        createPortal(
          <ul ref={menu} id={id} role="listbox" aria-label={label} style={place} className="fixed z-50 overflow-y-auto rounded-lg border border-border bg-raised p-1 shadow-float">
            {options.map((option, index) => (
              <li
                key={option.value}
                id={`${id}-${index}`}
                data-index={index}
                role="option"
                aria-selected={index === selected}
                onPointerEnter={() => setActive(index)}
                onClick={() => commit(index)}
                className={`flex h-[30px] cursor-pointer items-center gap-2 rounded-sm px-2 text-[11px] ${index === active ? "bg-border" : ""} ${index === selected ? "text-accent" : ""}`}
              >
                {option.preview}
                <span className="truncate">{option.label}</span>
                {index === selected && (
                  <span className="ml-auto">
                    <Icon name="check" size={12} />
                  </span>
                )}
              </li>
            ))}
          </ul>,
          document.body,
        )}
    </>
  );
}
