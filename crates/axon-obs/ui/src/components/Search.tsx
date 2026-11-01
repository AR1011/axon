import type { InputHTMLAttributes, ReactNode } from "react";
import { Icon } from "./Icon";

type Extra = Omit<InputHTMLAttributes<HTMLInputElement>, "value" | "onChange" | "className" | "aria-label" | "placeholder">;

/** A search field; `input` passes further attributes to the field, as a combobox needs, and `children` follow it. */
export function Search({ label, value, onChange, className = "", input, children }: { label: string; value: string; onChange: (value: string) => void; className?: string; input?: Extra; children?: ReactNode }) {
  return (
    <label className={`flex h-7 min-w-0 items-center gap-1.5 rounded-md border border-border px-2 text-muted focus-within:border-accent ${className}`}>
      <Icon name="search" size={12} />
      <input
        type="search"
        aria-label={label}
        placeholder={label}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full min-w-0 bg-transparent text-[11px] text-text outline-none placeholder:text-muted"
        {...input}
      />
      {children}
    </label>
  );
}
