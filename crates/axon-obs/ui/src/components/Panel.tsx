import type { ReactNode } from "react";

interface Props {
  title?: ReactNode;
  controls?: ReactNode;
  footer?: ReactNode;
  children: ReactNode;
  label?: string;
  flat?: boolean;
  className?: string;
}

export function Panel({ title, controls, footer, children, label, flat, className = "" }: Props) {
  return (
    <section aria-label={label} className={`flex min-h-0 min-w-0 flex-col overflow-hidden bg-panel ${flat ? "" : "rounded-md border border-border"} ${className}`}>
      {(title || controls) && (
        <header className="flex min-h-[37px] shrink-0 flex-wrap items-center gap-2 border-b border-border px-3 py-1">
          {title && <h2 className="flex min-w-0 items-center gap-2 truncate">{title}</h2>}
          <div className="ml-auto flex items-center gap-2">{controls}</div>
        </header>
      )}
      {children}
      {footer && <footer className="flex shrink-0 items-center gap-2 border-t border-border px-[11px] py-1.5 text-[10px] text-muted">{footer}</footer>}
    </section>
  );
}
