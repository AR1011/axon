import { useRef, type ReactNode } from "react";
import { Icon } from "./Icon";
import { useTip } from "./tip";

/** One point of a help dialog: a small drawing beside the words. */
export function Part({ title, children, picture }: { title: string; children: ReactNode; picture?: ReactNode }) {
  return (
    <section className={`grid items-center gap-4 border-t border-border py-3 first:border-t-0 max-[560px]:grid-cols-1 ${picture ? "grid-cols-[minmax(0,1fr)_minmax(0,1fr)]" : ""}`}>
      {picture && <div>{picture}</div>}
      <div>
        <h3 className="mb-1 text-[12px] font-medium">{title}</h3>
        <p className="leading-[1.6] text-muted">{children}</p>
      </div>
    </section>
  );
}

export function HelpDialog({ button, title, children }: { button: string; title: string; children: ReactNode }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const tip = useTip();
  return (
    <>
      {tip.node}
      <button type="button" aria-label={button} {...tip.bind(button)} onClick={() => dialog.current?.showModal()} className="flex size-[22px] shrink-0 items-center justify-center rounded-md border border-border text-muted hover:text-text">
        <Icon name="info" size={14} />
      </button>
      <dialog
        ref={dialog}
        aria-label={title}
        onClick={(e) => e.target === dialog.current && dialog.current.close()}
        className="m-auto w-[640px] max-w-[calc(100vw-32px)] rounded-lg border border-border bg-panel p-0 font-sans text-[11px] text-text shadow-float backdrop:bg-black/60"
      >
        <header className="flex items-center border-b border-border px-4 py-2.5">
          <h2 className="text-[13px] font-medium">{title}</h2>
          <form method="dialog" className="ml-auto">
            <button aria-label="Close" className="px-1 text-[14px] text-muted hover:text-text">
              ×
            </button>
          </form>
        </header>
        <div className="px-4 pb-2">{children}</div>
      </dialog>
    </>
  );
}
