import type { ReactNode } from "react";

export const GRAPH_BUTTON = "flex h-[25px] min-w-7 items-center justify-center rounded-sm px-1.5 text-[10px] text-muted hover:bg-raised hover:text-text";

/** A graph's zoom bar: zoom, fit, and a reset of the cards dragged off the layout; `children` add buttons at its end. */
export function GraphControls({ k, zoom, fit, drag, children }: { k: number; zoom: (factor: number) => void; fit: () => void; drag: { arranged: boolean; reset: () => void }; children?: ReactNode }) {
  return (
    <div data-no-pan className="absolute right-3 bottom-3 flex items-center gap-px rounded-lg border border-border bg-panel p-0.5 shadow-[0_2px_6px_#0000000c]">
      <span aria-live="polite" className="mono w-10 text-center text-[10px] text-muted">{Math.round(k * 100)}%</span>
      <button type="button" aria-label="Zoom out" title="Zoom out" onClick={() => zoom(1 / 1.2)} className={GRAPH_BUTTON}>−</button>
      <button type="button" aria-label="Zoom in" title="Zoom in" onClick={() => zoom(1.2)} className={GRAPH_BUTTON}>+</button>
      <button type="button" onClick={fit} className={GRAPH_BUTTON}>Fit</button>
      <button type="button" disabled={!drag.arranged} title={drag.arranged ? "Move each node back to its position in the layout" : "Nothing to reset: drag a node to move it"} onClick={drag.reset} className={`${GRAPH_BUTTON} disabled:opacity-40`}>Reset layout</button>
      {children}
    </div>
  );
}
