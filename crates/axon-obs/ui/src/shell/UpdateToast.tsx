import { useState } from "react";
import { useLive } from "../store";

/** The hash in this page's own bundle name; the dashboard reports the one it serves now. */
const BUNDLE = document.querySelector<HTMLScriptElement>('script[type="module"]')?.src.match(/index-([\w-]+)\.js/)?.[1];

export function UpdateToast() {
  const build = useLive((s) => s.overview?.build);
  const [dismissed, setDismissed] = useState<string | null>(null);
  if (!BUNDLE || !build || build === BUNDLE || dismissed === build) return null;
  return (
    <div role="status" className="fixed right-4 bottom-4 z-50 w-[300px] rounded-lg border border-border bg-raised p-3 font-sans shadow-float">
      <p className="text-[12px] font-[550]">New version available</p>
      <p className="mt-0.5 text-[10px] leading-[1.5] text-muted">Refresh to load it. You stay on this page.</p>
      <div className="mt-2.5 flex justify-end gap-1.5">
        <button type="button" onClick={() => setDismissed(build)} className="rounded-md border border-border px-2.5 py-1 text-[11px] text-muted hover:text-text">
          Later
        </button>
        <button type="button" onClick={() => location.reload()} className="rounded-md bg-accent px-2.5 py-1 text-[11px] font-[550] text-bg hover:opacity-90">
          Refresh
        </button>
      </div>
    </div>
  );
}
