import { useCallback, useRef, useState } from "react";

export const SIDEBAR = { min: 160, max: 320, initial: 176, key: "axon-sidebar:v1" };

interface Preference {
  width: number;
  collapsed: boolean;
}

function read(): Preference {
  try {
    const stored = JSON.parse(localStorage.getItem(SIDEBAR.key) ?? "null");
    const width = typeof stored?.width === "number" && Number.isFinite(stored.width) ? stored.width : SIDEBAR.initial;
    return { width: Math.max(SIDEBAR.min, Math.min(SIDEBAR.max, width)), collapsed: stored?.collapsed === true };
  } catch {
    return { width: SIDEBAR.initial, collapsed: false };
  }
}

function write(preference: Preference) {
  try {
    localStorage.setItem(SIDEBAR.key, JSON.stringify(preference));
  } catch {
    /* storage may be unavailable */
  }
}

/** The preference is read during the first render so the first frame already has the stored geometry. */
export function useSidebar(shell: React.RefObject<HTMLDivElement | null>) {
  const [preference, setPreference] = useState(read);
  const drag = useRef<{ frame: number } | null>(null);

  const commit = useCallback((next: Preference) => {
    const bounded = { ...next, width: Math.max(SIDEBAR.min, Math.min(SIDEBAR.max, Math.round(next.width))) };
    setPreference(bounded);
    write(bounded);
  }, []);

  const paint = (width: number) => shell.current?.style.setProperty("--sidebar", `${width}px`);

  const onPointerDown = (event: React.PointerEvent<HTMLElement>) => {
    if (preference.collapsed) return;
    const handle = event.currentTarget;
    handle.setPointerCapture(event.pointerId);
    let width = preference.width;
    drag.current = { frame: 0 };
    const move = (e: PointerEvent) => {
      width = Math.max(SIDEBAR.min, Math.min(SIDEBAR.max, e.clientX));
      cancelAnimationFrame(drag.current!.frame);
      drag.current!.frame = requestAnimationFrame(() => paint(width));
    };
    const finish = (save: boolean) => {
      cancelAnimationFrame(drag.current?.frame ?? 0);
      drag.current = null;
      handle.removeEventListener("pointermove", move);
      handle.removeEventListener("pointerup", up);
      handle.removeEventListener("pointercancel", cancel);
      handle.removeEventListener("lostpointercapture", cancel);
      window.removeEventListener("keydown", key);
      window.removeEventListener("blur", cancel);
      if (save) commit({ width, collapsed: false });
      else paint(preference.width);
    };
    const up = () => finish(true);
    const cancel = () => finish(false);
    const key = (e: KeyboardEvent) => e.key === "Escape" && finish(false);
    handle.addEventListener("pointermove", move);
    handle.addEventListener("pointerup", up);
    handle.addEventListener("pointercancel", cancel);
    handle.addEventListener("lostpointercapture", cancel);
    window.addEventListener("keydown", key);
    window.addEventListener("blur", cancel);
  };

  const onKeyDown = (event: React.KeyboardEvent) => {
    const step = event.shiftKey ? 32 : 8;
    const width = { ArrowLeft: preference.width - step, ArrowRight: preference.width + step, Home: SIDEBAR.min, End: SIDEBAR.max }[event.key];
    if (width === undefined) return;
    event.preventDefault();
    commit({ width, collapsed: false });
  };

  return {
    ...preference,
    toggle: () => commit({ ...preference, collapsed: !preference.collapsed }),
    reset: () => commit({ width: SIDEBAR.initial, collapsed: false }),
    onPointerDown,
    onKeyDown,
  };
}
