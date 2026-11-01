import { useEffect, useRef } from "react";
import { useAccent, useTheme } from "../hooks";

type Instance = { setOption: (option: unknown, settings: unknown) => void; resize: () => void; dispose: () => void; on: (event: "click", handler: (item: { data: unknown }) => void) => void };

/** `build` must be memoised by the caller: a new function is a new option. `onClick` gets the clicked item's data. */
export function Chart({ build, label, onClick }: { build: (theme: "dark" | "light") => unknown; label: string; onClick?: (data: unknown) => void }) {
  const host = useRef<HTMLDivElement>(null);
  const chart = useRef<Instance | null>(null);
  const latest = useRef(build);
  const clicked = useRef(onClick);
  const [theme] = useTheme();
  const accent = useAccent();

  useEffect(() => {
    clicked.current = onClick;
  }, [onClick]);

  useEffect(() => {
    latest.current = build;
    chart.current?.setOption(build(theme), { notMerge: false, replaceMerge: ["series", "visualMap"] });
  }, [build, theme, accent]);

  useEffect(() => {
    let gone = false;
    let observer: ResizeObserver | undefined;
    void import("./echarts").then(({ init }) => {
      if (gone || !host.current) return;
      const instance = init(host.current) as unknown as Instance;
      chart.current = instance;
      instance.setOption(latest.current(document.documentElement.dataset.theme === "light" ? "light" : "dark"), { notMerge: true });
      instance.on("click", (item) => clicked.current?.(item.data));
      observer = new ResizeObserver(() => instance.resize());
      observer.observe(host.current);
    });
    return () => {
      gone = true;
      observer?.disconnect();
      chart.current?.dispose();
      chart.current = null;
    };
  }, []);

  return <div ref={host} role="img" aria-label={label} className="size-full min-h-0" />;
}
