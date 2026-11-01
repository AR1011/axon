import { Suspense, useEffect, useRef, useState } from "react";
import { Outlet, useLocation } from "react-router";
import { Empty } from "../components/atoms";
import { pending, subscribe, useLive } from "../store";
import { Kpis } from "./Kpis";
import { Sidebar } from "./Sidebar";
import { TopBar } from "./TopBar";
import { UpdateToast } from "./UpdateToast";
import { useSidebar } from "./useSidebar";

export function Shell() {
  const shell = useRef<HTMLDivElement>(null);
  const sidebar = useSidebar(shell);
  const [generation, setGeneration] = useState(0);
  const path = useLocation().pathname.replace(/\/+$/, "");
  const kpis = ["/overview", "/nodes", "/actors"].includes(path);
  const full = !["/overview", "/settings"].includes(path);
  // Traces, dead letters and settings ask the server for their own rows and need no overview.
  const noOverview = useLive(pending);
  const waiting = /^\/(settings|traces|dead-letters)(\/|$)/.test(path) ? null : noOverview;

  useEffect(() => subscribe(), [generation]);

  return (
    <div ref={shell} className="shell" data-collapsed={sidebar.collapsed} style={{ "--sidebar": `${sidebar.width}px` } as React.CSSProperties}>
      <a href="#content" onClick={(e) => { e.preventDefault(); document.getElementById("content")?.focus(); }} className="fixed top-1 left-1 z-50 -translate-y-10 rounded-md bg-accent px-3 py-1.5 text-[11px] font-[550] text-bg focus:translate-y-0">Skip to content</a>
      <Sidebar sidebar={sidebar} />
      <UpdateToast />
      <div id="workspace" className="flex min-h-0 min-w-0 flex-col">
        <TopBar kpis={kpis} onRefresh={() => setGeneration((g) => g + 1)} />
        <main id="content" tabIndex={-1} data-full={full} className={`tiles outline-none flex min-h-0 flex-1 flex-col overflow-hidden ${full ? "" : "p-5 max-[1250px]:p-3.5 max-[900px]:p-2.5"}`}>
          {kpis && <Kpis />}
          {/* Before the first overview a page knows nothing yet, which its empty state would read as nothing there. */}
          {waiting ? <Empty title={waiting} detail={waiting === "Connecting" ? "The console server did not send its first overview yet." : undefined} /> : <Suspense><Outlet /></Suspense>}
        </main>
      </div>
    </div>
  );
}
