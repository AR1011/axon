import { useQuery } from "@tanstack/react-query";
import { NavLink } from "react-router";
import { getJson, signOut } from "../api";
import { Icon, Logo } from "../components/Icon";
import { useUrlState } from "../hooks";
import { availableNodes, deadLetters, pending, totals, useLive } from "../store";
import { SIDEBAR, type useSidebar } from "./useSidebar";

const NAV: { group?: string; links: string[] }[] = [
  { links: ["overview"] },
  { group: "Cluster", links: ["cluster", "nodes", "connections", "placement"] },
  { group: "Actors", links: ["actors", "workload", "dead-letters"] },
  { links: ["streams"] },
  { group: "Investigate", links: ["events", "traces", "explore"] },
];

/** Who is signed in, and the way out. Absent on a console without users. */
function Account({ narrow, stack }: { narrow: string; stack: string }) {
  const { data } = useQuery({ queryKey: ["session"], queryFn: ({ signal }) => getJson<{ user: string | null }>("session", {}, signal), staleTime: Infinity });
  if (!data?.user) return null;
  return (
    <div className={`flex min-w-0 items-center gap-2 px-1.5 ${stack}`}>
      <span className={`min-w-0 flex-1 truncate text-[11px] ${narrow}`} title={data.user}>
        <span className="sr-only">Signed in as </span>
        {data.user}
      </span>
      <button type="button" onClick={() => void signOut()} aria-label="Sign out" title={`Sign out ${data.user}`} className="flex h-7 shrink-0 items-center gap-1.5 rounded-md px-1.5 text-[11px] hover:bg-raised hover:text-text">
        <Icon name="sign-out" size={14} />
        <span className={narrow}>Sign out</span>
      </button>
    </div>
  );
}

export function Sidebar({ sidebar }: { sidebar: ReturnType<typeof useSidebar> }) {
  const nodes = useLive((s) => s.overview?.nodes.length ?? 0);
  const available = useLive(availableNodes);
  const actors = useLive((s) => (s.overview ? (totals.actors ?? 0) : 0));
  const dead = useLive(deadLetters);
  const waiting = useLive(pending);
  const counts: Record<string, number | null> = waiting ? {} : { nodes, actors, "dead-letters": dead || null };
  // A picked history source follows every page.
  const [history] = useUrlState("history", "");
  const keep = history ? `?${new URLSearchParams({ history })}` : "";
  const narrow = sidebar.collapsed ? "hidden" : "max-[700px]:hidden";
  const stack = sidebar.collapsed ? "flex-col" : "max-[700px]:flex-col";
  return (
    <aside id="sidebar" aria-label="Sidebar" className="relative flex min-h-0 flex-col overflow-hidden border-r border-border bg-header">
      <div className="flex h-10 shrink-0 items-center gap-2 px-3">
        <Logo />
        <span className={`text-[19px] font-[620] tracking-[-0.7px] ${narrow}`}>axon</span>
        <span className={`text-[10px] text-muted ${narrow}`}>console</span>
      </div>
      <div className={`mx-2 mt-2 mb-2 flex items-center gap-2 rounded-md border border-border px-2 py-1.5 ${narrow}`}>
        <span className="flex size-4 items-center justify-center rounded-sm bg-raised text-[10px]">A</span>
        <span className="truncate text-[11px]">Actor cluster</span>
        <span className={`ml-auto size-1 rounded-full ${available ? "bg-success" : "bg-muted"}`} />
      </div>
      <nav aria-label="Main navigation" className="flex min-h-0 flex-col gap-2 overflow-y-auto px-2 pb-2">
        {NAV.map(({ group, links }) => {
          const items = links.map((link) => (
            <NavLink
              key={link}
              to={`/${link}${keep}`}
              title={link.replace("-", " ")}
              className={({ isActive }) =>
                `flex h-7 shrink-0 items-center gap-2 rounded-md px-2 text-[11px] capitalize ${isActive ? "bg-raised text-text [&>svg]:text-accent" : "text-muted hover:text-text"}`
              }
            >
              <Icon name={link} size={14} />
              <span className={narrow}>{link.replace("-", " ")}</span>
              {counts[link] != null && <span title={link === "dead-letters" ? "Messages that actors dropped after each node started" : undefined} className={`mono ml-auto text-[10px] text-muted ${narrow}`}>{counts[link]}</span>}
            </NavLink>
          ));
          if (!group) return <div key={links[0]} className="flex flex-col gap-px">{items}</div>;
          const id = `nav-${group.toLowerCase()}`;
          return (
            <div key={group} role="group" aria-labelledby={id} className="flex flex-col gap-px">
              <h2 id={id} className={`px-2 pt-1 pb-0.5 text-[10px] font-normal text-muted ${narrow}`}>{group}</h2>
              <span aria-hidden="true" className={`mx-2 mb-1 border-t border-border ${sidebar.collapsed ? "" : "hidden max-[700px]:block"}`} />
              {items}
            </div>
          );
        })}
      </nav>
      <div className="mt-auto flex shrink-0 flex-col gap-1 border-t border-border p-2 text-muted">
        <div className={`flex items-center gap-1 ${stack}`}>
          <NavLink to={`/settings${keep}`} title="Settings" className={({ isActive }) => `flex h-7 min-w-0 flex-1 items-center gap-2 rounded-md px-1.5 text-[11px] ${isActive ? "bg-raised text-text [&>svg]:text-accent" : "hover:text-text"}`}>
            <Icon name="settings" size={14} />
            <span className={narrow}>Settings</span>
          </NavLink>
          <button
            type="button"
            aria-label={sidebar.collapsed ? "Expand sidebar" : "Collapse sidebar"}
            title={sidebar.collapsed ? "Expand sidebar" : "Collapse sidebar"}
            aria-expanded={!sidebar.collapsed}
            aria-controls="sidebar"
            onClick={sidebar.toggle}
            className="flex size-[25px] items-center justify-center rounded-sm hover:bg-raised hover:text-text max-[700px]:hidden"
          >
            <Icon name={sidebar.collapsed ? "sidebar-expand" : "sidebar-collapse"} size={14} />
          </button>
        </div>
        <Account narrow={narrow} stack={stack} />
      </div>
      {!sidebar.collapsed && (
        <div
          role="separator"
          tabIndex={0}
          aria-label="Resize sidebar"
          aria-orientation="vertical"
          aria-controls="workspace"
          aria-valuemin={SIDEBAR.min}
          aria-valuemax={SIDEBAR.max}
          aria-valuenow={sidebar.width}
          aria-valuetext={`${sidebar.width} pixels`}
          onPointerDown={sidebar.onPointerDown}
          onKeyDown={sidebar.onKeyDown}
          onDoubleClick={sidebar.reset}
          className="absolute inset-y-0 right-0 w-1 cursor-col-resize touch-none hover:bg-border focus-visible:bg-accent max-[700px]:hidden"
        />
      )}
    </aside>
  );
}
