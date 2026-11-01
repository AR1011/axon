const GRID = "M4 4h6v6H4z M14 4h6v6h-6z M4 14h6v6H4z M14 14h6v6h-6z";
const RACKS = "M3 3h18v7H3z M3 14h18v7H3z M7 6h.01 M7 17h.01 M15 6h3 M15 17h3";

const PATHS: Record<string, string> = {
  linear: "M4 19V5 M4 19h16 M7 16L19 4",
  log: "M4 19V5 M4 19h16 M7 16C8 7 12 5 20 4",
  traffic: "M3 8h18 M17 4l4 4-4 4 M21 16H3 M7 12l-4 4 4 4",
  messages: "M3 4h18v12H9l-5 4v-4H3z M7 8h10 M7 12h6",
  size: "M4 5h16v14H4z M8 9h8 M8 9v3 M16 9v3",
  latency: "M12 5a8 8 0 1 0 0 16 8 8 0 0 0 0-16 M12 9v5l3 2 M9 2h6",
  palette:
    "M12 3a9 9 0 1 0 0 18h2a2 2 0 0 0 0-4h-1a2 2 0 0 1 0-4h5a3 3 0 0 0 3-3c-1-4-4-7-9-7 M7 8h.01 M12 6h.01 M17 8h.01 M6 13h.01",
  queue: "M4 5h16 M4 12h12 M4 19h8",
  circular: "M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18 M12 3v3 M21 12h-3 M12 21v-3 M3 12h3",
  force: "M12 9a3 3 0 1 0 0 6 3 3 0 0 0 0-6 M12 9V2 M9 12H2 M15 12h7 M12 15v7 M3 3l4 4 M17 17l4 4",
  focus: "M8 3H3v5 M16 3h5v5 M21 16v5h-5 M8 21H3v-5 M12 8a4 4 0 1 0 0 8 4 4 0 0 0 0-8",
  labels: "M4 5h16 M12 5v15 M8 20h8",
  all: GRID,
  grid: GRID,
  running: "M8 4l12 8-12 8z",
  stopped: "M5 5h14v14H5z",
  draining: "M5 4v6h14V4 M5 14v6h14v-6 M5 10l14 10 M19 10L5 20",
  failed: "M5 5l14 14 M19 5L5 19",
  download: "M12 4v11 M7 10l5 5 5-5 M4 20h16",
  info: "M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18 M12 11v5 M12 8h.01",
  overview: "M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z",
  cluster: "M12 3a2.5 2.5 0 1 0 0 5 2.5 2.5 0 0 0 0-5 M5 16a2.5 2.5 0 1 0 0 5 2.5 2.5 0 0 0 0-5 M19 16a2.5 2.5 0 1 0 0 5 2.5 2.5 0 0 0 0-5 M10.8 7.5l-4.6 7 M13.2 7.5l4.6 7 M7.5 18.5h9",
  seed: "M12 21v-9 M12 12c0-4 3-6 8-6-1 4-3 6-8 6z M12 15c0-4-3-6-8-6 1 4 3 6 8 6z",
  memory: "M7 7h10v10H7z M10 3v4 M14 3v4 M10 17v4 M14 17v4 M3 10h4 M3 14h4 M17 10h4 M17 14h4",
  static: "M12 2a2 2 0 1 0 0 4 2 2 0 0 0 0-4 M12 6v16 M5 12H2a10 10 0 0 0 20 0h-3",
  provider: "M9 2v6 M15 2v6 M6 8h12v4a6 6 0 0 1-12 0z M12 18v4",
  connectivity: "M9 2h6v5H9z M2 17h6v5H2z M16 17h6v5h-6z M12 7v5 M5 17v-5h14v5",
  nodes: RACKS,
  roles: RACKS,
  actors: "M8 9a4 4 0 1 0 0-8 4 4 0 0 0 0 8 M1 22v-4a7 7 0 0 1 14 0v4 M16 3a4 4 0 0 1 0 8 M19 15a6 6 0 0 1 4 6",
  connections:
    "M6 8a3 3 0 1 0 0-6 3 3 0 0 0 0 6 M18 22a3 3 0 1 0 0-6 3 3 0 0 0 0 6 M6 8v7a4 4 0 0 0 4 4h5 M9 5h5a4 4 0 0 1 4 4v7",
  streams: "M3 6h6c4 0 4 6 8 6h4 M3 18h6c4 0 4-6 8-6 M18 9l3 3-3 3",
  events: "M3 12h4l3-8 4 16 3-8h4",
  placement: "M12 2a7 7 0 0 0-7 7c0 5 7 13 7 13s7-8 7-13a7 7 0 0 0-7-7 M12 6a3 3 0 1 0 0 6 3 3 0 0 0 0-6",
  explore: "M4 19V5 M4 19h16 M7 15l4-5 3 3 5-6",
  workload: "M3 3h5v5H3z M10 3h5v5h-5z M17 3h4v5h-4z M3 10h5v5H3z M10 10h5v5h-5z M3 17h5v4H3z",
  "dead-letters": "M3 5h18v14H3z M3 6l9 7 9-7 M9 14l-5 4 M15 14l5 4",
  traces: "M3 5h9 M7 10h10 M11 15h10 M15 20h6",
  refresh: "M20 9a8 8 0 1 0 0 6 M20 3v6h-6",
  settings: "M4 6h10 M18 6h2 M4 12h2 M10 12h10 M4 18h10 M18 18h2 M16 4v4 M8 10v4 M16 16v4",
  moon: "M20.5 13.2A8.5 8.5 0 1 1 10.8 3.5a6.6 6.6 0 0 0 9.7 9.7z",
  sun: "M12 8a4 4 0 1 0 0 8 4 4 0 0 0 0-8 M12 1v3 M12 20v3 M1 12h3 M20 12h3 M4 4l2 2 M18 18l2 2 M4 20l2-2 M18 6l2-2",
  search: "M10 3a7 7 0 1 0 0 14 7 7 0 0 0 0-14 M15 15l6 6",
  expand: "M8 3H3v5 M16 3h5v5 M21 16v5h-5 M8 21H3v-5",
  collapse: "M3 8h5V3 M21 8h-5V3 M16 21v-5h5 M3 16h5v5",
  chevron: "M6 9l6 6 6-6",
  check: "M5 12l5 5 9-10",
  "sidebar-collapse": "M3 4h18v16H3z M9 4v16 M16 9l-3 3 3 3",
  "sidebar-expand": "M3 4h18v16H3z M9 4v16 M14 9l3 3-3 3",
  "sign-out": "M14 4h5v16h-5 M10 8l-4 4 4 4 M6 12h10",
};

export function Icon({ name, size = 18, className = "" }: { name: string; size?: number; className?: string }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" className={`shrink-0 ${className}`}>
      <path d={PATHS[name] ?? PATHS.overview} />
    </svg>
  );
}

/** The Consul mark, from Simple Icons (CC0), in its brand colour. */
const CONSUL =
  "M14.0754 12.0285a2.5059 2.5059 0 0 0-2.506-2.506 2.5059 2.5059 0 0 0-2.5058 2.506 2.5059 2.5059 0 0 0 2.5059 2.5059 2.5059 2.5059 0 0 0 2.5059-2.506zm3.5317.0003a1.1559 1.1556 0 0 0-1.1558-1.1556 1.1559 1.1556 0 0 0-1.1559 1.1556 1.1559 1.1556 0 0 0 1.1559 1.1555 1.1559 1.1556 0 0 0 1.1558-1.1555zm4.7917 5.5103a1.1576 1.1564 0 0 0-1.1577-1.1564 1.1576 1.1564 0 0 0-1.1576 1.1564 1.1576 1.1564 0 0 0 1.1576 1.1564 1.1576 1.1564 0 0 0 1.1577-1.1564zM20.748 13.888a1.1534 1.157 0 0 0-1.1533-1.157 1.1534 1.157 0 0 0-1.1534 1.157 1.1534 1.157 0 0 0 1.1534 1.1571 1.1534 1.157 0 0 0 1.1533-1.157zm3.2512.0619a1.1542 1.1538 0 0 0-1.1542-1.1538 1.1542 1.1538 0 0 0-1.1542 1.1538 1.1542 1.1538 0 0 0 1.1542 1.1538 1.1542 1.1538 0 0 0 1.1542-1.1538zm-3.279-3.883a1.1561 1.1535 0 0 0-1.156-1.1535 1.1561 1.1535 0 0 0-1.1562 1.1535 1.1561 1.1535 0 0 0 1.1561 1.1535 1.1561 1.1535 0 0 0 1.1562-1.1535zm3.2798.045a1.1614 1.157 0 0 0-1.1614-1.157 1.1614 1.157 0 0 0-1.1613 1.157 1.1614 1.157 0 0 0 1.1613 1.1572A1.1614 1.157 0 0 0 24 10.1119zm-1.626-3.631a1.1575 1.1601 0 0 0-1.1575-1.16 1.1575 1.1601 0 0 0-1.1575 1.16 1.1575 1.1601 0 0 0 1.1575 1.1602A1.1575 1.1601 0 0 0 22.374 6.481zM11.6171.3832c-3.1098 0-6.029 1.2063-8.2197 3.3974C1.206 5.9822 0 8.9007 0 11.9998c0 3.109 1.2067 6.0279 3.3974 8.2193 2.2028 2.1922 5.1213 3.3978 8.2197 3.3978 2.578 0 5.0176-.8252 7.055-2.386l-1.4196-1.8524c-1.6263 1.246-3.5748 1.9048-5.6354 1.9048-2.4776 0-4.8112-.9641-6.5717-2.7154-1.748-1.7488-2.7118-4.0828-2.7118-6.568 0-2.4784.9645-4.812 2.7158-6.5703 1.7484-1.7488 4.0817-2.713 6.5677-2.713 2.058 0 4.0069.659 5.6365 1.9057l1.4179-1.854C16.6314 1.2083 14.1922.3831 11.617.3831Z";

/** The mark of a discovery provider or ownership authority: Consul's logo, or an icon per built-in kind. */
export function ProviderLogo({ kind, size = 16 }: { kind: string; size?: number }) {
  if (kind === "consul") {
    return (
      <svg width={size} height={size} viewBox="0 0 24 24" fill="#dc477d" role="img" aria-label="Consul" className="shrink-0">
        <path d={CONSUL} />
      </svg>
    );
  }
  return <Icon name={["seed", "memory", "static"].includes(kind) ? kind : "provider"} size={size} />;
}

/** The brand mark: a soma, its axon, a terminal. */
export function Logo({ size = 20 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 100 100" fill="var(--accent)" aria-hidden="true" className="shrink-0">
      <line x1="36" y1="64" x2="82" y2="20" stroke="var(--accent)" strokeWidth="11" strokeLinecap="round" />
      <circle cx="36" cy="64" r="26" />
      <circle cx="82" cy="20" r="11" />
    </svg>
  );
}
