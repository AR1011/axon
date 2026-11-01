// The cluster page, "nodes as the spine": the members at a glance, the two providers, one row per node with a cell per
// subsystem whose colour tells normal or not, and the counters of a node in a drawer.
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Link } from "react-router";
import { INBOUND_REJECTIONS, type NodeView, type ProviderInfo, type Snapshot } from "../../api";
import { Badge, Dot, Empty, Since, TONES } from "../../components/atoms";
import { ExportButton } from "../../components/Export";
import { NodeName } from "../../components/Flag";
import { Panel } from "../../components/Panel";
import { HelpDialog, Part } from "../../components/HelpDialog";
import { ProviderLogo } from "../../components/Icon";
import { Sparkline } from "../../components/Sparkline";
import { nodeStatus, useEscape, useHistory, useNow, useUrlState, type Tone } from "../../hooks";
import { clock, count, natural, percent, perSecond, uptime } from "../../model/format";
import { seriesLine } from "../../model/history";
import { acceptorRows, readiness, type AcceptorRow } from "../../model/rollout";
import { latest, rates } from "../../model/series";
import { useLive } from "../../store";
import { nodePath, reporter, roleOf } from "../select";

type C = Snapshot["cluster"];
type Sub = "membership" | "discovery" | "leases" | "quotas" | "calls";
/** `watch`: an increase of this counter now needs attention; `gauge`: a level, not a counter, so it has no rate. */
interface Def { id: string; label: string; help: string; get: (c: C) => number | undefined; watch?: boolean; gauge?: boolean }

const SUBS: { sub: Sub; title: string; help: string }[] = [
  { sub: "membership", title: "Membership", help: "The nodes that a node sees in the cluster, and if it can reach them." },
  { sub: "discovery", title: "Discovery", help: "The provider where nodes register and get the member list." },
  { sub: "leases", title: "Leases", help: "Keyed actors, and the leases that keep each key on one node only." },
  { sub: "quotas", title: "Quotas", help: "The quota replica on a node: what it refused and what it keeps." },
  { sub: "calls", title: "Calls", help: "Remote calls that a node sent (out) and received (in)." },
];

const REFUSALS: Record<string, string> = {
  unknown_actor: "No actor has that name or key on the node.",
  schema_mismatch: "The actor on the node uses a different schema of the protocol.",
  full: "The mailbox of the actor was full.",
  forbidden: "The authorizer refused the call.",
  invalid_message: "The node could not decode the bytes.",
  stopped: "The actor stopped. This is normal when keys move.",
};
const q = (pick: (s: NonNullable<C["quotas"]>) => number) => (c: C) => (c.quotas ? pick(c.quotas) : undefined);
const DEFS: Record<Sub, [string, Def[]][]> = {
  membership: [
    ["Now", [
      { id: "members", label: "Members", help: "The nodes in the view of the node, the node included.", get: (c) => c.members, gauge: true },
      { id: "unreachable", label: "Unreachable", help: "Members in the view that the node cannot reach now.", get: (c) => c.unreachable, gauge: true },
    ]],
    ["Since its start", [
      { id: "joined", label: "Joined", help: "Nodes that the view added.", get: (c) => c.joined },
      { id: "left", label: "Left", help: "Nodes that the view removed.", get: (c) => c.left },
      { id: "replaced", label: "Replaced", help: "A name that returned as a new process. This is a restart.", get: (c) => c.replaced },
      { id: "became_unreachable", label: "Became unreachable", help: "The number of times that a member stopped its replies, leaves included.", get: (c) => c.became_unreachable },
    ]],
  ],
  discovery: [
    ["Since its start", [
      { id: "discovery_errors", label: "Errors", help: "Reads of the member list from the provider that failed.", get: (c) => c.discovery_errors, watch: true },
      { id: "heartbeat_failures", label: "Heartbeat failures", help: "Registrations or heartbeats to the provider that failed.", get: (c) => c.heartbeat_failures, watch: true },
    ]],
  ],
  leases: [
    ["Keyed actors", [
      { id: "spawned", label: "Keyed actors now", help: "Keyed actors that the node runs now.", get: (c) => c.spawned, gauge: true },
      { id: "spawn_requests", label: "Start requests", help: "Requests from other nodes to start a keyed actor on the node.", get: (c) => c.spawn_requests },
      { id: "spawn_refused", label: "Starts refused", help: "Start requests that the node refused.", get: (c) => c.spawn_refused, watch: true },
      { id: "handoffs", label: "Handoffs", help: "Keyed actors that the node gave to a new owner after a membership change.", get: (c) => c.handoffs },
      { id: "handoffs_failed", label: "Handoffs failed", help: "The new owner did not start these actors. The next call starts them.", get: (c) => c.handoffs_failed, watch: true },
      { id: "keyed_failed", label: "Failed", help: "Actors that stopped because of their own failure, with no restart. The next call starts the key again.", get: (c) => c.keyed_failed, watch: true },
    ]],
    ["Losses", [
      { id: "leases_lost", label: "Leases lost", help: "Keys that the node released because it could not renew their lease. Their actors stopped.", get: (c) => c.leases_lost, watch: true },
      { id: "isolations", label: "Isolations", help: "Times that no peer confirmed the membership of the node. Then the node refuses new keys and /readyz is 503. With an ownership authority, each key runs until its lease stops it. Without an authority, or when a peer removed the node, the node stops its keyed actors immediately. The node is a member again after the cluster hears it for one full heartbeat grace.", get: (c) => c.isolations, watch: true },
    ]],
  ],
  quotas: [
    ["Refusals and admissions since its start", [
      { id: "denied_limit", label: "Denied by limit", help: "Refusals because the key is at its limit for the cluster.", get: q((s) => s.denied_limit), watch: true },
      { id: "denied_share", label: "Denied by share", help: "Refusals because the replica used all of its share until its next report.", get: q((s) => s.denied_share) },
      { id: "fallback", label: "Fallback units", help: "Units that the replica admitted without a new grant from most replicas of the key.", get: q((s) => s.fallback) },
      { id: "mismatched", label: "Mismatched reports", help: "Reports that give a different maximum for a key. The nodes have different limits.", get: q((s) => s.mismatched), watch: true },
    ]],
    ["Replica", [
      { id: "entries", label: "Entries", help: "Limits and keys that the node used recently.", get: q((s) => s.entries), gauge: true },
      { id: "permits", label: "Permits held", help: "Permits that the node holds, for all keys.", get: q((s) => s.permits), gauge: true },
      { id: "keys", label: "Keys replicated", help: "Keys for which the node keeps a replica of the counts.", get: q((s) => s.keys), gauge: true },
      { id: "rows", label: "Rows", help: "One row for each node that uses a replicated key.", get: q((s) => s.rows), gauge: true },
      { id: "reports", label: "Reports", help: "Usage reports between the replica and the other replicas.", get: q((s) => s.reports) },
      { id: "report_failures", label: "Report failures", help: "Reports that did not arrive.", get: q((s) => s.report_failures), watch: true },
    ]],
  ],
  calls: [
    ["Calls out", [
      { id: "calls", label: "Started", help: "Requests and tells that the node sent to other nodes.", get: (c) => c.engine.calls },
      { id: "succeeded", label: "Succeeded", help: "Requests with an answer, and tells that arrived.", get: (c) => c.engine.succeeded },
      { id: "rejected", label: "Rejected", help: "The far node refused the call. Its calls in give the cause.", get: (c) => c.engine.rejected, watch: true },
      { id: "timed_out", label: "Timed out", help: "No answer came before the request timeout.", get: (c) => c.engine.timed_out, watch: true },
      { id: "unknown_outcomes", label: "Unknown outcome", help: "The session closed before the answer. The call possibly ran.", get: (c) => c.engine.unknown_outcomes, watch: true },
      { id: "failed", label: "Failed", help: "The node could not send the call: no session, an encoding error, or a message that is too large.", get: (c) => c.engine.failed, watch: true },
      { id: "abandoned", label: "Abandoned", help: "The caller stopped the wait before the answer came.", get: (c) => c.engine.abandoned },
    ]],
    ["Calls in", [
      { id: "inbound_calls", label: "Received", help: "Calls that other nodes sent to the node.", get: (c) => c.engine.inbound_calls },
      { id: "inbound_expired", label: "Expired", help: "Calls that arrived after their deadline. The node did not run them.", get: (c) => c.engine.inbound_expired, watch: true },
      ...INBOUND_REJECTIONS.map((r, i): Def => ({ id: `in_${r}`, label: `Refused: ${r.replaceAll("_", " ")}`, help: REFUSALS[r] ?? "", get: (c) => c.engine.inbound_rejected[i], watch: r !== "stopped" })),
    ]],
  ],
};
const ALL_DEFS = Object.values(DEFS).flatMap((groups) => groups.flatMap(([, defs]) => defs));
const FAILED = ["rejected", "timed_out", "unknown_outcomes", "failed", "abandoned"];

/** Plain words for the keys that a provider reports; a key that is not in this list shows as the provider names it. */
const STAT_WORDS: Record<string, [string, string]> = {
  held: ["Leases held", "Leases that the node holds now as a proposer."],
  acquired: ["Acquired", "Leases that the node got since its start."],
  "held by other": ["Held by another node", "Requests for a key that another node held at that time."],
  lost: ["Lost", "Leases that the node could not renew."],
  leases: ["Promised (acceptor)", "Live leases for which this acceptor gave a promise."],
  floor: ["Floor (acceptor)", "After a restart, the acceptor accepts only epochs after this time. Thus no lease from before the restart can return."],
  warming: ["Warming (acceptor)", "After a restart, the acceptor refuses grants until all leases that it possibly promised are expired."],
  refused: ["Ballots refused (acceptor)", "Grants that the acceptor refused: an epoch at or below its floor, or a grant during warming."],
  "clock behind": ["Clock behind (acceptor)", "Ballots with an epoch too far ahead of the clock of this acceptor. Its clock is behind the clocks of the proposers."],
  acceptors: ["Acceptors", "The nodes that vote on each lease. A majority must answer."],
  lease: ["Lease", "The time that a lease stays valid without a renewal."],
  drift: ["Clock drift", "The margin that each lease keeps for clocks with different speeds."],
  nodes: ["Nodes", "The nodes in the list of the provider now."],
  revision: ["Revision", "The provider increases this number at each change to the list."],
  seeds: ["Seeds", "The seed servers that the node registers with."],
  tls: ["TLS", "The node uses TLS to the seed servers."],
  live: ["Live seeds", "Seed servers that answer now."],
  members: ["Members", "Members that the seed servers list."],
  agent: ["Agent", "The address of the Consul agent."],
  service: ["Service", "The service name in Consul."],
  ttl: ["TTL", "The time that a registration stays valid without a heartbeat."],
  session: ["Session", "The Consul session that holds the locks of the node."],
  keys: ["Keys", "Keys with a lease."],
  owners: ["Owners", "Nodes that hold a lease."],
};
const words = (key: string) => STAT_WORDS[key] ?? [key, ""];
const DISCOVERY: Record<string, [string, string]> = {
  memory: ["Memory", "A list in one process, for tests."],
  static: ["Static list", "A fixed list of nodes in the configuration."],
  seed: ["Seed servers", "Each node registers with the seed servers and gets the member list from them."],
  consul: ["Consul", "Each node registers as a Consul service with a TTL check."],
};
const OWNERSHIP: Record<string, [string, string]> = {
  memory: ["Memory leases", "Leases in one process, for tests."],
  paxos: ["Paxos leases", "A majority of the acceptors grants each lease. Each lease has a fencing epoch."],
  consul: ["Consul leases", "Each lease is a Consul lock in the session of the node."],
};

function statValue(key: string, value: string) {
  // The floor is a unix time in microseconds, from `trace::unix_micros`.
  if (key === "floor" && /^\d{13,}$/.test(value)) {
    return <span>{clock(Number(value) / 1000, 0, true)} <span className="text-muted">(<Since us={Number(value)} label="Floor at" /> ago)</span></span>;
  }
  return yesNo(value);
}
const yesNo = (value: string) => (value === "true" ? "yes" : value === "false" ? "no" : value);
/** A provider's settings and stats as the quiet line of a card shows them. */
const settingItems = (pairs: [string, string][]): [string, string][] => pairs.map(([k, v]) => [words(k)[0].toLowerCase(), yesNo(v)]);

const LINK_TONE: Record<string, Tone> = { synced: "success", answering: "success", open: "muted", idle: "muted", connecting: "warning", refused: "warning", failing: "danger", retired: "muted", timings: "warning" };
/** What a state that needs a word says, in the flag and on the badge. */
const LINK_WHY: Record<string, string> = { timings: "The seed uses different timings. This node does not count it." };
const WORST: Tone[] = ["danger", "warning", "success", "muted"];
const worst = (tones: Tone[]) => WORST.find((t) => tones.includes(t)) ?? "muted";
const odd = (t: Tone) => t === "danger" || t === "warning";
const TONE_WORD: Record<Tone, string> = { success: "normal", muted: "off", warning: "examine", danger: "wrong now" };

interface Cell { tone: Tone; why: string[] }
interface Row {
  view: NodeView;
  c: C | null;
  /** Per second now, by `Def.id`, from the last two snapshots of this node. */
  now: Map<string, number>;
  cells: Record<Sub, Cell>;
  tone: Tone;
  calls: { out: number | null; in: number | null; failed: number | null };
  denied: number | null;
  held: number | null;
  acceptor: AcceptorRow | null;
  sees: { id: string; state: "ok" | "unreachable" | "missing" }[];
  extra: string[];
}

const stat = (info: ProviderInfo | null | undefined, key: string) => info?.stats.find(([k]) => k === key)?.[1];
const pos = (n: number | undefined) => (n ?? 0) > 0;
const plural = (n: number, one: string) => `${n} ${one}${n === 1 ? "" : "s"}`;

/** One row per node with the problems of each subsystem: a problem is a fault now, not a counter above zero. */
function buildRows(nodes: NodeView[], at: number, accs: AcceptorRow[]): Row[] {
  const acceptors = new Map(accs.map((a) => [a.id, a]));
  const present = nodes.filter((n) => !n.leftAt);
  const presentIds = new Set(present.map((n) => n.node.id));
  return nodes.map((view) => {
    const s = view.snapshot;
    const c = s && !view.error ? s.cluster : null;
    const now = c ? rates(`cluster:${view.node.id}`, s!.incarnation, s!.timestamp_ms, new Map(ALL_DEFS.filter((d) => !d.gauge).map((d) => [d.id, d.get(c) ?? 0]))) : new Map<string, number>();
    const rising = (sub: Sub) => DEFS[sub].flatMap(([, defs]) => defs.filter((d) => d.watch && pos(now.get(d.id))).map((d) => `${d.label.toLowerCase()}: ${perSecond(now.get(d.id))} now`));
    const status = nodeStatus(view, at);
    const cells = {} as Record<Sub, Cell>;
    const seen = new Map((s?.members ?? []).map((m) => [m.record.id, m.reachable]));
    const sees = present.map((n) => ({ id: n.node.id, state: !seen.has(n.node.id) ? ("missing" as const) : seen.get(n.node.id) ? ("ok" as const) : ("unreachable" as const) }));
    const extra = [...seen.keys()].filter((id) => !presentIds.has(id));
    const acceptor = acceptors.get(view.node.id) ?? null;
    const acc = acceptor?.stats;
    const failed = FAILED.reduce((sum, id) => sum + Math.max(0, now.get(id) ?? 0), 0);
    if (!c) {
      const why = [view.error ?? (view.leftAt ? status.label : "no snapshot")];
      for (const { sub } of SUBS) cells[sub] = { tone: view.leftAt ? "muted" : "danger", why };
    } else {
      const missing = sees.filter((x) => x.state === "missing").map((x) => x.id);
      cells.membership = mark([
        c.isolated && ["danger", "isolated: no peer confirms its membership"],
        status.stale && ["warning", `snapshot ${status.label.toLowerCase()}`],
        c.unreachable > 0 && ["warning", `${plural(c.unreachable, "member")} unreachable`],
        missing.length > 0 && ["warning", `not in its view: ${missing.join(", ")}`],
        extra.length > 0 && ["warning", `in its view only: ${extra.join(", ")}`],
      ]);
      cells.discovery = mark([
        ...rising("discovery").map((w): Flag => ["warning", w]),
        !!c.last_heartbeat_error && ["warning", `last heartbeat error: ${c.last_heartbeat_error}`],
        ...c.discovery.links.filter((l) => l.error || LINK_TONE[l.state] === "warning").map((l): Flag => ["warning", `${l.target}: ${l.error ?? LINK_WHY[l.state] ?? l.state}`]),
      ]);
      cells.leases = c.ownership
        ? mark([
            pos(now.get("leases_lost")) && ["danger", `leases lost: ${perSecond(now.get("leases_lost"))} now`],
            acceptor?.state === "no report" && ["danger", "the acceptor sends no report"],
            acceptor?.state === "warming" && ["warning", "acceptor warming: it refuses grants after its restart"],
            !!c.warming && acceptor?.state !== "warming" && ["warning", "warming after a restart"],
            ...rising("leases").filter((w) => !w.startsWith("leases lost")).map((w): Flag => ["warning", w]),
            ...c.ownership.links.filter((l) => l.error || LINK_TONE[l.state] === "danger").map((l): Flag => ["warning", `link to ${l.target}: ${l.error ?? l.state}`]),
            ...(acc && Number(acc["clock behind"]) > 0 ? [["warning", `clock behind the proposers: ${acc["clock behind"]} ballots`] as Flag] : []),
          ])
        : { tone: "muted", why: ["ownership not configured. Keys are unique in each membership view only."] };
      cells.quotas = c.quotas
        ? mark([c.quotas.pulling && ["warning", "pulls the rows of the other replicas"], ...rising("quotas").map((w): Flag => ["warning", w])])
        : { tone: "muted", why: ["no Quotas on this node"] };
      const started = now.get("calls");
      const share = started ? Math.min(failed, started) / started : 0;
      cells.calls = mark([
        share > 0.05 && ["danger", `${percent(share * 100)} of calls out without success`],
        share > 0 && share <= 0.05 && ["warning", `${percent(share * 100)} of calls out without success`],
        ...rising("calls").filter((w) => w.startsWith("refused") || w.startsWith("expired")).map((w): Flag => ["warning", `in: ${w}`]),
      ]);
    }
    const held = stat(c?.ownership, "held");
    return {
      view,
      c,
      now,
      cells,
      tone: worst(Object.values(cells).map((x) => x.tone)),
      calls: { out: c ? (now.get("calls") ?? null) : null, in: c ? (now.get("inbound_calls") ?? null) : null, failed: c ? failed : null },
      denied: c?.quotas && now.has("denied_limit") ? Math.max(0, now.get("denied_limit")!) + Math.max(0, now.get("denied_share") ?? 0) : null,
      held: held != null ? Number(held) : null,
      acceptor,
      sees,
      extra,
    };
  });
}
type Flag = false | [Tone, string];
function mark(flags: Flag[]): Cell {
  const on = flags.filter((f): f is [Tone, string] => !!f);
  return on.length ? { tone: worst(on.map(([t]) => t)), why: on.map(([, w]) => w) } : { tone: "success", why: [] };
}

const CELL_BG: Record<Tone, string> = {
  danger: "bg-[color-mix(in_srgb,var(--danger)_14%,transparent)]",
  warning: "bg-[color-mix(in_srgb,var(--warning)_10%,transparent)]",
  success: "",
  muted: "",
};
const BAR: Record<Tone, string> = { danger: "bg-danger", warning: "bg-warning", success: "bg-edge", muted: "bg-muted" };

function Bar({ value, max, tone = "success" }: { value: number | null; max: number; tone?: Tone }) {
  const w = value && max > 0 ? Math.max(3, Math.min(100, (value / max) * 100)) : 0;
  return (
    <span className="block h-[3px] w-full min-w-[24px] overflow-hidden rounded-full bg-border" aria-hidden>
      <span className={`block h-full ${BAR[tone]}`} style={{ width: `${w}%` }} />
    </span>
  );
}

// Every row has the same two lines, normal or not, so that a new problem changes no height on the page.
const ROW_H = "h-[40px]";
const LINE = "flex h-4 min-w-0 items-center gap-1.5 overflow-hidden whitespace-nowrap";
const LINE2 = "flex h-3.5 min-w-0 items-center gap-1.5 overflow-hidden whitespace-nowrap";

/** A matrix cell: a button that opens the drawer. When it is not normal, its first reason replaces the second line. */
function MatrixCell({ cell, onOpen, label, main, sub }: { cell: Cell; onOpen: () => void; label: string; main: ReactNode; sub?: ReactNode }) {
  return (
    <td className={`h-auto border-l border-border p-0 align-top ${CELL_BG[cell.tone]}`}>
      <button type="button" onClick={onOpen} title={cell.why.join("\n") || "Normal"} aria-label={`${label}: ${cell.why.join(", ") || "normal"}`} className={`flex ${ROW_H} w-full min-w-0 flex-col justify-center gap-[3px] px-2.5 text-left hover:bg-raised focus-visible:outline-1`}>
        <span className={`${LINE} mono text-[11px]`}>{main}</span>
        <span className={`${LINE2} font-sans text-[10px] text-muted`}>
          {odd(cell.tone) ? (
            <>
              <span className={`min-w-0 truncate ${TONES[cell.tone]}`}>{cell.why[0]}</span>
              {cell.why.length > 1 && <span className="shrink-0">+{cell.why.length - 1}</span>}
            </>
          ) : sub}
        </span>
      </button>
    </td>
  );
}

function Dots({ sees }: { sees: Row["sees"] }) {
  if (sees.length > 24) return null;
  const tone = { ok: "bg-success", unreachable: "bg-warning", missing: "border border-danger" };
  return (
    <span className="flex gap-[2px] overflow-hidden" aria-hidden>
      {sees.map((s) => <span key={s.id} title={`${s.id}: ${s.state === "ok" ? "in its view, reachable" : s.state === "missing" ? "not in its view" : "unreachable"}`} className={`size-[5px] shrink-0 rounded-full ${tone[s.state]}`} />)}
    </span>
  );
}

/** Short facts on one line, separated by dots; only a fact with a tone gets a colour. */
function Status({ items }: { items: ([string, Tone?, string?] | false)[] }) {
  const shown = items.filter((i): i is [string, Tone?, string?] => !!i);
  return (
    <p className="h-[18px] truncate text-[11px]">
      {shown.map(([text, tone, title], i) => (
        <span key={text} title={title} className={tone ? TONES[tone] : "text-text"}>
          {i > 0 && <span className="text-muted"> · </span>}
          {text}
        </span>
      ))}
    </p>
  );
}

/** The provider's settings and other values, quiet, on one line; the full text shows on hover. */
function Settings({ items }: { items: [string, string][] }) {
  const text = items.map(([k, v]) => `${k} ${v}`).join(" · ");
  return !items.length ? null : <p className="h-4 truncate text-[10px] text-muted" title={text}>{text}</p>;
}

function Card({ kind, name, role, tone, bad, children }: { kind: string; name: string; role: string; tone: Tone; bad: number; children: ReactNode }) {
  return (
    <section aria-label={`${role}: ${name}`} className="flex min-w-0 flex-col overflow-hidden rounded-md border border-border bg-panel">
      <header className="flex h-8 shrink-0 items-center gap-2 border-b border-border px-3">
        <ProviderLogo kind={kind} size={14} />
        <h2 className="truncate text-[12px] font-[550]">{name}</h2>
        <span className="truncate text-[10px] text-muted">{role}</span>
        <span className="ml-auto shrink-0"><Badge tone={tone}>{bad ? `${plural(bad, "node")}: ${TONE_WORD[tone]}` : TONE_WORD[tone]}</Badge></span>
      </header>
      <div className="flex flex-col gap-1 px-3 py-2">{children}</div>
    </section>
  );
}

const LinkCell = ({ link }: { link?: ProviderInfo["links"][number] }) =>
  !link ? <span className="text-muted">—</span> : (
    <span className="flex min-w-0 items-center gap-1.5" title={link.error ?? LINK_WHY[link.state]}>
      <Badge tone={link.error ? "danger" : (LINK_TONE[link.state] ?? "muted")}>{link.state}</Badge>
      {link.since_ns > 0 && <span className="truncate text-muted">{uptime(link.since_ns / 1e6)}</span>}
    </span>
  );

const MINI_TD = "h-[22px] px-2 text-[10px]";

/** The discovery provider: its kind and settings, what it reports now, and the work of all nodes with it. */
function DiscoveryCard({ rows, by, cell }: { rows: Row[]; by: string; cell: Cell & { bad: number } }) {
  const live = rows.filter((r) => r.c);
  const info = live.find((r) => r.view.node.id === by)?.c?.discovery ?? live[0]?.c?.discovery;
  const kinds = [...new Set(live.map((r) => r.c!.discovery.kind))];
  const [name, about] = DISCOVERY[info?.kind ?? ""] ?? [info?.kind ?? "No report", ""];
  const errors = live.reduce((t, r) => t + r.c!.discovery_errors, 0);
  const beats = live.reduce((t, r) => t + r.c!.heartbeat_failures, 0);
  const last = live.find((r) => r.c!.last_heartbeat_error);
  const up = live.filter((r) => r.c!.discovery.links.length && r.c!.discovery.links.every((l) => LINK_TONE[l.state] === "success")).length;
  return (
    <Card kind={info?.kind ?? ""} name={name} role="discovery provider" tone={cell.tone} bad={cell.bad}>
      <p className="h-4 truncate text-[10px] text-muted" title={about}>{about}</p>
      <Status
        items={[
          [errors ? `${plural(errors, "member list error")}` : "no member list errors", errors ? "warning" : undefined, "Failed reads of the member list, all nodes, since their start."],
          [beats ? plural(beats, "heartbeat failure") : "no heartbeat failures", beats ? "warning" : undefined, "Failed registrations and heartbeats, all nodes, since their start."],
          !!last && [`last error on ${last.view.node.id}`, "warning", `${last.view.node.id}: ${last.c!.last_heartbeat_error}`],
          !!info?.links.length && [`seed links up on ${up} of ${live.length} nodes`, up < live.length ? "warning" : undefined],
          kinds.length > 1 && [`${kinds.length} different providers: ${kinds.join(", ")}`, "warning"],
        ]}
      />
      <Settings items={settingItems([...(info?.settings ?? []), ...(info?.stats ?? [])])} />
      {info && info.links.length > 0 && (
        <table className="mt-1 w-full table-fixed border-t border-border">
          <thead><tr><th className="px-2 py-1 text-[10px] text-muted">Seed</th><th className="px-2 py-1 text-[10px] text-muted">Link from {by}</th><th className="px-2 py-1 text-[10px] text-muted">Last error</th></tr></thead>
          <tbody>
            {info.links.map((l) => (
              <tr key={l.target}>
                <td className={MINI_TD}><NodeName id={l.target} link={false} /></td>
                <td className={MINI_TD}><LinkCell link={l} /></td>
                <td className={`${MINI_TD} ${l.error ? "text-warning" : "text-muted"}`} title={l.error ?? undefined}>{l.error ?? "none"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Card>
  );
}

/** Proposer counters that each node keeps for itself, so the card adds them up; the other stats are from one node. */
const SUMMED = new Set(["held", "acquired", "held by other", "lost", "requests", "failures"]);
const ACCEPTOR_ONLY = new Set(["leases", "floor", "warming", "refused", "clock behind"]);

/** The ownership authority: its kind and settings, the quorum margin and each acceptor under Paxos, and the lease work. */
function OwnershipCard({ rows, accs, by, cell }: { rows: Row[]; accs: AcceptorRow[]; by: string; cell: Cell & { bad: number } }) {
  const live = rows.filter((r) => r.c);
  const self = live.find((r) => r.view.node.id === by) ?? live[0];
  const info = self?.c?.ownership;
  if (!info) {
    return (
      <Card kind="" name="Not configured" role="ownership" tone="muted" bad={0}>
        <p className="text-[10px] text-muted">A keyed actor is unique in each membership view only. Set ClusterConfig::ownership to get leases with a fencing epoch.</p>
      </Card>
    );
  }
  const [name, about] = OWNERSHIP[info.kind] ?? [info.kind, ""];
  const active = accs.filter((a) => a.state === "active").length;
  const quorum = Math.floor(accs.length / 2) + 1;
  const margin = active - quorum;
  const warming = live.filter((r) => r.c!.warming).length;
  const holders = live.filter((r) => (r.held ?? 0) > 0).length;
  const summed = (k: string) => live.reduce((t, r) => t + (Number(stat(r.c!.ownership, k)) || 0), 0);
  const settings = info.settings.filter(([k]) => k !== "acceptors");
  const stats = info.stats.filter(([k]) => !ACCEPTOR_ONLY.has(k));
  return (
    <Card kind={info.kind} name={name} role="ownership authority" tone={cell.tone} bad={cell.bad}>
      <p className="h-4 truncate text-[10px] text-muted" title={about}>{about}</p>
      <Status
        items={[
          [`${count(summed("held"))} leases on ${plural(holders, "node")}`, undefined, `Acquired ${count(summed("acquired"))}, held by another node ${count(summed("held by other"))}: the sums over ${plural(live.length, "node")} since their start.`],
          accs.length > 0 && [`${active} of ${accs.length} acceptors active`, active < accs.length ? "warning" : undefined],
          accs.length > 0 && [margin >= 0 ? `${plural(margin, "acceptor")} can stop` : "no quorum", margin < 0 ? "danger" : margin === 0 ? "warning" : undefined, `A lease needs answers from ${quorum} of ${accs.length} acceptors.`],
          [summed("lost") ? plural(summed("lost"), "lease") + " lost" : "no leases lost", summed("lost") ? "warning" : undefined, "Since the start of each node."],
          !!warming && [`${plural(warming, "node")} warming`, "warning", "Nodes that refuse to start keyed actors after a restart, until their old leases expire."],
        ]}
      />
      <Settings items={settingItems([...settings, ...stats.filter(([k]) => !SUMMED.has(k))])} />
      {accs.length > 0 && (
        <table aria-label="Acceptors" className="mt-1 w-full table-fixed border-t border-border">
          <colgroup><col className="w-[18%]" /><col className="w-[13%]" /><col className="w-[10%]" /><col className="w-[9%]" /><col className="w-[8%]" /><col /><col className="w-[20%]" /></colgroup>
          <thead>
            <tr>
              <th className="px-2 py-1 text-[10px] text-muted" title="Each acceptor as it reports itself">Acceptor</th>
              <th className="px-2 py-1 text-[10px] text-muted">State</th>
              <th className="px-2 py-1 text-right text-[10px] text-muted" title={words("leases")[1]}>Promised</th>
              <th className="px-2 py-1 text-right text-[10px] text-muted" title={words("refused")[1]}>Refused</th>
              <th className="px-2 py-1 text-right text-[10px] text-muted" title={words("clock behind")[1]}>Behind</th>
              <th className="px-2 py-1 text-[10px] text-muted" title={words("floor")[1]}>Floor</th>
              <th className="px-2 py-1 text-[10px] text-muted" title={`The link from ${self.view.node.id} to the acceptor, and its time in that state`}>Link from {self.view.node.id}</th>
            </tr>
          </thead>
          <tbody>
            {accs.map(({ id, stats: s, state }) => (
              <tr key={id}>
                <td className={MINI_TD}><NodeName id={id} link={false} /></td>
                <td className={MINI_TD}><Badge tone={state === "no report" ? "danger" : state === "warming" ? "warning" : "success"}>{state}</Badge></td>
                <td className={`${MINI_TD} text-right`}>{s?.leases ?? "—"}</td>
                <td className={`${MINI_TD} text-right ${Number(s?.refused) > 0 ? "text-warning" : ""}`}>{s?.refused ?? "—"}</td>
                <td className={`${MINI_TD} text-right ${Number(s?.["clock behind"]) > 0 ? "text-warning" : ""}`}>{s?.["clock behind"] ?? "—"}</td>
                <td className={MINI_TD}>{s?.floor ? statValue("floor", s.floor) : "—"}</td>
                <td className={MINI_TD}><LinkCell link={info.links.find((l) => l.target === id)} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Card>
  );
}

export function ClusterHelp() {
  return (
    <HelpDialog button="What is this" title="What the cluster page shows">
      <Part title="One row for each node">
        Each cell shows one subsystem of one node. Green is normal. Amber tells you to examine the cell. Red is a fault now. Nodes with a problem show
        first. Click a cell to see the counters of that node. Click a column title to see the counters of all nodes.
      </Part>
      <Part title="Discovery">
        Each node registers with the discovery provider and sends heartbeats to it. The provider gives the member list. Discovery does not decide which
        node owns a key.
      </Part>
      <Part title="Ownership">
        With an authority, a node gets a lease with a fencing epoch before a keyed actor starts. The node renews the lease while the actor runs. With
        Paxos leases, a majority of the acceptors grants each lease. After a restart, an acceptor is warming and refuses grants until all old leases
        are expired. In a rolling restart, wait until the acceptor is active again. Then restart the next one.
      </Part>
      <Part title="Rates and counters">
        Rates are per second now, from the last two snapshots of each node. A counter starts at zero when its node starts. The card values come from the
        node of the console, except the sums over all nodes.
      </Part>
    </HelpDialog>
  );
}

/** A node's one word for this page: lost, left, unavailable, stale, not ready (with why), draining, or running. */
function statusOf(view: NodeView, now: number): { tone: Tone; label: string } {
  const s = nodeStatus(view, now);
  const ready = readiness(view);
  return !s.stale && ready?.length ? { tone: "warning", label: ready.join(" · ") } : { tone: s.tone, label: s.label.toLowerCase() };
}

function Figure({ n, label, tone = "muted", title }: { n: ReactNode; label: string; tone?: Tone; title?: string }) {
  return (
    <div title={title} className="flex items-baseline gap-1.5 whitespace-nowrap">
      <span className={`mono text-[18px] leading-none ${n === 0 ? "text-muted" : TONES[tone]}`}>{typeof n === "number" ? count(n) : n}</span>
      <span className="text-[10px] text-muted">{label}</span>
    </div>
  );
}

function Members({ nodes, departed, now }: { nodes: NodeView[]; departed: NodeView[]; now: number }) {
  const all = useMemo(() => [...nodes, ...departed].map((view) => ({ view, status: statusOf(view, now) })).sort((a, b) => natural(a.view.node.id, b.view.node.id)), [nodes, departed, now]);
  const unreachable = nodes.filter((n) => !n.reachable || n.error).length;
  const notReady = nodes.filter((n) => readiness(n)?.length).length;
  const lost = departed.filter((n) => n.leaveReason === null).length;
  const attention = all.filter((x) => x.status.tone !== "success" && x.status.tone !== "muted");
  return (
    <Panel label="Members" className="shrink-0">
      <div className="flex flex-wrap items-baseline gap-x-5 gap-y-1 px-3 pt-2.5">
        <Figure n={nodes.length} label="members" tone="success" title="Nodes discovery lists, as the console sees them" />
        <Figure n={unreachable} label="unreachable" tone="danger" title="Members the console node cannot reach" />
        <Figure n={notReady} label="not ready" tone="warning" title="Members whose /readyz answers 503" />
        <Figure n={lost} label="lost" tone="danger" title="Gone in the last 15 minutes without a drain" />
        <Figure n={departed.length - lost} label="left" title="Drained and left in the last 15 minutes" />
      </div>
      <ul aria-label="Nodes" className="flex flex-wrap gap-[3px] px-3 pt-2.5 pb-1">
        {all.map(({ view, status }) => (
          <li key={view.node.id} className={`${TONES[status.tone]} ${view.leftAt ? "opacity-40" : ""}`}>
            <Link to={nodePath(view.node.id)} title={`${view.node.id} · ${status.label}`} aria-label={`${view.node.id}, ${status.label}`} className="block size-[11px] rounded-[2px] bg-current hover:outline-1 hover:outline-text" />
          </li>
        ))}
      </ul>
      <ul aria-label="Nodes that need attention" className="flex min-h-[26px] flex-wrap items-center gap-x-3 gap-y-1 px-3 pb-2 text-[10px]">
        {attention.slice(0, 12).map(({ view, status }) => (
          <li key={view.node.id} className="inline-flex items-center gap-1.5">
            <Badge tone={status.tone}>{status.label}</Badge>
            <NodeName id={view.node.id} className="hover:underline" />
          </li>
        ))}
        {attention.length > 12 && <li className="text-muted">and {attention.length - 12} more</li>}
      </ul>
    </Panel>
  );
}

export function ClusterPage() {
  const at = useNow();
  const overview = useLive((s) => s.overview);
  const departed = useLive((s) => s.departed);
  const [open, setOpen] = useUrlState("node", "");
  const [sub, setSub] = useUrlState<Sub | "">("sub", "");
  // The drawer keeps what it showed while it slides out, after the URL has dropped it.
  const active = !!(open || sub);
  const [shown, setShown] = useState({ id: open, sub });
  if (active && (shown.id !== open || shown.sub !== sub)) setShown({ id: open, sub });
  const nodes = useMemo(() => overview?.nodes ?? [], [overview]);
  // A node is stale against the overview it came in, so a console that hears nothing paints no node as stale.
  const accs = useMemo(() => acceptorRows(nodes), [nodes]);
  // eslint-disable-next-line react-hooks/exhaustive-deps -- at: only for a console that hears nothing, and a new overview recomputes
  const rows = useMemo(() => buildRows(nodes, overview?.timestamp_ms ?? at, accs).sort((a, b) => WORST.indexOf(a.tone) - WORST.indexOf(b.tone) || natural(a.view.node.id, b.view.node.id)), [nodes, overview?.timestamp_ms, accs]);
  const live = rows.filter((r) => r.c);
  const gone = departed.filter((d) => !nodes.some((n) => n.node.id === d.node.id));
  const sum = (pick: (r: Row) => number | null | undefined) => live.reduce((t, r) => t + Math.max(0, pick(r) ?? 0), 0);
  const max = (pick: (r: Row) => number | null | undefined) => Math.max(0, ...live.map((r) => pick(r) ?? 0));
  const maxHeld = max((r) => r.held);
  const maxDenied = max((r) => r.denied);
  const maxCalls = max((r) => (r.calls.out ?? 0) + (r.calls.in ?? 0));
  const me = reporter(overview);
  const self = me?.snapshot?.cluster;
  const totalOut = sum((r) => r.calls.out);
  const totalFailed = sum((r) => r.calls.failed);
  const isolated = live.filter((r) => r.c?.isolated).length;
  const quoted = live.filter((r) => r.c?.quotas);
  const owned = live.find((r) => r.c?.ownership)?.c?.ownership;
  const select = (id: string | null, s: Sub | null) => {
    setOpen(id);
    setSub(s);
  };
  const totals = Object.fromEntries(SUBS.map(({ sub: s }) => {
    const bad = rows.filter((r) => odd(r.cells[s].tone));
    return [s, { tone: bad.length ? worst(bad.map((r) => r.cells[s].tone)) : "success", why: bad.map((r) => `${r.view.node.id}: ${r.cells[s].why[0]}`), bad: bad.length }];
  })) as Record<Sub, Cell & { bad: number }>;
  if (!overview) return <Empty title="No connection" detail="The console waits for the first overview." />;
  const by = me?.node.id ?? "";
  const failedShare = totalOut ? Math.min(totalFailed, totalOut) / totalOut : 0;
  return (
    <>
      <h1 className="sr-only-h1">Cluster</h1>
      <div className="scroll-y flex min-h-0 min-w-0 flex-1 flex-col gap-2.5">
        <div className="flex shrink-0 items-stretch gap-2">
          <div className="min-w-0 flex-1"><Members nodes={nodes} departed={departed} now={at} /></div>
          <div className="flex shrink-0 flex-col justify-center gap-1.5">
            <ExportButton dataset={{ name: "Cluster counters", count: live.length, filters: [], rows: () => live.map((r) => { const { engine: { peers: _p, request_rtt: _r, ...engine }, discovery, ownership, ...c } = r.c!; return { node: r.view.node.id, discovery: discovery.kind, ownership: ownership?.kind, ...c, engine }; }) }} />
          </div>
        </div>

        <div className="grid shrink-0 grid-cols-1 gap-2.5 min-[1100px]:grid-cols-[minmax(0,2fr)_minmax(0,3fr)]">
          <DiscoveryCard rows={rows} by={by} cell={totals.discovery} />
          <OwnershipCard rows={rows} accs={accs} by={by} cell={totals.leases} />
        </div>

        <section aria-label="Nodes by subsystem" data-sheet-keep className="shrink-0 overflow-hidden rounded-md border border-border bg-panel">
          <table className="w-full table-fixed text-[10px]">
            <colgroup>
              <col className="w-[18%] min-[1200px]:w-[15%]" />
              {SUBS.map(({ sub: s }) => <col key={s} />)}
            </colgroup>
            <thead className="text-left">
              <tr>
                <th scope="col" className="border-b border-border px-3 py-1.5 font-normal text-muted">Node</th>
                {SUBS.map(({ sub: s, title, help }) => (
                  <th key={s} scope="col" className="border-b border-l border-border p-0 font-normal">
                    <button type="button" onClick={() => select(null, s)} title={`${help} Click to see all nodes.`} className={`w-full truncate px-2.5 py-1.5 text-left hover:bg-raised hover:text-text ${sub === s && !open ? "text-accent" : "text-muted"}`}>{title}</button>
                  </th>
                ))}
              </tr>
              <tr className="bg-raised/50">
                <th scope="row" className="border-b border-border p-0 text-left align-top font-[550]">
                  <span className={`flex ${ROW_H} flex-col justify-center gap-[3px] px-3`}>
                    <span className={`${LINE} text-[11px]`}>Cluster</span>
                    <span className={`${LINE2} font-normal text-muted`}>{live.length} of {rows.length} with a report</span>
                  </span>
                </th>
                <MatrixCell cell={totals.membership} label="Cluster membership" onOpen={() => select(null, "membership")} main={self ? `${count(self.members)} members` : "—"} sub={self ? `${count(self.unreachable)} unreachable · ${isolated} isolated` : "no snapshot"} />
                <MatrixCell cell={totals.discovery} label="Cluster discovery" onOpen={() => select(null, "discovery")} main={<><ProviderLogo kind={self?.discovery.kind ?? ""} size={11} />{self?.discovery.kind ?? "—"}</>} sub={`${count(sum((r) => r.c?.discovery_errors))} errors · ${count(sum((r) => r.c?.heartbeat_failures))} heartbeat`} />
                <MatrixCell cell={totals.leases} label="Cluster leases" onOpen={() => select(null, "leases")} main={owned ? <><ProviderLogo kind={owned.kind} size={11} />{count(sum((r) => r.held))} held</> : "not configured"} sub={accs.length ? `${accs.filter((a) => a.state === "active").length} of ${accs.length} acceptors active` : `${count(sum((r) => r.c?.spawned))} keyed actors`} />
                <MatrixCell cell={totals.quotas} label="Cluster quotas" onOpen={() => select(null, "quotas")} main={quoted.length ? `${perSecond(sum((r) => r.denied))} denied` : "not in use"} sub={quoted.length ? `${count(sum((r) => r.c?.quotas?.rows))} rows on ${quoted.length} replicas` : "on no node"} />
                <MatrixCell cell={totals.calls} label="Cluster calls" onOpen={() => select(null, "calls")} main={perSecond(totalOut)} sub={totalOut ? `${percent(failedShare * 100)} without success` : "none now"} />
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const id = r.view.node.id;
                const c = r.c;
                const go = (s: Sub) => () => select(id, s);
                const seenOk = r.sees.filter((x) => x.state === "ok").length;
                const links = c?.discovery.links ?? [];
                return (
                  <tr key={id} aria-selected={active && open === id}>
                    <td className={`h-auto p-0 align-top font-sans text-[10px] ${r.tone === "danger" ? "shadow-[inset_3px_0_var(--danger)]" : r.tone === "warning" ? "shadow-[inset_3px_0_var(--warning)]" : ""}`}>
                      <button type="button" onClick={() => select(id, null)} className={`flex ${ROW_H} w-full min-w-0 flex-col justify-center gap-[3px] px-3 text-left hover:text-accent`}>
                        <span className={`${LINE} mono text-[11px]`}><NodeName id={id} link={false} className={odd(r.tone) ? TONES[r.tone] : ""} /></span>
                        <span className={`${LINE2} text-muted`}><span className="truncate">{[roleOf(r.view), r.acceptor && "acceptor"].filter(Boolean).join(" · ") || "—"}</span></span>
                      </button>
                    </td>
                    {!c ? (
                      <td colSpan={SUBS.length} className={`h-auto border-l border-border p-0 font-sans text-[10px] ${CELL_BG.danger} ${TONES.danger}`}>
                        <button type="button" onClick={() => select(id, null)} title={r.cells.membership.why[0]} className={`flex ${ROW_H} w-full items-center px-2.5 text-left`}><span className="truncate">{r.cells.membership.why[0]}</span></button>
                      </td>
                    ) : (
                      <>
                        <MatrixCell cell={r.cells.membership} label={`${id} membership`} onOpen={go("membership")} main={<>{c.isolated ? "isolated" : `sees ${seenOk}/${r.sees.length}`}{r.extra.length > 0 && <span className="text-warning">+{r.extra.length}</span>}</>} sub={<Dots sees={r.sees} />} />
                        <MatrixCell cell={r.cells.discovery} label={`${id} discovery`} onOpen={go("discovery")} main={<><ProviderLogo kind={c.discovery.kind} size={11} />{c.discovery.kind}</>} sub={links.length ? `${links.filter((l) => LINK_TONE[l.state] === "success").length}/${links.length} links up` : c.discovery_errors + c.heartbeat_failures ? `${count(c.discovery_errors + c.heartbeat_failures)} errors since its start` : "no errors"} />
                        <MatrixCell
                          cell={r.cells.leases}
                          label={`${id} leases`}
                          onOpen={go("leases")}
                          main={<>{c.ownership ? `${r.held ?? "—"} held` : "—"}{r.acceptor && <span title={`Paxos acceptor · ${r.acceptor.state}`} className={`rounded-sm border px-[3px] font-sans text-[9px] leading-[12px] ${r.acceptor.state === "active" ? "border-border text-muted" : "border-current text-warning"}`}>A{r.acceptor.state !== "active" && ` ${r.acceptor.state}`}</span>}</>}
                          sub={c.ownership ? <Bar value={r.held} max={maxHeld} /> : "not configured"}
                        />
                        <MatrixCell cell={r.cells.quotas} label={`${id} quotas`} onOpen={go("quotas")} main={c.quotas ? <>{c.quotas.pulling ? "pulls rows" : perSecond(r.denied)}<span className="font-sans text-[10px] text-muted">denied</span></> : "—"} sub={c.quotas ? <><Bar value={r.denied} max={maxDenied} /><span className="mono shrink-0">{count(c.quotas.rows)} rows</span></> : "not in use"} />
                        <MatrixCell cell={r.cells.calls} label={`${id} calls`} onOpen={go("calls")} main={<span title="Calls out per second">↑{perSecond(r.calls.out)}</span>} sub={<><Bar value={(r.calls.out ?? 0) + (r.calls.in ?? 0)} max={maxCalls} tone={r.cells.calls.tone} /><span className="mono shrink-0" title="Calls in per second">↓{perSecond(r.calls.in)}</span></>} />
                      </>
                    )}
                  </tr>
                );
              })}
              {gone.map((d) => (
                <tr key={d.node.id} className="text-muted">
                  <td className="h-[28px] px-3"><NodeName id={d.node.id} link={false} /></td>
                  <td colSpan={SUBS.length} className="h-[28px] border-l border-border px-2.5 font-sans text-[10px]">{nodeStatus(d, at).label}{d.leaveReason ? ` · ${d.leaveReason}` : ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="flex flex-wrap items-center gap-x-3 gap-y-1 border-t border-border px-3 py-1.5 text-[10px] text-muted">
            <span><Dot tone="success" />normal</span>
            <span><Dot tone="warning" />examine</span>
            <span><Dot tone="danger" />wrong now</span>
          </p>
        </section>
      </div>
      <Sheet open={active} onClose={() => select(null, null)}>
        <Drawer key={`${shown.id}/${shown.sub}`} rows={rows} id={shown.id} sub={(shown.sub || null) as Sub | null} onClose={() => select(null, null)} onPick={select} />
      </Sheet>
    </>
  );
}

const EXIT_MS = 200;
/**
 * A drawer at the right that slides and fades in and out; it stays mounted until its exit ends. It does not block the page,
 * so a click on another cell changes its content. The global reduced-motion rule in app.css removes the transition.
 */
function Sheet({ open, onClose, children }: { open: boolean; onClose: () => void; children: ReactNode }) {
  const [mounted, setMounted] = useState(open);
  if (open && !mounted) setMounted(true);
  useEffect(() => {
    if (open || !mounted) return;
    const t = setTimeout(() => setMounted(false), EXIT_MS);
    return () => clearTimeout(t);
  }, [open, mounted]);
  useEscape(open, onClose);
  useEffect(() => {
    if (!open) return;
    const down = (e: PointerEvent) => !(e.target as Element).closest("[data-sheet], [data-sheet-keep], dialog") && onClose();
    document.addEventListener("pointerdown", down);
    return () => document.removeEventListener("pointerdown", down);
  }, [open, onClose]);
  if (!mounted) return null;
  return (
    <aside
      data-sheet
      aria-label="Details"
      aria-hidden={!open}
      className={`fixed top-0 right-0 bottom-0 z-40 flex w-[min(480px,100vw)] flex-col border-l border-border bg-panel shadow-float transition-[translate,opacity] duration-200 ${open ? "translate-x-0 opacity-100 ease-out starting:translate-x-full starting:opacity-0" : "pointer-events-none translate-x-full opacity-0 ease-in"}`}
    >
      {children}
    </aside>
  );
}

function Drawer({ rows, id, sub, onClose, onPick }: { rows: Row[]; id: string; sub: Sub | null; onClose: () => void; onPick: (id: string | null, s: Sub | null) => void }) {
  const row = rows.find((r) => r.view.node.id === id);
  const at = useNow();
  const status = row && nodeStatus(row.view, at);
  const order = sub ? [SUBS.find((s) => s.sub === sub)!, ...SUBS.filter((s) => s.sub !== sub)] : SUBS;
  const i = rows.findIndex((r) => r.view.node.id === id);
  const step = (d: number) => rows[(i + d + rows.length) % rows.length]?.view.node.id ?? null;
  const ready = row && readiness(row.view);
  return (
    <>
      <header className="flex min-h-[44px] shrink-0 items-center gap-2 border-b border-border px-3">
        <h2 className="mono flex min-w-0 items-center gap-2 text-[13px] font-[550]">
          {id ? <NodeName id={id} link={false} /> : <span className="font-sans">{SUBS.find((s) => s.sub === sub)?.title} · all nodes</span>}
        </h2>
        {status && <Badge tone={status.tone}>{status.label}</Badge>}
        <span className="ml-auto flex items-center gap-2 text-[10px]">
          {id && rows.length > 1 && (
            <>
              <button type="button" onClick={() => onPick(step(-1), sub)} aria-label="Previous node" title="Previous node" className="rounded-sm px-1 text-[13px] text-muted hover:bg-raised hover:text-text">‹</button>
              <button type="button" onClick={() => onPick(step(1), sub)} aria-label="Next node" title="Next node" className="rounded-sm px-1 text-[13px] text-muted hover:bg-raised hover:text-text">›</button>
            </>
          )}
          {id && <Link to={nodePath(id)} className="text-muted hover:text-text">Open node ↗</Link>}
          <button type="button" onClick={onClose} aria-label="Close" className="text-[14px] text-muted hover:text-text">✕</button>
        </span>
      </header>
      <div className="scroll-y flex-1 transition-opacity duration-150 starting:opacity-0">
        {id && !row && <Empty title={`${id} is not in this cluster`} detail="It possibly left." />}
        {row && !row.c && <Empty title="No snapshot from this node" detail={row.view.error ?? "It did not answer yet."} />}
        {row?.c && (
          <>
            <p className={`border-b border-border px-3 py-2 text-[11px] ${ready?.length ? "text-warning" : "text-muted"}`}>
              {[roleOf(row.view), row.acceptor && `Paxos acceptor, ${row.acceptor.state}`].filter(Boolean).join(" · ")}
              {roleOf(row.view) || row.acceptor ? " · " : ""}
              {ready?.length ? `not ready: ${ready.join(", ")}` : "ready: passes /readyz"}
            </p>
            {order.map(({ sub: s, title, help }) => <NodeSection key={s} row={row} sub={s} title={title} help={help} focus={s === sub} />)}
          </>
        )}
        {!id && sub && <ClusterSection rows={rows} sub={sub} onPick={onPick} />}
      </div>
    </>
  );
}

function Section({ title, help, tone, why, focus, children }: { title: string; help: string; tone: Tone; why: string[]; focus?: boolean; children: ReactNode }) {
  return (
    <section className={`border-b border-border px-3 py-2.5 ${focus ? "bg-raised/40" : ""}`}>
      <h3 className="flex items-center gap-2 text-[12px] font-[550]">
        {title}
        <Badge tone={tone}>{TONE_WORD[tone]}</Badge>
      </h3>
      <p className="mb-1.5 text-[10px] text-muted">{help}</p>
      {why.length > 0 && (
        <ul className={`mb-2 list-disc pl-4 text-[11px] ${TONES[tone]}`}>
          {why.map((w) => <li key={w}>{w}</li>)}
        </ul>
      )}
      {children}
    </section>
  );
}

/** Counters with their meaning; a rate beside each that moves, in amber when an increase needs attention. */
function Counters({ groups, value, now }: { groups: [string, Def[]][]; value: (d: Def) => number | undefined; now?: (d: Def) => number | undefined }) {
  return (
    <>
      {groups.map(([group, defs]) => (
        <div key={group} className="mb-2">
          <p className="mb-0.5 text-[10px] text-muted">{group}</p>
          <dl>
            {defs.map((d) => {
              const rate = d.gauge ? undefined : now?.(d);
              const hot = d.watch && (rate ?? 0) > 0;
              return (
                <div key={d.id} className="grid grid-cols-[minmax(0,1fr)_auto_64px] items-baseline gap-x-3 border-t border-border/60 py-[3px] first:border-t-0" title={d.help}>
                  <dt className="min-w-0">
                    <span className="text-[11px]">{d.label}</span>
                    <span className="block truncate text-[10px] text-muted">{d.help}</span>
                  </dt>
                  <dd className={`mono text-right text-[11px] ${d.watch && (value(d) ?? 0) > 0 ? "text-warning" : ""}`}>{count(value(d))}</dd>
                  <dd className={`mono text-right text-[10px] ${hot ? "text-warning" : "text-muted"}`}>{rate === undefined ? "" : perSecond(Math.max(0, rate))}</dd>
                </div>
              );
            })}
          </dl>
        </div>
      ))}
    </>
  );
}

/** What one node reports of its own provider: its settings, its stats, its links. */
function Provider({ info }: { info: ProviderInfo }) {
  const rows = [...info.settings, ...info.stats];
  return (
    <div className="mb-2 rounded-md border border-border">
      <p className="flex items-center gap-2 border-b border-border px-2.5 py-1.5 text-[11px] font-[550] capitalize"><ProviderLogo kind={info.kind} size={14} />{info.kind}</p>
      <dl className="px-2.5 py-1">
        {rows.map(([k, v]) => (
          <div key={k} className="flex items-baseline justify-between gap-3 py-[2px]" title={words(k)[1]}>
            <dt className="min-w-0 truncate text-[10px] text-muted">{words(k)[0]}</dt>
            <dd className="mono min-w-0 text-right text-[11px] break-words">{statValue(k, v)}</dd>
          </div>
        ))}
      </dl>
      {info.links.length > 0 && (
        <table className="w-full table-fixed border-t border-border text-[10px]">
          <thead>
            <tr><th className="px-2.5 py-1 text-[10px] text-muted">Link to</th><th className="py-1 text-[10px] text-muted">State and time</th><th className="px-2.5 py-1 text-[10px] text-muted">Error</th></tr>
          </thead>
          <tbody>
            {info.links.map((l) => (
              <tr key={l.target} title={l.error ?? (l.identity ? `It answers as ${l.identity}` : undefined)}>
                <td className={MINI_TD}><NodeName id={l.target} link={false} /></td>
                <td className={MINI_TD}><LinkCell link={l} /></td>
                <td className={`${MINI_TD} ${l.error ? "text-warning" : "text-muted"}`}>{l.error ?? "none"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

/** One subsystem as one node reports it: only the figures of that node. */
function NodeSection({ row, sub, title, help, focus }: { row: Row; sub: Sub; title: string; help: string; focus: boolean }) {
  const c = row.c!;
  const cell = row.cells[sub];
  const missing = row.sees.filter((x) => x.state !== "ok");
  return (
    <Section title={title} help={help} tone={cell.tone} why={cell.why} focus={focus}>
      {sub === "membership" && (
        <>
          <p className="mb-2 text-[11px]">{c.isolated ? "Isolated. It stopped its keyed actors." : missing.length || row.extra.length ? "Its view is different from the view of the console." : `It sees all ${row.sees.length} members, and it can reach them.`}</p>
          {missing.length > 0 && <p className="mb-2 text-[10px] text-warning">{missing.map((m) => `${m.id} ${m.state === "missing" ? "not in its view" : "unreachable"}`).join(" · ")}</p>}
        </>
      )}
      {sub === "discovery" && <><Provider info={c.discovery} />{c.last_heartbeat_error && <p className="mb-2 text-[10px] text-warning">Last heartbeat error: {c.last_heartbeat_error}</p>}</>}
      {sub === "leases" && (c.ownership ? <Provider info={c.ownership} /> : <p className="mb-2 text-[11px]">Ownership is not configured. A keyed actor is unique in each membership view only.</p>)}
      {sub === "quotas" && !c.quotas ? <p className="text-[11px] text-muted">Quotas are not in use on this node.</p> : (
        <Counters groups={DEFS[sub]} value={(d) => d.get(c)} now={(d) => row.now.get(d.id)} />
      )}
    </Section>
  );
}

/** A column title opens this: one subsystem over all nodes, the nodes with a problem first. */
function ClusterSection({ rows, sub, onPick }: { rows: Row[]; sub: Sub; onPick: (id: string | null, s: Sub | null) => void }) {
  const overview = useLive((s) => s.overview);
  const live = rows.filter((r) => r.c);
  const self = reporter(overview);
  const info = SUBS.find((s) => s.sub === sub)!;
  const bad = rows.filter((r) => odd(r.cells[sub].tone));
  // Each node sees each join and leave, so membership comes from one node; the rest is work of each node, added up.
  const summed = sub !== "membership";
  const value = (d: Def) => (summed ? live.reduce((t, r) => t + (d.get(r.c!) ?? 0), 0) : self?.snapshot ? d.get(self.snapshot.cluster) : undefined);
  const now = (d: Def) => (summed ? live.reduce((t, r) => t + Math.max(0, r.now.get(d.id) ?? 0), 0) : self ? rows.find((r) => r.view.node.id === self.node.id)?.now.get(d.id) : undefined);
  return (
    <Section title={info.title} help={info.help} tone={bad.length ? worst(bad.map((r) => r.cells[sub].tone)) : "success"} why={[]} focus>
      {bad.length > 0 && (
        <ul className="mb-2 flex flex-col gap-1">
          {bad.map((r) => (
            <li key={r.view.node.id}>
              <button type="button" onClick={() => onPick(r.view.node.id, sub)} className={`mono flex w-full items-baseline gap-2 rounded-sm px-1.5 py-1 text-left text-[11px] hover:bg-raised ${CELL_BG[r.cells[sub].tone]}`}>
                {r.view.node.id}
                <span className={`min-w-0 truncate font-sans text-[10px] ${TONES[r.cells[sub].tone]}`}>{r.cells[sub].why.join(" · ")}</span>
              </button>
            </li>
          ))}
        </ul>
      )}
      <p className="mb-2 text-[10px] text-muted">{summed ? `The sum over the ${live.length} nodes with a report. Each node counts from its own start.` : `The view of ${self?.node.id ?? "the console node"}. Each node sees each join and leave, so a sum counts each one many times.`}</p>
      {sub === "membership" && <Readiness />}
      {sub === "quotas" && <QuotaTrend live={(reason) => live.reduce((t, r) => t + Math.max(0, r.now.get(`denied_${reason}`) ?? 0), 0)} />}
      <Counters groups={DEFS[sub]} value={value} now={now} />
    </Section>
  );
}

function Readiness() {
  const nodes = useLive((s) => s.overview?.nodes ?? []);
  const not = nodes.filter((n) => !n.leftAt).map((n) => [n.node.id, readiness(n)] as const).filter(([, r]) => !r || r.length);
  return (
    <p className={`mb-2 text-[11px] ${not.length ? "text-warning" : ""}`}>
      {not.length ? `Not ready: ${not.map(([id, r]) => `${id} (${r ? r.join(", ") : "no report"})`).join(" · ")}` : "All nodes pass /readyz."}
    </p>
  );
}

function QuotaTrend({ live }: { live: (reason: "limit" | "share") => number }) {
  const history = useHistory({ metrics: ["quota.denied"], span: 3_600_000, points: 144, sum: "cluster", by: ["reason"], refetchMs: 60_000 });
  return (
    <div className="mb-2 grid grid-cols-2 gap-3">
      {(["limit", "share"] as const).map((reason) => {
        const points = seriesLine(history.data, "quota.denied", (s) => s.labels.reason === reason, true);
        return (
          <div key={reason}>
            <div className="flex items-baseline justify-between gap-2 text-[10px] text-muted"><span>Denied by {reason}</span><span className="mono text-text">{perSecond(live(reason) || latest(points))}</span></div>
            <Sparkline points={points} label={`Denied by ${reason} per second · ${history.data ? "last hour" : "no history"}`} height={24} />
          </div>
        );
      })}
    </div>
  );
}
